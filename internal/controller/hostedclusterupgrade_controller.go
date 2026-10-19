/*
Copyright 2026 SRE Fleet Team.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	hypershiftv1beta1 "github.com/openshift/hypershift/api/hypershift/v1beta1"
	"github.com/prometheus/client_golang/prometheus"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	upgradev1alpha1 "github.com/duritong/hypershift-upgrade-coordinator/api/v1alpha1"
)

// LogScraper defines an interface for fetching HCCO logs (allows mock testing).
type LogScraper interface {
	GetHCCOLogs(ctx context.Context, namespace, podName string, since time.Time) (string, error)
}

// KubeLogScraper fetches HCCO logs from the cluster.
type KubeLogScraper struct {
	Client kubernetes.Interface
}

func (k *KubeLogScraper) GetHCCOLogs(ctx context.Context, namespace, podPrefix string, since time.Time) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, drainLogTimeout)
	defer cancel()
	if k.Client == nil {
		return "", fmt.Errorf("kubernetes client is nil")
	}

	podList, err := k.Client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: "app=hosted-cluster-config-operator",
	})
	if err != nil || len(podList.Items) == 0 {
		podList, err = k.Client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
		if err != nil {
			return "", fmt.Errorf("HCCO pod discovery unavailable")
		}
	}

	var targetPod string
	for _, p := range podList.Items {
		if strings.HasPrefix(p.Name, podPrefix) || p.Labels["app"] == "hosted-cluster-config-operator" {
			if p.Status.Phase == corev1.PodRunning {
				targetPod = p.Name
				break
			}
			if targetPod == "" {
				targetPod = p.Name
			}
		}
	}

	if targetPod == "" {
		return "", fmt.Errorf("no HCCO pod found in namespace %s", namespace)
	}

	tailLines := int64(200)
	limitBytes := int64(drainLogByteLimit)
	sinceTime := metav1.NewTime(since)
	req := k.Client.CoreV1().Pods(namespace).GetLogs(targetPod, &corev1.PodLogOptions{
		TailLines:  &tailLines,
		LimitBytes: &limitBytes,
		Timestamps: true,
		SinceTime:  &sinceTime,
	})
	podLogs, err := req.Stream(ctx)
	if err != nil {
		return "", fmt.Errorf("HCCO log request unavailable")
	}
	defer podLogs.Close()

	return readDrainLogs(podLogs)
}

// HostedClusterUpgradeReconciler reconciles a HostedClusterUpgrade object
type HostedClusterUpgradeReconciler struct {
	client.Client
	APIReader              client.Reader // Uncached, get-only access to HC pull secrets.
	ReleaseResolver        ReleaseMetadataResolver
	Scheme                 *runtime.Scheme
	HostedClusterNamespace string
	LogScraper             LogScraper // Injectable log scraper for testing
}

// Official GVKs from HyperShift API schema
var gvkCPC = hypershiftv1beta1.GroupVersion.WithKind("ControlPlaneComponent")
var gvkHC = hypershiftv1beta1.GroupVersion.WithKind("HostedCluster")
var gvkNP = hypershiftv1beta1.GroupVersion.WithKind("NodePool")

// DefaultMockLogScraper returns a static log block for testing if no LogScraper is injected.
type DefaultMockLogScraper struct {
	MockLogs string
}

func (d *DefaultMockLogScraper) GetHCCOLogs(ctx context.Context, namespace, podName string, since time.Time) (string, error) {
	return d.MockLogs, nil
}

// Prometheus SRE metrics definitions
var (
	metricDrainAvailable = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "hypershift_upgrade_drain_diagnostics_available",
		Help: "Whether best-effort cluster-level HCCO log collection succeeded; not drain health.",
	}, []string{"namespace", "name"})
	metricDrainObservations = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "hypershift_upgrade_drain_observations",
		Help: "Recent cluster-level drain/eviction log hints, not attributed NodePool blockers.",
	}, []string{"namespace", "name"})
	metricPhase = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "hypershift_upgrade_phase",
			Help: "Current phase of the upgrade state machine (integer mapped).",
		},
		[]string{"namespace", "name", "current_version", "target_version", "intermediate_version"},
	)
	metricDuration = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "hypershift_upgrade_duration_seconds",
			Help: "Active elapsed overall upgrade time in seconds.",
		},
		[]string{"namespace", "name", "current_version", "target_version", "intermediate_version"},
	)
	metricStepDuration = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "hypershift_upgrade_step_duration_seconds",
			Help: "Active elapsed duration for a specific step in seconds.",
		},
		[]string{"namespace", "name", "step", "current_version", "target_version"},
	)
	metricFailedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "hypershift_upgrade_failed_total",
			Help: "Total number of retries or failures encountered.",
		},
		[]string{"namespace", "name", "current_version", "target_version", "reason"},
	)
	metricComponentReady = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "hypershift_upgrade_controlplane_component_ready",
			Help: "Availability and completeness state of individual components (0=Unavailable, 1=Rolling, 2=Ready).",
		},
		[]string{"namespace", "name", "component"},
	)
	metricNodePoolBlocked = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "hypershift_upgrade_nodepool_blocked",
			Help: "Boolean flag (1=Blocked, 0=Active/Complete) indicating if a NodePool rollout has stalled.",
		},
		[]string{"namespace", "name", "nodepool", "current_version", "target_version"},
	)
	metricNodePoolRolloutRatio = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "hypershift_upgrade_nodepool_rollout_ratio",
			Help: "Ratio of nodes running the target version and ready.",
		},
		[]string{"namespace", "name", "nodepool"},
	)
	metricAggregatedNodeRolloutRatio = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "hypershift_upgrade_aggregated_node_rollout_ratio",
			Help: "Overall ratio of upgraded worker nodes (summed across all NodePools) to total desired worker nodes in the cluster.",
		},
		[]string{"namespace", "name"},
	)
)

func init() {
	// Register Prometheus SRE metrics on the controller-runtime metrics registry
	metrics.Registry.MustRegister(
		metricDrainAvailable, metricDrainObservations,
		metricPhase,
		metricDuration,
		metricStepDuration,
		metricFailedTotal,
		metricComponentReady,
		metricNodePoolBlocked,
		metricNodePoolRolloutRatio,
		metricAggregatedNodeRolloutRatio,
	)
}

//+kubebuilder:rbac:groups=upgrade.0xshift.dev,resources=hostedclusterupgrades,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=upgrade.0xshift.dev,resources=hostedclusterupgrades/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=upgrade.0xshift.dev,resources=hostedclusterupgrades/finalizers,verbs=update
//+kubebuilder:rbac:groups=hypershift.openshift.io,resources=controlplanecomponents;hostedclusters;nodepools,verbs=get;list;watch;update;patch
//+kubebuilder:rbac:groups=hypershift.openshift.io,resources=hostedcontrolplanes,verbs=get;list;watch
//+kubebuilder:rbac:groups="",resources=pods;pods/log,verbs=get;list
//+kubebuilder:rbac:groups="",resources=secrets,verbs=get
//+kubebuilder:rbac:groups=cluster.x-k8s.io,resources=machinedeployments;machinesets,verbs=get;list;watch

// Reconcile is the main state machine loop for HostedClusterUpgrade.
func (r *HostedClusterUpgradeReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	reqLogger := log.FromContext(ctx)
	reqLogger.Info("Reconciling HostedClusterUpgrade")

	// 1. Fetch the HostedClusterUpgrade object
	hcu := &upgradev1alpha1.HostedClusterUpgrade{}
	err := r.Get(ctx, req.NamespacedName, hcu)
	if err != nil {
		if apierrors.IsNotFound(err) {
			clearUpgradeGauges(req.Namespace, req.Name)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Filter by target namespace if configured
	if r.HostedClusterNamespace != "" && hcu.Namespace != r.HostedClusterNamespace {
		return ctrl.Result{}, nil
	}
	if !hcu.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(hcu, workerLocksFinalizer) {
			pools := &unstructured.UnstructuredList{}
			pools.SetGroupVersionKind(gvkNP)
			if err := r.List(ctx, pools, client.InNamespace(hcu.Namespace)); err != nil {
				return ctrl.Result{}, err
			}
			for _, pool := range pools.Items {
				if pool.GetAnnotations()[workerLockOwner] != string(hcu.UID) {
					continue
				}
				if changed, err := restoreNodePoolLock(&pool, hcu); err != nil {
					return ctrl.Result{}, err
				} else if changed {
					if err := r.Update(ctx, &pool); err != nil {
						return ctrl.Result{}, err
					}
				}
			}
			controllerutil.RemoveFinalizer(hcu, workerLocksFinalizer)
			if err := r.Update(ctx, hcu); err != nil {
				return ctrl.Result{}, err
			}
		}
		clearUpgradeGauges(hcu.Namespace, hcu.Name)
		return ctrl.Result{}, nil
	}
	// Register cleanup before acquiring any worker locks.
	if controllerutil.AddFinalizer(hcu, workerLocksFinalizer) {
		if err := r.Update(ctx, hcu); err != nil {
			return ctrl.Result{}, err
		}
	}

	// Save original status for deep-equal check later to avoid redundant updates
	originalStatus := hcu.Status.DeepCopy()

	// 2. Check Coordinator Pause State (spec.pausedUntil)
	isPaused, pausedRequeueAfter, err := checkPausedState(hcu.Spec.PausedUntil)
	if err != nil {
		reqLogger.Error(err, "Failed to parse spec.pausedUntil")
		markUpgradeBlocked(hcu)
		hcu.Status.Message = fmt.Errorf("invalid spec.pausedUntil format: %v", err).Error()

		// Record metric failure
		metricFailedTotal.WithLabelValues(hcu.Namespace, hcu.Name, "", "", "pausedUntilParseError").Inc()
		r.exposePhaseMetrics(ctx, hcu, nil)
		r.updateConditions(hcu)

		if !reflect.DeepEqual(originalStatus, &hcu.Status) {
			if statusErr := r.Status().Update(ctx, hcu); statusErr != nil {
				return ctrl.Result{}, statusErr
			}
		}
		return ctrl.Result{}, err
	}

	if isPaused {
		reqLogger.Info("Upgrade coordinator is currently paused (Observe-Only Mode)")
		if hcu.Status.Phase != upgradev1alpha1.PhasePaused {
			hcu.Status.PhaseBeforePause = hcu.Status.Phase
		}
		hcu.Status.Phase = upgradev1alpha1.PhasePaused
		hcu.Status.Message = "Upgrade coordinator is paused via spec.pausedUntil."

		// Optionally try to fetch target HostedCluster to aggregate status and expose metrics
		uHC := &unstructured.Unstructured{}
		uHC.SetGroupVersionKind(gvkHC)
		if getErr := r.Get(ctx, client.ObjectKey{Namespace: hcu.Namespace, Name: hcu.Name}, uHC); getErr != nil {
			uHC = nil
		}
		if uHC != nil && hcu.Status.InitialVersion == "" {
			_, hcu.Status.InitialVersion = GetHostedClusterCurrentRelease(uHC)
		}

		expectedCPVer, _ := upgradev1alpha1.ExtractVersion(hcu.Status.ControlPlaneStatus.TargetReleaseImage)
		_ = r.aggregateControlPlaneStatus(ctx, hcu, expectedCPVer)
		r.refreshDrainDiagnostics(ctx, hcu)

		// Expose Paused metrics
		r.exposePhaseMetrics(ctx, hcu, uHC)
		r.updateConditions(hcu)

		if pausedRequeueAfter > 0 {
			reqLogger.Info("Requeuing for scheduled pause expiration", "requeueAfter", pausedRequeueAfter)
		}

		if !reflect.DeepEqual(originalStatus, &hcu.Status) {
			err = r.Status().Update(ctx, hcu)
			if err != nil {
				return ctrl.Result{}, err
			}
		}
		if pausedRequeueAfter <= 0 || pausedRequeueAfter > 30*time.Second {
			pausedRequeueAfter = 30 * time.Second
		}
		return ctrl.Result{RequeueAfter: pausedRequeueAfter}, nil
	}

	// 3. Fetch the Target HostedCluster to extract versions and status
	uHC := &unstructured.Unstructured{}
	uHC.SetGroupVersionKind(gvkHC)
	err = r.Get(ctx, client.ObjectKey{Namespace: hcu.Namespace, Name: hcu.Name}, uHC)
	if err != nil {
		if apierrors.IsNotFound(err) {
			markUpgradeBlocked(hcu)
			hcu.Status.Message = fmt.Sprintf("Target HostedCluster '%s' does not exist inside namespace '%s'", hcu.Name, hcu.Namespace)

			// Record metric failure
			metricFailedTotal.WithLabelValues(hcu.Namespace, hcu.Name, "", "", "HostedClusterNotFound").Inc()
			r.exposePhaseMetrics(ctx, hcu, nil)

			r.updateConditions(hcu)
			if !reflect.DeepEqual(originalStatus, &hcu.Status) {
				if statusErr := r.Status().Update(ctx, hcu); statusErr != nil {
					return ctrl.Result{}, statusErr
				}
			}
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
		return ctrl.Result{}, err
	}

	if hcu.Status.InitialVersion == "" {
		_, hcu.Status.InitialVersion = GetHostedClusterCurrentRelease(uHC)
	}

	// Admission may have been bypassed, and the cluster may have changed since
	// admission. Recheck the remaining path before any resource mutations.
	if err := upgradev1alpha1.RejectControlPlaneReleaseOverride(uHC); err != nil {
		markUpgradeBlocked(hcu)
		hcu.Status.Message = err.Error()
		r.updateConditions(hcu)
		r.exposePhaseMetrics(ctx, hcu, uHC)
		if !reflect.DeepEqual(originalStatus, &hcu.Status) {
			return ctrl.Result{RequeueAfter: 30 * time.Second}, r.Status().Update(ctx, hcu)
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	if installing, _ := r.checkClusterInstallingAndHealth(uHC); !installing {
		currentRelease, currentVersion := GetHostedClusterCurrentRelease(uHC)
		if currentVersion != "" {
			currentRelease = currentVersion
		}
		if err := upgradev1alpha1.ValidateUpgradePath(currentRelease, hcu.Spec.TargetReleaseImage, hcu.Spec.IntermediateReleaseImage, true); err != nil {
			markUpgradeBlocked(hcu)
			hcu.Status.Message = fmt.Sprintf("Upgrade version safety gate blocked: %v", err)
			r.updateConditions(hcu)
			r.exposePhaseMetrics(ctx, hcu, uHC)
			if !reflect.DeepEqual(originalStatus, &hcu.Status) {
				return ctrl.Result{RequeueAfter: 30 * time.Second}, r.Status().Update(ctx, hcu)
			}
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
	}

	if err := r.verifyReleasePayloads(ctx, hcu, uHC); err != nil {
		markUpgradeBlocked(hcu)
		hcu.Status.Message = fmt.Sprintf("Release payload safety gate blocked: %v", err)
		r.updateConditions(hcu)
		r.exposePhaseMetrics(ctx, hcu, uHC)
		if !reflect.DeepEqual(originalStatus, &hcu.Status) {
			if err := r.Status().Update(ctx, hcu); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	// 4. Aggregate ControlPlaneComponent status (for SRE observability)
	expectedCPVer, _ := upgradev1alpha1.ExtractVersion(hcu.Status.ControlPlaneStatus.TargetReleaseImage)
	err = r.aggregateControlPlaneStatus(ctx, hcu, expectedCPVer)
	if err != nil {
		reqLogger.Error(err, "Failed to aggregate ControlPlaneComponent status")
	}

	// 5. Run State Machine transitions
	if hcu.Status.Phase == upgradev1alpha1.PhasePaused {
		hcu.Status.Phase = hcu.Status.PhaseBeforePause
		hcu.Status.PhaseBeforePause = ""
	}
	retryBlockedStep(hcu)
	if hcu.Status.Phase == upgradev1alpha1.PhaseUpgradingNodePools && hcu.Status.ControlPlaneStatus.TargetReleaseImage != "" && hcu.Status.ControlPlaneStatus.TargetReleaseImage != hcu.Spec.TargetReleaseImage {
		// A changed plan must verify its new control plane before touching workers.
		hcu.Status.Phase = upgradev1alpha1.PhasePreflightChecks
	}
	if hcu.Status.Phase == upgradev1alpha1.PhaseCompleted {
		completedImage := hcu.Status.ControlPlaneStatus.TargetReleaseImage
		if completedImage == "" {
			completedImage, _ = GetHostedClusterCurrentRelease(uHC)
		}
		if completedImage != hcu.Spec.TargetReleaseImage {
			hcu.Status = upgradev1alpha1.HostedClusterUpgradeStatus{}
			_, hcu.Status.InitialVersion = GetHostedClusterCurrentRelease(uHC)
		}
	}
	if hcu.Status.Phase == "" {
		hcu.Status.Phase = upgradev1alpha1.PhaseIdle
		hcu.Status.Message = "Ready to proceed. Initializing upgrade state machine."
	}

	// Set Start Time once we move past Idle
	if hcu.Status.Phase != upgradev1alpha1.PhaseIdle && hcu.Status.StartTime == nil {
		now := metav1.Now()
		hcu.Status.StartTime = &now
	}

	var stateRequeueAfter time.Duration

	switch hcu.Status.Phase {
	case upgradev1alpha1.PhaseIdle:
		hcu.Status.Phase = upgradev1alpha1.PhasePreflightChecks
		hcu.Status.Message = "Beginning Preflight Checks."

	case upgradev1alpha1.PhasePreflightChecks:
		isInstalling, err := r.checkClusterInstallingAndHealth(uHC)
		if err != nil {
			reqLogger.Error(err, "PreflightChecks failed")
			markUpgradeBlocked(hcu)
			hcu.Status.Message = fmt.Sprintf("Preflight Checks Blocked: %v", err)
			metricFailedTotal.WithLabelValues(hcu.Namespace, hcu.Name, "", "", "PreflightBlocked").Inc()
			break
		}

		if isInstalling {
			hcu.Status.Phase = upgradev1alpha1.PhaseInstalling
			hcu.Status.Message = "HostedCluster is currently in initial installation. Waiting for completion."
			break
		}

		// Ensure all NodePools are locked before progressing past preflight
		baseline, _ := GetHostedClusterCurrentRelease(uHC)
		desired, _, _ := unstructured.NestedString(uHC.Object, "spec", "release", "image")
		if samePayload(baseline, desired) {
			if err := r.validateControlPlaneWorkerSkew(ctx, hcu, uHC); err != nil {
				markUpgradeBlocked(hcu)
				hcu.Status.Message = err.Error()
				break
			}
			next, planErr := plannedControlPlaneRelease(uHC, hcu)
			if planErr != nil {
				markUpgradeBlocked(hcu)
				hcu.Status.Message = planErr.Error()
				break
			}
			if !samePayload(next, desired) {
				if err := canInitiateControlPlane(uHC); err != nil {
					hcu.Status.Message = err.Error()
					stateRequeueAfter = 10 * time.Second
					break
				}
			}
			if err := r.verifyControlPlane(ctx, hcu, uHC, baseline); err != nil {
				hcu.Status.Message = fmt.Sprintf("Preflight CP prerequisite: %v", err)
				stateRequeueAfter = 10 * time.Second
				break
			}
			if err := r.WorkerPrerequisite(ctx, hcu); err != nil {
				return ctrl.Result{}, fmt.Errorf("worker prerequisite before locks: %w", err)
			}
			if err := r.lockClusterNodePools(ctx, hcu); err != nil {
				return ctrl.Result{}, fmt.Errorf("lock NodePools during preflight: %w", err)
			}
		}

		hcu.Status.Phase = upgradev1alpha1.PhasePendingSchedule
		hcu.Status.Message = "Preflight Checks passed. Checking upgrade schedules."

	case upgradev1alpha1.PhaseInstalling:
		isInstalling, err := r.checkClusterInstallingAndHealth(uHC)
		if err != nil {
			markUpgradeBlocked(hcu)
			hcu.Status.Message = fmt.Sprintf("Installation failed or cluster became unhealthy: %v", err)
			metricFailedTotal.WithLabelValues(hcu.Namespace, hcu.Name, "", "", "InstallationUnhealthy").Inc()
			break
		}

		if !isInstalling {
			hcu.Status.Phase = upgradev1alpha1.PhasePendingSchedule
			hcu.Status.Message = "HostedCluster installation complete. Checking upgrade schedules."
		}

	case upgradev1alpha1.PhasePendingSchedule:
		if hcu.Spec.ControlPlane.StartAt != nil {
			now := time.Now()
			start := hcu.Spec.ControlPlane.StartAt.Time
			if now.Before(start) {
				requeueDur := start.Sub(now)
				hcu.Status.Phase = upgradev1alpha1.PhasePendingSchedule
				hcu.Status.Message = fmt.Sprintf("Control Plane upgrade scheduled to begin on %s (opens in %s)", start.Format(time.RFC3339), requeueDur.Round(time.Second).String())

				r.exposePhaseMetrics(ctx, hcu, uHC)
				r.updateConditions(hcu)
				if !reflect.DeepEqual(originalStatus, &hcu.Status) {
					if statusErr := r.Status().Update(ctx, hcu); statusErr != nil {
						return ctrl.Result{}, statusErr
					}
				}
				if requeueDur > 30*time.Second {
					requeueDur = 30 * time.Second
				}
				return ctrl.Result{RequeueAfter: requeueDur}, nil
			}
		}

		hcu.Status.Phase = upgradev1alpha1.PhaseUpgradingControlPlane
		hcu.Status.Message = "Control Plane schedule open. Ready to initiate rollout."

	case upgradev1alpha1.PhaseUpgradingControlPlane:
		// Pre-Step Health Re-verification check
		isInstalling, err := r.checkClusterInstallingAndHealth(uHC)
		if err != nil {
			markUpgradeBlocked(hcu)
			hcu.Status.Message = fmt.Sprintf("Pre-Step Health Gate Blocked CP Upgrade: %v", err)
			metricFailedTotal.WithLabelValues(hcu.Namespace, hcu.Name, "", "", "PreStepCPHealthBlocked").Inc()
			break
		}
		if isInstalling {
			hcu.Status.Phase = upgradev1alpha1.PhaseInstalling
			hcu.Status.Message = "HostedCluster is currently in initial installation. Waiting for completion."
			break
		}

		_, currentVer := GetHostedClusterCurrentRelease(uHC)
		targetVer, _ := upgradev1alpha1.ExtractVersion(hcu.Spec.TargetReleaseImage)
		nextReleaseImage, planErr := plannedControlPlaneRelease(uHC, hcu)
		if planErr != nil {
			markUpgradeBlocked(hcu)
			hcu.Status.Message = planErr.Error()
			break
		}
		stepLabel := "Target"
		if nextReleaseImage == hcu.Spec.IntermediateReleaseImage {
			stepLabel = "Intermediate"
		}
		previousImage, _, _ := unstructured.NestedString(uHC.Object, "spec", "release", "image")
		if samePayload(previousImage, nextReleaseImage) {
			hcu.Status.ControlPlaneStatus.TargetReleaseImage = nextReleaseImage
			hcu.Status.Phase = upgradev1alpha1.PhaseVerifyingControlPlane
			hcu.Status.Message = "Adopting current HostedCluster release request. Verifying exact payload completion."
			break
		}

		// Apply the version change on the HostedCluster spec!
		if start := hcu.Spec.ControlPlane.StartAt; start != nil && time.Now().Before(start.Time) {
			hcu.Status.Phase = upgradev1alpha1.PhasePendingSchedule
			hcu.Status.Message = "Control Plane schedule changed; waiting before initiating a new release."
			stateRequeueAfter = min(time.Until(start.Time), 5*time.Minute)
			break
		}
		if err := r.validateControlPlaneWorkerSkew(ctx, hcu, uHC); err != nil {
			markUpgradeBlocked(hcu)
			hcu.Status.Message = err.Error()
			break
		}
		if err := canInitiateControlPlane(uHC); err != nil {
			hcu.Status.Message = err.Error()
			stateRequeueAfter = 10 * time.Second
			break
		}
		baseline, _ := GetHostedClusterCurrentRelease(uHC)
		if err := r.verifyControlPlane(ctx, hcu, uHC, baseline); err != nil {
			hcu.Status.Message = fmt.Sprintf("Previous CP prerequisite: %v", err)
			stateRequeueAfter = 10 * time.Second
			break
		}
		if err := r.WorkerPrerequisite(ctx, hcu); err != nil {
			return ctrl.Result{}, fmt.Errorf("worker prerequisite before CP change: %w", err)
		}
		if err := r.lockClusterNodePools(ctx, hcu); err != nil {
			return ctrl.Result{}, fmt.Errorf("lock NodePools during CP upgrade: %w", err)
		}
		if err := r.WorkerPrerequisite(ctx, hcu); err != nil {
			return ctrl.Result{}, fmt.Errorf("worker prerequisite immediately before CP change: %w", err)
		}
		reqLogger.Info("Applying version change on HostedCluster spec", "step", stepLabel, "image", nextReleaseImage)
		err = unstructured.SetNestedField(uHC.Object, nextReleaseImage, "spec", "release", "image")
		if err != nil {
			markUpgradeBlocked(hcu)
			hcu.Status.Message = fmt.Sprintf("Failed to configure release image on HostedCluster: %v", err)
			metricFailedTotal.WithLabelValues(hcu.Namespace, hcu.Name, currentVer, targetVer, "HostedClusterSpecUpdateFailed").Inc()
			break
		}

		err = r.Update(ctx, uHC)
		if err != nil {
			return ctrl.Result{}, err
		}

		if previousImage != nextReleaseImage && (hcu.Status.ControlPlaneStatus.StartTime == nil || hcu.Status.ControlPlaneStatus.TargetReleaseImage != nextReleaseImage) {
			now := metav1.Now()
			hcu.Status.ControlPlaneStatus.StartTime = &now
		} else if hcu.Status.ControlPlaneStatus.TargetReleaseImage != nextReleaseImage {
			// An externally started rollout has no known actuation timestamp.
			hcu.Status.ControlPlaneStatus.StartTime = nil
		}
		hcu.Status.ControlPlaneStatus.Step = stepLabel
		hcu.Status.ControlPlaneStatus.TargetReleaseImage = nextReleaseImage
		hcu.Status.Phase = upgradev1alpha1.PhaseVerifyingControlPlane
		hcu.Status.Message = fmt.Sprintf("Initiated %s Control Plane rollout. Verifying component readiness.", stepLabel)

	case upgradev1alpha1.PhaseVerifyingControlPlane:
		expectedRelease := hcu.Status.ControlPlaneStatus.TargetReleaseImage
		if expectedRelease == "" {
			expectedRelease = hcu.Spec.TargetReleaseImage
			hcu.Status.ControlPlaneStatus.TargetReleaseImage = expectedRelease
		}
		expectedVer, _ := upgradev1alpha1.ExtractVersion(expectedRelease)
		targetVer, _ := upgradev1alpha1.ExtractVersion(hcu.Spec.TargetReleaseImage)
		_, completedVer := GetHostedClusterCurrentRelease(uHC)
		if upgradev1alpha1.IsUpstreamGreater(expectedVer, completedVer) {
			if completedVer != targetVer {
				hcu.Status.Phase = upgradev1alpha1.PhaseUpgradingControlPlane
				hcu.Status.Message = "Saved control plane hop already passed. Re-evaluating the remaining upgrade path."
				break
			}
			expectedRelease = hcu.Spec.TargetReleaseImage
			expectedVer = targetVer
			hcu.Status.ControlPlaneStatus.TargetReleaseImage = expectedRelease
			hcu.Status.ControlPlaneStatus.StartTime = nil
		}

		// Re-aggregate CPC status filtering by expectedVer
		if err := r.aggregateControlPlaneStatus(ctx, hcu, expectedVer); err != nil {
			reqLogger.Error(err, "Failed to aggregate ControlPlaneComponent status")
		}

		// Re-verify cluster health
		isInstalling, healthErr := r.checkClusterInstallingAndHealth(uHC)
		if healthErr != nil {
			markUpgradeBlocked(hcu)
			hcu.Status.Message = fmt.Sprintf("Control Plane Rollout Blocked: %v", healthErr)
			metricFailedTotal.WithLabelValues(hcu.Namespace, hcu.Name, "", expectedVer, "ControlPlaneRolloutFailed").Inc()
			break
		}
		if isInstalling {
			hcu.Status.Phase = upgradev1alpha1.PhaseInstalling
			hcu.Status.Message = "HostedCluster is currently in initial installation. Waiting for completion."
			break
		}

		if err := r.verifyControlPlane(ctx, hcu, uHC, expectedRelease); err != nil {
			hcu.Status.Message = err.Error()
			stateRequeueAfter = 10 * time.Second
			break
		}

		// 2. Check ControlPlaneComponent readiness on expectedVer
		if hcu.Status.ControlPlaneStatus.TotalComponents == 0 || hcu.Status.ControlPlaneStatus.UpgradedComponents < hcu.Status.ControlPlaneStatus.TotalComponents {
			hcu.Status.Message = fmt.Sprintf("Waiting for Control Plane components to complete rollout to %s (%d/%d ready).", expectedVer, hcu.Status.ControlPlaneStatus.UpgradedComponents, hcu.Status.ControlPlaneStatus.TotalComponents)
			stateRequeueAfter = 10 * time.Second
			break
		}

		// Control plane rollout to expectedVer is 100% verified!
		hcu.Status.ControlPlaneStatus.StartTime = nil
		if expectedVer != targetVer {
			// Intermediate hop verified. Advance to target release image!
			hcu.Status.Phase = upgradev1alpha1.PhaseUpgradingControlPlane
			hcu.Status.Message = fmt.Sprintf("Intermediate CP upgrade to %s verified. Proceeding with next upgrade jump.", expectedVer)
		} else {
			// Target control plane rollout completely verified!
			hcu.Status.Phase = upgradev1alpha1.PhaseUpgradingNodePools
			hcu.Status.Message = "Hosted Control Plane upgrade successfully verified. Initiating sequential NodePool rollouts."
		}

	case upgradev1alpha1.PhaseUpgradingNodePools:
		// Execute sequential NodePool Rollouts based on Priority Label Groups
		requeue, dur, err := r.reconcileNodePools(ctx, hcu, uHC)
		if err != nil {
			reqLogger.Error(err, "Failed to reconcile NodePools")
			markUpgradeBlocked(hcu)
			hcu.Status.Message = fmt.Sprintf("NodePool upgrade blocked: %v", err)
			metricFailedTotal.WithLabelValues(hcu.Namespace, hcu.Name, "", "", "NodePoolRolloutFailed").Inc()
			break
		}

		if requeue {
			stateRequeueAfter = dur
		}

	case upgradev1alpha1.PhaseCompleted:
		hcu.Status.Message = "All upgrade steps successfully verified and completed."
		if hcu.Status.CompletionTime == nil {
			now := metav1.Now()
			hcu.Status.CompletionTime = &now
		}

	}
	if hcu.Status.Phase == upgradev1alpha1.PhaseBlocked {
		stateRequeueAfter = 30 * time.Second
	} else {
		hcu.Status.PhaseBeforeBlock = ""
	}

	// Update GitOps standard conditions
	r.updateConditions(hcu)

	// Expose standard phase, overall duration and step-level duration metrics
	r.exposePhaseMetrics(ctx, hcu, uHC)

	// Update status if any field changed
	if !reflect.DeepEqual(originalStatus, &hcu.Status) {
		err = r.Status().Update(ctx, hcu)
		if err != nil {
			return ctrl.Result{}, err
		}
	}

	// Elapsed gauges and log observations cannot rely on status events during a stall.
	if hcu.Status.Phase != upgradev1alpha1.PhaseCompleted && (stateRequeueAfter <= 0 || stateRequeueAfter > 30*time.Second) {
		stateRequeueAfter = 30 * time.Second
	}
	return ctrl.Result{RequeueAfter: stateRequeueAfter}, nil
}

// updateConditions populates standard Kubernetes / GitOps conditions (Ready, Progressing, Failing)
func (r *HostedClusterUpgradeReconciler) updateConditions(hcu *upgradev1alpha1.HostedClusterUpgrade) {
	hcu.Status.ObservedGeneration = hcu.Generation
	now := metav1.Now()
	// Ready Condition
	readyCond := metav1.Condition{
		Type:               "Ready",
		ObservedGeneration: hcu.Generation,
		Status:             metav1.ConditionFalse,
		Reason:             string(hcu.Status.Phase),
		Message:            hcu.Status.Message,
		LastTransitionTime: now,
	}
	if hcu.Status.Phase == upgradev1alpha1.PhaseCompleted {
		readyCond.Status = metav1.ConditionTrue
		readyCond.Reason = "UpgradeCompleted"
	}

	// Progressing Condition
	progressingCond := metav1.Condition{
		Type:               "Progressing",
		ObservedGeneration: hcu.Generation,
		Status:             metav1.ConditionFalse,
		Reason:             string(hcu.Status.Phase),
		Message:            hcu.Status.Message,
		LastTransitionTime: now,
	}
	switch hcu.Status.Phase {
	case upgradev1alpha1.PhaseUpgradingControlPlane,
		upgradev1alpha1.PhaseVerifyingControlPlane,
		upgradev1alpha1.PhaseUpgradingNodePools,
		upgradev1alpha1.PhasePendingSchedule,
		upgradev1alpha1.PhaseInstalling,
		upgradev1alpha1.PhasePreflightChecks:
		progressingCond.Status = metav1.ConditionTrue
		progressingCond.Reason = "UpgradeProgressing"
	}

	// Failing Condition
	failingCond := metav1.Condition{
		Type:               "Failing",
		ObservedGeneration: hcu.Generation,
		Status:             metav1.ConditionFalse,
		Reason:             string(hcu.Status.Phase),
		Message:            hcu.Status.Message,
		LastTransitionTime: now,
	}
	if hcu.Status.Phase == upgradev1alpha1.PhaseBlocked {
		failingCond.Status = metav1.ConditionTrue
		failingCond.Reason = "UpgradeBlocked"
	}

	meta.SetStatusCondition(&hcu.Status.Conditions, readyCond)
	meta.SetStatusCondition(&hcu.Status.Conditions, progressingCond)
	meta.SetStatusCondition(&hcu.Status.Conditions, failingCond)
}

const (
	workerLocksFinalizer = "upgrade.huc.0xshift.dev/worker-locks"
	workerLockOwner      = "upgrade.huc.0xshift.dev/worker-lock-owner"
	workerLockOriginal   = "upgrade.huc.0xshift.dev/worker-lock-original-pause"
)

// nodePoolPause reads the effective user pause, not the temporary coordinator lock.
// JSON null preserves absence separately from an explicitly empty string.
func nodePoolPause(np *unstructured.Unstructured, hcu *upgradev1alpha1.HostedClusterUpgrade) (*string, bool, error) {
	value, found, err := unstructured.NestedString(np.Object, "spec", "pausedUntil")
	if err != nil {
		return nil, false, err
	}
	var pause *string
	if found {
		pause = &value
	}
	annotations := np.GetAnnotations()
	owner, locked := annotations[workerLockOwner]
	if !locked {
		return pause, false, nil
	}
	if owner != string(hcu.UID) {
		return pause, false, fmt.Errorf("NodePool %q has a foreign coordinator lock owned by %q", np.GetName(), owner)
	}
	if value != "true" {
		// A user replaced our lock. Never restore over their new value.
		return pause, true, nil
	}
	if err := json.Unmarshal([]byte(annotations[workerLockOriginal]), &pause); err != nil {
		return nil, false, fmt.Errorf("NodePool %q has invalid original pause: %w", np.GetName(), err)
	}
	return pause, true, nil
}

// restoreNodePoolLock only modifies the in-memory object; callers persist it with
// the release change in one optimistic-concurrency-protected update.
func restoreNodePoolLock(np *unstructured.Unstructured, hcu *upgradev1alpha1.HostedClusterUpgrade) (bool, error) {
	pause, owned, err := nodePoolPause(np, hcu)
	if err != nil || !owned {
		return false, err
	}
	if pause == nil {
		unstructured.RemoveNestedField(np.Object, "spec", "pausedUntil")
	} else {
		_ = unstructured.SetNestedField(np.Object, *pause, "spec", "pausedUntil")
	}
	annotations := np.GetAnnotations()
	delete(annotations, workerLockOwner)
	delete(annotations, workerLockOriginal)
	delete(annotations, workerLockGeneration)
	delete(annotations, workerLockConfig)
	np.SetAnnotations(annotations)
	return true, nil
}

// lockClusterNodePools pauses all NodePools belonging to the cluster during control plane transitions.
func (r *HostedClusterUpgradeReconciler) lockClusterNodePools(ctx context.Context, hcu *upgradev1alpha1.HostedClusterUpgrade) error {
	uNPList, err := r.listClusterNodePools(ctx, hcu.Namespace, hcu.Name)
	if err != nil {
		return err
	}

	for _, item := range uNPList.Items {
		clusterRef, found, _ := unstructured.NestedString(item.Object, "spec", "clusterName")
		if found && clusterRef == hcu.Name {
			pause, owned, err := nodePoolPause(&item, hcu)
			if err != nil {
				return err
			}
			if owned {
				current, _, _ := unstructured.NestedString(item.Object, "spec", "pausedUntil")
				if current != "true" {
					return fmt.Errorf("NodePool %q coordinator lock was replaced", item.GetName())
				}
				continue
			}
			if pause != nil && strings.EqualFold(strings.TrimSpace(*pause), "true") {
				continue
			}
			if _, _, err := checkPausedState(pause); err != nil {
				return err
			}
			if err := r.acquireWorkerLock(ctx, hcu, &item, pause); err != nil {
				return err
			}
		}
	}
	return nil
}

// clearUpgradeGauges leaves historical failure counters intact.
func clearUpgradeGauges(namespace, name string) {
	labels := prometheus.Labels{"namespace": namespace, "name": name}
	for _, gauge := range []*prometheus.GaugeVec{
		metricPhase, metricDuration, metricStepDuration, metricComponentReady,
		metricNodePoolBlocked, metricNodePoolRolloutRatio, metricAggregatedNodeRolloutRatio,
		metricDrainAvailable, metricDrainObservations,
	} {
		gauge.DeletePartialMatch(labels)
	}
}

// exposePhaseMetrics populates core SRE gauge values from the resulting status.
func (r *HostedClusterUpgradeReconciler) exposePhaseMetrics(ctx context.Context, hcu *upgradev1alpha1.HostedClusterUpgrade, uHC *unstructured.Unstructured) {
	exposeDrainMetrics(hcu)
	labels := prometheus.Labels{"namespace": hcu.Namespace, "name": hcu.Name}
	for _, gauge := range []*prometheus.GaugeVec{metricPhase, metricDuration, metricStepDuration} {
		gauge.DeletePartialMatch(labels)
	}
	var phaseVal float64
	switch hcu.Status.Phase {
	case upgradev1alpha1.PhaseInstalling:
		phaseVal = 1
	case upgradev1alpha1.PhasePreflightChecks, upgradev1alpha1.PhasePendingSchedule:
		phaseVal = 2
	case upgradev1alpha1.PhaseUpgradingControlPlane:
		phaseVal = 3
	case upgradev1alpha1.PhaseVerifyingControlPlane:
		phaseVal = 4
	case upgradev1alpha1.PhaseUpgradingNodePools:
		phaseVal = 5
	case upgradev1alpha1.PhaseCompleted:
		phaseVal = 6
	case upgradev1alpha1.PhaseBlocked:
		phaseVal = 7
	case upgradev1alpha1.PhasePaused:
		phaseVal = 8
	}
	currentVer := hcu.Status.InitialVersion
	if currentVer == "" && uHC != nil {
		_, currentVer = GetHostedClusterCurrentRelease(uHC)
	}
	if currentVer == "" {
		currentVer = "unknown"
	}
	targetVer, _ := upgradev1alpha1.ExtractVersion(hcu.Spec.TargetReleaseImage)
	// Read through the informer-backed client so deletion and version changes also
	// refresh worker gauges while the coordinator is paused or already completed.
	if pools, err := r.listClusterNodePools(ctx, hcu.Namespace, hcu.Name); err == nil {
		metricNodePoolBlocked.DeletePartialMatch(labels)
		metricNodePoolRolloutRatio.DeletePartialMatch(labels)
		metricAggregatedNodeRolloutRatio.DeletePartialMatch(labels)
		livePools := make(map[string]bool)
		var total, updated, ready int32
		for _, np := range pools.Items {
			cluster, _, _ := unstructured.NestedString(np.Object, "spec", "clusterName")
			if cluster != hcu.Name {
				continue
			}
			livePools[np.GetName()] = true
			var blocked float64
			for _, status := range hcu.Status.NodePoolStatuses {
				if status.Name == np.GetName() && status.TargetReleaseImage == hcu.Spec.TargetReleaseImage && len(status.Blockers) > 0 {
					blocked = 1
				}
			}
			metricNodePoolBlocked.WithLabelValues(hcu.Namespace, hcu.Name, np.GetName(), currentVer, targetVer).Set(blocked)
			assessment, assessmentErr := r.assessWorker(ctx, &np, hcu.Name, hcu.Spec.TargetReleaseImage)
			replicas, upgraded, readyNodes := assessment.total, assessment.updated, assessment.ready
			total += replicas
			updated += upgraded
			ready += readyNodes
			if hcu.Status.Phase == upgradev1alpha1.PhasePaused {
				for i := range hcu.Status.NodePoolStatuses {
					status := &hcu.Status.NodePoolStatuses[i]
					if status.Name != np.GetName() {
						continue
					}
					status.Nodes = upgradev1alpha1.NodeStatusSummary{Total: replicas, Updated: upgraded, Ready: readyNodes}
					if assessmentErr == nil && assessment.complete {
						status.Phase = "Completed"
						status.StartTime = nil
						status.Blockers = nil
						metricNodePoolBlocked.WithLabelValues(hcu.Namespace, hcu.Name, np.GetName(), currentVer, targetVer).Set(0)
					}
				}
			}
			var ratio float64
			if replicas > 0 {
				ratio = float64(upgraded) / float64(replicas)
			}
			metricNodePoolRolloutRatio.WithLabelValues(hcu.Namespace, hcu.Name, np.GetName()).Set(ratio)
		}
		var ratio float64
		if total > 0 {
			ratio = float64(updated) / float64(total)
		}
		metricAggregatedNodeRolloutRatio.WithLabelValues(hcu.Namespace, hcu.Name).Set(ratio)
		if hcu.Status.Phase == upgradev1alpha1.PhasePaused {
			hcu.Status.AggregatedNodeStatus = upgradev1alpha1.NodeStatusSummary{Total: total, Updated: updated, Ready: ready}
		}
		statuses := hcu.Status.NodePoolStatuses[:0]
		for _, status := range hcu.Status.NodePoolStatuses {
			if livePools[status.Name] {
				statuses = append(statuses, status)
			}
		}
		hcu.Status.NodePoolStatuses = statuses
	}

	var intermediateVer string
	if hcu.Spec.IntermediateReleaseImage != "" {
		intermediateVer, _ = upgradev1alpha1.ExtractVersion(hcu.Spec.IntermediateReleaseImage)
	}

	metricPhase.WithLabelValues(hcu.Namespace, hcu.Name, currentVer, targetVer, intermediateVer).Set(phaseVal)

	if phaseVal == 6.0 || phaseVal == 0.0 { // Completed or Idle
		metricDuration.WithLabelValues(hcu.Namespace, hcu.Name, currentVer, targetVer, intermediateVer).Set(0)
	} else if hcu.Status.StartTime != nil {
		duration := time.Since(hcu.Status.StartTime.Time).Seconds()
		metricDuration.WithLabelValues(hcu.Namespace, hcu.Name, currentVer, targetVer, intermediateVer).Set(duration)
	}
	if hcu.Status.Phase == upgradev1alpha1.PhaseCompleted {
		return
	}
	cp := hcu.Status.ControlPlaneStatus
	if start := cp.StartTime; start != nil && (cp.TargetReleaseImage == hcu.Spec.TargetReleaseImage || cp.TargetReleaseImage == hcu.Spec.IntermediateReleaseImage) {
		metricStepDuration.WithLabelValues(hcu.Namespace, hcu.Name, "ControlPlane", currentVer, targetVer).Set(time.Since(start.Time).Seconds())
	}
	for _, np := range hcu.Status.NodePoolStatuses {
		if np.StartTime != nil && np.Phase != "Completed" && np.TargetReleaseImage == hcu.Spec.TargetReleaseImage {
			metricStepDuration.WithLabelValues(hcu.Namespace, hcu.Name, "NodePool-"+np.Name, currentVer, targetVer).Set(time.Since(np.StartTime.Time).Seconds())
		}
	}
}

// reconcileNodePools fetches all associated NodePools, clusters them into priority groups, and triggers upgrades.
func (r *HostedClusterUpgradeReconciler) reconcileNodePools(ctx context.Context, hcu *upgradev1alpha1.HostedClusterUpgrade, uHC *unstructured.Unstructured) (bool, time.Duration, error) {
	reqLogger := log.FromContext(ctx)

	// Fetch associated NodePools in stable name order through the cache index.
	uNPList, err := r.listClusterNodePools(ctx, hcu.Namespace, hcu.Name)
	if err != nil {
		return false, 0, err
	}

	// 2. Filter NodePools belonging to our cluster
	clusterNodePools := []unstructured.Unstructured{}
	for _, item := range uNPList.Items {
		clusterRef, found, _ := unstructured.NestedString(item.Object, "spec", "clusterName")
		if found && clusterRef == hcu.Name {
			clusterNodePools = append(clusterNodePools, item)
		}
	}

	reqLogger.Info("Retrieved associated NodePools", "totalCount", len(clusterNodePools))

	if len(clusterNodePools) == 0 {
		hcu.Status.DrainDiagnostics = nil
		if err := r.verifyControlPlane(ctx, hcu, uHC, hcu.Spec.TargetReleaseImage); err != nil {
			hcu.Status.Message = err.Error()
			return true, 10 * time.Second, nil
		}
		hcu.Status.NodePoolStatuses = nil
		hcu.Status.AggregatedNodeStatus = upgradev1alpha1.NodeStatusSummary{}
		// No node pools associated with this cluster. Transition directly to Completed!
		hcu.Status.Phase = upgradev1alpha1.PhaseCompleted
		hcu.Status.Message = "No NodePools associated with this HostedCluster. Upgrade complete."
		return false, 0, nil
	}

	// Calculate and assign root-level AggregatedNodeStatus
	var totalReplicasSum int32
	var updatedReplicasSum int32
	var readyReplicasSum int32

	targetReleaseImage := hcu.Spec.TargetReleaseImage
	targetVer, _ := upgradev1alpha1.ExtractVersion(targetReleaseImage)

	assessments := make(map[string]workerAssessment, len(clusterNodePools))
	for _, np := range clusterNodePools {
		a, err := r.assessWorker(ctx, &np, hcu.Name, targetReleaseImage)
		if err != nil {
			return false, 0, err
		}
		assessments[np.GetName()] = a
		totalReplicasSum += a.total
		updatedReplicasSum += a.updated
		readyReplicasSum += a.ready
	}

	hcu.Status.AggregatedNodeStatus = upgradev1alpha1.NodeStatusSummary{
		Total:   totalReplicasSum,
		Updated: updatedReplicasSum,
		Ready:   readyReplicasSum,
	}

	// 3. Build group execution sequence and map schedules
	var groupSequence []string
	hasDefault := false
	knownGroups := map[string]bool{"default": true}
	seenGroups := make(map[string]bool)
	for _, g := range hcu.Spec.NodePoolGroups {
		if g.Name == "" || seenGroups[g.Name] {
			return false, 0, fmt.Errorf("NodePool group names must be nonempty and unique: %q", g.Name)
		}
		seenGroups[g.Name] = true
		knownGroups[g.Name] = true
		groupSequence = append(groupSequence, g.Name)
		if g.Name == "default" {
			hasDefault = true
		}
	}
	if !hasDefault {
		groupSequence = append(groupSequence, "default")
	}

	groupSchedules := make(map[string]*time.Time)
	for _, g := range hcu.Spec.NodePoolGroups {
		if g.StartAt != nil {
			t := g.StartAt.Time
			groupSchedules[g.Name] = &t
		}
	}

	// 4. Cluster NodePools by label group (defaulting to "default" if missing)
	type npContext struct {
		object      unstructured.Unstructured
		name        string
		group       string
		startAt     *time.Time
		pausedUntil string
		paused      bool
		pauseDelay  time.Duration
		pauseErr    error
		ownedLock   bool
		version     string
	}

	groupPoolsMap := make(map[string][]npContext)

	for _, np := range clusterNodePools {
		name := np.GetName()
		labels := np.GetLabels()
		groupName := "default"
		if labels != nil && labels["upgrade.huc.0xshift.dev/group"] != "" {
			groupName = labels["upgrade.huc.0xshift.dev/group"]
		}
		if !knownGroups[groupName] {
			return false, 0, fmt.Errorf("NodePool %q references unknown group %q; add it to spec.nodePoolGroups or change the pool label", name, groupName)
		}

		pUntil, _, _ := unstructured.NestedString(np.Object, "spec", "pausedUntil")
		userPause, ownedLock, pauseErr := nodePoolPause(&np, hcu)
		paused, pauseDelay, parseErr := checkPausedState(userPause)
		if pauseErr == nil {
			pauseErr = parseErr
		}
		version, _, _ := unstructured.NestedString(np.Object, "status", "version")

		startAt := groupSchedules[groupName]

		c := npContext{
			object:      np,
			name:        name,
			group:       groupName,
			startAt:     startAt,
			pausedUntil: pUntil,
			paused:      paused,
			pauseDelay:  pauseDelay,
			pauseErr:    pauseErr,
			ownedLock:   ownedLock,
			version:     version,
		}

		reqLogger.Info("Parsed NodePool schema info", "name", name, "group", groupName, "pausedUntil", pUntil, "version", version)
		groupPoolsMap[groupName] = append(groupPoolsMap[groupName], c)
	}
	// Enroll newly observed pools before releasing any group. An already running
	// external rollout is not cancelled by writing a lock, and must not be queued
	// as if it were idle. Unknown evidence is conservatively outstanding work.
	externalWork := make(map[string]bool)
	cpPrerequisiteErr := r.verifyControlPlane(ctx, hcu, uHC, targetReleaseImage)
	for group, pools := range groupPoolsMap {
		for i := range pools {
			np := &pools[i]
			started := false
			for _, previous := range hcu.Status.NodePoolStatuses {
				if previous.Name == np.name && previous.Phase != "Completed" && (previous.StartTime != nil || previous.Phase == "Upgrading") {
					started = true
				}
			}
			if started || assessments[np.name].complete {
				continue
			}
			if np.pauseErr != nil {
				externalWork[np.name] = true
				continue
			}
			q, err := r.assessWorkerQuiescence(ctx, hcu, &np.object)
			if err != nil {
				return false, 0, err
			}
			if !q.complete {
				externalWork[np.name] = true
				continue
			}
			if cpPrerequisiteErr == nil && !np.ownedLock && !strings.EqualFold(strings.TrimSpace(np.pausedUntil), "true") {
				pause, _, _ := nodePoolPause(&np.object, hcu)
				if err := r.acquireWorkerLock(ctx, hcu, &np.object, pause); err != nil {
					return false, 0, err
				}
				np.ownedLock, np.pausedUntil = true, "true"
			}
		}
		groupPoolsMap[group] = pools
	}

	hcuStatuses := make([]upgradev1alpha1.NodePoolUpgradeStatus, len(clusterNodePools))
	previousStatuses := hcu.Status.NodePoolStatuses
	i := 0
	for _, group := range groupSequence {
		for _, np := range groupPoolsMap[group] {
			hcuStatuses[i].Name = np.name
			for _, previous := range previousStatuses {
				if previous.Name == np.name {
					hcuStatuses[i] = previous
					break
				}
			}
			i++
		}
	}
	// Keep successful actuation timestamps even if a later pool update fails.
	hcu.Status.NodePoolStatuses = hcuStatuses
	idx := 0

	concurrency := 1
	if hcu.Spec.RolloutPolicy != nil && hcu.Spec.RolloutPolicy.MaxParallelNodePools > 0 {
		concurrency = int(hcu.Spec.RolloutPolicy.MaxParallelNodePools)
	}
	// A pause stops reconciliation, not the upstream rollout. Reserve slots for
	// all unfinished work, including older plans and pools outside the active group.
	reserved := make(map[string]bool)
	oldWork := make(map[string]bool)
	activeUpgrades := 0
	for _, pools := range groupPoolsMap {
		for _, np := range pools {
			done := assessments[np.name].complete
			started := false
			image, _, _ := unstructured.NestedString(np.object.Object, "spec", "release", "image")
			for _, previous := range previousStatuses {
				if previous.Name != np.name || previous.Phase == "Completed" {
					continue
				}
				started = previous.StartTime != nil || previous.Phase == "Upgrading"
				if started && previous.TargetReleaseImage != "" && (previous.TargetReleaseImage != targetReleaseImage || image != targetReleaseImage) {
					a, err := r.assessWorkerQuiescence(ctx, hcu, &np.object)
					if err != nil {
						return false, 0, err
					}
					done = a.complete || assessments[np.name].complete
					oldWork[np.name] = !done
				}
			}
			if !done && (started || externalWork[np.name] || (image == targetReleaseImage && !np.ownedLock)) {
				reserved[np.name] = true
				activeUpgrades++
			}
		}
	}
	hasOldWork := false
	for _, outstanding := range oldWork {
		hasOldWork = hasOldWork || outstanding
	}
	hasExternalWork := len(externalWork) > 0

	groupUpgradeTriggered := false
	pauseBlocked := false
	var shortestRequeue time.Duration
	allCompleted := true
	priorGroupsCompleted := true

	for _, grpName := range groupSequence {
		pools := groupPoolsMap[grpName]
		if len(pools) == 0 {
			continue // No NodePools assigned to this group in active cluster
		}

		groupCompleted := true

		reqLogger.Info("Processing NodePool group", "group", grpName, "concurrency", concurrency, "activeUpgrades", activeUpgrades, "poolsCount", len(pools))

		for _, np := range pools {
			startTime := hcuStatuses[idx].StartTime
			statusTarget := targetReleaseImage
			if externalWork[np.name] {
				statusTarget, _, _ = unstructured.NestedString(np.object.Object, "spec", "release", "image")
			}
			if oldWork[np.name] {
				statusTarget = hcuStatuses[idx].TargetReleaseImage
			}
			if !oldWork[np.name] && hcuStatuses[idx].TargetReleaseImage != targetReleaseImage {
				startTime = nil
			}
			assessment := assessments[np.name]
			replicas, updatedReplicas, readyReplicas := assessment.total, assessment.updated, assessment.ready

			npSpecRelease, _, _ := unstructured.NestedString(np.object.Object, "spec", "release", "image")
			var npVer string
			if np.version != "" {
				npVer, _ = upgradev1alpha1.ExtractVersion(np.version)
			} else if npSpecRelease != "" {
				npVer, _ = upgradev1alpha1.ExtractVersion(npSpecRelease)
			}

			var phase string
			var blockers []string

			// Re-run preflight checks immediately before triggering upgrades
			preflightErr := r.verifyControlPlane(ctx, hcu, uHC, targetReleaseImage)

			reqLogger.Info("Evaluating NodePool specs", "name", np.name, "group", grpName, "npVer", npVer, "targetVer", targetVer, "pausedUntil", np.pausedUntil)

			// Completion requires the expected version and every desired node updated and ready.
			isUpgraded := assessment.complete

			if np.pauseErr != nil {
				pauseBlocked = true
				phase = "Blocked"
				blockers = append(blockers, np.pauseErr.Error())
				groupCompleted = false
			} else if externalWork[np.name] {
				phase = "Blocked"
				blockers = append(blockers, "Externally started or unproven worker rollout is outstanding; waiting for positive quiescence")
				groupCompleted = false
			} else if oldWork[np.name] {
				phase = "Blocked"
				blockers = append(blockers, "Previous worker target still outstanding; waiting for authoritative completion before changing plans")
				groupCompleted = false
			} else if isUpgraded {
				if changed, err := restoreNodePoolLock(&np.object, hcu); err != nil {
					return false, 0, err
				} else if changed {
					if err := r.Update(ctx, &np.object); err != nil {
						return false, 0, err
					}
				}
				startTime = nil
				phase = "Completed"
				reqLogger.Info("NodePool upgrade already complete", "name", np.name)
			} else if np.paused {
				pauseBlocked = true
				phase = "Blocked"
				groupCompleted = false
				message := "NodePool independently paused indefinitely"
				if np.pauseDelay > 0 {
					message = fmt.Sprintf("NodePool independently paused; resumes in %s", np.pauseDelay.Round(time.Second))
					if shortestRequeue == 0 || np.pauseDelay < shortestRequeue {
						shortestRequeue = np.pauseDelay
					}
				}
				blockers = append(blockers, message)
			} else if preflightErr != nil {
				phase = "Blocked"
				blockers = append(blockers, fmt.Sprintf("Cluster health degraded: %v", preflightErr))
				groupCompleted = false
			} else if !priorGroupsCompleted {
				// Prior group is not yet completed. Hold queue.
				phase = "WaitingForGroup"
				groupCompleted = false
			} else {
				// Evaluate StartAt schedule
				now := time.Now()
				if np.startAt != nil && now.Before(*np.startAt) {
					phase = "Scheduled"
					groupCompleted = false

					requeueDur := np.startAt.Sub(now)
					if shortestRequeue == 0 || requeueDur < shortestRequeue {
						shortestRequeue = requeueDur
					}
				} else {
					// Schedule is open. Trigger Upgrade (subject to concurrency limits or existing in-flight upgrades)
					isAlreadyUpgrading := reserved[np.name] && npSpecRelease == targetReleaseImage

					if isAlreadyUpgrading || (!hasOldWork && !hasExternalWork && activeUpgrades < concurrency) {
						phase = "Upgrading"
						groupCompleted = false

						reqLogger.Info("Diagnostic compare values", "name", np.name, "npSpecRelease", npSpecRelease, "targetRelease", targetReleaseImage)

						if npSpecRelease != targetReleaseImage || np.ownedLock {
							actuated := npSpecRelease != targetReleaseImage || np.pausedUntil == "true"
							reqLogger.Info("Triggering upgrade rollout on NodePool", "name", np.name, "targetImage", targetReleaseImage)
							_ = unstructured.SetNestedField(np.object.Object, targetReleaseImage, "spec", "release", "image")
							if _, err := restoreNodePoolLock(&np.object, hcu); err != nil {
								return false, 0, err
							}
							err = r.Update(ctx, &np.object)
							if err != nil {
								return false, 0, err
							}
							if actuated {
								groupUpgradeTriggered = true
								if !reserved[np.name] {
									activeUpgrades++
									reserved[np.name] = true
								}
								if startTime == nil {
									now := metav1.Now()
									startTime = &now
								}
							}
						}

						if assessment.reason != "" {
							blockers = append(blockers, assessment.reason)
						}
					} else {
						// Concurrency throttled. Wait in queue.
						phase = "WaitingForConcurrency"
						groupCompleted = false
					}
				}
			}

			var schedStart *metav1.Time
			if np.startAt != nil {
				t := metav1.NewTime(*np.startAt)
				schedStart = &t
			}

			hcuStatuses[idx] = upgradev1alpha1.NodePoolUpgradeStatus{
				StartTime:          startTime,
				Name:               np.name,
				Group:              grpName,
				TargetReleaseImage: statusTarget,
				ScheduledStart:     schedStart,
				Phase:              phase,
				Nodes: upgradev1alpha1.NodeStatusSummary{
					Total:   replicas,
					Updated: updatedReplicas,
					Ready:   readyReplicas,
				},
				Blockers: blockers,
			}
			idx++
		}

		if !groupCompleted {
			priorGroupsCompleted = false
			allCompleted = false
		}
	}

	hcu.Status.NodePoolStatuses = hcuStatuses
	// Diagnostics are best-effort and run only after rollout actions, once per cluster.
	r.refreshDrainDiagnostics(ctx, hcu)

	if allCompleted {
		// All groups successfully verified and upgraded! Transition state machine to Completed.
		hcu.Status.Phase = upgradev1alpha1.PhaseCompleted
		hcu.Status.Message = "All worker NodePools successfully upgraded and verified on target version."
		return false, 0, nil
	}

	// Update status message with active scheduler/rollout telemetry
	if shortestRequeue > 0 {
		hcu.Status.Message = fmt.Sprintf("Worker upgrades waiting. Next schedule or user pause expires in %s.", shortestRequeue.Round(time.Second).String())
		if shortestRequeue > 30*time.Second {
			shortestRequeue = 30 * time.Second
		}
		return true, shortestRequeue, nil
	}

	hcu.Status.Message = "Worker NodePool upgrades actively rolling out."
	if pauseBlocked {
		hcu.Status.Message = "Worker NodePool upgrades blocked by independent pauses or lock ownership; see NodePool blockers."
	}
	if groupUpgradeTriggered {
		return true, time.Second * 5, nil // Requeue quickly to observe the unpause
	}

	return true, 30 * time.Second, nil
}

// checkPausedState evaluates if the coordinator is paused (indefinitely or timed).
func checkPausedState(pausedUntil *string) (bool, time.Duration, error) {
	if pausedUntil == nil || *pausedUntil == "" {
		return false, 0, nil
	}

	pStr := strings.TrimSpace(*pausedUntil)
	if strings.EqualFold(pStr, "true") {
		return true, 0, nil
	}
	if strings.EqualFold(pStr, "false") {
		return false, 0, nil
	}

	// Try to parse RFC3339 timestamp
	t, err := time.Parse(time.RFC3339, pStr)
	if err != nil {
		return false, 0, fmt.Errorf("unable to parse RFC3339 timestamp '%s': %v", pStr, err)
	}

	now := time.Now()
	if t.After(now) {
		return true, t.Sub(now), nil
	}

	return false, 0, nil
}

// SetupWithManager sets up the controller with the Manager and registers our event-driven watches.
func (r *HostedClusterUpgradeReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &upgradev1alpha1.HostedClusterUpgrade{}, controlPlaneNamespaceField, func(obj client.Object) []string {
		return []string{WorkerControlPlaneNamespace(obj.GetNamespace(), obj.GetName())}
	}); err != nil {
		return err
	}
	uCPC := &unstructured.Unstructured{}
	uCPC.SetGroupVersionKind(gvkCPC)

	uHC := &unstructured.Unstructured{}
	uHC.SetGroupVersionKind(gvkHC)

	uNP := &unstructured.Unstructured{}
	uNP.SetGroupVersionKind(gvkNP)
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), uNP, nodePoolClusterField, nodePoolClusterIndex); err != nil {
		return err
	}
	uHCP := &unstructured.Unstructured{}
	uHCP.SetGroupVersionKind(gvkHCP)
	uMD := &unstructured.Unstructured{}
	uMD.SetGroupVersionKind(gvkMachineDeployment)
	uMS := &unstructured.Unstructured{}
	uMS.SetGroupVersionKind(gvkMachineSet)

	return ctrl.NewControllerManagedBy(mgr).
		For(&upgradev1alpha1.HostedClusterUpgrade{}).
		Watches(uHCP, handler.EnqueueRequestsFromMapFunc(r.mapCPCToHostedClusterUpgrade)).
		Watches(uMD, handler.EnqueueRequestsFromMapFunc(r.mapWorkerToHostedClusterUpgrade)).
		Watches(uMS, handler.EnqueueRequestsFromMapFunc(r.mapWorkerToHostedClusterUpgrade)).
		Watches(
			uCPC,
			handler.EnqueueRequestsFromMapFunc(r.mapCPCToHostedClusterUpgrade),
			builder.WithPredicates(statusChangePredicate),
		).
		Watches(
			uHC,
			handler.EnqueueRequestsFromMapFunc(r.mapHostedClusterToHostedClusterUpgrade),
		).
		Watches(
			uNP,
			handler.EnqueueRequestsFromMapFunc(r.mapNodePoolToHostedClusterUpgrade),
		).
		Complete(r)
}

// mapHostedClusterToHostedClusterUpgrade maps HostedCluster events to the parent HostedClusterUpgrade.
func (r *HostedClusterUpgradeReconciler) mapHostedClusterToHostedClusterUpgrade(ctx context.Context, obj client.Object) []reconcile.Request {
	if r.HostedClusterNamespace != "" && obj.GetNamespace() != r.HostedClusterNamespace {
		return nil
	}
	return []reconcile.Request{
		{
			NamespacedName: client.ObjectKey{
				Namespace: obj.GetNamespace(),
				Name:      obj.GetName(),
			},
		},
	}
}

// mapNodePoolToHostedClusterUpgrade maps NodePool events to the parent HostedClusterUpgrade using spec.clusterName.
func (r *HostedClusterUpgradeReconciler) mapNodePoolToHostedClusterUpgrade(ctx context.Context, obj client.Object) []reconcile.Request {
	if r.HostedClusterNamespace != "" && obj.GetNamespace() != r.HostedClusterNamespace {
		return nil
	}
	uObj, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return nil
	}
	clusterName, found, _ := unstructured.NestedString(uObj.Object, "spec", "clusterName")
	if !found || clusterName == "" {
		return nil
	}
	return []reconcile.Request{
		{
			NamespacedName: client.ObjectKey{
				Namespace: obj.GetNamespace(),
				Name:      clusterName,
			},
		},
	}
}

// mapCPCToHostedClusterUpgrade reverses the normalized namespace using actual upgrades.
func (r *HostedClusterUpgradeReconciler) mapCPCToHostedClusterUpgrade(ctx context.Context, obj client.Object) []reconcile.Request {
	upgrades := &upgradev1alpha1.HostedClusterUpgradeList{}
	opts := []client.ListOption{client.MatchingFields{controlPlaneNamespaceField: obj.GetNamespace()}}
	if r.HostedClusterNamespace != "" {
		opts = append(opts, client.InNamespace(r.HostedClusterNamespace))
	}
	if err := r.List(ctx, upgrades, opts...); err != nil {
		log.FromContext(ctx).Error(err, "Unable to route control-plane event", "namespace", obj.GetNamespace())
		return nil
	}
	requests := make([]reconcile.Request, 0, len(upgrades.Items))
	for _, upgrade := range upgrades.Items {
		requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&upgrade)})
	}
	return requests
}

// statusChangePredicate filters out non-status mutations on ControlPlaneComponent resources.
var statusChangePredicate = predicate.Funcs{
	UpdateFunc: func(e event.UpdateEvent) bool {
		oldObj, okOld := e.ObjectOld.(*unstructured.Unstructured)
		newObj, okNew := e.ObjectNew.(*unstructured.Unstructured)
		if !okOld || !okNew {
			return false
		}

		// Extract status structures
		oldStatus, foundOld, _ := unstructured.NestedFieldCopy(oldObj.Object, "status")
		newStatus, foundNew, _ := unstructured.NestedFieldCopy(newObj.Object, "status")
		if !foundOld || !foundNew {
			return true // Reconcile if missing to be safe
		}

		// Deep compare the status subresources
		return !reflect.DeepEqual(oldStatus, newStatus)
	},
	CreateFunc: func(e event.CreateEvent) bool {
		return true
	},
	DeleteFunc: func(e event.DeleteEvent) bool {
		return true
	},
}

// GetHostedClusterCurrentRelease extracts the current release image and semantic version from HostedCluster status or spec.
func GetHostedClusterCurrentRelease(uHC *unstructured.Unstructured) (string, string) {
	if uHC == nil {
		return "", ""
	}
	history, found, _ := unstructured.NestedSlice(uHC.Object, "status", "version", "history")
	if found && len(history) > 0 {
		for _, item := range history {
			hMap, ok := item.(map[string]interface{})
			if !ok {
				continue
			}
			state, _ := hMap["state"].(string)
			if state == "Completed" {
				img, _ := hMap["image"].(string)
				ver, _ := hMap["version"].(string)
				if img != "" || ver != "" {
					if ver == "" {
						ver, _ = upgradev1alpha1.ExtractVersion(img)
					}
					return img, ver
				}
			}
		}
		if hMap, ok := history[0].(map[string]interface{}); ok {
			img, _ := hMap["image"].(string)
			ver, _ := hMap["version"].(string)
			if img != "" || ver != "" {
				if ver == "" {
					ver, _ = upgradev1alpha1.ExtractVersion(img)
				}
				return img, ver
			}
		}
	}

	desiredImg, foundImg, _ := unstructured.NestedString(uHC.Object, "status", "version", "desired", "image")
	desiredVer, _, _ := unstructured.NestedString(uHC.Object, "status", "version", "desired", "version")
	if foundImg && desiredImg != "" {
		if desiredVer == "" {
			desiredVer, _ = upgradev1alpha1.ExtractVersion(desiredImg)
		}
		return desiredImg, desiredVer
	}

	specImg, _, _ := unstructured.NestedString(uHC.Object, "spec", "release", "image")
	specVer, _ := upgradev1alpha1.ExtractVersion(specImg)
	return specImg, specVer
}

// GetNodePoolNodeCounts returns observed population, target-version nodes and
// target-ready nodes. CAPI desired replicas must additionally gate completion.
func GetNodePoolNodeCounts(np *unstructured.Unstructured, targetReleaseImage string) (int32, int32, int32) {
	// HyperShift copies AvailableReplicas here, not the total machine population.
	totalReplicas, _, _ := unstructured.NestedInt64(np.Object, "status", "replicas")
	desiredReplicas, desired, _ := unstructured.NestedInt64(np.Object, "spec", "replicas")
	if desired && desiredReplicas > totalReplicas {
		totalReplicas = desiredReplicas
	}
	if !desired && totalReplicas == 0 {
		// Missing status is not evidence of an intentionally scaled-to-zero pool.
		totalReplicas = 1
	}

	targetVer, err := upgradev1alpha1.ExtractVersion(targetReleaseImage)

	var updatedSum int32
	var readySum int32
	var population int64

	nodeVersions, found, _ := unstructured.NestedSlice(np.Object, "status", "nodesInfo", "nodeVersions")
	if found {
		for _, item := range nodeVersions {
			vMap, ok := item.(map[string]interface{})
			if !ok {
				continue
			}
			ocpVer, _ := vMap["ocpVersion"].(string)
			rCount, _, _ := unstructured.NestedInt64(vMap, "readyNodeCount")
			uCount, _, _ := unstructured.NestedInt64(vMap, "unreadyNodeCount")

			population += rCount + uCount

			if err == nil && ocpVer != "" {
				extractedOcpVer, errExt := upgradev1alpha1.ExtractVersion(ocpVer)
				if errExt == nil && extractedOcpVer == targetVer {
					updatedSum += int32(rCount + uCount)
					readySum += int32(rCount)
				}
			}
		}
	}

	if population > totalReplicas {
		totalReplicas = population
	}
	return int32(totalReplicas), updatedSum, readySum
}

// checkClusterInstallingAndHealth checks if the cluster is currently installing or contains failing/degraded conditions.
func (r *HostedClusterUpgradeReconciler) checkClusterInstallingAndHealth(uHC *unstructured.Unstructured) (bool, error) {
	history, foundHist, _ := unstructured.NestedSlice(uHC.Object, "status", "version", "history")
	hasCompleted := false
	if foundHist && len(history) > 0 {
		for _, item := range history {
			hMap, ok := item.(map[string]interface{})
			if !ok {
				continue
			}
			state, _ := hMap["state"].(string)
			if state == "Completed" {
				hasCompleted = true
				break
			}
		}
	}
	if !hasCompleted {
		return true, nil // No completed versions yet, meaning we are still in installation
	}

	conds, foundConds, _ := unstructured.NestedSlice(uHC.Object, "status", "conditions")
	positive := map[string]bool{"Available": false, "ClusterVersionAvailable": false, "ClusterVersionSucceeding": false}
	if foundConds {
		for _, c := range conds {
			condMap, ok := c.(map[string]interface{})
			if !ok {
				continue
			}
			cType, _ := condMap["type"].(string)
			cStatus, _ := condMap["status"].(string)
			cMsg, _ := condMap["message"].(string)
			if _, required := positive[cType]; required {
				if cStatus != "True" {
					return false, fmt.Errorf("HostedCluster %s=%s: %s", cType, cStatus, cMsg)
				}
				positive[cType] = true
			}

			if cType == "Failing" && cStatus == "True" {
				return false, fmt.Errorf("HostedCluster contains failing condition: %s", cMsg)
			}
			if cType == "ClusterVersionFailing" && cStatus == "True" {
				return false, fmt.Errorf("HostedCluster ClusterVersion is Failing: %s", cMsg)
			}
			if cType == "Degraded" && cStatus == "True" {
				return false, fmt.Errorf("HostedCluster contains degraded condition: %s", cMsg)
			}
			if cType == "ValidHostedControlPlaneConfiguration" && cStatus == "False" {
				return false, fmt.Errorf("HostedCluster has invalid control plane configuration: %s", cMsg)
			}
		}
	}
	for _, typ := range []string{"Available", "ClusterVersionAvailable", "ClusterVersionSucceeding"} {
		if !positive[typ] {
			return false, fmt.Errorf("HostedCluster is missing positive %s readiness", typ)
		}
	}

	return false, nil
}

// aggregateControlPlaneStatus queries and copies the individual states of the ControlPlaneComponents.
func (r *HostedClusterUpgradeReconciler) aggregateControlPlaneStatus(ctx context.Context, hcu *upgradev1alpha1.HostedClusterUpgrade, expectedVer string) error {
	cpNamespace := WorkerControlPlaneNamespace(hcu.Namespace, hcu.Name)
	metricComponentReady.DeletePartialMatch(prometheus.Labels{"namespace": hcu.Namespace, "name": hcu.Name})

	uList := &unstructured.UnstructuredList{}
	uList.SetGroupVersionKind(gvkCPC)

	err := r.List(ctx, uList, client.InNamespace(cpNamespace))
	if err != nil {
		hcu.Status.ControlPlaneStatus.Components = []upgradev1alpha1.ControlPlaneComponentStatus{}
		hcu.Status.ControlPlaneStatus.TotalComponents = 0
		hcu.Status.ControlPlaneStatus.UpgradedComponents = 0
		return err
	}

	sort.Slice(uList.Items, func(i, j int) bool { return uList.Items[i].GetName() < uList.Items[j].GetName() })
	components := make([]upgradev1alpha1.ControlPlaneComponentStatus, len(uList.Items))
	hcp := &unstructured.Unstructured{}
	hcp.SetGroupVersionKind(gvkHCP)
	hcpErr := r.Get(ctx, client.ObjectKey{Namespace: cpNamespace, Name: hcu.Name}, hcp)
	var upgradedCount int32
	blockers := []string{}

	for i, item := range uList.Items {
		name := item.GetName()
		version, _, _ := unstructured.NestedString(item.Object, "status", "version")

		conds, found, _ := unstructured.NestedSlice(item.Object, "status", "conditions")
		available := false
		rolloutComplete := false
		var failureMsgs []string

		if found {
			for _, c := range conds {
				condMap, ok := c.(map[string]interface{})
				if !ok {
					continue
				}
				condType, _ := condMap["type"].(string)
				condStatus, _ := condMap["status"].(string)
				condMsg, _ := condMap["message"].(string)
				condReason, _ := condMap["reason"].(string)

				if condType == "Available" {
					if condStatus == "True" {
						available = true
					} else {
						msg := condMsg
						if msg == "" && condReason != "" {
							msg = fmt.Sprintf("Available=False (%s)", condReason)
						}
						if msg != "" {
							failureMsgs = append(failureMsgs, fmt.Sprintf("Unavailable: %s", msg))
						}
					}
				}

				if condType == "RolloutComplete" {
					if condStatus == "True" {
						rolloutComplete = true
					} else {
						msg := condMsg
						if msg == "" && condReason != "" {
							msg = fmt.Sprintf("RolloutIncomplete (%s)", condReason)
						}
						if msg != "" {
							failureMsgs = append(failureMsgs, fmt.Sprintf("RolloutIncomplete: %s", msg))
						}
					}
				}
			}
		}

		compMsg := strings.Join(failureMsgs, "; ")

		var versionMatches bool
		if expectedVer == "" {
			versionMatches = true
		} else {
			compVer, _ := upgradev1alpha1.ExtractVersion(version)
			versionMatches = (compVer == expectedVer)
		}

		observed, _, _ := unstructured.NestedInt64(item.Object, "status", "observedGeneration")
		fresh := hcpErr == nil && hcp.GetGeneration() > 0 && observed >= hcp.GetGeneration()
		if available && rolloutComplete && versionMatches && fresh {
			upgradedCount++
		} else {
			blockDetail := compMsg
			if !versionMatches && available && rolloutComplete {
				blockDetail = fmt.Sprintf("running version %s (waiting for target %s)", version, expectedVer)
			} else if blockDetail == "" {
				blockDetail = "rollout in progress or status conditions not reported"
			}
			blockers = append(blockers, fmt.Sprintf("Component '%s' is not ready: %s", name, blockDetail))
		}
		var readiness float64
		if available {
			readiness = 1
			if rolloutComplete && versionMatches && fresh {
				readiness = 2
			}
		}
		metricComponentReady.WithLabelValues(hcu.Namespace, hcu.Name, name).Set(readiness)

		components[i] = upgradev1alpha1.ControlPlaneComponentStatus{
			Name:            name,
			Version:         version,
			Available:       available,
			RolloutComplete: rolloutComplete,
			Message:         compMsg,
		}
	}

	hcu.Status.ControlPlaneStatus.Components = components
	hcu.Status.ControlPlaneStatus.TotalComponents = int32(len(components))
	hcu.Status.ControlPlaneStatus.UpgradedComponents = upgradedCount
	hcu.Status.ControlPlaneStatus.Blockers = blockers

	return nil
}
