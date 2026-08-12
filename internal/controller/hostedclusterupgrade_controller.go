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
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/mod/semver"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
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
	GetHCCOLogs(ctx context.Context, namespace, podName string) (string, error)
}

// HostedClusterUpgradeReconciler reconciles a HostedClusterUpgrade object
type HostedClusterUpgradeReconciler struct {
	client.Client
	Scheme                 *runtime.Scheme
	HostedClusterNamespace string
	LogScraper             LogScraper // Injectable log scraper for testing
}

// Global Unstructured schema for ControlPlaneComponent
var gvkCPC = schema.GroupVersionKind{
	Group:   "controlplane.hypershift.openshift.io",
	Version: "v1alpha1",
	Kind:    "ControlPlaneComponent",
}

// Global Unstructured schema for HostedCluster
var gvkHC = schema.GroupVersionKind{
	Group:   "hypershift.openshift.io",
	Version: "v1beta1",
	Kind:    "HostedCluster",
}

// Global Unstructured schema for NodePool
var gvkNP = schema.GroupVersionKind{
	Group:   "hypershift.openshift.io",
	Version: "v1beta1",
	Kind:    "NodePool",
}

// Pullspec regex pattern validation matching: registry.domain.com/path:version-arch@sha256:digest
var imagePattern = regexp.MustCompile(`^(\S+)/(\S+):([0-9]+\.[0-9]+\.[0-9]+-[a-zA-Z0-9_]+)@sha256:([a-fA-F0-9]{64})$`)

// DefaultMockLogScraper returns a static log block for testing if no LogScraper is injected.
type DefaultMockLogScraper struct {
	MockLogs string
}

func (d *DefaultMockLogScraper) GetHCCOLogs(ctx context.Context, namespace, podName string) (string, error) {
	return d.MockLogs, nil
}

// Prometheus SRE metrics definitions
var (
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
)

func init() {
	// Register Prometheus SRE metrics on the controller-runtime metrics registry
	metrics.Registry.MustRegister(
		metricPhase,
		metricDuration,
		metricStepDuration,
		metricFailedTotal,
		metricComponentReady,
		metricNodePoolBlocked,
		metricNodePoolRolloutRatio,
	)
}

//+kubebuilder:rbac:groups=upgrade.0xshift.dev,resources=hostedclusterupgrades,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=upgrade.0xshift.dev,resources=hostedclusterupgrades/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=upgrade.0xshift.dev,resources=hostedclusterupgrades/finalizers,verbs=update
//+kubebuilder:rbac:groups=controlplane.hypershift.openshift.io,resources=controlplanecomponents,verbs=get;list;watch
//+kubebuilder:rbac:groups=hypershift.openshift.io,resources=hostedclusters;nodepools,verbs=get;list;watch;update;patch
//+kubebuilder:rbac:groups="",resources=pods;pods/log,verbs=get;list

// Reconcile is the main state machine loop for HostedClusterUpgrade.
func (r *HostedClusterUpgradeReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	reqLogger := log.FromContext(ctx)
	reqLogger.Info("Reconciling HostedClusterUpgrade")

	// 1. Fetch the HostedClusterUpgrade object
	hcu := &upgradev1alpha1.HostedClusterUpgrade{}
	err := r.Get(ctx, req.NamespacedName, hcu)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Save original status for deep-equal check later to avoid redundant updates
	originalStatus := hcu.Status.DeepCopy()

	// 2. Check Coordinator Pause State (spec.pausedUntil)
	isPaused, pausedRequeueAfter, err := checkPausedState(hcu.Spec.PausedUntil)
	if err != nil {
		reqLogger.Error(err, "Failed to parse spec.pausedUntil")
		hcu.Status.Phase = upgradev1alpha1.PhaseBlocked
		hcu.Status.Message = fmt.Errorf("invalid spec.pausedUntil format: %v", err).Error()

		// Record metric failure
		metricFailedTotal.WithLabelValues(hcu.Namespace, hcu.Name, "", "", "pausedUntilParseError").Inc()

		if !reflect.DeepEqual(originalStatus, &hcu.Status) {
			_ = r.Status().Update(ctx, hcu)
		}
		return ctrl.Result{}, err
	}

	// 3. Aggregate ControlPlaneComponent status (Always run, even if paused, for SRE observability)
	err = r.aggregateControlPlaneStatus(ctx, hcu)
	if err != nil {
		reqLogger.Error(err, "Failed to aggregate ControlPlaneComponent status")
	}

	if isPaused {
		reqLogger.Info("Upgrade coordinator is currently paused (Observe-Only Mode)")
		hcu.Status.Phase = upgradev1alpha1.PhasePaused
		hcu.Status.Message = "Upgrade coordinator is paused via spec.pausedUntil."

		// Expose Paused metrics
		r.exposePhaseMetrics(hcu, 8.0) // 8.0 is Paused

		if pausedRequeueAfter > 0 {
			reqLogger.Info("Requeuing for scheduled pause expiration", "requeueAfter", pausedRequeueAfter)
		}

		if !reflect.DeepEqual(originalStatus, &hcu.Status) {
			err = r.Status().Update(ctx, hcu)
			if err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{RequeueAfter: pausedRequeueAfter}, nil
	}

	// 4. Fetch the Target HostedCluster to extract versions and status
	uHC := &unstructured.Unstructured{}
	uHC.SetGroupVersionKind(gvkHC)
	err = r.Get(ctx, client.ObjectKey{Namespace: hcu.Namespace, Name: hcu.Name}, uHC)
	if err != nil {
		if apierrors.IsNotFound(err) {
			hcu.Status.Phase = upgradev1alpha1.PhaseBlocked
			hcu.Status.Message = fmt.Sprintf("Target HostedCluster '%s' does not exist inside namespace '%s'", hcu.Name, hcu.Namespace)

			// Record metric failure
			metricFailedTotal.WithLabelValues(hcu.Namespace, hcu.Name, "", "", "HostedClusterNotFound").Inc()

			if !reflect.DeepEqual(originalStatus, &hcu.Status) {
				_ = r.Status().Update(ctx, hcu)
			}
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// 5. Run State Machine transitions
	if hcu.Status.Phase == "" || hcu.Status.Phase == upgradev1alpha1.PhasePaused {
		hcu.Status.Phase = upgradev1alpha1.PhaseIdle
		hcu.Status.Message = "Ready to proceed. Initializing upgrade state machine."
		hcu.Status.StartTime = nil
		hcu.Status.CompletionTime = nil
	}

	// Set Start Time once we move past Idle
	if hcu.Status.Phase != upgradev1alpha1.PhaseIdle && hcu.Status.StartTime == nil {
		now := metav1.Now()
		hcu.Status.StartTime = &now
	}

	var stateRequeueAfter time.Duration

	// Map phase to metric float
	var phaseVal float64 = 0.0

	switch hcu.Status.Phase {
	case upgradev1alpha1.PhaseIdle:
		phaseVal = 0.0
		hcu.Status.Phase = upgradev1alpha1.PhasePreflightChecks
		hcu.Status.Message = "Beginning Preflight Checks."

	case upgradev1alpha1.PhasePreflightChecks:
		phaseVal = 2.0 // Preflight=2
		isInstalling, err := r.checkClusterInstallingAndHealth(uHC)
		if err != nil {
			reqLogger.Error(err, "PreflightChecks failed")
			hcu.Status.Phase = upgradev1alpha1.PhaseBlocked
			hcu.Status.Message = fmt.Sprintf("Preflight Checks Blocked: %v", err)
			metricFailedTotal.WithLabelValues(hcu.Namespace, hcu.Name, "", "", "PreflightBlocked").Inc()
			break
		}

		if isInstalling {
			hcu.Status.Phase = upgradev1alpha1.PhaseInstalling
			hcu.Status.Message = "HostedCluster is currently in initial installation. Waiting for completion."
			break
		}

		hcu.Status.Phase = upgradev1alpha1.PhasePendingSchedule
		hcu.Status.Message = "Preflight Checks passed. Checking upgrade schedules."

	case upgradev1alpha1.PhaseInstalling:
		phaseVal = 1.0 // Installing=1
		isInstalling, err := r.checkClusterInstallingAndHealth(uHC)
		if err != nil {
			hcu.Status.Phase = upgradev1alpha1.PhaseBlocked
			hcu.Status.Message = fmt.Sprintf("Installation failed or cluster became unhealthy: %v", err)
			metricFailedTotal.WithLabelValues(hcu.Namespace, hcu.Name, "", "", "InstallationUnhealthy").Inc()
			break
		}

		if !isInstalling {
			hcu.Status.Phase = upgradev1alpha1.PhasePendingSchedule
			hcu.Status.Message = "HostedCluster installation complete. Checking upgrade schedules."
		}

	case upgradev1alpha1.PhasePendingSchedule:
		phaseVal = 2.0 // PendingSchedule=2 (Preflight/schedule check)
		if hcu.Spec.ControlPlane.StartAt != nil {
			now := time.Now()
			start := hcu.Spec.ControlPlane.StartAt.Time
			if now.Before(start) {
				requeueDur := start.Sub(now)
				hcu.Status.Phase = upgradev1alpha1.PhasePendingSchedule
				hcu.Status.Message = fmt.Sprintf("Control Plane upgrade scheduled to begin on %s (opens in %s)", start.Format(time.RFC3339), requeueDur.Round(time.Second).String())

				r.exposePhaseMetrics(hcu, phaseVal)
				if !reflect.DeepEqual(originalStatus, &hcu.Status) {
					_ = r.Status().Update(ctx, hcu)
				}
				return ctrl.Result{RequeueAfter: requeueDur}, nil
			}
		}

		hcu.Status.Phase = upgradev1alpha1.PhaseUpgradingControlPlane
		hcu.Status.Message = "Control Plane schedule open. Ready to initiate rollout."

	case upgradev1alpha1.PhaseUpgradingControlPlane:
		phaseVal = 3.0 // UpgradingCP=3
		// Pre-Step Health Re-verification check
		_, err := r.checkClusterInstallingAndHealth(uHC)
		if err != nil {
			hcu.Status.Phase = upgradev1alpha1.PhaseBlocked
			hcu.Status.Message = fmt.Sprintf("Pre-Step Health Gate Blocked CP Upgrade: %v", err)
			metricFailedTotal.WithLabelValues(hcu.Namespace, hcu.Name, "", "", "PreStepCPHealthBlocked").Inc()
			break
		}

		currentRelease, _, _ := unstructured.NestedString(uHC.Object, "spec", "release", "image")
		currentVer, _ := parseVersionFromPullspec(currentRelease)
		targetVer, _ := parseVersionFromPullspec(hcu.Spec.TargetReleaseImage)

		var nextReleaseImage string
		var stepLabel string

		if hcu.Spec.IntermediateReleaseImage != "" {
			intImage := hcu.Spec.IntermediateReleaseImage
			intVer, _ := parseVersionFromPullspec(intImage)

			if isUpstreamGreater(currentVer, intVer) {
				nextReleaseImage = intImage
				stepLabel = fmt.Sprintf("Intermediate (%s)", intVer)
			}
		}

		if nextReleaseImage == "" && isUpstreamGreater(currentVer, targetVer) {
			nextReleaseImage = hcu.Spec.TargetReleaseImage
			stepLabel = fmt.Sprintf("Target (%s)", targetVer)
		}

		if nextReleaseImage == "" {
			hcu.Status.Phase = upgradev1alpha1.PhaseUpgradingNodePools
			hcu.Status.Message = "Hosted Control Plane is already at target version. Initiating NodePool rollouts."
			break
		}

		// Apply the version change on the HostedCluster spec!
		reqLogger.Info("Applying version change on HostedCluster spec", "step", stepLabel, "image", nextReleaseImage)
		err = unstructured.SetNestedField(uHC.Object, nextReleaseImage, "spec", "release", "image")
		if err != nil {
			hcu.Status.Phase = upgradev1alpha1.PhaseBlocked
			hcu.Status.Message = fmt.Sprintf("Failed to configure release image on HostedCluster: %v", err)
			metricFailedTotal.WithLabelValues(hcu.Namespace, hcu.Name, currentVer, targetVer, "HostedClusterSpecUpdateFailed").Inc()
			break
		}

		err = r.Update(ctx, uHC)
		if err != nil {
			return ctrl.Result{}, err
		}

		hcu.Status.ControlPlaneStatus.Step = stepLabel
		hcu.Status.ControlPlaneStatus.TargetReleaseImage = nextReleaseImage
		hcu.Status.Phase = upgradev1alpha1.PhaseVerifyingControlPlane
		hcu.Status.Message = fmt.Sprintf("Initiated %s Control Plane rollout. Verifying component readiness.", stepLabel)

	case upgradev1alpha1.PhaseVerifyingControlPlane:
		phaseVal = 4.0 // VerifyingCP=4
		hcStatusRelease, _, _ := unstructured.NestedString(uHC.Object, "status", "releaseImage")
		expectedRelease := hcu.Status.ControlPlaneStatus.TargetReleaseImage

		if hcStatusRelease != expectedRelease {
			hcu.Status.Message = fmt.Sprintf("Waiting for HostedCluster status to match expected release image '%s' (current: '%s').", expectedRelease, hcStatusRelease)
			break
		}

		if hcu.Status.ControlPlaneStatus.TotalComponents == 0 || hcu.Status.ControlPlaneStatus.UpgradedComponents < hcu.Status.ControlPlaneStatus.TotalComponents {
			hcu.Status.Message = fmt.Sprintf("Waiting for Control Plane components to complete rollout (%d/%d ready).", hcu.Status.ControlPlaneStatus.UpgradedComponents, hcu.Status.ControlPlaneStatus.TotalComponents)
			break
		}

		targetVer, _ := parseVersionFromPullspec(hcu.Spec.TargetReleaseImage)
		currentVer, _ := parseVersionFromPullspec(hcStatusRelease)

		if isUpstreamGreater(currentVer, targetVer) {
			hcu.Status.Phase = upgradev1alpha1.PhaseUpgradingControlPlane
			hcu.Status.Message = fmt.Sprintf("Intermediate CP upgrade verified. Proceeding with next upgrade jump.")
		} else {
			// Control plane upgrade completely verified! Transition to UpgradingNodePools stage.
			hcu.Status.Phase = upgradev1alpha1.PhaseUpgradingNodePools
			hcu.Status.Message = "Hosted Control Plane upgrade successfully verified. Initiating sequential NodePool rollouts."
		}

	case upgradev1alpha1.PhaseUpgradingNodePools:
		phaseVal = 5.0 // UpgradingNPs=5
		// Execute sequential NodePool Rollouts based on Priority Label Groups
		requeue, dur, err := r.reconcileNodePools(ctx, hcu, uHC)
		if err != nil {
			reqLogger.Error(err, "Failed to reconcile NodePools")
			hcu.Status.Phase = upgradev1alpha1.PhaseBlocked
			hcu.Status.Message = fmt.Sprintf("NodePool upgrade blocked: %v", err)
			metricFailedTotal.WithLabelValues(hcu.Namespace, hcu.Name, "", "", "NodePoolRolloutFailed").Inc()
			break
		}

		if requeue {
			stateRequeueAfter = dur
		}

	case upgradev1alpha1.PhaseCompleted:
		phaseVal = 6.0 // Completed=6
		hcu.Status.Message = "All upgrade steps successfully verified and completed."
		if hcu.Status.CompletionTime == nil {
			now := metav1.Now()
			hcu.Status.CompletionTime = &now
		}

	case upgradev1alpha1.PhaseBlocked:
		phaseVal = 7.0 // Blocked=7
		// SRE recovery gate
		_, err := r.checkClusterInstallingAndHealth(uHC)
		if err == nil {
			hcu.Status.Phase = upgradev1alpha1.PhaseIdle
			hcu.Status.Message = "Cluster has recovered health. Restarting state machine."
		}
	}

	// Expose standard phase, overall duration and step-level duration metrics
	r.exposePhaseMetrics(hcu, phaseVal)

	// Update status if any field changed
	if !reflect.DeepEqual(originalStatus, &hcu.Status) {
		err = r.Status().Update(ctx, hcu)
		if err != nil {
			return ctrl.Result{}, err
		}
	}

	return ctrl.Result{RequeueAfter: stateRequeueAfter}, nil
}

// exposePhaseMetrics populates core SRE gauge values
func (r *HostedClusterUpgradeReconciler) exposePhaseMetrics(hcu *upgradev1alpha1.HostedClusterUpgrade, phaseVal float64) {
	currentRelease := hcu.Status.ControlPlaneStatus.TargetReleaseImage
	if currentRelease == "" {
		currentRelease = "unknown"
	}
	currentVer, _ := parseVersionFromPullspec(currentRelease)
	targetVer, _ := parseVersionFromPullspec(hcu.Spec.TargetReleaseImage)

	var intermediateVer string
	if hcu.Spec.IntermediateReleaseImage != "" {
		intermediateVer, _ = parseVersionFromPullspec(hcu.Spec.IntermediateReleaseImage)
	}

	metricPhase.WithLabelValues(hcu.Namespace, hcu.Name, currentVer, targetVer, intermediateVer).Set(phaseVal)

	if hcu.Status.StartTime != nil {
		duration := time.Since(hcu.Status.StartTime.Time).Seconds()
		if hcu.Status.CompletionTime != nil {
			duration = hcu.Status.CompletionTime.Sub(hcu.Status.StartTime.Time).Seconds()
		}
		metricDuration.WithLabelValues(hcu.Namespace, hcu.Name, currentVer, targetVer, intermediateVer).Set(duration)

		// Set active step duration
		step := string(hcu.Status.Phase)
		if hcu.Status.Phase == upgradev1alpha1.PhaseUpgradingControlPlane || hcu.Status.Phase == upgradev1alpha1.PhaseVerifyingControlPlane {
			step = "ControlPlane"
		}
		metricStepDuration.WithLabelValues(hcu.Namespace, hcu.Name, step, currentVer, targetVer).Set(duration)
	}
}

// reconcileNodePools fetches all associated NodePools, clusters them into priority groups, and triggers upgrades.
func (r *HostedClusterUpgradeReconciler) reconcileNodePools(ctx context.Context, hcu *upgradev1alpha1.HostedClusterUpgrade, uHC *unstructured.Unstructured) (bool, time.Duration, error) {
	reqLogger := log.FromContext(ctx)

	// 1. Fetch ALL NodePools inside the HostedCluster namespace (e.g. "default")
	uNPList := &unstructured.UnstructuredList{}
	uNPList.SetGroupVersionKind(gvkNP)
	err := r.List(ctx, uNPList, client.InNamespace(hcu.Namespace))
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
		// No node pools associated with this cluster. Transition directly to Completed!
		hcu.Status.Phase = upgradev1alpha1.PhaseCompleted
		hcu.Status.Message = "No NodePools associated with this HostedCluster. Upgrade complete."
		return false, 0, nil
	}

	// 3. Cluster NodePools by label order priority (defaulting to 50 if missing)
	type npContext struct {
		object      unstructured.Unstructured
		name        string
		order       int
		startAt     *time.Time
		pausedUntil string
		version     string
	}

	orderGroups := make(map[int][]npContext)
	var orderKeys []int

	for _, np := range clusterNodePools {
		name := np.GetName()
		labels := np.GetLabels()
		order := 50
		if labels != nil && labels["upgrade.huc.0xshift.dev/order"] != "" {
			if val, err := strconv.Atoi(labels["upgrade.huc.0xshift.dev/order"]); err == nil {
				order = val
			}
		}

		pUntil, _, _ := unstructured.NestedString(np.Object, "spec", "pausedUntil")
		version, _, _ := unstructured.NestedString(np.Object, "status", "version")

		// Match against spec list to resolve custom StartAt schedules
		var startAt *time.Time
		for _, s := range hcu.Spec.NodePools {
			if s.Name == name && s.StartAt != nil {
				t := s.StartAt.Time
				startAt = &t
				break
			}
		}

		c := npContext{
			object:      np,
			name:        name,
			order:       order,
			startAt:     startAt,
			pausedUntil: pUntil,
			version:     version,
		}

		reqLogger.Info("Parsed NodePool schema info", "name", name, "order", order, "pausedUntil", pUntil, "version", version)

		if _, exists := orderGroups[order]; !exists {
			orderGroups[order] = []npContext{}
			orderKeys = append(orderKeys, order)
		}
		orderGroups[order] = append(orderGroups[order], c)
	}

	sort.Ints(orderKeys)

	// 4. Sequentially process priority groups (lowest first)
	hcuStatuses := make([]upgradev1alpha1.NodePoolUpgradeStatus, len(clusterNodePools))
	idx := 0

	concurrency := 1
	if hcu.Spec.RolloutPolicy != nil && hcu.Spec.RolloutPolicy.MaxParallelNodePools > 0 {
		concurrency = int(hcu.Spec.RolloutPolicy.MaxParallelNodePools)
	}

	groupUpgradeTriggered := false
	var shortestRequeue time.Duration
	allCompleted := true
	priorGroupsCompleted := true

	for _, order := range orderKeys {
		groupPools := orderGroups[order]
		groupCompleted := true

		// Count how many pools are currently active/upgrading in this group
		activeUpgrades := 0
		for _, np := range groupPools {
			curVer, _ := parseVersionFromPullspec(hcu.Spec.TargetReleaseImage)
			npVer, _ := parseVersionFromPullspec(np.version)
			if isUpstreamGreater(npVer, curVer) && np.pausedUntil != "true" && np.pausedUntil != "True" {
				activeUpgrades++
			}
		}

		reqLogger.Info("Processing NodePool priority group", "order", order, "concurrency", concurrency, "activeUpgrades", activeUpgrades, "groupPoolsCount", len(groupPools))

		for _, np := range groupPools {
			// Extract nodes counts
			replicas, _, _ := unstructured.NestedInt64(np.object.Object, "status", "replicas")
			updatedReplicas, _, _ := unstructured.NestedInt64(np.object.Object, "status", "updatedReplicas")
			readyReplicas, _, _ := unstructured.NestedInt64(np.object.Object, "status", "readyReplicas")

			curVer, _ := parseVersionFromPullspec(hcu.Spec.TargetReleaseImage)
			npVer, _ := parseVersionFromPullspec(np.version)

			var phase string
			var blockers []string

			// Re-run preflight checks immediately before triggering upgrades
			_, preflightErr := r.checkClusterInstallingAndHealth(uHC)

			reqLogger.Info("Evaluating NodePool specs", "name", np.name, "npVer", npVer, "targetVer", curVer, "pausedUntil", np.pausedUntil, "activeUpgrades", activeUpgrades)

			if !isUpstreamGreater(npVer, curVer) {
				phase = "Completed"
				reqLogger.Info("NodePool upgrade already complete", "name", np.name)
			} else if preflightErr != nil {
				phase = "Blocked"
				blockers = append(blockers, fmt.Sprintf("Cluster health degraded: %v", preflightErr))
				groupCompleted = false
			} else if !priorGroupsCompleted {
				// Prior priority group is not yet completed. Hold queue.
				phase = "WaitingForPriority"
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
					// Schedule is open. Trigger Upgrade (subject to concurrency limits)
					if activeUpgrades < concurrency || (np.pausedUntil != "true" && np.pausedUntil != "True" && np.pausedUntil != "") {
						phase = "Upgrading"
						groupCompleted = false

						// Set version and unpause NodePool spec!
						npSpecRelease, _, _ := unstructured.NestedString(np.object.Object, "spec", "release", "image")

						// LOG DIAGNOSTICS FOR FIELD VALUES
						reqLogger.Info("Diagnostic compare values", "name", np.name, "npSpecRelease", npSpecRelease, "targetRelease", hcu.Spec.TargetReleaseImage)

						if npSpecRelease != hcu.Spec.TargetReleaseImage || np.pausedUntil == "true" || np.pausedUntil == "True" {
							reqLogger.Info("Triggering upgrade rollout on NodePool", "name", np.name, "targetImage", hcu.Spec.TargetReleaseImage)
							_ = unstructured.SetNestedField(np.object.Object, hcu.Spec.TargetReleaseImage, "spec", "release", "image")
							_ = unstructured.SetNestedField(np.object.Object, "false", "spec", "pausedUntil")
							err = r.Update(ctx, &np.object)
							if err != nil {
								return false, 0, err
							}
							groupUpgradeTriggered = true
							activeUpgrades++
						}

						// Run HCCO container log scraping to trace blocked drains
						blockers = r.scrapeHCCOLogsForDrains(ctx, hcu.Namespace, hcu.Name, np.name)
					} else {
						// Concurrency throttled. Wait in queue.
						phase = "WaitingForConcurrency"
						groupCompleted = false
					}
				}
			}

			// Instrument NodePool blocked status and rollout ratio curves
			var isBlockedVal float64 = 0.0
			if len(blockers) > 0 {
				isBlockedVal = 1.0
			}
			metricNodePoolBlocked.WithLabelValues(hcu.Namespace, hcu.Name, np.name, npVer, curVer).Set(isBlockedVal)

			var ratio float64 = 0.0
			if replicas > 0 {
				ratio = float64(updatedReplicas) / float64(replicas)
			}
			metricNodePoolRolloutRatio.WithLabelValues(hcu.Namespace, hcu.Name, np.name).Set(ratio)

			var schedStart *metav1.Time
			if np.startAt != nil {
				t := metav1.NewTime(*np.startAt)
				schedStart = &t
			}

			hcuStatuses[idx] = upgradev1alpha1.NodePoolUpgradeStatus{
				Name:               np.name,
				TargetReleaseImage: hcu.Spec.TargetReleaseImage,
				ScheduledStart:     schedStart,
				Phase:              phase,
				Nodes: upgradev1alpha1.NodeStatusSummary{
					Total:   int32(replicas),
					Updated: int32(updatedReplicas),
					Ready:   int32(readyReplicas),
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

	if allCompleted {
		// All priority groups successfully verified and upgraded! Transition state machine to Completed.
		hcu.Status.Phase = upgradev1alpha1.PhaseCompleted
		hcu.Status.Message = "All worker NodePools successfully upgraded and verified on target version."
		return false, 0, nil
	}

	// Update status message with active scheduler/rollout telemetry
	if shortestRequeue > 0 {
		hcu.Status.Message = fmt.Sprintf("Upgrades active. Next scheduled NodePool window opens in %s.", shortestRequeue.Round(time.Second).String())
		return true, shortestRequeue, nil
	}

	hcu.Status.Message = "Worker NodePool upgrades actively rolling out."
	if groupUpgradeTriggered {
		return true, time.Second * 5, nil // Requeue quickly to observe the unpause
	}

	return false, 0, nil
}

// scrapeHCCOLogsForDrains scrapes HCCO logs to detect stuck node drains (utilizes injectable log scrapers).
func (r *HostedClusterUpgradeReconciler) scrapeHCCOLogsForDrains(ctx context.Context, namespace, clusterName, nodePoolName string) []string {
	blockers := []string{}
	if r.LogScraper == nil {
		return blockers
	}

	// HCCO is located inside the control plane namespace: <hc_namespace>-<cluster_name>
	cpNamespace := fmt.Sprintf("%s-%s", namespace, clusterName)

	// Retrieve logs via our mock/injectable scraper interface
	logsBlock, err := r.LogScraper.GetHCCOLogs(ctx, cpNamespace, "hosted-cluster-config-operator")
	if err != nil {
		return blockers
	}

	// Search and parse for HCCO failed drain warnings
	lines := strings.Split(logsBlock, "\n")
	for _, line := range lines {
		if strings.Contains(line, "cannot evict pod") && strings.Contains(line, "due to PDB") {
			// Extract pod and PDB details
			blockers = append(blockers, strings.TrimSpace(line))
		} else if strings.Contains(line, "failed to drain node") {
			blockers = append(blockers, strings.TrimSpace(line))
		}
	}

	return blockers
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
	uCPC := &unstructured.Unstructured{}
	uCPC.SetGroupVersionKind(gvkCPC)

	return ctrl.NewControllerManagedBy(mgr).
		For(&upgradev1alpha1.HostedClusterUpgrade{}).
		Watches(
			uCPC,
			handler.EnqueueRequestsFromMapFunc(r.mapCPCToHostedClusterUpgrade),
			builder.WithPredicates(statusChangePredicate),
		).
		Complete(r)
}

// mapCPCToHostedClusterUpgrade maps status changes of CPC objects in <hc_ns>-<name> to parent HCU in <hc_ns> sharing <name>.
func (r *HostedClusterUpgradeReconciler) mapCPCToHostedClusterUpgrade(ctx context.Context, obj client.Object) []reconcile.Request {
	prefix := r.HostedClusterNamespace + "-"
	cpcNs := obj.GetNamespace()
	if !strings.HasPrefix(cpcNs, prefix) {
		return nil
	}
	clusterName := strings.TrimPrefix(cpcNs, prefix)

	return []reconcile.Request{
		{
			NamespacedName: client.ObjectKey{
				Namespace: r.HostedClusterNamespace,
				Name:      clusterName,
			},
		},
	}
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

// parseVersionFromPullspec extracts the semantic version from the tag slice: <version>-<arch>
func parseVersionFromPullspec(pullspec string) (string, error) {
	matches := imagePattern.FindStringSubmatch(pullspec)
	if len(matches) < 4 {
		return "", fmt.Errorf("invalid pullspec format: must match registry.domain.com/path:version-arch@sha256:digest")
	}
	tag := matches[3] // e.g. "4.20.44-x86_64"
	idx := strings.Index(tag, "-")
	if idx == -1 {
		return tag, nil
	}
	return tag[:idx], nil // e.g. "4.20.44"
}

// isUpstreamGreater compares SemVer version strings to ensure vNext is strictly greater than vPrev.
func isUpstreamGreater(vPrev, vNext string) bool {
	pPrev := "v" + vPrev
	pNext := "v" + vNext
	return semver.Compare(pPrev, pNext) < 0
}

// checkClusterInstallingAndHealth checks if the cluster is currently installing or contains failing/degraded conditions.
func (r *HostedClusterUpgradeReconciler) checkClusterInstallingAndHealth(uHC *unstructured.Unstructured) (bool, error) {
	history, foundHist, _ := unstructured.NestedSlice(uHC.Object, "status", "version", "history")
	if !foundHist || len(history) == 0 {
		return true, nil // No completed versions yet, meaning we are still in installation
	}

	conds, foundConds, _ := unstructured.NestedSlice(uHC.Object, "status", "conditions")
	if foundConds {
		for _, c := range conds {
			condMap, ok := c.(map[string]interface{})
			if !ok {
				continue
			}
			cType, _ := condMap["type"].(string)
			cStatus, _ := condMap["status"].(string)
			cMsg, _ := condMap["message"].(string)

			if cType == "Failing" && cStatus == "True" {
				return false, fmt.Errorf("HostedCluster contains failing condition: %s", cMsg)
			}
			if cType == "Degraded" && cStatus == "True" {
				return false, fmt.Errorf("HostedCluster contains degraded condition: %s", cMsg)
			}
		}
	}

	return false, nil
}

// aggregateControlPlaneStatus queries and copies the individual states of the ControlPlaneComponents.
func (r *HostedClusterUpgradeReconciler) aggregateControlPlaneStatus(ctx context.Context, hcu *upgradev1alpha1.HostedClusterUpgrade) error {
	cpNamespace := fmt.Sprintf("%s-%s", hcu.Namespace, hcu.Name)

	uList := &unstructured.UnstructuredList{}
	uList.SetGroupVersionKind(gvkCPC)

	err := r.List(ctx, uList, client.InNamespace(cpNamespace))
	if err != nil {
		hcu.Status.ControlPlaneStatus.Components = []upgradev1alpha1.ControlPlaneComponentStatus{}
		hcu.Status.ControlPlaneStatus.TotalComponents = 0
		hcu.Status.ControlPlaneStatus.UpgradedComponents = 0
		return err
	}

	components := make([]upgradev1alpha1.ControlPlaneComponentStatus, len(uList.Items))
	var upgradedCount int32
	blockers := []string{}

	for i, item := range uList.Items {
		name := item.GetName()
		version, _, _ := unstructured.NestedString(item.Object, "status", "version")

		conds, found, _ := unstructured.NestedSlice(item.Object, "status", "conditions")
		available := false
		rolloutComplete := false
		message := ""

		if found {
			for _, c := range conds {
				condMap, ok := c.(map[string]interface{})
				if !ok {
					continue
				}
				condType, _ := condMap["type"].(string)
				condStatus, _ := condMap["status"].(string)
				condMsg, _ := condMap["message"].(string)

				if condType == "Available" && condStatus == "True" {
					available = true
				}
				if condType == "RolloutComplete" {
					if condStatus == "True" {
						rolloutComplete = true
					} else if condMsg != "" {
						message = condMsg
					}
				}
			}
		}

		if available && rolloutComplete {
			upgradedCount++
		} else if message != "" {
			blockers = append(blockers, fmt.Sprintf("Component '%s' is blocked: %s", name, message))
		}

		components[i] = upgradev1alpha1.ControlPlaneComponentStatus{
			Name:            name,
			Version:         version,
			Available:       available,
			RolloutComplete: rolloutComplete,
			Message:         message,
		}
	}

	hcu.Status.ControlPlaneStatus.Components = components
	hcu.Status.ControlPlaneStatus.TotalComponents = int32(len(components))
	hcu.Status.ControlPlaneStatus.UpgradedComponents = upgradedCount
	hcu.Status.ControlPlaneStatus.Blockers = blockers

	return nil
}
