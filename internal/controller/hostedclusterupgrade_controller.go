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
	"strings"
	"time"

	"golang.org/x/mod/semver"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	upgradev1alpha1 "github.com/duritong/hypershift-upgrade-coordinator/api/v1alpha1"
)

// HostedClusterUpgradeReconciler reconciles a HostedClusterUpgrade object
type HostedClusterUpgradeReconciler struct {
	client.Client
	Scheme                 *runtime.Scheme
	HostedClusterNamespace string
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

// Pullspec regex pattern validation matching: registry.domain.com/path:version-arch@sha256:digest
var imagePattern = regexp.MustCompile(`^(\S+)/(\S+):([0-9]+\.[0-9]+\.[0-9]+-[a-zA-Z0-9_]+)@sha256:([a-fA-F0-9]{64})$`)

//+kubebuilder:rbac:groups=upgrade.0xshift.dev,resources=hostedclusterupgrades,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=upgrade.0xshift.dev,resources=hostedclusterupgrades/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=upgrade.0xshift.dev,resources=hostedclusterupgrades/finalizers,verbs=update
//+kubebuilder:rbac:groups=controlplane.hypershift.openshift.io,resources=controlplanecomponents,verbs=get;list;watch
//+kubebuilder:rbac:groups=hypershift.openshift.io,resources=hostedclusters;nodepools,verbs=get;list;watch;update;patch

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
	}

	switch hcu.Status.Phase {
	case upgradev1alpha1.PhaseIdle:
		// Transition from Idle to PreflightChecks
		hcu.Status.Phase = upgradev1alpha1.PhasePreflightChecks
		hcu.Status.Message = "Beginning Preflight Checks."

	case upgradev1alpha1.PhasePreflightChecks:
		// Perform Preflight Checks (checks cluster health and installation status)
		isInstalling, err := r.checkClusterInstallingAndHealth(uHC)
		if err != nil {
			reqLogger.Error(err, "PreflightChecks failed")
			hcu.Status.Phase = upgradev1alpha1.PhaseBlocked
			hcu.Status.Message = fmt.Sprintf("Preflight Checks Blocked: %v", err)
			break
		}

		if isInstalling {
			hcu.Status.Phase = upgradev1alpha1.PhaseInstalling
			hcu.Status.Message = "HostedCluster is currently in initial installation. Waiting for completion."
			break
		}

		// Proceed to scheduling check
		hcu.Status.Phase = upgradev1alpha1.PhasePendingSchedule
		hcu.Status.Message = "Preflight Checks passed. Checking upgrade schedules."

	case upgradev1alpha1.PhaseInstalling:
		// Check if installation is complete
		isInstalling, err := r.checkClusterInstallingAndHealth(uHC)
		if err != nil {
			hcu.Status.Phase = upgradev1alpha1.PhaseBlocked
			hcu.Status.Message = fmt.Sprintf("Installation failed or cluster became unhealthy: %v", err)
			break
		}

		if !isInstalling {
			hcu.Status.Phase = upgradev1alpha1.PhasePendingSchedule
			hcu.Status.Message = "HostedCluster installation complete. Checking upgrade schedules."
		}

	case upgradev1alpha1.PhasePendingSchedule:
		// Check Control Plane StartAt schedule
		if hcu.Spec.ControlPlane.StartAt != nil {
			now := time.Now()
			start := hcu.Spec.ControlPlane.StartAt.Time
			if now.Before(start) {
				requeueDur := start.Sub(now)
				hcu.Status.Phase = upgradev1alpha1.PhasePendingSchedule
				hcu.Status.Message = fmt.Sprintf("Control Plane upgrade scheduled to begin on %s (opens in %s)", start.Format(time.RFC3339), requeueDur.Round(time.Second).String())

				// Requeue once the schedule is expected to open
				if !reflect.DeepEqual(originalStatus, &hcu.Status) {
					_ = r.Status().Update(ctx, hcu)
				}
				return ctrl.Result{RequeueAfter: requeueDur}, nil
			}
		}

		// Transition to CP upgrade
		hcu.Status.Phase = upgradev1alpha1.PhaseUpgradingControlPlane
		hcu.Status.Message = "Control Plane schedule open. Ready to initiate rollout."

	case upgradev1alpha1.PhaseUpgradingControlPlane:
		// Pre-Step Health Re-verification check (re-run preflights immediately before modifying spec)
		_, err := r.checkClusterInstallingAndHealth(uHC)
		if err != nil {
			hcu.Status.Phase = upgradev1alpha1.PhaseBlocked
			hcu.Status.Message = fmt.Sprintf("Pre-Step Health Gate Blocked CP Upgrade: %v", err)
			break
		}

		currentRelease, _, _ := unstructured.NestedString(uHC.Object, "spec", "release", "image")
		currentVer, _ := parseVersionFromPullspec(currentRelease)

		// Parse target and intermediate releases
		targetVer, _ := parseVersionFromPullspec(hcu.Spec.TargetReleaseImage)

		// Determine if EUS intermediate jump is needed
		var nextReleaseImage string
		var stepLabel string

		if len(hcu.Spec.IntermediateReleaseImages) > 0 {
			firstIntImage := hcu.Spec.IntermediateReleaseImages[0]
			firstIntVer, _ := parseVersionFromPullspec(firstIntImage)

			if isUpstreamGreater(currentVer, firstIntVer) {
				nextReleaseImage = firstIntImage
				stepLabel = fmt.Sprintf("Intermediate (%s)", firstIntVer)
			}
		}

		if nextReleaseImage == "" && isUpstreamGreater(currentVer, targetVer) {
			nextReleaseImage = hcu.Spec.TargetReleaseImage
			stepLabel = fmt.Sprintf("Target (%s)", targetVer)
		}

		if nextReleaseImage == "" {
			// Already at target version (or greater)
			hcu.Status.Phase = upgradev1alpha1.PhaseVerifyingControlPlane
			hcu.Status.Message = "Control Plane version change applied. Verifying component stability."
			break
		}

		// Apply the version change on the HostedCluster spec!
		reqLogger.Info("Applying version change on HostedCluster spec", "step", stepLabel, "image", nextReleaseImage)
		err = unstructured.SetNestedField(uHC.Object, nextReleaseImage, "spec", "release", "image")
		if err != nil {
			hcu.Status.Phase = upgradev1alpha1.PhaseBlocked
			hcu.Status.Message = fmt.Sprintf("Failed to configure release image on HostedCluster: %v", err)
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
		// Check if Control Plane upgrade step is fully completed and verified
		// 1. Verify HostedCluster status release matches the expected step image
		hcStatusRelease, _, _ := unstructured.NestedString(uHC.Object, "status", "releaseImage")
		expectedRelease := hcu.Status.ControlPlaneStatus.TargetReleaseImage

		if hcStatusRelease != expectedRelease {
			hcu.Status.Message = fmt.Sprintf("Waiting for HostedCluster status to match expected release image '%s' (current: '%s').", expectedRelease, hcStatusRelease)
			break
		}

		// 2. Verify all ControlPlaneComponents are Available=True and RolloutComplete=True
		if hcu.Status.ControlPlaneStatus.TotalComponents == 0 || hcu.Status.ControlPlaneStatus.UpgradedComponents < hcu.Status.ControlPlaneStatus.TotalComponents {
			hcu.Status.Message = fmt.Sprintf("Waiting for Control Plane components to complete rollout (%d/%d ready).", hcu.Status.ControlPlaneStatus.UpgradedComponents, hcu.Status.ControlPlaneStatus.TotalComponents)
			break
		}

		// 3. Step verified! Check if there are further intermediate jumps or if target version is completed
		targetVer, _ := parseVersionFromPullspec(hcu.Spec.TargetReleaseImage)
		currentVer, _ := parseVersionFromPullspec(hcStatusRelease)

		if isUpstreamGreater(currentVer, targetVer) {
			// We have intermediate release(s) left. Transition back to UpgradingCP to trigger next jump.
			hcu.Status.Phase = upgradev1alpha1.PhaseUpgradingControlPlane
			hcu.Status.Message = fmt.Sprintf("Intermediate CP upgrade verified. Proceeding with next upgrade jump.")
		} else {
			// Target Control Plane upgrade completed and verified!
			// For Milestone 4, we transition to PhaseUpgradingNodePools and then Completed for validation simplicity
			hcu.Status.Phase = upgradev1alpha1.PhaseCompleted // Completed CP Upgrades! (NodePools detailed in Milestone 5)
			hcu.Status.Message = "Hosted Control Plane upgrade successfully verified on target version."
		}

	case upgradev1alpha1.PhaseBlocked:
		// SRE escape hatch: SREs manually repair objects and unpause/resume.
		// On every reconcile, we re-run health checks to see if we can automatically recover
		_, err := r.checkClusterInstallingAndHealth(uHC)
		if err == nil {
			hcu.Status.Phase = upgradev1alpha1.PhaseIdle
			hcu.Status.Message = "Cluster has recovered health. Restarting state machine."
		}
	}

	// Update status if any field changed
	if !reflect.DeepEqual(originalStatus, &hcu.Status) {
		err = r.Status().Update(ctx, hcu)
		if err != nil {
			return ctrl.Result{}, err
		}
	}

	return ctrl.Result{}, nil
}

// checkClusterInstallingAndHealth checks if the cluster is currently installing or contains failing/degraded conditions.
func (r *HostedClusterUpgradeReconciler) checkClusterInstallingAndHealth(uHC *unstructured.Unstructured) (bool, error) {
	// 1. Check for initial installation phase (history is empty, and Progressing condition might be true)
	history, foundHist, _ := unstructured.NestedSlice(uHC.Object, "status", "version", "history")
	if !foundHist || len(history) == 0 {
		return true, nil // No completed versions yet, meaning we are still in installation
	}

	// 2. Check for active degraded or failing conditions on the HostedCluster
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
	cpcNs := obj.GetNamespace()
	prefix := r.HostedClusterNamespace + "-"
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
