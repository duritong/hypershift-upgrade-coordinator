package controller

import (
	"context"
	"fmt"
	"strings"

	upgradev1alpha1 "github.com/duritong/hypershift-upgrade-coordinator/api/v1alpha1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var gvkHCP = gvkHC.GroupVersion().WithKind("HostedControlPlane")

// Conservatively require explicit approval for patch as well as minor starts.
// Upgradeable describes the next upgrade, not readiness of the current release.
func canInitiateControlPlane(hc *unstructured.Unstructured) error {
	conditions, _, _ := unstructured.NestedSlice(hc.Object, "status", "conditions")
	for _, item := range conditions {
		c, _ := item.(map[string]interface{})
		if c["type"] == "ClusterVersionUpgradeable" && c["status"] == "True" {
			return nil
		}
	}
	return fmt.Errorf("a new control plane release requires ClusterVersionUpgradeable=True")
}

// Workers remain at their actual versions throughout all CP hops. A numerically
// valid CP path can still violate HyperShift's same-major n-3 worker skew.
func (r *HostedClusterUpgradeReconciler) validateControlPlaneWorkerSkew(ctx context.Context, hcu *upgradev1alpha1.HostedClusterUpgrade, hc *unstructured.Unstructured) error {
	_, current := GetHostedClusterCurrentRelease(hc)
	path := []string{current}
	for _, image := range []string{hcu.Spec.IntermediateReleaseImage, hcu.Spec.TargetReleaseImage} {
		if image == "" {
			continue
		}
		version, err := upgradev1alpha1.ExtractVersion(image)
		if err != nil {
			return err
		}
		if upgradev1alpha1.IsUpstreamGreater(current, version) {
			path = append(path, version)
		}
	}
	pools := &unstructured.UnstructuredList{}
	pools.SetGroupVersionKind(gvkNP)
	if err := r.List(ctx, pools, client.InNamespace(hcu.Namespace)); err != nil {
		return err
	}
	for _, np := range pools.Items {
		cluster, _, _ := unstructured.NestedString(np.Object, "spec", "clusterName")
		if cluster != hcu.Name {
			continue
		}
		version, _, _ := unstructured.NestedString(np.Object, "status", "version")
		versions := []string{version}
		buckets, found, err := unstructured.NestedSlice(np.Object, "status", "nodesInfo", "nodeVersions")
		if err != nil || !found || len(buckets) == 0 {
			return fmt.Errorf("NodePool %q is missing actual node version buckets; cannot prove planned CP skew", np.GetName())
		}
		for _, item := range buckets {
			bucket, _ := item.(map[string]interface{})
			version, _ := bucket["ocpVersion"].(string)
			versions = append(versions, version)
		}
		for _, worker := range versions {
			major, minor, err := upgradev1alpha1.GetMajorMinor(worker)
			if _, versionErr := upgradev1alpha1.ExtractVersion(worker); err != nil || versionErr != nil {
				return fmt.Errorf("NodePool %q has missing or invalid actual worker version %q", np.GetName(), worker)
			}
			for _, cp := range path {
				cpMajor, cpMinor, err := upgradev1alpha1.GetMajorMinor(cp)
				if err != nil || major != cpMajor || minor > cpMinor || cpMinor-minor > 3 || upgradev1alpha1.IsUpstreamGreater(cp, worker) {
					return fmt.Errorf("NodePool %q worker %s violates same-major n-3 skew at planned CP %s", np.GetName(), worker, cp)
				}
			}
		}
	}
	return nil
}

// platformHasCloudNetworkConfigController returns true for platforms where CNO deploys
// cloud-network-config-controller (AWS, Azure, GCP, OpenStack).
// Non-cloud platforms (Agent, KubeVirt, None, IBMCloud, PowerVS) never deploy it.
func platformHasCloudNetworkConfigController(platform string) bool {
	switch platform {
	case "AWS", "Azure", "GCP", "OpenStack":
		return true
	default:
		return false
	}
}

// isUpstreamCNOBug checks for the known upstream HyperShift bug (fixed in commit 36afffc64f)
// where CPO expects cloud-network-config-controller on non-cloud platforms.
// TODO: Remove this workaround once managed clusters are upgraded to HyperShift incorporating commit 36afffc64f.
func isUpstreamCNOBug(componentName, compMsg, platform string, available bool) bool {
	if componentName != "cluster-network-operator" || !available {
		return false
	}
	if platformHasCloudNetworkConfigController(platform) {
		return false
	}
	return strings.Contains(compMsg, "cloud-network-config-controller")
}

// isControlPlaneVersionCNOBugTolerated tolerates controlPlaneVersion staying in Partial
// when caused by the upstream HyperShift CNO bug on non-cloud platforms.
// TODO: Remove this workaround once managed clusters run HyperShift with commit 36afffc64f.
func isControlPlaneVersionCNOBugTolerated(hc *unstructured.Unstructured, version, image string) bool {
	platform, _, _ := unstructured.NestedString(hc.Object, "spec", "platform", "type")
	if platformHasCloudNetworkConfigController(platform) {
		return false
	}
	cpLatest := latestRelease(hc, "controlPlaneVersion")
	if cpLatest["state"] != "Partial" {
		return false
	}
	// Verify guest ClusterVersion status is fully completed at the expected version and payload.
	latest := latestRelease(hc, "version")
	completed, _ := latest["image"].(string)
	if latest["state"] != "Completed" || latest["version"] != version || !samePayload(completed, image) {
		return false
	}
	// Verify controlPlaneVersion desired image matches expected image
	cpDesired, _, _ := unstructured.NestedString(hc.Object, "status", "controlPlaneVersion", "desired", "image")
	if cpDesired != "" && !samePayload(cpDesired, image) {
		return false
	}
	cpCompleted, _ := cpLatest["image"].(string)
	return cpLatest["version"] == version && samePayload(cpCompleted, image)
}

// Payload identity is independent of registry mirrors and human-readable tags.
// Never fall back to version equality when comparing immutable payloads.
func samePayload(a, b string) bool {
	return upgradev1alpha1.SamePayload(a, b)
}

func latestRelease(hc *unstructured.Unstructured, field string) map[string]interface{} {
	history, _, _ := unstructured.NestedSlice(hc.Object, "status", field, "history")
	if len(history) > 0 {
		entry, _ := history[0].(map[string]interface{})
		return entry
	}
	return nil
}

// plannedControlPlaneRelease adopts only an active request on this plan, never
// selects a hop from historical Completed state over a newer external request.
func plannedControlPlaneRelease(hc *unstructured.Unstructured, hcu *upgradev1alpha1.HostedClusterUpgrade) (string, error) {
	baseline, current := GetHostedClusterCurrentRelease(hc)
	next := hcu.Spec.TargetReleaseImage
	if intermediate := hcu.Spec.IntermediateReleaseImage; intermediate != "" {
		v, _ := upgradev1alpha1.ExtractVersion(intermediate)
		if upgradev1alpha1.IsUpstreamGreater(current, v) {
			next = intermediate
		}
	}
	active := ""
	requests := []string{}
	desired, _, _ := unstructured.NestedString(hc.Object, "spec", "release", "image")
	requests = append(requests, desired)
	for _, field := range []string{"version", "controlPlaneVersion"} {
		image, _, _ := unstructured.NestedString(hc.Object, "status", field, "desired", "image")
		requests = append(requests, image)
		latest := latestRelease(hc, field)
		if latest["state"] == "Partial" {
			image, _ := latest["image"].(string)
			requests = append(requests, image)
		}
	}
	for _, image := range requests {
		if image == "" || samePayload(image, baseline) {
			continue
		}
		candidate := ""
		for _, planned := range []string{hcu.Spec.IntermediateReleaseImage, hcu.Spec.TargetReleaseImage} {
			if samePayload(image, planned) {
				candidate = planned
			}
		}
		if candidate == "" || (active != "" && !samePayload(active, candidate)) {
			return "", fmt.Errorf("conflicting in-flight control plane payload; refusing to replace the current request")
		}
		active = candidate
	}
	if active != "" {
		return active, nil
	}
	targetVersion, _ := upgradev1alpha1.ExtractVersion(next)
	if current == targetVersion && !samePayload(baseline, next) {
		return "", fmt.Errorf("same-version control plane payload replacement is unsupported")
	}
	return next, nil
}

func (r *HostedClusterUpgradeReconciler) verifyControlPlane(ctx context.Context, hcu *upgradev1alpha1.HostedClusterUpgrade, hc *unstructured.Unstructured, image string) error {
	_, err := r.verifyControlPlaneProof(ctx, hcu, hc, image)
	return err
}

// A proof describes the exact objects used by management verification, not fresh
// metadata sampled later. It stays local to one gate/action, never a reconciler.
type managementProof struct {
	hcUID, hcpUID               types.UID
	hcGeneration, hcpGeneration int64
}

func (r *HostedClusterUpgradeReconciler) verifyControlPlaneProof(ctx context.Context, hcu *upgradev1alpha1.HostedClusterUpgrade, hc *unstructured.Unstructured, image string) (managementProof, error) {
	var proof managementProof
	if hc.GetUID() == "" || hc.GetGeneration() < 1 {
		return proof, fmt.Errorf("HostedCluster identity/generation unavailable")
	}
	if err := upgradev1alpha1.RejectControlPlaneReleaseOverride(hc); err != nil {
		return proof, err
	}
	if installing, err := r.checkClusterInstallingAndHealth(hc); err != nil {
		return proof, err
	} else if installing {
		return proof, fmt.Errorf("HostedCluster installation is not complete")
	}
	version, err := upgradev1alpha1.ExtractVersion(image)
	if err != nil {
		baseline, baselineVersion := GetHostedClusterCurrentRelease(hc)
		if !samePayload(baseline, image) || baselineVersion == "" {
			return proof, err
		}
		version = baselineVersion
	}
	desired, _, _ := unstructured.NestedString(hc.Object, "spec", "release", "image")
	if !samePayload(desired, image) {
		return proof, fmt.Errorf("HostedCluster requested payload does not match expected release")
	}
	for _, field := range []string{"version", "controlPlaneVersion"} {
		latest := latestRelease(hc, field)
		completed, _ := latest["image"].(string)
		if latest["state"] != "Completed" || latest["version"] != version || !samePayload(completed, image) {
			if field == "controlPlaneVersion" && latest["state"] == "Partial" && isControlPlaneVersionCNOBugTolerated(hc, version, image) {
				// Upstream HyperShift bug (fixed in 36afffc64f): on non-cloud platforms,
				// controlPlaneVersion remains Partial because CNO expects cloud-network-config-controller.
				// Guest version is Completed at expected payload and version.
				continue
			}
			return proof, fmt.Errorf("waiting for latest %s Completed history with exact expected payload and version", field)
		}
		if requested, _, _ := unstructured.NestedString(hc.Object, "status", field, "desired", "image"); requested != "" && !samePayload(requested, image) {
			return proof, fmt.Errorf("%s desired payload conflicts with expected release", field)
		}
	}
	hcp := &unstructured.Unstructured{}
	hcp.SetGroupVersionKind(gvkHCP)
	if err := r.Get(ctx, client.ObjectKey{Namespace: WorkerControlPlaneNamespace(hcu.Namespace, hcu.Name), Name: hcu.Name}, hcp); err != nil {
		return proof, fmt.Errorf("waiting for HostedControlPlane: %w", err)
	}
	effective, _, _ := unstructured.NestedString(hcp.Object, "spec", "releaseImage")
	management, found, managementErr := unstructured.NestedString(hcp.Object, "spec", "controlPlaneReleaseImage")
	if managementErr != nil || (found && !samePayload(management, image)) {
		return proof, fmt.Errorf("HCP management release override conflicts with expected payload")
	}
	observed, _, _ := unstructured.NestedInt64(hc.Object, "status", "controlPlaneVersion", "observedGeneration")
	if hcp.GetUID() == "" || hcp.GetGeneration() < 1 || !samePayload(effective, image) || observed < hcp.GetGeneration() || !hcp.GetDeletionTimestamp().IsZero() {
		return proof, fmt.Errorf("waiting for effective HCP payload and fresh management version status")
	}
	if err := r.aggregateControlPlaneStatus(ctx, hcu, version); err != nil {
		return proof, err
	}
	if hcu.Status.ControlPlaneStatus.TotalComponents == 0 || hcu.Status.ControlPlaneStatus.UpgradedComponents != hcu.Status.ControlPlaneStatus.TotalComponents {
		return proof, fmt.Errorf("waiting for fresh, ready control plane components at %s", version)
	}
	return managementProof{hcUID: hc.GetUID(), hcGeneration: hc.GetGeneration(), hcpUID: hcp.GetUID(), hcpGeneration: hcp.GetGeneration()}, nil
}
