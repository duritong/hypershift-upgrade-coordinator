package controller

import (
	"context"
	"fmt"
	"strings"

	upgradev1alpha1 "github.com/duritong/hypershift-upgrade-coordinator/api/v1alpha1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
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

// Payload identity is independent of registry mirrors and human-readable tags.
// Never fall back to version equality when comparing immutable payloads.
func samePayload(a, b string) bool {
	_, ad, aok := strings.Cut(a, "@")
	_, bd, bok := strings.Cut(b, "@")
	return aok && bok && ad != "" && ad == bd
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
	if err := upgradev1alpha1.RejectControlPlaneReleaseOverride(hc); err != nil {
		return err
	}
	if installing, err := r.checkClusterInstallingAndHealth(hc); err != nil {
		return err
	} else if installing {
		return fmt.Errorf("HostedCluster installation is not complete")
	}
	version, err := upgradev1alpha1.ExtractVersion(image)
	if err != nil {
		baseline, baselineVersion := GetHostedClusterCurrentRelease(hc)
		if !samePayload(baseline, image) || baselineVersion == "" {
			return err
		}
		version = baselineVersion
	}
	desired, _, _ := unstructured.NestedString(hc.Object, "spec", "release", "image")
	if !samePayload(desired, image) {
		return fmt.Errorf("HostedCluster requested payload does not match expected release")
	}
	for _, field := range []string{"version", "controlPlaneVersion"} {
		latest := latestRelease(hc, field)
		completed, _ := latest["image"].(string)
		if latest["state"] != "Completed" || latest["version"] != version || !samePayload(completed, image) {
			return fmt.Errorf("waiting for latest %s Completed history with exact expected payload and version", field)
		}
		if requested, _, _ := unstructured.NestedString(hc.Object, "status", field, "desired", "image"); requested != "" && !samePayload(requested, image) {
			return fmt.Errorf("%s desired payload conflicts with expected release", field)
		}
	}
	hcp := &unstructured.Unstructured{}
	hcp.SetGroupVersionKind(gvkHCP)
	if err := r.Get(ctx, client.ObjectKey{Namespace: WorkerControlPlaneNamespace(hcu.Namespace, hcu.Name), Name: hcu.Name}, hcp); err != nil {
		return fmt.Errorf("waiting for HostedControlPlane: %w", err)
	}
	effective, _, _ := unstructured.NestedString(hcp.Object, "spec", "releaseImage")
	observed, _, _ := unstructured.NestedInt64(hc.Object, "status", "controlPlaneVersion", "observedGeneration")
	if hcp.GetGeneration() < 1 || !samePayload(effective, image) || observed < hcp.GetGeneration() {
		return fmt.Errorf("waiting for effective HCP payload and fresh management version status")
	}
	if err := r.aggregateControlPlaneStatus(ctx, hcu, version); err != nil {
		return err
	}
	if hcu.Status.ControlPlaneStatus.TotalComponents == 0 || hcu.Status.ControlPlaneStatus.UpgradedComponents != hcu.Status.ControlPlaneStatus.TotalComponents {
		return fmt.Errorf("waiting for fresh, ready control plane components at %s", version)
	}
	return nil
}
