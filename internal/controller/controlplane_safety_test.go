package controller

import (
	"context"
	"strings"
	"testing"

	upgradev1alpha1 "github.com/duritong/hypershift-upgrade-coordinator/api/v1alpha1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func readyHCP(hc *unstructured.Unstructured) *unstructured.Unstructured {
	hcp := &unstructured.Unstructured{}
	hcp.SetGroupVersionKind(gvkHCP)
	hcp.SetNamespace(WorkerControlPlaneNamespace(hc.GetNamespace(), hc.GetName()))
	hcp.SetName(hc.GetName())
	hcp.SetGeneration(1)
	image, _, _ := unstructured.NestedString(hc.Object, "spec", "release", "image")
	_ = unstructured.SetNestedField(hcp.Object, image, "spec", "releaseImage")
	return hcp
}

func TestControlPlaneInFlightIdentity(t *testing.T) {
	for _, target := range []bool{false, true} {
		hcu := regressionHCU(upgradev1alpha1.PhaseUpgradingControlPlane)
		hcu.Spec.IntermediateReleaseImage = regressionImage("4.21.0")
		hcu.Spec.TargetReleaseImage = regressionImage("4.22.0")
		hc := regressionHC("4.20.0")
		image := regressionImage("4.23.0")
		if target {
			image = hcu.Spec.TargetReleaseImage
		}
		_ = unstructured.SetNestedField(hc.Object, image, "spec", "release", "image")
		c := &regressionClient{objects: []client.Object{hcu, hc}}
		got := reconcileRegression(t, c)
		if c.writes != 0 || (target && got.Status.ControlPlaneStatus.TargetReleaseImage != image) || (!target && got.Status.Phase != upgradev1alpha1.PhaseBlocked) {
			t.Fatalf("in-flight request overwritten or not adopted: %+v writes=%d", got.Status, c.writes)
		}
	}
}

func TestControlPlaneCompletionEvidence(t *testing.T) {
	for _, defect := range []string{"none", "mirror", "different digest", "old completed", "partial", "wrong version", "missing HCP", "stale CPC", "wrong HCP", "stale management"} {
		t.Run(defect, func(t *testing.T) {
			hcu := regressionHCU(upgradev1alpha1.PhaseVerifyingControlPlane)
			hc := regressionHC("4.21.0")
			hcp, cpc := readyHCP(hc), readyStepCPC("4.21.0")
			cpc.SetGeneration(99) // CPC generation is unrelated to freshness.
			switch defect {
			case "mirror":
				_, digest, _ := strings.Cut(hcu.Spec.TargetReleaseImage, "@")
				_ = unstructured.SetNestedField(hc.Object, "mirror/ocp@"+digest, "spec", "release", "image")
			case "different digest":
				_ = unstructured.SetNestedField(hc.Object, "wrong@sha256:"+strings.Repeat("f", 64), "status", "version", "desired", "image")
			case "old completed", "partial":
				history, _, _ := unstructured.NestedSlice(hc.Object, "status", "version", "history")
				history = append([]interface{}{map[string]interface{}{"state": "Partial", "image": hcu.Spec.TargetReleaseImage, "version": "4.21.0"}}, history...)
				_ = unstructured.SetNestedSlice(hc.Object, history, "status", "version", "history")
			case "wrong version":
				_ = unstructured.SetNestedSlice(hc.Object, []interface{}{map[string]interface{}{"state": "Completed", "image": hcu.Spec.TargetReleaseImage, "version": "4.20.0"}}, "status", "version", "history")
			case "stale CPC":
				hcp.SetGeneration(2)
				_ = unstructured.SetNestedField(hc.Object, int64(2), "status", "controlPlaneVersion", "observedGeneration")
			case "wrong HCP":
				_ = unstructured.SetNestedField(hcp.Object, regressionImage("4.20.0"), "spec", "releaseImage")
			case "stale management":
				hcp.SetGeneration(2)
				_ = unstructured.SetNestedField(cpc.Object, int64(2), "status", "observedGeneration")
			}
			c := &regressionClient{objects: []client.Object{hc, cpc}}
			if defect != "missing HCP" {
				c.objects = append(c.objects, hcp)
			}
			err := (&HostedClusterUpgradeReconciler{Client: c}).verifyControlPlane(context.Background(), hcu, hc, hcu.Spec.TargetReleaseImage)
			if (err == nil) != (defect == "none" || defect == "mirror") {
				t.Fatalf("completion evidence %s: %v", defect, err)
			}
		})
	}
}

func TestControlPlaneOverrideRecovery(t *testing.T) {
	for _, phase := range []upgradev1alpha1.UpgradePhase{upgradev1alpha1.PhasePreflightChecks, upgradev1alpha1.PhaseUpgradingControlPlane, upgradev1alpha1.PhaseUpgradingNodePools} {
		hc := regressionHC("4.20.0")
		_ = unstructured.SetNestedField(hc.Object, regressionImage("4.22.0"), "spec", "controlPlaneRelease", "image")
		c := &regressionClient{objects: []client.Object{regressionHCU(phase), hc, regressionNP("workers", "")}}
		got := reconcileRegression(t, c)
		if got.Status.Phase != upgradev1alpha1.PhaseBlocked || c.writes != 0 {
			t.Fatalf("override must block without downstream writes: phase=%s writes=%d", got.Status.Phase, c.writes)
		}
	}
}
