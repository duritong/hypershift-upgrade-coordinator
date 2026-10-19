package controller

import (
	"testing"

	upgradev1alpha1 "github.com/duritong/hypershift-upgrade-coordinator/api/v1alpha1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

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
