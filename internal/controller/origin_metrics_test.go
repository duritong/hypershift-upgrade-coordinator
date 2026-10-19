package controller

import (
	"testing"

	upgradev1alpha1 "github.com/duritong/hypershift-upgrade-coordinator/api/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestUpgradeVersionLabelsKeepOrigin(t *testing.T) {
	metricPhase.Reset()
	t.Cleanup(metricPhase.Reset)
	hcu := regressionHCU(upgradev1alpha1.PhaseVerifyingControlPlane)
	hcu.Status.ControlPlaneStatus.TargetReleaseImage = hcu.Spec.TargetReleaseImage
	c := &regressionClient{objects: []client.Object{hcu, regressionHC("4.20.0")}}
	reconcileRegression(t, c)
	if err := c.store(regressionHC("4.21.0")); err != nil {
		t.Fatal(err)
	}
	reconcileRegression(t, c)
	metrics := collectedGauges(t, metricPhase)
	if len(metrics) != 1 {
		t.Fatalf("unexpected phase series: %v", metrics)
	}
	for _, label := range metrics[0].Label {
		if label.GetName() == "current_version" && label.GetValue() != "4.20.0" {
			t.Fatalf("origin label changed after CP advanced: %s", label.GetValue())
		}
	}
}
