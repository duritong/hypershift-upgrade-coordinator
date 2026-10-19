package controller

import (
	"reflect"
	"testing"

	upgradev1alpha1 "github.com/duritong/hypershift-upgrade-coordinator/api/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestPersistentWorkerFailureRemainsBlocked(t *testing.T) {
	hcu := regressionHCU(upgradev1alpha1.PhaseUpgradingNodePools)
	hcu.Status.ControlPlaneStatus.TargetReleaseImage = hcu.Spec.TargetReleaseImage
	np := regressionNP("workers", "unknown")
	c := &regressionClient{objects: []client.Object{hcu, regressionHC("4.21.0"), np}}
	var previous *upgradev1alpha1.HostedClusterUpgrade
	for range 5 {
		got := reconcileRegression(t, c)
		if got.Status.Phase != upgradev1alpha1.PhaseBlocked || got.Status.PhaseBeforeBlock != upgradev1alpha1.PhaseUpgradingNodePools {
			t.Fatalf("unresolved group error restarted workflow: %+v", got.Status)
		}
		if previous != nil && !reflect.DeepEqual(previous.Status, got.Status) {
			t.Fatal("unchanged blocker churned status")
		}
		previous = got
	}
	if c.writes != 0 {
		t.Fatal("persistent worker failure relocked or mutated workers")
	}
	np.SetLabels(nil)
	if err := c.store(np); err != nil {
		t.Fatal(err)
	}
	got := reconcileRegression(t, c)
	if got.Status.Phase != upgradev1alpha1.PhaseUpgradingNodePools {
		t.Fatalf("resolved blocker did not resume the failed worker step: %+v", got.Status)
	}
}
