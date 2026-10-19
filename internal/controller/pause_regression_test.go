package controller

import (
	"testing"
	"time"

	upgradev1alpha1 "github.com/duritong/hypershift-upgrade-coordinator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestPauseResumeKeepsActiveWorkerRollouts(t *testing.T) {
	hcu := regressionHCU(upgradev1alpha1.PhaseUpgradingNodePools)
	started := metav1.NewTime(time.Now().Add(-time.Hour))
	hcu.Status.StartTime = &started
	hcu.Status.ControlPlaneStatus.TargetReleaseImage = hcu.Spec.TargetReleaseImage
	hcu.Status.NodePoolStatuses = []upgradev1alpha1.NodePoolUpgradeStatus{{Name: "workers", Phase: "Upgrading", TargetReleaseImage: hcu.Spec.TargetReleaseImage, StartTime: &started}}
	np := regressionNP("workers", "")
	_ = unstructured.SetNestedField(np.Object, hcu.Spec.TargetReleaseImage, "spec", "release", "image")
	paused := "true"
	hcu.Spec.PausedUntil = &paused
	c := &regressionClient{objects: []client.Object{hcu, regressionHC("4.21.0"), np}}
	got := reconcileRegression(t, c)
	if got.Status.Phase != upgradev1alpha1.PhasePaused || c.writes != 0 {
		t.Fatalf("coordinator pause mutated underlying resources: phase=%s writes=%d", got.Status.Phase, c.writes)
	}
	got.Spec.PausedUntil = nil
	if err := c.store(got); err != nil {
		t.Fatal(err)
	}
	got = reconcileRegression(t, c)
	if got.Status.Phase != upgradev1alpha1.PhaseUpgradingNodePools || got.Status.StartTime == nil || !got.Status.StartTime.Equal(&started) || c.writes != 0 {
		t.Fatalf("resume restarted or relocked active rollout: status=%+v writes=%d", got.Status, c.writes)
	}
}

func TestPauseObservesWorkerCompletion(t *testing.T) {
	hcu := regressionHCU(upgradev1alpha1.PhaseUpgradingNodePools)
	paused := "true"
	hcu.Spec.PausedUntil = &paused
	hcu.Status.NodePoolStatuses = []upgradev1alpha1.NodePoolUpgradeStatus{{Name: "workers", Phase: "Upgrading", TargetReleaseImage: hcu.Spec.TargetReleaseImage}}
	np := regressionNP("workers", "")
	_ = unstructured.SetNestedField(np.Object, hcu.Spec.TargetReleaseImage, "spec", "release", "image")
	_ = unstructured.SetNestedField(np.Object, "4.21.0", "status", "version")
	_ = unstructured.SetNestedSlice(np.Object, []interface{}{map[string]interface{}{
		"ocpVersion": "4.21.0", "readyNodeCount": int64(1), "unreadyNodeCount": int64(0),
	}}, "status", "nodesInfo", "nodeVersions")
	c := &regressionClient{objects: []client.Object{hcu, regressionHC("4.21.0"), np}}
	got := reconcileRegression(t, c)
	if got.Status.Phase != upgradev1alpha1.PhasePaused || c.writes != 0 || got.Status.AggregatedNodeStatus.Ready != 1 || got.Status.NodePoolStatuses[0].Phase != "Completed" {
		t.Fatalf("pause did not observe worker completion without actuation: status=%+v writes=%d", got.Status, c.writes)
	}
}
