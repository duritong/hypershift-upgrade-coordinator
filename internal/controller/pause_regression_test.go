package controller

import (
	"context"
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
	md := readyWorkerFixture(np, hcu.Spec.TargetReleaseImage)
	c := &regressionClient{objects: []client.Object{hcu, regressionHC("4.21.0"), np, md}}
	got := reconcileRegression(t, c)
	if got.Status.Phase != upgradev1alpha1.PhasePaused || c.writes != 0 || got.Status.AggregatedNodeStatus.Ready != 1 || got.Status.NodePoolStatuses[0].Phase != "Completed" {
		t.Fatalf("pause did not observe worker completion without actuation: status=%+v writes=%d", got.Status, c.writes)
	}
}

func TestPausedTargetChangeUpgradesControlPlaneFirst(t *testing.T) {
	hcu := regressionHCU(upgradev1alpha1.PhaseUpgradingNodePools)
	hcu.Status.ControlPlaneStatus.TargetReleaseImage = hcu.Spec.TargetReleaseImage
	paused := "true"
	hcu.Spec.PausedUntil = &paused
	np := regressionNP("workers", "")
	c := &regressionClient{objects: []client.Object{hcu, regressionHC("4.21.0"), np}}
	got := reconcileRegression(t, c)
	got.Spec.TargetReleaseImage = regressionImage("4.22.0")
	got.Spec.PausedUntil = nil
	if err := c.store(got); err != nil {
		t.Fatal(err)
	}
	got = reconcileRegression(t, c)
	if got.Status.Phase == upgradev1alpha1.PhasePendingSchedule {
		got = reconcileRegression(t, c)
		got = reconcileRegression(t, c)
	}
	if got.Status.Phase != upgradev1alpha1.PhaseVerifyingControlPlane || got.Status.ControlPlaneStatus.TargetReleaseImage != got.Spec.TargetReleaseImage {
		t.Fatalf("new target bypassed control plane: %+v", got.Status)
	}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(np), np); err != nil {
		t.Fatal(err)
	}
	image, _, _ := unstructured.NestedString(np.Object, "spec", "release", "image")
	if image == got.Spec.TargetReleaseImage {
		t.Fatal("workers advanced before control plane verification")
	}
}

func TestChangedActivePlanRespectsControlPlaneSchedule(t *testing.T) {
	hcu := regressionHCU(upgradev1alpha1.PhaseUpgradingNodePools)
	hcu.Status.ControlPlaneStatus.TargetReleaseImage = hcu.Spec.TargetReleaseImage
	hcu.Spec.TargetReleaseImage = regressionImage("4.22.0")
	future := metav1.NewTime(time.Now().Add(time.Hour))
	hcu.Spec.ControlPlane.StartAt = &future
	hc := regressionHC("4.21.0")
	c := &regressionClient{objects: []client.Object{hcu, hc}}
	for range 2 {
		got := reconcileRegression(t, c)
		if got.Status.Phase != upgradev1alpha1.PhasePendingSchedule {
			t.Fatalf("changed plan bypassed future control plane schedule: %+v", got.Status)
		}
	}
	if c.writes != 0 {
		t.Fatal("changed plan actuated before schedule opened")
	}
}

func TestResumeVerifiesTargetWhenIntermediatePassed(t *testing.T) {
	hcu := regressionHCU(upgradev1alpha1.PhaseVerifyingControlPlane)
	hcu.Spec.IntermediateReleaseImage = regressionImage("4.21.0")
	hcu.Spec.TargetReleaseImage = regressionImage("4.22.0")
	hcu.Status.ControlPlaneStatus.TargetReleaseImage = hcu.Spec.IntermediateReleaseImage
	paused := "true"
	hcu.Spec.PausedUntil = &paused
	c := &regressionClient{objects: []client.Object{hcu, regressionHC("4.21.0"), readyStepCPC("4.21.0")}}
	got := reconcileRegression(t, c)
	if err := c.store(regressionHC("4.22.0")); err != nil {
		t.Fatal(err)
	}
	if err := c.store(readyStepCPC("4.22.0")); err != nil {
		t.Fatal(err)
	}
	got.Spec.PausedUntil = nil
	if err := c.store(got); err != nil {
		t.Fatal(err)
	}
	got = reconcileRegression(t, c)
	if got.Status.Phase != upgradev1alpha1.PhaseUpgradingNodePools || got.Status.ControlPlaneStatus.TargetReleaseImage != got.Spec.TargetReleaseImage {
		t.Fatalf("recovery stuck verifying passed intermediate: %+v", got.Status)
	}
}
