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

func TestWorkerOutstandingWorkReservesConcurrency(t *testing.T) {
	for _, oldTarget := range []bool{false, true} {
		t.Run(map[bool]string{false: "paused", true: "previous target"}[oldTarget], func(t *testing.T) {
			hcu := regressionHCU(upgradev1alpha1.PhaseUpgradingNodePools)
			a, b := regressionNP("a", ""), regressionNP("b", "")
			image := hcu.Spec.TargetReleaseImage
			if oldTarget {
				image = regressionImage("4.20.1")
			}
			_ = unstructured.SetNestedField(a.Object, image, "spec", "release", "image")
			_ = unstructured.SetNestedField(a.Object, "true", "spec", "pausedUntil")
			started := metav1.NewTime(time.Now().Add(-time.Hour))
			hcu.Status.NodePoolStatuses = []upgradev1alpha1.NodePoolUpgradeStatus{{Name: "a", Phase: "Upgrading", TargetReleaseImage: image, StartTime: &started}}
			c := &regressionClient{objects: append([]client.Object{a}, quiescentWorkerFixtures(b)...)}
			addReadyCP(c, regressionHC("4.21.0"))
			r := &HostedClusterUpgradeReconciler{Client: c}
			if _, _, err := r.reconcileNodePools(context.Background(), hcu, regressionHC("4.21.0")); err != nil {
				t.Fatal(err)
			}
			if c.writes != 1 { // Enrollment lock only; no new release work.
				t.Fatal("started new work while paused rollout remained outstanding")
			}
			status := hcu.Status.NodePoolStatuses[0]
			if status.StartTime == nil || !status.StartTime.Equal(&started) || status.TargetReleaseImage != image {
				t.Fatalf("lost persisted work across plan change: %+v", status)
			}
			if hcu.Status.NodePoolStatuses[1].Phase != "WaitingForConcurrency" {
				t.Fatalf("reservation ignored: %+v", hcu.Status)
			}
		})
	}
}
