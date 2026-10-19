package controller

import (
	"context"
	"testing"
	"time"

	upgradev1alpha1 "github.com/duritong/hypershift-upgrade-coordinator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestActiveStepsHaveObservabilityHeartbeat(t *testing.T) {
	for _, paused := range []bool{false, true} {
		t.Run(map[bool]string{false: "active", true: "paused"}[paused], func(t *testing.T) {
			hcu := regressionHCU(upgradev1alpha1.PhaseUpgradingNodePools)
			started := metav1.NewTime(time.Now().Add(-45 * time.Minute))
			hcu.Status.NodePoolStatuses = []upgradev1alpha1.NodePoolUpgradeStatus{{Name: "workers", Phase: "Upgrading", TargetReleaseImage: hcu.Spec.TargetReleaseImage, StartTime: &started}}
			if paused {
				pause := "true"
				hcu.Spec.PausedUntil = &pause
			}
			np := regressionNP("workers", "")
			md := readyWorkerFixture(np, regressionImage("4.20.0"))
			_ = unstructured.SetNestedField(np.Object, hcu.Spec.TargetReleaseImage, "spec", "release", "image")
			np.SetGeneration(2)
			c := &regressionClient{objects: []client.Object{hcu, regressionHC("4.21.0"), np, md}}
			addReadyCP(c, regressionHC("4.21.0"))
			r := &HostedClusterUpgradeReconciler{Client: c, ReleaseResolver: fixtureReleaseResolver{}}
			result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(hcu)})
			if err != nil {
				t.Fatal(err)
			}
			if result.RequeueAfter <= 0 || result.RequeueAfter > 30*time.Second {
				t.Fatalf("active step has no bounded refresh: %+v", result)
			}
		})
	}
}
