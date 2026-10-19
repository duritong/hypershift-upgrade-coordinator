package controller

import (
	"context"
	"testing"

	upgradev1alpha1 "github.com/duritong/hypershift-upgrade-coordinator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestDeletionRestoresOwnedWorkerLocks(t *testing.T) {
	hcu := regressionHCU(upgradev1alpha1.PhasePreflightChecks)
	hcu.UID = "upgrade-uid"
	hcu.Finalizers = []string{"upgrade.huc.0xshift.dev/worker-locks"}
	owned := regressionNP("owned", "")
	user := regressionNP("user", "")
	_ = unstructured.SetNestedField(user.Object, "true", "spec", "pausedUntil")
	c := &regressionClient{objects: []client.Object{hcu, owned, user}}
	r := &HostedClusterUpgradeReconciler{Client: c, APIReader: workerSource(c, hcu)}
	if err := r.lockClusterNodePools(context.Background(), hcu, regressionHC("4.21.0")); err != nil {
		t.Fatal(err)
	}
	now := metav1.Now()
	hcu.DeletionTimestamp = &now
	if err := c.store(hcu); err != nil {
		t.Fatal(err)
	}
	got := reconcileRegression(t, c)
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(owned), owned); err != nil {
		t.Fatal(err)
	}
	if _, found, _ := unstructured.NestedString(owned.Object, "spec", "pausedUntil"); found || owned.GetAnnotations()[workerLockOwner] != "" {
		t.Fatalf("deleted upgrade stranded owned worker: %+v", owned.Object)
	}
	if len(got.Finalizers) != 0 {
		t.Fatalf("cleanup retained finalizer: %v", got.Finalizers)
	}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(user), user); err != nil {
		t.Fatal(err)
	}
	pause, _, _ := unstructured.NestedString(user.Object, "spec", "pausedUntil")
	if pause != "true" {
		t.Fatal("deletion cleared independent user pause")
	}
}

func TestDeletionRetainsFinalizerOnRestoreFailure(t *testing.T) {
	hcu := regressionHCU(upgradev1alpha1.PhasePreflightChecks)
	hcu.UID = "upgrade-uid"
	hcu.Finalizers = []string{"upgrade.huc.0xshift.dev/worker-locks"}
	np := regressionNP("workers", "")
	base := &regressionClient{objects: []client.Object{hcu, np}}
	r := &HostedClusterUpgradeReconciler{Client: base, APIReader: workerSource(base, hcu)}
	if err := r.lockClusterNodePools(context.Background(), hcu, regressionHC("4.21.0")); err != nil {
		t.Fatal(err)
	}
	now := metav1.Now()
	hcu.DeletionTimestamp = &now
	if err := base.store(hcu); err != nil {
		t.Fatal(err)
	}
	r.Client = &stepUpdateFailureClient{regressionClient: base, failName: "workers"}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(hcu)}); err == nil {
		t.Fatal("worker restore failure did not retry deletion")
	}
	if err := base.Get(context.Background(), client.ObjectKeyFromObject(hcu), hcu); err != nil {
		t.Fatal(err)
	}
	if len(hcu.Finalizers) == 0 {
		t.Fatal("finalizer removed before locks were restored")
	}
}
