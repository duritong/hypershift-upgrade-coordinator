package controller

import (
	"context"
	"testing"

	upgradev1alpha1 "github.com/duritong/hypershift-upgrade-coordinator/api/v1alpha1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestWorkerPrerequisiteRequiresQuiescenceAfterLock(t *testing.T) {
	hcu := regressionHCU(upgradev1alpha1.PhasePreflightChecks)
	hcu.UID = "upgrade"
	np := regressionNP("workers", "")
	np.SetGeneration(5)
	md := readyWorkerFixture(np, regressionImage("4.20.0"))
	c := &regressionClient{objects: []client.Object{np, md}}
	r := &HostedClusterUpgradeReconciler{Client: c}
	if err := r.WorkerPrerequisite(context.Background(), hcu); err != nil {
		t.Fatal(err)
	}
	if err := r.lockClusterNodePools(context.Background(), hcu); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(np), np); err != nil {
		t.Fatal(err)
	}
	np.SetGeneration(6) // API server's pause-only spec write.
	if err := c.store(np); err != nil {
		t.Fatal(err)
	}
	if err := r.WorkerPrerequisite(context.Background(), hcu); err != nil {
		t.Fatalf("known pending lock mistaken for rollout: %v", err)
	}
	for _, defect := range []string{"config", "platform", "CAPI", "external spec"} {
		t.Run(defect, func(t *testing.T) {
			pool, backing := np.DeepCopy(), md.DeepCopy()
			switch defect {
			case "config":
				_ = unstructured.SetNestedSlice(pool.Object, []interface{}{map[string]interface{}{"type": "UpdatingConfig", "status": "True", "observedGeneration": int64(5)}}, "status", "conditions")
			case "platform":
				conditions, _, _ := unstructured.NestedSlice(pool.Object, "status", "conditions")
				conditions = append(conditions, map[string]interface{}{"type": "UpdatingPlatformMachineTemplate", "status": "True"})
				_ = unstructured.SetNestedSlice(pool.Object, conditions, "status", "conditions")
			case "CAPI":
				backing.SetGeneration(2)
			case "external spec":
				pool.SetGeneration(7)
			}
			c.objects = []client.Object{pool, backing}
			if err := r.WorkerPrerequisite(context.Background(), hcu); err == nil {
				t.Fatal("accepted nonquiescent locked pool")
			}
		})
	}
}

func TestWorkerDynamicEnrollmentLocksQuiescentPools(t *testing.T) {
	hcu := regressionHCU(upgradev1alpha1.PhaseUpgradingNodePools)
	hcu.UID = "upgrade"
	hcu.Spec.NodePoolGroups = []upgradev1alpha1.NodePoolGroupSpec{{Name: "first"}}
	a, later := regressionNP("a", "first"), regressionNP("later", "")
	mdA := readyWorkerFixture(a, regressionImage("4.20.0"))
	mdLater := readyWorkerFixture(later, regressionImage("4.20.0"))
	c := &regressionClient{objects: []client.Object{a, later, mdA, mdLater}}
	r := &HostedClusterUpgradeReconciler{Client: c}
	if _, _, err := r.reconcileNodePools(context.Background(), hcu, regressionHC("4.21.0")); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(later), later); err != nil {
		t.Fatal(err)
	}
	if later.GetAnnotations()[workerLockOwner] != string(hcu.UID) {
		t.Fatal("new waiting pool not locked before starting another group")
	}
}

func TestWorkerExternalRolloutBlocksNewWorkAcrossGroups(t *testing.T) {
	hcu := regressionHCU(upgradev1alpha1.PhaseUpgradingNodePools)
	hcu.UID = "upgrade"
	hcu.Spec.NodePoolGroups = []upgradev1alpha1.NodePoolGroupSpec{{Name: "first"}}
	a, external := regressionNP("a", "first"), regressionNP("external", "")
	md := readyWorkerFixture(a, regressionImage("4.20.0"))
	_ = unstructured.SetNestedField(external.Object, regressionImage("4.20.1"), "spec", "release", "image")
	_ = unstructured.SetNestedSlice(external.Object, []interface{}{map[string]interface{}{"type": "UpdatingVersion", "status": "True"}}, "status", "conditions")
	c := &regressionClient{objects: []client.Object{a, external, md}}
	r := &HostedClusterUpgradeReconciler{Client: c}
	if _, _, err := r.reconcileNodePools(context.Background(), hcu, regressionHC("4.21.0")); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(a), a); err != nil {
		t.Fatal(err)
	}
	image, _, _ := unstructured.NestedString(a.Object, "spec", "release", "image")
	if image == hcu.Spec.TargetReleaseImage {
		t.Fatal("started earlier group while externally enrolled pool was running")
	}
	status := hcu.Status.NodePoolStatuses[1]
	if status.Phase != "Blocked" || len(status.Blockers) == 0 {
		t.Fatalf("external work disguised as waiting: %+v", status)
	}
}

func TestWorkerKnownRolloutWithExternalSpecChangeBlocksNewWork(t *testing.T) {
	hcu := regressionHCU(upgradev1alpha1.PhaseUpgradingNodePools)
	hcu.Spec.RolloutPolicy = &upgradev1alpha1.RolloutPolicySpec{MaxParallelNodePools: 2}
	a, b := regressionNP("a", ""), regressionNP("b", "")
	hcu.Status.NodePoolStatuses = []upgradev1alpha1.NodePoolUpgradeStatus{{Name: "a", Phase: "Upgrading", TargetReleaseImage: hcu.Spec.TargetReleaseImage}}
	c := &regressionClient{objects: append([]client.Object{a}, quiescentWorkerFixtures(b)...)}
	r := &HostedClusterUpgradeReconciler{Client: c}
	if _, _, err := r.reconcileNodePools(context.Background(), hcu, regressionHC("4.21.0")); err != nil {
		t.Fatal(err)
	}
	for _, pool := range []*unstructured.Unstructured{a, b} {
		if err := c.Get(context.Background(), client.ObjectKeyFromObject(pool), pool); err != nil {
			t.Fatal(err)
		}
		image, _, _ := unstructured.NestedString(pool.Object, "spec", "release", "image")
		if image == hcu.Spec.TargetReleaseImage {
			t.Fatal("overwrote outstanding incompatible work or started another pool")
		}
	}
	if hcu.Status.NodePoolStatuses[0].TargetReleaseImage != hcu.Spec.TargetReleaseImage {
		t.Fatal("lost recorded coordinator target")
	}
}
