package controller

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	upgradev1alpha1 "github.com/duritong/hypershift-upgrade-coordinator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type indexedPoolClient struct {
	*regressionClient
	fail bool
}

func (c *indexedPoolClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	options := (&client.ListOptions{}).ApplyOptions(opts)
	if options.FieldSelector == nil {
		return fmt.Errorf("missing cluster field selector")
	}
	if c.fail {
		return fmt.Errorf("index unavailable")
	}
	return c.regressionClient.List(ctx, list, opts...)
}

func TestIndexedNodePoolNamespaceAndOrder(t *testing.T) {
	z, a := regressionNP("z", ""), regressionNP("a", "")
	otherCluster, otherNamespace := regressionNP("other", ""), regressionNP("elsewhere", "")
	_ = unstructured.SetNestedField(otherCluster.Object, "another", "spec", "clusterName")
	otherNamespace.SetNamespace("elsewhere")
	c := &indexedPoolClient{regressionClient: &regressionClient{objects: []client.Object{z, otherCluster, otherNamespace, a}}}
	r := &HostedClusterUpgradeReconciler{Client: c}
	for i := 0; i < 2; i++ {
		pools, err := r.listClusterNodePools(context.Background(), "clusters", "example")
		if err != nil || len(pools.Items) != 2 || pools.Items[0].GetName() != "a" || pools.Items[1].GetName() != "z" {
			t.Fatalf("indexed namespace/membership/order: pools=%v err=%v", pools, err)
		}
		c.objects[0], c.objects[3] = c.objects[3], c.objects[0]
	}
	c.fail = true
	if _, err := r.listClusterNodePools(context.Background(), "clusters", "example"); err == nil {
		t.Fatal("indexed list failure silently fell back")
	}
}

func TestCacheIterationStatusOrder(t *testing.T) {
	hcu := regressionHCU(upgradev1alpha1.PhaseUpgradingNodePools)
	z, a := regressionNP("z", ""), regressionNP("a", "")
	for _, np := range []*unstructured.Unstructured{z, a} {
		_ = unstructured.SetNestedField(np.Object, "true", "spec", "pausedUntil")
	}
	cpZ, cpA := &unstructured.Unstructured{}, &unstructured.Unstructured{}
	for i, cp := range []*unstructured.Unstructured{cpZ, cpA} {
		cp.SetGroupVersionKind(gvkCPC)
		cp.SetNamespace(WorkerControlPlaneNamespace(hcu.Namespace, hcu.Name))
		cp.SetName([]string{"z", "a"}[i])
	}
	c := &regressionClient{objects: []client.Object{z, a, cpZ, cpA}}
	r := &HostedClusterUpgradeReconciler{Client: c}
	var previous upgradev1alpha1.HostedClusterUpgradeStatus
	for i := 0; i < 2; i++ {
		if _, _, err := r.reconcileNodePools(context.Background(), hcu, regressionHC("4.21.0")); err != nil {
			t.Fatal(err)
		}
		if err := r.aggregateControlPlaneStatus(context.Background(), hcu, "4.21.0"); err != nil {
			t.Fatal(err)
		}
		if hcu.Status.NodePoolStatuses[0].Name != "a" || hcu.Status.ControlPlaneStatus.Components[0].Name != "a" {
			t.Fatalf("status not sorted: %+v", hcu.Status)
		}
		if i > 0 && !reflect.DeepEqual(previous, hcu.Status) {
			// Observation attempt time is intentionally fresh, not cache ordering.
			previous.GuestAssessment.LastAttemptTime = hcu.Status.GuestAssessment.LastAttemptTime
			if !reflect.DeepEqual(previous, hcu.Status) {
				t.Fatalf("iteration order changed status:\n%+v\n%+v", previous, hcu.Status)
			}
		}
		previous = *hcu.Status.DeepCopy()
		c.objects[0], c.objects[1] = c.objects[1], c.objects[0]
		c.objects[2], c.objects[3] = c.objects[3], c.objects[2]
	}
}

func TestDeletionRestoresDetachedOwnedPool(t *testing.T) {
	hcu := regressionHCU(upgradev1alpha1.PhasePreflightChecks)
	hcu.UID = "upgrade-uid"
	hcu.Finalizers = []string{workerLocksFinalizer}
	now := metav1.Now()
	hcu.DeletionTimestamp = &now
	np := regressionNP("detached", "")
	_ = unstructured.SetNestedField(np.Object, "another-cluster", "spec", "clusterName")
	_ = unstructured.SetNestedField(np.Object, "true", "spec", "pausedUntil")
	np.SetAnnotations(map[string]string{workerLockOwner: string(hcu.UID), workerLockOriginal: "null"})
	c := &regressionClient{objects: []client.Object{hcu, np}}
	got := reconcileRegression(t, c)
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(np), np); err != nil {
		t.Fatal(err)
	}
	if _, found, _ := unstructured.NestedString(np.Object, "spec", "pausedUntil"); found || len(got.Finalizers) != 0 {
		t.Fatal("finalizer failed to restore detached owned pool")
	}
}
