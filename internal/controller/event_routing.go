package controller

import (
	"context"
	"reflect"
	"sort"
	"strings"

	upgradev1 "github.com/duritong/hypershift-upgrade-coordinator/api/v1alpha1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const controlPlaneNamespaceField = "controlPlaneNamespace"
const nodePoolClusterField = "spec.clusterName"

// Only the primary HCU watch uses this filter. Own observations are refreshed by
// RequeueAfter, not by feeding their timestamps/diagnostics back into the queue.
// Phase changes still drive the state machine immediately, and the independent
// access writer can wake it when credential availability or identity changes.
var upgradeEventPredicate = predicate.Funcs{UpdateFunc: func(e event.UpdateEvent) bool {
	old, oldOK := e.ObjectOld.(*upgradev1.HostedClusterUpgrade)
	current, currentOK := e.ObjectNew.(*upgradev1.HostedClusterUpgrade)
	if !oldOK || !currentOK || old == nil || current == nil {
		return false
	}
	if old.UID != current.UID || old.Generation != current.Generation || !old.DeletionTimestamp.Equal(current.DeletionTimestamp) || !reflect.DeepEqual(old.Spec, current.Spec) || old.Status.Phase != current.Status.Phase || !reflect.DeepEqual(old.Status.GuestAccess, current.Status.GuestAccess) {
		return true
	}
	// Group advancement may require the next reconcile after a pool completes,
	// without changing the overall worker phase. Counts/blocker messages do not.
	if len(old.Status.NodePoolStatuses) != len(current.Status.NodePoolStatuses) {
		return true
	}
	phases := make(map[string]string, len(old.Status.NodePoolStatuses))
	for _, pool := range old.Status.NodePoolStatuses {
		phases[pool.Name] = pool.Phase
	}
	for _, pool := range current.Status.NodePoolStatuses {
		if phase, exists := phases[pool.Name]; !exists || phase != pool.Phase {
			return true
		}
	}
	for _, typ := range []string{"GuestAccessMaintenanceReady", "GuestCredentialAvailable"} {
		before := meta.FindStatusCondition(old.Status.Conditions, typ)
		after := meta.FindStatusCondition(current.Status.Conditions, typ)
		if before == nil || after == nil {
			if before != after {
				return true
			}
			continue
		}
		if before.Status != after.Status || before.Reason != after.Reason || before.ObservedGeneration != after.ObservedGeneration {
			return true
		}
	}
	return false
}}

func nodePoolClusterIndex(obj client.Object) []string {
	np := obj.(*unstructured.Unstructured)
	cluster, _, _ := unstructured.NestedString(np.Object, "spec", "clusterName")
	if cluster == "" {
		return nil
	}
	return []string{cluster}
}

func (r *HostedClusterUpgradeReconciler) listClusterNodePools(ctx context.Context, namespace, cluster string) (*unstructured.UnstructuredList, error) {
	pools := &unstructured.UnstructuredList{}
	pools.SetGroupVersionKind(gvkNP)
	if err := r.List(ctx, pools, client.InNamespace(namespace), client.MatchingFields{nodePoolClusterField: cluster}); err != nil {
		return nil, err
	}
	sort.Slice(pools.Items, func(i, j int) bool { return pools.Items[i].GetName() < pools.Items[j].GetName() })
	return pools, nil
}

func (r *HostedClusterUpgradeReconciler) mapWorkerToHostedClusterUpgrade(ctx context.Context, obj client.Object) []reconcile.Request {
	// The upstream annotation identifies the NodePool, not the HostedCluster.
	if namespace, name, ok := strings.Cut(obj.GetAnnotations()[workerPoolAnnotation], "/"); ok && namespace != "" && name != "" {
		if r.HostedClusterNamespace != "" && namespace != r.HostedClusterNamespace {
			return nil
		}
		np := &unstructured.Unstructured{}
		np.SetGroupVersionKind(gvkNP)
		if err := r.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, np); err == nil {
			return r.mapNodePoolToHostedClusterUpgrade(ctx, np)
		}
	}
	// HCP-owned workers (and deletion events after their pool disappears) still
	// identify the control-plane namespace. Never attempt to split it.
	return r.mapCPCToHostedClusterUpgrade(ctx, obj)
}
