package controller

import (
	"context"
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const controlPlaneNamespaceField = "controlPlaneNamespace"
const nodePoolClusterField = "spec.clusterName"

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
