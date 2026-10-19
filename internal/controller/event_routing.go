package controller

import (
	"context"
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const controlPlaneNamespaceField = "controlPlaneNamespace"

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
