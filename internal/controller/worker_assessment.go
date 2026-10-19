package controller

import (
	"context"
	"fmt"
	"strings"

	upgradev1alpha1 "github.com/duritong/hypershift-upgrade-coordinator/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var gvkMachineDeployment = schema.GroupVersionKind{Group: "cluster.x-k8s.io", Version: "v1beta1", Kind: "MachineDeployment"}
var gvkMachineSet = schema.GroupVersionKind{Group: "cluster.x-k8s.io", Version: "v1beta1", Kind: "MachineSet"}

const workerPoolAnnotation = "hypershift.openshift.io/nodePool"
const workerCurrentConfig = "hypershift.openshift.io/nodePoolCurrentConfigVersion"
const workerTargetConfig = "hypershift.openshift.io/nodePoolTargetConfigVersion"

// WorkerControlPlaneNamespace matches HyperShift's namespace naming, not its raw HC name.
func WorkerControlPlaneNamespace(namespace, name string) string {
	return namespace + "-" + strings.ReplaceAll(name, ".", "-")
}

func workerPlatformSupported(np *unstructured.Unstructured) error {
	platform, _, _ := unstructured.NestedString(np.Object, "spec", "platform", "type")
	provider, _, _ := unstructured.NestedString(np.Object, "spec", "platform", "ibmcloud", "providerType")
	if platform == "None" || (platform == "IBMCloud" && provider == "UPI") {
		return fmt.Errorf("NodePool %q uses unsupported manual machine-less platform %s; automated worker completion cannot be proven", np.GetName(), platform)
	}
	return nil
}

func workerCondition(np *unstructured.Unstructured, typ, status string, generation int64) bool {
	conditions, _, _ := unstructured.NestedSlice(np.Object, "status", "conditions")
	for _, item := range conditions {
		c, ok := item.(map[string]interface{})
		if !ok || c["type"] != typ {
			continue
		}
		observed, found, _ := unstructured.NestedInt64(c, "observedGeneration")
		return c["status"] == status && found && observed >= generation
	}
	return false
}

type workerAssessment struct {
	total, updated, ready int32
	complete              bool
	reason                string
}

// assessWorker requires positive, fresh evidence from both HyperShift and CAPI.
// No status.version-only or missing-condition completion is safe, including zero replicas.
func (r *HostedClusterUpgradeReconciler) assessWorker(ctx context.Context, np *unstructured.Unstructured, clusterName, image string) (workerAssessment, error) {
	total, updated, ready := GetNodePoolNodeCounts(np, image)
	a := workerAssessment{total: total, updated: updated, ready: ready, reason: "Waiting for fresh NodePool and CAPI rollout completion"}
	if err := workerPlatformSupported(np); err != nil {
		return a, err
	}
	strategy, _, _ := unstructured.NestedString(np.Object, "spec", "management", "upgradeType")
	backing := &unstructured.Unstructured{}
	backing.SetGroupVersionKind(gvkMachineDeployment)
	if strategy == "InPlace" {
		backing.SetGroupVersionKind(gvkMachineSet)
	} else if strategy != "Replace" {
		a.reason = "Unsupported or missing worker upgrade strategy"
		return a, nil
	}
	err := r.Get(ctx, client.ObjectKey{Namespace: WorkerControlPlaneNamespace(np.GetNamespace(), clusterName), Name: np.GetName()}, backing)
	if apierrors.IsNotFound(err) {
		a.reason = "Waiting for authoritative CAPI " + backing.GetKind()
		return a, nil
	}
	if err != nil {
		return a, err
	}
	if backing.GetAnnotations()[workerPoolAnnotation] != client.ObjectKeyFromObject(np).String() || !backing.GetDeletionTimestamp().IsZero() {
		a.reason = "CAPI worker ownership mismatch or deletion in progress"
		return a, nil
	}
	desired, found, _ := unstructured.NestedInt64(backing.Object, "spec", "replicas")
	if !found || desired < 0 {
		return a, nil
	}
	if int32(desired) > a.total {
		a.total = int32(desired)
	}
	// Only CAPI can establish that an autoscaled pool intentionally has no machines.
	if desired == 0 {
		a.total = 0
	}
	observed, found, _ := unstructured.NestedInt64(backing.Object, "status", "observedGeneration")
	if !found || observed < backing.GetGeneration() {
		return a, nil
	}
	for _, field := range []string{"replicas", "readyReplicas", "availableReplicas"} {
		value, found, _ := unstructured.NestedInt64(backing.Object, "status", field)
		if !found || value != desired {
			return a, nil
		}
	}
	if strategy == "Replace" {
		value, found, _ := unstructured.NestedInt64(backing.Object, "status", "updatedReplicas")
		if !found || value != desired {
			return a, nil
		}
	}
	if native, found, _ := unstructured.NestedMap(backing.Object, "status", "v1beta2"); found {
		fields := []string{"readyReplicas", "availableReplicas"}
		if strategy == "Replace" {
			fields = append(fields, "upToDateReplicas")
		}
		for _, field := range fields {
			value, found, _ := unstructured.NestedInt64(native, field)
			if !found || value != desired {
				return a, nil
			}
		}
	}
	specImage, _, _ := unstructured.NestedString(np.Object, "spec", "release", "image")
	version, _, _ := unstructured.NestedString(np.Object, "status", "version")
	target, err := upgradev1alpha1.ExtractVersion(image)
	if err != nil || specImage != image || version != target {
		return a, nil
	}
	for typ, status := range map[string]string{"Ready": "True", "UpdatingVersion": "False", "UpdatingConfig": "False"} {
		if !workerCondition(np, typ, status, np.GetGeneration()) {
			return a, nil
		}
	}
	conditions, _, _ := unstructured.NestedSlice(np.Object, "status", "conditions")
	for _, item := range conditions {
		c, _ := item.(map[string]interface{})
		if strings.HasPrefix(fmt.Sprint(c["type"]), "Updating") && c["status"] == "True" {
			return a, nil
		}
	}
	hash := np.GetAnnotations()[workerCurrentConfig]
	bootstrap, _, _ := unstructured.NestedString(backing.Object, "spec", "template", "spec", "bootstrap", "dataSecretName")
	if hash == "" || bootstrap != "user-data-"+np.GetName()+"-"+hash {
		return a, nil
	}
	if strategy == "InPlace" {
		// HCCO writes current=target only after every guest node completes its MCD
		// config hash. This is stronger than per-Machine version annotations, whose
		// last-machine update can be skipped by HCCO's early completion return.
		if backing.GetAnnotations()[workerTargetConfig] != hash || backing.GetAnnotations()[workerCurrentConfig] != hash {
			return a, nil
		}
		var healthy, unhealthy int64
		versions, _, _ := unstructured.NestedSlice(np.Object, "status", "nodesInfo", "nodeVersions")
		for _, item := range versions {
			v, _ := item.(map[string]interface{})
			r, _, _ := unstructured.NestedInt64(v, "readyNodeCount")
			u, _, _ := unstructured.NestedInt64(v, "unreadyNodeCount")
			healthy += r
			unhealthy += u
		}
		if healthy != desired || unhealthy != 0 {
			return a, nil
		}
		a.updated, a.ready = int32(desired), int32(desired)
	}
	if a.updated != int32(desired) || a.ready != int32(desired) || a.total != int32(desired) {
		return a, nil
	}
	a.complete, a.reason = true, ""
	return a, nil
}
