package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
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
const workerLockGeneration = "upgrade.huc.0xshift.dev/worker-lock-quiescent-generation"

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

// assessWorkerQuiescence ignores only the coordinator's proven pause-only spec
// write. Any subsequent generation change invalidates that proof. A lock write
// cannot cancel work already delegated to CAPI/HCCO.
func (r *HostedClusterUpgradeReconciler) assessWorkerQuiescence(ctx context.Context, hcu *upgradev1alpha1.HostedClusterUpgrade, np *unstructured.Unstructured) (workerAssessment, error) {
	observed := np.DeepCopy()
	_, owned, err := nodePoolPause(np, hcu)
	if err != nil {
		return workerAssessment{}, err
	}
	pause, _, _ := unstructured.NestedString(np.Object, "spec", "pausedUntil")
	baseline, err := strconv.ParseInt(np.GetAnnotations()[workerLockGeneration], 10, 64)
	if owned && pause == "true" && err == nil && np.GetGeneration() >= baseline && np.GetGeneration() <= baseline+1 {
		observed.SetGeneration(baseline)
	}
	image, _, _ := unstructured.NestedString(np.Object, "spec", "release", "image")
	return r.assessWorker(ctx, observed, hcu.Name, image)
}

// WorkerPrerequisite must be called immediately before a new control-plane
// mutation, even when workers were locked earlier. It makes no rollout changes.
// External writers must coordinate ownership operationally: informer observations
// and optimistic writes cannot prevent an external write racing this check.
func (r *HostedClusterUpgradeReconciler) WorkerPrerequisite(ctx context.Context, hcu *upgradev1alpha1.HostedClusterUpgrade) error {
	pools := &unstructured.UnstructuredList{}
	pools.SetGroupVersionKind(gvkNP)
	if err := r.List(ctx, pools, client.InNamespace(hcu.Namespace)); err != nil {
		return err
	}
	for _, np := range pools.Items {
		cluster, _, _ := unstructured.NestedString(np.Object, "spec", "clusterName")
		if cluster != hcu.Name {
			continue
		}
		a, err := r.assessWorkerQuiescence(ctx, hcu, &np)
		if err != nil {
			return err
		}
		if !a.complete {
			return fmt.Errorf("NodePool %q is not positively quiescent: %s", np.GetName(), a.reason)
		}
	}
	return nil
}

func (r *HostedClusterUpgradeReconciler) acquireWorkerLock(ctx context.Context, hcu *upgradev1alpha1.HostedClusterUpgrade, np *unstructured.Unstructured, pause *string) error {
	a, err := r.assessWorkerQuiescence(ctx, hcu, np)
	if err != nil {
		return err
	}
	original, _ := json.Marshal(pause)
	annotations := np.GetAnnotations()
	if annotations == nil {
		annotations = map[string]string{}
	}
	annotations[workerLockOwner] = string(hcu.UID)
	annotations[workerLockOriginal] = string(original)
	delete(annotations, workerLockGeneration)
	if a.complete {
		annotations[workerLockGeneration] = strconv.FormatInt(np.GetGeneration(), 10)
	}
	np.SetAnnotations(annotations)
	_ = unstructured.SetNestedField(np.Object, "true", "spec", "pausedUntil")
	return r.Update(ctx, np)
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
	if replicas, found, _ := unstructured.NestedInt64(np.Object, "spec", "replicas"); found && replicas != desired {
		return a, nil
	}
	if int32(desired) > a.total {
		a.total = int32(desired)
	}
	// Only CAPI can establish that an autoscaled pool intentionally has no machines.
	if desired == 0 {
		versions, _, _ := unstructured.NestedSlice(np.Object, "status", "nodesInfo", "nodeVersions")
		for _, item := range versions {
			v, _ := item.(map[string]interface{})
			r, _, _ := unstructured.NestedInt64(v, "readyNodeCount")
			u, _, _ := unstructured.NestedInt64(v, "unreadyNodeCount")
			if r != 0 || u != 0 {
				return a, nil
			}
		}
		a.total = 0
	}
	observed, found, _ := unstructured.NestedInt64(backing.Object, "status", "observedGeneration")
	if !found || observed < backing.GetGeneration() {
		return a, nil
	}
	for _, field := range []string{"replicas", "readyReplicas", "availableReplicas"} {
		// CAPI's scalar v1beta1 zero counts are omitted from JSON.
		value, found, err := unstructured.NestedInt64(backing.Object, "status", field)
		if err != nil || (!found && desired != 0) || value != desired {
			return a, nil
		}
	}
	if strategy == "Replace" {
		value, found, err := unstructured.NestedInt64(backing.Object, "status", "updatedReplicas")
		if err != nil || (!found && desired != 0) || value != desired {
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
