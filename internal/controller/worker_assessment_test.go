package controller

import (
	"context"
	"strings"
	"testing"

	upgradev1alpha1 "github.com/duritong/hypershift-upgrade-coordinator/api/v1alpha1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestWorkerCountsDoNotConfuseAvailableWithPopulation(t *testing.T) {
	np := regressionNP("workers", "")
	_ = unstructured.SetNestedField(np.Object, int64(0), "status", "replicas")
	_ = unstructured.SetNestedMap(np.Object, map[string]interface{}{"min": int64(0), "max": int64(5)}, "spec", "autoScaling")
	_ = unstructured.SetNestedSlice(np.Object, []interface{}{
		map[string]interface{}{"ocpVersion": "4.20.0", "readyNodeCount": int64(1)},
		map[string]interface{}{"ocpVersion": "4.21.0", "unreadyNodeCount": int64(1)},
	}, "status", "nodesInfo", "nodeVersions")
	total, updated, ready := GetNodePoolNodeCounts(np, regressionImage("4.21.0"))
	if total != 2 || updated != 1 || ready != 0 {
		t.Fatalf("counts = %d/%d/%d, want population 2, updated 1, target-ready 0", total, updated, ready)
	}
	unstructured.RemoveNestedField(np.Object, "status", "nodesInfo")
	total, _, _ = GetNodePoolNodeCounts(np, regressionImage("4.21.0"))
	if total == 0 {
		t.Fatal("unavailable autoscaled pool was reported as intentionally empty")
	}
}

func readyWorkerFixture(np *unstructured.Unstructured, image string) *unstructured.Unstructured {
	version, _ := upgradev1alpha1.ExtractVersion(image)
	_ = unstructured.SetNestedField(np.Object, image, "spec", "release", "image")
	_ = unstructured.SetNestedField(np.Object, version, "status", "version")
	_ = unstructured.SetNestedField(np.Object, "AWS", "spec", "platform", "type")
	_ = unstructured.SetNestedField(np.Object, "Replace", "spec", "management", "upgradeType")
	conditions := []interface{}{}
	for typ, status := range map[string]string{"Ready": "True", "UpdatingVersion": "False", "UpdatingConfig": "False"} {
		conditions = append(conditions, map[string]interface{}{"type": typ, "status": status, "observedGeneration": np.GetGeneration(), "reason": "AsExpected", "lastTransitionTime": "2026-01-01T00:00:00Z"})
	}
	_ = unstructured.SetNestedSlice(np.Object, conditions, "status", "conditions")
	_ = unstructured.SetNestedSlice(np.Object, []interface{}{map[string]interface{}{"ocpVersion": version, "readyNodeCount": int64(1), "unreadyNodeCount": int64(0)}}, "status", "nodesInfo", "nodeVersions")
	annotations := np.GetAnnotations()
	if annotations == nil {
		annotations = map[string]string{}
	}
	annotations[workerCurrentConfig] = "config-hash"
	np.SetAnnotations(annotations)
	backing := &unstructured.Unstructured{}
	backing.SetGroupVersionKind(gvkMachineDeployment)
	backing.SetName(np.GetName())
	cluster, _, _ := unstructured.NestedString(np.Object, "spec", "clusterName")
	backing.SetNamespace(WorkerControlPlaneNamespace(np.GetNamespace(), cluster))
	backing.SetGeneration(1)
	backing.SetAnnotations(map[string]string{workerPoolAnnotation: client.ObjectKeyFromObject(np).String()})
	_ = unstructured.SetNestedField(backing.Object, int64(1), "spec", "replicas")
	_ = unstructured.SetNestedField(backing.Object, "user-data-"+np.GetName()+"-config-hash", "spec", "template", "spec", "bootstrap", "dataSecretName")
	for _, field := range []string{"replicas", "updatedReplicas", "readyReplicas", "availableReplicas", "observedGeneration"} {
		_ = unstructured.SetNestedField(backing.Object, int64(1), "status", field)
	}
	return backing
}

func quiescentWorkerFixtures(pools ...*unstructured.Unstructured) []client.Object {
	objects := []client.Object{}
	for _, np := range pools {
		image, _, _ := unstructured.NestedString(np.Object, "spec", "release", "image")
		md := readyWorkerFixture(np, image)
		objects = append(objects, np, md)
	}
	return objects
}

func TestWorkerCompletionRequiresFreshPositiveEvidence(t *testing.T) {
	for _, defect := range []string{"missing CAPI", "stale CAPI", "surge", "unavailable", "native disagreement", "stale NP", "missing conditions", "config updating", "same version new digest"} {
		t.Run(defect, func(t *testing.T) {
			hcu := regressionHCU(upgradev1alpha1.PhaseUpgradingNodePools)
			np := regressionNP("workers", "")
			np.SetGeneration(2)
			md := readyWorkerFixture(np, hcu.Spec.TargetReleaseImage)
			objects := []client.Object{np, md}
			switch defect {
			case "missing CAPI":
				objects = objects[:1]
			case "stale CAPI":
				md.SetGeneration(2)
			case "surge":
				_ = unstructured.SetNestedField(md.Object, int64(2), "status", "replicas")
			case "unavailable":
				_ = unstructured.SetNestedField(md.Object, int64(3), "spec", "replicas")
			case "native disagreement":
				_ = unstructured.SetNestedMap(md.Object, map[string]interface{}{"upToDateReplicas": int64(0), "availableReplicas": int64(1)}, "status", "v1beta2")
			case "stale NP":
				np.SetGeneration(3)
			case "missing conditions":
				unstructured.RemoveNestedField(np.Object, "status", "conditions")
			case "config updating":
				_ = unstructured.SetNestedSlice(np.Object, []interface{}{map[string]interface{}{"type": "UpdatingConfig", "status": "True", "observedGeneration": int64(2)}}, "status", "conditions")
			case "same version new digest":
				np.SetGeneration(3)
				hcu.Spec.TargetReleaseImage = strings.Replace(regressionImage("4.21.0"), "sha256:1", "sha256:b", 1)
				_ = unstructured.SetNestedField(np.Object, hcu.Spec.TargetReleaseImage, "spec", "release", "image")
			}
			c := &regressionClient{objects: objects}
			r := &HostedClusterUpgradeReconciler{Client: c}
			if _, _, err := r.reconcileNodePools(context.Background(), hcu, regressionHC("4.21.0")); err != nil {
				t.Fatal(err)
			}
			if hcu.Status.Phase == upgradev1alpha1.PhaseCompleted {
				t.Fatal("unsafe worker completion: " + defect)
			}
		})
	}
}

func TestWorkerInPlaceHashProof(t *testing.T) {
	for _, defect := range []string{"none", "hash mismatch", "unhealthy", "stale NP"} {
		t.Run(defect, func(t *testing.T) {
			np := regressionNP("workers.with.dots", "")
			_ = unstructured.SetNestedField(np.Object, "example.with.dots", "spec", "clusterName")
			np.SetGeneration(2)
			ms := readyWorkerFixture(np, regressionImage("4.21.0"))
			ms.SetGroupVersionKind(gvkMachineSet)
			_ = unstructured.SetNestedField(np.Object, "InPlace", "spec", "management", "upgradeType")
			ms.SetAnnotations(map[string]string{workerPoolAnnotation: client.ObjectKeyFromObject(np).String(), workerCurrentConfig: "config-hash", workerTargetConfig: "config-hash"})
			// Reproduce the last-Machine annotation defect: all population is healthy
			// but version aggregation still contains the old release.
			_ = unstructured.SetNestedSlice(np.Object, []interface{}{map[string]interface{}{"ocpVersion": "4.20.0", "readyNodeCount": int64(1)}}, "status", "nodesInfo", "nodeVersions")
			switch defect {
			case "hash mismatch":
				annotations := ms.GetAnnotations()
				annotations[workerCurrentConfig] = "old"
				ms.SetAnnotations(annotations)
			case "unhealthy":
				_ = unstructured.SetNestedSlice(np.Object, []interface{}{map[string]interface{}{"ocpVersion": "4.20.0", "unreadyNodeCount": int64(1)}}, "status", "nodesInfo", "nodeVersions")
			case "stale NP":
				np.SetGeneration(3)
			}
			c := &regressionClient{objects: []client.Object{np, ms}}
			r := &HostedClusterUpgradeReconciler{Client: c}
			a, err := r.assessWorker(context.Background(), np, "example.with.dots", regressionImage("4.21.0"))
			if err != nil || a.complete != (defect == "none") {
				t.Fatalf("assessment=%+v err=%v", a, err)
			}
			if ms.GetNamespace() != "clusters-example-with-dots" || ms.GetName() != "workers.with.dots" {
				t.Fatal("incorrect upstream resource naming")
			}
		})
	}
}

func TestWorkerManualPlatformsBlockBeforeMutation(t *testing.T) {
	for _, platform := range []string{"None", "IBMCloud"} {
		hcu := regressionHCU(upgradev1alpha1.PhaseUpgradingNodePools)
		np := regressionNP("manual", "")
		_ = unstructured.SetNestedField(np.Object, platform, "spec", "platform", "type")
		_ = unstructured.SetNestedField(np.Object, "UPI", "spec", "platform", "ibmcloud", "providerType")
		c := &regressionClient{objects: []client.Object{regressionNP("automated", ""), np}}
		r := &HostedClusterUpgradeReconciler{Client: c}
		if _, _, err := r.reconcileNodePools(context.Background(), hcu, regressionHC("4.21.0")); err == nil || c.writes != 0 {
			t.Fatalf("manual pool not blocked before mutation: %v", err)
		}
	}
}

func TestWorkerAutoscalingRequiresAuthoritativeDesiredPopulation(t *testing.T) {
	for _, desired := range []int64{0, 3} {
		np := regressionNP("workers", "")
		md := readyWorkerFixture(np, regressionImage("4.21.0"))
		_ = unstructured.SetNestedField(np.Object, int64(0), "status", "replicas")
		_ = unstructured.SetNestedMap(np.Object, map[string]interface{}{"min": int64(0), "max": int64(5)}, "spec", "autoScaling")
		unstructured.RemoveNestedField(np.Object, "status", "nodesInfo")
		_ = unstructured.SetNestedField(md.Object, desired, "spec", "replicas")
		for _, field := range []string{"replicas", "updatedReplicas", "readyReplicas", "availableReplicas"} {
			_ = unstructured.SetNestedField(md.Object, int64(0), "status", field)
			if desired == 0 {
				unstructured.RemoveNestedField(md.Object, "status", field)
			}
		}
		r := &HostedClusterUpgradeReconciler{Client: &regressionClient{objects: []client.Object{np, md}}}
		a, err := r.assessWorker(context.Background(), np, "example", regressionImage("4.21.0"))
		if err != nil || a.total != int32(desired) || a.complete != (desired == 0) {
			t.Fatalf("desired=%d assessment=%+v err=%v", desired, a, err)
		}
	}
}
