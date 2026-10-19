package controller

import (
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
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
