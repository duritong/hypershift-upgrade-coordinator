package controller

import (
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestHostedClusterPositiveHealth(t *testing.T) {
	for _, typ := range []string{"Available", "ClusterVersionAvailable", "ClusterVersionSucceeding"} {
		for _, status := range []string{"False", "Unknown", "missing"} {
			t.Run(typ+"/"+status, func(t *testing.T) {
				hc := regressionHC("4.20.0")
				conditions := []interface{}{}
				for _, other := range []string{"Available", "ClusterVersionAvailable", "ClusterVersionSucceeding"} {
					value := "True"
					if other == typ {
						if status == "missing" {
							continue
						}
						value = status
					}
					conditions = append(conditions, map[string]interface{}{"type": other, "status": value})
				}
				_ = unstructured.SetNestedSlice(hc.Object, conditions, "status", "conditions")
				if _, err := (&HostedClusterUpgradeReconciler{}).checkClusterInstallingAndHealth(hc); err == nil {
					t.Fatal("unsafe health accepted")
				}
			})
		}
	}
}
