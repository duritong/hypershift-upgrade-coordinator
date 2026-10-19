package controller

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func gaugeValue(t *testing.T, gauge prometheus.Gauge) float64 {
	t.Helper()
	m := &dto.Metric{}
	if err := gauge.Write(m); err != nil {
		t.Fatal(err)
	}
	return m.GetGauge().GetValue()
}

func TestComponentReadinessMetrics(t *testing.T) {
	metricComponentReady.Reset()
	t.Cleanup(metricComponentReady.Reset)
	hcu := regressionHCU("")
	cpc := &unstructured.Unstructured{}
	cpc.SetGroupVersionKind(gvkCPC)
	cpc.SetNamespace("clusters-example")
	cpc.SetName("etcd")
	c := &regressionClient{objects: []client.Object{cpc}}
	r := &HostedClusterUpgradeReconciler{Client: c}
	for _, tt := range []struct {
		available, complete, version string
		want                         float64
	}{
		{"False", "False", "4.20.0", 0},
		{"True", "False", "4.21.0", 1},
		{"True", "True", "4.21.0", 2},
		{"True", "True", "4.20.0", 1},
		{"False", "True", "4.21.0", 0},
	} {
		_ = unstructured.SetNestedField(cpc.Object, tt.version, "status", "version")
		_ = unstructured.SetNestedSlice(cpc.Object, []interface{}{
			map[string]interface{}{"type": "Available", "status": tt.available},
			map[string]interface{}{"type": "RolloutComplete", "status": tt.complete},
		}, "status", "conditions")
		if err := r.aggregateControlPlaneStatus(context.Background(), hcu, "4.21.0"); err != nil {
			t.Fatal(err)
		}
		if got := gaugeValue(t, metricComponentReady.WithLabelValues(hcu.Namespace, hcu.Name, "etcd")); got != tt.want {
			t.Fatalf("available=%s complete=%s version=%s: readiness=%v, want %v", tt.available, tt.complete, tt.version, got, tt.want)
		}
	}
	metricComponentReady.WithLabelValues("other", "cluster", "etcd").Set(2)
	c.objects = nil
	if err := r.aggregateControlPlaneStatus(context.Background(), hcu, "4.21.0"); err != nil {
		t.Fatal(err)
	}
	if removed := metricComponentReady.DeleteLabelValues(hcu.Namespace, hcu.Name, "etcd"); removed {
		t.Fatal("deleted component retained a readiness series")
	}
	if got := gaugeValue(t, metricComponentReady.WithLabelValues("other", "cluster", "etcd")); got != 2 {
		t.Fatal("aggregation cleared another cluster's metrics")
	}
}
