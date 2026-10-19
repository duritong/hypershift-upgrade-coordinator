package controller

import (
	"context"
	"fmt"
	"testing"
	"time"

	upgradev1alpha1 "github.com/duritong/hypershift-upgrade-coordinator/api/v1alpha1"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func resetStepGauges(t *testing.T) {
	t.Helper()
	clearUpgradeGauges("clusters", "example")
	t.Cleanup(func() { clearUpgradeGauges("clusters", "example") })
}

func assertStepSeconds(t *testing.T, step string, want float64) {
	t.Helper()
	for _, metric := range collectedGauges(t, metricStepDuration) {
		for _, label := range metric.Label {
			if label.GetName() == "step" && label.GetValue() == step {
				got := metric.GetGauge().GetValue()
				if got < want || got > want+5 {
					t.Fatalf("%s duration = %v, want [%v,%v]", step, got, want, want+5)
				}
				return
			}
		}
	}
	t.Fatalf("missing step %s", step)
}

func collectedGauges(t *testing.T, gauge *prometheus.GaugeVec) []*dto.Metric {
	t.Helper()
	registry := prometheus.NewRegistry()
	registry.MustRegister(gauge)
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var result []*dto.Metric
	for _, family := range families {
		result = append(result, family.Metric...)
	}
	return result
}

func TestStepMetricsExcludeScheduling(t *testing.T) {
	metricStepDuration.Reset()
	t.Cleanup(metricStepDuration.Reset)
	hcu := regressionHCU(upgradev1alpha1.PhasePendingSchedule)
	start := metav1.NewTime(time.Now().Add(-time.Hour))
	hcu.Status.StartTime = &start
	future := metav1.NewTime(time.Now().Add(time.Hour))
	hcu.Spec.ControlPlane.StartAt = &future
	c := &regressionClient{objects: []client.Object{hcu, regressionHC("4.20.0")}}
	reconcileRegression(t, c)
	if got := collectedGauges(t, metricStepDuration); len(got) != 0 {
		t.Fatalf("scheduled upgrade exposes an active step: %v", got)
	}
}

func TestPhaseMetricsReportTransitionAndRemoveOldVersions(t *testing.T) {
	metricPhase.Reset()
	t.Cleanup(metricPhase.Reset)
	metricPhase.WithLabelValues("clusters", "example", "4.19.0", "4.20.0", "").Set(3)
	hcu := regressionHCU(upgradev1alpha1.PhaseIdle)
	c := &regressionClient{objects: []client.Object{hcu, regressionHC("4.20.0")}}
	reconcileRegression(t, c)
	if got := gaugeValue(t, metricPhase.WithLabelValues("clusters", "example", "4.20.0", "4.21.0", "")); got != 2 {
		t.Fatalf("phase gauge = %v, want post-transition PreflightChecks (2)", got)
	}
	if got := collectedGauges(t, metricPhase); len(got) != 1 {
		t.Fatalf("obsolete phase labels retained: %v", got)
	}
}

func readyStepCPC(version string) *unstructured.Unstructured {
	cpc := &unstructured.Unstructured{}
	cpc.SetGroupVersionKind(gvkCPC)
	cpc.SetNamespace("clusters-example")
	cpc.SetName("etcd")
	_ = unstructured.SetNestedField(cpc.Object, version, "status", "version")
	_ = unstructured.SetNestedSlice(cpc.Object, []interface{}{
		map[string]interface{}{"type": "Available", "status": "True"},
		map[string]interface{}{"type": "RolloutComplete", "status": "True"},
	}, "status", "conditions")
	return cpc
}

func TestControlPlaneStepTimerLifecycle(t *testing.T) {
	resetStepGauges(t)
	hcu := regressionHCU(upgradev1alpha1.PhasePendingSchedule)
	hcu.Spec.TargetReleaseImage = regressionImage("4.22.0")
	hcu.Spec.IntermediateReleaseImage = regressionImage("4.21.0")
	overall := metav1.NewTime(time.Now().Add(-time.Hour))
	hcu.Status.StartTime = &overall
	c := &regressionClient{objects: []client.Object{hcu, regressionHC("4.20.0")}}
	got := reconcileRegression(t, c)
	if got.Status.ControlPlaneStatus.StartTime != nil || len(collectedGauges(t, metricStepDuration)) != 0 {
		t.Fatal("opening the schedule started a timer before actuation")
	}
	before := time.Now().Add(-time.Second)
	got = reconcileRegression(t, c)
	if start := got.Status.ControlPlaneStatus.StartTime; start == nil || start.Time.Before(before) {
		t.Fatalf("actuation did not persist a new timer: %+v", got.Status.ControlPlaneStatus)
	}
	assertStepSeconds(t, "ControlPlane", 0)
	started := metav1.NewTime(time.Now().Add(-90 * time.Second))
	got.Status.ControlPlaneStatus.StartTime = &started
	if err := c.store(got); err != nil {
		t.Fatal(err)
	}
	got = reconcileRegression(t, c) // A new reconciler reads persisted status.
	if !got.Status.ControlPlaneStatus.StartTime.Equal(&started) {
		t.Fatal("reconcile reset the control plane timer")
	}
	assertStepSeconds(t, "ControlPlane", 90)
	paused := "true"
	got.Spec.PausedUntil = &paused
	if err := c.store(got); err != nil {
		t.Fatal(err)
	}
	got = reconcileRegression(t, c)
	if !got.Status.ControlPlaneStatus.StartTime.Equal(&started) {
		t.Fatal("pause reset the control plane timer")
	}
	got.Spec.PausedUntil = nil
	if err := c.store(got); err != nil {
		t.Fatal(err)
	}
	got = reconcileRegression(t, c)
	if !got.Status.ControlPlaneStatus.StartTime.Equal(&started) {
		t.Fatal("resume reset the control plane timer")
	}
	// Re-entering actuation for the same hop must not reset its timestamp.
	got.Status.Phase = upgradev1alpha1.PhaseUpgradingControlPlane
	if err := c.store(got); err != nil {
		t.Fatal(err)
	}
	got = reconcileRegression(t, c)
	if !got.Status.ControlPlaneStatus.StartTime.Equal(&started) {
		t.Fatal("reapplying the current hop reset its timer")
	}
	if err := c.store(regressionHC("4.21.0")); err != nil {
		t.Fatal(err)
	}
	c.objects = append(c.objects, readyStepCPC("4.21.0"))
	got = reconcileRegression(t, c)
	if got.Status.Phase != upgradev1alpha1.PhaseUpgradingControlPlane || got.Status.ControlPlaneStatus.StartTime != nil || len(collectedGauges(t, metricStepDuration)) != 0 {
		t.Fatalf("intermediate verification retained the timer: %+v", got.Status)
	}
	got = reconcileRegression(t, c)
	if start := got.Status.ControlPlaneStatus.StartTime; start == nil || !start.Time.After(started.Time) || got.Status.ControlPlaneStatus.TargetReleaseImage != hcu.Spec.TargetReleaseImage {
		t.Fatalf("target hop did not start its own timer: %+v", got.Status.ControlPlaneStatus)
	}
	assertStepSeconds(t, "ControlPlane", 0)
	if err := c.store(regressionHC("4.22.0")); err != nil {
		t.Fatal(err)
	}
	if err := c.store(readyStepCPC("4.22.0")); err != nil {
		t.Fatal(err)
	}
	got = reconcileRegression(t, c)
	if got.Status.Phase != upgradev1alpha1.PhaseUpgradingNodePools || got.Status.ControlPlaneStatus.StartTime != nil || len(collectedGauges(t, metricStepDuration)) != 0 {
		t.Fatalf("target verification retained the timer: %+v", got.Status)
	}
}

func TestNodePoolStepTimersPersistAndCompleteIndependently(t *testing.T) {
	resetStepGauges(t)
	hcu := regressionHCU(upgradev1alpha1.PhaseUpgradingNodePools)
	hcu.Spec.RolloutPolicy = &upgradev1alpha1.RolloutPolicySpec{MaxParallelNodePools: 2}
	overall := metav1.NewTime(time.Now().Add(-time.Hour))
	hcu.Status.StartTime = &overall
	c := &regressionClient{objects: append([]client.Object{hcu, regressionHC("4.21.0")}, quiescentWorkerFixtures(regressionNP("a", ""), regressionNP("b", ""))...)}
	got := reconcileRegression(t, c)
	if len(got.Status.NodePoolStatuses) != 2 {
		t.Fatalf("unexpected statuses: %+v", got.Status)
	}
	for i := range got.Status.NodePoolStatuses {
		status := &got.Status.NodePoolStatuses[i]
		if status.StartTime == nil {
			t.Fatalf("%s actuation did not persist a timer", status.Name)
		}
		assertStepSeconds(t, "NodePool-"+status.Name, 0)
		start := metav1.NewTime(time.Now().Add(-time.Duration(30+i*40) * time.Second))
		status.StartTime = &start
	}
	if err := c.store(got); err != nil {
		t.Fatal(err)
	}
	got = reconcileRegression(t, c)
	for i, status := range got.Status.NodePoolStatuses {
		assertStepSeconds(t, "NodePool-"+status.Name, float64(30+i*40))
	}
	paused := "true"
	got.Spec.PausedUntil = &paused
	if err := c.store(got); err != nil {
		t.Fatal(err)
	}
	pausedStatus := reconcileRegression(t, c)
	for i, status := range pausedStatus.Status.NodePoolStatuses {
		if !status.StartTime.Equal(got.Status.NodePoolStatuses[i].StartTime) {
			t.Fatal("pause changed a persisted worker timer")
		}
	}
	pausedStatus.Spec.PausedUntil = nil
	if err := c.store(pausedStatus); err != nil {
		t.Fatal(err)
	}
	resumed := reconcileRegression(t, c)
	for i, status := range resumed.Status.NodePoolStatuses {
		if !status.StartTime.Equal(got.Status.NodePoolStatuses[i].StartTime) {
			t.Fatal("resume changed a persisted worker timer")
		}
	}
	// Exercise worker completion without depending on pause-resume routing.
	resumed.Status.Phase = upgradev1alpha1.PhaseUpgradingNodePools
	if err := c.store(resumed); err != nil {
		t.Fatal(err)
	}
	for i, obj := range c.objects {
		if np, ok := obj.(*unstructured.Unstructured); ok && np.GroupVersionKind() == gvkNP && np.GetName() == "a" {
			np = np.DeepCopy()
			_ = unstructured.SetNestedField(np.Object, "4.21.0", "status", "version")
			_ = unstructured.SetNestedSlice(np.Object, []interface{}{map[string]interface{}{
				"ocpVersion": "4.21.0", "readyNodeCount": int64(1), "unreadyNodeCount": int64(0),
			}}, "status", "nodesInfo", "nodeVersions")
			c.objects = append(c.objects, readyWorkerFixture(np, resumed.Spec.TargetReleaseImage))
			c.objects[i] = np
		}
	}
	got = reconcileRegression(t, c)
	if got.Status.NodePoolStatuses[0].StartTime != nil || got.Status.NodePoolStatuses[0].Phase != "Completed" {
		t.Fatalf("completed pool retained its timer: %+v", got.Status.NodePoolStatuses)
	}
	if got := collectedGauges(t, metricStepDuration); len(got) != 1 {
		t.Fatalf("want only unfinished pool timer: %v", got)
	}
	assertStepSeconds(t, "NodePool-b", 70)
}

type stepUpdateFailureClient struct {
	*regressionClient
	failName string
}

func (c *stepUpdateFailureClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	if obj.GetName() == c.failName {
		return fmt.Errorf("injected update failure")
	}
	return c.regressionClient.Update(ctx, obj, opts...)
}

func TestStepTimersDoNotStartOnFailedActuation(t *testing.T) {
	for _, phase := range []upgradev1alpha1.UpgradePhase{upgradev1alpha1.PhaseUpgradingControlPlane, upgradev1alpha1.PhaseUpgradingNodePools} {
		t.Run(string(phase), func(t *testing.T) {
			resetStepGauges(t)
			hcu := regressionHCU(phase)
			base := &regressionClient{objects: []client.Object{hcu, regressionHC("4.20.0"), regressionNP("workers", "")}}
			name := "example"
			if phase == upgradev1alpha1.PhaseUpgradingNodePools {
				name = "workers"
			}
			r := &HostedClusterUpgradeReconciler{Client: &stepUpdateFailureClient{regressionClient: base, failName: name}}
			_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(hcu)})
			if phase == upgradev1alpha1.PhaseUpgradingControlPlane && err == nil {
				t.Fatal("expected failed CP update")
			}
			got := &upgradev1alpha1.HostedClusterUpgrade{}
			if err := base.Get(context.Background(), client.ObjectKeyFromObject(hcu), got); err != nil {
				t.Fatal(err)
			}
			if got.Status.ControlPlaneStatus.StartTime != nil {
				t.Fatal("failed CP actuation started a timer")
			}
			for _, status := range got.Status.NodePoolStatuses {
				if status.StartTime != nil {
					t.Fatal("failed worker actuation started a timer")
				}
			}
		})
	}
}

func TestStaleUpgradeGaugesAndDeletion(t *testing.T) {
	resetStepGauges(t)
	hcu := regressionHCU(upgradev1alpha1.PhaseUpgradingNodePools)
	started := metav1.NewTime(time.Now().Add(-time.Minute))
	hcu.Status.NodePoolStatuses = []upgradev1alpha1.NodePoolUpgradeStatus{{Name: "gone", TargetReleaseImage: hcu.Spec.TargetReleaseImage, StartTime: &started}}
	metricDuration.WithLabelValues("clusters", "example", "old", "old", "old").Set(999)
	metricStepDuration.WithLabelValues("clusters", "example", "NodePool-gone", "old", "old").Set(999)
	metricNodePoolBlocked.WithLabelValues("clusters", "example", "gone", "old", "old").Set(1)
	metricNodePoolBlocked.WithLabelValues("clusters", "example", "workers", "old", "old").Set(1)
	metricNodePoolRolloutRatio.WithLabelValues("clusters", "example", "gone").Set(0.5)
	metricAggregatedNodeRolloutRatio.WithLabelValues("clusters", "example").Set(0.5)
	c := &regressionClient{objects: append([]client.Object{hcu, regressionHC("4.21.0")}, quiescentWorkerFixtures(regressionNP("workers", ""))...)}
	got := reconcileRegression(t, c)
	if len(got.Status.NodePoolStatuses) != 1 || got.Status.NodePoolStatuses[0].Name != "workers" {
		t.Fatalf("disappeared pool retained status: %+v", got.Status.NodePoolStatuses)
	}
	for _, gauge := range []*prometheus.GaugeVec{metricPhase, metricDuration, metricStepDuration, metricNodePoolBlocked, metricNodePoolRolloutRatio} {
		if got := collectedGauges(t, gauge); len(got) != 1 {
			t.Fatalf("obsolete gauge label sets retained: %v", got)
		}
	}
	// Completed and paused reconciles must also remove disappeared worker series.
	for _, phase := range []upgradev1alpha1.UpgradePhase{upgradev1alpha1.PhasePaused, upgradev1alpha1.PhaseCompleted} {
		got.Status.Phase = phase
		got.Status.ControlPlaneStatus.TargetReleaseImage = got.Spec.TargetReleaseImage
		if phase == upgradev1alpha1.PhasePaused {
			paused := "true"
			got.Spec.PausedUntil = &paused
		} else {
			got.Spec.PausedUntil = nil
		}
		c.objects = []client.Object{got, regressionHC("4.21.0")}
		got = reconcileRegression(t, c)
		for _, gauge := range []*prometheus.GaugeVec{metricStepDuration, metricNodePoolBlocked, metricNodePoolRolloutRatio} {
			if metrics := collectedGauges(t, gauge); len(metrics) != 0 {
				t.Fatalf("%s retained disappeared worker gauges: %v", phase, metrics)
			}
		}
	}
	metricComponentReady.WithLabelValues("clusters", "example", "etcd").Set(2)
	counter := metricFailedTotal.WithLabelValues("clusters", "example", "", "", "step-metrics-test")
	counter.Inc()
	t.Cleanup(func() { metricFailedTotal.DeleteLabelValues("clusters", "example", "", "", "step-metrics-test") })
	gauges := []*prometheus.GaugeVec{metricPhase, metricDuration, metricStepDuration, metricComponentReady, metricNodePoolBlocked, metricNodePoolRolloutRatio, metricAggregatedNodeRolloutRatio}
	for _, gauge := range gauges {
		labels := prometheus.Labels{"namespace": "other", "name": "cluster"}
		// Seed every gauge, including vectors that currently have no series.
		switch gauge {
		case metricPhase, metricDuration:
			labels["current_version"], labels["target_version"], labels["intermediate_version"] = "other", "other", ""
		case metricStepDuration:
			labels["step"], labels["current_version"], labels["target_version"] = "other", "other", "other"
		case metricNodePoolBlocked:
			labels["nodepool"], labels["current_version"], labels["target_version"] = "other", "other", "other"
		case metricNodePoolRolloutRatio:
			labels["nodepool"] = "other"
		case metricComponentReady:
			labels["component"] = "other"
		}
		gauge.With(labels).Set(42)
		owned := prometheus.Labels{}
		for key, value := range labels {
			owned[key] = value
		}
		owned["namespace"], owned["name"] = "clusters", "example"
		gauge.With(owned).Set(1)
	}
	t.Cleanup(func() { clearUpgradeGauges("other", "cluster") })
	c.objects = nil
	r := &HostedClusterUpgradeReconciler{Client: c}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(hcu)}); err != nil {
		t.Fatal(err)
	}
	for _, gauge := range gauges {
		if metrics := collectedGauges(t, gauge); len(metrics) != 1 || metrics[0].GetGauge().GetValue() != 42 {
			t.Fatalf("deletion failed owner-scoped cleanup: %v", metrics)
		}
	}
	m := &dto.Metric{}
	if err := counter.Write(m); err != nil || m.GetCounter().GetValue() != 1 {
		t.Fatalf("deletion altered failure counter: %v, %v", m, err)
	}
}

func TestNodePoolStepTimersExcludeUnactuatedPools(t *testing.T) {
	for _, gate := range []string{"schedule", "prior group", "concurrency", "external rollout"} {
		t.Run(gate, func(t *testing.T) {
			resetStepGauges(t)
			hcu := regressionHCU(upgradev1alpha1.PhaseUpgradingNodePools)
			pool := regressionNP("workers", "")
			c := &regressionClient{objects: []client.Object{hcu, regressionHC("4.21.0"), pool}}
			switch gate {
			case "schedule":
				future := metav1.NewTime(time.Now().Add(time.Hour))
				hcu.Spec.NodePoolGroups = []upgradev1alpha1.NodePoolGroupSpec{{Name: "default", StartAt: &future}}
			case "prior group":
				hcu.Spec.NodePoolGroups = []upgradev1alpha1.NodePoolGroupSpec{{Name: "first"}}
				c.objects = append(c.objects, regressionNP("first", "first"))
			case "concurrency":
				active := regressionNP("active", "")
				_ = unstructured.SetNestedField(active.Object, hcu.Spec.TargetReleaseImage, "spec", "release", "image")
				c.objects = append(c.objects, active)
			case "external rollout":
				_ = unstructured.SetNestedField(pool.Object, hcu.Spec.TargetReleaseImage, "spec", "release", "image")
			}
			got := reconcileRegression(t, c)
			for _, status := range got.Status.NodePoolStatuses {
				if status.Name == "workers" && status.StartTime != nil {
					t.Fatalf("%s invented a start timestamp: %+v", gate, status)
				}
			}
			for _, metric := range collectedGauges(t, metricStepDuration) {
				for _, label := range metric.Label {
					if label.GetName() == "step" && label.GetValue() == "NodePool-workers" {
						t.Fatalf("%s exposes an unactuated worker timer", gate)
					}
				}
			}
		})
	}
}

func TestNodePoolStepTimerSurvivesLaterUpdateFailure(t *testing.T) {
	resetStepGauges(t)
	hcu := regressionHCU(upgradev1alpha1.PhaseUpgradingNodePools)
	hcu.Spec.RolloutPolicy = &upgradev1alpha1.RolloutPolicySpec{MaxParallelNodePools: 2}
	base := &regressionClient{objects: append([]client.Object{hcu, regressionHC("4.21.0")}, quiescentWorkerFixtures(regressionNP("a", ""), regressionNP("b", ""))...)}
	if err := (&HostedClusterUpgradeReconciler{Client: base}).lockClusterNodePools(context.Background(), hcu); err != nil {
		t.Fatal(err)
	}
	r := &HostedClusterUpgradeReconciler{Client: &stepUpdateFailureClient{regressionClient: base, failName: "b"}}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(hcu)}); err != nil {
		t.Fatal(err)
	}
	got := &upgradev1alpha1.HostedClusterUpgrade{}
	if err := base.Get(context.Background(), client.ObjectKeyFromObject(hcu), got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != upgradev1alpha1.PhaseBlocked || got.Status.NodePoolStatuses[0].StartTime == nil || got.Status.NodePoolStatuses[1].StartTime != nil {
		t.Fatalf("partial failure lost successful actuation or timed failed actuation: %+v", got.Status)
	}
	assertStepSeconds(t, "NodePool-a", 0)
}
