/*
Copyright 2026 SRE Fleet Team.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	upgradev1alpha1 "github.com/duritong/hypershift-upgrade-coordinator/api/v1alpha1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/yaml"
)

func deletingMachineFixture(name, poolRef, nodeName string, deletedAt, drainStartedAt time.Time, drainTimeout string, condType, condStatus, condReason, condMsg string) *unstructured.Unstructured {
	m := &unstructured.Unstructured{}
	m.SetGroupVersionKind(gvkMachine)
	m.SetNamespace("clusters-example")
	m.SetName(name)
	delTS := metav1.NewTime(deletedAt)
	m.SetDeletionTimestamp(&delTS)
	if poolRef != "" {
		m.SetAnnotations(map[string]string{workerPoolAnnotation: poolRef})
	}
	if nodeName != "" {
		_ = unstructured.SetNestedField(m.Object, nodeName, "status", "nodeRef", "name")
	}
	if !drainStartedAt.IsZero() {
		if condType == "VolumeDetachSucceeded" {
			_ = unstructured.SetNestedField(m.Object, drainStartedAt.UTC().Format(time.RFC3339), "status", "deletion", "waitForNodeVolumeDetachStartTime")
		} else {
			_ = unstructured.SetNestedField(m.Object, drainStartedAt.UTC().Format(time.RFC3339), "status", "deletion", "nodeDrainStartTime")
		}
	}
	if drainTimeout != "" {
		if condType == "VolumeDetachSucceeded" {
			_ = unstructured.SetNestedField(m.Object, drainTimeout, "spec", "nodeVolumeDetachTimeout")
		} else {
			_ = unstructured.SetNestedField(m.Object, drainTimeout, "spec", "nodeDrainTimeout")
		}
	}
	if condType != "" {
		_ = unstructured.SetNestedSlice(m.Object, []interface{}{
			map[string]interface{}{
				"type":               condType,
				"status":             condStatus,
				"reason":             condReason,
				"message":            condMsg,
				"lastTransitionTime": deletedAt.UTC().Format(time.RFC3339),
			},
		}, "status", "conditions")
	}
	return m
}

func TestCAPIMachineDrainTelemetryAndCountdown(t *testing.T) {
	now := time.Date(2026, 11, 18, 15, 0, 0, 0, time.UTC)
	clock := &flowClock{at: now}
	hcu := regressionHCU(upgradev1alpha1.PhaseUpgradingNodePools)
	t.Cleanup(func() { clearUpgradeGauges(hcu.Namespace, hcu.Name) })

	started := metav1.NewTime(now.Add(-45 * time.Minute))
	hcu.Status.NodePoolStatuses = []upgradev1alpha1.NodePoolUpgradeStatus{
		{Name: "workers", Phase: "Upgrading", TargetReleaseImage: hcu.Spec.TargetReleaseImage, StartTime: &started},
	}

	np := regressionNP("workers", "")
	md := readyWorkerFixture(np, regressionImage("4.20.0"))
	_ = unstructured.SetNestedField(np.Object, hcu.Spec.TargetReleaseImage, "spec", "release", "image")
	np.SetGeneration(2)

	// Machine 1: PDB-blocked drain, started 42m ago, nodeDrainTimeout=1h -> force cutoff in 18m.
	pdbMsg := "Drain not completed yet (started at 2026-11-18T14:18:00Z):\n* Pod app/web-0: Cannot evict pod as it would violate the pod's disruption budget."
	mBlocked := deletingMachineFixture(
		"example-workers-1", "clusters/workers", "worker-node-1",
		now.Add(-42*time.Minute), now.Add(-42*time.Minute), "1h0m0s",
		"DrainingSucceeded", "False", "Draining", pdbMsg,
	)
	// Machine 2: Normal graceful drain (pods already terminating), started 2m ago, nodeDrainTimeout=1h -> not blocked.
	mGraceful := deletingMachineFixture(
		"example-workers-2", "clusters/workers", "worker-node-2",
		now.Add(-2*time.Minute), now.Add(-2*time.Minute), "1h0m0s",
		"DrainingSucceeded", "False", "Draining", "Pods with deletionTimestamp that still exist: app/job-1",
	)
	// Machine 3: Unannotated/foreign machine in namespace must never be misattributed.
	mForeign := deletingMachineFixture(
		"foreign-machine", "other-ns/workers", "foreign-node",
		now.Add(-30*time.Minute), now.Add(-30*time.Minute), "10m",
		"DrainingSucceeded", "False", "DrainingFailed", "Cannot evict pod",
	)

	hc := regressionHC("4.21.0")
	c := &regressionClient{objects: []client.Object{hcu, hc, np, md, mBlocked, mGraceful, mForeign}}
	addReadyCP(c, hc)
	r := &HostedClusterUpgradeReconciler{
		Client:          c,
		APIReader:       workerSource(c, hcu),
		Clock:           clock,
		ReleaseResolver: fixtureReleaseResolver{},
		GuestAccess:     legacyGuest(c, hcu, nil),
	}

	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(hcu)}); err != nil {
		t.Fatal(err)
	}
	got := &upgradev1alpha1.HostedClusterUpgrade{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(hcu), got); err != nil {
		t.Fatal(err)
	}

	if got.Status.AggregatedNodeStatus.Draining != 2 || got.Status.AggregatedNodeStatus.DrainBlocked != 1 {
		t.Fatalf("unexpected aggregated drain counts: %+v", got.Status.AggregatedNodeStatus)
	}
	if got.Status.NodeBlockersSummary != "1 node drain blocked (force in 18m)" {
		t.Fatalf("NodeBlockersSummary = %q, want %q", got.Status.NodeBlockersSummary, "1 node drain blocked (force in 18m)")
	}
	if !strings.Contains(got.Status.Message, "worker-node-1") || !strings.Contains(got.Status.Message, "force cutoff in 18m") || !strings.Contains(got.Status.Message, "disruption budget") {
		t.Fatalf("Status.Message missing blocked node/cutoff/PDB detail: %q", got.Status.Message)
	}

	if len(got.Status.NodePoolStatuses) != 1 {
		t.Fatalf("expected 1 pool status, got %d", len(got.Status.NodePoolStatuses))
	}
	ps := got.Status.NodePoolStatuses[0]
	if ps.Phase != "Upgrading" {
		t.Fatalf("pool phase = %q, want Upgrading", ps.Phase)
	}
	if len(ps.Drains) != 2 {
		t.Fatalf("expected 2 structured drains (foreign excluded), got %+v", ps.Drains)
	}
	// Blocked drain sorts ahead of graceful drain.
	if !ps.Drains[0].Blocked || ps.Drains[0].Node != "worker-node-1" || ps.Drains[0].Timeout == nil || ps.Drains[0].Timeout.Duration != time.Hour {
		t.Fatalf("unexpected primary drain status: %+v", ps.Drains[0])
	}
	wantCutoff := now.Add(18 * time.Minute)
	if ps.Drains[0].ForceDrainAt == nil || !ps.Drains[0].ForceDrainAt.Time.Equal(wantCutoff) {
		t.Fatalf("ForceDrainAt = %v, want %v", ps.Drains[0].ForceDrainAt, wantCutoff)
	}
	if ps.Drains[1].Blocked || ps.Drains[1].Node != "worker-node-2" {
		t.Fatalf("graceful drain wrongly marked blocked: %+v", ps.Drains[1])
	}

	// Verify Prometheus gauges: draining=2, blocked=1, max duration=2520s (42m), shortest cutoff=1080s (18m).
	if g := collectedGauges(t, metricNodePoolDrainingMachines); len(g) != 1 || g[0].GetGauge().GetValue() != 2 {
		t.Fatalf("draining_machines gauge = %+v", g)
	}
	if g := collectedGauges(t, metricNodePoolDrainBlockedMachines); len(g) != 1 || g[0].GetGauge().GetValue() != 1 {
		t.Fatalf("drain_blocked_machines gauge = %+v", g)
	}
	if g := collectedGauges(t, metricNodePoolDrainDuration); len(g) != 1 || g[0].GetGauge().GetValue() != float64(42*60) {
		t.Fatalf("drain_duration_seconds gauge = %+v", g)
	}
	if g := collectedGauges(t, metricNodePoolForceDrainRemaining); len(g) != 1 || g[0].GetGauge().GetValue() != float64(18*60) {
		t.Fatalf("force_drain_remaining_seconds gauge = %+v", g)
	}

	// Coordinator pause must still refresh live CAPI drain telemetry in observe-only mode.
	pause := "true"
	got.Spec.PausedUntil = &pause
	if err := c.Update(context.Background(), got); err != nil {
		t.Fatal(err)
	}
	clock.Step(8 * time.Minute)
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(hcu)}); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(hcu), got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != upgradev1alpha1.PhasePaused || got.Status.NodeBlockersSummary != "1 node drain blocked (force in 10m)" {
		t.Fatalf("paused summary did not refresh countdown: phase=%s summary=%q", got.Status.Phase, got.Status.NodeBlockersSummary)
	}
	if g := collectedGauges(t, metricNodePoolForceDrainRemaining); len(g) != 1 || g[0].GetGauge().GetValue() != float64(10*60) {
		t.Fatalf("paused force_drain_remaining_seconds = %+v", g)
	}

	// Once completed, all CAPI drain series and summaries clear cleanly.
	got.Spec.PausedUntil = nil
	got.Status.Phase = upgradev1alpha1.PhaseCompleted
	got.Status.ControlPlaneStatus.TargetReleaseImage = got.Spec.TargetReleaseImage
	r.exposePhaseMetrics(context.Background(), got, hc)
	if got.Status.NodeBlockersSummary != "None" {
		t.Fatalf("completed summary = %q, want None", got.Status.NodeBlockersSummary)
	}
	for _, vec := range []*struct {
		name string
		n    int
	}{
		{"draining", len(collectedGauges(t, metricNodePoolDrainingMachines))},
		{"blocked", len(collectedGauges(t, metricNodePoolDrainBlockedMachines))},
		{"duration", len(collectedGauges(t, metricNodePoolDrainDuration))},
		{"remaining", len(collectedGauges(t, metricNodePoolForceDrainRemaining))},
	} {
		if vec.n != 0 {
			t.Fatalf("completed upgrade retained %s gauge (%d series)", vec.name, vec.n)
		}
	}
}

func TestInspectMachineDrainStagesAndV1Beta2Fallback(t *testing.T) {
	now := time.Date(2026, 11, 18, 15, 0, 0, 0, time.UTC)

	hook := deletingMachineFixture("m-hook", "clusters/workers", "node-hook", now.Add(-5*time.Minute), time.Time{}, "", "PreDrainDeleteHookSucceeded", "False", "WaitingExternalHook", "waiting on hook pre-drain.delete.hook.cluster.x-k8s.io/custom")
	if d, ok := inspectMachineDrain(hook); !ok || d.Phase != drainPhasePreDrainHook || !d.Blocked || d.ForceDrainAt != nil {
		t.Fatalf("unexpected pre-drain hook result: ok=%t %+v", ok, d)
	}

	detach := deletingMachineFixture("m-detach", "clusters/workers", "node-detach", now.Add(-4*time.Minute), now.Add(-3*time.Minute), "10m", "VolumeDetachSucceeded", "False", "WaitingForVolumeDetach", "Waiting for CSI volumes")
	if d, ok := inspectMachineDrain(detach); !ok || d.Phase != drainPhaseVolumeDetach || d.Blocked || d.ForceDrainAt == nil || !d.ForceDrainAt.Time.Equal(now.Add(7*time.Minute)) {
		t.Fatalf("unexpected volume detach result: ok=%t %+v", ok, d)
	}

	// v1beta2 Deleting condition + spec.deletion.nodeDrainTimeoutSeconds fallback.
	v1b2 := &unstructured.Unstructured{}
	v1b2.SetGroupVersionKind(gvkMachine)
	v1b2.SetName("m-v1b2")
	delTS := metav1.NewTime(now.Add(-10 * time.Minute))
	v1b2.SetDeletionTimestamp(&delTS)
	_ = unstructured.SetNestedField(v1b2.Object, int64(1800), "spec", "deletion", "nodeDrainTimeoutSeconds")
	_ = unstructured.SetNestedSlice(v1b2.Object, []interface{}{
		map[string]interface{}{
			"type":    "Deleting",
			"status":  "True",
			"reason":  "DrainingNode",
			"message": "Cannot evict pod as it would violate the pod's disruption budget",
		},
	}, "status", "v1beta2", "conditions")
	if d, ok := inspectMachineDrain(v1b2); !ok || d.Phase != drainPhaseDraining || !d.Blocked || d.Timeout == nil || d.Timeout.Duration != 30*time.Minute {
		t.Fatalf("unexpected v1beta2 drain result: ok=%t %+v", ok, d)
	}

	// Completed drain (DrainingSucceeded=True, VolumeDetachSucceeded=True) is no longer active.
	done := deletingMachineFixture("m-done", "clusters/workers", "node-done", now.Add(-5*time.Minute), now.Add(-5*time.Minute), "10m", "DrainingSucceeded", "True", "", "")
	if _, ok := inspectMachineDrain(done); ok {
		t.Fatal("completed drain should not report active")
	}
}

func TestNodeBlockersSummaryCoversInPlaceAndPoolBlocks(t *testing.T) {
	now := time.Date(2026, 11, 18, 15, 0, 0, 0, time.UTC)

	// 1. InPlace HCCO drain eviction failure (no CAPI Machine deletion, HCCO drainDiagnostics populated).
	inPlace := regressionHCU(upgradev1alpha1.PhaseUpgradingNodePools)
	inPlace.Status.NodePoolStatuses = []upgradev1alpha1.NodePoolUpgradeStatus{
		{Name: "az2", Phase: "Upgrading"},
		{Name: "az3", Phase: "WaitingForGroup"},
	}
	inPlace.Status.DrainDiagnostics = &upgradev1alpha1.DrainDiagnosticsStatus{
		Available: true,
		Reason:    "Collected",
		Messages:  []string{`node worker-az2-1: error when evicting pods/"router-0" -n "openshift-ingress": Cannot evict pod as it would violate the pod's disruption budget.`},
	}
	if got := computeNodeBlockersSummary(inPlace, now); got != "Drain eviction failing" {
		t.Fatalf("InPlace drain summary = %q, want %q", got, "Drain eviction failing")
	}
	if msg := summarizeActiveWorkerMessage(inPlace, false, now); !strings.Contains(msg, "drain issue observed") || !strings.Contains(msg, "worker-az2-1") {
		t.Fatalf("InPlace worker message = %q", msg)
	}

	// 2. Pool blocked by WorkerGuard / user pause.
	poolBlocked := regressionHCU(upgradev1alpha1.PhaseUpgradingNodePools)
	poolBlocked.Status.NodePoolStatuses = []upgradev1alpha1.NodePoolUpgradeStatus{
		{Name: "infra", Phase: "Completed"},
		{Name: "compute", Phase: "Blocked", Blockers: []string{"guest WorkerGuard: Unhealthy (CNVOutdatedWorkloads)"}},
	}
	if got := computeNodeBlockersSummary(poolBlocked, now); got != "1/2 pools blocked" {
		t.Fatalf("pool-blocked summary = %q, want %q", got, "1/2 pools blocked")
	}
	if msg := summarizeActiveWorkerMessage(poolBlocked, false, now); !strings.Contains(msg, "Worker NodePool 'compute' blocked") {
		t.Fatalf("pool-blocked message = %q", msg)
	}
}

func TestMachineDrainPredicateSkipsSteadyStateMachines(t *testing.T) {
	steady1 := &unstructured.Unstructured{}
	steady1.SetGroupVersionKind(gvkMachine)
	steady2 := steady1.DeepCopy()
	_ = unstructured.SetNestedField(steady2.Object, "Ready", "status", "phase")

	if machineDrainPredicate.Create(event.CreateEvent{Object: steady1}) {
		t.Fatal("non-deleting Machine create must not enqueue")
	}
	if machineDrainPredicate.Update(event.UpdateEvent{ObjectOld: steady1, ObjectNew: steady2}) {
		t.Fatal("non-deleting Machine status heartbeat must not enqueue")
	}
	if machineDrainPredicate.Delete(event.DeleteEvent{Object: steady1}) {
		t.Fatal("non-deleting Machine delete must not enqueue")
	}

	deleting := steady2.DeepCopy()
	ts := metav1.Now()
	deleting.SetDeletionTimestamp(&ts)
	if !machineDrainPredicate.Update(event.UpdateEvent{ObjectOld: steady2, ObjectNew: deleting}) {
		t.Fatal("transition into deletion must enqueue")
	}
	if !machineDrainPredicate.Delete(event.DeleteEvent{Object: deleting}) {
		t.Fatal("deleting Machine removal must enqueue")
	}
}

func TestCRDNodeBlockersPrintColumnAndDrainAlertRule(t *testing.T) {
	crdBytes, err := os.ReadFile(filepath.Join("..", "..", "config", "crd", "bases", "upgrade.0xshift.dev_hostedclusterupgrades.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var crd apiextensionsv1.CustomResourceDefinition
	if err := yaml.Unmarshal(crdBytes, &crd); err != nil {
		t.Fatal(err)
	}
	foundCol := false
	for _, v := range crd.Spec.Versions {
		for _, col := range v.AdditionalPrinterColumns {
			if col.Name == "Node-Blockers" && col.JSONPath == ".status.nodeBlockersSummary" && col.Type == "string" {
				foundCol = true
			}
		}
	}
	if !foundCol {
		t.Fatal("CRD missing Node-Blockers additionalPrinterColumn on .status.nodeBlockersSummary")
	}

	rulesBytes, err := os.ReadFile(filepath.Join("..", "..", "config", "prometheus", "prometheus_rule.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(rulesBytes), "alert: HypershiftUpgradeNodeDrainBlocked") || !strings.Contains(string(rulesBytes), "hypershift_upgrade_nodepool_drain_blocked_machines > 0") {
		t.Fatal("prometheus_rule.yaml missing HypershiftUpgradeNodeDrainBlocked alert")
	}
}
