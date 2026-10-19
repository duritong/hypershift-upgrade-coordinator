package controller

import (
	"context"
	"fmt"
	"testing"
	"time"

	upgradev1 "github.com/duritong/hypershift-upgrade-coordinator/api/v1alpha1"
	"github.com/duritong/hypershift-upgrade-coordinator/internal/guest"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func guestMetricSamples(t *testing.T, gauge *prometheus.GaugeVec, hcu *upgradev1.HostedClusterUpgrade) []*dto.Metric {
	t.Helper()
	var result []*dto.Metric
	for _, sample := range collectedGauges(t, gauge) {
		namespace, name := "", ""
		for _, label := range sample.Label {
			if label.GetName() == "namespace" {
				namespace = label.GetValue()
			}
			if label.GetName() == "name" {
				name = label.GetValue()
			}
		}
		if namespace == hcu.Namespace && name == hcu.Name {
			result = append(result, sample)
		}
	}
	return result
}

func TestGuestMetricsLifecycle(t *testing.T) {
	now := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
	hcu := &upgradev1.HostedClusterUpgrade{ObjectMeta: metav1.ObjectMeta{Namespace: "guest-metrics", Name: "example", Generation: 1}}
	hcu.Status.Phase = upgradev1.PhaseUpgradingNodePools
	expiry, read := metav1.NewTime(now.Add(time.Hour)), metav1.NewTime(now)
	hcu.Status.GuestAccess = &upgradev1.GuestAccessStatus{ObservedGeneration: 1, MaintenanceReady: false, CredentialAvailable: true, ExpiresAt: &expiry}
	hcu.Status.GuestAssessment = &upgradev1.GuestAssessmentStatus{ObservedGeneration: 1, PlanRevision: accessHash(hcu.Spec), State: "Unhealthy", Reason: "CODegraded", Purpose: string(guest.WorkerGuard), LastAttemptTime: read, LastSuccessfulObservation: &read, Operators: []upgradev1.GuestOperatorSummary{{Name: "monitoring"}, {Name: "network"}}, BlockerCount: 3, Truncated: true, Blockers: []upgradev1.GuestBlocker{{Operator: "monitoring", Reason: "CODegraded"}}}
	t.Cleanup(func() { clearUpgradeGauges(hcu.Namespace, hcu.Name) })
	value := func(g *prometheus.GaugeVec, want float64) {
		t.Helper()
		series := guestMetricSamples(t, g, hcu)
		if len(series) != 1 || series[0].GetGauge().GetValue() != want {
			t.Fatalf("unexpected gauge: %v want %v", series, want)
		}
	}
	exposeGuestMetrics(hcu, now)
	value(metricGuestLastRead, float64(now.Unix())) // bad health is still a successful read
	value(metricGuestCredentialAvailable, 1)
	value(metricGuestMaintenanceReady, 0)
	value(metricGuestReady, 0)
	value(metricGuestOperators, 2)
	value(metricGuestBlockers, 3)
	value(metricGuestTruncated, 1)
	value(metricGuestOperatorBlocker, 1)
	hcu.Status.GuestAssessment.Operators = []upgradev1.GuestOperatorSummary{{Name: "network"}}
	hcu.Status.GuestAssessment.Blockers = nil
	hcu.Status.GuestAssessment.BlockerCount = 0
	hcu.Status.GuestAssessment.State = "Ready"
	hcu.Status.GuestAssessment.Reason = "GuestReady"
	exposeGuestMetrics(hcu, now)
	value(metricGuestReady, 1)
	value(metricGuestOperators, 1)
	hcu.Status.GuestAccess.CredentialAvailable = false
	exposeGuestMetrics(hcu, now)
	value(metricGuestCredentialAvailable, 0)
	value(metricGuestReady, 1) // direct read verdict is independent of maintenance status
	if len(guestMetricSamples(t, metricGuestOperatorBlocker, hcu)) != 0 {
		t.Fatal("removed unhealthy CO retained metric")
	}
	// A failed subsequent read replaces verdict/operators but preserves the read
	// timestamp as a diagnostic, never as a healthy verdict.
	hcu.Status.GuestAssessment.State = "AccessDenied"
	hcu.Status.GuestAssessment.Reason = "raw token URL must never become a label"
	hcu.Status.GuestAssessment.Operators = nil
	exposeGuestMetrics(hcu, now)
	value(metricGuestReady, 0)
	value(metricGuestLastRead, float64(now.Unix()))
	for _, series := range guestMetricSamples(t, metricGuestAssessment, hcu) {
		for _, label := range series.Label {
			if label.GetName() == "reason" && label.GetValue() != "Other" {
				t.Fatal("uncontrolled reason exported")
			}
		}
	}
	for _, phase := range []upgradev1.UpgradePhase{upgradev1.PhasePaused, upgradev1.PhaseCompleted} {
		hcu.Status.Phase = phase
		exposeGuestMetrics(hcu, now)
		if len(guestMetricSamples(t, metricGuestAssessment, hcu)) != 0 || len(guestMetricSamples(t, metricGuestReady, hcu)) != 0 {
			t.Fatal("historical outcome exported as active verdict")
		}
		value(metricGuestLastRead, float64(now.Unix()))
	}
	other := hcu.DeepCopy()
	other.Namespace = "another-namespace"
	exposeGuestMetrics(other, now)
	t.Cleanup(func() { clearUpgradeGauges(other.Namespace, other.Name) })
	clearUpgradeGauges(hcu.Namespace, hcu.Name)
	for _, g := range guestMetricGauges() {
		if len(guestMetricSamples(t, g, hcu)) != 0 {
			t.Fatal("deletion retained guest gauge")
		}
	}
	if len(guestMetricSamples(t, metricGuestLastRead, other)) != 1 {
		t.Fatal("cleanup removed same-named HCU in another namespace")
	}
}

func TestGuestMetricsInvalidationAndBounds(t *testing.T) {
	now := time.Now().UTC()
	read, expiry := metav1.NewTime(now), metav1.NewTime(now.Add(time.Hour))
	base := &upgradev1.HostedClusterUpgrade{ObjectMeta: metav1.ObjectMeta{Namespace: "guest-metrics", Name: "bounds", Generation: 1}}
	base.Status.Phase = upgradev1.PhasePreflightChecks
	base.Status.GuestAccess = &upgradev1.GuestAccessStatus{ObservedGeneration: 1, MaintenanceReady: true, CredentialAvailable: true, ExpiresAt: &expiry}
	base.Status.GuestAssessment = &upgradev1.GuestAssessmentStatus{ObservedGeneration: 1, PlanRevision: accessHash(base.Spec), State: "Ready", Reason: "GuestReady", Purpose: string(guest.SourceBaseline), LastAttemptTime: read, LastSuccessfulObservation: &read}
	t.Cleanup(func() { clearUpgradeGauges(base.Namespace, base.Name) })
	for _, scenario := range []string{"stale", "future", "generation", "plan", "expired", "no-read", "observe", "unknown-purpose", "missing-plan", "changed-hc"} {
		t.Run(scenario, func(t *testing.T) {
			hcu := base.DeepCopy()
			at := now
			switch scenario {
			case "stale":
				at = now.Add(guest.DecisionAge + time.Second)
			case "future":
				at = now.Add(-time.Second)
			case "generation":
				hcu.Generation++
			case "plan":
				hcu.Spec.TargetReleaseImage = "edited"
			case "expired":
				hcu.Status.GuestAccess.ExpiresAt = &read
			case "no-read":
				hcu.Status.GuestAssessment.LastSuccessfulObservation = nil
			case "observe":
				hcu.Status.GuestAssessment.Purpose = string(guest.ObserveControlPlane)
			case "unknown-purpose":
				hcu.Status.GuestAssessment.Purpose = "arbitrary"
			case "missing-plan":
				hcu.Status.GuestAssessment.PlanRevision = ""
			case "changed-hc":
				hcu.Status.GuestAccess.HostedClusterUID = "replacement"
				hcu.Status.GuestAssessment.HostedClusterUID = "original"
			}
			exposeGuestMetrics(hcu, at)
			series := guestMetricSamples(t, metricGuestReady, hcu)
			if len(series) != 1 || series[0].GetGauge().GetValue() != 0 {
				t.Fatal("stale/invalid success active")
			}
		})
	}
	base.Status.GuestAssessment.State = "Unhealthy"
	for i := 0; i < 300; i++ {
		base.Status.GuestAssessment.Operators = append(base.Status.GuestAssessment.Operators, upgradev1.GuestOperatorSummary{Name: "monitoring"})
		base.Status.GuestAssessment.Blockers = append(base.Status.GuestAssessment.Blockers, upgradev1.GuestBlocker{Operator: fmt.Sprintf("operator-%03d", i), Reason: "raw message"})
	}
	exposeGuestMetrics(base, now)
	if got := guestMetricSamples(t, metricGuestOperators, base); len(got) != 1 || got[0].GetGauge().GetValue() != 256 {
		t.Fatal("operator count unbounded")
	}
	if len(guestMetricSamples(t, metricGuestOperatorBlocker, base)) != 32 {
		t.Fatal("blocker detail cap not enforced")
	}
	allowed := map[string]bool{"namespace": true, "name": true, "purpose": true, "state": true, "reason": true, "operator": true}
	for _, g := range guestMetricGauges() {
		for _, series := range guestMetricSamples(t, g, base) {
			for _, label := range series.Label {
				if !allowed[label.GetName()] {
					t.Fatalf("unexpected label %s", label.GetName())
				}
			}
		}
	}
	base.Status.GuestAssessment = nil
	exposeGuestMetrics(base, now)
	if len(guestMetricSamples(t, metricGuestOperatorBlocker, base)) != 0 {
		t.Fatal("absent assessment retained operator blockers")
	}
	if got := guestMetricSamples(t, metricGuestLastRead, base); len(got) != 1 || got[0].GetGauge().GetValue() != 0 {
		t.Fatal("never-read status exported a read timestamp")
	}
}

func TestGuestMetricsControllerHooks(t *testing.T) {
	hcu := &upgradev1.HostedClusterUpgrade{ObjectMeta: metav1.ObjectMeta{Namespace: "guest-metrics", Name: "hooks", Generation: 1}}
	r := &HostedClusterUpgradeReconciler{Client: &regressionClient{}}
	t.Cleanup(func() { clearUpgradeGauges(hcu.Namespace, hcu.Name) })
	r.exposePhaseMetrics(context.Background(), hcu, nil)
	if len(guestMetricSamples(t, metricGuestAssessment, hcu)) != 1 {
		t.Fatal("phase metrics did not expose missing guest assessment")
	}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(hcu)}); err != nil {
		t.Fatal(err)
	}
	for _, gauge := range guestMetricGauges() {
		if len(guestMetricSamples(t, gauge, hcu)) != 0 {
			t.Fatal("HCU NotFound reconcile retained guest metric")
		}
	}
}
