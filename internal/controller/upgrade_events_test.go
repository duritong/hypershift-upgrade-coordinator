package controller

import (
	"testing"
	"time"

	upgradev1 "github.com/duritong/hypershift-upgrade-coordinator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/event"
)

func TestUpgradeEventPredicate(t *testing.T) {
	before := regressionHCU(upgradev1.PhasePreflightChecks)
	before.Status.GuestAssessment = &upgradev1.GuestAssessmentStatus{State: "Unhealthy", Reason: "CODegraded", Purpose: "SourceBaseline", LastAttemptTime: metav1.NewTime(time.Now().Add(-time.Minute))}
	before.Status.Conditions = []metav1.Condition{{Type: "GuestAssessmentReady", Status: metav1.ConditionFalse, Reason: "CODegraded", ObservedGeneration: 1, LastTransitionTime: metav1.NewTime(time.Now().Add(-time.Hour))}}
	before.Status.GuestAccess = &upgradev1.GuestAccessStatus{MaintenanceReady: true, CredentialAvailable: true, Reason: "CredentialCurrent", ObserverUsability: "Unknown", ExpiresAt: &metav1.Time{Time: time.Now().Add(time.Hour)}}
	for _, tc := range []struct {
		name   string
		mutate func(*upgradev1.HostedClusterUpgrade)
		want   bool
	}{
		{"observation timestamps", func(u *upgradev1.HostedClusterUpgrade) {
			now := metav1.Now()
			u.Status.GuestAssessment.LastAttemptTime = now
			u.Status.GuestAssessment.LastSuccessfulObservation = &now
		}, false},
		{"guest assessment result", func(u *upgradev1.HostedClusterUpgrade) {
			u.Status.GuestAssessment.State = "Ready"
			u.Status.GuestAssessment.Reason = "GuestReady"
			u.Status.Conditions[0].Status = metav1.ConditionTrue
		}, false},
		{"telemetry", func(u *upgradev1.HostedClusterUpgrade) {
			u.Status.Message = "new diagnostics"
			u.Status.AggregatedNodeStatus.Ready++
		}, false},
		{"pool phase", func(u *upgradev1.HostedClusterUpgrade) {
			u.Status.NodePoolStatuses = []upgradev1.NodePoolUpgradeStatus{{Name: "workers", Phase: "Completed"}}
		}, true},
		{"own conditions", func(u *upgradev1.HostedClusterUpgrade) {
			u.Status.Conditions = append(u.Status.Conditions, metav1.Condition{Type: "Ready", Status: metav1.ConditionFalse, Reason: "Waiting"})
		}, false},
		{"resource version", func(u *upgradev1.HostedClusterUpgrade) { u.ResourceVersion = "next" }, false},
		{"finalizer registration", func(u *upgradev1.HostedClusterUpgrade) { u.Finalizers = []string{workerLocksFinalizer} }, false},
		{"phase", func(u *upgradev1.HostedClusterUpgrade) { u.Status.Phase = upgradev1.PhasePendingSchedule }, true},
		{"new generation", func(u *upgradev1.HostedClusterUpgrade) { u.Generation++ }, true},
		{"new UID", func(u *upgradev1.HostedClusterUpgrade) { u.UID = "replacement" }, true},
		{"pause", func(u *upgradev1.HostedClusterUpgrade) { p := "true"; u.Spec.PausedUntil = &p }, true},
		{"spec", func(u *upgradev1.HostedClusterUpgrade) { u.Spec.TargetReleaseImage = regressionImage("4.22.0") }, true},
		{"deletion", func(u *upgradev1.HostedClusterUpgrade) { now := metav1.Now(); u.DeletionTimestamp = &now }, true},
		{"access available", func(u *upgradev1.HostedClusterUpgrade) { u.Status.GuestAccess.CredentialAvailable = false }, true},
		{"access renewal", func(u *upgradev1.HostedClusterUpgrade) {
			u.Status.GuestAccess.ExpiresAt = &metav1.Time{Time: time.Now().Add(2 * time.Hour)}
		}, true},
		{"access endpoint", func(u *upgradev1.HostedClusterUpgrade) { u.Status.GuestAccess.EndpointRevision = "new-endpoint" }, true},
		{"access removed", func(u *upgradev1.HostedClusterUpgrade) { u.Status.GuestAccess = nil }, true},
		{"foreign condition timestamp", func(u *upgradev1.HostedClusterUpgrade) {
			u.Status.Conditions = append(u.Status.Conditions, metav1.Condition{Type: "Unrelated", Status: metav1.ConditionTrue})
		}, false},
		{"access condition", func(u *upgradev1.HostedClusterUpgrade) {
			u.Status.Conditions = append(u.Status.Conditions, metav1.Condition{Type: "GuestCredentialAvailable", Status: metav1.ConditionFalse, Reason: "CredentialExpired", ObservedGeneration: 1})
		}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			after := before.DeepCopy()
			tc.mutate(after)
			if got := upgradeEventPredicate.Update(event.UpdateEvent{ObjectOld: before, ObjectNew: after}); got != tc.want {
				t.Fatalf("enqueue=%t want=%t", got, tc.want)
			}
		})
	}
	if !upgradeEventPredicate.Create(event.CreateEvent{Object: before}) || !upgradeEventPredicate.Delete(event.DeleteEvent{Object: before}) || !upgradeEventPredicate.Generic(event.GenericEvent{Object: before}) {
		t.Fatal("lifecycle/manual events suppressed")
	}
	for _, typ := range []string{"GuestAccessMaintenanceReady", "GuestCredentialAvailable"} {
		a := before.DeepCopy()
		a.Status.Conditions = append(a.Status.Conditions, metav1.Condition{Type: typ, Status: metav1.ConditionTrue, Reason: "CredentialCurrent", ObservedGeneration: 1, LastTransitionTime: metav1.Now()})
		b := a.DeepCopy()
		b.Status.Conditions[len(b.Status.Conditions)-1].LastTransitionTime = metav1.NewTime(time.Now().Add(time.Second))
		b.Status.Conditions[len(b.Status.Conditions)-1].Message = "new diagnostic message"
		if upgradeEventPredicate.Update(event.UpdateEvent{ObjectOld: a, ObjectNew: b}) {
			t.Fatalf("%s timestamp/message woke queue", typ)
		}
		b.Status.Conditions[len(b.Status.Conditions)-1].Status = metav1.ConditionFalse
		if !upgradeEventPredicate.Update(event.UpdateEvent{ObjectOld: a, ObjectNew: b}) {
			t.Fatalf("%s readiness transition lost", typ)
		}
	}
	a := before.DeepCopy()
	a.Status.NodePoolStatuses = []upgradev1.NodePoolUpgradeStatus{{Name: "workers", Phase: "Upgrading"}, {Name: "later", Phase: "WaitingForGroup"}}
	b := a.DeepCopy()
	b.Status.NodePoolStatuses[0].Nodes.Ready++
	b.Status.NodePoolStatuses[0].Blockers = []string{"new diagnostics"}
	b.Status.NodePoolStatuses[0], b.Status.NodePoolStatuses[1] = b.Status.NodePoolStatuses[1], b.Status.NodePoolStatuses[0]
	if upgradeEventPredicate.Update(event.UpdateEvent{ObjectOld: a, ObjectNew: b}) {
		t.Fatal("pool telemetry/order change woke queue")
	}
}

func TestBlockedGuestObservationKeepsHeartbeat(t *testing.T) {
	f := newUpgradeFlow(t, false, "Replace", 0, 1)
	f.guestCondition(false, "Degraded", true)
	f.until(upgradev1.PhasePreflightChecks)
	if result := f.step(); result.RequeueAfter != 30*time.Second {
		t.Fatalf("blocked observation heartbeat=%s want=30s", result.RequeueAfter)
	}
}
