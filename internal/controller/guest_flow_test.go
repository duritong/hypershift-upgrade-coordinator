package controller

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	upgradev1 "github.com/duritong/hypershift-upgrade-coordinator/api/v1alpha1"
	"github.com/duritong/hypershift-upgrade-coordinator/internal/guest"
	configv1 "github.com/openshift/api/config/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
)

func (f *upgradeFlow) guestCondition(cv bool, name string, value bool) {
	f.t.Helper()
	f.guest.mu.Lock()
	defer f.guest.mu.Unlock()
	cs := &f.guest.operators[0].Status.Conditions
	if cv {
		cs = &f.guest.version.Status.Conditions
	}
	for i := range *cs {
		if string((*cs)[i].Type) == name {
			(*cs)[i].Status = configv1.ConditionFalse
			if value {
				(*cs)[i].Status = configv1.ConditionTrue
			}
			return
		}
	}
	f.t.Fatalf("missing fixture condition %s", name)
}
func (f *upgradeFlow) readyTarget() {
	f.t.Helper()
	hops := 1
	if f.hcu().Spec.IntermediateReleaseImage != "" {
		hops = 2
	}
	for range hops {
		f.until(upgradev1.PhaseVerifyingControlPlane)
		f.publish(f.completeCP())
		f.step()
	}
	f.until(upgradev1.PhaseUpgradingNodePools)
}
func (f *upgradeFlow) assertRequestsUnchanged(before []string) {
	f.t.Helper()
	if !reflect.DeepEqual(before, f.requests()) {
		f.t.Fatalf("forbidden release mutation: before=%v after=%v status=%+v", before, f.requests(), f.hcu().Status)
	}
}

// F3: No source-side guest failure may acquire initial holds or request a CP.
func TestGuestFlowSourceMandatoryBeforeLocks(t *testing.T) {
	for _, failure := range []string{"co", "cv", "denied", "nil-provider"} {
		t.Run(failure, func(t *testing.T) {
			f := newUpgradeFlow(t, false, "Replace", 2, 2)
			switch failure {
			case "co":
				f.guestCondition(false, "Degraded", true)
			case "cv":
				f.guestCondition(true, "Failing", true)
			case "denied":
				f.guest.mu.Lock()
				f.guest.denyObserver = true
				f.guest.mu.Unlock()
			case "nil-provider":
				f.r.GuestAccess = nil
			}
			for range 3 {
				f.step()
			}
			if f.c.writes != 0 || len(f.requests()) != 0 || f.hcu().Status.Phase != upgradev1.PhasePreflightChecks {
				t.Fatalf("unsafe source mutation: writes=%d status=%+v", f.c.writes, f.hcu().Status)
			}
			for _, name := range []string{"pool-0", "pool-1"} {
				if f.object("NodePool", name).GetAnnotations()[workerLockOwner] != "" {
					t.Fatal("source failure acquired worker hold")
				}
			}
			if failure == "nil-provider" {
				f.r.GuestAccess = &f.access.GuestAccessProvider
			} else if failure == "co" {
				f.guestCondition(false, "Degraded", false)
			} else if failure == "cv" {
				f.guestCondition(true, "Failing", false)
			} else {
				f.guest.mu.Lock()
				f.guest.denyObserver = false
				f.guest.mu.Unlock()
			}
			f.until(upgradev1.PhaseVerifyingControlPlane)
		})
	}
}

// F2/F4: traverse the actual final CP transition, not a seeded worker phase.
func TestGuestFlowWorkerEntryProgressionAndOldVersions(t *testing.T) {
	for _, eus := range []bool{false, true} {
		for _, strategy := range []string{"Replace", "InPlace"} {
			t.Run(fmt.Sprintf("eus=%t/%s", eus, strategy), func(t *testing.T) {
				f := newUpgradeFlow(t, eus, strategy, 2, 2)
				if eus {
					f.until(upgradev1.PhaseVerifyingControlPlane)
					f.publish(f.completeCP())
					f.guestCondition(false, "Progressing", true)
					before := f.requests()
					f.step()
					f.assertRequestsUnchanged(before)
					if f.hcu().Status.Phase != upgradev1.PhaseVerifyingControlPlane {
						t.Fatal("intermediate progression bypassed strict gate")
					}
					f.guestCondition(false, "Progressing", false)
					f.step()
				}
				f.until(upgradev1.PhaseVerifyingControlPlane)
				f.publish(f.completeCP())
				f.guestCondition(true, "Progressing", true)
				f.guestCondition(false, "Progressing", true)
				f.guest.mu.Lock()
				f.guest.operators[0].Status.Versions[0].Version = "4.20.0"
				f.guest.mu.Unlock()
				before := f.requests()
				f.step()
				f.assertRequestsUnchanged(before)
				if f.hcu().Status.Phase != upgradev1.PhaseVerifyingControlPlane || f.hcu().Status.GuestAssessment.Reason != "OperatorVersionMismatch" {
					t.Fatalf("old CO admitted worker entry: %+v", f.hcu().Status)
				}
				version, _ := upgradev1.ExtractVersion(f.hcu().Spec.TargetReleaseImage)
				f.guest.mu.Lock()
				f.guest.operators[0].Status.Versions[0].Version = version
				f.guest.mu.Unlock()
				f.step()
				if f.hcu().Status.Phase != upgradev1.PhaseUpgradingNodePools || f.hcu().Status.WorkerAdmission == nil {
					t.Fatalf("hidden strict entry gate: %+v", f.hcu().Status)
				}
				f.step()
				if len(f.requests()) != len(before)+2 {
					t.Fatalf("healthy progression did not start both workers: %v", f.requests())
				}
				f.completeWorker("pool-0")
				f.completeWorker("pool-1")
				f.step()
				if f.hcu().Status.Phase == upgradev1.PhaseCompleted {
					t.Fatal("progressing guest bypassed strict final completion")
				}
				f.guestCondition(true, "Progressing", false)
				f.guestCondition(false, "Progressing", false)
				f.until(upgradev1.PhaseCompleted)
			})
		}
	}
}

// F1/F2/F6: freshly read CV failure wins over unchanged healthy management proof.
func TestGuestFlowDirectCVFailureAndFinalBothBranches(t *testing.T) {
	for _, eus := range []bool{false, true} {
		for _, pools := range []int{0, 1} {
			t.Run(fmt.Sprintf("eus=%t/pools=%d", eus, pools), func(t *testing.T) {
				f := newUpgradeFlow(t, eus, "Replace", pools, 1)
				hops := 1
				if eus {
					hops = 2
				}
				for range hops {
					f.until(upgradev1.PhaseVerifyingControlPlane)
					f.publish(f.completeCP())
					f.guestCondition(true, "Failing", true)
					before := f.requests()
					f.step()
					f.assertRequestsUnchanged(before)
					if f.hcu().Status.Phase != upgradev1.PhaseVerifyingControlPlane || f.hcu().Status.GuestAssessment.State != "Unhealthy" {
						t.Fatalf("new CV failure hidden by healthy HC: %+v", f.hcu().Status)
					}
					f.guestCondition(true, "Failing", false)
					f.step()
				}
				f.until(upgradev1.PhaseUpgradingNodePools)
				if pools > 0 {
					f.step()
					f.completeWorker("pool-0")
				}
				f.guestCondition(true, "Failing", true)
				before := f.requests()
				f.step()
				f.assertRequestsUnchanged(before)
				if f.hcu().Status.Phase == upgradev1.PhaseCompleted {
					t.Fatal("final CV failure passed")
				}
				if pools > 0 && f.hcu().Status.NodePoolStatuses[0].Phase != "Completed" {
					t.Fatal("guest failure erased worker completion")
				}
				f.guestCondition(true, "Failing", false)
				f.until(upgradev1.PhaseCompleted)
			})
		}
	}
}

// F5/F7: live inventory and health inhibit only NEW actions, never delegated A.
func TestGuestFlowDynamicInventoryAndActiveReservation(t *testing.T) {
	for _, strategy := range []string{"Replace", "InPlace"} {
		t.Run(strategy, func(t *testing.T) {
			f := newUpgradeFlow(t, false, strategy, 2, 1)
			f.readyTarget()
			f.step()
			started := f.hcu().Status.NodePoolStatuses[0].StartTime.DeepCopy()
			f.guest.mu.Lock()
			bad := *f.guest.operators[0].DeepCopy()
			bad.Name = "new-operator"
			bad.UID = "new-uid"
			bad.Status.Conditions = fixtureConditions(map[string]bool{"Available": true, "Degraded": true, "Progressing": false})
			f.guest.operators = append(f.guest.operators, bad)
			f.guest.mu.Unlock()
			before := f.requests()
			f.step()
			f.assertRequestsUnchanged(before)
			if f.object("NodePool", "pool-0").GetAnnotations()[workerLockOwner] != "" || !f.hcu().Status.NodePoolStatuses[0].StartTime.Equal(started) {
				t.Fatal("guest failure cancelled/timed anew active worker")
			}
			f.completeWorker("pool-0")
			f.step()
			f.assertRequestsUnchanged(before)
			if f.hcu().Status.NodePoolStatuses[0].Phase != "Completed" {
				t.Fatal("completion lost while B blocked")
			}
			f.guest.mu.Lock()
			f.guest.operators = f.guest.operators[:1]
			f.guest.mu.Unlock()
			f.step()
			f.step()
			if len(f.requests()) != len(before)+1 || len(f.hcu().Status.GuestAssessment.Operators) != 1 {
				t.Fatalf("removed unhealthy CO retained: %+v", f.hcu().Status)
			}
			f.completeWorker("pool-1")
			f.guest.mu.Lock()
			remaining := *f.guest.operators[0].DeepCopy()
			remaining.Name = "replacement"
			remaining.UID = "replacement-uid"
			f.guest.operators = []configv1.ClusterOperator{remaining}
			f.guest.mu.Unlock()
			f.until(upgradev1.PhaseCompleted)
			if f.hcu().Status.GuestAssessment.Operators[0].Name != "replacement" {
				t.Fatal("removed healthy CO retained")
			}
		})
	}
}

func TestGuestFlowEmptyListAndFailureAtScheduledCPAction(t *testing.T) {
	f := newUpgradeFlow(t, false, "Replace", 1, 1)
	f.until(upgradev1.PhasePendingSchedule)
	before := f.requests()
	f.guest.mu.Lock()
	f.guest.operators = []configv1.ClusterOperator{}
	f.guest.mu.Unlock()
	f.step()
	f.step()
	f.assertRequestsUnchanged(before)
	if f.hcu().Status.Phase == upgradev1.PhaseVerifyingControlPlane {
		t.Fatal("empty list authorized CP mutation")
	}
	setFlowGuestRelease(f.guest, regressionImage("4.20.0"))
	f.until(upgradev1.PhaseVerifyingControlPlane)
}

type validatingGuestAccess struct {
	GuestAccessReader
	afterValidate func()
}

func (p validatingGuestAccess) ValidateTarget(ctx context.Context, target guest.Target) error {
	err := p.GuestAccessReader.ValidateTarget(ctx, target)
	if p.afterValidate != nil {
		p.afterValidate()
	}
	return err
}

// F8/F11: actual HTTP collection followed by current-identity/age revalidation.
func TestGuestFlowDecisionFences(t *testing.T) {
	for _, change := range []string{"secret", "target", "hcu-uid", "hc-uid", "stale"} {
		t.Run(change, func(t *testing.T) {
			f := newUpgradeFlow(t, false, "Replace", 1, 1)
			f.until(upgradev1.PhaseUpgradingControlPlane)
			if change == "stale" {
				f.r.GuestAccess = validatingGuestAccess{GuestAccessReader: &f.access.GuestAccessProvider, afterValidate: func() { f.clock.Step(guest.DecisionAge + time.Second) }}
			} else {
				f.guest.mu.Lock()
				f.guest.onObserverRead = func() {
					f.guest.onObserverRead = nil
					switch change {
					case "hcu-uid":
						u := f.hcu()
						u.UID = "replacement-upgrade"
						_ = f.c.store(u)
					case "secret":
						s := &corev1.Secret{}
						_ = f.c.Get(context.Background(), client.ObjectKey{Namespace: "operator", Name: deliveryName("flow-cluster")}, s)
						s.ResourceVersion += "-rotated"
						_ = f.c.store(s)
					case "target":
						u := f.hcu()
						u.Generation++
						u.Spec.ControlPlane.StartAt = &metav1.Time{Time: f.clock.Now().Add(time.Hour)}
						_ = f.c.store(u)
					case "hc-uid":
						hc := f.object("HostedCluster", "example")
						hc.SetUID("replacement-hc")
						_ = f.c.store(hc)
					}
				}
				f.guest.mu.Unlock()
			}
			before := f.requests()
			// A plan edit correctly refuses even stale status persistence.
			_, err := f.r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKey{Namespace: "clusters", Name: "example"}})
			if err != nil && change != "target" && change != "hcu-uid" {
				t.Fatal(err)
			}
			f.assertRequestsUnchanged(before)
			if change == "stale" && f.hcu().Status.GuestAssessment.Reason != "StaleOrIncompleteSnapshot" {
				t.Fatalf("stale observation accepted: %+v", f.hcu().Status.GuestAssessment)
			}
		})
	}
}

// F9/F10: paused observation, independent renewal, restart and status-loss adoption.
func TestGuestFlowPauseRenewalAndLostAdmission(t *testing.T) {
	f := newUpgradeFlow(t, false, "InPlace", 2, 1)
	f.readyTarget()
	f.step()
	hcu := f.hcu()
	pause := "true"
	hcu.Spec.PausedUntil = &pause
	f.publish(hcu)
	before := f.requests()
	tokens := f.guest.tokens
	f.clock.Step(51 * time.Minute)
	f.step()
	f.assertRequestsUnchanged(before)
	if f.guest.tokens != tokens+1 || f.hcu().Status.Phase != upgradev1.PhasePaused {
		t.Fatal("independent paused renewal failed")
	}
	hcu = f.hcu()
	hcu.Spec.PausedUntil = nil
	hcu.Status.WorkerAdmission = nil
	f.publish(hcu)
	f.r = &HostedClusterUpgradeReconciler{Client: f.c, APIReader: f.c, GuestAccess: &f.access.GuestAccessProvider, Clock: f.clock, ReleaseResolver: fixtureReleaseResolver{}}
	f.step()
	f.assertRequestsUnchanged(before)
	if f.hcu().Status.WorkerAdmission == nil || f.object("NodePool", "pool-0").GetAnnotations()[workerLockOwner] != "" {
		t.Fatal("restart did not adopt active work")
	}
	f.completeWorker("pool-0")
	f.step()
	f.step()
	f.completeWorker("pool-1")
	f.until(upgradev1.PhaseCompleted)
}

func TestGuestFlowAccessRefreshIndependentQueue(t *testing.T) {
	for _, kind := range []string{"admin-rotation", "forbidden"} {
		t.Run(kind, func(t *testing.T) {
			f := newUpgradeFlow(t, false, "Replace", 1, 1)
			f.until(upgradev1.PhaseUpgradingControlPlane)
			old := f.hcu()
			tokens := f.guest.tokens
			if kind == "admin-rotation" {
				s := &corev1.Secret{}
				f.get(s, "clusters", "guest-admin")
				s.ResourceVersion += "-rotated"
				f.publish(s)
			} else {
				f.guest.mu.Lock()
				f.guest.denyObserver = true
				f.guest.mu.Unlock()
			}
			before := f.requests()
			_, err := f.r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(old)})
			if err != nil {
				t.Fatal(err)
			}
			f.assertRequestsUnchanged(before)
			current := f.hcu()
			if !guestAccessRefreshEvent(event.UpdateEvent{ObjectOld: old, ObjectNew: current}) {
				t.Fatal("access failure did not wake independent queue")
			}
			if f.guest.tokens != tokens {
				t.Fatal("upgrade queue performed administrative issuance")
			}
			f.maintainAccess()
			if f.guest.tokens != tokens+1 {
				t.Fatal("independent queue waited for scheduled renewal")
			}
			f.guest.mu.Lock()
			f.guest.denyObserver = false
			f.guest.mu.Unlock()
			f.until(upgradev1.PhaseVerifyingControlPlane)
		})
	}
}

func TestGuestFlowCompletedNewRunPreservesAccess(t *testing.T) {
	f := newUpgradeFlow(t, false, "Replace", 0, 1)
	f.readyTarget()
	f.until(upgradev1.PhaseCompleted)
	u := f.hcu()
	access := u.Status.GuestAccess.DeepCopy()
	u.Generation++
	u.Spec.TargetReleaseImage = regressionImage("4.22.0")
	f.publish(u)
	f.step()
	if !reflect.DeepEqual(f.hcu().Status.GuestAccess, access) || f.hcu().Status.GuestAssessment != nil || f.hcu().Status.WorkerAdmission != nil || f.hcu().Status.Phase != upgradev1.PhasePreflightChecks {
		t.Fatalf("new run erased foreign state or retained authorization: %+v", f.hcu().Status)
	}
	for _, condition := range f.hcu().Status.Conditions {
		if strings.HasPrefix(condition.Type, "GuestAccess") && condition.Reason == "" {
			t.Fatal("foreign condition lost")
		}
	}
}

func TestGuestFlowFreshGateForEachAction(t *testing.T) {
	for _, boundary := range []string{"cp", "workers"} {
		t.Run(boundary, func(t *testing.T) {
			f := newUpgradeFlow(t, false, "Replace", 2, 2)
			if boundary == "cp" {
				f.until(upgradev1.PhaseUpgradingControlPlane)
			} else {
				f.readyTarget()
			}
			reads := 0
			// Change the CO at the next collection boundary, independent of the
			// number of optional-component reads inside each CV bracket.
			trigger := 3
			if boundary == "workers" {
				trigger = 5
			}
			f.guest.mu.Lock()
			f.guest.onObserverReadPath = func(path string) {
				if path != "/apis/config.openshift.io/v1/clusterversions/version" {
					return
				}
				reads++
				if reads == trigger {
					f.guest.operators[0].Status.Conditions = fixtureConditions(map[string]bool{"Available": true, "Degraded": true, "Progressing": false})
				}
			}
			f.guest.mu.Unlock()
			before := f.requests()
			f.step()
			if boundary == "cp" {
				f.assertRequestsUnchanged(before)
			} else {
				if len(f.requests()) != len(before)+1 || !strings.Contains(f.requests()[len(before)], "pool-0") {
					t.Fatalf("later action reused A's snapshot: %v", f.requests())
				}
				if f.object("NodePool", "pool-0").GetAnnotations()[workerLockOwner] != "" || f.object("NodePool", "pool-1").GetAnnotations()[workerLockOwner] == "" {
					t.Fatal("guard failure cancelled A or unlocked B")
				}
			}
		})
	}
}

func TestGuestFlowRootAdoptsAlreadyTargetActiveWorkers(t *testing.T) {
	f := newUpgradeFlow(t, false, "InPlace", 2, 1)
	f.readyTarget()
	f.step()
	u := f.hcu()
	u.Status = upgradev1.HostedClusterUpgradeStatus{GuestAccess: u.Status.GuestAccess}
	f.publish(u)
	f.guestCondition(true, "Progressing", true)
	f.guestCondition(false, "Progressing", true)
	before := f.requests()
	f.until(upgradev1.PhaseUpgradingNodePools)
	f.step()
	f.assertRequestsUnchanged(before)
	if f.object("NodePool", "pool-0").GetAnnotations()[workerLockOwner] != "" || f.hcu().Status.WorkerAdmission == nil {
		t.Fatal("root adoption relocked or demanded pristine source")
	}
	f.completeWorker("pool-0")
	f.step()
	f.step()
	f.completeWorker("pool-1")
	f.guestCondition(true, "Progressing", false)
	f.guestCondition(false, "Progressing", false)
	f.until(upgradev1.PhaseCompleted)
}

type conflictUpgradeClient struct {
	client.Client
	hook     func(context.Context) error
	attempts int
}
type conflictUpgradeWriter struct {
	client.SubResourceWriter
	owner *conflictUpgradeClient
}

func (c *conflictUpgradeClient) Status() client.SubResourceWriter {
	return conflictUpgradeWriter{SubResourceWriter: c.Client.Status(), owner: c}
}
func (w conflictUpgradeWriter) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	w.owner.attempts++
	if w.owner.hook != nil {
		hook := w.owner.hook
		w.owner.hook = nil
		if err := hook(ctx); err != nil {
			return err
		}
	}
	return w.SubResourceWriter.Update(ctx, obj, opts...)
}

func TestGuestFlowStatusConflictRebasesWithoutActuationReplay(t *testing.T) {
	f := newUpgradeFlow(t, false, "Replace", 1, 1)
	f.until(upgradev1.PhaseUpgradingControlPlane)
	c := &conflictUpgradeClient{Client: f.c}
	c.hook = func(ctx context.Context) error {
		fresh := f.hcu()
		fresh.Status.GuestAccess.Reason = "ConcurrentRotation"
		meta.SetStatusCondition(&fresh.Status.Conditions, metav1.Condition{Type: "GuestAccessMaintenanceReady", Status: metav1.ConditionTrue, Reason: "ConcurrentRotation", Message: "access writer", LastTransitionTime: metav1.NewTime(f.clock.Now())})
		meta.SetStatusCondition(&fresh.Status.Conditions, metav1.Condition{Type: "ExternalCondition", Status: metav1.ConditionTrue, Reason: "ExternalWriter", Message: "foreign writer", LastTransitionTime: metav1.NewTime(f.clock.Now())})
		f.publish(fresh)
		return apierrors.NewConflict(schema.GroupResource{Group: upgradev1.GroupVersion.Group, Resource: "hostedclusterupgrades"}, fresh.Name, errors.New("injected concurrent access status"))
	}
	f.r.Client = c
	before := f.requests()
	tokens := f.guest.tokens
	f.step()
	got := f.hcu()
	if c.attempts != 2 || len(f.requests()) != len(before)+1 || f.guest.tokens != tokens || got.Status.GuestAccess.Reason != "ConcurrentRotation" || meta.FindStatusCondition(got.Status.Conditions, "ExternalCondition") == nil || got.Status.Phase != upgradev1.PhaseVerifyingControlPlane {
		t.Fatalf("status conflict lost state/replayed action: attempts=%d requests=%v status=%+v", c.attempts, f.requests(), got.Status)
	}
}

func TestGuestFlowCancellationFailsClosed(t *testing.T) {
	f := newUpgradeFlow(t, false, "Replace", 1, 1)
	f.until(upgradev1.PhaseUpgradingControlPlane)
	f.guest.mu.Lock()
	f.guest.holdObserver = true
	f.guest.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	before := f.requests()
	_, err := f.r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKey{Namespace: "clusters", Name: "example"}})
	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	f.assertRequestsUnchanged(before)
	if f.hcu().Status.Phase == upgradev1.PhaseVerifyingControlPlane {
		t.Fatal("canceled guest read authorized CP")
	}
}

func TestGuestFlowMultiClusterIsolation(t *testing.T) {
	t.Cleanup(func() { clearUpgradeGauges("clusters", "other") })
	f := newUpgradeFlow(t, false, "Replace", 0, 1)
	g := newAccessGuest(t, f.clock.Now().Add(time.Hour))
	g.clusterID = "b5e99fb2-0598-4b34-9081-14236b744c16"
	setFlowGuestRelease(g, regressionImage("4.20.0"))
	hcu := regressionHCU("")
	hcu.Name = "other"
	hcu.UID = "other-upgrade"
	hc := regressionHC("4.20.0")
	hc.SetName("other")
	hc.SetUID("other-hc")
	hc.SetGeneration(1)
	setHealthyHostedClusterStatus(hc, f.clock.Now())
	_ = unstructured.SetNestedField(hc.Object, g.clusterID, "spec", "clusterID")
	_ = unstructured.SetNestedField(hc.Object, "other-admin", "status", "kubeconfig", "name")
	admin := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "clusters", Name: "other-admin", UID: "other-admin", ResourceVersion: "1"}, Data: map[string][]byte{"kubeconfig": accessKubeconfig(t, guestFixtureConfig(g), "admin-token")}}
	f.c.objects = append(f.c.objects, hcu, hc, admin)
	addReadyCP(f.c.regressionClient, hc)
	key := client.ObjectKeyFromObject(hcu)
	if _, err := f.access.Reconcile(context.Background(), ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatal(err)
	}
	g.mu.Lock()
	g.denyObserver = true
	g.mu.Unlock()
	for range 3 {
		if _, err := f.r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key}); err != nil {
			t.Fatal(err)
		}
	}
	other := &upgradev1.HostedClusterUpgrade{}
	f.get(other, key.Namespace, key.Name)
	if other.Status.Phase != upgradev1.PhasePreflightChecks || other.Status.GuestAssessment.State != "AccessDenied" {
		t.Fatalf("other cluster denial hidden: %+v", other.Status)
	}
	f.readyTarget()
	f.until(upgradev1.PhaseCompleted)
	f.get(other, key.Namespace, key.Name)
	if other.Status.Phase != upgradev1.PhasePreflightChecks {
		t.Fatal("healthy cluster authorized denied cluster")
	}
	for _, request := range f.requests() {
		if strings.Contains(request, "/other/") {
			t.Fatal("cross-cluster release write")
		}
	}
}

type lostUpgradeResponseClient struct {
	client.Client
	kind string
	lost bool
}

func (c *lostUpgradeResponseClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	if err := c.Client.Update(ctx, obj, opts...); err != nil {
		return err
	}
	if obj.GetObjectKind().GroupVersionKind().Kind == c.kind && !c.lost {
		c.lost = true
		return errors.New("simulated write response loss")
	}
	return nil
}

func TestGuestFlowAdoptsLostMutationResponses(t *testing.T) {
	for _, kind := range []string{"HostedCluster", "NodePool"} {
		t.Run(kind, func(t *testing.T) {
			f := newUpgradeFlow(t, false, "Replace", 1, 1)
			if kind == "HostedCluster" {
				f.until(upgradev1.PhaseUpgradingControlPlane)
			} else {
				f.readyTarget()
			}
			c := &lostUpgradeResponseClient{Client: f.c, kind: kind}
			f.r.Client = c
			_, err := f.r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKey{Namespace: "clusters", Name: "example"}})
			if kind == "HostedCluster" && err == nil {
				t.Fatal("CP write response loss not propagated")
			}
			if !c.lost {
				t.Fatal("no actual mutation response was lost")
			}
			before := f.requests()
			f.r.Client = f.c
			f.step()
			f.assertRequestsUnchanged(before)
			if kind == "HostedCluster" {
				f.publish(f.completeCP())
				f.until(upgradev1.PhaseUpgradingNodePools)
				f.step()
			}
			f.completeWorker("pool-0")
			f.until(upgradev1.PhaseCompleted)
			if len(f.requests()) != 2 {
				t.Fatalf("response loss replayed mutation: %v", f.requests())
			}
		})
	}
}

func TestGuestFlowNewRunResetConflictPreservesRotation(t *testing.T) {
	f := newUpgradeFlow(t, false, "Replace", 0, 1)
	f.readyTarget()
	f.until(upgradev1.PhaseCompleted)
	u := f.hcu()
	u.Generation++
	u.Spec.TargetReleaseImage = regressionImage("4.22.0")
	f.publish(u)
	c := &conflictUpgradeClient{Client: f.c, hook: func(context.Context) error {
		fresh := f.hcu()
		fresh.Status.GuestAccess.Reason = "ConcurrentRotation"
		f.publish(fresh)
		return apierrors.NewConflict(schema.GroupResource{Resource: "hostedclusterupgrades"}, fresh.Name, errors.New("access writer"))
	}}
	f.r.Client = c
	before := f.requests()
	f.step()
	f.assertRequestsUnchanged(before)
	if f.hcu().Status.GuestAccess.Reason != "ConcurrentRotation" || f.hcu().Status.GuestAssessment != nil || f.hcu().Status.WorkerAdmission != nil {
		t.Fatalf("reset/status conflict lost ownership: %+v", f.hcu().Status)
	}
}

type staleManagementHCClient struct {
	client.Client
	hc  *unstructured.Unstructured
	hcp *unstructured.Unstructured
}

func (c staleManagementHCClient) Get(ctx context.Context, key client.ObjectKey, out client.Object, opts ...client.GetOption) error {
	if u, ok := out.(*unstructured.Unstructured); ok && c.hcp != nil && u.GroupVersionKind() == gvkHCP && key == client.ObjectKeyFromObject(c.hcp) {
		u.Object = c.hcp.DeepCopy().Object
		return nil
	}
	if u, ok := out.(*unstructured.Unstructured); ok && u.GroupVersionKind() == gvkHC && key == client.ObjectKeyFromObject(c.hc) {
		u.Object = c.hc.DeepCopy().Object
		return nil
	}
	return c.Client.Get(ctx, key, out, opts...)
}

func TestGuestFlowManagementRevisionFence(t *testing.T) {
	for _, boundary := range []string{"worker", "zero-completion", "all-completion"} {
		for _, timing := range []string{"cache-lag", "during-read"} {
			for _, change := range []string{"hc-generation", "hcp-generation", "hc-override", "hcp-override", "hcp-uid"} {
				t.Run(boundary+"/"+timing+"/"+change, func(t *testing.T) {
					pools := 1
					if boundary == "zero-completion" {
						pools = 0
					}
					f := newUpgradeFlow(t, false, "Replace", pools, 1)
					f.readyTarget()
					if boundary == "all-completion" {
						f.step()
						f.completeWorker("pool-0")
					}
					hc := f.object("HostedCluster", "example")
					hcp := &unstructured.Unstructured{}
					hcp.SetGroupVersionKind(gvkHCP)
					f.get(hcp, WorkerControlPlaneNamespace("clusters", "example"), "example")
					f.r.Client = staleManagementHCClient{Client: f.c, hc: hc.DeepCopy(), hcp: hcp.DeepCopy()}
					edit := func() {
						obj := hc.DeepCopy()
						if strings.HasPrefix(change, "hcp-") {
							obj = hcp.DeepCopy()
						}
						obj.SetGeneration(obj.GetGeneration() + 1)
						switch change {
						case "hc-generation", "hcp-generation":
							_ = unstructured.SetNestedField(obj.Object, "changed", "spec", "nodeSelector", "revision")
						case "hc-override":
							_ = unstructured.SetNestedField(obj.Object, regressionImage("4.22.0"), "spec", "controlPlaneRelease", "image")
						case "hcp-override":
							_ = unstructured.SetNestedField(obj.Object, regressionImage("4.22.0"), "spec", "controlPlaneReleaseImage")
						case "hcp-uid":
							obj.SetUID("replacement-hcp")
						}
						_ = f.c.store(obj)
					}
					if timing == "cache-lag" {
						edit()
					} else {
						f.guest.mu.Lock()
						reads := 0
						// The first three requests are ordinary stage observation. Edit
						// during the ACTION's collection, after its management proof.
						f.guest.onObserverRead = func() {
							reads++
							if reads == 4 {
								f.guest.onObserverRead = nil
								edit()
							}
						}
						f.guest.mu.Unlock()
					}
					before := f.requests()
					f.step()
					f.assertRequestsUnchanged(before)
					if f.hcu().Status.Phase == upgradev1.PhaseCompleted {
						t.Fatal("stale management revision authorized completion")
					}
					if boundary == "all-completion" && f.hcu().Status.NodePoolStatuses[0].Phase != "Completed" {
						t.Fatal("revision conflict erased actual worker completion")
					}
					// A new revision is usable only after its actual management proof
					// is read and acknowledged. No historical guest/admission reuse.
					f.r.Client = f.c
					currentHC := f.object("HostedCluster", "example")
					currentHCP := &unstructured.Unstructured{}
					currentHCP.SetGroupVersionKind(gvkHCP)
					f.get(currentHCP, hcp.GetNamespace(), hcp.GetName())
					if _, found, _ := unstructured.NestedFieldNoCopy(currentHC.Object, "spec", "controlPlaneRelease"); found {
						unstructured.RemoveNestedField(currentHC.Object, "spec", "controlPlaneRelease")
						currentHC.SetGeneration(currentHC.GetGeneration() + 1)
					}
					if _, found, _ := unstructured.NestedFieldNoCopy(currentHCP.Object, "spec", "controlPlaneReleaseImage"); found {
						unstructured.RemoveNestedField(currentHCP.Object, "spec", "controlPlaneReleaseImage")
						currentHCP.SetGeneration(currentHCP.GetGeneration() + 1)
					}
					setHealthyHostedClusterStatus(currentHC, f.clock.Now())
					_ = unstructured.SetNestedField(currentHC.Object, currentHCP.GetGeneration(), "status", "controlPlaneVersion", "observedGeneration")
					f.publish(currentHC)
					f.publish(currentHCP)
					cpc := readyStepCPC("4.21.0")
					_ = unstructured.SetNestedField(cpc.Object, currentHCP.GetGeneration(), "status", "observedGeneration")
					f.publish(cpc)
					f.step()
					if boundary == "worker" {
						if len(f.requests()) != len(before)+1 {
							t.Fatal("freshly reverified revision did not recover worker action")
						}
					} else if f.hcu().Status.Phase != upgradev1.PhaseCompleted {
						t.Fatalf("freshly reverified revision did not recover completion: %+v", f.hcu().Status)
					}
				})
			}
		}
	}
}

func TestGuestFlowCompletedOwnedLockRequiresManagement(t *testing.T) {
	for _, defect := range []string{"stale", "degraded"} {
		t.Run(defect, func(t *testing.T) {
			f := newUpgradeFlow(t, false, "Replace", 1, 1)
			f.readyTarget()
			np := f.object("NodePool", "pool-0")
			np.SetGeneration(np.GetGeneration() + 1)
			backing := flowReadyWorker(np, f.hcu().Spec.TargetReleaseImage, "Replace")
			backing.SetGeneration(2)
			_ = unstructured.SetNestedField(backing.Object, int64(2), "status", "observedGeneration")
			f.publish(np)
			f.publish(backing)
			cpc := readyStepCPC("4.21.0")
			_ = unstructured.SetNestedField(cpc.Object, int64(2), "status", "observedGeneration")
			if defect == "stale" {
				_ = unstructured.SetNestedField(cpc.Object, int64(1), "status", "observedGeneration")
			} else {
				_ = unstructured.SetNestedSlice(cpc.Object, []interface{}{map[string]interface{}{"type": "Available", "status": "False"}}, "status", "conditions")
			}
			f.publish(cpc)
			writes := f.c.writes
			f.step()
			if f.c.writes != writes || f.object("NodePool", "pool-0").GetAnnotations()[workerLockOwner] == "" {
				t.Fatal("completed pool lock released despite failed management prerequisite")
			}
			if s := f.hcu().Status.NodePoolStatuses[0]; s.Phase != "Completed" || len(s.Blockers) == 0 || s.StartTime != nil {
				t.Fatalf("completed proof not preserved with release blocker: %+v", s)
			}
			cpc = readyStepCPC("4.21.0")
			_ = unstructured.SetNestedField(cpc.Object, int64(2), "status", "observedGeneration")
			f.publish(cpc)
			f.step()
			f.step()
			if f.c.writes != writes+1 || f.object("NodePool", "pool-0").GetAnnotations()[workerLockOwner] != "" {
				t.Fatal("recovery must release exactly once")
			}
		})
	}
}

func TestGuestFlowEnrollmentSourceFence(t *testing.T) {
	for _, change := range []string{"pause", "plan"} {
		t.Run(change, func(t *testing.T) {
			f := newUpgradeFlow(t, false, "Replace", 1, 1)
			f.readyTarget()
			f.step()
			started := f.hcu().Status.NodePoolStatuses[0].StartTime.DeepCopy()
			np := regressionNP("new-pool", "")
			np.SetGeneration(1)
			backing := flowReadyWorker(np, regressionImage("4.20.0"), "Replace")
			f.c.objects = append(f.c.objects, np, backing)
			f.guest.mu.Lock()
			f.guest.onObserverRead = func() {
				f.guest.onObserverRead = nil
				u := f.hcu()
				u.Generation++
				if change == "pause" {
					pause := "true"
					u.Spec.PausedUntil = &pause
				} else {
					u.Spec.RolloutPolicy.MaxParallelNodePools = 2
				}
				_ = f.c.store(u)
			}
			f.guest.mu.Unlock()
			writes := f.c.writes
			_, _ = f.r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKey{Namespace: "clusters", Name: "example"}})
			if f.c.writes != writes || f.object("NodePool", "new-pool").GetAnnotations()[workerLockOwner] != "" {
				t.Fatal("stale coordinator source acquired enrollment lock")
			}
			if f.object("NodePool", "pool-0").GetAnnotations()[workerLockOwner] != "" {
				t.Fatal("existing rollout relocked")
			}
			// The concurrent spec write may reject stale status persistence, but the
			// in-memory scheduler must also continue read-only active-work accounting.
			u := f.hcu()
			if !u.Status.NodePoolStatuses[0].StartTime.Equal(started) {
				t.Fatal("existing rollout reservation/timing lost")
			}
		})
	}
}

type editAfterLockClient struct {
	client.Client
	edit func()
}

func (c *editAfterLockClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	if err := c.Client.Update(ctx, obj, opts...); err != nil {
		return err
	}
	if obj.GetObjectKind().GroupVersionKind() == gvkNP && c.edit != nil {
		edit := c.edit
		c.edit = nil
		edit()
	}
	return nil
}

func TestGuestFlowInitialHoldRechecksSourceForEachPool(t *testing.T) {
	for _, change := range []string{"pause", "plan"} {
		t.Run(change, func(t *testing.T) {
			f := newUpgradeFlow(t, false, "Replace", 2, 1)
			f.until(upgradev1.PhasePreflightChecks)
			f.r.Client = &editAfterLockClient{Client: f.c, edit: func() {
				u := f.hcu()
				u.Generation++
				if change == "pause" {
					pause := "true"
					u.Spec.PausedUntil = &pause
				} else {
					u.Spec.RolloutPolicy.MaxParallelNodePools = 2
				}
				_ = f.c.store(u)
			}}
			_, err := f.r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKey{Namespace: "clusters", Name: "example"}})
			if err == nil {
				t.Fatal("concurrent source edit did not stop hold batch")
			}
			if f.c.writes != 1 || len(f.requests()) != 0 || f.object("NodePool", "pool-0").GetAnnotations()[workerLockOwner] == "" || f.object("NodePool", "pool-1").GetAnnotations()[workerLockOwner] != "" {
				t.Fatal("hold batch wrote beyond its current source authority")
			}
		})
	}
}

func TestWorkerHoldRequiresUncachedSourceReader(t *testing.T) {
	hcu := regressionHCU(upgradev1.PhasePreflightChecks)
	hc := regressionHC("4.20.0")
	np := regressionNP("workers", "")
	c := &regressionClient{objects: []client.Object{hcu, hc, np}}
	r := &HostedClusterUpgradeReconciler{Client: c}
	if err := r.lockClusterNodePools(context.Background(), hcu, hc); err == nil || c.writes != 0 {
		t.Fatalf("nil uncached reader authorized hold: err=%v writes=%d", err, c.writes)
	}
}

func TestGuestFlowFreshAccessCannotCombineStaleManagementProof(t *testing.T) {
	for _, change := range []string{"request", "uid", "cluster-id"} {
		t.Run(change, func(t *testing.T) {
			f := newUpgradeFlow(t, false, "Replace", 1, 1)
			f.readyTarget()
			cached := f.object("HostedCluster", "example")
			switch change {
			case "request":
				current := cached.DeepCopy()
				_ = unstructured.SetNestedField(current.Object, regressionImage("4.22.0"), "spec", "release", "image")
				current.SetGeneration(current.GetGeneration() + 1)
				f.publish(current)
			case "uid":
				cached.SetUID("old-management-identity")
			case "cluster-id":
				_ = unstructured.SetNestedField(cached.Object, "old-cluster-id", "spec", "clusterID")
			}
			f.r.Client = staleManagementHCClient{Client: f.c, hc: cached}
			before := f.requests()
			f.step()
			f.assertRequestsUnchanged(before)
			if a := f.hcu().Status.GuestAssessment; a.State != "IdentityMismatch" || a.Reason != "ManagementGuestTargetMismatch" {
				t.Fatalf("mixed management/guest identity passed: %+v", a)
			}
		})
	}
}

func flowHCO(version string, healthy bool, upgradeable bool, methods ...string) map[string]interface{} {
	h := "healthy"
	if !healthy {
		h = "warning"
	}
	upg := "True"
	if !upgradeable {
		upg = "False"
	}
	ms := []interface{}{}
	for _, m := range methods {
		ms = append(ms, m)
	}
	return map[string]interface{}{
		"apiVersion": "hco.kubevirt.io/v1beta1",
		"kind":       "HyperConvergedList",
		"metadata":   map[string]interface{}{"resourceVersion": "1"},
		"items": []interface{}{
			map[string]interface{}{
				"apiVersion": "hco.kubevirt.io/v1beta1", "kind": "HyperConverged",
				"spec": map[string]interface{}{"workloadUpdateStrategy": map[string]interface{}{"workloadUpdateMethods": ms}},
				"metadata": map[string]interface{}{
					"name":      "kubevirt-hyperconverged",
					"namespace": "openshift-cnv",
					"uid":       "hco-uid", "generation": int64(1), "resourceVersion": "1",
				},
				"status": map[string]interface{}{
					"systemHealthStatus": h,
					"observedGeneration": int64(1),
					"conditions": []interface{}{
						map[string]interface{}{"type": "ReconcileComplete", "status": "True"},
						map[string]interface{}{"type": "Upgradeable", "status": upg},
						map[string]interface{}{"type": "Available", "status": "True"},
						map[string]interface{}{"type": "Degraded", "status": "False"},
						map[string]interface{}{"type": "Progressing", "status": "False"},
					},
					"versions": []interface{}{
						map[string]interface{}{"name": "operator", "version": version},
					},
				},
			},
		},
	}
}

func flowKubeVirt(outdated int, methods ...string) map[string]interface{} {
	ms := []interface{}{}
	for _, m := range methods {
		ms = append(ms, m)
	}
	return map[string]interface{}{
		"apiVersion": "kubevirt.io/v1",
		"kind":       "KubeVirtList",
		"metadata":   map[string]interface{}{"resourceVersion": "1"},
		"items": []interface{}{
			map[string]interface{}{
				"apiVersion": "kubevirt.io/v1", "kind": "KubeVirt",
				"spec": map[string]interface{}{"workloadUpdateStrategy": map[string]interface{}{"workloadUpdateMethods": ms}},
				"metadata": map[string]interface{}{
					"name":      "kubevirt-kubevirt-hyperconverged",
					"namespace": "openshift-cnv",
					"uid":       "kv-uid", "generation": int64(1), "resourceVersion": "1",
					"ownerReferences": []interface{}{map[string]interface{}{"apiVersion": "hco.kubevirt.io/v1beta1", "kind": "HyperConverged", "name": "kubevirt-hyperconverged", "uid": "hco-uid"}},
				},
				"status": map[string]interface{}{
					"observedGeneration":      int64(1),
					"observedKubeVirtVersion": "v1.6.0", "targetKubeVirtVersion": "v1.6.0",
					"observedDeploymentID": "deployment-1", "targetDeploymentID": "deployment-1",
					"conditions": []interface{}{map[string]interface{}{"type": "Available", "status": "True"}, map[string]interface{}{"type": "Degraded", "status": "False"}, map[string]interface{}{"type": "Progressing", "status": "False"}},
					"outdatedVirtualMachineInstanceWorkloads": int64(outdated),
				},
			},
		},
	}
}

func TestGuestFlowEUSVirtualizationGate(t *testing.T) {
	f := newUpgradeFlow(t, true, "Replace", 1, 1)
	f.guest.mu.Lock()
	f.guest.objects["/apis/hco.kubevirt.io/v1beta1/hyperconvergeds"] = flowHCO("4.20.0", true, true)
	f.guest.objects["/apis/kubevirt.io/v1/kubevirts"] = flowKubeVirt(0)
	f.guest.mu.Unlock()

	f.until(upgradev1.PhaseVerifyingControlPlane)
	f.publish(f.completeCP())

	setFlowGuestRelease(f.guest, regressionImage("4.21.0"))
	f.step()

	if f.hcu().Status.Phase != upgradev1.PhaseVerifyingControlPlane {
		t.Fatalf("expected PhaseVerifyingControlPlane, got %s", f.hcu().Status.Phase)
	}
	if a := f.hcu().Status.GuestAssessment; a == nil || a.Reason != "CNVIntermediateVersionMismatch" {
		t.Fatalf("expected CNVIntermediateVersionMismatch, got %+v", a)
	}

	f.guest.mu.Lock()
	f.guest.objects["/apis/hco.kubevirt.io/v1beta1/hyperconvergeds"] = flowHCO("4.21.0", true, true)
	f.guest.mu.Unlock()

	f.step()
	if f.hcu().Status.Phase != upgradev1.PhaseUpgradingControlPlane {
		t.Fatalf("expected PhaseUpgradingControlPlane, got %s", f.hcu().Status.Phase)
	}

	f.until(upgradev1.PhaseVerifyingControlPlane)
	f.publish(f.completeCP())
	setFlowGuestRelease(f.guest, regressionImage("4.22.0"))

	// Methods restored, but HCO is still at 4.21.0 -> target version mismatch!
	f.guest.mu.Lock()
	f.guest.objects["/apis/hco.kubevirt.io/v1beta1/hyperconvergeds"] = flowHCO("4.21.0", true, true, "LiveMigrate")
	f.guest.objects["/apis/kubevirt.io/v1/kubevirts"] = flowKubeVirt(0, "LiveMigrate")
	f.guest.mu.Unlock()

	f.step()
	if f.hcu().Status.Phase != upgradev1.PhaseVerifyingControlPlane {
		t.Fatalf("expected PhaseVerifyingControlPlane, got %s", f.hcu().Status.Phase)
	}
	if a := f.hcu().Status.GuestAssessment; a == nil || a.Reason != "CNVTargetVersionMismatch" {
		t.Fatalf("expected CNVTargetVersionMismatch, got %+v", a)
	}

	// Target version matched, but workload update methods are still held empty
	f.guest.mu.Lock()
	f.guest.objects["/apis/hco.kubevirt.io/v1beta1/hyperconvergeds"] = flowHCO("4.22.0", true, true)
	f.guest.objects["/apis/kubevirt.io/v1/kubevirts"] = flowKubeVirt(2)
	f.guest.mu.Unlock()

	f.step()
	if f.hcu().Status.Phase != upgradev1.PhaseVerifyingControlPlane {
		t.Fatalf("expected PhaseVerifyingControlPlane, got %s", f.hcu().Status.Phase)
	}
	if a := f.hcu().Status.GuestAssessment; a == nil || a.Reason != "CNVWorkloadUpdatesStillHeld" {
		t.Fatalf("expected CNVWorkloadUpdatesStillHeld, got %+v", a)
	}

	// GitOps restores workloadUpdateMethods, but workloads are still migrating (outdated > 0)
	f.guest.mu.Lock()
	f.guest.objects["/apis/hco.kubevirt.io/v1beta1/hyperconvergeds"] = flowHCO("4.22.0", true, true, "LiveMigrate")
	f.guest.objects["/apis/kubevirt.io/v1/kubevirts"] = flowKubeVirt(2, "LiveMigrate")
	f.guest.mu.Unlock()

	f.step()
	if f.hcu().Status.Phase != upgradev1.PhaseVerifyingControlPlane {
		t.Fatalf("expected PhaseVerifyingControlPlane, got %s", f.hcu().Status.Phase)
	}
	if a := f.hcu().Status.GuestAssessment; a == nil || a.Reason != "CNVOutdatedWorkloads" {
		t.Fatalf("expected CNVOutdatedWorkloads, got %+v", a)
	}

	// Workload migrations complete (outdated == 0)
	f.guest.mu.Lock()
	f.guest.objects["/apis/kubevirt.io/v1/kubevirts"] = flowKubeVirt(0, "LiveMigrate")
	f.guest.mu.Unlock()

	f.step()
	if f.hcu().Status.Phase != upgradev1.PhaseUpgradingNodePools {
		t.Fatalf("expected PhaseUpgradingNodePools, got %s: %+v", f.hcu().Status.Phase, f.hcu().Status)
	}
}
