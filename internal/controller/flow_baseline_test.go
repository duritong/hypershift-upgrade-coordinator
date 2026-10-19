package controller

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	upgradev1alpha1 "github.com/duritong/hypershift-upgrade-coordinator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestUpgradeFlowBaseline(t *testing.T) {
	for _, eus := range []bool{false, true} {
		for _, strategy := range []string{"Replace", "InPlace"} {
			for _, parallel := range []int32{1, 2} {
				t.Run(fmt.Sprintf("eus=%t/%s/parallel=%d", eus, strategy, parallel), func(t *testing.T) {
					f := newUpgradeFlow(t, eus, strategy, 2, parallel)
					hcu := f.hcu()
					cpStart := metav1.NewTime(f.clock.Now().Add(time.Hour))
					npStart := metav1.NewTime(f.clock.Now().Add(2 * time.Hour))
					hcu.Spec.ControlPlane.StartAt = &cpStart
					hcu.Spec.NodePoolGroups = []upgradev1alpha1.NodePoolGroupSpec{{Name: "default", StartAt: &npStart}}
					if parallel == 1 {
						// Named groups must finish before the appended default group.
						hcu.Spec.NodePoolGroups = []upgradev1alpha1.NodePoolGroupSpec{{Name: "first", StartAt: &npStart}}
						np := f.object("NodePool", "pool-0")
						np.SetLabels(map[string]string{"upgrade.huc.0xshift.dev/group": "first"})
						f.publish(np)
					}
					f.publish(hcu)
					f.until(upgradev1alpha1.PhasePendingSchedule)
					writes := f.c.writes
					if got := f.step(); got.RequeueAfter != 30*time.Second {
						t.Fatalf("scheduled heartbeat=%v", got)
					}
					if f.c.writes != writes || len(f.requests()) != 0 {
						t.Fatal("mutation before CP start")
					}
					f.clock.Step(time.Hour)
					want := []string{"HostedCluster/example/4.21.0"}
					if eus {
						want = append(want, "HostedCluster/example/4.22.0")
					}
					for hop := range want {
						f.until(upgradev1alpha1.PhaseVerifyingControlPlane)
						if !reflect.DeepEqual(f.requests(), want[:hop+1]) {
							t.Fatalf("CP request order=%v, want %v", f.requests(), want[:hop+1])
						}
						cpc := f.completeCP()
						f.step() // Histories alone cannot authorize downstream work.
						if f.hcu().Status.Phase != upgradev1alpha1.PhaseVerifyingControlPlane || !reflect.DeepEqual(f.requests(), want[:hop+1]) {
							t.Fatal("stale CPC permitted progression")
						}
						f.publish(cpc)
						f.step()
					}
					f.until(upgradev1alpha1.PhaseUpgradingNodePools)
					writes = f.c.writes
					f.step()
					if f.c.writes != writes || !reflect.DeepEqual(f.requests(), want) {
						t.Fatal("mutation before NodePool start")
					}
					f.clock.Step(time.Hour)
					f.step()
					target, _ := upgradev1alpha1.ExtractVersion(f.hcu().Spec.TargetReleaseImage)
					want = append(want, "NodePool/pool-0/"+target)
					if parallel == 2 {
						want = append(want, "NodePool/pool-1/"+target)
					}
					if !reflect.DeepEqual(f.requests(), want) {
						t.Fatalf("worker request order=%v, want %v", f.requests(), want)
					}
					f.step()
					if !reflect.DeepEqual(f.requests(), want) || f.hcu().Status.Phase == upgradev1alpha1.PhaseCompleted {
						t.Fatal("unfinished workers duplicated or bypassed")
					}
					f.completeWorker("pool-0")
					f.step()
					if parallel == 1 {
						// The scheduler preserves its existing next-reconcile group boundary.
						f.step()
						want = append(want, "NodePool/pool-1/"+target)
					}
					if !reflect.DeepEqual(f.requests(), want) {
						t.Fatalf("later worker request=%v, want %v", f.requests(), want)
					}
					f.completeWorker("pool-1")
					f.until(upgradev1alpha1.PhaseCompleted)
					f.step()
					if !reflect.DeepEqual(f.requests(), want) || f.hcu().Status.CompletionTime == nil {
						t.Fatalf("completion trace=%v status=%+v", f.requests(), f.hcu().Status)
					}
					var phases []upgradev1alpha1.UpgradePhase
					for _, a := range f.c.actions {
						if hcu, ok := a.after.(*upgradev1alpha1.HostedClusterUpgrade); ok && a.status && hcu.Status.Phase != "" && (len(phases) == 0 || phases[len(phases)-1] != hcu.Status.Phase) {
							phases = append(phases, hcu.Status.Phase)
						}
					}
					wantPhases := []upgradev1alpha1.UpgradePhase{upgradev1alpha1.PhasePreflightChecks, upgradev1alpha1.PhasePendingSchedule, upgradev1alpha1.PhaseUpgradingControlPlane, upgradev1alpha1.PhaseVerifyingControlPlane}
					if eus {
						wantPhases = append(wantPhases, upgradev1alpha1.PhaseUpgradingControlPlane, upgradev1alpha1.PhaseVerifyingControlPlane)
					}
					wantPhases = append(wantPhases, upgradev1alpha1.PhaseUpgradingNodePools, upgradev1alpha1.PhaseCompleted)
					if !reflect.DeepEqual(phases, wantPhases) {
						t.Fatalf("phase order=%v, want %v; trace=%v", phases, wantPhases, f.c.actions)
					}
					for _, a := range f.c.actions {
						u, ok := a.after.(*unstructured.Unstructured)
						if !ok || u.GetKind() != "NodePool" {
							continue
						}
						image, _, _ := unstructured.NestedString(u.Object, "spec", "release", "image")
						if image == f.hcu().Spec.TargetReleaseImage {
							if a.at.Before(npStart.Time) || u.GetAnnotations()[workerLockOwner] != "" {
								t.Fatal("worker requested early or still locked")
							}
						} else if u.GetAnnotations()[workerLockOwner] != string(hcu.UID) {
							t.Fatal("worker hold lacks UID ownership")
						}
					}
				})
			}
		}
	}
}

func TestUpgradeFlowZeroPools(t *testing.T) {
	for _, eus := range []bool{false, true} {
		t.Run(fmt.Sprintf("eus=%t", eus), func(t *testing.T) {
			f := newUpgradeFlow(t, eus, "Replace", 0, 1)
			hops := 1
			if eus {
				hops = 2
			}
			for range hops {
				f.until(upgradev1alpha1.PhaseVerifyingControlPlane)
				f.publish(f.completeCP())
				f.step()
			}
			f.until(upgradev1alpha1.PhaseUpgradingNodePools)
			// The no-pool completion branch must still recheck fresh CP evidence.
			cpc := readyStepCPC("4.20.0")
			f.publish(cpc)
			f.step()
			if f.hcu().Status.Phase == upgradev1alpha1.PhaseCompleted {
				t.Fatal("zero pools bypassed final management verification")
			}
			version, _ := upgradev1alpha1.ExtractVersion(f.hcu().Spec.TargetReleaseImage)
			cpc = readyStepCPC(version)
			_ = unstructured.SetNestedField(cpc.Object, int64(hops+1), "status", "observedGeneration")
			f.publish(cpc)
			f.until(upgradev1alpha1.PhaseCompleted)
			want := []string{"HostedCluster/example/4.21.0"}
			if eus {
				want = append(want, "HostedCluster/example/4.22.0")
			}
			if !reflect.DeepEqual(f.requests(), want) {
				t.Fatalf("zero-pool requests=%v", f.requests())
			}
		})
	}
}

func TestUpgradeFlowClockPauseAndMetrics(t *testing.T) {
	f := newUpgradeFlow(t, false, "Replace", 1, 1)
	f.until(upgradev1alpha1.PhaseVerifyingControlPlane)
	initial := f.hcu()
	f.clock.Step(90 * time.Second)
	f.step()
	gauges := collectedGauges(t, metricDuration)
	if len(gauges) != 1 {
		t.Fatalf("overall elapsed series=%d, want 1", len(gauges))
	}
	for _, gauge := range gauges {
		if got := gauge.GetGauge().GetValue(); got != 90 {
			t.Fatalf("overall elapsed=%v, want 90", got)
		}
	}
	assertStepSeconds(t, "ControlPlane", 90)
	if !f.hcu().Status.ControlPlaneStatus.StartTime.Equal(initial.Status.ControlPlaneStatus.StartTime) {
		t.Fatal("CP start changed while waiting")
	}
	f.publish(f.completeCP())
	f.until(upgradev1alpha1.PhaseUpgradingNodePools)
	f.step()
	hcu := f.hcu()
	workerStart := hcu.Status.NodePoolStatuses[0].StartTime.DeepCopy()
	pause := f.clock.Now().Add(time.Minute).Format(time.RFC3339)
	hcu.Spec.PausedUntil = &pause
	f.publish(hcu)
	requests, writes := f.requests(), f.c.writes
	f.step()
	if got := f.hcu(); got.Status.Phase != upgradev1alpha1.PhasePaused || got.Status.PhaseBeforePause != upgradev1alpha1.PhaseUpgradingNodePools {
		t.Fatalf("pause lost worker stage: %+v", got.Status)
	}
	f.clock.Step(59 * time.Second)
	if result := f.step(); result.RequeueAfter != time.Second {
		t.Fatalf("pause countdown=%v", result)
	}
	if f.c.writes != writes {
		t.Fatal("downstream mutation while paused")
	}
	f.clock.Step(time.Second)
	// A new reconciler demonstrates that resume uses persisted stage, not memory.
	f.r = &HostedClusterUpgradeReconciler{Client: f.c, APIReader: f.c, GuestAccess: &f.access.GuestAccessProvider, Clock: f.clock, ReleaseResolver: fixtureReleaseResolver{}}
	f.step()
	if got := f.hcu(); got.Status.Phase != upgradev1alpha1.PhaseUpgradingNodePools || got.Status.PhaseBeforePause != "" || !got.Status.NodePoolStatuses[0].StartTime.Equal(workerStart) {
		t.Fatalf("resume restarted stage: %+v", got.Status)
	}
	assertStepSeconds(t, "NodePool-pool-0", 60)
	if !reflect.DeepEqual(f.requests(), requests) || f.c.writes != writes {
		t.Fatal("resume replayed release or worker locks")
	}
	f.completeWorker("pool-0")
	f.until(upgradev1alpha1.PhaseCompleted)
}

func TestUpgradeFlowStaleWorkerEvidence(t *testing.T) {
	for _, strategy := range []string{"Replace", "InPlace"} {
		t.Run(strategy, func(t *testing.T) {
			f := newUpgradeFlow(t, false, strategy, 1, 1)
			f.until(upgradev1alpha1.PhaseVerifyingControlPlane)
			f.publish(f.completeCP())
			f.until(upgradev1alpha1.PhaseUpgradingNodePools)
			f.step()
			f.completeWorker("pool-0")
			np := f.object("NodePool", "pool-0")
			backing := flowReadyWorker(np, f.hcu().Spec.TargetReleaseImage, strategy)
			f.get(backing, backing.GetNamespace(), backing.GetName())
			backing.SetGeneration(backing.GetGeneration() + 1)
			f.publish(backing)
			writes := f.c.writes
			f.step()
			if f.hcu().Status.Phase == upgradev1alpha1.PhaseCompleted || f.c.writes != writes {
				t.Fatal("fresh NP status bypassed stale CAPI proof or caused re-actuation")
			}
			_ = unstructured.SetNestedField(backing.Object, backing.GetGeneration(), "status", "observedGeneration")
			f.publish(backing)
			np.SetGeneration(np.GetGeneration() + 1)
			f.publish(np)
			f.step()
			if f.hcu().Status.Phase == upgradev1alpha1.PhaseCompleted || f.c.writes != writes {
				t.Fatal("fresh CAPI bypassed stale NP proof or caused re-actuation")
			}
			f.completeWorker("pool-0")
			f.until(upgradev1alpha1.PhaseCompleted)
		})
	}
}

func TestUpgradeFlowTimedControlPlanePause(t *testing.T) {
	f := newUpgradeFlow(t, false, "Replace", 1, 1)
	f.until(upgradev1alpha1.PhaseVerifyingControlPlane)
	hcu := f.hcu()
	start := hcu.Status.ControlPlaneStatus.StartTime.DeepCopy()
	pause := f.clock.Now().Add(time.Minute).Format(time.RFC3339)
	hcu.Spec.PausedUntil = &pause
	f.publish(hcu)
	writes := f.c.writes
	f.step()
	if f.hcu().Status.PhaseBeforePause != upgradev1alpha1.PhaseVerifyingControlPlane {
		t.Fatal("pause did not persist CP stage")
	}
	f.publish(f.completeCP()) // Delegated work may finish during coordinator pause.
	f.clock.Step(59 * time.Second)
	f.step()
	if f.hcu().Status.Phase != upgradev1alpha1.PhasePaused || f.c.writes != writes {
		t.Fatal("started new work before pause expiry")
	}
	if !f.hcu().Status.ControlPlaneStatus.StartTime.Equal(start) {
		t.Fatal("pause lost CP start")
	}
	f.clock.Step(time.Second)
	f.r = &HostedClusterUpgradeReconciler{Client: f.c, APIReader: f.c, GuestAccess: &f.access.GuestAccessProvider, Clock: f.clock, ReleaseResolver: fixtureReleaseResolver{}}
	f.step()
	if f.hcu().Status.Phase != upgradev1alpha1.PhaseUpgradingNodePools || f.c.writes != writes {
		t.Fatalf("resume did not verify persisted CP stage without re-actuation: %+v", f.hcu().Status)
	}
	f.step()
	f.completeWorker("pool-0")
	f.until(upgradev1alpha1.PhaseCompleted)
}
