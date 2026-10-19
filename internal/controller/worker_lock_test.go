package controller

import (
	"context"
	"fmt"
	"testing"
	"time"

	upgradev1alpha1 "github.com/duritong/hypershift-upgrade-coordinator/api/v1alpha1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestWorkerUserPauses(t *testing.T) {
	for _, pause := range []string{"true", time.Now().Add(time.Hour).UTC().Format(time.RFC3339)} {
		t.Run(pause, func(t *testing.T) {
			hcu := regressionHCU(upgradev1alpha1.PhaseUpgradingNodePools)
			np := regressionNP("workers", "")
			_ = unstructured.SetNestedField(np.Object, pause, "spec", "pausedUntil")
			c := &regressionClient{objects: quiescentWorkerFixtures(np)}
			r := &HostedClusterUpgradeReconciler{Client: c}
			if pause == "true" {
				if err := r.lockClusterNodePools(context.Background(), hcu); err != nil {
					t.Fatal(err)
				}
				if c.writes != 0 {
					t.Fatal("unannotated user pause was adopted")
				}
			}
			_, delay, err := r.reconcileNodePools(context.Background(), hcu, regressionHC("4.21.0"))
			wantWrites := 0
			if pause != "true" {
				wantWrites = 1
			}
			if err != nil || c.writes != wantWrites || len(hcu.Status.NodePoolStatuses[0].Blockers) == 0 || hcu.Status.NodePoolStatuses[0].StartTime != nil {
				t.Fatalf("user pause not respected: err=%v writes=%d status=%+v", err, c.writes, hcu.Status)
			}
			if pause != "true" && (delay <= 0 || delay > 5*time.Minute) {
				t.Fatalf("missing bounded expiry requeue: %s", delay)
			}
		})
	}
}

type workerLockFailureClient struct {
	*regressionClient
	failList bool
}

func (c *workerLockFailureClient) List(ctx context.Context, out client.ObjectList, opts ...client.ListOption) error {
	if c.failList && out.GetObjectKind().GroupVersionKind() == gvkNP {
		return fmt.Errorf("injected NodePool list failure")
	}
	return c.regressionClient.List(ctx, out, opts...)
}

func (c *workerLockFailureClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	if obj.GetObjectKind().GroupVersionKind() == gvkNP {
		return fmt.Errorf("injected NodePool lock failure")
	}
	return c.regressionClient.Update(ctx, obj, opts...)
}

func TestWorkerLockFailureStopsControlPlane(t *testing.T) {
	for _, phase := range []upgradev1alpha1.UpgradePhase{upgradev1alpha1.PhasePreflightChecks, upgradev1alpha1.PhaseUpgradingControlPlane} {
		for _, failList := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/list=%t", phase, failList), func(t *testing.T) {
				hcu := regressionHCU(phase)
				hcu.UID = "upgrade-uid"
				base := &regressionClient{objects: []client.Object{hcu, regressionHC("4.20.0"), regressionNP("workers", "")}}
				r := &HostedClusterUpgradeReconciler{Client: &workerLockFailureClient{regressionClient: base, failList: failList}, ReleaseResolver: fixtureReleaseResolver{}}
				if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(hcu)}); err == nil {
					t.Fatal("lock failure did not propagate")
				}
				got := &upgradev1alpha1.HostedClusterUpgrade{}
				if err := base.Get(context.Background(), client.ObjectKeyFromObject(hcu), got); err != nil {
					t.Fatal(err)
				}
				if base.writes != 0 || got.Status.Phase != phase || got.Status.ControlPlaneStatus.StartTime != nil {
					t.Fatalf("advanced after lock failure: writes=%d status=%+v", base.writes, got.Status)
				}
			})
		}
	}
}

func TestWorkerLockOwnershipChanges(t *testing.T) {
	for _, change := range []string{"user indefinite", "user future", "foreign owner", "missing owner", "invalid original"} {
		t.Run(change, func(t *testing.T) {
			hcu := regressionHCU(upgradev1alpha1.PhaseUpgradingNodePools)
			hcu.UID = "upgrade-uid"
			np := regressionNP("workers", "")
			c := &regressionClient{objects: []client.Object{np}}
			r := &HostedClusterUpgradeReconciler{Client: c}
			if err := r.lockClusterNodePools(context.Background(), hcu); err != nil {
				t.Fatal(err)
			}
			if err := c.Get(context.Background(), client.ObjectKeyFromObject(np), np); err != nil {
				t.Fatal(err)
			}
			annotations := np.GetAnnotations()
			switch change {
			case "user indefinite":
				// A distinct serialized value makes the replacement observable.
				_ = unstructured.SetNestedField(np.Object, "True", "spec", "pausedUntil")
			case "user future":
				_ = unstructured.SetNestedField(np.Object, time.Now().Add(time.Hour).UTC().Format(time.RFC3339), "spec", "pausedUntil")
			case "foreign owner":
				annotations[workerLockOwner] = "another-upgrade"
			case "missing owner":
				delete(annotations, workerLockOwner)
			case "invalid original":
				annotations[workerLockOriginal] = "invalid"
			}
			np.SetAnnotations(annotations)
			if err := c.store(np); err != nil {
				t.Fatal(err)
			}
			before, _, _ := unstructured.NestedString(np.Object, "spec", "pausedUntil")
			if _, _, err := r.reconcileNodePools(context.Background(), hcu, regressionHC("4.21.0")); err != nil {
				t.Fatal(err)
			}
			if err := c.Get(context.Background(), client.ObjectKeyFromObject(np), np); err != nil {
				t.Fatal(err)
			}
			after, _, _ := unstructured.NestedString(np.Object, "spec", "pausedUntil")
			image, _, _ := unstructured.NestedString(np.Object, "spec", "release", "image")
			if before != after || image == hcu.Spec.TargetReleaseImage || hcu.Status.NodePoolStatuses[0].Phase != "Blocked" {
				t.Fatalf("changed ownership bypassed: pause=%q -> %q status=%+v", before, after, hcu.Status)
			}
			if change == "foreign owner" && np.GetAnnotations()[workerLockOwner] != "another-upgrade" {
				t.Fatal("foreign lock changed")
			}
		})
	}
}

func TestWorkerTimedPauseExpiresDuringControlPlane(t *testing.T) {
	hcu := regressionHCU(upgradev1alpha1.PhaseUpgradingNodePools)
	hcu.UID = "upgrade-uid"
	np := regressionNP("workers", "")
	original := time.Now().Add(2 * time.Second).UTC().Format(time.RFC3339)
	_ = unstructured.SetNestedField(np.Object, original, "spec", "pausedUntil")
	c := &regressionClient{objects: quiescentWorkerFixtures(np)}
	r := &HostedClusterUpgradeReconciler{Client: c}
	if err := r.lockClusterNodePools(context.Background(), hcu); err != nil {
		t.Fatal(err)
	}
	// Let the actual timestamp expire without changing the persisted lock snapshot.
	expiry, _ := time.Parse(time.RFC3339, original)
	time.Sleep(time.Until(expiry) + 10*time.Millisecond)
	if err := r.lockClusterNodePools(context.Background(), hcu); err != nil {
		t.Fatal(err)
	}
	if c.writes != 1 {
		t.Fatalf("relocking changed snapshot: writes=%d", c.writes)
	}
	if _, _, err := r.reconcileNodePools(context.Background(), hcu, regressionHC("4.21.0")); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(np), np); err != nil {
		t.Fatal(err)
	}
	pause, _, _ := unstructured.NestedString(np.Object, "spec", "pausedUntil")
	image, _, _ := unstructured.NestedString(np.Object, "spec", "release", "image")
	if pause != original || image != hcu.Spec.TargetReleaseImage || hcu.Status.NodePoolStatuses[0].StartTime == nil {
		t.Fatalf("expired user pause not restored on actuation: pause=%q status=%+v", pause, hcu.Status)
	}
}

func TestWorkerLockRestoresOriginal(t *testing.T) {
	for _, original := range []string{"absent", "", "false", "False", time.Now().Add(-time.Hour).UTC().Format(time.RFC3339), time.Now().Add(time.Hour).UTC().Format(time.RFC3339)} {
		for _, completed := range []bool{false, true} {
			name := original
			if completed {
				name += "/completed"
			}
			t.Run(name, func(t *testing.T) {
				hcu := regressionHCU(upgradev1alpha1.PhaseUpgradingNodePools)
				hcu.UID = "upgrade-uid"
				np := regressionNP("workers", "")
				if original != "absent" {
					_ = unstructured.SetNestedField(np.Object, original, "spec", "pausedUntil")
				}
				if completed {
					_ = unstructured.SetNestedField(np.Object, hcu.Spec.TargetReleaseImage, "spec", "release", "image")
					_ = unstructured.SetNestedField(np.Object, "4.21.0", "status", "version")
					_ = unstructured.SetNestedSlice(np.Object, []interface{}{map[string]interface{}{"ocpVersion": "4.21.0", "readyNodeCount": int64(1)}}, "status", "nodesInfo", "nodeVersions")
				}
				c := &regressionClient{objects: quiescentWorkerFixtures(np)}
				r := &HostedClusterUpgradeReconciler{Client: c}
				if err := r.lockClusterNodePools(context.Background(), hcu); err != nil {
					t.Fatal(err)
				}
				if err := r.lockClusterNodePools(context.Background(), hcu); err != nil {
					t.Fatal(err)
				}
				if _, _, err := r.reconcileNodePools(context.Background(), hcu, regressionHC("4.21.0")); err != nil {
					t.Fatal(err)
				}
				got := &unstructured.Unstructured{}
				got.SetGroupVersionKind(gvkNP)
				if err := c.Get(context.Background(), client.ObjectKeyFromObject(np), got); err != nil {
					t.Fatal(err)
				}
				value, found, _ := unstructured.NestedString(got.Object, "spec", "pausedUntil")
				paused, _, _ := checkPausedState(&original)
				if paused && !completed {
					if value != "true" || got.GetAnnotations()[workerLockOwner] != string(hcu.UID) || hcu.Status.NodePoolStatuses[0].Phase != "Blocked" {
						t.Fatalf("future pause must retain lock until eligible selection: %+v", got.Object)
					}
					return
				}
				if (original == "absent" && found) || (original != "absent" && (!found || value != original)) {
					t.Fatalf("original pause lost: got %q present=%t want %q", value, found, original)
				}
				if _, locked := got.GetAnnotations()[workerLockOwner]; locked {
					t.Fatal("restoration retained ownership")
				}
				if _, saved := got.GetAnnotations()[workerLockOriginal]; saved {
					t.Fatal("restoration retained snapshot")
				}
			})
		}
	}
}

func TestWorkerCompletedUserPauseUnchanged(t *testing.T) {
	for _, pause := range []string{"true", time.Now().Add(time.Hour).UTC().Format(time.RFC3339)} {
		t.Run(pause, func(t *testing.T) {
			hcu := regressionHCU(upgradev1alpha1.PhaseUpgradingNodePools)
			np := regressionNP("workers", "")
			_ = unstructured.SetNestedField(np.Object, hcu.Spec.TargetReleaseImage, "spec", "release", "image")
			_ = unstructured.SetNestedField(np.Object, "4.21.0", "status", "version")
			_ = unstructured.SetNestedSlice(np.Object, []interface{}{map[string]interface{}{"ocpVersion": "4.21.0", "readyNodeCount": int64(1)}}, "status", "nodesInfo", "nodeVersions")
			_ = unstructured.SetNestedField(np.Object, pause, "spec", "pausedUntil")
			md := readyWorkerFixture(np, hcu.Spec.TargetReleaseImage)
			c := &regressionClient{objects: []client.Object{np, md}}
			r := &HostedClusterUpgradeReconciler{Client: c}
			if _, _, err := r.reconcileNodePools(context.Background(), hcu, regressionHC("4.21.0")); err != nil {
				t.Fatal(err)
			}
			if c.writes != 0 || hcu.Status.Phase != upgradev1alpha1.PhaseCompleted {
				t.Fatalf("completed user pause changed: writes=%d status=%+v", c.writes, hcu.Status)
			}
		})
	}
}

func TestWorkerLocksRespectGroupAndConcurrency(t *testing.T) {
	hcu := regressionHCU(upgradev1alpha1.PhaseUpgradingNodePools)
	hcu.UID = "upgrade-uid"
	hcu.Spec.NodePoolGroups = []upgradev1alpha1.NodePoolGroupSpec{{Name: "first"}}
	a, b, later := regressionNP("a", "first"), regressionNP("b", "first"), regressionNP("later", "")
	c := &regressionClient{objects: quiescentWorkerFixtures(a, b, later)}
	r := &HostedClusterUpgradeReconciler{Client: c}
	if err := r.lockClusterNodePools(context.Background(), hcu); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if _, _, err := r.reconcileNodePools(context.Background(), hcu, regressionHC("4.21.0")); err != nil {
			t.Fatal(err)
		}
	}
	for i, np := range []*unstructured.Unstructured{a, b, later} {
		if err := c.Get(context.Background(), client.ObjectKeyFromObject(np), np); err != nil {
			t.Fatal(err)
		}
		pause, found, _ := unstructured.NestedString(np.Object, "spec", "pausedUntil")
		if i == 0 && (found || hcu.Status.NodePoolStatuses[i].StartTime == nil) {
			t.Fatal("selected worker was not restored and timed")
		}
		if i > 0 && (pause != "true" || hcu.Status.NodePoolStatuses[i].StartTime != nil) {
			t.Fatalf("waiting worker released or timed: %+v", hcu.Status.NodePoolStatuses[i])
		}
	}
	if hcu.Status.NodePoolStatuses[1].Phase != "WaitingForConcurrency" || hcu.Status.NodePoolStatuses[2].Phase != "WaitingForGroup" {
		t.Fatalf("sequencing changed: %+v", hcu.Status)
	}
}

func TestWorkerUserUnpauseDoesNotInventStepStart(t *testing.T) {
	hcu := regressionHCU(upgradev1alpha1.PhaseUpgradingNodePools)
	hcu.UID = "upgrade-uid"
	np := regressionNP("workers", "")
	c := &regressionClient{objects: []client.Object{np}}
	r := &HostedClusterUpgradeReconciler{Client: c}
	if err := r.lockClusterNodePools(context.Background(), hcu); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(np), np); err != nil {
		t.Fatal(err)
	}
	_ = unstructured.SetNestedField(np.Object, "false", "spec", "pausedUntil")
	_ = unstructured.SetNestedField(np.Object, hcu.Spec.TargetReleaseImage, "spec", "release", "image")
	if err := c.store(np); err != nil {
		t.Fatal(err)
	}
	if _, _, err := r.reconcileNodePools(context.Background(), hcu, regressionHC("4.21.0")); err != nil {
		t.Fatal(err)
	}
	if hcu.Status.NodePoolStatuses[0].StartTime != nil {
		t.Fatal("metadata cleanup invented external rollout start")
	}
}

func TestWorkerFuturePauseCannotHidePreexistingRollout(t *testing.T) {
	hcu := regressionHCU(upgradev1alpha1.PhaseUpgradingNodePools)
	hcu.UID = "upgrade-uid"
	paused, other := regressionNP("paused", ""), regressionNP("other", "")
	_ = unstructured.SetNestedField(paused.Object, hcu.Spec.TargetReleaseImage, "spec", "release", "image")
	_ = unstructured.SetNestedField(paused.Object, time.Now().Add(time.Hour).UTC().Format(time.RFC3339), "spec", "pausedUntil")
	c := &regressionClient{objects: append([]client.Object{paused}, quiescentWorkerFixtures(other)...)}
	r := &HostedClusterUpgradeReconciler{Client: c}
	if err := r.lockClusterNodePools(context.Background(), hcu); err != nil {
		t.Fatal(err)
	}
	if _, _, err := r.reconcileNodePools(context.Background(), hcu, regressionHC("4.21.0")); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(paused), paused); err != nil {
		t.Fatal(err)
	}
	value, _, _ := unstructured.NestedString(paused.Object, "spec", "pausedUntil")
	if value != "true" || hcu.Status.NodePoolStatuses[1].StartTime != nil || hcu.Status.NodePoolStatuses[0].Phase != "Blocked" {
		t.Fatalf("future pause can bypass concurrency at expiry: %+v", hcu.Status)
	}
}

func TestWorkerCoordinatorPauseLeavesOwnedCompletedLock(t *testing.T) {
	hcu := regressionHCU(upgradev1alpha1.PhaseUpgradingNodePools)
	hcu.UID = "upgrade-uid"
	pause := "true"
	hcu.Spec.PausedUntil = &pause
	np := regressionNP("workers", "")
	_ = unstructured.SetNestedField(np.Object, hcu.Spec.TargetReleaseImage, "spec", "release", "image")
	_ = unstructured.SetNestedField(np.Object, "4.21.0", "status", "version")
	_ = unstructured.SetNestedSlice(np.Object, []interface{}{map[string]interface{}{"ocpVersion": "4.21.0", "readyNodeCount": int64(1)}}, "status", "nodesInfo", "nodeVersions")
	md := readyWorkerFixture(np, hcu.Spec.TargetReleaseImage)
	c := &regressionClient{objects: []client.Object{hcu, regressionHC("4.21.0"), np, md}}
	r := &HostedClusterUpgradeReconciler{Client: c}
	if err := r.lockClusterNodePools(context.Background(), hcu); err != nil {
		t.Fatal(err)
	}
	writes := c.writes
	got := reconcileRegression(t, c)
	if c.writes != writes || got.Status.Phase != upgradev1alpha1.PhasePaused {
		t.Fatal("coordinator pause released owned lock")
	}
	got.Spec.PausedUntil = nil
	if err := c.store(got); err != nil {
		t.Fatal(err)
	}
	got = reconcileRegression(t, c)
	if got.Status.Phase != upgradev1alpha1.PhaseCompleted || c.writes != writes+1 {
		t.Fatalf("resume did not restore completed lock: %+v", got.Status)
	}
}
