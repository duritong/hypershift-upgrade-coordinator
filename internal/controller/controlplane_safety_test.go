package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	upgradev1alpha1 "github.com/duritong/hypershift-upgrade-coordinator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func readyHCP(hc *unstructured.Unstructured) *unstructured.Unstructured {
	hcp := &unstructured.Unstructured{}
	hcp.SetGroupVersionKind(gvkHCP)
	hcp.SetNamespace(WorkerControlPlaneNamespace(hc.GetNamespace(), hc.GetName()))
	hcp.SetName(hc.GetName())
	hcp.SetUID("hcp-uid")
	hcp.SetGeneration(1)
	image, _, _ := unstructured.NestedString(hc.Object, "spec", "release", "image")
	_ = unstructured.SetNestedField(hcp.Object, image, "spec", "releaseImage")
	return hcp
}

func addReadyCP(c *regressionClient, hc *unstructured.Unstructured) {
	_, version := GetHostedClusterCurrentRelease(hc)
	cpc := readyStepCPC(version)
	cpc.SetNamespace(WorkerControlPlaneNamespace(hc.GetNamespace(), hc.GetName()))
	c.objects = append(c.objects, readyHCP(hc), cpc)
}

func TestControlPlaneInFlightIdentity(t *testing.T) {
	for _, target := range []bool{false, true} {
		hcu := regressionHCU(upgradev1alpha1.PhaseUpgradingControlPlane)
		hcu.Spec.IntermediateReleaseImage = regressionImage("4.21.0")
		hcu.Spec.TargetReleaseImage = regressionImage("4.22.0")
		hc := regressionHC("4.20.0")
		image := regressionImage("4.23.0")
		if target {
			image = hcu.Spec.TargetReleaseImage
		}
		_ = unstructured.SetNestedField(hc.Object, image, "spec", "release", "image")
		c := &regressionClient{objects: []client.Object{hcu, hc}}
		got := reconcileRegression(t, c)
		if c.writes != 0 || (target && got.Status.ControlPlaneStatus.TargetReleaseImage != image) || (!target && got.Status.Phase != upgradev1alpha1.PhaseBlocked) {
			t.Fatalf("in-flight request overwritten or not adopted: %+v writes=%d", got.Status, c.writes)
		}
	}
}

func TestSameVersionDifferentPayloadDoesNotRecoverAsSuccess(t *testing.T) {
	for _, phase := range []upgradev1alpha1.UpgradePhase{upgradev1alpha1.PhaseUpgradingControlPlane, upgradev1alpha1.PhaseVerifyingControlPlane, upgradev1alpha1.PhaseUpgradingNodePools} {
		hcu := regressionHCU(phase)
		hcu.Spec.TargetReleaseImage = strings.Split(hcu.Spec.TargetReleaseImage, "@")[0] + "@sha256:" + strings.Repeat("f", 64)
		hc := regressionHC("4.21.0")
		c := &regressionClient{objects: []client.Object{hcu, hc}}
		addReadyCP(c, hc)
		got := reconcileRegression(t, c)
		if c.writes != 0 || got.Status.Phase == upgradev1alpha1.PhaseCompleted || (phase == upgradev1alpha1.PhaseVerifyingControlPlane && got.Status.Phase != phase) {
			t.Fatalf("different payload recovered as success: %+v writes=%d", got.Status, c.writes)
		}
	}
}

func TestControlPlaneCompletionEvidence(t *testing.T) {
	for _, defect := range []string{"none", "mirror", "different digest", "old completed", "partial", "wrong version", "missing HCP", "stale CPC", "wrong HCP", "HCP management override", "stale management"} {
		t.Run(defect, func(t *testing.T) {
			hcu := regressionHCU(upgradev1alpha1.PhaseVerifyingControlPlane)
			hc := regressionHC("4.21.0")
			hcp, cpc := readyHCP(hc), readyStepCPC("4.21.0")
			cpc.SetGeneration(99) // CPC generation is unrelated to freshness.
			switch defect {
			case "mirror":
				_, digest, _ := strings.Cut(hcu.Spec.TargetReleaseImage, "@")
				mirror := "mirror/ocp@" + digest
				_ = unstructured.SetNestedField(hc.Object, mirror, "spec", "release", "image")
				_ = unstructured.SetNestedField(hcp.Object, mirror, "spec", "releaseImage")
				for _, field := range []string{"version", "controlPlaneVersion"} {
					history, _, _ := unstructured.NestedSlice(hc.Object, "status", field, "history")
					history[0].(map[string]interface{})["image"] = mirror
					_ = unstructured.SetNestedSlice(hc.Object, history, "status", field, "history")
				}
			case "different digest":
				_ = unstructured.SetNestedField(hc.Object, "wrong@sha256:"+strings.Repeat("f", 64), "status", "version", "desired", "image")
			case "old completed", "partial":
				history, _, _ := unstructured.NestedSlice(hc.Object, "status", "version", "history")
				history = append([]interface{}{map[string]interface{}{"state": "Partial", "image": hcu.Spec.TargetReleaseImage, "version": "4.21.0"}}, history...)
				_ = unstructured.SetNestedSlice(hc.Object, history, "status", "version", "history")
			case "wrong version":
				_ = unstructured.SetNestedSlice(hc.Object, []interface{}{map[string]interface{}{"state": "Completed", "image": hcu.Spec.TargetReleaseImage, "version": "4.20.0"}}, "status", "version", "history")
			case "stale CPC":
				hcp.SetGeneration(2)
				_ = unstructured.SetNestedField(hc.Object, int64(2), "status", "controlPlaneVersion", "observedGeneration")
			case "wrong HCP":
				_ = unstructured.SetNestedField(hcp.Object, regressionImage("4.20.0"), "spec", "releaseImage")
			case "HCP management override":
				_ = unstructured.SetNestedField(hcp.Object, regressionImage("4.22.0"), "spec", "controlPlaneReleaseImage")
			case "stale management":
				hcp.SetGeneration(2)
				_ = unstructured.SetNestedField(cpc.Object, int64(2), "status", "observedGeneration")
			}
			c := &regressionClient{objects: []client.Object{hc, cpc}}
			if defect != "missing HCP" {
				c.objects = append(c.objects, hcp)
			}
			err := (&HostedClusterUpgradeReconciler{Client: c}).verifyControlPlane(context.Background(), hcu, hc, hcu.Spec.TargetReleaseImage)
			if (err == nil) != (defect == "none" || defect == "mirror") {
				t.Fatalf("completion evidence %s: %v", defect, err)
			}
		})
	}
}

func TestControlPlaneOverrideRecovery(t *testing.T) {
	for _, phase := range []upgradev1alpha1.UpgradePhase{upgradev1alpha1.PhasePreflightChecks, upgradev1alpha1.PhaseUpgradingControlPlane, upgradev1alpha1.PhaseUpgradingNodePools} {
		hc := regressionHC("4.20.0")
		_ = unstructured.SetNestedField(hc.Object, regressionImage("4.22.0"), "spec", "controlPlaneRelease", "image")
		c := &regressionClient{objects: []client.Object{regressionHCU(phase), hc, regressionNP("workers", "")}}
		got := reconcileRegression(t, c)
		if got.Status.Phase != upgradev1alpha1.PhaseBlocked || c.writes != 0 {
			t.Fatalf("override must block without downstream writes: phase=%s writes=%d", got.Status.Phase, c.writes)
		}
	}
}

func TestControlPlaneRechecksPreviousHop(t *testing.T) {
	for _, phase := range []upgradev1alpha1.UpgradePhase{upgradev1alpha1.PhasePreflightChecks, upgradev1alpha1.PhaseUpgradingControlPlane} {
		for _, defect := range []string{"CPC", "HCP", "health", "paused worker"} {
			t.Run(string(phase)+"/"+defect, func(t *testing.T) {
				hcu := regressionHCU(phase)
				hcu.Spec.TargetReleaseImage = regressionImage("4.22.0")
				hcu.Spec.IntermediateReleaseImage = regressionImage("4.21.0")
				hc := regressionHC("4.21.0")
				np := regressionNP("workers", "")
				md := readyWorkerFixture(np, regressionImage("4.20.0"))
				c := &regressionClient{objects: []client.Object{hcu, hc, np, md}}
				addReadyCP(c, hc)
				switch defect {
				case "CPC":
					_ = unstructured.SetNestedField(c.objects[5].(*unstructured.Unstructured).Object, int64(0), "status", "observedGeneration")
				case "HCP":
					c.objects[4].SetGeneration(2)
				case "health":
					_ = unstructured.SetNestedSlice(hc.Object, nil, "status", "conditions")
				case "paused worker":
					_ = unstructured.SetNestedField(np.Object, "true", "spec", "pausedUntil")
					_ = unstructured.SetNestedSlice(np.Object, []interface{}{map[string]interface{}{"type": "UpdatingConfig", "status": "True"}}, "status", "conditions")
				}
				r := &HostedClusterUpgradeReconciler{Client: c, ReleaseResolver: fixtureReleaseResolver{}}
				_, _ = r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(hcu)})
				if c.writes != 0 {
					t.Fatalf("degraded prerequisite allowed locks or CP mutation: %d writes", c.writes)
				}
			})
		}
	}
}

func TestWorkersRecheckControlPlaneAfterSchedule(t *testing.T) {
	hcu := regressionHCU(upgradev1alpha1.PhaseUpgradingNodePools)
	future := metav1.NewTime(time.Now().Add(time.Hour))
	hcu.Spec.NodePoolGroups = []upgradev1alpha1.NodePoolGroupSpec{{Name: "default", StartAt: &future}}
	hc := regressionHC("4.21.0")
	a, b := regressionNP("active", ""), regressionNP("waiting", "")
	_ = unstructured.SetNestedField(a.Object, hcu.Spec.TargetReleaseImage, "spec", "release", "image")
	hcu.Status.NodePoolStatuses = []upgradev1alpha1.NodePoolUpgradeStatus{{Name: a.GetName(), Phase: "Upgrading", TargetReleaseImage: hcu.Spec.TargetReleaseImage}}
	c := &regressionClient{objects: append([]client.Object{a}, quiescentWorkerFixtures(b)...)}
	addReadyCP(c, hc)
	r := &HostedClusterUpgradeReconciler{Client: c}
	if _, _, err := r.reconcileNodePools(context.Background(), hcu, hc); err != nil {
		t.Fatal(err)
	}
	before := c.writes
	hcu.Spec.NodePoolGroups[0].StartAt = nil
	// The CP degrades while the later worker waits for its schedule.
	c.objects[len(c.objects)-2].SetGeneration(2)
	if _, _, err := r.reconcileNodePools(context.Background(), hcu, hc); err != nil {
		t.Fatal(err)
	}
	if c.writes != before {
		t.Fatal("CP degradation actuated new work or paused an active worker")
	}
	if pause, _, _ := unstructured.NestedString(a.Object, "spec", "pausedUntil"); pause != "" {
		t.Fatal("active worker paused")
	}
}

func TestUpgradeabilityIsOnlyANewControlPlaneGate(t *testing.T) {
	for _, status := range []string{"True", "False", "Unknown", "missing"} {
		for _, phase := range []upgradev1alpha1.UpgradePhase{upgradev1alpha1.PhaseUpgradingControlPlane, upgradev1alpha1.PhaseVerifyingControlPlane, upgradev1alpha1.PhaseUpgradingNodePools} {
			t.Run(status+"/"+string(phase), func(t *testing.T) {
				hcu := regressionHCU(phase)
				version := "4.21.0"
				if phase == upgradev1alpha1.PhaseUpgradingControlPlane {
					version = "4.20.0"
				}
				hc := regressionHC(version)
				conditions, _, _ := unstructured.NestedSlice(hc.Object, "status", "conditions")
				conditions = conditions[:3]
				if status != "missing" {
					conditions = append(conditions, map[string]interface{}{"type": "ClusterVersionUpgradeable", "status": status})
				}
				_ = unstructured.SetNestedSlice(hc.Object, conditions, "status", "conditions")
				c := &regressionClient{objects: []client.Object{hcu, hc}}
				addReadyCP(c, hc)
				if phase == upgradev1alpha1.PhaseUpgradingNodePools {
					c.objects = append(c.objects, quiescentWorkerFixtures(regressionNP("workers", ""))...)
				}
				got := reconcileRegression(t, c)
				if phase == upgradev1alpha1.PhaseUpgradingControlPlane {
					if (c.writes > 0) != (status == "True") {
						t.Fatalf("new release gate: writes=%d status=%+v", c.writes, got.Status)
					}
				} else if phase == upgradev1alpha1.PhaseVerifyingControlPlane && got.Status.Phase != upgradev1alpha1.PhaseUpgradingNodePools {
					t.Fatalf("next-upgrade condition prevented completion: %+v", got.Status)
				} else if phase == upgradev1alpha1.PhaseUpgradingNodePools && c.writes == 0 {
					t.Fatalf("next-upgrade condition prevented workers: %+v", got.Status)
				}
			})
		}
	}
}

func TestWholeControlPlanePathWorkerSkew(t *testing.T) {
	for _, defect := range []string{"too old next hop", "too old final hop", "newer worker", "old bucket", "missing status", "missing buckets", "missing bucket version", "safe"} {
		for _, phase := range []upgradev1alpha1.UpgradePhase{upgradev1alpha1.PhasePreflightChecks, upgradev1alpha1.PhaseUpgradingControlPlane} {
			t.Run(defect+"/"+string(phase), func(t *testing.T) {
				hcu := regressionHCU(phase)
				hc := regressionHC("4.20.0")
				np := regressionNP("workers", "")
				worker := "4.18.0"
				switch defect {
				case "too old next hop":
					worker = "4.17.0"
				case "too old final hop":
					hcu.Spec.IntermediateReleaseImage = regressionImage("4.21.0")
					hcu.Spec.TargetReleaseImage = regressionImage("4.22.0")
				case "newer worker":
					worker = "4.20.1"
				}
				md := readyWorkerFixture(np, regressionImage(worker))
				switch defect {
				case "old bucket":
					_ = unstructured.SetNestedSlice(np.Object, []interface{}{map[string]interface{}{"ocpVersion": "4.17.0", "readyNodeCount": int64(1)}}, "status", "nodesInfo", "nodeVersions")
				case "missing status":
					unstructured.RemoveNestedField(np.Object, "status", "version")
				case "missing buckets":
					unstructured.RemoveNestedField(np.Object, "status", "nodesInfo")
				case "missing bucket version":
					_ = unstructured.SetNestedSlice(np.Object, []interface{}{map[string]interface{}{"readyNodeCount": int64(1)}}, "status", "nodesInfo", "nodeVersions")
				}
				c := &regressionClient{objects: []client.Object{hcu, hc, np, md}}
				addReadyCP(c, hc)
				r := &HostedClusterUpgradeReconciler{Client: c, APIReader: c, ReleaseResolver: fixtureReleaseResolver{}, GuestAccess: legacyGuest(c, hcu, nil)}
				_, _ = r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(hcu)})
				if (c.writes > 0) != (defect == "safe") {
					t.Fatalf("whole path safety: %s allowed writes=%d", defect, c.writes)
				}
			})
		}
	}
}

func TestControlPlaneActuationHonorsChangedSchedule(t *testing.T) {
	hcu := regressionHCU(upgradev1alpha1.PhaseUpgradingControlPlane)
	future := metav1.NewTime(time.Now().Add(time.Hour))
	hcu.Spec.ControlPlane.StartAt = &future
	hc := regressionHC("4.20.0")
	c := &regressionClient{objects: []client.Object{hcu, hc}}
	addReadyCP(c, hc)
	got := reconcileRegression(t, c)
	if c.writes != 0 || got.Status.Phase != upgradev1alpha1.PhasePendingSchedule {
		t.Fatalf("changed schedule bypassed immediately before actuation: %+v writes=%d", got.Status, c.writes)
	}
	// Scheduling edits must not interfere with observing a previously requested rollout.
	_ = unstructured.SetNestedField(hc.Object, hcu.Spec.TargetReleaseImage, "spec", "release", "image")
	if err := c.store(hc); err != nil {
		t.Fatal(err)
	}
	got.Status.Phase = upgradev1alpha1.PhaseUpgradingControlPlane
	if err := c.store(got); err != nil {
		t.Fatal(err)
	}
	got = reconcileRegression(t, c)
	if got.Status.Phase != upgradev1alpha1.PhaseVerifyingControlPlane || c.writes != 0 {
		t.Fatal("future schedule prevented adoption of an already requested rollout")
	}
}
