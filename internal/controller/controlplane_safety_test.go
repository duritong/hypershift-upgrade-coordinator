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

func TestUpstreamCNOBugTolerationOnAgentPlatform(t *testing.T) {
	hcu := regressionHCU(upgradev1alpha1.PhasePreflightChecks)
	hcu.Spec.TargetReleaseImage = regressionImage("4.21.0")
	hc := regressionHC("4.21.0")
	_ = unstructured.SetNestedField(hc.Object, "Agent", "spec", "platform", "type")

	cpHistory, _, _ := unstructured.NestedSlice(hc.Object, "status", "controlPlaneVersion", "history")
	if len(cpHistory) > 0 {
		cpHistory[0].(map[string]interface{})["state"] = "Partial"
		_ = unstructured.SetNestedSlice(hc.Object, cpHistory, "status", "controlPlaneVersion", "history")
	}

	np := regressionNP("workers", "")
	md := readyWorkerFixture(np, regressionImage("4.21.0"))
	c := &regressionClient{objects: []client.Object{hcu, hc, np, md}}
	addReadyCP(c, hc)

	cno := &unstructured.Unstructured{}
	cno.SetGroupVersionKind(gvkCPC)
	cno.SetName("cluster-network-operator")
	cno.SetNamespace(WorkerControlPlaneNamespace(hc.GetNamespace(), hc.GetName()))
	_ = unstructured.SetNestedField(cno.Object, int64(1), "status", "observedGeneration")
	_ = unstructured.SetNestedSlice(cno.Object, []interface{}{
		map[string]interface{}{"type": "Available", "status": "True", "message": "Deployment cluster-network-operator is available"},
		map[string]interface{}{"type": "RolloutComplete", "status": "False", "reason": "WaitingForOperands", "message": `failed to get deployment cloud-network-config-controller: Deployment.apps "cloud-network-config-controller" not found`},
	}, "status", "conditions")
	c.objects = append(c.objects, cno)

	r := &HostedClusterUpgradeReconciler{Client: c, APIReader: c, ReleaseResolver: fixtureReleaseResolver{}, GuestAccess: legacyGuest(c, hcu, nil)}
	got := reconcileReleaseTest(t, r, c, hcu)
	if got.Status.Phase == upgradev1alpha1.PhaseBlocked {
		t.Fatalf("CNO bug should be tolerated on Agent platform, but got blocked: %s", got.Status.Message)
	}
}

func TestUpstreamCNOBugNotToleratedOnAWSPlatform(t *testing.T) {
	hcu := regressionHCU(upgradev1alpha1.PhasePreflightChecks)
	hcu.Spec.TargetReleaseImage = regressionImage("4.21.0")
	hc := regressionHC("4.21.0")
	_ = unstructured.SetNestedField(hc.Object, "AWS", "spec", "platform", "type")

	cpHistory, _, _ := unstructured.NestedSlice(hc.Object, "status", "controlPlaneVersion", "history")
	if len(cpHistory) > 0 {
		cpHistory[0].(map[string]interface{})["state"] = "Partial"
		_ = unstructured.SetNestedSlice(hc.Object, cpHistory, "status", "controlPlaneVersion", "history")
	}

	np := regressionNP("workers", "")
	md := readyWorkerFixture(np, regressionImage("4.21.0"))
	c := &regressionClient{objects: []client.Object{hcu, hc, np, md}}
	addReadyCP(c, hc)

	cno := &unstructured.Unstructured{}
	cno.SetGroupVersionKind(gvkCPC)
	cno.SetName("cluster-network-operator")
	cno.SetNamespace(WorkerControlPlaneNamespace(hc.GetNamespace(), hc.GetName()))
	_ = unstructured.SetNestedField(cno.Object, int64(1), "status", "observedGeneration")
	_ = unstructured.SetNestedSlice(cno.Object, []interface{}{
		map[string]interface{}{"type": "Available", "status": "True"},
		map[string]interface{}{"type": "RolloutComplete", "status": "False", "message": `cloud-network-config-controller`},
	}, "status", "conditions")
	c.objects = append(c.objects, cno)

	r := &HostedClusterUpgradeReconciler{Client: c, APIReader: c, ReleaseResolver: fixtureReleaseResolver{}, GuestAccess: legacyGuest(c, hcu, nil)}
	got := reconcileReleaseTest(t, r, c, hcu)
	if got.Status.Phase != upgradev1alpha1.PhaseBlocked && got.Status.Phase != upgradev1alpha1.PhaseVerifyingControlPlane {
		t.Fatalf("CNO bug must NOT be tolerated on AWS cloud platform; got phase: %s", got.Status.Phase)
	}
}

func TestControlPlaneRolloutToleratesTransientComponentRestarts(t *testing.T) {
	intermediateImg := regressionImage("4.21.49")
	targetImg := regressionImage("4.22.22")
	started := metav1.NewTime(time.Now().Add(-4 * time.Minute))

	for _, scenario := range []struct {
		name       string
		conditions []interface{}
	}{
		{
			name: "UnavailableReplicas Degraded during deployment rollout",
			conditions: []interface{}{
				map[string]interface{}{"type": "Available", "status": "True"},
				map[string]interface{}{"type": "ClusterVersionAvailable", "status": "True"},
				map[string]interface{}{"type": "ClusterVersionSucceeding", "status": "True"},
				map[string]interface{}{"type": "Degraded", "status": "True", "reason": "UnavailableReplicas", "message": "openshift-apiserver deployment has 1 unavailable replicas"},
			},
		},
		{
			name: "Etcd and KubeAPIServer restarting with Available False",
			conditions: []interface{}{
				map[string]interface{}{"type": "Available", "status": "False", "reason": "WaitingForAvailable", "message": "Waiting for Kube APIServer deployment to become available"},
				map[string]interface{}{"type": "EtcdAvailable", "status": "False", "reason": "EtcdQuorumUnavailable"},
				map[string]interface{}{"type": "KubeAPIServerAvailable", "status": "False", "reason": "WaitingForAvailable"},
				map[string]interface{}{"type": "ClusterVersionAvailable", "status": "Unknown", "message": "Condition not found in the CVO."},
				map[string]interface{}{"type": "ClusterVersionSucceeding", "status": "Unknown", "message": "Condition not found in the CVO."},
				map[string]interface{}{"type": "Degraded", "status": "True", "reason": "UnavailableReplicas", "message": "kube-apiserver deployment has 1 unavailable replicas"},
			},
		},
		{
			name: "CVO mid-upgrade transient ClusterVersionFailing",
			conditions: []interface{}{
				map[string]interface{}{"type": "Available", "status": "True"},
				map[string]interface{}{"type": "ClusterVersionAvailable", "status": "True"},
				map[string]interface{}{"type": "ClusterVersionSucceeding", "status": "False"},
				map[string]interface{}{"type": "ClusterVersionFailing", "status": "True", "message": "ClusterOperator authentication is not available"},
			},
		},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			// Simulate an HCU that was previously stuck in Blocked (PhaseBeforeBlock=VerifyingControlPlane)
			// to prove existing upgrades self-recover immediately on deployment.
			hcu := regressionHCU(upgradev1alpha1.PhaseBlocked)
			hcu.Status.PhaseBeforeBlock = upgradev1alpha1.PhaseVerifyingControlPlane
			hcu.Spec.IntermediateReleaseImage = intermediateImg
			hcu.Spec.TargetReleaseImage = targetImg
			hcu.Status.StartTime = &started
			hcu.Status.ControlPlaneStatus.Step = "Intermediate"
			hcu.Status.ControlPlaneStatus.StartTime = &started
			hcu.Status.ControlPlaneStatus.TargetReleaseImage = intermediateImg

			hc := regressionHC("4.20.24")
			_ = unstructured.SetNestedField(hc.Object, intermediateImg, "spec", "release", "image")
			_ = unstructured.SetNestedSlice(hc.Object, scenario.conditions, "status", "conditions")

			hcp := readyHCP(hc)
			etcdCPC := readyStepCPC("4.21.49")
			etcdCPC.SetName("etcd")
			etcdCPC.SetNamespace(WorkerControlPlaneNamespace(hc.GetNamespace(), hc.GetName()))

			oasCPC := readyStepCPC("4.20.24")
			oasCPC.SetName("openshift-apiserver")
			oasCPC.SetNamespace(WorkerControlPlaneNamespace(hc.GetNamespace(), hc.GetName()))
			_ = unstructured.SetNestedSlice(oasCPC.Object, []interface{}{
				map[string]interface{}{"type": "Available", "status": "True"},
				map[string]interface{}{"type": "RolloutComplete", "status": "False", "message": "Waiting for deployment openshift-apiserver rollout to finish"},
			}, "status", "conditions")

			c := &regressionClient{objects: []client.Object{hcu, hc, hcp, etcdCPC, oasCPC}}
			r := &HostedClusterUpgradeReconciler{Client: c, APIReader: c, ReleaseResolver: fixtureReleaseResolver{}, GuestAccess: legacyGuest(c, hcu, nil)}

			got := reconcileReleaseTest(t, r, c, hcu)
			if got.Status.Phase != upgradev1alpha1.PhaseVerifyingControlPlane {
				t.Fatalf("expected VerifyingControlPlane during component restart, got %s (%s)", got.Status.Phase, got.Status.Message)
			}
			if got.Status.PhaseBeforeBlock != "" {
				t.Fatalf("expected PhaseBeforeBlock to clear, got %q", got.Status.PhaseBeforeBlock)
			}
			if !got.Status.ControlPlaneStatus.StartTime.Equal(&started) {
				t.Fatalf("control plane timer changed during rollout: %v", got.Status.ControlPlaneStatus.StartTime)
			}
			wantMsg := "Waiting for Control Plane components to complete rollout to 4.21.49 (1/2 ready)."
			if got.Status.Message != wantMsg {
				t.Fatalf("message = %q, want %q", got.Status.Message, wantMsg)
			}
			if c.writes != 0 {
				t.Fatalf("unexpected downstream writes during verification: %d", c.writes)
			}

			// Even once all CPCs and histories report 4.21.49 Completed, step completion MUST still
			// hold in VerifyingControlPlane until transient Degraded/Unavailable conditions clear.
			oasReady := readyStepCPC("4.21.49")
			oasReady.SetName("openshift-apiserver")
			oasReady.SetNamespace(WorkerControlPlaneNamespace(hc.GetNamespace(), hc.GetName()))
			_ = c.store(oasReady)
			for _, field := range []string{"version", "controlPlaneVersion"} {
				_ = unstructured.SetNestedSlice(hc.Object, []interface{}{
					map[string]interface{}{"state": "Completed", "version": "4.21.49", "image": intermediateImg},
				}, "status", field, "history")
				_ = unstructured.SetNestedMap(hc.Object, map[string]interface{}{"version": "4.21.49", "image": intermediateImg}, "status", field, "desired")
			}
			_ = c.store(hc)

			got = reconcileReleaseTest(t, r, c, got)
			if got.Status.Phase != upgradev1alpha1.PhaseVerifyingControlPlane {
				t.Fatalf("unhealthy cluster advanced past intermediate verification: phase=%s msg=%s", got.Status.Phase, got.Status.Message)
			}
		})
	}
}

func TestControlPlaneRolloutAdoptsInFlightRequestDuringRestarts(t *testing.T) {
	for _, phase := range []upgradev1alpha1.UpgradePhase{upgradev1alpha1.PhasePreflightChecks, upgradev1alpha1.PhaseUpgradingControlPlane} {
		t.Run(string(phase), func(t *testing.T) {
			hcu := regressionHCU(phase)
			hcu.Spec.TargetReleaseImage = regressionImage("4.21.49")
			hc := regressionHC("4.20.24")
			_ = unstructured.SetNestedField(hc.Object, hcu.Spec.TargetReleaseImage, "spec", "release", "image")
			_ = unstructured.SetNestedSlice(hc.Object, []interface{}{
				map[string]interface{}{"type": "Available", "status": "False", "reason": "WaitingForAvailable"},
				map[string]interface{}{"type": "Degraded", "status": "True", "reason": "UnavailableReplicas", "message": "etcd statefulset rolling out"},
			}, "status", "conditions")

			c := &regressionClient{objects: []client.Object{hcu, hc, readyHCP(hc)}}
			r := &HostedClusterUpgradeReconciler{Client: c, APIReader: c, ReleaseResolver: fixtureReleaseResolver{}, GuestAccess: legacyGuest(c, hcu, nil)}

			got := reconcileReleaseTest(t, r, c, hcu)
			if got.Status.Phase != upgradev1alpha1.PhaseVerifyingControlPlane || c.writes != 0 {
				t.Fatalf("failed to adopt in-flight CP rollout during component restarts: phase=%s msg=%s writes=%d", got.Status.Phase, got.Status.Message, c.writes)
			}
		})
	}
}

func TestControlPlaneRolloutHardRejectionConditionsBlock(t *testing.T) {
	for _, condType := range []string{
		"ValidHostedControlPlaneConfiguration",
		"ValidConfiguration",
		"SupportedHostedCluster",
		"ValidReleaseImage",
		"ValidReleaseInfo",
		"IgnitionServerValidReleaseInfo",
		"ClusterVersionReleaseAccepted",
		"ReconciliationActive",
	} {
		t.Run(condType, func(t *testing.T) {
			hcu := regressionHCU(upgradev1alpha1.PhaseVerifyingControlPlane)
			hcu.Spec.TargetReleaseImage = regressionImage("4.21.49")
			hcu.Status.ControlPlaneStatus.TargetReleaseImage = hcu.Spec.TargetReleaseImage

			hc := regressionHC("4.20.24")
			_ = unstructured.SetNestedField(hc.Object, hcu.Spec.TargetReleaseImage, "spec", "release", "image")
			_ = unstructured.SetNestedSlice(hc.Object, []interface{}{
				map[string]interface{}{"type": condType, "status": "False", "message": "explicit rejection"},
			}, "status", "conditions")

			c := &regressionClient{objects: []client.Object{hcu, hc}}
			r := &HostedClusterUpgradeReconciler{Client: c, APIReader: c, ReleaseResolver: fixtureReleaseResolver{}}
			got := reconcileReleaseTest(t, r, c, hcu)
			if got.Status.Phase != upgradev1alpha1.PhaseBlocked {
				t.Fatalf("%s=False must block VerifyingControlPlane, got %s (%s)", condType, got.Status.Phase, got.Status.Message)
			}
		})
	}
}

func TestPatchUpgradeIgnoresUpgradeableFalseAndOCP422MigratorBugOnMinor(t *testing.T) {
	const (
		migratorOnlyMsg = `Cluster minor or major version upgrades are not allowed while resource deletions are in progress; resources=deployment "openshift-kube-storage-version-migrator-operator/kube-storage-version-migrator-operator",deployment "openshift-kube-storage-version-migrator/migrator"`
		multipleMsg     = "Cluster should not be upgraded between minor or major versions for multiple reasons: IncompatibleOperatorsInstalled,ResourceDeletesInProgress\n* maximum supported OCP version for openshift-logging/cluster-logging.v6.5.3 is 4.22\n* " + migratorOnlyMsg
	)

	t.Run("patch upgrade 4.22.10 to 4.22.22 proceeds despite MultipleReasons Upgradeable=False", func(t *testing.T) {
		hcu := regressionHCU(upgradev1alpha1.PhasePreflightChecks)
		hcu.Spec.TargetReleaseImage = regressionImage("4.22.22")
		hc := regressionHC("4.22.10")
		_ = unstructured.SetNestedField(hc.Object, "Agent", "spec", "platform", "type")
		conditions, _, _ := unstructured.NestedSlice(hc.Object, "status", "conditions")
		conditions = conditions[:3]
		conditions = append(conditions, map[string]interface{}{
			"type": "ClusterVersionUpgradeable", "status": "False",
			"reason": "MultipleReasons", "message": multipleMsg,
		})
		_ = unstructured.SetNestedSlice(hc.Object, conditions, "status", "conditions")

		np := regressionNP("workers", "")
		_ = unstructured.SetNestedField(np.Object, regressionImage("4.22.10"), "spec", "release", "image")
		_ = unstructured.SetNestedField(np.Object, "4.22.10", "status", "version")
		_ = unstructured.SetNestedSlice(np.Object, []interface{}{map[string]interface{}{
			"ocpVersion": "4.22.10", "readyNodeCount": int64(1), "unreadyNodeCount": int64(0),
		}}, "status", "nodesInfo", "nodeVersions")
		md := readyWorkerFixture(np, regressionImage("4.22.10"))

		c := &regressionClient{objects: []client.Object{hcu, hc, np, md}}
		addReadyCP(c, hc)

		// Preflight -> PendingSchedule -> UpgradingControlPlane -> VerifyingControlPlane
		got := reconcileRegression(t, c)
		if got.Status.Phase != upgradev1alpha1.PhasePendingSchedule {
			t.Fatalf("preflight held patch upgrade on Upgradeable=False: phase=%s msg=%s", got.Status.Phase, got.Status.Message)
		}
		got = reconcileRegression(t, c)
		if got.Status.Phase != upgradev1alpha1.PhaseUpgradingControlPlane {
			t.Fatalf("schedule check failed: phase=%s msg=%s", got.Status.Phase, got.Status.Message)
		}
		writesBeforeCP := c.writes
		got = reconcileRegression(t, c)
		if got.Status.Phase != upgradev1alpha1.PhaseVerifyingControlPlane || c.writes <= writesBeforeCP {
			t.Fatalf("patch CP rollout did not actuate: phase=%s msg=%s writes=%d", got.Status.Phase, got.Status.Message, c.writes)
		}
	})

	t.Run("minor upgrade 4.22.22 to 4.23.0 blocks on MultipleReasons but tolerates sole OCPBUGS-130965 on non-IBM platforms", func(t *testing.T) {
		for _, tc := range []struct {
			name      string
			platform  string
			reason    string
			msg       string
			wantWrite bool
		}{
			{"MultipleReasons held", "Agent", "MultipleReasons", multipleMsg, false},
			{"sole OCPBUGS-130965 tolerated on Agent", "Agent", "ResourceDeletesInProgress", migratorOnlyMsg, true},
			{"sole OCPBUGS-130965 tolerated on AWS", "AWS", "ResourceDeletesInProgress", migratorOnlyMsg, true},
			{"IBMCloud not exempted", "IBMCloud", "ResourceDeletesInProgress", migratorOnlyMsg, false},
			{"PowerVS not exempted", "PowerVS", "ResourceDeletesInProgress", migratorOnlyMsg, false},
		} {
			t.Run(tc.name, func(t *testing.T) {
				hcu := regressionHCU(upgradev1alpha1.PhaseUpgradingControlPlane)
				hcu.Spec.TargetReleaseImage = regressionImage("4.23.0")
				hc := regressionHC("4.22.22")
				_ = unstructured.SetNestedField(hc.Object, tc.platform, "spec", "platform", "type")
				conditions, _, _ := unstructured.NestedSlice(hc.Object, "status", "conditions")
				conditions = conditions[:3]
				conditions = append(conditions, map[string]interface{}{
					"type": "ClusterVersionUpgradeable", "status": "False",
					"reason": tc.reason, "message": tc.msg,
				})
				_ = unstructured.SetNestedSlice(hc.Object, conditions, "status", "conditions")

				c := &regressionClient{objects: []client.Object{hcu, hc}}
				addReadyCP(c, hc)
				got := reconcileRegression(t, c)
				if (c.writes > 0) != tc.wantWrite {
					t.Fatalf("writes=%d wantWrite=%t (phase=%s msg=%s)", c.writes, tc.wantWrite, got.Status.Phase, got.Status.Message)
				}
			})
		}
	})
}
