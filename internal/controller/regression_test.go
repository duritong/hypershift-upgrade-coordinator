package controller

import (
	"context"
	"crypto/sha256"
	"fmt"
	"reflect"
	"testing"
	"time"

	upgradev1alpha1 "github.com/duritong/hypershift-upgrade-coordinator/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// regressionClient keeps reconciliation tests independent of envtest and records
// spec writes separately from status writes.
type regressionClient struct {
	client.Client
	objects []client.Object
	writes  int
}

func (c *regressionClient) Get(_ context.Context, key client.ObjectKey, out client.Object, _ ...client.GetOption) error {
	for _, obj := range c.objects {
		if client.ObjectKeyFromObject(obj) == key && reflect.TypeOf(obj) == reflect.TypeOf(out) && obj.GetObjectKind().GroupVersionKind() == out.GetObjectKind().GroupVersionKind() {
			reflect.ValueOf(out).Elem().Set(reflect.ValueOf(obj.DeepCopyObject()).Elem())
			return nil
		}
	}
	return apierrors.NewNotFound(schema.GroupResource{Resource: "test"}, key.Name)
}

func (c *regressionClient) List(_ context.Context, out client.ObjectList, opts ...client.ListOption) error {
	options := &client.ListOptions{}
	options.ApplyOptions(opts)
	if list, ok := out.(*upgradev1alpha1.HostedClusterUpgradeList); ok {
		list.Items = nil
		for _, obj := range c.objects {
			u, ok := obj.(*upgradev1alpha1.HostedClusterUpgrade)
			if ok && (options.Namespace == "" || u.Namespace == options.Namespace) && (options.FieldSelector == nil || options.FieldSelector.Matches(fields.Set{controlPlaneNamespaceField: WorkerControlPlaneNamespace(u.Namespace, u.Name)})) {
				list.Items = append(list.Items, *u.DeepCopy())
			}
		}
		return nil
	}
	list := out.(*unstructured.UnstructuredList)
	list.Items = nil
	for _, obj := range c.objects {
		u, ok := obj.(*unstructured.Unstructured)
		if ok && u.GroupVersionKind() == list.GroupVersionKind() && (options.Namespace == "" || u.GetNamespace() == options.Namespace) {
			cluster, _, _ := unstructured.NestedString(u.Object, "spec", "clusterName")
			if options.FieldSelector != nil && !options.FieldSelector.Matches(fields.Set{nodePoolClusterField: cluster}) {
				continue
			}
			list.Items = append(list.Items, *u.DeepCopy())
		}
	}
	return nil
}

func (c *regressionClient) Update(_ context.Context, obj client.Object, _ ...client.UpdateOption) error {
	if _, downstream := obj.(*unstructured.Unstructured); downstream {
		c.writes++
	}
	return c.store(obj)
}

func (c *regressionClient) store(obj client.Object) error {
	for i, old := range c.objects {
		if client.ObjectKeyFromObject(old) == client.ObjectKeyFromObject(obj) && reflect.TypeOf(old) == reflect.TypeOf(obj) && old.GetObjectKind().GroupVersionKind() == obj.GetObjectKind().GroupVersionKind() {
			c.objects[i] = obj.DeepCopyObject().(client.Object)
			return nil
		}
	}
	return fmt.Errorf("object not found: %s", obj.GetName())
}

type regressionStatusWriter struct {
	client.SubResourceWriter
	c *regressionClient
}

func (c *regressionClient) Status() client.SubResourceWriter {
	return regressionStatusWriter{c: c}
}

func (s regressionStatusWriter) Update(_ context.Context, obj client.Object, _ ...client.SubResourceUpdateOption) error {
	return s.c.store(obj)
}

func regressionImage(version string) string {
	return fmt.Sprintf("quay.io/ocp:%s-x86_64@sha256:%x", version, sha256.Sum256([]byte(version)))
}

func regressionHC(version string) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(gvkHC)
	u.SetNamespace("clusters")
	u.SetName("example")
	_ = unstructured.SetNestedField(u.Object, regressionImage(version), "spec", "release", "image")
	_ = unstructured.SetNestedSlice(u.Object, []interface{}{map[string]interface{}{
		"state": "Completed", "version": version, "image": regressionImage(version),
	}}, "status", "version", "history")
	history, _, _ := unstructured.NestedSlice(u.Object, "status", "version", "history")
	_ = unstructured.SetNestedSlice(u.Object, history, "status", "controlPlaneVersion", "history")
	_ = unstructured.SetNestedField(u.Object, int64(1), "status", "controlPlaneVersion", "observedGeneration")
	setHealthyHostedClusterStatus(u)
	return u
}

func setHealthyHostedClusterStatus(hc *unstructured.Unstructured) {
	conditions := []interface{}{}
	for _, typ := range []string{"Available", "ClusterVersionAvailable", "ClusterVersionSucceeding", "ClusterVersionUpgradeable"} {
		conditions = append(conditions, map[string]interface{}{
			"type": typ, "status": "True", "reason": "AsExpected", "message": "Ready",
			"lastTransitionTime": time.Now().Format(time.RFC3339), "observedGeneration": hc.GetGeneration(),
		})
	}
	_ = unstructured.SetNestedSlice(hc.Object, conditions, "status", "conditions")
}

func regressionHCU(phase upgradev1alpha1.UpgradePhase) *upgradev1alpha1.HostedClusterUpgrade {
	return &upgradev1alpha1.HostedClusterUpgrade{
		ObjectMeta: metav1.ObjectMeta{Namespace: "clusters", Name: "example", Generation: 1},
		Spec:       upgradev1alpha1.HostedClusterUpgradeSpec{TargetReleaseImage: regressionImage("4.21.0")},
		Status:     upgradev1alpha1.HostedClusterUpgradeStatus{Phase: phase},
	}
}

func reconcileRegression(t *testing.T, c *regressionClient) *upgradev1alpha1.HostedClusterUpgrade {
	t.Helper()
	r := &HostedClusterUpgradeReconciler{Client: c, ReleaseResolver: fixtureReleaseResolver{}}
	key := client.ObjectKey{Namespace: "clusters", Name: "example"}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatal(err)
	}
	hcu := &upgradev1alpha1.HostedClusterUpgrade{}
	if err := c.Get(context.Background(), key, hcu); err != nil {
		t.Fatal(err)
	}
	return hcu
}

func TestCompletedUpgradeNewTarget(t *testing.T) {
	for _, changed := range []bool{false, true} {
		t.Run(fmt.Sprintf("changed=%t", changed), func(t *testing.T) {
			hcu := regressionHCU(upgradev1alpha1.PhaseCompleted)
			hcu.Status.ObservedGeneration = 1
			hcu.Status.ControlPlaneStatus.TargetReleaseImage = hcu.Spec.TargetReleaseImage
			started := metav1.NewTime(time.Now().Add(-time.Hour))
			hcu.Status.StartTime, hcu.Status.CompletionTime = &started, &started
			if changed {
				hcu.Generation = 2
				hcu.Spec.TargetReleaseImage = regressionImage("4.22.0")
			}
			c := &regressionClient{objects: []client.Object{hcu, regressionHC("4.21.0")}}
			got := reconcileRegression(t, c)
			if changed {
				if got.Status.Phase != upgradev1alpha1.PhasePreflightChecks || got.Status.CompletionTime != nil || got.Status.StartTime != nil || got.Status.ControlPlaneStatus.TargetReleaseImage != "" {
					t.Fatalf("new target retained completed state: %+v", got.Status)
				}
			} else if got.Status.Phase != upgradev1alpha1.PhaseCompleted {
				t.Fatalf("unchanged target restarted: %s", got.Status.Phase)
			}
			if got.Status.ObservedGeneration != hcu.Generation {
				t.Fatalf("observed generation = %d, want %d", got.Status.ObservedGeneration, hcu.Generation)
			}
		})
	}
}

func regressionNP(name, group string) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(gvkNP)
	u.SetNamespace("clusters")
	u.SetName(name)
	u.SetLabels(map[string]string{"upgrade.huc.0xshift.dev/group": group})
	_ = unstructured.SetNestedField(u.Object, "example", "spec", "clusterName")
	_ = unstructured.SetNestedField(u.Object, "AWS", "spec", "platform", "type")
	_ = unstructured.SetNestedField(u.Object, "Replace", "spec", "management", "upgradeType")
	_ = unstructured.SetNestedField(u.Object, regressionImage("4.20.0"), "spec", "release", "image")
	_ = unstructured.SetNestedField(u.Object, int64(1), "status", "replicas")
	return u
}

func TestNodePoolGroupSafety(t *testing.T) {
	for _, duplicate := range []bool{false, true} {
		t.Run(fmt.Sprintf("duplicate=%t", duplicate), func(t *testing.T) {
			hcu := regressionHCU(upgradev1alpha1.PhaseUpgradingNodePools)
			hcu.Spec.NodePoolGroups = []upgradev1alpha1.NodePoolGroupSpec{{Name: "infra"}}
			group := "unknown"
			if duplicate {
				group = "infra"
				hcu.Spec.NodePoolGroups = append(hcu.Spec.NodePoolGroups, upgradev1alpha1.NodePoolGroupSpec{Name: "infra"})
			}
			c := &regressionClient{objects: []client.Object{regressionNP("workers", group)}}
			r := &HostedClusterUpgradeReconciler{Client: c}
			_, _, err := r.reconcileNodePools(context.Background(), hcu, regressionHC("4.21.0"))
			if err == nil || c.writes != 0 || hcu.Status.Phase == upgradev1alpha1.PhaseCompleted {
				t.Fatalf("invalid groups must block without mutations: err=%v writes=%d phase=%s", err, c.writes, hcu.Status.Phase)
			}
		})
	}
}

func TestControllerVersionSafety(t *testing.T) {
	for _, phase := range []upgradev1alpha1.UpgradePhase{upgradev1alpha1.PhasePreflightChecks, upgradev1alpha1.PhaseUpgradingControlPlane, upgradev1alpha1.PhaseUpgradingNodePools} {
		for _, target := range []string{regressionImage("4.19.0"), regressionImage("5.0.0"), regressionImage("4.22.0"), "invalid"} {
			t.Run(string(phase)+"/"+target, func(t *testing.T) {
				hcu := regressionHCU(phase)
				hcu.Spec.TargetReleaseImage = target
				c := &regressionClient{objects: []client.Object{hcu, regressionHC("4.20.0"), regressionNP("workers", "")}}
				got := reconcileRegression(t, c)
				if got.Status.Phase != upgradev1alpha1.PhaseBlocked || c.writes != 0 {
					t.Fatalf("unsafe path was not blocked before writes: phase=%s writes=%d", got.Status.Phase, c.writes)
				}
			})
		}
	}
}

func TestControllerRecoveryAtTargetVerifiesComponents(t *testing.T) {
	hcu := regressionHCU(upgradev1alpha1.PhaseUpgradingControlPlane)
	c := &regressionClient{objects: []client.Object{hcu, regressionHC("4.21.0")}}
	got := reconcileRegression(t, c)
	if got.Status.Phase != upgradev1alpha1.PhaseVerifyingControlPlane {
		t.Fatalf("already-target recovery bypassed component verification: %s", got.Status.Phase)
	}
	got = reconcileRegression(t, c)
	if got.Status.Phase != upgradev1alpha1.PhaseVerifyingControlPlane {
		t.Fatalf("missing CPCs must prevent workers: %s", got.Status.Phase)
	}
}

func TestNodePoolReadinessEvidence(t *testing.T) {
	for _, tt := range []struct {
		name                              string
		replicas, ready, unready          int64
		version                           string
		info                              bool
		wantTotal, wantUpdated, wantReady int32
	}{
		{"version alone", 3, 0, 0, "4.21.0", false, 3, 0, 0},
		{"target ready", 3, 3, 0, "4.21.0", true, 3, 3, 3},
		{"target unready", 3, 0, 3, "4.21.0", true, 3, 3, 0},
		{"old ready", 3, 3, 0, "4.20.0", true, 3, 0, 0},
		{"newer is not target", 3, 3, 0, "4.22.0", true, 3, 0, 0},
		{"desired exceeds observed", 1, 1, 0, "4.21.0", true, 3, 1, 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			np := regressionNP("workers", "")
			_ = unstructured.SetNestedField(np.Object, int64(3), "spec", "replicas")
			_ = unstructured.SetNestedField(np.Object, tt.replicas, "status", "replicas")
			_ = unstructured.SetNestedField(np.Object, tt.version, "status", "version")
			if tt.info {
				_ = unstructured.SetNestedSlice(np.Object, []interface{}{map[string]interface{}{
					"ocpVersion": tt.version, "readyNodeCount": tt.ready, "unreadyNodeCount": tt.unready,
				}}, "status", "nodesInfo", "nodeVersions")
			}
			total, updated, ready := GetNodePoolNodeCounts(np, regressionImage("4.21.0"))
			if total != tt.wantTotal || updated != tt.wantUpdated || ready != tt.wantReady {
				t.Fatalf("counts = %d/%d/%d, want %d/%d/%d", total, updated, ready, tt.wantTotal, tt.wantUpdated, tt.wantReady)
			}
		})
	}
}

func TestNodePoolMissingReadinessDoesNotComplete(t *testing.T) {
	for _, missingReplicas := range []bool{false, true} {
		t.Run(fmt.Sprintf("missingReplicas=%t", missingReplicas), func(t *testing.T) {
			hcu := regressionHCU(upgradev1alpha1.PhaseUpgradingNodePools)
			np := regressionNP("workers", "")
			_ = unstructured.SetNestedField(np.Object, hcu.Spec.TargetReleaseImage, "spec", "release", "image")
			_ = unstructured.SetNestedField(np.Object, "4.21.0", "status", "version")
			if missingReplicas {
				unstructured.RemoveNestedField(np.Object, "status", "replicas")
			}
			c := &regressionClient{objects: []client.Object{np}}
			r := &HostedClusterUpgradeReconciler{Client: c}
			if _, _, err := r.reconcileNodePools(context.Background(), hcu, regressionHC("4.21.0")); err != nil {
				t.Fatal(err)
			}
			if hcu.Status.Phase == upgradev1alpha1.PhaseCompleted {
				t.Fatal("NodePool completed without node readiness evidence")
			}
		})
	}
}
