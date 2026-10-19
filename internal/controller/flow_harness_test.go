package controller

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	upgradev1alpha1 "github.com/duritong/hypershift-upgrade-coordinator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type flowClock struct {
	mu sync.Mutex
	at time.Time
}

func (c *flowClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.at
}

func (c *flowClock) Step(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.at = c.at.Add(d)
}

// flowAction captures successful production writes, not simulator publications.
// Full objects retain lock ownership, pauses, generation and status for assertions.
type flowAction struct {
	at     time.Time
	before client.Object
	after  client.Object
	status bool
}

func (a flowAction) String() string {
	if hcu, ok := a.after.(*upgradev1alpha1.HostedClusterUpgrade); ok {
		return fmt.Sprintf("%s HCU status=%t phase=%s: %s", a.at.Format(time.RFC3339), a.status, hcu.Status.Phase, hcu.Status.Message)
	}
	u, ok := a.after.(*unstructured.Unstructured)
	if !ok {
		return fmt.Sprintf("%s %T/%s", a.at.Format(time.RFC3339), a.after, a.after.GetName())
	}
	image, _, _ := unstructured.NestedString(u.Object, "spec", "release", "image")
	pause, _, _ := unstructured.NestedString(u.Object, "spec", "pausedUntil")
	return fmt.Sprintf("%s %s/%s generation=%d image=%s pause=%q owner=%q", a.at.Format(time.RFC3339), u.GetKind(), u.GetName(), u.GetGeneration(), image, pause, u.GetAnnotations()[workerLockOwner])
}

type flowClient struct {
	*regressionClient
	clock   Clock
	actions []flowAction
}

func (c *flowClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	before := obj.DeepCopyObject().(client.Object)
	if err := c.Get(ctx, client.ObjectKeyFromObject(obj), before); err != nil {
		return err
	}
	if u, ok := obj.(*unstructured.Unstructured); ok {
		old := before.(*unstructured.Unstructured)
		if !reflect.DeepEqual(old.Object["spec"], u.Object["spec"]) {
			u.SetGeneration(old.GetGeneration() + 1)
		}
	}
	if err := c.regressionClient.Update(ctx, obj, opts...); err != nil {
		return err
	}
	c.actions = append(c.actions, flowAction{at: c.clock.Now(), before: before, after: obj.DeepCopyObject().(client.Object)})
	return nil
}

type flowStatusWriter struct {
	client.SubResourceWriter
	c *flowClient
}

func (c *flowClient) Status() client.SubResourceWriter { return flowStatusWriter{c: c} }

func (s flowStatusWriter) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	before := obj.DeepCopyObject().(client.Object)
	if err := s.c.Get(ctx, client.ObjectKeyFromObject(obj), before); err != nil {
		return err
	}
	if err := s.c.regressionClient.Status().Update(ctx, obj, opts...); err != nil {
		return err
	}
	s.c.actions = append(s.c.actions, flowAction{at: s.c.clock.Now(), before: before, after: obj.DeepCopyObject().(client.Object), status: true})
	return nil
}

func (s flowStatusWriter) Patch(ctx context.Context, obj client.Object, _ client.Patch, _ ...client.SubResourcePatchOption) error {
	return s.Update(ctx, obj)
}

// upgradeFlow runs production decisions. External HyperShift/CAPI progress is
// published explicitly by tests, only after inspecting actual requested specs.
// It does not model API conflicts, authorization, caches or deployed rollouts.
type upgradeFlow struct {
	t         *testing.T
	clock     *flowClock
	c         *flowClient
	r         *HostedClusterUpgradeReconciler
	guest     *accessGuestFixture
	access    *GuestAccessReconciler
	accessDue time.Time
}

func newUpgradeFlow(t *testing.T, eus bool, strategy string, pools int, parallel int32) *upgradeFlow {
	t.Helper()
	resetStepGauges(t)
	clock := &flowClock{at: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	hcu, hc := regressionHCU(""), regressionHC("4.20.0")
	hcu.UID = "flow-upgrade"
	hc.SetUID("flow-cluster")
	hc.SetGeneration(1)
	_ = unstructured.SetNestedField(hc.Object, "a5e99fb2-0598-4b34-9081-14236b744c16", "spec", "clusterID")
	_ = unstructured.SetNestedField(hc.Object, "guest-admin", "status", "kubeconfig", "name")
	setHealthyHostedClusterStatus(hc, clock.Now())
	hcu.Spec.RolloutPolicy = &upgradev1alpha1.RolloutPolicySpec{MaxParallelNodePools: parallel}
	if eus {
		hcu.Spec.IntermediateReleaseImage = regressionImage("4.21.0")
		hcu.Spec.TargetReleaseImage = regressionImage("4.22.0")
	}
	c := &flowClient{regressionClient: &regressionClient{objects: []client.Object{hcu, hc}}, clock: clock}
	addReadyCP(c.regressionClient, hc)
	for i := 0; i < pools; i++ {
		np := regressionNP(fmt.Sprintf("pool-%d", i), "")
		np.SetGeneration(1)
		backing := flowReadyWorker(np, regressionImage("4.20.0"), strategy)
		c.objects = append(c.objects, np, backing)
	}
	g := newAccessGuest(t, clock.Now().Add(time.Hour))
	setFlowGuestRelease(g, regressionImage("4.20.0"))
	admin := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "guest-admin", Namespace: hcu.Namespace, UID: "admin-uid", ResourceVersion: "1"}, Data: map[string][]byte{"kubeconfig": accessKubeconfig(t, guestFixtureConfig(g), "admin-token")}}
	c.objects = append(c.objects, admin)
	access := &GuestAccessReconciler{Client: c, GuestAccessProvider: GuestAccessProvider{APIReader: c, OperatorNamespace: "operator", EndpointMode: GuestEndpointPublished, Clock: clock}}
	f := &upgradeFlow{t: t, clock: clock, c: c, guest: g, access: access, r: &HostedClusterUpgradeReconciler{Client: c, APIReader: c, GuestAccess: &access.GuestAccessProvider, Clock: clock, ReleaseResolver: fixtureReleaseResolver{}}}
	f.maintainAccess()
	return f
}

func flowReadyWorker(np *unstructured.Unstructured, image, strategy string) *unstructured.Unstructured {
	backing := readyWorkerFixture(np, image)
	version, _ := upgradev1alpha1.ExtractVersion(image)
	config := "config-" + version
	annotations := np.GetAnnotations()
	annotations[workerCurrentConfig] = config
	np.SetAnnotations(annotations)
	_ = unstructured.SetNestedField(backing.Object, "user-data-"+np.GetName()+"-"+config, "spec", "template", "spec", "bootstrap", "dataSecretName")
	if strategy == "InPlace" {
		_ = unstructured.SetNestedField(np.Object, strategy, "spec", "management", "upgradeType")
		backing.SetGroupVersionKind(gvkMachineSet)
		backing.SetAnnotations(map[string]string{workerPoolAnnotation: client.ObjectKeyFromObject(np).String(), workerCurrentConfig: config, workerTargetConfig: config})
	}
	return backing
}

func (f *upgradeFlow) hcu() *upgradev1alpha1.HostedClusterUpgrade {
	f.t.Helper()
	u := &upgradev1alpha1.HostedClusterUpgrade{}
	f.get(u, "clusters", "example")
	return u
}

func (f *upgradeFlow) get(obj client.Object, namespace, name string) {
	f.t.Helper()
	if err := f.c.Get(context.Background(), client.ObjectKey{Namespace: namespace, Name: name}, obj); err != nil {
		f.t.Fatal(err)
	}
}

func (f *upgradeFlow) object(kind, name string) *unstructured.Unstructured {
	f.t.Helper()
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(gvkHC.GroupVersion().WithKind(kind))
	f.get(u, "clusters", name)
	return u
}

func (f *upgradeFlow) publish(obj client.Object) {
	f.t.Helper()
	if err := f.c.store(obj); err != nil {
		f.t.Fatal(err)
	}
}

func (f *upgradeFlow) step() ctrl.Result {
	f.t.Helper()
	if !f.clock.Now().Before(f.accessDue) {
		f.maintainAccess()
	}
	// Deliver the same status event that wakes the independent production queue.
	if a := f.hcu().Status.GuestAssessment; a != nil && (a.State == "AccessDenied" || a.State == "Unavailable" || a.State == "IdentityMismatch") {
		f.maintainAccess()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result, err := f.r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKey{Namespace: "clusters", Name: "example"}})
	if err != nil {
		f.t.Fatalf("reconcile: %v; trace=%+v", err, f.c.actions)
	}
	return result
}

func (f *upgradeFlow) until(phase upgradev1alpha1.UpgradePhase) {
	f.t.Helper()
	for range 20 {
		if f.hcu().Status.Phase == phase {
			return
		}
		f.step()
	}
	f.t.Fatalf("did not reach %s: %+v; trace=%+v", phase, f.hcu().Status, f.c.actions)
}

// completeCP publishes the requested payload first with stale CPC evidence.
// The caller deliberately supplies the final fresh CPC acknowledgement later.
func (f *upgradeFlow) completeCP() *unstructured.Unstructured {
	f.t.Helper()
	hc := f.object("HostedCluster", "example")
	image, _, _ := unstructured.NestedString(hc.Object, "spec", "release", "image")
	version, _ := upgradev1alpha1.ExtractVersion(image)
	hcp := readyHCP(hc)
	oldHCP := hcp.DeepCopy()
	f.get(oldHCP, hcp.GetNamespace(), hcp.GetName())
	hcp.SetGeneration(oldHCP.GetGeneration() + 1)
	f.publish(hcp)
	for _, field := range []string{"version", "controlPlaneVersion"} {
		history, _, _ := unstructured.NestedSlice(hc.Object, "status", field, "history")
		history = append([]interface{}{map[string]interface{}{"state": "Completed", "image": image, "version": version}}, history...)
		_ = unstructured.SetNestedSlice(hc.Object, history, "status", field, "history")
	}
	_ = unstructured.SetNestedField(hc.Object, hcp.GetGeneration(), "status", "controlPlaneVersion", "observedGeneration")
	setHealthyHostedClusterStatus(hc, f.clock.Now())
	f.publish(hc)
	setFlowGuestRelease(f.guest, image)
	cpc := readyStepCPC(version)
	_ = unstructured.SetNestedField(cpc.Object, hcp.GetGeneration(), "status", "observedGeneration")
	return cpc
}

func (f *upgradeFlow) maintainAccess() {
	f.t.Helper()
	f.guest.mu.Lock()
	f.guest.expires = f.clock.Now().Add(time.Hour)
	f.guest.mu.Unlock()
	result, err := f.access.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKey{Namespace: "clusters", Name: "example"}})
	if err != nil {
		f.t.Fatal(err)
	}
	f.accessDue = f.clock.Now().Add(result.RequeueAfter)
}

func (f *upgradeFlow) completeWorker(name string) {
	f.t.Helper()
	np := f.object("NodePool", name)
	image, _, _ := unstructured.NestedString(np.Object, "spec", "release", "image")
	if image != f.hcu().Spec.TargetReleaseImage || np.GetAnnotations()[workerLockOwner] != "" {
		f.t.Fatalf("worker not actually requested/unlocked: %s", name)
	}
	strategy, _, _ := unstructured.NestedString(np.Object, "spec", "management", "upgradeType")
	backing := flowReadyWorker(np, image, strategy)
	old := backing.DeepCopy()
	f.get(old, backing.GetNamespace(), backing.GetName())
	backing.SetGeneration(old.GetGeneration() + 1)
	_ = unstructured.SetNestedField(backing.Object, backing.GetGeneration(), "status", "observedGeneration")
	f.publish(np)
	f.publish(backing)
}

func (f *upgradeFlow) requests() []string {
	var requests []string
	for _, a := range f.c.actions {
		u, ok := a.after.(*unstructured.Unstructured)
		if !ok || a.status {
			continue
		}
		old := a.before.(*unstructured.Unstructured)
		before, _, _ := unstructured.NestedString(old.Object, "spec", "release", "image")
		after, _, _ := unstructured.NestedString(u.Object, "spec", "release", "image")
		if before != after {
			version, _ := upgradev1alpha1.ExtractVersion(after)
			requests = append(requests, u.GetKind()+"/"+u.GetName()+"/"+version)
		}
	}
	return requests
}
