package controller

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"time"

	upgradev1 "github.com/duritong/hypershift-upgrade-coordinator/api/v1alpha1"
	"github.com/duritong/hypershift-upgrade-coordinator/internal/guest"
	configv1 "github.com/openshift/api/config/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func fixtureConditions(values map[string]bool) []configv1.ClusterOperatorStatusCondition {
	var out []configv1.ClusterOperatorStatusCondition
	// Fixed order keeps bracketing deterministic, independent of map iteration.
	for _, name := range []string{"Available", "Failing", "Degraded", "Progressing"} {
		v, exists := values[name]
		if !exists {
			continue
		}
		status := configv1.ConditionFalse
		if v {
			status = configv1.ConditionTrue
		}
		out = append(out, configv1.ClusterOperatorStatusCondition{Type: configv1.ClusterStatusConditionType(name), Status: status, LastTransitionTime: metav1.NewTime(time.Unix(1, 0))})
	}
	return out
}

func fixtureCV(image, clusterID string) configv1.ClusterVersion {
	version, _ := upgradev1.ExtractVersion(image)
	return configv1.ClusterVersion{TypeMeta: metav1.TypeMeta{Kind: "ClusterVersion", APIVersion: "config.openshift.io/v1"}, ObjectMeta: metav1.ObjectMeta{Name: "version", UID: "cv-uid", Generation: 1, ResourceVersion: "1"}, Spec: configv1.ClusterVersionSpec{ClusterID: configv1.ClusterID(clusterID)}, Status: configv1.ClusterVersionStatus{ObservedGeneration: 1, Desired: configv1.Release{Version: version, Image: image}, History: []configv1.UpdateHistory{{State: configv1.CompletedUpdate, Version: version, Image: image}}, Conditions: fixtureConditions(map[string]bool{"Available": true, "Failing": false, "Progressing": false})}}
}
func fixtureOperators(image string) []configv1.ClusterOperator {
	version, _ := upgradev1.ExtractVersion(image)
	return []configv1.ClusterOperator{{ObjectMeta: metav1.ObjectMeta{Name: "monitoring", UID: "monitoring-uid", ResourceVersion: "1"}, Status: configv1.ClusterOperatorStatus{Conditions: fixtureConditions(map[string]bool{"Available": true, "Degraded": false, "Progressing": false}), Versions: []configv1.OperandVersion{{Name: "operator", Version: version}}}}}
}
func setFlowGuestRelease(g *accessGuestFixture, image string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	v := fixtureCV(image, g.clusterID)
	g.version = &v
	g.operators = fixtureOperators(image)
}

// legacyGuestAccess supplies an external TLS guest for management-only regression
// fixtures. Their scripted HC history publications also publish guest releases.
// It returns clients, NEVER a pre-approved policy verdict. Full-flow tests use
// the production provider and independent stateful issuance fixture instead.
type legacyGuestAccess struct {
	client   client.Reader
	fallback *upgradev1.HostedClusterUpgrade
	clock    Clock
}

func legacyGuest(c client.Reader, hcu *upgradev1.HostedClusterUpgrade, clock Clock) GuestAccessReader {
	return &legacyGuestAccess{client: c, fallback: hcu, clock: clock}
}
func (p *legacyGuestAccess) now() time.Time {
	if p.clock != nil {
		return p.clock.Now()
	}
	return time.Now()
}
func (p *legacyGuestAccess) target(ctx context.Context, key client.ObjectKey, expected guest.Release) guest.Target {
	hcu := regressionHCU(upgradev1.PhaseUpgradingNodePools)
	if p.fallback != nil {
		hcu = p.fallback.DeepCopy()
	}
	if p.client != nil {
		fresh := &upgradev1.HostedClusterUpgrade{}
		if p.client.Get(ctx, key, fresh) == nil {
			hcu = fresh
		}
	}
	target := guest.Target{Namespace: key.Namespace, Name: key.Name, HCUUID: hcu.UID, Generation: hcu.Generation, HCUID: "hc-uid", HCGeneration: 1, HCPUID: "hcp-uid", HCPGeneration: 1, CVUID: "cv-uid", ClusterID: "a5e99fb2-0598-4b34-9081-14236b744c16", HCRelease: expected.Image, PlanRevision: accessHash(hcu.Spec), EndpointRevision: "legacy", ProfileRevision: "legacy", SecretUID: "legacy-secret", SecretRevision: "1", CredentialExpiresAt: p.now().Add(time.Hour), Expected: expected}
	if p.client != nil {
		hc := &unstructured.Unstructured{}
		hc.SetGroupVersionKind(gvkHC)
		if p.client.Get(ctx, key, hc) == nil {
			target.HCUID = hc.GetUID()
			target.HCGeneration = hc.GetGeneration()
			id, _, _ := unstructured.NestedString(hc.Object, "spec", "clusterID")
			target.ClusterID = configv1.ClusterID(id)
			target.HCRelease, _, _ = unstructured.NestedString(hc.Object, "spec", "release", "image")
		}
		hcp := &unstructured.Unstructured{}
		hcp.SetGroupVersionKind(gvkHCP)
		if p.client.Get(ctx, client.ObjectKey{Namespace: WorkerControlPlaneNamespace(key.Namespace, key.Name), Name: key.Name}, hcp) == nil {
			target.HCPUID = hcp.GetUID()
			target.HCPGeneration = hcp.GetGeneration()
		}
	}
	return target
}
func (p *legacyGuestAccess) BuildTargetAndReader(ctx context.Context, key client.ObjectKey, expected guest.Release) (guest.Target, *guest.Reader, error) {
	t := p.target(ctx, key, expected)
	image := expected.Image
	if p.client != nil {
		hc := &unstructured.Unstructured{}
		hc.SetGroupVersionKind(gvkHC)
		if p.client.Get(ctx, key, hc) == nil {
			image, _ = GetHostedClusterCurrentRelease(hc)
		}
	}
	v := fixtureCV(image, string(t.ClusterID))
	ops := fixtureOperators(image)
	var calls atomic.Int32
	var s *httptest.Server
	s = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 3 {
			defer func() { go s.Close() }()
		}
		w.Header().Set("Content-Type", "application/json")
		if r.Header.Get("Authorization") != "Bearer observer-token" {
			w.WriteHeader(403)
			return
		}
		if r.URL.Path == "/apis/config.openshift.io/v1/clusterversions/version" {
			_ = json.NewEncoder(w).Encode(v)
		} else {
			_ = json.NewEncoder(w).Encode(configv1.ClusterOperatorList{TypeMeta: metav1.TypeMeta{Kind: "ClusterOperatorList", APIVersion: "config.openshift.io/v1"}, ListMeta: metav1.ListMeta{ResourceVersion: "1"}, Items: ops})
		}
	}))
	// Each collector has exactly three requests; close the fixture when its
	// bounded decision context ends. Callers use context cancellation below.
	context.AfterFunc(ctx, s.Close)
	time.AfterFunc(guest.CollectionTimeout, s.Close)
	c := &rest.Config{Host: s.URL, TLSClientConfig: rest.TLSClientConfig{CAData: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: s.Certificate().Raw})}}
	data, err := observerKubeconfig(c, "observer-token")
	if err != nil {
		return t, nil, err
	}
	r, err := guest.NewReader(data, t, p.now)
	if err != nil {
		s.Close()
	}
	return t, r, err
}
func (p *legacyGuestAccess) ValidateTarget(ctx context.Context, previous guest.Target) error {
	current := p.target(ctx, client.ObjectKey{Namespace: previous.Namespace, Name: previous.Name}, previous.Expected)
	if current.HCUUID != previous.HCUUID || current.Generation != previous.Generation || current.PlanRevision != previous.PlanRevision || current.HCUID != previous.HCUID || current.HCGeneration != previous.HCGeneration || current.HCPUID != previous.HCPUID || current.HCPGeneration != previous.HCPGeneration || current.ClusterID != previous.ClusterID || current.HCRelease != previous.HCRelease {
		return accessFailure("TargetChanged")
	}
	return nil
}
