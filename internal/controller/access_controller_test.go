package controller

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	upgradev1alpha1 "github.com/duritong/hypershift-upgrade-coordinator/api/v1alpha1"
	"github.com/duritong/hypershift-upgrade-coordinator/internal/guest"
	configv1 "github.com/openshift/api/config/v1"
	authv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

type accessTestClock struct{ now time.Time }

func (c *accessTestClock) Now() time.Time { return c.now }

// This fixture implements stateful Kubernetes resource/TokenRequest wire APIs.
// It proves transport and lifecycle behavior, not Kubernetes authorization.
type accessGuestFixture struct {
	mu                                        sync.Mutex
	objects                                   map[string]map[string]interface{}
	writes, tokens, observerReads, adminReads int
	tokenPaths                                []string
	failToken, denyObserver, loseToken        bool
	onToken                                   func()
	expires                                   time.Time
	clusterID                                 string
	version                                   *configv1.ClusterVersion
	operators                                 []configv1.ClusterOperator
	onObserverRead                            func()
	onObserverReadPath                        func(string)
	holdObserver                              bool
	server                                    *httptest.Server
}

func newAccessGuest(t *testing.T, expiry time.Time) *accessGuestFixture {
	t.Helper()
	g := &accessGuestFixture{objects: map[string]map[string]interface{}{}, expires: expiry, clusterID: "a5e99fb2-0598-4b34-9081-14236b744c16"}
	g.server = httptest.NewTLSServer(http.HandlerFunc(g.serve))
	t.Cleanup(g.server.Close)
	return g
}
func (g *accessGuestFixture) serve(w http.ResponseWriter, r *http.Request) {
	g.mu.Lock()
	defer g.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	respond := func(status int, obj interface{}) { w.WriteHeader(status); _ = json.NewEncoder(w).Encode(obj) }
	deny := func(code int) {
		respond(code, metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"}, Status: "Failure", Reason: metav1.StatusReason(http.StatusText(code)), Code: int32(code)})
	}
	if r.Header.Get("Authorization") == "Bearer observer-token" {
		g.observerReads++
		if g.holdObserver {
			<-r.Context().Done()
			return
		}
		if g.onObserverRead != nil {
			g.onObserverRead()
		}
		if g.onObserverReadPath != nil {
			g.onObserverReadPath(r.URL.Path)
		}
		if g.denyObserver {
			deny(403)
			return
		}
	} else if r.Header.Get("Authorization") != "Bearer admin-token" {
		deny(401)
		return
	} else {
		g.adminReads++
	}
	if strings.HasSuffix(r.URL.Path, "/clusterversions/version") {
		if g.version != nil {
			respond(200, g.version)
			return
		}
		// Intentionally unhealthy: access bootstrap must not inspect health.
		respond(200, configv1.ClusterVersion{TypeMeta: metav1.TypeMeta{APIVersion: "config.openshift.io/v1", Kind: "ClusterVersion"}, ObjectMeta: metav1.ObjectMeta{Name: "version", UID: "cv-uid", Generation: 1, ResourceVersion: "1"}, Spec: configv1.ClusterVersionSpec{ClusterID: configv1.ClusterID(g.clusterID)}, Status: configv1.ClusterVersionStatus{ObservedGeneration: 1, Conditions: []configv1.ClusterOperatorStatusCondition{{Type: "Failing", Status: configv1.ConditionTrue}}}})
		return
	}
	if strings.HasSuffix(r.URL.Path, "/clusteroperators") {
		if g.version != nil {
			respond(200, configv1.ClusterOperatorList{TypeMeta: metav1.TypeMeta{APIVersion: "config.openshift.io/v1", Kind: "ClusterOperatorList"}, ListMeta: metav1.ListMeta{ResourceVersion: "1"}, Items: g.operators})
			return
		}
		respond(200, configv1.ClusterOperatorList{TypeMeta: metav1.TypeMeta{APIVersion: "config.openshift.io/v1", Kind: "ClusterOperatorList"}, ListMeta: metav1.ListMeta{ResourceVersion: "1"}})
		return
	}
	if strings.HasSuffix(r.URL.Path, "/token") {
		var request authv1.TokenRequest
		if json.NewDecoder(r.Body).Decode(&request) != nil || request.Spec.ExpirationSeconds == nil || *request.Spec.ExpirationSeconds != 3600 || len(request.Spec.Audiences) != 0 {
			deny(400)
			return
		}
		g.tokens++
		g.tokenPaths = append(g.tokenPaths, r.URL.Path)
		if g.onToken != nil {
			g.onToken()
		}
		if g.loseToken {
			connection, _, err := w.(http.Hijacker).Hijack()
			if err == nil {
				_ = connection.Close()
			}
			return
		}
		if g.failToken {
			deny(500)
			return
		}
		respond(201, authv1.TokenRequest{TypeMeta: metav1.TypeMeta{APIVersion: "authentication.k8s.io/v1", Kind: "TokenRequest"}, Status: authv1.TokenRequestStatus{Token: "observer-token", ExpirationTimestamp: metav1.NewTime(g.expires)}})
		return
	}
	switch r.Method {
	case http.MethodDelete:
		var options metav1.DeleteOptions
		if json.NewDecoder(r.Body).Decode(&options) != nil || options.Preconditions == nil || options.Preconditions.UID == nil || options.Preconditions.ResourceVersion == nil {
			deny(400)
			return
		}
		obj := g.objects[r.URL.Path]
		if obj == nil {
			deny(404)
			return
		}
		m := obj["metadata"].(map[string]interface{})
		if m["uid"] != string(*options.Preconditions.UID) || m["resourceVersion"] != *options.Preconditions.ResourceVersion {
			deny(409)
			return
		}
		delete(g.objects, r.URL.Path)
		g.writes++
		respond(200, metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"}, Status: "Success"})
	case http.MethodGet:
		if obj, ok := g.objects[r.URL.Path]; ok {
			respond(200, obj)
		} else {
			deny(404)
		}
	case http.MethodPost, http.MethodPut:
		var obj map[string]interface{}
		if json.NewDecoder(r.Body).Decode(&obj) != nil {
			deny(400)
			return
		}
		metadata := obj["metadata"].(map[string]interface{})
		path := r.URL.Path
		if r.Method == http.MethodPost {
			path += "/" + metadata["name"].(string)
		}
		if r.Method == http.MethodPost && g.objects[path] != nil {
			deny(409)
			return
		}
		metadata["uid"] = metadata["name"].(string) + "-uid"
		metadata["resourceVersion"] = fmt.Sprint(g.writes + 1)
		g.objects[path] = obj
		g.writes++
		respond(201, obj)
	default:
		deny(405)
	}
}

func accessKubeconfig(t *testing.T, c *rest.Config, token string) []byte {
	t.Helper()
	b, err := observerKubeconfig(c, token)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func newAccessManagement(t *testing.T, c *rest.Config, clock *accessTestClock) (*GuestAccessReconciler, client.ObjectKey) {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := upgradev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	key := client.ObjectKey{Namespace: "clusters", Name: "example"}
	hcu := &upgradev1alpha1.HostedClusterUpgrade{ObjectMeta: metav1.ObjectMeta{Name: key.Name, Namespace: key.Namespace, UID: "hcu-uid", Generation: 1}, Spec: upgradev1alpha1.HostedClusterUpgradeSpec{TargetReleaseImage: "quay.io/ocp/release:4.18.1-x86_64@sha256:" + strings.Repeat("a", 64)}}
	hc := &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": gvkHC.GroupVersion().String(), "kind": "HostedCluster", "metadata": map[string]interface{}{"name": key.Name, "namespace": key.Namespace, "uid": "hc-uid"}, "spec": map[string]interface{}{"clusterID": "a5e99fb2-0598-4b34-9081-14236b744c16", "release": map[string]interface{}{"image": "quay.io/ocp/release:4.17.1-x86_64@sha256:" + strings.Repeat("b", 64)}}, "status": map[string]interface{}{"kubeconfig": map[string]interface{}{"name": "admin", "key": "kubeconfig"}}}}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "admin", Namespace: key.Namespace, UID: "admin-uid"}, Data: map[string][]byte{"kubeconfig": accessKubeconfig(t, c, "admin-token")}}
	hc.SetGeneration(1)
	hcp := readyHCP(hc)
	k := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(hcu).WithObjects(hcu, hc, hcp, secret).WithInterceptorFuncs(interceptor.Funcs{Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
		if obj.GetUID() == "" {
			obj.SetUID(types.UID(obj.GetName() + "-uid"))
		}
		return c.Create(ctx, obj, opts...)
	}}).Build()
	return &GuestAccessReconciler{Client: k, GuestAccessProvider: GuestAccessProvider{APIReader: k, OperatorNamespace: "operator", HostedClusterNamespace: "clusters", EndpointMode: GuestEndpointPublished, Clock: clock}}, key
}

func accessReconcile(t *testing.T, r *GuestAccessReconciler, key client.ObjectKey) *upgradev1alpha1.HostedClusterUpgrade {
	t.Helper()
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatal(err)
	}
	hcu := &upgradev1alpha1.HostedClusterUpgrade{}
	if err := r.APIReader.Get(context.Background(), key, hcu); err != nil {
		t.Fatal(err)
	}
	return hcu
}
func guestFixtureConfig(g *accessGuestFixture) *rest.Config {
	return &rest.Config{Host: g.server.URL, TLSClientConfig: rest.TLSClientConfig{CAData: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: g.server.Certificate().Raw})}}
}
func accessDelivery(t *testing.T, r *GuestAccessReconciler) *corev1.Secret {
	t.Helper()
	secret := &corev1.Secret{}
	if err := r.APIReader.Get(context.Background(), client.ObjectKey{Namespace: r.OperatorNamespace, Name: deliveryName("hc-uid")}, secret); err != nil {
		t.Fatal(err)
	}
	return secret
}
func pauseAccess(t *testing.T, r *GuestAccessReconciler, key client.ObjectKey) {
	t.Helper()
	hcu := &upgradev1alpha1.HostedClusterUpgrade{}
	if err := r.APIReader.Get(context.Background(), key, hcu); err != nil {
		t.Fatal(err)
	}
	value := "true"
	hcu.Spec.PausedUntil = &value
	hcu.Generation++
	if err := r.Client.Update(context.Background(), hcu); err != nil {
		t.Fatal(err)
	}
}

func TestGuestAccessBootstrapAndRotation(t *testing.T) {
	clock := &accessTestClock{now: time.Now().UTC().Truncate(time.Second)}
	g := newAccessGuest(t, clock.now.Add(40*time.Minute))
	r, key := newAccessManagement(t, guestFixtureConfig(g), clock)
	hcu := accessReconcile(t, r, key)
	if !hcu.Status.GuestAccess.MaintenanceReady || !hcu.Status.GuestAccess.CredentialAvailable || hcu.Status.GuestAccess.ObserverUsability != "Unknown" {
		t.Fatalf("unexpected access status: %+v", hcu.Status.GuestAccess)
	}
	if g.tokens != 1 || g.writes != 4 {
		t.Fatalf("bootstrap writes=%d tokens=%d", g.writes, g.tokens)
	}
	secret := accessDelivery(t, r)
	if len(secret.Data) != 1 || len(secret.OwnerReferences) != 0 || strings.Contains(string(secret.Data["kubeconfig"]), "admin-token") {
		t.Fatal("delivery contains admin material or invalid ownership")
	}
	if secret.Annotations[accessPrefix+"expires-at"] != g.expires.Format(time.RFC3339) {
		t.Fatal("did not honor returned expiry")
	}
	role := g.objects["/apis/rbac.authorization.k8s.io/v1/clusterroles/"+GuestObserverName]
	rules := role["rules"].([]interface{})
	if len(rules) != 4 || accessHash(rules) != accessHash(observerProfile(&accessSource{hc: &unstructured.Unstructured{}}, GuestObserverNamespace)[2].object.Object["rules"]) {
		t.Fatalf("unexpected role: %v", role)
	}
	expected := guest.Release{Version: "4.18.1", Image: "quay.io/ocp/release:4.18.1-x86_64@sha256:" + strings.Repeat("a", 64)}
	target, reader, err := r.BuildTargetAndReader(context.Background(), key, expected)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.ValidateTarget(context.Background(), target); err != nil {
		t.Fatal(err)
	}
	g.denyObserver = true
	_, err = reader.Collect(context.Background())
	if f, ok := err.(*guest.Failure); !ok || f.State != guest.AccessDenied {
		t.Fatalf("expected observer denial, got %v", err)
	}
	adminReads := g.adminReads
	if g.observerReads != 1 {
		t.Fatalf("observer requests %d", g.observerReads)
	}
	// A second scoped failure never retries with admin.
	_, _ = reader.Collect(context.Background())
	if g.adminReads != adminReads {
		t.Fatal("admin observation fallback")
	}
	clock.now = clock.now.Add(31 * time.Minute)
	pauseAccess(t, r, key)
	g.expires = clock.now.Add(time.Hour)
	hcu = accessReconcile(t, r, key)
	if !hcu.Status.GuestAccess.MaintenanceReady || g.tokens != 2 || g.writes != 4 {
		t.Fatalf("paused renewal mutated profile or failed: %+v", hcu.Status.GuestAccess)
	}
	if err := r.ValidateTarget(context.Background(), target); err == nil {
		t.Fatal("rotation/plan edit retained target")
	}
	// Failed renewal retains the valid old Secret until its actual expiration.
	old := accessDelivery(t, r)
	clock.now = clock.now.Add(51 * time.Minute)
	g.failToken = true
	hcu = accessReconcile(t, r, key)
	if hcu.Status.GuestAccess.MaintenanceReady || !hcu.Status.GuestAccess.CredentialAvailable {
		t.Fatalf("valid old credential lost: %+v", hcu.Status.GuestAccess)
	}
	if accessDelivery(t, r).ResourceVersion != old.ResourceVersion {
		t.Fatal("renewal failure overwrote delivery")
	}
	clock.now = clock.now.Add(10 * time.Minute)
	hcu = accessReconcile(t, r, key)
	if hcu.Status.GuestAccess.CredentialAvailable {
		t.Fatal("expired credential reported available")
	}
}

func TestGuestAccessPauseAndProfileOwnership(t *testing.T) {
	for _, scenario := range []string{"initial-pause", "unowned-profile", "active-repair", "active-binding-repair", "paused-binding-drift", "paused-drift", "paused-missing-sa", "paused-missing-secret"} {
		t.Run(scenario, func(t *testing.T) {
			clock := &accessTestClock{now: time.Now().UTC().Truncate(time.Second)}
			g := newAccessGuest(t, clock.now.Add(time.Hour))
			r, key := newAccessManagement(t, guestFixtureConfig(g), clock)
			if scenario == "initial-pause" {
				pauseAccess(t, r, key)
				hcu := accessReconcile(t, r, key)
				if hcu.Status.GuestAccess.MaintenanceReady || g.writes != 0 || g.tokens != 0 {
					t.Fatal("paused bootstrap")
				}
				return
			}
			if scenario == "unowned-profile" {
				g.objects["/api/v1/namespaces/"+GuestObserverNamespace] = map[string]interface{}{"apiVersion": "v1", "kind": "Namespace", "metadata": map[string]interface{}{"name": GuestObserverNamespace, "uid": "user-uid"}}
				hcu := accessReconcile(t, r, key)
				if hcu.Status.GuestAccess.Reason != "ProfileOwnershipConflict" || g.writes != 0 {
					t.Fatalf("adopted collision: %+v", hcu.Status.GuestAccess)
				}
				return
			}
			accessReconcile(t, r, key)
			clock.now = clock.now.Add(51 * time.Minute)
			if scenario != "active-repair" && scenario != "active-binding-repair" {
				pauseAccess(t, r, key)
			}
			switch scenario {
			case "active-repair", "paused-drift":
				g.objects["/apis/rbac.authorization.k8s.io/v1/clusterroles/"+GuestObserverName]["rules"] = []interface{}{}
			case "active-binding-repair", "paused-binding-drift":
				g.objects["/apis/rbac.authorization.k8s.io/v1/clusterrolebindings/"+GuestObserverName]["roleRef"].(map[string]interface{})["name"] = "wrong-role"
			case "paused-missing-sa":
				delete(g.objects, "/api/v1/namespaces/"+GuestObserverNamespace+"/serviceaccounts/"+GuestObserverName)
			case "paused-missing-secret":
				if err := r.Client.Delete(context.Background(), accessDelivery(t, r)); err != nil {
					t.Fatal(err)
				}
			}
			hcu := accessReconcile(t, r, key)
			if scenario == "active-repair" || scenario == "active-binding-repair" {
				writes := 5
				if scenario == "active-binding-repair" {
					writes = 6
				}
				if !hcu.Status.GuestAccess.MaintenanceReady || g.writes != writes || g.tokens != 2 {
					t.Fatal("owned active drift was not repaired")
				}
			} else if hcu.Status.GuestAccess.MaintenanceReady || g.writes != 4 || g.tokens != 1 {
				t.Fatalf("paused writes: %+v writes=%d tokens=%d", hcu.Status.GuestAccess, g.writes, g.tokens)
			}
		})
	}
}

func TestGuestAccessPublicationFences(t *testing.T) {
	for _, race := range []string{"hc-replaced", "hcu-replaced", "hcu-removed", "paused-secret-deleted", "paused-secret-conflict", "pause-during-bootstrap", "sa-replaced", "wrong-cluster", "invalid-expiry"} {
		t.Run(race, func(t *testing.T) {
			clock := &accessTestClock{now: time.Now().UTC().Truncate(time.Second)}
			g := newAccessGuest(t, clock.now.Add(time.Hour))
			r, key := newAccessManagement(t, guestFixtureConfig(g), clock)
			paused := strings.HasPrefix(race, "paused-")
			if paused {
				accessReconcile(t, r, key)
				pauseAccess(t, r, key)
				clock.now = clock.now.Add(51 * time.Minute)
			}
			g.onToken = func() {
				ctx := context.Background()
				switch race {
				case "hc-replaced":
					hc := &unstructured.Unstructured{}
					hc.SetGroupVersionKind(gvkHC)
					if err := r.Client.Get(ctx, key, hc); err != nil {
						panic(err)
					}
					hc.SetUID("replacement")
					if err := r.Client.Update(ctx, hc); err != nil {
						panic(err)
					}
				case "hcu-replaced", "hcu-removed":
					hcu := &upgradev1alpha1.HostedClusterUpgrade{}
					if err := r.Client.Get(ctx, key, hcu); err != nil {
						panic(err)
					}
					if race == "hcu-removed" {
						if err := r.Client.Delete(ctx, hcu); err != nil {
							panic(err)
						}
					} else {
						hcu.UID = "replacement"
						if err := r.Client.Update(ctx, hcu); err != nil {
							panic(err)
						}
					}
				case "paused-secret-deleted":
					if err := r.Client.Delete(ctx, accessDelivery(t, r)); err != nil {
						panic(err)
					}
				case "paused-secret-conflict":
					secret := accessDelivery(t, r)
					secret.Annotations["concurrent"] = "write"
					if err := r.Client.Update(ctx, secret); err != nil {
						panic(err)
					}
				case "pause-during-bootstrap":
					pauseAccess(t, r, key)
				case "sa-replaced":
					obj := g.objects["/api/v1/namespaces/"+GuestObserverNamespace+"/serviceaccounts/"+GuestObserverName]
					obj["metadata"].(map[string]interface{})["uid"] = "replacement"
				}
			}
			if race == "wrong-cluster" {
				g.clusterID = "different"
			}
			if race == "invalid-expiry" {
				g.expires = clock.now
			}
			_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key})
			if err != nil {
				t.Fatal(err)
			}
			secret := &corev1.Secret{}
			err = r.APIReader.Get(context.Background(), client.ObjectKey{Namespace: r.OperatorNamespace, Name: deliveryName("hc-uid")}, secret)
			if paused && race != "paused-secret-deleted" {
				if err != nil || secret.Annotations[accessPrefix+"expires-at"] != clock.now.Add(9*time.Minute).Format(time.RFC3339) {
					t.Fatal("stale token replaced old credential")
				}
			} else if !apierrors.IsNotFound(err) {
				t.Fatalf("stale publication occurred: %v", err)
			}
			if race == "wrong-cluster" && (g.writes != 0 || g.tokens != 0) {
				t.Fatal("guest writes before cluster identity validation")
			}
		})
	}
}

func TestGuestAccessSafeAdminConfig(t *testing.T) {
	g := newAccessGuest(t, time.Now().Add(time.Hour))
	base := accessKubeconfig(t, guestFixtureConfig(g), "admin")
	mutations := map[string]func(*clientcmdapi.Config){
		"exec": func(k *clientcmdapi.Config) { k.AuthInfos["observer"].Exec = &clientcmdapi.ExecConfig{Command: "bad"} },
		"auth-provider": func(k *clientcmdapi.Config) {
			k.AuthInfos["observer"].AuthProvider = &clientcmdapi.AuthProviderConfig{Name: "bad"}
		},
		"token-file":         func(k *clientcmdapi.Config) { k.AuthInfos["observer"].TokenFile = "/bad" },
		"cert-file":          func(k *clientcmdapi.Config) { k.AuthInfos["observer"].ClientCertificate = "/bad" },
		"key-file":           func(k *clientcmdapi.Config) { k.AuthInfos["observer"].ClientKey = "/bad" },
		"ca-file":            func(k *clientcmdapi.Config) { k.Clusters["guest"].CertificateAuthority = "/bad" },
		"insecure":           func(k *clientcmdapi.Config) { k.Clusters["guest"].InsecureSkipTLSVerify = true },
		"proxy":              func(k *clientcmdapi.Config) { k.Clusters["guest"].ProxyURL = "https://bad" },
		"impersonate":        func(k *clientcmdapi.Config) { k.AuthInfos["observer"].Impersonate = "admin" },
		"impersonate-uid":    func(k *clientcmdapi.Config) { k.AuthInfos["observer"].ImpersonateUID = "admin" },
		"impersonate-groups": func(k *clientcmdapi.Config) { k.AuthInfos["observer"].ImpersonateGroups = []string{"system:masters"} },
		"impersonate-extra": func(k *clientcmdapi.Config) {
			k.AuthInfos["observer"].ImpersonateUserExtra = map[string][]string{"x": {"y"}}
		},
		"http": func(k *clientcmdapi.Config) { k.Clusters["guest"].Server = "http://bad" },
		"unused-plugin": func(k *clientcmdapi.Config) {
			k.AuthInfos["unused"] = &clientcmdapi.AuthInfo{Exec: &clientcmdapi.ExecConfig{Command: "bad"}}
		},
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			k, err := clientcmd.Load(base)
			if err != nil {
				t.Fatal(err)
			}
			mutate(k)
			b, err := clientcmd.Write(*k)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := SafeAdminConfig(b); err == nil {
				t.Fatal("unsafe admin accepted")
			}
		})
	}
	k, err := clientcmd.Load(base)
	if err != nil {
		t.Fatal(err)
	}
	k.AuthInfos["observer"] = &clientcmdapi.AuthInfo{ClientCertificateData: []byte("embedded-cert"), ClientKeyData: []byte("embedded-key")}
	b, err := clientcmd.Write(*k)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := SafeAdminConfig(b); err != nil {
		t.Fatal("embedded admin certificate rejected", err)
	}
	if _, err := guest.ScopedConfig(b); err == nil {
		t.Fatal("admin certificate accepted as observer")
	}
}

func TestGuestAccessInternalEndpointTLS(t *testing.T) {
	const dns = "kube-apiserver.clusters-example.svc"
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: dns}, DNSNames: []string{dns}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, IsCA: true, BasicConstraintsValid: true}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert := tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
	externalTemplate := *template
	externalTemplate.SerialNumber = big.NewInt(2)
	externalTemplate.DNSNames = []string{"published.example"}
	externalDER, err := x509.CreateCertificate(rand.Reader, &externalTemplate, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	externalCert := tls.Certificate{Certificate: [][]byte{externalDER}, PrivateKey: key}
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.TLS.ServerName != dns {
			t.Error("incorrect SNI")
		}
		_ = json.NewEncoder(w).Encode(configv1.ClusterVersion{ObjectMeta: metav1.ObjectMeta{Name: "version", UID: "cv"}})
	}))
	// Model HyperShift's private named certificate alongside the default external
	// certificate. Correct Service SNI must select the private serving certificate.
	server.TLS = &tls.Config{Certificates: []tls.Certificate{externalCert}, MinVersion: tls.VersionTLS12, GetCertificate: func(hello *tls.ClientHelloInfo) (*tls.Certificate, error) {
		if hello.ServerName == dns {
			return &cert, nil
		}
		return &externalCert, nil
	}}
	server.StartTLS()
	defer server.Close()
	clock := &accessTestClock{now: time.Now()}
	c := &rest.Config{Host: "https://published.invalid", TLSClientConfig: rest.TLSClientConfig{CAData: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), ServerName: "external.invalid"}}
	r, sourceKey := newAccessManagement(t, c, clock)
	r.EndpointMode = GuestEndpointInternal
	hcp := &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": gvkHCP.GroupVersion().String(), "kind": "HostedControlPlane", "metadata": map[string]interface{}{"name": "example", "namespace": "clusters-example", "uid": "hcp-uid", "annotations": map[string]interface{}{"hypershift.openshift.io/cluster": "clusters/example"}}, "spec": map[string]interface{}{"clusterID": "a5e99fb2-0598-4b34-9081-14236b744c16"}}}
	controller := true
	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "kube-apiserver", Namespace: "clusters-example", UID: "svc-uid", OwnerReferences: []metav1.OwnerReference{{APIVersion: gvkHCP.GroupVersion().String(), Kind: "HostedControlPlane", Name: "example", UID: "hcp-uid", Controller: &controller}}}, Spec: corev1.ServiceSpec{Ports: []corev1.ServicePort{{Name: "https", Port: 7443, TargetPort: intstr.FromInt32(6443), Protocol: corev1.ProtocolTCP}}}}
	existingHCP := &unstructured.Unstructured{}
	existingHCP.SetGroupVersionKind(gvkHCP)
	if err := r.APIReader.Get(context.Background(), client.ObjectKeyFromObject(hcp), existingHCP); err != nil {
		t.Fatal(err)
	}
	hcp.SetResourceVersion(existingHCP.GetResourceVersion())
	hcp.SetGeneration(existingHCP.GetGeneration())
	if err := r.Client.Update(context.Background(), hcp); err != nil {
		t.Fatal(err)
	}
	if err := r.Client.Create(context.Background(), svc); err != nil {
		t.Fatal(err)
	}
	s, err := r.source(context.Background(), sourceKey)
	if err != nil {
		t.Fatal(err)
	}
	resolved, _, err := r.endpoint(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Host != "https://"+dns+":7443" || resolved.ServerName != dns {
		t.Fatalf("wrong endpoint %s %s", resolved.Host, resolved.ServerName)
	}
	resolved.Dial = func(ctx context.Context, network, address string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, server.Listener.Addr().String())
	}
	for _, scenario := range []string{"valid", "wrong-san", "wrong-ca"} {
		t.Run(scenario, func(t *testing.T) {
			cfg := rest.CopyConfig(resolved)
			if scenario == "wrong-san" {
				cfg.ServerName = "other.invalid"
			}
			if scenario == "wrong-ca" {
				cfg.CAData = []byte("invalid")
			}
			h, err := rest.HTTPClientFor(cfg)
			if err == nil {
				defer h.CloseIdleConnections()
				_, err = adminClusterVersion(context.Background(), cfg, h)
			}
			if (scenario == "valid") != (err == nil) {
				t.Fatalf("TLS outcome %v", err)
			}
		})
	}
	svc.Spec.Ports = append(svc.Spec.Ports, corev1.ServicePort{Port: 8443, Protocol: corev1.ProtocolTCP})
	if err := r.Client.Update(context.Background(), svc); err != nil {
		t.Fatal(err)
	}
	if _, _, err := r.endpoint(context.Background(), s); err == nil {
		t.Fatal("ambiguous port accepted")
	}
	svc.Spec.Ports = svc.Spec.Ports[:1]
	svc.OwnerReferences[0].UID = "replacement"
	if err := r.Client.Update(context.Background(), svc); err != nil {
		t.Fatal(err)
	}
	if _, _, err := r.endpoint(context.Background(), s); err == nil {
		t.Fatal("wrong HCP association accepted")
	}
}

func TestGuestAccessCleanupPreconditionsAndStatusRebase(t *testing.T) {
	clock := &accessTestClock{now: time.Now().UTC().Truncate(time.Second)}
	g := newAccessGuest(t, clock.now.Add(time.Hour))
	r, key := newAccessManagement(t, guestFixtureConfig(g), clock)
	hcu := accessReconcile(t, r, key)
	secret := accessDelivery(t, r)
	if err := r.cleanup(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	if accessDelivery(t, r).UID != secret.UID {
		t.Fatal("cleaned enrolled credential")
	}
	base := r.Client
	conflicts := 0
	r.Client = interceptor.NewClient(base.(client.WithWatch), interceptor.Funcs{SubResourcePatch: func(ctx context.Context, c client.Client, sub string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
		if conflicts == 0 {
			conflicts++
			fresh := &upgradev1alpha1.HostedClusterUpgrade{}
			if err := c.Get(ctx, key, fresh); err != nil {
				return err
			}
			fresh.Status.Message = "upgrade writer"
			fresh.Status.Phase = upgradev1alpha1.PhaseUpgradingNodePools
			fresh.Status.Conditions = append(fresh.Status.Conditions, metav1.Condition{Type: "Ready", Status: metav1.ConditionFalse, Reason: "InFlight", Message: "upgrade", LastTransitionTime: metav1.Now()})
			if err := c.Status().Update(ctx, fresh); err != nil {
				return err
			}
			return apierrors.NewConflict(schema.GroupResource{Group: "upgrade.0xshift.dev", Resource: "hostedclusterupgrades"}, key.Name, fmt.Errorf("conflict"))
		}
		return c.SubResource(sub).Patch(ctx, obj, patch, opts...)
	}})
	s, err := r.source(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	hcu.Status.GuestAccess.Reason = "RenewalPending"
	if err := r.persistAccessStatus(context.Background(), s, hcu.Status.GuestAccess); err != nil {
		t.Fatal(err)
	}
	if err := base.Get(context.Background(), key, hcu); err != nil {
		t.Fatal(err)
	}
	if hcu.Status.Message != "upgrade writer" || hcu.Status.Phase != upgradev1alpha1.PhaseUpgradingNodePools {
		t.Fatal("access writer lost upgrade status")
	}
	if g.tokens != 1 {
		t.Fatal("status retry replayed issuance")
	}
	if err := base.Delete(context.Background(), hcu); err != nil {
		t.Fatal(err)
	}
	checked := false
	r.Client = interceptor.NewClient(base.(client.WithWatch), interceptor.Funcs{Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
		o := &client.DeleteOptions{}
		for _, v := range opts {
			v.ApplyToDelete(o)
		}
		if o.Preconditions == nil || o.Preconditions.UID == nil || *o.Preconditions.UID != secret.UID || o.Preconditions.ResourceVersion == nil || *o.Preconditions.ResourceVersion != secret.ResourceVersion {
			t.Fatal("missing UID/RV delete preconditions")
		}
		checked = true
		// Simulate a replacement after the uncached read, before Delete reaches API.
		if err := c.Delete(ctx, obj); err != nil {
			return err
		}
		replacement := secret.DeepCopy()
		replacement.ResourceVersion = ""
		replacement.UID = "replacement"
		if err := c.Create(ctx, replacement); err != nil {
			return err
		}
		return apierrors.NewConflict(schema.GroupResource{Resource: "secrets"}, obj.GetName(), fmt.Errorf("replacement"))
	}})
	if err := r.cleanup(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	if !checked || accessDelivery(t, r).UID != "replacement" {
		t.Fatal("cleanup deleted replacement")
	}
}

func TestGuestAccessTargetInvalidation(t *testing.T) {
	for _, change := range []string{"generation", "plan", "hc-uid", "hcu-uid", "cluster-id", "hc-release", "secret-uid", "secret-revision", "endpoint-revision", "expiry"} {
		t.Run(change, func(t *testing.T) {
			clock := &accessTestClock{now: time.Now().UTC().Truncate(time.Second)}
			g := newAccessGuest(t, clock.now.Add(time.Hour))
			r, key := newAccessManagement(t, guestFixtureConfig(g), clock)
			accessReconcile(t, r, key)
			target, _, err := r.BuildTargetAndReader(context.Background(), key, guest.Release{Version: "4.18.1", Image: "quay.io/ocp/release:4.18.1-x86_64@sha256:" + strings.Repeat("a", 64)})
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			switch change {
			case "generation", "plan", "hcu-uid":
				hcu := &upgradev1alpha1.HostedClusterUpgrade{}
				if err := r.Client.Get(ctx, key, hcu); err != nil {
					t.Fatal(err)
				}
				if change == "generation" {
					hcu.Generation++
				} else if change == "plan" {
					hcu.Spec.TargetReleaseImage = "different"
				} else {
					hcu.UID = "replacement"
				}
				if err := r.Client.Update(ctx, hcu); err != nil {
					t.Fatal(err)
				}
			case "hc-uid", "cluster-id", "hc-release":
				hc := &unstructured.Unstructured{}
				hc.SetGroupVersionKind(gvkHC)
				if err := r.Client.Get(ctx, key, hc); err != nil {
					t.Fatal(err)
				}
				if change == "hc-uid" {
					hc.SetUID("replacement")
				} else if change == "cluster-id" {
					_ = unstructured.SetNestedField(hc.Object, "replacement", "spec", "clusterID")
				} else {
					_ = unstructured.SetNestedField(hc.Object, "replacement", "spec", "release", "image")
				}
				if err := r.Client.Update(ctx, hc); err != nil {
					t.Fatal(err)
				}
			case "secret-uid", "secret-revision":
				secret := accessDelivery(t, r)
				if change == "secret-uid" {
					secret.UID = "replacement"
				} else {
					secret.Annotations["concurrent"] = "edit"
				}
				if err := r.Client.Update(ctx, secret); err != nil {
					t.Fatal(err)
				}
			case "endpoint-revision":
				secret := &corev1.Secret{}
				if err := r.Client.Get(ctx, client.ObjectKey{Namespace: key.Namespace, Name: "admin"}, secret); err != nil {
					t.Fatal(err)
				}
				secret.Annotations = map[string]string{"concurrent": "edit"}
				if err := r.Client.Update(ctx, secret); err != nil {
					t.Fatal(err)
				}
			case "expiry":
				clock.now = clock.now.Add(time.Hour)
			}
			if err := r.ValidateTarget(ctx, target); err == nil {
				t.Fatal("changed identity retained authorization")
			}
		})
	}
}

func TestGuestAccessPausedPublicationNoCreateFallback(t *testing.T) {
	for _, notFound := range []bool{false, true} {
		t.Run(fmt.Sprint("notfound=", notFound), func(t *testing.T) {
			clock := &accessTestClock{now: time.Now().UTC().Truncate(time.Second)}
			g := newAccessGuest(t, clock.now.Add(time.Hour))
			r, key := newAccessManagement(t, guestFixtureConfig(g), clock)
			accessReconcile(t, r, key)
			pauseAccess(t, r, key)
			clock.now = clock.now.Add(51 * time.Minute)
			g.expires = clock.now.Add(time.Hour)
			base := r.Client
			creates, updates := 0, 0
			r.Client = interceptor.NewClient(base.(client.WithWatch), interceptor.Funcs{
				Create: func(context.Context, client.WithWatch, client.Object, ...client.CreateOption) error {
					creates++
					return fmt.Errorf("forbidden create")
				},
				Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
					if secret, ok := obj.(*corev1.Secret); ok && secret.Namespace == r.OperatorNamespace {
						updates++
						if notFound {
							if err := c.Delete(ctx, obj); err != nil {
								return err
							}
							return apierrors.NewNotFound(schema.GroupResource{Resource: "secrets"}, obj.GetName())
						}
						return apierrors.NewConflict(schema.GroupResource{Resource: "secrets"}, obj.GetName(), fmt.Errorf("conflict"))
					}
					return c.Update(ctx, obj, opts...)
				},
			})
			hcu := accessReconcile(t, r, key)
			if creates != 0 || updates != 1 || hcu.Status.GuestAccess.MaintenanceReady {
				t.Fatalf("paused publication fallback: creates=%d updates=%d status=%+v", creates, updates, hcu.Status.GuestAccess)
			}
		})
	}
}

func TestGuestAccessPolicyClockIsNotNetworkDeadline(t *testing.T) {
	clock := &accessTestClock{now: time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)}
	g := newAccessGuest(t, clock.now.Add(5*time.Minute))
	r, key := newAccessManagement(t, guestFixtureConfig(g), clock)
	hcu := accessReconcile(t, r, key)
	if !hcu.Status.GuestAccess.MaintenanceReady {
		t.Fatalf("fake historical time broke real network: %+v", hcu.Status.GuestAccess)
	}
	// A shortened returned lifetime must not cause a one-second renewal loop.
	s, err := r.source(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	delay, err := r.maintain(context.Background(), s)
	if err != nil || delay < 2*time.Minute || delay >= 5*time.Minute {
		t.Fatalf("short lifetime renewal delay=%v error=%v", delay, err)
	}
}

func TestGuestAccessLostTokenResponseRetainsCredential(t *testing.T) {
	clock := &accessTestClock{now: time.Now().UTC().Truncate(time.Second)}
	g := newAccessGuest(t, clock.now.Add(time.Hour))
	r, key := newAccessManagement(t, guestFixtureConfig(g), clock)
	accessReconcile(t, r, key)
	old := accessDelivery(t, r)
	clock.now = clock.now.Add(51 * time.Minute)
	g.loseToken = true
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatal(err)
	}
	if accessDelivery(t, r).ResourceVersion != old.ResourceVersion {
		t.Fatal("lost issuer response overwrote valid old credential")
	}
	if _, _, err := r.BuildTargetAndReader(context.Background(), key, guest.Release{Version: "4.18.1", Image: "quay.io/ocp/release:4.18.1-x86_64@sha256:" + strings.Repeat("a", 64)}); err != nil {
		t.Fatal("old credential unavailable after response loss", err)
	}
}

func TestGuestAccessNetworkCancellation(t *testing.T) {
	clock := &accessTestClock{now: time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	defer server.Close()
	c := &rest.Config{Host: server.URL, TLSClientConfig: rest.TLSClientConfig{CAData: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})}}
	r, key := newAccessManagement(t, c, clock)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	started := time.Now()
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatal(err)
	}
	if time.Since(started) > time.Second {
		t.Fatal("administrative request ignored real cancellation")
	}
	if err := r.APIReader.Get(context.Background(), client.ObjectKey{Namespace: r.OperatorNamespace, Name: deliveryName("hc-uid")}, &corev1.Secret{}); !apierrors.IsNotFound(err) {
		t.Fatal("published credential after identity request timeout")
	}
}

func TestGuestAccessDeliveryCollision(t *testing.T) {
	clock := &accessTestClock{now: time.Now().UTC().Truncate(time.Second)}
	g := newAccessGuest(t, clock.now.Add(time.Hour))
	r, key := newAccessManagement(t, guestFixtureConfig(g), clock)
	userSecret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: deliveryName("hc-uid"), Namespace: r.OperatorNamespace}, Data: map[string][]byte{"user": []byte("preserve")}}
	if err := r.Client.Create(context.Background(), userSecret); err != nil {
		t.Fatal(err)
	}
	hcu := accessReconcile(t, r, key)
	if hcu.Status.GuestAccess.Reason != "DeliveryOwnershipConflict" || g.writes != 0 || g.tokens != 0 || string(accessDelivery(t, r).Data["user"]) != "preserve" {
		t.Fatal("adopted or overwrote unowned delivery collision")
	}
}

func TestGuestAccessCompletedMaintenanceAndRemoval(t *testing.T) {
	clock := &accessTestClock{now: time.Now().UTC().Truncate(time.Second)}
	g := newAccessGuest(t, clock.now.Add(time.Hour))
	r, key := newAccessManagement(t, guestFixtureConfig(g), clock)
	hcu := accessReconcile(t, r, key)
	hcu.Status.Phase = upgradev1alpha1.PhaseCompleted
	if err := r.Client.Status().Update(context.Background(), hcu); err != nil {
		t.Fatal(err)
	}
	clock.now = clock.now.Add(51 * time.Minute)
	g.expires = clock.now.Add(time.Hour)
	hcu = accessReconcile(t, r, key)
	if g.tokens != 2 || hcu.Status.Phase != upgradev1alpha1.PhaseCompleted || !hcu.Status.GuestAccess.MaintenanceReady {
		t.Fatal("Completed maintenance stopped or reopened upgrade")
	}
	if err := r.Client.Delete(context.Background(), hcu); err != nil {
		t.Fatal(err)
	}
	adminReads := g.adminReads
	g.failToken = true
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatal(err)
	}
	if g.adminReads != adminReads || g.tokens != 2 {
		t.Fatal("unenrollment cleanup contacted unavailable guest")
	}
	if err := r.APIReader.Get(context.Background(), client.ObjectKey{Namespace: r.OperatorNamespace, Name: deliveryName("hc-uid")}, &corev1.Secret{}); !apierrors.IsNotFound(err) {
		t.Fatal("unenrollment retained delivery credential")
	}
}
