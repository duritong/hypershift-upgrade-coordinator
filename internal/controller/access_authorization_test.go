package controller

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/duritong/hypershift-upgrade-coordinator/internal/guest"
	authv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

// TestGuestAccessRealAuthorization uses a real API server with RBAC and envtest's
// service-account signing keys/issuer. Missing API assets fail setup, never skip.
func TestGuestAccessRealAuthorization(t *testing.T) {
	for _, namespace := range []string{GuestObserverNamespace, "custom-guest-observer"} {
		t.Run(namespace, func(t *testing.T) { testGuestAccessRealAuthorization(t, namespace) })
	}
}

func testGuestAccessRealAuthorization(t *testing.T, observerNamespace string) {
	assets := os.Getenv("KUBEBUILDER_ASSETS")
	if assets == "" {
		assets = filepath.Join("..", "..", "bin", "k8s", "1.32.0-linux-amd64")
	}
	preserve := true
	var crds []*apiextensionsv1.CustomResourceDefinition
	for _, kind := range []struct{ singular, plural, kind string }{{"clusterversion", "clusterversions", "ClusterVersion"}, {"clusteroperator", "clusteroperators", "ClusterOperator"}} {
		crds = append(crds, &apiextensionsv1.CustomResourceDefinition{ObjectMeta: metav1.ObjectMeta{Name: kind.plural + ".config.openshift.io"}, Spec: apiextensionsv1.CustomResourceDefinitionSpec{Group: "config.openshift.io", Scope: apiextensionsv1.ClusterScoped, Names: apiextensionsv1.CustomResourceDefinitionNames{Singular: kind.singular, Plural: kind.plural, Kind: kind.kind, ListKind: kind.kind + "List"}, Versions: []apiextensionsv1.CustomResourceDefinitionVersion{{Name: "v1", Served: true, Storage: true, Schema: &apiextensionsv1.CustomResourceValidation{OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{Type: "object", XPreserveUnknownFields: &preserve}}}}}})
	}
	for _, kind := range []struct{ group, version, singular, plural, kind string }{
		{"hco.kubevirt.io", "v1beta1", "hyperconverged", "hyperconvergeds", "HyperConverged"},
		{"kubevirt.io", "v1", "kubevirt", "kubevirts", "KubeVirt"},
	} {
		crds = append(crds, &apiextensionsv1.CustomResourceDefinition{ObjectMeta: metav1.ObjectMeta{Name: kind.plural + "." + kind.group}, Spec: apiextensionsv1.CustomResourceDefinitionSpec{Group: kind.group, Scope: apiextensionsv1.NamespaceScoped, Names: apiextensionsv1.CustomResourceDefinitionNames{Singular: kind.singular, Plural: kind.plural, Kind: kind.kind, ListKind: kind.kind + "List"}, Versions: []apiextensionsv1.CustomResourceDefinitionVersion{{Name: kind.version, Served: true, Storage: true, Schema: &apiextensionsv1.CustomResourceValidation{OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{Type: "object", XPreserveUnknownFields: &preserve}}}}}})
	}
	env := &envtest.Environment{BinaryAssetsDirectory: assets, CRDs: crds}
	c, err := env.Start()
	if err != nil {
		t.Fatal("guest API setup failed:", err)
	}
	t.Cleanup(func() {
		if err := env.Stop(); err != nil {
			t.Error(err)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	admin, err := dynamic.NewForConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	cvResource := schema.GroupVersionResource{Group: "config.openshift.io", Version: "v1", Resource: "clusterversions"}
	coResource := cvResource
	coResource.Resource = "clusteroperators"
	cv, err := admin.Resource(cvResource).Create(ctx, &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": "config.openshift.io/v1", "kind": "ClusterVersion", "metadata": map[string]interface{}{"name": "version"}, "spec": map[string]interface{}{"clusterID": "a5e99fb2-0598-4b34-9081-14236b744c16"}, "status": map[string]interface{}{"conditions": []interface{}{map[string]interface{}{"type": "Failing", "status": "True"}}}}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Resource(coResource).Create(ctx, &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": "config.openshift.io/v1", "kind": "ClusterOperator", "metadata": map[string]interface{}{"name": "monitoring"}}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	clock := &accessTestClock{now: time.Now().UTC().Truncate(time.Second)}
	r, key := newAccessManagement(t, c, clock)
	r.ObserverNamespace = observerNamespace
	// Unlike observerKubeconfig, published admin kubeconfig embeds the envtest cert.
	k := clientcmdapi.NewConfig()
	k.Clusters["guest"] = &clientcmdapi.Cluster{Server: c.Host, CertificateAuthorityData: c.CAData}
	k.AuthInfos["admin"] = &clientcmdapi.AuthInfo{ClientCertificateData: c.CertData, ClientKeyData: c.KeyData}
	k.Contexts["admin"] = &clientcmdapi.Context{Cluster: "guest", AuthInfo: "admin"}
	k.CurrentContext = "admin"
	data, err := clientcmd.Write(*k)
	if err != nil {
		t.Fatal(err)
	}
	sourceSecret := &corev1.Secret{}
	if err := r.APIReader.Get(ctx, client.ObjectKey{Namespace: key.Namespace, Name: "admin"}, sourceSecret); err != nil {
		t.Fatal(err)
	}
	sourceSecret.Data["kubeconfig"] = data
	if err := r.Client.Update(ctx, sourceSecret); err != nil {
		t.Fatal(err)
	}
	hcu := accessReconcile(t, r, key)
	if !hcu.Status.GuestAccess.MaintenanceReady {
		t.Fatalf("real bootstrap failed: %+v", hcu.Status.GuestAccess)
	}
	delivery := accessDelivery(t, r)
	if delivery.Annotations[accessPrefix+"cv-uid"] != string(cv.GetUID()) {
		t.Fatal("wrong discovered CV UID")
	}
	scoped, err := guest.ScopedConfig(delivery.Data["kubeconfig"])
	if err != nil {
		t.Fatal(err)
	}
	observer, err := dynamic.NewForConfig(scoped)
	if err != nil {
		t.Fatal(err)
	}
	// Authorization caches propagate asynchronously; this wait is bounded by real
	// wall time, not the policy clock or an arbitrary health dwell interval.
	waitGuestCVPermission(t, ctx, observer, cvResource, true)
	operators, err := observer.Resource(coResource).List(ctx, metav1.ListOptions{})
	if err != nil || len(operators.Items) != 1 {
		t.Fatalf("observer CO list failed: %v", err)
	}
	for _, gvr := range []schema.GroupVersionResource{{Group: "hco.kubevirt.io", Version: "v1beta1", Resource: "hyperconvergeds"}, {Group: "kubevirt.io", Version: "v1", Resource: "kubevirts"}} {
		resource := observer.Resource(gvr)
		if _, err := resource.List(ctx, metav1.ListOptions{}); err != nil {
			t.Fatalf("observer %s list denied: %v", gvr.Resource, err)
		}
		kind := "HyperConverged"
		if gvr.Resource == "kubevirts" {
			kind = "KubeVirt"
		}
		obj, err := admin.Resource(gvr).Namespace(observerNamespace).Create(ctx, &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": gvr.Group + "/" + gvr.Version, "kind": kind, "metadata": map[string]interface{}{"name": "example", "namespace": observerNamespace}}}, metav1.CreateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if list, err := resource.List(ctx, metav1.ListOptions{}); err != nil || len(list.Items) != 1 {
			t.Fatalf("installed resource unreadable: %v", err)
		}
		for verb, request := range map[string]func() error{
			"get": func() error {
				_, err := resource.Namespace(observerNamespace).Get(ctx, "example", metav1.GetOptions{})
				return err
			},
			"create": func() error {
				_, err := resource.Namespace(observerNamespace).Create(ctx, obj.DeepCopy(), metav1.CreateOptions{})
				return err
			},
			"update": func() error {
				_, err := resource.Namespace(observerNamespace).Update(ctx, obj.DeepCopy(), metav1.UpdateOptions{})
				return err
			},
			"delete": func() error {
				return resource.Namespace(observerNamespace).Delete(ctx, "example", metav1.DeleteOptions{})
			},
			"patch": func() error {
				_, err := resource.Namespace(observerNamespace).Patch(ctx, "example", types.MergePatchType, []byte(`{"spec":{}}`), metav1.PatchOptions{})
				return err
			},
		} {
			t.Run(gvr.Resource+"-deny-"+verb, func(t *testing.T) {
				if err := request(); !apierrors.IsForbidden(err) {
					t.Fatalf("expected Forbidden, got %v", err)
				}
			})
		}
	}
	for name, request := range map[string]func() error{
		"list-cv": func() error { _, err := observer.Resource(cvResource).List(ctx, metav1.ListOptions{}); return err },
		"get-other-cv": func() error {
			_, err := observer.Resource(cvResource).Get(ctx, "other", metav1.GetOptions{})
			return err
		},
		"get-co": func() error {
			_, err := observer.Resource(coResource).Get(ctx, "monitoring", metav1.GetOptions{})
			return err
		},
		"read-secret": func() error {
			_, err := observer.Resource(corev1.SchemeGroupVersion.WithResource("secrets")).Namespace(observerNamespace).Get(ctx, "anything", metav1.GetOptions{})
			return err
		},
		"list-secrets": func() error {
			_, err := observer.Resource(corev1.SchemeGroupVersion.WithResource("secrets")).Namespace(observerNamespace).List(ctx, metav1.ListOptions{})
			return err
		},
		"list-pods": func() error {
			_, err := observer.Resource(corev1.SchemeGroupVersion.WithResource("pods")).Namespace(observerNamespace).List(ctx, metav1.ListOptions{})
			return err
		},
		"mutate-workload": func() error {
			_, err := observer.Resource(corev1.SchemeGroupVersion.WithResource("pods")).Namespace(observerNamespace).Create(ctx, &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": "v1", "kind": "Pod", "metadata": map[string]interface{}{"name": "forbidden"}}}, metav1.CreateOptions{})
			return err
		},
		"create-secret": func() error {
			_, err := observer.Resource(corev1.SchemeGroupVersion.WithResource("secrets")).Namespace(observerNamespace).Create(ctx, &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": "v1", "kind": "Secret", "metadata": map[string]interface{}{"name": "forbidden"}}}, metav1.CreateOptions{})
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := request(); !apierrors.IsForbidden(err) {
				t.Fatalf("expected Forbidden, got %v", err)
			}
		})
	}
	adminKube, err := kubernetes.NewForConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	review, err := adminKube.AuthenticationV1().TokenReviews().Create(ctx, &authv1.TokenReview{Spec: authv1.TokenReviewSpec{Token: scoped.BearerToken}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !review.Status.Authenticated || review.Status.User.Username != "system:serviceaccount:"+observerNamespace+":"+GuestObserverName || len(review.Status.Audiences) == 0 {
		t.Fatalf("wrong token identity or audience: %+v", review.Status)
	}
	if strings.Contains(string(delivery.Data["kubeconfig"]), "client-certificate-data") {
		t.Fatal("published admin cert")
	}
	exp, err := time.Parse(time.RFC3339, delivery.Annotations[accessPrefix+"expires-at"])
	if err != nil || !exp.After(time.Now()) || exp.After(time.Now().Add(61*time.Minute)) {
		t.Fatalf("unexpected real token expiration: %v %v", exp, err)
	}
	if observerNamespace == GuestObserverNamespace {
		r.ObserverNamespace = "migrated-guest-observer"
		hcu = accessReconcile(t, r, key)
		if !hcu.Status.GuestAccess.MaintenanceReady {
			t.Fatalf("real namespace migration failed: %+v", hcu.Status.GuestAccess)
		}
		replacement, err := guest.ScopedConfig(accessDelivery(t, r).Data["kubeconfig"])
		if err != nil {
			t.Fatal(err)
		}
		newObserver, err := dynamic.NewForConfig(replacement)
		if err != nil {
			t.Fatal(err)
		}
		waitGuestCVPermission(t, ctx, newObserver, cvResource, true)
		waitGuestCVPermission(t, ctx, observer, cvResource, false)
		review, err := adminKube.AuthenticationV1().TokenReviews().Create(ctx, &authv1.TokenReview{Spec: authv1.TokenReviewSpec{Token: replacement.BearerToken}}, metav1.CreateOptions{})
		if err != nil {
			t.Fatal("migrated token review failed:", err)
		}
		if !review.Status.Authenticated || review.Status.User.Username != "system:serviceaccount:"+r.ObserverNamespace+":"+GuestObserverName {
			t.Fatalf("wrong migrated token identity: %+v", review.Status)
		}
	}
	t.Run("management-secret-cache", func(t *testing.T) { testAccessSecretCache(t, ctx, c) })
}

func waitGuestCVPermission(t *testing.T, ctx context.Context, observer dynamic.Interface, resource schema.GroupVersionResource, allowed bool) {
	t.Helper()
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(25 * time.Millisecond)
	defer tick.Stop()
	for {
		_, err := observer.Resource(resource).Get(ctx, "version", metav1.GetOptions{})
		if (allowed && err == nil) || (!allowed && apierrors.IsForbidden(err)) {
			return
		}
		if err != nil && !apierrors.IsForbidden(err) {
			t.Fatalf("unexpected observer authentication result: %v", err)
		}
		select {
		case <-deadline.C:
			t.Fatalf("observer permission allowed=%t did not propagate", allowed)
		case <-tick.C:
		}
	}
}

type accessRoundTripper func(*http.Request) (*http.Response, error)

func (f accessRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func testAccessSecretCache(t *testing.T, ctx context.Context, adminConfig *rest.Config) {
	t.Helper()
	const operatorNamespace = "access-cache-test"
	k, err := kubernetes.NewForConfig(adminConfig)
	if err != nil {
		t.Fatal(err)
	}
	for _, ns := range []string{operatorNamespace, "access-source-test"} {
		if _, err := k.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	sa, err := k.CoreV1().ServiceAccounts(operatorNamespace).Create(ctx, &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "cache-reader"}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = k.RbacV1().Roles(operatorNamespace).Create(ctx, &rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Name: "delivery-lifecycle"}, Rules: []rbacv1.PolicyRule{{APIGroups: []string{""}, Resources: []string{"secrets"}, Verbs: []string{"get", "list", "watch", "create", "update", "patch", "delete"}}}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = k.RbacV1().RoleBindings(operatorNamespace).Create(ctx, &rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Name: "delivery-lifecycle"}, RoleRef: rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: "delivery-lifecycle"}, Subjects: []rbacv1.Subject{{Kind: "ServiceAccount", Namespace: operatorNamespace, Name: sa.Name}}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []*corev1.Secret{
		{ObjectMeta: metav1.ObjectMeta{Name: "delivery", Namespace: operatorNamespace, Labels: map[string]string{GuestAccessLabel: "true"}}},
		{ObjectMeta: metav1.ObjectMeta{Name: "admin", Namespace: operatorNamespace}},
		{ObjectMeta: metav1.ObjectMeta{Name: "other-source", Namespace: "access-source-test", Labels: map[string]string{GuestAccessLabel: "true"}}},
	} {
		if _, err := k.CoreV1().Secrets(secret.Namespace).Create(ctx, secret, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	seconds := int64(3600)
	token, err := k.CoreV1().ServiceAccounts(operatorNamespace).CreateToken(ctx, sa.Name, &authv1.TokenRequest{Spec: authv1.TokenRequestSpec{ExpirationSeconds: &seconds}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	c := rest.AnonymousClientConfig(adminConfig)
	c.BearerToken = token.Status.Token
	// Record actual informer request scope, not an inferred intended selector.
	var mu sync.Mutex
	var requests []string
	c.WrapTransport = func(rt http.RoundTripper) http.RoundTripper {
		return accessRoundTripper(func(req *http.Request) (*http.Response, error) {
			mu.Lock()
			requests = append(requests, req.URL.Path+"?"+req.URL.RawQuery)
			mu.Unlock()
			return rt.RoundTrip(req)
		})
	}
	options, err := GuestAccessCacheOptions(operatorNamespace)
	if err != nil {
		t.Fatal(err)
	}
	options.Scheme = runtime.NewScheme()
	if err := corev1.AddToScheme(options.Scheme); err != nil {
		t.Fatal(err)
	}
	secretCache, err := cache.New(c, options)
	if err != nil {
		t.Fatal(err)
	}
	cacheCtx, cancel := context.WithCancel(ctx)
	stopped := make(chan error, 1)
	go func() { stopped <- secretCache.Start(cacheCtx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-stopped:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(5 * time.Second):
			t.Error("Secret cache failed to stop")
		}
	})
	syncCtx, syncCancel := context.WithTimeout(cacheCtx, 15*time.Second)
	defer syncCancel()
	if _, err := secretCache.GetInformer(syncCtx, &corev1.Secret{}); err != nil {
		t.Fatal(err)
	}
	if !secretCache.WaitForCacheSync(syncCtx) {
		t.Fatal("namespace-scoped Secret cache did not sync")
	}
	if err := secretCache.Get(ctx, client.ObjectKey{Namespace: operatorNamespace, Name: "delivery"}, &corev1.Secret{}); err != nil {
		t.Fatal(err)
	}
	if err := secretCache.Get(ctx, client.ObjectKey{Namespace: operatorNamespace, Name: "admin"}, &corev1.Secret{}); !apierrors.IsNotFound(err) {
		t.Fatalf("unlabeled admin cached: %v", err)
	}
	list := &corev1.SecretList{}
	if err := secretCache.List(ctx, list, client.InNamespace(operatorNamespace)); err != nil || len(list.Items) != 1 {
		t.Fatalf("unexpected cached Secrets: count=%d err=%v", len(list.Items), err)
	}
	localKube, err := kubernetes.NewForConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := localKube.CoreV1().Secrets("").List(ctx, metav1.ListOptions{}); !apierrors.IsForbidden(err) {
		t.Fatalf("cache identity unexpectedly has fleet Secret list: %v", err)
	}
	// The Role is real namespace-wide authority, not a label-enforced boundary.
	if _, err := localKube.CoreV1().Secrets(operatorNamespace).Get(ctx, "admin", metav1.GetOptions{}); err != nil {
		t.Fatalf("local Role authority not namespace-wide: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	found := false
	for _, request := range requests {
		if strings.Contains(request, "labelSelector=") {
			found = true
			if !strings.HasPrefix(request, "/api/v1/namespaces/"+operatorNamespace+"/secrets?") || !strings.Contains(request, "labelSelector=upgrade.huc.0xshift.dev%2Fguest-observer%3Dtrue") {
				t.Fatalf("unsafe informer request %s", request)
			}
		}
	}
	if !found {
		t.Fatal("no real label-filtered informer requests recorded")
	}
	if _, err := GuestAccessCacheOptions(""); err == nil {
		t.Fatal("empty operator namespace enabled fleet Secret cache")
	}
}
