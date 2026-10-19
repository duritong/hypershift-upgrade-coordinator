package controller

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"reflect"
	"time"

	upgradev1alpha1 "github.com/duritong/hypershift-upgrade-coordinator/api/v1alpha1"
	"github.com/duritong/hypershift-upgrade-coordinator/internal/guest"
	configv1 "github.com/openshift/api/config/v1"
	authv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	controlleroptions "sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// GuestAccessReconciler owns only credential lifecycle and its status section.
// Network work runs on its own two-worker queue, never the upgrade queue.
type GuestAccessReconciler struct {
	GuestAccessProvider
	Client client.Client
}

// GuestAccessCacheOptions never configures a fleet-wide Secret informer. Other
// management resource caches retain the existing fleet discovery scope.
func GuestAccessCacheOptions(operatorNamespace string) (cache.Options, error) {
	if operatorNamespace == "" {
		return cache.Options{}, accessFailure("OperatorNamespaceRequired")
	}
	return cache.Options{ByObject: map[client.Object]cache.ByObject{
		&corev1.Secret{}: {Namespaces: map[string]cache.Config{operatorNamespace: {}}, Label: labels.SelectorFromSet(labels.Set{GuestAccessLabel: "true"})},
	}}, nil
}

//+kubebuilder:rbac:groups="",resources=services,verbs=get

func (r *GuestAccessReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.OperatorNamespace == "" {
		return accessFailure("OperatorNamespaceRequired")
	}
	if r.EndpointMode != "" && r.EndpointMode != GuestEndpointInternal && r.EndpointMode != GuestEndpointPublished {
		return accessFailure("UnsupportedEndpointMode")
	}
	hc := &unstructured.Unstructured{}
	hc.SetGroupVersionKind(gvkHC)
	mapSource := handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []reconcile.Request {
		if r.HostedClusterNamespace != "" && obj.GetNamespace() != r.HostedClusterNamespace {
			return nil
		}
		if err := r.Client.Get(ctx, client.ObjectKeyFromObject(obj), &upgradev1alpha1.HostedClusterUpgrade{}); err != nil {
			return nil
		}
		return []reconcile.Request{{NamespacedName: client.ObjectKeyFromObject(obj)}}
	})
	mapSecret := handler.EnqueueRequestsFromMapFunc(func(_ context.Context, obj client.Object) []reconcile.Request {
		if obj.GetNamespace() != r.OperatorNamespace || obj.GetLabels()[GuestAccessLabel] != "true" {
			return nil
		}
		a := obj.GetAnnotations()
		if a[accessPrefix+"source-namespace"] == "" || a[accessPrefix+"source-name"] == "" {
			return nil
		}
		return []reconcile.Request{{NamespacedName: client.ObjectKey{Namespace: a[accessPrefix+"source-namespace"], Name: a[accessPrefix+"source-name"]}}}
	})
	return ctrl.NewControllerManagedBy(mgr).Named("guest-access").
		For(&upgradev1alpha1.HostedClusterUpgrade{}, builder.WithPredicates(predicate.Or(predicate.GenerationChangedPredicate{}, predicate.Funcs{UpdateFunc: func(e event.UpdateEvent) bool {
			return !e.ObjectOld.GetDeletionTimestamp().Equal(e.ObjectNew.GetDeletionTimestamp()) || guestAccessRefreshEvent(e)
		}}))).
		Watches(hc, mapSource).Watches(&corev1.Secret{}, mapSecret).
		WithOptions(controlleroptions.Options{MaxConcurrentReconciles: 2, ReconciliationTimeout: 60 * time.Second}).Complete(r)
}

func (r *GuestAccessReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	if r.HostedClusterNamespace != "" && req.Namespace != r.HostedClusterNamespace {
		return ctrl.Result{}, nil
	}
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	if err := r.cleanup(ctx, req.NamespacedName); err != nil {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	s, err := r.source(ctx, req.NamespacedName)
	if err != nil {
		fresh := &upgradev1alpha1.HostedClusterUpgrade{}
		if readErr := r.APIReader.Get(ctx, req.NamespacedName, fresh); apierrors.IsNotFound(readErr) {
			return ctrl.Result{}, nil
		} else if readErr == nil && fresh.DeletionTimestamp.IsZero() {
			if statusErr := r.persistAccessStatus(ctx, &accessSource{hcu: fresh}, &upgradev1alpha1.GuestAccessStatus{ObservedGeneration: fresh.Generation, Reason: "SourceUnavailable", ObserverUsability: "Unknown"}); statusErr != nil {
				return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
			}
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	status := &upgradev1alpha1.GuestAccessStatus{ObservedGeneration: s.hcu.Generation, HostedClusterUID: string(s.hc.GetUID()), ClusterID: s.clusterID, ProfileRevision: GuestObserverProfile, ObserverUsability: "Unknown"}
	delay, err := r.maintain(ctx, s)
	status.Reason = "CredentialCurrent"
	status.MaintenanceReady = err == nil
	if err != nil {
		status.Reason = "AccessMaintenanceFailed"
		if f, ok := err.(*guest.Failure); ok {
			status.Reason = f.Reason
		}
		delay = 30*time.Second + accessJitter(string(s.hc.GetUID()), 15*time.Second)
	}
	// A renewal failure does not erase or overwrite the old credential.
	if target, _, e := r.buildTarget(ctx, req.NamespacedName, guest.Release{}); e == nil {
		status.CredentialAvailable = true
		expires := metav1.NewTime(target.CredentialExpiresAt)
		status.ExpiresAt = &expires
		status.ClusterVersionUID = string(target.CVUID)
		status.EndpointRevision = target.EndpointRevision
		status.SecretName = deliveryName(target.HCUID)
	}
	if err := r.persistAccessStatus(ctx, s, status); err != nil {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	return ctrl.Result{RequeueAfter: delay}, nil
}

// fence is a fresh read, including pause and plan, before each new write. Paused
// renewal is allowed only where the caller explicitly requests it.
func (r *GuestAccessReconciler) fence(ctx context.Context, s *accessSource, allowPaused bool) error {
	n, err := r.source(ctx, client.ObjectKeyFromObject(s.hcu))
	if err != nil || n.hcu.UID != s.hcu.UID || n.hcu.Generation != s.hcu.Generation || n.hc.GetUID() != s.hc.GetUID() || n.clusterID != s.clusterID || accessHash(n.hcu.Spec) != accessHash(s.hcu.Spec) {
		return accessFailure("SourceChanged")
	}
	if n.paused && !allowPaused {
		return accessFailure("PausedBootstrapDeferred")
	}
	_, current, err := r.endpoint(ctx, n)
	if err != nil || s.endpointRevision != current {
		return accessFailure("EndpointChanged")
	}
	return nil
}

func (r *GuestAccessReconciler) maintain(ctx context.Context, s *accessSource) (time.Duration, error) {
	c, revision, err := r.endpoint(ctx, s)
	if err != nil {
		return 0, err
	}
	s.endpointRevision = revision
	secret := &corev1.Secret{}
	key := client.ObjectKey{Namespace: r.OperatorNamespace, Name: deliveryName(s.hc.GetUID())}
	err = r.APIReader.Get(ctx, key, secret)
	exists := err == nil
	if err != nil && !apierrors.IsNotFound(err) {
		return 0, accessFailure("DeliveryReadFailed")
	}
	if exists && !deliveryOwned(secret, s) {
		return 0, accessFailure("DeliveryOwnershipConflict")
	}
	if s.paused && !exists {
		return 0, accessFailure("PausedBootstrapDeferred")
	}
	if exists && secret.Annotations[accessPrefix+"endpoint-revision"] == revision {
		exp, err := time.Parse(time.RFC3339, secret.Annotations[accessPrefix+"expires-at"])
		renewAt, renewalErr := time.Parse(time.RFC3339, secret.Annotations[accessPrefix+"renew-at"])
		scoped, configErr := guest.ScopedConfig(secret.Data["kubeconfig"])
		denied := s.hcu.Status.GuestAssessment != nil && s.hcu.Status.GuestAssessment.State == string(guest.AccessDenied) && s.hcu.Status.GuestAssessment.SecretUID == string(secret.UID) && s.hcu.Status.GuestAssessment.SecretRevision == secret.ResourceVersion
		if !denied && err == nil && renewalErr == nil && renewAt.Before(exp) && configErr == nil && scoped.Host == c.Host && scoped.ServerName == c.ServerName && reflect.DeepEqual(scoped.CAData, c.CAData) && secret.Annotations[accessPrefix+"cv-uid"] != "" && secret.Annotations[accessPrefix+"sa-uid"] != "" && r.now().Before(renewAt) {
			return renewAt.Sub(r.now()), nil
		}
	}
	httpClient, err := rest.HTTPClientFor(c)
	if err != nil {
		return 0, accessFailure("AdminTransportFailed")
	}
	httpClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	defer httpClient.CloseIdleConnections()
	// Identity alone, never cluster health, is the bootstrap prerequisite.
	cv, err := adminClusterVersion(ctx, c, httpClient)
	if err != nil {
		return 0, err
	}
	if string(cv.Spec.ClusterID) != s.clusterID || cv.UID == "" {
		return 0, accessFailure("GuestIdentityMismatch")
	}
	d, err := dynamic.NewForConfigAndClient(c, httpClient)
	if err != nil {
		return 0, accessFailure("AdminTransportFailed")
	}
	sa, err := r.ensureProfile(ctx, s, d)
	if err != nil {
		return 0, err
	}
	if exists && s.paused && (secret.Annotations[accessPrefix+"sa-uid"] != string(sa.GetUID()) || secret.Annotations[accessPrefix+"cv-uid"] != string(cv.UID)) {
		return 0, accessFailure("PausedIdentityChanged")
	}
	if err := r.fence(ctx, s, exists); err != nil {
		return 0, err
	}
	k, err := kubernetes.NewForConfigAndClient(c, httpClient)
	if err != nil {
		return 0, accessFailure("AdminTransportFailed")
	}
	seconds := int64(3600)
	token, err := k.CoreV1().ServiceAccounts(GuestObserverNamespace).CreateToken(ctx, GuestObserverName, &authv1.TokenRequest{Spec: authv1.TokenRequestSpec{ExpirationSeconds: &seconds}}, metav1.CreateOptions{})
	if err != nil {
		return 0, accessFailure("TokenRequestFailed")
	}
	if token.Status.Token == "" || !r.now().Add(time.Minute).Before(token.Status.ExpirationTimestamp.Time) {
		return 0, accessFailure("InvalidTokenExpiration")
	}
	// Revalidate identity/profile after issuance; a delayed response may belong to
	// a deleted SA/CV or a replaced management target.
	if err := r.fence(ctx, s, exists); err != nil {
		return 0, err
	}
	_, currentRevision, err := r.endpoint(ctx, s)
	if err != nil || currentRevision != revision {
		return 0, accessFailure("EndpointChanged")
	}
	currentCV, err := adminClusterVersion(ctx, c, httpClient)
	if err != nil || currentCV.UID != cv.UID || currentCV.Spec.ClusterID != cv.Spec.ClusterID {
		return 0, accessFailure("GuestIdentityMismatch")
	}
	verifiedSA, err := r.verifyProfile(ctx, s, d)
	if err != nil || verifiedSA.GetUID() != sa.GetUID() {
		return 0, accessFailure("ProfileChanged")
	}
	data, err := observerKubeconfig(c, token.Status.Token)
	if err != nil {
		return 0, accessFailure("CredentialEncodingFailed")
	}
	// Uncached collision/update read. A conflict/NotFound is never a Create fallback.
	latest := &corev1.Secret{}
	err = r.APIReader.Get(ctx, key, latest)
	if exists {
		if err != nil || latest.UID != secret.UID || latest.ResourceVersion != secret.ResourceVersion || !deliveryOwned(latest, s) {
			return 0, accessFailure("DeliveryChanged")
		}
	} else if !apierrors.IsNotFound(err) {
		return 0, accessFailure("DeliveryChanged")
	}
	if err := r.fence(ctx, s, exists); err != nil {
		return 0, err
	}
	secret.Name, secret.Namespace = key.Name, key.Namespace
	secret.Type = corev1.SecretTypeOpaque
	secret.Labels = map[string]string{GuestAccessLabel: "true"}
	ttl := token.Status.ExpirationTimestamp.Sub(r.now())
	if ttl <= time.Minute {
		return 0, accessFailure("InvalidTokenExpiration")
	}
	renewAt := token.Status.ExpirationTimestamp.Add(-min(10*time.Minute, ttl/3) - accessJitter(string(s.hc.GetUID()), min(2*time.Minute, ttl/20)))
	secret.Annotations = map[string]string{
		accessPrefix + "source-namespace": s.hcu.Namespace, accessPrefix + "source-name": s.hcu.Name,
		accessPrefix + "hc-uid": string(s.hc.GetUID()), accessPrefix + "hcu-uid": string(s.hcu.UID),
		accessPrefix + "cluster-id": s.clusterID, accessPrefix + "cv-uid": string(cv.UID), accessPrefix + "sa-uid": string(sa.GetUID()),
		accessPrefix + "profile": GuestObserverProfile, accessPrefix + "endpoint-revision": revision,
		accessPrefix + "expires-at": token.Status.ExpirationTimestamp.UTC().Format(time.RFC3339),
		accessPrefix + "renew-at":   renewAt.UTC().Format(time.RFC3339),
	}
	secret.Data = map[string][]byte{"kubeconfig": data}
	if exists {
		err = r.Client.Update(ctx, secret)
	} else {
		err = r.Client.Create(ctx, secret)
	}
	if err != nil {
		return 0, accessFailure("DeliveryPublicationFailed")
	}
	return max(time.Second, renewAt.Sub(r.now())), nil
}

// Observed access failures wake the independent queue, including admin/Service
// endpoint changes which the delivery-only Secret cache cannot watch. Repeated
// failed observations are naturally bounded by the upgrade's 30s heartbeat.
func guestAccessRefreshEvent(e event.UpdateEvent) bool {
	old, ok := e.ObjectOld.(*upgradev1alpha1.HostedClusterUpgrade)
	if !ok {
		return false
	}
	current, ok := e.ObjectNew.(*upgradev1alpha1.HostedClusterUpgrade)
	if !ok || current.Status.GuestAssessment == nil {
		return false
	}
	a := current.Status.GuestAssessment
	if a.State != string(guest.Unavailable) && a.State != string(guest.AccessDenied) && a.State != string(guest.IdentityMismatch) {
		return false
	}
	return !reflect.DeepEqual(old.Status.GuestAssessment, a)
}

func adminClusterVersion(ctx context.Context, c *rest.Config, h *http.Client) (*configv1.ClusterVersion, error) {
	ctx, cancel := context.WithTimeout(ctx, guest.RequestTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.Host+"/apis/config.openshift.io/v1/clusterversions/version", nil)
	if err != nil {
		return nil, accessFailure("GuestIdentityUnavailable")
	}
	resp, err := h.Do(req)
	if err != nil {
		return nil, accessFailure("GuestIdentityUnavailable")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, accessFailure("GuestIdentityUnavailable")
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, guest.MaxBytes+1))
	if err != nil || len(b) > guest.MaxBytes {
		return nil, accessFailure("GuestIdentityUnavailable")
	}
	cv := &configv1.ClusterVersion{}
	if json.Unmarshal(b, cv) != nil || cv.Name != "version" {
		return nil, accessFailure("GuestIdentityUnavailable")
	}
	return cv, nil
}

type profileObject struct {
	gvr    schema.GroupVersionResource
	object *unstructured.Unstructured
	fields []string
}

func observerProfile(s *accessSource) []profileObject {
	labels := map[string]string{GuestAccessLabel: "true"}
	annotations := map[string]string{accessPrefix + "hc-uid": string(s.hc.GetUID()), accessPrefix + "cluster-id": s.clusterID, accessPrefix + "profile": GuestObserverProfile}
	metadata := func(ns string) metav1.ObjectMeta {
		return metav1.ObjectMeta{Name: GuestObserverName, Namespace: ns, Labels: labels, Annotations: annotations}
	}
	no := false
	objects := []struct {
		obj    runtime.Object
		gvr    schema.GroupVersionResource
		fields []string
	}{
		{&corev1.Namespace{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Namespace"}, ObjectMeta: metadata("")}, corev1.SchemeGroupVersion.WithResource("namespaces"), nil},
		{&corev1.ServiceAccount{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "ServiceAccount"}, ObjectMeta: metadata(GuestObserverNamespace), AutomountServiceAccountToken: &no}, corev1.SchemeGroupVersion.WithResource("serviceaccounts"), []string{"automountServiceAccountToken"}},
		{&rbacv1.ClusterRole{TypeMeta: metav1.TypeMeta{APIVersion: rbacv1.SchemeGroupVersion.String(), Kind: "ClusterRole"}, ObjectMeta: metadata(""), Rules: []rbacv1.PolicyRule{
			{APIGroups: []string{"config.openshift.io"}, Resources: []string{"clusterversions"}, ResourceNames: []string{"version"}, Verbs: []string{"get"}},
			{APIGroups: []string{"config.openshift.io"}, Resources: []string{"clusteroperators"}, Verbs: []string{"list"}},
		}}, rbacv1.SchemeGroupVersion.WithResource("clusterroles"), []string{"rules", "aggregationRule"}},
		{&rbacv1.ClusterRoleBinding{TypeMeta: metav1.TypeMeta{APIVersion: rbacv1.SchemeGroupVersion.String(), Kind: "ClusterRoleBinding"}, ObjectMeta: metadata(""), RoleRef: rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: GuestObserverName}, Subjects: []rbacv1.Subject{{Kind: "ServiceAccount", Name: GuestObserverName, Namespace: GuestObserverNamespace}}}, rbacv1.SchemeGroupVersion.WithResource("clusterrolebindings"), []string{"roleRef", "subjects"}},
	}
	var result []profileObject
	for _, v := range objects {
		m, _ := runtime.DefaultUnstructuredConverter.ToUnstructured(v.obj)
		result = append(result, profileObject{v.gvr, &unstructured.Unstructured{Object: m}, v.fields})
	}
	return result
}

func profileOwned(obj *unstructured.Unstructured, s *accessSource) bool {
	a := obj.GetAnnotations()
	return obj.GetLabels()[GuestAccessLabel] == "true" && a[accessPrefix+"hc-uid"] == string(s.hc.GetUID()) && a[accessPrefix+"cluster-id"] == s.clusterID && a[accessPrefix+"profile"] == GuestObserverProfile && obj.GetDeletionTimestamp().IsZero() && len(obj.GetOwnerReferences()) == 0
}

func (r *GuestAccessReconciler) profile(ctx context.Context, s *accessSource, d dynamic.Interface, repair bool) (*unstructured.Unstructured, error) {
	var sa *unstructured.Unstructured
	for _, desired := range observerProfile(s) {
		resource := d.Resource(desired.gvr).Namespace(desired.object.GetNamespace())
		actual, err := resource.Get(ctx, desired.object.GetName(), metav1.GetOptions{})
		if apierrors.IsNotFound(err) && repair && !s.paused {
			if err := r.fence(ctx, s, false); err != nil {
				return nil, err
			}
			actual, err = resource.Create(ctx, desired.object, metav1.CreateOptions{})
		} else if err == nil {
			if !profileOwned(actual, s) {
				return nil, accessFailure("ProfileOwnershipConflict")
			}
			changed := false
			immutableRoleRefChanged := desired.gvr.Resource == "clusterrolebindings" && !reflect.DeepEqual(actual.Object["roleRef"], desired.object.Object["roleRef"])
			for _, field := range desired.fields {
				if !reflect.DeepEqual(actual.Object[field], desired.object.Object[field]) {
					changed = true
					if value, ok := desired.object.Object[field]; ok {
						actual.Object[field] = value
					} else {
						delete(actual.Object, field)
					}
				}
			}
			if changed {
				if !repair || s.paused {
					return nil, accessFailure("ProfileDrift")
				}
				if err := r.fence(ctx, s, false); err != nil {
					return nil, err
				}
				if immutableRoleRefChanged {
					uid, rv := actual.GetUID(), actual.GetResourceVersion()
					if err := resource.Delete(ctx, actual.GetName(), metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &rv}}); err != nil {
						return nil, accessFailure("ProfileRepairFailed")
					}
					if err := r.fence(ctx, s, false); err != nil {
						return nil, err
					}
					actual, err = resource.Create(ctx, desired.object, metav1.CreateOptions{})
				} else {
					actual, err = resource.Update(ctx, actual, metav1.UpdateOptions{})
				}
			}
		}
		if err != nil {
			return nil, accessFailure("ProfileUnavailable")
		}
		if !profileOwned(actual, s) || actual.GetUID() == "" {
			return nil, accessFailure("ProfileOwnershipConflict")
		}
		if desired.gvr.Resource == "serviceaccounts" {
			sa = actual
		}
	}
	return sa, nil
}
func (r *GuestAccessReconciler) ensureProfile(ctx context.Context, s *accessSource, d dynamic.Interface) (*unstructured.Unstructured, error) {
	return r.profile(ctx, s, d, true)
}
func (r *GuestAccessReconciler) verifyProfile(ctx context.Context, s *accessSource, d dynamic.Interface) (*unstructured.Unstructured, error) {
	return r.profile(ctx, s, d, false)
}

func (r *GuestAccessReconciler) cleanup(ctx context.Context, key client.ObjectKey) error {
	list := &corev1.SecretList{}
	if err := r.Client.List(ctx, list, client.InNamespace(r.OperatorNamespace), client.MatchingLabels{GuestAccessLabel: "true"}); err != nil {
		return err
	}
	for _, item := range list.Items {
		a := item.Annotations
		if a[accessPrefix+"source-namespace"] != key.Namespace || a[accessPrefix+"source-name"] != key.Name || a[accessPrefix+"hcu-uid"] == "" || a[accessPrefix+"hc-uid"] == "" || a[accessPrefix+"profile"] != GuestObserverProfile || item.Name != deliveryName(types.UID(a[accessPrefix+"hc-uid"])) || len(item.OwnerReferences) != 0 {
			continue
		}
		current := &corev1.Secret{}
		if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(&item), current); apierrors.IsNotFound(err) {
			continue
		} else if err != nil {
			return err
		}
		if current.UID != item.UID || current.ResourceVersion != item.ResourceVersion {
			continue
		}
		// Recheck enrollment immediately before deletion. Never infer removal from
		// transient API errors or from the label-filtered cache.
		hcu := &upgradev1alpha1.HostedClusterUpgrade{}
		err := r.APIReader.Get(ctx, key, hcu)
		if err != nil && !apierrors.IsNotFound(err) {
			return err
		}
		if err == nil && hcu.DeletionTimestamp.IsZero() && string(hcu.UID) == a[accessPrefix+"hcu-uid"] {
			hc := &unstructured.Unstructured{}
			hc.SetGroupVersionKind(gvkHC)
			err := r.APIReader.Get(ctx, key, hc)
			if err != nil && !apierrors.IsNotFound(err) {
				return err
			}
			if err == nil && hc.GetDeletionTimestamp().IsZero() && string(hc.GetUID()) == a[accessPrefix+"hc-uid"] {
				continue
			}
		}
		uid, rv := current.UID, current.ResourceVersion
		if err := r.Client.Delete(ctx, current, &client.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &rv}}); err != nil && !apierrors.IsNotFound(err) && !apierrors.IsConflict(err) {
			return err
		}
	}
	return nil
}

func (r *GuestAccessReconciler) persistAccessStatus(ctx context.Context, s *accessSource, status *upgradev1alpha1.GuestAccessStatus) error {
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		fresh := &upgradev1alpha1.HostedClusterUpgrade{}
		if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(s.hcu), fresh); err != nil {
			return err
		}
		if fresh.UID != s.hcu.UID || fresh.Generation != s.hcu.Generation {
			return nil
		}
		before := fresh.DeepCopy()
		fresh.Status.GuestAccess = status
		maintenance, available := metav1.ConditionFalse, metav1.ConditionFalse
		if status.MaintenanceReady {
			maintenance = metav1.ConditionTrue
		}
		if status.CredentialAvailable {
			available = metav1.ConditionTrue
		}
		for _, condition := range []metav1.Condition{
			{Type: "GuestAccessMaintenanceReady", Status: maintenance, Reason: status.Reason, Message: "Observer credential maintenance outcome", ObservedGeneration: fresh.Generation, LastTransitionTime: metav1.NewTime(r.now())},
			{Type: "GuestCredentialAvailable", Status: available, Reason: status.Reason, Message: "Credential presence and expiration only; not proof of observer authentication", ObservedGeneration: fresh.Generation, LastTransitionTime: metav1.NewTime(r.now())},
		} {
			meta.SetStatusCondition(&fresh.Status.Conditions, condition)
		}
		if reflect.DeepEqual(before.Status, fresh.Status) {
			return nil
		}
		return r.Client.Status().Patch(ctx, fresh, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{}))
	})
}
