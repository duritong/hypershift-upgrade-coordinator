package controller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	upgradev1alpha1 "github.com/duritong/hypershift-upgrade-coordinator/api/v1alpha1"
	"github.com/duritong/hypershift-upgrade-coordinator/internal/guest"
	configv1 "github.com/openshift/api/config/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	GuestEndpointInternal  = "internal"
	GuestEndpointPublished = "published"
	GuestAccessLabel       = "upgrade.huc.0xshift.dev/guest-observer"
	accessPrefix           = "upgrade.huc.0xshift.dev/"
	GuestObserverProfile   = "observer-v1"
	GuestObserverNamespace = "huc-guest-observer"
	GuestObserverName      = "huc-guest-observer"
)

func accessFailure(reason string) error {
	return &guest.Failure{State: guest.Unavailable, Reason: reason}
}
func accessHash(v interface{}) string {
	b, _ := json.Marshal(v)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func accessJitter(key string, bound time.Duration) time.Duration {
	h := sha256.Sum256([]byte(key))
	return time.Duration(binary.BigEndian.Uint64(h[:8]) % uint64(bound))
}

type accessTransport struct{ http.RoundTripper }

func (t accessTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := t.RoundTripper.RoundTrip(req)
	if err == nil {
		resp.Body = http.MaxBytesReader(nil, resp.Body, guest.MaxBytes)
	}
	return resp, err
}

// SafeAdminConfig is intentionally separate from guest.ScopedConfig: published
// admin credentials may use embedded client certificates, observer credentials may not.
func SafeAdminConfig(data []byte) (*rest.Config, error) {
	bad := func() (*rest.Config, error) { return nil, accessFailure("UnsafeAdminKubeconfig") }
	if len(data) == 0 || len(data) > guest.MaxBytes {
		return bad()
	}
	k, err := clientcmd.Load(data)
	if err != nil || len(k.Clusters) != 1 || len(k.AuthInfos) != 1 || len(k.Contexts) != 1 {
		return bad()
	}
	x := k.Contexts[k.CurrentContext]
	if x == nil || k.Clusters[x.Cluster] == nil || k.AuthInfos[x.AuthInfo] == nil {
		return bad()
	}
	c, a := k.Clusters[x.Cluster], k.AuthInfos[x.AuthInfo]
	u, err := url.Parse(c.Server)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") || c.CertificateAuthority != "" || len(c.CertificateAuthorityData) == 0 || c.InsecureSkipTLSVerify || c.ProxyURL != "" || c.DisableCompression || len(c.Extensions) != 0 {
		return bad()
	}
	if a.Exec != nil || a.AuthProvider != nil || a.TokenFile != "" || a.ClientCertificate != "" || a.ClientKey != "" || a.Impersonate != "" || a.ImpersonateUID != "" || len(a.ImpersonateGroups) != 0 || len(a.ImpersonateUserExtra) != 0 || a.Username != "" || a.Password != "" || len(a.Extensions) != 0 {
		return bad()
	}
	cert := len(a.ClientCertificateData) != 0 && len(a.ClientKeyData) != 0
	if (a.Token == "" && !cert) || ((len(a.ClientCertificateData) != 0 || len(a.ClientKeyData) != 0) && !cert) {
		return bad()
	}
	r, err := clientcmd.NewNonInteractiveClientConfig(*k, k.CurrentContext, &clientcmd.ConfigOverrides{}, nil).ClientConfig()
	if err != nil {
		return bad()
	}
	r.Timeout = guest.RequestTimeout
	r.Host = strings.TrimSuffix(r.Host, "/")
	r.ContentType = "application/json"
	r.AcceptContentTypes = "application/json"
	r.UserAgent = "hypershift-upgrade-coordinator/guest-access"
	r.Proxy = func(*http.Request) (*url.URL, error) { return nil, nil }
	r.WarningHandler = rest.NoWarnings{}
	r.WrapTransport = func(rt http.RoundTripper) http.RoundTripper { return accessTransport{rt} }
	return r, nil
}

type accessSource struct {
	hcu              *upgradev1alpha1.HostedClusterUpgrade
	hc               *unstructured.Unstructured
	clusterID        string
	paused           bool
	endpointRevision string
}

// GuestAccessProvider performs fresh uncached identity reads and retains no guest
// clients. BuildTargetAndReader never provisions and never uses admin guest I/O.
type GuestAccessProvider struct {
	APIReader              client.Reader
	OperatorNamespace      string
	HostedClusterNamespace string
	EndpointMode           string
	Clock                  Clock
}

func (p *GuestAccessProvider) now() time.Time {
	if p.Clock != nil {
		return p.Clock.Now()
	}
	return time.Now()
}
func (p *GuestAccessProvider) source(ctx context.Context, key client.ObjectKey) (*accessSource, error) {
	if p.APIReader == nil {
		return nil, accessFailure("SourceReaderUnavailable")
	}
	if p.HostedClusterNamespace != "" && key.Namespace != p.HostedClusterNamespace {
		return nil, accessFailure("NotEnrolled")
	}
	hcu := &upgradev1alpha1.HostedClusterUpgrade{}
	if err := p.APIReader.Get(ctx, key, hcu); err != nil {
		return nil, err
	}
	if !hcu.DeletionTimestamp.IsZero() || hcu.UID == "" {
		return nil, accessFailure("NotEnrolled")
	}
	hc := &unstructured.Unstructured{}
	hc.SetGroupVersionKind(gvkHC)
	if err := p.APIReader.Get(ctx, key, hc); err != nil {
		return nil, err
	}
	id, _, _ := unstructured.NestedString(hc.Object, "spec", "clusterID")
	if hc.GetUID() == "" || !hc.GetDeletionTimestamp().IsZero() || id == "" {
		return nil, accessFailure("SourceIdentityUnavailable")
	}
	if err := upgradev1alpha1.RejectControlPlaneReleaseOverride(hc); err != nil {
		return nil, &guest.Failure{State: guest.IdentityMismatch, Reason: "UnsupportedManagementOverride"}
	}
	paused, _, err := checkPausedState(hcu.Spec.PausedUntil, p.now())
	if err != nil {
		paused = true
	}
	return &accessSource{hcu: hcu, hc: hc, clusterID: id, paused: paused}, nil
}

func (p *GuestAccessProvider) endpoint(ctx context.Context, s *accessSource) (*rest.Config, string, error) {
	ref, _, _ := unstructured.NestedString(s.hc.Object, "status", "kubeconfig", "name")
	key, _, _ := unstructured.NestedString(s.hc.Object, "status", "kubeconfig", "key")
	if key == "" {
		key = "kubeconfig"
	}
	if ref == "" {
		return nil, "", accessFailure("AdminKubeconfigUnavailable")
	}
	secret := &corev1.Secret{}
	if err := p.APIReader.Get(ctx, client.ObjectKey{Namespace: s.hc.GetNamespace(), Name: ref}, secret); err != nil {
		return nil, "", accessFailure("AdminKubeconfigUnavailable")
	}
	c, err := SafeAdminConfig(secret.Data[key])
	if err != nil {
		return nil, "", err
	}
	identity := []string{string(secret.UID), secret.ResourceVersion, c.Host, c.ServerName, accessHash(c.CAData)}
	mode := p.EndpointMode
	if mode == "" {
		mode = GuestEndpointInternal
	}
	switch mode {
	case GuestEndpointInternal:
		ns := WorkerControlPlaneNamespace(s.hc.GetNamespace(), s.hc.GetName())
		hcp := &unstructured.Unstructured{}
		hcp.SetGroupVersionKind(gvkHCP)
		if err := p.APIReader.Get(ctx, client.ObjectKey{Namespace: ns, Name: s.hc.GetName()}, hcp); err != nil {
			return nil, "", accessFailure("EndpointUnavailable")
		}
		// HCPs use an explicit HC reference annotation, since cross-namespace owner refs are invalid.
		hcpClusterID, _, _ := unstructured.NestedString(hcp.Object, "spec", "clusterID")
		if hcp.GetAnnotations()["hypershift.openshift.io/cluster"] != s.hc.GetNamespace()+"/"+s.hc.GetName() || hcpClusterID != s.clusterID || hcp.GetUID() == "" || !hcp.GetDeletionTimestamp().IsZero() {
			return nil, "", accessFailure("EndpointOwnershipMismatch")
		}
		svc := &corev1.Service{}
		if err := p.APIReader.Get(ctx, client.ObjectKey{Namespace: ns, Name: "kube-apiserver"}, svc); err != nil {
			return nil, "", accessFailure("EndpointUnavailable")
		}
		owned := false
		for _, o := range svc.OwnerReferences {
			if o.UID == hcp.GetUID() && o.Name == hcp.GetName() && o.Kind == "HostedControlPlane" && o.APIVersion == gvkHCP.GroupVersion().String() && o.Controller != nil && *o.Controller {
				owned = true
			}
		}
		if !owned || !svc.DeletionTimestamp.IsZero() {
			return nil, "", accessFailure("EndpointOwnershipMismatch")
		}
		port := int32(0)
		for _, v := range svc.Spec.Ports {
			if v.Protocol == corev1.ProtocolTCP || v.Protocol == "" {
				if port != 0 || v.Port <= 0 {
					return nil, "", accessFailure("AmbiguousEndpointPort")
				}
				port = v.Port
			}
		}
		if port == 0 {
			return nil, "", accessFailure("EndpointUnavailable")
		}
		c.ServerName = "kube-apiserver." + ns + ".svc"
		c.Host = "https://" + net.JoinHostPort(c.ServerName, strconv.Itoa(int(port)))
		identity = append(identity, string(hcp.GetUID()), string(svc.UID), svc.ResourceVersion, c.Host, c.ServerName)
	case GuestEndpointPublished:
	default:
		return nil, "", accessFailure("UnsupportedEndpointMode")
	}
	return c, accessHash(identity), nil
}

func deliveryName(uid types.UID) string { return "guest-observer-" + string(uid) }
func deliveryOwned(secret *corev1.Secret, s *accessSource) bool {
	a := secret.Annotations
	return secret.Labels[GuestAccessLabel] == "true" && a[accessPrefix+"source-namespace"] == s.hcu.Namespace && a[accessPrefix+"source-name"] == s.hcu.Name && a[accessPrefix+"hc-uid"] == string(s.hc.GetUID()) && a[accessPrefix+"hcu-uid"] == string(s.hcu.UID) && a[accessPrefix+"cluster-id"] == s.clusterID && a[accessPrefix+"profile"] == GuestObserverProfile && len(secret.OwnerReferences) == 0 && secret.DeletionTimestamp.IsZero()
}

func (p *GuestAccessProvider) buildTarget(ctx context.Context, key client.ObjectKey, expected guest.Release) (guest.Target, []byte, error) {
	s, err := p.source(ctx, key)
	if err != nil {
		if failure, ok := err.(*guest.Failure); ok {
			return guest.Target{}, nil, failure
		}
		return guest.Target{}, nil, accessFailure("SourceUnavailable")
	}
	hcp := &unstructured.Unstructured{}
	hcp.SetGroupVersionKind(gvkHCP)
	if err := p.APIReader.Get(ctx, client.ObjectKey{Namespace: WorkerControlPlaneNamespace(key.Namespace, key.Name), Name: key.Name}, hcp); err != nil {
		return guest.Target{}, nil, accessFailure("ManagementIdentityUnavailable")
	}
	if s.hc.GetGeneration() < 1 || hcp.GetUID() == "" || hcp.GetGeneration() < 1 || !hcp.GetDeletionTimestamp().IsZero() {
		return guest.Target{}, nil, accessFailure("ManagementIdentityUnavailable")
	}
	release, _, _ := unstructured.NestedString(s.hc.Object, "spec", "release", "image")
	management, found, managementErr := unstructured.NestedString(hcp.Object, "spec", "controlPlaneReleaseImage")
	if managementErr != nil || (found && !samePayload(management, release)) {
		return guest.Target{}, nil, &guest.Failure{State: guest.IdentityMismatch, Reason: "UnsupportedManagementOverride"}
	}
	endpoint, revision, err := p.endpoint(ctx, s)
	if err != nil {
		return guest.Target{}, nil, err
	}
	secret := &corev1.Secret{}
	if err := p.APIReader.Get(ctx, client.ObjectKey{Namespace: p.OperatorNamespace, Name: deliveryName(s.hc.GetUID())}, secret); err != nil {
		return guest.Target{}, nil, accessFailure("CredentialUnavailable")
	}
	a := secret.Annotations
	expires, err := time.Parse(time.RFC3339, a[accessPrefix+"expires-at"])
	if !deliveryOwned(secret, s) || a[accessPrefix+"endpoint-revision"] != revision || a[accessPrefix+"cv-uid"] == "" || a[accessPrefix+"sa-uid"] == "" {
		return guest.Target{}, nil, accessFailure("CredentialIdentityMismatch")
	}
	if err != nil || !p.now().Add(guest.CollectionTimeout).Before(expires) {
		return guest.Target{}, nil, accessFailure("CredentialExpired")
	}
	scoped, err := guest.ScopedConfig(secret.Data["kubeconfig"])
	if err != nil || scoped.Host != endpoint.Host || scoped.ServerName != endpoint.ServerName || !bytes.Equal(scoped.CAData, endpoint.CAData) {
		return guest.Target{}, nil, accessFailure("CredentialEndpointMismatch")
	}
	t := guest.Target{Namespace: key.Namespace, Name: key.Name, HCUUID: s.hcu.UID, Generation: s.hcu.Generation, HCUID: s.hc.GetUID(), HCGeneration: s.hc.GetGeneration(), HCPUID: hcp.GetUID(), HCPGeneration: hcp.GetGeneration(), ClusterID: configv1.ClusterID(s.clusterID), CVUID: types.UID(a[accessPrefix+"cv-uid"]), HCRelease: release, PlanRevision: accessHash(s.hcu.Spec), EndpointRevision: revision, ProfileRevision: GuestObserverProfile, SecretUID: secret.UID, SecretRevision: secret.ResourceVersion, CredentialExpiresAt: expires, Expected: expected}
	return t, secret.Data["kubeconfig"], nil
}

func (p *GuestAccessProvider) BuildTargetAndReader(ctx context.Context, key client.ObjectKey, expected guest.Release) (guest.Target, *guest.Reader, error) {
	t, data, err := p.buildTarget(ctx, key, expected)
	if err != nil {
		return guest.Target{}, nil, err
	}
	r, err := guest.NewReader(data, t, p.now)
	return t, r, err
}

// ValidateTarget must be called immediately before EACH action, never only once
// per reconcile. Rotation, source replacement and plan edits invalidate equality.
func (p *GuestAccessProvider) ValidateTarget(ctx context.Context, previous guest.Target) error {
	t, _, err := p.buildTarget(ctx, client.ObjectKey{Namespace: previous.Namespace, Name: previous.Name}, previous.Expected)
	if err != nil {
		return err
	}
	if t != previous {
		return accessFailure("TargetChanged")
	}
	return nil
}

func observerKubeconfig(c *rest.Config, token string) ([]byte, error) {
	k := clientcmdapi.NewConfig()
	k.Clusters["guest"] = &clientcmdapi.Cluster{Server: c.Host, TLSServerName: c.ServerName, CertificateAuthorityData: c.CAData}
	k.AuthInfos["observer"] = &clientcmdapi.AuthInfo{Token: token}
	k.Contexts["observer"] = &clientcmdapi.Context{Cluster: "guest", AuthInfo: "observer"}
	k.CurrentContext = "observer"
	return clientcmd.Write(*k)
}
