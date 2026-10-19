package guest

import (
	"net/http"
	"net/url"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// ScopedConfig accepts only self-contained credentials and verified HTTPS. It
// inspects every entry before clientcmd can load files or instantiate plugins.
// The caller must obtain this kubeconfig from an owned observer delivery Secret;
// this helper cannot infer a token's RBAC privileges and never obtains admin data.
func ScopedConfig(data []byte) (*rest.Config, error) {
	bad := func() (*rest.Config, error) { return nil, &Failure{UnsupportedEvidence, "UnsafeKubeconfig"} }
	if len(data) == 0 || len(data) > MaxBytes {
		return bad()
	}
	k, err := clientcmd.Load(data)
	if err != nil {
		return bad()
	}
	if len(k.Clusters) != 1 || len(k.AuthInfos) != 1 || len(k.Contexts) != 1 {
		return bad()
	}
	selected := k.Contexts[k.CurrentContext]
	if selected == nil || k.Clusters[selected.Cluster] == nil || k.AuthInfos[selected.AuthInfo] == nil {
		return bad()
	}
	for _, c := range k.Clusters {
		if c == nil || c.CertificateAuthority != "" || c.InsecureSkipTLSVerify || c.ProxyURL != "" || c.DisableCompression || len(c.CertificateAuthorityData) == 0 {
			return bad()
		}
		u, e := url.Parse(c.Server)
		if e != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
			return bad()
		}
	}
	for _, a := range k.AuthInfos {
		if a == nil || a.Exec != nil || a.AuthProvider != nil || a.TokenFile != "" || a.ClientCertificate != "" || a.ClientKey != "" || a.Impersonate != "" || a.ImpersonateUID != "" || len(a.ImpersonateGroups) != 0 || len(a.ImpersonateUserExtra) != 0 || a.Username != "" || a.Password != "" {
			return bad()
		}
		// Delivery credentials are scoped bearer tokens, not administrative certs.
		if a.Token == "" || len(a.ClientCertificateData) != 0 || len(a.ClientKeyData) != 0 {
			return bad()
		}
	}
	c, err := clientcmd.NewNonInteractiveClientConfig(*k, k.CurrentContext, &clientcmd.ConfigOverrides{}, nil).ClientConfig()
	if err != nil {
		return bad()
	}
	c.Timeout = RequestTimeout
	c.UserAgent = "hypershift-upgrade-coordinator/guest-observer"
	c.Proxy = func(*http.Request) (*url.URL, error) { return nil, nil }
	c.WarningHandler = rest.NoWarnings{}
	return c, nil
}

// NewReader builds the production authenticated client-go REST transport. It has
// no admin fallback, proxy/environment proxy, redirects, discovery or guest cache.
func NewReader(kubeconfig []byte, target Target, now func() time.Time) (*Reader, error) {
	if !target.valid() {
		return nil, &Failure{IdentityMismatch, "InvalidTarget"}
	}
	c, err := ScopedConfig(kubeconfig)
	if err != nil {
		return nil, err
	}
	c.WrapTransport = func(rt http.RoundTripper) http.RoundTripper { return boundedTransport{rt} }
	c.NegotiatedSerializer = serializer.NewCodecFactory(runtime.NewScheme()).WithoutConversion()
	client, err := rest.HTTPClientFor(c)
	if err != nil {
		return nil, &Failure{Unavailable, "TransportConfiguration"}
	}
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	restClient, err := rest.UnversionedRESTClientForConfigAndClient(c, client)
	if err != nil {
		return nil, &Failure{Unavailable, "TransportConfiguration"}
	}
	if now == nil {
		now = time.Now
	}
	return &Reader{client: restClient, target: target, now: now}, nil
}
