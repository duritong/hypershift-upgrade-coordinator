package guest

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	configv1 "github.com/openshift/api/config/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

const serviceName = "kube-apiserver.clusters-test.svc"

func servingCertificate(t *testing.T) (tls.Certificate, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: serviceName}, DNSNames: []string{serviceName}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, IsCA: true, BasicConstraintsValid: true}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	cert, err := tls.X509KeyPair(ca, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}))
	if err != nil {
		t.Fatal(err)
	}
	return cert, ca
}

func guestServer(t *testing.T, handler http.HandlerFunc) (*httptest.Server, []byte) {
	t.Helper()
	cert, ca := servingCertificate(t)
	s := httptest.NewUnstartedServer(handler)
	s.TLS = &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	s.StartTLS()
	t.Cleanup(s.Close)
	return s, kubeconfig(t, s.URL, serviceName, ca)
}
func kubeconfig(t *testing.T, server, name string, ca []byte) []byte {
	t.Helper()
	k := clientcmdapi.Config{CurrentContext: "observer", Contexts: map[string]*clientcmdapi.Context{"observer": {Cluster: "guest", AuthInfo: "observer"}}, Clusters: map[string]*clientcmdapi.Cluster{"guest": {Server: server, TLSServerName: name, CertificateAuthorityData: ca}}, AuthInfos: map[string]*clientcmdapi.AuthInfo{"observer": {Token: "secret-observer-token"}}}
	b, err := clientcmd.Write(k)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
func page(ops []configv1.ClusterOperator, rv, next string) configv1.ClusterOperatorList {
	return configv1.ClusterOperatorList{TypeMeta: metav1.TypeMeta{Kind: "ClusterOperatorList", APIVersion: "config.openshift.io/v1"}, ListMeta: metav1.ListMeta{ResourceVersion: rv, Continue: next}, Items: ops}
}
func reader(t *testing.T, data []byte) *Reader {
	t.Helper()
	r, err := NewReader(data, target(), func() time.Time { return testNow })
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func requireFailure(t *testing.T, err error, state State, reason string) {
	t.Helper()
	f, ok := err.(*Failure)
	if !ok || f.State != state || (reason != "" && f.Reason != reason) {
		t.Fatalf("want %s/%s, got %v", state, reason, err)
	}
	if strings.Contains(err.Error(), "secret-observer-token") || strings.Contains(err.Error(), "raw-sensitive-response") {
		t.Fatalf("leaked server/auth data: %v", err)
	}
}

func TestRESTCollectionTLSAndPagination(t *testing.T) {
	s := healthySnapshot()
	other := *s.Operators[0].DeepCopy()
	other.Name = "new-operator"
	other.UID = "new-uid"
	var calls atomic.Int32
	_, data := guestServer(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.TLS.ServerName != serviceName || r.Header.Get("Authorization") != "Bearer secret-observer-token" || !strings.Contains(r.UserAgent(), "guest-observer") {
			t.Errorf("wrong scoped auth/SNI/agent")
		}
		switch r.URL.Path {
		case apiPath + "clusterversions/version":
			writeJSON(w, s.Version)
		case apiPath + "clusteroperators":
			if r.URL.Query().Get("limit") != "256" || r.URL.Query().Has("labelSelector") || r.URL.Query().Has("fieldSelector") {
				t.Error("not a bounded ALL-current list")
			}
			if r.URL.Query().Get("continue") == "next-token" {
				writeJSON(w, page([]configv1.ClusterOperator{other}, "20", ""))
			} else {
				writeJSON(w, page(s.Operators, "20", "next-token"))
			}
		default:
			t.Errorf("unexpected REST path %s", r.URL.Path)
			http.NotFound(w, r)
		}
	})
	r := reader(t, data)
	snapshot, err := r.Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 4 || len(snapshot.Operators) != 2 {
		t.Fatalf("not CV/list/list/CV: calls=%d snapshot=%+v", calls.Load(), snapshot)
	}
	if verdict := Assess(snapshot, target(), Policy{Purpose: WorkerEntry}, testNow); verdict.State != Ready {
		t.Fatalf("healthy real REST snapshot rejected: %+v", verdict)
	}
}

func TestCollectionRejectsIncompleteEvidence(t *testing.T) {
	for _, mode := range []string{"empty", "nil-items", "rv-conflict", "failed-page", "repeated-token", "empty-page", "remaining", "remaining-conflict", "cv-rv", "cv-condition", "cv-uid", "cv-generation", "cv-request", "cv-desired", "cv-history", "cv-cluster", "duplicate-json", "malformed-json", "operator-limit"} {
		t.Run(mode, func(t *testing.T) {
			s := healthySnapshot()
			var cvReads atomic.Int32
			_, data := guestServer(t, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == apiPath+"clusterversions/version" {
					v := *s.Version.DeepCopy()
					if cvReads.Add(1) > 1 {
						switch mode {
						case "cv-rv":
							v.ResourceVersion = "2"
						case "cv-condition":
							setCondition(&v.Status.Conditions, "Failing", "True")
						case "cv-uid":
							v.UID = "new"
						case "cv-generation":
							v.Generation++
						case "cv-request":
							v.Spec.DesiredUpdate = &configv1.Update{Version: "4.21.0"}
						case "cv-desired":
							v.Status.Desired.Version = "4.21.0"
						case "cv-history":
							v.Status.History[0].State = configv1.PartialUpdate
						case "cv-cluster":
							v.Spec.ClusterID = "new"
						}
					}
					writeJSON(w, v)
					return
				}
				p := page(s.Operators, "20", "")
				switch mode {
				case "empty":
					p.Items = []configv1.ClusterOperator{}
				case "nil-items":
					p.Items = nil
				case "rv-conflict", "failed-page", "repeated-token", "empty-page":
					p.Continue = "next"
					if r.URL.Query().Get("continue") != "" {
						switch mode {
						case "rv-conflict":
							p.ResourceVersion = "21"
							p.Continue = ""
						case "failed-page":
							http.Error(w, "raw-sensitive-response", 500)
							return
						case "empty-page":
							p.Items = []configv1.ClusterOperator{}
						}
					}
				case "remaining":
					n := int64(2)
					p.RemainingItemCount = &n
				case "remaining-conflict":
					n := int64(2)
					p.RemainingItemCount = &n
					p.Continue = "next"
					if r.URL.Query().Get("continue") != "" {
						p.Continue = ""
						n = 0
					}
				case "duplicate-json":
					w.Header().Set("Content-Type", "application/json")
					fmt.Fprint(w, `{"kind":"ClusterOperatorList","kind":"ClusterOperatorList","apiVersion":"config.openshift.io/v1","metadata":{"resourceVersion":"20"},"items":[]}`)
					return
				case "malformed-json":
					fmt.Fprint(w, `{"items":[`)
					return
				case "operator-limit":
					for len(p.Items) <= MaxOperators {
						p.Items = append(p.Items, s.Operators[0])
					}
				}
				writeJSON(w, p)
			})
			snapshot, err := reader(t, data).Collect(context.Background())
			if err == nil || snapshot.complete {
				t.Fatalf("partial/conflicting snapshot succeeded: %+v", snapshot)
			}
			state := UnsupportedEvidence
			if strings.HasPrefix(mode, "cv-") || mode == "rv-conflict" || mode == "failed-page" {
				state = Unavailable
			}
			requireFailure(t, err, state, "")
		})
	}
}

func TestTLSRejectsWrongCAAndHostname(t *testing.T) {
	_, data := guestServer(t, func(w http.ResponseWriter, r *http.Request) { t.Error("untrusted TLS reached HTTP handler") })
	for _, mode := range []string{"ca", "hostname"} {
		t.Run(mode, func(t *testing.T) {
			k, err := clientcmd.Load(data)
			if err != nil {
				t.Fatal(err)
			}
			if mode == "ca" {
				_, ca := servingCertificate(t)
				k.Clusters["guest"].CertificateAuthorityData = ca
			} else {
				k.Clusters["guest"].TLSServerName = "wrong-host.example"
			}
			b, err := clientcmd.Write(*k)
			if err != nil {
				t.Fatal(err)
			}
			_, err = reader(t, b).Collect(context.Background())
			requireFailure(t, err, Unavailable, "GuestTransportError")
		})
	}
}

func TestCancellationAndRealRequestDeadline(t *testing.T) {
	for _, mode := range []string{"cancel", "deadline"} {
		t.Run(mode, func(t *testing.T) {
			entered := make(chan struct{})
			done := make(chan struct{})
			_, data := guestServer(t, func(w http.ResponseWriter, r *http.Request) { close(entered); <-r.Context().Done(); close(done) })
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if mode == "cancel" {
				go func() { <-entered; cancel() }()
			}
			start := time.Now()
			_, err := reader(t, data).Collect(ctx)
			elapsed := time.Since(start)
			reason := "Canceled"
			if mode == "deadline" {
				reason = "DeadlineExceeded"
				if elapsed < RequestTimeout/2 || elapsed > RequestTimeout+3*time.Second {
					t.Fatalf("wrong real deadline with frozen clock: %s", elapsed)
				}
			}
			requireFailure(t, err, Unavailable, reason)
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				t.Fatal("server request not canceled")
			}
		})
	}
}

func TestRealCollectionDeadlineWithFrozenClock(t *testing.T) {
	s := healthySnapshot()
	_, data := guestServer(t, func(w http.ResponseWriter, r *http.Request) {
		// Each individual response could fit its 5s request budget, but the
		// three-response sequence must not exceed the real 10s collection limit.
		timer := time.NewTimer(3500 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-r.Context().Done():
			return
		case <-timer.C:
		}
		if r.URL.Path == apiPath+"clusterversions/version" {
			writeJSON(w, s.Version)
		} else {
			writeJSON(w, page(s.Operators, "20", ""))
		}
	})
	start := time.Now()
	_, err := reader(t, data).Collect(context.Background())
	requireFailure(t, err, Unavailable, "DeadlineExceeded")
	if elapsed := time.Since(start); elapsed < CollectionTimeout-time.Second || elapsed > CollectionTimeout+3*time.Second {
		t.Fatalf("wrong real collection deadline: %s", elapsed)
	}
}

func TestNamedServingCertificateSelection(t *testing.T) {
	s := healthySnapshot()
	cert, ca := servingCertificate(t)
	var selected atomic.Bool
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == apiPath+"clusterversions/version" {
			writeJSON(w, s.Version)
		} else {
			writeJSON(w, page(s.Operators, "20", ""))
		}
	}))
	// httptest supplies its ordinary default certificate, which does not cover
	// the service DNS. Only the matching SNI selects the trusted private cert.
	server.TLS = &tls.Config{MinVersion: tls.VersionTLS12, GetCertificate: func(hello *tls.ClientHelloInfo) (*tls.Certificate, error) {
		if hello.ServerName == serviceName {
			selected.Store(true)
			return &cert, nil
		}
		return nil, nil
	}}
	server.StartTLS()
	defer server.Close()
	_, err := reader(t, kubeconfig(t, server.URL, serviceName, ca)).Collect(context.Background())
	if err != nil || !selected.Load() {
		t.Fatalf("service named certificate not selected: selected=%t err=%v", selected.Load(), err)
	}
}

func TestByteLimitsBeforeDecoding(t *testing.T) {
	for _, mode := range []string{"content-length", "chunked", "aggregate", "error-body"} {
		t.Run(mode, func(t *testing.T) {
			s := healthySnapshot()
			_, data := guestServer(t, func(w http.ResponseWriter, r *http.Request) {
				if mode == "aggregate" {
					if r.URL.Path == apiPath+"clusterversions/version" {
						writeJSON(w, s.Version)
						return
					}
					p := page(s.Operators, "20", "next")
					p.Items[0].Status.Conditions[0].Message = strings.Repeat("x", MaxBytes/2)
					writeJSON(w, p)
					return
				}
				if mode == "content-length" {
					w.Header().Set("Content-Length", fmt.Sprint(MaxBytes+1))
				}
				if mode == "error-body" {
					w.WriteHeader(http.StatusForbidden)
				}
				if mode == "chunked" || mode == "error-body" {
					w.(http.Flusher).Flush()
				}
				fmt.Fprint(w, strings.Repeat("x", MaxBytes+1))
			})
			_, err := reader(t, data).Collect(context.Background())
			requireFailure(t, err, UnsupportedEvidence, "ByteLimit")
		})
	}
}

func TestDeniedErrorsAndRedirectsAreSanitized(t *testing.T) {
	for _, code := range []int{401, 403, 429, 500, 302, 206} {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			var redirectCalls atomic.Int32
			other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { redirectCalls.Add(1) }))
			defer other.Close()
			_, data := guestServer(t, func(w http.ResponseWriter, r *http.Request) {
				if code == 302 {
					w.Header().Set("Location", other.URL)
				}
				http.Error(w, "raw-sensitive-response secret-observer-token", code)
			})
			_, err := reader(t, data).Collect(context.Background())
			state := Unavailable
			if code == 401 || code == 403 {
				state = AccessDenied
			}
			requireFailure(t, err, state, "")
			if redirectCalls.Load() != 0 {
				t.Fatal("observer credentials followed redirect")
			}
		})
	}
}

func TestRESTDynamicAddRemoveAndFreshCVFailure(t *testing.T) {
	var stage atomic.Int32
	s := healthySnapshot()
	bad := *s.Operators[0].DeepCopy()
	bad.Name = "added"
	bad.UID = "added-uid"
	setCondition(&bad.Status.Conditions, "Degraded", "True")
	_, data := guestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == apiPath+"clusterversions/version" {
			v := *s.Version.DeepCopy()
			if stage.Load() == 4 {
				setCondition(&v.Status.Conditions, "Failing", "True")
			}
			writeJSON(w, v)
			return
		}
		ops := append([]configv1.ClusterOperator(nil), s.Operators...)
		switch stage.Load() {
		case 1:
			ops = append(ops, bad)
		case 3:
			ops = []configv1.ClusterOperator{}
		}
		writeJSON(w, page(ops, "20", ""))
	})
	r := reader(t, data)
	for i, want := range []State{Ready, Unhealthy, Ready, UnsupportedEvidence, Unhealthy} {
		stage.Store(int32(i))
		snapshot, err := r.Collect(context.Background())
		if i == 3 {
			requireFailure(t, err, want, "")
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		verdict := Assess(snapshot, target(), Policy{Purpose: WorkerGuard}, testNow)
		if verdict.State != want {
			t.Fatalf("stage %d: %+v", i, verdict)
		}
	}
}
