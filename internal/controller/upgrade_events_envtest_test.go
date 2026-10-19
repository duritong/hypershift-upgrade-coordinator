package controller

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"time"

	upgradev1 "github.com/duritong/hypershift-upgrade-coordinator/api/v1alpha1"
	"github.com/duritong/hypershift-upgrade-coordinator/internal/guest"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	configv1 "github.com/openshift/api/config/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	toolscache "k8s.io/client-go/tools/cache"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

// This external transport seam still collects and assesses the actual HTTP CV/CO
// responses. The policy clock is real wall time, not the frozen flow-harness clock.
type watchGuestReader struct {
	*legacyGuestAccess
	kubeconfig []byte
	reads      atomic.Int64
}

func (p *watchGuestReader) BuildTargetAndReader(ctx context.Context, key client.ObjectKey, expected guest.Release) (guest.Target, *guest.Reader, error) {
	target := p.target(ctx, key, expected)
	reader, err := guest.NewReader(p.kubeconfig, target, time.Now)
	if err == nil {
		p.reads.Add(1)
	}
	return target, reader, err
}

var _ = Describe("Primary upgrade event filtering", func() {
	It("does not feed blocked guest observation status back into the queue", func() {
		const namespace = "guest-observation-events"
		ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}
		Expect(k8sClient.Create(ctx, ns)).To(Succeed())
		DeferCleanup(func() { Expect(k8sClient.Delete(ctx, ns)).To(Succeed()) })
		Expect(k8sClient.Create(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "pull-secret"}, Data: map[string][]byte{corev1.DockerConfigJsonKey: []byte(`{"auths":{}}`)}})).To(Succeed())
		hc := createAndSetStatusHostedCluster(ctx, "blocked-guest", namespace, regressionImage("4.20.0"))
		createReadyControlPlane(ctx, hc)
		DeferCleanup(func() { _ = k8sClient.Delete(ctx, hc) })
		u := regressionHCU("")
		u.Namespace = namespace
		u.Name = hc.GetName()
		u.UID = ""
		u.Status = upgradev1.HostedClusterUpgradeStatus{}
		Expect(k8sClient.Create(ctx, u)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(ctx, u); clearUpgradeGauges(namespace, u.Name) })
		version := fixtureCV(regressionImage("4.20.0"), "a5e99fb2-0598-4b34-9081-14236b744c16")
		operators := fixtureOperators(regressionImage("4.20.0"))
		operators[0].Status.Conditions = fixtureConditions(map[string]bool{"Available": true, "Degraded": true, "Progressing": false})
		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			if r.Header.Get("Authorization") != "Bearer observer-token" {
				w.WriteHeader(http.StatusForbidden)
				return
			}
			if r.URL.Path == "/apis/config.openshift.io/v1/clusterversions/version" {
				_ = json.NewEncoder(w).Encode(version)
			} else {
				_ = json.NewEncoder(w).Encode(configv1.ClusterOperatorList{TypeMeta: metav1.TypeMeta{Kind: "ClusterOperatorList", APIVersion: "config.openshift.io/v1"}, ListMeta: metav1.ListMeta{ResourceVersion: "1"}, Items: operators})
			}
		}))
		defer server.Close()
		config := &rest.Config{Host: server.URL, TLSClientConfig: rest.TLSClientConfig{CAData: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})}}
		data, err := observerKubeconfig(config, "observer-token")
		Expect(err).NotTo(HaveOccurred())
		mgr, err := ctrl.NewManager(cfg, ctrl.Options{Scheme: k8sClient.Scheme(), Client: client.Options{Cache: &client.CacheOptions{Unstructured: true}}, Metrics: metricsserver.Options{BindAddress: "0"}})
		Expect(err).NotTo(HaveOccurred())
		provider := &watchGuestReader{legacyGuestAccess: &legacyGuestAccess{client: mgr.GetAPIReader()}, kubeconfig: data}
		r := &HostedClusterUpgradeReconciler{Client: mgr.GetClient(), APIReader: mgr.GetAPIReader(), Scheme: mgr.GetScheme(), HostedClusterNamespace: namespace, ReleaseResolver: fixtureReleaseResolver{}, GuestAccess: provider}
		Expect(r.setupWithManager(mgr, "upgrade-observation-events")).To(Succeed())
		runCtx, stop := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- mgr.Start(runCtx) }()
		defer func() { stop(); Eventually(done, 10*time.Second).Should(Receive(BeNil())) }()
		key := client.ObjectKeyFromObject(u)
		Eventually(func(g Gomega) {
			fresh := &upgradev1.HostedClusterUpgrade{}
			g.Expect(k8sClient.Get(ctx, key, fresh)).To(Succeed())
			g.Expect(fresh.Status.GuestAssessment).NotTo(BeNil())
			g.Expect(fresh.Status.GuestAssessment.State).To(Equal("Unhealthy"))
			g.Expect(provider.reads.Load()).To(Equal(int64(1)))
		}, 10*time.Second, 25*time.Millisecond).Should(Succeed())
		Consistently(provider.reads.Load, time.Second, 25*time.Millisecond).Should(Equal(int64(1)))

		// Publish a distinguishable observation-only event without depending on
		// metav1.Time's JSON second boundary. It is still recent, unhealthy evidence.
		fresh := &upgradev1.HostedClusterUpgrade{}
		Expect(k8sClient.Get(ctx, key, fresh)).To(Succeed())
		observationTime := metav1.NewTime(time.Now().Add(-2 * time.Second))
		fresh.Status.GuestAssessment.LastAttemptTime = observationTime
		fresh.Status.GuestAssessment.LastSuccessfulObservation = &observationTime
		Expect(k8sClient.Status().Update(ctx, fresh)).To(Succeed())
		Eventually(func() string {
			cached := &upgradev1.HostedClusterUpgrade{}
			if mgr.GetClient().Get(ctx, key, cached) != nil {
				return ""
			}
			return cached.ResourceVersion
		}, 5*time.Second, 25*time.Millisecond).Should(Equal(fresh.ResourceVersion))
		Consistently(provider.reads.Load, time.Second, 25*time.Millisecond).Should(Equal(int64(1)))

		// Access readiness is an external wakeup: it must cause a fresh actual
		// collection even while the guest remains blocked. No 30s sleep is needed.
		Expect(k8sClient.Get(ctx, key, fresh)).To(Succeed())
		fresh.Status.GuestAccess = &upgradev1.GuestAccessStatus{ObservedGeneration: fresh.Generation, Reason: "CredentialCurrent", MaintenanceReady: true, CredentialAvailable: true, ObserverUsability: "Unknown"}
		Expect(k8sClient.Status().Update(ctx, fresh)).To(Succeed())
		Eventually(func(g Gomega) {
			g.Expect(provider.reads.Load()).To(Equal(int64(2)))
			g.Expect(k8sClient.Get(ctx, key, fresh)).To(Succeed())
			g.Expect(fresh.Status.GuestAssessment.LastSuccessfulObservation.After(observationTime.Time)).To(BeTrue())
			g.Expect(fresh.Status.GuestAssessment.State).To(Equal("Unhealthy"))
		}, 5*time.Second, 25*time.Millisecond).Should(Succeed())
		Consistently(provider.reads.Load, time.Second, 25*time.Millisecond).Should(Equal(int64(2)))
	})
})

type fleetWatchGuestReaders map[client.ObjectKey]*watchGuestReader

func (p fleetWatchGuestReaders) BuildTargetAndReader(ctx context.Context, key client.ObjectKey, expected guest.Release) (guest.Target, *guest.Reader, error) {
	if reader := p[key]; reader != nil {
		return reader.BuildTargetAndReader(ctx, key, expected)
	}
	return guest.Target{}, nil, &guest.Failure{State: guest.Unavailable, Reason: "TestGuestNotEnrolled"}
}
func (p fleetWatchGuestReaders) ValidateTarget(ctx context.Context, target guest.Target) error {
	if reader := p[client.ObjectKey{Namespace: target.Namespace, Name: target.Name}]; reader != nil {
		return reader.ValidateTarget(ctx, target)
	}
	return &guest.Failure{State: guest.Unavailable, Reason: "TestGuestNotEnrolled"}
}

var _ = Describe("Primary upgrade fleet parallelism", func() {
	It("passes a healthy source gate while another guest request remains blocked", func() {
		const namespace = "guest-fleet-parallelism"
		ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}
		Expect(k8sClient.Create(ctx, ns)).To(Succeed())
		DeferCleanup(func() { Expect(k8sClient.Delete(ctx, ns)).To(Succeed()) })
		Expect(k8sClient.Create(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "pull-secret"}, Data: map[string][]byte{corev1.DockerConfigJsonKey: []byte(`{"auths":{}}`)}})).To(Succeed())
		mgr, err := ctrl.NewManager(cfg, ctrl.Options{Scheme: k8sClient.Scheme(), Client: client.Options{Cache: &client.CacheOptions{Unstructured: true}}, Metrics: metricsserver.Options{BindAddress: "0"}})
		Expect(err).NotTo(HaveOccurred())
		started, releaseSlow, healthyGate := make(chan struct{}), make(chan struct{}), make(chan struct{})
		var startOnce, releaseOnce, gateOnce sync.Once
		var slowHeld atomic.Bool
		release := func() { releaseOnce.Do(func() { close(releaseSlow) }) }
		defer release()
		providers := fleetWatchGuestReaders{}
		upgrades := map[string]*upgradev1.HostedClusterUpgrade{}
		for _, name := range []string{"slow-guest", "healthy-guest"} {
			hc := createAndSetStatusHostedCluster(ctx, name, namespace, regressionImage("4.20.0"))
			createReadyControlPlane(ctx, hc)
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, hc) })
			u := regressionHCU("")
			u.Name = name
			u.Namespace = namespace
			u.UID = ""
			u.Status = upgradev1.HostedClusterUpgradeStatus{}
			u.Spec.ControlPlane.StartAt = &metav1.Time{Time: time.Now().Add(time.Hour)}
			upgrades[name] = u
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, u); clearUpgradeGauges(namespace, name) })
			version := fixtureCV(regressionImage("4.20.0"), "a5e99fb2-0598-4b34-9081-14236b744c16")
			operators := fixtureOperators(regressionImage("4.20.0"))
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if request.Header.Get("Authorization") != "Bearer observer-token" {
					w.WriteHeader(http.StatusForbidden)
					return
				}
				if name == "slow-guest" {
					slowHeld.Store(true)
					defer slowHeld.Store(false)
					startOnce.Do(func() { close(started) })
					select {
					case <-releaseSlow:
					case <-request.Context().Done():
						return
					}
				}
				if request.URL.Path == "/apis/config.openshift.io/v1/clusterversions/version" {
					_ = json.NewEncoder(w).Encode(version)
				} else {
					_ = json.NewEncoder(w).Encode(configv1.ClusterOperatorList{TypeMeta: metav1.TypeMeta{Kind: "ClusterOperatorList", APIVersion: "config.openshift.io/v1"}, ListMeta: metav1.ListMeta{ResourceVersion: "1"}, Items: operators})
				}
			}))
			DeferCleanup(server.Close)
			config := &rest.Config{Host: server.URL, TLSClientConfig: rest.TLSClientConfig{CAData: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})}}
			data, err := observerKubeconfig(config, "observer-token")
			Expect(err).NotTo(HaveOccurred())
			providers[client.ObjectKeyFromObject(u)] = &watchGuestReader{legacyGuestAccess: &legacyGuestAccess{client: mgr.GetAPIReader()}, kubeconfig: data}
		}
		// Observe the ACTUAL persisted source-gate result even if the following
		// PendingSchedule observation replaces the purpose-specific condition.
		informer, err := mgr.GetCache().GetInformer(ctx, &upgradev1.HostedClusterUpgrade{}, cache.BlockUntilSynced(false))
		Expect(err).NotTo(HaveOccurred())
		_, err = informer.AddEventHandler(toolscache.ResourceEventHandlerFuncs{UpdateFunc: func(_, obj interface{}) {
			u, ok := obj.(*upgradev1.HostedClusterUpgrade)
			if ok && u.Namespace == namespace && u.Name == "healthy-guest" && u.Status.GuestAssessment != nil && u.Status.GuestAssessment.Purpose == string(guest.SourceBaseline) && u.Status.GuestAssessment.State == string(guest.Ready) && meta.IsStatusConditionTrue(u.Status.Conditions, "GuestAssessmentReady") {
				gateOnce.Do(func() { close(healthyGate) })
			}
		}})
		Expect(err).NotTo(HaveOccurred())
		r := &HostedClusterUpgradeReconciler{Client: mgr.GetClient(), APIReader: mgr.GetAPIReader(), Scheme: mgr.GetScheme(), HostedClusterNamespace: namespace, ReleaseResolver: fixtureReleaseResolver{}, GuestAccess: providers}
		Expect(r.setupWithManager(mgr, "upgrade-fleet-parallelism")).To(Succeed())
		Expect(k8sClient.Create(ctx, upgrades["slow-guest"])).To(Succeed())
		runCtx, stop := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- mgr.Start(runCtx) }()
		defer func() { release(); stop(); Eventually(done, 10*time.Second).Should(Receive(BeNil())) }()
		Eventually(started, 10*time.Second).Should(BeClosed())
		// Enqueue the second key only AFTER the first worker is inside real guest
		// network I/O. The whole assertion budget is shorter than the 5s request.
		deadline := 3 * time.Second
		began := time.Now()
		createCtx, cancel := context.WithTimeout(ctx, deadline)
		defer cancel()
		Expect(k8sClient.Create(createCtx, upgrades["healthy-guest"])).To(Succeed())
		remaining := deadline - time.Since(began)
		Expect(remaining).To(BeNumerically(">", 0))
		Eventually(healthyGate, remaining).Should(BeClosed(), "healthy HCU was serialized behind the blocked guest")
		Expect(slowHeld.Load()).To(BeTrue(), "healthy progress must precede slow request completion/timeout")
		select {
		case <-releaseSlow:
			Fail("slow guest released before healthy source gate")
		default:
		}
		fresh := &upgradev1.HostedClusterUpgrade{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(upgrades["healthy-guest"]), fresh)).To(Succeed())
		Expect(fresh.Status.Phase).To(Equal(upgradev1.PhasePendingSchedule))
		release()
	})
})
