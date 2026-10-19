package controller

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	upgradev1alpha1 "github.com/duritong/hypershift-upgrade-coordinator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestBoundedHCCOLogs(t *testing.T) {
	for _, mode := range []string{"ok", "oversized", "hung", "request-timeout", "error", "discovery-hung"} {
		t.Run(mode, func(t *testing.T) {
			since := time.Now().Add(-time.Minute).Truncate(time.Second)
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if !strings.HasSuffix(r.URL.Path, "/log") {
					if mode == "discovery-hung" {
						<-r.Context().Done()
						return
					}
					w.Header().Set("Content-Type", "application/json")
					fmt.Fprint(w, `{"apiVersion":"v1","kind":"PodList","items":[{"metadata":{"name":"hosted-cluster-config-operator-1"},"status":{"phase":"Running"}}]}`)
					return
				}
				q := r.URL.Query()
				if q.Get("timestamps") != "true" || q.Get("limitBytes") != "262144" || q.Get("sinceTime") != since.Format(time.RFC3339) {
					t.Errorf("unbounded request: %v", q)
				}
				switch mode {
				case "hung", "request-timeout":
					w.WriteHeader(200)
					w.(http.Flusher).Flush()
					<-r.Context().Done()
				case "oversized":
					fmt.Fprint(w, strings.Repeat("x", drainLogByteLimit+1))
				case "error":
					http.Error(w, "SECRET upstream details", 403)
				default:
					fmt.Fprint(w, "recent logs")
				}
			}))
			defer s.Close()
			client, err := kubernetes.NewForConfig(&rest.Config{Host: s.URL})
			if err != nil {
				t.Fatal(err)
			}
			timeout := 150 * time.Millisecond
			if mode == "request-timeout" {
				timeout = 2 * drainLogTimeout
			}
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			start := time.Now()
			logs, err := (&KubeLogScraper{Client: client}).GetHCCOLogs(ctx, "ns", "hosted-cluster-config-operator", since)
			if mode == "ok" {
				if err != nil || logs != "recent logs" {
					t.Fatalf("%q %v", logs, err)
				}
				return
			}
			if err == nil || logs != "" || strings.Contains(err.Error(), "SECRET") {
				t.Fatalf("unsafe result %q %v", logs, err)
			}
			bound := time.Second
			if mode == "request-timeout" {
				bound = drainLogTimeout + time.Second
			}
			if time.Since(start) > bound {
				t.Fatal("cancellation did not bound I/O")
			}
		})
	}
}

type diagnosticScraper struct {
	logs  string
	err   error
	calls int
	since time.Time
}

func (s *diagnosticScraper) GetHCCOLogs(_ context.Context, _, _ string, since time.Time) (string, error) {
	s.calls++
	s.since = since
	return s.logs, s.err
}

func TestDrainLogTruthfulness(t *testing.T) {
	since := time.Date(2026, 10, 19, 1, 0, 0, 0, time.UTC)
	line := func(offset int, body string) string {
		return since.Add(time.Duration(offset)*time.Second).Format(time.RFC3339Nano) + " " + body + "\n"
	}
	logs := line(-1, `{"msg":"failed to drain node","Node":{"name":"historic"}}`)
	logs += line(1, `{"msg":"failed to drain node","Node":{"name":"resolved"},"error":"drain failed: failed to drain node resolved: global timeout reached"}`)
	logs += line(2, `{"msg":"error when evicting pods/\"db-0\" -n \"app\" (will retry after 5s): Cannot evict pod as it would violate the pod's disruption budget.","Node":{"name":"resolved"}}`)
	logs += line(3, `{"msg":"Drain succeeded on node","name":"resolved","Node":{"name":"resolved"}}`)
	logs += line(4, `{"msg":"Pod drained","Node":{"name":"still-failing"},"Pod name":"db-0"}`)
	logs += line(5, `{"msg":"failed to drain node","Node":{"name":"still-failing"},"error":"timeout"}`)
	logs += line(6, `{"msg":"error when evicting pods/\"web-0\" -n \"app\" (will retry after 5s): Cannot evict pod as it would violate the pod's disruption budget."}`)
	logs += line(7, `{"msg":"error when evicting pods/\"web-0\" -n \"app\" (will retry after 5s): Cannot evict pod as it would violate the pod's disruption budget."}`)
	got := parseDrainLogs(logs, since)
	if len(got) != 2 || !strings.Contains(got[0], "error when evicting") || !strings.Contains(got[1], "still-failing") {
		t.Fatalf("incorrect observations: %v", got)
	}
	// Success clears the whole node drain, not unrelated or unattributed errors.
	logs += line(8, `{"msg":"Drain succeeded on node","name":"still-failing"}`)
	if got = parseDrainLogs(logs, since); len(got) != 1 || !strings.Contains(got[0], "web-0") {
		t.Fatalf("incorrect resolution: %v", got)
	}
	for i := 9; i < 25; i++ {
		logs += line(i, fmt.Sprintf(`{"msg":"failed to drain node","Node":{"name":"node-%d"}}`, i))
	}
	got = parseDrainLogs(logs, since)
	if len(got) != 10 || !strings.Contains(got[0], "node-24") || !strings.Contains(got[9], "node-15") {
		t.Fatalf("not bounded latest observations: %v", got)
	}
	if got := parseDrainLogs("failed to drain node without timestamp", since); len(got) != 0 {
		t.Fatal(got)
	}
}

func TestDrainDiagnosticsRefresh(t *testing.T) {
	clock := &flowClock{at: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	start := metav1.NewTime(clock.Now().Add(-time.Hour))
	hcu := regressionHCU(upgradev1alpha1.PhaseUpgradingNodePools)
	hcu.Status.NodePoolStatuses = []upgradev1alpha1.NodePoolUpgradeStatus{{Name: "a", Phase: "Upgrading", StartTime: &start}, {Name: "b", Phase: "Upgrading", StartTime: &start}}
	s := &diagnosticScraper{logs: clock.Now().Format(time.RFC3339Nano) + ` {"msg":"failed to drain node","Node":{"name":"unknown-pool-node"}}`}
	r := &HostedClusterUpgradeReconciler{LogScraper: s, Clock: clock}
	r.refreshDrainDiagnostics(context.Background(), hcu)
	if s.calls != 1 || !s.since.Equal(start.Time) || !hcu.Status.DrainDiagnostics.Available || len(hcu.Status.DrainDiagnostics.Messages) != 1 {
		t.Fatalf("incorrect collection: %+v %+v", s, hcu.Status.DrainDiagnostics)
	}
	first := hcu.Status.DeepCopy()
	r.refreshDrainDiagnostics(context.Background(), hcu)
	if !reflect.DeepEqual(*first, hcu.Status) {
		t.Fatal("unchanged diagnostics changed status")
	}
	for _, pool := range hcu.Status.NodePoolStatuses {
		if len(pool.Blockers) > 0 {
			t.Fatal("unattributed pool blocker")
		}
	}
	s.err = fmt.Errorf("SECRET arbitrary upstream response")
	r.refreshDrainDiagnostics(context.Background(), hcu)
	d := hcu.Status.DrainDiagnostics
	if d.Available || d.Reason != "CollectionFailed" || len(d.Messages) != 0 {
		t.Fatalf("failure conflated with empty success: %+v", d)
	}
	r.LogScraper = nil
	r.refreshDrainDiagnostics(context.Background(), hcu)
	if d = hcu.Status.DrainDiagnostics; d.Available || d.Reason != "ScraperNotConfigured" {
		t.Fatal(d)
	}
	r.LogScraper = s
	hcu.Status.NodePoolStatuses = []upgradev1alpha1.NodePoolUpgradeStatus{{Name: "external", Phase: "Blocked"}}
	r.refreshDrainDiagnostics(context.Background(), hcu)
	if !s.since.Equal(clock.Now().Add(-drainLogLookback)) {
		t.Fatal("unknown start did not use recent fallback")
	}
	clock.Step(time.Hour)
	r.refreshDrainDiagnostics(context.Background(), hcu)
	if !s.since.Equal(clock.Now().Add(-drainLogLookback)) {
		t.Fatal("diagnostic lookback did not advance with policy time")
	}
	hcu.Status.NodePoolStatuses[0].Phase = "Completed"
	r.refreshDrainDiagnostics(context.Background(), hcu)
	if hcu.Status.DrainDiagnostics != nil {
		t.Fatal("completed rollout retained observations")
	}
}

func TestDiagnosticFailureDoesNotPreventWorkerActions(t *testing.T) {
	for _, configured := range []bool{true, false} {
		t.Run(fmt.Sprintf("configured=%t", configured), func(t *testing.T) {
			hcu := regressionHCU(upgradev1alpha1.PhaseUpgradingNodePools)
			hcu.Spec.RolloutPolicy = &upgradev1alpha1.RolloutPolicySpec{MaxParallelNodePools: 2}
			base := &regressionClient{objects: append([]client.Object{hcu, regressionHC("4.21.0")}, quiescentWorkerFixtures(regressionNP("a", ""), regressionNP("b", ""))...)}
			addReadyCP(base, regressionHC("4.21.0"))
			s := &diagnosticScraper{err: fmt.Errorf("unavailable")}
			r := &HostedClusterUpgradeReconciler{Client: base, APIReader: workerSource(base, hcu), LogScraper: s, ReleaseResolver: fixtureReleaseResolver{}, GuestAccess: legacyGuest(base, hcu, nil)}
			if !configured {
				r.LogScraper = nil
			}
			if err := r.lockClusterNodePools(context.Background(), hcu, regressionHC("4.21.0")); err != nil {
				t.Fatal(err)
			}
			hc := regressionHC("4.21.0")
			if _, _, err := r.reconcileNodePools(context.Background(), hcu, hc); err != nil {
				t.Fatal(err)
			}
			if configured && s.calls != 1 {
				t.Fatalf("scraped %d times for parallel pools", s.calls)
			}
			for _, name := range []string{"a", "b"} {
				np := &unstructured.Unstructured{}
				np.SetGroupVersionKind(gvkNP)
				if err := base.Get(context.Background(), client.ObjectKey{Namespace: hcu.Namespace, Name: name}, np); err != nil {
					t.Fatal(err)
				}
				image, _, _ := unstructured.NestedString(np.Object, "spec", "release", "image")
				if image != hcu.Spec.TargetReleaseImage {
					t.Fatalf("diagnostic failure prevented rollout %s", name)
				}
			}
			if hcu.Status.DrainDiagnostics == nil || hcu.Status.DrainDiagnostics.Available {
				t.Fatal("diagnostic unavailability missing")
			}
		})
	}
}

func TestDrainMetricsCleanup(t *testing.T) {
	hcu := regressionHCU(upgradev1alpha1.PhaseUpgradingNodePools)
	t.Cleanup(func() { clearUpgradeGauges(hcu.Namespace, hcu.Name) })
	hcu.Status.DrainDiagnostics = &upgradev1alpha1.DrainDiagnosticsStatus{Available: true, Messages: []string{"hint"}}
	exposeDrainMetrics(hcu)
	if got := collectedGauges(t, metricDrainObservations); len(got) != 1 || got[0].GetGauge().GetValue() != 1 {
		t.Fatal(got)
	}
	hcu.Status.DrainDiagnostics = &upgradev1alpha1.DrainDiagnosticsStatus{Reason: "CollectionFailed"}
	exposeDrainMetrics(hcu)
	if got := collectedGauges(t, metricDrainObservations); len(got) != 1 || got[0].GetGauge().GetValue() != 0 {
		t.Fatal(got)
	}
	hcu.Status.Phase = upgradev1alpha1.PhasePaused
	exposeDrainMetrics(hcu)
	if len(collectedGauges(t, metricDrainAvailable)) != 0 || len(collectedGauges(t, metricDrainObservations)) != 0 {
		t.Fatal("inactive stale gauges")
	}
}
