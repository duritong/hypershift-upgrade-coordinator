package controller

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

func TestBoundedHCCOLogs(t *testing.T) {
	for _, mode := range []string{"ok", "oversized", "hung", "error", "discovery-hung"} {
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
				case "hung":
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
			ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
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
			if time.Since(start) > time.Second {
				t.Fatal("cancellation did not bound I/O")
			}
		})
	}
}
