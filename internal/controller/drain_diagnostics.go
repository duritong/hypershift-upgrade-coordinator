package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"strings"
	"time"

	upgradev1alpha1 "github.com/duritong/hypershift-upgrade-coordinator/api/v1alpha1"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	drainLogTimeout   = 3 * time.Second
	drainLogByteLimit = 256 * 1024
	drainLogLookback  = 15 * time.Minute
)

func readDrainLogs(r io.Reader) (string, error) {
	b, err := io.ReadAll(io.LimitReader(r, drainLogByteLimit+1))
	if err != nil {
		return "", fmt.Errorf("HCCO log read unavailable")
	}
	if len(b) > drainLogByteLimit {
		return "", fmt.Errorf("HCCO log byte limit exceeded")
	}
	return string(b), nil
}

func exposeDrainMetrics(hcu *upgradev1alpha1.HostedClusterUpgrade) {
	labels := prometheus.Labels{"namespace": hcu.Namespace, "name": hcu.Name}
	metricDrainAvailable.DeletePartialMatch(labels)
	metricDrainObservations.DeletePartialMatch(labels)
	if hcu.Status.Phase != upgradev1alpha1.PhaseUpgradingNodePools || hcu.Status.DrainDiagnostics == nil {
		return
	}
	d := hcu.Status.DrainDiagnostics
	available := 0.0
	if d.Available {
		available = 1
	}
	metricDrainAvailable.With(labels).Set(available)
	metricDrainObservations.With(labels).Set(float64(len(d.Messages)))
}

func (r *HostedClusterUpgradeReconciler) refreshDrainDiagnostics(ctx context.Context, hcu *upgradev1alpha1.HostedClusterUpgrade) {
	var since time.Time
	active, unknown := false, false
	for _, pool := range hcu.Status.NodePoolStatuses {
		if pool.Phase != "Upgrading" && pool.Phase != "Blocked" && pool.StartTime == nil {
			continue
		}
		active = true
		if pool.StartTime == nil {
			unknown = true
		} else if since.IsZero() || pool.StartTime.Time.Before(since) {
			since = pool.StartTime.Time
		}
	}
	hcu.Status.DrainDiagnostics = nil
	if !active {
		return
	}
	fallback := time.Now().Add(-drainLogLookback)
	if since.IsZero() || (unknown && fallback.Before(since)) {
		since = fallback
	}
	d := r.scrapeHCCOLogsForDrains(ctx, hcu.Namespace, hcu.Name, since)
	hcu.Status.DrainDiagnostics = &d
}

func (r *HostedClusterUpgradeReconciler) scrapeHCCOLogsForDrains(ctx context.Context, namespace, clusterName string, since time.Time) upgradev1alpha1.DrainDiagnosticsStatus {
	d := upgradev1alpha1.DrainDiagnosticsStatus{Reason: "ScraperNotConfigured"}
	if r.LogScraper == nil {
		return d
	}
	ctx, cancel := context.WithTimeout(ctx, drainLogTimeout)
	defer cancel()
	logs, err := r.LogScraper.GetHCCOLogs(ctx, WorkerControlPlaneNamespace(namespace, clusterName), "hosted-cluster-config-operator", since)
	if err != nil {
		d.Reason = "CollectionFailed"
		return d
	}
	d.Available, d.Reason = true, "Collected"
	d.Messages = parseDrainLogs(logs, since)
	return d
}

var textDrainNode = regexp.MustCompile(`(?:Node|name)="?([a-zA-Z0-9_.-]+)`)

// Kubernetes timestamps establish ordering and the cutoff. Untimestamped lines
// cannot be placed in the rollout interval and are deliberately excluded.
func parseDrainLogs(logs string, since time.Time) []string {
	type observation struct {
		node, message string
		resolved      bool
	}
	var observations []observation
	for _, line := range strings.Split(logs, "\n") {
		stamp, body, ok := strings.Cut(strings.TrimSpace(line), " ")
		ts, err := time.Parse(time.RFC3339Nano, stamp)
		if !ok || err != nil || ts.Before(since) {
			continue
		}
		msg, node := body, ""
		var fields map[string]interface{}
		if json.Unmarshal([]byte(body), &fields) == nil {
			msg, _ = fields["msg"].(string)
			if msg == "" {
				msg, _ = fields["message"].(string)
			}
			if n, ok := fields["Node"].(map[string]interface{}); ok {
				node, _ = n["name"].(string)
			}
			if node == "" {
				node, _ = fields["Node"].(string)
			}
			if node == "" {
				node, _ = fields["name"].(string)
			}
			if e, ok := fields["error"].(string); ok {
				msg += ": " + e
			}
		} else if match := textDrainNode.FindStringSubmatch(body); len(match) > 1 {
			node = match[1]
		}
		success := strings.Contains(msg, "Drain succeeded on node")
		if success && node != "" {
			for i := range observations {
				if observations[i].node == node {
					observations[i].resolved = true
				}
			}
			continue
		}
		if !strings.Contains(msg, "failed to drain node") && !strings.Contains(msg, "error when evicting pod") {
			continue
		}
		msg = strings.Join(strings.Fields(msg), " ")
		if node != "" {
			msg = "node " + node + ": " + msg
		}
		if len(msg) > 2048 {
			msg = msg[:2048]
		}
		observations = append(observations, observation{node: node, message: msg})
	}
	var messages []string
	seen := map[string]bool{}
	for i := len(observations) - 1; i >= 0 && len(messages) < 10; i-- {
		o := observations[i]
		if !o.resolved && !seen[o.message] {
			seen[o.message] = true
			messages = append(messages, o.message)
		}
	}
	return messages
}
