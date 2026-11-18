/*
Copyright 2026 SRE Fleet Team.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	upgradev1alpha1 "github.com/duritong/hypershift-upgrade-coordinator/api/v1alpha1"
	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

var gvkMachine = schema.GroupVersionKind{Group: "cluster.x-k8s.io", Version: "v1beta1", Kind: "Machine"}

const (
	maxDrainsPerNodePool     = 32
	maxDrainConditionMessage = 1024
	maxNodeBlockersSummary   = 128

	drainPhasePreDrainHook = "PreDrainHook"
	drainPhaseDraining     = "Draining"
	drainPhaseVolumeDetach = "VolumeDetach"
)

var (
	metricNodePoolDrainingMachines = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "hypershift_upgrade_nodepool_draining_machines",
			Help: "Number of CAPI Machines in a NodePool currently draining or waiting for volume detach.",
		},
		[]string{"namespace", "name", "nodepool"},
	)
	metricNodePoolDrainBlockedMachines = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "hypershift_upgrade_nodepool_drain_blocked_machines",
			Help: "Number of CAPI Machines in a NodePool reporting a blocked or failed node drain.",
		},
		[]string{"namespace", "name", "nodepool"},
	)
	metricNodePoolDrainDuration = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "hypershift_upgrade_nodepool_drain_duration_seconds",
			Help: "Longest active CAPI Machine drain or volume-detach elapsed duration in seconds for a NodePool.",
		},
		[]string{"namespace", "name", "nodepool"},
	)
	metricNodePoolForceDrainRemaining = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "hypershift_upgrade_nodepool_force_drain_remaining_seconds",
			Help: "Shortest remaining seconds until CAPI force-drain or volume-detach timeout expires for a draining Machine in a NodePool.",
		},
		[]string{"namespace", "name", "nodepool"},
	)
)

// machineDrainPredicate filters CAPI Machine watch events to deleting Machines
// so normal steady-state node heartbeats do not cause fleet reconcile churn.
var machineDrainPredicate = predicate.Funcs{
	CreateFunc: func(e event.CreateEvent) bool {
		return e.Object != nil && !e.Object.GetDeletionTimestamp().IsZero()
	},
	DeleteFunc: func(e event.DeleteEvent) bool {
		return e.Object != nil && !e.Object.GetDeletionTimestamp().IsZero()
	},
	UpdateFunc: func(e event.UpdateEvent) bool {
		if e.ObjectOld == nil || e.ObjectNew == nil {
			return false
		}
		oldDeleting := !e.ObjectOld.GetDeletionTimestamp().IsZero()
		newDeleting := !e.ObjectNew.GetDeletionTimestamp().IsZero()
		if !oldDeleting && !newDeleting {
			return false
		}
		if oldDeleting != newDeleting {
			return true
		}
		oldU, okOld := e.ObjectOld.(*unstructured.Unstructured)
		newU, okNew := e.ObjectNew.(*unstructured.Unstructured)
		if !okOld || !okNew {
			return true
		}
		return !reflect.DeepEqual(oldU.GetAnnotations(), newU.GetAnnotations()) ||
			!reflect.DeepEqual(oldU.Object["spec"], newU.Object["spec"]) ||
			!reflect.DeepEqual(oldU.Object["status"], newU.Object["status"])
	},
}

// collectClusterMachineDrains inspects deleting CAPI Machines in the HostedCluster's
// control-plane namespace and groups active drain/volume-detach telemetry by NodePool.
// Like HCCO drain logs, CAPI Machine drain telemetry is read-only diagnostic observation
// and never blocks worker actuation if Machine listing is unavailable.
func (r *HostedClusterUpgradeReconciler) collectClusterMachineDrains(ctx context.Context, hcu *upgradev1alpha1.HostedClusterUpgrade) map[string][]upgradev1alpha1.NodeDrainStatus {
	if r.Client == nil || hcu == nil {
		return nil
	}
	machines := &unstructured.UnstructuredList{}
	machines.SetGroupVersionKind(gvkMachine)
	cpNamespace := WorkerControlPlaneNamespace(hcu.Namespace, hcu.Name)
	if err := r.List(ctx, machines, client.InNamespace(cpNamespace)); err != nil {
		if meta.IsNoMatchError(err) {
			return nil
		}
		return nil
	}
	if len(machines.Items) == 0 {
		return nil
	}

	prefix := hcu.Namespace + "/"
	byPool := make(map[string][]upgradev1alpha1.NodeDrainStatus)
	for i := range machines.Items {
		m := &machines.Items[i]
		if m.GetDeletionTimestamp().IsZero() {
			continue
		}
		ref := m.GetAnnotations()[workerPoolAnnotation]
		if !strings.HasPrefix(ref, prefix) {
			continue
		}
		poolName := strings.TrimPrefix(ref, prefix)
		if poolName == "" || strings.Contains(poolName, "/") {
			continue
		}
		if d, active := inspectMachineDrain(m); active {
			byPool[poolName] = append(byPool[poolName], d)
		}
	}

	for pool, list := range byPool {
		sort.Slice(list, func(i, j int) bool {
			if list[i].Blocked != list[j].Blocked {
				return list[i].Blocked
			}
			return list[i].Machine < list[j].Machine
		})
		if len(list) > maxDrainsPerNodePool {
			list = list[:maxDrainsPerNodePool]
		}
		byPool[pool] = list
	}
	return byPool
}

type machineCondition struct {
	status             string
	reason             string
	message            string
	lastTransitionTime *metav1.Time
}

func inspectMachineDrain(m *unstructured.Unstructured) (upgradev1alpha1.NodeDrainStatus, bool) {
	conds := extractMachineConditions(m)
	nodeName, _, _ := unstructured.NestedString(m.Object, "status", "nodeRef", "name")
	nodeName = strings.TrimSpace(nodeName)
	delTS := m.GetDeletionTimestamp()

	if c, ok := conds["PreDrainDeleteHookSucceeded"]; ok && c.status == "False" {
		started := firstTimestamp(c.lastTransitionTime, delTS)
		return upgradev1alpha1.NodeDrainStatus{
			Machine:   m.GetName(),
			Node:      nodeName,
			Phase:     drainPhasePreDrainHook,
			Blocked:   true,
			StartedAt: started,
			Message:   sanitizeDrainText(firstNonEmpty(c.message, c.reason), maxDrainConditionMessage),
		}, true
	}

	if c, ok := conds["DrainingSucceeded"]; ok && c.status == "False" {
		drainStart := parseNestedRFC3339(m.Object, "status", "deletion", "nodeDrainStartTime")
		started := firstTimestamp(drainStart, c.lastTransitionTime, delTS)
		timeout := parseMachineTimeout(m.Object, "nodeDrainTimeout", "nodeDrainTimeoutSeconds")
		var forceAt *metav1.Time
		if started != nil && timeout != nil {
			cutoff := metav1.NewTime(started.Time.Add(timeout.Duration))
			forceAt = &cutoff
		}
		blocked := c.reason == "DrainingFailed" || isDrainMessageBlocked(c.message)
		return upgradev1alpha1.NodeDrainStatus{
			Machine:      m.GetName(),
			Node:         nodeName,
			Phase:        drainPhaseDraining,
			Blocked:      blocked,
			StartedAt:    started,
			Timeout:      timeout,
			ForceDrainAt: forceAt,
			Message:      sanitizeDrainText(firstNonEmpty(c.message, c.reason), maxDrainConditionMessage),
		}, true
	}

	if c, ok := conds["VolumeDetachSucceeded"]; ok && c.status == "False" {
		detachStart := parseNestedRFC3339(m.Object, "status", "deletion", "waitForNodeVolumeDetachStartTime")
		started := firstTimestamp(detachStart, c.lastTransitionTime, delTS)
		timeout := parseMachineTimeout(m.Object, "nodeVolumeDetachTimeout", "nodeVolumeDetachTimeoutSeconds")
		var forceAt *metav1.Time
		if started != nil && timeout != nil {
			cutoff := metav1.NewTime(started.Time.Add(timeout.Duration))
			forceAt = &cutoff
		}
		blocked := (c.reason != "" && c.reason != "WaitingForVolumeDetach") || isVolumeDetachMessageBlocked(c.message)
		return upgradev1alpha1.NodeDrainStatus{
			Machine:      m.GetName(),
			Node:         nodeName,
			Phase:        drainPhaseVolumeDetach,
			Blocked:      blocked,
			StartedAt:    started,
			Timeout:      timeout,
			ForceDrainAt: forceAt,
			Message:      sanitizeDrainText(firstNonEmpty(c.message, c.reason), maxDrainConditionMessage),
		}, true
	}

	return upgradev1alpha1.NodeDrainStatus{}, false
}

func extractMachineConditions(m *unstructured.Unstructured) map[string]machineCondition {
	out := make(map[string]machineCondition)
	readSlice := func(items []interface{}) {
		for _, item := range items {
			cm, ok := item.(map[string]interface{})
			if !ok {
				continue
			}
			typ, _ := cm["type"].(string)
			if typ == "" {
				continue
			}
			status, _ := cm["status"].(string)
			reason, _ := cm["reason"].(string)
			message, _ := cm["message"].(string)
			var ltt *metav1.Time
			if raw, _ := cm["lastTransitionTime"].(string); raw != "" {
				if parsed, err := time.Parse(time.RFC3339Nano, raw); err == nil {
					t := metav1.NewTime(parsed)
					ltt = &t
				}
			}
			out[typ] = machineCondition{
				status:             status,
				reason:             reason,
				message:            message,
				lastTransitionTime: ltt,
			}
		}
	}
	if v1b1, found, _ := unstructured.NestedSlice(m.Object, "status", "conditions"); found {
		readSlice(v1b1)
	}
	if v1b2, found, _ := unstructured.NestedSlice(m.Object, "status", "v1beta2", "conditions"); found {
		for _, item := range v1b2 {
			cm, ok := item.(map[string]interface{})
			if !ok {
				continue
			}
			typ, _ := cm["type"].(string)
			// Map CAPI v1beta2Deleting condition reasons if v1beta1 conditions are absent.
			if typ == "Deleting" {
				status, _ := cm["status"].(string)
				reason, _ := cm["reason"].(string)
				message, _ := cm["message"].(string)
				if status == "True" {
					switch reason {
					case "WaitingForPreDrainHook":
						if _, exists := out["PreDrainDeleteHookSucceeded"]; !exists {
							out["PreDrainDeleteHookSucceeded"] = machineCondition{status: "False", reason: "WaitingExternalHook", message: message}
						}
					case "DrainingNode":
						if _, exists := out["DrainingSucceeded"]; !exists {
							out["DrainingSucceeded"] = machineCondition{status: "False", reason: "Draining", message: message}
						}
					case "WaitingForVolumeDetach":
						if _, exists := out["VolumeDetachSucceeded"]; !exists {
							out["VolumeDetachSucceeded"] = machineCondition{status: "False", reason: "WaitingForVolumeDetach", message: message}
						}
					}
				}
			}
		}
	}
	return out
}

func parseNestedRFC3339(obj map[string]interface{}, fields ...string) *metav1.Time {
	raw, found, _ := unstructured.NestedString(obj, fields...)
	if !found || strings.TrimSpace(raw) == "" {
		return nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(raw))
	if err != nil {
		return nil
	}
	t := metav1.NewTime(parsed)
	return &t
}

func parseMachineTimeout(obj map[string]interface{}, v1b1Field, v1b2SecondsField string) *metav1.Duration {
	if raw, found, _ := unstructured.NestedString(obj, "spec", v1b1Field); found && strings.TrimSpace(raw) != "" {
		if d, err := time.ParseDuration(strings.TrimSpace(raw)); err == nil && d > 0 {
			return &metav1.Duration{Duration: d}
		}
	}
	if sec, found, err := unstructured.NestedInt64(obj, "spec", "deletion", v1b2SecondsField); err == nil && found && sec > 0 {
		return &metav1.Duration{Duration: time.Duration(sec) * time.Second}
	}
	return nil
}

func firstTimestamp(candidates ...*metav1.Time) *metav1.Time {
	for _, ts := range candidates {
		if ts != nil && !ts.IsZero() {
			cp := metav1.NewTime(ts.Time)
			return &cp
		}
	}
	return nil
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

func isDrainMessageBlocked(msg string) bool {
	lower := strings.ToLower(msg)
	for _, token := range []string{
		"cannot evict pod",
		"disruption budget",
		"pods failed eviction",
		"error when evicting",
		"error evicting",
		"failed to drain",
		"error draining",
		"unable to drain",
		"workload cluster is down",
	} {
		if strings.Contains(lower, token) {
			return true
		}
	}
	return false
}

func isVolumeDetachMessageBlocked(msg string) bool {
	lower := strings.ToLower(msg)
	return strings.Contains(lower, "error waiting for") || strings.Contains(lower, "failed to detach")
}

func sanitizeDrainText(s string, maxBytes int) string {
	clean := strings.Join(strings.Fields(s), " ")
	if len(clean) <= maxBytes {
		return clean
	}
	for len(clean) > maxBytes {
		_, size := utf8.DecodeLastRuneInString(clean)
		if size <= 0 {
			clean = clean[:maxBytes]
			break
		}
		clean = clean[:len(clean)-size]
	}
	return strings.TrimSpace(clean)
}

func drainCounts(drains []upgradev1alpha1.NodeDrainStatus) (draining int32, drainBlocked int32) {
	for _, d := range drains {
		draining++
		if d.Blocked {
			drainBlocked++
		}
	}
	return draining, drainBlocked
}

func formatCompactDuration(d time.Duration) string {
	if d <= 0 {
		return "0s"
	}
	if d >= time.Hour {
		h := int(d / time.Hour)
		m := int((d % time.Hour) / time.Minute)
		if m == 0 {
			return fmt.Sprintf("%dh", h)
		}
		return fmt.Sprintf("%dh%dm", h, m)
	}
	if d >= time.Minute {
		return fmt.Sprintf("%dm", int(d.Round(time.Minute)/time.Minute))
	}
	return fmt.Sprintf("%ds", int(d.Round(time.Second)/time.Second))
}

func formatMachineDrainBlocker(d upgradev1alpha1.NodeDrainStatus, now time.Time) string {
	target := "machine " + d.Machine
	if d.Node != "" {
		target = "node " + d.Node
	}
	var window []string
	if d.StartedAt != nil && now.After(d.StartedAt.Time) {
		window = append(window, formatCompactDuration(now.Sub(d.StartedAt.Time))+" elapsed")
	}
	if d.ForceDrainAt != nil {
		if now.Before(d.ForceDrainAt.Time) {
			window = append(window, "force cutoff in "+formatCompactDuration(d.ForceDrainAt.Time.Sub(now)))
		} else {
			window = append(window, "force cutoff expired")
		}
	} else if d.Phase == drainPhaseDraining {
		window = append(window, "no force-drain timeout")
	}
	stage := "drain"
	switch d.Phase {
	case drainPhasePreDrainHook:
		stage = "pre-drain hook"
	case drainPhaseVolumeDetach:
		stage = "volume detach"
	}
	prefix := fmt.Sprintf("CAPI %s on %s blocked", stage, target)
	if len(window) > 0 {
		prefix += " (" + strings.Join(window, ", ") + ")"
	}
	if d.Message != "" {
		prefix += ": " + d.Message
	}
	return sanitizeDrainText(prefix, maxDrainConditionMessage)
}

// computeNodeBlockersSummary builds a bounded one-column summary for
// `oc get hostedclusterupgrade` across CAPI Machine drains, blocked NodePools,
// and HCCO drain log observations.
func computeNodeBlockersSummary(hcu *upgradev1alpha1.HostedClusterUpgrade, now time.Time) string {
	if hcu == nil {
		return "None"
	}
	switch hcu.Status.Phase {
	case "", upgradev1alpha1.PhaseIdle, upgradev1alpha1.PhaseInstalling,
		upgradev1alpha1.PhasePreflightChecks, upgradev1alpha1.PhasePendingSchedule,
		upgradev1alpha1.PhaseUpgradingControlPlane, upgradev1alpha1.PhaseVerifyingControlPlane,
		upgradev1alpha1.PhaseCompleted:
		return "None"
	}

	var drainBlocked, draining, blockedPools int32
	var shortestBlockedCutoff, shortestActiveCutoff *time.Duration
	for _, pool := range hcu.Status.NodePoolStatuses {
		if pool.Phase == "Blocked" || (pool.Phase == "Completed" && len(pool.Blockers) > 0) {
			blockedPools++
		}
		for _, d := range pool.Drains {
			draining++
			var rem *time.Duration
			if d.ForceDrainAt != nil {
				diff := d.ForceDrainAt.Time.Sub(now)
				if diff < 0 {
					diff = 0
				}
				rem = &diff
			}
			if d.Blocked {
				drainBlocked++
				if rem != nil && (shortestBlockedCutoff == nil || *rem < *shortestBlockedCutoff) {
					v := *rem
					shortestBlockedCutoff = &v
				}
			} else if rem != nil && (shortestActiveCutoff == nil || *rem < *shortestActiveCutoff) {
				v := *rem
				shortestActiveCutoff = &v
			}
		}
	}

	var summary string
	switch {
	case drainBlocked > 0:
		noun := "node drain"
		if drainBlocked > 1 {
			noun = "node drains"
		}
		summary = fmt.Sprintf("%d %s blocked", drainBlocked, noun)
		if shortestBlockedCutoff != nil {
			if *shortestBlockedCutoff == 0 {
				summary += " (force due)"
			} else {
				summary += fmt.Sprintf(" (force in %s)", formatCompactDuration(*shortestBlockedCutoff))
			}
		}
		if blockedPools > 0 {
			summary += fmt.Sprintf(", %d pool(s) blocked", blockedPools)
		}
	case blockedPools > 0:
		summary = fmt.Sprintf("%d/%d pools blocked", blockedPools, len(hcu.Status.NodePoolStatuses))
	case hcu.Status.DrainDiagnostics != nil && len(hcu.Status.DrainDiagnostics.Messages) > 0:
		n := len(hcu.Status.DrainDiagnostics.Messages)
		if n == 1 {
			summary = "Drain eviction failing"
		} else {
			summary = fmt.Sprintf("Drain eviction failing (%d hints)", n)
		}
	case draining > 0:
		if shortestActiveCutoff != nil && *shortestActiveCutoff > 0 {
			summary = fmt.Sprintf("None (%d draining, force in %s)", draining, formatCompactDuration(*shortestActiveCutoff))
		} else {
			summary = fmt.Sprintf("None (%d draining)", draining)
		}
	default:
		summary = "None"
	}
	return sanitizeDrainText(summary, maxNodeBlockersSummary)
}

func summarizeActiveWorkerMessage(hcu *upgradev1alpha1.HostedClusterUpgrade, pauseBlocked bool, now time.Time) string {
	if pauseBlocked {
		return "Worker NodePool upgrades blocked by independent pauses or lock ownership; see NodePool blockers."
	}
	for _, pool := range hcu.Status.NodePoolStatuses {
		for _, d := range pool.Drains {
			if d.Blocked {
				return sanitizeDrainText(fmt.Sprintf("Worker NodePool '%s': %s", pool.Name, formatMachineDrainBlocker(d, now)), 512)
			}
		}
	}
	for _, pool := range hcu.Status.NodePoolStatuses {
		if pool.Phase == "Blocked" && len(pool.Blockers) > 0 {
			return sanitizeDrainText(fmt.Sprintf("Worker NodePool '%s' blocked: %s", pool.Name, pool.Blockers[0]), 512)
		}
	}
	if hcu.Status.DrainDiagnostics != nil && len(hcu.Status.DrainDiagnostics.Messages) > 0 {
		return sanitizeDrainText(fmt.Sprintf("Worker NodePool upgrades actively rolling out; drain issue observed (%s).", hcu.Status.DrainDiagnostics.Messages[0]), 512)
	}
	return "Worker NodePool upgrades actively rolling out."
}

func clearMachineDrainMetrics(namespace, name string) {
	labels := prometheus.Labels{"namespace": namespace, "name": name}
	metricNodePoolDrainingMachines.DeletePartialMatch(labels)
	metricNodePoolDrainBlockedMachines.DeletePartialMatch(labels)
	metricNodePoolDrainDuration.DeletePartialMatch(labels)
	metricNodePoolForceDrainRemaining.DeletePartialMatch(labels)
}

func exposeMachineDrainMetrics(hcu *upgradev1alpha1.HostedClusterUpgrade, now time.Time) {
	clearMachineDrainMetrics(hcu.Namespace, hcu.Name)
	if hcu.Status.Phase != upgradev1alpha1.PhaseUpgradingNodePools && hcu.Status.Phase != upgradev1alpha1.PhasePaused {
		return
	}
	for _, pool := range hcu.Status.NodePoolStatuses {
		draining, blocked := drainCounts(pool.Drains)
		metricNodePoolDrainingMachines.WithLabelValues(hcu.Namespace, hcu.Name, pool.Name).Set(float64(draining))
		metricNodePoolDrainBlockedMachines.WithLabelValues(hcu.Namespace, hcu.Name, pool.Name).Set(float64(blocked))

		var maxElapsed float64
		var hasElapsed bool
		var minRemaining float64
		var hasRemaining bool
		for _, d := range pool.Drains {
			if d.StartedAt != nil {
				elapsed := now.Sub(d.StartedAt.Time).Seconds()
				if elapsed < 0 {
					elapsed = 0
				}
				if !hasElapsed || elapsed > maxElapsed {
					maxElapsed = elapsed
					hasElapsed = true
				}
			}
			if d.ForceDrainAt != nil {
				rem := d.ForceDrainAt.Time.Sub(now).Seconds()
				if rem < 0 {
					rem = 0
				}
				if !hasRemaining || rem < minRemaining {
					minRemaining = rem
					hasRemaining = true
				}
			}
		}
		if hasElapsed {
			metricNodePoolDrainDuration.WithLabelValues(hcu.Namespace, hcu.Name, pool.Name).Set(maxElapsed)
		}
		if hasRemaining {
			metricNodePoolForceDrainRemaining.WithLabelValues(hcu.Namespace, hcu.Name, pool.Name).Set(minRemaining)
		}
	}
}
