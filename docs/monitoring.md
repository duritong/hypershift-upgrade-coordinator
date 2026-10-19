# HyperShift Upgrade Coordinator Monitoring & Alerting

This document describes the SRE alert rules and Grafana dashboard for the HyperShift Upgrade Coordinator.

---

## 1. SRE Prometheus Alert Rules

All alerting configurations are natively managed and deployed via Kustomize. SREs can inspect or apply the complete raw Prometheus manifest directly from the source file at [**`config/prometheus/prometheus_rule.yaml`**](../config/prometheus/prometheus_rule.yaml).

### Summary of Alert Rules

| Alert Name | Prometheus Expression | Severity | Description / SRE Action |
| :--- | :--- | :--- | :--- |
| **HypershiftUpgradeCoordinatorBlocked** | `hypershift_upgrade_phase == 7` | **Critical (Pager)** | The cluster upgrade is currently in a `Blocked` phase. Check preflight health status, failing conditions, or validation errors in the upgrade CR. |
| **HypershiftUpgradeCoordinatorNodePoolStuck** | `hypershift_upgrade_nodepool_blocked == 1` | **Warning** | A pool reports an assessment or orchestration blocker. Inspect NodePool status; HCCO logs are not attributed to pools. |
| **HypershiftUpgradeDrainObservations** | `hypershift_upgrade_drain_observations > 0` | **Warning** | Recent cluster-level drain/eviction hints persist. Inspect `status.drainDiagnostics` and guest resources; exact NodePool/PDB ownership is not established. |
| **HypershiftUpgradeDrainDiagnosticsUnavailable** | `hypershift_upgrade_drain_diagnostics_available == 0` | **Warning** | Best-effort collection is unavailable, not evidence of healthy drains. Rollout actions continue. |
| **HypershiftUpgradeStepDurationLong** | `hypershift_upgrade_step_duration_seconds > 14400` | **Warning** | An individual upgrade step (e.g., Control Plane rollout) has been active for more than 4 hours. Inspect underlying component rollout readiness. |

---

## 2. Grafana Dashboard Specification

The two drain gauges use only `namespace` and `name` labels and are separate from the dashboard's per-pool blocker metric. Availability is 1 for successful collection and 0 for unavailable collection; observation count is bounded to ten and resets to zero on unavailable collection. Both series are removed outside active worker reconciliation phases and on deletion. They reflect the most recent worker reconciliation, not a continuous log stream. See the [user guide](user-guide.md#troubleshooting-blocked-node-drains) for bounds, freshness, and attribution limits.

Import [the dashboard JSON](monitoring/grafana-dashboard.json) into Grafana and select a Prometheus datasource scraping the coordinator's `/metrics` endpoint. The dashboard refreshes every **30 seconds**. Instant panels show the latest samples at the selected time range's end; rollout curves show history across that range.

### Dashboard Variables

| Variable | Type | Purpose / Query |
| :--- | :--- | :--- |
| `$datasource` | Datasource (`prometheus`) | Selects the datasource used by every panel and version variable. |
| `$current_version` | Query, multi-select with All | `label_values(hypershift_upgrade_phase, current_version)` |
| `$target_version` | Query, multi-select with All | `label_values(hypershift_upgrade_phase, target_version)` |

Version filters use the labels exported by the coordinator. `current_version` is intended to represent the upgrade's origin version; retaining that origin throughout a rollout is a controller responsibility, not something the dashboard reconstructs.

### Dashboard Panels

| Panel | Behavior |
| :--- | :--- |
| Completed Upgrades | Instant count of matching upgrades in phase 6; not an all-phase fleet summary. |
| Total Active Upgrades | Instant count of matching upgrades in phases 3, 4, or 5 (control-plane rollout, control-plane verification, or worker rollout). Both stats return zero when no matching series exist. |
| Fleet Upgrade Current State | A single instant Prometheus table query exposes namespace, name, current/target/intermediate version labels and phase. It includes all matching phases, not just active upgrades. Phase values are mapped to readable names. It is not a joined duration/step matrix. |
| Upgrade Durations (Seconds) | Instant bar gauges for positive overall and ControlPlane step durations, in seconds. These are not restricted to currently active phases. |
| Control Plane Component Rollout Statuses | Instant table of component readiness for matching upgrades: 0 = unavailable, 1 = rolling, 2 = ready. |
| NodePool Upgrade Rollout Ratio Curve | Historical per-pool rollout percentages, with namespace/upgrade/pool identities in the legend. |
| Aggregated Cluster Node Rollout Curve | Historical per-cluster worker rollout percentages. |
| Blocked NodePools | Instant table listing matching pools whose blocked metric is 1. An empty table means no matching blocked pools are reported, not proof that every pool is healthy. |

Current-state tables use `instant: true`, `range: false`, and `format: "table"`. The fleet state table uses only the phase metric, so its labels and value form one table without cross-query joins or transformations. Component and blocked-pool tables likewise use a single instant query. Only the rollout curves use range queries.

Phase mapping: 0 = idle, 1 = installing, 2 = preflight checks or pending schedule, 3 = upgrading control plane, 4 = verifying control plane, 5 = upgrading NodePools, 6 = completed, 7 = blocked, 8 = paused.

### Query Contracts

The active count applies both version filters to both bounds and includes worker upgrades (phase 5):

```promql
count(
  (hypershift_upgrade_phase{current_version=~"$current_version", target_version=~"$target_version"} >= 3)
  and
  (hypershift_upgrade_phase{current_version=~"$current_version", target_version=~"$target_version"} <= 5)
) or vector(0)
```

Worker rollout ratios have only identity labels: `namespace`, `name`, and (for per-pool ratios) `nodepool`. They do **not** have version labels. Filter them using a set intersection with the version-filtered phase metric:

```promql
(
  hypershift_upgrade_nodepool_rollout_ratio
  and on(namespace, name)
  hypershift_upgrade_phase{current_version=~"$current_version", target_version=~"$target_version"}
) * 100
```

The aggregated curve uses the same expression with `hypershift_upgrade_aggregated_node_rollout_ratio`. The component readiness table also uses this identity intersection. `and on(namespace, name)` retains the left-hand values and labels, supports multiple pools/components per upgrade, and does not multiply ratios by the numeric phase. Matching both namespace and name avoids collisions between same-named upgrades in different namespaces. These intersections select versions, not active phases.

Unlike the ratios, the blocked-pool metric already exports version labels and can be filtered directly:

```promql
hypershift_upgrade_nodepool_blocked{current_version=~"$current_version", target_version=~"$target_version"} == 1
```

The queries assume `namespace` is the upgrade resource's namespace. Scrape configuration must preserve that metric label rather than replace it with the coordinator pod's namespace (for example, via an appropriate `honorLabels` or relabeling policy). Dashboard queries do not resolve scrape-label conflicts.

### Regression Validation

Run `go test ./internal/controller -run '^TestGrafanaDashboard$' -count=1` to validate the JSON, datasource wiring, version variables, complete query contracts, worker label selectors, instant/table versus range query modes, blocked-pool panel, and refresh cadence without starting the controller envtest suite. These are static dashboard contract tests, not live Prometheus query evaluation or Grafana rendering tests.
