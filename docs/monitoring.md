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
| **HypershiftUpgradeGuestCredentialUnavailable** | Credential availability = 0, relevant phase | **Warning, 5m** | No reported unexpired observer credential; inspect guestAccess, source kubeconfig and endpoint/ownership. |
| **HypershiftUpgradeGuestMaintenanceDegraded** | Maintenance ready = 0, relevant phase | **Warning, 10m** | Renewal/bootstrap is failing; an existing valid token can still work until expiry. |
| **HypershiftUpgradeGuestReadStale** | Last scoped collection older than 120s or never, relevant phase | **Warning, 5m** | Collection freshness failure, not proof of guest ill health. |
| **HypershiftUpgradeGuestAssessmentBlocked** | Hard failure/unknown/stale purpose verdict, relevant phase | **Warning, 10m** | Inspect the latest purpose-specific assessment; healthy progression alone is not alerted. |

Guest alerts are intersected with phases 2-5 or 7 using `and on(namespace, name)`:
preflight/schedule, control-plane rollout/verification, workers and Blocked.
Idle, Installing, Completed and Paused are excluded. These are coordinator
evidence/access alerts, **not guest firing-alert queries**. Querying guest alert
sets is deferred. Existing Completed outcomes remain historical; continuing
credential maintenance does not reopen an upgrade or promise ongoing health.

---

## 2. Grafana Dashboard Specification

The two drain gauges use only `namespace` and `name` labels and are separate from the dashboard's per-pool blocker metric. Availability is 1 for successful collection and 0 for unavailable collection; observation count is bounded to ten and resets to zero on unavailable collection. Both series are removed outside active worker reconciliation phases and on deletion. They reflect the most recent worker reconciliation, not a continuous log stream. See the [user guide](user-guide.md#troubleshooting-blocked-node-drains) for bounds, freshness, and attribution limits.

Import [the dashboard JSON](monitoring/grafana-dashboard.json) into Grafana and select a Prometheus datasource scraping the coordinator's `/metrics` endpoint. The dashboard refreshes every **30 seconds**. Instant panels show the latest samples at the selected time range's end; rollout curves show history across that range.

The coordinator refreshes non-completed upgrades at least every 30 seconds, even
when upstream status is quiet. Duration gauges therefore advance during stalls.
Overall duration is wall-clock age since the upgrade began, including schedule
waits. Step duration starts at actuation and excludes pre-start scheduling, but
includes coordinator pauses because HyperShift work can continue independently.
Completed steps stop exporting active-duration series; these gauges are not a
durable history of completed step durations.

### Dashboard Variables

| Variable | Type | Purpose / Query |
| :--- | :--- | :--- |
| `$datasource` | Datasource (`prometheus`) | Selects the datasource used by every panel and version variable. |
| `$current_version` | Query, multi-select with All | `label_values(hypershift_upgrade_phase, current_version)` |
| `$target_version` | Query, multi-select with All | `label_values(hypershift_upgrade_phase, target_version)` |
| `$guest_purpose` | Query, multi-select with All | `label_values(hypershift_upgrade_guest_assessment, purpose)`; filters verdict and operator-blocker panels, not credential/read-age gauges. |

Version filters use the labels exported by the coordinator. `current_version`
comes from persisted `status.initialVersion`, the upgrade run's origin version.
It does not change when intermediate or target control-plane releases complete.
Worker blocked gauges use the same origin label so fleet filters remain valid
even when a pool started at a different version. A new run after completion
records its own origin. Existing runs without an origin record their first
observable version after the coordinator is updated.

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
| Observer Credential Availability / Guest Access Maintenance Ready | Separate instant tables for credential presence/expiry and maintenance. Neither proves successful observer authentication. |
| Latest Guest Purpose Verdict | Instant table of latest purpose/state/controlled reason. Value is 1; absence for Paused/Completed is intentional. Not an all-purpose health promise. |
| Scoped Guest Read Age | Seconds since successful scoped collection, even when unhealthy. A never-observed timestamp is zero, producing a large age. Historical age remains visible after completion. |
| Observer Credential Remaining Lifetime | Expiration epoch minus current time, seconds; omitted when expiration is unknown. |
| Guest Assessment Blocker Count / Assessed Guest Operator Count | Latest fresh verdict totals; not an expected inventory or independent readiness proof. |
| Guest Operator Blockers | At most 32 current blocker details. Old reasons/names disappear on the next metric update; removed COs disappear after the next complete collection. Absence does not prove health. |

### Guest Metric Contract

All guest gauges use `namespace` and HCU `name`, without version labels. Additional
labels appear only where listed. Version filtering uses the same identity set
intersection with the phase metric as worker ratios, **never multiplication by
the numeric phase**. `honorLabels: true` remains required.

| Metric suffix (prefix `hypershift_upgrade_guest_`) | Meaning / Extra labels |
| :--- | :--- |
| `credential_available` | 0/1; access status reports available and its reported expiry is still future. Missing/expired evidence is 0. Not proof of actual auth. |
| `access_maintenance_ready` | 0/1; latest current-generation maintenance result. Renewal failure does not imply old credential rejection. |
| `credential_expiration_timestamp_seconds` | Unix seconds; absent when unknown. Access status may omit expiry once unavailable. |
| `last_successful_observation_timestamp_seconds` | Unix seconds of last successful collection, including unhealthy results; 0 if none recorded. Retained diagnostics can outlive current usability. |
| `assessment_timestamp_seconds` | Unix seconds of latest attempt, or 0 if absent. Distinct from CO condition transition times. |
| `assessment` | One current series, value 1; `purpose`, `state`, `reason`. A closed purpose/state/reason vocabulary; unrecognized reasons become `Other`. |
| `assessment_ready` | 0/1 for latest fresh, current-plan Ready verdict; `purpose`. Requires a recent successful read; known credential expiry invalidates it. Independent of maintenance/availability booleans. Not authorization: every action still recollects/revalidates. |
| `operator_count` | Latest fresh verdict's operator summaries, bounded to 256. Failed reads/unsupported evidence may have zero summaries; not a full-list success signal. |
| `blocker_count` | Total latest fresh verdict blockers, including omitted details; can exceed operator count because an operator can have several blockers. |
| `blockers_truncated` | 0/1 indicating bounded details were omitted. |
| `operator_blocker` | Value 1 per reported detail; `purpose`, `operator`, controlled `reason`; at most 32 details, no version label. CV-wide blockers appear only in aggregate/status. |

The exporter does not retain an operator inventory or count unchanged failures
each reconcile. No new counters are introduced. It replaces its owned series on
each update, so failed/partial reads do not retain prior per-operator results.
Verdicts older than 30 seconds become `Stale`; changed HCU generation/plan becomes
`Invalidated`. Fresh Ready additionally needs a recent successful read and is
invalidated by known credential expiry. Access-maintenance/availability booleans
do not substitute for the scoped result. Missing assessment is `NotAssessed`. Detailed reasons stay
in status, not metric labels. No UID, revision, hash, URL, credential, version or
raw message is added to guest labels.

Active verdict/count/blocker series are omitted for Paused/Completed; credential
and diagnostic timestamp gauges remain. All guest gauges are removed on HCU
deletion. Metrics are refreshed by reconciliation, not evaluated independently at
scrape time: allow the existing 30-second non-completed heartbeat plus scrape
latency. Alert expressions use `time()` for read age. Stale data or a process
outage also needs normal Prometheus target-down monitoring; these gauges do not
replace it.

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

The packaged ServiceMonitor sets `honorLabels: true` so `namespace` remains the
upgrade resource's namespace rather than the coordinator Service's namespace.
Custom scrape configurations must preserve that label as well; otherwise alerts
can report the wrong cluster namespace and dashboard identity joins are invalid.

### Regression Validation

Run `go test ./internal/controller -run '^TestGrafanaDashboard$' -count=1` to validate the JSON, datasource wiring, version variables, complete query contracts, worker label selectors, instant/table versus range query modes, blocked-pool panel, and refresh cadence without starting the controller envtest suite. These are static dashboard contract tests, not live Prometheus query evaluation or Grafana rendering tests.

`go test ./internal/controller -run '^(TestGuestMetrics.*|TestGuestAlertRules|TestGrafanaDashboard|TestGuestAccessManagementRBAC|TestScrapePreservesUpgradeNamespace)$' -count=1`
also checks metric lifecycle/invalidation, namespace-local RBAC and guest alert
contracts. Rule/dashboard expression checks are static string/schema contracts,
not a PromQL execution engine or proof of live alert firing/Grafana rendering.
