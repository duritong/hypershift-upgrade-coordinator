# HyperShift Upgrade Coordinator Monitoring & Alerting

This document contains the official SRE monitoring specifications for the HyperShift Upgrade Coordinator, detailing active **SRE Alert Rules** and **Grafana Dashboard** setups.

---

## 1. SRE Prometheus Alert Rules

All alerting configurations are natively managed and deployed via Kustomize. SREs can inspect or apply the complete raw Prometheus manifest directly from the source file at [**`config/prometheus/prometheus_rule.yaml`**](../config/prometheus/prometheus_rule.yaml).

### Summary of Alert Rules

| Alert Name | Prometheus Expression | Severity | Description / SRE Action |
| :--- | :--- | :--- | :--- |
| **HypershiftUpgradeCoordinatorBlocked** | `hypershift_upgrade_phase == 7` | **Critical (Pager)** | The cluster upgrade is currently in a `Blocked` phase. Check preflight health status, failing conditions, or validation errors in the upgrade CR. |
| **HypershiftUpgradeCoordinatorNodePoolStuck** | `hypershift_upgrade_nodepool_blocked == 1` | **Warning** | An individual worker NodePool upgrade has stalled during node drains. Check for stuck Pod Disruption Budgets (PDBs) or local storage volumes on the guest nodes. |
| **HypershiftUpgradeStepDurationLong** | `hypershift_upgrade_step_duration_seconds > 14400` | **Warning** | An individual upgrade step (e.g., Control Plane rollout) has been active for more than 4 hours. Inspect underlying component rollout readiness. |

---

## 2. Grafana Dashboard Specification

To visualize the upgrade fleet, SREs use a Grafana dashboard targeting the `/metrics` endpoint of the coordinator. By utilizing version-level metric labels, this dashboard supports robust path filtering and transition tables.

SREs can import the complete dashboard JSON definition directly from the source file at [**`docs/monitoring/grafana-dashboard.json`**](monitoring/grafana-dashboard.json).

### Dashboard Variables (Dynamic Filtering)
To make the dashboard interactive and searchable across a large fleet of clusters, the following Prometheus-backed template variables are defined:

1. **`$current_version`**: Filter by the version the cluster started from.
   - **Query:** `label_values(hypershift_upgrade_phase, current_version)`
2. **`$target_version`**: Filter by the desired upgrade version.
   - **Query:** `label_values(hypershift_upgrade_phase, target_version)`

---

### Dashboard Layout Overview
```
+----------------------------------------------------------------------------------------------------+
|  [ Variables: Current Version: $current_version | Target Version: $target_version ]                |
+----------------------------------------------------------------------------------------------------+
|  [ Panel 1: Fleet Upgrade State Summary ]     [ Panel 2: Total Active Upgrades ]                   |
|  Single-stat gauge showing count of Completed, Blocked, Paused, and Upgrading clusters.             |
+----------------------------------------------------------------------------------------------------+
|  [ Panel 3: Active Upgrade Fleet Transition Matrix ]                                               |
|  A table showing Name, Phase, Current Version, Target Version, Intermediate Version, Overall Dur,  |
|  and Active Step Duration (CP or individual NodePools).                                            |
+----------------------------------------------------------------------------------------------------+
|  [ Panel 4: Active Upgrades Live Duration vs Step Duration ]                                       |
|  Bar gauge sorting currently running upgrades by elapsed duration in hours (Alert line @ 4h overall|
|  and separate panels sorting by Control Plane vs NodePool step durations).                         |
+----------------------------------------------------------------------------------------------------+
|  [ Panel 5: Control Plane Component Rollout Statuses ]                                             |
|  A table listing all components (etcd, apiserver, etc.) across clusters with readiness states.      |
+----------------------------------------------------------------------------------------------------+
|  [ Panel 6: NodePool Upgrade Rollout Ratio Curve ]                                                 |
|  Time-series chart graphing rollout curves (updated/total nodes ratio) for active NodePools.       |
+----------------------------------------------------------------------------------------------------+
|  [ Panel 7: Aggregated Cluster Node Rollout Curve ]                                                |
|  Time-series chart graphing overall upgraded worker nodes percentage curve (0% to 100%).           |
+----------------------------------------------------------------------------------------------------+
```

### JSON Definition Template (Snippets)

Here are the specific query definitions used to construct the panels in Grafana, integrating the new version and step duration dimensions:

#### Panel 1: Count of Upgraded/Upgrading/Blocked Clusters (Stat Panel)
*   **Completed Upgrades Query:**
    ```promql
    count(hypershift_upgrade_phase{current_version=~"$current_version", target_version=~"$target_version"} == 6)
    ```
*   **Active CP Upgrades Query:**
    ```promql
    count(hypershift_upgrade_phase{current_version=~"$current_version", target_version=~"$target_version"} == 3 or hypershift_upgrade_phase == 4)
    ```
*   **Blocked/Stuck Upgrades Query:**
    ```promql
    count(hypershift_upgrade_phase{current_version=~"$current_version", target_version=~"$target_version"} == 7 or hypershift_upgrade_nodepool_blocked == 1)
    ```

#### Panel 3: Active Upgrade Fleet Transition Matrix (Table Panel)
*   To retrieve metadata and phase:
    ```promql
    hypershift_upgrade_phase{current_version=~"$current_version", target_version=~"$target_version"}
    ```
*   To join with overall duration:
    ```promql
    hypershift_upgrade_duration_seconds{current_version=~"$current_version", target_version=~"$target_version"}
    ```
*   To join with step duration:
    ```promql
    hypershift_upgrade_step_duration_seconds{current_version=~"$current_version", target_version=~"$target_version"}
    ```

#### Panel 4: Active Upgrades Duration Listing (Bar Gauge)
*   **Query for Overall Upgrade Duration:**
    ```promql
    hypershift_upgrade_duration_seconds{current_version=~"$current_version", target_version=~"$target_version"} > 0
    ```
*   **Query for Specific Step Duration (e.g. Control Plane):**
    ```promql
    hypershift_upgrade_step_duration_seconds{step="ControlPlane", current_version=~"$current_version", target_version=~"$target_version"} > 0
    ```
*   **Unit:** `seconds (s)`
*   **Thresholds**: `0s` (Green), `7200s` / 2h (Yellow), `14400s` / 4h (Red)

#### Panel 5: Blocked NodePools Alert Monitor (State Timeline)
*   **Query:**
    ```promql
    hypershift_upgrade_nodepool_blocked{current_version=~"$current_version", target_version=~"$target_version"} == 1
    ```
*   **Value Mapping**: `1` -> "BLOCKED" (Red), `0` -> "Healthy" (Green)

#### Panel 6: Worker Node Upgrade Curves (Time Series)
*   **Query:**
    ```promql
    hypershift_upgrade_nodepool_rollout_ratio{current_version=~"$current_version", target_version=~"$target_version"} * 100
    ```
*   **Unit**: `Percent (0-100)`

#### Panel 7: Aggregated Cluster Node Rollout Curve (Time Series)
*   **Query:**
    ```promql
    hypershift_upgrade_aggregated_node_rollout_ratio * 100
    ```
*   **Unit**: `Percent (0-100)`
