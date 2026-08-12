# HyperShift Upgrade Coordinator Monitoring & Alerting

This document contains the official SRE monitoring specs for the HyperShift Upgrade Coordinator, including **Prometheus Alerting Rules** and **Grafana Dashboard** configurations.

---

## 1. Prometheus Rule Custom Resource

The following `PrometheusRule` manifests define alerts for cluster upgrades. It separates alerts based on **overall upgrade duration** and **individual step-level duration** (such as control plane or worker NodePool stages), integrating version labels directly in descriptions.

```yaml
apiVersion: monitoring.coreos.com/v1
kind: PrometheusRule
metadata:
  name: hypershift-upgrade-coordinator-alerts
  namespace: monitoring
spec:
  groups:
  - name: hypershift-upgrade.alerts
    rules:

    # 1. Alert when a NodePool upgrade is blocked (High Severity - SRE Pager)
    - alert: HostedClusterNodePoolUpgradeBlocked
      expr: hypershift_upgrade_nodepool_blocked == 1
      for: 15m
      labels:
        severity: critical
        team: sre-fleet
      annotations:
        summary: "HyperShift NodePool upgrade is blocked on {{ $labels.nodepool }} (from {{ $labels.current_version }} to {{ $labels.target_version }})"
        description: "NodePool {{ $labels.nodepool }} in cluster {{ $labels.name }} (namespace {{ $labels.namespace }}) has been blocked during its upgrade rollout from version {{ $labels.current_version }} to {{ $labels.target_version }} for more than 15 minutes. This usually indicates a stuck node drain due to PDBs, local storage volumes, or non-disruptive pod blockages. Check HCCO logs."

    # 2. Alert when the CONTROL PLANE step duration is exceeded (Warning)
    - alert: HostedClusterControlPlaneUpgradeDurationExceeded
      expr: hypershift_upgrade_step_duration_seconds{step="ControlPlane"} > 7200
      for: 5m
      labels:
        severity: warning
        team: sre-fleet
      annotations:
        summary: "Hosted Control Plane upgrade for {{ $labels.name }} has exceeded 2 hours"
        description: "The control plane step upgrade of cluster {{ $labels.name }} from {{ $labels.current_version }} to {{ $labels.target_version }} is taking unusually long (active for {{ $value | humanizeDuration }}). Check for stuck operator pod rollouts or ControlPlaneComponent failures."

    # 3. Alert when an individual NODEPOOL step duration is exceeded (Warning)
    - alert: HostedClusterNodePoolUpgradeDurationExceeded
      expr: hypershift_upgrade_step_duration_seconds{step=~"NodePool-.*"} > 10800
      for: 5m
      labels:
        severity: warning
        team: sre-fleet
      annotations:
        summary: "NodePool {{ $labels.step }} upgrade for {{ $labels.name }} has exceeded 3 hours"
        description: "An individual worker NodePool step ({{ $labels.step }}) in cluster {{ $labels.name }} has been rolling out for over 3 hours. Nodes may be stuck draining or waiting for bare-metal/virtual machine provisioning."

    # 4. Alert when the OVERALL upgrade process exceeds SLA (Warning)
    - alert: HostedClusterOverallUpgradeDurationExceeded
      expr: hypershift_upgrade_duration_seconds > 28800
      for: 5m
      labels:
        severity: warning
        team: sre-fleet
      annotations:
        summary: "Overall upgrade process for cluster {{ $labels.name }} has exceeded 8 hours"
        description: "The overall upgrade coordinator lifecycle for cluster {{ $labels.name }} has been active for {{ $value | humanizeDuration }} from start to current state. This exceeds the general SRE multi-step performance threshold."

    # 5. Alert when a ControlPlane Component is stuck (Warning)
    - alert: HostedClusterControlPlaneComponentStuck
      expr: hypershift_upgrade_controlplane_component_ready{component!=""} == 1
      for: 30m
      labels:
        severity: warning
        team: sre-fleet
      annotations:
        summary: "Control Plane Component {{ $labels.component }} is stuck rolling out"
        description: "Control plane component {{ $labels.component }} for cluster {{ $labels.name }} has been in an intermediate rolling state (Available but not RolloutComplete) for over 30 minutes. Verify the component pods and replica sets."

    # 6. Alert when the upgrade coordinator has encountered multiple failures (Warning)
    - alert: HostedClusterUpgradeFailureRateHigh
      expr: rate(hypershift_upgrade_failed_total[10m]) > 0.05
      for: 5m
      labels:
        severity: warning
        team: sre-fleet
      annotations:
        summary: "High failure rate detected for cluster {{ $labels.name }} upgrade to {{ $labels.target_version }}"
        description: "The upgrade coordinator for cluster {{ $labels.name }} is experiencing a failure rate of {{ $value | printf \"%.2f\" }} errors per second during its upgrade from {{ $labels.current_version }} to {{ $labels.target_version }}. This suggests API request failures, webhook issues, or invalid state configuration."
```

---

## 2. Grafana Dashboard Specification

To visualize the upgrade fleet, SREs use a Grafana dashboard targeting the `/metrics` endpoint of the coordinator. By utilizing version-level metric labels, this dashboard supports robust path filtering and transition tables.

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
```

### JSON Definition Template (Snippets)

Here are the specific query definitions used to construct the panels in Grafana, integrating the new version and step duration dimensions:

#### Panel 1: Count of Upgraded/Upgrading/Blocked Clusters (Stat Panel)
- **Completed Upgrades Query:**
  ```promql
  count(hypershift_upgrade_phase{current_version=~"$current_version", target_version=~"$target_version"} == 6)
  ```
- **Active CP Upgrades Query:**
  ```promql
  count(hypershift_upgrade_phase{current_version=~"$current_version", target_version=~"$target_version"} == 3 or hypershift_upgrade_phase == 4)
  ```
- **Blocked/Stuck Upgrades Query:**
  ```promql
  count(hypershift_upgrade_phase{current_version=~"$current_version", target_version=~"$target_version"} == 7 or hypershift_upgrade_nodepool_blocked == 1)
  ```

#### Panel 3: Active Upgrade Fleet Transition Matrix (Table Panel)
Provides a real-time overview of the active fleet's upgrade paths, combining overall and step-specific duration statistics.
- **Columns to display:**
  - `name`: Cluster Name
  - `current_version`: Upgrading From
  - `target_version`: Upgrading To
  - `intermediate_version`: EUS Intermediate Hop
  - `phase`: State Machine State
  - `overall_duration`: Elapsed Overall Time
  - `step`: Current Upgrade Step
  - `step_duration`: Elapsed Time on Current Step
- **Queries:**
  - To retrieve metadata and phase:
    ```promql
    hypershift_upgrade_phase{current_version=~"$current_version", target_version=~"$target_version"}
    ```
  - To join with overall duration:
    ```promql
    hypershift_upgrade_duration_seconds{current_version=~"$current_version", target_version=~"$target_version"}
    ```
  - To join with step duration:
    ```promql
    hypershift_upgrade_step_duration_seconds{current_version=~"$current_version", target_version=~"$target_version"}
    ```

#### Panel 4: Active Upgrades Duration Listing (Bar Gauge)
- **Query for Overall Upgrade Duration:**
  ```promql
  hypershift_upgrade_duration_seconds{current_version=~"$current_version", target_version=~"$target_version"} > 0
  ```
- **Query for Specific Step Duration (e.g. Control Plane):**
  ```promql
  hypershift_upgrade_step_duration_seconds{step="ControlPlane", current_version=~"$current_version", target_version=~"$target_version"} > 0
  ```
- **Unit:** `seconds (s)`
- **Thresholds:**
  - `0s` (Green)
  - `7200s` / 2 Hours (Yellow)
  - `14400s` / 4 Hours (Red)

#### Panel 5: Blocked NodePools Alert Monitor (State Timeline)
- **Query:**
  ```promql
  hypershift_upgrade_nodepool_blocked{current_version=~"$current_version", target_version=~"$target_version"} == 1
  ```
- **Value Mapping:**
  - `1` -> "BLOCKED" (Red)
  - `0` -> "Healthy" (Green)

#### Panel 6: Worker Node Upgrade Curves (Time Series)
- **Query:**
  ```promql
  hypershift_upgrade_nodepool_rollout_ratio{current_version=~"$current_version", target_version=~"$target_version"} * 100
  ```
- **Unit:** `Percent (0-100)`
- **Format:** Time-series graph showing slope of node transitions as worker machines reboot/drain.
