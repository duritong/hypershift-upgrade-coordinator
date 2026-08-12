# Hypershift Upgrade Coordinator Architecture

The Hypershift Upgrade Coordinator is a fleet-level orchestration controller that manages the complex, multi-stage upgrade processes of OpenShift Hosted Control Planes (HCP) and their NodePools. It operates purely as an orchestrator, driving state transitions through declarative Custom Resources (CRs) without directly mutating the internal state of individual components.

- **API Group:** `upgrade.0xshift.dev`
- **Kind:** `HostedClusterUpgrade`

---

## 1. Core Architectural Guidelines

1. **Orchestration, Not Actuation:** The coordinator does not perform low-level upgrades. It manages the lifecycle of `HostedCluster` and `NodePool` resources by updating their specs (e.g., `release.image` and `pausedUntil`) and observing their status conditions.
2. **Reconciliation-Driven State Detection:** On every reconciliation loop, the coordinator must assess the actual state of the cluster (regardless of whether it was recently restarted, paused, or manually modified) and determine the appropriate step to execute or resume.
3. **Safety Verification Gates (with Expected Version):** Transitioning from one step to another requires exhaustive verification. A control plane or NodePool upgrade is not complete when `.status.conditions` update; the coordinator must explicitly verify that the reported active version matches the **expected target version** (to prevent false positives from previously ready components) and that all `ControlPlaneComponent` (CPC) resources are `Available=True` and `RolloutComplete=True`.
4. **Targeted Pausing via `pausedUntil`:** The coordinator leverages HyperShift's built-in `pausedUntil` field on `HostedCluster` and `NodePool` resources to halt reconciliation by the main HyperShift operator when necessary, allowing sequential, controlled, and scheduled rollouts.
5. **Optional EUS (Extended Update Support) Multi-Hop Upgrades:** 
   - **Multi-Hop Path:** If `IntermediateReleaseImages` is populated (jumping from an even minor to an even minor release, e.g., 4.20.x to 4.22.y), the coordinator routes the control plane through the specified intermediate release (4.21.z) while keeping NodePools paused. Once the control plane reaches the final version, NodePools are upgraded directly from their initial version to the final target version.
   - **Standard/Direct Path:** If `IntermediateReleaseImages` is omitted or empty (representing minor upgrades like 4.21.m to 4.22.n, or patch releases like 4.20.a to 4.20.b), the coordinator bypasses any intermediate state and directly upgrades the control plane to the `TargetReleaseImage` in a single step.
6. **Strict Pullspec Formatting & Monotonic Upgrades:**
   To guarantee platform stability and prevent catastrophic downsizes/downgrades (which are completely unsupported in OpenShift), the coordinator enforces strict formatting and version progression rules:
   - **Format Validation:** Every pullspec must match `registry.domain.com/repo/image:<version>-<arch>@sha256:<64-character-hex-digest>`. This allows the operator to safely extract the target OpenShift version and target architecture directly from the tag.
   - **Strict Semver Progression:** Upgrades must strictly jump to **higher versions only**. The controller and webhook parse the SemVer portion of the tags and assert that:
     $$\text{Current Cluster Version} < \text{Intermediate}[0] < \text{Intermediate}[1] \dots < \text{Target Version}$$
     If any segment of the upgrade path represents a downgrade or a lateral shift (same version), the webhook rejects the CR, and the controller's preflight gate blocks reconciliation.
7. **Coaligned Naming Convention (No Cluster Reference):** To eliminate redundant configuration and enforce a direct 1:1 relation, a `HostedClusterUpgrade` CR **must share the exact same name** as the target `HostedCluster` in its namespace. A validating admission webhook ensures this constraint and prevents orphan coordinators.
8. **Least Privilege & Multi-Namespace Isolation (No Cluster-Wide Admin Roles):**
   To strictly secure enterprise management clusters, the coordinator operates **without any cluster-wide permissions** (no global `ClusterRoleBindings` for resources).
   - **Target Namespace Flag:** The operator takes a `--hosted-cluster-namespace` flag (defaulting to `clusters`) to specify which namespace it manages.
   - **Dynamic CP Namespace Resolution:** When coordinating a cluster named `<cluster_name>` located in the hosted cluster namespace `<hc_namespace>`, the coordinator expects its control plane components to reside in `<hc_namespace>-<cluster_name>`.
   - **Delegated RoleBindings:** It is the responsibility of the operator/admin to wire up local `RoleBindings` in both `<hc_namespace>` and individual `<hc_namespace>-<cluster_name>` namespaces, linking the coordinator's `ServiceAccount` to standard read/write permissions for `HostedClusters`/`NodePools` and read-only permissions for `ControlPlaneComponents` and pod logs.
   - **Multi-Tenancy:** Multiple independent upgrade coordinators can run concurrently on the same management cluster, each managing a separate target namespace with absolute tenant-to-tenant RBAC isolation.
9. **Disruption-Aware Scheduling & Sequential Rollouts:**
   SREs can configure upgrades to execute either as immediate sequential steps, as staggered scheduled steps, or as a mixture of both:
   - **Omitted `StartAt` (Immediate Fallback):** If `StartAt` is omitted or `nil` on any step, the state machine treats the scheduling check as "always satisfied" and immediately proceeds once its prior dependencies are verified.
   - **Mixed Scheduling Example:** The control plane can be upgraded immediately (CP `StartAt` is empty), but the first NodePool has a `StartAt` timestamp set for Day 2 at night. The control plane upgrades and completes immediately, and the coordinator waits until Day 2 night to trigger the first NodePool. Once the first NodePool is complete, any subsequent NodePools (with empty `StartAt` fields) will upgrade sequentially and immediately.
10. **Event-Driven Status Monitoring (Watches & Predicates):**
   To maintain optimal performance, low latency, and token efficiency, the coordinator **must not poll or list** `ControlPlaneComponent` (CPC) resources. Instead, it registers a **Watch** for `ControlPlaneComponent` objects within the control plane namespace `<hc_namespace>-<cluster_name>`, mapping status updates back to the parent `HostedClusterUpgrade` resource (re-keying to the parent CR which shares the same name as the cluster). The controller uses status-modification `Predicates` to filter out non-status updates (e.g. metadata/annotation changes), ensuring reconciles only fire on actual component transitions.
11. **Robust Testing Mandates:** The codebase must be backed by an exhaustive testing suite:
   - **Unit Tests:** Rigorously mock state combinations to verify all possible state-machine transitions, edge cases, and unexpected failures.
   - **Integration Tests:** Test the coordinator controller with `envtest` against simulated `HostedCluster`, `HostedControlPlane`, `NodePool`, and `ControlPlaneComponent` CRs.
   - **End-to-End (E2E) Tests:** Full workflow test suite executing a complete EUS upgrade cycle in a real/simulated environment, verifying metrics and alert conditions.

---

## 2. Upgrade State Machine Flow

The upgrade coordinator operates as a deterministic state machine that continuously assesses the environment's readiness to progress or wait.

```
       [ Idle / Ready ]
              │
              ▼
     [ Preflight Checks ]  ◄─── (Verify cluster is healthy, parse versions, check progression, not installing)
              │
              ▼
    [ Schedule Check: CP ] ◄─── (Check CP StartAt; if empty, proceed immediately; else wait)
              │
              ▼
   [ Intermediate CP Upgrade ] ◄── (EUS multi-hop path only: Set CP target to 4.21, keep NPs paused)
              │
              ▼
   [ Verify Intermediate CP ]  ◄── (Check 4.21 expected version, CPCs availability & completion)
              │
              ▼
      [ Target CP Upgrade ]  ◄─── (Set CP target to 4.22)
              │
              ▼
      [ Verify Target CP ]   ◄─── (Check 4.22 expected version, CPCs availability & completion)
              │
              ▼
    [ Schedule Check: NP-x ] ◄── (Check NP StartAt; if empty, proceed immediately; else wait)
              │
              ▼
     [ NodePool Upgrade ]   ◄─── (Determine NodePool upgrade order, set target to 4.22)
              │
              ▼
     [ Verify NodePools ]   ◄─── (Check ready/updated counts, match expected version, monitor drains)
              │
              ▼
         [ Completed ]
```

### Detailed State Transitions & Scheduling Gates

#### A. Preflight Checks & Installation Phase
- **Installation Detection:** Before starting any upgrade, the coordinator must check if the `HostedCluster` is in its initial installation phase. If `HostedCluster.status.version.history` has no completed entries, or if the high-level progressing condition indicates initial setup (Progress of the `HostedCluster` is not yet `Completed`), the coordinator transitions to an `Installing` state and halts upgrade triggers.
- **Health & Version Progression Assessment:** Prior to changing any version fields, the coordinator verifies that:
  - No existing failing conditions are present on `HostedCluster`.
  - Underlying nodes are healthy and ready.
  - The API server is fully responsive.
  - The version parsed from `spec.targetReleaseImage` (and any `IntermediateReleaseImages`) represents a **strictly higher** version than the current active version reported in `.status.version.history`.

#### B. Control Plane Scheduling & Optional EUS Upgrades
- **Scheduling Gate:** When entering the Control Plane upgrade phase, the coordinator checks `spec.controlPlane.startAt`. If the timestamp is populated and the current time is before it, the coordinator stays in `PendingSchedule` state and updates `.status.message` (e.g., `"Control Plane upgrade scheduled to begin on 2026-08-12T10:00:00Z (opens in 2 hours)"`). If it is omitted or `nil`, the gate is bypassed immediately.
- **EUS Multi-Hop Flow:** Once the schedule opens:
  1. The coordinator sets `spec.pausedUntil: "true"` on all associated `NodePool` resources to prevent them from reacting to any control plane changes.
  2. If `IntermediateReleaseImages` is populated, the coordinator sets `HostedCluster.spec.release.image` to the intermediate image. It then waits for the control plane to reach the intermediate version and verifies it (verifying active version is intermediate and all CPCs are ready/complete).
  3. Once intermediate step is verified, or if no intermediate image was defined, the coordinator sets `HostedCluster.spec.release.image` to the `TargetReleaseImage`.
  4. The coordinator waits for the target control plane upgrade to reach `TargetReleaseImage` and stabilize.

#### C. Staggered NodePool Orchestration & Scheduling Gates
Once the control plane has been 100% verified on the target version, the coordinator begins sequentially rolling out the NodePools:

- **Label-Based Priority Grouping:** The coordinator fetches all associated NodePools and groups them by priority based on their `upgrade.hypershift.openshift.io/order` label (defaulting to `50` if missing).
- **Staggered, Scheduled Execution:**
  - The coordinator processes NodePool groups sequentially (lowest order value first).
  - For each NodePool in the active group, the coordinator evaluates its corresponding `StartAt` schedule defined in `spec.nodePools`.
  - **The Waiting Gate:** If a NodePool's `StartAt` is defined and the current time is *before* it, the coordinator pauses rollout for that specific NodePool, sets its status to `"Scheduled"`, and continuously reports the step readiness in `.status.message` (e.g., `"Control plane upgraded. NodePool 'worker-pool-1' is scheduled to upgrade on 2026-08-13T02:00:00Z (in 14 hours)"`).
  - **Immediate Fallback:** If `StartAt` is omitted or `nil`, the waiting gate is bypassed immediately.
  - **The Upgrade Gate:** Once any scheduled times are met or bypassed, the coordinator unpauses the NodePool (`spec.pausedUntil: "false"`), sets its release image to the target version, and monitors the rollout.
  - **Parallelism Constraints:** NodePools within the same group whose scheduled times are open or empty can upgrade in parallel, limited by `.spec.nodePools.concurrency`.
- **Draining Observation:** During the rollout, the coordinator compares updated node counts and monitors HCCO logs/events to detect stuck drains.

---

## 3. Pause & Resume Mechanics

The coordinator supports two levels of pausing:

1. **Coordinator-Level Pause (`spec.pausedUntil`):**
   - The coordinator's own CR supports a `.spec.pausedUntil` field, mirroring HyperShift's pause syntax:
     - `"true"` or `"True"`: Paused indefinitely. The coordinator enters an **observe-only** mode (does not write or mutate any object but continues monitoring and exposing metrics).
     - **RFC3339 Timestamp** (e.g., `"2026-08-15T22:00:00Z"`): Paused until that timestamp is reached. Once the current time is past the timestamp, the coordinator automatically resumes mutation.
     - `"false"`, `"False"`, empty (`""`), or omitted: Reconciliation is active and upgrades proceed normally.
2. **Object-Level Pause (`pausedUntil`):**
   - The coordinator manages the `spec.pausedUntil` field on target `HostedCluster` and `NodePool` resources.
   - If a NodePool is not yet ready to be upgraded according to the label-based sequence, or its individual scheduled start time is in the future, the coordinator sets its `.spec.pausedUntil: "true"`. This prevents the underlying HyperShift operators from performing any reconciliation on them.

---

## 4. Observability & Metrics

To provide SRE teams with deep visibility, the coordinator aggregates underlying infrastructure states and exposes them both inside the custom resource status and as a robust set of Prometheus metrics.

### Status Aggregation & ControlPlaneComponent Monitoring
- The controller **watches** the `ControlPlaneComponent` (CPC) CRs in the target cluster's control plane namespace (`<hc_namespace>-<cluster_name>`).
- For each component status change, it copies the following properties into `status.controlPlaneStatus.components`:
  - Active Version (`.status.version`)
  - Availability (`Available = True` condition)
  - Rollout Completeness (`RolloutComplete = True` condition)
  - Progress messages / blockers.

---

### Prometheus Metrics Specification
The operator exposes a Prometheus metric endpoint (`/metrics`) to facilitate dashboards and standard alerting rules. To enable robust fleet-wide analysis, **version metadata** is injected as dimensions directly on relevant metrics.

| Metric Name | Type | Labels | Description | SRE Use Case / Alert Pattern |
|---|---|---|---|---|
| `hypershift_upgrade_phase` | Gauge | `namespace`, `name`, `current_version`, `target_version`, `intermediate_version` | Current phase of the upgrade state machine (integer mapped: 0=Idle, 1=Installing, 2=Preflight, 3=UpgradingCP, 4=VerifyingCP, 5=UpgradingNPs, 6=Completed, 7=Blocked, 8=Paused). Includes version transition context. | Dashboard tracking fleet-wide rollout status and sorting/filtering by upgrade paths. |
| `hypershift_upgrade_duration_seconds` | Gauge | `namespace`, `name`, `current_version`, `target_version`, `intermediate_version` | Active elapsed overall upgrade time in seconds. Reset to 0 when completed or idle. Includes version transition context. | Alert if overall upgrade exceeds SLA. Allows aggregating upgrade performance by version path. |
| `hypershift_upgrade_step_duration_seconds` | Gauge | `namespace`, `name`, `step`, `current_version`, `target_version` | Active elapsed duration for a specific step (e.g., `step="ControlPlane"`, `step="NodePool-worker-1"`). Resets when the individual step completes. | Alert if a specific step exceeds its targeted performance threshold (e.g. CP > 2h, NodePool rollout > 3h). |
| `hypershift_upgrade_failed_total` | Counter | `namespace`, `name`, `current_version`, `target_version`, `reason` | Total number of retries or failures encountered during the upgrade. | Alert if rate of failures is high (e.g., rate > 0.1/min). |
| `hypershift_upgrade_controlplane_component_ready` | Gauge | `namespace`, `name`, `component` | Availability and completeness state of individual components (0=Unavailable, 1=Available/Rolling, 2=Available/Complete). | Alert if a critical component (e.g., `etcd`) remains at `0` or `1` for more than 45 minutes during an upgrade. |
| `hypershift_upgrade_nodepool_blocked` | Gauge | `namespace`, `name`, `nodepool`, `current_version`, `target_version` | Boolean flag (1=Blocked, 0=Active/Complete) indicating if a NodePool rollout has stalled, with version details. | Immediate high-severity SRE pager alert when `1` (usually due to a stuck node drain or PDB blocker). |
| `hypershift_upgrade_nodepool_rollout_ratio` | Gauge | `namespace`, `name`, `nodepool` | Ratio of nodes running the target version and ready (`updatedReplicas / totalReplicas`). | Visualizing rollout curves in SRE Grafana dashboards. |
