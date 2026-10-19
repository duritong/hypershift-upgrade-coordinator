# Hypershift Upgrade Coordinator Architecture

The Hypershift Upgrade Coordinator is a fleet-level orchestration controller that manages the complex, multi-stage upgrade processes of OpenShift Hosted Control Planes (HCP) and their NodePools. It operates purely as an orchestrator, driving state transitions through declarative Custom Resources (CRs) without directly mutating the internal state of individual components.

- **API Group:** `upgrade.0xshift.dev`
- **Kind:** `HostedClusterUpgrade`

---

## 1. Core Architectural Guidelines

1. **Orchestration, Not Actuation:** The coordinator does not perform low-level upgrades. It manages the lifecycle of `HostedCluster` and `NodePool` resources by updating their specs (e.g., `release.image` and `pausedUntil`) and observing their status conditions.
2. **Reconciliation-Driven State Detection:** On every reconciliation loop, the coordinator must assess the actual state of the cluster (regardless of whether it was recently restarted, paused, or manually modified) and determine the appropriate step to execute or resume.
3. **Safety Verification Gates (with Expected Version):** Transitioning from one step to another requires exhaustive verification. A control plane or NodePool upgrade is not complete when `.status.conditions` update; the coordinator must explicitly verify that the reported active version matches the **expected target version** (to prevent false positives from previously ready components) and that all `ControlPlaneComponent` (CPC) resources are `Available=True` and `RolloutComplete=True`.
4. **Pre-Step Health Re-verification:** Preflight checks are not executed solely on Day 0. To protect clusters during multi-stage rollouts, **the coordinator re-runs the entire preflight health-verification checks immediately before initiating any transition** (such as unpausing a NodePool, shifting from intermediate to target versions, or resuming after a scheduled pause). If the cluster is found degraded or failing at any step transition, the state machine halts, transitions to `Blocked` or `Failing`, and sounds an alarm. This is a critical testing scenario.
5. **Targeted Pausing via `pausedUntil` (With Continuous Observability):** The coordinator leverages HyperShift's built-in `pausedUntil` field on `HostedCluster` and `NodePool` resources to halt reconciliation by the main HyperShift operator when necessary, allowing sequential, controlled, and scheduled rollouts.
   - *Technical Nuance:* In upstream HyperShift, **status reconciliation is executed before the pause check is evaluated**. This means that setting `pausedUntil: "true"` on a NodePool halts spec mutations, but the NodePool controller **continues to reconcile and update its `.status` block** (including node counts, versions, and conditions) in real-time. This provides full read-only observability of paused objects.
6. **Optional EUS (Extended Update Support) Upgrades:** 
   - **EUS Leap Path:** If `intermediateReleaseImage` is populated (jumping from an even minor to an even minor release, e.g., 4.12.x to 4.14.y), the coordinator routes the control plane through the specified intermediate release (4.13.z) while keeping NodePools paused. Once the control plane reaches the final version, NodePools are upgraded directly from their initial version to the final target version.
   - **Standard/Direct Path:** If `intermediateReleaseImage` is omitted or empty (representing minor upgrades like 4.13.m to 4.14.n, or patch releases like 4.12.a to 4.12.b), the coordinator bypasses any intermediate state and directly upgrades the control plane to the `TargetReleaseImage` in a single step.
   - **Skip Behavior:** If `intermediateReleaseImage` is specified, but the cluster is already running a newer version, the coordinator automatically skips applying the intermediate image and proceeds directly to the target rollout.
   - **Strict 1-Minor Jump Gate:** To ensure Kubernetes node-to-control-plane compatibility, the validating admission webhook ensures that no single step (Current -> Intermediate, Intermediate -> Target, or Current -> Target) exceeds exactly 1 minor version jump. Direct multi-minor jumps (such as 4.20 directly to 4.22 without an intermediate) or overall jumps greater than 2 minor versions are strictly rejected.
7. **Strict Pullspec Formatting & Monotonic Upgrades:**
   To guarantee platform stability and prevent catastrophic downsizes/downgrades (which are completely unsupported in OpenShift), the coordinator enforces strict formatting and version progression rules:
   - **Format Validation:** Every pullspec must match `registry.domain.com/repo/image:<version>-<arch>@sha256:<64-character-hex-digest>`. This allows the operator to safely extract the target OpenShift version and target architecture directly from the tag.
   - **Strict Semver Progression:** Upgrades must strictly jump to **higher versions only**. The controller and webhook parse the SemVer portion of the tags and assert that:
     $$\text{Current Cluster Version} < \text{Intermediate Version} < \text{Target Version}$$
     If any segment of the upgrade path represents a downgrade or a lateral shift (same version), the webhook rejects the CR, and the controller's preflight gate blocks reconciliation. In case of issues requiring manual repair or overrides, the SRE escape hatch is to pause the coordinator (`spec.pausedUntil: "true"`) and directly adjust the objects.
8. **Coaligned Naming Convention (No Cluster Reference):** To eliminate redundant configuration and enforce a direct 1:1 relation, a `HostedClusterUpgrade` CR **must share the exact same name** as the target `HostedCluster` in its namespace. A validating admission webhook ensures this constraint and prevents orphan coordinators.
9. **Fleet-Level Cluster Architecture & RBAC Scoping:**
   Aligning with native OpenShift and HyperShift operators (such as the HyperShift Operator, MCE, and ACM), the coordinator runs as a cluster-scoped fleet operator on the trusted management cluster plane:
   - **Cluster-Scoped Informers & Dynamic HCP Namespaces:** Because HyperShift dynamically creates new control plane namespaces (`<hc_namespace>-<cluster_name>`) at runtime as new hosted clusters are provisioned, the coordinator uses cluster-scoped RBAC (`ClusterRole` + `ClusterRoleBinding`) to watch and reconcile `HostedClusters`, `NodePools`, and `ControlPlaneComponents` across all dynamically generated namespaces without requiring per-cluster local RBAC templating or controller restarts.
   - **Target Namespace Filter Flag:** The operator supports an optional `--hosted-cluster-namespace` flag (defaulting to all namespaces when empty, or targeted to a specific namespace such as `clusters`) for environments where an SRE team wants to restrict reconciliation to a designated namespace.
   - **Centralized Fleet Operations:** Deploying as a fleet operator eliminates the operational overhead of running $N$ independent controller deployments, leases, and Prometheus endpoints across tenant namespaces, providing a single control plane for fleet-wide upgrades.
10. **Disruption-Aware Scheduling & Sequential Rollouts:**
    SREs can configure upgrades to execute either as immediate sequential steps, as staggered scheduled steps, or as a mixture of both:
    - **Omitted `StartAt` (Immediate Fallback):** If `StartAt` is omitted or `nil` on any step, the state machine treats the scheduling check as "always satisfied" and immediately proceeds once its prior dependencies are verified.
    - **Mixed Scheduling Example:** The control plane can be upgraded immediately (CP `StartAt` is empty), but the first NodePool has a `StartAt` timestamp set for Day 2 at night. The control plane upgrades and completes immediately, and the coordinator waits until Day 2 night to trigger the first NodePool. Once the first NodePool is complete, any subsequent NodePools (with empty `StartAt` fields) will upgrade sequentially and immediately.
11. **Comprehensive NodePool Resolution (Zero-Orphan Architecture):**
    To ensure no NodePool is left permanently paused (`pausedUntil: "true"`), the coordinator always retrieves **all** NodePools associated with the cluster from `<hc_namespace>`, including those omitted from `.spec.nodePools` list in the CR:
    - **Priority Grouping:** Every NodePool (listed or omitted) is assigned its priority group based on its `upgrade.huc.0xshift.dev/order` label (defaulting to `50` if missing).
    - **Omitted NodePool Fallback:** Any NodePool omitted from the CR's `spec.nodePools` is treated as having an empty/nil `StartAt` timestamp (meaning it will roll out immediately once its priority group becomes active).
    - *Example:* A cluster has `pool-a` (order `10`, scheduled for Day 2) and `pool-b` (order `50`, scheduled for Day 3), and `pool-c` (order `50`, omitted from spec). During preflight, all pools are paused. On Day 2, `pool-a` upgrades. On Day 3, `pool-b` begins its scheduled rollout, and `pool-c` upgrades **in parallel** with it (since `pool-c` has default order `50` and an empty `StartAt`). This guarantees 100% of NodePools are upgraded and unpaused.
12. **Event-Driven Status Monitoring (Watches & Predicates):**
    To maintain optimal performance, low latency, and token efficiency, the coordinator **must not poll or list** `ControlPlaneComponent` (CPC) resources. Instead, it registers a **Watch** for `ControlPlaneComponent` objects within the control plane namespace `<hc_namespace>-<cluster_name>`, mapping status updates back to the parent `HostedClusterUpgrade` resource (re-keying to the parent CR which shares the same name as the cluster). The controller uses status-modification `Predicates` to filter out non-status updates (e.g. metadata/annotation changes), ensuring reconciles only fire on actual component transitions.
13. **Robust Testing Mandates:** The codebase must be backed by an exhaustive testing suite:
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
  - The version parsed from `spec.targetReleaseImage` (and `intermediateReleaseImage` if specified) represents a **strictly higher** version than the current active version reported in `.status.version.history`.

#### B. Control Plane Scheduling & Optional EUS Upgrades
- **Scheduling Gate:** When entering the Control Plane upgrade phase, the coordinator checks `spec.controlPlane.startAt`. If the timestamp is populated and the current time is before it, the coordinator stays in `PendingSchedule` state and updates `.status.message` (e.g., `"Control Plane upgrade scheduled to begin on 2026-08-12T10:00:00Z (opens in 2 hours)"`). If it is omitted or `nil`, the gate is bypassed immediately.
- **EUS Multi-Hop Flow:** Once the schedule opens:
  1. The coordinator sets `spec.pausedUntil: "true"` on all associated `NodePool` resources to prevent them from reacting to any control plane changes.
  2. If `intermediateReleaseImage` is specified and the cluster is not already on a newer version, the coordinator sets `HostedCluster.spec.release.image` to the intermediate image. It then waits for the control plane to reach the intermediate version and verifies it (verifying active version is intermediate and all CPCs are ready/complete).
  3. Once intermediate step is verified, or if no intermediate image was defined, the coordinator sets `HostedCluster.spec.release.image` to the `TargetReleaseImage`.
  4. The coordinator waits for the target control plane upgrade to reach `TargetReleaseImage` and stabilize.

#### C. Staggered NodePool Orchestration & HCCO Log-Scraping Draining Monitor
Once the control plane has been 100% verified on the target version, the coordinator begins sequentially rolling out the NodePools:

- **Group-Based Rollouts:** The coordinator fetches all associated NodePools and maps them to logical groups based on the label `upgrade.huc.0xshift.dev/group: <group-name>` (defaulting to the `"default"` group if missing).
- **Staggered, Scheduled Execution:**
  - The coordinator processes NodePool groups sequentially in the exact ordered sequence defined in `spec.nodePoolGroups`. Any un-labeled NodePools belong to `"default"`, which is automatically appended to the end of the sequence if omitted from the spec list.
  - For each group in the sequence, the coordinator evaluates its corresponding `StartAt` schedule defined in `spec.nodePoolGroups[]`.
  - **The Waiting Gate:** If a group's `StartAt` is defined and the current time is *before* it, the coordinator pauses rollout for that group, sets its NodePools status to `"Scheduled"`, and continuously reports the step readiness in `.status.message` (e.g., `"Control plane upgraded. NodePoolGroup 'compute' is scheduled to upgrade on 2026-08-13T02:00:00Z (in 14 hours)"`).
  - **Immediate Fallback:** If `StartAt` is omitted or `nil`, the waiting gate is bypassed immediately.
  - **The Upgrade Gate:** Once any scheduled times are met or bypassed, the coordinator unpauses the group's NodePools (`spec.pausedUntil: "false"`), sets their release image to the target version, and monitors the rollout.
  - **Parallelism Constraints:** NodePools within the same active group whose scheduled times are open or empty can upgrade in parallel, limited by `.spec.rolloutPolicy.maxParallelNodePools` (defaulting to 1). Subsequent groups remain in `"WaitingForGroup"` status until the current group finishes completely.
- **Log-Scraping Draining Observation:**
  * **The Challenge:** Upstream HyperShift's HCCO node drainer controller (`hosted-cluster-config-operator/controllers/drainer/drainer.go`) **does not emit Kubernetes Events** during node drains or evictions. It relies strictly on internal container logging.
  * **The Solution:** After worker rollout actions, a bounded best-effort request fetches HCCO logs once per cluster reconciliation. Timestamped generic `failed to drain node` messages and kubectl `error when evicting pods/` retries become cluster-level `status.drainDiagnostics.messages`, never attributed NodePool blockers. Later `Drain succeeded on node` messages clear earlier failures for the same identified node. Collection availability is separate from absence of observations; logs do not prove exact PDB ownership. Separate cluster-level availability and observation gauges support alerts. See the [user guide](user-guide.md#troubleshooting-blocked-node-drains) for bounds and limitations.

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
| `hypershift_upgrade_nodepool_blocked` | Gauge | `namespace`, `name`, `nodepool`, `current_version`, `target_version` | Pool assessment/orchestration blocker, not attributed HCCO log hints. | Warning after 30 minutes; inspect pool status. |
| `hypershift_upgrade_nodepool_rollout_ratio` | Gauge | `namespace`, `name`, `nodepool` | Ratio of nodes running the target version and ready (`updatedReplicas / totalReplicas`). | Visualizing rollout curves in SRE Grafana dashboards. |
