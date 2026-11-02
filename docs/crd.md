# HostedClusterUpgrade Custom Resource Specification

This document details all configuration fields inside the `HostedClusterUpgrade` Custom Resource (CR) spec and explains the observed telemetry fields reported in status.

---

## 1. Specification Fields (`.spec`)

| Field | Type | Required / Optional | Description |
| :--- | :--- | :--- | :--- |
| **`targetReleaseImage`** | `string` | **Required** | The target OpenShift release payload image. Must include the version tag and immutable SHA-256 digest (e.g. `quay.io/openshift-release-dev/ocp-release:4.14.0-x86_64@sha256:...`). |
| **`intermediateReleaseImage`** | `string` | *Optional* | A single intermediate release image used for 2-minor EUS jumps (e.g. moving from `4.12` to `4.14` via `4.13`). |
| **`pausedUntil`** | `string` | *Optional* | Pauses coordinator actions. Set to `"true"` for indefinite pausing, or an RFC3339 timestamp for a scheduled pause. Set to `"false"` or omit to resume active reconciliation. |
| **`controlPlane.startAt`** | `string` | *Optional* | An RFC3339 timestamp specifying the earliest time the control plane upgrade may start. If omitted, the control plane upgrades immediately after preflight checks pass. |
| **`rolloutPolicy.maxParallelNodePools`** | `integer` | *Optional* | Maximum number of NodePools within the active group that can upgrade concurrently. Defaults to `1`. |
| **`nodePoolGroups`** | `array` | *Optional* | Ordered list of NodePool rollout groups. Groups execute sequentially in list order. Each entry requires a `name` and an optional `startAt` timestamp. Unlabeled pools belong to group `"default"`, which is automatically appended last if not defined. |

---

## 2. Status Telemetry Fields (`.status`)

| Field | Type | Description |
| :--- | :--- | :--- |
| **`phase`** | `string` | Current upgrade state machine phase: `PreflightChecks`, `PendingSchedule`, `UpgradingControlPlane`, `VerifyingControlPlane`, `UpgradingNodePools`, `Completed`, `Blocked`, or `Paused`. |
| **`message`** | `string` | Human-readable explanation of current activity, schedule countdowns, or active blockers. |
| **`startTime`** | `string` | RFC3339 timestamp when the upgrade process actively started (transitioned past `Idle`). |
| **`completionTime`** | `string` | RFC3339 timestamp when the entire upgrade process completed. |
| **`aggregatedNodeStatus`** | `object` | Summary of total, updated, and ready worker nodes summed across all NodePools (e.g. `{"total": 12, "updated": 4, "ready": 10}`). |
| **`controlPlaneStatus`** | `object` | Detailed control plane component rollout status: active target image, total components, upgraded components, and component-specific blockers. |
| **`nodePoolStatuses`** | `array` | Per-NodePool status entries detailing pool phase (`Scheduled`, `Upgrading`, `Completed`, `Blocked`), active version, schedules, and blockers. |
| **`drainDiagnostics`** | `object` | Best-effort cluster-level drain/eviction hints collected from HCCO logs, including collection availability and recent messages. |
| **`guestAccess`** | `object` | Summary of observer credential lifecycle: maintenance readiness, credential availability, expiration timestamp, and delivery secret name. |
| **`guestAssessment`** | `object` | Result of guest cluster health assessment: verdict state (`Ready`, `Converging`, `Unhealthy`, `Unavailable`, `AccessDenied`), reason, expected version, operator summaries, and active blockers. |
| **`guestAssessment.virtualization`** | `object` | Dedicated summary of OpenShift Virtualization operator name and version (reported separately from in-tree ClusterOperators). |
| **`workerAdmission`** | `object` | Record of worker admission authorization, binding the upgrade plan to target release identity and admission timestamp. |

---

## 3. Status Conditions

The coordinator maintains standard Kubernetes conditions in `status.conditions`:

- **`GuestAccessMaintenanceReady`:** Indicates whether guest observer credentials and RBAC are maintained and ready for rotation.
- **`GuestCredentialAvailable`:** Reflects whether a valid, unexpired observer bearer token is delivered and usable.
- **`GuestAssessmentReady`:** Reports whether the latest scoped guest health assessment passed for the active upgrade purpose.

---

## 4. GitOps Integration

For detailed instructions on integrating `HostedClusterUpgrade` resources with ArgoCD, setting drift ignore rules, and registering custom health checks, see the [GitOps Integration Guide](gitops.md).
