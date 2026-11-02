# HostedClusterUpgrade Custom Resource Specification

This document specifies all available configuration knobs inside the `HostedClusterUpgrade` Custom Resource (CR) spec and details its status telemetry fields.

---

## 1. Custom Resource Fields Specification

The `HostedClusterUpgrade` CR is designed to be user-centric, explicit, and easy to configure via GitOps.

### A. Configuration Knobs (`.spec`)

| Field | Type | Required / Optional | Description / Allowed Values |
| :--- | :--- | :--- | :--- |
| **`targetReleaseImage`** | `string` | **Required** | The final targeted OpenShift release payload image. Must include the tag and SHA-256 digest (e.g., `quay.io/openshift-release-dev/ocp-release:4.14.0-x86_64@sha256:e2c7...`). |
| **`intermediateReleaseImage`** | `string` | *Optional* | An optional single intermediate OpenShift release payload image used for Extended Update Support (EUS) leaps (e.g., jumping from `4.12` to `4.14` via `4.13`). |
| **`pausedUntil`** | `string` | *Optional* | Pause new upgrade actions with `"true"` (or `"True"`) indefinitely, or a future RFC3339 timestamp until expiry. Existing guest credential renewal/update is allowed; bootstrap, RBAC repair and missing-identity/Secret recreation are not. Set to `"false"` or omit to run normally. |
| **`controlPlane.startAt`** | `string` | *Optional* | An RFC3339 timestamp indicating the earliest time the control plane upgrade may begin. If omitted or nil, the control plane upgrade starts immediately once preflight checks pass. |
| **`rolloutPolicy.maxParallelNodePools`** | `integer` | *Optional* | The maximum number of worker NodePools within the same priority group that can be upgraded concurrently. Defaults to `1`. |
| **`nodePoolGroups`** | `array` | *Optional* | List of sequential NodePool groups to coordinate rollout schedules. First groups in this list upgrade first. If omitted, all NodePools default to the `"default"` group. Each element requires a `name` and optional scheduled `startAt` RFC3339 timestamp. |

---

### B. Observed Telemetry (`.status`)

| Field | Type | Description |
| :--- | :--- | :--- |
| **`phase`** | `string` | Current high-level phase of the state machine (e.g., `PreflightChecks`, `PendingSchedule`, `UpgradingControlPlane`, `VerifyingControlPlane`, `UpgradingNodePools`, `Completed`, `Blocked`, `Paused`). |
| **`message`** | `string` | Human-readable details of the active step, countdown, or stuck drain blockages. |
| **`startTime`** | `string` | RFC3339 timestamp when the upgrade process actually commenced (moved past `Idle`). |
| **`completionTime`** | `string` | RFC3339 timestamp when the entire upgrade process completed successfully. |
| **`aggregatedNodeStatus`** | `object` | Summary of total, updated, and ready worker nodes summed across all NodePools (e.g., `{"total": 12, "updated": 4, "ready": 10}`). |
| **`controlPlaneStatus`** | `object` | Detailed metrics of the control plane component verification, listing total components, upgraded components, and active component rollout blockers. |
| **`nodePoolStatuses`** | `array` | NodePool-level progress, node counts, schedules, and authoritative assessment/orchestration blockers. Unattributed HCCO logs are not pool blockers. |
| **`drainDiagnostics`** | `object` | Best-effort cluster-level HCCO log hints: `available`, stable `reason` (`Collected`, `CollectionFailed`, `ScraperNotConfigured`), and at most ten recent `messages`. Unavailable collection is distinct from no observations. No NodePool/PDB ownership is inferred. |
| **`guestAccess`** | `object` | Access-controller-owned maintenance and credential summary: `observedGeneration`, `reason`, `maintenanceReady`, `credentialAvailable`, `expiresAt`, identity/profile/endpoint metadata and `secretName`. No credentials. `observerUsability` remains `Unknown`; availability/maintenance are not proof of observer authentication or guest health. |
| **`guestAssessment`** | `object` | Upgrade-controller-owned purpose-specific result: `state`, `reason`, `purpose`, `expectedVersion`, `observedGeneration`, `lastAttemptTime`, `lastSuccessfulObservation`, identity/revision fences, at most 256 CO name/version summaries, a separate optional `virtualization` name/version summary, and 32 blocker details, total `blockerCount` and `truncated`. |
| **`workerAdmission`** | `object` | Compatible stage-adoption record, bound to HCU/HC identities, generation, plan and release revisions, with `admittedAt`. Never replaces a fresh per-action guest gate. |

`GuestAccessMaintenanceReady` and `GuestCredentialAvailable` conditions belong to
the access controller; `GuestAssessmentReady` belongs to upgrade reconciliation.
The writers optimistically rebase only their owned status fields/conditions.
`lastSuccessfulObservation` is the time of a successful scoped collection,
**including unhealthy results**, not the last healthy decision. A later read
failure can retain that diagnostic timestamp while returning Unavailable or
AccessDenied. `state` also distinguishes Ready, Converging, Unhealthy,
IdentityMismatch and UnsupportedEvidence. Verdicts refer to their `purpose`, not
a global cluster health guarantee; historical Completed is not live health.

Guest observation is mandatory; no spec fields provide check skips, exclusions,
force or bypass behavior. The guest endpoint mode, guest observer namespace and
operator namespace are deployment configuration, not HCU-selected credential
references or guest RBAC policy.
See the [user guide](user-guide.md#3-mandatory-guest-cluster-access).

---

## 2. GitOps & ArgoCD Integration Guide

To deploy `HostedCluster` and `NodePool` resources via GitOps and coordinate automated, non-conflicting upgrades, please refer directly to the dedicated [**HostedClusterUpgrade GitOps & ArgoCD Integration Guide**](gitops.md).
