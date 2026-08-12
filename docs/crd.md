# HostedClusterUpgrade Custom Resource Definition Design

The **HostedClusterUpgrade** Custom Resource (CR) is the API through which SREs request, configure, and monitor a cluster upgrade. It represents the source of truth for the upgrade coordinator.

- **Group:** `upgrade.0xshift.dev`
- **Version:** `v1alpha1`
- **Kind:** `HostedClusterUpgrade`
- **Scope:** Namespaced (must match the namespace of the target `HostedCluster`)

---

## 1. Webhook Validation & Version Progression Gates

To enforce security and stability across the fleet, a validating admission webhook must be registered. It will enforce the following structural and logical constraints:

### A. 1:1 Naming Constraint
- **Rule:** A `HostedClusterUpgrade` CR **must have the exact same name** as the target `HostedCluster` it is coordinating in its namespace. Reject creation if no such `HostedCluster` exists.

### B. Pullspec Structural Regex
- **Rule:** Both `spec.targetReleaseImage` and `spec.intermediateReleaseImage` (if specified) must match a full registry pullspec with a `<version>-<arch>` tag and a `sha256` digest:
  - **Format:** `registry.domain.com/repo/image:<version>-<arch>@sha256:<64-hex-chars>`
  - **Example:** `quay.io/openshift-release-dev/ocp-release:4.14.0-x86_64@sha256:e2c70fca183e380c6121a1c847806f11e839482123277235a8579db472b9ccf2`
  - **Regex Annotation:** 
    `^(\S+)/(\S+):([0-9]+\.[0-9]+\.[0-9]+-[a-zA-Z0-9_]+)@sha256:([a-fA-F0-9]{64})$`

### C. Monotonic Version Progression & Strict 1-Minor Jump Gate
- **Rule:** The webhook must fetch the target `HostedCluster` and extract its current active version (from `.status.version.history` or `.status.releaseImage`).
- **Validation:** The webhook will parse the semantic version portion of all images and assert that:
  1. The intermediate version (if specified) is strictly **greater than** the current cluster version and is **at most 1 minor version jump** ahead (e.g. `4.12` -> `4.13`).
  2. The target version is strictly **greater than** the intermediate version (or current version if no intermediate is specified) and is **at most 1 minor version jump** ahead (e.g. `4.13` -> `4.14`, or direct `4.13` -> `4.14` without intermediate).
- **Outcome:** Reject CR creation or updates if any step represents a downgrade, a multi-minor jump without intermediate, or an overall jump greater than 2 minor versions (e.g. `4.12` -> `4.15` is blocked altogether). If issues occur with specific versions during runtime, the SRE's escape hatch is to pause the upgrade coordinator (`pausedUntil: "true"`) and manually intervene.

### D. StartAt Label-Order Consistency
- **Rule:** Validate that the scheduled `StartAt` times for NodePools are logically consistent with their priority groups (determined by the `upgrade.huc.0xshift.dev/order` label on each NodePool). Specifically, for any two NodePools $A$ and $B$:
     - If $Order(A) < Order(B)$ (meaning $A$ must upgrade *before* $B$), and both have `StartAt` timestamps defined, then $StartAt(B)$ must be **equal to or later** than $StartAt(A)$. Reject CR updates that violate this temporal order.

---

## 2. Custom Resource Schema (Go Types)

Below is the updated Go struct definitions representing the CRD schema.

```go
package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// HostedClusterUpgradeSpec defines the desired state of HostedClusterUpgrade
type HostedClusterUpgradeSpec struct {
	// TargetReleaseImage is the final desired OpenShift release payload image.
	// Must match pattern: registry.domain.com/path:version-arch@sha256:digest
	// +required
	// +kubebuilder:validation:Pattern=`^(\S+)/(\S+):([0-9]+\.[0-9]+\.[0-9]+-[a-zA-Z0-9_]+)@sha256:([a-fA-F0-9]{64})$`
	TargetReleaseImage string `json:"targetReleaseImage"`

	// IntermediateReleaseImage is the single optional OCP release payload image that the control plane
	// must jump through before upgrading to the TargetReleaseImage.
	// Used for EUS (Extended Update Support) intermediate hops (e.g., 4.12 -> 4.13 -> 4.14).
	// If omitted, the upgrade proceeds as a standard direct upgrade (e.g., 4.13 -> 4.14).
	// Must match pattern: registry.domain.com/path:version-arch@sha256:digest
	// +optional
	// +kubebuilder:validation:Pattern=`^(\S+)/(\S+):([0-9]+\.[0-9]+\.[0-9]+-[a-zA-Z0-9_]+)@sha256:([a-fA-F0-9]{64})$`
	IntermediateReleaseImage string `json:"intermediateReleaseImage,omitempty"`

	// PausedUntil can be used to pause reconciliation on this upgrade coordinator.
	// If set to "true" or "True", reconciliation is paused indefinitely (observe-only mode).
	// If set to an RFC3339 timestamp, reconciliation is paused until that time is reached.
	// If set to "false" or "False", empty, or omitted, reconciliation proceeds normally.
	// +optional
	// +kubebuilder:validation:XValidation:rule=`self.matches('^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}.*$') || self in ['true', 'false', 'True', 'False']`,message="PausedUntil must be a date in RFC3339 format or 'True', 'true', 'False' or 'false'"
	PausedUntil *string `json:"pausedUntil,omitempty"`

	// ControlPlane defines the scheduling details for upgrading the Hosted Control Plane (HCP).
	// Since CP upgrades are typically non-disruptive to user workloads, SREs can schedule
	// this stage immediately or during regular business hours.
	// +required
	ControlPlane ControlPlaneUpgradeSpec `json:"controlPlane"`

	// NodePools lists the scheduling and rollout configurations for worker NodePools.
	// Because NodePool upgrades require machine drains/reboots and are highly disruptive,
	// SREs schedule individual NodePools to execute sequentially during distinct,
	// low-traffic maintenance windows.
	// +required
	NodePools []NodePoolUpgradeSpec `json:"nodePools"`

	// RolloutPolicy defines global constraints on the upgrade execution.
	// +optional
	RolloutPolicy *RolloutPolicySpec `json:"rolloutPolicy,omitempty"`
}

type ControlPlaneUpgradeSpec struct {
	// StartAt is an RFC3339 timestamp indicating the earliest time the control plane upgrade may begin.
	// If omitted or nil, the control plane upgrade starts immediately when the CR is reconciled and preflight checks pass.
	// +optional
	StartAt *metav1.Time `json:"startAt,omitempty"`
}

type NodePoolUpgradeSpec struct {
	// Name specifies the exact name of the target NodePool CR to be upgraded.
	// +required
	Name string `json:"name"`

	// StartAt is an RFC3339 timestamp indicating the earliest time this specific NodePool upgrade may begin.
	// If omitted or nil, the NodePool upgrade begins immediately once:
	//   1. Its priority group (defined by labels) becomes active.
	//   2. The control plane upgrade has been fully completed and verified.
	// +optional
	StartAt *metav1.Time `json:"startAt,omitempty"`
}

type RolloutPolicySpec struct {
	// MaxParallelNodePools specifies the maximum number of NodePools that can be
	// upgraded in parallel within the same priority group.
	// Defaults to 1.
	// +optional
	// +kubebuilder:default=1
	MaxParallelNodePools int32 `json:"maxParallelNodePools,omitempty"`
}

// HostedClusterUpgradeStatus defines the observed state of HostedClusterUpgrade
type HostedClusterUpgradeStatus struct {
	// Phase is the current high-level state of the upgrade state machine.
	Phase UpgradePhase `json:"phase"`

	// Message describes the current step, pending schedules, or active blocker details.
	// e.g. "Control Plane upgrade verified. NodePool 'infra-pool' is waiting for its scheduled window (opens in 14 hours)."
	Message string `json:"message,omitempty"`

	// ObservedGeneration is the most recent generation observed by the coordinator.
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions represents the latest available observations of the upgrade's current state.
	// Standard conditions for GitOps (e.g. ArgoCD) compatibility:
	// - Ready: True if the upgrade is fully completed.
	// - Progressing: True if an upgrade is actively being rolled out.
	// - Failing: True if an upgrade is blocked or encountering errors.
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// StartTime is the time when the upgrade actually commenced.
	StartTime *metav1.Time `json:"startTime,omitempty"`

	// CompletionTime is the time when the upgrade successfully completed.
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`

	// ControlPlaneStatus details the progress of the Hosted Control Plane upgrade.
	ControlPlaneStatus ControlPlaneUpgradeStatus `json:"controlPlaneStatus,omitempty"`

	// NodePoolStatus details the progress of each associated NodePool.
	NodePoolStatuses []NodePoolUpgradeStatus `json:"nodePoolStatuses,omitempty"`
}

type UpgradePhase string

const (
	PhaseIdle                         UpgradePhase = "Idle"
	PhasePendingSchedule              UpgradePhase = "PendingSchedule"
	PhaseInstalling                   UpgradePhase = "Installing"
	PhasePreflightChecks              UpgradePhase = "PreflightChecks"
	PhaseUpgradingControlPlane        UpgradePhase = "UpgradingControlPlane"
	PhaseVerifyingControlPlane        UpgradePhase = "VerifyingControlPlane"
	PhaseUpgradingNodePools           UpgradePhase = "UpgradingNodePools"
	PhaseCompleted                    UpgradePhase = "Completed"
	PhaseBlocked                      UpgradePhase = "Blocked"
	PhasePaused                       UpgradePhase = "Paused"
)

type ControlPlaneUpgradeStatus struct {
	// TargetReleaseImage is the release image currently being applied.
	TargetReleaseImage string `json:"targetReleaseImage,omitempty"`

	// Step indicates if we are upgrading to an intermediate version or the final target version.
	Step string `json:"step,omitempty"`

	// ScheduledStart indicates when this step was scheduled to begin (copied from spec.controlPlane.startAt).
	ScheduledStart *metav1.Time `json:"scheduledStart,omitempty"`

	// TotalComponents is the number of control plane components detected.
	TotalComponents int32 `json:"totalComponents,omitempty"`

	// UpgradedComponents is the number of control plane components that have successfully completed rollout.
	UpgradedComponents int32 `json:"upgradedComponents,omitempty"`

	// Components lists the detailed status of each individual control plane component.
	Components []ControlPlaneComponentStatus `json:"components,omitempty"`

	// Blockers contains messages about specific components blocking the rollout.
	Blockers []string `json:"blockers,omitempty"`
}

type ControlPlaneComponentStatus struct {
	// Name of the component (e.g., "kube-apiserver", "etcd", "openshift-apiserver").
	Name string `json:"name"`

	// Version is the active version currently reported by this component.
	Version string `json:"version"`

	// Available reflects whether the component is fully available (Available = True).
	Available bool `json:"available"`

	// RolloutComplete reflects whether the component has finished rolling out (RolloutComplete = True).
	RolloutComplete bool `json:"rolloutComplete"`

	// Message is any descriptive details regarding availability or rollout blocks.
	Message string `json:"message,omitempty"`
}

type NodePoolUpgradeStatus struct {
	// Name of the NodePool.
	Name string `json:"name"`

	// TargetReleaseImage is the release image currently being applied to this NodePool.
	TargetReleaseImage string `json:"targetReleaseImage"`

	// ScheduledStart indicates when this NodePool is scheduled to begin upgrading (copied from spec.nodePools[].startAt).
	ScheduledStart *metav1.Time `json:"scheduledStart,omitempty"`

	// Phase is the current state of this specific NodePool upgrade.
	// Supported states: "Scheduled", "WaitingForControlPlane", "Upgrading", "Completed", "Blocked"
	Phase string `json:"phase"`

	// Nodes contains node-level metrics for the NodePool rollout.
	Nodes NodeStatusSummary `json:"nodes"`

	// Blockers lists detailed messages if the NodePool upgrade is stuck (e.g., node draining blocked).
	Blockers []string `json:"blockers,omitempty"`
}

type NodeStatusSummary struct {
	// Total is the desired replica count.
	Total int32 `json:"total"`
	// Updated is the number of nodes running the target release version.
	Updated int32 `json:"updated"`
	// Ready is the number of nodes that are running and healthy.
	Ready int32 `json:"ready"`
}
```

---

## 3. ArgoCD Compatibility & Health Checks

To make this coordinator GitOps friendly, it adheres to the following principles:

1. **Standard `.status.conditions`:**
   - When `Progressing` is `True`, ArgoCD recognizes the resource is in progress.
   - When `Ready` is `True` (and `Progressing` is `False`), ArgoCD identifies the resource is `Healthy`.
   - When `Failing` is `True`, ArgoCD identifies the resource is `Degraded`.
2. **ArgoCD Custom Health Assessment:**
   A custom Lua health check can be written for the `HostedClusterUpgrade` CRD in ArgoCD configuration:

```lua
hs = obj.status
if not hs or not hs.phase then
  return { status = "Progressing", message = "Upgrade has not started" }
end
if hs.phase == "Completed" then
  return { status = "Healthy", message = "Upgrade completed successfully" }
end
if hs.phase == "Blocked" or hs.phase == "Failing" then
  return { status = "Degraded", message = "Upgrade is blocked" }
end
if hs.phase == "Paused" then
  return { status = "Suspended", message = "Upgrade is paused" }
end
return { status = "Progressing", message = "Current phase: " .. hs.phase }
```
