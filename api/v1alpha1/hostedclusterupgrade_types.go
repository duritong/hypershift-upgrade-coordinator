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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// HostedClusterUpgradeSpec defines the desired state of HostedClusterUpgrade
type HostedClusterUpgradeSpec struct {
	// TargetReleaseImage is the final desired OpenShift release payload image.
	// Must match pattern: registry.domain.com/path:version-arch@sha256:digest
	// +required
	// +kubebuilder:validation:Pattern=`^(\S+)/(\S+):([0-9]+\.[0-9]+\.[0-9]+-[a-zA-Z0-9_][a-zA-Z0-9_.-]*)@sha256:([a-fA-F0-9]{64})$`
	TargetReleaseImage string `json:"targetReleaseImage"`

	// IntermediateReleaseImage is the single optional OCP release payload image that the control plane
	// must jump through before upgrading to the TargetReleaseImage.
	// Used for EUS (Extended Update Support) intermediate hops (e.g., 4.12 -> 4.13 -> 4.14).
	// If omitted, the upgrade proceeds as a standard direct upgrade (e.g., 4.13 -> 4.14).
	// Must match pattern: registry.domain.com/path:version-arch@sha256:digest
	// +optional
	// +kubebuilder:validation:Pattern=`^(\S+)/(\S+):([0-9]+\.[0-9]+\.[0-9]+-[a-zA-Z0-9_][a-zA-Z0-9_.-]*)@sha256:([a-fA-F0-9]{64})$`
	IntermediateReleaseImage string `json:"intermediateReleaseImage,omitempty"`

	// PausedUntil can be used to pause reconciliation on this upgrade coordinator.
	// If set to "true" or "True", reconciliation is paused indefinitely (observe-only mode).
	// If set to an RFC3339 timestamp, reconciliation is paused until that time is reached.
	// If set to "false" or "False", empty, or omitted, reconciliation proceeds normally.
	// +optional
	PausedUntil *string `json:"pausedUntil,omitempty"`

	// ControlPlane defines the scheduling details for upgrading the Hosted Control Plane (HCP).
	// Since CP upgrades are typically non-disruptive to user workloads, SREs can schedule
	// this stage immediately or during regular business hours.
	// If omitted or empty, the control plane upgrade starts immediately when prerequisites pass.
	// +optional
	// +nullable
	ControlPlane ControlPlaneUpgradeSpec `json:"controlPlane,omitempty"`

	// NodePoolGroups lists the sequential rollout groups and their maintenance window start schedules.
	// First groups in this list upgrade first. Un-labeled NodePools belong to the "default" group.
	// +optional
	// +nullable
	// +listType=atomic
	NodePoolGroups []NodePoolGroupSpec `json:"nodePoolGroups,omitempty"`

	// RolloutPolicy defines global constraints on the upgrade execution.
	// +optional
	// +nullable
	RolloutPolicy *RolloutPolicySpec `json:"rolloutPolicy,omitempty"`
}

type ControlPlaneUpgradeSpec struct {
	// StartAt is an RFC3339 timestamp indicating the earliest time the control plane upgrade may begin.
	// If omitted or nil, the control plane upgrade starts immediately when the CR is reconciled and preflight checks pass.
	// +optional
	StartAt *metav1.Time `json:"startAt,omitempty"`
}

type NodePoolGroupSpec struct {
	// Name specifies the exact name of the NodePool group (defined by label upgrade.huc.0xshift.dev/group on individual NodePools).
	// +required
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// StartAt is an RFC3339 timestamp indicating the earliest time this specific NodePool group upgrade may begin.
	// If omitted or nil, the NodePool group upgrade begins immediately once:
	//   1. Its position in the ordered list is active (all prior groups completed).
	//   2. The control plane upgrade has been fully completed and verified.
	// +optional
	StartAt *metav1.Time `json:"startAt,omitempty"`
}

type RolloutPolicySpec struct {
	// MaxParallelNodePools specifies the maximum number of NodePools that can be
	// upgraded in parallel within the same priority group.
	// Defaults to 1 when omitted in JSON. A zero Go value is omitted by omitempty;
	// an explicit JSON zero or a provided zero policy in direct validation is invalid.
	// +optional
	// +kubebuilder:default=1
	// +kubebuilder:validation:Minimum=1
	MaxParallelNodePools int32 `json:"maxParallelNodePools,omitempty"`
}

// HostedClusterUpgradeStatus defines the observed state of HostedClusterUpgrade
type HostedClusterUpgradeStatus struct {
	// GuestAccess is owned exclusively by the guest-access controller.
	// +optional
	GuestAccess *GuestAccessStatus `json:"guestAccess,omitempty"`
	// GuestAssessment is owned by the upgrade controller. Historical success is
	// diagnostic only; every new action requires a fresh scoped observation.
	// +optional
	GuestAssessment *GuestAssessmentStatus `json:"guestAssessment,omitempty"`
	// WorkerAdmission records compatible stage adoption, never authorization.
	// +optional
	WorkerAdmission *GuestWorkerAdmission `json:"workerAdmission,omitempty"`

	// InitialVersion is the observed source version for this upgrade run.
	// It stays stable through control-plane hops and worker rollouts.
	// +optional
	InitialVersion string `json:"initialVersion,omitempty"`

	// Phase is the current high-level state of the upgrade state machine.
	// It is absent until the upgrade writer initializes; access may report first.
	// +optional
	Phase UpgradePhase `json:"phase,omitempty"`

	// PhaseBeforePause is the state to resume without restarting active rollouts.
	// +optional
	PhaseBeforePause UpgradePhase `json:"phaseBeforePause,omitempty"`

	// PhaseBeforeBlock is the failed step to retry without restarting earlier work.
	// +optional
	PhaseBeforeBlock UpgradePhase `json:"phaseBeforeBlock,omitempty"`

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

	// DrainDiagnostics contains best-effort cluster-level HCCO log observations,
	// not attributed NodePool blockers or proof of PDB ownership.
	// +optional
	DrainDiagnostics *DrainDiagnosticsStatus `json:"drainDiagnostics,omitempty"`

	// AggregatedNodeStatus summarizes overall node upgrades progress across all associated NodePools.
	// +optional
	AggregatedNodeStatus NodeStatusSummary `json:"aggregatedNodeStatus,omitempty"`
}

type UpgradePhase string

type GuestAssessmentStatus struct {
	ObservedGeneration int64 `json:"observedGeneration"`
	// +kubebuilder:validation:Enum=Ready;Converging;Unhealthy;Unavailable;AccessDenied;IdentityMismatch;UnsupportedEvidence
	State string `json:"state"`
	// +kubebuilder:validation:MaxLength=64
	Reason string `json:"reason"`
	// +kubebuilder:validation:MaxLength=64
	Purpose                   string       `json:"purpose"`
	LastAttemptTime           metav1.Time  `json:"lastAttemptTime"`
	LastSuccessfulObservation *metav1.Time `json:"lastSuccessfulObservation,omitempty"`
	// +kubebuilder:validation:MaxLength=64
	HostedClusterUID string `json:"hostedClusterUID,omitempty"`
	// +kubebuilder:validation:MaxLength=64
	SecretUID string `json:"secretUID,omitempty"`
	// +kubebuilder:validation:MaxLength=128
	SecretRevision string `json:"secretRevision,omitempty"`
	// +kubebuilder:validation:MaxLength=64
	PlanRevision string `json:"planRevision,omitempty"`
	// +kubebuilder:validation:MaxLength=128
	ExpectedVersion string `json:"expectedVersion,omitempty"`
	// +kubebuilder:validation:MaxItems=256
	Operators []GuestOperatorSummary `json:"operators,omitempty"`
	// Virtualization is separate from the bounded ALL-current-CO inventory.
	// +optional
	Virtualization *GuestOperatorSummary `json:"virtualization,omitempty"`
	// +kubebuilder:validation:MaxItems=32
	Blockers     []GuestBlocker `json:"blockers,omitempty"`
	BlockerCount int32          `json:"blockerCount"`
	Truncated    bool           `json:"truncated"`
}

type GuestOperatorSummary struct {
	// +kubebuilder:validation:MaxLength=253
	Name string `json:"name"`
	// +kubebuilder:validation:MaxLength=128
	Version string `json:"version,omitempty"`
}

type GuestBlocker struct {
	// +kubebuilder:validation:MaxLength=253
	Operator string `json:"operator,omitempty"`
	// +kubebuilder:validation:MaxLength=64
	Reason string `json:"reason"`
}

type GuestWorkerAdmission struct {
	// +kubebuilder:validation:MaxLength=64
	HostedClusterUpgradeUID string `json:"hostedClusterUpgradeUID"`
	ObservedGeneration      int64  `json:"observedGeneration"`
	// +kubebuilder:validation:MaxLength=64
	HostedClusterUID string `json:"hostedClusterUID"`
	// +kubebuilder:validation:MaxLength=64
	ClusterID string `json:"clusterID"`
	// +kubebuilder:validation:MaxLength=64
	PlanRevision string `json:"planRevision"`
	// ReleaseRevision hashes the expected version and immutable payload identity.
	// The desired image remains in spec; it is not copied into bounded status.
	// +kubebuilder:validation:MaxLength=64
	ReleaseRevision string      `json:"releaseRevision"`
	AdmittedAt      metav1.Time `json:"admittedAt"`
}

// GuestAccessStatus describes credential maintenance, not guest health or proof
// that the observer can currently authenticate. No credentials are exposed here.
type GuestAccessStatus struct {
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// +kubebuilder:validation:MaxLength=64
	Reason              string `json:"reason"`
	MaintenanceReady    bool   `json:"maintenanceReady"`
	CredentialAvailable bool   `json:"credentialAvailable"`
	// ObserverUsability remains Unknown until a scoped observation is performed.
	// +kubebuilder:validation:Enum=Unknown;Ready;Unavailable;AccessDenied
	ObserverUsability string `json:"observerUsability"`
	// +kubebuilder:validation:MaxLength=64
	HostedClusterUID string `json:"hostedClusterUID,omitempty"`
	// +kubebuilder:validation:MaxLength=64
	ClusterID string `json:"clusterID,omitempty"`
	// +kubebuilder:validation:MaxLength=64
	ClusterVersionUID string `json:"clusterVersionUID,omitempty"`
	// +kubebuilder:validation:MaxLength=253
	SecretName string `json:"secretName,omitempty"`
	// +kubebuilder:validation:MaxLength=64
	ProfileRevision string `json:"profileRevision,omitempty"`
	// +kubebuilder:validation:MaxLength=64
	EndpointRevision string       `json:"endpointRevision,omitempty"`
	ExpiresAt        *metav1.Time `json:"expiresAt,omitempty"`
}

type DrainDiagnosticsStatus struct {
	// Available means log collection succeeded, not that all drains are healthy.
	Available bool `json:"available"`
	// Reason is a stable collection outcome, separate from rollout health.
	Reason string `json:"reason"`
	// Messages are recent cluster-level drain/eviction hints, newest first.
	// +kubebuilder:validation:MaxItems=10
	Messages []string `json:"messages,omitempty"`
}

const (
	PhaseIdle                  UpgradePhase = "Idle"
	PhasePendingSchedule       UpgradePhase = "PendingSchedule"
	PhaseInstalling            UpgradePhase = "Installing"
	PhasePreflightChecks       UpgradePhase = "PreflightChecks"
	PhaseUpgradingControlPlane UpgradePhase = "UpgradingControlPlane"
	PhaseVerifyingControlPlane UpgradePhase = "VerifyingControlPlane"
	PhaseUpgradingNodePools    UpgradePhase = "UpgradingNodePools"
	PhaseCompleted             UpgradePhase = "Completed"
	PhaseBlocked               UpgradePhase = "Blocked"
	PhasePaused                UpgradePhase = "Paused"
)

type ControlPlaneUpgradeStatus struct {
	// StartTime is when the current release hop was actuated, cleared after verification.
	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty"`

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
	// StartTime is when this NodePool rollout was actuated, cleared after completion.
	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty"`

	// Name of the NodePool.
	Name string `json:"name"`

	// Group specifies the logical rollout group this NodePool is assigned to (defined by label upgrade.huc.0xshift.dev/group).
	Group string `json:"group"`

	// TargetReleaseImage is the release image currently being applied to this NodePool.
	TargetReleaseImage string `json:"targetReleaseImage"`

	// ScheduledStart indicates when this NodePool's group is scheduled to begin upgrading (copied from spec.nodePoolGroups[].startAt).
	ScheduledStart *metav1.Time `json:"scheduledStart,omitempty"`

	// Phase is the current state of this specific NodePool upgrade.
	// Supported states: "Scheduled", "WaitingForGroup", "Upgrading", "Completed", "Blocked"
	Phase string `json:"phase"`

	// Nodes contains node-level metrics for the NodePool rollout.
	Nodes NodeStatusSummary `json:"nodes"`

	// Blockers lists authoritative NodePool assessment and orchestration blockers.
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

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status
//+kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase",description="Current phase of the upgrade state machine"
//+kubebuilder:printcolumn:name="Nodes-Updated",type="integer",JSONPath=".status.aggregatedNodeStatus.updated",description="Total worker nodes updated"
//+kubebuilder:printcolumn:name="Nodes-Total",type="integer",JSONPath=".status.aggregatedNodeStatus.total",description="Total worker nodes in cluster"
//+kubebuilder:printcolumn:name="Target-Image",type="string",JSONPath=".spec.targetReleaseImage",description="Target OpenShift release image"
//+kubebuilder:printcolumn:name="Status-Message",type="string",JSONPath=".status.message",description="Current status message"
//+kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"

// HostedClusterUpgrade is the Schema for the hostedclusterupgrades API
type HostedClusterUpgrade struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   HostedClusterUpgradeSpec   `json:"spec,omitempty"`
	Status HostedClusterUpgradeStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// HostedClusterUpgradeList contains a list of HostedClusterUpgrade
type HostedClusterUpgradeList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []HostedClusterUpgrade `json:"items"`
}

func init() {
	SchemeBuilder.Register(&HostedClusterUpgrade{}, &HostedClusterUpgradeList{})
}
