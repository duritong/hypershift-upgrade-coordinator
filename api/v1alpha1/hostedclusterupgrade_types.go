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
	// +required
	ControlPlane ControlPlaneUpgradeSpec `json:"controlPlane"`

	// NodePoolGroups lists the sequential rollout groups and their maintenance window start schedules.
	// First groups in this list upgrade first. Un-labeled NodePools belong to the "default" group.
	// +optional
	// +listType=atomic
	NodePoolGroups []NodePoolGroupSpec `json:"nodePoolGroups,omitempty"`

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

type NodePoolGroupSpec struct {
	// Name specifies the exact name of the NodePool group (defined by label upgrade.huc.0xshift.dev/group on individual NodePools).
	// +required
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
	// Defaults to 1.
	// +optional
	// +kubebuilder:default=1
	MaxParallelNodePools int32 `json:"maxParallelNodePools,omitempty"`
}

// HostedClusterUpgradeStatus defines the observed state of HostedClusterUpgrade
type HostedClusterUpgradeStatus struct {
	// Phase is the current high-level state of the upgrade state machine.
	Phase UpgradePhase `json:"phase"`

	// PhaseBeforePause is the state to resume without restarting active rollouts.
	// +optional
	PhaseBeforePause UpgradePhase `json:"phaseBeforePause,omitempty"`

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

	// AggregatedNodeStatus summarizes overall node upgrades progress across all associated NodePools.
	// +optional
	AggregatedNodeStatus NodeStatusSummary `json:"aggregatedNodeStatus,omitempty"`
}

type UpgradePhase string

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
