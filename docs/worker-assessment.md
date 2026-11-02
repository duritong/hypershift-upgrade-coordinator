# Worker NodePool Assessment

HUC orchestrates worker upgrades by managing NodePool specifications and observing the underlying Cluster API (CAPI) machine resources. This document explains how HUC assesses worker readiness and verifies rollout completion.

---

## 1. Rollout Strategies: Replace vs. InPlace

HyperShift supports two distinct upgrade strategies for NodePools:

### A. Replace Strategy (`MachineDeployment`)
- **Mechanism:** Worker nodes are upgraded by rolling out new machine instances. A new CAPI `MachineSet` is created, and nodes are replaced progressively according to rollout settings.
- **Completion Criteria:** A Replace rollout is verified when:
  - The CAPI `MachineDeployment` reports `observedGeneration == generation`.
  - Replicas, updated replicas, and available replicas all match the desired count (`replicas == updatedReplicas == availableReplicas`).
  - The `NodePool` reports `Ready=True`, `UpdatingVersion=False`, and `UpdatingConfig=False`.
  - The active release image and configuration hash match the target release.

### B. InPlace Strategy (`MachineSet`)
- **Mechanism:** Operating system and container runtime updates are applied in place to existing machines without recreating physical/virtual instances.
- **Completion Criteria:** A MachineSet in-place rollout is verified when:
  - The underlying CAPI `MachineSet` reports matching current and target configuration hashes (`targetConfigVersionHash == currentConfigVersionHash`).
  - All worker nodes have completed Machine Config Daemon (MCD) reconciliation.
  - The `NodePool` reports `Ready=True`, `UpdatingVersion=False`, and `UpdatingConfig=False`.
  - Active replicas match desired capacity and report healthy status.

---

## 2. Worker Quiescence Before Control Plane Changes

To ensure platform stability, HUC verifies that worker nodes are **quiescent** before initiating any control plane release change:

- **Definition of Quiescence:** A NodePool is quiescent when it is paused (`pausedUntil: "true"`) and is not currently executing in-flight version or configuration rollouts.
- **Enforcement:** Before updating `HostedCluster.spec.release.image` (for both intermediate and target hops), HUC verifies that all associated NodePools are quiescent.
- **Lock Acquisition:** HUC acquires coordinator-owned worker locks on all NodePools, recording the original user pause state so it can be restored upon rollout completion.

---

## 3. Worker Lock Lifecycle

- **Ownership Tracking:** HUC tracks its worker locks using the `upgrade.huc.0xshift.dev/worker-lock-owner` annotation containing the upgrade CR's UID.
- **Preserving User Pauses:** If a NodePool was already paused by an SRE prior to the upgrade, HUC records the original pause state in `upgrade.huc.0xshift.dev/worker-lock-original-pause`. When the upgrade finishes, HUC restores the original user pause rather than forcing the pool to unpause.
- **Finalizer Restoration:** HUC registers a finalizer on the `HostedClusterUpgrade` CR. If the upgrade CR is deleted while worker locks are active, HUC cleanly restores the original pause states on all NodePools before completing deletion.

---

## 4. Sequential Group Orchestration

- **Priority Ordering:** NodePools are organized into logical rollout groups using the label `upgrade.huc.0xshift.dev/group`.
- **Group Sequence:** Groups roll out in the sequence specified in `spec.nodePoolGroups[]`. The `"default"` group is automatically appended to the end for any unlabeled pools.
- **Concurrency Control:** Within an active group, HUC upgrades NodePools in parallel up to the limit set by `spec.rolloutPolicy.maxParallelNodePools` (defaulting to 1). Subsequent groups wait until the active group completes verification.
