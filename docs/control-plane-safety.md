# Control Plane Safety Policy

The HyperShift Upgrade Coordinator (HUC) enforces strict safety gates before and during control plane upgrades. This document explains the version skew rules, health requirements, and verification checks applied to Hosted Control Planes.

---

## 1. Upgrade Plan & Version Progression

- **Supported Path Responsibility:** HUC validates that planned upgrade hops follow valid semantic versioning rules (same major version, at most one minor hop per step). SRE teams must ensure that the planned path is supported by the OpenShift update graph and HyperShift release matrix. HUC does not query the OpenShift update graph service dynamically.
- **Monotonic Progression:** Upgrades must strictly advance forward:
  $$\text{Current Version} < \text{Intermediate Version} < \text{Target Version}$$
  Downgrades and lateral version changes with differing digests are rejected by admission webhooks and preflight controllers.
- **Digest-Pinned Payloads:** All release images must specify immutable digest references (`@sha256:<64-hex>`). Tags alone are not treated as authoritative.

---

## 2. Worker Version Skew Gates (n-3)

To prevent worker nodes from becoming unsupported or disconnected during control plane upgrades, HUC validates version skew across all NodePools before:
1. Acquiring initial worker locks.
2. Requesting any control plane release change (both intermediate and target hops).

### Skew Rules
- **Maximum Skew:** Every NodePool (`status.version` and `status.nodesInfo.nodeVersions[].ocpVersion`) must be within **three minor versions (n-3)** of the active control plane and all remaining planned hops.
- **No Workers Ahead of Control Plane:** Worker nodes cannot run a version newer than the control plane (including newer patch versions).
- **Observable Telemetry Required:** NodePools must report active version information. If a pool has unpopulated version telemetry (for example, a pool newly created or scaled to zero without version history), HUC pauses the upgrade until observable version status is available.

---

## 3. Pre-Upgrade Health & Upgradeability Verification

Before modifying `HostedCluster.spec.release.image`, HUC verifies that the cluster is healthy and ready to upgrade:

- **Upgradeable Condition:** The control plane must explicitly report `ClusterVersionUpgradeable=True` (or condition status True on guest `ClusterVersion`). If an operator reports `Upgradeable=False`, the upgrade halts to allow SRE investigation.
- **Availability:** Core control plane conditions must be healthy: `Available=True`, `ClusterVersionAvailable=True`, and `ClusterVersionSucceeding=True`, with no active `Degraded` or `Failing` conditions.
- **Worker Quiescence:** All NodePools must be quiescent (paused and not actively rolling out changes) before a control plane release is updated.

---

## 4. Control Plane Verification & Rollout Completion

A control plane upgrade step is only verified as complete when all of the following criteria are satisfied:

1. **History Completion:** The latest entry in both `HostedCluster.status.version.history` and guest `ClusterVersion.status.history` reports `State: Completed` with the exact expected version and payload digest.
2. **HCP Specification & Generation:** `HostedControlPlane.spec.releaseImage` matches the expected payload, and `status.controlPlaneVersion.observedGeneration` reflects the current generation.
3. **ControlPlaneComponent (CPC) Readiness:** Every `ControlPlaneComponent` in the hosted control plane namespace (such as `kube-apiserver`, `etcd`, and `openshift-apiserver`) reports:
   - `Available = True`
   - `RolloutComplete = True`
   - Active version matching the expected release.
4. **Guest Cluster Verification:** Guest `ClusterVersion` and all in-tree `ClusterOperators` report matching versions and healthy conditions (`Available=True`, `Degraded=False`, `Progressing=False`).

Only after all four layers are positively verified does HUC proceed to the next scheduled step (such as advancing to the target release in an EUS upgrade, or admitting worker NodePools).
