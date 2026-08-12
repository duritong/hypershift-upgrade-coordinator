# HostedClusterUpgrade Custom Resource Specification & GitOps Integration Guide

This document specifies all available configuration knobs inside the `HostedClusterUpgrade` Custom Resource (CR) spec, details its status fields, and provides a comprehensive **GitOps / ArgoCD Integration Guide** for deploying clusters and coordinating automated upgrades without sync conflicts.

---

## 1. Custom Resource Fields Specification

The `HostedClusterUpgrade` CR is designed to be user-centric, explicit, and easy to configure via GitOps.

### A. Configuration Knobs (`.spec`)

| Field | Type | Required / Optional | Description / Allowed Values |
| :--- | :--- | :--- | :--- |
| **`targetReleaseImage`** | `string` | **Required** | The final targeted OpenShift release payload image. Must include the tag and SHA-256 digest (e.g., `quay.io/openshift-release-dev/ocp-release:4.14.0-x86_64@sha256:e2c7...`). |
| **`intermediateReleaseImage`** | `string` | *Optional* | An optional single intermediate OpenShift release payload image used for Extended Update Support (EUS) leaps (e.g., jumping from `4.12` to `4.14` via `4.13`). |
| **`pausedUntil`** | `string` | *Optional* | SRE escape hatch to pause coordinator reconciliation. Set to `"true"` (or `"True"`) to pause indefinitely in safe read-only "Observe-Only" mode. Set to a future RFC3339 timestamp (e.g. `"2026-08-16T12:00:00Z"`) to pause until that time. Set to `"false"` or omit to run normally. |
| **`controlPlane.startAt`** | `string` | *Optional* | An RFC3339 timestamp indicating the earliest time the control plane upgrade may begin. If omitted or nil, the control plane upgrade starts immediately once preflight checks pass. |
| **`rolloutPolicy.maxParallelNodePools`** | `integer` | *Optional* | The maximum number of worker NodePools within the same priority group that can be upgraded concurrently. Defaults to `1`. |
| **`nodePools`** | `array` | **Required** | List of worker NodePools to coordinate. Each element requires a `name` and optional scheduled `startAt` RFC3339 timestamp. |

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
| **`nodePoolStatuses`** | `array` | Detailed list of NodePool-level progress, including individual nodes counts, scheduled start timestamps, active state, and failed node drain blockers (such as stuck PDBs). |

---

## 2. GitOps & ArgoCD Integration Guide

When deploying clusters via GitOps, SREs define the desired state of `HostedCluster` and `NodePool` resources in Git, and ArgoCD continuously reconciles the active cluster state to match the Git repository.

### The Reconciliation Conflict Problem
By default, the upgrade coordinator executes upgrades by **mutating the active resources on the cluster**:
1.  During control plane upgrades, the coordinator updates `.spec.release.image` on the `HostedCluster`.
2.  During NodePool upgrades, the coordinator updates `.spec.release.image` and unpauses `.spec.pausedUntil` (setting it to `"false"`) on `NodePool` resources.

If ArgoCD is set to auto-sync, **it will detect these mutations as out-of-sync drift and immediately overwrite (revert) them back to the old versions declared in Git**, interrupting the upgrade.

### The Solution: `ignoreDifferences`
To solve this, we configure ArgoCD to **ignore drift on specific fields managed by the coordinator** (while still accepting and syncing their initial bootstrap state). SREs then trigger upgrades exclusively by committing a separate `HostedClusterUpgrade` resource to Git!

---

### A. Concrete ArgoCD Application Configuration Example

Below is an example of an ArgoCD `Application` definition configured to ignore upgrade-related drift on `HostedCluster` and `NodePool` resources, while allowing the coordinator to safely manage rollouts:

```yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: production-cluster-01
  namespace: argocd
spec:
  project: default
  source:
    repoURL: 'https://git.example.com/sre-fleet/clusters.git'
    targetRevision: HEAD
    path: clusters/production-cluster-01
  destination:
    server: 'https://kubernetes.default.svc'
    namespace: clusters
  syncPolicy:
    automated:
      prune: true
      selfHeal: true
  
  # Configure ArgoCD to ignore drift on fields mutated by the Upgrade Coordinator
  ignoreDifferences:
    # 1. Ignore release image mutations on the HostedCluster spec
    - group: hypershift.openshift.io
      kind: HostedCluster
      jsonPointers:
        - /spec/release/image

    # 2. Ignore release image and pause mutations on the NodePool spec
    - group: hypershift.openshift.io
      kind: NodePool
      jsonPointers:
        - /spec/release/image
        - /spec/pausedUntil
```

---

### B. Git Repository Structure Example

SREs organize their GitOps repository folder `clusters/production-cluster-01/` with these resources:

#### 1. `hostedcluster.yaml` (Declares the initial/current state)
```yaml
apiVersion: hypershift.openshift.io/v1beta1
kind: HostedCluster
metadata:
  name: production-cluster-01
  namespace: clusters
spec:
  # Initial version bootstrap (ArgoCD will sync this on creation, but ignore any subsequent coordinator updates)
  release:
    image: quay.io/openshift-release-dev/ocp-release:4.13.0-x86_64@sha256:74b23ed4bbb593195a721373ed6693687a9b444c97065ce8ac653ba464375711
  # ... rest of HostedCluster specs
```

#### 2. `nodepools.yaml` (Declares worker NodePools, initially paused)
```yaml
apiVersion: hypershift.openshift.io/v1beta1
kind: NodePool
metadata:
  name: workers-us-east-1a
  namespace: clusters
  labels:
    upgrade.huc.0xshift.dev/order: "10" # Priority Order Group 10
spec:
  clusterName: production-cluster-01
  replicas: 3
  # NodePools must start as paused, allowing the coordinator to unpause them sequentially
  pausedUntil: "true"
  release:
    image: quay.io/openshift-release-dev/ocp-release:4.13.0-x86_64@sha256:74b23ed4bbb593195a721373ed6693687a9b444c97065ce8ac653ba464375711
---
apiVersion: hypershift.openshift.io/v1beta1
kind: NodePool
metadata:
  name: workers-us-east-1b
  namespace: clusters
  labels:
    upgrade.huc.0xshift.dev/order: "20" # Priority Order Group 20
spec:
  clusterName: production-cluster-01
  replicas: 6
  pausedUntil: "true"
  release:
    image: quay.io/openshift-release-dev/ocp-release:4.13.0-x86_64@sha256:74b23ed4bbb593195a721373ed6693687a9b444c97065ce8ac653ba464375711
```

#### 3. `upgrade.yaml` (Commit this to trigger the automated upgrade to 4.14.0)
To trigger a safe, staggered, and fully coordinated upgrade to `4.14.0` via `4.13.0`, SREs simply commit this manifest to the Git repository:
```yaml
apiVersion: upgrade.0xshift.dev/v1alpha1
kind: HostedClusterUpgrade
metadata:
  name: production-cluster-01       # MUST match target cluster name
  namespace: clusters               # MUST match target cluster namespace
spec:
  # Desired target version tag + digest
  targetReleaseImage: quay.io/openshift-release-dev/ocp-release:4.14.0-x86_64@sha256:e2c70fca183e380c6121a1c847806f11e839482123277235a8579db472b9ccf2

  controlPlane:
    startAt: "2026-08-15T02:00:00Z"

  rolloutPolicy:
    maxParallelNodePools: 1

  nodePools:
    - name: workers-us-east-1a
      startAt: "2026-08-15T04:00:00Z"
    - name: workers-us-east-1b
      startAt: "2026-08-15T05:00:00Z"
```

---

## 3. Critical Analysis & Troubleshooting Potential Issues

| Potential Issue | Root Cause | Operator Behavior / Mitigation |
| :--- | :--- | :--- |
| **Initial Bootstrap Overwrite** | SRE commits `HostedCluster` with version `A` and `HostedClusterUpgrade` with version `B` at the exact same time during cluster creation. | The coordinator's **preflight checks** will detect that the cluster is in its initial installation phase (`Installing` status). The coordinator halts and does not mutate any fields until the bootstrap installation on version `A` finishes completely, preventing race conditions. |
| **Unintended Rollback Drift** | SRE accidentally updates the `HostedCluster` or `NodePool` release image fields inside Git to an old version after the upgrade has completed. | Because of the `ignoreDifferences` rules configured in ArgoCD, ArgoCD will **ignore** any difference between Git and the active cluster for these fields. It will not trigger a sync-rollback. Overrides can only be executed by deleting the old upgrade CR and creating a new one. |
| **NodePool Pausing Lock** | SRE commits `nodepools.yaml` with `pausedUntil: "true"` to Git, but the coordinator sets it to `"false"` during rollout. | If `pausedUntil` is **not** included in the `ignoreDifferences` block, ArgoCD will instantly overwrite the coordinator's unpause instruction and set it back to `"true"`, freezing the NodePool upgrade permanently. **Mitigation**: Ensure `/spec/pausedUntil` is always included in the NodePool `ignoreDifferences` list as shown in the Application example above. |
| **ArgoCD Health Check Out-of-Sync** | ArgoCD marks the `HostedClusterUpgrade` resource as `Degraded` when an upgrade is paused or temporarily blocked. | **Solution**: Register a custom Lua health check in ArgoCD (configured in `argocd-cm` ConfigMap) to map `phase: Paused` to `Suspended` and `phase: Blocked` to `Progressing` or `Degraded` only if it exceeds an SLA threshold. |

### ArgoCD Custom Lua Health Check
Register this check in the `argocd-cm` ConfigMap under `resource.customizations.health.upgrade.0xshift.dev_HostedClusterUpgrade`:

```lua
hs = obj.status
if not hs or not hs.phase then
  return { status = "Progressing", message = "Upgrade has not started" }
end
if hs.phase == "Completed" then
  return { status = "Healthy", message = "Upgrade completed successfully" }
end
if hs.phase == "Blocked" or hs.phase == "Failing" then
  return { status = "Degraded", message = "Upgrade is blocked: " .. (hs.message or "") }
end
if hs.phase == "Paused" then
  return { status = "Suspended", message = "Upgrade is paused" }
end
return { status = "Progressing", message = "Current phase: " .. hs.phase .. " - " .. (hs.message or "") }
```
