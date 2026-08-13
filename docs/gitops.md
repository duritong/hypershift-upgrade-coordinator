# HostedClusterUpgrade GitOps & ArgoCD Integration Guide

When deploying HyperShift clusters via GitOps, SREs define the desired state of `HostedCluster` and `NodePool` resources in Git, and ArgoCD continuously reconciles the active cluster state to match. This document provides a comprehensive integration guide for deploying clusters and coordinating automated upgrades without sync conflicts.

---

## 1. The Reconciliation Conflict Problem

By default, the upgrade coordinator executes upgrades by **mutating the active resources on the cluster**:
1.  During control plane upgrades, the coordinator updates `.spec.release.image` on the `HostedCluster`.
2.  During NodePool upgrades, the coordinator updates `.spec.release.image` and unpauses `.spec.pausedUntil` (setting it to `"false"`) on `NodePool` resources.

If ArgoCD is set to auto-sync, **it will detect these mutations as out-of-sync drift and immediately overwrite (revert) them back to the old versions declared in Git**, interrupting the upgrade.

### The Solution: `ignoreDifferences`
To solve this, we configure ArgoCD to **ignore drift on specific fields managed by the coordinator** (while still accepting and syncing their initial bootstrap state). SREs then trigger upgrades exclusively by committing a separate `HostedClusterUpgrade` resource to Git!

---

## 2. ArgoCD Application Configuration Example

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

## 3. Git Repository Structure Example

SREs organize their GitOps repository folder `clusters/production-cluster-01/` with these resources:

### A. `hostedcluster.yaml` (Declares the initial/current state)
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

### B. `nodepools.yaml` (Declares worker NodePools, initially paused)
```yaml
apiVersion: hypershift.openshift.io/v1beta1
kind: NodePool
metadata:
  name: workers-us-east-1a
  namespace: clusters
  labels:
    upgrade.huc.0xshift.dev/group: "infra" # Rollout Group: infra
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
    upgrade.huc.0xshift.dev/group: "compute" # Rollout Group: compute
spec:
  clusterName: production-cluster-01
  replicas: 6
  pausedUntil: "true"
  release:
    image: quay.io/openshift-release-dev/ocp-release:4.13.0-x86_64@sha256:74b23ed4bbb593195a721373ed6693687a9b444c97065ce8ac653ba464375711
```

### C. `upgrade.yaml` (Commit this to trigger the automated upgrade to 4.14.0)
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

  nodePoolGroups:
    - name: infra
      startAt: "2026-08-15T04:00:00Z"
    - name: compute
      startAt: "2026-08-15T05:00:00Z"
```

---

## 4. Critical Analysis & Troubleshooting Potential Issues

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
