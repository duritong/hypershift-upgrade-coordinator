# GitOps & ArgoCD Integration Guide

When managing HyperShift clusters via GitOps, SRE teams declare the desired state of `HostedCluster` and `NodePool` resources in Git, while ArgoCD continuously reconciles the active cluster state to match. This document explains how to configure ArgoCD so that HUC can orchestrate upgrades without triggering reconciliation conflicts or accidental rollbacks.

---

## 1. The Reconciliation Conflict Problem

During an automated upgrade, HUC mutates live cluster resources:
1. **HostedCluster:** HUC updates `.spec.release.image` during control plane hops.
2. **NodePool:** HUC manages worker locks using `.spec.pausedUntil` and updates `.spec.release.image` during worker rollouts.

If ArgoCD is configured with automatic synchronization without drift ignore rules, **it will detect these live mutations as out-of-sync drift and immediately revert them to the older versions declared in Git**, interrupting the upgrade.

### The Solution: `ignoreDifferences` & `RespectIgnoreDifferences=true`

To prevent ArgoCD from overwriting active upgrades:
- Use `ignoreDifferences` to tell ArgoCD to ignore coordinator-managed fields.
- Set `syncOptions: [RespectIgnoreDifferences=true]` so that automated or manual syncs do not overwrite these fields on existing resources.

*Note: On initial creation, ArgoCD still applies the bootstrap values declared in Git. After creation, release changes are coordinated through the `HostedClusterUpgrade` resource.*

---

## 2. ArgoCD Application Manifest Example

Configure your ArgoCD `Application` to ignore coordinator-managed release and pause fields:

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
    syncOptions:
      - RespectIgnoreDifferences=true
  
  # Ignore live mutations made by the HyperShift Upgrade Coordinator
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

## 3. Git Repository Organization

Structure your GitOps repository folder (e.g. `clusters/production-cluster-01/`) with the following manifests:

### A. `hostedcluster.yaml` (Initial / Bootstrap State)
```yaml
apiVersion: hypershift.openshift.io/v1beta1
kind: HostedCluster
metadata:
  name: production-cluster-01
  namespace: clusters
spec:
  # Initial release image used for bootstrap
  release:
    image: quay.io/openshift-release-dev/ocp-release:4.13.0-x86_64@sha256:74b23ed4bbb593195a721373ed6693687a9b444c97065ce8ac653ba464375711
  # ... other HostedCluster configuration ...
```

### B. `nodepools.yaml` (Worker NodePools)
Assign each NodePool an upgrade group label (`upgrade.huc.0xshift.dev/group`). Do not configure `pausedUntil` in Git unless you intend to hold the pool indefinitely.

```yaml
apiVersion: hypershift.openshift.io/v1beta1
kind: NodePool
metadata:
  name: workers-infra
  namespace: clusters
  labels:
    upgrade.huc.0xshift.dev/group: "infra"
spec:
  clusterName: production-cluster-01
  replicas: 3
  release:
    image: quay.io/openshift-release-dev/ocp-release:4.13.0-x86_64@sha256:74b23ed4bbb593195a721373ed6693687a9b444c97065ce8ac653ba464375711
---
apiVersion: hypershift.openshift.io/v1beta1
kind: NodePool
metadata:
  name: workers-compute
  namespace: clusters
  labels:
    upgrade.huc.0xshift.dev/group: "compute"
spec:
  clusterName: production-cluster-01
  replicas: 6
  release:
    image: quay.io/openshift-release-dev/ocp-release:4.13.0-x86_64@sha256:74b23ed4bbb593195a721373ed6693687a9b444c97065ce8ac653ba464375711
```

### C. `upgrade.yaml` (Upgrade Coordinator Specification)
Commit this manifest to trigger and schedule the automated upgrade:

```yaml
apiVersion: upgrade.0xshift.dev/v1alpha1
kind: HostedClusterUpgrade
metadata:
  name: production-cluster-01       # Must match target HostedCluster name exactly
  namespace: clusters               # Must match target HostedCluster namespace
spec:
  targetReleaseImage: quay.io/openshift-release-dev/ocp-release:4.14.0-x86_64@sha256:e2c70fca183e380c6121a1c847806f11e839482123277235a8579db472b9ccf2
  intermediateReleaseImage: quay.io/openshift-release-dev/ocp-release:4.13.0-x86_64@sha256:74b23ed4bbb593195a721373ed6693687a9b444c97065ce8ac653ba464375711

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

## 4. Ownership Boundaries & Operational Guidelines

- **Creation vs. Delegation:** Git owns initial resource creation and non-delegated configuration. After resource creation, HostedCluster `spec.release.image` and NodePool `spec.release.image` / `spec.pausedUntil` are managed by HUC.
- **Bootstrapping & Future Upgrades:** A `HostedClusterUpgrade` manifest can be committed alongside initial cluster resources targeting the cluster's initial release image (same version and exact digest). HUC verifies and adopts the existing control plane without rewriting it. NodePools already on the target do not roll out; any lagging NodePool is upgraded according to its group schedule and the configured concurrency limit, so confirm worker alignment or schedule the groups before onboarding. To execute a subsequent upgrade, update `targetReleaseImage` (and optional `intermediateReleaseImage`) in the existing manifest; the CR need not be recreated.
- **NodePool Membership:** Keep NodePool membership and group labels stable during active rollouts. New NodePools should be provisioned before an upgrade starts or after it finishes.
- **Independent User Pauses:** If an SRE sets `spec.pausedUntil: "true"` on a NodePool manually, HUC preserves this pause and will not unpause the pool upon completing other rollouts.

---

## 5. ArgoCD Custom Lua Health Check

By default, ArgoCD may mark `HostedClusterUpgrade` resources as `Degraded` when an upgrade is paused or temporarily waiting for a schedule.

To map phases accurately in the ArgoCD web console, register this custom Lua health check in the `argocd-cm` ConfigMap under `resource.customizations.health.upgrade.0xshift.dev_HostedClusterUpgrade`:

```yaml
data:
  resource.customizations.health.upgrade.0xshift.dev_HostedClusterUpgrade: |
    hs = {}
    if obj.status ~= nil then
      if obj.status.phase == "Completed" then
        hs.status = "Healthy"
        hs.message = obj.status.message or "Upgrade completed successfully."
        return hs
      end
      if obj.status.phase == "Paused" then
        hs.status = "Suspended"
        hs.message = obj.status.message or "Upgrade coordinator is paused."
        return hs
      end
      if obj.status.phase == "Blocked" then
        hs.status = "Degraded"
        hs.message = obj.status.message or "Upgrade is blocked."
        return hs
      end
      if obj.status.phase == "PreflightChecks" or obj.status.phase == "PendingSchedule" or obj.status.phase == "UpgradingControlPlane" or obj.status.phase == "VerifyingControlPlane" or obj.status.phase == "UpgradingNodePools" then
        hs.status = "Progressing"
        hs.message = obj.status.message or "Upgrade is in progress."
        return hs
      end
    end
    hs.status = "Progressing"
    hs.message = "Initializing upgrade coordinator..."
    return hs
```
