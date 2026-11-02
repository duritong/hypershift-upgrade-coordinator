# HostedClusterUpgrade GitOps & ArgoCD Integration Guide

When deploying HyperShift clusters via GitOps, SREs define the desired state of `HostedCluster` and `NodePool` resources in Git, and ArgoCD continuously reconciles the active cluster state to match. This document provides a comprehensive integration guide for deploying clusters and coordinating automated upgrades without sync conflicts.

## Mandatory Guest Verification Rollout

The guest-observation implementation is enabled in the standard
workflow for every managed HCU, including existing runs. Roll out its updated CRD,
fleet get-only Service/source Secret reads, namespace-local delivery Secret
Role/RoleBinding, Deployment namespace environment and operator image as one
coordinated change, before relying on new status/metrics. Keep the Role in the
actual operator namespace; it grants whole-namespace Secret lifecycle authority.
The packaged `OPERATOR_NAMESPACE` comes from the Pod namespace through the
Downward API; local runs must provide it or `--operator-namespace` explicitly.

Do not declare observer delivery Secret contents, fixed guest profile ownership
annotations, `status.guestAccess`, `status.guestAssessment`, or `workerAdmission`
as desired state in Git. Do not select alternate admin Secrets: access requires
the current HC-published kubeconfig reference and verified guest cluster identity.
Leave generated credentials to the access controller and avoid independent
reconciliation/pruning of its guest profile. No cross-namespace HC owner reference
is used for delivery Secrets; lifecycle cleanup checks source UIDs explicitly.

The `ignoreDifferences` rules below delegate live HC/NodePool fields; they are
**not** guest-check bypasses. Check skipping, operator exclusions and force flags
are not available. Missing access or bad evidence withholds new actions without
cancelling delegated work. Paused credential renewal can update an existing
owned delivery Secret, but cannot bootstrap, repair RBAC or recreate missing
resources. See [access/network prerequisites](user-guide.md#3-mandatory-guest-cluster-access).
Validate actual HyperShift API connectivity, credential rotation and upgrade
behavior before production rollout; automated simulations are not deployed
upgrade validation.

---

## 1. The Reconciliation Conflict Problem

By default, the upgrade coordinator executes upgrades by **mutating the active resources on the cluster**:
1.  During control plane upgrades, the coordinator updates `.spec.release.image` on the `HostedCluster`.
2.  During NodePool upgrades, the coordinator updates `.spec.release.image` and releases its owned worker locks by restoring the original `.spec.pausedUntil` value, including an absent field or an expired timestamp. It does not always set `"false"` or clear independent user pauses.

If ArgoCD is set to auto-sync, **it will detect these mutations as out-of-sync drift and immediately overwrite (revert) them back to the old versions declared in Git**, interrupting the upgrade.

### The Solution: `ignoreDifferences` and `RespectIgnoreDifferences=true`
Configure both options below: `ignoreDifferences` suppresses diff reporting, while `RespectIgnoreDifferences=true` also prevents later syncs (including syncs for unrelated changes) from overwriting these live fields. On resource creation there is no live state to preserve, so ArgoCD still applies the bootstrap values from Git. After creation, release changes belong in the matching `HostedClusterUpgrade`, not in the ignored fields of the cluster manifests.

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
    syncOptions:
      - RespectIgnoreDifferences=true
  
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

### B. `nodepools.yaml` (Declares worker NodePools at the bootstrap release)
Omit `pausedUntil` for normal bootstrap. An unannotated `pausedUntil: "true"` is a user-owned hold: the coordinator will neither adopt nor clear it, so it blocks worker upgrades until the user explicitly releases it. Let the coordinator acquire its own temporary locks; do not declare `worker-lock-owner` or `worker-lock-original-pause` annotation values in Git.

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
  # No independent pause; the coordinator manages its own temporary worker lock.
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
  release:
    image: quay.io/openshift-release-dev/ocp-release:4.13.0-x86_64@sha256:74b23ed4bbb593195a721373ed6693687a9b444c97065ce8ac653ba464375711
```

### C. `upgrade.yaml` (Commit this to trigger the automated upgrade to 4.14.0)
After the matching HostedCluster exists, commit this manifest to upgrade from the bootstrap release `4.13.0` to `4.14.0`. Admission requires that HostedCluster to exist; use sync ordering or separate bootstrap and upgrade syncs.
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

## 4. Ownership Contract

- Git owns initial resource creation and non-delegated configuration. After creation, delegate HostedCluster `spec.release.image` and NodePool `spec.release.image` / `spec.pausedUntil` to the coordinator using both ArgoCD settings above. Ignored Git values are still used if a resource is recreated; keep bootstrap manifests appropriate for that case.
- Keep NodePool membership, `spec.clusterName`, and group labels stable during an active upgrade. Create new pools before starting or after completion and release of owned locks, using a release compatible with the current cluster. Adding or recreating pools mid-upgrade is outside this supported workflow; discovery is not a guarantee of continuous locking or schedule enforcement for newly created pools.
- Do not let other automation or manual release writes compete with the coordinator during an active upgrade. External writes or removal of worker locks can bypass ordering, schedules, and pool concurrency limits. This is an operational contract, not admission-enforced exclusive ownership.
- Lock annotations are runtime bookkeeping, not GitOps desired values. Do not template, copy, or force-remove them. Independent user pauses remain user-owned; see the [user guide](user-guide.md#independent-nodepool-pauses) for deliberate pause ownership changes.
- Pausing the `HostedClusterUpgrade` is observe-only, not quiescence: it does not cancel HyperShift work already started, stop external writers, or release locks. Do not treat pause as a safe handoff for pool creation or competing release writes.

## 5. Critical Analysis & Troubleshooting Potential Issues

| Potential Issue | Root Cause | Operator Behavior / Mitigation |
| :--- | :--- | :--- |
| **Initial Bootstrap Ordering** | HostedCluster and upgrade resources are synced together. | Admission requires the HostedCluster to exist first. Order creation; the coordinator then waits in `Installing` while initial installation is incomplete. This is not protection against arbitrary concurrent writers. |
| **Unintended Rollback Drift** | Git still contains an older bootstrap release. | Both `ignoreDifferences` and `RespectIgnoreDifferences=true` preserve delegated live fields on later syncs. Change `targetReleaseImage` (and the optional intermediate) on the existing upgrade CR for the next valid forward upgrade; deletion/recreation is not required. Version safety validation still applies. |
| **NodePool Pausing Lock** | A pool was created with an unannotated `pausedUntil: "true"`, or ArgoCD overwrites a coordinator lock. | Explicitly release the user-owned hold in the live NodePool; changing only the ignored Git field will not do so on later syncs. Configure both ArgoCD options to preserve coordinator changes, and do not manage lock annotation desired values. |
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
