# HyperShift Upgrade Coordinator User Guide

This user guide explains how to install, configure, and operate the **HyperShift Upgrade Coordinator (HUC)** to automate and observe upgrades for OpenShift Hosted Control Planes and worker NodePools.

---

## 1. Overview

The HyperShift Upgrade Coordinator is a Kubernetes operator that automates multi-stage upgrades of OpenShift Hosted Control Planes and worker NodePools.

### Key Capabilities
- **Coaligned Naming:** Binds each `HostedClusterUpgrade` directly to its matching `HostedCluster` by name and namespace.
- **EUS Multi-Hop Upgrades:** Orchestrates 2-minor version hops (e.g. 4.14 → 4.15 → 4.16) in a single declarative resource, holding worker nodes paused until the control plane reaches the target release.
- **Scheduled Rollouts:** Configures independent earliest-start gates (`startAt`) for control plane and worker groups.
- **Group Rollout Sequencing:** Upgrades worker NodePools sequentially by group (`upgrade.huc.0xshift.dev/group`), allowing critical infrastructure pools to verify before general compute nodes upgrade.
- **NodePool Concurrency Bounds:** Limits concurrent NodePool upgrades within an active group (`maxParallelNodePools`).
- **Comprehensive Guest Cluster Observation:** Validates guest `ClusterVersion`, in-tree `ClusterOperators`, and OpenShift Virtualization health at every upgrade step using scoped, short-lived tokens.
- **Automated Drain Diagnostics:** Gathers HCCO logs to surface cluster-level drain and pod eviction blockers during rolling worker updates.

---

## 2. Prerequisites

Before installing the coordinator:
1. An active Kubernetes or OpenShift cluster running the HyperShift operator.
2. CLI access via `oc` or `kubectl` with cluster-admin privileges.
3. The `kustomize` CLI binary (v5 or later).
4. One or more managed `HostedCluster` resources (e.g. in the `clusters` namespace).

---

## 3. Installation & Guest Access Setup

By default, the coordinator runs in the `hypershift-upgrade-coordinator` namespace.

### Step 1: Create the Operator Namespace
```bash
oc create namespace hypershift-upgrade-coordinator
```

### Step 2: Deploy HUC via Kustomize
Deploy all CRDs, RBAC, admission webhooks, Prometheus rules, and the coordinator deployment:
```bash
kustomize build config/default | oc apply -f -
```

### Step 3: Verify the Deployment
Confirm that the coordinator pod is running:
```bash
oc get pods -n hypershift-upgrade-coordinator
```

---

## 4. Configuration Flags & Guest Connectivity

The coordinator supports the following deployment flags:

| Flag | Default | Description |
| :--- | :--- | :--- |
| `--operator-namespace` | Derived from Pod namespace | Namespace where HUC is deployed and stores observer delivery secrets. |
| `--hosted-cluster-namespace` | `clusters` | Filters which namespaces HUC reconciles. Leave empty (`""`) to manage all namespaces across the fleet. |
| `--guest-endpoint-mode` | `internal` | How HUC connects to the guest kube-apiserver: `internal` (uses cluster-internal Service DNS) or `published` (uses external kubeconfig address). |
| `--guest-observer-namespace` | `huc-guest-observer` | Dedicated namespace created inside each hosted guest cluster for observer resources. |

### Network Connectivity Requirements
- **Management Plane:** HUC must have network egress to management cluster APIs and the HCP namespaces (`clusters-<name>`).
- **Guest Cluster Access:**
  - In `internal` mode (default), HUC connects directly to `kube-apiserver.<derived-control-plane-namespace>.svc` on its Service port with private TLS verification. Ensure network policies allow egress from HUC to the control plane namespaces.
  - In `published` mode, ensure the external/published API server endpoint is reachable from the coordinator pod.

### Scoped Observer Credentials
HUC uses least-privilege credentials to inspect the guest cluster:
- **Bootstrap:** Uses the administrative kubeconfig from `HostedCluster.status.kubeconfig` once to establish observer RBAC.
- **Observer Role:** Creates a dedicated ServiceAccount and ClusterRole (`observer-v2`) in the guest cluster with strictly read-only permissions (`get` on ClusterVersion; `list` on ClusterOperators, HyperConverged, and KubeVirt).
- **Short-Lived Tokens:** Issues 1-hour bearer tokens via `TokenRequest` and stores them in a Secret (`guest-observer-<HC_UID>`) inside HUC's deployment namespace.
- **Automatic Renewal:** Automatically rotates tokens before expiration.

---

## 5. Configuring NodePool Groups

Before initiating an upgrade, assign NodePools to logical rollout groups using labels:

```bash
# Infrastructure pools upgrade first:
oc label nodepool infra-pool -n clusters upgrade.huc.0xshift.dev/group=infra

# Compute pools upgrade afterward:
oc label nodepool workers-zone-a -n clusters upgrade.huc.0xshift.dev/group=compute
oc label nodepool workers-zone-b -n clusters upgrade.huc.0xshift.dev/group=compute
```

*Note: Any pool lacking an `upgrade.huc.0xshift.dev/group` label defaults to group `"default"`. Unlabeled pools are automatically upgraded in the final group.*

---

## 6. Triggering an Upgrade

To initiate an upgrade, apply a `HostedClusterUpgrade` manifest matching the target `HostedCluster` name and namespace:

```yaml
apiVersion: upgrade.0xshift.dev/v1alpha1
kind: HostedClusterUpgrade
metadata:
  name: production-cluster-01       # Must match target HostedCluster name exactly
  namespace: clusters               # Must match target HostedCluster namespace
spec:
  # Final target release (digest-pinned)
  targetReleaseImage: quay.io/openshift-release-dev/ocp-release:4.14.0-x86_64@sha256:e2c70fca183e380c6121a1c847806f11e839482123277235a8579db472b9ccf2

  # Optional intermediate release for 2-minor EUS hops
  intermediateReleaseImage: quay.io/openshift-release-dev/ocp-release:4.13.0-x86_64@sha256:74b23ed4bbb593195a721373ed6693687a9b444c97065ce8ac653ba464375711

  # Earliest start for the control plane
  controlPlane:
    startAt: "2026-08-15T02:00:00Z"

  # Maximum concurrent NodePool upgrades within an active group
  rolloutPolicy:
    maxParallelNodePools: 1

  # Ordered sequence of rollout groups
  nodePoolGroups:
    - name: infra
      startAt: "2026-08-15T04:00:00Z"
    - name: compute
      startAt: "2026-08-15T05:00:00Z"
```

Apply the manifest:
```bash
oc apply -f upgrade.yaml
```

To schedule a subsequent upgrade in the future, simply update `targetReleaseImage` (and `intermediateReleaseImage`) in the existing manifest. Deleting and recreating the CR is not required.

---

## 7. Safety Gates & Upgrade Progression

HUC drives upgrades through a deterministic state machine with strict safety gates:

```
[Day 0: Apply CR]
       │
       ▼
[PreflightChecks] ─────► Verifies cluster health, version progression, and worker skew (n-3)
       │
       ▼
[PendingSchedule] ─────► Waits until controlPlane.startAt opens
       │
       ▼
[UpgradingControlPlane] ─► Upgrades CP (intermediate release first if EUS path configured)
       │
       ▼
[VerifyingControlPlane] ─► Verifies 100% of CPCs and guest operators are Available & RolloutComplete
       │
       ▼
[UpgradingNodePools] ────► Sequential group rollouts, respecting schedules and maxParallelNodePools
       │
       ▼
[Completed] ────────────► Final verification of all components and NodePools
```

### Safety Features
- **Strict 1-Minor Jumps:** Validating webhooks reject any single step exceeding 1 minor version jump.
- **Worker Skew Protection (n-3):** Control plane upgrades halt if any worker NodePool is more than 3 minor versions behind the target release.
- **Self-Healing Skip:** If a cluster has already reached the intermediate release, HUC skips the redundant intermediate step and proceeds directly to the target release.
- **Lock Preservation:** If an SRE sets `spec.pausedUntil: "true"` on a NodePool manually, HUC preserves the user pause and will not unpause the pool when upgrades finish.
- **OpenShift Virtualization Gates:** If virtualization is detected, HUC ensures that operator versions align with OpenShift minor versions and that all VMs are ready for live migration before worker nodes drain. See [OpenShift Virtualization Assessment](virtualization-assessment.md).

---

## 8. Monitoring & Troubleshooting

### Tracking Upgrade Progress
Check status and human-readable messages via CLI:
```bash
oc get hostedclusterupgrade production-cluster-01 -n clusters
```

View detailed component progress, blockers, and guest assessments:
```bash
oc describe hostedclusterupgrade production-cluster-01 -n clusters
```

### Troubleshooting Blocked Node Drains
If a worker node drain stalls (e.g. due to PodDisruptionBudgets or non-migratable VMs):
1. Check `status.drainDiagnostics.messages` for recent eviction errors gathered from HCCO logs.
2. Check `status.nodePoolStatuses` to see which pool is blocked.
3. For clusters running OpenShift Virtualization, audit running VMs to ensure they have `evictionStrategy: LiveMigrate` and are not bound to local storage. See [Non-Migratable VMs](virtualization-assessment.md#non-migratable-vms-and-nodepool-drain-blockers).

### Observability & Dashboards
- **Grafana Dashboard:** Import [`docs/monitoring/grafana-dashboard.json`](monitoring/grafana-dashboard.json) to monitor fleet upgrades, phase progressions, and drain hints in real time.
- **Prometheus Alerts:** Pre-packaged alert rules notify SREs if an upgrade is blocked, credentials fail to rotate, or a node drain exceeds SLA thresholds. See [Monitoring & Alerting](monitoring.md).
