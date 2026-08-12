# HyperShift Upgrade Coordinator User Guide

This user guide provides step-by-step instructions on how to install, configure, and operate the **HyperShift Upgrade Coordinator (HUC)**, as well as how to trigger and monitor automated Hosted Control Plane and NodePool upgrades.

---

## 1. Overview

The HyperShift Upgrade Coordinator is an offline-capable, highly observable Kubernetes operator designed to automate staggered, multi-day, or sequential upgrades of OpenShift Hosted Control Planes and worker NodePools.

### Key Capabilities
*   **1:1 Naming Coalignment**: Enforces strict validation that the upgrade resource name exactly matches the target `HostedCluster`.
*   **Staged Multi-Step Hops**: Supports upgrading via intermediate versions (e.g. EUS-to-EUS jumps) automatically.
*   **Scheduled Phases**: Allows separate, chronological maintenance windows for the control plane and worker pools.
*   **Priority serialization**: Groups and unpauses NodePool upgrades sequentially according to custom label ordering (`upgrade.huc.0xshift.dev/order`).
*   **Live SRE Diagnostics**: Scrapes HCCO pod logs to extract PDBs and node drain blockages in real-time.

---

## 2. Prerequisites

Before setting up the coordinator, ensure you have:
1.  An active OpenShift or Kubernetes cluster where HyperShift is deployed.
2.  Administrative access to the cluster via the `oc` or `kubectl` CLI.
3.  The `kustomize` client binary (installed natively).
4.  One or more managed `HostedCluster` resources deployed inside your cluster (defaulting to the `clusters` namespace or custom target namespaces).

---

## 3. Installation & Setup

By default, the operator runs inside the dedicated namespace `hypershift-upgrade-coordinator`. 

### Step 1: Create the target namespace
```bash
oc create namespace hypershift-upgrade-coordinator
```

### Step 2: Deploy using Kustomize
Deploy all Custom Resource Definitions, RBAC configurations, validating webhooks, Prometheus monitors, NetworkPolicies, and the manager deployment in a single command:
```bash
kustomize build config/default | oc apply -f -
```

### Step 3: Verify the deployment
Verify that the coordinator manager pod is running successfully:
```bash
oc get pods -n hypershift-upgrade-coordinator
```

---

## 4. Configuring NodePool Priorities

Before triggering an upgrade, label your NodePools to define their serialized upgrade sequence. Lower order numbers are upgraded first. NodePools sharing the same order number are upgraded concurrently up to your specified parallelism limit.

### Example Label Commands:
Label your critical infrastructure pools to upgrade first (e.g., order `10`):
```bash
oc label nodepool infra-pool -n default upgrade.huc.0xshift.dev/order=10
```

Label your general compute worker pools to upgrade next (e.g., order `50`):
```bash
oc label nodepool workers-us-east-1a -n default upgrade.huc.0xshift.dev/order=50
oc label nodepool workers-us-east-1b -n default upgrade.huc.0xshift.dev/order=50
```

*Note: If a NodePool lacks an upgrade order label, it defaults to order `50` automatically.*

---

## 5. Triggering an Upgrade

To initiate an upgrade, create a `HostedClusterUpgrade` Custom Resource (CR).

### Coaligned Naming Mandate
The CR **name** and **namespace** must exactly match the name and namespace of the target `HostedCluster` you wish to upgrade.

### Complete Upgrade Manifest Example (`upgrade.yaml`)
```yaml
apiVersion: upgrade.0xshift.dev/v1alpha1
kind: HostedClusterUpgrade
metadata:
  name: production-cluster-01       # MUST match target HostedCluster name
  namespace: default                # MUST match target HostedCluster namespace
spec:
  # Target Release Image to upgrade the cluster to
  targetReleaseImage: quay.io/openshift-release-dev/ocp-release:4.14.10-x86_64

  # Optional: Intermediate hops (e.g. EUS-to-EUS jumps)
  intermediateReleaseImages:
    - quay.io/openshift-release-dev/ocp-release:4.13.20-x86_64

  # Maintenance window for the Hosted Control Plane
  controlPlane:
    startAt: "2026-08-15T02:00:00Z" # UTC Timestamp

  # Concurrency settings for NodePool unpausing
  rolloutPolicy:
    maxParallelNodePools: 2         # Up to 2 NodePools upgrading in parallel per priority group

  # Chronological start windows for specific NodePools
  nodePools:
    - name: infra-pool
      startAt: "2026-08-15T04:00:00Z"
    - name: workers-us-east-1a
      startAt: "2026-08-15T05:00:00Z"
    - name: workers-us-east-1b
      startAt: "2026-08-15T05:00:00Z"
```

Apply the upgrade CR to trigger the automation:
```bash
oc apply -f upgrade.yaml
```

---

## 6. Upgrade Progression and Lifecycle

The coordinator progresses the upgrade through a structured state machine:

1.  **Preflight Checks (`PreflightChecks`)**: Asserts that the target `HostedCluster` is healthy, not currently in initial installation, and contains no failing/degraded conditions.
2.  **Pending Schedule (`PendingSchedule`)**: Waits until the Control Plane's scheduled `startAt` window opens.
3.  **Upgrading Control Plane (`UpgradingControlPlane`)**: Sets the control plane release image on the `HostedCluster` resource (initiating the first intermediate hop or the final target image).
4.  **Verifying Control Plane (`VerifyingControlPlane`)**: Waits for all individual `ControlPlaneComponents` (apiserver, etcd, etc.) to report `Available` and `RolloutComplete` at the expected version.
5.  **Upgrading NodePools (`UpgradingNodePools`)**: Unpauses and upgrades worker NodePools in sequential order based on label priorities, respecting individual `startAt` schedules and the concurrency policy.
6.  **Completed (`Completed`)**: Sets the final status once all Hosted Control Plane components and worker NodePools are fully verified on the target version.

---

## 7. Pausing & Troubleshooting

### How to Pause the Operator Lifecycle
You can pause the entire upgrade coordinator at any time. This halts all state transitions and spec modifications, entering a safe, read-only "Observe-Only" monitoring mode.

#### Option A: Indefinite Pause
Set `spec.pausedUntil: "true"` on your upgrade resource:
```bash
oc patch hostedclusterupgrade production-cluster-01 -n default --type=merge -p '{"spec":{"pausedUntil":"true"}}'
```

#### Option B: Scheduled Pause (Maintenance Hold)
Set `spec.pausedUntil` to a future RFC3339 timestamp. The operator will remain paused until that time is reached, after which it will resume automatically:
```bash
oc patch hostedclusterupgrade production-cluster-01 -n default --type=merge -p '{"spec":{"pausedUntil":"2026-08-16T12:00:00Z"}}'
```

#### How to Resume
Set `spec.pausedUntil: "false"` (or remove the field) to resume coordinator operations:
```bash
oc patch hostedclusterupgrade production-cluster-01 -n default --type=merge -p '{"spec":{"pausedUntil":"false"}}'
```

---

### Troubleshooting Blocked Node Drains
If a worker NodePool rollout stalls, the coordinator automatically scrapes the logs of the `hosted-cluster-config-operator` (HCCO) inside the target control plane namespace. 

1.  **Check CR status**: View detailed blocker reasons, stuck PDB names, or eviction errors directly inside the status:
    ```bash
    oc get hostedclusterupgrade production-cluster-01 -n default -o yaml
    ```
2.  **Inspect the NodePool blockers section**:
    ```yaml
    status:
      nodePoolStatuses:
        - name: workers-us-east-1a
          phase: Upgrading
          blockers:
            - "cannot evict pod 'my-app-pod' in namespace 'app' due to PDB 'my-app-pdb'"
    ```
3.  **Resolve blockages**: Temporarily scale down or bypass the offending PDBs/deployments inside the guest cluster to allow the node drainer to proceed smoothly.
