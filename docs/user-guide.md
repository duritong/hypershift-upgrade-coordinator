# HyperShift Upgrade Coordinator User Guide

This user guide provides step-by-step instructions on how to install, configure, and operate the **HyperShift Upgrade Coordinator (HUC)**, as well as how to trigger and monitor automated Hosted Control Plane and NodePool upgrades.

---

## 1. Overview

The HyperShift Upgrade Coordinator is an Kubernetes operator designed to automate staggered, multi-day, or sequential upgrades of OpenShift Hosted Control Planes and worker NodePools.

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

## 4. Configuring NodePool Groups

Before triggering an upgrade, assign your NodePools to logical rollout groups. The sequence of group upgrades is defined explicitly in the upgrade CR, eliminating scheduling conflicts.

### Example Label Commands:
Label your critical infrastructure pools to upgrade first (e.g., group `infra`):
```bash
oc label nodepool infra-pool -n clusters upgrade.huc.0xshift.dev/group=infra
```

Label your general compute worker pools to upgrade next (e.g., group `compute`):
```bash
oc label nodepool workers-us-east-1a -n clusters upgrade.huc.0xshift.dev/group=compute
oc label nodepool workers-us-east-1b -n clusters upgrade.huc.0xshift.dev/group=compute
```

*Note: If a NodePool lacks an upgrade group label, it defaults to the group `"default"`. If `"default"` is not explicitly defined in your upgrade spec, the coordinator automatically appends it to the very end of the execution sequence, upgrading un-labeled NodePools last.*

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
  namespace: clusters               # MUST match target HostedCluster namespace
spec:
  # Target Release Image to upgrade the cluster to (Tag + SHA Digest are mandatory)
  targetReleaseImage: quay.io/openshift-release-dev/ocp-release:4.14.0-x86_64@sha256:e2c70fca183e380c6121a1c847806f11e839482123277235a8579db472b9ccf2

  # Optional EUS Intermediate release hop (Tag + SHA Digest are mandatory)
  intermediateReleaseImage: quay.io/openshift-release-dev/ocp-release:4.13.0-x86_64@sha256:74b23ed4bbb593195a721373ed6693687a9b444c97065ce8ac653ba464375711

  # Maintenance window for the Hosted Control Plane
  controlPlane:
    startAt: "2026-08-15T02:00:00Z" # UTC Timestamp

  # Concurrency settings for NodePool unpausing
  rolloutPolicy:
    maxParallelNodePools: 2         # Up to 2 NodePools upgrading in parallel per priority group

  # Ordered rollout sequence: first group in the list upgrades first!
  nodePoolGroups:
    - name: infra
      startAt: "2026-08-15T04:00:00Z"
    - name: compute
      startAt: "2026-08-15T05:00:00Z"
```
```

Apply the upgrade CR to trigger the automation:
```bash
oc apply -f upgrade.yaml
```

---

## 6. SRE Safety Gates and Skip Behavior

To protect workloads and preserve cluster connectivity, the operator enforces several strict version boundaries:

### A. Strict 1-Minor Version Jump Validation
To respect the maximum 2-minor node-to-control-plane version skew limit in Kubernetes, the validating admission webhook ensures:
*   **Direct upgrades**: If no `intermediateReleaseImage` is specified, the jump from the current version to the target version **cannot exceed 1 minor version** (e.g. `4.13` -> `4.14` is allowed; `4.12` -> `4.14` direct is blocked).
*   **EUS upgrades**: If `intermediateReleaseImage` is specified, both the $Current \rightarrow Intermediate$ and $Intermediate \rightarrow Target$ jumps **cannot exceed exactly 1 minor version** (e.g. `4.12` -> `4.13` -> `4.14` is allowed; `4.12` -> `4.13` -> `4.15` is blocked).
Any configuration violating these limits is immediately rejected by the validating webhook at the API layer.

### B. Self-Healing Skip Behavior
If an SRE specifies an `intermediateReleaseImage` (e.g. `4.13.0`), but the cluster is already running a newer version (e.g. `4.13.12` or `4.14.0` due to manual manual SRE intervention or partial rollouts), the coordinator dynamically **skips** applying the intermediate version and proceeds directly to the `targetReleaseImage`. This protects the cluster from redundant downgrade attempts.

---

## 7. Upgrade Progression and Lifecycle

The coordinator progresses the upgrade through a structured, multi-step state machine:

### A. Preflight Checks (`PreflightChecks` Phase)
Before making any mutations to the cluster, the coordinator performs strict, automated health-gate checks. These checks are designed to prevent initiating upgrades on cluster setups that are in a compromised or unfinished state:
1.  **Installation Phase Check**: Inspects `.status.version.history` on the target `HostedCluster`. If no history entries are present (or if the initial installation progressing status is active), the coordinator holds the upgrade, transitions to the `Installing` phase, and waits for the initial installation to complete successfully.
2.  **Cluster Degradation Checks**: Iterates over all conditions inside `.status.conditions` on the `HostedCluster`:
    *   **Failing Condition Gate**: If the `Failing` condition is `True`, the upgrade is blocked, transitioning the coordinator to `Blocked` with the failure message.
    *   **Degraded Condition Gate**: If the `Degraded` condition is `True`, the upgrade is blocked, transitioning the coordinator to `Blocked` with the degraded details.
3.  **Pre-Step Health Re-verification**: These preflight checks do not run only once. The coordinator **re-runs these exact health evaluations immediately before initiating any transition** (e.g. unpausing a NodePool or shifting between EUS intermediate and target control plane versions) to ensure safety throughout the entire lifecycle.

### B. Pending Schedule (`PendingSchedule` Phase)
Waits until the Hosted Control Plane's scheduled `startAt` maintenance window opens.

### C. Upgrading Control Plane (`UpgradingControlPlane` Phase)
Applies the next release version (the `intermediateReleaseImage` if defined and not skipped, or the final `targetReleaseImage`) directly on the `spec.release.image` field of the target `HostedCluster`.

### D. Verifying Control Plane (`VerifyingControlPlane` Phase)
The coordinator does not rely on simple high-level conditions to assume the control plane is upgraded. Instead, it runs exhaustive, low-level component verification:
1.  **Expected Release Image Check**: Ensures that the `HostedCluster.status.releaseImage` has been updated by the underlying HyperShift operator to match the exact expected release image.
2.  **Control Plane Component (CPC) Deep-Verification**: List all `ControlPlaneComponent` (CPC) resources residing in the control plane namespace (e.g., `<hc_namespace>-<cluster_name>`, verifying components like `kube-apiserver`, `etcd`, `openshift-apiserver`):
    *   **Availability**: Asserts that each component's `Available` condition is strictly `True`.
    *   **Rollout Completeness**: Asserts that each component's `RolloutComplete` condition is strictly `True`.
3.  **Complete Readiness Gate**: Verification only succeeds when **all** active control plane components simultaneously report both `Available` and `RolloutComplete` on the expected target version. Only then is the control plane verified and worker NodePool upgrades initiated.

### E. Upgrading NodePools (`UpgradingNodePools` Phase)
Unpauses and upgrades worker NodePools in sequential order based on spec-ordered groups, respecting group `startAt` schedules and global concurrency limits.

### F. Completed (`Completed` Phase)
Sets the final status once all Hosted Control Plane components and worker NodePools are fully verified on the target version.

---

## 7. Pausing & Troubleshooting

### How to Pause the Operator Lifecycle
You can pause the entire upgrade coordinator at any time. This halts all state transitions and spec modifications, entering a safe, read-only "Observe-Only" monitoring mode.

#### Option A: Indefinite Pause
Set `spec.pausedUntil: "true"` on your upgrade resource:
```bash
oc patch hostedclusterupgrade production-cluster-01 -n clusters --type=merge -p '{"spec":{"pausedUntil":"true"}}'
```

#### Option B: Scheduled Pause (Maintenance Hold)
Set `spec.pausedUntil` to a future RFC3339 timestamp. The operator will remain paused until that time is reached, after which it will resume automatically:
```bash
oc patch hostedclusterupgrade production-cluster-01 -n clusters --type=merge -p '{"spec":{"pausedUntil":"2026-08-16T12:00:00Z"}}'
```

#### How to Resume
Set `spec.pausedUntil: "false"` (or remove the field) to resume coordinator operations:
```bash
oc patch hostedclusterupgrade production-cluster-01 -n clusters --type=merge -p '{"spec":{"pausedUntil":"false"}}'
```

---

### Troubleshooting Blocked Node Drains
If a worker NodePool rollout stalls, the coordinator automatically scrapes the logs of the `hosted-cluster-config-operator` (HCCO) inside the target control plane namespace. 

1.  **Check CR status**: View detailed blocker reasons, stuck PDB names, or eviction errors directly inside the status:
    ```bash
    oc get hostedclusterupgrade production-cluster-01 -n clusters -o yaml
    ```
2.  **Inspect the NodePool blockers section**:
    ```yaml
    status:
      nodePoolStatuses:
        - name: workers-us-east-1a
          phase: Upgrading
          blockers:
            - "failed to drain node 'ip-10-0-142-3.ec2.internal': cannot evict pod 'my-app-pod' in namespace 'app' due to PDB 'my-app-pdb'"
    ```
3.  **Resolve blockages**: Temporarily scale down or bypass the offending PDBs/deployments inside the guest cluster to allow the node drainer to proceed smoothly.

---

### Maintenance & Keeping HyperShift CRDs Up to Date
To keep the envtest test suite updated with the latest upstream `openshift/hypershift` CustomResourceDefinitions:

```bash
# Fetch and update official CRDs into test/crds/
make update-crds

# Execute test suite against updated schemas
make test
```
