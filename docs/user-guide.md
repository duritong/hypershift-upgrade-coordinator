# HyperShift Upgrade Coordinator User Guide

This user guide provides step-by-step instructions on how to install, configure, and operate the **HyperShift Upgrade Coordinator (HUC)**, as well as how to trigger and monitor automated Hosted Control Plane and NodePool upgrades.

---

## 1. Overview

The HyperShift Upgrade Coordinator is an Kubernetes operator designed to automate staggered, multi-day, or sequential upgrades of OpenShift Hosted Control Planes and worker NodePools.

### Key Capabilities
*   **1:1 Naming Coalignment**: Enforces strict validation that the upgrade resource name exactly matches the target `HostedCluster`.
*   **Staged Multi-Step Hops**: Supports upgrading via intermediate versions (e.g. EUS-to-EUS jumps) automatically.
*   **Scheduled Phases**: Provides separate earliest-start gates for the control plane and worker groups, not bounded maintenance windows.
*   **Group serialization**: Upgrades NodePools in `spec.nodePoolGroups` order using the label `upgrade.huc.0xshift.dev/group`.
*   **Best-Effort Drain Diagnostics**: Collects bounded HCCO logs for cluster-level drain/eviction hints, separately reporting unavailable collection. Logs do not establish NodePool or exact PDB ownership.

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

### Mandatory Guest Access (Unreleased)

Deploy the updated HCU CRD, management RBAC (including the namespace-local
delivery Secret Role/RoleBinding), and operator image together. Existing HCUs are
also enrolled: guest verification is mandatory in the normal workflow, not an
opt-in feature. **Check skipping, operator exclusions, bypass and force flags are
not available.** Automated tests cover simulated complete upgrade flows, real API
authorization and token issuance, and TLS behavior. These do not replace deployed
HyperShift validation of public/private connectivity, credential rotation, actual
node rollouts, fleet capacity or live monitoring before production use.

`--operator-namespace` is required, or supplied through `OPERATOR_NAMESPACE`.
The packaged Deployment sets that environment variable from its Pod namespace
using the Downward API. This is the coordinator's deployment namespace, not the
HostedCluster namespace. `--hosted-cluster-namespace` defaults to `clusters`;
empty selects all HC namespaces and is a reconciliation filter, not an RBAC limit.

The independently queued `guest-access` controller requires the current HC's
`status.kubeconfig` published administrative Secret reference and usable
`spec.clusterID`. Bootstrap first reads guest `ClusterVersion/version`, verifies
its cluster ID against the HC, and discovers its UID. Guest health need not be
good to bootstrap access. Only supported embedded certificates/tokens and a
trusted embedded CA are accepted; plugins, local files, proxies, impersonation
and insecure TLS are rejected. No other admin credential source is substituted.

Endpoint selection is explicit:

- `--guest-endpoint-mode=internal` is the default. It uses
  `kube-apiserver.<derived-control-plane-namespace>.svc`, the actual TCP Service
  port (not targetPort or an assumed 6443), verified HCP association, the matching
  private serving-certificate SAN/SNI, and the trusted guest CA.
- `--guest-endpoint-mode=published` retains the published kubeconfig endpoint and
  verified TLS settings. There is no automatic fallback between modes.

Allow coordinator egress to management APIs and DNS, and to every selected guest
API endpoint. For internal mode, allow ingress to the HCP kube-apiserver Service
from the coordinator namespace on its **Service port**, including the actual
backing listener in pod/network-policy rules. Cross-namespace DNS, private named
serving certificates and CA trust must work from the coordinator Pod. Published
mode requires the corresponding external/private published address to be
reachable. Packaged policies are not proof that fleet-specific guest connectivity
is permitted. A TLS, ownership, port or reachability failure withholds new work;
never work around it by disabling verification.

### Credential Ownership And Lifecycle

The dedicated guest namespace defaults to `huc-guest-observer`. Configure it for
all managed hosted clusters with the deployment-wide flag:

```text
--guest-observer-namespace=custom-guest-observer
```

This is a namespace inside each hosted cluster, not HUC's management-cluster
`--operator-namespace`. It must be a valid Kubernetes namespace name. HUC creates
and owns it; existing unowned or shared namespaces are not adopted. The
ServiceAccount, ClusterRole and binding names remain `huc-guest-observer`.
They are tagged with HC UID, cluster ID and profile revision;
unowned/conflicting resources are refused. The guest role grants **only** `get`
on `config.openshift.io/clusterversions` named `version` and `list` on all
`clusteroperators`. It grants no guest Secret/workload access, mutations or watches.
Owned profile drift is repaired only while active.

Changing this flag invalidates old observer connections and provisions the new
dedicated namespace/ServiceAccount before reissuing credentials and replacing the
owned delivery Secret. It may temporarily withhold upgrade actions while access
converges. Migration waits for resume when an HCU is paused; it cannot bootstrap a
new namespace or identity under the renewal-only exception. Existing credentials
without namespace metadata continue to work only with the unchanged default.
Old guest namespaces and ServiceAccounts are not automatically deleted; after
migration, the owned ClusterRoleBinding refers only to the new ServiceAccount.

TokenRequest requests one hour with API-default audiences and honors the actual
returned expiration. No legacy ServiceAccount token Secret is created. Only the
observer kubeconfig is delivered in `guest-observer-<HC UID>` in the operator
namespace, with source/identity/profile/endpoint/expiry metadata and no
cross-namespace owner reference. Admin and pull Secrets are uncached get-only;
only labeled local delivery Secrets are cached.

The local management Role grants **actual whole-namespace Secret lifecycle
authority**, not an RBAC-enforced prefix or label restriction. Ownership checks
are application safeguards. Existing fleet-wide admin Secret get access remains
powerful; scoped tokens are auditability and privilege-use discipline, not a
sandbox against a compromised coordinator process.

Renewal occurs before expiry with bounded jitter/retries. A failed renewal retains
an existing valid credential; at expiry observations become unavailable. Observer
401/403 never causes ordinary reads to fall back to admin; access maintenance may
verify/repair/renew separately. Maintenance continues while the HCU exists,
including Completed. HCU removal stops issuance and cleans up owned delivery
Secrets using UID/resourceVersion preconditions without depending on guest
availability. Guest read-only profile resources may remain. HC UID replacement
invalidates prior access and observations. Secret deletion does not revoke an
already-issued token; its finite lifetime bounds that risk.

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

  # Earliest start for the Hosted Control Plane
  controlPlane:
    startAt: "2026-08-15T02:00:00Z" # UTC Timestamp

  # Pool concurrency, not a node disruption budget
  rolloutPolicy:
    maxParallelNodePools: 2         # Up to 2 NodePools upgrading in parallel within the active group

  # Ordered rollout sequence: first group in the list upgrades first!
  nodePoolGroups:
    - name: infra
      startAt: "2026-08-15T04:00:00Z"
    - name: compute
      startAt: "2026-08-15T05:00:00Z"
```

Apply the upgrade CR to trigger the automation:
```bash
oc apply -f upgrade.yaml
```

For the next valid forward upgrade, update the existing CR's `targetReleaseImage`
and optional `intermediateReleaseImage`; deletion/recreation is not required.
Release changes remain subject to admission and controller version safety gates.
For GitOps, configure both `ignoreDifferences` and `RespectIgnoreDifferences=true`
as shown in the [GitOps guide](gitops.md). Initial creation applies Git's bootstrap
values; later syncs preserve the delegated live fields. Do not declare worker-lock
annotation values in Git.

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

This release-path optimization is not check skipping. There is no supported
guest-check opt-out, per-operator exclusion, health waiver or force-upgrade flag.

### C. Direct Guest Gates

Every decision uses a new scoped collection: CV, the complete paginated list of
**all current** ClusterOperators, then CV again. Bounds are five seconds per
request, ten seconds per collection, 256 operators, four MiB, and 30 seconds of
decision age. Identity, generation, plan, payload and credential revisions are
rechecked before each action. Persisted Ready/admission status cannot authorize it.

Source baselines and pre-CP requests require strict convergence; intermediate
verification before another hop is strict. At the final-target CP transition,
`WorkerEntry` allows known healthy `Progressing=True`, as does `WorkerGuard`
before each new pool action. Both still require exact target CV desired/latest
Completed evidence, current full operator versions, availability and no degradation,
plus independent HC/HCP/CPC and CAPI/HyperShift proofs. Healthy **old** operator
versions still block. Final overall completion is strict (`Progressing=False`),
including the zero-NodePool path. Existing delegated rollouts are not cancelled
or relocked because guest evidence worsens; new disruptive actions wait.

There is no expected or retained operator inventory: additions are assessed and
removed operators disappear on the next complete snapshot, including their
blockers/metrics. An empty/partial list never passes. This cannot prove that an
independently expected operator is present. Successful reads prove collection
freshness, not an operator heartbeat. Monitoring CO health **does not query firing
alerts**; alert querying is explicitly deferred. No guest operator/OLM execution,
intermediate worker upgrades, or Virtualization-specific checks are implemented.

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
Waits until the Hosted Control Plane's `startAt` earliest-start gate. Missing timestamps mean immediate execution after prerequisites pass; there is no end time or automatic stop when a maintenance window ends.

### C. Upgrading Control Plane (`UpgradingControlPlane` Phase)
Applies the next release version (the `intermediateReleaseImage` if defined and not skipped, or the final `targetReleaseImage`) directly on the `spec.release.image` field of the target `HostedCluster`.

The HostedCluster release also drives guest-cluster operands, not just hosted
control-plane pods. Worker scheduling does not defer those operand updates.
Control-plane upgrades are typically less disruptive to workloads than worker
drains, but API availability, networking, and guest operand changes can affect
workloads; there is no guaranteed disruption-free control-plane phase.

### D. Verifying Control Plane (`VerifyingControlPlane` Phase)
The coordinator does not rely on simple high-level conditions to assume the control plane is upgraded. Instead, it runs exhaustive, low-level component verification:
1.  **Expected Release Image Check**: Ensures that the `HostedCluster.status.releaseImage` has been updated by the underlying HyperShift operator to match the exact expected release image.
2.  **Control Plane Component (CPC) Deep-Verification**: List all `ControlPlaneComponent` (CPC) resources residing in the control plane namespace (e.g., `<hc_namespace>-<cluster_name>`, verifying components like `kube-apiserver`, `etcd`, `openshift-apiserver`):
    *   **Availability**: Asserts that each component's `Available` condition is strictly `True`.
    *   **Rollout Completeness**: Asserts that each component's `RolloutComplete` condition is strictly `True`.
3.  **Complete Readiness Gate**: Verification only succeeds when **all** active control plane components simultaneously report both `Available` and `RolloutComplete` on the expected target version. Only then is the control plane verified and worker NodePool upgrades initiated.

### E. Upgrading NodePools (`UpgradingNodePools` Phase)
Restores owned worker pauses and upgrades NodePools in spec-ordered groups, respecting group `startAt` gates and `rolloutPolicy.maxParallelNodePools` (default one) within the active group. This counts pools, not nodes: per-pool HyperShift rollout settings, replica counts, workload placement, and PDBs determine node-level disruption and available capacity.

### F. Completed (`Completed` Phase)
Sets the final status once all Hosted Control Plane components and worker NodePools are fully verified on the target version.

---

## 7. Pausing & Troubleshooting

### How to Pause the Operator Lifecycle
You can request a pause at any time. Once observed, the coordinator enters read-only "Observe-Only" monitoring mode for upgrade execution. This is not a quiescence barrier: already-started HyperShift rollouts and guest operand updates can continue, external writers are not stopped, and owned locks are not released. Deletion still invokes finalizer cleanup. Do not use pause as a safe ownership handoff for competing writes or new pools.

The narrow credential exception permits verification reads and TokenRequest for
an already-owned guest identity, and **update of an existing delivery Secret**.
Pause does not permit initial bootstrap, RBAC repair, missing-SA/Secret recreation,
or create fallback after an update conflict/NotFound. Observation may become
unavailable at token expiry until resume allows repair.

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

### Independent NodePool Pauses

A NodePool's own `spec.pausedUntil` is independent of the coordinator pause.
Unannotated `"true"` pauses are user-owned and are never automatically adopted or
cleared. Future RFC3339 pauses block worker rollout until expiry; NodePool status
reports the blocker and countdown, and reconciliation is scheduled for expiry.
For normal bootstrap, omit `pausedUntil`; do not pre-pause pools with `"true"`
expecting the coordinator to clear it.

During control-plane work, the coordinator may temporarily extend a timed pause
to `"true"` so it cannot expire before workers are eligible. It records the HCU UID
in `upgrade.huc.0xshift.dev/worker-lock-owner` and the exact original pause in
`upgrade.huc.0xshift.dev/worker-lock-original-pause`. Worker selection restores that
original value, including an absent field or an expired timestamp. Already-ready
pools also have their coordinator locks restored. Foreign-owned locks block
progress rather than being adopted. Failure to acquire worker locks prevents
control-plane mutation.

Deleting a coordinator restores its owned worker pauses before removing its
`upgrade.huc.0xshift.dev/worker-locks` finalizer. It does not revert release images
or cancel rollouts already started by HyperShift. Do not force-remove the
finalizer unless you have restored its owned worker pauses manually.

Changing a coordinator-locked pool to a different pause value preserves your new
value. To replace its `"true"` lock with your own identical `"true"` pause, remove
both lock annotations in the same update: writing the same value alone is not an
observable ownership change. Do not manage these annotations as GitOps desired
state. Pausing the coordinator remains observe-only and does not release locks or
cancel rollouts already started by HyperShift.

### Pool and Release Ownership

Keep pool membership, `spec.clusterName`, and group labels stable during an active
upgrade. Create or recreate pools before starting or after completion and release
of owned locks, with releases compatible with the current cluster. Mid-upgrade
pool creation is outside the supported workflow; discovery does not guarantee
continuous locking or schedule enforcement for new pools. Avoid external release
writes and lock removal during an upgrade: they can bypass ordering, schedules,
and pool concurrency limits. These are operational requirements, not exclusive
ownership enforced by admission. See the [GitOps ownership contract](gitops.md#4-ownership-contract).

### Troubleshooting Blocked Node Drains
During outstanding worker rollouts, the coordinator fetches HCCO logs once per worker reconciliation, after rollout actions. Inspect `oc get hostedclusterupgrade production-cluster-01 -n clusters -o yaml`:

```yaml
status:
  drainDiagnostics:
    available: true
    reason: Collected
    messages:
      - 'node worker-1: failed to drain node: drain failed: global timeout reached'
```

These are **cluster-level observations**, not attributed NodePool blockers. Eviction logs may hint at disruption budgets but do not prove which PDB owns a blockage. Investigate the guest node, pods, and matching PDB selectors before making workload or availability-policy changes.

`available: false` with `CollectionFailed` or `ScraperNotConfigured` means diagnostics are unavailable, not that drains are healthy. Failure does not stop rollout actions. Successful collection with no messages only means no matching unresolved hints were found in the bounded log slice.

Collection has a three-second deadline, 256 KiB byte cap, and a 200-line tail. Timestamped logs are filtered from the earliest outstanding rollout start, with a recent 15-minute fallback for unknown external starts. At most ten deduplicated observations are retained, newest first; later `Drain succeeded on node` messages clear failures attributed to that node, not unrelated or unattributed messages. Log rotation, pod selection, truncation, and missing success messages limit completeness. Refresh follows worker reconciliations, not an independent log-polling loop. No refresh timestamp is written solely to trigger another reconcile.

---

### Maintenance & Keeping HyperShift CRDs Up to Date
To keep the envtest test suite updated with the latest upstream `openshift/hypershift` CustomResourceDefinitions:

```bash
# Fetch and update official CRDs into test/crds/
make update-crds

# Execute test suite against updated schemas
make test
```
