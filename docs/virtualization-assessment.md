# OpenShift Virtualization Assessment

HUC observes virtualization inside the hosted guest, not the platform hosting its
NodePool machines. It never updates CatalogSources, Subscriptions, InstallPlans,
HCO/KubeVirt configuration, ArgoCD applications, VMs or migrations. The external
operator/GitOps owner remains responsible for those changes.

## Supported evidence

The observer lists HyperConverged resources using `hco.kubevirt.io/v1` when
present, falling back to `hco.kubevirt.io/v1beta1`. KubeVirt resources are
listed using `kubevirt.io/v1`. The supported installation is one HCO and one
KubeVirt, in the same namespace, with a KubeVirt owner reference identifying that
HCO by API version, kind, name and UID. Multiple installations are rejected rather
than selecting the first object. A complete empty HCO list means no instance is
installed. A version-endpoint 404 means absence only if the HCO API group also
returns 404; an existing group with an unsupported API blocks. Forbidden,
malformed, partial, deleting and ambiguous evidence never means absence. No
retained installation inventory is maintained.

Lists require valid type and resource-version evidence, no continuation or
remaining items, and at most one instance. Reads share the existing four-MiB and
ten-second collection budgets, five-second request deadline and 30-second action
age. HCO and KubeVirt are read again to detect changes across collection, inside
the ClusterVersion bracket. Reads are not an atomic transaction or controller
heartbeat proof.

Both resources need valid identities and status observed generations equal to
their own metadata generations. HCO must explicitly report healthy system status,
Available=True, Degraded=False, Progressing=False and ReconcileComplete=True.
Pre-request and intermediate gates also require Upgradeable=True. Missing,
unknown and malformed required evidence blocks. KubeVirt must be Available,
non-Degraded, non-Progressing, with equal nonempty target/observed KubeVirt versions
and deployment IDs. A zero workload count must be explicitly observed; missing,
null, wrong-type and negative counters are not zero.

## Upgrade boundaries

| Boundary | Version | Workloads and external preparation |
| --- | --- | --- |
| Source/preflight | HCO major/minor matches current OCP | Explicit zero outdated workloads; intermediate paths require explicitly disabled automatic updates in both HCO and effective KubeVirt spec |
| Before each CP request | HCO major/minor matches current OCP | Explicit zero, except a verified intermediate source with updates held; holds required until target CP |
| Intermediate verification | HCO major/minor matches intermediate OCP | Outdated workloads allowed; automatic workload updates must remain disabled |
| Each new worker action | HCO major/minor matches target OCP | Explicit zero; intermediate paths require that automatic workload updates have been restored (`CNVWorkloadUpdatesStillHeld` blocks if methods remain empty) and migrations completed |
| Final completion, including no NodePools | HCO major/minor matches target OCP | Explicit zero |

Intermediate workload skew is intentional: requiring zero after the intermediate
operator update while workload updates are disabled could deadlock a valid EUS
path. The same exception permits adoption of an already-reached intermediate
payload, verified through the existing management and guest proofs. It never
applies to the initial source or worker actions.
HCO must have an explicit empty update-method list (its default is LiveMigrate).
The effective KubeVirt list must be empty or omitted: the supported KubeVirt v1
API defines omission as no automatic workload updates and serializes empty lists
with `omitempty`.
Health/version evidence is checked again at action boundaries; historical worker
admission and an earlier healthy preflight do not authorize later actions.
Already-started HyperShift rollouts are not cancelled or relocked by a failed gate.

**Version policy:** a virtualization version matching the current OCP major/minor
(y-stream) is sufficient at each boundary. Its patch (z-stream) need not match the
OCP patch or the latest available virtualization release. This is the accepted
policy, not an outstanding validation gap; no catalog/latest-patch check is needed.
All health, freshness, convergence and workload gates above still apply.

The external owner remains responsible for approvals, disabling/restoring workload
updates and migrations according to the supported release-specific procedure.
Do not interpret `GuestReady` as proof of every VM's migration readiness.

Reference: [Red Hat control-plane-only virtualization updates](https://docs.redhat.com/en/documentation/openshift_container_platform/4.18/html/virtualization/updating).

## Non-migratable VMs and NodePool Drain Blockers

During NodePool worker rollouts, machines are cordoned and drained via the Kubernetes
eviction API. Virtual machine instances (VMIs) running on worker nodes must either
live-migrate to another worker or allow termination during the drain.

### Causes of Non-Migratable VMs
1. **Local storage:** VMs using hostpath provisioners, local PVs, or non-shared storage
   cannot migrate across compute nodes.
2. **Missing eviction strategy:** VMs lacking `spec.template.spec.evictionStrategy: LiveMigrate`
   cannot be live-migrated by KubeVirt during eviction.
3. **Hardware passthrough:** VMs with dedicated host devices (e.g. pinned GPUs, non-SR-IOV
   passthrough devices) that bind to a specific node.
4. **Capacity constraints:** Destination nodes lacking sufficient spare memory, CPU, or
   matching node affinities/tolerations for the migrating VM.

### Coordinator Behavior and Detection
- When a VMI cannot live-migrate, the virt-launcher pod cannot be evicted. The node
  drain will stall until the drain timeout (typically 1 hour) or block the pool rollout.
- HUC observes cluster-level drain failures via best-effort HCCO log collection and
  reports them in `status.drainDiagnostics.messages`.
- The blocked NodePool transitions to `phase: "Blocked"`, reporting `"drain"` blocker.
- SRE Prometheus alerts fire:
  - `HypershiftUpgradeCoordinatorNodePoolStuck` fires after 30 minutes of a stalled pool.
  - `HypershiftUpgradeDrainObservations` fires if eviction errors are actively logged.

### Actionable Remediation for SREs
1. **Audit VMIs prior to upgrade:**
   Check eviction strategies across all namespaces:
   ```bash
   oc get vmi -A -o custom-columns=NAMESPACE:.metadata.namespace,NAME:.metadata.name,STRATEGY:.spec.evictionStrategy
   ```
2. **Workaround for non-migratable VMs:**
   For workloads that do not require continuous availability during machine reboots,
   configure automated power-off during drains:
   - Set `spec.template.spec.evictionStrategy: None`
   - Set `spec.runStrategy: Always`
   This allows the node drain to terminate the VMI cleanly; KubeVirt will restart it
   automatically on a ready worker node after the drain.
3. **Inspect ongoing migrations:**
   If migrations stall during rolling upgrades:
   ```bash
   oc get vmim -A
   ```
   Check `.status.phase` and error messages on VirtualMachineInstanceMigration objects.

## Status and remediation

`status.guestAssessment.virtualization` reports the validated HCO version separately
from the bounded ALL-current-ClusterOperator inventory. Fixed `CNV*` reasons appear
in status and existing guest assessment/operator-blocker metrics. For example,
`CNVWorkloadUpdatesNotHeld` requires external preparation,
`CNVIntermediateVersionMismatch` requires the intermediate virtualization update,
and `CNVOutdatedWorkloads` requires workload updates/migrations. Unknown or stale
evidence must be repaired, not bypassed. Reconciliation retries automatically.

## Observer-profile migration

Profile `observer-v2` adds list-only access to HCO and KubeVirt. Observation never
falls back to administrative credentials. Recognized v1 resources may be migrated
only after existing ownership/target checks and fresh write fences. Migration
updates owned guest resources and the existing delivery Secret in place, publishes
a renewed scoped credential, and is idempotent. Partial migration can be retried.
Unknown profiles and foreign identities are never adopted.

Old-profile credentials cannot authorize actions. While paused, profile migration
and RBAC repair are deferred until resume; no Secret recreation or bootstrap is
introduced. Current-profile renewal retains its previous pause policy. Recognized
old delivery Secrets are still cleaned up when enrollment is removed. Deploy the
updated CRD with the operator image so the separate summary is retained.

Tests cover bounded wire reads, pure policy, real observer RBAC, migration and
simulated production-reconciler paths. They do not replace deployed HyperShift/CNV
testing or establish support for APIs outside the evidence contract above.
