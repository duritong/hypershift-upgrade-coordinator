# OpenShift Virtualization Assessment

The HyperShift Upgrade Coordinator (HUC) monitors the health and version compatibility of OpenShift Virtualization inside the hosted guest cluster.

HUC acts purely as a safety gate: it does not modify CatalogSources, Subscriptions, or virtual machine workloads. External GitOps tooling (such as ArgoCD) or cluster administrators remain responsible for orchestrating virtualization updates and VM live migrations.

---

## 1. Supported Architecture & Discovery

- **Supported Topology:** HUC validates clusters running a standard singleton installation of OpenShift Virtualization: one `HyperConverged` resource (typically in `openshift-cnv`) and its associated `KubeVirt` operand resource.
- **Dual API Support:** HUC automatically discovers `HyperConverged` using `hco.kubevirt.io/v1` if available, gracefully falling back to `hco.kubevirt.io/v1beta1`. `KubeVirt` is discovered via `kubevirt.io/v1`.
- **Optional Presence:** If OpenShift Virtualization is not installed on the cluster, HUC detects its absence and skips all virtualization gates without error.
- **Fail-Closed Evidence:** If virtualization custom resources exist but return errors, unpopulated status, or degraded conditions, HUC pauses upgrade transitions until health is verified.

---

## 2. Health & Readiness Criteria

When OpenShift Virtualization is detected, HUC inspects:

1. **System Health:** `HyperConverged.status.systemHealthStatus` must report `"healthy"`.
2. **Aggregated Conditions:** `HyperConverged` conditions must report:
   - `Available = True`
   - `Degraded = False`
   - `Progressing = False` (convergence allowed during rolling worker updates)
   - `ReconcileComplete = True`
   - `Upgradeable = True` (required prior to control plane mutations and during intermediate verification)
3. **KubeVirt Operand Status:** The child `KubeVirt` resource must be healthy and fully converged (`targetKubeVirtVersion == observedKubeVirtVersion` and `targetDeploymentID == observedDeploymentID`).
4. **Outdated Workloads:** `KubeVirt.status.outdatedVirtualMachineInstanceWorkloads` must be `0` before starting an upgrade, before worker rollouts, and at final completion. A non-zero count indicates that running VMs still use older launcher pods from a previous update and must complete live migration.
5. **Freshness:** Both resources must report `observedGeneration == generation`.

---

## 3. Upgrade Boundaries & EUS Flow

| Upgrade Phase | Expected Version | Workload Updates & Migration Rules |
| :--- | :--- | :--- |
| **Source / Preflight** (`SourceBaseline`) | HCO major/minor matches current OCP | Outdated workloads must be `0`. On EUS intermediate paths, automatic workload updates must be disabled (`workloadUpdateMethods: []`). |
| **Before Control Plane Hop** (`BeforeControlPlaneRequest`) | HCO major/minor matches current OCP | Outdated workloads must be `0` (except during a held intermediate hop). Workload updates must remain disabled until target CP completes. |
| **Intermediate CP Verification** (`IntermediateVerification`) | HCO major/minor matches intermediate OCP | Outdated workloads are allowed while updates are held. Workload updates must remain disabled (`[]`). |
| **Worker Rollout Entry** (`WorkerEntry`) | HCO major/minor matches target OCP | Outdated workloads must be `0`. On intermediate paths, workload updates must be restored (`CNVWorkloadUpdatesStillHeld` blocks if methods remain empty). |
| **Active Worker Rollouts** (`WorkerGuard`) | HCO major/minor matches target OCP | Outdated workloads must be `0`. Monitored before admitting each subsequent NodePool. |
| **Final Completion** (`FinalUpgradeVerification`) | HCO major/minor matches target OCP | Outdated workloads must be `0`. Evaluated across both standard and zero-NodePool paths. |

### EUS Workload Update Strategy (Hold & Restore)
During an Extended Update Support (EUS) 2-minor upgrade (e.g., 4.14 → 4.15 → 4.16):
1. **Hold Phase:** Before the control plane updates to the intermediate version, automatic workload updates should be turned off (`workloadUpdateStrategy.workloadUpdateMethods: []`). This prevents VMs from live-migrating back and forth during intermediate control plane updates.
2. **Restore Phase:** Once the control plane reaches the final target release, GitOps restores `workloadUpdateMethods: ["LiveMigrate"]`. HUC holds worker admission (`CNVWorkloadUpdatesStillHeld`) until this configuration is restored, ensuring that live migration is active when worker nodes begin draining.

### Version Alignment Policy
A virtualization version matching the current OpenShift major/minor (**y-stream**) is sufficient at each boundary. The patch level (**z-stream**) does not need to match the OpenShift patch or the latest catalog release. For example, OpenShift Virtualization 4.15.2 satisfies the intermediate gate for OpenShift Container Platform 4.15.28.

---

## 4. Non-Migratable VMs and NodePool Drain Blockers

During NodePool worker rollouts, machines are cordoned and drained via the Kubernetes eviction API. Virtual machine instances (VMIs) running on worker nodes must either live-migrate to another node or allow termination during the drain.

### Common Causes of Stuck Drains
1. **Local Storage:** VMs using hostpath provisioners, local PVs, or non-shared storage cannot migrate across compute nodes.
2. **Missing Eviction Strategy:** VMs lacking `spec.template.spec.evictionStrategy: LiveMigrate` cannot be live-migrated by KubeVirt during eviction.
3. **Hardware Passthrough:** VMs with dedicated host devices (e.g. pinned GPUs or non-SR-IOV devices) bound to a specific physical node.
4. **Capacity Constraints:** Destination worker nodes lacking sufficient spare memory, CPU, or matching node affinities/tolerations for the migrating VM.

### Coordinator Detection & Alerts
- When a VMI cannot live-migrate, the virt-launcher pod blocks node eviction. The node drain will stall until the drain timeout (typically 1 hour).
- HUC observes cluster-level drain failures via best-effort HCCO log collection and reports them in `status.drainDiagnostics.messages`.
- The blocked NodePool transitions to `phase: "Blocked"` with reason `"drain"`.
- SRE Prometheus alerts fire:
  - `HypershiftUpgradeCoordinatorNodePoolStuck` fires after 30 minutes of a stalled pool.
  - `HypershiftUpgradeDrainObservations` fires if eviction errors are actively logged.

### SRE Action & Remediation
1. **Audit VMIs Prior to Upgrade:**
   Identify VMs lacking a live migration strategy:
   ```bash
   oc get vmi -A -o custom-columns=NAMESPACE:.metadata.namespace,NAME:.metadata.name,STRATEGY:.spec.evictionStrategy
   ```
2. **Workaround for Non-Migratable Workloads:**
   For VMs that can tolerate temporary downtime during node reboots:
   - Set `spec.template.spec.evictionStrategy: None`
   - Set `spec.runStrategy: Always`
   This allows the node drain to terminate the VMI cleanly; KubeVirt will restart it automatically on a ready worker node after the drain.
3. **Inspect Active Migrations:**
   If live migrations stall during rolling upgrades:
   ```bash
   oc get vmim -A
   ```
   Inspect `.status.phase` and error messages on VirtualMachineInstanceMigration objects.

---

## 5. Status Telemetry & Alerts

- **Status Summary:** When present, the active operator version appears in `status.guestAssessment.virtualization`.
- **Blockers:** Active virtualization blockers appear in `status.guestAssessment.blockers`:
  - `CNVIntermediateVersionMismatch`: Virtualization has not yet upgraded to the intermediate minor version.
  - `CNVTargetVersionMismatch`: Virtualization has not yet upgraded to the target minor version.
  - `CNVWorkloadUpdatesNotHeld`: Workload updates must be disabled before intermediate control plane upgrades.
  - `CNVWorkloadUpdatesStillHeld`: Workload updates must be restored before worker node rollouts.
  - `CNVOutdatedWorkloads`: Running VMs still have outdated launcher pods; waiting for live migrations to complete.
  - `CNVNotUpgradeable`: HyperConverged reports `Upgradeable=False`.
  - `CNVSystemUnhealthy`: HyperConverged reports an unhealthy system status.
- **Prometheus Alert:** `HypershiftUpgradeGuestVirtualizationBlocked` fires if any virtualization blocker persists for more than 15 minutes during an active upgrade.
