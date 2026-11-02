# Guest Cluster Observation

The HyperShift Upgrade Coordinator (HUC) runs on the management cluster and orchestrates upgrades by modifying `HostedCluster` and `NodePool` specifications. HyperShift and the guest cluster components perform the actual rollouts.

To ensure safety before and during rollouts, HUC directly observes the internal state of the hosted guest cluster. This document describes how guest observation works, how access is secured, and how health is evaluated at every upgrade boundary.

---

## 1. Core Principles

1. **Safety Gating, Not Actuation:** Observation provides safety evidence. HUC never mutates workloads, operator configurations, Subscriptions, or ClusterVersions in the guest cluster.
2. **Least Privilege & Auditability:** HUC accesses the guest cluster using scoped, short-lived bearer tokens created via Kubernetes `TokenRequest`. It never uses admin credentials for routine observation.
3. **No Silent Fallback:** If guest observation is unavailable, degraded, or returns malformed evidence, HUC halts upgrade transitions and reports blockers. It never falls back to unvalidated management-only assumptions.
4. **Fresh Evidence at Action Boundaries:** Persisted `Ready` status alone does not authorize actions. HUC collects a fresh snapshot immediately before any mutating action (starting a control plane hop, admitting a NodePool, or marking an upgrade complete).

---

## 2. Access Lifecycle

```text
Management Cluster:
  HUC Reconciler
    │
    ├─► Reads HostedCluster status.kubeconfig (admin credentials, get-only)
    │
    ├─► Reconciles guest observer ServiceAccount & ClusterRole (observer-v2)
    │
    ├─► Issues short-lived token (1 hour) via TokenRequest API
    │
    └─► Stores scoped observer kubeconfig in management Secret (huc-guest-observer-<HC_UID>)
          │
          ▼
Scoped Guest Reader (uses short-lived token)
    │
    ├─► config.openshift.io/v1 ClusterVersion (get version)
    ├─► config.openshift.io/v1 ClusterOperators (list all)
    ├─► hco.kubevirt.io (v1 / v1beta1) HyperConverged (list, if present)
    └─► kubevirt.io/v1 KubeVirt (list, if present)
```

### Access Bootstrap & Rotation
1. **Discovery:** HUC reads the guest admin kubeconfig referenced by `HostedCluster.status.kubeconfig`.
2. **Provisioning:** HUC idempotently reconciles an observer namespace (default `huc-guest-observer`), ServiceAccount, and ClusterRole (`observer-v2`).
3. **Scoped Permissions:** The guest ClusterRole grants **strictly read-only** access:
   - `config.openshift.io`: `get` on `clusterversions/version`, `list` on `clusteroperators`.
   - `hco.kubevirt.io`: `list` on `hyperconvergeds`.
   - `kubevirt.io`: `list` on `kubevirts`.
   It grants **no** access to secrets, nodes, pods, or mutation endpoints.
4. **Token Issuance:** HUC issues a 1-hour token via `TokenRequest` and publishes a scoped kubeconfig in a Secret (`guest-observer-<HC_UID>`) inside HUC's operator namespace.
5. **Renewal:** HUC automatically renews tokens before expiration with randomized jitter. If renewal fails, the still-valid token is retained until expiration while reporting access degradation.
6. **Profile Migration:** When HUC upgrades from `observer-v1` to `observer-v2`, it automatically and idempotently updates the guest ClusterRole and delivery Secret without disruption.
7. **Cleanup:** When a `HostedClusterUpgrade` is deleted, HUC cleans up the delivery Secret and cached clients.

---

## 3. Observation Contract & Guardrails

### Bounded Snapshots
To prevent unbounded latency and denial of service:
- **Per-request timeout:** 5 seconds.
- **Total collection timeout:** 10 seconds.
- **Maximum data budget:** 4 MiB of decoded JSON across all responses.
- **Maximum operators:** Up to 256 ClusterOperators.
- **Freshness fence:** A snapshot must be less than 30 seconds old at the moment an action is executed.

### Consistency Bracketing
Because reading multiple resources across network calls is not an atomic transaction:
1. `ClusterVersion` is read **before** and **after** reading the operator lists.
2. The snapshot is rejected if `resourceVersion`, generation, desired payload, or history changed during collection.
3. For OpenShift Virtualization, both `HyperConverged` and `KubeVirt` are similarly verified against concurrent modifications.

---

## 4. Assessment Gates & Boundaries

HUC evaluates guest snapshots at specific lifecycle boundaries:

| Phase / Purpose | In-Tree ClusterOperators & ClusterVersion | OpenShift Virtualization (if installed) |
| :--- | :--- | :--- |
| **`SourceBaseline`** (Preflight) | All COs `Available=True`, `Degraded=False`, `Progressing=False`. Validates upgrade eligibility (`Upgradeable=True`) for minor hops. | HCO healthy (`systemHealthStatus: healthy`), `Upgradeable=True`, not degraded. Outdated workloads must be `0`. EUS paths require workload updates disabled. |
| **`BeforeControlPlaneRequest`** | All COs healthy, not degraded. Re-verifies `Upgradeable=True` immediately before mutating HostedCluster spec. | HCO healthy, `Upgradeable=True`. EUS paths verify workload updates remain disabled. |
| **`ObserveControlPlane`** | Tracks progress during control plane rollout. | Monitors that HCO does not become degraded during CP rollout. |
| **`IntermediateVerification`** | Verified at intermediate release: CV completed, all COs matching intermediate release version and Available/not Degraded. | HCO operator version matches intermediate OCP minor release. HCO healthy and `Upgradeable=True`. Workload update methods must remain disabled. |
| **`WorkerEntry`** | Verified at target release: CV completed, all COs matching target release version. | HCO operator version matches target OCP minor release. Workload update methods must be restored (`LiveMigrate`). Outdated workloads must be `0`. |
| **`WorkerGuard`** | Checked before each NodePool rollout: all COs Available and not Degraded. Progressing allowed during rolling node updates. | HCO Available, not Degraded, healthy. Outdated workloads must be `0`. |
| **`FinalUpgradeVerification`** | All COs strictly completed (`Progressing=False`), Available, matching target version. | HCO healthy, matching target version, `0` outdated workloads. |

---

## 5. Status Reporting & Observability

- **Status Field:** High-level verdict and detailed blockers appear in `status.guestAssessment`:
  ```yaml
  status:
    guestAssessment:
      state: Ready # Ready, Converging, Unhealthy, Unavailable, AccessDenied, IdentityMismatch, UnsupportedEvidence
      reason: GuestReady
      purpose: WorkerEntry
      expectedVersion: "4.16.8"
      operators:
        - name: authentication
          version: "4.16.8"
      virtualization:
        name: openshift-virtualization
        version: "4.16.8"
      blockers: []
      blockerCount: 0
  ```
- **Prometheus Metrics:** HUC exports purpose-specific gauges, credential availability, observation age, operator counts, and individual blocker gauges (`hypershift_upgrade_guest_operator_blocker`).
- **Alerts:** Management alert rules fire if credentials expire, collections stall, or blockers persist beyond configured thresholds (see [Monitoring & Alerting](monitoring.md)).
