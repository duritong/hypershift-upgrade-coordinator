# HyperShift Upgrade Coordinator

The **HyperShift Upgrade Coordinator** is an automation controller designed to help SRE teams upgrade large fleets of OpenShift Hosted Control Planes (HCP) in a safe, controlled, and highly observable manner. 

It manages the entire lifecycle of cluster upgrades—from preflight health checks, control plane multi-hop sequencing (such as Extended Update Support upgrades), worker node pool draining, to final verification—using standard Kubernetes Custom Resources (CRs) as the single source of truth.

---

## 🛠️ What the `HostedClusterUpgrade` CRD Enables

Upgrading a HyperShift cluster traditionally requires SREs to manually coordinate several low-level resources, pause NodePools to prevent version-skew breakage, sequentially edit images, and monitor drains. 

The `HostedClusterUpgrade` Custom Resource Definition (CRD) completely simplifies this. SREs define the **entire multi-step upgrade path once as a single declarative document** in Git, enabling:

*   **Disruption-Aware Step Scheduling:** Schedule the control plane upgrade (non-disruptive to end-users) and the worker NodePool reboots (highly disruptive machine drains) to occur during completely separate maintenance windows.
*   **Declarative EUS (Extended Update Support) Multi-Minor Jumps:** Declare a 2-minor version upgrade path (e.g., `4.12` -> `4.14` via EUS intermediate `4.13`) in a single field. The coordinator takes care of the sequential hops while keeping NodePools safely locked on the old version until the control plane reaches the final version.
*   **Group-Based Ordered Rollouts:** Group worker NodePools into sequential upgrade phases using labels (`upgrade.huc.0xshift.dev/group`), enforcing that critical infrastructure nodes upgrade and verify before general compute nodes are touched.
*   **Global Concurrency Limits:** Cap the number of NodePools allowed to roll out concurrently within a priority group (via `rolloutPolicy.maxParallelNodePools`) to preserve cluster capacity.
*   **Granular Pause & Resume Support:** Features both coordinator-level pausing (via `pausedUntil`, which supports indefinite pausing with `"true"` or scheduled pausing with an RFC3339 timestamp) and object-level control using HyperShift's native `spec.pausedUntil` fields.
*   **Least Privilege & Multi-Namespace Isolation:** Operates **without any cluster-wide permissions**. It takes a flag targeting a single HostedCluster namespace, allowing separate isolated controller instances to run concurrently on the same cluster for different tenants.
*   **GitOps & ArgoCD Friendly:**  Upgrades can be merged upfront on Day 0 via GitOps, but executed staggered on Day 1 (non-disruptive Control Plane upgrade) and consecutive nights (disruptive worker NodePool reboots).

---

## 📋 Declarative Usage Example

SREs simply commit a single manifest matching the target `HostedCluster` name and namespace to trigger a safe, observed rollout:

```yaml
apiVersion: upgrade.0xshift.dev/v1alpha1
kind: HostedClusterUpgrade
metadata:
  name: production-cluster-01       # Must match target HostedCluster name exactly
  namespace: clusters               # Must match target HostedCluster namespace
spec:
  # Desired final version tag + SHA-256 digest (Mandatory)
  targetReleaseImage: quay.io/openshift-release-dev/ocp-release:4.14.0-x86_64@sha256:e2c70fca183e380c6121a1c847806f11e839482123277235a8579db472b9ccf2

  # Optional EUS intermediate release hop tag + SHA-256 digest
  intermediateReleaseImage: quay.io/openshift-release-dev/ocp-release:4.13.0-x86_64@sha256:74b23ed4bbb593195a721373ed6693687a9b444c97065ce8ac653ba464375711

  # Maintenance window for the Hosted Control Plane
  controlPlane:
    startAt: "2026-08-15T02:00:00Z"

  # Concurrency limits for NodePool unpausing
  rolloutPolicy:
    maxParallelNodePools: 1

  # Chronological start windows for individual NodePools
  nodePools:
    - name: workers-us-east-1a
      startAt: "2026-08-15T04:00:00Z"
    - name: workers-us-east-1b
      startAt: "2026-08-15T05:00:00Z"
```

---

## 👶 How the Coordinator Supervises the Upgrade Process

Once you apply a `HostedClusterUpgrade` CR, the coordinator acts as a vigilant, 24/7 SRE superviser, carrying the cluster safely through every stage of the lifecycle:

```
[Day 0: Apply CR] ──► [Preflight Gates] ──► [Scheduling Gate] ──► [Control Plane Rollout] ──► [Component Verification] ──► [Priority Serialized Rollout] ──► [Blocked Drain Log-Scraping] ──► [Verification & Completed]
```

1.  **Preflight Health Gates:**
    *   Verifies the cluster is not in its initial installation phase.
    *   Checks that there are no active `Failing` or `Degraded` conditions on the `HostedCluster`.
    *   *If unhealthy, the operator halts instantly and sounds the alarm (via Prometheus alerts).*
2.  **Scheduling Window Gate:**
    *   Computes exact count-downs and halts until your scheduled maintenance window opens.
3.  **Control Plane Hops (EUS Hops):**
    *   Automatically locks/pauses all worker NodePools to guarantee zero control-plane-to-node version skew breakage.
    *   Upgrades the control plane to the intermediate version first, verifies it, and only then proceeds to the final target version image.
    *   *Self-Healing Skip:* If the cluster is already running a newer version than the intermediate, the operator intelligently skips it.
4.  **Control Plane Verification:**
    *   Queries low-level `ControlPlaneComponent` (CPC) resources (etcd, apiserver, etc.).
    *   Demands that **100% of components** are both `Available=True` and `RolloutComplete=True` before touching any worker nodes.
5.  **Group-Serialized NodePool Rollouts:**
    *   Evaluates all associated NodePools, clustering them into spec-ordered rollout groups.
    *   Unpauses and upgrades groups sequentially, respecting your defined maximum parallelism limits.
6.  **Troubleshooting & Drain Supervising:**
    *   If a node drain stalls, the coordinator **scrapes the logs** of the `hosted-cluster-config-operator` (HCCO) inside the control plane.
    *   It extracts exact Pod Disruption Budget (PDB) blockers and identifies the specific blocked host name, surfacing them directly in the CR status and Grafana.
7.  **Final Verification & Sign-off:**
    *   Ensures all control plane components and worker nodes are successfully running and verified on the target version.
    *   Transitions to `Completed`, records duration analytics, and silences active alerts.

---

## 📂 Documentation

Detailed designs, guides, and specifications can be found in the `docs/` folder:

* **[User Guide](docs/user-guide.md):** Comprehensive step-by-step instructions on operator installation, setup, triggering upgrades, configuring NodePool groups, and troubleshooting blocked node drains.
* **[GitOps Integration Guide](docs/gitops.md):** Detailed guide on deploying clusters and worker nodes via ArgoCD without reconciliation fights, complete with ignore-differences rules and custom health scripts.
* **[North Star Vision](docs/vision.md):** Proactive SRE vision, core pillars (staggered scheduling, zero-trust verification, readiness dashboards), and operational context.
* **[Architecture & Design Doc](docs/architecture.md):** Detailed explanation of the state machine scheduling gates, Extended Update Support (EUS) mechanics, group-based NodePool sequence unpausing, multi-namespace least-privilege security model, and pause/resume execution.
* **[CRD Specification](docs/crd.md):** The Custom Resource schema (`HostedClusterUpgrade`), including spec configuration knobs and status telemetry fields.
* **[Monitoring & Alerting](docs/monitoring.md):** Specification of Prometheus alerts (`PrometheusRule` custom resources) and SRE Grafana dashboard layouts.

---

## 🏗️ Technical Stack

The operator is built on standard Kubernetes operator tooling:
- **Language:** Go
- **Framework:** `operator-sdk` (Go v4 plugin, using `controller-runtime` under the hood)
- **APIs Consumed:**
  - `hypershift.openshift.io/v1beta1` (`HostedCluster`, `NodePool`, `HostedControlPlane`)
  - `controlplane.hypershift.openshift.io/v1alpha1` (`ControlPlaneComponent`)
- **Metrics:** Exposes Prometheus metrics via `/metrics` endpoint (such as active upgrade phase, duration, and blocked NodePool counts).
