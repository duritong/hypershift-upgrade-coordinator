# HyperShift Upgrade Coordinator

The **HyperShift Upgrade Coordinator** is an automation controller designed to help SRE teams upgrade large fleets of OpenShift Hosted Control Planes (HCP) in a safe, controlled, and highly observable manner. 

It manages the entire lifecycle of cluster upgrades—from preflight health checks, control plane multi-hop sequencing (such as Extended Update Support upgrades), worker node pool draining, to final verification—using standard Kubernetes Custom Resources (CRs) as the single source of truth.

---

## 🚀 Key Features

* **Disruption-Aware Step Scheduling:** Upgrades can be merged upfront on Day 0 via GitOps, but executed staggered on Day 1 (non-disruptive Control Plane upgrade) and consecutive nights (disruptive worker NodePool reboots).
* **Step-Level Countdown Observability:** Continuously computes and communicates "Step Readiness" and countdown messages directly in `.status.message` (e.g. waiting for a maintenance window that opens in 14 hours).
* **Safety Gates & Preflight Verification:** Never initiates upgrades unless the target cluster is in a known good state. It handles clusters currently in their initial installation phase, ensures that each step of the upgrade is 100% verified against the expected target version, and assesses all `ControlPlaneComponent` statuses and node health before proceeding.
* **Extended Update Support (EUS) Multi-Hop:** Safely coordinates jumps over two minor versions (e.g., `4.20.x` to `4.22.y`). The coordinator orchestrates the control plane to transition through an intermediate release (`4.21.z`), while keeping NodePools paused. Once the control plane reaches the target version (`4.22.y`), the NodePools are directly upgraded to the final target version.
* **Granular Pause and Resume Support:** Features both coordinator-level pausing (via `pausedUntil`, which supports indefinite pausing with `"true"` or scheduled pausing with an RFC3339 timestamp) and object-level control using HyperShift's native `spec.pausedUntil` fields.
* **Label-Based NodePool Ordering:** Upgrades NodePools according to decentralized label configuration (`upgrade.hypershift.openshift.io/order`). NodePools with lower values are upgraded earlier; those with matching values are upgraded in parallel.
* **Draining Obstacle Detection:** Inspects NodePool rollouts and monitors the `hosted-cluster-config-operator` (HCCO) or machine controllers to identify stuck node drains (e.g., due to Pod Disruption Budgets (PDBs) or local storage) and surfaces detailed blockers directly in the CR status and Prometheus metrics.
* **Least Privilege & Multi-Namespace Isolation:** Operates **without any cluster-wide permissions**. It takes a flag targeting a single HostedCluster namespace, allowing separate isolated controller instances to run concurrently on the same cluster for different tenants.
* **GitOps Friendly:** Built with standard API condition patterns compatible with ArgoCD health checks.

---

## 📂 Documentation

Detailed designs and specifications can be found in the `docs/` folder:

* **[North Star Vision](docs/vision.md):** Proactive SRE vision, core pillars (staggered scheduling, zero-trust verification, readiness dashboards), and operational context.
* **[Architecture & Design Doc](docs/architecture.md):** Detailed explanation of the state machine scheduling gates, Extended Update Support (EUS) mechanics, label-based NodePool ordering, multi-namespace least-privilege security model, and pause/resume execution.
* **[CRD Specification](docs/crd.md):** The Custom Resource schema (`HostedClusterUpgrade`), including API layout, struct definitions, and ArgoCD integration.
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
