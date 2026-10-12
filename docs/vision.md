# HyperShift Upgrade Coordinator: North Star Vision

This document serves as the vision for all design, architectural, and implementation decisions for the HyperShift Upgrade Coordinator. It outlines the core realities of running massive fleet upgrades, our operational pillars, and how we measure success.

---

## 1. The Operational Context

SRE teams managing large fleets of HyperShift-based OpenShift clusters face a fundamental challenge: **Upgrades are easy to trigger but incredibly hard to validate, orchestrate and observe.**

Currently, upgrading a cluster is done by updating the `.spec.release.image` on the `HostedCluster` and individual `NodePool` CRs. While the HyperShift Operator is excellent at enacting this change (reconstituting the control plane and rolling out new machine configurations), SREs are left with critical gaps:
1. **The Verification Black Box:** There is a severe lack of structured validation, verification, and centralized observability during the rollout. Component rollouts and worker node drains happen asynchronously with minimal visibility.
2. **Disruption Asymmetry:**
   - **Control Plane Upgrades** are potentially disruptive to management-plane components (e.g., cluster operators, controllers, APIs) but are typically **non-disruptive to end-user workloads**.
   - **Worker Node Upgrades** (data plane) are **disruptive** to end-users because they require cordoning, draining, evicting pods, and rebooting or recreating physical/virtual machines.
3. **The GitOps Scheduling Dilemma:** SREs drive infrastructure via GitOps (declarative pull models). However, they cannot simply merge an upgrade pull request at 10:00 AM if they want worker nodes to roll out sequentially at 2:00 AM on consecutive nights. Merging must happen "Upfront" (Day 0), but execution must be precisely scheduled and metered.

---

## 2. Core Architectural Pillars (The North Star)

Every feature, pull request, and refactoring effort must align with these four fundamental pillars:

### I. Multi-Day, Disruption-Aware Scheduling
* **Principle:** Upgrades are not single events; they are multi-stage processes spread across time.
* **Guideline:** The coordinator must allow SREs to declaratively schedule the control plane and individual NodePools independently in a single CR. 
* **Execution:** A typical timeline must be fully supported:
  - **Day 0:** SRE merges the `HostedClusterUpgrade` CR containing the target release, intermediate release, and step-level scheduling windows.
  - **Day 1 (Anytime):** Control plane upgrades. Since it is non-disruptive to users, it can run during business hours as soon as preflight checks pass.
  - **Day 2 (Night Window):** NodePool 1 upgrades (drains and reboots worker nodes).
  - **Day 3 (Night Window):** NodePool 2 upgrades.
  - **Day 4 (Night Window):** NodePool 3 upgrades.

### II. Rigid Step Verification (Zero Trust Rollouts)
* **Principle:** Never assume success; prove it.
* **Guideline:** The coordinator acts as a strict verification gate. No downstream upgrade step (such as rolling out NodePool 1) can proceed unless the upstream step (control plane upgrade) has been exhaustively verified against the expected version and is in a known stable, healthy state.
* **Execution:** A step is only "Verified" when the active version matches the target version, all `ControlPlaneComponent` CRs show `Available=True` and `RolloutComplete=True`, and no alerts are firing.

### III. Proactive SRE Observability & "Step Readiness"
* **Principle:** The operator's main output is clarity, not just actuation.
* **Guideline:** SREs must never ask "what is the coordinator doing?" The CR status and Prometheus metrics must explicitly communicate current status, active blockers, and upcoming actions.
* **Execution:** The coordinator must continuously compute and report "Step Readiness." If the control plane is finished but NodePool 1 is waiting for its scheduled window, the status must clearly report: `Phase: UpgradingNodePools`, `Message: Control Plane upgrade verified. NodePool 'infra-pool' is waiting for its scheduled window which opens on 2026-08-13T02:00:00Z (in 14 hours).`

### IV. Centralized Fleet-Level Architecture
* **Principle:** Fleet management must scale dynamically with cluster lifecycle without operational friction.
* **Guideline:** The coordinator operates as a cluster-scoped fleet manager on the management cluster plane. As new clusters and their associated dynamic control plane namespaces are provisioned at runtime, the coordinator automatically discovers and manages their upgrade lifecycles with zero per-cluster RBAC templating overhead. SREs can optionally restrict management scope via a namespace filter flag when operating in partitioned environments.

---

## 3. Success Metrics (How We Measure Our Design)

Our design is successful if:
1. An SRE can define a complex, 4-day staggered cluster upgrade in **one declarative pull request** and merge it safely during normal working hours.
2. Any stuck node drain is surfaced via a Prometheus alert to the SRE pager **within a certain time** of becoming blocked, complete with the specific blocking pod details.
3. If an unexpected component failure occurs during the intermediate control plane upgrade, the state machine halts immediately, sounds an alert, and **never** touches the data plane (workers).
4. SREs have a single, unified Grafana dashboard displaying the progress, target versions, and estimated completion times for hundreds of concurrent cluster upgrades across the fleet.
