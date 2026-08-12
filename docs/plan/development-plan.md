# HyperShift Upgrade Coordinator: Iterative Development & Verification Plan

This document outlines the step-by-step, zero-regression development roadmap for the HyperShift Upgrade Coordinator. Each milestone represents a clear, testable, and verifiable increment toward the final production-ready operator.

---

## 🛠️ Global Technical Standards

To satisfy SRE security, disconnected environment compliance, and quality standards, the codebase must strictly adhere to the following rules:

1. **Language & Environment:** Built using **Golang 1.26**.
2. **Hardened Red Hat Base Images:** The operator container image is compiled via `Containerfile` using:
   - **Builder:** `registry.access.redhat.com/hi/go:1.26`
   - **Runtime:** `registry.access.redhat.com/hi/core-runtime:latest` (or `registry.access.redhat.com/hi/static:latest` if fully statically linked).
3. **Disconnected Compliance (Offline-Capable):** All Go dependencies must be vendored using `go mod vendor` and committed directly to the git repository. Standard builds must run with `-mod=vendor`.
4. **No Binary Commits:** Binaries, swp files, and build outputs are strictly kept out of Git via `.gitignore`.
5. **Container Tooling:** The `Makefile` must drive compilation using standard `podman` or `buildah` for container builds.
6. **Integration Testing (`envtest`):** The testing suite will spin up a real, local Kubernetes API server via controller-runtime's `envtest` to validate webhook assertions, controller cache watches, and state-machine transitions.
7. **Deployment Target Spec:** 
   - **Image Path:** `quay.io/duritong/huc:latest`
   - **Deploy Namespace:** `hypershift-upgrade-coordinator`
   - **Network Security:** Must include OpenShift-compliant `NetworkPolicy` objects that permit ingress scraping to `/metrics` from the `openshift-monitoring` namespace.
8. **Grafana Validation Gate:** The Grafana dashboard JSON schema must be syntactically validated during compilation/testing using a dedicated unit test in Go (unmarshalling the JSON payload to verify structural and syntax correctness) to guarantee zero formatting errors.

---

## 🗺️ Milestone Roadmap

The development is divided into 6 distinct, sequential milestones.

```
┌────────────────────────────────────────────────────────┐
│  Milestone 1: Scaffolding, Hardened Build, & Vendoring  │
└───────────────────────────┬────────────────────────────┘
                            │
                            ▼
┌────────────────────────────────────────────────────────┐
│  Milestone 2: API Definitions, Webhook, & envtest Setup│
└───────────────────────────┬────────────────────────────┘
                            │
                            ▼
┌────────────────────────────────────────────────────────┐
│  Milestone 3: Reconciler Skelet, Watches & Status Agg  │
└───────────────────────────┬────────────────────────────┘
                            │
                            ▼
┌────────────────────────────────────────────────────────┐
│  Milestone 4: State Machine - Preflight & CP Upgrades   │
└───────────────────────────┬────────────────────────────┘
                            │
                            ▼
┌────────────────────────────────────────────────────────┐
│  Milestone 5: Priority NodePools & HCCO Log-Scraping   │
└───────────────────────────┬────────────────────────────┘
                            │
                            ▼
┌────────────────────────────────────────────────────────┐
│  Milestone 6: Kustomize, NetPols, SRE Metrics & Grafana│
└────────────────────────────────────────────────────────┘
```

---

### **Milestone 1: Scaffolding, Hardened Build, & Vendoring**
* **Goal:** Scaffolds the workspace, configures Go 1.26, and validates offline-capable compilation.
* **Deliverables:**
  - `go.mod` (configured for Go 1.26).
  - `Containerfile` specifying the Red Hat hardened multi-stage build.
  - `Makefile` containing `make build` (using `podman` or `buildah`), `make test`, and `make vendor`.
  - Scaffolding of the project structure via `operator-sdk` under the `upgrade.0xshift.dev` API group.
* **Verification Strategy:**
  - Execute `make vendor` and verify the `/vendor` folder is populated and added to git.
  - Execute `make build` using Podman (targeting image `quay.io/duritong/huc:latest`) and verify the container compiles and packages successfully.
  - Execute `make test` and verify that the boilerplate suite executes cleanly with 0 failures.

---

### **Milestone 2: API Definitions, Webhook, & `envtest` Setup**
* **Goal:** Create the core API structures and establish the validating webhook.
* **Deliverables:**
  - `api/v1alpha1/hostedclusterupgrade_types.go` containing structures from `docs/crd.md`.
  - Registration of the validating admission webhook.
  - Logic implementing **1:1 Naming Constraint**, **monotonic version progression**, and **StartAt label-priority temporal checks**.
  - Boilerplate `envtest` suite initialization in the test folder.
* **Verification Strategy:**
  - **Unit Tests:** Mock SemVer comparisons to test version string extraction (`4.20.44-x86_64` ➔ `4.20.44`).
  - **`envtest` Integration Tests:** Start `envtest` and attempt to apply:
    - A CR with a name mismatching its `HostedCluster`. (Must be rejected).
    - A CR representing a downgrade (e.g., `4.22.2` to `4.21.5`). (Must be rejected).
    - A CR representing a lateral upgrade (identical version tags and identical digests). (Must be rejected).
    - An upgrade CR with a NodePool `StartAt` scheduled *earlier* than a lower-order NodePool. (Must be rejected).

---

### **Milestone 3: Reconciler Skeleton & Event-Driven Watches**
* **Goal:** Initialize the controller reconciler and build the read-only observability pipeline.
* **Deliverables:**
  - `internal/controller/hostedclusterupgrade_controller.go` skeleton.
  - Controller registration of the **event-driven Watch** on `ControlPlaneComponent` (CPC) resources in namespace `<hc_namespace>-<cluster_name>`, mapping updates back to the parent `HostedClusterUpgrade` CR in `<hc_namespace>` sharing the cluster's name.
  - Status update logic copying CPC properties (Active Version, Availability, and RolloutComplete conditions) into `.status.controlPlaneStatus.components` inside the custom resource status.
* **Verification Strategy:**
  - **Integration Tests (`envtest`):**
    - Create a mock `HostedCluster` and mock `ControlPlaneComponents` (e.g. `kube-apiserver`, `etcd`).
    - Apply a `HostedClusterUpgrade` CR.
    - Manually mutate a CPC's status (e.g. change `RolloutComplete` to `True`).
    - Verify that the coordinator's reconciler triggers immediately (via the Watch event), and the parent CR status is updated with the exact component states.

---

### **Milestone 4: State Machine - Preflight & CP Upgrades**
* **Goal:** Implement the preflight and control plane upgrade sequence.
* **Deliverables:**
  - Reconcile loop core state-machine (PreflightChecks ➔ CP Scheduling ➔ CP Rollout ➔ CP Verification).
  - Implementation of **Pre-Step Health Re-verification check** (triggering full preflight checks immediately before unpausing objects or changing specs).
  - Implementation of the EUS intermediate release-image hop logic vs standard direct upgrades.
* **Verification Strategy:**
  - **Unit/Integration Tests:**
    - Simulate a standard upgrade: Ensure the `TargetReleaseImage` is applied directly to the `HostedCluster` spec without any intermediate jump.
    - Simulate an EUS upgrade: Ensure `IntermediateReleaseImages[0]` is applied first, verified, and only then is `TargetReleaseImage` applied.
    - Simulate scheduled pauses: Ensure that if `controlPlane.startAt` is in the future, the state machine logs the countdown message and pauses. Simulate time passing and ensure it automatically unpauses and executes.
    - Test health regression: Degrade the mock `HostedCluster` (apply a failing condition) while the upgrade is paused on a schedule. Verify that when the schedule opens, the pre-step health gate halts the upgrade, transitions the phase to `Blocked`, and rejects the rollout.

---

### **Milestone 5: Priority NodePools & HCCO Log Scraping**
* **Goal:** Implement worker NodePool serialization, concurrency limits, and log-scraping draining monitors.
* **Deliverables:**
  - Label-based NodePool sequence priority grouping (`upgrade.huc.0xshift.dev/order`).
  - Handling of omitted/orphan NodePools (automatically unpausing and upgrading them under priority `50` with `nil` schedules).
  - Implementation of `spec.rolloutPolicy.maxParallelNodePools` rollout limits.
  - HCCO container log-scraping worker executing inside the controller pod to trace blockages.
* **Verification Strategy:**
  - **Integration Tests (`envtest`):**
    - Create 4 mock NodePools with orders: `10` (scheduled), `50` (scheduled), `50` (omitted from Spec), and `100` (scheduled).
    - Run the reconciler and verify they upgrade in the exact sequential priority groups.
    - Verify the omitted NodePool is resolved, upgraded, and successfully unpaused.
    - Verify concurrency: Ensure no more than `maxParallelNodePools` are upgraded at once within group `50`.
  - **Log-Scraping Tests:** Mock the pod log stream API of Kubernetes to return mock HCCO logs (e.g. `"cannot evict pod my-pod due to PDB my-pdb"`). Verify the reconciler parses these logs, populates `.status.nodePoolStatuses[].blockers`, and raises the metrics.

---

### **Milestone 6: Kustomize, NetPols, SRE Metrics & Grafana**
* **Goal:** Package all deployment assets, instrument metrics, set up network security policies, and validate Grafana dashboards.
* **Deliverables:**
  - **Kustomize Structure:** Production-ready `config/` directory scaffolding out deployment overlays and CRDs, configured to run inside namespace `hypershift-upgrade-coordinator` using the image `quay.io/duritong/huc:latest`.
  - **Alerts Packaging:** Integrate the `PrometheusRule` Custom Resource manifest directly into the Kustomize deployment pipeline.
  - **Network Policies:** Write and package `NetworkPolicy` manifests inside the Kustomize overlays, permitting Prometheus metrics ingress from the `openshift-monitoring` namespace to our designated metrics port.
  - **Grafana Dashboard JSON:** Packaged dashboard ready for import on Grafana instances.
  - **Grafana Syntax Validation Unit Test:** A dedicated Go unit test that dynamically parses and unmarshals the Grafana dashboard JSON files during `make test` to verify syntax and JSON-schema structural correctness.
  - **Instrumentation:** Finalize metrics registration and run a comprehensive E2E integration test simulating the entire workflow.
* **Verification Strategy:**
  - Execute a full EUS upgrade workflow inside `envtest`.
  - Assert that metrics are registered, and check that metric values (phase, duration, step-level duration) update correctly as state transitions occur.
  - Execute `make test` and verify the **Grafana Dashboard JSON validation test** passes.
  - Verify that the generated YAML manifests (CRDs, Roles, Webhooks, NetworkPolicies, PrometheusRules) compile cleanly with no linting errors under Kustomize.
  - Dry-run Kustomize output: `kustomize build config/default` to verify perfect compilation of all manifests.

---

## 📈 SRE Coordination Protocol

As the Project Coordinator, on every milestone:
1. I will formulate a specific plan of action.
2. I will invoke the implementation and validation agents.
3. I will review the unit/integration test results.
4. I will compile a **Milestone Success Report** detailing the code changes and test outputs, presenting them to you before we move to the next milestone.
5. All design and runtime questions will be resolved by referencing `docs/architecture.md`. If any fundamental ambiguity arises, I will stop and consult you immediately.
