# Worker Assessment

Worker completion requires positive evidence from the management-cluster
NodePool and its CAPI MachineDeployment (Replace) or MachineSet (InPlace).
The CAPI resource has the raw NodePool name, in the namespace returned by
`WorkerControlPlaneNamespace`: `<namespace>-<HostedCluster name with dots replaced by hyphens>`.
The resource's `hypershift.openshift.io/nodePool` annotation must identify the
NodePool by namespace/name.

NodePool `status.replicas` is upstream's available replica count, not desired
population. CAPI desired replicas and fresh status establish population, including
intentional autoscaling to zero. Replace completion requires exact replica,
updated, ready and available counts, observed generation, and any native
`status.v1beta2` ready/available/up-to-date counts. NodePool Ready=True,
UpdatingVersion=False and UpdatingConfig=False must observe its current
generation. Any positive Updating condition blocks completion. The expected
release image, status version, and completed config hash/bootstrap secret must
also agree. Old healthy nodes cannot offset unready target nodes.

For InPlace, HyperShift's last-Machine release annotation can remain stale because
HCCO returns from its completion branch before updating that annotation. Instead,
the coordinator requires nonempty matching MachineSet target/current
config-version hashes and the same completed NodePool hash/bootstrap secret,
fresh NodePool version/conditions, current CAPI generation and healthy exact
population. HCCO writes the MachineSet current hash only after all guest nodes
complete the requested MCD configuration. Only with that combined proof are all
healthy nodes credited to the target. Missing hashes or unhealthy population
block completion; there is no status.version-only fallback. Manual None and
IBMCloud UPI pools are unsupported and block automated worker mutations.

## Control-Plane Integration

Call `HostedClusterUpgradeReconciler.WorkerPrerequisite(ctx, hcu)` before acquiring
initial worker locks and immediately before every new control-plane mutation,
including changes to an active plan. It returns an error unless all associated
pools are positively quiescent at their own specified release. The caller must
surface that error as a waiting/blocking prerequisite, not advance the CP.
Lock acquisition alone is not evidence that existing CAPI/HCCO work stopped.

Locks acquired with positive quiescence record the NodePool generation. Subsequent
checks discount only the coordinator's pause-only generation change; any further
spec change invalidates this proof. Older locks without this proof fail closed
until fresh upstream evidence establishes quiescence. UID ownership, original
user pauses, and finalizer restoration remain authoritative.

During worker reconciliation, newly enrolled quiescent pools are locked before
any pool is released in group order. External or unproven rollouts block new work
regardless of group and reserve concurrency even if independently paused.
Previously started coordinator work retains its saved target and step timestamp
until it has authoritative completion evidence.

The manager integration must cache/watch CAPI `cluster.x-k8s.io/v1beta1`
MachineDeployments and MachineSets and route their changes back to the upgrade.
Read-only fleet RBAC and minimal envtest CRDs are included. A deployment must
serve that API version; converted native v1beta2 status is cross-checked when
present. External writers must coordinate changes operationally: informer-backed
observations and optimistic writes cannot prevent a racing external spec write.
