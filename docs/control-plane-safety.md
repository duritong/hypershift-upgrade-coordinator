# Control Plane Safety Policy

The coordinator accepts an operator-supplied, trusted release plan. Numeric
same-major, one-minor-hop validation and verified release metadata are necessary
but **do not establish a supported CVO update-graph edge or recommendation**.
Operators must validate the complete plan against the applicable OpenShift update
graph, conditional recommendations, platform support, and HyperShift support
matrix before submitting it. The coordinator does not query or invent graph
approvals. There is no override for failed local safety checks.

Before initial worker locks and each new CP release request, actual NodePool
`status.version` and every `status.nodesInfo.nodeVersions[].ocpVersion` must be
known and satisfy same-major n-3 skew against the current CP and every remaining
planned CP hop. Workers newer than the current CP, including a newer patch, are
rejected. Missing or empty bucket evidence blocks, including scaled-to-zero pools;
this conservative restriction can require restoring observable worker evidence
before starting a CP upgrade. This follows the same-major portion of upstream
`support/supportedversion.ValidateVersionSkew`; cross-major plans remain unsupported.

Every new CP release, including a patch, requires explicit
`ClusterVersionUpgradeable=True`. This condition is not required to observe an
already requested rollout, verify its completion, or finish worker upgrades.
Health still requires `Available`, `ClusterVersionAvailable`, and
`ClusterVersionSucceeding` to be true and no explicit failing/degraded condition.

Guest and management payloads must remain coordinated: an independent HC
`spec.controlPlaneRelease` override is rejected by admission and reconciliation,
never overwritten. An existing request matching a planned hop can be adopted;
conflicting requests are not replaced. Same-version, different-digest lateral
upgrades are not supported. Payload equality compares immutable digests, allowing
canonical digest-only status references and registry mirrors.

Completion requires the latest guest and management history entries to be
`Completed` at the exact expected version and digest. It also requires the actual
HCP `spec.releaseImage`, management `observedGeneration`, and all observed CPCs
to agree with that payload and HCP generation. CPC freshness is relative to HCP
generation, not CPC or HC generation. Missing HCP or CPC evidence blocks progress.
An HCP `spec.controlPlaneReleaseImage` differing from the expected payload also
blocks, even if the HC override has already been removed.
The guest version status generation is not treated as an HC generation fence.

These prerequisites are checked again before subsequent hops and new worker
actions, including after schedule waits. Degradation prevents new work; it does
not pause active worker rollouts. Existing worker rollouts, even paused ones,
must be positively quiescent before a new CP request. Only proven coordinator
pause-only generation changes are discounted when evaluating quiescence.

Observations use the controller client and optimistic resource updates. They are
not a cross-resource transaction and cannot prevent a concurrent external writer
from starting work after a prerequisite check. Operational field ownership and
coordination are still required. HCP access is read-only fleet-wide. Tests simulate
HyperShift evidence with explicit fixtures; envtest is not a deployed HyperShift
end-to-end validation.
