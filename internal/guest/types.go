// Package guest collects scoped, bounded OpenShift evidence and evaluates it
// without management-cluster I/O. A Ready result is additional evidence, never a
// replacement for management-side checks or an authorization to mutate by itself.
package guest

import (
	"time"

	configv1 "github.com/openshift/api/config/v1"
	"k8s.io/apimachinery/pkg/types"
)

const (
	RequestTimeout    = 5 * time.Second
	CollectionTimeout = 10 * time.Second
	DecisionAge       = 30 * time.Second
	MaxOperators      = 256
	MaxBytes          = 4 << 20
)

// Release carries independently verified payload metadata. Image must be digest
// pinned; a tag is not proof of either version or content.
type Release struct{ Version, Image string }

// Target is a comparable identity fence. Rebuild it from current management and
// access evidence before EACH action; rotation and plan edits invalidate equality.
// CVUID is learned during access identity verification, not inferred from a name.
type Target struct {
	Namespace, Name                                            string
	HCUUID                                                     types.UID
	Generation                                                 int64
	HCUID                                                      types.UID
	HCGeneration                                               int64
	HCPUID                                                     types.UID
	HCPGeneration                                              int64
	ClusterID                                                  configv1.ClusterID
	CVUID                                                      types.UID
	HCRelease, PlanRevision, EndpointRevision, ProfileRevision string
	SecretUID                                                  types.UID
	SecretRevision                                             string
	CredentialExpiresAt                                        time.Time
	Expected                                                   Release
}

type Purpose string

const (
	SourceBaseline            Purpose = "SourceBaseline"
	BeforeControlPlaneRequest Purpose = "BeforeControlPlaneRequest"
	ObserveControlPlane       Purpose = "ObserveControlPlane"
	IntermediateVerification  Purpose = "IntermediateVerification"
	WorkerEntry               Purpose = "WorkerEntry"
	WorkerGuard               Purpose = "WorkerGuard"
	FinalUpgradeVerification  Purpose = "FinalUpgradeVerification"
)

// Policy describes the next request only at source/pre-request boundaries.
// NextRelease is mandatory there, so minor eligibility cannot be accidentally
// omitted. Elsewhere it must be zero. There is deliberately no bypass policy.
type Policy struct {
	Purpose     Purpose
	NextRelease Release
}

type State string

const (
	Ready               State = "Ready"
	Converging          State = "Converging"
	Unhealthy           State = "Unhealthy"
	Unavailable         State = "Unavailable"
	AccessDenied        State = "AccessDenied"
	IdentityMismatch    State = "IdentityMismatch"
	UnsupportedEvidence State = "UnsupportedEvidence"
)

// Failure contains only fixed, safe reason codes, never underlying HTTP errors.
type Failure struct {
	State  State
	Reason string
}

func (f *Failure) Error() string { return string(f.State) + ": " + f.Reason }

// Snapshot is an owned value. Treat it as immutable after collection; it contains
// only a complete current list, not a retained membership registry.
type Snapshot struct {
	Target                 Target
	StartedAt, CompletedAt time.Time
	Version                configv1.ClusterVersion
	Operators              []configv1.ClusterOperator
	ListRevision           string
	complete               bool
}

type Blocker struct {
	Operator, Reason string
	State            State
}
type OperatorSummary struct{ Name, Version string }
type Result struct {
	State        State
	Reason       string
	Operators    []OperatorSummary
	Blockers     []Blocker
	BlockerCount int
	Truncated    bool
}

func (t Target) valid() bool {
	return t.Namespace != "" && t.Name != "" && t.HCUUID != "" && t.Generation > 0 &&
		t.HCUID != "" && t.HCGeneration > 0 && t.HCPUID != "" && t.HCPGeneration > 0 && t.ClusterID != "" && t.CVUID != "" && t.HCRelease != "" &&
		t.PlanRevision != "" && t.EndpointRevision != "" && t.ProfileRevision != "" &&
		t.SecretUID != "" && t.SecretRevision != "" && !t.CredentialExpiresAt.IsZero() && validRelease(t.Expected)
}
