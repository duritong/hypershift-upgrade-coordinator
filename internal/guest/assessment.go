package guest

import (
	"sort"
	"time"

	upgradev1 "github.com/duritong/hypershift-upgrade-coordinator/api/v1alpha1"
	configv1 "github.com/openshift/api/config/v1"
	"k8s.io/apimachinery/pkg/util/validation"
)

// Assess is pure. current must be freshly reconstructed at the action boundary,
// not copied from the snapshot. Management proofs remain independently required.
func Assess(s Snapshot, current Target, policy Policy, now time.Time) Result {
	fail := func(state State, reason string) Result { return Result{State: state, Reason: reason} }
	if !current.valid() || s.Target != current {
		return fail(IdentityMismatch, "TargetChanged")
	}
	if !now.Before(current.CredentialExpiresAt) {
		return fail(Unavailable, "CredentialExpired")
	}
	if !s.complete || s.StartedAt.IsZero() || s.CompletedAt.Before(s.StartedAt) || s.CompletedAt.After(now) || s.CompletedAt.Sub(s.StartedAt) > CollectionTimeout || now.Sub(s.CompletedAt) > DecisionAge {
		return fail(Unavailable, "StaleOrIncompleteSnapshot")
	}
	if s.ListRevision == "" || len(s.Operators) == 0 || len(s.Operators) > MaxOperators {
		return fail(UnsupportedEvidence, "InvalidOperatorList")
	}
	preRequest := policy.Purpose == SourceBaseline || policy.Purpose == BeforeControlPlaneRequest
	worker := policy.Purpose == WorkerEntry || policy.Purpose == WorkerGuard
	observing := policy.Purpose == ObserveControlPlane
	switch policy.Purpose {
	case SourceBaseline, BeforeControlPlaneRequest, ObserveControlPlane, IntermediateVerification, WorkerEntry, WorkerGuard, FinalUpgradeVerification:
	default:
		return fail(UnsupportedEvidence, "InvalidPurpose")
	}
	minor := false
	if preRequest {
		if !validRelease(policy.NextRelease) || !upgradev1.IsUpstreamGreater(current.Expected.Version, policy.NextRelease.Version) {
			return fail(UnsupportedEvidence, "InvalidNextRelease")
		}
		major, m, _ := upgradev1.GetMajorMinor(current.Expected.Version)
		nextMajor, n, _ := upgradev1.GetMajorMinor(policy.NextRelease.Version)
		if major != nextMajor || n-m > 1 {
			return fail(UnsupportedEvidence, "InvalidNextRelease")
		}
		minor = n != m
	} else if policy.NextRelease != (Release{}) {
		return fail(UnsupportedEvidence, "UnexpectedNextRelease")
	}

	r := Result{State: Ready, Reason: "GuestReady"}
	add := func(operator string, state State, reason string) {
		r.BlockerCount++
		if len(r.Blockers) < 32 {
			r.Blockers = append(r.Blockers, Blocker{Operator: operator, State: state, Reason: reason})
		} else {
			r.Truncated = true
		}
		if severity(state) > severity(r.State) {
			r.State, r.Reason = state, reason
		}
	}
	cv := &s.Version
	if cv.Name != "version" || cv.UID != current.CVUID || cv.Spec.ClusterID != current.ClusterID {
		return fail(IdentityMismatch, "ClusterVersionIdentity")
	}
	if cv.Kind != "ClusterVersion" || cv.APIVersion != "config.openshift.io/v1" || cv.ResourceVersion == "" || cv.Generation <= 0 || cv.Namespace != "" {
		return fail(UnsupportedEvidence, "MalformedClusterVersion")
	}
	if cv.Status.ObservedGeneration != cv.Generation {
		add("", Unavailable, "StaleClusterVersionGeneration")
	}
	cc, ok := conditions(cv.Status.Conditions)
	if !ok {
		add("", UnsupportedEvidence, "MalformedCVConditions")
	}
	check := func(name string, want configv1.ConditionStatus, optional bool, convergence bool) {
		value, exists := cc[name]
		if !exists && optional {
			return
		}
		if !exists || value == configv1.ConditionUnknown {
			add("", UnsupportedEvidence, "CV"+name+"Unknown")
			return
		}
		if value != want {
			state := Unhealthy
			if convergence {
				state = Converging
			}
			add("", state, "CV"+name)
		}
	}
	check("Available", configv1.ConditionTrue, false, observing)
	check("Failing", configv1.ConditionFalse, false, false)
	check("Degraded", configv1.ConditionFalse, true, false)
	check("ReleaseAccepted", configv1.ConditionTrue, true, false)
	if worker || observing {
		if cc["Progressing"] != configv1.ConditionTrue && cc["Progressing"] != configv1.ConditionFalse {
			add("", UnsupportedEvidence, "CVProgressingUnknown")
		}
	} else {
		check("Progressing", configv1.ConditionFalse, false, true)
	}
	if preRequest {
		check("Upgradeable", configv1.ConditionTrue, true, false)
	}
	if u := cv.Spec.DesiredUpdate; u != nil {
		if (u.Image == "" && u.Version == "") || (u.Image != "" && !samePayload(u.Image, current.Expected.Image)) || (u.Version != "" && (!fullVersion(u.Version) || u.Version != current.Expected.Version)) {
			add("", IdentityMismatch, "ConflictingCVRequest")
		}
	}
	if !validRelease(Release{cv.Status.Desired.Version, cv.Status.Desired.Image}) {
		add("", UnsupportedEvidence, "MalformedCVDesiredRelease")
	} else if !matches(cv.Status.Desired.Version, cv.Status.Desired.Image, current.Expected) {
		add("", Converging, "CVDesiredReleaseMismatch")
	}
	if len(cv.Status.History) == 0 {
		add("", UnsupportedEvidence, "MissingCVHistory")
	} else {
		h := cv.Status.History[0]
		if !validRelease(Release{h.Version, h.Image}) {
			add("", UnsupportedEvidence, "MalformedCVHistoryRelease")
		}
		if h.State != configv1.CompletedUpdate && h.State != configv1.PartialUpdate {
			add("", UnsupportedEvidence, "MalformedCVHistory")
		}
		if h.State != configv1.CompletedUpdate || !matches(h.Version, h.Image, current.Expected) {
			add("", Converging, "CVLatestNotCompleted")
		}
	}

	// Sort a private slice, not the caller's snapshot. No historical membership is
	// consulted: even a formerly unhealthy removed CO disappears immediately.
	ops := append([]configv1.ClusterOperator(nil), s.Operators...)
	sort.Slice(ops, func(i, j int) bool { return ops[i].Name < ops[j].Name })
	seenNames, seenUIDs := map[string]bool{}, map[string]bool{}
	for _, co := range ops {
		if len(validation.IsDNS1123Subdomain(co.Name)) != 0 || co.UID == "" || co.ResourceVersion == "" || co.Namespace != "" || seenNames[co.Name] || seenUIDs[string(co.UID)] {
			add("", UnsupportedEvidence, "MalformedOperatorIdentity")
			continue
		}
		seenNames[co.Name], seenUIDs[string(co.UID)] = true, true
		cs, valid := conditions(co.Status.Conditions)
		if !valid {
			add(co.Name, UnsupportedEvidence, "MalformedCOConditions")
		}
		for _, condition := range []struct {
			name string
			want configv1.ConditionStatus
		}{{"Available", configv1.ConditionTrue}, {"Degraded", configv1.ConditionFalse}} {
			value, exists := cs[condition.name]
			if !exists || value == configv1.ConditionUnknown {
				add(co.Name, UnsupportedEvidence, "CO"+condition.name+"Unknown")
			} else if value != condition.want {
				add(co.Name, Unhealthy, "CO"+condition.name)
			}
		}
		progressing := cs["Progressing"]
		if progressing != configv1.ConditionTrue && progressing != configv1.ConditionFalse {
			add(co.Name, UnsupportedEvidence, "COProgressingUnknown")
		} else if progressing == configv1.ConditionTrue && !worker && !observing {
			add(co.Name, Converging, "COProgressing")
		}
		if preRequest && minor && cs["Upgradeable"] == configv1.ConditionFalse {
			add(co.Name, Unhealthy, "COUpgradeNotAllowed")
		}
		version := ""
		seenVersions := map[string]bool{}
		for _, v := range co.Status.Versions {
			if v.Name == "" || seenVersions[v.Name] {
				add(co.Name, UnsupportedEvidence, "MalformedCOVersions")
			}
			seenVersions[v.Name] = true
			if v.Name == "operator" {
				version = v.Version
			}
		}
		if !fullVersion(version) {
			add(co.Name, UnsupportedEvidence, "MissingOrMalformedOperatorVersion")
			version = ""
		} else if version != current.Expected.Version {
			add(co.Name, Converging, "OperatorVersionMismatch")
		}
		r.Operators = append(r.Operators, OperatorSummary{Name: co.Name, Version: version})
	}

	r.Virtualization = assessVirtualization(s.Virtualization, current.Expected, policy, add)

	if observing && r.State == Ready {
		r.State, r.Reason = Converging, "ObservationOnly"
	}
	return r
}

func conditions(in []configv1.ClusterOperatorStatusCondition) (map[string]configv1.ConditionStatus, bool) {
	out := make(map[string]configv1.ConditionStatus, len(in))
	valid := true
	for _, c := range in {
		if _, exists := out[string(c.Type)]; exists || c.Type == "" || (c.Status != configv1.ConditionTrue && c.Status != configv1.ConditionFalse && c.Status != configv1.ConditionUnknown) {
			valid = false
		}
		out[string(c.Type)] = c.Status
	}
	return out, valid
}

func severity(s State) int {
	switch s {
	case IdentityMismatch:
		return 6
	case Unhealthy:
		return 5
	case UnsupportedEvidence:
		return 4
	case AccessDenied:
		return 3
	case Unavailable:
		return 2
	case Converging:
		return 1
	default:
		return 0
	}
}
