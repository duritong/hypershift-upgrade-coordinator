package guest

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	configv1 "github.com/openshift/api/config/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

var testNow = time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)

func release(version string) Release {
	return Release{version, "quay.io/ocp/release@sha256:" + strings.Repeat("a", 64)}
}
func target() Target {
	return Target{Namespace: "clusters", Name: "test", HCUUID: "hcu", Generation: 1, HCUID: "hc", HCGeneration: 1, HCPUID: "hcp", HCPGeneration: 1, ClusterID: "cluster-id", CVUID: "cv", HCRelease: release("4.20.1").Image, PlanRevision: "plan-1", EndpointRevision: "endpoint-1", ProfileRevision: "profile-1", SecretUID: "secret", SecretRevision: "1", CredentialExpiresAt: testNow.Add(time.Hour), Expected: release("4.20.1")}
}
func condition(name string, value configv1.ConditionStatus) configv1.ClusterOperatorStatusCondition {
	return configv1.ClusterOperatorStatusCondition{Type: configv1.ClusterStatusConditionType(name), Status: value, LastTransitionTime: metav1.NewTime(testNow.Add(-365 * 24 * time.Hour))}
}
func healthySnapshot() Snapshot {
	t := target()
	cv := configv1.ClusterVersion{TypeMeta: metav1.TypeMeta{Kind: "ClusterVersion", APIVersion: "config.openshift.io/v1"}, ObjectMeta: metav1.ObjectMeta{Name: "version", UID: t.CVUID, ResourceVersion: "1", Generation: 2}, Spec: configv1.ClusterVersionSpec{ClusterID: t.ClusterID}, Status: configv1.ClusterVersionStatus{ObservedGeneration: 2, Desired: configv1.Release{Version: t.Expected.Version, Image: t.Expected.Image}, History: []configv1.UpdateHistory{{State: configv1.CompletedUpdate, Version: t.Expected.Version, Image: t.Expected.Image}}, Conditions: []configv1.ClusterOperatorStatusCondition{condition("Available", configv1.ConditionTrue), condition("Failing", configv1.ConditionFalse), condition("Progressing", configv1.ConditionFalse)}}}
	co := configv1.ClusterOperator{ObjectMeta: metav1.ObjectMeta{Name: "monitoring", UID: "monitoring-uid", ResourceVersion: "20"}, Status: configv1.ClusterOperatorStatus{Conditions: []configv1.ClusterOperatorStatusCondition{condition("Available", configv1.ConditionTrue), condition("Degraded", configv1.ConditionFalse), condition("Progressing", configv1.ConditionFalse)}, Versions: []configv1.OperandVersion{{Name: "operator", Version: t.Expected.Version}, {Name: "operand", Version: "unrelated-version"}}}}
	return Snapshot{Target: t, StartedAt: testNow.Add(-time.Second), CompletedAt: testNow, Version: cv, Operators: []configv1.ClusterOperator{co}, ListRevision: "20", complete: true}
}
func setCondition(cs *[]configv1.ClusterOperatorStatusCondition, name, value string) {
	for i := range *cs {
		if string((*cs)[i].Type) == name {
			if value == "missing" {
				*cs = append((*cs)[:i], (*cs)[i+1:]...)
				return
			}
			(*cs)[i].Status = configv1.ConditionStatus(value)
			return
		}
	}
	if value != "missing" {
		*cs = append(*cs, condition(name, configv1.ConditionStatus(value)))
	}
}
func policyFor(p Purpose) Policy {
	if p == SourceBaseline || p == BeforeControlPlaneRequest {
		return Policy{Purpose: p, NextRelease: release("4.21.0")}
	}
	return Policy{Purpose: p}
}

func TestConditionMatrix(t *testing.T) {
	purposes := []Purpose{SourceBaseline, BeforeControlPlaneRequest, ObserveControlPlane, IntermediateVerification, WorkerEntry, WorkerGuard, FinalUpgradeVerification}
	for _, p := range purposes {
		for _, object := range []string{"CV", "CO"} {
			names := []string{"Available", "Progressing", "Degraded", "Upgradeable"}
			if object == "CV" {
				names = append(names, "Failing", "ReleaseAccepted")
			}
			for _, name := range names {
				for _, value := range []string{"True", "False", "Unknown", "missing", "invalid", "duplicate"} {
					t.Run(fmt.Sprintf("%s/%s/%s/%s", p, object, name, value), func(t *testing.T) {
						s := healthySnapshot()
						cs := &s.Version.Status.Conditions
						if object == "CO" {
							cs = &s.Operators[0].Status.Conditions
						}
						if value == "duplicate" {
							setCondition(cs, name, "True")
							*cs = append(*cs, condition(name, configv1.ConditionTrue))
						} else {
							setCondition(cs, name, value)
						}
						allowed := true
						switch name {
						case "Available":
							allowed = value == "True"
						case "Failing":
							allowed = value == "False"
						case "Degraded":
							allowed = value == "False" || (object == "CV" && value == "missing")
						case "ReleaseAccepted":
							allowed = value == "True" || value == "missing"
						case "Progressing":
							allowed = value == "False" || (value == "True" && (p == WorkerEntry || p == WorkerGuard))
						case "Upgradeable":
							allowed = value != "invalid" && value != "duplicate"
							if p == SourceBaseline || p == BeforeControlPlaneRequest {
								if object == "CV" {
									allowed = value == "True" || value == "missing"
								} else {
									allowed = value == "True" || value == "Unknown" || value == "missing"
								}
							}
						}
						if p == ObserveControlPlane {
							allowed = false
						}
						r := Assess(s, s.Target, policyFor(p), testNow)
						if (r.State == Ready) != allowed {
							t.Fatalf("Ready=%t, expected %t: %+v", r.State == Ready, allowed, r)
						}
						if object == "CV" && ((name == "Failing" && value == "True") || (name == "Degraded" && value == "True") || (name == "ReleaseAccepted" && value == "False")) && r.State != Unhealthy {
							t.Fatalf("direct CV failure hidden: %+v", r)
						}
					})
				}
			}
		}
	}
}

func TestEvidenceFailures(t *testing.T) {
	cases := []struct {
		name   string
		change func(*Snapshot)
		state  State
	}{
		{"stale-generation", func(s *Snapshot) { s.Version.Status.ObservedGeneration-- }, Unavailable},
		{"zero-generation", func(s *Snapshot) { s.Version.Generation = 0; s.Version.Status.ObservedGeneration = 0 }, UnsupportedEvidence},
		{"wrong-cv-uid", func(s *Snapshot) { s.Version.UID = "replacement" }, IdentityMismatch},
		{"wrong-cluster-id", func(s *Snapshot) { s.Version.Spec.ClusterID = "replacement" }, IdentityMismatch},
		{"wrong-name", func(s *Snapshot) { s.Version.Name = "other" }, IdentityMismatch},
		{"desired-wrong-digest", func(s *Snapshot) { s.Version.Status.Desired.Image = "mirror/release@sha256:" + strings.Repeat("b", 64) }, Converging},
		{"history-wrong-digest", func(s *Snapshot) {
			s.Version.Status.History[0].Image = "mirror/release@sha256:" + strings.Repeat("b", 64)
		}, Converging},
		{"partial-latest-not-old-completed", func(s *Snapshot) {
			s.Version.Status.History = append([]configv1.UpdateHistory{{State: configv1.PartialUpdate, Version: s.Target.Expected.Version, Image: s.Target.Expected.Image}}, s.Version.Status.History...)
		}, Converging},
		{"history-missing", func(s *Snapshot) { s.Version.Status.History = nil }, UnsupportedEvidence},
		{"conflicting-request", func(s *Snapshot) { s.Version.Spec.DesiredUpdate = &configv1.Update{Version: "4.21.0"} }, IdentityMismatch},
		{"empty-explicit-request", func(s *Snapshot) { s.Version.Spec.DesiredUpdate = &configv1.Update{} }, IdentityMismatch},
		{"empty-list", func(s *Snapshot) { s.Operators = nil }, UnsupportedEvidence},
		{"partial", func(s *Snapshot) { s.complete = false }, Unavailable},
		{"stale", func(s *Snapshot) { s.CompletedAt = testNow.Add(-31 * time.Second); s.StartedAt = s.CompletedAt }, Unavailable},
		{"future", func(s *Snapshot) { s.CompletedAt = testNow.Add(time.Second) }, Unavailable},
		{"long-collection", func(s *Snapshot) { s.StartedAt = testNow.Add(-11 * time.Second) }, Unavailable},
		{"missing-version", func(s *Snapshot) { s.Operators[0].Status.Versions = nil }, UnsupportedEvidence},
		{"duplicate-version", func(s *Snapshot) {
			s.Operators[0].Status.Versions = append(s.Operators[0].Status.Versions, s.Operators[0].Status.Versions[0])
		}, UnsupportedEvidence},
		{"duplicate-operator", func(s *Snapshot) { s.Operators = append(s.Operators, s.Operators[0]) }, UnsupportedEvidence},
		{"malformed-name", func(s *Snapshot) { s.Operators[0].Name = "token=secret" }, UnsupportedEvidence},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := healthySnapshot()
			tc.change(&s)
			r := Assess(s, s.Target, Policy{Purpose: WorkerEntry}, testNow)
			if r.State != tc.state {
				t.Fatalf("want %s: %+v", tc.state, r)
			}
		})
	}
}

func TestFullOperatorVersions(t *testing.T) {
	for _, version := range []string{"4.19.1", "4.20.1-rc.1", "4.20.1+build.1", "4.20", "v4.20.1", "", "4.20.1"} {
		t.Run(version, func(t *testing.T) {
			s := healthySnapshot()
			s.Operators[0].Status.Versions[0].Version = version
			setCondition(&s.Operators[0].Status.Conditions, "Progressing", "True")
			r := Assess(s, s.Target, Policy{Purpose: WorkerGuard}, testNow)
			if (r.State == Ready) != (version == "4.20.1") {
				t.Fatalf("unexpected version decision: %+v", r)
			}
		})
	}
	s := healthySnapshot()
	s.Target.Expected = release("4.20.1-rc.1")
	s.Version.Status.Desired.Version = s.Target.Expected.Version
	s.Version.Status.History[0].Version = s.Target.Expected.Version
	s.Operators[0].Status.Versions[0].Version = s.Target.Expected.Version
	if r := Assess(s, s.Target, Policy{Purpose: WorkerEntry}, testNow); r.State != Ready {
		t.Fatalf("matching full prerelease rejected: %+v", r)
	}
}

func TestTargetFences(t *testing.T) {
	for _, change := range []func(*Target){func(t *Target) { t.HCGeneration++ }, func(t *Target) { t.HCPUID = "replaced" }, func(t *Target) { t.HCPGeneration++ }} {
		s := healthySnapshot()
		current := s.Target
		change(&current)
		if r := Assess(s, current, Policy{Purpose: WorkerGuard}, testNow); r.State != IdentityMismatch {
			t.Fatalf("management revision did not invalidate snapshot: %+v", r)
		}
	}
	changes := []func(*Target){func(t *Target) { t.HCUUID = "new" }, func(t *Target) { t.Generation++ }, func(t *Target) { t.HCUID = "new" }, func(t *Target) { t.CVUID = "new" }, func(t *Target) { t.ClusterID = "new" }, func(t *Target) { t.HCRelease = "new" }, func(t *Target) { t.PlanRevision = "new" }, func(t *Target) { t.EndpointRevision = "new" }, func(t *Target) { t.ProfileRevision = "new" }, func(t *Target) { t.SecretUID = "new" }, func(t *Target) { t.SecretRevision = "new" }, func(t *Target) { t.Expected = release("4.21.0") }, func(t *Target) { t.CredentialExpiresAt = testNow }}
	for i, change := range changes {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			s := healthySnapshot()
			current := s.Target
			change(&current)
			if r := Assess(s, current, Policy{Purpose: WorkerGuard}, testNow); r.State != IdentityMismatch {
				t.Fatalf("invalidated target passed: %+v", r)
			}
		})
	}
	s := healthySnapshot()
	s.Target.CredentialExpiresAt = testNow
	if r := Assess(s, s.Target, Policy{Purpose: WorkerGuard}, testNow); r.State != Unavailable {
		t.Fatalf("expired credential passed: %+v", r)
	}
}

func TestManagementTargetRequiresExplicitRevisions(t *testing.T) {
	for _, change := range []func(*Target){func(t *Target) { t.HCGeneration = 0 }, func(t *Target) { t.HCPUID = "" }, func(t *Target) { t.HCPGeneration = 0 }} {
		target := target()
		change(&target)
		if target.valid() {
			t.Fatalf("incomplete management target accepted: %+v", target)
		}
	}
}

func TestDynamicMembershipAndBounds(t *testing.T) {
	s := healthySnapshot()
	bad := *s.Operators[0].DeepCopy()
	bad.Name = "new-operator"
	bad.UID = "new-uid"
	setCondition(&bad.Status.Conditions, "Degraded", "True")
	s.Operators = append(s.Operators, bad)
	if r := Assess(s, s.Target, Policy{Purpose: WorkerEntry}, testNow); r.State != Unhealthy {
		t.Fatalf("addition ignored: %+v", r)
	}
	s.Operators = s.Operators[:1]
	if r := Assess(s, s.Target, Policy{Purpose: WorkerEntry}, testNow); r.State != Ready || len(r.Operators) != 1 {
		t.Fatalf("removed unhealthy operator retained: %+v", r)
	}
	s.Operators = []configv1.ClusterOperator{bad}
	setCondition(&s.Operators[0].Status.Conditions, "Degraded", "False")
	if r := Assess(s, s.Target, Policy{Purpose: WorkerEntry}, testNow); r.State != Ready || r.Operators[0].Name != "new-operator" {
		t.Fatalf("removed healthy operator retained: %+v", r)
	}
	s.Operators = nil
	for i := 0; i < MaxOperators; i++ {
		co := *bad.DeepCopy()
		co.Name = fmt.Sprintf("operator-%03d", MaxOperators-i)
		co.UID = types.UID(co.Name)
		setCondition(&co.Status.Conditions, "Degraded", "True")
		s.Operators = append(s.Operators, co)
	}
	r := Assess(s, s.Target, Policy{Purpose: WorkerGuard}, testNow)
	if r.State != Unhealthy || r.BlockerCount != MaxOperators || len(r.Blockers) != 32 || !r.Truncated || len(r.Operators) != MaxOperators || r.Operators[0].Name != "operator-001" {
		t.Fatalf("bad bounded result: %+v", r)
	}
}

func TestMinorEligibilityAndPayloadMirrors(t *testing.T) {
	s := healthySnapshot()
	setCondition(&s.Operators[0].Status.Conditions, "Upgradeable", "False")
	if r := Assess(s, s.Target, Policy{Purpose: SourceBaseline, NextRelease: release("4.20.2")}, testNow); r.State != Ready {
		t.Fatalf("CO minor veto applied to patch: %+v", r)
	}
	for _, p := range []Purpose{WorkerEntry, WorkerGuard, FinalUpgradeVerification} {
		if r := Assess(s, s.Target, Policy{Purpose: p}, testNow); r.State != Ready {
			t.Fatalf("future eligibility applied at %s: %+v", p, r)
		}
	}
	s = healthySnapshot()
	s.Version.Status.Desired.Image = "mirror.example/ocp:other@sha256:" + strings.Repeat("a", 64)
	if r := Assess(s, s.Target, Policy{Purpose: WorkerEntry}, testNow); r.State != Ready {
		t.Fatalf("mirror rejected: %+v", r)
	}
	for _, p := range []Policy{{Purpose: "unknown"}, {Purpose: SourceBaseline}, {Purpose: SourceBaseline, NextRelease: release("4.22.0")}, {Purpose: SourceBaseline, NextRelease: release("5.0.0")}, {Purpose: WorkerGuard, NextRelease: release("4.21.0")}} {
		if r := Assess(s, s.Target, p, testNow); r.State != UnsupportedEvidence {
			t.Fatalf("invalid policy passed: %+v", r)
		}
	}
}

func TestFreshnessBoundaryAndBoundedSanitizedResult(t *testing.T) {
	s := healthySnapshot()
	for _, age := range []time.Duration{DecisionAge, DecisionAge + time.Nanosecond} {
		r := Assess(s, s.Target, Policy{Purpose: WorkerGuard}, testNow.Add(age))
		if (r.State == Ready) != (age == DecisionAge) {
			t.Fatalf("incorrect decision age %s: %+v", age, r)
		}
	}
	co := s.Operators[0]
	s.Operators = nil
	// Worst supported names/versions still fit the result's 128KiB status budget.
	version := "4.20.1-" + strings.Repeat("a", 121)
	for i := 0; i < MaxOperators; i++ {
		item := *co.DeepCopy()
		item.Name = fmt.Sprintf("%03d.", i) + strings.Repeat("a", 63) + "." + strings.Repeat("b", 63) + "." + strings.Repeat("c", 63) + "." + strings.Repeat("d", 57)
		item.UID = types.UID(item.Name)
		item.Status.Versions[0].Version = version
		item.Status.Conditions[0].Message = "raw-sensitive-response secret-observer-token"
		s.Operators = append(s.Operators, item)
	}
	r := Assess(s, s.Target, Policy{Purpose: WorkerGuard}, testNow)
	b, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	if len(b) > 128<<10 || strings.Contains(string(b), "secret-observer-token") || len(r.Operators) != MaxOperators || r.State == Ready {
		t.Fatalf("unbounded or unsafe result: bytes=%d operators=%d state=%s", len(b), len(r.Operators), r.State)
	}
}

func healthyVirtualization(version string) *VirtualizationSnapshot {
	return &VirtualizationSnapshot{
		Present:            true,
		Name:               "kubevirt-hyperconverged",
		Namespace:          "openshift-cnv",
		SystemHealthStatus: "healthy",
		Conditions: map[string]configv1.ConditionStatus{
			"ReconcileComplete": configv1.ConditionTrue,
			"Upgradeable":       configv1.ConditionTrue,
			"Available":         configv1.ConditionTrue,
			"Degraded":          configv1.ConditionFalse,
			"Progressing":       configv1.ConditionFalse,
		},
		OperatorVersion:       version,
		OutdatedWorkloads:     0,
		WorkloadsObserved:     true,
		HCO:                   metav1.ObjectMeta{Name: "kubevirt-hyperconverged", Namespace: "openshift-cnv", UID: "hco-uid", Generation: 1, ResourceVersion: "1"},
		KubeVirt:              metav1.ObjectMeta{Name: "kubevirt-kubevirt-hyperconverged", Namespace: "openshift-cnv", UID: "kv-uid", Generation: 1, ResourceVersion: "1"},
		HCOObservedGeneration: 1, KubeVirtObservedGeneration: 1, KubeVirtConverged: true,
		WorkloadMethodsObserved: true,
	}
}

func TestVirtualizationAssessmentMatrix(t *testing.T) {
	purposes := []Purpose{
		SourceBaseline,
		BeforeControlPlaneRequest,
		ObserveControlPlane,
		IntermediateVerification,
		WorkerEntry,
		WorkerGuard,
		FinalUpgradeVerification,
	}

	t.Run("absent-virt-passes-all", func(t *testing.T) {
		for _, p := range purposes {
			s := healthySnapshot()
			s.Virtualization = nil
			r := Assess(s, s.Target, policyFor(p), testNow)
			if p == ObserveControlPlane {
				if r.State != Converging || r.Reason != "ObservationOnly" {
					t.Fatalf("%s: expected ObservationOnly, got %+v", p, r)
				}
			} else if r.State != Ready {
				t.Fatalf("%s: expected Ready, got %+v", p, r)
			}

			s.Virtualization = nil
			r = Assess(s, s.Target, policyFor(p), testNow)
			if p == ObserveControlPlane {
				if r.State != Converging || r.Reason != "ObservationOnly" {
					t.Fatalf("%s: expected ObservationOnly, got %+v", p, r)
				}
			} else if r.State != Ready {
				t.Fatalf("%s: expected Ready, got %+v", p, r)
			}
		}
	})

	t.Run("healthy-virt-passes-and-summarizes", func(t *testing.T) {
		for _, p := range purposes {
			s := healthySnapshot()
			s.Virtualization = healthyVirtualization(s.Target.Expected.Version)
			r := Assess(s, s.Target, policyFor(p), testNow)
			if p == ObserveControlPlane {
				if r.State != Converging {
					t.Fatalf("%s: expected Converging, got %+v", p, r)
				}
			} else if r.State != Ready {
				t.Fatalf("%s: expected Ready, got %+v", p, r)
			}
			if r.Virtualization == nil || r.Virtualization.Version != s.Target.Expected.Version {
				t.Fatalf("%s: openshift-virtualization missing from operators summary: %+v", p, r.Operators)
			}
		}
	})

	t.Run("system-health-unhealthy", func(t *testing.T) {
		for _, health := range []string{"warning", "error", "unknown"} {
			for _, p := range []Purpose{SourceBaseline, BeforeControlPlaneRequest, IntermediateVerification, WorkerEntry, WorkerGuard, FinalUpgradeVerification} {
				s := healthySnapshot()
				s.Virtualization = healthyVirtualization(s.Target.Expected.Version)
				s.Virtualization.SystemHealthStatus = health
				r := Assess(s, s.Target, policyFor(p), testNow)
				if r.State != Unhealthy || r.Reason != "CNVSystemUnhealthy" {
					t.Fatalf("%s (%s): expected Unhealthy/CNVSystemUnhealthy, got %+v", p, health, r)
				}
			}
		}
	})

	t.Run("condition-upgradeable", func(t *testing.T) {
		cases := []struct {
			purpose Purpose
			nextRel string
			want    State
			reason  string
		}{
			{SourceBaseline, "4.21.0", Unhealthy, "CNVNotUpgradeable"}, // minor hop gates on Upgradeable
			{SourceBaseline, "4.20.2", Unhealthy, "CNVNotUpgradeable"},
			{BeforeControlPlaneRequest, "4.21.0", Unhealthy, "CNVNotUpgradeable"},
			{IntermediateVerification, "", Unhealthy, "CNVNotUpgradeable"}, // intermediate must be ready for next hop
			{WorkerEntry, "", Ready, "GuestReady"},
			{WorkerGuard, "", Ready, "GuestReady"},
		}
		for _, tc := range cases {
			t.Run(string(tc.purpose)+"-"+tc.nextRel, func(t *testing.T) {
				s := healthySnapshot()
				s.Virtualization = healthyVirtualization(s.Target.Expected.Version)
				s.Virtualization.Conditions["Upgradeable"] = configv1.ConditionFalse
				pol := Policy{Purpose: tc.purpose}
				if tc.nextRel != "" {
					pol.NextRelease = release(tc.nextRel)
				}
				r := Assess(s, s.Target, pol, testNow)
				if r.State != tc.want || r.Reason != tc.reason {
					t.Fatalf("expected %s/%s, got %+v", tc.want, tc.reason, r)
				}
			})
		}
	})

	t.Run("condition-degraded", func(t *testing.T) {
		for _, p := range purposes {
			s := healthySnapshot()
			s.Virtualization = healthyVirtualization(s.Target.Expected.Version)
			s.Virtualization.Conditions["Degraded"] = configv1.ConditionTrue
			r := Assess(s, s.Target, policyFor(p), testNow)
			if r.State != Unhealthy || r.Reason != "CNVDegraded" {
				t.Fatalf("%s: expected Unhealthy/CNVDegraded, got %+v", p, r)
			}
		}
	})

	t.Run("condition-not-available", func(t *testing.T) {
		for _, p := range purposes {
			s := healthySnapshot()
			s.Virtualization = healthyVirtualization(s.Target.Expected.Version)
			s.Virtualization.Conditions["Available"] = configv1.ConditionFalse
			r := Assess(s, s.Target, policyFor(p), testNow)
			if r.State != Unhealthy || r.Reason != "CNVNotAvailable" {
				t.Fatalf("%s: expected Unhealthy/CNVNotAvailable, got %+v", p, r)
			}
		}
	})

	t.Run("condition-progressing", func(t *testing.T) {
		cases := []struct {
			purpose Purpose
			want    State
			reason  string
		}{
			{SourceBaseline, Converging, "CNVProgressing"},
			{BeforeControlPlaneRequest, Converging, "CNVProgressing"},
			{IntermediateVerification, Converging, "CNVProgressing"},
			{WorkerEntry, Converging, "CNVProgressing"},
			{WorkerGuard, Converging, "CNVProgressing"},
			{FinalUpgradeVerification, Converging, "CNVProgressing"},
		}
		for _, tc := range cases {
			t.Run(string(tc.purpose), func(t *testing.T) {
				s := healthySnapshot()
				s.Virtualization = healthyVirtualization(s.Target.Expected.Version)
				s.Virtualization.Conditions["Progressing"] = configv1.ConditionTrue
				r := Assess(s, s.Target, policyFor(tc.purpose), testNow)
				if r.State != tc.want || r.Reason != tc.reason {
					t.Fatalf("expected %s/%s, got %+v", tc.want, tc.reason, r)
				}
			})
		}
	})

	t.Run("outdated-workloads", func(t *testing.T) {
		cases := []struct {
			purpose Purpose
			want    State
			reason  string
		}{
			{SourceBaseline, Converging, "CNVOutdatedWorkloads"},
			{BeforeControlPlaneRequest, Converging, "CNVOutdatedWorkloads"},
			{IntermediateVerification, Ready, "GuestReady"},
			{WorkerEntry, Converging, "CNVOutdatedWorkloads"},
			{WorkerGuard, Converging, "CNVOutdatedWorkloads"},
			{FinalUpgradeVerification, Converging, "CNVOutdatedWorkloads"},
		}
		for _, tc := range cases {
			t.Run(string(tc.purpose), func(t *testing.T) {
				s := healthySnapshot()
				s.Virtualization = healthyVirtualization(s.Target.Expected.Version)
				s.Virtualization.OutdatedWorkloads = 5
				r := Assess(s, s.Target, policyFor(tc.purpose), testNow)
				if r.State != tc.want || r.Reason != tc.reason {
					t.Fatalf("expected %s/%s, got %+v", tc.want, tc.reason, r)
				}
			})
		}
	})

	t.Run("version-alignment", func(t *testing.T) {
		// IntermediateVerification: expected is 4.21.0
		s := healthySnapshot()
		s.Target.Expected = release("4.21.0")
		s.Version.Status.Desired.Version = s.Target.Expected.Version
		s.Version.Status.History[0].Version = s.Target.Expected.Version
		s.Operators[0].Status.Versions[0].Version = s.Target.Expected.Version

		// HCO at 4.20.1 -> mismatch!
		s.Virtualization = healthyVirtualization("4.20.1")
		r := Assess(s, s.Target, Policy{Purpose: IntermediateVerification}, testNow)
		if r.State != Converging || r.Reason != "CNVIntermediateVersionMismatch" {
			t.Fatalf("expected IntermediateVersionMismatch, got %+v", r)
		}

		// HCO updated to 4.21.3 -> matches intermediate minor!
		s.Virtualization = healthyVirtualization("4.21.3")
		r = Assess(s, s.Target, Policy{Purpose: IntermediateVerification}, testNow)
		if r.State != Ready {
			t.Fatalf("expected Ready on matched intermediate version, got %+v", r)
		}

		// WorkerEntry: expected is target 4.22.0
		s.Target.Expected = release("4.22.0")
		s.Version.Status.Desired.Version = s.Target.Expected.Version
		s.Version.Status.History[0].Version = s.Target.Expected.Version
		s.Operators[0].Status.Versions[0].Version = s.Target.Expected.Version

		// HCO still at 4.21.3 -> target mismatch!
		s.Virtualization = healthyVirtualization("4.21.3")
		r = Assess(s, s.Target, Policy{Purpose: WorkerEntry}, testNow)
		if r.State != Converging || r.Reason != "CNVTargetVersionMismatch" {
			t.Fatalf("expected TargetVersionMismatch, got %+v", r)
		}

		// HCO updated to 4.22.0 -> matches target!
		s.Virtualization = healthyVirtualization("4.22.0")
		r = Assess(s, s.Target, Policy{Purpose: WorkerEntry}, testNow)
		if r.State != Ready {
			t.Fatalf("expected Ready on matched target version, got %+v", r)
		}

		// SourceBaseline: expected is 4.20.1, HCO at 4.19.0 -> mismatch!
		s.Target.Expected = release("4.20.1")
		s.Version.Status.Desired.Version = s.Target.Expected.Version
		s.Version.Status.History[0].Version = s.Target.Expected.Version
		s.Operators[0].Status.Versions[0].Version = s.Target.Expected.Version
		s.Virtualization = healthyVirtualization("4.19.0")
		r = Assess(s, s.Target, policyFor(SourceBaseline), testNow)
		if r.State != Converging || r.Reason != "CNVVersionMismatch" {
			t.Fatalf("expected CNVVersionMismatch at SourceBaseline, got %+v", r)
		}
	})

	t.Run("malformed-operator-version", func(t *testing.T) {
		for _, badVersion := range []string{"invalid", "4.20", "4.20.1/build", ""} {
			s := healthySnapshot()
			s.Virtualization = healthyVirtualization(badVersion)
			r := Assess(s, s.Target, Policy{Purpose: IntermediateVerification}, testNow)
			if r.State != UnsupportedEvidence || r.Reason != "MissingOrMalformedOperatorVersion" {
				t.Fatalf("expected UnsupportedEvidence/MissingOrMalformedOperatorVersion for %q, got %+v", badVersion, r)
			}
		}
	})
}
