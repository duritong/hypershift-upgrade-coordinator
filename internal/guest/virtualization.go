package guest

import (
	"context"
	"net/url"
	"reflect"

	upgradev1 "github.com/duritong/hypershift-upgrade-coordinator/api/v1alpha1"
	configv1 "github.com/openshift/api/config/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
)

const hcoV1Path = "/apis/hco.kubevirt.io/v1/hyperconvergeds"
const hcoV1Beta1Path = "/apis/hco.kubevirt.io/v1beta1/hyperconvergeds"
const kubeVirtPath = "/apis/kubevirt.io/v1/kubevirts"

// These intentionally narrow wire types retain validity/presence information.
// Unknown API fields are allowed; required evidence is never defaulted to healthy.
type virtualizationList struct {
	APIVersion      string `json:"apiVersion"`
	Kind            string `json:"kind"`
	metav1.ListMeta `json:"metadata"`
	Items           []virtualizationResource `json:"items"`
}

type virtualizationResource struct {
	APIVersion        string `json:"apiVersion"`
	Kind              string `json:"kind"`
	metav1.ObjectMeta `json:"metadata"`
	Spec              struct {
		WorkloadUpdateStrategy struct {
			Methods *[]string `json:"workloadUpdateMethods"`
		} `json:"workloadUpdateStrategy"`
	} `json:"spec"`
	Status struct {
		SystemHealthStatus string                    `json:"systemHealthStatus"`
		ObservedGeneration int64                     `json:"observedGeneration"`
		Conditions         []metav1.Condition        `json:"conditions"`
		Versions           []configv1.OperandVersion `json:"versions"`
		OutdatedWorkloads  *int                      `json:"outdatedVirtualMachineInstanceWorkloads"`
		ObservedVersion    string                    `json:"observedKubeVirtVersion"`
		TargetVersion      string                    `json:"targetKubeVirtVersion"`
		ObservedDeployment string                    `json:"observedDeploymentID"`
		TargetDeployment   string                    `json:"targetDeploymentID"`
	} `json:"status"`
}

func validVirtualizationIdentity(m metav1.ObjectMeta) bool {
	return m.Name != "" && len(validation.IsDNS1123Subdomain(m.Name)) == 0 &&
		m.Namespace != "" && len(validation.IsDNS1123Label(m.Namespace)) == 0 &&
		m.UID != "" && m.ResourceVersion != "" && m.Generation > 0 && m.DeletionTimestamp.IsZero()
}

func validVirtualizationList(l virtualizationList, api, kind string) bool {
	// A singleton installation is the supported contract. Do not silently select
	// the first installation or authorize from the first page of a larger list.
	return l.APIVersion == api && l.Kind == kind && l.ResourceVersion != "" &&
		l.Items != nil && len(l.Items) <= 1 && l.Continue == "" &&
		(l.RemainingItemCount == nil || *l.RemainingItemCount == 0)
}

func virtualizationConditions(cs []metav1.Condition) (map[string]configv1.ConditionStatus, bool) {
	out := map[string]configv1.ConditionStatus{}
	for _, c := range cs {
		if _, exists := out[c.Type]; exists || c.Type == "" || (c.Status != metav1.ConditionTrue && c.Status != metav1.ConditionFalse && c.Status != metav1.ConditionUnknown) {
			return nil, false
		}
		out[c.Type] = configv1.ConditionStatus(c.Status)
	}
	return out, true
}

func (r *Reader) collectVirtualization(ctx context.Context, remaining *int64) (*VirtualizationSnapshot, error) {
	bad := func() (*VirtualizationSnapshot, error) {
		return nil, &Failure{UnsupportedEvidence, "CNVMalformedEvidence"}
	}
	var before virtualizationList
	activeHCOPath := hcoV1Path
	activeAPIVersion := "hco.kubevirt.io/v1"
	found, err := r.getAbs(ctx, activeHCOPath, url.Values{"limit": {"2"}}, &before, remaining, true)
	if err != nil {
		return nil, err
	}
	if !found {
		activeHCOPath = hcoV1Beta1Path
		activeAPIVersion = "hco.kubevirt.io/v1beta1"
		found, err = r.getAbs(ctx, activeHCOPath, url.Values{"limit": {"2"}}, &before, remaining, true)
		if err != nil {
			return nil, err
		}
	}
	if !found {
		// A version-specific 404 alone does not prove the product is absent:
		// another served API version may exist. Fail closed when the group exists.
		var group metav1.APIGroup
		exists, err := r.getAbs(ctx, "/apis/hco.kubevirt.io", nil, &group, remaining, true)
		if err != nil {
			return nil, err
		}
		if exists {
			return nil, &Failure{UnsupportedEvidence, "CNVUnsupportedAPI"}
		}
		return nil, nil
	}
	if !validVirtualizationList(before, activeAPIVersion, "HyperConvergedList") {
		return bad()
	}
	if len(before.Items) == 0 {
		return nil, nil
	}
	hco := before.Items[0]
	if hco.Kind != "HyperConverged" || hco.APIVersion != before.APIVersion || !validVirtualizationIdentity(hco.ObjectMeta) {
		return bad()
	}
	cs, valid := virtualizationConditions(hco.Status.Conditions)
	if !valid {
		return bad()
	}
	version := ""
	seen := map[string]bool{}
	for _, v := range hco.Status.Versions {
		if v.Name == "" || seen[v.Name] {
			return bad()
		}
		seen[v.Name] = true
		if v.Name == "operator" {
			version = v.Version
		}
	}
	var kvList virtualizationList
	// A missing dependent resource is unavailable evidence, not absence of CNV.
	if _, err := r.getAbs(ctx, kubeVirtPath, url.Values{"limit": {"2"}}, &kvList, remaining, false); err != nil {
		return nil, err
	}
	if !validVirtualizationList(kvList, "kubevirt.io/v1", "KubeVirtList") || len(kvList.Items) != 1 {
		return bad()
	}
	kv := kvList.Items[0]
	if kv.Kind != "KubeVirt" || kv.APIVersion != kvList.APIVersion || !validVirtualizationIdentity(kv.ObjectMeta) || kv.Namespace != hco.Namespace {
		return bad()
	}
	owned := false
	for _, owner := range kv.OwnerReferences {
		if owner.UID == hco.UID && owner.Name == hco.Name && owner.Kind == "HyperConverged" && owner.APIVersion == hco.APIVersion {
			owned = true
		}
	}
	if !owned {
		return bad()
	}
	kcs, valid := virtualizationConditions(kv.Status.Conditions)
	if !valid {
		return bad()
	}
	var after virtualizationList
	if _, err := r.getAbs(ctx, activeHCOPath, url.Values{"limit": {"2"}}, &after, remaining, false); err != nil {
		return nil, err
	}
	if !reflect.DeepEqual(before, after) {
		return nil, &Failure{Unavailable, "CNVObservationConflict"}
	}
	var kvAfter virtualizationList
	if _, err := r.getAbs(ctx, kubeVirtPath, url.Values{"limit": {"2"}}, &kvAfter, remaining, false); err != nil {
		return nil, err
	}
	if !reflect.DeepEqual(kvList, kvAfter) {
		return nil, &Failure{Unavailable, "CNVObservationConflict"}
	}
	s := &VirtualizationSnapshot{Present: true, Name: hco.Name, Namespace: hco.Namespace,
		HCO: hco.ObjectMeta, KubeVirt: kv.ObjectMeta, HCOObservedGeneration: hco.Status.ObservedGeneration,
		KubeVirtObservedGeneration: kv.Status.ObservedGeneration, Conditions: cs,
		SystemHealthStatus: hco.Status.SystemHealthStatus, OperatorVersion: version,
		KubeVirtConverged: kcs["Available"] == configv1.ConditionTrue && kcs["Degraded"] == configv1.ConditionFalse && kcs["Progressing"] == configv1.ConditionFalse &&
			kv.Status.ObservedVersion != "" && kv.Status.ObservedVersion == kv.Status.TargetVersion && kv.Status.ObservedDeployment != "" && kv.Status.ObservedDeployment == kv.Status.TargetDeployment,
	}
	if kv.Status.OutdatedWorkloads != nil && *kv.Status.OutdatedWorkloads >= 0 {
		s.WorkloadsObserved, s.OutdatedWorkloads = true, *kv.Status.OutdatedWorkloads
	}
	if hco.Spec.WorkloadUpdateStrategy.Methods != nil {
		s.WorkloadMethodsObserved = true
		s.HCOMethods = append([]string(nil), (*hco.Spec.WorkloadUpdateStrategy.Methods)...)
		// KubeVirt v1 explicitly defaults an empty/omitted methods list to no
		// automated updates (and serializes empty lists with omitempty). HCO
		// defaults to LiveMigrate instead, so its explicit empty list is required.
		if kv.Spec.WorkloadUpdateStrategy.Methods != nil {
			s.KubeVirtMethods = append([]string(nil), (*kv.Spec.WorkloadUpdateStrategy.Methods)...)
		}
	}
	return s, nil
}

func assessVirtualization(v *VirtualizationSnapshot, expected Release, policy Policy, add func(string, State, string)) *OperatorSummary {
	if v == nil {
		return nil
	}
	block := func(s State, reason string) { add("openshift-virtualization", s, reason) }
	if !v.Present || !validVirtualizationIdentity(v.HCO) || !validVirtualizationIdentity(v.KubeVirt) {
		block(UnsupportedEvidence, "CNVMalformedEvidence")
		return nil
	}
	if v.HCOObservedGeneration != v.HCO.Generation || v.KubeVirtObservedGeneration != v.KubeVirt.Generation {
		block(Unavailable, "CNVStaleGeneration")
	}
	if !v.KubeVirtConverged {
		block(Converging, "CNVKubeVirtConverging")
	}
	if v.SystemHealthStatus == "" {
		block(UnsupportedEvidence, "CNVHealthUnknown")
	} else if v.SystemHealthStatus != "healthy" {
		block(Unhealthy, "CNVSystemUnhealthy")
	}
	for _, c := range []struct {
		name   string
		want   configv1.ConditionStatus
		reason string
	}{
		{"Available", configv1.ConditionTrue, "CNVNotAvailable"},
		{"Degraded", configv1.ConditionFalse, "CNVDegraded"},
		{"Progressing", configv1.ConditionFalse, "CNVProgressing"},
		{"ReconcileComplete", configv1.ConditionTrue, "CNVReconcileIncomplete"},
	} {
		value := v.Conditions[c.name]
		if value != configv1.ConditionTrue && value != configv1.ConditionFalse {
			block(UnsupportedEvidence, "CNVConditionsUnknown")
			continue
		}
		if value != c.want {
			state := Unhealthy
			if c.name == "Progressing" || c.name == "ReconcileComplete" {
				state = Converging
			}
			block(state, c.reason)
		}
	}
	if policy.Purpose == SourceBaseline || policy.Purpose == BeforeControlPlaneRequest || policy.Purpose == IntermediateVerification {
		if v.Conditions["Upgradeable"] != configv1.ConditionTrue {
			block(Unhealthy, "CNVNotUpgradeable")
		}
	}
	if policy.HoldVirtualizationWorkloads && (!v.WorkloadMethodsObserved || len(v.HCOMethods) != 0 || len(v.KubeVirtMethods) != 0) {
		block(Unhealthy, "CNVWorkloadUpdatesNotHeld")
	}
	if policy.RequireRestoredWorkloads && v.WorkloadMethodsObserved && len(v.HCOMethods) == 0 && len(v.KubeVirtMethods) == 0 {
		block(Converging, "CNVWorkloadUpdatesStillHeld")
	}
	// Intermediate workload skew is intentional while automatic updates are held.
	// New worker actions, unlike observation of already-running work, require zero.
	intermediateSource := (policy.Purpose == SourceBaseline || policy.Purpose == BeforeControlPlaneRequest) && policy.HoldVirtualizationWorkloads && policy.AllowIntermediateWorkloadSkew
	if policy.Purpose != IntermediateVerification && policy.Purpose != ObserveControlPlane && !intermediateSource {
		if !v.WorkloadsObserved || v.OutdatedWorkloads < 0 {
			block(UnsupportedEvidence, "CNVWorkloadsUnknown")
		} else if v.OutdatedWorkloads > 0 {
			block(Converging, "CNVOutdatedWorkloads")
		}
	}
	if !fullVersion(v.OperatorVersion) {
		block(UnsupportedEvidence, "MissingOrMalformedOperatorVersion")
		return nil
	}
	major, minor, _ := upgradev1.GetMajorMinor(v.OperatorVersion)
	emajor, eminor, _ := upgradev1.GetMajorMinor(expected.Version)
	if major != emajor || minor != eminor {
		reason := "CNVVersionMismatch"
		if policy.Purpose == IntermediateVerification {
			reason = "CNVIntermediateVersionMismatch"
		}
		if policy.Purpose == WorkerEntry || policy.Purpose == WorkerGuard || policy.Purpose == FinalUpgradeVerification {
			reason = "CNVTargetVersionMismatch"
		}
		block(Converging, reason)
	}
	return &OperatorSummary{Name: "openshift-virtualization", Version: v.OperatorVersion}
}
