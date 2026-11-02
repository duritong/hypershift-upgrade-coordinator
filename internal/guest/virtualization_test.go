package guest

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"

	configv1 "github.com/openshift/api/config/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func virtualizationWire() (virtualizationList, virtualizationList) {
	v := healthyVirtualization("4.20.1")
	hco := virtualizationResource{APIVersion: "hco.kubevirt.io/v1beta1", Kind: "HyperConverged", ObjectMeta: v.HCO}
	hco.Status.SystemHealthStatus, hco.Status.ObservedGeneration = "healthy", 1
	hco.Status.Versions = []configv1.OperandVersion{{Name: "operator", Version: "4.20.1"}}
	for _, name := range []string{"Available", "Degraded", "Progressing", "Upgradeable", "ReconcileComplete"} {
		hco.Status.Conditions = append(hco.Status.Conditions, metav1.Condition{Type: name, Status: metav1.ConditionStatus(v.Conditions[name])})
	}
	kv := virtualizationResource{APIVersion: "kubevirt.io/v1", Kind: "KubeVirt", ObjectMeta: v.KubeVirt}
	kv.OwnerReferences = []metav1.OwnerReference{{APIVersion: hco.APIVersion, Kind: hco.Kind, Name: hco.Name, UID: hco.UID}}
	kv.Status.ObservedGeneration = 1
	kv.Status.Conditions = append([]metav1.Condition(nil), hco.Status.Conditions[:3]...)
	kv.Status.ObservedVersion, kv.Status.TargetVersion = "v1.6.0", "v1.6.0"
	kv.Status.ObservedDeployment, kv.Status.TargetDeployment = "deployment-1", "deployment-1"
	zero := 0
	kv.Status.OutdatedWorkloads = &zero
	empty := []string{}
	hco.Spec.WorkloadUpdateStrategy.Methods = &empty
	kv.Spec.WorkloadUpdateStrategy.Methods = &empty
	list := func(item virtualizationResource) virtualizationList {
		return virtualizationList{APIVersion: item.APIVersion, Kind: item.Kind + "List", ListMeta: metav1.ListMeta{ResourceVersion: "1"}, Items: []virtualizationResource{item}}
	}
	return list(hco), list(kv)
}

func TestVirtualizationRejectsIncompleteEvidence(t *testing.T) {
	changes := map[string]func(*virtualizationList, *virtualizationList){
		"hco-kind":      func(h, k *virtualizationList) { h.Kind = "Unexpected" },
		"hco-api":       func(h, k *virtualizationList) { h.APIVersion = "hco.kubevirt.io/v999" },
		"hco-rv":        func(h, k *virtualizationList) { h.ResourceVersion = "" },
		"hco-nil-items": func(h, k *virtualizationList) { h.Items = nil },
		"hco-multiple":  func(h, k *virtualizationList) { h.Items = append(h.Items, h.Items[0]) },
		"hco-continue":  func(h, k *virtualizationList) { h.Continue = "next" },
		"hco-remaining": func(h, k *virtualizationList) { n := int64(1); h.RemainingItemCount = &n },
		"hco-uid":       func(h, k *virtualizationList) { h.Items[0].UID = "" },
		"hco-deleting":  func(h, k *virtualizationList) { at := metav1.NewTime(testNow); h.Items[0].DeletionTimestamp = &at },
		"hco-duplicate-condition": func(h, k *virtualizationList) {
			h.Items[0].Status.Conditions = append(h.Items[0].Status.Conditions, h.Items[0].Status.Conditions[0])
		},
		"hco-invalid-condition": func(h, k *virtualizationList) { h.Items[0].Status.Conditions[0].Status = "invalid" },
		"hco-duplicate-version": func(h, k *virtualizationList) {
			h.Items[0].Status.Versions = append(h.Items[0].Status.Versions, h.Items[0].Status.Versions[0])
		},
		"kv-empty":       func(h, k *virtualizationList) { k.Items = []virtualizationResource{} },
		"kv-multiple":    func(h, k *virtualizationList) { k.Items = append(k.Items, k.Items[0]) },
		"kv-continue":    func(h, k *virtualizationList) { k.Continue = "next" },
		"kv-rv":          func(h, k *virtualizationList) { k.ResourceVersion = "" },
		"kv-namespace":   func(h, k *virtualizationList) { k.Items[0].Namespace = "another" },
		"kv-unowned":     func(h, k *virtualizationList) { k.Items[0].OwnerReferences = nil },
		"kv-wrong-owner": func(h, k *virtualizationList) { k.Items[0].OwnerReferences[0].UID = "other" },
		"kv-duplicate-condition": func(h, k *virtualizationList) {
			k.Items[0].Status.Conditions = append(k.Items[0].Status.Conditions, k.Items[0].Status.Conditions[0])
		},
		"kv-missing-counter":   func(h, k *virtualizationList) { k.Items[0].Status.OutdatedWorkloads = nil },
		"kv-negative-counter":  func(h, k *virtualizationList) { n := -1; k.Items[0].Status.OutdatedWorkloads = &n },
		"kv-stale":             func(h, k *virtualizationList) { k.Items[0].Status.ObservedGeneration = 0 },
		"hco-stale":            func(h, k *virtualizationList) { h.Items[0].Status.ObservedGeneration = 0 },
		"kv-target-version":    func(h, k *virtualizationList) { k.Items[0].Status.TargetVersion = "v1.7.0" },
		"kv-target-deployment": func(h, k *virtualizationList) { k.Items[0].Status.TargetDeployment = "new" },
	}
	for name, change := range changes {
		t.Run(name, func(t *testing.T) {
			h, k := virtualizationWire()
			change(&h, &k)
			s := healthySnapshot()
			_, data := guestServer(t, func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case apiPath + "clusterversions/version":
					writeJSON(w, s.Version)
				case apiPath + "clusteroperators":
					writeJSON(w, page(s.Operators, "20", ""))
				case hcoV1Path:
					if h.APIVersion == "hco.kubevirt.io/v1" {
						writeJSON(w, h)
					} else {
						http.NotFound(w, r)
					}
				case hcoV1Beta1Path:
					writeJSON(w, h)
				case kubeVirtPath:
					writeJSON(w, k)
				default:
					t.Errorf("unexpected %s", r.URL.Path)
				}
			})
			got, err := reader(t, data).Collect(context.Background())
			if err == nil && Assess(got, got.Target, policyFor(SourceBaseline), testNow).State == Ready {
				t.Fatal("incomplete evidence authorized upgrade")
			}
		})
	}
}

func TestVirtualizationCollectionFailuresAndConflicts(t *testing.T) {
	for _, mode := range []string{"absent", "empty-hco", "unsupported-api", "kv-404", "kv-403", "hco-conflict", "kv-conflict", "wrong-counter-type", "invalid-item", "byte-limit"} {
		t.Run(mode, func(t *testing.T) {
			h, k := virtualizationWire()
			s := healthySnapshot()
			hr, kr := 0, 0
			_, data := guestServer(t, func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case apiPath + "clusterversions/version":
					writeJSON(w, s.Version)
				case apiPath + "clusteroperators":
					writeJSON(w, page(s.Operators, "20", ""))
				case "/apis/hco.kubevirt.io":
					if mode == "unsupported-api" {
						writeJSON(w, metav1.APIGroup{})
						return
					}
					http.NotFound(w, r)
				case hcoV1Path:
					http.NotFound(w, r)
				case hcoV1Beta1Path:
					hr++
					if mode == "absent" || mode == "unsupported-api" {
						http.NotFound(w, r)
						return
					}
					if mode == "empty-hco" {
						h.Items = []virtualizationResource{}
					}
					if mode == "hco-conflict" && hr > 1 {
						h.Items[0].ResourceVersion = "2"
					}
					if mode == "invalid-item" {
						fmt.Fprint(w, `{"apiVersion":"hco.kubevirt.io/v1beta1","kind":"HyperConvergedList","metadata":{"resourceVersion":"1"},"items":[null]}`)
						return
					}
					writeJSON(w, h)
				case kubeVirtPath:
					kr++
					if mode == "kv-404" {
						http.NotFound(w, r)
						return
					}
					if mode == "kv-403" {
						http.Error(w, "sensitive", 403)
						return
					}
					if mode == "kv-conflict" && kr > 1 {
						k.Items[0].ResourceVersion = "2"
					}
					if mode == "wrong-counter-type" {
						fmt.Fprint(w, `{"items":[{"status":{"outdatedVirtualMachineInstanceWorkloads":"unknown"}}]}`)
						return
					}
					if mode == "byte-limit" {
						fmt.Fprint(w, strings.Repeat("x", MaxBytes+1))
						return
					}
					writeJSON(w, k)
				}
			})
			got, err := reader(t, data).Collect(context.Background())
			if mode == "absent" || mode == "empty-hco" {
				if err != nil || got.Virtualization != nil {
					t.Fatalf("absence failed: %v", err)
				}
				return
			}
			if err == nil || got.complete {
				t.Fatal("failed/conflicting read returned usable snapshot")
			}
			if mode == "hco-conflict" || mode == "kv-conflict" {
				requireFailure(t, err, Unavailable, "CNVObservationConflict")
			}
		})
	}
}

func TestVirtualizationMandatoryConditionMatrix(t *testing.T) {
	for _, p := range []Purpose{SourceBaseline, BeforeControlPlaneRequest, IntermediateVerification, WorkerEntry, WorkerGuard, FinalUpgradeVerification} {
		for _, name := range []string{"Available", "Degraded", "Progressing", "ReconcileComplete", "Upgradeable"} {
			for _, value := range []configv1.ConditionStatus{"True", "False", "Unknown", "invalid", ""} {
				t.Run(fmt.Sprintf("%s/%s/%s", p, name, value), func(t *testing.T) {
					s := healthySnapshot()
					s.Virtualization = healthyVirtualization("4.20.1")
					s.Virtualization.Conditions[name] = value
					want := configv1.ConditionTrue
					if name == "Degraded" || name == "Progressing" {
						want = configv1.ConditionFalse
					}
					allowed := value == want
					if name == "Upgradeable" && (p == WorkerEntry || p == WorkerGuard || p == FinalUpgradeVerification) {
						allowed = true
					}
					r := Assess(s, s.Target, policyFor(p), testNow)
					if (r.State == Ready) != allowed {
						t.Fatalf("allowed=%v result=%+v", allowed, r)
					}
				})
			}
		}
	}
}

func TestVirtualizationVersionAndBounds(t *testing.T) {
	for _, p := range []Purpose{SourceBaseline, BeforeControlPlaneRequest, IntermediateVerification, WorkerEntry, WorkerGuard, FinalUpgradeVerification} {
		// Both older and newer patches within the expected y-stream are valid;
		// neither OCP patch equality nor catalog latest is required.
		for _, version := range []string{"4.19.0", "5.20.0", "4.21.0", "", "invalid", strings.Repeat("x", 129), "4.20.0", "4.20.1", "4.20.9"} {
			s := healthySnapshot()
			s.Virtualization = healthyVirtualization(version)
			r := Assess(s, s.Target, policyFor(p), testNow)
			if (r.State == Ready) != (version == "4.20.0" || version == "4.20.1" || version == "4.20.9") {
				t.Fatalf("%s/%s: %+v", p, version, r)
			}
			if !fullVersion(version) && r.Virtualization != nil {
				t.Fatal("unvalidated version published")
			}
		}
	}
	s := healthySnapshot()
	s.Virtualization = healthyVirtualization("4.20.1")
	co := s.Operators[0]
	s.Operators = nil
	for i := 0; i < MaxOperators; i++ {
		item := *co.DeepCopy()
		item.Name = fmt.Sprintf("co-%d", i)
		item.UID = types.UID(item.Name)
		s.Operators = append(s.Operators, item)
	}
	r := Assess(s, s.Target, policyFor(WorkerGuard), testNow)
	if r.State != Ready || len(r.Operators) != MaxOperators || r.Virtualization == nil {
		t.Fatalf("summary bounds regressed: %+v", r)
	}
}

func TestVirtualizationIntermediateWorkloadPolicy(t *testing.T) {
	for _, p := range []Purpose{SourceBaseline, BeforeControlPlaneRequest, IntermediateVerification, WorkerGuard, FinalUpgradeVerification} {
		for _, held := range []bool{false, true} {
			for _, outdated := range []int{0, 3} {
				s := healthySnapshot()
				s.Virtualization = healthyVirtualization("4.20.1")
				s.Virtualization.OutdatedWorkloads = outdated
				if !held {
					s.Virtualization.HCOMethods = []string{"LiveMigrate"}
					s.Virtualization.KubeVirtMethods = []string{"LiveMigrate"}
				}
				pol := policyFor(p)
				pol.HoldVirtualizationWorkloads = true
				pol.AllowIntermediateWorkloadSkew = p == BeforeControlPlaneRequest
				r := Assess(s, s.Target, pol, testNow)
				allowed := held && (outdated == 0 || p == IntermediateVerification || p == BeforeControlPlaneRequest)
				if (r.State == Ready) != allowed {
					t.Fatalf("%s held=%v outdated=%d: %+v", p, held, outdated, r)
				}
			}
		}
	}
}

func TestVirtualizationWorkloadMethodDefaults(t *testing.T) {
	for _, mode := range []string{"explicit-empty", "kv-omitted", "hco-omitted", "kv-enabled", "hco-enabled"} {
		t.Run(mode, func(t *testing.T) {
			h, k := virtualizationWire()
			s := healthySnapshot()
			enabled := []string{"LiveMigrate"}
			switch mode {
			case "kv-omitted":
				k.Items[0].Spec.WorkloadUpdateStrategy.Methods = nil
			case "hco-omitted":
				h.Items[0].Spec.WorkloadUpdateStrategy.Methods = nil
			case "kv-enabled":
				k.Items[0].Spec.WorkloadUpdateStrategy.Methods = &enabled
			case "hco-enabled":
				h.Items[0].Spec.WorkloadUpdateStrategy.Methods = &enabled
			}
			_, data := guestServer(t, func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case apiPath + "clusterversions/version":
					writeJSON(w, s.Version)
				case apiPath + "clusteroperators":
					writeJSON(w, page(s.Operators, "20", ""))
				case hcoV1Path:
					http.NotFound(w, r)
				case hcoV1Beta1Path:
					writeJSON(w, h)
				case kubeVirtPath:
					writeJSON(w, k)
				}
			})
			got, err := reader(t, data).Collect(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			pol := policyFor(SourceBaseline)
			pol.HoldVirtualizationWorkloads = true
			r := Assess(got, got.Target, pol, testNow)
			allowed := mode == "explicit-empty" || mode == "kv-omitted"
			if (r.State == Ready) != allowed {
				t.Fatalf("defaults %s: %+v", mode, r)
			}
		})
	}
}

func TestVirtualizationMissingHealthAndSnapshotFields(t *testing.T) {
	for _, p := range []Purpose{SourceBaseline, BeforeControlPlaneRequest, IntermediateVerification, WorkerEntry, WorkerGuard, FinalUpgradeVerification} {
		for _, mode := range []string{"health", "conditions", "present", "hco-generation", "kv-generation", "kv-completion", "version"} {
			s := healthySnapshot()
			s.Virtualization = healthyVirtualization("4.20.1")
			switch mode {
			case "health":
				s.Virtualization.SystemHealthStatus = ""
			case "conditions":
				s.Virtualization.Conditions = nil
			case "present":
				s.Virtualization.Present = false
			case "hco-generation":
				s.Virtualization.HCOObservedGeneration = 0
			case "kv-generation":
				s.Virtualization.KubeVirtObservedGeneration = 0
			case "kv-completion":
				s.Virtualization.KubeVirtConverged = false
			case "version":
				s.Virtualization.OperatorVersion = ""
			}
			if r := Assess(s, s.Target, policyFor(p), testNow); r.State == Ready {
				t.Fatalf("%s accepted missing %s", p, mode)
			}
		}
	}
}

func TestVirtualizationAPIVersionDiscovery(t *testing.T) {
	for _, version := range []string{"v1", "v1beta1"} {
		t.Run(version, func(t *testing.T) {
			h, k := virtualizationWire()
			h.APIVersion = "hco.kubevirt.io/" + version
			h.Items[0].APIVersion = h.APIVersion
			k.Items[0].OwnerReferences[0].APIVersion = h.APIVersion
			s := healthySnapshot()
			_, data := guestServer(t, func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case apiPath + "clusterversions/version":
					writeJSON(w, s.Version)
				case apiPath + "clusteroperators":
					writeJSON(w, page(s.Operators, "20", ""))
				case hcoV1Path:
					if version == "v1" {
						writeJSON(w, h)
					} else {
						http.NotFound(w, r)
					}
				case hcoV1Beta1Path:
					if version == "v1beta1" {
						writeJSON(w, h)
					} else {
						http.NotFound(w, r)
					}
				case kubeVirtPath:
					writeJSON(w, k)
				default:
					http.NotFound(w, r)
				}
			})
			got, err := reader(t, data).Collect(context.Background())
			if err != nil {
				t.Fatalf("discovery of %s failed: %v", version, err)
			}
			if got.Virtualization == nil || got.Virtualization.HCO.Name != "kubevirt-hyperconverged" {
				t.Fatalf("expected discovered virtualization for %s, got %+v", version, got.Virtualization)
			}
		})
	}
}

func TestVirtualizationWorkloadUpdatesRestorationCheck(t *testing.T) {
	s := healthySnapshot()
	s.Virtualization = healthyVirtualization("4.22.0")
	s.Target.Expected = release("4.22.0")
	s.Version.Status.Desired.Version = s.Target.Expected.Version
	s.Version.Status.History[0].Version = s.Target.Expected.Version
	s.Operators[0].Status.Versions[0].Version = s.Target.Expected.Version

	// Empty methods with RequireRestoredWorkloads -> blocks with CNVWorkloadUpdatesStillHeld
	s.Virtualization.HCOMethods = []string{}
	s.Virtualization.KubeVirtMethods = []string{}
	pol := Policy{Purpose: WorkerEntry, RequireRestoredWorkloads: true}
	r := Assess(s, s.Target, pol, testNow)
	if r.State != Converging || r.Reason != "CNVWorkloadUpdatesStillHeld" {
		t.Fatalf("expected Converging/CNVWorkloadUpdatesStillHeld, got %+v", r)
	}

	// Restored methods -> passes!
	s.Virtualization.HCOMethods = []string{"LiveMigrate"}
	s.Virtualization.KubeVirtMethods = []string{"LiveMigrate"}
	r = Assess(s, s.Target, pol, testNow)
	if r.State != Ready {
		t.Fatalf("expected Ready after restoration, got %+v", r)
	}
}
