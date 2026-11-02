package controller

import (
	"fmt"
	"testing"

	upgradev1 "github.com/duritong/hypershift-upgrade-coordinator/api/v1alpha1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func (f *upgradeFlow) virtualization(version string, outdated int, methods ...string) {
	f.guest.mu.Lock()
	defer f.guest.mu.Unlock()
	f.guest.objects["/apis/hco.kubevirt.io/v1beta1/hyperconvergeds"] = flowHCO(version, true, true, methods...)
	f.guest.objects["/apis/kubevirt.io/v1/kubevirts"] = flowKubeVirt(outdated, methods...)
}

func TestVirtualizationFullUpgradePaths(t *testing.T) {
	for _, path := range []string{"direct", "patch", "intermediate"} {
		for _, strategy := range []string{"Replace", "InPlace"} {
			for _, pools := range []int{0, 2} {
				t.Run(fmt.Sprintf("%s/%s/pools-%d", path, strategy, pools), func(t *testing.T) {
					f := newUpgradeFlow(t, path == "intermediate", strategy, pools, 1)
					if path == "patch" {
						u := f.hcu()
						u.Spec.TargetReleaseImage = regressionImage("4.20.1")
						f.publish(u)
					}
					f.virtualization("4.20.0", 0)
					hops := 1
					if path == "intermediate" {
						hops = 2
					}
					for i := 0; i < hops; i++ {
						f.until(upgradev1.PhaseVerifyingControlPlane)
						f.publish(f.completeCP())
						u := f.hcu()
						version, _ := upgradev1.ExtractVersion(u.Status.ControlPlaneStatus.TargetReleaseImage)
						methods := []string{}
						if path == "intermediate" && i == hops-1 {
							methods = []string{"LiveMigrate"}
						}
						f.virtualization(version, 0, methods...)
						f.step()
					}
					f.until(upgradev1.PhaseUpgradingNodePools)
					version, _ := upgradev1.ExtractVersion(f.hcu().Spec.TargetReleaseImage)
					for i := 0; i < pools; i++ {
						f.step()
						f.completeWorker(fmt.Sprintf("pool-%d", i))
					}
					methods := []string{}
					if path == "intermediate" {
						methods = []string{"LiveMigrate"}
					}
					// Both completion branches must fail closed and later recover.
					f.virtualization(version, 2, methods...)
					before := f.requests()
					f.step()
					f.assertRequestsUnchanged(before)
					if f.hcu().Status.Phase == upgradev1.PhaseCompleted {
						t.Fatal("completed with outdated workloads")
					}
					f.virtualization(version, 0, methods...)
					f.until(upgradev1.PhaseCompleted)
					if f.hcu().Status.GuestAssessment.Virtualization == nil {
						t.Fatal("validated virtualization summary not published")
					}
				})
			}
		}
	}
}

func TestVirtualizationFreshActionBoundary(t *testing.T) {
	for _, strategy := range []string{"Replace", "InPlace"} {
		for _, boundary := range []string{"source", "cp-request", "intermediate-request", "first-worker", "next-worker"} {
			for _, failure := range []string{"version", "workloads", "health", "generation", "missing-kubevirt"} {
				if boundary == "intermediate-request" && failure == "workloads" {
					continue
				} // deliberate held intermediate skew
				t.Run(strategy+"/"+boundary+"/"+failure, func(t *testing.T) {
					f := newUpgradeFlow(t, boundary == "intermediate-request", strategy, 2, 1)
					version := "4.20.0"
					f.virtualization(version, 0)
					if boundary == "cp-request" {
						f.until(upgradev1.PhaseUpgradingControlPlane)
					}
					if boundary == "intermediate-request" {
						f.until(upgradev1.PhaseVerifyingControlPlane)
						f.publish(f.completeCP())
						version = "4.21.0"
						f.virtualization(version, 3)
						f.step()
						if f.hcu().Status.Phase != upgradev1.PhaseUpgradingControlPlane {
							t.Fatal("held intermediate skew blocked legitimate hop")
						}
					}
					if boundary == "first-worker" || boundary == "next-worker" {
						f.until(upgradev1.PhaseVerifyingControlPlane)
						f.publish(f.completeCP())
						version = "4.21.0"
						f.virtualization(version, 0)
						f.step()
						f.until(upgradev1.PhaseUpgradingNodePools)
						if boundary == "next-worker" {
							f.step()
							f.completeWorker("pool-0")
						}
					}
					f.virtualization(version, 0)
					f.guest.mu.Lock()
					h := f.guest.objects["/apis/hco.kubevirt.io/v1beta1/hyperconvergeds"]["items"].([]interface{})[0].(map[string]interface{})
					switch failure {
					case "version":
						_ = unstructured.SetNestedSlice(h, []interface{}{map[string]interface{}{"name": "operator", "version": "4.19.0"}}, "status", "versions")
					case "workloads":
						f.guest.objects["/apis/kubevirt.io/v1/kubevirts"] = flowKubeVirt(3)
					case "health":
						_ = unstructured.SetNestedField(h, "warning", "status", "systemHealthStatus")
					case "generation":
						_ = unstructured.SetNestedField(h, int64(2), "metadata", "generation")
					case "missing-kubevirt":
						delete(f.guest.objects, "/apis/kubevirt.io/v1/kubevirts")
					}
					f.guest.mu.Unlock()
					before := f.requests()
					for range 3 {
						f.step()
					}
					f.assertRequestsUnchanged(before)
					if boundary == "source" {
						for _, name := range []string{"pool-0", "pool-1"} {
							if f.object("NodePool", name).GetAnnotations()[workerLockOwner] != "" {
								t.Fatal("source failure acquired lock")
							}
						}
					}
					if boundary == "next-worker" && f.hcu().Status.NodePoolStatuses[0].StartTime != nil {
						t.Fatal("completed worker timing restarted")
					}
					f.virtualization(version, 0)
					if boundary == "source" || boundary == "cp-request" || boundary == "intermediate-request" {
						f.until(upgradev1.PhaseVerifyingControlPlane)
					} else {
						f.step()
						if len(f.requests()) == len(before) {
							t.Fatal("worker did not recover")
						}
					}
				})
			}
		}
	}
}

func TestVirtualizationIntermediatePreparation(t *testing.T) {
	for _, resource := range []string{"hco", "kubevirt"} {
		f := newUpgradeFlow(t, true, "Replace", 1, 1)
		f.virtualization("4.20.0", 0)
		f.guest.mu.Lock()
		path := "/apis/hco.kubevirt.io/v1beta1/hyperconvergeds"
		if resource == "kubevirt" {
			path = "/apis/kubevirt.io/v1/kubevirts"
		}
		item := f.guest.objects[path]["items"].([]interface{})[0].(map[string]interface{})
		_ = unstructured.SetNestedSlice(item, []interface{}{"LiveMigrate"}, "spec", "workloadUpdateStrategy", "workloadUpdateMethods")
		f.guest.mu.Unlock()
		for range 3 {
			f.step()
		}
		if len(f.requests()) != 0 || f.hcu().Status.GuestAssessment.Reason != "CNVWorkloadUpdatesNotHeld" {
			t.Fatal("external preparation not required")
		}
	}
}

func TestVirtualizationResumeAdoptionAndActiveReservation(t *testing.T) {
	for _, strategy := range []string{"Replace", "InPlace"} {
		for _, scenario := range []string{"resume-cp", "adopt-workers", "active-reservation"} {
			t.Run(strategy+"/"+scenario, func(t *testing.T) {
				f := newUpgradeFlow(t, false, strategy, 2, 1)
				if scenario == "resume-cp" {
					f.virtualization("4.20.0", 0)
					f.until(upgradev1.PhaseUpgradingControlPlane)
					u := f.hcu()
					pause := "true"
					u.Spec.PausedUntil = &pause
					u.Generation++
					f.publish(u)
					f.until(upgradev1.PhasePaused)
					u = f.hcu()
					u.Spec.PausedUntil = nil
					u.Generation++
					f.publish(u)
					f.virtualization("4.19.0", 0)
					before := f.requests()
					for range 3 {
						f.step()
					}
					f.assertRequestsUnchanged(before)
					f.virtualization("4.20.0", 0)
					f.until(upgradev1.PhaseVerifyingControlPlane)
					return
				}
				f.readyTarget()
				f.virtualization("4.21.0", 0)
				if scenario == "adopt-workers" {
					u := f.hcu()
					u.Status.WorkerAdmission = nil
					f.publish(u)
					f.virtualization("4.19.0", 0)
					before := f.requests()
					f.step()
					f.assertRequestsUnchanged(before)
					f.virtualization("4.21.0", 0)
					f.step()
					if len(f.requests()) == len(before) {
						t.Fatal("adoption did not recover")
					}
					return
				}
				f.step()
				start := f.hcu().Status.NodePoolStatuses[0].StartTime.DeepCopy()
				if start == nil {
					t.Fatal("worker not started")
				}
				f.virtualization("4.19.0", 3)
				before := f.requests()
				for range 3 {
					f.step()
				}
				f.assertRequestsUnchanged(before)
				if !f.hcu().Status.NodePoolStatuses[0].StartTime.Equal(start) || f.object("NodePool", "pool-0").GetAnnotations()[workerLockOwner] != "" {
					t.Fatal("guest failure restarted/relocked active worker")
				}
				f.virtualization("4.21.0", 0)
				f.step()
				f.assertRequestsUnchanged(before)
				f.completeWorker("pool-0")
				f.step()
				if len(f.requests()) == len(before) {
					t.Fatal("reservation did not release on completion")
				}
			})
		}
	}
}

func TestVirtualizationAdoptsAlreadyReachedIntermediate(t *testing.T) {
	for _, strategy := range []string{"Replace", "InPlace"} {
		t.Run(strategy, func(t *testing.T) {
			f := newUpgradeFlow(t, true, strategy, 1, 1)
			f.virtualization("4.20.0", 0)
			f.until(upgradev1.PhaseVerifyingControlPlane)
			f.publish(f.completeCP())
			// Simulate fresh enrollment/recovery after another actor completed the
			// intermediate CP: no persisted phase or historical guest admission.
			u := f.hcu()
			u.Status = upgradev1.HostedClusterUpgradeStatus{}
			f.publish(u)
			f.virtualization("4.21.0", 3)
			before := len(f.requests())
			f.until(upgradev1.PhaseVerifyingControlPlane)
			if len(f.requests()) != before+1 || f.hcu().Status.ControlPlaneStatus.TargetReleaseImage != f.hcu().Spec.TargetReleaseImage {
				t.Fatal("adoption repeated intermediate or failed to request target")
			}
		})
	}
}
