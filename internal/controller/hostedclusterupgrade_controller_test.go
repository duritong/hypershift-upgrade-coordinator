/*
Copyright 2026 SRE Fleet Team.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	upgradev1alpha1 "github.com/duritong/hypershift-upgrade-coordinator/api/v1alpha1"
)

var _ = Describe("HostedClusterUpgrade Controller Reconciler", func() {
	const (
		namespace = "default"
		timeout   = time.Second * 10
		interval  = time.Millisecond * 250
	)

	var (
		ctx context.Context
	)

	BeforeEach(func() {
		ctx = context.Background()
		testLogScraper.MockLogs = "" // reset mock logs before each test
	})

	Context("Reconciler Status Aggregation & Watch pipeline", func() {
		It("should initialize state machine, pass preflights and execute direct upgrade rollout", func() {
			clusterName := "test-cluster-1"
			cpNamespace := fmt.Sprintf("%s-%s", namespace, clusterName)
			hcuKey := types.NamespacedName{Name: clusterName, Namespace: namespace}

			// 1. Create target HostedCluster
			uHC := &unstructured.Unstructured{}
			uHC.SetGroupVersionKind(gvkHC)
			uHC.SetName(clusterName)
			uHC.SetNamespace(namespace)

			err := unstructured.SetNestedField(uHC.Object, "quay.io/ocp:4.20.44-x86_64@sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef", "spec", "release", "image")
			Expect(err).NotTo(HaveOccurred())
			err = unstructured.SetNestedField(uHC.Object, "quay.io/ocp:4.20.44-x86_64@sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef", "status", "releaseImage")
			Expect(err).NotTo(HaveOccurred())

			history := []interface{}{
				map[string]interface{}{
					"version": "4.20.44",
					"state":   "Completed",
				},
			}
			err = unstructured.SetNestedSlice(uHC.Object, history, "status", "version", "history")
			Expect(err).NotTo(HaveOccurred())

			Expect(k8sClient.Create(ctx, uHC)).To(Succeed())

			defer func() {
				_ = k8sClient.Delete(ctx, uHC)
			}()

			// 2. Create the HostedClusterUpgrade CR
			hcu := &upgradev1alpha1.HostedClusterUpgrade{
				ObjectMeta: metav1.ObjectMeta{
					Name:      clusterName,
					Namespace: namespace,
				},
				Spec: upgradev1alpha1.HostedClusterUpgradeSpec{
					TargetReleaseImage: "quay.io/ocp:4.20.50-x86_64@sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
					NodePools:          []upgradev1alpha1.NodePoolUpgradeSpec{},
					ControlPlane:       upgradev1alpha1.ControlPlaneUpgradeSpec{},
				},
			}

			Expect(k8sClient.Create(ctx, hcu)).To(Succeed())

			defer func() {
				_ = k8sClient.Delete(ctx, hcu)
			}()

			// Create a mock ControlPlaneComponent (CPC) in the control plane namespace
			cpcName := "etcd"
			uCPC := &unstructured.Unstructured{}
			uCPC.SetGroupVersionKind(gvkCPC)
			uCPC.SetName(cpcName)
			uCPC.SetNamespace(cpNamespace)

			err = unstructured.SetNestedField(uCPC.Object, "4.20.44", "status", "version")
			Expect(err).NotTo(HaveOccurred())

			conditions := []interface{}{
				map[string]interface{}{
					"type":    "Available",
					"status":  "True",
					"message": "Etcd is ready",
				},
				map[string]interface{}{
					"type":    "RolloutComplete",
					"status":  "True",
					"message": "Etcd rollout is complete",
				},
			}
			err = unstructured.SetNestedSlice(uCPC.Object, conditions, "status", "conditions")
			Expect(err).NotTo(HaveOccurred())

			// Ensure target namespace exists inside envtest
			ns := &unstructured.Unstructured{}
			ns.SetGroupVersionKind(schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Namespace"})
			ns.SetName(cpNamespace)
			_ = k8sClient.Create(ctx, ns)

			Expect(k8sClient.Create(ctx, uCPC)).To(Succeed())

			defer func() {
				_ = k8sClient.Delete(ctx, uCPC)
				_ = k8sClient.Delete(ctx, ns)
			}()

			// 3. Verify that the coordinator commands the HostedCluster to upgrade to target version (4.20.50)
			Eventually(func() string {
				fetchedHC := &unstructured.Unstructured{}
				fetchedHC.SetGroupVersionKind(gvkHC)
				if err := k8sClient.Get(ctx, hcuKey, fetchedHC); err != nil {
					return ""
				}
				val, _, _ := unstructured.NestedString(fetchedHC.Object, "spec", "release", "image")
				return val
			}, timeout, interval).Should(Equal("quay.io/ocp:4.20.50-x86_64@sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"))
		})

		It("should enter paused phase when spec.pausedUntil is true but still continuously aggregate status", func() {
			clusterName := "test-cluster-2"
			cpNamespace := fmt.Sprintf("%s-%s", namespace, clusterName)
			hcuKey := types.NamespacedName{Name: clusterName, Namespace: namespace}
			pausedVal := "true"

			// Create a paused HostedClusterUpgrade
			hcu := &upgradev1alpha1.HostedClusterUpgrade{
				ObjectMeta: metav1.ObjectMeta{
					Name:      clusterName,
					Namespace: namespace,
				},
				Spec: upgradev1alpha1.HostedClusterUpgradeSpec{
					TargetReleaseImage: "quay.io/ocp:4.22.2-x86_64@sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
					NodePools:          []upgradev1alpha1.NodePoolUpgradeSpec{},
					ControlPlane:       upgradev1alpha1.ControlPlaneUpgradeSpec{},
					PausedUntil:        &pausedVal,
				},
			}

			Expect(k8sClient.Create(ctx, hcu)).To(Succeed())

			defer func() {
				_ = k8sClient.Delete(ctx, hcu)
			}()

			// Verify phase transitions to Paused
			Eventually(func() upgradev1alpha1.UpgradePhase {
				fetched := &upgradev1alpha1.HostedClusterUpgrade{}
				if err := k8sClient.Get(ctx, hcuKey, fetched); err != nil {
					return ""
				}
				return fetched.Status.Phase
			}, timeout, interval).Should(Equal(upgradev1alpha1.PhasePaused))

			// Create a mock ControlPlaneComponent (CPC) in the control plane namespace
			cpcName := "kube-apiserver"
			uCPC := &unstructured.Unstructured{}
			uCPC.SetGroupVersionKind(gvkCPC)
			uCPC.SetName(cpcName)
			uCPC.SetNamespace(cpNamespace)

			err := unstructured.SetNestedField(uCPC.Object, "4.21.0", "status", "version")
			Expect(err).NotTo(HaveOccurred())

			// Ensure namespace exists
			ns := &unstructured.Unstructured{}
			ns.SetGroupVersionKind(schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Namespace"})
			ns.SetName(cpNamespace)
			_ = k8sClient.Create(ctx, ns)

			Expect(k8sClient.Create(ctx, uCPC)).To(Succeed())

			defer func() {
				_ = k8sClient.Delete(ctx, uCPC)
				_ = k8sClient.Delete(ctx, ns)
			}()

			// Verify status still aggregates CPC counts in real-time, even though the operator is paused!
			Eventually(func() int32 {
				fetched := &upgradev1alpha1.HostedClusterUpgrade{}
				if err := k8sClient.Get(ctx, hcuKey, fetched); err != nil {
					return -1
				}
				return fetched.Status.ControlPlaneStatus.TotalComponents
			}, timeout, interval).Should(Equal(int32(1)))

			Eventually(func() string {
				fetched := &upgradev1alpha1.HostedClusterUpgrade{}
				_ = k8sClient.Get(ctx, hcuKey, fetched)
				if len(fetched.Status.ControlPlaneStatus.Components) > 0 {
					return fetched.Status.ControlPlaneStatus.Components[0].Version
				}
				return ""
			}, timeout, interval).Should(Equal("4.21.0"))
		})

		It("should execute sequential NodePool rollouts with custom label priorities and extract HCCO stuck drains", func() {
			clusterName := "test-cluster-3"
			hcuKey := types.NamespacedName{Name: clusterName, Namespace: namespace}

			// Setup Mock HCCO Logs
			testLogScraper.MockLogs = "2026-08-12T12:00:00Z failed to drain node pool-b-node-1: cannot evict pod db-pod due to PDB db-pdb"

			// 1. Create target HostedCluster already at target version to bypass CP upgrade step
			uHC := &unstructured.Unstructured{}
			uHC.SetGroupVersionKind(gvkHC)
			uHC.SetName(clusterName)
			uHC.SetNamespace(namespace)

			err := unstructured.SetNestedField(uHC.Object, "quay.io/ocp:4.22.2-x86_64@sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef", "spec", "release", "image")
			Expect(err).NotTo(HaveOccurred())
			err = unstructured.SetNestedField(uHC.Object, "quay.io/ocp:4.22.2-x86_64@sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef", "status", "releaseImage")
			Expect(err).NotTo(HaveOccurred())

			history := []interface{}{
				map[string]interface{}{
					"version": "4.22.2",
					"state":   "Completed",
				},
			}
			err = unstructured.SetNestedSlice(uHC.Object, history, "status", "version", "history")
			Expect(err).NotTo(HaveOccurred())

			Expect(k8sClient.Create(ctx, uHC)).To(Succeed())

			defer func() {
				_ = k8sClient.Delete(ctx, uHC)
			}()

			// 2. Create three mock NodePools associated with this cluster
			// pool-a: priority 10, version 4.20.44, listed in spec
			npA := &unstructured.Unstructured{}
			npA.SetGroupVersionKind(gvkNP)
			npA.SetName("pool-a")
			npA.SetNamespace(namespace)
			npA.SetLabels(map[string]string{
				"upgrade.huc.0xshift.dev/order": "10",
			})
			_ = unstructured.SetNestedField(npA.Object, clusterName, "spec", "clusterName")
			_ = unstructured.SetNestedField(npA.Object, "quay.io/ocp:4.20.44-x86_64@sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef", "spec", "release", "image")
			_ = unstructured.SetNestedField(npA.Object, "true", "spec", "pausedUntil")
			_ = unstructured.SetNestedField(npA.Object, "quay.io/ocp:4.20.44-x86_64@sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef", "status", "version")

			Expect(k8sClient.Create(ctx, npA)).To(Succeed())

			defer func() {
				_ = k8sClient.Delete(ctx, npA)
			}()

			// pool-b: priority 50, version 4.20.44, listed in spec
			npB := &unstructured.Unstructured{}
			npB.SetGroupVersionKind(gvkNP)
			npB.SetName("pool-b")
			npB.SetNamespace(namespace)
			npB.SetLabels(map[string]string{
				"upgrade.huc.0xshift.dev/order": "50",
			})
			_ = unstructured.SetNestedField(npB.Object, clusterName, "spec", "clusterName")
			_ = unstructured.SetNestedField(npB.Object, "quay.io/ocp:4.20.44-x86_64@sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef", "spec", "release", "image")
			_ = unstructured.SetNestedField(npB.Object, "true", "spec", "pausedUntil")
			_ = unstructured.SetNestedField(npB.Object, "quay.io/ocp:4.20.44-x86_64@sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef", "status", "version")

			Expect(k8sClient.Create(ctx, npB)).To(Succeed())

			defer func() {
				_ = k8sClient.Delete(ctx, npB)
			}()

			// pool-c: priority 50, version 4.20.44, OMITTED from HCU spec (should still roll out sequential)
			npC := &unstructured.Unstructured{}
			npC.SetGroupVersionKind(gvkNP)
			npC.SetName("pool-c")
			npC.SetNamespace(namespace)
			npC.SetLabels(map[string]string{
				"upgrade.huc.0xshift.dev/order": "50",
			})
			_ = unstructured.SetNestedField(npC.Object, clusterName, "spec", "clusterName")
			_ = unstructured.SetNestedField(npC.Object, "quay.io/ocp:4.20.44-x86_64@sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef", "spec", "release", "image")
			_ = unstructured.SetNestedField(npC.Object, "true", "spec", "pausedUntil")
			_ = unstructured.SetNestedField(npC.Object, "quay.io/ocp:4.20.44-x86_64@sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef", "status", "version")

			Expect(k8sClient.Create(ctx, npC)).To(Succeed())

			defer func() {
				_ = k8sClient.Delete(ctx, npC)
			}()

			// 3. Create the HCU Upgrade Coordinator CR (concurrency = 2)
			hcu := &upgradev1alpha1.HostedClusterUpgrade{
				ObjectMeta: metav1.ObjectMeta{
					Name:      clusterName,
					Namespace: namespace,
				},
				Spec: upgradev1alpha1.HostedClusterUpgradeSpec{
					TargetReleaseImage: "quay.io/ocp:4.22.2-x86_64@sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
					ControlPlane:       upgradev1alpha1.ControlPlaneUpgradeSpec{},
					RolloutPolicy: &upgradev1alpha1.RolloutPolicySpec{
						MaxParallelNodePools: 2,
					},
					NodePools: []upgradev1alpha1.NodePoolUpgradeSpec{
						{
							Name: "pool-a",
						},
						{
							Name: "pool-b",
						},
					},
				},
			}

			Expect(k8sClient.Create(ctx, hcu)).To(Succeed())

			defer func() {
				_ = k8sClient.Delete(ctx, hcu)
			}()

			// 4. Verify that the priority 10 NodePool (pool-a) upgrades first!
			Eventually(func() string {
				fetchedNP := &unstructured.Unstructured{}
				fetchedNP.SetGroupVersionKind(gvkNP)
				_ = k8sClient.Get(ctx, types.NamespacedName{Name: "pool-a", Namespace: namespace}, fetchedNP)
				val, _, _ := unstructured.NestedString(fetchedNP.Object, "spec", "release", "image")
				return val
			}, timeout, interval).Should(Equal("quay.io/ocp:4.22.2-x86_64@sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"))

			Eventually(func() string {
				fetchedNP := &unstructured.Unstructured{}
				fetchedNP.SetGroupVersionKind(gvkNP)
				_ = k8sClient.Get(ctx, types.NamespacedName{Name: "pool-a", Namespace: namespace}, fetchedNP)
				pUntil, _, _ := unstructured.NestedString(fetchedNP.Object, "spec", "pausedUntil")
				return pUntil
			}, timeout, interval).Should(Equal("false"))

			// Verify that priority 50 NodePools (pool-b and pool-c) remain paused
			Consistently(func() string {
				fetchedNP := &unstructured.Unstructured{}
				fetchedNP.SetGroupVersionKind(gvkNP)
				_ = k8sClient.Get(ctx, types.NamespacedName{Name: "pool-b", Namespace: namespace}, fetchedNP)
				pUntil, _, _ := unstructured.NestedString(fetchedNP.Object, "spec", "pausedUntil")
				return pUntil
			}, time.Second*2, interval).Should(Equal("true"))

			// 5. Simulate pool-a completes its upgrade
			fetchedNPA := &unstructured.Unstructured{}
			fetchedNPA.SetGroupVersionKind(gvkNP)
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "pool-a", Namespace: namespace}, fetchedNPA)).To(Succeed())
			_ = unstructured.SetNestedField(fetchedNPA.Object, "quay.io/ocp:4.22.2-x86_64@sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef", "status", "version")
			Expect(k8sClient.Update(ctx, fetchedNPA)).To(Succeed())

			// 6. Verify that priority 50 group NodePools (pool-b and pool-c) both begin upgrading in parallel!
			Eventually(func() string {
				fetchedNP := &unstructured.Unstructured{}
				fetchedNP.SetGroupVersionKind(gvkNP)
				_ = k8sClient.Get(ctx, types.NamespacedName{Name: "pool-b", Namespace: namespace}, fetchedNP)
				val, _, _ := unstructured.NestedString(fetchedNP.Object, "spec", "release", "image")
				return val
			}, timeout, interval).Should(Equal("quay.io/ocp:4.22.2-x86_64@sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"))

			Eventually(func() string {
				fetchedNP := &unstructured.Unstructured{}
				fetchedNP.SetGroupVersionKind(gvkNP)
				_ = k8sClient.Get(ctx, types.NamespacedName{Name: "pool-c", Namespace: namespace}, fetchedNP)
				val, _, _ := unstructured.NestedString(fetchedNP.Object, "spec", "release", "image")
				return val
			}, timeout, interval).Should(Equal("quay.io/ocp:4.22.2-x86_64@sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"))

			// 7. Verify that HCCO log-scraping successfully parses the mock logs and populates the status blockers!
			Eventually(func() []string {
				fetched := &upgradev1alpha1.HostedClusterUpgrade{}
				_ = k8sClient.Get(ctx, hcuKey, fetched)
				for _, s := range fetched.Status.NodePoolStatuses {
					if s.Name == "pool-b" {
						return s.Blockers
					}
				}
				return nil
			}, timeout, interval).Should(ContainElement(ContainSubstring("cannot evict pod db-pod due to PDB db-pdb")))
		})
	})
})
