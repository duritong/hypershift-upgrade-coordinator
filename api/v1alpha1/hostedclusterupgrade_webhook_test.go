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

package v1alpha1

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var _ = Describe("HostedClusterUpgrade Webhook validations", func() {
	Context("Unit tests for helper functions", func() {
		It("should correctly parse semantic version from a compliant release pullspec", func() {
			ps := "quay.io/openshift-release-dev/ocp-release:4.20.44-x86_64@sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
			v, err := parseVersionFromPullspec(ps)
			Expect(err).NotTo(HaveOccurred())
			Expect(v).To(Equal("4.20.44"))
		})

		It("should fail to parse version if pullspec format is invalid", func() {
			invalidPS := "quay.io/openshift-release-dev/ocp-release:invalid-tag"
			_, err := parseVersionFromPullspec(invalidPS)
			Expect(err).To(HaveOccurred())
		})

		It("should correctly assess monotonic version jumps", func() {
			Expect(isUpstreamGreater("4.20.44", "4.21.0")).To(BeTrue())
			Expect(isUpstreamGreater("4.21.5", "4.22.2")).To(BeTrue())
			Expect(isUpstreamGreater("4.22.2", "4.21.5")).To(BeFalse())
			Expect(isUpstreamGreater("4.22.2", "4.22.2")).To(BeFalse())
		})
	})

	Context("Integration tests with envtest API server", func() {
		var (
			nsName = "default"
		)

		It("should reject HostedClusterUpgrade CR if target HostedCluster does not exist", func() {
			hcu := &HostedClusterUpgrade{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: nsName,
					Name:      "non-existent-cluster",
				},
				Spec: HostedClusterUpgradeSpec{
					TargetReleaseImage: "quay.io/ocp:4.22.2-x86_64@sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
					NodePools:          []NodePoolUpgradeSpec{},
					ControlPlane:       ControlPlaneUpgradeSpec{},
				},
			}

			err := k8sClient.Create(context.Background(), hcu)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("does not exist"))
		})

		It("should validate version progression against existing HostedCluster", func() {
			clusterName := "my-cluster"

			// Create a mock HostedCluster using unstructured client to avoid API module imports
			hc := &unstructured.Unstructured{}
			hc.SetGroupVersionKind(schema.GroupVersionKind{
				Group:   "hypershift.openshift.io",
				Version: "v1beta1",
				Kind:    "HostedCluster",
			})
			hc.SetName(clusterName)
			hc.SetNamespace(nsName)

			// Set spec.release.image to current version 4.20.44
			err := unstructured.SetNestedField(hc.Object, "quay.io/ocp:4.20.44-x86_64@sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef", "spec", "release", "image")
			Expect(err).NotTo(HaveOccurred())

			err = k8sClient.Create(context.Background(), hc)
			Expect(err).NotTo(HaveOccurred())

			// Defer deletion of the mock HostedCluster
			defer func() {
				_ = k8sClient.Delete(context.Background(), hc)
			}()

			// Scenario 1: Attempt to downgrade (4.20.44 -> 4.20.40) - Should be rejected
			hcuDowngrade := &HostedClusterUpgrade{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: nsName,
					Name:      clusterName,
				},
				Spec: HostedClusterUpgradeSpec{
					TargetReleaseImage: "quay.io/ocp:4.20.40-x86_64@sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
					NodePools:          []NodePoolUpgradeSpec{},
					ControlPlane:       ControlPlaneUpgradeSpec{},
				},
			}

			err = k8sClient.Create(context.Background(), hcuDowngrade)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("not strictly greater"))

			// Scenario 2: Valid upgrade (4.20.44 -> 4.22.2 via intermediate 4.21.5) - Should pass
			hcuValid := &HostedClusterUpgrade{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: nsName,
					Name:      clusterName,
				},
				Spec: HostedClusterUpgradeSpec{
					TargetReleaseImage: "quay.io/ocp:4.22.2-x86_64@sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
					IntermediateReleaseImages: []string{
						"quay.io/ocp:4.21.5-x86_64@sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
					},
					NodePools:    []NodePoolUpgradeSpec{},
					ControlPlane: ControlPlaneUpgradeSpec{},
				},
			}

			err = k8sClient.Create(context.Background(), hcuValid)
			Expect(err).NotTo(HaveOccurred())

			// Cleanup the upgrade CR
			_ = k8sClient.Delete(context.Background(), hcuValid)
		})

		It("should reject conflicting NodePool scheduled times based on label priority", func() {
			clusterName := "sched-cluster"

			// Create target HostedCluster
			hc := &unstructured.Unstructured{}
			hc.SetGroupVersionKind(schema.GroupVersionKind{
				Group:   "hypershift.openshift.io",
				Version: "v1beta1",
				Kind:    "HostedCluster",
			})
			hc.SetName(clusterName)
			hc.SetNamespace(nsName)
			_ = unstructured.SetNestedField(hc.Object, "quay.io/ocp:4.20.44-x86_64@sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef", "spec", "release", "image")
			err := k8sClient.Create(context.Background(), hc)
			Expect(err).NotTo(HaveOccurred())

			defer func() {
				_ = k8sClient.Delete(context.Background(), hc)
			}()

			// Create NodePool A (order label: "10")
			npA := &unstructured.Unstructured{}
			npA.SetGroupVersionKind(schema.GroupVersionKind{
				Group:   "hypershift.openshift.io",
				Version: "v1beta1",
				Kind:    "NodePool",
			})
			npA.SetName("pool-a")
			npA.SetNamespace(nsName)
			npA.SetLabels(map[string]string{
				"upgrade.huc.0xshift.dev/order": "10",
			})
			err = k8sClient.Create(context.Background(), npA)
			Expect(err).NotTo(HaveOccurred())

			defer func() {
				_ = k8sClient.Delete(context.Background(), npA)
			}()

			// Create NodePool B (order label: "20")
			npB := &unstructured.Unstructured{}
			npB.SetGroupVersionKind(schema.GroupVersionKind{
				Group:   "hypershift.openshift.io",
				Version: "v1beta1",
				Kind:    "NodePool",
			})
			npB.SetName("pool-b")
			npB.SetNamespace(nsName)
			npB.SetLabels(map[string]string{
				"upgrade.huc.0xshift.dev/order": "20",
			})
			err = k8sClient.Create(context.Background(), npB)
			Expect(err).NotTo(HaveOccurred())

			defer func() {
				_ = k8sClient.Delete(context.Background(), npB)
			}()

			// Try to apply HCU where pool-b (order 20) is scheduled BEFORE pool-a (order 10)
			timeNow := metav1.Now()
			timeLater := metav1.NewTime(timeNow.Add(1 * time.Hour))

			hcuConflicting := &HostedClusterUpgrade{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: nsName,
					Name:      clusterName,
				},
				Spec: HostedClusterUpgradeSpec{
					TargetReleaseImage: "quay.io/ocp:4.22.2-x86_64@sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
					ControlPlane:       ControlPlaneUpgradeSpec{},
					NodePools: []NodePoolUpgradeSpec{
						{
							Name:    "pool-a",
							StartAt: &timeLater, // Scheduled later
						},
						{
							Name:    "pool-b",
							StartAt: &timeNow, // Scheduled earlier (violates order label: 20 should be >= 10)
						},
					},
				},
			}

			err = k8sClient.Create(context.Background(), hcuConflicting)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("temporal schedule inconsistency"))
		})
	})
})
