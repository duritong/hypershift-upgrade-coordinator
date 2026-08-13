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
			v, err := ParseVersionFromPullspec(ps)
			Expect(err).NotTo(HaveOccurred())
			Expect(v).To(Equal("4.20.44"))
		})

		It("should fail to parse version if pullspec format is invalid", func() {
			invalidPS := "quay.io/openshift-release-dev/ocp-release:invalid-tag"
			_, err := ParseVersionFromPullspec(invalidPS)
			Expect(err).To(HaveOccurred())
		})

		It("should correctly assess monotonic version jumps", func() {
			Expect(IsUpstreamGreater("4.20.44", "4.21.0")).To(BeTrue())
			Expect(IsUpstreamGreater("4.21.5", "4.22.2")).To(BeTrue())
			Expect(IsUpstreamGreater("4.22.2", "4.21.5")).To(BeFalse())
			Expect(IsUpstreamGreater("4.22.2", "4.22.2")).To(BeFalse())
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
					NodePoolGroups:     []NodePoolGroupSpec{},
					ControlPlane:       ControlPlaneUpgradeSpec{},
				},
			}

			err := k8sClient.Create(context.Background(), hcu)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("does not exist"))
		})

		It("should validate version progression against existing HostedCluster", func() {
			clusterName := "my-cluster"

			hc := createMockHostedCluster(clusterName, nsName, "quay.io/ocp:4.20.44-x86_64@sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef")
			err := k8sClient.Create(context.Background(), hc)
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
					NodePoolGroups:     []NodePoolGroupSpec{},
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
					TargetReleaseImage:       "quay.io/ocp:4.22.2-x86_64@sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
					IntermediateReleaseImage: "quay.io/ocp:4.21.5-x86_64@sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
					NodePoolGroups:           []NodePoolGroupSpec{},
					ControlPlane:             ControlPlaneUpgradeSpec{},
				},
			}

			err = k8sClient.Create(context.Background(), hcuValid)
			Expect(err).NotTo(HaveOccurred())

			// Cleanup the upgrade CR
			_ = k8sClient.Delete(context.Background(), hcuValid)

			// Scenario 3: Direct 2-minor jump (4.20.44 -> 4.22.2) without intermediate - Should be rejected
			hcuDirectMultiMinor := &HostedClusterUpgrade{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: nsName,
					Name:      clusterName,
				},
				Spec: HostedClusterUpgradeSpec{
					TargetReleaseImage: "quay.io/ocp:4.22.2-x86_64@sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
					NodePoolGroups:     []NodePoolGroupSpec{},
					ControlPlane:       ControlPlaneUpgradeSpec{},
				},
			}

			err = k8sClient.Create(context.Background(), hcuDirectMultiMinor)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("unsupported direct multi-minor jump"))

			// Scenario 4: 3-minor jump with intermediate (4.20.44 -> 4.23.2 via 4.21.5) - Should be rejected because 4.21.5 -> 4.23.2 exceeds 1-minor jump
			hcuThreeMinor := &HostedClusterUpgrade{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: nsName,
					Name:      clusterName,
				},
				Spec: HostedClusterUpgradeSpec{
					TargetReleaseImage:       "quay.io/ocp:4.23.2-x86_64@sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
					IntermediateReleaseImage: "quay.io/ocp:4.21.5-x86_64@sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
					NodePoolGroups:           []NodePoolGroupSpec{},
					ControlPlane:             ControlPlaneUpgradeSpec{},
				},
			}

			err = k8sClient.Create(context.Background(), hcuThreeMinor)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("is more than 1 minor version ahead of intermediate version"))
		})

		It("should reject conflicting NodePoolGroup scheduled times based on ordered list position", func() {
			clusterName := "sched-cluster"

			// Create target HostedCluster
			hc := createMockHostedCluster(clusterName, nsName, "quay.io/ocp:4.20.44-x86_64@sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef")
			err := k8sClient.Create(context.Background(), hc)
			Expect(err).NotTo(HaveOccurred())

			defer func() {
				_ = k8sClient.Delete(context.Background(), hc)
			}()

			timeNow := metav1.Now()
			timeLater := metav1.NewTime(timeNow.Add(1 * time.Hour))

			hcuConflicting := &HostedClusterUpgrade{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: nsName,
					Name:      clusterName,
				},
				Spec: HostedClusterUpgradeSpec{
					TargetReleaseImage:       "quay.io/ocp:4.22.2-x86_64@sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
					IntermediateReleaseImage: "quay.io/ocp:4.21.5-x86_64@sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
					ControlPlane:             ControlPlaneUpgradeSpec{},
					NodePoolGroups: []NodePoolGroupSpec{
						{
							Name:    "infra",
							StartAt: &timeLater, // Scheduled later
						},
						{
							Name:    "compute",
							StartAt: &timeNow, // Scheduled earlier (violates ordered list sequence: compute comes AFTER infra, so compute.StartAt must be >= infra.StartAt)
						},
					},
				},
			}

			err = k8sClient.Create(context.Background(), hcuConflicting)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("temporal schedule inconsistency"))
		})

		It("should validate update and delete custom validator methods", func() {
			clusterName := "val-update-cluster"
			hc := createMockHostedCluster(clusterName, nsName, "quay.io/ocp:4.20.44-x86_64@sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef")
			err := k8sClient.Create(context.Background(), hc)
			Expect(err).NotTo(HaveOccurred())
			defer func() { _ = k8sClient.Delete(context.Background(), hc) }()

			val := &HostedClusterUpgradeCustomValidator{Client: k8sClient}

			// ValidateDelete should succeed
			warns, err := val.ValidateDelete(context.Background(), &HostedClusterUpgrade{})
			Expect(err).NotTo(HaveOccurred())
			Expect(warns).To(BeNil())

			// ValidateUpdate with valid HostedClusterUpgrade
			hcuVal := &HostedClusterUpgrade{
				ObjectMeta: metav1.ObjectMeta{Namespace: nsName, Name: clusterName},
				Spec: HostedClusterUpgradeSpec{
					TargetReleaseImage: "quay.io/ocp:4.21.0-x86_64@sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
					NodePoolGroups:     []NodePoolGroupSpec{},
					ControlPlane:       ControlPlaneUpgradeSpec{},
				},
			}
			warnsUp, errUp := val.ValidateUpdate(context.Background(), hcuVal, hcuVal)
			Expect(errUp).NotTo(HaveOccurred())
			Expect(warnsUp).To(BeNil())
		})

		It("should reject invalid major version jumps and invalid image formats", func() {
			clusterName := "major-jump-cluster"
			hc := createMockHostedCluster(clusterName, nsName, "quay.io/ocp:4.20.44-x86_64@sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef")
			err := k8sClient.Create(context.Background(), hc)
			Expect(err).NotTo(HaveOccurred())
			defer func() { _ = k8sClient.Delete(context.Background(), hc) }()

			// Major version jump (4.20.44 -> 5.0.0)
			hcuMajor := &HostedClusterUpgrade{
				ObjectMeta: metav1.ObjectMeta{Namespace: nsName, Name: clusterName},
				Spec: HostedClusterUpgradeSpec{
					TargetReleaseImage: "quay.io/ocp:5.0.0-x86_64@sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
					NodePoolGroups:     []NodePoolGroupSpec{},
					ControlPlane:       ControlPlaneUpgradeSpec{},
				},
			}
			err = k8sClient.Create(context.Background(), hcuMajor)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("cannot upgrade across different major versions"))

			// Invalid intermediate release image format
			hcuBadIntermediate := &HostedClusterUpgrade{
				ObjectMeta: metav1.ObjectMeta{Namespace: nsName, Name: clusterName},
				Spec: HostedClusterUpgradeSpec{
					TargetReleaseImage:       "quay.io/ocp:4.21.0-x86_64@sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
					IntermediateReleaseImage: "invalid-image-format",
					NodePoolGroups:           []NodePoolGroupSpec{},
					ControlPlane:             ControlPlaneUpgradeSpec{},
				},
			}
			err = k8sClient.Create(context.Background(), hcuBadIntermediate)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("invalid-image-format"))
		})
	})
})

func createMockHostedCluster(name, namespace, releaseImage string) *unstructured.Unstructured {
	hc := &unstructured.Unstructured{}
	hc.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "hypershift.openshift.io",
		Version: "v1beta1",
		Kind:    "HostedCluster",
	})
	hc.SetName(name)
	hc.SetNamespace(namespace)

	_ = unstructured.SetNestedField(hc.Object, releaseImage, "spec", "release", "image")
	_ = unstructured.SetNestedMap(hc.Object, map[string]interface{}{"type": "None"}, "spec", "platform")
	_ = unstructured.SetNestedMap(hc.Object, map[string]interface{}{"name": "pull-secret"}, "spec", "pullSecret")
	_ = unstructured.SetNestedMap(hc.Object, map[string]interface{}{"managementType": "Managed", "managed": map[string]interface{}{"storage": map[string]interface{}{"type": "PersistentVolume"}}}, "spec", "etcd")
	_ = unstructured.SetNestedMap(hc.Object, map[string]interface{}{
		"clusterNetwork": []interface{}{map[string]interface{}{"cidr": "10.128.0.0/14"}},
		"serviceNetwork": []interface{}{map[string]interface{}{"cidr": "172.30.0.0/16"}},
		"networkType":    "OVNKubernetes",
	}, "spec", "networking")

	services := []interface{}{
		map[string]interface{}{"service": "APIServer", "servicePublishingStrategy": map[string]interface{}{"type": "LoadBalancer"}},
		map[string]interface{}{"service": "OAuthServer", "servicePublishingStrategy": map[string]interface{}{"type": "Route"}},
		map[string]interface{}{"service": "Konnectivity", "servicePublishingStrategy": map[string]interface{}{"type": "Route"}},
		map[string]interface{}{"service": "Ignition", "servicePublishingStrategy": map[string]interface{}{"type": "Route"}},
	}
	_ = unstructured.SetNestedSlice(hc.Object, services, "spec", "services")

	return hc
}
