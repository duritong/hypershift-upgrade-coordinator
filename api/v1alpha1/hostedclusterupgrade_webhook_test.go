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
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type upgradePathTestClient struct {
	client.Client
	hc    *unstructured.Unstructured
	calls *int
}

func TestWebhookRejectsIndependentControlPlaneRelease(t *testing.T) {
	hc := createMockHostedCluster("cluster", "clusters", "4.20.0")
	_ = unstructured.SetNestedField(hc.Object, "independent", "spec", "controlPlaneRelease", "image")
	v := &HostedClusterUpgradeCustomValidator{Client: upgradePathTestClient{hc: hc}}
	hcu := &HostedClusterUpgrade{Spec: HostedClusterUpgradeSpec{TargetReleaseImage: "quay.io/ocp:4.21.0-x86_64@sha256:" + strings.Repeat("a", 64)}}
	if _, err := v.ValidateCreate(context.Background(), hcu); err == nil || !strings.Contains(err.Error(), "controlPlaneRelease") {
		t.Fatalf("create accepted override: %v", err)
	}
	if _, err := v.ValidateUpdate(context.Background(), hcu.DeepCopy(), hcu); err == nil || !strings.Contains(err.Error(), "controlPlaneRelease") {
		t.Fatalf("unchanged-image update accepted override: %v", err)
	}
}

func (c upgradePathTestClient) Get(_ context.Context, _ client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
	if c.calls != nil {
		*c.calls++
	}
	*obj.(*unstructured.Unstructured) = *c.hc.DeepCopy()
	return nil
}

func TestWebhookUpgradePath(t *testing.T) {
	image := func(version string) string {
		return "quay.io/ocp:" + version + "-x86_64@sha256:" + strings.Repeat("a", 64)
	}
	for _, tt := range []struct {
		name, current, target, intermediate string
		wantErr                             bool
	}{
		{"direct", "4.20.0", image("4.21.0"), "", false},
		{"eus", "4.20.0", image("4.22.0"), image("4.21.0"), false},
		{"skip intermediate", "4.21.5", image("4.22.0"), image("4.21.0"), false},
		{"equal target rejected", "4.21.0", image("4.21.0"), "", true},
		{"downgrade rejected", "4.21.0", image("4.20.0"), "", true},
		{"bare target rejected", "4.20.0", "4.21.0", "", true},
		{"bare intermediate rejected", "4.20.0", image("4.22.0"), "4.21.0", true},
		{"skipped cross major rejected", "4.21.0", image("4.22.0"), image("3.21.0"), true},
		{"missing arch rejected", "4.20.0", "quay.io/ocp:4.21.0@sha256:" + strings.Repeat("a", 64), "", true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			hc := createMockHostedCluster("cluster", "clusters", image(tt.current))
			validator := &HostedClusterUpgradeCustomValidator{Client: upgradePathTestClient{hc: hc}}
			hcu := &HostedClusterUpgrade{
				ObjectMeta: metav1.ObjectMeta{Name: hc.GetName(), Namespace: hc.GetNamespace()},
				Spec:       HostedClusterUpgradeSpec{TargetReleaseImage: tt.target, IntermediateReleaseImage: tt.intermediate},
			}
			_, err := validator.ValidateCreate(context.Background(), hcu)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ValidateCreate() error = %v, wantErr %v", err, tt.wantErr)
			}
			old := hcu.DeepCopy()
			old.Spec.TargetReleaseImage = image("4.19.0")
			_, err = validator.ValidateUpdate(context.Background(), old, hcu)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ValidateUpdate() error = %v, wantErr %v", err, tt.wantErr)
			}
			// Non-image updates must remain possible after reaching target.
			if tt.name == "equal target rejected" {
				old = hcu.DeepCopy()
				paused := "true"
				hcu.Spec.PausedUntil = &paused
				if _, err := validator.ValidateUpdate(context.Background(), old, hcu); err != nil {
					t.Fatalf("unchanged-image update rejected after reaching target: %v", err)
				}
			}
		})
	}
}

func TestWebhookLocalValidation(t *testing.T) {
	for _, tt := range []struct {
		name   string
		change func(*HostedClusterUpgradeSpec)
		field  string
	}{
		{"omitted", func(*HostedClusterUpgradeSpec) {}, ""},
		{"positive concurrency", func(s *HostedClusterUpgradeSpec) { s.RolloutPolicy = &RolloutPolicySpec{MaxParallelNodePools: 2} }, ""},
		{"zero concurrency", func(s *HostedClusterUpgradeSpec) { s.RolloutPolicy = &RolloutPolicySpec{} }, "maxParallelNodePools"},
		{"negative concurrency", func(s *HostedClusterUpgradeSpec) { s.RolloutPolicy = &RolloutPolicySpec{MaxParallelNodePools: -1} }, "maxParallelNodePools"},
		{"empty group", func(s *HostedClusterUpgradeSpec) { s.NodePoolGroups = []NodePoolGroupSpec{{Name: ""}} }, "nodePoolGroups"},
		{"whitespace group", func(s *HostedClusterUpgradeSpec) { s.NodePoolGroups = []NodePoolGroupSpec{{Name: " \t"}} }, "nodePoolGroups"},
		{"duplicate groups", func(s *HostedClusterUpgradeSpec) {
			s.NodePoolGroups = []NodePoolGroupSpec{{Name: "infra"}, {Name: "infra"}}
		}, "nodePoolGroups"},
		{"distinct groups", func(s *HostedClusterUpgradeSpec) {
			s.NodePoolGroups = []NodePoolGroupSpec{{Name: "infra"}, {Name: "default"}}
		}, ""},
		{"invalid pause", func(s *HostedClusterUpgradeSpec) { value := "tomorrow"; s.PausedUntil = &value }, "pausedUntil"},
		{"whitespace pause", func(s *HostedClusterUpgradeSpec) { value := " "; s.PausedUntil = &value }, "pausedUntil"},
		{"invalid timestamp", func(s *HostedClusterUpgradeSpec) { value := "2026-02-30T00:00:00Z"; s.PausedUntil = &value }, "pausedUntil"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			for _, withClient := range []bool{false, true} {
				calls := 0
				v := &HostedClusterUpgradeCustomValidator{}
				if withClient {
					v.Client = upgradePathTestClient{hc: createMockHostedCluster("cluster", "clusters", "quay.io/ocp:4.20.0-x86_64@sha256:"+strings.Repeat("a", 64)), calls: &calls}
				}
				old := &HostedClusterUpgrade{Spec: HostedClusterUpgradeSpec{TargetReleaseImage: "quay.io/ocp:4.21.0-x86_64@sha256:" + strings.Repeat("a", 64)}}
				obj := old.DeepCopy()
				tt.change(&obj.Spec)
				for _, operation := range []string{"create", "unchanged images", "changed images"} {
					calls = 0
					var err error
					if operation == "create" {
						_, err = v.ValidateCreate(context.Background(), obj)
					} else {
						previous := old.DeepCopy()
						if operation == "changed images" {
							previous.Spec.TargetReleaseImage = "previous-image"
						}
						_, err = v.ValidateUpdate(context.Background(), previous, obj)
					}
					if tt.field == "" && err != nil || tt.field != "" && (err == nil || !strings.Contains(err.Error(), tt.field)) {
						t.Errorf("%s (client=%t): error = %v, want field %q", operation, withClient, err, tt.field)
					}
					if tt.field != "" && calls != 0 {
						t.Errorf("%s: cluster lookup preceded local validation", operation)
					}
				}
			}
		})
	}
	for _, value := range []string{"", "true", "True", "TRUE", "false", "False", "FALSE", "2026-01-01T00:00:00Z", "2027-01-01T00:00:00.123+02:00"} {
		obj := &HostedClusterUpgrade{Spec: HostedClusterUpgradeSpec{PausedUntil: &value}}
		v := &HostedClusterUpgradeCustomValidator{}
		if _, err := v.ValidateCreate(context.Background(), obj); err != nil {
			t.Errorf("valid pause %q rejected: %v", value, err)
		}
	}
}

func TestAdmissionDigestOnlyCurrentRelease(t *testing.T) {
	hc := createMockHostedCluster("cluster", "clusters", "quay.io/ocp@sha256:"+strings.Repeat("a", 64))
	_ = unstructured.SetNestedSlice(hc.Object, []interface{}{map[string]interface{}{
		"state": "Completed", "version": "4.20.0", "image": "quay.io/ocp@sha256:" + strings.Repeat("a", 64),
	}}, "status", "version", "history")
	v := &HostedClusterUpgradeCustomValidator{Client: upgradePathTestClient{hc: hc}}
	hcu := &HostedClusterUpgrade{ObjectMeta: metav1.ObjectMeta{Name: "cluster", Namespace: "clusters"},
		Spec: HostedClusterUpgradeSpec{TargetReleaseImage: "quay.io/ocp:4.21.0-x86_64@sha256:" + strings.Repeat("b", 64)}}
	if _, err := v.ValidateCreate(context.Background(), hcu); err != nil {
		t.Fatalf("digest-only current image with explicit status version rejected: %v", err)
	}
}

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

		It("should default omitted concurrency but reject explicit invalid local fields on create and update", func() {
			ctx := context.Background()
			hc := createMockHostedCluster("local-fields", nsName, "quay.io/ocp:4.20.0-x86_64@sha256:"+strings.Repeat("a", 64))
			Expect(k8sClient.Create(ctx, hc)).To(Succeed())
			defer func() { _ = k8sClient.Delete(ctx, hc) }()
			obj := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": GroupVersion.String(), "kind": "HostedClusterUpgrade",
				"metadata": map[string]interface{}{"name": hc.GetName(), "namespace": nsName},
				"spec": map[string]interface{}{
					"targetReleaseImage": "quay.io/ocp:4.21.0-x86_64@sha256:" + strings.Repeat("a", 64),
					"controlPlane":       map[string]interface{}{}, "rolloutPolicy": map[string]interface{}{},
				},
			}}
			Expect(k8sClient.Create(ctx, obj)).To(Succeed())
			defer func() { _ = k8sClient.Delete(ctx, obj) }()
			parallel, found, err := unstructured.NestedInt64(obj.Object, "spec", "rolloutPolicy", "maxParallelNodePools")
			Expect(err).NotTo(HaveOccurred())
			Expect(found).To(BeTrue())
			Expect(parallel).To(Equal(int64(1)))
			for _, tt := range []struct {
				path  []string
				value interface{}
				field string
			}{
				{[]string{"spec", "rolloutPolicy", "maxParallelNodePools"}, int64(0), "maxParallelNodePools"},
				{[]string{"spec", "rolloutPolicy", "maxParallelNodePools"}, int64(-1), "maxParallelNodePools"},
				{[]string{"spec", "nodePoolGroups"}, []interface{}{map[string]interface{}{"name": ""}}, "name"},
				{[]string{"spec", "nodePoolGroups"}, []interface{}{map[string]interface{}{"name": " \t"}}, "nodePoolGroups"},
				{[]string{"spec", "nodePoolGroups"}, []interface{}{map[string]interface{}{"name": "infra"}, map[string]interface{}{"name": "infra"}}, "nodePoolGroups"},
				{[]string{"spec", "pausedUntil"}, "not-a-time", "pausedUntil"},
			} {
				candidate := obj.DeepCopy()
				Expect(unstructured.SetNestedField(candidate.Object, tt.value, tt.path...)).To(Succeed())
				err := k8sClient.Update(ctx, candidate)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring(tt.field))
				candidate.SetResourceVersion("")
				candidate.SetName("missing-local-fields")
				err = k8sClient.Create(ctx, candidate)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring(tt.field))
			}
		})

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

		It("Issue 2.1: should allow updating HostedClusterUpgrade when cluster has reached target version", func() {
			clusterName := "progressed-cluster"
			hc := createMockHostedCluster(clusterName, nsName, "quay.io/ocp:4.20.44-x86_64@sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef")
			err := k8sClient.Create(context.Background(), hc)
			Expect(err).NotTo(HaveOccurred())
			defer func() { _ = k8sClient.Delete(context.Background(), hc) }()

			hcu := &HostedClusterUpgrade{
				ObjectMeta: metav1.ObjectMeta{Namespace: nsName, Name: clusterName},
				Spec: HostedClusterUpgradeSpec{
					TargetReleaseImage: "quay.io/ocp:4.21.0-x86_64@sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
					NodePoolGroups:     []NodePoolGroupSpec{},
					ControlPlane:       ControlPlaneUpgradeSpec{},
				},
			}
			err = k8sClient.Create(context.Background(), hcu)
			Expect(err).NotTo(HaveOccurred())
			defer func() { _ = k8sClient.Delete(context.Background(), hcu) }()

			// Simulate cluster upgrade completion: HostedCluster now reports 4.21.0
			nowStr := time.Now().Format(time.RFC3339)
			history := []interface{}{
				map[string]interface{}{
					"image":          "quay.io/ocp:4.21.0-x86_64@sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
					"version":        "4.21.0",
					"state":          "Completed",
					"verified":       true,
					"startedTime":    nowStr,
					"completionTime": nowStr,
				},
			}
			desired := map[string]interface{}{
				"image":   "quay.io/ocp:4.21.0-x86_64@sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
				"version": "4.21.0",
			}
			_ = unstructured.SetNestedMap(hc.Object, desired, "status", "version", "desired")
			_ = unstructured.SetNestedSlice(hc.Object, history, "status", "version", "history")
			_ = unstructured.SetNestedSlice(hc.Object, []interface{}{}, "status", "version", "availableUpdates")
			_ = unstructured.SetNestedField(hc.Object, int64(1), "status", "version", "observedGeneration")
			Expect(k8sClient.Status().Update(context.Background(), hc)).To(Succeed())

			// Update HostedClusterUpgrade (e.g. setting pausedUntil: "true")
			// Webhook MUST NOT reject this with "target version '4.21.0' is not strictly greater than current version '4.21.0'"
			pausedVal := "true"
			hcu.Spec.PausedUntil = &pausedVal
			err = k8sClient.Update(context.Background(), hcu)
			Expect(err).NotTo(HaveOccurred())
		})

		It("Issue 2.2: should allow skip behavior when cluster is already at or past intermediate version", func() {
			clusterName := "skip-intermediate-cluster"
			// Cluster is already at 4.21.0
			hc := createMockHostedCluster(clusterName, nsName, "quay.io/ocp:4.21.0-x86_64@sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef")
			nowStr := time.Now().Format(time.RFC3339)
			history := []interface{}{
				map[string]interface{}{
					"image":          "quay.io/ocp:4.21.0-x86_64@sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
					"version":        "4.21.0",
					"state":          "Completed",
					"verified":       true,
					"startedTime":    nowStr,
					"completionTime": nowStr,
				},
			}
			desired := map[string]interface{}{
				"image":   "quay.io/ocp:4.21.0-x86_64@sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
				"version": "4.21.0",
			}
			_ = unstructured.SetNestedMap(hc.Object, desired, "status", "version", "desired")
			_ = unstructured.SetNestedSlice(hc.Object, history, "status", "version", "history")
			_ = unstructured.SetNestedSlice(hc.Object, []interface{}{}, "status", "version", "availableUpdates")
			_ = unstructured.SetNestedField(hc.Object, int64(1), "status", "version", "observedGeneration")
			err := k8sClient.Create(context.Background(), hc)
			Expect(err).NotTo(HaveOccurred())
			Expect(k8sClient.Status().Update(context.Background(), hc)).To(Succeed())
			Expect(err).NotTo(HaveOccurred())
			defer func() { _ = k8sClient.Delete(context.Background(), hc) }()

			// SRE applies CR specifying intermediate 4.21.0 and target 4.22.0
			// Webhook must allow this skip intermediate configuration without rejecting it!
			hcu := &HostedClusterUpgrade{
				ObjectMeta: metav1.ObjectMeta{Namespace: nsName, Name: clusterName},
				Spec: HostedClusterUpgradeSpec{
					TargetReleaseImage:       "quay.io/ocp:4.22.0-x86_64@sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
					IntermediateReleaseImage: "quay.io/ocp:4.21.0-x86_64@sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
					NodePoolGroups:           []NodePoolGroupSpec{},
					ControlPlane:             ControlPlaneUpgradeSpec{},
				},
			}
			err = k8sClient.Create(context.Background(), hcu)
			Expect(err).NotTo(HaveOccurred())
			defer func() { _ = k8sClient.Delete(context.Background(), hcu) }()
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
