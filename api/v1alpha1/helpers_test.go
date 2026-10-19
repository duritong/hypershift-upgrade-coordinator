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
	"strings"
	"testing"

	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestParseVersionFromPullspec(t *testing.T) {
	g := NewWithT(t)

	// Valid pullspec with arch suffix
	v, err := ParseVersionFromPullspec("quay.io/openshift-release-dev/ocp-release:4.20.44-x86_64@sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef")
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(v).To(Equal("4.20.44"))

	// Valid pullspec with alternate arch suffix
	vArch2, errArch2 := ParseVersionFromPullspec("registry.example.com/repo/ocp:4.14.0-amd64@sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef")
	g.Expect(errArch2).NotTo(HaveOccurred())
	g.Expect(vArch2).To(Equal("4.14.0"))

	// Architecture suffix is required.
	_, errNoDash := ParseVersionFromPullspec("registry.example.com/repo/ocp:4.14.0@sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef")
	g.Expect(errNoDash).To(HaveOccurred())

	// Valid pullspec with release candidate tag (Issue 2.3)
	vRC, errRC := ParseVersionFromPullspec("quay.io/openshift-release-dev/ocp-release:4.16.0-rc.1-x86_64@sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef")
	g.Expect(errRC).NotTo(HaveOccurred())
	g.Expect(vRC).To(Equal("4.16.0"))

	// Valid pullspec with nightly tag (Issue 2.3)
	vNightly, errNightly := ParseVersionFromPullspec("quay.io/openshift-release-dev/ocp-release:4.16.0-0.nightly-2024-10-12-044234@sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef")
	g.Expect(errNightly).NotTo(HaveOccurred())
	g.Expect(vNightly).To(Equal("4.16.0"))

	// Invalid pullspecs
	_, errInvalid := ParseVersionFromPullspec("invalid-pullspec")
	g.Expect(errInvalid).To(HaveOccurred())

	_, errNoDigest := ParseVersionFromPullspec("quay.io/ocp:4.14.0")
	g.Expect(errNoDigest).To(HaveOccurred())
}

func TestValidateUpgradePath(t *testing.T) {
	image := func(version string) string {
		return "quay.io/ocp:" + version + "-x86_64@sha256:" + strings.Repeat("a", 64)
	}
	for _, tt := range []struct {
		name, current, target, intermediate string
		allowEqual, wantErr                 bool
	}{
		{"patch", "4.20.0", image("4.20.1"), "", false, false},
		{"minor", "4.20.0", image("4.21.0"), "", false, false},
		{"current pullspec", image("4.20.0"), image("4.21.0"), "", false, false},
		{"eus", "4.20.0", image("4.22.0"), image("4.21.0"), false, false},
		{"patch intermediate", "4.20.0", image("4.21.0"), image("4.20.1"), false, false},
		{"intermediate reached", "4.21.0", image("4.22.0"), image("4.21.0"), false, false},
		{"intermediate passed", "4.21.5", image("4.22.0"), image("4.21.0"), false, false},
		{"older intermediate skipped", "4.21.0", image("4.22.0"), image("4.19.0"), false, false},
		{"admission equality", "4.22.0", image("4.22.0"), "", false, true},
		{"recovery equality", "4.22.0", image("4.22.0"), "", true, false},
		{"recovery intermediate skipped", "4.22.0", image("4.22.0"), image("4.21.0"), true, false},
		{"recovery intermediate equals target", "4.22.0", image("4.22.0"), image("4.22.0"), true, true},
		{"recovery intermediate above target", "4.22.0", image("4.22.0"), image("4.23.0"), true, true},
		{"recovery downgrade", "4.22.1", image("4.22.0"), "", true, true},
		{"admission downgrade", "4.22.1", image("4.22.0"), "", false, true},
		{"major jump", "4.22.0", image("5.0.0"), "", true, true},
		{"skipped major mismatch", "4.21.0", image("4.22.0"), image("3.21.0"), false, true},
		{"future major mismatch", "4.21.0", image("4.22.0"), image("5.0.0"), false, true},
		{"direct multi minor", "4.20.0", image("4.22.0"), "", false, true},
		{"first hop multi minor", "4.20.0", image("4.23.0"), image("4.22.0"), false, true},
		{"second hop multi minor", "4.20.0", image("4.23.0"), image("4.21.0"), false, true},
		{"skip multi minor", "4.21.0", image("4.23.0"), image("4.20.0"), false, true},
		{"intermediate equals target", "4.20.0", image("4.21.0"), image("4.21.0"), false, true},
		{"intermediate above target", "4.20.0", image("4.21.0"), image("4.21.1"), false, true},
		{"empty current", "", image("4.21.0"), "", true, true},
		{"invalid current", "invalid", image("4.21.0"), "", true, true},
		{"short current", "4.20", image("4.21.0"), "", true, true},
		{"invalid current image", "quay.io/ocp:4.20.0", image("4.21.0"), "", true, true},
		{"empty target", "4.20.0", "", "", true, true},
		{"bare target", "4.20.0", "4.21.0", "", true, true},
		{"invalid target", "4.20.0", image("04.21.0"), "", true, true},
		{"missing arch", "4.20.0", "quay.io/ocp:4.21.0@sha256:" + strings.Repeat("a", 64), "", true, true},
		{"invalid digest", "4.20.0", image("4.21.0") + "a", "", true, true},
		{"bare intermediate", "4.20.0", image("4.22.0"), "4.21.0", true, true},
		{"invalid skipped intermediate", "4.22.0", image("4.22.0"), image("04.21.0"), true, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateUpgradePath(tt.current, tt.target, tt.intermediate, tt.allowEqual)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ValidateUpgradePath() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestExtractVersion(t *testing.T) {
	g := NewWithT(t)

	// Empty string
	_, errEmpty := ExtractVersion("")
	g.Expect(errEmpty).To(HaveOccurred())

	// From pullspec
	v1, err1 := ExtractVersion("quay.io/openshift-release-dev/ocp-release:4.20.44-x86_64@sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef")
	g.Expect(err1).NotTo(HaveOccurred())
	g.Expect(v1).To(Equal("4.20.44"))

	// From raw semver string
	v2, err2 := ExtractVersion("4.13.0")
	g.Expect(err2).NotTo(HaveOccurred())
	g.Expect(v2).To(Equal("4.13.0"))

	// From semver string with arch suffix
	v3, err3 := ExtractVersion("4.13.0-x86_64")
	g.Expect(err3).NotTo(HaveOccurred())
	g.Expect(v3).To(Equal("4.13.0"))

	// From v-prefixed string
	v4, err4 := ExtractVersion("v4.14.2")
	g.Expect(err4).NotTo(HaveOccurred())
	g.Expect(v4).To(Equal("4.14.2"))

	// Invalid semver string
	_, errInv := ExtractVersion("not-a-version")
	g.Expect(errInv).To(HaveOccurred())
}

func TestIsUpstreamGreater(t *testing.T) {
	g := NewWithT(t)

	// Greater
	g.Expect(IsUpstreamGreater("4.20.44", "4.21.0")).To(BeTrue())
	g.Expect(IsUpstreamGreater("quay.io/ocp:4.20.44-x86_64@sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef", "4.21.0")).To(BeTrue())

	// Not greater
	g.Expect(IsUpstreamGreater("4.21.0", "4.20.44")).To(BeFalse())
	g.Expect(IsUpstreamGreater("4.21.0", "4.21.0")).To(BeFalse())

	// Invalid inputs
	g.Expect(IsUpstreamGreater("invalid", "4.21.0")).To(BeFalse())
	g.Expect(IsUpstreamGreater("4.20.0", "invalid")).To(BeFalse())
}

func TestGetMajorMinor(t *testing.T) {
	g := NewWithT(t)

	major, minor, err := GetMajorMinor("4.14.2")
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(major).To(Equal(4))
	g.Expect(minor).To(Equal(14))

	// From pullspec
	majorPS, minorPS, errPS := GetMajorMinor("quay.io/ocp:4.20.44-x86_64@sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef")
	g.Expect(errPS).NotTo(HaveOccurred())
	g.Expect(majorPS).To(Equal(4))
	g.Expect(minorPS).To(Equal(20))

	// Invalid inputs
	_, _, errInv := GetMajorMinor("invalid")
	g.Expect(errInv).To(HaveOccurred())

	_, _, errShort := GetMajorMinor("4")
	g.Expect(errShort).To(HaveOccurred())

	_, _, errBadMajor := GetMajorMinor("abc.12.0")
	g.Expect(errBadMajor).To(HaveOccurred())

	_, _, errBadMinor := GetMajorMinor("4.abc.0")
	g.Expect(errBadMinor).To(HaveOccurred())
}

func TestHostedClusterUpgradeDeepCopy(t *testing.T) {
	g := NewWithT(t)

	now := metav1.Now()
	paused := "true"
	hcu := &HostedClusterUpgrade{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
		Spec: HostedClusterUpgradeSpec{
			TargetReleaseImage:       "quay.io/ocp:4.14.0-x86_64@sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
			IntermediateReleaseImage: "quay.io/ocp:4.13.0-x86_64@sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
			PausedUntil:              &paused,
			ControlPlane: ControlPlaneUpgradeSpec{
				StartAt: &now,
			},
			NodePoolGroups: []NodePoolGroupSpec{
				{Name: "infra", StartAt: &now},
			},
			RolloutPolicy: &RolloutPolicySpec{
				MaxParallelNodePools: 2,
			},
		},
		Status: HostedClusterUpgradeStatus{
			Phase:          PhaseUpgradingControlPlane,
			StartTime:      &now,
			CompletionTime: &now,
			ControlPlaneStatus: ControlPlaneUpgradeStatus{
				ScheduledStart: &now,
				Components: []ControlPlaneComponentStatus{
					{Name: "etcd", Available: true},
				},
				Blockers: []string{"none"},
			},
			NodePoolStatuses: []NodePoolUpgradeStatus{
				{Name: "pool1", Phase: "Upgrading", ScheduledStart: &now, Blockers: []string{"drain"}},
			},
		},
	}

	cp := hcu.DeepCopy()
	g.Expect(cp).To(Equal(hcu))

	cpObj := hcu.DeepCopyObject()
	g.Expect(cpObj).NotTo(BeNil())

	list := &HostedClusterUpgradeList{
		Items: []HostedClusterUpgrade{*hcu},
	}
	listCp := list.DeepCopy()
	g.Expect(listCp).To(Equal(list))

	listObj := list.DeepCopyObject()
	g.Expect(listObj).NotTo(BeNil())
}
