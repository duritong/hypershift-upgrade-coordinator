package v1alpha1

import (
	"testing"

	"sigs.k8s.io/yaml"
)

func TestHostedClusterUpgradeSpecYAMLPermutations(t *testing.T) {
	tests := []struct {
		name    string
		yamlDoc string
		wantErr bool
	}{
		{
			name: "minimal spec without controlPlane",
			yamlDoc: `
apiVersion: upgrade.0xshift.dev/v1alpha1
kind: HostedClusterUpgrade
metadata:
  name: test-cluster
  namespace: clusters
spec:
  targetReleaseImage: quay.io/ocp:4.22.0-x86_64@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
`,
			wantErr: false,
		},
		{
			name: "spec with empty controlPlane object",
			yamlDoc: `
apiVersion: upgrade.0xshift.dev/v1alpha1
kind: HostedClusterUpgrade
metadata:
  name: test-cluster
  namespace: clusters
spec:
  targetReleaseImage: quay.io/ocp:4.22.0-x86_64@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
  controlPlane: {}
`,
			wantErr: false,
		},
		{
			name: "spec with empty controlPlane key (null in YAML)",
			yamlDoc: `
apiVersion: upgrade.0xshift.dev/v1alpha1
kind: HostedClusterUpgrade
metadata:
  name: test-cluster
  namespace: clusters
spec:
  targetReleaseImage: quay.io/ocp:4.22.0-x86_64@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
  controlPlane:
`,
			wantErr: false,
		},
		{
			name: "spec with empty nodePoolGroups key (null in YAML)",
			yamlDoc: `
apiVersion: upgrade.0xshift.dev/v1alpha1
kind: HostedClusterUpgrade
metadata:
  name: test-cluster
  namespace: clusters
spec:
  targetReleaseImage: quay.io/ocp:4.22.0-x86_64@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
  controlPlane: {}
  nodePoolGroups:
`,
			wantErr: false,
		},
		{
			name: "spec with empty rolloutPolicy key (null in YAML)",
			yamlDoc: `
apiVersion: upgrade.0xshift.dev/v1alpha1
kind: HostedClusterUpgrade
metadata:
  name: test-cluster
  namespace: clusters
spec:
  targetReleaseImage: quay.io/ocp:4.22.0-x86_64@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
  controlPlane: {}
  rolloutPolicy:
`,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var hcu HostedClusterUpgrade
			err := yaml.Unmarshal([]byte(tt.yamlDoc), &hcu)
			if (err != nil) != tt.wantErr {
				t.Fatalf("yaml.Unmarshal() error = %v, wantErr %v", err, tt.wantErr)
			}
			if hcu.Spec.ControlPlane.StartAt != nil {
				t.Fatalf("expected nil StartAt, got %v", hcu.Spec.ControlPlane.StartAt)
			}
		})
	}
}
