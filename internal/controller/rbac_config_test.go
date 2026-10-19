package controller

import (
	"os"
	"path/filepath"
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"
	"sigs.k8s.io/yaml"
)

func TestObservedInfrastructureRBACIsReadOnly(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "config", "rbac", "role.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var role rbacv1.ClusterRole
	if err := yaml.Unmarshal(data, &role); err != nil {
		t.Fatal(err)
	}
	for _, rule := range role.Rules {
		for _, resource := range rule.Resources {
			if resource != "controlplanecomponents" && resource != "hostedcontrolplanes" && resource != "machinedeployments" && resource != "machinesets" {
				continue
			}
			for _, verb := range rule.Verbs {
				if verb != "get" && verb != "list" && verb != "watch" {
					t.Fatalf("observed %s has unnecessary %s permission", resource, verb)
				}
			}
		}
	}
}
