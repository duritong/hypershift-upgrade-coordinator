package controller

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"
	"sigs.k8s.io/yaml"
)

func TestGuestAccessManagementRBAC(t *testing.T) {
	read := func(name string, object interface{}) {
		t.Helper()
		data, err := os.ReadFile(filepath.Join("..", "..", "config", "rbac", name))
		if err != nil {
			t.Fatal(err)
		}
		if err := yaml.Unmarshal(data, object); err != nil {
			t.Fatal(err)
		}
	}
	fleet := &rbacv1.ClusterRole{}
	read("role.yaml", fleet)
	secrets, services := false, false
	for _, rule := range fleet.Rules {
		for _, group := range rule.APIGroups {
			if group != "" {
				continue
			}
			for _, resource := range rule.Resources {
				if resource == "secrets" || resource == "services" {
					if !reflect.DeepEqual(rule.Verbs, []string{"get"}) {
						t.Fatalf("fleet %s exceeds get-only: %+v", resource, rule)
					}
					secrets = secrets || resource == "secrets"
					services = services || resource == "services"
				}
			}
		}
	}
	if !secrets || !services {
		t.Fatal("missing fleet get-only source/endpoint read permission")
	}
	local := &rbacv1.Role{}
	read("guest_access_role.yaml", local)
	if local.Kind != "Role" || local.Namespace != "system" || len(local.Rules) != 1 {
		t.Fatalf("not a single local Role: %+v", local)
	}
	want := rbacv1.PolicyRule{APIGroups: []string{""}, Resources: []string{"secrets"}, Verbs: []string{"get", "list", "watch", "create", "update", "patch", "delete"}}
	if !reflect.DeepEqual(local.Rules[0], want) {
		t.Fatalf("unexpected local permissions: %+v", local.Rules)
	}
	binding := &rbacv1.RoleBinding{}
	read("guest_access_role_binding.yaml", binding)
	if binding.Kind != "RoleBinding" || binding.Namespace != local.Namespace || binding.RoleRef.Kind != "Role" || binding.RoleRef.Name != local.Name || len(binding.Subjects) != 1 || binding.Subjects[0].Namespace != local.Namespace || binding.Subjects[0].Name != "controller-manager" {
		t.Fatalf("invalid local RoleBinding: %+v", binding)
	}
}
