package controller

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"
)

// Static expression contracts only; this does not execute PromQL or alerts.
func TestGuestAlertRules(t *testing.T) {
	b, err := os.ReadFile(filepath.Join("..", "..", "config", "prometheus", "prometheus_rule.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var document struct {
		Spec struct {
			Groups []struct {
				Rules []struct {
					Alert string `json:"alert"`
					Expr  string `json:"expr"`
					For   string `json:"for"`
				} `json:"rules"`
			} `json:"groups"`
		} `json:"spec"`
	}
	if err := yaml.Unmarshal(b, &document); err != nil {
		t.Fatal(err)
	}
	const active = ` and on(namespace, name) (((hypershift_upgrade_phase >= 2) and (hypershift_upgrade_phase <= 5)) or (hypershift_upgrade_phase == 7))`
	want := map[string]string{
		"HypershiftUpgradeGuestCredentialUnavailable": `(hypershift_upgrade_guest_credential_available == 0)` + active,
		"HypershiftUpgradeGuestMaintenanceDegraded":   `(hypershift_upgrade_guest_access_maintenance_ready == 0)` + active,
		"HypershiftUpgradeGuestReadStale":             `(time() - hypershift_upgrade_guest_last_successful_observation_timestamp_seconds > 120)` + active,
		"HypershiftUpgradeGuestAssessmentBlocked":     `(hypershift_upgrade_guest_assessment{state=~"Unhealthy|Unavailable|AccessDenied|IdentityMismatch|UnsupportedEvidence|Stale|Invalidated|NotAssessed|Unknown"} == 1)` + active,
		"HypershiftUpgradeGuestVirtualizationBlocked": `(hypershift_upgrade_guest_operator_blocker{operator="openshift-virtualization"} == 1)` + active,
	}
	compact := func(s string) string { return strings.Join(strings.Fields(s), "") }
	for _, group := range document.Spec.Groups {
		for _, rule := range group.Rules {
			if !strings.HasPrefix(rule.Alert, "HypershiftUpgradeGuest") {
				continue
			}
			expected, ok := want[rule.Alert]
			if !ok {
				t.Fatalf("unexpected/duplicate guest alert %s", rule.Alert)
			}
			if compact(rule.Expr) != compact(expected) || rule.For == "" {
				t.Errorf("incorrect guest alert %s: %s", rule.Alert, rule.Expr)
			}
			delete(want, rule.Alert)
		}
	}
	if len(want) != 0 {
		t.Fatalf("missing guest rules: %v", want)
	}
}
