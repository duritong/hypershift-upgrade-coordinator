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
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

func TestGrafanaDashboard(t *testing.T) {
	type datasource struct {
		Type string `json:"type"`
		UID  string `json:"uid"`
	}
	type target struct {
		Expr       string     `json:"expr"`
		Instant    bool       `json:"instant"`
		Range      bool       `json:"range"`
		Format     string     `json:"format"`
		Datasource datasource `json:"datasource"`
	}
	type panel struct {
		ID      int      `json:"id"`
		Title   string   `json:"title"`
		Type    string   `json:"type"`
		Targets []target `json:"targets"`
	}
	var dashboard struct {
		Title      string  `json:"title"`
		Refresh    string  `json:"refresh"`
		Panels     []panel `json:"panels"`
		Templating struct {
			List []struct {
				Name       string     `json:"name"`
				Type       string     `json:"type"`
				Query      any        `json:"query"`
				Datasource datasource `json:"datasource"`
			} `json:"list"`
		} `json:"templating"`
	}
	data, err := os.ReadFile(filepath.Join("..", "..", "docs", "monitoring", "grafana-dashboard.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &dashboard); err != nil {
		t.Fatal(err)
	}
	const phase = `hypershift_upgrade_phase{current_version=~"$current_version", target_version=~"$target_version"}`
	// Exact, whitespace-insensitive contracts catch unfiltered OR branches and
	// invalid label selectors without adding a PromQL engine dependency.
	compact := func(s string) string { return strings.Join(strings.Fields(s), "") }
	contracts := []struct {
		id    int
		title string
		kind  string
		exprs []string
	}{
		{1, "Completed Upgrades", "stat", []string{`count(` + phase + ` == 6) or vector(0)`}},
		{2, "Total Active Upgrades", "stat", []string{`count((` + phase + ` >= 3) and (` + phase + ` <= 5)) or vector(0)`}},
		{3, "Fleet Upgrade Current State", "table", []string{phase}},
		{4, "Upgrade Durations (Seconds)", "bargauge", []string{
			`hypershift_upgrade_duration_seconds{current_version=~"$current_version", target_version=~"$target_version"} > 0`,
			`hypershift_upgrade_step_duration_seconds{step="ControlPlane", current_version=~"$current_version", target_version=~"$target_version"} > 0`,
		}},
		{5, "Control Plane Component Rollout Statuses", "table", []string{
			`hypershift_upgrade_controlplane_component_ready{component!=""} and on(namespace, name) ` + phase,
		}},
		{6, "NodePool Upgrade Rollout Ratio Curve", "timeseries", []string{
			`(hypershift_upgrade_nodepool_rollout_ratio and on(namespace, name) ` + phase + `) * 100`,
		}},
		{7, "Aggregated Cluster Node Rollout Curve", "timeseries", []string{
			`(hypershift_upgrade_aggregated_node_rollout_ratio and on(namespace, name) ` + phase + `) * 100`,
		}},
		{8, "Blocked NodePools", "table", []string{
			`hypershift_upgrade_nodepool_blocked{current_version=~"$current_version", target_version=~"$target_version"} == 1`,
		}},
		{9, "Observer Credential Availability", "table", []string{`hypershift_upgrade_guest_credential_available and on(namespace, name) ` + phase}},
		{10, "Guest Access Maintenance Ready", "table", []string{`hypershift_upgrade_guest_access_maintenance_ready and on(namespace, name) ` + phase}},
		{11, "Latest Guest Purpose Verdict", "table", []string{`hypershift_upgrade_guest_assessment{purpose=~"$guest_purpose"} and on(namespace, name) ` + phase}},
		{12, "Scoped Guest Read Age", "table", []string{`(time() - hypershift_upgrade_guest_last_successful_observation_timestamp_seconds) and on(namespace, name) ` + phase}},
		{13, "Observer Credential Remaining Lifetime", "table", []string{`(hypershift_upgrade_guest_credential_expiration_timestamp_seconds - time()) and on(namespace, name) ` + phase}},
		{14, "Guest Assessment Blocker Count", "table", []string{`hypershift_upgrade_guest_blocker_count and on(namespace, name) ` + phase}},
		{15, "Guest Operator Blockers", "table", []string{`hypershift_upgrade_guest_operator_blocker{purpose=~"$guest_purpose"} and on(namespace, name) ` + phase}},
		{16, "Assessed Guest Operator Count", "table", []string{`hypershift_upgrade_guest_operator_count and on(namespace, name) ` + phase}},
	}
	for _, contract := range contracts {
		t.Run(contract.title, func(t *testing.T) {
			var found *panel
			for i := range dashboard.Panels {
				if dashboard.Panels[i].ID == contract.id {
					if found != nil {
						t.Fatalf("duplicate panel ID %d", contract.id)
					}
					found = &dashboard.Panels[i]
				}
			}
			if found == nil {
				t.Fatalf("missing panel %d", contract.id)
			}
			if found.Title != contract.title || found.Type != contract.kind {
				t.Errorf("panel identity = %q (%s), want %q (%s)", found.Title, found.Type, contract.title, contract.kind)
			}
			if len(found.Targets) != len(contract.exprs) {
				t.Fatalf("got %d targets, want %d", len(found.Targets), len(contract.exprs))
			}
			for i, query := range found.Targets {
				if compact(query.Expr) != compact(contract.exprs[i]) {
					t.Errorf("target %d query = %s; want %s", i, query.Expr, contract.exprs[i])
				}
				wantRange := contract.kind == "timeseries"
				if query.Range != wantRange || query.Instant == wantRange {
					t.Errorf("target %d: range=%t instant=%t, want range=%t instant=%t", i, query.Range, query.Instant, wantRange, !wantRange)
				}
				if contract.kind == "table" && query.Format != "table" {
					t.Errorf("target %d: current-state tables need Prometheus table format, got %q", i, query.Format)
				}
			}
		})
	}
	if len(dashboard.Panels) != len(contracts) {
		t.Fatalf("unexpected dashboard panels: %d", len(dashboard.Panels))
	}
	t.Run("datasource and version variables", func(t *testing.T) {
		variables := make(map[string]bool)
		for _, variable := range dashboard.Templating.List {
			if variables[variable.Name] {
				t.Errorf("duplicate variable %q", variable.Name)
			}
			variables[variable.Name] = true
			if variable.Name == "datasource" {
				if variable.Type != "datasource" || variable.Query != "prometheus" {
					t.Errorf("datasource must be a Prometheus datasource variable: %+v", variable)
				}
				continue
			}
			if variable.Name == "current_version" || variable.Name == "target_version" {
				query, ok := variable.Query.(map[string]any)
				if variable.Type != "query" || !ok || query["query"] != "label_values(hypershift_upgrade_phase, "+variable.Name+")" {
					t.Errorf("invalid version variable: %+v", variable)
				}
				if variable.Datasource != (datasource{Type: "prometheus", UID: "${datasource}"}) {
					t.Errorf("version variable %s does not use selected datasource", variable.Name)
				}
			}
			if variable.Name == "guest_purpose" {
				query, ok := variable.Query.(map[string]any)
				if variable.Type != "query" || !ok || query["query"] != "label_values(hypershift_upgrade_guest_assessment, purpose)" || variable.Datasource != (datasource{Type: "prometheus", UID: "${datasource}"}) {
					t.Errorf("invalid guest purpose variable: %+v", variable)
				}
			}
		}
		for _, name := range []string{"datasource", "current_version", "target_version", "guest_purpose"} {
			if !variables[name] {
				t.Errorf("missing template variable %s", name)
			}
		}
		for _, p := range dashboard.Panels {
			for _, query := range p.Targets {
				if query.Datasource != (datasource{Type: "prometheus", UID: "${datasource}"}) {
					t.Errorf("panel %d does not use selected datasource", p.ID)
				}
			}
		}
	})
	t.Run("worker label contracts", func(t *testing.T) {
		// Worker ratios have no version labels. Filters must be applied to phase
		// and joined by BOTH identity labels, preserving each NodePool's value.
		selector := regexp.MustCompile(`(hypershift_upgrade_(?:nodepool_rollout_ratio|aggregated_node_rollout_ratio))(\{[^}]*\})?`)
		for _, p := range dashboard.Panels {
			for _, query := range p.Targets {
				for _, match := range selector.FindAllStringSubmatch(query.Expr, -1) {
					if strings.Contains(match[2], "version") {
						t.Errorf("%s filters worker metric on nonexistent version labels", query.Expr)
					}
				}
			}
		}
	})
	t.Run("refresh", func(t *testing.T) {
		refresh, err := time.ParseDuration(dashboard.Refresh)
		if err != nil || refresh < 5*time.Second || refresh > time.Minute {
			t.Errorf("dashboard needs a live refresh between 5s and 1m, got %q", dashboard.Refresh)
		}
	})
}
