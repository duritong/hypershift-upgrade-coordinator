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
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// Minimal structures for Grafana JSON parsing validation
type GrafanaTarget struct {
	Expr string `json:"expr"`
}

type GrafanaPanel struct {
	Title   string          `json:"title"`
	Type    string          `json:"type"`
	Targets []GrafanaTarget `json:"targets"`
}

type GrafanaVariable struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type GrafanaTemplating struct {
	List []GrafanaVariable `json:"list"`
}

type GrafanaDashboard struct {
	Title      string            `json:"title"`
	Panels     []GrafanaPanel    `json:"panels"`
	Templating GrafanaTemplating `json:"templating"`
}

var _ = Describe("Grafana Dashboard Validation", func() {
	It("should be a valid JSON file and contain all required SRE metrics and template variables", func() {
		dashboardPath := filepath.Join("..", "..", "docs", "monitoring", "grafana-dashboard.json")

		// Assert file exists
		_, err := os.Stat(dashboardPath)
		Expect(err).NotTo(HaveOccurred(), "Grafana dashboard JSON file should exist at docs/monitoring/grafana-dashboard.json")

		// Read and Parse JSON
		data, err := os.ReadFile(dashboardPath)
		Expect(err).NotTo(HaveOccurred(), "Failed to read Grafana dashboard JSON file")

		var dashboard GrafanaDashboard
		err = json.Unmarshal(data, &dashboard)
		Expect(err).NotTo(HaveOccurred(), "Grafana dashboard should be a valid JSON document")

		// Verify Dashboard Title
		Expect(dashboard.Title).To(ContainSubstring("HyperShift Upgrade Coordinator"))

		// Verify Variables (current_version, target_version)
		var hasCurrentVersion, hasTargetVersion bool
		for _, v := range dashboard.Templating.List {
			if v.Name == "current_version" && v.Type == "query" {
				hasCurrentVersion = true
			}
			if v.Name == "target_version" && v.Type == "query" {
				hasTargetVersion = true
			}
		}
		Expect(hasCurrentVersion).To(BeTrue(), "Dashboard must have 'current_version' query template variable")
		Expect(hasTargetVersion).To(BeTrue(), "Dashboard must have 'target_version' query template variable")

		// Verify that critical panels exist and reference HUC custom metrics
		requiredPanels := []string{
			"Fleet Upgrade State Summary",
			"Total Active Upgrades",
			"Active Upgrade Fleet Transition Matrix",
			"Active Upgrades Live Duration vs Step Duration",
			"Control Plane Component Rollout Statuses",
			"NodePool Upgrade Rollout Ratio Curve",
		}

		foundPanels := make(map[string]bool)
		var allExpressions []string

		for _, p := range dashboard.Panels {
			foundPanels[p.Title] = true
			for _, t := range p.Targets {
				allExpressions = append(allExpressions, t.Expr)
			}
		}

		for _, rp := range requiredPanels {
			Expect(foundPanels[rp]).To(BeTrue(), "Missing required panel: %s", rp)
		}

		// Verify references to our precise Prometheus metrics
		requiredMetrics := []string{
			"hypershift_upgrade_phase",
			"hypershift_upgrade_duration_seconds",
			"hypershift_upgrade_step_duration_seconds",
			"hypershift_upgrade_controlplane_component_ready",
			"hypershift_upgrade_nodepool_rollout_ratio",
		}

		combinedExprs := strings.Join(allExpressions, " ")
		for _, m := range requiredMetrics {
			Expect(combinedExprs).To(ContainSubstring(m), "Grafana queries must reference the custom metric: %s", m)
		}
	})
})
