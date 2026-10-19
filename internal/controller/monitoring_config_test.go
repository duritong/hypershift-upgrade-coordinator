package controller

import (
	"os"
	"path/filepath"
	"testing"

	"sigs.k8s.io/yaml"
)

func TestScrapePreservesUpgradeNamespace(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "config", "prometheus", "monitor.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var monitor struct {
		Spec struct {
			Endpoints []struct {
				HonorLabels bool `json:"honorLabels"`
			} `json:"endpoints"`
		} `json:"spec"`
	}
	if err := yaml.Unmarshal(data, &monitor); err != nil {
		t.Fatal(err)
	}
	if len(monitor.Spec.Endpoints) == 0 {
		t.Fatal("missing metrics endpoint")
	}
	for _, endpoint := range monitor.Spec.Endpoints {
		if !endpoint.HonorLabels {
			t.Fatal("scrape target namespace would overwrite the upgrade namespace metric label")
		}
	}
}
