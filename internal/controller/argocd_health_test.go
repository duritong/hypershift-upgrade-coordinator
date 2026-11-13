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
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	upgradev1alpha1 "github.com/duritong/hypershift-upgrade-coordinator/api/v1alpha1"
	lua "github.com/yuin/gopher-lua"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"sigs.k8s.io/yaml"
)

type argoHealthResult struct {
	Status          string
	Message         string
	DeletionMessage string
}

type sampleExpectation struct {
	file            string
	wantPhase       upgradev1alpha1.UpgradePhase
	wantArgoStatus  string
	wantArgoMessage string
	wantDeletionMsg string
}

var allSampleExpectations = []sampleExpectation{
	{
		file:            "01_uninitialized_minimal.yaml",
		wantPhase:       "",
		wantArgoStatus:  "Progressing",
		wantArgoMessage: "Initializing upgrade coordinator...",
	},
	{
		file:            "02_guest_access_bootstrap_only.yaml",
		wantPhase:       "",
		wantArgoStatus:  "Progressing",
		wantArgoMessage: "Initializing upgrade coordinator...",
	},
	{
		file:            "03_idle.yaml",
		wantPhase:       upgradev1alpha1.PhaseIdle,
		wantArgoStatus:  "Progressing",
		wantArgoMessage: "Beginning Preflight Checks.",
	},
	{
		file:            "04_installing.yaml",
		wantPhase:       upgradev1alpha1.PhaseInstalling,
		wantArgoStatus:  "Progressing",
		wantArgoMessage: "HostedCluster is currently in initial installation. Waiting for completion.",
	},
	{
		file:            "05_preflight_checks.yaml",
		wantPhase:       upgradev1alpha1.PhasePreflightChecks,
		wantArgoStatus:  "Progressing",
		wantArgoMessage: "guest SourceBaseline: Converging (ClusterVersionProgressing)",
	},
	{
		file:            "06_pending_schedule.yaml",
		wantPhase:       upgradev1alpha1.PhasePendingSchedule,
		wantArgoStatus:  "Progressing",
		wantArgoMessage: "Preflight checks passed. Control Plane upgrade is scheduled to start at 2026-08-15T02:00:00Z (in 2h0m0s).",
	},
	{
		file:            "07_upgrading_control_plane_intermediate.yaml",
		wantPhase:       upgradev1alpha1.PhaseUpgradingControlPlane,
		wantArgoStatus:  "Progressing",
		wantArgoMessage: "Initiating Intermediate (4.21.42) Control Plane rollout.",
	},
	{
		file:            "08_verifying_control_plane.yaml",
		wantPhase:       upgradev1alpha1.PhaseVerifyingControlPlane,
		wantArgoStatus:  "Progressing",
		wantArgoMessage: "waiting for latest controlPlaneVersion Completed history with exact expected payload and version",
	},
	{
		file:            "09_upgrading_nodepools_with_drains.yaml",
		wantPhase:       upgradev1alpha1.PhaseUpgradingNodePools,
		wantArgoStatus:  "Progressing",
		wantArgoMessage: "Upgrading NodePool group 'compute' (1/2 pools completed); waiting on scheduled start for remaining pool.",
	},
	{
		file:            "10_paused_timed_with_phase_before_pause.yaml",
		wantPhase:       upgradev1alpha1.PhasePaused,
		wantArgoStatus:  "Suspended",
		wantArgoMessage: "Upgrade coordinator is paused via spec.pausedUntil.",
	},
	{
		file:            "11_blocked_guest_and_nodepool.yaml",
		wantPhase:       upgradev1alpha1.PhaseBlocked,
		wantArgoStatus:  "Degraded",
		wantArgoMessage: "guest WorkerGuard: Unhealthy (VirtualizationOutdatedWorkloads)",
	},
	{
		file:            "12_completed.yaml",
		wantPhase:       upgradev1alpha1.PhaseCompleted,
		wantArgoStatus:  "Healthy",
		wantArgoMessage: "All worker NodePools successfully upgraded and verified on target version.",
	},
	{
		file:            "13_day2_spec_bump_unobserved.yaml",
		wantPhase:       upgradev1alpha1.PhaseCompleted,
		wantArgoStatus:  "Progressing",
		wantArgoMessage: "Waiting for upgrade spec update to be observed",
	},
	{
		file:            "14_terminating_with_finalizer.yaml",
		wantPhase:       upgradev1alpha1.PhaseCompleted,
		wantArgoStatus:  "Progressing",
		wantArgoMessage: "Deleting HostedClusterUpgrade: restoring coordinator-owned NodePool pause locks (upgrade.huc.0xshift.dev/worker-locks)",
		wantDeletionMsg: "Deleting HostedClusterUpgrade: restoring coordinator-owned NodePool pause locks (upgrade.huc.0xshift.dev/worker-locks)",
	},
}

func extractGitOpsHealthLua(t *testing.T) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", "docs", "gitops.md"))
	if err != nil {
		t.Fatalf("read docs/gitops.md: %v", err)
	}
	blocks := regexp.MustCompile("(?s)```yaml\\n(.*?)\\n```").FindAllSubmatch(data, -1)
	if len(blocks) < 2 {
		t.Fatalf("expected YAML blocks in docs/gitops.md, found %d", len(blocks))
	}

	var operatorScript, cmScript string
	for _, block := range blocks {
		var raw map[string]any
		if err := yaml.Unmarshal(block[1], &raw); err != nil {
			continue
		}
		kind, _ := raw["kind"].(string)
		switch kind {
		case "ArgoCD":
			spec, _ := raw["spec"].(map[string]any)
			if _, deprecated := spec["resourceCustomizations"]; deprecated {
				t.Fatal("ArgoCD example in docs/gitops.md must not use removed spec.resourceCustomizations")
			}
			var cr struct {
				APIVersion string `json:"apiVersion"`
				Kind       string `json:"kind"`
				Spec       struct {
					ResourceHealthChecks []struct {
						Group string `json:"group"`
						Kind  string `json:"kind"`
						Check string `json:"check"`
					} `json:"resourceHealthChecks"`
				} `json:"spec"`
			}
			if err := yaml.Unmarshal(block[1], &cr); err != nil {
				t.Fatalf("unmarshal ArgoCD CR example: %v", err)
			}
			if cr.APIVersion != "argoproj.io/v1beta1" {
				t.Fatalf("ArgoCD apiVersion = %q, want argoproj.io/v1beta1", cr.APIVersion)
			}
			for _, hc := range cr.Spec.ResourceHealthChecks {
				if hc.Group == "upgrade.0xshift.dev" && hc.Kind == "HostedClusterUpgrade" {
					operatorScript = strings.TrimSpace(hc.Check)
				}
			}
		case "ConfigMap":
			var cm struct {
				Metadata struct {
					Name string `json:"name"`
				} `json:"metadata"`
				Data map[string]string `json:"data"`
			}
			if err := yaml.Unmarshal(block[1], &cm); err != nil {
				t.Fatalf("unmarshal ConfigMap example: %v", err)
			}
			if cm.Metadata.Name == "argocd-cm" {
				if _, legacy := cm.Data["resource.customizations"]; legacy {
					t.Fatal("argocd-cm example in docs/gitops.md must not use legacy monolithic resource.customizations")
				}
				cmScript = strings.TrimSpace(cm.Data["resource.customizations.health.upgrade.0xshift.dev_HostedClusterUpgrade"])
			}
		}
	}

	if operatorScript == "" {
		t.Fatal("missing ArgoCD spec.resourceHealthChecks script for HostedClusterUpgrade in docs/gitops.md")
	}
	if cmScript == "" {
		t.Fatal("missing argocd-cm resource.customizations.health.upgrade.0xshift.dev_HostedClusterUpgrade script in docs/gitops.md")
	}
	if operatorScript != cmScript {
		t.Fatal("ArgoCD CR and argocd-cm Lua health check scripts in docs/gitops.md differ")
	}
	return operatorScript
}

// evalArgoHealthLua executes a custom Lua health check using Argo CD's default
// sandbox configuration (useOpenLibs=false: only package, base, and table libs).
func evalArgoHealthLua(t *testing.T, script string, hcu *upgradev1alpha1.HostedClusterUpgrade) argoHealthResult {
	t.Helper()
	rawJSON, err := json.Marshal(hcu)
	if err != nil {
		t.Fatalf("marshal HostedClusterUpgrade: %v", err)
	}
	var unstructuredMap map[string]any
	if err := json.Unmarshal(rawJSON, &unstructuredMap); err != nil {
		t.Fatalf("unmarshal HostedClusterUpgrade to map: %v", err)
	}

	l := lua.NewState(lua.Options{SkipOpenLibs: true})
	defer l.Close()

	for _, lib := range []struct {
		name string
		fn   lua.LGFunction
	}{
		{lua.LoadLibName, lua.OpenPackage},
		{lua.BaseLibName, lua.OpenBase},
		{lua.TabLibName, lua.OpenTable},
	} {
		if err := l.CallByParam(lua.P{
			Fn:      l.NewFunction(lib.fn),
			NRet:    0,
			Protect: true,
		}, lua.LString(lib.name)); err != nil {
			t.Fatalf("open lua lib %s: %v", lib.name, err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	l.SetContext(ctx)
	l.SetGlobal("obj", goValueToLua(l, unstructuredMap))

	if err := l.DoString(script); err != nil {
		t.Fatalf("execute Argo CD Lua health script: %v", err)
	}

	ret := l.Get(-1)
	tbl, ok := ret.(*lua.LTable)
	if !ok {
		t.Fatalf("Lua health script returned %s, want table", ret.Type())
	}

	luaField := func(key string) string {
		val := l.GetField(tbl, key)
		if s, ok := val.(lua.LString); ok {
			return string(s)
		}
		if val == lua.LNil {
			return ""
		}
		t.Fatalf("health field %q has non-string Lua type %s", key, val.Type())
		return ""
	}

	res := argoHealthResult{
		Status:          luaField("status"),
		Message:         luaField("message"),
		DeletionMessage: luaField("deletionMessage"),
	}
	switch res.Status {
	case "Healthy", "Progressing", "Degraded", "Suspended":
	default:
		t.Fatalf("invalid Argo CD health status %q", res.Status)
	}
	return res
}

func goValueToLua(l *lua.LState, v any) lua.LValue {
	switch val := v.(type) {
	case nil:
		return lua.LNil
	case bool:
		return lua.LBool(val)
	case float64:
		return lua.LNumber(val)
	case string:
		return lua.LString(val)
	case []any:
		tbl := l.CreateTable(len(val), 0)
		for i, item := range val {
			tbl.RawSetInt(i+1, goValueToLua(l, item))
		}
		return tbl
	case map[string]any:
		tbl := l.CreateTable(0, len(val))
		for k, item := range val {
			tbl.RawSetString(k, goValueToLua(l, item))
		}
		return tbl
	default:
		return lua.LNil
	}
}

func loadHCUCRDSchema(t *testing.T) *apiextensionsv1.JSONSchemaProps {
	t.Helper()
	crdPath := filepath.Join("..", "..", "config", "crd", "bases", "upgrade.0xshift.dev_hostedclusterupgrades.yaml")
	data, err := os.ReadFile(crdPath)
	if err != nil {
		t.Fatalf("read CRD %s: %v", crdPath, err)
	}
	var crd apiextensionsv1.CustomResourceDefinition
	if err := yaml.Unmarshal(data, &crd); err != nil {
		t.Fatalf("unmarshal CRD: %v", err)
	}
	for i := range crd.Spec.Versions {
		if crd.Spec.Versions[i].Name == "v1alpha1" && crd.Spec.Versions[i].Schema != nil {
			return crd.Spec.Versions[i].Schema.OpenAPIV3Schema
		}
	}
	t.Fatal("v1alpha1 OpenAPIV3Schema not found in CRD")
	return nil
}

func validateAgainstJSONSchema(path string, val any, schema *apiextensionsv1.JSONSchemaProps) error {
	if schema == nil {
		return nil
	}
	if val == nil {
		if schema.Nullable {
			return nil
		}
		return fmt.Errorf("%s: null is not allowed", path)
	}

	if len(schema.Enum) > 0 {
		rawVal, err := json.Marshal(val)
		if err != nil {
			return fmt.Errorf("%s: marshal enum value: %w", path, err)
		}
		matched := false
		for _, allowed := range schema.Enum {
			if string(rawVal) == string(allowed.Raw) {
				matched = true
				break
			}
		}
		if !matched {
			return fmt.Errorf("%s: value %s not in allowed enum", path, string(rawVal))
		}
	}

	switch schema.Type {
	case "object":
		m, ok := val.(map[string]any)
		if !ok {
			return fmt.Errorf("%s: expected object, got %T", path, val)
		}
		for _, req := range schema.Required {
			if v, exists := m[req]; !exists || v == nil {
				return fmt.Errorf("%s: missing required field %q", path, req)
			}
		}
		preserveUnknown := schema.XPreserveUnknownFields != nil && *schema.XPreserveUnknownFields
		if len(schema.Properties) > 0 || !preserveUnknown {
			for k, child := range m {
				propSchema, declared := schema.Properties[k]
				if !declared {
					if preserveUnknown || schema.AdditionalProperties != nil {
						continue
					}
					return fmt.Errorf("%s: unknown field %q not in CRD schema", path, k)
				}
				if err := validateAgainstJSONSchema(path+"."+k, child, &propSchema); err != nil {
					return err
				}
			}
		}
	case "array":
		arr, ok := val.([]any)
		if !ok {
			return fmt.Errorf("%s: expected array, got %T", path, val)
		}
		if schema.MaxItems != nil && int64(len(arr)) > *schema.MaxItems {
			return fmt.Errorf("%s: length %d exceeds maxItems %d", path, len(arr), *schema.MaxItems)
		}
		if schema.Items != nil && schema.Items.Schema != nil {
			for i, elem := range arr {
				if err := validateAgainstJSONSchema(fmt.Sprintf("%s[%d]", path, i), elem, schema.Items.Schema); err != nil {
					return err
				}
			}
		}
	case "string":
		s, ok := val.(string)
		if !ok {
			return fmt.Errorf("%s: expected string, got %T", path, val)
		}
		runeLen := int64(utf8.RuneCountInString(s))
		if schema.MinLength != nil && runeLen < *schema.MinLength {
			return fmt.Errorf("%s: string length %d < minLength %d", path, runeLen, *schema.MinLength)
		}
		if schema.MaxLength != nil && runeLen > *schema.MaxLength {
			return fmt.Errorf("%s: string length %d > maxLength %d", path, runeLen, *schema.MaxLength)
		}
		if schema.Pattern != "" {
			re, err := regexp.Compile(schema.Pattern)
			if err != nil {
				return fmt.Errorf("%s: invalid CRD pattern %q: %w", path, schema.Pattern, err)
			}
			if !re.MatchString(s) {
				return fmt.Errorf("%s: value %q does not match pattern %q", path, s, schema.Pattern)
			}
		}
		if schema.Format == "date-time" {
			if _, err := time.Parse(time.RFC3339, s); err != nil {
				if _, errNano := time.Parse(time.RFC3339Nano, s); errNano != nil {
					return fmt.Errorf("%s: invalid date-time %q: %w", path, s, err)
				}
			}
		}
	case "integer":
		num, ok := val.(float64)
		if !ok || num != float64(int64(num)) {
			return fmt.Errorf("%s: expected integer, got %v (%T)", path, val, val)
		}
		if schema.Minimum != nil && num < *schema.Minimum {
			return fmt.Errorf("%s: %v < minimum %v", path, num, *schema.Minimum)
		}
		if schema.Maximum != nil && num > *schema.Maximum {
			return fmt.Errorf("%s: %v > maximum %v", path, num, *schema.Maximum)
		}
	case "boolean":
		if _, ok := val.(bool); !ok {
			return fmt.Errorf("%s: expected boolean, got %T", path, val)
		}
	}
	return nil
}

func validateAnonymizedSample(t *testing.T, filename string, rawBytes []byte, hcu *upgradev1alpha1.HostedClusterUpgrade) {
	t.Helper()
	allowedGroups := map[string]bool{"infra": true, "compute": true, "default": true}

	if hcu.Name != "example-cluster-01" || hcu.Namespace != "clusters" {
		t.Fatalf("%s: sample must use anonymized clusters/example-cluster-01 identity, got %s/%s", filename, hcu.Namespace, hcu.Name)
	}
	checkImage := func(field, img string) {
		if img == "" {
			return
		}
		if !strings.HasPrefix(img, "registry.example.com/") {
			t.Fatalf("%s: %s %q must use registry.example.com", filename, field, img)
		}
		if _, err := upgradev1alpha1.ParseVersionFromPullspec(img); err != nil {
			t.Fatalf("%s: %s %q failed ParseVersionFromPullspec: %v", filename, field, img, err)
		}
	}
	checkImage("spec.targetReleaseImage", hcu.Spec.TargetReleaseImage)
	checkImage("spec.intermediateReleaseImage", hcu.Spec.IntermediateReleaseImage)
	checkImage("status.controlPlaneStatus.targetReleaseImage", hcu.Status.ControlPlaneStatus.TargetReleaseImage)

	for _, grp := range hcu.Spec.NodePoolGroups {
		if !allowedGroups[grp.Name] {
			t.Fatalf("%s: non-generic nodePoolGroup name %q", filename, grp.Name)
		}
	}
	for _, np := range hcu.Status.NodePoolStatuses {
		if !allowedGroups[np.Group] {
			t.Fatalf("%s: non-generic nodePoolStatus group %q", filename, np.Group)
		}
		if !strings.HasPrefix(np.Name, "workers-") {
			t.Fatalf("%s: nodePoolStatus name %q must use generic workers-* prefix", filename, np.Name)
		}
		checkImage("status.nodePoolStatuses.targetReleaseImage", np.TargetReleaseImage)
	}

	allowedDomains := map[string]bool{
		"registry.example.com":    true,
		"upgrade.0xshift.dev":     true,
		"upgrade.huc.0xshift.dev": true,
		"kubectl.kubernetes.io":   true,
	}
	domainPrefix := regexp.MustCompile(`([a-z0-9-]+(?:\.[a-z0-9-]+)+)/`)
	for _, match := range domainPrefix.FindAllStringSubmatch(strings.ToLower(string(rawBytes)), -1) {
		if !allowedDomains[match[1]] {
			t.Fatalf("%s: contains non-anonymized domain %q", filename, match[1])
		}
	}
}

func TestHostedClusterUpgradeSamples(t *testing.T) {
	crdSchema := loadHCUCRDSchema(t)
	specSchema := crdSchema.Properties["spec"]
	statusSchema := crdSchema.Properties["status"]
	luaScript := extractGitOpsHealthLua(t)

	sampleFiles, err := filepath.Glob(filepath.Join("testdata", "samples", "*.yaml"))
	if err != nil {
		t.Fatalf("glob sample manifests: %v", err)
	}
	if len(sampleFiles) != len(allSampleExpectations) {
		t.Fatalf("found %d files in testdata/samples, expected %d", len(sampleFiles), len(allSampleExpectations))
	}
	onDisk := make(map[string]bool, len(sampleFiles))
	for _, p := range sampleFiles {
		onDisk[filepath.Base(p)] = true
	}

	for _, exp := range allSampleExpectations {
		if !onDisk[exp.file] {
			t.Fatalf("expected sample file %s missing from testdata/samples", exp.file)
		}
		t.Run(exp.file, func(t *testing.T) {
			path := filepath.Join("testdata", "samples", exp.file)
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read %s: %v", path, err)
			}

			// 1. Strict Go struct deserialization (rejects unknown/misspelled fields).
			var hcu upgradev1alpha1.HostedClusterUpgrade
			if err := yaml.UnmarshalStrict(data, &hcu); err != nil {
				t.Fatalf("yaml.UnmarshalStrict failed: %v", err)
			}
			if hcu.APIVersion != "upgrade.0xshift.dev/v1alpha1" || hcu.Kind != "HostedClusterUpgrade" {
				t.Fatalf("unexpected GVK: %s %s", hcu.APIVersion, hcu.Kind)
			}
			if hcu.Status.Phase != exp.wantPhase {
				t.Fatalf("status.phase = %q, want %q", hcu.Status.Phase, exp.wantPhase)
			}

			// 2. CRD OpenAPI v3 schema validation on raw YAML structure (spec & status).
			var rawDoc map[string]any
			if err := yaml.Unmarshal(data, &rawDoc); err != nil {
				t.Fatalf("unmarshal raw map: %v", err)
			}
			if err := validateAgainstJSONSchema("spec", rawDoc["spec"], &specSchema); err != nil {
				t.Fatalf("CRD spec schema validation failed: %v", err)
			}
			if statusRaw, hasStatus := rawDoc["status"]; hasStatus {
				if err := validateAgainstJSONSchema("status", statusRaw, &statusSchema); err != nil {
					t.Fatalf("CRD status schema validation failed: %v", err)
				}
			}

			// 3. Local webhook spec field validation.
			if err := upgradev1alpha1.ValidateUpgradeSpecFields(hcu.Spec); err != nil {
				t.Fatalf("ValidateUpgradeSpecFields failed: %v", err)
			}

			// 4. DeepCopy & JSON round-trip fidelity.
			copied := hcu.DeepCopy()
			if !reflect.DeepEqual(&hcu, copied) {
				t.Fatal("DeepCopy mutated or dropped fields")
			}
			encoded, err := json.Marshal(&hcu)
			if err != nil {
				t.Fatalf("json.Marshal failed: %v", err)
			}
			var roundTripped upgradev1alpha1.HostedClusterUpgrade
			if err := yaml.UnmarshalStrict(encoded, &roundTripped); err != nil {
				t.Fatalf("round-trip UnmarshalStrict failed: %v", err)
			}
			if !reflect.DeepEqual(&hcu, &roundTripped) {
				t.Fatal("JSON round-trip did not preserve HostedClusterUpgrade")
			}

			// 5. Anonymization invariants.
			validateAnonymizedSample(t, exp.file, data, &hcu)

			// 6. Argo CD Lua health check evaluation in restricted sandbox.
			gotHealth := evalArgoHealthLua(t, luaScript, &hcu)
			if gotHealth.Status != exp.wantArgoStatus {
				t.Fatalf("Argo CD status = %q, want %q", gotHealth.Status, exp.wantArgoStatus)
			}
			if gotHealth.Message != exp.wantArgoMessage {
				t.Fatalf("Argo CD message = %q, want %q", gotHealth.Message, exp.wantArgoMessage)
			}
			if gotHealth.DeletionMessage != exp.wantDeletionMsg {
				t.Fatalf("Argo CD deletionMessage = %q, want %q", gotHealth.DeletionMessage, exp.wantDeletionMsg)
			}
		})
	}
}

func TestArgoCDHealthCheckEdgeCases(t *testing.T) {
	script := extractGitOpsHealthLua(t)
	data, err := os.ReadFile(filepath.Join("testdata", "samples", "08_verifying_control_plane.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var base upgradev1alpha1.HostedClusterUpgrade
	if err := yaml.UnmarshalStrict(data, &base); err != nil {
		t.Fatal(err)
	}
	reconciler := &HostedClusterUpgradeReconciler{}

	t.Run("empty condition message falls back to status.message despite Lua empty-string truthiness", func(t *testing.T) {
		hcu := base.DeepCopy()
		hcu.Status.Phase = upgradev1alpha1.PhaseBlocked
		hcu.Status.Message = "Preflight Checks Blocked: ClusterOperator 'ingress' is degraded"
		reconciler.updateConditions(hcu)
		for i := range hcu.Status.Conditions {
			if hcu.Status.Conditions[i].Type == "Failing" {
				hcu.Status.Conditions[i].Message = ""
			}
		}

		got := evalArgoHealthLua(t, script, hcu)
		if got.Status != "Degraded" || got.Message != hcu.Status.Message {
			t.Fatalf("got %+v, want Degraded with %q", got, hcu.Status.Message)
		}
	})

	t.Run("phase fallback when conditions are absent", func(t *testing.T) {
		for _, tc := range []struct {
			phase      upgradev1alpha1.UpgradePhase
			wantStatus string
		}{
			{upgradev1alpha1.PhaseCompleted, "Healthy"},
			{upgradev1alpha1.PhasePaused, "Suspended"},
			{upgradev1alpha1.PhaseBlocked, "Degraded"},
			{upgradev1alpha1.PhaseUpgradingControlPlane, "Progressing"},
		} {
			hcu := base.DeepCopy()
			hcu.Status.Conditions = nil
			hcu.Status.ObservedGeneration = hcu.Generation
			hcu.Status.Phase = tc.phase
			hcu.Status.Message = ""

			got := evalArgoHealthLua(t, script, hcu)
			if got.Status != tc.wantStatus || got.Message == "" {
				t.Fatalf("phase %s fallback got %+v, want status %s with default message", tc.phase, got, tc.wantStatus)
			}
		}
	})
}
