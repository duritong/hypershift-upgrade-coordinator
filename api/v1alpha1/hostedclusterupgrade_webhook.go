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
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"golang.org/x/mod/semver"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// log is for logging in this package.
var hostedclusterupgradelog = logf.Log.WithName("hostedclusterupgrade-resource")

// webhookClient is set up inside SetupWebhookWithManager to allow unstructured API server queries.
var webhookClient client.Client

// Pullspec regex pattern validation matching: registry.domain.com/path:version-arch@sha256:digest
var imagePattern = regexp.MustCompile(`^(\S+)/(\S+):([0-9]+\.[0-9]+\.[0-9]+-[a-zA-Z0-9_]+)@sha256:([a-fA-F0-9]{64})$`)

func (r *HostedClusterUpgrade) SetupWebhookWithManager(mgr ctrl.Manager) error {
	webhookClient = mgr.GetClient()
	return ctrl.NewWebhookManagedBy(mgr).
		For(r).
		Complete()
}

//+kubebuilder:webhook:path=/validate-upgrade-0xshift-dev-v1alpha1-hostedclusterupgrade,mutating=false,failurePolicy=fail,sideEffects=None,groups=upgrade.0xshift.dev,resources=hostedclusterupgrades,verbs=create;update,versions=v1alpha1,name=vhostedclusterupgrade.kb.io,admissionReviewVersions=v1

var _ webhook.Validator = &HostedClusterUpgrade{}

// ValidateCreate implements webhook.Validator so a webhook will be registered for the type
func (r *HostedClusterUpgrade) ValidateCreate() (admission.Warnings, error) {
	hostedclusterupgradelog.Info("validate create", "name", r.Name)
	return nil, r.validateUpgradeSpec()
}

// ValidateUpdate implements webhook.Validator so a webhook will be registered for the type
func (r *HostedClusterUpgrade) ValidateUpdate(old runtime.Object) (admission.Warnings, error) {
	hostedclusterupgradelog.Info("validate update", "name", r.Name)
	return nil, r.validateUpgradeSpec()
}

// ValidateDelete implements webhook.Validator so a webhook will be registered for the type
func (r *HostedClusterUpgrade) ValidateDelete() (admission.Warnings, error) {
	hostedclusterupgradelog.Info("validate delete", "name", r.Name)
	return nil, nil
}

// validateUpgradeSpec aggregates our core SRE API safety validation gates.
func (r *HostedClusterUpgrade) validateUpgradeSpec() error {
	if webhookClient == nil {
		// Skip validation if client is not initialized (e.g. during standalone unit tests)
		return nil
	}

	ctx := context.TODO()

	// 1. Fetch target HostedCluster to enforce 1:1 Name Constraint & extract current version
	uHC := &unstructured.Unstructured{}
	uHC.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "hypershift.openshift.io",
		Version: "v1beta1",
		Kind:    "HostedCluster",
	})

	err := webhookClient.Get(ctx, client.ObjectKey{Namespace: r.Namespace, Name: r.Name}, uHC)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return fmt.Errorf("target HostedCluster '%s' does not exist in namespace '%s'", r.Name, r.Namespace)
		}
		return fmt.Errorf("failed to verify target HostedCluster: %v", err)
	}

	// 2. Extract current version from the HostedCluster status.
	// We fall back to the spec release image if status.releaseImage is not populated.
	currentRelease, _, _ := unstructured.NestedString(uHC.Object, "status", "releaseImage")
	if currentRelease == "" {
		currentRelease, _, _ = unstructured.NestedString(uHC.Object, "spec", "release", "image")
	}

	if currentRelease == "" {
		return fmt.Errorf("unable to determine current version from target HostedCluster '%s' spec or status", r.Name)
	}

	currentVersion, err := parseVersionFromPullspec(currentRelease)
	if err != nil {
		return fmt.Errorf("current cluster release image format is invalid: %v", err)
	}

	// 3. Parse target and intermediate release images
	targetVersion, err := parseVersionFromPullspec(r.Spec.TargetReleaseImage)
	if err != nil {
		return fmt.Errorf("spec.targetReleaseImage format is invalid: %v", err)
	}

	var intermediateVersion string
	if r.Spec.IntermediateReleaseImage != "" {
		v, err := parseVersionFromPullspec(r.Spec.IntermediateReleaseImage)
		if err != nil {
			return fmt.Errorf("spec.intermediateReleaseImage format is invalid: %v", err)
		}
		intermediateVersion = v
	}

	// 4. Enforce Monotonic Version Progression and Strict At-Most-1-Minor Jumps
	currMajor, currMinor, err := getMajorMinor(currentVersion)
	if err != nil {
		return err
	}

	tgtMajor, tgtMinor, err := getMajorMinor(targetVersion)
	if err != nil {
		return err
	}

	if intermediateVersion != "" {
		intMajor, intMinor, err := getMajorMinor(intermediateVersion)
		if err != nil {
			return err
		}

		// First step: Current -> Intermediate
		if !isUpstreamGreater(currentVersion, intermediateVersion) {
			return fmt.Errorf("invalid version progression: intermediate version '%s' is not strictly greater than current version '%s'", intermediateVersion, currentVersion)
		}
		if currMajor != intMajor {
			return fmt.Errorf("invalid major version jump: cannot upgrade across different major versions (%d -> %d)", currMajor, intMajor)
		}
		if intMinor-currMinor > 1 {
			return fmt.Errorf("invalid minor version jump: intermediate version '%s' is more than 1 minor version ahead of current version '%s'", intermediateVersion, currentVersion)
		}

		// Second step: Intermediate -> Target
		if !isUpstreamGreater(intermediateVersion, targetVersion) {
			return fmt.Errorf("invalid version progression: target version '%s' is not strictly greater than intermediate version '%s'", targetVersion, intermediateVersion)
		}
		if intMajor != tgtMajor {
			return fmt.Errorf("invalid major version jump: cannot upgrade across different major versions (%d -> %d)", intMajor, tgtMajor)
		}
		if tgtMinor-intMinor > 1 {
			return fmt.Errorf("invalid minor version jump: target version '%s' is more than 1 minor version ahead of intermediate version '%s'", targetVersion, intermediateVersion)
		}
	} else {
		// Direct step: Current -> Target
		if !isUpstreamGreater(currentVersion, targetVersion) {
			return fmt.Errorf("invalid version progression: target version '%s' is not strictly greater than current version '%s'", targetVersion, currentVersion)
		}
		if currMajor != tgtMajor {
			return fmt.Errorf("invalid major version jump: cannot upgrade across different major versions (%d -> %d)", currMajor, tgtMajor)
		}
		if tgtMinor-currMinor > 1 {
			return fmt.Errorf("unsupported direct multi-minor jump: target version '%s' is more than 1 minor version ahead of current version '%s'. You must specify a singular intermediateReleaseImage for EUS hops (at most 2 minor versions overall).", targetVersion, currentVersion)
		}
	}

	// 5. NodePool Group-Level Chronological Consistency Check
	for i := 0; i < len(r.Spec.NodePoolGroups); i++ {
		for j := i + 1; j < len(r.Spec.NodePoolGroups); j++ {
			grpA := r.Spec.NodePoolGroups[i]
			grpB := r.Spec.NodePoolGroups[j]

			if grpA.StartAt == nil || grpB.StartAt == nil {
				continue
			}

			if grpB.StartAt.Before(grpA.StartAt) {
				return fmt.Errorf("temporal schedule inconsistency: NodePoolGroup '%s' (position %d) is scheduled to upgrade at '%s', which is earlier than priority-higher group '%s' (position %d) scheduled at '%s'",
					grpB.Name, j, grpB.StartAt.Time.Format(time.RFC3339),
					grpA.Name, i, grpA.StartAt.Time.Format(time.RFC3339))
			}
		}
	}

	return nil
}

// parseVersionFromPullspec extracts the semantic version from the tag slice: <version>-<arch>
func parseVersionFromPullspec(pullspec string) (string, error) {
	matches := imagePattern.FindStringSubmatch(pullspec)
	if len(matches) < 4 {
		return "", fmt.Errorf("invalid pullspec format: must match registry.domain.com/path:version-arch@sha256:digest")
	}
	tag := matches[3] // e.g. "4.20.44-x86_64"
	idx := strings.Index(tag, "-")
	if idx == -1 {
		return tag, nil
	}
	return tag[:idx], nil // e.g. "4.20.44"
}

// isUpstreamGreater compares SemVer version strings to ensure vNext is strictly greater than vPrev.
func isUpstreamGreater(vPrev, vNext string) bool {
	// prefix with "v" for golang.org/x/mod/semver compatibility
	pPrev := "v" + vPrev
	pNext := "v" + vNext
	return semver.Compare(pPrev, pNext) < 0
}

// getMajorMinor parses a version string like "4.20.5" or "4.12" and returns major, minor as integers.
func getMajorMinor(version string) (int, int, error) {
	version = strings.TrimPrefix(version, "v")
	parts := strings.Split(version, ".")
	if len(parts) < 2 {
		return 0, 0, fmt.Errorf("invalid semantic version format (expected major.minor.patch): %s", version)
	}
	major, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, 0, fmt.Errorf("failed to parse major version: %v", err)
	}
	minor, err := strconv.Atoi(parts[1])
	if err != nil {
		return 0, 0, fmt.Errorf("failed to parse minor version: %v", err)
	}
	return major, minor, nil
}
