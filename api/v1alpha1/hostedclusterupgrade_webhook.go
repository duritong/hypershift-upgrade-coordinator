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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

	parsedIntermediates := make([]string, len(r.Spec.IntermediateReleaseImages))
	for i, img := range r.Spec.IntermediateReleaseImages {
		v, err := parseVersionFromPullspec(img)
		if err != nil {
			return fmt.Errorf("spec.intermediateReleaseImages[%d] format is invalid: %v", i, err)
		}
		parsedIntermediates[i] = v
	}

	// 4. Enforce Monotonic Version Progression (strictly greater transitions)
	lastVersion := currentVersion
	for i, v := range parsedIntermediates {
		if !isUpstreamGreater(lastVersion, v) {
			return fmt.Errorf("invalid version progression: intermediate version '%s' at index %d is not strictly greater than previous version '%s'", v, i, lastVersion)
		}
		lastVersion = v
	}

	if !isUpstreamGreater(lastVersion, targetVersion) {
		return fmt.Errorf("invalid version progression: target version '%s' is not strictly greater than previous version '%s'", targetVersion, lastVersion)
	}

	// 5. NodePool Priority Order & StartAt Chronological Consistency Check
	type npInfo struct {
		name    string
		order   int
		startAt *metav1.Time
	}

	nodePoolInfos := make([]npInfo, len(r.Spec.NodePools))

	for i, npSpec := range r.Spec.NodePools {
		uNP := &unstructured.Unstructured{}
		uNP.SetGroupVersionKind(schema.GroupVersionKind{
			Group:   "hypershift.openshift.io",
			Version: "v1beta1",
			Kind:    "NodePool",
		})

		// Retrieve each declared NodePool CR to fetch its order priority label
		err := webhookClient.Get(ctx, client.ObjectKey{Namespace: r.Namespace, Name: npSpec.Name}, uNP)
		if err != nil {
			if apierrors.IsNotFound(err) {
				return fmt.Errorf("declared NodePool '%s' does not exist in namespace '%s'", npSpec.Name, r.Namespace)
			}
			return fmt.Errorf("failed to verify NodePool '%s': %v", npSpec.Name, err)
		}

		labels := uNP.GetLabels()
		order := 50 // default SRE priority order
		if labels != nil && labels["upgrade.huc.0xshift.dev/order"] != "" {
			if val, err := strconv.Atoi(labels["upgrade.huc.0xshift.dev/order"]); err == nil {
				order = val
			}
		}

		nodePoolInfos[i] = npInfo{
			name:    npSpec.Name,
			order:   order,
			startAt: npSpec.StartAt,
		}
	}

	// Perform temporal/chronological checks between any two declared NodePools
	for i := 0; i < len(nodePoolInfos); i++ {
		for j := i + 1; j < len(nodePoolInfos); j++ {
			npA := nodePoolInfos[i]
			npB := nodePoolInfos[j]

			// Skip if either has no scheduled StartAt time
			if npA.startAt == nil || npB.startAt == nil {
				continue
			}

			// If NodePool A has lower label priority order than NodePool B (A must upgrade first),
			// then B's StartAt schedule cannot be earlier than A's StartAt schedule.
			if npA.order < npB.order && npB.startAt.Before(npA.startAt) {
				return fmt.Errorf("temporal schedule inconsistency: NodePool '%s' (order %d) is scheduled to upgrade at '%s', which is earlier than priority-higher NodePool '%s' (order %d) scheduled at '%s'",
					npB.name, npB.order, npB.startAt.Time.Format(time.RFC3339),
					npA.name, npA.order, npA.startAt.Time.Format(time.RFC3339))
			}

			// Reverse check: If NodePool B has lower label priority order than NodePool A
			if npB.order < npA.order && npA.startAt.Before(npB.startAt) {
				return fmt.Errorf("temporal schedule inconsistency: NodePool '%s' (order %d) is scheduled to upgrade at '%s', which is earlier than priority-higher NodePool '%s' (order %d) scheduled at '%s'",
					npA.name, npA.order, npA.startAt.Time.Format(time.RFC3339),
					npB.name, npB.order, npB.startAt.Time.Format(time.RFC3339))
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
