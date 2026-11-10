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
	"strings"
	"time"

	hypershiftv1beta1 "github.com/openshift/hypershift/api/hypershift/v1beta1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// log is for logging in this package.
var hostedclusterupgradelog = logf.Log.WithName("hostedclusterupgrade-resource")

func (r *HostedClusterUpgrade) SetupWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr, &HostedClusterUpgrade{}).
		WithValidator(&HostedClusterUpgradeCustomValidator{Client: mgr.GetClient()}).
		Complete()
}

//+kubebuilder:webhook:path=/validate-upgrade-0xshift-dev-v1alpha1-hostedclusterupgrade,mutating=false,failurePolicy=fail,sideEffects=None,groups=upgrade.0xshift.dev,resources=hostedclusterupgrades,verbs=create;update,versions=v1alpha1,name=vhostedclusterupgrade.kb.io,admissionReviewVersions=v1

// +kubebuilder:object:generate=false
type HostedClusterUpgradeCustomValidator struct {
	Client client.Client
}

var _ admission.Validator[*HostedClusterUpgrade] = &HostedClusterUpgradeCustomValidator{}

// ValidateCreate implements admission.Validator so a webhook will be registered for the type
func (v *HostedClusterUpgradeCustomValidator) ValidateCreate(ctx context.Context, obj *HostedClusterUpgrade) (admission.Warnings, error) {
	hostedclusterupgradelog.Info("validate create", "name", obj.Name)
	if err := ValidateUpgradeSpecFields(obj.Spec); err != nil {
		return nil, err
	}
	return nil, v.validateUpgradeSpec(ctx, obj)
}

// ValidateUpdate implements admission.Validator so a webhook will be registered for the type
func (v *HostedClusterUpgradeCustomValidator) ValidateUpdate(ctx context.Context, oldObj, newObj *HostedClusterUpgrade) (admission.Warnings, error) {
	hostedclusterupgradelog.Info("validate update", "name", newObj.Name)
	if err := ValidateUpgradeSpecFields(newObj.Spec); err != nil {
		return nil, err
	}

	// Only validate version progression when target or intermediate release images are modified
	if oldObj.Spec.TargetReleaseImage != newObj.Spec.TargetReleaseImage ||
		oldObj.Spec.IntermediateReleaseImage != newObj.Spec.IntermediateReleaseImage {
		return nil, v.validateUpgradeSpec(ctx, newObj)
	}
	if v.Client != nil {
		hc := &unstructured.Unstructured{}
		hc.SetGroupVersionKind(hypershiftv1beta1.GroupVersion.WithKind("HostedCluster"))
		if err := v.Client.Get(ctx, client.ObjectKeyFromObject(newObj), hc); err != nil {
			return nil, err
		}
		return nil, RejectControlPlaneReleaseOverride(hc)
	}

	return nil, nil
}

// ValidateDelete implements admission.Validator so a webhook will be registered for the type
func (v *HostedClusterUpgradeCustomValidator) ValidateDelete(ctx context.Context, obj *HostedClusterUpgrade) (admission.Warnings, error) {
	hostedclusterupgradelog.Info("validate delete", "name", obj.Name)
	return nil, nil
}

// ValidateUpgradeSpecFields validates local rollout, pause, and schedule fields
// without a cluster lookup. A provided rollout policy must have positive concurrency.
func ValidateUpgradeSpecFields(spec HostedClusterUpgradeSpec) error {
	if spec.RolloutPolicy != nil && spec.RolloutPolicy.MaxParallelNodePools <= 0 {
		return fmt.Errorf("spec.rolloutPolicy.maxParallelNodePools must be at least 1")
	}
	names := make(map[string]struct{}, len(spec.NodePoolGroups))
	for i, group := range spec.NodePoolGroups {
		if strings.TrimSpace(group.Name) == "" {
			return fmt.Errorf("spec.nodePoolGroups[%d].name must not be blank", i)
		}
		if _, exists := names[group.Name]; exists {
			return fmt.Errorf("spec.nodePoolGroups[%d].name duplicates group %q", i, group.Name)
		}
		names[group.Name] = struct{}{}
	}
	if spec.PausedUntil != nil {
		pause := *spec.PausedUntil
		if pause != "" && !strings.EqualFold(pause, "true") && !strings.EqualFold(pause, "false") {
			if _, err := time.Parse(time.RFC3339, pause); err != nil {
				return fmt.Errorf("spec.pausedUntil must be empty, true, false, or an RFC3339 timestamp: %w", err)
			}
		}
	}
	return validateNodePoolGroupSchedule(spec.NodePoolGroups)
}

func validateNodePoolGroupSchedule(groups []NodePoolGroupSpec) error {
	for i := 0; i < len(groups); i++ {
		for j := i + 1; j < len(groups); j++ {
			grpA := groups[i]
			grpB := groups[j]

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

// validateUpgradeSpec aggregates our core SRE API safety validation gates.
func (v *HostedClusterUpgradeCustomValidator) validateUpgradeSpec(ctx context.Context, r *HostedClusterUpgrade) error {
	cli := v.Client
	if cli == nil {
		// Local fields are already validated; only cluster-dependent checks are skipped.
		return nil
	}

	// 1. Fetch target HostedCluster to enforce 1:1 Name Constraint & extract current version
	uHC := &unstructured.Unstructured{}
	uHC.SetGroupVersionKind(hypershiftv1beta1.GroupVersion.WithKind("HostedCluster"))

	err := cli.Get(ctx, client.ObjectKey{Namespace: r.Namespace, Name: r.Name}, uHC)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return fmt.Errorf("target HostedCluster '%s' does not exist in namespace '%s'", r.Name, r.Namespace)
		}
		return fmt.Errorf("failed to verify target HostedCluster: %v", err)
	}

	// 2. Extract current version from the HostedCluster status or spec (preferring Completed history entry).
	if err := RejectControlPlaneReleaseOverride(uHC); err != nil {
		return err
	}
	currentImage, currentVersion := GetHostedClusterCurrentRelease(uHC)
	if currentVersion == "" {
		currentVersion = currentImage
	}
	if currentVersion == "" {
		return fmt.Errorf("unable to determine current version from target HostedCluster '%s' spec or status", r.Name)
	}

	// 3. Use status version for progression while retaining the image digest
	// as the immutable identity required for same-version baseline adoption.
	if err := validateUpgradePath(currentVersion, currentImage, r.Spec.TargetReleaseImage, r.Spec.IntermediateReleaseImage, false); err != nil {
		return err
	}

	return nil
}

// RejectControlPlaneReleaseOverride keeps guest and management payloads on the
// same coordinated path. Independent management-plane upgrades are unsupported.
func RejectControlPlaneReleaseOverride(hc *unstructured.Unstructured) error {
	if value, found, err := unstructured.NestedFieldNoCopy(hc.Object, "spec", "controlPlaneRelease"); err != nil {
		return err
	} else if found && value != nil {
		return fmt.Errorf("HostedCluster spec.controlPlaneRelease override is unsupported; remove it before coordinating an upgrade")
	}
	return nil
}
