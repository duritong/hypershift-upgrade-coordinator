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
	return nil, v.validateUpgradeSpec(ctx, obj)
}

// ValidateUpdate implements admission.Validator so a webhook will be registered for the type
func (v *HostedClusterUpgradeCustomValidator) ValidateUpdate(ctx context.Context, oldObj, newObj *HostedClusterUpgrade) (admission.Warnings, error) {
	hostedclusterupgradelog.Info("validate update", "name", newObj.Name)
	// NodePool Group-Level Chronological Consistency Check must always hold
	if err := validateNodePoolGroupSchedule(newObj.Spec.NodePoolGroups); err != nil {
		return nil, err
	}

	// Only validate version progression when target or intermediate release images are modified
	if oldObj.Spec.TargetReleaseImage != newObj.Spec.TargetReleaseImage ||
		oldObj.Spec.IntermediateReleaseImage != newObj.Spec.IntermediateReleaseImage {
		return nil, v.validateUpgradeSpec(ctx, newObj)
	}

	return nil, nil
}

// ValidateDelete implements admission.Validator so a webhook will be registered for the type
func (v *HostedClusterUpgradeCustomValidator) ValidateDelete(ctx context.Context, obj *HostedClusterUpgrade) (admission.Warnings, error) {
	hostedclusterupgradelog.Info("validate delete", "name", obj.Name)
	return nil, nil
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
		// Skip validation if client is not initialized (e.g. during standalone unit tests)
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
	var currentRelease string
	history, found, _ := unstructured.NestedSlice(uHC.Object, "status", "version", "history")
	if found && len(history) > 0 {
		for _, item := range history {
			hMap, ok := item.(map[string]interface{})
			if !ok {
				continue
			}
			state, _ := hMap["state"].(string)
			if state == "Completed" {
				currentRelease, _ = hMap["version"].(string)
				if currentRelease == "" {
					currentRelease, _ = hMap["image"].(string)
				}
				break
			}
		}
		if currentRelease == "" {
			if hMap, ok := history[0].(map[string]interface{}); ok {
				currentRelease, _ = hMap["version"].(string)
				if currentRelease == "" {
					currentRelease, _ = hMap["image"].(string)
				}
			}
		}
	}
	if currentRelease == "" {
		currentRelease, _, _ = unstructured.NestedString(uHC.Object, "status", "version", "desired", "version")
	}
	if currentRelease == "" {
		currentRelease, _, _ = unstructured.NestedString(uHC.Object, "status", "version", "desired", "image")
	}
	if currentRelease == "" {
		currentRelease, _, _ = unstructured.NestedString(uHC.Object, "spec", "release", "image")
	}

	if currentRelease == "" {
		return fmt.Errorf("unable to determine current version from target HostedCluster '%s' spec or status", r.Name)
	}

	// 3. Share path validation with controller preflight, but keep admission strict.
	if err := ValidateUpgradePath(currentRelease, r.Spec.TargetReleaseImage, r.Spec.IntermediateReleaseImage, false); err != nil {
		return err
	}

	// 4. NodePool Group-Level Chronological Consistency Check
	return validateNodePoolGroupSchedule(r.Spec.NodePoolGroups)
}
