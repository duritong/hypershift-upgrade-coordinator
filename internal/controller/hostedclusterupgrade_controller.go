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
	"fmt"
	"reflect"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	upgradev1alpha1 "github.com/duritong/hypershift-upgrade-coordinator/api/v1alpha1"
)

// HostedClusterUpgradeReconciler reconciles a HostedClusterUpgrade object
type HostedClusterUpgradeReconciler struct {
	client.Client
	Scheme                 *runtime.Scheme
	HostedClusterNamespace string
}

// Global Unstructured schema for ControlPlaneComponent
var gvkCPC = schema.GroupVersionKind{
	Group:   "controlplane.hypershift.openshift.io",
	Version: "v1alpha1",
	Kind:    "ControlPlaneComponent",
}

//+kubebuilder:rbac:groups=upgrade.0xshift.dev,resources=hostedclusterupgrades,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=upgrade.0xshift.dev,resources=hostedclusterupgrades/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=upgrade.0xshift.dev,resources=hostedclusterupgrades/finalizers,verbs=update
//+kubebuilder:rbac:groups=controlplane.hypershift.openshift.io,resources=controlplanecomponents,verbs=get;list;watch

// Reconcile is the main state machine loop for HostedClusterUpgrade.
func (r *HostedClusterUpgradeReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	reqLogger := log.FromContext(ctx)
	reqLogger.Info("Reconciling HostedClusterUpgrade")

	// 1. Fetch the HostedClusterUpgrade object
	hcu := &upgradev1alpha1.HostedClusterUpgrade{}
	err := r.Get(ctx, req.NamespacedName, hcu)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Save original status for deep-equal check later to avoid redundant updates
	originalStatus := hcu.Status.DeepCopy()

	// 2. Check Coordinator Pause State (spec.pausedUntil)
	isPaused, requeueAfter, err := checkPausedState(hcu.Spec.PausedUntil)
	if err != nil {
		reqLogger.Error(err, "Failed to parse spec.pausedUntil")
		hcu.Status.Phase = upgradev1alpha1.PhaseBlocked
		hcu.Status.Message = fmt.Errorf("invalid spec.pausedUntil format: %v", err).Error()
		if !reflect.DeepEqual(originalStatus, &hcu.Status) {
			_ = r.Status().Update(ctx, hcu)
		}
		return ctrl.Result{}, err
	}

	// 3. Aggregate ControlPlaneComponent status (Always run, even if paused, for observe-only metrics)
	err = r.aggregateControlPlaneStatus(ctx, hcu)
	if err != nil {
		reqLogger.Error(err, "Failed to aggregate ControlPlaneComponent status")
	}

	if isPaused {
		reqLogger.Info("Upgrade coordinator is currently paused (Observe-Only Mode)")
		hcu.Status.Phase = upgradev1alpha1.PhasePaused
		hcu.Status.Message = "Upgrade coordinator is paused via spec.pausedUntil."

		// If there is a timed pause expiration, requeue for that time
		if requeueAfter > 0 {
			reqLogger.Info("Requeuing for scheduled pause expiration", "requeueAfter", requeueAfter)
		}

		if !reflect.DeepEqual(originalStatus, &hcu.Status) {
			err = r.Status().Update(ctx, hcu)
			if err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{RequeueAfter: requeueAfter}, nil
	}

	// 4. State Machine implementation logic skeleton (To be filled in Milestones 4-5)
	if hcu.Status.Phase == "" || hcu.Status.Phase == upgradev1alpha1.PhasePaused {
		hcu.Status.Phase = upgradev1alpha1.PhaseIdle
		hcu.Status.Message = "Ready to proceed. Initializing upgrade state machine."
	}

	// Update status if any field changed
	if !reflect.DeepEqual(originalStatus, &hcu.Status) {
		err = r.Status().Update(ctx, hcu)
		if err != nil {
			return ctrl.Result{}, err
		}
	}

	return ctrl.Result{RequeueAfter: requeueAfter}, nil
}

// aggregateControlPlaneStatus queries and copies the individual states of the ControlPlaneComponents.
func (r *HostedClusterUpgradeReconciler) aggregateControlPlaneStatus(ctx context.Context, hcu *upgradev1alpha1.HostedClusterUpgrade) error {
	// Resolved dynamic control plane namespace: <hc_namespace>-<cluster_name>
	cpNamespace := fmt.Sprintf("%s-%s", hcu.Namespace, hcu.Name)

	uList := &unstructured.UnstructuredList{}
	uList.SetGroupVersionKind(gvkCPC)

	// List all CPC resources in the target cluster's namespace
	err := r.List(ctx, uList, client.InNamespace(cpNamespace))
	if err != nil {
		// If CRD or Namespace does not exist yet (e.g. initial setup), we reset and skip
		hcu.Status.ControlPlaneStatus.Components = []upgradev1alpha1.ControlPlaneComponentStatus{}
		hcu.Status.ControlPlaneStatus.TotalComponents = 0
		hcu.Status.ControlPlaneStatus.UpgradedComponents = 0
		return err
	}

	components := make([]upgradev1alpha1.ControlPlaneComponentStatus, len(uList.Items))
	var upgradedCount int32
	blockers := []string{}

	for i, item := range uList.Items {
		name := item.GetName()
		version, _, _ := unstructured.NestedString(item.Object, "status", "version")

		// Extract conditions
		conds, found, _ := unstructured.NestedSlice(item.Object, "status", "conditions")
		available := false
		rolloutComplete := false
		message := ""

		if found {
			for _, c := range conds {
				condMap, ok := c.(map[string]interface{})
				if !ok {
					continue
				}
				condType, _ := condMap["type"].(string)
				condStatus, _ := condMap["status"].(string)
				condMsg, _ := condMap["message"].(string)

				if condType == "Available" && condStatus == "True" {
					available = true
				}
				if condType == "RolloutComplete" {
					if condStatus == "True" {
						rolloutComplete = true
					} else if condMsg != "" {
						message = condMsg
					}
				}
			}
		}

		if available && rolloutComplete {
			upgradedCount++
		} else if message != "" {
			blockers = append(blockers, fmt.Sprintf("Component '%s' is blocked: %s", name, message))
		}

		components[i] = upgradev1alpha1.ControlPlaneComponentStatus{
			Name:            name,
			Version:         version,
			Available:       available,
			RolloutComplete: rolloutComplete,
			Message:         message,
		}
	}

	hcu.Status.ControlPlaneStatus.Components = components
	hcu.Status.ControlPlaneStatus.TotalComponents = int32(len(components))
	hcu.Status.ControlPlaneStatus.UpgradedComponents = upgradedCount
	hcu.Status.ControlPlaneStatus.Blockers = blockers

	return nil
}

// checkPausedState evaluates if the coordinator is paused (indefinitely or timed).
func checkPausedState(pausedUntil *string) (bool, time.Duration, error) {
	if pausedUntil == nil || *pausedUntil == "" {
		return false, 0, nil
	}

	pStr := strings.TrimSpace(*pausedUntil)
	if strings.EqualFold(pStr, "true") {
		return true, 0, nil
	}
	if strings.EqualFold(pStr, "false") {
		return false, 0, nil
	}

	// Try to parse RFC3339 timestamp
	t, err := time.Parse(time.RFC3339, pStr)
	if err != nil {
		return false, 0, fmt.Errorf("unable to parse RFC3339 timestamp '%s': %v", pStr, err)
	}

	now := time.Now()
	if t.After(now) {
		return true, t.Sub(now), nil
	}

	return false, 0, nil
}

// SetupWithManager sets up the controller with the Manager and registers our event-driven watches.
func (r *HostedClusterUpgradeReconciler) SetupWithManager(mgr ctrl.Manager) error {
	uCPC := &unstructured.Unstructured{}
	uCPC.SetGroupVersionKind(gvkCPC)

	return ctrl.NewControllerManagedBy(mgr).
		For(&upgradev1alpha1.HostedClusterUpgrade{}).
		Watches(
			uCPC,
			handler.EnqueueRequestsFromMapFunc(r.mapCPCToHostedClusterUpgrade),
			builder.WithPredicates(statusChangePredicate),
		).
		Complete(r)
}

// mapCPCToHostedClusterUpgrade maps status changes of CPC objects in <hc_ns>-<name> to parent HCU in <hc_ns> sharing <name>.
func (r *HostedClusterUpgradeReconciler) mapCPCToHostedClusterUpgrade(ctx context.Context, obj client.Object) []reconcile.Request {
	cpcNs := obj.GetNamespace()
	prefix := r.HostedClusterNamespace + "-"
	if !strings.HasPrefix(cpcNs, prefix) {
		return nil
	}
	clusterName := strings.TrimPrefix(cpcNs, prefix)

	return []reconcile.Request{
		{
			NamespacedName: client.ObjectKey{
				Namespace: r.HostedClusterNamespace,
				Name:      clusterName,
			},
		},
	}
}

// statusChangePredicate filters out non-status mutations on ControlPlaneComponent resources.
var statusChangePredicate = predicate.Funcs{
	UpdateFunc: func(e event.UpdateEvent) bool {
		oldObj, okOld := e.ObjectOld.(*unstructured.Unstructured)
		newObj, okNew := e.ObjectNew.(*unstructured.Unstructured)
		if !okOld || !okNew {
			return false
		}

		// Extract status structures
		oldStatus, foundOld, _ := unstructured.NestedFieldCopy(oldObj.Object, "status")
		newStatus, foundNew, _ := unstructured.NestedFieldCopy(newObj.Object, "status")
		if !foundOld || !foundNew {
			return true // Reconcile if missing to be safe
		}

		// Deep compare the status subresources
		return !reflect.DeepEqual(oldStatus, newStatus)
	},
	CreateFunc: func(e event.CreateEvent) bool {
		return true
	},
	DeleteFunc: func(e event.DeleteEvent) bool {
		return true
	},
}
