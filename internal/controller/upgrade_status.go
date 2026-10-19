package controller

import (
	"context"
	"reflect"

	upgradev1alpha1 "github.com/duritong/hypershift-upgrade-coordinator/api/v1alpha1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func upgradeCondition(typ string) bool {
	return typ == "Ready" || typ == "Progressing" || typ == "Failing" || typ == "GuestAssessmentReady"
}

// Retry only status persistence, never the reconciliation/action that produced
// desired. Fresh access-owned fields and all foreign conditions survive rebasing.
func (r *HostedClusterUpgradeReconciler) persistUpgradeStatus(ctx context.Context, desired *upgradev1alpha1.HostedClusterUpgrade) error {
	reader := r.APIReader
	if reader == nil {
		reader = r.Client
	}
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		fresh := &upgradev1alpha1.HostedClusterUpgrade{}
		if err := reader.Get(ctx, client.ObjectKeyFromObject(desired), fresh); err != nil {
			return err
		}
		if fresh.UID != desired.UID || fresh.Generation != desired.Generation || !reflect.DeepEqual(fresh.Spec, desired.Spec) {
			return accessFailure("UpgradeChangedDuringStatusWrite")
		}
		before := fresh.Status.DeepCopy()
		conditions := fresh.Status.Conditions
		for i := len(conditions) - 1; i >= 0; i-- {
			if upgradeCondition(conditions[i].Type) {
				conditions = append(conditions[:i], conditions[i+1:]...)
			}
		}
		for _, c := range desired.Status.Conditions {
			if upgradeCondition(c.Type) {
				meta.SetStatusCondition(&conditions, c)
			}
		}
		// All status fields other than GuestAccess and foreign conditions are owned
		// by this controller. Copy first so a failed update cannot alias desired.
		next := desired.Status.DeepCopy()
		next.GuestAccess, next.Conditions = fresh.Status.GuestAccess, conditions
		fresh.Status = *next
		if reflect.DeepEqual(before, &fresh.Status) {
			return nil
		}
		return r.Status().Update(ctx, fresh)
	})
}
