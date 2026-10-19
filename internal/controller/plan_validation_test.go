package controller

import (
	"testing"

	upgradev1alpha1 "github.com/duritong/hypershift-upgrade-coordinator/api/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestPlanValidationPrecedesControlPlaneStart(t *testing.T) {
	for _, defect := range []string{"unknown group", "duplicate groups", "zero concurrency"} {
		t.Run(defect, func(t *testing.T) {
			hcu := regressionHCU(upgradev1alpha1.PhasePendingSchedule)
			np := regressionNP("workers", "")
			switch defect {
			case "unknown group":
				np.SetLabels(map[string]string{"upgrade.huc.0xshift.dev/group": "unknown"})
			case "duplicate groups":
				hcu.Spec.NodePoolGroups = []upgradev1alpha1.NodePoolGroupSpec{{Name: "infra"}, {Name: "infra"}}
			case "zero concurrency":
				hcu.Spec.RolloutPolicy = &upgradev1alpha1.RolloutPolicySpec{}
			}
			c := &regressionClient{objects: []client.Object{hcu, regressionHC("4.20.0"), np}}
			got := reconcileRegression(t, c)
			if got.Status.Phase != upgradev1alpha1.PhaseBlocked || c.writes != 0 {
				t.Fatalf("invalid plan advanced: %+v writes=%d", got.Status, c.writes)
			}
		})
	}
}
