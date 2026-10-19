package controller

import (
	"context"
	"fmt"

	upgradev1alpha1 "github.com/duritong/hypershift-upgrade-coordinator/api/v1alpha1"
)

func (r *HostedClusterUpgradeReconciler) validateUpgradePlan(ctx context.Context, hcu *upgradev1alpha1.HostedClusterUpgrade) error {
	if err := upgradev1alpha1.ValidateUpgradeSpecFields(hcu.Spec); err != nil {
		return err
	}
	groups := map[string]bool{"default": true}
	for _, group := range hcu.Spec.NodePoolGroups {
		groups[group.Name] = true
	}
	pools, err := r.listClusterNodePools(ctx, hcu.Namespace, hcu.Name)
	if err != nil {
		return err
	}
	for _, pool := range pools.Items {
		group := pool.GetLabels()["upgrade.huc.0xshift.dev/group"]
		if group != "" && !groups[group] {
			return fmt.Errorf("NodePool %q references unknown group %q", pool.GetName(), group)
		}
	}
	return nil
}
