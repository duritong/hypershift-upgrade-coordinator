package controller

import upgradev1alpha1 "github.com/duritong/hypershift-upgrade-coordinator/api/v1alpha1"

func markUpgradeBlocked(hcu *upgradev1alpha1.HostedClusterUpgrade) {
	if hcu.Status.Phase != upgradev1alpha1.PhaseBlocked {
		hcu.Status.PhaseBeforeBlock = hcu.Status.Phase
		if hcu.Status.PhaseBeforeBlock == upgradev1alpha1.PhasePaused {
			hcu.Status.PhaseBeforeBlock = hcu.Status.PhaseBeforePause
		}
		if hcu.Status.PhaseBeforeBlock == "" || hcu.Status.PhaseBeforeBlock == upgradev1alpha1.PhaseIdle {
			hcu.Status.PhaseBeforeBlock = upgradev1alpha1.PhasePreflightChecks
		}
	}
	hcu.Status.Phase = upgradev1alpha1.PhaseBlocked
}

func retryBlockedStep(hcu *upgradev1alpha1.HostedClusterUpgrade) {
	if hcu.Status.Phase != upgradev1alpha1.PhaseBlocked {
		return
	}
	phase := hcu.Status.PhaseBeforeBlock
	if phase == "" || phase == upgradev1alpha1.PhaseBlocked || phase == upgradev1alpha1.PhasePaused {
		// Existing persisted Blocked resources may predate the saved retry phase.
		phase = upgradev1alpha1.PhasePreflightChecks
		if len(hcu.Status.NodePoolStatuses) > 0 {
			phase = upgradev1alpha1.PhaseUpgradingNodePools
		} else if hcu.Status.ControlPlaneStatus.TargetReleaseImage != "" {
			phase = upgradev1alpha1.PhaseVerifyingControlPlane
		}
	}
	hcu.Status.Phase = phase
}
