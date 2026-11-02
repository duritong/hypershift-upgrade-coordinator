package controller

import "testing"

func TestVirtualizationMetricReasons(t *testing.T) {
	for _, reason := range []string{"CNVUnsupportedAPI", "CNVMalformedEvidence", "CNVObservationConflict", "CNVStaleGeneration", "CNVKubeVirtConverging", "CNVHealthUnknown", "CNVSystemUnhealthy", "CNVConditionsUnknown", "CNVNotAvailable", "CNVDegraded", "CNVProgressing", "CNVReconcileIncomplete", "CNVNotUpgradeable", "CNVWorkloadsUnknown", "CNVOutdatedWorkloads", "CNVWorkloadUpdatesNotHeld", "CNVWorkloadUpdatesStillHeld", "CNVVersionMismatch", "CNVIntermediateVersionMismatch", "CNVTargetVersionMismatch"} {
		if guestMetricReason(reason) != reason {
			t.Fatalf("virtualization reason collapsed: %s", reason)
		}
	}
	if guestMetricReason("CNVUntrustedArbitraryValue") != "Other" {
		t.Fatal("metric reason vocabulary is not closed")
	}
}
