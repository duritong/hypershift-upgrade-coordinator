package controller

import (
	"strings"
	"testing"

	upgradev1alpha1 "github.com/duritong/hypershift-upgrade-coordinator/api/v1alpha1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestDigestOnlyCurrentRelease(t *testing.T) {
	hcu := regressionHCU(upgradev1alpha1.PhasePreflightChecks)
	hc := regressionHC("4.20.0")
	_ = unstructured.SetNestedSlice(hc.Object, []interface{}{map[string]interface{}{
		"state": "Completed", "version": "4.20.0", "image": "quay.io/ocp@sha256:" + strings.Repeat("a", 64),
	}}, "status", "version", "history")
	got := reconcileRegression(t, &regressionClient{objects: []client.Object{hcu, hc}})
	if got.Status.Phase != upgradev1alpha1.PhasePendingSchedule {
		t.Fatalf("valid upstream version was rejected: %+v", got.Status)
	}
}
