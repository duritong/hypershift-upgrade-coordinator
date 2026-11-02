package controller

import (
	"context"
	"errors"
	"fmt"

	upgradev1 "github.com/duritong/hypershift-upgrade-coordinator/api/v1alpha1"
	"github.com/duritong/hypershift-upgrade-coordinator/internal/guest"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// GuestAccessReader is an access/transport seam, not an injectable health verdict.
// Implementations must validate current management/credential identity. Production
// uses GuestAccessProvider; tests still execute the actual collector and policy.
type GuestAccessReader interface {
	BuildTargetAndReader(context.Context, client.ObjectKey, guest.Release) (guest.Target, *guest.Reader, error)
	ValidateTarget(context.Context, guest.Target) error
}

func guestRelease(image string) guest.Release {
	version, _ := upgradev1.ExtractVersion(image)
	return guest.Release{Version: version, Image: image}
}

// guestGate collects a NEW snapshot on every call, validates identity after the
// network read, then checks its age at the action boundary. No healthy bool or
// persisted admission is consulted as authorization.
func (r *HostedClusterUpgradeReconciler) guestGate(ctx context.Context, hcu *upgradev1.HostedClusterUpgrade, hc *unstructured.Unstructured, expected string, policy guest.Policy) (guest.Target, error) {
	if hcu.Spec.IntermediateReleaseImage != "" {
		if !samePayload(expected, hcu.Spec.TargetReleaseImage) {
			policy.HoldVirtualizationWorkloads = true
			policy.AllowIntermediateWorkloadSkew = samePayload(expected, hcu.Spec.IntermediateReleaseImage)
		} else if policy.Purpose == guest.WorkerEntry || policy.Purpose == guest.FinalUpgradeVerification {
			policy.RequireRestoredWorkloads = true
		}
	}
	var target guest.Target
	var snapshot guest.Snapshot
	var result guest.Result
	var err error
	var proof managementProof
	if policy.Purpose != guest.ObserveControlPlane {
		proof, err = r.verifyControlPlaneProof(ctx, hcu, hc, expected)
		if err != nil {
			err = accessFailure("ManagementPrerequisiteUnavailable")
		}
	}
	expectedRelease := guestRelease(expected)
	if expectedRelease.Version == "" {
		// Installed source pullspecs may be digest-only. Use the independently
		// observed management version, never extract identity from a mutable tag.
		hc := &unstructured.Unstructured{}
		hc.SetGroupVersionKind(gvkHC)
		reader := r.APIReader
		if reader == nil {
			reader = r.Client
		}
		if reader != nil && reader.Get(ctx, client.ObjectKeyFromObject(hcu), hc) == nil {
			image, version := GetHostedClusterCurrentRelease(hc)
			if samePayload(image, expected) {
				expectedRelease.Version = version
			}
		}
	}
	if r.GuestAccess == nil {
		err = accessFailure("GuestAccessNotConfigured")
	} else if err == nil {
		var reader *guest.Reader
		target, reader, err = r.GuestAccess.BuildTargetAndReader(ctx, client.ObjectKeyFromObject(hcu), expectedRelease)
		if err == nil && (target.HCUUID != hcu.UID || target.Generation != hcu.Generation || target.PlanRevision != accessHash(hcu.Spec)) {
			err = accessFailure("UpgradeTargetChanged")
		}
		if err == nil {
			request, _, _ := unstructured.NestedString(hc.Object, "spec", "release", "image")
			clusterID, _, _ := unstructured.NestedString(hc.Object, "spec", "clusterID")
			if target.HCUID != hc.GetUID() || target.HCGeneration != hc.GetGeneration() || string(target.ClusterID) != clusterID || target.HCRelease != request || (policy.Purpose != guest.ObserveControlPlane && (!samePayload(target.HCRelease, expectedRelease.Image) || target.HCPUID != proof.hcpUID || target.HCPGeneration != proof.hcpGeneration || target.HCUID != proof.hcUID || target.HCGeneration != proof.hcGeneration)) {
				err = &guest.Failure{State: guest.IdentityMismatch, Reason: "ManagementGuestTargetMismatch"}
			}
		}
		if err == nil && reader == nil {
			err = accessFailure("GuestReaderUnavailable")
		}
		if err == nil {
			snapshot, err = reader.Collect(ctx)
		}
		if err == nil {
			err = r.GuestAccess.ValidateTarget(ctx, target)
		}
		if err == nil {
			result = guest.Assess(snapshot, target, policy, r.now())
		}
	}
	if err != nil {
		result = guest.Result{State: guest.Unavailable, Reason: "GuestAccessUnavailable"}
		var failure *guest.Failure
		if errors.As(err, &failure) {
			result.State, result.Reason = failure.State, failure.Reason
		}
	}
	status := &upgradev1.GuestAssessmentStatus{ObservedGeneration: hcu.Generation, State: string(result.State), Reason: result.Reason, Purpose: string(policy.Purpose), LastAttemptTime: metav1.NewTime(r.now()), HostedClusterUID: string(target.HCUID), SecretUID: string(target.SecretUID), SecretRevision: target.SecretRevision, PlanRevision: target.PlanRevision, ExpectedVersion: expectedRelease.Version, BlockerCount: int32(result.BlockerCount), Truncated: result.Truncated}
	if previous := hcu.Status.GuestAssessment; previous != nil && target.HCUID != "" && previous.HostedClusterUID == string(target.HCUID) && previous.PlanRevision == target.PlanRevision {
		status.LastSuccessfulObservation = previous.LastSuccessfulObservation.DeepCopy()
	}
	if err == nil {
		t := metav1.NewTime(snapshot.CompletedAt)
		status.LastSuccessfulObservation = &t
	}
	for _, op := range result.Operators {
		status.Operators = append(status.Operators, upgradev1.GuestOperatorSummary{Name: op.Name, Version: op.Version})
	}
	if op := result.Virtualization; op != nil {
		status.Virtualization = &upgradev1.GuestOperatorSummary{Name: op.Name, Version: op.Version}
	}
	for _, b := range result.Blockers {
		status.Blockers = append(status.Blockers, upgradev1.GuestBlocker{Operator: b.Operator, Reason: b.Reason})
	}
	hcu.Status.GuestAssessment = status
	ready := metav1.ConditionFalse
	if result.State == guest.Ready {
		ready = metav1.ConditionTrue
	}
	meta.SetStatusCondition(&hcu.Status.Conditions, metav1.Condition{Type: "GuestAssessmentReady", Status: ready, Reason: result.Reason, Message: string(policy.Purpose) + ": " + string(result.State), ObservedGeneration: hcu.Generation, LastTransitionTime: metav1.NewTime(r.now())})
	if result.State != guest.Ready {
		return target, fmt.Errorf("guest %s: %s (%s)", policy.Purpose, result.State, result.Reason)
	}
	return target, nil
}

func (r *HostedClusterUpgradeReconciler) admitWorkers(hcu *upgradev1.HostedClusterUpgrade, t guest.Target) {
	if a := hcu.Status.WorkerAdmission; a != nil && a.HostedClusterUpgradeUID == string(t.HCUUID) && a.HostedClusterUID == string(t.HCUID) && a.ObservedGeneration == t.Generation && a.PlanRevision == t.PlanRevision && a.ReleaseRevision == accessHash(t.Expected) {
		return
	}
	hcu.Status.WorkerAdmission = &upgradev1.GuestWorkerAdmission{HostedClusterUpgradeUID: string(t.HCUUID), ObservedGeneration: t.Generation, HostedClusterUID: string(t.HCUID), ClusterID: string(t.ClusterID), PlanRevision: t.PlanRevision, ReleaseRevision: accessHash(t.Expected), AdmittedAt: metav1.NewTime(r.now())}
}

func (r *HostedClusterUpgradeReconciler) observeGuest(ctx context.Context, hcu *upgradev1.HostedClusterUpgrade, hc *unstructured.Unstructured) {
	if hc == nil {
		return
	}
	expected, _, _ := unstructured.NestedString(hc.Object, "spec", "release", "image")
	_, _ = r.guestGate(ctx, hcu, hc, expected, guest.Policy{Purpose: guest.ObserveControlPlane})
}
