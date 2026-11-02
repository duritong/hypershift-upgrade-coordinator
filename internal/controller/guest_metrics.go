package controller

import (
	"time"

	upgradev1 "github.com/duritong/hypershift-upgrade-coordinator/api/v1alpha1"
	"github.com/duritong/hypershift-upgrade-coordinator/internal/guest"
	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

var (
	metricGuestCredentialAvailable = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "hypershift_upgrade_guest_credential_available", Help: "Owned credential reported available and not expired; not proof of observer authentication."}, []string{"namespace", "name"})
	metricGuestMaintenanceReady    = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "hypershift_upgrade_guest_access_maintenance_ready", Help: "Latest current-generation credential maintenance outcome, separate from observer assessment."}, []string{"namespace", "name"})
	metricGuestCredentialExpiry    = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "hypershift_upgrade_guest_credential_expiration_timestamp_seconds", Help: "Reported observer credential expiration as Unix seconds; absent when unknown."}, []string{"namespace", "name"})
	metricGuestLastRead            = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "hypershift_upgrade_guest_last_successful_observation_timestamp_seconds", Help: "Last successful scoped collection timestamp, even when health is bad; zero if none recorded."}, []string{"namespace", "name"})
	metricGuestLastAttempt         = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "hypershift_upgrade_guest_assessment_timestamp_seconds", Help: "Latest guest assessment attempt timestamp; zero if none recorded."}, []string{"namespace", "name"})
	metricGuestAssessment          = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "hypershift_upgrade_guest_assessment", Help: "Latest purpose-specific verdict (value 1), not global guest health or authorization. Stale evidence is explicitly classified."}, []string{"namespace", "name", "purpose", "state", "reason"})
	metricGuestReady               = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "hypershift_upgrade_guest_assessment_ready", Help: "Latest fresh current-plan purpose-specific Ready verdict; never authorizes an action."}, []string{"namespace", "name", "purpose"})
	metricGuestOperators           = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "hypershift_upgrade_guest_operator_count", Help: "Number of operator summaries in the latest fresh assessment; not an expected inventory."}, []string{"namespace", "name"})
	metricGuestBlockers            = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "hypershift_upgrade_guest_blocker_count", Help: "Total blockers in the latest fresh assessment, including omitted details."}, []string{"namespace", "name"})
	metricGuestTruncated           = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "hypershift_upgrade_guest_blockers_truncated", Help: "Whether latest fresh assessment blocker details are truncated."}, []string{"namespace", "name"})
	metricGuestOperatorBlocker     = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "hypershift_upgrade_guest_operator_blocker", Help: "Reported operator blocker (value 1), at most 32 details; absence is not proof of health."}, []string{"namespace", "name", "purpose", "operator", "reason"})
)

func guestMetricGauges() []*prometheus.GaugeVec {
	return []*prometheus.GaugeVec{metricGuestCredentialAvailable, metricGuestMaintenanceReady, metricGuestCredentialExpiry, metricGuestLastRead, metricGuestLastAttempt, metricGuestAssessment, metricGuestReady, metricGuestOperators, metricGuestBlockers, metricGuestTruncated, metricGuestOperatorBlocker}
}

func init() {
	for _, gauge := range guestMetricGauges() {
		metrics.Registry.MustRegister(gauge)
	}
}

func clearGuestMetrics(namespace, name string) {
	for _, gauge := range guestMetricGauges() {
		gauge.DeletePartialMatch(prometheus.Labels{"namespace": namespace, "name": name})
	}
}

func guestMetricPurpose(purpose string) string {
	switch guest.Purpose(purpose) {
	case guest.SourceBaseline, guest.BeforeControlPlaneRequest, guest.ObserveControlPlane, guest.IntermediateVerification, guest.WorkerEntry, guest.WorkerGuard, guest.FinalUpgradeVerification:
		return purpose
	default:
		return "Unknown"
	}
}

// Deliberately closed label vocabulary. Detailed reasons remain in status; never
// allow arbitrary status strings, object identities or messages into labels.
func guestMetricReason(reason string) string {
	switch reason {
	case "GuestReady", "ObservationOnly", "OperatorVersionMismatch", "CVFailing", "CVDegraded", "CVProgressing", "CVLatestNotCompleted", "CVDesiredReleaseMismatch", "COAvailable", "CODegraded", "COProgressing", "COUpgradeNotAllowed", "CredentialExpired", "StaleOrIncompleteSnapshot", "TargetChanged", "MissingOrMalformedOperatorVersion":
		return reason
	case "CNVUnsupportedAPI", "CNVMalformedEvidence", "CNVObservationConflict", "CNVStaleGeneration", "CNVKubeVirtConverging", "CNVHealthUnknown", "CNVSystemUnhealthy", "CNVConditionsUnknown", "CNVNotAvailable", "CNVDegraded", "CNVProgressing", "CNVReconcileIncomplete", "CNVNotUpgradeable", "CNVWorkloadsUnknown", "CNVOutdatedWorkloads", "CNVWorkloadUpdatesNotHeld", "CNVWorkloadUpdatesStillHeld", "CNVVersionMismatch", "CNVIntermediateVersionMismatch", "CNVTargetVersionMismatch":
		return reason
	default:
		return "Other"
	}
}

func exposeGuestMetrics(hcu *upgradev1.HostedClusterUpgrade, now time.Time) {
	// Replace all owned series, including reasons and removed operator names. No
	// failed/partial read can leave a formerly healthy operator series active.
	clearGuestMetrics(hcu.Namespace, hcu.Name)
	boolValue := func(v bool) float64 {
		if v {
			return 1
		}
		return 0
	}
	access := hcu.Status.GuestAccess
	available, maintenance := false, false
	if access != nil {
		maintenance = access.MaintenanceReady && access.ObservedGeneration == hcu.Generation
		if access.ExpiresAt != nil && !access.ExpiresAt.IsZero() {
			metricGuestCredentialExpiry.WithLabelValues(hcu.Namespace, hcu.Name).Set(float64(access.ExpiresAt.Unix()))
			available = access.CredentialAvailable && now.Before(access.ExpiresAt.Time)
		}
	}
	metricGuestCredentialAvailable.WithLabelValues(hcu.Namespace, hcu.Name).Set(boolValue(available))
	metricGuestMaintenanceReady.WithLabelValues(hcu.Namespace, hcu.Name).Set(boolValue(maintenance))
	assessment := hcu.Status.GuestAssessment
	lastRead, lastAttempt := float64(0), float64(0)
	if assessment != nil {
		if assessment.LastSuccessfulObservation != nil && !assessment.LastSuccessfulObservation.IsZero() {
			lastRead = float64(assessment.LastSuccessfulObservation.Unix())
		}
		if !assessment.LastAttemptTime.IsZero() {
			lastAttempt = float64(assessment.LastAttemptTime.Unix())
		}
	}
	metricGuestLastRead.WithLabelValues(hcu.Namespace, hcu.Name).Set(lastRead)
	metricGuestLastAttempt.WithLabelValues(hcu.Namespace, hcu.Name).Set(lastAttempt)
	if hcu.Status.Phase == upgradev1.PhaseCompleted || hcu.Status.Phase == upgradev1.PhasePaused {
		return
	}
	purpose, state, reason := "Unknown", "NotAssessed", "NotAssessed"
	fresh := false
	if assessment != nil {
		purpose, reason = guestMetricPurpose(assessment.Purpose), guestMetricReason(assessment.Reason)
		switch guest.State(assessment.State) {
		case guest.Ready, guest.Converging, guest.Unhealthy, guest.Unavailable, guest.AccessDenied, guest.IdentityMismatch, guest.UnsupportedEvidence:
			state = assessment.State
		default:
			state, reason = "Unknown", "Other"
		}
		fresh = !assessment.LastAttemptTime.IsZero() && !assessment.LastAttemptTime.After(now) && now.Sub(assessment.LastAttemptTime.Time) <= guest.DecisionAge
		if assessment.ObservedGeneration != hcu.Generation || (assessment.PlanRevision != "" && assessment.PlanRevision != accessHash(hcu.Spec)) || (state == string(guest.Ready) && assessment.PlanRevision == "") || (access != nil && access.HostedClusterUID != "" && assessment.HostedClusterUID != "" && access.HostedClusterUID != assessment.HostedClusterUID) {
			state, reason, fresh = "Invalidated", "TargetChanged", false
		} else if !fresh {
			state, reason = "Stale", "StaleAssessment"
		}
		if state == string(guest.Ready) && access != nil && access.ExpiresAt != nil && !now.Before(access.ExpiresAt.Time) {
			state, reason, fresh = "Stale", "CredentialExpired", false
		}
		if state == string(guest.Ready) && (assessment.LastSuccessfulObservation == nil || assessment.LastSuccessfulObservation.After(now) || now.Sub(assessment.LastSuccessfulObservation.Time) > guest.DecisionAge || purpose == "Unknown" || purpose == string(guest.ObserveControlPlane)) {
			state, reason, fresh = "Stale", "StaleAssessment", false
		}
	}
	metricGuestAssessment.WithLabelValues(hcu.Namespace, hcu.Name, purpose, state, reason).Set(1)
	metricGuestReady.WithLabelValues(hcu.Namespace, hcu.Name, purpose).Set(boolValue(state == string(guest.Ready)))
	if !fresh {
		return
	}
	metricGuestOperators.WithLabelValues(hcu.Namespace, hcu.Name).Set(float64(min(len(assessment.Operators), guest.MaxOperators)))
	metricGuestBlockers.WithLabelValues(hcu.Namespace, hcu.Name).Set(float64(max(0, assessment.BlockerCount)))
	metricGuestTruncated.WithLabelValues(hcu.Namespace, hcu.Name).Set(boolValue(assessment.Truncated))
	for _, blocker := range assessment.Blockers[:min(len(assessment.Blockers), 32)] {
		if blocker.Operator == "" || len(validation.IsDNS1123Subdomain(blocker.Operator)) != 0 {
			continue
		}
		metricGuestOperatorBlocker.WithLabelValues(hcu.Namespace, hcu.Name, purpose, blocker.Operator, guestMetricReason(blocker.Reason)).Set(1)
	}
}
