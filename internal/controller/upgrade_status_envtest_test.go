package controller

import (
	"context"
	"fmt"

	upgradev1 "github.com/duritong/hypershift-upgrade-coordinator/api/v1alpha1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var _ = Describe("Upgrade status ownership", func() {
	It("persists virtualization separately alongside the maximum CO inventory", func() {
		ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "virtualization-status-bounds"}}
		Expect(k8sClient.Create(ctx, ns)).To(Succeed())
		DeferCleanup(func() { Expect(k8sClient.Delete(ctx, ns)).To(Succeed()) })
		u := regressionHCU("")
		u.Namespace = ns.Name
		u.UID = ""
		u.Status = upgradev1.HostedClusterUpgradeStatus{}
		Expect(k8sClient.Create(ctx, u)).To(Succeed())
		DeferCleanup(func() { Expect(k8sClient.Delete(ctx, u)).To(Succeed()) })
		u.Status.GuestAssessment = &upgradev1.GuestAssessmentStatus{ObservedGeneration: u.Generation, State: "Ready", Reason: "GuestReady", Purpose: "WorkerGuard", LastAttemptTime: metav1.Now(), Virtualization: &upgradev1.GuestOperatorSummary{Name: "openshift-virtualization", Version: "4.21.0"}}
		for i := 0; i < 256; i++ {
			u.Status.GuestAssessment.Operators = append(u.Status.GuestAssessment.Operators, upgradev1.GuestOperatorSummary{Name: fmt.Sprintf("co-%d", i), Version: "4.21.0"})
		}
		Expect(k8sClient.Status().Update(ctx, u)).To(Succeed())
		fresh := &upgradev1.HostedClusterUpgrade{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(u), fresh)).To(Succeed())
		Expect(fresh.Status.GuestAssessment.Operators).To(HaveLen(256))
		Expect(fresh.Status.GuestAssessment.Virtualization).To(Equal(u.Status.GuestAssessment.Virtualization))
	})
	It("rebases only upgrade-owned fields after a real API status conflict", func() {
		// The background upgrade reconciler intentionally filters this namespace.
		ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "status-ownership"}}
		Expect(k8sClient.Create(ctx, ns)).To(Succeed())
		DeferCleanup(func() { Expect(k8sClient.Delete(ctx, ns)).To(Succeed()) })
		u := regressionHCU("")
		u.Namespace = ns.Name
		u.UID = ""
		u.Status = upgradev1.HostedClusterUpgradeStatus{}
		Expect(k8sClient.Create(ctx, u)).To(Succeed())
		DeferCleanup(func() { Expect(k8sClient.Delete(ctx, u)).To(Succeed()) })
		desired := u.DeepCopy()
		desired.Status.Phase = upgradev1.PhaseUpgradingNodePools
		c := &conflictUpgradeClient{Client: k8sClient}
		c.hook = func(ctx context.Context) error {
			access := &GuestAccessReconciler{Client: k8sClient, GuestAccessProvider: GuestAccessProvider{APIReader: k8sClient}}
			return access.persistAccessStatus(ctx, &accessSource{hcu: u}, &upgradev1.GuestAccessStatus{ObservedGeneration: u.Generation, Reason: "ConcurrentRotation", MaintenanceReady: true, CredentialAvailable: true, ObserverUsability: "Unknown"})
		}
		r := &HostedClusterUpgradeReconciler{Client: c, APIReader: k8sClient}
		r.updateConditions(desired)
		Expect(r.persistUpgradeStatus(ctx, desired)).To(Succeed())
		Expect(c.attempts).To(Equal(2))
		fresh := &upgradev1.HostedClusterUpgrade{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(u), fresh)).To(Succeed())
		Expect(fresh.Status.Phase).To(Equal(upgradev1.PhaseUpgradingNodePools))
		Expect(fresh.Status.GuestAccess.Reason).To(Equal("ConcurrentRotation"))
		Expect(meta.FindStatusCondition(fresh.Status.Conditions, "GuestAccessMaintenanceReady").Reason).To(Equal("ConcurrentRotation"))
		fresh.Spec.TargetReleaseImage = regressionImage("4.22.0")
		Expect(k8sClient.Update(ctx, fresh)).To(Succeed())
		Expect(r.persistUpgradeStatus(ctx, desired)).NotTo(Succeed())
	})
})
