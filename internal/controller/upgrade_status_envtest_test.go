package controller

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	upgradev1 "github.com/duritong/hypershift-upgrade-coordinator/api/v1alpha1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"
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
	It("accepts and round-trips all sample HostedClusterUpgrade manifests and statuses through kube-apiserver", func() {
		ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "sample-manifests"}}
		Expect(k8sClient.Create(ctx, ns)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(ctx, ns) })

		files, err := filepath.Glob(filepath.Join("testdata", "samples", "*.yaml"))
		Expect(err).NotTo(HaveOccurred())
		Expect(files).To(HaveLen(len(allSampleExpectations)))

		for i, file := range files {
			raw, readErr := os.ReadFile(file)
			Expect(readErr).NotTo(HaveOccurred(), file)

			sample := &upgradev1.HostedClusterUpgrade{}
			Expect(yaml.UnmarshalStrict(raw, sample)).To(Succeed(), file)

			wantDeleting := sample.DeletionTimestamp != nil
			wantGeneration := sample.Generation
			desiredStatus := sample.Status.DeepCopy()
			targetAfterBump := sample.Spec.TargetReleaseImage

			cr := &upgradev1.HostedClusterUpgrade{
				ObjectMeta: metav1.ObjectMeta{
					Name:        fmt.Sprintf("sample-%02d-%s", i+1, strings.TrimSuffix(filepath.Base(file), ".yaml")),
					Namespace:   ns.Name,
					Finalizers:  append([]string(nil), sample.Finalizers...),
					Annotations: sample.Annotations,
				},
				Spec: *sample.Spec.DeepCopy(),
			}
			if len(cr.Name) > 63 {
				cr.Name = cr.Name[:63]
			}
			cr.Name = strings.TrimRight(strings.ReplaceAll(cr.Name, "_", "-"), "-")

			if wantGeneration == 2 && !wantDeleting {
				cr.Spec.TargetReleaseImage = "registry.example.com/ocp-release/release:4.21.42-x86_64@sha256:2222222222222222222222222222222222222222222222222222222222222222"
			}

			Expect(k8sClient.Create(ctx, cr)).To(Succeed(), file)
			DeferCleanup(func(name string) func() {
				return func() {
					cleanupObj := &upgradev1.HostedClusterUpgrade{}
					if k8sClient.Get(ctx, client.ObjectKey{Namespace: ns.Name, Name: name}, cleanupObj) == nil {
						if len(cleanupObj.Finalizers) > 0 {
							cleanupObj.Finalizers = nil
							_ = k8sClient.Update(ctx, cleanupObj)
						}
						_ = k8sClient.Delete(ctx, cleanupObj)
					}
				}
			}(cr.Name))

			cr.Status = *desiredStatus.DeepCopy()
			Expect(k8sClient.Status().Update(ctx, cr)).To(Succeed(), file)

			if wantGeneration == 2 && !wantDeleting {
				Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(cr), cr)).To(Succeed(), file)
				cr.Spec.TargetReleaseImage = targetAfterBump
				Expect(k8sClient.Update(ctx, cr)).To(Succeed(), file)
			}
			if wantDeleting {
				Expect(k8sClient.Delete(ctx, cr)).To(Succeed(), file)
			}

			got := &upgradev1.HostedClusterUpgrade{}
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(cr), got)).To(Succeed(), file)
			Expect(got.Generation).To(Equal(wantGeneration), file)
			Expect(got.DeletionTimestamp != nil).To(Equal(wantDeleting), file)
			Expect(got.Status.Phase).To(Equal(desiredStatus.Phase), file)
			Expect(got.Status.ObservedGeneration).To(Equal(desiredStatus.ObservedGeneration), file)
			Expect(got.Status.Conditions).To(HaveLen(len(desiredStatus.Conditions)), file)
		}
	})
})
