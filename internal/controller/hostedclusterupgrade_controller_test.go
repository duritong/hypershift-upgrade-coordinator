/*
Copyright 2026 SRE Fleet Team.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
"metav1" was imported but not used.
*/

package controller

import (
	"context"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	upgradev1alpha1 "github.com/duritong/hypershift-upgrade-coordinator/api/v1alpha1"
)

var _ = Describe("HostedClusterUpgrade Controller Reconciler", func() {
	const (
		namespace = "default"
		timeout   = time.Second * 10
		interval  = time.Millisecond * 250
	)

	var (
		ctx context.Context
	)

	BeforeEach(func() {
		ctx = context.Background()
	})

	Context("Reconciler Status Aggregation & Watch pipeline", func() {
		It("should initialize state machine and aggregate ControlPlaneComponent statuses", func() {
			clusterName := "test-cluster-1"
			cpNamespace := fmt.Sprintf("%s-%s", namespace, clusterName)
			hcuKey := types.NamespacedName{Name: clusterName, Namespace: namespace}

			// 1. Create the HostedClusterUpgrade CR
			hcu := &upgradev1alpha1.HostedClusterUpgrade{
				ObjectMeta: metav1.ObjectMeta{
					Name:      clusterName,
					Namespace: namespace,
				},
				Spec: upgradev1alpha1.HostedClusterUpgradeSpec{
					TargetReleaseImage: "quay.io/ocp:4.22.2-x86_64@sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
					NodePools:          []upgradev1alpha1.NodePoolUpgradeSpec{},
					ControlPlane:       upgradev1alpha1.ControlPlaneUpgradeSpec{},
				},
			}

			Expect(k8sClient.Create(ctx, hcu)).To(Succeed())

			// Defer HCU deletion
			defer func() {
				_ = k8sClient.Delete(ctx, hcu)
			}()

			// 2. Verify state machine transitions to Idle
			Eventually(func() upgradev1alpha1.UpgradePhase {
				fetched := &upgradev1alpha1.HostedClusterUpgrade{}
				if err := k8sClient.Get(ctx, hcuKey, fetched); err != nil {
					return ""
				}
				return fetched.Status.Phase
			}, timeout, interval).Should(Equal(upgradev1alpha1.PhaseIdle))

			// 3. Create a mock ControlPlaneComponent (CPC) in the control plane namespace
			cpcName := "etcd"
			uCPC := &unstructured.Unstructured{}
			uCPC.SetGroupVersionKind(gvkCPC)
			uCPC.SetName(cpcName)
			uCPC.SetNamespace(cpNamespace)

			// Setup status fields on the unstructured CPC
			err := unstructured.SetNestedField(uCPC.Object, "4.20.44", "status", "version")
			Expect(err).NotTo(HaveOccurred())

			conditions := []interface{}{
				map[string]interface{}{
					"type":    "Available",
					"status":  "True",
					"message": "Etcd is ready",
				},
				map[string]interface{}{
					"type":    "RolloutComplete",
					"status":  "False",
					"message": "Etcd is rolling out",
				},
			}
			err = unstructured.SetNestedSlice(uCPC.Object, conditions, "status", "conditions")
			Expect(err).NotTo(HaveOccurred())

			// Ensure target namespace exists inside envtest
			ns := &unstructured.Unstructured{}
			ns.SetGroupVersionKind(schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Namespace"})
			ns.SetName(cpNamespace)
			_ = k8sClient.Create(ctx, ns)

			Expect(k8sClient.Create(ctx, uCPC)).To(Succeed())

			// Defer CPC and namespace deletion
			defer func() {
				_ = k8sClient.Delete(ctx, uCPC)
				_ = k8sClient.Delete(ctx, ns)
			}()

			// 4. Verify that the coordinator reconciles (triggered by the Watch) and aggregates the CPC status
			Eventually(func() int32 {
				fetched := &upgradev1alpha1.HostedClusterUpgrade{}
				if err := k8sClient.Get(ctx, hcuKey, fetched); err != nil {
					return -1
				}
				return fetched.Status.ControlPlaneStatus.TotalComponents
			}, timeout, interval).Should(Equal(int32(1)))

			Eventually(func() string {
				fetched := &upgradev1alpha1.HostedClusterUpgrade{}
				_ = k8sClient.Get(ctx, hcuKey, fetched)
				if len(fetched.Status.ControlPlaneStatus.Components) > 0 {
					return fetched.Status.ControlPlaneStatus.Components[0].Version
				}
				return ""
			}, timeout, interval).Should(Equal("4.20.44"))

			// 5. Mutate the CPC status (Available=True and RolloutComplete=True)
			conditions[1] = map[string]interface{}{
				"type":    "RolloutComplete",
				"status":  "True",
				"message": "Etcd rollout is complete",
			}
			err = unstructured.SetNestedSlice(uCPC.Object, conditions, "status", "conditions")
			Expect(err).NotTo(HaveOccurred())

			// We use standard k8sClient.Update because mock CRD does not have /status subresource enabled
			Expect(k8sClient.Update(ctx, uCPC)).To(Succeed())

			// Verify that upgraded components count updates automatically via the watch trigger
			Eventually(func() int32 {
				fetched := &upgradev1alpha1.HostedClusterUpgrade{}
				_ = k8sClient.Get(ctx, hcuKey, fetched)
				return fetched.Status.ControlPlaneStatus.UpgradedComponents
			}, timeout, interval).Should(Equal(int32(1)))
		})

		It("should enter paused phase when spec.pausedUntil is true but still continuously aggregate status", func() {
			clusterName := "test-cluster-2"
			cpNamespace := fmt.Sprintf("%s-%s", namespace, clusterName)
			hcuKey := types.NamespacedName{Name: clusterName, Namespace: namespace}
			pausedVal := "true"

			// Create a paused HostedClusterUpgrade
			hcu := &upgradev1alpha1.HostedClusterUpgrade{
				ObjectMeta: metav1.ObjectMeta{
					Name:      clusterName,
					Namespace: namespace,
				},
				Spec: upgradev1alpha1.HostedClusterUpgradeSpec{
					TargetReleaseImage: "quay.io/ocp:4.22.2-x86_64@sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
					NodePools:          []upgradev1alpha1.NodePoolUpgradeSpec{},
					ControlPlane:       upgradev1alpha1.ControlPlaneUpgradeSpec{},
					PausedUntil:        &pausedVal,
				},
			}

			Expect(k8sClient.Create(ctx, hcu)).To(Succeed())

			defer func() {
				_ = k8sClient.Delete(ctx, hcu)
			}()

			// Verify phase transitions to Paused
			Eventually(func() upgradev1alpha1.UpgradePhase {
				fetched := &upgradev1alpha1.HostedClusterUpgrade{}
				if err := k8sClient.Get(ctx, hcuKey, fetched); err != nil {
					return ""
				}
				return fetched.Status.Phase
			}, timeout, interval).Should(Equal(upgradev1alpha1.PhasePaused))

			// Create a mock ControlPlaneComponent (CPC) in the control plane namespace
			cpcName := "kube-apiserver"
			uCPC := &unstructured.Unstructured{}
			uCPC.SetGroupVersionKind(gvkCPC)
			uCPC.SetName(cpcName)
			uCPC.SetNamespace(cpNamespace)

			err := unstructured.SetNestedField(uCPC.Object, "4.21.0", "status", "version")
			Expect(err).NotTo(HaveOccurred())

			// Ensure namespace exists
			ns := &unstructured.Unstructured{}
			ns.SetGroupVersionKind(schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Namespace"})
			ns.SetName(cpNamespace)
			_ = k8sClient.Create(ctx, ns)

			Expect(k8sClient.Create(ctx, uCPC)).To(Succeed())

			defer func() {
				_ = k8sClient.Delete(ctx, uCPC)
				_ = k8sClient.Delete(ctx, ns)
			}()

			// Verify status still aggregates CPC counts in real-time, even though the operator is paused!
			Eventually(func() int32 {
				fetched := &upgradev1alpha1.HostedClusterUpgrade{}
				if err := k8sClient.Get(ctx, hcuKey, fetched); err != nil {
					return -1
				}
				return fetched.Status.ControlPlaneStatus.TotalComponents
			}, timeout, interval).Should(Equal(int32(1)))

			Eventually(func() string {
				fetched := &upgradev1alpha1.HostedClusterUpgrade{}
				_ = k8sClient.Get(ctx, hcuKey, fetched)
				if len(fetched.Status.ControlPlaneStatus.Components) > 0 {
					return fetched.Status.ControlPlaneStatus.Components[0].Version
				}
				return ""
			}, timeout, interval).Should(Equal("4.21.0"))
		})
	})
})
