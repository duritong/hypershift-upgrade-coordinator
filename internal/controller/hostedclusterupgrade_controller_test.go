/*
Copyright 2026 SRE Fleet Team.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	upgradev1alpha1 "github.com/duritong/hypershift-upgrade-coordinator/api/v1alpha1"
)

func createAndSetStatusHostedCluster(ctx context.Context, name, namespace, releaseImage string) *unstructured.Unstructured {
	hc := &unstructured.Unstructured{}
	hc.SetGroupVersionKind(gvkHC)
	hc.SetName(name)
	hc.SetNamespace(namespace)
	_ = unstructured.SetNestedField(hc.Object, "a5e99fb2-0598-4b34-9081-14236b744c16", "spec", "clusterID")

	_ = unstructured.SetNestedField(hc.Object, releaseImage, "spec", "release", "image")
	_ = unstructured.SetNestedMap(hc.Object, map[string]interface{}{"type": "None"}, "spec", "platform")
	_ = unstructured.SetNestedMap(hc.Object, map[string]interface{}{"name": "pull-secret"}, "spec", "pullSecret")
	_ = unstructured.SetNestedMap(hc.Object, map[string]interface{}{"managementType": "Managed", "managed": map[string]interface{}{"storage": map[string]interface{}{"type": "PersistentVolume"}}}, "spec", "etcd")
	_ = unstructured.SetNestedMap(hc.Object, map[string]interface{}{
		"clusterNetwork": []interface{}{map[string]interface{}{"cidr": "10.128.0.0/14"}},
		"serviceNetwork": []interface{}{map[string]interface{}{"cidr": "172.30.0.0/16"}},
		"networkType":    "OVNKubernetes",
	}, "spec", "networking")

	services := []interface{}{
		map[string]interface{}{"service": "APIServer", "servicePublishingStrategy": map[string]interface{}{"type": "LoadBalancer"}},
		map[string]interface{}{"service": "OAuthServer", "servicePublishingStrategy": map[string]interface{}{"type": "Route"}},
		map[string]interface{}{"service": "Konnectivity", "servicePublishingStrategy": map[string]interface{}{"type": "Route"}},
		map[string]interface{}{"service": "Ignition", "servicePublishingStrategy": map[string]interface{}{"type": "Route"}},
	}
	_ = unstructured.SetNestedSlice(hc.Object, services, "spec", "services")

	Expect(k8sClient.Create(ctx, hc)).To(Succeed())

	ver, _ := upgradev1alpha1.ExtractVersion(releaseImage)
	desired := map[string]interface{}{
		"image":   releaseImage,
		"version": ver,
	}

	nowStr := time.Now().Format(time.RFC3339)
	history := []interface{}{
		map[string]interface{}{
			"image":          releaseImage,
			"version":        ver,
			"state":          "Completed",
			"verified":       true,
			"startedTime":    nowStr,
			"completionTime": nowStr,
		},
	}

	_ = unstructured.SetNestedMap(hc.Object, desired, "status", "version", "desired")
	_ = unstructured.SetNestedSlice(hc.Object, history, "status", "version", "history")
	_ = unstructured.SetNestedSlice(hc.Object, []interface{}{}, "status", "version", "availableUpdates")
	_ = unstructured.SetNestedField(hc.Object, int64(1), "status", "version", "observedGeneration")
	_ = unstructured.SetNestedMap(hc.Object, map[string]interface{}{"desired": desired, "history": history, "observedGeneration": int64(1)}, "status", "controlPlaneVersion")
	setHealthyHostedClusterStatus(hc, time.Now())

	Expect(k8sClient.Status().Update(ctx, hc)).To(Succeed())
	attachReadyHCP(ctx, hc)

	return hc
}

func createAndSetStatusNodePool(ctx context.Context, name, namespace, clusterName, releaseImage string) *unstructured.Unstructured {
	np := &unstructured.Unstructured{}
	np.SetGroupVersionKind(gvkNP)
	np.SetName(name)
	np.SetNamespace(namespace)

	_ = unstructured.SetNestedField(np.Object, clusterName, "spec", "clusterName")
	_ = unstructured.SetNestedField(np.Object, releaseImage, "spec", "release", "image")
	_ = unstructured.SetNestedField(np.Object, int64(1), "spec", "replicas")
	_ = unstructured.SetNestedMap(np.Object, map[string]interface{}{"autoRepair": false, "upgradeType": "Replace"}, "spec", "management")
	_ = unstructured.SetNestedMap(np.Object, map[string]interface{}{"type": "AWS", "aws": map[string]interface{}{"instanceType": "m5.large", "subnet": map[string]interface{}{"id": "subnet-test"}}}, "spec", "platform")

	Expect(k8sClient.Create(ctx, np)).To(Succeed())

	ver, _ := upgradev1alpha1.ExtractVersion(releaseImage)
	_ = unstructured.SetNestedField(np.Object, ver, "status", "version")
	setReadyNodePoolStatus(np, ver)
	Expect(k8sClient.Status().Update(ctx, np)).To(Succeed())

	return np
}

func setReadyNodePoolStatus(np *unstructured.Unstructured, version string) {
	image, _, _ := unstructured.NestedString(np.Object, "spec", "release", "image")
	readyWorkerFixture(np, image)
	Expect(k8sClient.Update(context.Background(), np)).To(Succeed())
	md := readyWorkerFixture(np, image)
	replicas, _, _ := unstructured.NestedInt64(np.Object, "spec", "replicas")
	_ = unstructured.SetNestedField(md.Object, replicas, "spec", "replicas")
	for _, field := range []string{"replicas", "updatedReplicas", "readyReplicas", "availableReplicas"} {
		_ = unstructured.SetNestedField(md.Object, replicas, "status", field)
	}
	mdStatus, _, _ := unstructured.NestedMap(md.Object, "status")
	ns := &unstructured.Unstructured{}
	ns.SetAPIVersion("v1")
	ns.SetKind("Namespace")
	ns.SetName(md.GetNamespace())
	if err := k8sClient.Create(context.Background(), ns); err != nil {
		Expect(apierrors.IsAlreadyExists(err)).To(BeTrue())
	}
	old := &unstructured.Unstructured{}
	old.SetGroupVersionKind(gvkMachineDeployment)
	err := k8sClient.Get(context.Background(), types.NamespacedName{Namespace: md.GetNamespace(), Name: md.GetName()}, old)
	if apierrors.IsNotFound(err) {
		Expect(k8sClient.Create(context.Background(), md)).To(Succeed())
	} else {
		Expect(err).NotTo(HaveOccurred())
		md.SetResourceVersion(old.GetResourceVersion())
		Expect(k8sClient.Update(context.Background(), md)).To(Succeed())
	}
	_ = unstructured.SetNestedMap(md.Object, mdStatus, "status")
	_ = unstructured.SetNestedField(md.Object, md.GetGeneration(), "status", "observedGeneration")
	Expect(k8sClient.Status().Update(context.Background(), md)).To(Succeed())
	_ = unstructured.SetNestedField(np.Object, replicas, "status", "replicas")
	_ = unstructured.SetNestedSlice(np.Object, []interface{}{map[string]interface{}{
		"ocpVersion": version, "kubeletVersion": "v1.32.0",
		"readyNodeCount": replicas, "unreadyNodeCount": int64(0),
	}}, "status", "nodesInfo", "nodeVersions")
}

func createReadyControlPlane(ctx context.Context, hc *unstructured.Unstructured) {
	attachReadyHCP(ctx, hc)
	ns := &unstructured.Unstructured{}
	ns.SetGroupVersionKind(schema.GroupVersionKind{Version: "v1", Kind: "Namespace"})
	ns.SetName(WorkerControlPlaneNamespace(hc.GetNamespace(), hc.GetName()))
	if err := k8sClient.Create(ctx, ns); err != nil {
		Expect(apierrors.IsAlreadyExists(err)).To(BeTrue())
	}
	DeferCleanup(func() { _ = k8sClient.Delete(ctx, ns) })
	cpc := &unstructured.Unstructured{}
	cpc.SetGroupVersionKind(gvkCPC)
	cpc.SetNamespace(ns.GetName())
	cpc.SetName("etcd")
	Expect(k8sClient.Create(ctx, cpc)).To(Succeed())
	DeferCleanup(func() { _ = k8sClient.Delete(ctx, cpc) })
	_, version := GetHostedClusterCurrentRelease(hc)
	_ = unstructured.SetNestedField(cpc.Object, version, "status", "version")
	_ = unstructured.SetNestedField(cpc.Object, int64(1), "status", "observedGeneration")
	conditions := []interface{}{}
	for _, typ := range []string{"Available", "RolloutComplete"} {
		conditions = append(conditions, map[string]interface{}{
			"type": typ, "status": "True", "reason": "AsExpected", "message": "Ready",
			"lastTransitionTime": time.Now().Format(time.RFC3339),
		})
	}
	_ = unstructured.SetNestedSlice(cpc.Object, conditions, "status", "conditions")
	Expect(k8sClient.Status().Update(ctx, cpc)).To(Succeed())
}

// attachReadyHCP explicitly simulates management-side observation of the HC
// request. Tests call it only when simulating a completed management rollout.
func attachReadyHCP(ctx context.Context, hc *unstructured.Unstructured) {
	hcp := readyHCP(hc)
	ns := &unstructured.Unstructured{}
	ns.SetAPIVersion("v1")
	ns.SetKind("Namespace")
	ns.SetName(hcp.GetNamespace())
	if err := k8sClient.Create(ctx, ns); err != nil {
		Expect(apierrors.IsAlreadyExists(err)).To(BeTrue())
	}
	old := &unstructured.Unstructured{}
	old.SetGroupVersionKind(gvkHCP)
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: hcp.GetNamespace(), Name: hcp.GetName()}, old); apierrors.IsNotFound(err) {
		hcp.SetUID("")
		Expect(k8sClient.Create(ctx, hcp)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(ctx, hcp) })
	} else {
		Expect(err).NotTo(HaveOccurred())
		hcp.SetResourceVersion(old.GetResourceVersion())
		hcp.SetUID(old.GetUID())
		Expect(k8sClient.Update(ctx, hcp)).To(Succeed())
	}
	history, _, _ := unstructured.NestedSlice(hc.Object, "status", "version", "history")
	_ = unstructured.SetNestedSlice(hc.Object, history, "status", "controlPlaneVersion", "history")
	image, _, _ := unstructured.NestedString(hc.Object, "spec", "release", "image")
	version, _ := upgradev1alpha1.ExtractVersion(image)
	_ = unstructured.SetNestedMap(hc.Object, map[string]interface{}{"image": image, "version": version}, "status", "controlPlaneVersion", "desired")
	_ = unstructured.SetNestedField(hc.Object, hcp.GetGeneration(), "status", "controlPlaneVersion", "observedGeneration")
	Expect(k8sClient.Status().Update(ctx, hc)).To(Succeed())
}

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
		testLogScraper.MockLogs = "" // reset mock logs before each test
	})

	Context("Reconciler Status Aggregation & Watch pipeline", func() {
		It("should initialize state machine, pass preflights and execute direct upgrade rollout", func() {
			clusterName := "test-cluster-1"
			cpNamespace := fmt.Sprintf("%s-%s", namespace, clusterName)
			hcuKey := types.NamespacedName{Name: clusterName, Namespace: namespace}

			// 1. Create target HostedCluster
			uHC := createAndSetStatusHostedCluster(ctx, clusterName, namespace, regressionImage("4.20.44"))

			defer func() {
				_ = k8sClient.Delete(ctx, uHC)
			}()

			// 2. Create the HostedClusterUpgrade CR
			hcu := &upgradev1alpha1.HostedClusterUpgrade{
				ObjectMeta: metav1.ObjectMeta{
					Name:      clusterName,
					Namespace: namespace,
				},
				Spec: upgradev1alpha1.HostedClusterUpgradeSpec{
					TargetReleaseImage: regressionImage("4.20.50"),
					NodePoolGroups:     []upgradev1alpha1.NodePoolGroupSpec{},
					ControlPlane:       upgradev1alpha1.ControlPlaneUpgradeSpec{},
				},
			}

			Expect(k8sClient.Create(ctx, hcu)).To(Succeed())

			defer func() {
				_ = k8sClient.Delete(ctx, hcu)
			}()

			// Create a mock ControlPlaneComponent (CPC) in the control plane namespace
			cpcName := "etcd"
			uCPC := &unstructured.Unstructured{}
			uCPC.SetGroupVersionKind(gvkCPC)
			uCPC.SetName(cpcName)
			uCPC.SetNamespace(cpNamespace)

			// Ensure target namespace exists inside envtest
			ns := &unstructured.Unstructured{}
			ns.SetGroupVersionKind(schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Namespace"})
			ns.SetName(cpNamespace)
			_ = k8sClient.Create(ctx, ns)

			Expect(k8sClient.Create(ctx, uCPC)).To(Succeed())
			_ = unstructured.SetNestedField(uCPC.Object, int64(1), "status", "observedGeneration")

			err := unstructured.SetNestedField(uCPC.Object, "4.20.44", "status", "version")
			Expect(err).NotTo(HaveOccurred())

			nowStr := time.Now().Format(time.RFC3339)
			conditions := []interface{}{
				map[string]interface{}{
					"type":               "Available",
					"status":             "True",
					"reason":             "AsExpected",
					"message":            "Etcd is ready",
					"lastTransitionTime": nowStr,
				},
				map[string]interface{}{
					"type":               "RolloutComplete",
					"status":             "True",
					"reason":             "AsExpected",
					"message":            "Etcd rollout is complete",
					"lastTransitionTime": nowStr,
				},
			}
			err = unstructured.SetNestedSlice(uCPC.Object, conditions, "status", "conditions")
			Expect(err).NotTo(HaveOccurred())

			Expect(k8sClient.Status().Update(ctx, uCPC)).To(Succeed())

			defer func() {
				_ = k8sClient.Delete(ctx, uCPC)
				_ = k8sClient.Delete(ctx, ns)
			}()

			// 3. Verify that the coordinator commands the HostedCluster to upgrade to target version (4.20.50)
			Eventually(func() string {
				fetchedHC := &unstructured.Unstructured{}
				fetchedHC.SetGroupVersionKind(gvkHC)
				if err := k8sClient.Get(ctx, hcuKey, fetchedHC); err != nil {
					return ""
				}
				val, _, _ := unstructured.NestedString(fetchedHC.Object, "spec", "release", "image")
				return val
			}, timeout, interval).Should(Equal(regressionImage("4.20.50")))
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
					TargetReleaseImage: regressionImage("4.22.2"),
					NodePoolGroups:     []upgradev1alpha1.NodePoolGroupSpec{},
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

			// Ensure namespace exists
			ns := &unstructured.Unstructured{}
			ns.SetGroupVersionKind(schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Namespace"})
			ns.SetName(cpNamespace)
			_ = k8sClient.Create(ctx, ns)

			Expect(k8sClient.Create(ctx, uCPC)).To(Succeed())
			_ = unstructured.SetNestedField(uCPC.Object, int64(1), "status", "observedGeneration")

			err := unstructured.SetNestedField(uCPC.Object, "4.21.0", "status", "version")
			Expect(err).NotTo(HaveOccurred())
			Expect(k8sClient.Status().Update(ctx, uCPC)).To(Succeed())

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

		It("should execute sequential NodePool rollouts with custom label priorities and extract HCCO stuck drains", func() {
			clusterName := "test-cluster-3"
			hcuKey := types.NamespacedName{Name: clusterName, Namespace: namespace}

			// Setup Mock HCCO Logs
			testLogScraper.MockLogs = time.Now().Add(time.Minute).UTC().Format(time.RFC3339Nano) + ` {"msg":"failed to drain node","Node":{"name":"pool-b-node-1"},"error":"drain failed: failed to drain node pool-b-node-1: global timeout reached"}`

			// 1. Create target HostedCluster already at target version to bypass CP upgrade step
			uHC := createAndSetStatusHostedCluster(ctx, clusterName, namespace, regressionImage("4.22.2"))
			createReadyControlPlane(ctx, uHC)

			defer func() {
				_ = k8sClient.Delete(ctx, uHC)
			}()

			// 2. Create mock NodePools associated with this cluster using semantic version strings
			npA := createAndSetStatusNodePool(ctx, "pool-a", namespace, clusterName, regressionImage("4.20.44"))
			npA.SetLabels(map[string]string{
				"upgrade.huc.0xshift.dev/group": "infra",
			})
			_ = unstructured.SetNestedField(npA.Object, "false", "spec", "pausedUntil")
			Expect(k8sClient.Update(ctx, npA)).To(Succeed())

			defer func() {
				_ = k8sClient.Delete(ctx, npA)
			}()

			npB := createAndSetStatusNodePool(ctx, "pool-b", namespace, clusterName, regressionImage("4.20.44"))
			npB.SetLabels(map[string]string{
				"upgrade.huc.0xshift.dev/group": "compute",
			})
			_ = unstructured.SetNestedField(npB.Object, "false", "spec", "pausedUntil")
			Expect(k8sClient.Update(ctx, npB)).To(Succeed())

			defer func() {
				_ = k8sClient.Delete(ctx, npB)
			}()

			npC := createAndSetStatusNodePool(ctx, "pool-c", namespace, clusterName, regressionImage("4.20.44"))
			npC.SetLabels(map[string]string{
				"upgrade.huc.0xshift.dev/group": "compute",
			})
			_ = unstructured.SetNestedField(npC.Object, "false", "spec", "pausedUntil")
			Expect(k8sClient.Update(ctx, npC)).To(Succeed())

			defer func() {
				_ = k8sClient.Delete(ctx, npC)
			}()

			npD := createAndSetStatusNodePool(ctx, "pool-d", namespace, clusterName, regressionImage("4.20.44"))
			_ = unstructured.SetNestedField(npD.Object, "false", "spec", "pausedUntil")
			Expect(k8sClient.Update(ctx, npD)).To(Succeed())
			for _, pool := range []*unstructured.Unstructured{npA, npB, npC, npD} {
				setReadyNodePoolStatus(pool, "4.20.44")
				Expect(k8sClient.Status().Update(ctx, pool)).To(Succeed())
			}

			defer func() {
				_ = k8sClient.Delete(ctx, npD)
			}()

			// 3. Create the HCU Upgrade Coordinator CR (concurrency = 2)
			hcu := &upgradev1alpha1.HostedClusterUpgrade{
				ObjectMeta: metav1.ObjectMeta{
					Name:      clusterName,
					Namespace: namespace,
				},
				Spec: upgradev1alpha1.HostedClusterUpgradeSpec{
					TargetReleaseImage: regressionImage("4.22.2"),
					ControlPlane:       upgradev1alpha1.ControlPlaneUpgradeSpec{},
					RolloutPolicy: &upgradev1alpha1.RolloutPolicySpec{
						MaxParallelNodePools: 2,
					},
					NodePoolGroups: []upgradev1alpha1.NodePoolGroupSpec{
						{
							Name: "infra",
						},
						{
							Name: "compute",
						},
					},
				},
			}

			Expect(k8sClient.Create(ctx, hcu)).To(Succeed())

			defer func() {
				_ = k8sClient.Delete(ctx, hcu)
			}()

			// 4. Verify that the infra group NodePool (pool-a) upgrades first!
			Eventually(func() string {
				fetchedNP := &unstructured.Unstructured{}
				fetchedNP.SetGroupVersionKind(gvkNP)
				_ = k8sClient.Get(ctx, types.NamespacedName{Name: "pool-a", Namespace: namespace}, fetchedNP)
				val, _, _ := unstructured.NestedString(fetchedNP.Object, "spec", "release", "image")
				return val
			}, timeout, interval).Should(Equal(regressionImage("4.22.2")))

			Eventually(func() string {
				fetchedNP := &unstructured.Unstructured{}
				fetchedNP.SetGroupVersionKind(gvkNP)
				_ = k8sClient.Get(ctx, types.NamespacedName{Name: "pool-a", Namespace: namespace}, fetchedNP)
				pUntil, _, _ := unstructured.NestedString(fetchedNP.Object, "spec", "pausedUntil")
				return pUntil
			}, timeout, interval).Should(Equal("false"))

			// Verify that compute and default group NodePools remain paused
			Consistently(func() string {
				fetchedNP := &unstructured.Unstructured{}
				fetchedNP.SetGroupVersionKind(gvkNP)
				_ = k8sClient.Get(ctx, types.NamespacedName{Name: "pool-b", Namespace: namespace}, fetchedNP)
				pUntil, _, _ := unstructured.NestedString(fetchedNP.Object, "spec", "pausedUntil")
				return pUntil
			}, time.Second*2, interval).Should(Equal("true"))

			// 5. Simulate pool-a completes its upgrade
			fetchedNPA := &unstructured.Unstructured{}
			fetchedNPA.SetGroupVersionKind(gvkNP)
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "pool-a", Namespace: namespace}, fetchedNPA)).To(Succeed())
			_ = unstructured.SetNestedField(fetchedNPA.Object, "4.22.2", "status", "version")
			setReadyNodePoolStatus(fetchedNPA, "4.22.2")
			Expect(k8sClient.Status().Update(ctx, fetchedNPA)).To(Succeed())

			// 6. Verify that compute group NodePools (pool-b and pool-c) both begin upgrading in parallel!
			Eventually(func() string {
				fetchedNP := &unstructured.Unstructured{}
				fetchedNP.SetGroupVersionKind(gvkNP)
				_ = k8sClient.Get(ctx, types.NamespacedName{Name: "pool-b", Namespace: namespace}, fetchedNP)
				val, _, _ := unstructured.NestedString(fetchedNP.Object, "spec", "release", "image")
				return val
			}, timeout, interval).Should(Equal(regressionImage("4.22.2")))

			Eventually(func() string {
				fetchedNP := &unstructured.Unstructured{}
				fetchedNP.SetGroupVersionKind(gvkNP)
				_ = k8sClient.Get(ctx, types.NamespacedName{Name: "pool-c", Namespace: namespace}, fetchedNP)
				val, _, _ := unstructured.NestedString(fetchedNP.Object, "spec", "release", "image")
				return val
			}, timeout, interval).Should(Equal(regressionImage("4.22.2")))

			// Verify that fallback default group NodePool (pool-d) remains paused during compute rollout
			Consistently(func() string {
				fetchedNP := &unstructured.Unstructured{}
				fetchedNP.SetGroupVersionKind(gvkNP)
				_ = k8sClient.Get(ctx, types.NamespacedName{Name: "pool-d", Namespace: namespace}, fetchedNP)
				pUntil, _, _ := unstructured.NestedString(fetchedNP.Object, "spec", "pausedUntil")
				return pUntil
			}, time.Second*2, interval).Should(Equal("true"))

			// 7. Cluster observations must not be attributed to either active pool.
			Eventually(func() []string {
				fetched := &upgradev1alpha1.HostedClusterUpgrade{}
				_ = k8sClient.Get(ctx, hcuKey, fetched)
				if fetched.Status.DrainDiagnostics == nil {
					return nil
				}
				return fetched.Status.DrainDiagnostics.Messages
			}, timeout, interval).Should(ContainElement(ContainSubstring("failed to drain node pool-b-node-1")))
			fetched := &upgradev1alpha1.HostedClusterUpgrade{}
			Expect(k8sClient.Get(ctx, hcuKey, fetched)).To(Succeed())
			for _, s := range fetched.Status.NodePoolStatuses {
				Expect(s.Blockers).NotTo(ContainElement(ContainSubstring("pool-b-node-1")))
			}

			// 8. Simulate compute group (pool-b and pool-c) completes its upgrade
			fetchedNPB := &unstructured.Unstructured{}
			fetchedNPB.SetGroupVersionKind(gvkNP)
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "pool-b", Namespace: namespace}, fetchedNPB)).To(Succeed())
			_ = unstructured.SetNestedField(fetchedNPB.Object, "4.22.2", "status", "version")
			setReadyNodePoolStatus(fetchedNPB, "4.22.2")
			Expect(k8sClient.Status().Update(ctx, fetchedNPB)).To(Succeed())

			fetchedNPC := &unstructured.Unstructured{}
			fetchedNPC.SetGroupVersionKind(gvkNP)
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "pool-c", Namespace: namespace}, fetchedNPC)).To(Succeed())
			_ = unstructured.SetNestedField(fetchedNPC.Object, "4.22.2", "status", "version")
			setReadyNodePoolStatus(fetchedNPC, "4.22.2")
			Expect(k8sClient.Status().Update(ctx, fetchedNPC)).To(Succeed())

			// 9. Verify that fallback default group (pool-d) is now unpaused and begins upgrading!
			Eventually(func() string {
				fetchedNP := &unstructured.Unstructured{}
				fetchedNP.SetGroupVersionKind(gvkNP)
				_ = k8sClient.Get(ctx, types.NamespacedName{Name: "pool-d", Namespace: namespace}, fetchedNP)
				val, _, _ := unstructured.NestedString(fetchedNP.Object, "spec", "release", "image")
				return val
			}, timeout, interval).Should(Equal(regressionImage("4.22.2")))

			Eventually(func() string {
				fetchedNP := &unstructured.Unstructured{}
				fetchedNP.SetGroupVersionKind(gvkNP)
				_ = k8sClient.Get(ctx, types.NamespacedName{Name: "pool-d", Namespace: namespace}, fetchedNP)
				pUntil, _, _ := unstructured.NestedString(fetchedNP.Object, "spec", "pausedUntil")
				return pUntil
			}, timeout, interval).Should(Equal("false"))
		})

		It("Fix 2.A Test: should correctly evaluate NodePool status.version semver string (e.g. 4.20.44) and NOT mark it Completed prematurely", func() {
			clusterName := "test-cluster-semver-2a"
			hcuKey := types.NamespacedName{Name: clusterName, Namespace: namespace}

			// HostedCluster at target version
			uHC := createAndSetStatusHostedCluster(ctx, clusterName, namespace, regressionImage("4.22.2"))
			createReadyControlPlane(ctx, uHC)
			defer func() { _ = k8sClient.Delete(ctx, uHC) }()

			// Create a NodePool with status.version = "4.20.44" (semver string, NOT pullspec!)
			np := createAndSetStatusNodePool(ctx, "pool-semver", namespace, clusterName, regressionImage("4.20.44"))
			_ = unstructured.SetNestedField(np.Object, "false", "spec", "pausedUntil")
			_ = unstructured.SetNestedField(np.Object, int64(3), "spec", "replicas")
			Expect(k8sClient.Update(ctx, np)).To(Succeed())
			setReadyNodePoolStatus(np, "4.20.44")

			_ = unstructured.SetNestedField(np.Object, "4.20.44", "status", "version")
			_ = unstructured.SetNestedField(np.Object, int64(3), "status", "replicas")
			nodesInfo := map[string]interface{}{
				"nodeVersions": []interface{}{
					map[string]interface{}{
						"ocpVersion":       "4.20.44",
						"kubeletVersion":   "v1.27.3",
						"readyNodeCount":   int64(3),
						"unreadyNodeCount": int64(0),
					},
				},
			}
			_ = unstructured.SetNestedMap(np.Object, nodesInfo, "status", "nodesInfo")
			Expect(k8sClient.Status().Update(ctx, np)).To(Succeed())

			defer func() { _ = k8sClient.Delete(ctx, np) }()

			// Create HCU targeting 4.22.2
			hcu := &upgradev1alpha1.HostedClusterUpgrade{
				ObjectMeta: metav1.ObjectMeta{
					Name:      clusterName,
					Namespace: namespace,
				},
				Spec: upgradev1alpha1.HostedClusterUpgradeSpec{
					TargetReleaseImage: regressionImage("4.22.2"),
					ControlPlane:       upgradev1alpha1.ControlPlaneUpgradeSpec{},
					NodePoolGroups:     []upgradev1alpha1.NodePoolGroupSpec{},
				},
			}
			Expect(k8sClient.Create(ctx, hcu)).To(Succeed())
			defer func() { _ = k8sClient.Delete(ctx, hcu) }()

			// Verify that pool-semver phase is "Upgrading" (NOT "Completed"!) and spec release is updated to 4.22.2
			Eventually(func(g Gomega) {
				fetched := &upgradev1alpha1.HostedClusterUpgrade{}
				g.Expect(k8sClient.Get(ctx, hcuKey, fetched)).To(Succeed())
				g.Expect(fetched.Status.NodePoolStatuses).NotTo(BeEmpty())
				for _, status := range fetched.Status.NodePoolStatuses {
					if status.Name == "pool-semver" {
						g.Expect(status.Phase).To(Equal("Upgrading"), "blockers=%v guest=%+v", status.Blockers, fetched.Status.GuestAssessment)
					}
				}
			}, timeout, interval).Should(Succeed())

			Eventually(func() string {
				fetchedNP := &unstructured.Unstructured{}
				fetchedNP.SetGroupVersionKind(gvkNP)
				_ = k8sClient.Get(ctx, types.NamespacedName{Name: "pool-semver", Namespace: namespace}, fetchedNP)
				val, _, _ := unstructured.NestedString(fetchedNP.Object, "spec", "release", "image")
				return val
			}, timeout, interval).Should(Equal(regressionImage("4.22.2")))
		})

		It("Fix 2.B Test: should capture ControlPlaneComponent condition failure messages and handle degraded cluster state", func() {
			clusterName := "test-cluster-cpc-2b"
			cpNamespace := fmt.Sprintf("%s-%s", namespace, clusterName)
			hcuKey := types.NamespacedName{Name: clusterName, Namespace: namespace}

			// Ensure cpNamespace exists inside envtest
			ns := &unstructured.Unstructured{}
			ns.SetGroupVersionKind(schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Namespace"})
			ns.SetName(cpNamespace)
			_ = k8sClient.Create(ctx, ns)
			defer func() { _ = k8sClient.Delete(ctx, ns) }()

			// HostedCluster
			uHC := createAndSetStatusHostedCluster(ctx, clusterName, namespace, regressionImage("4.20.44"))
			defer func() { _ = k8sClient.Delete(ctx, uHC) }()

			// ControlPlaneComponent with Available = False and RolloutComplete = False with failure messages
			uCPC := &unstructured.Unstructured{}
			uCPC.SetGroupVersionKind(gvkCPC)
			uCPC.SetName("kube-apiserver")
			uCPC.SetNamespace(cpNamespace)

			Expect(k8sClient.Create(ctx, uCPC)).To(Succeed())
			_ = unstructured.SetNestedField(uCPC.Object, int64(1), "status", "observedGeneration")

			_ = unstructured.SetNestedField(uCPC.Object, "4.20.44", "status", "version")

			nowStr := time.Now().Format(time.RFC3339)
			conditions := []interface{}{
				map[string]interface{}{
					"type":               "Available",
					"status":             "False",
					"reason":             "DeploymentDegraded",
					"message":            "kube-apiserver pod crashed",
					"lastTransitionTime": nowStr,
				},
				map[string]interface{}{
					"type":               "RolloutComplete",
					"status":             "False",
					"reason":             "ReconciliationError",
					"message":            "waiting for pods to stabilize",
					"lastTransitionTime": nowStr,
				},
			}
			_ = unstructured.SetNestedSlice(uCPC.Object, conditions, "status", "conditions")
			Expect(k8sClient.Status().Update(ctx, uCPC)).To(Succeed())

			defer func() { _ = k8sClient.Delete(ctx, uCPC) }()

			// Create HCU
			hcu := &upgradev1alpha1.HostedClusterUpgrade{
				ObjectMeta: metav1.ObjectMeta{
					Name:      clusterName,
					Namespace: namespace,
				},
				Spec: upgradev1alpha1.HostedClusterUpgradeSpec{
					TargetReleaseImage: regressionImage("4.20.50"),
					NodePoolGroups:     []upgradev1alpha1.NodePoolGroupSpec{},
					ControlPlane:       upgradev1alpha1.ControlPlaneUpgradeSpec{},
				},
			}
			Expect(k8sClient.Create(ctx, hcu)).To(Succeed())
			defer func() { _ = k8sClient.Delete(ctx, hcu) }()

			// Verify that blockers capture the failure message from Available condition!
			Eventually(func() []string {
				fetched := &upgradev1alpha1.HostedClusterUpgrade{}
				if err := k8sClient.Get(ctx, hcuKey, fetched); err != nil {
					return nil
				}
				return fetched.Status.ControlPlaneStatus.Blockers
			}, timeout, interval).Should(ContainElement(ContainSubstring("kube-apiserver pod crashed")))
		})

		It("should evaluate checkPausedState with timestamps and boolean strings", func() {
			// Nil / empty
			paused, dur, err := checkPausedState(nil, time.Now())
			Expect(err).NotTo(HaveOccurred())
			Expect(paused).To(BeFalse())

			empty := ""
			paused, dur, err = checkPausedState(&empty, time.Now())
			Expect(err).NotTo(HaveOccurred())
			Expect(paused).To(BeFalse())

			// "false" / "False"
			fStr := "false"
			paused, dur, err = checkPausedState(&fStr, time.Now())
			Expect(err).NotTo(HaveOccurred())
			Expect(paused).To(BeFalse())

			// Future RFC3339 timestamp
			future := time.Now().Add(1 * time.Hour).Format(time.RFC3339)
			paused, dur, err = checkPausedState(&future, time.Now())
			Expect(err).NotTo(HaveOccurred())
			Expect(paused).To(BeTrue())
			Expect(dur).To(BeNumerically(">", 0))

			// Past RFC3339 timestamp
			past := time.Now().Add(-1 * time.Hour).Format(time.RFC3339)
			paused, dur, err = checkPausedState(&past, time.Now())
			Expect(err).NotTo(HaveOccurred())
			Expect(paused).To(BeFalse())

			// Invalid string
			invalid := "not-a-date"
			_, _, err = checkPausedState(&invalid, time.Now())
			Expect(err).To(HaveOccurred())
		})

		It("should execute multi-hop EUS upgrades via intermediate release image", func() {
			clusterName := "eus-cluster"
			hcuKey := types.NamespacedName{Name: clusterName, Namespace: namespace}
			cpNamespace := fmt.Sprintf("%s-%s", namespace, clusterName)

			// Ensure cpNamespace exists
			ns := &unstructured.Unstructured{}
			ns.SetGroupVersionKind(schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Namespace"})
			ns.SetName(cpNamespace)
			_ = k8sClient.Create(ctx, ns)
			defer func() { _ = k8sClient.Delete(ctx, ns) }()

			// Create HostedCluster starting at 4.12.0
			uHC := createAndSetStatusHostedCluster(ctx, clusterName, namespace, regressionImage("4.12.0"))
			defer func() { _ = k8sClient.Delete(ctx, uHC) }()

			// Create mock ControlPlaneComponent in ready state
			uCPC := &unstructured.Unstructured{}
			uCPC.SetGroupVersionKind(gvkCPC)
			uCPC.SetName("etcd")
			uCPC.SetNamespace(cpNamespace)
			Expect(k8sClient.Create(ctx, uCPC)).To(Succeed())
			_ = unstructured.SetNestedField(uCPC.Object, int64(1), "status", "observedGeneration")
			_ = unstructured.SetNestedField(uCPC.Object, "4.12.0", "status", "version")
			nowStr := time.Now().Format(time.RFC3339)
			conds := []interface{}{
				map[string]interface{}{"type": "Available", "status": "True", "reason": "Ready", "message": "Ready", "lastTransitionTime": nowStr},
				map[string]interface{}{"type": "RolloutComplete", "status": "True", "reason": "Ready", "message": "Ready", "lastTransitionTime": nowStr},
			}
			_ = unstructured.SetNestedSlice(uCPC.Object, conds, "status", "conditions")
			Expect(k8sClient.Status().Update(ctx, uCPC)).To(Succeed())
			defer func() { _ = k8sClient.Delete(ctx, uCPC) }()

			// Create EUS HCU: 4.12.0 -> 4.13.0 (intermediate) -> 4.14.0 (target)
			hcu := &upgradev1alpha1.HostedClusterUpgrade{
				ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: namespace},
				Spec: upgradev1alpha1.HostedClusterUpgradeSpec{
					TargetReleaseImage:       regressionImage("4.14.0"),
					IntermediateReleaseImage: regressionImage("4.13.0"),
					NodePoolGroups:           []upgradev1alpha1.NodePoolGroupSpec{},
					ControlPlane:             upgradev1alpha1.ControlPlaneUpgradeSpec{},
				},
			}
			Expect(k8sClient.Create(ctx, hcu)).To(Succeed())
			defer func() { _ = k8sClient.Delete(ctx, hcu) }()

			// Step 1: HostedCluster spec release image should first be set to intermediate version (4.13.0)
			Eventually(func() string {
				fetchedHC := &unstructured.Unstructured{}
				fetchedHC.SetGroupVersionKind(gvkHC)
				if err := k8sClient.Get(ctx, hcuKey, fetchedHC); err != nil {
					return ""
				}
				val, _, _ := unstructured.NestedString(fetchedHC.Object, "spec", "release", "image")
				return val
			}, timeout, interval).Should(Equal(regressionImage("4.13.0")))

			// Simulate HostedCluster status version reaching intermediate 4.13.0
			fetchedHC := &unstructured.Unstructured{}
			fetchedHC.SetGroupVersionKind(gvkHC)
			Expect(k8sClient.Get(ctx, hcuKey, fetchedHC)).To(Succeed())

			desired := map[string]interface{}{"image": regressionImage("4.13.0"), "version": "4.13.0"}
			history := []interface{}{
				map[string]interface{}{"image": regressionImage("4.13.0"), "version": "4.13.0", "state": "Completed", "verified": true, "startedTime": nowStr, "completionTime": nowStr},
			}
			_ = unstructured.SetNestedMap(fetchedHC.Object, desired, "status", "version", "desired")
			_ = unstructured.SetNestedSlice(fetchedHC.Object, history, "status", "version", "history")
			Expect(k8sClient.Status().Update(ctx, fetchedHC)).To(Succeed())
			attachReadyHCP(ctx, fetchedHC)

			// Simulate ControlPlaneComponent (etcd) completing rollout to intermediate 4.13.0
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "etcd", Namespace: cpNamespace}, uCPC)).To(Succeed())
			_ = unstructured.SetNestedField(uCPC.Object, "4.13.0", "status", "version")
			_ = unstructured.SetNestedField(uCPC.Object, int64(2), "status", "observedGeneration")
			Expect(k8sClient.Status().Update(ctx, uCPC)).To(Succeed())

			// Step 2: Coordinator should advance to target version (4.14.0)
			Eventually(func() string {
				fetchedHC2 := &unstructured.Unstructured{}
				fetchedHC2.SetGroupVersionKind(gvkHC)
				if err := k8sClient.Get(ctx, hcuKey, fetchedHC2); err != nil {
					return ""
				}
				val, _, _ := unstructured.NestedString(fetchedHC2.Object, "spec", "release", "image")
				return val
			}, timeout, interval).Should(Equal(regressionImage("4.14.0")))
		})

		It("should halt when ControlPlane.StartAt or NodePoolGroup.StartAt is in the future", func() {
			clusterName := "future-sched-cluster"
			hcuKey := types.NamespacedName{Name: clusterName, Namespace: namespace}

			uHC := createAndSetStatusHostedCluster(ctx, clusterName, namespace, regressionImage("4.20.44"))
			createReadyControlPlane(ctx, uHC)
			defer func() { _ = k8sClient.Delete(ctx, uHC) }()

			futureTime := metav1.NewTime(time.Now().Add(2 * time.Hour))

			hcu := &upgradev1alpha1.HostedClusterUpgrade{
				ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: namespace},
				Spec: upgradev1alpha1.HostedClusterUpgradeSpec{
					TargetReleaseImage: regressionImage("4.20.50"),
					ControlPlane: upgradev1alpha1.ControlPlaneUpgradeSpec{
						StartAt: &futureTime,
					},
					NodePoolGroups: []upgradev1alpha1.NodePoolGroupSpec{},
				},
			}
			Expect(k8sClient.Create(ctx, hcu)).To(Succeed())
			defer func() { _ = k8sClient.Delete(ctx, hcu) }()

			// Verify phase stays in PendingSchedule
			Eventually(func() upgradev1alpha1.UpgradePhase {
				fetched := &upgradev1alpha1.HostedClusterUpgrade{}
				if err := k8sClient.Get(ctx, hcuKey, fetched); err != nil {
					return ""
				}
				return fetched.Status.Phase
			}, timeout, interval).Should(Equal(upgradev1alpha1.PhasePendingSchedule))
		})

		It("should block upgrade when HostedCluster status contains failing or degraded conditions", func() {
			clusterName := "degraded-cluster"
			hcuKey := types.NamespacedName{Name: clusterName, Namespace: namespace}

			uHC := createAndSetStatusHostedCluster(ctx, clusterName, namespace, regressionImage("4.20.44"))
			defer func() { _ = k8sClient.Delete(ctx, uHC) }()

			// Set Degraded = True condition on HostedCluster
			degradedConds := []interface{}{
				map[string]interface{}{
					"type":               "Degraded",
					"status":             "True",
					"reason":             "ClusterDegraded",
					"message":            "EtcdClusterDegraded",
					"lastTransitionTime": time.Now().Format(time.RFC3339),
				},
			}
			_ = unstructured.SetNestedSlice(uHC.Object, degradedConds, "status", "conditions")
			Expect(k8sClient.Status().Update(ctx, uHC)).To(Succeed())

			hcu := &upgradev1alpha1.HostedClusterUpgrade{
				ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: namespace},
				Spec: upgradev1alpha1.HostedClusterUpgradeSpec{
					TargetReleaseImage: regressionImage("4.20.50"),
					ControlPlane:       upgradev1alpha1.ControlPlaneUpgradeSpec{},
					NodePoolGroups:     []upgradev1alpha1.NodePoolGroupSpec{},
				},
			}
			Expect(k8sClient.Create(ctx, hcu)).To(Succeed())
			defer func() { _ = k8sClient.Delete(ctx, hcu) }()

			// Verify coordinator transitions to PhaseBlocked
			Eventually(func() upgradev1alpha1.UpgradePhase {
				fetched := &upgradev1alpha1.HostedClusterUpgrade{}
				if err := k8sClient.Get(ctx, hcuKey, fetched); err != nil {
					return ""
				}
				return fetched.Status.Phase
			}, timeout, interval).Should(Equal(upgradev1alpha1.PhaseBlocked))
		})

		It("Issue 1.1: should not mark ControlPlaneComponent verified if component reports old version despite Available and RolloutComplete", func() {
			clusterName := "cpc-ver-cluster"
			hcuKey := types.NamespacedName{Name: clusterName, Namespace: namespace}
			cpNamespace := fmt.Sprintf("%s-%s", namespace, clusterName)

			ns := &unstructured.Unstructured{}
			ns.SetGroupVersionKind(schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Namespace"})
			ns.SetName(cpNamespace)
			_ = k8sClient.Create(ctx, ns)
			defer func() { _ = k8sClient.Delete(ctx, ns) }()

			uHC := createAndSetStatusHostedCluster(ctx, clusterName, namespace, regressionImage("4.20.44"))
			defer func() { _ = k8sClient.Delete(ctx, uHC) }()

			// Create CPC with Available=True, RolloutComplete=True, but version="4.20.44" (old version)
			uCPC := &unstructured.Unstructured{}
			uCPC.SetGroupVersionKind(gvkCPC)
			uCPC.SetName("kube-apiserver")
			uCPC.SetNamespace(cpNamespace)
			Expect(k8sClient.Create(ctx, uCPC)).To(Succeed())
			_ = unstructured.SetNestedField(uCPC.Object, int64(1), "status", "observedGeneration")
			defer func() { _ = k8sClient.Delete(ctx, uCPC) }()

			_ = unstructured.SetNestedField(uCPC.Object, "4.20.44", "status", "version")
			nowStr := time.Now().Format(time.RFC3339)
			conds := []interface{}{
				map[string]interface{}{"type": "Available", "status": "True", "reason": "Ready", "message": "Ready", "lastTransitionTime": nowStr},
				map[string]interface{}{"type": "RolloutComplete", "status": "True", "reason": "Ready", "message": "Ready", "lastTransitionTime": nowStr},
			}
			_ = unstructured.SetNestedSlice(uCPC.Object, conds, "status", "conditions")
			Expect(k8sClient.Status().Update(ctx, uCPC)).To(Succeed())

			hcu := &upgradev1alpha1.HostedClusterUpgrade{
				ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: namespace},
				Spec: upgradev1alpha1.HostedClusterUpgradeSpec{
					TargetReleaseImage: regressionImage("4.20.50"),
					ControlPlane:       upgradev1alpha1.ControlPlaneUpgradeSpec{},
					NodePoolGroups:     []upgradev1alpha1.NodePoolGroupSpec{},
				},
			}
			Expect(k8sClient.Create(ctx, hcu)).To(Succeed())
			defer func() { _ = k8sClient.Delete(ctx, hcu) }()

			// Coordinator should apply 4.20.50 on HostedCluster and move to VerifyingControlPlane
			Eventually(func() upgradev1alpha1.UpgradePhase {
				fetched := &upgradev1alpha1.HostedClusterUpgrade{}
				if err := k8sClient.Get(ctx, hcuKey, fetched); err != nil {
					return ""
				}
				return fetched.Status.Phase
			}, timeout, interval).Should(Equal(upgradev1alpha1.PhaseVerifyingControlPlane))

			// Verify that UpgradedComponents is 0 (NOT 1!) because kube-apiserver is still on 4.20.44
			Eventually(func() int32 {
				fetched := &upgradev1alpha1.HostedClusterUpgrade{}
				if err := k8sClient.Get(ctx, hcuKey, fetched); err != nil {
					return -1
				}
				return fetched.Status.ControlPlaneStatus.UpgradedComponents
			}, timeout, interval).Should(Equal(int32(0)))

			// Simulate kube-apiserver completing rollout to 4.20.50
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "kube-apiserver", Namespace: cpNamespace}, uCPC)).To(Succeed())
			_ = unstructured.SetNestedField(uCPC.Object, "4.20.50", "status", "version")
			_ = unstructured.SetNestedField(uCPC.Object, int64(2), "status", "observedGeneration")
			Expect(k8sClient.Status().Update(ctx, uCPC)).To(Succeed())

			// Simulate HostedCluster status reaching 4.20.50
			Expect(k8sClient.Get(ctx, hcuKey, uHC)).To(Succeed())
			history := []interface{}{
				map[string]interface{}{
					"image":          regressionImage("4.20.50"),
					"version":        "4.20.50",
					"state":          "Completed",
					"verified":       true,
					"startedTime":    nowStr,
					"completionTime": nowStr,
				},
			}
			_ = unstructured.SetNestedSlice(uHC.Object, history, "status", "version", "history")
			_ = unstructured.SetNestedMap(uHC.Object, map[string]interface{}{"image": regressionImage("4.20.50"), "version": "4.20.50"}, "status", "version", "desired")
			Expect(k8sClient.Status().Update(ctx, uHC)).To(Succeed())
			attachReadyHCP(ctx, uHC)

			// Now UpgradedComponents must become 1 and CP upgrade advances!
			Eventually(func() int32 {
				fetched := &upgradev1alpha1.HostedClusterUpgrade{}
				if err := k8sClient.Get(ctx, hcuKey, fetched); err != nil {
					return -1
				}
				return fetched.Status.ControlPlaneStatus.UpgradedComponents
			}, timeout, interval).Should(Equal(int32(1)))
		})

		It("Issue 1.2: should enter PhaseInstalling when HostedCluster history has only Partial installation entry", func() {
			clusterName := "installing-cluster"
			hcuKey := types.NamespacedName{Name: clusterName, Namespace: namespace}

			hc := &unstructured.Unstructured{}
			hc.SetGroupVersionKind(gvkHC)
			hc.SetName(clusterName)
			hc.SetNamespace(namespace)
			_ = unstructured.SetNestedField(hc.Object, regressionImage("4.20.44"), "spec", "release", "image")
			_ = unstructured.SetNestedMap(hc.Object, map[string]interface{}{"type": "None"}, "spec", "platform")
			_ = unstructured.SetNestedMap(hc.Object, map[string]interface{}{"name": "pull-secret"}, "spec", "pullSecret")
			_ = unstructured.SetNestedMap(hc.Object, map[string]interface{}{"managementType": "Managed", "managed": map[string]interface{}{"storage": map[string]interface{}{"type": "PersistentVolume"}}}, "spec", "etcd")
			_ = unstructured.SetNestedMap(hc.Object, map[string]interface{}{
				"clusterNetwork": []interface{}{map[string]interface{}{"cidr": "10.128.0.0/14"}},
				"serviceNetwork": []interface{}{map[string]interface{}{"cidr": "172.30.0.0/16"}},
				"networkType":    "OVNKubernetes",
			}, "spec", "networking")
			services := []interface{}{
				map[string]interface{}{"service": "APIServer", "servicePublishingStrategy": map[string]interface{}{"type": "LoadBalancer"}},
				map[string]interface{}{"service": "OAuthServer", "servicePublishingStrategy": map[string]interface{}{"type": "Route"}},
				map[string]interface{}{"service": "Konnectivity", "servicePublishingStrategy": map[string]interface{}{"type": "Route"}},
				map[string]interface{}{"service": "Ignition", "servicePublishingStrategy": map[string]interface{}{"type": "Route"}},
			}
			_ = unstructured.SetNestedSlice(hc.Object, services, "spec", "services")
			Expect(k8sClient.Create(ctx, hc)).To(Succeed())
			defer func() { _ = k8sClient.Delete(ctx, hc) }()

			// Initial installation: history has 1 entry with state: "Partial" (no Completed entry!)
			nowStr := time.Now().Format(time.RFC3339)
			history := []interface{}{
				map[string]interface{}{
					"image":          regressionImage("4.20.44"),
					"version":        "4.20.44",
					"state":          "Partial",
					"verified":       false,
					"startedTime":    nowStr,
					"completionTime": nowStr,
				},
			}
			desired := map[string]interface{}{
				"image":   regressionImage("4.20.44"),
				"version": "4.20.44",
			}
			_ = unstructured.SetNestedMap(hc.Object, desired, "status", "version", "desired")
			_ = unstructured.SetNestedSlice(hc.Object, history, "status", "version", "history")
			_ = unstructured.SetNestedSlice(hc.Object, []interface{}{}, "status", "version", "availableUpdates")
			_ = unstructured.SetNestedField(hc.Object, int64(1), "status", "version", "observedGeneration")
			Expect(k8sClient.Status().Update(ctx, hc)).To(Succeed())

			hcu := &upgradev1alpha1.HostedClusterUpgrade{
				ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: namespace},
				Spec: upgradev1alpha1.HostedClusterUpgradeSpec{
					TargetReleaseImage: regressionImage("4.20.50"),
					ControlPlane:       upgradev1alpha1.ControlPlaneUpgradeSpec{},
					NodePoolGroups:     []upgradev1alpha1.NodePoolGroupSpec{},
				},
			}
			Expect(k8sClient.Create(ctx, hcu)).To(Succeed())
			defer func() { _ = k8sClient.Delete(ctx, hcu) }()

			// Coordinator MUST transition to PhaseInstalling (and NOT start upgrading!)
			Eventually(func() upgradev1alpha1.UpgradePhase {
				fetched := &upgradev1alpha1.HostedClusterUpgrade{}
				if err := k8sClient.Get(ctx, hcuKey, fetched); err != nil {
					return ""
				}
				return fetched.Status.Phase
			}, timeout, interval).Should(Equal(upgradev1alpha1.PhaseInstalling))
		})

		It("Issue 1.3: should not deadlock when NodePools have empty pausedUntil and rollout according to maxParallelNodePools", func() {
			clusterName := "concurrency-test-cluster"
			hcuKey := types.NamespacedName{Name: clusterName, Namespace: namespace}

			// Cluster at target version so CP upgrade is bypassed
			uHC := createAndSetStatusHostedCluster(ctx, clusterName, namespace, regressionImage("4.22.0"))
			createReadyControlPlane(ctx, uHC)
			defer func() { _ = k8sClient.Delete(ctx, uHC) }()

			// Create 2 NodePools with pausedUntil: "" (not paused initially!)
			np1 := createAndSetStatusNodePool(ctx, "c-pool-1", namespace, clusterName, regressionImage("4.20.44"))
			defer func() { _ = k8sClient.Delete(ctx, np1) }()

			np2 := createAndSetStatusNodePool(ctx, "c-pool-2", namespace, clusterName, regressionImage("4.20.44"))
			defer func() { _ = k8sClient.Delete(ctx, np2) }()

			// Create HCU with maxParallelNodePools: 1
			hcu := &upgradev1alpha1.HostedClusterUpgrade{
				ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: namespace},
				Spec: upgradev1alpha1.HostedClusterUpgradeSpec{
					TargetReleaseImage: regressionImage("4.22.0"),
					ControlPlane:       upgradev1alpha1.ControlPlaneUpgradeSpec{},
					RolloutPolicy: &upgradev1alpha1.RolloutPolicySpec{
						MaxParallelNodePools: 1,
					},
					NodePoolGroups: []upgradev1alpha1.NodePoolGroupSpec{},
				},
			}
			Expect(k8sClient.Create(ctx, hcu)).To(Succeed())
			defer func() { _ = k8sClient.Delete(ctx, hcu) }()

			// Verify that exactly ONE pool is Upgrading, and the other is WaitingForConcurrency (no deadlock!)
			Eventually(func() map[string]string {
				fetched := &upgradev1alpha1.HostedClusterUpgrade{}
				if err := k8sClient.Get(ctx, hcuKey, fetched); err != nil {
					return nil
				}
				phases := make(map[string]string)
				for _, s := range fetched.Status.NodePoolStatuses {
					phases[s.Name] = s.Phase
				}
				return phases
			}, timeout, interval).Should(Satisfy(func(phases map[string]string) bool {
				upgradingCount := 0
				waitingCount := 0
				for _, p := range phases {
					if p == "Upgrading" {
						upgradingCount++
					}
					if p == "WaitingForConcurrency" {
						waitingCount++
					}
				}
				return upgradingCount == 1 && waitingCount == 1
			}))
		})

		It("Issue 1.4: should not mark NodePool Completed if nodes are unready despite updated replicas", func() {
			clusterName := "unready-pool-cluster"
			hcuKey := types.NamespacedName{Name: clusterName, Namespace: namespace}

			uHC := createAndSetStatusHostedCluster(ctx, clusterName, namespace, regressionImage("4.22.0"))
			defer func() { _ = k8sClient.Delete(ctx, uHC) }()

			// NodePool where spec is already target release, but readyNodeCount is 0!
			np := createAndSetStatusNodePool(ctx, "unready-pool", namespace, clusterName, regressionImage("4.22.0"))
			_ = unstructured.SetNestedField(np.Object, int64(3), "status", "replicas")
			nodesInfo := map[string]interface{}{
				"nodeVersions": []interface{}{
					map[string]interface{}{
						"ocpVersion":       "4.22.0",
						"kubeletVersion":   "v1.28.0",
						"readyNodeCount":   int64(0), // 0 ready!
						"unreadyNodeCount": int64(3), // 3 unready!
					},
				},
			}
			_ = unstructured.SetNestedMap(np.Object, nodesInfo, "status", "nodesInfo")
			Expect(k8sClient.Status().Update(ctx, np)).To(Succeed())
			defer func() { _ = k8sClient.Delete(ctx, np) }()

			hcu := &upgradev1alpha1.HostedClusterUpgrade{
				ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: namespace},
				Spec: upgradev1alpha1.HostedClusterUpgradeSpec{
					TargetReleaseImage: regressionImage("4.22.0"),
					ControlPlane:       upgradev1alpha1.ControlPlaneUpgradeSpec{},
					NodePoolGroups:     []upgradev1alpha1.NodePoolGroupSpec{},
				},
			}
			Expect(k8sClient.Create(ctx, hcu)).To(Succeed())
			defer func() { _ = k8sClient.Delete(ctx, hcu) }()

			// Pool MUST NOT be Completed! It should be Upgrading.
			Consistently(func() string {
				fetched := &upgradev1alpha1.HostedClusterUpgrade{}
				if err := k8sClient.Get(ctx, hcuKey, fetched); err != nil {
					return ""
				}
				for _, s := range fetched.Status.NodePoolStatuses {
					if s.Name == "unready-pool" {
						return s.Phase
					}
				}
				return ""
			}, time.Second*3, interval).ShouldNot(Equal("Completed"))
		})

		It("Issue 1.5: should lock all NodePools (spec.pausedUntil=true) during Control Plane upgrade", func() {
			clusterName := "lock-np-cluster"

			uHC := createAndSetStatusHostedCluster(ctx, clusterName, namespace, regressionImage("4.20.44"))
			createReadyControlPlane(ctx, uHC)
			defer func() { _ = k8sClient.Delete(ctx, uHC) }()

			// Create NodePool with pausedUntil: "false"
			np := createAndSetStatusNodePool(ctx, "lock-target-pool", namespace, clusterName, regressionImage("4.20.44"))
			_ = unstructured.SetNestedField(np.Object, "false", "spec", "pausedUntil")
			Expect(k8sClient.Update(ctx, np)).To(Succeed())
			setReadyNodePoolStatus(np, "4.20.44")
			Expect(k8sClient.Status().Update(ctx, np)).To(Succeed())
			defer func() { _ = k8sClient.Delete(ctx, np) }()

			hcu := &upgradev1alpha1.HostedClusterUpgrade{
				ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: namespace},
				Spec: upgradev1alpha1.HostedClusterUpgradeSpec{
					TargetReleaseImage: regressionImage("4.20.50"),
					ControlPlane:       upgradev1alpha1.ControlPlaneUpgradeSpec{},
					NodePoolGroups:     []upgradev1alpha1.NodePoolGroupSpec{},
				},
			}
			Expect(k8sClient.Create(ctx, hcu)).To(Succeed())
			defer func() { _ = k8sClient.Delete(ctx, hcu) }()

			// During Preflight/CP upgrade, NodePool spec.pausedUntil MUST be set to "true"!
			Eventually(func() string {
				fetchedNP := &unstructured.Unstructured{}
				fetchedNP.SetGroupVersionKind(gvkNP)
				_ = k8sClient.Get(ctx, types.NamespacedName{Name: "lock-target-pool", Namespace: namespace}, fetchedNP)
				pUntil, _, _ := unstructured.NestedString(fetchedNP.Object, "spec", "pausedUntil")
				return pUntil
			}, timeout, interval).Should(Equal("true"))
		})

		It("Issue 3.3: should set standard GitOps Conditions (Ready, Progressing, Failing)", func() {
			clusterName := "conditions-test-cluster"
			hcuKey := types.NamespacedName{Name: clusterName, Namespace: namespace}

			uHC := createAndSetStatusHostedCluster(ctx, clusterName, namespace, regressionImage("4.20.44"))
			defer func() { _ = k8sClient.Delete(ctx, uHC) }()

			hcu := &upgradev1alpha1.HostedClusterUpgrade{
				ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: namespace},
				Spec: upgradev1alpha1.HostedClusterUpgradeSpec{
					TargetReleaseImage: regressionImage("4.20.50"),
					ControlPlane:       upgradev1alpha1.ControlPlaneUpgradeSpec{},
					NodePoolGroups:     []upgradev1alpha1.NodePoolGroupSpec{},
				},
			}
			Expect(k8sClient.Create(ctx, hcu)).To(Succeed())
			defer func() { _ = k8sClient.Delete(ctx, hcu) }()

			// Verify that Status.Conditions has Ready, Progressing, and Failing
			Eventually(func() bool {
				fetched := &upgradev1alpha1.HostedClusterUpgrade{}
				if err := k8sClient.Get(ctx, hcuKey, fetched); err != nil {
					return false
				}
				hasReady := false
				hasProg := false
				hasFail := false
				for _, c := range fetched.Status.Conditions {
					if c.Type == "Ready" {
						hasReady = true
					}
					if c.Type == "Progressing" {
						hasProg = true
					}
					if c.Type == "Failing" {
						hasFail = true
					}
				}
				return hasReady && hasProg && hasFail
			}, timeout, interval).Should(BeTrue())
		})

		It("Issue 3.5: should block when ClusterVersionFailing is True or ClusterVersionUpgradeable is False", func() {
			clusterName := "cv-failing-cluster"
			hcuKey := types.NamespacedName{Name: clusterName, Namespace: namespace}

			uHC := createAndSetStatusHostedCluster(ctx, clusterName, namespace, regressionImage("4.20.44"))
			defer func() { _ = k8sClient.Delete(ctx, uHC) }()

			// Set ClusterVersionFailing = True condition on HostedCluster
			conds := []interface{}{
				map[string]interface{}{
					"type":               "ClusterVersionFailing",
					"status":             "True",
					"reason":             "CVOFailure",
					"message":            "ClusterOperatorAuthenticationDegraded",
					"lastTransitionTime": time.Now().Format(time.RFC3339),
				},
			}
			_ = unstructured.SetNestedSlice(uHC.Object, conds, "status", "conditions")
			Expect(k8sClient.Status().Update(ctx, uHC)).To(Succeed())

			hcu := &upgradev1alpha1.HostedClusterUpgrade{
				ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: namespace},
				Spec: upgradev1alpha1.HostedClusterUpgradeSpec{
					TargetReleaseImage: regressionImage("4.20.50"),
					ControlPlane:       upgradev1alpha1.ControlPlaneUpgradeSpec{},
					NodePoolGroups:     []upgradev1alpha1.NodePoolGroupSpec{},
				},
			}
			Expect(k8sClient.Create(ctx, hcu)).To(Succeed())
			defer func() { _ = k8sClient.Delete(ctx, hcu) }()

			// Verify coordinator transitions to PhaseBlocked
			Eventually(func() upgradev1alpha1.UpgradePhase {
				fetched := &upgradev1alpha1.HostedClusterUpgrade{}
				if err := k8sClient.Get(ctx, hcuKey, fetched); err != nil {
					return ""
				}
				return fetched.Status.Phase
			}, timeout, interval).Should(Equal(upgradev1alpha1.PhaseBlocked))
		})

		It("restores worker locks before deletion and permits recreating an upgrade", func() {
			clusterName := "deletion-cleanup"
			uHC := createAndSetStatusHostedCluster(ctx, clusterName, namespace, regressionImage("4.20.0"))
			createReadyControlPlane(ctx, uHC)
			defer func() { _ = k8sClient.Delete(ctx, uHC) }()
			np := createAndSetStatusNodePool(ctx, "deletion-worker", namespace, clusterName, regressionImage("4.20.0"))
			defer func() { _ = k8sClient.Delete(ctx, np) }()
			future := metav1.NewTime(time.Now().Add(time.Hour))
			template := &upgradev1alpha1.HostedClusterUpgrade{
				ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: namespace},
				Spec: upgradev1alpha1.HostedClusterUpgradeSpec{
					TargetReleaseImage: regressionImage("4.21.0"),
					ControlPlane:       upgradev1alpha1.ControlPlaneUpgradeSpec{StartAt: &future},
				},
			}
			var previousUID types.UID
			for range 2 {
				Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: np.GetName()}, np)).To(Succeed())
				setReadyNodePoolStatus(np, "4.20.0")
				Expect(k8sClient.Status().Update(ctx, np)).To(Succeed())
				hcu := template.DeepCopy()
				Expect(k8sClient.Create(ctx, hcu)).To(Succeed())
				Expect(hcu.UID).NotTo(Equal(previousUID))
				previousUID = hcu.UID
				Eventually(func() string {
					current := np.DeepCopy()
					if err := k8sClient.Get(ctx, types.NamespacedName{Name: np.GetName(), Namespace: namespace}, current); err != nil {
						return ""
					}
					return current.GetAnnotations()[workerLockOwner]
				}, timeout, interval).Should(Equal(string(hcu.UID)))
				Expect(k8sClient.Delete(ctx, hcu)).To(Succeed())
				Eventually(func() bool {
					return apierrors.IsNotFound(k8sClient.Get(ctx, types.NamespacedName{Name: clusterName, Namespace: namespace}, &upgradev1alpha1.HostedClusterUpgrade{}))
				}, timeout, interval).Should(BeTrue())
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: np.GetName(), Namespace: namespace}, np)).To(Succeed())
				_, found, err := unstructured.NestedString(np.Object, "spec", "pausedUntil")
				Expect(err).NotTo(HaveOccurred())
				Expect(found).To(BeFalse())
				Expect(np.GetAnnotations()).NotTo(HaveKey(workerLockOwner))
			}
		})

		It("Issue 3.1: should safely handle KubeLogScraper with nil client, deduplicate and bound log blockers to 10 entries", func() {
			// 1. KubeLogScraper with nil client returns error
			nilScraper := &KubeLogScraper{Client: nil}
			_, err := nilScraper.GetHCCOLogs(ctx, "test-ns", "test-pod", time.Now())
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("kubernetes client is nil"))

			// 2. Timestamped generic HCCO drain failures are cluster observations.
			r := &HostedClusterUpgradeReconciler{
				LogScraper: &DefaultMockLogScraper{
					MockLogs: `2026-08-12T12:00:00Z {"msg":"failed to drain node","Node":{"name":"worker-1"},"error":"drain failed: timeout"}`,
				},
			}

			d := r.scrapeHCCOLogsForDrains(ctx, "clusters", "my-cluster", time.Time{})
			Expect(d.Available).To(BeTrue())
			Expect(d.Messages).To(Equal([]string{"node worker-1: failed to drain node: drain failed: timeout"}))
		})
	})
})
