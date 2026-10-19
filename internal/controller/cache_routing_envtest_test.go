package controller

import (
	"time"

	upgradev1alpha1 "github.com/duritong/hypershift-upgrade-coordinator/api/v1alpha1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var _ = Describe("Informer-backed routing indexes", func() {
	It("queries unstructured NodePools and upgrades through registered cache indexes", func() {
		pool := createAndSetStatusNodePool(ctx, "cache-index-workers", "default", "cache-index-cluster", regressionImage("4.20.0"))
		DeferCleanup(func() { Expect(k8sClient.Delete(ctx, pool)).To(Succeed()) })
		hcu := regressionHCU(upgradev1alpha1.PhasePreflightChecks)
		hcu.Namespace, hcu.Name = "default", "cache-index-cluster"
		pause := "true"
		hcu.Spec.PausedUntil = &pause
		Expect(k8sClient.Create(ctx, hcu)).To(Succeed())
		DeferCleanup(func() { Expect(k8sClient.Delete(ctx, hcu)).To(Succeed()) })
		r := &HostedClusterUpgradeReconciler{Client: managerClient}
		Eventually(func(g Gomega) {
			pools, err := r.listClusterNodePools(ctx, "default", hcu.Name)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(pools.Items).To(HaveLen(1))
			g.Expect(pools.Items[0].GetName()).To(Equal(pool.GetName()))
			component := &unstructured.Unstructured{}
			component.SetNamespace(WorkerControlPlaneNamespace(hcu.Namespace, hcu.Name))
			requests := r.mapCPCToHostedClusterUpgrade(ctx, component)
			g.Expect(requests).To(HaveLen(1))
			g.Expect(requests[0].NamespacedName).To(Equal(client.ObjectKeyFromObject(hcu)))
		}, 10*time.Second, 50*time.Millisecond).Should(Succeed())
		// An API-server List cannot satisfy the custom field selector above. This
		// also verifies watch updates refresh the unstructured indexed cache.
		pool.SetLabels(map[string]string{"cache-test": "updated"})
		Expect(k8sClient.Update(ctx, pool)).To(Succeed())
		Eventually(func(g Gomega) {
			pools, err := r.listClusterNodePools(ctx, "default", hcu.Name)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(pools.Items).To(HaveLen(1))
			g.Expect(pools.Items[0].GetLabels()["cache-test"]).To(Equal("updated"))
		}, 10*time.Second, 50*time.Millisecond).Should(Succeed())
	})
})
