package controller

import (
	"context"
	"reflect"
	"testing"
	"time"

	upgradev1alpha1 "github.com/duritong/hypershift-upgrade-coordinator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func TestControlPlaneEventRouting(t *testing.T) {
	upgrades := []client.Object{}
	want := []reconcile.Request{}
	// All three identities normalize to the same control-plane namespace.
	for _, key := range []client.ObjectKey{{Namespace: "team-prod", Name: "api.east"}, {Namespace: "team-prod", Name: "api-east"}, {Namespace: "team", Name: "prod-api-east"}} {
		upgrades = append(upgrades, &upgradev1alpha1.HostedClusterUpgrade{ObjectMeta: metav1.ObjectMeta{Namespace: key.Namespace, Name: key.Name}})
		want = append(want, reconcile.Request{NamespacedName: key})
	}
	for _, scope := range []string{"", "team-prod", "other"} {
		for _, gvk := range []schema.GroupVersionKind{gvkCPC, gvkHCP, gvkMachineDeployment, gvkMachineSet} {
			r := &HostedClusterUpgradeReconciler{Client: &regressionClient{objects: upgrades}, HostedClusterNamespace: scope}
			obj := &unstructured.Unstructured{}
			obj.SetGroupVersionKind(gvk)
			obj.SetNamespace("team-prod-api-east")
			obj.SetName("api.east") // HCP names need not be "cluster".
			expected := want
			if scope == "team-prod" {
				expected = want[:2]
			} else if scope == "other" {
				expected = []reconcile.Request{}
			}
			mapper := r.mapCPCToHostedClusterUpgrade
			if gvk == gvkMachineDeployment || gvk == gvkMachineSet {
				mapper = r.mapWorkerToHostedClusterUpgrade
			}
			if got := mapper(context.Background(), obj); !reflect.DeepEqual(got, expected) {
				t.Fatalf("scope=%q kind=%s: got %v, want %v", scope, gvk.Kind, got, expected)
			}
		}
	}
}

func TestWorkerAnnotationAndMembershipUpdateRouting(t *testing.T) {
	np := regressionNP("pool-not-cluster", "")
	np.SetNamespace("team-prod")
	_ = unstructured.SetNestedField(np.Object, "api.east", "spec", "clusterName")
	r := &HostedClusterUpgradeReconciler{Client: &regressionClient{objects: []client.Object{np}}}
	worker := &unstructured.Unstructured{}
	worker.SetGroupVersionKind(gvkMachineDeployment)
	worker.SetAnnotations(map[string]string{workerPoolAnnotation: "team-prod/pool-not-cluster"})
	want := reconcile.Request{NamespacedName: client.ObjectKey{Namespace: "team-prod", Name: "api.east"}}
	if got := r.mapWorkerToHostedClusterUpgrade(context.Background(), worker); !reflect.DeepEqual(got, []reconcile.Request{want}) {
		t.Fatalf("annotation did not resolve through pool: %v", got)
	}
	old := np.DeepCopy()
	_ = unstructured.SetNestedField(np.Object, "other", "spec", "clusterName")
	queue := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
	defer queue.ShutDown()
	handler.EnqueueRequestsFromMapFunc(r.mapNodePoolToHostedClusterUpgrade).Update(context.Background(), event.UpdateEvent{ObjectOld: old, ObjectNew: np}, queue)
	if queue.Len() != 2 {
		t.Fatalf("membership update queued %d requests, want both old and new", queue.Len())
	}
	first, _ := queue.Get()
	second, _ := queue.Get()
	if first != want || second.Name != "other" || second.Namespace != "team-prod" {
		t.Fatalf("incorrect membership routing: %v %v", first, second)
	}
}

type namespaceLogScraper struct{ namespace string }

func (s *namespaceLogScraper) GetHCCOLogs(_ context.Context, namespace, _ string, _ time.Time) (string, error) {
	s.namespace = namespace
	return "", nil
}

func TestDottedClusterLogNamespace(t *testing.T) {
	s := &namespaceLogScraper{}
	r := &HostedClusterUpgradeReconciler{LogScraper: s}
	r.scrapeHCCOLogsForDrains(context.Background(), "team-prod", "api.east", "workers")
	if s.namespace != "team-prod-api-east" {
		t.Fatalf("log namespace = %q", s.namespace)
	}
}
