package controller

import (
	"context"
	"testing"
	"time"

	upgradev1 "github.com/duritong/hypershift-upgrade-coordinator/api/v1alpha1"
	"github.com/duritong/hypershift-upgrade-coordinator/internal/guest"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestGuestAccessObserverProfileMigration(t *testing.T) {
	for _, scenario := range []string{"active", "paused", "partial", "foreign-uid", "unknown-profile", "cleanup"} {
		t.Run(scenario, func(t *testing.T) {
			clock := &accessTestClock{now: time.Now().UTC().Truncate(time.Second)}
			g := newAccessGuest(t, clock.now.Add(time.Hour))
			r, key := newAccessManagement(t, guestFixtureConfig(g), clock)
			accessReconcile(t, r, key)
			secret := accessDelivery(t, r)
			uid := secret.UID
			secret.Annotations[accessPrefix+"profile"] = "observer-v1"
			if scenario == "unknown-profile" {
				secret.Annotations[accessPrefix+"profile"] = "unrecognized"
			}
			if err := r.Client.Update(context.Background(), secret); err != nil {
				t.Fatal(err)
			}
			for _, raw := range g.objects {
				obj := &unstructured.Unstructured{Object: raw}
				if obj.GetKind() == "ClusterRole" {
					raw["rules"] = raw["rules"].([]interface{})[:2]
				}
				annotations := obj.GetAnnotations()
				annotations[accessPrefix+"profile"] = "observer-v1"
				if scenario == "partial" && obj.GetKind() == "Namespace" {
					annotations[accessPrefix+"profile"] = GuestObserverProfile
				}
				if scenario == "foreign-uid" {
					annotations[accessPrefix+"hc-uid"] = "someone-else"
				}
				obj.SetAnnotations(annotations)
			}
			if _, _, err := r.BuildTargetAndReader(context.Background(), key, guest.Release{Version: "4.18.1", Image: regressionImage("4.18.1")}); err == nil {
				t.Fatal("old profile authorized observation")
			}
			writes, tokens := g.writes, g.tokens
			if scenario == "cleanup" {
				u := &upgradev1.HostedClusterUpgrade{}
				if err := r.Client.Get(context.Background(), key, u); err != nil {
					t.Fatal(err)
				}
				if err := r.Client.Delete(context.Background(), u); err != nil {
					t.Fatal(err)
				}
				if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key}); err != nil {
					t.Fatal(err)
				}
				if err := r.Client.Get(context.Background(), client.ObjectKeyFromObject(secret), secret); err == nil {
					t.Fatal("old delivery retained after deletion")
				}
				return
			}
			if scenario == "paused" {
				pauseAccess(t, r, key)
			}
			u := accessReconcile(t, r, key)
			if scenario == "paused" || scenario == "foreign-uid" || scenario == "unknown-profile" {
				if u.Status.GuestAccess.MaintenanceReady || g.writes != writes || g.tokens != tokens {
					t.Fatalf("unauthorized migration: %+v", u.Status.GuestAccess)
				}
				if scenario != "paused" {
					return
				}
				u.Spec.PausedUntil = nil
				u.Generation++
				if err := r.Client.Update(context.Background(), u); err != nil {
					t.Fatal(err)
				}
				u = accessReconcile(t, r, key)
			}
			if !u.Status.GuestAccess.MaintenanceReady || !u.Status.GuestAccess.CredentialAvailable {
				t.Fatalf("migration failed: %+v", u.Status.GuestAccess)
			}
			secret = accessDelivery(t, r)
			if secret.UID != uid || secret.Annotations[accessPrefix+"profile"] != GuestObserverProfile {
				t.Fatal("delivery replaced or not migrated")
			}
			for _, raw := range g.objects {
				obj := &unstructured.Unstructured{Object: raw}
				if obj.GetAnnotations()[accessPrefix+"profile"] != GuestObserverProfile {
					t.Fatal("guest object not migrated")
				}
				if obj.GetKind() == "ClusterRole" && len(raw["rules"].([]interface{})) != 4 {
					t.Fatal("migration did not expand the observer's read-only rules")
				}
			}
			if g.tokens != tokens+1 {
				t.Fatal("migration did not publish a new scoped credential")
			}
			writes, tokens = g.writes, g.tokens
			accessReconcile(t, r, key)
			if g.writes != writes || g.tokens != tokens {
				t.Fatal("migration was not idempotent")
			}
		})
	}
}
