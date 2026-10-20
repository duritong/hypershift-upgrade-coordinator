package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestGuestAccessConfiguredObserverNamespace(t *testing.T) {
	const namespace = "custom-guest-observer"
	clock := &accessTestClock{now: time.Now().UTC().Truncate(time.Second)}
	g := newAccessGuest(t, clock.now.Add(time.Hour))
	r, key := newAccessManagement(t, guestFixtureConfig(g), clock)
	r.ObserverNamespace = namespace
	hcu := accessReconcile(t, r, key)
	if !hcu.Status.GuestAccess.MaintenanceReady || !hcu.Status.GuestAccess.CredentialAvailable {
		t.Fatalf("custom observer namespace was not provisioned: %+v", hcu.Status.GuestAccess)
	}
	assertObserverNamespace(t, g, namespace)
}

func assertObserverNamespace(t *testing.T, g *accessGuestFixture, namespace string) {
	t.Helper()
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.objects["/api/v1/namespaces/"+namespace] == nil || g.objects["/api/v1/namespaces/"+namespace+"/serviceaccounts/"+GuestObserverName] == nil {
		t.Fatalf("observer namespace/ServiceAccount missing from %q", namespace)
	}
	binding := g.objects["/apis/rbac.authorization.k8s.io/v1/clusterrolebindings/"+GuestObserverName]
	if binding == nil {
		t.Fatal("observer binding missing")
	}
	subjects := binding["subjects"].([]interface{})
	if len(subjects) != 1 || subjects[0].(map[string]interface{})["namespace"] != namespace {
		t.Fatalf("binding does not select configured namespace: %v", subjects)
	}
	if len(g.tokenPaths) == 0 || g.tokenPaths[len(g.tokenPaths)-1] != "/api/v1/namespaces/"+namespace+"/serviceaccounts/"+GuestObserverName+"/token" {
		t.Fatalf("TokenRequest used wrong namespace: %v", g.tokenPaths)
	}
}

func TestGuestAccessNamespaceChangeWaitsForResume(t *testing.T) {
	clock := &accessTestClock{now: time.Now().UTC().Truncate(time.Second)}
	g := newAccessGuest(t, clock.now.Add(time.Hour))
	r, key := newAccessManagement(t, guestFixtureConfig(g), clock)
	accessReconcile(t, r, key)
	expected := guestRelease(regressionImage("4.18.1"))
	oldTarget, _, err := r.BuildTargetAndReader(context.Background(), key, expected)
	if err != nil {
		t.Fatal(err)
	}
	original := accessDelivery(t, r).DeepCopy()
	const namespace = "new-guest-observer"
	r.ObserverNamespace = namespace
	if err := r.ValidateTarget(context.Background(), oldTarget); err == nil {
		t.Fatal("old namespace credentials remained valid for a new configuration")
	}
	pauseAccess(t, r, key)
	writes, tokens := g.writes, g.tokens
	hcu := accessReconcile(t, r, key)
	if hcu.Status.GuestAccess.CredentialAvailable || g.writes != writes || g.tokens != tokens {
		t.Fatalf("namespace change bootstrapped/issued credentials during pause: %+v", hcu.Status.GuestAccess)
	}
	if current := accessDelivery(t, r); current.ResourceVersion != original.ResourceVersion {
		t.Fatal("paused migration replaced original credential Secret")
	}
	hcu.Spec.PausedUntil = nil
	hcu.Generation++
	if err := r.Client.Update(context.Background(), hcu); err != nil {
		t.Fatal(err)
	}
	hcu = accessReconcile(t, r, key)
	if !hcu.Status.GuestAccess.MaintenanceReady || !hcu.Status.GuestAccess.CredentialAvailable {
		t.Fatalf("namespace migration did not complete: %+v", hcu.Status.GuestAccess)
	}
	assertObserverNamespace(t, g, namespace)
	if current := accessDelivery(t, r); current.UID != original.UID || current.ResourceVersion == original.ResourceVersion {
		t.Fatal("migration must update the owned delivery Secret in place")
	}
	if err := r.ValidateTarget(context.Background(), oldTarget); err == nil {
		t.Fatal("migration did not invalidate old observation target")
	}
}

func TestGuestAccessCustomNamespaceRefusesUnownedNamespace(t *testing.T) {
	const namespace = "existing-observer"
	clock := &accessTestClock{now: time.Now().UTC().Truncate(time.Second)}
	g := newAccessGuest(t, clock.now.Add(time.Hour))
	g.objects["/api/v1/namespaces/"+namespace] = map[string]interface{}{"apiVersion": "v1", "kind": "Namespace", "metadata": map[string]interface{}{"name": namespace, "uid": "user-owned"}}
	r, key := newAccessManagement(t, guestFixtureConfig(g), clock)
	r.ObserverNamespace = namespace
	hcu := accessReconcile(t, r, key)
	if hcu.Status.GuestAccess.MaintenanceReady || hcu.Status.GuestAccess.Reason != "ProfileOwnershipConflict" || g.writes != 0 || g.tokens != 0 {
		t.Fatalf("unowned namespace was adopted or ignored: %+v", hcu.Status.GuestAccess)
	}
	secretKey := client.ObjectKey{Namespace: r.OperatorNamespace, Name: deliveryName("hc-uid")}
	if err := r.APIReader.Get(context.Background(), secretKey, &corev1.Secret{}); err == nil {
		t.Fatal("conflicting namespace produced credentials")
	}
}

func TestGuestObserverNamespaceValidation(t *testing.T) {
	for _, name := range []string{"", "Uppercase", "contains.dot", "contains/slash", "-leading", "trailing-", strings.Repeat("a", 64)} {
		if err := ValidateGuestObserverNamespace(name); err == nil {
			t.Errorf("accepted invalid namespace %q", name)
		}
	}
	for _, name := range []string{GuestObserverNamespace, "custom-observer", "huc2"} {
		if err := ValidateGuestObserverNamespace(name); err != nil {
			t.Errorf("rejected valid namespace %q: %v", name, err)
		}
	}
}

func TestGuestAccessLegacyDefaultNamespaceCredential(t *testing.T) {
	clock := &accessTestClock{now: time.Now().UTC().Truncate(time.Second)}
	g := newAccessGuest(t, clock.now.Add(time.Hour))
	r, key := newAccessManagement(t, guestFixtureConfig(g), clock)
	accessReconcile(t, r, key)
	secret := accessDelivery(t, r)
	delete(secret.Annotations, accessPrefix+"observer-namespace")
	if err := r.Client.Update(context.Background(), secret); err != nil {
		t.Fatal(err)
	}
	writes, tokens := g.writes, g.tokens
	hcu := accessReconcile(t, r, key)
	if !hcu.Status.GuestAccess.CredentialAvailable || !hcu.Status.GuestAccess.MaintenanceReady || g.tokens != tokens || g.writes != writes {
		t.Fatal("unchanged default namespace forced unnecessary credential/profile migration")
	}
	if _, _, err := r.BuildTargetAndReader(context.Background(), key, guestRelease(regressionImage("4.18.1"))); err != nil {
		t.Fatal(err)
	}
	r.ObserverNamespace = "custom-observer"
	if _, _, err := r.BuildTargetAndReader(context.Background(), key, guestRelease(regressionImage("4.18.1"))); err == nil {
		t.Fatal("legacy namespace metadata was assumed to mean a custom namespace")
	}
}
