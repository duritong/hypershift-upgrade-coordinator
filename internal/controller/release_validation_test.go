package controller

import (
	"context"
	"fmt"
	"strings"
	"testing"

	upgradev1alpha1 "github.com/duritong/hypershift-upgrade-coordinator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Only test fixtures may derive metadata from a tag instead of the payload.
type fixtureReleaseResolver struct{}

func (fixtureReleaseResolver) Resolve(_ context.Context, image string, _ []byte) (ReleaseMetadata, error) {
	version, arch, err := upgradev1alpha1.ParseReleaseImageMetadata(image)
	return ReleaseMetadata{version, arch}, err
}

type releaseResolverFunc func(context.Context, string, []byte) (ReleaseMetadata, error)

func (f releaseResolverFunc) Resolve(ctx context.Context, image string, secret []byte) (ReleaseMetadata, error) {
	return f(ctx, image, secret)
}

type releaseSecretReader struct {
	client.Reader
	reads      int
	err        error
	management client.Reader
}

func (r *releaseSecretReader) Get(ctx context.Context, key client.ObjectKey, out client.Object, opts ...client.GetOption) error {
	if _, secret := out.(*corev1.Secret); !secret && r.management != nil {
		return r.management.Get(ctx, key, out, opts...)
	}
	r.reads++
	if r.err != nil {
		return r.err
	}
	return r.Reader.Get(ctx, key, out, opts...)
}

func reconcileReleaseTest(t *testing.T, r *HostedClusterUpgradeReconciler, c *regressionClient, hcu *upgradev1alpha1.HostedClusterUpgrade) *upgradev1alpha1.HostedClusterUpgrade {
	t.Helper()
	if reader, ok := r.APIReader.(*releaseSecretReader); ok {
		reader.management = c
	}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(hcu)}); err != nil {
		t.Fatal(err)
	}
	got := &upgradev1alpha1.HostedClusterUpgrade{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(hcu), got); err != nil {
		t.Fatal(err)
	}
	return got
}

func TestReleaseValidationFailsBeforeDownstreamWrites(t *testing.T) {
	const sensitive = "credential-content-must-not-appear"
	for _, phase := range []upgradev1alpha1.UpgradePhase{
		upgradev1alpha1.PhasePreflightChecks, upgradev1alpha1.PhasePendingSchedule,
		upgradev1alpha1.PhaseUpgradingControlPlane, upgradev1alpha1.PhaseVerifyingControlPlane,
		upgradev1alpha1.PhaseUpgradingNodePools,
	} {
		for _, failure := range []string{"nil resolver", "resolver error", "version mismatch", "architecture mismatch", "missing secret", "missing key", "empty key", "missing reader", "reader error", "empty reference", "malformed reference"} {
			t.Run(string(phase)+"/"+failure, func(t *testing.T) {
				hcu, hc, np := regressionHCU(phase), regressionHC("4.20.0"), regressionNP("workers", "")
				c := &regressionClient{objects: []client.Object{hcu, hc, np}}
				r := &HostedClusterUpgradeReconciler{Client: c, ReleaseResolver: fixtureReleaseResolver{}}
				calls := 0
				if strings.Contains(failure, "secret") || strings.Contains(failure, "key") || strings.Contains(failure, "reader") || strings.Contains(failure, "reference") {
					_ = unstructured.SetNestedMap(hc.Object, map[string]interface{}{"name": "pull-secret"}, "spec", "pullSecret")
					reader := &releaseSecretReader{Reader: &regressionClient{}}
					r.APIReader = reader
					if failure == "reader error" {
						reader.err = fmt.Errorf("forbidden: %s", sensitive)
					}
					if failure == "missing key" || failure == "empty key" {
						secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "pull-secret", Namespace: "clusters"}, Data: map[string][]byte{"other": []byte(sensitive)}}
						if failure == "empty key" {
							secret.Data[corev1.DockerConfigJsonKey] = []byte{}
						}
						reader.Reader = &regressionClient{objects: []client.Object{secret}}
					}
					if failure == "missing reader" {
						r.APIReader = nil
					}
					if failure == "empty reference" {
						_ = unstructured.SetNestedMap(hc.Object, map[string]interface{}{}, "spec", "pullSecret")
					}
					if failure == "malformed reference" {
						_ = unstructured.SetNestedField(hc.Object, sensitive, "spec", "pullSecret")
					}
				}
				if failure == "nil resolver" {
					r.ReleaseResolver = nil
				} else {
					r.ReleaseResolver = releaseResolverFunc(func(ctx context.Context, image string, secret []byte) (ReleaseMetadata, error) {
						calls++
						metadata, err := (fixtureReleaseResolver{}).Resolve(ctx, image, secret)
						switch failure {
						case "resolver error":
							return ReleaseMetadata{}, fmt.Errorf("registry returned %s", sensitive)
						case "version mismatch":
							metadata.Version = "4.19.0"
						case "architecture mismatch":
							metadata.Architecture = "aarch64"
						}
						return metadata, err
					})
				}
				got := reconcileReleaseTest(t, r, c, hcu)
				if got.Status.Phase != upgradev1alpha1.PhaseBlocked || c.writes != 0 || !strings.Contains(got.Status.Message, "Release payload safety gate blocked") {
					t.Fatalf("unsafe release acted: phase=%s downstream writes=%d message=%s", got.Status.Phase, c.writes, got.Status.Message)
				}
				if strings.Contains(got.Status.Message, sensitive) {
					t.Fatal("sensitive error content reached status")
				}
				if r.APIReader != nil && calls != 0 {
					t.Fatal("resolver called despite pull-secret failure")
				}
			})
		}
	}
}

func TestReleaseValidationActualPayloadMismatch(t *testing.T) {
	host := releaseTestRegistry(t, nil)
	for _, tt := range []struct{ name, version, arch string }{
		{"version", "4.19.0", "amd64"}, {"architecture", "4.21.0", "arm64"},
	} {
		for _, phase := range []upgradev1alpha1.UpgradePhase{upgradev1alpha1.PhasePreflightChecks, upgradev1alpha1.PhaseUpgradingControlPlane, upgradev1alpha1.PhaseUpgradingNodePools} {
			t.Run(tt.name+"/"+string(phase), func(t *testing.T) {
				img := releaseTestImage(t, tt.arch, releaseTestLayer(t, map[string]string{releaseImageReferences: releaseTestReferences(tt.version)}, true))
				hcu := regressionHCU(phase)
				hcu.Spec.TargetReleaseImage = releaseTestPush(t, host, "4.21.0-x86_64", img)
				c := &regressionClient{objects: []client.Object{hcu, regressionHC("4.20.0"), regressionNP("workers", "")}}
				r := &HostedClusterUpgradeReconciler{Client: c, ReleaseResolver: NewRegistryReleaseMetadataResolver()}
				got := reconcileReleaseTest(t, r, c, hcu)
				if got.Status.Phase != upgradev1alpha1.PhaseBlocked || c.writes != 0 {
					t.Fatalf("lying tag allowed mutation: phase=%s writes=%d", got.Status.Phase, c.writes)
				}
			})
		}
	}
}

func TestReleaseValidationIntermediateAndPullSecret(t *testing.T) {
	for _, scenario := range []string{"both needed", "already at intermediate", "already at target", "intermediate version mismatch", "intermediate architecture mismatch", "different hop architectures"} {
		t.Run(scenario, func(t *testing.T) {
			hcu := regressionHCU(upgradev1alpha1.PhasePendingSchedule)
			hcu.Spec.IntermediateReleaseImage, hcu.Spec.TargetReleaseImage = regressionImage("4.21.0"), regressionImage("4.22.0")
			current := "4.20.0"
			if scenario == "already at intermediate" {
				current = "4.21.0"
			} else if scenario == "already at target" {
				current = "4.22.0"
			}
			if scenario == "different hop architectures" {
				hcu.Spec.IntermediateReleaseImage = strings.Replace(hcu.Spec.IntermediateReleaseImage, "-x86_64@", "-aarch64@", 1)
			}
			hc := regressionHC(current)
			_ = unstructured.SetNestedMap(hc.Object, map[string]interface{}{"name": "hc-credentials"}, "spec", "pullSecret")
			secretBytes := []byte(`{"auths":{"registry.invalid":{"username":"user","password":"password"}}}`)
			secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "clusters", Name: "hc-credentials"}, Data: map[string][]byte{corev1.DockerConfigJsonKey: secretBytes}}
			reader := &releaseSecretReader{Reader: &regressionClient{objects: []client.Object{secret}}}
			// The cached client deliberately has no Secret; only APIReader can get it.
			c := &regressionClient{objects: []client.Object{hcu, hc}}
			var calls []string
			r := &HostedClusterUpgradeReconciler{Client: c, APIReader: reader, ReleaseResolver: releaseResolverFunc(func(ctx context.Context, image string, secret []byte) (ReleaseMetadata, error) {
				if string(secret) != string(secretBytes) {
					t.Fatal("resolver did not receive the HC pull secret")
				}
				calls = append(calls, image)
				metadata, err := (fixtureReleaseResolver{}).Resolve(ctx, image, secret)
				if image == hcu.Spec.IntermediateReleaseImage {
					if scenario == "intermediate version mismatch" {
						metadata.Version = "4.19.0"
					} else if scenario == "intermediate architecture mismatch" {
						metadata.Architecture = "aarch64"
					}
				}
				return metadata, err
			})}
			got := reconcileReleaseTest(t, r, c, hcu)
			blocked := strings.Contains(scenario, "mismatch") || scenario == "different hop architectures"
			if (got.Status.Phase == upgradev1alpha1.PhaseBlocked) != blocked || c.writes != 0 {
				t.Fatalf("phase=%s writes=%d, want blocked=%t", got.Status.Phase, c.writes, blocked)
			}
			if reader.reads != 1 {
				t.Fatalf("uncached secret reads=%d, want 1", reader.reads)
			}
			if !blocked {
				wantCalls := 2
				if current != "4.20.0" {
					wantCalls = 1
				}
				if len(calls) != wantCalls || calls[0] != hcu.Spec.TargetReleaseImage {
					t.Fatalf("resolved images=%v, want %d calls including target", calls, wantCalls)
				}
			}
		})
	}
}

func TestReleaseValidationPauseDoesNotRequireRegistry(t *testing.T) {
	hcu := regressionHCU(upgradev1alpha1.PhaseUpgradingNodePools)
	pause := "true"
	hcu.Spec.PausedUntil = &pause
	c := &regressionClient{objects: []client.Object{hcu, regressionHC("4.20.0"), regressionNP("workers", "")}}
	got := reconcileReleaseTest(t, &HostedClusterUpgradeReconciler{Client: c}, c, hcu)
	if got.Status.Phase != upgradev1alpha1.PhasePaused || c.writes != 0 {
		t.Fatalf("pause required registry or performed writes: phase=%s writes=%d", got.Status.Phase, c.writes)
	}
}

func TestReleasePayloadMatchesWorkerArchitecture(t *testing.T) {
	hcu := regressionHCU(upgradev1alpha1.PhasePendingSchedule)
	np := regressionNP("arm-workers", "")
	_ = unstructured.SetNestedField(np.Object, "arm64", "spec", "arch")
	c := &regressionClient{objects: []client.Object{hcu, regressionHC("4.20.0"), np}}
	r := &HostedClusterUpgradeReconciler{Client: c, ReleaseResolver: fixtureReleaseResolver{}}
	got := reconcileReleaseTest(t, r, c, hcu)
	if got.Status.Phase != upgradev1alpha1.PhaseBlocked || !strings.Contains(got.Status.Message, "architecture") || c.writes != 0 {
		t.Fatalf("incompatible worker architecture was accepted: %+v writes=%d", got.Status, c.writes)
	}
}
