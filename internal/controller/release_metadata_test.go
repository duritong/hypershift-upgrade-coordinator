package controller

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/compression"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/registry"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/static"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
	"github.com/google/go-containerregistry/pkg/v1/types"
)

func releaseTestRegistry(t *testing.T, wrap func(http.Handler) http.Handler) string {
	t.Helper()
	var handler http.Handler = registry.New(registry.Logger(log.New(io.Discard, "", 0)))
	if wrap != nil {
		handler = wrap(handler)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return strings.TrimPrefix(server.URL, "http://")
}

func releaseTestLayer(t *testing.T, files map[string]string, gzip bool) v1.Layer {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for file, content := range files {
		if err := tw.WriteHeader(&tar.Header{Name: file, Mode: 0644, Size: int64(len(content)), Typeflag: tar.TypeReg}); err != nil {
			t.Fatal(err)
		}
		if _, err := io.WriteString(tw, content); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if gzip {
		layer, err := tarball.LayerFromReader(bytes.NewReader(buf.Bytes()))
		if err != nil {
			t.Fatal(err)
		}
		return layer
	}
	return static.NewLayer(buf.Bytes(), types.OCIUncompressedLayer)
}

func releaseTestReferences(version string) string {
	return fmt.Sprintf("apiVersion: image.openshift.io/v1\nkind: ImageStream\nmetadata:\n  name: %s\nspec:\n  tags: []\n", version)
}

func releaseTestImage(t *testing.T, arch string, layers ...v1.Layer) v1.Image {
	t.Helper()
	img, err := mutate.AppendLayers(empty.Image, layers...)
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := img.ConfigFile()
	if err != nil {
		t.Fatal(err)
	}
	cfg.Architecture, cfg.OS = arch, "linux"
	img, err = mutate.ConfigFile(img, cfg)
	if err != nil {
		t.Fatal(err)
	}
	return img
}

func releaseTestPush(t *testing.T, host, tag string, img v1.Image) string {
	t.Helper()
	ref, err := name.NewTag(host + "/release:" + tag)
	if err != nil {
		t.Fatal(err)
	}
	if err := remote.Write(ref, img); err != nil {
		t.Fatal(err)
	}
	digest, err := img.Digest()
	if err != nil {
		t.Fatal(err)
	}
	return ref.Name() + "@" + digest.String()
}

func TestReleaseMetadataPayloadValidation(t *testing.T) {
	host := releaseTestRegistry(t, nil)
	for _, tt := range []struct {
		name, tag, payloadVersion, configArch, metadata, wantError string
		gzip                                                       bool
	}{
		{name: "amd64 gzip", tag: "4.20.1-x86_64", payloadVersion: "4.20.1", configArch: "amd64", gzip: true},
		{name: "arm64", tag: "4.20.1-aarch64", payloadVersion: "4.20.1", configArch: "arm64"},
		{name: "ppc64le", tag: "4.20.1-ppc64le", payloadVersion: "4.20.1", configArch: "ppc64le"},
		{name: "s390x", tag: "4.20.1-s390x", payloadVersion: "4.20.1", configArch: "s390x"},
		{name: "prerelease", tag: "4.20.0-rc.1-x86_64", payloadVersion: "4.20.0-rc.1", configArch: "amd64"},
		{name: "lying tag", tag: "4.20.1-x86_64", payloadVersion: "4.19.9", configArch: "amd64", wantError: "tag version"},
		{name: "prerelease mismatch", tag: "4.20.0-rc.1-x86_64", payloadVersion: "4.20.0", configArch: "amd64", wantError: "tag version"},
		{name: "lying arch", tag: "4.20.1-x86_64", payloadVersion: "4.20.1", configArch: "arm64", wantError: "tag architecture"},
		{name: "unsupported config", tag: "4.20.1-x86_64", payloadVersion: "4.20.1", configArch: "riscv64", wantError: "tag architecture"},
		{name: "unsupported suffix", tag: "4.20.1-riscv64", payloadVersion: "4.20.1", configArch: "riscv64", wantError: "unsupported release architecture"},
		{name: "multi", tag: "4.20.1-multi", payloadVersion: "4.20.1", configArch: "amd64", wantError: "explicit platform strategy"},
		{name: "invalid version", tag: "4.20.1-x86_64", payloadVersion: "not-a-version", configArch: "amd64", wantError: "semantic version"},
		{name: "malformed", tag: "4.20.1-x86_64", metadata: "{broken", configArch: "amd64", wantError: "malformed release image-references"},
		{name: "wrong kind", tag: "4.20.1-x86_64", metadata: "kind: ConfigMap\napiVersion: v1\nmetadata:\n  name: 4.20.1", configArch: "amd64", wantError: "expected ImageStream"},
		{name: "missing", tag: "4.20.1-x86_64", configArch: "amd64", wantError: "missing release-manifests/image-references"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			files := map[string]string{"unrelated": "content"}
			if tt.payloadVersion != "" {
				files[releaseImageReferences] = releaseTestReferences(tt.payloadVersion)
			} else if tt.metadata != "" {
				files[releaseImageReferences] = tt.metadata
			}
			img := releaseTestImage(t, tt.configArch, releaseTestLayer(t, files, tt.gzip))
			image := releaseTestPush(t, host, tt.tag, img)
			got, err := NewRegistryReleaseMetadataResolver().Resolve(context.Background(), image, nil)
			if tt.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantError) {
					t.Fatalf("Resolve = %+v, %v; want error containing %q", got, err, tt.wantError)
				}
				return
			}
			arch := tt.tag[strings.LastIndex(tt.tag, "-")+1:]
			if err != nil || got != (ReleaseMetadata{tt.payloadVersion, arch}) {
				t.Fatalf("Resolve = %+v, %v", got, err)
			}
		})
	}
}

func TestReleaseMetadataPinnedDigestAndCache(t *testing.T) {
	var requests atomic.Int64
	var tagReads atomic.Int64
	host := releaseTestRegistry(t, func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			requests.Add(1)
			if req.Method == http.MethodGet && strings.Contains(req.URL.Path, "/manifests/4.") {
				tagReads.Add(1)
			}
			next.ServeHTTP(w, req)
		})
	})
	img := releaseTestImage(t, "amd64", releaseTestLayer(t, map[string]string{releaseImageReferences: releaseTestReferences("4.20.1")}, false))
	pinned := releaseTestPush(t, host, "4.20.1-x86_64", img)
	wrong := releaseTestImage(t, "amd64", releaseTestLayer(t, map[string]string{releaseImageReferences: releaseTestReferences("4.19.1")}, false))
	releaseTestPush(t, host, "4.20.1-x86_64", wrong)
	r := NewRegistryReleaseMetadataResolver()
	r.maxCacheEntries = 1
	if _, err := r.Resolve(context.Background(), pinned, nil); err != nil {
		t.Fatal(err)
	}
	before := requests.Load()
	if _, err := r.Resolve(context.Background(), pinned, nil); err != nil || requests.Load() != before {
		t.Fatalf("cache hit performed IO or failed: %v", err)
	}
	if tagReads.Load() != 0 {
		t.Fatal("resolver fetched a mutable tag")
	}
	// A different tag claim must not reuse verification of the same digest.
	if _, err := r.Resolve(context.Background(), strings.Replace(pinned, ":4.20.1-", ":4.20.2-", 1), nil); err == nil {
		t.Fatal("cached digest bypassed tag mismatch")
	}
	if len(r.cache) != 1 {
		t.Fatal("failure was cached")
	}
	// A distinct credential scope evicts the sole entry.
	if _, err := r.Resolve(context.Background(), pinned, []byte(`{"auths":{}}`)); err != nil {
		t.Fatal(err)
	}
	if len(r.cache) != 1 || len(r.cacheOrder) != 1 {
		t.Fatal("cache exceeded capacity")
	}
	before = requests.Load()
	if _, err := r.Resolve(context.Background(), pinned, nil); err != nil || requests.Load() == before {
		t.Fatalf("evicted entry did not fetch again: %v", err)
	}
}

func TestReleaseMetadataIndex(t *testing.T) {
	host := releaseTestRegistry(t, nil)
	amd := releaseTestImage(t, "amd64", releaseTestLayer(t, map[string]string{releaseImageReferences: releaseTestReferences("4.20.1")}, false))
	arm := releaseTestImage(t, "arm64", releaseTestLayer(t, map[string]string{releaseImageReferences: releaseTestReferences("4.20.1")}, false))
	for _, tt := range []struct {
		name, tag, wantError string
		platforms            []mutate.IndexAddendum
	}{
		{name: "select arm", tag: "4.20.1-aarch64", platforms: []mutate.IndexAddendum{
			{Add: amd, Descriptor: v1.Descriptor{Platform: &v1.Platform{OS: "linux", Architecture: "amd64"}}},
			{Add: arm, Descriptor: v1.Descriptor{Platform: &v1.Platform{OS: "linux", Architecture: "arm64"}}},
		}},
		{name: "absent", tag: "4.20.1-s390x", wantError: "found 0", platforms: []mutate.IndexAddendum{{Add: amd, Descriptor: v1.Descriptor{Platform: &v1.Platform{OS: "linux", Architecture: "amd64"}}}}},
		{name: "ambiguous", tag: "4.20.1-x86_64", wantError: "found 2", platforms: []mutate.IndexAddendum{
			{Add: amd, Descriptor: v1.Descriptor{Platform: &v1.Platform{OS: "linux", Architecture: "amd64"}}},
			{Add: amd, Descriptor: v1.Descriptor{Platform: &v1.Platform{OS: "linux", Architecture: "amd64", Variant: "v3"}}},
		}},
		{name: "lying platform", tag: "4.20.1-x86_64", wantError: "tag architecture", platforms: []mutate.IndexAddendum{{Add: arm, Descriptor: v1.Descriptor{Platform: &v1.Platform{OS: "linux", Architecture: "amd64"}}}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			index := mutate.AppendManifests(empty.Index, tt.platforms...)
			ref, err := name.NewTag(host + "/release:" + tt.tag)
			if err != nil {
				t.Fatal(err)
			}
			if err := remote.WriteIndex(ref, index); err != nil {
				t.Fatal(err)
			}
			digest, err := index.Digest()
			if err != nil {
				t.Fatal(err)
			}
			got, err := NewRegistryReleaseMetadataResolver().Resolve(context.Background(), ref.Name()+"@"+digest.String(), nil)
			if tt.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantError) {
					t.Fatalf("Resolve = %+v, %v; want %q", got, err, tt.wantError)
				}
			} else if err != nil || got.Architecture != "aarch64" {
				t.Fatalf("Resolve = %+v, %v", got, err)
			}
		})
	}
}

func TestReleaseMetadataAuthentication(t *testing.T) {
	var protected atomic.Bool
	host := releaseTestRegistry(t, func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			if protected.Load() {
				user, password, ok := req.BasicAuth()
				if !ok || user != "release-user" || password != "release-password" {
					w.Header().Set("WWW-Authenticate", `Basic realm="release"`)
					w.WriteHeader(http.StatusUnauthorized)
					return
				}
			}
			next.ServeHTTP(w, req)
		})
	})
	img := releaseTestImage(t, "amd64", releaseTestLayer(t, map[string]string{releaseImageReferences: releaseTestReferences("4.20.1")}, false))
	image := releaseTestPush(t, host, "4.20.1-x86_64", img)
	protected.Store(true)
	encoded := base64.StdEncoding.EncodeToString([]byte("release-user:release-password"))
	for _, tt := range []struct {
		name, key  string
		credential map[string]string
		wantError  bool
	}{
		{"basic", host, map[string]string{"username": "release-user", "password": "release-password"}, false},
		{"encoded", host, map[string]string{"auth": encoded}, false},
		{"repository", host + "/release", map[string]string{"auth": encoded}, false},
		{"url host", "https://" + host + "/v1/", map[string]string{"auth": encoded}, false},
		{"unrelated", "other.invalid", map[string]string{"auth": encoded}, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			secret, err := json.Marshal(map[string]any{"auths": map[string]any{tt.key: tt.credential}})
			if err != nil {
				t.Fatal(err)
			}
			r := NewRegistryReleaseMetadataResolver()
			_, err = r.Resolve(context.Background(), image, secret)
			if (err != nil) != tt.wantError {
				t.Fatalf("Resolve error = %v, wantError %t", err, tt.wantError)
			}
			if tt.wantError && len(r.cache) != 0 {
				t.Fatal("authentication failure cached")
			}
		})
	}
	r := NewRegistryReleaseMetadataResolver()
	if _, err := r.Resolve(context.Background(), image, nil); err == nil {
		t.Fatal("private image resolved anonymously")
	}
	valid := []byte(fmt.Sprintf(`{"auths":{%q:{"auth":%q}}}`, host, encoded))
	if _, err := r.Resolve(context.Background(), image, valid); err != nil {
		t.Fatalf("failure poisoned retry: %v", err)
	}
	for _, secret := range []string{`{"auths":`, `{"auths":{"registry":{"auth":"not base64!"}}}`, `{"credsStore":"must-not-execute"}`} {
		if _, err := r.Resolve(context.Background(), image, []byte(secret)); err == nil {
			t.Fatalf("accepted invalid or helper-based secret %s", secret)
		}
	}
}

func TestReleaseMetadataIntegrityAndLimits(t *testing.T) {
	var corrupt atomic.Bool
	var blobPath string
	var corruptedBlob []byte
	host := releaseTestRegistry(t, func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			if corrupt.Load() && req.URL.Path == blobPath && req.Method == http.MethodGet {
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write(corruptedBlob)
				return
			}
			next.ServeHTTP(w, req)
		})
	})
	layer := releaseTestLayer(t, map[string]string{releaseImageReferences: releaseTestReferences("4.20.1"), "large": strings.Repeat("a", 32<<10)}, true)
	img := releaseTestImage(t, "amd64", layer)
	image := releaseTestPush(t, host, "4.20.1-x86_64", img)
	digest, err := layer.Digest()
	if err != nil {
		t.Fatal(err)
	}
	blobPath = "/v2/release/blobs/" + digest.String()
	compressed, err := layer.Compressed()
	if err != nil {
		t.Fatal(err)
	}
	corruptedBlob, err = io.ReadAll(compressed)
	if err != nil {
		t.Fatal(err)
	}
	if err := compressed.Close(); err != nil {
		t.Fatal(err)
	}
	// Change only gzip's timestamp: decompression and tar metadata stay valid,
	// so rejecting this requires the compressed blob's digest check at EOF.
	corruptedBlob[4] ^= 1
	corrupt.Store(true)
	r := NewRegistryReleaseMetadataResolver()
	if _, err := r.Resolve(context.Background(), image, nil); err == nil || !strings.Contains(err.Error(), "checksum") {
		t.Fatalf("blob integrity error = %v", err)
	}
	corrupt.Store(false)
	if _, err := r.Resolve(context.Background(), image, nil); err != nil {
		t.Fatalf("integrity failure cached: %v", err)
	}
	for _, tt := range []struct {
		name      string
		configure func(*RegistryReleaseMetadataResolver)
		want      string
	}{
		{"response", func(r *RegistryReleaseMetadataResolver) { r.maxBlobBytes = 32 }, "byte limit"},
		{"expanded", func(r *RegistryReleaseMetadataResolver) { r.maxExpandedBytes = 1024 }, "byte limit"},
		{"metadata", func(r *RegistryReleaseMetadataResolver) { r.maxMetadataBytes = 8 }, "oversized"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			r := NewRegistryReleaseMetadataResolver()
			tt.configure(r)
			if _, err := r.Resolve(context.Background(), image, nil); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Resolve error = %v, want %q", err, tt.want)
			}
			if len(r.cache) != 0 {
				t.Fatal("limit failure cached")
			}
		})
	}
}

func TestReleaseMetadataCancellation(t *testing.T) {
	var stalled atomic.Bool
	host := releaseTestRegistry(t, func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			if stalled.Load() {
				<-req.Context().Done()
				return
			}
			next.ServeHTTP(w, req)
		})
	})
	img := releaseTestImage(t, "amd64", releaseTestLayer(t, map[string]string{releaseImageReferences: releaseTestReferences("4.20.1")}, false))
	image := releaseTestPush(t, host, "4.20.1-x86_64", img)
	r := NewRegistryReleaseMetadataResolver()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := r.Resolve(ctx, image, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("Resolve cancellation = %v", err)
	}
	stalled.Store(true)
	r.timeout = 50 * time.Millisecond
	start := time.Now()
	if _, err := r.Resolve(context.Background(), image, nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Resolve deadline = %v", err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("default resolver deadline not honored")
	}
	stalled.Store(false)
	if _, err := r.Resolve(context.Background(), image, nil); err != nil {
		t.Fatalf("cancellation cached: %v", err)
	}
}

func TestReleaseMetadataOverlay(t *testing.T) {
	host := releaseTestRegistry(t, nil)
	base := releaseTestLayer(t, map[string]string{releaseImageReferences: releaseTestReferences("4.19.1")}, false)
	for _, tt := range []struct {
		name      string
		files     map[string]string
		wantError bool
	}{
		{"replacement", map[string]string{releaseImageReferences: releaseTestReferences("4.20.1")}, false},
		{"whiteout", map[string]string{"release-manifests/.wh.image-references": ""}, true},
		{"opaque", map[string]string{"release-manifests/.wh..wh..opq": ""}, true},
		{"directory removed", map[string]string{".wh.release-manifests": ""}, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			img := releaseTestImage(t, "amd64", base, releaseTestLayer(t, tt.files, false))
			image := releaseTestPush(t, host, "4.20.1-x86_64", img)
			_, err := NewRegistryReleaseMetadataResolver().Resolve(context.Background(), image, nil)
			if (err != nil) != tt.wantError {
				t.Fatalf("Resolve error = %v, wantError %t", err, tt.wantError)
			}
		})
	}
}

func TestReleaseMetadataDockerHubAuth(t *testing.T) {
	repo, err := name.NewRepository("docker.io/library/release")
	if err != nil {
		t.Fatal(err)
	}
	auth, err := releaseRegistryAuth([]byte(`{"auths":{"https://index.docker.io/v1/":{"username":"user","password":"password"}}}`), repo)
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := auth.Authorization()
	if err != nil || cfg.Username != "user" || cfg.Password != "password" {
		t.Fatalf("Docker Hub auth = %+v, %v", cfg, err)
	}
	anon, err := releaseRegistryAuth(nil, repo)
	if err != nil || anon != authn.Anonymous {
		t.Fatalf("anonymous auth = %v, %v", anon, err)
	}
}

func TestReleaseMetadataZstd(t *testing.T) {
	layer := releaseTestLayer(t, map[string]string{releaseImageReferences: releaseTestReferences("4.20.1")}, false)
	raw, err := layer.Uncompressed()
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	zstdLayer, err := tarball.LayerFromReader(raw, tarball.WithCompression(compression.ZStd), tarball.WithMediaType(types.OCILayerZStd))
	if err != nil {
		t.Fatal(err)
	}
	host := releaseTestRegistry(t, nil)
	image := releaseTestPush(t, host, "4.20.1-x86_64", releaseTestImage(t, "amd64", zstdLayer))
	got, err := NewRegistryReleaseMetadataResolver().Resolve(context.Background(), image, nil)
	if err != nil || got != (ReleaseMetadata{"4.20.1", "x86_64"}) {
		t.Fatalf("Resolve zstd = %+v, %v", got, err)
	}
}

func TestReleaseMetadataManifestAndConfigIntegrity(t *testing.T) {
	var tamperPath atomic.Value
	tamperPath.Store("")
	host := releaseTestRegistry(t, func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			if req.Method != http.MethodGet || req.URL.Path != tamperPath.Load().(string) {
				next.ServeHTTP(w, req)
				return
			}
			recorder := httptest.NewRecorder()
			next.ServeHTTP(recorder, req)
			body := recorder.Body.Bytes()
			if len(body) > 0 {
				body[len(body)-1] ^= 1
			}
			for key, values := range recorder.Header() {
				w.Header()[key] = values
			}
			w.WriteHeader(recorder.Code)
			_, _ = w.Write(body)
		})
	})
	img := releaseTestImage(t, "amd64", releaseTestLayer(t, map[string]string{releaseImageReferences: releaseTestReferences("4.20.1")}, false))
	image := releaseTestPush(t, host, "4.20.1-x86_64", img)
	digest, err := img.Digest()
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := img.Manifest()
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct{ name, path, want string }{
		{"manifest", "/v2/release/manifests/" + digest.String(), "manifest digest"},
		{"config", "/v2/release/blobs/" + manifest.Config.Digest.String(), "checksum"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			tamperPath.Store(tt.path)
			r := NewRegistryReleaseMetadataResolver()
			if _, err := r.Resolve(context.Background(), image, nil); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Resolve integrity error = %v, want %q", err, tt.want)
			}
			if len(r.cache) != 0 {
				t.Fatal("integrity failure cached")
			}
		})
	}
}

func TestReleaseMetadataUninitialized(t *testing.T) {
	for _, r := range []*RegistryReleaseMetadataResolver{nil, {}} {
		if _, err := r.Resolve(context.Background(), "", nil); err == nil || !strings.Contains(err.Error(), "not initialized") {
			t.Fatalf("uninitialized resolver must fail closed: %v", err)
		}
	}
}
