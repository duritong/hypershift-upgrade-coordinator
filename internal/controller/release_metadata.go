package controller

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net/http"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/docker/cli/cli/config"
	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/types"
	"github.com/klauspost/compress/zstd"
	imagev1 "github.com/openshift/api/image/v1"
	"golang.org/x/mod/semver"
	"sigs.k8s.io/yaml"

	upgradev1alpha1 "github.com/duritong/hypershift-upgrade-coordinator/api/v1alpha1"
)

// ReleaseMetadata describes the digest-pinned payload, not its mutable tag.
type ReleaseMetadata struct {
	Version      string
	Architecture string
}

// ReleaseMetadataResolver verifies release payload metadata using HC pull-secret bytes.
type ReleaseMetadataResolver interface {
	Resolve(ctx context.Context, image string, pullSecret []byte) (ReleaseMetadata, error)
}

// ReleaseMetadataResolverWithCA is an optional interface for resolvers supporting custom CA certificate bundles.
type ReleaseMetadataResolverWithCA interface {
	ResolveWithCA(ctx context.Context, image string, pullSecret, caBundle []byte) (ReleaseMetadata, error)
}

const releaseImageReferences = "release-manifests/image-references"

// RegistryReleaseMetadataResolver has bounded network, extraction, and cache costs.
// Construct it with NewRegistryReleaseMetadataResolver; it is safe for concurrent use.
type RegistryReleaseMetadataResolver struct {
	timeout          time.Duration
	maxBlobBytes     int64
	maxExpandedBytes int64
	maxMetadataBytes int64
	maxCacheEntries  int
	transport        http.RoundTripper
	mu               sync.Mutex
	cache            map[string]ReleaseMetadata
	cacheOrder       []string
}

var _ ReleaseMetadataResolver = (*RegistryReleaseMetadataResolver)(nil)

// NewRegistryReleaseMetadataResolver uses a 30s deadline, 64 MiB per response,
// 256 MiB total expanded tar data, 4 MiB metadata, and 128 successful cache entries.
// No credentials are read from the process environment or credential helpers.
func NewRegistryReleaseMetadataResolver() *RegistryReleaseMetadataResolver {
	return &RegistryReleaseMetadataResolver{
		timeout: 30 * time.Second, maxBlobBytes: 64 << 20,
		maxExpandedBytes: 256 << 20, maxMetadataBytes: 4 << 20,
		maxCacheEntries: 128, transport: remote.DefaultTransport,
		cache: make(map[string]ReleaseMetadata),
	}
}

func (r *RegistryReleaseMetadataResolver) Resolve(ctx context.Context, image string, pullSecret []byte) (ReleaseMetadata, error) {
	return r.ResolveWithCA(ctx, image, pullSecret, nil)
}

func (r *RegistryReleaseMetadataResolver) ResolveWithCA(ctx context.Context, image string, pullSecret, caBundle []byte) (ReleaseMetadata, error) {
	if r == nil || r.timeout <= 0 || r.maxBlobBytes <= 0 || r.maxExpandedBytes <= 0 || r.maxMetadataBytes <= 0 {
		return ReleaseMetadata{}, fmt.Errorf("release metadata resolver is not initialized")
	}
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return ReleaseMetadata{}, err
	}
	version, arch, err := upgradev1alpha1.ParseReleaseImageMetadata(image)
	if err != nil {
		return ReleaseMetadata{}, err
	}
	platformArch := map[string]string{"x86_64": "amd64", "aarch64": "arm64", "ppc64le": "ppc64le", "s390x": "s390x"}[arch]
	if platformArch == "" {
		return ReleaseMetadata{}, fmt.Errorf("unsupported release architecture %q: multi requires an explicit platform strategy", arch)
	}
	// Drop the tag entirely: it must never participate in registry resolution.
	repository, digest, _ := strings.Cut(image, "@")
	repository = repository[:strings.LastIndex(repository, ":")]
	ref, err := name.NewDigest(repository + "@" + strings.ToLower(digest))
	if err != nil {
		return ReleaseMetadata{}, fmt.Errorf("invalid release digest reference: %w", err)
	}
	auth, err := releaseRegistryAuth(pullSecret, ref.Repository)
	if err != nil {
		return ReleaseMetadata{}, err
	}
	// Scope successes to credentials, CA bundle, as well as the complete claim being checked.
	key := fmt.Sprintf("%s:%x:%x", image, sha256.Sum256(pullSecret), sha256.Sum256(caBundle))
	r.mu.Lock()
	metadata, cached := r.cache[key]
	r.mu.Unlock()
	if cached {
		return metadata, nil
	}
	transport := r.transport
	if len(caBundle) > 0 {
		var err error
		transport, err = releaseTransportWithCABundle(r.transport, caBundle)
		if err != nil {
			return ReleaseMetadata{}, fmt.Errorf("invalid CA bundle for release registry: %w", err)
		}
	}
	opts := []remote.Option{remote.WithContext(ctx), remote.WithAuth(auth), remote.WithTransport(releaseBoundedTransport{transport, r.maxBlobBytes})}
	desc, err := remote.Get(ref, opts...)
	if err != nil {
		return ReleaseMetadata{}, fmt.Errorf("fetch pinned release manifest: %w", err)
	}
	if desc.MediaType.IsIndex() {
		index, err := desc.ImageIndex()
		if err != nil {
			return ReleaseMetadata{}, fmt.Errorf("read release index: %w", err)
		}
		manifest, err := index.IndexManifest()
		if err != nil {
			return ReleaseMetadata{}, fmt.Errorf("read release index manifest: %w", err)
		}
		var matches []v1.Descriptor
		for _, candidate := range manifest.Manifests {
			if p := candidate.Platform; p != nil && p.OS == "linux" && p.Architecture == platformArch {
				matches = append(matches, candidate)
			}
		}
		if len(matches) != 1 {
			return ReleaseMetadata{}, fmt.Errorf("release index requires exactly one linux/%s platform, found %d", platformArch, len(matches))
		}
		desc, err = remote.Get(ref.Repository.Digest(matches[0].Digest.String()), opts...)
		if err != nil {
			return ReleaseMetadata{}, fmt.Errorf("fetch release platform manifest: %w", err)
		}
	}
	if !desc.MediaType.IsImage() {
		return ReleaseMetadata{}, fmt.Errorf("unsupported release manifest media type %q", desc.MediaType)
	}
	img, err := desc.Image()
	if err != nil {
		return ReleaseMetadata{}, fmt.Errorf("read release image: %w", err)
	}
	cfg, err := img.ConfigFile()
	if err != nil {
		return ReleaseMetadata{}, fmt.Errorf("read release config: %w", err)
	}
	if cfg.OS != "linux" || cfg.Architecture != platformArch {
		return ReleaseMetadata{}, fmt.Errorf("tag architecture %q mismatches payload platform %s/%s", arch, cfg.OS, cfg.Architecture)
	}
	layers, err := img.Layers()
	if err != nil {
		return ReleaseMetadata{}, fmt.Errorf("read release layers: %w", err)
	}
	remaining := r.maxExpandedBytes
	var contents []byte
	// Search newest first, respecting whiteouts rather than trusting hidden files.
	for i := len(layers) - 1; i >= 0; i-- {
		contents, err = r.releaseReferences(ctx, layers[i], &remaining)
		if err != nil {
			return ReleaseMetadata{}, fmt.Errorf("extract release layer %d: %w", i, err)
		}
		if contents != nil {
			break
		}
	}
	if contents == nil {
		return ReleaseMetadata{}, fmt.Errorf("release payload missing %s", releaseImageReferences)
	}
	var stream imagev1.ImageStream
	if err := yaml.UnmarshalStrict(contents, &stream); err != nil {
		return ReleaseMetadata{}, fmt.Errorf("malformed release image-references: %w", err)
	}
	if stream.Kind != "ImageStream" || stream.APIVersion != "image.openshift.io/v1" || !semver.IsValid("v"+stream.Name) {
		return ReleaseMetadata{}, fmt.Errorf("malformed release image-references: expected ImageStream with semantic version metadata.name")
	}
	if stream.Name != version {
		return ReleaseMetadata{}, fmt.Errorf("tag version %q mismatches payload version %q", version, stream.Name)
	}
	if err := ctx.Err(); err != nil {
		return ReleaseMetadata{}, err
	}
	metadata = ReleaseMetadata{Version: stream.Name, Architecture: arch}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.maxCacheEntries > 0 {
		if _, exists := r.cache[key]; !exists {
			if len(r.cacheOrder) >= r.maxCacheEntries {
				delete(r.cache, r.cacheOrder[0])
				r.cacheOrder = r.cacheOrder[1:]
			}
			r.cache[key] = metadata
			r.cacheOrder = append(r.cacheOrder, key)
		}
	}
	return metadata, nil
}

func releaseRegistryAuth(secret []byte, repository name.Repository) (authn.Authenticator, error) {
	if len(secret) == 0 {
		return authn.Anonymous, nil
	}
	cf, err := config.LoadFromReader(bytes.NewReader(secret))
	if err != nil {
		// Docker config errors can contain credential values.
		return nil, fmt.Errorf("invalid release pull secret Docker config")
	}
	if cf.CredentialsStore != "" || len(cf.CredentialHelpers) != 0 {
		return nil, fmt.Errorf("release pull secret must contain inline auths, not credential helpers")
	}
	candidates := []string{
		repository.Name(),
		repository.RegistryStr(),
		"https://" + repository.Name(),
		"https://" + repository.RegistryStr(),
		"http://" + repository.Name(),
		"http://" + repository.RegistryStr(),
	}
	for _, key := range candidates {
		if key == name.DefaultRegistry {
			key = authn.DefaultAuthKey
		}
		cfg, err := cf.GetAuthConfig(key)
		if err != nil {
			return nil, fmt.Errorf("cannot select release registry credentials")
		}
		if cfg.Username != "" || cfg.Password != "" || cfg.Auth != "" || cfg.IdentityToken != "" || cfg.RegistryToken != "" {
			return authn.FromConfig(authn.AuthConfig{Username: cfg.Username, Password: cfg.Password, Auth: cfg.Auth, IdentityToken: cfg.IdentityToken, RegistryToken: cfg.RegistryToken}), nil
		}
	}
	return authn.Anonymous, nil
}

func releaseTransportWithCABundle(base http.RoundTripper, caBundle []byte) (http.RoundTripper, error) {
	if len(caBundle) == 0 {
		return base, nil
	}
	pool, err := x509.SystemCertPool()
	if err != nil || pool == nil {
		pool = x509.NewCertPool()
	} else {
		pool = pool.Clone()
	}
	if !pool.AppendCertsFromPEM(caBundle) {
		return nil, fmt.Errorf("failed to parse certificates from CA bundle")
	}
	var t *http.Transport
	if baseTransport, ok := base.(*http.Transport); ok {
		t = baseTransport.Clone()
	} else {
		t = http.DefaultTransport.(*http.Transport).Clone()
	}
	if t.TLSClientConfig == nil {
		t.TLSClientConfig = &tls.Config{}
	} else {
		t.TLSClientConfig = t.TLSClientConfig.Clone()
	}
	t.TLSClientConfig.RootCAs = pool
	return t, nil
}

func (r *RegistryReleaseMetadataResolver) releaseReferences(ctx context.Context, layer v1.Layer, remaining *int64) ([]byte, error) {
	compressed, err := layer.Compressed()
	if err != nil {
		return nil, err
	}
	defer compressed.Close()
	bounded := &releaseBoundedBody{ReadCloser: compressed, remaining: r.maxBlobBytes}
	mt, err := layer.MediaType()
	if err != nil {
		return nil, err
	}
	var reader io.Reader = bounded
	switch mt {
	case types.OCILayer, types.DockerLayer, types.DockerForeignLayer, types.OCIRestrictedLayer:
		gz, err := gzip.NewReader(reader)
		if err != nil {
			return nil, fmt.Errorf("malformed gzip layer: %w", err)
		}
		defer gz.Close()
		reader = gz
	case types.OCILayerZStd:
		decoder, err := zstd.NewReader(reader, zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxMemory(16<<20))
		if err != nil {
			return nil, fmt.Errorf("malformed zstd layer: %w", err)
		}
		defer decoder.Close()
		reader = decoder
	case types.OCIUncompressedLayer, types.DockerUncompressedLayer:
	default:
		return nil, fmt.Errorf("unsupported release layer media type %q", mt)
	}
	limited := &io.LimitedReader{R: releaseContextReader{ctx, reader}, N: *remaining + 1}
	tr := tar.NewReader(limited)
	var contents []byte
	removed := false
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			if limited.N == 0 {
				return nil, fmt.Errorf("expanded release layers exceed byte limit")
			}
			return nil, fmt.Errorf("malformed release tar: %w", err)
		}
		member := path.Clean(strings.TrimPrefix(header.Name, "./"))
		switch member {
		case "release-manifests":
			if header.Typeflag != tar.TypeDir {
				return nil, fmt.Errorf("malformed release payload: release-manifests is not a directory")
			}
		case ".wh.release-manifests", ".wh..wh..opq", "release-manifests/.wh.image-references", "release-manifests/.wh..wh..opq":
			removed = true
		case releaseImageReferences:
			if header.Typeflag != tar.TypeReg || header.Size > r.maxMetadataBytes || contents != nil {
				return nil, fmt.Errorf("invalid or oversized release image-references tar member")
			}
			contents, err = io.ReadAll(io.LimitReader(tr, r.maxMetadataBytes+1))
			if err != nil {
				return nil, err
			}
		}
	}
	// Include padding/trailing data in the expansion budget and check gzip EOF.
	if _, err := io.Copy(io.Discard, limited); err != nil {
		return nil, err
	}
	consumed := *remaining + 1 - limited.N
	*remaining -= consumed
	if *remaining < 0 {
		return nil, fmt.Errorf("expanded release layers exceed byte limit")
	}
	// Decompressors may stop without reading the underlying blob to EOF. The
	// remote library only verifies its digest at EOF, so drain it before trust.
	if _, err := io.Copy(io.Discard, releaseContextReader{ctx, bounded}); err != nil {
		return nil, err
	}
	if removed && contents == nil {
		return nil, fmt.Errorf("release payload missing %s (whiteout)", releaseImageReferences)
	}
	return contents, nil
}

type releaseContextReader struct {
	ctx context.Context
	io.Reader
}

func (r releaseContextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.Reader.Read(p)
}

type releaseBoundedTransport struct {
	http.RoundTripper
	limit int64
}

func (t releaseBoundedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := t.RoundTripper.RoundTrip(req)
	if err == nil {
		resp.Body = &releaseBoundedBody{ReadCloser: resp.Body, remaining: t.limit}
	}
	return resp, err
}

type releaseBoundedBody struct {
	io.ReadCloser
	remaining int64
}

func (b *releaseBoundedBody) Read(p []byte) (int, error) {
	if b.remaining < 0 {
		return 0, fmt.Errorf("registry response exceeds byte limit")
	}
	if int64(len(p)) > b.remaining+1 {
		p = p[:b.remaining+1]
	}
	n, err := b.ReadCloser.Read(p)
	b.remaining -= int64(n)
	if b.remaining < 0 {
		return n, fmt.Errorf("registry response exceeds byte limit")
	}
	return n, err
}
