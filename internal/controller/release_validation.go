package controller

import (
	"context"
	"fmt"
	"strings"

	upgradev1alpha1 "github.com/duritong/hypershift-upgrade-coordinator/api/v1alpha1"
	hypershiftv1beta1 "github.com/openshift/hypershift/api/hypershift/v1beta1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// verifyReleasePayloads gates every active reconciliation, including recovery
// directly into a rollout phase. Never propagate registry or Secret error text:
// remote endpoints and credential parsers may include sensitive data in errors.
func (r *HostedClusterUpgradeReconciler) verifyReleasePayloads(ctx context.Context, hcu *upgradev1alpha1.HostedClusterUpgrade, hc *unstructured.Unstructured) error {
	if r.ReleaseResolver == nil {
		return fmt.Errorf("release metadata resolver is not configured")
	}
	var pullSecret []byte
	ref, specified, err := unstructured.NestedMap(hc.Object, "spec", "pullSecret")
	if err != nil {
		return fmt.Errorf("HostedCluster pullSecret reference is malformed")
	}
	if specified {
		name, ok := ref["name"].(string)
		if !ok || name == "" {
			return fmt.Errorf("HostedCluster pullSecret reference must name a Secret")
		}
		if r.APIReader == nil {
			return fmt.Errorf("uncached pull-secret reader is not configured")
		}
		secret := &corev1.Secret{}
		if err := r.APIReader.Get(ctx, client.ObjectKey{Namespace: hc.GetNamespace(), Name: name}, secret); err != nil {
			return fmt.Errorf("cannot read HostedCluster pull Secret")
		}
		pullSecret = secret.Data[corev1.DockerConfigJsonKey]
		if len(pullSecret) == 0 {
			return fmt.Errorf("HostedCluster pull Secret has missing or empty .dockerconfigjson")
		}
	}

	var caBundle []byte
	if trustRef, found, _ := unstructured.NestedMap(hc.Object, "spec", "additionalTrustBundle"); found && trustRef != nil {
		if name, ok := trustRef["name"].(string); ok && name != "" && r.APIReader != nil {
			cm := &corev1.ConfigMap{}
			if err := r.APIReader.Get(ctx, client.ObjectKey{Namespace: hc.GetNamespace(), Name: name}, cm); err == nil && cm.Data != nil {
				if certs, ok := cm.Data["ca-bundle.crt"]; ok && certs != "" {
					caBundle = []byte(certs)
				}
			}
		}
	}

	_, targetArch, err := upgradev1alpha1.ParseReleaseImageMetadata(hcu.Spec.TargetReleaseImage)
	if err != nil {
		return fmt.Errorf("target release image claim is invalid")
	}
	pools, err := r.listClusterNodePools(ctx, hcu.Namespace, hcu.Name)
	if err != nil {
		return fmt.Errorf("cannot observe worker architectures for release validation")
	}
	for _, pool := range pools.Items {
		arch, _, err := unstructured.NestedString(pool.Object, "spec", "arch")
		if arch == "" {
			arch = "amd64"
		}
		if err != nil || hypershiftv1beta1.ArchAliases[arch] != targetArch {
			return fmt.Errorf("NodePool %q architecture %q is incompatible with target payload architecture %q", pool.GetName(), arch, targetArch)
		}
	}
	images := []struct{ step, image string }{{"target", hcu.Spec.TargetReleaseImage}}
	if intermediate := hcu.Spec.IntermediateReleaseImage; intermediate != "" {
		version, arch, err := upgradev1alpha1.ParseReleaseImageMetadata(intermediate)
		if err != nil {
			return fmt.Errorf("intermediate release image claim is invalid")
		}
		current, currentVersion := GetHostedClusterCurrentRelease(hc)
		if currentVersion != "" {
			current = currentVersion
		}
		_, currentErr := upgradev1alpha1.ExtractVersion(current)
		if currentErr != nil || upgradev1alpha1.IsUpstreamGreater(current, version) {
			if arch != targetArch {
				return fmt.Errorf("intermediate and target release architectures must match")
			}
			images = append(images, struct{ step, image string }{"intermediate", intermediate})
		}
	}
	hcMirrors := hostedClusterMirrors(hc)
	mgmtMirrors := r.managementClusterMirrors(ctx)
	reqLogger := log.FromContext(ctx)

	for _, candidate := range images {
		version, arch, err := upgradev1alpha1.ParseReleaseImageMetadata(candidate.image)
		if err != nil {
			return fmt.Errorf("%s release image claim is invalid", candidate.step)
		}

		candidatesToTry := resolveImageCandidates(candidate.image, hcMirrors, mgmtMirrors)
		var verifiedMetadata *ReleaseMetadata
		var lastResolveErr error
		var attempted []string

		for _, imageRef := range candidatesToTry {
			attempted = append(attempted, imageRef)
			var metadata ReleaseMetadata
			var resolveErr error
			if caResolver, ok := r.ReleaseResolver.(ReleaseMetadataResolverWithCA); ok && len(caBundle) > 0 {
				metadata, resolveErr = caResolver.ResolveWithCA(ctx, imageRef, pullSecret, caBundle)
			} else {
				metadata, resolveErr = r.ReleaseResolver.Resolve(ctx, imageRef, pullSecret)
			}

			if resolveErr != nil {
				lastResolveErr = resolveErr
				reqLogger.Info("Release payload candidate resolution failed",
					"step", candidate.step,
					"candidate", imageRef,
					"error", resolveErr.Error())
				continue
			}
			if metadata.Version != version {
				lastResolveErr = fmt.Errorf("release payload version %q does not match claimed tag %q", metadata.Version, version)
				reqLogger.Info("Release payload version mismatch on candidate",
					"step", candidate.step,
					"candidate", imageRef,
					"resolvedVersion", metadata.Version,
					"claimedVersion", version)
				continue
			}
			if metadata.Architecture != arch {
				lastResolveErr = fmt.Errorf("release payload architecture %q does not match claimed tag %q", metadata.Architecture, arch)
				reqLogger.Info("Release payload architecture mismatch on candidate",
					"step", candidate.step,
					"candidate", imageRef,
					"resolvedArchitecture", metadata.Architecture,
					"claimedArchitecture", arch)
				continue
			}

			verifiedMetadata = &metadata
			if imageRef != candidate.image {
				reqLogger.Info("Verified release payload via registry mirror",
					"step", candidate.step,
					"claimedImage", candidate.image,
					"resolvedMirror", imageRef,
					"version", metadata.Version,
					"architecture", metadata.Architecture)
			}
			break
		}

		if verifiedMetadata == nil {
			if lastResolveErr != nil {
				reqLogger.Info("All release payload candidate resolutions failed", "step", candidate.step, "error", lastResolveErr.Error())
			}
			return fmt.Errorf("%s release payload could not be verified (attempted %d candidate(s): %s); check registry access, credentials, digest integrity, and payload metadata", candidate.step, len(attempted), strings.Join(attempted, ", "))
		}
	}
	return nil
}

// hostedClusterMirrors extracts source-to-mirrors mappings from HostedCluster spec.imageContentSources.
func hostedClusterMirrors(hc *unstructured.Unstructured) map[string][]string {
	if hc == nil {
		return nil
	}
	sources, found, _ := unstructured.NestedSlice(hc.Object, "spec", "imageContentSources")
	if !found || len(sources) == 0 {
		return nil
	}
	mirrorsMap := make(map[string][]string)
	for _, item := range sources {
		entry, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		source, _ := entry["source"].(string)
		source = strings.TrimSpace(source)
		if source == "" {
			continue
		}
		rawMirrors, _, _ := unstructured.NestedSlice(entry, "mirrors")
		for _, m := range rawMirrors {
			mirrorStr, _ := m.(string)
			mirrorStr = strings.TrimSpace(mirrorStr)
			if mirrorStr != "" {
				mirrorsMap[source] = append(mirrorsMap[source], mirrorStr)
			}
		}
	}
	return mirrorsMap
}

// managementClusterMirrors queries cluster-scoped ImageDigestMirrorSet and ImageContentSourcePolicy on the management cluster.
func (r *HostedClusterUpgradeReconciler) managementClusterMirrors(ctx context.Context) map[string][]string {
	if r.Client == nil {
		return nil
	}
	mirrorsMap := make(map[string][]string)

	// 1. Try ImageDigestMirrorSet (config.openshift.io/v1)
	idmsList := &unstructured.UnstructuredList{}
	idmsList.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "config.openshift.io",
		Version: "v1",
		Kind:    "ImageDigestMirrorSet",
	})
	if err := r.Client.List(ctx, idmsList); err == nil {
		for _, item := range idmsList.Items {
			entries, found, _ := unstructured.NestedSlice(item.Object, "spec", "imageDigestMirrors")
			if !found {
				continue
			}
			for _, entry := range entries {
				m, ok := entry.(map[string]interface{})
				if !ok {
					continue
				}
				source, _ := m["source"].(string)
				source = strings.TrimSpace(source)
				if source == "" {
					continue
				}
				rawMirrors, _, _ := unstructured.NestedSlice(m, "mirrors")
				for _, rawMirror := range rawMirrors {
					mirrorStr, _ := rawMirror.(string)
					mirrorStr = strings.TrimSpace(mirrorStr)
					if mirrorStr != "" {
						mirrorsMap[source] = append(mirrorsMap[source], mirrorStr)
					}
				}
			}
		}
	}

	// 2. Try ImageContentSourcePolicy (operator.openshift.io/v1alpha1)
	icspList := &unstructured.UnstructuredList{}
	icspList.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "operator.openshift.io",
		Version: "v1alpha1",
		Kind:    "ImageContentSourcePolicy",
	})
	if err := r.Client.List(ctx, icspList); err == nil {
		for _, item := range icspList.Items {
			entries, found, _ := unstructured.NestedSlice(item.Object, "spec", "repositoryDigestMirrors")
			if !found {
				continue
			}
			for _, entry := range entries {
				m, ok := entry.(map[string]interface{})
				if !ok {
					continue
				}
				source, _ := m["source"].(string)
				source = strings.TrimSpace(source)
				if source == "" {
					continue
				}
				rawMirrors, _, _ := unstructured.NestedSlice(m, "mirrors")
				for _, rawMirror := range rawMirrors {
					mirrorStr, _ := rawMirror.(string)
					mirrorStr = strings.TrimSpace(mirrorStr)
					if mirrorStr != "" {
						mirrorsMap[source] = append(mirrorsMap[source], mirrorStr)
					}
				}
			}
		}
	}

	return mirrorsMap
}

// resolveImageCandidates generates candidate image pullspecs by applying registry mirrors.
// Mirrors from the HostedCluster and management cluster take priority in order;
// the original image is always included as the final fallback.
func resolveImageCandidates(image string, mirrorMaps ...map[string][]string) []string {
	seen := make(map[string]bool)
	var candidates []string

	addCandidate := func(candidate string) {
		candidate = strings.TrimSpace(candidate)
		if candidate != "" && !seen[candidate] {
			seen[candidate] = true
			candidates = append(candidates, candidate)
		}
	}

	for _, mirrors := range mirrorMaps {
		for source, mirrorList := range mirrors {
			cleanSource := strings.TrimSuffix(strings.TrimSpace(source), "/")
			if cleanSource == "" {
				continue
			}
			var suffix string
			var matched bool
			if image == cleanSource {
				matched = true
				suffix = ""
			} else if strings.HasPrefix(image, cleanSource+"/") {
				matched = true
				suffix = "/" + strings.TrimPrefix(image, cleanSource+"/")
			} else if strings.HasPrefix(image, cleanSource+":") {
				matched = true
				suffix = ":" + strings.TrimPrefix(image, cleanSource+":")
			} else if strings.HasPrefix(image, cleanSource+"@") {
				matched = true
				suffix = "@" + strings.TrimPrefix(image, cleanSource+"@")
			}
			if matched {
				for _, mirror := range mirrorList {
					cleanMirror := strings.TrimSuffix(strings.TrimSpace(mirror), "/")
					if cleanMirror != "" {
						addCandidate(cleanMirror + suffix)
					}
				}
			}
		}
	}

	addCandidate(image)
	return candidates
}
