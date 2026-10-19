package controller

import (
	"context"
	"fmt"

	upgradev1alpha1 "github.com/duritong/hypershift-upgrade-coordinator/api/v1alpha1"
	hypershiftv1beta1 "github.com/openshift/hypershift/api/hypershift/v1beta1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
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
	for _, candidate := range images {
		version, arch, err := upgradev1alpha1.ParseReleaseImageMetadata(candidate.image)
		if err != nil {
			return fmt.Errorf("%s release image claim is invalid", candidate.step)
		}
		metadata, err := r.ReleaseResolver.Resolve(ctx, candidate.image, pullSecret)
		if err != nil {
			return fmt.Errorf("%s release payload could not be verified; check registry access, credentials, digest integrity, and payload metadata", candidate.step)
		}
		if metadata.Version != version {
			return fmt.Errorf("%s release payload version does not match its tag", candidate.step)
		}
		if metadata.Architecture != arch {
			return fmt.Errorf("%s release payload architecture does not match its tag", candidate.step)
		}
	}
	return nil
}
