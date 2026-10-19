package guest

import (
	"regexp"
	"strings"

	upgradev1 "github.com/duritong/hypershift-upgrade-coordinator/api/v1alpha1"
)

var pinnedImage = regexp.MustCompile(`^[^\s@]+@sha256:[a-fA-F0-9]{64}$`)

func fullVersion(v string) bool {
	if len(v) > 128 {
		return false
	}
	p, err := upgradev1.ExtractVersion(v)
	return err == nil && p == v && !strings.Contains(v, "/")
}

func validRelease(r Release) bool { return fullVersion(r.Version) && pinnedImage.MatchString(r.Image) }

// Mirror repositories and tags may differ; only validated digest equality proves
// immutable payload identity. Version identity is checked separately.
func samePayload(a, b string) bool {
	if !pinnedImage.MatchString(a) || !pinnedImage.MatchString(b) {
		return false
	}
	_, ad, _ := strings.Cut(a, "@")
	_, bd, _ := strings.Cut(b, "@")
	return ad == bd
}

func matches(version, image string, expected Release) bool {
	return fullVersion(version) && version == expected.Version && samePayload(image, expected.Image)
}
