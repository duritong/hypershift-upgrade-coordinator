/*
Copyright 2026 mh@immerda.ch

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"golang.org/x/mod/semver"
)

// ImagePattern matches: registry.domain.com/path:version-arch@sha256:digest
var ImagePattern = regexp.MustCompile(`^(\S+)/(\S+):([0-9]+\.[0-9]+\.[0-9]+-[a-zA-Z0-9_][a-zA-Z0-9_.-]*)@sha256:([a-fA-F0-9]{64})$`)

// ParseVersionFromPullspec extracts the semantic version from the tag slice: <version>-<arch>
func ParseVersionFromPullspec(pullspec string) (string, error) {
	matches := ImagePattern.FindStringSubmatch(pullspec)
	if len(matches) < 4 {
		return "", fmt.Errorf("invalid pullspec format: must match registry.domain.com/path:version-arch@sha256:digest")
	}
	tag := matches[3] // e.g. "4.20.44-x86_64", "4.16.0-rc.1-x86_64"
	idx := strings.Index(tag, "-")
	version := tag[:idx]
	if !semver.IsValid("v" + version) {
		return "", fmt.Errorf("invalid semantic version extracted from pullspec: %s", version)
	}
	return version, nil
}

// ValidateUpgradePath validates release images and same-major, at-most-one-minor
// forward hops. currentRelease may be a version or pullspec; intermediatePullspec
// may be empty. Admission must set allowTargetEqualCurrent to false. Controllers
// may set it to true when recovering an upgrade whose control plane reached target.
// An intermediate at or behind current is skipped, but must still precede target.
func ValidateUpgradePath(currentRelease, targetPullspec, intermediatePullspec string, allowTargetEqualCurrent bool) error {
	currentVersion, err := ExtractVersion(currentRelease)
	if err != nil {
		return fmt.Errorf("current cluster release image format is invalid: %w", err)
	}
	if len(strings.Split(currentVersion, ".")) != 3 {
		return fmt.Errorf("invalid current version (expected major.minor.patch): %s", currentVersion)
	}
	targetVersion, err := ParseVersionFromPullspec(targetPullspec)
	if err != nil {
		return fmt.Errorf("spec.targetReleaseImage format is invalid (%q): %w", targetPullspec, err)
	}
	var intermediateVersion string
	if intermediatePullspec != "" {
		intermediateVersion, err = ParseVersionFromPullspec(intermediatePullspec)
		if err != nil {
			return fmt.Errorf("spec.intermediateReleaseImage format is invalid (%q): %w", intermediatePullspec, err)
		}
	}

	comparison := semver.Compare("v"+currentVersion, "v"+targetVersion)
	if comparison > 0 || (comparison == 0 && !allowTargetEqualCurrent) {
		return fmt.Errorf("invalid version progression: target version '%s' is not strictly greater than current version '%s' (equality allowed: %t)", targetVersion, currentVersion, allowTargetEqualCurrent)
	}
	currMajor, currMinor, err := GetMajorMinor(currentVersion)
	if err != nil {
		return err
	}
	tgtMajor, tgtMinor, err := GetMajorMinor(targetVersion)
	if err != nil {
		return err
	}
	if currMajor != tgtMajor {
		return fmt.Errorf("invalid major version jump: cannot upgrade across different major versions (%d -> %d)", currMajor, tgtMajor)
	}
	if intermediateVersion != "" {
		intMajor, intMinor, err := GetMajorMinor(intermediateVersion)
		if err != nil {
			return err
		}
		if currMajor != intMajor {
			return fmt.Errorf("invalid major version jump: cannot upgrade across different major versions (%d -> %d)", currMajor, intMajor)
		}
		if !IsUpstreamGreater(intermediateVersion, targetVersion) {
			return fmt.Errorf("invalid version progression: target version '%s' is not strictly greater than intermediate version '%s'", targetVersion, intermediateVersion)
		}
		if IsUpstreamGreater(currentVersion, intermediateVersion) {
			if intMinor-currMinor > 1 {
				return fmt.Errorf("invalid minor version jump: intermediate version '%s' is more than 1 minor version ahead of current version '%s'", intermediateVersion, currentVersion)
			}
			if tgtMinor-intMinor > 1 {
				return fmt.Errorf("invalid minor version jump: target version '%s' is more than 1 minor version ahead of intermediate version '%s'", targetVersion, intermediateVersion)
			}
			return nil
		}
	}
	if tgtMinor-currMinor > 1 {
		return fmt.Errorf("unsupported direct multi-minor jump: target version '%s' is more than 1 minor version ahead of current version '%s'", targetVersion, currentVersion)
	}
	return nil
}

// ExtractVersion parses a semantic version string from either a full pullspec image or a raw version string (e.g. "4.13.0-x86_64" -> "4.13.0").
func ExtractVersion(input string) (string, error) {
	if input == "" {
		return "", fmt.Errorf("empty version string")
	}

	if strings.Contains(input, "@sha256:") || strings.Contains(input, "/") {
		v, err := ParseVersionFromPullspec(input)
		if err != nil {
			return "", err
		}
		if !semver.IsValid("v" + v) {
			return "", fmt.Errorf("invalid semantic version extracted from pullspec: %s", v)
		}
		return v, nil
	}

	v := strings.TrimPrefix(input, "v")
	idx := strings.Index(v, "-")
	if idx != -1 {
		v = v[:idx]
	}

	if !semver.IsValid("v" + v) {
		return "", fmt.Errorf("invalid semantic version: %s", input)
	}
	return v, nil
}

// IsUpstreamGreater compares two version/pullspec strings to ensure vNext is strictly greater than vPrev.
func IsUpstreamGreater(vPrev, vNext string) bool {
	pVer, errPrev := ExtractVersion(vPrev)
	nVer, errNext := ExtractVersion(vNext)
	if errPrev != nil || errNext != nil {
		return false
	}
	return semver.Compare("v"+pVer, "v"+nVer) < 0
}

// GetMajorMinor parses a version or pullspec string and returns major, minor as integers.
func GetMajorMinor(versionInput string) (int, int, error) {
	version, err := ExtractVersion(versionInput)
	if err != nil {
		return 0, 0, err
	}

	parts := strings.Split(version, ".")
	if len(parts) < 2 {
		return 0, 0, fmt.Errorf("invalid semantic version format (expected major.minor.patch): %s", version)
	}

	major, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, 0, fmt.Errorf("failed to parse major version: %v", err)
	}

	minor, err := strconv.Atoi(parts[1])
	if err != nil {
		return 0, 0, fmt.Errorf("failed to parse minor version: %v", err)
	}

	return major, minor, nil
}
