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
var ImagePattern = regexp.MustCompile(`^(\S+)/(\S+):([0-9]+\.[0-9]+\.[0-9]+(-[a-zA-Z0-9_.-]+)?)@sha256:([a-fA-F0-9]{64})$`)

// ParseVersionFromPullspec extracts the semantic version from the tag slice: <version>-<arch>
func ParseVersionFromPullspec(pullspec string) (string, error) {
	matches := ImagePattern.FindStringSubmatch(pullspec)
	if len(matches) < 4 {
		return "", fmt.Errorf("invalid pullspec format: must match registry.domain.com/path:version-arch@sha256:digest")
	}
	tag := matches[3] // e.g. "4.20.44-x86_64", "4.16.0-rc.1-x86_64", "4.14.0"
	idx := strings.Index(tag, "-")
	if idx == -1 {
		return tag, nil
	}
	return tag[:idx], nil // e.g. "4.20.44"
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
