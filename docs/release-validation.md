# Release Payload Validation

Admission validates release pullspec syntax and the claimed version path without
contacting registries. Before active reconciliation can lock workers or change a
HostedCluster or NodePool, the controller also verifies the actual digest-pinned
target payload and any intermediate payload still ahead of the control plane.
An intermediate already reached is not fetched. Target validation still runs
when the control plane is already at target and workers need upgrading.

The registry lookup discards the mutable tag and fetches the pinned digest. The
authoritative version is the ImageStream `metadata.name` in
`release-manifests/image-references`; architecture comes from the image config.
Both must match the tag, including prerelease versions. Needed intermediate and
target hops must have the same architecture. Indexes require exactly one matching
Linux platform selected by the tag suffix (`x86_64`, `aarch64`, `ppc64le`, or
`s390x`). A `multi` suffix is rejected because there is no explicit platform
selection policy. This gate does not yet validate individual NodePool architecture
compatibility or the current cluster's architecture.

## Private Registries

The controller retrieves the Secret named by the HostedCluster's `spec.pullSecret`
in the HostedCluster namespace using the uncached API reader. It requires a
nonempty `.dockerconfigjson` entry. Missing or unreadable Secrets, malformed
references/configs, and authentication failures block upgrades; they never fall
back to anonymous access. A Docker config without credentials for the selected
registry can use anonymous registry access. Inline `auths` entries support Docker
credentials and tokens through the registry library. Credential helpers, process
Docker configuration, and external commands such as `oc` are not used.

Anonymous access without a Secret is allowed only when the HostedCluster has no
pull-secret reference (useful for lightweight fixtures); production HyperShift
schemas require that reference. Fleet RBAC grants only `get` on Secrets, not
`list` or `watch`, and no Secret informer is started by these lookups.

Secret bytes, credential-parser errors, registry responses, and resolver errors
are not copied into controller logs or upgrade status. A failed lookup reports a
sanitized payload-verification message and retries after 30 seconds. Check Secret
existence, key contents, registry connectivity, permissions, and the payload using
trusted administrative tooling when diagnosing that message.

## Disconnected Registries

The coordinator contacts the registry in the configured release pullspec directly.
It does not apply node-side ImageDigestMirrorSet, ImageContentSourcePolicy, or
HyperShift mirror settings. In disconnected environments, use a digest-pinned
pullspec naming a reachable mirror whose payload version and architecture match
the tag, and include credentials for that mirror in the HC pull Secret.

TLS verification uses the coordinator container's system trust store. There is
no per-HostedCluster CA, insecure-registry switch, or automatic import of cluster
registry CAs. Private-CA registries require an appropriately configured system
trust store in the coordinator deployment; do not disable verification.

## Resource Bounds

Each lookup has a 30-second deadline (or an earlier caller deadline), a 64 MiB
per-response/compressed-layer limit, a 256 MiB total expanded-layer limit, and a
4 MiB image-references limit. Layer extraction streams compressed and expanded
bytes instead of buffering an entire layer. Only the metadata member is retained;
zstd decoder memory is capped at 16 MiB. The decompressed stream and underlying
compressed blob are drained through EOF before trusting metadata, so the registry
library completes digest integrity checks even after the tar member was found.
Oversized or unsupported payloads fail closed. These limits are currently fixed,
not runtime flags; parallel lookups and manifest/config parsing still consume
memory, so monitor the deployment's memory budget.

Only successful immutable lookups are cached, in a concurrency-safe FIFO cache of
128 entries keyed by the pinned reference, full version/architecture claim, and a
hash of the pull-secret bytes. Credential rotation therefore forces a fresh
lookup. Failures are never cached. The controller independently checks returned
metadata against its claims, even when using an injected resolver.
