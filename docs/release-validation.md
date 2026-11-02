# Release Payload Validation

Before updating a `HostedCluster` or `NodePool`, HUC validates that the configured release image exists in the registry, matches the claimed version, and is compatible with the cluster's CPU architecture.

---

## 1. Release Image Syntax & Digest Pinning

Every release image must use a fully-qualified, digest-pinned pullspec:

```text
registry.example.com/openshift-release-dev/ocp-release:<version>-<arch>@sha256:<64-hex-digest>
```

- **Digest Pinning:** Tags are mutable and can be changed in a registry. HUC uses the `@sha256:` digest as the immutable payload identity.
- **Syntactic Validation:** Admission webhooks validate that the pullspec matches the required format and that version tags follow semantic versioning.
- **Architecture Consistency:** The CPU architecture indicated in the tag (e.g. `x86_64`, `aarch64`) must match all associated NodePools (`spec.arch`, defaulting to `amd64`). Multi-architecture payloads that do not specify a platform suffix are currently rejected to guarantee consistent architecture selection.

---

## 2. Registry Verification

Before modifying any cluster specifications, HUC directly contacts the image registry to inspect the release payload metadata:

1. **Layer Streaming:** HUC streams image metadata without downloading full payload layers. It inspects the `release-manifests/image-references` file inside the payload to verify:
   - The payload version (`metadata.name`) matches the claimed version.
   - The image configuration matches the target architecture.
2. **Digest Integrity:** The downloaded bytes are checked against the specified SHA-256 digest.
3. **Caching:** Successfully verified payloads are cached in an in-memory FIFO cache (up to 128 entries) keyed by the pullspec and pull-secret hash. Failed lookups are never cached and retry after 30 seconds.

---

## 3. Pull Secrets & Authentication

HUC authenticates to container registries using the pull secret configured on the `HostedCluster`:

- **Secret Retrieval:** HUC reads the secret referenced by `HostedCluster.spec.pullSecret` from the HostedCluster namespace.
- **Format:** The secret must contain a valid `.dockerconfigjson` entry.
- **Security:** Secret contents, tokens, and raw registry credentials are never logged or stored in CR status fields. If authentication fails, HUC surfaces a sanitized verification error and retries periodically.
- **Anonymous Registries:** Anonymous access is permitted only if the `HostedCluster` does not define a pull secret (primarily used in local development fixtures).

---

## 4. Disconnected & Air-Gapped Environments

In disconnected environments:

- **Mirrored Pullspecs:** Configure the `HostedClusterUpgrade` to point directly to the reachable mirror registry containing the pinned payload digest.
- **Mirror Credentials:** Ensure the HostedCluster pull secret includes valid credentials for the mirror registry.
- **Custom Certificates:** If the mirror registry uses a private Certificate Authority (CA), ensure the coordinator deployment trusts that CA in its container trust store.

---

## 5. Resource Limits & Safety Bounds

To ensure controller reliability when interacting with external registries:
- **Request Timeout:** 30 seconds per registry request.
- **Data Limits:** Maximum 64 MiB per compressed layer, 256 MiB total decompressed metadata, and 16 MiB decoder memory.
- Payloads exceeding these bounds fail closed to prevent denial-of-service or memory exhaustion on the controller.
