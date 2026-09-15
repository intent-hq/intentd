# Intent guest image

The aarch64 Debian-slim rootfs booted by the libkrun microVM backend for
sandboxed agent workspaces ([intent-hq/monorepo#1120](https://github.com/intent-hq/monorepo/issues/1120)).
Built by [`scripts/build-guest-image.sh`](../scripts/build-guest-image.sh),
published as `guest-image-vX.Y.Z` release assets on this repo and mirrored to
the public [intent-hq/intentd-releases](https://github.com/intent-hq/intentd-releases)
repo. The daemon downloads images on first use from a manifest URL,
sha256-verifies the rootfs, and caches it under `<data_dir>/guest-images/`
(see `intent-services::sandbox_image`).

## Contents

| File | Purpose |
| --- | --- |
| `Dockerfile` | The base image recipe: Node 24 Debian-slim + git/gh/rtk/ripgrep/shell baseline, ca-certificates, pinned provider CLIs, vsock exec agent. |
| `intent-init` | Guest init entrypoint (`vsockExec.init` in the manifest): mounts pseudo-filesystems, execs the exec agent. |
| `intent-vsock-exec` | The vsock exec agent (protocol `intent-exec/1`, port 4088): one vsock connection = one command with raw stdio bridging. |

## Building

```bash
# From the repo root; requires Docker (Docker Desktop on macOS is fine —
# everything arch-sensitive runs inside linux/arm64 containers).
./scripts/build-guest-image.sh 0.1.0
```

Outputs land in `guest-image/out/`: `rootfs.tar.xz`, `rootfs.tar.xz.sha256`,
and a conforming `manifest.json`. The build is reproducible: tar entries are
sorted, mtimes clamped to `SOURCE_DATE_EPOCH` (default 0), ownership
normalized — identical inputs give identical bytes. Version pins live as
`ARG`s at the top of the `Dockerfile`; the manifest's tool inventory is probed
from the built rootfs, not assumed.

## Image contract (manifest schema v1)

Any image whose manifest conforms works — the base image is just the default.
The daemon validates (see `sandbox_image::ImageManifest::validate`):

- `schema: 1`, `arch: "aarch64"`, `rootfs.format: "tar.xz"`,
  `rootfs.sha256` (64-char hex, verified on download)
- `vsockExec`: `init` (absolute guest path), `port` (non-zero),
  `protocol: "intent-exec/1"`
- `providers`: provider-id → included map (informational for gating)
- `tools`: name → version inventory (informational)

Hard requirements carried by the base image that any custom image must keep:
glibc userland (NOT Alpine/musl), `ca-certificates` installed (codex TLS
fails without), a non-empty TSI-compatible `/etc/resolv.conf`, and the vsock
exec agent contract above.

## Customization — build FROM the base

The supported customization path is rebuilding with your own Dockerfile
against this build context:

1. Copy `guest-image/Dockerfile` and append your layers (extra apt packages,
   language toolchains, internal CA certs, …). Keep the contract items above.
2. Build with your Dockerfile and a manifest URL base you control:

   ```bash
   ./scripts/build-guest-image.sh 1.0.0 \
     --dockerfile my/Dockerfile \
     --id my-org-guest \
     --base-url https://example.com/guest-images/v1.0.0
   ```

3. Host `rootfs.tar.xz` + `manifest.json` at that base URL.
4. Point a repo at it via `.intent/config.json`:

   ```json
   {
     "executionEnvironment": {
       "image": {
         "manifestUrl": "https://example.com/guest-images/v1.0.0/manifest.json",
         "sha256": "<optional hex sha256 of manifest.json>"
       }
     }
   }
   ```

Image resolution order at spawn: repo `.intent/config.json` → sandbox-profile
default → built-in pin (`BUILTIN_IMAGE_VERSION` in
`crates/intent-services/src/sandbox_image.rs`).

## Releasing

Dispatch the `Release guest image` workflow with the version. It builds on a
native arm64 runner, publishes `guest-image-v<version>` release assets here,
and mirrors them to intentd-releases (dual-publish: the release's
`manifest.json` points at the mirror — the canonical public URL and the
built-in pin's target — while `manifest-intentd.json` points at this repo's
assets). To make a new version the built-in pin, bump `BUILTIN_IMAGE_VERSION`.
