#!/usr/bin/env bash
# Build the Intent guest image (monorepo#1120, EE-3): a reproducible aarch64
# Debian-slim rootfs archive + conforming manifest.json for the libkrun
# microVM backend. Runs on macOS (Docker Desktop) or Linux CI — everything
# arch-sensitive happens inside linux/arm64 containers (QEMU on x86 runners).
#
# Usage:
#   scripts/build-guest-image.sh <version> [--dockerfile <path>] [--out <dir>] \
#       [--base-url <url>] [--id <image-id>]
#
#   <version>     image version (e.g. 0.1.0) — recorded in the manifest and
#                 the default asset URL (guest-image-v<version> release tag)
#   --dockerfile  alternate Dockerfile (customization path: copy
#                 guest-image/Dockerfile, add layers, rebuild — "build FROM
#                 the base"); the build context stays guest-image/
#   --out         output directory (default: guest-image/out)
#   --base-url    base URL recorded in the manifest's rootfs.url (default:
#                 the intent-hq/intentd-releases mirror for the version tag)
#   --id          image id in the manifest (default: intent-guest-base)
#
# Outputs in --out: rootfs.tar.xz, rootfs.tar.xz.sha256, manifest.json.
# Reproducibility: rootfs tar entries are sorted, mtimes clamped to
# SOURCE_DATE_EPOCH (default 0), owners forced numeric root, and xz runs
# single-threaded with no timestamp — identical inputs give identical bytes.
set -euo pipefail

usage="usage: build-guest-image.sh <version> [--dockerfile <path>] [--out <dir>] [--base-url <url>] [--id <image-id>]"
VERSION="${1:?$usage}"
shift
if [[ ! "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$ ]]; then
  echo "error: version must look like X.Y.Z[-prerelease]" >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONTEXT_DIR="$SCRIPT_DIR/../guest-image"
DOCKERFILE="$CONTEXT_DIR/Dockerfile"
OUT_DIR="$CONTEXT_DIR/out"
BASE_URL=""
IMAGE_ID="intent-guest-base"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --dockerfile) DOCKERFILE="$2"; shift 2 ;;
    --out) OUT_DIR="$2"; shift 2 ;;
    --base-url) BASE_URL="$2"; shift 2 ;;
    --id) IMAGE_ID="$2"; shift 2 ;;
    *) echo "error: unknown argument $1" >&2; echo "$usage" >&2; exit 1 ;;
  esac
done
if [[ -z "$BASE_URL" ]]; then
  BASE_URL="https://github.com/intent-hq/intentd-releases/releases/download/guest-image-v$VERSION"
fi

PLATFORM=linux/arm64
TAG="intent-guest-image:build-$VERSION"
export SOURCE_DATE_EPOCH="${SOURCE_DATE_EPOCH:-0}"
mkdir -p "$OUT_DIR"
OUT_DIR="$(cd "$OUT_DIR" && pwd)"

echo "==> [1/4] docker build ($PLATFORM, $DOCKERFILE)"
docker build --platform "$PLATFORM" -t "$TAG" -f "$DOCKERFILE" "$CONTEXT_DIR"

echo "==> [2/4] export container filesystem"
cid=$(docker create --platform "$PLATFORM" "$TAG" /usr/local/bin/intent-init)
trap 'docker rm -f "$cid" >/dev/null 2>&1 || true' EXIT
docker export "$cid" -o "$OUT_DIR/rootfs.raw.tar"
docker rm -f "$cid" >/dev/null
trap - EXIT

echo "==> [3/4] normalize + compress (reproducible tar.xz) + tool inventory"
# Repack inside a linux/arm64 debian container: GNU tar for --sort/--mtime
# normalization, plus a chroot smoke test + tool-version probe of the actual
# rootfs (versions go into the manifest inventory).
docker run --rm --platform "$PLATFORM" \
  -v "$OUT_DIR:/out" -e SOURCE_DATE_EPOCH "$TAG" bash -euo pipefail -c '
    mkdir /tree && tar -xf /out/rootfs.raw.tar -C /tree
    rm -f /tree/.dockerenv
    : > /tree/etc/hostname
    # TSI-compatible resolv.conf (docker build cannot write it: bind-mounted
    # read-only during build; docker export leaves it empty).
    printf "nameserver 8.8.8.8\nnameserver 1.1.1.1\n" > /tree/etc/resolv.conf
    # Baseline device nodes (docker export strips /dev): needed only by the
    # chroot smoke test below — removed again before repack. The guest mounts
    # devtmpfs over /dev at boot (intent-init), and device nodes in the
    # tarball would break the daemon-side non-root extraction on macOS
    # (mknod requires root, so bsdtar exits non-zero).
    mknod -m 666 /tree/dev/null c 1 3 2>/dev/null || true
    mknod -m 666 /tree/dev/zero c 1 5 2>/dev/null || true
    mknod -m 666 /tree/dev/random c 1 8 2>/dev/null || true
    mknod -m 666 /tree/dev/urandom c 1 9 2>/dev/null || true

    echo "--- chroot smoke test (linux/arm64) ---"
    chroot /tree /usr/local/bin/node --version
    chroot /tree /usr/bin/git --version
    chroot /tree /usr/bin/gh --version | head -1
    chroot /tree /usr/local/bin/rtk --version
    chroot /tree /usr/bin/rg --version | head -1
    chroot /tree /usr/bin/python3 --version
    chroot /tree /bin/sh -n /usr/local/bin/intent-init && echo "intent-init: sh syntax OK"
    chroot /tree /usr/bin/python3 -m py_compile /usr/local/bin/intent-vsock-exec && echo "intent-vsock-exec: py syntax OK"
    test -x /tree/usr/local/bin/intent-init
    test -x /tree/usr/local/bin/intent-vsock-exec
    test -s /tree/etc/resolv.conf
    test -d /tree/etc/ssl/certs

    echo "--- tool inventory ---"
    node_v=$(chroot /tree /usr/local/bin/node --version | sed s/^v//)
    git_v=$(chroot /tree /usr/bin/git --version | awk "{print \$3}")
    gh_v=$(chroot /tree /usr/bin/gh --version | head -1 | awk "{print \$3}")
    rtk_v=$(chroot /tree /usr/local/bin/rtk --version | awk "{print \$2}")
    rg_v=$(chroot /tree /usr/bin/rg --version | head -1 | awk "{print \$2}")
    py_v=$(chroot /tree /usr/bin/python3 --version | awk "{print \$2}")
    auggie_v=$(chroot /tree /usr/local/bin/node -e "console.log(require(\"/usr/local/lib/node_modules/@augmentcode/auggie/package.json\").version)")
    claude_acp_v=$(chroot /tree /usr/local/bin/node -e "console.log(require(\"/usr/local/lib/node_modules/@agentclientprotocol/claude-agent-acp/package.json\").version)")
    codex_acp_v=$(chroot /tree /usr/local/bin/node -e "console.log(require(\"/usr/local/lib/node_modules/@agentclientprotocol/codex-acp/package.json\").version)")
    pi_acp_v=$(chroot /tree /usr/local/bin/node -e "console.log(require(\"/usr/local/lib/node_modules/pi-acp/package.json\").version)")
    opencode_v=$(chroot /tree /usr/local/bin/node -e "console.log(require(\"/usr/local/lib/node_modules/opencode-ai/package.json\").version)")
    jq -n --arg node "$node_v" --arg git "$git_v" --arg gh "$gh_v" \
      --arg rtk "$rtk_v" --arg rg "$rg_v" --arg python3 "$py_v" \
      --arg auggie "$auggie_v" --arg claude "$claude_acp_v" \
      --arg codex "$codex_acp_v" --arg pi "$pi_acp_v" --arg opencode "$opencode_v" \
      "{node: \$node, git: \$git, gh: \$gh, rtk: \$rtk, ripgrep: \$rg, python3: \$python3, auggie: \$auggie, \"claude-agent-acp\": \$claude, \"codex-acp\": \$codex, \"pi-acp\": \$pi, opencode: \$opencode}" \
      > /out/tools.json

    # Scrub probe residue: the chroot smoke test/inventory above runs real
    # binaries that write state (gh mints a random device-id under
    # /root/.local/state/gh), which would break reproducibility.
    rm -rf /tree/root/.local /tree/root/.cache /tree/root/.config /tree/root/.npm
    find /tree -name "__pycache__" -type d -prune -exec rm -rf {} +
    # Drop everything under /dev before packing (see the mknod note above):
    # keep the empty /dev directory itself — intent-init needs it as the
    # devtmpfs mountpoint.
    find /tree/dev -mindepth 1 -delete

    echo "--- deterministic repack ---"
    tar --sort=name --mtime="@$SOURCE_DATE_EPOCH" --owner=0 --group=0 --numeric-owner \
      --pax-option=exthdr.name=%d/PaxHeaders/%f,delete=atime,delete=ctime \
      -C /tree -cf /out/rootfs.tar .
    rm -f /out/rootfs.tar.xz
    XZ_OPT="-T1 -6" xz --no-adjust /out/rootfs.tar
  '
rm -f "$OUT_DIR/rootfs.raw.tar"

echo "==> [4/4] manifest.json + sha256 sidecar"
ROOTFS_SHA=$(shasum -a 256 "$OUT_DIR/rootfs.tar.xz" 2>/dev/null | awk '{print $1}' \
  || sha256sum "$OUT_DIR/rootfs.tar.xz" | awk '{print $1}')
printf '%s  rootfs.tar.xz\n' "$ROOTFS_SHA" > "$OUT_DIR/rootfs.tar.xz.sha256"
SIZE_BYTES=$(wc -c < "$OUT_DIR/rootfs.tar.xz" | tr -d ' ')

jq -n \
  --arg id "$IMAGE_ID" --arg version "$VERSION" --arg sha "$ROOTFS_SHA" \
  --arg url "$BASE_URL/rootfs.tar.xz" --argjson size "$SIZE_BYTES" \
  --slurpfile tools "$OUT_DIR/tools.json" \
  '{
    schema: 1, id: $id, version: $version, arch: "aarch64",
    rootfs: { url: $url, format: "tar.xz", sha256: $sha, sizeBytes: $size },
    vsockExec: { init: "/usr/local/bin/intent-init", port: 4088, protocol: "intent-exec/1" },
    providers: { auggie: true, "claude-code": true, codex: true, pi: true,
                 droid: true, grok: true, opencode: true,
                 unsloth: false, cortex: false, mock: false },
    tools: $tools[0]
  }' > "$OUT_DIR/manifest.json"
rm -f "$OUT_DIR/tools.json"

echo "==> done:"
ls -lh "$OUT_DIR/rootfs.tar.xz" "$OUT_DIR/manifest.json"
echo "rootfs sha256: $ROOTFS_SHA"
