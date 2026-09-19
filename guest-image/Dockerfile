# Intent guest base image (monorepo#1120, EE-3): aarch64 Debian-slim rootfs
# booted by the libkrun microVM helper. Built via scripts/build-guest-image.sh
# (docker buildx, linux/arm64) and exported as a normalized rootfs.tar.xz +
# manifest.json — NOT run as a container in production.
#
# Contract requirements (see intent-services::sandbox_image):
# - glibc userland (Debian-slim; NOT Alpine/musl)
# - ca-certificates present (codex TLS fails without)
# - TSI-compatible /etc/resolv.conf baked in (libkrun TSI hijacks socket
#   connects, but resolvers must be non-empty for libc/Node DNS paths)
# - vsock exec agent at /usr/local/bin/intent-vsock-exec, started by
#   /usr/local/bin/intent-init (the manifest's vsockExec.init)
#
# Version pins are build args so scripts/build-guest-image.sh is the single
# place that bumps them (recorded into the manifest tool inventory).
# trixie (Debian 13, glibc 2.41): the rtk aarch64-gnu binary needs GLIBC 2.39+.
FROM node:24-trixie-slim

ARG GH_VERSION=2.97.0
ARG RTK_VERSION=0.44.2
ARG AUGGIE_VERSION=0.34.0
ARG CLAUDE_AGENT_ACP_VERSION=0.60.0
ARG CODEX_ACP_VERSION=1.1.7
ARG PI_ACP_VERSION=0.0.31
ARG OPENCODE_VERSION=1.18.11
ARG DROID_VERSION=0.178.0
ARG GROK_VERSION=0.2.114

# Shell baseline + toolchain. `--no-install-recommends` keeps the image lean;
# fd ships as `fdfind` on Debian, symlinked to the conventional name below.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        bash ca-certificates curl git jq python3 build-essential \
        ripgrep fd-find xz-utils unzip procps openssh-client \
    && rm -rf /var/lib/apt/lists/* \
    && ln -s /usr/bin/fdfind /usr/local/bin/fd

# GitHub CLI — official arm64 .deb, pinned.
RUN curl -fsSL -o /tmp/gh.deb \
        "https://github.com/cli/cli/releases/download/v${GH_VERSION}/gh_${GH_VERSION}_linux_arm64.deb" \
    && dpkg -i /tmp/gh.deb \
    && rm /tmp/gh.deb

# rtk — pinned aarch64-gnu release tarball.
RUN curl -fsSL -o /tmp/rtk.tar.gz \
        "https://github.com/rtk-ai/rtk/releases/download/v${RTK_VERSION}/rtk-aarch64-unknown-linux-gnu.tar.gz" \
    && tar -xzf /tmp/rtk.tar.gz -C /usr/local/bin rtk \
    && chmod +x /usr/local/bin/rtk \
    && rm /tmp/rtk.tar.gz

# Provider CLIs / ACP adapters with Linux arm64 artifacts, pinned globally so
# spawns never hit npx cold-install latency in-guest. The unsloth CLI stays
# host-side (Metal); opencode is included as its own ACP runtime (provider
# gating for opencode/unsloth is a daemon-side decision, libkrunfw#137).
RUN npm install -g \
        "@augmentcode/auggie@${AUGGIE_VERSION}" \
        "@agentclientprotocol/claude-agent-acp@${CLAUDE_AGENT_ACP_VERSION}" \
        "@agentclientprotocol/codex-acp@${CODEX_ACP_VERSION}" \
        "pi-acp@${PI_ACP_VERSION}" \
        "opencode-ai@${OPENCODE_VERSION}" \
    && npm cache clean --force

# droid + grok — native Linux aarch64 binaries, pinned.
RUN curl -fsSL -o /usr/local/bin/droid \
        "https://downloads.factory.ai/factory-cli/releases/${DROID_VERSION}/linux/arm64/droid" \
    && curl -fsSL -o /usr/local/bin/grok \
        "https://storage.googleapis.com/grok-build-public-artifacts/cli/grok-${GROK_VERSION}-linux-aarch64" \
    && chmod +x /usr/local/bin/droid /usr/local/bin/grok

# vsock exec agent + init entrypoint (the manifest's vsockExec contract).
# NOTE: /etc/resolv.conf is bind-mounted (read-only) during docker build, so
# the TSI-compatible resolv.conf is baked into the rootfs by the repack step
# in scripts/build-guest-image.sh and re-asserted at boot by intent-init.
COPY intent-vsock-exec /usr/local/bin/intent-vsock-exec
COPY intent-init /usr/local/bin/intent-init
RUN chmod +x /usr/local/bin/intent-vsock-exec /usr/local/bin/intent-init \
    && mkdir -p /workspace
