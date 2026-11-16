# The intent-node base image, read by intent-node.mjs (BASE_IMAGE). Pinned by
# digest so the layers above it stay cached across releases; Dependabot bumps
# the digest weekly, which rebuilds every layer once and brings Debian fixes.
FROM node:26-bookworm@sha256:b7087bc4d973034b30854b378ba107f9fab622b47514cb7e2592e41b8bab8d49
