# The intent-node base image, read by intent-node.mjs (BASE_IMAGE). Pinned by
# digest so the layers above it stay cached across releases; Dependabot bumps
# the digest weekly, which rebuilds every layer once and brings Debian fixes.
FROM node:24-bookworm@sha256:9d266751ca3d7fdbe651c878ffd5ad0a42fed49a18c283509267c97f025b7496
