#!/bin/sh
# Bazel credential helper for --config=remote-cache (see .bazelrc).
#
# Bazel runs `remote-cache-credentials.sh get` with a JSON request such as
# {"uri": "https://host/path"} on stdin and the client's environment. Reply
# with the Bearer token from BAZEL_REMOTE_CACHE_TOKEN only when the URI is on
# BAZEL_REMOTE_CACHE_URL's host, so the token never reaches crates.io or other
# download hosts. Any other request gets no headers.
set -eu
[ "${1:-}" = get ] || { echo "usage: $0 get" >&2; exit 2; }
request=$(cat)
host_of() { printf '%s' "$1" | sed -E 's#^[a-zA-Z][a-zA-Z0-9+.-]*://##; s#[/?].*$##'; }
uri=$(printf '%s' "$request" | sed -nE 's/.*"uri"[[:space:]]*:[[:space:]]*"([^"]*)".*/\1/p')
cache_host=$(host_of "${BAZEL_REMOTE_CACHE_URL:-}")
token=${BAZEL_REMOTE_CACHE_TOKEN:-}
if [ -n "$token" ] && [ -n "$cache_host" ] && [ "$(host_of "$uri")" = "$cache_host" ]; then
  printf '{"headers":{"Authorization":["Bearer %s"]}}\n' "$token"
else
  printf '{"headers":{}}\n'
fi
