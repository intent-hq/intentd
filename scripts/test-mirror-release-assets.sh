#!/usr/bin/env bash
# Offline functional tests. Every gh call uses a strict fake and dummy tokens;
# sleep/date fixtures make retry bounds deterministic without real waiting.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
script="$here/mirror-release-assets.sh"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
mkdir -p "$tmp/bin"
cat >"$tmp/bin/gh" <<'STUB'
#!/usr/bin/env bash
set -euo pipefail
args=("$@")
printf '%q ' "$@" >>"$STUB_DIR/args"
printf '\n' >>"$STUB_DIR/args"
die() { echo "stub gh: $*" >&2; exit 99; }
record() { echo "$1" >>"$STUB_DIR/calls"; }
has_arg() { local arg; for arg in "${args[@]}"; do [[ "$arg" != "$1" ]] || return 0; done; return 1; }
repo='' notes='' dir='' pattern=''
while (($#)); do
  case "$1" in
    --repo) repo=$2; shift ;;
    --notes-file) notes=$2; shift ;;
    --dir) dir=$2; shift ;;
    --pattern) pattern=$2; shift ;;
  esac
  shift
done
set -- "${args[@]}"
lookup() {
  record lookup
  local n=0 result=$STUB_SCENARIO status=200 headers='' body rc=1
  [[ ! -f "$STUB_DIR/reads" ]] || n=$(cat "$STUB_DIR/reads")
  n=$((n + 1)); echo "$n" >"$STUB_DIR/reads"
  case "$result" in
    race|still_absent|reread_error)
      if ((n == 1)); then result=absent
      elif [[ "$result" == race ]]; then result=existing
      elif [[ "$result" == still_absent ]]; then result=absent
      else result=service; fi ;;
    transient|quota|secondary|throttle|retry_date|both_headers|past_reset|transport_recovery|boundary|available_quota)
      if ((n > 1)); then result=existing; fi ;;
  esac
  body='{"id":42,"tag_name":"v1.2.3"}'
  case "$result" in
    absent|explicit_absent|stable_absent) status=404 ;;
    unauthorized) status=401 ;;
    forbidden) status=403 ;;
    quota) status=403; headers='X-RateLimit-Remaining: 0\r\nx-ratelimit-reset: 1700000005\r\n' ;;
    past_reset) status=403; headers='X-RateLimit-Remaining: 0\r\nX-RateLimit-Reset: 1699999990\r\n' ;;
    secondary) status=403; headers='Retry-After: 6\r\n' ;;
    boundary) status=429; headers='Retry-After: 30\r\n' ;;
    bad_reset) status=403; headers='X-RateLimit-Remaining: 0\r\nX-RateLimit-Reset: unsafe-token-value\r\n' ;;
    throttle) status=429; headers='retry-after: 7\r\n' ;;
    retry_date) status=429; headers='Retry-After: Tue, 14 Nov 2023 22:13:28 GMT\r\n' ;;
    both_headers) status=429; headers='Retry-After: 3\r\nX-RateLimit-Reset: 1700000010\r\n' ;;
    long_reset) status=403; headers='X-RateLimit-Remaining: 0\r\nX-RateLimit-Reset: 1700003600\r\n' ;;
    long_retry) status=429; headers='Retry-After: 9999999999999999999999\r\n' ;;
    bad_retry) status=429; headers='Retry-After: unsafe-token-value\r\n' ;;
    wait_budget) status=429; headers='Retry-After: 20\r\n' ;;
    available_quota) status=503; headers='X-RateLimit-Remaining: 4999\r\nX-RateLimit-Reset: 1700003600\r\n' ;;
    service|transient|sleep_error|time_error) status=503 ;;
    transport|transport_recovery) status=''; ;;
    malformed) body='not json'; rc=0 ;;
    wrong_tag) body='{"id":42,"tag_name":"v9.9.9"}'; rc=0 ;;
    empty) body='{}'; rc=0 ;;
    multiple) body=$'{"id":42,"tag_name":"v1.2.3"}\n{"id":43,"tag_name":"v1.2.3"}'; rc=0 ;;
    partial) status=200 ;;
    unclassified) status='' ;;
    *) rc=0 ;;
  esac
  if [[ "$1" == api ]]; then
    [[ -z "$status" ]] || printf 'HTTP/2.0 %s Test\r\n%b\r\n%s\n' "$status" "$headers" "$body"
  elif [[ "$status" == 200 ]]; then
    echo "$body"
  fi
  if ((rc != 0)); then
    # Deliberately hostile diagnostics must never reach mirror logs.
    echo "gh: lookup failed HTTP ${status:-unknown}; Authorization: Bearer $GH_TOKEN; $GITHUB_TOKEN; $DEST_GH_TOKEN" >&2
  fi
  return "$rc"
}
case "$1 $2" in
  'api repos/mirror/releases/releases/tags/v1.2.3')
    [[ "$GH_TOKEN" == "$DEST_GH_TOKEN" ]] || die 'wrong lookup token'
    has_arg --include || die 'lookup needs HTTP metadata'
    lookup api ;;
  'release view')
    if [[ "$repo" == source/intentd ]]; then
      record source
      [[ "$GH_TOKEN" == mirror-dummy-source ]] || die 'wrong source token'
      echo '{"publishedAt":"2026-09-28T00:00:00Z","isPrerelease":true,"body":"Download https://github.com/source/intentd/releases/download/v1.2.3/intentd-x86_64-linux.tar.xz","assets":[{"name":"intentd-x86_64-linux.tar.xz"},{"name":"intentd-x86_64-linux.tar.xz.sha256"},{"name":"ignored.txt"}]}' | jq --arg scenario "$STUB_SCENARIO" '.isPrerelease = ($scenario != "stable_absent")'
    elif [[ "$repo" == mirror/releases ]]; then
      if has_arg --json; then
        record assets
        echo '{"assets":[{"name":"intentd-x86_64-linux.tar.xz"},{"name":"intentd-aarch64-linux.zip"},{"name":"keep.txt"}]}'
      else lookup view; fi
    else die 'wrong view repo'; fi ;;
  'release download')
    [[ "$repo" == source/intentd && -n "$dir" ]] || die 'wrong download target'
    printf 'fixture asset %s\n' "$pattern" >"$dir/$pattern" ;;
  'release create')
    record create
    [[ "$repo" == mirror/releases && "$3" == v1.2.3 ]] || die 'wrong create target'
    has_arg --latest=false || die 'release became latest'
    if [[ "$STUB_SCENARIO" == stable_absent ]]; then
      ! has_arg --prerelease || die 'stable release became prerelease'
    else has_arg --prerelease || die 'prerelease flag missing'; fi
    cp "$notes" "$STUB_DIR/notes"
    case "$STUB_SCENARIO" in
      race|still_absent|reread_error)
        echo "HTTP 422: already exists; $DEST_GH_TOKEN" >&2; exit 1 ;;
    esac ;;
  'release edit')
    record edit
    [[ "$repo" == mirror/releases && "$3" == v1.2.3 && "${#args[@]}" == 7 ]] || die 'edit changed identity or flags'
    cp "$notes" "$STUB_DIR/notes" ;;
  'release delete-asset')
    record prune
    [[ "$repo" == mirror/releases && "$4" == intentd-aarch64-linux.zip ]] || die 'wrong stale asset' ;;
  'release upload')
    record upload
    [[ "$repo" == mirror/releases && "${#args[@]}" == 8 ]] || die 'wrong upload target/count'
    has_arg --clobber || die 'asset refresh must clobber'
    [[ "$(cat "$4")" == 'fixture asset intentd-x86_64-linux.tar.xz' ]] || die 'wrong asset content'
    [[ "$(cat "$5")" == 'fixture asset intentd-x86_64-linux.tar.xz.sha256' ]] || die 'wrong sidecar' ;;
  *) die "unexpected command: $*" ;;
esac
STUB
cat >"$tmp/bin/sleep" <<'STUB'
#!/usr/bin/env bash
set -euo pipefail
echo "sleep:$1" >>"$STUB_DIR/calls"
[[ "$STUB_SCENARIO" != sleep_error ]] || exit 1
echo "$(( $(cat "$STUB_DIR/now") + $1 ))" >"$STUB_DIR/now"
STUB
cat >"$tmp/bin/date" <<'STUB'
#!/usr/bin/env bash
set -euo pipefail
[[ "$STUB_SCENARIO" != time_error ]] || exit 1
case "$*" in
  '+%s'|'-u +%s') cat "$STUB_DIR/now" ;;
  '-u -d Tue, 14 Nov 2023 22:13:28 GMT +%s') echo 1700000008 ;;
  *) exit 1 ;;
esac
STUB
chmod +x "$tmp/bin/"*
export PATH="$tmp/bin:$PATH"
export GH_TOKEN=mirror-dummy-source GITHUB_TOKEN=mirror-dummy-github DEST_GH_TOKEN=mirror-dummy-dest
export SOURCE_REPO=source/intentd DEST_REPO=mirror/releases GH_REPO=wrong/repository
unset GH_DEBUG GH_HOST RELEASE_NOTES RELEASE_TITLE ASSET_REGEX PRUNE_STALE
fail() { echo "FAIL: $*" >&2; cat "$STUB_DIR/calls" "$STUB_DIR/stderr" >&2; exit 1; }
contains() { grep -qF -- "$2" "$1" || fail "expected $2 in $1"; }
not_contains() { ! grep -qF -- "$2" "$1" || fail "unexpected $2 in $1"; }
expect_calls() {
  local actual
  actual=$(tr '\n' ' ' <"$STUB_DIR/calls")
  [[ "$actual" == "source $1 " ]] || fail "expected '$1', got '$actual'"
}
run_case() (
  export STUB_SCENARIO=$1 STUB_DIR="$tmp/$1" PRUNE_STALE=true
  mkdir -p "$STUB_DIR"
  : >"$STUB_DIR/calls"
  echo 1700000000 >"$STUB_DIR/now"
  case "$1" in
    explicit|explicit_absent) export RELEASE_NOTES='Explicit sitter notes' ;;
    no_prune) unset PRUNE_STALE ;;
  esac
  status=0
  bash "$script" v1.2.3 >"$STUB_DIR/stdout" 2>"$STUB_DIR/stderr" || status=$?
  not_contains "$STUB_DIR/stderr" 'stub gh:'
  for stream in stdout stderr args; do
    for token in "$GH_TOKEN" "$GITHUB_TOKEN" "$DEST_GH_TOKEN"; do
      not_contains "$STUB_DIR/$stream" "$token"
    done
  done
  case "$1" in
    forbidden|unauthorized|sleep_error|time_error|long_reset|long_retry|bad_retry|bad_reset|malformed|multiple|wrong_tag|empty|unclassified|partial|service|transport|wait_budget|still_absent|reread_error)
      [[ "$status" != 0 ]] || fail 'lookup failure must fail mirroring'
      not_contains "$STUB_DIR/stderr" 'does not exist'
      case "$1" in
        service|transport|unclassified) expect_calls 'lookup sleep:1 lookup sleep:2 lookup' ;;
        sleep_error) expect_calls 'lookup sleep:1' ;;
        wait_budget) expect_calls 'lookup sleep:20 lookup' ;;
        still_absent) expect_calls 'lookup create lookup'; contains "$STUB_DIR/stderr" 'HTTP 404' ;;
        reread_error) expect_calls 'lookup create lookup sleep:1 lookup sleep:2 lookup'; contains "$STUB_DIR/stderr" 'HTTP 503' ;;
        *) expect_calls lookup ;;
      esac
      case "$1" in
        forbidden) contains "$STUB_DIR/stderr" 'HTTP 403' ;;
        unauthorized) contains "$STUB_DIR/stderr" 'HTTP 401' ;;
        long_reset) contains "$STUB_DIR/stderr" '1700003600'; contains "$STUB_DIR/stderr" 'budget' ;;
        long_retry|wait_budget) contains "$STUB_DIR/stderr" 'budget' ;;
      esac ;;
    *)
      [[ "$status" == 0 ]] || fail 'mirror should succeed'
      case "$1" in
        absent|explicit_absent|stable_absent) expect_calls 'lookup create assets prune upload' ;;
        race) expect_calls 'lookup create lookup assets prune upload' ;;
        explicit) expect_calls 'lookup assets prune upload' ;;
        no_prune) expect_calls 'lookup edit upload' ;;
        transient|past_reset|transport_recovery|available_quota) expect_calls 'lookup sleep:1 lookup edit assets prune upload' ;;
        quota) expect_calls 'lookup sleep:5 lookup edit assets prune upload' ;;
        secondary) expect_calls 'lookup sleep:6 lookup edit assets prune upload' ;;
        boundary) expect_calls 'lookup sleep:30 lookup edit assets prune upload' ;;
        throttle) expect_calls 'lookup sleep:7 lookup edit assets prune upload' ;;
        retry_date) expect_calls 'lookup sleep:8 lookup edit assets prune upload' ;;
        both_headers) expect_calls 'lookup sleep:10 lookup edit assets prune upload' ;;
        existing) expect_calls 'lookup edit assets prune upload' ;;
        *) fail "unknown scenario $1" ;;
      esac
      if [[ "$1" == explicit_absent ]]; then
        [[ "$(cat "$STUB_DIR/notes")" == 'Explicit sitter notes' ]] || fail 'explicit create notes changed'
      elif [[ -f "$STUB_DIR/notes" ]]; then
        contains "$STUB_DIR/notes" 'https://github.com/mirror/releases/releases/download/'
        not_contains "$STUB_DIR/notes" 'https://github.com/source/intentd/'
      fi ;;
  esac
)
if (($# == 0)); then
  set -- forbidden unauthorized sleep_error time_error service transport malformed wrong_tag empty multiple partial unclassified long_reset long_retry bad_retry bad_reset wait_budget existing absent explicit_absent stable_absent race still_absent reread_error explicit no_prune transient transport_recovery available_quota quota secondary boundary throttle retry_date both_headers past_reset
fi
failed=0
for scenario in "$@"; do
  if run_case "$scenario"; then echo "PASS: $scenario"; else failed=$((failed + 1)); fi
done
[[ "$failed" == 0 ]] || { echo "$failed scenario(s) failed" >&2; exit 1; }
echo 'mirror-release-assets tests passed'
