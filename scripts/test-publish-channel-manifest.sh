#!/usr/bin/env bash
# Offline regression tests: the real publisher and timestamp refresher use only
# fake gh/sleep executables, temporary manifests and dummy credentials.
# Run ./scripts/test-publish-channel-manifest.sh [scenario ...].
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
script="$here/publish-channel-manifest.sh"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
mkdir -p "$tmp/bin"

cat >"$tmp/bin/gh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf '%q ' "$@" >>"$STUB_DIR/args"
printf '\n' >>"$STUB_DIR/args"
die() { echo "stub gh: $*" >&2; exit 99; }
record() { echo "$1" >>"$STUB_DIR/calls"; }
has_arg() { local arg; for arg in "${args[@]}"; do [[ "$arg" != "$1" ]] || return 0; done; return 1; }
args=("$@")
repo="" query="" target=""
while (($#)); do
  case "$1" in
    --repo) repo=$2; shift ;;
    --jq) query=$2; shift ;;
    --target) target=$2; shift ;;
  esac
  shift
done
set -- "${args[@]}"

lookup() {
  record lookup
  local n=0 result
  [[ ! -f "$STUB_DIR/reads" ]] || n=$(cat "$STUB_DIR/reads")
  n=$((n + 1))
  echo "$n" >"$STUB_DIR/reads"
  result=$STUB_SCENARIO
  case "$result" in
    race|reread_error|still_absent)
      if [[ "$n" == 1 ]]; then result=absent
      elif [[ "$result" == race ]]; then result=existing
      elif [[ "$result" == reread_error ]]; then result=service
      else result=absent; fi ;;
  esac
  case "$result" in
    forbidden) echo 'gh: API rate limit exceeded (HTTP 403)' >&2; return 1 ;;
    unauthorized) echo 'gh: Bad credentials (HTTP 401)' >&2; return 1 ;;
    inaccessible) echo 'gh: Not Found (HTTP 404)' >&2; return 1 ;;
    service) echo 'gh: Service Unavailable (HTTP 503)' >&2; return 1 ;;
    transport) echo 'dial tcp: lookup api.github.com: no such host' >&2; return 1 ;;
    partial)
      echo "channel-$STUB_CHANNEL"
      echo 'gh: Service Unavailable on page 2 (HTTP 503)' >&2
      return 1 ;;
  esac
  if [[ "$1" == view ]]; then
    if [[ "$result" == absent || "$result" == empty ]]; then
      echo 'release not found' >&2
      return 1
    fi
    echo 'channel release'
    return 0
  fi
  # Apply the real --jq expression to each page, like gh --paginate does.
  # The sought tag appears on page 2; prefix and other-channel tags on page 1
  # must never be mistaken for it. An empty repository is valid too.
  if [[ "$result" == empty ]]; then
    echo '[]' | jq -r "$query"
  else
    printf '[{"tag_name":"channel-%s-old"},{"tag_name":"v1.2.3"}]\n' "$STUB_CHANNEL" | jq -r "$query"
    if [[ "$result" != absent ]]; then
      printf '[{"tag_name":"channel-%s","draft":%s}]\n' "$STUB_CHANNEL" "${STUB_DRAFT:-false}" | jq -r "$query"
    fi
  fi
}

case "$1 $2" in
  'release view')
    # Retained so regression tests also exercise the unchanged buggy script.
    [[ "$repo" == "$STUB_REPO" && "$3" == "channel-$STUB_CHANNEL" ]] || die 'wrong release lookup target'
    lookup view ;;
  'release create')
    record create
    [[ "$repo" == "$STUB_REPO" && "$3" == "channel-$STUB_CHANNEL" ]] || die 'wrong release create target'
    has_arg --latest=false || die 'channel became latest'
    [[ "$target" == "$STUB_TARGET" ]] || die 'wrong commit pin'
    case "$STUB_SCENARIO" in
      race|reread_error|still_absent|forbidden|service)
        echo 'HTTP 422: Validation Failed; Release.tag_name already exists' >&2
        exit 1 ;;
    esac ;;
  'release upload')
    record upload
    [[ "$repo" == "$STUB_REPO" && "$3" == "channel-$STUB_CHANNEL" && "$4" == "$STUB_MANIFEST" ]] || die 'wrong upload target'
    has_arg --clobber || die 'repeat upload must clobber'
    if [[ "$STUB_SCENARIO" == upload_error ]]; then
      echo 'gh: upload failed (HTTP 502)' >&2
      exit 1
    fi
    cp "$4" "$STUB_DIR/uploaded.json" ;;
  'api --paginate')
    [[ "$3" == "repos/$STUB_REPO/releases?per_page=100" && -n "$query" ]] || die 'lookup must paginate the explicit repo'
    lookup list ;;
  "api repos/$STUB_REPO/releases/tags/channel-$STUB_CHANNEL")
    record refresh-read
    echo '{"id":42,"draft":false,"prerelease":true}' ;;
  'api --method')
    [[ "$3" == PATCH && "$4" == "repos/$STUB_REPO/releases/42" ]] || die 'wrong refresh target'
    if has_arg draft=true; then
      record draft
      if [[ "$STUB_SCENARIO" == draft_error ]]; then
        echo 'gh: draft refresh failed (HTTP 503)' >&2
        exit 1
      fi
    elif has_arg draft=false; then
      record publish
      has_arg prerelease=true && has_arg make_latest=false || die 'refresh changed release flags'
      if [[ "$STUB_SCENARIO" == refresh_error ]]; then
        echo 'gh: un-draft failed (HTTP 503)' >&2
        exit 1
      fi
      echo '{"draft":false}'
    else die 'unexpected refresh patch'; fi ;;
  *) die "unexpected command: $*" ;;
esac
EOF
cat >"$tmp/bin/sleep" <<'EOF'
#!/usr/bin/env bash
# Keep the existing timestamp repair retries offline and instant.
exit 0
EOF
chmod +x "$tmp/bin/gh" "$tmp/bin/sleep"
export PATH="$tmp/bin:$PATH"
export GH_TOKEN=publisher-test-gh-token GITHUB_TOKEN=publisher-test-github-token
export GH_REPO=wrong/repository GITHUB_REPOSITORY=source/intentd GITHUB_SHA=source-commit
unset GH_DEBUG

fail() {
  echo "FAIL: $*" >&2
  cat "$STUB_DIR/calls" "$STUB_DIR/stderr" >&2
  exit 1
}
contains() { grep -qF -- "$2" "$1" || fail "expected $2 in $1"; }
not_contains() { ! grep -qF -- "$2" "$1" || fail "unexpected $2 in $1"; }
expect_calls() {
  local actual
  actual=$(tr '\n' ' ' <"$STUB_DIR/calls")
  [[ "$actual" == "$1 " ]] || fail "expected calls '$1', got '$actual'"
}
run_publisher() {
  status=0
  "$script" "$STUB_CHANNEL" "$STUB_MANIFEST" "$STUB_REPO" >"$STUB_DIR/stdout" 2>"$STUB_DIR/stderr" || status=$?
  not_contains "$STUB_DIR/stderr" 'stub gh:'
  for stream in stdout stderr args; do
    not_contains "$STUB_DIR/$stream" "$GH_TOKEN"
    not_contains "$STUB_DIR/$stream" "$GITHUB_TOKEN"
  done
}

run_case() (
  export STUB_SCENARIO=$1 STUB_DIR="$tmp/$1" STUB_REPO=mirror/releases STUB_CHANNEL=alpha STUB_TARGET=""
  mkdir -p "$STUB_DIR"
  : >"$STUB_DIR/calls"
  export STUB_MANIFEST="$STUB_DIR/alpha manifest.json"
  echo '{"version":"1.2.3"}' >"$STUB_MANIFEST"
  case "$1" in
    same_repo) export STUB_SCENARIO=absent STUB_REPO=$GITHUB_REPOSITORY STUB_TARGET=$GITHUB_SHA ;;
    stable|beta) export STUB_SCENARIO=absent STUB_CHANNEL=$1 ;;
    drafted) export STUB_DRAFT=true ;;
  esac
  run_publisher
  case "$1" in
    forbidden|unauthorized|inaccessible|service|transport|partial)
      [[ "$status" != 0 ]] || fail 'read error must fail publication'
      expect_calls lookup
      case "$1" in
        forbidden) contains "$STUB_DIR/stderr" 'HTTP 403' ;;
        unauthorized) contains "$STUB_DIR/stderr" 'HTTP 401' ;;
        inaccessible) contains "$STUB_DIR/stderr" 'HTTP 404' ;;
        service|partial) contains "$STUB_DIR/stderr" 'HTTP 503' ;;
        transport) contains "$STUB_DIR/stderr" 'no such host' ;;
      esac
      not_contains "$STUB_DIR/stderr" 'does not exist' ;;
    absent|empty|same_repo|stable|beta)
      [[ "$status" == 0 ]] || fail 'confirmed absence must allow creation'
      expect_calls 'lookup create upload refresh-read draft publish' ;;
    existing|drafted)
      [[ "$status" == 0 ]] || fail 'existing release must accept upload'
      expect_calls 'lookup upload refresh-read draft publish' ;;
    repeat)
      [[ "$status" == 0 ]] || fail 'first publication failed'
      run_publisher
      [[ "$status" == 0 ]] || fail 'same-version publication failed'
      expect_calls 'lookup upload refresh-read draft publish lookup upload refresh-read draft publish' ;;
    race)
      [[ "$status" == 0 ]] || fail 'lost creation race must recover'
      expect_calls 'lookup create lookup upload refresh-read draft publish'
      contains "$STUB_DIR/stderr" 'HTTP 422' ;;
    reread_error|still_absent)
      [[ "$status" != 0 ]] || fail 'unconfirmed creation must fail'
      expect_calls 'lookup create lookup'
      contains "$STUB_DIR/stderr" 'HTTP 422'
      if [[ "$1" == reread_error ]]; then
        contains "$STUB_DIR/stderr" 'HTTP 503'
        not_contains "$STUB_DIR/stderr" 'does not exist'
      else contains "$STUB_DIR/stderr" 'does not exist'; fi ;;
    upload_error)
      [[ "$status" != 0 ]] || fail 'upload failure must fail publication'
      expect_calls 'lookup upload'
      contains "$STUB_DIR/stderr" 'upload failed (HTTP 502)' ;;
    draft_error)
      [[ "$status" == 0 ]] || fail 'cosmetic refresh failure must stay fail-soft'
      expect_calls 'lookup upload refresh-read draft'
      contains "$STUB_DIR/stderr" 'skipping published_at refresh' ;;
    refresh_error)
      [[ "$status" != 0 ]] || fail 'failed mandatory re-publish must fail publication'
      expect_calls 'lookup upload refresh-read draft publish publish publish publish publish'
      contains "$STUB_DIR/stderr" 'un-draft failed (HTTP 503)'
      contains "$STUB_DIR/stderr" 'stuck in DRAFT' ;;
    *) fail "unknown scenario $1" ;;
  esac
  if [[ -f "$STUB_DIR/uploaded.json" ]]; then
    cmp -s "$STUB_MANIFEST" "$STUB_DIR/uploaded.json" || fail 'uploaded manifest changed'
  fi
)

if (($# == 0)); then
  set -- forbidden service unauthorized inaccessible transport partial absent empty existing drafted same_repo stable beta repeat race reread_error still_absent upload_error draft_error refresh_error
fi
failed=0
for scenario in "$@"; do
  if run_case "$scenario"; then
    echo "PASS: $scenario"
  else
    failed=$((failed + 1))
  fi
done
[[ "$failed" == 0 ]] || { echo "$failed scenario(s) failed" >&2; exit 1; }
bash -n "$script" "$here/refresh-release-published-at.sh" "${BASH_SOURCE[0]}"
echo 'publish-channel-manifest tests passed'
