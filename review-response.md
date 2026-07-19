## Review Response

All three blocking issues have been addressed in commit 0b84118:

### 1. ✅ WSS e2e test added

Added `wss_file_tracking_load_commits_bounded` in `crates/intentd/tests/wss_integration.rs` (lines 1719-1868).

The test:
- Creates a git repo with a base commit on `main` and a workspace commit on `feat/test` branch
- Creates a workspace with `baseRef=main` and `baseCommitSha` set
- Verifies `boundarySha` is returned and matches the base commit
- Asserts only workspace commits are returned (1 commit, not 2)
- Tests `includeOlder: true` returns pre-boundary commits (the base commit)
- Tests unbounded behavior when no boundary info exists (returns all commits, `boundarySha` null)

Verification: `cargo test -p intentd --test wss_integration wss_file_tracking_load_commits_bounded` passes ✅

### 2. ✅ Detached HEAD bug fixed

Fixed in `crates/intent-git/src/history.rs` lines 248-252:
- Now gets `head_oid` early and returns `Ok(None)` immediately if `HEAD` is unavailable
- No longer uses `ZERO_SHA1` fallback which caused nonsensical merge-base results
- The `baseCommitSha` fallback section now reuses `head_oid` instead of re-querying `HEAD`

### 3. ✅ `includeOlder` parameter documented

Updated PR description with detailed documentation:
- Parameter name: `includeOlder`
- Type: optional boolean
- Default: `false`
- Behavior:
  - `false` (default): Returns commits in `boundary..HEAD` range (workspace-owned commits only)
  - `true`: Returns commits **before** the boundary (pre-workspace history, for "show previous" toggle)
  - When no boundary info exists (no `baseRef`/`baseCommitSha`), behavior is unbounded regardless of this flag

### Quality Gates

All passing:
- `cargo fmt --check` ✅
- `cargo clippy -- -D warnings` ✅  
- `cargo test -p intent-git history` ✅ (15/15 tests)
- `cargo test -p intentd --test wss_integration wss_file_tracking_load_commits_bounded` ✅

Ready for re-review.
