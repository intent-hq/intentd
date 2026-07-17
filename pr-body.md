Implements workspace boundary resolution for `file-tracking.loadCommits` so the Changes panel only shows workspace-owned commits.

## Changes

### Core Implementation

- **intent-git/history.rs**: New boundary resolution logic
  - `resolve_workspace_boundary()`: Prefers merge-base of HEAD vs `origin/<baseRef>` or `<baseRef>` (rebase-resilient), falls back to `baseCommitSha` when it's a valid ancestor
  - `history_bounded()`: Returns commits in `boundary..HEAD` range with optional `includeOlder` parameter for pre-boundary commits (powers FE "show previous" toggle)

- **intent-services/lib.rs**: Updated `file_tracking_load_commits`
  - Calls `resolve_workspace_boundary()` to get boundary SHA
  - Returns `boundarySha` in result envelope: `{ commits, boundarySha, nextToken }`
  - Added `includeOlder` parameter (optional bool, default false)
  - Returns empty when boundary info exists but fails to resolve (safety net against showing arbitrary base-branch commits)

- **intent-core/traits.rs**: Updated `WorkspaceApi` trait signature to include `includeOlder` parameter

- **intent-transport/router.rs**: Wired `includeOlder` parameter from JSON-RPC request

### Tests

Added 7 unit tests in intent-git covering all boundary scenarios:
- No boundary info returns None
- baseCommitSha fallback when valid ancestor
- Non-ancestor SHA rejected
- Bounded returns commits after boundary
- At-head boundary returns empty
- No boundary returns all commits
- includeOlder fetches pre-boundary commits

Updated existing test in intent-services to pass new parameter.

All tests pass: `cargo fmt --check && cargo clippy -- -D warnings && cargo test`

## Wire Shape

**Additive, backward-compatible change**

Result now returns:
```typescript
{
  commits: CommitWithAttribution[],
  boundarySha: string | null,  // NEW
  nextToken: string | null
}
```

Request accepts optional parameter:
```typescript
{
  workspaceId: string,
  limit?: number,
  nextToken?: string,
  includeOlder?: boolean  // NEW (optional, default: false)
                           // When true, returns commits BEFORE the workspace boundary
                           // (powers FE "show previous" toggle)
                           // When false (default), returns commits in boundary..HEAD range
}
```

**Parameter details:**
- `includeOlder` (optional boolean, default `false`):
  - `false` (default): Returns commits in `boundary..HEAD` range (workspace-owned commits only)
  - `true`: Returns commits **before and including** the boundary (pre-workspace history, for "show previous" toggle; the boundary commit itself is included)
  - When no boundary info exists (no `baseRef`/`baseCommitSha`), behavior is unbounded regardless of this flag

PROTOCOL.md update will be handled in monorepo Task 3.

## Related

Part of workspace-start-boundary fix (STAB-89). This PR implements the BE side; FE integration follows in cloudlands-fe.

Fixes the Changes panel "Workspace start" marker to reflect the workspace's actual base commit rather than an arbitrary point ~50 commits in the past.
