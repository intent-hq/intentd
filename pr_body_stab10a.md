Implement git.getConfig to retire FE filesystem reads of .git/config.

## Summary

STAB-10a task: give the daemon the workspace-context surface so the FE can stop reading .git/config and context data from disk.

**Outcome:**
- workspace.getContext / workspace.updateContext already existed (full implementation)
- Added git.getConfig RPC to complete the surface

## RPCs Shipped

1. workspace.getContext (already existed)
   - Returns { items: ContextItem[] }
   - Router: workspace.getContext

2. workspace.updateContext (already existed)
   - Returns { items: ContextItem[] }
   - Emits workspace:context-changed event
   - Router: workspace.updateContext

3. git.getConfig (newly implemented)
   - Returns { config: String } (raw .git/config content)
   - Router: git.getConfig
   - Empty string for remote workspaces / non-repos
   - Parent git root lookup fallback (FE parity)

## Implementation

Files Changed:
- crates/intent-core/src/traits.rs - Added git_get_config trait method
- crates/intent-services/src/lib.rs - Implemented git_get_config service (34 lines)
- crates/intent-transport/src/router.rs - Added git.getConfig route

## Gates

✅ cargo fmt --check
✅ cargo clippy -- -D warnings
✅ cargo build
✅ cargo test --lib (365 tests pass)

## Next Steps

FE adoption task will update DaemonWorkspaceRepository to call these RPCs instead of reading filesystem.

## Resolves

STAB-10a: implement context.* and git-config RPCs (intentd)

## Related

- docs/01_stabilizing/KNOWN_ISSUES.md STAB-10
- cloudlands-fe PR #60 (documented FS fallbacks left in DaemonWorkspaceRepository)
