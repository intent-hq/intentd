## Summary
Make daemon-side binary discovery find tools the same way a user's terminal would, so `host.findBinary` / `host.toolAvailability` / `host.checkAuggie` succeed in Finder-launched (minimal PATH) sessions.

## Changes
- **path_utils.rs**: Add login-shell PATH capture (unix only, cached, 2s timeout)
  - `capture_login_shell_path_with()`: Run `$SHELL -lc 'printf %s "$PATH"'` once
  - `login_shell_dirs()`: Cached accessor using `OnceLock`
  - Silent degradation on failure (timeout, spawn error, non-unix)
- **path_utils.rs**: Update `enriched_tool_dirs()` to include login-shell PATH entries
- **host_ops.rs**: Update `resolve_binary_path()` precedence:
  1. PATH which (existing)
  2. Caller-supplied common_paths hints (existing)
  3. **NEW**: Enriched tool dirs (hardcoded + login-shell PATH)
  4. Common OS directories fallback (existing)

## Testing
- Unit tests with injectable fake shell (no reliance on CI machine's real shell config)
- Tests verify silent degradation on invalid shell / missing $SHELL
- All existing tests pass
- `cargo test -p intent-core -p intent-transport` ✅
- `cargo clippy -p intent-core -p intent-transport -- -D warnings` ✅
- `cargo fmt --check` ✅

## Verification
```bash
cargo test -p intent-transport -p intent-core
cargo clippy -p intent-core -p intent-transport -- -D warnings
cargo fmt --check
```

Fixes host.findBinary / host.toolAvailability / host.checkAuggie in Finder-launched sessions by searching the login-shell PATH for binaries that live in directories only present there (e.g. `~/Library/Application Support/revedev-*/bin`).
