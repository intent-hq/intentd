**Problem:**
In sidecar-managed runs (dev AND packaged; FE spawns `intentd serve --listen uds`), toggling `server.wsApi.enabled=true` via the Settings UI fails with:
```
Internal("WSS listener not available (daemon started with --listen uds)")
```
The compensating hook reverts the setting, and the WSS listener never starts.

**Root cause:**
`main.rs` only constructed `WsRuntimeControl` when `serve_tcp_enabled` (--listen tcp/both). Under `--listen uds`, `DaemonControl.ws_runtime` was `None`, so `start_ws_listener` failed.

**Fix:**
- `WsRuntimeControl` now constructed for ALL listen modes (uds/tcp/both), not just tcp/both
- Boot-time auto-start remains ONLY for --listen tcp/both (CLI/env always win over persisted settings)
- Runtime toggle via `settings.update server.wsApi.enabled=true` now works for all modes including --listen uds (the sidecar contract)
- Persisted settings (`server.wsApi.enabled`) NOT honored at boot — only CLI `--listen` and env (`INTENTD_TCP_PORT`, `INTENTD_DISCOVERY`) matter. Persisted settings are only applied via runtime `settings.update` hooks.
- TLS cert + token store are always provisioned (even for --listen uds) so runtime toggle has all required args. Trade-off: TLS/token failure prevents UDS-only daemon startup (accepted per task scope).
- Refactored `DaemonControl` and `DaemonPairingInfo` to use `Arc<WsRuntimeControl>` (non-optional) instead of `Option<Arc<...>>` — eliminates all `expect()` calls that could panic, makes requirement compile-time instead of runtime.

**Boot semantics (preserved):**
- With --listen tcp/both: listener auto-starts at boot (CLI/env win over persisted settings, logged when they do)
- With --listen uds: listener does NOT auto-start at boot, but CAN be started at runtime via settings.update

**Tests:**
- Updated `uds_server_control.rs` header comment
- Kept rollback test (still valuable for failure path)
- All existing tests pass (`uds_server_control`, `e2e_wss_runtime_control`)

**Documentation:**
- Added `docs/01_stabilizing/KNOWN_ISSUES.md` with STAB-1 entry (P1, area: intentd runtime listener control, repro, root cause, fix summary)

**Verification:**
```bash
cargo fmt --check && cargo clippy -- -D warnings && cargo test
```
All passing ✅
