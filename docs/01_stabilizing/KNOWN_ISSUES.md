# Known Issues — Stabilization Phase

Issues discovered during dogfooding (using intentd + cloudlands-fe for daily development work).

## STAB-1 (2026-07-16) — intentd runtime listener control

**Area:** intentd runtime listener control  
**Severity:** P1 (broken feature)  
**Date filed:** 2026-07-16

**Repro:**
1. Start intentd in sidecar mode (FE spawns `intentd serve --listen uds` in dev OR packaged builds)
2. Open Settings UI → WebSocket API
3. Toggle `server.wsApi.enabled` to `true`
4. **Expected:** WSS listener starts, bound port visible in system.status
5. **Actual:** Error: "WSS listener not available (daemon started with --listen uds)", setting reverted

**Root cause:**
`main.rs` only constructed `WsRuntimeControl` when `serve_tcp_enabled` (--listen tcp/both). Under `--listen uds`, `DaemonControl.ws_runtime` was `None`, so `start_ws_listener` failed with the error above.

**Status:** fixed (PR #XXXX, 2026-07-16)

**Fix summary:**
- `WsRuntimeControl` now constructed for ALL listen modes (uds/tcp/both)
- Boot-time auto-start remains ONLY for --listen tcp/both (preserves existing behavior)
- Runtime toggle works for all modes: `settings.update server.wsApi.enabled=true` starts the listener under --listen uds
- Persisted `enabled=true` honored on next boot (same contract as tcp/both)
