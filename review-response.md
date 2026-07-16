## Copilot review addressed

All 6 comments resolved:

1. **Empty test**: Marked as `#[ignore]` placeholder for e2e coverage (e2e_wss_runtime_control.rs provides the real coverage)
2. **expect() panics (2 comments)**: Refactored `ws_runtime` to `Arc<WsRuntimeControl>` (non-optional) — compile-time guarantee instead of runtime checks
3. **Boot semantics claims (2 comments)**: Updated comments to clarify persisted settings NOT honored at boot (only CLI/env matter)
4. **TLS provisioning for UDS**: This is intentional per task scope — TLS/token must be provisioned for all modes to enable runtime toggling. The trade-off is accepted: if TLS provisioning fails, the daemon won't start (even in UDS-only mode), but this ensures the runtime toggle path is always available. Alternative (lazy provisioning) would require a larger refactor and falls outside this fix's scope.

Latest commit: 7ab4a32
