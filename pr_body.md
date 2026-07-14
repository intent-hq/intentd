Fixes STAB-33: activity-based session/prompt idle timeout.

## Problem

The fixed 1-hour `PROMPT_TIMEOUT` kills healthy long-running turns that actively stream `session/update` notifications. Any turn exceeding 60 minutes dies with "request `session/prompt` timed out", even while the agent is working. Observed during implementor turns combining CI watches with multi-thread review sweeps.

## Solution

Replace the fixed deadline with an **idle-based timeout** (default 15 minutes, configurable via `INTENTD_PROMPT_IDLE_TIMEOUT_MS`):
- The timer resets on every `session/update` notification
- Actively-streaming turns never time out
- Only silent/wedged turns are killed after sustained idle period

## Implementation

1. **ActivityTracker**: atomic timestamp tracker updated on each notification
2. **session::prompt**: polls idle duration every second instead of fixed deadline
3. **run_prompt_turn**: touches activity tracker on every incoming notification
4. **Error message**: distinguishes idle timeouts from other failures
5. **Regression tests**: verify ActivityTracker idle measurement, reset on touch, and periodic activity behavior

## Testing

All existing tests pass. New tests in `crates/intent-acp/tests/idle_timeout.rs` verify ActivityTracker correctness.

```bash
cargo fmt --check
cargo clippy -- -D warnings
cargo test
```

## Design Decision: No Absolute Ceiling

This implementation has **no absolute ceiling** — an actively-streaming turn can run indefinitely as long as notifications arrive within the idle window. This matches the task requirement and is justified by:
- Real cancellation flows through `session/cancel`, not timeout
- Long CI watches + review sweeps are legitimate use cases
- The idle timeout catches truly stuck/silent processes
- Users can still interrupt via normal cancellation if needed

If a ceiling is desired later, it can be added with a separate env var (e.g., `INTENTD_PROMPT_MAX_DURATION_MS`) without changing the idle-timeout semantics.

## Known Limitations

- **Pending-map leak**: The idle timeout returns early without cleaning up the Connection's pending request map entry. The entry will leak until the 24h fallback timeout expires or the agent closes stdout. This is acceptable for the current use case (one leaked entry per idle-timed-out turn, cleaned within 24h), but a future `Connection::cancel(id)` API would allow proper cleanup.
- **Integration test coverage**: Full end-to-end integration tests (asserting that `session::prompt` actually times out after the idle window with the mock ACP agent) are deferred.
