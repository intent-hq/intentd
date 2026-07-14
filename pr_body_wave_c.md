## Summary

Documents ACP session lifetime semantics and audits transcript persistence transaction boundaries (STAB-17 investigation Track 3+4).

## Changes

### 1. ACP Session Lifetime Documentation
- Added comprehensive module-level documentation in session.rs explaining that ACP session ids are process-local
- Documents that post-restart session/load reliably fails with -32602 (provider has no record of stale id)
- Explains this is a design limitation of process-local state, not a bug
- Documents the recreate+resend fallback implemented in AgentManager::start_session

### 2. Transcript Persistence Audit
- Documented transaction boundaries in append_agent_message_with_id
- Confirmed crash safety: SELECT/INSERT split leaves seq gaps but never loses committed messages
- Verified all message-append paths (sendMessage, forceMessage, appendMessage, persist_user, wake delivery) use the same atomic store operation
- AgentManager single-flight slot serializes assistant-message appends
- UNIQUE(agent_id, seq) constraint prevents silent corruption on concurrent user-message appends

## Verification
- cargo fmt --check: passed
- cargo clippy -- -D warnings: passed
- cargo build: passed
- cargo test --package intent-store --lib: passed (39/39 tests)

## Related
- STAB-17 investigation (ACP session/load resume fails)
- Track 3: session docs + fallback test
- Track 4: transcript-persistence audit
