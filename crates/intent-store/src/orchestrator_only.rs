//! [`OrchestratorOnly`]: proof that this process runs in orchestrator mode,
//! required to open the Postgres store.
//!
//! Postgres (and with it more than one daemon per tenant) exists only in
//! orchestrator mode. The real guard is the intentd binary's startup check,
//! which refuses `INTENTD_POSTGRES_URL` in any other mode before it writes a
//! file; this token makes the Postgres opener say so in its signature, so a
//! caller cannot reach it without passing through that check.
//!
//! The type is not behind the `postgres` feature: the binary mints it even in
//! builds that cannot open Postgres yet.
//!
//! Construction is limited three ways:
//! - the field is private, and there is no `Default` and no conversion from
//!   the public, `Copy` [`DaemonMode`](intent_core::settings_file::DaemonMode)
//!   enum (`tests/compile_fail.rs`);
//! - [`OrchestratorOnly::mint_for_intentd_serve`] is hidden and called only by
//!   the intentd binary once it has resolved orchestrator mode
//!   (`tests/orchestrator_only_lint.rs` pins that one call site);
//! - [`OrchestratorOnly::for_tests`] exists only with `cfg(test)` or the
//!   `test-support` feature, and the same lint keeps it in test code.

/// Proof that the daemon resolved orchestrator mode at startup. Required by
/// value by the Postgres opener. Cloning it is fine: it proves a fact about
/// the process, not a right to one open.
#[derive(Debug, Clone)]
#[must_use]
pub struct OrchestratorOnly {
    _sealed: (),
}

impl OrchestratorOnly {
    /// **intentd binary only.** Mint the token after the daemon mode resolved
    /// to orchestrator (`crates/intentd/src/store_backend.rs`). Any other
    /// caller fails `tests/orchestrator_only_lint.rs`.
    #[doc(hidden)]
    pub fn mint_for_intentd_serve() -> Self {
        Self { _sealed: () }
    }

    /// Test-only token, for tests that open the Postgres store directly.
    #[cfg(any(test, feature = "test-support"))]
    pub fn for_tests() -> Self {
        Self { _sealed: () }
    }
}
