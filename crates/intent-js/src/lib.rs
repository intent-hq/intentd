//! intent-js — `QuickJS` execution engine for agent-supplied JavaScript.
//!
//! This crate is a spike that proves the daemon can run untrusted-ish user
//! code with a shape compatible with the reference `workspace-js-api-tool.ts`
//! (Node `vm.runInNewContext` + 30s timeout). It uses [`rquickjs`] (`QuickJS`
//! bindings) via its async API so a single host function can `await` tokio
//! work while JavaScript sees a normal `Promise`.
//!
//! Design goals proved by the tests in this crate:
//!
//! - Run `(async () => { <code> })()` and return its awaited result as JSON.
//! - Bind one async host function (`host(arg)`) that awaits a Rust future.
//! - Enforce a wall-clock timeout that interrupts both **hot loops**
//!   (via `AsyncRuntime::set_interrupt_handler`) and **pending awaits**
//!   (via `tokio::time::timeout` on the outer future).
//! - Per-execution isolation: every call constructs a fresh `AsyncRuntime` +
//!   `AsyncContext`, so globals never leak between invocations.
//!
//! The public surface is intentionally minimal — just [`eval`] + [`EvalOptions`] +
//! [`HostFn`] + [`JsError`]. Real `ws.*` bindings and MCP tool wiring live elsewhere.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

/// Boxed, `Send` future used for host bindings.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Host function bound to `globalThis.host(arg)` in the JS runtime.
///
/// The argument is any JSON value the script passed. The future resolves to
/// either a JSON value (turned into the host promise's resolution) or an
/// error string (turned into a JS `Error` and rejected).
pub type HostFn = Arc<
    dyn Fn(serde_json::Value) -> BoxFuture<'static, Result<serde_json::Value, String>>
        + Send
        + Sync,
>;

/// Default wall-clock timeout — mirrors the reference TS tool.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

/// Default `QuickJS` memory ceiling (64 MB). The engine executes untrusted-ish
/// agent code, so the default must be bounded; unlimited memory is only
/// reachable by explicitly setting `memory_limit_bytes: None`.
pub const DEFAULT_MEMORY_LIMIT_BYTES: usize = 64 * 1024 * 1024;

/// Extra time the outer `tokio::time::timeout` waits past the interrupt
/// deadline so the interrupt handler has a chance to raise an uncatchable
/// JS exception before we drop the whole future.
const OUTER_SAFETY_MARGIN: Duration = Duration::from_millis(250);

/// Failure modes surfaced by [`eval`].
#[derive(Debug, thiserror::Error)]
pub enum JsError {
    /// The wall-clock budget elapsed before the script finished.
    #[error("javascript execution timed out after {ms}ms")]
    Timeout { ms: u64 },
    /// The script threw / rejected. The message is the stringified error,
    /// suitable for surfacing directly to the agent.
    #[error("javascript error: {0}")]
    Runtime(String),
    /// The engine itself failed to start (allocation, context init, etc.).
    #[error("engine error: {0}")]
    Engine(String),
}

/// Options controlling one [`eval`] invocation.
#[derive(Clone, Debug)]
pub struct EvalOptions {
    /// Wall-clock budget, enforced by both a `QuickJS` interrupt handler and
    /// an outer `tokio::time::timeout`.
    pub timeout: Duration,
    /// `QuickJS` memory ceiling; defaults to [`DEFAULT_MEMORY_LIMIT_BYTES`].
    /// `None` disables the cap entirely — an explicit opt-out, never the
    /// default.
    pub memory_limit_bytes: Option<usize>,
}

impl Default for EvalOptions {
    fn default() -> Self {
        Self {
            timeout: DEFAULT_TIMEOUT,
            memory_limit_bytes: Some(DEFAULT_MEMORY_LIMIT_BYTES),
        }
    }
}

mod engine;
pub use engine::eval;
