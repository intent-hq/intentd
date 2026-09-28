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
//! [`eval_guarded`] additionally supports opaque admission of an original host
//! reply. It supplies ownership mechanics, not an authorization policy. Real
//! `ws.*` bindings and MCP tool wiring live elsewhere.

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

/// Host binding that explicitly distinguishes ordinary and guarded replies.
///
/// The engine allocates the original call identity and completion slot before
/// invoking this closure. The closure can capture its context synchronously,
/// before returning the future. Equal JSON arguments never imply equal calls.
pub type GuardedHostFn =
    Arc<dyn Fn(serde_json::Value, HostCallId) -> BoxFuture<'static, HostReply> + Send + Sync>;

/// An engine-created call identity. It cannot be constructed, cloned, or
/// reconstructed from public values. Keep it in the original admission object.
///
/// ```compile_fail
/// let forged = intent_js::HostCallId(std::sync::Arc::new(()));
/// ```
pub struct HostCallId(Arc<()>);

/// A trusted host's actual outcome and, when needed, its admission policy.
/// JSON fields and error strings never select the admission path.
pub enum HostReply {
    /// The existing host behavior, with no additional admission.
    Ordinary(Result<serde_json::Value, String>),
    /// Withhold the encoded outcome unless its original transfer is admitted.
    Guarded {
        /// The actual host outcome, encoded before admission runs.
        outcome: Result<serde_json::Value, String>,
        /// Opaque policy owned by the caller, not by this leaf crate.
        admission: Box<dyn HostReplyAdmission>,
    },
}

/// One-use admission of a prepared reply to its original JS host promise.
///
/// Implementors revalidate their original authority, then call
/// [`PreparedHostTransfer::transfer`] within their consuming boundary. That
/// action only transfers ownership; all policy guards must be released before
/// this future resolves. The engine waits for that resolution before reading
/// the slot or allowing `QuickJS` to observe the reply. No runtime installs this
/// policy by default. Implementors must not detach the transfer or its future.
pub trait HostReplyAdmission: Send {
    /// Consume this admission and the packet. Refusal must not include private
    /// diagnostics: the engine supplies a fixed non-private control error.
    fn admit(
        self: Box<Self>,
        transfer: PreparedHostTransfer,
    ) -> BoxFuture<'static, HostAdmissionOutcome>;
}

/// A receipt only the original prepared transfer can create. Not cloneable.
pub struct HostTransferReceipt(Arc<()>);

/// Result of the consuming action. A receipt cannot be fabricated by an adapter.
pub enum HostAdmissionOutcome {
    /// The bytes entered the original slot. This effect cannot be recalled and
    /// does not authorize any later output or artifact publication.
    Transferred(HostTransferReceipt),
    /// The original consumer has gone away; its bytes were discarded.
    ConsumerClosed,
    /// A different call identity was offered; its bytes were discarded.
    ForeignCall,
    /// No private bytes are authorized for this original completion.
    Refused,
}

/// Encoded bytes bound to one engine-owned completion. Fields are private;
/// there is no constructor, clone, payload accessor, or replacement-slot API.
#[must_use = "dropping a prepared transfer withholds its private reply"]
pub struct PreparedHostTransfer {
    receipt: HostTransferReceipt,
    sender: tokio::sync::oneshot::Sender<String>,
    encoded: String,
}

impl PreparedHostTransfer {
    /// Move the prebuilt bytes into their original slot, exactly once.
    ///
    /// This synchronous action performs no JS execution, JSON construction,
    /// await, I/O, or spawn. The receiver remains unread until `admit` returns.
    /// A foreign identity consumes and discards the packet rather than
    /// redirecting it. A closed consumer similarly discards the packet.
    ///
    /// ```compile_fail,E0382
    /// use intent_js::{HostCallId, PreparedHostTransfer};
    /// fn reuse(packet: PreparedHostTransfer, id: &HostCallId) {
    ///     let _ = packet.transfer(id);
    ///     let _ = packet.transfer(id); // the original packet was consumed
    /// }
    /// ```
    #[must_use]
    pub fn transfer(self, original: &HostCallId) -> HostAdmissionOutcome {
        if !Arc::ptr_eq(&self.receipt.0, &original.0) {
            return HostAdmissionOutcome::ForeignCall;
        }
        match self.sender.send(self.encoded) {
            Ok(()) => HostAdmissionOutcome::Transferred(self.receipt),
            Err(_) => HostAdmissionOutcome::ConsumerClosed,
        }
    }
}

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
pub use engine::{eval, eval_guarded};
