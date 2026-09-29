//! Process-independent agent execution boundary.
//!
//! The orchestrator owns durable sessions and turn scheduling. A runtime owns
//! execution resources and their cleanup. Associated protocol types let the
//! local ACP adapter retain its error and activity semantics without making
//! this leaf crate depend on ACP or a network transport.

use std::time::Duration;

use crate::BoxFuture;

/// One running provider session. Implementations need not own an OS process.
///
/// Dropping an in-flight prompt must abandon that request without stopping the
/// runtime; cancellation and teardown are explicit, separate operations.
pub trait AgentRuntime: Send + Sync {
    /// Content delivered to the provider for one turn.
    type Prompt: Send;
    /// Provider completion including usage and finish reason.
    type PromptOutcome: Send;
    /// Shared activity clock used to enforce idle (not wall-clock) timeout.
    type Activity: Sync;
    /// Typed provider/protocol error, retained for retry classification.
    type Error: Send;
    /// Shared, single-consumer stream of provider notifications.
    type Notifications: Clone + Send + Sync;

    /// Deliver a turn, preserving the implementation's idle/error semantics.
    ///
    /// # Errors
    ///
    /// Returns the adapter's typed delivery, timeout or provider error.
    fn prompt<'a>(
        &'a self,
        session_id: &'a str,
        prompt: Self::Prompt,
        activity: &'a Self::Activity,
    ) -> BoxFuture<'a, std::result::Result<Self::PromptOutcome, Self::Error>>;

    /// Ask the provider to cancel a turn without destroying the session.
    /// The caller bounds delivery and can stop an unresponsive runtime.
    ///
    /// # Errors
    ///
    /// Returns the adapter's typed error when cancellation cannot be delivered.
    fn cancel<'a>(
        &'a self,
        session_id: &'a str,
    ) -> BoxFuture<'a, std::result::Result<(), Self::Error>>;

    /// Obtain the runtime's notification stream; consumers serialize access.
    fn notifications(&self) -> Self::Notifications;

    /// Whether the provider still appears available for another turn.
    fn is_alive(&self) -> bool;

    /// Monotonic count of provider requests that may have caused side effects.
    /// A changed watermark prevents unsafe automatic prompt retries.
    fn client_request_seq(&self) -> u64;

    /// Monotonic response watermark used to settle an abandoned prompt.
    fn response_seq(&self) -> u64;

    /// Wait at most `timeout` for a response after `since`.
    fn await_response_after(&self, since: u64, timeout: Duration) -> BoxFuture<'_, bool>;

    /// Local spawn-time process identity for diagnostics, if one exists.
    /// Remote and process-free implementations must return `None`.
    fn spawned_pid(&self) -> Option<u32>;

    /// A currently live local process root for local memory accounting.
    /// Omit dead or indeterminate roots rather than attributing a recycled PID.
    fn root_pid(&self) -> Option<u32>;

    /// Detach execution resources immediately and await their owned cleanup.
    /// Cleanup must continue if the returned future is dropped; repeated calls
    /// are harmless. Dropping the runtime must provide the same cleanup fallback.
    fn stop(&self) -> BoxFuture<'static, ()>;
}
