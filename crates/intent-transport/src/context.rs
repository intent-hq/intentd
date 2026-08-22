//! Connection context: thread transport origin (UDS vs TCP) through request handling.
//!
//! Uses tokio task-local storage to carry connection origin from the transport
//! layer (`listener.rs`, `ws.rs`) through the router/dispatcher to service code
//! without adding parameters to every method. This lets `ServerControl::is_tcp_connection`
//! return the real value and guard against self-terminating stop calls from TCP clients.
//!
//! ## Invariant: Context Propagation Across Task Spawns
//!
//! The connection origin is established at the transport layer and MUST survive
//! into all spawned handler tasks:
//!
//! 1. **Transport establishes context** (`listener.rs`, `ws.rs`):
//!    - UDS wraps `process_frame` in `with_connection_context(false, ...)`
//!    - WSS wraps `process_frame` in `with_connection_context(true, ...)`
//!
//! 2. **Spawned tasks re-establish context**:
//!    - Before spawning, capture `is_tcp_connection()` from the current context
//!    - Wrap spawned work in `with_connection_context(is_tcp, ...)` with the captured value
//!    - This ensures the transport origin is visible to all code in the spawned task
//!    - All spawns in `conn.rs` follow this pattern: `host::handle`, `browser::handle`, `handle_message`
//!
//! 3. **Origin checks run within established context**:
//!    - `server.*` RPCs: inline on read loop, context guaranteed
//!    - `settings.update` WSS guard: runs inside spawned `handle_message` task with re-established context
//!
//! The fallback (`unwrap_or(true)`) is fail-closed: missing context is treated
//! as remote/untrusted. Request-handling paths are guaranteed to establish context;
//! other code (e.g., background tasks) may call this without established context.

use std::cell::RefCell;

tokio::task_local! {
    /// Connection origin for the current request task. Set by the transport layer
    /// (UDS sets `false`, WSS sets `true`) before spawning the request handler.
    /// Queried by `ServerControl::is_tcp_connection()` to enforce safety guards.
    static IS_TCP: RefCell<bool>;
}

/// Whether the current request is over a TCP transport (WSS). Returns `true`
/// (remote/untrusted) for TCP connections or when called outside a request
/// context (fail-closed). Thread-safe.
#[must_use]
pub fn is_tcp_connection() -> bool {
    IS_TCP.try_with(|cell| *cell.borrow()).unwrap_or(true)
}

/// Run a future within a connection-context scope. The `is_tcp` flag will be
/// visible to all code running within `f` via `is_tcp_connection()`.
pub async fn with_connection_context<F, R>(is_tcp: bool, f: F) -> R
where
    F: std::future::Future<Output = R>,
{
    IS_TCP.scope(RefCell::new(is_tcp), f).await
}
