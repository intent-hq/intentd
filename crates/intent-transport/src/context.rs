//! Connection context: thread transport origin (UDS vs TCP) through request handling.
//!
//! Uses tokio task-local storage to carry connection origin from the transport
//! layer (`listener.rs`, `ws.rs`) through the router/dispatcher to service code
//! without adding parameters to every method. This lets `ServerControl::is_tcp_connection`
//! return the real value and guard against self-terminating stop calls from TCP clients.

use std::cell::RefCell;

tokio::task_local! {
    /// Connection origin for the current request task. Set by the transport layer
    /// (UDS sets `false`, WSS sets `true`) before spawning the request handler.
    /// Queried by `ServerControl::is_tcp_connection()` to enforce safety guards.
    static IS_TCP: RefCell<bool>;
}

/// Whether the current request is over a TCP transport (WSS). Returns `false`
/// for UDS connections or when called outside a request context. Thread-safe.
pub fn is_tcp_connection() -> bool {
    IS_TCP.try_with(|cell| *cell.borrow()).unwrap_or(false)
}

/// Run a future within a connection-context scope. The `is_tcp` flag will be
/// visible to all code running within `f` via `is_tcp_connection()`.
pub async fn with_connection_context<F, R>(is_tcp: bool, f: F) -> R
where
    F: std::future::Future<Output = R>,
{
    IS_TCP.scope(RefCell::new(is_tcp), f).await
}
