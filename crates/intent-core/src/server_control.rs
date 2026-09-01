//! Server runtime control: apply-hook seam for `settings.update` (§5.12).
//!
//! When persisted `server.*` settings change, the composition root can be asked
//! to start/stop the WSS listener without a daemon restart. This trait
//! is implemented in the binary (`intentd`) and wired into [`Services`] so the
//! `settings.update` handler can invoke it after persisting the new values.

use std::future::Future;
use std::pin::Pin;

use crate::Result;

/// Runtime control surface for the WSS listener, implemented by the
/// daemon composition root and wired into `Services` so `settings.update` can
/// apply `server.wsApi.enabled` changes at runtime.
pub trait ServerControl: Send + Sync {
    /// Start the WSS listener if not already running. Returns the bound port on
    /// success, or an error if the port cannot be bound. Idempotent: if already
    /// started, returns the current port.
    fn start_ws_listener(&self) -> Pin<Box<dyn Future<Output = Result<u16>> + Send + '_>>;

    /// Stop the WSS listener gracefully (close clients, release port). Idempotent:
    /// if not running, does nothing.
    fn stop_ws_listener(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>>;

    /// Current bound port, or `None` when the listener is stopped.
    fn ws_listener_port(&self) -> Pin<Box<dyn Future<Output = Option<u16>> + Send + '_>>;

    /// Whether the requesting connection is over the TCP listener. Used to guard
    /// against stopping the listener while the settings.update caller is on it.
    /// Always returns `false` for UDS connections.
    fn is_tcp_connection(&self) -> bool;

    /// Start the tailcat tunnel sidecar if not already running. Requires the
    /// WSS listener to be up (returns an error otherwise). Returns the stable
    /// `tc...` address once tailcat reports it. Idempotent: if already
    /// running, returns the current address. Default: unsupported error, so
    /// implementations without a tunnel (test mocks) need no override.
    fn start_tunnel(&self) -> Pin<Box<dyn Future<Output = Result<String>> + Send + '_>> {
        Box::pin(async {
            Err(crate::Error::Internal(
                "tunnel is not supported by this server".to_string(),
            ))
        })
    }

    /// Stop the tailcat tunnel sidecar (kill the child process). Idempotent:
    /// if not running, does nothing.
    fn stop_tunnel(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async {})
    }

    /// Current `tc...` address, or `None` when the tunnel is stopped.
    fn tunnel_address(&self) -> Pin<Box<dyn Future<Output = Option<String>> + Send + '_>> {
        Box::pin(async { None })
    }
}
