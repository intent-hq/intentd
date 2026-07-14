//! Server runtime control: apply-hook seam for `settings.update` (§5.12).
//!
//! When persisted `server.*` settings change, the composition root can be asked
//! to start/stop the WSS listener + mDNS without a daemon restart. This trait
//! is implemented in the binary (`intentd`) and wired into [`Services`] so the
//! `settings.update` handler can invoke it after persisting the new values.

use std::future::Future;
use std::pin::Pin;

use crate::Result;

/// Runtime control surface for the WSS listener + mDNS, implemented by the
/// daemon composition root and wired into `Services` so `settings.update` can
/// apply `server.wsApi.enabled` / `server.discovery.enabled` changes at runtime.
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

    /// Start mDNS discovery advertisement for the WSS listener if not already
    /// running. Returns `Ok(())` on success or if already active; returns an error
    /// if the listener is not running or in insecure mode. Idempotent.
    fn start_discovery(&self) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>>;

    /// Stop mDNS discovery advertisement if currently active. Idempotent: if not
    /// active, does nothing.
    fn stop_discovery(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>>;

    /// Whether mDNS discovery is currently active.
    fn is_discovery_active(&self) -> Pin<Box<dyn Future<Output = bool> + Send + '_>>;
}
