//! Single-flight start/stop, race guards, fail-fast bind (§5.6).
//!
//! Ports the robustness guarantees of `websocket-api-server.ts` that prevent
//! the double-start / shutdown-race bugs the TS code was hardened against.
//! Concurrent `start()` callers share one in-flight future (a
//! `Shared<BoxFuture>`); a `stop()` during an in-flight `start()` bumps a
//! monotonic `external_stop_generation`, which the bind path re-checks and
//! unwinds on. The listener performs exactly one bind attempt on the configured
//! port and surfaces the OS error verbatim if that port is not available —
//! there is no port walking. `stop()` runs the canonical shutdown ordering so a
//! subsequent `start()` cannot race the freed listen port.

use std::io;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use futures::future::{BoxFuture, Shared};
use futures::FutureExt;
use intent_core::Error;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use crate::discovery::{advertise_if_enabled, Discovery};
use crate::ws::{ConnCmd, WsInner};

/// Default listen port (PROTOCOL §1). The listener binds exactly this port; if
/// it is busy, `start()` returns the bind error immediately (no port walking).
pub const DEFAULT_PORT: u16 = 5181;
/// Heartbeat ping cadence (`HEARTBEAT_INTERVAL_MS`).
pub const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);
/// No-pong deadline before a client is terminated (`HEARTBEAT_TIMEOUT_MS`).
pub const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(60);

/// The shared, clonable start future (`io::Error` boxed in `Arc` so it is
/// `Clone` for the `Shared` combinator).
type StartFuture = Shared<BoxFuture<'static, Result<u16, Arc<io::Error>>>>;

/// Handles for a running listener, taken by `stop()` to tear it down in order.
///
/// Discovery ownership rationale: The [`Discovery`] handle is owned here
/// (rather than in a separate manager) to enforce the product invariant that
/// mDNS advertisement **requires an active listener**. This coupling prevents
/// advertising a service that clients cannot connect to, which would violate
/// the mDNS contract. The ownership model ensures:
/// - Discovery is created only after the listener binds (uses the real port)
/// - Discovery is unpublished **before** the listener stops (no stale records)
/// - Discovery cannot exist without a running listener (enforced by runtime guards)
///
/// Independent runtime control (`start_discovery` / `stop_discovery`) allows
/// toggling discovery while the listener runs, without restarting the listener.
pub(crate) struct RunningHandles {
    pub accept_task: JoinHandle<()>,
    pub heartbeat_task: JoinHandle<()>,
    pub shutdown_tx: Option<oneshot::Sender<()>>,
    /// Live mDNS advertisement (§5.4), present only when discovery is enabled;
    /// unpublished first during graceful shutdown so no stale record lingers.
    pub discovery: Option<Discovery>,
}

/// Lifecycle state guarded by a single async mutex (the TS instance fields).
#[derive(Default)]
pub(crate) struct StartState {
    pub started: bool,
    pub shutting_down: bool,
    pub port: Option<u16>,
    pub start_task: Option<StartFuture>,
    pub running: Option<RunningHandles>,
}

impl WsInner {
    /// Single-flight start: concurrent callers share one in-flight future;
    /// once running, returns the bound port immediately.
    pub(crate) async fn start(self: &Arc<Self>) -> io::Result<u16> {
        let fut = {
            let mut st = self.state.lock().await;
            if st.started {
                if let Some(port) = st.port {
                    return Ok(port);
                }
            }
            st.shutting_down = false;
            if let Some(existing) = st.start_task.clone() {
                existing
            } else {
                let generation = self.external_stop_generation.load(Ordering::SeqCst);
                let me = self.clone();
                let fut = async move { me.do_start(generation).await }
                    .boxed()
                    .shared();
                st.start_task = Some(fut.clone());
                fut
            }
        };
        fut.await
            .map_err(|e| io::Error::new(e.kind(), e.to_string()))
    }

    /// Bind once, then spawn the accept + heartbeat loops and record their
    /// handles. Re-checks the stop generation before binding.
    async fn do_start(self: Arc<Self>, generation: u64) -> Result<u16, Arc<io::Error>> {
        let (listener, port) = self.bind_once(generation).await?;
        tracing::info!(port, "intentd WSS listening");
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let accept_task = tokio::spawn(self.clone().accept_loop(listener, shutdown_rx));
        let heartbeat_task = tokio::spawn(self.clone().heartbeat_loop());
        // Advertise the bound port over mDNS (§5.4); a no-op (and `None`) when
        // discovery is disabled, no fingerprint is available (insecure mode), or
        // registration fails. The TXT record carries the resolved locality
        // (§5.14): remote for a plain TCP/WSS listener, local when forced via
        // `--mode`/`server.locality`.
        let discovery = self.fingerprint.as_deref().and_then(|fp| {
            advertise_if_enabled(self.discovery_enabled, port, fp, self.locality_is_local)
        });
        let mut st = self.state.lock().await;
        st.started = true;
        st.port = Some(port);
        st.start_task = None;
        st.running = Some(RunningHandles {
            accept_task,
            heartbeat_task,
            shutdown_tx: Some(shutdown_tx),
            discovery,
        });
        Ok(port)
    }

    /// One TCP bind attempt on the configured port; any failure (including
    /// `EADDRINUSE`) is returned to the caller as-is. Re-checks the stop
    /// generation so a concurrent `stop()` unwinds instead of binding.
    async fn bind_once(
        self: &Arc<Self>,
        generation: u64,
    ) -> Result<(TcpListener, u16), Arc<io::Error>> {
        if self.external_stop_generation.load(Ordering::SeqCst) != generation {
            return Err(Arc::new(io::Error::new(
                io::ErrorKind::Interrupted,
                "ws start aborted by concurrent stop",
            )));
        }
        match TcpListener::bind((self.bind_address, self.base_port)).await {
            Ok(listener) => Ok((listener, self.base_port)),
            Err(e) => Err(Arc::new(e)),
        }
    }

    /// Graceful shutdown in the canonical order (port of `stop()`): bump the
    /// stop generation (cancels an in-flight start), stop the heartbeat, close
    /// every client with `1001`, drop their subscriptions, stop accepting and
    /// drop the listener, then await the accept loop so a subsequent `start()`
    /// cannot hit `EADDRINUSE`.
    pub(crate) async fn stop(self: &Arc<Self>) {
        self.external_stop_generation.fetch_add(1, Ordering::SeqCst);
        let (running, start_task) = {
            let mut st = self.state.lock().await;
            st.shutting_down = true;
            st.started = false;
            (st.running.take(), st.start_task.take())
        };
        // Let an in-flight start observe the generation bump and unwind first.
        if let Some(task) = start_task {
            let _ = task.await;
        }
        if let Some(mut running) = running {
            // (0) unpublish the mDNS advert first so no client resolves a
            // listener that is mid-teardown (§5.4 graceful-shutdown ordering).
            if let Some(discovery) = running.discovery.take() {
                discovery.stop();
            }
            // (1) stop the heartbeat.
            running.heartbeat_task.abort();
            // (2)+(3) close every client with 1001; the connection loop drops
            // its subscriptions when it exits.
            let handles: Vec<_> = {
                let mut map = self.clients.lock().expect("ws clients poisoned");
                map.drain().map(|(_, h)| h).collect()
            };
            for client in &handles {
                let _ = client.cmd_tx.send(ConnCmd::Close).await;
            }
            // Brief grace so the 1001 frame is flushed before we terminate.
            if !handles.is_empty() {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            // (4)+(6) remove the upgrade handler / close the listener.
            if let Some(tx) = running.shutdown_tx.take() {
                let _ = tx.send(());
            }
            // (5) terminate any lingering client connections.
            for client in &handles {
                client.abort.abort();
            }
            // (7) await the listener closure so the port is fully released.
            let _ = running.accept_task.await;
            let _ = running.heartbeat_task.await;
        }
        let mut st = self.state.lock().await;
        st.shutting_down = false;
        st.port = None;
    }

    /// Start mDNS discovery advertisement if not already running and the listener
    /// is started. Idempotent: if discovery is already active, does nothing.
    pub(crate) async fn start_discovery(self: &Arc<Self>) -> Result<(), Error> {
        let mut st = self.state.lock().await;

        // Check if listener is running
        if !st.started {
            return Err(Error::Internal(
                "cannot start discovery: listener not running".to_string(),
            ));
        }

        // Check if discovery is already active
        if let Some(ref running) = st.running {
            if running.discovery.is_some() {
                return Ok(()); // Already active, idempotent
            }
        }

        // Get the port and fingerprint
        let port = st
            .port
            .ok_or_else(|| Error::Internal("listener started but port not set".to_string()))?;

        let fingerprint = self.fingerprint.as_deref().ok_or_else(|| {
            Error::Internal("cannot start discovery in insecure mode (no fingerprint)".to_string())
        })?;

        // Start discovery - advertise_if_enabled returns None on failure
        let discovery = advertise_if_enabled(true, port, fingerprint, self.locality_is_local)
            .ok_or_else(|| Error::Internal("failed to start mDNS discovery".to_string()))?;

        // Store the discovery handle
        if let Some(ref mut running) = st.running {
            running.discovery = Some(discovery);
        }

        Ok(())
    }

    /// Stop mDNS discovery advertisement if currently running. Idempotent: if
    /// discovery is not active, does nothing.
    pub(crate) async fn stop_discovery(self: &Arc<Self>) {
        // Extract discovery handle without holding lock across the blocking stop() call
        let discovery = {
            let mut st = self.state.lock().await;
            if let Some(ref mut running) = st.running {
                running.discovery.take()
            } else {
                None
            }
        };

        // Stop outside the lock (discovery.stop() can do blocking work)
        if let Some(d) = discovery {
            d.stop();
        }
    }

    /// Whether mDNS discovery is currently active.
    pub(crate) async fn is_discovery_active(self: &Arc<Self>) -> bool {
        let st = self.state.lock().await;

        if let Some(ref running) = st.running {
            running.discovery.is_some()
        } else {
            false
        }
    }
}
