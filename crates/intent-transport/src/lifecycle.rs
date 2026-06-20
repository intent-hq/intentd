//! Single-flight start/stop, race guards, port backoff (§5.6).
//!
//! Ports the robustness guarantees of `websocket-api-server.ts` that prevent
//! the EADDRINUSE / double-start / shutdown-race bugs the TS code was hardened
//! against. Concurrent `start()` callers share one in-flight future (a
//! `Shared<BoxFuture>`); a `stop()` during an in-flight `start()` bumps a
//! monotonic `external_stop_generation`, which the bind loop re-checks and
//! unwinds on. `stop()` then runs the canonical shutdown ordering so a
//! subsequent `start()` cannot race the freed listen port.

use std::io;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use futures::future::{BoxFuture, Shared};
use futures::FutureExt;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use crate::ws::{ConnCmd, WsInner};

/// Default base listen port (PROTOCOL §1). If busy, the listener walks forward.
pub const DEFAULT_PORT: u16 = 5180;
/// Maximum number of distinct ports to try (`WS_API_MAX_PORT_ATTEMPTS`).
pub const MAX_PORT_ATTEMPTS: u16 = 10;
/// Same-port EADDRINUSE backoff before advancing to the next port (ms).
pub const SAME_PORT_BACKOFF_MS: [u64; 3] = [100, 200, 400];
/// Heartbeat ping cadence (`HEARTBEAT_INTERVAL_MS`).
pub const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);
/// No-pong deadline before a client is terminated (`HEARTBEAT_TIMEOUT_MS`).
pub const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(60);

/// The shared, clonable start future (`io::Error` boxed in `Arc` so it is
/// `Clone` for the `Shared` combinator).
type StartFuture = Shared<BoxFuture<'static, Result<u16, Arc<io::Error>>>>;

/// Handles for a running listener, taken by `stop()` to tear it down in order.
pub(crate) struct RunningHandles {
    pub accept_task: JoinHandle<()>,
    pub heartbeat_task: JoinHandle<()>,
    pub shutdown_tx: Option<oneshot::Sender<()>>,
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

    /// Bind (with port backoff), then spawn the accept + heartbeat loops and
    /// record their handles. Re-checks the stop generation before each bind.
    async fn do_start(self: Arc<Self>, generation: u64) -> Result<u16, Arc<io::Error>> {
        let (listener, port) = self.bind_with_backoff(generation).await?;
        tracing::info!(port, "intentd WSS listening");
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let accept_task = tokio::spawn(self.clone().accept_loop(listener, shutdown_rx));
        let heartbeat_task = tokio::spawn(self.clone().heartbeat_loop());
        let mut st = self.state.lock().await;
        st.started = true;
        st.port = Some(port);
        st.start_task = None;
        st.running = Some(RunningHandles {
            accept_task,
            heartbeat_task,
            shutdown_tx: Some(shutdown_tx),
        });
        Ok(port)
    }

    /// Walk forward up to [`MAX_PORT_ATTEMPTS`] ports; on each, try once then
    /// retry with [`SAME_PORT_BACKOFF_MS`] on EADDRINUSE before advancing.
    async fn bind_with_backoff(
        self: &Arc<Self>,
        generation: u64,
    ) -> Result<(TcpListener, u16), Arc<io::Error>> {
        let mut last_err: Option<io::Error> = None;
        for attempt in 0..MAX_PORT_ATTEMPTS {
            let Some(port) = self.base_port.checked_add(attempt) else {
                break;
            };
            for delay in std::iter::once(0).chain(SAME_PORT_BACKOFF_MS) {
                if delay > 0 {
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                }
                if self.external_stop_generation.load(Ordering::SeqCst) != generation {
                    return Err(Arc::new(io::Error::new(
                        io::ErrorKind::Interrupted,
                        "ws start aborted by concurrent stop",
                    )));
                }
                match TcpListener::bind((self.bind_address, port)).await {
                    Ok(listener) => return Ok((listener, port)),
                    Err(e) if e.kind() == io::ErrorKind::AddrInUse => {
                        last_err = Some(e);
                    }
                    Err(e) => return Err(Arc::new(e)),
                }
            }
        }
        Err(Arc::new(last_err.unwrap_or_else(|| {
            io::Error::new(io::ErrorKind::AddrInUse, "no free port for WSS listener")
        })))
    }

    /// Graceful shutdown in the canonical order (port of `stop()`): bump the
    /// stop generation (cancels an in-flight start), stop the heartbeat, close
    /// every client with `1001`, drop their subscriptions, stop accepting and
    /// drop the listener, then await the accept loop so a subsequent `start()`
    /// cannot hit EADDRINUSE.
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
}
