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
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use futures::future::{BoxFuture, Shared};
use futures::FutureExt;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use crate::ws::{ConnCmd, WsInner};

/// Default listen port (PROTOCOL §1). The listener binds exactly this port; if
/// it is busy, `start()` returns the bind error immediately (no port walking).
pub const DEFAULT_PORT: u16 = 5181;
/// Heartbeat ping cadence (`HEARTBEAT_INTERVAL_MS`).
pub(crate) const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);
/// No-pong deadline before a client is terminated (`HEARTBEAT_TIMEOUT_MS`).
pub(crate) const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(60);

/// The shared, clonable start future (`io::Error` boxed in `Arc` so it is
/// `Clone` for the `Shared` combinator).
type StartFuture = Shared<BoxFuture<'static, Result<u16, Arc<io::Error>>>>;

/// Handles for a running listener, taken by `stop()` to tear it down in
/// order. One accept task + shutdown signal per bound address
/// (`server.bindAddress` may list several; monorepo#3314).
pub(crate) struct RunningHandles {
    pub accept_tasks: Vec<JoinHandle<()>>,
    pub heartbeat_task: JoinHandle<()>,
    pub shutdown_txs: Vec<oneshot::Sender<()>>,
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

    /// Bind every configured address, then spawn one accept loop per
    /// listener plus the heartbeat loop and record their handles. Re-checks
    /// the stop generation before binding AND again under the state lock
    /// before spawning/installing: `stop()` may have taken `start_task` and
    /// `running` while a later bind in the set was in flight, and installing
    /// after that would leave live listeners nothing tears down. Dropping
    /// the bound listeners on that path closes them.
    async fn do_start(self: Arc<Self>, generation: u64) -> Result<u16, Arc<io::Error>> {
        let (listeners, port) = self.bind_once(generation).await?;
        let mut st = self.state.lock().await;
        if self.external_stop_generation.load(Ordering::SeqCst) != generation {
            st.start_task = None;
            return Err(Arc::new(io::Error::new(
                io::ErrorKind::Interrupted,
                "ws start aborted by concurrent stop",
            )));
        }
        for listener in &listeners {
            if let Ok(addr) = listener.local_addr() {
                tracing::info!(address = %addr.ip(), port, "intentd WSS listening");
            }
        }
        let mut accept_tasks = Vec::with_capacity(listeners.len());
        let mut shutdown_txs = Vec::with_capacity(listeners.len());
        for listener in listeners {
            let (shutdown_tx, shutdown_rx) = oneshot::channel();
            accept_tasks.push(tokio::spawn(
                self.clone().accept_loop(listener, shutdown_rx),
            ));
            shutdown_txs.push(shutdown_tx);
        }
        let heartbeat_task = tokio::spawn(self.clone().heartbeat_loop());
        st.started = true;
        st.port = Some(port);
        st.start_task = None;
        st.running = Some(RunningHandles {
            accept_tasks,
            heartbeat_task,
            shutdown_txs,
        });
        Ok(port)
    }

    /// One TCP bind attempt per configured address on the configured port —
    /// all-or-nothing: any failure (including `EADDRINUSE`) drops the
    /// already-bound listeners and is returned as-is, so the daemon never
    /// silently serves fewer interfaces than configured (monorepo#3314). A
    /// `base_port` of 0 (the E2E ephemeral seam) binds the first address on
    /// an OS-assigned port and the remaining addresses on that same port.
    /// An IPv6-unspecified (`::`) bind is made explicitly dual-stack (see
    /// [`bind_listener`]). Re-checks the stop generation so a concurrent
    /// `stop()` unwinds instead of binding.
    async fn bind_once(
        self: &Arc<Self>,
        generation: u64,
    ) -> Result<(Vec<TcpListener>, u16), Arc<io::Error>> {
        if self.external_stop_generation.load(Ordering::SeqCst) != generation {
            return Err(Arc::new(io::Error::new(
                io::ErrorKind::Interrupted,
                "ws start aborted by concurrent stop",
            )));
        }
        // Hard error (not just a debug assertion): `WsOptions` is public, and
        // an empty set completing "successfully" would start the heartbeat
        // and report a port with no TCP listener behind it.
        if self.bind_addresses.is_empty() {
            return Err(Arc::new(io::Error::new(
                io::ErrorKind::InvalidInput,
                "WsOptions.bind_addresses must be non-empty",
            )));
        }
        let mut listeners = Vec::with_capacity(self.bind_addresses.len());
        let mut port = self.base_port;
        for addr in &self.bind_addresses {
            match bind_listener(*addr, port).await {
                Ok(listener) => {
                    // First bind resolves an ephemeral port 0; the rest of
                    // the set joins it on the same resolved port.
                    port = listener.local_addr().map_err(Arc::new)?.port();
                    listeners.push(listener);
                }
                Err(e) => {
                    return Err(Arc::new(io::Error::new(
                        e.kind(),
                        format!("bind {addr}:{port}: {e}"),
                    )))
                }
            }
        }
        Ok((listeners, port))
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
            // (4)+(6) remove the upgrade handlers / close every listener.
            for tx in running.shutdown_txs.drain(..) {
                let _ = tx.send(());
            }
            // (5) terminate any lingering client connections.
            for client in &handles {
                client.abort.abort();
            }
            // (7) await every listener closure so the port is fully released.
            for task in running.accept_tasks.drain(..) {
                let _ = task.await;
            }
            let _ = running.heartbeat_task.await;
        }
        let mut st = self.state.lock().await;
        st.shutting_down = false;
        st.port = None;
    }
}

/// Bind one TCP listener at `addr:port`. An IPv6-unspecified (`::`) bind is
/// explicitly configured dual-stack (`IPV6_V6ONLY = false`) before binding,
/// so the IPv4 routes the pairing / `system.status` surfaces advertise for
/// it are reachable via v4-mapped sockets regardless of the OS default
/// (Windows and some Linux configurations default to IPv6-only). Every other
/// address keeps the plain `TcpListener::bind` path; the socket setup here
/// mirrors what that path does (non-blocking, `SO_REUSEADDR` on Unix).
async fn bind_listener(addr: IpAddr, port: u16) -> io::Result<TcpListener> {
    match addr {
        IpAddr::V6(v6) if v6.is_unspecified() => {
            let sock_addr = SocketAddr::new(addr, port);
            let socket = socket2::Socket::new(
                socket2::Domain::IPV6,
                socket2::Type::STREAM,
                Some(socket2::Protocol::TCP),
            )?;
            socket.set_only_v6(false)?;
            #[cfg(unix)]
            socket.set_reuse_address(true)?;
            socket.set_nonblocking(true)?;
            socket.bind(&sock_addr.into())?;
            socket.listen(1024)?;
            TcpListener::from_std(socket.into())
        }
        _ => TcpListener::bind((addr, port)).await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv6Addr;

    /// Reachability regression for the `::` bind: `advertised_hosts` includes
    /// the machine's IPv4 enumeration for an IPv6-unspecified bind, so the
    /// listener must actually accept plain IPv4 connections (dual-stack) —
    /// not depend on the OS's `IPV6_V6ONLY` default.
    #[tokio::test]
    async fn ipv6_unspecified_bind_accepts_ipv4() {
        let listener = match bind_listener(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 0).await {
            Ok(l) => l,
            // Hosts without IPv6 support cannot exercise this path at all.
            Err(e)
                if matches!(
                    e.kind(),
                    io::ErrorKind::Unsupported | io::ErrorKind::AddrNotAvailable
                ) =>
            {
                eprintln!("skipping: IPv6 unavailable ({e})");
                return;
            }
            Err(e) => panic!("bind [::]:0 failed: {e}"),
        };
        let port = listener.local_addr().expect("local addr").port();
        let (conn, accepted) = tokio::join!(
            tokio::net::TcpStream::connect(("127.0.0.1", port)),
            listener.accept()
        );
        conn.expect("IPv4 connect to a dual-stack :: listener must succeed");
        accepted.expect("dual-stack listener accepts the v4-mapped connection");
    }

    /// The non-`::` arm keeps plain bind semantics: a loopback IPv4 bind
    /// still works through the helper.
    #[tokio::test]
    async fn specific_bind_still_works() {
        let listener = bind_listener(IpAddr::from([127, 0, 0, 1]), 0)
            .await
            .expect("bind 127.0.0.1:0");
        let port = listener.local_addr().expect("local addr").port();
        let (conn, accepted) = tokio::join!(
            tokio::net::TcpStream::connect(("127.0.0.1", port)),
            listener.accept()
        );
        conn.expect("loopback connect");
        accepted.expect("accept loopback connection");
    }
}
