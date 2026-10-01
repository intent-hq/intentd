//! Single-flight start/stop, race guards, fail-fast bind (§5.6).
//!
//! Ports the robustness guarantees of `websocket-api-server.ts` that prevent
//! the double-start / shutdown-race bugs the TS code was hardened against.
//! Concurrent `start()` callers share one in-flight future (a
//! `Shared<BoxFuture>`); a `stop()` during an in-flight `start()` bumps a
//! monotonic `external_stop_generation`, which the bind path re-checks and
//! unwinds on. Fixed callers bind exactly their configured port. First-start
//! assignment tries consecutive ports, committing before any accept task starts. `stop()` runs the canonical shutdown ordering so a
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

/// Preferred first-assignment port (PROTOCOL §1); ordinary start remains fixed.
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
    pub assigned_port: Option<u16>,
    pub shutting_down: bool,
    pub port: Option<u16>,
    pub start_task: Option<StartFuture>,
    pub running: Option<RunningHandles>,
}

impl WsInner {
    /// Single-flight start: concurrent callers share one in-flight future;
    /// once running, returns the bound port immediately.
    pub(crate) async fn start(
        self: &Arc<Self>,
        assignment: Option<PortAssignment>,
    ) -> io::Result<u16> {
        let fut = {
            let mut st = self.state.lock().await;
            if st.started {
                if let Some(port) = st.port {
                    return Ok(port);
                }
            }
            if st.shutting_down {
                return Err(start_cancelled());
            }
            if let Some(existing) = st.start_task.clone() {
                existing
            } else {
                let generation = self.external_stop_generation.load(Ordering::SeqCst);
                let me = self.clone();
                let assigned_port = st.assigned_port;
                let fut = async move {
                    let result = me
                        .clone()
                        .do_start(generation, assigned_port, assignment)
                        .await;
                    if result.is_err() {
                        let mut st = me.state.lock().await;
                        if me.external_stop_generation.load(Ordering::SeqCst) == generation {
                            st.start_task = None;
                        }
                    }
                    result
                }
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
    async fn do_start(
        self: Arc<Self>,
        generation: u64,
        assigned_port: Option<u16>,
        assignment: Option<PortAssignment>,
    ) -> Result<u16, Arc<io::Error>> {
        let select = assignment.is_some() && assigned_port.is_none();
        let first = assigned_port.unwrap_or(self.base_port);
        let last = if select { u16::MAX } else { first };
        let cancelled = || self.external_stop_generation.load(Ordering::SeqCst) != generation;
        let (listeners, port) = select_port(first, last, cancelled, |port| {
            bind_all(&self.bind_addresses, port)
        })
        .await
        .map_err(Arc::new)?;
        let mut st = self.state.lock().await;
        if cancelled() || st.shutting_down {
            return Err(Arc::new(start_cancelled()));
        }
        // No accept task, readiness, or bound port is published until this
        // synchronous commit succeeds. Errors drop every reserved socket.
        if select {
            assignment.expect("selection has a persistence callback")(port).map_err(Arc::new)?;
            st.assigned_port = Some(port);
        }
        if cancelled() || st.shutting_down {
            return Err(Arc::new(start_cancelled()));
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
        // REV-2: `client:*` events for the shared registry (no-op when the
        // UDS listener already spawned the publisher).
        self.reverse_registry
            .spawn_client_event_publisher(self.api.clone());
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

    /// Graceful shutdown in the canonical order (port of `stop()`): bump the
    /// stop generation (cancels an in-flight start), stop the heartbeat, close
    /// every client with `1001`, drop their subscriptions, stop accepting and
    /// drop the listener, then await the accept loop so a subsequent `start()`
    /// cannot hit `EADDRINUSE`.
    pub(crate) async fn stop(self: &Arc<Self>) {
        self.external_stop_generation.fetch_add(1, Ordering::SeqCst);
        let _stop = self.stop_gate.lock().await;
        let (running, start_task) = {
            let mut st = self.state.lock().await;
            st.shutting_down = true;
            st.started = false;
            st.port = None;
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

/// The composition root commits the selected port while all listeners are held.
pub(crate) type PortAssignment = Arc<dyn Fn(u16) -> io::Result<()> + Send + Sync>;

fn start_cancelled() -> io::Error {
    io::Error::new(
        io::ErrorKind::Interrupted,
        "ws start aborted by concurrent stop",
    )
}

/// Bounded, consecutive selection. The injected binder keeps range/error tests
/// hermetic; production binds the entire address set in each attempt.
async fn select_port<T, F, Fut>(
    first: u16,
    last: u16,
    cancelled: impl Fn() -> bool,
    mut bind: F,
) -> io::Result<(T, u16)>
where
    F: FnMut(u16) -> Fut,
    Fut: std::future::Future<Output = io::Result<(T, u16)>>,
{
    for port in first..=last {
        if cancelled() {
            return Err(start_cancelled());
        }
        match bind(port).await {
            Ok(reserved) => return Ok(reserved),
            Err(e) if e.kind() == io::ErrorKind::AddrInUse && port < last => {
                // Bind often completes without yielding. Let stop() cancel even
                // an entirely occupied range on a single-thread runtime.
                tokio::task::yield_now().await;
            }
            Err(e) if e.kind() == io::ErrorKind::AddrInUse && first != last => {
                return Err(io::Error::new(
                    e.kind(),
                    format!("WSS port range {first}..={last} exhausted: {e}"),
                ));
            }
            Err(e) => return Err(e),
        }
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidInput,
        "empty WSS port range",
    ))
}

async fn bind_all(addresses: &[IpAddr], mut port: u16) -> io::Result<(Vec<TcpListener>, u16)> {
    if addresses.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "WsOptions.bind_addresses must be non-empty",
        ));
    }
    let mut listeners = Vec::with_capacity(addresses.len());
    for addr in addresses {
        let listener = bind_listener(*addr, port)
            .await
            .map_err(|e| io::Error::new(e.kind(), format!("bind {addr}:{port}: {e}")))?;
        port = listener.local_addr()?.port();
        listeners.push(listener);
    }
    Ok((listeners, port))
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

    #[tokio::test]
    async fn selection_is_consecutive_and_stops_at_first_success() {
        for occupied in 0..=2 {
            let attempts = std::sync::Mutex::new(Vec::new());
            let ((), port) = select_port(
                5181,
                u16::MAX,
                || false,
                |port| {
                    attempts.lock().unwrap().push(port);
                    std::future::ready(if port < 5181 + occupied {
                        Err(io::Error::from(io::ErrorKind::AddrInUse))
                    } else {
                        Ok(((), port))
                    })
                },
            )
            .await
            .unwrap();
            assert_eq!(port, 5181 + occupied);
            assert_eq!(*attempts.lock().unwrap(), (5181..=port).collect::<Vec<_>>());
        }
    }

    #[tokio::test]
    async fn selection_never_wraps_and_only_retries_contention() {
        for kind in [
            io::ErrorKind::AddrInUse,
            io::ErrorKind::PermissionDenied,
            io::ErrorKind::AddrNotAvailable,
        ] {
            let mut attempts = Vec::new();
            let error = select_port(
                65534,
                65535,
                || false,
                |port| {
                    attempts.push(port);
                    std::future::ready(Err::<((), u16), _>(io::Error::from(kind)))
                },
            )
            .await
            .unwrap_err();
            assert_eq!(error.kind(), kind);
            assert_eq!(
                attempts,
                if kind == io::ErrorKind::AddrInUse {
                    vec![65534, 65535]
                } else {
                    vec![65534]
                }
            );
        }
    }

    #[tokio::test]
    async fn selection_yields_and_cancels_between_busy_candidates() {
        let cancelled = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let signal = cancelled.clone();
        let stop = tokio::spawn(async move {
            signal.store(true, Ordering::SeqCst);
        });
        let mut attempts = 0;
        let error = select_port(
            5181,
            65535,
            || cancelled.load(Ordering::SeqCst),
            |_| {
                attempts += 1;
                std::future::ready(Err::<((), u16), _>(io::Error::from(
                    io::ErrorKind::AddrInUse,
                )))
            },
        )
        .await
        .unwrap_err();
        stop.await.unwrap();
        assert_eq!(error.kind(), io::ErrorKind::Interrupted);
        assert_eq!(attempts, 1);
    }

    #[tokio::test]
    async fn partial_bind_is_released_before_advancing() {
        let first = IpAddr::from([127, 0, 0, 1]);
        let second = IpAddr::V6(Ipv6Addr::LOCALHOST);
        let hog = match bind_listener(second, 0).await {
            Ok(listener) => listener,
            Err(e)
                if matches!(
                    e.kind(),
                    io::ErrorKind::AddrNotAvailable | io::ErrorKind::Unsupported
                ) =>
            {
                return
            }
            Err(e) => panic!("bind IPv6 loopback: {e}"),
        };
        let busy = hog.local_addr().unwrap().port();
        let error = bind_all(&[first, second], busy).await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::AddrInUse);
        let released = bind_listener(first, busy)
            .await
            .expect("partial first listener released");
        drop(released);
        let addresses = [first, second];
        let (listeners, port) =
            select_port(busy, u16::MAX, || false, |port| bind_all(&addresses, port))
                .await
                .unwrap();
        assert!(port > busy);
        assert_eq!(listeners.len(), 2);
        assert!(bind_listener(first, port).await.is_err());
        assert!(bind_listener(second, port).await.is_err());
    }

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
