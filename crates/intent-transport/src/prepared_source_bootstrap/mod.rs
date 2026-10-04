//! Isolated authenticated bootstrap test composition. Never installed by shipping listeners.
//! Strict authenticated hello precedes the prepared source-only lifecycle dispatcher.
//!
//! Each Services root lazily reserves at most240 read and16 cleanup context cells.
//! A pending accept owns one cell before TLS task spawn. Read guest limits are
//! additionally enforced against exact authenticated principals; cleanup bypasses
//! that read quota, while administrators still consume a context cell. Replacing
//! a listener preserves the root's outstanding cells; replacing Services rotates
//! its incarnation. Dropping an unretired context preserves process-local debt.
//!
//! Eligibility is ten monotonic seconds from accepted TCP ownership (idle accept
//! has no timer). Cancellation drops owned local handles, but joins pending token
//! loads and retains pending Store/validation futures before normal retirement.
//! Unknown/Internal/task failure retains the same cell. Local disposal is not
//! peer receipt, kernel drain, generic pooled-SQL cleanup acknowledgement, or
//! durable restart accounting.
//!
//! The TCP wrapper admits at most65536 actual bytes per direction; HTTP headers
//! including start line and delimiter fit16384 per direction, and complete escaped
//! hello JSON fits8192 per direction. No compression is negotiated. This bootstrap
//! transfers its SAME TLS/parser owner after successful hello flush. Checked TCP
//! totals remain monotonic with bootstrap end marks; active IO chunks are8192.
//! Active WS input frame/message limits are65536 and output remains8192+256.
//! The cumulative handshake ceiling is not a source-stream quota.
//! WS frame/message/read buffers are8192 and write buffering is8192+256. Rustls,
//! parsed Values/strings, auth results, Store pools, allocator overhead and kernel
//! buffers are separate from the fixed4096-byte encoded context cell and are not
//! claimed to fit an aggregate heap/physical budget.
//!
//! Exclusive sink readiness precedes final credential validation; then local
//! deadline/stop/revocation checks and `start_send` have no intervening await.
//! These are separate authorization and local disclosure boundaries, not atomic
//! latest-database-state at send. A local flush is not remote consumption.
mod hello;
mod io;
mod lifecycle;
#[cfg(test)]
mod tests;

#[cfg(test)]
pub(crate) async fn read_prepared_head_for_test<S: tokio::io::AsyncRead + Unpin>(
    stream: &mut S,
) -> std::io::Result<Vec<u8>> {
    io::read_head(stream).await
}

use self::io::{CountedTcp, Meter};
use crate::{AsyncTokenStore, SharedGuestLimits, TlsCertificate};
use base64::Engine as _;
use futures_util::{SinkExt, StreamExt};
use intent_core::{Error, PrincipalId, Result, WorkspaceApi};
use intent_services::{
    prepared_source_bootstrap::{Context, Contexts, Mode},
    Services,
};
use std::{
    future::Future,
    net::SocketAddr,
    sync::{atomic::Ordering, Arc},
    time::Duration,
};
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
    sync::watch,
    task::{JoinHandle, JoinSet},
    time::Instant,
};
use tokio_rustls::TlsAcceptor;
use tokio_tungstenite::{
    tungstenite::{
        handshake::derive_accept_key,
        protocol::{Role, WebSocketConfig},
        Message,
    },
    WebSocketStream,
};

/// Trusted endpoint pair; it contains no credentials and cannot select shipping listeners.
#[derive(Clone, Copy, Debug)]
pub struct Endpoints {
    pub read: SocketAddr,
    pub cleanup: SocketAddr,
}

/// Loopback-only explicit composition. Dropping requests stop, never aborts admitted work.
pub struct PreparedBootstrap {
    endpoints: Endpoints,
    stop: watch::Sender<bool>,
    tasks: Vec<JoinHandle<()>>,
    root: Arc<Contexts>,
}

struct Shared {
    api: Arc<Services>,
    root: Arc<Contexts>,
    tls: TlsAcceptor,
    token: AsyncTokenStore,
    guests: SharedGuestLimits,
    #[cfg(test)]
    observe: std::sync::Mutex<Vec<Observation>>,
    #[cfg(test)]
    hello_pending: tokio::sync::Notify,
    #[cfg(test)]
    validation_pending: tokio::sync::Notify,
    #[cfg(test)]
    partial_hello: std::sync::atomic::AtomicBool,
    #[cfg(test)]
    control_pending: tokio::sync::Notify,
    #[cfg(test)]
    source_pending: std::sync::atomic::AtomicUsize,
    #[cfg(test)]
    source_partial: std::sync::atomic::AtomicUsize,
    #[cfg(test)]
    partial_control: std::sync::atomic::AtomicBool,
    #[cfg(test)]
    auth_io: std::sync::Mutex<Vec<(usize, usize, usize, usize)>>,
    #[cfg(test)]
    page_auth_gate: std::sync::Mutex<Option<Arc<AuthGate>>>,
    #[cfg(test)]
    page_auth_pending: tokio::sync::Notify,
}

#[cfg(test)]
#[derive(Default)]
struct AuthGate {
    reached: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

#[cfg(test)]
#[derive(Debug)]
struct Observation {
    fail_write_at: usize,
    ready: bool,
    read: usize,
    written: usize,
    http_in: usize,
    http_out: usize,
    hello_in: usize,
    hello_out: usize,
    closed: bool,
}

impl PreparedBootstrap {
    /// Explicit prepared constructor; never invoked by ordinary daemon composition.
    ///
    /// # Errors
    /// Returns an error if TLS configuration or either loopback listener cannot be created.
    pub async fn start(
        api: Arc<Services>,
        certificate: &TlsCertificate,
        token: AsyncTokenStore,
        guests: SharedGuestLimits,
    ) -> Result<Self> {
        let shared = Arc::new(Shared {
            root: api.prepared_source_contexts(),
            api,
            tls: crate::ws::build_acceptor(certificate)?,
            token,
            guests,
            #[cfg(test)]
            observe: std::sync::Mutex::new(Vec::new()),
            #[cfg(test)]
            hello_pending: tokio::sync::Notify::new(),
            #[cfg(test)]
            validation_pending: tokio::sync::Notify::new(),
            #[cfg(test)]
            partial_hello: std::sync::atomic::AtomicBool::new(false),
            #[cfg(test)]
            control_pending: tokio::sync::Notify::new(),
            #[cfg(test)]
            source_pending: std::sync::atomic::AtomicUsize::new(0),
            #[cfg(test)]
            source_partial: std::sync::atomic::AtomicUsize::new(0),
            #[cfg(test)]
            partial_control: std::sync::atomic::AtomicBool::new(false),
            #[cfg(test)]
            auth_io: std::sync::Mutex::new(Vec::new()),
            #[cfg(test)]
            page_auth_gate: std::sync::Mutex::new(None),
            #[cfg(test)]
            page_auth_pending: tokio::sync::Notify::new(),
        });
        Self::start_shared(shared).await
    }
    async fn start_shared(shared: Arc<Shared>) -> Result<Self> {
        let read = TcpListener::bind("127.0.0.1:0").await.map_err(internal)?;
        let cleanup = TcpListener::bind("127.0.0.1:0").await.map_err(internal)?;
        let endpoints = Endpoints {
            read: read.local_addr().map_err(internal)?,
            cleanup: cleanup.local_addr().map_err(internal)?,
        };
        let (stop, _) = watch::channel(false);
        let tasks = [(Mode::Read, read), (Mode::Cleanup, cleanup)]
            .into_iter()
            .map(|(mode, listener)| {
                tokio::spawn(accept(shared.clone(), mode, listener, stop.subscribe()))
            })
            .collect();
        Ok(Self {
            endpoints,
            stop,
            tasks,
            root: shared.root.clone(),
        })
    }
    #[must_use]
    pub const fn endpoints(&self) -> Endpoints {
        self.endpoints
    }
    #[must_use]
    pub fn contexts(&self) -> Arc<Contexts> {
        self.root.clone()
    }
    pub fn request_stop(&self) {
        let _ = self.stop.send(true);
    }
    /// Await actual accept-loop/connection completion. A held auth load keeps this pending.
    pub async fn stop(mut self) {
        self.request_stop();
        for task in self.tasks.drain(..) {
            let _ = task.await;
        }
    }
}
impl Drop for PreparedBootstrap {
    fn drop(&mut self) {
        self.request_stop();
    }
}

fn internal(e: impl std::fmt::Display) -> Error {
    Error::Internal(e.to_string())
}
async fn cancelled(stop: &mut watch::Receiver<bool>) {
    loop {
        if *stop.borrow_and_update() {
            return;
        }
        if stop.changed().await.is_err() {
            return;
        }
    }
}

async fn accept(
    shared: Arc<Shared>,
    mode: Mode,
    listener: TcpListener,
    mut stop: watch::Receiver<bool>,
) {
    let mut tasks = JoinSet::new();
    loop {
        if *stop.borrow() {
            break;
        }
        // Register notification before inspecting capacity to avoid lost wakeups.
        let changed = shared.root.changed(mode);
        tokio::pin!(changed);
        let context = shared.root.try_admit(mode);
        let Some(context) = context else {
            tokio::select! { () = cancelled(&mut stop) => break, () = &mut changed => {}, Some(_) = tasks.join_next(), if !tasks.is_empty() => {} }
            continue;
        };
        let tcp = tokio::select! { () = cancelled(&mut stop) => { context.retire(); break; }, value = listener.accept() => value };
        if let Ok((tcp, _)) = tcp {
            let deadline = Instant::now() + Duration::from_secs(10);
            if *stop.borrow() {
                drop(tcp);
                context.retire();
                break;
            }
            let owned = shared.clone();
            let rx = stop.clone();
            tasks.spawn(async move {
                connection(owned, context, tcp, deadline, rx).await;
            });
        } else {
            context.retire();
            break;
        }
        while tasks.try_join_next().is_some() {}
    }
    drop(listener);
    // No abort_all/drop of live owned auth. Runtime/task loss leaves context quarantined.
    while tasks.join_next().await.is_some() {}
}

async fn eligible<F: Future>(
    future: F,
    deadline: Instant,
    stop: &mut watch::Receiver<bool>,
) -> Option<F::Output> {
    if *stop.borrow() || Instant::now() >= deadline {
        return None;
    }
    tokio::select! { biased; () = cancelled(stop) => None, () = tokio::time::sleep_until(deadline) => None, value = future => Some(value) }
}

struct Revocations {
    events: Option<tokio::sync::broadcast::Receiver<intent_core::PrincipalRevocation>>,
    rotation: Option<crate::auth::LegacyRotation>,
    principal: PrincipalId,
}
impl Revocations {
    fn now(&mut self) -> bool {
        if self
            .rotation
            .as_ref()
            .is_some_and(crate::auth::LegacyRotation::prepared_revoked_now)
        {
            return true;
        }
        if let Some(events) = &mut self.events {
            loop {
                match events.try_recv() {
                    Ok(event) if event.principal_id == self.principal => return true,
                    Ok(_) => {}
                    Err(tokio::sync::broadcast::error::TryRecvError::Empty) => break,
                    Err(_) => return true,
                }
            }
        }
        false
    }
    async fn wait(&mut self) {
        if self.now() {
            return;
        }
        loop {
            tokio::select! {
                () = async { match self.rotation.as_mut() { Some(rotation) => rotation.prepared_revoked().await, None => std::future::pending().await } } => return,
                event = crate::ws::recv_revocation(&mut self.events) => match event {
                    Ok(event) if event.principal_id != self.principal => {},
                    _ => return,
                }
            }
        }
    }
}

async fn authorized_wait<F: Future>(
    future: F,
    deadline: Instant,
    stop: &mut watch::Receiver<bool>,
    revoke: &mut Revocations,
) -> Option<F::Output> {
    if revoke.now() {
        return None;
    }
    tokio::select! { biased; () = revoke.wait() => None, value = eligible(future,deadline,stop) => value }
}

async fn connection(
    shared: Arc<Shared>,
    mut context: Context,
    tcp: TcpStream,
    deadline: Instant,
    mut stop: watch::Receiver<bool>,
) {
    let meter = Arc::new(Meter::default());
    let stream = CountedTcp::new(tcp, meter.clone());
    context.phase(1);
    let result = handshake(&shared, &mut context, stream, deadline, &mut stop).await;
    context.phase(6);
    #[cfg(test)]
    {
        let mut observations = shared.observe.lock().expect("test observations");
        // Test witness bounded independently; never holds auth data/source/frames.
        if observations.len() < 256 {
            observations.push(Observation {
                fail_write_at: meter.fail_write_at.load(Ordering::Acquire),
                ready: meter.ready.load(Ordering::Acquire),
                read: meter.read.load(Ordering::Acquire),
                written: meter.written.load(Ordering::Acquire),
                http_in: meter.http_in.load(Ordering::Acquire),
                http_out: meter.http_out.load(Ordering::Acquire),
                hello_in: meter.hello_in.load(Ordering::Acquire),
                hello_out: meter.hello_out.load(Ordering::Acquire),
                closed: meter.closed.load(Ordering::Acquire),
            });
        }
    }
    if !matches!(result, Err(Error::Internal(_))) && meter.closed.load(Ordering::Acquire) {
        context.retire();
    }
    // Every Internal/panic/unknown destruction keeps this same root cell uncertain.
}

struct Upgrade {
    token: String,
    key: String,
}
fn parse_upgrade(head: &[u8]) -> Result<Upgrade> {
    let mut headers = [httparse::EMPTY_HEADER; 64];
    let mut request = httparse::Request::new(&mut headers);
    if request
        .parse(head)
        .map_err(|_| Error::InvalidParams("HTTP".into()))?
        != httparse::Status::Complete(head.len())
        || request.method != Some("GET")
        || request.version != Some(1)
    {
        return Err(Error::InvalidParams("HTTP".into()));
    }
    let target = request.path.unwrap_or("");
    if target.split('?').next() != Some("/ws") {
        return Err(Error::InvalidParams("path".into()));
    }
    let get = |name: &str| -> Result<Option<&str>> {
        let mut matches = request
            .headers
            .iter()
            .filter(|h| h.name.eq_ignore_ascii_case(name));
        let value = matches
            .next()
            .map(|h| std::str::from_utf8(h.value))
            .transpose()
            .map_err(|_| Error::InvalidParams("header UTF8".into()))?;
        if matches.next().is_some() {
            return Err(Error::InvalidParams("duplicate header".into()));
        }
        Ok(value)
    };
    if !crate::auth::is_allowed_origin(get("origin")?)
        || get("sec-websocket-version")? != Some("13")
        || !get("upgrade")?.is_some_and(|s| s.eq_ignore_ascii_case("websocket"))
        || !get("connection")?.is_some_and(|s| {
            s.split(',')
                .any(|s| s.trim().eq_ignore_ascii_case("upgrade"))
        })
    {
        return Err(Error::Forbidden("upgrade refused".into()));
    }
    let key = get("sec-websocket-key")?.ok_or_else(|| Error::InvalidParams("key".into()))?;
    if base64::engine::general_purpose::STANDARD
        .decode(key)
        .map_or(true, |b| b.len() != 16)
    {
        return Err(Error::InvalidParams("key".into()));
    }
    let token = crate::auth::extract_token(get("authorization")?, target)
        .ok_or_else(|| Error::Forbidden("credential required".into()))?;
    Ok(Upgrade {
        token,
        key: key.into(),
    })
}

async fn handshake(
    shared: &Shared,
    context: &mut Context,
    stream: CountedTcp,
    deadline: Instant,
    stop: &mut watch::Receiver<bool>,
) -> Result<()> {
    let meter = stream.meter.clone();
    let Some(tls) = eligible(shared.tls.accept(stream), deadline, stop).await else {
        return Ok(());
    };
    let mut tls = Some(tls.map_err(|_| Error::InvalidParams("TLS refused".into()))?);
    context.phase(2);
    let Some(head) = eligible(io::read_head(tls.as_mut().expect("TLS")), deadline, stop).await
    else {
        return Ok(());
    };
    let head = head.map_err(|_| Error::InvalidParams("HTTP budget or closed".into()))?;
    meter.http_in.store(head.len(), Ordering::Release);
    let upgrade = parse_upgrade(&head)?;
    // Capture both actual feeds BEFORE async credential resolution can stall.
    let events = shared.api.subscribe_principal_revocations();
    let rotation = crate::auth::LegacyRotation::new(&shared.token, &upgrade.token);
    context.phase(3);
    let auth = async {
        let credential = crate::auth::prepared_validate_token(
            &shared.token,
            shared.api.as_ref(),
            &upgrade.token,
        )
        .await?
        .ok_or_else(|| Error::Forbidden("credential refused".into()))?;
        let caller = credential.prepared_into_caller(shared.api.as_ref()).await?;
        Ok::<_, Error>((credential, caller))
    };
    tokio::pin!(auth);
    let (credential, caller) = if let Some(outcome) = eligible(&mut auth, deadline, stop).await {
        outcome?
    } else {
        context.phase(6);
        drop(tls.take());
        let outcome = auth.await;
        return outcome.map(|_| ());
    };
    let admitted =
        crate::auth::AdmittedCredential::new(credential, upgrade.token.clone(), rotation);
    let mut revoke = Revocations {
        events,
        rotation: None,
        principal: caller.principal_id().expect("wire caller").clone(),
    };
    // Keep the credential and its revocation watcher distinct so validation can
    // be owned concurrently with cancellation, without detaching either future.
    let mut admitted = admitted;
    revoke.rotation = admitted.rotation.take();
    if revoke.now() {
        return Err(Error::Forbidden("credential revoked".into()));
    }
    let mut epoch = [0; 16];
    getrandom::fill(&mut epoch).map_err(internal)?;
    let guest_limits = (!caller.is_administrator()).then(|| {
        let limits = shared.guests.get();
        (
            limits.max_guest_connections as usize,
            limits.max_connections_per_guest as usize,
        )
    });
    if !context.bind(
        caller.principal_id().expect("wire principal").as_str(),
        epoch,
        guest_limits,
    ) {
        return Err(Error::InvalidParams("source-session-capacity".into()));
    }
    let response = format!("HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: {}\r\n\r\n",derive_accept_key(upgrade.key.as_bytes()));
    if response.len() > io::HTTP_LIMIT {
        return Err(Error::InvalidParams("HTTP budget".into()));
    }
    meter.http_out.store(response.len(), Ordering::Release);
    let Some(written) = authorized_wait(
        async {
            let s = tls.as_mut().expect("TLS");
            s.write_all(response.as_bytes()).await?;
            s.flush().await
        },
        deadline,
        stop,
        &mut revoke,
    )
    .await
    else {
        return Ok(());
    };
    written.map_err(|_| Error::InvalidParams("upgrade write failed".into()))?;
    let config = WebSocketConfig::default()
        .read_buffer_size(8192)
        .write_buffer_size(0)
        .max_write_buffer_size(8192 + 256)
        .max_message_size(Some(8192))
        .max_frame_size(Some(8192));
    // No extension/compression negotiation, no generic dispatcher or response queue.
    let mut ws = Some(
        WebSocketStream::from_raw_socket(tls.take().expect("TLS"), Role::Server, Some(config))
            .await,
    );
    context.phase(4);
    let Some(frame) =
        authorized_wait(ws.as_mut().expect("WS").next(), deadline, stop, &mut revoke).await
    else {
        return Ok(());
    };
    let Some(Ok(Message::Text(text))) = frame else {
        return Err(Error::InvalidParams("hello frame".into()));
    };
    meter.hello_in.store(text.len(), Ordering::Release);
    let incarnation = context.incarnation().to_owned();
    let hello = hello::response(
        &text,
        context.mode(),
        &incarnation,
        caller.clone(),
        &shared.api,
    );
    tokio::pin!(hello);
    // Test observation only: notify after the actual persistence future polls Pending.
    #[cfg(test)]
    let hello = futures_util::future::poll_fn(|cx| {
        let polled = hello.as_mut().poll(cx);
        if polled.is_pending() {
            shared.hello_pending.notify_one();
        }
        polled
    });
    #[cfg(test)]
    tokio::pin!(hello);
    let response =
        if let Some(value) = authorized_wait(&mut hello, deadline, stop, &mut revoke).await {
            value?
        } else {
            context.phase(6);
            drop(ws.take());
            let outcome = hello.await;
            return outcome.map(|_| ());
        };
    let ready = futures_util::future::poll_fn(|cx| ws.as_mut().expect("WS").poll_ready_unpin(cx));
    let Some(ready) = authorized_wait(ready, deadline, stop, &mut revoke).await else {
        return Ok(());
    };
    ready.map_err(|_| Error::InvalidParams("hello sink readiness".into()))?;
    let validation = admitted.prepared_valid_for(&shared.token, shared.api.as_ref(), &caller);
    tokio::pin!(validation);
    #[cfg(test)]
    let validation = futures_util::future::poll_fn(|cx| {
        let polled = validation.as_mut().poll(cx);
        if polled.is_pending() {
            shared.validation_pending.notify_one();
        }
        polled
    });
    #[cfg(test)]
    tokio::pin!(validation);
    match authorized_wait(&mut validation, deadline, stop, &mut revoke).await {
        Some(Ok(true)) => {}
        Some(Ok(false)) => return Err(Error::Forbidden("credential revoked".into())),
        Some(Err(error)) => return Err(error),
        None => {
            context.phase(6);
            drop(ws.take());
            return validation.await.map(|_| ());
        }
    }
    // Exclusive sink readiness is retained across validation. No other writer,
    // queue or await exists between local checks and start_send.
    if Instant::now() >= deadline || *stop.borrow() || revoke.now() {
        return Ok(());
    }
    meter.hello_out.store(response.len(), Ordering::Release);
    #[cfg(test)]
    if shared.partial_hello.load(Ordering::Acquire) {
        // Fault injection AFTER actual authentication, persistence and revalidation:
        // allow eight real TLS bytes into TCP, then fail the continuation.
        meter
            .fail_write_at
            .store(meter.written.load(Ordering::Acquire) + 8, Ordering::Release);
    }
    ws.as_mut()
        .expect("WS")
        .start_send_unpin(Message::Text(response.into()))
        .map_err(|_| Error::InvalidParams("hello start_send".into()))?;
    let Some(sent) = authorized_wait(
        ws.as_mut().expect("WS").flush(),
        deadline,
        stop,
        &mut revoke,
    )
    .await
    else {
        return Ok(());
    };
    sent.map_err(|_| Error::InvalidParams("hello send failed".into()))?;
    if Instant::now() >= deadline || *stop.borrow() || revoke.now() {
        return Ok(());
    }
    meter.ready.store(true, Ordering::Release);
    context.phase(5);
    let source = shared
        .api
        .prepared_source_connection(context, caller.clone())
        .map_err(|e| Error::InvalidParams(e.code().into()))?;
    // Complete authenticated Text hello, successful owned flush and final local
    // checks precede this SAME-stream update. No next() lookahead or reconstruction.
    // Only input limits change; known-valid output thresholds remain untouched.
    ws.as_mut().expect("WS").set_config(|config| {
        config.max_message_size = Some(65536);
        config.max_frame_size = Some(65536);
    });
    ws.as_mut()
        .expect("WS")
        .get_mut()
        .get_mut()
        .1
        .set_buffer_limit(Some(65536));
    meter.activate().map_err(internal)?;
    lifecycle::run(
        shared,
        context,
        &mut ws,
        caller.clone(),
        &admitted,
        stop,
        &mut revoke,
        source,
    )
    .await
}
