//! Git sessions over the link's git connection.
//!
//! Head dials both connections, but git transfers start on the node: the
//! node's `git-remote-intent` helper opens a session for one agent remote
//! and service, and head answers it with `git upload-pack` or
//! `git receive-pack` confined to that agent's refs (see
//! [`intent_git::node_git`]). Sessions are multiplexed on the one git
//! connection; every message is
//!
//! ```text
//! [kind: u8][session: u32 big-endian][payload]
//! ```
//!
//! - `OPEN` (node → head): JSON [`OpenRequest`].
//! - `ACCEPT` (head → node): the service is running.
//! - `DATA` (both): protocol bytes; node → head is the service's stdin.
//! - `EOF` (node → head): the client has nothing more to send.
//! - `CLOSE` (both): the session ended; head's carries JSON [`CloseStatus`].
//!
//! Head authorizes every session through [`NodeGitAccess`] with the
//! connection's binding, so a session for an agent on another lease, or on a
//! binding that is no longer current, is refused before git runs.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::sync::mpsc;

use intent_core::node_link::LeaseBinding;
use intent_core::{BoxFuture, Error, Result};
use intent_git::node_git::{service_command, AgentRemote, GitService};

use super::{GitConnection, GitSender};

const OPEN: u8 = 1;
const ACCEPT: u8 = 2;
const DATA: u8 = 3;
const EOF: u8 = 4;
const CLOSE: u8 = 5;

const HEADER: usize = 5;
/// Largest data payload per message.
const CHUNK: usize = 64 * 1024;
/// Concurrent sessions head runs for one node.
pub const MAX_SESSIONS: usize = 8;
/// Messages queued per session before the connection's reader waits.
const SESSION_QUEUE: usize = 16;
/// How long a node session waits for head's git connection to attach.
/// Head binds the link and dials the git connection right after, so a
/// launch's checkout can open its session first (intent-hq/intent#8383).
pub const ATTACH_WAIT: std::time::Duration = std::time::Duration::from_secs(10);

/// Stable session error codes.
pub mod codes {
    /// Head refused the remote: the agent is not on this lease, or the
    /// binding is no longer current.
    pub const DENIED: &str = "node-git-denied";
    /// Head already runs [`super::MAX_SESSIONS`] sessions for this node.
    pub const BUSY: &str = "node-git-busy";
    /// The open request was malformed.
    pub const INVALID: &str = "node-git-invalid";
    /// The git service could not start or failed.
    pub const FAILED: &str = "node-git-failed";
    /// No git connection, or it closed during the session.
    pub const UNAVAILABLE: &str = "node-git-unavailable";
    /// A push on a forge remote (nodeProtocol 17): sub-agents hand work
    /// back through their agent refs.
    pub const PUSH_REFUSED: &str = "node-git-push-refused";
}

/// What the helper prints when head refuses a push on a forge remote.
pub const PUSH_REFUSED_MESSAGE: &str =
    "sub-agents hand work back through intent1; ask your parent to push";

/// The warning a forge ACCEPT carries when head served the last fetched
/// refs because the GitHub refresh failed.
pub const FORGE_REFRESH_FAILED: &str = "forge-refresh-failed";

/// What the helper prints for [`FORGE_REFRESH_FAILED`].
pub const FORGE_REFRESH_FAILED_MESSAGE: &str =
    "could not refresh from GitHub; showing the last fetched refs";

/// Forge sessions the node opens at a time; more wait for a slot.
pub const MAX_FORGE_SESSIONS: usize = 2;

/// How long a forge session waits on the node for a slot before it fails
/// with [`codes::BUSY`].
pub const FORGE_SLOT_WAIT: std::time::Duration = std::time::Duration::from_secs(30);

/// A forge ACCEPT's payload (nodeProtocol 17).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AcceptStatus {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub warning: Option<String>,
}

/// The repository head serves a forge remote from, and the warning its
/// ACCEPT carries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForgeServe {
    pub repository: PathBuf,
    pub warning: Option<&'static str>,
}

/// What a node asks head to run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct OpenRequest {
    pub service: String,
    pub url: String,
}

/// How head ended a session; `error` is one of [`codes`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CloseStatus {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

fn encode(kind: u8, session: u32, payload: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(HEADER + payload.len());
    bytes.push(kind);
    bytes.extend_from_slice(&session.to_be_bytes());
    bytes.extend_from_slice(payload);
    bytes
}

fn decode(mut bytes: Vec<u8>) -> Option<(u8, u32, Vec<u8>)> {
    if bytes.len() < HEADER {
        return None;
    }
    let kind = bytes[0];
    let session = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
    bytes.drain(..HEADER);
    Some((kind, session, bytes))
}

fn session_error(code: &str) -> Error {
    match code {
        codes::DENIED => Error::Forbidden(codes::DENIED.into()),
        codes::BUSY => Error::Internal(codes::BUSY.into()),
        codes::INVALID => Error::InvalidParams(codes::INVALID.into()),
        codes::UNAVAILABLE => Error::Internal(codes::UNAVAILABLE.into()),
        codes::PUSH_REFUSED => Error::Forbidden(codes::PUSH_REFUSED.into()),
        _ => Error::Internal(codes::FAILED.into()),
    }
}

/// The [`codes`] entry a session error carries.
#[cfg(unix)]
fn code_of(error: &Error) -> &str {
    match error {
        Error::Forbidden(m) | Error::Internal(m) | Error::InvalidParams(m)
            if m.starts_with("node-git-") =>
        {
            m.as_str()
        }
        _ => codes::FAILED,
    }
}

fn close_message(session: u32, error: Option<&'static str>) -> Vec<u8> {
    let status = CloseStatus {
        error: error.map(str::to_owned),
    };
    let payload = serde_json::to_vec(&status).unwrap_or_default();
    encode(CLOSE, session, &payload)
}

/// Head's authority over node git sessions.
pub trait NodeGitAccess: Send + Sync {
    /// The repository holding `remote`'s refs for `service`, only if
    /// `binding` is the node's current binding and `remote`'s agent runs on
    /// its lease. An error carrying [`codes::UNAVAILABLE`] or [`codes::INVALID`]
    /// closes the session with that code; any other refuses it
    /// ([`codes::DENIED`]).
    fn repository<'a>(
        &'a self,
        binding: &'a LeaseBinding,
        remote: &'a AgentRemote,
        service: GitService,
    ) -> BoxFuture<'a, Result<PathBuf>>;

    /// The repository serving `remote`'s forge upload-pack (nodeProtocol
    /// 17), refreshed from GitHub, under the same authority as
    /// [`Self::repository`]. Errors map to codes as there.
    fn forge<'a>(
        &'a self,
        binding: &'a LeaseBinding,
        remote: &'a AgentRemote,
    ) -> BoxFuture<'a, Result<ForgeServe>> {
        let _ = (binding, remote);
        Box::pin(async { Err(Error::InvalidParams(codes::INVALID.into())) })
    }

    /// Whether the link negotiated forge remotes (nodeProtocol 17); a
    /// forge session on an older link is [`codes::INVALID`].
    fn forge_supported(&self) -> bool {
        false
    }
}

struct HeadSession {
    stdin: Option<mpsc::Sender<Vec<u8>>>,
    task: tokio::task::AbortHandle,
}

/// Head side: answer the node's git sessions on `connection` until it
/// closes. A protocol violation closes the connection; dropping the future
/// ends every running session (see [`Service`] for what becomes of its
/// service).
pub async fn serve_git(mut connection: GitConnection, access: Arc<dyn NodeGitAccess>) {
    let sender = connection.sender();
    // A slot is held until the session's service exits, so services that
    // outlive their sessions still count against [`MAX_SESSIONS`].
    let slots = Arc::new(tokio::sync::Semaphore::new(MAX_SESSIONS));
    let binding = connection.binding().clone();
    let mut tasks = tokio::task::JoinSet::new();
    let mut sessions: HashMap<u32, HeadSession> = HashMap::new();
    loop {
        let message = tokio::select! {
            message = connection.recv() => message,
            Some(done) = tasks.join_next() => {
                if let Ok(id) = done {
                    sessions.remove(&id);
                }
                continue;
            }
        };
        let Some((kind, id, payload)) = message.and_then(decode) else {
            break;
        };
        match kind {
            OPEN => {
                if sessions.contains_key(&id) {
                    break;
                }
                let Ok(slot) = slots.clone().try_acquire_owned() else {
                    let _ = sender.send(close_message(id, Some(codes::BUSY))).await;
                    continue;
                };
                let (stdin, stdin_rx) = mpsc::channel(SESSION_QUEUE);
                let task = tasks.spawn(head_session(
                    id,
                    payload,
                    binding.clone(),
                    access.clone(),
                    sender.clone(),
                    stdin_rx,
                    slot,
                ));
                sessions.insert(
                    id,
                    HeadSession {
                        stdin: Some(stdin),
                        task,
                    },
                );
            }
            DATA => {
                let stdin = sessions.get(&id).and_then(|s| s.stdin.clone());
                if let Some(stdin) = stdin {
                    let _ = stdin.send(payload).await;
                }
            }
            EOF => {
                if let Some(session) = sessions.get_mut(&id) {
                    session.stdin = None;
                }
            }
            CLOSE => {
                if let Some(session) = sessions.remove(&id) {
                    session.task.abort();
                }
            }
            _ => break,
        }
    }
    sender.close();
}

async fn head_session(
    id: u32,
    open: Vec<u8>,
    binding: LeaseBinding,
    access: Arc<dyn NodeGitAccess>,
    sender: GitSender,
    stdin_rx: mpsc::Receiver<Vec<u8>>,
    slot: tokio::sync::OwnedSemaphorePermit,
) -> u32 {
    let result = run_service(
        id,
        &open,
        &binding,
        access.as_ref(),
        &sender,
        stdin_rx,
        slot,
    )
    .await;
    let _ = sender.send(close_message(id, result.err())).await;
    id
}

/// The session code for an access refusal: [`codes::UNAVAILABLE`] or
/// [`codes::INVALID`] when the error says so, [`codes::DENIED`] otherwise.
fn access_code(error: &intent_core::Error) -> &'static str {
    match error {
        intent_core::Error::Internal(message) if message.starts_with(codes::UNAVAILABLE) => {
            codes::UNAVAILABLE
        }
        intent_core::Error::InvalidParams(message) if message == codes::INVALID => codes::INVALID,
        _ => codes::DENIED,
    }
}

async fn run_service(
    id: u32,
    open: &[u8],
    binding: &LeaseBinding,
    access: &dyn NodeGitAccess,
    sender: &GitSender,
    mut stdin_rx: mpsc::Receiver<Vec<u8>>,
    slot: tokio::sync::OwnedSemaphorePermit,
) -> std::result::Result<(), &'static str> {
    let open: OpenRequest = serde_json::from_slice(open).map_err(|_| codes::INVALID)?;
    let service = GitService::parse(&open.service).map_err(|_| codes::INVALID)?;
    let remote = AgentRemote::parse(&open.url).map_err(|_| codes::INVALID)?;
    let (command, accept) = if remote.is_forge() {
        if !access.forge_supported() {
            return Err(codes::INVALID);
        }
        if service == GitService::ReceivePack {
            return Err(codes::PUSH_REFUSED);
        }
        let served = access
            .forge(binding, &remote)
            .await
            .map_err(|error| access_code(&error))?;
        let accept = served.warning.map(|warning| {
            serde_json::to_vec(&AcceptStatus {
                warning: Some(warning.to_owned()),
            })
            .unwrap_or_default()
        });
        (
            intent_git::node_git::forge::upload_pack_command(&served.repository),
            accept.unwrap_or_default(),
        )
    } else {
        let repository = access
            .repository(binding, &remote, service)
            .await
            .map_err(|error| access_code(&error))?;
        (
            service_command(service, &repository, &remote).map_err(|_| codes::FAILED)?,
            Vec::new(),
        )
    };
    // A push is not killed with its session: see [`Service`]. A fetch
    // writes no ref, so it is.
    let drain = service == GitService::ReceivePack;
    let mut command = command;
    command.kill_on_drop(!drain);
    let mut child = command.spawn().map_err(|_| codes::FAILED)?;
    let (Some(mut stdin), Some(stdout)) = (child.stdin.take(), child.stdout.take()) else {
        return Err(codes::FAILED);
    };
    let mut service = Service {
        child: Some(child),
        stdout: Some(stdout),
        drain,
        slot: Some(slot),
    };
    sender
        .send(encode(ACCEPT, id, &accept))
        .await
        .map_err(|_| codes::UNAVAILABLE)?;
    let input = async move {
        while let Some(bytes) = stdin_rx.recv().await {
            if stdin.write_all(&bytes).await.is_err() {
                return;
            }
        }
        // Dropping stdin tells the service the client is done.
    };
    let stdout = service.stdout.as_mut().ok_or(codes::FAILED)?;
    let output = async {
        let mut buffer = vec![0; CHUNK];
        loop {
            let count = stdout.read(&mut buffer).await.map_err(|_| codes::FAILED)?;
            if count == 0 {
                return Ok::<(), &'static str>(());
            }
            sender
                .send(encode(DATA, id, &buffer[..count]))
                .await
                .map_err(|_| codes::UNAVAILABLE)?;
        }
    };
    tokio::pin!(input, output);
    let mut input_done = false;
    let streamed = loop {
        tokio::select! {
            result = &mut output => break result,
            () = &mut input, if !input_done => input_done = true,
        }
    };
    streamed?;
    let mut child = service.child.take().ok_or(codes::FAILED)?;
    let status = child.wait().await.map_err(|_| codes::FAILED)?;
    if status.success() {
        Ok(())
    } else {
        Err(codes::FAILED)
    }
}

/// Longest a service whose session ended early may run on before it is
/// killed.
const SERVICE_DRAIN: std::time::Duration = std::time::Duration::from_secs(60);

/// A session's git service. When the session ends before a receive-pack
/// does (a `CLOSE`, a lost link), it is not killed: `SIGKILL` during its
/// ref update leaves the ref's `.lock` behind for good, and every later
/// write to that ref fails. Its input is closed instead and its output
/// drained, so it ends on its own: a push received whole still lands, a
/// partial one aborts without touching a ref. Only one still running
/// after [`SERVICE_DRAIN`] is killed. It holds its session's slot until
/// it exits. An upload-pack is killed with its session.
struct Service {
    child: Option<tokio::process::Child>,
    stdout: Option<tokio::process::ChildStdout>,
    drain: bool,
    slot: Option<tokio::sync::OwnedSemaphorePermit>,
}

impl Drop for Service {
    fn drop(&mut self) {
        let Some(mut child) = self.child.take() else {
            return;
        };
        if !self.drain {
            // `kill_on_drop`.
            return;
        }
        let stdout = self.stdout.take();
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            // No runtime to wait on: the service runs on unattended.
            return;
        };
        let slot = self.slot.take();
        runtime.spawn(async move {
            let drain = async {
                if let Some(mut stdout) = stdout {
                    let _ = tokio::io::copy(&mut stdout, &mut tokio::io::sink()).await;
                }
                child.wait().await
            };
            if tokio::time::timeout(SERVICE_DRAIN, drain).await.is_err() {
                tracing::warn!("node git: a service outlived its session; killing it");
                let _ = child.kill().await;
            }
            drop(slot);
        });
    }
}

/// An event for one node-side session.
#[derive(Debug)]
enum SessionEvent {
    /// `None` for an empty payload; else the decoded [`AcceptStatus`]
    /// warning, `Err` when the payload is not one.
    Accepted(Option<std::result::Result<Option<String>, ()>>),
    Data(Vec<u8>),
    Closed(Option<String>),
}

#[derive(Debug, Default)]
struct NodeState {
    connection: Option<(u64, GitSender)>,
    epoch: u64,
    next_id: u32,
    sessions: HashMap<u32, mpsc::Sender<SessionEvent>>,
}

/// Node side: opens git sessions on the current git connection.
#[derive(Clone)]
pub struct NodeGitSessions {
    state: Arc<Mutex<NodeState>>,
    /// [`MAX_FORGE_SESSIONS`] slots for forge sessions.
    forge_slots: Arc<tokio::sync::Semaphore>,
    /// Bumped on every attach, so an open waiting for a connection wakes.
    attached: tokio::sync::watch::Sender<u64>,
}

impl Default for NodeGitSessions {
    fn default() -> Self {
        Self {
            state: Arc::default(),
            forge_slots: Arc::new(tokio::sync::Semaphore::new(MAX_FORGE_SESSIONS)),
            attached: tokio::sync::watch::channel(0).0,
        }
    }
}

impl std::fmt::Debug for NodeGitSessions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeGitSessions").finish_non_exhaustive()
    }
}

fn lock(state: &Mutex<NodeState>) -> std::sync::MutexGuard<'_, NodeState> {
    state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

impl NodeGitSessions {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Use `connection` for new sessions. Sessions on the previous
    /// connection end as unavailable.
    pub fn attach(&self, mut connection: GitConnection) {
        let sender = connection.sender();
        let (epoch, previous) = {
            let mut state = lock(&self.state);
            state.epoch += 1;
            let epoch = state.epoch;
            let previous = state.connection.replace((epoch, sender.clone()));
            state.sessions.clear();
            (state.epoch, previous)
        };
        self.attached
            .send_modify(|epoch| *epoch = epoch.wrapping_add(1));
        // The previous connection's head sessions end when it closes, and
        // its reader stops even if it was waiting on a full session queue.
        if let Some((_, previous)) = previous {
            previous.close();
        }
        let state = self.state.clone();
        tokio::spawn(async move {
            while let Some((kind, id, payload)) = connection.recv().await.and_then(decode) {
                let event = match kind {
                    ACCEPT if payload.is_empty() => SessionEvent::Accepted(None),
                    ACCEPT => SessionEvent::Accepted(Some(
                        serde_json::from_slice::<AcceptStatus>(&payload)
                            .map(|status| status.warning)
                            .map_err(|_| ()),
                    )),
                    DATA => SessionEvent::Data(payload),
                    CLOSE => SessionEvent::Closed(
                        serde_json::from_slice::<CloseStatus>(&payload)
                            .map_or_else(|_| Some(codes::FAILED.to_owned()), |s| s.error),
                    ),
                    _ => break,
                };
                let session = {
                    let state = lock(&state);
                    if state.epoch != epoch {
                        break;
                    }
                    state.sessions.get(&id).cloned()
                };
                if let Some(session) = session {
                    tokio::select! {
                        _ = session.send(event) => {}
                        () = sender.closed() => break,
                    }
                }
            }
            connection.close();
            let mut state = lock(&state);
            if state.epoch == epoch {
                state.connection = None;
                state.sessions.clear();
            }
        });
    }

    /// Whether a git connection is attached.
    #[must_use]
    pub fn is_connected(&self) -> bool {
        lock(&self.state).connection.is_some()
    }

    /// Open a session for `service` on `remote` and wait for head to accept
    /// it.
    ///
    /// # Errors
    ///
    /// A [`codes`] error: unavailable when no connection attaches within
    /// [`ATTACH_WAIT`], or head's refusal.
    pub async fn open(&self, service: GitService, remote: &AgentRemote) -> Result<GitSession> {
        let slot = if remote.is_forge() {
            match tokio::time::timeout(FORGE_SLOT_WAIT, self.forge_slots.clone().acquire_owned())
                .await
            {
                Ok(Ok(slot)) => Some(slot),
                _ => return Err(session_error(codes::BUSY)),
            }
        } else {
            None
        };
        let (events_tx, mut events) = mpsc::channel(SESSION_QUEUE);
        let mut attached = self.attached.subscribe();
        let deadline = tokio::time::Instant::now() + ATTACH_WAIT;
        let (id, epoch, sender) = loop {
            {
                let mut state = lock(&self.state);
                if let Some((epoch, sender)) = state.connection.clone() {
                    let id = state.next_id;
                    state.next_id = state.next_id.wrapping_add(1);
                    state.sessions.insert(id, events_tx);
                    break (id, epoch, sender);
                }
            }
            // Head dials the git connection right after binding the link;
            // wait for it rather than fail the launch's checkout.
            match tokio::time::timeout_at(deadline, attached.changed()).await {
                Ok(Ok(())) => {}
                _ => return Err(session_error(codes::UNAVAILABLE)),
            }
        };
        let mut session = GitSession {
            id,
            epoch,
            sender,
            events: None,
            state: self.state.clone(),
            closed: false,
            warning: None,
            _slot: slot,
        };
        let open = OpenRequest {
            service: service.as_str().to_owned(),
            url: remote.url(),
        };
        let payload = serde_json::to_vec(&open).map_err(|_| session_error(codes::INVALID))?;
        session
            .sender
            .send(encode(OPEN, id, &payload))
            .await
            .map_err(|_| session_error(codes::UNAVAILABLE))?;
        match events.recv().await {
            Some(SessionEvent::Accepted(None)) => {
                session.events = Some(events);
                Ok(session)
            }
            Some(SessionEvent::Accepted(Some(Ok(warning)))) if remote.is_forge() => {
                session.events = Some(events);
                session.warning = warning;
                Ok(session)
            }
            // A payload on another remote, or one that does not decode:
            // dropping the session closes it.
            Some(SessionEvent::Accepted(_)) => Err(session_error(codes::INVALID)),
            Some(SessionEvent::Closed(error)) => {
                session.closed = true;
                Err(session_error(error.as_deref().unwrap_or(codes::FAILED)))
            }
            Some(SessionEvent::Data(_)) | None => Err(session_error(codes::UNAVAILABLE)),
        }
    }
}

/// One open session. Dropping it before head closed it cancels the service.
#[derive(Debug)]
pub struct GitSession {
    id: u32,
    epoch: u64,
    sender: GitSender,
    events: Option<mpsc::Receiver<SessionEvent>>,
    state: Arc<Mutex<NodeState>>,
    closed: bool,
    warning: Option<String>,
    _slot: Option<tokio::sync::OwnedSemaphorePermit>,
}

impl GitSession {
    /// The warning head's ACCEPT carried (a forge session only).
    #[must_use]
    pub fn warning(&self) -> Option<&str> {
        self.warning.as_deref()
    }

    /// Send bytes to the service's stdin.
    ///
    /// # Errors
    ///
    /// `node-git-unavailable` when the connection is gone.
    pub async fn send(&self, bytes: &[u8]) -> Result<()> {
        for chunk in bytes.chunks(CHUNK) {
            self.sender
                .send(encode(DATA, self.id, chunk))
                .await
                .map_err(|_| session_error(codes::UNAVAILABLE))?;
        }
        Ok(())
    }

    /// Tell the service there is no more input.
    ///
    /// # Errors
    ///
    /// `node-git-unavailable` when the connection is gone.
    pub async fn finish(&self) -> Result<()> {
        self.sender
            .send(encode(EOF, self.id, &[]))
            .await
            .map_err(|_| session_error(codes::UNAVAILABLE))
    }

    /// The service's next output, `None` once it finished successfully.
    ///
    /// # Errors
    ///
    /// Head's failure code, or `node-git-unavailable` when the connection
    /// closed first.
    pub async fn recv(&mut self) -> Result<Option<Vec<u8>>> {
        if self.closed {
            return Ok(None);
        }
        let Some(events) = self.events.as_mut() else {
            return Err(session_error(codes::UNAVAILABLE));
        };
        match events.recv().await {
            Some(SessionEvent::Data(bytes)) => Ok(Some(bytes)),
            Some(SessionEvent::Closed(None)) => {
                self.closed = true;
                Ok(None)
            }
            Some(SessionEvent::Closed(Some(code))) => {
                self.closed = true;
                Err(session_error(&code))
            }
            Some(SessionEvent::Accepted(_)) | None => {
                self.closed = true;
                Err(session_error(codes::UNAVAILABLE))
            }
        }
    }
}

impl Drop for GitSession {
    fn drop(&mut self) {
        let mut state = lock(&self.state);
        if state.epoch == self.epoch {
            state.sessions.remove(&self.id);
        }
        drop(state);
        if self.closed {
            return;
        }
        // Head must learn of the cancellation, or its service and session
        // slot stay live: wait for queue space, or close the connection
        // when there is no runtime to wait on.
        if let Err(close) = self.sender.try_send(encode(CLOSE, self.id, &[])) {
            match tokio::runtime::Handle::try_current() {
                Ok(runtime) => {
                    let sender = self.sender.clone();
                    runtime.spawn(async move {
                        let _ = sender.send(close).await;
                    });
                }
                Err(_) => self.sender.close(),
            }
        }
    }
}

/// Node side: serve `git-remote-intent` helpers on the node's local socket.
/// Each helper connection writes one `<service> <url>\n` line, gets back one
/// status byte (`0` accepted, `1` refused followed by an error code line),
/// then carries the session's bytes both ways. A forge session whose ACCEPT
/// carried a warning answers `2` followed by the warning line instead of
/// `0`.
#[cfg(unix)]
pub async fn serve_helpers(listener: tokio::net::UnixListener, sessions: NodeGitSessions) {
    serve_helpers_with(listener, sessions, None).await;
}

/// The machine's current forge token for `host`, for
/// [`serve_helpers_with`] (nodeProtocol 15).
#[cfg(unix)]
pub type HelperCredential =
    std::sync::Arc<dyn Fn(&str) -> Option<intent_core::node_link::SecretValue> + Send + Sync>;

/// The request line of `intentd git-credential --node`: the token for
/// `<host>` follows.
#[cfg(unix)]
pub const CREDENTIAL_REQUEST: &str = "credential";

/// [`serve_helpers`], also answering `credential <host>\n` from
/// `credential`: status byte `0` then the token and `\n`, or `1` then an
/// error code line. The token is never logged.
#[cfg(unix)]
pub async fn serve_helpers_with(
    listener: tokio::net::UnixListener,
    sessions: NodeGitSessions,
    credential: Option<HelperCredential>,
) {
    while let Ok((stream, _)) = listener.accept().await {
        let sessions = sessions.clone();
        let credential = credential.clone();
        tokio::spawn(async move {
            let _ = serve_helper(stream, &sessions, credential.as_ref(), None).await;
        });
    }
}

/// One sub-agent's own helper socket (nodeProtocol 17): it opens only
/// `forge`, that agent's forge remote, and answers no credential request.
/// Another agent's forge remote is [`codes::DENIED`]; any other request is
/// [`codes::INVALID`].
#[cfg(unix)]
pub async fn serve_forge_helpers(
    listener: tokio::net::UnixListener,
    sessions: NodeGitSessions,
    forge: AgentRemote,
) {
    let forge = Arc::new(forge);
    while let Ok((stream, _)) = listener.accept().await {
        let sessions = sessions.clone();
        let forge = forge.clone();
        tokio::spawn(async move {
            let _ = serve_helper(stream, &sessions, None, Some(&forge)).await;
        });
    }
}

#[cfg(unix)]
async fn serve_helper(
    stream: tokio::net::UnixStream,
    sessions: &NodeGitSessions,
    credential: Option<&HelperCredential>,
    only: Option<&AgentRemote>,
) -> Result<()> {
    use tokio::io::AsyncBufReadExt as _;

    let (read, mut write) = stream.into_split();
    let mut read = tokio::io::BufReader::new(read);
    let mut line = Vec::new();
    (&mut read)
        .take(super::git_helper::MAX_REQUEST)
        .read_until(b'\n', &mut line)
        .await
        .map_err(|_| session_error(codes::INVALID))?;
    let request = std::str::from_utf8(&line)
        .ok()
        .and_then(|line| line.strip_suffix('\n'))
        .and_then(|line| line.split_once(' '));
    if let (Some((CREDENTIAL_REQUEST, host)), None) = (request, only) {
        let token = credential.and_then(|credential| credential(host));
        let reply = match &token {
            Some(token) => zeroize::Zeroizing::new(format!("\x00{}\n", token.expose())),
            None => zeroize::Zeroizing::new(format!("\x01{}\n", codes::UNAVAILABLE)),
        };
        let _ = write.write_all(reply.as_bytes()).await;
        let _ = write.shutdown().await;
        return Ok(());
    }
    let opened = match request {
        Some((service, url)) => match (GitService::parse(service), AgentRemote::parse(url)) {
            (Ok(_), Ok(remote)) if only.is_some() && !remote.is_forge() => {
                Err(session_error(codes::INVALID))
            }
            (Ok(_), Ok(remote)) if only.is_some_and(|only| *only != remote) => {
                Err(session_error(codes::DENIED))
            }
            (Ok(service), Ok(remote)) => sessions.open(service, &remote).await,
            _ => Err(session_error(codes::INVALID)),
        },
        None => Err(session_error(codes::INVALID)),
    };
    let mut session = match opened {
        Ok(session) => session,
        Err(error) => {
            let code = code_of(&error);
            let _ = write.write_all(format!("\x01{code}\n").as_bytes()).await;
            return Err(error);
        }
    };
    let status = match session.warning() {
        Some(warning) => format!("\x02{warning}\n").into_bytes(),
        None => vec![0],
    };
    write
        .write_all(&status)
        .await
        .map_err(|_| session_error(codes::UNAVAILABLE))?;
    let mut buffer = vec![0; CHUNK];
    let mut input_open = true;
    loop {
        tokio::select! {
            count = read.read(&mut buffer), if input_open => {
                match count {
                    Ok(0) | Err(_) => {
                        input_open = false;
                        session.finish().await?;
                    }
                    Ok(count) => session.send(&buffer[..count]).await?,
                }
            }
            output = session.recv() => {
                let Some(bytes) = output? else {
                    let _ = write.shutdown().await;
                    return Ok(());
                };
                write
                    .write_all(&bytes)
                    .await
                    .map_err(|_| session_error(codes::UNAVAILABLE))?;
            }
        }
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod access_code_tests {
    use super::*;

    #[test]
    fn an_unavailable_repository_is_not_reported_as_denied() {
        let unavailable = intent_core::Error::Internal(format!(
            "{}: the workspace's repository is gone",
            codes::UNAVAILABLE
        ));
        assert_eq!(access_code(&unavailable), codes::UNAVAILABLE);
        assert_eq!(
            access_code(&intent_core::Error::Forbidden("node-git-denied".into())),
            codes::DENIED
        );
        assert_eq!(
            access_code(&intent_core::Error::Internal("boom".into())),
            codes::DENIED
        );
    }
}
