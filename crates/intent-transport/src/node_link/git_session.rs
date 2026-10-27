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
    /// The repository holding `remote`'s refs, only if `binding` is the
    /// node's current binding and `remote`'s agent runs on its lease.
    fn repository<'a>(
        &'a self,
        binding: &'a LeaseBinding,
        remote: &'a AgentRemote,
    ) -> BoxFuture<'a, Result<PathBuf>>;
}

struct HeadSession {
    stdin: Option<mpsc::Sender<Vec<u8>>>,
    task: tokio::task::AbortHandle,
}

/// Head side: answer the node's git sessions on `connection` until it
/// closes. A protocol violation closes the connection; dropping the future
/// stops every running service.
pub async fn serve_git(mut connection: GitConnection, access: Arc<dyn NodeGitAccess>) {
    let sender = connection.sender();
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
                if sessions.len() >= MAX_SESSIONS {
                    let _ = sender.send(close_message(id, Some(codes::BUSY))).await;
                    continue;
                }
                let (stdin, stdin_rx) = mpsc::channel(SESSION_QUEUE);
                let task = tasks.spawn(head_session(
                    id,
                    payload,
                    binding.clone(),
                    access.clone(),
                    sender.clone(),
                    stdin_rx,
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
) -> u32 {
    let result = run_service(id, &open, &binding, access.as_ref(), &sender, stdin_rx).await;
    let _ = sender.send(close_message(id, result.err())).await;
    id
}

async fn run_service(
    id: u32,
    open: &[u8],
    binding: &LeaseBinding,
    access: &dyn NodeGitAccess,
    sender: &GitSender,
    mut stdin_rx: mpsc::Receiver<Vec<u8>>,
) -> std::result::Result<(), &'static str> {
    let open: OpenRequest = serde_json::from_slice(open).map_err(|_| codes::INVALID)?;
    let service = GitService::parse(&open.service).map_err(|_| codes::INVALID)?;
    let remote = AgentRemote::parse(&open.url).map_err(|_| codes::INVALID)?;
    let repository = access
        .repository(binding, &remote)
        .await
        .map_err(|_| codes::DENIED)?;
    let mut child = service_command(service, &repository, &remote)
        .spawn()
        .map_err(|_| codes::FAILED)?;
    let (Some(mut stdin), Some(mut stdout)) = (child.stdin.take(), child.stdout.take()) else {
        return Err(codes::FAILED);
    };
    sender
        .send(encode(ACCEPT, id, &[]))
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
    let status = child.wait().await.map_err(|_| codes::FAILED)?;
    if status.success() {
        Ok(())
    } else {
        Err(codes::FAILED)
    }
}

/// An event for one node-side session.
#[derive(Debug)]
enum SessionEvent {
    Accepted,
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
#[derive(Clone, Default)]
pub struct NodeGitSessions {
    state: Arc<Mutex<NodeState>>,
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
        // The previous connection's head sessions end when it closes, and
        // its reader stops even if it was waiting on a full session queue.
        if let Some((_, previous)) = previous {
            previous.close();
        }
        let state = self.state.clone();
        tokio::spawn(async move {
            while let Some((kind, id, payload)) = connection.recv().await.and_then(decode) {
                let event = match kind {
                    ACCEPT => SessionEvent::Accepted,
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
    /// A [`codes`] error: unavailable without a connection, or head's
    /// refusal.
    pub async fn open(&self, service: GitService, remote: &AgentRemote) -> Result<GitSession> {
        let (events_tx, mut events) = mpsc::channel(SESSION_QUEUE);
        let (id, epoch, sender) = {
            let mut state = lock(&self.state);
            let Some((epoch, sender)) = state.connection.clone() else {
                return Err(session_error(codes::UNAVAILABLE));
            };
            let id = state.next_id;
            state.next_id = state.next_id.wrapping_add(1);
            state.sessions.insert(id, events_tx);
            (id, epoch, sender)
        };
        let mut session = GitSession {
            id,
            epoch,
            sender,
            events: None,
            state: self.state.clone(),
            closed: false,
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
            Some(SessionEvent::Accepted) => {
                session.events = Some(events);
                Ok(session)
            }
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
}

impl GitSession {
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
            Some(SessionEvent::Accepted) | None => {
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
/// then carries the session's bytes both ways.
#[cfg(unix)]
pub async fn serve_helpers(listener: tokio::net::UnixListener, sessions: NodeGitSessions) {
    while let Ok((stream, _)) = listener.accept().await {
        let sessions = sessions.clone();
        tokio::spawn(async move {
            let _ = serve_helper(stream, &sessions).await;
        });
    }
}

#[cfg(unix)]
async fn serve_helper(stream: tokio::net::UnixStream, sessions: &NodeGitSessions) -> Result<()> {
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
    let opened = match request {
        Some((service, url)) => match (GitService::parse(service), AgentRemote::parse(url)) {
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
    write
        .write_all(&[0])
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
