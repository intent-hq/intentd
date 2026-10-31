//! Machine side of the daemon-to-daemon executor link, served on the
//! daemon's own WSS listener at [`LINK_PATH`] and [`GIT_PATH`].
//!
//! The listener's upgrade gate admits only executor credentials there (see
//! `ws.rs`); this module then runs the bind exchange, keeps one binding per
//! credential (persisted through [`WorkspaceApi::bind_executor`]), refuses
//! git connections for any binding but the credential's current one, and
//! closes a revoked credential's connections with `1008`.

use std::collections::HashMap;
use std::sync::Arc;

use futures_util::{SinkExt as _, StreamExt as _};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{broadcast, watch};
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

use intent_core::machines::ExecutorCredential;
use intent_core::node_link::{
    BindDecision, LeaseBinding, LinkFrame, LinkHello, LinkWelcome, NODE_LINK_PROTOCOLS,
};
use intent_core::nodes::NodeCounter;
use intent_core::{Error, ExecutorCredentialId, Result, WorkspaceApi};

use super::dial::{HANDSHAKE_TIMEOUT, HEADER_GENERATION, HEADER_HEAD, HEADER_LEASE, HEADER_NODE};
use super::git::GitCloser;
use super::link::Tuning;
use super::{codes, GitConnection, NodeConnection, NodeLink};

#[cfg(doc)]
use super::{GIT_PATH, LINK_PATH};

/// WebSocket close code for a revoked credential (policy violation).
pub const REVOKED_CLOSE_CODE: u16 = 1008;
/// Close reason sent with [`REVOKED_CLOSE_CODE`].
pub const REVOKED_CLOSE_REASON: &str = "credential revoked";
/// Close code when the listener stops.
const GOING_AWAY: u16 = 1001;

/// What the daemon's executor service provides to the listener. Until one
/// is installed the listener answers `/node/*` with `404`.
pub trait ExecutorHost: Send + Sync {
    /// `remoteAgents.enabled`: while `false`, `/node/*` upgrades get `403`.
    fn enabled(&self) -> bool;
    /// This machine's daemon id; a bind must name it as `nodeId`.
    fn node_id(&self) -> String;
    /// The welcome for an accepted hello: capabilities, routes and this
    /// binding's running agents. The listener sets `protocol` and `nodeId`.
    fn welcome(&self, credential: &ExecutorCredential, hello: &LinkHello) -> LinkWelcome;
    /// Take an admitted connection. Called with the link server's lock
    /// held, so it must not block or call back into [`ExecutorLinks`].
    fn accept(&self, credential: ExecutorCredential, connection: NodeConnection);
}

/// The live state of one credential's binding.
struct Current {
    binding: LeaseBinding,
    link: Option<NodeLink>,
    /// Flips to `true` when the binding is replaced, closing its git
    /// connections.
    retire: watch::Sender<bool>,
    gits: Vec<GitCloser>,
}

impl Current {
    fn close_with(&mut self, code: u16, reason: &'static str) {
        if let Some(link) = self.link.take() {
            link.close_with(code, reason);
        }
        for git in self.gits.drain(..) {
            git.close_with(code, reason);
        }
        self.retire.send_replace(true);
    }
}

/// The executor link server shared by the daemon's listeners.
pub struct ExecutorLinks {
    host: Arc<dyn ExecutorHost>,
    tuning: Tuning,
    /// Held across the bind decision so the persisted and in-memory
    /// bindings change together.
    current: tokio::sync::Mutex<HashMap<ExecutorCredentialId, Current>>,
    /// Advanced (under the `current` lock) whenever the listener stops. A
    /// connection admitted in an older epoch can no longer bind, register
    /// or reach the host, so nothing outlives `stop()`.
    epoch: watch::Sender<u64>,
}

impl std::fmt::Debug for ExecutorLinks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExecutorLinks").finish_non_exhaustive()
    }
}

/// The connection an admitted `/node/*` upgrade asked for.
#[derive(Debug)]
pub(crate) enum ExecutorRoute {
    Link,
    Git {
        binding: LeaseBinding,
        retire: watch::Receiver<bool>,
    },
}

impl ExecutorLinks {
    #[must_use]
    pub fn new(host: Arc<dyn ExecutorHost>) -> Arc<Self> {
        Self::with_tuning(host, Tuning::default())
    }

    pub(super) fn with_tuning(host: Arc<dyn ExecutorHost>, tuning: Tuning) -> Arc<Self> {
        Arc::new(Self {
            host,
            tuning,
            current: tokio::sync::Mutex::new(HashMap::new()),
            epoch: watch::channel(0).0,
        })
    }

    /// Whether the machine serves executor links (`remoteAgents.enabled`).
    #[must_use]
    pub fn enabled(&self) -> bool {
        self.host.enabled()
    }

    /// Admit a git upgrade only for the credential's current binding, named
    /// by the binding headers. `None` refuses it (`409`).
    pub(crate) async fn git_route(
        &self,
        credential: &ExecutorCredential,
        header: impl Fn(&str) -> Option<String>,
    ) -> Option<ExecutorRoute> {
        let offered = LeaseBinding {
            head_id: header(HEADER_HEAD)?,
            node_id: header(HEADER_NODE)?,
            lease_id: header(HEADER_LEASE)?,
            generation: NodeCounter(header(HEADER_GENERATION)?.parse().ok()?),
        };
        let current = self.current.lock().await;
        let entry = current.get(&credential.id)?;
        (entry.binding == offered).then(|| ExecutorRoute::Git {
            binding: offered,
            retire: entry.retire.subscribe(),
        })
    }

    /// Close every connection of `credential_id` with `1008` and forget its
    /// live binding. The persisted binding stays, so the revoked backend can
    /// never bind with an older generation again.
    pub async fn revoke(&self, credential_id: &ExecutorCredentialId) {
        self.drop_credential(credential_id, REVOKED_CLOSE_CODE, REVOKED_CLOSE_REASON)
            .await;
    }

    async fn drop_credential(
        &self,
        credential_id: &ExecutorCredentialId,
        code: u16,
        reason: &'static str,
    ) {
        if let Some(mut entry) = self.current.lock().await.remove(credential_id) {
            entry.close_with(code, reason);
        }
    }

    /// The current listener epoch; an upgrade records it before its `101`.
    pub(crate) fn epoch(&self) -> u64 {
        *self.epoch.borrow()
    }

    /// Close every executor connection (the listener is stopping) and fence
    /// out connections upgraded before now that have not bound yet.
    pub async fn close_all(&self) {
        let mut current = self.current.lock().await;
        self.epoch
            .send_modify(|epoch| *epoch = epoch.wrapping_add(1));
        for (_, mut entry) in current.drain() {
            entry.close_with(GOING_AWAY, "server shutting down");
        }
    }

    /// Serve an upgraded `/node/*` connection admitted in listener `epoch`.
    /// `revocations` must have been subscribed before the credential was
    /// resolved, so a revocation racing the upgrade is not missed.
    pub(crate) async fn serve<S>(
        self: Arc<Self>,
        api: Arc<dyn WorkspaceApi>,
        credential: ExecutorCredential,
        route: ExecutorRoute,
        mut socket: WebSocketStream<S>,
        revocations: Option<broadcast::Receiver<ExecutorCredentialId>>,
        epoch: u64,
    ) where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        match route {
            ExecutorRoute::Git { binding, retire } => {
                let git = GitConnection::start(socket, binding.clone(), Some(retire));
                let closer = git.closer();
                let id = credential.id.clone();
                {
                    // The hand-off happens under the lock, so a stop or
                    // revocation either sees this connection or fences it.
                    let mut current = self.current.lock().await;
                    match current.get_mut(&credential.id) {
                        Some(entry) if entry.binding == binding && self.epoch() == epoch => {
                            entry.gits.retain(|g| !g.is_closed());
                            entry.gits.push(closer.clone());
                            self.host.accept(credential, NodeConnection::Git(git));
                        }
                        // Replaced, revoked or stopped since the upgrade
                        // check.
                        _ => {
                            git.close();
                            return;
                        }
                    }
                }
                watch_revocation(self, id, revocations, async move { closer.closed().await });
            }
            ExecutorRoute::Link => {
                let mut stopped = self.epoch.subscribe();
                let bound = tokio::select! {
                    bound = tokio::time::timeout(
                        HANDSHAKE_TIMEOUT,
                        self.bind(api.as_ref(), &credential, &mut socket, epoch),
                    ) => bound,
                    // The listener stopped while the backend had not bound
                    // yet: drop the connection instead of binding later.
                    _ = stopped.wait_for(|current| *current != epoch) => return,
                };
                let hello = match bound {
                    Ok(Ok(hello)) => hello,
                    Ok(Err(error)) => {
                        tracing::info!(
                            code = super::error_code(&error).unwrap_or(codes::PROTOCOL),
                            "executor link: bind refused"
                        );
                        return;
                    }
                    Err(_) => {
                        tracing::info!(code = codes::TIMEOUT, "executor link: bind timed out");
                        return;
                    }
                };
                let (link, events) =
                    NodeLink::start_with(socket, hello.binding.clone(), self.tuning, true);
                let id = credential.id.clone();
                let watched = link.clone();
                {
                    // The hand-off happens under the lock, so a stop or
                    // revocation either sees this link or fences it.
                    let mut current = self.current.lock().await;
                    match current.get_mut(&credential.id) {
                        Some(entry) if entry.binding == hello.binding && self.epoch() == epoch => {
                            entry.link = Some(link.clone());
                            self.host.accept(
                                credential,
                                NodeConnection::Link {
                                    link,
                                    events,
                                    hello,
                                },
                            );
                        }
                        // A newer bind, a revocation or a stop won while
                        // this one finished.
                        _ => {
                            link.close();
                            return;
                        }
                    }
                }
                watch_revocation(self, id, revocations, async move { watched.closed().await });
            }
        }
    }

    /// Read the backend's hello, decide and persist the bind, close the
    /// credential's older connections and send the welcome.
    async fn bind<S>(
        &self,
        api: &dyn WorkspaceApi,
        credential: &ExecutorCredential,
        socket: &mut WebSocketStream<S>,
        epoch: u64,
    ) -> Result<LinkHello>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let hello = loop {
            match socket.next().await {
                // Decode errors quote the peer's bytes; report a fixed code.
                Some(Ok(Message::Binary(bytes))) => {
                    if let Ok(LinkFrame::Hello(hello)) = LinkFrame::decode(&bytes) {
                        break hello;
                    }
                    reject(socket, codes::PROTOCOL).await;
                    return Err(super::protocol("expected hello"));
                }
                Some(Ok(Message::Ping(_) | Message::Pong(_))) => {}
                _ => return Err(super::unavailable()),
            }
        };
        let Some(protocol) = NODE_LINK_PROTOCOLS.highest_common(hello.protocol) else {
            reject(
                socket,
                &format!("{}: {}", codes::VERSION_MISMATCH, NODE_LINK_PROTOCOLS.max),
            )
            .await;
            return Err(Error::InvalidParams(codes::VERSION_MISMATCH.into()));
        };
        let node_id = self.host.node_id();
        {
            let mut current = self.current.lock().await;
            if self.epoch() != epoch {
                drop(current);
                reject(socket, codes::UNAVAILABLE).await;
                return Err(super::unavailable());
            }
            let decision = api
                .bind_executor(
                    credential.id.clone(),
                    node_id.clone(),
                    hello.binding.clone(),
                )
                .await;
            let refusal = match decision {
                Ok(BindDecision::Accept) => None,
                Ok(BindDecision::WrongNode) => Some(codes::WRONG_NODE.to_owned()),
                Ok(BindDecision::OtherHead) => Some(codes::OTHER_HEAD.to_owned()),
                Ok(BindDecision::Stale { current }) => {
                    Some(format!("{}: {}", codes::STALE, current.0))
                }
                // Revoked since the upgrade gate.
                Err(Error::NotFound(_)) => Some(codes::UNAUTHORIZED.to_owned()),
                Err(_) => Some(codes::BINDING_STORE.to_owned()),
            };
            if let Some(reason) = refusal {
                drop(current);
                reject(socket, &reason).await;
                let code = reason.split(':').next().unwrap_or(codes::PROTOCOL);
                return Err(Error::InvalidParams(code.to_owned()));
            }
            if let Some(mut older) = current.insert(
                credential.id.clone(),
                Current {
                    binding: hello.binding.clone(),
                    link: None,
                    retire: watch::channel(false).0,
                    gits: Vec::new(),
                },
            ) {
                older.close_with(GOING_AWAY, "binding replaced");
            }
        }
        let mut welcome = self.host.welcome(credential, &hello);
        welcome.protocol = protocol;
        welcome.node_id = node_id;
        let bytes = LinkFrame::Welcome(welcome).encode()?;
        socket
            .send(Message::Binary(bytes.into()))
            .await
            .map_err(|_| super::unavailable())?;
        Ok(hello)
    }
}

/// Close `id`'s connections when it is revoked, until `closed` resolves. A
/// lagged or closed feed fails closed.
fn watch_revocation(
    links: Arc<ExecutorLinks>,
    id: ExecutorCredentialId,
    revocations: Option<broadcast::Receiver<ExecutorCredentialId>>,
    closed: impl std::future::Future<Output = ()> + Send + 'static,
) {
    let Some(mut revocations) = revocations else {
        return;
    };
    tokio::spawn(async move {
        tokio::pin!(closed);
        loop {
            tokio::select! {
                () = &mut closed => return,
                revoked = revocations.recv() => match revoked {
                    Ok(revoked) if revoked != id => {}
                    Ok(_) => {
                        links.revoke(&id).await;
                        return;
                    }
                    // A missed revocation must not leave the connection
                    // open; a still-valid backend simply redials.
                    Err(_) => {
                        links
                            .drop_credential(&id, GOING_AWAY, "revocation feed interrupted")
                            .await;
                        return;
                    }
                },
            }
        }
    });
}

async fn reject<S>(socket: &mut WebSocketStream<S>, reason: &str)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let _ = socket
        .send(Message::Close(Some(CloseFrame {
            code: CloseCode::Policy,
            reason: reason.to_owned().into(),
        })))
        .await;
    let _ = socket.flush().await;
}
