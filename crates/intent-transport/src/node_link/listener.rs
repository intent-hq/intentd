//! Node side: the TLS listener that admits head's link and git
//! connections.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use futures_util::{SinkExt as _, StreamExt as _};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch, Semaphore};
use tokio_rustls::TlsAcceptor;
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::{http, Message};
use tokio_tungstenite::WebSocketStream;

use intent_core::node_link::{
    BindDecision, LeaseBinding, LinkFrame, LinkHello, LinkWelcome, NODE_LINK_PROTOCOLS,
};
use intent_core::nodes::NodeCounter;
use intent_core::{Error, Result};

use super::dial::{
    socket_config, HANDSHAKE_TIMEOUT, HEADER_GENERATION, HEADER_HEAD, HEADER_LEASE, HEADER_NODE,
};
use super::link::Tuning;
use super::{
    codes, error_code, pin, BindingFile, GitConnection, LeaseToken, LinkEvents, NodeLink, GIT_PATH,
    LINK_PATH,
};

/// Builds the node's welcome for an accepted hello: its platform, capacity
/// and running agents. The listener fills in `protocol` and `nodeId`.
pub type WelcomeFn = Arc<dyn Fn(&LinkHello) -> LinkWelcome + Send + Sync>;

/// What the node listener needs.
#[derive(Debug, Clone)]
pub struct NodeListenerConfig {
    pub node_id: String,
    pub token: LeaseToken,
    pub binding_file: BindingFile,
    pub certificate: crate::TlsCertificate,
}

/// An admitted connection.
#[derive(Debug)]
pub enum NodeConnection {
    /// Head bound with a newer generation. Any older link has been closed.
    Link {
        link: NodeLink,
        events: LinkEvents,
        hello: LinkHello,
    },
    /// A git connection for the current binding.
    Git(GitConnection),
}

/// Concurrent handshakes in flight.
const MAX_HANDSHAKES: usize = 16;

struct Current {
    binding: Option<LeaseBinding>,
    link: Option<NodeLink>,
    /// Flips to `true` when this binding is replaced, closing its git
    /// connections.
    retire: watch::Sender<bool>,
}

struct Shared {
    node_id: String,
    token: LeaseToken,
    file: BindingFile,
    welcome: WelcomeFn,
    tuning: Tuning,
    current: Mutex<Current>,
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// The node's link listener. Dropping it stops accepting; established
/// connections live until closed.
pub struct NodeListener {
    local_addr: SocketAddr,
    incoming: mpsc::Receiver<NodeConnection>,
    shared: Arc<Shared>,
    task: tokio::task::JoinHandle<()>,
}

impl NodeListener {
    /// Load the persisted binding and start listening on `address`.
    ///
    /// # Errors
    ///
    /// `node-link-binding-store` for an unreadable binding file,
    /// `node-link-config` for bad TLS material, or the bind failure.
    pub async fn bind(
        address: SocketAddr,
        config: NodeListenerConfig,
        welcome: WelcomeFn,
    ) -> Result<Self> {
        Self::bind_with(address, config, welcome, Tuning::default()).await
    }

    pub(super) async fn bind_with(
        address: SocketAddr,
        config: NodeListenerConfig,
        welcome: WelcomeFn,
        tuning: Tuning,
    ) -> Result<Self> {
        let binding = config.binding_file.load()?;
        let tls = TlsAcceptor::from(Arc::new(pin::server_config(&config.certificate)?));
        let tcp = TcpListener::bind(address)
            .await
            .map_err(|e| Error::Internal(format!("node link listener bind: {e}")))?;
        let local_addr = tcp
            .local_addr()
            .map_err(|e| Error::Internal(format!("node link listener address: {e}")))?;
        let shared = Arc::new(Shared {
            node_id: config.node_id,
            token: config.token,
            file: config.binding_file,
            welcome,
            tuning,
            current: Mutex::new(Current {
                binding,
                link: None,
                retire: watch::channel(false).0,
            }),
        });
        let (tx, incoming) = mpsc::channel(MAX_HANDSHAKES);
        let task = tokio::spawn(accept_loop(tcp, tls, shared.clone(), tx));
        Ok(Self {
            local_addr,
            incoming,
            shared,
            task,
        })
    }

    #[must_use]
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// The node's current binding.
    #[must_use]
    pub fn binding(&self) -> Option<LeaseBinding> {
        lock(&self.shared.current).binding.clone()
    }

    /// The next admitted connection. Failed handshakes are logged by code
    /// and never surface here.
    pub async fn accept(&mut self) -> Option<NodeConnection> {
        self.incoming.recv().await
    }
}

impl Drop for NodeListener {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn accept_loop(
    tcp: TcpListener,
    tls: TlsAcceptor,
    shared: Arc<Shared>,
    tx: mpsc::Sender<NodeConnection>,
) {
    let permits = Arc::new(Semaphore::new(MAX_HANDSHAKES));
    loop {
        let Ok(permit) = permits.clone().acquire_owned().await else {
            return;
        };
        let Ok((stream, _)) = tcp.accept().await else {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            continue;
        };
        let tls = tls.clone();
        let shared = shared.clone();
        let tx = tx.clone();
        tokio::spawn(async move {
            let _permit = permit;
            match tokio::time::timeout(HANDSHAKE_TIMEOUT, admit(stream, tls, &shared)).await {
                Ok(Ok(connection)) => {
                    let _ = tx.send(connection).await;
                }
                Ok(Err(error)) => {
                    tracing::info!(
                        code = error_code(&error).unwrap_or(codes::PROTOCOL),
                        "node link: connection refused"
                    );
                }
                Err(_) => {
                    tracing::info!(code = codes::TIMEOUT, "node link: handshake timed out");
                }
            }
        });
    }
}

/// Which connection the upgrade request asked for.
enum Route {
    Link,
    Git {
        binding: LeaseBinding,
        retire: watch::Receiver<bool>,
    },
}

#[expect(
    clippy::result_large_err,
    reason = "tungstenite's upgrade callback returns the HTTP error response by value"
)]
fn check_upgrade(
    shared: &Shared,
    request: &Request,
) -> std::result::Result<Route, http::Response<Option<String>>> {
    let refuse = |status: http::StatusCode, code: &str| {
        let mut response = http::Response::new(Some(code.to_owned()));
        *response.status_mut() = status;
        response
    };
    let presented = request
        .headers()
        .get(http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    if request.headers().contains_key(http::header::ORIGIN)
        || !presented.is_some_and(|token| shared.token.matches(token))
    {
        return Err(refuse(http::StatusCode::UNAUTHORIZED, codes::UNAUTHORIZED));
    }
    match request.uri().path() {
        LINK_PATH if request.uri().query().is_none() => Ok(Route::Link),
        GIT_PATH if request.uri().query().is_none() => {
            let header = |name: &str| {
                request
                    .headers()
                    .get(name)
                    .and_then(|v| v.to_str().ok())
                    .map(str::to_owned)
            };
            let offered = (|| {
                Some(LeaseBinding {
                    head_id: header(HEADER_HEAD)?,
                    node_id: header(HEADER_NODE)?,
                    lease_id: header(HEADER_LEASE)?,
                    generation: NodeCounter(header(HEADER_GENERATION)?.parse().ok()?),
                })
            })();
            let current = lock(&shared.current);
            match offered {
                Some(offered) if current.binding.as_ref() == Some(&offered) => Ok(Route::Git {
                    binding: offered,
                    retire: current.retire.subscribe(),
                }),
                _ => Err(refuse(http::StatusCode::CONFLICT, codes::STALE)),
            }
        }
        _ => Err(refuse(http::StatusCode::NOT_FOUND, codes::PROTOCOL)),
    }
}

async fn admit(
    stream: TcpStream,
    tls: TlsAcceptor,
    shared: &Arc<Shared>,
) -> Result<NodeConnection> {
    let _ = stream.set_nodelay(true);
    let stream = tls.accept(stream).await.map_err(|_| super::unavailable())?;
    let mut route = None;
    #[expect(
        clippy::result_large_err,
        reason = "tungstenite's upgrade callback returns the HTTP error response by value"
    )]
    let mut socket = tokio_tungstenite::accept_hdr_async_with_config(
        stream,
        |request: &Request, response: Response| {
            let checked = check_upgrade(shared, request)?;
            route = Some(checked);
            Ok(response)
        },
        Some(socket_config()),
    )
    .await
    .map_err(|_| Error::Forbidden(format!("{}: upgrade refused", codes::UNAUTHORIZED)))?;
    match route.ok_or_else(|| super::protocol("upgrade without a route"))? {
        Route::Git { binding, retire } => Ok(NodeConnection::Git(GitConnection::start(
            socket,
            binding,
            Some(retire),
        ))),
        Route::Link => bind(&mut socket, shared).await.map(|hello| {
            let (link, events) = NodeLink::start(socket, hello.binding.clone(), shared.tuning);
            let mut current = lock(&shared.current);
            if current.binding.as_ref() == Some(&hello.binding) {
                current.link = Some(link.clone());
            } else {
                // A newer bind won the race while this one finished.
                link.close();
            }
            drop(current);
            NodeConnection::Link {
                link,
                events,
                hello,
            }
        }),
    }
}

/// Read head's hello, decide, persist an accepted binding, close the older
/// connection and send the welcome.
async fn bind<S>(socket: &mut WebSocketStream<S>, shared: &Shared) -> Result<LinkHello>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let hello = loop {
        match socket.next().await {
            // Decode errors quote the peer's bytes; report a fixed code.
            Some(Ok(Message::Binary(bytes))) => match LinkFrame::decode(&bytes) {
                Ok(LinkFrame::Hello(hello)) => break hello,
                _ => return Err(super::protocol("expected hello")),
            },
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
    let accepted = {
        let mut current = lock(&shared.current);
        match LeaseBinding::decide(&shared.node_id, current.binding.as_ref(), &hello.binding) {
            BindDecision::Accept => match shared.file.store(&hello.binding) {
                Ok(()) => {
                    current.binding = Some(hello.binding.clone());
                    current.retire.send_replace(true);
                    current.retire = watch::channel(false).0;
                    Ok(current.link.take())
                }
                Err(error) => Err((error, codes::BINDING_STORE.to_owned())),
            },
            BindDecision::WrongNode => Err((
                Error::InvalidParams(codes::WRONG_NODE.into()),
                codes::WRONG_NODE.to_owned(),
            )),
            BindDecision::OtherHead => Err((
                Error::InvalidParams(codes::OTHER_HEAD.into()),
                codes::OTHER_HEAD.to_owned(),
            )),
            BindDecision::Stale { current } => Err((
                Error::InvalidParams(codes::STALE.into()),
                format!("{}: {}", codes::STALE, current.0),
            )),
        }
    };
    let older = match accepted {
        Ok(older) => older,
        Err((error, reason)) => {
            reject(socket, &reason).await;
            return Err(error);
        }
    };
    if let Some(older) = older {
        older.close();
    }
    let mut welcome = (shared.welcome)(&hello);
    welcome.protocol = protocol;
    welcome.node_id.clone_from(&shared.node_id);
    let bytes = LinkFrame::Welcome(welcome).encode()?;
    socket
        .send(Message::Binary(bytes.into()))
        .await
        .map_err(|_| super::unavailable())?;
    Ok(hello)
}

async fn reject<S>(socket: &mut WebSocketStream<S>, reason: &str)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let _ = socket
        .send(Message::Close(Some(CloseFrame {
            code: CloseCode::Policy,
            reason: reason.to_owned().into(),
        })))
        .await;
    let _ = socket.flush().await;
}
