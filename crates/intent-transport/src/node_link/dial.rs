//! Head side: dial a configured node.

use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt as _, StreamExt as _};
use tokio::net::TcpStream;
use tokio_rustls::client::TlsStream;
use tokio_rustls::TlsConnector;
use tokio_tungstenite::tungstenite::client::IntoClientRequest as _;
use tokio_tungstenite::tungstenite::{self, http, Message};
use tokio_tungstenite::WebSocketStream;

use intent_core::node_link::{
    ensure_protocol, LeaseBinding, LinkFrame, LinkHello, LinkWelcome, NODE_LINK_PROTOCOLS,
};
use intent_core::{Error, Result};

use super::link::Tuning;
use super::pin::{self, CertificatePin, PIN_MISMATCH};
use super::{codes, ExecutorToken, GitConnection, LinkEvents, NodeLink, GIT_PATH, LINK_PATH};

/// Bound on connecting, the TLS and WebSocket handshakes and the bind
/// exchange.
pub(super) const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// Headers naming the binding a git connection belongs to.
pub(super) const HEADER_HEAD: &str = "x-intent-head-id";
pub(super) const HEADER_NODE: &str = "x-intent-node-id";
pub(super) const HEADER_LEASE: &str = "x-intent-lease-id";
pub(super) const HEADER_GENERATION: &str = "x-intent-generation";

/// How head reaches one node: its `wss://host:port` base URL, the pinned
/// certificate and the lease token. `Debug` never shows the token.
///
/// With `tailcat` set the TCP hop is replaced by a tailcat pipe client
/// (`tailcat <address> <port>`, the port taken from `url`); the TLS pin and
/// the token work exactly as on a direct route.
#[derive(Debug, Clone)]
pub struct NodeEndpoint {
    pub url: String,
    pub pin: CertificatePin,
    pub token: ExecutorToken,
    pub tailcat: Option<TailcatRoute>,
}

/// A tailcat route to a machine: the sitter-shipped tailcat binary run in
/// pipe-client mode, one child per connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TailcatRoute {
    /// The tailcat binary.
    pub bin: std::path::PathBuf,
    /// The machine's `tc...` address.
    pub address: String,
}

impl NodeEndpoint {
    /// A direct route to `wss://host:port`.
    #[must_use]
    pub fn direct(url: impl Into<String>, pin: CertificatePin, token: ExecutorToken) -> Self {
        Self {
            url: url.into(),
            pin,
            token,
            tailcat: None,
        }
    }

    /// A tailcat route to the machine's listener `port` at `address`.
    #[must_use]
    pub fn via_tailcat(
        route: TailcatRoute,
        port: u16,
        pin: CertificatePin,
        token: ExecutorToken,
    ) -> Self {
        Self {
            url: format!("wss://localhost:{port}"),
            pin,
            token,
            tailcat: Some(route),
        }
    }
}

/// A link head dialed, with the node's welcome.
#[derive(Debug)]
pub struct DialedLink {
    pub link: NodeLink,
    pub events: LinkEvents,
    pub welcome: LinkWelcome,
}

type Socket = WebSocketStream<TlsStream<DialStream>>;

/// Dial the control/journal/RPC connection and bind with `hello`. Head must
/// have durably advanced `hello.binding.generation` before calling, and
/// uses a new generation for every dial.
///
/// # Errors
///
/// A link error whose code says why: unreachable, certificate mismatch,
/// unauthorized, stale generation, other head, wrong node or version
/// mismatch.
pub async fn dial_link(endpoint: &NodeEndpoint, hello: LinkHello) -> Result<DialedLink> {
    dial_link_with(endpoint, hello, Tuning::default()).await
}

pub(super) async fn dial_link_with(
    endpoint: &NodeEndpoint,
    hello: LinkHello,
    tuning: Tuning,
) -> Result<DialedLink> {
    let binding = hello.binding.clone();
    let (socket, welcome) = tokio::time::timeout(HANDSHAKE_TIMEOUT, async {
        let mut socket = connect(endpoint, LINK_PATH, &[]).await?;
        let bytes = LinkFrame::Hello(hello).encode()?;
        socket
            .send(Message::Binary(bytes.into()))
            .await
            .map_err(|_| super::unavailable())?;
        let welcome = read_welcome(&mut socket, &binding).await?;
        Ok::<_, Error>((socket, welcome))
    })
    .await
    .map_err(|_| super::unavailable())??;
    let (link, events) = NodeLink::start(socket, binding, tuning);
    Ok(DialedLink {
        link,
        events,
        welcome,
    })
}

/// Dial the git connection for `binding`, which must be the node's current
/// binding.
///
/// # Errors
///
/// As [`dial_link`]; `node-link-stale` when the binding is not current.
pub async fn dial_git(endpoint: &NodeEndpoint, binding: &LeaseBinding) -> Result<GitConnection> {
    let generation = binding.generation.0.to_string();
    let headers = [
        (HEADER_HEAD, binding.head_id.as_str()),
        (HEADER_NODE, binding.node_id.as_str()),
        (HEADER_LEASE, binding.lease_id.as_str()),
        (HEADER_GENERATION, generation.as_str()),
    ];
    let socket = tokio::time::timeout(HANDSHAKE_TIMEOUT, connect(endpoint, GIT_PATH, &headers))
        .await
        .map_err(|_| super::unavailable())??;
    Ok(GitConnection::start(socket, binding.clone(), None))
}

pub(super) async fn read_welcome<S>(
    socket: &mut WebSocketStream<S>,
    binding: &LeaseBinding,
) -> Result<LinkWelcome>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    loop {
        match socket.next().await {
            Some(Ok(Message::Binary(bytes))) => {
                let Ok(LinkFrame::Welcome(welcome)) = LinkFrame::decode(&bytes) else {
                    return Err(super::protocol("expected welcome"));
                };
                ensure_protocol(welcome.protocol)?;
                if welcome.node_id != binding.node_id {
                    return Err(Error::InvalidParams(format!(
                        "{}: the node answered with a different node id",
                        codes::WRONG_NODE
                    )));
                }
                return Ok(welcome);
            }
            Some(Ok(Message::Ping(_) | Message::Pong(_))) => {}
            Some(Ok(Message::Close(Some(frame)))) => return Err(decode_rejection(&frame.reason)),
            _ => return Err(super::unavailable()),
        }
    }
}

/// Turn a node's close reason into a link error. Only known codes are
/// recognized, and only a generation number is taken from the text, so a
/// peer cannot inject arbitrary text into head's errors.
pub(super) fn decode_rejection(reason: &str) -> Error {
    let (code, detail) = reason.split_once(':').unwrap_or((reason, ""));
    let number = || {
        let digits: String = detail.chars().filter(char::is_ascii_digit).collect();
        digits.parse::<u64>().ok()
    };
    match code {
        codes::STALE => match number() {
            Some(current) => Error::InvalidParams(format!(
                "{}: the node is bound at generation {current}",
                codes::STALE
            )),
            None => Error::InvalidParams(format!("{}: generation is not newer", codes::STALE)),
        },
        codes::OTHER_HEAD => Error::InvalidParams(format!(
            "{}: the node is bound to another head; reset its binding to move it",
            codes::OTHER_HEAD
        )),
        codes::UNAUTHORIZED => Error::Forbidden(format!(
            "{}: the machine no longer accepts this credential",
            codes::UNAUTHORIZED
        )),
        codes::WRONG_NODE => Error::InvalidParams(format!(
            "{}: the endpoint serves a different node",
            codes::WRONG_NODE
        )),
        codes::VERSION_MISMATCH => match number() {
            Some(peer) => Error::InvalidParams(format!(
                "{}: node speaks {peer}, this daemon {NODE_LINK_PROTOCOLS}",
                codes::VERSION_MISMATCH
            )),
            None => Error::InvalidParams(format!(
                "{}: this daemon speaks {NODE_LINK_PROTOCOLS}",
                codes::VERSION_MISMATCH
            )),
        },
        codes::BINDING_STORE => Error::Internal(format!(
            "{}: the node cannot read or persist its binding",
            codes::BINDING_STORE
        )),
        _ => super::protocol("node closed the link during the handshake"),
    }
}

async fn connect(
    endpoint: &NodeEndpoint,
    path: &str,
    headers: &[(&'static str, &str)],
) -> Result<Socket> {
    let base = endpoint.url.trim_end_matches('/');
    let mut request = format!("{base}{path}")
        .into_client_request()
        .map_err(|_| super::config("node endpoint must be wss://host:port"))?;
    let uri = request.uri();
    if uri.scheme_str() != Some("wss")
        || uri.authority().is_none_or(|a| a.as_str().contains('@'))
        || uri.query().is_some()
    {
        return Err(super::config("node endpoint must be wss://host:port"));
    }
    let host = uri
        .host()
        .ok_or_else(|| super::config("node endpoint must be wss://host:port"))?
        .trim_start_matches('[')
        .trim_end_matches(']')
        .to_owned();
    let port = uri.port_u16().unwrap_or(443);
    let server_name = rustls_pki_types::ServerName::try_from(host.clone())
        .map_err(|_| super::config("node endpoint host is invalid"))?;
    let tls = TlsConnector::from(Arc::new(pin::client_config(endpoint.pin)?));

    let transport = match &endpoint.tailcat {
        None => {
            let tcp = TcpStream::connect((host.as_str(), port))
                .await
                .map_err(|_| super::unavailable())?;
            let _ = tcp.set_nodelay(true);
            DialStream::Tcp(tcp)
        }
        Some(route) => DialStream::tailcat(route, port)?,
    };
    // The pin is checked here, before any HTTP byte (and so the token) is
    // written.
    let stream = tls
        .connect(server_name, transport)
        .await
        .map_err(|e| tls_error(&e))?;

    let auth = format!("Bearer {}", endpoint.token.expose());
    let headers_mut = request.headers_mut();
    headers_mut.insert(
        http::header::AUTHORIZATION,
        http::HeaderValue::from_str(&auth).map_err(|_| super::config("invalid lease token"))?,
    );
    for (name, value) in headers {
        headers_mut.insert(
            *name,
            http::HeaderValue::from_str(value)
                .map_err(|_| super::config("binding ids must be header-safe"))?,
        );
    }
    let (socket, _) =
        tokio_tungstenite::client_async_with_config(request, stream, Some(socket_config()))
            .await
            .map_err(|e| upgrade_error(&e))?;
    Ok(socket)
}

pub(crate) fn socket_config() -> tungstenite::protocol::WebSocketConfig {
    tungstenite::protocol::WebSocketConfig::default()
        .max_message_size(Some(intent_core::node_link::MAX_FRAME_BYTES))
        .max_frame_size(Some(intent_core::node_link::MAX_FRAME_BYTES))
}

fn tls_error(error: &std::io::Error) -> Error {
    let pin_mismatch = error
        .get_ref()
        .and_then(|inner| inner.downcast_ref::<rustls::Error>())
        .is_some_and(|e| matches!(e, rustls::Error::General(m) if m == PIN_MISMATCH));
    if pin_mismatch {
        Error::InvalidParams(format!(
            "{}: the node's certificate does not match the configured pin",
            codes::CERTIFICATE_MISMATCH
        ))
    } else {
        super::unavailable()
    }
}

fn upgrade_error(error: &tungstenite::Error) -> Error {
    match error {
        tungstenite::Error::Http(response) => match response.status() {
            http::StatusCode::UNAUTHORIZED => Error::Forbidden(format!(
                "{}: the node rejected the lease token",
                codes::UNAUTHORIZED
            )),
            http::StatusCode::CONFLICT => Error::InvalidParams(format!(
                "{}: the binding is no longer current on the node",
                codes::STALE
            )),
            _ => super::protocol("unexpected upgrade response"),
        },
        _ => super::unavailable(),
    }
}

/// The byte stream under TLS: a direct TCP connection or a tailcat pipe
/// client's stdio. The child is killed when the stream is dropped.
pub(super) enum DialStream {
    Tcp(TcpStream),
    Tailcat(Box<TailcatPipe>),
}

/// A tailcat pipe client's stdio; the child dies with it.
pub(super) struct TailcatPipe {
    _child: tokio::process::Child,
    stdout: tokio::process::ChildStdout,
    stdin: tokio::process::ChildStdin,
}

impl DialStream {
    fn tailcat(route: &TailcatRoute, port: u16) -> Result<Self> {
        if route.address.is_empty() || route.address.starts_with('-') {
            return Err(super::config("tailcat address is invalid"));
        }
        let mut child = tokio::process::Command::new(&route.bin)
            .arg(&route.address)
            .arg(port.to_string())
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .map_err(|_| super::unavailable())?;
        let (Some(stdout), Some(stdin)) = (child.stdout.take(), child.stdin.take()) else {
            return Err(super::unavailable());
        };
        Ok(Self::Tailcat(Box::new(TailcatPipe {
            _child: child,
            stdout,
            stdin,
        })))
    }
}

impl tokio::io::AsyncRead for DialStream {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Tcp(s) => std::pin::Pin::new(s).poll_read(cx, buf),
            Self::Tailcat(pipe) => std::pin::Pin::new(&mut pipe.stdout).poll_read(cx, buf),
        }
    }
}

impl tokio::io::AsyncWrite for DialStream {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        match self.get_mut() {
            Self::Tcp(s) => std::pin::Pin::new(s).poll_write(cx, buf),
            Self::Tailcat(pipe) => std::pin::Pin::new(&mut pipe.stdin).poll_write(cx, buf),
        }
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Tcp(s) => std::pin::Pin::new(s).poll_flush(cx),
            Self::Tailcat(pipe) => std::pin::Pin::new(&mut pipe.stdin).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Tcp(s) => std::pin::Pin::new(s).poll_shutdown(cx),
            Self::Tailcat(pipe) => std::pin::Pin::new(&mut pipe.stdin).poll_shutdown(cx),
        }
    }
}
