//! A scripted server that stands in for the Northflank API, its command
//! execution WebSocket and an OCI registry, on one listener.
//!
//! Each plain HTTP connection serves one request with the next queued
//! [`Reply`]. Each WebSocket upgrade plays the next queued [`WsSession`].
//! Every request is recorded, including the upgrade's URL and headers, and
//! every text frame the client sends on a WebSocket.

use std::collections::VecDeque;
use std::fmt::Write as _;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::{SinkExt as _, StreamExt as _};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::Message;

#[derive(Debug, Clone)]
pub struct Recorded {
    pub method: String,
    pub target: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Recorded {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    pub fn json(&self) -> serde_json::Value {
        serde_json::from_slice(&self.body).expect("request body is JSON")
    }

    /// `METHOD target`, for order assertions.
    pub fn line(&self) -> String {
        format!("{} {}", self.method, self.target)
    }
}

pub struct Reply {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl Reply {
    pub fn status(status: u16) -> Self {
        Self {
            status,
            headers: vec![],
            body: vec![],
        }
    }

    pub fn json(status: u16, body: &serde_json::Value) -> Self {
        Self {
            status,
            headers: vec![("content-type".into(), "application/json".into())],
            body: serde_json::to_vec(body).unwrap(),
        }
    }

    /// A Northflank success envelope: `{ "data": data }`.
    pub fn data(data: &serde_json::Value) -> Self {
        Self::json(200, &serde_json::json!({ "data": data }))
    }

    /// A Northflank error envelope.
    pub fn error(status: u16, message: &str) -> Self {
        Self::json(
            status,
            &serde_json::json!({"error": {"status": status, "message": message}}),
        )
    }

    #[must_use]
    pub fn with_header(mut self, name: &str, value: &str) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }
}

/// One step of a scripted command WebSocket.
pub enum WsStep {
    /// Wait for the client's next text frame and record it.
    Receive,
    Send(serde_json::Value),
    SendText(String),
    Sleep(Duration),
    Ping,
    /// A normal close (1000).
    Close,
    /// Drop the TCP connection without a close frame.
    Drop,
}

pub enum WsSession {
    Steps(Vec<WsStep>),
    /// Refuse the upgrade with this HTTP status and body.
    Refuse(u16, String),
}

#[derive(Clone)]
pub struct FakeServer {
    pub url: String,
    /// The same listener, as a `ws://` URL.
    pub ws_url: String,
    requests: Arc<Mutex<Vec<Recorded>>>,
    replies: Arc<Mutex<VecDeque<Reply>>>,
    sessions: Arc<Mutex<VecDeque<WsSession>>>,
    frames: Arc<Mutex<Vec<String>>>,
}

impl FakeServer {
    pub async fn start(replies: Vec<Reply>) -> Self {
        Self::start_with(replies, vec![]).await
    }

    pub async fn start_with(replies: Vec<Reply>, sessions: Vec<WsSession>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = Self {
            url: format!("http://{addr}"),
            ws_url: format!("ws://{addr}"),
            requests: Arc::default(),
            replies: Arc::new(Mutex::new(replies.into())),
            sessions: Arc::new(Mutex::new(sessions.into())),
            frames: Arc::default(),
        };
        let accept = server.clone();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let conn = accept.clone();
                tokio::spawn(async move { conn.serve(stream).await });
            }
        });
        server
    }

    /// The host and port, as an image reference names a registry.
    pub fn host(&self) -> String {
        self.url.trim_start_matches("http://").to_string()
    }

    pub fn requests(&self) -> Vec<Recorded> {
        self.requests.lock().unwrap().clone()
    }

    pub fn lines(&self) -> Vec<String> {
        self.requests().iter().map(Recorded::line).collect()
    }

    /// Text frames the clients sent on command sockets.
    pub fn frames(&self) -> Vec<String> {
        self.frames.lock().unwrap().clone()
    }

    pub fn push(&self, reply: Reply) {
        self.replies.lock().unwrap().push_back(reply);
    }

    async fn serve(&self, stream: TcpStream) {
        let Some(head) = peek_head(&stream).await else {
            return;
        };
        if head.to_ascii_lowercase().contains("upgrade: websocket") {
            self.serve_ws(stream).await;
        } else {
            self.serve_http(stream).await;
        }
    }

    async fn serve_http(&self, mut stream: TcpStream) {
        let Some(request) = read_request(&mut stream).await else {
            return;
        };
        self.requests.lock().unwrap().push(request);
        let reply = self.replies.lock().unwrap().pop_front();
        match reply {
            None => write_full(&mut stream, 599, &[], b"no scripted reply").await,
            Some(reply) => write_full(&mut stream, reply.status, &reply.headers, &reply.body).await,
        }
        let _ = stream.shutdown().await;
    }

    #[expect(
        clippy::result_large_err,
        reason = "the handshake callback signature is tungstenite's"
    )]
    async fn serve_ws(&self, mut stream: TcpStream) {
        let session = self.sessions.lock().unwrap().pop_front();
        let steps = match session {
            None => {
                let _ = read_request(&mut stream).await;
                write_full(&mut stream, 599, &[], b"no scripted session").await;
                return;
            }
            Some(WsSession::Refuse(status, body)) => {
                if let Some(request) = read_request(&mut stream).await {
                    self.requests.lock().unwrap().push(request);
                }
                write_full(&mut stream, status, &[], body.as_bytes()).await;
                let _ = stream.shutdown().await;
                return;
            }
            Some(WsSession::Steps(steps)) => steps,
        };
        let requests = self.requests.clone();
        let record = move |req: &Request, resp: Response| {
            let headers = req
                .headers()
                .iter()
                .map(|(k, v)| {
                    (
                        k.as_str().to_ascii_lowercase(),
                        v.to_str().unwrap_or("").to_string(),
                    )
                })
                .collect();
            requests.lock().unwrap().push(Recorded {
                method: req.method().to_string(),
                target: req.uri().to_string(),
                headers,
                body: vec![],
            });
            Ok(resp)
        };
        let Ok(mut ws) = tokio_tungstenite::accept_hdr_async(stream, record).await else {
            return;
        };
        for step in steps {
            match step {
                WsStep::Receive => loop {
                    match ws.next().await {
                        Some(Ok(Message::Text(text))) => {
                            self.frames.lock().unwrap().push(text.to_string());
                            break;
                        }
                        Some(Ok(_)) => {}
                        _ => return,
                    }
                },
                WsStep::Send(value) => {
                    let _ = ws.send(Message::text(value.to_string())).await;
                }
                WsStep::SendText(text) => {
                    let _ = ws.send(Message::text(text)).await;
                }
                WsStep::Sleep(d) => tokio::time::sleep(d).await,
                WsStep::Ping => {
                    let _ = ws.send(Message::Ping(Vec::new().into())).await;
                }
                WsStep::Close => {
                    let _ = ws
                        .close(Some(CloseFrame {
                            code: CloseCode::Normal,
                            reason: "".into(),
                        }))
                        .await;
                    // Let the client answer the close.
                    while let Some(Ok(_)) = ws.next().await {}
                    return;
                }
                WsStep::Drop => return,
            }
        }
        // Script done: keep the socket open until the client goes.
        while let Some(Ok(msg)) = ws.next().await {
            if let Message::Text(text) = msg {
                self.frames.lock().unwrap().push(text.to_string());
            }
        }
    }
}

async fn peek_head(stream: &TcpStream) -> Option<String> {
    let mut buf = vec![0u8; 16 * 1024];
    for _ in 0..200 {
        let n = stream.peek(&mut buf).await.ok()?;
        if n == 0 {
            return None;
        }
        if let Some(i) = buf[..n].windows(4).position(|w| w == b"\r\n\r\n") {
            return Some(String::from_utf8_lossy(&buf[..i]).to_string());
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    None
}

async fn write_full(
    stream: &mut TcpStream,
    status: u16,
    headers: &[(String, String)],
    body: &[u8],
) {
    let mut head = format!(
        "HTTP/1.1 {status} X\r\ncontent-length: {}\r\nconnection: close\r\n",
        body.len()
    );
    for (k, v) in headers {
        let _ = write!(head, "{k}: {v}\r\n");
    }
    head.push_str("\r\n");
    let _ = stream.write_all(head.as_bytes()).await;
    let _ = stream.write_all(body).await;
    let _ = stream.flush().await;
}

async fn read_request(stream: &mut TcpStream) -> Option<Recorded> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    let head_end = loop {
        if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break i;
        }
        let n = stream.read(&mut chunk).await.ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&chunk[..n]);
    };
    let head = String::from_utf8_lossy(&buf[..head_end]).to_string();
    let mut lines = head.split("\r\n");
    let mut first = lines.next()?.split(' ');
    let method = first.next()?.to_string();
    let target = first.next()?.to_string();
    let headers: Vec<(String, String)> = lines
        .filter_map(|l| l.split_once(':'))
        .map(|(k, v)| (k.trim().to_ascii_lowercase(), v.trim().to_string()))
        .collect();
    let len: usize = headers
        .iter()
        .find(|(k, _)| k == "content-length")
        .and_then(|(_, v)| v.parse().ok())
        .unwrap_or(0);
    let mut body = buf[head_end + 4..].to_vec();
    while body.len() < len {
        let n = stream.read(&mut chunk).await.ok()?;
        if n == 0 {
            break;
        }
        body.extend_from_slice(&chunk[..n]);
    }
    Some(Recorded {
        method,
        target,
        headers,
        body,
    })
}
