//! A local OpenAI-compatible chat-completions server for `input.autocomplete`
//! tests: it answers every request with one canned reply and records what it
//! received, so tests never reach the network.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use serde_json::{json, Value};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::TcpListener;

/// The canned reply.
#[derive(Debug, Clone)]
pub enum MockReply {
    /// Answer with this HTTP status and raw body.
    Status(u16, String),
    /// Accept the request and never answer (exercises the client timeout).
    Hang,
}

impl MockReply {
    /// A 200 chat-completions reply whose first choice says `content`.
    #[must_use]
    pub fn content(content: &str) -> Self {
        Self::Status(
            200,
            json!({
                "id": "mock",
                "object": "chat.completion",
                "choices": [{
                    "index": 0,
                    "message": { "role": "assistant", "content": content },
                    "finish_reason": "stop",
                }],
            })
            .to_string(),
        )
    }
}

/// One request the mock received.
#[derive(Debug, Clone)]
pub struct RecordedRequest {
    /// The request line, e.g. `POST /chat/completions HTTP/1.1`.
    pub request_line: String,
    /// Headers with lower-cased names.
    pub headers: Vec<(String, String)>,
    /// The JSON body (`Value::Null` when it was not JSON).
    pub body: Value,
}

impl RecordedRequest {
    /// The first value of header `name` (case-insensitive).
    #[must_use]
    pub fn header(&self, name: &str) -> Option<&str> {
        let name = name.to_ascii_lowercase();
        self.headers
            .iter()
            .find(|(k, _)| *k == name)
            .map(|(_, v)| v.as_str())
    }
}

/// The running mock server; stops when dropped.
#[derive(Debug)]
pub struct MockChatUpstream {
    addr: SocketAddr,
    requests: Arc<Mutex<Vec<RecordedRequest>>>,
    handle: tokio::task::JoinHandle<()>,
}

impl MockChatUpstream {
    /// Bind 127.0.0.1 on a free port and answer every request with `reply`.
    ///
    /// # Panics
    /// When the loopback listener cannot be bound.
    pub async fn start(reply: MockReply) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock chat upstream");
        let addr = listener.local_addr().expect("mock chat upstream addr");
        let requests = Arc::new(Mutex::new(Vec::new()));
        let recorded = requests.clone();
        // caller-binding: allow — a loopback test server that never reaches a capability gate
        let handle = tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    return;
                };
                let recorded = recorded.clone();
                let reply = reply.clone();
                // caller-binding: allow — a loopback test server that never reaches a capability gate
                tokio::spawn(async move {
                    let Some(request) = read_request(&mut sock).await else {
                        return;
                    };
                    recorded.lock().unwrap().push(request);
                    match reply {
                        MockReply::Hang => std::future::pending::<()>().await,
                        MockReply::Status(status, body) => {
                            let response = format!(
                                "HTTP/1.1 {status} Mock\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                                body.len()
                            );
                            let _ = sock.write_all(response.as_bytes()).await;
                            let _ = sock.shutdown().await;
                        }
                    }
                });
            }
        });
        Self {
            addr,
            requests,
            handle,
        }
    }

    /// The chat-completions URL served by this mock.
    #[must_use]
    pub fn url(&self) -> String {
        format!("http://{}/chat/completions", self.addr)
    }

    /// Every request received so far.
    ///
    /// # Panics
    /// When the recording mutex is poisoned.
    #[must_use]
    pub fn requests(&self) -> Vec<RecordedRequest> {
        self.requests.lock().unwrap().clone()
    }
}

impl Drop for MockChatUpstream {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

async fn read_request(sock: &mut tokio::net::TcpStream) -> Option<RecordedRequest> {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 4096];
    loop {
        let n = sock.read(&mut tmp).await.ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&tmp[..n]);
        let Some(head_end) = buf.windows(4).position(|w| w == b"\r\n\r\n") else {
            continue;
        };
        let head = String::from_utf8_lossy(&buf[..head_end]).into_owned();
        let mut lines = head.split("\r\n");
        let request_line = lines.next().unwrap_or_default().to_string();
        let headers: Vec<(String, String)> = lines
            .filter_map(|l| l.split_once(':'))
            .map(|(k, v)| (k.trim().to_ascii_lowercase(), v.trim().to_string()))
            .collect();
        let content_length = headers
            .iter()
            .find(|(k, _)| k == "content-length")
            .and_then(|(_, v)| v.parse::<usize>().ok())
            .unwrap_or(0);
        if buf.len() < head_end + 4 + content_length {
            continue;
        }
        let body_bytes = &buf[head_end + 4..head_end + 4 + content_length];
        let body = serde_json::from_slice(body_bytes).unwrap_or(Value::Null);
        return Some(RecordedRequest {
            request_line,
            headers,
            body,
        });
    }
}
