//! A scripted HTTP/1.1 server that stands in for the E2B API and envd.
//! Each connection serves one request with the next queued reply.

use std::collections::VecDeque;
use std::fmt::Write as _;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

#[derive(Debug, Clone)]
pub struct Recorded {
    pub method: String,
    pub target: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
    pub at: Instant,
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
}

pub enum Reply {
    Full {
        status: u16,
        headers: Vec<(String, String)>,
        body: Vec<u8>,
        delay: Duration,
    },
    /// Streamed body parts; a part with a gate waits for it before it is written.
    Stream {
        parts: Vec<(Vec<u8>, Option<oneshot::Receiver<()>>)>,
    },
}

impl Reply {
    pub fn status(status: u16) -> Self {
        Self::Full {
            status,
            headers: vec![],
            body: vec![],
            delay: Duration::ZERO,
        }
    }

    pub fn json(status: u16, body: &serde_json::Value) -> Self {
        Self::Full {
            status,
            headers: vec![("content-type".into(), "application/json".into())],
            body: serde_json::to_vec(body).unwrap(),
            delay: Duration::ZERO,
        }
    }

    pub fn error(status: u16, error_code: &str, message: &str) -> Self {
        Self::json(
            status,
            &serde_json::json!({"code": status, "message": message, "error_code": error_code}),
        )
    }

    #[must_use]
    pub fn with_header(mut self, name: &str, value: &str) -> Self {
        if let Self::Full { headers, .. } = &mut self {
            headers.push((name.into(), value.into()));
        }
        self
    }

    #[must_use]
    pub fn with_delay(mut self, value: Duration) -> Self {
        if let Self::Full { delay, .. } = &mut self {
            *delay = value;
        }
        self
    }
}

#[derive(Clone)]
pub struct FakeServer {
    pub url: String,
    requests: Arc<Mutex<Vec<Recorded>>>,
    replies: Arc<Mutex<VecDeque<Reply>>>,
    in_flight: Arc<AtomicUsize>,
    max_in_flight: Arc<AtomicUsize>,
}

impl FakeServer {
    pub async fn start(replies: Vec<Reply>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = Self {
            url,
            requests: Arc::default(),
            replies: Arc::new(Mutex::new(replies.into())),
            in_flight: Arc::default(),
            max_in_flight: Arc::default(),
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

    pub fn requests(&self) -> Vec<Recorded> {
        self.requests.lock().unwrap().clone()
    }

    pub fn max_in_flight(&self) -> usize {
        self.max_in_flight.load(Ordering::SeqCst)
    }

    async fn serve(&self, mut stream: TcpStream) {
        let Some(request) = read_request(&mut stream).await else {
            return;
        };
        let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_in_flight.fetch_max(now, Ordering::SeqCst);
        self.requests.lock().unwrap().push(request);
        let reply = self.replies.lock().unwrap().pop_front();
        match reply {
            None => write_full(&mut stream, 599, &[], b"no scripted reply").await,
            Some(Reply::Full {
                status,
                headers,
                body,
                delay,
            }) => {
                tokio::time::sleep(delay).await;
                write_full(&mut stream, status, &headers, &body).await;
            }
            Some(Reply::Stream { parts }) => {
                let head = "HTTP/1.1 200 OK\r\ncontent-type: application/connect+json\r\nconnection: close\r\n\r\n";
                let _ = stream.write_all(head.as_bytes()).await;
                let _ = stream.flush().await;
                for (bytes, gate) in parts {
                    if let Some(gate) = gate {
                        let _ = gate.await;
                    }
                    let _ = stream.write_all(&bytes).await;
                    let _ = stream.flush().await;
                }
            }
        }
        let _ = stream.shutdown().await;
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
    }
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
        at: Instant::now(),
    })
}

/// One Connect protocol frame (flags, big-endian length, JSON payload).
pub fn frame(flags: u8, payload: &serde_json::Value) -> Vec<u8> {
    let json = serde_json::to_vec(payload).unwrap();
    let mut out = vec![flags];
    out.extend_from_slice(&u32::try_from(json.len()).unwrap().to_be_bytes());
    out.extend_from_slice(&json);
    out
}
