//! Minimal in-process HTTP/1.1 mock for provider tests: accepts connections
//! on a loopback listener, captures each request (head + body), and replies
//! with the next scripted `(status, json_body)` response. No TLS — engines
//! point `base_url` at `http://127.0.0.1:{port}`.

use std::sync::{Arc, Mutex};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// One captured request: the raw head (request line + headers) and body.
#[derive(Debug, Clone)]
pub(crate) struct CapturedRequest {
    pub head: String,
    pub body: Vec<u8>,
}

impl CapturedRequest {
    /// The request body as (lossy) UTF-8 — fine for multipart text asserts.
    pub(crate) fn body_text(&self) -> String {
        String::from_utf8_lossy(&self.body).to_string()
    }
}

/// Spawn a mock server that serves `responses` in order (repeating the last
/// one if more requests arrive). Returns the base URL and the capture log.
pub async fn spawn(responses: Vec<(u16, String)>) -> (String, Arc<Mutex<Vec<CapturedRequest>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().unwrap().port();
    let captured: Arc<Mutex<Vec<CapturedRequest>>> = Arc::new(Mutex::new(Vec::new()));
    let log = captured.clone();
    tokio::spawn(async move {
        let mut served = 0usize;
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            let mut buf = Vec::new();
            let mut tmp = [0u8; 4096];
            let (head, body) = loop {
                let Ok(n) = stream.read(&mut tmp).await else {
                    return;
                };
                if n == 0 {
                    return;
                }
                buf.extend_from_slice(&tmp[..n]);
                if let Some(pos) = find_header_end(&buf) {
                    let head = String::from_utf8_lossy(&buf[..pos]).to_string();
                    let content_length = content_length(&head).unwrap_or(0);
                    let body_start = pos + 4;
                    while buf.len() < body_start + content_length {
                        let Ok(n) = stream.read(&mut tmp).await else {
                            return;
                        };
                        if n == 0 {
                            break;
                        }
                        buf.extend_from_slice(&tmp[..n]);
                    }
                    break (head, buf[body_start..].to_vec());
                }
            };
            log.lock().unwrap().push(CapturedRequest { head, body });
            let idx = served.min(responses.len().saturating_sub(1));
            let (status, body) = &responses[idx];
            served += 1;
            let reason = match *status {
                200 => "OK",
                401 => "Unauthorized",
                404 => "Not Found",
                429 => "Too Many Requests",
                _ => "Error",
            };
            let resp = format!(
                "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\n\
                 Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = stream.write_all(resp.as_bytes()).await;
            let _ = stream.shutdown().await;
        }
    });
    (format!("http://127.0.0.1:{port}"), captured)
}

fn find_header_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n")
}

fn content_length(head: &str) -> Option<usize> {
    head.lines().find_map(|l| {
        let (name, value) = l.split_once(':')?;
        if name.eq_ignore_ascii_case("content-length") {
            value.trim().parse().ok()
        } else {
            None
        }
    })
}
