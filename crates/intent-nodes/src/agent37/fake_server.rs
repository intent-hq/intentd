//! An HTTP server around [`super::fake_api::Agent37Api`]: the REST API and
//! the sandbox host's exec on one listener, both under `/v1`. A [`Host`]
//! runs each exec's command and sees each lifecycle [`Event`]. Tests can
//! queue one-shot overrides for a route, and read every request.

use std::collections::VecDeque;
use std::fmt::Write as _;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use super::fake_api::{exec_reply, Agent37Api, Event, ExecStart, Reply};

/// Runs what the instances do.
pub trait Host: Send + Sync + 'static {
    /// Run a checked exec and answer it.
    fn exec(&self, start: ExecStart) -> Pin<Box<dyn Future<Output = Reply> + Send + '_>>;

    /// Lifecycle changes, in order, before the reply that caused them.
    fn events(&self, _events: Vec<Event>) {}
}

/// Answers every exec with exit 0 and no output.
pub struct NoopHost;

impl Host for NoopHost {
    fn exec(&self, _start: ExecStart) -> Pin<Box<dyn Future<Output = Reply> + Send + '_>> {
        Box::pin(async { exec_reply(0, "", "") })
    }
}

#[derive(Debug, Clone)]
pub struct Recorded {
    pub method: String,
    /// The path after `/v1`, with its query.
    pub path: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Recorded {
    #[must_use]
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    #[must_use]
    pub fn json(&self) -> Value {
        serde_json::from_slice(&self.body).unwrap_or(Value::Null)
    }

    /// `METHOD path`, for order assertions.
    #[must_use]
    pub fn line(&self) -> String {
        format!("{} {}", self.method, self.path)
    }
}

struct Override {
    /// `METHOD path` where `*` matches one path part.
    route: String,
    reply: Reply,
}

#[derive(Clone)]
pub struct FakeServer {
    /// The base URL, including `/v1`: both the REST and the exec base.
    pub url: String,
    pub api: Arc<Mutex<Agent37Api>>,
    host: Arc<dyn Host>,
    requests: Arc<Mutex<Vec<Recorded>>>,
    overrides: Arc<Mutex<VecDeque<Override>>>,
}

fn route_matches(route: &str, method: &str, path: &str) -> bool {
    let path = path.split('?').next().unwrap_or_default();
    let Some((want_method, want_path)) = route.split_once(' ') else {
        return false;
    };
    let want: Vec<&str> = want_path.trim_matches('/').split('/').collect();
    let got: Vec<&str> = path.trim_matches('/').split('/').collect();
    want_method == method
        && want.len() == got.len()
        && want.iter().zip(&got).all(|(w, g)| *w == "*" || w == g)
}

impl FakeServer {
    /// Serve `api` on a new loopback port.
    ///
    /// # Panics
    ///
    /// When no loopback port can be bound.
    pub async fn start(api: Agent37Api, host: Arc<dyn Host>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let server = Self {
            url: format!("http://{addr}/v1"),
            api: Arc::new(Mutex::new(api)),
            host,
            requests: Arc::default(),
            overrides: Arc::default(),
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

    /// The model, locked.
    ///
    /// # Panics
    ///
    /// When a thread panicked while holding it.
    pub fn state(&self) -> std::sync::MutexGuard<'_, Agent37Api> {
        self.api.lock().expect("fake Agent37 state")
    }

    /// Answer the next request to `route` (`METHOD /path`, `*` for one
    /// part) with `reply` instead of the model.
    ///
    /// # Panics
    ///
    /// When a thread panicked while holding the queue.
    pub fn push_override(&self, route: &str, reply: Reply) {
        self.overrides
            .lock()
            .expect("overrides")
            .push_back(Override {
                route: route.to_owned(),
                reply,
            });
    }

    /// Drop every override not used yet.
    ///
    /// # Panics
    ///
    /// When a thread panicked while holding the queue.
    pub fn clear_overrides(&self) {
        self.overrides.lock().expect("overrides").clear();
    }

    /// # Panics
    ///
    /// When a thread panicked while holding the list.
    #[must_use]
    pub fn requests(&self) -> Vec<Recorded> {
        self.requests.lock().expect("requests").clone()
    }

    #[must_use]
    pub fn lines(&self) -> Vec<String> {
        self.requests().iter().map(Recorded::line).collect()
    }

    fn take_override(&self, method: &str, path: &str) -> Option<Reply> {
        let mut queue = self.overrides.lock().expect("overrides");
        let at = queue
            .iter()
            .position(|o| route_matches(&o.route, method, path))?;
        queue.remove(at).map(|o| o.reply)
    }

    async fn answer(&self, request: &Recorded) -> Reply {
        if let Some(reply) = self.take_override(&request.method, &request.path) {
            return reply;
        }
        let body: Option<Value> = serde_json::from_slice(&request.body).ok();
        let path = request
            .path
            .split('?')
            .next()
            .unwrap_or_default()
            .to_owned();
        let parts: Vec<&str> = path.trim_matches('/').split('/').collect();
        if let ["instances", id, "exec"] = parts.as_slice() {
            let checked = {
                let mut api = self.state();
                let checked = api.exec(
                    id,
                    request.header("x-agent37-key"),
                    body.as_ref().unwrap_or(&Value::Null),
                );
                (checked, api.drain_events())
            };
            self.host.events(checked.1);
            return match checked.0 {
                Ok(start) if start.background => {
                    let _ = self.host.exec(start).await;
                    Reply::ok(serde_json::json!({ "pid": 4821 }))
                }
                Ok(start) => self.host.exec(start).await,
                Err(reply) => reply,
            };
        }
        let bearer = request
            .header("authorization")
            .and_then(|v| v.strip_prefix("Bearer "));
        let (reply, events) = {
            let mut api = self.state();
            let reply = api.handle(&request.method, &path, bearer, body.as_ref());
            (reply, api.drain_events())
        };
        self.host.events(events);
        reply
    }

    async fn serve(&self, mut stream: TcpStream) {
        let Some(mut request) = read_request(&mut stream).await else {
            return;
        };
        let Some(path) = request.path.strip_prefix("/v1").map(str::to_owned) else {
            write_full(&mut stream, 404, b"{}").await;
            return;
        };
        request.path = path;
        self.requests
            .lock()
            .expect("requests")
            .push(request.clone());
        let reply = self.answer(&request).await;
        let body = serde_json::to_vec(&reply.body).unwrap_or_default();
        write_full(&mut stream, reply.status, &body).await;
        let _ = stream.shutdown().await;
    }
}

async fn write_full(stream: &mut TcpStream, status: u16, body: &[u8]) {
    let mut head = format!(
        "HTTP/1.1 {status} X\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n",
        body.len()
    );
    let _ = write!(head, "\r\n");
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
    let path = first.next()?.to_string();
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
        path,
        headers,
        body,
    })
}
