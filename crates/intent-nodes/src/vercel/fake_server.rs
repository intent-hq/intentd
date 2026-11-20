//! An HTTP server around [`super::fake_api::VercelApi`]: the REST API, the
//! command stream and the registry's manifest lookup on one listener. A
//! [`Host`] runs each command and sees each lifecycle [`Event`]. Tests can
//! queue one-shot overrides for a route, and read every request.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use base64::Engine as _;
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use super::fake_api::{command_stream, CommandStart, Event, Reply, VercelApi};

/// What a command did: its exit code, stdout and stderr.
pub type Ran = (i32, String, String);

/// Runs what the sandboxes do.
pub trait Host: Send + Sync + 'static {
    /// Run a checked command.
    fn run(&self, start: CommandStart) -> Pin<Box<dyn Future<Output = Ran> + Send + '_>>;

    /// Lifecycle changes, in order, before the reply that caused them.
    fn events(&self, _events: Vec<Event>) {}
}

/// Runs every command as exit 0 with no output.
pub struct NoopHost;

impl Host for NoopHost {
    fn run(&self, _start: CommandStart) -> Pin<Box<dyn Future<Output = Ran> + Send + '_>> {
        Box::pin(async { (0, String::new(), String::new()) })
    }
}

#[derive(Debug, Clone)]
pub struct Recorded {
    pub method: String,
    /// The path, with its query.
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

    /// `METHOD path` without the query, for order assertions.
    #[must_use]
    pub fn line(&self) -> String {
        format!(
            "{} {}",
            self.method,
            self.path.split('?').next().unwrap_or_default()
        )
    }
}

/// A one-shot answer instead of the model's.
struct Override {
    /// `METHOD path` where `*` matches one path part.
    route: String,
    reply: Reply,
}

#[derive(Clone)]
pub struct FakeServer {
    /// The base URL: both the REST and the registry base.
    pub url: String,
    pub api: Arc<Mutex<VercelApi>>,
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

/// `(user, password)` of a basic `Authorization` header.
fn basic(header: Option<&str>) -> Option<(String, String)> {
    let encoded = header?.strip_prefix("Basic ")?;
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(encoded.trim())
        .ok()?;
    let text = String::from_utf8(decoded).ok()?;
    let (user, password) = text.split_once(':')?;
    Some((user.to_owned(), password.to_owned()))
}

impl FakeServer {
    /// Serve `api` on a new loopback port.
    ///
    /// # Panics
    ///
    /// When no loopback port can be bound.
    pub async fn start(api: VercelApi, host: Arc<dyn Host>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let server = Self {
            url: format!("http://{addr}"),
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
    pub fn state(&self) -> std::sync::MutexGuard<'_, VercelApi> {
        self.api.lock().expect("fake Vercel state")
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

    /// The reply, and whether it is a newline-delimited stream.
    async fn answer(&self, request: &Recorded) -> (Reply, bool) {
        if let Some(reply) = self.take_override(&request.method, &request.path) {
            return (reply, false);
        }
        let route = request.path.split('?').next().unwrap_or_default();
        if request.method == "HEAD" && route.contains("/manifests/") {
            let credentials = basic(request.header("authorization"));
            let reply = self.state().manifest(
                route,
                credentials
                    .as_ref()
                    .map(|(user, password)| (user.as_str(), password.as_str())),
            );
            return (reply, false);
        }
        let body: Option<Value> = serde_json::from_slice(&request.body).ok();
        let bearer = request
            .header("authorization")
            .and_then(|v| v.strip_prefix("Bearer "));
        let parts: Vec<&str> = route.trim_matches('/').split('/').collect();
        if let ["v2", "sandboxes", "sessions", session, "cmd"] = parts.as_slice() {
            if request.method == "POST" {
                let (checked, events, id) = {
                    let mut api = self.state();
                    let checked = api.command(
                        session,
                        &request.path,
                        bearer,
                        body.as_ref().unwrap_or(&Value::Null),
                    );
                    let id = api.command_id();
                    (checked, api.drain_events(), id)
                };
                self.host.events(events);
                return match checked {
                    Ok(start) => {
                        let (exit, stdout, stderr) = self.host.run(start.clone()).await;
                        let lines = command_stream(&start, &id, exit, &stdout, &stderr);
                        (Reply::ok(Value::Array(lines)), true)
                    }
                    Err(reply) => (reply, false),
                };
            }
        }
        let (reply, events) = {
            let mut api = self.state();
            let reply = api.handle(&request.method, &request.path, bearer, body.as_ref());
            (reply, api.drain_events())
        };
        self.host.events(events);
        (reply, false)
    }

    async fn serve(&self, mut stream: TcpStream) {
        let Some(request) = read_request(&mut stream).await else {
            return;
        };
        self.requests
            .lock()
            .expect("requests")
            .push(request.clone());
        let (reply, ndjson) = self.answer(&request).await;
        let (content_type, body) = if ndjson {
            let lines: Vec<String> = reply
                .body
                .as_array()
                .map(|lines| lines.iter().map(Value::to_string).collect())
                .unwrap_or_default();
            (
                "application/x-ndjson",
                (lines.join("\n") + "\n").into_bytes(),
            )
        } else if reply.body.is_null() {
            ("application/json", Vec::new())
        } else {
            (
                "application/json",
                serde_json::to_vec(&reply.body).unwrap_or_default(),
            )
        };
        let head = format!(
            "HTTP/1.1 {} X\r\ncontent-type: {content_type}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
            reply.status,
            body.len()
        );
        let _ = stream.write_all(head.as_bytes()).await;
        if request.method != "HEAD" {
            let _ = stream.write_all(&body).await;
        }
        let _ = stream.flush().await;
        let _ = stream.shutdown().await;
    }
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
