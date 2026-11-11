//! Northflank command execution: one command per WebSocket, as
//! `@northflank/js-client` 0.12 speaks it.
//!
//! The first frame authenticates with the API token (never the URL). Then
//! `stdOut` / `stdErr` frames carry output and one `completion` frame the
//! exit. Output may still follow `completion`; the stream is read until
//! Northflank closes it, or until it has been silent for the drain time.
//!
//! GUARDRAIL: frames carry the token and the command's environment, so they
//! are never logged.

use std::sync::Arc;

use futures_util::{SinkExt as _, StreamExt as _};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::time::Instant;
use tokio_rustls::TlsConnector;
use tokio_tungstenite::tungstenite::client::IntoClientRequest as _;
use tokio_tungstenite::tungstenite::{self, Message};
use tokio_tungstenite::WebSocketStream;

use super::client::{error_message, scrub, validate_id};
use super::{NorthflankClient, HOME};
use crate::cloud::{
    CloudError, CloudErrorKind, CloudInstance, ProcessExit, ProcessOutput, ProcessSpec,
    SecretString,
};

/// Per-stream cap of collected output, as `ProcessStream::collect`.
const MAX_COLLECTED: usize = 16 * 1024 * 1024;
/// Largest frame accepted from Northflank.
const MAX_FRAME: usize = 8 * 1024 * 1024;

const STREAM_ENDED: &str = "the command stream ended before the command finished";

trait Io: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Io for T {}

type Socket = WebSocketStream<Box<dyn Io>>;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Completion {
    #[serde(default)]
    exit_code: Option<i32>,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    message: Option<String>,
}

/// `env [-C cwd] HOME=/home/user K=V… cmd args…`: `env` sets the
/// environment and folder without a shell. `HOME` is the volume's home
/// unless the spec sets it.
pub(super) fn argv(spec: &ProcessSpec) -> Vec<String> {
    let mut argv = vec!["env".to_string()];
    if let Some(cwd) = &spec.cwd {
        argv.push("-C".into());
        argv.push(cwd.clone());
    }
    if !spec.envs.contains_key("HOME") {
        argv.push(format!("HOME={HOME}"));
    }
    // The image runs everything as root; Claude Code refuses its
    // bypass-permissions mode as root outside a declared sandbox.
    if !spec.envs.contains_key("IS_SANDBOX") {
        argv.push("IS_SANDBOX=1".into());
    }
    for (key, value) in &spec.envs {
        argv.push(format!("{key}={}", value.expose()));
    }
    argv.push(spec.cmd.clone());
    argv.extend(spec.args.iter().cloned());
    argv
}

struct Collected {
    stdout: Vec<u8>,
    stderr: Vec<u8>,
    truncated: bool,
}

impl Collected {
    fn push(&mut self, stderr: bool, data: &str) {
        let into = if stderr {
            &mut self.stderr
        } else {
            &mut self.stdout
        };
        let room = MAX_COLLECTED.saturating_sub(into.len());
        let bytes = data.as_bytes();
        if bytes.len() > room {
            self.truncated = true;
        }
        into.extend_from_slice(&bytes[..bytes.len().min(room)]);
    }
}

impl NorthflankClient {
    /// Run `spec` in service `instance.id` and collect its output.
    pub(super) async fn exec(
        &self,
        instance: &CloudInstance,
        spec: &ProcessSpec,
    ) -> Result<ProcessOutput, CloudError> {
        validate_id(&instance.id, "service id")?;
        if spec.cmd.is_empty() {
            return Err(CloudError::new(
                CloudErrorKind::InvalidArgument,
                "the command is empty",
            ));
        }
        let secrets: Vec<&str> = std::iter::once(self.token.expose())
            .chain(spec.envs.values().map(SecretString::expose))
            .collect();
        let run = self.exec_inner(&instance.id, spec, &secrets);
        let result = match spec.timeout {
            Some(limit) => tokio::time::timeout(limit, run).await.unwrap_or_else(|_| {
                Err(CloudError::new(
                    CloudErrorKind::Other,
                    format!(
                        "the Northflank command timed out after {} ms",
                        limit.as_millis()
                    ),
                ))
            }),
            None => run.await,
        };
        result.map_err(|e| rescrub(e, &secrets))
    }

    async fn exec_inner(
        &self,
        service_id: &str,
        spec: &ProcessSpec,
        secrets: &[&str],
    ) -> Result<ProcessOutput, CloudError> {
        let url = format!(
            "{}/command-exec/projects/{}/services/{service_id}",
            self.config.exec_url.trim_end_matches('/'),
            self.project_id()?
        );
        let mut ws = tokio::time::timeout(self.config.request_timeout, self.open(&url))
            .await
            .map_err(|_| {
                CloudError::new(
                    CloudErrorKind::Other,
                    "the Northflank command stream did not open in time",
                )
            })??;
        let mut context = json!({ "command": argv(spec), "shell": "none" });
        if let Some(user) = &spec.user {
            context["user"] = json!(user);
        }
        let init = json!({
            "type": "init",
            "data": {
                "auth": { "type": "apiToken", "apiToken": self.token.expose() },
                "context": context,
            }
        });
        ws.send(Message::text(init.to_string()))
            .await
            .map_err(|e| stream_error(&e, secrets))?;

        let mut collected = Collected {
            stdout: Vec::new(),
            stderr: Vec::new(),
            truncated: false,
        };
        let mut completion: Option<Completion> = None;
        // After `completion`, counted from the last output frame: control
        // frames (pings) never extend it.
        let mut drain: Option<Instant> = None;
        loop {
            let next = match drain {
                None => ws.next().await,
                Some(deadline) => match tokio::time::timeout_at(deadline, ws.next()).await {
                    Ok(next) => next,
                    Err(_) => break,
                },
            };
            let text = match next {
                Some(Ok(Message::Text(text))) => text.to_string(),
                Some(Ok(Message::Binary(bytes))) => String::from_utf8_lossy(&bytes).to_string(),
                Some(Ok(Message::Close(_))) | None => break,
                Some(Ok(_)) => continue,
                Some(Err(e)) => {
                    if completion.is_some() {
                        break;
                    }
                    return Err(stream_error(&e, secrets));
                }
            };
            let Ok(frame) = serde_json::from_str::<Value>(&text) else {
                return Err(CloudError::new(
                    CloudErrorKind::Other,
                    "Northflank sent a malformed command frame",
                ));
            };
            let data = frame.get("data").cloned().unwrap_or(Value::Null);
            match frame.get("type").and_then(Value::as_str) {
                Some("init") if data.get("auth").and_then(Value::as_str) != Some("successful") => {
                    return Err(init_refused(&data, secrets));
                }
                Some(kind @ ("stdOut" | "stdErr")) => {
                    collected.push(kind == "stdErr", data.as_str().unwrap_or(""));
                    if drain.is_some() {
                        drain = Some(Instant::now() + self.config.exec_drain_timeout);
                    }
                }
                Some("completion") if completion.is_none() => {
                    drain = Some(Instant::now() + self.config.exec_drain_timeout);
                    completion = Some(serde_json::from_value(data).unwrap_or(Completion {
                        exit_code: None,
                        status: None,
                        message: None,
                    }));
                }
                Some("error") if completion.is_none() => {
                    let message = error_message(&json!({ "data": data }))
                        .or_else(|| data.as_str().map(str::to_owned))
                        .unwrap_or_else(|| "unknown error".into());
                    return Err(CloudError::new(
                        CloudErrorKind::Other,
                        format!(
                            "the Northflank command failed: {}",
                            scrub(&message, secrets)
                        ),
                    ));
                }
                _ => {}
            }
        }
        let _ = ws.close(None).await;
        let Some(done) = completion else {
            return Err(CloudError::new(CloudErrorKind::Other, STREAM_ENDED));
        };
        let status = done.status.unwrap_or_else(|| "Unknown".into());
        let error = (status == "Failure").then(|| {
            done.message
                .as_deref()
                .map_or_else(|| "the command failed".into(), |m| scrub(m, secrets))
        });
        Ok(ProcessOutput {
            pid: None,
            exit: ProcessExit {
                exit_code: done.exit_code.unwrap_or(-1),
                exited: true,
                status,
                error,
            },
            stdout: collected.stdout,
            stderr: collected.stderr,
            truncated: collected.truncated,
        })
    }

    async fn open(&self, url: &str) -> Result<Socket, CloudError> {
        let request = url.into_client_request().map_err(|_| {
            CloudError::new(
                CloudErrorKind::InvalidArgument,
                "bad Northflank command URL",
            )
        })?;
        let uri = request.uri().clone();
        let host = uri.host().unwrap_or("").to_string();
        let tls = match uri.scheme_str() {
            Some("wss") => true,
            Some("ws") => false,
            _ => {
                return Err(CloudError::new(
                    CloudErrorKind::InvalidArgument,
                    "the Northflank command URL must be ws:// or wss://",
                ))
            }
        };
        let port = uri.port_u16().unwrap_or(if tls { 443 } else { 80 });
        let tcp = tokio::time::timeout(
            self.config.connect_timeout,
            TcpStream::connect((host.as_str(), port)),
        )
        .await
        .map_err(|_| unreachable("the connection timed out"))?
        .map_err(|e| unreachable(&e.to_string()))?;
        let _ = tcp.set_nodelay(true);
        let stream: Box<dyn Io> = if tls {
            let name = rustls_pki_types::ServerName::try_from(host.clone()).map_err(|_| {
                CloudError::new(
                    CloudErrorKind::InvalidArgument,
                    "bad Northflank command host",
                )
            })?;
            let connector = TlsConnector::from(tls_config());
            Box::new(
                connector
                    .connect(name, tcp)
                    .await
                    .map_err(|e| unreachable(&format!("TLS failed: {e}")))?,
            )
        } else {
            Box::new(tcp)
        };
        let mut config = tungstenite::protocol::WebSocketConfig::default();
        config.max_message_size = Some(MAX_FRAME);
        config.max_frame_size = Some(MAX_FRAME);
        match tokio_tungstenite::client_async_with_config(request, stream, Some(config)).await {
            Ok((ws, _)) => Ok(ws),
            Err(tungstenite::Error::Http(response)) => Err(self.upgrade_refused(&response)),
            Err(e) => Err(CloudError::new(
                CloudErrorKind::Other,
                format!(
                    "the Northflank command stream failed to open: {}",
                    self.scrub(&e.to_string())
                ),
            )),
        }
    }

    fn upgrade_refused(
        &self,
        response: &tungstenite::http::Response<Option<Vec<u8>>>,
    ) -> CloudError {
        let status = response.status().as_u16();
        let body = response.body().as_deref().unwrap_or(&[]);
        let message = serde_json::from_slice::<Value>(body)
            .ok()
            .and_then(|v| error_message(&v))
            .unwrap_or_else(|| String::from_utf8_lossy(body).trim().to_string());
        let message = self.scrub(&message);
        let (kind, retryable) = match status {
            401 => (CloudErrorKind::Unauthorized, false),
            403 => (CloudErrorKind::PermissionDenied, false),
            404 => (CloudErrorKind::NotFound, false),
            409 => (CloudErrorKind::Conflict, true),
            429 => (CloudErrorKind::Quota(crate::cloud::QuotaLimit::Rate), true),
            500..=599 => (CloudErrorKind::Api, true),
            _ => (CloudErrorKind::Api, false),
        };
        let mut error = CloudError::new(
            kind,
            format!("Northflank refused the command stream (HTTP {status}): {message}"),
        )
        .with_status(status)
        .retryable(retryable);
        if kind == CloudErrorKind::PermissionDenied {
            error = error.with_action("exec");
        }
        error
    }
}

fn tls_config() -> Arc<rustls::ClientConfig> {
    static CONFIG: std::sync::OnceLock<Arc<rustls::ClientConfig>> = std::sync::OnceLock::new();
    CONFIG
        .get_or_init(|| {
            let roots = rustls::RootCertStore {
                roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
            };
            let provider = Arc::new(rustls::crypto::ring::default_provider());
            Arc::new(
                rustls::ClientConfig::builder_with_provider(provider)
                    .with_safe_default_protocol_versions()
                    .expect("ring supports the default TLS versions")
                    .with_root_certificates(roots)
                    .with_no_client_auth(),
            )
        })
        .clone()
}

fn unreachable(why: &str) -> CloudError {
    CloudError::new(
        CloudErrorKind::Other,
        format!("Northflank command execution is unreachable: {why}"),
    )
    .retryable(true)
}

fn stream_error(error: &tungstenite::Error, secrets: &[&str]) -> CloudError {
    match error {
        tungstenite::Error::ConnectionClosed
        | tungstenite::Error::AlreadyClosed
        | tungstenite::Error::Protocol(_)
        | tungstenite::Error::Io(_) => CloudError::new(CloudErrorKind::Other, STREAM_ENDED),
        other => CloudError::new(
            CloudErrorKind::Other,
            format!(
                "the Northflank command stream failed: {}",
                scrub(&other.to_string(), secrets)
            ),
        ),
    }
}

/// An `init` answer other than `successful`: a rejected token, or a token
/// without the permission to run commands, by its message.
fn init_refused(data: &Value, secrets: &[&str]) -> CloudError {
    let message = data
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("Reason unknown");
    let message = scrub(message, secrets);
    let lower = message.to_ascii_lowercase();
    let denied = [
        "permission",
        "forbidden",
        "not allowed",
        "not permitted",
        "access denied",
    ]
    .iter()
    .any(|w| lower.contains(w));
    if denied {
        CloudError::new(
            CloudErrorKind::PermissionDenied,
            format!("the Northflank API token may not exec: {message}"),
        )
        .with_action("exec")
    } else {
        CloudError::new(
            CloudErrorKind::Unauthorized,
            format!("Northflank rejected the API token for command execution: {message}"),
        )
    }
}

/// Scrub an error once more with the command's own secrets.
fn rescrub(error: CloudError, secrets: &[&str]) -> CloudError {
    let text = error.to_string();
    if secrets.iter().any(|s| !s.is_empty() && text.contains(s)) {
        let mut clean = CloudError::new(error.kind(), scrub(&text, secrets))
            .retryable(error.is_retryable())
            .with_retry_after(error.retry_after());
        if let Some(status) = error.status() {
            clean = clean.with_status(status);
        }
        if let Some(action) = error.action() {
            clean = clean.with_action(action);
        }
        clean
    } else {
        error
    }
}
