use std::fmt::Write as _;
use std::time::Duration;

use reqwest::{Method, Response, StatusCode};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::time::Instant;

use crate::cloud::{
    CloudError, CloudErrorKind, ProcessExit, ProcessOutput, ProcessSpec, QuotaLimit, SecretString,
};

pub const DEFAULT_API_URL: &str = "https://api.agent37.com/v1";
pub const DEFAULT_EXEC_URL: &str = "https://sandbox.agent37.app/v1";

/// Longest error message kept from an Agent37 response.
const MAX_ERROR_MESSAGE: usize = 512;
/// Wait after a 429 that says nothing about when to come back.
const DEFAULT_RETRY_WAIT: Duration = Duration::from_secs(1);
/// Longest wait between two retries of a transient refusal.
const MAX_BACKOFF: Duration = Duration::from_secs(8);
/// Agent37 holds an exec at most 15 minutes; the client waits a little longer.
const EXEC_GRACE: Duration = Duration::from_secs(30);

#[derive(Debug, Clone)]
pub struct Agent37Config {
    /// REST base, including `/v1`.
    pub api_url: String,
    /// The sandbox host's base, including `/v1`, for exec.
    pub exec_url: String,
    /// Budget for one quick REST call.
    pub request_timeout: Duration,
    pub connect_timeout: Duration,
    /// Budget for one exec without its own timeout.
    pub exec_timeout: Duration,
    /// How long transient refusals (`try_again`, a wake in progress) are
    /// retried before the call fails.
    pub retry_window: Duration,
    /// The first wait between two such retries; it doubles up to 8 s.
    pub retry_interval: Duration,
    /// Budget for a call that waits for Agent37 (a create, a template
    /// import) and for a wake to finish.
    pub ready_timeout: Duration,
    pub poll_interval: Duration,
}

impl Default for Agent37Config {
    fn default() -> Self {
        Self {
            api_url: DEFAULT_API_URL.into(),
            exec_url: DEFAULT_EXEC_URL.into(),
            request_timeout: Duration::from_secs(60),
            connect_timeout: Duration::from_secs(10),
            exec_timeout: Duration::from_secs(900),
            retry_window: Duration::from_secs(120),
            retry_interval: Duration::from_secs(1),
            ready_timeout: Duration::from_secs(600),
            poll_interval: Duration::from_secs(1),
        }
    }
}

/// Client for one Agent37 workspace, authenticated with its API key.
#[derive(Debug)]
pub struct Agent37Client {
    pub(super) http: reqwest::Client,
    pub(super) key: SecretString,
    pub(super) config: Agent37Config,
}

/// What a call is, for its error: the action a 403 names, what a 404
/// means, and whether it waits for Agent37.
#[derive(Debug, Clone, Copy)]
pub(super) struct Call {
    pub action: &'static str,
    pub not_found: CloudErrorKind,
    pub slow: bool,
}

impl Call {
    pub const fn new(action: &'static str) -> Self {
        Self {
            action,
            not_found: CloudErrorKind::NotFound,
            slow: false,
        }
    }

    pub const fn slow(action: &'static str) -> Self {
        Self {
            action,
            not_found: CloudErrorKind::NotFound,
            slow: true,
        }
    }
}

/// The fields of an instance this adapter reads.
#[derive(Debug, Deserialize)]
pub(super) struct InstanceBody {
    pub id: String,
    pub status: String,
    #[serde(default)]
    pub image_ref: Option<String>,
    #[serde(default)]
    pub metadata: Option<Value>,
    #[serde(default)]
    pub created: Option<u64>,
    #[serde(default)]
    pub status_reason: Option<Value>,
}

#[derive(Debug, Deserialize)]
pub(super) struct InstanceList {
    pub data: Vec<InstanceBody>,
}

/// The fields of a template this adapter reads.
#[derive(Debug, Deserialize)]
pub(super) struct TemplateBody {
    pub name: String,
    #[serde(default)]
    pub image_ref: Option<String>,
}

/// A lifecycle ack: `{ id, status }`.
#[derive(Debug, Deserialize)]
pub(super) struct Ack {
    pub status: String,
}

#[derive(Debug, Deserialize)]
pub(super) struct ExecBody {
    pub exit_code: i32,
    #[serde(default)]
    pub stdout: String,
    #[serde(default)]
    pub stderr: String,
    #[serde(default)]
    pub truncated: bool,
}

/// A refusal, as either plane sends it.
struct Refusal {
    status: u16,
    code: String,
    message: String,
    retry_after: Option<Duration>,
}

/// How a refusal is handled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Handling {
    /// Wait and send the same call again, within the retry window.
    Retry,
    Fail(CloudErrorKind, bool),
}

fn handling(status: u16, code: &str) -> Handling {
    use CloudErrorKind as K;
    match (status, code) {
        (401, _) => Handling::Fail(K::Unauthorized, false),
        (402, _) => Handling::Fail(K::Quota(QuotaLimit::Plan), false),
        (403, "forbidden") => Handling::Fail(K::Api, false),
        (403, _) => Handling::Fail(K::PermissionDenied, false),
        (404, _) => Handling::Fail(K::NotFound, false),
        (409, "instance_limit_reached") => Handling::Fail(K::Quota(QuotaLimit::Concurrency), true),
        (409 | 503, "capacity_unavailable" | "no_capacity") => Handling::Fail(K::NoCapacity, true),
        (409, "template_conflict") => Handling::Fail(K::Conflict, false),
        (409 | 503 | 504, "try_again")
        | (503, "wake_failed" | "wake_timeout" | "host_mesh_not_ready" | "instance_saturated")
        | (502, "container_unreachable" | "upstream_unreachable") => Handling::Retry,
        (409, _) => Handling::Fail(K::Conflict, true),
        (429, _) => Handling::Fail(K::Quota(QuotaLimit::Rate), true),
        (500..=599, _) => Handling::Fail(K::Api, true),
        _ => Handling::Fail(K::Api, false),
    }
}

/// `'text'`, safe for `sh`.
fn quote(text: &str) -> String {
    format!("'{}'", text.replace('\'', r"'\''"))
}

/// The `sh -c` line that runs `spec`: its working folder, its environment,
/// then the command and its arguments, each quoted, ended after its
/// timeout.
pub(super) fn command_line(spec: &ProcessSpec) -> Result<String, CloudError> {
    let mut line = String::new();
    if let Some(cwd) = &spec.cwd {
        let _ = write!(line, "cd {} && ", quote(cwd));
    }
    for (name, value) in &spec.envs {
        let ok = !name.is_empty()
            && !name.starts_with(|c: char| c.is_ascii_digit())
            && name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_');
        if !ok {
            return Err(CloudError::new(
                CloudErrorKind::InvalidArgument,
                format!("bad environment variable name {name:?}"),
            ));
        }
        let _ = write!(line, "export {name}={} && ", quote(value.expose()));
    }
    line.push_str("exec ");
    // `--foreground` keeps the command in the caller's process group, so a
    // daemon it starts in the background stays with the machine's others.
    if let Some(timeout) = spec.timeout {
        let _ = write!(
            line,
            "timeout --foreground -k 5 {} ",
            timeout.as_secs().max(1)
        );
    }
    line.push_str(&quote(&spec.cmd));
    for arg in &spec.args {
        line.push(' ');
        line.push_str(&quote(arg));
    }
    Ok(line)
}

impl Agent37Client {
    /// # Errors
    ///
    /// [`CloudErrorKind::NotConfigured`] for an empty key,
    /// [`CloudErrorKind::Other`] when the HTTP client cannot be built.
    pub fn new(key: SecretString, config: Agent37Config) -> Result<Self, CloudError> {
        if key.is_empty() {
            return Err(CloudError::new(
                CloudErrorKind::NotConfigured,
                "the Agent37 API key is not configured",
            ));
        }
        let http = reqwest::Client::builder()
            .connect_timeout(config.connect_timeout)
            .tcp_keepalive(Duration::from_secs(15))
            .build()
            .map_err(|e| CloudError::new(CloudErrorKind::Other, e.to_string()))?;
        Ok(Self { http, key, config })
    }

    fn backoff(&self, attempt: u32) -> Duration {
        self.config
            .retry_interval
            .saturating_mul(1 << attempt.min(4))
            .min(MAX_BACKOFF)
    }

    /// Send a REST call with the bearer key, retrying transient refusals
    /// within the retry window, and map any other failure.
    pub(super) async fn request(
        &self,
        call: Call,
        method: Method,
        path: &str,
        body: Option<&Value>,
    ) -> Result<Response, CloudError> {
        let url = format!("{}{path}", self.config.api_url.trim_end_matches('/'));
        let timeout = if call.slow {
            self.config.ready_timeout
        } else {
            self.config.request_timeout
        };
        let started = Instant::now();
        let mut attempt = 0;
        loop {
            let mut request = self
                .http
                .request(method.clone(), &url)
                .bearer_auth(self.key.expose())
                .timeout(timeout);
            if let Some(body) = body {
                request = request
                    .header("Content-Type", "application/json")
                    .body(body.to_string());
            }
            let response = request.send().await.map_err(|e| self.transport(&e))?;
            if response.status().is_success() {
                return Ok(response);
            }
            let refusal = self.refusal(response).await;
            if let Some(wait) = self.retry_wait(&refusal, started, attempt) {
                attempt += 1;
                tokio::time::sleep(wait).await;
                continue;
            }
            return Err(Self::error(call, &refusal));
        }
    }

    /// How long to wait before sending a refused call again, or `None`
    /// when it fails now.
    fn retry_wait(&self, refusal: &Refusal, started: Instant, attempt: u32) -> Option<Duration> {
        let wait = match handling(refusal.status, &refusal.code) {
            Handling::Retry => self.backoff(attempt),
            Handling::Fail(CloudErrorKind::Quota(QuotaLimit::Rate), _) if attempt < 3 => {
                refusal.retry_after.unwrap_or(DEFAULT_RETRY_WAIT)
            }
            Handling::Fail(..) => return None,
        };
        (started.elapsed() + wait <= self.config.retry_window).then_some(wait)
    }

    pub(super) async fn get_json<T: DeserializeOwned>(
        &self,
        call: Call,
        path: &str,
    ) -> Result<T, CloudError> {
        let response = self.request(call, Method::GET, path, None).await?;
        parse(response, call.action).await
    }

    pub(super) async fn send_json<T: DeserializeOwned>(
        &self,
        call: Call,
        method: Method,
        path: &str,
        body: &Value,
    ) -> Result<T, CloudError> {
        let response = self.request(call, method, path, Some(body)).await?;
        parse(response, call.action).await
    }

    /// `GET /instances/{id}`.
    pub(super) async fn instance(&self, id: &str) -> Result<InstanceBody, CloudError> {
        validate_id(id)?;
        self.get_json(Call::new("read-instance"), &format!("/instances/{id}"))
            .await
    }

    /// Run one command through `POST {exec}/instances/{id}/exec`.
    ///
    /// # Errors
    ///
    /// A refusal the retry window did not clear; a gone instance is
    /// [`CloudErrorKind::NotFound`].
    pub(super) async fn exec_line(
        &self,
        id: &str,
        line: &str,
        user: Option<&str>,
        timeout: Option<Duration>,
    ) -> Result<ProcessOutput, CloudError> {
        validate_id(id)?;
        let url = format!(
            "{}/instances/{id}/exec",
            self.config.exec_url.trim_end_matches('/')
        );
        // The name makes a resend safe: Agent37 answers it with the first
        // run's result instead of running the command again.
        let name = format!("intent-{}", uuid::Uuid::new_v4().simple());
        let mut body = json!({ "command": line, "name": name });
        if let Some(user) = user {
            body["user"] = json!(user);
        }
        let budget = timeout
            .unwrap_or(self.config.exec_timeout)
            .min(self.config.exec_timeout)
            + EXEC_GRACE;
        let started = Instant::now();
        let mut attempt = 0;
        loop {
            let sent = self
                .http
                .post(&url)
                .header("X-Agent37-Key", self.key.expose())
                .header("Content-Type", "application/json")
                .timeout(budget)
                .body(body.to_string())
                .send()
                .await;
            let response = match sent {
                Ok(response) => response,
                Err(e) => {
                    let error = self.transport(&e);
                    let next = self.backoff(attempt);
                    if error.is_retryable() && started.elapsed() + next <= self.config.retry_window
                    {
                        attempt += 1;
                        tokio::time::sleep(next).await;
                        continue;
                    }
                    return Err(error);
                }
            };
            if response.status().is_success() {
                let out: ExecBody = parse(response, "exec").await?;
                return Ok(ProcessOutput {
                    pid: None,
                    exit: ProcessExit {
                        exit_code: out.exit_code,
                        exited: true,
                        status: format!("exit status {}", out.exit_code),
                        error: None,
                    },
                    stdout: out.stdout.into_bytes(),
                    stderr: out.stderr.into_bytes(),
                    truncated: out.truncated,
                });
            }
            let refusal = self.refusal(response).await;
            let mut wait = self.retry_wait(&refusal, started, attempt);
            // A wake that another call started answers 400: wait for it.
            if wait.is_none() && refusal.status == 400 && self.waking(id).await {
                let next = self.backoff(attempt);
                wait = (started.elapsed() + next <= self.config.retry_window).then_some(next);
            }
            if let Some(wait) = wait {
                attempt += 1;
                tokio::time::sleep(wait).await;
                continue;
            }
            return Err(Self::error(Call::new("exec"), &refusal));
        }
    }

    /// Whether instance `id` is between two states.
    async fn waking(&self, id: &str) -> bool {
        self.instance(id).await.is_ok_and(|i| {
            matches!(
                i.status.as_str(),
                "waking" | "starting" | "restarting" | "updating" | "provisioning"
            )
        })
    }

    /// Run `spec` in instance `id`.
    ///
    /// # Errors
    ///
    /// [`CloudErrorKind::InvalidArgument`] for a user other than the
    /// image's own or `root`; else as [`Self::exec_line`].
    pub(super) async fn exec_spec(
        &self,
        id: &str,
        spec: &ProcessSpec,
    ) -> Result<ProcessOutput, CloudError> {
        let user = match spec.user.as_deref() {
            None => None,
            Some(user) if user == super::provider::RUN_AS => None,
            Some("root") => Some("root"),
            Some(other) => {
                return Err(CloudError::new(
                    CloudErrorKind::InvalidArgument,
                    format!("Agent37 runs commands as the image's user or root, not {other:?}"),
                ))
            }
        };
        let line = command_line(spec)?;
        self.exec_line(id, &line, user, spec.timeout).await
    }

    async fn refusal(&self, response: Response) -> Refusal {
        let status = response.status().as_u16();
        let retry_after = retry_after(&response);
        let bytes = response.bytes().await.unwrap_or_default();
        let value: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
        let (code, message) = match value.get("error") {
            Some(Value::String(code)) => (
                code.clone(),
                value
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or(code)
                    .to_owned(),
            ),
            Some(error) => (
                error
                    .get("code")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
                error
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
            ),
            None => (
                String::new(),
                String::from_utf8_lossy(&bytes).trim().to_owned(),
            ),
        };
        Refusal {
            status,
            code,
            message: self.scrub(&message),
            retry_after,
        }
    }

    fn error(call: Call, refusal: &Refusal) -> CloudError {
        let status = refusal.status;
        let action = call.action;
        let detail = if refusal.code.is_empty() {
            refusal.message.clone()
        } else {
            format!("{}: {}", refusal.code, refusal.message)
        };
        let (kind, retryable) = match handling(status, &refusal.code) {
            Handling::Fail(CloudErrorKind::NotFound, r) => (call.not_found, r),
            Handling::Fail(kind, r) => (kind, r),
            Handling::Retry => (CloudErrorKind::Api, true),
        };
        let text = match kind {
            CloudErrorKind::Unauthorized => {
                format!("Agent37 rejected the API key (HTTP {status}): {detail}")
            }
            CloudErrorKind::PermissionDenied => {
                format!("Agent37 refused {action} for this key (HTTP {status}): {detail}")
            }
            _ => format!("Agent37 refused {action} (HTTP {status}): {detail}"),
        };
        let mut error = CloudError::new(kind, text)
            .with_status(status)
            .with_retry_after(refusal.retry_after)
            .retryable(retryable);
        if kind == CloudErrorKind::PermissionDenied {
            error = error.with_action(action);
        }
        error
    }

    pub(super) fn transport(&self, error: &reqwest::Error) -> CloudError {
        let message = self.scrub(&error.to_string());
        if error.is_connect() || error.is_timeout() {
            CloudError::new(
                CloudErrorKind::Other,
                format!("Agent37 is unreachable: {message}"),
            )
            .retryable(true)
        } else {
            CloudError::new(
                CloudErrorKind::Other,
                format!("the Agent37 request failed: {message}"),
            )
        }
    }

    /// Remove the key from a server-supplied message, then bound its length.
    pub(super) fn scrub(&self, message: &str) -> String {
        scrub(message, self.key.expose())
    }
}

pub(super) fn scrub(message: &str, key: &str) -> String {
    let out = if key.is_empty() {
        message.to_owned()
    } else {
        message.replace(key, "<redacted>")
    };
    out.chars()
        .map(|c| if c.is_control() { ' ' } else { c })
        .take(MAX_ERROR_MESSAGE)
        .collect()
}

fn retry_after(response: &Response) -> Option<Duration> {
    if response.status() != StatusCode::TOO_MANY_REQUESTS {
        return None;
    }
    response
        .headers()
        .get("retry-after")?
        .to_str()
        .ok()?
        .trim()
        .parse::<u64>()
        .ok()
        .map(Duration::from_secs)
}

pub(super) async fn parse<T: DeserializeOwned>(
    response: Response,
    what: &str,
) -> Result<T, CloudError> {
    let bytes = response.bytes().await.map_err(|e| {
        CloudError::new(
            CloudErrorKind::Other,
            format!("the Agent37 response broke off: {}", e.without_url()),
        )
    })?;
    serde_json::from_slice(&bytes).map_err(|e| {
        CloudError::new(
            CloudErrorKind::Other,
            format!(
                "unexpected Agent37 {what} response: malformed at line {} column {}",
                e.line(),
                e.column()
            ),
        )
    })
}

/// An Agent37 instance id: lowercase letters and digits.
pub(super) fn validate_id(id: &str) -> Result<(), CloudError> {
    let ok = !id.is_empty()
        && id.len() <= 64
        && id
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit());
    if ok {
        Ok(())
    } else {
        Err(CloudError::new(
            CloudErrorKind::InvalidArgument,
            format!("bad Agent37 instance id {id:?}"),
        ))
    }
}
