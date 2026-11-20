use std::collections::BTreeMap;
use std::time::Duration;

use reqwest::{Method, Response, StatusCode};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::time::Instant;

use crate::cloud::{
    CloudError, CloudErrorKind, ProcessExit, ProcessOutput, ProcessSpec, QuotaLimit, SecretString,
};

pub const DEFAULT_API_URL: &str = "https://api.vercel.com";
pub const DEFAULT_REGISTRY_URL: &str = "https://vcr.vercel.com";

/// What a manifest lookup accepts: an index (the published image is one)
/// or a single manifest, OCI or Docker.
pub(super) const MANIFEST_ACCEPT: &str = "application/vnd.oci.image.index.v1+json, application/vnd.docker.distribution.manifest.list.v2+json, application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json";

/// Longest error message kept from a Vercel response.
const MAX_ERROR_MESSAGE: usize = 512;
/// Most bytes kept of each output stream of a command.
const MAX_OUTPUT: usize = 512 * 1024;
/// Wait after a 429 that says nothing about when to come back.
const DEFAULT_RETRY_WAIT: Duration = Duration::from_secs(1);
/// Longest wait between two retries of a transient refusal.
const MAX_BACKOFF: Duration = Duration::from_secs(8);
/// A command gets this much longer than its own timeout.
const COMMAND_GRACE: Duration = Duration::from_secs(30);

#[derive(Debug, Clone)]
pub struct VercelConfig {
    /// The team every call is scoped to (`teamId`).
    pub team_id: String,
    /// The project the sandboxes run in.
    pub project_id: String,
    pub api_url: String,
    /// The container registry, for the image check.
    pub registry_url: String,
    /// Budget for one quick REST call.
    pub request_timeout: Duration,
    pub connect_timeout: Duration,
    /// Budget for one command without its own timeout.
    pub command_timeout: Duration,
    /// Budget for a call that waits for Vercel: a create, a stop (which
    /// snapshots the disk) and a resume.
    pub slow_timeout: Duration,
    /// How long transient refusals are retried before the call fails.
    pub retry_window: Duration,
    /// The first wait between two such retries; it doubles up to 8 s.
    pub retry_interval: Duration,
    /// Budget for a sandbox to reach `running`.
    pub ready_timeout: Duration,
    pub poll_interval: Duration,
}

impl Default for VercelConfig {
    fn default() -> Self {
        Self {
            team_id: String::new(),
            project_id: String::new(),
            api_url: DEFAULT_API_URL.into(),
            registry_url: DEFAULT_REGISTRY_URL.into(),
            request_timeout: Duration::from_secs(60),
            connect_timeout: Duration::from_secs(10),
            command_timeout: Duration::from_secs(900),
            slow_timeout: Duration::from_secs(300),
            retry_window: Duration::from_secs(120),
            retry_interval: Duration::from_secs(1),
            ready_timeout: Duration::from_secs(300),
            poll_interval: Duration::from_secs(1),
        }
    }
}

/// Client for one Vercel project, authenticated with an access token.
#[derive(Debug)]
pub struct VercelClient {
    pub(super) http: reqwest::Client,
    pub(super) token: SecretString,
    pub(super) config: VercelConfig,
}

/// What a call is, for its error: the action a 403 names, and whether it
/// waits for Vercel or may be sent again.
#[derive(Debug, Clone, Copy)]
pub(super) struct Call {
    pub action: &'static str,
    pub slow: bool,
    /// Whether a transient refusal may be retried (the call is safe to
    /// send twice).
    pub retry: bool,
}

impl Call {
    pub const fn new(action: &'static str) -> Self {
        Self {
            action,
            slow: false,
            retry: true,
        }
    }

    pub const fn slow(action: &'static str) -> Self {
        Self {
            action,
            slow: true,
            retry: true,
        }
    }

    pub const fn once(self) -> Self {
        Self {
            retry: false,
            ..self
        }
    }
}

/// The fields of a sandbox this adapter reads.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct SandboxBody {
    pub name: String,
    pub status: String,
    #[serde(default)]
    pub tags: BTreeMap<String, String>,
    #[serde(default)]
    pub created_at: Option<u64>,
    #[serde(default)]
    pub image: Option<String>,
}

/// The fields of a session this adapter reads.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct SessionBody {
    pub id: String,
    pub status: String,
    #[serde(default)]
    pub started_at: Option<u64>,
    /// Milliseconds from `started_at` to the session's end.
    pub timeout: u64,
}

/// A sandbox with its current session (create, read, resume, stop).
#[derive(Debug, Deserialize)]
pub(super) struct SessionReply {
    pub sandbox: SandboxBody,
    pub session: SessionBody,
    #[serde(default)]
    pub resumed: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub(super) struct Pagination {
    pub next: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(super) struct SandboxList {
    pub sandboxes: Vec<SandboxBody>,
    pub pagination: Pagination,
}

#[derive(Debug, Deserialize)]
pub(super) struct ExtendReply {
    pub session: SessionBody,
}

/// The command of a command stream's first and last lines.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct CommandBody {
    pub exit_code: Option<i32>,
}

/// One line of a command stream: the command, or output.
#[derive(Debug, Deserialize)]
pub(super) struct LogLine {
    #[serde(default)]
    pub command: Option<CommandBody>,
    #[serde(default)]
    pub stream: Option<String>,
    #[serde(default)]
    pub data: Option<Value>,
}

/// A refusal.
struct Refusal {
    status: u16,
    code: String,
    message: String,
    invalid_token: bool,
    retry_after: Option<Duration>,
}

/// How a refusal is handled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Handling {
    /// Wait and send the same call again, within the retry window.
    Retry,
    Fail(CloudErrorKind, bool),
}

fn handling(refusal: &Refusal) -> Handling {
    use CloudErrorKind as K;
    match (refusal.status, refusal.code.as_str()) {
        (401, _) => Handling::Fail(K::Unauthorized, false),
        (403, _) if refusal.invalid_token => Handling::Fail(K::Unauthorized, false),
        (403, _) => Handling::Fail(K::PermissionDenied, false),
        (404, _) if refusal.message.starts_with("Could not find project") => {
            Handling::Fail(K::ProjectMissing, false)
        }
        // A stopped session takes no command or extension: it is not
        // running, as a gone one.
        (404 | 410, _) => Handling::Fail(K::NotFound, false),
        (400, "sandbox_timeout_invalid") => Handling::Fail(K::SessionCap, false),
        (400, "bad_request") if refusal.message.contains("resources.vcpus") => {
            Handling::Fail(K::Quota(QuotaLimit::Plan), false)
        }
        (409, _) => Handling::Fail(K::Conflict, true),
        (429, _) => Handling::Fail(K::Quota(QuotaLimit::Rate), true),
        (502..=504, _) => Handling::Retry,
        (500..=599, _) => Handling::Fail(K::Api, true),
        _ => Handling::Fail(K::Api, false),
    }
}

/// The current time, in epoch milliseconds.
pub(super) fn now_ms() -> u64 {
    u64::try_from(time::OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000)
        .unwrap_or_default()
}

impl VercelClient {
    /// # Errors
    ///
    /// [`CloudErrorKind::NotConfigured`] for an empty token,
    /// [`CloudErrorKind::Other`] when the HTTP client cannot be built.
    pub fn new(token: SecretString, config: VercelConfig) -> Result<Self, CloudError> {
        if token.is_empty() {
            return Err(CloudError::new(
                CloudErrorKind::NotConfigured,
                "the Vercel access token is not configured",
            ));
        }
        let http = reqwest::Client::builder()
            .connect_timeout(config.connect_timeout)
            .tcp_keepalive(Duration::from_secs(15))
            .build()
            .map_err(|e| CloudError::new(CloudErrorKind::Other, e.to_string()))?;
        Ok(Self {
            http,
            token,
            config,
        })
    }

    fn backoff(&self, attempt: u32) -> Duration {
        self.config
            .retry_interval
            .saturating_mul(1 << attempt.min(4))
            .min(MAX_BACKOFF)
    }

    /// `path` with the team: every REST call is scoped to it.
    fn url(&self, path: &str) -> String {
        let sep = if path.contains('?') { '&' } else { '?' };
        format!(
            "{}{path}{sep}teamId={}",
            self.config.api_url.trim_end_matches('/'),
            self.config.team_id
        )
    }

    /// Send a REST call with the bearer token, retrying transient refusals
    /// within the retry window, and map any other failure.
    pub(super) async fn request(
        &self,
        call: Call,
        method: Method,
        path: &str,
        body: Option<&Value>,
        timeout: Option<Duration>,
    ) -> Result<Response, CloudError> {
        let url = self.url(path);
        let timeout = timeout.unwrap_or(if call.slow {
            self.config.slow_timeout
        } else {
            self.config.request_timeout
        });
        let started = Instant::now();
        let mut attempt = 0;
        loop {
            // Vercel answers 415 to a POST without a JSON content type,
            // even with no body.
            let request = self
                .http
                .request(method.clone(), &url)
                .bearer_auth(self.token.expose())
                .header("Content-Type", "application/json")
                .timeout(timeout)
                .body(body.map(Value::to_string).unwrap_or_default());
            let response = match request.send().await {
                Ok(response) => response,
                Err(e) => {
                    let error = self.transport(&e);
                    let next = self.backoff(attempt);
                    if call.retry
                        && error.is_retryable()
                        && started.elapsed() + next <= self.config.retry_window
                    {
                        attempt += 1;
                        tokio::time::sleep(next).await;
                        continue;
                    }
                    return Err(error);
                }
            };
            if response.status().is_success() {
                return Ok(response);
            }
            let refusal = self.refusal(response).await;
            if let Some(wait) = self
                .retry_wait(&refusal, started, attempt)
                .filter(|_| call.retry)
            {
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
        let wait = match handling(refusal) {
            Handling::Retry | Handling::Fail(CloudErrorKind::Api, true) => self.backoff(attempt),
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
        let response = self.request(call, Method::GET, path, None, None).await?;
        parse(response, call.action).await
    }

    pub(super) async fn send_json<T: DeserializeOwned>(
        &self,
        call: Call,
        method: Method,
        path: &str,
        body: Option<&Value>,
    ) -> Result<T, CloudError> {
        let response = self.request(call, method, path, body, None).await?;
        parse(response, call.action).await
    }

    /// `GET /v2/sandboxes/{name}`, resuming a stopped one when `resume`.
    pub(super) async fn sandbox(
        &self,
        name: &str,
        resume: bool,
    ) -> Result<SessionReply, CloudError> {
        validate_name(name)?;
        let mut path = format!("/v2/sandboxes/{name}?projectId={}", self.config.project_id);
        let call = if resume {
            path.push_str("&resume=true");
            Call::slow("resume")
        } else {
            Call::new("read-sandbox")
        };
        self.get_json(call, &path).await
    }

    /// Run `spec` in the running session of sandbox `name`.
    ///
    /// # Errors
    ///
    /// [`CloudErrorKind::InvalidArgument`] for a user other than the
    /// image's own (the image has no `sudo`); [`CloudErrorKind::NotFound`]
    /// when the sandbox is gone or not running.
    pub(super) async fn command(
        &self,
        name: &str,
        spec: &ProcessSpec,
    ) -> Result<ProcessOutput, CloudError> {
        match spec.user.as_deref() {
            None => {}
            Some(user) if user == super::provider::RUN_AS => {}
            Some(other) => {
                return Err(CloudError::new(
                    CloudErrorKind::InvalidArgument,
                    format!("Vercel runs commands as the image's user only, not {other:?}"),
                ))
            }
        }
        let current = self.sandbox(name, false).await?;
        if current.session.status != "running" {
            return Err(CloudError::new(
                CloudErrorKind::NotFound,
                format!("the Vercel sandbox {name} is {}", current.session.status),
            ));
        }
        let clamped = spec.timeout.map(|t| t.min(self.config.command_timeout));
        let (command, args) = match clamped {
            // `--foreground` keeps the command in the caller's process
            // group, so a daemon it starts stays with the others.
            Some(timeout) => {
                let mut args = vec![
                    "--foreground".to_owned(),
                    "-k".to_owned(),
                    "5".to_owned(),
                    timeout.as_secs().max(1).to_string(),
                    spec.cmd.clone(),
                ];
                args.extend(spec.args.iter().cloned());
                ("timeout".to_owned(), args)
            }
            None => (spec.cmd.clone(), spec.args.clone()),
        };
        let env: serde_json::Map<String, Value> = spec
            .envs
            .iter()
            .map(|(k, v)| (k.clone(), json!(v.expose())))
            .collect();
        let mut body = json!({
            "command": command,
            "args": args,
            "env": env,
            "wait": true,
            "logs": true,
        });
        if let Some(cwd) = &spec.cwd {
            body["cwd"] = json!(cwd);
        }
        let budget = clamped.unwrap_or(self.config.command_timeout) + COMMAND_GRACE;
        let path = format!("/v2/sandboxes/sessions/{}/cmd", current.session.id);
        validate_session(&current.session.id)?;
        let response = self
            .request(
                Call::new("run-command").once(),
                Method::POST,
                &path,
                Some(&body),
                Some(budget),
            )
            .await?;
        self.read_stream(response).await
    }

    /// Read a command's newline-delimited stream to its end.
    async fn read_stream(&self, response: Response) -> Result<ProcessOutput, CloudError> {
        let mut out = ProcessOutput {
            pid: None,
            exit: ProcessExit {
                exit_code: -1,
                exited: false,
                status: String::new(),
                error: None,
            },
            stdout: Vec::new(),
            stderr: Vec::new(),
            truncated: false,
        };
        let mut pending = Vec::new();
        let mut response = response;
        let mut finished = false;
        loop {
            let chunk = response.chunk().await.map_err(|e| {
                CloudError::new(
                    CloudErrorKind::Other,
                    format!("the Vercel command stream broke off: {}", e.without_url()),
                )
                .retryable(true)
            })?;
            let Some(chunk) = chunk else { break };
            pending.extend_from_slice(&chunk);
            while let Some(at) = pending.iter().position(|b| *b == b'\n') {
                let line: Vec<u8> = pending.drain(..=at).collect();
                finished |= self.take_line(&line, &mut out)?;
            }
        }
        if !pending.is_empty() {
            finished |= self.take_line(&pending, &mut out)?;
        }
        if !finished {
            return Err(CloudError::new(
                CloudErrorKind::Other,
                "the Vercel command stream ended before the command finished",
            )
            .retryable(true));
        }
        Ok(out)
    }

    /// One stream line; `true` once the command finished.
    fn take_line(&self, line: &[u8], out: &mut ProcessOutput) -> Result<bool, CloudError> {
        if line.iter().all(u8::is_ascii_whitespace) {
            return Ok(false);
        }
        let parsed: LogLine = serde_json::from_slice(line).map_err(|e| {
            CloudError::new(
                CloudErrorKind::Other,
                format!(
                    "unexpected Vercel command stream: malformed at column {}",
                    e.column()
                ),
            )
        })?;
        if let Some(command) = parsed.command {
            if let Some(code) = command.exit_code {
                out.exit = ProcessExit {
                    exit_code: code,
                    exited: true,
                    status: format!("exit status {code}"),
                    error: None,
                };
                return Ok(true);
            }
            return Ok(false);
        }
        let text = |data: Option<&Value>| match data {
            Some(Value::String(text)) => text.clone(),
            Some(other) => other.to_string(),
            None => String::new(),
        };
        match parsed.stream.as_deref() {
            Some("stdout") => append(
                &mut out.stdout,
                &text(parsed.data.as_ref()),
                &mut out.truncated,
            ),
            Some("stderr") => append(
                &mut out.stderr,
                &text(parsed.data.as_ref()),
                &mut out.truncated,
            ),
            Some("error") => {
                let data = parsed.data.unwrap_or(Value::Null);
                let code = data.get("code").and_then(Value::as_str).unwrap_or("error");
                let message = data
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                return Err(CloudError::new(
                    CloudErrorKind::Other,
                    format!("the Vercel command failed: {code}: {}", self.scrub(message)),
                )
                .retryable(true));
            }
            _ => {}
        }
        Ok(false)
    }

    /// `HEAD` the manifest of `image` (`<repository>:<tag>`) with the team
    /// and token as basic credentials: `Some(true)` found, `Some(false)`
    /// missing, `None` when the registry does not answer this team for
    /// the repository.
    pub(super) async fn manifest(&self, image: &str) -> Result<Option<bool>, CloudError> {
        let (repository, tag) = image.rsplit_once(':').ok_or_else(|| {
            CloudError::new(
                CloudErrorKind::InvalidArgument,
                format!("the image {image:?} has no tag"),
            )
        })?;
        let url = format!(
            "{}/v2/{repository}/manifests/{tag}",
            self.config.registry_url.trim_end_matches('/')
        );
        let response = self
            .http
            .head(url)
            .basic_auth(&self.config.team_id, Some(self.token.expose()))
            .header("Accept", MANIFEST_ACCEPT)
            .timeout(self.config.request_timeout)
            .send()
            .await
            .map_err(|e| self.transport(&e))?;
        match response.status().as_u16() {
            200..=299 => Ok(Some(true)),
            404 => Ok(Some(false)),
            401 | 403 => Ok(None),
            status => Err(CloudError::new(
                CloudErrorKind::Api,
                format!("the Vercel registry answered HTTP {status} for {image}"),
            )
            .with_status(status)
            .retryable(status >= 500)),
        }
    }

    async fn refusal(&self, response: Response) -> Refusal {
        let status = response.status().as_u16();
        let retry_after = retry_after(&response);
        let bytes = response.bytes().await.unwrap_or_default();
        let value: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
        let error = value.get("error").cloned().unwrap_or(Value::Null);
        let text = |key: &str| {
            error
                .get(key)
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned()
        };
        let message = match text("message") {
            m if m.is_empty() && error.is_null() => {
                String::from_utf8_lossy(&bytes).trim().to_owned()
            }
            m => m,
        };
        Refusal {
            status,
            code: text("code"),
            message: self.scrub(&message),
            invalid_token: error.get("invalidToken").and_then(Value::as_bool) == Some(true),
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
        let (kind, retryable) = match handling(refusal) {
            Handling::Fail(kind, r) => (kind, r),
            Handling::Retry => (CloudErrorKind::Api, true),
        };
        let text = match kind {
            CloudErrorKind::Unauthorized => {
                format!("Vercel rejected the access token (HTTP {status}): {detail}")
            }
            CloudErrorKind::PermissionDenied => {
                format!("Vercel refused {action} for this token (HTTP {status}): {detail}")
            }
            _ => format!("Vercel refused {action} (HTTP {status}): {detail}"),
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
                format!("Vercel is unreachable: {message}"),
            )
            .retryable(true)
        } else {
            CloudError::new(
                CloudErrorKind::Other,
                format!("the Vercel request failed: {message}"),
            )
        }
    }

    /// Remove the token from a server-supplied message, then bound its
    /// length.
    pub(super) fn scrub(&self, message: &str) -> String {
        scrub(message, self.token.expose())
    }
}

fn append(buffer: &mut Vec<u8>, text: &str, truncated: &mut bool) {
    let room = MAX_OUTPUT.saturating_sub(buffer.len());
    if text.len() > room {
        *truncated = true;
    }
    buffer.extend_from_slice(&text.as_bytes()[..text.len().min(room)]);
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
            format!("the Vercel response broke off: {}", e.without_url()),
        )
    })?;
    serde_json::from_slice(&bytes).map_err(|e| {
        CloudError::new(
            CloudErrorKind::Other,
            format!(
                "unexpected Vercel {what} response: malformed at line {} column {}",
                e.line(),
                e.column()
            ),
        )
    })
}

fn id_like(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 128
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
}

/// A sandbox name, safe in a path.
pub(super) fn validate_name(name: &str) -> Result<(), CloudError> {
    if id_like(name) {
        Ok(())
    } else {
        Err(CloudError::new(
            CloudErrorKind::InvalidArgument,
            format!("bad Vercel sandbox name {name:?}"),
        ))
    }
}

pub(super) fn validate_session(id: &str) -> Result<(), CloudError> {
    if id_like(id) {
        Ok(())
    } else {
        Err(CloudError::new(
            CloudErrorKind::Other,
            format!("Vercel returned a bad session id {id:?}"),
        ))
    }
}
