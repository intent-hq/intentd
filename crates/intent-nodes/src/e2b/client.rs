use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::time::Duration;

use reqwest::{RequestBuilder, Response, StatusCode};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tokio::time::Instant;

use super::{E2bError, SecretString};

pub const DEFAULT_API_URL: &str = "https://api.e2b.app";
pub const DEFAULT_ENVD_URL: &str = "https://sandbox.e2b.app";

/// Longest error message kept from an E2B response body.
const MAX_ERROR_MESSAGE: usize = 512;
/// Guard against a server that keeps handing out page tokens.
const MAX_LIST_PAGES: usize = 1000;

#[derive(Debug, Clone)]
pub struct E2bConfig {
    pub api_url: String,
    /// envd entry point; requests are routed by the `E2b-Sandbox-Id` header.
    pub envd_url: String,
    /// Budget for one REST call. Process streams have no default budget.
    pub request_timeout: Duration,
    pub connect_timeout: Duration,
    /// Retries after a 429 (or a create still in progress) before giving up.
    pub max_rate_limit_retries: u32,
    /// Upper bound for one wait, whatever `Retry-After` asks for.
    pub max_retry_wait: Duration,
    /// Wait used when a 429 has no usable `Retry-After`.
    pub default_retry_wait: Duration,
    /// Least time between two creates (Hobby allows 1 create/s).
    pub create_spacing: Duration,
}

impl Default for E2bConfig {
    fn default() -> Self {
        Self {
            api_url: DEFAULT_API_URL.into(),
            envd_url: DEFAULT_ENVD_URL.into(),
            request_timeout: Duration::from_secs(120),
            connect_timeout: Duration::from_secs(10),
            max_rate_limit_retries: 5,
            max_retry_wait: Duration::from_secs(30),
            default_retry_wait: Duration::from_secs(1),
            create_spacing: Duration::from_secs(1),
        }
    }
}

/// Client for one E2B team, authenticated with its API key.
#[derive(Debug)]
pub struct E2bClient {
    pub(super) http: reqwest::Client,
    pub(super) key: SecretString,
    pub(super) config: E2bConfig,
    /// Held across a whole create so creates never overlap; holds the end
    /// time of the last one for spacing.
    create_gate: Mutex<Option<Instant>>,
}

#[derive(Debug, Clone, Default)]
pub struct CreateSandbox {
    pub template_id: String,
    /// Seconds until E2B pauses or kills the sandbox (E2B default 300).
    pub timeout_secs: Option<u32>,
    /// Pause instead of kill when the timeout fires.
    pub auto_pause: Option<bool>,
    pub metadata: BTreeMap<String, String>,
    /// `[a-z0-9-]{1,255}`; E2B keeps it for 24 h. Reuse it on every retry.
    pub idempotency_key: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CreateBody<'a> {
    #[serde(rename = "templateID")]
    template_id: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    timeout: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    auto_pause: Option<bool>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    metadata: &'a BTreeMap<String, String>,
}

#[derive(Serialize)]
struct TimeoutBody {
    timeout: u32,
}

/// A sandbox as returned by create and connect.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Sandbox {
    #[serde(rename = "sandboxID")]
    pub sandbox_id: String,
    #[serde(rename = "templateID")]
    pub template_id: String,
    #[serde(default)]
    pub alias: Option<String>,
    #[serde(default)]
    pub envd_version: String,
    /// Authenticates envd calls (`X-Access-Token`).
    #[serde(default)]
    pub envd_access_token: Option<SecretString>,
    #[serde(default)]
    pub traffic_access_token: Option<SecretString>,
    #[serde(default)]
    pub domain: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ConnectedSandbox {
    pub sandbox: Sandbox,
    /// `true` when the sandbox was paused and E2B resumed it.
    pub resumed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SandboxState {
    Running,
    Paused,
    /// A state this client does not know.
    #[serde(other)]
    Other,
}

impl SandboxState {
    fn as_query(self) -> Option<&'static str> {
        match self {
            Self::Running => Some("running"),
            Self::Paused => Some("paused"),
            Self::Other => None,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct ListSandboxes {
    /// Every pair must match.
    pub metadata: BTreeMap<String, String>,
    /// Empty means the E2B default.
    pub states: Vec<SandboxState>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListedSandbox {
    #[serde(rename = "sandboxID")]
    pub sandbox_id: String,
    #[serde(rename = "templateID")]
    pub template_id: String,
    #[serde(default)]
    pub alias: Option<String>,
    pub state: SandboxState,
    #[serde(default)]
    pub metadata: BTreeMap<String, String>,
    #[serde(default)]
    pub started_at: String,
    #[serde(default)]
    pub end_at: String,
    #[serde(default)]
    pub cpu_count: u32,
    #[serde(default, rename = "memoryMB")]
    pub memory_mb: u32,
    #[serde(default)]
    pub envd_version: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Op {
    Create,
    Connect,
    Pause,
    Timeout,
    Delete,
    List,
    Process,
}

#[derive(Deserialize)]
struct ErrorBody {
    #[serde(default)]
    message: Option<String>,
    #[serde(default)]
    error_code: Option<String>,
}

impl E2bClient {
    /// # Errors
    ///
    /// [`E2bError::NotConfigured`] for an empty key, [`E2bError::Transport`]
    /// when the HTTP client cannot be built.
    pub fn new(key: SecretString, config: E2bConfig) -> Result<Self, E2bError> {
        if key.is_empty() {
            return Err(E2bError::NotConfigured);
        }
        let http = reqwest::Client::builder()
            .connect_timeout(config.connect_timeout)
            .build()
            .map_err(|e| E2bError::Transport(e.to_string()))?;
        Ok(Self {
            http,
            key,
            config,
            create_gate: Mutex::new(None),
        })
    }

    /// Create a sandbox. Creates are serialized and spaced by
    /// [`E2bConfig::create_spacing`]. A 409 "in progress" is retried with the
    /// same idempotency key.
    ///
    /// # Errors
    ///
    /// [`E2bError::IdempotencyOutcomeUnknown`] on 422: look the sandbox up with
    /// [`Self::list`] before a new create. Other HTTP failures map to their
    /// [`E2bError`] variants.
    pub async fn create(&self, request: &CreateSandbox) -> Result<Sandbox, E2bError> {
        if request.template_id.is_empty() {
            return Err(E2bError::InvalidArgument("templateID is empty".into()));
        }
        if request.timeout_secs == Some(0) {
            return Err(E2bError::InvalidArgument(
                "timeout must be at least 1 s".into(),
            ));
        }
        if let Some(key) = &request.idempotency_key {
            validate_idempotency_key(key)?;
        }
        let body = serde_json::to_vec(&CreateBody {
            template_id: &request.template_id,
            timeout: request.timeout_secs,
            auto_pause: request.auto_pause,
            metadata: &request.metadata,
        })
        .map_err(|e| E2bError::InvalidArgument(e.to_string()))?;
        let url = format!("{}/v2/sandboxes", self.api());

        let mut gate = self.create_gate.lock().await;
        if let Some(last) = *gate {
            tokio::time::sleep_until(last + self.config.create_spacing).await;
        }
        let result = self
            .execute(Op::Create, &[], || {
                let req = self.rest(self.http.post(&url)).json_body(body.clone());
                match &request.idempotency_key {
                    Some(key) => req.header("Idempotency-Key", key),
                    None => req,
                }
            })
            .await;
        *gate = Some(Instant::now());
        drop(gate);
        parse_json(result?, "create").await
    }

    /// Connect to a sandbox, resuming it when paused. E2B only extends the
    /// timeout to at least `timeout_secs`; use [`Self::set_timeout`] to shorten it.
    ///
    /// # Errors
    ///
    /// [`E2bError::NotFound`] when the sandbox is gone, [`E2bError::Conflict`]
    /// while it changes state.
    pub async fn connect(
        &self,
        sandbox_id: &str,
        timeout_secs: u32,
    ) -> Result<ConnectedSandbox, E2bError> {
        validate_sandbox_id(sandbox_id)?;
        if timeout_secs == 0 {
            return Err(E2bError::InvalidArgument(
                "timeout must be at least 1 s".into(),
            ));
        }
        let url = format!("{}/v2/sandboxes/{sandbox_id}/connect", self.api());
        let body = serde_json::to_vec(&TimeoutBody {
            timeout: timeout_secs,
        })
        .map_err(|e| E2bError::InvalidArgument(e.to_string()))?;
        let response = self
            .execute(Op::Connect, &[], || {
                self.rest(self.http.post(&url)).json_body(body.clone())
            })
            .await?;
        let resumed = response.status() == StatusCode::CREATED;
        let sandbox = parse_json(response, "connect").await?;
        Ok(ConnectedSandbox { sandbox, resumed })
    }

    /// Pause a running sandbox, keeping its memory.
    ///
    /// # Errors
    ///
    /// [`E2bError::PauseRefused`] on 503 (node busy, retry later),
    /// [`E2bError::Conflict`] when the state does not allow a pause.
    pub async fn pause(&self, sandbox_id: &str) -> Result<(), E2bError> {
        validate_sandbox_id(sandbox_id)?;
        let url = format!("{}/sandboxes/{sandbox_id}/pause", self.api());
        self.execute(Op::Pause, &[], || self.rest(self.http.post(&url)))
            .await
            .map(drop)
    }

    /// Set the sandbox to expire `timeout_secs` from now (replaces the old value).
    ///
    /// # Errors
    ///
    /// [`E2bError::NotFound`], [`E2bError::Conflict`] or an API error.
    pub async fn set_timeout(&self, sandbox_id: &str, timeout_secs: u32) -> Result<(), E2bError> {
        validate_sandbox_id(sandbox_id)?;
        let url = format!("{}/sandboxes/{sandbox_id}/timeout", self.api());
        let body = serde_json::to_vec(&TimeoutBody {
            timeout: timeout_secs,
        })
        .map_err(|e| E2bError::InvalidArgument(e.to_string()))?;
        self.execute(Op::Timeout, &[], || {
            self.rest(self.http.post(&url)).json_body(body.clone())
        })
        .await
        .map(drop)
    }

    /// Kill a sandbox. Returns `false` when it was already gone.
    ///
    /// # Errors
    ///
    /// [`E2bError::Conflict`] while it resumes, or an API error.
    pub async fn delete(&self, sandbox_id: &str) -> Result<bool, E2bError> {
        validate_sandbox_id(sandbox_id)?;
        let url = format!("{}/sandboxes/{sandbox_id}", self.api());
        match self
            .execute(Op::Delete, &[], || self.rest(self.http.delete(&url)))
            .await
        {
            Ok(_) => Ok(true),
            Err(E2bError::NotFound(_)) => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// List sandboxes that match the filter, following every page.
    ///
    /// # Errors
    ///
    /// An API error, or [`E2bError::Protocol`] for a malformed page.
    pub async fn list(&self, filter: &ListSandboxes) -> Result<Vec<ListedSandbox>, E2bError> {
        let url = format!("{}/v2/sandboxes", self.api());
        let mut query: Vec<(&str, String)> = Vec::new();
        if !filter.metadata.is_empty() {
            query.push(("metadata", encode_metadata(&filter.metadata)));
        }
        let states: Vec<_> = filter.states.iter().filter_map(|s| s.as_query()).collect();
        if !states.is_empty() {
            query.push(("state", states.join(",")));
        }
        let mut found = Vec::new();
        let mut next: Option<String> = None;
        for _ in 0..MAX_LIST_PAGES {
            let mut page_query = query.clone();
            if let Some(token) = &next {
                page_query.push(("nextToken", token.clone()));
            }
            let response = self
                .execute(Op::List, &[], || {
                    let req = self.rest(self.http.get(&url));
                    if page_query.is_empty() {
                        req
                    } else {
                        req.query(&page_query)
                    }
                })
                .await?;
            next = response
                .headers()
                .get("x-next-token")
                .and_then(|v| v.to_str().ok())
                .filter(|v| !v.is_empty())
                .map(str::to_owned);
            let page: Vec<ListedSandbox> = parse_json(response, "list").await?;
            found.extend(page);
            if next.is_none() {
                return Ok(found);
            }
        }
        Err(E2bError::Protocol("the sandbox list did not end".into()))
    }

    fn api(&self) -> &str {
        self.config.api_url.trim_end_matches('/')
    }

    fn rest(&self, request: RequestBuilder) -> RequestBuilder {
        request
            .header("X-API-Key", self.key.expose())
            .timeout(self.config.request_timeout)
    }

    /// Send a request, retrying 429 (and 409 "in progress" on create), and map
    /// any non-success status to an [`E2bError`]. `secrets` are extra values
    /// to redact from error messages.
    pub(super) async fn execute(
        &self,
        op: Op,
        secrets: &[&str],
        build: impl Fn() -> RequestBuilder,
    ) -> Result<Response, E2bError> {
        let mut attempt = 0;
        let mut in_progress_wait = self.config.default_retry_wait;
        loop {
            let response = self.send(op, build(), secrets).await?;
            let status = response.status();
            if status.is_success() {
                return Ok(response);
            }
            let retry_budget_left = attempt < self.config.max_rate_limit_retries;
            let retry_after = retry_after(&response);
            if status == StatusCode::TOO_MANY_REQUESTS {
                if !retry_budget_left {
                    return Err(E2bError::RateLimited { retry_after });
                }
                attempt += 1;
                let wait = retry_after.unwrap_or(self.config.default_retry_wait);
                tokio::time::sleep(wait.min(self.config.max_retry_wait)).await;
                continue;
            }
            let error = self.map_error(op, response, secrets).await;
            if op == Op::Create
                && matches!(error, E2bError::IdempotencyInProgress)
                && retry_budget_left
            {
                attempt += 1;
                let wait = retry_after.unwrap_or(in_progress_wait);
                in_progress_wait = (in_progress_wait * 2).min(self.config.max_retry_wait);
                tokio::time::sleep(wait.min(self.config.max_retry_wait)).await;
                continue;
            }
            return Err(error);
        }
    }

    /// REST calls carry the reqwest timeout. A process stream has none, so
    /// only its wait for response headers is bounded here.
    async fn send(
        &self,
        op: Op,
        request: RequestBuilder,
        secrets: &[&str],
    ) -> Result<Response, E2bError> {
        let sent = if op == Op::Process {
            tokio::time::timeout(self.config.request_timeout, request.send())
                .await
                .map_err(|_| E2bError::Transport("envd sent no response headers in time".into()))?
        } else {
            request.send().await
        };
        sent.map_err(|e| self.transport(&e, secrets))
    }

    async fn map_error(&self, op: Op, response: Response, secrets: &[&str]) -> E2bError {
        let status = response.status().as_u16();
        let should_retry = response
            .headers()
            .get("should-retry")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.trim().parse::<bool>().ok());
        let bytes = response.bytes().await.unwrap_or_default();
        let (message, error_code) = match serde_json::from_slice::<ErrorBody>(&bytes) {
            Ok(body) => (body.message.unwrap_or_default(), body.error_code),
            Err(_) => (String::from_utf8_lossy(&bytes).trim().to_string(), None),
        };
        let message = self.scrub(&message, secrets);
        let code = error_code.as_deref().unwrap_or("");
        match status {
            401 | 403 => E2bError::Unauthorized { status },
            404 => E2bError::NotFound(message),
            409 if code == "idempotency_request_mismatch" => E2bError::IdempotencyMismatch,
            409 if code == "idempotency_in_progress" => E2bError::IdempotencyInProgress,
            409 => E2bError::Conflict(message),
            422 if code == "idempotency_outcome_unknown" => E2bError::IdempotencyOutcomeUnknown,
            503 if op == Op::Pause => E2bError::PauseRefused(message),
            502 if op == Op::Process => E2bError::NotFound(message),
            _ => E2bError::Api {
                status,
                error_code,
                message,
                should_retry,
            },
        }
    }

    pub(super) fn transport(&self, error: &reqwest::Error, secrets: &[&str]) -> E2bError {
        let message = self.scrub(&error.to_string(), secrets);
        if error.is_connect() {
            E2bError::Unreachable(message)
        } else {
            E2bError::Transport(message)
        }
    }

    /// Remove the API key and `secrets` from a server-supplied message, then
    /// bound its length. Redaction comes first so a cut cannot split a secret.
    pub(super) fn scrub(&self, message: &str, secrets: &[&str]) -> String {
        let all: Vec<&str> = std::iter::once(self.key.expose())
            .chain(secrets.iter().copied())
            .collect();
        scrub(message, &all)
    }
}

/// Replace every secret in `message`, then bound its length.
pub(super) fn scrub(message: &str, secrets: &[&str]) -> String {
    let mut out = message.to_string();
    for secret in secrets {
        if !secret.is_empty() && out.contains(secret) {
            out = out.replace(secret, "<redacted>");
        }
    }
    out.chars().take(MAX_ERROR_MESSAGE).collect()
}

trait JsonBody {
    fn json_body(self, body: Vec<u8>) -> Self;
}

impl JsonBody for RequestBuilder {
    fn json_body(self, body: Vec<u8>) -> Self {
        self.header("Content-Type", "application/json").body(body)
    }
}

pub(super) async fn parse_json<T: serde::de::DeserializeOwned>(
    response: Response,
    what: &str,
) -> Result<T, E2bError> {
    let bytes = response
        .bytes()
        .await
        .map_err(|e| E2bError::Transport(e.without_url().to_string()))?;
    // serde messages can quote field values, so keep only the position.
    serde_json::from_slice(&bytes).map_err(|e| {
        E2bError::Protocol(format!(
            "malformed {what} response at line {} column {}",
            e.line(),
            e.column()
        ))
    })
}

fn retry_after(response: &Response) -> Option<Duration> {
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

pub(super) fn validate_sandbox_id(id: &str) -> Result<(), E2bError> {
    let ok = !id.is_empty()
        && id.len() <= 128
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_');
    if ok {
        Ok(())
    } else {
        Err(E2bError::InvalidArgument(format!("bad sandbox id {id:?}")))
    }
}

fn validate_idempotency_key(key: &str) -> Result<(), E2bError> {
    let ok = (1..=255).contains(&key.len())
        && key
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-');
    if ok {
        Ok(())
    } else {
        Err(E2bError::InvalidArgument(
            "the idempotency key must match [a-z0-9-]{1,255}".into(),
        ))
    }
}

/// `k1=v1&k2=v2` with each key and value percent-encoded, as E2B expects
/// inside the `metadata` query parameter.
fn encode_metadata(metadata: &BTreeMap<String, String>) -> String {
    let mut out = String::new();
    for (i, (key, value)) in metadata.iter().enumerate() {
        if i > 0 {
            out.push('&');
        }
        percent_encode(key, &mut out);
        out.push('=');
        percent_encode(value, &mut out);
    }
    out
}

fn percent_encode(text: &str, out: &mut String) {
    for b in text.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~') {
            out.push(char::from(b));
        } else {
            let _ = write!(out, "%{b:02X}");
        }
    }
}
