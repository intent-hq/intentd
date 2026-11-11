use std::time::Duration;

use reqwest::{Method, RequestBuilder, Response, StatusCode};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::Value;

use crate::cloud::{CloudError, CloudErrorKind, QuotaLimit, SecretString};

pub const DEFAULT_API_URL: &str = "https://api.northflank.com/v1";
pub const DEFAULT_EXEC_URL: &str = "wss://api.northflank.com/v1";

/// Longest error message kept from a Northflank response.
const MAX_ERROR_MESSAGE: usize = 512;
/// Guard against a server that keeps handing out cursors.
const MAX_LIST_PAGES: usize = 1000;
/// Northflank's largest page.
const PAGE_SIZE: &str = "100";
/// Longest compact JSON of a machine's tags. It is stored as a Kubernetes
/// annotation, whose values may total 256 KiB; this keeps it far below.
pub(super) const MAX_TAGS_JSON: usize = 16 * 1024;
/// Wait after a 429 that says nothing about when to come back.
const DEFAULT_RETRY_WAIT: Duration = Duration::from_secs(1);

#[derive(Debug, Clone)]
pub struct NorthflankConfig {
    /// REST base, including `/v1`.
    pub api_url: String,
    /// Command execution base, including `/v1` (`wss://`, or `ws://` in tests).
    pub exec_url: String,
    pub project_id: String,
    /// Budget for one REST call.
    pub request_timeout: Duration,
    pub connect_timeout: Duration,
    /// How long a create or resume waits for the deployment to run.
    pub ready_timeout: Duration,
    /// Time between two status reads while waiting.
    pub poll_interval: Duration,
    /// Retries after a 429 before giving up.
    pub max_rate_limit_retries: u32,
    /// Longest wait for one retry. A 429 that asks for longer is reported
    /// at once (Northflank's limit is per hour).
    pub max_retry_wait: Duration,
    /// How long a command stream may stay silent after `completion` before
    /// its output is taken as complete (the JavaScript client's 30 s).
    pub exec_drain_timeout: Duration,
    /// Test seam: send every image registry request (manifest and anonymous
    /// token) to this base URL (`http://` allowed) instead of the image's
    /// host, keeping the paths. `None` in production.
    pub registry_url: Option<String>,
}

impl Default for NorthflankConfig {
    fn default() -> Self {
        Self {
            api_url: DEFAULT_API_URL.into(),
            exec_url: DEFAULT_EXEC_URL.into(),
            project_id: String::new(),
            request_timeout: Duration::from_secs(60),
            connect_timeout: Duration::from_secs(10),
            ready_timeout: Duration::from_secs(600),
            poll_interval: Duration::from_secs(3),
            max_rate_limit_retries: 3,
            max_retry_wait: Duration::from_secs(30),
            exec_drain_timeout: Duration::from_secs(30),
            registry_url: None,
        }
    }
}

/// Client for one Northflank project, authenticated with an API token.
#[derive(Debug)]
pub struct NorthflankClient {
    pub(super) http: reqwest::Client,
    pub(super) token: SecretString,
    pub(super) config: NorthflankConfig,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectInfo {
    pub id: String,
    /// For example `europe-west`; empty when Northflank did not say.
    pub region: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PlanInfo {
    pub id: String,
    /// vCPUs.
    pub cpu: f64,
    pub memory_mib: u32,
}

/// What a call is, for its error: the action a 403 names and what a 404 means.
#[derive(Debug, Clone, Copy)]
pub(super) struct Call {
    pub action: &'static str,
    pub not_found: CloudErrorKind,
}

impl Call {
    pub const fn new(action: &'static str) -> Self {
        Self {
            action,
            not_found: CloudErrorKind::NotFound,
        }
    }
}

#[derive(Deserialize)]
struct Envelope<T> {
    data: T,
    #[serde(default)]
    pagination: Option<Pagination>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Pagination {
    #[serde(default)]
    has_next_page: bool,
    #[serde(default)]
    cursor: Option<String>,
}

#[derive(Deserialize)]
struct ProjectBody {
    id: String,
    #[serde(default)]
    deployment: Option<ProjectDeployment>,
}

#[derive(Deserialize)]
struct ProjectDeployment {
    #[serde(default)]
    region: String,
}

#[derive(Deserialize)]
struct PlansBody {
    plans: Vec<PlanBody>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PlanBody {
    id: String,
    #[serde(default)]
    cpu_resource: f64,
    #[serde(default)]
    ram_resource: f64,
}

impl NorthflankClient {
    /// # Errors
    ///
    /// [`CloudErrorKind::NotConfigured`] for an empty token,
    /// [`CloudErrorKind::Other`] when the HTTP client cannot be built.
    pub fn new(token: SecretString, config: NorthflankConfig) -> Result<Self, CloudError> {
        if token.is_empty() {
            return Err(CloudError::new(
                CloudErrorKind::NotConfigured,
                "the Northflank API token is not configured",
            ));
        }
        let http = reqwest::Client::builder()
            .connect_timeout(config.connect_timeout)
            .build()
            .map_err(|e| CloudError::new(CloudErrorKind::Other, e.to_string()))?;
        Ok(Self {
            http,
            token,
            config,
        })
    }

    /// The project, with its region.
    ///
    /// # Errors
    ///
    /// `401` is [`CloudErrorKind::Unauthorized`], `403`
    /// [`CloudErrorKind::PermissionDenied`] (`read-project`), `404`
    /// [`CloudErrorKind::ProjectMissing`].
    pub async fn project(&self) -> Result<ProjectInfo, CloudError> {
        let call = Call {
            action: "read-project",
            not_found: CloudErrorKind::ProjectMissing,
        };
        let body: ProjectBody = self
            .get_data(call, &format!("/projects/{}", self.project_id()?), &[])
            .await?;
        Ok(ProjectInfo {
            id: body.id,
            region: body.deployment.map(|d| d.region).unwrap_or_default(),
        })
    }

    /// Northflank's compute plans.
    ///
    /// # Errors
    ///
    /// An API error.
    pub async fn plans(&self) -> Result<Vec<PlanInfo>, CloudError> {
        let body: PlansBody = self
            .get_data(Call::new("list-plans"), "/plans", &[])
            .await?;
        Ok(body
            .plans
            .into_iter()
            .map(|p| PlanInfo {
                id: p.id,
                cpu: p.cpu_resource,
                memory_mib: as_u32(p.ram_resource),
            })
            .collect())
    }

    /// Whether the token may list the project's services (the reaper does).
    ///
    /// # Errors
    ///
    /// `403` is [`CloudErrorKind::PermissionDenied`] (`list-services`).
    pub async fn check_services(&self) -> Result<(), CloudError> {
        let path = format!("/projects/{}/services", self.project_id()?);
        self.request(
            Call::new("list-services"),
            Method::GET,
            &path,
            &[("per_page", "1".to_string())],
            None,
        )
        .await
        .map(drop)
    }

    pub(super) fn project_id(&self) -> Result<&str, CloudError> {
        let id = self.config.project_id.as_str();
        validate_id(id, "project id")?;
        Ok(id)
    }

    fn api(&self) -> &str {
        self.config.api_url.trim_end_matches('/')
    }

    pub(super) async fn get_data<T: DeserializeOwned>(
        &self,
        call: Call,
        path: &str,
        query: &[(&str, String)],
    ) -> Result<T, CloudError> {
        let response = self.request(call, Method::GET, path, query, None).await?;
        Ok(parse::<Envelope<T>>(response, call.action).await?.data)
    }

    pub(super) async fn post_data<T: DeserializeOwned>(
        &self,
        call: Call,
        path: &str,
        body: &Value,
    ) -> Result<T, CloudError> {
        let response = self
            .request(call, Method::POST, path, &[], Some(body))
            .await?;
        Ok(parse::<Envelope<T>>(response, call.action).await?.data)
    }

    /// Every item of a paginated list. `key` names the array inside
    /// `data`; `None` when `data` is the array itself.
    pub(super) async fn list_all<T: DeserializeOwned>(
        &self,
        call: Call,
        path: &str,
        key: Option<&str>,
    ) -> Result<Vec<T>, CloudError> {
        let mut found = Vec::new();
        let mut cursor: Option<String> = None;
        for _ in 0..MAX_LIST_PAGES {
            let mut query = vec![("per_page", PAGE_SIZE.to_string())];
            if let Some(cursor) = &cursor {
                query.push(("cursor", cursor.clone()));
            }
            let response = self.request(call, Method::GET, path, &query, None).await?;
            let page: Envelope<Value> = parse(response, call.action).await?;
            let items = match key {
                Some(key) => page.data.get(key).cloned().unwrap_or(Value::Array(vec![])),
                None => page.data,
            };
            let items: Vec<T> = serde_json::from_value(items).map_err(|e| {
                CloudError::new(
                    CloudErrorKind::Other,
                    format!(
                        "unexpected Northflank {} response: {}",
                        call.action,
                        serde_position(&e)
                    ),
                )
            })?;
            found.extend(items);
            cursor = match page.pagination {
                Some(Pagination {
                    has_next_page: true,
                    cursor: Some(next),
                }) if !next.is_empty() => Some(next),
                _ => return Ok(found),
            };
        }
        Err(CloudError::new(
            CloudErrorKind::Other,
            format!("the Northflank {} list did not end", call.action),
        ))
    }

    /// Send a REST call with the bearer token, retrying 429 within the
    /// budget, and map any other failure to a [`CloudError`].
    pub(super) async fn request(
        &self,
        call: Call,
        method: Method,
        path: &str,
        query: &[(&str, String)],
        body: Option<&Value>,
    ) -> Result<Response, CloudError> {
        let url = format!("{}{path}", self.api());
        let mut attempt = 0;
        loop {
            let mut request = self
                .http
                .request(method.clone(), &url)
                .bearer_auth(self.token.expose())
                .timeout(self.config.request_timeout);
            if !query.is_empty() {
                request = request.query(query);
            }
            request = with_body(request, body);
            let response = request.send().await.map_err(|e| self.transport(&e))?;
            let status = response.status();
            if status.is_success() {
                return Ok(response);
            }
            if status == StatusCode::TOO_MANY_REQUESTS {
                let wait = retry_after(&response);
                let message = self.read_error(response).await;
                let fits = wait.unwrap_or(DEFAULT_RETRY_WAIT) <= self.config.max_retry_wait;
                if attempt < self.config.max_rate_limit_retries && fits {
                    attempt += 1;
                    tokio::time::sleep(wait.unwrap_or(DEFAULT_RETRY_WAIT)).await;
                    continue;
                }
                return Err(CloudError::new(
                    CloudErrorKind::Quota(QuotaLimit::Rate),
                    format!("Northflank rate limit reached: {message}"),
                )
                .with_status(429)
                .with_retry_after(wait)
                .retryable(true));
            }
            return Err(self.map_error(call, response).await);
        }
    }

    async fn map_error(&self, call: Call, response: Response) -> CloudError {
        let status = response.status().as_u16();
        let message = self.read_error(response).await;
        let action = call.action;
        let (kind, retryable) = match status {
            400 | 402 | 403 | 409 | 422 if is_plan_limit(&message) => {
                (CloudErrorKind::Quota(QuotaLimit::Plan), false)
            }
            401 => (CloudErrorKind::Unauthorized, false),
            403 => (CloudErrorKind::PermissionDenied, false),
            404 => (call.not_found, false),
            409 => (CloudErrorKind::Conflict, true),
            500..=599 => (CloudErrorKind::Api, true),
            _ => (CloudErrorKind::Api, false),
        };
        let text = match kind {
            CloudErrorKind::Unauthorized => {
                format!("Northflank rejected the API token (HTTP {status}): {message}")
            }
            CloudErrorKind::PermissionDenied => {
                format!("the Northflank API token may not {action} (HTTP {status}): {message}")
            }
            _ => format!("Northflank refused {action} (HTTP {status}): {message}"),
        };
        let mut error = CloudError::new(kind, text)
            .with_status(status)
            .retryable(retryable);
        if kind == CloudErrorKind::PermissionDenied {
            error = error.with_action(action);
        }
        error
    }

    pub(super) fn transport(&self, error: &reqwest::Error) -> CloudError {
        let message = self.scrub(&error.to_string());
        if error.is_connect() {
            CloudError::new(
                CloudErrorKind::Other,
                format!("Northflank is unreachable: {message}"),
            )
            .retryable(true)
        } else {
            CloudError::new(
                CloudErrorKind::Other,
                format!("the Northflank request failed: {message}"),
            )
        }
    }

    /// The `error.message` of a Northflank error body, or its raw text, scrubbed.
    async fn read_error(&self, response: Response) -> String {
        let bytes = response.bytes().await.unwrap_or_default();
        let text = serde_json::from_slice::<Value>(&bytes)
            .ok()
            .and_then(|v| error_message(&v))
            .unwrap_or_else(|| String::from_utf8_lossy(&bytes).trim().to_string());
        self.scrub(&text)
    }

    /// Remove the token from a server-supplied message, then bound its
    /// length. Redaction comes first so a cut cannot split the token.
    pub(super) fn scrub(&self, message: &str) -> String {
        scrub(message, &[self.token.expose()])
    }
}

/// `error.message`, `message` or `data.message` of a JSON body.
pub(super) fn error_message(value: &Value) -> Option<String> {
    [
        value.pointer("/error/message"),
        value.get("message"),
        value.pointer("/data/message"),
    ]
    .into_iter()
    .flatten()
    .find_map(|v| v.as_str().map(str::to_owned))
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

fn with_body(request: RequestBuilder, body: Option<&Value>) -> RequestBuilder {
    match body {
        Some(body) => request
            .header("Content-Type", "application/json")
            .body(body.to_string()),
        None => request,
    }
}

/// Northflank documents no code for a refusal over the team's resource
/// quota or plan, so match a message about a quota or a limit being hit.
fn is_plan_limit(message: &str) -> bool {
    let text = message.to_ascii_lowercase();
    text.contains("quota")
        || (text.contains("limit")
            && ["exceed", "reached", "maximum", "insufficient", "plan"]
                .iter()
                .any(|w| text.contains(w)))
}

/// `Retry-After`, else Northflank's `x-ratelimit-reset` (seconds).
fn retry_after(response: &Response) -> Option<Duration> {
    ["retry-after", "x-ratelimit-reset"]
        .iter()
        .find_map(|name| {
            response
                .headers()
                .get(*name)?
                .to_str()
                .ok()?
                .trim()
                .parse::<u64>()
                .ok()
                .map(Duration::from_secs)
        })
}

pub(super) async fn parse<T: DeserializeOwned>(
    response: Response,
    what: &str,
) -> Result<T, CloudError> {
    let bytes = response.bytes().await.map_err(|e| {
        CloudError::new(
            CloudErrorKind::Other,
            format!("the Northflank response broke off: {}", e.without_url()),
        )
    })?;
    serde_json::from_slice(&bytes).map_err(|e| {
        CloudError::new(
            CloudErrorKind::Other,
            format!(
                "unexpected Northflank {what} response: {}",
                serde_position(&e)
            ),
        )
    })
}

/// serde messages can quote field values, so keep only the position.
fn serde_position(error: &serde_json::Error) -> String {
    format!(
        "malformed at line {} column {}",
        error.line(),
        error.column()
    )
}

/// A Northflank id (project, service, volume): letters, digits and hyphens.
pub(super) fn validate_id(id: &str, what: &str) -> Result<(), CloudError> {
    let ok = !id.is_empty()
        && id.len() <= 100
        && id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-');
    if ok {
        Ok(())
    } else {
        Err(CloudError::new(
            CloudErrorKind::InvalidArgument,
            format!("bad Northflank {what} {id:?}"),
        ))
    }
}

#[expect(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "clamped to 0..=u32::MAX first"
)]
fn as_u32(value: f64) -> u32 {
    if value.is_finite() && value > 0.0 {
        value.min(f64::from(u32::MAX)).round() as u32
    } else {
        0
    }
}
