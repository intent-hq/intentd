//! GitLab connection engine: instance host normalization, the OAuth
//! **device authorization grant** (RFC 8628), and personal-access-token (PAT)
//! validation — the `gitlab` sibling of [`crate::device_flow`].
//!
//! Device grant: [`start_device_grant`] posts to
//! `https://<host>/oauth/authorize_device` for a `user_code` the user enters
//! at the returned `verification_uri`, then [`GitlabDeviceFlow::poll_once`]
//! polls `POST /oauth/token` (`grant_type=urn:ietf:params:oauth:grant-type:device_code`)
//! until the user authorizes (or the code expires / is denied). On success
//! the access token is persisted straight into the file-backed secret store
//! under `sourceControl.gitlab.token` — the slot [`crate::gitlab_token`]
//! reads first. GitLab added the grant in 17.1 (17.2 for self-managed
//! behind a feature flag); an older instance answers 404, and an instance
//! whose application is not enabled for the grant answers
//! `unauthorized_client` — both map to [`Error::DeviceGrantUnsupported`] so
//! the caller can offer the PAT path.
//!
//! PAT: [`validate_pat`] proves a pasted token against `GET /api/v4/user`.
//!
//! Only a *public* OAuth application `client_id` is needed (no secret). 🔒
//! Tokens and the `device_code` are secrets: never logged, never carried in
//! any `Debug`/`Serialize` shape, never returned to callers.

use std::time::Duration;

use intent_core::FileSecretStore;
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::time::timeout;

use crate::error::{Error, Result};
use crate::github::{CONNECT_TIMEOUT, READ_WRITE_TIMEOUT};
use crate::gitlab_token::{REFRESH_SECRET_ACCOUNT, SECRET_ACCOUNT};

/// Canonical host of the hosted (gitlab.com) instance.
pub const GITLAB_COM_HOST: &str = "gitlab.com";

/// Default OAuth application client id for the device grant on gitlab.com
/// (public by design — device-grant apps have no secret). Used **only** when
/// the canonical host is [`GITLAB_COM_HOST`]; self-managed instances must
/// configure `sourceControl.gitlab.oauthClientId`. This is the Application
/// ID of the intent-hq public (non-confidential) OAuth application on
/// gitlab.com — an identifier, not a secret. Empty would mean "no client id":
/// the device grant is reported unsupported and the PAT path is the only
/// connection method.
pub const GITLAB_COM_OAUTH_CLIENT_ID: &str =
    "e65857695d3980e00e7e7b0fa99816b55381b932627c68fc8fd320179f88df89";

/// Scopes requested by the device grant. `api` (rather than the narrower
/// `read_api` + `write_repository`) because snippet create/delete — planned
/// for the pasteboard work — is only reachable through the full `api` scope.
pub const DEVICE_GRANT_SCOPES: &[&str] = &["api"];

/// Extra seconds RFC 8628 §3.5 mandates after a `slow_down` response.
const SLOW_DOWN_BUMP_SECS: u64 = 5;

/// Bounded wait for a blocking secret-store write/delete (mirrors
/// `crate::device_flow`).
const SECRET_WRITE_TIMEOUT: Duration = Duration::from_secs(10);

/// A validated GitLab instance: canonical lowercase host plus the `https://`
/// base every `/oauth/*` and `/api/v4/*` request hangs off.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitlabHost {
    host: String,
    base_url: String,
}

impl GitlabHost {
    /// Normalize a user-supplied instance reference: `gitlab.com`,
    /// `GitLab.Acme.internal/`, `https://gitlab.acme.internal/gitlab` (a
    /// relative-URL-root prefix is kept), trailing slashes tolerated. The
    /// scheme must be `https`; plain `http` is accepted only for loopback
    /// (`127.0.0.1`, `::1`, `localhost`) as the test seam.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Config`] for an empty/unparseable value, a non-https
    /// non-loopback scheme, or embedded credentials / query / fragment.
    pub fn parse(input: &str) -> Result<Self> {
        let trimmed = input.trim();
        if trimmed.is_empty() {
            return Err(Error::Config("gitlab host is empty".to_string()));
        }
        let with_scheme = if trimmed.contains("://") {
            trimmed.to_string()
        } else {
            format!("https://{trimmed}")
        };
        let mut url = reqwest::Url::parse(&with_scheme)
            .map_err(|e| Error::Config(format!("invalid gitlab host {trimmed:?}: {e}")))?;
        let Some(host) = url.host_str().map(str::to_ascii_lowercase) else {
            return Err(Error::Config(format!(
                "invalid gitlab host {trimmed:?}: no host"
            )));
        };
        if !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(Error::Config(
                "gitlab host must not carry credentials, a query, or a fragment".to_string(),
            ));
        }
        let loopback = host == "localhost"
            || host
                .trim_matches(['[', ']'])
                .parse::<std::net::IpAddr>()
                .is_ok_and(|ip| ip.is_loopback());
        match url.scheme() {
            "https" => {}
            "http" if loopback => {}
            other => {
                return Err(Error::Config(format!(
                    "gitlab host {trimmed:?} must use https (got {other}; http is allowed only for loopback)"
                )))
            }
        }
        let path = url.path().trim_end_matches('/').to_string();
        url.set_path(&path);
        let authority = url
            .port()
            .map_or_else(|| host.clone(), |port| format!("{host}:{port}"));
        let base_url = url.as_str().trim_end_matches('/').to_string();
        Ok(Self {
            host: authority,
            base_url,
        })
    }

    /// Canonical lowercase host, with the explicit port when one was given
    /// (`gitlab.com`, `gitlab.acme.internal`, `127.0.0.1:4321`).
    #[must_use]
    pub fn host(&self) -> &str {
        &self.host
    }

    /// Instance root without a trailing slash (`https://gitlab.com`,
    /// `https://gitlab.acme.internal/gitlab`).
    #[must_use]
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// REST v4 base (`<base_url>/api/v4`).
    #[must_use]
    pub fn api_base(&self) -> String {
        format!("{}/api/v4", self.base_url)
    }

    /// True for the hosted instance, where [`GITLAB_COM_OAUTH_CLIENT_ID`] applies.
    #[must_use]
    pub fn is_gitlab_com(&self) -> bool {
        self.host == GITLAB_COM_HOST
    }
}

/// The OAuth client id the device grant uses for `host`: the configured
/// `sourceControl.gitlab.oauthClientId` when non-empty, else the compiled
/// [`GITLAB_COM_OAUTH_CLIENT_ID`] — but only on gitlab.com, and only when it
/// is filled in. `None` means the device grant is unavailable for this
/// instance ([`start_device_grant`] reports [`Error::DeviceGrantUnsupported`]).
#[must_use]
pub fn resolve_client_id(configured: &str, host: &GitlabHost) -> Option<String> {
    let configured = configured.trim();
    if !configured.is_empty() {
        return Some(configured.to_string());
    }
    (host.is_gitlab_com() && !GITLAB_COM_OAUTH_CLIENT_ID.is_empty())
        .then(|| GITLAB_COM_OAUTH_CLIENT_ID.to_string())
}

/// Shared HTTP client for the auth endpoints: bounded connect and total
/// request time so a dark connection fails instead of pending forever (same
/// budgets and rationale as [`crate::github`], intent-hq/monorepo#1988).
fn http_client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .connect_timeout(CONNECT_TIMEOUT)
        .timeout(READ_WRITE_TIMEOUT)
        .user_agent(concat!("intentd/", env!("CARGO_PKG_VERSION")))
        .build()
        .map_err(|e| Error::Config(format!("gitlab http client: {e}")))
}

/// Transport failures. reqwest's messages carry the URL (never headers or
/// form bodies), so no token material can reach the error text.
fn transport(e: &reqwest::Error) -> Error {
    Error::Api(format!("gitlab request failed: {e}"))
}

/// User-facing half of the device-grant start response. Deliberately
/// excludes the secret `device_code` (kept inside [`GitlabDeviceFlow`]) so
/// this shape is safe to serialize onto the wire.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GitlabDeviceAuthorization {
    /// Short code the user types at [`Self::verification_uri`].
    pub user_code: String,
    /// Where the user enters the code (`https://<host>/oauth/device`).
    pub verification_uri: String,
    /// The verification URI with the code pre-filled, when the instance
    /// provides one (RFC 8628 §3.2 `verification_uri_complete`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub verification_uri_complete: Option<String>,
    /// Seconds until the codes expire.
    pub expires_in: u64,
    /// Minimum seconds between polls.
    pub interval: u64,
}

/// Terminal-visible poll states surfaced to callers. `slow_down` is absorbed
/// internally (the next-poll interval grows) and reported as [`Self::Pending`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GitlabPollStatus {
    /// The user authorized; the token is now persisted in the secret store.
    Authorized,
    /// The user has not entered the code yet — poll again after
    /// [`GitlabDeviceFlow::interval_secs`].
    Pending,
    /// The device/user codes expired; restart the flow.
    Expired,
    /// The user denied the authorization request.
    Denied,
}

/// Opaque in-flight device-grant handle returned by [`start_device_grant`].
/// Holds the secret `device_code` privately; `Debug` is redacted.
pub struct GitlabDeviceFlow {
    client: reqwest::Client,
    token_url: String,
    client_id: SecretString,
    device_code: SecretString,
    interval: u64,
    store: FileSecretStore,
}

impl std::fmt::Debug for GitlabDeviceFlow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GitlabDeviceFlow")
            .field("token_url", &self.token_url)
            .field("interval", &self.interval)
            .finish_non_exhaustive()
    }
}

/// Raw `/oauth/authorize_device` success body (RFC 8628 §3.2). Crate-private:
/// carries the secret `device_code`.
#[derive(Deserialize)]
struct DeviceAuthorizationResponse {
    device_code: String,
    user_code: String,
    verification_uri: String,
    #[serde(default)]
    verification_uri_complete: Option<String>,
    expires_in: u64,
    #[serde(default = "default_interval")]
    interval: u64,
}

fn default_interval() -> u64 {
    5
}

/// Start a device grant against `host` for the public `client_id`, requesting
/// [`DEVICE_GRANT_SCOPES`]. The token is persisted to the daemon's default
/// secret store on authorization.
///
/// # Errors
///
/// Returns [`Error::Config`] if `client_id` is empty;
/// [`Error::DeviceGrantUnsupported`] when the instance answers 404 on
/// `/oauth/authorize_device` (GitLab < 17.1) or rejects the request with the
/// OAuth `unauthorized_client` error (application not enabled for the device
/// grant); propagates other HTTP/decoding failures.
pub async fn start_device_grant(
    host: &GitlabHost,
    client_id: &str,
) -> Result<(GitlabDeviceAuthorization, GitlabDeviceFlow)> {
    start_device_grant_with_store(host, client_id, FileSecretStore::new()).await
}

/// [`start_device_grant`] persisting into an explicit `store` — the seam that
/// lets tests drive connect → poll → authorized against a loopback mock
/// without touching the real `~/intent/.secrets.json`.
///
/// # Errors
///
/// As [`start_device_grant`].
pub async fn start_device_grant_with_store(
    host: &GitlabHost,
    client_id: &str,
    store: FileSecretStore,
) -> Result<(GitlabDeviceAuthorization, GitlabDeviceFlow)> {
    let client_id = client_id.trim();
    if client_id.is_empty() {
        return Err(Error::Config(
            "gitlab device grant requires a non-empty oauth client id \
             (sourceControl.gitlab.oauthClientId)"
                .to_string(),
        ));
    }
    let client = http_client()?;
    let scope = DEVICE_GRANT_SCOPES.join(" ");
    let response = client
        .post(format!("{}/oauth/authorize_device", host.base_url()))
        .form(&[("client_id", client_id), ("scope", scope.as_str())])
        .send()
        .await
        .map_err(|e| transport(&e))?;
    let status = response.status();
    if status == reqwest::StatusCode::NOT_FOUND {
        return Err(Error::DeviceGrantUnsupported(host.host().to_string()));
    }
    let body: Value = response.json().await.map_err(|e| {
        Error::Decode(format!(
            "gitlab device authorization response ({status}) is not json: {e}"
        ))
    })?;
    if !status.is_success() {
        if body.get("error").and_then(Value::as_str) == Some("unauthorized_client") {
            return Err(Error::DeviceGrantUnsupported(host.host().to_string()));
        }
        return Err(Error::Api(format!(
            "gitlab device authorization failed ({status}): {}",
            oauth_error_summary(&body)
        )));
    }
    let codes: DeviceAuthorizationResponse = serde_json::from_value(body).map_err(|e| {
        Error::Decode(format!(
            "unrecognized gitlab device authorization response: {e}"
        ))
    })?;
    let auth = GitlabDeviceAuthorization {
        user_code: codes.user_code,
        verification_uri: codes.verification_uri,
        verification_uri_complete: codes.verification_uri_complete,
        expires_in: codes.expires_in,
        interval: codes.interval,
    };
    let flow = GitlabDeviceFlow {
        client,
        token_url: format!("{}/oauth/token", host.base_url()),
        client_id: SecretString::from(client_id.to_string()),
        device_code: SecretString::from(codes.device_code),
        interval: codes.interval,
        store,
    };
    Ok((auth, flow))
}

/// The `error` / `error_description` fields of an OAuth error body — never
/// the raw body, which is not guaranteed to be free of credential material.
fn oauth_error_summary(body: &Value) -> String {
    let code = body
        .get("error")
        .and_then(Value::as_str)
        .unwrap_or("unknown error");
    match body.get("error_description").and_then(Value::as_str) {
        Some(desc) if !desc.is_empty() => format!("{code}: {desc}"),
        _ => code.to_string(),
    }
}

impl GitlabDeviceFlow {
    /// Minimum seconds callers must wait before the next [`Self::poll_once`]
    /// (grows when the instance answers `slow_down`).
    #[must_use]
    pub fn interval_secs(&self) -> u64 {
        self.interval
    }

    /// Poll the token endpoint once. On [`GitlabPollStatus::Authorized`] the
    /// access token (and the refresh token, when granted) has already been
    /// persisted to the secret store — it is never returned to the caller.
    ///
    /// # Errors
    ///
    /// Returns an error when the token request fails, the response cannot be
    /// classified, or persisting the token fails. Grant expiration and denial
    /// are not errors — they are reported as [`GitlabPollStatus::Expired`]
    /// and [`GitlabPollStatus::Denied`].
    pub async fn poll_once(&mut self) -> Result<GitlabPollStatus> {
        // Doorkeeper reports pending/slow_down/expired/denied as an `error`
        // code on an HTTP 400 body, so classify the body regardless of status.
        let response = self
            .client
            .post(&self.token_url)
            .form(&[
                ("client_id", self.client_id.expose_secret()),
                ("device_code", self.device_code.expose_secret()),
                ("grant_type", "urn:ietf:params:oauth:grant-type:device_code"),
            ])
            .send()
            .await
            .map_err(|e| transport(&e))?;
        let status = response.status();
        let body: Value = response.json().await.map_err(|e| {
            Error::Decode(format!(
                "gitlab device token response ({status}) is not json: {e}"
            ))
        })?;
        match parse_poll_response(&body)? {
            PollResponse::Authorized {
                access_token,
                refresh_token,
            } => {
                persist_tokens(self.store.clone(), access_token, refresh_token).await?;
                Ok(GitlabPollStatus::Authorized)
            }
            PollResponse::Pending => Ok(GitlabPollStatus::Pending),
            PollResponse::SlowDown { interval } => {
                self.interval = next_interval(self.interval, interval);
                Ok(GitlabPollStatus::Pending)
            }
            PollResponse::Expired => Ok(GitlabPollStatus::Expired),
            PollResponse::Denied => Ok(GitlabPollStatus::Denied),
        }
    }
}

/// Classified device-token poll response (crate-private: the authorized arm
/// carries raw tokens, which must not escape this module).
enum PollResponse {
    Authorized {
        access_token: SecretString,
        refresh_token: Option<SecretString>,
    },
    Pending,
    SlowDown {
        interval: Option<u64>,
    },
    Expired,
    Denied,
}

/// Manual `Debug` so the authorized arm's tokens can never leak through
/// formatting (tests and error paths format this type).
impl std::fmt::Debug for PollResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Authorized { .. } => f.write_str("Authorized { <redacted> }"),
            Self::Pending => f.write_str("Pending"),
            Self::SlowDown { interval } => f
                .debug_struct("SlowDown")
                .field("interval", interval)
                .finish(),
            Self::Expired => f.write_str("Expired"),
            Self::Denied => f.write_str("Denied"),
        }
    }
}

/// Classify a device-token poll body per RFC 8628 §3.5. Unknown error codes
/// (`invalid_client`, `invalid_grant`, `unsupported_grant_type`, …) are
/// non-retryable and surface as [`Error::Api`] carrying only the error code
/// and description — never the raw body.
fn parse_poll_response(body: &Value) -> Result<PollResponse> {
    if let Some(token) = body.get("access_token").and_then(Value::as_str) {
        if !token.is_empty() {
            return Ok(PollResponse::Authorized {
                access_token: SecretString::from(token.to_string()),
                refresh_token: body
                    .get("refresh_token")
                    .and_then(Value::as_str)
                    .filter(|t| !t.is_empty())
                    .map(|t| SecretString::from(t.to_string())),
            });
        }
    }
    match body.get("error").and_then(Value::as_str) {
        Some("authorization_pending") => Ok(PollResponse::Pending),
        Some("slow_down") => Ok(PollResponse::SlowDown {
            interval: body.get("interval").and_then(Value::as_u64),
        }),
        Some("expired_token") => Ok(PollResponse::Expired),
        Some("access_denied") => Ok(PollResponse::Denied),
        Some(_) => Err(Error::Api(format!(
            "gitlab device grant error: {}",
            oauth_error_summary(body)
        ))),
        None => Err(Error::Decode(
            "unrecognized gitlab device-grant poll response".to_string(),
        )),
    }
}

/// Next poll interval after a `slow_down`: at least the mandated current + 5s
/// (RFC 8628 §3.5), growing further to a hinted `interval` when larger —
/// never shrinking below the mandated bump.
fn next_interval(current: u64, hinted: Option<u64>) -> u64 {
    let bumped = current.saturating_add(SLOW_DOWN_BUMP_SECS);
    hinted.unwrap_or(bumped).max(bumped)
}

/// The identity a validated PAT resolves to (`GET /api/v4/user`). Serializes
/// camelCase for the wire; deserializes GitLab's `snake_case` body as well.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GitlabUser {
    pub id: u64,
    pub username: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, alias = "avatar_url", skip_serializing_if = "Option::is_none")]
    pub avatar_url: Option<String>,
}

/// Validate a pasted personal access token against `host`: `GET /api/v4/user`
/// as the bearer must succeed. When the instance reports the token's scopes
/// (`GET /api/v4/personal_access_tokens/self` answers 200 with a `scopes`
/// array) they must include `api`; an instance that does not report them
/// (any non-200, OAuth tokens, older versions) is **not** failed closed.
/// The token is never persisted here — see [`persist_gitlab_token`].
///
/// # Errors
///
/// Returns [`Error::Auth`] for a 401/403 (rejected token) or a confirmed
/// missing `api` scope; [`Error::Api`] / [`Error::Decode`] for other failures.
pub async fn validate_pat(host: &GitlabHost, token: &str) -> Result<GitlabUser> {
    let token = token.trim();
    if token.is_empty() {
        return Err(Error::Auth("gitlab token is empty".to_string()));
    }
    let client = http_client()?;
    let api = host.api_base();
    let response = client
        .get(format!("{api}/user"))
        .bearer_auth(token)
        .send()
        .await
        .map_err(|e| transport(&e))?;
    let status = response.status();
    match status.as_u16() {
        401 | 403 => {
            return Err(Error::Auth(format!(
                "gitlab rejected the token for {} ({status})",
                host.host()
            )))
        }
        429 => {
            return Err(Error::RateLimited(format!(
                "gitlab rate limited the token check for {}",
                host.host()
            )))
        }
        _ if !status.is_success() => {
            return Err(Error::Api(format!(
                "gitlab user lookup on {} failed ({status})",
                host.host()
            )))
        }
        _ => {}
    }
    let user: GitlabUser = response
        .json()
        .await
        .map_err(|e| Error::Decode(format!("unrecognized gitlab user response: {e}")))?;

    let scopes = client
        .get(format!("{api}/personal_access_tokens/self"))
        .bearer_auth(token)
        .send()
        .await
        .ok()
        .filter(|r| r.status().is_success());
    if let Some(scopes) = scopes {
        let body: Value = scopes.json().await.unwrap_or(Value::Null);
        check_api_scope(reported_scopes(&body).as_deref(), host.host())?;
    }
    Ok(user)
}

/// The `scopes` array of a `personal_access_tokens/self` body, or `None`
/// when the body does not report one.
fn reported_scopes(body: &Value) -> Option<Vec<String>> {
    body.get("scopes")?.as_array().map(|a| {
        a.iter()
            .filter_map(Value::as_str)
            .map(str::to_string)
            .collect()
    })
}

/// Reject only when the instance *reported* scopes that lack `api`; an
/// absent report is accepted.
fn check_api_scope(scopes: Option<&[String]>, host: &str) -> Result<()> {
    match scopes {
        Some(scopes) if !scopes.iter().any(|s| s == "api") => Err(Error::Auth(format!(
            "gitlab token for {host} lacks the `api` scope (has: {})",
            if scopes.is_empty() {
                "none".to_string()
            } else {
                scopes.join(", ")
            }
        ))),
        _ => Ok(()),
    }
}

/// Persist a validated access token into `store` under
/// `sourceControl.gitlab.token` (the first slot of the GitLab resolution
/// chain) — the PAT path calls this after [`validate_pat`]. Any previously
/// stored device-grant refresh token is removed so a stale one cannot outlive
/// the credential it belonged to. Blocking writes run on the pool bounded by
/// [`SECRET_WRITE_TIMEOUT`].
///
/// # Errors
///
/// Returns [`Error::Api`] when the write fails or times out.
pub async fn persist_gitlab_token(store: FileSecretStore, token: SecretString) -> Result<()> {
    persist_tokens(store, token, None).await
}

/// Persist the access token and, when granted, the refresh token; a `None`
/// refresh token clears any stored one.
async fn persist_tokens(
    store: FileSecretStore,
    token: SecretString,
    refresh_token: Option<SecretString>,
) -> Result<()> {
    run_blocking(
        move || {
            store.store(SECRET_ACCOUNT, token.expose_secret())?;
            match refresh_token {
                Some(refresh) => store.store(REFRESH_SECRET_ACCOUNT, refresh.expose_secret()),
                None => store.delete(REFRESH_SECRET_ACCOUNT),
            }
        },
        "persist",
    )
    .await
}

/// Delete the stored `sourceControl.gitlab.token` (and refresh token) from
/// `store` — revoke / disconnect. Absence is an idempotent success, mirroring
/// [`FileSecretStore::delete`]. The `GITLAB_TOKEN` env fallback is untouched.
///
/// # Errors
///
/// Returns [`Error::Api`] when the delete fails or times out.
pub async fn revoke_gitlab_token(store: FileSecretStore) -> Result<()> {
    run_blocking(
        move || {
            store.delete(SECRET_ACCOUNT)?;
            store.delete(REFRESH_SECRET_ACCOUNT)
        },
        "delete",
    )
    .await
}

async fn run_blocking<F>(write: F, what: &str) -> Result<()>
where
    F: FnOnce() -> intent_core::Result<()> + Send + 'static,
{
    match timeout(SECRET_WRITE_TIMEOUT, tokio::task::spawn_blocking(write)).await {
        Ok(Ok(Ok(()))) => Ok(()),
        Ok(Ok(Err(e))) => Err(Error::Api(format!("could not {what} gitlab token: {e}"))),
        Ok(Err(join_err)) => Err(Error::Api(format!(
            "secret-store {what} task failed: {join_err}"
        ))),
        Err(_) => Err(Error::Api(format!(
            "secret-store {what} timed out for {SECRET_ACCOUNT}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use serde_json::json;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    use super::*;

    // ---- host normalization -------------------------------------------------

    #[test]
    fn bare_host_and_trailing_slash_normalize_to_https() {
        for input in ["gitlab.com", "GitLab.com/", " https://gitlab.com/ "] {
            let h = GitlabHost::parse(input).expect(input);
            assert_eq!(h.host(), "gitlab.com", "{input}");
            assert_eq!(h.base_url(), "https://gitlab.com", "{input}");
            assert_eq!(h.api_base(), "https://gitlab.com/api/v4", "{input}");
            assert!(h.is_gitlab_com(), "{input}");
        }
    }

    #[test]
    fn self_managed_host_keeps_port_and_relative_root() {
        let h = GitlabHost::parse("https://GitLab.Acme.internal:8443/gitlab/").unwrap();
        assert_eq!(h.host(), "gitlab.acme.internal:8443");
        assert_eq!(h.base_url(), "https://gitlab.acme.internal:8443/gitlab");
        assert_eq!(
            h.api_base(),
            "https://gitlab.acme.internal:8443/gitlab/api/v4"
        );
        assert!(!h.is_gitlab_com());
    }

    #[test]
    fn http_is_accepted_only_for_loopback() {
        for ok in [
            "http://127.0.0.1:4321",
            "http://localhost",
            "http://[::1]:80/",
        ] {
            assert!(GitlabHost::parse(ok).is_ok(), "{ok}");
        }
        for bad in [
            "http://gitlab.com",
            "http://gitlab.acme.internal",
            "ftp://gitlab.com",
            "",
            "   ",
            "https://user:pw@gitlab.com",
            "https://gitlab.com/?x=1",
            "https://gitlab.com/#frag",
            "https://",
        ] {
            assert!(
                matches!(GitlabHost::parse(bad), Err(Error::Config(_))),
                "{bad:?} must be rejected"
            );
        }
    }

    // ---- client id resolution -----------------------------------------------

    #[test]
    fn configured_client_id_wins_and_default_applies_only_to_gitlab_com() {
        let com = GitlabHost::parse("gitlab.com").unwrap();
        let acme = GitlabHost::parse("gitlab.acme.internal").unwrap();
        assert_eq!(
            resolve_client_id(" configured-id ", &acme).as_deref(),
            Some("configured-id")
        );
        assert_eq!(
            resolve_client_id("configured-id", &com).as_deref(),
            Some("configured-id")
        );
        assert_eq!(resolve_client_id("", &acme), None);
        assert!(!GITLAB_COM_OAUTH_CLIENT_ID.is_empty());
        assert_eq!(
            resolve_client_id("", &com).as_deref(),
            Some(GITLAB_COM_OAUTH_CLIENT_ID)
        );
        assert_eq!(
            resolve_client_id("  ", &com).as_deref(),
            Some(GITLAB_COM_OAUTH_CLIENT_ID)
        );
    }

    #[test]
    fn device_grant_requests_the_api_scope() {
        assert_eq!(DEVICE_GRANT_SCOPES, &["api"]);
    }

    // ---- poll classification ------------------------------------------------

    fn classify(body: &Value) -> PollResponse {
        parse_poll_response(body).expect("classifiable response")
    }

    #[test]
    fn authorized_carries_access_and_optional_refresh_token() {
        match classify(&json!({
            "access_token": "glat-x", "token_type": "bearer", "expires_in": 7200,
            "refresh_token": "glrt-y", "scope": "api"
        })) {
            PollResponse::Authorized {
                access_token,
                refresh_token,
            } => {
                assert_eq!(access_token.expose_secret(), "glat-x");
                assert_eq!(refresh_token.unwrap().expose_secret(), "glrt-y");
            }
            other => panic!("expected Authorized, got {other:?}"),
        }
        assert!(matches!(
            classify(&json!({ "access_token": "glat-x" })),
            PollResponse::Authorized {
                refresh_token: None,
                ..
            }
        ));
        assert!(matches!(
            parse_poll_response(&json!({ "access_token": "" })).unwrap_err(),
            Error::Decode(_)
        ));
    }

    #[test]
    fn rfc8628_error_codes_classify() {
        assert!(matches!(
            classify(&json!({ "error": "authorization_pending" })),
            PollResponse::Pending
        ));
        assert!(matches!(
            classify(&json!({ "error": "slow_down", "interval": 10 })),
            PollResponse::SlowDown { interval: Some(10) }
        ));
        assert!(matches!(
            classify(&json!({ "error": "slow_down" })),
            PollResponse::SlowDown { interval: None }
        ));
        assert!(matches!(
            classify(&json!({ "error": "expired_token" })),
            PollResponse::Expired
        ));
        assert!(matches!(
            classify(&json!({ "error": "access_denied" })),
            PollResponse::Denied
        ));
        match parse_poll_response(&json!({
            "error": "invalid_client", "error_description": "Client authentication failed",
            "leak": "must-not-appear"
        }))
        .unwrap_err()
        {
            Error::Api(msg) => {
                assert!(msg.contains("invalid_client: Client authentication failed"));
                assert!(!msg.contains("must-not-appear"));
            }
            other => panic!("expected Api error, got {other:?}"),
        }
        match parse_poll_response(&json!({ "unexpected": "shape" })).unwrap_err() {
            Error::Decode(msg) => assert!(!msg.contains("unexpected")),
            other => panic!("expected Decode error, got {other:?}"),
        }
    }

    #[test]
    fn slow_down_interval_never_shrinks() {
        assert_eq!(next_interval(5, None), 10);
        assert_eq!(next_interval(5, Some(15)), 15);
        assert_eq!(next_interval(10, Some(5)), 15);
    }

    #[test]
    fn api_scope_check_only_rejects_reported_scopes_without_api() {
        assert!(check_api_scope(None, "h").is_ok());
        assert!(check_api_scope(Some(&["api".to_string()]), "h").is_ok());
        assert!(check_api_scope(Some(&["read_user".to_string(), "api".to_string()]), "h").is_ok());
        let err = check_api_scope(Some(&["read_user".to_string()]), "h").unwrap_err();
        assert!(
            matches!(&err, Error::Auth(m) if m.contains("lacks the `api` scope") && m.contains("read_user"))
        );
        assert!(matches!(
            check_api_scope(Some(&[]), "h"),
            Err(Error::Auth(_))
        ));
        assert_eq!(reported_scopes(&json!({})), None);
        assert_eq!(reported_scopes(&json!({ "scopes": "api" })), None);
        assert_eq!(
            reported_scopes(&json!({ "scopes": ["api", 3] })),
            Some(vec!["api".to_string()])
        );
    }

    #[test]
    fn device_authorization_serializes_camel_case_without_device_code() {
        let auth = GitlabDeviceAuthorization {
            user_code: "ABCD-1234".to_string(),
            verification_uri: "https://gitlab.com/oauth/device".to_string(),
            verification_uri_complete: None,
            expires_in: 300,
            interval: 5,
        };
        let v = serde_json::to_value(&auth).expect("serialize");
        assert_eq!(v["userCode"], "ABCD-1234");
        assert_eq!(v["verificationUri"], "https://gitlab.com/oauth/device");
        assert_eq!(v["expiresIn"], 300);
        assert_eq!(v["interval"], 5);
        assert!(v.get("deviceCode").is_none());
        assert!(v.get("verificationUriComplete").is_none());
    }

    // ---- loopback mock GitLab -----------------------------------------------

    /// One recorded request: method + path, then the form/JSON body text.
    type Recorded = (String, String);
    /// Per-request responder: `(method, path, body)` → `(status, json body)`.
    type Responder = Arc<dyn Fn(&str, &str, &str) -> (u16, Value) + Send + Sync>;

    struct MockGitlab {
        host: GitlabHost,
        requests: Arc<Mutex<Vec<Recorded>>>,
    }

    impl MockGitlab {
        fn requests(&self) -> Vec<Recorded> {
            self.requests.lock().unwrap().clone()
        }
    }

    async fn spawn_mock(respond: Responder) -> MockGitlab {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .expect("bind mock gitlab");
        let port = listener.local_addr().unwrap().port();
        let requests: Arc<Mutex<Vec<Recorded>>> = Arc::default();
        let log = requests.clone();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                let respond = respond.clone();
                let log = log.clone();
                tokio::spawn(async move {
                    let _ = serve_conn(stream, respond.as_ref(), &log).await;
                });
            }
        });
        MockGitlab {
            host: GitlabHost::parse(&format!("http://127.0.0.1:{port}")).unwrap(),
            requests,
        }
    }

    /// Minimal HTTP/1.1 handler: read one request (head + content-length
    /// body), record it, answer with the responder's status + JSON, close.
    async fn serve_conn(
        mut stream: TcpStream,
        respond: &(dyn Fn(&str, &str, &str) -> (u16, Value) + Send + Sync),
        log: &Mutex<Vec<Recorded>>,
    ) -> std::io::Result<()> {
        let mut buf = Vec::new();
        let mut tmp = [0u8; 1024];
        let (head_end, body_start) = loop {
            let n = stream.read(&mut tmp).await?;
            if n == 0 {
                return Ok(());
            }
            buf.extend_from_slice(&tmp[..n]);
            if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                break (pos, pos + 4);
            }
        };
        let head = String::from_utf8_lossy(&buf[..head_end]).to_string();
        let content_length = head
            .lines()
            .find_map(|l| {
                let (name, value) = l.split_once(':')?;
                name.trim()
                    .eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().ok())?
            })
            .unwrap_or(0);
        while buf.len() < body_start + content_length {
            let n = stream.read(&mut tmp).await?;
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&tmp[..n]);
        }
        let mut first = head.lines().next().unwrap_or_default().split_whitespace();
        let method = first.next().unwrap_or_default().to_string();
        let path = first.next().unwrap_or_default().to_string();
        let body = String::from_utf8_lossy(&buf[body_start..]).to_string();
        let auth = head
            .lines()
            .find_map(|l| {
                let (name, value) = l.split_once(':')?;
                name.trim()
                    .eq_ignore_ascii_case("authorization")
                    .then(|| value.trim().to_string())
            })
            .unwrap_or_default();
        log.lock()
            .unwrap()
            .push((format!("{method} {path}"), format!("{auth}|{body}")));
        let (status, body) = respond(&method, &path, &body);
        let body = body.to_string();
        let resp = format!(
            "HTTP/1.1 {status} X\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
            body.len(),
        );
        stream.write_all(resp.as_bytes()).await?;
        stream.flush().await
    }

    fn temp_store() -> (tempfile::TempDir, FileSecretStore) {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = FileSecretStore::with_path(dir.path().join("secrets.json"));
        (dir, store)
    }

    fn codes_body() -> Value {
        json!({
            "device_code": "dev-secret", "user_code": "WXYZ-9876",
            "verification_uri": "http://mock/oauth/device",
            "verification_uri_complete": "http://mock/oauth/device?user_code=WXYZ-9876",
            "expires_in": 300, "interval": 5
        })
    }

    #[tokio::test]
    async fn device_grant_pending_slow_down_then_authorized_persists_tokens() {
        let polls = Arc::new(AtomicUsize::new(0));
        let mock = spawn_mock(Arc::new({
            let polls = polls.clone();
            move |method, path, _body| match (method, path) {
                ("POST", "/oauth/authorize_device") => (200, codes_body()),
                ("POST", "/oauth/token") => match polls.fetch_add(1, Ordering::SeqCst) {
                    0 => (400, json!({ "error": "authorization_pending" })),
                    1 => (400, json!({ "error": "slow_down" })),
                    _ => (
                        200,
                        json!({ "access_token": "glat-granted", "refresh_token": "glrt-granted",
                                "token_type": "Bearer", "expires_in": 7200, "scope": "api" }),
                    ),
                },
                _ => (404, json!({ "error": "unexpected" })),
            }
        }))
        .await;
        let (_dir, store) = temp_store();

        let (auth, mut flow) = start_device_grant_with_store(&mock.host, "client-1", store.clone())
            .await
            .expect("start");
        assert_eq!(auth.user_code, "WXYZ-9876");
        assert_eq!(auth.verification_uri, "http://mock/oauth/device");
        assert_eq!(
            auth.verification_uri_complete.as_deref(),
            Some("http://mock/oauth/device?user_code=WXYZ-9876")
        );
        assert_eq!((auth.expires_in, auth.interval), (300, 5));
        assert_eq!(flow.interval_secs(), 5);

        assert_eq!(flow.poll_once().await.unwrap(), GitlabPollStatus::Pending);
        assert_eq!(flow.interval_secs(), 5);
        assert_eq!(flow.poll_once().await.unwrap(), GitlabPollStatus::Pending);
        assert_eq!(flow.interval_secs(), 10, "slow_down bumps the interval");
        assert_eq!(
            store.load(SECRET_ACCOUNT).unwrap(),
            None,
            "nothing persisted yet"
        );

        assert_eq!(
            flow.poll_once().await.unwrap(),
            GitlabPollStatus::Authorized
        );
        assert_eq!(
            store.load(SECRET_ACCOUNT).unwrap().as_deref(),
            Some("glat-granted")
        );
        assert_eq!(
            store.load(REFRESH_SECRET_ACCOUNT).unwrap().as_deref(),
            Some("glrt-granted")
        );

        let requests = mock.requests();
        assert_eq!(requests[0].0, "POST /oauth/authorize_device");
        assert!(requests[0].1.contains("client_id=client-1"));
        assert!(requests[0].1.contains("scope=api"));
        assert_eq!(requests[1].0, "POST /oauth/token");
        assert!(requests[1].1.contains("device_code=dev-secret"));
        assert!(requests[1]
            .1
            .contains("grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Adevice_code"));

        revoke_gitlab_token(store.clone()).await.expect("revoke");
        assert_eq!(store.load(SECRET_ACCOUNT).unwrap(), None);
        assert_eq!(store.load(REFRESH_SECRET_ACCOUNT).unwrap(), None);
        revoke_gitlab_token(store)
            .await
            .expect("revoke twice is idempotent");
    }

    #[tokio::test]
    async fn device_grant_expired_and_denied_are_terminal_statuses() {
        for (code, expected) in [
            ("expired_token", GitlabPollStatus::Expired),
            ("access_denied", GitlabPollStatus::Denied),
        ] {
            let mock = spawn_mock(Arc::new(move |_m, path, _b| {
                if path == "/oauth/authorize_device" {
                    (200, codes_body())
                } else {
                    (400, json!({ "error": code }))
                }
            }))
            .await;
            let (_dir, store) = temp_store();
            let (_auth, mut flow) =
                start_device_grant_with_store(&mock.host, "client-1", store.clone())
                    .await
                    .unwrap();
            assert_eq!(flow.poll_once().await.unwrap(), expected, "{code}");
            assert_eq!(store.load(SECRET_ACCOUNT).unwrap(), None, "{code}");
        }
    }

    #[tokio::test]
    async fn device_grant_404_maps_to_unsupported_on_this_instance() {
        let mock = spawn_mock(Arc::new(|_m, _p, _b| {
            (404, json!({ "error": "Not Found" }))
        }))
        .await;
        let (_dir, store) = temp_store();
        let err = start_device_grant_with_store(&mock.host, "client-1", store)
            .await
            .unwrap_err();
        assert!(
            matches!(&err, Error::DeviceGrantUnsupported(host) if host == mock.host.host()),
            "{err:?}"
        );
        assert!(err.to_string().contains("personal access token"));
    }

    #[tokio::test]
    async fn device_grant_unauthorized_client_maps_to_unsupported_on_this_instance() {
        let mock = spawn_mock(Arc::new(|_m, _p, _b| {
            (
                400,
                json!({ "error": "unauthorized_client",
                        "error_description": "The client is not authorized to request a token using this method." }),
            )
        }))
        .await;
        let (_dir, store) = temp_store();
        let err = start_device_grant_with_store(&mock.host, "client-1", store)
            .await
            .unwrap_err();
        assert!(
            matches!(&err, Error::DeviceGrantUnsupported(host) if host == mock.host.host()),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn device_grant_other_oauth_errors_surface_the_code_only() {
        let mock = spawn_mock(Arc::new(|_m, _p, _b| {
            (401, json!({ "error": "invalid_client", "error_description": "unknown client", "extra": "hidden" }))
        }))
        .await;
        let (_dir, store) = temp_store();
        let err = start_device_grant_with_store(&mock.host, "client-1", store)
            .await
            .unwrap_err();
        match err {
            Error::Api(msg) => {
                assert!(msg.contains("invalid_client: unknown client"), "{msg}");
                assert!(!msg.contains("hidden"));
            }
            other => panic!("expected Api, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn device_grant_requires_a_client_id_without_any_request() {
        let mock = spawn_mock(Arc::new(|_m, _p, _b| (200, codes_body()))).await;
        let (_dir, store) = temp_store();
        let err = start_device_grant_with_store(&mock.host, "  ", store)
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Config(_)));
        assert!(mock.requests().is_empty());
    }

    // ---- PAT validation -----------------------------------------------------

    fn user_body() -> Value {
        json!({
            "id": 42, "username": "octo", "name": "Octo Cat",
            "avatar_url": "http://mock/avatar.png", "email": "hidden@example.com"
        })
    }

    #[tokio::test]
    async fn validate_pat_returns_identity_and_accepts_an_absent_scope_report() {
        let mock = spawn_mock(Arc::new(|_m, path, _b| match path {
            "/api/v4/user" => (200, user_body()),
            _ => (404, json!({ "message": "404 Not Found" })),
        }))
        .await;
        let user = validate_pat(&mock.host, " glpat-secret ")
            .await
            .expect("valid");
        assert_eq!(
            user,
            GitlabUser {
                id: 42,
                username: "octo".to_string(),
                name: Some("Octo Cat".to_string()),
                avatar_url: Some("http://mock/avatar.png".to_string()),
            }
        );
        let requests = mock.requests();
        assert_eq!(requests[0].0, "GET /api/v4/user");
        assert!(requests[0].1.starts_with("Bearer glpat-secret|"));
        assert_eq!(requests[1].0, "GET /api/v4/personal_access_tokens/self");
        let v = serde_json::to_value(&user).unwrap();
        assert_eq!(v["avatarUrl"], "http://mock/avatar.png");
    }

    #[tokio::test]
    async fn validate_pat_maps_401_to_auth_and_rejects_reported_scopes_without_api() {
        let mock = spawn_mock(Arc::new(|_m, _p, _b| {
            (401, json!({ "message": "401 Unauthorized" }))
        }))
        .await;
        let err = validate_pat(&mock.host, "glpat-bad").await.unwrap_err();
        assert!(
            matches!(&err, Error::Auth(m) if m.contains("401")),
            "{err:?}"
        );
        assert!(!err.to_string().contains("glpat-bad"));

        let mock = spawn_mock(Arc::new(|_m, path, _b| match path {
            "/api/v4/user" => (200, user_body()),
            "/api/v4/personal_access_tokens/self" => {
                (200, json!({ "id": 7, "scopes": ["read_user", "read_api"] }))
            }
            _ => (404, Value::Null),
        }))
        .await;
        let err = validate_pat(&mock.host, "glpat-narrow").await.unwrap_err();
        assert!(
            matches!(&err, Error::Auth(m) if m.contains("lacks the `api` scope")),
            "{err:?}"
        );

        let mock = spawn_mock(Arc::new(|_m, path, _b| match path {
            "/api/v4/user" => (200, user_body()),
            "/api/v4/personal_access_tokens/self" => (200, json!({ "id": 7, "scopes": ["api"] })),
            _ => (404, Value::Null),
        }))
        .await;
        assert_eq!(validate_pat(&mock.host, "glpat-api").await.unwrap().id, 42);

        assert!(matches!(
            validate_pat(&mock.host, "   ").await.unwrap_err(),
            Error::Auth(_)
        ));
    }

    #[tokio::test]
    async fn validate_pat_maps_429_and_5xx() {
        let mock = spawn_mock(Arc::new(|_m, _p, _b| (429, json!({ "message": "slow" })))).await;
        assert!(matches!(
            validate_pat(&mock.host, "glpat-x").await.unwrap_err(),
            Error::RateLimited(_)
        ));
        let mock = spawn_mock(Arc::new(|_m, _p, _b| (503, json!({ "message": "down" })))).await;
        assert!(matches!(
            validate_pat(&mock.host, "glpat-x").await.unwrap_err(),
            Error::Api(_)
        ));
    }

    #[tokio::test]
    async fn persist_gitlab_token_replaces_the_credential_and_drops_a_stale_refresh_token() {
        let (_dir, store) = temp_store();
        store.store(REFRESH_SECRET_ACCOUNT, "glrt-old").unwrap();
        persist_gitlab_token(store.clone(), SecretString::from("glpat-pasted"))
            .await
            .expect("persist");
        assert_eq!(
            store.load(SECRET_ACCOUNT).unwrap().as_deref(),
            Some("glpat-pasted")
        );
        assert_eq!(store.load(REFRESH_SECRET_ACCOUNT).unwrap(), None);
        assert_eq!(store.load(crate::token::SECRET_ACCOUNT).unwrap(), None);
    }
}
