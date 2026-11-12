//! Signed HTTP calls to the push relay (`intent-hq/intent-push`, README → Contract).

use std::collections::BTreeMap;
use std::time::Duration;

use serde_json::{json, Value};

use super::proof::DaemonKey;
use super::state::WorkspaceState;

pub(crate) const DEFAULT_RELAY_URL: &str = "https://push.intentapp.dev";
/// Test seam: a relay origin other than production. Only `https://` or a
/// loopback `http://` origin is honored.
pub(crate) const RELAY_URL_ENV: &str = "INTENTD_PUSH_RELAY_URL";
const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);

pub(crate) fn relay_url() -> String {
    std::env::var(RELAY_URL_ENV)
        .ok()
        .map(|s| s.trim().trim_end_matches('/').to_string())
        .filter(|url| {
            url.starts_with("https://")
                || ["http://127.0.0.1", "http://localhost", "http://[::1]"]
                    .iter()
                    .any(|origin| {
                        url.strip_prefix(origin)
                            .is_some_and(|rest| rest.is_empty() || rest.starts_with(':'))
                    })
        })
        .unwrap_or_else(|| DEFAULT_RELAY_URL.to_string())
}

#[derive(Debug)]
pub(crate) enum RelayError {
    Status(u16, String),
    Network(String),
}

impl RelayError {
    /// 429, 5xx and network failures: retry later. Other statuses will not
    /// succeed on retry.
    pub(crate) fn is_transient(&self) -> bool {
        match self {
            Self::Network(_) => true,
            Self::Status(code, _) => *code == 429 || *code >= 500,
        }
    }
}

impl RelayError {
    /// The relay does not know this daemon's key (`401 {code: "unknown_signer"}`).
    pub(crate) fn is_unknown_signer(&self) -> bool {
        match self {
            Self::Status(401, body) => serde_json::from_str::<Value>(body)
                .ok()
                .and_then(|v| {
                    v.get("code")
                        .and_then(Value::as_str)
                        .map(|c| c == "unknown_signer")
                })
                .unwrap_or(false),
            _ => false,
        }
    }
}

impl std::fmt::Display for RelayError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Status(code, body) => write!(f, "relay answered {code}: {body}"),
            Self::Network(e) => write!(f, "relay unreachable: {e}"),
        }
    }
}

pub(crate) struct RelayClient {
    pub base: String,
    http: reqwest::Client,
}

impl RelayClient {
    pub(crate) fn new(base: String) -> Self {
        let http = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()
            .unwrap_or_default();
        Self { base, http }
    }

    async fn send(
        &self,
        key: &DaemonKey,
        method: reqwest::Method,
        path: &str,
        body: Option<&Value>,
    ) -> Result<Option<Value>, RelayError> {
        let bytes = body
            .map(|b| serde_json::to_vec(b).unwrap_or_default())
            .unwrap_or_default();
        let iat = chrono::Utc::now().timestamp();
        let jti = uuid::Uuid::new_v4().simple().to_string();
        let proof = key.proof(method.as_str(), path, &bytes, iat, &jti);
        let mut request = self
            .http
            .request(method, format!("{}{path}", self.base))
            .header("authorization", format!("IntentProof {proof}"));
        if body.is_some() {
            request = request
                .header("content-type", "application/json")
                .body(bytes);
        }
        let response = request
            .send()
            .await
            .map_err(|e| RelayError::Network(e.to_string()))?;
        let status = response.status();
        let text = response
            .text()
            .await
            .map_err(|e| RelayError::Network(e.to_string()))?;
        if !status.is_success() {
            return Err(RelayError::Status(
                status.as_u16(),
                text.chars().take(200).collect(),
            ));
        }
        Ok(serde_json::from_str(&text).ok())
    }

    pub(crate) async fn register(&self, key: &DaemonKey) -> Result<(), RelayError> {
        let body = json!({ "publicKey": key.public_key() });
        self.send(key, reqwest::Method::POST, "/v1/daemons", Some(&body))
            .await
            .map(|_| ())
    }

    /// `{linkToken, expiresAt}`.
    pub(crate) async fn create_link(
        &self,
        key: &DaemonKey,
        subject: &str,
    ) -> Result<(String, String), RelayError> {
        let path = format!("/v1/daemons/{}/links", key.daemon_id());
        let reply = self
            .send(
                key,
                reqwest::Method::POST,
                &path,
                Some(&json!({ "subject": subject })),
            )
            .await?
            .unwrap_or_default();
        let field = |name: &str| reply.get(name).and_then(Value::as_str).map(str::to_string);
        match (field("linkToken"), field("expiresAt")) {
            (Some(token), Some(expires)) => Ok((token, expires)),
            _ => Err(RelayError::Status(
                200,
                "link reply is missing fields".into(),
            )),
        }
    }

    pub(crate) async fn revoke_link(
        &self,
        key: &DaemonKey,
        subject: &str,
    ) -> Result<(), RelayError> {
        let path = format!("/v1/daemons/{}/links/{subject}", key.daemon_id());
        match self.send(key, reqwest::Method::DELETE, &path, None).await {
            Ok(_) | Err(RelayError::Status(404, _)) => Ok(()),
            Err(e) => Err(e),
        }
    }

    pub(crate) async fn put_workspace(
        &self,
        key: &DaemonKey,
        workspace_id: &str,
        state: Option<&WorkspaceState>,
    ) -> Result<(), RelayError> {
        let path = format!("/v1/daemons/{}/workspaces/{workspace_id}", key.daemon_id());
        self.send(
            key,
            reqwest::Method::PUT,
            &path,
            Some(&json!({ "state": state })),
        )
        .await
        .map(|_| ())
    }

    pub(crate) async fn put_all(
        &self,
        key: &DaemonKey,
        workspaces: &BTreeMap<String, WorkspaceState>,
    ) -> Result<(), RelayError> {
        let path = format!("/v1/daemons/{}/workspaces", key.daemon_id());
        self.send(
            key,
            reqwest::Method::PUT,
            &path,
            Some(&json!({ "workspaces": workspaces })),
        )
        .await
        .map(|_| ())
    }
}
