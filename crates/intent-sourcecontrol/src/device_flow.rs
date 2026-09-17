//! GitHub OAuth **device flow** engine (no `gh` dependency).
//!
//! Drives the flow described in
//! <https://docs.github.com/en/apps/oauth-apps/building-oauth-apps/authorizing-oauth-apps#device-flow>:
//! [`start`] asks GitHub for a `user_code` the user types at
//! `github.com/login/device`, then [`DeviceFlow::poll_once`] polls the token
//! endpoint until the user authorizes (or the code expires / is denied). On
//! success the access token is persisted straight into the file-backed secret
//! store under account `sourceControl.github.token` — the exact slot the
//! existing resolution chain ([`crate::token`]) already reads first — so every
//! octocrab consumer picks it up with zero resolution changes.
//!
//! Only a *public* OAuth App `client_id` is needed (no client secret, no
//! callback URL). 🔒 The `access_token` and `device_code` are secrets: they
//! are never logged, never carried in any `Debug`/`Serialize` shape, and the
//! token never leaves this module — callers only see [`PollStatus`].

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use intent_core::FileSecretStore;
use secrecy::{ExposeSecret, SecretString};
use serde::Serialize;
use serde_json::{json, Value};
use tokio::time::timeout;

use crate::error::{Error, Result};
use crate::model::UserIdentity;
use crate::token::SECRET_ACCOUNT;
use crate::SourceControl;

#[cfg(test)]
use intent_core::settings_file::DEFAULT_GITHUB_OAUTH_CLIENT_ID as DEFAULT_OAUTH_CLIENT_ID;

/// Extra seconds GitHub mandates after a `slow_down` response when the reply
/// carries no explicit `interval` hint.
const SLOW_DOWN_BUMP_SECS: u64 = 5;

/// Bounded wait for a blocking secret-store write/delete before the caller
/// gives up, mirroring the write budget in `intent-services`
/// (`DEFAULT_WRITE_TIMEOUT`): callers never wait indefinitely on a wedged
/// backing filesystem (the stuck blocking task itself is abandoned, not
/// cancelled — `spawn_blocking` closures cannot be interrupted).
const SECRET_WRITE_TIMEOUT: Duration = Duration::from_secs(10);

/// Default scopes requested by the device flow (§spec: PR/issue/review work,
/// org-repo listing, workflow-file pushes, and the secret proof gist of the
/// gist identity-proof join flow — [`crate::identity_proof`]). Tokens granted
/// before `gist` was added keep working; they only need a re-authorization
/// when the user first joins a workspace as a guest.
pub const DEFAULT_SCOPES: &[&str] = &["repo", "read:org", "workflow", "gist"];

/// User-facing half of the device-flow start response. Deliberately excludes
/// the secret `device_code` (which stays inside [`DeviceFlow`]) so this shape
/// is safe to serialize onto the wire.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DeviceAuthorization {
    /// Short code the user types at [`Self::verification_uri`].
    pub user_code: String,
    /// Where the user enters the code (`https://github.com/login/device`).
    pub verification_uri: String,
    /// Seconds until the codes expire (GitHub default: 900).
    pub expires_in: u64,
    /// Minimum seconds between polls.
    pub interval: u64,
}

/// Terminal-visible poll states surfaced to callers. `slow_down` is absorbed
/// internally (the next-poll interval grows) and reported as [`Self::Pending`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PollStatus {
    /// The user authorized; the token is now persisted in the secret store.
    Authorized,
    /// The user has not entered the code yet — poll again after
    /// [`DeviceFlow::interval_secs`].
    Pending,
    /// The device/user codes expired; restart the flow.
    Expired,
    /// The user denied the authorization request.
    Denied,
    /// The user authorized, but the [`IdentityGuard`] refused the granted
    /// account: the token was discarded, nothing was persisted, and the
    /// previously stored credential (if any) is untouched. Terminal.
    Refused,
}

/// Opaque value an [`IdentityGuard`] returns on admission. The flow holds
/// it across the token write and drops it right after, so whatever the
/// guard pinned while deciding (the daemon's identity transition lock)
/// stays pinned until the credential and the identity it verified are
/// both on disk — no invite or concurrent switch can land in between.
pub type IdentityLease = Box<dyn std::any::Any + Send>;

/// Pre-persist hook of a [`DeviceFlow`] (multiplayer w4): once a grant
/// arrives, the hook receives a forge client bound to the *new* token —
/// never the token itself — and decides before anything is written.
/// `Ok(lease)` persists the token while the [`IdentityLease`] is held;
/// `Err(reason)` discards it and the poll reports [`PollStatus::Refused`].
/// The daemon uses it to verify `GET /user` against the primary
/// principal's reconnect guard so a different GitHub account cannot
/// replace the publishing credential while collaborators or open invites
/// depend on the cached identity.
pub type IdentityGuard = Arc<
    dyn Fn(
            Arc<dyn SourceControl>,
        )
            -> Pin<Box<dyn Future<Output = std::result::Result<IdentityLease, String>> + Send>>
        + Send
        + Sync,
>;

/// Opaque in-flight flow handle returned by [`start`]. Holds the secret
/// `device_code` privately; intentionally no `Debug`/`Serialize`.
pub struct DeviceFlow {
    crab: octocrab::Octocrab,
    client_id: SecretString,
    device_code: SecretString,
    interval: u64,
    store: FileSecretStore,
    /// Optional pre-persist hook plus the API base its client talks to
    /// (`None` = api.github.com).
    identity_guard: Option<(Option<String>, IdentityGuard)>,
}

/// The production login host the device flow talks to.
pub const DEFAULT_LOGIN_BASE_URI: &str = "https://github.com";

/// Build the login client the flow requires (`base_uri` + `Accept:
/// application/json`, per octocrab's `authenticate_as_device` contract).
/// Connect/read/write timeouts mirror the main client's so a dead connection
/// fails instead of pending forever (intent-hq/monorepo#1988; see
/// [`crate::github`] for the constants' rationale).
fn login_client(base_uri: &str) -> Result<octocrab::Octocrab> {
    Ok(octocrab::Octocrab::builder()
        .base_uri(base_uri)
        .map_err(|e| Error::Config(format!("invalid github login base uri: {e}")))?
        .add_header(http::header::ACCEPT, "application/json".to_string())
        .set_connect_timeout(Some(crate::github::CONNECT_TIMEOUT))
        .set_read_timeout(Some(crate::github::READ_WRITE_TIMEOUT))
        .set_write_timeout(Some(crate::github::READ_WRITE_TIMEOUT))
        .build()?)
}

/// Start a device flow: request codes for `client_id` (a *public* OAuth App
/// client id, e.g. [`intent_core::settings_file::DEFAULT_GITHUB_OAUTH_CLIENT_ID`])
/// and the given `scopes`.
/// Returns the user-facing codes plus the opaque poll handle.
///
/// # Errors
///
/// Returns [`Error::Config`] if `client_id` is empty; propagates HTTP/deserialization failures from the code request.
pub async fn start(client_id: &str, scopes: &[&str]) -> Result<(DeviceAuthorization, DeviceFlow)> {
    start_at(DEFAULT_LOGIN_BASE_URI, client_id, scopes).await
}

/// [`start`] against an explicit login `base_uri` — the test seam that lets
/// integration tests drive the full connect → poll → authorized path against
/// a local mock of `/login/device/code` + `/login/oauth/access_token` without
/// touching github.com. Production callers use [`start`].
///
/// # Errors
///
/// Returns [`Error::Config`] if `client_id` is empty; propagates HTTP/deserialization failures from the code request.
pub async fn start_at(
    base_uri: &str,
    client_id: &str,
    scopes: &[&str],
) -> Result<(DeviceAuthorization, DeviceFlow)> {
    if client_id.trim().is_empty() {
        return Err(Error::Config(
            "github device flow requires a non-empty oauth client id \
             (sourceControl.github.oauthClientId)"
                .to_string(),
        ));
    }
    let crab = login_client(base_uri)?;
    let client_id = SecretString::from(client_id.to_string());
    let codes = crab.authenticate_as_device(&client_id, scopes).await?;
    let auth = DeviceAuthorization {
        user_code: codes.user_code.clone(),
        verification_uri: codes.verification_uri.clone(),
        expires_in: codes.expires_in,
        interval: codes.interval,
    };
    let flow = DeviceFlow {
        crab,
        client_id,
        device_code: SecretString::from(codes.device_code),
        interval: codes.interval,
        store: FileSecretStore::new(),
        identity_guard: None,
    };
    Ok((auth, flow))
}

impl DeviceFlow {
    /// Minimum seconds callers must wait before the next [`Self::poll_once`]
    /// (grows when GitHub answers `slow_down`).
    pub fn interval_secs(&self) -> u64 {
        self.interval
    }

    /// Install an [`IdentityGuard`] consulted between the grant and the
    /// token write; `api_base_uri` is where its client's API calls go
    /// (`None` = api.github.com, the test seam points it at a mock).
    #[must_use]
    pub fn with_identity_guard(mut self, api_base_uri: Option<&str>, guard: IdentityGuard) -> Self {
        self.identity_guard = Some((api_base_uri.map(str::to_string), guard));
        self
    }

    /// Poll the token endpoint once. On [`PollStatus::Authorized`] the access
    /// token has already been persisted to the secret store under
    /// `sourceControl.github.token` — it is never returned to the caller.
    /// With an [`IdentityGuard`] installed, the grant is first handed to the
    /// guard as a token-bound client; a refusal drops the token unpersisted
    /// and reports [`PollStatus::Refused`], and an admission's
    /// [`IdentityLease`] is held by the token write itself until it returns
    /// — even when this caller has given up on it after
    /// [`SECRET_WRITE_TIMEOUT`].
    ///
    /// # Errors
    ///
    /// Returns an error when the token request fails, the response cannot be classified, or persisting the token to the secret store fails. Grant expiration and denial are not errors — they are reported as [`PollStatus::Expired`] and [`PollStatus::Denied`].
    pub async fn poll_once(&mut self) -> Result<PollStatus> {
        // GitHub's device-token endpoint reports pending/slow_down/expired/
        // denied as an `error` code in an HTTP 200 body. octocrab's
        // `DeviceCodes::poll_once` (untagged `TokenResponse`) cannot represent
        // the terminal errors — deserialization fails and the expired/denied
        // distinction is lost — so we post the same grant ourselves through
        // the same octocrab client and classify the raw body.
        let body: Value = self
            .crab
            .post(
                "/login/oauth/access_token",
                Some(&json!({
                    "client_id": self.client_id.expose_secret(),
                    "device_code": self.device_code.expose_secret(),
                    "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
                })),
            )
            .await?;
        match parse_poll_response(&body)? {
            PollResponse::Authorized { access_token } => {
                let mut lease: Option<IdentityLease> = None;
                if let Some((api_base_uri, guard)) = &self.identity_guard {
                    let client: Arc<dyn SourceControl> =
                        Arc::new(crate::github::GitHubSourceControl::new(
                            access_token.expose_secret(),
                            api_base_uri.as_deref(),
                        )?);
                    match guard(client).await {
                        Ok(held) => lease = Some(held),
                        Err(reason) => {
                            drop(access_token);
                            tracing::warn!(
                                reason,
                                "github device flow grant refused by identity guard"
                            );
                            return Ok(PollStatus::Refused);
                        }
                    }
                }
                persist_token(self.store.clone(), access_token, lease).await?;
                Ok(PollStatus::Authorized)
            }
            PollResponse::Pending => Ok(PollStatus::Pending),
            PollResponse::SlowDown { interval } => {
                self.interval = next_interval(self.interval, interval);
                Ok(PollStatus::Pending)
            }
            PollResponse::Expired => Ok(PollStatus::Expired),
            PollResponse::Denied => Ok(PollStatus::Denied),
        }
    }
}

/// Terminal-visible poll states of an [`IdentityFlow`] (multiplayer w4).
/// Unlike [`PollStatus`], the authorized arm carries the proven identity —
/// the access token itself was used once for `GET /user` and dropped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdentityPollStatus {
    /// The user authorized and `GET /user` resolved their identity.
    Authorized(UserIdentity),
    /// Poll again after [`IdentityFlow::interval_secs`].
    Pending,
    /// The device/user codes expired; restart the flow.
    Expired,
    /// The user denied the authorization request.
    Denied,
}

/// Identity-only device flow (multiplayer w4): the same GitHub device grant
/// as [`DeviceFlow`], requested with **no scopes**, whose access token is
/// spent on exactly one `GET /user` and never persisted — it proves *who*
/// the invitee is without granting the daemon any access as them.
pub struct IdentityFlow {
    crab: octocrab::Octocrab,
    api_base_uri: Option<String>,
    client_id: SecretString,
    device_code: SecretString,
    interval: u64,
    /// Set the moment a grant is received: the device code is spent, so no
    /// further poll can (or may) re-exchange it — see [`Self::is_spent`].
    spent: bool,
}

/// Start an identity-only device flow for `client_id` against github.com.
///
/// # Errors
///
/// Returns [`Error::Config`] if `client_id` is empty; propagates HTTP/deserialization failures from the code request.
pub async fn start_identity(client_id: &str) -> Result<(DeviceAuthorization, IdentityFlow)> {
    start_identity_at(DEFAULT_LOGIN_BASE_URI, None, client_id).await
}

/// [`start_identity`] against an explicit login `base_uri` and API base
/// (`None` = api.github.com) — the test seam for a local mock of
/// `/login/device/code`, `/login/oauth/access_token` and `/user`.
///
/// # Errors
///
/// Returns [`Error::Config`] if `client_id` is empty; propagates HTTP/deserialization failures from the code request.
pub async fn start_identity_at(
    base_uri: &str,
    api_base_uri: Option<&str>,
    client_id: &str,
) -> Result<(DeviceAuthorization, IdentityFlow)> {
    if client_id.trim().is_empty() {
        return Err(Error::Config(
            "github device flow requires a non-empty oauth client id \
             (sourceControl.github.oauthClientId)"
                .to_string(),
        ));
    }
    let crab = login_client(base_uri)?;
    let client_id = SecretString::from(client_id.to_string());
    let codes = crab
        .authenticate_as_device(&client_id, &[] as &[&str])
        .await?;
    let auth = DeviceAuthorization {
        user_code: codes.user_code.clone(),
        verification_uri: codes.verification_uri.clone(),
        expires_in: codes.expires_in,
        interval: codes.interval,
    };
    let flow = IdentityFlow {
        crab,
        api_base_uri: api_base_uri.map(str::to_string),
        client_id,
        device_code: SecretString::from(codes.device_code),
        interval: codes.interval,
        spent: false,
    };
    Ok((auth, flow))
}

impl IdentityFlow {
    /// Minimum seconds callers must wait before the next [`Self::poll_once`]
    /// (grows when GitHub answers `slow_down`).
    pub fn interval_secs(&self) -> u64 {
        self.interval
    }

    /// True once a grant was received: every post-grant outcome is
    /// terminal. A failed identity lookup after the grant is reported as an
    /// error, but the grant is not polled again — the device code has been
    /// discarded and a further [`Self::poll_once`] fails without any request.
    pub fn is_spent(&self) -> bool {
        self.spent
    }

    /// Poll the token endpoint once. On authorization the token is used for
    /// a single `GET /user` and dropped; only the resolved identity returns.
    ///
    /// # Errors
    ///
    /// Returns an error when the token request or the identity lookup fails, or the response cannot be classified. Grant expiration and denial are not errors — they are reported as [`IdentityPollStatus::Expired`] and [`IdentityPollStatus::Denied`]. Once [`Self::is_spent`], every call fails immediately.
    pub async fn poll_once(&mut self) -> Result<IdentityPollStatus> {
        if self.spent {
            return Err(Error::Api(
                "identity device grant already consumed; start a new flow".to_string(),
            ));
        }
        let body: Value = self
            .crab
            .post(
                "/login/oauth/access_token",
                Some(&json!({
                    "client_id": self.client_id.expose_secret(),
                    "device_code": self.device_code.expose_secret(),
                    "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
                })),
            )
            .await?;
        match parse_poll_response(&body)? {
            PollResponse::Authorized { access_token } => {
                // The grant is single-use: retire the device code before the
                // lookup so an identity failure cannot lead to a re-poll.
                self.spent = true;
                self.device_code = SecretString::from(String::new());
                let client = crate::github::GitHubSourceControl::new(
                    access_token.expose_secret(),
                    self.api_base_uri.as_deref(),
                )?;
                drop(access_token);
                let identity = client.get_user().await?;
                Ok(IdentityPollStatus::Authorized(identity))
            }
            PollResponse::Pending => Ok(IdentityPollStatus::Pending),
            PollResponse::SlowDown { interval } => {
                self.interval = next_interval(self.interval, interval);
                Ok(IdentityPollStatus::Pending)
            }
            PollResponse::Expired => Ok(IdentityPollStatus::Expired),
            PollResponse::Denied => Ok(IdentityPollStatus::Denied),
        }
    }
}

/// Classified device-token poll response (crate-private: the authorized arm
/// carries the raw token, which must not escape this module).
enum PollResponse {
    Authorized { access_token: SecretString },
    Pending,
    SlowDown { interval: Option<u64> },
    Expired,
    Denied,
}

/// Manual `Debug` so the authorized arm's token can never leak through
/// formatting (tests and error paths format this type).
impl std::fmt::Debug for PollResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Authorized { .. } => f.write_str("Authorized { access_token: <redacted> }"),
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

/// Classify a device-token poll body per the GitHub device-flow error table.
/// Unknown error codes (bad client id, disabled device flow, …) are
/// non-retryable and surface as [`Error::Api`] carrying only the error code —
/// never the raw body, which could contain credential material.
fn parse_poll_response(body: &Value) -> Result<PollResponse> {
    if let Some(token) = body.get("access_token").and_then(Value::as_str) {
        if !token.is_empty() {
            return Ok(PollResponse::Authorized {
                access_token: SecretString::from(token.to_string()),
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
        Some(other) => Err(Error::Api(format!("github device flow error: {other}"))),
        None => Err(Error::Decode(
            "unrecognized github device-flow poll response".to_string(),
        )),
    }
}

/// Next poll interval after a `slow_down`: at least the mandated
/// current + 5s, growing further to GitHub's hinted `interval` when the hint
/// is larger — never shrinking below the mandated bump.
fn next_interval(current: u64, hinted: Option<u64>) -> u64 {
    let bumped = current.saturating_add(SLOW_DOWN_BUMP_SECS);
    hinted.unwrap_or(bumped).max(bumped)
}

/// Persist an access token into `store` under `sourceControl.github.token`
/// (the first slot of the existing resolution chain). Runs on the blocking
/// pool like the loads in [`crate::token`], bounded by
/// [`SECRET_WRITE_TIMEOUT`] so a wedged filesystem cannot hang the caller.
/// The [`IdentityLease`] travels with the write and is released only once
/// it returns, so a caller that gives up on the timeout cannot release the
/// identity transition while the abandoned write is still in flight.
async fn persist_token(
    store: FileSecretStore,
    token: SecretString,
    lease: Option<IdentityLease>,
) -> Result<()> {
    persist_with(
        move || store.store(SECRET_ACCOUNT, token.expose_secret()),
        lease,
        SECRET_WRITE_TIMEOUT,
    )
    .await
}

/// Run the blocking `write` on the pool with `lease` held inside the closure
/// until the write returns, waiting at most `budget` for the result.
async fn persist_with<F>(write: F, lease: Option<IdentityLease>, budget: Duration) -> Result<()>
where
    F: FnOnce() -> intent_core::Result<()> + Send + 'static,
{
    let handle = tokio::task::spawn_blocking(move || {
        let result = write();
        drop(lease);
        result
    });
    match timeout(budget, handle).await {
        Ok(Ok(Ok(()))) => Ok(()),
        Ok(Ok(Err(e))) => Err(Error::Api(format!("could not persist github token: {e}"))),
        Ok(Err(join_err)) => Err(Error::Api(format!(
            "secret-store write task failed: {join_err}"
        ))),
        Err(_) => Err(Error::Api(
            "secret-store write timed out for sourceControl.github.token".to_string(),
        )),
    }
}

/// Delete the stored `sourceControl.github.token` entry from `store` (revoke /
/// disconnect). Absence is an idempotent success, mirroring
/// [`FileSecretStore::delete`], and the blocking delete is bounded by
/// [`SECRET_WRITE_TIMEOUT`]. Env / `gh` fallbacks are untouched.
#[cfg(test)]
pub(crate) async fn revoke_token(store: FileSecretStore) -> Result<()> {
    let handle = tokio::task::spawn_blocking(move || store.delete(SECRET_ACCOUNT));
    match timeout(SECRET_WRITE_TIMEOUT, handle).await {
        Ok(Ok(Ok(()))) => Ok(()),
        Ok(Ok(Err(e))) => Err(Error::Api(format!("could not delete github token: {e}"))),
        Ok(Err(join_err)) => Err(Error::Api(format!(
            "secret-store delete task failed: {join_err}"
        ))),
        Err(_) => Err(Error::Api(
            "secret-store delete timed out for sourceControl.github.token".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn classify(body: &Value) -> PollResponse {
        parse_poll_response(body).expect("classifiable response")
    }

    #[test]
    fn authorized_when_access_token_present() {
        let r = classify(&json!({
            "access_token": "gho_test_value",
            "token_type": "bearer",
            "scope": "repo,read:org,workflow"
        }));
        assert!(matches!(
            r,
            PollResponse::Authorized { access_token } if access_token.expose_secret() == "gho_test_value"
        ));
    }

    #[test]
    fn empty_access_token_is_not_authorized() {
        let err = parse_poll_response(&json!({ "access_token": "" })).unwrap_err();
        assert!(matches!(err, Error::Decode(_)));
    }

    #[test]
    fn pending_and_terminal_error_codes_classify() {
        assert!(matches!(
            classify(&json!({ "error": "authorization_pending" })),
            PollResponse::Pending
        ));
        assert!(matches!(
            classify(&json!({ "error": "expired_token" })),
            PollResponse::Expired
        ));
        assert!(matches!(
            classify(&json!({ "error": "access_denied" })),
            PollResponse::Denied
        ));
    }

    #[test]
    fn slow_down_carries_the_optional_interval_hint() {
        assert!(matches!(
            classify(&json!({ "error": "slow_down", "interval": 10 })),
            PollResponse::SlowDown { interval: Some(10) }
        ));
        assert!(matches!(
            classify(&json!({ "error": "slow_down" })),
            PollResponse::SlowDown { interval: None }
        ));
    }

    #[test]
    fn unknown_error_code_is_a_non_retryable_api_error() {
        let err = parse_poll_response(&json!({ "error": "device_flow_disabled" })).unwrap_err();
        match err {
            Error::Api(msg) => assert!(msg.contains("device_flow_disabled")),
            other => panic!("expected Api error, got {other:?}"),
        }
    }

    #[test]
    fn unrecognized_body_is_a_decode_error_without_the_body() {
        let err = parse_poll_response(&json!({ "unexpected": "shape" })).unwrap_err();
        match err {
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

    #[tokio::test]
    async fn persist_then_revoke_round_trips_through_a_temp_store() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = FileSecretStore::with_path(dir.path().join("secrets.json"));

        persist_token(store.clone(), SecretString::from("gho_roundtrip"), None)
            .await
            .expect("persist");
        assert_eq!(
            store.load("sourceControl.github.token").expect("load"),
            Some("gho_roundtrip".to_string())
        );

        revoke_token(store.clone()).await.expect("revoke");
        assert_eq!(
            store.load("sourceControl.github.token").expect("load"),
            None
        );

        // Revoking an already-absent entry stays an idempotent success.
        revoke_token(store).await.expect("revoke twice");
    }

    /// The lease outlives the caller's wait: when the write is held past the
    /// budget the caller times out, but the lease stays held until the
    /// blocking write actually returns, and is released right after.
    #[tokio::test]
    async fn lease_is_held_until_the_abandoned_write_returns() {
        struct Released(Arc<std::sync::atomic::AtomicBool>);
        impl Drop for Released {
            fn drop(&mut self) {
                self.0.store(true, std::sync::atomic::Ordering::SeqCst);
            }
        }
        let released = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let lease: IdentityLease = Box::new(Released(released.clone()));
        let (entered_tx, entered_rx) = std::sync::mpsc::channel::<()>();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let (returned_tx, returned_rx) = std::sync::mpsc::channel::<()>();

        let err = persist_with(
            move || {
                entered_tx.send(()).expect("signal entry");
                release_rx.recv().expect("wait for release");
                returned_tx.send(()).expect("signal return");
                Ok(())
            },
            Some(lease),
            Duration::from_millis(50),
        )
        .await
        .expect_err("caller times out while the write is held");
        assert!(matches!(&err, Error::Api(msg) if msg.contains("timed out")));

        entered_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("write entered");
        assert!(
            !released.load(std::sync::atomic::Ordering::SeqCst),
            "the lease must not be released while the write is still in flight"
        );

        release_tx.send(()).expect("release the writer");
        returned_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("write returned");
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while !released.load(std::sync::atomic::Ordering::SeqCst) {
            assert!(
                std::time::Instant::now() < deadline,
                "the lease is released once the write returns"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    #[test]
    fn device_authorization_serializes_camel_case_without_device_code() {
        let auth = DeviceAuthorization {
            user_code: "ABCD-1234".to_string(),
            verification_uri: "https://github.com/login/device".to_string(),
            expires_in: 900,
            interval: 5,
        };
        let v = serde_json::to_value(&auth).expect("serialize");
        assert_eq!(v["userCode"], "ABCD-1234");
        assert_eq!(v["verificationUri"], "https://github.com/login/device");
        assert_eq!(v["expiresIn"], 900);
        assert_eq!(v["interval"], 5);
        assert!(v.get("deviceCode").is_none());
    }

    #[test]
    fn default_scopes_and_client_id_match_the_registered_oauth_app() {
        assert_eq!(DEFAULT_SCOPES, &["repo", "read:org", "workflow", "gist"]);
        assert!(DEFAULT_SCOPES.contains(&crate::identity_proof::REQUIRED_SCOPE));
        assert_eq!(DEFAULT_OAUTH_CLIENT_ID, "Ov23li8bvmPsd4B4pW38");
    }
}
