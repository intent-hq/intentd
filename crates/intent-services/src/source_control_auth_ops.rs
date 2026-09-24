//! Provider-generic forge auth (`sourceControl.authStatus` / `connect` /
//! `cancelAuth` / `revoke` / `getUser`, PROTOCOL §5.27 "Provider-generic auth
//! — `sourceControl.*`", v10.5): param parsing, the GitLab device-grant slot +
//! poll loop, the GitLab credential probe with proactive / 401-triggered
//! refresh, and the wire DTOs. The `github.*` auth quintet is served as
//! aliases of these with `provider: "github"` pinned (see `lib.rs`); the
//! GitHub device flow itself stays in [`crate::github_auth_ops`].
//!
//! 🔒 Tokens, device codes and refresh tokens never cross this module's
//! boundary: the engine (`intent_sourcecontrol::gitlab_auth`) persists them
//! straight into the file-backed secret store (an authorized device grant
//! travels only as the opaque `GitlabGrant` this module commits or drops),
//! and every DTO here carries only user-facing codes, derived identity and
//! connection state.

use std::collections::HashSet;
use std::sync::Arc;

use intent_core::events::SOURCE_CONTROL_AUTH_CHANGED;
use intent_core::{now_iso, Error, FileSecretStore, IdentityProofErrorKind, Result, WorkspaceId};
use intent_sourcecontrol::gitlab_auth::{
    refresh_access_token, revoke_gitlab_token, stored_credential, validate_pat,
};
use intent_sourcecontrol::gitlab_token::SECRET_ACCOUNT as GITLAB_SECRET_ACCOUNT;
use intent_sourcecontrol::identity_proof::provider::{CreatedProof, ProofProvider};
use intent_sourcecontrol::{
    GitlabDeviceFlow, GitlabExchange, GitlabHost, GitlabUser, StoredCredential, UserIdentity,
};
use intent_store::NewEvent;
use serde_json::{json, Value};
use tokio::time::Instant;

use crate::events::EventBus;
use crate::github_auth_ops::{self, FlowPhase, FlowSlot, MAX_CONSECUTIVE_POLL_ERRORS};
use crate::{publish_event, system_actor};

/// Env override for the origin GitLab API / OAuth calls go to — the
/// spawned-daemon test seam (consulted only when
/// `sourceControl.gitlab.apiBaseUrl` is unset; same loopback-only cleartext
/// rule as the GitHub login override, enforced by `GitlabHost::parse`).
pub(crate) const GITLAB_API_BASE_URI_ENV: &str = "INTENTD_GITLAB_API_BASE_URI";

/// Env var the GitLab resolution chain falls back to when nothing is stored.
pub(crate) const GITLAB_TOKEN_ENV: &str = "GITLAB_TOKEN";

/// Secret-store marker written next to `sourceControl.github.token` when the
/// GitHub device flow authorized, so `sourceControl.authStatus` can report
/// `method: "device"` for it; a token without the marker (settings.update
/// PAT, pre-10.5) reports `"pat"`. Deleted with the token on revoke.
pub(crate) const GITHUB_TOKEN_METHOD_ACCOUNT: &str = "sourceControl.github.tokenMethod";

/// The `host` every GitHub auth result reports.
pub(crate) const GITHUB_HOST: &str = "github.com";

/// `provider` param of every `sourceControl.*` method.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Provider {
    Github,
    Gitlab,
}

impl Provider {
    /// Parse the wire `provider`; anything but `github` / `gitlab` → `-32602`.
    pub(crate) fn parse(raw: &str) -> Result<Self> {
        match raw.trim() {
            "github" => Ok(Self::Github),
            "gitlab" => Ok(Self::Gitlab),
            other => Err(Error::InvalidParams(format!(
                "provider must be \"github\" or \"gitlab\" (got {other:?})"
            ))),
        }
    }

    pub(crate) fn as_wire(self) -> &'static str {
        match self {
            Self::Github => "github",
            Self::Gitlab => "gitlab",
        }
    }
}

/// A validated `(provider, host)` target. `host` is gitlab-only; the GitHub
/// side has no instance selection in v10.5.
#[derive(Debug, Clone)]
pub(crate) enum Target {
    Github,
    /// Whether `host` is the bound instance (`sourceControl.gitlab.host`) —
    /// the only host whose stored token applies — is deliberately NOT
    /// snapshotted here: handlers re-read it (`Services::gitlab_host_is_bound`)
    /// at the moment they act, under the [`GitlabCredentialGate`].
    Gitlab {
        host: GitlabHost,
    },
}

/// Validate the wire `host` for gitlab: a bare `host[:port]` — no scheme, no
/// path, no credentials (`-32602` otherwise) — normalized by
/// [`GitlabHost::parse`] (lowercase, `https://`).
pub(crate) fn parse_gitlab_host(raw: &str) -> Result<GitlabHost> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(Error::InvalidParams("host must not be empty".to_string()));
    }
    if trimmed.contains("://")
        || trimmed.contains('/')
        || trimmed.contains('@')
        || trimmed.contains('?')
        || trimmed.contains('#')
        || trimmed.chars().any(char::is_whitespace)
    {
        return Err(Error::InvalidParams(format!(
            "host must be a bare host[:port] without scheme or path (got {trimmed:?})"
        )));
    }
    GitlabHost::parse(trimmed).map_err(|e| Error::InvalidParams(format!("invalid host: {e}")))
}

/// Resolve the `(provider, host)` target of a `sourceControl.*` call.
/// `configured_host` is `sourceControl.gitlab.host` (the default and the
/// bound instance); `api_origin` is the optional origin override applied to
/// the bound host only (`sourceControl.gitlab.apiBaseUrl`, else the env
/// seam) — it never changes the reported host.
pub(crate) fn resolve_target(
    provider: Provider,
    host: Option<&str>,
    configured_host: &str,
    api_origin: Option<&str>,
) -> Result<Target> {
    let host = host.map(str::trim).filter(|h| !h.is_empty());
    match provider {
        Provider::Github => match host {
            Some(h) => Err(Error::InvalidParams(format!(
                "host is only accepted for provider \"gitlab\" (got {h:?})"
            ))),
            None => Ok(Target::Github),
        },
        Provider::Gitlab => {
            let bound_host = parse_gitlab_host(configured_host).map_err(|e| {
                Error::Internal(format!("sourceControl.gitlab.host is invalid: {e}"))
            })?;
            let mut resolved = match host {
                Some(h) => parse_gitlab_host(h)?,
                None => bound_host.clone(),
            };
            if resolved.host() == bound_host.host() {
                if let Some(origin) = api_origin.map(str::trim).filter(|o| !o.is_empty()) {
                    resolved = resolved.with_api_origin(origin).map_err(|e| {
                        Error::Internal(format!("gitlab api origin override rejected: {e}"))
                    })?;
                }
            }
            Ok(Target::Gitlab { host: resolved })
        }
    }
}

/// The GitLab device-grant slot: the single in-flight (or last-terminal)
/// flow, tagged with the instance it targets so `cancelAuth` / `revoke` /
/// `authStatus` act on exactly `(gitlab, host)`.
pub(crate) struct GitlabFlowSlot {
    pub(crate) host: String,
    pub(crate) slot: FlowSlot,
}

/// Shared GitLab auth state: the flow slot plus the hosts that reported the
/// device grant unsupported (404 / `unauthorized_client`), which flips
/// `deviceGrantSupported` to `false` until the daemon restarts.
#[derive(Default)]
pub(crate) struct GitlabAuthState {
    pub(crate) flow: Option<GitlabFlowSlot>,
    pub(crate) unsupported_hosts: HashSet<String>,
}

pub(crate) type GitlabAuthStateHandle = Arc<tokio::sync::Mutex<GitlabAuthState>>;

pub(crate) fn new_gitlab_state() -> GitlabAuthStateHandle {
    Arc::new(tokio::sync::Mutex::new(GitlabAuthState::default()))
}

/// Serialises every mutation of the stored GitLab credential pair — token
/// refresh (rotation), PAT persist, the device completion's commit, revoke /
/// expiry deletion — and every credential read a probe makes, so that two
/// concurrent probes never race a rotation (the instance accepts a refresh
/// token once; a second exchange with the consumed one is `invalid_grant`
/// and would disconnect a live connection) and a stale device completion
/// never overwrites or deletes a newer connection.
///
/// The contract every credential mutation follows (the daemon holds ONE
/// credential, for ONE bound host, so any path that acted on state read
/// before it waited here has already caused a lost or leaked credential):
///
/// 1. take this gate;
/// 2. inside the hold, re-read what the action is conditioned on — the host
///    binding (`Services::gitlab_host_is_bound`, never a resolve-time
///    snapshot) and the generation it was started for: the device
///    completion's slot residency (`flow_id`), a refresh / disconnect's
///    stored access token (still the one it read / the instance rejected);
/// 3. act only while that still holds — otherwise a no-op with no store
///    write, no binding change and no event;
/// 4. store write, slot / binding update and `sourceControl:auth-changed`
///    ride the same hold, so subscribers observe events in store order.
///
/// | path | gate | re-read under the hold | acts only if |
/// |---|---|---|---|
/// | PAT connect (`gitlab_connect_pat`) | yes | — (a user action: it *sets* the binding) | always; supersedes any pending flow |
/// | device completion (`run_gitlab_poll_loop`) | yes, across the exchange | slot residency | still resident after the exchange |
/// | `cancelAuth` | no — slot only, never the store, no event | slot host + phase | a pending flow for that host |
/// | `revoke` | yes | binding, stored credential | host bound now and something stored |
/// | proactive / 401 refresh (`probe_gitlab`) | yes, across the exchange | binding, stored pair | bound and the stored token is the one it read |
/// | disconnect on failed refresh | yes | binding, stored token | bound and the stored token is the rejected one |
///
/// Held across the device poll's token exchange and the refresh exchange but
/// never across the `GET /api/v4/user` probe itself. Lock order: this gate
/// first, the [`GitlabAuthStateHandle`] slot lock only ever nested inside it.
pub(crate) type GitlabCredentialGate = Arc<tokio::sync::Mutex<()>>;

pub(crate) fn new_gitlab_credential_gate() -> GitlabCredentialGate {
    Arc::new(tokio::sync::Mutex::new(()))
}

/// Build a `sourceControl:auth-changed { provider, host, status }` event —
/// global like `settings:changed` (empty workspace id); never a token.
pub(crate) fn auth_changed_event(provider: Provider, host: &str, status: &str) -> NewEvent {
    NewEvent {
        workspace_id: WorkspaceId::from_string(String::new()),
        timestamp: now_iso(),
        event_type: SOURCE_CONTROL_AUTH_CHANGED.to_string(),
        actor: system_actor(),
        session_id: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data: json!({ "provider": provider.as_wire(), "host": host, "status": status }),
    }
}

/// Publish the transition for `(provider, host)`: GitHub transitions emit
/// the unchanged `github:auth-changed { status }` first, then every provider
/// emits `sourceControl:auth-changed`.
pub(crate) async fn publish_auth_changed(
    bus: Option<&EventBus>,
    provider: Provider,
    host: &str,
    status: &str,
) {
    if provider == Provider::Github {
        publish_event(bus, github_auth_ops::auth_changed_event(status)).await;
    }
    publish_event(bus, auth_changed_event(provider, host, status)).await;
}

/// True iff this task's flow is still the resident slot for `host`.
async fn is_resident(state: &GitlabAuthStateHandle, flow_id: u64) -> bool {
    let guard = state.lock().await;
    matches!(guard.flow.as_ref(), Some(f) if f.slot.flow_id == flow_id)
}

/// The daemon-owned GitLab poll loop `sourceControl.connect` spawns — the
/// gitlab twin of [`github_auth_ops::run_poll_loop`] (same cooperative
/// cancellation: the task exits at its next tick once cancel / revoke / a
/// newer connect replaced the slot). On an authorized exchange the grant is
/// committed to the store only if this flow is **still** the resident slot;
/// then the slot is cleared, the host is bound (`sourceControl.gitlab.host`)
/// and `sourceControl:auth-changed` is emitted. A completion whose slot was
/// cancelled or replaced while its exchange was in flight drops the grant
/// uncommitted — the store (a PAT connected before the flow, a newer
/// connection) is left exactly as it was.
///
/// Every exchange runs under the [`GitlabCredentialGate`]: the residency
/// check, the exchange, the residency re-check and the commit + slot clear +
/// bind are one atomic step against a PAT connect, revoke or refresh on the
/// same store.
#[expect(clippy::too_many_arguments)]
pub(crate) async fn run_gitlab_poll_loop(
    state: GitlabAuthStateHandle,
    bus: Option<EventBus>,
    registry: Option<Arc<crate::SettingsRegistry>>,
    gate: GitlabCredentialGate,
    flow_id: u64,
    host: String,
    mut flow: GitlabDeviceFlow,
    deadline: Instant,
) {
    let mut consecutive_errors: u32 = 0;
    let phase: FlowPhase = loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break FlowPhase::Expired;
        }
        tokio::time::sleep(github_auth_ops::poll_sleep(flow.interval_secs()).min(remaining)).await;
        let _gate = gate.lock().await;
        if !is_resident(&state, flow_id).await {
            tracing::info!(host, "gitlab device grant superseded; poll loop stopped");
            return;
        }
        if Instant::now() >= deadline {
            break FlowPhase::Expired;
        }
        match flow.exchange_once().await {
            Ok(GitlabExchange::Pending) => consecutive_errors = 0,
            Ok(GitlabExchange::Authorized(grant)) => {
                let mut guard = state.lock().await;
                if !matches!(guard.flow.as_ref(), Some(f) if f.slot.flow_id == flow_id) {
                    drop(grant);
                    tracing::info!(
                        host,
                        "gitlab device grant superseded before commit; discarded"
                    );
                    return;
                }
                if let Err(e) = grant.commit().await {
                    // The instance issued the grant once; there is nothing
                    // left to poll for.
                    tracing::warn!(error = %e, host, "could not persist gitlab device grant");
                    drop(guard);
                    break FlowPhase::Error;
                }
                guard.flow = None;
                drop(guard);
                bind_gitlab_host(registry.as_deref(), &host);
                tracing::info!(status = "authorized", host, "gitlab device grant finished");
                publish_auth_changed(bus.as_ref(), Provider::Gitlab, &host, "authorized").await;
                return;
            }
            Ok(GitlabExchange::Expired) => break FlowPhase::Expired,
            Ok(GitlabExchange::Denied) => break FlowPhase::Denied,
            Err(e) => {
                consecutive_errors += 1;
                tracing::warn!(
                    error = %e,
                    consecutive_errors,
                    host,
                    "gitlab device-grant poll failed"
                );
                if consecutive_errors >= MAX_CONSECUTIVE_POLL_ERRORS {
                    break FlowPhase::Error;
                }
            }
        }
    };
    {
        let mut guard = state.lock().await;
        match guard.flow.as_mut() {
            Some(f) if f.slot.flow_id == flow_id => f.slot.phase = phase,
            _ => return,
        }
    }
    let status = phase.as_wire();
    tracing::info!(status, host, "gitlab device grant finished");
    publish_auth_changed(bus.as_ref(), Provider::Gitlab, &host, status).await;
}

/// Persist `host` as `sourceControl.gitlab.host` (the bound instance) after a
/// successful connect. Fail-soft: a pinned key or a missing registry
/// (read-only wiring) only logs — the credential is already stored.
pub(crate) fn bind_gitlab_host(registry: Option<&crate::SettingsRegistry>, host: &str) {
    let Some(registry) = registry else {
        return;
    };
    if registry
        .get("sourceControl.gitlab.host")
        .and_then(|v| v.as_str().map(str::to_string))
        == Some(host.to_string())
    {
        return;
    }
    if let Err(e) = registry.apply(&[("sourceControl.gitlab.host".to_string(), json!(host))]) {
        tracing::warn!(error = %e, host, "could not persist sourceControl.gitlab.host");
    }
}

/// Where the GitLab credential probe landed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ProbeOutcome {
    /// No credential resolves for the host (or a device credential whose
    /// refresh failed was just cleared).
    NotConfigured,
    /// The host accepted the credential; `method` is its provenance.
    Configured {
        user: GitlabUser,
        method: &'static str,
    },
    /// The host rejected the credential in use.
    Rejected,
}

/// The stored GitLab access token, `None` when the slot is absent / blank.
async fn stored_access_token(store: &FileSecretStore) -> Result<Option<String>> {
    let store = store.clone();
    let loaded = tokio::task::spawn_blocking(move || store.load(GITLAB_SECRET_ACCOUNT))
        .await
        .map_err(|e| Error::Internal(format!("secret-store read task failed: {e}")))?
        .map_err(|e| Error::Internal(format!("could not read gitlab token: {e}")))?;
    Ok(loaded.filter(|t| !t.trim().is_empty()))
}

/// Read the GitLab access token for a probe of the **bound** instance: the
/// stored slot, then `GITLAB_TOKEN`. Every credential the daemon holds —
/// stored or environment — belongs to the bound instance only, so callers
/// never resolve one for another host (see [`probe_gitlab`]). Returns the
/// token with its provenance; `None` when nothing resolves.
async fn load_gitlab_token(
    store: &FileSecretStore,
    credential: StoredCredential,
) -> Result<Option<(String, &'static str)>> {
    if credential != StoredCredential::None {
        if let Some(token) = stored_access_token(store).await? {
            let method = match credential {
                StoredCredential::Device { .. } => "device",
                _ => "pat",
            };
            return Ok(Some((token, method)));
        }
    }
    Ok(std::env::var(GITLAB_TOKEN_ENV)
        .ok()
        .map(|t| t.trim().to_string())
        .filter(|t| !t.is_empty())
        .map(|t| (t, "env")))
}

/// Clear a device-grant connection whose access token can no longer be
/// renewed, and tell subscribers (`status: "expired"`). The caller holds the
/// [`GitlabCredentialGate`].
async fn disconnect_gitlab(store: FileSecretStore, bus: Option<&EventBus>, host: &str) {
    if let Err(e) = revoke_gitlab_token(store).await {
        tracing::warn!(error = %e, host, "could not clear the gitlab credential");
    }
    publish_auth_changed(bus, Provider::Gitlab, host, "expired").await;
}

/// [`disconnect_gitlab`] only if `host` is still the bound instance and the
/// stored access token is still `rejected` — a peer probe that rotated the
/// pair in the meantime holds a credential the instance never saw fail, and
/// that connection stays; a host that lost the binding no longer owns the
/// stored credential.
async fn disconnect_gitlab_if_current(
    store: FileSecretStore,
    bound: &(dyn Fn() -> bool + Sync),
    gate: &GitlabCredentialGate,
    bus: Option<&EventBus>,
    host: &str,
    rejected: &str,
) -> Result<()> {
    let _gate = gate.lock().await;
    if bound() && stored_access_token(&store).await?.as_deref() == Some(rejected) {
        disconnect_gitlab(store, bus, host).await;
    }
    Ok(())
}

/// Probe the credential in use for `host` against `GET /api/v4/user`, running
/// the device-grant refresh policy around it: a stored device credential is
/// refreshed **proactively** when its recorded expiry is near / past, and
/// **once more on a 401** before the credential is declared rejected. A
/// refresh the instance refuses (`invalid_grant`) clears the connection
/// (`sourceControl:auth-changed { status: "expired" }`) and reports
/// [`ProbeOutcome::NotConfigured`]; a transport failure during refresh keeps
/// the stored pair and falls through to the probe. PAT / env credentials are
/// never refreshed.
///
/// An **unbound** host (not the configured instance) never resolves a
/// credential — stored or `GITLAB_TOKEN` — and is answered
/// [`ProbeOutcome::NotConfigured`] without a request, so a typo'd / switched
/// host never receives the bound instance's secret. `bound` is evaluated
/// under `gate` right before every credential read (not once up front): a
/// probe that waited on the gate re-reads the binding it may have lost in
/// the meantime, so the binding check and the read it guards are one atomic
/// step against every gated writer.
///
/// Both refresh paths run under `gate` and re-read the stored pair first: a
/// concurrent probe may already have rotated it, in which case this one just
/// uses the rotated token instead of replaying the consumed refresh token
/// (which the instance would refuse, disconnecting a live connection). A
/// disconnect only happens while the stored token is still the one rejected.
///
/// # Errors
///
/// Rate limiting and non-auth forge failures propagate (mapped like the
/// other `github.*` methods); a rejected credential is an outcome, not an
/// error.
pub(crate) async fn probe_gitlab(
    host: &GitlabHost,
    bound: &(dyn Fn() -> bool + Sync),
    client_id: Option<&str>,
    store: FileSecretStore,
    gate: &GitlabCredentialGate,
    bus: Option<&EventBus>,
) -> Result<ProbeOutcome> {
    let (token, method, refreshed) = {
        let _gate = gate.lock().await;
        if !bound() {
            return Ok(ProbeOutcome::NotConfigured);
        }
        let mut credential = stored_credential(store.clone())
            .await
            .map_err(crate::pr_ops::map_sc_err)?;
        let mut refreshed = false;
        if credential.needs_refresh() {
            match try_refresh(host, client_id, store.clone()).await {
                Ok(()) => {
                    refreshed = true;
                    credential = stored_credential(store.clone())
                        .await
                        .map_err(crate::pr_ops::map_sc_err)?;
                }
                Err(RefreshFailure::Unrecoverable) => {
                    disconnect_gitlab(store, bus, host.host()).await;
                    return Ok(ProbeOutcome::NotConfigured);
                }
                Err(RefreshFailure::Transient) => {}
            }
        }
        let Some((token, method)) = load_gitlab_token(&store, credential).await? else {
            return Ok(ProbeOutcome::NotConfigured);
        };
        (token, method, refreshed)
    };
    match validate_pat(host, &token).await {
        Ok(user) => return Ok(ProbeOutcome::Configured { user, method }),
        Err(intent_sourcecontrol::Error::Auth(_)) if method == "device" && !refreshed => {}
        Err(intent_sourcecontrol::Error::Auth(_)) => return Ok(ProbeOutcome::Rejected),
        Err(e) => return Err(crate::pr_ops::map_sc_err(e)),
    }
    // 401 on a device credential: one refresh, one retry. Skip the exchange
    // when a peer already replaced the rejected token; retry with theirs.
    let (token, method) = {
        let _gate = gate.lock().await;
        if !bound() {
            return Ok(ProbeOutcome::NotConfigured);
        }
        if stored_access_token(&store).await?.as_deref() == Some(token.as_str()) {
            match try_refresh(host, client_id, store.clone()).await {
                Ok(()) => {}
                Err(RefreshFailure::Unrecoverable) => {
                    disconnect_gitlab(store, bus, host.host()).await;
                    return Ok(ProbeOutcome::NotConfigured);
                }
                Err(RefreshFailure::Transient) => return Ok(ProbeOutcome::Rejected),
            }
        }
        let credential = stored_credential(store.clone())
            .await
            .map_err(crate::pr_ops::map_sc_err)?;
        let Some((token, method)) = load_gitlab_token(&store, credential).await? else {
            return Ok(ProbeOutcome::NotConfigured);
        };
        (token, method)
    };
    match validate_pat(host, &token).await {
        Ok(user) => Ok(ProbeOutcome::Configured { user, method }),
        Err(intent_sourcecontrol::Error::Auth(_)) => {
            disconnect_gitlab_if_current(store, bound, gate, bus, host.host(), &token).await?;
            Ok(ProbeOutcome::NotConfigured)
        }
        Err(e) => Err(crate::pr_ops::map_sc_err(e)),
    }
}

enum RefreshFailure {
    /// The instance refused the refresh token (or none is stored / no client
    /// id can run the exchange): the connection cannot be renewed.
    Unrecoverable,
    /// Transport / instance failure: the stored pair may still be good.
    Transient,
}

async fn try_refresh(
    host: &GitlabHost,
    client_id: Option<&str>,
    store: FileSecretStore,
) -> std::result::Result<(), RefreshFailure> {
    let Some(client_id) = client_id else {
        tracing::warn!(
            host = host.host(),
            "gitlab token refresh needs an oauth client id"
        );
        return Err(RefreshFailure::Unrecoverable);
    };
    match refresh_access_token(host, client_id, store).await {
        Ok(()) => {
            tracing::info!(host = host.host(), "gitlab access token refreshed");
            Ok(())
        }
        Err(
            e @ (intent_sourcecontrol::Error::Auth(_) | intent_sourcecontrol::Error::Config(_)),
        ) => {
            tracing::warn!(error = %e, host = host.host(), "gitlab token refresh refused");
            Err(RefreshFailure::Unrecoverable)
        }
        Err(e) => {
            tracing::warn!(error = %e, host = host.host(), "gitlab token refresh failed");
            Err(RefreshFailure::Transient)
        }
    }
}

/// `SourceControlUser` (§5.27): `{ id: string, login, displayName?, avatarUrl? }`
/// — optional fields omitted, never null. Never carries a token.
pub(crate) fn source_control_user(
    id: &str,
    login: &str,
    display_name: Option<&str>,
    avatar_url: Option<&str>,
) -> Value {
    let mut user = json!({ "id": id, "login": login });
    if let Some(name) = display_name.filter(|n| !n.is_empty()) {
        user["displayName"] = json!(name);
    }
    if let Some(url) = avatar_url.filter(|u| !u.is_empty()) {
        user["avatarUrl"] = json!(url);
    }
    user
}

pub(crate) fn gitlab_user_to_wire(user: &GitlabUser) -> Value {
    source_control_user(
        &user.id.to_string(),
        &user.username,
        user.name.as_deref(),
        user.avatar_url.as_deref(),
    )
}

pub(crate) fn github_user_to_wire(user: &UserIdentity) -> Value {
    source_control_user(
        &user.id.map_or_else(String::new, |id| id.to_string()),
        &user.login,
        user.name.as_deref(),
        user.avatar_url.as_deref(),
    )
}

/// The `sourceControl.authStatus` result: the `github.authStatus` shape
/// (`base`, from [`github_auth_ops::auth_status_to_wire`]) plus the additive
/// `provider` / `host` / `method` / `user?` / `deviceGrantSupported` fields.
/// `user` is present iff configured; `method` is `null` when not configured.
pub(crate) fn auth_status_to_wire(
    mut base: Value,
    provider: Provider,
    host: &str,
    method: Option<&str>,
    user: Option<Value>,
    device_grant_supported: bool,
) -> Value {
    base["provider"] = json!(provider.as_wire());
    base["host"] = json!(host);
    base["method"] = method.map_or(Value::Null, |m| json!(m));
    if let Some(user) = user {
        base["user"] = user;
    }
    base["deviceGrantSupported"] = json!(device_grant_supported);
    base
}

/// The `sourceControl.connect { method: "pat" }` success payload.
pub(crate) fn pat_connect_response() -> Value {
    json!({ "ok": true, "method": "pat" })
}

impl crate::Services {
    /// Parse + resolve the `(provider, host)` of a `sourceControl.*` call
    /// against the effective `sourceControl.gitlab.*` settings.
    pub(crate) fn resolve_source_control_target(
        &self,
        provider: &str,
        host: Option<&str>,
    ) -> Result<Target> {
        let provider = Provider::parse(provider)?;
        let gitlab = self.effective_settings().source_control.gitlab;
        let env_origin = std::env::var(GITLAB_API_BASE_URI_ENV).ok();
        let api_origin = gitlab
            .api_base_url
            .as_deref()
            .map(str::trim)
            .filter(|o| !o.is_empty())
            .or(env_origin.as_deref());
        resolve_target(provider, host, &gitlab.host, api_origin)
    }

    /// Whether `host` is the bound instance (`sourceControl.gitlab.host`)
    /// **right now** — re-read from the effective settings on every call so a
    /// probe that waited on the [`GitlabCredentialGate`] sees a rebind that
    /// landed while it waited. An unparsable configured host binds nothing.
    pub(crate) fn gitlab_host_is_bound(&self, host: &GitlabHost) -> bool {
        parse_gitlab_host(&self.effective_settings().source_control.gitlab.host)
            .is_ok_and(|bound| bound.host() == host.host())
    }

    /// The identity-proof provider of a `(provider, host)` target
    /// (`sourceControl.identityProof.*`, protocol 10.8): the GitHub gist
    /// proof against the same API host the reconnect guard uses, or the
    /// GitLab snippet proof on the resolved instance.
    pub(crate) fn proof_provider(&self, target: &Target) -> ProofProvider {
        match target {
            Target::Github => ProofProvider::Github {
                api_base_url: crate::invite_ops::resolve_api_base_uri(
                    self.github_api_base_uri.as_deref(),
                ),
            },
            Target::Gitlab { host } => ProofProvider::Gitlab { host: host.clone() },
        }
    }

    /// The **stored** credential the guest half signs a proof with. Only the
    /// stored token counts for either provider — never the env / `gh`
    /// fallbacks — so the proof is always made with the account the user
    /// signed in with; for GitLab the token applies to the bound instance
    /// only, so a `host` that is not bound is `gitlab-not-connected` like an
    /// absent token. Read under the [`GitlabCredentialGate`] so a rebind or
    /// revoke in flight is not raced.
    pub(crate) async fn stored_proof_token(&self, target: &Target) -> Result<String> {
        match target {
            Target::Github => github_auth_ops::load_stored_token(&self.secrets).await,
            Target::Gitlab { host } => {
                let _gate = self.gitlab_credential_gate.lock().await;
                if !self.gitlab_host_is_bound(host) {
                    return Err(Error::IdentityProof(
                        IdentityProofErrorKind::GitlabNotConnected,
                    ));
                }
                stored_access_token(&self.gitlab_secret_store)
                    .await?
                    .ok_or(Error::IdentityProof(
                        IdentityProofErrorKind::GitlabNotConnected,
                    ))
            }
        }
    }

    /// Host half: the daemon's own GitLab credential for `host` — the
    /// stored slot, then `GITLAB_TOKEN` — when `host` is the bound instance;
    /// `None` otherwise (every credential the daemon holds belongs to the
    /// bound instance only). What `invite.prove` and the pin lookup send so
    /// an instance that restricts anonymous reads still answers the host
    /// that is connected to it. Read under the [`GitlabCredentialGate`].
    pub(crate) async fn own_gitlab_token(&self, host: &GitlabHost) -> Option<String> {
        let _gate = self.gitlab_credential_gate.lock().await;
        if !self.gitlab_host_is_bound(host) {
            return None;
        }
        match stored_access_token(&self.gitlab_secret_store).await {
            Ok(Some(token)) => Some(token),
            Ok(None) => std::env::var(GITLAB_TOKEN_ENV)
                .ok()
                .map(|t| t.trim().to_string())
                .filter(|t| !t.is_empty()),
            Err(e) => {
                tracing::debug!(error = %e, host = host.host(), "gitlab token read failed");
                None
            }
        }
    }

    /// Guest half of `sourceControl.identityProof.create` (and its
    /// `github.identityProof.create` alias): validate the proof lines,
    /// resolve the target, and publish `nonce` with the stored token.
    /// 🔒 Returns the proof id and the owner identity only, never the token.
    pub(crate) async fn identity_proof_create(
        &self,
        provider: &str,
        host: Option<&str>,
        nonce: &str,
        host_label: &str,
    ) -> Result<(ProofProvider, CreatedProof)> {
        let nonce = github_auth_ops::proof_line_param("nonce", nonce)?;
        let host_label = github_auth_ops::proof_line_param("hostLabel", host_label)?;
        let kind = Provider::parse(provider)?;
        let target = self.resolve_source_control_target(provider, host)?;
        let proof = self.proof_provider(&target);
        let token = self.stored_proof_token(&target).await?;
        let created = proof
            .create(&token, &nonce, &host_label)
            .await
            .map_err(|e| github_auth_ops::map_identity_proof_err_for(kind, e))?;
        Ok((proof, created))
    }

    /// Guest half of `sourceControl.identityProof.delete` (and its
    /// `github.identityProof.delete` alias): the id is shape-checked as
    /// `id_param` (`-32602`) before any credential is read; the engine then
    /// reads the proof back and refuses anything that is not an Intent proof.
    pub(crate) async fn identity_proof_delete(
        &self,
        provider: &str,
        host: Option<&str>,
        id_param: &str,
        proof_id: &str,
    ) -> Result<()> {
        let kind = Provider::parse(provider)?;
        let target = self.resolve_source_control_target(provider, host)?;
        let proof = self.proof_provider(&target);
        let proof_id = proof_id.trim();
        if !proof.valid_proof_id(proof_id) {
            return Err(Error::InvalidParams(match kind {
                Provider::Github => {
                    format!("{id_param} must be a non-empty alphanumeric gist id")
                }
                Provider::Gitlab => format!("{id_param} must be a numeric snippet id"),
            }));
        }
        let token = self.stored_proof_token(&target).await?;
        proof
            .delete(&token, proof_id)
            .await
            .map_err(|e| github_auth_ops::map_identity_proof_err_for(kind, e))
    }

    /// The OAuth client id the GitLab device grant uses for `host`
    /// (`sourceControl.gitlab.oauthClientId`, else the compiled gitlab.com id).
    pub(crate) fn gitlab_client_id(&self, host: &GitlabHost) -> Option<String> {
        let configured = self
            .effective_settings()
            .source_control
            .gitlab
            .oauth_client_id;
        intent_sourcecontrol::gitlab_auth::resolve_client_id(&configured, host)
    }

    /// Provenance of the GitHub credential in use: `"device"` when the
    /// device-flow marker sits next to the stored token, `"pat"` for a stored
    /// token without it, `"env"` when nothing is stored (env / `gh` fallback).
    pub(crate) async fn github_credential_method(&self) -> &'static str {
        let stored = self
            .secrets
            .load(github_auth_ops::SECRET_ACCOUNT)
            .await
            .ok()
            .flatten()
            .is_some_and(|t| !t.trim().is_empty());
        if !stored {
            return "env";
        }
        let marker = self
            .secrets
            .load(GITHUB_TOKEN_METHOD_ACCOUNT)
            .await
            .ok()
            .flatten();
        match marker.as_deref() {
            Some("device") => "device",
            _ => "pat",
        }
    }

    /// `sourceControl.connect { provider: "gitlab", method: "pat" }`: validate
    /// the token against `host`, persist it, bind the host, abort any pending
    /// device flow and emit `authorized`. A rejected token stores nothing and
    /// surfaces as `source-control-unauthorized`.
    ///
    /// Persist, slot clear, bind and event ride one hold of the
    /// [`GitlabCredentialGate`], so a device completion cannot land between
    /// them (it either finished before, and the PAT replaces its pair, or it
    /// finds its slot gone and exchanges nothing) and the event order matches
    /// the store order. The slot is cleared whatever host the flow targets:
    /// the daemon holds one credential for one bound host, so a flow started
    /// before this connection — for this instance or another — would, on
    /// completion, overwrite the PAT and re-bind; it is superseded the same
    /// way a newer device connect supersedes it.
    pub(crate) async fn gitlab_connect_pat(
        &self,
        host: GitlabHost,
        token: String,
    ) -> Result<Value> {
        match validate_pat(&host, &token).await {
            Ok(_) => {}
            Err(intent_sourcecontrol::Error::Auth(_)) => {
                return Err(Error::SourceControlUnauthorized {
                    provider: Provider::Gitlab.as_wire().to_string(),
                    host: host.host().to_string(),
                })
            }
            Err(e) => return Err(crate::pr_ops::map_sc_err(e)),
        }
        let _gate = self.gitlab_credential_gate.lock().await;
        intent_sourcecontrol::gitlab_auth::persist_gitlab_token(
            self.gitlab_secret_store.clone(),
            intent_sourcecontrol::SecretString::from(token),
        )
        .await
        .map_err(crate::pr_ops::map_sc_err)?;
        self.gitlab_auth.lock().await.flow = None;
        bind_gitlab_host(self.settings_registry.as_deref(), host.host());
        tracing::info!(host = host.host(), "gitlab personal access token connected");
        publish_auth_changed(
            self.event_bus.as_ref(),
            Provider::Gitlab,
            host.host(),
            "authorized",
        )
        .await;
        Ok(pat_connect_response())
    }

    /// `sourceControl.connect { provider: "gitlab" }` (device grant): the
    /// gitlab twin of `github.connect` — idempotent while a flow for `host`
    /// is live, replaces a terminal / other-host slot, and spawns the poll
    /// loop. A host that cannot run the grant → `device-grant-unsupported`.
    pub(crate) async fn gitlab_connect_device(&self, host: GitlabHost) -> Result<Value> {
        let unsupported = || Error::DeviceGrantUnsupported {
            provider: Provider::Gitlab.as_wire().to_string(),
            host: host.host().to_string(),
        };
        {
            let mut guard = self.gitlab_auth.lock().await;
            if let Some(f) = guard.flow.as_ref() {
                if f.host == host.host() && f.slot.is_live() {
                    return Ok(github_auth_ops::connect_response(&f.slot));
                }
            }
            guard.flow = None;
            if guard.unsupported_hosts.contains(host.host()) {
                return Err(unsupported());
            }
        }
        let Some(client_id) = self.gitlab_client_id(&host) else {
            return Err(unsupported());
        };
        let started = intent_sourcecontrol::gitlab_auth::start_device_grant_with_store(
            &host,
            &client_id,
            self.gitlab_secret_store.clone(),
        )
        .await;
        let (auth, flow) = match started {
            Ok(pair) => pair,
            Err(intent_sourcecontrol::Error::DeviceGrantUnsupported(reason)) => {
                tracing::warn!(
                    host = host.host(),
                    reason,
                    "gitlab device grant unsupported"
                );
                self.gitlab_auth
                    .lock()
                    .await
                    .unsupported_hosts
                    .insert(host.host().to_string());
                return Err(unsupported());
            }
            Err(e) => return Err(crate::pr_ops::map_sc_err(e)),
        };
        let mut guard = self.gitlab_auth.lock().await;
        if let Some(f) = guard.flow.as_ref() {
            if f.host == host.host() && f.slot.is_live() {
                return Ok(github_auth_ops::connect_response(&f.slot));
            }
        }
        let flow_id = github_auth_ops::next_flow_id();
        let deadline = Instant::now() + std::time::Duration::from_secs(auth.expires_in);
        intent_core::spawn_daemon(run_gitlab_poll_loop(
            self.gitlab_auth.clone(),
            self.event_bus.clone(),
            self.settings_registry.clone(),
            self.gitlab_credential_gate.clone(),
            flow_id,
            host.host().to_string(),
            flow,
            deadline,
        ));
        let slot = FlowSlot {
            flow_id,
            user_code: auth.user_code,
            verification_uri: auth
                .verification_uri_complete
                .unwrap_or(auth.verification_uri),
            interval: auth.interval,
            deadline,
            phase: FlowPhase::Pending,
        };
        let resp = github_auth_ops::connect_response(&slot);
        guard.flow = Some(GitlabFlowSlot {
            host: host.host().to_string(),
            slot,
        });
        tracing::info!(host = host.host(), "gitlab device grant started");
        Ok(resp)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_parses_the_two_forges_only() {
        assert_eq!(Provider::parse("github").unwrap(), Provider::Github);
        assert_eq!(Provider::parse(" gitlab ").unwrap(), Provider::Gitlab);
        for bad in ["", "GitHub", "bitbucket"] {
            assert!(
                matches!(Provider::parse(bad), Err(Error::InvalidParams(_))),
                "{bad:?}"
            );
        }
    }

    #[test]
    fn gitlab_host_must_be_bare() {
        assert_eq!(
            parse_gitlab_host("GitLab.Acme.internal:8443")
                .unwrap()
                .host(),
            "gitlab.acme.internal:8443"
        );
        for bad in [
            "",
            "https://gitlab.com",
            "gitlab.com/",
            "gitlab.com/gitlab",
            "user@gitlab.com",
            "gitlab.com?x=1",
            "git lab.com",
        ] {
            assert!(
                matches!(parse_gitlab_host(bad), Err(Error::InvalidParams(_))),
                "{bad:?}"
            );
        }
    }

    #[test]
    fn target_resolution_binds_and_overrides_the_configured_host_only() {
        assert!(matches!(
            resolve_target(Provider::Github, None, "gitlab.com", None).unwrap(),
            Target::Github
        ));
        assert!(matches!(
            resolve_target(Provider::Github, Some("github.com"), "gitlab.com", None),
            Err(Error::InvalidParams(_))
        ));
        assert!(matches!(
            resolve_target(Provider::Github, Some("  "), "gitlab.com", None).unwrap(),
            Target::Github
        ));

        let origin = Some("http://127.0.0.1:4321");
        match resolve_target(Provider::Gitlab, None, "gitlab.com", origin).unwrap() {
            Target::Gitlab { host } => {
                assert_eq!(host.host(), "gitlab.com");
                assert_eq!(host.base_url(), "http://127.0.0.1:4321");
            }
            Target::Github => panic!("expected a gitlab target"),
        }
        match resolve_target(
            Provider::Gitlab,
            Some("gitlab.acme.internal"),
            "gitlab.com",
            origin,
        )
        .unwrap()
        {
            Target::Gitlab { host } => {
                // Another instance is not the bound one: no origin override.
                assert_eq!(host.base_url(), "https://gitlab.acme.internal");
            }
            Target::Github => panic!("expected a gitlab target"),
        }
        assert!(matches!(
            resolve_target(
                Provider::Gitlab,
                None,
                "gitlab.com",
                Some("http://evil.example")
            ),
            Err(Error::Internal(_))
        ));
        assert!(matches!(
            resolve_target(Provider::Gitlab, Some("https://x"), "gitlab.com", None),
            Err(Error::InvalidParams(_))
        ));
    }

    #[test]
    fn source_control_user_omits_absent_optionals() {
        let user = gitlab_user_to_wire(&GitlabUser {
            id: 42,
            username: "octocat".into(),
            name: None,
            avatar_url: Some(String::new()),
        });
        assert_eq!(user, json!({ "id": "42", "login": "octocat" }));
        let user = github_user_to_wire(&UserIdentity {
            login: "octocat".into(),
            id: Some(583_231),
            name: Some("The Octocat".into()),
            avatar_url: Some("https://avatars.example/u/1".into()),
            html_url: Some("https://github.com/octocat".into()),
        });
        assert_eq!(
            user,
            json!({
                "id": "583231", "login": "octocat", "displayName": "The Octocat",
                "avatarUrl": "https://avatars.example/u/1"
            })
        );
        assert!(user.get("htmlUrl").is_none());
    }

    #[test]
    fn auth_status_additive_fields_extend_the_legacy_shape_only() {
        let base = github_auth_ops::auth_status_to_wire(true, None);
        let mut full = auth_status_to_wire(
            base.clone(),
            Provider::Gitlab,
            "gitlab.com",
            Some("device"),
            Some(json!({ "id": "1", "login": "u" })),
            true,
        );
        assert_eq!(full["provider"], "gitlab");
        assert_eq!(full["host"], "gitlab.com");
        assert_eq!(full["method"], "device");
        assert_eq!(full["user"]["login"], "u");
        assert_eq!(full["deviceGrantSupported"], true);
        let obj = full.as_object_mut().unwrap();
        for key in ["provider", "host", "method", "user", "deviceGrantSupported"] {
            obj.remove(key);
        }
        assert_eq!(
            full, base,
            "the legacy github.authStatus shape is untouched"
        );

        let unconfigured =
            auth_status_to_wire(base, Provider::Github, GITHUB_HOST, None, None, true);
        assert_eq!(unconfigured["method"], Value::Null);
        assert!(unconfigured.get("user").is_none());
    }

    #[test]
    fn auth_changed_event_is_global_and_carries_only_the_transition() {
        let ev = auth_changed_event(Provider::Gitlab, "gitlab.com", "authorized");
        assert_eq!(ev.event_type, "sourceControl:auth-changed");
        assert!(ev.workspace_id.as_str().is_empty());
        assert_eq!(
            ev.data,
            json!({ "provider": "gitlab", "host": "gitlab.com", "status": "authorized" })
        );
    }
}
