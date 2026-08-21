//! State + pure DTO glue for the `github.connect` / `github.cancelAuth` /
//! `github.revoke` device-flow wire surface (PROTOCOL §5.27). The daemon owns
//! the flow: `github.connect` starts it and spawns a background poll task;
//! terminal transitions land here as a [`FlowPhase`] and are broadcast as
//! `github:auth-changed`. The engine (`intent_sourcecontrol::device_flow`)
//! keeps the `device_code` / access token private — this module only ever
//! sees the user-facing codes, so nothing sensitive can cross the wire.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use intent_core::events::GITHUB_AUTH_CHANGED;
use intent_core::{now_iso, Result, WorkspaceId};
use intent_sourcecontrol::{DeviceFlow, PollStatus};
use intent_store::NewEvent;
use serde_json::{json, Value};
use tokio::time::Instant;

use crate::events::EventBus;
use crate::{publish_event, system_actor};

/// Secret-store account the device flow persists the token under — the first
/// slot of the existing resolution chain (`intent_sourcecontrol::token`).
pub(crate) const SECRET_ACCOUNT: &str = "sourceControl.github.token";

/// Env override for the GitHub login host the device flow talks to — the
/// spawned-daemon test seam (e2e points it at a local mock).
pub(crate) const LOGIN_BASE_URI_ENV: &str = "INTENTD_GITHUB_LOGIN_BASE_URI";

/// Consecutive poll errors tolerated before the flow is marked [`FlowPhase::Error`]
/// (transient network blips must not kill a 15-minute flow).
pub(crate) const MAX_CONSECUTIVE_POLL_ERRORS: u32 = 3;

/// Where the in-flight (or most recently finished) device flow stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FlowPhase {
    /// Waiting for the user to enter the code; the poll task is live.
    Pending,
    /// The codes expired before the user authorized; restart to retry.
    Expired,
    /// The user denied the authorization request.
    Denied,
    /// Polling failed repeatedly (network / non-retryable API error).
    Error,
}

impl FlowPhase {
    /// Wire string for `deviceFlow.status` and `github:auth-changed`.
    pub(crate) fn as_wire(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Expired => "expired",
            Self::Denied => "denied",
            Self::Error => "error",
        }
    }
}

/// The single in-flight / last-terminal device-flow slot. Holds only the
/// user-facing codes (never `device_code` or the token).
///
/// Cancellation is **cooperative**: cancel/revoke/connect-replace remove or
/// replace the slot, and the background poll task exits at its next tick when
/// its generation is no longer resident. No `abort()` — a hard abort could
/// land while the engine's non-cancellable `spawn_blocking` token write is in
/// flight, leaving a token on disk that a revoke meant to prevent; the
/// cooperative task instead reconciles (deletes) such a write itself.
pub(crate) struct FlowSlot {
    /// Generation guard: a poll task only polls/mutates while its own flow
    /// is still the resident one (cancel/revoke/a newer `connect` orphan it).
    pub(crate) flow_id: u64,
    pub(crate) user_code: String,
    pub(crate) verification_uri: String,
    /// FE polling hint, captured at start. A `slow_down` grows the engine's
    /// live cadence but not this snapshot, so the hint can understate the
    /// real interval — harmless, the poll loop reads the engine each tick.
    pub(crate) interval: u64,
    /// When the codes expire (`start` + `expires_in`).
    pub(crate) deadline: Instant,
    pub(crate) phase: FlowPhase,
}

impl FlowSlot {
    /// Seconds until the codes expire (0 when already past the deadline).
    pub(crate) fn remaining_secs(&self) -> u64 {
        self.deadline
            .saturating_duration_since(Instant::now())
            .as_secs()
    }

    /// True iff the flow is pending *and* its codes have not expired.
    pub(crate) fn is_live(&self) -> bool {
        self.phase == FlowPhase::Pending && self.remaining_secs() > 0
    }
}

/// Shared single-flow state: at most one device flow exists at a time.
pub(crate) type FlowState = Arc<tokio::sync::Mutex<Option<FlowSlot>>>;

/// Mint a process-unique flow id (generation guard for [`FlowSlot`]).
pub(crate) fn next_flow_id() -> u64 {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

/// Build a `github:auth-changed { status }` event. Global like
/// `settings:changed` (empty workspace id) so subscribers that omit a
/// `workspaceId` filter still receive it. Carries only the transition —
/// never a token or code.
pub(crate) fn auth_changed_event(status: &str) -> NewEvent {
    NewEvent {
        workspace_id: WorkspaceId::from_string(String::new()),
        timestamp: now_iso(),
        event_type: GITHUB_AUTH_CHANGED.to_string(),
        actor: system_actor(),
        session_id: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data: json!({ "status": status }),
    }
}

/// True iff this task's flow is still the resident slot.
async fn is_resident(state: &FlowState, flow_id: u64) -> bool {
    let slot = state.lock().await;
    matches!(slot.as_ref(), Some(s) if s.flow_id == flow_id)
}

/// The daemon-owned poll loop `github.connect` spawns: polls GitHub at the
/// engine's cadence until a terminal transition, then updates the slot (iff
/// its generation still matches — cancel/revoke/a newer `connect` orphan this
/// task) and emits `github:auth-changed`. On `Authorized` the engine has
/// already persisted the token, so the slot is cleared and
/// `github.authStatus` reflects the configured token from then on. If the
/// flow was orphaned while the authorizing poll was in flight, the persisted
/// token is deleted again (see [`FlowSlot`] on cooperative cancellation).
///
/// `sync_gh` opts the authorized transition into the best-effort gh CLI token
/// sync ([`intent_sourcecontrol::gh_sync`]); `github.connect` sets it only
/// when the flow targets the production login host, so mock-host tests never
/// spawn a real `gh` (or feed it a mock token). The sync runs as a detached
/// task after the slot update + event emit — fail-soft, it can never fail or
/// delay the device flow.
pub(crate) async fn run_poll_loop(
    state: FlowState,
    bus: Option<EventBus>,
    secrets: Arc<crate::settings::AsyncSecretStore>,
    flow_id: u64,
    mut flow: DeviceFlow,
    deadline: Instant,
    sync_gh: bool,
) {
    let mut consecutive_errors: u32 = 0;
    // `None` = authorized (slot cleared); `Some(phase)` = terminal failure.
    let outcome: Option<FlowPhase> = loop {
        // Sleep the engine cadence, capped at the deadline so a grown
        // (`slow_down`) interval can never delay the expiry transition.
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            // GitHub kept answering pending past `expires_in`; expire locally
            // so the loop cannot poll forever.
            break Some(FlowPhase::Expired);
        }
        tokio::time::sleep(poll_sleep(flow.interval_secs()).min(remaining)).await;
        // Cooperative cancellation: stop before touching the network once
        // cancel/revoke/a newer connect removed or replaced the slot.
        if !is_resident(&state, flow_id).await {
            return;
        }
        if Instant::now() >= deadline {
            break Some(FlowPhase::Expired);
        }
        match flow.poll_once().await {
            Ok(PollStatus::Pending) => consecutive_errors = 0,
            Ok(PollStatus::Authorized) => break None,
            Ok(PollStatus::Expired) => break Some(FlowPhase::Expired),
            Ok(PollStatus::Denied) => break Some(FlowPhase::Denied),
            Err(e) => {
                consecutive_errors += 1;
                tracing::warn!(
                    error = %e,
                    consecutive_errors,
                    "github device-flow poll failed"
                );
                if consecutive_errors >= MAX_CONSECUTIVE_POLL_ERRORS {
                    break Some(FlowPhase::Error);
                }
            }
        }
    };
    {
        let mut slot = state.lock().await;
        match slot.as_mut() {
            // Only touch the slot while this task's flow is still resident.
            Some(s) if s.flow_id == flow_id => match outcome {
                None => *slot = None,
                Some(phase) => s.phase = phase,
            },
            _ => {
                // Orphaned while the last poll was in flight. If that poll
                // authorized, the engine persisted a token a concurrent
                // cancel/revoke meant to prevent — reconcile by deleting it.
                if outcome.is_none() {
                    if let Err(e) = secrets.delete(SECRET_ACCOUNT).await {
                        tracing::warn!(
                            error = %e,
                            "could not delete github token after orphaned authorize"
                        );
                    }
                }
                return;
            }
        }
    }
    let status = outcome.map_or("authorized", FlowPhase::as_wire);
    tracing::info!(status, "github device flow finished");
    publish_event(&bus, auth_changed_event(status)).await;
    if outcome.is_none() && sync_gh {
        // Best-effort gh CLI sync: loads the token back from the secret store
        // (it never leaves the engine) and pipes it to `gh` via stdin only.
        tokio::spawn(intent_sourcecontrol::gh_sync::sync_token_to_gh(
            intent_core::FileSecretStore::new(),
        ));
    }
}

/// Delete the stored `sourceControl.github.token` through the services
/// secret-store seam (cache-coherent with `settings.*`, test-injectable).
pub(crate) async fn delete_stored_token(secrets: &crate::settings::AsyncSecretStore) -> Result<()> {
    secrets.delete(SECRET_ACCOUNT).await
}

/// Resolve the login host. The builder override wins over the env override
/// (the env is only consulted when no builder override is set); the winning
/// candidate is then safety-checked — an unsafe value falls back directly to
/// the `github.com` default, it does NOT fall through to the other override.
/// 🔒 Cleartext `http://` overrides are only honored for loopback hosts (the
/// hermetic-test seam); anything else would hand the access token to an
/// unencrypted non-local endpoint.
pub(crate) fn resolve_login_base_uri(override_uri: Option<&str>) -> String {
    override_uri
        .map(str::to_string)
        .or_else(|| {
            std::env::var(LOGIN_BASE_URI_ENV)
                .ok()
                .filter(|s| !s.trim().is_empty())
        })
        .filter(|uri| {
            if is_safe_login_base_uri(uri) {
                true
            } else {
                tracing::warn!(
                    uri,
                    "ignoring github login base-uri override: must be https:// \
                     or cleartext http:// on a loopback host"
                );
                false
            }
        })
        .unwrap_or_else(|| intent_sourcecontrol::device_flow::DEFAULT_LOGIN_BASE_URI.to_string())
}

/// True iff `base_uri` is the production github.com login host — the shared
/// gate for gh CLI side effects (login sync on authorize, logout on revoke):
/// a mock-host flow (test seam) stores a token gh cannot use, and touching
/// `gh` from it would reach the host's real login state from tests. Trailing
/// slashes are insignificant ("<https://github.com>/" is the same host), so
/// normalize before comparing.
pub(crate) fn is_production_login_host(base_uri: &str) -> bool {
    base_uri.trim_end_matches('/')
        == intent_sourcecontrol::device_flow::DEFAULT_LOGIN_BASE_URI.trim_end_matches('/')
}

/// True iff `uri` is `https://…` or a cleartext `http://` pointing at a
/// loopback host (`127.0.0.1`, `localhost`, `[::1]`).
fn is_safe_login_base_uri(uri: &str) -> bool {
    if uri.starts_with("https://") {
        return true;
    }
    let Some(rest) = uri.strip_prefix("http://") else {
        return false;
    };
    let authority = rest.split('/').next().unwrap_or_default();
    let host = authority.strip_prefix("[::1]").map_or_else(
        || authority.split(':').next().unwrap_or_default(),
        |_| "::1",
    );
    matches!(host, "127.0.0.1" | "localhost" | "::1")
}

/// Poll cadence floor: the engine's interval comes from the server and a
/// mock returning `0` must not turn the poll loop into a busy spin.
pub(crate) fn poll_sleep(interval_secs: u64) -> Duration {
    Duration::from_secs(interval_secs.max(1))
}

/// The `github.connect` success payload (§5.27): the user-facing codes plus
/// the remaining validity window. Identical shape whether the flow was just
/// started or an existing pending flow was returned.
pub(crate) fn connect_response(slot: &FlowSlot) -> Value {
    json!({
        "ok": true,
        "userCode": slot.user_code,
        "verificationUri": slot.verification_uri,
        "expiresIn": slot.remaining_secs(),
        "interval": slot.interval,
    })
}

/// The `deviceFlow` object embedded in `github.authStatus` (§5.27).
pub(crate) fn flow_to_wire(slot: &FlowSlot) -> Value {
    json!({
        "status": slot.phase.as_wire(),
        "userCode": slot.user_code,
        "verificationUri": slot.verification_uri,
        "expiresIn": slot.remaining_secs(),
        "interval": slot.interval,
    })
}

/// Build the `github.authStatus` result (§5.27). `deviceFlow` is `null` when
/// no flow is in flight; `oauthUrl` carries the verification URI while a flow
/// is live so existing FE consumers can link to it. Never carries a token.
pub(crate) fn auth_status_to_wire(is_configured: bool, slot: Option<&FlowSlot>) -> Value {
    let oauth_url = slot
        .filter(|s| s.is_live())
        .map(|s| s.verification_uri.clone())
        .unwrap_or_default();
    json!({
        "isConfigured": is_configured,
        "oauthUrl": oauth_url,
        "configuredButNeedsUpdate": false,
        "updatedScopes": "",
        "deviceFlow": slot.map_or(Value::Null, flow_to_wire),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn slot(phase: FlowPhase, remaining: Duration) -> FlowSlot {
        FlowSlot {
            flow_id: 1,
            user_code: "ABCD-1234".into(),
            verification_uri: "https://github.com/login/device".into(),
            interval: 5,
            deadline: Instant::now() + remaining,
            phase,
        }
    }

    #[test]
    fn pending_slot_within_deadline_is_live() {
        assert!(slot(FlowPhase::Pending, Duration::from_secs(60)).is_live());
        assert!(!slot(FlowPhase::Pending, Duration::ZERO).is_live());
        assert!(!slot(FlowPhase::Expired, Duration::from_secs(60)).is_live());
        assert!(!slot(FlowPhase::Denied, Duration::from_secs(60)).is_live());
        assert!(!slot(FlowPhase::Error, Duration::from_secs(60)).is_live());
    }

    #[test]
    fn connect_response_carries_codes_and_remaining_window() {
        let v = connect_response(&slot(FlowPhase::Pending, Duration::from_secs(120)));
        assert_eq!(v["ok"], true);
        assert_eq!(v["userCode"], "ABCD-1234");
        assert_eq!(v["verificationUri"], "https://github.com/login/device");
        assert_eq!(v["interval"], 5);
        let remaining = v["expiresIn"].as_u64().expect("expiresIn");
        assert!(remaining > 0 && remaining <= 120);
        // Nothing sensitive on the wire.
        assert!(v.get("deviceCode").is_none());
        assert!(v.get("accessToken").is_none());
    }

    #[test]
    fn auth_status_without_flow_matches_the_legacy_shape() {
        let v = auth_status_to_wire(true, None);
        assert_eq!(v["isConfigured"], true);
        assert_eq!(v["oauthUrl"], "");
        assert_eq!(v["configuredButNeedsUpdate"], false);
        assert_eq!(v["updatedScopes"], "");
        assert_eq!(v["deviceFlow"], Value::Null);
    }

    #[test]
    fn auth_status_with_pending_flow_carries_device_flow_and_oauth_url() {
        let s = slot(FlowPhase::Pending, Duration::from_secs(60));
        let v = auth_status_to_wire(false, Some(&s));
        assert_eq!(v["oauthUrl"], "https://github.com/login/device");
        assert_eq!(v["deviceFlow"]["status"], "pending");
        assert_eq!(v["deviceFlow"]["userCode"], "ABCD-1234");
        assert!(v["deviceFlow"]["expiresIn"].as_u64().unwrap() <= 60);
    }

    #[test]
    fn auth_status_with_terminal_flow_keeps_oauth_url_empty() {
        let s = slot(FlowPhase::Denied, Duration::from_secs(60));
        let v = auth_status_to_wire(false, Some(&s));
        assert_eq!(v["oauthUrl"], "");
        assert_eq!(v["deviceFlow"]["status"], "denied");
    }

    #[test]
    fn phase_wire_strings() {
        assert_eq!(FlowPhase::Pending.as_wire(), "pending");
        assert_eq!(FlowPhase::Expired.as_wire(), "expired");
        assert_eq!(FlowPhase::Denied.as_wire(), "denied");
        assert_eq!(FlowPhase::Error.as_wire(), "error");
    }

    #[test]
    fn login_base_uri_resolution_prefers_override() {
        assert_eq!(
            resolve_login_base_uri(Some("http://127.0.0.1:9")),
            "http://127.0.0.1:9"
        );
        // Without an override, resolution falls through to env/default — the
        // env branch is exercised by the spawned-daemon e2e (process-global
        // env vars are racy inside a multi-threaded test binary).
    }

    #[test]
    fn cleartext_override_is_only_honored_for_loopback_hosts() {
        // 🔒 A non-loopback http:// override would hand the token to an
        // unencrypted endpoint — it falls back to the production default.
        assert_eq!(
            resolve_login_base_uri(Some("http://evil.example.com")),
            intent_sourcecontrol::device_flow::DEFAULT_LOGIN_BASE_URI
        );
        assert!(is_safe_login_base_uri("https://github.example.com"));
        assert!(is_safe_login_base_uri("http://127.0.0.1:8080"));
        assert!(is_safe_login_base_uri("http://localhost:8080/path"));
        assert!(is_safe_login_base_uri("http://[::1]:8080"));
        assert!(!is_safe_login_base_uri("http://10.0.0.5:8080"));
        assert!(!is_safe_login_base_uri("http://localhost.evil.com"));
        assert!(!is_safe_login_base_uri("ftp://127.0.0.1"));
    }

    #[test]
    fn poll_sleep_floors_at_one_second() {
        assert_eq!(poll_sleep(0), Duration::from_secs(1));
        assert_eq!(poll_sleep(5), Duration::from_secs(5));
    }
}
