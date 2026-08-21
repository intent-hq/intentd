//! ACP handshake: `initialize` → conditional `authenticate` → `session/set_mode`
//! (§6.4).
//!
//! `initialize` negotiates protocol version 1 and advertises the client
//! capabilities `{ fs: { readTextFile, writeTextFile }, terminal: true }`.
//! `authenticate` is sent only when the provider implements it; on failure the
//! provider's auth-error patterns are matched against the error text and
//! captured stderr, surfacing a provider-specific login hint. `set_session_mode`
//! is session-scoped and exposed here for M3.4 (it is not part of the initial
//! connection handshake, which runs before any session exists).

use std::time::Duration;

use agent_client_protocol::schema::v1::{
    AuthMethodId, AuthenticateRequest, ClientCapabilities, FileSystemCapabilities, Implementation,
    InitializeRequest, InitializeResponse, SessionModeState, SetSessionModeRequest,
};
use agent_client_protocol::schema::ProtocolVersion;
use intent_providers::{auth_error_message, is_provider_authentication_error, ProviderConfig};

use crate::error::{AcpError, AcpResult};
use crate::transport::Connection;

/// Client name advertised to agents in `clientInfo`.
const CLIENT_NAME: &str = "Intent";
/// Default timeout for the `initialize` request. Deliberately much more
/// generous than the 5s [`DEFAULT_REQUEST_TIMEOUT`](crate::transport::DEFAULT_REQUEST_TIMEOUT):
/// `initialize` is the first reply from a freshly spawned agent process, so it
/// absorbs node cold-start and provider startup work, which under host load
/// spikes can far exceed 5s (monorepo#616). Compare: the npx provider probe
/// allows 45s and the daemon's own MCP handshake 10s.
const DEFAULT_INITIALIZE_TIMEOUT: Duration = Duration::from_secs(30);

/// Timeout for the `initialize` handshake request. Overridable via
/// `INTENTD_ACP_INITIALIZE_TIMEOUT_MS` (positive integer milliseconds;
/// unset/invalid → default), primarily for tests/CI.
fn initialize_timeout() -> Duration {
    initialize_timeout_from(std::env::var("INTENTD_ACP_INITIALIZE_TIMEOUT_MS").ok())
}

/// Parse the timeout override, falling back to [`DEFAULT_INITIALIZE_TIMEOUT`]
/// when absent, non-numeric, or zero.
fn initialize_timeout_from(val: Option<String>) -> Duration {
    if let Some(val) = val {
        if let Ok(ms) = val.trim().parse::<u64>() {
            if ms > 0 {
                return Duration::from_millis(ms);
            }
        }
    }
    DEFAULT_INITIALIZE_TIMEOUT
}
/// Auth method id used when no interactive auth is required (parity: TS sends
/// `methodId: "none"`).
const NO_AUTH_METHOD_ID: &str = "none";
/// Session mode id that asks the provider to skip its own permission prompts
/// (parity: TS acp-provider sends `session/set_mode { modeId: "bypassPermissions" }`
/// when the provider actually advertises that mode; the backend then locally
/// auto-approves anything the provider still surfaces). Only requested when it
/// appears in the session's `availableModes` — see [`select_preferred_mode`].
pub(crate) const BYPASS_PERMISSIONS_MODE: &str = "bypassPermissions";
/// Logical key looked up in [`ProviderConfig::mode_map`] to obtain a
/// provider-specific override for the bypass-permissions preference (used by
/// agents that name their permissive mode something other than
/// `bypassPermissions`).
pub(crate) const BYPASS_LOGICAL_KEY: &str = "bypass";

/// Outcome of a completed handshake.
#[derive(Debug)]
pub struct HandshakeResult {
    /// The agent's `initialize` response (capabilities, auth methods, info).
    pub initialize: InitializeResponse,
    /// Whether an `authenticate` call was made and succeeded.
    pub authenticated: bool,
}

/// Run the full connection handshake: `initialize` then conditional
/// `authenticate`.
///
/// # Errors
///
/// Propagates errors from [`initialize`] and [`authenticate`].
pub async fn handshake(conn: &Connection, provider: &ProviderConfig) -> AcpResult<HandshakeResult> {
    let initialize = initialize(conn).await?;
    let authenticated = authenticate(conn, provider).await?;
    Ok(HandshakeResult {
        initialize,
        authenticated,
    })
}

/// Send `initialize`, advertising client capabilities and client info (§6.4.1).
///
/// # Errors
///
/// Returns [`AcpError::Protocol`] if the response does not deserialize; otherwise propagates the transport/RPC error from the request.
pub async fn initialize(conn: &Connection) -> AcpResult<InitializeResponse> {
    let request = InitializeRequest::new(ProtocolVersion::V1)
        .client_capabilities(
            ClientCapabilities::new()
                .fs(FileSystemCapabilities::new()
                    .read_text_file(true)
                    .write_text_file(true))
                .terminal(true),
        )
        .client_info(Implementation::new(CLIENT_NAME, env!("CARGO_PKG_VERSION")));

    let params = serde_json::to_value(&request)?;
    let result = conn
        .request_timeout("initialize", params, initialize_timeout())
        .await?;
    serde_json::from_value(result)
        .map_err(|e| AcpError::Protocol(format!("invalid initialize response: {e}")))
}

/// Conditionally send `authenticate` (§6.4.2).
///
/// Returns `Ok(false)` when the provider does not implement authentication.
/// On an auth failure, returns [`AcpError::Auth`] carrying the provider login
/// hint; other errors are propagated unchanged.
///
/// # Errors
///
/// Returns [`AcpError::Auth`] (with the provider login hint) on an authentication failure; other request errors are propagated unchanged.
pub async fn authenticate(conn: &Connection, provider: &ProviderConfig) -> AcpResult<bool> {
    if !provider.supports_authenticate {
        return Ok(false);
    }

    let request = AuthenticateRequest::new(AuthMethodId::new(NO_AUTH_METHOD_ID));
    let params = serde_json::to_value(&request)?;
    match conn.request("authenticate", params).await {
        Ok(_) => Ok(true),
        Err(err) => {
            let mut haystack = err.to_string();
            for line in conn.recent_stderr() {
                haystack.push(' ');
                haystack.push_str(&line);
            }
            if conn.auth_error_detected()
                || is_provider_authentication_error(provider.id, &haystack)
            {
                Err(AcpError::Auth(auth_error_message(provider.id, false)))
            } else {
                Err(err)
            }
        }
    }
}

/// Set the agent's mode for a session (§6.4.3). Session-scoped; call after a
/// session exists (M3.4).
pub(crate) async fn set_session_mode(
    conn: &Connection,
    session_id: &str,
    mode_id: &str,
) -> AcpResult<()> {
    let request = SetSessionModeRequest::new(session_id.to_string(), mode_id.to_string());
    let params = serde_json::to_value(&request)?;
    conn.request("session/set_mode", params).await?;
    Ok(())
}

/// Pick the preferred permissive mode id to request via `session/set_mode` from
/// the modes the provider actually advertised in `session/new` / `session/load`.
///
/// Selection order:
/// 1. Provider [`mode_map`](ProviderConfig::mode_map) override under
///    [`BYPASS_LOGICAL_KEY`] — used only if the mapped id appears in
///    `available_modes` (a config typo shouldn't force a `-32602`).
/// 2. Otherwise [`BYPASS_PERMISSIONS_MODE`] if present in `available_modes`.
/// 3. Otherwise `None` — the caller skips the call so we never ask a provider
///    for a mode it never offered.
pub fn select_preferred_mode<'a>(
    mode_map: Option<&'a [(&'a str, &'a str)]>,
    available_modes: &'a [agent_client_protocol::schema::v1::SessionMode],
) -> Option<&'a str> {
    if let Some(map) = mode_map {
        if let Some((_, mapped)) = map
            .iter()
            .find(|(logical, _)| *logical == BYPASS_LOGICAL_KEY)
        {
            if available_modes.iter().any(|m| m.id.0.as_ref() == *mapped) {
                return Some(mapped);
            }
        }
    }
    if available_modes
        .iter()
        .any(|m| m.id.0.as_ref() == BYPASS_PERMISSIONS_MODE)
    {
        return Some(BYPASS_PERMISSIONS_MODE);
    }
    None
}

/// Best-effort `session/set_mode` to run the provider in a permissive mode
/// (parity with the reference acp-provider). Consults [`select_preferred_mode`]
/// against the modes the provider actually advertised in the session response,
/// so an agent that doesn't offer `bypassPermissions` (or a `mode_map`-mapped
/// equivalent) is left alone rather than triggering a JSON-RPC `-32602`
/// invalid-params error. When a call is attempted and the provider still fails
/// on an ADVERTISED mode, the failure is logged at WARN so it stays visible.
/// Returns `true` when the provider accepted the mode change.
pub async fn try_bypass_permissions_mode(
    conn: &Connection,
    provider: &ProviderConfig,
    session_id: &str,
    modes: Option<&SessionModeState>,
) -> bool {
    let Some(state) = modes else {
        tracing::debug!(
            provider = provider.id,
            session_id,
            "session response advertised no modes; local AllowAll auto-approves prompts"
        );
        return false;
    };
    let Some(mode_id) = select_preferred_mode(provider.mode_map, &state.available_modes) else {
        tracing::debug!(
            provider = provider.id,
            session_id,
            current_mode = %state.current_mode_id,
            "no advertised bypass-equivalent mode; local AllowAll auto-approves prompts"
        );
        return false;
    };
    match set_session_mode(conn, session_id, mode_id).await {
        Ok(()) => {
            tracing::debug!(
                provider = provider.id,
                session_id,
                mode = mode_id,
                "session/set_mode accepted; provider running in bypass mode"
            );
            true
        }
        Err(e) => {
            tracing::warn!(
                provider = provider.id,
                session_id,
                mode = mode_id,
                error = %e,
                "session/set_mode failed on advertised mode; falling back to local AllowAll auto-approve"
            );
            false
        }
    }
}

#[cfg(test)]
mod mode_select_tests {
    use super::*;
    use agent_client_protocol::schema::v1::SessionMode;

    fn mode(id: &str) -> SessionMode {
        SessionMode::new(id.to_string(), id.to_string())
    }

    #[test]
    fn prefers_bypass_permissions_when_advertised() {
        let modes = vec![mode("default"), mode(BYPASS_PERMISSIONS_MODE)];
        assert_eq!(
            select_preferred_mode(None, &modes),
            Some(BYPASS_PERMISSIONS_MODE)
        );
    }

    #[test]
    fn skips_when_bypass_not_advertised_and_no_mode_map() {
        // Auggie's real advertised modes today: `default` + `ask`, neither
        // permissive-labelled — we must skip rather than trigger `-32602`.
        let modes = vec![mode("default"), mode("ask")];
        assert_eq!(select_preferred_mode(None, &modes), None);
    }

    #[test]
    fn mode_map_override_wins_when_mapped_id_is_advertised() {
        let map: &[(&str, &str)] = &[("bypass", "yolo")];
        let modes = vec![mode("default"), mode("yolo"), mode(BYPASS_PERMISSIONS_MODE)];
        // The override takes precedence over the default `bypassPermissions`
        // fallback so per-provider quirks stay data.
        assert_eq!(select_preferred_mode(Some(map), &modes), Some("yolo"));
    }

    #[test]
    fn mode_map_override_is_ignored_when_mapped_id_is_not_advertised() {
        // A stale `mode_map` entry must not force a `-32602`: fall through to
        // the `bypassPermissions` fallback if that one is actually offered.
        let map: &[(&str, &str)] = &[("bypass", "ghost")];
        let modes = vec![mode("default"), mode(BYPASS_PERMISSIONS_MODE)];
        assert_eq!(
            select_preferred_mode(Some(map), &modes),
            Some(BYPASS_PERMISSIONS_MODE)
        );
    }

    #[test]
    fn empty_available_modes_returns_none() {
        assert_eq!(select_preferred_mode(None, &[]), None);
    }
}

#[cfg(test)]
mod initialize_timeout_tests {
    use super::*;

    #[test]
    fn defaults_to_30s_when_unset() {
        assert_eq!(initialize_timeout_from(None), DEFAULT_INITIALIZE_TIMEOUT);
        assert_eq!(DEFAULT_INITIALIZE_TIMEOUT, Duration::from_secs(30));
    }

    #[test]
    fn valid_override_is_applied() {
        assert_eq!(
            initialize_timeout_from(Some("1500".to_string())),
            Duration::from_millis(1500)
        );
    }

    #[test]
    fn invalid_override_falls_back_to_default() {
        for bad in ["", "abc", "-5", "1.5", "0"] {
            assert_eq!(
                initialize_timeout_from(Some(bad.to_string())),
                DEFAULT_INITIALIZE_TIMEOUT,
                "override {bad:?} must fall back to the default"
            );
        }
    }
}
