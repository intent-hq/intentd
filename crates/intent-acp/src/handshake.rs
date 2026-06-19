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

use agent_client_protocol::schema::{
    AuthMethodId, AuthenticateRequest, ClientCapabilities, FileSystemCapabilities, Implementation,
    InitializeRequest, InitializeResponse, ProtocolVersion, SetSessionModeRequest,
};
use intent_providers::{auth_error_message, is_provider_authentication_error, ProviderConfig};

use crate::error::{AcpError, AcpResult};
use crate::transport::Connection;

/// Client name advertised to agents in `clientInfo`.
const CLIENT_NAME: &str = "Intent";
/// Auth method id used when no interactive auth is required (parity: TS sends
/// `methodId: "none"`).
const NO_AUTH_METHOD_ID: &str = "none";

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
pub async fn handshake(conn: &Connection, provider: &ProviderConfig) -> AcpResult<HandshakeResult> {
    let initialize = initialize(conn).await?;
    let authenticated = authenticate(conn, provider).await?;
    Ok(HandshakeResult {
        initialize,
        authenticated,
    })
}

/// Send `initialize`, advertising client capabilities and client info (§6.4.1).
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
    let result = conn.request("initialize", params).await?;
    serde_json::from_value(result)
        .map_err(|e| AcpError::Protocol(format!("invalid initialize response: {e}")))
}

/// Conditionally send `authenticate` (§6.4.2).
///
/// Returns `Ok(false)` when the provider does not implement authentication.
/// On an auth failure, returns [`AcpError::Auth`] carrying the provider login
/// hint; other errors are propagated unchanged.
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
pub async fn set_session_mode(conn: &Connection, session_id: &str, mode_id: &str) -> AcpResult<()> {
    let request = SetSessionModeRequest::new(session_id.to_string(), mode_id.to_string());
    let params = serde_json::to_value(&request)?;
    conn.request("session/set_mode", params).await?;
    Ok(())
}
