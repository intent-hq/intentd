//! Error type for the ACP client transport and handshake (§6.2–§6.4).

use std::fmt;
use std::time::Duration;

/// Stable Display prefix of [`AcpError::PromptIdleTimeout`]. The service layer
/// flattens prompt errors to strings at its wrap boundary
/// (`session/prompt failed: …`), so downstream classification is
/// prefix-anchored string matching — this const is the contract that survives
/// the flatten (see `acp_error_prompt_idle_timeout_display_is_prefix_anchored`
/// in `tests.rs`, which pins the Display rendering to it).
pub const PROMPT_IDLE_TIMEOUT_PREFIX: &str = "session/prompt idle timeout";

/// A JSON-RPC 2.0 error object returned by the agent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JsonRpcError {
    /// Numeric error code.
    pub code: i64,
    /// Human-readable error message.
    pub message: String,
    /// Optional structured error data.
    pub data: Option<serde_json::Value>,
}

/// Cap on the rendered `data` payload appended by [`JsonRpcError`]'s Display
/// (monorepo#519): `data` is provider-controlled and unbounded, and the
/// rendered string flows into `stop_reason` persistence, `agent:failed`
/// events, and logs. Sized so real actionable details (e.g. the `ChatGPT`
/// backend 400 nested by codex-acp, ~300 bytes — monorepo#479) render in
/// full while pathological payloads stay bounded.
pub(crate) const MAX_RENDERED_DATA_BYTES: usize = 1024;

impl fmt::Display for JsonRpcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "JSON-RPC error {}: {}", self.code, self.message)?;
        // Providers put the real failure detail in `data` (e.g. codex-acp's
        // -32603 "Internal error" carries the actual cause there), so append
        // it when present. Strings render raw (no JSON quoting noise); other
        // values render as compact JSON; null/absent/empty add nothing. The
        // appended portion is capped at [`MAX_RENDERED_DATA_BYTES`] — the
        // leading detail is where the actionable message lives.
        match &self.data {
            None | Some(serde_json::Value::Null) => Ok(()),
            Some(serde_json::Value::String(s)) if s.is_empty() => Ok(()),
            Some(serde_json::Value::String(s)) => write_bounded_data(f, s),
            Some(other) => write_bounded_data(f, &other.to_string()),
        }
    }
}

/// Append `: {data}` to the rendered error, truncating past
/// [`MAX_RENDERED_DATA_BYTES`] with an ellipsis marker (backing off to the
/// previous char boundary so multi-byte chars never split).
fn write_bounded_data(f: &mut fmt::Formatter<'_>, data: &str) -> fmt::Result {
    if data.len() <= MAX_RENDERED_DATA_BYTES {
        return write!(f, ": {data}");
    }
    let mut end = MAX_RENDERED_DATA_BYTES;
    while !data.is_char_boundary(end) {
        end -= 1;
    }
    write!(f, ": {}… [truncated]", &data[..end])
}

/// Errors raised while spawning, talking to, or handshaking with an ACP agent.
#[derive(Debug, thiserror::Error)]
pub enum AcpError {
    /// The provider process could not be spawned or its pipes were missing.
    #[error("failed to spawn provider: {0}")]
    Spawn(String),

    /// The transport (writer/reader task or pipe) is closed or broke.
    #[error("transport closed: {0}")]
    Transport(String),

    /// A request did not receive a response within the timeout.
    #[error("request `{0}` timed out")]
    Timeout(String),

    /// The `session/prompt` turn went the whole idle window with no
    /// `session/update` traffic (activity-based idle timeout, distinct from
    /// the per-request [`AcpError::Timeout`]). Carries the idle window that
    /// elapsed. Structurally distinguishable so the turn worker can
    /// warn-and-continue instead of failing the turn; the Display rendering
    /// is pinned to [`PROMPT_IDLE_TIMEOUT_PREFIX`].
    #[error("session/prompt idle timeout ({0:?} of silence)")]
    PromptIdleTimeout(Duration),

    /// The agent returned a JSON-RPC error response.
    #[error("{0}")]
    Rpc(JsonRpcError),

    /// Serialization/deserialization of a payload failed.
    #[error("serialization error: {0}")]
    Serde(String),

    /// The agent requires authentication; carries the provider login hint.
    #[error("{0}")]
    Auth(String),

    /// A protocol-level violation (malformed/unexpected message).
    #[error("protocol error: {0}")]
    Protocol(String),

    /// A client-served filesystem request failed: either a sandbox violation
    /// (path outside the session worktree) or an underlying IO error (§6.7).
    #[error("filesystem error: {0}")]
    Fs(String),

    /// A client-served terminal request failed on the PTY host (spawn failure,
    /// unknown terminal id, or IO error) (§6.7).
    #[error("terminal error: {0}")]
    Terminal(String),
}

impl From<serde_json::Error> for AcpError {
    fn from(e: serde_json::Error) -> Self {
        AcpError::Serde(e.to_string())
    }
}

/// Convenience result alias for ACP operations.
pub type AcpResult<T> = Result<T, AcpError>;

/// Connection-class disconnect markers: substrings (matched case-insensitively)
/// that identify a transient upstream drop — the kind a laptop sleep or a
/// remote-side reconnect produces, which a sleep-resume attempt can recover
/// from. Kept to the concrete phrasings that transports and upstream providers
/// actually emit; anything not on this allowlist is treated as terminal.
const TRANSIENT_DISCONNECT_MARKERS: &[&str] = &[
    // TCP resets and their raw errno/OS renderings.
    "connection reset",
    "reset by peer",
    "econnreset",
    "connection aborted",
    "econnaborted",
    // Half-open / dead pipe writes.
    "broken pipe",
    "epipe",
    // Truncated reads: the peer vanished mid-frame.
    "unexpected eof",
    "unexpected end of file",
    "connection closed",
    "closed mid-response",
    "closed mid response",
    "stream closed",
    // The turn ended without the terminal `stop_reason` frame — the upstream
    // stream was cut short rather than completing.
    "stream ended",
    "without a stop reason",
    "without stop reason",
    "ended without stop",
];

/// Terminal markers that override the transient allowlist: an HTTP 4xx-class
/// failure or an auth/quota rejection must surface even if its text also
/// happens to mention a closed connection. These are unambiguous request-level
/// phrasings, not connection-class noise.
const TERMINAL_MESSAGE_MARKERS: &[&str] = &[
    // HTTP 4xx status phrasings (client errors — retrying can't fix them).
    "http 4",
    "status 4",
    "status: 4",
    "status code 4",
    // JSON renderings (no space after the colon), e.g. a bridge error
    // embedding `{"status":404,...}` verbatim.
    "\"status\":4",
    "\"status\": 4",
    "400 bad request",
    "401 unauthorized",
    "403 forbidden",
    "404 not found",
    "422 unprocessable",
    "429 too many requests",
    "bad request",
    "unauthorized",
    "forbidden",
    "too many requests",
    // Provider request-level rejections (Anthropic/OpenAI-style error types
    // and their common phrasings) — retrying cannot fix these either.
    "invalid_request_error",
    "authentication_error",
    "permission_error",
    "not_found_error",
    "model not found",
    "invalid api key",
];

/// Provider-fetch failure markers (matched case-insensitively) beyond the
/// connection-class [`TRANSIENT_DISCONNECT_MARKERS`]: the shapes a provider
/// bridge (e.g. codex-acp / auggie wrapping a Node `fetch`) renders into its
/// `-32603 Internal error` when the upstream model endpoint is transiently
/// unreachable (intent-hq/monorepo#3007). The observed instance:
/// `fetch failed (EPIPE: connect EPIPE 34.36.229.120:443):
/// {"apiStatus":"unavailable",…}`.
const TRANSIENT_PROVIDER_FETCH_MARKERS: &[&str] = &[
    // The provider itself reported a transient availability problem.
    "\"apistatus\":\"unavailable\"",
    "\"apistatus\": \"unavailable\"",
    "apistatus: unavailable",
    "\"apistatus\":\"overloaded\"",
    "\"apistatus\": \"overloaded\"",
    "apistatus: overloaded",
    // Node/undici connect-level fetch failures.
    "fetch failed",
    "econnrefused",
    "etimedout",
    "socket hang up",
];

/// Classify a rendered error message: does it describe a transient upstream
/// disconnect (eligible for sleep-resume) rather than a terminal failure?
///
/// The service layer flattens prompt errors to plain strings at its wrap
/// boundary (see [`PROMPT_IDLE_TIMEOUT_PREFIX`]), so downstream callers only
/// have the message. This is the shared string path used by both that flattened
/// form and [`is_transient_upstream_disconnect`].
///
/// Denylist wins over allowlist: a message carrying a terminal marker (4xx /
/// auth / quota) is never transient, even if it also mentions a closed
/// connection.
pub(crate) fn message_is_transient_upstream_disconnect(message: &str) -> bool {
    let msg = message.to_ascii_lowercase();
    if TERMINAL_MESSAGE_MARKERS.iter().any(|m| msg.contains(m)) {
        return false;
    }
    TRANSIENT_DISCONNECT_MARKERS.iter().any(|m| msg.contains(m))
}

/// Decide whether `err` is a transient upstream disconnect that a sleep-resume
/// attempt can recover from, as opposed to a real, terminal error that must
/// still surface.
///
/// Structurally terminal variants ([`AcpError::Auth`], [`AcpError::Serde`],
/// [`AcpError::Protocol`], [`AcpError::PromptIdleTimeout`]) are rejected by
/// construction — regardless of any incidental substring in their rendered
/// text. Everything else is classified by its rendered message via
/// [`message_is_transient_upstream_disconnect`], since connection-class drops
/// surface as [`AcpError::Transport`]/[`AcpError::Rpc`] text (and, once
/// flattened, as bare strings).
///
/// Pure classification only: no I/O, no state, no resume decision.
pub fn is_transient_upstream_disconnect(err: &AcpError) -> bool {
    match err {
        AcpError::Auth(_)
        | AcpError::Serde(_)
        | AcpError::Protocol(_)
        | AcpError::PromptIdleTimeout(_) => false,
        other => message_is_transient_upstream_disconnect(&other.to_string()),
    }
}

/// Decide whether `err` is a transient provider-fetch failure that an
/// in-place `session/prompt` retry can recover from
/// (intent-hq/monorepo#3007): the provider bridge answered the prompt with a
/// JSON-RPC error whose rendered text describes a transient upstream fault —
/// a connect-level fetch failure (EPIPE/ECONNRESET/ECONNREFUSED/timeout) or
/// an explicit provider `apiStatus: unavailable`/`overloaded` payload.
///
/// Deliberately narrower than [`is_transient_upstream_disconnect`]:
/// only [`AcpError::Rpc`] qualifies. A provider bridge that answered with an
/// error is alive and can serve a retried prompt on the same connection;
/// transport-shaped failures (closed pipe, dead child — including the
/// synthesized code-0 "agent stdout closed") are excluded because retrying on
/// a dead transport cannot succeed — those keep their existing recovery paths
/// (silent redrive on a fresh child, sleep-resume enrollment). The terminal
/// denylist wins as usual: auth failures, invalid requests, and
/// model-not-found rejections are never retried.
pub fn is_transient_provider_fetch_failure(err: &AcpError) -> bool {
    match err {
        AcpError::Rpc(e) => {
            !(e.code == 0 && e.message == "agent stdout closed")
                && message_is_transient_provider_fetch_failure(&err.to_string())
        }
        _ => false,
    }
}

/// Message-level classification backing [`is_transient_provider_fetch_failure`]:
/// the rendered text (message + bounded `data`) matches a transient
/// provider-fetch or connection-class marker, and no terminal marker.
/// Denylist wins over allowlist, same contract as
/// [`message_is_transient_upstream_disconnect`].
pub(crate) fn message_is_transient_provider_fetch_failure(message: &str) -> bool {
    let msg = message.to_ascii_lowercase();
    if TERMINAL_MESSAGE_MARKERS.iter().any(|m| msg.contains(m)) {
        return false;
    }
    TRANSIENT_PROVIDER_FETCH_MARKERS
        .iter()
        .any(|m| msg.contains(m))
        || TRANSIENT_DISCONNECT_MARKERS.iter().any(|m| msg.contains(m))
}

#[cfg(test)]
mod classifier_tests {
    use super::*;

    #[test]
    fn classifies_connection_reset_strings_as_transient() {
        for msg in [
            "transport closed: Connection reset by peer (os error 54)",
            "read error: ECONNRESET",
            "write failed: Broken pipe (os error 32)",
            "reader error: unexpected EOF during frame read",
            "connection closed by upstream before response",
            "provider closed mid-response",
            "stream ended without a stop reason",
            "the turn ended without stop reason",
        ] {
            assert!(
                message_is_transient_upstream_disconnect(msg),
                "expected transient: {msg:?}"
            );
        }
    }

    #[test]
    fn classifies_transient_transport_error_variant() {
        let err = AcpError::Transport("Connection reset by peer".to_string());
        assert!(is_transient_upstream_disconnect(&err));
    }

    #[test]
    fn classifies_rpc_connection_drop_as_transient() {
        let err = AcpError::Rpc(JsonRpcError {
            code: -32603,
            message: "upstream connection closed mid-response".to_string(),
            data: None,
        });
        assert!(is_transient_upstream_disconnect(&err));
    }

    #[test]
    fn classifies_auth_error_as_terminal() {
        let err = AcpError::Auth("login required: run `provider auth login`".to_string());
        assert!(!is_transient_upstream_disconnect(&err));
    }

    #[test]
    fn classifies_protocol_and_serde_errors_as_terminal() {
        let protocol = AcpError::Protocol("invalid session/prompt response".to_string());
        let serde = AcpError::Serde("expected value at line 1 column 1".to_string());
        assert!(!is_transient_upstream_disconnect(&protocol));
        assert!(!is_transient_upstream_disconnect(&serde));
    }

    #[test]
    fn classifies_idle_timeout_as_terminal() {
        let err = AcpError::PromptIdleTimeout(Duration::from_secs(120));
        assert!(!is_transient_upstream_disconnect(&err));
    }

    #[test]
    fn classifies_http_4xx_messages_as_terminal() {
        for msg in [
            "HTTP 400 Bad Request",
            "request failed with status 401",
            "403 Forbidden",
            "429 Too Many Requests",
        ] {
            assert!(
                !message_is_transient_upstream_disconnect(msg),
                "expected terminal: {msg:?}"
            );
        }
    }

    #[test]
    fn classifier_denylist_beats_allowlist() {
        // A 4xx that also mentions a closed connection stays terminal.
        let msg = "HTTP 400 Bad Request: connection closed";
        assert!(!message_is_transient_upstream_disconnect(msg));
    }

    #[test]
    fn classifier_rejects_unrelated_errors() {
        for msg in [
            "failed to spawn provider: no such file or directory",
            "filesystem error: path outside session worktree",
            "some unrelated failure",
        ] {
            assert!(
                !message_is_transient_upstream_disconnect(msg),
                "expected terminal: {msg:?}"
            );
        }
    }

    /// The observed monorepo#3007 shape: `-32603` wrapping a Node fetch EPIPE
    /// with an `apiStatus: unavailable` payload in `data` classifies as a
    /// transient provider-fetch failure.
    #[test]
    fn classifies_provider_fetch_epipe_unavailable_as_transient() {
        let err = AcpError::Rpc(JsonRpcError {
            code: -32603,
            message: "Internal error".to_string(),
            data: Some(serde_json::Value::String(
                "fetch failed (EPIPE: connect EPIPE 34.36.229.120:443): \
                 {\"apiStatus\":\"unavailable\",\"message\":\"fetch failed (EPIPE: connect EPIPE 34.36.229.120:443)\"}"
                    .to_string(),
            )),
        });
        assert!(is_transient_provider_fetch_failure(&err));
    }

    #[test]
    fn classifies_provider_fetch_failure_messages_as_transient() {
        for msg in [
            "JSON-RPC error -32603: Internal error: fetch failed (ECONNREFUSED)",
            "JSON-RPC error -32603: Internal error: {\"apiStatus\":\"unavailable\"}",
            "JSON-RPC error -32603: Internal error: {\"apiStatus\": \"overloaded\"}",
            "JSON-RPC error -32603: Internal error: socket hang up",
            "JSON-RPC error -32603: Internal error: connect ETIMEDOUT 1.2.3.4:443",
        ] {
            assert!(
                message_is_transient_provider_fetch_failure(msg),
                "expected transient: {msg:?}"
            );
        }
    }

    /// Genuinely terminal provider rejections never classify as retryable
    /// fetch failures — the denylist wins even when the text also carries a
    /// fetch-failure phrase.
    #[test]
    fn provider_fetch_classifier_keeps_terminal_errors_terminal() {
        for msg in [
            "JSON-RPC error -32603: Internal error: 401 Unauthorized",
            "JSON-RPC error -32603: Internal error: invalid_request_error",
            "JSON-RPC error -32603: Internal error: model not found: claude-nope",
            "JSON-RPC error -32603: Internal error: fetch failed: 403 Forbidden",
            // JSON status rendering (no space after the colon).
            "JSON-RPC error -32603: Internal error: fetch failed: {\"status\":404,\"error\":\"not found\"}",
            "JSON-RPC error -32603: Internal error: invalid api key",
            "some unrelated failure",
        ] {
            assert!(
                !message_is_transient_provider_fetch_failure(msg),
                "expected terminal: {msg:?}"
            );
        }
    }

    /// Only `Rpc`-variant errors qualify: transport-shaped failures keep
    /// their existing recovery paths (silent redrive, sleep-resume).
    #[test]
    fn provider_fetch_classifier_rejects_non_rpc_variants() {
        let transport = AcpError::Transport("broken pipe (EPIPE)".to_string());
        assert!(!is_transient_provider_fetch_failure(&transport));
        let stdout_closed = AcpError::Rpc(JsonRpcError {
            code: 0,
            message: "agent stdout closed".to_string(),
            data: None,
        });
        assert!(!is_transient_provider_fetch_failure(&stdout_closed));
        let auth = AcpError::Auth("login required".to_string());
        assert!(!is_transient_provider_fetch_failure(&auth));
    }
}
