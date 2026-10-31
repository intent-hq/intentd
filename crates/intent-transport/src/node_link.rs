//! The private head-to-node link for remote agent launch: a WSS connection
//! authenticated by a pinned server certificate (head side) and a per-lease
//! bearer token (node side), carrying [`LinkFrame`]s for control, journal and
//! request/response RPC, plus a second authenticated connection for git
//! traffic.
//!
//! Head dials the node. TLS, including the certificate pin, is verified
//! before any HTTP byte (and so the token) is written. The node checks the
//! token before upgrading, then accepts the head's [`LeaseBinding`] only if
//! its generation is newer than the persisted one; the older connection,
//! and any git connection of an older generation, is closed.
//!
//! Error text is fixed per failure kind and never carries the token, the
//! endpoint's credentials or a peer-supplied string, so link errors and logs
//! are safe to surface.
//!
//! Daemon-to-daemon remote agents serve the same link on the daemon's own
//! WSS listener ([`ExecutorLinks`], admitted by executor credentials and
//! bound per credential). The separate node listener ([`NodeListener`]),
//! its certificate, [`BindingFile`] and [`LeaseToken`] remain only for
//! `intentd node` and are removed together with it when the runner moves
//! into the daemon.

mod binding_file;
mod dial;
mod executor;
mod git;
#[cfg(unix)]
mod git_helper;
mod git_session;
mod link;
mod listener;
mod pin;
mod reconnect;
mod secret;

pub use binding_file::BindingFile;
pub(crate) use dial::socket_config;
pub use dial::{dial_git, dial_link, DialedLink, NodeEndpoint, TailcatRoute};
pub(crate) use executor::ExecutorRoute;
pub use executor::{ExecutorHost, ExecutorLinks, REVOKED_CLOSE_CODE, REVOKED_CLOSE_REASON};
pub use git::{GitConnection, GitSender};
#[cfg(unix)]
pub use git_helper::run_git_remote_helper;
#[cfg(unix)]
pub use git_session::serve_helpers;
pub use git_session::{
    codes as git_codes, serve_git, CloseStatus, GitSession, NodeGitAccess, NodeGitSessions,
    OpenRequest, MAX_SESSIONS,
};
pub use intent_core::node_link::*;
pub use link::{IncomingRequest, LinkEvent, LinkEvents, NodeLink};
pub use listener::{NodeConnection, NodeListener, NodeListenerConfig, WelcomeFn};
pub use pin::CertificatePin;
pub use reconnect::{dial_with_backoff, Backoff};
pub use secret::LeaseToken;

use intent_core::Error;

/// URL path of the control/journal/RPC connection.
pub const LINK_PATH: &str = "/node/link";
/// URL path of the git connection.
pub const GIT_PATH: &str = "/node/git";

/// Stable error codes. Each link error's message starts with one of these.
pub mod codes {
    /// The node is unreachable, the connection dropped or timed out.
    pub const UNAVAILABLE: &str = "node-link-unavailable";
    /// The node's certificate does not match the configured pin.
    pub const CERTIFICATE_MISMATCH: &str = "node-link-certificate-mismatch";
    /// The node rejected the lease token.
    pub const UNAUTHORIZED: &str = "node-link-unauthorized";
    /// The offered generation is not newer than the node's binding, or a git
    /// connection names a binding that is no longer current.
    pub const STALE: &str = "node-link-stale";
    /// Another head owns the node.
    pub const OTHER_HEAD: &str = "node-link-other-head";
    /// The binding names a different node.
    pub const WRONG_NODE: &str = "node-link-wrong-node";
    /// The peers speak different link protocol versions.
    pub const VERSION_MISMATCH: &str = "node-link-version-mismatch";
    /// The peer broke the link protocol.
    pub const PROTOCOL: &str = "node-link-protocol";
    /// The configured endpoint, pin or token is malformed.
    pub const CONFIG: &str = "node-link-config";
    /// The link closed before a request completed.
    pub const CLOSED: &str = "node-link-closed";
    /// A request timed out.
    pub const TIMEOUT: &str = "node-link-timeout";
    /// The node could not read or persist its binding.
    pub const BINDING_STORE: &str = "node-link-binding-store";
}

fn unavailable() -> Error {
    Error::Internal(codes::UNAVAILABLE.into())
}

fn protocol(detail: &'static str) -> Error {
    Error::InvalidParams(format!("{}: {detail}", codes::PROTOCOL))
}

fn config(detail: &'static str) -> Error {
    Error::InvalidParams(format!("{}: {detail}", codes::CONFIG))
}

/// Whether a log event is the WebSocket library's wire logging: its trace
/// output prints whole upgrade requests (including the lease token header)
/// and frames, and its debug output prints peer-supplied close reasons.
/// Daemon log outputs must drop these so a `trace` or `debug` filter cannot
/// leak the token or log peer text.
#[must_use]
pub fn is_wire_trace(target: &str, level: &tracing::Level) -> bool {
    let wire = |prefix: &str| {
        target == prefix
            || target
                .strip_prefix(prefix)
                .is_some_and(|rest| rest.starts_with("::"))
    };
    *level >= tracing::Level::DEBUG && (wire("tungstenite") || wire("tokio_tungstenite"))
}

/// Wait until a watched flag turns `true` (or its sender is gone), without
/// holding the non-`Send` borrow across an await.
async fn wait_true(rx: &mut tokio::sync::watch::Receiver<bool>) {
    let _ = rx.wait_for(|flag| *flag).await;
}

/// The error code a link error carries, if it is a link error.
#[must_use]
pub fn error_code(error: &Error) -> Option<&str> {
    let message = match error {
        Error::Internal(m) | Error::InvalidParams(m) | Error::Forbidden(m) => m.as_str(),
        _ => return None,
    };
    let code = message.split(':').next().unwrap_or(message);
    code.starts_with("node-link-").then_some(code)
}

/// Whether redialing could succeed without an operator changing the
/// configuration: the node was unreachable, or a newer generation is needed.
/// A certificate, token, ownership or version failure is permanent.
#[must_use]
pub fn is_retryable(error: &Error) -> bool {
    matches!(
        error_code(error),
        Some(codes::UNAVAILABLE | codes::STALE | codes::CLOSED | codes::TIMEOUT | codes::PROTOCOL)
    )
}

#[cfg(test)]
mod tests;
