//! ACP requests that import a provider's existing sessions into Intent:
//! the capability probe, `session/list` and `session/fork`.
//!
//! The requests are built and read as raw JSON. `session/fork` is behind the
//! schema crate's `unstable_session_fork` feature, which intentd does not
//! enable, so the typed `InitializeResponse` drops the fork capability; the
//! probe reads it from the raw `initialize` result instead.

use std::path::Path;
use std::time::Duration;

use serde_json::{json, Value};

use crate::error::{AcpError, AcpResult};
use crate::transport::Connection;

/// What an adapter advertised in `initialize` for importing sessions.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ImportCapabilities {
    /// `agentCapabilities.sessionCapabilities.list` is present.
    pub list: bool,
    /// `agentCapabilities.loadSession` is `true`.
    pub load: bool,
    /// `agentCapabilities.sessionCapabilities.fork` is present.
    pub fork: bool,
}

impl ImportCapabilities {
    /// Read the capabilities from a raw `initialize` result.
    #[must_use]
    pub fn from_initialize(raw: &Value) -> Self {
        let caps = &raw["agentCapabilities"];
        let present = |v: &Value| v.is_object();
        Self {
            list: present(&caps["sessionCapabilities"]["list"]),
            load: caps["loadSession"].as_bool() == Some(true),
            fork: present(&caps["sessionCapabilities"]["fork"]),
        }
    }

    /// Whether sessions can be listed and then brought into Intent: listed,
    /// and either loaded or forked.
    #[must_use]
    pub fn importable(self) -> bool {
        self.list && (self.load || self.fork)
    }
}

/// One page of `session/list`: the raw `sessions` entries and the opaque
/// `nextCursor`, if any.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SessionPage {
    /// Raw `SessionInfo` objects (`sessionId`, `cwd`, `title?`, `updatedAt?`).
    pub sessions: Vec<Value>,
    /// The cursor of the next page; `None` on the last page.
    pub next_cursor: Option<String>,
}

/// `session/list { cursor? }`. No `cwd` filter: every session of the adapter.
///
/// # Errors
///
/// Propagates the transport/RPC error, or [`AcpError::Protocol`] when the
/// result has no `sessions` array.
pub async fn list_sessions(
    conn: &Connection,
    cursor: Option<&str>,
    timeout: Duration,
) -> AcpResult<SessionPage> {
    let mut params = json!({});
    if let Some(cursor) = cursor {
        params["cursor"] = json!(cursor);
    }
    let result = conn
        .request_timeout("session/list", params, timeout)
        .await?;
    let sessions = result["sessions"]
        .as_array()
        .cloned()
        .ok_or_else(|| AcpError::Protocol("invalid session/list response: no sessions".into()))?;
    let next_cursor = result["nextCursor"]
        .as_str()
        .filter(|c| !c.is_empty())
        .map(str::to_owned);
    Ok(SessionPage {
        sessions,
        next_cursor,
    })
}

/// `session/fork { sessionId, cwd, mcpServers: [] }` → the new session id.
///
/// # Errors
///
/// Propagates the transport/RPC error, or [`AcpError::Protocol`] when the
/// result has no `sessionId`.
pub async fn fork_session(
    conn: &Connection,
    session_id: &str,
    cwd: &Path,
    timeout: Duration,
) -> AcpResult<String> {
    let params = json!({ "sessionId": session_id, "cwd": cwd, "mcpServers": [] });
    let result = conn
        .request_timeout("session/fork", params, timeout)
        .await?;
    result["sessionId"]
        .as_str()
        .filter(|id| !id.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| AcpError::Protocol("invalid session/fork response: no sessionId".into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capabilities_read_list_load_and_fork() {
        let raw = json!({ "agentCapabilities": {
            "loadSession": true,
            "sessionCapabilities": { "list": {}, "fork": {} }
        }});
        let caps = ImportCapabilities::from_initialize(&raw);
        assert_eq!(
            caps,
            ImportCapabilities {
                list: true,
                load: true,
                fork: true
            }
        );
        assert!(caps.importable());
    }

    #[test]
    fn list_without_load_or_fork_is_not_importable() {
        let raw = json!({ "agentCapabilities": { "sessionCapabilities": { "list": {} } } });
        assert!(!ImportCapabilities::from_initialize(&raw).importable());
        let raw = json!({ "agentCapabilities": { "loadSession": true } });
        assert!(!ImportCapabilities::from_initialize(&raw).importable());
        assert!(!ImportCapabilities::from_initialize(&json!({})).importable());
    }
}
