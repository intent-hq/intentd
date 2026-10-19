//! Narrow recovery for Codex rejecting a sandbox override before turn/start.
//!
//! codex-acp 2.1.1 advertises these mode ids, stores `set_mode` in memory, and
//! applies its `sandboxPolicy` on the NEXT prompt. A successful `set_mode` is not
//! evidence that enterprise policy permits that mode.

use agent_client_protocol::schema::v1::{
    PermissionOptionKind, RequestPermissionRequest, SessionModeState, ToolCallStatus, ToolKind,
};

use crate::{handshake::set_session_mode, AcpError, Connection};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    Full,
    Workspace,
    ReadOnly,
}

impl Mode {
    fn id(self) -> &'static str {
        match self {
            Self::Full => "agent-full-access",
            Self::Workspace => "workspace-write",
            Self::ReadOnly => "read-only",
        }
    }

    fn policy_name(self) -> &'static str {
        match self {
            Self::Full => "DangerFullAccess",
            Self::Workspace => "WorkspaceWrite",
            Self::ReadOnly => "ReadOnly",
        }
    }
}

#[derive(Debug)]
pub(crate) struct CodexSandbox {
    current: Mode,
    workspace: bool,
    read_only: bool,
}

impl CodexSandbox {
    fn from_modes(state: &SessionModeState) -> Option<Self> {
        let current = match state.current_mode_id.0.as_ref() {
            "agent-full-access" => Mode::Full,
            "workspace-write" | "agent" => Mode::Workspace,
            "read-only" => Mode::ReadOnly,
            _ => return None,
        };
        let supports = |id| {
            state
                .available_modes
                .iter()
                .any(|mode| mode.id.0.as_ref() == id)
        };
        Some(Self {
            current,
            workspace: supports("workspace-write"),
            read_only: supports("read-only"),
        })
    }

    fn next(&mut self, error: &AcpError) -> Option<Mode> {
        let allowed = allowed_modes(error, self.current)?;
        let next = if self.current == Mode::Full
            && self.workspace
            && allowed.contains(&"WorkspaceWrite")
        {
            Mode::Workspace
        } else if self.current != Mode::ReadOnly && self.read_only && allowed.contains(&"ReadOnly")
        {
            Mode::ReadOnly
        } else {
            return None;
        };
        // Reserve before awaiting set_mode: rejected candidates are never tried
        // twice, and later prompts can only move toward a stricter sandbox.
        self.current = next;
        Some(next)
    }
}

/// Recognize the nested JSON-RPC error from intent#6973, without matching
/// arbitrary provider prose, transport failures, or a different rejected mode.
fn allowed_modes(error: &AcpError, current: Mode) -> Option<Vec<&str>> {
    let AcpError::Rpc(error) = error else {
        return None;
    };
    if !matches!(error.code, -32603 | -32602) {
        return None;
    }
    let details = error.data.as_ref()?.get("details")?.as_str()?;
    let prefix = format!("invalid thread settings override: invalid value for `sandbox_mode`: `{}` is not in the allowed set [", current.policy_name());
    let remaining = details.strip_prefix(&prefix)?;
    let (allowed, source) = remaining.split_once(']')?;
    if !source.starts_with(" (set by ") || !source.contains("requirements") {
        return None;
    }
    Some(allowed.split(',').map(str::trim).collect())
}

/// Record only the actual mode returned by a Codex session. Repeated setup on
/// the same connection/session must not reset a previously selected fallback.
///
/// # Panics
/// Panics if the connection sandbox mutex is poisoned.
pub fn configure(
    conn: &Connection,
    provider: &str,
    session_id: &str,
    modes: Option<&SessionModeState>,
) {
    if provider != "codex" {
        return;
    }
    if let Some(state) = modes.and_then(CodexSandbox::from_modes) {
        conn.codex_sandboxes
            .lock()
            .unwrap()
            .entry(session_id.to_owned())
            .or_insert(state);
    }
}

/// Select the first supported, policy-permitted stricter mode. The caller must
/// prove the rejected prompt produced no output or client-served work before
/// calling this and again before replaying it.
///
/// # Errors
/// Returns unrelated set-mode errors unchanged. Policy rejections may advance
/// to the next candidate; exhaustion returns the last rejection.
///
/// # Panics
/// Panics if the connection sandbox mutex is poisoned.
pub async fn fallback(
    conn: &Connection,
    session_id: &str,
    error: &AcpError,
) -> crate::error::AcpResult<Option<&'static str>> {
    let next = conn
        .codex_sandboxes
        .lock()
        .unwrap()
        .get_mut(session_id)
        .and_then(|s| s.next(error));
    let Some(mut mode) = next else {
        return Ok(None);
    };
    loop {
        match set_session_mode(conn, session_id, mode.id()).await {
            Ok(()) => return Ok(Some(mode.id())),
            Err(error) => {
                let next = conn
                    .codex_sandboxes
                    .lock()
                    .unwrap()
                    .get_mut(session_id)
                    .and_then(|s| s.next(&error));
                match next {
                    Some(next) => mode = next,
                    None => return Err(error),
                }
            }
        }
    }
}

/// Never let the daemon silently approve sandbox escalation in a restricted mode.
pub(crate) fn restricted(conn: &Connection, session_id: &str) -> bool {
    conn.codex_sandboxes
        .lock()
        .unwrap()
        .get(session_id)
        .is_some_and(|s| s.current != Mode::Full)
}

/// codex-acp 2.1.1's `PlanReviewReporter` asks for plan approval through the same
/// ACP method as sandbox escalation. Its `implement_plan` branch changes only
/// collaboration mode and sends the next prompt with the SAME `agentMode`.
/// Recognize that specific request; unknown requests remain denied.
pub(crate) fn is_plan_approval(request: &RequestPermissionRequest) -> bool {
    let call = &request.tool_call;
    let fields = &call.fields;
    fields.kind == Some(ToolKind::SwitchMode)
        && fields.status == Some(ToolCallStatus::Pending)
        && fields.title.as_deref() == Some("Implement this plan?")
        && call
            .tool_call_id
            .0
            .strip_prefix("plan-review:")
            .is_some_and(|id| !id.is_empty())
        && fields
            .raw_input
            .as_ref()
            .and_then(serde_json::Value::as_object)
            .is_some_and(|input| {
                input.len() == 1 && input.get("plan").is_some_and(serde_json::Value::is_string)
            })
        && request.options.len() == 2
        && request.options.iter().any(|option| {
            option.option_id.0.as_ref() == "implement_plan"
                && option.kind == PermissionOptionKind::AllowOnce
        })
        && request.options.iter().any(|option| {
            option.option_id.0.as_ref() == "revise_plan"
                && option.kind == PermissionOptionKind::RejectOnce
        })
}

pub(crate) fn read_only(conn: &Connection, session_id: &str) -> bool {
    conn.codex_sandboxes
        .lock()
        .unwrap()
        .get(session_id)
        .is_some_and(|s| s.current == Mode::ReadOnly)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::JsonRpcError;
    use serde_json::json;

    #[test]
    fn only_pinned_non_escalating_plan_request_keeps_configured_permissions() {
        let request = json!({
            "sessionId": "session",
            "toolCall": {
                "toolCallId": "plan-review:plan-1", "title": "Implement this plan?",
                "kind": "switch_mode", "status": "pending",
                "rawInput": {"plan": "Inspect the workspace."}
            },
            "options": [
                {"optionId": "implement_plan", "name": "Yes, implement this plan", "kind": "allow_once"},
                {"optionId": "revise_plan", "name": "No, and tell Codex what to do differently", "kind": "reject_once"}
            ]
        });
        assert!(is_plan_approval(
            &serde_json::from_value(request.clone()).unwrap()
        ));
        for (path, value) in [
            ("/toolCall/kind", json!("execute")),
            ("/toolCall/kind", json!("edit")),
            ("/toolCall/toolCallId", json!("plan-review:")),
            ("/toolCall/title", json!("Change sandbox mode?")),
            (
                "/toolCall/rawInput",
                json!({"plan": "Plan", "command": "escape"}),
            ),
            ("/options/0/kind", json!("allow_always")),
            ("/options/0/optionId", json!("allow_once")),
            ("/options/1/optionId", json!("reject_once")),
        ] {
            let mut altered = request.clone();
            *altered.pointer_mut(path).unwrap() = value;
            assert!(
                !is_plan_approval(&serde_json::from_value(altered).unwrap()),
                "{path}"
            );
        }
    }

    fn rejection(mode: Mode, allowed: &str) -> AcpError {
        AcpError::Rpc(JsonRpcError {
            code: -32603,
            message: "Internal error".into(),
            data: Some(
                json!({"details":format!("invalid thread settings override: invalid value for `sandbox_mode`: `{}` is not in the allowed set [{allowed}] (set by enterprise-managed requirements All users (9da573b3-18c6-493b-b593-89d0770df50a))",mode.policy_name())}),
            ),
        })
    }

    fn state(current: Mode, workspace: bool, read_only: bool) -> CodexSandbox {
        CodexSandbox {
            current,
            workspace,
            read_only,
        }
    }

    #[test]
    fn exact_nested_policy_error_selects_workspace_then_read_only_once() {
        let mut s = state(Mode::Full, true, true);
        assert_eq!(
            s.next(&rejection(Mode::Full, "WorkspaceWrite, ReadOnly")),
            Some(Mode::Workspace)
        );
        assert_eq!(
            s.next(&rejection(Mode::Full, "WorkspaceWrite, ReadOnly")),
            None
        );
        assert_eq!(
            s.next(&rejection(Mode::Workspace, "ReadOnly")),
            Some(Mode::ReadOnly)
        );
        assert_eq!(s.next(&rejection(Mode::ReadOnly, "WorkspaceWrite")), None);
    }

    #[test]
    fn candidates_require_both_advertised_support_and_policy_permission() {
        for workspace in [false, true] {
            let mut s = state(Mode::Full, workspace, true);
            assert_eq!(
                s.next(&rejection(Mode::Full, "ReadOnly")),
                Some(Mode::ReadOnly)
            );
        }
        let mut s = state(Mode::Full, false, true);
        assert_eq!(
            s.next(&rejection(Mode::Full, "WorkspaceWrite, ReadOnly")),
            Some(Mode::ReadOnly)
        );
        for (workspace, read_only, allowed) in [
            (false, false, "WorkspaceWrite, ReadOnly"),
            (true, true, ""),
            (false, true, "WorkspaceWrite"),
        ] {
            assert_eq!(
                state(Mode::Full, workspace, read_only).next(&rejection(Mode::Full, allowed)),
                None
            );
        }
    }

    #[test]
    fn unrelated_or_unstructured_errors_and_stricter_modes_never_escalate() {
        let full = rejection(Mode::Full, "WorkspaceWrite, ReadOnly");
        assert_eq!(state(Mode::ReadOnly, true, true).next(&full), None);
        assert_eq!(state(Mode::Workspace, true, true).next(&full), None);
        for error in [
            AcpError::Protocol(full.to_string()),
            AcpError::Rpc(JsonRpcError {
                code: -32603,
                message: full.to_string(),
                data: None,
            }),
            AcpError::Rpc(JsonRpcError {
                code: -32603,
                message: "Internal error".into(),
                data: Some(json!({"details":"model unavailable"})),
            }),
        ] {
            assert_eq!(state(Mode::Full, true, true).next(&error), None);
        }
    }
}

#[cfg(test)]
mod wire_tests {
    use super::*;
    use crate::{error::JsonRpcError, ConnectionHooks};
    use serde_json::{json, Value};
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

    fn error(mode: &str, allowed: &str) -> AcpError {
        AcpError::Rpc(JsonRpcError {
            code: -32603,
            message: "Internal error".into(),
            data: Some(
                json!({"details":format!("invalid thread settings override: invalid value for `sandbox_mode`: `{mode}` is not in the allowed set [{allowed}] (set by enterprise-managed requirements All users)")}),
            ),
        })
    }

    #[tokio::test]
    async fn set_mode_policy_rejection_advances_but_unrelated_failure_stops() {
        for unrelated in [false, true] {
            let (input, output) = tokio::io::duplex(4096);
            let (mut replies, read) = tokio::io::duplex(4096);
            let conn = Connection::new(input, read, None, ConnectionHooks::default());
            let modes: SessionModeState = serde_json::from_value(json!({"currentModeId":"agent-full-access","availableModes":[{"id":"workspace-write","name":"Workspace"},{"id":"read-only","name":"Read"}]})).unwrap();
            configure(&conn, "codex", "s", Some(&modes));
            let initial = error("DangerFullAccess", "WorkspaceWrite, ReadOnly");
            let call = fallback(&conn, "s", &initial);
            let peer = async {
                let mut reader = BufReader::new(output).lines();
                let first: Value =
                    serde_json::from_str(&reader.next_line().await.unwrap().unwrap()).unwrap();
                assert_eq!(first["params"]["modeId"], "workspace-write");
                let AcpError::Rpc(mut rejection) = error("WorkspaceWrite", "ReadOnly") else {
                    unreachable!()
                };
                if unrelated {
                    rejection.data = Some(json!({"details":"model unavailable"}));
                }
                replies.write_all(format!("{}\n",json!({"jsonrpc":"2.0","id":first["id"],"error":{"code":rejection.code,"message":rejection.message,"data":rejection.data}})).as_bytes()).await.unwrap();
                if !unrelated {
                    let second: Value =
                        serde_json::from_str(&reader.next_line().await.unwrap().unwrap()).unwrap();
                    assert_eq!(second["params"]["modeId"], "read-only");
                    replies
                        .write_all(
                            format!(
                                "{}\n",
                                json!({"jsonrpc":"2.0","id":second["id"],"result":{}})
                            )
                            .as_bytes(),
                        )
                        .await
                        .unwrap();
                }
            };
            let (result, ()) = tokio::join!(call, peer);
            if unrelated {
                assert!(result
                    .unwrap_err()
                    .to_string()
                    .contains("model unavailable"));
            } else {
                assert_eq!(result.unwrap(), Some("read-only"));
                configure(&conn, "codex", "s", Some(&modes));
                assert!(read_only(&conn, "s"), "repeated setup must retain fallback");
                assert_eq!(fallback(&conn, "s", &initial).await.unwrap(), None);
            }
        }
    }

    #[tokio::test]
    async fn only_codex_advertised_modes_enable_recovery() {
        let (input, _output) = tokio::io::duplex(4096);
        let (_replies, read) = tokio::io::duplex(4096);
        let conn = Connection::new(input, read, None, ConnectionHooks::default());
        let modes: SessionModeState = serde_json::from_value(json!({"currentModeId":"read-only","availableModes":[{"id":"read-only","name":"Read"}]})).unwrap();
        configure(&conn, "mock", "s", Some(&modes));
        assert!(!restricted(&conn, "s"));
        configure(&conn, "codex", "s", None);
        assert!(!restricted(&conn, "s"));
        configure(&conn, "codex", "s", Some(&modes));
        assert!(restricted(&conn, "s"));
        assert_eq!(
            fallback(&conn, "s", &error("ReadOnly", "WorkspaceWrite"))
                .await
                .unwrap(),
            None
        );
    }
}
