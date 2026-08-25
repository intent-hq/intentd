//! `ws.chat.*` bindings — the calling agent's own conversation.
//!
//! `unread()` returns the compact digest of the caller's unread conversation
//! tail (messages after the per-conversation seen marker, human-boundary
//! semantics in [`intent_core::WorkspaceApi::agent_chat_unread`]). TOP-LEVEL
//! agents only — a sub-agent has no user-facing chat — and gated behind
//! `agentFeatures.unreadSummaries` (default off): the toggle prunes this
//! namespace from the prelude and tool description, and the dispatch host
//! denies its frames (`gated_prefixes`), the same machinery as the other
//! feature gates. A successful read arms the caller's same-turn summarize
//! gate over the returned `fromMessageId..toMessageId` range (cleared at
//! turn end). On a truncated digest that range spans the ENTIRE unread tail
//! — `items` keeps only the oldest rows — so the gate deliberately covers
//! rows not shown as items (a summarize consumer hydrates full messages
//! itself).

use std::sync::Arc;

use intent_core::{AgentId, TurnAttachmentRegistry, WorkspaceApi, WorkspaceId};
use serde_json::Value;

use super::map_err;

pub(crate) const PRELUDE: &str = r"
    globalThis.ws = globalThis.ws || {};
    ws.chat = {
        unread: () => host({ method: 'chat.unread' }),
    };
";

/// Dispatch one `ws.chat.<method>` frame. `caller_agent_id` scopes the read
/// to the calling agent's own conversation; `turn_attachments` carries the
/// registry whose summarize gate the read arms (inert when absent — FE front
/// door, tests).
pub(crate) async fn dispatch(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    caller_agent_id: Option<&AgentId>,
    turn_attachments: Option<&Arc<TurnAttachmentRegistry>>,
    method: &str,
    _args: &Value,
) -> Result<Value, String> {
    match method {
        "unread" => {
            let caller = caller_agent_id.ok_or_else(|| {
                "ws.chat.unread is only available from a live agent turn (no caller agent is \
                 wired)"
                    .to_string()
            })?;
            let digest = api
                .agent_chat_unread(ws.clone(), caller.clone())
                .await
                .map_err(map_err)?;
            // Arm the same-turn summarize gate over exactly the returned
            // range; an empty digest (null toMessageId) arms nothing.
            if let (Some(registry), Some(to)) = (
                turn_attachments,
                digest.get("toMessageId").and_then(Value::as_str),
            ) {
                let from = digest.get("fromMessageId").and_then(Value::as_str);
                registry.arm_summarize_gate(caller, from, to);
            }
            Ok(digest)
        }
        other => Err(format!("host: unknown method `chat.{other}`")),
    }
}
