//! Turn-attachment registry (§7.1 deterministic attach).
//!
//! In-process store for canonical MIME-typed resource blocks a tool wants
//! attached to the calling agent's transcript. Providers echo MCP tool
//! outputs back to the daemon with no fidelity guarantee (auggie collapses
//! the content-item array into one hard-wrapped string, dropping resource
//! items — the intent-hq/monorepo#511 regression class), so the daemon-side
//! tool dispatch registers the canonical payload here *before* returning to
//! the provider, keyed by a short nonce embedded in the model-facing output.
//! When the provider's `tool_call_update` echo arrives, the transcript
//! writer claims the entry (nonce match, with a FIFO fallback for garbled
//! echoes of the daemon's own `workspace_api` tool) and attaches the
//! canonical block — the echo is never parsed on a registry hit. Ordering is
//! guaranteed by construction: registration happens while the tool call is
//! being served, strictly before the provider can echo its completion.
//!
//! Entries carry an [`AttachmentPolicy`]: `AtToolResult` blocks are attached
//! right after the registering tool call's `tool_result`; `AtTurnEnd` blocks
//! are appended when the assistant turn finalizes. Unclaimed `AtToolResult`
//! leftovers are dropped at turn end so nothing leaks across turns.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use serde_json::{json, Value};

use crate::AgentId;

/// JSON key the dispatch layer stamps into a registered payload (and its
/// model-facing echo) so the claim can match the echo back to the entry.
pub const ATTACHMENT_ID_KEY: &str = "attachmentId";

/// Nonce prefix — short enough that a provider's hard-wrap (1000-char
/// columns) rarely splits the id; the FIFO fallback covers when it does.
const NONCE_PREFIX: &str = "tar-";

/// Entries older than this are evicted on any registry touch: an attachment
/// whose turn never completed (provider crash, daemon-side error path that
/// skipped the drain) must not attach to a later turn.
const TTL: Duration = Duration::from_secs(10 * 60);

/// Per-agent entry cap — a runaway tool loop cannot grow the registry
/// unboundedly; oldest entries are dropped first.
const MAX_PER_AGENT: usize = 32;

/// Mint a fresh attachment nonce: `tar-` + 12 hex chars. Short (16 chars
/// total) so a provider's column-wrap is unlikely to split it mid-id;
/// collision within one agent's TTL window is negligible.
pub fn new_attachment_id() -> String {
    let hex = uuid::Uuid::new_v4().simple().to_string();
    format!("{NONCE_PREFIX}{}", &hex[..12])
}

/// Where in the turn transcript a registered attachment is emitted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttachmentPolicy {
    /// Attach right after the registering tool call's `tool_result` block.
    AtToolResult,
    /// Attach as a trailing block when the assistant turn finalizes.
    AtTurnEnd,
}

/// One registered attachment: the canonical resource-block fields plus the
/// nonce that links it to the tool call's echoed output.
#[derive(Debug, Clone)]
pub struct TurnAttachment {
    /// The nonce embedded in the model-facing tool output ([`new_attachment_id`]).
    pub id: String,
    /// Where in the transcript this attachment is emitted.
    pub policy: AttachmentPolicy,
    /// Resource MIME type (e.g. `application/vnd.intent.proposal+json`).
    pub mime_type: String,
    /// Resource URI (e.g. `intent-proposal://settings-change/...`).
    pub uri: String,
    /// Human-readable resource name.
    pub name: String,
    /// Canonical serialized payload — the resource item's `text`.
    pub text: String,
}

impl TurnAttachment {
    /// Build the canonical `{ type: "resource", resource: {…} }` content item
    /// the transcript writer turns into a standalone block (§7.1 shape).
    pub fn resource_item(&self) -> Value {
        json!({
            "type": "resource",
            "resource": {
                "uri": self.uri,
                "name": self.name,
                "mimeType": self.mime_type,
                "text": self.text,
            }
        })
    }
}

struct Entry {
    attachment: TurnAttachment,
    registered_at: Instant,
}

/// Daemon-wide registry of pending turn attachments, keyed by agent. Shared
/// (via `Arc`) between the per-agent MCP dispatch (registration side) and the
/// transcript writer in `intent-services` (claim/drain side).
#[derive(Default)]
pub struct TurnAttachmentRegistry {
    inner: Mutex<HashMap<AgentId, Vec<Entry>>>,
}

impl TurnAttachmentRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a pending attachment for `agent_id`. Evicts expired entries
    /// and enforces the per-agent cap (oldest dropped first).
    pub fn register(&self, agent_id: &AgentId, attachment: TurnAttachment) {
        let mut inner = self.inner.lock().unwrap();
        let entries = inner.entry(agent_id.clone()).or_default();
        evict_expired(entries);
        if entries.len() >= MAX_PER_AGENT {
            entries.remove(0);
        }
        entries.push(Entry {
            attachment,
            registered_at: Instant::now(),
        });
    }

    /// Claim the `AtToolResult` attachment for a completed tool call.
    ///
    /// Precise path: the serialized `echoed_output` contains an entry's nonce
    /// (the dispatch layer stamped it into the model-facing output, so any
    /// non-garbled echo carries it). Fallback path: when no nonce matches and
    /// `tool_name` is the daemon's own `workspace_api` tool, the oldest
    /// `AtToolResult` entry is claimed FIFO — a garbled echo cannot defeat
    /// the attach, and only the tool that registers through this registry can
    /// trigger the blind claim. Returns `None` when nothing is pending (the
    /// caller falls back to echo parsing).
    pub fn claim_at_tool_result(
        &self,
        agent_id: &AgentId,
        echoed_output: Option<&Value>,
        tool_name: &str,
    ) -> Option<TurnAttachment> {
        let mut inner = self.inner.lock().unwrap();
        let entries = inner.get_mut(agent_id)?;
        evict_expired(entries);
        let echo = echoed_output.map(Value::to_string).unwrap_or_default();
        let by_nonce = entries.iter().position(|e| {
            e.attachment.policy == AttachmentPolicy::AtToolResult
                && !echo.is_empty()
                && echo.contains(&e.attachment.id)
        });
        let pos = by_nonce.or_else(|| {
            tool_name.contains("workspace_api").then(|| {
                entries
                    .iter()
                    .position(|e| e.attachment.policy == AttachmentPolicy::AtToolResult)
            })?
        })?;
        Some(entries.remove(pos).attachment)
    }

    /// Finish `agent_id`'s turn: return the pending `AtTurnEnd` attachments
    /// (in registration order) and clear ALL remaining entries — unclaimed
    /// `AtToolResult` leftovers are dropped so they cannot attach to a later
    /// turn.
    pub fn finish_turn(&self, agent_id: &AgentId) -> Vec<TurnAttachment> {
        let mut inner = self.inner.lock().unwrap();
        let Some(mut entries) = inner.remove(agent_id) else {
            return Vec::new();
        };
        evict_expired(&mut entries);
        entries
            .into_iter()
            .filter(|e| e.attachment.policy == AttachmentPolicy::AtTurnEnd)
            .map(|e| e.attachment)
            .collect()
    }
}

fn evict_expired(entries: &mut Vec<Entry>) {
    entries.retain(|e| e.registered_at.elapsed() < TTL);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn attachment(id: &str, policy: AttachmentPolicy) -> TurnAttachment {
        TurnAttachment {
            id: id.to_string(),
            policy,
            mime_type: "application/vnd.intent.proposal+json".to_string(),
            uri: format!("intent-proposal://test/{id}"),
            name: "Test".to_string(),
            text: format!("{{\"attachmentId\":\"{id}\"}}"),
        }
    }

    fn agent() -> AgentId {
        AgentId::from_string("agent-test")
    }

    #[test]
    fn new_attachment_id_is_short_and_prefixed() {
        let id = new_attachment_id();
        assert!(id.starts_with(NONCE_PREFIX));
        assert_eq!(id.len(), NONCE_PREFIX.len() + 12);
        assert_ne!(id, new_attachment_id());
    }

    #[test]
    fn claim_matches_nonce_in_echoed_output() {
        let reg = TurnAttachmentRegistry::new();
        let a = agent();
        reg.register(&a, attachment("tar-aaa", AttachmentPolicy::AtToolResult));
        reg.register(&a, attachment("tar-bbb", AttachmentPolicy::AtToolResult));
        // The echo carries the SECOND nonce — nonce match must beat FIFO.
        let echo = json!({ "output": "…\"attachmentId\": \"tar-bbb\"…" });
        let claimed = reg
            .claim_at_tool_result(&a, Some(&echo), "some_other_tool")
            .expect("nonce claim");
        assert_eq!(claimed.id, "tar-bbb");
        // First entry still pending.
        let rest = reg
            .claim_at_tool_result(&a, Some(&json!("tar-aaa")), "x")
            .expect("second claim");
        assert_eq!(rest.id, "tar-aaa");
    }

    #[test]
    fn claim_falls_back_to_fifo_only_for_workspace_api() {
        let reg = TurnAttachmentRegistry::new();
        let a = agent();
        reg.register(&a, attachment("tar-aaa", AttachmentPolicy::AtToolResult));
        // Garbled echo (no nonce) + foreign tool name → no claim.
        let garbled = json!({ "output": "garbage" });
        assert!(reg
            .claim_at_tool_result(&a, Some(&garbled), "str_replace")
            .is_none());
        // Same echo but the daemon's own tool (possibly prefixed) → FIFO claim.
        let claimed = reg
            .claim_at_tool_result(&a, Some(&garbled), "workspace-mcp_workspace_api")
            .expect("fifo claim");
        assert_eq!(claimed.id, "tar-aaa");
        assert!(reg
            .claim_at_tool_result(&a, Some(&garbled), "workspace_api")
            .is_none());
    }

    #[test]
    fn claim_ignores_turn_end_entries_and_other_agents() {
        let reg = TurnAttachmentRegistry::new();
        let a = agent();
        reg.register(&a, attachment("tar-end", AttachmentPolicy::AtTurnEnd));
        assert!(reg
            .claim_at_tool_result(&a, None, "workspace_api")
            .is_none());
        assert!(reg
            .claim_at_tool_result(&AgentId::from_string("agent-other"), None, "workspace_api")
            .is_none());
    }

    #[test]
    fn finish_turn_returns_turn_end_and_drops_leftovers() {
        let reg = TurnAttachmentRegistry::new();
        let a = agent();
        reg.register(&a, attachment("tar-r1", AttachmentPolicy::AtToolResult));
        reg.register(&a, attachment("tar-e1", AttachmentPolicy::AtTurnEnd));
        reg.register(&a, attachment("tar-e2", AttachmentPolicy::AtTurnEnd));
        let drained = reg.finish_turn(&a);
        assert_eq!(
            drained.iter().map(|t| t.id.as_str()).collect::<Vec<_>>(),
            vec!["tar-e1", "tar-e2"]
        );
        // Everything (including the unclaimed AtToolResult) is gone.
        assert!(reg
            .claim_at_tool_result(&a, None, "workspace_api")
            .is_none());
        assert!(reg.finish_turn(&a).is_empty());
    }

    #[test]
    fn register_enforces_per_agent_cap() {
        let reg = TurnAttachmentRegistry::new();
        let a = agent();
        for i in 0..(MAX_PER_AGENT + 4) {
            reg.register(
                &a,
                attachment(&format!("tar-{i:03}"), AttachmentPolicy::AtToolResult),
            );
        }
        // Oldest were dropped: FIFO claim yields the first surviving entry.
        let claimed = reg
            .claim_at_tool_result(&a, None, "workspace_api")
            .expect("claim");
        assert_eq!(claimed.id, "tar-004");
    }

    #[test]
    fn resource_item_shape_matches_protocol() {
        let item = attachment("tar-x", AttachmentPolicy::AtToolResult).resource_item();
        assert_eq!(item["type"], "resource");
        assert_eq!(
            item["resource"]["mimeType"],
            "application/vnd.intent.proposal+json"
        );
        assert_eq!(item["resource"]["uri"], "intent-proposal://test/tar-x");
        assert!(item["resource"]["text"].is_string());
    }
}
