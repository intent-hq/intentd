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
use std::sync::atomic::{AtomicU64, Ordering};
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
#[must_use]
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
    #[must_use]
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
    /// Registration-batch id: all attachments registered by ONE tool
    /// invocation share a batch, so a claim attaches them together (a nonce
    /// match on any member claims the whole batch).
    batch: u64,
    registered_at: Instant,
}

/// The message range a `ws.chat.unread` read covered — the same-turn
/// summarize gate's arm state (`ws.chat.summarizeUnread` may only run over a
/// range read earlier in the SAME turn).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SummarizeGate {
    /// First unread message id the digest covered (`None` for an empty tail
    /// boundary — never armed in practice, the binding skips empty digests).
    pub from_message_id: Option<String>,
    /// Last unread message id the digest covered.
    pub to_message_id: String,
}

/// Daemon-wide registry of pending turn attachments, keyed by agent. Shared
/// (via `Arc`) between the per-agent MCP dispatch (registration side) and the
/// transcript writer in `intent-services` (claim/drain side). Also carries
/// the per-turn `ws.chat.unread` summarize-gate arm state ([`SummarizeGate`])
/// — same lifecycle as attachments: registered mid-dispatch, cleared when
/// the turn finishes.
#[derive(Default)]
pub struct TurnAttachmentRegistry {
    inner: Mutex<HashMap<AgentId, Vec<Entry>>>,
    batch_seq: AtomicU64,
    summarize_gates: Mutex<HashMap<AgentId, SummarizeGate>>,
}

impl TurnAttachmentRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register one pending attachment for `agent_id` (a single-item batch).
    pub fn register(&self, agent_id: &AgentId, attachment: TurnAttachment) {
        self.register_all(agent_id, vec![attachment]);
    }

    /// Register the attachments produced by ONE tool invocation as a single
    /// batch — a later claim attaches all of them together. Evicts expired
    /// entries and enforces the per-agent cap (oldest dropped first).
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned (a prior panic while holding the lock).
    pub fn register_all(&self, agent_id: &AgentId, attachments: Vec<TurnAttachment>) {
        if attachments.is_empty() {
            return;
        }
        let batch = self.batch_seq.fetch_add(1, Ordering::Relaxed);
        let mut inner = self.inner.lock().unwrap();
        let entries = inner.entry(agent_id.clone()).or_default();
        evict_expired(entries);
        for attachment in attachments {
            if entries.len() >= MAX_PER_AGENT {
                entries.remove(0);
            }
            entries.push(Entry {
                attachment,
                batch,
                registered_at: Instant::now(),
            });
        }
    }

    /// Claim the `AtToolResult` attachments for a completed tool call — the
    /// full registration batch, in registration order.
    ///
    /// Precise path: the serialized `echoed_output` contains a batch member's
    /// nonce (the dispatch layer stamped it into the model-facing output, so
    /// any non-garbled echo carries it). Fallback path: when no nonce matches
    /// and `tool_name` is the daemon's own `workspace_api` tool, the oldest
    /// batch with an `AtToolResult` entry is claimed FIFO — a garbled echo
    /// cannot defeat the attach, and only the tool that registers through
    /// this registry can trigger the blind claim. Empty when nothing is
    /// pending (the caller falls back to echo parsing).
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned (a prior panic while holding the lock).
    pub fn claim_at_tool_result(
        &self,
        agent_id: &AgentId,
        echoed_output: Option<&Value>,
        tool_name: &str,
    ) -> Vec<TurnAttachment> {
        let mut inner = self.inner.lock().unwrap();
        let Some(entries) = inner.get_mut(agent_id) else {
            return Vec::new();
        };
        evict_expired(entries);
        let echo = echoed_output.map(Value::to_string).unwrap_or_default();
        let is_claimable = |e: &Entry| e.attachment.policy == AttachmentPolicy::AtToolResult;
        let by_nonce = entries
            .iter()
            .find(|e| is_claimable(e) && !echo.is_empty() && echo.contains(&e.attachment.id))
            .map(|e| e.batch);
        let batch = by_nonce.or_else(|| {
            tool_name
                .contains("workspace_api")
                .then(|| entries.iter().find(|e| is_claimable(e)).map(|e| e.batch))?
        });
        let Some(batch) = batch else {
            return Vec::new();
        };
        let mut claimed = Vec::new();
        entries.retain(|e| {
            if e.batch == batch && is_claimable(e) {
                claimed.push(e.attachment.clone());
                false
            } else {
                true
            }
        });
        claimed
    }

    /// Count `agent_id`'s pending attachments carrying `mime_type` (expired
    /// entries evicted first). Read-only introspection — backs the
    /// `numQuestionsAsked` field of the agent state snapshot, which counts
    /// questions registered earlier in the same turn that are still waiting
    /// for the turn-end drain.
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned (a prior panic while holding the lock).
    pub fn pending_count_by_mime(&self, agent_id: &AgentId, mime_type: &str) -> usize {
        let mut inner = self.inner.lock().unwrap();
        let Some(entries) = inner.get_mut(agent_id) else {
            return 0;
        };
        evict_expired(entries);
        entries
            .iter()
            .filter(|e| e.attachment.mime_type == mime_type)
            .count()
    }

    /// Finish `agent_id`'s turn: return the pending `AtTurnEnd` attachments
    /// (in registration order) and clear ALL remaining entries — unclaimed
    /// `AtToolResult` leftovers are dropped so they cannot attach to a later
    /// turn.
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned (a prior panic while holding the lock).
    pub fn finish_turn(&self, agent_id: &AgentId) -> Vec<TurnAttachment> {
        self.summarize_gates.lock().unwrap().remove(agent_id);
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

    /// Arm `agent_id`'s same-turn summarize gate over the message range a
    /// `ws.chat.unread` read just covered. A repeat read within the same turn
    /// re-arms with the newer range. Cleared by [`Self::finish_turn`].
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned (a prior panic while holding the lock).
    pub fn arm_summarize_gate(&self, agent_id: &AgentId, from: Option<&str>, to: &str) {
        self.summarize_gates.lock().unwrap().insert(
            agent_id.clone(),
            SummarizeGate {
                from_message_id: from.map(str::to_string),
                to_message_id: to.to_string(),
            },
        );
    }

    /// The caller's current summarize-gate arm state, if a `ws.chat.unread`
    /// read armed it earlier in the same turn.
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned (a prior panic while holding the lock).
    #[must_use]
    pub fn summarize_gate(&self, agent_id: &AgentId) -> Option<SummarizeGate> {
        self.summarize_gates.lock().unwrap().get(agent_id).cloned()
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

    fn ids(claimed: &[TurnAttachment]) -> Vec<&str> {
        claimed.iter().map(|t| t.id.as_str()).collect()
    }

    #[test]
    fn claim_matches_nonce_in_echoed_output() {
        let reg = TurnAttachmentRegistry::new();
        let a = agent();
        reg.register(&a, attachment("tar-aaa", AttachmentPolicy::AtToolResult));
        reg.register(&a, attachment("tar-bbb", AttachmentPolicy::AtToolResult));
        // The echo carries the SECOND nonce — nonce match must beat FIFO.
        let echo = json!({ "output": "…\"attachmentId\": \"tar-bbb\"…" });
        let claimed = reg.claim_at_tool_result(&a, Some(&echo), "some_other_tool");
        assert_eq!(ids(&claimed), vec!["tar-bbb"]);
        // First entry still pending.
        let rest = reg.claim_at_tool_result(&a, Some(&json!("tar-aaa")), "x");
        assert_eq!(ids(&rest), vec!["tar-aaa"]);
    }

    #[test]
    fn claim_attaches_full_registration_batch() {
        let reg = TurnAttachmentRegistry::new();
        let a = agent();
        // One tool invocation registered TWO resources (a batch); a second
        // invocation registered another.
        reg.register_all(
            &a,
            vec![
                attachment("tar-b1a", AttachmentPolicy::AtToolResult),
                attachment("tar-b1b", AttachmentPolicy::AtToolResult),
            ],
        );
        reg.register(&a, attachment("tar-b2", AttachmentPolicy::AtToolResult));
        // A nonce match on ANY batch member claims the whole batch, in order.
        let echo = json!({ "output": "…tar-b1b…" });
        let claimed = reg.claim_at_tool_result(&a, Some(&echo), "x");
        assert_eq!(ids(&claimed), vec!["tar-b1a", "tar-b1b"]);
        // The other batch is untouched.
        let rest = reg.claim_at_tool_result(&a, None, "workspace_api");
        assert_eq!(ids(&rest), vec!["tar-b2"]);
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
            .is_empty());
        // Same echo but the daemon's own tool (possibly prefixed) → FIFO claim.
        let claimed = reg.claim_at_tool_result(&a, Some(&garbled), "workspace-mcp_workspace_api");
        assert_eq!(ids(&claimed), vec!["tar-aaa"]);
        assert!(reg
            .claim_at_tool_result(&a, Some(&garbled), "workspace_api")
            .is_empty());
    }

    #[test]
    fn claim_ignores_turn_end_entries_and_other_agents() {
        let reg = TurnAttachmentRegistry::new();
        let a = agent();
        reg.register(&a, attachment("tar-end", AttachmentPolicy::AtTurnEnd));
        assert!(reg
            .claim_at_tool_result(&a, None, "workspace_api")
            .is_empty());
        assert!(reg
            .claim_at_tool_result(&AgentId::from_string("agent-other"), None, "workspace_api")
            .is_empty());
    }

    #[test]
    fn finish_turn_returns_turn_end_and_drops_leftovers() {
        let reg = TurnAttachmentRegistry::new();
        let a = agent();
        reg.register(&a, attachment("tar-r1", AttachmentPolicy::AtToolResult));
        reg.register(&a, attachment("tar-e1", AttachmentPolicy::AtTurnEnd));
        reg.register(&a, attachment("tar-e2", AttachmentPolicy::AtTurnEnd));
        let drained = reg.finish_turn(&a);
        assert_eq!(ids(&drained), vec!["tar-e1", "tar-e2"]);
        // Everything (including the unclaimed AtToolResult) is gone.
        assert!(reg
            .claim_at_tool_result(&a, None, "workspace_api")
            .is_empty());
        assert!(reg.finish_turn(&a).is_empty());
    }

    #[test]
    fn summarize_gate_arms_rearms_and_clears_at_turn_end() {
        let reg = TurnAttachmentRegistry::new();
        let a = agent();
        assert_eq!(reg.summarize_gate(&a), None);
        reg.arm_summarize_gate(&a, Some("msg-1"), "msg-9");
        assert_eq!(
            reg.summarize_gate(&a),
            Some(SummarizeGate {
                from_message_id: Some("msg-1".into()),
                to_message_id: "msg-9".into(),
            })
        );
        // A repeat read re-arms with the newer range; other agents are
        // unaffected.
        reg.arm_summarize_gate(&a, None, "msg-12");
        assert_eq!(
            reg.summarize_gate(&a),
            Some(SummarizeGate {
                from_message_id: None,
                to_message_id: "msg-12".into(),
            })
        );
        assert_eq!(
            reg.summarize_gate(&AgentId::from_string("agent-other")),
            None
        );
        // Turn end clears the gate.
        reg.finish_turn(&a);
        assert_eq!(reg.summarize_gate(&a), None);
    }

    #[test]
    fn pending_count_by_mime_filters_and_scopes() {
        let reg = TurnAttachmentRegistry::new();
        let a = agent();
        let mime = "application/vnd.intent.proposal+json";
        assert_eq!(reg.pending_count_by_mime(&a, mime), 0);
        reg.register(&a, attachment("tar-c1", AttachmentPolicy::AtTurnEnd));
        reg.register(&a, attachment("tar-c2", AttachmentPolicy::AtToolResult));
        assert_eq!(reg.pending_count_by_mime(&a, mime), 2);
        assert_eq!(reg.pending_count_by_mime(&a, "text/plain"), 0);
        assert_eq!(
            reg.pending_count_by_mime(&AgentId::from_string("agent-other"), mime),
            0
        );
        // Counting never consumes entries.
        assert_eq!(reg.pending_count_by_mime(&a, mime), 2);
        reg.finish_turn(&a);
        assert_eq!(reg.pending_count_by_mime(&a, mime), 0);
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
        let claimed = reg.claim_at_tool_result(&a, None, "workspace_api");
        assert_eq!(ids(&claimed), vec!["tar-004"]);
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
