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

/// The `workspace_api` MCP tool's input schema: a string `code` (the JS to
/// run) plus a string `summary` (the model-authored one-line description),
/// and nothing else. Both keys are required by the schema and no daemon tool
/// carries that pair, so the exact shape identifies the tool on its own; any
/// extra key (other than a daemon-stamped `_acpTitle` echo) means some other
/// tool's arguments and disqualifies the match. Some ACP providers (auggie)
/// title a `workspace_api` call with its `summary` and carry no tool
/// identifier anywhere in the frame (intent-hq/intent#4491), so the input
/// shape is the one signal shared by every provider. Used by both the name
/// derivation in `intent-acp` and the FIFO claim gate in
/// [`TurnAttachmentRegistry::claim_at_tool_result`] so the two agree.
#[must_use]
pub fn is_workspace_api_input(input: &Value) -> bool {
    let Some(obj) = input.as_object() else {
        return false;
    };
    obj.get("code")
        .and_then(Value::as_str)
        .is_some_and(|s| !s.is_empty())
        && obj.get("summary").is_some_and(Value::is_string)
        && obj
            .keys()
            .all(|k| matches!(k.as_str(), "code" | "summary" | "_acpTitle"))
}

/// The auggie frame signature (intent-hq/intent#4491): a recorded tool input
/// with the [`workspace_api` schema](is_workspace_api_input) whose echoed ACP
/// title (`_acpTitle`, stamped by the transcript writer) is the call's own
/// `summary` — the provider titled the call with the model-authored summary,
/// so no tool identifier exists anywhere in the frame. A foreign tool that
/// merely shares the argument shape is titled by its own name (codex
/// `server`/`tool` metadata, `mcp.<server>.<tool>`, `mcp__<server>__<tool>`)
/// and never matches, which keeps the claim gate consistent with the
/// mapper's authoritative-name ordering.
fn is_summary_titled_workspace_api_input(input: &Value) -> bool {
    is_workspace_api_input(input)
        && input
            .get("_acpTitle")
            .is_some_and(|title| Some(title) == input.get("summary"))
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

/// Daemon-wide registry of pending turn attachments, keyed by agent. Shared
/// (via `Arc`) between the per-agent MCP dispatch (registration side) and the
/// transcript writer in `intent-services` (claim/drain side).
#[derive(Default)]
pub struct TurnAttachmentRegistry {
    inner: Mutex<HashMap<AgentId, Vec<Entry>>>,
    batch_seq: AtomicU64,
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
    /// and the call is the daemon's own `workspace_api` tool — EITHER
    /// `tool_name` contains `workspace_api` OR `tool_input` (the recorded
    /// `tool_use` input, `_acpTitle` included) carries the auggie frame
    /// signature: the `workspace_api` schema ([`is_workspace_api_input`])
    /// titled by its own `summary`, so the frame holds no tool identifier
    /// and the recorded name is whatever the mapper made of the prose
    /// (intent-hq/intent#4491) — the oldest batch with an `AtToolResult`
    /// entry is claimed FIFO — a garbled echo cannot defeat the attach, and
    /// only the tool that registers through this registry can trigger the
    /// blind claim. The input gate is independent of the mapper's name
    /// derivation (a second line of defense should a title rule ever
    /// pre-empt the shape rule again) but defers to it on authority: a
    /// foreign tool sharing the argument shape is titled by its own name and
    /// never opens the gate. Empty when nothing is pending (the caller falls
    /// back to echo parsing).
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned (a prior panic while holding the lock).
    pub fn claim_at_tool_result(
        &self,
        agent_id: &AgentId,
        echoed_output: Option<&Value>,
        tool_name: &str,
        tool_input: Option<&Value>,
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
        let is_workspace_api = tool_name.contains("workspace_api")
            || tool_input.is_some_and(is_summary_titled_workspace_api_input);
        let batch = by_nonce.or_else(|| {
            is_workspace_api.then(|| entries.iter().find(|e| is_claimable(e)).map(|e| e.batch))?
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
        let claimed = reg.claim_at_tool_result(&a, Some(&echo), "some_other_tool", None);
        assert_eq!(ids(&claimed), vec!["tar-bbb"]);
        // First entry still pending.
        let rest = reg.claim_at_tool_result(&a, Some(&json!("tar-aaa")), "x", None);
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
        let claimed = reg.claim_at_tool_result(&a, Some(&echo), "x", None);
        assert_eq!(ids(&claimed), vec!["tar-b1a", "tar-b1b"]);
        // The other batch is untouched.
        let rest = reg.claim_at_tool_result(&a, None, "workspace_api", None);
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
            .claim_at_tool_result(&a, Some(&garbled), "str_replace", None)
            .is_empty());
        // Same echo but the daemon's own tool (possibly prefixed) → FIFO claim.
        let claimed =
            reg.claim_at_tool_result(&a, Some(&garbled), "workspace-mcp_workspace_api", None);
        assert_eq!(ids(&claimed), vec!["tar-aaa"]);
        assert!(reg
            .claim_at_tool_result(&a, Some(&garbled), "workspace_api", None)
            .is_empty());
    }

    /// intent-hq/intent#4491: a provider that titles the call with its
    /// `summary` records no usable name, so the auggie frame signature —
    /// `workspace_api` schema + `_acpTitle == summary` — is the second FIFO
    /// gate, independent of whatever name the mapper recorded. `{code}`
    /// alone, a non-string `summary`, no input, or a shaped input titled by
    /// something other than its summary does not open it; the name gate
    /// still works with no input.
    #[test]
    fn claim_falls_back_to_fifo_on_summary_titled_workspace_api_input() {
        let reg = TurnAttachmentRegistry::new();
        let a = agent();
        reg.register(&a, attachment("tar-aaa", AttachmentPolicy::AtToolResult));
        let garbled = json!({ "output": "garbage" });
        let prose = "Propose a follow-up workspace";
        assert!(reg
            .claim_at_tool_result(&a, Some(&garbled), prose, None)
            .is_empty());
        for shaped_wrong in [
            json!({ "code": "ws.workspace.proposeSibling(p)", "_acpTitle": prose }),
            json!({ "code": "x", "summary": 42, "_acpTitle": prose }),
            json!({ "code": "x", "summary": prose }),
            json!({ "code": "x", "summary": prose, "_acpTitle": "Something else" }),
            json!({ "code": "x", "summary": prose, "_acpTitle": prose, "language": "py" }),
        ] {
            assert!(
                reg.claim_at_tool_result(&a, Some(&garbled), prose, Some(&shaped_wrong))
                    .is_empty(),
                "input={shaped_wrong}"
            );
        }
        // The mapper split `"Inspect: tool calls"` into a bogus name — the
        // gate does not care what the recorded name is.
        let shaped = json!({
            "code": "ws.workspace.proposeSibling(p)",
            "summary": prose,
            "_acpTitle": prose,
        });
        let claimed = reg.claim_at_tool_result(&a, Some(&garbled), "Inspect", Some(&shaped));
        assert_eq!(ids(&claimed), vec!["tar-aaa"]);
        assert!(reg
            .claim_at_tool_result(&a, Some(&garbled), prose, Some(&shaped))
            .is_empty());
        // Name gate alone still claims with no input.
        reg.register(&a, attachment("tar-bbb", AttachmentPolicy::AtToolResult));
        let claimed = reg.claim_at_tool_result(&a, None, "workspace_api", None);
        assert_eq!(ids(&claimed), vec!["tar-bbb"]);
    }

    /// Authoritative names win: a foreign tool whose arguments happen to be
    /// `{ code, summary }` is titled by its own name (`_acpTitle` is the
    /// codex / claude namespaced title, not the summary), so the input gate
    /// stays shut and the batch waits for the daemon's own tool.
    #[test]
    fn foreign_tool_with_workspace_api_shaped_arguments_does_not_claim() {
        let reg = TurnAttachmentRegistry::new();
        let a = agent();
        reg.register(&a, attachment("tar-aaa", AttachmentPolicy::AtToolResult));
        let garbled = json!({ "output": "ok" });
        for title in ["mcp.python.execute", "mcp__python__execute", "execute"] {
            let input = json!({ "code": "print(1)", "summary": "Run Python", "_acpTitle": title });
            assert!(
                reg.claim_at_tool_result(&a, Some(&garbled), "python_execute", Some(&input))
                    .is_empty(),
                "title={title}"
            );
        }
        let claimed = reg.claim_at_tool_result(&a, Some(&garbled), "workspace_api", None);
        assert_eq!(ids(&claimed), vec!["tar-aaa"]);
    }

    #[test]
    fn is_workspace_api_input_requires_exact_schema() {
        assert!(is_workspace_api_input(
            &json!({ "code": "return 1", "summary": "One" })
        ));
        assert!(is_workspace_api_input(
            &json!({ "code": "return 1", "summary": "One", "_acpTitle": "One" })
        ));
        assert!(!is_workspace_api_input(&json!({ "code": "return 1" })));
        assert!(!is_workspace_api_input(&json!({ "summary": "One" })));
        assert!(!is_workspace_api_input(
            &json!({ "code": null, "summary": "One" })
        ));
        assert!(!is_workspace_api_input(
            &json!({ "code": "", "summary": "One" })
        ));
        assert!(!is_workspace_api_input(
            &json!({ "code": ["x"], "summary": "One" })
        ));
        assert!(!is_workspace_api_input(
            &json!({ "code": "x", "summary": 42 })
        ));
        assert!(!is_workspace_api_input(
            &json!({ "code": "print(1)", "summary": "Run", "language": "python" })
        ));
        assert!(!is_workspace_api_input(&json!("code summary")));
        assert!(!is_workspace_api_input(&Value::Null));
    }

    #[test]
    fn claim_ignores_turn_end_entries_and_other_agents() {
        let reg = TurnAttachmentRegistry::new();
        let a = agent();
        reg.register(&a, attachment("tar-end", AttachmentPolicy::AtTurnEnd));
        assert!(reg
            .claim_at_tool_result(&a, None, "workspace_api", None)
            .is_empty());
        assert!(reg
            .claim_at_tool_result(
                &AgentId::from_string("agent-other"),
                None,
                "workspace_api",
                None
            )
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
            .claim_at_tool_result(&a, None, "workspace_api", None)
            .is_empty());
        assert!(reg.finish_turn(&a).is_empty());
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
        let claimed = reg.claim_at_tool_result(&a, None, "workspace_api", None);
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
