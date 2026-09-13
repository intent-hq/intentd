//! Egress registry + contract test for
//! [`intent_core::model::AGENT_HIDDEN_FIELDS`].
//!
//! [`EGRESS_REGISTRY`] enumerates every agent-facing egress that carries
//! session / event data — one `(name, producer)` entry per surface — and
//! [`no_agent_hidden_field_survives_any_egress`] drives each producer through
//! the REAL production code path (the `workspace_api` MCP tool → JS engine →
//! `bindings::try_dispatch`, or the services wake-metadata builder) against a
//! stub [`WorkspaceApi`] whose every session, list row, queue entry,
//! conversation `messageMetadata`, and event `data` is poisoned with every
//! hidden key. The assertion walks the output recursively and fails naming
//! the egress, the JSON path, and the surviving key.
//!
//! The fixture is key-agnostic: the injected keys are read from
//! `AGENT_HIDDEN_FIELDS` at runtime, so adding a key to the const needs no
//! edit here. Adding a NEW agent-facing egress that serves session or event
//! data DOES: append a registry entry (and stub whatever trait method it
//! reads, poisoned via [`poison`] / [`poison_typed`]).

#![cfg(test)]

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use intent_core::model::AGENT_HIDDEN_FIELDS;
use intent_core::{
    ActorType, AgentActivity, AgentId, AgentLite, AgentMetadata, AgentStatus, BoxFuture, Error,
    Event, EventActor, EventQueryParams, Result, Workspace, WorkspaceActivity, WorkspaceApi,
    WorkspaceAttention, WorkspaceEventSummary, WorkspaceId, WorkspaceStatus,
};
use serde_json::{json, Map, Value};

use crate::WorkspaceMcpServer;

/// Marker value injected under every hidden key; the assertion looks for the
/// KEY, so the value only needs to survive serialization.
const SENTINEL: &str = "AGENT_HIDDEN_FIELD_SENTINEL";

/// Insert every `AGENT_HIDDEN_FIELDS` key (sentinel-valued) into the
/// top-level object of `value`. No-op on non-objects.
fn poison(value: &mut Value) {
    if let Some(obj) = value.as_object_mut() {
        poison_map(obj);
    }
}

fn poison_map(obj: &mut Map<String, Value>) {
    for key in AGENT_HIDDEN_FIELDS {
        obj.insert((*key).to_string(), json!(SENTINEL));
    }
}

/// Candidate sentinel values for a hidden key on a TYPED struct, tried in
/// order until the round trip type-checks: `bool`, `String`, number, array,
/// object. All are non-null so an `Option<_>` field is populated (a `None`
/// would be skipped on serialization and never reach the egress).
fn typed_sentinel_candidates() -> [Value; 5] {
    [
        json!(true),
        json!(SENTINEL),
        json!(1),
        json!([SENTINEL]),
        json!({ "sentinel": SENTINEL }),
    ]
}

/// Poison a typed wire struct: serialize, inject every hidden key, and
/// deserialize back. For each key the first candidate from
/// [`typed_sentinel_candidates`] that deserializes is kept, so a hidden key
/// that is a real field of any common type lands populated without a fixture
/// edit. When no candidate fits (an enum or structured required field) the
/// struct's own already-serialized non-null value is kept — the key is
/// present either way, which is all the egress assertion needs. `serde`
/// ignores keys the struct does not carry (the first candidate then trivially
/// succeeds), which is the correct outcome for a hidden key with no field on
/// this type: it cannot reach the egress through it.
fn poison_typed<T: serde::Serialize + serde::de::DeserializeOwned>(value: T) -> T {
    let type_name = std::any::type_name::<T>();
    let mut v = serde_json::to_value(value).expect("fixture serializes");
    if v.is_object() {
        for key in AGENT_HIDDEN_FIELDS {
            let accepted = typed_sentinel_candidates().into_iter().find(|candidate| {
                let mut probe = v.clone();
                probe[*key] = candidate.clone();
                serde_json::from_value::<T>(probe).is_ok()
            });
            match accepted {
                Some(candidate) => v[*key] = candidate,
                None if v.get(*key).is_some_and(|orig| !orig.is_null()) => {}
                None => panic!(
                    "poison_typed: {type_name} field `{key}` accepts none of the sentinel candidates and the fixture leaves it unset — populate it in the fixture or extend `typed_sentinel_candidates`"
                ),
            }
        }
    }
    serde_json::from_value(v)
        .unwrap_or_else(|e| panic!("poison_typed: {type_name} round trip failed: {e}"))
}

/// Recursively find the first surviving hidden key; returns `(json_path, key)`.
fn find_hidden_key(value: &Value, path: &str) -> Option<(String, String)> {
    match value {
        Value::Object(obj) => {
            for key in AGENT_HIDDEN_FIELDS {
                if obj.contains_key(*key) {
                    return Some((path.to_string(), (*key).to_string()));
                }
            }
            obj.iter()
                .find_map(|(k, v)| find_hidden_key(v, &format!("{path}.{k}")))
        }
        Value::Array(items) => items
            .iter()
            .enumerate()
            .find_map(|(i, v)| find_hidden_key(v, &format!("{path}[{i}]"))),
        _ => None,
    }
}

// ================================================================
// Stub WorkspaceApi — every served payload is poisoned.
// ================================================================

#[derive(Default)]
struct FakeApi {
    /// `event_query` calls, so the test can prove the fixture rows were the
    /// ones the egress read (not an empty default).
    event_query_calls: Mutex<u32>,
}

const WS: &str = "amber-forest";
const CALLER: &str = "a-1";
const TARGET: &str = "a-2";

fn stub_agent(id: &str, ws: &WorkspaceId) -> AgentLite {
    poison_typed(AgentLite {
        harness_version: intent_core::CURRENT_HARNESS_VERSION.to_string(),
        harness_features: None,
        id: AgentId::from(id),
        workspace_id: ws.clone(),
        parent_agent_id: None,
        backend_session_id: None,
        acp_session_id: None,
        name: format!("agent-{id}"),
        name_explicitly_set: false,
        model: None,
        reasoning_effort: None,
        effort_levels: None,
        provider: None,
        status: AgentStatus::Idle,
        is_active: false,
        is_streaming: false,
        is_processing: false,
        is_responding: false,
        is_waiting_on_tool: false,
        is_waiting_for_other_agents: false,
        waiting_for_agent_ids: vec![],
        waiting_on_hooks: vec![],
        waiting_on_pr_monitors: vec![],
        turn_in_flight: false,
        last_stream_activity_at: None,
        context_usage: None,
        stats: None,
        created_at: "2026-01-01T00:00:00Z".to_string(),
        updated_at: "2026-01-01T00:00:00Z".to_string(),
        last_activity: Some("2026-01-01T00:00:00Z".to_string()),
        message_count: 3,
        digest: None,
        last_agent_response: None,
        last_user_message: None,
        last_message_role: None,
        last_message_id: None,
        last_tool_use: None,
        context_references: None,
        file_blocks: None,
        stop_reason: None,
        stop_reason_timestamp: None,
        session_corrupted: false,
        pending_delete_at: None,
        retired_at: None,
        notifications_muted: false,
        metadata: AgentMetadata {
            is_background: false,
            specialist: None,
            created_by_agent_id: None,
            task_note_id: None,
            completion_report: None,
            completion_report_timestamp: None,
            attention_request_kind: None,
            attention_request_reason: None,
            attention_request_timestamp: None,
            delegation_depth: None,
            sandbox_id: None,
            sandbox_branch: None,
            sandbox_path: None,
            dismissed_questions_message_id: None,
            pending_questions_message_id: None,
            pending_proposals: Vec::new(),
            proposal_resolutions: Map::new(),
            last_seen_message_id: None,
            is_initial_agent: None,
            sponsor_agent_id: None,
        },
    })
}

fn stub_workspace(id: &str) -> Workspace {
    Workspace {
        id: WorkspaceId::from_string(id),
        title: format!("Workspace {id}"),
        branch: "main".to_string(),
        base_ref: None,
        base_commit_sha: None,
        status: WorkspaceStatus::Active,
        status_message: None,
        status_image_asset_id: None,
        activity: WorkspaceActivity::Idle,
        attention: WorkspaceAttention::None,
        created_at: "2026-01-01T00:00:00Z".to_string(),
        updated_at: "2026-01-01T00:00:00Z".to_string(),
        last_activity: None,
        tags: vec![],
        path: None,
        repository_path: None,
        repository_owner: None,
        repository_name: None,
        worktree_path: None,
        scope: None,
        skip_worktree: false,
        setup_script: None,
        is_remote: false,
        default_model: None,
        pr_number: None,
        pr_url: None,
        pr_status: None,
        active_pull_request: None,
        pull_requests: None,
        pull_requests_total: None,
        context_links: None,
        archived: false,
        archived_at: None,
        task_stats: None,
        agent_summary: None,
        diff_summary: None,
        token_usage: None,
        cow_supported: None,
        browser_client_id: None,
        display_status: None,
        waiting: false,
        checkout_mode: None,
        disk_usage: None,
        pending_delete_at: None,
        membership: None,
    }
}

/// The agent-lifecycle event types whose persisted `data` carries the hidden
/// fields in production.
const AGENT_EVENT_TYPES: &[&str] = &[
    intent_core::events::AGENT_UPDATED,
    intent_core::events::AGENT_IDLE,
    intent_core::events::AGENT_ATTENTION_REQUESTED,
];

/// One poisoned `agent:*` event row per type (the `event.query` wire shape),
/// plus a non-agent row so the scrub is proven to leave other rows intact.
fn event_rows() -> Vec<Value> {
    let mut rows: Vec<Value> = AGENT_EVENT_TYPES
        .iter()
        .enumerate()
        .map(|(i, ty)| {
            let mut data = json!({ "agentId": TARGET, "status": "idle" });
            poison(&mut data);
            json!({
                "id": format!("evt-{i}"),
                "type": ty,
                "eventType": ty,
                "timestamp": "2026-01-01T00:00:00Z",
                "actor": { "type": "agent", "id": TARGET },
                "actorId": TARGET,
                "data": data,
            })
        })
        .collect();
    rows.push(json!({
        "id": "evt-file",
        "type": "file:changed",
        "eventType": "file:changed",
        "actorId": TARGET,
        "data": { "path": "src/a.rs" },
    }));
    rows
}

/// The same rows as typed [`Event`]s — the input shape of the services
/// wake-metadata builder.
fn typed_events() -> Vec<Event> {
    AGENT_EVENT_TYPES
        .iter()
        .enumerate()
        .map(|(i, ty)| {
            let mut data = json!({ "agentId": TARGET, "status": "idle", "stallSuspected": true });
            poison(&mut data);
            Event {
                id: format!("evt-{i}"),
                workspace_id: WorkspaceId::from_string(WS),
                timestamp: "2026-01-01T00:00:00Z".to_string(),
                event_type: (*ty).to_string(),
                actor: EventActor {
                    actor_type: ActorType::Agent,
                    id: Some(TARGET.to_string()),
                    name: Some("Target".to_string()),
                    email: None,
                    model: None,
                    metadata: None,
                },
                session_id: None,
                correlation_id: None,
                parent_event_id: None,
                metadata: None,
                data,
            }
        })
        .collect()
}

/// Conversation messages whose `messageMetadata` (and the row itself) are
/// poisoned — what a persisted wake or agent message would look like if the
/// write-side scrub were ever missed.
fn conversation_messages() -> Vec<Value> {
    (0..3)
        .map(|i| {
            let mut metadata = json!({ "type": "event_notification", "eventCount": 1 });
            poison(&mut metadata);
            let mut msg = json!({
                "id": format!("msg-{i}"),
                "role": if i % 2 == 0 { "user" } else { "assistant" },
                "contentBlocks": [{ "type": "text", "text": format!("Message {i}") }],
                "messageMetadata": metadata,
            });
            poison(&mut msg);
            msg
        })
        .collect()
}

fn queue_entries() -> Vec<Value> {
    (0..2)
        .map(|i| {
            let mut metadata = json!({ "fromAgentId": CALLER, "fromAgentName": "Caller" });
            poison(&mut metadata);
            let mut entry = json!({
                "id": format!("q-{i}"),
                "content": format!("queued {i}"),
                "queuedAt": "2026-01-01T00:00:00Z",
                "position": i,
                "messageMetadata": metadata,
            });
            poison(&mut entry);
            entry
        })
        .collect()
}

impl WorkspaceApi for FakeApi {
    // Pin the `workspaceApi.*` output knobs to plain JSON / no limit so the
    // body can be parsed back and walked.
    fn settings_get(&self, path: String) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move {
            let value = match path.as_str() {
                "workspaceApi.toonOutput" => json!(false),
                "workspaceApi.maxOutputChars" => json!(0),
                _ => Value::Null,
            };
            Ok(json!({ "path": path, "value": value }))
        })
    }

    fn list_workspaces(&self, _include_archived: bool) -> BoxFuture<'_, Result<Vec<Workspace>>> {
        Box::pin(async move { Ok(vec![stub_workspace(WS)]) })
    }

    fn get_workspace(&self, id: WorkspaceId) -> BoxFuture<'_, Result<Workspace>> {
        Box::pin(async move {
            if id.as_str() == WS {
                Ok(stub_workspace(WS))
            } else {
                Err(Error::NotFound(format!(
                    "Workspace not found: {}",
                    id.as_str()
                )))
            }
        })
    }

    fn agent_list(&self, ws: WorkspaceId) -> BoxFuture<'_, Result<Vec<AgentLite>>> {
        Box::pin(async move { Ok(vec![stub_agent(CALLER, &ws), stub_agent(TARGET, &ws)]) })
    }

    fn agent_get(
        &self,
        agent_id: AgentId,
        workspace_id: Option<WorkspaceId>,
    ) -> BoxFuture<'_, Result<AgentLite>> {
        let ws = workspace_id.unwrap_or_else(|| WorkspaceId::from_string(WS));
        Box::pin(async move { Ok(stub_agent(agent_id.as_str(), &ws)) })
    }

    fn agent_get_queue(
        &self,
        _agent_id: AgentId,
        _workspace_id: Option<WorkspaceId>,
    ) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move { Ok(json!({ "success": true, "queue": queue_entries() })) })
    }

    fn agent_diagnostics(
        &self,
        _workspace_id: WorkspaceId,
        _agent_id: Option<AgentId>,
        _task_note_id: Option<intent_core::NoteId>,
        _stale_responding_after_ms: Option<i64>,
    ) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move {
            let agents: Vec<Value> = [CALLER, TARGET]
                .iter()
                .map(|id| {
                    let mut row = json!({ "agentId": id, "status": "idle", "queueLength": 0 });
                    poison(&mut row);
                    row
                })
                .collect();
            let mut out = json!({
                "diagnostics": { "agents": agents, "subscriptions": [], "queues": [] },
                "text": "diagnostics",
            });
            poison(&mut out);
            Ok(out)
        })
    }

    fn agent_snapshot(
        &self,
        _workspace_id: WorkspaceId,
        _agent_id: AgentId,
    ) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move {
            let mut child = json!({ "agentId": TARGET, "status": "idle" });
            poison(&mut child);
            let mut out = json!({
                "time": "2026-01-01T00:00:00Z",
                "unsettledSubAgents": [child],
            });
            poison(&mut out);
            Ok(out)
        })
    }

    fn agent_get_conversation(
        &self,
        _agent_id: AgentId,
        _limit: Option<i64>,
        _workspace_id: Option<WorkspaceId>,
        _page_token: Option<String>,
        _around_message_id: Option<String>,
        _around_index: Option<i64>,
        _projection: Option<intent_core::ConversationProjection>,
        _include_in_progress: bool,
    ) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move {
            let messages = conversation_messages();
            Ok(json!({ "messages": messages, "totalMessages": messages.len() }))
        })
    }

    fn agent_summary(
        &self,
        _workspace_id: WorkspaceId,
        agent_id: AgentId,
    ) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move {
            let mut out = json!({
                "agentId": agent_id.as_str(),
                "summary": "did things",
                "agent": serde_json::to_value(stub_agent(agent_id.as_str(), &WorkspaceId::from_string(WS))).unwrap(),
            });
            poison(&mut out);
            Ok(out)
        })
    }

    fn event_query(
        &self,
        _workspace_id: WorkspaceId,
        params: EventQueryParams,
    ) -> BoxFuture<'_, Result<Value>> {
        *self.event_query_calls.lock().unwrap() += 1;
        Box::pin(async move {
            let rows = event_rows();
            Ok(if params.paginate == Some(true) {
                json!({ "events": rows, "nextPageToken": "tok-2" })
            } else {
                Value::Array(rows)
            })
        })
    }

    fn event_agent_activity(
        &self,
        _workspace_id: WorkspaceId,
        _agent_id: Option<String>,
        _minutes_ago: Option<i64>,
    ) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move {
            let mut out = json!({ "events": event_rows(), "agentId": TARGET });
            poison(&mut out);
            Ok(out)
        })
    }

    fn event_workspace_summary(
        &self,
        _workspace_id: WorkspaceId,
        _minutes_ago: Option<i64>,
    ) -> BoxFuture<'_, Result<WorkspaceEventSummary>> {
        Box::pin(async move {
            Ok(poison_typed(WorkspaceEventSummary {
                recent_files: vec![],
                active_agents: vec![poison_typed(AgentActivity {
                    agent_id: TARGET.to_string(),
                    agent_name: Some("Target".to_string()),
                    event_count: 3,
                    tool_calls: 1,
                    files_modified: vec![],
                    last_active: "2026-01-01T00:00:00Z".to_string(),
                })],
                event_rate: 1.0,
                top_changed_files: vec![],
            }))
        })
    }
}

// ================================================================
// Harness + egress registry
// ================================================================

struct Harness {
    api: Arc<FakeApi>,
    /// `workspace_api` server for an ordinary workspace, caller `a-1`.
    srv: WorkspaceMcpServer,
    /// `workspace_api` server for the Chief-of-Staff workspace (`ws.app.*`).
    chief: WorkspaceMcpServer,
}

impl Harness {
    fn new() -> Self {
        let api = Arc::new(FakeApi::default());
        let srv = WorkspaceMcpServer::new(api.clone(), WorkspaceId::from_string(WS))
            .with_caller_agent_id(Some(AgentId::from(CALLER)));
        let chief = WorkspaceMcpServer::new(api.clone(), WorkspaceId::chief())
            .with_caller_agent_id(Some(AgentId::from("chief-agent")));
        Self { api, srv, chief }
    }

    /// Run `code` through the real `workspace_api` tool and return the
    /// parsed body; a tool error fails the test naming the egress.
    async fn mcp(srv: &WorkspaceMcpServer, name: &str, code: &str) -> Value {
        let resp = srv
            .handle_message(&json!({
                "jsonrpc": "2.0", "id": 1, "method": "tools/call",
                "params": {
                    "name": "workspace_api",
                    "arguments": { "code": code, "summary": "hidden-field egress contract" }
                }
            }))
            .await
            .expect("tools/call must produce a response");
        let text = resp["result"]["content"][0]["text"]
            .as_str()
            .unwrap_or_else(|| panic!("{name}: no text content: {resp}"));
        assert_eq!(
            resp["result"]["isError"],
            json!(false),
            "{name}: workspace_api returned an error: {text}"
        );
        serde_json::from_str(text)
            .unwrap_or_else(|e| panic!("{name}: body is not JSON ({e}): {text}"))
    }
}

type Producer = for<'a> fn(&'a Harness) -> Pin<Box<dyn Future<Output = Value> + 'a>>;

/// Every agent-facing egress that serves session or event data. A new such
/// egress MUST be appended here (see the module docs).
const EGRESS_REGISTRY: &[(&str, Producer)] = &[
    ("ws.agent.status", |h| {
        Box::pin(Harness::mcp(
            &h.srv,
            "ws.agent.status",
            "return await ws.agent.status('a-2');",
        ))
    }),
    ("ws.agent.list", |h| {
        Box::pin(Harness::mcp(
            &h.srv,
            "ws.agent.list",
            "return await ws.agent.list();",
        ))
    }),
    ("ws.agent.getQueue", |h| {
        Box::pin(Harness::mcp(
            &h.srv,
            "ws.agent.getQueue",
            "return await ws.agent.getQueue('a-2');",
        ))
    }),
    ("ws.agent.diagnostics", |h| {
        Box::pin(Harness::mcp(
            &h.srv,
            "ws.agent.diagnostics",
            "return await ws.agent.diagnostics();",
        ))
    }),
    ("ws.agent.snapshot", |h| {
        Box::pin(Harness::mcp(
            &h.srv,
            "ws.agent.snapshot",
            "return await ws.agent.snapshot();",
        ))
    }),
    ("ws.agent.readConversation", |h| {
        Box::pin(Harness::mcp(
            &h.srv,
            "ws.agent.readConversation",
            "return await ws.agent.readConversation('a-2', { lastN: 10 });",
        ))
    }),
    ("ws.agent.summary", |h| {
        Box::pin(Harness::mcp(
            &h.srv,
            "ws.agent.summary",
            "return await ws.agent.summary('a-2');",
        ))
    }),
    ("ws.event.query", |h| {
        Box::pin(Harness::mcp(
            &h.srv,
            "ws.event.query",
            "return await ws.event.query({ eventType: 'agent:*' });",
        ))
    }),
    ("ws.event.query (paginated)", |h| {
        Box::pin(Harness::mcp(
            &h.srv,
            "ws.event.query (paginated)",
            "return await ws.event.query({ eventType: 'agent:*', paginate: true });",
        ))
    }),
    ("ws.event.agentActivity (agentId)", |h| {
        Box::pin(Harness::mcp(
            &h.srv,
            "ws.event.agentActivity (agentId)",
            "return await ws.event.agentActivity('a-2');",
        ))
    }),
    ("ws.event.agentActivity (workspace)", |h| {
        Box::pin(Harness::mcp(
            &h.srv,
            "ws.event.agentActivity (workspace)",
            "return await ws.event.agentActivity();",
        ))
    }),
    ("ws.event.workspaceSummary", |h| {
        Box::pin(Harness::mcp(
            &h.srv,
            "ws.event.workspaceSummary",
            "return await ws.event.workspaceSummary();",
        ))
    }),
    ("wake metadata (build_event_notification_metadata)", |_h| {
        Box::pin(async {
            let events = typed_events();
            let refs: Vec<&Event> = events.iter().collect();
            intent_services::build_event_notification_metadata(&refs)
        })
    }),
    ("ws.app.agents.list", |h| {
        Box::pin(Harness::mcp(
            &h.chief,
            "ws.app.agents.list",
            "return await ws.app.agents.list();",
        ))
    }),
    ("ws.app.agents.readConversation", |h| {
        Box::pin(Harness::mcp(
            &h.chief,
            "ws.app.agents.readConversation",
            "return await ws.app.agents.readConversation('amber-forest', 'a-2', { lastN: 10 });",
        ))
    }),
];

/// The contract: no `AGENT_HIDDEN_FIELDS` key survives on any registered
/// egress. Every producer runs (failures are collected, not short-circuited)
/// so one run names every leaking egress.
#[tokio::test]
async fn no_agent_hidden_field_survives_any_egress() {
    assert!(
        !AGENT_HIDDEN_FIELDS.is_empty(),
        "AGENT_HIDDEN_FIELDS is empty — the contract is vacuous"
    );
    let h = Harness::new();
    let mut leaks: Vec<String> = Vec::new();
    for (name, producer) in EGRESS_REGISTRY {
        let out = producer(&h).await;
        assert!(
            !out.is_null(),
            "{name}: egress produced null — the fixture did not reach it"
        );
        if let Some((path, key)) = find_hidden_key(&out, "$") {
            leaks.push(format!("{name}: hidden field `{key}` survived at {path}"));
        }
    }
    assert!(
        leaks.is_empty(),
        "AGENT_HIDDEN_FIELDS leaked on {} egress(es):\n  {}",
        leaks.len(),
        leaks.join("\n  ")
    );
    assert!(
        *h.api.event_query_calls.lock().unwrap() > 0,
        "event fixture was never read — the ws.event.* entries did not exercise event_query"
    );
}

/// Fixture sanity: the poison really lands on every input the egresses read,
/// so a green contract test means the scrub removed it, not that it was
/// never there. Also proves [`find_hidden_key`] reports path + key.
#[test]
fn fixture_is_poisoned_with_every_hidden_field() {
    let ws = WorkspaceId::from_string(WS);
    let agent = serde_json::to_value(stub_agent(TARGET, &ws)).unwrap();
    // Typed: only hidden keys that are real `AgentLite` fields can be
    // present; each of those must carry a populated (non-null) sentinel, and
    // at least one must exist or the typed egresses (`status` / `list`) are
    // never really exercised.
    let typed_hits = AGENT_HIDDEN_FIELDS
        .iter()
        .filter(|key| agent.get(**key).is_some())
        .inspect(|key| {
            assert!(
                !agent[**key].is_null(),
                "AgentLite fixture must carry a populated {key}"
            );
        })
        .count();
    assert!(
        typed_hits > 0,
        "AgentLite carries none of AGENT_HIDDEN_FIELDS"
    );

    let rows = event_rows();
    let typed = typed_events();
    let messages = conversation_messages();
    let queue = queue_entries();
    for key in AGENT_HIDDEN_FIELDS {
        for (label, v) in [
            ("event row", &rows[0]["data"]),
            ("typed event", &typed[0].data),
            (
                "conversation messageMetadata",
                &messages[0]["messageMetadata"],
            ),
            ("queue entry messageMetadata", &queue[0]["messageMetadata"]),
        ] {
            assert_eq!(v[*key], json!(SENTINEL), "{label} fixture must carry {key}");
        }
    }
    let found = find_hidden_key(
        &json!({ "events": [ { "data": rows[0]["data"].clone() } ] }),
        "$",
    );
    assert_eq!(
        found,
        Some((
            "$.events[0].data".to_string(),
            AGENT_HIDDEN_FIELDS[0].to_string()
        ))
    );
    assert_eq!(find_hidden_key(&json!({ "a": [ { "b": 1 } ] }), "$"), None);
}
