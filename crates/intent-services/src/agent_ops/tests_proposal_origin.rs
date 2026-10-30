//! `workspace.create { origin: { kind: "proposal", agentId, proposalId } }`
//! (intent-hq/intent#8086): a card Apply's create resolves the proposing
//! agent's pending proposal daemon-side, so the `proposal_resolved` notice
//! never waits for the proposing conversation to be open in a client.

use intent_core::{
    now_iso, with_caller, AgentId, Caller, HostRole, PrincipalId, WorkspaceApi, WorkspaceCreate,
    WorkspaceCreateResult, WorkspaceId,
};
use serde_json::{json, Value};

use super::tests::{create_agent, proposal_blocks, setup_with_bus, TempDb};
use crate::Services;

/// The shared agent-ops harness plus a scratch workspaces root, so a
/// `workspace.create` never provisions under the real `$HOME` default.
async fn setup() -> (TempDb, Services, WorkspaceId, crate::EventBus) {
    let (tmp, svc, ws, bus) = setup_with_bus().await;
    let svc = svc.with_workspaces_root(tmp.path.with_extension("workspaces"));
    (tmp, svc, ws, bus)
}

/// A persisted assistant content-block array carrying one lifted
/// `workspace-create` proposal (the shape `ws.workspace.proposeSibling`
/// produces), identified by `applyToolCallId = proposal_id`, whose params
/// carry `idempotency_key` when given.
fn create_proposal_blocks(proposal_id: &str, idempotency_key: Option<&str>) -> Value {
    let mut params = json!({ "title": "Follow-up", "skipIsolation": true });
    if let Some(key) = idempotency_key {
        params["idempotencyKey"] = json!(key);
    }
    let proposal = json!({
        "kind": "workspace-create",
        "applyToolCallId": proposal_id,
        "preview": { "title": "Create workspace: Follow-up" },
        "payload": { "operation": "workspace.create", "params": params },
    });
    json!([
        { "type": "text", "text": "Here is a proposal." },
        {
            "type": "resource",
            "id": "m:1",
            "resource": {
                "uri": format!("intent-proposal://workspace-create/{proposal_id}"),
                "name": "Create workspace: Follow-up",
                "mimeType": "application/vnd.intent.proposal+json",
                "text": serde_json::to_string(&proposal).unwrap(),
            }
        }
    ])
}

/// Record `blocks` (carrying `proposal_id`) as a pending proposal on `agent`.
async fn seed(
    svc: &Services,
    ws: &WorkspaceId,
    agent: &AgentId,
    proposal_id: &str,
    blocks: &Value,
) {
    let msg = svc
        .store()
        .append_agent_message(agent, "assistant", blocks, &now_iso())
        .await
        .expect("append proposal");
    assert!(
        svc.record_pending_proposals(ws, agent, &msg.id, &[proposal_id.to_string()])
            .await
    );
}

/// Seed a key-less `workspace-create` proposal `proposal_id` as pending on
/// a fresh agent in `ws`.
async fn pending_proposal(svc: &Services, ws: &WorkspaceId, proposal_id: &str) -> AgentId {
    let id = create_agent(svc, ws, "Proposer").await;
    seed(
        svc,
        ws,
        &id,
        proposal_id,
        &create_proposal_blocks(proposal_id, None),
    )
    .await;
    id
}

/// Parse a `workspace.create` request exactly as the router does: from the
/// wire JSON object.
fn wire_create(params: Value) -> WorkspaceCreate {
    serde_json::from_value(params).expect("wire workspace.create params")
}

/// `workspace.create` as the host owner's wire connection (the method is
/// creator-gated and every capability gate needs a bound caller).
async fn create_as_owner(
    svc: &Services,
    input: WorkspaceCreate,
    idempotency_key: Option<&str>,
) -> intent_core::Result<WorkspaceCreateResult> {
    with_caller(
        Caller::Wire {
            principal_id: PrincipalId::new(),
            host_role: HostRole::Owner,
        },
        WorkspaceApi::create_workspace(svc, input, idempotency_key.map(str::to_string)),
    )
    .await
}

fn proposal_resolved_notices(messages: &[intent_core::AgentMessage]) -> Vec<String> {
    messages
        .iter()
        .filter(|m| {
            m.metadata
                .as_ref()
                .and_then(|md| md.get("type"))
                .and_then(Value::as_str)
                == Some("proposal_resolved")
        })
        .map(|m| {
            m.content[0]["text"]
                .as_str()
                .unwrap_or_default()
                .to_string()
        })
        .collect()
}

/// Regression (intent-hq/intent#8086): a create carrying a proposal `origin`
/// resolves the pending proposal `applied` and delivers exactly one notice
/// naming the created workspace — with no client-side `agent.resolveProposal`.
#[tokio::test]
async fn create_with_proposal_origin_resolves_and_notifies() {
    let (_t, svc, ws, _bus) = setup().await;
    let agent = pending_proposal(&svc, &ws, "tc-1").await;

    let created = create_as_owner(
        &svc,
        wire_create(json!({
            "title": "Follow-up",
            "skipIsolation": true,
            "origin": { "kind": "proposal", "agentId": agent.0, "proposalId": "tc-1" },
        })),
        None,
    )
    .await
    .expect("workspace.create");
    let new_id = created.workspace.id.0.clone();
    assert_eq!(reported(&created), Some(outcome("tc-1", "applied")));

    let session = svc
        .store()
        .get_agent_session(&agent)
        .await
        .expect("session");
    assert!(
        session.pending_proposals().is_empty(),
        "the create must resolve the pending proposal"
    );
    assert_eq!(
        session.proposal_resolutions().get("tc-1"),
        Some(&json!("applied"))
    );
    let messages = svc
        .store()
        .get_agent_messages(&agent, None)
        .await
        .expect("messages");
    assert_eq!(
        proposal_resolved_notices(&messages),
        vec![format!(
            "User applied the proposal 'Create workspace: Follow-up'. Created workspace {new_id}."
        )],
    );
}

fn reported(result: &WorkspaceCreateResult) -> Option<(String, String)> {
    result
        .proposal_origin
        .as_ref()
        .map(|o| (o.proposal_id.clone(), o.outcome.clone()))
}

fn outcome(proposal_id: &str, outcome: &str) -> (String, String) {
    (proposal_id.to_string(), outcome.to_string())
}

fn origin(agent: &AgentId, proposal_id: &str) -> Value {
    json!({ "kind": "proposal", "agentId": agent.0, "proposalId": proposal_id })
}

async fn notices_for(svc: &Services, agent: &AgentId) -> Vec<String> {
    let messages = svc
        .store()
        .get_agent_messages(agent, None)
        .await
        .expect("messages");
    proposal_resolved_notices(&messages)
}

/// A same-key replay returns the stored workspace and re-runs the resolve
/// step idempotently: still exactly one notice. A later client-side
/// `agent.resolveProposal` (the older-daemon fallback) is a no-op too.
#[tokio::test]
async fn replay_and_client_fallback_never_duplicate_the_notice() {
    let (_t, svc, ws, _bus) = setup().await;
    let agent = pending_proposal(&svc, &ws, "tc-1").await;
    let params =
        json!({ "title": "Follow-up", "skipIsolation": true, "origin": origin(&agent, "tc-1") });

    let first = create_as_owner(&svc, wire_create(params.clone()), Some("key-1"))
        .await
        .expect("first create");
    let replay = create_as_owner(&svc, wire_create(params), Some("key-1"))
        .await
        .expect("replayed create");
    assert_eq!(
        first.workspace.id, replay.workspace.id,
        "same key, same workspace"
    );
    assert_eq!(reported(&first), Some(outcome("tc-1", "applied")));
    assert_eq!(
        reported(&replay),
        Some(outcome("tc-1", "applied")),
        "a replay reports the persisted outcome"
    );

    let fallback = svc
        .agent_resolve_proposal_op(
            ws.clone(),
            agent.clone(),
            "tc-1".to_string(),
            "applied".to_string(),
            Some("Created workspace x.".to_string()),
        )
        .await
        .expect("client fallback resolve");
    assert_eq!(fallback["outcome"], json!("applied"));
    assert_eq!(
        notices_for(&svc, &agent).await.len(),
        1,
        "exactly one notice"
    );
}

/// A proposal the user already dismissed keeps that outcome: the create
/// still succeeds and the agent hears nothing more.
#[tokio::test]
async fn already_resolved_proposal_keeps_its_outcome() {
    let (_t, svc, ws, _bus) = setup().await;
    let agent = pending_proposal(&svc, &ws, "tc-1").await;
    svc.agent_resolve_proposal_op(
        ws.clone(),
        agent.clone(),
        "tc-1".to_string(),
        "dismissed".to_string(),
        None,
    )
    .await
    .expect("dismiss");

    let created = create_as_owner(
        &svc,
        wire_create(
            json!({ "title": "Late", "skipIsolation": true, "origin": origin(&agent, "tc-1") }),
        ),
        None,
    )
    .await
    .expect("create still succeeds");
    assert_eq!(reported(&created), Some(outcome("tc-1", "dismissed")));

    let session = svc
        .store()
        .get_agent_session(&agent)
        .await
        .expect("session");
    assert_eq!(
        session.proposal_resolutions().get("tc-1"),
        Some(&json!("dismissed"))
    );
    assert_eq!(
        notices_for(&svc, &agent).await.len(),
        1,
        "only the dismissal notice"
    );
}

/// Fail-soft lookups: an unknown agent, or a proposal id that was never
/// pending, never blocks the create and never errors.
#[tokio::test]
async fn unknown_agent_or_proposal_still_creates() {
    let (_t, svc, ws, _bus) = setup().await;
    let agent = pending_proposal(&svc, &ws, "tc-1").await;

    let ghost = create_as_owner(
        &svc,
        wire_create(json!({
            "title": "Ghost agent",
            "skipIsolation": true,
            "origin": origin(&AgentId::from("agent-missing"), "tc-1"),
        })),
        None,
    )
    .await
    .expect("unknown agent is fail-soft");
    let other = create_as_owner(
        &svc,
        wire_create(json!({ "title": "Other card", "skipIsolation": true, "origin": origin(&agent, "tc-other") })),
        None,
    )
    .await
    .expect("unknown proposal is fail-soft");
    assert_eq!(reported(&ghost), Some(outcome("tc-1", "notResolved")));
    assert_eq!(reported(&other), Some(outcome("tc-other", "notResolved")));

    let session = svc
        .store()
        .get_agent_session(&agent)
        .await
        .expect("session");
    assert_eq!(session.pending_proposals().len(), 1, "tc-1 is untouched");
    assert!(notices_for(&svc, &agent).await.is_empty());
}

/// A malformed origin is rejected `-32602` before anything is created; an
/// unknown `kind` already fails deserialization (the router's `-32602`).
#[tokio::test]
async fn malformed_origin_is_rejected_before_any_side_effect() {
    let (_t, svc, ws, _bus) = setup().await;
    let agent = pending_proposal(&svc, &ws, "tc-1").await;
    let before = svc
        .store()
        .list_workspaces(intent_core::WorkspaceListFilter::default())
        .await
        .expect("list")
        .len();

    let too_long = "x".repeat(super::MAX_MESSAGE_ID_LEN + 1);
    for bad in [
        json!({ "kind": "proposal", "agentId": "  ", "proposalId": "tc-1" }),
        json!({ "kind": "proposal", "agentId": agent.0, "proposalId": "" }),
        json!({ "kind": "proposal", "agentId": agent.0, "proposalId": too_long }),
    ] {
        let err = create_as_owner(
            &svc,
            wire_create(json!({ "title": "Bad", "skipIsolation": true, "origin": bad })),
            None,
        )
        .await
        .expect_err("malformed origin must reject");
        assert!(
            matches!(err, intent_core::Error::InvalidParams(_)),
            "-32602 for {bad}: {err:?}"
        );
    }
    assert!(
        serde_json::from_value::<WorkspaceCreate>(json!({
            "origin": { "kind": "discussion", "agentId": agent.0, "proposalId": "tc-1" }
        }))
        .is_err(),
        "unknown origin kind must not parse"
    );
    assert_eq!(
        svc.store()
            .list_workspaces(intent_core::WorkspaceListFilter::default())
            .await
            .expect("list")
            .len(),
        before,
        "nothing was created"
    );
    let session = svc
        .store()
        .get_agent_session(&agent)
        .await
        .expect("session");
    assert_eq!(session.pending_proposals().len(), 1);
}

/// A failed create resolves nothing, so the card stays pending for Retry.
/// The failure here is raised inside the create op (a `contextLinks` entry
/// with number 0), after the origin was validated and recorded.
#[tokio::test]
async fn failed_create_leaves_the_proposal_pending() {
    let (_t, svc, ws, _bus) = setup().await;
    let agent = pending_proposal(&svc, &ws, "tc-1").await;

    let err = create_as_owner(
        &svc,
        wire_create(json!({
            "title": "Broken",
            "skipIsolation": true,
            "contextLinks": [{
                "kind": "issue",
                "url": "https://github.com/intent-hq/intent/issues/1",
                "owner": "intent-hq",
                "repo": "intent",
                "number": 0,
            }],
            "origin": origin(&agent, "tc-1"),
        })),
        None,
    )
    .await
    .expect_err("an invalid context link fails the create");
    assert!(
        matches!(err, intent_core::Error::InvalidParams(ref m) if m.contains("contextLinks")),
        "{err:?}"
    );

    let session = svc
        .store()
        .get_agent_session(&agent)
        .await
        .expect("session");
    assert_eq!(session.pending_proposals().len(), 1, "still pending");
    assert!(notices_for(&svc, &agent).await.is_empty());
}

/// `workspace:created` echoes the provenance with the daemon-derived source
/// workspace, ignoring a client-sent `sourceWorkspaceId`.
#[tokio::test]
async fn workspace_created_event_carries_the_derived_origin() {
    let (_t, svc, ws, bus) = setup().await;
    let agent = pending_proposal(&svc, &ws, "tc-1").await;
    let mut sub = bus.subscribe(crate::SubscriptionFilter {
        event_types: vec![intent_core::events::WORKSPACE_CREATED.to_string()],
        ..Default::default()
    });

    let created = create_as_owner(
        &svc,
        wire_create(json!({
            "title": "Echo",
            "skipIsolation": true,
            "origin": {
                "kind": "proposal",
                "agentId": agent.0,
                "proposalId": "tc-1",
                "sourceWorkspaceId": "ws-forged",
            },
        })),
        None,
    )
    .await
    .expect("create");

    let batch = tokio::time::timeout(
        intentd_test_support::test_timeout(std::time::Duration::from_secs(2)),
        sub.recv(),
    )
    .await
    .expect("recv timed out")
    .expect("subscription closed");
    let event = batch
        .iter()
        .find(|e| e.data["workspaceId"] == json!(created.workspace.id.0))
        .expect("workspace:created for the new workspace");
    assert_eq!(
        event.data["origin"],
        json!({
            "kind": "proposal",
            "proposalId": "tc-1",
            "agentId": agent.0,
            "sourceWorkspaceId": ws.0,
        })
    );
}

/// An in-process caller that already set `origin` (`ws.workspace.applyProposal`)
/// owns the resolution: a wire `origin` alongside it is ignored, so the
/// create itself resolves nothing.
#[tokio::test]
async fn in_process_origin_takes_precedence_over_the_wire_param() {
    let (_t, svc, ws, _bus) = setup().await;
    let agent = pending_proposal(&svc, &ws, "tc-1").await;
    let mut input = wire_create(
        json!({ "title": "Binding", "skipIsolation": true, "origin": origin(&agent, "tc-1") }),
    );
    input.origin = Some(intent_core::WorkspaceCreateOrigin::Proposal {
        proposal_id: "tc-1".to_string(),
        agent_id: agent.clone(),
        source_workspace_id: ws.clone(),
    });

    let created = create_as_owner(&svc, input, None).await.expect("create");
    assert_eq!(
        reported(&created),
        None,
        "no proposalOrigin for an in-process origin"
    );

    let session = svc
        .store()
        .get_agent_session(&agent)
        .await
        .expect("session");
    assert_eq!(
        session.pending_proposals().len(),
        1,
        "the binding resolves its own proposal"
    );
    assert!(notices_for(&svc, &agent).await.is_empty());
}

/// Only a `workspace-create` proposal can be applied by a create: naming a
/// pending proposal of another kind (here a settings change) resolves
/// nothing, so that card stays actionable.
#[tokio::test]
async fn non_workspace_create_proposal_is_not_resolved() {
    let (_t, svc, ws, _bus) = setup().await;
    let agent = create_agent(&svc, &ws, "Proposer").await;
    seed(
        &svc,
        &ws,
        &agent,
        "tc-settings",
        &proposal_blocks("tc-settings"),
    )
    .await;

    let created = create_as_owner(
        &svc,
        wire_create(json!({ "title": "Wrong card", "skipIsolation": true, "origin": origin(&agent, "tc-settings") })),
        None,
    )
    .await
    .expect("create still succeeds");
    assert_eq!(
        reported(&created),
        Some(outcome("tc-settings", "notResolved"))
    );

    let session = svc
        .store()
        .get_agent_session(&agent)
        .await
        .expect("session");
    assert_eq!(
        session.pending_proposals().len(),
        1,
        "settings card stays pending"
    );
    assert!(notices_for(&svc, &agent).await.is_empty());
}

/// A card whose params carry an `idempotencyKey` is applied only by a create
/// under that key — so a replay of another card's key cannot mark it applied.
#[tokio::test]
async fn origin_is_bound_to_the_cards_idempotency_key() {
    let (_t, svc, ws, _bus) = setup().await;
    let agent = create_agent(&svc, &ws, "Proposer").await;
    seed(
        &svc,
        &ws,
        &agent,
        "tc-1",
        &create_proposal_blocks("tc-1", Some("key-1")),
    )
    .await;
    seed(
        &svc,
        &ws,
        &agent,
        "tc-2",
        &create_proposal_blocks("tc-2", Some("key-2")),
    )
    .await;

    // Card 1 applied under its own key.
    let one = create_as_owner(
        &svc,
        wire_create(
            json!({ "title": "One", "skipIsolation": true, "origin": origin(&agent, "tc-1") }),
        ),
        Some("key-1"),
    )
    .await
    .expect("create card 1");
    assert_eq!(reported(&one), Some(outcome("tc-1", "applied")));
    // A replay of key-1 naming card 2, and a key-less create naming card 2:
    // both report `notResolved`, so a client never falls back to resolving
    // card 2 itself.
    let replay = create_as_owner(
        &svc,
        wire_create(
            json!({ "title": "One", "skipIsolation": true, "origin": origin(&agent, "tc-2") }),
        ),
        Some("key-1"),
    )
    .await
    .expect("replay of key-1");
    let keyless = create_as_owner(
        &svc,
        wire_create(
            json!({ "title": "Two", "skipIsolation": true, "origin": origin(&agent, "tc-2") }),
        ),
        None,
    )
    .await
    .expect("key-less create");
    assert_eq!(reported(&replay), Some(outcome("tc-2", "notResolved")));
    assert_eq!(reported(&keyless), Some(outcome("tc-2", "notResolved")));

    let session = svc
        .store()
        .get_agent_session(&agent)
        .await
        .expect("session");
    assert_eq!(
        session.proposal_resolutions().get("tc-1"),
        Some(&json!("applied"))
    );
    let pending: Vec<String> = session
        .pending_proposals()
        .into_iter()
        .map(|p| p.proposal_id)
        .collect();
    assert_eq!(pending, vec!["tc-2".to_string()], "card 2 is untouched");
    assert_eq!(notices_for(&svc, &agent).await.len(), 1);
}

/// A replay returns a workspace an earlier request created, so it can only
/// prove which card it applies when that card is bound to the replayed key.
/// Naming a key-less pending card on a replay is declined.
#[tokio::test]
async fn replay_naming_a_keyless_card_is_declined() {
    let (_t, svc, ws, _bus) = setup().await;
    let agent = create_agent(&svc, &ws, "Proposer").await;
    seed(
        &svc,
        &ws,
        &agent,
        "tc-a",
        &create_proposal_blocks("tc-a", None),
    )
    .await;
    seed(
        &svc,
        &ws,
        &agent,
        "tc-b",
        &create_proposal_blocks("tc-b", None),
    )
    .await;

    let first = create_as_owner(
        &svc,
        wire_create(
            json!({ "title": "A", "skipIsolation": true, "origin": origin(&agent, "tc-a") }),
        ),
        Some("key-a"),
    )
    .await
    .expect("create card A");
    let replay = create_as_owner(
        &svc,
        wire_create(
            json!({ "title": "A", "skipIsolation": true, "origin": origin(&agent, "tc-b") }),
        ),
        Some("key-a"),
    )
    .await
    .expect("replay of key-a naming card B");

    assert_eq!(first.workspace.id, replay.workspace.id);
    assert_eq!(reported(&first), Some(outcome("tc-a", "applied")));
    assert_eq!(reported(&replay), Some(outcome("tc-b", "notResolved")));
    let session = svc
        .store()
        .get_agent_session(&agent)
        .await
        .expect("session");
    let pending: Vec<String> = session
        .pending_proposals()
        .into_iter()
        .map(|p| p.proposal_id)
        .collect();
    assert_eq!(pending, vec!["tc-b".to_string()], "card B is untouched");
    assert_eq!(notices_for(&svc, &agent).await.len(), 1);
}

/// Store errors while validating the origin are not "declined": a failed
/// read of the card's carrying message fails the create before any side
/// effect (no workspace, card still pending), and a retry once the read
/// recovers creates the workspace and resolves the card.
#[tokio::test]
async fn origin_lookup_store_error_fails_the_create_and_a_retry_recovers() {
    let (_t, svc, ws, _bus) = setup().await;
    let agent = pending_proposal(&svc, &ws, "tc-1").await;
    let workspaces = |svc: &Services| {
        let store = svc.store().clone();
        async move {
            store
                .list_workspaces(intent_core::WorkspaceListFilter::default())
                .await
                .expect("list")
                .len()
        }
    };
    let before = workspaces(&svc).await;
    let params =
        json!({ "title": "Follow-up", "skipIsolation": true, "origin": origin(&agent, "tc-1") });

    // Inject a read failure on the message lookup only (the session row
    // stays readable): hide the transcript table.
    sqlx::query("ALTER TABLE agent_message RENAME TO agent_message_hidden")
        .execute(svc.store().write_pool())
        .await
        .expect("hide agent_message");
    let err = create_as_owner(&svc, wire_create(params.clone()), Some("key-1"))
        .await
        .expect_err("a store error during origin validation fails the create");
    assert!(
        !matches!(err, intent_core::Error::InvalidParams(_)),
        "a store failure is not a request-shape error: {err:?}"
    );
    sqlx::query("ALTER TABLE agent_message_hidden RENAME TO agent_message")
        .execute(svc.store().write_pool())
        .await
        .expect("restore agent_message");

    assert_eq!(workspaces(&svc).await, before, "nothing was created");
    let session = svc
        .store()
        .get_agent_session(&agent)
        .await
        .expect("session");
    assert_eq!(session.pending_proposals().len(), 1, "card still pending");

    let created = create_as_owner(&svc, wire_create(params), Some("key-1"))
        .await
        .expect("retry after the read recovers");
    assert_eq!(reported(&created), Some(outcome("tc-1", "applied")));
    assert_eq!(workspaces(&svc).await, before + 1);
    assert_eq!(notices_for(&svc, &agent).await.len(), 1);
}
