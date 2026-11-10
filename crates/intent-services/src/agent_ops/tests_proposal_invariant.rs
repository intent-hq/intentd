//! A proposal id is never both pending and resolved (PROTOCOL §5.5). Legacy
//! metadata holding an id in both `pendingProposals` and
//! `proposalResolutions` reads as resolved (and heals on the next write),
//! and re-recording a resolved id reopens it as a fresh pending card.

use intent_core::events::AGENT_UPDATED;
use intent_core::{now_iso, AgentId, WorkspaceId};
use serde_json::{json, Value};
use tokio::time::{timeout, Duration};

use super::tests::{create_agent, proposal_blocks, setup, setup_with_bus};
use crate::{Services, SubscriptionFilter};

async fn record(svc: &Services, ws: &WorkspaceId, agent: &AgentId, ids: &[&str]) -> String {
    let blocks = Value::Array(
        ids.iter()
            .flat_map(|id| proposal_blocks(id).as_array().unwrap().clone())
            .collect(),
    );
    let msg = svc
        .store()
        .append_agent_message(agent, "assistant", &blocks, &now_iso())
        .await
        .expect("append proposal");
    let ids: Vec<String> = ids.iter().map(|id| (*id).to_string()).collect();
    assert!(svc.record_pending_proposals(ws, agent, &msg.id, &ids).await);
    msg.id
}

async fn seed_key(svc: &Services, ws: &WorkspaceId, agent: &AgentId, key: &str, value: Value) {
    svc.store()
        .set_agent_session_metadata_key_json(
            ws,
            agent,
            key,
            &serde_json::to_string(&value).unwrap(),
            &now_iso(),
        )
        .await
        .expect("seed metadata key");
}

async fn session(svc: &Services, agent: &AgentId) -> intent_core::AgentSession {
    svc.store()
        .get_agent_session_with_transcript(agent)
        .await
        .expect("session")
}

fn raw_ids(session: &intent_core::AgentSession) -> Vec<String> {
    session
        .pending_proposals_raw()
        .into_iter()
        .map(|p| p.proposal_id)
        .collect()
}

fn pending_ids(session: &intent_core::AgentSession) -> Vec<String> {
    session
        .pending_proposals()
        .into_iter()
        .map(|p| p.proposal_id)
        .collect()
}

async fn resolved_notices(svc: &Services, agent: &AgentId, proposal_id: &str) -> usize {
    svc.store()
        .get_agent_messages(agent, None)
        .await
        .expect("messages")
        .iter()
        .filter(|m| {
            m.metadata.as_ref().is_some_and(|md| {
                md["type"] == "proposal_resolved" && md["proposalId"] == proposal_id
            })
        })
        .count()
}

/// Legacy contradictory metadata (`tc-1` both pending and resolved) reads as
/// resolved in every projection; resolving it again is idempotent (persisted
/// outcome echoed, no notice) and drops the stale raw entry, leaving the
/// genuinely pending `tc-2` and the recorded outcome untouched.
#[tokio::test]
async fn legacy_contradictory_entry_reads_resolved_and_resolve_heals_it() {
    let (_t, svc, ws) = setup().await;
    let id = create_agent(&svc, &ws, "Proposer").await;
    record(&svc, &ws, &id, &["tc-1", "tc-2"]).await;
    seed_key(
        &svc,
        &ws,
        &id,
        intent_core::PROPOSAL_RESOLUTIONS_KEY,
        json!({ "tc-1": "applied" }),
    )
    .await;
    seed_key(
        &svc,
        &ws,
        &id,
        intent_core::PROPOSAL_CREATED_WORKSPACES_KEY,
        json!({ "tc-1": "ws-created" }),
    )
    .await;

    let before = session(&svc, &id).await;
    assert_eq!(raw_ids(&before), ["tc-1", "tc-2"], "legacy data as stored");
    assert_eq!(pending_ids(&before), ["tc-2"], "resolved id is not pending");
    let lite = intent_core::AgentLite::from_session(before, 0, None, None, None, None, None);
    assert_eq!(
        lite.metadata
            .pending_proposals
            .iter()
            .map(|p| p.proposal_id.as_str())
            .collect::<Vec<_>>(),
        ["tc-2"]
    );
    assert_eq!(
        lite.metadata.proposal_resolutions.get("tc-1"),
        Some(&json!("applied"))
    );

    for _ in 0..2 {
        let out = svc
            .agent_resolve_proposal_op(
                ws.clone(),
                id.clone(),
                "tc-1".to_string(),
                "dismissed".to_string(),
                None,
                None,
            )
            .await
            .expect("resolve is idempotent");
        assert_eq!(out["outcome"], json!("applied"), "persisted outcome wins");
    }
    assert_eq!(resolved_notices(&svc, &id, "tc-1").await, 0, "no notice");

    let after = session(&svc, &id).await;
    assert_eq!(raw_ids(&after), ["tc-2"], "stale raw entry dropped");
    assert_eq!(
        Value::Object(after.proposal_resolutions()),
        json!({ "tc-1": "applied" })
    );
    assert_eq!(
        Value::Object(after.proposal_created_workspaces()),
        json!({ "tc-1": "ws-created" })
    );

    // `tc-2` resolves normally.
    svc.agent_resolve_proposal_op(
        ws.clone(),
        id.clone(),
        "tc-2".to_string(),
        "applied".to_string(),
        None,
        None,
    )
    .await
    .expect("resolve tc-2");
    assert_eq!(resolved_notices(&svc, &id, "tc-2").await, 1);
    assert!(session(&svc, &id).await.pending_proposals_raw().is_empty());
}

/// Re-recording an id that is already resolved (an id-less card re-proposed
/// under a resolved title) reopens it as a fresh pending card: the stale
/// outcome and created-workspace link are dropped, `agent:updated` carries
/// the cleared maps, and a later resolution records a new outcome with its
/// own notice.
#[tokio::test]
async fn rerecording_resolved_id_reopens_it_as_fresh_pending() {
    let (_t, svc, ws, bus) = setup_with_bus().await;
    let id = create_agent(&svc, &ws, "Proposer").await;
    record(&svc, &ws, &id, &["tc-1", "tc-keep"]).await;
    svc.agent_resolve_proposal_op(
        ws.clone(),
        id.clone(),
        "tc-1".to_string(),
        "applied".to_string(),
        None,
        Some(WorkspaceId("ws-created".to_string())),
    )
    .await
    .expect("resolve tc-1");
    svc.agent_resolve_proposal_op(
        ws.clone(),
        id.clone(),
        "tc-keep".to_string(),
        "dismissed".to_string(),
        None,
        None,
    )
    .await
    .expect("resolve tc-keep");
    assert_eq!(
        session(&svc, &id)
            .await
            .proposal_created_workspaces()
            .get("tc-1"),
        Some(&json!("ws-created"))
    );

    let mut sub = bus.subscribe(SubscriptionFilter {
        event_types: vec![AGENT_UPDATED.to_string()],
        ..Default::default()
    });
    let m2 = record(&svc, &ws, &id, &["tc-1"]).await;

    let s = session(&svc, &id).await;
    assert_eq!(raw_ids(&s), ["tc-1"]);
    assert_eq!(s.pending_proposals()[0].message_id, m2);
    assert_eq!(
        Value::Object(s.proposal_resolutions()),
        json!({ "tc-keep": "dismissed" }),
        "only the re-recorded id's outcome is dropped"
    );
    assert!(s.proposal_created_workspaces().is_empty());

    let batch = timeout(
        intentd_test_support::test_timeout(Duration::from_secs(2)),
        sub.recv(),
    )
    .await
    .expect("recv timed out")
    .expect("subscription closed");
    assert_eq!(batch.len(), 1);
    assert_eq!(
        batch[0].data["pendingProposals"],
        json!([{ "proposalId": "tc-1", "messageId": m2 }])
    );
    assert_eq!(
        batch[0].data["proposalResolutions"],
        json!({ "tc-keep": "dismissed" })
    );
    assert_eq!(batch[0].data["proposalCreatedWorkspaces"], json!({}));

    let out = svc
        .agent_resolve_proposal_op(
            ws.clone(),
            id.clone(),
            "tc-1".to_string(),
            "dismissed".to_string(),
            None,
            None,
        )
        .await
        .expect("resolve reopened card");
    assert_eq!(out["outcome"], json!("dismissed"), "new outcome recorded");
    assert_eq!(resolved_notices(&svc, &id, "tc-1").await, 2);
    let s = session(&svc, &id).await;
    assert!(s.pending_proposals_raw().is_empty());
    assert_eq!(
        s.proposal_resolutions().get("tc-1"),
        Some(&json!("dismissed"))
    );
}

/// Recording an unrelated proposal rewrites the pending list without the
/// legacy contradictory entry, and leaves its recorded outcome in place.
#[tokio::test]
async fn recording_other_proposal_drops_legacy_contradictory_entry() {
    let (_t, svc, ws) = setup().await;
    let id = create_agent(&svc, &ws, "Proposer").await;
    record(&svc, &ws, &id, &["tc-1"]).await;
    seed_key(
        &svc,
        &ws,
        &id,
        intent_core::PROPOSAL_RESOLUTIONS_KEY,
        json!({ "tc-1": "dismissed" }),
    )
    .await;

    record(&svc, &ws, &id, &["tc-2"]).await;
    let s = session(&svc, &id).await;
    assert_eq!(raw_ids(&s), ["tc-2"]);
    assert_eq!(
        Value::Object(s.proposal_resolutions()),
        json!({ "tc-1": "dismissed" })
    );
}
