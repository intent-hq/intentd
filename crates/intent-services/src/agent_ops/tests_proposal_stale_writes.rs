//! Proposal outcomes are daemon-owned session metadata: a whole-session
//! write from a snapshot read before the resolution (a busy agent's rename,
//! a completion report) cannot bring the card back as pending or erase the
//! recorded outcome and created workspace.

use intent_core::{now_iso, WorkspaceId};
use serde_json::{json, Value};

use super::tests::{create_agent, proposal_blocks, setup};

#[tokio::test]
async fn stale_whole_session_writes_keep_proposal_outcomes() {
    let (_t, svc, ws) = setup().await;
    let agent = create_agent(&svc, &ws, "Proposer").await;
    for full_row in [true, false] {
        let case = format!("full_row={full_row}");
        let resolved = format!("resolved-{full_row}");
        let created = WorkspaceId::from(format!("ws-created-{full_row}"));
        let msg = svc
            .store()
            .append_agent_message(&agent, "assistant", &proposal_blocks(&resolved), &now_iso())
            .await
            .expect("append proposal");
        assert!(
            svc.record_pending_proposals(&ws, &agent, &msg.id, std::slice::from_ref(&resolved))
                .await
        );
        let stale = svc
            .store()
            .get_agent_session_with_transcript(&agent)
            .await
            .expect("snapshot");
        svc.agent_resolve_proposal_op(
            ws.clone(),
            agent.clone(),
            resolved.clone(),
            "applied".to_string(),
            None,
            Some(created.clone()),
        )
        .await
        .expect("resolve");

        if full_row {
            svc.store()
                .update_agent_session(&ws, &stale)
                .await
                .expect("whole-row write");
        } else {
            svc.store()
                .update_agent_session_metadata(&ws, &agent, stale.metadata.as_ref(), &now_iso())
                .await
                .expect("whole-metadata write");
        }

        let lite = svc
            .agent_get_op(agent.clone(), Some(ws.clone()))
            .await
            .expect("agent.get");
        assert!(
            lite.metadata
                .pending_proposals
                .iter()
                .all(|p| p.proposal_id != resolved),
            "{case}: card came back as pending"
        );
        assert_eq!(
            lite.metadata.proposal_resolutions.get(&resolved),
            Some(&json!("applied")),
            "{case}"
        );
        assert_eq!(
            lite.metadata.proposal_created_workspaces.get(&resolved),
            Some(&Value::String(created.0.clone())),
            "{case}"
        );
        let stored = svc
            .store()
            .get_agent_session_with_transcript(&agent)
            .await
            .expect("session");
        assert!(
            stored
                .pending_proposals_raw()
                .iter()
                .all(|p| p.proposal_id != resolved),
            "{case}: stale pending entry written back"
        );
    }
}
