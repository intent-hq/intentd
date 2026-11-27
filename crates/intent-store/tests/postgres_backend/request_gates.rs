//! Read-modify-write gates that request writes rely on, converted from a
//! process-local lock to a check inside the write's own transaction (see
//! docs/ARCHITECTURE.md, "Owner work vs request writes", gate conversion
//! table). Each test opens two handles on one Postgres tenant through the
//! `*_as_process` seams: only the database orders their writes, as between
//! two daemons, so the process-local gate of each handle sees nothing of
//! the other.

use intent_core::{
    AgentId, AgentSession, Principal, PrincipalId, PrincipalIdentity, Workspace, WorkspaceId,
};
use intent_store::{AgentQueueRow, Store};
use serde_json::json;

use crate::harness::{open_as_process, test_db, TestDb};

const T1: &str = "2026-11-27T00:00:01.000Z";
const TENANT: &str = "rmw";

fn pid(id: &str) -> PrincipalId {
    PrincipalId(id.into())
}

fn person(id: &str, github: i64) -> Principal {
    let identity = PrincipalIdentity::github(github);
    Principal {
        id: pid(id),
        github_user_id: identity.github_user_id(),
        identity: Some(identity),
        login: Some(id.into()),
        display_name: Some(id.into()),
        avatar_url: None,
        is_primary: false,
        created_at: T1.into(),
        updated_at: T1.into(),
    }
}

async fn two_processes(db: &TestDb) -> (Store, Store) {
    (
        open_as_process(db, TENANT, "process-a").await,
        open_as_process(db, TENANT, "process-b").await,
    )
}

// ---- human_instruction_authority -------------------------------------------

fn agent() -> AgentId {
    AgentId("agent-q".into())
}

/// A queued instruction; `from` stamps `messageMetadata.fromPrincipalId`.
fn queued(id: &str, from: Option<&str>) -> AgentQueueRow {
    let payload = match from {
        Some(from) => json!({ "text": id, "messageMetadata": { "fromPrincipalId": from } }),
        None => json!({ "text": id }),
    };
    AgentQueueRow {
        id: id.into(),
        agent_id: agent(),
        position: 0,
        payload,
        created_at: T1.into(),
        turn_id: format!("turn-{id}"),
    }
}

fn ids(rows: &[AgentQueueRow]) -> Vec<&str> {
    rows.iter().map(|r| r.id.as_str()).collect()
}

async fn stored_ids(store: &Store) -> Vec<String> {
    store
        .load_all_agent_queues()
        .await
        .unwrap()
        .into_iter()
        .filter(|r| r.agent_id == agent())
        .map(|r| r.id)
        .collect()
}

/// A workspace, an agent with a session row, and carol, a non-primary
/// principal whose instructions are queued.
async fn seed_queue(store: &Store) {
    let ws = WorkspaceId("ws-q".into());
    store
        .insert_workspace(&Workspace {
            id: ws.clone(),
            title: "queue".into(),
            branch: "main".into(),
            created_at: T1.into(),
            updated_at: T1.into(),
            ..Default::default()
        })
        .await
        .unwrap();
    store
        .insert_agent_session(&AgentSession {
            id: agent(),
            workspace_id: ws,
            name: "queue agent".into(),
            created_at: T1.into(),
            updated_at: T1.into(),
            ..AgentSession::default()
        })
        .await
        .unwrap();
    store
        .upsert_principal(&person("carol", 7001))
        .await
        .unwrap();
}

// Failure mode 3: a removal deletes the removed principal's queue rows in
// its transaction. Another process that still holds them (its seen queue
// version unchanged) must learn of it on refresh, and must not write them
// back on its next queue write.
#[tokio::test]
async fn a_removal_in_one_process_drops_the_instruction_another_process_holds() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let (a, b) = two_processes(&db).await;
    seed_queue(&a).await;
    b.load_all_agent_queues().await.unwrap();
    let held = [queued("q-carol", Some("carol")), queued("q-own", None)];
    b.sync_agent_queue(&agent(), &held, &[]).await.unwrap();

    a.revoke_principal_access(&pid("carol")).await.unwrap();
    assert_eq!(stored_ids(&a).await, ["q-own"]);

    let refreshed = b
        .refresh_agent_queue(&agent(), &held)
        .await
        .unwrap()
        .expect("the removal changed the stored queue: B must refresh");
    assert_eq!(ids(&refreshed), ["q-own"]);
}

#[tokio::test]
async fn a_stale_queue_write_cannot_resurrect_a_removed_principals_instruction() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let (a, b) = two_processes(&db).await;
    seed_queue(&a).await;
    b.load_all_agent_queues().await.unwrap();
    let held = [queued("q-carol", Some("carol")), queued("q-own", None)];
    b.sync_agent_queue(&agent(), &held, &[]).await.unwrap();

    a.revoke_principal_access(&pid("carol")).await.unwrap();

    // B never refreshed: it appends to the queue it still holds.
    let mut next = held.to_vec();
    next.push(queued("q-next", None));
    let sync = b.sync_agent_queue(&agent(), &next, &[]).await.unwrap();
    assert_eq!(stored_ids(&a).await, ["q-own", "q-next"]);
    let merged = sync
        .merged
        .expect("B's memory must drop carol's instruction");
    assert_eq!(ids(&merged), ["q-own", "q-next"]);
}

// Failure mode 4: B admitted carol's instruction before A removed her, and
// persists it after. It was never stored, so the three-way merge sees a
// local add; the queue write itself must refuse it.
#[tokio::test]
async fn an_instruction_admitted_before_a_removal_is_not_persisted_after_it() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let (a, b) = two_processes(&db).await;
    seed_queue(&a).await;
    b.load_all_agent_queues().await.unwrap();
    b.sync_agent_queue(&agent(), &[queued("q-own", None)], &[])
        .await
        .unwrap();

    a.revoke_principal_access(&pid("carol")).await.unwrap();

    let admitted = [queued("q-own", None), queued("q-carol-late", Some("carol"))];
    let sync = b.sync_agent_queue(&agent(), &admitted, &[]).await.unwrap();
    assert_eq!(stored_ids(&a).await, ["q-own"]);
    let merged = sync
        .merged
        .expect("B's memory must drop the refused instruction");
    assert_eq!(ids(&merged), ["q-own"]);
}
