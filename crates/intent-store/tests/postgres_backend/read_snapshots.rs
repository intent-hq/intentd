//! Multi-statement reads answer from one snapshot (audit finding 7), and
//! tenant provisioning runs under the tenant writer lock (finding 6).
//!
//! Each reader test pauses the reader after its first statement
//! (`postgres::read_probe`), commits a write through the admin pool in the
//! gap, then releases it. On one snapshot the reader's answer matches the
//! state its first statement saw; on READ COMMITTED (plain `begin()`) the
//! later statements see the write and the answer is torn.
//!
//! Probes are process-wide by tenant name, so every test uses its own
//! tenant; every store runs on the leak-trapped database.

use std::time::Duration;

use intent_core::discussion::{DiscussionAuthor, DiscussionTarget, DEFAULT_CONVERSATION};
use intent_core::nodes::{
    LeaseOwner, LeaseState, NodeArch, NodeAssignment, NodeCapacity, NodeCheckpoint, NodeCounter,
    NodeKind, NodeLease, NodeOs, NodePath, NodeRecord, NodeState,
};
use intent_core::script_lists::ScriptListOptions;
use intent_core::{
    AgentId, AgentSession, Error, PrincipalId, Script, ScriptMode, ScriptPurpose, Workspace,
    WorkspaceId,
};
use intent_store::postgres::read_probe::{self, ReadPause};
use intent_store::postgres::{PostgresOptions, TenantId};
use intent_store::Store;
use intentd_test_support::test_timeout;

use crate::harness::{exec, hold_writer_lock, open_trapped, trapped_db, TestDb};

const T0: &str = "2026-01-01T00:00:00.000Z";
const WS: &str = "ws-snap";

fn ws() -> WorkspaceId {
    WorkspaceId(WS.into())
}

fn aid(id: &str) -> AgentId {
    AgentId(id.into())
}

async fn workspace(store: &Store) {
    store
        .insert_workspace(&Workspace {
            id: ws(),
            title: "Snapshots".into(),
            branch: "main".into(),
            created_at: T0.into(),
            updated_at: T0.into(),
            ..Default::default()
        })
        .await
        .unwrap();
}

async fn agent(store: &Store, id: &str) {
    store
        .insert_agent_session(&AgentSession {
            id: aid(id),
            workspace_id: ws(),
            name: id.into(),
            created_at: T0.into(),
            updated_at: T0.into(),
            ..AgentSession::default()
        })
        .await
        .unwrap();
}

async fn primary(db: &TestDb, tenant: &str) -> PrincipalId {
    let id: String =
        sqlx::query_scalar("SELECT id FROM principal WHERE tenant_id = $1 AND is_primary = 1")
            .bind(tenant)
            .fetch_one(&db.admin)
            .await
            .unwrap();
    PrincipalId(id)
}

/// Runs `read` with `reader` paused after its first statement, commits
/// `write` in the gap, then releases it.
async fn read_across<T: Send + 'static>(
    tenant: &str,
    reader: &'static str,
    write: impl std::future::Future<Output = ()>,
    read: impl std::future::Future<Output = T> + Send + 'static,
) -> T {
    let ReadPause { entered, release } = read_probe::pause(tenant, reader);
    let task = tokio::spawn(read);
    tokio::time::timeout(test_timeout(Duration::from_secs(30)), entered.notified())
        .await
        .unwrap_or_else(|_| panic!("{reader} reached its probe"));
    write.await;
    release.notify_one();
    tokio::time::timeout(test_timeout(Duration::from_secs(30)), task)
        .await
        .unwrap_or_else(|_| panic!("{reader} finished"))
        .unwrap()
}

/// [`read_across`] with admin SQL (`{t}` the tenant) as the write.
async fn read_across_write<T: Send + 'static>(
    db: &TestDb,
    tenant: &str,
    reader: &'static str,
    write: &str,
    read: impl std::future::Future<Output = T> + Send + 'static,
) -> T {
    read_across(
        tenant,
        reader,
        exec(&db.admin, write.replace("{t}", tenant)),
        read,
    )
    .await
}

#[tokio::test]
async fn reaction_state_reads_its_access_check_and_state_from_one_snapshot() {
    let Some(db) = trapped_db().await else {
        return;
    };
    let tenant = "snap-reaction";
    let store = open_trapped(&db, tenant).await;
    workspace(&store).await;
    let me = primary(&db, tenant).await;
    let author = DiscussionAuthor {
        principal_id: Some(me.clone()),
        agent_id: None,
        login: None,
        display_name: None,
        avatar_url: None,
    };
    let (sent, _) = store
        .send_discussion_message(&ws(), "m1", "react", &author, &DiscussionTarget::default())
        .await
        .unwrap();
    let message = sent.message.id.clone();
    // The actor reacts between the access check and the state read (the
    // reader holds no writer lock, so the write commits in the gap).
    let write = async {
        store
            .set_discussion_reaction(&ws(), DEFAULT_CONVERSATION, &message, &me, "👍", true)
            .await
            .unwrap();
    };
    let reader = store.clone();
    let (w, m, p) = (ws(), message.clone(), me.clone());
    let state = read_across(
        tenant,
        "discussion_actor_reaction_state",
        write,
        async move {
            reader
                .discussion_reaction_state(&w, DEFAULT_CONVERSATION, &m, &p)
                .await
        },
    )
    .await
    .unwrap();
    assert_eq!(
        state.reaction_version, 0,
        "state read after the check's snapshot"
    );
    assert!(state.reactions.is_empty(), "{:?}", state.reactions);
    // A fresh read sees the write.
    let after = store
        .discussion_reaction_state(&ws(), DEFAULT_CONVERSATION, &message, &me)
        .await
        .unwrap();
    assert_eq!(after.reaction_version, 1);
    assert!(after.reactions[0].reacted);
}

/// A workspace conversation `c1`, agent `agent-a`, and with `grant` its
/// grant on `c1`.
async fn conversation_and_agent(db: &TestDb, store: &Store, tenant: &str, grant: bool) {
    workspace(store).await;
    agent(store, "agent-a").await;
    exec(
        &db.admin,
        format!(
            "INSERT INTO workspace_discussion_conversation (tenant_id, workspace_id, id, name, created_at) \
             VALUES ('{tenant}', '{WS}', 'c1', 'one', '{T0}')"
        ),
    )
    .await;
    if grant {
        exec(
            &db.admin,
            format!(
                "INSERT INTO discussion_agent_grant (tenant_id, workspace_id, conversation_id, agent_id) \
                 VALUES ('{tenant}', '{WS}', 'c1', 'agent-a')"
            ),
        )
        .await;
    }
}

const DELETE_C1: &str =
    "UPDATE workspace_discussion_conversation SET deleted_at = '2026-01-02T00:00:00.000Z' \
     WHERE tenant_id = '{t}' AND id = 'c1'";

#[tokio::test]
async fn agent_conversations_list_the_granted_ids_and_their_rows_from_one_snapshot() {
    let Some(db) = trapped_db().await else {
        return;
    };
    let tenant = "snap-agent-conversations";
    let store = open_trapped(&db, tenant).await;
    conversation_and_agent(&db, &store, tenant, true).await;
    // The conversation is deleted after its id was listed: on READ COMMITTED
    // the row read then finds it deleted and the whole page fails.
    let reader = store.clone();
    let page = read_across_write(
        &db,
        tenant,
        "discussion_agent_conversations",
        DELETE_C1,
        async move {
            reader
                .discussion_agent_conversations(&ws(), &aid("agent-a"), None, 10)
                .await
        },
    )
    .await
    .expect("the page lists c1 as of its snapshot");
    assert_eq!(page["conversations"][0]["id"], "c1");
    assert_eq!(page["conversations"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn agent_access_reads_the_conversation_and_grant_from_one_snapshot() {
    let Some(db) = trapped_db().await else {
        return;
    };
    let tenant = "snap-agent-access";
    let store = open_trapped(&db, tenant).await;
    conversation_and_agent(&db, &store, tenant, false).await;
    // Between the conversation check and the grant check, the agent gets a
    // grant and the conversation is deleted. Access was never allowed (no
    // grant while the conversation lived); a torn read says it is.
    let write = format!(
        "INSERT INTO discussion_agent_grant (tenant_id, workspace_id, conversation_id, agent_id) \
         VALUES ('{{t}}', '{WS}', 'c1', 'agent-a'); {DELETE_C1}"
    );
    let reader = store.clone();
    let allowed = read_across_write(&db, tenant, "discussion_agent_access", &write, async move {
        reader
            .discussion_agent_access(&ws(), "c1", &aid("agent-a"))
            .await
    })
    .await
    .unwrap();
    assert!(!allowed, "no grant existed while c1 was live");
}

#[tokio::test]
async fn subscription_projections_read_sessions_and_hooks_from_one_snapshot() {
    let Some(db) = trapped_db().await else {
        return;
    };
    let tenant = "snap-projections";
    let store = open_trapped(&db, tenant).await;
    workspace(&store).await;
    agent(&store, "agent-a").await;
    // A hook is scheduled for the agent after its session row was read.
    let write = format!(
        "INSERT INTO hook (tenant_id, hook_id, workspace_id, agent_id, name, code, delay_ms, \
         state, created_at) VALUES ('{{t}}', 'h1', '{WS}', 'agent-a', 'late', 'x', 1, \
         'scheduled', '{T0}')"
    );
    let reader = store.clone();
    let rows = read_across_write(
        &db,
        tenant,
        "get_subscription_agent_projections",
        &write,
        async move {
            reader
                .get_subscription_agent_projections(&[aid("agent-a")])
                .await
        },
    )
    .await
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(
        rows[0].waiting_on_hooks.is_empty(),
        "hook committed after the snapshot: {:?}",
        rows[0].waiting_on_hooks
    );
    let fresh = store
        .get_subscription_agent_projections(&[aid("agent-a")])
        .await
        .unwrap();
    assert_eq!(fresh[0].waiting_on_hooks.len(), 1);
}

#[tokio::test]
async fn review_context_enrollment_reads_both_checks_from_one_snapshot() {
    let Some(db) = trapped_db().await else {
        return;
    };
    let tenant = "snap-enrollment";
    let store = open_trapped(&db, tenant).await;
    workspace(&store).await;
    agent(&store, "agent-a").await;
    store
        .set_acp_session_id(&ws(), &aid("agent-a"), "acp-1")
        .await
        .unwrap();
    // No enrollment at the first read; the session's private family lands
    // before the second. As of the snapshot the session is not private.
    let write = "INSERT INTO review_context_private_family (tenant_id, acp_session_id, version) \
                 VALUES ('{t}', 'acp-1', 1)";
    let reader = store.clone();
    let found = read_across_write(
        &db,
        tenant,
        "get_review_context_enrollment",
        write,
        async move {
            reader
                .get_review_context_enrollment(&ws(), &aid("agent-a"), "acp-1")
                .await
        },
    )
    .await;
    assert!(
        matches!(found, Ok(None)),
        "not private as of the snapshot: {:?}",
        found.map(|e| e.is_some())
    );
    // A fresh read sees the private family.
    assert!(store
        .get_review_context_enrollment(&ws(), &aid("agent-a"), "acp-1")
        .await
        .is_err());
}

#[tokio::test]
async fn script_summary_page_reads_keys_and_rows_from_one_snapshot() {
    let Some(db) = trapped_db().await else {
        return;
    };
    let tenant = "snap-scripts";
    let store = open_trapped(&db, tenant).await;
    workspace(&store).await;
    let scripts: Vec<Script> = ["s1", "s2"]
        .into_iter()
        .map(|id| Script {
            id: id.into(),
            workspace_id: WS.into(),
            name: id.into(),
            command: "true".into(),
            cwd: None,
            env: None,
            mode: ScriptMode::Command,
            category: None,
            source: "user".into(),
            auto_start: None,
            created_at: T0.into(),
            updated_at: None,
            purpose: ScriptPurpose::Saved,
            archived_at: None,
            last_run: None,
            remote_author: None,
        })
        .collect();
    store.upsert_scripts(&scripts).await.unwrap();
    // A script on the page is archived after the page's keys were read.
    let write = "UPDATE script SET archived_at = '2026-01-02T00:00:00.000Z' \
                 WHERE tenant_id = '{t}' AND id = 's1'";
    let reader = store.clone();
    let options = ScriptListOptions {
        summary: true,
        limit: 10,
        cursor: None,
    };
    let page = read_across_write(&db, tenant, "script_summary_page", write, async move {
        reader
            .script_summary_page(&ws(), None, false, &options)
            .await
    })
    .await
    .unwrap();
    let names: Vec<&str> = page["scripts"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["name"].as_str().unwrap())
        .collect();
    assert_eq!(names.len(), 2, "both keys keep their rows: {names:?}");
}

#[tokio::test]
async fn discussion_conversation_reads_its_fallback_chain_from_one_snapshot() {
    let Some(db) = trapped_db().await else {
        return;
    };
    let tenant = "snap-conversation";
    let store = open_trapped(&db, tenant).await;
    workspace(&store).await;
    exec(
        &db.admin,
        format!(
            "INSERT INTO discussion_conversation (tenant_id, id, workspace_id, kind, \
             participants_json, created_at) VALUES ('{tenant}', 'p1', '{WS}', 'direct', '[]', '{T0}')"
        ),
    )
    .await;
    // The private conversation is deleted after the workspace-conversation
    // lookup missed: on READ COMMITTED the fallback read finds nothing.
    let write = "DELETE FROM discussion_conversation WHERE tenant_id = '{t}' AND id = 'p1'";
    let reader = store.clone();
    let found = read_across_write(&db, tenant, "discussion_conversation", write, async move {
        reader.discussion_conversation(&ws(), "p1").await
    })
    .await
    .expect("p1 existed as of the snapshot");
    assert_eq!((found.id.as_str(), found.kind.as_str()), ("p1", "direct"));
}

#[tokio::test]
async fn discussion_conversations_read_the_page_and_default_from_one_snapshot() {
    let Some(db) = trapped_db().await else {
        return;
    };
    let tenant = "snap-conversations";
    let store = open_trapped(&db, tenant).await;
    workspace(&store).await;
    let me = primary(&db, tenant).await;
    // The default conversation is renamed after the page was read.
    let write = format!(
        "INSERT INTO workspace_discussion_conversation (tenant_id, workspace_id, id, name, created_at) \
         VALUES ('{{t}}', '{WS}', 'default', 'renamed', '{T0}')"
    );
    let reader = store.clone();
    let page = read_across_write(
        &db,
        tenant,
        "discussion_conversations",
        &write,
        async move { reader.discussion_conversations(&ws(), &me, None, 10).await },
    )
    .await
    .unwrap();
    assert_eq!(page["conversations"][0]["id"], DEFAULT_CONVERSATION);
    assert!(
        page["conversations"][0]["name"].is_null(),
        "default as of the page's snapshot: {page}"
    );
}

/// A ready node lease and agent `agent-n` placed on it with `repo-a`.
async fn placed_agent(store: &Store, tenant: &str) -> NodeAssignment {
    workspace(store).await;
    agent(store, "agent-n").await;
    let record = NodeRecord {
        id: "node-a".into(),
        head_id: "head-1".into(),
        node_identity: "identity-node-a".into(),
        name: format!("{tenant} node"),
        kind: NodeKind::Static,
        os: NodeOs::Linux,
        arch: NodeArch::X86_64,
        state: NodeState::Offline,
        draining: false,
        capacity: NodeCapacity {
            max_agents: 4,
            reserved_agents: 0,
            memory_budget_bytes: 1 << 30,
            used_memory_bytes: 0,
            exclusive_reserved: false,
        },
        version: "1.0.0".into(),
        endpoint: Some("wss://node.example:443/node".into()),
        certificate_sha256: Some("a".repeat(64)),
        last_seen_at: None,
    };
    let initial = NodeLease {
        owner: LeaseOwner {
            head_id: record.head_id.clone(),
            node_id: record.id.clone(),
            node_identity: record.node_identity.clone(),
            lease_id: "lease-a".into(),
            incarnation: "00000000-0000-4000-8000-000000000003".into(),
        },
        state: LeaseState::Acquiring,
        created_at: T0.into(),
        last_seen_at: None,
        ack_seq: NodeCounter(0),
        link_generation: NodeCounter(0),
        release_requested: false,
        fence: None,
    };
    store.enroll_node(&record, &initial).await.unwrap();
    let ready = store
        .record_node_welcome(&initial.owner, NodeOs::Linux, NodeArch::X86_64, 4, "1.0.0")
        .await
        .unwrap();
    store
        .assign_node_run(&NodeAssignment {
            agent_id: aid("agent-n"),
            workspace_id: ws(),
            owner: ready.owner,
            run_id: "00000000-0000-4000-8100-000000000003".into(),
            assignment_epoch: NodeCounter(1),
            repo_keys: vec!["repo-a".into()],
            node_path: Some(NodePath("/checkout".into())),
            active: true,
            tombstoned: false,
            inherited_checkpoint_id: None,
            merge_target_agent_id: None,
        })
        .await
        .unwrap()
}

#[tokio::test]
async fn node_repo_authorization_reads_lease_and_assignment_from_one_snapshot() {
    let Some(db) = trapped_db().await else {
        return;
    };
    let tenant = "snap-node-repo";
    let store = open_trapped(&db, tenant).await;
    let placed = placed_agent(&store, tenant).await;
    // In one commit the lease loses its dispatch authority and the
    // assignment gains repo-b: repo-b was never granted under a live lease.
    let write = "UPDATE node_lease SET record_json = jsonb_set(record_json::jsonb, \
         '{releaseRequested}', 'true')::text WHERE tenant_id = '{t}' AND id = 'lease-a'; \
         UPDATE node_assignment SET record_json = jsonb_set(record_json::jsonb, '{repoKeys}', \
         '[\"repo-a\", \"repo-b\"]')::text WHERE tenant_id = '{t}' AND agent_id = 'agent-n'";
    let reader = store.clone();
    let expected = placed.clone();
    let result = read_across_write(&db, tenant, "authorize_node_repo", write, async move {
        reader.authorize_node_repo(&expected, "repo-b").await
    })
    .await;
    assert!(
        matches!(result, Err(Error::NotFound(_))),
        "repo-b granted on a torn read: {result:?}"
    );
}

#[tokio::test]
async fn node_checkpoint_reads_assignment_and_checkpoint_from_one_snapshot() {
    let Some(db) = trapped_db().await else {
        return;
    };
    let tenant = "snap-node-checkpoint";
    let store = open_trapped(&db, tenant).await;
    let placed = placed_agent(&store, tenant).await;
    let committed = NodeCheckpoint {
        id: "10000000-0000-4000-8000-000000000001".into(),
        agent_id: placed.agent_id.clone(),
        workspace_id: placed.workspace_id.clone(),
        owner: placed.owner.clone(),
        run_id: placed.run_id.clone(),
        assignment_epoch: placed.assignment_epoch,
        capture_revision: NodeCounter(1),
        journal_seq: NodeCounter(0),
        manifest_sha256: "c".repeat(64),
        captured_at: T0.into(),
        committed_at: String::new(),
    };
    store.next_node_capture_revision(&placed).await.unwrap();
    // A checkpoint commits after the assignment was read.
    let write = async {
        store.commit_node_checkpoint(&committed).await.unwrap();
    };
    let reader = store.clone();
    let current = read_across(tenant, "current_node_checkpoint", write, async move {
        reader.current_node_checkpoint(&aid("agent-n"), &ws()).await
    })
    .await
    .unwrap();
    assert_eq!(current, None, "checkpoint committed after the snapshot");
    assert!(store
        .current_node_checkpoint(&aid("agent-n"), &ws())
        .await
        .unwrap()
        .is_some());
}

#[tokio::test]
async fn workspace_usage_reads_sessions_and_cells_from_one_snapshot() {
    let Some(db) = trapped_db().await else {
        return;
    };
    let tenant = "snap-usage";
    let store = open_trapped(&db, tenant).await;
    workspace(&store).await;
    agent(&store, "agent-u").await;
    // A usage cell lands after the session rows were read.
    let write = "INSERT INTO agent_usage_cell (tenant_id, agent_id, model, input_tokens) \
                 VALUES ('{t}', 'agent-u', 'm1', 5)";
    let reader = store.clone();
    let rows = read_across_write(
        &db,
        tenant,
        "get_workspace_agent_usage_data",
        write,
        async move { reader.get_workspace_agent_usage_data(&ws()).await },
    )
    .await
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(rows[0].5.is_empty(), "cell committed after the snapshot");
    let fresh = store.get_workspace_agent_usage_data(&ws()).await.unwrap();
    assert_eq!(fresh[0].5.len(), 1);
}

/// A store for `tenant` with a short writer wait, so a held writer lock
/// fails it quickly instead of after the default budget.
async fn open_quick(db: &TestDb, tenant: &str, budget: Duration) -> intent_core::Result<Store> {
    let mut options = PostgresOptions {
        rls_guard: true,
        ..PostgresOptions::default()
    };
    options.writer.wait_budget = budget;
    options.writer.lock_attempt = budget.min(options.writer.lock_attempt);
    Store::open_postgres(db.app.clone(), TenantId::new(tenant).unwrap(), options).await
}

#[tokio::test]
async fn provisioning_waits_for_the_tenant_writer_lock() {
    let Some(db) = trapped_db().await else {
        return;
    };
    let tenant = "snap-provision-lock";
    // Another process holds the tenant's writer lock.
    let holder = hold_writer_lock(&db.admin, tenant).await;
    let blocked = open_quick(&db, tenant, Duration::from_millis(400)).await;
    assert!(
        matches!(blocked, Err(Error::StoreBusy(_))),
        "provisioning ran without the writer lock: {:?}",
        blocked.err()
    );
    let seeded: i64 = sqlx::query_scalar("SELECT count(*) FROM principal WHERE tenant_id = $1")
        .bind(tenant)
        .fetch_one(&db.admin)
        .await
        .unwrap();
    assert_eq!(seeded, 0, "nothing was provisioned while the lock was held");
    holder.rollback().await.unwrap();
    open_quick(&db, tenant, Duration::from_secs(15))
        .await
        .expect("provisioning runs once the lock is free");
}

#[tokio::test]
async fn concurrent_provisioning_of_one_tenant_seeds_it_once() {
    let Some(db) = trapped_db().await else {
        return;
    };
    let tenant = "snap-provision-race";
    let opens: Vec<_> = (0..8)
        .map(|_| {
            let app = db.app.clone();
            tokio::spawn(async move {
                Store::open_postgres(
                    app,
                    TenantId::new(tenant).unwrap(),
                    PostgresOptions {
                        rls_guard: true,
                        ..PostgresOptions::default()
                    },
                )
                .await
                .map(|_| ())
            })
        })
        .collect();
    for open in opens {
        tokio::time::timeout(test_timeout(Duration::from_secs(60)), open)
            .await
            .expect("open finished")
            .unwrap()
            .expect("open succeeded");
    }
    for (table, filter) in [
        ("principal", "is_primary = 1"),
        ("workspace", "id = '__chief__'"),
        ("host_membership_state", "id = 1"),
    ] {
        let n: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT count(*) FROM {table} WHERE tenant_id = $1 AND {filter}"
        )))
        .bind(tenant)
        .fetch_one(&db.admin)
        .await
        .unwrap();
        assert_eq!(n, 1, "{table} seeded once");
    }
}

#[tokio::test]
async fn transfer_table_stats_reads_its_columns_and_aggregate_from_one_snapshot() {
    let Some(db) = trapped_db().await else {
        return;
    };
    let tenant = "snap-transfer-stats";
    let store = open_trapped(&db, tenant).await;
    workspace(&store).await;
    let count = |stats: &[intent_core::transfer::TransferTableStat]| {
        stats
            .iter()
            .find(|s| s.name == "workspace_discussion_conversation")
            .map(|s| s.row_count)
            .expect("conversation table is a transfer table")
    };
    let before = count(&store.transfer_table_stats(&ws()).await.unwrap());
    // A conversation commits between the column read and the aggregate.
    let write = format!(
        "INSERT INTO workspace_discussion_conversation \
         (tenant_id, workspace_id, id, name, created_at) \
         VALUES ('{{t}}', '{WS}', 'c-gap', 'gap', '{T0}')"
    );
    let reader = store.clone();
    let stats = read_across_write(&db, tenant, "transfer_table_stats", &write, async move {
        reader.transfer_table_stats(&ws()).await
    })
    .await
    .unwrap();
    assert_eq!(
        count(&stats),
        before,
        "aggregate read after the column read's snapshot"
    );
    // A fresh read sees the write.
    let after = count(&store.transfer_table_stats(&ws()).await.unwrap());
    assert_eq!(after, before + 1);
}
