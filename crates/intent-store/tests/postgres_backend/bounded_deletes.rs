//! Bounded, resumable workspace and agent deletes on Postgres (audit
//! finding 4): a delete drains its heavy child tables in batches, each its
//! own writer transaction, so other writers of the tenant (desktop writes of
//! other workspaces included) run between batches; a delete stopped
//! part-way is listed by `pending_deletes` and finished by the next delete
//! of the same workspace or agent; and the decoy tenant, seeded with the
//! same ids, is never touched.
//!
//! Every store runs on the leak-trapped database, so a purge statement
//! missing its tenant filter fails as soon as it reaches a decoy row.

use std::sync::Arc;
use std::time::Duration;

use intent_core::{AgentId, AgentSession, AgentStatus, Error, Workspace, WorkspaceId};
use intent_store::postgres::delete_probe::{self, DeleteProbe};
use intent_store::{PendingDelete, RepositoryWorkspaceDeleteDisposition, Store};
use intentd_test_support::test_timeout;
use serde_json::json;
use tokio::sync::Notify;

use crate::harness::{admin_write, open_trapped, sqlite_store, tenant_rows, trapped_db, TestDb};

const T0: &str = "2026-01-01T00:00:00.000Z";
const WS: &str = "ws-big";
const KEEP: &str = "ws-keep";
/// Child rows per heavy table; with [`BATCH`] each table takes 3+ batches.
const ROWS: i64 = 120;
const BATCH: i64 = 50;

fn wid(id: &str) -> WorkspaceId {
    WorkspaceId(id.into())
}

fn aid(id: &str) -> AgentId {
    AgentId(id.into())
}

fn workspace(id: &str) -> Workspace {
    Workspace {
        id: wid(id),
        title: format!("title {id}"),
        branch: "main".into(),
        created_at: T0.into(),
        updated_at: T0.into(),
        ..Default::default()
    }
}

fn agent(id: &str, ws: &str) -> AgentSession {
    AgentSession {
        id: aid(id),
        workspace_id: wid(ws),
        name: id.into(),
        status: AgentStatus::Idle,
        created_at: T0.into(),
        updated_at: T0.into(),
        ..AgentSession::default()
    }
}

/// `ws-big` with agents `a1` and `a2`, [`ROWS`] messages (each with a
/// payload), queue entries and sender receipts per agent, and [`ROWS`] notes (each with a
/// version and a comment, every other one parented); `ws-keep` with agent
/// `k1` and its own messages and notes, which no delete here may touch.
async fn seed(db: &TestDb, store: &Store, tenant: &str) {
    for ws in [WS, KEEP] {
        store.insert_workspace(&workspace(ws)).await.unwrap();
    }
    for (id, ws) in [("a1", WS), ("a2", WS), ("k1", KEEP)] {
        store.insert_agent_session(&agent(id, ws)).await.unwrap();
        admin_write(
            &db.admin,
            tenant,
            format!(
                "INSERT INTO agent_message (tenant_id, id, agent_id, seq, role, content, created_at) \
                 SELECT '{tenant}', '{id}-m' || g, '{id}', g, 'user', '[]', '{T0}' \
                 FROM generate_series(1, {ROWS}) g; \
                 INSERT INTO agent_message_payload (tenant_id, message_id, agent_id, block_ordinal, \
                 kind, encoding, body) \
                 SELECT '{tenant}', '{id}-m' || g, '{id}', 0, 'tool_input', 'raw', '\\x00' \
                 FROM generate_series(1, {ROWS}) g; \
                 INSERT INTO agent_queue (tenant_id, id, agent_id, position, payload, created_at) \
                 SELECT '{tenant}', '{id}-q' || g, '{id}', g, '{{}}', '{T0}' \
                 FROM generate_series(1, {ROWS}) g; \
                 INSERT INTO agent_browser_sender (tenant_id, agent_id, submission_id, received_order) \
                 SELECT '{tenant}', '{id}', '{id}-s' || g, g::text \
                 FROM generate_series(1, {ROWS}) g;"
            ),
        )
        .await;
    }
    for ws in [WS, KEEP] {
        admin_write(
            &db.admin,
            tenant,
            format!(
                "INSERT INTO note (tenant_id, id, workspace_id, title, content, created_at, \
                 updated_at, parent_id) \
                 SELECT '{tenant}', '{ws}-n' || g, '{ws}', 't', 'c', '{T0}', '{T0}', \
                 CASE WHEN g % 2 = 0 THEN '{ws}-n1' END FROM generate_series(1, {ROWS}) g; \
                 INSERT INTO note_version (tenant_id, note_id, workspace_id, v, date, author_id, \
                 author_name, author_type, title, content) \
                 SELECT '{tenant}', '{ws}-n' || g, '{ws}', 1, '{T0}', 'a', 'a', 'human', 't', 'c' \
                 FROM generate_series(1, {ROWS}) g; \
                 INSERT INTO comment (tenant_id, id, thread_id, note_id, workspace_id, kind, \
                 content, author, author_type, anchor_json, created_at, updated_at) \
                 SELECT '{tenant}', '{ws}-c' || g, 'th', '{ws}-n' || g, '{ws}', 'note', 'c', 'a', \
                 'human', '{{}}', '{T0}', '{T0}' FROM generate_series(1, {ROWS}) g; \
                 INSERT INTO script (tenant_id, id, workspace_id, name, command, mode, source, \
                 created_at) SELECT '{tenant}', '{ws}-s' || g, '{ws}', 'n', 'c', 'command', \
                 'agent', '{T0}' FROM generate_series(1, {ROWS}) g; \
                 INSERT INTO attachments (tenant_id, id, workspace_id, file_name, size, \
                 uploaded_at, stored_path) SELECT '{tenant}', '{ws}-at' || g, '{ws}', 'f', 1, \
                 '{T0}', '/p' FROM generate_series(1, {ROWS}) g; \
                 INSERT INTO attachment_idempotency_keys (tenant_id, workspace_id, key, \
                 attachment_id, fingerprint, created_at) SELECT '{tenant}', '{ws}', 'k' || g, \
                 '{ws}-at' || g, 'f', '{T0}' FROM generate_series(1, {ROWS}) g; \
                 INSERT INTO tracked_changes (tenant_id, id, workspace_id, path, stage, status, \
                 created_at, updated_at) SELECT '{tenant}', '{ws}-tc' || g, '{ws}', 'p' || g, \
                 'unstaged', 'modified', '{T0}', '{T0}' FROM generate_series(1, {ROWS}) g;"
            ),
        )
        .await;
    }
    // Rows the session row's delete trigger or cascade would take in one
    // statement: per agent, script monitors, notification jobs, queue
    // admissions, and a discussion delivery and activity per message.
    for ws in [WS, KEEP] {
        admin_write(
            &db.admin,
            tenant,
            format!(
                "INSERT INTO workspace_discussion_message (tenant_id, id, workspace_id, sequence, \
                 client_message_id, body, author_json, created_at) \
                 SELECT '{tenant}', '{ws}-dm' || g, '{ws}', g, 'c' || g, 'b', '{{}}', '{T0}' \
                 FROM generate_series(1, {ROWS}) g;"
            ),
        )
        .await;
    }
    for (id, ws) in [("a1", WS), ("a2", WS), ("k1", KEEP)] {
        admin_write(
            &db.admin,
            tenant,
            format!(
                "INSERT INTO script_monitor (tenant_id, id, workspace_id, agent_id, script_id, \
                 run_id, state, row_json, created_at) \
                 SELECT '{tenant}', '{id}-sm' || g, '{ws}', '{id}', '{id}-sc' || g, 'r', 'active', \
                 '{{}}', '{T0}' FROM generate_series(1, {ROWS}) g; \
                 INSERT INTO script_notification_job (tenant_id, wake_id, workspace_id, agent_id, \
                 script_id, run_id, metadata, body, phase, created_at) \
                 SELECT '{tenant}', '{id}-w' || g, '{ws}', '{id}', 's', 'r', '{{}}', 'b', \
                 'pending', '{T0}' FROM generate_series(1, {ROWS}) g; \
                 INSERT INTO agent_queue_admission (tenant_id, id, agent_id, workspace_id, turn_id, \
                 payload, created_at, phase) \
                 SELECT '{tenant}', '{id}-qa' || g, '{id}', '{ws}', 't', '{{}}', '{T0}', 'pending' \
                 FROM generate_series(1, {ROWS}) g; \
                 INSERT INTO discussion_agent_delivery (tenant_id, message_id, agent_id, \
                 workspace_id) SELECT '{tenant}', '{ws}-dm' || g, '{id}', '{ws}' \
                 FROM generate_series(1, {ROWS}) g; \
                 INSERT INTO discussion_agent_activity (tenant_id, message_id, agent_id, attempt, \
                 version, state, updated_at, turn_id) \
                 SELECT '{tenant}', '{ws}-dm' || g, '{id}', 1, 1, 'completed', '{T0}', \
                 '{id}-t' || g FROM generate_series(1, {ROWS}) g;"
            ),
        )
        .await;
    }
}

/// The heavy tables of an agent (`agent_id` scoped), for the gone, kept
/// and batch checks.
const AGENT_TABLES: [&str; 9] = [
    "agent_message",
    "agent_message_payload",
    "agent_queue",
    "agent_browser_sender",
    "script_monitor",
    "script_notification_job",
    "agent_queue_admission",
    "discussion_agent_delivery",
    "discussion_agent_activity",
];

/// The heavy tables of a workspace (`workspace_id` scoped), for the
/// gone and kept checks.
const WORKSPACE_TABLES: [&str; 8] = [
    "workspace_discussion_message",
    "note",
    "note_version",
    "comment",
    "script",
    "attachments",
    "attachment_idempotency_keys",
    "tracked_changes",
];

/// Rows of `tenant` in `table` matching `filter`.
async fn count(db: &TestDb, tenant: &str, table: &str, filter: &str) -> i64 {
    sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT count(*) FROM {table} WHERE tenant_id = '{tenant}' AND ({filter})"
    )))
    .fetch_one(&db.admin)
    .await
    .expect("count rows")
}

/// Every child row of `ws-big` and its agents is gone, its record too, and
/// `ws-keep` still has all of its own.
async fn assert_workspace_gone(db: &TestDb, store: &Store, tenant: &str) {
    assert!(matches!(
        store.get_workspace(&wid(WS)).await,
        Err(Error::NotFound(_))
    ));
    for (table, filter) in [
        ("agent_session", "workspace_id = 'ws-big'"),
        ("delete_purge", "true"),
    ] {
        assert_eq!(
            count(db, tenant, table, filter).await,
            0,
            "{tenant} {table}"
        );
    }
    for table in WORKSPACE_TABLES {
        assert_eq!(
            count(db, tenant, table, "workspace_id = 'ws-big'").await,
            0,
            "{tenant} {table}"
        );
        assert_eq!(
            count(db, tenant, table, "workspace_id = 'ws-keep'").await,
            ROWS,
            "{tenant} {table} kept"
        );
    }
    for table in AGENT_TABLES {
        assert_eq!(
            count(db, tenant, table, "agent_id IN ('a1', 'a2')").await,
            0,
            "{tenant} {table}"
        );
        assert_eq!(
            count(db, tenant, table, "agent_id = 'k1'").await,
            ROWS,
            "{tenant} {table} kept"
        );
    }
}

/// A trapped database with the decoy seeded first; its rows, to compare
/// after the subject's run.
async fn with_decoy() -> Option<(TestDb, Vec<(String, String)>)> {
    let db = trapped_db().await?;
    let decoy = open_trapped(&db, "decoy").await;
    seed(&db, &decoy, "decoy").await;
    let before = tenant_rows(&db.admin, "decoy").await;
    Some((db, before))
}

async fn assert_decoy_untouched(db: &TestDb, before: &[(String, String)]) {
    assert!(
        tenant_rows(&db.admin, "decoy").await == before,
        "the subject's delete changed the decoy tenant's rows"
    );
}

/// Install a probe for `tenant`. Probes are process-wide by tenant name, so
/// every test here uses its own subject tenant.
fn probe(tenant: &str, configure: impl FnOnce(&mut DeleteProbe)) -> Arc<DeleteProbe> {
    let mut probe = DeleteProbe {
        batch_rows: Some(BATCH),
        ..DeleteProbe::default()
    };
    configure(&mut probe);
    let probe = Arc::new(probe);
    delete_probe::set(tenant, Some(probe.clone()));
    probe
}

/// A probe that pauses the purge after its first batch: returns it and the
/// (entered, release) pair.
fn pausing_probe(tenant: &str) -> (Arc<DeleteProbe>, Arc<Notify>, Arc<Notify>) {
    let (entered, release) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
    let probe = probe(tenant, |p| {
        p.pause_after_first_batch = std::sync::Mutex::new(Some((entered.clone(), release.clone())));
    });
    (probe, entered, release)
}

async fn until_paused(entered: &Notify) {
    tokio::time::timeout(test_timeout(Duration::from_secs(30)), entered.notified())
        .await
        .expect("the purge ran a first batch");
}

/// How many batches the probe saw on `table`.
fn batches_on(probe: &DeleteProbe, table: &str) -> usize {
    probe.batches().iter().filter(|(t, _)| t == table).count()
}

fn spawn_delete(
    store: &Store,
) -> tokio::task::JoinHandle<intent_store::RepositoryWorkspaceDeleteOutcome> {
    let store = store.clone();
    tokio::spawn(async move { store.delete_workspace_with_outcome(&wid(WS)).await })
}

#[tokio::test]
async fn a_workspace_delete_drains_its_children_in_bounded_batches() {
    const SUBJECT: &str = "bd-ws-batches";
    let Some((db, decoy)) = with_decoy().await else {
        return;
    };
    let store = open_trapped(&db, SUBJECT).await;
    seed(&db, &store, SUBJECT).await;
    let probe = probe(SUBJECT, |_| {});

    let outcome = store.delete_workspace_with_outcome(&wid(WS)).await;
    delete_probe::set(SUBJECT, None);
    outcome.result.unwrap();
    assert_eq!(
        outcome.disposition,
        RepositoryWorkspaceDeleteDisposition::Committed
    );

    // 2 agents x 120 rows in batches of 50: 3 batches per agent.
    for table in AGENT_TABLES {
        assert_eq!(
            batches_on(&probe, table),
            6,
            "{table}: {:?}",
            probe.batches()
        );
    }
    for table in WORKSPACE_TABLES {
        assert_eq!(
            batches_on(&probe, table),
            3,
            "{table}: {:?}",
            probe.batches()
        );
    }
    assert!(
        probe.batches().iter().all(|(_, rows)| *rows <= 50),
        "{:?}",
        probe.batches()
    );
    assert_workspace_gone(&db, &store, SUBJECT).await;
    assert_decoy_untouched(&db, &decoy).await;
}

#[tokio::test]
async fn an_agent_delete_drains_its_children_in_bounded_batches() {
    const SUBJECT: &str = "bd-agent-batches";
    let Some((db, decoy)) = with_decoy().await else {
        return;
    };
    let store = open_trapped(&db, SUBJECT).await;
    seed(&db, &store, SUBJECT).await;
    let probe = probe(SUBJECT, |_| {});

    let removed = store.delete_agent_session(&wid(WS), &aid("a1")).await;
    delete_probe::set(SUBJECT, None);
    assert!(removed.unwrap());

    for table in AGENT_TABLES {
        assert_eq!(
            batches_on(&probe, table),
            3,
            "{table}: {:?}",
            probe.batches()
        );
    }
    for (table, filter, left) in [
        ("agent_session", "id = 'a1'", 0),
        ("delete_purge", "true", 0),
    ] {
        assert_eq!(count(&db, SUBJECT, table, filter).await, left, "{table}");
    }
    for table in AGENT_TABLES {
        assert_eq!(
            count(&db, SUBJECT, table, "agent_id = 'a1'").await,
            0,
            "{table}"
        );
        assert_eq!(
            count(&db, SUBJECT, table, "agent_id = 'a2'").await,
            ROWS,
            "{table}"
        );
    }
    assert!(!store
        .delete_agent_session(&wid(WS), &aid("a1"))
        .await
        .unwrap());
    assert_decoy_untouched(&db, &decoy).await;
}

/// The one-shot delete held the writer lock for every child row, so this
/// write waited for the whole delete; now it runs between two batches.
#[tokio::test]
async fn a_writer_of_the_same_tenant_runs_between_purge_batches() {
    const SUBJECT: &str = "bd-interleave";
    let Some((db, decoy)) = with_decoy().await else {
        return;
    };
    let store = open_trapped(&db, SUBJECT).await;
    seed(&db, &store, SUBJECT).await;
    let (probe, entered, release) = pausing_probe(SUBJECT);

    let deleting = spawn_delete(&store);
    until_paused(&entered).await;
    // The delete is between batches: another writer of the tenant commits.
    tokio::time::timeout(
        test_timeout(Duration::from_secs(5)),
        store.set_setting("userRules", "\"x\""),
    )
    .await
    .expect("a writer ran while the delete was between batches")
    .unwrap();
    store
        .insert_agent_session(&agent("k2", KEEP))
        .await
        .unwrap();
    assert_eq!(probe.batches().len(), 1);
    release.notify_one();

    let outcome = deleting.await.unwrap();
    delete_probe::set(SUBJECT, None);
    outcome.result.unwrap();
    assert_eq!(
        outcome.disposition,
        RepositoryWorkspaceDeleteDisposition::Committed
    );
    assert!(probe.batches().len() > 1);
    assert_eq!(
        store.get_setting("userRules").await.unwrap().as_deref(),
        Some("\"x\"")
    );
    assert_workspace_gone(&db, &store, SUBJECT).await;
    assert_decoy_untouched(&db, &decoy).await;
}

/// Children and a session added to the doomed workspace while it drains
/// are gone once the delete commits: the late session is drained and
/// deleted, the late children swept by the final transaction.
#[tokio::test]
async fn rows_added_while_a_workspace_drains_go_with_it() {
    const SUBJECT: &str = "bd-late-rows";
    let Some((db, decoy)) = with_decoy().await else {
        return;
    };
    let store = open_trapped(&db, SUBJECT).await;
    seed(&db, &store, SUBJECT).await;
    let (probe, entered, release) = pausing_probe(SUBJECT);

    let deleting = spawn_delete(&store);
    until_paused(&entered).await;
    store.insert_agent_session(&agent("a3", WS)).await.unwrap();
    admin_write(
        &db.admin,
        SUBJECT,
        format!(
            "INSERT INTO agent_message (tenant_id, id, agent_id, seq, role, content, created_at) \
             SELECT '{SUBJECT}', 'a3-m' || g, 'a3', g, 'user', '[]', '{T0}' \
             FROM generate_series(1, {ROWS}) g; \
             INSERT INTO note (tenant_id, id, workspace_id, title, content, created_at, \
             updated_at) VALUES ('{SUBJECT}', 'late-n', '{WS}', 't', 'c', '{T0}', '{T0}'); \
             INSERT INTO script (tenant_id, id, workspace_id, name, command, mode, source, \
             created_at) VALUES ('{SUBJECT}', 'late-s', '{WS}', 'n', 'c', 'command', 'agent', \
             '{T0}')"
        ),
    )
    .await;
    release.notify_one();

    let outcome = deleting.await.unwrap();
    delete_probe::set(SUBJECT, None);
    outcome.result.unwrap();
    assert_eq!(
        outcome.disposition,
        RepositoryWorkspaceDeleteDisposition::Committed
    );
    // a3's messages were drained in batches too, not left to the cascade.
    assert_eq!(
        batches_on(&probe, "agent_message"),
        9,
        "{:?}",
        probe.batches()
    );
    for (table, filter) in [
        ("agent_session", "id = 'a3'"),
        ("agent_message", "agent_id = 'a3'"),
        ("note", "id = 'late-n'"),
        ("script", "id = 'late-s'"),
    ] {
        assert_eq!(count(&db, SUBJECT, table, filter).await, 0, "{table}");
    }
    assert_workspace_gone(&db, &store, SUBJECT).await;
    assert_decoy_untouched(&db, &decoy).await;
}

/// The drain holds only the doomed workspace's desktop gate: another
/// workspace's desktop write and a desktop restart invalidation (which
/// takes the maintenance gate for writing) both run mid-drain, while the
/// doomed workspace's desktop writes are refused.
#[tokio::test]
async fn desktop_writes_of_other_workspaces_run_while_a_workspace_drains() {
    const SUBJECT: &str = "bd-desktop";
    let Some((db, decoy)) = with_decoy().await else {
        return;
    };
    let store = open_trapped(&db, SUBJECT).await;
    seed(&db, &store, SUBJECT).await;
    let principal = store.get_primary_principal().await.unwrap().id;
    let terminal = |store: Store, ws: &'static str, agent: &'static str, id: &'static str| {
        let principal = principal.clone();
        async move {
            store
                .desktop_insert_terminal(
                    id,
                    &wid(ws),
                    &aid(agent),
                    &json!({
                        "principalId": principal, "computerId": "comp",
                        "requestId": format!("r-{id}"),
                    }),
                    &format!("hash-{id}"),
                )
                .await
        }
    };
    let (_probe, entered, release) = pausing_probe(SUBJECT);

    let deleting = spawn_delete(&store);
    until_paused(&entered).await;
    tokio::time::timeout(
        test_timeout(Duration::from_secs(5)),
        store.desktop_invalidate_restart(),
    )
    .await
    .expect("desktop restart invalidation ran mid-drain")
    .unwrap();
    tokio::time::timeout(
        test_timeout(Duration::from_secs(5)),
        terminal(store.clone(), KEEP, "k1", "s-keep"),
    )
    .await
    .expect("another workspace's desktop write ran mid-drain")
    .unwrap();
    assert!(matches!(
        terminal(store.clone(), WS, "a1", "s-doomed").await,
        Err(Error::Forbidden(_))
    ));
    release.notify_one();

    let outcome = deleting.await.unwrap();
    delete_probe::set(SUBJECT, None);
    outcome.result.unwrap();
    assert_eq!(
        count(&db, SUBJECT, "settings", "key LIKE 'desktop.v1/%s-keep%'").await,
        1
    );
    assert_workspace_gone(&db, &store, SUBJECT).await;
    assert_decoy_untouched(&db, &decoy).await;
}

/// Stopped after one batch, a workspace delete stays half-purged (as on
/// `SQLite`) and listed by `pending_deletes`; a new open does not touch it;
/// the next delete of the workspace finishes it.
#[tokio::test]
async fn a_workspace_delete_stopped_part_way_finishes_on_the_next_delete() {
    const SUBJECT: &str = "bd-ws-resume";
    let Some((db, decoy)) = with_decoy().await else {
        return;
    };
    let store = open_trapped(&db, SUBJECT).await;
    seed(&db, &store, SUBJECT).await;
    probe(SUBJECT, |p| p.fail_after_batches = Some(1));

    let outcome = store.delete_workspace_with_outcome(&wid(WS)).await;
    delete_probe::set(SUBJECT, None);
    assert!(outcome.result.is_err());
    assert_eq!(
        outcome.disposition,
        RepositoryWorkspaceDeleteDisposition::Unknown
    );
    let half_purged = tenant_rows(&db.admin, SUBJECT).await;
    assert!(store.get_workspace(&wid(WS)).await.is_ok());
    assert_eq!(
        count(&db, SUBJECT, "agent_message_payload", "agent_id = 'a1'").await,
        ROWS - BATCH
    );
    let reopened = open_trapped(&db, SUBJECT).await;
    assert_eq!(
        reopened.pending_deletes().await.unwrap(),
        [PendingDelete::Workspace(wid(WS))]
    );
    assert!(
        tenant_rows(&db.admin, SUBJECT).await == half_purged,
        "an open changed a stopped delete's rows"
    );

    let outcome = reopened.delete_workspace_with_outcome(&wid(WS)).await;
    outcome.result.unwrap();
    assert_eq!(
        outcome.disposition,
        RepositoryWorkspaceDeleteDisposition::Committed
    );
    assert!(reopened.pending_deletes().await.unwrap().is_empty());
    assert_workspace_gone(&db, &reopened, SUBJECT).await;
    assert_decoy_untouched(&db, &decoy).await;
}

#[tokio::test]
async fn an_agent_delete_stopped_part_way_finishes_on_the_next_delete() {
    const SUBJECT: &str = "bd-agent-resume";
    let Some((db, decoy)) = with_decoy().await else {
        return;
    };
    let store = open_trapped(&db, SUBJECT).await;
    seed(&db, &store, SUBJECT).await;
    probe(SUBJECT, |p| p.fail_after_batches = Some(1));

    assert!(store
        .delete_agent_session(&wid(WS), &aid("a1"))
        .await
        .is_err());
    delete_probe::set(SUBJECT, None);
    assert_eq!(count(&db, SUBJECT, "agent_session", "id = 'a1'").await, 1);
    let reopened = open_trapped(&db, SUBJECT).await;
    assert_eq!(
        reopened.pending_deletes().await.unwrap(),
        [PendingDelete::Agent {
            workspace_id: wid(WS),
            agent_id: aid("a1"),
        }]
    );

    assert!(reopened
        .delete_agent_session(&wid(WS), &aid("a1"))
        .await
        .unwrap());
    assert!(reopened.pending_deletes().await.unwrap().is_empty());
    for (table, filter, left) in [
        ("agent_session", "id = 'a1'", 0),
        ("agent_message", "agent_id = 'a1'", 0),
        ("agent_message_payload", "agent_id = 'a1'", 0),
        ("agent_queue", "agent_id = 'a1'", 0),
        ("agent_session", "id = 'a2'", 1),
        ("agent_message", "agent_id = 'a2'", ROWS),
    ] {
        assert_eq!(count(&db, SUBJECT, table, filter).await, left, "{table}");
    }
    assert_decoy_untouched(&db, &decoy).await;
}

/// A record whose workspace or agent is already gone is dropped by the
/// next delete of it, which still reports no effect; nothing else changes.
#[tokio::test]
async fn a_record_of_a_workspace_or_agent_already_gone_is_dropped_by_its_next_delete() {
    const SUBJECT: &str = "bd-stale";
    let Some((db, decoy)) = with_decoy().await else {
        return;
    };
    let store = open_trapped(&db, SUBJECT).await;
    seed(&db, &store, SUBJECT).await;
    admin_write(
        &db.admin,
        SUBJECT,
        format!(
            "INSERT INTO delete_purge (tenant_id, kind, workspace_id, agent_id, started_at) \
             VALUES ('{SUBJECT}', 'workspace', 'ws-gone', '', 'T0'), \
             ('{SUBJECT}', 'agent', 'ws-big', 'a-gone', 'T1')"
        ),
    )
    .await;
    assert_eq!(store.pending_deletes().await.unwrap().len(), 2);
    let rows_before = tenant_rows(&db.admin, SUBJECT).await;

    let outcome = store.delete_workspace_with_outcome(&wid("ws-gone")).await;
    assert!(matches!(outcome.result, Err(Error::NotFound(_))));
    assert_eq!(
        outcome.disposition,
        RepositoryWorkspaceDeleteDisposition::NoEffect
    );
    assert!(!store
        .delete_agent_session(&wid(WS), &aid("a-gone"))
        .await
        .unwrap());
    assert!(store.pending_deletes().await.unwrap().is_empty());
    let without_records: Vec<_> = rows_before
        .into_iter()
        .filter(|(table, _)| table != "delete_purge")
        .collect();
    assert!(
        tenant_rows(&db.admin, SUBJECT).await == without_records,
        "dropping the stale records changed other rows"
    );
    assert_decoy_untouched(&db, &decoy).await;
}

/// Another workspace's retry key naming one of the doomed workspace's
/// attachments blocks the delete before any batch: nothing changes and
/// nothing is recorded, as with the one-shot delete.
#[tokio::test]
async fn a_foreign_retry_key_fails_the_delete_before_any_batch() {
    const SUBJECT: &str = "bd-foreign-key";
    let Some((db, decoy)) = with_decoy().await else {
        return;
    };
    let store = open_trapped(&db, SUBJECT).await;
    seed(&db, &store, SUBJECT).await;
    admin_write(
        &db.admin,
        SUBJECT,
        format!(
            "INSERT INTO attachment_idempotency_keys (tenant_id, workspace_id, key, \
             attachment_id, fingerprint, created_at) \
             VALUES ('{SUBJECT}', '{KEEP}', 'foreign', '{WS}-at1', 'f', '{T0}')"
        ),
    )
    .await;
    let rows_before = tenant_rows(&db.admin, SUBJECT).await;
    let probe = probe(SUBJECT, |_| {});

    let outcome = store.delete_workspace_with_outcome(&wid(WS)).await;
    delete_probe::set(SUBJECT, None);
    assert!(outcome.result.is_err());
    assert!(probe.batches().is_empty(), "{:?}", probe.batches());
    assert!(
        tenant_rows(&db.admin, SUBJECT).await == rows_before,
        "a refused delete changed rows"
    );
    assert!(store.pending_deletes().await.unwrap().is_empty());
    assert_decoy_untouched(&db, &decoy).await;
}

#[tokio::test]
async fn sqlite_has_no_pending_deletes() {
    let (store, _dir) = sqlite_store().await;
    assert!(store.pending_deletes().await.unwrap().is_empty());
}

/// The final transaction committed, so a failed lifecycle settlement of a
/// session created during the purge still reports `Committed`, with the
/// settlement's error.
#[tokio::test]
async fn a_failed_late_session_settlement_still_reports_the_commit() {
    const SUBJECT: &str = "bd-late-settle";
    let Some((db, decoy)) = with_decoy().await else {
        return;
    };
    let store = open_trapped(&db, SUBJECT).await;
    seed(&db, &store, SUBJECT).await;
    let (entered, release) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
    probe(SUBJECT, |p| {
        p.pause_after_first_batch = std::sync::Mutex::new(Some((entered.clone(), release.clone())));
        p.fail_late_settlement = true;
    });

    let deleting = spawn_delete(&store);
    until_paused(&entered).await;
    store.insert_agent_session(&agent("a3", WS)).await.unwrap();
    release.notify_one();

    let outcome = deleting.await.unwrap();
    delete_probe::set(SUBJECT, None);
    assert!(
        matches!(&outcome.result, Err(Error::Internal(m)) if m.contains("late settlement")),
        "{:?}",
        outcome.result
    );
    assert_eq!(
        outcome.disposition,
        RepositoryWorkspaceDeleteDisposition::Committed
    );
    assert_eq!(count(&db, SUBJECT, "agent_session", "id = 'a3'").await, 0);
    assert_workspace_gone(&db, &store, SUBJECT).await;
    assert_decoy_untouched(&db, &decoy).await;
}
