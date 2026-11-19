//! Bounded, resumable workspace and agent deletes on Postgres (audit
//! finding 4): a delete drains its heavy child tables in batches, each its
//! own writer transaction, so another writer of the tenant runs between
//! batches; a delete stopped part-way finishes on the next open; and the
//! decoy tenant, seeded with the same ids, is never touched.
//!
//! Every store runs on the leak-trapped database, so a purge statement
//! missing its tenant filter fails as soon as it reaches a decoy row.

use std::sync::Arc;
use std::time::Duration;

use intent_core::{AgentId, AgentSession, AgentStatus, Error, Workspace, WorkspaceId};
use intent_store::postgres::delete_probe::{self, DeleteProbe};
use intent_store::{RepositoryWorkspaceDeleteDisposition, Store};
use intentd_test_support::test_timeout;
use tokio::sync::Notify;

use crate::harness::{exec, open_trapped, tenant_rows, trapped_db, TestDb};

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
/// payload) and queue entries per agent, and [`ROWS`] notes (each with a
/// version and a comment, every other one parented); `ws-keep` with agent
/// `k1` and its own messages and notes, which no delete here may touch.
async fn seed(db: &TestDb, store: &Store, tenant: &str) {
    for ws in [WS, KEEP] {
        store.insert_workspace(&workspace(ws)).await.unwrap();
    }
    for (id, ws) in [("a1", WS), ("a2", WS), ("k1", KEEP)] {
        store.insert_agent_session(&agent(id, ws)).await.unwrap();
        exec(
            &db.admin,
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
                 FROM generate_series(1, {ROWS}) g;"
            ),
        )
        .await;
    }
    for ws in [WS, KEEP] {
        exec(
            &db.admin,
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
                 'human', '{{}}', '{T0}', '{T0}' FROM generate_series(1, {ROWS}) g;"
            ),
        )
        .await;
    }
}

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
        ("agent_message", "agent_id IN ('a1', 'a2')"),
        ("agent_message_payload", "agent_id IN ('a1', 'a2')"),
        ("agent_queue", "agent_id IN ('a1', 'a2')"),
        ("note", "workspace_id = 'ws-big'"),
        ("note_version", "workspace_id = 'ws-big'"),
        ("comment", "workspace_id = 'ws-big'"),
        ("delete_purge", "true"),
    ] {
        assert_eq!(
            count(db, tenant, table, filter).await,
            0,
            "{tenant} {table}"
        );
    }
    for (table, filter) in [
        ("agent_message", "agent_id = 'k1'"),
        ("agent_message_payload", "agent_id = 'k1'"),
        ("agent_queue", "agent_id = 'k1'"),
        ("note", "workspace_id = 'ws-keep'"),
        ("note_version", "workspace_id = 'ws-keep'"),
        ("comment", "workspace_id = 'ws-keep'"),
    ] {
        assert_eq!(
            count(db, tenant, table, filter).await,
            ROWS,
            "{tenant} {table}"
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

/// How many batches the probe saw on `table`.
fn batches_on(probe: &DeleteProbe, table: &str) -> usize {
    probe.batches().iter().filter(|(t, _)| t == table).count()
}

/// Waits (bounded) until `check` holds.
async fn eventually<F, Fut>(what: &str, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = std::time::Instant::now() + test_timeout(Duration::from_secs(30));
    while std::time::Instant::now() < deadline {
        if check().await {
            return;
        }
        // The resumed delete runs in a task the open spawned; only its rows show it.
        // timing-guard: poll interval of a bounded wait for that background task
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("timed out waiting until {what}");
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
    for table in ["agent_message", "agent_message_payload", "agent_queue"] {
        assert_eq!(
            batches_on(&probe, table),
            6,
            "{table}: {:?}",
            probe.batches()
        );
    }
    for table in ["note", "note_version", "comment"] {
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

    for table in ["agent_message", "agent_message_payload", "agent_queue"] {
        assert_eq!(
            batches_on(&probe, table),
            3,
            "{table}: {:?}",
            probe.batches()
        );
    }
    for (table, filter, left) in [
        ("agent_session", "id = 'a1'", 0),
        ("agent_message", "agent_id = 'a1'", 0),
        ("agent_message_payload", "agent_id = 'a1'", 0),
        ("agent_queue", "agent_id = 'a1'", 0),
        ("delete_purge", "true", 0),
        ("agent_message", "agent_id = 'a2'", ROWS),
        ("agent_queue", "agent_id = 'a2'", ROWS),
    ] {
        assert_eq!(count(&db, SUBJECT, table, filter).await, left, "{table}");
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
    let (entered, release) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
    let probe = probe(SUBJECT, |p| {
        p.pause_after_first_batch = std::sync::Mutex::new(Some((entered.clone(), release.clone())));
    });

    let deleting = {
        let store = store.clone();
        tokio::spawn(async move { store.delete_workspace_with_outcome(&wid(WS)).await })
    };
    tokio::time::timeout(test_timeout(Duration::from_secs(30)), entered.notified())
        .await
        .expect("the purge ran a first batch");
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

#[tokio::test]
async fn a_workspace_delete_stopped_after_one_batch_finishes_on_the_next_open() {
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
    // Stopped part-way: one batch gone, the workspace and its record left.
    assert!(store.get_workspace(&wid(WS)).await.is_ok());
    assert_eq!(
        count(&db, SUBJECT, "agent_message_payload", "agent_id = 'a1'").await,
        ROWS - BATCH
    );
    assert_eq!(
        count(&db, SUBJECT, "delete_purge", "kind = 'workspace'").await,
        1
    );

    // A new open (as after a restart) finishes it in the background.
    let reopened = open_trapped(&db, SUBJECT).await;
    eventually("the resumed delete finished", || async {
        count(&db, SUBJECT, "delete_purge", "true").await == 0
    })
    .await;
    assert_workspace_gone(&db, &reopened, SUBJECT).await;
    assert_decoy_untouched(&db, &decoy).await;
}

#[tokio::test]
async fn an_agent_delete_stopped_after_one_batch_finishes_on_the_next_open() {
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
    assert_eq!(
        count(
            &db,
            SUBJECT,
            "delete_purge",
            "kind = 'agent' AND agent_id = 'a1'"
        )
        .await,
        1
    );

    open_trapped(&db, SUBJECT).await;
    eventually("the resumed delete finished", || async {
        count(&db, SUBJECT, "delete_purge", "true").await == 0
    })
    .await;
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

/// A record whose workspace is already gone (deleted another way after a
/// crash) is dropped by the next open, and nothing else changes.
#[tokio::test]
async fn a_record_of_a_workspace_already_gone_is_dropped_on_open() {
    const SUBJECT: &str = "bd-stale";
    let Some((db, decoy)) = with_decoy().await else {
        return;
    };
    let store = open_trapped(&db, SUBJECT).await;
    seed(&db, &store, SUBJECT).await;
    exec(
        &db.admin,
        format!(
            "INSERT INTO delete_purge (tenant_id, kind, workspace_id, agent_id, started_at) \
             VALUES ('{SUBJECT}', 'workspace', 'ws-gone', '', 'T0'), \
             ('{SUBJECT}', 'agent', 'ws-big', 'a-gone', 'T0')"
        ),
    )
    .await;
    let rows_before = tenant_rows(&db.admin, SUBJECT).await;

    open_trapped(&db, SUBJECT).await;
    eventually("the stale records were dropped", || async {
        count(&db, SUBJECT, "delete_purge", "true").await == 0
    })
    .await;
    let rows_after = tenant_rows(&db.admin, SUBJECT).await;
    let without_records: Vec<_> = rows_before
        .into_iter()
        .filter(|(table, _)| table != "delete_purge")
        .collect();
    assert!(rows_after == without_records, "the open changed other rows");
    assert_decoy_untouched(&db, &decoy).await;
}
