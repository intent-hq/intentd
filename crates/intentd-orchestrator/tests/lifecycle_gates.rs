//! Multi-step lifecycle gates on a Postgres tenant shared by two processes:
//! each `Services` runs on its own store of the tenant, as two daemons
//! would. An archive fence or a retirement gate held in one blocks the same
//! one in the other until it is released; other targets stay free. Each
//! lifecycle operation that lost its lease mid-way writes nothing more, and
//! the other process then finishes it (`intent_services::lifecycle_fence`).
//!
//! Needs `--features postgres` and `INTENT_STORE_TEST_PG_URL` (the
//! `store-postgres` CI job sets both); skipped without the URL unless
//! `INTENT_STORE_REQUIRE_PG=1`.
#![cfg(feature = "postgres")]

use std::time::Duration;

use intent_core::{
    with_caller, AgentId, AgentSession, AgentStatus, Caller, Workspace, WorkspaceApi, WorkspaceId,
};
use intent_services::lifecycle_fence::{park_lifecycle_step_for_test, steps};
use intent_services::Services;
use intent_store::postgres::{self, PgPool, PostgresOptions, TenantId};
use intent_store::{
    OrchestratorOnly, Store, AGENT_RETIREMENT_SCOPE, ARCHIVE_FENCE_SCOPE, WORKSPACE_DELETION_SCOPE,
    WORKSPACE_RETIREMENT_SCOPE,
};
use intentd_test_support::test_timeout;

async fn pool() -> Option<PgPool> {
    let Ok(url) = std::env::var("INTENT_STORE_TEST_PG_URL") else {
        assert!(
            std::env::var("INTENT_STORE_REQUIRE_PG").as_deref() != Ok("1"),
            "INTENT_STORE_REQUIRE_PG=1 but INTENT_STORE_TEST_PG_URL is unset"
        );
        eprintln!("skipping: INTENT_STORE_TEST_PG_URL is unset");
        return None;
    };
    let pool = postgres::connect(&url, 8).await.expect("connect");
    postgres::migrate(&pool).await.expect("migrate");
    Some(pool)
}

async fn open(pool: &PgPool, tenant: &str, process: &str) -> Store {
    Store::open_postgres_as_process(
        pool.clone(),
        TenantId::new(tenant).unwrap(),
        PostgresOptions::default(),
        process,
        OrchestratorOnly::for_tests(),
    )
    .await
    .expect("open the tenant store")
}

/// Waits until waiters in this process found (`scope`, `key`) held at
/// least `n` times: the waiter is really waiting on the other process.
async fn wait_contended(scope: &str, key: &str, n: u64) {
    tokio::time::timeout(test_timeout(Duration::from_secs(30)), async {
        while intent_store::contended_lease_attempts_for_tests(scope, key) < n {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the waiter tries the held lease");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_archive_fence_in_one_process_blocks_the_same_fence_in_another() {
    let Some(pool) = pool().await else { return };
    let tenant = format!(
        "lifecycle-archive-{}",
        &uuid::Uuid::new_v4().simple().to_string()[..12]
    );
    let a = Services::new_isolated(open(&pool, &tenant, "process-a").await);
    let b = Services::new_isolated(open(&pool, &tenant, "process-b").await);
    let ws = WorkspaceId("ws-archive".into());

    let held = a
        .feature_archive_fence()
        .acquire(&ws)
        .await
        .expect("A takes the fence");
    let waiter = {
        let (b, ws) = (b.clone(), ws.clone());
        tokio::spawn(async move {
            let guard = b.feature_archive_fence().acquire(&ws).await;
            guard.map(drop)
        })
    };
    // Another workspace's fence is other work.
    let other = b
        .feature_archive_fence()
        .acquire(&WorkspaceId("ws-other".into()))
        .await
        .expect("another workspace is free");
    wait_contended(intent_store::ARCHIVE_FENCE_SCOPE, "ws-archive", 2).await;
    assert!(
        !waiter.is_finished(),
        "B's archive waits while A holds the fence"
    );
    drop(other);
    drop(held);
    tokio::time::timeout(test_timeout(Duration::from_secs(30)), waiter)
        .await
        .expect("B takes the fence once A released it")
        .unwrap()
        .expect("B's fence");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_retirement_in_one_process_blocks_the_same_retirement_in_another() {
    let Some(pool) = pool().await else { return };
    let tenant = format!(
        "lifecycle-retire-{}",
        &uuid::Uuid::new_v4().simple().to_string()[..12]
    );
    let a = Services::new_isolated(open(&pool, &tenant, "process-a").await);
    let b = Services::new_isolated(open(&pool, &tenant, "process-b").await);
    let agent = AgentId("agent-retiring".into());

    let gate = a.feature_agent_wake().retirement_gate(&a, &agent);
    let held = gate.lock().await.expect("A takes the retirement gate");
    let waiter = {
        let (b, agent) = (b.clone(), agent.clone());
        tokio::spawn(async move {
            let gate = b.feature_agent_wake().retirement_gate(&b, &agent);
            gate.lock().await.map(drop)
        })
    };
    let other = b
        .feature_agent_wake()
        .retirement_gate(&b, &AgentId("agent-other".into()));
    let other = other.lock().await.expect("another agent is free");
    wait_contended(intent_store::AGENT_RETIREMENT_SCOPE, "agent-retiring", 2).await;
    assert!(
        !waiter.is_finished(),
        "B's retirement waits while A holds the gate"
    );
    drop(other);
    drop(held);
    tokio::time::timeout(test_timeout(Duration::from_secs(30)), waiter)
        .await
        .expect("B takes the gate once A released it")
        .unwrap()
        .expect("B's gate");
}

/// A `Services` on `store` whose workspace folders live under `dir`, never
/// under the developer's home.
fn services(store: Store, dir: &tempfile::TempDir) -> Services {
    Services::new_isolated(store).with_workspaces_root(dir.path().join("workspaces"))
}

fn tenant(prefix: &str) -> String {
    format!(
        "{prefix}-{}",
        &uuid::Uuid::new_v4().simple().to_string()[..12]
    )
}

async fn seed_workspace(store: &Store) -> WorkspaceId {
    let id = WorkspaceId::new();
    let now = intent_core::now_iso();
    store
        .insert_workspace(&Workspace {
            id: id.clone(),
            title: "lifecycle fence".into(),
            branch: "main".into(),
            created_at: now.clone(),
            updated_at: now,
            ..Workspace::default()
        })
        .await
        .expect("seed the workspace");
    id
}

async fn seed_agent(store: &Store, ws: &WorkspaceId, id: &str) -> AgentId {
    let id = AgentId::from(id);
    let now = intent_core::now_iso();
    store
        .insert_agent_session(&AgentSession {
            id: id.clone(),
            workspace_id: ws.clone(),
            name: id.0.clone(),
            name_explicitly_set: true,
            status: AgentStatus::Idle,
            created_at: now.clone(),
            updated_at: now,
            ..AgentSession::default()
        })
        .await
        .expect("seed the agent");
    id
}

/// Expires (`scope`, `key`) of `tenant` by the database clock, as if its
/// holder's renewals had failed past the TTL.
async fn expire_lease(pool: &PgPool, tenant: &str, scope: &str, key: &str) {
    let expired = sqlx::query(
        "UPDATE lease SET expires_at = clock_timestamp() \
         WHERE tenant_id = $1 AND scope = $2 AND lease_key = $3 AND lease_id IS NOT NULL",
    )
    .bind(tenant)
    .bind(scope)
    .bind(key)
    .execute(pool)
    .await
    .unwrap()
    .rows_affected();
    assert_eq!(expired, 1, "the {scope} lease of {key} is held");
}

/// Waits until the operation `run` reaches its parked step; fails with its
/// result when it ended before reaching it.
async fn wait_parked<T: std::fmt::Debug>(
    park: &intent_services::lifecycle_fence::LifecyclePark,
    run: &mut tokio::task::JoinHandle<T>,
) {
    tokio::time::timeout(test_timeout(Duration::from_secs(60)), async {
        tokio::select! {
            () = park.entered.notified() => {}
            ended = run => panic!("the operation ended before its parked step: {ended:?}"),
        }
    })
    .await
    .expect("the operation reaches its parked step");
}

/// The stalled operation's result once released; it must end on its own
/// (a holder that kept writing could wait for the key the other process
/// holds).
async fn finish<T>(run: tokio::task::JoinHandle<T>) -> T {
    tokio::time::timeout(test_timeout(Duration::from_secs(60)), run)
        .await
        .expect("the operation that lost its lease stops")
        .unwrap()
}

fn wire_owner() -> Caller {
    Caller::Wire {
        principal_id: intent_core::PrincipalId::new(),
        host_role: intent_core::HostRole::Owner,
    }
}

fn is_lease_lost<T: std::fmt::Debug>(result: &intent_core::Result<T>) -> bool {
    matches!(result, Err(intent_core::Error::LeaseLost(_)))
}

/// A workspace deletion whose lease is lost mid-way (held right before its
/// row delete) writes nothing more: another process takes the key, the old
/// holder's delete fails with `LeaseLost` and leaves the row, and the other
/// process then finishes the delete. Once the lease lapsed, the other
/// process's producers are admitted again; the finishing delete sweeps them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_deletion_that_lost_its_lease_writes_nothing_and_another_process_finishes_it() {
    let Some(pool) = pool().await else { return };
    let tenant = tenant("lifecycle-delete-fence");
    let store_a = open(&pool, &tenant, "process-a").await;
    let store_b = open(&pool, &tenant, "process-b").await;
    let dir = tempfile::tempdir().unwrap();
    let a = services(store_a, &dir);
    let b = services(store_b.clone(), &dir);
    let ws = seed_workspace(&store_b).await;
    seed_agent(&store_b, &ws, "agent-before-delete").await;

    let park = park_lifecycle_step_for_test(steps::DELETE_BEFORE_ROW_DELETE, ws.as_str());
    let mut stale = tokio::spawn({
        let (a, ws) = (a.clone(), ws.clone());
        with_caller(Caller::Daemon, async move { a.delete_workspace(ws).await })
    });
    wait_parked(&park, &mut stale).await;
    expire_lease(&pool, &tenant, WORKSPACE_DELETION_SCOPE, ws.as_str()).await;
    // The lapsed lease no longer refuses the other process's producers.
    let late_agent = seed_agent(&store_b, &ws, "agent-after-expiry").await;
    let taken = tokio::time::timeout(
        test_timeout(Duration::from_secs(30)),
        store_b.hold_lifecycle_lease(WORKSPACE_DELETION_SCOPE, ws.as_str()),
    )
    .await
    .expect("the lapsed lease is free")
    .unwrap();

    park.release.notify_one();
    let late = finish(stale).await;
    assert!(is_lease_lost(&late), "the stale delete stops: {late:?}");
    store_b
        .get_workspace(&ws)
        .await
        .expect("the stale holder deleted nothing");
    store_b
        .get_agent_session_summary(&late_agent)
        .await
        .expect("the agent admitted after expiry is kept");

    taken.release().await;
    with_caller(Caller::Daemon, b.delete_workspace(ws.clone()))
        .await
        .expect("another process finishes the delete");
    assert!(matches!(
        store_b.get_workspace(&ws).await,
        Err(intent_core::Error::NotFound(_))
    ));
    assert!(store_b
        .list_agent_session_summaries(&ws)
        .await
        .unwrap()
        .is_empty());
}

/// An agent retire whose lease is lost right before its retired mark writes
/// nothing: the agent's retirement lease (`agent-retirement`) or the
/// last-top-level guard's (`workspace-retirement`), each taken by another
/// process meanwhile. That process then retires the agent.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_retire_that_lost_its_lease_writes_nothing_and_another_process_finishes_it() {
    let Some(pool) = pool().await else { return };
    for lost in [AGENT_RETIREMENT_SCOPE, WORKSPACE_RETIREMENT_SCOPE] {
        let tenant = tenant("lifecycle-retire-fence");
        let store_a = open(&pool, &tenant, "process-a").await;
        let store_b = open(&pool, &tenant, "process-b").await;
        let dir = tempfile::tempdir().unwrap();
        let a = services(store_a, &dir);
        let b = services(store_b.clone(), &dir);
        let ws = seed_workspace(&store_b).await;
        let agent = seed_agent(&store_b, &ws, &format!("agent-retiring-{tenant}")).await;
        seed_agent(&store_b, &ws, &format!("agent-staying-{tenant}")).await;
        let key = if lost == AGENT_RETIREMENT_SCOPE {
            agent.as_str().to_owned()
        } else {
            ws.as_str().to_owned()
        };

        let park = park_lifecycle_step_for_test(steps::RETIRE_BEFORE_MARK, agent.as_str());
        let mut stale = tokio::spawn({
            let (a, agent) = (a.clone(), agent.clone());
            with_caller(wire_owner(), async move {
                a.agent_retire_op_for_test(agent, None, None).await
            })
        });
        wait_parked(&park, &mut stale).await;
        expire_lease(&pool, &tenant, lost, &key).await;
        let taken = tokio::time::timeout(
            test_timeout(Duration::from_secs(30)),
            store_b.hold_lifecycle_lease(lost, &key),
        )
        .await
        .expect("the lapsed lease is free")
        .unwrap();

        park.release.notify_one();
        let late = finish(stale).await;
        assert!(
            is_lease_lost(&late),
            "{lost}: the stale retire stops: {late:?}"
        );
        let row = store_b.get_agent_session_summary(&agent).await.unwrap();
        assert_eq!(row.retired_at, None, "{lost}: no retired mark landed");

        taken.release().await;
        let done = with_caller(
            wire_owner(),
            b.agent_retire_op_for_test(agent.clone(), None, None),
        )
        .await
        .expect("another process retires the agent");
        assert_eq!(
            done["alreadyRetired"],
            serde_json::Value::Null,
            "{lost}: {done}"
        );
        let row = store_b.get_agent_session_summary(&agent).await.unwrap();
        assert!(row.retired_at.is_some(), "{lost}: retired");
    }
}

/// An archive whose archive-fence lease is lost right before its commit
/// writes nothing: the workspace stays active while another process holds
/// the key, and that process then archives it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_archive_that_lost_its_lease_writes_nothing_and_another_process_finishes_it() {
    let Some(pool) = pool().await else { return };
    let tenant = tenant("lifecycle-archive-fence");
    let store_a = open(&pool, &tenant, "process-a").await;
    let store_b = open(&pool, &tenant, "process-b").await;
    let dir = tempfile::tempdir().unwrap();
    let a = services(store_a, &dir);
    let b = services(store_b.clone(), &dir);
    let ws = seed_workspace(&store_b).await;

    let park = park_lifecycle_step_for_test(steps::ARCHIVE_BEFORE_COMMIT, ws.as_str());
    let mut stale = tokio::spawn({
        let (a, ws) = (a.clone(), ws.clone());
        with_caller(Caller::Daemon, async move {
            a.archive_workspace(ws, None).await
        })
    });
    wait_parked(&park, &mut stale).await;
    expire_lease(&pool, &tenant, ARCHIVE_FENCE_SCOPE, ws.as_str()).await;
    let taken = tokio::time::timeout(
        test_timeout(Duration::from_secs(30)),
        store_b.hold_lifecycle_lease(ARCHIVE_FENCE_SCOPE, ws.as_str()),
    )
    .await
    .expect("the lapsed lease is free")
    .unwrap();

    park.release.notify_one();
    let late = finish(stale).await;
    assert!(is_lease_lost(&late), "the stale archive stops: {late:?}");
    let row = store_b.get_workspace(&ws).await.unwrap();
    assert!(!row.archived, "no Archived flip landed");
    assert_eq!(row.archived_at, None);

    taken.release().await;
    let archived = with_caller(Caller::Daemon, b.archive_workspace(ws.clone(), None))
        .await
        .expect("another process archives it");
    assert!(archived.archived);
    assert!(store_b.get_workspace(&ws).await.unwrap().archived);
}
