//! Quotas across the processes of one Postgres tenant (stateless replicas
//! groundwork): two `Services`, each on its own unclaimed store of the
//! tenant as two daemons would run, never admit more than one quota between
//! them. A PR-monitor registration counts the agent's active monitors and
//! writes under the agent's quota row lock; an `invite.challenge` counts the
//! outstanding nonces and inserts under the tenant's.
//!
//! Needs `--features postgres` and `INTENT_STORE_TEST_PG_URL` (the
//! `store-postgres` CI job sets both); skipped without the URL unless
//! `INTENT_STORE_REQUIRE_PG=1`.
#![cfg(feature = "postgres")]

use std::sync::Arc;
use std::time::Duration;

use intent_core::{
    with_caller, AgentId, AgentSession, AgentStatus, Caller, Error, InviteErrorKind, InviteScope,
    PrMonitor, PrMonitorId, PrMonitorState, RepositoryProvider, RepositoryResourceKind,
    RepositoryTarget, ReviewTarget, Workspace, WorkspaceApi, WorkspaceId,
};
use intent_services::test_support::stub_forge::{GetPrPark, StubForge};
use intent_services::{Services, SettingsRegistry};
use intent_store::postgres::{self, PgPool, PostgresOptions, TenantId};
use intent_store::{MonitorTargetProvenance, OrchestratorOnly, Store};
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
    let pool = postgres::connect(&url, 16).await.expect("connect");
    postgres::migrate(&pool).await.expect("migrate");
    Some(pool)
}

/// An unclaimed store of `tenant` with its own process-local state, as if
/// opened by the daemon `process`.
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

fn tenant(prefix: &str) -> String {
    format!(
        "{prefix}-{}",
        &uuid::Uuid::new_v4().simple().to_string()[..12]
    )
}

/// A workspace of repository `o/r` and its agent `owner`.
async fn seed(store: &Store, owner: &AgentId) -> WorkspaceId {
    let id = WorkspaceId::new();
    let now = intent_core::now_iso();
    store
        .insert_workspace(&Workspace {
            id: id.clone(),
            title: "quota".into(),
            branch: "main".into(),
            created_at: now.clone(),
            updated_at: now.clone(),
            repository_owner: Some("o".into()),
            repository_name: Some("r".into()),
            ..Workspace::default()
        })
        .await
        .expect("seed the workspace");
    store
        .insert_agent_session(&AgentSession {
            id: owner.clone(),
            workspace_id: id.clone(),
            name: "Owner".into(),
            name_explicitly_set: true,
            status: AgentStatus::Active,
            created_at: now.clone(),
            updated_at: now,
            ..AgentSession::default()
        })
        .await
        .expect("seed the agent");
    id
}

/// One process's services: its own parked forge, the shared quota setting.
fn process(store: Store, settings: &Arc<SettingsRegistry>) -> (Services, Arc<GetPrPark>) {
    let park = Arc::new(GetPrPark::default());
    let forge = Arc::new(StubForge {
        get_pr_park: Some(Arc::clone(&park)),
        ..StubForge::default()
    });
    let services = Services::new_isolated(store)
        .with_source_control(forge)
        .with_settings_registry(Arc::clone(settings));
    (services, park)
}

fn monitor_quota(dir: &tempfile::TempDir, max: u32) -> Arc<SettingsRegistry> {
    let path = dir.path().join("settings.toml");
    std::fs::write(&path, format!("[prMonitor]\nmaxPerAgent = {max}\n")).unwrap();
    Arc::new(SettingsRegistry::load(path).unwrap())
}

async fn active_monitors(store: &Store, agent: &AgentId) -> usize {
    store
        .list_active_pr_monitors_by_agent(agent)
        .await
        .unwrap()
        .len()
}

async fn parked(park: &GetPrPark) {
    tokio::time::timeout(
        test_timeout(Duration::from_secs(30)),
        park.entered.notified(),
    )
    .await
    .expect("the registration reaches its forge read");
}

/// The one outcome of two registrations that both passed the early count:
/// one registered, the other refused at the cap by its write.
#[track_caller]
fn assert_one_admitted<T: std::fmt::Debug>(
    left: intent_core::Result<T>,
    right: intent_core::Result<T>,
) {
    let admitted = [&left, &right].iter().filter(|r| r.is_ok()).count();
    assert_eq!(
        admitted, 1,
        "one registration takes the last slot: {left:?} / {right:?}"
    );
    let refused = [left, right].into_iter().find(Result::is_err).unwrap();
    assert!(
        matches!(&refused, Err(Error::InvalidParams(m)) if m.contains("max 1")),
        "the other is refused at the cap: {refused:?}"
    );
}

/// Both processes register a different PR for one agent with one slot left.
/// Both count zero monitors before their forge read (held there), then both
/// write: only one write lands, so the agent never holds more than its cap.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_processes_never_register_past_the_agent_cap() {
    let Some(pool) = pool().await else { return };
    let tenant = tenant("quota-monitor");
    let store_a = open(&pool, &tenant, "process-a").await;
    let store_b = open(&pool, &tenant, "process-b").await;
    let owner = AgentId::from("agent-quota");
    let ws = seed(&store_a, &owner).await;
    let dir = tempfile::tempdir().unwrap();
    let settings = monitor_quota(&dir, 1);
    let (a, park_a) = process(store_a, &settings);
    let (b, park_b) = process(store_b.clone(), &settings);

    let run_a = tokio::spawn({
        let (ws, owner) = (ws.clone(), owner.clone());
        async move { a.pr_monitor_register(&ws, &owner, "o", "r", 1).await }
    });
    let run_b = tokio::spawn({
        let (ws, owner) = (ws.clone(), owner.clone());
        async move { b.pr_monitor_register(&ws, &owner, "o", "r", 2).await }
    });
    // Both passed the early count and wait in their forge read.
    parked(&park_a).await;
    parked(&park_b).await;
    park_a.release.notify_one();
    park_b.release.notify_one();
    assert_one_admitted(run_a.await.unwrap(), run_b.await.unwrap());
    assert_eq!(active_monitors(&store_b, &owner).await, 1);
}

/// One process adopts an orphaned monitor for the agent while the other
/// registers a new PR for it, with one slot left: an adoption counts
/// against the adopter's cap in the same write, so only one lands.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_adoption_and_a_registration_never_pass_the_agent_cap() {
    let Some(pool) = pool().await else { return };
    let tenant = tenant("quota-adopt");
    let store_a = open(&pool, &tenant, "process-a").await;
    let store_b = open(&pool, &tenant, "process-b").await;
    let owner = AgentId::from("agent-quota");
    let ws = seed(&store_a, &owner).await;
    // PR 1's monitor belongs to an agent that finished: an orphan.
    let now = intent_core::now_iso();
    store_a
        .insert_agent_session(&AgentSession {
            id: AgentId::from("agent-gone"),
            workspace_id: ws.clone(),
            name: "Gone".into(),
            status: AgentStatus::Completed,
            created_at: now.clone(),
            updated_at: now.clone(),
            ..AgentSession::default()
        })
        .await
        .expect("seed the finished agent");
    let orphan = PrMonitor {
        monitor_id: PrMonitorId::new(),
        workspace_id: ws.clone(),
        agent_id: AgentId::from("agent-gone"),
        repo_owner: "o".into(),
        repo_name: "r".into(),
        pr_number: 1,
        state: PrMonitorState::Active,
        last_snapshot: None,
        baseline_snapshot: None,
        pending_changes: Vec::new(),
        pending_since: None,
        last_change_at: None,
        last_polled_at: None,
        last_error: None,
        created_at: now.clone(),
        updated_at: now,
    };
    let target = ReviewTarget {
        repository: RepositoryTarget {
            provider: RepositoryProvider::Github,
            instance_base_url: "https://github.com".into(),
            project_path: "o/r".into(),
        },
        kind: RepositoryResourceKind::PullRequest,
        number: 1,
    };
    assert!(store_a
        .insert_qualified_pr_monitor(
            &orphan,
            &target,
            MonitorTargetProvenance::LegacyGithubWriter
        )
        .await
        .unwrap());
    let dir = tempfile::tempdir().unwrap();
    let settings = monitor_quota(&dir, 1);
    let (a, park_a) = process(store_a, &settings);
    let (b, park_b) = process(store_b.clone(), &settings);

    let adopt = tokio::spawn({
        let (ws, owner) = (ws.clone(), owner.clone());
        async move { a.pr_monitor_register(&ws, &owner, "o", "r", 1).await }
    });
    let register = tokio::spawn({
        let (ws, owner) = (ws.clone(), owner.clone());
        async move { b.pr_monitor_register(&ws, &owner, "o", "r", 2).await }
    });
    parked(&park_a).await;
    parked(&park_b).await;
    park_a.release.notify_one();
    park_b.release.notify_one();
    assert_one_admitted(adopt.await.unwrap(), register.await.unwrap());
    assert_eq!(active_monitors(&store_b, &owner).await, 1);
}

/// Issue challenges on one invite from both processes: they share the
/// invite's outstanding-nonce cap, so the second process is refused once
/// the first spent it, and a nonce one process issued is seen by the other.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_processes_share_the_invite_nonce_cap() {
    let Some(pool) = pool().await else { return };
    let tenant = tenant("quota-nonce");
    let store_a = open(&pool, &tenant, "process-a").await;
    let store_b = open(&pool, &tenant, "process-b").await;
    let owner = AgentId::from("agent-quota");
    let ws = seed(&store_a, &owner).await;
    let a = Services::new_isolated(store_a).with_source_control(Arc::new(StubForge::default()));
    let b = Services::new_isolated(store_b).with_source_control(Arc::new(StubForge::default()));
    let created = with_caller(
        Caller::Daemon,
        a.workspace_invite_create(ws.clone(), None, None),
    )
    .await
    .expect("create the invite");
    let id = created["invite"]["id"].as_str().unwrap().to_string();
    let secret = created["secret"].as_str().unwrap().to_string();
    async fn challenge(svc: &Services, id: &str, secret: &str) -> intent_core::Result<()> {
        svc.invite_challenge(id.into(), secret.into(), InviteScope::Workspace)
            .await
            .map(drop)
    }
    let mut issued = 0;
    while challenge(&a, &id, &secret).await.is_ok() {
        issued += 1;
        assert!(issued <= 64, "process A's challenges are capped");
    }
    assert!(issued > 0);
    assert!(
        matches!(
            challenge(&b, &id, &secret).await,
            Err(Error::Invite(InviteErrorKind::FlowBusy))
        ),
        "process B is refused: the invite's cap is spent across both"
    );
}
