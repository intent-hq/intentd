//! Read-modify-write gates of request writes on a Postgres tenant shared by
//! two processes (docs/ARCHITECTURE.md, "Owner work vs request writes"):
//! each `Services` runs on its own unclaimed store of the tenant, as two
//! daemons would, so one process's in-memory gate does not order the other's
//! writes. A test seam parks process A right before its store write, after
//! its own checks; process B commits a conflicting change in that window;
//! A's write must then refuse instead of committing on a stale check.
//!
//! Needs `--features postgres` and `INTENT_STORE_TEST_PG_URL` (the
//! `store-postgres` CI job sets both); skipped without the URL unless
//! `INTENT_STORE_REQUIRE_PG=1`.
#![cfg(feature = "postgres")]

use std::sync::Arc;

use intent_core::{
    now_iso, with_caller, Caller, Error, HostRole, InviteErrorKind, PrincipalId, PrincipalIdentity,
    WorkspaceApi, WorkspaceId, WorkspaceInvite,
};
use intent_services::test_support::stub_forge::StubForge;
use intent_services::Services;
use intent_sourcecontrol::UserIdentity;
use intent_store::postgres::{self, PgPool, PostgresOptions, TenantId};
use intent_store::{OrchestratorOnly, Store};
use intentd_test_support::PausePoint;

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

/// `Services` for process `store`, with a connected stub forge (minting
/// needs one) and its own GitLab secret file.
fn services(store: Store, dir: &tempfile::TempDir, name: &str) -> Services {
    Services::new_isolated(store)
        .with_gitlab_secret_store(intent_core::FileSecretStore::with_path(
            dir.path().join(format!("{name}.secrets.json")),
        ))
        .with_source_control(Arc::new(StubForge::default()))
}

fn wire(principal_id: &PrincipalId) -> Caller {
    Caller::Wire {
        principal_id: principal_id.clone(),
        host_role: HostRole::Owner,
    }
}

/// A workspace and a primary principal linked to github account 10.
async fn seed(store: &Store) -> (WorkspaceId, PrincipalId) {
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&intent_services::test_support::fixtures::workspace(&ws))
        .await
        .expect("workspace");
    let mut primary = store.get_primary_principal().await.expect("primary");
    primary.set_github_user_id(Some(10));
    primary.login = Some("first".into());
    store
        .upsert_principal(&primary)
        .await
        .expect("link primary");
    (ws, primary.id)
}

// Failure mode 1: process A qualifies the creator's identity and mints;
// process B re-keys the primary identity before A's insert. The invite would
// be minted from an identity that no longer holds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_invite_is_not_minted_from_an_identity_another_process_switched() {
    let Some(pool) = pool().await else { return };
    let tenant = tenant("mint-rekey");
    let (store_a, store_b) = (
        open(&pool, &tenant, "process-a").await,
        open(&pool, &tenant, "process-b").await,
    );
    let (ws, primary) = seed(&store_a).await;
    let dir = tempfile::tempdir().unwrap();
    let park = PausePoint::new("invite mint insert");
    let a = services(store_a, &dir, "a").with_invite_mint_park_for_test(park.clone());

    let mut mint = tokio::spawn({
        let ws = ws.clone();
        let caller = wire(&primary);
        async move { with_caller(caller, a.workspace_invite_create(ws, None, None)).await }
    });
    assert!(park.reached(&mut mint).await.is_none());

    // Process B switches the primary to another account meanwhile.
    let mut switched = store_b.get_principal(&primary).await.unwrap();
    switched.set_identity(PrincipalIdentity::github(9999));
    store_b.upsert_principal(&switched).await.unwrap();
    park.release();

    let minted = mint.await.expect("mint task");
    assert!(
        matches!(minted, Err(Error::Internal(ref m)) if m.contains("changed while minting")),
        "the stale mint is refused: {minted:?}"
    );
    assert_eq!(store_b.count_open_workspace_invites().await.unwrap(), 0);
}

// Failure mode 2: process A finds the identity unlocked and switches it;
// process B mints an invite before A's write. The identity would change while
// an invite depends on it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_identity_does_not_switch_after_another_process_minted_an_invite() {
    let Some(pool) = pool().await else { return };
    let tenant = tenant("rekey-mint");
    let (store_a, store_b) = (
        open(&pool, &tenant, "process-a").await,
        open(&pool, &tenant, "process-b").await,
    );
    let (ws, primary) = seed(&store_a).await;
    let dir = tempfile::tempdir().unwrap();
    let park = PausePoint::new("identity write");
    let a = services(store_a, &dir, "a").with_identity_write_park_for_test(park.clone());

    let snapshot = store_b.get_principal(&primary).await.unwrap();
    let mut switch = tokio::spawn(async move {
        a.apply_primary_identity_for_test(
            snapshot,
            &UserIdentity {
                login: "second".into(),
                id: Some(20),
                name: None,
                avatar_url: None,
                html_url: None,
            },
        )
        .await
    });
    assert!(park.reached(&mut switch).await.is_none());

    // Process B mints an invite meanwhile: the identity is now locked.
    let now = now_iso();
    store_b
        .insert_workspace_invite(&WorkspaceInvite {
            id: uuid::Uuid::new_v4().to_string(),
            workspace_id: ws,
            secret_hash: "h".into(),
            secret: None,
            created_by_principal_id: primary.clone(),
            pin_identity: None,
            pin_github_user_id: None,
            pin_login: None,
            created_at: now,
            expires_at: "2099-01-01T00:00:00.000Z".into(),
            redeemed_at: None,
            redeemed_by_principal_id: None,
            revoked_at: None,
            redemption_count: 0,
        })
        .await
        .unwrap();
    park.release();

    let switched = switch.await.expect("switch task");
    assert!(
        matches!(
            switched,
            Err(Error::Invite(InviteErrorKind::IdentityLocked))
        ),
        "the switch is refused once an invite depends on the identity: {switched:?}"
    );
    let stored = store_b.get_principal(&primary).await.unwrap();
    assert_eq!(stored.identity_key(), Some(PrincipalIdentity::github(10)));
    assert_eq!(stored.login.as_deref(), Some("first"));
}
