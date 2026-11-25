//! Tenant settings and the GitLab credential pair live in the tenant's
//! Postgres rows, not under its directory: a value set over the WSS
//! listener is read back by the instance that takes the tenant over on a
//! fresh data dir, and no `config.toml` or `secrets.json` is ever written.
//! Two registries writing one tenant's settings from one revision: one
//! wins, the other gets a revision conflict; a served tenant adopts another
//! writer's revision.
//!
//! Needs `--features postgres` and `INTENT_STORE_TEST_PG_URL` (the
//! `store-postgres` CI job sets both); skipped without the URL unless
//! `INTENT_STORE_REQUIRE_PG=1`.
#![cfg(all(unix, feature = "postgres"))]

mod common;

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use common::{result, until, ws_rpc, wss};
use intent_core::Error;
use intent_services::SettingsRegistry;
use intent_store::postgres::{self, PgPool, PostgresOptions, SecretKeyring, TenantId};
use intent_store::{OrchestratorOnly, Store};
use intent_transport::auth::TOKEN_ACCOUNT;
use intentd_orchestrator::listen::ListenConfig;
use intentd_orchestrator::serve::{serve_tenant, LeaseTiming, ServedTenant};
use intentd_orchestrator::settings::TenantSettings;
use serde_json::{json, Value};

const GITLAB_TOKEN: &str = "sourceControl.gitlab.token";

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

fn keyring() -> Arc<SecretKeyring> {
    Arc::new(SecretKeyring::new("k1", &[0x44; 32]).unwrap())
}

fn run_id() -> String {
    uuid::Uuid::new_v4().simple().to_string()[..12].to_owned()
}

fn listen_config(dir: &Path) -> ListenConfig {
    ListenConfig {
        socket_path: dir.join("o.sock"),
        bind_addresses: vec![std::net::IpAddr::from([127, 0, 0, 1])],
        ws_port: 0,
        tls_dir: dir.join("tls"),
        insecure: false,
    }
}

/// A short lease, so a takeover does not wait long once renewals stop.
fn short() -> LeaseTiming {
    LeaseTiming {
        ttl: Duration::from_secs(3),
        interval: Duration::from_millis(100),
    }
}

/// Serve `tenant` from `data_dir` as `instance`, retrying while another
/// instance's lease is still live.
async fn serve_when_free(
    pool: &PgPool,
    tenant: &str,
    data_dir: &Path,
    instance: &str,
    timing: LeaseTiming,
) -> ServedTenant {
    tokio::time::timeout(common::wait(), async {
        loop {
            match serve_tenant(
                pool.clone(),
                tenant,
                data_dir,
                instance,
                timing,
                keyring(),
                OrchestratorOnly::for_tests(),
            )
            .await
            {
                Ok(served) => return served,
                Err(Error::NotOwner(_)) => {
                    // timing-guard: poll interval while the previous instance's lease runs out
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
                Err(other) => panic!("serve {tenant}: {other:?}"),
            }
        }
    })
    .await
    .expect("the lease frees up")
}

/// Every file under `root` named like the desktop's settings or secrets.
fn desktop_files(root: &Path) -> Vec<std::path::PathBuf> {
    let mut found = Vec::new();
    let mut dirs = vec![root.to_path_buf()];
    while let Some(dir) = dirs.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let name = entry.file_name().to_string_lossy().into_owned();
            if name == "config.toml"
                || name.ends_with("secrets.json")
                || name.ends_with(".collaboration")
                || name.starts_with(".config.toml.tmp")
            {
                found.push(path.clone());
            }
            if path.is_dir() {
                dirs.push(path);
            }
        }
    }
    found
}

/// Connect to `served`'s WSS listener with its owner token.
async fn connect(
    served: &ServedTenant,
    listen_dir: &Path,
) -> (common::Wss, intentd_orchestrator::listen::ClientListeners) {
    let listeners = served.listen(&listen_config(listen_dir)).await.unwrap();
    let fingerprint = listeners.fingerprint().unwrap().to_owned();
    let token = served
        .services()
        .secret_source()
        .load(TOKEN_ACCOUNT)
        .unwrap()
        .expect("the owner token is in the tenant's rows");
    let ws = wss(listeners.ws_port(), &fingerprint, Some(&token))
        .await
        .unwrap();
    (ws, listeners)
}

fn value_of(response: &Value) -> &Value {
    &result(response)["value"]
}

/// Definition of done: `settings.update` over WSS, then the instance that takes the tenant
/// over on a fresh data dir reads the same value — settings and the GitLab
/// token alike — and neither data dir ever holds a `config.toml` or a
/// `secrets.json`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn settings_set_over_wss_survive_a_restart_on_a_fresh_data_dir() {
    let Some(pool) = pool().await else { return };
    let run = run_id();
    let tenant = format!("settings-{run}");
    let first_dir = tempfile::tempdir().unwrap();
    let mut first = serve_when_free(
        &pool,
        &tenant,
        first_dir.path(),
        &format!("first-{run}"),
        short(),
    )
    .await;
    let (mut ws, listeners) = connect(&first, first_dir.path()).await;
    result(
        &ws_rpc(
            &mut ws,
            1,
            "client.hello",
            json!({ "clientId": "web-settings" }),
        )
        .await,
    );
    let updated = ws_rpc(
        &mut ws,
        2,
        "settings.update",
        json!({ "changes": [
            { "path": "git.autoCommit", "value": false },
            { "path": "agents.maxConcurrent", "value": 7 },
            { "path": GITLAB_TOKEN, "value": "glpat-follows" },
        ] }),
    )
    .await;
    result(&updated);
    assert_eq!(
        value_of(
            &ws_rpc(
                &mut ws,
                3,
                "settings.get",
                json!({ "path": "git.autoCommit" })
            )
            .await
        ),
        &json!(false)
    );
    let row: (String, i64) =
        sqlx::query_as("SELECT body, revision FROM tenant_config WHERE tenant_id = $1")
            .bind(&tenant)
            .fetch_one(&pool)
            .await
            .expect("the settings are the tenant's row");
    assert!(row.0.contains("autoCommit = false"), "{}", row.0);
    assert!(row.1 >= 1);
    let gitlab_rows: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM tenant_secrets WHERE tenant_id = $1 AND account = $2",
    )
    .bind(&tenant)
    .bind(GITLAB_TOKEN)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        gitlab_rows, 1,
        "the GitLab token is an encrypted tenant row"
    );
    assert_eq!(
        desktop_files(first_dir.path()),
        Vec::<std::path::PathBuf>::new()
    );

    // The first instance stops (as on a lost lease); a second one, on
    // another host's fresh data dir, takes the tenant over.
    first.fence().fire("test: the first instance stops");
    tokio::time::timeout(common::wait(), first.stopped())
        .await
        .expect("the first instance stops");
    listeners.stop().await;
    let second_dir = tempfile::tempdir().unwrap();
    let second = serve_when_free(
        &pool,
        &tenant,
        second_dir.path(),
        &format!("second-{run}"),
        LeaseTiming::default(),
    )
    .await;
    let (mut ws, listeners) = connect(&second, second_dir.path()).await;
    result(
        &ws_rpc(
            &mut ws,
            1,
            "client.hello",
            json!({ "clientId": "web-settings-2" }),
        )
        .await,
    );
    assert_eq!(
        value_of(
            &ws_rpc(
                &mut ws,
                2,
                "settings.get",
                json!({ "path": "git.autoCommit" })
            )
            .await
        ),
        &json!(false),
        "the new instance reads what the old one wrote"
    );
    assert_eq!(
        value_of(
            &ws_rpc(
                &mut ws,
                3,
                "settings.get",
                json!({ "path": "agents.maxConcurrent" })
            )
            .await
        ),
        &json!(7.0),
        "number settings read back as numbers (the wire type is f64)"
    );
    assert_eq!(
        second
            .services()
            .secret_source()
            .load(GITLAB_TOKEN)
            .unwrap()
            .as_deref(),
        Some("glpat-follows"),
        "the GitLab token follows the tenant"
    );
    // A write on the new instance continues from the stored revision.
    result(
        &ws_rpc(
            &mut ws,
            4,
            "settings.update",
            json!({ "changes": [{ "path": "git.autoCommit", "value": true }] }),
        )
        .await,
    );
    let revision: i64 =
        sqlx::query_scalar("SELECT revision FROM tenant_config WHERE tenant_id = $1")
            .bind(&tenant)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(revision, row.1 + 1);
    for root in [first_dir.path(), second_dir.path()] {
        assert_eq!(
            desktop_files(root),
            Vec::<std::path::PathBuf>::new(),
            "no settings or secrets file under {}",
            root.display()
        );
    }
    second.fence().fire("test: done");
    listeners.stop().await;
}

async fn registry_on(pool: &PgPool, tenant: &str) -> Arc<SettingsRegistry> {
    let store = Store::open_postgres(
        pool.clone(),
        TenantId::new(tenant).unwrap(),
        PostgresOptions::default(),
        OrchestratorOnly::for_tests(),
    )
    .await
    .unwrap();
    Arc::new(
        SettingsRegistry::load_from_store(Arc::new(TenantSettings::new(&store).unwrap())).unwrap(),
    )
}

/// Definition of done: two registries on one tenant, both at the same revision, apply at
/// once: one wins, the other gets a revision conflict and changes nothing;
/// after a refresh the loser sees the winner's value and its retry lands.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn of_two_registries_applying_at_once_one_wins_and_the_other_conflicts() {
    let Some(pool) = pool().await else { return };
    let tenant = format!("settings-race-{}", run_id());
    let a = registry_on(&pool, &tenant).await;
    let b = registry_on(&pool, &tenant).await;
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let apply = |registry: Arc<SettingsRegistry>, value: i64| {
        let barrier = barrier.clone();
        std::thread::spawn(move || {
            barrier.wait();
            registry.apply(&[("agents.maxConcurrent".to_owned(), json!(value))])
        })
    };
    let from_a = apply(a.clone(), 3);
    let from_b = apply(b.clone(), 5);
    let outcomes = [from_a.join().unwrap(), from_b.join().unwrap()];
    let wins = outcomes.iter().filter(|o| o.is_ok()).count();
    let conflicts = outcomes
        .iter()
        .filter(|o| matches!(o, Err(Error::Conflict { .. })))
        .count();
    assert_eq!((wins, conflicts), (1, 1), "{outcomes:?}");
    let (winner, loser, value) = if outcomes[0].is_ok() {
        (&a, &b, 3)
    } else {
        (&b, &a, 5)
    };
    assert_eq!(winner.get("agents.maxConcurrent"), Some(json!(value)));
    assert_eq!(winner.store_revision(), 1);
    assert_eq!(loser.store_revision(), 0, "the loser kept its state");
    assert_ne!(loser.get("agents.maxConcurrent"), Some(json!(value + 100)));

    loser.refresh().unwrap();
    assert_eq!(loser.get("agents.maxConcurrent"), Some(json!(value)));
    loser
        .apply(&[("agents.maxConcurrent".to_owned(), json!(9))])
        .expect("the retry after a refresh lands");
    assert_eq!(loser.store_revision(), 2);
    winner.refresh().unwrap();
    assert_eq!(winner.get("agents.maxConcurrent"), Some(json!(9)));
}

/// A served tenant follows its settings row: a write another registry
/// makes reaches the tenant's `settings.get` without a restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_served_tenant_adopts_another_writers_revision() {
    let Some(pool) = pool().await else { return };
    let run = run_id();
    let tenant = format!("settings-follow-{run}");
    let dir = tempfile::tempdir().unwrap();
    let served = serve_when_free(
        &pool,
        &tenant,
        dir.path(),
        &format!("instance-{run}"),
        LeaseTiming::default(),
    )
    .await;
    let other = registry_on(&pool, &tenant).await;
    other
        .apply(&[("git.autoCommit".to_owned(), json!(false))])
        .expect("another writer updates the tenant's settings");
    let services = served.services().clone();
    let deadline = common::wait();
    tokio::time::timeout(deadline, async {
        loop {
            let got = intent_core::with_caller(
                intent_core::Caller::Daemon,
                intent_core::WorkspaceApi::settings_get(&services, "git.autoCommit".into()),
            )
            .await
            .unwrap();
            if got["value"] == json!(false) {
                return;
            }
            // timing-guard: poll interval while the tenant's settings follower re-reads the revision
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("the served tenant adopts the newer revision");
    until("the served registry to hold the new revision", || {
        services
            .settings_registry()
            .is_some_and(|registry| registry.store_revision() == 1)
    })
    .await;
    served.fence().fire("test: done");
}

/// The settings follower ends before `drain` and `stopped` return: no
/// settings adoption outlives a drained or fenced tenant.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drain_and_stopped_join_the_settings_follower() {
    let Some(pool) = pool().await else { return };
    let run = run_id();

    let drained_dir = tempfile::tempdir().unwrap();
    let drained = serve_when_free(
        &pool,
        &format!("settings-drain-{run}"),
        drained_dir.path(),
        &format!("drain-{run}"),
        LeaseTiming::default(),
    )
    .await;
    assert!(drained.settings_follower_running());
    drained.drain(None).await;
    assert!(
        !drained.settings_follower_running(),
        "drain returns only once the follower ended"
    );
    drained.fence().fire("test: done");

    let fenced_dir = tempfile::tempdir().unwrap();
    let mut fenced = serve_when_free(
        &pool,
        &format!("settings-fence-{run}"),
        fenced_dir.path(),
        &format!("fence-{run}"),
        LeaseTiming::default(),
    )
    .await;
    assert!(fenced.settings_follower_running());
    fenced.fence().fire("test: the lease is lost");
    tokio::time::timeout(common::wait(), fenced.stopped())
        .await
        .expect("the fence stops the tenant");
    assert!(
        !fenced.settings_follower_running(),
        "stopped returns only once the follower ended"
    );
}
