//! A tenant's repository context, file watchers and `config.toml` live
//! reload run as `intentd serve` runs them: after the tenant restarts on
//! another instance, a workspace on head reports its repository (the
//! metadata prewarm fills its owner and name), a file edit in it refreshes
//! its git status (`changes:git-status`), and an edit of the tenant's
//! `config.toml` is applied without a restart. All of it stops with the
//! tenant's fence.
//!
//! Needs `--features postgres` and `INTENT_STORE_TEST_PG_URL` (the
//! `store-postgres` CI job sets both); skipped without the URL unless
//! `INTENT_STORE_REQUIRE_PG=1`.
#![cfg(feature = "postgres")]

use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use intent_core::events::CHANGES_GIT_STATUS;
use intent_core::WorkspaceId;
use intent_services::SubscriptionFilter;
use intent_store::postgres::{self, PgPool, SecretKeyring};
use intent_store::OrchestratorOnly;
use intentd_orchestrator::serve::{serve_tenant, LeaseTiming, ServedTenant};

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

fn wait() -> Duration {
    intentd_test_support::test_timeout(Duration::from_secs(60))
}

fn keyring() -> Arc<SecretKeyring> {
    Arc::new(SecretKeyring::new("k1", &[0x3c; 32]).unwrap())
}

#[track_caller]
fn git(dir: &Path, args: &[&str]) {
    let output = Command::new("git")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .arg("-C")
        .arg(dir)
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

/// A repository on head with one commit and a GitHub origin.
fn repository(dir: &Path) {
    std::fs::create_dir_all(dir).unwrap();
    git(dir, &["init", "-q", "-b", "main"]);
    std::fs::write(dir.join("README.md"), "base\n").unwrap();
    git(dir, &["add", "README.md"]);
    git(
        dir,
        &[
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.invalid",
            "commit",
            "-q",
            "-m",
            "base",
        ],
    );
    git(
        dir,
        &[
            "remote",
            "add",
            "origin",
            "https://github.com/acme/widgets.git",
        ],
    );
}

async fn serve(pool: &PgPool, tenant: &str, data_dir: &Path, instance: &str) -> ServedTenant {
    serve_tenant(
        pool.clone(),
        tenant,
        data_dir,
        instance,
        LeaseTiming::default(),
        keyring(),
        OrchestratorOnly::for_tests(),
    )
    .await
    .unwrap_or_else(|e| panic!("serve {tenant} as {instance}: {e:?}"))
}

/// Fire `served`'s fence, wait for its work to stop and give its lease back.
async fn stop(mut served: ServedTenant) {
    assert!(served.fence().fire("test: the instance stops"));
    let report = tokio::time::timeout(wait(), served.stopped())
        .await
        .expect("the tenant stops");
    assert!(report.overran.is_empty(), "{:?}", report.overran);
    assert!(
        served.heartbeat().release().await.expect("release"),
        "the lease is released"
    );
}

fn rtk_enabled(served: &ServedTenant) -> bool {
    served
        .services()
        .settings_registry()
        .expect("the tenant has a settings registry")
        .snapshot()
        .effective
        .rtk
        .enabled
}

/// Replace the tenant's `config.toml` as an editor saves: write a temporary
/// file and rename it over.
fn save_config(dir: &Path, text: &str) {
    let tmp = dir.join("config.toml.tmp");
    std::fs::write(&tmp, text).unwrap();
    std::fs::rename(&tmp, dir.join("config.toml")).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restarted_tenant_reports_its_repository_git_status_and_live_config() {
    let Some(pool) = pool().await else { return };
    if Command::new("git").arg("--version").output().is_err() {
        eprintln!("skipping: git is not on PATH");
        return;
    }
    let data = tempfile::tempdir().unwrap();
    let run = uuid::Uuid::new_v4().simple().to_string()[..12].to_owned();
    let tenant = format!("repo-{run}");

    // The first instance: a workspace on a repository on head is added to
    // the tenant's store directly (in orchestrator mode `workspace.create`
    // needs a home machine), so no lifecycle event announces it.
    let first = serve(&pool, &tenant, data.path(), &format!("first-{run}")).await;
    let repo = data.path().join("repo");
    repository(&repo);
    let workspace = WorkspaceId(format!("ws-{run}"));
    let now = intent_core::now_iso();
    first
        .services()
        .store()
        .insert_workspace(&intent_core::Workspace {
            id: workspace.clone(),
            title: "Repository context".into(),
            branch: "main".into(),
            repository_path: Some(repo.to_string_lossy().into_owned()),
            created_at: now.clone(),
            updated_at: now,
            ..Default::default()
        })
        .await
        .expect("seed the workspace");
    stop(first).await;

    // The tenant restarts on a second instance, over the same files.
    let second = serve(&pool, &tenant, data.path(), &format!("second-{run}")).await;
    let mut git_status = second.bus().subscribe(SubscriptionFilter {
        event_types: vec![CHANGES_GIT_STATUS.to_owned()],
        workspace_id: Some(workspace.as_str().to_owned()),
        ..SubscriptionFilter::default()
    });

    // Its repository: the metadata prewarm reads the origin remote.
    let store = second.services().store().clone();
    tokio::time::timeout(wait(), async {
        loop {
            let ws = store.get_workspace(&workspace).await.expect("read");
            if ws.repository_owner.as_deref() == Some("acme")
                && ws.repository_name.as_deref() == Some("widgets")
            {
                return;
            }
            // timing-guard: poll until the background prewarm has written
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("the restarted tenant reports the workspace's repository");

    // Its git status: an edit in the checkout refreshes it. The watch goes
    // live in the background, so the edit repeats until the refresh comes.
    let refreshed = tokio::time::timeout(wait(), async {
        let mut edit = 0;
        loop {
            edit += 1;
            std::fs::write(repo.join("notes.txt"), format!("edit {edit}\n")).unwrap();
            // timing-guard: one edit per debounce window while the watch registers
            // fixed-deadline: allow — the re-edit interval, not a hang guard; `wait()` bounds the loop
            let next = tokio::time::timeout(Duration::from_secs(2), git_status.recv()).await;
            if let Ok(Some(batch)) = next {
                if let Some(event) = batch.into_iter().next() {
                    return event;
                }
            }
        }
    })
    .await
    .expect("an edit in the workspace refreshes its git status");
    assert_eq!(refreshed.workspace_id, workspace);

    // Its config: an edit of the tenant's config.toml applies at once.
    let before = rtk_enabled(&second);
    save_config(second.dir(), &format!("[rtk]\nenabled = {}\n", !before));
    tokio::time::timeout(wait(), async {
        while rtk_enabled(&second) == before {
            // timing-guard: poll until the debounced reload has applied
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("the config.toml edit is applied without a restart");

    // Everything stops with the fence: a later config edit is not applied.
    let applied = !before;
    let dir = second.dir().to_path_buf();
    let services = second.services().clone();
    stop(second).await;
    save_config(&dir, &format!("[rtk]\nenabled = {before}\n"));
    // timing-guard: give a still-running watcher several debounce windows
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(
        services
            .settings_registry()
            .unwrap()
            .snapshot()
            .effective
            .rtk
            .enabled,
        applied,
        "a fenced tenant reloads nothing"
    );
}
