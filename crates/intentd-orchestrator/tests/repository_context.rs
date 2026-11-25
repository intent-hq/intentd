//! A tenant's repository context and file watchers run as `intentd serve`
//! runs them: after the tenant restarts on another instance, a workspace on
//! head reports its repository (the metadata prewarm fills its owner and
//! name) and a file edit in it refreshes its git status
//! (`changes:git-status`). A workspace whose home is on a machine gets its
//! status from that home only: head's own copy of it is neither watched nor
//! scanned. (A tenant's settings live in its Postgres row; live settings
//! changes are covered by `tenant_settings.rs`.)
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
use intent_core::node_link::{HomeEvent, HomeEvents};
use intent_core::{HomeId, HomeState, WorkspaceHome, WorkspaceId};
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

/// The machine the homed workspace lives on.
const MACHINE: &str = "machine-1";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restarted_tenant_reports_its_repository_and_git_status() {
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
    // A workspace homed on a machine, with a clean copy of its repository
    // left on head.
    let homed_repo = data.path().join("homed");
    repository(&homed_repo);
    let homed = WorkspaceId(format!("ws-homed-{run}"));
    let home_id = HomeId::from(format!("home-{run}").as_str());
    let now = intent_core::now_iso();
    first
        .services()
        .store()
        .insert_workspace(&intent_core::Workspace {
            id: homed.clone(),
            title: "Homed".into(),
            branch: "main".into(),
            repository_path: Some(homed_repo.to_string_lossy().into_owned()),
            created_at: now.clone(),
            updated_at: now.clone(),
            ..Default::default()
        })
        .await
        .expect("seed the homed workspace");
    first
        .services()
        .store()
        .set_workspace_home(
            &homed,
            &WorkspaceHome {
                home_id: home_id.clone(),
                node_id: Some(MACHINE.to_owned()),
                cloud_machine_id: None,
                name: "machine".into(),
                state: HomeState::Attached,
                since: now,
                last_backup_at: None,
                backup_warning: false,
                setup: None,
            },
        )
        .await
        .expect("home the workspace on a machine");
    stop(first).await;

    // The tenant restarts on a second instance, over the same files.
    let second = serve(&pool, &tenant, data.path(), &format!("second-{run}")).await;
    let mut git_status = second.bus().subscribe(SubscriptionFilter {
        event_types: vec![CHANGES_GIT_STATUS.to_owned()],
        workspace_id: Some(workspace.as_str().to_owned()),
        ..SubscriptionFilter::default()
    });
    let mut homed_status = second.bus().subscribe(SubscriptionFilter {
        event_types: vec![CHANGES_GIT_STATUS.to_owned()],
        workspace_id: Some(homed.as_str().to_owned()),
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
    // Each round the homed workspace's head copy is edited too, and its
    // machine relays a file event (its home is dirty, head's copy clean).
    let refreshed = tokio::time::timeout(wait(), async {
        let mut edit = 0;
        loop {
            edit += 1;
            std::fs::write(repo.join("notes.txt"), format!("edit {edit}\n")).unwrap();
            std::fs::write(homed_repo.join("README.md"), "base\n").unwrap();
            second
                .services()
                .publish_home_events(
                    MACHINE,
                    HomeEvents {
                        workspace_id: homed.clone(),
                        home_id: home_id.clone(),
                        events: vec![HomeEvent {
                            event_type: "file:changed".into(),
                            data: serde_json::json!({ "path": "notes.txt" }),
                        }],
                    },
                )
                .await;
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
    // Longer than the refresher's debounce: a head scan of the homed
    // workspace would have published by now.
    let homed_scan = tokio::time::timeout(
        intentd_test_support::test_timeout(Duration::from_secs(3)),
        homed_status.recv(),
    )
    .await;
    assert!(
        homed_scan.is_err(),
        "head published a git status for a workspace homed on a machine: {homed_scan:?}"
    );

    stop(second).await;
}

/// Add a workspace over the repository `repo` on head to the tenant's store.
async fn seed_workspace(services: &intent_services::Services, id: &WorkspaceId, repo: &Path) {
    let now = intent_core::now_iso();
    services
        .store()
        .insert_workspace(&intent_core::Workspace {
            id: id.clone(),
            title: id.as_str().to_owned(),
            branch: "main".into(),
            repository_path: Some(repo.to_string_lossy().into_owned()),
            created_at: now.clone(),
            updated_at: now,
            ..Default::default()
        })
        .await
        .expect("seed the workspace");
}

/// Home workspace `id` on [`MACHINE`].
async fn home_on_machine(services: &intent_services::Services, id: &WorkspaceId, home: &HomeId) {
    services
        .store()
        .set_workspace_home(
            id,
            &WorkspaceHome {
                home_id: home.clone(),
                node_id: Some(MACHINE.to_owned()),
                cloud_machine_id: None,
                name: "machine".into(),
                state: HomeState::Attached,
                since: intent_core::now_iso(),
                last_backup_at: None,
                backup_warning: false,
                setup: None,
            },
        )
        .await
        .expect("home the workspace on a machine");
}

async fn publish_opened(served: &ServedTenant, id: &WorkspaceId) {
    served
        .bus()
        .publish(&intent_store::NewEvent {
            workspace_id: id.clone(),
            timestamp: intent_core::now_iso(),
            event_type: intent_core::events::WORKSPACE_OPENED.to_owned(),
            actor: intent_services::system_actor(),
            session_id: None,
            correlation_id: None,
            parent_event_id: None,
            metadata: None,
            data: serde_json::json!({}),
        })
        .await
        .expect("publish workspace:opened");
}

/// While the tenant serves, a workspace homed on a machine never gets a
/// watch on head's copy: not when it is opened, and not when a workspace
/// head already watches moves its home to a machine. Head publishes no file
/// event for edits to those copies; the machine's relayed events still
/// arrive.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn head_does_not_watch_a_copy_of_a_workspace_homed_on_a_machine() {
    let Some(pool) = pool().await else { return };
    if Command::new("git").arg("--version").output().is_err() {
        eprintln!("skipping: git is not on PATH");
        return;
    }
    let data = tempfile::tempdir().unwrap();
    let run = uuid::Uuid::new_v4().simple().to_string()[..12].to_owned();
    let tenant = format!("homed-{run}");
    let served = serve(&pool, &tenant, data.path(), &format!("only-{run}")).await;
    let services = served.services().clone();
    let mut files = served.bus().subscribe(SubscriptionFilter {
        event_types: vec!["file:*".to_owned()],
        ..SubscriptionFilter::default()
    });

    // Opened while the tenant serves, already homed on a machine.
    let opened = WorkspaceId(format!("ws-opened-{run}"));
    let opened_repo = data.path().join("opened");
    repository(&opened_repo);
    let opened_home = HomeId::from(format!("home-opened-{run}").as_str());
    seed_workspace(&services, &opened, &opened_repo).await;
    home_on_machine(&services, &opened, &opened_home).await;
    publish_opened(&served, &opened).await;

    // Watched on head, then homed on a machine.
    let moved = WorkspaceId(format!("ws-moved-{run}"));
    let moved_repo = data.path().join("moved");
    repository(&moved_repo);
    seed_workspace(&services, &moved, &moved_repo).await;
    publish_opened(&served, &moved).await;
    home_on_machine(
        &services,
        &moved,
        &HomeId::from(format!("home-moved-{run}").as_str()),
    )
    .await;
    services.publish_workspace_home_changed(&moved).await;

    // The control: a workspace on head, opened after both. Once an edit to
    // it is seen, the registry has handled every event above.
    let control = WorkspaceId(format!("ws-control-{run}"));
    let control_repo = data.path().join("control");
    repository(&control_repo);
    seed_workspace(&services, &control, &control_repo).await;
    publish_opened(&served, &control).await;

    let mut seen = Vec::new();
    tokio::time::timeout(wait(), async {
        let mut edit = 0;
        loop {
            edit += 1;
            for repo in [&opened_repo, &moved_repo, &control_repo] {
                std::fs::write(repo.join("notes.txt"), format!("edit {edit}\n")).unwrap();
            }
            // fixed-deadline: allow — the re-edit interval, not a hang guard; `wait()` bounds the loop
            let next = tokio::time::timeout(Duration::from_secs(2), files.recv()).await;
            if let Ok(Some(batch)) = next {
                let done = batch.iter().any(|event| event.workspace_id == control);
                seen.extend(batch);
                if done {
                    return;
                }
            }
        }
    })
    .await
    .expect("head watches the control workspace's checkout");

    // `moved` was watched until its home changed: drop what head published
    // before then (and what it was still debouncing), then edit both copies
    // again while the machine relays an edit in its home.
    // fixed-deadline: allow — a settle window longer than the watcher's debounce; it only discards events
    let _ = tokio::time::timeout(Duration::from_secs(2), async {
        while let Some(batch) = files.recv().await {
            seen.extend(batch);
        }
    })
    .await;
    seen.retain(|event| event.workspace_id != moved);
    for repo in [&opened_repo, &moved_repo] {
        std::fs::write(repo.join("notes.txt"), "after\n").unwrap();
    }
    services
        .publish_home_events(
            MACHINE,
            HomeEvents {
                workspace_id: opened.clone(),
                home_id: opened_home.clone(),
                events: vec![HomeEvent {
                    event_type: "file:changed".into(),
                    data: serde_json::json!({ "path": "relayed.txt" }),
                }],
            },
        )
        .await;
    // Longer than the file watcher's debounce: head's events for the homed
    // copies would have arrived by now.
    let _ = tokio::time::timeout(
        intentd_test_support::test_timeout(Duration::from_secs(3)),
        async {
            while let Some(batch) = files.recv().await {
                seen.extend(batch);
            }
        },
    )
    .await;
    let relayed = |event: &intent_core::Event| event.data["path"] == "relayed.txt";
    let from_head: Vec<_> = seen
        .iter()
        .filter(|event| {
            (event.workspace_id == opened || event.workspace_id == moved) && !relayed(event)
        })
        .collect();
    assert!(
        from_head.is_empty(),
        "head published file events for copies of workspaces homed on a machine: {from_head:?}"
    );
    assert!(
        seen.iter()
            .any(|event| event.workspace_id == opened && relayed(event)),
        "the machine's relayed file event arrives"
    );
    served.fence().fire("test: done");
}
