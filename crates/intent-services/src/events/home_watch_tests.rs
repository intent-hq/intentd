//! A home checkout watched under its hidden record: its file and git status
//! events reach the home tap only, with checkout-relative paths, and stop
//! when the watch is dropped.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use intent_core::{BoxFuture, Error, Event, Result, Workspace, WorkspaceApi, WorkspaceId};
use intent_store::Store;
use tokio::sync::broadcast;

use super::*;
use crate::events::LIVENESS;

struct Api(Workspace);

impl WorkspaceApi for Api {
    fn get_workspace(&self, id: WorkspaceId) -> BoxFuture<'_, Result<Workspace>> {
        let found = (id == self.0.id).then(|| self.0.clone());
        Box::pin(async move { found.ok_or_else(|| Error::NotFound(id.0)) })
    }
}

fn git(root: &Path, args: &[&str]) {
    let out = std::process::Command::new("git")
        .args(args)
        .current_dir(root)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .output()
        .expect("run git");
    assert!(out.status.success(), "git {args:?}: {out:?}");
}

fn wait() -> Duration {
    intentd_test_support::test_timeout(Duration::from_secs(20))
}

/// The next tapped event of `record` that `accept` takes.
async fn next(
    rx: &mut broadcast::Receiver<Arc<Event>>,
    record: &WorkspaceId,
    accept: impl Fn(&Event) -> bool,
) -> Arc<Event> {
    tokio::time::timeout(wait(), async {
        loop {
            match rx.recv().await {
                Ok(event) if &event.workspace_id == record && accept(&event) => return event,
                Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => {}
                Err(broadcast::error::RecvError::Closed) => panic!("tap closed"),
            }
        }
    })
    .await
    .expect("no tapped event")
}

#[tokio::test]
#[expect(clippy::await_holding_lock)]
async fn a_watched_home_checkout_reaches_the_tap_only() {
    let _serial = super::super::WATCHER_TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::tempdir().unwrap();
    let work: PathBuf = std::fs::canonicalize(dir.path()).unwrap().join("work");
    std::fs::create_dir_all(&work).unwrap();
    git(&work, &["init", "--quiet"]);
    std::fs::write(work.join("README.md"), "base\n").unwrap();
    git(&work, &["add", "README.md"]);
    git(
        &work,
        &[
            "-c",
            "user.name=T",
            "-c",
            "user.email=t@example.invalid",
            "commit",
            "--quiet",
            "-m",
            "base",
        ],
    );
    let db = tempfile::tempdir().unwrap();
    let store = Store::open(&db.path().join("t.db")).await.unwrap();
    let bus = EventBus::new(store);
    let mut seen = bus.subscribe(crate::events::SubscriptionFilter::default());
    let record = WorkspaceId::new_home_record();
    let mut row = intent_core::chief_workspace();
    row.id = record.clone();
    row.worktree_path = Some(work.to_string_lossy().into_owned());
    let watches = HomeWatches::start(
        bus.clone(),
        Arc::new(Api(row)),
        Arc::new(crate::git_status_cache::GitStatusCache::new()),
        crate::worktree_presence::WorktreePresence::default(),
    );
    let mut rx = watches.events();
    let watch = watches.watch(&record, &work);
    watch.wait_established(LIVENESS).await;
    // The watch starts with a fresh status.
    next(&mut rx, &record, |event| {
        event.event_type == "changes:git-status"
    })
    .await;

    std::fs::write(work.join("new.txt"), "x\n").unwrap();
    let file = next(&mut rx, &record, |event| {
        event.event_type.starts_with("file:")
    })
    .await;
    assert_eq!(file.data["path"], "new.txt", "{:?}", file.data);
    let status = next(&mut rx, &record, |event| {
        event.event_type == "changes:git-status" && event.data["status"]["uncommittedCount"] == 1
    })
    .await;
    assert!(
        !status.data.to_string().contains(&*work.to_string_lossy()),
        "{:?}",
        status.data
    );

    // None of it was published to the machine's own subscribers.
    let marker = bus
        .publish(&intent_store::NewEvent {
            workspace_id: WorkspaceId::from("ws-local"),
            timestamp: intent_core::now_iso(),
            event_type: "note:created".into(),
            actor: intent_core::EventActor::default(),
            session_id: None,
            correlation_id: None,
            parent_event_id: None,
            metadata: None,
            data: serde_json::json!({}),
        })
        .await
        .unwrap();
    let first = tokio::time::timeout(wait(), seen.recv())
        .await
        .expect("no event")
        .expect("closed");
    assert_eq!(first[0].id, marker.id, "{first:?}");

    // Dropped, the watch ends: a later change gives no event of it, and a
    // refresh asked for after it is the next event of the record.
    drop(watch);
    std::fs::write(work.join("after.txt"), "x\n").unwrap();
    watches.refresh(&record);
    let after = next(&mut rx, &record, |_| true).await;
    assert_eq!(after.event_type, "changes:git-status", "{:?}", after.data);
}
