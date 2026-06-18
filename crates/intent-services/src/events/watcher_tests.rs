//! Integration test for [`FileWatcher`] over a real temp directory + SQLite
//! store: writing/removing a file under the watched root publishes debounced
//! `file:changed` events whose payload matches the TS `FileChangedEvent` shape.

use std::path::PathBuf;
use std::time::Duration;

use intent_core::{ActorType, Event, WorkspaceId};
use intent_store::Store;
use tokio::time::{timeout, Instant};

use super::bus::EventBus;
use super::filter::SubscriptionFilter;
use super::watcher::FileWatcher;

/// Self-cleaning temp directory (db file + watched workspace root).
struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new(tag: &str) -> Self {
        let path =
            std::env::temp_dir().join(format!("intentd-watch-{tag}-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&path).expect("create temp dir");
        Self { path }
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

struct TempDb {
    path: PathBuf,
}

impl TempDb {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("intentd-watch-{}.db", uuid::Uuid::new_v4()));
        Self { path }
    }
}

impl Drop for TempDb {
    fn drop(&mut self) {
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(PathBuf::from(format!("{}{suffix}", self.path.display())));
        }
    }
}

/// Drain the subscription until a `file:changed` event for `rel_path` (and, when
/// `action` is set, that exact action) arrives or `overall` elapses.
async fn next_for(
    sub: &mut super::bus::Subscription,
    rel_path: &str,
    action: Option<&str>,
    overall: Duration,
) -> Option<Event> {
    let deadline = Instant::now() + overall;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return None;
        }
        match timeout(remaining, sub.recv()).await {
            Ok(Some(batch)) => {
                for ev in batch {
                    if ev.event_type != "file:changed" {
                        continue;
                    }
                    if ev.data["relativePath"] != rel_path {
                        continue;
                    }
                    match action {
                        Some(a) if ev.data["action"] != a => continue,
                        _ => return Some(ev),
                    }
                }
            }
            _ => return None,
        }
    }
}

#[tokio::test]
async fn create_and_delete_emit_file_changed() {
    let db = TempDb::new();
    let store = Store::open(&db.path).await.expect("open store");
    let bus = EventBus::new(store);
    let mut sub = bus.subscribe(SubscriptionFilter::default());

    let dir = TempDir::new("cd");
    let ws = WorkspaceId::from("ws-1");
    let _watcher =
        FileWatcher::start(bus.clone(), ws.clone(), dir.path.clone()).expect("start watcher");

    // Let the OS watch establish before mutating (FSEvents/inotify warm-up).
    tokio::time::sleep(Duration::from_millis(250)).await;

    let file = dir.path.join("foo.txt");
    std::fs::write(&file, b"hello").expect("write file");

    let ev = next_for(&mut sub, "foo.txt", None, Duration::from_secs(5))
        .await
        .expect("create/modify event for foo.txt");
    assert_eq!(ev.event_type, "file:changed");
    assert_eq!(ev.data["path"], "foo.txt");
    assert_eq!(ev.data["relativePath"], "foo.txt");
    assert_eq!(ev.actor.actor_type, ActorType::System);
    let action = ev.data["action"].as_str().expect("action string");
    assert!(action == "create" || action == "modify", "got {action}");

    std::fs::remove_file(&file).expect("remove file");
    let ev = next_for(&mut sub, "foo.txt", Some("delete"), Duration::from_secs(5))
        .await
        .expect("delete event for foo.txt");
    assert_eq!(ev.data["action"], "delete");
    assert_eq!(ev.workspace_id, ws);
}

#[tokio::test]
async fn ignored_paths_emit_nothing() {
    let db = TempDb::new();
    let store = Store::open(&db.path).await.expect("open store");
    let bus = EventBus::new(store);
    let mut sub = bus.subscribe(SubscriptionFilter::default());

    let dir = TempDir::new("ig");
    std::fs::create_dir_all(dir.path.join("node_modules")).expect("mk node_modules");
    let _watcher = FileWatcher::start(bus.clone(), WorkspaceId::from("ws-1"), dir.path.clone())
        .expect("start watcher");
    tokio::time::sleep(Duration::from_millis(250)).await;

    std::fs::write(dir.path.join("node_modules/dep.js"), b"x").expect("write ignored");

    // No event should surface for the ignored file within the debounce + margin.
    let got = next_for(
        &mut sub,
        "node_modules/dep.js",
        None,
        Duration::from_millis(800),
    )
    .await;
    assert!(got.is_none(), "ignored path must not emit: {got:?}");
}
