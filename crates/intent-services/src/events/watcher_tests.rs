//! Integration test for [`FileWatcher`] over a real temp directory + `SQLite`
//! store: writing/removing a file under the watched root publishes debounced
//! `file:*` events (create → `file:created`, delete → `file:deleted`, modify →
//! `file:changed`) whose payload matches the TS `FileChangedEvent` shape.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use intent_core::{ActorType, Event, WorkspaceId};
use intent_store::Store;
use tokio::time::{timeout, Instant};

use super::bus::EventBus;
use super::filter::SubscriptionFilter;
use super::shared_watch::SharedWatchHub;
use super::watcher::{flush_due, Action, FileWatcher};
use super::LIVENESS;

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

/// Drain the subscription until a `file:*` event for `rel_path` (and, when
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
                    if !ev.event_type.starts_with("file:") {
                        continue;
                    }
                    if ev.data["relativePath"] != rel_path {
                        continue;
                    }
                    match action {
                        Some(a) if ev.data["action"] != a => {}
                        _ => return Some(ev),
                    }
                }
            }
            _ => return None,
        }
    }
}

#[tokio::test]
async fn create_and_delete_emit_distinct_file_events() {
    let db = TempDb::new();
    let store = Store::open(&db.path).await.expect("open store");
    let bus = EventBus::new(store);
    let mut sub = bus.subscribe(SubscriptionFilter::default());

    let dir = TempDir::new("cd");
    let ws = WorkspaceId::from("ws-1");
    let _watcher = FileWatcher::start(
        &SharedWatchHub::new(),
        bus.clone(),
        ws.clone(),
        &dir.path.clone(),
    );

    // Let the OS watch establish before mutating (FSEvents/inotify warm-up).
    _watcher.wait_established(LIVENESS).await;
    tokio::time::sleep(Duration::from_millis(250)).await;

    let file = dir.path.join("foo.txt");
    std::fs::write(&file, b"hello").expect("write file");

    let ev = next_for(&mut sub, "foo.txt", None, LIVENESS)
        .await
        .expect("create/modify event for foo.txt");
    assert_eq!(ev.data["path"], "foo.txt");
    assert_eq!(ev.data["relativePath"], "foo.txt");
    assert_eq!(ev.actor.actor_type, ActorType::System);
    // A fresh write coalesces to `create` (file:created); if the OS only reports
    // the content write it stays `modify` (file:changed). Either way the type
    // tracks the action per the TS `getEventType` taxonomy.
    let action = ev.data["action"].as_str().expect("action string");
    assert!(action == "create" || action == "modify", "got {action}");
    let expected_type = if action == "create" {
        "file:created"
    } else {
        "file:changed"
    };
    assert_eq!(ev.event_type, expected_type);

    std::fs::remove_file(&file).expect("remove file");
    let ev = next_for(&mut sub, "foo.txt", Some("delete"), LIVENESS)
        .await
        .expect("delete event for foo.txt");
    assert_eq!(ev.event_type, "file:deleted");
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
    let _watcher = FileWatcher::start(
        &SharedWatchHub::new(),
        bus.clone(),
        WorkspaceId::from("ws-1"),
        &dir.path.clone(),
    );
    _watcher.wait_established(LIVENESS).await;
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

#[tokio::test]
async fn modifying_pre_existing_file_emits_changed_event() {
    let db = TempDb::new();
    let store = Store::open(&db.path).await.expect("open store");
    let bus = EventBus::new(store);
    let mut sub = bus.subscribe(SubscriptionFilter::default());

    let dir = TempDir::new("mod");
    let file = dir.path.join("bar.txt");
    // Pre-seed the file BEFORE the watcher starts so notify doesn't see the
    // initial create and coalesce it with the subsequent modify.
    std::fs::write(&file, b"v1").expect("write v1");
    tokio::time::sleep(Duration::from_millis(100)).await;

    let _watcher = FileWatcher::start(
        &SharedWatchHub::new(),
        bus.clone(),
        WorkspaceId::from("ws-1"),
        &dir.path.clone(),
    );
    _watcher.wait_established(LIVENESS).await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    std::fs::write(&file, b"v2 longer content").expect("write v2");
    let ev = next_for(&mut sub, "bar.txt", None, LIVENESS)
        .await
        .expect("event for bar.txt after modify");
    // FSEvents/inotify normalize a write to an existing path differently per
    // OS (modify on Linux, sometimes create on macOS due to cumulative flags),
    // so accept any non-delete action and assert the event_type follows the
    // TS taxonomy for that action.
    let action = ev.data["action"].as_str().expect("action string");
    assert_ne!(action, "delete", "writing must not produce a delete event");
    let expected_type = match action {
        "create" => "file:created",
        "modify" | "rename" => "file:changed",
        other => panic!("unexpected action {other}"),
    };
    assert_eq!(ev.event_type, expected_type);
    assert_eq!(ev.data["path"], "bar.txt");
    assert_eq!(ev.data["relativePath"], "bar.txt");
}

#[tokio::test]
async fn burst_above_threshold_collapses_to_directory_summaries() {
    let db = TempDb::new();
    let store = Store::open(&db.path).await.expect("open store");
    let bus = EventBus::new(store.clone());
    let mut sub = bus.subscribe(SubscriptionFilter::default());

    let dir = TempDir::new("burst");
    let ws = WorkspaceId::from("ws-burst");
    let _watcher = FileWatcher::start(
        &SharedWatchHub::new(),
        bus.clone(),
        ws.clone(),
        &dir.path.clone(),
    );
    _watcher.wait_established(LIVENESS).await;
    tokio::time::sleep(Duration::from_millis(250)).await;

    // Create 150 files rapidly (all within a few ms) so they accumulate in the
    // pending map before the debounce timer fires, ensuring they all flush together.
    let files: Vec<_> = (0..150)
        .map(|i| dir.path.join(format!("file{i:03}.txt")))
        .collect();
    for file in &files {
        std::fs::write(file, b"burst").expect("write file");
    }

    // Drain all file:* events from the subscription. Poll for up to `LIVENESS`
    // to allow the watcher to process all events, even under slow coverage
    // instrumentation. Exit when we see a burst event followed by 1s of silence,
    // or when we hit the overall deadline.
    let mut events = Vec::new();
    let start = Instant::now();
    let deadline = Instant::now() + LIVENESS;
    let mut seen_burst = false;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        // Once we've seen a burst event, wait up to 1s for stragglers (but no
        // longer than the overall deadline), then exit. Otherwise keep polling
        // until the deadline.
        let poll_timeout = if seen_burst {
            Duration::from_millis(1000).min(remaining)
        } else {
            remaining
        };
        match timeout(poll_timeout, sub.recv()).await {
            Ok(Some(batch)) => {
                for ev in batch {
                    if ev.event_type.starts_with("file:") {
                        if ev.data.get("burst").and_then(serde_json::Value::as_bool) == Some(true) {
                            seen_burst = true;
                        }
                        eprintln!(
                            "[STAB-121] +{:>6}ms #{:<3} type={} path={} action={} burst={:?} affected={:?}",
                            start.elapsed().as_millis(),
                            events.len(),
                            ev.event_type,
                            ev.data["relativePath"],
                            ev.data["action"],
                            ev.data.get("burst"),
                            ev.data.get("affectedCount"),
                        );
                        events.push(ev);
                    }
                }
            }
            Ok(None) => {
                // Subscription closed unexpectedly
                assert!(
                    seen_burst,
                    "Event subscription closed before burst was observed; \
                     got {} events total",
                    events.len()
                );
                break;
            }
            Err(_) => {
                // Timeout
                if seen_burst {
                    // Quiet period after burst => done collecting
                    break;
                }
                // else keep polling until deadline
            }
        }
    }

    // The test requires a burst event to have been seen. If the system is under such
    // extreme load that the FS watcher never delivered the burst, fail explicitly.
    assert!(
        seen_burst,
        "FS watcher did not deliver a burst event within {LIVENESS:?}; \
         got {} events total (system may be under extreme load, but the test cannot pass)",
        events.len()
    );

    // Should have emitted fewer events than the 150 individual files.
    // When burst threshold is exceeded, we emit per-directory summaries instead.
    assert!(
        events.len() < 80,
        "Expected burst collapse to <80 events, got {} events (should be << 150)",
        events.len()
    );

    // At least one event should have burst=true indicating coalescing occurred.
    let burst_events: Vec<_> = events
        .iter()
        .filter(|e| e.data.get("burst").and_then(serde_json::Value::as_bool) == Some(true))
        .collect();
    assert!(
        !burst_events.is_empty(),
        "expected at least one burst event, got {} normal events",
        events.len()
    );

    // Verify burst events report the files they collapsed.
    let total_affected: u64 = burst_events
        .iter()
        .filter_map(|e| {
            e.data
                .get("affectedCount")
                .and_then(serde_json::Value::as_u64)
        })
        .sum();
    assert!(
        total_affected >= 50,
        "burst events should cover significant files, got {total_affected}"
    );
}

#[tokio::test]
async fn normal_single_file_edit_still_emits_individual_event() {
    let db = TempDb::new();
    let store = Store::open(&db.path).await.expect("open store");
    let bus = EventBus::new(store);
    let mut sub = bus.subscribe(SubscriptionFilter::default());

    let dir = TempDir::new("single");
    let ws = WorkspaceId::from("ws-single");
    let _watcher = FileWatcher::start(
        &SharedWatchHub::new(),
        bus.clone(),
        ws.clone(),
        &dir.path.clone(),
    );
    _watcher.wait_established(LIVENESS).await;
    tokio::time::sleep(Duration::from_millis(250)).await;

    let file = dir.path.join("single.txt");
    std::fs::write(&file, b"content").expect("write file");

    let ev = next_for(&mut sub, "single.txt", None, LIVENESS)
        .await
        .expect("single-file event");
    // Should be a normal individual event, not a burst summary.
    assert!(
        ev.data.get("burst").is_none() || ev.data["burst"] == false,
        "normal edit should not be a burst event"
    );
    assert_eq!(ev.data["relativePath"], "single.txt");
}

/// Drain already-published `file:*` events from the subscription; stops after
/// `quiet` with no new batch. Used by the deterministic `flush_due` tests
/// below, where every publish has completed before the first `recv`.
async fn drain_file_events(sub: &mut super::bus::Subscription, quiet: Duration) -> Vec<Event> {
    let mut events = Vec::new();
    while let Ok(Some(batch)) = timeout(quiet, sub.recv()).await {
        for ev in batch {
            if ev.event_type.starts_with("file:") {
                events.push(ev);
            }
        }
    }
    events
}

fn burst_flag(ev: &Event) -> Option<bool> {
    ev.data.get("burst").and_then(serde_json::Value::as_bool)
}

fn affected_sum(events: &[Event]) -> u64 {
    events
        .iter()
        .filter_map(|e| {
            e.data
                .get("affectedCount")
                .and_then(serde_json::Value::as_u64)
        })
        .sum()
}

/// STAB-121 regression: a bulk churn whose per-path deadlines are spread out
/// (staggered OS delivery / slow publishes) comes due across several flushes
/// that are each below the burst threshold. The burst decision must look at
/// the whole pending backlog, not just the instantaneously-due set.
#[tokio::test]
async fn backlog_above_threshold_collapses_flush_even_when_due_set_is_small() {
    let db = TempDb::new();
    let store = Store::open(&db.path).await.expect("open store");
    let bus = EventBus::new(store);
    let mut sub = bus.subscribe(SubscriptionFilter::default());

    let now = Instant::now();
    let mut pending: HashMap<String, (Action, Instant)> = HashMap::new();
    // 40 paths already due, 110 more still inside their debounce window:
    // 150 in-flight total, well above BURST_THRESHOLD (100).
    for i in 0..40 {
        pending.insert(
            format!("due{i:03}.txt"),
            (Action::Create, now - Duration::from_millis(5)),
        );
    }
    for i in 0..110 {
        pending.insert(
            format!("later{i:03}.txt"),
            (Action::Create, now + Duration::from_secs(60)),
        );
    }

    let mut burst_until = None;
    flush_due(
        &bus,
        &WorkspaceId::from("ws-backlog"),
        &mut pending,
        &mut burst_until,
    )
    .await;

    assert_eq!(pending.len(), 110, "only due paths are flushed");
    assert!(
        burst_until.is_some(),
        "collapsed flush must arm the cooldown"
    );
    let events = drain_file_events(&mut sub, Duration::from_millis(300)).await;
    assert!(!events.is_empty(), "expected directory summaries");
    for ev in &events {
        assert_eq!(
            burst_flag(ev),
            Some(true),
            "large backlog must collapse to summaries, got {:?}",
            ev.data
        );
    }
    assert_eq!(
        affected_sum(&events),
        40,
        "summaries must cover all due paths"
    );
}

/// STAB-121 regression: after a burst flush, a trailing wave of the same churn
/// (e.g. late modify re-notifications) that is below the threshold on its own
/// must still collapse while the cooldown is active — without extending it,
/// so unrelated small activity cannot keep the watcher in summary mode.
#[tokio::test]
async fn cooldown_collapses_trailing_wave_below_threshold() {
    let db = TempDb::new();
    let store = Store::open(&db.path).await.expect("open store");
    let bus = EventBus::new(store);
    let mut sub = bus.subscribe(SubscriptionFilter::default());

    let now = Instant::now();
    let mut pending: HashMap<String, (Action, Instant)> = HashMap::new();
    for i in 0..60 {
        pending.insert(
            format!("trail{i:03}.txt"),
            (Action::Modify, now - Duration::from_millis(5)),
        );
    }

    let cooldown_end = now + Duration::from_millis(500);
    let mut burst_until = Some(cooldown_end);
    flush_due(
        &bus,
        &WorkspaceId::from("ws-trail"),
        &mut pending,
        &mut burst_until,
    )
    .await;

    assert!(pending.is_empty(), "all due paths are flushed");
    assert_eq!(
        burst_until,
        Some(cooldown_end),
        "cooldown-only collapse must consume the window, not extend it"
    );
    let events = drain_file_events(&mut sub, Duration::from_millis(300)).await;
    assert!(!events.is_empty(), "expected directory summaries");
    for ev in &events {
        assert_eq!(
            burst_flag(ev),
            Some(true),
            "trailing wave inside cooldown must collapse, got {:?}",
            ev.data
        );
    }
    assert_eq!(affected_sum(&events), 60);
}

/// The documented trade-off, pinned: while the cooldown is active even a lone
/// file change flushes as a directory summary (`burst=true, affectedCount=1`)
/// instead of a per-file event.
#[tokio::test]
async fn cooldown_collapses_single_file_flush() {
    let db = TempDb::new();
    let store = Store::open(&db.path).await.expect("open store");
    let bus = EventBus::new(store);
    let mut sub = bus.subscribe(SubscriptionFilter::default());

    let now = Instant::now();
    let mut pending: HashMap<String, (Action, Instant)> = HashMap::new();
    pending.insert(
        "lone.txt".to_string(),
        (Action::Modify, now - Duration::from_millis(5)),
    );

    let cooldown_end = now + Duration::from_millis(500);
    let mut burst_until = Some(cooldown_end);
    flush_due(
        &bus,
        &WorkspaceId::from("ws-lone"),
        &mut pending,
        &mut burst_until,
    )
    .await;

    assert!(pending.is_empty());
    assert_eq!(burst_until, Some(cooldown_end), "cooldown not extended");
    let events = drain_file_events(&mut sub, Duration::from_millis(300)).await;
    assert_eq!(events.len(), 1);
    assert_eq!(burst_flag(&events[0]), Some(true));
    assert_eq!(affected_sum(&events), 1);
}

/// A small flush with no backlog and an expired cooldown keeps the normal
/// per-file behavior.
#[tokio::test]
async fn small_flush_after_cooldown_expiry_emits_individual_events() {
    let db = TempDb::new();
    let store = Store::open(&db.path).await.expect("open store");
    let bus = EventBus::new(store);
    let mut sub = bus.subscribe(SubscriptionFilter::default());

    let now = Instant::now();
    let mut pending: HashMap<String, (Action, Instant)> = HashMap::new();
    for i in 0..5 {
        pending.insert(
            format!("solo{i}.txt"),
            (Action::Create, now - Duration::from_millis(5)),
        );
    }

    let expired = now - Duration::from_millis(100);
    let mut burst_until = Some(expired);
    flush_due(
        &bus,
        &WorkspaceId::from("ws-solo"),
        &mut pending,
        &mut burst_until,
    )
    .await;

    assert!(pending.is_empty());
    assert_eq!(
        burst_until,
        Some(expired),
        "per-file flush must not re-arm the cooldown"
    );
    let events = drain_file_events(&mut sub, Duration::from_millis(300)).await;
    assert_eq!(events.len(), 5, "each file gets its own event");
    for ev in &events {
        assert_ne!(
            burst_flag(ev),
            Some(true),
            "small flush must not summarize, got {:?}",
            ev.data
        );
    }
}

#[tokio::test]
async fn dedupe_within_window_emits_one_event_per_path() {
    let db = TempDb::new();
    let store = Store::open(&db.path).await.expect("open store");
    let bus = EventBus::new(store);
    let mut sub = bus.subscribe(SubscriptionFilter::default());

    let dir = TempDir::new("dedupe");
    let ws = WorkspaceId::from("ws-dedupe");
    let _watcher = FileWatcher::start(
        &SharedWatchHub::new(),
        bus.clone(),
        ws.clone(),
        &dir.path.clone(),
    );
    _watcher.wait_established(LIVENESS).await;
    tokio::time::sleep(Duration::from_millis(250)).await;

    let file = dir.path.join("dedupe.txt");
    // Rapidly write to the same file multiple times within the debounce window.
    for i in 0..5 {
        std::fs::write(&file, format!("v{i}")).expect("write file");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Wait for debounce to flush.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Collect all file:* events for "dedupe.txt": wait up to `LIVENESS` for
    // the FIRST event (positive — delivery may lag under parallel load,
    // monorepo#1630), then keep draining through a short quiet window to
    // catch would-be duplicates (negative — stays short).
    let mut count = 0;
    let deadline = Instant::now() + LIVENESS;
    loop {
        let wait = if count > 0 {
            Duration::from_secs(2)
        } else {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            remaining
        };
        match timeout(wait, sub.recv()).await {
            Ok(Some(batch)) => {
                for ev in batch {
                    if ev.event_type.starts_with("file:") && ev.data["relativePath"] == "dedupe.txt"
                    {
                        count += 1;
                    }
                }
            }
            _ => break,
        }
    }

    // Should emit exactly one event for the path (debounce coalesces the 5 writes).
    assert_eq!(
        count, 1,
        "Expected 1 coalesced event, got {count} for rapid writes"
    );
}

// ---------------------------------------------------------------------------
// Gitignore suppression (intent-hq/monorepo#1457): ignored paths must never
// surface as `file:*` events. Each test writes the suppressed path(s) first
// and then a non-ignored *control* file: the control event arriving proves the
// watcher processed the batch (and that non-ignored untracked files still
// emit), while any event for a suppressed path fails the assertion.
//
// Host dependency: whenever the temp dir is a git repo, the watcher loads the
// *host's* global excludes (`core.excludesFile` / `~/.config/git/ignore`) via
// `Gitignore::global()`. That lookup reads process-wide env (`HOME`,
// `XDG_CONFIG_HOME`, git config) at rebuild time, and env mutation races
// parallel tests in the same process, so we can't hermetically neutralize it
// here (cf. the host-independence fix in intentd#899, which had a
// subprocess boundary to scope env to). These tests therefore assume the
// host has no pathological global excludes — i.e. no pattern or `!` negation
// matching `.gitignore`, the suppressed fixtures, or the `*control*`/
// `*.secret`/`dist*` filenames used below.
// ---------------------------------------------------------------------------

/// `git init` the temp dir (plus user config) so the watcher sees a real repo.
fn git_init(root: &Path) {
    let run = |args: &[&str]| {
        let out = std::process::Command::new("git")
            .arg("-C")
            .arg(root)
            .args(args)
            .output()
            .expect("run git");
        assert!(
            out.status.success(),
            "git {args:?} failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    };
    run(&["init", "-q"]);
    run(&["config", "user.email", "watcher-test@example.com"]);
    run(&["config", "user.name", "Watcher Test"]);
}

/// Wait until the control path's event arrives (up to `LIVENESS`), asserting
/// no `file:*` event for any suppressed path shows up — then keep draining
/// until an 800 ms quiet window passes to catch stragglers flushed after the
/// control.
async fn expect_suppressed(sub: &mut super::bus::Subscription, suppressed: &[&str], control: &str) {
    let deadline = Instant::now() + LIVENESS;
    let mut seen_control = false;
    loop {
        let wait = if seen_control {
            Duration::from_millis(800)
        } else {
            let remaining = deadline.saturating_duration_since(Instant::now());
            assert!(
                !remaining.is_zero(),
                "control event for {control} never arrived"
            );
            remaining
        };
        match timeout(wait, sub.recv()).await {
            Ok(Some(batch)) => {
                for ev in batch {
                    if !ev.event_type.starts_with("file:") {
                        continue;
                    }
                    let rel = ev.data["relativePath"].as_str().unwrap_or_default();
                    assert!(
                        !suppressed.contains(&rel),
                        "suppressed path {rel} emitted an event: {:?}",
                        ev.data
                    );
                    if rel == control {
                        seen_control = true;
                    }
                }
            }
            Ok(None) => panic!("subscription closed before control event for {control}"),
            Err(_) => {
                assert!(seen_control, "control event for {control} never arrived");
                return;
            }
        }
    }
}

#[tokio::test]
async fn gitignored_generated_dir_is_suppressed() {
    let db = TempDb::new();
    let store = Store::open(&db.path).await.expect("open store");
    let bus = EventBus::new(store);
    let mut sub = bus.subscribe(SubscriptionFilter::default());

    let dir = TempDir::new("gi-gen");
    git_init(&dir.path);
    std::fs::write(dir.path.join(".gitignore"), ".svelte-kit/\n").expect("write .gitignore");
    std::fs::create_dir_all(dir.path.join(".svelte-kit/output")).expect("mk .svelte-kit");
    let _watcher = FileWatcher::start(
        &SharedWatchHub::new(),
        bus.clone(),
        WorkspaceId::from("ws-gi"),
        &dir.path.clone(),
    );
    _watcher.wait_established(LIVENESS).await;
    tokio::time::sleep(Duration::from_millis(250)).await;

    std::fs::write(dir.path.join(".svelte-kit/output/x.d.ts"), b"x").expect("write ignored");
    std::fs::write(dir.path.join(".svelte-kit/output/x.d.ts"), b"xy").expect("modify ignored");
    tokio::time::sleep(Duration::from_millis(100)).await;
    std::fs::write(dir.path.join("control-gen.txt"), b"c").expect("write control");

    expect_suppressed(
        &mut sub,
        &[
            ".svelte-kit",
            ".svelte-kit/output",
            ".svelte-kit/output/x.d.ts",
        ],
        "control-gen.txt",
    )
    .await;
}

#[tokio::test]
async fn user_ignored_path_suppressed_across_create_modify_delete() {
    let db = TempDb::new();
    let store = Store::open(&db.path).await.expect("open store");
    let bus = EventBus::new(store);
    let mut sub = bus.subscribe(SubscriptionFilter::default());

    let dir = TempDir::new("gi-user");
    git_init(&dir.path);
    std::fs::write(dir.path.join(".gitignore"), "scratch.txt\n").expect("write .gitignore");
    let _watcher = FileWatcher::start(
        &SharedWatchHub::new(),
        bus.clone(),
        WorkspaceId::from("ws-gi"),
        &dir.path.clone(),
    );
    _watcher.wait_established(LIVENESS).await;
    tokio::time::sleep(Duration::from_millis(250)).await;

    let scratch = dir.path.join("scratch.txt");
    std::fs::write(&scratch, b"v1").expect("create ignored");
    tokio::time::sleep(Duration::from_millis(400)).await;
    std::fs::write(&scratch, b"v2 longer").expect("modify ignored");
    tokio::time::sleep(Duration::from_millis(400)).await;
    std::fs::remove_file(&scratch).expect("delete ignored");
    tokio::time::sleep(Duration::from_millis(100)).await;
    std::fs::write(dir.path.join("untracked-control.txt"), b"c").expect("write control");

    expect_suppressed(&mut sub, &["scratch.txt"], "untracked-control.txt").await;
}

#[tokio::test]
async fn nested_gitignore_applies_only_under_its_directory() {
    let db = TempDb::new();
    let store = Store::open(&db.path).await.expect("open store");
    let bus = EventBus::new(store);
    let mut sub = bus.subscribe(SubscriptionFilter::default());

    let dir = TempDir::new("gi-nested");
    git_init(&dir.path);
    std::fs::create_dir_all(dir.path.join("sub")).expect("mk sub");
    std::fs::write(dir.path.join("sub/.gitignore"), "*.secret\n").expect("write nested");
    let _watcher = FileWatcher::start(
        &SharedWatchHub::new(),
        bus.clone(),
        WorkspaceId::from("ws-gi"),
        &dir.path.clone(),
    );
    _watcher.wait_established(LIVENESS).await;
    tokio::time::sleep(Duration::from_millis(250)).await;

    std::fs::write(dir.path.join("sub/data.secret"), b"x").expect("write ignored");
    tokio::time::sleep(Duration::from_millis(100)).await;
    // The control matches the nested pattern but sits *outside* `sub/`, so it
    // must still emit — proving the nested file is scoped to its directory.
    std::fs::write(dir.path.join("top.secret"), b"c").expect("write control");

    expect_suppressed(&mut sub, &["sub/data.secret"], "top.secret").await;
}

#[tokio::test]
async fn git_info_exclude_is_honored() {
    let db = TempDb::new();
    let store = Store::open(&db.path).await.expect("open store");
    let bus = EventBus::new(store);
    let mut sub = bus.subscribe(SubscriptionFilter::default());

    let dir = TempDir::new("gi-excl");
    git_init(&dir.path);
    std::fs::create_dir_all(dir.path.join(".git/info")).expect("mk info");
    std::fs::write(dir.path.join(".git/info/exclude"), "excluded-local.txt\n")
        .expect("write exclude");
    let _watcher = FileWatcher::start(
        &SharedWatchHub::new(),
        bus.clone(),
        WorkspaceId::from("ws-gi"),
        &dir.path.clone(),
    );
    _watcher.wait_established(LIVENESS).await;
    tokio::time::sleep(Duration::from_millis(250)).await;

    std::fs::write(dir.path.join("excluded-local.txt"), b"x").expect("write ignored");
    tokio::time::sleep(Duration::from_millis(100)).await;
    std::fs::write(dir.path.join("exclude-control.txt"), b"c").expect("write control");

    expect_suppressed(&mut sub, &["excluded-local.txt"], "exclude-control.txt").await;
}

#[tokio::test]
async fn negation_reincludes_file_in_ignored_dir() {
    let db = TempDb::new();
    let store = Store::open(&db.path).await.expect("open store");
    let bus = EventBus::new(store);
    let mut sub = bus.subscribe(SubscriptionFilter::default());

    let dir = TempDir::new("gi-neg");
    git_init(&dir.path);
    std::fs::write(dir.path.join(".gitignore"), "dist2/\n!dist2/keep.txt\n")
        .expect("write .gitignore");
    std::fs::create_dir_all(dir.path.join("dist2")).expect("mk dist2");
    let _watcher = FileWatcher::start(
        &SharedWatchHub::new(),
        bus.clone(),
        WorkspaceId::from("ws-gi"),
        &dir.path.clone(),
    );
    _watcher.wait_established(LIVENESS).await;
    tokio::time::sleep(Duration::from_millis(250)).await;

    std::fs::write(dir.path.join("dist2/other.txt"), b"x").expect("write ignored");
    tokio::time::sleep(Duration::from_millis(100)).await;
    // The negated path is itself the control: it must emit despite `dist2/`.
    std::fs::write(dir.path.join("dist2/keep.txt"), b"c").expect("write control");

    expect_suppressed(&mut sub, &["dist2/other.txt", "dist2"], "dist2/keep.txt").await;
}

#[tokio::test]
async fn gitignore_edit_takes_effect_without_restart() {
    let db = TempDb::new();
    let store = Store::open(&db.path).await.expect("open store");
    let bus = EventBus::new(store);
    let mut sub = bus.subscribe(SubscriptionFilter::default());

    let dir = TempDir::new("gi-edit");
    git_init(&dir.path);
    std::fs::write(dir.path.join(".gitignore"), "initial-ignored.txt\n").expect("write .gitignore");
    let _watcher = FileWatcher::start(
        &SharedWatchHub::new(),
        bus.clone(),
        WorkspaceId::from("ws-gi"),
        &dir.path.clone(),
    );
    _watcher.wait_established(LIVENESS).await;
    tokio::time::sleep(Duration::from_millis(250)).await;

    // Not ignored yet: must emit.
    std::fs::write(dir.path.join("runtime-ignored.txt"), b"v1").expect("write pre-rule");
    next_for(&mut sub, "runtime-ignored.txt", None, LIVENESS)
        .await
        .expect("event before the ignore rule exists");

    // Add the rule at runtime; the `.gitignore` event itself marks the matcher
    // dirty and it rebuilds on next use — no daemon restart.
    std::fs::write(
        dir.path.join(".gitignore"),
        "initial-ignored.txt\nruntime-ignored.txt\n",
    )
    .expect("edit .gitignore");
    next_for(&mut sub, ".gitignore", None, LIVENESS)
        .await
        .expect("event for the .gitignore edit");

    std::fs::write(dir.path.join("runtime-ignored.txt"), b"v2 longer").expect("write ignored");
    tokio::time::sleep(Duration::from_millis(100)).await;
    std::fs::write(dir.path.join("after-edit-control.txt"), b"c").expect("write control");

    expect_suppressed(&mut sub, &["runtime-ignored.txt"], "after-edit-control.txt").await;
}

#[tokio::test]
async fn default_patterns_apply_without_gitignore_rule() {
    let db = TempDb::new();
    let store = Store::open(&db.path).await.expect("open store");
    let bus = EventBus::new(store);
    let mut sub = bus.subscribe(SubscriptionFilter::default());

    let dir = TempDir::new("gi-def");
    git_init(&dir.path);
    // The repo's .gitignore does NOT mention *.log or .env — the TS-parity
    // defaults must suppress them on their own.
    std::fs::write(dir.path.join(".gitignore"), "unrelated.txt\n").expect("write .gitignore");
    let _watcher = FileWatcher::start(
        &SharedWatchHub::new(),
        bus.clone(),
        WorkspaceId::from("ws-gi"),
        &dir.path.clone(),
    );
    _watcher.wait_established(LIVENESS).await;
    tokio::time::sleep(Duration::from_millis(250)).await;

    std::fs::write(dir.path.join("app.log"), b"x").expect("write log");
    std::fs::write(dir.path.join(".env"), b"SECRET=1").expect("write env");
    tokio::time::sleep(Duration::from_millis(100)).await;
    std::fs::write(dir.path.join("default-control.txt"), b"c").expect("write control");

    expect_suppressed(&mut sub, &["app.log", ".env"], "default-control.txt").await;
}

/// PR 903 review regression: a runtime `info/exclude` edit lands as a raw
/// event whose path is prefiltered (it lives under `.git`), so nothing else
/// forces a rebuild. The ingest fast-path must not consult a stale
/// `has_whitelists` — a freshly added `!dist` negation has to rescue
/// `dist/…` (an [`IGNORED_DIRS`] entry) on the very next event, and the
/// exclude-path comparison must hold whether notify reports the canonical or
/// the resolved form of the path.
#[tokio::test]
async fn runtime_info_exclude_negation_rescues_prefiltered_path() {
    let db = TempDb::new();
    let store = Store::open(&db.path).await.expect("open store");
    let bus = EventBus::new(store);
    let mut sub = bus.subscribe(SubscriptionFilter::default());

    let dir = TempDir::new("gi-excl-edit");
    git_init(&dir.path);
    std::fs::create_dir_all(dir.path.join(".git/info")).expect("mk info");
    std::fs::create_dir_all(dir.path.join("dist")).expect("mk dist");
    let _watcher = FileWatcher::start(
        &SharedWatchHub::new(),
        bus.clone(),
        WorkspaceId::from("ws-gi"),
        &dir.path.clone(),
    );
    _watcher.wait_established(LIVENESS).await;
    tokio::time::sleep(Duration::from_millis(250)).await;

    // Edit info/exclude at runtime: the raw event for this path is the ONLY
    // dirty trigger — it is prefiltered, so a stale fast-path would skip the
    // rebuild and keep dropping `dist/…` below.
    std::fs::write(dir.path.join(".git/info/exclude"), "!dist\n").expect("write exclude");
    tokio::time::sleep(Duration::from_millis(400)).await;

    std::fs::write(dir.path.join("dist/bundle.js"), b"js").expect("write negated");
    let ev = next_for(&mut sub, "dist/bundle.js", None, LIVENESS)
        .await
        .expect("runtime exclude negation must rescue the prefiltered path");
    assert_eq!(ev.data["relativePath"], "dist/bundle.js");
}

#[tokio::test]
async fn user_negation_overrides_default_pattern() {
    let db = TempDb::new();
    let store = Store::open(&db.path).await.expect("open store");
    let bus = EventBus::new(store);
    let mut sub = bus.subscribe(SubscriptionFilter::default());

    let dir = TempDir::new("gi-override");
    git_init(&dir.path);
    // `dist` is both a default pattern and an IGNORED_DIRS entry; a user
    // negation must win over both.
    std::fs::write(dir.path.join(".gitignore"), "!dist\n").expect("write .gitignore");
    std::fs::create_dir_all(dir.path.join("dist")).expect("mk dist");
    let _watcher = FileWatcher::start(
        &SharedWatchHub::new(),
        bus.clone(),
        WorkspaceId::from("ws-gi"),
        &dir.path.clone(),
    );
    _watcher.wait_established(LIVENESS).await;
    tokio::time::sleep(Duration::from_millis(250)).await;

    std::fs::write(dir.path.join("dist/bundle.js"), b"js").expect("write negated");
    let ev = next_for(&mut sub, "dist/bundle.js", None, LIVENESS)
        .await
        .expect("negated default must emit");
    assert_eq!(ev.data["relativePath"], "dist/bundle.js");
}

#[tokio::test]
async fn non_git_root_still_applies_default_patterns() {
    let db = TempDb::new();
    let store = Store::open(&db.path).await.expect("open store");
    let bus = EventBus::new(store);
    let mut sub = bus.subscribe(SubscriptionFilter::default());

    // No git init: defaults must still suppress, non-default files still emit.
    let dir = TempDir::new("gi-nongit");
    std::fs::create_dir_all(dir.path.join(".svelte-kit")).expect("mk .svelte-kit");
    let _watcher = FileWatcher::start(
        &SharedWatchHub::new(),
        bus.clone(),
        WorkspaceId::from("ws-gi"),
        &dir.path.clone(),
    );
    _watcher.wait_established(LIVENESS).await;
    tokio::time::sleep(Duration::from_millis(250)).await;

    std::fs::write(dir.path.join(".svelte-kit/x.js"), b"x").expect("write ignored");
    std::fs::write(dir.path.join("noise.log"), b"x").expect("write log");
    tokio::time::sleep(Duration::from_millis(100)).await;
    std::fs::write(dir.path.join("non-git-control.txt"), b"c").expect("write control");

    expect_suppressed(
        &mut sub,
        &[".svelte-kit", ".svelte-kit/x.js", "noise.log"],
        "non-git-control.txt",
    )
    .await;
}
