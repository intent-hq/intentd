//! Unit tests for the browser tab registry repository (REV-2 Model 2 & 6).

use intent_core::{
    now_iso, BrowserTabInput, BrowserTabSize, BrowserTabUpsertOutcome, BrowserTabVisibility,
    ClientHostInfo, ClientId, Error, Workspace, WorkspaceActivity, WorkspaceAttention, WorkspaceId,
    WorkspaceStatus,
};
use serde_json::json;
use uuid::Uuid;

use crate::Store;

/// A unique temp DB path cleaned up on drop (mirrors `crate::tests::TempDb`,
/// which is private to that module).
struct TempDb {
    path: std::path::PathBuf,
}

impl TempDb {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("test-browser-tab-{}.db", Uuid::new_v4()));
        Self { path }
    }
}

impl Drop for TempDb {
    fn drop(&mut self) {
        for suffix in ["", "-wal", "-shm"] {
            let mut sidecar = self.path.clone().into_os_string();
            sidecar.push(suffix);
            let _ = std::fs::remove_file(&sidecar);
        }
    }
}

fn test_workspace(ws_id: &WorkspaceId, ts: &str) -> Workspace {
    Workspace {
        id: ws_id.clone(),
        title: "Test".to_string(),
        branch: "main".to_string(),
        base_ref: None,
        base_commit_sha: None,
        status: WorkspaceStatus::Active,
        status_message: None,
        status_image_asset_id: None,
        activity: WorkspaceActivity::Idle,
        attention: WorkspaceAttention::None,
        created_at: ts.to_string(),
        updated_at: ts.to_string(),
        last_activity: None,
        tags: vec![],
        path: None,
        repository_path: None,
        repository_owner: None,
        repository_name: None,
        worktree_path: None,
        scope: None,
        skip_worktree: false,
        setup_script: None,
        is_remote: false,
        default_model: None,
        pr_number: None,
        pr_url: None,
        pr_status: None,
        active_pull_request: None,
        pull_requests: None,
        context_links: None,
        archived: false,
        archived_at: None,
        task_stats: None,
        agent_summary: None,
        diff_summary: None,
        token_usage: None,
        cow_supported: None,
        display_status: None,
        waiting: false,
        checkout_mode: None,
        disk_usage: None,
        pending_delete_at: None,
        browser_client_id: None,
    }
}

struct Fixture {
    _tmp: TempDb,
    store: Store,
    ws: WorkspaceId,
    host_a: ClientId,
    host_b: ClientId,
}

async fn fixture() -> Fixture {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ts = now_iso();
    let ws = WorkspaceId(format!("ws-{}", Uuid::new_v4()));
    store
        .insert_workspace(&test_workspace(&ws, &ts))
        .await
        .expect("insert workspace");
    let host_a = ClientId("client-a".to_string());
    let host_b = ClientId("client-b".to_string());
    for (id, name) in [(&host_a, "Desktop A"), (&host_b, "Desktop B")] {
        store
            .upsert_client(
                id,
                Some(name),
                Some(&json!({ "browserExec": true })),
                &ClientHostInfo::default(),
            )
            .await
            .expect("upsert client");
    }
    Fixture {
        _tmp: tmp,
        store,
        ws,
        host_a,
        host_b,
    }
}

fn input(ws: &WorkspaceId, tab_id: &str, url: &str) -> BrowserTabInput {
    BrowserTabInput {
        tab_id: tab_id.to_string(),
        workspace_id: ws.clone(),
        url: url.to_string(),
        requested_url: None,
        title: None,
        owner_agent_id: None,
        owner_agent_name: None,
        visibility: BrowserTabVisibility::Visible,
        emulated_size: None,
    }
}

#[tokio::test]
async fn upsert_new_then_update_then_noop() {
    let f = fixture().await;
    let opened = f
        .store
        .upsert_browser_tab(&f.host_a, input(&f.ws, "tab-1", "https://a.test/"))
        .await
        .unwrap();
    let BrowserTabUpsertOutcome::Opened(tab) = opened else {
        panic!("first report opens: {opened:?}");
    };
    assert_eq!(tab.host_client_id, f.host_a);
    assert_eq!(tab.url, "https://a.test/");
    assert_eq!(tab.created_at, tab.updated_at);

    let mut second = input(&f.ws, "tab-1", "https://a.test/page");
    second.title = Some("Page".to_string());
    second.visibility = BrowserTabVisibility::Hidden;
    second.emulated_size = Some(BrowserTabSize {
        width: 1280,
        height: 800,
    });
    let updated = f
        .store
        .upsert_browser_tab(&f.host_a, second.clone())
        .await
        .unwrap();
    let BrowserTabUpsertOutcome::Updated { tab, changes } = updated else {
        panic!("changed report updates: {updated:?}");
    };
    assert_eq!(
        changes,
        json!({
            "url": "https://a.test/page",
            "title": "Page",
            "visibility": "hidden",
            "emulatedSize": { "width": 1280, "height": 800 },
        })
    );
    assert_eq!(tab.title.as_deref(), Some("Page"));
    assert_eq!(tab.visibility, BrowserTabVisibility::Hidden);

    let again = f.store.upsert_browser_tab(&f.host_a, second).await.unwrap();
    assert!(
        matches!(again, BrowserTabUpsertOutcome::Unchanged(_)),
        "identical report is a no-op: {again:?}"
    );
    let stored = f.store.get_browser_tab("tab-1").await.unwrap().unwrap();
    assert_eq!(stored.updated_at, tab.updated_at);
    assert_eq!(
        stored.emulated_size,
        Some(BrowserTabSize {
            width: 1280,
            height: 800
        })
    );
}

#[tokio::test]
async fn foreign_host_writes_are_rejected() {
    let f = fixture().await;
    f.store
        .upsert_browser_tab(&f.host_a, input(&f.ws, "tab-1", "https://a.test/"))
        .await
        .unwrap();
    let err = f
        .store
        .upsert_browser_tab(&f.host_b, input(&f.ws, "tab-1", "https://b.test/"))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::InvalidParams(_)), "{err:?}");
    let err = f
        .store
        .remove_browser_tab(&f.host_b, "tab-1")
        .await
        .unwrap_err();
    assert!(matches!(err, Error::InvalidParams(_)), "{err:?}");
    // The row is untouched.
    let stored = f.store.get_browser_tab("tab-1").await.unwrap().unwrap();
    assert_eq!(stored.host_client_id, f.host_a);
    assert_eq!(stored.url, "https://a.test/");
}

#[tokio::test]
async fn remove_deletes_and_is_idempotent() {
    let f = fixture().await;
    f.store
        .upsert_browser_tab(&f.host_a, input(&f.ws, "tab-1", "https://a.test/"))
        .await
        .unwrap();
    let removed = f
        .store
        .remove_browser_tab(&f.host_a, "tab-1")
        .await
        .unwrap();
    assert_eq!(removed.map(|t| t.tab_id), Some("tab-1".to_string()));
    assert!(f.store.get_browser_tab("tab-1").await.unwrap().is_none());
    assert!(f
        .store
        .remove_browser_tab(&f.host_a, "tab-1")
        .await
        .unwrap()
        .is_none());
    assert!(f
        .store
        .remove_browser_tab(&f.host_a, "never-seen")
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn lists_scope_by_workspace_and_host_and_hide_tombstones() {
    let f = fixture().await;
    let ts = now_iso();
    let ws2 = WorkspaceId(format!("ws-{}", Uuid::new_v4()));
    f.store
        .insert_workspace(&test_workspace(&ws2, &ts))
        .await
        .unwrap();
    for (host, ws, id) in [
        (&f.host_a, &f.ws, "tab-a1"),
        (&f.host_a, &ws2, "tab-a2"),
        (&f.host_b, &f.ws, "tab-b1"),
    ] {
        f.store
            .upsert_browser_tab(host, input(ws, id, "https://x.test/"))
            .await
            .unwrap();
    }
    let ids =
        |tabs: Vec<intent_core::BrowserTab>| tabs.into_iter().map(|t| t.tab_id).collect::<Vec<_>>();
    assert_eq!(
        ids(f.store.list_browser_tabs(&f.ws).await.unwrap()),
        vec!["tab-a1", "tab-b1"]
    );
    assert_eq!(
        ids(f.store.list_browser_tabs_by_host(&f.host_a).await.unwrap()),
        vec!["tab-a1", "tab-a2"]
    );
    // Daemon-side close tombstones the row: gone from every list and from
    // `get`, but the id is still remembered for the host's next sync.
    let closed = f.store.close_browser_tab("tab-a1").await.unwrap();
    assert_eq!(closed.map(|t| t.tab_id), Some("tab-a1".to_string()));
    assert!(f.store.close_browser_tab("tab-a1").await.unwrap().is_none());
    assert_eq!(
        ids(f.store.list_browser_tabs(&f.ws).await.unwrap()),
        vec!["tab-b1"]
    );
    assert_eq!(
        ids(f.store.list_browser_tabs_by_host(&f.host_a).await.unwrap()),
        vec!["tab-a2"]
    );
    assert!(f.store.get_browser_tab("tab-a1").await.unwrap().is_none());
    // Unknown workspace / host: lenient empty reads.
    assert!(f
        .store
        .list_browser_tabs(&WorkspaceId("ws-ghost".to_string()))
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn sync_reconciles_host_snapshot() {
    let f = fixture().await;
    // Host A's known state: kept (unchanged), changed, gone-from-host, and
    // closed daemon-side while A was offline.
    for id in ["tab-keep", "tab-change", "tab-gone", "tab-daemon-closed"] {
        f.store
            .upsert_browser_tab(&f.host_a, input(&f.ws, id, "https://a.test/"))
            .await
            .unwrap();
    }
    f.store
        .close_browser_tab("tab-daemon-closed")
        .await
        .unwrap();
    // Host B owns one tab that A also (wrongly) reports.
    f.store
        .upsert_browser_tab(&f.host_b, input(&f.ws, "tab-b", "https://b.test/"))
        .await
        .unwrap();

    let mut changed = input(&f.ws, "tab-change", "https://a.test/new");
    changed.title = Some("New".to_string());
    let snapshot = vec![
        input(&f.ws, "tab-keep", "https://a.test/"),
        changed,
        input(&f.ws, "tab-daemon-closed", "https://a.test/"),
        input(&f.ws, "tab-b", "https://b.test/"),
        input(&f.ws, "tab-new", "https://a.test/fresh"),
        // Duplicate ids after the first are ignored.
        input(&f.ws, "tab-new", "https://a.test/dup"),
    ];
    let result = f
        .store
        .sync_browser_tabs(&f.host_a, snapshot)
        .await
        .unwrap();

    assert_eq!(result.drop, vec!["tab-daemon-closed", "tab-b"]);
    assert_eq!(
        result
            .opened
            .iter()
            .map(|t| (t.tab_id.as_str(), t.url.as_str()))
            .collect::<Vec<_>>(),
        vec![("tab-new", "https://a.test/fresh")]
    );
    assert_eq!(result.updated.len(), 1);
    assert_eq!(result.updated[0].0.tab_id, "tab-change");
    assert_eq!(
        result.updated[0].1,
        json!({ "url": "https://a.test/new", "title": "New" })
    );
    assert_eq!(
        result
            .closed
            .iter()
            .map(|t| t.tab_id.as_str())
            .collect::<Vec<_>>(),
        vec!["tab-gone"]
    );

    // Resulting state: A hosts keep/change/new; the tombstone is retained
    // (still reported ⇒ not yet acknowledged); B's tab is untouched.
    let mut a_ids: Vec<String> = f
        .store
        .list_browser_tabs_by_host(&f.host_a)
        .await
        .unwrap()
        .into_iter()
        .map(|t| t.tab_id)
        .collect();
    a_ids.sort();
    assert_eq!(a_ids, vec!["tab-change", "tab-keep", "tab-new"]);
    assert!(f.store.get_browser_tab("tab-gone").await.unwrap().is_none());
    assert!(
        f.store
            .get_stored_tab("tab-daemon-closed")
            .await
            .unwrap()
            .is_some_and(|s| s.closed),
        "a reported tombstone survives the sync"
    );
    let b = f.store.get_browser_tab("tab-b").await.unwrap().unwrap();
    assert_eq!(b.host_client_id, f.host_b);
    // A second sync that omits the tombstoned id acknowledges the drop and
    // purges it; otherwise a no-op: nothing to open, update or close.
    let again = f
        .store
        .sync_browser_tabs(
            &f.host_a,
            vec![
                input(&f.ws, "tab-keep", "https://a.test/"),
                {
                    let mut c = input(&f.ws, "tab-change", "https://a.test/new");
                    c.title = Some("New".to_string());
                    c
                },
                input(&f.ws, "tab-new", "https://a.test/fresh"),
                input(&f.ws, "tab-b", "https://b.test/"),
            ],
        )
        .await
        .unwrap();
    assert_eq!(again.drop, vec!["tab-b"]);
    assert!(again.opened.is_empty() && again.updated.is_empty() && again.closed.is_empty());
    assert!(
        f.store
            .get_stored_tab("tab-daemon-closed")
            .await
            .unwrap()
            .is_none(),
        "omitting the id purges the tombstone"
    );
}

#[tokio::test]
async fn repeated_stale_snapshot_keeps_answering_drop() {
    let f = fixture().await;
    f.store
        .upsert_browser_tab(&f.host_a, input(&f.ws, "tab-1", "https://a.test/"))
        .await
        .unwrap();
    f.store.close_browser_tab("tab-1").await.unwrap();
    // The host never saw the first `drop` (socket dropped mid-response) and
    // resends the identical snapshot on reconnect: every attempt must answer
    // `drop` and none may revive the row.
    for attempt in 0..3 {
        let result = f
            .store
            .sync_browser_tabs(&f.host_a, vec![input(&f.ws, "tab-1", "https://a.test/")])
            .await
            .unwrap();
        assert_eq!(result.drop, vec!["tab-1"], "attempt {attempt}");
        assert!(
            result.opened.is_empty() && result.updated.is_empty() && result.closed.is_empty(),
            "attempt {attempt}: {result:?}"
        );
        assert!(f.store.get_browser_tab("tab-1").await.unwrap().is_none());
        assert!(f.store.list_browser_tabs(&f.ws).await.unwrap().is_empty());
    }
    // The host's own remove acknowledges the drop too.
    assert!(f
        .store
        .remove_browser_tab(&f.host_a, "tab-1")
        .await
        .unwrap()
        .is_none());
    assert!(f.store.get_stored_tab("tab-1").await.unwrap().is_none());
}

#[tokio::test]
async fn upsert_never_revives_or_steals_a_tombstone() {
    let f = fixture().await;
    f.store
        .upsert_browser_tab(&f.host_a, input(&f.ws, "tab-1", "https://a.test/"))
        .await
        .unwrap();
    f.store.close_browser_tab("tab-1").await.unwrap();
    // Owning host's stale report: rejected, tombstone intact.
    let err = f
        .store
        .upsert_browser_tab(&f.host_a, input(&f.ws, "tab-1", "https://a.test/again"))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::InvalidParams(_)), "{err:?}");
    // Foreign host reusing the id: rejected, tombstone intact.
    let err = f
        .store
        .upsert_browser_tab(&f.host_b, input(&f.ws, "tab-1", "https://b.test/"))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::InvalidParams(_)), "{err:?}");
    let err = f
        .store
        .remove_browser_tab(&f.host_b, "tab-1")
        .await
        .unwrap_err();
    assert!(matches!(err, Error::InvalidParams(_)), "{err:?}");
    let stored = f.store.get_stored_tab("tab-1").await.unwrap().unwrap();
    assert!(stored.closed);
    assert_eq!(stored.tab.host_client_id, f.host_a);
    assert_eq!(stored.tab.url, "https://a.test/");
    assert!(f.store.list_browser_tabs(&f.ws).await.unwrap().is_empty());
    // The original host still gets its drop instruction.
    let result = f
        .store
        .sync_browser_tabs(&f.host_a, vec![input(&f.ws, "tab-1", "https://a.test/")])
        .await
        .unwrap();
    assert_eq!(result.drop, vec!["tab-1"]);
}

#[tokio::test]
async fn concurrent_first_reports_admit_exactly_one_host() {
    let f = fixture().await;
    // Park the single write connection so both upserts queue behind it and
    // are released together: without a transaction around read/check/write
    // both would read "absent" and the second would re-home the row.
    let writer = f.store.write_pool().acquire().await.unwrap();
    let race = async {
        tokio::join!(
            f.store
                .upsert_browser_tab(&f.host_a, input(&f.ws, "race", "https://a.test/")),
            f.store
                .upsert_browser_tab(&f.host_b, input(&f.ws, "race", "https://b.test/")),
        )
    };
    tokio::pin!(race);
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(200), &mut race)
            .await
            .is_err(),
        "both reports wait on the write connection"
    );
    drop(writer);
    let (a, b) = race.await;
    let winner = match (&a, &b) {
        (Ok(BrowserTabUpsertOutcome::Opened(t)), Err(Error::InvalidParams(_)))
        | (Err(Error::InvalidParams(_)), Ok(BrowserTabUpsertOutcome::Opened(t))) => {
            t.host_client_id.clone()
        }
        other => panic!("exactly one host wins: {other:?}"),
    };
    let stored = f.store.get_browser_tab("race").await.unwrap().unwrap();
    assert_eq!(stored.host_client_id, winner);
}

#[tokio::test]
async fn failed_sync_persists_nothing() {
    let f = fixture().await;
    f.store
        .upsert_browser_tab(&f.host_a, input(&f.ws, "tab-old", "https://a.test/"))
        .await
        .unwrap();
    let missing_ws = WorkspaceId("ws-missing".to_string());
    // The second entry violates the workspace FK; the first must not survive
    // the failure, and the absent-row sweep must not have run either.
    let err = f
        .store
        .sync_browser_tabs(
            &f.host_a,
            vec![
                input(&f.ws, "tab-partial", "https://a.test/new"),
                input(&missing_ws, "tab-bad", "https://a.test/bad"),
            ],
        )
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Internal(_)), "{err:?}");
    assert!(f
        .store
        .get_browser_tab("tab-partial")
        .await
        .unwrap()
        .is_none());
    assert!(
        f.store.get_browser_tab("tab-old").await.unwrap().is_some(),
        "the sweep of the failed sync was rolled back"
    );
    // A retry with a valid snapshot reports the new tab as opened (it was
    // never persisted) and the omitted one as closed.
    let result = f
        .store
        .sync_browser_tabs(
            &f.host_a,
            vec![input(&f.ws, "tab-partial", "https://a.test/new")],
        )
        .await
        .unwrap();
    assert_eq!(result.opened.len(), 1);
    assert_eq!(result.opened[0].tab_id, "tab-partial");
    assert_eq!(result.closed.len(), 1);
    assert_eq!(result.closed[0].tab_id, "tab-old");
}

#[tokio::test]
async fn list_by_workspace_is_one_ordered_partial_index_scan() {
    let f = fixture().await;
    let rows = sqlx::query(&format!(
        "EXPLAIN QUERY PLAN SELECT {} FROM browser_tab \
         WHERE workspace_id = ? AND closed_at IS NULL ORDER BY created_at, tab_id",
        super::COLUMNS
    ))
    .bind(&f.ws.0)
    .fetch_all(f.store.read_pool())
    .await
    .unwrap();
    let plan: Vec<String> = rows
        .iter()
        .map(|r| sqlx::Row::get::<String, _>(r, "detail"))
        .collect();
    assert_eq!(plan.len(), 1, "single step: {plan:?}");
    assert!(
        plan[0].contains("USING INDEX idx_browser_tab_workspace_open"),
        "{plan:?}"
    );
    assert!(
        !plan.iter().any(|d| d.contains("TEMP B-TREE")),
        "no temporary sort: {plan:?}"
    );
}

#[tokio::test]
async fn rows_cascade_with_workspace_delete() {
    let f = fixture().await;
    f.store
        .upsert_browser_tab(&f.host_a, input(&f.ws, "tab-1", "https://a.test/"))
        .await
        .unwrap();
    sqlx::query("DELETE FROM workspace WHERE id = ?")
        .bind(&f.ws.0)
        .execute(f.store.write_pool())
        .await
        .unwrap();
    assert!(f.store.get_browser_tab("tab-1").await.unwrap().is_none());
    assert!(f
        .store
        .list_browser_tabs_by_host(&f.host_a)
        .await
        .unwrap()
        .is_empty());
}
