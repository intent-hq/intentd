//! PR <-> workspace links (migration 0161, `workspace.findByPullRequest`):
//! the recording triggers on `workspace` / `workspace_git_root` and the
//! `list_pull_request_workspaces` lookup.

use std::time::Duration;

use intent_core::{
    now_iso, PullRequestInfo, PullRequestStatus, WorkspaceGitRoot, WorkspaceGitRootId,
    WorkspaceGitRootSource, WorkspaceId,
};

use super::{sample_workspace, TempDb};
use crate::Store;

const PR: &str = "https://github.com/intent-hq/intentd/pull/7";
/// The canonical key the triggers record for [`PR`].
const KEY: &str = "github.com/intent-hq/intentd/pull/7";

fn pr(url: &str) -> PullRequestInfo {
    PullRequestInfo {
        id: format!("pr-{url}"),
        number: 7,
        url: url.to_string(),
        title: "Add feature".to_string(),
        status: PullRequestStatus::Open,
        created_at: now_iso(),
        updated_at: now_iso(),
        base_ref: Some("main".to_string()),
        head_ref: Some("feature/foo".to_string()),
        head_sha: None,
        author: None,
        mergeable: None,
        mergeable_state: None,
        is_draft: None,
        is_in_merge_queue: None,
    }
}

fn root(ws: &WorkspaceId, prs: Option<Vec<PullRequestInfo>>) -> WorkspaceGitRoot {
    let ts = now_iso();
    WorkspaceGitRoot {
        id: WorkspaceGitRootId::new(),
        workspace_id: ws.clone(),
        path: format!("/tmp/root-{}", ws.0),
        source: WorkspaceGitRootSource::Auto,
        repo_owner: Some("intent-hq".to_string()),
        repo_name: Some("intentd".to_string()),
        registered_by_agent_ids: vec![],
        registered_commit_sha: None,
        pr_number: None,
        pr_url: None,
        pr_status: None,
        pull_requests: prs,
        created_at: ts.clone(),
        updated_at: ts,
    }
}

async fn linked(store: &Store, key: &str) -> Vec<(String, bool)> {
    store
        .list_pull_request_workspaces(key, false)
        .await
        .expect("lookup")
        .into_iter()
        .map(|row| (row.id.0, row.archived))
        .collect()
}

async fn link_rows(store: &Store) -> Vec<(String, String, String)> {
    sqlx::query_as(
        "SELECT pr_key, workspace_id, source FROM pull_request_workspace ORDER BY pr_key, workspace_id",
    )
    .fetch_all(store.read_pool())
    .await
    .expect("read links")
}

/// Distinct `linked_at` values: the triggers stamp millisecond precision.
async fn tick() {
    tokio::time::sleep(Duration::from_millis(15)).await;
}

#[tokio::test]
async fn workspace_pool_active_pr_and_pr_url_are_recorded_under_the_canonical_key() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let id = WorkspaceId("ws-a".into());
    let mut ws = sample_workspace(&id, "A", false);
    ws.pull_requests = Some(vec![pr("https://GitHub.com/Intent-HQ/IntentD/pull/7")]);
    ws.active_pull_request = Some(pr("https://github.com/intent-hq/intentd/pull/8"));
    ws.pr_url = Some("https://www.github.com/intent-hq/intentd/pull/9/".into());
    store.insert_workspace(&ws).await.expect("insert");

    let rows = link_rows(&store).await;
    let keys: Vec<&str> = rows.iter().map(|(k, _, _)| k.as_str()).collect();
    assert_eq!(
        keys,
        [
            "github.com/intent-hq/intentd/pull/7",
            "github.com/intent-hq/intentd/pull/8",
            "github.com/intent-hq/intentd/pull/9",
        ]
    );
    assert!(rows.iter().all(|(_, w, s)| w == "ws-a" && s == "workspace"));
    let key = intent_core::pull_request_key("https://github.com/INTENT-HQ/intentd/pull/7/files")
        .expect("pr key");
    assert_eq!(linked(&store, &key).await, [("ws-a".to_string(), false)]);
}

#[tokio::test]
async fn non_pull_request_urls_and_remote_workspaces_are_not_recorded() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let mut ws = sample_workspace(&WorkspaceId("ws-a".into()), "A", false);
    ws.pull_requests = Some(vec![
        pr("https://github.com/o/r/issues/7"),
        pr("https://example.com/pr/7"),
        pr("https://github.com/o/r/pull/7/files"),
        pr("https://github.com/pull/r/pull/x"),
    ]);
    store.insert_workspace(&ws).await.expect("insert");
    let mut remote = sample_workspace(&WorkspaceId("ws-remote".into()), "R", false);
    remote.is_remote = true;
    remote.pull_requests = Some(vec![pr(PR)]);
    store
        .insert_workspace(&remote)
        .await
        .expect("insert remote");

    assert!(link_rows(&store).await.is_empty());
}

#[tokio::test]
async fn several_workspaces_list_most_recently_linked_first_and_resightings_do_not_reorder() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let a = WorkspaceId("ws-a".into());
    let b = WorkspaceId("ws-b".into());
    let mut ws_a = sample_workspace(&a, "A", false);
    ws_a.pull_requests = Some(vec![pr(PR)]);
    store.insert_workspace(&ws_a).await.expect("insert a");
    tick().await;
    let mut ws_b = sample_workspace(&b, "B", false);
    store.insert_workspace(&ws_b).await.expect("insert b");
    ws_b.pull_requests = Some(vec![pr(PR)]);
    store.update_workspace(&ws_b).await.expect("link b");

    let order = || async {
        linked(&store, KEY)
            .await
            .into_iter()
            .map(|(id, _)| id)
            .collect::<Vec<_>>()
    };
    assert_eq!(order().await, ["ws-b", "ws-a"]);

    tick().await;
    let mut resighted = store.get_workspace(&a).await.expect("get a");
    resighted.pull_requests = Some(vec![PullRequestInfo {
        status: PullRequestStatus::Merged,
        ..pr(PR)
    }]);
    store.update_workspace(&resighted).await.expect("resight a");
    assert_eq!(order().await, ["ws-b", "ws-a"], "first link time is kept");
}

#[tokio::test]
async fn links_survive_the_pool_dropping_the_pr_and_archive() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let id = WorkspaceId("ws-a".into());
    let mut ws = sample_workspace(&id, "A", false);
    ws.pull_requests = Some(vec![pr(PR)]);
    store.insert_workspace(&ws).await.expect("insert");

    ws.pull_requests = Some(vec![]);
    store.update_workspace(&ws).await.expect("drop from pool");
    sqlx::query("UPDATE workspace SET archived = 1, status = 'Archived' WHERE id = ?")
        .bind(&id.0)
        .execute(store.write_pool())
        .await
        .expect("archive");

    assert_eq!(linked(&store, KEY).await, [("ws-a".to_string(), true)]);
}

#[tokio::test]
async fn unchanged_pr_columns_record_nothing_and_a_changed_pool_records_its_entries() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let id = WorkspaceId("ws-a".into());
    let mut ws = sample_workspace(&id, "A", false);
    ws.pull_requests = Some(vec![pr(PR)]);
    store.insert_workspace(&ws).await.expect("insert");
    // Stand in for a pool persisted before the migration (no backfill).
    sqlx::query("DELETE FROM pull_request_workspace")
        .execute(store.write_pool())
        .await
        .expect("clear links");

    ws.title = "Renamed".into();
    store
        .update_workspace(&ws)
        .await
        .expect("title-only update");
    assert!(link_rows(&store).await.is_empty(), "no PR column changed");

    ws.pull_requests = Some(vec![pr(PR), pr("https://github.com/o/r/pull/2")]);
    store.update_workspace(&ws).await.expect("pool change");
    assert_eq!(link_rows(&store).await.len(), 2);
}

#[tokio::test]
async fn git_root_pools_are_recorded_and_survive_unregistration() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let id = WorkspaceId("ws-a".into());
    store
        .insert_workspace(&sample_workspace(&id, "A", false))
        .await
        .expect("insert");
    let mut r = root(&id, None);
    store
        .upsert_workspace_git_root(&r)
        .await
        .expect("register root");
    assert!(link_rows(&store).await.is_empty());

    r.pull_requests = Some(vec![pr(PR)]);
    store
        .update_workspace_git_root_pr(&r)
        .await
        .expect("root pool");
    assert_eq!(
        link_rows(&store).await,
        [(KEY.to_string(), "ws-a".to_string(), "git_root".to_string())]
    );

    store
        .delete_workspace_git_root(&r.id)
        .await
        .expect("unregister");
    assert_eq!(linked(&store, KEY).await, [("ws-a".to_string(), false)]);
}

#[tokio::test]
async fn deleting_a_workspace_removes_only_its_links() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    for id in ["ws-a", "ws-b"] {
        let mut ws = sample_workspace(&WorkspaceId(id.into()), id, false);
        ws.pull_requests = Some(vec![pr(PR)]);
        store.insert_workspace(&ws).await.expect("insert");
        tick().await;
    }
    store
        .delete_workspace(&WorkspaceId("ws-b".into()))
        .await
        .expect("delete b");

    assert_eq!(linked(&store, KEY).await, [("ws-a".to_string(), false)]);
    assert_eq!(link_rows(&store).await.len(), 1);
}

/// The SQL normalizer (trigger `pull_request_workspace_candidate_record`) and
/// `intent_core::pull_request_key` must agree: every key the trigger records is
/// exactly the key the lookup computes for the same URL, so a stored link can
/// always be found. URLs the trigger skips are either rejected by the lookup
/// too, or are click-only decorations (`/files`, query, fragment) that stored
/// forge URLs never carry.
#[tokio::test]
async fn trigger_and_lookup_normalizers_agree() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let cases: &[(&str, bool)] = &[
        ("https://github.com/o/r/pull/7", true),
        ("http://github.com/o/r/pull/7", true),
        ("HTTPS://GitHub.COM/Org/Repo/pull/12", true),
        ("https://www.github.com/o/r/pull/7", true),
        ("https://github.com/o/r/pull/7/", true),
        ("https://github.com/o/r/pull/7//", true),
        ("  https://github.com/o/r/pull/7  ", true),
        ("https://ghe.example.com:8443/o/r/pull/3", true),
        ("https://github.com/pull/r/pull/9", true),
        ("https://github.com/o/r2/pull/99999999999999999999999", true),
        ("https://github.com/o/r/pull/007", false),
        ("https://github.com/o/r/pull/0", false),
        ("https://github.com/o/r/pull/", false),
        ("https://github.com/o/r/pull/1a", false),
        ("https://github.com//r/pull/1", false),
        ("https:///o/r/pull/1", false),
        ("https://www./o/r/pull/1", false),
        ("https://user@github.com/o/r/pull/1", false),
        ("https://github.com/o/r/issues/1", false),
        ("https://github.com/o/pull/x/1", false),
        ("ftp://github.com/o/r/pull/1", false),
        ("github.com/o/r/pull/1", false),
        ("https://gitlab.com/g/p/-/merge_requests/1", false),
    ];
    let click_only = [
        "https://github.com/o/r/pull/7/files",
        "https://github.com/o/r/pull/7?w=1",
        "https://github.com/o/r/pull/7#discussion_r1",
    ];
    for (i, (url, recorded)) in cases
        .iter()
        .copied()
        .chain(click_only.iter().map(|u| (*u, false)))
        .enumerate()
    {
        let id = WorkspaceId(format!("ws-{i}"));
        let mut ws = sample_workspace(&id, "W", false);
        ws.pr_url = Some(url.to_string());
        store.insert_workspace(&ws).await.expect("insert");
        let stored: Vec<String> =
            sqlx::query_scalar("SELECT pr_key FROM pull_request_workspace WHERE workspace_id = ?")
                .bind(&id.0)
                .fetch_all(store.read_pool())
                .await
                .expect("read key");
        let computed = intent_core::pull_request_key(url);
        if recorded {
            assert_eq!(stored.len(), 1, "{url}: trigger should record it");
            assert_eq!(computed.as_deref(), Some(stored[0].as_str()), "{url}");
        } else {
            assert!(
                stored.is_empty(),
                "{url}: trigger should skip it, got {stored:?}"
            );
            assert_eq!(
                computed.is_some(),
                click_only.contains(&url),
                "{url}: only click decorations may be lookup-only"
            );
        }
    }
    let funnel: i64 = sqlx::query_scalar("SELECT count(*) FROM pull_request_workspace_candidate")
        .fetch_one(store.read_pool())
        .await
        .expect("count funnel");
    assert_eq!(funnel, 0, "the candidate funnel never stores rows");
}

/// A workspace transfer keeps every link with its original `linked_at`,
/// including links for PRs that already left the pool and links that came
/// from git roots (which do not transfer), even though the destination's
/// workspace insert trigger re-links the current snapshot first.
#[tokio::test]
async fn transfer_round_trip_keeps_pruned_and_git_root_links_with_their_times() {
    let source_db = TempDb::new();
    let source = Store::open(&source_db.path).await.expect("open source");
    let id = WorkspaceId("ws-a".into());
    let mut ws = sample_workspace(&id, "A", false);
    ws.pull_requests = Some(vec![pr(PR)]);
    source.insert_workspace(&ws).await.expect("insert");
    let mut r = root(
        &id,
        Some(vec![pr(
            "https://github.com/intent-hq/cloudlands-fe/pull/3",
        )]),
    );
    source.upsert_workspace_git_root(&r).await.expect("root");
    r.pull_requests = Some(vec![pr(
        "https://github.com/intent-hq/cloudlands-fe/pull/3",
    )]);
    tick().await;
    ws.pull_requests = Some(vec![pr("https://github.com/intent-hq/intentd/pull/8")]);
    source
        .update_workspace(&ws)
        .await
        .expect("prune #7, link #8");
    let before: Vec<(String, String)> =
        sqlx::query_as("SELECT pr_key, linked_at FROM pull_request_workspace ORDER BY pr_key")
            .fetch_all(source.read_pool())
            .await
            .expect("read source links");
    assert_eq!(before.len(), 3);

    let rows = source.transfer_export_rows(&id).await.expect("export");
    tick().await;
    let dest_db = TempDb::new();
    let dest = Store::open(&dest_db.path).await.expect("open dest");
    dest.transfer_import_rows(&rows).await.expect("import");

    let after: Vec<(String, String)> =
        sqlx::query_as("SELECT pr_key, linked_at FROM pull_request_workspace ORDER BY pr_key")
            .fetch_all(dest.read_pool())
            .await
            .expect("read dest links");
    assert_eq!(after, before);
    assert_eq!(linked(&dest, KEY).await, [("ws-a".to_string(), false)]);
}
