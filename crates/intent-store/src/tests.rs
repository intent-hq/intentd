//! Unit tests: open a temp `SQLite` DB, run migrations, and round-trip
//! workspaces and notes including the `include_archived` filter.

use std::path::PathBuf;

use intent_core::{
    events, now_iso, ActorType, AgentId, AgentSession, AgentStatus, AuthorType, ClientId, Comment,
    CommentAnchor, CommentAnchorType, CommentStatus, CommentType, ContentType, Error, EventActor,
    Hook, HookId, HookState, Note, NoteId, NoteMetadata, NoteVersionAuthor, NoteVisibility,
    TaskMetadata, TaskStatus, Workspace, WorkspaceActivity, WorkspaceAttention, WorkspaceId,
    WorkspaceStatus,
};
use serde_json::json;
use sqlx::Row;

use crate::{AgentQueueRow, AutoVacuumActivation, EventQuery, NewEvent, Store, MAX_NOTE_VERSIONS};

/// A unique temp DB path that cleans up its `.db`/`-wal`/`-shm` files on drop.
struct TempDb {
    path: PathBuf,
}

impl TempDb {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("intentd-test-{}.db", uuid::Uuid::new_v4()));
        Self { path }
    }
}

impl Drop for TempDb {
    fn drop(&mut self) {
        for suffix in ["", "-wal", "-shm"] {
            let p = PathBuf::from(format!("{}{suffix}", self.path.display()));
            let _ = std::fs::remove_file(p);
        }
    }
}

fn sample_workspace(id: &WorkspaceId, title: &str, archived: bool) -> Workspace {
    let ts = now_iso();
    Workspace {
        id: id.clone(),
        title: title.to_string(),
        branch: "feature/test".to_string(),
        base_ref: Some("main".to_string()),
        base_commit_sha: None,
        status: if archived {
            WorkspaceStatus::Archived
        } else {
            WorkspaceStatus::Active
        },
        status_message: Some("working".to_string()),
        status_image_asset_id: None,
        activity: WorkspaceActivity::Idle,
        attention: WorkspaceAttention::Unread,
        created_at: ts.clone(),
        updated_at: ts.clone(),
        last_activity: Some(ts),
        tags: vec!["alpha".to_string(), "beta".to_string()],
        path: Some("/tmp/ws-meta".to_string()),
        repository_path: Some("/tmp/repo".to_string()),
        repository_owner: Some("intent-hq".to_string()),
        repository_name: Some("intentd".to_string()),
        worktree_path: None,
        scope: None,
        skip_worktree: false,
        setup_script: None,
        is_remote: false,
        default_model: Some("opus".to_string()),
        pr_number: Some(42),
        pr_url: None,
        pr_status: None,
        active_pull_request: None,
        pull_requests: None,
        archived,
        archived_at: if archived { Some(now_iso()) } else { None },
        task_stats: None,
        agent_summary: None,
        diff_summary: None,
        token_usage: None,
        cow_supported: None,
        display_status: None,
        waiting: false,
        checkout_mode: None,
        execution_environment: None,
        disk_usage: None,
        pending_delete_at: None,
    }
}

#[tokio::test]
async fn migration_status_reports_current_after_open() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let status = store.migration_status().await.expect("migration status");
    assert!(status.is_current(), "fresh open must apply all migrations");
    assert_eq!(
        status.expected,
        vec![
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
            25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46,
            47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68,
            69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90,
            91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105
        ]
    );
    assert_eq!(
        status.applied,
        vec![
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
            25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46,
            47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68,
            69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90,
            91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105
        ]
    );
}

/// `Store::open` refuses to run against a database whose `_sqlx_migrations`
/// table records a version not embedded in this build (i.e. the DB was
/// created by a newer intentd): it must surface a clear "downgrades are
/// unsupported" error naming the offending version, not the raw sqlx
/// `VersionMissing` message or a generic migration failure.
#[tokio::test]
async fn open_rejects_database_from_newer_build() {
    let tmp = TempDb::new();
    {
        let store = Store::open(&tmp.path).await.expect("initial open");
        sqlx::query(
            "INSERT INTO _sqlx_migrations \
             (version, description, installed_on, success, checksum, execution_time) \
             VALUES (999999, 'from the future', CURRENT_TIMESTAMP, 1, x'00', 0)",
        )
        .execute(store.write_pool())
        .await
        .expect("seed future migration row");
    }

    let result = Store::open(&tmp.path).await;
    let Err(err) = result else {
        panic!("reopen must refuse a schema from a newer build")
    };
    let msg = err.to_string();
    assert!(msg.contains("downgrades are unsupported"), "got {msg:?}");
    assert!(msg.contains("999999"), "got {msg:?}");
    assert!(
        msg.contains("upgrade intentd to the version that created this database"),
        "got {msg:?}"
    );
}

/// The 0031 backfill derives `repository_name` from the `repository_path`
/// basename for rows missing a name, leaves explicit names untouched, and
/// skips rows without a path. Exercised by re-running the migration SQL
/// against rows shaped like the pre-0031 legacy state (the embedded migrator
/// has already run against an empty DB by the time we can insert rows).
#[tokio::test]
async fn backfill_repository_name_from_path_basename() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");

    let legacy_id = WorkspaceId::new();
    let named_id = WorkspaceId::new();
    let pathless_id = WorkspaceId::new();
    let windows_id = WorkspaceId::new();
    let mut legacy = sample_workspace(&legacy_id, "legacy", false);
    legacy.repository_path = Some("/Users/me/src/describe-workspace".to_string());
    legacy.repository_name = None;
    let named = sample_workspace(&named_id, "named", false);
    let mut pathless = sample_workspace(&pathless_id, "pathless", false);
    pathless.repository_path = None;
    pathless.repository_name = None;
    let mut windows = sample_workspace(&windows_id, "windows", false);
    windows.repository_path = Some(r"C:\Users\me\src\describe-workspace".to_string());
    windows.repository_name = None;
    for ws in [&legacy, &named, &pathless, &windows] {
        store.insert_workspace(ws).await.expect("insert");
    }

    sqlx::raw_sql(include_str!(
        "../migrations/0031_workspace_repository_name_backfill.sql"
    ))
    .execute(store.write_pool())
    .await
    .expect("re-run backfill");

    let get_name = |id: WorkspaceId| {
        let store = store.clone();
        async move { store.get_workspace(&id).await.expect("get").repository_name }
    };
    assert_eq!(
        get_name(legacy_id).await.as_deref(),
        Some("describe-workspace"),
        "NULL name with a path backfills to the basename"
    );
    assert_eq!(
        get_name(named_id).await.as_deref(),
        Some("intentd"),
        "explicit name is never overwritten"
    );
    assert_eq!(
        get_name(pathless_id).await,
        None,
        "no repository_path stays NULL"
    );
    assert_eq!(
        get_name(windows_id).await.as_deref(),
        Some("describe-workspace"),
        "Windows-style `\\` separators backfill to the basename too"
    );
}

/// Migration 0034 heals legacy rows where `intent-services::create_workspace`
/// seeded `title = id` (slug-shaped placeholder from before the Untitled-parity
/// fix): it clears those to `""` so the FE renders "Untitled". User-typed
/// titles that differ from the id are preserved verbatim, and the
/// Chief-of-Staff row (`id = '__chief__'`) is exempted. The rare collision
/// case — a user title that happens to equal the workspace id — is
/// indistinguishable from a slug seed at rest and is also cleared; this is an
/// accepted trade-off documented on the collision row below.
#[tokio::test]
async fn heal_slug_seeded_titles_clears_only_matching_rows() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");

    // Legacy slug-seeded row: `title == id` — should be cleared.
    let slug_id = WorkspaceId::from("amber-fox");
    let mut slug = sample_workspace(&slug_id, "amber-fox", false);
    slug.repository_name = Some("intentd".to_string());
    // Row with a real user title — must be preserved verbatim.
    let user_id = WorkspaceId::new();
    let user = sample_workspace(&user_id, "Add dark mode support", false);
    // Row with a title that already equals `""` — must stay `""`.
    let empty_id = WorkspaceId::new();
    let empty = sample_workspace(&empty_id, "", false);
    // Coincidental collision: user-typed title happens to match the id shape.
    // The heal cannot distinguish this from a slug seed, and the task note is
    // explicit that clearing it is the accepted trade-off (slug-seeded rows
    // are the reference case). Documented here so the assertion is intentional.
    let collision_id = WorkspaceId::from("blue-heron");
    let collision = sample_workspace(&collision_id, "blue-heron", false);

    for ws in [&slug, &user, &empty, &collision] {
        store.insert_workspace(ws).await.expect("insert");
    }

    sqlx::raw_sql(include_str!(
        "../migrations/0034_workspace_title_untitled_heal.sql"
    ))
    .execute(store.write_pool())
    .await
    .expect("re-run heal");

    assert_eq!(
        store.get_workspace(&slug_id).await.unwrap().title,
        "",
        "slug-seeded title cleared"
    );
    assert_eq!(
        store.get_workspace(&user_id).await.unwrap().title,
        "Add dark mode support",
        "user-set title preserved"
    );
    assert_eq!(
        store.get_workspace(&empty_id).await.unwrap().title,
        "",
        "already-empty title stays empty"
    );
    assert_eq!(
        store.get_workspace(&collision_id).await.unwrap().title,
        "",
        "collision case documented: title=id rows are cleared"
    );

    // Chief-of-Staff (seeded by migration 0033) keeps its canonical title,
    // guaranteed by the `id != '__chief__'` clause in the heal SQL.
    let chief = store
        .get_workspace(&WorkspaceId::from("__chief__"))
        .await
        .expect("chief row present");
    assert_eq!(chief.title, "Chief of Staff");
}

#[tokio::test]
async fn workspace_round_trip_and_archive_filter() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");

    let active_id = WorkspaceId::new();
    let archived_id = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&active_id, "Active WS", false))
        .await
        .expect("insert active");
    store
        .insert_workspace(&sample_workspace(&archived_id, "Archived WS", true))
        .await
        .expect("insert archived");

    let all = store.list_workspaces(true).await.expect("list all");
    assert_eq!(all.len(), 2);

    let visible = store.list_workspaces(false).await.expect("list visible");
    assert_eq!(visible.len(), 1);
    assert_eq!(visible[0].id, active_id);

    let got = &visible[0];
    assert_eq!(got.title, "Active WS");
    assert_eq!(got.branch, "feature/test");
    assert_eq!(got.tags, vec!["alpha".to_string(), "beta".to_string()]);
    assert_eq!(got.status, WorkspaceStatus::Active);
    assert_eq!(got.attention, WorkspaceAttention::Unread);
    assert_eq!(got.pr_number, Some(42));
    assert_eq!(got.repository_name, Some("intentd".to_string()));
    // `path` and `repository_path` survive insert → list (§9.1 TS parity).
    assert_eq!(got.path, Some("/tmp/ws-meta".to_string()));
    assert_eq!(got.repository_path, Some("/tmp/repo".to_string()));
    assert!(!got.archived);
}

/// `bump_workspace_last_activity` (monorepo#1580) is scoped and monotonic:
/// it writes only `last_activity` (never `updated_at` or any other column),
/// declines a stale or equal timestamp, fills a NULL column, overwrites a
/// malformed stored value, tolerates differing fractional-second precision,
/// and reports `NotFound` for a missing workspace.
#[tokio::test]
async fn workspace_last_activity_bump_is_scoped_and_monotonic() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");

    let id = WorkspaceId::new();
    let mut seed = sample_workspace(&id, "Bump WS", false);
    seed.last_activity = None;
    seed.updated_at = "2020-01-01T00:00:00Z".to_string();
    store.insert_workspace(&seed).await.expect("insert");

    // NULL column: first bump writes.
    assert!(store
        .bump_workspace_last_activity(&id, "2026-01-01T00:00:00Z")
        .await
        .expect("bump from null"));
    let got = store.get_workspace(&id).await.expect("get");
    assert_eq!(got.last_activity.as_deref(), Some("2026-01-01T00:00:00Z"));
    // Scoped: no other column moved.
    assert_eq!(got.updated_at, "2020-01-01T00:00:00Z");
    assert_eq!(got.title, "Bump WS");

    // Newer wins even with coarser fractional-second precision on the stored
    // side (julianday comparison, not lexicographic).
    assert!(store
        .bump_workspace_last_activity(&id, "2026-01-01T00:00:00.500Z")
        .await
        .expect("bump newer"));
    assert_eq!(
        store
            .get_workspace(&id)
            .await
            .expect("get")
            .last_activity
            .as_deref(),
        Some("2026-01-01T00:00:00.500Z")
    );

    // Older declines, leaving the stored value intact.
    assert!(!store
        .bump_workspace_last_activity(&id, "2025-06-01T00:00:00Z")
        .await
        .expect("bump older"));
    // Equal declines too.
    assert!(!store
        .bump_workspace_last_activity(&id, "2026-01-01T00:00:00.500Z")
        .await
        .expect("bump equal"));
    // Malformed input never writes.
    assert!(!store
        .bump_workspace_last_activity(&id, "not-a-timestamp")
        .await
        .expect("bump malformed"));
    assert_eq!(
        store
            .get_workspace(&id)
            .await
            .expect("get")
            .last_activity
            .as_deref(),
        Some("2026-01-01T00:00:00.500Z")
    );
    assert_eq!(
        store.get_workspace(&id).await.expect("get").updated_at,
        "2020-01-01T00:00:00Z"
    );

    // A malformed STORED value parses to NULL and is treated as older, so the
    // bump repairs a corrupted column.
    let corrupt_id = WorkspaceId::new();
    let mut corrupt = sample_workspace(&corrupt_id, "Corrupt WS", false);
    corrupt.last_activity = Some("not-a-timestamp".to_string());
    store.insert_workspace(&corrupt).await.expect("insert");
    assert!(store
        .bump_workspace_last_activity(&corrupt_id, "2026-01-01T00:00:00Z")
        .await
        .expect("bump over malformed stored value"));
    assert_eq!(
        store
            .get_workspace(&corrupt_id)
            .await
            .expect("get")
            .last_activity
            .as_deref(),
        Some("2026-01-01T00:00:00Z")
    );

    // Missing workspace → NotFound.
    assert!(matches!(
        store
            .bump_workspace_last_activity(&WorkspaceId::from("nope"), "2026-01-01T00:00:00Z")
            .await,
        Err(intent_core::Error::NotFound(_))
    ));
}

/// Regression (monorepo#1585): `update_workspace` — the full-row replace —
/// routes `last_activity` through the same monotonic guard as
/// `bump_workspace_last_activity`, so a get → mutate → write flow whose read
/// predated a concurrent bump can no longer silently revert (or clear) the
/// column. A newer candidate still advances it through the same write.
#[tokio::test]
async fn workspace_full_row_update_cannot_regress_last_activity() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");

    let id = WorkspaceId::new();
    let mut seed = sample_workspace(&id, "Clobber WS", false);
    seed.last_activity = None;
    store.insert_workspace(&seed).await.expect("insert");

    // Stale snapshot read BEFORE a concurrent bump lands (the monorepo#1585
    // interleaving: debounce bump vs. get → mutate → update_workspace).
    let stale = store.get_workspace(&id).await.expect("stale read");
    assert!(store
        .bump_workspace_last_activity(&id, "2026-01-01T00:00:00Z")
        .await
        .expect("bump"));

    // Full-row write off the stale snapshot (`last_activity: None`): every
    // other column replaces, the bumped column holds.
    let mut renamed = stale.clone();
    renamed.title = "Renamed".to_string();
    store
        .update_workspace(&renamed)
        .await
        .expect("update stale");
    let got = store.get_workspace(&id).await.expect("get");
    assert_eq!(got.title, "Renamed", "other columns still full-row replace");
    assert_eq!(
        got.last_activity.as_deref(),
        Some("2026-01-01T00:00:00Z"),
        "a None candidate must not clear the bumped column"
    );

    // An older candidate is declined, leaving the stored value intact.
    let mut older = got.clone();
    older.last_activity = Some("2020-01-01T00:00:00Z".to_string());
    store.update_workspace(&older).await.expect("update older");
    assert_eq!(
        store
            .get_workspace(&id)
            .await
            .expect("get")
            .last_activity
            .as_deref(),
        Some("2026-01-01T00:00:00Z"),
        "a stale candidate must not walk last_activity backwards"
    );

    // A malformed candidate never writes.
    let mut malformed = got.clone();
    malformed.last_activity = Some("not-a-timestamp".to_string());
    store
        .update_workspace(&malformed)
        .await
        .expect("update malformed");
    assert_eq!(
        store
            .get_workspace(&id)
            .await
            .expect("get")
            .last_activity
            .as_deref(),
        Some("2026-01-01T00:00:00Z")
    );

    // A newer candidate advances the column through the same write.
    let mut newer = got.clone();
    newer.last_activity = Some("2027-01-01T00:00:00Z".to_string());
    store.update_workspace(&newer).await.expect("update newer");
    assert_eq!(
        store
            .get_workspace(&id)
            .await
            .expect("get")
            .last_activity
            .as_deref(),
        Some("2027-01-01T00:00:00Z")
    );
}

#[tokio::test]
async fn workspace_get_update_delete() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");

    let id = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&id, "Original", false))
        .await
        .expect("insert");

    // get
    let got = store.get_workspace(&id).await.expect("get");
    assert_eq!(got.title, "Original");

    // missing get → NotFound
    let missing = store.get_workspace(&WorkspaceId::from("nope")).await;
    assert!(matches!(missing, Err(intent_core::Error::NotFound(_))));

    // `path`/`repository_path` round-trip through insert → get.
    assert_eq!(got.path, Some("/tmp/ws-meta".to_string()));
    assert_eq!(got.repository_path, Some("/tmp/repo".to_string()));

    // update (full row replace)
    let mut updated = got.clone();
    updated.title = "Renamed".to_string();
    updated.attention = WorkspaceAttention::None;
    updated.tags = vec!["x".to_string()];
    updated.path = Some("/tmp/ws-meta2".to_string());
    updated.repository_path = Some("/tmp/repo2".to_string());
    store.update_workspace(&updated).await.expect("update");
    let reread = store.get_workspace(&id).await.expect("reget");
    assert_eq!(reread.title, "Renamed");
    assert_eq!(reread.attention, WorkspaceAttention::None);
    assert_eq!(reread.tags, vec!["x".to_string()]);
    assert_eq!(reread.path, Some("/tmp/ws-meta2".to_string()));
    assert_eq!(reread.repository_path, Some("/tmp/repo2".to_string()));

    // update missing → NotFound
    let mut ghost = updated.clone();
    ghost.id = WorkspaceId::from("nope");
    assert!(matches!(
        store.update_workspace(&ghost).await,
        Err(intent_core::Error::NotFound(_))
    ));

    // delete
    store.delete_workspace(&id).await.expect("delete");
    assert!(matches!(
        store.get_workspace(&id).await,
        Err(intent_core::Error::NotFound(_))
    ));
    // delete missing → NotFound
    assert!(matches!(
        store.delete_workspace(&id).await,
        Err(intent_core::Error::NotFound(_))
    ));
}

/// `pull_requests` (the persisted `Vec<PullRequestInfo>` alongside the
/// `active_pull_request` scalar) round-trips through insert → get → update →
/// get, including a clear back to `None` (§7.6). Migration 0035 adds the
/// column; the store maps it as a JSON TEXT payload.
#[tokio::test]
async fn workspace_pull_requests_round_trip_and_clear() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");

    let id = WorkspaceId::new();
    let mut ws = sample_workspace(&id, "PR list", false);
    let pr = intent_core::PullRequestInfo {
        id: "pr-1".to_string(),
        number: 7,
        url: "https://example.com/pr/7".to_string(),
        title: "Add feature".to_string(),
        status: intent_core::PullRequestStatus::Open,
        created_at: now_iso(),
        updated_at: now_iso(),
        base_ref: Some("main".to_string()),
        head_ref: Some("feature/foo".to_string()),
        head_sha: None,
        author: None,
        mergeable: None,
        mergeable_state: None,
        is_draft: None,
    };
    ws.pull_requests = Some(vec![pr.clone()]);
    store.insert_workspace(&ws).await.expect("insert");

    let got = store.get_workspace(&id).await.expect("get");
    assert_eq!(got.pull_requests.as_deref(), Some(&[pr][..]));

    // Clear via update: `pull_requests = None` drops the column.
    let mut cleared = got.clone();
    cleared.pull_requests = None;
    store.update_workspace(&cleared).await.expect("update");
    let reread = store.get_workspace(&id).await.expect("re-get");
    assert!(reread.pull_requests.is_none());
}

/// `update_workspace_pr_linkage` writes ONLY the PR columns + `updated_at`:
/// a stale snapshot carrying old values for other columns (title, archived)
/// must never clobber a concurrent mutation of those columns.
#[tokio::test]
async fn workspace_pr_linkage_update_is_scoped() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");

    let id = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&id, "Before", false))
        .await
        .expect("insert");
    // Stale snapshot taken before a concurrent full-row mutation.
    let mut stale = store.get_workspace(&id).await.expect("snapshot");

    let mut concurrent = store.get_workspace(&id).await.expect("get");
    concurrent.title = "Renamed meanwhile".to_string();
    concurrent.archived = true;
    concurrent.archived_at = Some(now_iso());
    concurrent.status = WorkspaceStatus::Archived;
    store
        .update_workspace(&concurrent)
        .await
        .expect("concurrent mutation");

    stale.pr_number = Some(99);
    stale.pr_url = Some("https://example.com/pr/99".to_string());
    stale.pr_status = Some(intent_core::PullRequestStatus::Merged);
    stale.updated_at = now_iso();
    store
        .update_workspace_pr_linkage(&stale)
        .await
        .expect("scoped pr write");

    let after = store.get_workspace(&id).await.expect("re-get");
    assert_eq!(after.pr_number, Some(99), "pr columns written");
    assert_eq!(
        after.pr_status,
        Some(intent_core::PullRequestStatus::Merged)
    );
    assert_eq!(
        after.title, "Renamed meanwhile",
        "stale snapshot must not clobber a concurrent title edit"
    );
    assert!(after.archived, "stale snapshot must not resurrect archived");

    let missing = store
        .update_workspace_pr_linkage(&sample_workspace(&WorkspaceId::new(), "?", false))
        .await;
    assert!(matches!(missing, Err(crate::Error::NotFound(_))));
}

/// `delete_workspace` records a tombstone so `workspace_id_ever_used` keeps
/// reporting the id as used after the row is gone — `workspace.create` relies
/// on this to never recycle a deleted workspace id.
#[tokio::test]
async fn delete_workspace_tombstones_id() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");

    let id = WorkspaceId::from("auth-fix");
    assert!(
        !store.workspace_id_ever_used(&id).await.expect("unused"),
        "never-created id reports unused"
    );

    store
        .insert_workspace(&sample_workspace(&id, "WS", false))
        .await
        .expect("insert");
    assert!(
        store.workspace_id_ever_used(&id).await.expect("live"),
        "live row reports used"
    );

    store.delete_workspace(&id).await.expect("delete");
    assert!(matches!(
        store.get_workspace(&id).await,
        Err(intent_core::Error::NotFound(_))
    ));
    assert!(
        store.workspace_id_ever_used(&id).await.expect("tombstone"),
        "deleted id stays used via the tombstone"
    );

    // A NotFound delete records no tombstone for a never-created id.
    let ghost = WorkspaceId::from("ghost-id");
    assert!(matches!(
        store.delete_workspace(&ghost).await,
        Err(intent_core::Error::NotFound(_))
    ));
    assert!(
        !store.workspace_id_ever_used(&ghost).await.expect("ghost"),
        "failed delete of an unknown id leaves it unused"
    );
}

#[tokio::test]
async fn note_round_trip() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");

    let ws_id = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws_id, "WS", false))
        .await
        .expect("insert ws");

    let ts = now_iso();
    let note = Note {
        id: NoteId::new(),
        workspace_id: ws_id.clone(),
        title: "Spec".to_string(),
        content: "# Hello".to_string(),
        content_type: ContentType::Markdown,
        tags: vec!["spec".to_string()],
        is_pinned: true,
        is_archived: false,
        is_default: true,
        parent_id: None,
        visibility: NoteVisibility::Workspace,
        metadata: NoteMetadata {
            task: Some(TaskMetadata {
                status: TaskStatus::InProgress,
                ..Default::default()
            }),
        },
        created_at: ts.clone(),
        rev: 0,
        updated_at: ts,
    };
    store.insert_note(&note).await.expect("insert note");

    let notes = store.list_notes(&ws_id).await.expect("list notes");
    assert_eq!(notes.len(), 1);
    let got = &notes[0];
    assert_eq!(got.id, note.id);
    assert_eq!(got.title, "Spec");
    assert_eq!(got.content, "# Hello");
    assert_eq!(got.content_type, ContentType::Markdown);
    assert_eq!(got.tags, vec!["spec".to_string()]);
    assert!(got.is_pinned);
    assert!(got.is_default);
    assert_eq!(
        got.metadata.task.as_ref().map(|t| t.status),
        Some(TaskStatus::InProgress)
    );

    let fetched = store.get_note(&ws_id, &note.id).await.expect("get note");
    assert_eq!(fetched.id, note.id);
}

/// `max_note_updated_at` (monorepo#3058): the newest note `updated_at` per
/// workspace as a single aggregate — `None` for a workspace with no notes,
/// the max across notes otherwise, matching what folding hydrated
/// `list_notes` rows would produce. Backs the `lastActivity` derivation on
/// the hot list/get emit paths without note-body hydration.
#[tokio::test]
async fn max_note_updated_at_matches_list_notes_fold() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");

    let ws_id = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws_id, "WS", false))
        .await
        .expect("insert ws");

    // No notes: the aggregate reads None (SQL MAX over zero rows is NULL).
    assert_eq!(
        store.max_note_updated_at(&ws_id).await.expect("empty max"),
        None
    );

    let mk_note = |id: &str, updated_at: &str| Note {
        id: NoteId::from(id),
        workspace_id: ws_id.clone(),
        title: id.to_string(),
        content: "body".to_string(),
        content_type: ContentType::Markdown,
        tags: vec![],
        is_pinned: false,
        is_archived: false,
        is_default: false,
        parent_id: None,
        visibility: NoteVisibility::Workspace,
        metadata: NoteMetadata::default(),
        created_at: updated_at.to_string(),
        rev: 0,
        updated_at: updated_at.to_string(),
    };
    for (id, ts) in [
        ("n-old", "2026-01-01T00:00:00Z"),
        ("n-new", "2026-03-01T00:00:00Z"),
        ("n-mid", "2026-02-01T00:00:00Z"),
    ] {
        store.insert_note(&mk_note(id, ts)).await.expect("insert");
    }

    let max = store.max_note_updated_at(&ws_id).await.expect("max");
    assert_eq!(max.as_deref(), Some("2026-03-01T00:00:00Z"));

    // Parity with the old fold over hydrated rows.
    let notes = store.list_notes(&ws_id).await.expect("list");
    let folded = notes.iter().map(|n| n.updated_at.as_str()).max();
    assert_eq!(max.as_deref(), folded);

    // Scoped per workspace: another workspace's notes never leak in.
    let other = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&other, "Other", false))
        .await
        .expect("insert other ws");
    assert_eq!(
        store.max_note_updated_at(&other).await.expect("other max"),
        None
    );

    // The aggregate must be answered from the covering
    // idx_note_workspace_updated_at index — never by visiting note rows
    // (whose bodies can be large), or the O(notes) read this replaces
    // silently returns (monorepo#3058).
    let details: Vec<String> =
        sqlx::query("EXPLAIN QUERY PLAN SELECT MAX(updated_at) FROM note WHERE workspace_id = ?")
            .bind(&ws_id.0)
            .fetch_all(store.read_pool())
            .await
            .expect("explain query plan")
            .iter()
            .map(|row| row.get::<String, _>("detail"))
            .collect();
    assert!(
        details
            .iter()
            .any(|d| d.contains("COVERING INDEX idx_note_workspace_updated_at")),
        "aggregate must use the covering index, plan: {details:?}"
    );
}

#[tokio::test]
async fn note_version_append_list_get_and_prune() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");

    let ws_id = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws_id, "WS", false))
        .await
        .expect("insert ws");

    let ts = now_iso();
    let mut note = Note {
        id: NoteId::new(),
        workspace_id: ws_id.clone(),
        title: "Versioned".to_string(),
        content: String::new(),
        content_type: ContentType::Markdown,
        tags: vec![],
        is_pinned: false,
        is_archived: false,
        is_default: false,
        parent_id: None,
        visibility: NoteVisibility::Workspace,
        metadata: NoteMetadata::default(),
        created_at: ts.clone(),
        rev: 0,
        updated_at: ts.clone(),
    };
    store.insert_note(&note).await.expect("insert note");

    let author = NoteVersionAuthor {
        id: "system".to_string(),
        name: "intentd".to_string(),
        author_type: "system".to_string(),
    };
    // Append MAX + 5 versions; only the newest MAX survive the prune.
    let total = MAX_NOTE_VERSIONS + 5;
    for i in 1..=total {
        note.content = format!("content v{i}");
        let v = store
            .append_note_version(&note, &author, &ts)
            .await
            .expect("append version");
        assert_eq!(v, i, "version numbers are strictly increasing");
    }

    let versions = store
        .list_note_versions(&ws_id, &note.id)
        .await
        .expect("list versions");
    assert_eq!(
        versions.len(),
        usize::try_from(MAX_NOTE_VERSIONS).expect("value fits in usize")
    );
    assert_eq!(versions.first().map(|e| e.v), Some(6), "oldest 5 pruned");
    assert_eq!(versions.last().map(|e| e.v), Some(total));
    assert!(versions.iter().all(|e| e.entry_type == "snapshot"));
    assert_eq!(
        versions.last().map(|e| e.content_length),
        Some(i64::try_from(note.content.len()).expect("value fits in i64"))
    );

    let got = store
        .get_note_version(&ws_id, &note.id, 6)
        .await
        .expect("get version 6");
    assert_eq!(got.content, "content v6");
    assert_eq!(got.author.author_type, "system");
    // Pruned and never-existing versions are NotFound.
    assert!(store.get_note_version(&ws_id, &note.id, 5).await.is_err());
    assert!(store
        .get_note_version(&ws_id, &note.id, total + 1)
        .await
        .is_err());

    // Deleting the note cascades to its versions.
    store
        .delete_note(&ws_id, &note.id)
        .await
        .expect("delete note");
    let after = store
        .list_note_versions(&ws_id, &note.id)
        .await
        .expect("list after delete");
    assert!(after.is_empty(), "note delete cascades to note_version");
}

/// A failed statement inside `append_note_version`'s transaction rolls the
/// whole write back and leaves the pooled write connection usable: appending
/// for an absent note trips the composite `(note_id, workspace_id)` FK at
/// INSERT time (`foreign_keys = ON`), nothing persists, and a subsequent
/// append for a real note succeeds.
#[tokio::test]
async fn append_note_version_rolls_back_on_body_error() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws_id = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws_id, "WS", false))
        .await
        .expect("insert ws");
    let note = task_note(&ws_id, "Note", None);
    store.insert_note(&note).await.expect("insert note");
    let author = NoteVersionAuthor {
        id: "system".to_string(),
        name: "intentd".to_string(),
        author_type: "system".to_string(),
    };
    let ts = now_iso();

    // Ghost note row → the version INSERT violates the FK immediately.
    let mut ghost = note.clone();
    ghost.id = NoteId::new();
    assert!(store
        .append_note_version(&ghost, &author, &ts)
        .await
        .is_err());
    assert!(store
        .list_note_versions(&ws_id, &ghost.id)
        .await
        .expect("list ghost versions")
        .is_empty());

    // The write connection is clean: a normal append still works.
    let v = store
        .append_note_version(&note, &author, &ts)
        .await
        .expect("append after body error");
    assert_eq!(v, 1);
}

/// Regression for monorepo#657: a failed COMMIT in `append_note_version`
/// must roll the transaction back so the sole write-pool connection
/// (`max_connections=1`) is not returned to the pool still holding an open
/// transaction + write lock. `defer_foreign_keys = ON` postpones a
/// ghost-note FK violation to COMMIT time, forcing the COMMIT itself to
/// fail; pre-fix code propagated the error without ROLLBACK, poisoning the
/// connection so every later `BEGIN IMMEDIATE` failed.
#[tokio::test]
async fn append_note_version_rolls_back_on_failed_commit() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws_id = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws_id, "WS", false))
        .await
        .expect("insert ws");
    let note = task_note(&ws_id, "Note", None);
    store.insert_note(&note).await.expect("insert note");
    let author = NoteVersionAuthor {
        id: "system".to_string(),
        name: "intentd".to_string(),
        author_type: "system".to_string(),
    };
    let ts = now_iso();

    // Arm the deferred-FK trap on the single write-pool connection; the
    // pragma stays in effect until the next transaction concludes, so the
    // append below passes its INSERT and fails at COMMIT instead.
    {
        let mut conn = store
            .write_pool()
            .acquire()
            .await
            .expect("acquire write conn");
        sqlx::query("PRAGMA defer_foreign_keys = ON")
            .execute(&mut *conn)
            .await
            .expect("defer FKs");
    }
    let mut ghost = note.clone();
    ghost.id = NoteId::new();
    let err = store
        .append_note_version(&ghost, &author, &ts)
        .await
        .expect_err("COMMIT must fail on the deferred FK violation");
    assert!(
        err.to_string().contains("commit"),
        "unexpected error: {err}"
    );

    // Nothing persisted for the ghost note...
    assert!(store
        .list_note_versions(&ws_id, &ghost.id)
        .await
        .expect("list ghost versions")
        .is_empty());
    // ...and the failed COMMIT was rolled back, not left open: the next
    // append reuses the same pooled connection and commits normally.
    let v = store
        .append_note_version(&note, &author, &ts)
        .await
        .expect("append after failed COMMIT");
    assert_eq!(v, 1);
}

/// Regression for monorepo#680: when the explicit ROLLBACK after a body
/// error also fails, `append_note_version` must detach+close the connection
/// instead of returning the potentially poisoned handle to the sole-
/// connection write pool. A `RAISE(ROLLBACK)` trigger fails the version
/// INSERT *and* auto-rolls the transaction back, so the explicit ROLLBACK
/// that follows fails ("cannot rollback - no transaction is active");
/// pre-fix that failure was ignored and the connection went back to the
/// pool (write pool size stayed 1 instead of dropping to 0).
#[tokio::test]
async fn append_note_version_detaches_conn_on_failed_body_error_rollback() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws_id = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws_id, "WS", false))
        .await
        .expect("insert ws");
    let note = task_note(&ws_id, "Note", None);
    store.insert_note(&note).await.expect("insert note");
    let author = NoteVersionAuthor {
        id: "system".to_string(),
        name: "intentd".to_string(),
        author_type: "system".to_string(),
    };
    let ts = now_iso();

    // Arm the trap: the version INSERT fails its own statement and rolls
    // the whole transaction back, so the explicit ROLLBACK finds none open.
    sqlx::query(
        "CREATE TRIGGER rollback_trap AFTER INSERT ON note_version BEGIN
             SELECT RAISE(ROLLBACK, 'rollback trap');
         END",
    )
    .execute(store.write_pool())
    .await
    .expect("create trap trigger");

    let err = store
        .append_note_version(&note, &author, &ts)
        .await
        .expect_err("INSERT must fail on the rollback trigger");
    assert!(
        err.to_string().contains("insert note_version failed"),
        "unexpected error: {err}"
    );

    // The failed ROLLBACK detached+closed the connection rather than
    // returning it to the pool.
    assert_eq!(store.write_pool().size(), 0);

    // Nothing persisted from the trapped append...
    assert!(store
        .list_note_versions(&ws_id, &note.id)
        .await
        .expect("list versions")
        .is_empty());
    // ...and the pool opens a fresh replacement on demand: with the trap
    // disarmed, the next append succeeds.
    sqlx::query("DROP TRIGGER rollback_trap")
        .execute(store.write_pool())
        .await
        .expect("drop trap trigger");
    let v = store
        .append_note_version(&note, &author, &ts)
        .await
        .expect("append after detach");
    assert_eq!(v, 1);
}

/// Captures WARN-level tracing events as flat `field=value` strings so tests
/// can assert on the poisoned-connection event (monorepo#711) without a
/// `tracing-subscriber` dev-dependency.
struct WarnCapture {
    events: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
    next_span_id: std::sync::atomic::AtomicU64,
}

impl tracing::Subscriber for WarnCapture {
    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        *metadata.level() == tracing::Level::WARN
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        let id = self
            .next_span_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        tracing::span::Id::from_u64(id)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        struct Visitor<'a>(&'a mut String);
        impl tracing::field::Visit for Visitor<'_> {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                use std::fmt::Write;
                let _ = write!(self.0, "{}={:?} ", field.name(), value);
            }
        }
        let mut line = String::new();
        event.record(&mut Visitor(&mut line));
        self.events.lock().unwrap().push(line);
    }
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
}

/// Regression for monorepo#711: when the detach+close path of
/// `rollback_or_poison` fires (failed ROLLBACK after a failed body, per the
/// #680 repro above), it must emit a WARN-level tracing event carrying the
/// ROLLBACK error instead of dropping it silently.
#[tokio::test]
async fn rollback_or_poison_emits_warn_on_detach() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws_id = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws_id, "WS", false))
        .await
        .expect("insert ws");
    let note = task_note(&ws_id, "Note", None);
    store.insert_note(&note).await.expect("insert note");
    let author = NoteVersionAuthor {
        id: "system".to_string(),
        name: "intentd".to_string(),
        author_type: "system".to_string(),
    };
    let ts = now_iso();

    // Arm the trap: the version INSERT fails its own statement and rolls
    // the whole transaction back, so the explicit ROLLBACK finds none open.
    sqlx::query(
        "CREATE TRIGGER rollback_trap AFTER INSERT ON note_version BEGIN
             SELECT RAISE(ROLLBACK, 'rollback trap');
         END",
    )
    .execute(store.write_pool())
    .await
    .expect("create trap trigger");

    let captured = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let guard = tracing::subscriber::set_default(WarnCapture {
        events: captured.clone(),
        next_span_id: std::sync::atomic::AtomicU64::new(1),
    });
    store
        .append_note_version(&note, &author, &ts)
        .await
        .expect_err("INSERT must fail on the rollback trigger");
    drop(guard);

    // The detach path fired (pool size dropped to 0) and emitted a WARN
    // event carrying the ROLLBACK error.
    assert_eq!(store.write_pool().size(), 0);
    let events = captured.lock().unwrap();
    let warn = events
        .iter()
        .find(|e| e.contains("rollback_error="))
        .unwrap_or_else(|| panic!("no poisoned-connection WARN captured; got: {events:?}"));
    let rollback_error = warn
        .split("rollback_error=")
        .nth(1)
        .map(str::trim)
        .unwrap_or_default();
    assert!(
        !rollback_error.is_empty() && rollback_error != "\"\"",
        "WARN must carry a non-empty rollback_error, got: {warn}"
    );
    assert!(
        warn.contains("ROLLBACK failed"),
        "WARN must describe the detach+close path, got: {warn}"
    );
}

#[tokio::test]
async fn note_rev_increments_on_update() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");

    let ws_id = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws_id, "WS", false))
        .await
        .expect("insert ws");

    let ts = now_iso();
    let mut note = Note {
        id: NoteId::new(),
        workspace_id: ws_id.clone(),
        title: "Spec".to_string(),
        content: "# Hello".to_string(),
        content_type: ContentType::Markdown,
        tags: vec![],
        is_pinned: false,
        is_archived: false,
        is_default: false,
        parent_id: None,
        visibility: NoteVisibility::Workspace,
        metadata: NoteMetadata::default(),
        created_at: ts.clone(),
        rev: 0,
        updated_at: ts,
    };
    store.insert_note(&note).await.expect("insert note");

    // Fresh insert starts at rev 0.
    let after_insert = store.get_note(&ws_id, &note.id).await.expect("get note");
    assert_eq!(after_insert.rev, 0);

    // First update bumps rev → 1 (the bump is store-owned, ignoring the stale
    // in-memory `rev` carried by the passed-in note).
    note.content = "# Hello v2".to_string();
    store.update_note(&note).await.expect("update note");
    let after_first = store.get_note(&ws_id, &note.id).await.expect("get note");
    assert_eq!(after_first.rev, 1);
    assert_eq!(after_first.content, "# Hello v2");

    // A second update bumps again → 2 (monotonic).
    note.content = "# Hello v3".to_string();
    store.update_note(&note).await.expect("update note");
    let after_second = store.get_note(&ws_id, &note.id).await.expect("get note");
    assert_eq!(after_second.rev, 2);
}

#[tokio::test]
async fn update_note_versioned_hit_miss_and_absent() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");

    let ws_id = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws_id, "WS", false))
        .await
        .expect("insert ws");

    let ts = now_iso();
    let mut note = Note {
        id: NoteId::new(),
        workspace_id: ws_id.clone(),
        title: "Spec".to_string(),
        content: "v0".to_string(),
        content_type: ContentType::Markdown,
        tags: vec![],
        is_pinned: false,
        is_archived: false,
        is_default: false,
        parent_id: None,
        visibility: NoteVisibility::Workspace,
        metadata: NoteMetadata::default(),
        created_at: ts.clone(),
        rev: 0,
        updated_at: ts,
    };
    store.insert_note(&note).await.expect("insert note");

    // HIT: expected_version matches the stored rev (0) → write + bump to 1.
    note.content = "v1".to_string();
    store
        .update_note_versioned(&note, Some(0))
        .await
        .expect("versioned hit");
    let after_hit = store.get_note(&ws_id, &note.id).await.expect("get note");
    assert_eq!(after_hit.rev, 1);
    assert_eq!(after_hit.content, "v1");

    // MISS: stale expected_version (0, but stored rev is now 1) → Conflict
    // carrying the current entity under `current` (rev 1, unchanged content).
    note.content = "v2-should-not-persist".to_string();
    let conflict = store.update_note_versioned(&note, Some(0)).await;
    match conflict {
        Err(intent_core::Error::Conflict { current }) => {
            assert_eq!(current["rev"], 1);
            assert_eq!(current["content"], "v1");
            assert_eq!(current["id"], note.id.0);
        }
        other => panic!("expected Conflict, got {other:?}"),
    }
    // The failed conditional write must not have persisted or bumped.
    let after_miss = store.get_note(&ws_id, &note.id).await.expect("get note");
    assert_eq!(after_miss.rev, 1);
    assert_eq!(after_miss.content, "v1");

    // ABSENT: no expected_version → last-writer-wins (unconditional bump → 2).
    note.content = "v2".to_string();
    store
        .update_note_versioned(&note, None)
        .await
        .expect("absent degrades to last-writer-wins");
    let after_absent = store.get_note(&ws_id, &note.id).await.expect("get note");
    assert_eq!(after_absent.rev, 2);
    assert_eq!(after_absent.content, "v2");

    // A versioned write against a missing row is NotFound (not Conflict).
    let mut ghost = note.clone();
    ghost.id = NoteId::new();
    match store.update_note_versioned(&ghost, Some(5)).await {
        Err(intent_core::Error::NotFound(_)) => {}
        other => panic!("expected NotFound for absent row, got {other:?}"),
    }
}

#[tokio::test]
async fn delete_note_versioned_hit_miss_and_absent() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");

    let ws_id = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws_id, "WS", false))
        .await
        .expect("insert ws");

    let ts = now_iso();
    let note = Note {
        id: NoteId::new(),
        workspace_id: ws_id.clone(),
        title: "Spec".to_string(),
        content: "v0".to_string(),
        content_type: ContentType::Markdown,
        tags: vec![],
        is_pinned: false,
        is_archived: false,
        is_default: false,
        parent_id: None,
        visibility: NoteVisibility::Workspace,
        metadata: NoteMetadata::default(),
        created_at: ts.clone(),
        rev: 0,
        updated_at: ts,
    };
    store.insert_note(&note).await.expect("insert note");

    // MISS: stale expected_version (5, but stored rev is 0) → Conflict carrying
    // the current entity snapshot prior to deletion; the row survives.
    let conflict = store.delete_note_versioned(&ws_id, &note.id, Some(5)).await;
    match conflict {
        Err(intent_core::Error::Conflict { current }) => {
            assert_eq!(current["rev"], 0);
            assert_eq!(current["id"], note.id.0);
        }
        other => panic!("expected Conflict, got {other:?}"),
    }
    assert!(store.get_note(&ws_id, &note.id).await.is_ok());

    // HIT: expected_version matches the stored rev (0) → row deleted.
    store
        .delete_note_versioned(&ws_id, &note.id, Some(0))
        .await
        .expect("versioned delete hit");
    match store.get_note(&ws_id, &note.id).await {
        Err(intent_core::Error::NotFound(_)) => {}
        other => panic!("expected NotFound after delete, got {other:?}"),
    }

    // A versioned delete against a missing row is NotFound (not Conflict).
    match store.delete_note_versioned(&ws_id, &note.id, Some(0)).await {
        Err(intent_core::Error::NotFound(_)) => {}
        other => panic!("expected NotFound for absent row, got {other:?}"),
    }

    // ABSENT: a fresh row with no expected_version → unconditional delete.
    let ts2 = now_iso();
    let other = Note {
        id: NoteId::new(),
        created_at: ts2.clone(),
        updated_at: ts2,
        ..note.clone()
    };
    store.insert_note(&other).await.expect("insert note 2");
    store
        .delete_note_versioned(&ws_id, &other.id, None)
        .await
        .expect("unconditional delete");
    match store.get_note(&ws_id, &other.id).await {
        Err(intent_core::Error::NotFound(_)) => {}
        other => panic!("expected NotFound after unconditional delete, got {other:?}"),
    }
}

/// A top-level, non-task, non-archived note in the pre-#110 stray-spec shape
/// (adoption candidate when titled "Spec").
fn stray_note(ws_id: &WorkspaceId, id: &str, title: &str) -> Note {
    let ts = now_iso();
    Note {
        id: NoteId::from(id),
        workspace_id: ws_id.clone(),
        title: title.to_string(),
        content: "# stray".to_string(),
        content_type: ContentType::Markdown,
        tags: vec![],
        is_pinned: false,
        is_archived: false,
        is_default: false,
        parent_id: None,
        visibility: NoteVisibility::Workspace,
        metadata: NoteMetadata::default(),
        created_at: ts.clone(),
        rev: 0,
        updated_at: ts,
    }
}

/// Prove no transaction was left open on the sole write-pool connection: with
/// `max_connections=1` this acquires the SAME connection the previous call
/// used, and a leaked open transaction would make `BEGIN IMMEDIATE` fail with
/// "cannot start a transaction within a transaction".
async fn assert_no_open_write_transaction(store: &Store, context: &str) {
    let mut conn = store.write_pool().acquire().await.expect("acquire probe");
    sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut *conn)
        .await
        .unwrap_or_else(|e| panic!("transaction leaked after {context}: {e}"));
    sqlx::query("COMMIT")
        .execute(&mut *conn)
        .await
        .expect("commit probe txn");
}

/// Post-conversion to raw `BEGIN IMMEDIATE` (monorepo#796, mirroring the #783
/// shape): the spec-already-exists early return writes nothing AND leaves no
/// transaction open on the write-pool connection. (The third early-return
/// path — `rows_affected != 1` on the id rewrite — is unreachable without an
/// injection seam: the candidate SELECT and the UPDATE run inside one
/// IMMEDIATE transaction, so no other writer can remove the row in between;
/// the same guard-COMMIT covers it.)
#[tokio::test]
async fn adopt_stray_spec_when_spec_exists_leaves_no_open_transaction() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws_id = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws_id, "WS", false))
        .await
        .expect("insert ws");
    store
        .insert_note(&stray_note(&ws_id, "spec", "Spec"))
        .await
        .expect("insert real spec");
    store
        .insert_note(&stray_note(&ws_id, "stray-1", "Spec"))
        .await
        .expect("insert stray");

    let adopted = store.adopt_stray_spec_note(&ws_id).await.expect("adopt");
    assert!(adopted.is_none(), "must bail when spec already exists");

    assert_no_open_write_transaction(&store, "spec-exists early return").await;

    // The stray is untouched — no partial write escaped the transaction.
    let stray = store
        .get_note(&ws_id, &NoteId::from("stray-1"))
        .await
        .expect("stray still present");
    assert!(!stray.is_pinned);
    assert!(!stray.is_default);
}

/// The candidate-count early return (zero candidates, then ≥2 candidates)
/// leaves no open transaction and no partial write (monorepo#796).
#[tokio::test]
async fn adopt_stray_spec_without_single_candidate_leaves_no_open_transaction() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws_id = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws_id, "WS", false))
        .await
        .expect("insert ws");

    // Zero candidates.
    let adopted = store.adopt_stray_spec_note(&ws_id).await.expect("adopt");
    assert!(adopted.is_none(), "no candidate must adopt nothing");
    assert_no_open_write_transaction(&store, "zero-candidate early return").await;

    // ≥2 candidates (trim + case-insensitive title match).
    store
        .insert_note(&stray_note(&ws_id, "stray-1", "Spec"))
        .await
        .expect("insert stray 1");
    store
        .insert_note(&stray_note(&ws_id, "stray-2", "  spec  "))
        .await
        .expect("insert stray 2");
    let adopted = store.adopt_stray_spec_note(&ws_id).await.expect("adopt");
    assert!(adopted.is_none(), "ambiguous candidates must adopt nothing");
    assert_no_open_write_transaction(&store, "ambiguous-candidate early return").await;

    // Both strays untouched under their original ids.
    for id in ["stray-1", "stray-2"] {
        let n = store
            .get_note(&ws_id, &NoteId::from(id))
            .await
            .expect("stray still present");
        assert!(!n.is_pinned);
        assert!(!n.is_default);
    }
}

/// FK deferral under raw `BEGIN IMMEDIATE` (monorepo#796): an adoption whose
/// stray carries dependents keyed by the composite `(note_id, workspace_id)`
/// FKs — a child note, a version, line attribution, a comment — commits
/// cleanly (the deferred check passes once the referenced key and its
/// dependents are rewritten together) and leaves no open transaction.
#[tokio::test]
async fn adopt_stray_spec_with_dependents_commits_cleanly() {
    use std::collections::BTreeMap;

    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws_id = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws_id, "WS", false))
        .await
        .expect("insert ws");

    let stray = stray_note(&ws_id, "stray-1", "Spec");
    store.insert_note(&stray).await.expect("insert stray");
    let mut child = stray_note(&ws_id, "child-1", "Child");
    child.parent_id = Some(stray.id.clone());
    store.insert_note(&child).await.expect("insert child");

    let author = NoteVersionAuthor {
        id: "user".to_string(),
        name: "User".to_string(),
        author_type: "user".to_string(),
    };
    store
        .append_note_version(&stray, &author, &stray.created_at)
        .await
        .expect("append version");

    let mut attributions = BTreeMap::new();
    attributions.insert(
        "1".to_string(),
        intent_core::LineAttributionInfo {
            timestamp: 0,
            author: None,
        },
    );
    store
        .upsert_note_line_attribution(&intent_core::LineAttributionData {
            note_id: stray.id.clone(),
            workspace_id: ws_id.clone(),
            computed_at: stray.created_at.clone(),
            attributions,
        })
        .await
        .expect("upsert attribution");

    let comment = Comment {
        id: "c1".to_string(),
        thread_id: "t1".to_string(),
        note_id: Some(stray.id.clone()),
        kind: CommentType::Comment,
        content: "hi".to_string(),
        author: "user".to_string(),
        author_type: AuthorType::User,
        status: CommentStatus::Open,
        parent_id: None,
        anchor: Some(CommentAnchor {
            kind: CommentAnchorType::Range,
            ..Default::default()
        }),
        anchor_text: None,
        anchor_before: None,
        anchor_after: None,
        suggestion_original: None,
        suggestion_proposed: None,
        agent_id: None,
        is_orphaned: None,
        created_at: stray.created_at.clone(),
        updated_at: stray.created_at.clone(),
    };
    store
        .insert_comment(&ws_id, &comment)
        .await
        .expect("insert comment");

    let adopted = store.adopt_stray_spec_note(&ws_id).await.expect("adopt");
    assert_eq!(adopted, Some((stray.id.clone(), "Spec".to_string())));

    assert_no_open_write_transaction(&store, "successful adoption").await;

    let spec_id = NoteId::from("spec");
    let spec = store.get_note(&ws_id, &spec_id).await.expect("spec");
    assert!(spec.is_pinned);
    assert!(spec.is_default);
    assert!(spec.tags.iter().any(|t| t == "spec"));
    let child_now = store
        .get_note(&ws_id, &NoteId::from("child-1"))
        .await
        .expect("child");
    assert_eq!(child_now.parent_id, Some(spec_id.clone()));
    let versions = store
        .list_note_versions(&ws_id, &spec_id)
        .await
        .expect("versions");
    assert_eq!(versions.len(), 1);
    let attr = store
        .get_note_line_attribution(&ws_id, &spec_id)
        .await
        .expect("attribution");
    assert!(attr.is_some());
    let comments = store
        .list_comments_in_workspace(&ws_id, &spec_id)
        .await
        .expect("comments");
    assert_eq!(comments.len(), 1);
}

/// `PRAGMA defer_foreign_keys = ON` under raw `BEGIN IMMEDIATE` defers — but
/// does not disable — FK enforcement: a genuinely broken composite FK passes
/// at statement time and fails the COMMIT (monorepo#796).
#[tokio::test]
async fn deferred_fk_enforcement_still_fires_at_commit() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws_id = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws_id, "WS", false))
        .await
        .expect("insert ws");

    let mut conn = store.write_pool().acquire().await.expect("acquire");
    sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut *conn)
        .await
        .expect("begin");
    sqlx::query("PRAGMA defer_foreign_keys = ON")
        .execute(&mut *conn)
        .await
        .expect("defer FKs");
    // Violates the composite FK on note_version — deferred, so the statement
    // itself succeeds.
    sqlx::query(
        "INSERT INTO note_version (note_id, workspace_id, v, date, author_id, \
         author_name, author_type, title, content) \
         VALUES ('no-such-note', ?, 1, '2026-01-01', 'u', 'U', 'user', 't', 'c')",
    )
    .bind(&ws_id.0)
    .execute(&mut *conn)
    .await
    .expect("deferred FK violation must not fail at statement time");
    let commit = sqlx::query("COMMIT").execute(&mut *conn).await;
    assert!(
        commit.is_err(),
        "deferred FK violation must fail the COMMIT"
    );
    // A failed deferred-FK COMMIT leaves the transaction active; close it.
    sqlx::query("ROLLBACK")
        .execute(&mut *conn)
        .await
        .expect("rollback after failed commit");
    drop(conn);

    assert_no_open_write_transaction(&store, "failed deferred-FK commit").await;
    let versions = store
        .list_note_versions(&ws_id, &NoteId::from("no-such-note"))
        .await
        .expect("list versions");
    assert!(versions.is_empty(), "violating row must not persist");
}

fn task_note(ws_id: &WorkspaceId, title: &str, task: Option<TaskMetadata>) -> Note {
    let ts = now_iso();
    Note {
        id: NoteId::new(),
        workspace_id: ws_id.clone(),
        title: title.to_string(),
        content: String::new(),
        content_type: ContentType::Markdown,
        tags: vec![],
        is_pinned: false,
        is_archived: false,
        is_default: false,
        parent_id: None,
        visibility: NoteVisibility::Workspace,
        metadata: NoteMetadata { task },
        created_at: ts.clone(),
        rev: 0,
        updated_at: ts,
    }
}

#[tokio::test]
async fn task_metadata_round_trip_and_list_tasks() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws_id = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws_id, "WS", false))
        .await
        .expect("insert ws");

    let meta = TaskMetadata {
        status: TaskStatus::ReviewRequired,
        assigned_agent_ids: vec![AgentId::from("agent-1"), AgentId::from("agent-2")],
        acceptance_criteria: vec!["builds".to_string(), "tests pass".to_string()],
        estimated_effort: Some("M".to_string()),
        actual_effort: None,
        blocked_reason: None,
        started_at: Some(now_iso()),
        completed_at: None,
        peer_order: Some(100),
        depends_on: vec![NoteId::from("dep-1")],
        conflicts_with: vec![NoteId::from("conflict-1")],
        unmet_depends_on: Vec::new(),
    };
    store
        .insert_note(&task_note(&ws_id, "Task A", Some(meta.clone())))
        .await
        .expect("insert task note");
    store
        .insert_note(&task_note(&ws_id, "Plain note", None))
        .await
        .expect("insert plain note");

    let tasks = store.list_tasks(&ws_id).await.expect("list tasks");
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].metadata.task, Some(meta.clone()));

    // The computed `unmet_depends_on` projection is stripped at encode time
    // (monorepo#1979): a note written with it populated reads back clean.
    let mut projected = task_note(
        &ws_id,
        "Task B",
        Some(TaskMetadata {
            unmet_depends_on: vec![NoteId::from("dep-1")],
            ..meta
        }),
    );
    store.insert_note(&projected).await.expect("insert B");
    let read = store.get_note(&ws_id, &projected.id).await.expect("get B");
    assert!(read.metadata.task.unwrap().unmet_depends_on.is_empty());
    projected.metadata.task.as_mut().unwrap().unmet_depends_on = vec![NoteId::from("dep-2")];
    store.update_note(&projected).await.expect("update B");
    let read = store
        .get_note(&ws_id, &projected.id)
        .await
        .expect("get B after update");
    assert!(read.metadata.task.unwrap().unmet_depends_on.is_empty());
}

fn sample_comment(note_id: &NoteId, thread_id: &str, id: &str) -> Comment {
    let ts = now_iso();
    Comment {
        id: id.to_string(),
        thread_id: thread_id.to_string(),
        note_id: Some(note_id.clone()),
        kind: CommentType::Suggestion,
        content: "please rename".to_string(),
        author: "alice".to_string(),
        author_type: AuthorType::User,
        status: CommentStatus::Open,
        parent_id: None,
        anchor: Some(CommentAnchor {
            kind: CommentAnchorType::Range,
            start_id: Some("a1".to_string()),
            end_id: Some("a2".to_string()),
            point_id: None,
        }),
        anchor_text: Some("foo".to_string()),
        anchor_before: Some("the ".to_string()),
        anchor_after: Some(" bar".to_string()),
        suggestion_original: Some("foo".to_string()),
        suggestion_proposed: Some("baz".to_string()),
        agent_id: Some(AgentId::from("agent-9")),
        is_orphaned: None,
        created_at: ts.clone(),
        updated_at: ts,
    }
}

#[tokio::test]
async fn comment_round_trip_update_delete_and_thread() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws_id = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws_id, "WS", false))
        .await
        .expect("insert ws");
    let note = task_note(&ws_id, "Note", None);
    store.insert_note(&note).await.expect("insert note");

    let c1 = sample_comment(&note.id, "thread-1", "c1");
    // c2 is a legacy-row scenario: a reply persisted before monorepo#729 that
    // still carries a cloned anchor — it must round-trip unchanged.
    let mut c2 = sample_comment(&note.id, "thread-1", "c2");
    c2.parent_id = Some("c1".to_string());
    c2.kind = CommentType::Comment;
    c2.anchor = Some(CommentAnchor {
        kind: CommentAnchorType::Point,
        point_id: Some("p1".to_string()),
        ..Default::default()
    });
    c2.anchor_before = None;
    c2.anchor_after = None;
    c2.suggestion_original = None;
    c2.suggestion_proposed = None;
    c2.agent_id = None;
    // c3 is a contract-shaped reply: no anchor at all — the store must
    // persist `null` and decode it back as `None` (monorepo#729).
    let mut c3 = sample_comment(&note.id, "thread-1", "c3");
    c3.parent_id = Some("c1".to_string());
    c3.kind = CommentType::Comment;
    c3.anchor = None;
    c3.anchor_text = None;
    c3.anchor_before = None;
    c3.anchor_after = None;
    c3.suggestion_original = None;
    c3.suggestion_proposed = None;
    c3.agent_id = None;
    store.insert_comment(&ws_id, &c1).await.expect("insert c1");
    store.insert_comment(&ws_id, &c2).await.expect("insert c2");
    store.insert_comment(&ws_id, &c3).await.expect("insert c3");

    let got = store.get_comment("c1").await.expect("get c1");
    assert_eq!(got, c1);
    let got = store.get_comment("c2").await.expect("get c2");
    assert_eq!(got, c2);
    let got = store.get_comment("c3").await.expect("get c3");
    assert_eq!(got, c3);
    assert!(got.anchor.is_none());

    let by_note = store.list_comments(&note.id).await.expect("list comments");
    assert_eq!(by_note.len(), 3);

    let thread = store.get_thread("thread-1").await.expect("get thread");
    assert_eq!(thread.thread_id, "thread-1");
    assert_eq!(thread.comments.len(), 3);

    let mut updated = c1.clone();
    updated.status = CommentStatus::Resolved;
    updated.content = "resolved now".to_string();
    store
        .update_comment(&ws_id, &updated)
        .await
        .expect("update c1");
    let reread = store.get_comment("c1").await.expect("reget c1");
    assert_eq!(reread.status, CommentStatus::Resolved);
    assert_eq!(reread.content, "resolved now");

    store.delete_comment(&ws_id, "c1").await.expect("delete c1");
    assert!(store.get_comment("c1").await.is_err());
    assert_eq!(
        store
            .list_comments(&note.id)
            .await
            .expect("list after del")
            .len(),
        2
    );
}

/// `update_note_with_comment` commits the note rewrite + comment INSERT in
/// one transaction (monorepo#638): success returns the post-rewrite `rev`
/// and persists both; a failed INSERT rolls the note rewrite back (no
/// anchor markers without a comment row); an absent note is `NotFound`.
#[tokio::test]
async fn update_note_with_comment_is_atomic_and_returns_rev() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws_id = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws_id, "WS", false))
        .await
        .expect("insert ws");
    let mut note = task_note(&ws_id, "Note", None);
    store.insert_note(&note).await.expect("insert note");

    // Success: both persist, returned rev is the post-rewrite value (0 → 1).
    note.content = "with <!--anchor:c1:start-->markers<!--anchor:c1:end-->".to_string();
    let c1 = sample_comment(&note.id, "c1", "c1");
    let rev = store
        .update_note_with_comment(&note, &c1)
        .await
        .expect("atomic update+insert");
    assert_eq!(rev, 1);
    let stored = store.get_note(&ws_id, &note.id).await.expect("get note");
    assert_eq!(stored.rev, 1);
    assert_eq!(stored.content, note.content);
    assert_eq!(store.get_comment("c1").await.expect("get c1"), c1);

    // Failure (duplicate comment id → INSERT fails): the note rewrite must
    // roll back — content and rev stay at the committed state above.
    note.content = "rewrite-that-must-roll-back".to_string();
    let dup = sample_comment(&note.id, "c1", "c1");
    assert!(store.update_note_with_comment(&note, &dup).await.is_err());
    let after_fail = store.get_note(&ws_id, &note.id).await.expect("get note");
    assert_eq!(after_fail.rev, 1);
    assert_eq!(after_fail.content, stored.content);

    // Absent note row → NotFound, and the comment must not persist.
    let mut ghost = note.clone();
    ghost.id = NoteId::new();
    let c2 = sample_comment(&ghost.id, "c2", "c2");
    match store.update_note_with_comment(&ghost, &c2).await {
        Err(intent_core::Error::NotFound(_)) => {}
        other => panic!("expected NotFound for absent note, got {other:?}"),
    }
    assert!(store.get_comment("c2").await.is_err());
}

/// Regression for monorepo#680 at the `update_note_with_comment` site: a
/// `RAISE(ROLLBACK)` trigger on the note UPDATE fails the body *and*
/// auto-rolls the transaction back, so the explicit ROLLBACK fails and the
/// hardened arm must detach+close the connection (write pool size drops to
/// 0) instead of pooling the potentially poisoned handle.
#[tokio::test]
async fn update_note_with_comment_detaches_conn_on_failed_body_error_rollback() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws_id = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws_id, "WS", false))
        .await
        .expect("insert ws");
    let mut note = task_note(&ws_id, "Note", None);
    store.insert_note(&note).await.expect("insert note");

    // Arm the trap: the note UPDATE fails its own statement and rolls the
    // whole transaction back, so the explicit ROLLBACK finds none open.
    sqlx::query(
        "CREATE TRIGGER rollback_trap AFTER UPDATE ON note BEGIN
             SELECT RAISE(ROLLBACK, 'rollback trap');
         END",
    )
    .execute(store.write_pool())
    .await
    .expect("create trap trigger");

    note.content = "rewrite-that-must-roll-back".to_string();
    let c1 = sample_comment(&note.id, "c1", "c1");
    let err = store
        .update_note_with_comment(&note, &c1)
        .await
        .expect_err("UPDATE must fail on the rollback trigger");
    assert!(
        err.to_string().contains("update note failed"),
        "unexpected error: {err}"
    );

    // The failed ROLLBACK detached+closed the connection rather than
    // returning it to the pool.
    assert_eq!(store.write_pool().size(), 0);

    // Nothing persisted from the trapped call...
    let stored = store.get_note(&ws_id, &note.id).await.expect("get note");
    assert_eq!(stored.rev, 0);
    assert!(store.get_comment("c1").await.is_err());
    // ...and the pool opens a fresh replacement on demand: with the trap
    // disarmed, the same call succeeds.
    sqlx::query("DROP TRIGGER rollback_trap")
        .execute(store.write_pool())
        .await
        .expect("drop trap trigger");
    let rev = store
        .update_note_with_comment(&note, &c1)
        .await
        .expect("update after detach");
    assert_eq!(rev, 1);
}

/// `update_comment` must not drop legacy/unknown `extra_json` keys preserved
/// by `insert_comment_with_extras` (legacy importer): the update rebuilds the
/// known fields but carries unknown keys over from the existing row.
#[tokio::test]
async fn comment_update_preserves_legacy_extra_keys() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws_id = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws_id, "WS", false))
        .await
        .expect("insert ws");
    let note = task_note(&ws_id, "Note", None);
    store.insert_note(&note).await.expect("insert note");

    let c1 = sample_comment(&note.id, "thread-1", "c1");
    let mut legacy = serde_json::Map::new();
    legacy.insert("legacyMarkId".to_string(), json!("mark-7"));
    legacy.insert("legacyRev".to_string(), json!(3));
    // Wrong-typed legacy isOrphaned preserved verbatim by the importer.
    legacy.insert("isOrphaned".to_string(), json!("yes"));
    store
        .insert_comment_with_extras(&ws_id, &c1, &legacy)
        .await
        .expect("insert with extras");

    let mut updated = c1.clone();
    updated.status = CommentStatus::Resolved;
    store
        .update_comment(&ws_id, &updated)
        .await
        .expect("update c1");

    // Known fields round-trip through the wire-facing Comment...
    let reread = store.get_comment("c1").await.expect("reget c1");
    assert_eq!(reread.status, CommentStatus::Resolved);
    assert_eq!(reread.anchor_before, c1.anchor_before);
    // ...and the raw extra_json blob still carries the legacy keys.
    let row = sqlx::query("SELECT extra_json FROM comment WHERE id = 'c1'")
        .fetch_one(store.read_pool())
        .await
        .expect("raw extra_json");
    let raw: Option<String> = sqlx::Row::get(&row, "extra_json");
    let blob: serde_json::Value =
        serde_json::from_str(&raw.expect("extra_json present")).expect("valid json");
    assert_eq!(blob["legacyMarkId"], json!("mark-7"));
    assert_eq!(blob["legacyRev"], json!(3));
    // The non-bool isOrphaned survives updates too, even though the key is
    // otherwise store-owned.
    assert_eq!(blob["isOrphaned"], json!("yes"));
}

/// Store-layer defense-in-depth for comment mutations: UPDATE/DELETE and
/// `set_thread_status` all scope by `(id, workspace_id)`, so a caller
/// declaring workspace B cannot mutate a comment row that belongs to
/// workspace A. Bare-id probes surface as `NotFound` / zero-row updates
/// depending on the mutation shape (mirrors the `note_repo` 0022 pattern).
#[tokio::test]
async fn comment_mutations_reject_cross_workspace_bare_id_writes() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws_a = WorkspaceId::new();
    let ws_b = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws_a, "A", false))
        .await
        .expect("insert ws_a");
    store
        .insert_workspace(&sample_workspace(&ws_b, "B", false))
        .await
        .expect("insert ws_b");
    let note = task_note(&ws_a, "Note", None);
    store.insert_note(&note).await.expect("insert note");
    let c1 = sample_comment(&note.id, "thread-x", "c1");
    store
        .insert_comment(&ws_a, &c1)
        .await
        .expect("insert c1 in ws_a");

    // Cross-workspace UPDATE returns NotFound and does NOT mutate the row.
    let mut mutated = c1.clone();
    mutated.content = "cross-ws mutation".to_string();
    let err = store
        .update_comment(&ws_b, &mutated)
        .await
        .expect_err("cross-ws update must not mutate");
    assert!(matches!(err, Error::NotFound(_)), "update: {err:?}");
    let reread = store.get_comment("c1").await.expect("still readable");
    assert_ne!(reread.content, "cross-ws mutation");

    // Cross-workspace DELETE returns NotFound and does NOT remove the row.
    let err = store
        .delete_comment(&ws_b, "c1")
        .await
        .expect_err("cross-ws delete must not remove");
    assert!(matches!(err, Error::NotFound(_)), "delete: {err:?}");
    store.get_comment("c1").await.expect("row still present");

    // Cross-workspace resolve is a no-op (zero rows affected).
    let rows = store
        .set_thread_status(&ws_b, "thread-x", CommentStatus::Resolved, "now")
        .await
        .expect("set_thread_status returns");
    assert_eq!(rows, 0, "cross-ws set_thread_status must affect zero rows");
    let reread = store.get_comment("c1").await.expect("still readable");
    assert_eq!(
        reread.status,
        CommentStatus::Open,
        "row still open after failed cross-ws resolve"
    );

    // Owner can still resolve; row count is 1 (only the ws_a row matches).
    let rows = store
        .set_thread_status(&ws_a, "thread-x", CommentStatus::Resolved, "later")
        .await
        .expect("owner resolve");
    assert_eq!(rows, 1);
}

fn file_event(ws: &WorkspaceId, ts: &str, path: &str, actor: EventActor) -> NewEvent {
    NewEvent {
        workspace_id: ws.clone(),
        timestamp: ts.to_string(),
        event_type: events::FILE_CHANGED.to_string(),
        actor,
        session_id: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data: json!({ "path": path, "action": "modify" }),
    }
}

#[tokio::test]
async fn event_round_trip_and_queries() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    let other = WorkspaceId::new();

    let user = EventActor {
        actor_type: ActorType::User,
        id: Some("u1".to_string()),
        name: Some("Alice".to_string()),
        email: Some("alice@example.com".to_string()),
        ..Default::default()
    };
    let agent = EventActor {
        actor_type: ActorType::Agent,
        id: Some("agent-7".to_string()),
        model: Some("opus".to_string()),
        ..Default::default()
    };

    // Insert returns a UUIDv7 id and round-trips actor + data.
    let inserted = store
        .insert_event(&file_event(
            &ws,
            "2026-01-01T00:00:01Z",
            "src/a.rs",
            user.clone(),
        ))
        .await
        .expect("insert e1");
    assert_eq!(inserted.id.len(), 36, "UUIDv7 string id");
    assert_eq!(inserted.actor, user);
    assert_eq!(
        inserted.data,
        json!({ "path": "src/a.rs", "action": "modify" })
    );

    store
        .insert_event(&file_event(
            &ws,
            "2026-01-01T00:00:02Z",
            "docs/readme.md",
            agent.clone(),
        ))
        .await
        .expect("insert e2");
    store
        .insert_event(&NewEvent {
            workspace_id: ws.clone(),
            timestamp: "2026-01-01T00:00:03Z".to_string(),
            event_type: events::AGENT_TOOL_CALL.to_string(),
            actor: agent.clone(),
            session_id: Some("sess-1".to_string()),
            correlation_id: None,
            parent_event_id: None,
            metadata: None,
            data: json!({ "tool": "edit" }),
        })
        .await
        .expect("insert e3");
    // A different workspace must never leak into ws queries.
    store
        .insert_event(&file_event(
            &other,
            "2026-01-01T00:00:09Z",
            "src/z.rs",
            user,
        ))
        .await
        .expect("insert other");

    // events_by_workspace: all three ws events, newest first; no leak.
    let all = store.events_by_workspace(&ws, 10).await.expect("by ws");
    assert_eq!(all.len(), 3);
    assert_eq!(all[0].timestamp, "2026-01-01T00:00:03Z");

    // events_by_type filters on the taxonomy string.
    let edits = store
        .events_by_type(&ws, events::FILE_CHANGED, 10)
        .await
        .expect("by type");
    assert_eq!(edits.len(), 2);
    let calls = store
        .events_by_type(&ws, events::AGENT_TOOL_CALL, 10)
        .await
        .expect("by type tool-call");
    assert_eq!(calls.len(), 1);

    // path_prefix: prefix filter on data.path.
    let in_src = store
        .query_events(&EventQuery {
            workspace_id: Some(ws.clone()),
            event_types: vec![events::FILE_CHANGED.to_string()],
            path_prefix: Some("src/".to_string()),
            ..Default::default()
        })
        .await
        .expect("dir changes");
    assert_eq!(in_src.len(), 1);
    assert_eq!(in_src[0].data["path"], json!("src/a.rs"));

    // generic query: by actor type + session_id + time window.
    let by_agent = store
        .query_events(&EventQuery {
            workspace_id: Some(ws.clone()),
            actor_type: Some(ActorType::Agent),
            ..Default::default()
        })
        .await
        .expect("by actor");
    assert_eq!(by_agent.len(), 2);

    let by_session = store
        .query_events(&EventQuery {
            workspace_id: Some(ws.clone()),
            session_id: Some("sess-1".to_string()),
            ..Default::default()
        })
        .await
        .expect("by session");
    assert_eq!(by_session.len(), 1);
    assert_eq!(by_session[0].data, json!({ "tool": "edit" }));

    let windowed = store
        .query_events(&EventQuery {
            workspace_id: Some(ws.clone()),
            since: Some("2026-01-01T00:00:02Z".to_string()),
            until: Some("2026-01-01T00:00:02Z".to_string()),
            ..Default::default()
        })
        .await
        .expect("windowed");
    assert_eq!(windowed.len(), 1);
    assert_eq!(windowed[0].timestamp, "2026-01-01T00:00:02Z");

    // limit + offset paginate the newest-first stream.
    let page = store
        .query_events(&EventQuery {
            workspace_id: Some(ws.clone()),
            limit: Some(1),
            offset: Some(1),
            ..Default::default()
        })
        .await
        .expect("page");
    assert_eq!(page.len(), 1);
    assert_eq!(page[0].timestamp, "2026-01-01T00:00:02Z");
}

fn typed_event(ws: &WorkspaceId, ts: &str, event_type: &str, actor: EventActor) -> NewEvent {
    NewEvent {
        workspace_id: ws.clone(),
        timestamp: ts.to_string(),
        event_type: event_type.to_string(),
        actor,
        session_id: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data: json!({}),
    }
}

#[tokio::test]
async fn event_metadata_round_trips_through_store() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    let actor = EventActor {
        actor_type: ActorType::Agent,
        id: Some("agent-9".to_string()),
        ..Default::default()
    };

    // With metadata: persists and reads back the camelCase `metadata` object.
    let with_meta = NewEvent {
        workspace_id: ws.clone(),
        timestamp: "2026-01-01T00:00:01Z".to_string(),
        event_type: events::AGENT_MESSAGE.to_string(),
        actor: actor.clone(),
        session_id: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: Some(json!({ "source": "test", "retryCount": 2 })),
        data: json!({}),
    };
    let inserted = store.insert_event(&with_meta).await.expect("insert");
    assert_eq!(
        inserted.metadata,
        Some(json!({ "source": "test", "retryCount": 2 }))
    );

    // Without metadata: stays `None` through the round-trip (column is NULL).
    let without_meta = NewEvent {
        timestamp: "2026-01-01T00:00:02Z".to_string(),
        metadata: None,
        ..with_meta.clone()
    };
    store.insert_event(&without_meta).await.expect("insert");

    let all = store.events_by_workspace(&ws, 10).await.expect("query");
    assert_eq!(all.len(), 2);
    let newest = &all[0];
    assert_eq!(newest.metadata, None);
    let oldest = &all[1];
    assert_eq!(
        oldest.metadata,
        Some(json!({ "source": "test", "retryCount": 2 }))
    );
}

/// Regression for monorepo#670: a failed COMMIT in `insert_events` must roll
/// the transaction back so the sole write-pool connection (`max_connections=1`)
/// is not returned to the pool still holding an open transaction + write
/// lock. The `event` table has no FK on `workspace_id`, so unlike the
/// `note_version` variant this test plants a trap: a trigger on `event`
/// inserts into a table whose FK is `DEFERRABLE INITIALLY DEFERRED`, so the
/// violation only surfaces at COMMIT time, forcing the COMMIT itself to
/// fail. Pre-fix code propagated the error without ROLLBACK, poisoning the
/// connection so every later `BEGIN IMMEDIATE` failed.
#[tokio::test]
async fn insert_events_rolls_back_on_failed_commit() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws, "WS", false))
        .await
        .expect("insert ws");
    let actor = EventActor {
        actor_type: ActorType::Agent,
        id: Some("agent-1".to_string()),
        ..Default::default()
    };

    // Plant the COMMIT trap: every event insert also inserts a row whose
    // deferred FK points at a nonexistent workspace, so the violation is
    // only detected when `insert_events` issues its COMMIT.
    sqlx::query(
        "CREATE TABLE commit_trap (
             event_id TEXT PRIMARY KEY,
             ws_ref   TEXT NOT NULL REFERENCES workspace(id)
                      DEFERRABLE INITIALLY DEFERRED
         )",
    )
    .execute(store.write_pool())
    .await
    .expect("create trap table");
    sqlx::query(
        "CREATE TRIGGER commit_trap_trigger AFTER INSERT ON event BEGIN
             INSERT INTO commit_trap (event_id, ws_ref)
             VALUES (NEW.id, 'ghost-workspace');
         END",
    )
    .execute(store.write_pool())
    .await
    .expect("create trap trigger");

    let trapped = typed_event(
        &ws,
        "2026-01-01T00:00:01Z",
        events::AGENT_MESSAGE,
        actor.clone(),
    );
    let err = store
        .insert_events(std::slice::from_ref(&trapped))
        .await
        .expect_err("COMMIT must fail on the deferred FK violation");
    assert!(
        err.to_string().contains("commit"),
        "unexpected error: {err}"
    );

    // Nothing persisted for the trapped insert...
    assert!(store
        .events_by_workspace(&ws, 10)
        .await
        .expect("query events")
        .is_empty());
    // ...and the failed COMMIT was rolled back, not left open: with the
    // trap disarmed, the next insert reuses the same pooled connection and
    // commits normally.
    sqlx::query("DROP TRIGGER commit_trap_trigger")
        .execute(store.write_pool())
        .await
        .expect("drop trap trigger");
    let ok_event = typed_event(&ws, "2026-01-01T00:00:02Z", events::AGENT_MESSAGE, actor);
    let inserted = store
        .insert_events(std::slice::from_ref(&ok_event))
        .await
        .expect("insert after failed COMMIT");
    assert_eq!(inserted.len(), 1);
}

/// A failed statement inside `insert_events`' transaction rolls the whole
/// batch back and leaves the pooled write connection usable (monorepo#669,
/// style of #453's `append_note_version_rolls_back_on_body_error`): an
/// `AFTER INSERT` trigger raising ABORT fails the multi-row INSERT at
/// statement time (not COMMIT time), nothing persists, and a subsequent
/// batch on the same pooled connection succeeds.
#[tokio::test]
async fn insert_events_rolls_back_on_body_error() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws, "WS", false))
        .await
        .expect("insert ws");
    let actor = EventActor {
        actor_type: ActorType::Agent,
        id: Some("agent-1".to_string()),
        ..Default::default()
    };

    // Arm the body trap: any event insert aborts its own statement.
    sqlx::query(
        "CREATE TRIGGER body_trap AFTER INSERT ON event BEGIN
             SELECT RAISE(ABORT, 'body trap');
         END",
    )
    .execute(store.write_pool())
    .await
    .expect("create trap trigger");

    let events = vec![
        typed_event(
            &ws,
            "2026-01-01T00:00:01Z",
            events::AGENT_MESSAGE,
            actor.clone(),
        ),
        typed_event(
            &ws,
            "2026-01-01T00:00:02Z",
            events::AGENT_MESSAGE,
            actor.clone(),
        ),
    ];
    let err = store
        .insert_events(&events)
        .await
        .expect_err("INSERT must fail on the abort trigger");
    assert!(
        err.to_string().contains("insert events failed"),
        "unexpected error: {err}"
    );

    // Nothing persisted from the failed batch...
    assert!(store
        .events_by_workspace(&ws, 10)
        .await
        .expect("query events")
        .is_empty());
    // ...and the write connection is clean: with the trap disarmed, the same
    // batch succeeds on the same pooled connection.
    sqlx::query("DROP TRIGGER body_trap")
        .execute(store.write_pool())
        .await
        .expect("drop trap trigger");
    let inserted = store
        .insert_events(&events)
        .await
        .expect("insert after body error");
    assert_eq!(inserted.len(), 2);
}

/// Regression for monorepo#680 at the `insert_events` site: unlike the
/// `RAISE(ABORT)` body trap above (statement fails, transaction stays open,
/// explicit ROLLBACK succeeds), a `RAISE(ROLLBACK)` trigger fails the body
/// *and* auto-rolls the transaction back, so the explicit ROLLBACK fails
/// and the hardened arm must detach+close the connection (write pool size
/// drops to 0) instead of pooling the potentially poisoned handle.
#[tokio::test]
async fn insert_events_detaches_conn_on_failed_body_error_rollback() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws, "WS", false))
        .await
        .expect("insert ws");
    let actor = EventActor {
        actor_type: ActorType::Agent,
        id: Some("agent-1".to_string()),
        ..Default::default()
    };

    // Arm the trap: the event INSERT fails its own statement and rolls the
    // whole transaction back, so the explicit ROLLBACK finds none open.
    sqlx::query(
        "CREATE TRIGGER rollback_trap AFTER INSERT ON event BEGIN
             SELECT RAISE(ROLLBACK, 'rollback trap');
         END",
    )
    .execute(store.write_pool())
    .await
    .expect("create trap trigger");

    let trapped = typed_event(
        &ws,
        "2026-01-01T00:00:01Z",
        events::AGENT_MESSAGE,
        actor.clone(),
    );
    let err = store
        .insert_events(std::slice::from_ref(&trapped))
        .await
        .expect_err("INSERT must fail on the rollback trigger");
    assert!(
        err.to_string().contains("insert events failed"),
        "unexpected error: {err}"
    );

    // The failed ROLLBACK detached+closed the connection rather than
    // returning it to the pool.
    assert_eq!(store.write_pool().size(), 0);

    // Nothing persisted from the trapped insert...
    assert!(store
        .events_by_workspace(&ws, 10)
        .await
        .expect("query events")
        .is_empty());
    // ...and the pool opens a fresh replacement on demand: with the trap
    // disarmed, the next insert succeeds.
    sqlx::query("DROP TRIGGER rollback_trap")
        .execute(store.write_pool())
        .await
        .expect("drop trap trigger");
    let inserted = store
        .insert_events(std::slice::from_ref(&trapped))
        .await
        .expect("insert after detach");
    assert_eq!(inserted.len(), 1);
}

/// Finding F4: extended ephemeral-event retention sweep deletes high-volume
/// families (`agent:stream:*`, `file:*`, `terminal:data`, `host:exec:*`,
/// `script:output`) older than the cutoff while preserving lifecycle/tool/
/// note/task/workspace events regardless of age. The sweep is idempotent and
/// the legacy `delete_stream_events_before` alias still works.
#[tokio::test]
async fn ephemeral_event_retention_sweep_extended_families() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    let agent = EventActor {
        actor_type: ActorType::Agent,
        id: Some("agent-1".to_string()),
        ..Default::default()
    };

    // Mixed families across ages. Old = 2026-01-01, new = 2026-06-01.
    let old = "2026-01-01T00:00:00Z";
    let new = "2026-06-01T00:00:00Z";
    let seed = vec![
        // Old ephemeral families (should be deleted by the sweep).
        typed_event(&ws, old, events::AGENT_STREAM_START, agent.clone()),
        typed_event(&ws, old, events::AGENT_STREAM_ACTIVITY, agent.clone()),
        typed_event(&ws, old, events::AGENT_STREAM_END, agent.clone()),
        typed_event(&ws, old, events::FILE_CHANGED, agent.clone()),
        typed_event(&ws, old, events::FILE_CREATED, agent.clone()),
        typed_event(&ws, old, events::FILE_DELETED, agent.clone()),
        typed_event(&ws, old, events::TERMINAL_DATA, agent.clone()),
        typed_event(&ws, old, events::HOST_EXEC_STDOUT, agent.clone()),
        typed_event(&ws, old, events::HOST_EXEC_STDERR, agent.clone()),
        typed_event(&ws, old, events::HOST_EXEC_EXIT, agent.clone()),
        typed_event(&ws, old, events::SCRIPT_OUTPUT, agent.clone()),
        // New ephemeral events (within TTL — must survive).
        typed_event(&ws, new, events::AGENT_STREAM_ACTIVITY, agent.clone()),
        typed_event(&ws, new, events::FILE_CHANGED, agent.clone()),
        typed_event(&ws, new, events::TERMINAL_DATA, agent.clone()),
        typed_event(&ws, new, events::HOST_EXEC_STDOUT, agent.clone()),
        typed_event(&ws, new, events::SCRIPT_OUTPUT, agent.clone()),
        // Old non-ephemeral families (must NEVER be deleted regardless of age).
        typed_event(&ws, old, events::AGENT_STARTED, agent.clone()),
        typed_event(&ws, old, events::AGENT_TOOL_CALL, agent.clone()),
        typed_event(&ws, old, events::NOTE_UPDATED, agent.clone()),
        typed_event(&ws, old, events::TASK_STATUS_CHANGED, agent.clone()),
        typed_event(&ws, old, events::TERMINAL_EXIT, agent.clone()), // not terminal:data
        typed_event(&ws, old, events::GIT_COMMIT, agent.clone()),
        typed_event(&ws, old, events::SCRIPT_STATE, agent.clone()), // not script:output
    ];
    for ev in &seed {
        store.insert_event(ev).await.expect("insert seed event");
    }

    // Cutoff between old and new: old ephemeral families are eligible.
    let cutoff = "2026-03-01T00:00:00Z";
    let removed = store
        .delete_ephemeral_events_before(cutoff)
        .await
        .expect("sweep");
    assert_eq!(
        removed, 11,
        "11 old ephemeral events removed (3 stream + 3 file + 1 terminal + 3 host:exec + 1 script:output)"
    );

    let remaining = store
        .events_by_workspace(&ws, 100)
        .await
        .expect("remaining");
    assert_eq!(
        remaining.len(),
        12,
        "5 new ephemeral + 7 preserved families"
    );

    // New ephemeral events survive.
    for t in [
        events::AGENT_STREAM_ACTIVITY,
        events::FILE_CHANGED,
        events::TERMINAL_DATA,
        events::HOST_EXEC_STDOUT,
        events::SCRIPT_OUTPUT,
    ] {
        assert!(
            remaining
                .iter()
                .any(|e| e.event_type == t && e.timestamp == new),
            "new ephemeral {t} missing"
        );
    }

    // Non-ephemeral families survive, including old ones.
    for t in [
        events::AGENT_STARTED,
        events::AGENT_TOOL_CALL,
        events::NOTE_UPDATED,
        events::TASK_STATUS_CHANGED,
        events::TERMINAL_EXIT,
        events::GIT_COMMIT,
        events::SCRIPT_STATE,
    ] {
        assert!(
            remaining.iter().any(|e| e.event_type == t),
            "preserved family {t} missing"
        );
    }

    // Explicit script:* assertions (monorepo#620): script:output is exact,
    // not a prefix, so its lifecycle sibling script:state must be preserved.
    // Stated directly rather than left to the count-based checks above so a
    // future regression on either side is unambiguous.
    assert!(
        remaining
            .iter()
            .any(|e| e.event_type == events::SCRIPT_STATE && e.timestamp == old),
        "script:state (old) must survive the ephemeral sweep"
    );
    assert!(
        !remaining
            .iter()
            .any(|e| e.event_type == events::SCRIPT_OUTPUT && e.timestamp == old),
        "old script:output must be pruned by the ephemeral sweep"
    );
    assert!(
        remaining
            .iter()
            .any(|e| e.event_type == events::SCRIPT_OUTPUT && e.timestamp == new),
        "new script:output (within TTL) must survive the ephemeral sweep"
    );

    // Idempotent: a re-run with the same cutoff removes nothing more.
    let removed_again = store
        .delete_ephemeral_events_before(cutoff)
        .await
        .expect("sweep re-run");
    assert_eq!(removed_again, 0);

    // Legacy alias still works (for backward compat during transition).
    let removed_via_alias = store
        .delete_stream_events_before(cutoff)
        .await
        .expect("legacy alias");
    assert_eq!(removed_via_alias, 0, "idempotent via alias too");
}

/// Spec P3: the high-churn state-notification families (`workspace:updated`,
/// `draft:changed`, `agent:status-changed`, `agent:idle`,
/// `agent:subscriptions-changed`, `settings:changed`,
/// `workspace:tokenUsage-changed`, `agent:queue:updated`) are swept on the
/// same cutoff as the other ephemeral families — every consumer takes them
/// from the live bus, nothing reads them back from the persisted log. Their
/// lifecycle siblings (`workspace:created`, `agent:created`,
/// `agent:completed`, `agent:queue:processing`, ...) are audit history and
/// must survive regardless of age (exact-type scoping, no prefix bleed).
#[tokio::test]
async fn state_notification_retention_sweep_churn_families() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    let agent = EventActor {
        actor_type: ActorType::Agent,
        id: Some("agent-1".to_string()),
        ..Default::default()
    };

    let old = "2026-01-01T00:00:00Z";
    let new = "2026-06-01T00:00:00Z";
    let churn = [
        events::WORKSPACE_UPDATED,
        events::DRAFT_CHANGED,
        events::AGENT_STATUS_CHANGED,
        events::AGENT_IDLE,
        events::AGENT_SUBSCRIPTIONS_CHANGED,
        events::SETTINGS_CHANGED,
        events::WORKSPACE_TOKEN_USAGE_CHANGED,
        events::AGENT_QUEUE_UPDATED,
    ];
    let preserved = [
        events::WORKSPACE_CREATED,
        events::WORKSPACE_DELETED,
        events::AGENT_CREATED,
        events::AGENT_COMPLETED,
        events::AGENT_QUEUE_PROCESSING, // sibling of agent:queue:updated
        events::TASK_STATUS_CHANGED,
    ];
    let mut seed = Vec::new();
    for t in churn {
        // Old rows are eligible; in-window rows must survive.
        seed.push(typed_event(&ws, old, t, agent.clone()));
        seed.push(typed_event(&ws, new, t, agent.clone()));
    }
    for t in preserved {
        seed.push(typed_event(&ws, old, t, agent.clone()));
    }
    for ev in &seed {
        store.insert_event(ev).await.expect("insert seed event");
    }

    let cutoff = "2026-03-01T00:00:00Z";
    let removed = store
        .delete_ephemeral_events_before(cutoff)
        .await
        .expect("sweep");
    assert_eq!(
        removed,
        churn.len() as u64,
        "exactly the old churn-family rows are removed"
    );

    let remaining = store
        .events_by_workspace(&ws, 100)
        .await
        .expect("remaining");
    assert_eq!(remaining.len(), churn.len() + preserved.len());
    for t in churn {
        assert!(
            !remaining
                .iter()
                .any(|e| e.event_type == t && e.timestamp == old),
            "old {t} must be pruned"
        );
        assert!(
            remaining
                .iter()
                .any(|e| e.event_type == t && e.timestamp == new),
            "in-window {t} must survive"
        );
    }
    for t in preserved {
        assert!(
            remaining
                .iter()
                .any(|e| e.event_type == t && e.timestamp == old),
            "lifecycle family {t} must survive regardless of age"
        );
    }

    // Idempotent: a re-run with the same cutoff removes nothing more.
    let removed_again = store
        .delete_ephemeral_events_before(cutoff)
        .await
        .expect("sweep re-run");
    assert_eq!(removed_again, 0);
}

/// Earlier test retained for coverage of the legacy behavior (stream-only sweep);
/// the new `ephemeral_event_retention_sweep_extended_families` above covers the
/// extended scope (finding F4).
#[tokio::test]
async fn stream_retention_sweep_trims_only_old_stream_events() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    let agent = EventActor {
        actor_type: ActorType::Agent,
        id: Some("agent-1".to_string()),
        ..Default::default()
    };

    // Mixed families across ages. Old = 2026-01-01, new = 2026-06-01.
    let old = "2026-01-01T00:00:00Z";
    let new = "2026-06-01T00:00:00Z";
    let seed = vec![
        // Old stream chunks (should be deleted by the sweep).
        typed_event(&ws, old, events::AGENT_STREAM_START, agent.clone()),
        typed_event(&ws, old, events::AGENT_STREAM_ACTIVITY, agent.clone()),
        typed_event(&ws, old, events::AGENT_STREAM_END, agent.clone()),
        // New stream event (within TTL — must survive).
        typed_event(&ws, new, events::AGENT_STREAM_ACTIVITY, agent.clone()),
        // Old non-stream families (must NEVER be deleted regardless of age).
        typed_event(&ws, old, events::AGENT_STARTED, agent.clone()),
        typed_event(&ws, old, events::AGENT_TOOL_CALL, agent.clone()),
        typed_event(&ws, old, events::FILE_CHANGED, agent.clone()),
        typed_event(&ws, old, events::NOTE_UPDATED, agent.clone()),
        typed_event(&ws, old, events::TASK_STATUS_CHANGED, agent.clone()),
    ];
    for ev in &seed {
        store.insert_event(ev).await.expect("insert seed event");
    }

    // Cutoff between old and new: only old stream chunks are eligible.
    let cutoff = "2026-03-01T00:00:00Z";
    let removed = store
        .delete_stream_events_before(cutoff)
        .await
        .expect("sweep");
    // NOTE: this test predates the finding F4 extension; after the extension
    // landed, `delete_stream_events_before` became an alias for
    // `delete_ephemeral_events_before`, so the old FILE_CHANGED event is now
    // also removed. The assertion below reflects the new behavior (4 removed:
    // 3 stream + 1 file). The legacy name is preserved for backward compat.
    assert_eq!(removed, 4, "extended sweep includes file:* (finding F4)");

    let remaining = store
        .events_by_workspace(&ws, 100)
        .await
        .expect("remaining");
    assert_eq!(remaining.len(), 5);
    // The surviving stream event is the new one; no old stream events remain.
    let stream_types: Vec<&str> = remaining
        .iter()
        .filter(|e| e.event_type.starts_with(events::AGENT_STREAM_PREFIX))
        .map(|e| e.timestamp.as_str())
        .collect();
    assert_eq!(stream_types, vec![new]);
    // Every non-ephemeral family survives, including the old ones (file:* is
    // now ephemeral so the old FILE_CHANGED was removed).
    for t in [
        events::AGENT_STARTED,
        events::AGENT_TOOL_CALL,
        events::NOTE_UPDATED,
        events::TASK_STATUS_CHANGED,
    ] {
        assert!(
            remaining.iter().any(|e| e.event_type == t),
            "preserved family {t} missing"
        );
    }

    // Idempotent: a re-run with the same cutoff removes nothing more.
    let removed_again = store
        .delete_stream_events_before(cutoff)
        .await
        .expect("sweep re-run");
    assert_eq!(removed_again, 0);
}

/// Finding F4 (fsync half): `connect()` sets `PRAGMA synchronous = NORMAL`
/// (safe under WAL) to cut fsync load on high-write workloads. This test
/// asserts that a fresh pool has `synchronous = NORMAL` (2 in `SQLite`'s integer
/// encoding: 0=OFF, 1=NORMAL, 2=FULL).
#[tokio::test]
async fn connect_sets_synchronous_normal_under_wal() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");

    // Query the PRAGMA value. SQLite returns the integer code: 0=OFF, 1=NORMAL, 2=FULL.
    let row: (i64,) = sqlx::query_as("PRAGMA synchronous")
        .fetch_one(store.read_pool())
        .await
        .expect("query pragma");
    assert_eq!(row.0, 1, "synchronous should be NORMAL (1) under WAL");

    // Verify WAL mode is also set (journal_mode).
    let jm: (String,) = sqlx::query_as("PRAGMA journal_mode")
        .fetch_one(store.read_pool())
        .await
        .expect("query journal_mode");
    assert_eq!(jm.0.to_lowercase(), "wal", "journal_mode should be WAL");
}

#[tokio::test]
async fn stream_retention_sweep_disabled_is_noop_in_practice() {
    // The daemon disables the sweep at TTL=0 by never calling it; at the store
    // layer a cutoff older than every row is a safe no-op (nothing eligible).
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    let agent = EventActor {
        actor_type: ActorType::Agent,
        id: Some("agent-1".to_string()),
        ..Default::default()
    };
    store
        .insert_event(&typed_event(
            &ws,
            "2026-06-01T00:00:00Z",
            events::AGENT_STREAM_ACTIVITY,
            agent,
        ))
        .await
        .expect("insert");

    let removed = store
        .delete_stream_events_before("2020-01-01T00:00:00Z")
        .await
        .expect("sweep");
    assert_eq!(removed, 0);
    assert_eq!(store.events_by_workspace(&ws, 10).await.unwrap().len(), 1);
}

/// `agent:tool:call` has its own TTL sweep (`delete_tool_call_events_before`):
/// tool calls older than the cutoff are removed, newer ones are retained, and
/// no other family is touched. The ephemeral sweep continues to leave tool
/// calls alone regardless of age.
#[tokio::test]
async fn tool_call_retention_sweep_trims_only_old_tool_calls() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    let agent = EventActor {
        actor_type: ActorType::Agent,
        id: Some("agent-1".to_string()),
        ..Default::default()
    };

    let old = "2026-01-01T00:00:00Z";
    let new = "2026-06-01T00:00:00Z";
    let seed = vec![
        // Old tool calls (eligible for the tool-call TTL sweep).
        typed_event(&ws, old, events::AGENT_TOOL_CALL, agent.clone()),
        typed_event(&ws, old, events::AGENT_TOOL_CALL, agent.clone()),
        // New tool call (within TTL — must survive).
        typed_event(&ws, new, events::AGENT_TOOL_CALL, agent.clone()),
        // Other old families — the tool-call sweep must not touch them.
        typed_event(&ws, old, events::AGENT_STARTED, agent.clone()),
        typed_event(&ws, old, events::NOTE_UPDATED, agent.clone()),
        typed_event(&ws, old, events::FILE_CHANGED, agent.clone()),
    ];
    for ev in &seed {
        store.insert_event(ev).await.expect("insert seed event");
    }

    let cutoff = "2026-03-01T00:00:00Z";
    let removed = store
        .delete_tool_call_events_before(cutoff)
        .await
        .expect("tool-call sweep");
    assert_eq!(removed, 2, "only the 2 old tool calls are removed");

    let remaining = store
        .events_by_workspace(&ws, 100)
        .await
        .expect("remaining");
    assert_eq!(remaining.len(), 4);
    assert!(
        remaining
            .iter()
            .any(|e| e.event_type == events::AGENT_TOOL_CALL && e.timestamp == new),
        "new tool call must survive"
    );
    for t in [
        events::AGENT_STARTED,
        events::NOTE_UPDATED,
        events::FILE_CHANGED,
    ] {
        assert!(
            remaining.iter().any(|e| e.event_type == t),
            "other family {t} must be untouched"
        );
    }

    // Idempotent: a re-run with the same cutoff removes nothing more.
    let removed_again = store
        .delete_tool_call_events_before(cutoff)
        .await
        .expect("re-run");
    assert_eq!(removed_again, 0);

    // The ephemeral sweep still never touches tool calls, even with a cutoff
    // newer than every row (it removes only the old FILE_CHANGED here).
    let ephemeral_removed = store
        .delete_ephemeral_events_before("2027-01-01T00:00:00Z")
        .await
        .expect("ephemeral sweep");
    assert_eq!(ephemeral_removed, 1, "only FILE_CHANGED is ephemeral here");
    let after = store.events_by_workspace(&ws, 100).await.expect("after");
    assert!(
        after
            .iter()
            .any(|e| e.event_type == events::AGENT_TOOL_CALL && e.timestamp == new),
        "tool call survives the ephemeral sweep"
    );
}

/// Chunked deletion completes: seeding more old tool-call rows than
/// `RETENTION_DELETE_CHUNK` still sweeps them all in a single call (the
/// chunk loop keeps going until a short chunk), and newer rows survive.
#[tokio::test]
async fn retention_sweep_chunked_deletion_completes() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    let agent = EventActor {
        actor_type: ActorType::Agent,
        id: Some("agent-1".to_string()),
        ..Default::default()
    };

    let old = "2026-01-01T00:00:00Z";
    let new = "2026-06-01T00:00:00Z";
    let total_old = usize::try_from(crate::event_repo::RETENTION_DELETE_CHUNK)
        .expect("value fits in usize")
        * 2
        + 50;
    let mut seed: Vec<NewEvent> = (0..total_old)
        .map(|_| typed_event(&ws, old, events::AGENT_TOOL_CALL, agent.clone()))
        .collect();
    seed.push(typed_event(
        &ws,
        new,
        events::AGENT_TOOL_CALL,
        agent.clone(),
    ));
    for batch in seed.chunks(500) {
        store.insert_events(batch).await.expect("insert batch");
    }

    let removed = store
        .delete_tool_call_events_before("2026-03-01T00:00:00Z")
        .await
        .expect("chunked sweep");
    assert_eq!(removed, total_old as u64, "all old chunks swept");

    let remaining = store
        .events_by_type(&ws, events::AGENT_TOOL_CALL, 10)
        .await
        .expect("remaining");
    assert_eq!(remaining.len(), 1, "only the new tool call survives");
    assert_eq!(remaining[0].timestamp, new);
}

/// The retention DELETEs must be index-driven (no full table scan): the inner
/// row-selection of both the prefix-family and exact-type shapes uses the
/// composite `idx_event_type_time` index (migration 0051).
#[tokio::test]
async fn retention_delete_query_plan_uses_index() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");

    let prefix_plan: Vec<(i64, i64, i64, String)> = sqlx::query_as(
        "EXPLAIN QUERY PLAN DELETE FROM event WHERE rowid IN (
            SELECT rowid FROM event
            WHERE event_type >= ? AND event_type < ? AND timestamp < ?
            LIMIT ?
        )",
    )
    .bind("agent:stream:")
    .bind("agent:stream;")
    .bind("2026-01-01T00:00:00Z")
    .bind(1000_i64)
    .fetch_all(store.read_pool())
    .await
    .expect("explain prefix delete");

    let exact_plan: Vec<(i64, i64, i64, String)> = sqlx::query_as(
        "EXPLAIN QUERY PLAN DELETE FROM event WHERE rowid IN (
            SELECT rowid FROM event
            WHERE event_type = ? AND timestamp < ?
            LIMIT ?
        )",
    )
    .bind("agent:tool:call")
    .bind("2026-01-01T00:00:00Z")
    .bind(1000_i64)
    .fetch_all(store.read_pool())
    .await
    .expect("explain exact delete");

    for (name, plan) in [("prefix", &prefix_plan), ("exact", &exact_plan)] {
        let details: Vec<&str> = plan.iter().map(|r| r.3.as_str()).collect();
        assert!(
            details.iter().any(|d| d.contains("idx_event_type_time")),
            "{name} plan should use idx_event_type_time: {details:?}"
        );
        assert!(
            !details.iter().any(|d| d.trim() == "SCAN event"),
            "{name} plan must not full-scan the event table: {details:?}"
        );
    }
}

/// Disk-space reclamation: a freshly created database is in
/// `auto_vacuum = INCREMENTAL` mode, pages emptied by retention deletes land
/// on the freelist, and bounded `Store::incremental_vacuum` calls actually
/// release them (freelist shrinks, logical `page_count` drops).
#[tokio::test]
async fn incremental_vacuum_releases_freelist_pages() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    let agent = EventActor {
        actor_type: ActorType::Agent,
        id: Some("agent-1".to_string()),
        ..Default::default()
    };

    // New databases must be created in incremental auto-vacuum mode (2).
    let auto_vacuum: i64 = sqlx::query("PRAGMA auto_vacuum")
        .fetch_one(store.write_pool())
        .await
        .expect("query auto_vacuum")
        .get(0);
    assert_eq!(auto_vacuum, 2, "new DB should have auto_vacuum=INCREMENTAL");

    // Seed enough bulky ephemeral events to allocate a meaningful number of
    // pages (~4KB payload each, several hundred rows).
    let payload = "x".repeat(4096);
    let seed: Vec<NewEvent> = (0..300)
        .map(|_| NewEvent {
            workspace_id: ws.clone(),
            timestamp: "2026-01-01T00:00:00Z".to_string(),
            event_type: events::TERMINAL_DATA.to_string(),
            actor: agent.clone(),
            session_id: None,
            correlation_id: None,
            parent_event_id: None,
            metadata: None,
            data: json!({ "chunk": payload }),
        })
        .collect();
    for batch in seed.chunks(100) {
        store.insert_events(batch).await.expect("insert batch");
    }

    let pages_before: i64 = sqlx::query("PRAGMA page_count")
        .fetch_one(store.write_pool())
        .await
        .expect("page_count before")
        .get(0);

    // Retention sweep deletes every seeded row; the emptied pages go to the
    // freelist instead of being returned to the filesystem.
    let removed = store
        .delete_ephemeral_events_before("2027-01-01T00:00:00Z")
        .await
        .expect("sweep");
    assert_eq!(removed, 300);
    let freelist_after_delete = store.freelist_count().await.expect("freelist");
    assert!(
        freelist_after_delete > 0,
        "deletes should leave pages on the freelist (got {freelist_after_delete})"
    );

    // A bounded call frees at most `max_pages` per invocation.
    let freed_bounded = store.incremental_vacuum(8).await.expect("bounded vacuum");
    assert!(
        freed_bounded > 0 && freed_bounded <= 8,
        "bounded incremental_vacuum should free 1..=8 pages (got {freed_bounded})"
    );

    // Draining the rest empties the freelist and shrinks the logical DB size.
    let freed_rest = store
        .incremental_vacuum(1_000_000)
        .await
        .expect("drain vacuum");
    assert_eq!(
        freed_bounded + freed_rest,
        freelist_after_delete.cast_unsigned(),
        "all freelist pages should be released"
    );
    assert_eq!(store.freelist_count().await.expect("freelist"), 0);

    let pages_after: i64 = sqlx::query("PRAGMA page_count")
        .fetch_one(store.write_pool())
        .await
        .expect("page_count after")
        .get(0);
    assert!(
        pages_after < pages_before,
        "page_count should shrink after vacuum ({pages_before} -> {pages_after})"
    );
}

/// One-time activation (monorepo#720 finding 1): a legacy database created
/// without `auto_vacuum` stays in NONE mode when reopened through `Store::open`
/// (the connect pragma is recorded but inert), so
/// `Store::activate_incremental_vacuum` must run a VACUUM that converts it to
/// incremental mode, shrinks the file, and makes subsequent bounded
/// `incremental_vacuum` calls effective. A second activation is a no-op.
#[tokio::test]
async fn activate_incremental_vacuum_converts_legacy_none_db() {
    let tmp = TempDb::new();

    // Build the legacy database with a raw connection that does NOT apply the
    // auto_vacuum pragma (SQLite defaults to NONE), then seed + delete bulky
    // rows so the freelist is non-empty and the file is bloated.
    {
        let opts = sqlx::sqlite::SqliteConnectOptions::new()
            .filename(&tmp.path)
            .create_if_missing(true);
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(opts)
            .await
            .expect("open legacy pool");
        sqlx::query("CREATE TABLE filler (id INTEGER PRIMARY KEY, data BLOB)")
            .execute(&pool)
            .await
            .expect("create filler");
        for i in 0..64i64 {
            sqlx::query("INSERT INTO filler (id, data) VALUES (?, zeroblob(65536))")
                .bind(i)
                .execute(&pool)
                .await
                .expect("insert filler");
        }
        sqlx::query("DELETE FROM filler")
            .execute(&pool)
            .await
            .expect("delete filler");
        let mode: i64 = sqlx::query("PRAGMA auto_vacuum")
            .fetch_one(&pool)
            .await
            .expect("query auto_vacuum")
            .get(0);
        assert_eq!(mode, 0, "legacy DB should be created in NONE mode");
        pool.close().await;
    }

    let store = Store::open(&tmp.path).await.expect("open store");
    // The connect pragma alone does not convert an existing NONE database.
    let mode: i64 = sqlx::query("PRAGMA auto_vacuum")
        .fetch_one(store.write_pool())
        .await
        .expect("query auto_vacuum")
        .get(0);
    assert_eq!(mode, 0, "reopened legacy DB should still report NONE");
    assert!(
        store.freelist_count().await.expect("freelist") > 0,
        "seed deletes should leave pages on the freelist"
    );

    // Activation runs the one-time VACUUM: incremental mode on, file shrunk.
    match store
        .activate_incremental_vacuum()
        .await
        .expect("activation")
    {
        AutoVacuumActivation::Activated {
            pages_before,
            pages_after,
            ..
        } => assert!(
            pages_after < pages_before,
            "VACUUM should shrink the file ({pages_before} -> {pages_after})"
        ),
        AutoVacuumActivation::AlreadyIncremental => {
            panic!("first activation on a NONE DB should run VACUUM")
        }
    }
    let mode: i64 = sqlx::query("PRAGMA auto_vacuum")
        .fetch_one(store.write_pool())
        .await
        .expect("query auto_vacuum")
        .get(0);
    assert_eq!(mode, 2, "activation should leave auto_vacuum=INCREMENTAL");

    // Post-activation churn proves incremental_vacuum is now effective:
    // deletes land on the freelist and the bounded call releases them.
    sqlx::query("CREATE TABLE churn (id INTEGER PRIMARY KEY, data BLOB)")
        .execute(store.write_pool())
        .await
        .expect("create churn");
    for i in 0..16i64 {
        sqlx::query("INSERT INTO churn (id, data) VALUES (?, zeroblob(65536))")
            .bind(i)
            .execute(store.write_pool())
            .await
            .expect("insert churn");
    }
    sqlx::query("DELETE FROM churn")
        .execute(store.write_pool())
        .await
        .expect("delete churn");
    assert!(
        store.freelist_count().await.expect("freelist") > 0,
        "churn deletes should leave pages on the freelist"
    );
    let freed = store
        .incremental_vacuum(1_000_000)
        .await
        .expect("incremental vacuum");
    assert!(
        freed > 0,
        "incremental_vacuum should release pages after activation (got {freed})"
    );

    // Second activation is a no-op on the now-incremental database.
    assert_eq!(
        store
            .activate_incremental_vacuum()
            .await
            .expect("second activation"),
        AutoVacuumActivation::AlreadyIncremental
    );
}

fn sample_agent_session(id: &AgentId, ws: &WorkspaceId) -> AgentSession {
    let ts = now_iso();
    AgentSession {
        harness_version: intent_core::CURRENT_HARNESS_VERSION.to_string(),
        harness_features: None,
        id: id.clone(),
        workspace_id: ws.clone(),
        parent_agent_id: None,
        backend_session_id: None,
        acp_session_id: None,
        name: "Builder".to_string(),
        name_explicitly_set: false,
        model: Some("opus".to_string()),
        reasoning_effort: None,
        effort_levels: None,
        provider: None,
        system_prompt: Some("be helpful".to_string()),
        specialist: None,
        status: AgentStatus::Pending,
        is_active: false,
        messages: Vec::new(),
        stats: None,
        task_note_id: None,
        skip_auto_commit: false,
        completion_report: None,
        completion_report_timestamp: None,
        attention_request_kind: None,
        attention_request_reason: None,
        attention_request_timestamp: None,
        delegation_depth: None,
        initial_message: None,
        context_references: None,
        image_blocks: None,
        file_blocks: None,
        is_background: false,
        metadata: None,
        stop_reason: None,
        stop_reason_timestamp: None,
        session_corrupted: false,
        pending_delete_at: None,
        created_at: ts.clone(),
        updated_at: ts,
        sandbox_id: None,
        sandbox_path: None,
        sandbox_branch: None,
    }
}
/// The `IS NULL` guard's race contract at the store level: the first
/// materialization wins, and a second call with a DIFFERENT snapshot (the
/// lost-race path — caller's in-memory session still NULL while the DB row
/// was stamped concurrently) leaves the row untouched and returns the
/// winner's snapshot. `updated_at` stays untouched throughout.
#[tokio::test]
async fn materialize_harness_features_first_write_wins() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws, "WS", false))
        .await
        .expect("insert ws");
    let agent_id = AgentId::from("agent-aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");
    let session = sample_agent_session(&agent_id, &ws);
    let original_updated_at = session.updated_at.clone();
    store
        .insert_agent_session(&session)
        .await
        .expect("insert session");

    let winner = serde_json::json!({"taskGraph": true});
    let loser = serde_json::json!({"taskGraph": false});
    let first = store
        .materialize_agent_session_harness_features(&agent_id, &winner)
        .await
        .expect("first materialization");
    assert_eq!(first, Some(winner.clone()), "first write persists");
    let second = store
        .materialize_agent_session_harness_features(&agent_id, &loser)
        .await
        .expect("second materialization");
    assert_eq!(
        second,
        Some(winner),
        "lost race adopts the winner's snapshot, not the loser's"
    );
    let row = store
        .get_agent_session(&agent_id)
        .await
        .expect("get session");
    assert_eq!(
        row.updated_at, original_updated_at,
        "materialization must not touch updated_at"
    );
}

#[tokio::test]
async fn agent_session_round_trip_and_append_only_log() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws, "WS", false))
        .await
        .expect("insert ws");

    let agent_id = AgentId::from("agent-aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");
    store
        .insert_agent_session(&sample_agent_session(&agent_id, &ws))
        .await
        .expect("insert session");

    // Append-only log: seq is monotonic per agent, starting at 0.
    let m0 = store
        .append_agent_message(
            &agent_id,
            "user",
            &json!([{ "type": "text", "text": "hi" }]),
            "t0",
        )
        .await
        .expect("append m0");
    let m1 = store
        .append_agent_message(
            &agent_id,
            "assistant",
            &json!([{ "type": "text", "text": "yo" }]),
            "t1",
        )
        .await
        .expect("append m1");
    assert_eq!(m0.seq, 0);
    assert_eq!(m1.seq, 1);

    // Round-trip the session with its full message log (chronological order).
    let loaded = store.get_agent_session(&agent_id).await.expect("get");
    assert_eq!(loaded.name, "Builder");
    assert_eq!(loaded.model.as_deref(), Some("opus"));
    assert_eq!(loaded.status, AgentStatus::Pending);
    assert_eq!(loaded.messages.len(), 2);
    assert_eq!(loaded.messages[0].seq, 0);
    assert_eq!(loaded.messages[0].role, "user");
    assert_eq!(
        loaded.messages[0].content,
        json!([{ "type": "text", "text": "hi" }])
    );
    assert_eq!(loaded.messages[1].seq, 1);

    // getConversation cap: most-recent N, still oldest→newest.
    assert_eq!(
        store.count_agent_messages(&agent_id).await.expect("count"),
        2
    );
    let recent = store
        .get_agent_messages(&agent_id, Some(1))
        .await
        .expect("recent");
    assert_eq!(recent.len(), 1);
    assert_eq!(recent[0].seq, 1);

    let listed = store.list_agent_sessions(&ws).await.expect("list");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].id, agent_id);
    assert_eq!(listed[0].messages.len(), 2);
}

/// Attention-request fields (`attention_request_kind` / `..._reason` /
/// `..._timestamp`) round-trip through insert → `set_attention_request` →
/// get, and `clear_attention_request` clears them exactly once: `true` when a
/// request was pending, `false` on the no-op repeat, `NotFound` for a missing
/// session or workspace mismatch.
#[tokio::test]
async fn agent_session_attention_request_round_trip_and_clear() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws, "WS", false))
        .await
        .expect("insert ws");

    let agent_id = AgentId::from("agent-eeeeeeee-1111-2222-3333-555555555555");
    store
        .insert_agent_session(&sample_agent_session(&agent_id, &ws))
        .await
        .expect("insert session");

    // No request pending: the clear is a no-op returning false.
    assert!(!store
        .clear_attention_request(&ws, &agent_id, "t-clear-0")
        .await
        .expect("noop clear"));

    // Persist a pending request via the narrow attention writer.
    store
        .set_attention_request(&ws, &agent_id, "blocker", "sandbox is broken", "t-attn")
        .await
        .expect("set attention request");

    let loaded = store.get_agent_session(&agent_id).await.expect("reload");
    assert_eq!(loaded.attention_request_kind.as_deref(), Some("blocker"));
    assert_eq!(
        loaded.attention_request_reason.as_deref(),
        Some("sandbox is broken")
    );
    assert_eq!(
        loaded.attention_request_timestamp.as_deref(),
        Some("t-attn")
    );

    // Workspace mismatch → NotFound, and the pending request survives.
    let other_ws = WorkspaceId::new();
    assert!(matches!(
        store
            .clear_attention_request(&other_ws, &agent_id, "t-clear-x")
            .await,
        Err(Error::NotFound(_))
    ));

    // Present → cleared (true), fields NULLed, updated_at refreshed.
    assert!(store
        .clear_attention_request(&ws, &agent_id, "t-clear-1")
        .await
        .expect("clear"));
    let cleared = store.get_agent_session(&agent_id).await.expect("cleared");
    assert_eq!(cleared.attention_request_kind, None);
    assert_eq!(cleared.attention_request_reason, None);
    assert_eq!(cleared.attention_request_timestamp, None);
    assert_eq!(cleared.updated_at, "t-clear-1");

    // Repeat is the no-op false again; a missing session is NotFound.
    assert!(!store
        .clear_attention_request(&ws, &agent_id, "t-clear-2")
        .await
        .expect("noop clear 2"));
    assert!(matches!(
        store
            .clear_attention_request(&ws, &AgentId::from("agent-missing"), "t")
            .await,
        Err(Error::NotFound(_))
    ));

    // Narrow-writer NotFound parity: missing session and workspace mismatch.
    assert!(matches!(
        store
            .set_attention_request(&ws, &AgentId::from("agent-missing"), "blocker", "r", "t")
            .await,
        Err(Error::NotFound(_))
    ));
    assert!(matches!(
        store
            .set_attention_request(&WorkspaceId::new(), &agent_id, "blocker", "r", "t")
            .await,
        Err(Error::NotFound(_))
    ));
}

/// G9 regression (attention-clobber race): the full-row
/// `update_agent_session` must NOT write the `attention_request_*` columns.
/// A long-lived in-memory `AgentSession` persisted mid-race can therefore
/// neither resurrect a request that `clear_attention_request` already `NULLed`
/// nor clobber a fresh one written by `set_attention_request` in the interim.
#[tokio::test]
async fn full_row_update_never_touches_attention_request_columns() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws, "WS", false))
        .await
        .expect("insert ws");
    let agent_id = AgentId::from("agent-eeeeeeee-1111-2222-3333-666666666666");
    store
        .insert_agent_session(&sample_agent_session(&agent_id, &ws))
        .await
        .expect("insert session");

    // Race A: a stale session snapshot carrying a pending request must not
    // resurrect it after a narrow clear.
    store
        .set_attention_request(&ws, &agent_id, "blocker", "sandbox broken", "t-attn")
        .await
        .expect("set attention request");
    let stale = store.get_agent_session(&agent_id).await.expect("snapshot");
    assert_eq!(stale.attention_request_kind.as_deref(), Some("blocker"));
    assert!(store
        .clear_attention_request(&ws, &agent_id, "t-clear")
        .await
        .expect("clear"));
    store
        .update_agent_session(&ws, &stale)
        .await
        .expect("racing full-row persist");
    let after = store.get_agent_session(&agent_id).await.expect("reload");
    assert_eq!(
        after.attention_request_kind, None,
        "full-row persist of a stale session must not resurrect a cleared request"
    );
    assert_eq!(after.attention_request_reason, None);
    assert_eq!(after.attention_request_timestamp, None);

    // Race B: a stale request-free snapshot must not clobber a fresh request.
    let stale = store.get_agent_session(&agent_id).await.expect("snapshot");
    store
        .set_attention_request(&ws, &agent_id, "discussion", "need a decision", "t-attn-2")
        .await
        .expect("set attention request");
    store
        .update_agent_session(&ws, &stale)
        .await
        .expect("racing full-row persist");
    let after = store.get_agent_session(&agent_id).await.expect("reload");
    assert_eq!(
        after.attention_request_kind.as_deref(),
        Some("discussion"),
        "full-row persist of a stale session must not clobber a fresh request"
    );
    assert_eq!(
        after.attention_request_reason.as_deref(),
        Some("need a decision")
    );
    assert_eq!(
        after.attention_request_timestamp.as_deref(),
        Some("t-attn-2")
    );
}

/// `insert_agent_session_with_messages` persists the session and its whole
/// transcript in one transaction: on success everything lands with 0-based
/// monotonic seq, and on failure (duplicate session id) NOTHING lands — no
/// session row and no message rows (the legacy importer's retry-safety
/// contract).
#[tokio::test]
async fn agent_session_with_messages_is_atomic() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws, "WS", false))
        .await
        .expect("insert ws");

    let agent_id = AgentId::from("agent-cccccccc-1111-2222-3333-444444444444");
    let metadata = json!({ "source": "legacy" });
    let contents = [
        json!([{ "type": "text", "text": "hi" }]),
        json!([{ "type": "text", "text": "yo" }]),
    ];
    let rows = vec![
        crate::ReplaceMessage {
            role: "user",
            content: &contents[0],
            metadata: Some(&metadata),
            created_at: "t0",
        },
        crate::ReplaceMessage {
            role: "assistant",
            content: &contents[1],
            metadata: None,
            created_at: "t1",
        },
    ];
    store
        .insert_agent_session_with_messages(&sample_agent_session(&agent_id, &ws), &rows)
        .await
        .expect("insert with messages");

    let loaded = store.get_agent_session(&agent_id).await.expect("get");
    assert_eq!(loaded.messages.len(), 2);
    assert_eq!(loaded.messages[0].seq, 0);
    assert_eq!(loaded.messages[0].role, "user");
    assert_eq!(loaded.messages[0].metadata.as_ref(), Some(&metadata));
    assert_eq!(loaded.messages[1].seq, 1);
    assert!(loaded.messages[1].metadata.is_none());

    // Re-inserting the same id fails wholesale: the original transcript is
    // untouched and no extra rows landed.
    let err = store
        .insert_agent_session_with_messages(&sample_agent_session(&agent_id, &ws), &rows)
        .await;
    assert!(err.is_err());
    assert_eq!(
        store.count_agent_messages(&agent_id).await.expect("count"),
        2
    );
}

/// `get_agent_session_status` is the lightweight status-only accessor backing
/// the STAB-52 queue-drain gate: it returns the persisted status without
/// loading the message log, tracks `set_agent_session_status` updates, and
/// mirrors `get_agent_session`'s `NotFound` for missing rows.
#[tokio::test]
async fn agent_session_status_only_lookup() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws, "WS", false))
        .await
        .expect("insert ws");
    let agent_id = AgentId::from("agent-aaaaaaaa-5555-6666-7777-888888888888");
    store
        .insert_agent_session(&sample_agent_session(&agent_id, &ws))
        .await
        .expect("insert session");

    let status = store
        .get_agent_session_status(&agent_id)
        .await
        .expect("status");
    assert_eq!(status, AgentStatus::Pending);

    store
        .set_agent_session_status(&ws, &agent_id, AgentStatus::Error, false, "t1", None)
        .await
        .expect("set status");
    let status = store
        .get_agent_session_status(&agent_id)
        .await
        .expect("status after update");
    assert_eq!(status, AgentStatus::Error);

    let missing = AgentId::from("agent-ffffffff-0000-0000-0000-000000000000");
    assert!(matches!(
        store.get_agent_session_status(&missing).await,
        Err(intent_core::Error::NotFound(_))
    ));
}

/// `set_agent_session_resolved_model` (D13/D14) guard: the write lands only
/// while `model` still equals `expected_model` (`None` matches NULL) — a
/// mismatch (concurrent `agent.setModel`) returns `false` and leaves
/// `resolved_model` untouched.
/// `clear_agent_session_resolved_model` is idempotent (already-NULL column
/// and absent row are both no-ops).
#[tokio::test]
async fn agent_session_resolved_model_guard_and_clear() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws, "WS", false))
        .await
        .expect("insert ws");
    let agent_id = AgentId::from("agent-aaaaaaaa-1111-2222-3333-555555555555");
    let mut session = sample_agent_session(&agent_id, &ws);
    session.model = Some("claude-code:claude-fable-5[1m]".to_string());
    store
        .insert_agent_session(&session)
        .await
        .expect("insert session");

    // Guard failure: expected_model no longer matches → false, no write.
    let landed = store
        .set_agent_session_resolved_model(
            &ws,
            &agent_id,
            Some("claude-code:sonnet"),
            Some("Sonnet 5"),
        )
        .await
        .expect("guarded write");
    assert!(!landed, "mismatched expected_model must not land");
    let (_, resolved, _, _) = store
        .get_agent_session_token_usage(&ws, &agent_id)
        .await
        .expect("read");
    assert_eq!(resolved, None, "resolved_model untouched on guard failure");

    // Guard failure: expecting NULL against a non-NULL model → false.
    let landed = store
        .set_agent_session_resolved_model(&ws, &agent_id, None, Some("Fable 5"))
        .await
        .expect("guarded write");
    assert!(!landed, "None expected_model must not match a set model");

    // Guard success: matching expected_model lands the resolution.
    let landed = store
        .set_agent_session_resolved_model(
            &ws,
            &agent_id,
            Some("claude-code:claude-fable-5[1m]"),
            Some("Fable 5"),
        )
        .await
        .expect("guarded write");
    assert!(landed);
    let (_, resolved, _, _) = store
        .get_agent_session_token_usage(&ws, &agent_id)
        .await
        .expect("read");
    assert_eq!(resolved.as_deref(), Some("Fable 5"));

    // A None resolution overwrites (clears) via the same guarded write.
    let landed = store
        .set_agent_session_resolved_model(
            &ws,
            &agent_id,
            Some("claude-code:claude-fable-5[1m]"),
            None,
        )
        .await
        .expect("guarded clear");
    assert!(landed);
    let (_, resolved, _, _) = store
        .get_agent_session_token_usage(&ws, &agent_id)
        .await
        .expect("read");
    assert_eq!(resolved, None);

    // clear is idempotent: already-NULL column and absent row are no-ops.
    store
        .clear_agent_session_resolved_model(&ws, &agent_id)
        .await
        .expect("clear on already-NULL column");
    let missing = AgentId::from("agent-ffffffff-0000-0000-0000-000000000001");
    store
        .clear_agent_session_resolved_model(&ws, &missing)
        .await
        .expect("clear on absent row is a no-op");

    // D13: a NULL-model session is guarded with `None` expected_model.
    let null_model_id = AgentId::from("agent-aaaaaaaa-1111-2222-3333-666666666666");
    let mut null_session = sample_agent_session(&null_model_id, &ws);
    null_session.model = None;
    store
        .insert_agent_session(&null_session)
        .await
        .expect("insert NULL-model session");
    let landed = store
        .set_agent_session_resolved_model(&ws, &null_model_id, None, Some("Opus 4.8"))
        .await
        .expect("guarded write on NULL model");
    assert!(landed, "None expected_model matches NULL");
    let (model, resolved, _, _) = store
        .get_agent_session_token_usage(&ws, &null_model_id)
        .await
        .expect("read");
    assert_eq!(model, None, "model stays NULL");
    assert_eq!(resolved.as_deref(), Some("Opus 4.8"));
}

/// `set_agent_session_status` `stop_reason` parameter: `None` leaves the column
/// untouched; `Some(None)` clears it to NULL; `Some(Some(reason))` sets the
/// new value. Exercises the three-way encoding for set/clear/leave-unchanged
/// across a status update. `stop_reason_timestamp` is coupled: set → stamped
/// with the update's `updated_at`, clear → NULL, unchanged → untouched.
#[tokio::test]
async fn agent_session_stop_reason_set_clear_unchanged() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws, "WS", false))
        .await
        .expect("insert ws");
    let agent_id = AgentId::from("agent-aaaaaaaa-1111-2222-3333-444444444444");
    let mut session = sample_agent_session(&agent_id, &ws);
    session.stop_reason = None;
    store
        .insert_agent_session(&session)
        .await
        .expect("insert session");

    // Verify initial state: stop_reason is None.
    let loaded = store
        .get_agent_session(&agent_id)
        .await
        .expect("get session");
    assert_eq!(loaded.stop_reason, None);
    assert_eq!(loaded.stop_reason_timestamp, None);

    // Set a stop reason via set_agent_session_status with Some(Some("error")).
    store
        .set_agent_session_status(
            &ws,
            &agent_id,
            AgentStatus::Error,
            false,
            "t1",
            Some(Some("error".to_string())),
        )
        .await
        .expect("set status with stop_reason");
    let loaded = store
        .get_agent_session(&agent_id)
        .await
        .expect("get after set");
    assert_eq!(loaded.stop_reason, Some("error".to_string()));
    assert_eq!(loaded.stop_reason_timestamp, Some("t1".to_string()));
    assert_eq!(loaded.status, AgentStatus::Error);

    // Leave stop_reason untouched (pass None) when updating status.
    store
        .set_agent_session_status(&ws, &agent_id, AgentStatus::RuntimeIdle, false, "t2", None)
        .await
        .expect("set status without touching stop_reason");
    let loaded = store
        .get_agent_session(&agent_id)
        .await
        .expect("get after unchanged");
    assert_eq!(loaded.stop_reason, Some("error".to_string()));
    assert_eq!(loaded.stop_reason_timestamp, Some("t1".to_string()));
    assert_eq!(loaded.status, AgentStatus::RuntimeIdle);

    // Clear stop_reason via Some(None).
    store
        .set_agent_session_status(
            &ws,
            &agent_id,
            AgentStatus::Pending,
            false,
            "t3",
            Some(None),
        )
        .await
        .expect("clear stop_reason");
    let loaded = store
        .get_agent_session(&agent_id)
        .await
        .expect("get after clear");
    assert_eq!(loaded.stop_reason, None);
    assert_eq!(loaded.stop_reason_timestamp, None);
    assert_eq!(loaded.status, AgentStatus::Pending);

    // update_agent_session also persists stop_reason + stop_reason_timestamp.
    let mut updated = loaded.clone();
    updated.stop_reason = Some("max_turns".to_string());
    updated.stop_reason_timestamp = Some("t4".to_string());
    updated.status = AgentStatus::Completed;
    store
        .update_agent_session(&ws, &updated)
        .await
        .expect("update with stop_reason");
    let loaded = store
        .get_agent_session(&agent_id)
        .await
        .expect("get after update");
    assert_eq!(loaded.stop_reason, Some("max_turns".to_string()));
    assert_eq!(loaded.stop_reason_timestamp, Some("t4".to_string()));
    assert_eq!(loaded.status, AgentStatus::Completed);
}

/// `append_agent_message_with_metadata` persists the opaque per-message
/// `messageMetadata` payload (PROTOCOL §5.5) verbatim on the row and
/// round-trips it on transcript reads; the plain `append_agent_message`
/// path continues to store `NULL` for messages without metadata.
#[tokio::test]
async fn agent_message_metadata_round_trip() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws, "WS", false))
        .await
        .expect("insert ws");
    let agent_id = AgentId::from("agent-aaaaaaaa-1111-2222-3333-444444444444");
    store
        .insert_agent_session(&sample_agent_session(&agent_id, &ws))
        .await
        .expect("insert session");

    let metadata = json!({ "source": "system", "tag": "restart" });
    let stored = store
        .append_agent_message_with_metadata(
            &agent_id,
            "user",
            &json!([{ "type": "text", "text": "hi" }]),
            Some(&metadata),
            "t0",
        )
        .await
        .expect("append with metadata");
    assert_eq!(stored.metadata.as_ref(), Some(&metadata));

    // Plain append leaves metadata as NULL.
    let plain = store
        .append_agent_message(
            &agent_id,
            "assistant",
            &json!([{ "type": "text", "text": "yo" }]),
            "t1",
        )
        .await
        .expect("append plain");
    assert!(
        plain.metadata.is_none(),
        "plain append persists NULL metadata"
    );

    // Both survive the read path in order.
    let messages = store
        .get_agent_messages(&agent_id, None)
        .await
        .expect("read messages");
    assert_eq!(messages.len(), 2);
    assert_eq!(messages[0].metadata.as_ref(), Some(&metadata));
    assert!(messages[1].metadata.is_none());

    // The metadata payload also round-trips through the get_agent_session
    // aggregate loader (transcript embedded on the session).
    let session = store.get_agent_session(&agent_id).await.expect("get");
    assert_eq!(session.messages.len(), 2);
    assert_eq!(session.messages[0].metadata.as_ref(), Some(&metadata));
    assert!(session.messages[1].metadata.is_none());
}

/// Attachment-registry rows round-trip (insert → get), an unknown id is
/// `NotFound`, and the optional `mime_type` persists as NULL when absent
/// (PROTOCOL §5.9).
#[tokio::test]
async fn attachment_registry_round_trip() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();

    let record = crate::AttachmentRecord {
        id: "0193e001-0000-7000-8000-000000000001".to_string(),
        workspace_id: ws.clone(),
        file_name: "report.pdf".to_string(),
        mime_type: Some("application/pdf".to_string()),
        size: 12345,
        uploaded_at: "2026-08-12T00:00:00Z".to_string(),
        stored_path: ".intent/attachments/report.pdf".to_string(),
    };
    store.insert_attachment(&record).await.expect("insert");
    let loaded = store.get_attachment(&record.id).await.expect("get");
    assert_eq!(loaded, record);

    // Absent mime_type stays None.
    let no_mime = crate::AttachmentRecord {
        id: "0193e001-0000-7000-8000-000000000002".to_string(),
        mime_type: None,
        ..record.clone()
    };
    store.insert_attachment(&no_mime).await.expect("insert 2");
    let loaded2 = store.get_attachment(&no_mime.id).await.expect("get 2");
    assert_eq!(loaded2.mime_type, None);

    // Unknown id → NotFound.
    let missing = store.get_attachment("no-such-id").await;
    assert!(
        matches!(missing, Err(intent_core::Error::NotFound(_))),
        "{missing:?}"
    );
}

/// The P3-1.2b persistence-gap fields round-trip through insert → get →
/// update → get: `completion_report(_timestamp)`, `delegation_depth`,
/// `initial_message`, the JSON `context_references` / `image_blocks`, and
/// `is_background` (G-A1/P3-1.2c).
#[tokio::test]
async fn agent_session_gap_fields_round_trip() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws, "WS", false))
        .await
        .expect("insert ws");

    let agent_id = AgentId::from("agent-aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");
    let mut session = sample_agent_session(&agent_id, &ws);
    session.delegation_depth = Some(2);
    session.initial_message = Some("kick off".to_string());
    session.context_references = Some(json!([{ "type": "file", "path": "src/a.rs" }]));
    session.image_blocks = Some(json!([{ "type": "image", "data": "abc" }]));
    session.file_blocks =
        Some(json!([{ "type": "file", "attachmentId": "att-1", "fileName": "r.pdf" }]));
    session.is_background = true;
    store
        .insert_agent_session(&session)
        .await
        .expect("insert session");

    let loaded = store.get_agent_session(&agent_id).await.expect("get");
    assert_eq!(loaded.delegation_depth, Some(2));
    assert!(loaded.is_background, "is_background must round-trip");
    assert_eq!(loaded.initial_message.as_deref(), Some("kick off"));
    assert_eq!(
        loaded.context_references,
        Some(json!([{ "type": "file", "path": "src/a.rs" }]))
    );
    assert_eq!(
        loaded.image_blocks,
        Some(json!([{ "type": "image", "data": "abc" }]))
    );
    assert_eq!(
        loaded.file_blocks,
        Some(json!([{ "type": "file", "attachmentId": "att-1", "fileName": "r.pdf" }]))
    );
    assert_eq!(loaded.completion_report, None);

    // The report fields land via the update path.
    let mut updated = loaded.clone();
    updated.completion_report = Some("done".to_string());
    updated.completion_report_timestamp = Some("t9".to_string());
    store
        .update_agent_session(&ws, &updated)
        .await
        .expect("update");
    let reloaded = store.get_agent_session(&agent_id).await.expect("reload");
    assert_eq!(reloaded.completion_report.as_deref(), Some("done"));
    assert_eq!(reloaded.completion_report_timestamp.as_deref(), Some("t9"));
    // The spawn-time fields survive the update untouched.
    assert_eq!(reloaded.delegation_depth, Some(2));
    assert_eq!(reloaded.initial_message.as_deref(), Some("kick off"));
    assert!(reloaded.is_background, "is_background survives update");
}

/// Transient streaming flags are NEVER persisted (P3-1.2b; the daemon-side
/// mirror of the FE `performAtomicWrite` scrub): the `agent_session` schema
/// has no column for them, and the persisted session's wire form carries no
/// `isResponding` / `isStreaming` / `isProcessing` / `currentStreamId` keys —
/// those exist only on the runtime-overlaid `AgentLite` projection.
#[tokio::test]
async fn transient_streaming_flags_are_never_persisted() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws, "WS", false))
        .await
        .expect("insert ws");

    // Schema guard: no transient-flag column exists on agent_session.
    let cols: Vec<String> = sqlx::query("PRAGMA table_info(agent_session)")
        .fetch_all(store.read_pool())
        .await
        .expect("pragma")
        .iter()
        .map(|r| sqlx::Row::get::<String, _>(r, "name"))
        .collect();
    for forbidden in [
        "is_responding",
        "is_streaming",
        "is_processing",
        "current_stream_id",
    ] {
        assert!(
            !cols.iter().any(|c| c == forbidden),
            "agent_session must not have a `{forbidden}` column"
        );
    }

    // Wire guard: a persisted-and-reloaded session serializes without any
    // transient streaming keys.
    let agent_id = AgentId::from("agent-aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");
    store
        .insert_agent_session(&sample_agent_session(&agent_id, &ws))
        .await
        .expect("insert session");
    let loaded = store.get_agent_session(&agent_id).await.expect("get");
    let v = serde_json::to_value(&loaded).expect("session json");
    let obj = v.as_object().expect("object");
    for forbidden in [
        "isResponding",
        "isStreaming",
        "isProcessing",
        "currentStreamId",
    ] {
        assert!(
            !obj.contains_key(forbidden),
            "persisted AgentSession must not carry `{forbidden}`"
        );
    }
}

#[tokio::test]
async fn agent_session_parent_agent_id_round_trips() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws, "WS", false))
        .await
        .expect("insert ws");

    // Default: no parent linkage persists as None.
    let orphan = AgentId::from("agent-11111111-2222-3333-4444-555555555555");
    store
        .insert_agent_session(&sample_agent_session(&orphan, &ws))
        .await
        .expect("insert orphan");
    assert_eq!(
        store
            .get_agent_session(&orphan)
            .await
            .expect("get")
            .parent_agent_id,
        None
    );

    // Inserted parent linkage round-trips on get and list.
    let parent = AgentId::from("agent-aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");
    let child = AgentId::from("agent-bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb");
    let mut session = sample_agent_session(&child, &ws);
    session.parent_agent_id = Some(parent.clone());
    store
        .insert_agent_session(&session)
        .await
        .expect("insert child");
    assert_eq!(
        store
            .get_agent_session(&child)
            .await
            .expect("get")
            .parent_agent_id,
        Some(parent.clone())
    );

    // Update clears the linkage back to None.
    let mut cleared = store.get_agent_session(&child).await.expect("get");
    cleared.parent_agent_id = None;
    store
        .update_agent_session(&ws, &cleared)
        .await
        .expect("update");
    assert_eq!(
        store
            .get_agent_session(&child)
            .await
            .expect("get")
            .parent_agent_id,
        None
    );
}

#[tokio::test]
async fn agent_session_specialist_round_trips() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws, "WS", false))
        .await
        .expect("insert ws");

    // Default: no specialist persists as None.
    let plain = AgentId::from("agent-cccccccc-cccc-cccc-cccc-cccccccccccc");
    store
        .insert_agent_session(&sample_agent_session(&plain, &ws))
        .await
        .expect("insert plain");
    assert_eq!(
        store
            .get_agent_session(&plain)
            .await
            .expect("get")
            .specialist,
        None
    );

    // Inserted specialist round-trips on get and survives an update.
    let spec_agent = AgentId::from("agent-dddddddd-dddd-dddd-dddd-dddddddddddd");
    let mut session = sample_agent_session(&spec_agent, &ws);
    session.specialist = Some("implementor".to_string());
    store
        .insert_agent_session(&session)
        .await
        .expect("insert specialist");
    assert_eq!(
        store
            .get_agent_session(&spec_agent)
            .await
            .expect("get")
            .specialist,
        Some("implementor".to_string())
    );

    let mut updated = store.get_agent_session(&spec_agent).await.expect("get");
    updated.name = "Renamed".to_string();
    store
        .update_agent_session(&ws, &updated)
        .await
        .expect("update");
    assert_eq!(
        store
            .get_agent_session(&spec_agent)
            .await
            .expect("get")
            .specialist,
        Some("implementor".to_string())
    );
}

#[tokio::test]
async fn agent_acp_session_id_is_write_once() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws, "WS", false))
        .await
        .expect("insert ws");
    let agent_id = AgentId::new();
    store
        .insert_agent_session(&sample_agent_session(&agent_id, &ws))
        .await
        .expect("insert session");

    // First write succeeds; re-setting the same value is idempotent.
    store
        .set_acp_session_id(&ws, &agent_id, "acp-1")
        .await
        .expect("first set");
    store
        .set_acp_session_id(&ws, &agent_id, "acp-1")
        .await
        .expect("idempotent set");
    // Changing it to a different value is rejected.
    assert!(store
        .set_acp_session_id(&ws, &agent_id, "acp-2")
        .await
        .is_err());

    // update_agent_session also refuses to overwrite a set acpSessionId.
    let mut s = store.get_agent_session(&agent_id).await.expect("get");
    s.acp_session_id = Some("acp-3".to_string());
    assert!(store.update_agent_session(&ws, &s).await.is_err());
}

#[tokio::test]
async fn replace_acp_session_id_is_no_clobber_cas() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws, "WS", false))
        .await
        .expect("insert ws");
    let agent_id = AgentId::new();
    store
        .insert_agent_session(&sample_agent_session(&agent_id, &ws))
        .await
        .expect("insert session");

    store
        .set_acp_session_id(&ws, &agent_id, "acp-1")
        .await
        .expect("first set");

    // CAS no-clobber: a stale expected-old does NOT overwrite the canonical id;
    // the stored value is returned for the caller to reuse.
    let kept = store
        .replace_acp_session_id(&ws, &agent_id, "wrong-old", "acp-2")
        .await
        .expect("cas returns canonical");
    assert_eq!(kept, "acp-1", "diverged expected-old reuses the stored id");
    let stored = store.get_agent_session(&agent_id).await.expect("get");
    assert_eq!(stored.acp_session_id.as_deref(), Some("acp-1"));

    // CAS swap: a matching expected-old replaces with the fresh id (the
    // resume-impossible fallback, where `set_acp_session_id` would reject).
    let swapped = store
        .replace_acp_session_id(&ws, &agent_id, "acp-1", "acp-2")
        .await
        .expect("cas swaps on match");
    assert_eq!(swapped, "acp-2");
    let stored = store.get_agent_session(&agent_id).await.expect("get");
    assert_eq!(stored.acp_session_id.as_deref(), Some("acp-2"));

    // A missing session surfaces NotFound rather than a silent no-op.
    assert!(store
        .replace_acp_session_id(&ws, &AgentId::new(), "acp-2", "acp-x")
        .await
        .is_err());
}

#[tokio::test]
async fn agent_provider_is_immutable_once_set() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws, "WS", false))
        .await
        .expect("insert ws");
    let agent_id = AgentId::new();
    store
        .insert_agent_session(&sample_agent_session(&agent_id, &ws))
        .await
        .expect("insert session");

    // Provider starts unset, so first set (via update) is allowed.
    let mut s = store.get_agent_session(&agent_id).await.expect("get");
    s.provider = Some("auggie".to_string());
    s.status = AgentStatus::Active;
    store
        .update_agent_session(&ws, &s)
        .await
        .expect("set provider");

    // Provider can still be changed before first real use (before acp_session_id is set).
    let mut s = store.get_agent_session(&agent_id).await.expect("get");
    s.provider = Some("opencode".to_string());
    store
        .update_agent_session(&ws, &s)
        .await
        .expect("change provider before first use");

    // Once acp_session_id is set (first real use), provider becomes immutable.
    store
        .set_acp_session_id(&ws, &agent_id, "acp-1")
        .await
        .expect("set acp session id");

    // Now changing the provider is rejected.
    let mut s = store.get_agent_session(&agent_id).await.expect("get");
    s.provider = Some("claude-code".to_string());
    assert!(store.update_agent_session(&ws, &s).await.is_err());
}

#[tokio::test]
async fn agent_provider_immutable_after_acp_session_even_from_none() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws, "WS", false))
        .await
        .expect("insert ws");
    let agent_id = AgentId::new();
    store
        .insert_agent_session(&sample_agent_session(&agent_id, &ws))
        .await
        .expect("insert session");

    // Provider starts None, and we set acp_session_id while provider is still None.
    store
        .set_acp_session_id(&ws, &agent_id, "acp-1")
        .await
        .expect("set acp session id");

    // Now trying to set provider from None→Some should be rejected.
    let mut s = store.get_agent_session(&agent_id).await.expect("get");
    assert_eq!(s.provider, None);
    s.provider = Some("auggie".to_string());
    assert!(store.update_agent_session(&ws, &s).await.is_err());
}

#[tokio::test]
async fn agent_provider_change_rejected_when_setting_acp_session_id() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws, "WS", false))
        .await
        .expect("insert ws");
    let agent_id = AgentId::new();
    store
        .insert_agent_session(&sample_agent_session(&agent_id, &ws))
        .await
        .expect("insert session");

    // Set provider first
    let mut s = store.get_agent_session(&agent_id).await.expect("get");
    s.provider = Some("auggie".to_string());
    store
        .update_agent_session(&ws, &s)
        .await
        .expect("set provider");

    // Trying to change provider in the same update that sets acp_session_id should be rejected.
    let mut s = store.get_agent_session(&agent_id).await.expect("get");
    s.provider = Some("opencode".to_string());
    s.acp_session_id = Some("acp-1".to_string());
    assert!(store.update_agent_session(&ws, &s).await.is_err());
}

#[tokio::test]
async fn client_upsert_sets_first_seen_once_and_touches_last_seen() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let id = ClientId::from_string("cli-abc");

    store
        .upsert_client(&id, Some("Laptop"), Some(&json!({ "forward": true })))
        .await
        .expect("insert client");
    let first = store.get_client(&id).await.expect("get").expect("present");
    assert_eq!(first.name, Some("Laptop".to_string()));
    assert_eq!(first.capabilities, json!({ "forward": true }));

    // Re-hello updates name/capabilities and touches last_seen; first_seen stays.
    store
        .upsert_client(&id, Some("Desktop"), Some(&json!({ "forward": false })))
        .await
        .expect("re-upsert");
    let again = store.get_client(&id).await.expect("get").expect("present");
    assert_eq!(again.name, Some("Desktop".to_string()));
    assert_eq!(again.capabilities, json!({ "forward": false }));
    assert_eq!(
        again.first_seen, first.first_seen,
        "first_seen is preserved"
    );
    assert!(store
        .get_client(&ClientId::from_string("missing"))
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn draft_round_trip_upsert_get_delete() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws, "WS", false))
        .await
        .expect("insert ws");
    let client = ClientId::from_string("cli-1");
    store
        .upsert_client(&client, None, None)
        .await
        .expect("client");
    let agent = AgentId::from_string("agent-1");

    assert!(store
        .get_draft(&ws, &agent, &client)
        .await
        .unwrap()
        .is_none());
    store
        .upsert_draft(&ws, &agent, &client, "hello", None)
        .await
        .expect("set");
    let got = store
        .get_draft(&ws, &agent, &client)
        .await
        .unwrap()
        .expect("present");
    assert_eq!(got.text, "hello");
    assert_eq!(
        got.attachments, None,
        "a text-only draft has no attachments"
    );

    // Upsert overwrites text and attachments in place.
    let attachments = json!([{ "type": "image", "imageData": "aGk=" }]);
    store
        .upsert_draft(&ws, &agent, &client, "world", Some(&attachments))
        .await
        .expect("set2");
    let got = store
        .get_draft(&ws, &agent, &client)
        .await
        .unwrap()
        .expect("present");
    assert_eq!(got.text, "world");
    assert_eq!(
        got.attachments,
        Some(attachments),
        "attachments round-trip verbatim"
    );

    // An upsert without attachments drops the stored ones.
    store
        .upsert_draft(&ws, &agent, &client, "world", None)
        .await
        .expect("set3");
    assert_eq!(
        store
            .get_draft(&ws, &agent, &client)
            .await
            .unwrap()
            .unwrap()
            .attachments,
        None
    );

    assert!(
        store.delete_draft(&ws, &agent, &client).await.unwrap(),
        "row removed"
    );
    assert!(
        !store.delete_draft(&ws, &agent, &client).await.unwrap(),
        "idempotent no-op"
    );
    assert!(store
        .get_draft(&ws, &agent, &client)
        .await
        .unwrap()
        .is_none());
}

/// Regression (PROTOCOL §5.16 "Opaque keys & reserved sentinels"): draft keys
/// are opaque — the daemon never validates `workspace_id` against live
/// workspaces, so a `drafts.set` under the FE's `__new-workspace__` /
/// `__initializer__` sentinel pair (no workspace row exists yet) must succeed
/// and round-trip.
#[tokio::test]
async fn draft_round_trip_for_workspace_id_without_row() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let client = ClientId::from_string("cli-1");
    store
        .upsert_client(&client, None, None)
        .await
        .expect("client");
    let ws = WorkspaceId::from("__new-workspace__");
    let agent = AgentId::from_string("__initializer__");

    store
        .upsert_draft(&ws, &agent, &client, "pre-create draft", None)
        .await
        .expect("drafts.set succeeds for a workspaceId with no workspace row");
    let got = store
        .get_draft(&ws, &agent, &client)
        .await
        .unwrap()
        .expect("present");
    assert_eq!(got.text, "pre-create draft");

    assert!(
        store.delete_draft(&ws, &agent, &client).await.unwrap(),
        "clear removes the sentinel draft"
    );
    assert!(store
        .get_draft(&ws, &agent, &client)
        .await
        .unwrap()
        .is_none());
}

/// The 0050 rebuild (drop the draft→workspace FK) upgrades the pre-0050
/// schema in place without losing rows — text, attachments, and the
/// NULL-attachments shape all survive the create-new → copy → drop → rename
/// cycle, and the workspace FK is actually gone afterwards. The embedded
/// migrator has already run against an empty DB by the time we can insert
/// rows, so the old 0007+0048 table shape (workspace FK included) is rebuilt
/// by hand before replaying the migration SQL.
#[tokio::test]
async fn draft_fk_drop_migration_preserves_existing_rows() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws, "WS", false))
        .await
        .expect("insert ws");
    let client = ClientId::from_string("cli-1");
    store.upsert_client(&client, None, None).await.unwrap();

    // Restore the pre-0050 shape: 0007 columns + workspace FK, with the 0048
    // `attachments` column appended.
    sqlx::raw_sql(
        "DROP TABLE draft;
         CREATE TABLE draft (
           workspace_id TEXT NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
           agent_id     TEXT NOT NULL,
           client_id    TEXT NOT NULL REFERENCES client(id) ON DELETE CASCADE,
           text         TEXT NOT NULL,
           updated_at   TEXT NOT NULL,
           attachments  TEXT,
           PRIMARY KEY (workspace_id, agent_id, client_id)
         );
         CREATE INDEX idx_draft_client ON draft(client_id);",
    )
    .execute(store.write_pool())
    .await
    .expect("rebuild pre-0050 shape");

    let agent = AgentId::from_string("agent-1");
    let attachments = json!([{ "type": "image", "imageData": "aGk=" }]);
    store
        .upsert_draft(&ws, &agent, &client, "keep me", Some(&attachments))
        .await
        .unwrap();
    let plain_agent = AgentId::from_string("agent-2");
    store
        .upsert_draft(&ws, &plain_agent, &client, "no attachments", None)
        .await
        .unwrap();
    let sentinel_ws = WorkspaceId::from("__new-workspace__");
    let sentinel_agent = AgentId::from_string("__initializer__");
    store
        .upsert_draft(&sentinel_ws, &sentinel_agent, &client, "rejected", None)
        .await
        .expect_err("pre-0050 FK rejects the sentinel workspaceId");

    sqlx::raw_sql(include_str!(
        "../migrations/0050_draft_drop_workspace_fk.sql"
    ))
    .execute(store.write_pool())
    .await
    .expect("run rebuild against the old shape");

    // The FK is really gone: the sentinel write now succeeds.
    store
        .upsert_draft(&sentinel_ws, &sentinel_agent, &client, "accepted", None)
        .await
        .expect("post-0050 sentinel write succeeds");

    let got = store
        .get_draft(&ws, &agent, &client)
        .await
        .unwrap()
        .expect("row survives the rebuild");
    assert_eq!(got.text, "keep me");
    assert_eq!(got.attachments, Some(attachments));
    let got = store
        .get_draft(&ws, &plain_agent, &client)
        .await
        .unwrap()
        .expect("attachment-less row survives too");
    assert_eq!(got.text, "no attachments");
    assert_eq!(got.attachments, None);
}

#[tokio::test]
async fn drafts_are_isolated_by_client_and_removed_on_workspace_delete() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws, "WS", false))
        .await
        .expect("insert ws");
    let agent = AgentId::from_string("agent-1");
    let a = ClientId::from_string("cli-a");
    let b = ClientId::from_string("cli-b");
    store.upsert_client(&a, None, None).await.unwrap();
    store.upsert_client(&b, None, None).await.unwrap();

    store
        .upsert_draft(&ws, &agent, &a, "from-a", None)
        .await
        .unwrap();
    store
        .upsert_draft(&ws, &agent, &b, "from-b", None)
        .await
        .unwrap();
    assert_eq!(
        store
            .get_draft(&ws, &agent, &a)
            .await
            .unwrap()
            .unwrap()
            .text,
        "from-a"
    );
    assert_eq!(
        store
            .get_draft(&ws, &agent, &b)
            .await
            .unwrap()
            .unwrap()
            .text,
        "from-b"
    );

    store.delete_workspace(&ws).await.expect("delete ws");
    assert!(
        store.get_draft(&ws, &agent, &a).await.unwrap().is_none(),
        "delete_workspace removes the workspace's drafts"
    );
    assert!(
        store.get_draft(&ws, &agent, &b).await.unwrap().is_none(),
        "delete_workspace removes drafts for every client"
    );
}

#[tokio::test]
async fn known_repo_upsert_idempotent_on_path() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");

    store
        .upsert_known_repo("/src/a", "a", Some("owner-a"))
        .await
        .expect("insert");
    // Re-upserting the same path must not create a duplicate row.
    store
        .upsert_known_repo("/src/a", "a", Some("owner-a"))
        .await
        .expect("upsert");
    let repos = store.list_known_repos().await.expect("list");
    assert_eq!(repos.len(), 1, "path is unique");
    let added_at = repos[0].added_at.clone();

    // Conflict updates name/owner when provided and keeps the original addedAt.
    store
        .upsert_known_repo("/src/a", "a-renamed", Some("owner-b"))
        .await
        .expect("update");
    let repos = store.list_known_repos().await.expect("list");
    assert_eq!(repos.len(), 1);
    assert_eq!(repos[0].name, "a-renamed");
    assert_eq!(repos[0].owner.as_deref(), Some("owner-b"));
    assert_eq!(repos[0].added_at, added_at, "addedAt is preserved");

    // An absent owner preserves the existing one (TS `?? existing`).
    store
        .upsert_known_repo("/src/a", "a-renamed", None)
        .await
        .expect("update no owner");
    let repos = store.list_known_repos().await.expect("list");
    assert_eq!(repos[0].owner.as_deref(), Some("owner-b"));
}

#[tokio::test]
async fn known_repo_list_orders_by_last_used_desc() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");

    // Insert three repos; each upsert stamps last_used_at = now, so the most
    // recently upserted sorts first. Small sleeps keep the ISO timestamps
    // strictly ordered regardless of clock resolution.
    let gap = std::time::Duration::from_millis(5);
    store.upsert_known_repo("/src/a", "a", None).await.unwrap();
    tokio::time::sleep(gap).await;
    store.upsert_known_repo("/src/b", "b", None).await.unwrap();
    tokio::time::sleep(gap).await;
    store.upsert_known_repo("/src/c", "c", None).await.unwrap();
    tokio::time::sleep(gap).await;
    // Bump /src/a so it becomes the most-recently-used.
    store.upsert_known_repo("/src/a", "a", None).await.unwrap();

    let repos = store.list_known_repos().await.expect("list");
    let paths: Vec<&str> = repos.iter().map(|r| r.path.as_str()).collect();
    assert_eq!(paths, vec!["/src/a", "/src/c", "/src/b"]);
}

#[tokio::test]
async fn known_repo_remove_deletes_by_path_and_tolerates_missing() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");

    store.upsert_known_repo("/src/a", "a", None).await.unwrap();
    store.upsert_known_repo("/src/b", "b", None).await.unwrap();

    // Removing a registered path deletes exactly that row.
    let removed = store.remove_known_repo("/src/a").await.expect("remove");
    assert!(removed, "existing path reports removed=true");
    let repos = store.list_known_repos().await.expect("list");
    let paths: Vec<&str> = repos.iter().map(|r| r.path.as_str()).collect();
    assert_eq!(paths, vec!["/src/b"], "only the targeted repo is deleted");

    // Removing an unregistered path is a no-op, not an error.
    let removed = store.remove_known_repo("/src/a").await.expect("remove");
    assert!(!removed, "missing path reports removed=false");
    assert_eq!(store.list_known_repos().await.expect("list").len(), 1);
}

#[tokio::test]
async fn script_upsert_list_remove_round_trip() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");

    let mut env = std::collections::BTreeMap::new();
    env.insert("PORT".to_string(), "3000".to_string());
    let script = intent_core::Script {
        id: "s-1".to_string(),
        workspace_id: "ws-1".to_string(),
        name: "dev server".to_string(),
        command: "npm run dev".to_string(),
        cwd: Some("web".to_string()),
        env: Some(env),
        mode: intent_core::ScriptMode::Service,
        category: Some("dev".to_string()),
        source: "user".to_string(),
        auto_start: Some(true),
        created_at: now_iso(),
        updated_at: None,
    };
    store.upsert_script(&script).await.expect("insert");

    // Every field round-trips, including the JSON env map and optionals.
    let listed = store.list_all_scripts().await.expect("list");
    assert_eq!(listed, vec![script.clone()]);

    // Upsert on the same id replaces the row (no duplicate).
    let mut renamed = script.clone();
    renamed.name = "dev server 2".to_string();
    renamed.updated_at = Some(now_iso());
    store.upsert_script(&renamed).await.expect("replace");
    let listed = store.list_all_scripts().await.expect("list");
    assert_eq!(listed, vec![renamed]);

    // Sparse optionals persist as NULL and read back as None.
    let sparse = intent_core::Script {
        id: "s-2".to_string(),
        workspace_id: "ws-2".to_string(),
        name: "build".to_string(),
        command: "make".to_string(),
        cwd: None,
        env: None,
        mode: intent_core::ScriptMode::Command,
        category: None,
        source: "user".to_string(),
        auto_start: None,
        created_at: now_iso(),
        updated_at: None,
    };
    store.upsert_script(&sparse).await.expect("insert sparse");
    let listed = store.list_all_scripts().await.expect("list");
    assert_eq!(listed.len(), 2);
    assert!(listed.contains(&sparse));

    // Remove deletes exactly the targeted row; a missing id is not an error.
    assert!(store.remove_script("s-1").await.expect("remove"));
    assert!(!store.remove_script("s-1").await.expect("remove again"));
    let listed = store.list_all_scripts().await.expect("list");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].id, "s-2");
}

#[tokio::test]
async fn script_bulk_upsert_round_trip_replace_and_chunking() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");

    // An empty slice is a no-op, not an error.
    store.upsert_scripts(&[]).await.expect("empty upsert");
    assert!(store.list_all_scripts().await.expect("list").is_empty());

    // 2100 scripts cross the 2048-rows-per-statement chunk boundary; every
    // field round-trips, including the JSON env map and sparse optionals.
    let mut env = std::collections::BTreeMap::new();
    env.insert("PORT".to_string(), "3000".to_string());
    let scripts: Vec<intent_core::Script> = (0..2100)
        .map(|i| intent_core::Script {
            id: format!("s-{i}"),
            workspace_id: "ws-1".to_string(),
            name: format!("script {i}"),
            command: format!("echo {i}"),
            cwd: (i % 2 == 0).then(|| "web".to_string()),
            env: (i % 2 == 0).then(|| env.clone()),
            mode: intent_core::ScriptMode::Command,
            category: (i % 2 == 0).then(|| "dev".to_string()),
            source: "user".to_string(),
            auto_start: (i % 2 == 0).then_some(true),
            created_at: now_iso(),
            updated_at: None,
        })
        .collect();
    store.upsert_scripts(&scripts).await.expect("bulk insert");
    let listed = store.list_all_scripts().await.expect("list");
    assert_eq!(listed.len(), 2100);
    for script in &scripts {
        assert!(listed.contains(script), "missing {}", script.id);
    }

    // Bulk upsert on existing ids replaces the rows (no duplicates).
    let mut renamed = scripts.clone();
    for script in &mut renamed {
        script.name = format!("{} v2", script.name);
    }
    store.upsert_scripts(&renamed).await.expect("bulk replace");
    let listed = store.list_all_scripts().await.expect("list");
    assert_eq!(listed.len(), 2100);
    for script in &renamed {
        assert!(listed.contains(script), "missing replaced {}", script.id);
    }
}

#[tokio::test]
async fn script_was_running_marker_set_clear_and_reset_semantics() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");

    let script = |ws: &str, id: &str| intent_core::Script {
        id: id.to_string(),
        workspace_id: ws.to_string(),
        name: "dev".to_string(),
        command: "npm run dev".to_string(),
        cwd: None,
        env: None,
        mode: intent_core::ScriptMode::Service,
        category: None,
        source: "user".to_string(),
        auto_start: None,
        created_at: now_iso(),
        updated_at: None,
    };
    store
        .upsert_script(&script("ws-1", "s-1"))
        .await
        .expect("insert");
    store
        .upsert_script(&script("ws-1", "s-2"))
        .await
        .expect("insert");

    // Fresh rows carry no marker.
    assert!(store
        .list_was_running_script_ids()
        .await
        .expect("list")
        .is_empty());

    // Set is scoped to the targeted (workspace, id) and survives repeated
    // reads.
    store
        .set_script_was_running("ws-1", "s-1", true)
        .await
        .expect("set");
    assert_eq!(
        store.list_was_running_script_ids().await.expect("list"),
        vec![("ws-1".to_string(), "s-1".to_string())]
    );
    assert_eq!(
        store.list_was_running_script_ids().await.expect("list"),
        vec![("ws-1".to_string(), "s-1".to_string())],
        "marker persists until explicitly cleared"
    );

    // A write against the same id in a different workspace must not touch
    // the row owned by ws-1 (the runtime registry permits the same
    // client-supplied id in separate workspaces).
    store
        .set_script_was_running("ws-2", "s-1", false)
        .await
        .expect("cross-workspace no-op");
    assert_eq!(
        store.list_was_running_script_ids().await.expect("list"),
        vec![("ws-1".to_string(), "s-1".to_string())],
        "cross-workspace write is a no-op"
    );

    // An upsert (INSERT OR REPLACE) resets the marker — a replaced
    // definition starts a fresh runtime life.
    store
        .upsert_script(&script("ws-1", "s-1"))
        .await
        .expect("replace");
    assert!(store
        .list_was_running_script_ids()
        .await
        .expect("list")
        .is_empty());

    // Clear is durable; an unknown id is a no-op, not an error.
    store
        .set_script_was_running("ws-1", "s-2", true)
        .await
        .expect("set");
    store
        .set_script_was_running("ws-1", "s-2", false)
        .await
        .expect("clear");
    store
        .set_script_was_running("ws-1", "missing", true)
        .await
        .expect("unknown id no-op");
    assert!(store
        .list_was_running_script_ids()
        .await
        .expect("list")
        .is_empty());

    // Remove deletes the row (marker gone with it).
    store
        .set_script_was_running("ws-1", "s-2", true)
        .await
        .expect("set");
    assert!(store.remove_script("s-2").await.expect("remove"));
    assert!(store
        .list_was_running_script_ids()
        .await
        .expect("list")
        .is_empty());
}

#[tokio::test]
async fn idempotency_get_put_round_trips_and_dedupes() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");

    assert!(
        store.get_idempotent("ws-1", "k1").await.unwrap().is_none(),
        "missing key reads as None"
    );
    let inserted = store
        .put_idempotent("ws-1", "k1", "note.create", "{\"id\":\"n1\"}")
        .await
        .unwrap();
    assert!(inserted, "first put inserts the row");
    assert_eq!(
        store.get_idempotent("ws-1", "k1").await.unwrap().as_deref(),
        Some("{\"id\":\"n1\"}")
    );
    // A second put under the same (ws,key) is an INSERT OR IGNORE no-op that
    // keeps the original stored result.
    let inserted_again = store
        .put_idempotent("ws-1", "k1", "note.create", "{\"id\":\"n2\"}")
        .await
        .unwrap();
    assert!(!inserted_again, "duplicate put is a no-op");
    assert_eq!(
        store.get_idempotent("ws-1", "k1").await.unwrap().as_deref(),
        Some("{\"id\":\"n1\"}"),
        "duplicate put must not overwrite the original result"
    );
    // The same key is independent across workspaces (and the "" global sentinel).
    assert!(store.get_idempotent("ws-2", "k1").await.unwrap().is_none());
    assert!(store.get_idempotent("", "k1").await.unwrap().is_none());
}

#[tokio::test]
async fn idempotency_reaper_deletes_only_rows_older_than_cutoff() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");

    // One row well in the past, one fresh row (put stamps created_at = now).
    let old_ts = intent_core::iso_minutes_ago(48 * 60);
    sqlx::query(
        "INSERT INTO idempotency_key \
         (workspace_id, idempotency_key, method, result_json, created_at) \
         VALUES (?,?,?,?,?)",
    )
    .bind("ws-1")
    .bind("old")
    .bind("note.create")
    .bind("{\"id\":\"old\"}")
    .bind(&old_ts)
    .execute(store.write_pool())
    .await
    .expect("seed old row");
    store
        .put_idempotent("ws-1", "fresh", "note.create", "{\"id\":\"fresh\"}")
        .await
        .unwrap();

    let cutoff = intent_core::iso_minutes_ago(24 * 60);
    let removed = store.reap_idempotent(&cutoff).await.expect("reap");
    assert_eq!(removed, 1, "only the >24h row is reaped");
    assert!(store.get_idempotent("ws-1", "old").await.unwrap().is_none());
    assert!(store
        .get_idempotent("ws-1", "fresh")
        .await
        .unwrap()
        .is_some());

    // Re-running the sweep removes nothing more (idempotent).
    assert_eq!(store.reap_idempotent(&cutoff).await.expect("reap"), 0);
}

/// Concurrent write + read stress test: verify that the single-writer pool
/// eliminates `SQLITE_BUSY` (code 5) errors under heavy concurrent load.
/// Spawns ~50 concurrent writes + concurrent reads; all must succeed without
/// `busy_timeout` errors, and no read may be latency-coupled to the write storm
/// (which is what a shared read/write pool regression looks like).
#[tokio::test]
async fn concurrent_writes_no_sqlite_busy() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.unwrap();

    let storm_start = std::time::Instant::now();

    // Spawn 50 concurrent single-row writes
    let write_handles: Vec<_> = (0..50)
        .map(|i| {
            let store = store.clone();
            tokio::spawn(async move {
                let ws_id = WorkspaceId::new();
                let ts = now_iso();
                let workspace = Workspace {
                    id: ws_id.clone(),
                    title: format!("Workspace {i}"),
                    branch: format!("main-{i}"),
                    base_ref: None,
                    base_commit_sha: None,
                    status: WorkspaceStatus::Active,
                    status_message: None,
                    status_image_asset_id: None,
                    activity: WorkspaceActivity::Idle,
                    attention: WorkspaceAttention::Unread,
                    created_at: ts.clone(),
                    updated_at: ts.clone(),
                    last_activity: None,
                    tags: vec![],
                    path: Some(format!("/tmp/ws-{i}")),
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
                    execution_environment: None,
                    disk_usage: None,
                    pending_delete_at: None,
                };
                store.insert_workspace(&workspace).await
            })
        })
        .collect();

    // Spawn 50 concurrent reads (list workspaces); each measures its own
    // completion latency so the read bound is not inflated by waiting on the
    // write joins below (the 50 fsync-heavy writes can take a while on a
    // loaded host).
    let read_handles: Vec<_> = (0..50)
        .map(|_| {
            let store = store.clone();
            let started = std::time::Instant::now();
            tokio::spawn(async move {
                let result = store.list_workspaces(false).await.map(|ws| ws.len());
                (result, started.elapsed())
            })
        })
        .collect();

    // All writes must succeed (no SQLITE_BUSY errors)
    for handle in write_handles {
        let result = handle.await.unwrap();
        assert!(
            result.is_ok(),
            "Write failed (SQLITE_BUSY would be Error::Internal with 'database is locked'): {result:?}"
        );
    }
    let write_storm = storm_start.elapsed();

    // All reads must succeed, and none may be latency-coupled to the write
    // storm. The budget is relative to the measured storm duration (floor 5s)
    // rather than an absolute wall-clock bound: on a CI box co-tenant with
    // other heavy jobs (monorepo#1239) fsync/scheduler contention slows
    // everything uniformly, but only a shared-pool regression makes reads
    // queue behind the writers and finish with the storm.
    let mut slowest_read = std::time::Duration::ZERO;
    for handle in read_handles {
        let (result, elapsed) = handle.await.unwrap();
        assert!(result.is_ok(), "Read failed: {result:?}");
        slowest_read = slowest_read.max(elapsed);
    }
    let read_budget = std::cmp::max(std::time::Duration::from_secs(5), write_storm / 2);
    assert!(
        slowest_read < read_budget,
        "Slowest read took {slowest_read:?} (budget {read_budget:?}, write storm {write_storm:?}) — reads queueing behind writers?"
    );

    // Verify all 50 workspaces were written
    let all_workspaces = store.list_workspaces(false).await.unwrap();
    assert_eq!(
        all_workspaces.len(),
        50,
        "Expected 50 workspaces, got {}",
        all_workspaces.len()
    );
}

/// Test the periodic WAL checkpoint task: verify it runs on schedule and stops
/// cleanly when the handle is aborted. The task runs every 60s and executes
/// PRAGMA `wal_checkpoint(PASSIVE)` via the write pool.
#[tokio::test]
async fn periodic_wal_checkpoint_runs_and_stops() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.unwrap();

    // Spawn the periodic checkpoint task
    let handle = store.spawn_periodic_wal_checkpoint();

    // The task should be running (not immediately finished)
    assert!(!handle.is_finished(), "checkpoint task finished too early");

    // Abort the task (simulates daemon shutdown)
    handle.abort();

    // Give it a moment to clean up
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Task should be finished now
    assert!(
        handle.is_finished(),
        "checkpoint task did not stop after abort"
    );

    // Verify the store still works after aborting the checkpoint task
    let ws = sample_workspace(&WorkspaceId::new(), "Test", false);
    store.insert_workspace(&ws).await.unwrap();
    let loaded = store.list_workspaces(false).await.unwrap();
    assert_eq!(loaded.len(), 1);
}

/// Smoke test: verify the two-pool split sizing (write pool `max_connections=1`,
/// read pool `max_connections=32`) and that both pools support basic queries.
/// The write pool is single-connection to serialize all mutations and eliminate
/// in-process writer-vs-writer `busy_timeout` contention. The read pool size (32)
/// is sized to absorb the client-driven startup read burst without slow-acquire
/// warnings (STAB-6, STAB-46), scaled up from 16 for the RAM-based agent
/// process cap raise to 56 (intent-hq/intentd#296).
#[tokio::test]
async fn pool_smoke_test_with_explicit_config() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");

    // Verify the write pool is single-connection.
    assert_eq!(store.write_pool().options().get_max_connections(), 1);

    // Verify the read pool is 32 connections.
    assert_eq!(store.read_pool().options().get_max_connections(), 32);

    // Verify both pools are usable and the configuration doesn't break basic ops.
    let row: (i64,) = sqlx::query_as("SELECT 1")
        .fetch_one(store.read_pool())
        .await
        .expect("basic query works on read pool");
    assert_eq!(row.0, 1);

    let row2: (i64,) = sqlx::query_as("SELECT 1")
        .fetch_one(store.write_pool())
        .await
        .expect("basic query works on write pool");
    assert_eq!(row2.0, 1);

    // Verify multiple concurrent acquires work on the read pool (max_connections=32).
    let mut handles = Vec::new();
    for _ in 0..5 {
        let p = store.read_pool().clone();
        handles.push(tokio::spawn(async move {
            let row: (i64,) = sqlx::query_as("SELECT 1").fetch_one(&p).await?;
            Ok::<_, sqlx::Error>(row.0)
        }));
    }
    for h in handles {
        assert_eq!(h.await.expect("task completes").expect("query"), 1);
    }

    store.close().await;
}

/// Regression test for STAB-19: appending a message to an agent session
/// must refresh `agent_session.updated_at` so the FE agent-card timestamp
/// reflects real activity, not just status transitions.
#[tokio::test]
async fn agent_message_append_refreshes_updated_at() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws, "WS", false))
        .await
        .expect("insert ws");

    let agent_id = AgentId::from("agent-bbbbbbbb-1111-2222-3333-444444444444");
    let mut session = sample_agent_session(&agent_id, &ws);
    session.updated_at = "2026-01-01T00:00:00Z".to_string();
    store
        .insert_agent_session(&session)
        .await
        .expect("insert session");

    // Baseline: the session was inserted with updated_at = "2026-01-01T00:00:00Z".
    let before = store.get_agent_session(&agent_id).await.expect("get");
    assert_eq!(before.updated_at, "2026-01-01T00:00:00Z");

    // Append a message with a later timestamp.
    let later = "2026-01-01T01:00:00Z";
    store
        .append_agent_message(
            &agent_id,
            "user",
            &json!([{ "type": "text", "text": "test message" }]),
            later,
        )
        .await
        .expect("append message");

    // STAB-19 fix: refresh_agent_session_timestamp is called by the services
    // layer after append_agent_message. Simulate that here.
    store
        .refresh_agent_session_timestamp(&ws, &agent_id, later)
        .await
        .expect("refresh timestamp");

    // The session's updated_at should now reflect the message timestamp.
    let after = store.get_agent_session(&agent_id).await.expect("get");
    assert_eq!(
        after.updated_at, later,
        "updated_at must advance when a message is appended"
    );
    assert_eq!(after.messages.len(), 1, "message log should have 1 entry");
}

fn queue_row(agent_id: &AgentId, position: i64, content: &str) -> AgentQueueRow {
    // Matches production, where the row id is the queued message id and a
    // fresh enqueue's turn_id equals its id (monorepo#1022).
    let id = uuid::Uuid::new_v4().to_string();
    AgentQueueRow {
        id: id.clone(),
        agent_id: agent_id.clone(),
        position,
        payload: json!({
            "id": id,
            "turnId": id,
            "content": content,
            "queuedAt": now_iso(),
            "editing": false,
            "persisted": true,
            "requeuedAfterFailure": false,
            "messageMetadata": { "source": "test" },
        }),
        created_at: now_iso(),
        turn_id: id,
    }
}

#[tokio::test]
async fn agent_queue_replace_load_delete_round_trip() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws, "WS", false))
        .await
        .expect("insert ws");
    let agent = AgentId::new();
    store
        .insert_agent_session(&sample_agent_session(&agent, &ws))
        .await
        .expect("insert session");

    // Snapshot with two entries round-trips in position order with the payload intact.
    let rows = vec![
        queue_row(&agent, 0, "first"),
        queue_row(&agent, 1, "second"),
    ];
    store
        .replace_agent_queue(&agent, &rows)
        .await
        .expect("replace queue");
    let loaded = store.load_all_agent_queues().await.expect("load queues");
    assert_eq!(loaded.len(), 2);
    assert_eq!(loaded[0].agent_id, agent);
    assert_eq!(loaded[0].position, 0);
    assert_eq!(loaded[0].payload["content"], "first");
    assert_eq!(loaded[0].payload["persisted"], json!(true));
    assert_eq!(loaded[0].payload["messageMetadata"]["source"], "test");
    assert_eq!(loaded[0].turn_id, rows[0].turn_id, "turn_id round-trips");
    assert_eq!(loaded[1].position, 1);
    assert_eq!(loaded[1].payload["content"], "second");
    assert_eq!(loaded[1].turn_id, rows[1].turn_id, "turn_id round-trips");

    // Replace is a whole-queue snapshot: a shorter snapshot drops the rest.
    store
        .replace_agent_queue(&agent, &[queue_row(&agent, 0, "only")])
        .await
        .expect("replace shorter");
    let loaded = store.load_all_agent_queues().await.expect("load queues");
    assert_eq!(loaded.len(), 1);
    assert_eq!(loaded[0].payload["content"], "only");

    // Per-agent delete clears the persisted queue.
    store
        .delete_agent_queue(&agent)
        .await
        .expect("delete queue");
    assert!(store
        .load_all_agent_queues()
        .await
        .expect("load queues")
        .is_empty());
}

#[tokio::test]
async fn agent_queue_move_is_atomic_hand_off() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws, "WS", false))
        .await
        .expect("insert ws");
    let poisoned = AgentId::new();
    let target = AgentId::new();
    store
        .insert_agent_session(&sample_agent_session(&poisoned, &ws))
        .await
        .expect("insert poisoned");
    store
        .insert_agent_session(&sample_agent_session(&target, &ws))
        .await
        .expect("insert target");

    // Seed persisted rows on BOTH agents. The migrated entries reuse the
    // poisoned rows' ids — `agent_queue.id` is a global primary key, so a
    // non-atomic clear-then-replace would conflict; the single-transaction
    // move must land them under the target while both agents' old rows go.
    let old = vec![
        queue_row(&poisoned, 0, "first"),
        queue_row(&poisoned, 1, "second"),
    ];
    store
        .replace_agent_queue(&poisoned, &old)
        .await
        .expect("seed poisoned");
    store
        .replace_agent_queue(&target, &[queue_row(&target, 0, "stale-target")])
        .await
        .expect("seed target");

    let moved: Vec<AgentQueueRow> = old
        .iter()
        .map(|r| AgentQueueRow {
            agent_id: target.clone(),
            ..r.clone()
        })
        .collect();
    store
        .move_agent_queue(&poisoned, &target, &moved)
        .await
        .expect("move queue");

    // One load observes the whole hand-off: the poisoned queue is empty and
    // the target holds exactly the moved rows (ids preserved, in order).
    let loaded = store.load_all_agent_queues().await.expect("load queues");
    assert!(loaded.iter().all(|r| r.agent_id == target));
    assert_eq!(loaded.len(), 2);
    assert_eq!(loaded[0].id, old[0].id);
    assert_eq!(loaded[0].payload["content"], "first");
    assert_eq!(loaded[1].id, old[1].id);
    assert_eq!(loaded[1].payload["content"], "second");

    // Row/agent mismatch (rows not owned by `to`) fails fast without
    // touching either queue.
    let bad = vec![queue_row(&poisoned, 0, "wrong-owner")];
    assert!(store
        .move_agent_queue(&poisoned, &target, &bad)
        .await
        .is_err());
    let loaded = store.load_all_agent_queues().await.expect("load queues");
    assert_eq!(loaded.len(), 2);
    assert!(loaded.iter().all(|r| r.agent_id == target));

    // An empty move just clears both queues.
    store
        .move_agent_queue(&target, &poisoned, &[])
        .await
        .expect("empty move");
    assert!(store
        .load_all_agent_queues()
        .await
        .expect("load queues")
        .is_empty());
}

#[tokio::test]
async fn agent_queue_cascades_with_agent_session() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws, "WS", false))
        .await
        .expect("insert ws");
    let doomed = AgentId::new();
    let survivor = AgentId::new();
    for agent in [&doomed, &survivor] {
        store
            .insert_agent_session(&sample_agent_session(agent, &ws))
            .await
            .expect("insert session");
        store
            .replace_agent_queue(agent, &[queue_row(agent, 0, "queued")])
            .await
            .expect("replace queue");
    }

    // Deleting the agent session cascades its queue rows via the FK.
    assert!(store
        .delete_agent_session(&ws, &doomed)
        .await
        .expect("delete session"));
    let loaded = store.load_all_agent_queues().await.expect("load queues");
    assert_eq!(loaded.len(), 1);
    assert_eq!(loaded[0].agent_id, survivor);

    // Workspace delete cascades the remaining session and its queue rows.
    store.delete_workspace(&ws).await.expect("delete ws");
    assert!(store
        .load_all_agent_queues()
        .await
        .expect("load queues")
        .is_empty());
}

#[tokio::test]
async fn agent_queue_replace_rejects_mismatched_agent_id() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws, "WS", false))
        .await
        .expect("insert ws");
    let agent = AgentId::new();
    store
        .insert_agent_session(&sample_agent_session(&agent, &ws))
        .await
        .expect("insert session");

    // A row stamped with a different agent id fails fast instead of being
    // silently persisted under `agent`.
    let other = AgentId::new();
    let err = store
        .replace_agent_queue(&agent, &[queue_row(&other, 0, "misfiled")])
        .await
        .expect_err("mismatched row must be rejected");
    assert!(err.to_string().contains("belongs to agent"));
    assert!(store
        .load_all_agent_queues()
        .await
        .expect("load queues")
        .is_empty());
}

#[tokio::test]
async fn agent_queue_load_survives_corrupt_payload() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws, "WS", false))
        .await
        .expect("insert ws");
    let agent = AgentId::new();
    store
        .insert_agent_session(&sample_agent_session(&agent, &ws))
        .await
        .expect("insert session");
    let rows = vec![queue_row(&agent, 0, "bad"), queue_row(&agent, 1, "good")];
    let corrupt_id = rows[0].id.clone();
    store
        .replace_agent_queue(&agent, &rows)
        .await
        .expect("replace queue");

    // Corrupt one payload behind the API's back (e.g. a manual DB edit).
    sqlx::query("UPDATE agent_queue SET payload = 'not json' WHERE id = ?")
        .bind(&corrupt_id)
        .execute(store.write_pool())
        .await
        .expect("corrupt payload");

    // Load stays best-effort: the corrupt row comes back as Null instead of
    // failing the whole load, and the healthy row is intact.
    let loaded = store.load_all_agent_queues().await.expect("load queues");
    assert_eq!(loaded.len(), 2);
    assert_eq!(loaded[0].payload, serde_json::Value::Null);
    assert_eq!(loaded[1].payload["content"], "good");
}

/// Legacy rows persisted before migration 0065 have a NULL `turn_id` column;
/// the load query defaults them to the row `id` (monorepo#1022).
#[tokio::test]
async fn agent_queue_load_defaults_null_turn_id_to_row_id() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws, "WS", false))
        .await
        .expect("insert ws");
    let agent = AgentId::new();
    store
        .insert_agent_session(&sample_agent_session(&agent, &ws))
        .await
        .expect("insert session");

    // Insert a legacy-shaped row directly (no turn_id column value), the way
    // a pre-0065 daemon would have left it.
    sqlx::query(
        "INSERT INTO agent_queue (id, agent_id, position, payload, created_at) \
         VALUES (?,?,?,?,?)",
    )
    .bind("legacy-row")
    .bind(&agent.0)
    .bind(0i64)
    .bind(r#"{"id":"legacy-row","content":"old","queuedAt":"2026-01-01T00:00:00Z"}"#)
    .bind(now_iso())
    .execute(store.write_pool())
    .await
    .expect("insert legacy row");

    let loaded = store.load_all_agent_queues().await.expect("load queues");
    assert_eq!(loaded.len(), 1);
    assert_eq!(loaded[0].id, "legacy-row");
    assert_eq!(
        loaded[0].turn_id, "legacy-row",
        "NULL turn_id must default to the row id"
    );
}

/// A transient `SQLITE_BUSY` error as surfaced by the repositories
/// (monorepo#1139: "get note failed: ... (code: 5) database is locked").
fn busy_error() -> Error {
    Error::Internal(
        "get note failed: error returned from database: (code: 5) database is locked".to_string(),
    )
}

/// `with_read_retry` retries a closure that fails with a `code: 5` Internal
/// error N times before succeeding, and returns the eventual Ok
/// (monorepo#1139).
#[tokio::test]
async fn read_retry_retries_busy_then_succeeds() {
    use std::sync::atomic::{AtomicU32, Ordering};
    let calls = AtomicU32::new(0);
    let result = crate::with_read_retry(|| async {
        let n = calls.fetch_add(1, Ordering::SeqCst);
        if n < 2 {
            Err(busy_error())
        } else {
            Ok(42u32)
        }
    })
    .await;
    assert_eq!(result.expect("busy failures should be retried"), 42);
    assert_eq!(calls.load(Ordering::SeqCst), 3);
}

/// Non-busy errors are NOT retried: the closure runs exactly once and the
/// error is surfaced immediately.
#[tokio::test]
async fn read_retry_does_not_retry_non_busy_errors() {
    use std::sync::atomic::{AtomicU32, Ordering};
    let calls = AtomicU32::new(0);
    let result: crate::Result<u32> = crate::with_read_retry(|| async {
        calls.fetch_add(1, Ordering::SeqCst);
        Err(Error::Internal("boom".to_string()))
    })
    .await;
    match result {
        Err(Error::Internal(msg)) => assert_eq!(msg, "boom"),
        other => panic!("expected Internal(boom), got {other:?}"),
    }
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

/// Extended busy-family codes (261 `SQLITE_BUSY_RECOVERY`, 517
/// `SQLITE_BUSY_SNAPSHOT`, 773 `SQLITE_BUSY_TIMEOUT`) are retried like the base
/// `(code: 5)`, while unrelated 5xx codes (e.g. 516 `SQLITE_ABORT_ROLLBACK`)
/// are not.
#[tokio::test]
async fn read_retry_classifies_busy_family_codes() {
    use std::sync::atomic::{AtomicU32, Ordering};
    for code in [261u32, 517, 773] {
        let calls = AtomicU32::new(0);
        let result = crate::with_read_retry(|| async {
            let n = calls.fetch_add(1, Ordering::SeqCst);
            if n < 1 {
                Err(Error::Internal(format!(
                    "error returned from database: (code: {code}) database is locked"
                )))
            } else {
                Ok(code)
            }
        })
        .await;
        assert_eq!(result.expect("busy-family code should be retried"), code);
        assert_eq!(calls.load(Ordering::SeqCst), 2, "code {code}");
    }

    let calls = AtomicU32::new(0);
    let result: crate::Result<u32> = crate::with_read_retry(|| async {
        calls.fetch_add(1, Ordering::SeqCst);
        Err(Error::Internal(
            "error returned from database: (code: 516) abort due to ROLLBACK".to_string(),
        ))
    })
    .await;
    assert!(matches!(result, Err(Error::Internal(_))));
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "non-busy 5xx code must not be retried"
    );
}

/// `NotFound` is not a busy error: it passes through without retries.
#[tokio::test]
async fn read_retry_passes_through_not_found() {
    use std::sync::atomic::{AtomicU32, Ordering};
    let calls = AtomicU32::new(0);
    let result: crate::Result<u32> = crate::with_read_retry(|| async {
        calls.fetch_add(1, Ordering::SeqCst);
        Err(Error::NotFound("note spec".to_string()))
    })
    .await;
    assert!(matches!(result, Err(Error::NotFound(_))));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

/// The shared loop returns the last error once the deadline is exhausted.
/// Uses a small injected deadline so the test stays fast (the production
/// wrappers use a ~30s window).
#[tokio::test]
async fn busy_retry_returns_last_error_after_deadline() {
    use std::sync::atomic::{AtomicU32, Ordering};
    let calls = AtomicU32::new(0);
    let result: crate::Result<u32> = crate::with_busy_retry(
        || async {
            calls.fetch_add(1, Ordering::SeqCst);
            Err(busy_error())
        },
        std::time::Duration::from_millis(150),
    )
    .await;
    match result {
        Err(Error::Internal(msg)) => assert!(msg.contains("code: 5"), "unexpected error: {msg}"),
        other => panic!("expected Internal busy error, got {other:?}"),
    }
    assert!(
        calls.load(Ordering::SeqCst) >= 2,
        "expected at least one retry before the deadline"
    );
}

/// `with_write_txn_retry` (STAB-7) shares the same loop: busy failures are
/// retried until success.
#[tokio::test]
async fn write_txn_retry_retries_busy_then_succeeds() {
    use std::sync::atomic::{AtomicU32, Ordering};
    let calls = AtomicU32::new(0);
    let result = crate::with_write_txn_retry(|| async {
        let n = calls.fetch_add(1, Ordering::SeqCst);
        if n < 1 {
            Err(busy_error())
        } else {
            Ok("done")
        }
    })
    .await;
    assert_eq!(result.expect("busy failures should be retried"), "done");
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

/// Guard against duplicate migration version numbers: two files sharing a
/// version (e.g. two `0062_*.sql`) embed fine but make every `Store::open`
/// fail at runtime with a UNIQUE constraint violation on
/// `_sqlx_migrations.version`.
#[test]
#[allow(clippy::case_sensitive_file_extension_comparisons)] // extensions generated by our own code with fixed case
fn migrations_have_unique_versions() {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("migrations");
    let mut versions: std::collections::HashMap<i64, Vec<String>> =
        std::collections::HashMap::new();
    for entry in std::fs::read_dir(&dir).expect("read migrations dir") {
        let name = entry.expect("dir entry").file_name();
        let name = name.to_string_lossy().to_string();
        if !name.ends_with(".sql") {
            continue;
        }
        let digits: String = name.chars().take_while(char::is_ascii_digit).collect();
        let version: i64 = digits
            .parse()
            .unwrap_or_else(|_| panic!("migration '{name}' has no numeric version prefix"));
        versions.entry(version).or_default().push(name);
    }
    assert!(!versions.is_empty(), "no migrations found in {dir:?}");
    let dupes: Vec<_> = versions.iter().filter(|(_, v)| v.len() > 1).collect();
    assert!(
        dupes.is_empty(),
        "duplicate migration version numbers found: {dupes:?}"
    );
}

fn sample_hook(id: &HookId, ws: &WorkspaceId, agent: &AgentId, name: &str) -> Hook {
    Hook {
        hook_id: id.clone(),
        workspace_id: ws.clone(),
        agent_id: agent.clone(),
        name: name.to_string(),
        code: "return { dispatch: false }".to_string(),
        delay_ms: 60_000,
        state: HookState::Scheduled,
        created_at: now_iso(),
        last_run_at: None,
        next_run_at: Some(now_iso()),
        run_count: 0,
        last_error: None,
        last_logs: None,
        last_state: None,
        expires_at: Some(now_iso()),
        perpetual: false,
        dispatch_count: 0,
    }
}

/// Seed a workspace + agent session (hooks cascade with both) and return
/// their ids.
async fn seed_hook_owner(store: &Store) -> (WorkspaceId, AgentId) {
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws, "Hook WS", false))
        .await
        .expect("insert ws");
    let agent = AgentId(format!("agent-{}", uuid::Uuid::new_v4()));
    store
        .insert_agent_session(&sample_agent_session(&agent, &ws))
        .await
        .expect("insert agent");
    (ws, agent)
}

/// Insert → get round-trips every column, including optionals.
#[tokio::test]
async fn hook_insert_get_round_trip() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let (ws, agent) = seed_hook_owner(&store).await;

    let id = HookId(format!("hook-{}", uuid::Uuid::new_v4()));
    let hook = sample_hook(&id, &ws, &agent, "poll-ci");
    store.insert_hook(&hook).await.expect("insert hook");

    let got = store.get_hook(&id).await.expect("get hook");
    assert_eq!(got, hook);

    let missing = HookId("hook-missing".to_string());
    let err = store.get_hook(&missing).await.expect_err("missing hook");
    assert!(matches!(err, Error::NotFound(_)), "got {err:?}");
}

/// A perpetual hook round-trips its `perpetual` / `dispatch_count` columns,
/// and the dispatch-count bump persists.
#[tokio::test]
async fn hook_perpetual_and_dispatch_count_round_trip() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let (ws, agent) = seed_hook_owner(&store).await;

    // Default (one-shot) row reads back as `false` / 0.
    let one_shot_id = HookId(format!("hook-{}", uuid::Uuid::new_v4()));
    store
        .insert_hook(&sample_hook(&one_shot_id, &ws, &agent, "one-shot"))
        .await
        .expect("insert one-shot");
    let one_shot = store.get_hook(&one_shot_id).await.expect("get one-shot");
    assert!(!one_shot.perpetual);
    assert_eq!(one_shot.dispatch_count, 0);

    let id = HookId(format!("hook-{}", uuid::Uuid::new_v4()));
    let mut hook = sample_hook(&id, &ws, &agent, "perpetual");
    hook.perpetual = true;
    hook.dispatch_count = 2;
    store.insert_hook(&hook).await.expect("insert perpetual");
    assert_eq!(store.get_hook(&id).await.expect("get hook"), hook);

    store
        .increment_hook_dispatch_count(&id)
        .await
        .expect("bump dispatch count");
    let bumped = store.get_hook(&id).await.expect("get bumped");
    assert!(bumped.perpetual);
    assert_eq!(bumped.dispatch_count, 3);

    let err = store
        .increment_hook_dispatch_count(&HookId("hook-missing".to_string()))
        .await
        .expect_err("missing hook");
    assert!(matches!(err, Error::NotFound(_)), "got {err:?}");
}

/// Workspace/agent list filters return only the matching rows.
#[tokio::test]
async fn hook_list_filters_by_workspace_and_agent() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let (ws_a, agent_a) = seed_hook_owner(&store).await;
    let (ws_b, agent_b) = seed_hook_owner(&store).await;

    let id_a1 = HookId("hook-a1".to_string());
    let id_a2 = HookId("hook-a2".to_string());
    let id_b = HookId("hook-b".to_string());
    store
        .insert_hook(&sample_hook(&id_a1, &ws_a, &agent_a, "a1"))
        .await
        .expect("insert a1");
    store
        .insert_hook(&sample_hook(&id_a2, &ws_a, &agent_a, "a2"))
        .await
        .expect("insert a2");
    store
        .insert_hook(&sample_hook(&id_b, &ws_b, &agent_b, "b"))
        .await
        .expect("insert b");

    let ws_hooks = store.list_hooks_by_workspace(&ws_a).await.expect("list ws");
    let mut ws_ids: Vec<&str> = ws_hooks.iter().map(|h| h.hook_id.0.as_str()).collect();
    ws_ids.sort_unstable();
    assert_eq!(ws_ids, vec!["hook-a1", "hook-a2"]);

    let agent_hooks = store
        .list_hooks_by_agent(&agent_b)
        .await
        .expect("list agent");
    assert_eq!(agent_hooks.len(), 1);
    assert_eq!(agent_hooks[0].hook_id, id_b);

    let empty = store
        .list_hooks_by_agent(&AgentId("agent-none".to_string()))
        .await
        .expect("list none");
    assert!(empty.is_empty());
}

/// `count_active_hooks_by_agent` counts only `scheduled`/`running` rows for
/// the given agent — terminal rows and other agents' hooks are excluded, and
/// an unknown agent counts zero.
#[tokio::test]
async fn hook_count_active_by_agent() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let (ws, agent) = seed_hook_owner(&store).await;
    let (ws_other, agent_other) = seed_hook_owner(&store).await;

    let scheduled = sample_hook(&HookId("hook-sched".into()), &ws, &agent, "sched");
    let mut running = sample_hook(&HookId("hook-run".into()), &ws, &agent, "run");
    running.state = HookState::Running;
    let mut done = sample_hook(&HookId("hook-done".into()), &ws, &agent, "done");
    done.state = HookState::Dispatched;
    let mut gone = sample_hook(&HookId("hook-gone".into()), &ws, &agent, "gone");
    gone.state = HookState::Expired;
    let foreign = sample_hook(
        &HookId("hook-foreign".into()),
        &ws_other,
        &agent_other,
        "foreign",
    );
    for h in [&scheduled, &running, &done, &gone, &foreign] {
        store.insert_hook(h).await.expect("insert hook");
    }

    assert_eq!(
        store
            .count_active_hooks_by_agent(&agent)
            .await
            .expect("count"),
        2,
        "scheduled + running only"
    );
    assert_eq!(
        store
            .count_active_hooks_by_agent(&AgentId("agent-none".into()))
            .await
            .expect("count none"),
        0
    );
}

/// State transitions, run bookkeeping, and last-error updates persist; every
/// updater maps an unknown id to `NotFound`.
#[tokio::test]
async fn hook_state_run_and_error_updates() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let (ws, agent) = seed_hook_owner(&store).await;

    let id = HookId("hook-upd".to_string());
    store
        .insert_hook(&sample_hook(&id, &ws, &agent, "upd"))
        .await
        .expect("insert");

    store
        .update_hook_state(&id, HookState::Running)
        .await
        .expect("scheduled -> running");
    assert_eq!(store.get_hook(&id).await.unwrap().state, HookState::Running);

    let ran_at = now_iso();
    let next_at = now_iso();
    store
        .update_hook_run(&id, &ran_at, Some(&next_at))
        .await
        .expect("record run");
    let got = store.get_hook(&id).await.unwrap();
    assert_eq!(got.run_count, 1);
    assert_eq!(got.last_run_at.as_deref(), Some(ran_at.as_str()));
    assert_eq!(got.next_run_at.as_deref(), Some(next_at.as_str()));

    // Atomic expiry: one call flips state AND clears next_run_at together.
    store.expire_hook(&id).await.expect("atomic expiry");
    let got = store.get_hook(&id).await.unwrap();
    assert_eq!(got.state, HookState::Expired);
    assert_eq!(got.next_run_at, None);

    store
        .update_hook_next_run(&id, None)
        .await
        .expect("clear next run");
    assert_eq!(store.get_hook(&id).await.unwrap().next_run_at, None);

    store
        .update_hook_last_error(&id, Some("timeout after 5000ms"))
        .await
        .expect("set last error");
    store
        .update_hook_last_logs(&id, Some("checked 3 PRs\nall green"))
        .await
        .expect("set last logs");
    store
        .update_hook_last_state(&id, Some("{\"seen\":3}"))
        .await
        .expect("set last state");
    store
        .update_hook_state(&id, HookState::Evicted)
        .await
        .expect("running -> evicted");
    let got = store.get_hook(&id).await.unwrap();
    assert_eq!(got.state, HookState::Evicted);
    assert_eq!(got.last_error.as_deref(), Some("timeout after 5000ms"));
    assert_eq!(got.last_logs.as_deref(), Some("checked 3 PRs\nall green"));
    assert_eq!(got.last_state.as_deref(), Some("{\"seen\":3}"));

    store
        .update_hook_last_logs(&id, None)
        .await
        .expect("clear last logs");
    assert_eq!(store.get_hook(&id).await.unwrap().last_logs, None);

    store
        .update_hook_last_state(&id, None)
        .await
        .expect("clear last state");
    assert_eq!(store.get_hook(&id).await.unwrap().last_state, None);

    let missing = HookId("hook-missing".to_string());
    for err in [
        store
            .update_hook_state(&missing, HookState::Cancelled)
            .await
            .expect_err("state"),
        store
            .update_hook_run(&missing, &ran_at, None)
            .await
            .expect_err("run"),
        store
            .update_hook_next_run(&missing, None)
            .await
            .expect_err("next run"),
        store.expire_hook(&missing).await.expect_err("expire"),
        store
            .update_hook_last_error(&missing, None)
            .await
            .expect_err("last error"),
        store
            .update_hook_last_logs(&missing, None)
            .await
            .expect_err("last logs"),
        store
            .update_hook_last_state(&missing, None)
            .await
            .expect_err("last state"),
        store.delete_hook(&missing).await.expect_err("delete"),
    ] {
        assert!(matches!(err, Error::NotFound(_)), "got {err:?}");
    }
}

/// `load_active_hooks` returns only `scheduled`/`running` rows (the boot
/// rehydration read), and `delete_hook` removes a row.
#[tokio::test]
async fn hook_load_active_and_delete() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let (ws, agent) = seed_hook_owner(&store).await;

    let states = [
        ("hook-sched", HookState::Scheduled),
        ("hook-run", HookState::Running),
        ("hook-disp", HookState::Dispatched),
        ("hook-evic", HookState::Evicted),
        ("hook-canc", HookState::Cancelled),
    ];
    for (id, state) in &states {
        let hook_id = HookId(id.to_string());
        store
            .insert_hook(&sample_hook(&hook_id, &ws, &agent, id))
            .await
            .expect("insert");
        store
            .update_hook_state(&hook_id, *state)
            .await
            .expect("set state");
    }

    let active = store.load_active_hooks().await.expect("load active");
    let mut ids: Vec<&str> = active.iter().map(|h| h.hook_id.0.as_str()).collect();
    ids.sort_unstable();
    assert_eq!(ids, vec!["hook-run", "hook-sched"]);

    store
        .delete_hook(&HookId("hook-sched".to_string()))
        .await
        .expect("delete");
    let active = store.load_active_hooks().await.expect("reload active");
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].hook_id.0, "hook-run");
}

/// Agent-flipped completion rows: dedup by primary key, oldest-first eviction
/// at the per-agent cap, cross-agent removal by task, and survival across a
/// store reopen (daemon restart).
#[tokio::test]
async fn agent_flipped_completion_record_dedup_cap_remove_and_reopen() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws, "WS", false))
        .await
        .expect("insert ws");
    let agent_a = AgentId::from("agent-flip-a");
    let agent_b = AgentId::from("agent-flip-b");
    for id in [&agent_a, &agent_b] {
        store
            .insert_agent_session(&sample_agent_session(id, &ws))
            .await
            .expect("insert session");
    }

    // Dedup: re-recording the same pair keeps one row.
    let first = NoteId::from("task-first");
    store
        .record_agent_flipped_completion(&agent_a, &ws, &first, "2026-01-01T00:00:00Z")
        .await
        .expect("record");
    store
        .record_agent_flipped_completion(&agent_a, &ws, &first, "2026-01-01T00:00:01Z")
        .await
        .expect("re-record");
    assert_eq!(
        store
            .list_agent_flipped_completions(&agent_a)
            .await
            .expect("list"),
        vec![(ws.clone(), first.clone())]
    );

    // Cap: recording CAP more pairs evicts the oldest (`first`), leaving
    // exactly CAP rows with the newest present.
    for i in 0..crate::AGENT_FLIPPED_COMPLETIONS_CAP {
        store
            .record_agent_flipped_completion(
                &agent_a,
                &ws,
                &NoteId::from(format!("task-{i:02}")),
                &format!("2026-01-01T00:01:{i:02}Z"),
            )
            .await
            .expect("record capped");
    }
    let listed = store
        .list_agent_flipped_completions(&agent_a)
        .await
        .expect("list capped");
    assert_eq!(
        listed.len(),
        usize::try_from(crate::AGENT_FLIPPED_COMPLETIONS_CAP).expect("value fits in usize")
    );
    assert!(
        !listed.iter().any(|(_, n)| n == &first),
        "oldest row evicted at the cap"
    );
    assert_eq!(listed.last().unwrap().1, NoteId::from("task-49"));

    // Removal by task deletes the pair for EVERY recording agent.
    let shared = NoteId::from("task-00");
    store
        .record_agent_flipped_completion(&agent_b, &ws, &shared, "2026-01-01T00:02:00Z")
        .await
        .expect("record b");
    store
        .remove_agent_flipped_completions_for_task(&ws, &shared)
        .await
        .expect("remove");
    assert!(!store
        .list_agent_flipped_completions(&agent_a)
        .await
        .expect("list a")
        .iter()
        .any(|(_, n)| n == &shared));
    assert!(store
        .list_agent_flipped_completions(&agent_b)
        .await
        .expect("list b")
        .is_empty());

    // Reopen the store from the same path: rows persist across a restart.
    drop(store);
    let reopened = Store::open(&tmp.path).await.expect("reopen store");
    let listed = reopened
        .list_agent_flipped_completions(&agent_a)
        .await
        .expect("list after reopen");
    assert_eq!(
        listed.len(),
        usize::try_from(crate::AGENT_FLIPPED_COMPLETIONS_CAP).expect("value fits in usize") - 1
    );

    // Take (consume-on-stamp read): returns the rows oldest-first and
    // clears them — a second take is empty. Empty take is a no-op.
    let taken = reopened
        .take_agent_flipped_completions(&agent_a)
        .await
        .expect("take");
    assert_eq!(taken, listed);
    assert!(reopened
        .list_agent_flipped_completions(&agent_a)
        .await
        .expect("list after take")
        .is_empty());
    assert!(reopened
        .take_agent_flipped_completions(&agent_a)
        .await
        .expect("second take")
        .is_empty());

    // Deleting the recording agent's session cascades its flip rows via the
    // FK; other agents' rows are untouched.
    for (agent, note) in [(&agent_a, "task-keep"), (&agent_b, "task-doomed")] {
        reopened
            .record_agent_flipped_completion(
                agent,
                &ws,
                &NoteId::from(note),
                "2026-01-01T00:03:00Z",
            )
            .await
            .expect("record for cascade");
    }
    assert!(reopened
        .delete_agent_session(&ws, &agent_b)
        .await
        .expect("delete session"));
    assert!(reopened
        .list_agent_flipped_completions(&agent_b)
        .await
        .expect("list b after cascade")
        .is_empty());
    assert_eq!(
        reopened
            .list_agent_flipped_completions(&agent_a)
            .await
            .expect("list a after cascade"),
        vec![(ws.clone(), NoteId::from("task-keep"))]
    );
}
