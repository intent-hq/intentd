//! Unit tests: open a temp SQLite DB, run migrations, and round-trip
//! workspaces and notes including the `include_archived` filter.

use std::path::PathBuf;

use intent_core::{
    events, now_iso, ActorType, AgentId, AgentSession, AgentStatus, AuthorType, ClientId, Comment,
    CommentAnchor, CommentAnchorType, CommentStatus, CommentType, ContentType, Error, EventActor,
    Note, NoteId, NoteMetadata, NoteVersionAuthor, NoteVisibility, TaskMetadata, TaskStatus,
    Workspace, WorkspaceActivity, WorkspaceAttention, WorkspaceId, WorkspaceStatus,
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
        checkout_mode: None,
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
            47, 48, 49, 50, 51, 52, 53, 54
        ]
    );
    assert_eq!(
        status.applied,
        vec![
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
            25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46,
            47, 48, 49, 50, 51, 52, 53, 54
        ]
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
    assert_eq!(versions.len(), MAX_NOTE_VERSIONS as usize);
    assert_eq!(versions.first().map(|e| e.v), Some(6), "oldest 5 pruned");
    assert_eq!(versions.last().map(|e| e.v), Some(total));
    assert!(versions.iter().all(|e| e.entry_type == "snapshot"));
    assert_eq!(
        versions.last().map(|e| e.content_length),
        Some(note.content.len() as i64)
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
/// (max_connections=1) is not returned to the pool still holding an open
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
    assert_eq!(tasks[0].metadata.task, Some(meta));
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
/// workspace A. Bare-id probes surface as NotFound / zero-row updates
/// depending on the mutation shape (mirrors the note_repo 0022 pattern).
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

    // recent_files: newest first, scoped to workspace, only file:changed.
    let recent = store.recent_files(&ws, 10).await.expect("recent");
    assert_eq!(recent.len(), 2);
    assert_eq!(recent[0].timestamp, "2026-01-01T00:00:02Z");
    assert_eq!(recent[1].timestamp, "2026-01-01T00:00:01Z");

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

    // directory_changes: prefix filter on data.path.
    let in_src = store
        .directory_changes(&ws, "src/", 10)
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
/// the transaction back so the sole write-pool connection (max_connections=1)
/// is not returned to the pool still holding an open transaction + write
/// lock. The `event` table has no FK on `workspace_id`, so unlike the
/// note_version variant this test plants a trap: a trigger on `event`
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
        typed_event(&ws, old, events::AGENT_STREAM_CHUNK, agent.clone()),
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
        typed_event(&ws, new, events::AGENT_STREAM_CHUNK, agent.clone()),
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
        events::AGENT_STREAM_CHUNK,
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
        typed_event(&ws, old, events::AGENT_STREAM_CHUNK, agent.clone()),
        typed_event(&ws, old, events::AGENT_STREAM_END, agent.clone()),
        // New stream chunk (within TTL — must survive).
        typed_event(&ws, new, events::AGENT_STREAM_CHUNK, agent.clone()),
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
/// asserts that a fresh pool has `synchronous = NORMAL` (2 in SQLite's integer
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
            events::AGENT_STREAM_CHUNK,
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
    let total_old = crate::event_repo::RETENTION_DELETE_CHUNK as usize * 2 + 50;
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
/// release them (freelist shrinks, logical page_count drops).
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
        freelist_after_delete as u64,
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
/// without auto_vacuum stays in NONE mode when reopened through `Store::open`
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
        id: id.clone(),
        workspace_id: ws.clone(),
        parent_agent_id: None,
        backend_session_id: None,
        acp_session_id: None,
        name: "Builder".to_string(),
        name_explicitly_set: false,
        model: Some("opus".to_string()),
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
        delegation_depth: None,
        initial_message: None,
        context_references: None,
        image_blocks: None,
        is_background: false,
        metadata: None,
        stop_reason: None,
        created_at: ts.clone(),
        updated_at: ts,
        sandbox_id: None,
        sandbox_path: None,
        sandbox_branch: None,
    }
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

/// `set_agent_session_status` stop_reason parameter: `None` leaves the column
/// untouched; `Some(None)` clears it to NULL; `Some(Some(reason))` sets the
/// new value. Exercises the three-way encoding for set/clear/leave-unchanged
/// across a status update.
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
    assert_eq!(loaded.status, AgentStatus::Pending);

    // update_agent_session also persists stop_reason.
    let mut updated = loaded.clone();
    updated.stop_reason = Some("max_turns".to_string());
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
/// eliminates SQLITE_BUSY (code 5) errors under heavy concurrent load.
/// Spawns ~50 concurrent writes + concurrent reads; all must succeed without
/// busy_timeout errors, and reads must stay fast while writes are in flight.
#[tokio::test]
async fn concurrent_writes_no_sqlite_busy() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.unwrap();

    // Spawn 50 concurrent single-row writes
    let write_handles: Vec<_> = (0..50)
        .map(|i| {
            let store = store.clone();
            tokio::spawn(async move {
                let ws_id = WorkspaceId::new();
                let ts = now_iso();
                let workspace = Workspace {
                    id: ws_id.clone(),
                    title: format!("Workspace {}", i),
                    branch: format!("main-{}", i),
                    base_ref: None,
                    base_commit_sha: None,
                    status: WorkspaceStatus::Active,
                    status_message: None,
                    activity: WorkspaceActivity::Idle,
                    attention: WorkspaceAttention::Unread,
                    created_at: ts.clone(),
                    updated_at: ts.clone(),
                    last_activity: None,
                    tags: vec![],
                    path: Some(format!("/tmp/ws-{}", i)),
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
                    checkout_mode: None,
                };
                store.insert_workspace(&workspace).await
            })
        })
        .collect();

    // Spawn 50 concurrent reads (list workspaces) and time them
    let read_start = std::time::Instant::now();
    let read_handles: Vec<_> = (0..50)
        .map(|_| {
            let store = store.clone();
            tokio::spawn(async move { store.list_workspaces(false).await.map(|ws| ws.len()) })
        })
        .collect();

    // All writes must succeed (no SQLITE_BUSY errors)
    for handle in write_handles {
        let result = handle.await.unwrap();
        assert!(
            result.is_ok(),
            "Write failed (SQLITE_BUSY would be Error::Internal with 'database is locked'): {:?}",
            result
        );
    }

    // All reads must succeed and stay fast (complete in < 5s even with writes in flight)
    for handle in read_handles {
        let result = handle.await.unwrap();
        assert!(result.is_ok(), "Read failed: {:?}", result);
    }
    let read_elapsed = read_start.elapsed();
    assert!(
        read_elapsed.as_secs() < 5,
        "Reads took too long: {:?} (writes blocking readers?)",
        read_elapsed
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
/// PRAGMA wal_checkpoint(PASSIVE) via the write pool.
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

/// Smoke test: verify the two-pool split sizing (write pool max_connections=1,
/// read pool max_connections=32) and that both pools support basic queries.
/// The write pool is single-connection to serialize all mutations and eliminate
/// in-process writer-vs-writer busy_timeout contention. The read pool size (32)
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
    // Matches production, where the row id is the queued message id.
    let id = uuid::Uuid::new_v4().to_string();
    AgentQueueRow {
        id: id.clone(),
        agent_id: agent_id.clone(),
        position,
        payload: json!({
            "id": id,
            "content": content,
            "queuedAt": now_iso(),
            "editing": false,
            "persisted": true,
            "requeuedAfterFailure": false,
            "messageMetadata": { "source": "test" },
        }),
        created_at: now_iso(),
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
    assert_eq!(loaded[1].position, 1);
    assert_eq!(loaded[1].payload["content"], "second");

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
