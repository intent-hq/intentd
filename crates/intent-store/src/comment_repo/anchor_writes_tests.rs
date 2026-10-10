use super::*;
use serde_json::json;
use sqlx::SqliteConnection;

async fn fixture() -> (tempfile::TempDir, Store, WorkspaceId) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    for ws in ["owner", "keeper"] {
        sqlx::query("INSERT INTO workspace(id,title,branch,created_at,updated_at) VALUES(?,?,'main','t0','t0')")
            .bind(ws).bind(ws).execute(store.write_pool()).await.unwrap();
        for note in ["a", "b"] {
            sqlx::query("INSERT INTO note(id,workspace_id,title,content,created_at,updated_at) VALUES(?,?,'N','😀<!--anchor:x:start-->hello<!--anchor:x:end--><!--anchor:y:point-->','t0','t0')")
                .bind(note).bind(ws).execute(store.write_pool()).await.unwrap();
        }
    }
    let mut tx = store
        .write_pool()
        .begin_with("BEGIN IMMEDIATE")
        .await
        .unwrap();
    crate::note_page_index::rebuild_pending(&mut tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    (dir, store, WorkspaceId("owner".into()))
}

fn comment(id: &str, note: Option<&str>) -> Comment {
    serde_json::from_value(json!({"id":id,"threadId":"thread","noteId":note,
        "type":"comment","content":"Original","author":"Author","authorType":"user",
        "status":"open","createdAt":"t0","updatedAt":"t0"}))
    .unwrap()
}

async fn anchors(store: &Store, ws: &WorkspaceId, note: &str) -> i64 {
    assert!(
        store
            .note_annotation_epochs(ws, &NoteId(note.into()))
            .await
            .unwrap()
            .anchors_ready
    );
    sqlx::query_scalar("SELECT COUNT(*) FROM note_comment_anchor a JOIN note_annotation_head h ON h.id=a.head_id WHERE h.workspace_id=? AND h.note_id=?")
        .bind(ws.as_str()).bind(note).fetch_one(store.read_pool()).await.unwrap()
}

type CommentSnapshot = (String, Option<String>, String, String, Option<String>);

async fn snapshot(conn: &mut SqliteConnection) -> Value {
    let comments: Vec<CommentSnapshot> =
        sqlx::query_as("SELECT id,note_id,content,status,extra_json FROM comment ORDER BY id")
            .fetch_all(&mut *conn)
            .await
            .unwrap();
    let heads: Vec<(i64, String, i64, i64, i64)> = sqlx::query_as(
        "SELECT id,comment_revision,anchors_rev,thread_count,comment_count FROM note_annotation_head ORDER BY id",
    ).fetch_all(&mut *conn).await.unwrap();
    let anchors: Vec<(i64, i64, String, i64, i64)> = sqlx::query_as(
        "SELECT id,head_id,comment_id,start,end FROM note_comment_anchor ORDER BY id",
    )
    .fetch_all(&mut *conn)
    .await
    .unwrap();
    let covers: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM note_comment_anchor_cover")
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    json!({"comments":comments,"heads":heads,"anchors":anchors,"coverCount":covers})
}

#[tokio::test]
async fn crud_move_thread_status_preserve_scopes_extras_and_creation_identity() {
    let (_dir, store, ws) = fixture().await;
    let mut x = comment("x", Some("a"));
    x.author_principal_id = Some(intent_core::PrincipalId("original-principal".into()));
    let extras = json!({"legacy":{"kept":true},"isOrphaned":"true"});
    insert(&store, &ws, &x, extras.as_object().unwrap())
        .await
        .unwrap();
    assert_eq!(anchors(&store, &ws, "a").await, 1);
    x.note_id = Some(NoteId("b".into()));
    x.content = "Edited".into();
    x.author_principal_id = Some(intent_core::PrincipalId("forged-principal".into()));
    x.created_at = "forged-created".into();
    update(&store, &ws, &x).await.unwrap();
    assert_eq!(anchors(&store, &ws, "a").await, 0);
    assert_eq!(anchors(&store, &ws, "b").await, 1);
    let stored = store.get_comment("x").await.unwrap();
    assert_eq!(stored.created_at, "t0");
    assert_eq!(
        stored.author_principal_id.unwrap().as_str(),
        "original-principal"
    );
    assert_eq!(stored.is_orphaned, None);
    let extra: String = sqlx::query_scalar("SELECT extra_json FROM comment WHERE id='x'")
        .fetch_one(store.read_pool())
        .await
        .unwrap();
    let extra: Value = serde_json::from_str(&extra).unwrap();
    assert_eq!(extra["legacy"], json!({"kept":true}));
    assert_eq!(extra["isOrphaned"], "true");
    insert(&store, &ws, &comment("y", Some("a")), &Map::new())
        .await
        .unwrap();
    assert_eq!(
        set_status(&store, &ws, "thread", CommentStatus::Resolved, "t1")
            .await
            .unwrap(),
        2
    );
    assert_eq!(anchors(&store, &ws, "a").await, 1);
    assert_eq!(anchors(&store, &ws, "b").await, 1);
    assert_eq!(
        set_status(&store, &ws, "absent", CommentStatus::Open, "t2")
            .await
            .unwrap(),
        0
    );
    assert!(matches!(
        delete_in_note(&store, &ws, &NoteId("a".into()), "x").await,
        Err(Error::NotFound(_))
    ));
    assert!(matches!(
        delete(&store, &WorkspaceId("keeper".into()), "x").await,
        Err(Error::NotFound(_))
    ));
    assert_eq!(
        delete_in_note(&store, &ws, &NoteId("b".into()), "x")
            .await
            .unwrap(),
        "thread"
    );
    assert_eq!(anchors(&store, &ws, "b").await, 0);
    delete(&store, &ws, "y").await.unwrap();
    assert_eq!(anchors(&store, &ws, "a").await, 0);
    insert(&store, &ws, &comment("unscoped", None), &Map::new())
        .await
        .unwrap();
    delete(&store, &ws, "unscoped").await.unwrap();
}

#[tokio::test]
async fn retirement_rejects_delete_and_none_note_paths_without_mutation() {
    let (_dir, store, ws) = fixture().await;
    let x = comment("x", Some("a"));
    let unscoped = comment("unscoped", None);
    insert(&store, &ws, &x, &Map::new()).await.unwrap();
    insert(&store, &ws, &unscoped, &Map::new()).await.unwrap();
    sqlx::query("INSERT INTO note_annotation_workspace_retirement VALUES('owner')")
        .execute(store.write_pool())
        .await
        .unwrap();
    let before = snapshot(&mut store.read_pool().acquire().await.unwrap()).await;
    assert!(matches!(
        delete(&store, &ws, "unscoped").await,
        Err(Error::NotFound(_))
    ));
    assert!(matches!(
        delete_in_note(&store, &ws, &NoteId("a".into()), "x").await,
        Err(Error::NotFound(_))
    ));
    assert!(matches!(
        update(&store, &ws, &unscoped).await,
        Err(Error::NotFound(_))
    ));
    assert!(matches!(
        insert(&store, &ws, &comment("other", None), &Map::new()).await,
        Err(Error::NotFound(_))
    ));
    assert!(matches!(
        set_status(&store, &ws, "thread", CommentStatus::Resolved, "t1").await,
        Err(Error::NotFound(_))
    ));
    assert_eq!(
        snapshot(&mut store.read_pool().acquire().await.unwrap()).await,
        before
    );
}

#[tokio::test]
async fn later_note_finalizer_failure_rolls_back_all_thread_changes() {
    let (_dir, store, ws) = fixture().await;
    insert(&store, &ws, &comment("x", Some("a")), &Map::new())
        .await
        .unwrap();
    insert(&store, &ws, &comment("y", Some("b")), &Map::new())
        .await
        .unwrap();
    // First note can publish, second cannot. All status/epoch/index changes
    // must still disappear when the helper returns its finalizer error.
    sqlx::query(
        "UPDATE note_page_head SET indexed_rev=-1 WHERE workspace_id='owner' AND note_id='b'",
    )
    .execute(store.write_pool())
    .await
    .unwrap();
    let before = snapshot(&mut store.read_pool().acquire().await.unwrap()).await;
    assert!(
        set_status(&store, &ws, "thread", CommentStatus::Resolved, "t1")
            .await
            .is_err()
    );
    let mut barrier = store
        .write_pool()
        .begin_with("BEGIN IMMEDIATE")
        .await
        .unwrap();
    assert_eq!(snapshot(&mut barrier).await, before);
    barrier.rollback().await.unwrap();
}

#[tokio::test]
async fn dropping_changed_writer_before_finalization_rolls_back_then_retry_commits() {
    let (_dir, store, ws) = fixture().await;
    let mut x = comment("x", Some("a"));
    insert(&store, &ws, &x, &Map::new()).await.unwrap();
    let before = snapshot(&mut store.read_pool().acquire().await.unwrap()).await;
    x.note_id = Some(NoteId("b".into()));
    x.content = "Changed inside cancelled writer".into();
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    let mut pending = Box::pin(AFTER_MUTATION.scope(sender, update(&store, &ws, &x)));
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        tokio::select! {
            reached = receiver.recv() => assert_eq!(reached, Some(())),
            result = &mut pending => panic!("writer escaped mutation barrier: {result:?}"),
        }
    })
    .await
    .unwrap();
    drop(pending);
    let mut barrier = store
        .write_pool()
        .begin_with("BEGIN IMMEDIATE")
        .await
        .unwrap();
    assert_eq!(snapshot(&mut barrier).await, before);
    barrier.rollback().await.unwrap();
    update(&store, &ws, &x).await.unwrap();
    assert_eq!(anchors(&store, &ws, "a").await, 0);
    assert_eq!(anchors(&store, &ws, "b").await, 1);
    assert_eq!(store.get_comment("x").await.unwrap().content, x.content);
}
