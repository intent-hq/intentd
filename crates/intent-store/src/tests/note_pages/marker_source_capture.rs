//! Explicit fresh lexical capture. The frontend supplies native capture later.
use super::{record_source_window_closure, setup};
use serde_json::{Value, json};
use std::io::Write as _;

#[tokio::test]
#[ignore = "requires coordinated fresh marker producer and a new transcript directory"]
async fn capture_original_marker_source_with_owned_root_and_complete_context() {
    let directory = std::path::PathBuf::from(std::env::var("NOTE_PAGE_TRANSCRIPT_DIR").unwrap());
    assert!(directory.is_dir());
    let output = directory.join("marker-source.json");
    let database = directory.join("marker-source.db");
    assert!(
        !output.exists() && !database.exists(),
        "never overwrite capture evidence"
    );
    let root_id = "11111111-1111-4111-8111-111111111111";
    let literal = format!("<!--anchor:{root_id}:point-->");
    let paragraph = format!("A {literal} B");
    let source = format!("prefix😀\n\n{paragraph}");
    assert_eq!(source.encode_utf16().count(), 70);
    assert_eq!(literal.encode_utf16().count(), 56);
    // setup uses ordinary Store::insert_workspace/insert_note, not SQL identity seeds.
    let (store, _temporary, note) = setup(&source).await;
    let root: intent_core::Comment = serde_json::from_value(json!({"id":root_id,"threadId":root_id,"noteId":note.id,
        "type":"comment","content":"Original marker root","author":"Alice","authorType":"user","status":"open",
        "createdAt":note.created_at,"updatedAt":note.updated_at})).unwrap();
    store
        .insert_comment(&note.workspace_id, &root)
        .await
        .unwrap();
    let retained_root = store.get_comment(root_id).await.unwrap();
    assert_eq!(retained_root.id, root_id);
    assert_eq!(retained_root.note_id.as_ref(), Some(&note.id));
    assert!(retained_root.parent_id.is_none());
    assert_ne!(retained_root.is_orphaned, Some(true));
    let epochs = store
        .note_annotation_epochs(&note.workspace_id, &note.id)
        .await
        .unwrap();
    assert!(epochs.anchors_ready);
    let epoch_json:String=sqlx::query_scalar("SELECT json_object('headId',a.id,'sourceRev',a.source_rev,'commentRevision',a.comment_revision,'anchorsRev',a.anchors_rev,'instanceId',p.instance_id,'sourceRevision',s.source_revision,'stateGeneration',s.state_generation,'deleted',s.deleted) FROM note_annotation_head a JOIN note_page_head p USING(workspace_id,note_id) JOIN note_annotation_state s USING(workspace_id,note_id,instance_id) WHERE a.workspace_id=? AND a.note_id=?")
        .bind(note.workspace_id.as_str()).bind(note.id.as_str()).fetch_one(store.read_pool()).await.unwrap();
    let captured_at_ms = intent_core::now_epoch_ms();
    let mut calls = Vec::new();
    let first = record_source_window_closure(
        &store,
        json!({"kind":"source","at":10,"maxSourceBytes":4096,"maxWireBytes":8192,"maxItems":64}),
        &mut calls,
    )
    .await;
    assert_eq!(first["text"], paragraph);
    assert_eq!(first["range"], json!({"start":10,"end":70}));
    assert_eq!(first["sourceLength"], 70);
    assert!(first["nextCursor"].is_null());
    assert!(calls.len() > 1, "actual context/resource closure retained");
    assert_eq!(
        store
            .get_note(&note.workspace_id, &note.id)
            .await
            .unwrap()
            .content,
        source
    );
    sqlx::query("VACUUM INTO ?")
        .bind(database.to_str().unwrap())
        .execute(store.write_pool())
        .await
        .unwrap();
    let artifact = json!({"backendHead":std::env::var("NOTE_PAGE_CAPTURE_HEAD").unwrap(),
        "capturedAtMs":captured_at_ms,"source":source,"at":10,"workspaceId":"pages","noteId":"spec","principal":"alice","rpcId":1,
        "requestedCommentId":root_id,"commentId":retained_root.id,"originalRoot":retained_root,
        "internalStoreOracle":{"annotation":serde_json::from_str::<Value>(&epoch_json).unwrap()},
        "expectedGeometry":{"parentRange":{"start":10,"end":70},"literalRange":{"start":12,"end":68},"sourceLength":70},
        "calls":calls,"retainedDatabase":database,
        "claim":"actual ordinary Store note/root writers and unchanged lexical source/context closure; native marker capture is separate"});
    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(output)
        .unwrap()
        .write_all(&serde_json::to_vec_pretty(&artifact).unwrap())
        .unwrap();
}
