use super::*;
use crate::Store;
use intent_core::{
    note_stage::{
        NoteStageAppend, NoteStageBegin, NoteStageManifestEntry, NoteStageSeal, NoteStageStream,
        NOTE_STAGE_STREAMS,
    },
    ContentType, Note, NoteId, NoteMetadata, NoteVisibility, WorkspaceId,
};
use sha2::{Digest, Sha256};
use std::fmt::Write as _;

struct Fixture {
    store: Store,
    _tmp: tempfile::TempDir,
    note: Note,
    begin: NoteStageBegin,
    operation: String,
    length: u64,
}
fn resource(value: &Value) -> String {
    intent_core::note_artifact::canonical::canonical_json(&value.to_string()).unwrap()
}
fn reference(id: &str, text: &str) -> Value {
    let hash = Sha256::digest(text.as_bytes()).iter().fold(
        String::with_capacity(64),
        |mut output, byte| {
            write!(output, "{byte:02x}").expect("writing to a String");
            output
        },
    );
    json!({"textId":id,"length":text.encode_utf16().count(),"utf8Bytes":text.len(),"sha256":hash})
}
// Real Store bootstrap, original upload bytes, public begin/append/seal. Only
// workspace setup is SQL; no manually asserted digest cache or graph authority.
async fn fixture(prefix: &str, text: &str, start: u64, end: u64, attributes: bool) -> Fixture {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open(&tmp.path().join("selection.db")).await.unwrap();
    let now = intent_core::now_iso();
    sqlx::query("INSERT INTO workspace(id,title,branch,status,created_at,updated_at) VALUES('ws','Selection','test','Active',?,?)")
        .bind(&now).bind(&now).execute(store.write_pool()).await.unwrap();
    let note = Note {
        id: NoteId::from("note"),
        workspace_id: WorkspaceId::from("ws"),
        title: "Selection".into(),
        content: format!("{prefix}{text}\ntrailing"),
        content_type: ContentType::Markdown,
        tags: vec![],
        is_pinned: false,
        is_archived: false,
        is_default: false,
        parent_id: None,
        visibility: NoteVisibility::Workspace,
        metadata: NoteMetadata::default(),
        created_at: now.clone(),
        updated_at: now,
        rev: 0,
    };
    store.insert_note(&note).await.unwrap();
    let page = store
        .read_note_page(
            "ws",
            "note",
            "alice",
            serde_json::from_value(json!({"kind":"source","maxSourceBytes":64})).unwrap(),
            &json!(1),
        )
        .await
        .unwrap();
    let mut value = page["scope"].clone();
    value["operationId"] = json!(uuid::Uuid::new_v4().to_string());
    value["expiresAt"] = json!(format!(
        "{}.000Z",
        &intent_core::iso_ms_from_now(60_000)[..19]
    ));
    value["headerDigest"] = json!("0".repeat(64));
    value["header"] = json!({"baseRevision":page["sourceRevision"],"editorSessionId":"selection","localEditSequence":0,"liveGeneration":1,"selectionGeneration":1,"action":"read","output":"selectionMarkdown","selection":"ranges"});
    let mut begin: NoteStageBegin = serde_json::from_value(value).unwrap();
    begin.header_digest = begin.computed_digest().unwrap();
    store.begin_note_stage("alice", &begin).await.unwrap();
    let base = u64::try_from(prefix.encode_utf16().count()).unwrap();
    let units = u64::try_from(text.encode_utf16().count()).unwrap();
    let mut paragraph = json!({"version":1,"nodeType":"paragraph","parentOrdinal":null,"nativeRange":{"from":0,"to":units+2}});
    let mut inline = json!({"version":1,"nodeType":"text","parentOrdinal":0,"nativeRange":{"from":1+start,"to":1+end}});
    if attributes {
        paragraph["attributesRef"] = json!("attrs");
        inline["attributesRef"] = json!("attrs");
    }
    let paragraph = resource(&paragraph);
    let inline = resource(&inline);
    let attrs = resource(
        &json!({"id":"attrs-entry","parentId":null,"type":"object","childrenRef":"empty-directory"}),
    );
    let directory = resource(&json!({"kind":"metadataChildren","items":[],"nextRef":null}));
    let mut manifest = Vec::new();
    for stream in NOTE_STAGE_STREAMS {
        let records = match stream {
            NoteStageStream::Text => vec![
                json!({"kind":"text","id":"paragraph","offset":0,"text":paragraph}),
                json!({"kind":"text","id":"inline","offset":0,"text":inline}),
                json!({"kind":"text","id":"attrs","offset":0,"text":attrs}),
                json!({"kind":"text","id":"empty-directory","offset":0,"text":directory}),
            ],
            NoteStageStream::Selection => vec![
                json!({"kind":"range","ordinal":0,"start":base+start,"end":base+end,"direction":"backward","anchorAffinity":"after","headAffinity":"before"}),
            ],
            NoteStageStream::Live => vec![
                json!({"kind":"projection","ordinal":0,"sourceRange":{"start":base,"end":base+units},"role":"selection-owner","detail":reference("paragraph",&paragraph)}),
                json!({"kind":"projection","ordinal":1,"sourceRange":{"start":base+start,"end":base+end},"role":"inline-span","detail":reference("inline",&inline)}),
            ],
            _ => vec![],
        };
        let count = u64::try_from(records.len()).unwrap();
        let digest = if records.is_empty() {
            None
        } else {
            let mut append = NoteStageAppend {
                backend_id: begin.backend_id.clone(),
                workspace_id: begin.workspace_id.clone(),
                note_id: begin.note_id.clone(),
                note_instance_id: begin.note_instance_id.clone(),
                operation_id: begin.operation_id.clone(),
                header_digest: begin.header_digest.clone(),
                stream,
                sequence: 0,
                previous_digest: None,
                records,
                chunk_digest: String::new(),
            };
            append.chunk_digest = append.computed_digest().unwrap();
            store.append_note_stage("alice", &append).await.unwrap();
            Some(append.chunk_digest)
        };
        manifest.push(NoteStageManifestEntry {
            stream,
            chunks: u64::from(digest.is_some()),
            records: count,
            last_digest: digest,
        });
    }
    let mut seal = NoteStageSeal {
        backend_id: begin.backend_id.clone(),
        workspace_id: begin.workspace_id.clone(),
        note_id: begin.note_id.clone(),
        note_instance_id: begin.note_instance_id.clone(),
        operation_id: begin.operation_id.clone(),
        header_digest: begin.header_digest.clone(),
        manifest,
        payload_digest: String::new(),
    };
    seal.payload_digest = seal.computed_digest().unwrap();
    store.seal_note_stage("alice", &seal).await.unwrap();
    let operation =
        sqlx::query_scalar("SELECT operation_key FROM note_operation WHERE operation_id=?")
            .bind(&begin.operation_id)
            .fetch_one(store.read_pool())
            .await
            .unwrap();
    let length = u64::try_from(note.content.encode_utf16().count()).unwrap();
    Fixture {
        store,
        _tmp: tmp,
        note,
        begin,
        operation,
        length,
    }
}
async fn output(f: &Fixture) -> Result<String> {
    let mut tx = f
        .store
        .write_pool()
        .begin_with("BEGIN IMMEDIATE")
        .await
        .unwrap();
    prepare(&mut tx, &f.operation, &f.begin.header, 0, f.length).await
}

#[tokio::test]
async fn selection_output_uses_real_sealed_resources_and_frozen_paragraph_across_pieces() {
    let mut f = fixture(&"😀".repeat(1023), &"a".repeat(4096), 4090, 4096, true).await;
    f.note.content = "remote replacement".into();
    f.store.update_note(&f.note).await.unwrap();
    assert_eq!(output(&f).await.unwrap(), "aaaaaa");
    let f = fixture("prefix\n", "one two", 3, 7, true).await;
    assert_eq!(output(&f).await.unwrap(), "two");
}

#[tokio::test]
async fn selection_output_rejects_defaults_unsupported_text_and_internal_no_copy() {
    for (text, start, end, attrs) in [
        ("abc", 0, 3, false),
        ("a b", 1, 2, true),
        ("abc", 1, 1, true),
        ("a*b", 0, 3, true),
        ("a😀b", 0, 1, true),
    ] {
        let f = fixture("", text, start, end, attrs).await;
        assert!(matches!(output(&f).await, Err(Error::Unsupported(_))));
    }
}

#[tokio::test]
async fn selection_output_requires_exact_header_view_and_stream_closure() {
    let f = fixture("", "abc", 0, 3, true).await;
    let mut tx = f
        .store
        .write_pool()
        .begin_with("BEGIN IMMEDIATE")
        .await
        .unwrap();
    assert!(prepare(&mut tx, &f.operation, &f.begin.header, 1, f.length)
        .await
        .is_err());
    assert!(
        prepare(&mut tx, &f.operation, &f.begin.header, 0, f.length + 1)
            .await
            .is_err()
    );
    assert!(prepare(&mut tx, "foreign", &f.begin.header, 0, f.length)
        .await
        .is_err());
    let mut header = f.begin.header.clone();
    header.selection_generation += 1;
    assert!(prepare(&mut tx, &f.operation, &header, 0, f.length)
        .await
        .is_err());
    sqlx::query("UPDATE note_stage_stream SET records=3 WHERE operation_key=? AND stream='live'")
        .bind(&f.operation)
        .execute(&mut *tx)
        .await
        .unwrap();
    assert!(prepare(&mut tx, &f.operation, &f.begin.header, 0, f.length)
        .await
        .is_err());
    tx.rollback().await.unwrap();
    let mut tx = f
        .store
        .write_pool()
        .begin_with("BEGIN IMMEDIATE")
        .await
        .unwrap();
    sqlx::query("INSERT INTO note_stage_record SELECT operation_key,stream,chunk_sequence,2,value FROM note_stage_record WHERE operation_key=? AND stream='live' AND ordinal=1").bind(&f.operation).execute(&mut *tx).await.unwrap();
    assert!(prepare(&mut tx, &f.operation, &f.begin.header, 0, f.length)
        .await
        .is_err());
    tx.rollback().await.unwrap();
    assert_eq!(output(&f).await.unwrap(), "abc");
}

#[tokio::test]
async fn selection_output_rejects_changed_bytes_missing_verified_ownership_and_wrong_generation() {
    let f = fixture("", "abc", 0, 3, true).await;
    for sql in [
        "UPDATE note_stage_text_piece SET text=replace(text,'paragraph','Paragraph') WHERE operation_key=? AND text_id='paragraph'",
        "UPDATE note_stage_text SET sha256=NULL WHERE operation_key=? AND text_id='attrs'",
        "DELETE FROM note_stage_validation WHERE operation_key=? AND kind='entry' AND id='attrs'",
        "UPDATE note_stage_validation SET owner='foreign' WHERE operation_key=? AND kind='directory'",
        "UPDATE note_stage_validation SET value=json_set(value,'$.generation',1) WHERE operation_key=? AND kind='live'",
        "UPDATE note_stage_record SET value=json_set(value,'$.sourceRange.start',1) WHERE operation_key=? AND stream='live' AND ordinal=0",
    ]{
        let mut tx=f.store.write_pool().begin_with("BEGIN IMMEDIATE").await.unwrap();
        sqlx::query(sql).bind(&f.operation).execute(&mut *tx).await.unwrap();
        assert!(prepare(&mut tx,&f.operation,&f.begin.header,0,f.length).await.is_err(),"{sql}");
        tx.rollback().await.unwrap();
    }
    assert_eq!(output(&f).await.unwrap(), "abc");
}

#[tokio::test]
async fn selection_output_rejects_paragraph_above_explicit_subset_budget() {
    let f = fixture("", &"a".repeat(4097), 0, 1, true).await;
    assert!(matches!(
        output(&f).await,
        Err(Error::NoteMutation(NoteMutationError::Budget))
    ));
}

#[tokio::test]
async fn selection_public_pages_bind_kind_scope_cursor_budget_and_original_expiry() {
    use intent_core::{note_page::NotePageError, note_stage_read::NoteStageRead};
    let f = fixture(&"x".repeat(65536), "one two three", 4, 13, true).await;
    let mut value = serde_json::to_value(&f.begin).unwrap();
    value.as_object_mut().unwrap().remove("header");
    value.as_object_mut().unwrap().remove("expiresAt");
    value["kind"] = json!("selectionMarkdown");
    value["maxSourceBytes"] = json!(4);
    value["maxWireBytes"] = json!(4096);
    let mut query: NoteStageRead = serde_json::from_value(value).unwrap();
    let id = json!("escaped\"request");
    let first = f
        .store
        .read_note_stage_source("alice", &query, &id)
        .await
        .unwrap();
    assert_eq!(first["outputKind"], "selectionMarkdown");
    assert_eq!(first["sourceLength"], f.length);
    assert_eq!(first["items"], json!([{"offset":0,"text":"two "}]));
    query.cursor = Some(first["nextCursor"].as_str().unwrap().into());
    for variant in 0..4 {
        let mut wrong = query.clone();
        match variant {
            0 => wrong.kind = intent_core::note_stage_read::NoteStageReadKind::Source,
            1 => wrong.max_source_bytes = Some(8),
            2 => wrong.operation_id = uuid::Uuid::new_v4().to_string(),
            _ => wrong.header_digest = "f".repeat(64),
        }
        assert!(matches!(
            f.store.read_note_stage_source("alice", &wrong, &id).await,
            Err(Error::NotePage(NotePageError::CursorInvalid))
        ));
    }
    assert!(matches!(
        f.store.read_note_stage_source("bob", &query, &id).await,
        Err(Error::NotePage(NotePageError::CursorInvalid))
    ));
    let mut text = "two ".to_owned();
    let mut frames = 1;
    loop {
        let page = f
            .store
            .read_note_stage_source("alice", &query, &id)
            .await
            .unwrap();
        assert_eq!(page["scope"], first["scope"]);
        for field in [
            "operationId",
            "headerDigest",
            "payloadDigest",
            "viewId",
            "expiresAt",
            "sourceLength",
        ] {
            assert_eq!(page[field], first[field]);
        }
        assert!(
            json!({"jsonrpc":"2.0","id":id,"result":page})
                .to_string()
                .len()
                <= 4096
        );
        assert_eq!(page["items"][0]["offset"], text.len());
        text.push_str(page["items"][0]["text"].as_str().unwrap());
        frames += 1;
        if page["nextCursor"].is_null() {
            break;
        }
        assert!(frames < 4);
        query.cursor = Some(page["nextCursor"].as_str().unwrap().into());
    }
    assert_eq!(text, "two three");
    assert_eq!(frames, 3);
    sqlx::query("UPDATE note_operation SET outcome=json_set(outcome,'$.expiresAt','2000-01-01T00:00:00.000Z') WHERE operation_key=?")
        .bind(&f.operation).execute(f.store.write_pool()).await.unwrap();
    assert!(matches!(
        f.store.read_note_stage_source("alice", &query, &id).await,
        Err(Error::NotePage(NotePageError::Expired))
    ));
}
