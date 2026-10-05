//! Real Store upload/seal/output regressions. Registration and production v2
//! adapter wiring are owner work; these fixtures never seed a verified cache.
use crate::Store;
use intent_core::{
    note_receipt_detail::NoteOperationReceiptRead,
    note_stage::{
        NoteStageAppend, NoteStageBegin, NoteStageManifestEntry, NoteStageSeal, NoteStageStream,
        NOTE_STAGE_STREAMS,
    },
    note_stage_read::NoteStageRead,
    ContentType, Note, NoteId, NoteMetadata, NoteVisibility, WorkspaceId,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::Row;
use std::fmt::Write as _;

const START: u64 = 7; // "pre😀\n\n" in UTF16
struct Fixture {
    store: Store,
    _tmp: tempfile::TempDir,
    note: Note,
    begin: NoteStageBegin,
    operation: String,
    rendered: String,
}
#[derive(Clone, Copy)]
enum Defect {
    None,
    Missing,
    Foreign,
    Length,
    Hash,
    Unknown,
    SourceMode,
}
fn canonical(v: &Value) -> String {
    intent_core::note_artifact::canonical::canonical_json(&v.to_string()).unwrap()
}
fn reference(id: &str, text: &str) -> Value {
    let hash =
        Sha256::digest(text.as_bytes())
            .iter()
            .fold(String::with_capacity(64), |mut out, byte| {
                write!(out, "{byte:02x}").unwrap();
                out
            });
    json!({"textId":id,"length":text.encode_utf16().count(),"utf8Bytes":text.len(),"sha256":hash})
}
fn stream_name(stream: NoteStageStream) -> &'static str {
    super::stream_name(stream)
}
async fn append(
    store: &Store,
    begin: &NoteStageBegin,
    stream: NoteStageStream,
    records: Vec<Value>,
) {
    let row=sqlx::query("SELECT s.next_sequence,s.last_digest FROM note_stage_stream s JOIN note_operation o USING(operation_key) WHERE o.operation_id=? AND s.stream=?")
        .bind(&begin.operation_id).bind(stream_name(stream)).fetch_one(store.read_pool()).await.unwrap();
    let mut request = NoteStageAppend {
        backend_id: begin.backend_id.clone(),
        workspace_id: begin.workspace_id.clone(),
        note_id: begin.note_id.clone(),
        note_instance_id: begin.note_instance_id.clone(),
        operation_id: begin.operation_id.clone(),
        header_digest: begin.header_digest.clone(),
        stream,
        sequence: u64::try_from(row.get::<i64, _>("next_sequence")).unwrap(),
        previous_digest: row.get("last_digest"),
        records,
        chunk_digest: String::new(),
    };
    request.chunk_digest = request.computed_digest().unwrap();
    store.append_note_stage("alice", &request).await.unwrap();
}
async fn seal_request(f: &Fixture) -> NoteStageSeal {
    let mut manifest = Vec::new();
    for stream in NOTE_STAGE_STREAMS {
        let row=sqlx::query("SELECT next_sequence,last_digest,records FROM note_stage_stream WHERE operation_key=? AND stream=?").bind(&f.operation).bind(stream_name(stream)).fetch_one(f.store.read_pool()).await.unwrap();
        manifest.push(NoteStageManifestEntry {
            stream,
            chunks: u64::try_from(row.get::<i64, _>("next_sequence")).unwrap(),
            records: u64::try_from(row.get::<i64, _>("records")).unwrap(),
            last_digest: row.get("last_digest"),
        });
    }
    let b = &f.begin;
    let mut request = NoteStageSeal {
        backend_id: b.backend_id.clone(),
        workspace_id: b.workspace_id.clone(),
        note_id: b.note_id.clone(),
        note_instance_id: b.note_instance_id.clone(),
        operation_id: b.operation_id.clone(),
        header_digest: b.header_digest.clone(),
        manifest,
        payload_digest: String::new(),
    };
    request.payload_digest = request.computed_digest().unwrap();
    request
}
async fn fixture(
    source: &str,
    rendered: &str,
    query: &str,
    selection: (u64, u64),
    defect: Defect,
) -> Fixture {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open(&tmp.path().join("rendered.db")).await.unwrap();
    let now = intent_core::now_iso();
    sqlx::query("INSERT INTO workspace(id,title,branch,status,created_at,updated_at) VALUES('ws','Rendered','test','Active',?,?)").bind(&now).bind(&now).execute(store.write_pool()).await.unwrap();
    let note = Note {
        id: NoteId::from("note"),
        workspace_id: WorkspaceId::from("ws"),
        title: "Rendered".into(),
        content: format!("pre😀\n\n{source}\n\npost"),
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
    let mut raw = page["scope"].clone();
    raw["operationId"] = json!(uuid::Uuid::new_v4().to_string());
    raw["expiresAt"] = json!(format!(
        "{}.000Z",
        &intent_core::iso_ms_from_now(60_000)[..19]
    ));
    raw["headerDigest"] = json!("0".repeat(64));
    raw["header"] = json!({"baseRevision":page["sourceRevision"],"editorSessionId":"rendered-test","localEditSequence":0,"liveGeneration":1,"selectionGeneration":1,"action":"read","output":"search","selection":"ranges","query":{"text":query,"caseSensitive":false,"mode":if matches!(defect,Defect::SourceMode){"source"}else{"renderedText"}}});
    let mut begin: NoteStageBegin = serde_json::from_value(raw).unwrap();
    begin.header_digest = begin.computed_digest().unwrap();
    store.begin_note_stage("alice", &begin).await.unwrap();
    if matches!(defect, Defect::Foreign) {
        let mut foreign = begin.clone();
        foreign.operation_id = uuid::Uuid::new_v4().to_string();
        foreign.header_digest = foreign.computed_digest().unwrap();
        store.begin_note_stage("alice", &foreign).await.unwrap();
        append(
            &store,
            &foreign,
            NoteStageStream::Text,
            vec![json!({"kind":"text","id":"captured-text","offset":0,"text":rendered})],
        )
        .await;
    }
    let length = u64::try_from(source.encode_utf16().count()).unwrap();
    let parent = canonical(
        &json!({"version":1,"nodeType":"paragraph","parentOrdinal":null,"nativeRange":{"from":100,"to":102+length},"attributesRef":"attrs"}),
    );
    let mut text_ref = reference("captured-text", rendered);
    if matches!(defect, Defect::Length) {
        text_ref["length"] = json!(999);
    }
    if matches!(defect, Defect::Hash) {
        text_ref["sha256"] = json!("0".repeat(64));
    }
    let mut leaf = json!({"version":2,"nodeType":"text","parentOrdinal":0,"nativeRange":{"from":101,"to":101+length},"attributesRef":"attrs","renderedText":text_ref});
    if matches!(defect, Defect::Unknown) {
        leaf["unknown"] = json!(true);
    }
    let leaf = canonical(&leaf);
    let attrs = canonical(
        &json!({"id":"attrs-entry","parentId":null,"type":"object","childrenRef":"empty"}),
    );
    let empty = canonical(&json!({"kind":"metadataChildren","items":[],"nextRef":null}));
    let mut texts = vec![
        json!({"kind":"text","id":"parent","offset":0,"text":parent}),
        json!({"kind":"text","id":"leaf","offset":0,"text":leaf}),
        json!({"kind":"text","id":"attrs","offset":0,"text":attrs}),
        json!({"kind":"text","id":"empty","offset":0,"text":empty}),
    ];
    if !matches!(defect, Defect::Missing | Defect::Foreign) {
        texts.push(json!({"kind":"text","id":"captured-text","offset":0,"text":rendered}));
    }
    append(&store, &begin, NoteStageStream::Text, texts).await;
    append(&store,&begin,NoteStageStream::Selection,vec![json!({"kind":"range","ordinal":0,"start":START+selection.0,"end":START+selection.1,"direction":"backward","anchorAffinity":"after","headAffinity":"before"})]).await;
    append(&store,&begin,NoteStageStream::Live,vec![json!({"kind":"projection","ordinal":0,"role":"selection-owner","sourceRange":{"start":START,"end":START+length},"detail":reference("parent",&parent)}),json!({"kind":"projection","ordinal":1,"role":"inline-span","sourceRange":{"start":START,"end":START+length},"detail":reference("leaf",&leaf)})]).await;
    let operation =
        sqlx::query_scalar("SELECT operation_key FROM note_operation WHERE operation_id=?")
            .bind(&begin.operation_id)
            .fetch_one(store.read_pool())
            .await
            .unwrap();
    Fixture {
        store,
        _tmp: tmp,
        note,
        begin,
        operation,
        rendered: rendered.into(),
    }
}
async fn snapshot(f: &Fixture) -> Value {
    let row=sqlx::query("SELECT s.phase,s.payload_digest,s.view_id,o.outcome FROM note_stage s JOIN note_operation o USING(operation_key) WHERE s.operation_key=?").bind(&f.operation).fetch_one(f.store.read_pool()).await.unwrap();
    let mut out = json!({"phase":row.get::<String,_>("phase"),"payload":row.get::<Option<String>,_>("payload_digest"),"view":row.get::<Option<String>,_>("view_id"),"outcome":row.get::<String,_>("outcome")});
    for table in [
        "note_stage_view",
        "note_stage_view_piece",
        "note_stage_validation",
        "note_stage_record",
    ] {
        let count: i64 = sqlx::query_scalar(&format!(
            "SELECT COUNT(*) FROM {table} WHERE operation_key=?"
        ))
        .bind(&f.operation)
        .fetch_one(f.store.read_pool())
        .await
        .unwrap();
        out[table] = json!(count);
    }
    let cached: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM note_stage_text WHERE operation_key=? AND sha256 IS NOT NULL",
    )
    .bind(&f.operation)
    .fetch_one(f.store.read_pool())
    .await
    .unwrap();
    out["verifiedTexts"] = json!(cached);
    out
}
fn read_request(f: &Fixture) -> NoteStageRead {
    let b = &f.begin;
    serde_json::from_value(json!({"backendId":b.backend_id,"workspaceId":b.workspace_id,"noteId":b.note_id,"noteInstanceId":b.note_instance_id,"operationId":b.operation_id,"headerDigest":b.header_digest,"kind":"search","maxItems":1,"maxSourceBytes":4,"maxWireBytes":4096})).unwrap()
}
async fn collect(f: &Fixture) -> (Vec<Value>, Value) {
    let mut request = read_request(f);
    let mut hits = Vec::new();
    let mut prior: Option<Value> = None;
    for _ in 0..64 {
        let page = f
            .store
            .read_note_stage_source("alice", &request, &json!("\u{1}".repeat(64)))
            .await
            .unwrap();
        assert_eq!(page["outputKind"], "search");
        assert_eq!(page["expiresAt"], f.begin.expires_at);
        assert!(
            json!({"jsonrpc":"2.0","id":"\u{1}".repeat(64),"result":page})
                .to_string()
                .len()
                <= 4096
        );
        if let Some(ref first) = prior {
            for field in [
                "scope",
                "headerDigest",
                "payloadDigest",
                "viewId",
                "expiresAt",
                "sourceLength",
            ] {
                assert_eq!(page[field], first[field]);
            }
        } else {
            prior = Some(page.clone());
        }
        hits.extend(page["items"].as_array().unwrap().iter().cloned());
        request.cursor = page["nextCursor"].as_str().map(str::to_owned);
        if request.cursor.is_none() {
            assert_eq!(page["count"], json!({"value":hits.len(),"exact":true}));
            return (hits, page);
        }
        assert_eq!(page["count"]["exact"], false);
    }
    panic!("bounded rendered search did not terminate");
}

#[tokio::test]
async fn rendered_capture_seal_rejects_missing_foreign_length_hash_and_rolls_back() {
    for defect in [
        Defect::Missing,
        Defect::Foreign,
        Defect::Length,
        Defect::Hash,
    ] {
        let f = fixture("Straße😀", "Straße😀", "STRASSE", (0, 8), defect).await;
        let before = snapshot(&f).await;
        let request = seal_request(&f).await;
        for _ in 0..2 {
            assert!(f.store.seal_note_stage("alice", &request).await.is_err());
            assert_eq!(snapshot(&f).await, before);
        }
        if matches!(defect, Defect::Missing | Defect::Foreign) {
            append(
                &f.store,
                &f.begin,
                NoteStageStream::Text,
                vec![json!({"kind":"text","id":"captured-text","offset":0,"text":f.rendered})],
            )
            .await;
            let accepted = f
                .store
                .seal_note_stage("alice", &seal_request(&f).await)
                .await
                .unwrap();
            assert_eq!(accepted["phase"], "sealed");
            assert_eq!(collect(&f).await.0.len(), 1);
        }
    }
}
#[tokio::test]
async fn rendered_capture_rejects_v2_unknown_fields_and_wrong_mode_at_seal() {
    for defect in [Defect::Unknown, Defect::SourceMode] {
        let f = fixture("Straße😀", "Straße😀", "STRASSE", (0, 8), defect).await;
        let before = snapshot(&f).await;
        assert!(f
            .store
            .seal_note_stage("alice", &seal_request(&f).await)
            .await
            .is_err());
        assert_eq!(snapshot(&f).await, before);
    }
}
#[tokio::test]
async fn rendered_capture_publication_failure_rolls_back_then_exact_retry_commits() {
    let f = fixture("Straße😀", "Straße😀", "STRASSE", (0, 8), Defect::None).await;
    let before = snapshot(&f).await;
    let request = seal_request(&f).await;
    sqlx::query("CREATE TRIGGER reject_rendered_seal BEFORE UPDATE OF phase ON note_stage WHEN NEW.phase='sealed' BEGIN SELECT RAISE(ABORT,'rendered seal publish failed'); END").execute(f.store.write_pool()).await.unwrap();
    assert!(f.store.seal_note_stage("alice", &request).await.is_err());
    assert_eq!(snapshot(&f).await, before);
    sqlx::query("DROP TRIGGER reject_rendered_seal")
        .execute(f.store.write_pool())
        .await
        .unwrap();
    let result = f.store.seal_note_stage("alice", &request).await.unwrap();
    assert_eq!(result["phase"], "sealed");
    assert_eq!(
        f.store.seal_note_stage("alice", &request).await.unwrap(),
        result
    );
}
#[tokio::test]
async fn rendered_capture_search_clips_scalar_domain_preserves_spaces_and_frozen_source() {
    for (source, query, selection, expected) in [
        ("Straße😀", "STRASSE", (0, 8), vec![(7, 13)]),
        ("Straße😀", "STRASSE", (0, 5), vec![]),
        ("Straße😀", "😀", (6, 8), vec![(13, 15)]),
        ("Straße😀", "S", (6, 6), vec![]),
        (" Straße😀 ", " ", (0, 10), vec![(7, 8), (16, 17)]),
    ] {
        let mut f = fixture(source, source, query, selection, Defect::None).await;
        f.store
            .seal_note_stage("alice", &seal_request(&f).await)
            .await
            .unwrap();
        f.note.content = "remote changed content".into();
        f.store.update_note(&f.note).await.unwrap();
        let (hits, page) = collect(&f).await;
        assert_eq!(
            hits.iter()
                .map(|hit| (
                    hit["sourceRange"]["start"].as_u64().unwrap(),
                    hit["sourceRange"]["end"].as_u64().unwrap()
                ))
                .collect::<Vec<_>>(),
            expected
        );
        assert_eq!(
            page["sourceLength"],
            START + u64::try_from(source.encode_utf16().count()).unwrap() + 6
        );
        assert!(hits
            .iter()
            .all(|h| h["detailRef"].as_str().is_some_and(|s| !s.is_empty())));
    }
}
#[tokio::test]
async fn rendered_capture_nonidentity_bytes_cannot_produce_search_success() {
    let f = fixture("abc", "abd", "a", (0, 3), Defect::None).await;
    // Structural seal may accept this well-formed reference; semantic output
    // must reject. A stricter seal is also valid, never an empty search success.
    if f.store
        .seal_note_stage("alice", &seal_request(&f).await)
        .await
        .is_ok()
    {
        assert!(f
            .store
            .read_note_stage_source("alice", &read_request(&f), &json!(1))
            .await
            .is_err());
    }
}

#[tokio::test]
// This control proves raw-text reachability, binding and bounded publication.
// It does not compare every reconstructed metadata-tree field. Full logical
// stagedRenderedHit schema fidelity requires the dedicated detail adapter oracle.
async fn rendered_capture_hit_detail_retains_whole_raw_leaf_not_only_match() {
    use std::collections::{HashSet, VecDeque};
    let source = " Straße😀 ";
    let mut f = fixture(source, source, "STRASSE", (1, 7), Defect::None).await;
    f.store
        .seal_note_stage("alice", &seal_request(&f).await)
        .await
        .unwrap();
    let (hits, search_page) = collect(&f).await;
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0]["sourceRange"], json!({"start":8,"end":14}));
    f.note.content = "new current note".into();
    f.store.update_note(&f.note).await.unwrap();
    let mut raw = serde_json::to_value(read_request(&f)).unwrap();
    raw["kind"] = json!("detail");
    raw["ref"] = hits[0]["detailRef"].clone();
    let typed: NoteOperationReceiptRead = serde_json::from_value(raw).unwrap();
    let base = typed.query().unwrap();
    let mut pending = VecDeque::from([base.reference.clone()]);
    let mut seen = HashSet::new();
    let mut scalar = None;
    let mut calls = 0;
    while let Some(reference) = pending.pop_front() {
        assert!(
            seen.insert(reference.clone()),
            "cyclic rendered detail directory"
        );
        let mut query = base.clone();
        query.reference = reference;
        loop {
            calls += 1;
            assert!(calls <= 256, "fixture detail traversal did not terminate");
            let page = f
                .store
                .read_note_stage_search_detail("alice", &query, &json!("id\"\\"))
                .await
                .unwrap();
            for field in [
                "scope",
                "headerDigest",
                "payloadDigest",
                "viewId",
                "expiresAt",
                "sourceLength",
            ] {
                assert_eq!(page[field], search_page[field], "{field}");
            }
            assert!(
                json!({"jsonrpc":"2.0","id":"id\"\\","result":page})
                    .to_string()
                    .len()
                    <= 4096
            );
            for item in page["items"].as_array().unwrap() {
                if let Some(child) = item["childrenRef"].as_str() {
                    pending.push_back(child.into());
                }
                if item["key"] == "renderedText" && item["type"] == "string" {
                    assert!(scalar.is_none(), "one captured text leaf");
                    scalar = Some(
                        item["valueRef"]
                            .as_str()
                            .expect("even small rendered text owns scalar ref")
                            .to_owned(),
                    );
                }
            }
            query.cursor = page["nextCursor"].as_str().map(str::to_owned);
            if query.cursor.is_none() {
                break;
            }
        }
    }
    let mut query = base.clone();
    query.reference = scalar.expect("renderedText metadata entry must be reachable");
    let mut text = String::new();
    let mut terminal = false;
    for _ in 0..64 {
        let page = f
            .store
            .read_note_stage_search_detail("alice", &query, &json!(1))
            .await
            .unwrap();
        for field in [
            "scope",
            "headerDigest",
            "payloadDigest",
            "viewId",
            "expiresAt",
            "sourceLength",
        ] {
            assert_eq!(page[field], search_page[field], "{field}");
        }
        assert!(
            json!({"jsonrpc":"2.0","id":1,"result":page})
                .to_string()
                .len()
                <= 4096
        );
        assert!(page["nextCursor"].is_null());
        let items = page["items"].as_array().unwrap();
        assert_eq!(items.len(), 1);
        let item = &items[0];
        assert_eq!(item["field"], "renderedText");
        assert_eq!(item["offset"], text.encode_utf16().count());
        let part = item["text"].as_str().unwrap();
        assert!(!part.is_empty() && part.len() <= 4);
        text.push_str(part);
        if let Some(next) = item["nextRef"].as_str() {
            query.reference = next.into();
        } else {
            terminal = true;
            break;
        }
    }
    assert!(terminal);
    assert_eq!(text, source);
    assert!(f
        .store
        .read_note_stage_search_detail("bob", &base, &json!(1))
        .await
        .is_err());
}
