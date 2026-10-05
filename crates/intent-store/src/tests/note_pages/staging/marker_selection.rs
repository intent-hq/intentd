//! Public Store reads from original upload resources and a real sealed witness.
use super::{request, seal_request, setup};
use crate::{
    tests::{sample_comment, TempDb},
    Store,
};
use intent_core::{
    note_stage::{NoteStageAppend, NoteStageBegin},
    note_stage_read::NoteStageRead,
    Error,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::fmt::Write as _;

const ID: &str = "11111111-1111-4111-8111-111111111111";
const LITERAL: &str = "<!--anchor:11111111-1111-4111-8111-111111111111:point-->";
const PREFIX: &str = "prefix😀\n\n";
fn units(text: &str) -> u64 {
    u64::try_from(text.encode_utf16().count()).unwrap()
}
fn canonical(value: &Value) -> String {
    intent_core::note_artifact::canonical::canonical_json(&value.to_string()).unwrap()
}
fn reference(id: &str, text: &str) -> Value {
    let digest =
        Sha256::digest(text.as_bytes())
            .iter()
            .fold(String::with_capacity(64), |mut out, b| {
                write!(out, "{b:02x}").unwrap();
                out
            });
    json!({"textId":id,"length":units(text),"utf8Bytes":text.len(),"sha256":digest})
}
async fn upload(store: &Store, begin: &NoteStageBegin, stream: &str, records: Vec<Value>) {
    let mut value = serde_json::to_value(begin).unwrap();
    let object = value.as_object_mut().unwrap();
    object.remove("header");
    object.remove("expiresAt");
    object.extend(serde_json::from_value::<serde_json::Map<String,Value>>(json!({"stream":stream,"sequence":0,"previousDigest":null,"records":records,"chunkDigest":"0".repeat(64)})).unwrap());
    let mut append: NoteStageAppend = serde_json::from_value(value).unwrap();
    append.chunk_digest = append.computed_digest().unwrap();
    store.append_note_stage("alice", &append).await.unwrap();
}
struct Fixture {
    store: Store,
    temp: TempDb,
    query: NoteStageRead,
    operation: String,
    source: String,
}

async fn fixture(left: &str, right: &str, start: u64, end: u64) -> Fixture {
    fixture_with_dirty(left, right, start, end, false).await
}

async fn fixture_with_dirty(left: &str, right: &str, start: u64, end: u64, dirty: bool) -> Fixture {
    fixture_with_fence(left, right, start, end, dirty, u64::from(dirty)).await
}

async fn fixture_with_fence(
    left: &str,
    right: &str,
    start: u64,
    end: u64,
    dirty: bool,
    local_sequence: u64,
) -> Fixture {
    let source = format!("{PREFIX}{left}{LITERAL}{right}\n\ntail");
    let (store, tmp, note) = setup(&source).await;
    store
        .insert_comment(&note.workspace_id, &sample_comment(&note.id, ID, ID))
        .await
        .unwrap();
    let mut begin = request(&store).await;
    begin.header.action = intent_core::note_stage::NoteStageAction::Read;
    begin.header.output = intent_core::note_stage::NoteStageOutput::SelectionMarkdown;
    begin.header.selection = intent_core::note_stage::NoteStageSelection::Ranges;
    begin.header.local_edit_sequence = local_sequence;
    begin.header.live_generation = 1;
    begin.header.selection_generation = 1;
    begin.header_digest = begin.computed_digest().unwrap();
    store.begin_note_stage("alice", &begin).await.unwrap();
    let base = units(PREFIX);
    let l = units(left);
    let r = units(right);
    let m = units(LITERAL);
    let mut resources: Vec<(String, String)> = Vec::new();
    for name in ["paragraph", "text"] {
        resources.push((format!("{name}-attrs"),canonical(&json!({"id":format!("{name}-entry"),"parentId":null,"type":"object","childrenRef":format!("{name}-directory")}))));
        resources.push((
            format!("{name}-directory"),
            canonical(&json!({"kind":"metadataChildren","items":[],"nextRef":null})),
        ));
    }
    for (key, text) in [
        ("commentId", ID.to_owned()),
        ("id", format!("{ID}:point")),
        ("type", "point".to_owned()),
    ] {
        resources.push((format!("marker-{key}-value"), text));
        resources.push((format!("marker-{key}-entry"),canonical(&json!({"id":format!("marker-{key}"),"parentId":"marker-root","key":key,"type":"string","valueRef":format!("marker-{key}-value")}))));
    }
    resources.push(("marker-directory".into(),canonical(&json!({"kind":"metadataChildren","items":["marker-commentId-entry","marker-id-entry","marker-type-entry"],"nextRef":null}))));
    resources.push(("marker-attrs".into(),canonical(&json!({"id":"marker-root","parentId":null,"type":"object","childrenRef":"marker-directory"}))));
    let descriptors = [
        json!({"version":1,"nodeType":"paragraph","parentOrdinal":null,"nativeRange":{"from":0,"to":l+r+3},"attributesRef":"paragraph-attrs"}),
        json!({"version":1,"nodeType":"text","parentOrdinal":0,"nativeRange":{"from":1,"to":1+l},"attributesRef":"text-attrs"}),
        json!({"version":1,"nodeType":"commentAnchor","parentOrdinal":0,"nativeRange":{"from":1+l,"to":2+l},"attributesRef":"marker-attrs"}),
        json!({"version":1,"nodeType":"text","parentOrdinal":0,"nativeRange":{"from":2+l,"to":2+l+r},"attributesRef":"text-attrs"}),
    ];
    let ranges = [
        (base, base + l + m + r),
        (base, base + l),
        (base + l, base + l + m),
        (base + l + m, base + l + m + r),
    ];
    let mut live = Vec::new();
    for (ordinal, descriptor) in descriptors.iter().enumerate() {
        let id = format!("descriptor-{ordinal}");
        let text = canonical(descriptor);
        let mut record = json!({"kind":"projection","ordinal":ordinal,"sourceRange":{"start":ranges[ordinal].0,"end":ranges[ordinal].1},"role":if ordinal==0 {"selection-owner"} else if ordinal==2 {"marker-occurrence"} else {"inline-span"},"detail":reference(&id,&text)});
        if ordinal == 2 {
            record["canonicalId"] = json!(ID);
        }
        live.push(record);
        resources.push((id, text));
    }
    assert_eq!(resources.len(), 16);
    if dirty {
        resources.push(("dirty-p".into(), "p".into()));
    }
    upload(
        &store,
        &begin,
        "text",
        resources
            .into_iter()
            .map(|(id, text)| json!({"kind":"text","id":id,"offset":0,"text":text}))
            .collect(),
    )
    .await;
    upload(&store,&begin,"selection",vec![json!({"kind":"range","ordinal":0,"start":base+start,"end":base+end,"direction":"backward","anchorAffinity":"after","headAffinity":"before"})]).await;
    upload(&store, &begin, "live", live).await;
    if dirty {
        upload(&store, &begin, "dirty", vec![json!({"kind":"splice","localSequence":1,"ordinal":0,"start":0,"end":1,"replacement":reference("dirty-p","p")})]).await;
    }
    store
        .seal_note_stage("alice", &seal_request(&store, &begin).await)
        .await
        .unwrap();
    let mut query = serde_json::to_value(&begin).unwrap();
    query.as_object_mut().unwrap().remove("header");
    query.as_object_mut().unwrap().remove("expiresAt");
    query["kind"] = json!("selectionMarkdown");
    query["maxSourceBytes"] = json!(4);
    query["maxWireBytes"] = json!(4096);
    query["maxItems"] = json!(64);
    let operation =
        sqlx::query_scalar("SELECT operation_key FROM note_operation WHERE operation_id=?")
            .bind(&begin.operation_id)
            .fetch_one(store.read_pool())
            .await
            .unwrap();
    Fixture {
        store,
        temp: tmp,
        query: serde_json::from_value(query).unwrap(),
        operation,
        source,
    }
}
async fn read(f: &Fixture) -> intent_core::Result<Value> {
    f.store
        .read_note_stage_source("alice", &f.query, &json!("marker\"selection"))
        .await
}

#[tokio::test]
async fn marker_selection_shared_attrs_preserve_spaces_and_frozen_witness_after_delete_reopen() {
    let m = units(LITERAL);
    let mut f = fixture("abc ", " def", 1, 4 + m + 3).await;
    let first = read(&f).await.unwrap();
    assert_eq!(first["items"], json!([{"offset":0,"text":"bc  "}]));
    assert_eq!(first["sourceLength"], units(&f.source));
    assert_eq!(first["outputKind"], "selectionMarkdown");
    f.store
        .delete_comment(&intent_core::WorkspaceId::from("pages"), ID)
        .await
        .unwrap();
    assert_eq!(read(&f).await.unwrap(), first);
    f.store = Store::open(&f.temp.path).await.unwrap();
    let mut result = String::from("bc  ");
    f.query.cursor = Some(first["nextCursor"].as_str().unwrap().into());
    let next = read(&f).await.unwrap();
    assert_eq!(next["items"], json!([{"offset":4,"text":"de"}]));
    assert!(next["nextCursor"].is_null());
    result.push_str(next["items"][0]["text"].as_str().unwrap());
    assert_eq!(result, "bc  de");
    for field in [
        "scope",
        "operationId",
        "headerDigest",
        "payloadDigest",
        "viewId",
        "sourceLength",
        "expiresAt",
    ] {
        assert_eq!(next[field], first[field]);
    }
    assert!(
        json!({"jsonrpc":"2.0","id":"marker\"selection","result":next})
            .to_string()
            .len()
            <= 4096
    );
}

#[tokio::test]
async fn marker_selection_requires_exact_retained_witness_and_complete_resource_ownership() {
    let f = fixture("A", "B", 0, 2 + units(LITERAL)).await;
    assert!(read(&f).await.is_ok());
    let original: String = sqlx::query_scalar(
        "SELECT value FROM note_stage_validation WHERE operation_key=? AND kind='live' AND id='2'",
    )
    .bind(&f.operation)
    .fetch_one(f.store.read_pool())
    .await
    .unwrap();
    for path in [
        "$.markerWitness.viewId",
        "$.markerWitness.rootKey",
        "$.markerWitness.canonicalId",
        "$.markerWitness.type",
        "$.markerWitness.admission.commentRevision",
        "$.markerWitness.rootRange.start",
        "$.generation",
    ] {
        sqlx::query("UPDATE note_stage_validation SET value=json_set(value,?,'wrong') WHERE operation_key=? AND kind='live' AND id='2'").bind(path).bind(&f.operation).execute(f.store.write_pool()).await.unwrap();
        assert!(read(&f).await.is_err(), "{path}");
        sqlx::query("UPDATE note_stage_validation SET value=? WHERE operation_key=? AND kind='live' AND id='2'").bind(&original).bind(&f.operation).execute(f.store.write_pool()).await.unwrap();
    }
    sqlx::query("DELETE FROM note_stage_validation WHERE operation_key=? AND kind='entry' AND id='text-attrs'").bind(&f.operation).execute(f.store.write_pool()).await.unwrap();
    assert!(read(&f).await.is_err());
}

#[tokio::test]
async fn marker_selection_rejects_no_copy_literal_interior_and_unsupported_text() {
    let m = units(LITERAL);
    for (left, right, start, end) in [
        ("A", "B", 1, 1 + m),
        ("A", "B", 0, 0),
        ("A", "B", 2, 2 + m),
        ("A*", "B", 0, 3 + m),
        ("A ", " B", 1, 3 + m),
    ] {
        let f = fixture(left, right, start, end).await;
        assert!(matches!(
            read(&f).await,
            Err(Error::Unsupported(_) | Error::NoteMutation(_))
        ));
    }
}

#[tokio::test]
async fn marker_selection_cursor_binds_scope_kind_budgets_and_original_deadline() {
    let mut f = fixture("ABC", "DEF", 0, 6 + units(LITERAL)).await;
    let page = read(&f).await.unwrap();
    f.query.cursor = Some(page["nextCursor"].as_str().unwrap().into());
    assert!(f
        .store
        .read_note_stage_source("bob", &f.query, &json!(1))
        .await
        .is_err());
    for variant in 0..3 {
        let mut wrong = f.query.clone();
        match variant {
            0 => wrong.max_source_bytes = Some(5),
            1 => wrong.header_digest = "f".repeat(64),
            _ => wrong.kind = intent_core::note_stage_read::NoteStageReadKind::Source,
        }
        assert!(f
            .store
            .read_note_stage_source("alice", &wrong, &json!(1))
            .await
            .is_err());
    }
    sqlx::query("UPDATE note_operation SET outcome=json_set(outcome,'$.expiresAt','2000-01-01T00:00:00.000Z') WHERE operation_key=?").bind(&f.operation).execute(f.store.write_pool()).await.unwrap();
    assert!(matches!(
        read(&f).await,
        Err(Error::NotePage(
            intent_core::note_page::NotePageError::Expired
        ))
    ));
}

#[tokio::test]
async fn marker_selection_refuses_sealed_dirty_view_without_restoration_authority() {
    let f = fixture_with_dirty("A", "B", 0, 2 + units(LITERAL), true).await;
    assert!(matches!(read(&f).await, Err(Error::Unsupported(_))));
}

#[tokio::test]
async fn marker_selection_accepts_clean_view_with_nonzero_history_fence() {
    let f = fixture_with_fence("A", "B", 0, 2 + units(LITERAL), false, 7).await;
    let (fence, dirty): (i64, i64) = sqlx::query_as(
        "SELECT json_extract(s.header,'$.localEditSequence'),d.records FROM note_stage s JOIN note_stage_stream d USING(operation_key) WHERE s.operation_key=? AND d.stream='dirty' AND s.phase='sealed'",
    )
    .bind(&f.operation)
    .fetch_one(f.store.read_pool())
    .await
    .unwrap();
    assert_eq!((fence, dirty), (7, 0));
    let page = read(&f).await.unwrap();
    assert_eq!(page["items"], json!([{"offset":0,"text":"AB"}]));
    assert!(page["nextCursor"].is_null());
}
