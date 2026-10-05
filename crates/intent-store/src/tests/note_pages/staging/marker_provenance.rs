//! Actual uploads exercise provenance admission, never a seeded verified ledger.
use super::{count, request, seal_request, setup};
use crate::{Store, tests::sample_comment};
use intent_core::{
    Error,
    note_stage::{NoteStageAppend, NoteStageBegin},
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::fmt::Write as _;

const ID: &str = "legacy-marker";
const LITERAL: &str = "<!--anchor:legacy-marker:point-->";
const START: u64 = 2; // Leading emoji is two UTF16 units, four UTF8 bytes.

#[derive(Clone, Copy)]
enum Edit {
    None,
    Shift,
    Replace,
    Retype,
    Duplicate,
    Mixed,
}
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
            .fold(String::with_capacity(64), |mut out, byte| {
                write!(out, "{byte:02x}").unwrap();
                out
            });
    json!({"textId":id,"length":units(text),"utf8Bytes":text.len(),"sha256":digest})
}
async fn upload(store: &Store, begin: &NoteStageBegin, stream: &str, records: Vec<Value>) {
    let mut chunk:NoteStageAppend=serde_json::from_value(json!({"backendId":begin.backend_id,"workspaceId":begin.workspace_id,"noteId":begin.note_id,"noteInstanceId":begin.note_instance_id,"operationId":begin.operation_id,"headerDigest":begin.header_digest,"stream":stream,"sequence":0,"previousDigest":null,"records":records,"chunkDigest":"0".repeat(64)})).unwrap();
    chunk.chunk_digest = chunk.computed_digest().unwrap();
    store.append_note_stage("alice", &chunk).await.unwrap();
}
fn splice(group: u64, start: u64, end: u64, id: &str, text: &str) -> Value {
    json!({"kind":"splice","localSequence":group,"ordinal":0,"start":start,"end":end,"replacement":reference(id,text)})
}
async fn stage(store: &Store, edit: Edit) -> NoteStageBegin {
    let mut begin = request(store).await;
    begin.header.local_edit_sequence = match edit {
        Edit::None => 0,
        Edit::Retype => 2,
        _ => 1,
    };
    begin.header.live_generation = 1;
    begin.header_digest = begin.computed_digest().unwrap();
    store.begin_note_stage("alice", &begin).await.unwrap();
    let width = units(LITERAL);
    let (marker_start, replacement, dirty) = match edit {
        Edit::None => (START, "", vec![]),
        Edit::Shift => (
            START + 3,
            "Q😀",
            vec![splice(1, 0, 0, "replacement", "Q😀")],
        ),
        Edit::Replace => (
            START,
            LITERAL,
            vec![splice(1, START, START + width, "replacement", LITERAL)],
        ),
        Edit::Retype => (
            START,
            LITERAL,
            vec![
                splice(1, START, START + width, "empty", ""),
                splice(2, START, START, "replacement", LITERAL),
            ],
        ),
        Edit::Duplicate => (
            START + width,
            LITERAL,
            vec![splice(
                1,
                START + width,
                START + width,
                "replacement",
                LITERAL,
            )],
        ),
        // Replace only '<' with itself. Remaining marker is inherited, but the
        // complete literal no longer has contiguous original-root provenance.
        Edit::Mixed => (
            START,
            "<",
            vec![splice(1, START, START + 1, "replacement", "<")],
        ),
    };
    let descriptor = canonical(
        &json!({"version":1,"nodeType":"commentAnchor","parentOrdinal":null,"nativeRange":{"from":3,"to":4},"attributesRef":"attrs"}),
    );
    let mut texts = vec![
        ("descriptor", descriptor.clone()),
        (
            "attrs",
            canonical(
                &json!({"id":"attribute-root","parentId":null,"type":"object","childrenRef":"directory"}),
            ),
        ),
        (
            "directory",
            canonical(
                &json!({"kind":"metadataChildren","items":["comment-entry","id-entry","type-entry"],"nextRef":null}),
            ),
        ),
        (
            "comment-entry",
            canonical(
                &json!({"id":"comment-attribute","parentId":"attribute-root","key":"commentId","type":"string","valueRef":"comment-value"}),
            ),
        ),
        (
            "id-entry",
            canonical(
                &json!({"id":"id-attribute","parentId":"attribute-root","key":"id","type":"string","valueRef":"id-value"}),
            ),
        ),
        (
            "type-entry",
            canonical(
                &json!({"id":"type-attribute","parentId":"attribute-root","key":"type","type":"string","valueRef":"type-value"}),
            ),
        ),
        ("comment-value", ID.into()),
        ("id-value", format!("{ID}:point")),
        ("type-value", "point".into()),
    ];
    if !dirty.is_empty() {
        texts.push(("replacement", replacement.into()));
    }
    if matches!(edit, Edit::Retype) {
        texts.push(("empty", String::new()));
    }
    upload(
        store,
        &begin,
        "text",
        texts
            .into_iter()
            .map(|(id, text)| json!({"kind":"text","id":id,"offset":0,"text":text}))
            .collect(),
    )
    .await;
    if !dirty.is_empty() {
        upload(store, &begin, "dirty", dirty).await;
    }
    upload(store,&begin,"live",vec![json!({"kind":"projection","ordinal":0,"sourceRange":{"start":marker_start,"end":marker_start+width},"role":"marker-occurrence","canonicalId":ID,"detail":reference("descriptor",&descriptor)})]).await;
    begin
}
async fn reject_without_publication(store: &Store, begin: &NoteStageBegin) {
    let seal = seal_request(store, begin).await;
    assert!(matches!(
        store.seal_note_stage("alice", &seal).await,
        Err(Error::Unsupported(_))
    ));
    let phase: String = sqlx::query_scalar("SELECT phase FROM note_stage")
        .fetch_one(store.read_pool())
        .await
        .unwrap();
    assert_eq!(phase, "staging");
    for table in [
        "note_stage_view",
        "note_stage_view_piece",
        "note_stage_validation",
    ] {
        assert_eq!(count(store, table).await, 0, "{table}");
    }
    let cached: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM note_stage_text WHERE sha256 IS NOT NULL")
            .fetch_one(store.read_pool())
            .await
            .unwrap();
    assert_eq!(cached, 0, "failed seal must roll back digest cache too");
    assert!(
        count(store, "note_stage_record").await > 0,
        "uploads survive refusal"
    );
}

#[tokio::test]
async fn inherited_marker_survives_dirty_prefix_shift_and_seal_replay_after_delete_reopen() {
    let source = format!("😀{LITERAL}tail");
    let (store, tmp, note) = setup(&source).await;
    let root = sample_comment(&note.id, ID, ID);
    store
        .insert_comment(&note.workspace_id, &root)
        .await
        .unwrap();
    assert!(
        store
            .note_annotation_epochs(&note.workspace_id, &note.id)
            .await
            .unwrap()
            .anchors_ready
    );
    let begin = stage(&store, Edit::Shift).await;
    let seal = seal_request(&store, &begin).await;
    let sealed = store.seal_note_stage("alice", &seal).await.unwrap();
    assert_eq!(sealed["phase"], "sealed");
    assert_eq!(sealed["payloadDigest"], seal.payload_digest);
    assert_eq!(sealed["viewLength"], units(&source) + 3);
    assert_eq!(
        store
            .get_note(&note.workspace_id, &note.id)
            .await
            .unwrap()
            .content,
        source
    );
    // Original source position START becomes START+3 in the dirty generation.
    let ranges:Vec<(i64,i64,String,i64)>=sqlx::query_as("SELECT start,end,origin_kind,origin_start FROM note_stage_view_piece WHERE generation=1 ORDER BY start").fetch_all(store.read_pool()).await.unwrap();
    assert_eq!(
        ranges,
        vec![
            (0, 3, "text".into(), 0),
            (
                3,
                i64::try_from(units(&source) + 3).unwrap(),
                "root".into(),
                0
            )
        ]
    );
    let proof: Vec<(String, String, String)> =
        sqlx::query_as("SELECT kind,id,value FROM note_stage_validation ORDER BY kind,id")
            .fetch_all(store.read_pool())
            .await
            .unwrap();
    assert!(!proof.is_empty());
    let retained: String = sqlx::query_scalar("SELECT json_extract(value,'$.markerWitness') FROM note_stage_validation WHERE kind='live' AND id='0'")
        .fetch_one(store.read_pool()).await.unwrap();
    let witness: Value = serde_json::from_str(&retained).unwrap();
    assert_eq!(witness["version"], 1);
    assert_eq!(witness["canonicalId"], ID);
    assert_eq!(witness["threadId"], ID);
    assert_eq!(witness["type"], "point");
    assert_eq!(
        witness["rootRange"],
        json!({"start":START,"end":START+units(LITERAL)})
    );
    let (view, root, pin): (String, String, String) =
        sqlx::query_as("SELECT view_id,root_key,marker_admission FROM note_stage")
            .fetch_one(store.read_pool())
            .await
            .unwrap();
    assert_eq!(witness["viewId"], view);
    assert_eq!(witness["rootKey"], root);
    assert_eq!(
        witness["admission"],
        serde_json::from_str::<Value>(&pin).unwrap()
    );
    store.delete_comment(&note.workspace_id, ID).await.unwrap();
    assert_eq!(store.seal_note_stage("alice", &seal).await.unwrap(), sealed);
    let after: Vec<(String, String, String)> =
        sqlx::query_as("SELECT kind,id,value FROM note_stage_validation ORDER BY kind,id")
            .fetch_all(store.read_pool())
            .await
            .unwrap();
    assert_eq!(
        after, proof,
        "replay preserves retained proof instead of re-resolving current ownership"
    );
    drop(store);
    let store = Store::open(&tmp.path).await.unwrap();
    assert_eq!(store.seal_note_stage("alice", &seal).await.unwrap(), sealed);
}

#[tokio::test]
async fn identical_replacement_retyping_duplicate_and_mixed_origins_are_not_inherited_markers() {
    for edit in [Edit::Replace, Edit::Retype, Edit::Duplicate, Edit::Mixed] {
        let source = format!("😀{LITERAL}tail");
        let (store, _tmp, note) = setup(&source).await;
        store
            .insert_comment(&note.workspace_id, &sample_comment(&note.id, ID, ID))
            .await
            .unwrap();
        let begin = stage(&store, edit).await;
        reject_without_publication(&store, &begin).await;
        assert_eq!(
            store
                .get_note(&note.workspace_id, &note.id)
                .await
                .unwrap()
                .content,
            source
        );
    }
}

#[tokio::test]
async fn before_seal_ownership_epoch_change_refuses_without_refreshing_begin_pin() {
    for orphan in [false, true] {
        let (store, _tmp, note) = setup(&format!("😀{LITERAL}tail")).await;
        let mut root = sample_comment(&note.id, ID, ID);
        store
            .insert_comment(&note.workspace_id, &root)
            .await
            .unwrap();
        let begin = stage(&store, Edit::None).await;
        let pin: String = sqlx::query_scalar("SELECT marker_admission FROM note_stage")
            .fetch_one(store.read_pool())
            .await
            .unwrap();
        if orphan {
            root.is_orphaned = Some(true);
            store
                .update_comment(&note.workspace_id, &root)
                .await
                .unwrap();
        } else {
            store.delete_comment(&note.workspace_id, ID).await.unwrap();
        }
        reject_without_publication(&store, &begin).await;
        store.begin_note_stage("alice", &begin).await.unwrap();
        let after: String = sqlx::query_scalar("SELECT marker_admission FROM note_stage")
            .fetch_one(store.read_pool())
            .await
            .unwrap();
        assert_eq!(after, pin);
    }
}

#[tokio::test]
async fn final_marker_witness_failure_rolls_back_entire_seal_and_retry_uses_same_upload() {
    let (store, _tmp, note) = setup(&format!("😀{LITERAL}tail")).await;
    store
        .insert_comment(&note.workspace_id, &sample_comment(&note.id, ID, ID))
        .await
        .unwrap();
    let begin = stage(&store, Edit::Shift).await;
    let seal = seal_request(&store, &begin).await;
    let uploaded: Vec<String> = sqlx::query_scalar(
        "SELECT value FROM note_stage_record ORDER BY stream,chunk_sequence,ordinal",
    )
    .fetch_all(store.read_pool())
    .await
    .unwrap();
    sqlx::query("CREATE TRIGGER reject_marker_witness BEFORE UPDATE OF value ON note_stage_validation WHEN new.kind='live' AND json_type(new.value,'$.markerWitness')='object' BEGIN SELECT RAISE(ABORT,'injected final marker witness'); END")
        .execute(store.write_pool()).await.unwrap();
    let error = store.seal_note_stage("alice", &seal).await.unwrap_err();
    assert!(
        matches!(&error, Error::Internal(message) if message.contains("injected final marker witness")),
        "{error:?}"
    );
    let state: (String, Option<String>, Option<String>) =
        sqlx::query_as("SELECT phase,payload_digest,view_id FROM note_stage")
            .fetch_one(store.read_pool())
            .await
            .unwrap();
    assert_eq!(state, ("staging".into(), None, None));
    for table in [
        "note_stage_view",
        "note_stage_view_piece",
        "note_stage_validation",
    ] {
        assert_eq!(count(&store, table).await, 0, "{table}");
    }
    let cached: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM note_stage_text WHERE sha256 IS NOT NULL")
            .fetch_one(store.read_pool())
            .await
            .unwrap();
    assert_eq!(cached, 0);
    let after: Vec<String> = sqlx::query_scalar(
        "SELECT value FROM note_stage_record ORDER BY stream,chunk_sequence,ordinal",
    )
    .fetch_all(store.read_pool())
    .await
    .unwrap();
    assert_eq!(after, uploaded);
    sqlx::query("DROP TRIGGER reject_marker_witness")
        .execute(store.write_pool())
        .await
        .unwrap();
    let sealed = store.seal_note_stage("alice", &seal).await.unwrap();
    assert_eq!(sealed["phase"], "sealed");
    assert_eq!(sealed["payloadDigest"], seal.payload_digest);
    let witnesses:i64=sqlx::query_scalar("SELECT COUNT(*) FROM note_stage_validation WHERE kind='live' AND json_type(value,'$.markerWitness')='object'")
        .fetch_one(store.read_pool()).await.unwrap();
    assert_eq!(witnesses, 1);
    assert_eq!(store.seal_note_stage("alice", &seal).await.unwrap(), sealed);
}
