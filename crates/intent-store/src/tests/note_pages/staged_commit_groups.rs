//! Actual Store staged writer/receipt regression controls. Registered by the
//! staging parent; all source reconstruction below is a small test oracle.
use super::{append, request, seal_request, setup};
use crate::{NoteMutationWrite, StageCommitAdmission, Store};
use intent_core::{
    note_mutation::{apply_note_splices, NoteMutationError, NoteSourceHistory, NoteSplice},
    note_receipt_detail::{ReceiptDetailKind, ReceiptDetailQuery},
    note_stage::{NoteStageBegin, NoteStageCommit, NoteStageStream},
    NoteVersionAuthor,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

fn reference(id: &str, text: &str) -> Value {
    json!({"textId":id,"length":text.encode_utf16().count(),"utf8Bytes":text.len(),"sha256":format!("{:x}",Sha256::digest(text.as_bytes()))})
}
fn dirty(sequence: u64, ordinal: u64, start: u64, end: u64, replacement: &Value) -> Value {
    json!({"kind":"splice","localSequence":sequence,"ordinal":ordinal,"start":start,"end":end,"replacement":replacement})
}
fn mutation(ordinal: u64, start: u64, end: u64, replacement: &Value) -> Value {
    json!({"kind":"splice","ordinal":ordinal,"start":start,"end":end,"replacement":replacement})
}

async fn upload_texts(store: &Store, begin: &NoteStageBegin, texts: &[(&str, &str)]) {
    let mut sequence = 0;
    let mut previous = None;
    for (id, text) in texts {
        let (mut byte, mut units) = (0, 0);
        loop {
            let mut end = (byte + 4096).min(text.len());
            while !text.is_char_boundary(end) {
                end -= 1;
            }
            let piece = &text[byte..end];
            let mut chunk = append(begin, "");
            chunk.sequence = sequence;
            chunk.previous_digest = previous;
            chunk.records = vec![json!({"kind":"text","id":id,"offset":units,"text":piece})];
            chunk.chunk_digest = chunk.computed_digest().unwrap();
            assert_eq!(
                store.append_note_stage("alice", &chunk).await.unwrap()["nextSequence"],
                sequence + 1
            );
            previous = Some(chunk.chunk_digest);
            sequence += 1;
            units += piece.encode_utf16().count();
            byte = end;
            if byte == text.len() {
                break;
            }
        }
    }
}
async fn capture(
    store: &Store,
    local_sequence: u64,
    texts: &[(&str, &str)],
    dirty: Vec<Value>,
    mutation: Vec<Value>,
) -> NoteStageCommit {
    let mut begin = request(store).await;
    begin.header.local_edit_sequence = local_sequence;
    begin.header_digest = begin.computed_digest().unwrap();
    store.begin_note_stage("alice", &begin).await.unwrap();
    upload_texts(store, &begin, texts).await;
    for (stream, records) in [
        (NoteStageStream::Dirty, dirty),
        (NoteStageStream::Mutation, mutation),
    ] {
        if records.is_empty() {
            continue;
        }
        let mut chunk = append(&begin, "");
        chunk.stream = stream;
        chunk.records = records;
        chunk.chunk_digest = chunk.computed_digest().unwrap();
        store.append_note_stage("alice", &chunk).await.unwrap();
    }
    let seal = seal_request(store, &begin).await;
    store.seal_note_stage("alice", &seal).await.unwrap();
    NoteStageCommit {
        backend_id: seal.backend_id,
        workspace_id: seal.workspace_id,
        note_id: seal.note_id,
        note_instance_id: seal.note_instance_id,
        operation_id: seal.operation_id,
        header_digest: seal.header_digest,
        payload_digest: seal.payload_digest,
    }
}
async fn writer(store: &Store, request: &NoteStageCommit) -> Box<NoteMutationWrite> {
    let StageCommitAdmission::Reserved(reserved) = store
        .reserve_note_stage_commit("alice", request)
        .await
        .unwrap()
    else {
        panic!("new reservation expected")
    };
    reserved.into_mutation(|_| Ok(())).await.unwrap()
}
async fn publish(mut writer: Box<NoteMutationWrite>) -> Value {
    writer
        .persist_source(
            &NoteVersionAuthor {
                id: "alice".into(),
                name: "Alice".into(),
                author_type: "user".into(),
            },
            &intent_core::now_iso(),
        )
        .await
        .unwrap();
    writer.commit().await.unwrap()
}
fn query(receipt: &Value, kind: ReceiptDetailKind, reference: &str) -> ReceiptDetailQuery {
    ReceiptDetailQuery {
        scope: serde_json::from_value(receipt["scope"].clone()).unwrap(),
        operation_id: receipt["operationId"].as_str().unwrap().into(),
        payload_digest: None,
        header_digest: Some(receipt["headerDigest"].as_str().unwrap().into()),
        kind,
        reference: reference.into(),
        cursor: None,
        max_items: 2,
        max_wire_bytes: 4096,
        max_source_bytes: 128,
        operation_envelope: true,
        context_envelope: false,
        text_id: None,
        offset: None,
    }
}
async fn read(store: &Store, query: &ReceiptDetailQuery) -> Value {
    let id = json!("inverse-\\\"😀");
    let result = store
        .read_note_receipt_detail("alice", query, &id)
        .await
        .unwrap();
    assert!(
        json!({"jsonrpc":"2.0","id":id,"result":result})
            .to_string()
            .len()
            <= query.max_wire_bytes
    );
    result
}
async fn inverse_items(store: &Store, receipt: &Value) -> Vec<Value> {
    let mut query = query(
        receipt,
        ReceiptDetailKind::Inverse,
        receipt["inverseRef"].as_str().unwrap(),
    );
    let mut rows = Vec::new();
    loop {
        let page = read(store, &query).await;
        assert_eq!(page["headerDigest"], receipt["headerDigest"]);
        assert_eq!(page["viewId"], receipt["viewId"]);
        let items = page["items"].as_array().unwrap();
        assert!(items.len() <= 2);
        rows.extend(items.iter().cloned());
        let Some(next) = page["nextCursor"].as_str() else {
            break;
        };
        assert!(!items.is_empty());
        assert_ne!(query.cursor.as_deref(), Some(next));
        query.cursor = Some(next.into());
    }
    rows
}
async fn inverse_text(store: &Store, receipt: &Value, replacement: &Value) -> String {
    let mut query = query(
        receipt,
        ReceiptDetailKind::InverseText,
        receipt["inverseRef"].as_str().unwrap(),
    );
    query.text_id = Some(replacement["textId"].as_str().unwrap().into());
    let mut text = String::new();
    let mut offset = 0;
    loop {
        let page = read(store, &query).await;
        let items = page["items"].as_array().unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0]["offset"], offset);
        let part = items[0]["text"].as_str().unwrap();
        assert!(part.len() <= 128);
        offset += part.encode_utf16().count();
        text.push_str(part);
        let Some(next) = page["nextCursor"].as_str() else {
            break;
        };
        assert!(!part.is_empty());
        assert_ne!(query.cursor.as_deref(), Some(next));
        query.cursor = Some(next.into());
    }
    assert_eq!(replacement["length"], offset);
    assert_eq!(replacement["utf8Bytes"], text.len());
    assert_eq!(
        replacement["sha256"],
        format!("{:x}", Sha256::digest(text.as_bytes()))
    );
    text
}
async fn undo_groups(
    store: &Store,
    receipt: &Value,
    source: &str,
    expected: &[&str],
) -> Vec<Value> {
    let rows = inverse_items(store, receipt).await;
    let mut state = receipt["afterRevision"].clone();
    let persisted: String =
        sqlx::query_scalar("SELECT content FROM note WHERE workspace_id=? AND id=?")
            .bind(receipt["scope"]["workspaceId"].as_str().unwrap())
            .bind(receipt["scope"]["noteId"].as_str().unwrap())
            .fetch_one(store.read_pool())
            .await
            .unwrap();
    assert_eq!(
        persisted, source,
        "commit persisted the actual final source"
    );
    let mut source = persisted;
    let mut at = 0;
    let mut groups = Vec::new();
    while at < rows.len() {
        let id = rows[at]["historyGroup"].clone();
        assert!(!groups.contains(&id));
        let end = (at + 1..rows.len())
            .find(|&i| rows[i]["historyGroup"] != id)
            .unwrap_or(rows.len());
        let output = rows[at]["outputState"].clone();
        let mut edits = Vec::new();
        for (ordinal, row) in rows[at..end].iter().enumerate() {
            assert_eq!(row["ordinal"], ordinal);
            assert_eq!(row["inputState"], state);
            assert_eq!(row["outputState"], output);
            let detail = query(
                receipt,
                ReceiptDetailKind::Detail,
                row["provenanceRef"].as_str().unwrap(),
            );
            assert!(
                !read(store, &detail).await["items"]
                    .as_array()
                    .unwrap()
                    .is_empty(),
                "owned provenance is reachable"
            );
            edits.push(NoteSplice {
                start: row["start"].as_u64().unwrap(),
                end: row["end"].as_u64().unwrap(),
                text: inverse_text(store, receipt, &row["replacement"]).await,
            });
        }
        // Staged groups deliberately have no inline32-splice/16KiB ceiling.
        // All bounded fixtures also use inline admission as a second oracle.
        let mut history = NoteSourceHistory::new(source.clone());
        history.apply_phase(&edits).unwrap();
        if edits.len() <= 32 && edits.iter().map(|e| e.text.len()).sum::<usize>() <= 16384 {
            assert_eq!(
                apply_note_splices(&source, &edits).unwrap().source,
                history.source()
            );
        }
        source = history.source().into();
        assert_eq!(source, expected[groups.len()]);
        groups.push(id);
        state = output;
        at = end;
    }
    assert_eq!(groups.len(), expected.len());
    assert_eq!(state, receipt["beforeRevision"]);
    rows
}

#[tokio::test]
async fn staged_real_receipt_retains_newest_dirty_noop_and_earlier_history() {
    let (store, _tmp, _note) = setup("abcd").await;
    let request = capture(
        &store,
        2,
        &[("upper", "B"), ("bang", "!")],
        vec![
            dirty(1, 0, 1, 2, &reference("upper", "B")),
            dirty(2, 0, 4, 4, &reference("bang", "!")),
        ],
        vec![],
    )
    .await;
    let mut write = writer(&store, &request).await;
    assert_eq!(write.source(), "aBcd!");
    write
        .apply_recorded_phase(
            "anchor-repair",
            &[NoteSplice {
                start: 4,
                end: 5,
                text: String::new(),
            }],
        )
        .await
        .unwrap();
    let receipt = publish(write).await;
    let rows = undo_groups(&store, &receipt, "aBcd", &["aBcd", "abcd"]).await;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["historyGroup"], "2");
    assert_eq!(rows[1]["historyGroup"], "1");
    assert_eq!(rows[0]["start"], 0);
    assert_eq!(rows[0]["end"], 0);
    assert_eq!(rows[0]["replacement"]["length"], 0);
    assert_ne!(rows[0]["inputState"], rows[0]["outputState"]);
}

#[tokio::test]
async fn staged_real_receipt_retains_explicit_noop_mutation_and_earlier_dirty() {
    let (store, _tmp, _note) = setup("abcd").await;
    let request = capture(
        &store,
        1,
        &[("upper", "B"), ("empty", "")],
        vec![dirty(1, 0, 1, 2, &reference("upper", "B"))],
        vec![mutation(0, 0, 0, &reference("empty", ""))],
    )
    .await;
    let write = writer(&store, &request).await;
    assert_eq!(write.source(), "aBcd");
    let receipt = publish(write).await;
    let rows = undo_groups(&store, &receipt, "aBcd", &["aBcd", "abcd"]).await;
    assert_eq!(rows.len(), 2);
    assert_ne!(rows[0]["historyGroup"], rows[1]["historyGroup"]);
    assert_eq!(rows[1]["historyGroup"], "1");
    assert_eq!(rows[0]["replacement"]["utf8Bytes"], 0);
    assert_ne!(rows[0]["inputState"], rows[0]["outputState"]);
}

#[tokio::test]
async fn staged_real_receipt_normalizes_earlier_adjacent_deletions_and_replacement() {
    for replacement in ["", "界"] {
        let (store, _tmp, _note) = setup("A😀z").await;
        let dirty_source = format!("{replacement}z");
        let end = u64::try_from(dirty_source.encode_utf16().count()).unwrap();
        let request = capture(
            &store,
            1,
            &[("empty", ""), ("replacement", replacement), ("bang", "!")],
            vec![
                dirty(1, 0, 0, 1, &reference("empty", "")),
                dirty(1, 1, 1, 3, &reference("replacement", replacement)),
            ],
            vec![mutation(0, end, end, &reference("bang", "!"))],
        )
        .await;
        let final_source = format!("{dirty_source}!");
        let write = writer(&store, &request).await;
        assert_eq!(write.source(), final_source);
        let receipt = publish(write).await;
        let rows = undo_groups(&store, &receipt, &final_source, &[&dirty_source, "A😀z"]).await;
        assert_eq!(
            rows.len(),
            2,
            "older same-group deletions normalize to one admissible inverse"
        );
        assert_eq!(rows[1]["historyGroup"], "1");
        assert_eq!(rows[1]["replacement"]["length"], 3);
        assert_eq!(rows[1]["start"], 0);
        assert_eq!(rows[1]["end"], replacement.encode_utf16().count());
    }
}

#[tokio::test]
async fn staged_real_commit_accepts_forty_splices_without_inline_batch_ceiling() {
    let base = "a.".repeat(40);
    let expected = "b.".repeat(40);
    let (store, _tmp, _note) = setup(&base).await;
    let edits: Vec<_> = (0..40)
        .map(|i| NoteSplice {
            start: i * 2,
            end: i * 2 + 1,
            text: "b".into(),
        })
        .collect();
    assert_eq!(
        apply_note_splices(&base, &edits),
        Err(NoteMutationError::Budget)
    );
    let records = (0..40)
        .map(|i| mutation(i, i * 2, i * 2 + 1, &reference("replacement", "b")))
        .collect();
    let request = capture(&store, 0, &[("replacement", "b")], vec![], records).await;
    let write = writer(&store, &request).await;
    assert_eq!(write.source(), expected);
    let receipt = publish(write).await;
    let rows = undo_groups(&store, &receipt, &expected, &[&base]).await;
    assert_eq!(
        rows.len(),
        40,
        "noncontiguous ranges must not be merged across unchanged dots"
    );
}

#[tokio::test]
async fn staged_real_commit_accepts_large_logical_replacement_across_upload_chunks() {
    let replacement = "a😀\r\n".repeat(3000);
    assert!(replacement.len() > 16384);
    let (store, _tmp, _note) = setup("old").await;
    assert_eq!(
        apply_note_splices(
            "old",
            &[NoteSplice {
                start: 0,
                end: 3,
                text: replacement.clone()
            }]
        ),
        Err(NoteMutationError::Budget)
    );
    let request = capture(
        &store,
        0,
        &[("large", &replacement)],
        vec![],
        vec![mutation(0, 0, 3, &reference("large", &replacement))],
    )
    .await;
    let write = writer(&store, &request).await;
    assert_eq!(write.source(), replacement);
    let receipt = publish(write).await;
    let rows = undo_groups(&store, &receipt, &replacement, &["old"]).await;
    assert_eq!(rows.len(), 1);
    let pieces: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM note_stage_text_piece WHERE text_id='large'")
            .fetch_one(store.read_pool())
            .await
            .unwrap();
    assert!(pieces > 1);
}

async fn provenance_tree(store: &Store, receipt: &Value, reference: &str) -> (Value, Vec<Value>) {
    fn resolve(node: &Value, nodes: &std::collections::BTreeMap<String, Value>) -> Value {
        if node["type"] != "object" {
            assert!(matches!(node["type"].as_str(), Some("string" | "number")));
            return node["value"].clone();
        }
        let mut out = serde_json::Map::new();
        for child in nodes
            .values()
            .filter(|child| child["parentId"] == node["id"])
        {
            assert!(out
                .insert(child["key"].as_str().unwrap().into(), resolve(child, nodes))
                .is_none());
        }
        Value::Object(out)
    }
    let mut pending = vec![reference.to_owned()];
    let mut nodes = std::collections::BTreeMap::new();
    let mut transcript = Vec::new();
    while let Some(reference) = pending.pop() {
        let mut request = query(receipt, ReceiptDetailKind::Detail, &reference);
        loop {
            let page = read(store, &request).await;
            assert_eq!(page["headerDigest"], receipt["headerDigest"]);
            assert_eq!(page["viewId"], receipt["viewId"]);
            for item in page["items"].as_array().unwrap() {
                assert!(item.get("valueRef").is_none(), "fixture scalars are inline");
                if let Some(children) = item["childrenRef"].as_str() {
                    pending.push(children.into());
                }
                assert!(nodes
                    .insert(item["id"].as_str().unwrap().to_owned(), item.clone())
                    .is_none());
            }
            transcript.push(json!({"normalizedRequest":request,"cursor":request.cursor,"offset":request.offset,"response":page}));
            let Some(next) = page["nextCursor"].as_str() else {
                break;
            };
            assert_ne!(request.cursor.as_deref(), Some(next));
            request.cursor = Some(next.into());
        }
    }
    assert_eq!(nodes.len(), 15, "complete expected provenance graph");
    let roots: Vec<_> = nodes
        .values()
        .filter(|node| node["parentId"].is_null())
        .collect();
    assert_eq!(roots.len(), 1);
    (resolve(roots[0], &nodes), transcript)
}

#[tokio::test]
async fn staged_receipt_noncontiguous_groups_reconstruct_complete_provenance() {
    let (store, _tmp, _note) = setup("ab😀cd").await;
    let request = capture(
        &store,
        3,
        &[("upper-b", "B"), ("upper-c", "C"), ("bang", "!")],
        vec![
            dirty(1, 0, 1, 2, &reference("upper-b", "B")),
            dirty(3, 0, 4, 5, &reference("upper-c", "C")),
            dirty(3, 1, 6, 6, &reference("bang", "!")),
        ],
        vec![],
    )
    .await;
    let mut write = writer(&store, &request).await;
    assert_eq!(write.source(), "aB😀Cd!");
    write
        .persist_source(
            &NoteVersionAuthor {
                id: "alice".into(),
                name: "Alice".into(),
                author_type: "user".into(),
            },
            &intent_core::now_iso(),
        )
        .await
        .unwrap();
    write.publish_annotation_anchors(&[]).await.unwrap();
    let receipt = write.commit().await.unwrap();
    let rows = undo_groups(&store, &receipt, "aB😀Cd!", &["aB😀cd", "ab😀cd"]).await;
    assert_eq!(rows.len(), 3);
    let mut transcripts = Vec::new();
    for (index, (group, ordinal, start, end, prior_start, prior_end, text)) in [
        ("3", 0, 4, 5, 4, 5, "c"),
        ("3", 1, 6, 7, 6, 6, ""),
        ("1", 0, 1, 2, 1, 2, "b"),
    ]
    .into_iter()
    .enumerate()
    {
        let row = &rows[index];
        assert_eq!(row["historyGroup"], group);
        assert_eq!(row["ordinal"], ordinal);
        assert_eq!(row["start"], start);
        assert_eq!(row["end"], end);
        assert_eq!(
            inverse_text(&store, &receipt, &row["replacement"]).await,
            text
        );
        let (tree, pages) =
            provenance_tree(&store, &receipt, row["provenanceRef"].as_str().unwrap()).await;
        assert_eq!(
            tree,
            json!({"kind":"sourceProvenance","inputState":row["inputState"],"outputState":row["outputState"],"baseRange":{"start":prior_start,"end":prior_end},"finalRange":{"start":start,"end":end},"replacement":row["replacement"]})
        );
        transcripts
            .push(json!({"inverse":row,"rawReplacement":text,"provenance":tree,"pages":pages}));
    }
    let effects = read(
        &store,
        &query(
            &receipt,
            ReceiptDetailKind::Effects,
            receipt["effectsRef"].as_str().unwrap(),
        ),
    )
    .await;
    assert_eq!(effects["nextCursor"], Value::Null);
    assert_eq!(effects["items"].as_array().unwrap().len(), 1);
    assert_eq!(effects["items"][0]["kind"], "annotationInvalidation");
    eprintln!(
        "STAGED_RECEIPT_PRODUCER_CAPTURE={}",
        json!({"base":"ab😀cd","capturedGroups":[1,3],"final":"aB😀Cd!","receipt":receipt,"effects":effects,"transcripts":transcripts})
    );
}
