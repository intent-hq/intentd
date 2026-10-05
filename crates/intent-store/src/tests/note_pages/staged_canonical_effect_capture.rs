//! Actual staged Store writer output for bounded FE validation, not native adoption.
use super::{append, dirty, reference, request, seal_request, setup, writer};
use crate::Store;
use intent_core::{
    note_mutation::NoteSplice,
    note_receipt_detail::NoteOperationReceiptRead,
    note_stage::{NoteStageCommit, NoteStageStream},
    NoteVersionAuthor,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, VecDeque};

fn units(text: &str) -> u64 {
    u64::try_from(text.encode_utf16().count()).unwrap()
}

fn params(receipt: &Value, kind: &str, reference: &str) -> Value {
    let mut value = receipt["scope"].clone();
    value["operationId"] = receipt["operationId"].clone();
    value["headerDigest"] = receipt["headerDigest"].clone();
    value["kind"] = json!(kind);
    value["ref"] = json!(reference);
    value["maxItems"] = json!(if kind == "effects" { 64 } else { 16 });
    value["maxWireBytes"] = json!(8192);
    value
}

async fn pages(
    store: &Store,
    receipt: &Value,
    base_length: u64,
    mut request: Value,
    transcript: &mut Vec<Value>,
) -> Vec<Value> {
    let mut rows = Vec::new();
    loop {
        assert!(transcript.len() < 128);
        let query = serde_json::from_value::<NoteOperationReceiptRead>(request.clone())
            .unwrap()
            .query()
            .unwrap();
        assert_eq!(query.max_source_bytes, 16384);
        let id = json!("canonical-effect-capture");
        let response = store
            .read_note_receipt_detail("alice", &query, &id)
            .await
            .unwrap();
        assert_eq!(response["sourceLength"], base_length);
        for field in [
            "scope",
            "operationId",
            "headerDigest",
            "payloadDigest",
            "viewId",
        ] {
            assert_eq!(response[field], receipt[field]);
        }
        assert_eq!(response["expiresAt"], receipt["receiptExpiresAt"]);
        assert_eq!(response["outputKind"], request["kind"]);
        let frame = json!({"jsonrpc":"2.0","id":id,"result":response});
        assert!(frame.to_string().len() <= 8192);
        let items = response["items"].as_array().unwrap();
        assert!(items.len() <= query.max_items);
        rows.extend(items.iter().cloned());
        transcript.push(json!({"method":"note.operation.read","params":request,
            "normalizedRequest":query,"rpcId":id,"response":response}));
        let Some(next) = response["nextCursor"].as_str() else {
            break;
        };
        assert!(!items.is_empty());
        assert_ne!(request["cursor"].as_str(), Some(next));
        request["cursor"] = json!(next);
    }
    rows
}

fn reconstruct(
    id: &str,
    nodes: &BTreeMap<String, Value>,
    scalars: &BTreeMap<String, String>,
) -> Value {
    let node = &nodes[id];
    if node["type"] == "object" {
        let mut object = serde_json::Map::new();
        for (child_id, child) in nodes {
            if child["parentId"] == id {
                assert!(object
                    .insert(
                        child["key"].as_str().unwrap().into(),
                        reconstruct(child_id, nodes, scalars)
                    )
                    .is_none());
            }
        }
        Value::Object(object)
    } else if node.get("valueRef").is_some() {
        json!(scalars[id])
    } else {
        node["value"].clone()
    }
}

async fn detail(
    store: &Store,
    receipt: &Value,
    base_length: u64,
    reference: &str,
    transcript: &mut Vec<Value>,
) -> (Value, usize) {
    let mut pending = VecDeque::from([reference.to_owned()]);
    let mut nodes = BTreeMap::<String, Value>::new();
    let mut scalars = BTreeMap::<String, String>::new();
    let mut root = None;
    let mut fragments = 0;
    while let Some(reference) = pending.pop_front() {
        for node in pages(
            store,
            receipt,
            base_length,
            params(receipt, "detail", &reference),
            transcript,
        )
        .await
        {
            let id = node["id"].as_str().unwrap().to_owned();
            if node["kind"] == "fragment" {
                fragments += 1;
                let owner = &nodes[&id];
                assert_eq!(node["field"], owner["key"]);
                let text = scalars.entry(id).or_default();
                assert_eq!(node["offset"], units(text));
                let part = node["text"].as_str().unwrap();
                assert!(!part.is_empty());
                text.push_str(part);
                if let Some(next) = node["nextRef"].as_str() {
                    assert_ne!(next, reference);
                    pending.push_front(next.into());
                }
            } else {
                if node["parentId"].is_null() {
                    assert!(root.replace(id.clone()).is_none());
                }
                for field in ["childrenRef", "valueRef"] {
                    if let Some(next) = node[field].as_str() {
                        pending.push_back(next.into());
                    }
                }
                assert!(nodes.insert(id, node).is_none());
            }
        }
    }
    (
        reconstruct(root.as_ref().unwrap(), &nodes, &scalars),
        fragments,
    )
}

#[tokio::test]
async fn staged_canonical_effect_details_capture_original_pages() {
    let removed = "q😀\"\\\r\n".repeat(1400);
    let base = format!("L{removed}R");
    let input = format!("LL{removed}R");
    let final_source = "LLok😀tail";
    let (store, _tmp, _note) = setup(&base).await;
    let mut begin = request(&store).await;
    begin.header.local_edit_sequence = 7;
    begin.header_digest = begin.computed_digest().unwrap();
    let clock = intent_core::now_iso();
    let begin_response = store.begin_note_stage("alice", &begin).await.unwrap();
    let mut chunk = append(&begin, "LL");
    // Keep the exact typed upload resource used by the captured dirty group.
    chunk.records = vec![json!({"kind":"text","id":"prefix","offset":0,"text":"LL"})];
    chunk.chunk_digest = chunk.computed_digest().unwrap();
    let text_response = store.append_note_stage("alice", &chunk).await.unwrap();
    let mut group = append(&begin, "");
    group.stream = NoteStageStream::Dirty;
    group.records = vec![dirty(7, 0, 0, 1, &reference("prefix", "LL"))];
    group.chunk_digest = group.computed_digest().unwrap();
    let group_response = store.append_note_stage("alice", &group).await.unwrap();
    let seal = seal_request(&store, &begin).await;
    let seal_response = store.seal_note_stage("alice", &seal).await.unwrap();
    let commit = NoteStageCommit {
        backend_id: seal.backend_id.clone(),
        workspace_id: seal.workspace_id.clone(),
        note_id: seal.note_id.clone(),
        note_instance_id: seal.note_instance_id.clone(),
        operation_id: seal.operation_id.clone(),
        header_digest: seal.header_digest.clone(),
        payload_digest: seal.payload_digest.clone(),
    };
    let mut write = writer(&store, &commit).await;
    assert_eq!(write.source(), input);
    let edits = [
        NoteSplice {
            start: 2,
            end: 2 + units(&removed),
            text: "ok😀".into(),
        },
        NoteSplice {
            start: 2 + units(&removed),
            end: units(&input),
            text: "tail".into(),
        },
    ];
    write
        .apply_recorded_phase("anchor-repair", &edits)
        .await
        .unwrap();
    assert_eq!(write.source(), final_source);
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
    assert_eq!(receipt["sourceLength"], units(final_source));
    let mut transcript = Vec::new();
    let effects = pages(
        &store,
        &receipt,
        units(&base),
        params(&receipt, "effects", receipt["effectsRef"].as_str().unwrap()),
        &mut transcript,
    )
    .await;
    assert_eq!(effects.len(), 3);
    assert_eq!(effects[2]["kind"], "annotationInvalidation");
    for (index, old) in [removed.as_str(), "R"].into_iter().enumerate() {
        let effect = &effects[index];
        assert_eq!(effect["kind"], "sourceEffect");
        assert_eq!(effect["reason"], "anchor-repair");
        assert_eq!(effect["inputState"], effects[0]["inputState"]);
        assert_eq!(effect["outputState"], effects[0]["outputState"]);
        assert_eq!(effect["insertedLength"], units(&edits[index].text));
        assert_eq!(
            effect["beforeDigest"],
            format!("{:x}", Sha256::digest(old.as_bytes()))
        );
        assert_eq!(
            effect["afterDigest"],
            format!("{:x}", Sha256::digest(edits[index].text.as_bytes()))
        );
        let (tree, fragments) = detail(
            &store,
            &receipt,
            units(&base),
            effect["detailRef"].as_str().unwrap(),
            &mut transcript,
        )
        .await;
        assert_eq!(
            tree,
            json!({"inputState":effect["inputState"],"outputState":effect["outputState"],
            "range":{"start":edits[index].start,"end":edits[index].end},"removed":old,"inserted":edits[index].text})
        );
        assert_eq!(effect["range"], tree["range"]);
        if index == 0 {
            assert!(fragments > 1);
        } else {
            assert_eq!(fragments, 0);
        }
    }
    eprintln!(
        "STAGED_CANONICAL_EFFECT_CAPTURE={}",
        json!({"clock":clock,"base":base,"input":input,"final":final_source,
        "baseLength":units(&base),"inputLength":units(&input),"finalLength":units(final_source),
        "begin":{"request":begin,"response":begin_response},"uploads":[{"request":chunk,"response":text_response},{"request":group,"response":group_response}],
        "seal":{"request":seal,"response":seal_response},"commitRequest":commit,"receipt":receipt,"transcript":transcript,
        "producer":"Store staged writer apply_recorded_phase; controlled canonical edits, no native or Services parser invocation"})
    );
}
