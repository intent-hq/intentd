//! Actual Services canonicalization; no controlled canonical phase or native-adoption claim.
use super::Services;
use crate::tests::setup;
use intent_core::{
    note_mutation::{apply_note_splices, NoteSplice},
    note_receipt_detail::NoteOperationReceiptRead,
    note_stage::{
        NoteStageAppend, NoteStageBegin, NoteStageCommit, NoteStageManifestEntry, NoteStageSeal,
        NoteStageStream, NOTE_STAGE_STREAMS,
    },
    NoteId, WorkspaceApi, WorkspaceId,
};
use serde_json::{json, Value};
use std::collections::{BTreeMap, VecDeque};

fn digest(text: &str) -> String {
    crate::attachment_upload::sha256_hex(text.as_bytes())
}
async fn captured(
    services: &Services,
    workspace: &WorkspaceId,
    note: &NoteId,
    replacement: &str,
) -> NoteStageCommit {
    let state = services
        .store
        .read_note_page_state(workspace, note, None)
        .await
        .unwrap();
    let mut value = state["scope"].clone();
    value["operationId"] = json!(uuid::Uuid::new_v4().to_string());
    value["expiresAt"] = json!(format!(
        "{}.000Z",
        &intent_core::iso_ms_from_now(60_000)[..19]
    ));
    value["headerDigest"] = json!("0".repeat(64));
    value["header"] = json!({"baseRevision":state["sourceRevision"],"editorSessionId":"commit","localEditSequence":7,"liveGeneration":0,"selectionGeneration":0,"action":"mutate","output":"source","selection":"all"});
    let mut begin: NoteStageBegin = serde_json::from_value(value).unwrap();
    begin.header_digest = begin.computed_digest().unwrap();
    services.note_operation_begin(begin.clone()).await.unwrap();
    let mut manifest: Vec<_> = NOTE_STAGE_STREAMS
        .into_iter()
        .map(|stream| NoteStageManifestEntry {
            stream,
            chunks: 0,
            records: 0,
            last_digest: None,
        })
        .collect();
    for (stream, records) in [
        (
            NoteStageStream::Text,
            vec![json!({"kind":"text","id":"insert","offset":0,"text":replacement})],
        ),
        (
            NoteStageStream::Dirty,
            vec![
                json!({"kind":"splice","localSequence":7,"ordinal":0,"start":1,"end":1,"replacement":{"textId":"insert","length":replacement.encode_utf16().count(),"utf8Bytes":replacement.len(),"sha256":crate::attachment_upload::sha256_hex(replacement.as_bytes())}}),
            ],
        ),
    ] {
        let mut chunk = NoteStageAppend {
            backend_id: begin.backend_id.clone(),
            workspace_id: begin.workspace_id.clone(),
            note_id: begin.note_id.clone(),
            note_instance_id: begin.note_instance_id.clone(),
            operation_id: begin.operation_id.clone(),
            header_digest: begin.header_digest.clone(),
            stream,
            sequence: 0,
            previous_digest: None,
            chunk_digest: String::new(),
            records,
        };
        chunk.chunk_digest = chunk.computed_digest().unwrap();
        services.note_operation_append(chunk.clone()).await.unwrap();
        let entry = manifest
            .iter_mut()
            .find(|entry| entry.stream == stream)
            .unwrap();
        entry.chunks = 1;
        entry.records = chunk.records.len() as u64;
        entry.last_digest = Some(chunk.chunk_digest);
    }
    let mut seal = NoteStageSeal {
        backend_id: begin.backend_id,
        workspace_id: begin.workspace_id,
        note_id: begin.note_id,
        note_instance_id: begin.note_instance_id,
        operation_id: begin.operation_id,
        header_digest: begin.header_digest,
        payload_digest: String::new(),
        manifest,
    };
    seal.payload_digest = seal.computed_digest().unwrap();
    services.note_operation_seal(seal.clone()).await.unwrap();
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

fn params(receipt: &Value, kind: &str, reference: &str) -> Value {
    let mut value = receipt["scope"].clone();
    value["operationId"] = receipt["operationId"].clone();
    value["headerDigest"] = receipt["headerDigest"].clone();
    value["kind"] = json!(kind);
    value["ref"] = json!(reference);
    value["maxItems"] = json!(16);
    value["maxWireBytes"] = json!(8192);
    value
}
async fn pages(services: &Services, receipt: &Value, mut request: Value) -> Vec<Value> {
    let mut rows = Vec::new();
    for _ in 0..16 {
        let query = serde_json::from_value::<NoteOperationReceiptRead>(request.clone())
            .unwrap()
            .query()
            .unwrap();
        let page = services
            .get_note_receipt_detail(query, json!("phantom-proof"))
            .await
            .unwrap();
        for field in [
            "scope",
            "operationId",
            "headerDigest",
            "payloadDigest",
            "viewId",
        ] {
            assert_eq!(page[field], receipt[field]);
        }
        assert_eq!(page["expiresAt"], receipt["receiptExpiresAt"]);
        assert_eq!(page["outputKind"], request["kind"]);
        assert_eq!(
            page["sourceLength"],
            if matches!(request["kind"].as_str(), Some("inverse" | "inverseText")) {
                3
            } else {
                2
            }
        );
        assert!(
            json!({"jsonrpc":"2.0","id":"phantom-proof","result":page})
                .to_string()
                .len()
                <= 8192
        );
        if request["kind"] == "inverseText" {
            assert_eq!(page.get("nextCursor"), Some(&Value::Null));
        }
        let items = page["items"].as_array().unwrap();
        assert!(items.len() <= 16);
        rows.extend(items.iter().cloned());
        let Some(next) = page["nextCursor"].as_str() else {
            assert_eq!(page.get("nextCursor"), Some(&Value::Null));
            return rows;
        };
        assert!(!items.is_empty());
        assert_ne!(request["cursor"].as_str(), Some(next));
        request.as_object_mut().unwrap().remove("offset");
        request["cursor"] = json!(next);
    }
    panic!("unexpected receipt traversal size")
}
fn reconstruct(id: &str, nodes: &BTreeMap<String, Value>) -> Value {
    let node = &nodes[id];
    if node["type"] != "object" {
        assert!(
            node.get("valueRef").is_none(),
            "small scalar fixture must be inline"
        );
        return node["value"].clone();
    }
    let mut object = serde_json::Map::new();
    for (child_id, child) in nodes {
        if child["parentId"] == id {
            assert!(object
                .insert(
                    child["key"].as_str().unwrap().into(),
                    reconstruct(child_id, nodes)
                )
                .is_none());
        }
    }
    Value::Object(object)
}
async fn detail(services: &Services, receipt: &Value, reference: &str) -> Value {
    let mut pending = VecDeque::from([reference.to_owned()]);
    let mut nodes = BTreeMap::new();
    let mut root = None;
    while let Some(reference) = pending.pop_front() {
        assert!(nodes.len() < 32);
        for node in pages(services, receipt, params(receipt, "detail", &reference)).await {
            let id = node["id"].as_str().unwrap().to_owned();
            if node["parentId"].is_null() {
                assert!(root.replace(id.clone()).is_none());
            }
            if let Some(next) = node["childrenRef"].as_str() {
                pending.push_back(next.into());
            }
            assert!(nodes.insert(id, node).is_none());
        }
    }
    reconstruct(root.as_ref().unwrap(), &nodes)
}
#[intent_test_macros::daemon_test]
async fn staged_services_scrub_inserted_phantom_into_newest_dirty_inverse() {
    let phantom = "<!--anchor:00000000-0000-4000-8000-000000000001:point-->";
    assert_eq!(phantom.len(), 56);
    let replacement = format!("X{phantom}");
    let caller_result = format!("aX{phantom}b");
    assert_eq!(caller_result.encode_utf16().count(), 59);
    let (_tmp, services, workspace, note) = setup("ab").await;
    let request = captured(&services, &workspace, &note, &replacement).await;
    // Public Services commit chooses the actual parser-driven canonical plan.
    let receipt = services
        .note_operation_commit(request.clone())
        .await
        .unwrap();
    assert_eq!(receipt["sourceLength"], 3);
    assert_eq!(receipt["headerDigest"], request.header_digest);
    assert_eq!(receipt["payloadDigest"], request.payload_digest);
    assert_eq!(receipt["operationId"], request.operation_id);
    assert_eq!(receipt["scope"]["backendId"], request.backend_id);
    assert_eq!(receipt["scope"]["workspaceId"], request.workspace_id);
    assert_eq!(receipt["scope"]["noteId"], request.note_id);
    assert_eq!(receipt["scope"]["noteInstanceId"], request.note_instance_id);
    let final_source = services
        .store
        .get_note(&workspace, &note)
        .await
        .unwrap()
        .content;
    assert_eq!(final_source, "aXb");
    assert_eq!(
        services.store.list_notes(&workspace).await.unwrap().len(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM comment")
            .fetch_one(services.store.read_pool())
            .await
            .unwrap(),
        0
    );
    let effects = pages(
        &services,
        &receipt,
        params(&receipt, "effects", receipt["effectsRef"].as_str().unwrap()),
    )
    .await;
    assert_eq!(
        effects.len(),
        2,
        "one scrub plus the standard annotation invalidation"
    );
    assert_eq!(effects[1]["kind"], "annotationInvalidation");
    assert_eq!(effects[1]["sourceRevision"], receipt["afterRevision"]);
    let effect = &effects[0];
    assert_eq!(effect["kind"], "sourceEffect");
    assert_eq!(effect["reason"], "phantom-scrub");
    assert_eq!(effect["range"], json!({"start":2,"end":58}));
    assert_eq!(effect["insertedLength"], 0);
    assert_eq!(effect["beforeDigest"], digest(phantom));
    assert_eq!(effect["afterDigest"], digest(""));
    assert_ne!(effect["inputState"], effect["outputState"]);
    assert_eq!(
        detail(&services, &receipt, effect["detailRef"].as_str().unwrap()).await,
        json!({"inputState":effect["inputState"],"outputState":effect["outputState"],
            "range":{"start":2,"end":58},"removed":phantom,"inserted":""})
    );
    let operation_key: String = sqlx::query_scalar("SELECT o.operation_key FROM note_operation o JOIN note_stage s ON s.operation_key=o.operation_key WHERE o.principal=? AND s.header_digest=? AND o.backend_id=? AND o.workspace_id=? AND o.note_id=? AND o.instance_id=? AND o.operation_id=? AND o.payload_digest=?")
        .bind("daemon").bind(&request.header_digest).bind(&request.backend_id).bind(&request.workspace_id).bind(&request.note_id)
        .bind(&request.note_instance_id).bind(&request.operation_id).bind(&request.payload_digest)
        .fetch_one(services.store.read_pool()).await.unwrap();
    // Verify retained phase states, not merely predicted range arithmetic.
    for (phase, expected) in [
        ("callerResult", caller_result.as_str()),
        (
            effect["inputState"].as_str().unwrap(),
            caller_result.as_str(),
        ),
        (effect["outputState"].as_str().unwrap(), "aXb"),
    ] {
        let chunks: Vec<String> = sqlx::query_scalar(
            "SELECT text FROM note_operation_source WHERE operation_key=? AND phase=? ORDER BY start",
        )
        .bind(&operation_key).bind(phase)
        .fetch_all(services.store.read_pool())
        .await
        .unwrap();
        assert_eq!(chunks.concat(), expected);
    }
    let inverse_ref = receipt["inverseRef"].as_str().unwrap();
    let inverse = pages(
        &services,
        &receipt,
        params(&receipt, "inverse", inverse_ref),
    )
    .await;
    assert_eq!(inverse.len(), 1);
    let row = &inverse[0];
    assert_eq!(row["historyGroup"], "7");
    assert_eq!(row["inputState"], receipt["afterRevision"]);
    assert_eq!(row["outputState"], receipt["beforeRevision"]);
    assert_eq!(row["start"], 1);
    assert_eq!(row["end"], 2);
    assert_eq!(row["replacement"]["length"], 0);
    assert_eq!(row["replacement"]["utf8Bytes"], 0);
    assert_eq!(row["replacement"]["sha256"], digest(""));
    let mut text_request = params(&receipt, "inverseText", inverse_ref);
    text_request["textId"] = row["replacement"]["textId"].clone();
    text_request["offset"] = json!(0);
    let fragments = pages(&services, &receipt, text_request).await;
    assert_eq!(
        fragments,
        vec![json!({"textId":row["replacement"]["textId"],"offset":0,"text":""})]
    );
    let text = fragments
        .iter()
        .map(|item| item["text"].as_str().unwrap())
        .collect::<String>();
    assert_eq!(text, "");
    let restored = apply_note_splices(
        &final_source,
        &[NoteSplice {
            start: 1,
            end: 2,
            text,
        }],
    )
    .unwrap();
    assert_eq!(restored.source, "ab");
    assert_eq!(
        services.note_operation_commit(request).await.unwrap(),
        receipt
    );
}
