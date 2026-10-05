//! Explicit fresh FE-upload replay against its retained actual Store identity.
//! This records Store responses, not fabricated JSON-RPC transport responses or
//! native authority. Original deadlines are never rewritten to rescue a replay.
use crate::Store;
use intent_core::{
    note_page::NotePageError, note_receipt_detail::NoteOperationReceiptRead,
    note_stage_read::NoteStageRead, Error,
};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};

fn checked_page(page: &Value, identity: &Value) {
    for field in [
        "scope",
        "operationId",
        "headerDigest",
        "payloadDigest",
        "viewId",
        "expiresAt",
        "sourceLength",
    ] {
        assert_eq!(page[field], identity[field], "{field}");
    }
    assert_eq!(page["sourceLength"], 138);
    // Measurement only: the original Store response itself is retained below.
    assert!(
        serde_json::to_vec(&json!({"jsonrpc":"2.0","id":1,"result":page}))
            .unwrap()
            .len()
            <= 8192
    );
    assert!(page["items"].as_array().unwrap().len() <= 16);
}

async fn detail_page(
    store: &Store,
    params: &Value,
    identity: &Value,
    calls: &mut Vec<Value>,
) -> Value {
    assert!(calls.len() < 10000, "capture traversal did not terminate");
    let request: NoteOperationReceiptRead = serde_json::from_value(params.clone()).unwrap();
    let normalized = request.query().unwrap();
    let response = store
        .read_note_stage_search_detail("alice", &normalized, &json!(1))
        .await
        .unwrap();
    checked_page(&response, identity);
    assert_eq!(response["outputKind"], "detail");
    calls.push(json!({"method":"note.operation.read","params":params,
        "normalizedRequest":normalized,"cursor":normalized.cursor,"offset":normalized.offset,
        "rpcId":1,"response":response}));
    response
}

async fn resolve_entry(
    store: &Store,
    base: &Value,
    entry: &Value,
    identity: &Value,
    calls: &mut Vec<Value>,
    seen: &mut BTreeSet<String>,
) -> Value {
    let id = entry["id"].as_str().unwrap();
    assert!(seen.insert(id.into()), "duplicate/cyclic metadata node");
    assert!(seen.len() <= 128, "fixed rendered tree grew unexpectedly");
    match entry["type"].as_str().unwrap() {
        "object" => {
            let mut request = base.clone();
            request["ref"] = entry["childrenRef"].clone();
            let mut object = serde_json::Map::new();
            let mut previous: Option<String> = None;
            let mut cursors = BTreeSet::new();
            loop {
                let page = detail_page(store, &request, identity, calls).await;
                for child in page["items"].as_array().unwrap() {
                    assert_eq!(child["parentId"], id);
                    let key = child["key"].as_str().unwrap();
                    if let Some(prior) = previous.as_deref() {
                        assert!(prior < key);
                    }
                    previous = Some(key.into());
                    let value =
                        Box::pin(resolve_entry(store, base, child, identity, calls, seen)).await;
                    assert!(object.insert(key.into(), value).is_none());
                }
                if let Some(cursor) = page["nextCursor"].as_str() {
                    assert!(!page["items"].as_array().unwrap().is_empty());
                    assert!(cursors.insert(cursor.to_owned()));
                    request["cursor"] = json!(cursor);
                } else {
                    break;
                }
            }
            Value::Object(object)
        }
        "string" => {
            if let Some(value) = entry.get("value") {
                assert!(entry.get("valueRef").is_none());
                assert!(value.as_str().unwrap().len() <= 1024);
                return value.clone();
            }
            assert_eq!(
                entry["key"], "renderedText",
                "only wholeleaf strings are external in this fixture"
            );
            let mut request = base.clone();
            request["ref"] = entry["valueRef"].clone();
            let mut text = String::new();
            let mut refs = BTreeSet::new();
            loop {
                assert!(refs.insert(request["ref"].as_str().unwrap().to_owned()));
                let page = detail_page(store, &request, identity, calls).await;
                assert!(page["nextCursor"].is_null());
                assert_eq!(page["items"].as_array().unwrap().len(), 1);
                let item = &page["items"][0];
                assert_eq!(item["kind"], "fragment");
                assert_eq!(item["id"], id);
                assert_eq!(item["field"], "renderedText");
                assert_eq!(item["offset"], text.encode_utf16().count());
                let part = item["text"].as_str().unwrap();
                assert!(!part.is_empty() && part.len() <= 1024);
                text.push_str(part);
                if item["nextRef"].is_null() {
                    break;
                }
                request["ref"] = item["nextRef"].clone();
            }
            json!(text)
        }
        "number" | "boolean" | "null" => entry["value"].clone(),
        other => panic!("unexpected fixed-context metadata type {other}"),
    }
}

fn uploaded_descriptors(captured: &Value) -> (Value, Value) {
    let mut texts: BTreeMap<String, String> = BTreeMap::new();
    let mut live = Vec::new();
    for call in captured["requests"].as_array().unwrap() {
        if call["method"] != "note.operation.append" {
            continue;
        }
        let params = &call["params"];
        for record in params["records"].as_array().unwrap() {
            if params["stream"] == "text" {
                let text = texts
                    .entry(record["id"].as_str().unwrap().into())
                    .or_default();
                assert_eq!(record["offset"], text.encode_utf16().count());
                text.push_str(record["text"].as_str().unwrap());
            } else if params["stream"] == "live" {
                live.push(record.clone());
            }
        }
    }
    assert_eq!(live.len(), 2);
    for (ordinal, record) in live.iter().enumerate() {
        assert_eq!(record["ordinal"], ordinal);
    }
    let descriptor = |index: usize| {
        serde_json::from_str::<Value>(&texts[live[index]["detail"]["textId"].as_str().unwrap()])
            .unwrap()
    };
    (descriptor(0), descriptor(1))
}

#[tokio::test]
#[ignore = "requires fresh original frontend rendered capture and retained phase-one database"]
async fn replay_actual_frontend_rendered_upload_and_capture_search_details() {
    let input = std::env::var("NOTE_RENDERED_FE_CAPTURE").unwrap();
    let output = std::path::PathBuf::from(std::env::var("NOTE_RENDERED_STORE_CAPTURE").unwrap());
    let retained_database = output.with_extension("db");
    assert!(
        !output.exists() && !retained_database.exists(),
        "use a new versioned capture destination"
    );
    let captured: Value = serde_json::from_slice(&std::fs::read(&input).unwrap()).unwrap();
    let source: Value = serde_json::from_slice(
        &std::fs::read(captured["sourceCapture"].as_str().unwrap()).unwrap(),
    )
    .unwrap();
    let started_at = intent_core::now_epoch_ms();
    let temporary = tempfile::tempdir().unwrap();
    let database = temporary.path().join("replay.db");
    std::fs::copy(source["retainedDatabase"].as_str().unwrap(), &database).unwrap();
    let store = Store::open(&database).await.unwrap();
    let mut calls = Vec::new();
    let mut initial = None;
    let mut cancel = None;
    let mut deadline = None;
    for call in captured["requests"].as_array().unwrap() {
        let params = call["params"].clone();
        let response = match call["method"].as_str().unwrap() {
            "note.operation.begin" => {
                assert!(deadline.replace(params["expiresAt"].clone()).is_none());
                assert_eq!(
                    params["header"]["query"],
                    json!({"text":"STRASSE","caseSensitive":false,"mode":"renderedText"})
                );
                store
                    .begin_note_stage("alice", &serde_json::from_value(params.clone()).unwrap())
                    .await
                    .unwrap()
            }
            "note.operation.append" => store
                .append_note_stage("alice", &serde_json::from_value(params.clone()).unwrap())
                .await
                .unwrap(),
            "note.operation.seal" => store
                .seal_note_stage("alice", &serde_json::from_value(params.clone()).unwrap())
                .await
                .unwrap(),
            "note.operation.read" => {
                // Preserve the exact initial FE read. Controlled response refs and
                // cursors are never replayed as if they were backend-issued tokens.
                if params["kind"] == "search" && initial.is_none() {
                    assert!(params.get("cursor").is_none());
                    initial = Some(params);
                }
                continue;
            }
            "note.operation.cancel" => {
                assert!(cancel.replace(params).is_none());
                continue;
            }
            other => panic!("unexpected captured method {other}"),
        };
        calls.push(json!({"method":call["method"],"params":params,"response":response}));
    }
    let initial = initial.unwrap();
    assert_eq!(initial["maxItems"], 16);
    assert_eq!(initial["maxSourceBytes"], 1024);
    assert_eq!(initial["maxWireBytes"], 8192);
    let mut request = initial.clone();
    let mut hits = Vec::new();
    let mut cursors = BTreeSet::new();
    let mut identity = None;
    let mut frontier = 0;
    loop {
        assert!(calls.len() < 10000, "search replay did not terminate");
        let normalized: NoteStageRead = serde_json::from_value(request.clone()).unwrap();
        let response = store
            .read_note_stage_source("alice", &normalized, &json!(1))
            .await
            .unwrap();
        if identity.is_none() {
            identity = Some(response.clone());
        }
        checked_page(&response, identity.as_ref().unwrap());
        assert_eq!(response["expiresAt"], deadline.as_ref().unwrap().clone());
        assert_eq!(response["outputKind"], "search");
        let next_frontier = response["scannedThrough"].as_u64().unwrap();
        assert!(next_frontier >= frontier);
        frontier = next_frontier;
        hits.extend(response["items"].as_array().unwrap().iter().cloned());
        assert_eq!(response["count"]["value"], hits.len());
        calls.push(json!({"method":"note.operation.read","params":request,"normalizedRequest":normalized,"cursor":normalized.cursor,"rpcId":1,"response":response}));
        if response["nextCursor"].is_null() {
            assert_eq!(response["count"]["exact"], true);
            break;
        }
        assert_eq!(response["count"]["exact"], false);
        let cursor = response["nextCursor"].as_str().unwrap();
        assert!(cursors.insert(cursor.to_owned()));
        request["cursor"] = json!(cursor);
    }
    assert_eq!(hits.len(), 17);
    assert!(frontier >= 129);
    let mut ids = BTreeSet::new();
    for (index, hit) in hits.iter().enumerate() {
        assert!(ids.insert(hit["hitId"].as_str().unwrap()));
        assert_eq!(
            hit["sourceRange"],
            json!({"start":10+index*7,"end":16+index*7})
        );
    }
    let identity = identity.unwrap();
    let (parent, leaf) = uploaded_descriptors(&captured);
    let whole_leaf = format!("{}😀", "Straße ".repeat(18));
    let mut base = initial.clone();
    base["kind"] = json!("detail");
    for hit in &hits {
        let mut request = base.clone();
        request["ref"] = hit["detailRef"].clone();
        let root = detail_page(&store, &request, &identity, &mut calls).await;
        assert!(root["nextCursor"].is_null());
        assert_eq!(root["items"].as_array().unwrap().len(), 1);
        assert!(root["items"][0]["parentId"].is_null());
        let logical = resolve_entry(
            &store,
            &base,
            &root["items"][0],
            &identity,
            &mut calls,
            &mut BTreeSet::new(),
        )
        .await;
        let start = hit["sourceRange"]["start"].as_u64().unwrap();
        let end = hit["sourceRange"]["end"].as_u64().unwrap();
        assert_eq!(
            logical,
            json!({"kind":"stagedRenderedHit","mapping":"identity","sourceRange":{"start":start,"end":end},"renderedRange":{"start":start-10,"end":end-10},
            "parent":{"ordinal":0,"sourceRange":{"start":10,"end":138},"descriptor":parent,"attributes":{}},
            "leaf":{"ordinal":1,"sourceRange":{"start":10,"end":138},"descriptor":leaf,"attributes":{},"renderedText":whole_leaf}})
        );
    }
    let mut wrong = initial.clone();
    wrong["kind"] = json!("source");
    let error = store
        .read_note_stage_source(
            "alice",
            &serde_json::from_value(wrong.clone()).unwrap(),
            &json!(1),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        &error,
        Error::NotePage(NotePageError::CursorInvalid)
    ));
    let wrong_error = format!("{error:?}");
    let cancel = cancel.unwrap();
    let cancelled = store
        .cancel_note_stage("alice", &serde_json::from_value(cancel.clone()).unwrap())
        .await
        .unwrap();
    assert_eq!(cancelled["phase"], "cancelled");
    calls.push(json!({"method":"note.operation.cancel","params":cancel,"response":cancelled}));
    let error = store
        .read_note_stage_source(
            "alice",
            &serde_json::from_value(initial.clone()).unwrap(),
            &json!(1),
        )
        .await
        .unwrap_err();
    assert!(matches!(&error, Error::NotePage(NotePageError::Expired)));
    let expired_error = format!("{error:?}");
    sqlx::query("VACUUM INTO ?")
        .bind(retained_database.to_str().unwrap())
        .execute(store.write_pool())
        .await
        .unwrap();
    std::fs::write(output,serde_json::to_vec_pretty(&json!({
        "claim":"actual Store replay of unchanged frontend upload against retained database; original responses and actual Store refs/cursors, no transport/native authority claim",
        "frontendCapture":input,"sourceCapture":captured["sourceCapture"],"startedAt":started_at,"clock":intent_core::now_epoch_ms(),"originalExpiresAt":deadline,
        "calls":calls,"retainedDatabase":retained_database,"hitCount":hits.len(),"wholeLeafText":whole_leaf,
        "refusals":[{"phase":"beforeCancel","params":wrong,"typedStoreError":"NotePage(CursorInvalid)","actualDebug":wrong_error},
            {"phase":"afterCancel","params":initial,"typedStoreError":"NotePage(Expired)","actualDebug":expired_error}]
    })).unwrap()).unwrap();
}
