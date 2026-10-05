//! Replay unchanged FE uploads in their retained database, never extend a lease.
use crate::Store;
use intent_core::{
    note_page::NotePageError,
    note_stage::{NoteStageBegin, NoteStageSeal},
    note_stage_read::NoteStageRead,
};
use serde_json::{json, Value};
use std::{collections::BTreeSet, io::Write as _};

#[tokio::test]
#[ignore = "requires fresh original frontend marker-selection capture and its retained database"]
async fn replay_actual_frontend_marker_selection_upload_and_capture_output_lifecycle() {
    let input = std::env::var("NOTE_MARKER_SELECTION_FE_CAPTURE").unwrap();
    let output_path =
        std::path::PathBuf::from(std::env::var("NOTE_MARKER_SELECTION_STORE_CAPTURE").unwrap());
    let retained_database = output_path.with_extension("db");
    assert!(
        !output_path.exists() && !retained_database.exists(),
        "never overwrite original evidence"
    );
    let captured: Value = serde_json::from_slice(&std::fs::read(&input).unwrap()).unwrap();
    let source: Value = serde_json::from_slice(
        &std::fs::read(captured["sourceCapture"].as_str().unwrap()).unwrap(),
    )
    .unwrap();
    assert_eq!(source["principal"], "alice");
    let tmp = tempfile::tempdir().unwrap();
    let database = tmp.path().join("replay.db");
    std::fs::copy(source["retainedDatabase"].as_str().unwrap(), &database).unwrap();
    let store = Store::open(&database).await.unwrap();
    let original_root = store
        .get_comment(source["commentId"].as_str().unwrap())
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_value(&original_root).unwrap(),
        source["originalRoot"],
        "reopened root must match the independent phase-one ownership capture"
    );

    let expected_source = format!(
        "prefix😀\n\n{} <!--anchor:11111111-1111-4111-8111-111111111111:point--> {}",
        "A".repeat(1024),
        "B".repeat(1024)
    );
    let expected_output = format!("{}  {}", "A".repeat(1024), "B".repeat(1024));
    assert_eq!(source["source"], expected_source);
    assert_eq!(source["expectedSelection"], expected_output);
    assert_eq!(captured["expected"], expected_output);
    let mut uploaded_text_ids = BTreeSet::new();
    let mut uploaded_live = Vec::new();
    let mut uploaded_selection = Vec::new();
    let mut calls = Vec::new();
    let mut begin = None;
    let mut seal = None;
    let mut initial = None;
    let mut cancel = None;
    for call in captured["requests"].as_array().unwrap() {
        let params = call["params"].clone();
        let response = match call["method"].as_str().unwrap() {
            "note.operation.begin" => {
                let typed: NoteStageBegin = serde_json::from_value(params.clone()).unwrap();
                assert_eq!(typed.header_digest, typed.computed_digest().unwrap());
                let lexical = &source["calls"][0]["response"];
                assert_eq!(json!(typed.scope()), lexical["scope"]);
                assert_eq!(typed.header.base_revision, lexical["sourceRevision"]);
                let original_expiry =
                    intent_core::parse_iso(lexical["expiresAt"].as_str().unwrap()).unwrap();
                let staged_expiry = intent_core::parse_iso(&typed.expires_at).unwrap();
                assert!(
                    staged_expiry <= original_expiry,
                    "stage must not extend original lexical lifetime; shorter/truncated deadlines are allowed"
                );

                assert_eq!(params["header"]["action"], "read");
                assert_eq!(params["header"]["output"], "selectionMarkdown");
                assert_eq!(params["header"]["selection"], "ranges");
                // Clean capture means no dirty records, not an invented zero history fence.
                assert!(begin.replace(typed.clone()).is_none());
                store.begin_note_stage("alice", &typed).await.unwrap()
            }
            "note.operation.append" => {
                assert!(begin.is_some() && seal.is_none());
                assert!(
                    matches!(
                        params["stream"].as_str(),
                        Some("text" | "live" | "selection")
                    ),
                    "clean marker-selection slice has no dirty/mutation records"
                );
                let records = params["records"].as_array().unwrap();
                match params["stream"].as_str().unwrap() {
                    "text" => {
                        for record in records {
                            assert_eq!(record["kind"], "text");
                            uploaded_text_ids.insert(record["id"].as_str().unwrap().to_owned());
                        }
                    }
                    "live" => uploaded_live.extend(records.iter().cloned()),
                    "selection" => uploaded_selection.extend(records.iter().cloned()),
                    _ => unreachable!(),
                }
                store
                    .append_note_stage("alice", &serde_json::from_value(params.clone()).unwrap())
                    .await
                    .unwrap()
            }
            "note.operation.seal" => {
                let typed: NoteStageSeal = serde_json::from_value(params.clone()).unwrap();
                assert_eq!(typed.payload_digest, typed.computed_digest().unwrap());
                let response = store.seal_note_stage("alice", &typed).await.unwrap();
                assert_eq!(response["phase"], "sealed");
                assert_eq!(response["viewLength"], 2116);
                assert_eq!(response["payloadDigest"], typed.payload_digest);
                assert!(seal.replace(typed).is_none());
                response
            }
            "note.operation.read" => {
                if initial.is_none() {
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
    assert_eq!(uploaded_text_ids.len(), 16);
    assert_eq!(uploaded_live.len(), 4);
    assert_eq!(
        uploaded_selection,
        vec![json!({"kind":"range","ordinal":0,"start":10,"end":2116,
        "direction":"backward","anchorAffinity":"after","headAffinity":"after"})],
        "actual native affinities are compared, never rewritten"
    );
    for (ordinal, (role, start, end)) in [
        ("selection-owner", 10, 2116),
        ("inline-span", 10, 1035),
        ("marker-occurrence", 1035, 1091),
        ("inline-span", 1091, 2116),
    ]
    .into_iter()
    .enumerate()
    {
        assert_eq!(uploaded_live[ordinal]["ordinal"], ordinal);
        assert_eq!(uploaded_live[ordinal]["role"], role);
        assert_eq!(
            uploaded_live[ordinal]["sourceRange"],
            json!({"start":start,"end":end})
        );
    }
    assert_eq!(uploaded_live[2]["canonicalId"], source["commentId"]);
    let begin = begin.unwrap();
    let seal = seal.unwrap();
    let initial = initial.unwrap();
    assert_eq!(initial["kind"], "selectionMarkdown");
    assert_eq!(initial["maxItems"], 64);
    assert_eq!(initial["maxSourceBytes"], 1024);
    assert_eq!(initial["maxWireBytes"], 8192);
    assert!(
        initial.get("payloadDigest").is_none(),
        "header-only selector"
    );
    let normalized: NoteStageRead = serde_json::from_value(initial.clone()).unwrap();
    assert_eq!(normalized.scope(), begin.scope());
    assert_eq!(normalized.operation_id, begin.operation_id);
    assert_eq!(normalized.header_digest, begin.header_digest);
    let scope = begin.scope();
    let (operation,view,pin,root_key):(String,String,String,String)=sqlx::query_as("SELECT s.operation_key,s.view_id,s.marker_admission,s.root_key FROM note_stage s JOIN note_operation o USING(operation_key) WHERE o.principal=? AND o.backend_id=? AND o.workspace_id=? AND o.note_id=? AND o.instance_id=? AND o.operation_id=? AND o.method_kind='staged' AND s.header_digest=? AND s.payload_digest=? AND s.phase='sealed'")
        .bind("alice").bind(&scope.backend_id).bind(&scope.workspace_id).bind(&scope.note_id).bind(&scope.note_instance_id).bind(&begin.operation_id).bind(&begin.header_digest).bind(&seal.payload_digest).fetch_one(store.read_pool()).await.unwrap();
    let rows:Vec<(String,String)>=sqlx::query_as("SELECT id,value FROM note_stage_validation WHERE operation_key=? AND kind='live' ORDER BY CAST(id AS INTEGER)").bind(&operation).fetch_all(store.read_pool()).await.unwrap();
    assert_eq!(rows.len(), 4);
    assert_eq!(rows[0].0, "0");
    assert_eq!(rows[1].0, "1");
    assert_eq!(rows[2].0, "2");
    assert_eq!(rows[3].0, "3");
    let ledger: Vec<Value> = rows
        .iter()
        .map(|(_, value)| serde_json::from_str(value).unwrap())
        .collect();
    for (ordinal, (node, from, to)) in [
        ("paragraph", 0, 2053),
        ("text", 1, 1026),
        ("commentAnchor", 1026, 1027),
        ("text", 1027, 2052),
    ]
    .into_iter()
    .enumerate()
    {
        assert_eq!(ledger[ordinal]["nodeType"], node);
        assert_eq!(ledger[ordinal]["nativeRange"], json!({"from":from,"to":to}));
        assert_eq!(
            ledger[ordinal]["parentOrdinal"],
            if ordinal == 0 { Value::Null } else { json!(0) }
        );
    }
    assert_eq!(
        ledger[1]["attributesRef"], ledger[3]["attributesRef"],
        "native producer shares explicit text attributes"
    );
    assert!(ledger[1]["attributesRef"].is_string());
    let marker = &ledger[2];
    let witness = &marker["markerWitness"];
    assert_eq!(witness["version"], 1);
    assert_eq!(witness["canonicalId"], source["commentId"]);
    assert_eq!(witness["type"], "point");
    assert_eq!(
        &witness["threadId"],
        source["originalRoot"]
            .get("threadId")
            .expect("captured root thread")
    );
    assert_eq!(witness["rootKey"], root_key);

    assert_eq!(witness["rootRange"], json!({"start":1035,"end":1091}));
    assert_eq!(witness["viewId"], view);
    assert_eq!(
        witness["admission"],
        serde_json::from_str::<Value>(&pin).unwrap()
    );
    for field in [
        "headId",
        "sourceRev",
        "commentRevision",
        "stateGeneration",
        "sourceRevision",
    ] {
        let captured_field = source["internalStoreOracle"]["annotation"]
            .get(field)
            .expect("phase-one annotation field is required");
        assert_eq!(
            &witness["admission"][field], captured_field,
            "retained witness must bind original phase-one {field}"
        );
    }

    let mut request = initial.clone();
    let mut output = String::new();
    let mut lengths = Vec::new();
    let mut cursors = BTreeSet::new();
    loop {
        assert!(
            calls.len() < 128,
            "bounded marker selection capture must terminate"
        );
        let normalized: NoteStageRead = serde_json::from_value(request.clone()).unwrap();
        let response = store
            .read_note_stage_source("alice", &normalized, &json!(1))
            .await
            .unwrap();
        assert_eq!(response["scope"], json!(begin.scope()));
        assert_eq!(response["operationId"], begin.operation_id);
        assert_eq!(response["headerDigest"], begin.header_digest);
        assert_eq!(response["payloadDigest"], seal.payload_digest);
        assert_eq!(response["expiresAt"], begin.expires_at);
        assert_eq!(response["viewId"], view);
        assert_eq!(response["sourceLength"], 2116);
        assert_eq!(response["outputKind"], "selectionMarkdown");
        assert!(response["items"].as_array().unwrap().len() <= 64);
        let mut page_bytes = 0;
        for item in response["items"].as_array().unwrap() {
            assert_eq!(item["offset"], output.encode_utf16().count());
            let text = item["text"].as_str().unwrap();
            assert!(!text.is_empty() && text.len() <= 1024);
            page_bytes += text.len();
            output.push_str(text);
        }
        assert!(
            page_bytes > 0 && page_bytes <= 1024,
            "aggregate response source budget"
        );
        lengths.push(page_bytes);
        assert!(lengths.len() <= 3);
        assert_eq!(
            response["nextCursor"].is_null(),
            lengths.len() == 3,
            "exact two continuations then terminal third response"
        );
        assert!(
            json!({"jsonrpc":"2.0","id":1,"result":response})
                .to_string()
                .len()
                <= 8192
        );
        calls.push(json!({"method":"note.operation.read","params":request,"normalizedRequest":normalized,"cursor":normalized.cursor,"rpcId":1,"response":response}));
        if response["nextCursor"].is_null() {
            break;
        }
        assert!(!response["items"].as_array().unwrap().is_empty());
        assert!(cursors.insert(response["nextCursor"].as_str().unwrap().to_owned()));
        request["cursor"] = response["nextCursor"].clone();
    }
    assert_eq!(output, expected_output);
    assert_eq!(lengths, [1024, 1024, 2]);
    assert_eq!(output.encode_utf16().count(), 2050);
    let mut wrong = initial.clone();
    wrong["kind"] = json!("source");
    super::assert_error(
        store
            .read_note_stage_source(
                "alice",
                &serde_json::from_value(wrong.clone()).unwrap(),
                &json!(1),
            )
            .await,
        NotePageError::CursorInvalid,
    );
    let cancel = cancel.unwrap();
    for field in [
        "backendId",
        "workspaceId",
        "noteId",
        "noteInstanceId",
        "operationId",
        "headerDigest",
    ] {
        assert_eq!(cancel[field], initial[field], "original cancel {field}");
    }

    let cancelled = store
        .cancel_note_stage("alice", &serde_json::from_value(cancel.clone()).unwrap())
        .await
        .unwrap();
    assert_eq!(cancelled["phase"], "cancelled");
    calls.push(json!({"method":"note.operation.cancel","params":cancel,"response":cancelled}));
    super::assert_error(
        store
            .read_note_stage_source(
                "alice",
                &serde_json::from_value(initial.clone()).unwrap(),
                &json!(1),
            )
            .await,
        NotePageError::Expired,
    );
    let final_note = store
        .get_note(
            &intent_core::WorkspaceId(scope.workspace_id.clone()),
            &intent_core::NoteId(scope.note_id.clone()),
        )
        .await
        .unwrap();
    assert_eq!(final_note.content, source["source"].as_str().unwrap());
    sqlx::query("VACUUM INTO ?")
        .bind(retained_database.to_str().unwrap())
        .execute(store.write_pool())
        .await
        .unwrap();
    let artifact = json!({"claim":"actual Store replay of unchanged native begin/append/seal uploads; real selection-output cursors and EOF cancellation, no native authority inferred from Store",
        "frontendCapture":input,"sourceCapture":captured["sourceCapture"],"clock":intent_core::now_epoch_ms(),"capturedAtMs":source["capturedAtMs"],"calls":calls,"retainedDatabase":retained_database,
        "internalStoreOracle":{"sealedViewId":view,"sealedRootKey":root_key,"markerWitness":witness,"liveLedger":rows,"markerAdmission":serde_json::from_str::<Value>(&pin).unwrap()},
        "unchangedSource":final_note.content,"output":output,"outputPageLengths":lengths,
        "refusals":[{"phase":"beforeCancel","params":wrong,"typedStoreError":"NotePage(CursorInvalid)"},{"phase":"afterCancel","params":initial,"typedStoreError":"NotePage(Expired)"}]});
    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(output_path)
        .unwrap()
        .write_all(&serde_json::to_vec_pretty(&artifact).unwrap())
        .unwrap();
}
