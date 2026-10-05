//! Explicit artifact replay: original frontend uploads against retained Store identity.
use crate::Store;
use serde_json::{json, Value};

#[tokio::test]
#[ignore = "requires original frontend capture and its retained fixture database"]
async fn replay_actual_frontend_selection_upload_and_capture_output() {
    let input = std::env::var("NOTE_SELECTION_FE_CAPTURE").unwrap();
    let captured: Value = serde_json::from_slice(&std::fs::read(&input).unwrap()).unwrap();
    let source: Value = serde_json::from_slice(
        &std::fs::read(captured["sourceCapture"].as_str().unwrap()).unwrap(),
    )
    .unwrap();
    let temporary = tempfile::tempdir().unwrap();
    let database = temporary.path().join("replay.db");
    std::fs::copy(source["retainedDatabase"].as_str().unwrap(), &database).unwrap();
    let store = Store::open(&database).await.unwrap();
    let mut calls = Vec::new();
    let mut read = None;
    for call in captured["requests"].as_array().unwrap() {
        let params = call["params"].clone();
        let response = match call["method"].as_str().unwrap() {
            "note.operation.begin" => store
                .begin_note_stage("alice", &serde_json::from_value(params.clone()).unwrap())
                .await
                .unwrap(),
            "note.operation.append" => store
                .append_note_stage("alice", &serde_json::from_value(params.clone()).unwrap())
                .await
                .unwrap(),
            "note.operation.seal" => store
                .seal_note_stage("alice", &serde_json::from_value(params.clone()).unwrap())
                .await
                .unwrap(),
            "note.operation.read" => {
                // Controlled frontend response cursors are not backend tokens.
                // Keep the exact initial request, then use only actual Store cursors.
                if read.is_none() {
                    assert!(params.get("cursor").is_none());
                    read = Some(params);
                }
                continue;
            }
            "note.operation.cancel" => continue,
            other => panic!("unexpected captured method {other}"),
        };
        calls.push(json!({"method":call["method"],"params":params,"response":response}));
    }
    let mut request = read.unwrap();
    assert_eq!(request["maxSourceBytes"], 1024);
    assert_eq!(request["maxWireBytes"], 8192);
    let mut output = String::new();
    let mut lengths = Vec::new();
    loop {
        let response = store
            .read_note_stage_source(
                "alice",
                &serde_json::from_value(request.clone()).unwrap(),
                &json!(1),
            )
            .await
            .unwrap();
        assert_eq!(response["sourceLength"], 2060);
        let item = &response["items"][0];
        assert_eq!(item["offset"], output.encode_utf16().count());
        let text = item["text"].as_str().unwrap();
        lengths.push(text.len());
        output.push_str(text);
        assert!(
            serde_json::to_vec(&json!({"jsonrpc":"2.0","id":1,"result":response}))
                .unwrap()
                .len()
                <= 8192
        );
        calls.push(json!({"method":"note.operation.read","params":request,"response":response}));
        if response["nextCursor"].is_null() {
            break;
        }
        request["cursor"] = response["nextCursor"].clone();
    }
    assert_eq!(lengths, [1024, 1024, 2]);
    assert_eq!(output, captured["expected"].as_str().unwrap());
    std::fs::write(
        std::env::var("NOTE_SELECTION_STORE_CAPTURE").unwrap(),
        serde_json::to_vec_pretty(&json!({
            "claim":"actual Store replay of unchanged frontend begin/append/seal; actual Store output cursors",
            "frontendCapture":input,"sourceCapture":captured["sourceCapture"],
            "clock":intent_core::now_epoch_ms(),"calls":calls
        })).unwrap(),
    ).unwrap();
}
