//! Real public v2 upload, rendered search, metadata directories and whole-leaf fragments.
use super::{boot, connect, wss_rpc, wss_rpc_raw};
use intent_core::note_stage::{NoteStageAppend, NoteStageBegin, NoteStageSeal};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::fmt::Write as _;

fn resource(id: &str, value: &Value) -> (Value, Value) {
    let text = intent_core::note_artifact::canonical::canonical_json(&value.to_string()).unwrap();
    let reference = json!({"textId":id,"length":text.encode_utf16().count(),"utf8Bytes":text.len(),"sha256":Sha256::digest(text.as_bytes()).iter().fold(String::with_capacity(64), |mut output, byte| { write!(output, "{byte:02x}").expect("writing to a String"); output })});
    (
        json!({"kind":"text","id":id,"offset":0,"text":text}),
        reference,
    )
}

#[tokio::test]
async fn rendered_search_upload_preserves_unicode_and_owned_detail_lifecycle() {
    let fx = boot().await;
    let mut rpc = connect(fx.port, fx.cfg.clone()).await;
    let workspace = wss_rpc(
        &mut rpc,
        1,
        "workspace.create",
        json!({"title":"selection","path":"."}),
    )
    .await;
    let ws = workspace["workspace"]["id"].as_str().unwrap();
    let start = 65538_u64;
    let captured = " Straße😀 Straße ";
    let source = format!("{}\n\n{captured}\n\ntail", "x".repeat(65536));
    let created = wss_rpc(
        &mut rpc,
        2,
        "note.create",
        json!({"workspaceId":ws,"title":"selection","content":source}),
    )
    .await;
    let note = created["note"]["id"].as_str().unwrap();
    let page = wss_rpc(
        &mut rpc,
        3,
        "note.get",
        json!({"workspaceId":ws,"noteId":note,"page":{"kind":"source"}}),
    )
    .await;
    let mut value = page["scope"].clone();
    value["operationId"] = json!(uuid::Uuid::new_v4().to_string());
    value["expiresAt"] = json!(format!(
        "{}.000Z",
        &intent_core::iso_ms_from_now(60_000)[..19]
    ));
    value["headerDigest"] = json!("0".repeat(64));
    value["header"] = json!({"baseRevision":page["sourceRevision"],"editorSessionId":"selection-wss","localEditSequence":0,"liveGeneration":7,"selectionGeneration":9,"action":"read","output":"search","selection":"ranges","query":{"mode":"renderedText","text":"STRASSE","caseSensitive":false}});
    let mut begin: NoteStageBegin = serde_json::from_value(value).unwrap();
    begin.header_digest = begin.computed_digest().unwrap();
    wss_rpc(
        &mut rpc,
        4,
        "note.operation.begin",
        serde_json::to_value(&begin).unwrap(),
    )
    .await;
    let mut identity = page["scope"].clone();
    identity["operationId"] = json!(begin.operation_id);
    identity["headerDigest"] = json!(begin.header_digest);
    let (attrs, _) = resource(
        "attrs",
        &json!({"id":"attrs-root","parentId":null,"type":"object","childrenRef":"attrs-directory"}),
    );
    let (directory, _) = resource(
        "attrs-directory",
        &json!({"kind":"metadataChildren","items":[],"nextRef":null}),
    );
    let (paragraph, paragraph_ref) = resource(
        "paragraph",
        &json!({"version":1,"nodeType":"paragraph","parentOrdinal":null,"nativeRange":{"from":0,"to":19},"attributesRef":"attrs"}),
    );
    let rendered_ref = json!({"textId":"captured","length":captured.encode_utf16().count(),"utf8Bytes":captured.len(),"sha256":Sha256::digest(captured.as_bytes()).iter().fold(String::with_capacity(64), |mut out, byte| {write!(out,"{byte:02x}").unwrap();out})});
    let (inline, inline_ref) = resource(
        "inline",
        &json!({"version":2,"nodeType":"text","parentOrdinal":0,"nativeRange":{"from":1,"to":18},"attributesRef":"attrs","renderedText":rendered_ref}),
    );
    let streams = [
        (
            "text",
            vec![
                attrs,
                directory,
                paragraph,
                inline,
                json!({"kind":"text","id":"captured","offset":0,"text":captured}),
            ],
        ),
        ("dirty", vec![]),
        (
            "selection",
            vec![
                json!({"kind":"range","ordinal":0,"start":start+1,"end":start+9,"direction":"backward","anchorAffinity":"after","headAffinity":"before"}),
            ],
        ),
        ("mutation", vec![]),
        (
            "live",
            vec![
                json!({"kind":"projection","ordinal":0,"sourceRange":{"start":start,"end":start+17},"role":"selection-owner","detail":paragraph_ref}),
                json!({"kind":"projection","ordinal":1,"sourceRange":{"start":start,"end":start+17},"role":"inline-span","detail":inline_ref}),
            ],
        ),
    ];
    let mut manifest = Vec::new();
    for (stream, records) in streams {
        let count = records.len();
        let digest = if count == 0 {
            Value::Null
        } else {
            let mut chunk = identity.clone();
            chunk["stream"] = json!(stream);
            chunk["sequence"] = json!(0);
            chunk["previousDigest"] = Value::Null;
            chunk["records"] = json!(records);
            chunk["chunkDigest"] = json!("0".repeat(64));
            let mut chunk: NoteStageAppend = serde_json::from_value(chunk).unwrap();
            chunk.chunk_digest = chunk.computed_digest().unwrap();
            wss_rpc(
                &mut rpc,
                5,
                "note.operation.append",
                serde_json::to_value(&chunk).unwrap(),
            )
            .await;
            json!(chunk.chunk_digest)
        };
        manifest.push(json!({"stream":stream,"chunks":usize::from(count>0),"records":count,"lastDigest":digest}));
    }
    let mut seal = identity.clone();
    seal["manifest"] = json!(manifest);
    seal["payloadDigest"] = json!("0".repeat(64));
    let mut seal: NoteStageSeal = serde_json::from_value(seal).unwrap();
    seal.payload_digest = seal.computed_digest().unwrap();
    let sealed = wss_rpc(
        &mut rpc,
        6,
        "note.operation.seal",
        serde_json::to_value(&seal).unwrap(),
    )
    .await;
    let mut read = identity.clone();
    read["kind"] = json!("search");
    read["maxItems"] = json!(1);
    read["maxSourceBytes"] = json!(4);
    read["maxWireBytes"] = json!(4096);
    assert_eq!(sealed["viewLength"], source.encode_utf16().count());
    let mut hits = Vec::new();
    let mut view = Value::Null;
    for turn in 0..20 {
        let frame = wss_rpc_raw(&mut rpc, 7, "note.operation.read", read.clone()).await;
        assert!(frame["error"].is_null(), "{frame}");
        assert!(frame.to_string().len() <= 4096);
        let result = &frame["result"];
        assert_envelope(
            result,
            &identity,
            &seal,
            &begin,
            source.encode_utf16().count(),
            &mut view,
        );
        hits.extend(result["items"].as_array().unwrap().iter().cloned());
        if result["nextCursor"].is_null() {
            assert_eq!(result["count"], json!({"value":1,"exact":true}));
            break;
        }
        assert!(turn < 19);
        read["cursor"] = result["nextCursor"].clone();
    }
    assert_eq!(hits.len(), 1);
    assert_eq!(
        hits[0]["sourceRange"],
        json!({"start":start+1,"end":start+7})
    );
    let mut detail = identity.clone();
    detail["kind"] = json!("detail");
    detail["maxItems"] = json!(2);
    detail["maxSourceBytes"] = json!(4);
    detail["maxWireBytes"] = json!(4096);
    let mut pending = std::collections::VecDeque::from([hits[0]["detailRef"].clone()]);
    let mut seen = std::collections::HashSet::new();
    let mut raw = String::new();
    let mut calls = 0;
    let mut directories = 0;
    while let Some(reference) = pending.pop_front() {
        assert!(seen.insert(reference.to_string()));
        detail["ref"] = reference;
        detail.as_object_mut().unwrap().remove("cursor");
        loop {
            calls += 1;
            assert!(calls <= 256);
            let frame = wss_rpc_raw(&mut rpc, 8, "note.operation.read", detail.clone()).await;
            assert!(frame["error"].is_null(), "{frame}");
            assert!(frame.to_string().len() <= 4096);
            let result = &frame["result"];
            assert_envelope(
                result,
                &identity,
                &seal,
                &begin,
                source.encode_utf16().count(),
                &mut view,
            );
            assert_eq!(result["outputKind"], "detail");
            for item in result["items"].as_array().unwrap() {
                if item["field"] == "renderedText" {
                    assert_eq!(item["offset"], raw.encode_utf16().count());
                    let text = item["text"].as_str().unwrap();
                    assert!(!text.is_empty() && text.len() <= 4);
                    raw.push_str(text);
                    if !item["nextRef"].is_null() {
                        pending.push_back(item["nextRef"].clone());
                    }
                }
                for key in ["childrenRef", "valueRef"] {
                    if item[key].is_string() {
                        pending.push_back(item[key].clone());
                    }
                }
            }
            if result["nextCursor"].is_null() {
                break;
            }
            directories += 1;
            detail["cursor"] = result["nextCursor"].clone();
        }
    }
    assert_eq!(raw, captured);
    assert!(directories > 0);
    let mut wrong = read.clone();
    wrong["kind"] = json!("source");
    assert!(wss_rpc_raw(&mut rpc, 9, "note.operation.read", wrong).await["error"].is_object());
    wss_rpc(&mut rpc, 10, "note.operation.cancel", identity.clone()).await;
    detail["ref"] = hits[0]["detailRef"].clone();
    detail.as_object_mut().unwrap().remove("cursor");
    assert!(wss_rpc_raw(&mut rpc, 11, "note.operation.read", detail).await["error"].is_object());
    let current = wss_rpc(
        &mut rpc,
        11,
        "note.get",
        json!({"workspaceId":ws,"noteId":note,"page":{"kind":"source"}}),
    )
    .await;
    assert_eq!(current["sourceRevision"], page["sourceRevision"]);
    rpc.close(None).await.unwrap();
    fx.ws.stop().await;
}

fn assert_envelope(
    result: &Value,
    identity: &Value,
    seal: &NoteStageSeal,
    begin: &NoteStageBegin,
    length: usize,
    view: &mut Value,
) {
    for key in ["operationId", "headerDigest"] {
        assert_eq!(result[key], identity[key]);
    }
    for key in ["backendId", "workspaceId", "noteId", "noteInstanceId"] {
        assert_eq!(result["scope"][key], identity[key]);
    }
    assert_eq!(result["payloadDigest"], seal.payload_digest);
    assert_eq!(result["expiresAt"], begin.expires_at);
    assert_eq!(result["sourceLength"], length);
    assert!(result["viewId"].as_str().is_some_and(|id| !id.is_empty()));
    if view.is_null() {
        *view = result["viewId"].clone();
    } else {
        assert_eq!(&result["viewId"], view);
    }
}
