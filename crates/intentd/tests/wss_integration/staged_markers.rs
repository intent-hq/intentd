//! Controlled v1 marker descriptors over actual public note/comment ownership.
//! This tests server admission/lifetime, not configured native-editor capture.
use super::{boot, connect, wss_rpc, wss_rpc_raw};
use intent_core::note_stage::{NoteStageAppend, NoteStageBegin, NoteStageSeal};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::fmt::Write as _;

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
fn resource(id: &str, value: Value) -> (Value, Value) {
    let text = intent_core::note_artifact::canonical::canonical_json(&value.to_string()).unwrap();
    (
        json!({"kind":"text","id":id,"offset":0,"text":text}),
        reference(id, &text),
    )
}
fn frame_bound(id: u64, method: &str, params: &Value) {
    assert!(
        json!({"jsonrpc":"2.0","id":id,"method":method,"params":params})
            .to_string()
            .len()
            <= 4096
    );
}

#[tokio::test]
async fn public_staged_inherited_marker_seal_pins_ownership_and_preserves_replay_after_delete() {
    let fx = boot().await;
    let mut rpc = connect(fx.port, fx.cfg.clone()).await;
    let workspace = wss_rpc(
        &mut rpc,
        1,
        "workspace.create",
        json!({"title":"marker provenance","path":"."}),
    )
    .await;
    let ws = workspace["workspace"]["id"].as_str().unwrap();
    for delete_before_seal in [false, true] {
        let created = wss_rpc(
            &mut rpc,
            2,
            "note.create",
            json!({"workspaceId":ws,"title":"marker","content":"😀 hello world"}),
        )
        .await;
        let note = created["note"]["id"].as_str().unwrap();
        let added=wss_rpc(&mut rpc,3,"comment.add",json!({"workspaceId":ws,"noteId":note,"searchContext":"😀 hello world","commentTarget":"hello","comment":"Root","authorType":"user"})).await;
        let root = added["commentId"].as_str().unwrap();
        let source_frame = wss_rpc_raw(
            &mut rpc,
            4,
            "note.get",
            json!({"workspaceId":ws,"noteId":note,"page":{"kind":"source","maxWireBytes":4096}}),
        )
        .await;
        assert!(source_frame["error"].is_null(), "{source_frame}");
        assert!(source_frame.to_string().len() <= 4096);
        let page = &source_frame["result"];
        assert!(page["nextCursor"].is_null());
        let source = page["text"].as_str().unwrap();
        let literal = format!("<!--anchor:{root}:start-->");
        let byte_start = source
            .find(&literal)
            .expect("public comment.add emitted actual literal");
        let marker_start = u64::try_from(source[..byte_start].encode_utf16().count()).unwrap();
        let marker_end = marker_start + u64::try_from(literal.encode_utf16().count()).unwrap();
        let mut raw = page["scope"].clone();
        raw["operationId"] = json!(uuid::Uuid::new_v4().to_string());
        raw["expiresAt"] = json!(format!(
            "{}.000Z",
            &intent_core::iso_ms_from_now(60_000)[..19]
        ));
        raw["headerDigest"] = json!("0".repeat(64));
        raw["header"] = json!({"baseRevision":page["sourceRevision"],"editorSessionId":"marker-wss","localEditSequence":1,"liveGeneration":1,"selectionGeneration":0,"action":"read","output":"source","selection":"all"});
        let mut begin: NoteStageBegin = serde_json::from_value(raw).unwrap();
        begin.header_digest = begin.computed_digest().unwrap();
        let begin_params = serde_json::to_value(&begin).unwrap();
        frame_bound(5, "note.operation.begin", &begin_params);
        wss_rpc(&mut rpc, 5, "note.operation.begin", begin_params).await;
        let mut identity = page["scope"].clone();
        identity["operationId"] = json!(begin.operation_id);
        identity["headerDigest"] = json!(begin.header_digest);
        let (descriptor, detail) = resource(
            "descriptor",
            json!({"version":1,"nodeType":"commentAnchor","parentOrdinal":null,"nativeRange":{"from":3,"to":4},"attributesRef":"attrs"}),
        );
        let mut texts = vec![descriptor];
        for (id, value) in [
            (
                "attrs",
                json!({"id":"root","parentId":null,"type":"object","childrenRef":"directory"}),
            ),
            (
                "directory",
                json!({"kind":"metadataChildren","items":["comment-entry","id-entry","type-entry"],"nextRef":null}),
            ),
            (
                "comment-entry",
                json!({"id":"comment-attribute","parentId":"root","key":"commentId","type":"string","valueRef":"comment-value"}),
            ),
            (
                "id-entry",
                json!({"id":"id-attribute","parentId":"root","key":"id","type":"string","valueRef":"id-value"}),
            ),
            (
                "type-entry",
                json!({"id":"type-attribute","parentId":"root","key":"type","type":"string","valueRef":"type-value"}),
            ),
        ] {
            texts.push(resource(id, value).0);
        }
        for (id, text) in [
            ("comment-value", root.to_owned()),
            ("id-value", format!("{root}:start")),
            ("type-value", "start".into()),
            ("prefix", "Q😀".into()),
        ] {
            texts.push(json!({"kind":"text","id":id,"offset":0,"text":text}));
        }
        let streams = [
            ("text", texts),
            (
                "dirty",
                vec![
                    json!({"kind":"splice","localSequence":1,"ordinal":0,"start":0,"end":0,"replacement":reference("prefix","Q😀")}),
                ],
            ),
            ("selection", vec![]),
            ("mutation", vec![]),
            (
                "live",
                vec![
                    json!({"kind":"projection","ordinal":0,"role":"marker-occurrence","canonicalId":root,"sourceRange":{"start":marker_start+3,"end":marker_end+3},"detail":detail}),
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
                let params = serde_json::to_value(&chunk).unwrap();
                frame_bound(6, "note.operation.append", &params);
                wss_rpc(&mut rpc, 6, "note.operation.append", params).await;
                json!(chunk.chunk_digest)
            };
            manifest.push(json!({"stream":stream,"chunks":usize::from(count>0),"records":count,"lastDigest":digest}));
        }
        let mut seal = identity.clone();
        seal["manifest"] = json!(manifest);
        seal["payloadDigest"] = json!("0".repeat(64));
        let mut seal: NoteStageSeal = serde_json::from_value(seal).unwrap();
        seal.payload_digest = seal.computed_digest().unwrap();
        let seal_params = serde_json::to_value(&seal).unwrap();
        frame_bound(7, "note.operation.seal", &seal_params);
        let delete = json!({"workspaceId":ws,"noteId":note,"commentId":root});
        if delete_before_seal {
            assert_eq!(
                wss_rpc(&mut rpc, 8, "comment.delete", delete).await["success"],
                true
            );
            let refused = wss_rpc_raw(&mut rpc, 7, "note.operation.seal", seal_params).await;
            assert!(refused.to_string().len() <= 4096);
            assert_eq!(refused["error"]["code"], -32603, "{refused}");
            assert_eq!(
                refused["error"]["message"], "unsupported: Note operation unavailable",
                "{refused}"
            );
            assert!(refused["result"].is_null());
        } else {
            let sealed = wss_rpc(&mut rpc, 7, "note.operation.seal", seal_params.clone()).await;
            assert_eq!(sealed["phase"], "sealed");
            assert_eq!(sealed["payloadDigest"], seal.payload_digest);
            let mut read = identity.clone();
            read["kind"] = json!("source");
            read["maxItems"] = json!(1);
            read["maxSourceBytes"] = json!(4096);
            read["maxWireBytes"] = json!(4096);
            frame_bound(9, "note.operation.read", &read);
            let before = wss_rpc_raw(&mut rpc, 9, "note.operation.read", read.clone()).await;
            assert!(before["error"].is_null(), "{before}");
            assert!(before.to_string().len() <= 4096);
            let output = &before["result"];
            assert_eq!(output["scope"], page["scope"]);
            assert_eq!(output["operationId"], begin.operation_id);
            assert_eq!(output["headerDigest"], begin.header_digest);
            assert_eq!(output["payloadDigest"], seal.payload_digest);
            assert_eq!(output["expiresAt"], begin.expires_at);
            assert!(output["viewId"].as_str().is_some_and(|id| !id.is_empty()));
            assert_eq!(output["sourceLength"], source.encode_utf16().count() + 3);
            assert_eq!(output["items"].as_array().unwrap().len(), 1);
            assert_eq!(output["items"][0]["offset"], 0);
            assert_eq!(output["items"][0]["text"], format!("Q😀{source}"));
            assert!(output["nextCursor"].is_null());
            assert_eq!(
                wss_rpc(&mut rpc, 8, "comment.delete", delete).await["success"],
                true
            );
            assert_eq!(
                wss_rpc(&mut rpc, 7, "note.operation.seal", seal_params).await,
                sealed
            );
            let after = wss_rpc_raw(&mut rpc, 9, "note.operation.read", read).await;
            assert!(after.to_string().len() <= 4096);
            assert_eq!(
                after, before,
                "original frozen source/envelope survives ownership deletion"
            );
        }
        wss_rpc(&mut rpc, 10, "note.operation.cancel", identity).await;
    }
    rpc.close(None).await.unwrap();
    fx.ws.stop().await;
}
