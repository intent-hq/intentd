//! Controlled point-root ownership via ordinary Store insertion; public staged
//! transport thereafter. This is not native capture or public comment creation.
use super::{boot, connect, wss_rpc, wss_rpc_raw};
use intent_core::note_stage::{NoteStageAppend, NoteStageBegin, NoteStageSeal};
use intent_core::{Comment, WorkspaceId};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::fmt::Write as _;

fn resource(id: &str, value: &Value) -> (Value, Value) {
    let text = intent_core::note_artifact::canonical::canonical_json(&value.to_string()).unwrap();
    raw_resource(id, &text)
}
fn raw_resource(id: &str, text: &str) -> (Value, Value) {
    let hash =
        Sha256::digest(text.as_bytes())
            .iter()
            .fold(String::with_capacity(64), |mut out, byte| {
                write!(out, "{byte:02x}").unwrap();
                out
            });
    (
        json!({"kind":"text","id":id,"offset":0,"text":text}),
        json!({"textId":id,"length":text.encode_utf16().count(),"utf8Bytes":text.len(),"sha256":hash}),
    )
}
fn request_bound(id: u64, method: &str, params: &Value) {
    assert!(
        json!({"jsonrpc":"2.0","id":id,"method":method,"params":params})
            .to_string()
            .len()
            <= 4096
    );
}

#[tokio::test]
async fn point_marker_selection_preserves_seam_spaces_and_historical_pages_after_root_delete() {
    let fx = boot().await;
    let mut rpc = connect(fx.port, fx.cfg.clone()).await;
    let workspace = wss_rpc(
        &mut rpc,
        1,
        "workspace.create",
        json!({"title":"marker selection","path":"."}),
    )
    .await;
    let ws = workspace["workspace"]["id"].as_str().unwrap();
    // A  B is exactly the minimum four-byte output budget, hence terminal.
    // The second independent oracle exercises real output-offset continuation.
    for (left, right, expected, expected_pages) in
        [("A ", " B", "A  B", 1), ("ab ", " cd", "ab  cd", 2)]
    {
        let root_id = uuid::Uuid::new_v4().to_string();
        let literal = format!("<!--anchor:{root_id}:point-->");
        let source = format!("{left}{literal}{right}");
        let created = wss_rpc(
            &mut rpc,
            2,
            "note.create",
            json!({"workspaceId":ws,"title":"point selection","content":source}),
        )
        .await;
        let note = created["note"]["id"].as_str().unwrap();
        let now = intent_core::now_iso();
        let root: Comment = serde_json::from_value(json!({"id":root_id,"threadId":root_id,"noteId":note,"type":"comment","content":"Point root","author":"fixture","authorType":"user","status":"open","anchor":{"type":"point","pointId":format!("{root_id}:point")},"createdAt":now,"updatedAt":now})).unwrap();
        fx.store
            .insert_comment(&WorkspaceId::from(ws), &root)
            .await
            .unwrap();
        let page = wss_rpc(
            &mut rpc,
            3,
            "note.get",
            json!({"workspaceId":ws,"noteId":note,"page":{"kind":"source","maxWireBytes":4096}}),
        )
        .await;
        assert_eq!(page["text"], source);
        assert!(page["nextCursor"].is_null());
        let mut raw = page["scope"].clone();
        raw["operationId"] = json!(uuid::Uuid::new_v4().to_string());
        raw["expiresAt"] = json!(format!(
            "{}.000Z",
            &intent_core::iso_ms_from_now(60_000)[..19]
        ));
        raw["headerDigest"] = json!("0".repeat(64));
        raw["header"] = json!({"baseRevision":page["sourceRevision"],"editorSessionId":"marker-selection-wss","localEditSequence":0,"liveGeneration":1,"selectionGeneration":1,"action":"read","output":"selectionMarkdown","selection":"ranges"});
        let mut begin: NoteStageBegin = serde_json::from_value(raw).unwrap();
        begin.header_digest = begin.computed_digest().unwrap();
        let params = serde_json::to_value(&begin).unwrap();
        request_bound(4, "note.operation.begin", &params);
        wss_rpc(&mut rpc, 4, "note.operation.begin", params).await;
        let mut identity = page["scope"].clone();
        identity["operationId"] = json!(begin.operation_id);
        identity["headerDigest"] = json!(begin.header_digest);
        let l = left.len();
        let r = right.len();
        let m = literal.len();
        let mut texts = Vec::new();
        let mut details = Vec::new();
        for (id, node, parent, from, to, attrs) in [
            (
                "paragraph",
                "paragraph",
                Value::Null,
                0,
                l + r + 3,
                "paragraph-attrs",
            ),
            ("left", "text", json!(0), 1, l + 1, "text-attrs"),
            (
                "point",
                "commentAnchor",
                json!(0),
                l + 1,
                l + 2,
                "marker-attrs",
            ),
            ("right", "text", json!(0), l + 2, l + r + 2, "text-attrs"),
        ] {
            let (record, reference) = resource(
                id,
                &json!({"version":1,"nodeType":node,"parentOrdinal":parent,"nativeRange":{"from":from,"to":to},"attributesRef":attrs}),
            );
            texts.push(record);
            details.push(reference);
        }
        for (id, entry, directory) in [
            (
                "paragraph-attrs",
                "paragraph-attrs-root",
                "paragraph-directory",
            ),
            ("text-attrs", "text-attrs-root", "text-directory"),
        ] {
            texts.push(
                resource(
                    id,
                    &json!({"id":entry,"parentId":null,"type":"object","childrenRef":directory}),
                )
                .0,
            );
            texts.push(
                resource(
                    directory,
                    &json!({"kind":"metadataChildren","items":[],"nextRef":null}),
                )
                .0,
            );
        }
        texts.push(resource("marker-attrs",&json!({"id":"marker-root","parentId":null,"type":"object","childrenRef":"marker-directory"})).0);
        texts.push(resource("marker-directory",&json!({"kind":"metadataChildren","items":["comment-entry","id-entry","type-entry"],"nextRef":null})).0);
        for (id, key, reference) in [
            ("comment-entry", "commentId", "comment-value"),
            ("id-entry", "id", "id-value"),
            ("type-entry", "type", "type-value"),
        ] {
            texts.push(resource(id,&json!({"id":format!("entry-{key}"),"parentId":"marker-root","key":key,"type":"string","valueRef":reference})).0);
        }
        for (id, text) in [
            ("comment-value", root_id.clone()),
            ("id-value", format!("{root_id}:point")),
            ("type-value", "point".into()),
        ] {
            texts.push(raw_resource(id, &text).0);
        }
        assert_eq!(texts.len(), 16);
        let live: Vec<_> = [("selection-owner",0,source.len()),("inline-span",0,l),("marker-occurrence",l,l+m),("inline-span",l+m,source.len())].into_iter().enumerate().map(|(ordinal,(role,start,end))| {
            let mut value=json!({"kind":"projection","ordinal":ordinal,"sourceRange":{"start":start,"end":end},"role":role,"detail":details[ordinal]});
            if ordinal==2 {value["canonicalId"]=json!(root_id);} value
        }).collect();
        let streams = [
            ("text", texts),
            ("dirty", vec![]),
            (
                "selection",
                vec![
                    json!({"kind":"range","ordinal":0,"start":0,"end":source.len(),"direction":"backward","anchorAffinity":"after","headAffinity":"before"}),
                ],
            ),
            ("mutation", vec![]),
            ("live", live),
        ];
        let mut manifest = Vec::new();
        for (stream, records) in streams {
            let mut previous = Value::Null;
            let mut chunks = 0;
            // Four resources per request keep the actual escaped upload frame bounded.
            for (sequence, part) in records.chunks(4).enumerate() {
                let mut raw = identity.clone();
                raw["stream"] = json!(stream);
                raw["sequence"] = json!(sequence);
                raw["previousDigest"] = previous;
                raw["records"] = json!(part);
                raw["chunkDigest"] = json!("0".repeat(64));
                let mut append: NoteStageAppend = serde_json::from_value(raw).unwrap();
                append.chunk_digest = append.computed_digest().unwrap();
                let params = serde_json::to_value(&append).unwrap();
                request_bound(5, "note.operation.append", &params);
                wss_rpc(&mut rpc, 5, "note.operation.append", params).await;
                previous = json!(append.chunk_digest);
                chunks += 1;
            }
            manifest.push(json!({"stream":stream,"chunks":chunks,"records":records.len(),"lastDigest":previous}));
        }
        let mut raw = identity.clone();
        raw["manifest"] = json!(manifest);
        raw["payloadDigest"] = json!("0".repeat(64));
        let mut seal: NoteStageSeal = serde_json::from_value(raw).unwrap();
        seal.payload_digest = seal.computed_digest().unwrap();
        let seal_params = serde_json::to_value(&seal).unwrap();
        request_bound(6, "note.operation.seal", &seal_params);
        let sealed = wss_rpc(&mut rpc, 6, "note.operation.seal", seal_params.clone()).await;
        assert_eq!(sealed["phase"], "sealed");
        assert_eq!(sealed["payloadDigest"], seal.payload_digest);
        assert_eq!(sealed["viewLength"], source.len());
        let mut first = identity.clone();
        first["kind"] = json!("selectionMarkdown");
        first["maxItems"] = json!(1);
        first["maxSourceBytes"] = json!(4);
        first["maxWireBytes"] = json!(4096);
        let mut view = None;
        let mut originals = Vec::new();
        for deleted in [false, true] {
            if deleted {
                assert_eq!(
                    wss_rpc(
                        &mut rpc,
                        8,
                        "comment.delete",
                        json!({"workspaceId":ws,"noteId":note,"commentId":root_id})
                    )
                    .await["success"],
                    true
                );
                assert_eq!(
                    wss_rpc(&mut rpc, 6, "note.operation.seal", seal_params.clone()).await,
                    sealed
                );
            }
            let mut read = first.clone();
            let mut output = String::new();
            let mut count = 0;
            loop {
                request_bound(7, "note.operation.read", &read);
                let frame = wss_rpc_raw(&mut rpc, 7, "note.operation.read", read.clone()).await;
                assert!(frame["error"].is_null(), "{frame}");
                assert!(frame.to_string().len() <= 4096);
                let result = &frame["result"];
                assert_eq!(result["scope"], page["scope"]);
                assert_eq!(result["operationId"], begin.operation_id);
                assert_eq!(result["headerDigest"], begin.header_digest);
                assert_eq!(result["payloadDigest"], seal.payload_digest);
                assert_eq!(result["expiresAt"], begin.expires_at);
                assert_eq!(result["sourceLength"], source.len());
                assert_eq!(result["outputKind"], "selectionMarkdown");
                assert!(result["viewId"].as_str().is_some_and(|id| !id.is_empty()));
                if let Some(view) = &view {
                    assert_eq!(&result["viewId"], view);
                } else {
                    view = Some(result["viewId"].clone());
                }
                assert_eq!(result["items"].as_array().unwrap().len(), 1);
                assert_eq!(result["items"][0]["offset"], output.len());
                let text = result["items"][0]["text"].as_str().unwrap();
                assert!(!text.is_empty() && text.len() <= 4);
                output.push_str(text);
                if deleted {
                    assert_eq!(
                        frame, originals[count],
                        "exact historical frame after ownership deletion"
                    );
                } else {
                    originals.push(frame.clone());
                }
                count += 1;
                if result["nextCursor"].is_null() {
                    break;
                }
                assert!(count < expected_pages);
                read["cursor"] = result["nextCursor"].clone();
            }
            assert_eq!(output, expected);
            assert_eq!(count, expected_pages);
        }
        wss_rpc(&mut rpc, 9, "note.operation.cancel", identity).await;
        let refused = wss_rpc_raw(&mut rpc, 7, "note.operation.read", first).await;
        assert!(refused.to_string().len() <= 4096);
        assert!(refused["error"].is_object());
        assert!(refused["result"].is_null());
    }
    rpc.close(None).await.unwrap();
    fx.ws.stop().await;
}
