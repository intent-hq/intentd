//! Ordinary writer readiness through the existing authenticated WSS harness.
//! This does not cover standalone root CRUD, import, adoption or orphan repair.
use super::*;
use intent_core::WorkspaceRole;
use serde_json::json;

async fn current_source(writer: &mut Guest, workspace: &WorkspaceId, note: &str) -> Value {
    let frame = writer
        .call(
            "note.get",
            json!({"workspaceId":workspace,"noteId":note,
                "page":{"kind":"source","maxWireBytes":4096}}),
        )
        .await;
    assert_eq!(frame["result"]["kind"], "noteSourcePage", "{frame}");
    assert!(frame.to_string().len() <= 4096);
    assert!(frame["result"]["nextCursor"].is_null(), "{frame}");
    assert!(frame["result"]["text"].is_string(), "{frame}");
    frame["result"].clone()
}

fn params(source: &Value, page: Value) -> Value {
    let mut request = source["scope"].clone();
    request["sourceRevision"] = source["sourceRevision"].clone();
    request["page"] = page;
    request
}

async fn assert_ready(writer: &mut Guest, source: &Value, root: Option<&str>) {
    let anchored = writer
        .call(
            "comment.list",
            params(
                source,
                json!({"kind":"comments","anchorState":"anchored",
                "ranges":[{"start":0,"end":source["sourceLength"]}],
                "maxItems":8,"maxWireBytes":4096}),
            ),
        )
        .await;
    assert_eq!(anchored["result"]["kind"], "noteCommentPage", "{anchored}");
    assert!(anchored.to_string().len() <= 4096);
    assert!(anchored["result"]["nextCursor"].is_null());
    let count = usize::from(root.is_some());
    assert_eq!(anchored["result"]["items"].as_array().unwrap().len(), count);
    assert_eq!(anchored["result"]["totalThreads"], json!(count));
    assert_eq!(anchored["result"]["totalComments"], json!(count));
    let orphaned = writer
        .call(
            "comment.list",
            params(
                source,
                json!({"kind":"comments","anchorState":"orphaned",
                "ranges":[],"maxItems":8,"maxWireBytes":4096}),
            ),
        )
        .await;
    assert_eq!(orphaned["result"]["kind"], "noteCommentPage", "{orphaned}");
    assert!(orphaned.to_string().len() <= 4096);
    assert_eq!(orphaned["result"]["items"], json!([]));
    assert_eq!(orphaned["result"]["totalThreads"], 0);
    assert!(orphaned["result"]["nextCursor"].is_null());
    if let Some(root) = root {
        let item = &anchored["result"]["items"][0];
        assert_eq!(item["rootCommentId"], root);
        assert_eq!(item["rootState"], "present");
        assert!(item["anchorRef"].is_string());
        let mut request = params(
            source,
            json!({"kind":"context",
            "contextRef":item["anchorRef"],"maxItems":8,"maxWireBytes":4096}),
        );
        request["commentRevision"] = anchored["result"]["commentRevision"].clone();
        let context = writer.call("note.get", request).await;
        assert_eq!(context["result"]["kind"], "noteContextPage", "{context}");
        assert!(context.to_string().len() <= 4096);
        assert!(context["result"]["nextCursor"].is_null());
        let spans: Vec<_> = context["result"]["items"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|item| item["kind"] == "span")
            .collect();
        assert_eq!(spans.len(), 1, "{context}");
        let raw = source["text"].as_str().unwrap();
        let open = format!("<!--anchor:{root}:start-->");
        let close = format!("<!--anchor:{root}:end-->");
        let start = raw.find(&open).expect("healthy literal start") + open.len();
        let end = start + raw[start..].find(&close).expect("healthy literal end");
        assert_eq!(&raw[start..end], "hello");
        assert_eq!(
            spans[0]["sourceRange"],
            json!({
                "start":raw[..start].encode_utf16().count(),
                "end":raw[..end].encode_utf16().count()
            })
        );
    }
}

#[tokio::test]
async fn ordinary_note_writers_publish_ready_anchors_after_root_and_metadata_mutations() {
    let srv = start(WsOptions::default()).await;
    let mut writer = Guest::connect(&srv, &"91".repeat(32)).await;
    let workspace = WorkspaceId::new();
    srv.store
        .insert_workspace(&fixture_workspace(&workspace))
        .await
        .unwrap();
    srv.store
        .add_workspace_member(
            &workspace,
            &writer.principal.id,
            WorkspaceRole::Collaborator,
        )
        .await
        .unwrap();
    let created = writer
        .call(
            "note.create",
            json!({"workspaceId":workspace,"title":"Anchor writer","content":"hello world"}),
        )
        .await;
    assert!(created.get("error").is_none(), "{created}");
    let note = created["result"]["note"]["id"]
        .as_str()
        .expect("created note");
    let empty = current_source(&mut writer, &workspace, note).await;
    assert_eq!(empty["text"], "hello world");
    assert_ready(&mut writer, &empty, None).await;

    // Public comment.add inserts the root AFTER the source update in the same
    // Store transaction. A source-only finalizer would leave this read stale.
    let added = writer.call("comment.add", json!({"workspaceId":workspace,"noteId":note,
        "searchContext":"hello world","commentTarget":"hello","comment":"Root body","authorType":"user"})).await;
    let root = added["result"]["commentId"].as_str().expect("root comment");
    let anchored = current_source(&mut writer, &workspace, note).await;
    assert_ready(&mut writer, &anchored, Some(root)).await;

    let changed_text = format!("😀 header\n{}", anchored["text"].as_str().unwrap());
    let updated = writer
        .call(
            "note.update",
            json!({"workspaceId":workspace,"noteId":note,
        "content":changed_text}),
        )
        .await;
    assert!(updated.get("error").is_none(), "{updated}");
    let changed = current_source(&mut writer, &workspace, note).await;
    assert_eq!(changed["text"], changed_text);
    assert_ne!(changed["sourceRevision"], anchored["sourceRevision"]);
    assert_ready(&mut writer, &changed, Some(root)).await;

    // This route supplies metadata, not a replacement source body.
    let metadata = writer
        .call(
            "note.updateMetadata",
            json!({"workspaceId":workspace,
        "noteId":note,"title":"Renamed anchor writer","tags":["anchor-writer"]}),
        )
        .await;
    assert!(metadata.get("error").is_none(), "{metadata}");
    let after_metadata = current_source(&mut writer, &workspace, note).await;
    assert_eq!(after_metadata["text"], changed_text);
    assert_ne!(after_metadata["sourceRevision"], changed["sourceRevision"]);
    assert_ready(&mut writer, &after_metadata, Some(root)).await;
    drop(writer);
    srv.ws.stop().await;
}
