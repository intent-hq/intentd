//! Annotation opt-in through the existing authenticated WSS harness.
use super::*;

fn annotation_request(source: &Value, page: Value, epoch: Option<&Value>) -> Value {
    let mut params = source["scope"].clone();
    params["sourceRevision"] = source["sourceRevision"].clone();
    params["page"] = page;
    if let Some(epoch) = epoch {
        params["commentRevision"] = epoch.clone();
    }
    params
}

fn assert_annotation_frame(frame: &Value, kind: &str) {
    assert_eq!(frame["result"]["kind"], kind, "{frame}");
    assert!(frame.to_string().len() <= 4096);
    assert!(frame["result"].get("rootComment").is_none());
    assert!(frame["result"].get("replies").is_none());
}

#[tokio::test]
async fn annotation_wss_pages_context_revocation_and_deleted_root_preserve_identity() {
    let srv = start(WsOptions::default()).await;
    let mut alice = Guest::connect(&srv, &"71".repeat(32)).await;
    let mut bob = Guest::connect(&srv, &"72".repeat(32)).await;
    let ws = WorkspaceId::new();
    srv.store
        .insert_workspace(&fixture_workspace(&ws))
        .await
        .unwrap();
    let note = fixture_note(&ws, "spec", "hello world");
    srv.store.insert_note(&note).await.unwrap();
    for principal in [&alice.principal.id, &bob.principal.id] {
        srv.store
            .add_workspace_member(&ws, principal, WorkspaceRole::Collaborator)
            .await
            .unwrap();
    }
    let body = "\"\\\n\0😀".repeat(2000);
    for index in 0..20 {
        let comment: intent_core::Comment = serde_json::from_value(json!({"id":format!("comment{index:02}"),"threadId":"thread-not-root","noteId":"spec","type":"comment","content":body,"author":"Alice","authorType":"user","authorPrincipalId":alice.principal.id,"status":"open","parentId":if index==0{Value::Null}else{json!("comment00")},"createdAt":format!("2026-10-05T00:00:{index:02}Z"),"updatedAt":"2026-10-05T00:00:00Z"})).unwrap();
        srv.store.insert_comment(&ws, &comment).await.unwrap();
    }
    let source = alice
        .call(
            "note.get",
            json!({"workspaceId":ws,"noteId":"spec","page":{"kind":"source"}}),
        )
        .await;
    assert_eq!(source["result"]["kind"], "noteSourcePage");
    let mut params = annotation_request(
        &source["result"],
        json!({"kind":"replies","maxItems":2,"maxWireBytes":4096}),
        None,
    );
    params["threadId"] = json!("thread-not-root");
    let first = alice.call("comment.getThread", params.clone()).await;
    assert_annotation_frame(&first, "noteReplyPage");
    assert_eq!(first["result"]["threadId"], "thread-not-root");
    assert_eq!(first["result"]["rootCommentId"], "comment00");
    assert_eq!(first["result"]["rootState"], "present");
    assert_eq!(first["result"]["totalComments"], 20);
    let root = &first["result"]["items"][0];
    assert_eq!(root["commentId"], "comment00");
    assert!(root["authorPrincipalIdRef"].is_string());
    let mut context = annotation_request(
        &source["result"],
        json!({"kind":"context","contextRef":root["bodyRef"],"maxWireBytes":4096}),
        Some(&first["result"]["commentRevision"]),
    );
    let mut reconstructed = String::new();
    loop {
        let frame = alice.call("note.get", context.clone()).await;
        assert_annotation_frame(&frame, "noteContextPage");
        for fragment in frame["result"]["items"].as_array().unwrap() {
            assert_eq!(
                fragment["offset"].as_u64().unwrap(),
                u64::try_from(reconstructed.encode_utf16().count()).unwrap()
            );
            reconstructed.push_str(fragment["text"].as_str().unwrap());
        }
        if frame["result"]["nextCursor"].is_null() {
            break;
        }
        context["page"]["cursor"] = frame["result"]["nextCursor"].clone();
    }
    assert_eq!(reconstructed, body);
    let legacy = alice
        .call(
            "comment.getThread",
            json!({"workspaceId":ws,"noteId":"spec","threadId":"thread-not-root"}),
        )
        .await;
    assert_eq!(legacy["result"]["rootComment"]["content"], body);
    assert_eq!(legacy["result"]["replies"].as_array().unwrap().len(), 19);
    params["commentRevision"] = first["result"]["commentRevision"].clone();
    params["page"]["cursor"] = first["result"]["nextCursor"].clone();
    let crossed = bob.call("comment.getThread", params.clone()).await;
    assert_eq!(crossed["error"]["data"]["code"], "note-page-cursor-invalid");
    srv.store
        .remove_workspace_member(&ws, &alice.principal.id)
        .await
        .unwrap();
    let revoked = alice.call("comment.getThread", params.clone()).await;
    assert_eq!(revoked["error"]["data"]["code"], "not-found");
    assert!(!revoked.to_string().contains("comment00"));
    srv.store
        .add_workspace_member(&ws, &alice.principal.id, WorkspaceRole::Collaborator)
        .await
        .unwrap();
    let deletion = alice
        .call(
            "comment.delete",
            json!({"workspaceId":ws,"noteId":"spec","commentId":"comment00"}),
        )
        .await;
    assert_eq!(deletion["result"]["success"], true, "{deletion}");
    let stale = alice.call("comment.getThread", params).await;
    assert_eq!(stale["error"]["data"]["code"], "note-page-stale");
    let source = alice
        .call(
            "note.get",
            json!({"workspaceId":ws,"noteId":"spec","page":{"kind":"source"}}),
        )
        .await;
    let mut params = annotation_request(
        &source["result"],
        json!({"kind":"replies","maxItems":2,"maxWireBytes":4096}),
        None,
    );
    params["threadId"] = json!("thread-not-root");
    let survivor = alice.call("comment.getThread", params).await;
    assert_annotation_frame(&survivor, "noteReplyPage");
    assert_eq!(survivor["result"]["rootCommentId"], "comment00");
    assert_eq!(survivor["result"]["rootState"], "deleted");
    assert_eq!(survivor["result"]["items"][0]["commentId"], "comment01");
    let summary = alice
        .call(
            "comment.list",
            annotation_request(
                &source["result"],
                json!({"kind":"comments","ranges":[],"anchorState":"all","maxWireBytes":4096}),
                None,
            ),
        )
        .await;
    assert_annotation_frame(&summary, "noteCommentPage");
    assert_eq!(summary["result"]["items"][0]["rootCommentId"], "comment00");
    assert_eq!(summary["result"]["items"][0]["rootState"], "deleted");
    assert!(summary["result"]["items"][0]["anchorRef"].is_null());
    drop((alice, bob));
    srv.ws.stop().await;
}
