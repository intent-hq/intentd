//! Public standalone status/delete and legacy source/orphan publication.
use super::*;
use intent_core::WorkspaceRole;
use serde_json::json;

fn annotation_params(source: &Value, page: Value) -> Value {
    let mut params = source["scope"].clone();
    params["sourceRevision"] = source["sourceRevision"].clone();
    params["page"] = page;
    params
}

fn bounded(frame: &Value, kind: &str) {
    assert_eq!(frame["result"]["kind"], kind, "{frame}");
    assert!(frame.to_string().len() <= 4096, "{frame}");
    assert!(frame["result"]["nextCursor"].is_null(), "{frame}");
}

async fn source(writer: &mut Guest, ws: &WorkspaceId, note: &str) -> Value {
    let frame = writer
        .call(
            "note.get",
            json!({"workspaceId":ws,"noteId":note,
        "page":{"kind":"source","maxWireBytes":4096}}),
        )
        .await;
    bounded(&frame, "noteSourcePage");
    frame["result"].clone()
}

async fn threads(writer: &mut Guest, source: &Value, state: &str) -> Value {
    let ranges = if state == "anchored" {
        json!([{"start":0,"end":source["sourceLength"]}])
    } else {
        json!([])
    };
    let frame = writer
        .call(
            "comment.list",
            annotation_params(
                source,
                json!({"kind":"comments","anchorState":state,"ranges":ranges,
            "maxItems":8,"maxWireBytes":4096}),
            ),
        )
        .await;
    bounded(&frame, "noteCommentPage");
    frame["result"].clone()
}

fn context_params(source: &Value, threads: &Value) -> Value {
    let mut params = annotation_params(
        source,
        json!({"kind":"context",
        "contextRef":threads["items"][0]["anchorRef"],"maxItems":8,"maxWireBytes":4096}),
    );
    params["commentRevision"] = threads["commentRevision"].clone();
    params
}

#[tokio::test]
async fn standalone_comment_status_delete_and_source_orphaning_keep_pages_ready() {
    let srv = start(WsOptions::default()).await;
    let mut writer = Guest::connect(&srv, &"92".repeat(32)).await;
    let ws = WorkspaceId::new();
    srv.store
        .insert_workspace(&fixture_workspace(&ws))
        .await
        .unwrap();
    srv.store
        .add_workspace_member(&ws, &writer.principal.id, WorkspaceRole::Collaborator)
        .await
        .unwrap();
    let created = writer
        .call(
            "note.create",
            json!({"workspaceId":ws,
        "title":"Comment writer","content":"hello world"}),
        )
        .await;
    assert!(created.get("error").is_none(), "{created}");
    let note = created["result"]["note"]["id"]
        .as_str()
        .expect("created note");
    let added = writer.call("comment.add", json!({"workspaceId":ws,"noteId":note,
        "searchContext":"hello world","commentTarget":"hello","comment":"root","authorType":"user"})).await;
    let root = added["result"]["commentId"].as_str().expect("root");
    let responded = writer
        .call(
            "comment.respond",
            json!({"workspaceId":ws,"noteId":note,
        "threadId":root,"comment":"surviving reply","authorType":"user"}),
        )
        .await;
    let reply = responded["result"]["comment"]["id"]
        .as_str()
        .expect("reply");

    for (resolved, status) in [(true, "resolved"), (false, "open")] {
        let changed = writer
            .call(
                "comment.resolveThread",
                json!({"workspaceId":ws,
            "noteId":note,"threadId":root,"resolved":resolved}),
            )
            .await;
        assert_eq!(changed["result"]["success"], true, "{changed}");
        let current = source(&mut writer, &ws, note).await;
        let anchored = threads(&mut writer, &current, "anchored").await;
        assert_eq!(anchored["totalThreads"], 1);
        assert_eq!(anchored["totalComments"], 2);
        assert_eq!(anchored["items"][0]["rootCommentId"], root);
        assert_eq!(anchored["items"][0]["status"], status);
        assert_eq!(
            threads(&mut writer, &current, "orphaned").await["items"],
            json!([])
        );
        let context = writer
            .call("note.get", context_params(&current, &anchored))
            .await;
        bounded(&context, "noteContextPage");
        assert_eq!(
            context["result"]["items"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|item| item["kind"] == "span")
                .count(),
            1
        );
    }

    let before = source(&mut writer, &ws, note).await;
    let before_threads = threads(&mut writer, &before, "anchored").await;
    let old_context = context_params(&before, &before_threads);
    // Remove both literal markers and the original target/context so partial
    // marker recovery cannot legitimately reanchor this captured root.
    let replacement = "😀 unrelated replacement";
    let changed = writer
        .call(
            "note.update",
            json!({"workspaceId":ws,"noteId":note,
        "content":replacement}),
        )
        .await;
    assert!(changed.get("error").is_none(), "{changed}");
    let stale = writer.call("note.get", old_context).await;
    assert_eq!(stale["error"]["data"]["code"], "note-page-stale", "{stale}");
    let after = source(&mut writer, &ws, note).await;
    assert_eq!(after["text"], replacement);
    assert_eq!(
        threads(&mut writer, &after, "anchored").await["items"],
        json!([])
    );
    let orphaned = threads(&mut writer, &after, "orphaned").await;
    assert_eq!(orphaned["totalThreads"], 1);
    assert_eq!(orphaned["totalComments"], 2);
    assert_eq!(orphaned["items"][0]["rootCommentId"], root);
    assert_eq!(orphaned["items"][0]["rootState"], "present");
    let orphan_context = writer
        .call("note.get", context_params(&after, &orphaned))
        .await;
    bounded(&orphan_context, "noteContextPage");
    assert_eq!(orphan_context["result"]["orphaned"], true);
    assert!(orphan_context["result"]["items"]
        .as_array()
        .unwrap()
        .iter()
        .all(|item| item["kind"] != "span"));
    let legacy = writer
        .call(
            "comment.getThread",
            json!({"workspaceId":ws,
        "noteId":note,"threadId":root}),
        )
        .await;
    assert_eq!(
        legacy["result"]["rootComment"]["isOrphaned"], true,
        "{legacy}"
    );

    let deleted = writer
        .call(
            "comment.delete",
            json!({"workspaceId":ws,
        "noteId":note,"commentId":root}),
        )
        .await;
    assert_eq!(deleted["result"]["success"], true, "{deleted}");
    let after_delete = source(&mut writer, &ws, note).await;
    let survivor = threads(&mut writer, &after_delete, "orphaned").await;
    assert_eq!(survivor["totalThreads"], 1);
    assert_eq!(survivor["totalComments"], 1);
    assert_eq!(survivor["items"][0]["rootCommentId"], root);
    assert_eq!(survivor["items"][0]["rootState"], "deleted");
    assert_eq!(survivor["items"][0]["latestCommentId"], reply);
    assert!(survivor["items"][0]["anchorRef"].is_null());
    assert_eq!(
        threads(&mut writer, &after_delete, "anchored").await["items"],
        json!([])
    );
    drop(writer);
    srv.ws.stop().await;
}
