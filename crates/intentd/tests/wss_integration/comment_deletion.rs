//! Deletion delivery through authenticated collaborator admission and egress.
use super::*;
use serde_json::json;

#[tokio::test]
async fn collaborator_comment_deletion_refreshes_and_removes_thread() {
    let srv = start(WsOptions::default()).await;
    let mut writer = Guest::connect(&srv, &"da".repeat(32)).await;
    let mut subscriber = Guest::connect(&srv, &"db".repeat(32)).await;
    let mut outsider = Guest::connect(&srv, &"dc".repeat(32)).await;
    let workspace = WorkspaceId::new();
    srv.store
        .insert_workspace(&fixture_workspace(&workspace))
        .await
        .unwrap();
    for principal in [&writer.principal, &subscriber.principal] {
        srv.store
            .add_workspace_member(
                &workspace,
                &principal.id,
                intent_core::WorkspaceRole::Collaborator,
            )
            .await
            .unwrap();
    }
    let note = writer
        .call(
            "note.create",
            json!({"workspaceId":workspace,"title":"N","content":"hello world"}),
        )
        .await;
    let note_id = note["result"]["note"]["id"].as_str().expect("created note");
    let root = writer.call("comment.add", json!({"workspaceId":workspace,"noteId":note_id,"searchContext":"hello world","commentTarget":"hello","comment":"root","authorType":"user"})).await;
    let root_id = root["result"]["commentId"].as_str().expect("root");
    let reply = subscriber.call("comment.respond", json!({"workspaceId":workspace,"noteId":note_id,"threadId":root_id,"comment":"reply","authorType":"user"})).await;
    let reply_id = reply["result"]["comment"]["id"].as_str().expect("reply");
    let params = json!({"workspaceId":workspace,"noteId":note_id});
    // Collection channels acknowledge admission but project an empty snapshot
    // when the caller cannot read the workspace; mutations still fail.
    let outsider_sub = outsider.call("comment.subscribe", params.clone()).await;
    assert!(outsider_sub["result"]["subscriptionId"].is_string());
    let outsider_snapshot = next_subscription_push(&mut outsider.ws).await;
    assert_eq!(outsider_snapshot["kind"], "snapshot");
    assert_eq!(outsider_snapshot["snapshot"], json!([]));
    let acknowledgement = subscriber.call("comment.subscribe", params).await;
    let sub_id = acknowledgement["result"]["subscriptionId"]
        .as_str()
        .expect("subscription");
    let snapshot = next_subscription_push(&mut subscriber.ws).await;
    assert_eq!(snapshot["kind"], "snapshot");
    assert_eq!(snapshot["seq"], 0);
    assert_eq!(snapshot["snapshot"][0]["commentCount"], 2);
    assert_eq!(
        snapshot["snapshot"][0]["latestCommentAuthorPrincipalId"],
        subscriber.principal.id.as_str()
    );
    assert_eq!(
        snapshot["snapshot"][0]["comments"][0]["authorPrincipalId"],
        writer.principal.id.as_str()
    );
    let denied = outsider
        .call(
            "comment.delete",
            json!({"workspaceId":workspace,"noteId":note_id,"commentId":reply_id}),
        )
        .await;
    assert!(denied.get("error").is_some(), "{denied}");
    for (seq, comment) in [(1, reply_id), (2, root_id)] {
        let deleted = writer
            .call(
                "comment.delete",
                json!({"workspaceId":workspace,"noteId":note_id,"commentId":comment}),
            )
            .await;
        assert_eq!(deleted["result"]["success"], true, "{deleted}");
        let push = next_subscription_push(&mut subscriber.ws).await;
        assert_eq!(push["subscriptionId"], sub_id);
        assert_eq!(push["kind"], "delta");
        assert_eq!(push["seq"], seq);
        let fresh = writer
            .call(
                "comment.list",
                json!({"workspaceId":workspace,"noteId":note_id,"includeComments":true}),
            )
            .await;
        if seq == 1 {
            assert_eq!(push["delta"]["updated"], fresh["result"]["threads"]);
            assert_eq!(push["delta"]["updated"][0]["commentCount"], 1);
            assert_eq!(
                push["delta"]["updated"][0]["latestCommentAuthorPrincipalId"],
                writer.principal.id.as_str()
            );
        } else {
            assert_eq!(push["delta"]["removedIds"], json!([root_id]));
            assert_eq!(fresh["result"]["threads"], json!([]));
        }
    }
    srv.ws.stop().await;
}
