//! Annotation page-state subscriptions through the authenticated WSS harness.
use super::*;

fn bounded_annotation_push(push: &Value) {
    assert_eq!(push["kind"], "snapshot", "{push}");
    assert!(push["snapshot"].is_object());
    assert_eq!(push["snapshot"]["kind"], "notePageState");
    assert!(push.get("delta").is_none());
    assert!(
        json!({"jsonrpc":"2.0","method":"subscription.push","params":push})
            .to_string()
            .len()
            <= 4096
    );
}

#[tokio::test]
async fn annotation_wss_page_state_mutation_and_fresh_subscription_are_bounded() {
    let srv = start(WsOptions::default()).await;
    let mut writer = Guest::connect(&srv, &"73".repeat(32)).await;
    let mut subscriber = Guest::connect(&srv, &"74".repeat(32)).await;
    let ws = WorkspaceId::new();
    srv.store
        .insert_workspace(&fixture_workspace(&ws))
        .await
        .unwrap();
    srv.store
        .insert_note(&fixture_note(&ws, "spec", "hello world"))
        .await
        .unwrap();
    for principal in [&writer.principal.id, &subscriber.principal.id] {
        srv.store
            .add_workspace_member(&ws, principal, WorkspaceRole::Collaborator)
            .await
            .unwrap();
    }
    let params = json!({"workspaceId":ws,"noteId":"spec","projection":"pageState"});
    let acknowledgement = subscriber.call("comment.subscribe", params.clone()).await;
    assert!(acknowledgement["result"]["subscriptionId"].is_string());
    let first = next_subscription_push(&mut subscriber.ws).await;
    bounded_annotation_push(&first);
    let changed=writer.call("comment.add",json!({"workspaceId":ws,"noteId":"spec","searchContext":"hello world","commentTarget":"hello","comment":"body","authorType":"user"})).await;
    assert!(changed["result"]["commentId"].is_string(), "{changed}");
    let current = srv
        .store
        .read_note_page_state(&ws, &NoteId::from("spec"), None)
        .await
        .unwrap();
    let mut observed = false;
    for _ in 0..8 {
        let push = next_subscription_push(&mut subscriber.ws).await;
        bounded_annotation_push(&push);
        if push["snapshot"] == current {
            observed = true;
            break;
        }
    }
    assert!(observed);
    assert_ne!(
        first["snapshot"]["commentRevision"],
        current["commentRevision"]
    );
    assert!(
        current["stateGeneration"]
            .as_str()
            .unwrap()
            .parse::<u64>()
            .unwrap()
            > first["snapshot"]["stateGeneration"]
                .as_str()
                .unwrap()
                .parse::<u64>()
                .unwrap()
    );
    drop(subscriber);
    let mut fresh = Guest::connect(&srv, &"75".repeat(32)).await;
    srv.store
        .add_workspace_member(&ws, &fresh.principal.id, WorkspaceRole::Collaborator)
        .await
        .unwrap();
    let acknowledgement = fresh.call("comment.subscribe", params).await;
    assert!(acknowledgement["result"]["subscriptionId"].is_string());
    let push = next_subscription_push(&mut fresh.ws).await;
    bounded_annotation_push(&push);
    assert_eq!(push["seq"], 0);
    assert_eq!(push["snapshot"], current);
    drop((writer, fresh));
    srv.ws.stop().await;
}
