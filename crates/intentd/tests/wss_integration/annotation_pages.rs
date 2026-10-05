//! Annotation page-state subscriptions through the authenticated WSS harness.
use super::*;
use intent_core::WorkspaceRole;
use serde_json::json;

fn bounded_annotation_push(push: &Value) {
    assert_eq!(push["kind"], "snapshot", "{push}");
    assert!(push["snapshot"].is_object());
    assert_eq!(push["snapshot"]["kind"], "notePageState");
    assert!(push.get("delta").is_none());
    assert!(push.get("payload").is_none());
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
    let reconnect_principal = subscriber.principal.clone();
    drop(subscriber);
    // Reconnect the existing credential; creating a new Guest would create a
    // different principal and would not prove same-caller reconnect behavior.
    let url = format!("wss://localhost:{}/ws?token={}", srv.port, "74".repeat(32));
    let mut reconnected = Guest {
        principal: reconnect_principal,
        ws: common::wss_connect_with_retry(srv.port, srv.cfg.clone(), &url).await,
        next_id: 100,
    };
    let reconnect_ack = reconnected.call("comment.subscribe", params.clone()).await;
    assert!(reconnect_ack["result"]["subscriptionId"].is_string());
    let reconnect_push = next_subscription_push(&mut reconnected.ws).await;
    bounded_annotation_push(&reconnect_push);
    assert_eq!(reconnect_push["seq"], 0);
    assert_eq!(reconnect_push["snapshot"], current);
    // Both opt-in channels expose the same persisted generation and epochs.
    let note_ack = reconnected.call("note.subscribe", params.clone()).await;
    assert!(note_ack["result"]["subscriptionId"].is_string());
    let note_push = next_subscription_push(&mut reconnected.ws).await;
    bounded_annotation_push(&note_push);
    assert_eq!(note_push["seq"], 0);
    assert_eq!(note_push["snapshot"], current);
    let mut fresh = Guest::connect(&srv, &"75".repeat(32)).await;
    srv.store
        .add_workspace_member(&ws, &fresh.principal.id, WorkspaceRole::Collaborator)
        .await
        .unwrap();
    let acknowledgement = fresh.call("comment.subscribe", params.clone()).await;
    assert!(acknowledgement["result"]["subscriptionId"].is_string());
    let push = next_subscription_push(&mut fresh.ws).await;
    bounded_annotation_push(&push);
    assert_eq!(push["seq"], 0);
    assert_eq!(push["snapshot"], current);
    for method in ["comment.subscribe", "note.subscribe"] {
        let mut invalid = params.clone();
        invalid["projection"] = json!("unknown-projection");
        let rejected = fresh.call(method, invalid).await;
        assert_eq!(rejected["error"]["code"], -32602, "{rejected}");
    }
    let legacy = fresh
        .call(
            "comment.subscribe",
            json!({"workspaceId":ws,"noteId":"spec"}),
        )
        .await;
    assert!(legacy["result"]["subscriptionId"].is_string());
    let legacy_push = next_subscription_push(&mut fresh.ws).await;
    assert!(legacy_push["snapshot"].is_array(), "{legacy_push}");
    assert_eq!(legacy_push["snapshot"].as_array().unwrap().len(), 1);
    drop((writer, fresh, reconnected));
    srv.ws.stop().await;
}

#[tokio::test]
async fn annotation_wss_attribution_compute_publishes_ready_state_without_changing_other_epochs() {
    let srv = start(WsOptions::default()).await;
    let mut writer = Guest::connect(&srv, &"76".repeat(32)).await;
    let mut subscriber = Guest::connect(&srv, &"77".repeat(32)).await;
    let ws = WorkspaceId::new();
    srv.store
        .insert_workspace(&fixture_workspace(&ws))
        .await
        .unwrap();
    srv.store
        .insert_note(&fixture_note(&ws, "spec", "one\ntwo"))
        .await
        .unwrap();
    for principal in [&writer.principal.id, &subscriber.principal.id] {
        srv.store
            .add_workspace_member(&ws, principal, WorkspaceRole::Collaborator)
            .await
            .unwrap();
    }
    let ack = subscriber
        .call(
            "comment.subscribe",
            json!({"workspaceId":ws,"noteId":"spec","projection":"pageState"}),
        )
        .await;
    assert!(ack["result"]["subscriptionId"].is_string(), "{ack}");
    let initial = next_subscription_push(&mut subscriber.ws).await;
    bounded_annotation_push(&initial);
    assert_eq!(initial["snapshot"]["attributionState"], "pending");
    let compute = writer
        .call(
            "note.lineAttribution.computeNow",
            json!({"workspaceId":ws,"noteId":"spec"}),
        )
        .await;
    assert_eq!(compute["result"]["ok"], true, "{compute}");
    let ready = srv
        .store
        .read_note_page_state(&ws, &NoteId::from("spec"), None)
        .await
        .unwrap();
    assert_eq!(
        ready["attributionState"], "ready",
        "legacy computation must publish the normalized generation too"
    );
    assert_eq!(
        ready["sourceRevision"],
        initial["snapshot"]["sourceRevision"]
    );
    assert_eq!(
        ready["commentRevision"],
        initial["snapshot"]["commentRevision"]
    );
    assert_ne!(
        ready["attributionGeneration"],
        initial["snapshot"]["attributionGeneration"]
    );
    let mut observed_ready = false;
    for _ in 0..8 {
        let pushed = next_subscription_push(&mut subscriber.ws).await;
        bounded_annotation_push(&pushed);
        assert_eq!(
            pushed["snapshot"]["sourceRevision"],
            ready["sourceRevision"]
        );
        assert_eq!(
            pushed["snapshot"]["commentRevision"],
            ready["commentRevision"]
        );
        if pushed["snapshot"] == ready {
            observed_ready = true;
            break;
        }
    }
    assert!(observed_ready);
    let mut request = ready["scope"].clone();
    request["sourceRevision"] = ready["sourceRevision"].clone();
    request["attributionGeneration"] = ready["attributionGeneration"].clone();
    request["page"] = json!({"kind":"attribution","ranges":[],"maxWireBytes":4096});
    let page = writer.call("note.lineAttribution.load", request).await;
    assert_eq!(page["result"]["kind"], "noteAttributionPage", "{page}");
    assert_eq!(page["result"]["state"], "ready");
    assert_eq!(
        page["result"]["attributionGeneration"],
        ready["attributionGeneration"]
    );
    assert!(page.to_string().len() <= 4096);
    drop((writer, subscriber));
    srv.ws.stop().await;
}
