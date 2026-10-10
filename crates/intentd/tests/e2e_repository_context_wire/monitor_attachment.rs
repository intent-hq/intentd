//! Real daemon, Store, registry and native sockets; no provider calls.
use super::*;

async fn connect(h: &Harness, wss: bool) -> Client {
    if wss {
        h.wss(TOKEN).await
    } else {
        h.uds().await
    }
}

fn attachment(id: &Value) -> Value {
    json!({"clientId":id,"connectionRole":"review-monitor","capabilities":{"reviewMonitoring":1}})
}

async fn check_native_attachment(wss: bool) {
    let h = Harness::boot().await;
    let store = Store::open(&h.dir.path().join("intentd.db")).await.unwrap();
    let mut original = connect(&h, wss).await;
    let profile = json!({"clientId":"monitor-desktop", "name":"Original desktop",
        "hostname":"original-host", "deviceKind":"desktop",
        "capabilities":{"browserExec":true,"reviewMonitoring":1}});
    let hello = original.rpc("client.hello", profile.clone()).await;
    let canonical = success(&hello)["clientId"].clone();
    assert!(
        success(&hello)["server"]["capabilities"]
            .get("reviewMonitoring")
            .is_none(),
        "production support stays disabled; explicit candidate protocol test only"
    );
    let client_id = intent_core::ClientId::from_string(canonical.as_str().unwrap());
    let persisted = store.get_client(&client_id).await.unwrap().unwrap();
    let before = original.rpc("client.list", json!({})).await;
    let before = success(&before).clone();
    let pin = original
        .rpc(
            "workspace.setBrowserClient",
            json!({"workspaceId":h.workspace,"clientId":canonical}),
        )
        .await;
    let pin = success(&pin).clone();
    let presence = original.rpc("presence.snapshot", h.query()).await;
    let presence = success(&presence).clone();

    let mut a = connect(&h, wss).await;
    let mut b = connect(&h, wss).await;
    for document in [&mut a, &mut b] {
        let result = document.rpc("client.hello", attachment(&canonical)).await;
        assert_eq!(success(&result)["clientId"], canonical);
        assert_eq!(success(&result)["connectionRole"], "review-monitor");
    }
    assert_eq!(
        store.get_client(&client_id).await.unwrap().unwrap(),
        persisted
    );
    let listed = original.rpc("client.list", json!({})).await;
    assert_eq!(
        success(&listed),
        &before,
        "attachments do not create or reprofile logical devices"
    );
    let current_pin = original.rpc("workspace.getBrowserClient", h.query()).await;
    assert_eq!(success(&current_pin), &pin);
    let current_presence = original.rpc("presence.snapshot", h.query()).await;
    assert_eq!(success(&current_presence), &presence);

    // Changed or missing role on an established document never writes a profile.
    for role in [Value::Null, json!("unknown"), json!(false)] {
        let mut params = profile.clone();
        params["connectionRole"] = role;
        let result = a.rpc("client.hello", params).await;
        assert_eq!(result["error"]["code"], -32602);
    }
    let result = a.rpc("client.hello", profile.clone()).await;
    assert_eq!(result["error"]["code"], -32602);
    assert_eq!(
        store.get_client(&client_id).await.unwrap().unwrap(),
        persisted
    );
    drop(a);
    let response = b.rpc("client.hello", attachment(&canonical)).await;
    assert_eq!(
        success(&response)["connectionRole"],
        "review-monitor",
        "other document survives"
    );

    // A foreign authenticated principal cannot attach via a public canonical id.
    let mut foreign = h.wss(GUEST).await;
    let response = foreign.rpc("client.hello", attachment(&canonical)).await;
    assert_eq!(response["error"]["code"], -32602);

    // The established ordinary socket cannot turn into an attachment.
    let refused = original.rpc("client.hello", attachment(&canonical)).await;
    assert_eq!(refused["error"]["code"], -32602);
    let response = b.rpc("client.hello", attachment(&canonical)).await;
    assert_eq!(
        response["error"]["code"], -32602,
        "source rehello retired old document"
    );
    success(&original.rpc("client.hello", profile.clone()).await);
    let response = b.rpc("client.hello", attachment(&canonical)).await;
    assert_eq!(
        response["error"]["code"], -32602,
        "old document cannot reparent"
    );

    // Two ordinary sources with the same identity are ambiguous, not latest-wins.
    let mut duplicate = connect(&h, wss).await;
    success(&duplicate.rpc("client.hello", profile).await);
    let mut fresh = connect(&h, wss).await;
    let response = fresh.rpc("client.hello", attachment(&canonical)).await;
    assert_eq!(response["error"]["code"], -32602);
    drop(duplicate);
    drop(original);
    // Observe actual transport cleanup, not an arbitrary delay.
    let mut observer = h.uds().await;
    timeout(common::rpc_read_timeout(), async {
        loop {
            let list = observer.rpc("client.list", json!({})).await;
            if success(&list)["clients"]
                .as_array()
                .unwrap()
                .iter()
                .all(|row| row["clientId"] != canonical)
            {
                break;
            }
            // timing-guard: wait for the actual source transport close to retire registry entries
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let response = fresh.rpc("client.hello", attachment(&canonical)).await;
    assert_eq!(
        response["error"]["code"], -32602,
        "attachment-only sockets cannot keep source online"
    );
}

#[tokio::test]
async fn monitor_attachment_real_uds_preserves_original_profile_and_registry() {
    check_native_attachment(false).await;
}

#[tokio::test]
async fn monitor_attachment_real_wss_preserves_original_profile_and_registry() {
    check_native_attachment(true).await;
}
