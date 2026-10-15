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

async fn unresolved_monitor(store: &Store, workspace: &WorkspaceId) -> intent_core::PrMonitor {
    use intent_core::{AgentId, PrMonitor, PrMonitorId, PrMonitorState};
    let agent = AgentId::new();
    sqlx::query("INSERT INTO agent_session(id,workspace_id,name,status,created_at,updated_at) VALUES (?,?,'Original monitor owner','idle',?,?)")
        .bind(&agent.0).bind(&workspace.0).bind(now_iso()).bind(now_iso())
        .execute(store.write_pool()).await.unwrap();
    let row = PrMonitor {
        monitor_id: PrMonitorId::new(),
        workspace_id: workspace.clone(),
        agent_id: agent,
        repo_owner: "original".into(),
        repo_name: "imported".into(),
        pr_number: 42,
        state: PrMonitorState::Active,
        last_snapshot: Some("{\"private\":\"saved observation\"}".into()),
        baseline_snapshot: Some("original baseline bytes".into()),
        pending_changes: vec!["saved private pending content".into()],
        pending_since: Some(now_iso()),
        last_change_at: Some(now_iso()),
        last_polled_at: None,
        last_error: Some("saved private error".into()),
        created_at: now_iso(),
        updated_at: now_iso(),
    };
    assert!(store.insert_pr_monitor(&row).await.unwrap());
    sqlx::query("UPDATE pr_monitor SET target_provider=NULL,target_instance_base_url=NULL,target_project_path=NULL,target_kind=NULL,target_provenance='unresolved',target_unresolved_reason='unverified-import' WHERE monitor_id=?")
        .bind(&row.monitor_id.0).execute(store.write_pool()).await.unwrap();
    row
}

async fn check_native_unresolved_delivery(wss: bool, support: bool) {
    let h = Harness::boot_with_monitor_support(None, support).await;
    let store = Store::open(&h.dir.path().join("intentd.db")).await.unwrap();
    let row = unresolved_monitor(&store, &h.workspace).await;
    let mut ordinary = connect(&h, wss).await;
    let hello = ordinary
        .rpc("client.hello", json!({"clientId":"monitor-events-desktop"}))
        .await;
    let id = success(&hello)["clientId"].clone();
    assert_eq!(
        success(&hello)["server"]["capabilities"].get("reviewMonitoring"),
        support.then_some(&json!(1)),
    );
    let legacy_list = ordinary.rpc("prMonitor.list", h.query()).await;
    assert_eq!(success(&legacy_list)["monitors"], json!([]));
    let mut doc = connect(&h, wss).await;
    let attachment_hello = doc.rpc("client.hello", attachment(&id)).await;
    assert_eq!(
        success(&attachment_hello)["connectionRole"],
        "review-monitor"
    );
    assert_eq!(
        success(&attachment_hello)["server"]["capabilities"].get("reviewMonitoring"),
        support.then_some(&json!(1)),
    );
    let list = doc.rpc("prMonitor.list", h.query()).await;
    assert_eq!(success(&list)["monitors"][0]["monitorId"], row.monitor_id.0);
    assert_eq!(
        success(&list)["monitors"][0]["monitorAvailability"]["state"],
        "unresolved"
    );
    assert!(!list.to_string().contains("saved private"));
    assert_eq!(store.get_pr_monitor(&row.monitor_id).await.unwrap(), row);
    let subscribe = doc
        .rpc(
            "events.subscribe",
            json!({"workspaceId":h.workspace,"eventTypes":["prMonitor:*"]}),
        )
        .await;
    let sid = success(&subscribe)["subscriptionId"].clone();
    let cancelled = doc
        .rpc(
            "prMonitor.cancel",
            json!({"workspaceId":h.workspace,"monitorId":row.monitor_id}),
        )
        .await;
    assert_eq!(success(&cancelled)["ok"], true);
    let event =
        timeout(common::rpc_read_timeout(), async {
            loop {
                if let Some(pos) = doc.notices.iter().position(|v| {
                    v["method"] == "events.event" && v["params"]["subscriptionId"] == sid
                }) {
                    break doc.notices.remove(pos);
                }
                let notice = doc.next().await;
                doc.notices.push(notice);
            }
        })
        .await
        .unwrap();
    assert_eq!(event["params"]["event"]["type"], "prMonitor:cancelled");
    assert_eq!(
        event["params"]["event"]["data"]["monitorId"],
        row.monitor_id.0
    );
    assert!(!event.to_string().contains("saved private"));
    let saved = store.get_pr_monitor(&row.monitor_id).await.unwrap();
    let mut expected = row.clone();
    expected.state = intent_core::PrMonitorState::Cancelled;
    expected.updated_at.clone_from(&saved.updated_at);
    assert_eq!(saved, expected);
    if let Ok(dir) = std::env::var("INTENT_MONITOR_FIXTURE_DIR") {
        std::fs::create_dir_all(&dir).unwrap();
        let name = match (support, wss) {
            (true, true) => "producer-supported-unresolved-wss.json",
            (true, false) => "producer-supported-unresolved-uds.json",
            (false, true) => "producer-unresolved-wss.json",
            (false, false) => "producer-unresolved-uds.json",
        };
        std::fs::write(
            Path::new(&dir).join(name),
            serde_json::to_vec_pretty(&json!({
                "scope":"actual unresolved recovery only", "testHostSupport":support,
                "ordinaryHello":hello,"attachmentHello":attachment_hello,"legacyList":legacy_list,
                "list":list,"subscription":subscribe,"cancel":cancelled,"event":event,
            }))
            .unwrap(),
        )
        .unwrap();
    }
}

#[tokio::test]
async fn monitor_unresolved_real_uds_list_cancel_and_original_event() {
    check_native_unresolved_delivery(false, false).await;
}
#[tokio::test]
async fn monitor_unresolved_real_wss_list_cancel_and_original_event() {
    check_native_unresolved_delivery(true, false).await;
}

#[cfg(feature = "repository-test-fixtures")]
#[tokio::test]
async fn monitor_supported_real_uds_negotiates_list_cancel_and_event() {
    check_native_unresolved_delivery(false, true).await;
}
#[cfg(feature = "repository-test-fixtures")]
#[tokio::test]
async fn monitor_supported_real_wss_negotiates_list_cancel_and_event() {
    check_native_unresolved_delivery(true, true).await;
}
