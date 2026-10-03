//! Real HTTP forge → services → authenticated, origin-checked TLS WSS.
use super::*;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

const BASE: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const HEAD: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

fn configure(mock: &qwen::MockQwen) {
    mock.edit(|s| {
        s.nodes.clear();
        s.pr["state"] = json!("OPEN");
        s.pr["mergeStateStatus"] = json!("CLEAN");
        s.pr["headRefOid"] = json!(HEAD);
        s.pr["baseRef"] = json!({"target":{"oid":BASE}});
        s.pr["baseRefOid"] = json!("cccccccccccccccccccccccccccccccccccccccc");
        s.pr["headRepository"] = json!({"id":"fork-fixture"});
        s.pr["updatedAt"] = json!("");
    });
}

async fn browser_connect(
    fx: &Fixture,
    origin: &str,
    token: &str,
) -> Result<TlsWs, tokio_tungstenite::tungstenite::Error> {
    timeout(common::rpc_read_timeout(), async {
        let tcp = TcpStream::connect((Ipv4Addr::LOCALHOST, fx.port))
            .await
            .unwrap();
        let tls = tokio_rustls::TlsConnector::from(fx.cfg.clone())
            .connect(ServerName::try_from("localhost").unwrap(), tcp)
            .await
            .expect("pinned TLS handshake");
        let mut request = format!("wss://localhost:{}/ws", fx.port)
            .into_client_request()
            .unwrap();
        request
            .headers_mut()
            .insert("Origin", origin.parse().unwrap());
        request
            .headers_mut()
            .insert("Authorization", format!("Bearer {token}").parse().unwrap());
        tokio_tungstenite::client_async(request, tls)
            .await
            .map(|(ws, _)| ws)
    })
    .await
    .expect("WSS upgrade timeout")
}

async fn list(fx: &Fixture, rpc: &mut TlsWs) -> Value {
    let envelope = wss_call(rpc, 20, "prMonitor.list", json!({"workspaceId":fx.ws_id})).await;
    assert_eq!(envelope["jsonrpc"], "2.0");
    assert_eq!(envelope["id"], 20);
    assert!(envelope.get("error").is_none(), "{envelope}");
    assert_eq!(envelope["result"]["monitors"].as_array().unwrap().len(), 1);
    envelope["result"]["monitors"][0].clone()
}

fn known(count: u64) -> Value {
    json!({"status":"known","baseSha":BASE,"headSha":HEAD,"behindBy":count})
}

#[intent_test_macros::daemon_test]
async fn ancestry_checklist_matrix_over_authenticated_wss() {
    for (state, count, unreadable, queued, ready) in [
        ("CLEAN", 1, false, false, true),
        ("BEHIND", 1, false, false, false),
        ("DIRTY", 1, false, false, false),
        ("CLEAN", 0, false, false, true),
        ("CLEAN", 0, true, false, true),
        ("CLEAN", 1, false, true, false),
        ("BEHIND", 0, true, false, false),
    ] {
        let mock = qwen::MockQwen::start(10978).await;
        configure(&mock);
        mock.edit(|s| {
            s.pr["mergeStateStatus"] = json!(state);
            s.mergeable = if state == "DIRTY" {
                "CONFLICTING"
            } else {
                "MERGEABLE"
            }
            .into();
            s.in_merge_queue = queued;
            s.compare = json!({"behind_by":count});
            if unreadable {
                s.compare_status = 404;
                s.compare = json!({"message":"fork commit unavailable"});
            }
        });
        let fx = boot_with_source_control(Some(mock.sc.clone())).await;
        let mut sub = browser_connect(&fx, "http://localhost", TOKEN)
            .await
            .unwrap();
        let subscribed = wss_call(
            &mut sub,
            1,
            "events.subscribe",
            json!({
                "workspaceId":fx.ws_id,"eventTypes":["prMonitor:registered"]
            }),
        )
        .await;
        assert_eq!(subscribed["jsonrpc"], "2.0");
        assert_eq!(subscribed["id"], 1);
        assert!(subscribed["result"]["subscriptionId"].is_string());
        let (monitor, requirements) = fx
            .services
            .pr_monitor_register(&fx.ws_id, &fx.agent_id, "o", "r", 10978)
            .await
            .unwrap();
        let requirements = serde_json::to_value(requirements.unwrap()).unwrap();
        let ancestry = if unreadable {
            json!({"status":"unknown"})
        } else {
            known(count)
        };
        assert_eq!(requirements["ancestry"], ancestry);
        let event = next_event(&mut sub, "prMonitor:registered").await;
        assert_eq!(
            event["data"],
            json!({
                "workspaceId":fx.ws_id,"agentId":fx.agent_id,"monitorId":monitor.monitor_id,
                "repo":"o/r","prNumber":10978,"state":"active"
            }),
            "lifecycle events retain their canonical shape"
        );
        let mut rpc = browser_connect(&fx, "http://localhost", TOKEN)
            .await
            .unwrap();
        let row = list(&fx, &mut rpc).await;
        let snapshot = &row["lastSnapshot"];
        assert_eq!(snapshot["ancestry"], ancestry);
        assert_eq!(snapshot["isBehind"], state == "BEHIND");
        assert_eq!(snapshot["hasConflicts"], state == "DIRTY");
        if queued {
            assert_eq!(snapshot["isInMergeQueue"], true);
        } else {
            assert!(snapshot.get("isInMergeQueue").is_none());
        }
        for projection in [&requirements, snapshot] {
            if state == "DIRTY" {
                assert!(
                    projection.get("branchUpdateRequired").is_none(),
                    "{projection}"
                );
            } else {
                assert_eq!(projection["branchUpdateRequired"], state == "BEHIND");
            }
        }
        let workspace = wss_rpc(
            &mut rpc,
            21,
            "workspace.get",
            json!({"workspaceId":fx.ws_id}),
        )
        .await;
        assert_eq!(
            workspace["workspace"]["displayStatus"] == "pr_ready",
            ready,
            "state={state}, unreadable={unreadable}, queued={queued}: {workspace}"
        );
        assert_eq!(mock.calls("/compare/"), 1, "list/get do not compare again");
        assert_eq!(
            mock.calls(&format!(
                "/repos/o/r/compare/{BASE}...{HEAD}?per_page=1&page=1"
            )),
            1
        );
    }
}

#[intent_test_macros::daemon_test]
async fn ancestry_old_baseline_and_neutral_changes_over_wss() {
    let mock = qwen::MockQwen::start(10978).await;
    configure(&mock);
    let fx = boot_with_source_control(Some(mock.sc.clone())).await;
    for (origin, token, status) in [
        ("https://untrusted.invalid", TOKEN, 403),
        ("http://localhost", "invalid-token", 401),
    ] {
        match browser_connect(&fx, origin, token).await {
            Err(tokio_tungstenite::tungstenite::Error::Http(response)) => {
                assert_eq!(response.status().as_u16(), status);
            }
            other => panic!("expected rejected upgrade: {other:?}"),
        }
    }
    let (monitor, _) = fx
        .services
        .pr_monitor_register(&fx.ws_id, &fx.agent_id, "o", "r", 10978)
        .await
        .unwrap();
    let mut old: Value = serde_json::from_str(monitor.last_snapshot.as_deref().unwrap()).unwrap();
    let requirements = old["requirements"].as_object_mut().unwrap();
    requirements.remove("ancestry");
    requirements.remove("branchUpdateRequired");
    let old = serde_json::to_string(&old).unwrap();
    assert!(fx
        .services
        .store()
        .update_pr_monitor_poll(
            &monitor.monitor_id,
            PrMonitorPollUpdate {
                last_snapshot: Some(&old),
                baseline_snapshot: Some(&old),
                pending_changes: &[],
                pending_since: None,
                last_change_at: None,
                last_polled_at: monitor.last_polled_at.as_deref(),
                last_error: None,
                updated_at: &now_iso(),
                expected_updated_at: &monitor.updated_at,
            }
        )
        .await
        .unwrap());
    let mut rpc = browser_connect(&fx, "http://localhost", TOKEN)
        .await
        .unwrap();
    let row = list(&fx, &mut rpc).await;
    assert_eq!(row["lastSnapshot"]["ancestry"], json!({"status":"unknown"}));
    assert!(row["lastSnapshot"].get("branchUpdateRequired").is_none());
    assert_eq!(row["lastSnapshot"]["isBehind"], false);
    let mut sub = browser_connect(&fx, "http://localhost", TOKEN)
        .await
        .unwrap();
    wss_rpc(
        &mut sub,
        2,
        "events.subscribe",
        json!({
            "workspaceId":fx.ws_id,"eventTypes":["prMonitor:changed","prMonitor:emitted"]
        }),
    )
    .await;
    // An old baseline still reports the newly known forge verdict, while
    // ancestry availability itself adds no change line.
    mock.edit(|s| s.compare = json!({"behind_by":1}));
    fx.services.poll_pr_monitors().await;
    let event = next_event(&mut sub, "prMonitor:changed").await;
    assert_eq!(
        event["data"]["changes"],
        json!(["forge branch-update requirement available: not required"])
    );
    let flushed = wss_rpc(
        &mut rpc,
        3,
        "prMonitor.flush",
        json!({
            "workspaceId":fx.ws_id,"monitorId":monitor.monitor_id
        }),
    )
    .await;
    assert_eq!(flushed, json!({"ok":true,"flushed":true}));
    next_event(&mut sub, "prMonitor:emitted").await;
    let before = owner_messages(&fx).await;

    for count in [0, 1, 2] {
        mock.edit(|s| s.compare = json!({"behind_by":count}));
        fx.services.poll_pr_monitors().await;
        let row = list(&fx, &mut rpc).await;
        assert_eq!(row["lastSnapshot"]["ancestry"], known(count));
        assert_eq!(row["pendingChanges"], json!([]));
        assert_eq!(row["hasPendingChanges"], false);
        let flushed = wss_rpc(
            &mut rpc,
            3,
            "prMonitor.flush",
            json!({
                "workspaceId":fx.ws_id,"monitorId":monitor.monitor_id
            }),
        )
        .await;
        assert_eq!(flushed, json!({"ok":true,"flushed":false}));
        assert_eq!(owner_messages(&fx).await, before);
    }
    for (state, expected) in [
        (
            "BEHIND",
            "forge now requires a branch update before merging",
        ),
        ("DIRTY", "merge conflicts appeared"),
    ] {
        mock.edit(|s| {
            s.pr["mergeStateStatus"] = json!(state);
            s.mergeable = if state == "DIRTY" {
                "CONFLICTING"
            } else {
                "MERGEABLE"
            }
            .into();
            s.compare = json!({"behind_by":3});
        });
        fx.services.poll_pr_monitors().await;
        // A stale ancestry-only event would be returned here and fail this assertion.
        let event = next_event(&mut sub, "prMonitor:changed").await;
        let row = list(&fx, &mut rpc).await;
        assert_eq!(event["data"]["changes"], row["pendingChanges"]);
        assert!(row["pendingChanges"]
            .as_array()
            .unwrap()
            .contains(&json!(expected)));
        assert!(!row["pendingChanges"]
            .to_string()
            .contains("branch ancestry"));
        let flushed = wss_rpc(
            &mut rpc,
            3,
            "prMonitor.flush",
            json!({
                "workspaceId":fx.ws_id,"monitorId":monitor.monitor_id
            }),
        )
        .await;
        assert_eq!(flushed, json!({"ok":true,"flushed":true}));
        let emitted = next_event(&mut sub, "prMonitor:emitted").await;
        assert!(emitted["data"].get("ancestry").is_none());
        assert!(owner_messages(&fx).await.contains(expected));
    }
}
