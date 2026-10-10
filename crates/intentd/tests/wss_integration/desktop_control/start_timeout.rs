//! A lost native readiness reply cannot grant control, even if it arrives later.
use super::*;

#[tokio::test]
async fn desktop_wss_lost_start_reply_times_out_and_late_ready_never_grants() {
    let (srv, services) = super::super::authenticated_devices::start_roster().await;
    let ws = WorkspaceId::new();
    srv.store
        .insert_workspace(&fixture_workspace(&ws))
        .await
        .unwrap();
    srv.registry
        .apply(&[
            ("model.defaultProvider".into(), json!("auggie")),
            ("providers.paths".into(), json!({"auggie":"/bin/sh"})),
        ])
        .unwrap();
    let owner = Caller::Wire {
        principal_id: srv.store.get_primary_principal().await.unwrap().id,
        host_role: HostRole::Owner,
    };
    let created = intent_core::with_caller(
        owner.clone(),
        services.agent_create(
            ws.clone(),
            Some("Lost readiness reply".into()),
            None,
            None,
            None,
            None,
            intent_core::AgentCreateExtra::default(),
        ),
    )
    .await
    .unwrap();
    let agent = AgentId::from(created["agent"]["id"].as_str().unwrap());
    let caller = Caller::Agent {
        agent_id: agent.clone(),
    };
    let url = format!("wss://localhost:{}/ws?token={TOKEN}", srv.port);
    let mut socket = common::wss_connect_with_retry(srv.port, srv.cfg.clone(), &url).await;
    let mut calls = Vec::new();
    assert!(call(
        &mut socket,
        "client.hello",
        json!({"clientId":"lost-start","capabilities":{"browserExec":true,"desktopControl":1}}),
        &mut calls
    )
    .await
    .get("error")
    .is_none());
    assert!(call(
        &mut socket,
        "events.subscribe",
        json!({"workspaceId":ws,"eventTypes":["desktop:*"]}),
        &mut calls
    )
    .await
    .get("error")
    .is_none());
    let pending = drive(
        &mut socket,
        intent_core::with_caller(
            caller.clone(),
            services.desktop_agent_call(ws.clone(), "startControl".into(), json!({})),
        ),
        &mut calls,
    )
    .await
    .unwrap();
    socket.send(Message::Text(json!({"jsonrpc":"2.0","id":8101,"method":"desktop.respondPermission","params":{"workspaceId":ws,"requestId":pending["requestId"],"decision":"allow_once"}}).to_string().into())).await.unwrap();
    let mut withheld = None;
    let failure = tokio::time::timeout(Duration::from_secs(25), async {
        loop {
            let frame = socket.next().await.unwrap().unwrap();
            if let Message::Text(text) = frame {
                let v: Value = serde_json::from_str(&text).unwrap();
                if v["method"] == "desktop.control" {
                    match v["params"]["operation"].as_str().unwrap() {
                        "startControl" => {
                            assert!(withheld.is_none());
                            withheld = Some(v);
                        }
                        "endControl" => executor_reply(&mut socket, v, &mut calls).await,
                        other => panic!(
                            "no native input or renewal permitted while readiness missing: {other}"
                        ),
                    }
                } else if v["method"] == "events.event" {
                    let event = &v["params"]["event"];
                    assert_ne!(event["data"]["status"], "active");
                    assert_ne!(event["data"]["outcome"], "granted");
                    if event["type"] == "desktop:permission-resolved" {
                        break event["data"].clone();
                    }
                } else if v["id"] == 8101 {
                    assert_eq!(v["result"]["accepted"], true);
                }
            }
        }
    })
    .await
    .expect("bounded native start timeout and cleanup");
    assert_eq!(failure["requestId"], pending["requestId"]);
    assert_eq!(failure["outcome"], "failed");
    assert_eq!(failure["error"]["execution"], "not_started");
    assert!(failure["error"]["detail"]
        .as_str()
        .unwrap()
        .contains("timed out"));
    let start = withheld.unwrap();
    assert!(calls
        .iter()
        .any(|p| p["operation"] == "endControl" && p["sessionId"] == start["params"]["sessionId"]));
    socket.send(Message::Text(json!({"jsonrpc":"2.0","id":start["id"],"result":{"ready":true,"sessionId":start["params"]["sessionId"],"computerId":"wss-physical"}}).to_string().into())).await.unwrap();
    // The next same-socket RPC is a FIFO barrier after the stale response.
    let state = call(
        &mut socket,
        "desktop.getState",
        json!({"workspaceId":ws,"agentId":agent}),
        &mut calls,
    )
    .await;
    assert_eq!(state["result"]["state"]["status"], "inactive");
    let action = intent_core::with_caller(
        caller,
        services.desktop_agent_call(ws.clone(), "listDisplay".into(), json!({})),
    )
    .await;
    assert!(action.is_err(), "late ready must never authorize actions");
    let records:Vec<String>=sqlx::query_scalar("SELECT json_extract(value,'$.payload') FROM settings WHERE key GLOB 'desktop.v1/outbox/*' AND json_extract(value,'$.payload.requestId')=?").bind(pending["requestId"].as_str().unwrap()).fetch_all(srv.store.read_pool()).await.unwrap();
    assert_eq!(records.len(), 1);
    let payload: Value = serde_json::from_str(&records[0]).unwrap();
    assert_eq!(payload["outcome"], "failed");
    assert!(payload["message"].as_str().unwrap().contains("timed out"));
    assert!(!calls
        .iter()
        .any(|p| p["operation"] == "execute" || p["operation"] == "renew"));
}
