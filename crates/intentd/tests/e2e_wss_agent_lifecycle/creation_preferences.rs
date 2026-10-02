//! Manual specialist preferences over authenticated, pinned WSS, including restart.
use super::*;

async fn rpc_envelope<S>(ws: &mut WebSocketStream<S>, id: i64, method: &str, params: Value) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    ws.send(Message::Text(
        json!({"jsonrpc":"2.0","id":id,"method":method,"params":params})
            .to_string()
            .into(),
    ))
    .await
    .unwrap();
    loop {
        match timeout(Duration::from_secs(15), ws.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
        {
            Message::Text(text) => {
                let value: Value = serde_json::from_str(&text).unwrap();
                if value["id"] == id {
                    assert_eq!(value["jsonrpc"], "2.0");
                    return value;
                }
            }
            Message::Ping(data) => ws.send(Message::Pong(data)).await.unwrap(),
            _ => {}
        }
    }
}

#[intent_test_macros::daemon_test]
async fn creation_preferences_survive_restart_and_rejected_requests_over_wss() {
    let data = temp_data_dir();
    let first = seed_workspace_only(data.path()).await;
    let second = seed_workspace_only(data.path()).await;
    let env = [("INTENTD_AUTH_TOKEN", TOKEN)];
    let mut daemon = Some(Daemon {
        child: spawn_serve(data.path(), "both", &env),
    });
    let socket = data.path().join("intentd.sock");
    assert!(await_uds(&socket).await);
    let status = common::await_wss_status(&socket).await;
    let port = u16::try_from(status["result"]["port"].as_u64().unwrap()).unwrap();
    let mut rpc = connect_ws(
        port,
        client_config(status["result"]["fingerprint"].as_str().unwrap()),
    )
    .await;
    let read = rpc_envelope(
        &mut rpc,
        1,
        "agent.getCreationPreferences",
        json!({"workspaceId":first}),
    )
    .await;
    assert_eq!(read, json!({"jsonrpc":"2.0","id":1,"result":{}}));
    let created = wss_rpc(&mut rpc, 2, "agent.create", json!({"workspaceId":first,"specialistId":"coordinator","rememberSpecialist":true,"provider":"mock","model":"default"})).await;
    let id = created["agent"]["id"].as_str().unwrap();
    assert_eq!(
        wss_rpc(
            &mut rpc,
            3,
            "agent.getCreationPreferences",
            json!({"workspaceId":first})
        )
        .await,
        json!({"specialistId":"spec-writer"})
    );
    assert_eq!(
        wss_rpc(
            &mut rpc,
            4,
            "agent.getCreationPreferences",
            json!({"workspaceId":second})
        )
        .await,
        json!({})
    );
    for params in [
        json!({"workspaceId":first,"specialistId":"nonexistent","rememberSpecialist":true}),
        json!({"workspaceId":first,"rememberSpecialist":"yes"}),
    ] {
        let bad = rpc_envelope(&mut rpc, 5, "agent.create", params).await;
        assert_eq!(bad["error"]["code"], -32602);
    }
    let bad = rpc_envelope(&mut rpc, 6, "agent.update", json!({"agentId":id,"workspaceId":first,"changes":{"specialist":"nonexistent","rememberSpecialist":true}})).await;
    assert_eq!(bad["error"]["code"], -32602);
    assert_eq!(
        wss_rpc(
            &mut rpc,
            7,
            "agent.getCreationPreferences",
            json!({"workspaceId":first})
        )
        .await,
        json!({"specialistId":"spec-writer"})
    );
    let updated = wss_rpc(&mut rpc, 8, "agent.update", json!({"agentId":id,"workspaceId":first,"changes":{"specialist":"implementor","rememberSpecialist":true}})).await;
    assert_eq!(updated["success"], true);
    assert_eq!(updated["agent"]["name"], "Implementor");
    assert_eq!(updated["agent"]["nameExplicitlySet"], false);
    wss_rpc(
        &mut rpc,
        9,
        "agent.create",
        json!({"workspaceId":second,"rememberSpecialist":true,"provider":"mock","model":"default"}),
    )
    .await;
    wss_rpc(&mut rpc, 10, "agent.create", json!({"workspaceId":first,"rememberSpecialist":true,"isBackground":true,"provider":"mock","model":"default"})).await;
    drop(rpc);
    drop(daemon.take());
    daemon = Some(Daemon {
        child: spawn_serve(data.path(), "both", &env),
    });
    assert!(await_uds(&socket).await);
    let status = common::await_wss_status(&socket).await;
    let port = u16::try_from(status["result"]["port"].as_u64().unwrap()).unwrap();
    let mut rpc = connect_ws(
        port,
        client_config(status["result"]["fingerprint"].as_str().unwrap()),
    )
    .await;
    assert_eq!(
        rpc_envelope(
            &mut rpc,
            11,
            "agent.getCreationPreferences",
            json!({"workspaceId":first})
        )
        .await,
        json!({"jsonrpc":"2.0","id":11,"result":{"specialistId":"implementor"}})
    );
    assert_eq!(
        wss_rpc(
            &mut rpc,
            12,
            "agent.getCreationPreferences",
            json!({"workspaceId":second})
        )
        .await,
        json!({"specialistId":null})
    );
    wss_rpc(&mut rpc, 13, "agent.update", json!({"agentId":id,"workspaceId":first,"changes":{"specialist":null,"rememberSpecialist":true}})).await;
    assert_eq!(
        wss_rpc(
            &mut rpc,
            14,
            "agent.getCreationPreferences",
            json!({"workspaceId":first})
        )
        .await,
        json!({"specialistId":null})
    );
    drop(daemon);
}
