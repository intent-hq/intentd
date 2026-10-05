use super::*;
use crate::desktop::tests::Harness;
use intent_core::desktop::DesktopState;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

#[tokio::test]
async fn desktop_lifecycle_normal_turn_and_mcp_transport_recreation_keep_control() {
    let h = Harness::new().await;
    h.remember().await;
    let active = h.agent("startControl", json!({})).await.unwrap();
    let manager = AgentManager::new(
        h.services.clone(),
        Arc::new(BusEventSink::new(h.services.event_bus.clone().unwrap())),
        4,
    );
    assert!(manager.try_begin(&h.agent, &h.workspace).await);
    manager.end_turn(&h.agent).await;
    assert_eq!(
        h.services
            .store
            .get_agent_session_summary(&h.agent)
            .await
            .unwrap()
            .status,
        AgentStatus::RuntimeIdle
    );
    for _ in 0..2 {
        let server = Arc::new(
            WorkspaceMcpServer::new(Arc::new(h.services.clone()), h.workspace.clone())
                .with_caller_agent_id(Some(h.agent.clone())),
        );
        let bridge = intent_acp::serve_workspace_mcp_tcp(server).await.unwrap();
        let stream = tokio::net::TcpStream::connect(bridge.addr()).await.unwrap();
        let (read, mut write) = stream.into_split();
        let request = json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"workspace_api","arguments":{"code":"return await ws.desktop.startControl();","summary":"Verify recovered desktop session"}}});
        write
            .write_all(format!("{request}\n").as_bytes())
            .await
            .unwrap();
        let mut reader = BufReader::new(read);
        let mut line = String::new();
        tokio::time::timeout(Duration::from_secs(10), reader.read_line(&mut line))
            .await
            .unwrap()
            .unwrap();
        let result: Value = serde_json::from_str(&line).unwrap();
        assert_ne!(result["result"]["isError"], true, "{result}");
        assert!(line.contains("alreadyGranted"), "{line}");
        assert!(
            line.contains(active["sessionId"].as_str().unwrap()),
            "{line}"
        );
        drop(write);
        drop(reader);
        drop(bridge);
        manager.kill_child_only(&h.agent).await;
        assert!(
            matches!(intent_core::with_caller(Caller::Daemon, h.services.desktop_current_state(&h.agent)).await,DesktopState::Active{session_id,..} if session_id==active["sessionId"].as_str().unwrap())
        );
    }
    h.services.desktop_terminate_agent(&h.agent).await;
    assert_eq!(
        intent_core::with_caller(Caller::Daemon, h.services.desktop_current_state(&h.agent)).await,
        DesktopState::Inactive
    );
    assert_eq!(
        h.agent("listDisplay", json!({})).await.unwrap_err().code,
        "desktop-not-active"
    );
}
