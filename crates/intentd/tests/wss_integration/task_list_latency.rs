//! Manual loopback WSS timings, separate from TLS/upgrade setup. Fixtures match
//! the service baseline's cardinality, ids, timestamps, spec and body sizes.
use super::*;
use std::time::Instant;

#[intent_test_macros::daemon_test]
#[ignore = "manual synthetic task.list timing baseline; run with --ignored --nocapture"]
async fn wss_task_list_latency_baseline() {
    let srv = start(WsOptions::default()).await;
    let ws = WorkspaceId::from("task-list-benchmark");
    srv.store
        .insert_workspace(&fixture_workspace(&ws))
        .await
        .unwrap();
    let mut spec = String::new();
    for i in 0..64 {
        writeln!(spec, "- [Task](intent://local/task/task-{i:03})").unwrap();
    }
    srv.store
        .insert_note(&fixture_note(&ws, "spec", &spec))
        .await
        .unwrap();
    for i in 0..256 {
        let id = if i < 64 {
            format!("task-{i:03}")
        } else {
            format!("plain-{i:03}")
        };
        let mut n = fixture_note(&ws, &id, "");
        n.created_at = format!("2026-01-01T00:00:00.{i:03}Z");
        n.updated_at.clone_from(&n.created_at);
        if i < 64 {
            n.parent_id = Some(NoteId::from("spec"));
            n.metadata.task = Some(TaskMetadata::default());
        }
        srv.store.insert_note(&n).await.unwrap();
    }
    let start = Instant::now();
    let mut socket = connect_ws(srv.port, srv.cfg.clone()).await;
    eprintln!(
        "TASK_LIST_WSS tls_and_upgrade_us={}",
        start.elapsed().as_micros()
    );
    let request = serde_json::json!({"jsonrpc":"2.0", "id":1, "method":"task.list", "params":{"workspaceId":ws}}).to_string();
    let mut expected = None;
    for (label, task_bytes, plain_bytes) in [
        ("empty", 0, 0),
        ("large_tasks", 1024 * 1024, 0),
        ("large_plain", 0, 1024 * 1024),
        ("large_both", 1024 * 1024, 1024 * 1024),
    ] {
        sqlx::query("UPDATE note SET content = CASE WHEN task_json IS NULL THEN ? ELSE ? END WHERE workspace_id = ? AND id != 'spec'")
            .bind("p".repeat(plain_bytes)).bind("t".repeat(task_bytes)).bind(ws.as_str())
            .execute(srv.store.write_pool()).await.unwrap();
        for sample in 0..8 {
            let start = Instant::now();
            socket
                .send(Message::Text(request.clone().into()))
                .await
                .unwrap();
            let text = tokio::time::timeout(Duration::from_secs(30), async {
                loop {
                    match socket.next().await {
                        Some(Ok(Message::Text(text))) => break text,
                        Some(Ok(Message::Ping(payload))) => {
                            socket.send(Message::Pong(payload)).await.unwrap();
                        }
                        other => panic!("expected task.list response, got {other:?}"),
                    }
                }
            })
            .await
            .expect("task.list response deadline");
            let roundtrip_us = start.elapsed().as_micros();
            let response: Value = serde_json::from_str(&text).unwrap();
            assert_eq!(response["jsonrpc"], "2.0");
            assert_eq!(response["id"], 1);
            assert!(response.get("error").is_none(), "{response}");
            assert_eq!(response["result"]["tasks"].as_array().unwrap().len(), 64);
            assert_eq!(response["result"]["stats"]["total"], 64);
            if let Some(ref expected) = expected {
                assert_eq!(&response, expected);
            } else {
                expected = Some(response.clone());
            }
            // Separate direct invocation, not subtraction from the same request:
            // WSS and direct handler have different scheduling/cache conditions.
            let start = Instant::now();
            let direct = srv.api.task_list(ws.clone(), None).await.unwrap();
            let direct_handler_us = start.elapsed().as_micros();
            assert_eq!(serde_json::to_value(direct).unwrap(), response["result"]);
            eprintln!("TASK_LIST_WSS fixture={label} sample={sample} tasks=64 plain=192 task_body_bytes={task_bytes} plain_body_bytes={plain_bytes} request_bytes={} response_bytes={} roundtrip_us={roundtrip_us} direct_handler_us={direct_handler_us}", request.len(), text.len());
        }
    }
    socket.close(None).await.unwrap();
    srv.ws.stop().await;
}
