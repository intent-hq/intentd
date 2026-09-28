//! Real bridge/queue/TCP tests with injected neutral policy, not native proof.
use super::*;
use crate::mcp_server::private_results::tests::{call, refused, server, Api, Policy, SECRET, WAIT};
use crate::mcp_server::private_results::{DeliveryOutcome, McpPrivateBoundaryKind};
use std::sync::atomic::Ordering;
use tokio::io::AsyncBufReadExt;

#[tokio::test]
async fn tcp_final_admission_retirement_orders_and_completed_effects() {
    for admitted in [false, true] {
        let policy = Policy::new();
        let gate = policy.pause(McpPrivateBoundaryKind::TcpResponse, admitted);
        let api = Arc::new(Api::new());
        let server = Arc::new(server(api.clone(), policy.clone()));
        let bridge = serve_workspace_mcp_tcp(server).await.unwrap();
        let socket = TcpStream::connect(bridge.addr()).await.unwrap();
        let (read, mut write) = socket.into_split();
        let mut lines = BufReader::new(read).lines();
        let message = call(
            7,
            "const result=await ws.git.listRoots(); await ws.workspace.info(); return result;",
        );
        write
            .write_all(format!("{message}\n").as_bytes())
            .await
            .unwrap();
        gate.reached().await;
        policy.retire();
        gate.release.add_permits(1);
        let line = tokio::time::timeout(WAIT, lines.next_line())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let value: Value = serde_json::from_str(&line).unwrap();
        if admitted {
            assert!(line.contains(SECRET));
        } else {
            refused(&value);
        }
        assert_eq!(api.acquired.load(Ordering::SeqCst), 1);
        assert!(api.ordinary.load(Ordering::SeqCst) > 0);
    }
}

#[tokio::test]
async fn full_original_response_queue_refuses_using_its_one_unused_permit() {
    let policy = Policy::new();
    let api = Arc::new(Api::new());
    let server = server(api, policy.clone());
    let response = server
        .handle_message_for_delivery(
            &call(1, "return await ws.git.listRoots();"),
            server.capture_request_context(),
        )
        .await
        .unwrap();
    let (sender, mut receiver) = mpsc::channel(1);
    sender
        .send(PreparedBridgeLine::plain(json!({"sentinel":"ordinary"})))
        .await
        .unwrap();
    let lifetime = ConnectionLifetime::new();
    let connection = lifetime.token();
    let task = tokio::spawn(async move { response.enqueue(sender, &connection).await });
    tokio::task::yield_now().await;
    assert!(!task.is_finished());
    policy.retire();
    assert!(receiver
        .recv()
        .await
        .unwrap()
        .into_line(&lifetime.token())
        .contains("ordinary"));
    tokio::time::timeout(WAIT, task).await.unwrap().unwrap();
    let line = receiver.recv().await.unwrap().into_line(&lifetime.token());
    refused(&serde_json::from_str(&line).unwrap());
    assert!(receiver.recv().await.is_none());
}

#[tokio::test]
async fn closed_original_response_consumer_drops_packet_without_admission_or_retry() {
    let policy = Policy::new();
    let api = Arc::new(Api::new());
    let server = server(api.clone(), policy.clone());
    let response = server
        .handle_message_for_delivery(
            &call(1, "return await ws.git.listRoots();"),
            server.capture_request_context(),
        )
        .await
        .unwrap();
    let (sender, receiver) = mpsc::channel(1);
    drop(receiver);
    let lifetime = ConnectionLifetime::new();
    assert_eq!(
        response.enqueue(sender, &lifetime.token()).await,
        DeliveryOutcome::ConsumerClosed
    );
    assert_eq!(api.acquired.load(Ordering::SeqCst), 1);
    assert_eq!(policy.events.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn full_queue_budget_expiry_is_a_delivery_refusal_without_a_second_slot() {
    let policy = Policy::new();
    let server = server(Arc::new(Api::new()), policy.clone());
    let response = server
        .handle_message_for_delivery(
            &call(1, "return await ws.git.listRoots();"),
            server.capture_request_context(),
        )
        .await
        .unwrap();
    let (sender, mut receiver) = mpsc::channel(1);
    sender
        .send(PreparedBridgeLine::plain(json!({"sentinel":"ordinary"})))
        .await
        .unwrap();
    let lifetime = ConnectionLifetime::new();
    let connection = lifetime.token();
    let task = tokio::spawn(async move { response.enqueue(sender, &connection).await });
    tokio::task::yield_now().await;
    assert!(!task.is_finished());
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(121)).await;
    tokio::task::yield_now().await;
    assert!(
        !task.is_finished(),
        "budget expiry must not silently discard the response reservation"
    );
    tokio::time::resume();
    assert!(receiver
        .recv()
        .await
        .unwrap()
        .into_line(&lifetime.token())
        .contains("ordinary"));
    assert_eq!(task.await.unwrap(), DeliveryOutcome::Refused);
    let control = receiver
        .recv()
        .await
        .expect("the original slot must carry a refusal");
    refused(&serde_json::from_str(&control.into_line(&lifetime.token())).unwrap());
    assert!(receiver.recv().await.is_none());
    assert_eq!(
        policy.events.lock().unwrap().len(),
        1,
        "no output admission without its original slot"
    );
}

#[tokio::test]
async fn cancelled_queue_admission_never_publishes_partial_or_replacement_packet() {
    let policy = Policy::new();
    let gate = policy.pause(McpPrivateBoundaryKind::TcpResponse, false);
    let server = server(Arc::new(Api::new()), policy);
    let response = server
        .handle_message_for_delivery(
            &call(1, "return await ws.git.listRoots();"),
            server.capture_request_context(),
        )
        .await
        .unwrap();
    let (sender, mut receiver) = mpsc::channel(1);
    let lifetime = ConnectionLifetime::new();
    let connection = lifetime.token();
    let task = tokio::spawn(async move { response.enqueue(sender, &connection).await });
    gate.reached().await;
    assert!(receiver.try_recv().is_err());
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    gate.release.add_permits(1);
    assert!(receiver.recv().await.is_none());
}
