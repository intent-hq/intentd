//! Two backends in one process (one orchestrator serving two tenants): each
//! keeps its own link session, machine-list lock and links.

use super::*;

/// The `headSession` of the next link `node` dials to `machine`.
async fn dialed_session(node: &RemoteNode, machine: &mut Machine) -> String {
    let dial = {
        let node = node.clone();
        tokio::spawn(async move { node.connect().await })
    };
    let end = machine.accept().await;
    dial.await.unwrap().expect("the connector binds");
    end.hello.head_session.expect("a hello names its session")
}

#[tokio::test]
async fn each_backend_offers_its_own_head_session_on_every_link() {
    let a = Backend::new().await;
    let b = Backend::new().await;
    let mut machine_a = a.machine().await;
    let mut machine_b = b.machine().await;

    a.call("add", machine_a.add_params()).await.expect("add");
    let probe_a = machine_a.accept().await;
    b.call("add", machine_b.add_params()).await.expect("add");
    let probe_b = machine_b.accept().await;

    let node_a = a.node(&machine_a.id()).await;
    let node_b = b.node(&machine_b.id()).await;
    let session_a = dialed_session(&node_a, &mut machine_a).await;
    let session_b = dialed_session(&node_b, &mut machine_b).await;

    assert_ne!(session_a, session_b, "two tenants never share a session");
    assert!(uuid::Uuid::parse_str(&session_a).is_ok());
    assert_eq!(
        probe_a.hello.head_session.as_deref(),
        Some(session_a.as_str()),
        "the add probe offers the backend's session too"
    );
    assert_eq!(
        probe_b.hello.head_session.as_deref(),
        Some(session_b.as_str())
    );
    let feature_a = a.services.remote_feature().unwrap();
    assert_eq!(feature_a.head_session(), session_a);
}

#[tokio::test]
async fn closing_one_backends_links_leaves_the_others_up() {
    let a = Backend::new().await;
    let b = Backend::new().await;
    let mut machine_a = a.machine().await;
    let mut machine_b = b.machine().await;
    a.call("add", machine_a.add_params()).await.expect("add");
    machine_a.accept().await.closed_by_head().await;
    b.call("add", machine_b.add_params()).await.expect("add");
    machine_b.accept().await.closed_by_head().await;
    let node_a = a.node(&machine_a.id()).await;
    let node_b = b.node(&machine_b.id()).await;
    dialed_session(&node_a, &mut machine_a).await;
    dialed_session(&node_b, &mut machine_b).await;
    bound(&node_a).await;
    bound(&node_b).await;

    let closed = a.services.remote_feature().unwrap().close_links();

    assert_eq!(closed, 1);
    disconnected(&node_a).await;
    assert!(a
        .services
        .remote_feature()
        .unwrap()
        .running_nodes()
        .is_empty());
    let feature_b = b.services.remote_feature().unwrap();
    assert_eq!(feature_b.running_nodes().len(), 1);
    assert!(
        node_b.status().connected,
        "the other tenant's link stays up"
    );
    assert_eq!(feature_b.connected_machines(), 1);
}

#[tokio::test]
async fn one_backends_machine_list_change_does_not_wait_for_anothers() {
    let a = Backend::new().await;
    let b = Backend::new().await;
    let held = crate::order::lock(&a.services).await;

    let order = b.call("order", json!({})).await.unwrap()["order"].clone();
    let set = tokio::time::timeout(wait(), b.call("setOrder", json!({ "order": order })))
        .await
        .expect("a held machine-list lock of one tenant does not stall another");
    set.expect("setOrder");
    drop(held);
}
