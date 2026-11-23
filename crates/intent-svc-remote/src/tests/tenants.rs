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

    let again = a
        .services
        .remote_feature()
        .unwrap()
        .node(&a.services, &machine_a.id())
        .await;
    assert!(again.is_err(), "a closed backend starts no connector again");
    assert!(a
        .services
        .remote_feature()
        .unwrap()
        .running_nodes()
        .is_empty());
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

#[tokio::test]
async fn closing_a_backends_links_ends_its_cloud_supervisor() {
    let backend = Backend::new().await;
    backend.services.resume_exposed_machines().await;
    let feature = backend.services.remote_feature().unwrap();
    tokio::time::timeout(wait(), async {
        while !feature.cloud.supervisor_running() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the supervisor starts");

    feature.close_links();

    tokio::time::timeout(wait(), async {
        while feature.cloud.supervisor_running() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("a closed backend's supervisor ends, so it makes no more E2B calls");
}

#[tokio::test]
async fn closing_a_backends_links_ends_its_background_work_and_starts_no_more() {
    let backend = Backend::new().await;
    let feature = backend.services.remote_feature().unwrap();
    let (started_tx, started) = tokio::sync::oneshot::channel();
    let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let guard = DropFlag(dropped.clone());
    crate::spawn_until_closed(backend.services.clone(), async move {
        let _guard = guard;
        let _ = started_tx.send(());
        // A provider call that would outlive the fence.
        std::future::pending::<()>().await;
    });
    tokio::time::timeout(wait(), started)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(feature.background_tasks(), 1);

    feature.close_links();
    tokio::time::timeout(wait(), feature.background_stopped())
        .await
        .expect("closing the links ends the background work");

    assert!(dropped.load(std::sync::atomic::Ordering::SeqCst));
    assert_eq!(feature.background_tasks(), 0);
    let ran = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let late = ran.clone();
    crate::spawn_until_closed(backend.services.clone(), async move {
        late.store(true, std::sync::atomic::Ordering::SeqCst);
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !ran.load(std::sync::atomic::Ordering::SeqCst),
        "no background work starts once the links are closed"
    );
}

struct DropFlag(Arc<std::sync::atomic::AtomicBool>);

impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.store(true, std::sync::atomic::Ordering::SeqCst);
    }
}

#[tokio::test]
async fn each_backend_runs_its_own_home_move_of_a_shared_workspace_id() {
    let a = Backend::new().await;
    let b = Backend::new().await;
    let workspace = workspace();

    assert!(crate::home_move::claim(&a.services, &workspace));
    assert!(
        crate::home_move::claim(&b.services, &workspace),
        "another tenant's move of the same workspace id is not held up"
    );
    assert!(
        !crate::home_move::claim(&a.services, &workspace),
        "within one tenant, one runner per workspace"
    );
}
