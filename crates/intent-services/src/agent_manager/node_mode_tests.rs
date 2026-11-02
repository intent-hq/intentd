//! Node mode closes the agent manager's local front doors with `node-mode`.

use super::*;

#[tokio::test]
async fn node_mode_refuses_the_local_agent_front_doors() {
    let dir = crate::test_support::test_tempdir("node-mode-front-doors");
    let store = intent_store::Store::open(&dir.path().join("state.db"))
        .await
        .unwrap();
    let bus = EventBus::new(store.clone());
    let services = Services::new(store)
        .with_event_bus(bus.clone())
        .with_node_mode(true);
    let manager = Arc::new(AgentManager::new(
        services,
        Arc::new(BusEventSink::new(bus)),
        4,
    ));
    let config = intent_providers::find_provider("claude-code").unwrap();
    let opts = SpawnOptions::new(config);
    let error = manager
        .create_agent(
            AgentId::from("agent-node"),
            WorkspaceId::from("ws-node"),
            "Local agent",
            "implementor",
            dir.path().to_path_buf(),
            &opts,
        )
        .await
        .unwrap_err();
    assert!(matches!(error, Error::NodeMode(_)), "{error:?}");
    assert_eq!(error.code(), -32003);
    let error = manager
        .send_message(
            AgentId::from("agent-node"),
            WorkspaceId::from("ws-node"),
            "hello".into(),
            None,
            TurnOptions::default(),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, Error::NodeMode(_)), "{error:?}");
    assert!(
        manager.handles.lock().unwrap().is_empty(),
        "nothing spawned"
    );
}
