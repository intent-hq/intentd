//! `remoteMachines.*` on the backend: probe-before-save, the route race,
//! routes refreshed from the welcome, the token kept in the secret store,
//! refresh, revocation, removal and the `remoteMachine:changed` event.

use intent_core::Error;
use intent_services::feature_api::SubscriptionFilter;

use super::*;

fn data(error: &Error) -> Value {
    match error {
        Error::RemoteMachine(failure) => failure.data.clone(),
        other => panic!("expected a remote machine error, got {other:?}"),
    }
}

/// The next `remoteMachine:changed` payloads, up to `count`.
async fn changes(sub: &mut intent_services::feature_api::Subscription, count: usize) -> Vec<Value> {
    let mut out = Vec::new();
    tokio::time::timeout(wait(), async {
        while out.len() < count {
            for event in sub.recv().await.expect("subscription open") {
                out.push(event.data);
            }
        }
    })
    .await
    .expect("remoteMachine:changed arrives");
    out
}

fn subscribe(backend: &Backend) -> intent_services::feature_api::Subscription {
    backend.bus.subscribe(SubscriptionFilter {
        event_types: vec![intent_core::events::REMOTE_MACHINE_CHANGED.to_owned()],
        ..SubscriptionFilter::default()
    })
}

#[tokio::test]
async fn backend_info_is_this_daemons_id_and_name() {
    let backend = Backend::new().await;
    let info = backend.call("backendInfo", json!({})).await.unwrap();
    assert_eq!(info["backendId"], json!(backend.id().await));
    assert!(!info["backendName"].as_str().unwrap().is_empty());
    assert!(info.get("fingerprint").is_none(), "no listener here");
}

#[tokio::test]
async fn add_probes_then_saves_the_machine_and_keeps_the_token_secret() {
    let backend = Backend::new().await;
    let mut machine = backend.machine().await;
    *machine.host.hosts.lock().unwrap() = vec!["127.0.0.1".into(), "192.0.2.10".into()];
    *machine.host.tc_address.lock().unwrap() = Some("tc-machine".into());
    let mut sub = subscribe(&backend);

    let added = backend.call("add", machine.add_params()).await.unwrap();
    let probe = machine.accept().await;
    assert_eq!(probe.hello.binding.head_id, backend.id().await);
    assert_eq!(probe.hello.binding.node_id, machine.id());
    assert_eq!(probe.hello.acked_seq, NodeCounter(0));

    let machine_json = &added["machine"];
    assert_eq!(machine_json["id"], json!(machine.id()));
    assert_eq!(machine_json["name"], "Test machine");
    assert_eq!(
        machine_json["platform"],
        json!({"os": "macos", "arch": "aarch64"})
    );
    // The routes are what the machine reported in its welcome.
    assert_eq!(machine_json["hosts"], json!(["127.0.0.1", "192.0.2.10"]));
    assert_eq!(machine_json["tcAddress"], "tc-machine");
    assert_eq!(machine_json["port"], json!(machine.port));
    assert_eq!(machine_json["state"], "disconnected");
    assert_eq!(machine_json["runningAgents"], 0);
    assert_eq!(machine_json["capabilities"]["capacity"]["agentCap"], 2);
    assert!(machine_json["addedAt"].is_string());
    assert!(machine_json["lastConnectedAt"].is_string());
    assert!(added.get("replacedCredentialId").is_none());

    let listed = backend.call("list", json!({})).await.unwrap();
    assert_eq!(listed["machines"].as_array().unwrap().len(), 1);
    for text in [added.to_string(), listed.to_string()] {
        assert!(!text.contains(&machine.token), "never the token");
    }
    assert_eq!(
        backend
            .services
            .load_remote_machine_token(&machine.id())
            .await
            .unwrap(),
        Some(machine.token.clone())
    );
    let row = backend
        .store()
        .get_exposed_machine(&machine.id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.credential_id, machine.credential.id);

    let event = changes(&mut sub, 1).await.remove(0);
    assert_eq!(event["machine"]["id"], json!(machine.id()));
    assert!(!event.to_string().contains(&machine.token));
}

#[tokio::test]
async fn the_direct_routes_race_and_the_first_reachable_one_binds() {
    let backend = Backend::new().await;
    let mut machine = backend.machine().await;
    let mut params = machine.add_params();
    // An unroutable documentation address first: the race takes the host
    // that answers.
    params["hosts"] = json!(["192.0.2.1", "127.0.0.1"]);
    backend.call("add", params).await.unwrap();
    machine.accept().await;
}

#[tokio::test]
async fn an_unreachable_machine_saves_nothing_and_names_the_routes_tried() {
    let backend = Backend::new().await;
    let machine = backend.machine().await;
    machine.stop().await;
    let mut sub = subscribe(&backend);
    let error = backend.call("add", machine.add_params()).await.unwrap_err();
    assert_eq!(error.code(), -32603);
    let data = data(&error);
    assert_eq!(data["code"], "machine-unreachable");
    assert_eq!(
        data["attempts"],
        json!([{ "route": format!("127.0.0.1:{}", machine.port), "error": "node-link-unavailable" }])
    );
    assert!(!error.to_string().contains(&machine.token));
    assert!(backend
        .store()
        .list_exposed_machines()
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        backend
            .services
            .load_remote_machine_token(&machine.id())
            .await
            .unwrap(),
        None
    );
    assert!(
        tokio::time::timeout(
            intentd_test_support::test_timeout(Duration::from_millis(200)),
            sub.recv(),
        )
        .await
        .is_err(),
        "nothing is announced"
    );
}

#[tokio::test]
async fn a_wrong_token_or_pin_is_unreachable_with_the_link_code() {
    let backend = Backend::new().await;
    let machine = backend.machine().await;

    let mut wrong_token = machine.add_params();
    wrong_token["token"] = json!("f".repeat(64));
    let error = backend.call("add", wrong_token).await.unwrap_err();
    let refused = data(&error);
    assert_eq!(refused["code"], "machine-unreachable");
    assert_eq!(refused["attempts"][0]["error"], "node-link-unauthorized");

    let other = generate_certificate(vec!["127.0.0.1".into()]).unwrap();
    let mut wrong_pin = machine.add_params();
    wrong_pin["fingerprint"] = json!(other.fingerprint256);
    let error = backend.call("add", wrong_pin).await.unwrap_err();
    assert_eq!(
        data(&error)["attempts"][0]["error"],
        "node-link-certificate-mismatch"
    );
    assert!(backend
        .store()
        .list_exposed_machines()
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn add_rejects_malformed_params_before_dialing() {
    let backend = Backend::new().await;
    let machine = backend.machine().await;
    for (field, value) in [
        ("hosts", json!([])),
        ("hosts", json!("127.0.0.1")),
        ("port", json!(0)),
        ("machineId", json!("bad id")),
        ("fingerprint", json!("nope")),
        ("token", json!("short")),
    ] {
        let mut params = machine.add_params();
        params[field] = value;
        let error = backend.call("add", params).await.unwrap_err();
        assert!(
            matches!(error, Error::InvalidParams(_)),
            "{field}: {error:?}"
        );
        assert!(!error.to_string().contains(&machine.token));
    }
}

#[tokio::test]
async fn mutations_need_the_experimental_setting() {
    let backend = Backend::new().await;
    let machine = backend.machine().await;
    backend
        .services
        .settings_registry()
        .unwrap()
        .apply(&[("experimental.remoteAgents".to_owned(), json!(false))])
        .unwrap();
    for (method, params) in [
        ("add", machine.add_params()),
        ("remove", json!({ "machineId": machine.id() })),
        ("refresh", json!({ "machineId": machine.id() })),
    ] {
        let error = backend.call(method, params).await.unwrap_err();
        assert!(
            matches!(
                error,
                Error::RemoteAgentsDisabled {
                    setting: "experimental.remoteAgents"
                }
            ),
            "{method}: {error:?}"
        );
    }
    assert_eq!(
        backend.call("list", json!({})).await.unwrap(),
        json!({ "machines": [] })
    );
}

#[tokio::test]
async fn refresh_connects_on_demand_and_records_the_report() {
    let backend = Backend::new().await;
    let mut machine = backend.machine().await;
    backend.expose(&mut machine).await;
    machine.no_link(Duration::from_millis(200)).await;
    let mut sub = subscribe(&backend);

    let refreshing = {
        let services = backend.services.clone();
        let id = machine.id();
        tokio::spawn(async move {
            with_caller(
                Caller::Daemon,
                services.remote_machines("refresh", json!({ "machineId": id })),
            )
            .await
        })
    };
    let mut link = machine.accept().await;
    link.answer_capabilities(capabilities(1)).await;
    let refreshed = refreshing.await.unwrap().unwrap();
    assert_eq!(refreshed["machine"]["state"], "connected");
    assert_eq!(
        refreshed["machine"]["capabilities"]["capacity"]["runningAgents"],
        1
    );
    let row = backend
        .store()
        .get_exposed_machine(&machine.id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.capabilities.unwrap().capacity.running_agents, 1);
    let events = changes(&mut sub, 2).await;
    assert!(
        events
            .iter()
            .any(|event| event["machine"]["state"] == "connected"),
        "{events:?}"
    );

    let unknown = backend
        .call("refresh", json!({ "machineId": "nope" }))
        .await
        .unwrap_err();
    assert_eq!(data(&unknown)["code"], "not-found");
    assert_eq!(unknown.code(), -32602);
}

#[tokio::test]
async fn a_revoked_credential_marks_the_machine_revoked() {
    let backend = Backend::new().await;
    let mut machine = backend.machine().await;
    backend.expose(&mut machine).await;
    machine.revoke().await;
    let error = backend
        .call("refresh", json!({ "machineId": machine.id() }))
        .await
        .unwrap_err();
    assert_eq!(error.code(), -32603);
    assert_eq!(data(&error)["code"], "machine-revoked");
    let listed = backend.call("list", json!({})).await.unwrap();
    assert_eq!(listed["machines"][0]["state"], "revoked");
}

#[tokio::test]
async fn an_unreachable_refresh_marks_the_machine_unreachable() {
    let backend = Backend::new().await;
    let mut machine = backend.machine().await;
    backend.expose(&mut machine).await;
    machine.stop().await;
    let error = backend
        .call("refresh", json!({ "machineId": machine.id() }))
        .await
        .unwrap_err();
    assert_eq!(data(&error)["code"], "machine-unreachable");
    let listed = backend.call("list", json!({})).await.unwrap();
    assert_eq!(listed["machines"][0]["state"], "unreachable");

    // Back again: the next connect restores the healthy state.
    machine.restart().await;
    let refreshing = {
        let services = backend.services.clone();
        let id = machine.id();
        tokio::spawn(async move {
            with_caller(
                Caller::Daemon,
                services.remote_machines("refresh", json!({ "machineId": id })),
            )
            .await
        })
    };
    machine
        .accept()
        .await
        .answer_capabilities(capabilities(0))
        .await;
    refreshing.await.unwrap().unwrap();
    let row = backend
        .store()
        .get_exposed_machine(&machine.id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        row.state,
        intent_core::machines::RemoteMachineState::Disconnected
    );
}

#[tokio::test]
async fn remove_refuses_while_agents_run_unless_told_to_stop_them() {
    let backend = Backend::new().await;
    let mut machine = backend.machine().await;
    let node = backend.expose(&mut machine).await;
    let agent = AgentId::new();
    let runtime = node.attach(&agent, &workspace());
    let mut link = machine.accept().await;
    link.replay_complete(0).await;

    let error = backend
        .call("remove", json!({ "machineId": machine.id() }))
        .await
        .unwrap_err();
    assert_eq!(error.code(), -32602);
    assert_eq!(data(&error)["code"], "machine-in-use");
    assert_eq!(data(&error)["runningAgents"], 1);

    let mut sub = subscribe(&backend);
    let removed = backend
        .call(
            "remove",
            json!({ "machineId": machine.id(), "stopAgents": true }),
        )
        .await
        .unwrap();
    assert_eq!(
        removed,
        json!({ "ok": true, "credentialId": machine.credential.id })
    );
    link.expect(|frame| match frame {
        LinkFrame::Stop { agent_id } if agent_id == agent => Some(()),
        _ => None,
    })
    .await;
    assert!(!runtime.is_running());
    assert!(backend
        .store()
        .list_exposed_machines()
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        backend
            .services
            .load_remote_machine_token(&machine.id())
            .await
            .unwrap(),
        None
    );
    let leases = backend
        .store()
        .list_owned_node_leases(&backend.id().await)
        .await
        .unwrap();
    assert!(leases.iter().all(|lease| lease.state.is_terminal()));
    let event = changes(&mut sub, 1).await.remove(0);
    assert_eq!(event, json!({ "machineId": machine.id(), "removed": true }));

    // Unknown: a no-op.
    assert_eq!(
        backend
            .call("remove", json!({ "machineId": machine.id() }))
            .await
            .unwrap(),
        json!({ "ok": true })
    );
}

#[tokio::test]
async fn adding_again_with_a_new_credential_starts_a_fresh_lease() {
    let backend = Backend::new().await;
    let mut machine = backend.machine().await;
    let node = backend.expose(&mut machine).await;
    let old_lease = node.status().lease_id;
    let old_credential = machine.credential.id.clone();

    let token = "a".repeat(64);
    let credential = Machine::mint(machine.services.store(), &backend.id().await, &token).await;
    let mut params = machine.add_params();
    params["credentialId"] = json!(credential.id);
    params["token"] = json!(token);
    let added = backend.call("add", params).await.unwrap();
    machine.accept().await;
    assert_eq!(added["replacedCredentialId"], json!(old_credential));

    let fresh = backend.node(&machine.id()).await;
    assert_ne!(fresh.status().lease_id, old_lease);
    fresh.connect().await.unwrap();
    let link = machine.accept().await;
    assert_eq!(link.hello.binding.lease_id, fresh.status().lease_id);
    assert_eq!(link.hello.acked_seq, NodeCounter(0), "a fresh journal");
    fresh.shutdown();
}
