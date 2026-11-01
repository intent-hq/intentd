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

/// A stand-in for the sitter-shipped tailcat binary in pipe-client mode:
/// for `address` it pipes stdin/stdout to `127.0.0.1:<port>`.
#[cfg(unix)]
fn fake_tailcat(dir: &std::path::Path, address: &str) -> std::path::PathBuf {
    use std::os::unix::fs::PermissionsExt as _;
    let path = dir.join("tailcat");
    std::fs::write(
        &path,
        format!(
            "#!/usr/bin/env bash\n[ \"$1\" = \"{address}\" ] || exit 7\n\
             exec 3<>/dev/tcp/127.0.0.1/$2 || exit 8\ncat <&3 &\ncat >&3\n"
        ),
    )
    .unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    path
}

/// A machine whose listener is loopback-only mints `hosts: []` with its
/// tailcat address: it is added, and later reached, over tailcat alone.
#[cfg(unix)]
#[tokio::test]
async fn a_tailcat_only_machine_with_no_hosts_is_added_and_refreshed() {
    let backend = Backend::new().await;
    let mut machine = backend.machine().await;
    let dir = tempfile::tempdir().unwrap();
    backend
        .services
        .configure_remote_machines(Some(fake_tailcat(dir.path(), "tcmachine1")));
    *machine.host.hosts.lock().unwrap() = Vec::new();
    *machine.host.tc_address.lock().unwrap() = Some("tcmachine1".into());
    let mut params = machine.add_params();
    params["hosts"] = json!([]);
    params["tcAddress"] = json!("tcmachine1");

    let added = backend.call("add", params).await.unwrap();
    machine.accept().await.closed_by_head().await;
    assert_eq!(added["machine"]["hosts"], json!([]));
    assert_eq!(added["machine"]["tcAddress"], "tcmachine1");

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
    let refreshed = refreshing.await.unwrap().unwrap();
    assert_eq!(refreshed["machine"]["state"], "connected");
    let row = backend
        .store()
        .get_exposed_machine(&machine.id())
        .await
        .unwrap()
        .unwrap();
    assert!(row.hosts.is_empty());
    assert_eq!(row.tc_address.as_deref(), Some("tcmachine1"));
    assert!(row.last_connected_at.is_some());
}

/// With neither a host nor a tailcat address there is no route at all.
#[tokio::test]
async fn add_without_any_route_is_rejected_before_dialing() {
    let backend = Backend::new().await;
    let machine = backend.machine().await;
    for tc_address in [None, Some(json!("")), Some(json!(null))] {
        let mut params = machine.add_params();
        params["hosts"] = json!([]);
        if let Some(tc_address) = tc_address {
            params["tcAddress"] = tc_address;
        }
        let error = backend.call("add", params).await.unwrap_err();
        assert!(matches!(error, Error::InvalidParams(_)), "{error:?}");
        assert!(error.to_string().contains("tcAddress"), "{error}");
    }
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

/// No setting gates the backend side: on a fresh config the capability is
/// advertised and the mutations run. The model-facing placement text (the
/// `ws.agent.create` / `delegate` descriptions and `ws.help("agent")`)
/// appears only while a machine is exposed to this backend.
#[tokio::test]
async fn backend_side_needs_no_setting_and_help_follows_exposed_machines() {
    use intent_core::WorkspaceApi as _;
    let backend = Backend::new().await;
    let port = backend.services.feature_remote_launch_port();
    assert!(backend.services.remote_machines_capability());
    assert_eq!(port.placement_help(&backend.services).await, None);
    assert!(port
        .placements(&backend.services)
        .await
        .unwrap()
        .supported_platforms
        .is_empty());

    let mut machine = backend.machine().await;
    backend.expose(&mut machine).await.shutdown();
    let help = port
        .placement_help(&backend.services)
        .await
        .expect("an exposed machine is listed");
    assert!(help.starts_with("Remote placement"), "{help}");
    assert!(help.contains("\"Test machine\""), "{help}");

    backend
        .call("remove", json!({ "machineId": machine.id() }))
        .await
        .expect("remove");
    assert_eq!(port.placement_help(&backend.services).await, None);
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

/// A machine whose link is up but that does not report in time (a freshly
/// started daemon before its first provider report) is `machine-unreachable`
/// naming the live route and the link code, not a bare internal error.
#[tokio::test]
async fn a_refresh_the_machine_does_not_answer_is_machine_unreachable() {
    let mut quick = tuning();
    quick.capabilities_timeout = Duration::from_millis(300);
    let backend = Backend::with_tuning(quick).await;
    let mut machine = backend.machine().await;
    backend.expose(&mut machine).await;
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
    // Bound, but the capabilities request is never answered.
    let _link = machine.accept().await;
    let error = refreshing.await.unwrap().unwrap_err();
    assert_eq!(error.code(), -32603, "{error:?}");
    let data = data(&error);
    assert_eq!(data["code"], "machine-unreachable", "{data}");
    assert_eq!(
        data["attempts"],
        json!([{ "route": format!("127.0.0.1:{}", machine.port), "error": "node-link-timeout" }])
    );
    let message = error.to_string();
    assert!(message.contains("Test machine"), "{message}");
    assert!(!message.contains(&machine.token));
}

/// Revocation stops the binding's agents on the machine, so once the
/// machine refuses the credential the backend no longer counts them: they
/// end with `machine-revoked`, and a plain remove goes through.
#[tokio::test]
async fn a_revoked_machine_no_longer_counts_its_agents() {
    let backend = Backend::new().await;
    let mut machine = backend.machine().await;
    let node = backend.expose(&mut machine).await;
    let agent = AgentId::new();
    let runtime = node.attach(&agent, &workspace());
    let link = machine.accept().await;
    link.replay_complete(0).await;
    assert_eq!(node.running(), 1);

    machine.stop().await;
    machine.revoke().await;
    machine.restart().await;
    tokio::time::timeout(wait(), async {
        while node.running() > 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the revoked machine's agents are no longer counted");
    assert!(!runtime.is_running());
    let exit = runtime.exit_error().unwrap_or_default();
    assert!(exit.contains("machine-revoked"), "{exit}");
    let listed = backend.call("list", json!({})).await.unwrap();
    assert_eq!(listed["machines"][0]["state"], "revoked");
    assert_eq!(listed["machines"][0]["runningAgents"], 0);
    assert_eq!(
        backend
            .call("remove", json!({ "machineId": machine.id() }))
            .await
            .unwrap(),
        json!({ "ok": true, "credentialId": machine.credential.id })
    );
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
    // Review of intentd#2872: handing the stop to the link is not delivery.
    // The removal waits for the machine's acknowledgement of that stop.
    let remove = backend.call(
        "remove",
        json!({ "machineId": machine.id(), "stopAgents": true }),
    );
    tokio::pin!(remove);
    let stop_id = tokio::select! {
        result = &mut remove => panic!("removed before the machine acked the stop: {result:?}"),
        stop_id = async {
            let stop_id = link.stop_for(&agent).await;
            tokio::time::sleep(std::time::Duration::from_millis(300)).await;
            stop_id
        } => stop_id,
    };
    assert_eq!(
        backend.store().list_exposed_machines().await.unwrap().len(),
        1,
        "the machine stays until its stops are confirmed"
    );
    let (removed, ()) = tokio::join!(remove, link.ack_stop(stop_id, &agent, StopOutcome::Stopped));
    assert_eq!(
        removed.unwrap(),
        json!({ "ok": true, "credentialId": machine.credential.id })
    );
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
    // The stopped agent's count change may be announced first.
    let removed_event = json!({ "machineId": machine.id(), "removed": true });
    let mut seen = Vec::new();
    while !seen.contains(&removed_event) {
        seen.extend(changes(&mut sub, 1).await);
    }
    assert_eq!(
        seen.last(),
        Some(&removed_event),
        "removal is announced last"
    );

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

/// Review of intentd#2872: a direct host that accepts TCP but fails the
/// bind (here another daemon with a different certificate) must not hide a
/// valid host listed after it. Which host accepts first is a race, so the
/// connect is repeated.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn a_host_that_fails_the_bind_does_not_hide_a_valid_one() {
    let backend = Backend::new().await;
    let mut machine = backend.machine().await;
    let decoy = Machine::start_on([127, 0, 0, 2], &backend.id().await, machine.port).await;
    assert_eq!(decoy.port, machine.port);
    let mut params = machine.add_params();
    params["hosts"] = json!(["127.0.0.2", "127.0.0.1"]);
    backend
        .call("add", params)
        .await
        .expect("add past the decoy");
    machine.accept().await;

    let target = crate::machine::MachineTarget::from_parts(
        &machine.id(),
        "Test machine",
        vec!["127.0.0.2".into(), "127.0.0.1".into()],
        machine.port,
        None,
        &machine.certificate.fingerprint256,
        &machine.token,
        None,
    )
    .unwrap();
    let owner = crate::lease::new_owner(backend.store(), &machine.id())
        .await
        .unwrap();
    for generation in 10..18 {
        let connected = crate::connect::connect(&target, &tuning().connect, None, || {
            let hello = crate::lease::probe_hello(&owner, NodeCounter(generation));
            async move { Ok(hello) }
        })
        .await
        .unwrap_or_else(|failure| panic!("connect {generation}: {failure:?}"));
        connected.dialed.link.close();
        machine.accept().await;
    }
    drop(decoy);
}

/// Review of intentd#2872: a host that accepts TCP but then stalls in the
/// handshake gets one attempt's budget, not the whole connect's, so the valid
/// host behind it is still tried.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn a_host_that_stalls_the_bind_does_not_hide_a_valid_one() {
    let backend = Backend::new().await;
    let mut machine = backend.machine().await;
    let stalled = tokio::net::TcpListener::bind(("127.0.0.2", machine.port))
        .await
        .expect("bind the stalled host");
    let held = tokio::spawn(async move {
        let mut sockets = Vec::new();
        while let Ok((socket, _)) = stalled.accept().await {
            sockets.push(socket);
        }
    });
    let target = crate::machine::MachineTarget::from_parts(
        &machine.id(),
        "Test machine",
        vec!["127.0.0.2".into(), "127.0.0.1".into()],
        machine.port,
        None,
        &machine.certificate.fingerprint256,
        &machine.token,
        None,
    )
    .unwrap();
    let owner = crate::lease::new_owner(backend.store(), &machine.id())
        .await
        .unwrap();
    // The whole budget stays under the dialer's own 10 s handshake bound,
    // which is the bound a stalled host used to consume.
    let budget = crate::connect::ConnectTuning {
        attempt: std::time::Duration::from_secs(2),
        total: std::time::Duration::from_secs(8),
    };
    for generation in 1..5 {
        let connected = crate::connect::connect(&target, &budget, None, || {
            let hello = crate::lease::probe_hello(&owner, NodeCounter(generation));
            async move { Ok(hello) }
        })
        .await
        .unwrap_or_else(|failure| panic!("connect {generation}: {failure:?}"));
        connected.dialed.link.close();
        machine.accept().await;
    }
    held.abort();
}

/// A removal whose stops the machine does not acknowledge in time fails
/// `machine-unreachable`, naming the unconfirmed agents, and deletes
/// nothing; the stops stay queued.
#[tokio::test]
async fn remove_fails_and_keeps_everything_when_stops_go_unacknowledged() {
    let mut quick = tuning();
    quick.remove_timeout = std::time::Duration::from_millis(800);
    let backend = Backend::with_tuning(quick).await;
    let mut machine = backend.machine().await;
    let node = backend.expose(&mut machine).await;
    let agent = AgentId::new();
    let _runtime = node.attach(&agent, &workspace());
    let mut link = machine.accept().await;
    link.replay_complete(0).await;

    let (error, _) = tokio::join!(
        backend.call(
            "remove",
            json!({ "machineId": machine.id(), "stopAgents": true }),
        ),
        link.stop_for(&agent),
    );
    let error = error.unwrap_err();
    assert_eq!(error.code(), -32603, "{error:?}");
    assert_eq!(data(&error)["code"], "machine-unreachable");
    assert_eq!(data(&error)["unconfirmedStops"], json!([agent.to_string()]));
    assert_eq!(data(&error)["attempts"], json!([]));
    assert_eq!(
        backend.store().list_exposed_machines().await.unwrap().len(),
        1
    );
    assert!(backend
        .services
        .load_remote_machine_token(&machine.id())
        .await
        .unwrap()
        .is_some());
    assert!(
        node.shared_ref().has_pending_stops(),
        "the stop stays queued"
    );
    node.shutdown();
}

/// Review of intentd#2872: a connector is created only under the machine's
/// lifecycle lock, so `refresh` (or a placement) racing a credential
/// replacement cannot cache a connector built on the old lease.
#[tokio::test]
async fn a_connector_is_created_only_under_the_lifecycle_lock() {
    let backend = Backend::new().await;
    let mut machine = backend.machine().await;
    backend.expose(&mut machine).await;
    let feature = backend.services.remote_feature().unwrap();
    feature.take_node(&machine.id()).unwrap().shutdown();

    let held = feature.lifecycle(&machine.id()).await;
    let refresh = backend.call("refresh", json!({ "machineId": machine.id() }));
    tokio::pin!(refresh);
    tokio::select! {
        result = &mut refresh => panic!("refresh ran under a held lifecycle lock: {result:?}"),
        () = tokio::time::sleep(std::time::Duration::from_millis(300)) => {}
    }
    assert!(
        feature.running_node(&machine.id()).is_none(),
        "no connector is created while the lock is held"
    );
    drop(held);
    let (refreshed, ()) = tokio::join!(refresh, async {
        machine
            .accept()
            .await
            .answer_capabilities(capabilities(0))
            .await;
    });
    refreshed.expect("refresh once the lock is free");
    feature.take_node(&machine.id()).unwrap().shutdown();
}

/// Review of intentd#2872: adding the same credential again with a new
/// listener port moves the retained connector to that port too, not only
/// its hosts.
#[tokio::test]
async fn adding_again_on_a_new_port_moves_the_connector() {
    let backend = Backend::new().await;
    let mut machine = backend.machine().await;
    let proxy = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let old_port = proxy.local_addr().unwrap().port();
    let real_port = machine.port;
    let forward = tokio::spawn(async move {
        while let Ok((mut inbound, _)) = proxy.accept().await {
            tokio::spawn(async move {
                let Ok(mut outbound) =
                    tokio::net::TcpStream::connect(("127.0.0.1", real_port)).await
                else {
                    return;
                };
                let _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await;
            });
        }
    });
    let mut params = machine.add_params();
    params["port"] = json!(old_port);
    backend
        .call("add", params)
        .await
        .expect("add on the old port");
    machine.accept().await.closed_by_head().await;
    let node = backend.node(&machine.id()).await;
    node.connect().await.unwrap();
    let first = machine.accept().await;

    backend
        .call("add", machine.add_params())
        .await
        .expect("add again on the new port");
    machine.accept().await;
    forward.abort();
    first.link.close();
    disconnected(&node).await;

    node.connect()
        .await
        .expect("the connector dials the new port");
    machine.accept().await;
    node.shutdown();
}

/// Review of intentd#2872: adding a machine again with the same credential
/// (a route refresh) keeps its lease and offers a newer generation, so the
/// machine does not refuse it as stale.
#[tokio::test]
async fn adding_again_with_the_same_credential_keeps_the_lease() {
    let backend = Backend::new().await;
    let mut machine = backend.machine().await;
    let node = backend.expose(&mut machine).await;
    let lease = node.status().lease_id;
    node.connect().await.unwrap();
    let first = machine.accept().await;

    let again = backend
        .call("add", machine.add_params())
        .await
        .expect("add again");
    assert!(again.get("replacedCredentialId").is_none(), "{again}");
    let probe = machine.accept().await;
    assert_eq!(probe.hello.binding.lease_id, lease, "the same lease");
    assert!(probe.hello.binding.generation > first.hello.binding.generation);
    assert_eq!(backend.node(&machine.id()).await.status().lease_id, lease);
    let leases = backend
        .store()
        .list_owned_node_leases(&backend.id().await)
        .await
        .unwrap();
    assert_eq!(
        leases.iter().filter(|l| !l.state.is_terminal()).count(),
        1,
        "no second live lease"
    );
}

/// Review of intentd#2872: removing a machine whose link is down must not
/// drop the stops it still owes the machine. The removal fails, the machine
/// and its credential stay, and once the machine is back the stop is
/// delivered and the removal goes through.
#[tokio::test]
async fn remove_keeps_a_disconnected_machine_until_its_stops_are_delivered() {
    let backend = Backend::new().await;
    let mut machine = backend.machine().await;
    let node = backend.expose(&mut machine).await;
    let agent = AgentId::new();
    let runtime = node.attach(&agent, &workspace());
    let link = machine.accept().await;
    link.replay_complete(0).await;
    machine.stop().await;
    disconnected(&node).await;

    let error = backend
        .call(
            "remove",
            json!({ "machineId": machine.id(), "stopAgents": true }),
        )
        .await
        .unwrap_err();
    assert_eq!(data(&error)["code"], "machine-unreachable", "{error:?}");
    assert_eq!(data(&error)["unconfirmedStops"], json!([agent.to_string()]));
    assert!(!runtime.is_running(), "the agent is stopped on this side");
    assert_eq!(
        backend.store().list_exposed_machines().await.unwrap().len(),
        1
    );
    assert!(backend
        .services
        .load_remote_machine_token(&machine.id())
        .await
        .unwrap()
        .is_some());

    machine.restart().await;
    let mut link = machine.accept().await;
    let stop_id = link.stop_for(&agent).await;
    // The stop counts only once the machine acks it.
    assert!(node.shared_ref().has_pending_stops());
    link.ack_stop(stop_id, &agent, StopOutcome::Stopped).await;
    tokio::time::timeout(wait(), async {
        while node.shared_ref().has_pending_stops() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the machine's answer confirms the stop");
    let removed = backend
        .call(
            "remove",
            json!({ "machineId": machine.id(), "stopAgents": true }),
        )
        .await;
    assert_eq!(
        removed.unwrap(),
        json!({ "ok": true, "credentialId": machine.credential.id })
    );
    assert!(backend
        .store()
        .list_exposed_machines()
        .await
        .unwrap()
        .is_empty());
}
