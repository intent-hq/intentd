//! `remoteMachines.*` on the backend (monorepo
//! `docs/protocol/methods/nodes.md`, "Backend methods: exposed machines").
//! The services have already checked the caller is the owner.
//!
//! `add` probes before saving: it races the machine's routes, checks the
//! pin and the token, binds and reads the capabilities from the welcome.
//! Only then are the machine row, its token (in the secret store, never the
//! row) and a fresh lease saved. Nothing ever returns the token.

use serde_json::{json, Map, Value};

use intent_core::machine_order::MachineOrderKey;
use intent_core::machines::{ExposedMachine, RemoteMachineState};
use intent_core::nodes::NodeCounter;
use intent_core::{now_iso, AgentId, Error, ExecutorCredentialId, Result};
use intent_services::feature_api::Services;

use crate::connect::{connect, ConnectFailure, Connected};
use crate::connector::Shared;
use crate::lease::{new_owner, probe_hello, record_new_lease, HeadLease};
use crate::machine::MachineTarget;
use crate::{RemoteFeature, RemoteNode};

/// Longest id, name, credential id or route, in UTF-8 bytes.
const MAX_TEXT_BYTES: usize = 256;
/// Most hosts a machine may list.
const MAX_HOSTS: usize = 32;

/// `RemoteMachine` on the wire. `live` is the machine's connector, when one
/// runs: it overlays `connected` and counts this backend's live agents.
pub(crate) fn machine_json(row: &ExposedMachine, live: Option<&Shared>) -> Value {
    let connected = live.is_some_and(Shared::is_connected);
    let state = if connected {
        RemoteMachineState::Connected
    } else {
        row.state
    };
    let mut machine = Map::new();
    machine.insert("id".into(), json!(row.id));
    machine.insert("name".into(), json!(row.name));
    if let Some(platform) = &row.platform {
        machine.insert("platform".into(), json!(platform));
    }
    machine.insert("hosts".into(), json!(row.hosts));
    machine.insert("port".into(), json!(row.port));
    if let Some(tc_address) = &row.tc_address {
        machine.insert("tcAddress".into(), json!(tc_address));
    }
    machine.insert("fingerprint".into(), json!(row.fingerprint));
    machine.insert("state".into(), json!(state));
    machine.insert(
        "runningAgents".into(),
        json!(live.map_or(0, |shared| shared.agents.live_count())),
    );
    if let Some(capabilities) = &row.capabilities {
        machine.insert("capabilities".into(), json!(capabilities));
    }
    if let Some(observed_at) = &row.observed_at {
        machine.insert("observedAt".into(), json!(observed_at));
    }
    machine.insert("addedAt".into(), json!(row.added_at));
    if let Some(last) = &row.last_connected_at {
        machine.insert("lastConnectedAt".into(), json!(last));
    }
    // Served with `remoteScripts: 1`: a machine below 8 needs an update
    // before its agents can use saved scripts.
    if let Some(protocol) = row.node_protocol {
        machine.insert("nodeProtocol".into(), json!(protocol));
    }
    if let Some(cloud) = &row.cloud {
        machine.insert(
            "cloud".into(),
            cloud.to_wire(time::OffsetDateTime::now_utc()),
        );
    }
    Value::Object(machine)
}

/// Publish a machine row change: `cloudMachine:changed` for a cloud machine
/// (never `remoteMachine:changed`), else `remoteMachine:changed`.
pub(crate) async fn publish_machine(svc: &Services, row: &ExposedMachine, live: Option<&Shared>) {
    let machine = machine_json(row, live);
    if row.is_cloud() {
        svc.publish_cloud_machine_changed(
            json!({ "machine": machine }),
            row.cloud.as_ref().map(|cloud| cloud.workspace_id.as_str()),
        )
        .await;
    } else {
        svc.publish_remote_machine_changed(json!({ "machine": machine }))
            .await;
    }
}

fn coded(internal: bool, code: &str, message: impl Into<String>, extra: Value) -> Error {
    let mut data = json!({ "code": code });
    if let (Some(data), Value::Object(extra)) = (data.as_object_mut(), extra) {
        data.extend(extra);
    }
    Error::remote_machine(internal, message, data)
}

fn text<'a>(params: &'a Value, name: &str, required: bool) -> Result<Option<&'a str>> {
    match params.get(name) {
        None | Some(Value::Null) if !required => Ok(None),
        None | Some(Value::Null) => Err(Error::InvalidParams(format!("{name} is required"))),
        Some(Value::String(value)) if !value.trim().is_empty() && value.len() <= MAX_TEXT_BYTES => {
            Ok(Some(value.as_str()))
        }
        Some(_) => Err(Error::InvalidParams(format!(
            "{name} must be a non-blank string of at most {MAX_TEXT_BYTES} bytes"
        ))),
    }
}

fn required<'a>(params: &'a Value, name: &str) -> Result<&'a str> {
    text(params, name, true).map(Option::unwrap_or_default)
}

fn not_found(machine_id: &str) -> Error {
    coded(
        false,
        "not-found",
        format!("No exposed machine has id {machine_id}."),
        json!({}),
    )
}

/// Run `remoteMachines.<method>`.
pub(crate) async fn call(
    feature: &RemoteFeature,
    svc: &Services,
    method: &str,
    params: Value,
) -> Result<Value> {
    match method {
        "backendInfo" => backend_info(svc).await,
        "list" => list(feature, svc).await,
        "models" => models(feature, svc, &params).await,
        "order" => crate::order::order(svc).await,
        "setOrder" => crate::order::set_order(svc, &params).await,
        "add" => add(feature, svc, &params).await,
        "claim" => crate::claim::claim(feature, svc, &params).await,
        "remove" => remove(feature, svc, &params).await,
        "refresh" => refresh(feature, svc, &params).await,
        other => Err(Error::InvalidParams(format!(
            "unknown method remoteMachines.{other}"
        ))),
    }
}

/// This backend's display name, as `remoteMachines.backendInfo` reports it.
pub(crate) async fn backend_name(svc: &Services) -> String {
    svc.own_machine_routes()
        .await
        .map(|routes| routes.name.trim().to_owned())
        .filter(|name| !name.is_empty())
        .unwrap_or_else(host_name)
}

async fn backend_info(svc: &Services) -> Result<Value> {
    let backend_id = svc.store().daemon_id().await?;
    let routes = svc.own_machine_routes().await;
    let name = backend_name(svc).await;
    let mut info = json!({ "backendId": backend_id, "backendName": name });
    if let Some(routes) = routes {
        info["fingerprint"] = json!(routes.fingerprint);
    }
    Ok(info)
}

/// This host's name, when the listener cannot report it.
fn host_name() -> String {
    #[cfg(unix)]
    {
        let mut buffer = [0_u8; 256];
        // SAFETY: the buffer is valid for its length; gethostname writes a
        // NUL-terminated name into it (or truncates).
        let done = unsafe { libc::gethostname(buffer.as_mut_ptr().cast(), buffer.len()) };
        if done == 0 {
            let end = buffer.iter().position(|b| *b == 0).unwrap_or(buffer.len());
            if let Ok(name) = std::str::from_utf8(&buffer[..end]) {
                if !name.trim().is_empty() {
                    return name.trim().to_owned();
                }
            }
        }
    }
    "Intent backend".to_owned()
}

async fn list(feature: &RemoteFeature, svc: &Services) -> Result<Value> {
    let machines: Vec<Value> = svc
        .store()
        .list_exposed_machines()
        .await?
        .iter()
        .map(|row| {
            let live = feature.running_node(&row.id);
            machine_json(row, live.as_ref().map(RemoteNode::shared_ref))
        })
        .collect();
    Ok(json!({ "machines": machines }))
}

/// `remoteMachines.models`: each exposed machine's state and, from its last
/// report, its providers' sign-in status and full model lists, in the
/// ordered machine list's order, with cloud entries as rows. Reads the saved rows only: no connect, no refresh. `nodeId`,
/// `provider` and `model` filter by exact match.
async fn models(feature: &RemoteFeature, svc: &Services, params: &Value) -> Result<Value> {
    let node_id = text(params, "nodeId", false)?;
    let provider = text(params, "provider", false)?;
    let model = text(params, "model", false)?;
    let all_rows = svc.store().list_exposed_machines().await?;
    let mut rows = all_rows.clone();
    if let Some(node_id) = node_id {
        rows.retain(|row| row.id == node_id);
        if rows.is_empty() {
            return Err(coded(
                false,
                "machine-unknown",
                format!("No exposed machine has id {node_id}."),
                json!({}),
            ));
        }
    }
    // The ordered machine list decides the rows' order and rank ("This
    // computer" counts but is not a row); cloud entries are rows too.
    let order = crate::order::current(svc).await?;
    let entries = crate::cloud::config::load_all(svc).await?;
    let sandboxes = if node_id.is_none() && model.is_none() {
        svc.store().list_cloud_machines().await?
    } else {
        Vec::new()
    };
    let mut machines = Vec::new();
    // `rank` numbers the rows the agent-facing list shows, whatever the
    // filters keep: not "This computer", not a revoked machine.
    let listed_rows = order.iter().filter(|row| match &row.key {
        MachineOrderKey::Local => false,
        MachineOrderKey::Machine { node_id } => all_rows
            .iter()
            .any(|machine| &machine.id == node_id && machine.state != RemoteMachineState::Revoked),
        MachineOrderKey::Cloud { .. } => true,
    });
    for (at, listed) in listed_rows.enumerate() {
        let rank = at + 1;
        match &listed.key {
            MachineOrderKey::Local => {}
            MachineOrderKey::Machine { node_id } => {
                let Some(row) = rows.iter().find(|row| &row.id == node_id) else {
                    continue;
                };
                let live = feature.running_node(&row.id);
                if let Some(mut machine) = machine_models_json(
                    row,
                    live.as_ref().map(RemoteNode::shared_ref),
                    provider,
                    model,
                ) {
                    machine["kind"] = json!("machine");
                    machine["rank"] = json!(rank);
                    machines.push(machine);
                }
            }
            MachineOrderKey::Cloud { cloud_machine_id } => {
                if node_id.is_some() || model.is_some() {
                    continue;
                }
                let Some((_, entry)) = entries.iter().find(|(id, _)| id == cloud_machine_id) else {
                    continue;
                };
                let providers: Vec<Value> = crate::cloud::config::agent_providers()
                    .into_iter()
                    .filter(|id| provider.is_none_or(|wanted| id == wanted))
                    .map(|id| json!({ "id": id, "status": "unknown", "models": [], "modelsStale": true }))
                    .collect();
                if providers.is_empty() && provider.is_some() {
                    continue;
                }
                let running: usize = sandboxes
                    .iter()
                    .filter(|row| {
                        row.cloud
                            .as_ref()
                            .is_some_and(|cloud| &cloud.cloud_machine_id == cloud_machine_id)
                    })
                    .filter_map(|row| feature.running_node(&row.id))
                    .map(|node| node.shared_ref().agents.live_count())
                    .sum();
                let state = if entry.state == crate::cloud::config::ProviderState::Ready {
                    RemoteMachineState::Connected
                } else {
                    RemoteMachineState::Unreachable
                };
                machines.push(json!({
                    "kind": "cloud",
                    "cloudMachineId": cloud_machine_id,
                    "name": listed.name,
                    "rank": rank,
                    "state": state,
                    "runningAgents": running,
                    "providers": providers,
                }));
            }
        }
    }
    Ok(json!({ "machines": machines }))
}

/// One `MachineModels` row, or `None` when a `provider` or `model` filter
/// leaves it no provider. Never carries routes, the pin, the capacity or
/// the link protocol.
fn machine_models_json(
    row: &ExposedMachine,
    live: Option<&Shared>,
    provider: Option<&str>,
    model: Option<&str>,
) -> Option<Value> {
    let state = if live.is_some_and(Shared::is_connected) {
        RemoteMachineState::Connected
    } else {
        row.state
    };
    let providers: Vec<Value> = row
        .capabilities
        .iter()
        .flat_map(|caps| caps.providers.iter())
        .filter(|entry| provider.is_none_or(|id| entry.id == id))
        .filter(|entry| model.is_none_or(|model| entry.models.iter().any(|m| m == model)))
        .map(|entry| {
            json!({
                "id": entry.id,
                "status": entry.status,
                "models": entry.models,
                "modelsStale": entry.models_stale,
            })
        })
        .collect();
    if providers.is_empty() && (provider.is_some() || model.is_some()) {
        return None;
    }
    let mut machine = Map::new();
    machine.insert("nodeId".into(), json!(row.id));
    machine.insert("name".into(), json!(row.name));
    if let Some(platform) = &row.platform {
        machine.insert("platform".into(), json!(platform));
    }
    machine.insert("state".into(), json!(state));
    if let Some(last) = &row.last_connected_at {
        machine.insert("lastConnectedAt".into(), json!(last));
    }
    if let Some(observed_at) = &row.observed_at {
        machine.insert("observedAt".into(), json!(observed_at));
    }
    machine.insert(
        "runningAgents".into(),
        json!(live.map_or(0, |shared| shared.agents.live_count())),
    );
    machine.insert("providers".into(), Value::Array(providers));
    if let Some(disabled) = row
        .capabilities
        .as_ref()
        .map(|caps| &caps.disabled_providers)
        .filter(|disabled| !disabled.is_empty())
    {
        machine.insert("disabledProviders".into(), json!(disabled));
    }
    Some(Value::Object(machine))
}

/// The validated `add` params.
pub(crate) struct AddParams {
    pub(crate) machine_id: String,
    pub(crate) name: String,
    pub(crate) hosts: Vec<String>,
    pub(crate) port: u16,
    pub(crate) tc_address: Option<String>,
    pub(crate) fingerprint: String,
    pub(crate) credential_id: String,
    pub(crate) token: String,
}

fn add_params(params: &Value) -> Result<AddParams> {
    let machine_id = required(params, "machineId")?;
    if !machine_id
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
    {
        return Err(Error::InvalidParams(
            "machineId must be letters, digits, '-', '_' or '.'".into(),
        ));
    }
    let name = required(params, "name")?;
    if name.len() > 128 || name.chars().any(char::is_control) {
        return Err(Error::InvalidParams(
            "name must be 1-128 UTF-8 bytes without control characters".into(),
        ));
    }
    let hosts = params
        .get("hosts")
        .and_then(Value::as_array)
        .ok_or_else(|| Error::InvalidParams("hosts must be an array of strings".into()))?;
    if hosts.len() > MAX_HOSTS {
        return Err(Error::InvalidParams(format!(
            "hosts must list at most {MAX_HOSTS} hosts"
        )));
    }
    let hosts = hosts
        .iter()
        .map(|host| match host.as_str() {
            Some(host)
                if !host.trim().is_empty()
                    && host.len() <= MAX_TEXT_BYTES
                    && !host.contains(['/', '?', '#', '@', ' ']) =>
            {
                Ok(host.trim().to_owned())
            }
            _ => Err(Error::InvalidParams(
                "each host must be a host name or address".into(),
            )),
        })
        .collect::<Result<Vec<_>>>()?;
    let port = params
        .get("port")
        .and_then(Value::as_u64)
        .and_then(|port| u16::try_from(port).ok())
        .filter(|port| *port != 0)
        .ok_or_else(|| Error::InvalidParams("port must be 1-65535".into()))?;
    let tc_address = text(params, "tcAddress", false)?.map(str::to_owned);
    // A loopback-only listener advertises no hosts: its tailcat address is
    // then the only route.
    if hosts.is_empty() && tc_address.is_none() {
        return Err(Error::InvalidParams(
            "hosts or tcAddress must give at least one route".into(),
        ));
    }
    Ok(AddParams {
        machine_id: machine_id.to_owned(),
        name: name.to_owned(),
        hosts,
        port,
        tc_address,
        fingerprint: required(params, "fingerprint")?.to_owned(),
        credential_id: required(params, "credentialId")?.to_owned(),
        // Validated (and never echoed) when the target is built.
        token: params
            .get("token")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::InvalidParams("token is required".into()))?
            .to_owned(),
    })
}

async fn add(feature: &RemoteFeature, svc: &Services, params: &Value) -> Result<Value> {
    let params = add_params(params)?;
    let (_, result) = add_machine(feature, svc, params, None).await?;
    Ok(result)
}

/// Probe and save a machine, as `remoteMachines.add` does; with `cloud`, a
/// cloud machine row the backend started itself. Returns the saved row and
/// the method's result.
pub(crate) async fn add_machine(
    feature: &RemoteFeature,
    svc: &Services,
    params: AddParams,
    cloud: Option<intent_core::machines::CloudMachine>,
) -> Result<(ExposedMachine, Value)> {
    // Held until saved, so no agent starts on the machine between the
    // in-use check and a credential replacement.
    let _lifecycle = feature.lifecycle(&params.machine_id).await;
    let store = svc.store();
    let previous = store.get_exposed_machine(&params.machine_id).await?;
    // A cloud machine's id is never taken over by the expose flow, nor the
    // other way round.
    if previous
        .as_ref()
        .is_some_and(|row| row.is_cloud() != cloud.is_some())
    {
        return Err(Error::InvalidParams(format!(
            "machine {} is already known to this backend as another kind of machine",
            params.machine_id
        )));
    }
    let replacing_credential = previous
        .as_ref()
        .is_some_and(|row| row.credential_id.as_str() != params.credential_id);
    let mut current = feature.running_node(&params.machine_id);
    if replacing_credential {
        // After a backend restart, survivors are known only once the old
        // credential's connector binds: reclaim them before replacing it.
        if let Some(previous) = previous
            .as_ref()
            .filter(|_| current.as_ref().is_none_or(|node| !node.has_claimed()))
        {
            current = recover_survivors(feature, svc, previous, current).await?;
        }
        if let Some(node) = &current {
            let running = node.running();
            if running > 0 {
                return Err(in_use(&params.name, running));
            }
        }
    }
    let target = MachineTarget::from_parts(
        &params.machine_id,
        &params.name,
        params.hosts.clone(),
        params.port,
        params.tc_address.clone(),
        &params.fingerprint,
        &params.token,
        feature.tailcat_bin(),
    )?
    .tapped(svc.link_traffic().clone());

    // Adding the machine's current credential again only refreshes its
    // routes: keep its live lease and offer that lease's next generation, as
    // the connector would, since the machine refuses one it already saw.
    let kept = if previous.is_some() && !replacing_credential {
        HeadLease::open(store.clone(), &params.machine_id)
            .await
            .ok()
    } else {
        None
    };
    // Otherwise probe a new lease of this credential, exactly as the
    // connector will, without saving anything yet.
    let owner = match &kept {
        Some(lease) => lease.owner().clone(),
        None => new_owner(store, &params.machine_id).await?,
    };
    let acked = match &kept {
        Some(lease) => lease.acked().await?,
        None => NodeCounter(0),
    };
    let generation = std::sync::atomic::AtomicU64::new(0);
    let probed = connect(&target, &feature.tuning().connect, None, || {
        let kept = kept.clone();
        let next = generation.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
        let fresh = probe_hello(&owner, NodeCounter(next));
        async move {
            match kept {
                Some(lease) => lease.next_hello(acked).await,
                None => Ok(fresh),
            }
        }
    })
    .await
    .map_err(|failure| match failure {
        // A refused credential at add time is one more route error.
        ConnectFailure::Revoked(attempts) => ConnectFailure::Unreachable(attempts),
        other => other,
    })
    .map_err(|failure| failure.into_error(&params.name))?;
    let Connected { dialed, .. } = probed;
    dialed.link.close();
    let welcome = dialed.welcome;
    let bound_at = NodeCounter(generation.load(std::sync::atomic::Ordering::SeqCst));

    let hosts = if welcome.hosts.is_empty() && welcome.tc_address.is_none() {
        params.hosts.clone()
    } else {
        welcome.hosts.clone()
    };
    // The tailcat address is stable, so a machine whose tunnel is down
    // right now keeps the one it was added with or last reported.
    let tc_address = welcome
        .tc_address
        .clone()
        .or_else(|| params.tc_address.clone())
        .or_else(|| {
            previous
                .as_ref()
                .filter(|row| same_fingerprint(&row.fingerprint, &params.fingerprint))
                .and_then(|row| row.tc_address.clone())
        });
    let now = now_iso();
    let row = ExposedMachine {
        id: params.machine_id.clone(),
        name: params.name.clone(),
        hosts,
        port: params.port,
        tc_address,
        fingerprint: params.fingerprint.clone(),
        credential_id: ExecutorCredentialId::from(params.credential_id.clone()),
        platform: Some(welcome.capabilities.platform),
        capabilities: Some(welcome.capabilities.clone()),
        observed_at: Some(welcome.capabilities.observed_at.clone()),
        state: RemoteMachineState::Disconnected,
        added_at: previous
            .as_ref()
            .map_or_else(|| now.clone(), |row| row.added_at.clone()),
        last_connected_at: Some(now),
        node_protocol: Some(welcome.protocol),
        cloud,
    };

    // Save. A new credential replaces the lease: the old connector stops
    // first, so nothing dials with the old credential or lease once the new
    // ones are in place. The same credential keeps its connector (and any
    // live agents), which takes the refreshed routes.
    if kept.is_none() {
        if let Some(node) = feature.take_node(&params.machine_id) {
            node.shutdown();
        }
    }
    svc.store_remote_machine_token(&params.machine_id, &params.token)
        .await?;
    let saved = async {
        if kept.is_none() {
            record_new_lease(store, &target, &owner, bound_at).await?;
        }
        store.save_exposed_machine(&row).await
    }
    .await;
    let replaced = match saved {
        Ok(replaced) => replaced,
        Err(error) => {
            if previous.is_none() {
                let _ = svc.delete_remote_machine_token(&params.machine_id).await;
            }
            return Err(error);
        }
    };
    let node = feature.node_locked(svc, &params.machine_id).await?;
    let mut accepted = target;
    accepted.hosts.clone_from(&row.hosts);
    accepted.tc_address.clone_from(&row.tc_address);
    node.shared_ref().set_target(accepted);
    let machine = machine_json(&row, Some(node.shared_ref()));
    publish_machine(svc, &row, Some(node.shared_ref())).await;
    if !row.is_cloud() {
        crate::order::publish(svc).await;
    }
    let mut result = json!({ "machine": machine });
    if let Some(replaced) = replaced {
        result["replacedCredentialId"] = json!(replaced);
    }
    Ok((row, result))
}

fn same_fingerprint(a: &str, b: &str) -> bool {
    let normalize = |fingerprint: &str| {
        fingerprint
            .chars()
            .filter(|c| *c != ':')
            .map(|c| c.to_ascii_lowercase())
            .collect::<String>()
    };
    normalize(a) == normalize(b)
}

pub(crate) fn in_use(name: &str, running: usize) -> Error {
    coded(
        false,
        "machine-in-use",
        format!(
            "{running} remote agent(s) still run on {name}; stop them or pass stopAgents: true."
        ),
        json!({ "runningAgents": running }),
    )
}

async fn remove(feature: &RemoteFeature, svc: &Services, params: &Value) -> Result<Value> {
    let machine_id = required(params, "machineId")?.to_owned();
    let stop_agents = match params.get("stopAgents") {
        None | Some(Value::Null) => false,
        Some(Value::Bool(stop)) => *stop,
        Some(_) => {
            return Err(Error::InvalidParams("stopAgents must be a boolean".into()));
        }
    };
    let _lifecycle = feature.lifecycle(&machine_id).await;
    let store = svc.store();
    // A cloud machine is unknown here: `cloudMachines.kill` removes it.
    let Some(row) = store
        .get_exposed_machine(&machine_id)
        .await?
        .filter(|row| !row.is_cloud())
    else {
        return Ok(json!({ "ok": true }));
    };
    let node = match feature.running_node(&machine_id) {
        Some(node) if node.has_claimed() => Some(node),
        unbound => recover_survivors(feature, svc, &row, unbound).await?,
    };
    if let Some(node) = node {
        let running = node.running();
        if running > 0 && !stop_agents {
            return Err(in_use(&row.name, running));
        }
        let stopped = node.stop_all().await;
        deliver_stops(feature, &node, &stopped, &row.name).await?;
    }
    if let Some(node) = feature.take_node(&machine_id) {
        node.shutdown();
    }
    // The machine is forgotten: no connector will resend a final stop it
    // did not acknowledge (it was revoked), so their records go too.
    let head_id = store.daemon_id().await?;
    for lease in store.list_owned_node_leases(&head_id).await? {
        if lease.owner.node_id != machine_id {
            continue;
        }
        for stop in store.list_final_stops(&lease.owner.lease_id).await? {
            store
                .finish_final_stop(&stop.agent_id, &stop.stop_id)
                .await?;
        }
    }
    let credential = store.remove_exposed_machine(&machine_id).await?;
    svc.delete_remote_machine_token(&machine_id).await?;
    store
        .end_machine_leases(&machine_id, &store.daemon_id().await?)
        .await?;
    svc.publish_remote_machine_changed(json!({ "machineId": machine_id, "removed": true }))
        .await;
    crate::order::forget(
        svc,
        &MachineOrderKey::Machine {
            node_id: machine_id.clone(),
        },
    )
    .await;
    let mut result = json!({ "ok": true });
    if let Some(credential) = credential {
        result["credentialId"] = json!(credential);
    }
    Ok(result)
}

/// The connector to use for removing `row` when no bind in this process has
/// claimed its agents yet (no connector, one that has not connected since it
/// started, or one whose first bind is still between its route and its claim):
/// after a backend restart the machine keeps its agents running, and only a
/// bind's welcome tells which of the agents recorded on its live leases
/// still run. With none recorded, `unbound` is kept as is. Otherwise the
/// connector is started (the caller holds the lifecycle lock) and connected
/// within [`crate::connector::Tuning::remove_timeout`], so the welcome
/// claims the survivors before they are counted. A revoked credential needs
/// no stops; an unreachable machine fails the removal `machine-unreachable`
/// and keeps the machine, its token and its lease.
pub(crate) async fn recover_survivors(
    feature: &RemoteFeature,
    svc: &Services,
    row: &ExposedMachine,
    unbound: Option<RemoteNode>,
) -> Result<Option<RemoteNode>> {
    let store = svc.store();
    let head_id = store.daemon_id().await?;
    let mut recorded = 0;
    for lease in store.list_owned_node_leases(&head_id).await? {
        if lease.owner.node_id == row.id && !lease.state.is_terminal() {
            recorded += store.list_remote_agents(&lease.owner.lease_id).await?.len();
        }
    }
    if recorded == 0 {
        return Ok(unbound);
    }
    let node = match unbound {
        Some(node) => node,
        None => feature.node_locked(svc, &row.id).await?,
    };
    let connected = tokio::time::timeout(feature.tuning().remove_timeout, node.connect()).await;
    match connected {
        // Connected, but its bind may not have read this lease's records:
        // claim again before anything is counted, and refuse while the
        // survivors cannot be told apart.
        Ok(Ok(())) if node.reclaim().await => Ok(Some(node)),
        Ok(Ok(())) => Err(Error::Internal(format!(
            "{}: could not tell which of its {recorded} recorded remote agent(s) still run, because this backend's agent records could not be read; nothing was changed. Try again.",
            row.name
        ))),
        Ok(Err(Error::RemoteMachine(failure)))
            if failure.data["code"] == crate::connect::MACHINE_REVOKED =>
        {
            Ok(Some(node))
        }
        Ok(Err(Error::RemoteMachine(failure))) => Err(survivors_unknown(
            &row.name,
            recorded,
            &failure.data["attempts"],
        )),
        Ok(Err(other)) => Err(other),
        Err(_) => Err(survivors_unknown(&row.name, recorded, &json!([]))),
    }
}

fn survivors_unknown(name: &str, recorded: usize, attempts: &Value) -> Error {
    coded(
        true,
        crate::connect::MACHINE_UNREACHABLE,
        format!(
            "{name} is unreachable, so it cannot confirm whether its {recorded} remote agent(s) still run; try again when it is reachable, or revoke the credential on the machine."
        ),
        json!({ "attempts": attempts }),
    )
}

/// Wait for the machine to acknowledge the stops of `agents` before it is
/// forgotten: the machine keeps its agents running across a lost link, and
/// once its credential and lease are gone nothing could reach them. Waits at
/// most [`crate::connector::Tuning::remove_timeout`] (30 s), the connect
/// included, or [`crate::connector::Tuning::final_remove_timeout`] (90 s, to
/// fit the machine's 60 s final push) once the link speaks nodeProtocol 9. When any stop is unacknowledged the removal fails
/// `machine-unreachable` with `unconfirmedStops` and everything stays, the
/// stops still queued and resent. A revoked credential needs no stops.
pub(crate) async fn deliver_stops(
    feature: &RemoteFeature,
    node: &RemoteNode,
    agents: &[AgentId],
    name: &str,
) -> Result<()> {
    if agents.is_empty() {
        return Ok(());
    }
    let started = tokio::time::Instant::now();
    let deadline = started + feature.tuning().remove_timeout;
    let mut attempts = json!([]);
    // A connect that does not finish in time leaves the stops unconfirmed.
    if let Ok(Err(error)) = tokio::time::timeout_at(deadline, node.connect()).await {
        match error {
            Error::RemoteMachine(failure)
                if failure.data["code"] == crate::connect::MACHINE_REVOKED =>
            {
                return Ok(());
            }
            Error::RemoteMachine(failure) => attempts = failure.data["attempts"].clone(),
            other => return Err(other),
        }
    }
    // An unreachable machine cannot ack now; the stops stay queued.
    let unreachable = attempts.as_array().is_some_and(|a| !a.is_empty());
    let wait_until = if unreachable {
        tokio::time::Instant::now()
    } else {
        started + node.shared_ref().remove_budget()
    };
    // The agents' stops and their script runs' stops are awaited together,
    // within the same budget: every script stop is sent before any wait, and
    // an unacknowledged script stop leaves its agent unconfirmed.
    let shared = node.shared();
    let (mut unconfirmed, scripts_unacked) = tokio::join!(
        node.wait_stops(agents, wait_until),
        shared.end_agents_machine_runs(
            agents,
            wait_until,
            "remote-script-lost: the machine was removed",
        ),
    );
    for agent in scripts_unacked {
        if !unconfirmed.contains(&agent) {
            unconfirmed.push(agent);
        }
    }
    if unconfirmed.is_empty() {
        return Ok(());
    }
    Err(coded(
        true,
        crate::connect::MACHINE_UNREACHABLE,
        format!(
            "{name} did not acknowledge the stops of {} agent(s); try again when it is reachable, or revoke the credential on the machine.",
            unconfirmed.len()
        ),
        json!({
            "attempts": attempts,
            "unconfirmedStops": unconfirmed.iter().map(ToString::to_string).collect::<Vec<_>>(),
        }),
    ))
}

async fn refresh(feature: &RemoteFeature, svc: &Services, params: &Value) -> Result<Value> {
    let machine_id = required(params, "machineId")?.to_owned();
    if svc
        .store()
        .get_exposed_machine(&machine_id)
        .await?
        .is_none_or(|row| row.is_cloud())
    {
        return Err(not_found(&machine_id));
    }
    let node = feature.node(svc, &machine_id).await?;
    if let Err(error) = node.refresh_capabilities().await {
        // A connect failure is already `machine-unreachable` / `-revoked` /
        // `-update-required`; a link that bound and then failed (no report
        // in time, closed) is unreachable on that route too.
        return Err(match intent_transport::node_link::error_code(&error) {
            Some(code) => crate::connect::live_link_failure(
                &node.status().node_name,
                node.route().as_deref().unwrap_or("none"),
                code,
            ),
            None => error,
        });
    }
    let row = svc
        .store()
        .get_exposed_machine(&machine_id)
        .await?
        .ok_or_else(|| not_found(&machine_id))?;
    Ok(json!({ "machine": machine_json(&row, Some(node.shared_ref())) }))
}
