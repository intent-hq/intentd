//! `cloudMachines.configure`, `list`, `remove`, `pause` and `kill`. The
//! services already checked the caller: the owner for `configure` and
//! `remove`, the owner or a shared-host member for the others; `pause` and
//! `kill` then admit only the owner and the machine's starter.

use serde_json::{json, Value};

use intent_core::machines::{
    with_cloud_machine_id, MAX_CLOUD_MACHINE_ENTRIES, MAX_CLOUD_MACHINE_ID_BYTES,
};
use intent_core::machines::{CloudMachineSize, ExposedMachine, CLOUD_PROVIDER_E2B};
use intent_core::{now_iso, Error, Result, WorkspaceId};
use intent_nodes::cloud::SecretString;
use intent_services::feature_api::Services;

use super::config::{self, ProviderConfig};
use crate::RemoteFeature;

/// Longest accepted API key, in bytes.
const MAX_KEY_BYTES: usize = 512;
/// Longest accepted template project slug, in bytes.
const MAX_PROJECT_BYTES: usize = 64;

/// Run `cloudMachines.<method>`.
pub(crate) async fn call(svc: &Services, method: &str, params: Value) -> Result<Value> {
    let feature = super::feature(svc)?;
    match method {
        "configure" => configure(feature, svc, &params).await,
        "list" => list(feature, svc).await,
        "remove" => remove(feature, svc, &params).await,
        "pause" => pause(feature, svc, &params).await,
        "kill" => kill(feature, svc, &params).await,
        other => Err(Error::InvalidParams(format!(
            "unknown method cloudMachines.{other}"
        ))),
    }
}

/// `provider`, optional: `"e2b"` is the only value.
fn provider_param(params: &Value) -> Result<()> {
    match params.get("provider") {
        None | Some(Value::Null) => Ok(()),
        Some(Value::String(provider)) if provider == CLOUD_PROVIDER_E2B => Ok(()),
        Some(_) => Err(Error::InvalidParams(
            "provider must be \"e2b\", the only cloud machine provider".into(),
        )),
    }
}

/// An entry id member (`cloudMachineId`, `apiKeyFrom`), when given.
fn entry_param(params: &Value, name: &str) -> Result<Option<String>> {
    match params.get(name) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(id)) if id.len() <= MAX_CLOUD_MACHINE_ID_BYTES => Ok(Some(id.clone())),
        Some(_) => Err(Error::InvalidParams(format!(
            "{name} must be a string of at most {MAX_CLOUD_MACHINE_ID_BYTES} bytes"
        ))),
    }
}

/// `-32602` `not-found` for an entry id that names no entry.
fn entry_not_found(id: &str, what: &str) -> Error {
    Error::remote_machine(
        false,
        format!("No E2B entry {id}{what}."),
        json!({ "code": "not-found" }),
    )
}

/// The E2B entry cloud machine `row` belongs to.
pub(crate) fn row_entry(row: &ExposedMachine) -> &str {
    row.cloud.as_ref().map_or(config::legacy_entry(), |cloud| {
        cloud.cloud_machine_id.as_str()
    })
}

fn bool_param(params: &Value, name: &str) -> Result<bool> {
    match params.get(name) {
        None | Some(Value::Null) => Ok(false),
        Some(Value::Bool(value)) => Ok(*value),
        Some(_) => Err(Error::InvalidParams(format!("{name} must be a boolean"))),
    }
}

fn range_param(params: &Value, name: &str, min: u32, max: u32) -> Result<Option<u32>> {
    match params.get(name) {
        None | Some(Value::Null) => Ok(None),
        Some(value) => value
            .as_u64()
            .and_then(|n| u32::try_from(n).ok())
            .filter(|n| (min..=max).contains(n))
            .map(Some)
            .ok_or_else(|| Error::InvalidParams(format!("{name} must be an integer {min}-{max}"))),
    }
}

/// The validated `configure` params. `api_key` is never echoed.
struct ConfigureParams {
    cloud_machine_id: Option<String>,
    name: Option<String>,
    api_key_from: Option<String>,
    api_key: Option<SecretString>,
    size: Option<CloudMachineSize>,
    idle_pause_minutes: Option<u32>,
    paused_delete_days: Option<u32>,
    template_project: Option<String>,
    validate_only: bool,
}

fn configure_params(params: &Value) -> Result<ConfigureParams> {
    provider_param(params)?;
    let api_key = match params.get("apiKey") {
        None | Some(Value::Null) => None,
        Some(Value::String(key))
            if !key.trim().is_empty()
                && key.len() <= MAX_KEY_BYTES
                && !key.chars().any(char::is_control) =>
        {
            Some(SecretString::new(key.trim()))
        }
        Some(_) => {
            return Err(Error::InvalidParams(format!(
                "apiKey must be a non-blank string of at most {MAX_KEY_BYTES} bytes"
            )))
        }
    };
    let size = match params.get("size") {
        None | Some(Value::Null) => None,
        Some(Value::String(id)) => Some(CloudMachineSize::parse(id).ok_or_else(|| {
            Error::InvalidParams("size must be one of \"2x4\", \"4x8\", \"8x16\", \"16x32\"".into())
        })?),
        Some(_) => return Err(Error::InvalidParams("size must be a string".into())),
    };
    let template_project = match params.get("templateProject") {
        None | Some(Value::Null) => None,
        Some(Value::String(slug))
            if !slug.is_empty()
                && slug.len() <= MAX_PROJECT_BYTES
                && slug
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.')) =>
        {
            Some(slug.clone())
        }
        Some(_) => {
            return Err(Error::InvalidParams(
                "templateProject must be an E2B project slug (letters, digits, '-', '_', '.')"
                    .into(),
            ))
        }
    };
    let name = match params.get("name") {
        None | Some(Value::Null) => None,
        Some(Value::String(name)) => Some(config::entry_name(name)?),
        Some(_) => return Err(Error::InvalidParams("name must be a string".into())),
    };
    let api_key_from = entry_param(params, "apiKeyFrom")?;
    if api_key.is_some() && api_key_from.is_some() {
        return Err(Error::InvalidParams(
            "give apiKey or apiKeyFrom, not both".into(),
        ));
    }
    Ok(ConfigureParams {
        cloud_machine_id: entry_param(params, "cloudMachineId")?,
        name,
        api_key_from,
        api_key,
        size,
        idle_pause_minutes: range_param(params, "idlePauseMinutes", 1, 1440)?,
        paused_delete_days: range_param(params, "pausedDeleteDays", 1, 30)?,
        template_project,
        validate_only: bool_param(params, "validateOnly")?,
    })
}

/// The entry a `configure` call writes: `(id, saved)`. With
/// `cloudMachineId`, that entry (`not-found` when unknown); with only
/// `name`, a new entry (`None`, `None`); with neither, the entry `"e2b"`,
/// new when missing.
fn configure_target(
    entries: &[(String, ProviderConfig)],
    params: &ConfigureParams,
) -> Result<(Option<String>, Option<ProviderConfig>)> {
    let find = |id: &str| {
        entries
            .iter()
            .find(|(entry, _)| entry == id)
            .map(|(_, saved)| saved.clone())
    };
    match (&params.cloud_machine_id, &params.name) {
        (Some(id), _) => {
            let saved = find(id).ok_or_else(|| entry_not_found(id, ""))?;
            Ok((Some(id.clone()), Some(saved)))
        }
        (None, Some(_)) => Ok((None, None)),
        (None, None) => {
            let id = config::legacy_entry();
            Ok((Some(id.to_owned()), find(id)))
        }
    }
}

async fn configure(feature: &RemoteFeature, svc: &Services, params: &Value) -> Result<Value> {
    let params = configure_params(params)?;
    let _settings = feature.cloud.settings.lock().await;
    let entries = config::load_all(svc).await?;
    let (id, saved) = configure_target(&entries, &params)?;
    let tag = |error: Error| with_cloud_machine_id(error, id.as_deref().unwrap_or_default());
    let creating = saved.is_none();
    if creating && entries.len() >= MAX_CLOUD_MACHINE_ENTRIES {
        return Err(Error::InvalidParams(format!(
            "at most {MAX_CLOUD_MACHINE_ENTRIES} E2B entries can be set up"
        )));
    }
    let name = match (&params.name, &saved) {
        (Some(name), _) => name.clone(),
        (None, Some(saved)) => saved.name.clone(),
        (None, None) => config::free_legacy_name(&entries, config::legacy_entry()),
    };
    let taken = entries.iter().any(|(other, saved)| {
        Some(other) != id.as_ref() && saved.name.to_lowercase() == name.to_lowercase()
    });
    if taken {
        return Err(Error::InvalidParams(format!(
            "an E2B entry named \"{name}\" already exists"
        )));
    }
    let copied = match &params.api_key_from {
        Some(from) if !entries.iter().any(|(entry, _)| entry == from) => {
            return Err(entry_not_found(from, ""));
        }
        Some(from) if Some(from) == id.as_ref() => {
            return Err(Error::InvalidParams(
                "apiKeyFrom must name another E2B entry".into(),
            ))
        }
        Some(from) => Some(
            config::saved_key(svc, from)
                .await?
                .ok_or_else(|| entry_not_found(from, " with a saved key"))?,
        ),
        None => None,
    };
    let mut next = saved.clone().unwrap_or_else(|| ProviderConfig::new(&name));
    next.name.clone_from(&name);
    if let Some(size) = params.size {
        next.size = size;
    }
    if let Some(minutes) = params.idle_pause_minutes {
        next.idle_pause_minutes = minutes;
    }
    if let Some(days) = params.paused_delete_days {
        next.paused_delete_days = days;
    }
    if let Some(project) = &params.template_project {
        next.template_project.clone_from(project);
    }
    let given = params.api_key.clone().or(copied);
    let key = match (&given, &id) {
        (Some(key), _) => Some(key.clone()),
        (None, Some(id)) if !creating => config::saved_key(svc, id).await?,
        (None, _) => None,
    };
    let Some(key) = key else {
        return Err(tag(config::not_configured()));
    };
    // The key is tested before anything is saved, whenever one is given or
    // with `validateOnly`. The published sizes come from the release's
    // template manifest, never from probing templates with the key; they
    // are read whenever the call can change the template.
    if given.is_some() || params.validate_only {
        config::client(key.clone())
            .map_err(tag)?
            .check_key()
            .await
            .map_err(|error| tag(config::to_error(&error, config::Phase::Other)))?;
        config::note_outcome(&mut next, None);
    }
    let check_template = params.validate_only
        || given.is_some()
        || params.size.is_some()
        || params.template_project.is_some()
        || creating;
    if check_template {
        next.set_sizes(config::published_sizes(&next.template_project).await);
        if !next.allows(next.size) {
            return Err(tag(config::template_missing(&next.template(next.size))));
        }
    }
    if params.validate_only {
        let mut provider = next.to_json(id.as_deref().filter(|_| !creating).unwrap_or(""));
        provider["state"] = json!("ready");
        return Ok(json!({ "provider": provider }));
    }
    let id = match id {
        Some(id) => id,
        None => config::new_cloud_machine_id(svc).await?,
    };
    if creating {
        next.configured_at = now_iso();
    }
    // The key goes first: an entry is never saved without one.
    if let Some(key) = &given {
        svc.store_cloud_machine_key(&id, key.expose()).await?;
    }
    config::save(svc, &id, &next).await?;
    let provider = next.to_json(&id);
    svc.publish_cloud_machine_changed(json!({ "provider": provider.clone() }), None)
        .await;
    if let Some(saved) = saved.filter(|saved| saved.name != next.name) {
        rename(feature, svc, &id, &saved.name, &next.name).await;
    }
    Ok(json!({ "provider": provider }))
}

/// Entry `id` was renamed from `old` to `new`: rename its machine rows and
/// set its agents' `nodeName`, announcing each.
async fn rename(feature: &RemoteFeature, svc: &Services, id: &str, old: &str, new: &str) {
    let rows = entry_rows(svc, id).await.unwrap_or_default();
    for row in rows {
        let prefix = format!("{old} · ");
        let agent = row.name.strip_prefix(&prefix).unwrap_or(&row.name);
        let name = super::start::machine_name(new, agent);
        match svc.store().rename_cloud_machine(&row.id, &name).await {
            Ok(true) => {
                if let Ok(Some(renamed)) = svc.store().get_exposed_machine(&row.id).await {
                    let live = feature.running_node(&row.id);
                    crate::methods::publish_machine(
                        svc,
                        &renamed,
                        live.as_ref().map(crate::RemoteNode::shared_ref),
                    )
                    .await;
                }
            }
            Ok(false) => {}
            Err(error) => {
                tracing::warn!(machine = %row.id, %error, "cloud machines: could not rename a machine");
            }
        }
    }
    let agents = svc
        .store()
        .list_cloud_placed_agents()
        .await
        .unwrap_or_default();
    for (workspace_id, agent_id) in agents {
        let changed = crate::view::update(svc.store(), &workspace_id, &agent_id, |view| {
            if view.placement.cloud_machine_id.as_deref() == Some(id) {
                new.clone_into(&mut view.node_name);
            }
        })
        .await;
        if matches!(changed, Ok(true)) {
            svc.publish_agent_placement_changed(&workspace_id, &agent_id)
                .await;
        }
    }
}

/// The machines `svc`'s caller may see: all for the owner, those of
/// workspaces a member can manage.
async fn visible(svc: &Services, rows: Vec<ExposedMachine>) -> Vec<ExposedMachine> {
    let mut shown = Vec::with_capacity(rows.len());
    for row in rows {
        let Some(cloud) = &row.cloud else { continue };
        let workspace = WorkspaceId::from_string(cloud.workspace_id.clone());
        if svc
            .require_workspace_manager(&workspace, "cloudMachines.list")
            .await
            .is_ok()
        {
            shown.push(row);
        }
    }
    shown
}

async fn list(feature: &RemoteFeature, svc: &Services) -> Result<Value> {
    let mut providers = Vec::new();
    for (id, mut saved) in config::load_all(svc).await? {
        if saved.sizes_stale() {
            refresh_sizes(feature, svc, &id, &mut saved).await;
        }
        providers.push(saved.to_json(&id));
    }
    let machines: Vec<Value> = visible(svc, svc.store().list_cloud_machines().await?)
        .await
        .iter()
        .map(|row| {
            let live = feature.running_node(&row.id);
            crate::methods::machine_json(row, live.as_ref().map(crate::RemoteNode::shared_ref))
        })
        .collect();
    Ok(json!({ "providers": providers, "machines": machines }))
}

/// Read the published sizes again (from the release manifest) and test the
/// saved key. A change of the list or the key's state is saved and
/// announced.
async fn refresh_sizes(
    feature: &RemoteFeature,
    svc: &Services,
    id: &str,
    saved: &mut ProviderConfig,
) {
    let Ok(_settings) = feature.cloud.settings.try_lock() else {
        return;
    };
    // A configure or remove may have run since the list read it.
    match config::load(svc, id).await {
        Ok(Some(current)) => *saved = current,
        _ => return,
    }
    let mut next = saved.clone();
    next.set_sizes(config::published_sizes(&saved.template_project).await);
    // The key too, so one revoked at E2B shows as rejected and placement
    // refuses before any sandbox.
    match config::saved_client(svc, id).await {
        Ok(client) => match client.check_key().await {
            Ok(()) => config::note_outcome(&mut next, None),
            Err(error) => config::note_outcome(
                &mut next,
                Some(&config::to_error(&error, config::Phase::Other)),
            ),
        },
        Err(error) => config::note_outcome(&mut next, Some(&error)),
    }
    let announce = next.sizes != saved.sizes
        || next.sizes_verified != saved.sizes_verified
        || next.state != saved.state;
    if let Err(error) = config::save(svc, id, &next).await {
        tracing::warn!(%error, "cloud machines: could not save the checked sizes");
        return;
    }
    *saved = next;
    if announce {
        svc.publish_cloud_machine_changed(json!({ "provider": saved.to_json(id) }), None)
            .await;
    }
}

/// The cloud machines of entry `id`.
async fn entry_rows(svc: &Services, id: &str) -> Result<Vec<ExposedMachine>> {
    Ok(svc
        .store()
        .list_cloud_machines()
        .await?
        .into_iter()
        .filter(|row| row_entry(row) == id)
        .collect())
}

async fn remove(feature: &RemoteFeature, svc: &Services, params: &Value) -> Result<Value> {
    provider_param(params)?;
    let id =
        entry_param(params, "cloudMachineId")?.unwrap_or_else(|| config::legacy_entry().to_owned());
    let stop_agents = bool_param(params, "stopAgents")?;
    let _settings = feature.cloud.settings.lock().await;
    // A removed entry's key kept for the reaper names no entry.
    if config::is_retired(svc, &id).await? {
        return Ok(json!({ "ok": true, "unconfirmedStops": [] }));
    }
    let saved = config::load(svc, &id).await?;
    // An id that names no entry is a no-op, unless an earlier remove
    // stopped part way (a store or secret-store error after the settings
    // went): then this one finishes the cleanup.
    if saved.is_none()
        && entry_rows(svc, &id).await?.is_empty()
        && config::saved_key(svc, &id).await?.is_none()
    {
        return Ok(json!({ "ok": true, "unconfirmedStops": [] }));
    }
    // A sandbox still starting counts as live.
    let starting = feature.cloud.starts_in_flight(&id);
    let rows = entry_rows(svc, &id).await?;
    if (!rows.is_empty() || !starting.is_empty()) && !stop_agents {
        let running: usize = rows
            .iter()
            .map(|row| {
                feature
                    .running_node(&row.id)
                    .map_or(0, |node| node.running())
            })
            .sum();
        let name = saved
            .as_ref()
            .map_or(intent_core::machines::CLOUD_PROVIDER_E2B_NAME, |saved| {
                saved.name.as_str()
            });
        return Err(crate::methods::in_use(
            name,
            running.max(rows.len() + starting.len()),
        ));
    }
    // Order matters, so no sandbox outlives the key that can delete it:
    // 1. Forget the entry's settings: admission refuses new starts with it,
    //    and a start already past create finds them gone and deletes its
    //    own sandbox. The key stays until the end, for these deletes.
    // 2. Wait for every start with this entry registered by now (a later
    //    one reads the settings after step 1 and makes no sandbox), within
    //    one deadline.
    // 3. Stop and delete every sandbox of the entry at once, so the
    //    final-stop wait is paid once, not per sandbox.
    // 4. Agents left on the entry lose it, so their next start picks one
    //    again (a later entry "e2b" never inherits them by accident).
    // 5. Forget the key; when E2B kept one of the entry's sandboxes, keep
    //    it hidden for the reaper.
    // Other entries, their sandboxes and their starts are untouched.
    let client = config::saved_client(svc, &id).await.ok();
    svc.store().delete_cloud_machine_provider(&id).await?;
    let deadline = tokio::time::Instant::now() + super::start::START_WAIT;
    for mut start in feature.cloud.starts_in_flight(&id) {
        let _ = tokio::time::timeout_at(deadline, start.wait_for(Option::is_some)).await;
    }
    let mut deletes = tokio::task::JoinSet::new();
    for row in entry_rows(svc, &id).await? {
        let svc = svc.clone();
        let client = client.clone();
        deletes.spawn(async move {
            let Ok(feature) = super::feature(&svc) else {
                return Vec::new();
            };
            super::start::delete_machine(feature, &svc, &row, client.as_deref()).await
        });
    }
    let mut unconfirmed = Vec::new();
    while let Some(done) = deletes.join_next().await {
        match done {
            Ok(agents) => unconfirmed.extend(agents),
            Err(error) => {
                tracing::warn!(%error, "cloud machines: a sandbox delete did not finish");
            }
        }
    }
    forget_entry_on_agents(svc, &id).await?;
    let left = match &client {
        Some(client) => super::lifecycle::entry_sandboxes_left(svc, &**client, &id).await,
        None => false,
    };
    if left {
        retire_key(svc, &id).await?;
    }
    svc.delete_cloud_machine_key(&id).await?;
    svc.publish_cloud_machine_changed(json!({ "providerId": id, "removed": true }), None)
        .await;
    Ok(json!({ "ok": true, "unconfirmedStops": unconfirmed }))
}

/// Clear `placement.cloudMachineId` on every agent still on entry `id`.
async fn forget_entry_on_agents(svc: &Services, id: &str) -> Result<()> {
    for (workspace_id, agent_id) in svc.store().list_cloud_placed_agents().await? {
        let changed = crate::view::update(svc.store(), &workspace_id, &agent_id, |view| {
            if view.placement.cloud_machine_id.as_deref() == Some(id) {
                view.placement.cloud_machine_id = None;
            }
        })
        .await?;
        if changed {
            svc.publish_agent_placement_changed(&workspace_id, &agent_id)
                .await;
        }
    }
    Ok(())
}

/// Keep removed entry `id`'s key, hidden from every read, under a row the
/// reaper alone uses (it deletes both once E2B has none of this backend's
/// sandboxes of the entry left).
async fn retire_key(svc: &Services, id: &str) -> Result<()> {
    let Some(key) = config::saved_key(svc, id).await? else {
        return Ok(());
    };
    let retired = config::retired_id();
    let mut row = ProviderConfig::new(id);
    row.retired_from = Some(id.to_owned());
    svc.store_cloud_machine_key(&retired, key.expose()).await?;
    config::save(svc, &retired, &row).await?;
    tracing::info!(entry = %id, "cloud machines: E2B kept a sandbox of a removed entry; its key stays for the reaper");
    Ok(())
}

/// `-32602` `not-found` for a `machineId` that names no cloud machine.
pub(crate) fn not_found(machine_id: &str) -> Error {
    Error::remote_machine(
        false,
        format!("No cloud machine {machine_id}."),
        json!({ "code": "not-found" }),
    )
}

/// The cloud machine `params.machineId` names, once the caller may act on
/// it: the owner, or the principal that started it.
async fn own_machine(svc: &Services, params: &Value, what: &str) -> Result<ExposedMachine> {
    let machine_id = match params.get("machineId") {
        Some(Value::String(id)) if !id.trim().is_empty() && id.len() <= 256 => id.clone(),
        None | Some(Value::Null) => {
            return Err(Error::InvalidParams("machineId is required".into()))
        }
        Some(_) => {
            return Err(Error::InvalidParams(
                "machineId must be a non-blank string".into(),
            ))
        }
    };
    let row = svc
        .store()
        .get_exposed_machine(&machine_id)
        .await?
        .filter(ExposedMachine::is_cloud)
        .ok_or_else(|| not_found(&machine_id))?;
    if svc.caller_is_machine_owner().await {
        return Ok(row);
    }
    let starter = row
        .cloud
        .as_ref()
        .map(|cloud| cloud.started_by.principal_id.as_str())
        .unwrap_or_default();
    match intent_core::current_caller() {
        Some(intent_core::Caller::Wire { principal_id, .. })
            if !starter.is_empty() && principal_id.to_string() == starter =>
        {
            Ok(row)
        }
        _ => Err(Error::Forbidden(format!(
            "cloudMachines.{what} requires the owner or the member who started the machine"
        ))),
    }
}

async fn pause(feature: &RemoteFeature, svc: &Services, params: &Value) -> Result<Value> {
    let row = own_machine(svc, params, "pause").await?;
    let entry = row_entry(&row).to_owned();
    paused(feature, svc, &row)
        .await
        .map_err(|error| with_cloud_machine_id(error, &entry))
}

async fn paused(feature: &RemoteFeature, svc: &Services, row: &ExposedMachine) -> Result<Value> {
    let entry = row_entry(row);
    if let Some(saved) = config::load(svc, entry).await? {
        if saved.state == config::ProviderState::KeyRejected {
            return Err(config::key_rejected(None));
        }
    }
    let client = config::saved_client(svc, entry).await?;
    let paused = super::lifecycle::pause(svc, feature, &*client, &row.id).await?;
    let live = feature.running_node(&paused.id);
    Ok(json!({
        "machine": crate::methods::machine_json(&paused, live.as_ref().map(crate::RemoteNode::shared_ref)),
    }))
}

async fn kill(feature: &RemoteFeature, svc: &Services, params: &Value) -> Result<Value> {
    let row = own_machine(svc, params, "kill").await?;
    // Without a working key the records still go; the reaper deletes the
    // sandbox once a working key is saved.
    let client = config::saved_client(svc, row_entry(&row)).await.ok();
    let unconfirmed = super::start::delete_machine(feature, svc, &row, client.as_deref()).await;
    Ok(json!({ "ok": true, "unconfirmedStops": unconfirmed }))
}
