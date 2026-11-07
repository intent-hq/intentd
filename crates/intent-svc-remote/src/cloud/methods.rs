//! `cloudMachines.configure`, `list` and `remove`. The services already
//! checked the caller: the owner for `configure` and `remove`, the owner or a
//! shared-host member for `list`.

use serde_json::{json, Value};

use intent_core::machines::{CloudMachineSize, ExposedMachine, CLOUD_PROVIDER_E2B};
use intent_core::{now_iso, Error, Result, WorkspaceId};
use intent_nodes::e2b::SecretString;
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
        other => Err(Error::InvalidParams(format!(
            "unknown method cloudMachines.{other}"
        ))),
    }
}

fn provider_param(params: &Value) -> Result<()> {
    match params.get("provider") {
        Some(Value::String(provider)) if provider == CLOUD_PROVIDER_E2B => Ok(()),
        None | Some(Value::Null) => Err(Error::InvalidParams("provider is required".into())),
        Some(_) => Err(Error::InvalidParams(
            "provider must be \"e2b\", the only cloud machine provider".into(),
        )),
    }
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
    Ok(ConfigureParams {
        api_key,
        size,
        idle_pause_minutes: range_param(params, "idlePauseMinutes", 1, 1440)?,
        paused_delete_days: range_param(params, "pausedDeleteDays", 1, 30)?,
        template_project,
        validate_only: bool_param(params, "validateOnly")?,
    })
}

async fn configure(feature: &RemoteFeature, svc: &Services, params: &Value) -> Result<Value> {
    let params = configure_params(params)?;
    let _settings = feature.cloud.settings.lock().await;
    let saved = config::load(svc).await?;
    let mut next = saved.clone().unwrap_or_else(ProviderConfig::new);
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
    let key = match &params.api_key {
        Some(key) => Some(key.clone()),
        None => config::saved_key(svc).await?,
    };
    let Some(key) = key else {
        return Err(config::not_configured());
    };
    // The key and the template are tested before anything is saved. A key
    // is tested whenever one is given; the template whenever the call can
    // change it, or always with `validateOnly`.
    let check_template = params.validate_only
        || params.api_key.is_some()
        || params.size.is_some()
        || params.template_project.is_some()
        || saved.is_none();
    if params.api_key.is_some() || params.validate_only || check_template {
        let client = config::client(key.clone())?;
        if params.api_key.is_some() || params.validate_only {
            client
                .check_key()
                .await
                .map_err(|error| config::to_error(&error, config::Phase::Other))?;
        }
        if check_template {
            next.sizes = config::published_sizes(&client, &next.template_project).await?;
            next.sizes_checked_at = Some(now_iso());
            next.sizes_version = Some(config::intentd_version().to_owned());
            if !next.sizes.contains(&next.size) {
                return Err(config::template_missing(&next.template(next.size)));
            }
        }
        config::note_outcome(&mut next, None);
    }
    if params.validate_only {
        let mut provider = next.to_json();
        provider["state"] = json!("ready");
        return Ok(json!({ "provider": provider }));
    }
    if saved.is_none() {
        next.configured_at = now_iso();
    }
    if let Some(key) = &params.api_key {
        svc.store_cloud_machine_key(CLOUD_PROVIDER_E2B, key.expose())
            .await?;
    }
    config::save(svc, &next).await?;
    let provider = next.to_json();
    svc.publish_cloud_machine_changed(json!({ "provider": provider.clone() }))
        .await;
    Ok(json!({ "provider": provider }))
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
    if let Some(mut saved) = config::load(svc).await? {
        if saved.sizes_stale() {
            refresh_sizes(feature, svc, &mut saved).await;
        }
        providers.push(saved.to_json());
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

/// Check the published sizes again; a failed check keeps the last list. A
/// change of the list or the key's state is saved and announced.
async fn refresh_sizes(feature: &RemoteFeature, svc: &Services, saved: &mut ProviderConfig) {
    let Ok(_settings) = feature.cloud.settings.try_lock() else {
        return;
    };
    let checked = match config::saved_client(svc).await {
        Ok(client) => config::published_sizes(&client, &saved.template_project).await,
        Err(error) => Err(error),
    };
    let mut next = saved.clone();
    match &checked {
        Ok(sizes) => {
            next.sizes.clone_from(sizes);
            next.sizes_checked_at = Some(now_iso());
            next.sizes_version = Some(config::intentd_version().to_owned());
            config::note_outcome(&mut next, None);
        }
        Err(error) => config::note_outcome(&mut next, Some(error)),
    }
    let announce = next.sizes != saved.sizes || next.state != saved.state;
    if let Err(error) = config::save(svc, &next).await {
        tracing::warn!(%error, "cloud machines: could not save the checked sizes");
        return;
    }
    *saved = next;
    if announce {
        svc.publish_cloud_machine_changed(json!({ "provider": saved.to_json() }))
            .await;
    }
}

async fn remove(feature: &RemoteFeature, svc: &Services, params: &Value) -> Result<Value> {
    provider_param(params)?;
    let stop_agents = bool_param(params, "stopAgents")?;
    let _settings = feature.cloud.settings.lock().await;
    if config::load(svc).await?.is_none() {
        return Ok(json!({ "ok": true, "unconfirmedStops": [] }));
    }
    let rows = svc.store().list_cloud_machines().await?;
    if !rows.is_empty() && !stop_agents {
        let running: usize = rows
            .iter()
            .map(|row| {
                feature
                    .running_node(&row.id)
                    .map_or(0, |node| node.running())
            })
            .sum();
        return Err(crate::methods::in_use("E2B", running.max(rows.len())));
    }
    let client = config::saved_client(svc).await.ok();
    let mut unconfirmed = Vec::new();
    for row in rows {
        unconfirmed.extend(super::start::delete_machine(feature, svc, &row, client.as_ref()).await);
    }
    svc.store()
        .delete_cloud_machine_provider(CLOUD_PROVIDER_E2B)
        .await?;
    svc.delete_cloud_machine_key(CLOUD_PROVIDER_E2B).await?;
    svc.publish_cloud_machine_changed(json!({ "providerId": CLOUD_PROVIDER_E2B, "removed": true }))
        .await;
    Ok(json!({ "ok": true, "unconfirmedStops": unconfirmed }))
}
