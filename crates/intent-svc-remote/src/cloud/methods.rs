//! `cloudMachines.configure`, `list`, `remove`, `pause` and `kill`. The
//! services already checked the caller: the owner for `configure` and
//! `remove`, the owner or a shared-host member for the others; `pause` and
//! `kill` then admit only the owner and the machine's starter.

use serde_json::{json, Value};

use intent_core::machines::{
    with_cloud_machine_id, MAX_CLOUD_MACHINE_ENTRIES, MAX_CLOUD_MACHINE_ID_BYTES,
};
use intent_core::machines::{
    CloudMachineSize, ExposedMachine, CLOUD_PROVIDER_AGENT37, CLOUD_PROVIDER_E2B,
    CLOUD_PROVIDER_KINDS, CLOUD_PROVIDER_NORTHFLANK, CLOUD_PROVIDER_VERCEL,
};
use intent_core::{now_iso, Error, Result, WorkspaceId};
use intent_nodes::cloud::SecretString;
use intent_nodes::northflank::MIN_DISK_GIB;
use intent_services::feature_api::Services;

use super::config::{self, ProviderConfig};
use crate::RemoteFeature;

/// Longest accepted API key, in bytes: Northflank tokens are JWTs well over
/// 1 KB, and 8 KiB is the usual cap on one HTTP header that carries the key.
const MAX_KEY_BYTES: usize = 8 * 1024;
/// Longest accepted template project slug, in bytes.
const MAX_PROJECT_BYTES: usize = 64;
/// Longest accepted Northflank image repository and plan id, in bytes.
const MAX_IMAGE_BYTES: usize = 256;
const MAX_PLAN_BYTES: usize = 64;

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

/// `provider`, optional: a `CloudProviderKind` this backend can configure.
fn provider_param(params: &Value) -> Result<Option<String>> {
    match params.get("provider") {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(provider)) if CLOUD_PROVIDER_KINDS.contains(&provider.as_str()) => {
            Ok(Some(provider.clone()))
        }
        Some(_) => Err(Error::InvalidParams(
            "provider must be \"e2b\", \"northflank\", \"agent37\" or \"vercel\"".into(),
        )),
    }
}

/// A Northflank id: 3-100 bytes of letters, digits and single hyphens.
fn northflank_id(id: &str) -> bool {
    (3..=100).contains(&id.len())
        && id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
        && !id.starts_with('-')
        && !id.ends_with('-')
        && !id.contains("--")
}

/// An image repository without tag or digest: `[host[:port]/]path`.
fn image_repository(image: &str) -> bool {
    if image.is_empty() || image.len() > MAX_IMAGE_BYTES || image.contains('@') {
        return false;
    }
    let mut parts = image.split('/');
    let first = parts.next().unwrap_or_default();
    let rest: Vec<&str> = parts.collect();
    let path_part = |part: &str| {
        !part.is_empty()
            && part.bytes().all(|b| {
                b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'.' | b'_' | b'-')
            })
    };
    let host = |part: &str| {
        !part.is_empty()
            && part
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-' | b':'))
    };
    if rest.is_empty() {
        return path_part(first);
    }
    let first_ok = if first.contains('.') || first.contains(':') || first == "localhost" {
        host(first)
    } else {
        path_part(first)
    };
    first_ok && rest.iter().all(|part| path_part(part))
}

/// The Northflank-only `configure` members (`diskGiB` is shared with
/// Agent37: [`disk_param`]).
#[derive(Default)]
struct NorthflankParams {
    project_id: Option<String>,
    plans: Option<Vec<(CloudMachineSize, String)>>,
    image: Option<String>,
}

impl NorthflankParams {
    fn any(&self) -> bool {
        self.project_id.is_some() || self.plans.is_some() || self.image.is_some()
    }
}

/// The Agent37-only `configure` members (`diskGiB` is shared with
/// Northflank).
#[derive(Default)]
struct Agent37Params {
    instance_type: Option<String>,
    region: Option<String>,
}

impl Agent37Params {
    fn any(&self) -> bool {
        self.instance_type.is_some() || self.region.is_some()
    }
}

fn agent37_params(params: &Value) -> Result<Agent37Params> {
    let pick = |name: &str, allowed: &[&str]| match params.get(name) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) if allowed.contains(&value.as_str()) => Ok(Some(value.clone())),
        Some(_) => Err(Error::InvalidParams(format!(
            "{name} must be one of {}",
            allowed
                .iter()
                .map(|v| format!("\"{v}\""))
                .collect::<Vec<_>>()
                .join(", ")
        ))),
    };
    Ok(Agent37Params {
        instance_type: pick("instanceType", &intent_nodes::agent37::INSTANCE_TYPES)?,
        region: pick("region", &intent_nodes::agent37::REGIONS)?,
    })
}

/// The Vercel-only `configure` members (`projectId` and `image` are shared
/// with Northflank, with Vercel's own rules).
#[derive(Default)]
struct VercelParams {
    team_id: Option<String>,
    project_id: Option<String>,
    image: Option<String>,
}

impl VercelParams {
    fn scope_given(&self) -> bool {
        self.team_id.is_some() || self.project_id.is_some()
    }
}

fn vercel_params(params: &Value) -> Result<VercelParams> {
    let text = |name: &str, valid: fn(&str) -> bool, rule: &str| match params.get(name) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) if valid(value) => Ok(Some(value.clone())),
        Some(_) => Err(Error::InvalidParams(format!("{name} must be {rule}"))),
    };
    let image = match params.get("image") {
        Some(Value::String(image)) if image.is_empty() => {
            Some(intent_nodes::vercel::DEFAULT_IMAGE.to_owned())
        }
        _ => text(
            "image",
            intent_nodes::vercel::valid_image,
            "a Vercel Container Registry repository without a tag: <team>/<project>/<repository>, or <repository>",
        )?,
    };
    Ok(VercelParams {
        team_id: text(
            "teamId",
            intent_nodes::vercel::valid_team_id,
            "a Vercel team id: team_ and then 1-64 letters or digits",
        )?,
        project_id: text(
            "projectId",
            intent_nodes::vercel::valid_project_id,
            "a Vercel project id: prj_ and then 1-64 letters or digits",
        )?,
        image,
    })
}

fn northflank_params(params: &Value) -> Result<NorthflankParams> {
    let project_id = match params.get("projectId") {
        None | Some(Value::Null) => None,
        Some(Value::String(id)) if northflank_id(id) => Some(id.clone()),
        Some(_) => {
            return Err(Error::InvalidParams(
                "projectId must be a Northflank id: 3-100 letters, digits and single hyphens"
                    .into(),
            ))
        }
    };
    let plans = match params.get("plans") {
        None | Some(Value::Null) => None,
        Some(Value::Object(map)) => {
            let mut plans = Vec::new();
            for (size, plan) in map {
                let size = CloudMachineSize::parse(size)
                    .filter(|size| CloudMachineSize::NORTHFLANK.contains(size))
                    .ok_or_else(|| {
                        Error::InvalidParams(format!(
                            "plans: Northflank entries offer the sizes \"2x4\", \"4x8\" and \"8x16\", not \"{size}\""
                        ))
                    })?;
                let Some(plan) = plan
                    .as_str()
                    .filter(|plan| plan.len() <= MAX_PLAN_BYTES && northflank_id(plan))
                else {
                    return Err(Error::InvalidParams(format!(
                        "plans.{}: a Northflank deployment plan id",
                        size.id()
                    )));
                };
                plans.push((size, plan.to_owned()));
            }
            Some(plans)
        }
        Some(_) => {
            return Err(Error::InvalidParams(
                "plans must be an object from size to plan id".into(),
            ))
        }
    };
    let image = match params.get("image") {
        None | Some(Value::Null) => None,
        Some(Value::String(image)) if image_repository(image) => Some(image.clone()),
        Some(_) => {
            return Err(Error::InvalidParams(
                "image must be an image repository without a tag or digest".into(),
            ))
        }
    };
    Ok(NorthflankParams {
        project_id,
        plans,
        image,
    })
}

/// `diskGiB` for an entry of provider `kind`: a Northflank volume is
/// 6-500 GiB, an Agent37 disk 4-100 GiB, and E2B takes none.
fn disk_param(params: &Value, kind: &str) -> Result<Option<u32>> {
    if params.get("diskGiB").is_none_or(Value::is_null) {
        return Ok(None);
    }
    match kind {
        CLOUD_PROVIDER_NORTHFLANK => range_param(params, "diskGiB", MIN_DISK_GIB, 500).map_err(|_| {
            Error::InvalidParams(format!(
                "diskGiB must be an integer {MIN_DISK_GIB}-500: a Northflank volume is at least {MIN_DISK_GIB} GiB"
            ))
        }),
        CLOUD_PROVIDER_AGENT37 => range_param(params, "diskGiB", 4, 100),
        _ => Err(Error::InvalidParams(format!(
            "diskGiB is a Northflank or Agent37 setting; a{} {} entry has none",
            if kind == CLOUD_PROVIDER_E2B { "n" } else { "" },
            intent_core::machines::cloud_provider_name(kind)
        ))),
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
    provider: Option<String>,
    agent37: Agent37Params,
    /// The members whose rules depend on the entry's provider, as sent
    /// (`projectId`, `plans`, `image`, `teamId`, `diskGiB`): checked once
    /// the provider is known.
    raw: Value,
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
    let provider = provider_param(params)?;
    let agent37 = agent37_params(params)?;
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
            Error::InvalidParams(
                "size must be one of \"2x4\", \"4x8\", \"8x16\", \"16x32\", \"4x16\", \"4x32\""
                    .into(),
            )
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
    let raw = ["projectId", "plans", "image", "teamId", "diskGiB"]
        .into_iter()
        .map(|name| {
            (
                name.to_owned(),
                params.get(name).cloned().unwrap_or(Value::Null),
            )
        })
        .collect();
    Ok(ConfigureParams {
        provider,
        agent37,
        raw: Value::Object(raw),
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
            if params
                .provider
                .as_ref()
                .is_some_and(|provider| *provider != saved.provider)
            {
                return Err(Error::InvalidParams(format!(
                    "the entry {id} is a {} entry; its provider never changes",
                    saved.provider_name()
                )));
            }
            Ok((Some(id.clone()), Some(saved)))
        }
        (None, Some(_)) => Ok((None, None)),
        (None, None)
            if params
                .provider
                .as_deref()
                .is_some_and(|provider| provider != CLOUD_PROVIDER_E2B) =>
        {
            Err(Error::InvalidParams(format!(
                "a new {} entry needs a name",
                intent_core::machines::cloud_provider_name(
                    params.provider.as_deref().unwrap_or_default()
                )
            )))
        }
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
    let kind = saved.as_ref().map_or_else(
        || {
            params
                .provider
                .clone()
                .unwrap_or_else(|| CLOUD_PROVIDER_E2B.to_owned())
        },
        |saved| saved.provider.clone(),
    );
    let northflank = kind == CLOUD_PROVIDER_NORTHFLANK;
    let agent37 = kind == CLOUD_PROVIDER_AGENT37;
    let vercel = kind == CLOUD_PROVIDER_VERCEL;
    let given = |name: &str| params.raw.get(name).is_some_and(|v| !v.is_null());
    let nf = if northflank {
        northflank_params(&params.raw)?
    } else {
        NorthflankParams::default()
    };
    let vc = if vercel {
        vercel_params(&params.raw)?
    } else {
        VercelParams::default()
    };
    let disk_gib = disk_param(&params.raw, &kind)?;
    let tag = |error: Error| {
        intent_core::machines::with_cloud_provider(
            with_cloud_machine_id(error, id.as_deref().unwrap_or_default()),
            &kind,
        )
    };
    let provider_name = intent_core::machines::cloud_provider_name(&kind);
    if kind != CLOUD_PROVIDER_E2B && params.template_project.is_some() {
        return Err(Error::InvalidParams(format!(
            "templateProject is an E2B setting; a {provider_name} entry has none"
        )));
    }
    if !northflank && given("plans") {
        return Err(Error::InvalidParams(format!(
            "plans is a Northflank setting; a {provider_name} entry has none"
        )));
    }
    if !northflank && !vercel && (given("projectId") || given("image")) {
        return Err(Error::InvalidParams(format!(
            "projectId and image are Northflank or Vercel settings; a {provider_name} entry has none"
        )));
    }
    if !vercel && given("teamId") {
        return Err(Error::InvalidParams(format!(
            "teamId is a Vercel setting; a {provider_name} entry has none"
        )));
    }
    if !agent37 && params.agent37.any() {
        return Err(Error::InvalidParams(format!(
            "instanceType and region are Agent37 settings; a {provider_name} entry has none"
        )));
    }
    let offered: &[CloudMachineSize] = match kind.as_str() {
        CLOUD_PROVIDER_NORTHFLANK => &CloudMachineSize::NORTHFLANK,
        CLOUD_PROVIDER_AGENT37 => &CloudMachineSize::AGENT37,
        CLOUD_PROVIDER_VERCEL => &CloudMachineSize::VERCEL,
        _ => &CloudMachineSize::E2B,
    };
    if let Some(size) = params.size.filter(|size| !offered.contains(size)) {
        return Err(Error::InvalidParams(format!(
            "{provider_name} entries offer the sizes {}, not \"{}\"",
            offered
                .iter()
                .map(|s| format!("\"{}\"", s.id()))
                .collect::<Vec<_>>()
                .join(", "),
            size.id()
        )));
    }
    let creating = saved.is_none();
    if creating && northflank && nf.project_id.is_none() {
        return Err(Error::InvalidParams(
            "a new Northflank entry needs projectId".into(),
        ));
    }
    if creating && vercel && (vc.team_id.is_none() || vc.project_id.is_none()) {
        return Err(Error::InvalidParams(
            "a new Vercel entry needs teamId and projectId".into(),
        ));
    }
    if creating && entries.len() >= MAX_CLOUD_MACHINE_ENTRIES {
        return Err(Error::InvalidParams(format!(
            "at most {MAX_CLOUD_MACHINE_ENTRIES} cloud machine entries can be set up"
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
            "an entry named \"{name}\" already exists"
        )));
    }
    let copied = match &params.api_key_from {
        Some(from) if !entries.iter().any(|(entry, _)| entry == from) => {
            return Err(entry_not_found(from, ""));
        }
        Some(from) if Some(from) == id.as_ref() => {
            return Err(Error::InvalidParams(
                "apiKeyFrom must name another entry".into(),
            ))
        }
        Some(from)
            if entries
                .iter()
                .any(|(entry, other)| entry == from && other.provider != kind) =>
        {
            return Err(Error::InvalidParams(format!(
                "apiKeyFrom must name another {} entry",
                intent_core::machines::cloud_provider_name(&kind)
            )))
        }
        Some(from) => Some(
            config::saved_key(svc, from)
                .await?
                .ok_or_else(|| entry_not_found(from, " with a saved key"))?,
        ),
        None => None,
    };
    let mut next = saved.clone().unwrap_or_else(|| {
        if northflank {
            ProviderConfig::new_northflank(&name, nf.project_id.as_deref().unwrap_or_default())
        } else if vercel {
            ProviderConfig::new_vercel(
                &name,
                vc.team_id.as_deref().unwrap_or_default(),
                vc.project_id.as_deref().unwrap_or_default(),
            )
        } else if agent37 {
            ProviderConfig::new_agent37(&name)
        } else {
            ProviderConfig::new(&name)
        }
    });
    next.name.clone_from(&name);
    let moved = saved.as_ref().is_some_and(|saved| {
        let nf_moved = saved
            .northflank
            .as_ref()
            .zip(nf.project_id.as_ref())
            .is_some_and(|(old, project)| old.project_id != *project);
        let vc_moved = saved.vercel.as_ref().is_some_and(|old| {
            vc.team_id.as_ref().is_some_and(|team| old.team_id != *team)
                || vc
                    .project_id
                    .as_ref()
                    .is_some_and(|project| old.project_id != *project)
        });
        nf_moved || vc_moved
    });
    if let (Some(id), true) = (&id, moved) {
        let rows = entry_rows(svc, id).await?;
        let starting = feature.cloud.starts_in_flight(id);
        if !rows.is_empty() || !starting.is_empty() {
            return Err(crate::methods::in_use(&name, rows.len() + starting.len()));
        }
    }
    if let Some(saved_nf) = next.northflank.as_mut() {
        if let Some(project) = &nf.project_id {
            saved_nf.project_id.clone_from(project);
        }
        if let Some(plans) = &nf.plans {
            for (size, plan) in plans {
                saved_nf.plans.insert(size.id().to_owned(), plan.clone());
            }
        }
        if let Some(image) = &nf.image {
            saved_nf.image.clone_from(image);
        }
        if let Some(disk) = disk_gib {
            saved_nf.disk_gib = disk;
        }
    }
    if let Some(saved_vc) = next.vercel.as_mut() {
        if let Some(team) = &vc.team_id {
            saved_vc.team_id.clone_from(team);
        }
        if let Some(project) = &vc.project_id {
            saved_vc.project_id.clone_from(project);
        }
        if let Some(image) = &vc.image {
            saved_vc.image.clone_from(image);
        }
    }
    if let Some(a37) = next.agent37.as_mut() {
        if let Some(instance_type) = &params.agent37.instance_type {
            a37.instance_type.clone_from(instance_type);
        }
        if let Some(region) = &params.agent37.region {
            a37.region.clone_from(region);
        }
        if let Some(disk) = disk_gib {
            a37.disk_gib = disk;
        }
        if !intent_nodes::agent37::region_allows(&a37.instance_type, &a37.region) {
            return Err(Error::InvalidParams(format!(
                "Agent37 runs {} instances only in \"us\"; region \"{}\" needs instanceType \"vm\"",
                a37.instance_type, a37.region
            )));
        }
    }
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
    if northflank {
        let check = creating || params.validate_only || given.is_some() || nf.any();
        if check {
            check_northflank(&mut next, key.clone())
                .await
                .map_err(tag)?;
            config::note_outcome(&mut next, None);
        }
    } else if agent37 {
        if creating || params.validate_only || given.is_some() {
            check_agent37(key.clone()).await.map_err(tag)?;
            config::note_outcome(&mut next, None);
        }
    } else if vercel {
        if creating
            || params.validate_only
            || given.is_some()
            || vc.scope_given()
            || vc.image.is_some()
        {
            check_vercel(&next, key.clone()).await.map_err(tag)?;
            config::note_outcome(&mut next, None);
        }
    } else if given.is_some() || params.validate_only {
        config::client(&next, key.clone())
            .map_err(tag)?
            .check_key()
            .await
            .map_err(|error| tag(config::to_error(&error, config::Phase::Other, &kind)))?;
        config::note_outcome(&mut next, None);
    }
    let check_template = kind == CLOUD_PROVIDER_E2B
        && (params.validate_only
            || given.is_some()
            || params.size.is_some()
            || params.template_project.is_some()
            || creating);
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
        None => config::new_cloud_machine_id(svc, &kind).await?,
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
    let renamed = saved.as_ref().is_some_and(|saved| saved.name != next.name);
    if let Some(saved) = saved.filter(|saved| saved.name != next.name) {
        rename(feature, svc, &id, &saved.name, &next.name).await;
    }
    if creating || renamed {
        crate::order::publish(svc).await;
    }
    Ok(json!({ "provider": provider }))
}

/// The tests a Northflank entry passes before it is saved, in order: the
/// project (reading its region), each size's plan (it exists with the
/// size's vCPU and memory), the image tag, and the service list the reaper
/// needs.
async fn check_northflank(next: &mut ProviderConfig, key: SecretString) -> Result<()> {
    let Some(nf) = next.northflank.as_mut() else {
        return Ok(());
    };
    let kind = CLOUD_PROVIDER_NORTHFLANK;
    let client = config::northflank_client(nf, key)?;
    let to_error = |error: intent_nodes::cloud::CloudError| {
        config::to_error(&error, config::Phase::Other, kind)
    };
    let project = client.project().await.map_err(to_error)?;
    nf.region = project.region;
    let plans = client.plans().await.map_err(to_error)?;
    for size in CloudMachineSize::NORTHFLANK {
        let Some(id) = nf.plan(size) else { continue };
        let fits = plans.iter().any(|plan| {
            plan.id == id
                && (plan.cpu - f64::from(size.vcpu())).abs() < 0.01
                && plan.memory_mib == size.memory_mib()
        });
        if !fits {
            return Err(Error::InvalidParams(format!(
                "plans.{}: Northflank has no deployment plan {id} with {} vCPU and {} GiB",
                size.id(),
                size.vcpu(),
                size.memory_mib() / 1024
            )));
        }
    }
    let image = format!("{}:{}", nf.image, config::intentd_version());
    if !client.image_exists(&image).await.map_err(to_error)? {
        return Err(config::image_missing(&image));
    }
    client.check_services().await.map_err(to_error)?;
    Ok(())
}

/// The tests an Agent37 entry passes before it is saved, in order: the
/// template for this backend's version (imported when missing, which also
/// tests the key), then the instance list the reaper needs.
async fn check_agent37(key: SecretString) -> Result<()> {
    use intent_nodes::cloud::CloudProvider as _;
    let kind = CLOUD_PROVIDER_AGENT37;
    let client = config::agent37_client(key)?;
    let to_error = |error: intent_nodes::cloud::CloudError| {
        config::to_error(&error, config::Phase::Other, kind)
    };
    let image = intent_nodes::agent37::image_ref(config::intentd_version());
    if !client.image_ready(&image).await.map_err(to_error)? {
        return Err(config::agent37_image_missing(&image));
    }
    client.check_key().await.map_err(to_error)?;
    Ok(())
}

/// The tests a Vercel entry passes before it is saved, in order: the
/// sandbox list of its project, which tests the token, the team and the
/// project and is what the reaper needs, then the image tag.
async fn check_vercel(next: &ProviderConfig, key: SecretString) -> Result<()> {
    use intent_nodes::cloud::CloudProvider as _;
    let Some(settings) = next.vercel.as_ref() else {
        return Ok(());
    };
    let kind = CLOUD_PROVIDER_VERCEL;
    let client = config::vercel_client(settings, key)?;
    let to_error = |error: intent_nodes::cloud::CloudError| {
        config::to_error(&error, config::Phase::Other, kind)
    };
    client.check_key().await.map_err(to_error)?;
    let image = intent_nodes::vercel::image_ref(&settings.image, config::intentd_version());
    if !client.image_ready(&image).await.map_err(to_error)? {
        return Err(config::vercel_image_missing(&image));
    }
    Ok(())
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
    Ok(json!({
        "providers": providers,
        "machines": machines,
        "providerKinds": CLOUD_PROVIDER_KINDS,
    }))
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
    if next.is_e2b() {
        next.set_sizes(config::published_sizes(&saved.template_project).await);
    } else {
        next.sizes_checked_at = Some(now_iso());
        next.sizes_version = Some(config::intentd_version().to_owned());
    }
    // The key too, so one revoked at E2B shows as rejected and placement
    // refuses before any sandbox.
    match config::saved_client(svc, id).await {
        Ok(client) => match client.check_key().await {
            Ok(()) => config::note_outcome(&mut next, None),
            Err(error) => config::note_outcome(
                &mut next,
                Some(&config::to_error(
                    &error,
                    config::Phase::Other,
                    client.kind(),
                )),
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
    let provider = provider_param(params)?;
    let id =
        entry_param(params, "cloudMachineId")?.unwrap_or_else(|| config::legacy_entry().to_owned());
    let stop_agents = bool_param(params, "stopAgents")?;
    let _settings = feature.cloud.settings.lock().await;
    // A removed entry's key kept for the reaper names no entry.
    if config::is_retired(svc, &id).await? {
        return Ok(json!({ "ok": true, "unconfirmedStops": [] }));
    }
    let saved = config::load(svc, &id).await?;
    if let (Some(provider), Some(saved)) = (&provider, &saved) {
        if *provider != saved.provider {
            return Err(Error::InvalidParams(format!(
                "the entry {id} is a {} entry, not {provider}",
                saved.provider_name()
            )));
        }
    }
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
        retire_key(svc, &id, saved.as_ref()).await?;
    }
    svc.delete_cloud_machine_key(&id).await?;
    svc.publish_cloud_machine_changed(json!({ "providerId": id, "removed": true }), None)
        .await;
    crate::order::forget(
        svc,
        &intent_core::machine_order::MachineOrderKey::Cloud {
            cloud_machine_id: id.clone(),
        },
    )
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
async fn retire_key(svc: &Services, id: &str, saved: Option<&ProviderConfig>) -> Result<()> {
    let Some(key) = config::saved_key(svc, id).await? else {
        return Ok(());
    };
    let retired = config::retired_id();
    let mut row = saved.cloned().unwrap_or_else(|| ProviderConfig::new(id));
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
/// it: the owner, the principal that started it, or the current owner of
/// every workspace whose home it holds (nodes.md "Cloud homes" 2).
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
    let Some(intent_core::Caller::Wire { principal_id, .. }) = intent_core::current_caller() else {
        return Err(refused(what));
    };
    if !starter.is_empty() && principal_id.to_string() == starter {
        return Ok(row);
    }
    // The owner of every workspace whose home the machine holds.
    let homes = svc.store().homes_on_machine(&row.id).await?;
    let mut owns_all = !homes.is_empty();
    for (workspace_id, _) in &homes {
        let owner = svc
            .store()
            .get_workspace_owner_principal_id(workspace_id)
            .await?;
        owns_all &= owner.as_ref() == Some(&principal_id);
    }
    if owns_all {
        return Ok(row);
    }
    Err(refused(what))
}

fn refused(what: &str) -> Error {
    Error::Forbidden(format!(
        "cloudMachines.{what} requires the owner or the member who started the machine"
    ))
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
            return Err(config::key_rejected_by(&saved.provider, None));
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
