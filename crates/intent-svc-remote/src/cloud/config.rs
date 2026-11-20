//! The saved settings of each named cloud machine entry (E2B, Northflank,
//! Agent37 or Vercel), its provider client and the mapping of provider failures
//! onto the documented `cloud-*` errors.
//!
//! Every entry is one `cloud_machine_provider` row (its id is the entry id)
//! with its own key in the secret store under `cloudMachines.<id>.apiKey`.
//! A single-provider setup saved before entries existed is the entry
//! [`CLOUD_E2B_LEGACY_ENTRY`]: its row and key account already have that
//! id, and its settings read with the name [`CLOUD_PROVIDER_E2B_NAME`].

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use std::collections::BTreeMap;

use intent_core::machines::{
    cloud_machine_unavailable, cloud_provider_name, cloud_quota, with_cloud_machine_id,
    with_cloud_provider, CloudMachineSize, CLOUD_E2B_LEGACY_ENTRY, CLOUD_PROVIDER_AGENT37,
    CLOUD_PROVIDER_E2B, CLOUD_PROVIDER_E2B_NAME, CLOUD_PROVIDER_NORTHFLANK, CLOUD_PROVIDER_VERCEL,
};
use intent_core::{now_iso, Error, Result};
use intent_nodes::agent37::{Agent37Client, Agent37Config};
use intent_nodes::cloud::{CloudError, CloudErrorKind, CloudProvider, QuotaLimit, SecretString};
use intent_nodes::e2b::{E2bClient, E2bConfig};
use intent_nodes::northflank::{NorthflankClient, NorthflankConfig, MIN_DISK_GIB};
use intent_nodes::vercel::{VercelClient, VercelConfig};
use intent_services::feature_api::Services;

/// The E2B project that publishes the intent-node templates (Intent's).
pub(crate) const DEFAULT_TEMPLATE_PROJECT: &str = "intent";

/// The base providers whose CLIs the intent-node templates install.
const TEMPLATE_AGENT_PROVIDERS: &[&str] = &["claude-code", "codex"];

/// How old the published-size list may get before `cloudMachines.list`
/// checks the tags again.
const SIZES_MAX_AGE: time::Duration = time::Duration::hours(1);

/// Test and development seams, read from this process's environment: the
/// E2B API and envd base URLs, and the base providers a test template
/// installs (comma list).
const API_URL_ENV: &str = "INTENTD_E2B_API_URL";
const ENVD_URL_ENV: &str = "INTENTD_E2B_ENVD_URL";
const AGENT_PROVIDERS_ENV: &str = "INTENTD_E2B_AGENT_PROVIDERS";
/// Where releases publish their template manifest (test seam).
const MANIFEST_BASE_URL_ENV: &str = "INTENTD_E2B_MANIFEST_BASE_URL";
/// The Northflank REST and command-exec base URLs, and the registry the
/// image check asks (test seams).
const NORTHFLANK_API_URL_ENV: &str = "INTENTD_NORTHFLANK_API_URL";
const NORTHFLANK_EXEC_URL_ENV: &str = "INTENTD_NORTHFLANK_EXEC_URL";
const NORTHFLANK_REGISTRY_URL_ENV: &str = "INTENTD_NORTHFLANK_REGISTRY_URL";
/// The Agent37 REST and exec base URLs (test seams).
const AGENT37_API_URL_ENV: &str = "INTENTD_AGENT37_API_URL";
const AGENT37_EXEC_URL_ENV: &str = "INTENTD_AGENT37_EXEC_URL";
/// The Vercel REST and container registry base URLs (test seams).
const VERCEL_API_URL_ENV: &str = "INTENTD_VERCEL_API_URL";
const VERCEL_REGISTRY_URL_ENV: &str = "INTENTD_VERCEL_REGISTRY_URL";

/// The image Northflank entries start unless the owner sets another.
pub(crate) const DEFAULT_NORTHFLANK_IMAGE: &str = "ghcr.io/intent-hq/intent-node";
/// The volume size of a new Northflank service unless the owner sets one.
pub(crate) const DEFAULT_DISK_GIB: u32 = 20;

/// Longest wait for a release's template manifest.
const MANIFEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// The sizes every release builds (E2B Hobby limits), offered when the
/// release's manifest cannot be read.
const FALLBACK_SIZES: [CloudMachineSize; 2] = [CloudMachineSize::S2x4, CloudMachineSize::S4x8];

/// The result of the last E2B call with the saved key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum ProviderState {
    #[default]
    Ready,
    KeyRejected,
    Unreachable,
}

/// Longest entry name, in UTF-8 bytes.
pub(crate) const MAX_ENTRY_NAME_BYTES: usize = 64;

fn legacy_name() -> String {
    CLOUD_PROVIDER_E2B_NAME.to_owned()
}

fn e2b_kind() -> String {
    CLOUD_PROVIDER_E2B.to_owned()
}

/// A Northflank entry's own settings (`CloudMachineProvider.northflank`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct NorthflankSettings {
    pub(crate) project_id: String,
    /// Read from the project; never a parameter.
    #[serde(default)]
    pub(crate) region: String,
    /// Repository without a tag; the backend appends its version.
    pub(crate) image: String,
    /// The plans the owner set, by size id; other sizes use their default.
    #[serde(default)]
    pub(crate) plans: BTreeMap<String, String>,
    pub(crate) disk_gib: u32,
}

impl NorthflankSettings {
    pub(crate) fn new(project_id: &str) -> Self {
        Self {
            project_id: project_id.to_owned(),
            region: String::new(),
            image: DEFAULT_NORTHFLANK_IMAGE.to_owned(),
            plans: BTreeMap::new(),
            disk_gib: DEFAULT_DISK_GIB,
        }
    }

    /// The deployment plan of `size`, `None` for a size Northflank entries
    /// do not offer.
    pub(crate) fn plan(&self, size: CloudMachineSize) -> Option<String> {
        self.plans
            .get(size.id())
            .cloned()
            .or_else(|| size.default_deployment_plan().map(str::to_owned))
    }

    /// Every offered size's plan, as `plans` on the wire.
    fn plans_json(&self) -> Value {
        Value::Object(
            CloudMachineSize::NORTHFLANK
                .into_iter()
                .filter_map(|size| Some((size.id().to_owned(), json!(self.plan(size)?))))
                .collect(),
        )
    }
}

/// The disk of a new Agent37 instance unless the owner sets one.
pub(crate) const DEFAULT_AGENT37_DISK_GIB: u32 = 20;

/// An Agent37 entry's own settings (`CloudMachineProvider.agent37`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Agent37Settings {
    pub(crate) instance_type: String,
    pub(crate) region: String,
    pub(crate) disk_gib: u32,
}

impl Default for Agent37Settings {
    fn default() -> Self {
        Self {
            instance_type: "default".to_owned(),
            region: "us".to_owned(),
            disk_gib: DEFAULT_AGENT37_DISK_GIB,
        }
    }
}

impl Agent37Settings {
    fn vm(&self) -> bool {
        self.instance_type == "vm"
    }

    /// The disk a new instance of `size` gets: the setting, capped at the
    /// shape's largest disk.
    pub(crate) fn disk_for(&self, size: CloudMachineSize) -> u32 {
        size.agent37_max_disk_gib(self.vm())
            .map_or(self.disk_gib, |max| self.disk_gib.min(max))
    }
}

/// A Vercel entry's own settings (`CloudMachineProvider.vercel`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct VercelSettings {
    pub(crate) team_id: String,
    pub(crate) project_id: String,
    /// Repository without a tag; the backend appends its version.
    pub(crate) image: String,
}

impl VercelSettings {
    pub(crate) fn new(team_id: &str, project_id: &str) -> Self {
        Self {
            team_id: team_id.to_owned(),
            project_id: project_id.to_owned(),
            image: intent_nodes::vercel::DEFAULT_IMAGE.to_owned(),
        }
    }
}

/// The saved settings of one entry (`cloud_machine_provider.config_json`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ProviderConfig {
    /// The cloud provider (`CloudProviderKind`); rows saved before
    /// Northflank read as E2B. It never changes.
    #[serde(default = "e2b_kind")]
    pub(crate) provider: String,
    /// Northflank entries only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) northflank: Option<NorthflankSettings>,
    /// Agent37 entries only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) agent37: Option<Agent37Settings>,
    /// Vercel entries only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) vercel: Option<VercelSettings>,
    /// The entry's name; settings saved before entries existed read as
    /// "E2B".
    #[serde(default = "legacy_name")]
    pub(crate) name: String,
    /// Set on the row of a removed entry whose key is kept only so the
    /// reaper can delete sandboxes E2B would not delete at removal: the
    /// removed entry's id. Such a row is no entry.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) retired_from: Option<String>,
    /// Whether the row was saved as an entry. `false` only on a single
    /// provider saved before entries existed, until [`migrate`] ran.
    #[serde(default)]
    pub(crate) migrated: bool,
    pub(crate) template_project: String,
    pub(crate) size: CloudMachineSize,
    pub(crate) idle_pause_minutes: u32,
    pub(crate) paused_delete_days: u32,
    pub(crate) configured_at: String,
    #[serde(default)]
    pub(crate) checked_at: Option<String>,
    #[serde(default)]
    pub(crate) state: ProviderState,
    /// Sizes whose template is tagged with this backend's version.
    #[serde(default)]
    pub(crate) sizes: Vec<CloudMachineSize>,
    #[serde(default)]
    pub(crate) sizes_checked_at: Option<String>,
    /// The intentd version `sizes` was checked for.
    #[serde(default)]
    pub(crate) sizes_version: Option<String>,
    /// Whether `sizes` came from the release's manifest; `false` for the
    /// fallback list, which does not refuse other sizes.
    #[serde(default)]
    pub(crate) sizes_verified: bool,
}

impl ProviderConfig {
    pub(crate) fn new(name: &str) -> Self {
        Self {
            provider: e2b_kind(),
            northflank: None,
            agent37: None,
            vercel: None,
            name: name.to_owned(),
            retired_from: None,
            migrated: true,
            template_project: DEFAULT_TEMPLATE_PROJECT.to_owned(),
            size: CloudMachineSize::DEFAULT,
            idle_pause_minutes: 10,
            paused_delete_days: 7,
            configured_at: now_iso(),
            checked_at: None,
            state: ProviderState::Ready,
            sizes: Vec::new(),
            sizes_checked_at: None,
            sizes_version: None,
            sizes_verified: false,
        }
    }

    /// A new Northflank entry in project `project_id`: its three sizes,
    /// each with its default plan.
    pub(crate) fn new_northflank(name: &str, project_id: &str) -> Self {
        let mut config = Self::new(name);
        CLOUD_PROVIDER_NORTHFLANK.clone_into(&mut config.provider);
        config.template_project = String::new();
        config.northflank = Some(NorthflankSettings::new(project_id));
        config.sizes = CloudMachineSize::NORTHFLANK.to_vec();
        config.sizes_verified = true;
        config.sizes_version = Some(intentd_version().to_owned());
        config
    }

    /// A new Agent37 entry: `default` instances in `us`, its four sizes.
    pub(crate) fn new_agent37(name: &str) -> Self {
        let mut config = Self::new(name);
        CLOUD_PROVIDER_AGENT37.clone_into(&mut config.provider);
        config.template_project = String::new();
        config.agent37 = Some(Agent37Settings::default());
        config.sizes = CloudMachineSize::AGENT37.to_vec();
        config.sizes_verified = true;
        config.sizes_version = Some(intentd_version().to_owned());
        config
    }

    /// A new Vercel entry in team `team_id` and project `project_id`: the
    /// published image, its three sizes.
    pub(crate) fn new_vercel(name: &str, team_id: &str, project_id: &str) -> Self {
        let mut config = Self::new(name);
        CLOUD_PROVIDER_VERCEL.clone_into(&mut config.provider);
        config.template_project = String::new();
        config.vercel = Some(VercelSettings::new(team_id, project_id));
        config.sizes = CloudMachineSize::VERCEL.to_vec();
        config.sizes_verified = true;
        config.sizes_version = Some(intentd_version().to_owned());
        config
    }

    pub(crate) fn is_vercel(&self) -> bool {
        self.provider == CLOUD_PROVIDER_VERCEL
    }

    pub(crate) fn is_northflank(&self) -> bool {
        self.provider == CLOUD_PROVIDER_NORTHFLANK
    }

    pub(crate) fn is_agent37(&self) -> bool {
        self.provider == CLOUD_PROVIDER_AGENT37
    }

    pub(crate) fn is_e2b(&self) -> bool {
        self.provider == CLOUD_PROVIDER_E2B
    }

    /// The sizes this entry's provider offers, smallest first.
    pub(crate) fn offered_sizes(&self) -> &'static [CloudMachineSize] {
        if self.is_northflank() {
            &CloudMachineSize::NORTHFLANK
        } else if self.is_agent37() {
            &CloudMachineSize::AGENT37
        } else if self.is_vercel() {
            &CloudMachineSize::VERCEL
        } else {
            &CloudMachineSize::E2B
        }
    }

    /// The provider's display name, for messages.
    pub(crate) fn provider_name(&self) -> &'static str {
        cloud_provider_name(&self.provider)
    }

    /// The compute plan a new sandbox of `size` uses (Northflank only).
    pub(crate) fn plan(&self, size: CloudMachineSize) -> Option<String> {
        self.northflank.as_ref().and_then(|nf| nf.plan(size))
    }

    /// Record a published-size check. A check that could not read the
    /// manifest (`None`) keeps the last list for this version, and leaves
    /// it stale so the next read tries again; with no list yet it offers
    /// the fallback.
    pub(crate) fn set_sizes(&mut self, checked: Option<Published>) {
        match checked {
            Some(published) => {
                self.sizes = published.sizes;
                self.sizes_verified = published.verified;
                self.sizes_checked_at = Some(now_iso());
                self.sizes_version = Some(intentd_version().to_owned());
            }
            None if self.sizes_version.as_deref() == Some(intentd_version())
                && !self.sizes.is_empty() => {}
            None => {
                let fallback = Published::fallback();
                self.sizes = fallback.sizes;
                self.sizes_verified = false;
                self.sizes_checked_at = None;
                self.sizes_version = Some(intentd_version().to_owned());
            }
        }
    }

    /// Whether `size` may be saved: any size while the list is not
    /// verified (the create is the real check), else a listed one.
    pub(crate) fn allows(&self, size: CloudMachineSize) -> bool {
        if !self.offered_sizes().contains(&size) {
            return false;
        }
        self.is_northflank()
            || self.is_agent37()
            || self.is_vercel()
            || !self.sizes_verified
            || self.sizes.contains(&size)
    }

    /// The template (E2B, Agent37) or image (Northflank, Vercel) a new
    /// sandbox of `size` starts from, as the entry reports it.
    pub(crate) fn template(&self, size: CloudMachineSize) -> String {
        if let Some(nf) = &self.northflank {
            return format!("{}:{}", nf.image, intentd_version());
        }
        if let Some(vercel) = &self.vercel {
            return intent_nodes::vercel::image_ref(&vercel.image, intentd_version());
        }
        if self.agent37.is_some() {
            return intent_nodes::agent37::template_name(&self.start_image(size));
        }
        format!(
            "{}:{}",
            template_name(&self.template_project, size),
            intentd_version()
        )
    }

    /// What a create asks the provider to start: the template, except on
    /// Agent37, whose adapter imports the image as its template.
    pub(crate) fn start_image(&self, size: CloudMachineSize) -> String {
        if self.agent37.is_some() {
            return intent_nodes::agent37::image_ref(intentd_version());
        }
        self.template(size)
    }

    /// Whether the published-size list is missing, stale or for another
    /// version.
    pub(crate) fn sizes_stale(&self) -> bool {
        if self.sizes_version.as_deref() != Some(intentd_version()) {
            return true;
        }
        let Some(checked) = self.sizes_checked_at.as_deref() else {
            return true;
        };
        time::OffsetDateTime::parse(checked, &time::format_description::well_known::Rfc3339)
            .map_or(true, |at| {
                time::OffsetDateTime::now_utc() - at > SIZES_MAX_AGE
            })
    }

    /// `CloudMachineProvider` on the wire for entry `id`. Never holds the
    /// key.
    pub(crate) fn to_json(&self, id: &str) -> Value {
        let sizes: Vec<Value> = if let Some(nf) = &self.northflank {
            CloudMachineSize::NORTHFLANK
                .into_iter()
                .filter_map(|size| Some(size.deployment_info_json(&nf.plan(size)?)))
                .collect()
        } else if self.agent37.is_some() || self.vercel.is_some() {
            self.offered_sizes()
                .iter()
                .map(|size| size.bare_info_json())
                .collect()
        } else {
            self.sizes.iter().map(|size| size.info_json()).collect()
        };
        let mut provider = json!({
            "id": id,
            "provider": self.provider,
            "name": self.name,
            "state": self.state,
            "template": self.template(self.size),
            "agentProviders": agent_providers(),
            "sizes": sizes,
            "size": self.size,
            "idlePauseMinutes": self.idle_pause_minutes,
            "pausedDeleteDays": self.paused_delete_days,
            "configuredAt": self.configured_at,
        });
        match (&self.northflank, &self.agent37, &self.vercel) {
            (Some(nf), _, _) => {
                provider["northflank"] = json!({
                    "projectId": nf.project_id,
                    "region": nf.region,
                    "image": nf.image,
                    "plans": nf.plans_json(),
                    "diskGiB": nf.disk_gib.max(MIN_DISK_GIB),
                });
            }
            (None, Some(a37), _) => {
                provider["agent37"] = json!({
                    "instanceType": a37.instance_type,
                    "region": a37.region,
                    "diskGiB": a37.disk_gib,
                });
            }
            (None, None, Some(vercel)) => {
                provider["vercel"] = json!({
                    "teamId": vercel.team_id,
                    "projectId": vercel.project_id,
                    "image": vercel.image,
                });
            }
            (None, None, None) => provider["templateProject"] = json!(self.template_project),
        }
        if let Some(checked) = &self.checked_at {
            provider["checkedAt"] = json!(checked);
        }
        provider
    }
}

/// `<templateProject>/intent-node-<size>`.
pub(crate) fn template_name(project: &str, size: CloudMachineSize) -> String {
    format!("{project}/intent-node-{}", size.id())
}

/// The intentd version whose template tag this backend starts.
pub(crate) fn intentd_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

/// The base providers new sandboxes can run.
pub(crate) fn agent_providers() -> Vec<String> {
    match std::env::var(AGENT_PROVIDERS_ENV) {
        Ok(list) if !list.trim().is_empty() => list
            .split(',')
            .map(str::trim)
            .filter(|id| !id.is_empty())
            .map(str::to_owned)
            .collect(),
        _ => TEMPLATE_AGENT_PROVIDERS
            .iter()
            .map(|id| (*id).to_owned())
            .collect(),
    }
}

/// Entry `id`'s saved settings, `None` when there is no such entry (or it
/// was removed and only its key waits for the reaper).
pub(crate) async fn load(svc: &Services, id: &str) -> Result<Option<ProviderConfig>> {
    let Some(json) = svc.store().get_cloud_machine_provider(id).await? else {
        return Ok(None);
    };
    Ok(Some(decode(&json)?).filter(|config| config.retired_from.is_none()))
}

fn decode(json: &str) -> Result<ProviderConfig> {
    serde_json::from_str(json)
        .map_err(|e| Error::Internal(format!("the saved E2B settings are unreadable: {e}")))
}

/// Every saved row, removed entries waiting for the reaper included, in the
/// order they were added. An unreadable one is left out (and logged).
async fn load_rows(svc: &Services) -> Result<Vec<(String, ProviderConfig)>> {
    Ok(svc
        .store()
        .list_cloud_machine_providers()
        .await?
        .into_iter()
        .filter_map(|(id, json)| match decode(&json) {
            Ok(config) => Some((id, config)),
            Err(error) => {
                tracing::warn!(entry = %id, %error, "cloud machines: skipping an unreadable E2B entry");
                None
            }
        })
        .collect())
}

/// Every entry, in the order they were added.
pub(crate) async fn load_all(svc: &Services) -> Result<Vec<(String, ProviderConfig)>> {
    Ok(load_rows(svc)
        .await?
        .into_iter()
        .filter(|(_, config)| config.retired_from.is_none())
        .collect())
}

/// The removed entries whose key is kept only for the reaper: `(row id,
/// removed entry id, its settings)`.
pub(crate) async fn load_retired(svc: &Services) -> Result<Vec<(String, String, ProviderConfig)>> {
    Ok(load_rows(svc)
        .await?
        .into_iter()
        .filter_map(|(id, config)| {
            let from = config.retired_from.clone()?;
            Some((id, from, config))
        })
        .collect())
}

pub(crate) async fn save(svc: &Services, id: &str, config: &ProviderConfig) -> Result<()> {
    let json = serde_json::to_string(config)
        .map_err(|e| Error::Internal(format!("encode the E2B settings: {e}")))?;
    svc.store().save_cloud_machine_provider(id, &json).await
}

/// Entry `id`'s saved key.
pub(crate) async fn saved_key(svc: &Services, id: &str) -> Result<Option<SecretString>> {
    Ok(svc
        .load_cloud_machine_key(id)
        .await?
        .filter(|key| !key.is_empty())
        .map(SecretString::new))
}

/// `<kind>-<12 lowercase hex>`.
fn random_id(kind: &str) -> String {
    let hex = uuid::Uuid::new_v4().simple().to_string();
    format!("{kind}-{}", &hex[..12])
}

/// The id of a new entry of provider `kind`: for E2B, `"e2b"` while no
/// saved row has it, else `e2b-<12 hex>`; for Northflank, always
/// `northflank-<12 hex>`.
pub(crate) async fn new_cloud_machine_id(svc: &Services, kind: &str) -> Result<String> {
    let rows = load_rows(svc).await?;
    // A removed "e2b" whose key waits for the reaper keeps the id taken, so
    // the reaper never counts a new entry's sandboxes as the removed one's.
    let taken = |id: &str| {
        rows.iter()
            .any(|(row, saved)| row == id || saved.retired_from.as_deref() == Some(id))
    };
    if kind == CLOUD_PROVIDER_E2B && !taken(CLOUD_E2B_LEGACY_ENTRY) {
        return Ok(CLOUD_E2B_LEGACY_ENTRY.to_owned());
    }
    loop {
        let id = random_id(kind);
        if !taken(&id) {
            return Ok(id);
        }
    }
}

/// Whether `id` is the row of a removed entry's key kept for the reaper.
pub(crate) async fn is_retired(svc: &Services, id: &str) -> Result<bool> {
    let Some(json) = svc.store().get_cloud_machine_provider(id).await? else {
        return Ok(false);
    };
    Ok(decode(&json)?.retired_from.is_some())
}

/// The row id a removed entry's key is kept under for the reaper. It is not
/// an entry id, so a new entry never reuses it.
pub(crate) fn retired_id() -> String {
    format!("retired-{}", random_id(CLOUD_PROVIDER_E2B))
}

/// A valid entry name: 1-64 UTF-8 bytes after trimming, no control
/// characters.
///
/// # Errors
///
/// `InvalidParams` otherwise.
pub(crate) fn entry_name(raw: &str) -> Result<String> {
    let name = raw.trim();
    if name.is_empty() || name.len() > MAX_ENTRY_NAME_BYTES || name.chars().any(char::is_control) {
        return Err(Error::InvalidParams(format!(
            "name must be 1-{MAX_ENTRY_NAME_BYTES} UTF-8 bytes with no control characters"
        )));
    }
    Ok(name.to_owned())
}

/// The first of "E2B", "E2B 2", "E2B 3", ... no entry other than `id` has
/// (ignoring case).
pub(crate) fn free_legacy_name(entries: &[(String, ProviderConfig)], id: &str) -> String {
    let taken = |name: &str| {
        entries
            .iter()
            .any(|(other, saved)| other != id && saved.name.to_lowercase() == name.to_lowercase())
    };
    let mut n = 1u32;
    loop {
        let name = if n == 1 {
            CLOUD_PROVIDER_E2B_NAME.to_owned()
        } else {
            format!("{CLOUD_PROVIDER_E2B_NAME} {n}")
        };
        if !taken(&name) {
            return name;
        }
        n += 1;
    }
}

/// The entry a new sandbox starts with: with no entry at all,
/// `not-configured`; the `requested` entry, of any provider
/// (`not-configured` when unknown). Without one, among the *candidates*
/// (every entry when `any_provider`, else the E2B entries): the entry of
/// the creating agent's chain (`chain`, when it is a candidate); else the
/// only candidate; else, for E2B only, `"e2b"`; else `entry-required`
/// (`not-configured` with no candidate at all).
///
/// # Errors
///
/// As above, each `cloud-machine-unavailable` naming the entry when known.
pub(crate) async fn resolve(
    svc: &Services,
    requested: Option<&str>,
    chain: Option<&str>,
    any_provider: bool,
) -> Result<(String, ProviderConfig)> {
    let mut all = load_all(svc).await?;
    if all.is_empty() {
        return Err(match requested {
            Some(id) => with_cloud_machine_id(not_configured(), id),
            None => not_configured(),
        });
    }
    let find = |all: &mut Vec<(String, ProviderConfig)>, id: &str| {
        all.iter()
            .position(|(entry, _)| entry == id)
            .map(|at| all.swap_remove(at))
    };
    if let Some(id) = requested {
        return find(&mut all, id).ok_or_else(|| with_cloud_machine_id(not_configured(), id));
    }
    if !any_provider {
        all.retain(|(_, saved)| saved.is_e2b());
        if all.is_empty() {
            return Err(not_configured());
        }
    }
    if let Some(entry) = chain.and_then(|id| find(&mut all, id)) {
        return Ok(entry);
    }
    if all.len() == 1 {
        return Ok(all.remove(0));
    }
    if !any_provider {
        if let Some(entry) = find(&mut all, CLOUD_E2B_LEGACY_ENTRY) {
            return Ok(entry);
        }
    }
    Err(entry_required(&all, any_provider))
}

/// `cloud-machine-unavailable` `entry-required`, listing `entries`.
fn entry_required(entries: &[(String, ProviderConfig)], any_provider: bool) -> Error {
    let names: Vec<&str> = entries
        .iter()
        .map(|(_, saved)| saved.name.as_str())
        .collect();
    let what = if any_provider { "cloud machine" } else { "E2B" };
    let error = cloud_machine_unavailable(
        "entry-required",
        false,
        None,
        format!(
            "Several {what} entries are set up; pick one of: {}.",
            names.join(", ")
        ),
    );
    match error {
        Error::RemoteMachine(mut failure) => {
            failure.data["entries"] = Value::Array(
                entries
                    .iter()
                    .map(|(id, saved)| {
                        json!({ "cloudMachineId": id, "name": saved.name, "provider": saved.provider })
                    })
                    .collect(),
            );
            Error::RemoteMachine(failure)
        }
        other => other,
    }
}

/// The entry id a cloud machine row saved before entries existed belongs
/// to.
pub(crate) fn legacy_entry() -> &'static str {
    CLOUD_E2B_LEGACY_ENTRY
}

/// Turn a single provider saved before entries existed into the entry
/// `"e2b"` named "E2B", once: its sandboxes and agents get
/// `cloudMachineId: "e2b"`, then the row is marked `migrated` (a migration
/// cut short runs again). Runs under the settings lock, so no `configure`,
/// `remove` or size check interleaves.
pub(crate) async fn migrate(svc: &Services) -> Result<()> {
    let feature = super::feature(svc)?;
    let _settings = feature.cloud.settings.lock().await;
    let Some(mut legacy) = load(svc, CLOUD_E2B_LEGACY_ENTRY).await? else {
        return Ok(());
    };
    if legacy.migrated {
        return Ok(());
    }
    for row in svc.store().list_cloud_machines().await? {
        if let Some(cloud) = &row.cloud {
            svc.store().update_cloud_machine(&row.id, cloud).await?;
        }
    }
    for (workspace_id, agent_id) in svc.store().list_cloud_placed_agents().await? {
        crate::view::update(svc.store(), &workspace_id, &agent_id, |view| {
            if view.placement.cloud_machine_id.is_none() {
                view.placement.cloud_machine_id = Some(CLOUD_E2B_LEGACY_ENTRY.to_owned());
            }
        })
        .await?;
    }
    legacy.migrated = true;
    save(svc, CLOUD_E2B_LEGACY_ENTRY, &legacy).await?;
    tracing::info!("cloud machines: the saved E2B provider is now the entry \"e2b\"");
    Ok(())
}

/// One provider account, shared by the calls that use its saved key.
pub(crate) type Provider = std::sync::Arc<dyn CloudProvider>;

/// A Northflank client for entry settings `nf` with `key`.
pub(crate) fn northflank_client(
    nf: &NorthflankSettings,
    key: SecretString,
) -> Result<NorthflankClient> {
    let env = |name: &str| std::env::var(name).ok().filter(|url| !url.is_empty());
    let defaults = NorthflankConfig::default();
    let config = NorthflankConfig {
        project_id: nf.project_id.clone(),
        api_url: env(NORTHFLANK_API_URL_ENV).unwrap_or_else(|| defaults.api_url.clone()),
        exec_url: env(NORTHFLANK_EXEC_URL_ENV).unwrap_or_else(|| defaults.exec_url.clone()),
        registry_url: env(NORTHFLANK_REGISTRY_URL_ENV),
        ..defaults
    };
    NorthflankClient::new(key, config)
        .map_err(|error| to_error(&error, Phase::Other, CLOUD_PROVIDER_NORTHFLANK))
}

/// An Agent37 client with `key`.
pub(crate) fn agent37_client(key: SecretString) -> Result<Agent37Client> {
    let env = |name: &str| std::env::var(name).ok().filter(|url| !url.is_empty());
    let defaults = Agent37Config::default();
    let config = Agent37Config {
        api_url: env(AGENT37_API_URL_ENV).unwrap_or_else(|| defaults.api_url.clone()),
        exec_url: env(AGENT37_EXEC_URL_ENV).unwrap_or_else(|| defaults.exec_url.clone()),
        ..defaults
    };
    Agent37Client::new(key, config)
        .map_err(|error| to_error(&error, Phase::Other, CLOUD_PROVIDER_AGENT37))
}

/// A Vercel client for entry settings `vercel` with `key`.
pub(crate) fn vercel_client(vercel: &VercelSettings, key: SecretString) -> Result<VercelClient> {
    let env = |name: &str| std::env::var(name).ok().filter(|url| !url.is_empty());
    let defaults = VercelConfig::default();
    let config = VercelConfig {
        team_id: vercel.team_id.clone(),
        project_id: vercel.project_id.clone(),
        api_url: env(VERCEL_API_URL_ENV).unwrap_or_else(|| defaults.api_url.clone()),
        registry_url: env(VERCEL_REGISTRY_URL_ENV).unwrap_or_else(|| defaults.registry_url.clone()),
        ..defaults
    };
    VercelClient::new(key, config)
        .map_err(|error| to_error(&error, Phase::Other, CLOUD_PROVIDER_VERCEL))
}

/// A provider client for entry `saved` with `key`.
pub(crate) fn client(saved: &ProviderConfig, key: SecretString) -> Result<Provider> {
    if let Some(nf) = &saved.northflank {
        return Ok(std::sync::Arc::new(northflank_client(nf, key)?));
    }
    if let Some(vercel) = &saved.vercel {
        return Ok(std::sync::Arc::new(vercel_client(vercel, key)?));
    }
    if saved.agent37.is_some() {
        return Ok(std::sync::Arc::new(agent37_client(key)?));
    }
    let mut config = E2bConfig::default();
    if let Ok(url) = std::env::var(API_URL_ENV) {
        if !url.is_empty() {
            config.api_url = url;
        }
    }
    if let Ok(url) = std::env::var(ENVD_URL_ENV) {
        if !url.is_empty() {
            config.envd_url = url;
        }
    }
    match E2bClient::new(key, config) {
        Ok(client) => Ok(std::sync::Arc::new(client)),
        Err(error) => Err(to_error(&error.into(), Phase::Other, CLOUD_PROVIDER_E2B)),
    }
}

/// A provider for entry `id`'s saved key.
///
/// # Errors
///
/// `cloud-machine-unavailable` `not-configured` without a saved key.
pub(crate) async fn saved_client(svc: &Services, id: &str) -> Result<Provider> {
    let saved = load(svc, id).await?.ok_or_else(not_configured)?;
    let key = saved_key(svc, id).await?.ok_or_else(not_configured)?;
    client(&saved, key)
}

pub(crate) fn not_configured() -> Error {
    cloud_machine_unavailable(
        "not-configured",
        false,
        None,
        "This E2B entry is not set up on this backend: add one with an E2B API key in Settings → Nodes.",
    )
}

pub(crate) fn key_rejected(status: Option<u16>) -> Error {
    key_rejected_by(CLOUD_PROVIDER_E2B, status)
}

/// `key-rejected` from cloud provider `kind`.
pub(crate) fn key_rejected_by(kind: &str, status: Option<u16>) -> Error {
    let message = if kind == CLOUD_PROVIDER_NORTHFLANK {
        "Northflank rejected the saved API token: save a working token for this Northflank entry in Settings → Nodes."
    } else if kind == CLOUD_PROVIDER_AGENT37 {
        "Agent37 rejected the saved API key: save a working key for this Agent37 entry in Settings → Nodes."
    } else if kind == CLOUD_PROVIDER_VERCEL {
        "Vercel rejected the saved access token: save a working token for this Vercel entry in Settings → Nodes."
    } else {
        "E2B rejected the saved API key: save a working key for this E2B entry in Settings → Nodes."
    };
    with_cloud_provider(
        cloud_machine_unavailable("key-rejected", false, status, message),
        kind,
    )
}

/// `project-missing`: the Northflank or Vercel project does not exist or
/// the token cannot see it.
pub(crate) fn project_missing(kind: &str, status: Option<u16>) -> Error {
    let message = if kind == CLOUD_PROVIDER_VERCEL {
        "Vercel has no project with this entry's project id in its team, or the token cannot see it: check the team and project ids in Settings → Nodes."
    } else {
        "Northflank has no project with this entry's project id, or the token cannot see it: check the project id in Settings → Nodes."
    };
    with_cloud_provider(
        cloud_machine_unavailable("project-missing", false, status, message),
        kind,
    )
}

/// `permission-denied`: the provider `kind` refused `action` for the key
/// (a Northflank token without the permission, an Agent37 key whose IP
/// allowlist leaves this backend out).
pub(crate) fn permission_denied(kind: &str, action: &str, status: Option<u16>) -> Error {
    let message = if kind == CLOUD_PROVIDER_AGENT37 {
        format!(
            "Agent37 refused {action} for the saved key: its IP allowlist does not include this backend's address. Allow it at Agent37, or save another key in Settings → Nodes."
        )
    } else if kind == CLOUD_PROVIDER_VERCEL {
        format!(
            "Vercel refused {action} for the saved token: give it access to the entry's team and the sandboxes of its project, then save it in Settings → Nodes."
        )
    } else {
        format!(
            "The Northflank token lacks the permission for {action}: give it access to the project's services, volumes, tags and command execution, then save it in Settings → Nodes."
        )
    };
    let error = cloud_machine_unavailable("permission-denied", false, status, message);
    match with_cloud_provider(error, kind) {
        Error::RemoteMachine(mut failure) => {
            failure.data["action"] = json!(action);
            Error::RemoteMachine(failure)
        }
        other => other,
    }
}

pub(crate) fn template_missing(template: &str) -> Error {
    cloud_machine_unavailable(
        "template-missing",
        false,
        None,
        format!(
            "E2B has no template {template} for this backend's Intent version; no sandbox was left behind. Pick another size, or update Intent."
        ),
    )
}

/// `template-missing` for a Northflank image whose tag does not exist.
pub(crate) fn image_missing(image: &str) -> Error {
    with_cloud_provider(
        cloud_machine_unavailable(
            "template-missing",
            false,
            None,
            format!(
                "The image {image} for this backend's Intent version does not exist or is not public; no service was started. Update Intent, or set another image."
            ),
        ),
        CLOUD_PROVIDER_NORTHFLANK,
    )
}

/// `template-missing` for an Agent37 template whose image Agent37 cannot
/// import.
pub(crate) fn agent37_image_missing(image: &str) -> Error {
    with_cloud_provider(
        cloud_machine_unavailable(
            "template-missing",
            false,
            None,
            format!(
                "Agent37 could not import the image {image} for this backend's Intent version; no instance was started. Update Intent."
            ),
        ),
        CLOUD_PROVIDER_AGENT37,
    )
}

/// `template-missing` for a Vercel image whose tag does not exist.
pub(crate) fn vercel_image_missing(image: &str) -> Error {
    with_cloud_provider(
        cloud_machine_unavailable(
            "template-missing",
            false,
            None,
            format!(
                "Vercel has no image {image} for this backend's Intent version; no sandbox was started. Update Intent, or set another image."
            ),
        ),
        CLOUD_PROVIDER_VERCEL,
    )
}

/// `no-capacity`: Agent37 has no room for the entry's instance where it
/// asked; it never places it elsewhere.
pub(crate) fn no_capacity(detail: &str, status: Option<u16>) -> Error {
    with_cloud_provider(
        cloud_machine_unavailable(
            "no-capacity",
            true,
            status,
            format!(
                "Agent37 has no room for {detail} right now; nothing was started elsewhere. Try again later, or pick another size or region in Settings → Nodes."
            ),
        ),
        CLOUD_PROVIDER_AGENT37,
    )
}

/// What the backend was doing when E2B failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Phase {
    Create,
    Other,
}

/// The documented error for a failure of cloud provider `kind`. Messages
/// are the provider's own, which the client already scrubbed of the key and
/// tokens.
pub(crate) fn to_error(error: &CloudError, phase: Phase, kind: &str) -> Error {
    with_cloud_provider(provider_error(error, phase, kind), kind)
}

fn provider_error(error: &CloudError, phase: Phase, kind: &str) -> Error {
    if kind == CLOUD_PROVIDER_AGENT37 {
        return agent37_error(error, phase);
    }
    if kind == CLOUD_PROVIDER_VERCEL {
        return vercel_error(error, phase);
    }
    let northflank = kind == CLOUD_PROVIDER_NORTHFLANK;
    if let Some(limit) = error.quota_limit() {
        let retry_after = error.retry_after().map(|d| d.as_secs());
        let message = match (limit, northflank) {
            (QuotaLimit::Plan, true) => "Northflank refused for the project's or account's quota: pick a smaller size in Settings → Nodes, or raise the quota at Northflank.",
            (QuotaLimit::Plan, false) => "E2B refused this sandbox size for the project's plan or limits: pick a smaller size in Settings → Nodes, or use E2B Pro and ask E2B support to raise the project's limits.",
            (QuotaLimit::Concurrency, true) => "Northflank's limit on running services for this project is reached: pause another one, or raise the quota at Northflank.",
            (QuotaLimit::Concurrency, false) => "E2B's limit on running sandboxes for this project is reached: stop or pause another sandbox, or raise the plan's limit.",
            (QuotaLimit::Rate, true) => "Northflank's request rate limit is reached: try again shortly.",
            (QuotaLimit::Rate, false) => "E2B's request rate limit is reached: try again shortly.",
        };
        return cloud_quota(limit.as_str(), retry_after, message);
    }
    if northflank {
        match error.kind() {
            CloudErrorKind::Unauthorized => return key_rejected_by(kind, error.status()),
            CloudErrorKind::PermissionDenied => {
                return permission_denied(
                    kind,
                    error.action().unwrap_or("this call"),
                    error.status(),
                )
            }
            CloudErrorKind::ProjectMissing => return project_missing(kind, error.status()),
            CloudErrorKind::NotFound if phase == Phase::Create => {
                return cloud_machine_unavailable(
                    "template-missing",
                    false,
                    error.status(),
                    format!("Northflank could not start the Intent image for this backend's version; no service was left behind: {error}"),
                )
            }
            _ => {}
        }
    }
    match error.kind() {
        CloudErrorKind::NotConfigured => not_configured(),
        // A create E2B forbids is about the template (another project's,
        // not public), not the key: a rejected key fails with 401.
        CloudErrorKind::Unauthorized if error.status() == Some(403) && phase == Phase::Create => {
            cloud_machine_unavailable(
                "template-missing",
                false,
                Some(403),
                "E2B does not let this project use the Intent template for this backend's version and size; no sandbox was left behind.",
            )
        }
        CloudErrorKind::Unauthorized => key_rejected(error.status()),
        CloudErrorKind::NotFound if phase == Phase::Create => cloud_machine_unavailable(
            "template-missing",
            false,
            Some(404),
            "E2B has no template for this backend's Intent version and size; no sandbox was left behind.",
        ),
        kind => {
            let status = match kind {
                CloudErrorKind::Api => error.status(),
                _ => None,
            };
            cloud_machine_unavailable(
                "api-error",
                error.is_retryable(),
                status,
                if northflank {
                    format!("Northflank failed: {error}")
                } else {
                    format!("E2B failed: {error}")
                },
            )
        }
    }
}

fn agent37_error(error: &CloudError, phase: Phase) -> Error {
    let kind = CLOUD_PROVIDER_AGENT37;
    if let Some(limit) = error.quota_limit() {
        let retry_after = error.retry_after().map(|d| d.as_secs());
        let message = match limit {
            QuotaLimit::Plan => "The Agent37 wallet cannot pay for this instance: top up the Agent37 wallet, then try again.",
            QuotaLimit::Concurrency => "Agent37's limit on awake memory for this workspace is reached: pause another Agent37 machine, or raise the limit at Agent37.",
            QuotaLimit::Rate => "Agent37's request rate limit is reached: try again shortly.",
        };
        return cloud_quota(limit.as_str(), retry_after, message);
    }
    match error.kind() {
        CloudErrorKind::NotConfigured => not_configured(),
        CloudErrorKind::Unauthorized => key_rejected_by(kind, error.status()),
        CloudErrorKind::PermissionDenied => {
            permission_denied(kind, error.action().unwrap_or("this call"), error.status())
        }
        CloudErrorKind::NoCapacity => {
            no_capacity("this entry's instance type and size in its region", error.status())
        }
        CloudErrorKind::NotFound if phase == Phase::Create => cloud_machine_unavailable(
            "template-missing",
            false,
            error.status(),
            format!("Agent37 could not start the Intent image for this backend's version; no instance was left behind: {error}"),
        ),
        other => cloud_machine_unavailable(
            "api-error",
            error.is_retryable(),
            match other {
                CloudErrorKind::Api => error.status(),
                _ => None,
            },
            format!("Agent37 failed: {error}"),
        ),
    }
}

fn vercel_error(error: &CloudError, phase: Phase) -> Error {
    let kind = CLOUD_PROVIDER_VERCEL;
    if let Some(limit) = error.quota_limit() {
        let retry_after = error.retry_after().map(|d| d.as_secs());
        let message = match limit {
            QuotaLimit::Plan => "The Vercel plan does not allow this sandbox size: pick a smaller size in Settings → Nodes, or change the Vercel plan.",
            QuotaLimit::Concurrency => "Vercel's limit on running sandboxes for this team is reached: pause another Vercel machine, or raise the limit at Vercel.",
            QuotaLimit::Rate => "Vercel refused for its rate limits (requests, concurrent sandboxes or vCPU allocation for the team): try again shortly, or pause another Vercel machine.",
        };
        return cloud_quota(limit.as_str(), retry_after, message);
    }
    match error.kind() {
        CloudErrorKind::NotConfigured => not_configured(),
        CloudErrorKind::Unauthorized => key_rejected_by(kind, error.status()),
        CloudErrorKind::PermissionDenied => {
            permission_denied(kind, error.action().unwrap_or("this call"), error.status())
        }
        CloudErrorKind::ProjectMissing => project_missing(kind, error.status()),
        CloudErrorKind::NotFound if phase == Phase::Create => cloud_machine_unavailable(
            "template-missing",
            false,
            error.status(),
            format!("Vercel could not start the Intent image for this backend's version; no sandbox was left behind: {error}"),
        ),
        _ => cloud_machine_unavailable(
            "api-error",
            error.is_retryable(),
            error.status(),
            format!("Vercel failed: {error}"),
        ),
    }
}

/// The sizes published for this backend's version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Published {
    pub(crate) sizes: Vec<CloudMachineSize>,
    /// From the release's manifest; `false` for the fallback list.
    pub(crate) verified: bool,
}

impl Published {
    /// The sizes every release builds, unverified.
    pub(crate) fn fallback() -> Self {
        Self {
            sizes: FALLBACK_SIZES.to_vec(),
            verified: false,
        }
    }
}

/// The sizes of `project` published for this backend's version, from the
/// release's template manifest. E2B cannot answer this with the owner's
/// key: its template lookup covers only the key's own project, so a probe
/// of Intent's public templates fails for every other key. When the
/// manifest is missing, unreadable, or for another project or version
/// (a custom `templateProject`), the sizes every release builds are
/// offered, unverified: the create stays the real check.
pub(crate) async fn published_sizes(project: &str) -> Option<Published> {
    let base = std::env::var(MANIFEST_BASE_URL_ENV)
        .ok()
        .filter(|url| !url.is_empty())
        .unwrap_or_else(|| intent_nodes::e2b::DEFAULT_MANIFEST_BASE_URL.to_owned());
    match intent_nodes::e2b::fetch_template_manifest(&base, intentd_version(), MANIFEST_TIMEOUT)
        .await
    {
        Ok(manifest) if manifest.project == project && manifest.version == intentd_version() => {
            Some(Published {
                sizes: CloudMachineSize::E2B
                    .into_iter()
                    .filter(|size| manifest.sizes.iter().any(|id| id == size.id()))
                    .collect(),
                verified: true,
            })
        }
        Ok(manifest) => {
            tracing::info!(project = %manifest.project, version = %manifest.version, "cloud machines: the template manifest is for another project or version; sizes unverified");
            Some(Published::fallback())
        }
        Err(intent_nodes::e2b::E2bError::NotFound(_)) => {
            tracing::info!(
                "cloud machines: this release has no template manifest; sizes unverified"
            );
            Some(Published::fallback())
        }
        Err(error) => {
            tracing::warn!(%error, "cloud machines: could not read the release's template manifest; keeping the last sizes");
            None
        }
    }
}

/// Mark entry `id`'s saved key rejected after E2B refused `refused` (401)
/// on another call, so placement refuses before any sandbox, and announce
/// the change. Under the settings lock, and only while `refused` is still
/// the entry's saved key: a start that used a key the owner has since
/// replaced never marks the new one.
pub(crate) async fn note_key_rejected(svc: &Services, id: &str, refused: &SecretString) {
    let Ok(feature) = super::feature(svc) else {
        return;
    };
    let _settings = feature.cloud.settings.lock().await;
    if !matches!(saved_key(svc, id).await, Ok(Some(saved)) if &saved == refused) {
        return;
    }
    let Ok(Some(mut saved)) = load(svc, id).await else {
        return;
    };
    if saved.state == ProviderState::KeyRejected {
        return;
    }
    saved.state = ProviderState::KeyRejected;
    if let Err(error) = save(svc, id, &saved).await {
        tracing::warn!(%error, "cloud machines: could not record the rejected key");
        return;
    }
    svc.publish_cloud_machine_changed(json!({ "provider": saved.to_json(id) }), None)
        .await;
}

/// Record the outcome of an E2B call on the saved settings: a rejected key
/// or unreachable E2B, or a success.
pub(crate) fn note_outcome(config: &mut ProviderConfig, error: Option<&Error>) {
    let reason = error.and_then(|error| match error {
        Error::RemoteMachine(failure) => failure.data["reason"].as_str().map(str::to_owned),
        _ => None,
    });
    match reason.as_deref() {
        None if error.is_none() => {
            config.state = ProviderState::Ready;
            config.checked_at = Some(now_iso());
        }
        Some("key-rejected") => config.state = ProviderState::KeyRejected,
        Some("api-error") => config.state = ProviderState::Unreachable,
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_entry_saved_under_the_northflank_disk_minimum_reports_the_disk_it_gets() {
        let mut config = ProviderConfig::new_northflank("nf", "p1");
        config.northflank.as_mut().unwrap().disk_gib = 5;
        assert_eq!(config.to_json("e")["northflank"]["diskGiB"], json!(6));
        config.northflank.as_mut().unwrap().disk_gib = 40;
        assert_eq!(config.to_json("e")["northflank"]["diskGiB"], json!(40));
    }

    #[test]
    fn an_agent37_disk_is_capped_at_the_shapes_largest_disk() {
        let mut a37 = Agent37Settings {
            disk_gib: 30,
            ..Agent37Settings::default()
        };
        assert_eq!(a37.disk_for(CloudMachineSize::S2x4), 12);
        assert_eq!(a37.disk_for(CloudMachineSize::S4x8), 20);
        assert_eq!(a37.disk_for(CloudMachineSize::S4x16), 30);
        a37.instance_type = "vm".into();
        assert_eq!(a37.disk_for(CloudMachineSize::S2x4), 30);
        a37.disk_gib = 100;
        assert_eq!(a37.disk_for(CloudMachineSize::S4x32), 100);
    }

    #[test]
    fn each_provider_allows_only_its_own_sizes() {
        let e2b = ProviderConfig::new("e");
        let nf = ProviderConfig::new_northflank("n", "p1");
        let a37 = ProviderConfig::new_agent37("a");
        assert!(e2b.allows(CloudMachineSize::S16x32));
        assert!(!e2b.allows(CloudMachineSize::S4x16));
        assert!(nf.allows(CloudMachineSize::S8x16));
        assert!(!nf.allows(CloudMachineSize::S4x32));
        assert!(a37.allows(CloudMachineSize::S4x32));
        assert!(!a37.allows(CloudMachineSize::S8x16));
        assert_eq!(
            a37.start_image(CloudMachineSize::S4x8),
            format!("ghcr.io/intent-hq/intent-node:{}", intentd_version())
        );
        assert_eq!(
            a37.template(CloudMachineSize::S4x8),
            format!("intent-node-{}", intentd_version().replace('.', "-"))
        );
    }
}
