//! The E2B provider's saved settings, its client and the mapping of E2B
//! failures onto the documented `cloud-*` errors.

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use intent_core::machines::{
    cloud_machine_unavailable, cloud_quota, CloudMachineSize, CLOUD_PROVIDER_E2B,
    CLOUD_PROVIDER_E2B_NAME,
};
use intent_core::{now_iso, Error, Result};
use intent_nodes::e2b::{E2bClient, E2bConfig, E2bError, QuotaLimit, SecretString};
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

/// The saved provider settings (`cloud_machine_provider.config_json`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ProviderConfig {
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
    pub(crate) fn new() -> Self {
        Self {
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
        !self.sizes_verified || self.sizes.contains(&size)
    }

    /// The template a new sandbox of `size` starts from.
    pub(crate) fn template(&self, size: CloudMachineSize) -> String {
        format!(
            "{}:{}",
            template_name(&self.template_project, size),
            intentd_version()
        )
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

    /// `CloudMachineProvider` on the wire. Never holds the key.
    pub(crate) fn to_json(&self) -> Value {
        let mut provider = json!({
            "id": CLOUD_PROVIDER_E2B,
            "name": CLOUD_PROVIDER_E2B_NAME,
            "state": self.state,
            "template": self.template(self.size),
            "templateProject": self.template_project,
            "agentProviders": agent_providers(),
            "sizes": self.sizes.iter().map(|size| size.info_json()).collect::<Vec<_>>(),
            "size": self.size,
            "idlePauseMinutes": self.idle_pause_minutes,
            "pausedDeleteDays": self.paused_delete_days,
            "configuredAt": self.configured_at,
        });
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

/// The saved settings, `None` when E2B is not configured.
pub(crate) async fn load(svc: &Services) -> Result<Option<ProviderConfig>> {
    let Some(json) = svc
        .store()
        .get_cloud_machine_provider(CLOUD_PROVIDER_E2B)
        .await?
    else {
        return Ok(None);
    };
    serde_json::from_str(&json)
        .map(Some)
        .map_err(|e| Error::Internal(format!("the saved E2B settings are unreadable: {e}")))
}

pub(crate) async fn save(svc: &Services, config: &ProviderConfig) -> Result<()> {
    let json = serde_json::to_string(config)
        .map_err(|e| Error::Internal(format!("encode the E2B settings: {e}")))?;
    svc.store()
        .save_cloud_machine_provider(CLOUD_PROVIDER_E2B, &json)
        .await
}

/// The saved key.
pub(crate) async fn saved_key(svc: &Services) -> Result<Option<SecretString>> {
    Ok(svc
        .load_cloud_machine_key(CLOUD_PROVIDER_E2B)
        .await?
        .filter(|key| !key.is_empty())
        .map(SecretString::new))
}

/// A client for `key`.
pub(crate) fn client(key: SecretString) -> Result<E2bClient> {
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
    E2bClient::new(key, config).map_err(|error| to_error(&error, Phase::Other))
}

/// A client for the saved key.
///
/// # Errors
///
/// `cloud-machine-unavailable` `not-configured` without a saved key.
pub(crate) async fn saved_client(svc: &Services) -> Result<E2bClient> {
    let key = saved_key(svc).await?.ok_or_else(not_configured)?;
    client(key)
}

pub(crate) fn not_configured() -> Error {
    cloud_machine_unavailable(
        "not-configured",
        false,
        None,
        "E2B is not set up on this backend: add an E2B API key in Settings → Machines.",
    )
}

pub(crate) fn key_rejected(status: Option<u16>) -> Error {
    cloud_machine_unavailable(
        "key-rejected",
        false,
        status,
        "E2B rejected the saved API key: save a working key in Settings → Machines.",
    )
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

/// What the backend was doing when E2B failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Phase {
    Create,
    Other,
}

/// The documented error for an E2B failure. Messages are E2B's own, which
/// the client already scrubbed of the key and tokens.
pub(crate) fn to_error(error: &E2bError, phase: Phase) -> Error {
    if let Some(limit) = error.quota_limit() {
        let retry_after = match error {
            E2bError::RateLimited { retry_after, .. } => retry_after.map(|d| d.as_secs()),
            _ => None,
        };
        let message = match limit {
            QuotaLimit::Plan => "E2B refused this sandbox size for the project's plan or limits: pick a smaller size in Settings → Machines, or use E2B Pro and ask E2B support to raise the project's limits.",
            QuotaLimit::Concurrency => "E2B's limit on running sandboxes for this project is reached: stop or pause another sandbox, or raise the plan's limit.",
            QuotaLimit::Rate => "E2B's request rate limit is reached: try again shortly.",
        };
        return cloud_quota(limit.as_str(), retry_after, message);
    }
    match error {
        E2bError::NotConfigured => not_configured(),
        // A create E2B forbids is about the template (another project's,
        // not public), not the key: a rejected key fails with 401.
        E2bError::Unauthorized { status: 403 } if phase == Phase::Create => {
            cloud_machine_unavailable(
                "template-missing",
                false,
                Some(403),
                "E2B does not let this project use the Intent template for this backend's version and size; no sandbox was left behind.",
            )
        }
        E2bError::Unauthorized { status } => key_rejected(Some(*status)),
        E2bError::NotFound(_) if phase == Phase::Create => cloud_machine_unavailable(
            "template-missing",
            false,
            Some(404),
            "E2B has no template for this backend's Intent version and size; no sandbox was left behind.",
        ),
        other => {
            let status = match other {
                E2bError::Api { status, .. } => Some(*status),
                _ => None,
            };
            cloud_machine_unavailable(
                "api-error",
                other.is_retryable(),
                status,
                format!("E2B failed: {other}"),
            )
        }
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
                sizes: CloudMachineSize::ALL
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

/// Mark the saved key rejected after E2B refused it (401) on another call,
/// so placement refuses before any sandbox, and announce the change.
pub(crate) async fn note_key_rejected(svc: &Services) {
    let Ok(Some(mut saved)) = load(svc).await else {
        return;
    };
    if saved.state == ProviderState::KeyRejected {
        return;
    }
    saved.state = ProviderState::KeyRejected;
    if let Err(error) = save(svc, &saved).await {
        tracing::warn!(%error, "cloud machines: could not record the rejected key");
        return;
    }
    svc.publish_cloud_machine_changed(json!({ "provider": saved.to_json() }), None)
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
