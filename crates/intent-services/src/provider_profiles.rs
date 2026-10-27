//! Named provider profiles (PROTOCOL §5.54): storage, validation and the
//! administrator-only `providers.profiles.*` surface.
//!
//! - **Metadata** (`id`, `baseProvider`, `name`, timestamps) lives in the
//!   daemon-owned `<data_dir>/provider-profiles.json`, written atomically
//!   (temp file, fsync, rename; mode `0600`) and read only at startup and after
//!   the daemon's own writes. It is not a `settings.*` path and not in
//!   `config.toml` (whose `SettingsFile` denies unknown fields, so an older
//!   daemon would refuse to start). The array order is the saved profile
//!   order (`providers.profiles.reorder`), and `backup` marks the profiles
//!   quota failover may switch to.
//! - **Environment entries** live in the existing secret store under
//!   `providerProfiles.<id>.env`, one JSON document per profile, and are only
//!   ever returned masked. They never enter events, logs, catalogs or the
//!   metadata file.
//!
//! Mutations are serialized by one async lock and validate fully before any
//! write. Create writes the secret before the metadata and removes it if the
//! metadata write fails; delete removes the metadata before the secret.
//!
//! **Orphan cleanup.** The secret store is a single file shared by every
//! daemon of the user (dev sandboxes included) while the metadata file is per
//! data dir, so a blind startup sweep of `providerProfiles.*` accounts could
//! delete another daemon's profiles. Instead each create and delete first
//! journals the profile id in the metadata file's `pendingEnvCleanup` list
//! (omitted when empty, so the steady-state file is exactly the documented
//! shape); startup removes the secrets of journaled ids that have no metadata.
//!
//! [`Services::resolve_profile_env`] turns a qualified reference into the
//! overlay every execution path applies (see `intent_acp::ProfileEnv`).
//! [`Services::ensure_provider_ref_selectable`] gates every selection front
//! door and turn start: a qualified reference whose profile is missing or
//! whose base provider is disabled needs an explicit replacement and never
//! falls back to another account. `activation.runningProcesses` and an
//! agent's `usingEarlierProfileSettings` come from the live-process registry
//! in [`crate::profile_activation`].

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use intent_core::{Error, Result};
use intent_store::NewEvent;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::settings::AsyncSecretStore;
use crate::Services;

/// File name of the metadata document inside the daemon data dir.
pub(crate) const PROFILES_FILE: &str = "provider-profiles.json";
/// The only metadata file version this daemon reads or writes.
const FILE_VERSION: u32 = 1;
/// Canonical base provider ids that advertise `supportsProfiles: true`.
pub(crate) const PROFILE_BASE_PROVIDERS: &[&str] = intent_providers::PROFILE_BASE_PROVIDERS;
/// Separator between a base provider id and a profile id in a reference.
pub(crate) const REF_SEPARATOR: char = intent_providers::PROFILE_REF_SEPARATOR;
/// `agent_session.metadata` key holding the profile `settingsFingerprint`
/// the stored native session was opened under (an opaque token, never env
/// keys or values; not part of the `AgentLite` metadata projection).
pub(crate) const NATIVE_SESSION_SETTINGS_KEY: &str = "nativeSessionSettings";
/// Masked read value of a saved non-empty `set` entry.
pub(crate) const MASK: &str = "********";

const MAX_PROFILES_PER_BASE: usize = 32;
const MAX_ENV_ENTRIES: usize = 64;
const MAX_NAME_CHARS: usize = 64;
const MAX_KEY_CHARS: usize = 128;
const MAX_VALUE_BYTES: usize = 32 * 1024;
/// Crockford-style lowercase base-32 alphabet for minted ids/fingerprints.
const ID_ALPHABET: &[u8; 32] = b"0123456789abcdefghjkmnpqrstvwxyz";

/// Secret-store account holding one profile's environment document.
pub(crate) fn env_account(profile_id: &str) -> String {
    format!("providerProfiles.{profile_id}.env")
}

/// Whether `id` matches `^[a-z0-9][a-z0-9-]{0,31}$`.
pub(crate) fn is_valid_profile_id(id: &str) -> bool {
    let bytes = id.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 32
        && (bytes[0].is_ascii_lowercase() || bytes[0].is_ascii_digit())
        && bytes
            .iter()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'-')
}

/// Split a qualified reference `<base>@<profileId>` into its parts. Returns
/// `None` for a bare id or any malformed reference (non-profile base, case
/// variants, whitespace, extra `@`, invalid id).
pub(crate) fn parse_profile_ref(reference: &str) -> Option<(&'static str, &str)> {
    intent_providers::split_profile_ref(reference)
}

/// Whether `reference` is meant as a profile reference (contains `@`).
pub(crate) fn is_qualified_ref(reference: &str) -> bool {
    reference.contains(REF_SEPARATOR)
}

/// Whether `reference` names the Default configuration of a profile-capable
/// base provider (bare canonical `codex` / `claude-code`), which may carry
/// saved environment overrides.
pub(crate) fn is_default_config_ref(reference: &str) -> bool {
    PROFILE_BASE_PROVIDERS.contains(&reference)
}

/// Whether a process for `reference` may run with a saved overlay: a profile
/// reference, or the Default configuration of a profile-capable base.
pub(crate) fn may_have_overlay(reference: &str) -> bool {
    is_qualified_ref(reference) || is_default_config_ref(reference)
}

/// The selection identity of a provider id: a qualified reference is its own
/// identity (two profiles, or a profile and Default, never compare equal);
/// a bare id or legacy alias canonicalizes through the registry.
pub(crate) fn canonical_selection(provider_id: &str) -> String {
    if is_qualified_ref(provider_id) {
        provider_id.to_owned()
    } else {
        intent_providers::provider_config(provider_id).id.to_owned()
    }
}

/// A human-readable label for a selection in daemon-written text: a bare id
/// unchanged, a saved profile as `<base> (<name>)`, a reference whose
/// profile no longer exists as `<base> (removed profile)`, and any reference
/// as just `<base>` when the profile list is unreadable (`None`), since the
/// profile may still exist. Never the internal reference.
pub(crate) fn readable_selection(selection: &str, profiles: Option<&[ProfileMeta]>) -> String {
    if !is_qualified_ref(selection) {
        return selection.to_owned();
    }
    let (base, id) = selection
        .split_once(REF_SEPARATOR)
        .unwrap_or((selection, ""));
    let Some(profiles) = profiles else {
        return base.to_owned();
    };
    profiles
        .iter()
        .find(|p| p.id == id && p.base_provider == base)
        .map_or_else(
            || format!("{base} (removed profile)"),
            |p| format!("{base} ({})", p.name),
        )
}

fn provider_ref(base: &str, id: &str) -> String {
    format!("{base}{REF_SEPARATOR}{id}")
}

fn random_token(len: usize) -> String {
    let mut out = String::with_capacity(len);
    while out.len() < len {
        for byte in uuid::Uuid::new_v4().as_bytes() {
            if out.len() == len {
                break;
            }
            out.push(char::from(ID_ALPHABET[usize::from(byte & 0x1f)]));
        }
    }
    out
}

fn now_rfc3339() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

// ---------------------------------------------------------------------------
// Persisted shapes
// ---------------------------------------------------------------------------

/// One profile's metadata row (no secrets).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ProfileMeta {
    pub(crate) id: String,
    pub(crate) base_provider: String,
    pub(crate) name: String,
    pub(crate) created_at: String,
    pub(crate) updated_at: String,
    /// Whether quota failover may switch to this profile. Omitted from the
    /// file while false, so files without backups keep the original shape
    /// and files written before the field existed load as `false`.
    #[serde(default, skip_serializing_if = "is_false")]
    pub(crate) backup: bool,
}

#[expect(
    clippy::trivially_copy_pass_by_ref,
    reason = "serde `skip_serializing_if` passes `&bool`"
)]
fn is_false(value: &bool) -> bool {
    !*value
}

impl ProfileMeta {
    pub(crate) fn provider_ref(&self) -> String {
        provider_ref(&self.base_provider, &self.id)
    }

    fn summary_json(&self) -> Value {
        json!({
            "id": self.id,
            "providerRef": self.provider_ref(),
            "baseProviderId": self.base_provider,
            "name": self.name,
            "backup": self.backup,
        })
    }
}

/// One base provider's Default configuration overrides (PROTOCOL §5.54
/// "Default configuration overrides"). `name` is present only for a renamed
/// Default; `env_id` only while environment entries are saved. It names the
/// secret-store document (`providerProfiles.<envId>.env`), minted like a
/// profile id so a daemon sharing the secret store never reads or overwrites
/// another daemon's Default environment.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DefaultMeta {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) env_id: Option<String>,
}

impl DefaultMeta {
    fn is_empty(&self) -> bool {
        self.name.is_none() && self.env_id.is_none()
    }
}

/// Default overrides keyed by base provider id (only non-empty entries).
pub(crate) type Defaults = std::collections::BTreeMap<String, DefaultMeta>;

/// The display name of an unrenamed Default configuration.
pub(crate) const DEFAULT_NAME: &str = "Default";

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ProfilesFile {
    version: u32,
    profiles: Vec<ProfileMeta>,
    /// Default configuration overrides; omitted when none is saved, so the
    /// steady-state file keeps its original shape. An older daemon ignores
    /// it and drops it on its next write.
    #[serde(default, skip_serializing_if = "Defaults::is_empty")]
    defaults: Defaults,
    /// Ids whose secret may exist without metadata (see module docs).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pending_env_cleanup: Vec<String>,
}

/// One saved environment entry. Deliberately not `Debug`: it carries values.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub(crate) enum StoredEnv {
    Set { key: String, value: String },
    Unset { key: String },
}

impl StoredEnv {
    pub(crate) fn key(&self) -> &str {
        match self {
            Self::Set { key, .. } | Self::Unset { key } => key,
        }
    }

    fn masked_json(&self) -> Value {
        match self {
            Self::Set { key, value } => json!({
                "key": key,
                "kind": "set",
                "value": if value.is_empty() { "" } else { MASK },
            }),
            Self::Unset { key } => json!({ "key": key, "kind": "unset" }),
        }
    }
}

/// The secret-store document for one profile. Not `Debug` (carries values).
#[derive(Clone, Default, Serialize, Deserialize)]
pub(crate) struct EnvDoc {
    /// Opaque token regenerated whenever the saved entries change.
    pub(crate) fingerprint: String,
    pub(crate) entries: Vec<StoredEnv>,
}

impl EnvDoc {
    fn fresh(entries: Vec<StoredEnv>) -> Self {
        Self {
            fingerprint: format!("f{}", random_token(16)),
            entries,
        }
    }

    fn encode(&self) -> Result<String> {
        serde_json::to_string(self)
            .map_err(|e| Error::Internal(format!("encode provider profile env failed: {e}")))
    }
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// One `fieldErrors` row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FieldError {
    pub(crate) field: String,
    pub(crate) code: &'static str,
    pub(crate) message: String,
}

impl FieldError {
    fn new(field: impl Into<String>, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            code,
            message: message.into(),
        }
    }
}

fn field_errors(errors: &[FieldError]) -> Error {
    let rows: Vec<Value> = errors
        .iter()
        .map(|e| json!({ "field": e.field, "code": e.code, "message": e.message }))
        .collect();
    Error::provider_profile(
        false,
        "invalid params: provider profile validation failed",
        json!({ "code": "invalid-params", "fieldErrors": rows }),
    )
}

fn unreadable() -> Error {
    Error::provider_profile(
        true,
        "provider profiles file is unreadable; fix or remove it and restart intentd",
        json!({ "code": "provider-profiles-unreadable" }),
    )
}

fn profile_not_found(profile_id: &str) -> Error {
    Error::provider_profile(
        false,
        format!("not found: provider profile {profile_id}"),
        json!({ "code": "not-found" }),
    )
}

/// A qualified reference names no usable saved profile. Execution never falls
/// back to the base provider's Default configuration.
pub(crate) fn replacement_required(reference: &str, reason: &str) -> Error {
    Error::provider_profile(
        false,
        // The reference is in `data.providerRef`; the prose stays readable.
        format!(
            "invalid params: {}; choose another model to continue",
            if reason == "provider-disabled" {
                "the selected profile's provider is disabled"
            } else {
                "the selected provider profile is no longer available"
            }
        ),
        json!({
            "code": "provider-profile-replacement-required",
            "providerRef": reference,
            "reason": reason,
        }),
    )
}

/// `settings.update` named a profile reference with no saved profile.
pub(crate) fn reference_not_found(path: &str, reference: &str) -> Error {
    Error::provider_profile(
        false,
        format!("invalid params: {path} names no saved provider profile: {reference}"),
        json!({ "code": "provider-profile-not-found", "path": path, "providerRef": reference }),
    )
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

/// A parsed `EnvEntryWrite`. Not `Debug` (carries values).
#[derive(Clone)]
pub(crate) enum EnvWrite {
    Set { key: String, value: String },
    Unset { key: String },
    Keep { key: String },
}

impl EnvWrite {
    fn key(&self) -> &str {
        match self {
            Self::Set { key, .. } | Self::Unset { key } | Self::Keep { key } => key,
        }
    }
}

/// Parse the wire `env` array. Structural problems (not an array, an entry
/// that is not an object, missing/non-string `key`, unknown `kind`, a `set`
/// without a string `value`) are malformed requests → plain `-32602`.
pub(crate) fn parse_env_writes(env: &Value) -> Result<Vec<EnvWrite>> {
    let entries = env
        .as_array()
        .ok_or_else(|| Error::InvalidParams("env must be an array".into()))?;
    entries
        .iter()
        .enumerate()
        .map(|(i, entry)| {
            let obj = entry
                .as_object()
                .ok_or_else(|| Error::InvalidParams(format!("env[{i}] must be an object")))?;
            let key = obj
                .get("key")
                .and_then(Value::as_str)
                .ok_or_else(|| Error::InvalidParams(format!("env[{i}].key must be a string")))?
                .to_string();
            match obj.get("kind").and_then(Value::as_str) {
                Some("set") => {
                    let value = obj.get("value").and_then(Value::as_str).ok_or_else(|| {
                        Error::InvalidParams(format!("env[{i}].value must be a string"))
                    })?;
                    Ok(EnvWrite::Set {
                        key,
                        value: value.to_string(),
                    })
                }
                Some("unset") => Ok(EnvWrite::Unset { key }),
                Some("keep") => Ok(EnvWrite::Keep { key }),
                _ => Err(Error::InvalidParams(format!(
                    "env[{i}].kind must be \"set\", \"unset\" or \"keep\""
                ))),
            }
        })
        .collect()
}

/// Keys Intent writes into provider launches as session wiring, built from the
/// same constants the launch code injects with: git commit identity and the
/// credential-helper `GIT_CONFIG_PARAMETERS` (every provider), and the Codex
/// permission mode. A profile cannot set or unset them.
fn launch_wired_keys(base_provider: &str) -> impl Iterator<Item = &'static str> {
    intent_git::identity::GIT_IDENTITY_ENV_VARS
        .into_iter()
        .chain([intent_git::auth::GIT_CONFIG_PARAMETERS_ENV])
        .chain((base_provider == "codex").then_some(intent_providers::CODEX_INITIAL_AGENT_MODE_ENV))
}

/// Why a key is reserved, or `None` when the key may be saved.
pub(crate) fn reserved_key_reason(key: &str, base_provider: &str) -> Option<String> {
    let upper = key.to_ascii_uppercase();
    let lower = key.to_ascii_lowercase();
    let account_var = if base_provider == "claude-code" {
        "CLAUDE_CONFIG_DIR"
    } else {
        "CODEX_HOME"
    };
    let reason = match upper.as_str() {
        "PATH" => "Intent rewrites PATH at launch with the enhanced executable path".to_string(),
        "CODEX_PATH" | "CLAUDE_CODE_EXECUTABLE" => {
            "Intent selects the installed executable through this variable".to_string()
        }
        "CODEX_CONFIG" => "Intent sets the enforced Codex policy after every env merge".to_string(),
        "NODE_OPTIONS" => "Intent composes the adapter memory cap in NODE_OPTIONS".to_string(),
        "HOME" | "USERPROFILE" => format!(
            "Intent reads {upper} to find the shared installation; use {account_var} to select an account"
        ),
        _ if launch_wired_keys(base_provider).any(|k| k.eq_ignore_ascii_case(key)) => {
            "Intent sets this at launch (session wiring and commit identity)".to_string()
        }
        _ if upper.starts_with("INTENT_") || upper.starts_with("INTENTD_") => {
            "Intent owns its session and bootstrap variables".to_string()
        }
        _ if lower.starts_with("npm_config_workspace")
            || lower.starts_with("npm_workspace")
            || lower == "npm_config_include_workspace_root" =>
        {
            "Intent removes npm workspace variables from adapter launches".to_string()
        }
        _ => return None,
    };
    Some(format!("{key} is reserved: {reason}"))
}

fn is_valid_key(key: &str) -> bool {
    let bytes = key.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= MAX_KEY_CHARS
        && (bytes[0].is_ascii_alphabetic() || bytes[0] == b'_')
        && bytes
            .iter()
            .all(|b| b.is_ascii_alphanumeric() || *b == b'_')
}

/// Expand a leading `~` in an account-directory value at resolution time
/// against the daemon user's home ([`intent_acp::profile_env::expand_account_dir`]).
/// The saved value is never rewritten.
pub(crate) fn expand_account_dir(key: &str, value: &str, home: Option<&Path>) -> String {
    intent_acp::profile_env::expand_account_dir(key, value, home)
}

/// The daemon user's home directory, as the installed-CLI context resolves
/// it for the Default configuration (`HOME`, else `USERPROFILE`).
fn daemon_home() -> Option<PathBuf> {
    std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .filter(|h| !h.is_empty())
        .map(PathBuf::from)
}

/// Case-insensitive name comparison key (per-char lowercase, an approximation
/// of Unicode simple case folding).
fn fold_name(name: &str) -> String {
    name.chars().flat_map(char::to_lowercase).collect()
}

/// Whether a Default configuration name is not a rename: blank, or
/// `Default` ignoring case (after trimming).
pub(crate) fn is_unrenamed_default_name(name: &str) -> bool {
    let name = name.trim();
    name.is_empty() || fold_name(name) == fold_name(DEFAULT_NAME)
}

/// The shared shape rule for every display name: no control characters and
/// at most [`MAX_NAME_CHARS`] after trimming.
fn check_name_shape(name: &str, errors: &mut Vec<FieldError>) {
    if name.chars().any(char::is_control) || name.chars().count() > MAX_NAME_CHARS {
        errors.push(FieldError::new(
            "name",
            "name-invalid",
            format!("Use at most {MAX_NAME_CHARS} characters and no control characters."),
        ));
    }
}

/// Whether another profile of `base_provider` (not `self_id`) uses `name`,
/// ignoring case.
fn profile_name_taken(
    name: &str,
    base_provider: &str,
    existing: &[ProfileMeta],
    self_id: Option<&str>,
) -> bool {
    let folded = fold_name(name);
    existing.iter().any(|p| {
        p.base_provider == base_provider
            && Some(p.id.as_str()) != self_id
            && fold_name(&p.name) == folded
    })
}

/// Validate a display name; returns the trimmed name.
pub(crate) fn validate_name(
    raw: &str,
    base_provider: &str,
    existing: &[ProfileMeta],
    self_id: Option<&str>,
    errors: &mut Vec<FieldError>,
) -> String {
    let name = raw.trim().to_string();
    if name.is_empty() {
        errors.push(FieldError::new(
            "name",
            "name-blank",
            "Enter a profile name.",
        ));
        return name;
    }
    check_name_shape(&name, errors);
    if fold_name(&name) == fold_name(DEFAULT_NAME) {
        errors.push(FieldError::new(
            "name",
            "name-reserved",
            "Default names the original configuration; choose another name.",
        ));
    } else if profile_name_taken(&name, base_provider, existing, self_id) {
        errors.push(FieldError::new(
            "name",
            "name-duplicate",
            "Another profile for this provider already uses this name.",
        ));
    }
    name
}

/// A profile name that collides with its base provider's renamed Default
/// (ignoring case) is a duplicate: both label configurations of one provider.
pub(crate) fn check_default_name_conflict(
    name: &str,
    default_name: Option<&str>,
    errors: &mut Vec<FieldError>,
) {
    if default_name.is_some_and(|d| fold_name(d) == fold_name(name))
        && !errors.iter().any(|e| e.field == "name")
    {
        errors.push(FieldError::new(
            "name",
            "name-duplicate",
            "The Default configuration of this provider already uses this name.",
        ));
    }
}

/// Validate a Default configuration name (`updateDefault`). Returns `None`
/// for a name that is not a rename (blank, or `Default` ignoring case),
/// otherwise the trimmed name.
pub(crate) fn validate_default_name(
    raw: &str,
    base_provider: &str,
    existing: &[ProfileMeta],
    errors: &mut Vec<FieldError>,
) -> Option<String> {
    if is_unrenamed_default_name(raw) {
        return None;
    }
    let name = raw.trim().to_string();
    check_name_shape(&name, errors);
    if profile_name_taken(&name, base_provider, existing, None) {
        errors.push(FieldError::new(
            "name",
            "name-duplicate",
            "A profile for this provider already uses this name.",
        ));
    }
    Some(name)
}

/// Validate env writes against the saved entries (`None` on create) and
/// resolve them into the new stored list.
pub(crate) fn validate_env(
    writes: &[EnvWrite],
    base_provider: &str,
    saved: Option<&[StoredEnv]>,
    errors: &mut Vec<FieldError>,
) -> Vec<StoredEnv> {
    if writes.len() > MAX_ENV_ENTRIES {
        errors.push(FieldError::new(
            "env",
            "env-limit",
            format!("A profile can have at most {MAX_ENV_ENTRIES} environment entries."),
        ));
    }
    let mut seen: Vec<String> = Vec::with_capacity(writes.len());
    let mut resolved = Vec::with_capacity(writes.len());
    for (i, write) in writes.iter().enumerate() {
        let key = write.key();
        let key_field = format!("env[{i}].key");
        if is_valid_key(key) {
            let folded = key.to_ascii_uppercase();
            if seen.contains(&folded) {
                errors.push(FieldError::new(
                    &key_field,
                    "env-key-duplicate",
                    format!("{key} appears more than once (keys ignore case)."),
                ));
            }
            seen.push(folded);
            if let Some(reason) = reserved_key_reason(key, base_provider) {
                errors.push(FieldError::new(&key_field, "env-key-reserved", reason));
            }
        } else {
            errors.push(FieldError::new(
                &key_field,
                "env-key-invalid",
                "Use letters, digits and underscores, starting with a letter or underscore (at most 128).",
            ));
        }
        match write {
            EnvWrite::Set { key, value } => {
                if value.contains('\0') || value.len() > MAX_VALUE_BYTES {
                    errors.push(FieldError::new(
                        format!("env[{i}].value"),
                        "env-value-invalid",
                        "Values cannot contain NUL characters or exceed 32 KiB.",
                    ));
                } else if value == MASK {
                    errors.push(FieldError::new(
                        format!("env[{i}].value"),
                        "env-value-placeholder",
                        "The masked placeholder cannot be saved; keep the saved value instead.",
                    ));
                }
                resolved.push(StoredEnv::Set {
                    key: key.clone(),
                    value: value.clone(),
                });
            }
            EnvWrite::Unset { key } => resolved.push(StoredEnv::Unset { key: key.clone() }),
            EnvWrite::Keep { key } => {
                let previous = saved
                    .and_then(|entries| entries.iter().find(|e| e.key().eq_ignore_ascii_case(key)));
                match previous {
                    Some(StoredEnv::Set { value, .. }) => resolved.push(StoredEnv::Set {
                        key: key.clone(),
                        value: value.clone(),
                    }),
                    Some(StoredEnv::Unset { .. }) => {
                        resolved.push(StoredEnv::Unset { key: key.clone() });
                    }
                    None => errors.push(FieldError::new(
                        format!("env[{i}].kind"),
                        "env-keep-missing",
                        if saved.is_some() {
                            format!("{key} has no saved entry to keep.")
                        } else {
                            "keep is only valid when updating a saved entry.".to_string()
                        },
                    )),
                }
            }
        }
    }
    resolved
}

// ---------------------------------------------------------------------------
// Store
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct Loaded {
    /// `None` when the file is malformed or has an unknown version.
    profiles: Option<Arc<Vec<ProfileMeta>>>,
    /// Default configuration overrides (meaningful only while readable).
    defaults: Arc<Defaults>,
    pending: Vec<String>,
}

/// Metadata store + mutation lock + process-local revision watermark.
pub(crate) struct ProviderProfileStore {
    /// `None` keeps metadata in memory only (unit/read-only wiring).
    path: Option<PathBuf>,
    loaded: RwLock<Loaded>,
    mutation: tokio::sync::Mutex<()>,
    revision: AtomicU64,
}

impl Default for ProviderProfileStore {
    fn default() -> Self {
        Self::in_memory()
    }
}

impl ProviderProfileStore {
    pub(crate) fn in_memory() -> Self {
        Self {
            path: None,
            loaded: RwLock::new(Loaded {
                profiles: Some(Arc::new(Vec::new())),
                defaults: Arc::default(),
                pending: Vec::new(),
            }),
            mutation: tokio::sync::Mutex::new(()),
            revision: AtomicU64::new(0),
        }
    }

    /// Read `<data_dir>/provider-profiles.json`. A missing file is an empty
    /// store; a malformed one leaves the file untouched and the store
    /// unreadable (the daemon still starts).
    pub(crate) fn load(data_dir: &Path) -> Self {
        let path = data_dir.join(PROFILES_FILE);
        let loaded = match std::fs::read(&path) {
            Ok(bytes) => {
                if let Some(file) = parse_file(&bytes) {
                    Loaded {
                        profiles: Some(Arc::new(file.profiles)),
                        defaults: Arc::new(file.defaults),
                        pending: file.pending_env_cleanup,
                    }
                } else {
                    tracing::warn!(path = %path.display(), "provider profiles file is malformed or has an unknown version; profiles are unavailable until it is fixed");
                    Loaded {
                        profiles: None,
                        defaults: Arc::default(),
                        pending: Vec::new(),
                    }
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Loaded {
                profiles: Some(Arc::new(Vec::new())),
                defaults: Arc::default(),
                pending: Vec::new(),
            },
            Err(e) => {
                tracing::warn!(path = %path.display(), error = %e, "provider profiles file is unreadable");
                Loaded {
                    profiles: None,
                    defaults: Arc::default(),
                    pending: Vec::new(),
                }
            }
        };
        Self {
            path: Some(path),
            loaded: RwLock::new(loaded),
            mutation: tokio::sync::Mutex::new(()),
            revision: AtomicU64::new(0),
        }
    }

    /// Current revision watermark.
    pub(crate) fn revision(&self) -> u64 {
        self.revision.load(Ordering::SeqCst)
    }

    pub(crate) fn bump_revision(&self) -> u64 {
        self.revision.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// Saved profiles, or the unreadable error.
    pub(crate) fn profiles(&self) -> Result<Arc<Vec<ProfileMeta>>> {
        self.loaded
            .read()
            .unwrap()
            .profiles
            .clone()
            .ok_or_else(unreadable)
    }

    /// Whether `reference` names a saved profile (false while unreadable).
    pub(crate) fn has_reference(&self, reference: &str) -> bool {
        let Some((base, id)) = parse_profile_ref(reference) else {
            return false;
        };
        self.loaded
            .read()
            .unwrap()
            .profiles
            .as_ref()
            .is_some_and(|p| p.iter().any(|m| m.id == id && m.base_provider == base))
    }

    /// Saved Default configuration overrides, or the unreadable error.
    pub(crate) fn defaults(&self) -> Result<Arc<Defaults>> {
        let loaded = self.loaded.read().unwrap();
        loaded.profiles.as_ref().ok_or_else(unreadable)?;
        Ok(loaded.defaults.clone())
    }

    /// The renamed Default of `base`, if any (`None` while unreadable).
    pub(crate) fn default_name(&self, base: &str) -> Option<String> {
        let loaded = self.loaded.read().unwrap();
        loaded.profiles.as_ref()?;
        loaded.defaults.get(base).and_then(|d| d.name.clone())
    }

    pub(crate) async fn lock(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.mutation.lock().await
    }

    fn pending(&self) -> Vec<String> {
        self.loaded.read().unwrap().pending.clone()
    }

    /// Atomically persist `profiles` + `pending`, then publish the snapshot.
    /// Callers hold the mutation lock.
    fn persist(&self, profiles: Vec<ProfileMeta>, pending: Vec<String>) -> Result<()> {
        let defaults = self.loaded.read().unwrap().defaults.as_ref().clone();
        self.persist_with_defaults(profiles, defaults, pending)
    }

    /// [`Self::persist`] that also replaces the Default overrides. Empty
    /// entries are dropped so the file only records real overrides.
    fn persist_with_defaults(
        &self,
        profiles: Vec<ProfileMeta>,
        mut defaults: Defaults,
        pending: Vec<String>,
    ) -> Result<()> {
        defaults.retain(|_, d| !d.is_empty());
        if let Some(path) = &self.path {
            let file = ProfilesFile {
                version: FILE_VERSION,
                profiles: profiles.clone(),
                defaults: defaults.clone(),
                pending_env_cleanup: pending.clone(),
            };
            let bytes = serde_json::to_vec_pretty(&file)
                .map_err(|e| Error::Internal(format!("encode provider profiles failed: {e}")))?;
            write_atomic(path, &bytes)?;
        }
        *self.loaded.write().unwrap() = Loaded {
            profiles: Some(Arc::new(profiles)),
            defaults: Arc::new(defaults),
            pending,
        };
        Ok(())
    }
}

fn parse_file(bytes: &[u8]) -> Option<ProfilesFile> {
    let mut file: ProfilesFile = serde_json::from_slice(bytes).ok()?;
    // A blank or `Default` name is not a rename (hand edits included).
    for default in file.defaults.values_mut() {
        default.name = default
            .name
            .take()
            .filter(|n| !is_unrenamed_default_name(n))
            .map(|n| n.trim().to_owned());
    }
    // An entry for a base this daemon does not know (a newer daemon's) is
    // ignored rather than making every profile unreadable.
    file.defaults
        .retain(|base, d| !d.is_empty() && PROFILE_BASE_PROVIDERS.contains(&base.as_str()));
    if file.version != FILE_VERSION {
        return None;
    }
    let mut ids = std::collections::HashSet::new();
    let well_formed = file.profiles.iter().all(|p| {
        is_valid_profile_id(&p.id)
            && PROFILE_BASE_PROVIDERS.contains(&p.base_provider.as_str())
            && ids.insert(p.id.clone())
    }) && file
        .pending_env_cleanup
        .iter()
        .all(|id| is_valid_profile_id(id))
        && file.defaults.values().all(|d| {
            d.env_id
                .as_ref()
                .is_none_or(|id| is_valid_profile_id(id) && ids.insert(id.clone()))
        });
    well_formed.then_some(file)
}

/// Temp file in the same directory (mode `0600`, fsynced), then rename.
fn write_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(parent)
        .map_err(|e| Error::Internal(format!("create provider profiles dir failed: {e}")))?;
    let tmp = parent.join(format!(
        ".{PROFILES_FILE}.tmp-{}-{}",
        std::process::id(),
        uuid::Uuid::new_v4().simple()
    ));
    intent_core::write_private(&tmp, bytes).map_err(|e| {
        let _ = std::fs::remove_file(&tmp);
        Error::Internal(format!("write provider profiles failed: {e}"))
    })?;
    std::fs::rename(&tmp, path).map_err(|e| {
        let _ = std::fs::remove_file(&tmp);
        Error::Internal(format!("persist provider profiles failed: {e}"))
    })?;
    #[cfg(unix)]
    if let Ok(dir) = std::fs::File::open(parent) {
        let _ = dir.sync_all();
    }
    Ok(())
}

async fn load_env(secrets: &AsyncSecretStore, profile_id: &str) -> Result<Option<EnvDoc>> {
    match secrets.load_fresh(&env_account(profile_id)).await? {
        None => Ok(None),
        Some(raw) => serde_json::from_str(&raw).map(Some).map_err(|_| {
            Error::Internal(format!(
                "provider profile {profile_id} environment is unreadable"
            ))
        }),
    }
}

fn profile_json(meta: &ProfileMeta, env: &EnvDoc, running_processes: usize) -> Value {
    json!({
        "id": meta.id,
        "providerRef": meta.provider_ref(),
        "baseProviderId": meta.base_provider,
        "name": meta.name,
        "backup": meta.backup,
        "env": env.entries.iter().map(StoredEnv::masked_json).collect::<Vec<_>>(),
        "settingsFingerprint": env.fingerprint,
        "createdAt": meta.created_at,
        "updatedAt": meta.updated_at,
        "activation": { "runningProcesses": running_processes },
    })
}

/// The wire `DefaultConfig` of one base provider (masked; no values).
fn default_config_json(base: &str, name: Option<&str>, env: &EnvDoc, running: usize) -> Value {
    json!({
        "baseProviderId": base,
        "providerRef": base,
        "name": name.unwrap_or(DEFAULT_NAME),
        "env": env.entries.iter().map(StoredEnv::masked_json).collect::<Vec<_>>(),
        "settingsFingerprint": env.fingerprint,
        "activation": { "runningProcesses": running },
    })
}

/// Prefix of a Default configuration's activation-registry key. The `:`
/// keeps it disjoint from every profile id.
const DEFAULT_ACTIVATION_PREFIX: &str = "default:";

/// The activation-registry key of a base provider's Default configuration.
pub(crate) fn default_activation_key(base: &str) -> String {
    format!("{DEFAULT_ACTIVATION_PREFIX}{base}")
}

/// The base provider of a Default configuration's activation key, or `None`
/// for a profile id.
pub(crate) fn parse_default_activation_key(key: &str) -> Option<&str> {
    key.strip_prefix(DEFAULT_ACTIVATION_PREFIX)
}

/// `change: "default-updated"` / `"default-activation"`: carries the base
/// provider, never a profile id, summary or environment.
fn default_changed_event(
    revision: u64,
    change: &str,
    base_provider: &str,
    env_changed: Option<bool>,
) -> NewEvent {
    profiles_changed_event_with(
        json!({ "revision": revision, "change": change, "baseProviderId": base_provider }),
        None,
        env_changed,
    )
}

fn profiles_changed_event(
    revision: u64,
    change: &str,
    profile_id: &str,
    summary: Option<Value>,
    env_changed: Option<bool>,
) -> NewEvent {
    profiles_changed_event_with(
        json!({ "revision": revision, "change": change, "profileId": profile_id }),
        summary,
        env_changed,
    )
}

/// `change: "reordered"`: the order of one base provider's profiles moved.
/// No single profile changed, so there is no `profileId` or `profile`.
fn profiles_reordered_event(revision: u64, base_provider: &str) -> NewEvent {
    profiles_changed_event_with(
        json!({ "revision": revision, "change": "reordered", "baseProviderId": base_provider }),
        None,
        None,
    )
}

fn profiles_changed_event_with(
    mut data: Value,
    summary: Option<Value>,
    env_changed: Option<bool>,
) -> NewEvent {
    if let Some(summary) = summary {
        data["profile"] = summary;
    }
    if let Some(env_changed) = env_changed {
        data["envChanged"] = json!(env_changed);
    }
    NewEvent {
        workspace_id: intent_core::WorkspaceId::from_string(String::new()),
        timestamp: intent_core::now_iso(),
        event_type: intent_core::events::PROVIDERS_PROFILES_CHANGED.to_string(),
        actor: crate::system_actor(),
        session_id: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data,
    }
}

fn opt_string(params: &Value, key: &str) -> Result<Option<String>> {
    match params.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(s)) => Ok(Some(s.clone())),
        Some(_) => Err(Error::InvalidParams(format!("{key} must be a string"))),
    }
}

/// An optional boolean member: absent is `None`; any other non-boolean,
/// explicit `null` included, is a malformed request (§5.54).
fn opt_bool(params: &Value, key: &str) -> Result<Option<bool>> {
    match params.get(key) {
        None => Ok(None),
        Some(Value::Bool(b)) => Ok(Some(*b)),
        Some(_) => Err(Error::InvalidParams(format!("{key} must be a boolean"))),
    }
}

/// Parse `providers.profiles.reorder` `profileIds`: an array of strings.
/// Anything else is a malformed request → plain `-32602`.
fn parse_profile_ids(params: &Value) -> Result<Vec<String>> {
    let ids = params
        .get("profileIds")
        .ok_or_else(|| Error::InvalidParams("missing required param: profileIds".into()))?
        .as_array()
        .ok_or_else(|| Error::InvalidParams("profileIds must be an array".into()))?;
    ids.iter()
        .enumerate()
        .map(|(i, id)| {
            id.as_str()
                .map(str::to_owned)
                .ok_or_else(|| Error::InvalidParams(format!("profileIds[{i}] must be a string")))
        })
        .collect()
}

/// Validate a reorder request against the saved profiles: `profile_ids`
/// must list every saved profile of `base` exactly once and nothing else.
/// Returns the new saved list, with `base`'s profiles placed in the
/// requested order into the slots they already occupy (other bases' rows
/// keep their positions).
pub(crate) fn reorder_profiles(
    profiles: &[ProfileMeta],
    base: &str,
    profile_ids: &[String],
) -> std::result::Result<Vec<ProfileMeta>, Vec<FieldError>> {
    let mut errors = Vec::new();
    if !PROFILE_BASE_PROVIDERS.contains(&base) {
        errors.push(FieldError::new(
            "baseProviderId",
            "base-provider-unsupported",
            format!("{base} does not support profiles."),
        ));
        return Err(errors);
    }
    let mut seen = std::collections::HashSet::new();
    for (i, id) in profile_ids.iter().enumerate() {
        let field = format!("profileIds[{i}]");
        if !seen.insert(id.as_str()) {
            errors.push(FieldError::new(
                field,
                "profile-order-duplicate",
                format!("{id} appears more than once."),
            ));
        } else if !profiles
            .iter()
            .any(|p| p.id == *id && p.base_provider == base)
        {
            errors.push(FieldError::new(
                field,
                "profile-order-unknown",
                format!("{id} is not a saved {base} profile."),
            ));
        }
    }
    let missing = profiles
        .iter()
        .filter(|p| p.base_provider == base && !seen.contains(p.id.as_str()))
        .count();
    if missing > 0 {
        errors.push(FieldError::new(
            "profileIds",
            "profile-order-incomplete",
            format!("List every saved {base} profile exactly once ({missing} missing)."),
        ));
    }
    if !errors.is_empty() {
        return Err(errors);
    }
    let mut ordered = profile_ids.iter().map(|id| {
        profiles
            .iter()
            .find(|p| p.id == *id)
            .cloned()
            .expect("validated id")
    });
    Ok(profiles
        .iter()
        .map(|p| {
            if p.base_provider == base {
                ordered.next().expect("validated permutation")
            } else {
                p.clone()
            }
        })
        .collect())
}

fn opt_env(params: &Value) -> Result<Option<Vec<EnvWrite>>> {
    match params.get("env") {
        None | Some(Value::Null) => Ok(None),
        Some(env) => parse_env_writes(env).map(Some),
    }
}

/// Saved settings that explicitly select `reference` (delete blocking).
pub(crate) fn in_use_reasons(
    reference: &str,
    default_provider: Option<&str>,
    specialists: &[Value],
) -> Vec<Value> {
    let mut reasons = Vec::new();
    if default_provider.map(str::trim) == Some(reference) {
        reasons.push(json!({ "kind": "defaultProvider" }));
    }
    for def in specialists {
        let id = def.get("id").and_then(Value::as_str).unwrap_or_default();
        let source = def
            .get("source")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if def
            .get("codingAgent")
            .and_then(Value::as_str)
            .map(str::trim)
            == Some(reference)
        {
            reasons.push(
                json!({ "kind": "specialistProvider", "specialistId": id, "source": source }),
            );
        }
        if let Some(options) = def.get("modelOptions").and_then(Value::as_array) {
            for (index, option) in options.iter().enumerate() {
                if option
                    .get("provider")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    == Some(reference)
                {
                    reasons.push(json!({
                        "kind": "specialistModelOption",
                        "specialistId": id,
                        "source": source,
                        "index": index,
                    }));
                }
            }
        }
    }
    reasons
}

/// Reject `settings.update` references to missing profiles (§5.54
/// "References in settings"): `model.defaultProvider`, and
/// `model.providerDefaults` / `providers.fastMode` keys that are new or whose
/// value changed versus `stored_defaults` / `stored_fast_mode`.
pub(crate) fn validate_settings_references(
    store: &ProviderProfileStore,
    changes: &Value,
    stored_defaults: &std::collections::BTreeMap<String, String>,
    stored_fast_mode: &std::collections::BTreeMap<String, bool>,
) -> Result<()> {
    let Some(entries) = changes.as_array() else {
        return Ok(());
    };
    for entry in entries {
        match entry.get("path").and_then(Value::as_str) {
            Some(path @ "model.defaultProvider") => {
                if let Some(reference) = entry.get("value").and_then(Value::as_str).map(str::trim) {
                    if is_qualified_ref(reference) && !store.has_reference(reference) {
                        return Err(reference_not_found(path, reference));
                    }
                }
            }
            Some(path @ "model.providerDefaults") => {
                let Some(map) = entry.get("value").and_then(Value::as_object) else {
                    continue;
                };
                for (key, value) in map {
                    if !is_qualified_ref(key) {
                        continue;
                    }
                    let unchanged = stored_defaults
                        .get(key)
                        .is_some_and(|stored| Some(stored.as_str()) == value.as_str());
                    if !unchanged && !store.has_reference(key) {
                        return Err(reference_not_found(path, key));
                    }
                }
            }
            Some(path @ "providers.fastMode") => {
                let Some(map) = entry.get("value").and_then(Value::as_object) else {
                    continue;
                };
                for (key, value) in map {
                    if !is_qualified_ref(key) {
                        continue;
                    }
                    let unchanged = stored_fast_mode
                        .get(key)
                        .is_some_and(|stored| Some(*stored) == value.as_bool());
                    if !unchanged && !store.has_reference(key) {
                        return Err(reference_not_found(path, key));
                    }
                }
            }
            _ => {}
        }
    }
    Ok(())
}

/// Whether a settings batch may carry a profile reference (and therefore
/// must serialize with profile mutations).
pub(crate) fn batch_has_references(changes: &Value) -> bool {
    changes.as_array().into_iter().flatten().any(|entry| {
        match entry.get("path").and_then(Value::as_str) {
            Some("model.defaultProvider") => entry
                .get("value")
                .and_then(Value::as_str)
                .is_some_and(is_qualified_ref),
            Some("model.providerDefaults" | "providers.fastMode") => entry
                .get("value")
                .and_then(Value::as_object)
                .is_some_and(|m| m.keys().any(|k| is_qualified_ref(k))),
            _ => false,
        }
    })
}

/// Whether a recorded native-session binding (`None` = never recorded)
/// matches `fingerprint`: an absent binding counts as `""`, the fingerprint
/// of a Default configuration with no saved overrides.
pub(crate) fn binding_matches(binding: Option<&str>, fingerprint: &str) -> bool {
    binding.unwrap_or("") == fingerprint
}

/// The recorded native-session settings binding of `stored`, if any.
fn native_session_binding(stored: &intent_core::AgentSession) -> Option<&str> {
    stored
        .metadata
        .as_ref()
        .and_then(|m| m.get(NATIVE_SESSION_SETTINGS_KEY))
        .and_then(Value::as_str)
}

/// Build the launch overlay of a saved environment document. Reserved keys
/// are dropped even if the stored document was edited outside the daemon,
/// and `~` in an account directory expands against `home` here, before
/// account-home lookup, isolation and fingerprinting, so every path agrees.
/// `None` keeps values as typed (a remote node expands against its own home).
fn overlay_from(
    reference: &str,
    base: &str,
    doc: EnvDoc,
    home: Option<&Path>,
) -> intent_acp::ProfileEnv {
    let entries = doc
        .entries
        .into_iter()
        .filter(|e| reserved_key_reason(e.key(), base).is_none())
        .map(|e| match e {
            StoredEnv::Set { key, value } => {
                let value = expand_account_dir(&key, &value, home);
                (key, Some(value))
            }
            StoredEnv::Unset { key } => (key, None),
        })
        .collect();
    intent_acp::ProfileEnv::new(reference, doc.fingerprint, entries)
}

// ---------------------------------------------------------------------------
// Service surface
// ---------------------------------------------------------------------------

impl Services {
    /// Remove secrets journaled by an interrupted create/delete whose
    /// metadata never landed (or was removed). Called once at startup.
    pub async fn sweep_provider_profile_secrets(&self) {
        let store = &self.provider_profiles;
        let _guard = store.lock().await;
        let pending = store.pending();
        if pending.is_empty() {
            return;
        }
        let Ok(profiles) = store.profiles() else {
            return;
        };
        let defaults = store.defaults().unwrap_or_default();
        let mut remaining = Vec::new();
        for id in pending {
            if profiles.iter().any(|p| p.id == id)
                || defaults
                    .values()
                    .any(|d| d.env_id.as_deref() == Some(id.as_str()))
            {
                continue;
            }
            if let Err(e) = self.secrets.delete(&env_account(&id)).await {
                tracing::warn!(profile_id = %id, error = %e, "provider profile secret cleanup failed; will retry at next start");
                remaining.push(id);
            }
        }
        if let Err(e) = store.persist(profiles.as_ref().clone(), remaining) {
            tracing::warn!(error = %e, "provider profile cleanup journal update failed");
        }
    }

    /// Resolve the provider reference a process or probe is about to use into
    /// its saved overlay. A bare id is the Default configuration (`None`). A
    /// qualified reference that is malformed, unknown, deleted or unreadable
    /// fails with `provider-profile-replacement-required`; an unreadable
    /// environment document fails closed. Reserved keys are dropped even if
    /// the stored document was edited outside the daemon. Values are read
    /// fresh, so a newly started process always uses the saved settings.
    pub(crate) async fn resolve_profile_env(
        &self,
        reference: &str,
    ) -> Result<Option<intent_acp::ProfileEnv>> {
        self.resolve_profile_env_for(reference, daemon_home().as_deref())
            .await
    }

    /// The overlay a remote node launch for `reference` must run with, under
    /// the same selection gate as a local launch (a missing profile or a
    /// disabled base fails with `provider-profile-replacement-required` and
    /// never falls back). `None` means no overrides: the node uses its own
    /// login. Account directories are returned as typed; the node expands a
    /// leading `~` against its own home ([`intent_acp::ProfileEnv::for_node`]).
    ///
    /// # Errors
    /// Replacement-required for an unselectable reference; an unreadable
    /// environment document fails closed.
    pub async fn resolve_remote_profile_env(
        &self,
        reference: &str,
    ) -> Result<Option<intent_acp::ProfileEnv>> {
        self.ensure_provider_ref_selectable(reference)?;
        self.resolve_profile_env_for(reference, None).await
    }

    async fn resolve_profile_env_for(
        &self,
        reference: &str,
        home: Option<&Path>,
    ) -> Result<Option<intent_acp::ProfileEnv>> {
        if !is_qualified_ref(reference) {
            return self.resolve_default_env(reference, home).await;
        }
        let missing = || replacement_required(reference, "profile-missing");
        let (base, id) = parse_profile_ref(reference).ok_or_else(missing)?;
        let known = self
            .provider_profiles
            .profiles()
            .map_err(|_| missing())?
            .iter()
            .any(|p| p.id == id && p.base_provider == base);
        if !known {
            return Err(missing());
        }
        let doc = load_env(&self.secrets, id).await?.ok_or_else(|| {
            Error::Internal(format!(
                "provider profile {reference} environment is unavailable"
            ))
        })?;
        Ok(Some(overlay_from(reference, base, doc, home)))
    }

    /// The Default configuration's saved overlay (PROTOCOL §5.54 "Default
    /// configuration overrides"), keyed by the bare reference. `None` when
    /// the reference is not a profile-capable base, nothing is saved, or the
    /// profile file is unreadable (bare references keep today's behavior
    /// then). A recorded environment whose secret cannot be read fails
    /// closed: the Default never silently runs without its overrides.
    async fn resolve_default_env(
        &self,
        reference: &str,
        home: Option<&Path>,
    ) -> Result<Option<intent_acp::ProfileEnv>> {
        let Some(base) = PROFILE_BASE_PROVIDERS
            .iter()
            .copied()
            .find(|base| *base == reference)
        else {
            return Ok(None);
        };
        let Some(env_id) = self
            .provider_profiles
            .defaults()
            .ok()
            .and_then(|d| d.get(base).and_then(|d| d.env_id.clone()))
        else {
            return Ok(None);
        };
        let doc = load_env(&self.secrets, &env_id).await?.ok_or_else(|| {
            Error::Internal(format!(
                "the {base} Default configuration environment is unavailable"
            ))
        })?;
        Ok(Some(overlay_from(reference, base, doc, home)))
    }

    /// Selection gate for a provider reference about to be persisted or
    /// started (create, setModel, delegate, quick actions, turn start). A
    /// bare id passes untouched — legacy selections keep today's behavior,
    /// including the disabled-provider re-home. A qualified reference must
    /// name a saved profile (`profile-missing`) whose base provider is
    /// enabled (`provider-disabled`); either failure is
    /// `provider-profile-replacement-required` and never falls back to the
    /// base's Default configuration or the app default.
    pub(crate) fn ensure_provider_ref_selectable(&self, reference: &str) -> Result<()> {
        self.ensure_provider_ref_selectable_in(reference, &self.effective_settings().providers)
    }

    /// [`Self::ensure_provider_ref_selectable`] against an already-read
    /// providers snapshot (turn start gates on the settings it spawns with).
    pub(crate) fn ensure_provider_ref_selectable_in(
        &self,
        reference: &str,
        providers: &intent_core::settings_file::ProvidersSettings,
    ) -> Result<()> {
        match self.provider_replacement_reason_in(reference, providers) {
            Some(reason) => Err(replacement_required(reference, reason)),
            None => Ok(()),
        }
    }

    /// Why the next turn on `reference` would be refused
    /// (`profile-missing` / `provider-disabled`), or `None` when it may run.
    /// Always `None` for a bare id.
    fn provider_replacement_reason_in(
        &self,
        reference: &str,
        providers: &intent_core::settings_file::ProvidersSettings,
    ) -> Option<&'static str> {
        if !is_qualified_ref(reference) {
            return None;
        }
        let Some((base, _)) = parse_profile_ref(reference) else {
            return Some("profile-missing");
        };
        if !self.provider_profiles.has_reference(reference) {
            return Some("profile-missing");
        }
        if crate::agent_ops::provider_is_disabled(base, providers.enabled.as_ref()) {
            return Some("provider-disabled");
        }
        None
    }

    /// `AgentLite` `providerReplacementRequired` for a session's saved
    /// provider: `{ providerRef, reason }` when its next turn would be
    /// refused. Bare (legacy) selections never carry it.
    pub(crate) fn provider_replacement_required(&self, provider: Option<&str>) -> Option<Value> {
        let reference = provider.filter(|p| is_qualified_ref(p))?;
        let reason =
            self.provider_replacement_reason_in(reference, &self.effective_settings().providers)?;
        Some(json!({ "providerRef": reference, "reason": reason }))
    }

    /// The live-process activation registry (PROTOCOL §5.54 activation).
    pub(crate) fn profile_activation(&self) -> &crate::profile_activation::ProfileActivation {
        &self.profile_activation
    }

    /// Record a newly started provider process for `agent_id` running
    /// `profile`'s saved overlay. The returned lease lives with the process
    /// and unregisters it when the process is torn down.
    pub(crate) async fn register_profile_process(
        &self,
        workspace_id: &intent_core::WorkspaceId,
        agent_id: &intent_core::AgentId,
        profile: &intent_acp::ProfileEnv,
    ) -> Option<crate::profile_activation::ActivationLease> {
        self.register_overlay_process(
            workspace_id,
            agent_id,
            profile.provider_ref(),
            profile.fingerprint(),
        )
        .await
    }

    /// Record a newly started Default-configuration process that runs with
    /// no saved overrides (fingerprint `""`), so a later edit can report it
    /// as running on earlier settings.
    pub(crate) async fn register_default_process(
        &self,
        workspace_id: &intent_core::WorkspaceId,
        agent_id: &intent_core::AgentId,
        base: &str,
    ) -> Option<crate::profile_activation::ActivationLease> {
        self.register_overlay_process(workspace_id, agent_id, base, "")
            .await
    }

    async fn register_overlay_process(
        &self,
        workspace_id: &intent_core::WorkspaceId,
        agent_id: &intent_core::AgentId,
        reference: &str,
        fingerprint: &str,
    ) -> Option<crate::profile_activation::ActivationLease> {
        let (token, changes) =
            self.profile_activation
                .register(workspace_id, agent_id, reference, fingerprint)?;
        self.publish_activation_changes(changes).await;
        Some(crate::profile_activation::ActivationLease::new(
            self.clone(),
            agent_id.clone(),
            token,
        ))
    }

    /// Emit the events an activation change calls for: one
    /// `providers:profiles-changed` (`change: "activation"`, no count) per
    /// saved profile whose `runningProcesses` moved, and one `agent:updated`
    /// per agent whose `usingEarlierProfileSettings` flipped.
    pub(crate) async fn publish_activation_changes(
        &self,
        changes: crate::profile_activation::ActivationChanges,
    ) {
        if changes.is_empty() {
            return;
        }
        let saved = self.provider_profiles.profiles().ok();
        for profile_id in &changes.profiles {
            if let Some(base) = parse_default_activation_key(profile_id) {
                let revision = self.provider_profiles.bump_revision();
                crate::publish_event(
                    self.event_bus.as_ref(),
                    default_changed_event(revision, "default-activation", base, None),
                )
                .await;
                continue;
            }
            if !saved
                .as_ref()
                .is_some_and(|all| all.iter().any(|p| p.id == *profile_id))
            {
                continue;
            }
            let revision = self.provider_profiles.bump_revision();
            crate::publish_event(
                self.event_bus.as_ref(),
                profiles_changed_event(revision, "activation", profile_id, None, None),
            )
            .await;
        }
        for (workspace_id, agent_id, using_earlier) in changes.agents {
            self.publish_agent_mutation_event(
                &workspace_id,
                &agent_id,
                intent_core::events::AGENT_UPDATED,
                json!({ "agentId": agent_id.0, "usingEarlierProfileSettings": using_earlier }),
            )
            .await;
        }
    }

    /// The settings fingerprint the process serving `agent_id` under
    /// `selection` runs with: the live registration when present, else the
    /// saved one (the fingerprint a process started now would read). `None`
    /// for a bare selection without Default overrides support or an
    /// unresolvable profile. A Default configuration with no saved entries
    /// (or an unreadable one) reads `""`.
    pub(crate) async fn profile_process_fingerprint(
        &self,
        agent_id: &intent_core::AgentId,
        selection: &str,
    ) -> Option<String> {
        if !may_have_overlay(selection) {
            return None;
        }
        if is_default_config_ref(selection) {
            if let Some(live) = self
                .profile_activation
                .live_fingerprint(agent_id, selection)
            {
                return Some(live);
            }
            return Some(
                Box::pin(self.resolve_profile_env(selection))
                    .await
                    .ok()
                    .flatten()
                    .map(|overlay| overlay.fingerprint().to_owned())
                    .unwrap_or_default(),
            );
        }
        if let Some(live) = self
            .profile_activation
            .live_fingerprint(agent_id, selection)
        {
            return Some(live);
        }
        Box::pin(self.resolve_profile_env(selection))
            .await
            .ok()
            .flatten()
            .map(|profile| profile.fingerprint().to_owned())
    }

    /// Whether the stored native session of `stored` was opened under the
    /// settings `fingerprint` (PROTOCOL §5.54: a native session id never
    /// crosses an account-relevant settings change). A session with no
    /// recorded binding counts as bound to `""`, the fingerprint of a
    /// Default configuration with no saved overrides; profile fingerprints
    /// are never empty, so an unbound session is foreign to every profile
    /// and is recreated with Intent history rather than loaded on possibly
    /// another account.
    pub(crate) fn native_session_bound_to(
        stored: &intent_core::AgentSession,
        fingerprint: &str,
    ) -> bool {
        binding_matches(native_session_binding(stored), fingerprint)
    }

    /// Whether recording `fingerprint` for `stored` would change what
    /// [`Self::native_session_bound_to`] reads (an absent binding already
    /// reads as `""`).
    pub(crate) fn native_session_binding_differs(
        stored: &intent_core::AgentSession,
        fingerprint: &str,
    ) -> bool {
        !binding_matches(native_session_binding(stored), fingerprint)
    }

    /// Record that `agent_id`'s current native session was opened under the
    /// profile settings `fingerprint`. Best-effort: a failed write only
    /// costs a recreate (with history) on the next resume.
    pub(crate) async fn bind_native_session_settings(
        &self,
        workspace_id: &intent_core::WorkspaceId,
        agent_id: &intent_core::AgentId,
        fingerprint: &str,
    ) {
        if let Err(e) = self
            .store
            .set_agent_session_metadata_key(
                workspace_id,
                agent_id,
                NATIVE_SESSION_SETTINGS_KEY,
                fingerprint,
                None,
                None,
            )
            .await
        {
            tracing::warn!(agent = %agent_id, error = %e, "recording the native session's profile settings failed");
        }
    }

    /// Turn admission (`agent.sendMessage`): refuse a message whose next turn
    /// would be refused at spawn, before anything is persisted, with the
    /// structured `provider-profile-replacement-required` error. Resolves the
    /// session's selection like the spawn path (saved provider, then the app
    /// default). Bare legacy selections pass untouched.
    pub(crate) async fn ensure_turn_provider_selectable(
        &self,
        agent_id: &intent_core::AgentId,
    ) -> Result<()> {
        let session = self.store.get_agent_session_summary(agent_id).await?;
        let settings = self.effective_settings();
        let Some(selection) = crate::agent_session::resolve_provider_id(
            session.provider.as_deref(),
            crate::agent_session::derived_default_provider(&settings).as_deref(),
        ) else {
            return Ok(());
        };
        self.ensure_provider_ref_selectable_in(&selection, &settings.providers)
    }

    /// [`Self::ensure_provider_ref_selectable`] then
    /// [`Self::resolve_profile_env`]: the overlay a new process for
    /// `reference` must run with (`None` = Default configuration).
    pub(crate) async fn resolve_selected_profile(
        &self,
        reference: &str,
    ) -> Result<Option<Arc<intent_acp::ProfileEnv>>> {
        self.ensure_provider_ref_selectable(reference)?;
        Ok(self.resolve_profile_env(reference).await?.map(Arc::new))
    }

    /// `providers.catalog` `profiles`: one summary per saved profile, no env
    /// and no counts. An unreadable profile file yields an empty list.
    pub(crate) fn provider_profile_summaries(&self) -> Vec<Value> {
        self.provider_profiles
            .profiles()
            .map(|profiles| profiles.iter().map(ProfileMeta::summary_json).collect())
            .unwrap_or_default()
    }

    /// `host.providerAuthStatus` for profile references (PROTOCOL §5.54).
    /// Each profile is probed with its own saved overlay and cached under
    /// its reference plus settings fingerprint, so a verdict never crosses
    /// profiles, the Default configuration or a saved edit. One profile:
    /// a missing reference fails with `provider-profile-replacement-required`
    /// (a disabled base still reports its saved profile's status). Sweep: an
    /// unresolvable profile reports `authenticated: null`.
    pub(crate) async fn provider_profiles_auth_status_impl(
        &self,
        provider_ref: Option<String>,
        force: bool,
    ) -> Result<Value> {
        let single = provider_ref.is_some();
        let refs = provider_ref.map_or_else(|| self.provider_profile_refs(), |r| vec![r]);
        let paths = self.effective_settings().providers.paths;
        // Unknown until its probe reports; a panicked probe stays unknown.
        let mut entries: Vec<Value> = refs
            .iter()
            .map(|r| json!({ "id": r, "authenticated": null }))
            .collect();
        // Probes run in parallel and abort with this future (JoinSet), like
        // the base sweep; response order follows the saved profile order.
        let mut probes = tokio::task::JoinSet::new();
        for (index, reference) in refs.into_iter().enumerate() {
            let profile = match self.resolve_profile_env(&reference).await {
                Ok(Some(profile)) => profile,
                Ok(None) => {
                    return Err(Error::InvalidParams(format!(
                        "invalid params: not a provider profile reference: {reference}"
                    )))
                }
                Err(e) if single => return Err(e),
                Err(e) => {
                    tracing::warn!(provider_ref = %reference, error = %e, "provider profile auth check skipped");
                    continue;
                }
            };
            let override_path = paths
                .get(intent_providers::base_provider_id(&reference))
                .cloned();
            probes.spawn(async move {
                let entry = crate::provider_auth::profile_auth_status(
                    Arc::new(profile),
                    force,
                    override_path,
                )
                .await;
                (index, entry)
            });
        }
        while let Some(joined) = probes.join_next().await {
            if let Ok((index, entry)) = joined {
                entries[index] = entry;
            }
        }
        Ok(Value::Array(entries))
    }

    /// Whether `provider_id`'s Default configuration has saved environment
    /// overrides (in-memory; `false` while the profile file is unreadable).
    pub(crate) fn default_has_overrides(&self, provider_id: &str) -> bool {
        is_default_config_ref(provider_id)
            && self
                .provider_profiles
                .defaults()
                .ok()
                .is_some_and(|d| d.get(provider_id).is_some_and(|d| d.env_id.is_some()))
    }

    /// [`crate::agent_ops::ensure_provider_available`] for a selection: a
    /// Default configuration with saved overrides is not refused on the bare
    /// installation's verdict (its own verdict is keyed by its overlay).
    #[expect(
        clippy::missing_errors_doc,
        reason = "internal doc-hidden feature API, not a public library API"
    )]
    pub fn ensure_provider_available_for(
        &self,
        method: &str,
        provider_id: &str,
        providers: &intent_core::settings_file::ProvidersSettings,
    ) -> Result<()> {
        crate::agent_ops::ensure_provider_available_checked(
            method,
            provider_id,
            providers,
            !self.default_has_overrides(provider_id),
        )
    }

    /// The fresh cached auth verdict of a selection's saved settings: a
    /// profile's or an overridden Default's overlay verdict, else the bare
    /// verdict. An unresolvable overlay is an error (fail closed).
    pub(crate) async fn selection_auth_verdict(&self, selection: &str) -> Result<Option<bool>> {
        if !may_have_overlay(selection) {
            return Ok(crate::provider_auth::cached_auth_verdict(selection));
        }
        Ok(match Box::pin(self.resolve_profile_env(selection)).await? {
            Some(overlay) => crate::provider_auth::cached_profile_auth_verdict(&overlay),
            None => crate::provider_auth::cached_auth_verdict(selection),
        })
    }

    /// `host.providerAuthStatus` rows for Default configurations with saved
    /// overrides: each is probed with its overlay and cached under its
    /// reference plus fingerprint (never the base verdict). An unresolvable
    /// Default reports `authenticated: null`.
    pub(crate) async fn provider_default_auth_status_impl(
        &self,
        provider_id: Option<String>,
        force: bool,
    ) -> Result<Value> {
        let paths = self.effective_settings().providers.paths;
        let bases: Vec<&'static str> = PROFILE_BASE_PROVIDERS
            .iter()
            .copied()
            .filter(|base| provider_id.as_deref().is_none_or(|wanted| wanted == *base))
            .filter(|base| self.default_has_overrides(base))
            .collect();
        let mut entries: Vec<Value> = bases
            .iter()
            .map(|base| json!({ "id": base, "authenticated": null }))
            .collect();
        // Probes run in parallel, like the base and profile sweeps.
        let mut probes = tokio::task::JoinSet::new();
        for (index, base) in bases.into_iter().enumerate() {
            match self.resolve_profile_env(base).await {
                Ok(Some(overlay)) => {
                    let override_path = paths.get(base).cloned();
                    probes.spawn(async move {
                        let entry = crate::provider_auth::profile_auth_status(
                            Arc::new(overlay),
                            force,
                            override_path,
                        )
                        .await;
                        (index, entry)
                    });
                }
                Ok(None) => {}
                Err(e) => {
                    tracing::warn!(provider = %base, error = %e, "Default configuration auth check skipped");
                }
            }
        }
        while let Some(joined) = probes.join_next().await {
            if let Ok((index, entry)) = joined {
                entries[index] = entry;
            }
        }
        Ok(Value::Array(entries))
    }

    /// `host.providerTestPrompt { providerId: providerRef }` (PROTOCOL
    /// §5.54): the live probe on the base installation under the profile's
    /// saved overlay. A missing profile needs a replacement.
    pub(crate) async fn provider_profiles_test_prompt_impl(
        &self,
        provider_ref: String,
        model: Option<String>,
        node_max_old_space_mb: Option<u32>,
    ) -> Result<Value> {
        // A Default configuration runs under its saved overrides, when any
        // (the bare base otherwise, exactly as before).
        let profile = self.resolve_profile_env(&provider_ref).await?;
        if profile.is_none() && !is_default_config_ref(&provider_ref) {
            return Err(Error::InvalidParams(format!(
                "invalid params: not a provider profile reference: {provider_ref}"
            )));
        }
        let paths: std::collections::HashMap<String, String> = self
            .effective_settings()
            .providers
            .paths
            .into_iter()
            .collect();
        crate::provider_test_prompt::provider_test_prompt_for(
            Some(self),
            intent_providers::base_provider_id(&provider_ref),
            model.as_deref(),
            &paths,
            node_max_old_space_mb,
            profile.map(Arc::new),
        )
        .await
        .map_err(Error::InvalidParams)
    }

    /// Saved profile references, for the opt-in auth sweep.
    pub(crate) fn provider_profile_refs(&self) -> Vec<String> {
        self.provider_profiles
            .profiles()
            .map(|profiles| profiles.iter().map(ProfileMeta::provider_ref).collect())
            .unwrap_or_default()
    }

    pub(crate) async fn provider_profiles_list_impl(
        &self,
        base_provider_id: Option<String>,
    ) -> Result<Value> {
        Self::require_administrator("providers.profiles.list")?;
        let store = &self.provider_profiles;
        let _guard = store.lock().await;
        let profiles = store.profiles()?;
        let mut rows = Vec::new();
        for meta in profiles.iter().filter(|p| {
            base_provider_id
                .as_deref()
                .is_none_or(|base| p.base_provider == base)
        }) {
            let env = load_env(&self.secrets, &meta.id).await?.unwrap_or_default();
            let running = self.profile_activation.running_processes(&meta.id);
            rows.push(profile_json(meta, &env, running));
        }
        let saved_defaults = store.defaults()?;
        let mut defaults = Vec::new();
        for base in PROFILE_BASE_PROVIDERS.iter().filter(|base| {
            base_provider_id
                .as_deref()
                .is_none_or(|wanted| wanted == **base)
        }) {
            let saved = saved_defaults.get(*base).cloned().unwrap_or_default();
            let env = self.load_default_env(&saved).await?;
            let running = self
                .profile_activation
                .running_processes(&default_activation_key(base));
            defaults.push(default_config_json(
                base,
                saved.name.as_deref(),
                &env,
                running,
            ));
        }
        Ok(json!({ "profiles": rows, "defaults": defaults, "revision": store.revision() }))
    }

    /// The saved environment of one Default configuration for reads and
    /// edits: empty (with the `""` fingerprint) when none is recorded or the
    /// recorded document is missing, as for profiles, so a save can replace
    /// it; an undecodable document is an error. Launches never use this:
    /// they fail closed on a missing document ([`Self::resolve_profile_env`]).
    async fn load_default_env(&self, saved: &DefaultMeta) -> Result<EnvDoc> {
        let Some(env_id) = &saved.env_id else {
            return Ok(EnvDoc::default());
        };
        let doc = load_env(&self.secrets, env_id).await?;
        if doc.is_none() {
            tracing::warn!("a Default configuration environment is missing; launches fail until it is saved again");
        }
        Ok(doc.unwrap_or_default())
    }

    /// `providers.catalog` `defaultNames`: the renamed Default of each base
    /// provider, or `None` when none is renamed (or the file is unreadable).
    pub(crate) fn provider_default_names(&self) -> Option<Value> {
        let defaults = self.provider_profiles.defaults().ok()?;
        let names: serde_json::Map<String, Value> = defaults
            .iter()
            .filter_map(|(base, d)| Some((base.clone(), json!(d.name.as_ref()?))))
            .collect();
        (!names.is_empty()).then_some(Value::Object(names))
    }

    pub(crate) async fn provider_profiles_update_default_impl(
        &self,
        params: Value,
    ) -> Result<Value> {
        Self::require_administrator("providers.profiles.updateDefault")?;
        self.owned_profile_mutation(move |svc| async move {
            svc.provider_profiles_update_default_owned(params).await
        })
        .await
    }

    /// `providers.profiles.updateDefault` (PROTOCOL §5.54 "Default
    /// configuration overrides"). An environment change writes the new
    /// document under a freshly minted `envId`, journals the new and the
    /// previous id, then commits name and `envId` together through the
    /// profile file's atomic rename; the previous secret is removed after
    /// the commit. A failed file write removes the new secret, so the saved
    /// name and environment are unchanged.
    async fn provider_profiles_update_default_owned(&self, params: Value) -> Result<Value> {
        let base = opt_string(&params, "baseProviderId")?
            .ok_or_else(|| Error::InvalidParams("missing required param: baseProviderId".into()))?;
        let raw_name = opt_string(&params, "name")?;
        let writes = opt_env(&params)?;

        let store = &self.provider_profiles;
        let _guard = store.lock().await;
        let profiles = store.profiles()?;
        let mut defaults = store.defaults()?.as_ref().clone();
        let Some(base) = PROFILE_BASE_PROVIDERS
            .iter()
            .copied()
            .find(|candidate| *candidate == base)
        else {
            return Err(field_errors(&[FieldError::new(
                "baseProviderId",
                "base-provider-unsupported",
                format!("{base} does not support profiles."),
            )]));
        };
        let current = defaults.get(base).cloned().unwrap_or_default();
        let saved_doc = self.load_default_env(&current).await?;

        let mut errors = Vec::new();
        let name = raw_name.map(|raw| validate_default_name(&raw, base, &profiles, &mut errors));
        let entries = writes
            .as_ref()
            .map(|writes| validate_env(writes, base, Some(&saved_doc.entries), &mut errors));
        if !errors.is_empty() {
            return Err(field_errors(&errors));
        }

        // A recorded environment whose document is missing reads empty
        // (with no fingerprint); any env write replaces or clears it.
        let saved_missing = current.env_id.is_some() && saved_doc.fingerprint.is_empty();
        let env_changed = entries
            .as_ref()
            .is_some_and(|entries| *entries != saved_doc.entries || saved_missing);
        let name_changed = name.as_ref().is_some_and(|n| *n != current.name);
        let key = default_activation_key(base);
        if !env_changed && !name_changed {
            let running = self.profile_activation.running_processes(&key);
            return Ok(json!({
                "defaultConfig": default_config_json(base, current.name.as_deref(), &saved_doc, running),
                "revision": store.revision(),
                "activation": { "envChanged": false, "runningProcesses": running },
            }));
        }

        let mut next = current.clone();
        if let Some(name) = name {
            next.name = name;
        }
        let mut env = saved_doc;
        let pending = store.pending();
        let mut new_secret: Option<String> = None;
        if env_changed {
            let entries = entries.unwrap_or_default();
            if entries.is_empty() {
                next.env_id = None;
                env = EnvDoc::default();
            } else {
                let doc = EnvDoc::fresh(entries);
                let id = loop {
                    let candidate = format!("d{}", random_token(12));
                    if !profiles.iter().any(|p| p.id == candidate)
                        && !pending.contains(&candidate)
                        && !defaults
                            .values()
                            .any(|d| d.env_id.as_deref() == Some(candidate.as_str()))
                    {
                        break candidate;
                    }
                };
                let mut journaled = pending.clone();
                journaled.push(id.clone());
                if let Some(previous) = &current.env_id {
                    journaled.push(previous.clone());
                }
                store.persist(profiles.as_ref().clone(), journaled)?;
                if let Err(e) = self.secrets.store(&env_account(&id), &doc.encode()?).await {
                    if let Err(journal) = store.persist(profiles.as_ref().clone(), pending.clone())
                    {
                        tracing::warn!(error = %journal, "provider default journal rollback failed");
                    }
                    return Err(e);
                }
                new_secret = Some(id.clone());
                next.env_id = Some(id);
                env = doc;
            }
        }

        // Commit point: name and envId land together. A removed environment
        // journals its previous secret here; a replaced one already did.
        let mut journal = store.pending();
        if env_changed && new_secret.is_none() {
            if let Some(previous) = &current.env_id {
                journal.push(previous.clone());
            }
        }
        defaults.insert(base.to_owned(), next.clone());
        if let Err(e) = store.persist_with_defaults(
            profiles.as_ref().clone(),
            defaults.clone(),
            journal.clone(),
        ) {
            if let Some(id) = &new_secret {
                if let Err(cleanup) = self.secrets.delete(&env_account(id)).await {
                    tracing::warn!(error = %cleanup, "provider default secret rollback failed; it is removed at next start");
                }
            }
            return Err(e);
        }
        if env_changed {
            let mut remaining = journal;
            if let Some(id) = &new_secret {
                remaining.retain(|p| p != id);
            }
            if let Some(previous) = &current.env_id {
                match self.secrets.delete(&env_account(previous)).await {
                    Ok(()) => remaining.retain(|p| p != previous),
                    Err(e) => {
                        tracing::warn!(error = %e, "previous provider default secret delete failed; it is removed at next start");
                    }
                }
            }
            if let Err(e) =
                store.persist_with_defaults(profiles.as_ref().clone(), defaults, remaining)
            {
                tracing::warn!(error = %e, "provider default cleanup journal update failed");
            }
        }

        let revision = store.bump_revision();
        crate::publish_event(
            self.event_bus.as_ref(),
            default_changed_event(revision, "default-updated", base, Some(env_changed)),
        )
        .await;
        if env_changed {
            let changes = self
                .profile_activation
                .saved_changed(&key, Some(&env.fingerprint));
            self.publish_activation_changes(changes).await;
        }
        let running = self.profile_activation.running_processes(&key);
        Ok(json!({
            "defaultConfig": default_config_json(base, next.name.as_deref(), &env, running),
            "revision": revision,
            "activation": { "envChanged": env_changed, "runningProcesses": running },
        }))
    }

    /// Run one profile mutation as an owned daemon task over a settled
    /// secret-store operation: a write that outlives its budget is awaited to
    /// its actual outcome (never reported failed while it may still land),
    /// caller cancellation cannot abandon the journal/secret/metadata
    /// sequence half-way, and the operation's receipts are acknowledged.
    async fn owned_profile_mutation<F, Fut>(&self, mutation: F) -> Result<Value>
    where
        F: FnOnce(Services) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<Value>> + Send + 'static,
    {
        let caller = intent_core::current_caller()
            .ok_or_else(|| Error::Internal("provider profile caller missing".into()))?;
        let credential = intent_core::caller::current_wire_credential();
        let mut operation = self.clone();
        operation.secrets = Arc::new(
            self.secrets
                .settled_operation(Arc::new(tokio::sync::Notify::new())),
        );
        intent_core::spawn_daemon(intent_core::with_caller(
            caller,
            intent_core::caller::with_wire_credential(credential, async move {
                let result = mutation(operation.clone()).await;
                operation.secrets.finish_operation().await;
                result
            }),
        ))
        .await
        .map_err(|e| {
            Error::Internal(format!(
                "provider profile operation failed: {e}; state may be unknown"
            ))
        })?
    }

    pub(crate) async fn provider_profiles_create_impl(&self, params: Value) -> Result<Value> {
        Self::require_administrator("providers.profiles.create")?;
        self.owned_profile_mutation(move |svc| async move {
            svc.provider_profiles_create_owned(params).await
        })
        .await
    }

    pub(crate) async fn provider_profiles_update_impl(
        &self,
        profile_id: String,
        params: Value,
    ) -> Result<Value> {
        Self::require_administrator("providers.profiles.update")?;
        self.owned_profile_mutation(move |svc| async move {
            svc.provider_profiles_update_owned(profile_id, params).await
        })
        .await
    }

    pub(crate) async fn provider_profiles_delete_impl(&self, profile_id: String) -> Result<Value> {
        Self::require_administrator("providers.profiles.delete")?;
        self.owned_profile_mutation(move |svc| async move {
            svc.provider_profiles_delete_owned(profile_id).await
        })
        .await
    }

    pub(crate) async fn provider_profiles_reorder_impl(&self, params: Value) -> Result<Value> {
        Self::require_administrator("providers.profiles.reorder")?;
        self.owned_profile_mutation(move |svc| async move {
            svc.provider_profiles_reorder_owned(params).await
        })
        .await
    }

    /// Persist a new order for one base provider's profiles (failover tries
    /// backups in this order): one atomic metadata write. The response rows
    /// (which read each profile's masked env) are built first, so a failed
    /// read changes nothing (§5.54). Every successful call commits, bumps
    /// the revision and emits one `reordered` event, even when the order is
    /// unchanged.
    async fn provider_profiles_reorder_owned(&self, params: Value) -> Result<Value> {
        let base = opt_string(&params, "baseProviderId")?
            .ok_or_else(|| Error::InvalidParams("missing required param: baseProviderId".into()))?;
        let profile_ids = parse_profile_ids(&params)?;

        let store = &self.provider_profiles;
        let _guard = store.lock().await;
        let profiles = store.profiles()?;
        let after = reorder_profiles(&profiles, &base, &profile_ids)
            .map_err(|errors| field_errors(&errors))?;
        let mut rows = Vec::new();
        for meta in after.iter().filter(|p| p.base_provider == base) {
            let env = load_env(&self.secrets, &meta.id).await?.unwrap_or_default();
            let running = self.profile_activation.running_processes(&meta.id);
            rows.push(profile_json(meta, &env, running));
        }
        store.persist(after, store.pending())?;
        let revision = store.bump_revision();
        crate::publish_event(
            self.event_bus.as_ref(),
            profiles_reordered_event(revision, &base),
        )
        .await;
        Ok(json!({ "profiles": rows, "revision": revision }))
    }

    async fn provider_profiles_create_owned(&self, params: Value) -> Result<Value> {
        let base = opt_string(&params, "baseProviderId")?
            .ok_or_else(|| Error::InvalidParams("missing required param: baseProviderId".into()))?;
        let raw_name = opt_string(&params, "name")?
            .ok_or_else(|| Error::InvalidParams("missing required param: name".into()))?;
        let writes = opt_env(&params)?.unwrap_or_default();
        let backup = opt_bool(&params, "backup")?.unwrap_or(false);

        let store = &self.provider_profiles;
        let _guard = store.lock().await;
        let profiles = store.profiles()?;
        let mut errors = Vec::new();
        if !PROFILE_BASE_PROVIDERS.contains(&base.as_str()) {
            errors.push(FieldError::new(
                "baseProviderId",
                "base-provider-unsupported",
                format!("{base} does not support profiles."),
            ));
        } else if profiles.iter().filter(|p| p.base_provider == base).count()
            >= MAX_PROFILES_PER_BASE
        {
            errors.push(FieldError::new(
                "baseProviderId",
                "profile-limit",
                format!("A provider can have at most {MAX_PROFILES_PER_BASE} profiles."),
            ));
        }
        let name = validate_name(&raw_name, &base, &profiles, None, &mut errors);
        check_default_name_conflict(&name, store.default_name(&base).as_deref(), &mut errors);
        let entries = validate_env(&writes, &base, None, &mut errors);
        if !errors.is_empty() {
            return Err(field_errors(&errors));
        }

        let pending = store.pending();
        let id = loop {
            let candidate = format!("p{}", random_token(12));
            if !profiles.iter().any(|p| p.id == candidate) && !pending.contains(&candidate) {
                break candidate;
            }
        };
        let now = now_rfc3339();
        let meta = ProfileMeta {
            id: id.clone(),
            base_provider: base,
            name,
            created_at: now.clone(),
            updated_at: now,
            backup,
        };
        let env = EnvDoc::fresh(entries);
        let account = env_account(&id);

        // Journal → secret → metadata (see module docs).
        let before = profiles.as_ref().clone();
        let mut journaled = pending.clone();
        journaled.push(id.clone());
        store.persist(before.clone(), journaled)?;
        if let Err(e) = self.secrets.store(&account, &env.encode()?).await {
            if let Err(journal) = store.persist(before, pending) {
                tracing::warn!(error = %journal, "provider profile journal rollback failed");
            }
            return Err(e);
        }
        let mut after = before;
        after.push(meta.clone());
        if let Err(e) = store.persist(after, pending) {
            // The journaled id stays on disk, so a failed delete here is
            // still removed at the next start.
            if let Err(cleanup) = self.secrets.delete(&account).await {
                tracing::warn!(profile_id = %id, error = %cleanup, "provider profile secret rollback failed; it is removed at next start");
            }
            return Err(e);
        }

        let revision = store.bump_revision();
        crate::publish_event(
            self.event_bus.as_ref(),
            profiles_changed_event(revision, "created", &id, Some(meta.summary_json()), None),
        )
        .await;
        let running = self.profile_activation.running_processes(&meta.id);
        Ok(json!({ "profile": profile_json(&meta, &env, running), "revision": revision }))
    }

    async fn provider_profiles_update_owned(
        &self,
        profile_id: String,
        params: Value,
    ) -> Result<Value> {
        let raw_name = opt_string(&params, "name")?;
        let writes = opt_env(&params)?;
        let backup = opt_bool(&params, "backup")?;

        let store = &self.provider_profiles;
        let _guard = store.lock().await;
        let profiles = store.profiles()?;
        let index = profiles
            .iter()
            .position(|p| p.id == profile_id)
            .ok_or_else(|| profile_not_found(&profile_id))?;
        let current = profiles[index].clone();
        let saved = load_env(&self.secrets, &profile_id).await?;
        let saved_doc = saved.clone().unwrap_or_default();

        let mut errors = Vec::new();
        let name = raw_name.map(|raw| {
            let name = validate_name(
                &raw,
                &current.base_provider,
                &profiles,
                Some(&profile_id),
                &mut errors,
            );
            check_default_name_conflict(
                &name,
                store.default_name(&current.base_provider).as_deref(),
                &mut errors,
            );
            name
        });
        let entries = writes.as_ref().map(|writes| {
            validate_env(
                writes,
                &current.base_provider,
                Some(&saved_doc.entries),
                &mut errors,
            )
        });
        if !errors.is_empty() {
            return Err(field_errors(&errors));
        }

        let env_changed = entries
            .as_ref()
            .is_some_and(|entries| *entries != saved_doc.entries || saved.is_none());
        let name_changed = name.as_ref().is_some_and(|n| *n != current.name);
        let backup_changed = backup.is_some_and(|b| b != current.backup);
        if !env_changed && !name_changed && !backup_changed {
            let running = self.profile_activation.running_processes(&profile_id);
            return Ok(json!({
                "profile": profile_json(&current, &saved_doc, running),
                "revision": store.revision(),
                "activation": { "envChanged": false, "runningProcesses": running },
            }));
        }

        let account = env_account(&profile_id);
        let env = if env_changed {
            let next = EnvDoc::fresh(entries.unwrap_or_default());
            self.secrets.store(&account, &next.encode()?).await?;
            next
        } else {
            saved_doc
        };
        let mut meta = current;
        if let Some(name) = name {
            meta.name = name;
        }
        if let Some(backup) = backup {
            meta.backup = backup;
        }
        meta.updated_at = now_rfc3339();
        let mut after = profiles.as_ref().clone();
        after[index] = meta.clone();
        if let Err(e) = store.persist(after, store.pending()) {
            if env_changed {
                let restored = match &saved {
                    Some(previous) => match previous.encode() {
                        Ok(raw) => self.secrets.store(&account, &raw).await,
                        Err(err) => Err(err),
                    },
                    None => self.secrets.delete(&account).await,
                };
                if let Err(restore) = restored {
                    tracing::warn!(profile_id = %profile_id, error = %restore, "provider profile env rollback failed");
                }
            }
            return Err(e);
        }

        let revision = store.bump_revision();
        crate::publish_event(
            self.event_bus.as_ref(),
            profiles_changed_event(
                revision,
                "updated",
                &profile_id,
                Some(meta.summary_json()),
                Some(env_changed),
            ),
        )
        .await;
        // Running processes keep the settings they started with; only the
        // saved fingerprint moves. Nothing is interrupted or restarted.
        if env_changed {
            let changes = self
                .profile_activation
                .saved_changed(&profile_id, Some(&env.fingerprint));
            self.publish_activation_changes(changes).await;
        }
        let running = self.profile_activation.running_processes(&profile_id);
        Ok(json!({
            "profile": profile_json(&meta, &env, running),
            "revision": revision,
            "activation": { "envChanged": env_changed, "runningProcesses": running },
        }))
    }

    async fn provider_profiles_delete_owned(&self, profile_id: String) -> Result<Value> {
        let store = &self.provider_profiles;
        let _guard = store.lock().await;
        let profiles = store.profiles()?;
        let meta = profiles
            .iter()
            .find(|p| p.id == profile_id)
            .cloned()
            .ok_or_else(|| profile_not_found(&profile_id))?;
        let reference = meta.provider_ref();

        let settings = self.effective_settings();
        let services = self.clone();
        let specialists = tokio::task::spawn_blocking(move || {
            services
                .specialists_service()
                .list(None)
                .map(|v| v["specialists"].as_array().cloned().unwrap_or_default())
        })
        .await
        .map_err(|e| Error::Internal(format!("specialist scan failed: {e}")))??;
        let reasons = in_use_reasons(
            &reference,
            settings.model.default_provider.as_deref(),
            &specialists,
        );
        if !reasons.is_empty() {
            return Err(Error::provider_profile(
                false,
                format!(
                    "invalid params: provider profile {} is still selected by saved settings",
                    meta.name
                ),
                json!({ "code": "provider-profile-in-use", "inUse": reasons }),
            ));
        }

        // Metadata (journaling the id) → secret → clear the journal entry.
        let remaining: Vec<ProfileMeta> = profiles
            .iter()
            .filter(|p| p.id != profile_id)
            .cloned()
            .collect();
        let mut pending = store.pending();
        pending.push(profile_id.clone());
        store.persist(remaining.clone(), pending.clone())?;
        match self.secrets.delete(&env_account(&profile_id)).await {
            Ok(()) => {
                pending.retain(|p| p != &profile_id);
                if let Err(e) = store.persist(remaining, pending) {
                    tracing::warn!(error = %e, "provider profile cleanup journal update failed");
                }
            }
            Err(e) => {
                tracing::warn!(profile_id = %profile_id, error = %e, "provider profile secret delete failed; it is removed at next start");
            }
        }

        let revision = store.bump_revision();
        crate::publish_event(
            self.event_bus.as_ref(),
            profiles_changed_event(revision, "deleted", &profile_id, None, None),
        )
        .await;
        // An in-progress turn finishes on its process; later turns need a
        // replacement (`providerReplacementRequired`), not earlier settings.
        let changes = self.profile_activation.saved_changed(&profile_id, None);
        self.publish_activation_changes(changes).await;

        // Prune the profile's `model.providerDefaults` and
        // `providers.fastMode` keys in one batch (emits `settings:changed`).
        // A failure leaves inert keys behind. Re-read now: the snapshot above
        // predates the secret I/O, and a concurrent writer may have replaced
        // the maps since.
        let current = self.effective_settings();
        let mut changes = Vec::new();
        let mut defaults = current.model.provider_defaults;
        if defaults.remove(&reference).is_some() {
            changes.push(json!({ "path": "model.providerDefaults", "value": defaults }));
        }
        let mut fast_mode = current.providers.fast_mode;
        if fast_mode.remove(&reference).is_some() {
            changes.push(json!({ "path": "providers.fastMode", "value": fast_mode }));
        }
        if !changes.is_empty() {
            if let Err(e) = self
                .owned_settings_mutation(Value::Array(changes), None)
                .await
            {
                tracing::warn!(profile_id = %profile_id, error = %e, "pruning the deleted profile's settings keys failed");
            }
        }
        Ok(json!({ "profileId": profile_id, "revision": revision }))
    }

    /// `settings.update` front half: serialize with profile mutations when the
    /// batch carries a profile reference and reject references to missing
    /// profiles before anything is written.
    pub(crate) async fn settings_update_checking_profiles(&self, changes: Value) -> Result<Value> {
        if !batch_has_references(&changes) {
            return self.owned_settings_mutation(changes, None).await;
        }
        Self::require_administrator("settings.update")?;
        let _guard = self.provider_profiles.lock().await;
        let settings = self.effective_settings();
        validate_settings_references(
            &self.provider_profiles,
            &changes,
            &settings.model.provider_defaults,
            &settings.providers.fast_mode,
        )?;
        self.owned_settings_mutation(changes, None).await
    }
}

#[cfg(test)]
#[path = "provider_profiles_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "provider_profiles_routing_tests.rs"]
mod routing_tests;

#[cfg(test)]
#[path = "provider_profiles_activation_tests.rs"]
mod activation_tests;
