//! Remote compatibility as a saved choice (PROTOCOL §5.54 "Remote
//! compatibility") and the launch rules for a remote agent (nodes.md
//! "Placement across machines").
//!
//! - The "Use on remote machines" choices live in their own daemon-owned
//!   file, [`REMOTE_CHOICES_FILE`], which an older daemon never reads or
//!   rewrites, so a downgrade and upgrade keeps them.
//! - A configuration with no saved choice is not migrated yet and reads as
//!   the migration rule: today's behaviour. Startup writes the rule into the
//!   file once ([`Services::migrate_remote_choices`]).
//! - A configuration that sets an account folder is never marked.
//! - `providers.remoteMachineSignIn` lets an unmarked Default run with the
//!   machine's own sign-in, sending nothing.

use std::collections::BTreeMap;
use std::path::Path;

use intent_core::{Error, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use super::{
    default_changed_event, is_folder_path_key, is_qualified_ref, load_env, may_have_overlay,
    parse_profile_ref, unreadable, write_atomic, FieldError, ProviderProfileStore,
    RemoteCompatibility, StoredEnv, DEFAULT_NAME, PROFILE_BASE_PROVIDERS,
    PROFILE_NOT_REMOTE_COMPATIBLE, REMOTE_INCOMPATIBLE_ACCOUNT_FOLDER,
};
use crate::Services;

/// File name of the remote-compatible choices inside the daemon data dir.
pub(crate) const REMOTE_CHOICES_FILE: &str = "provider-remote-compatible.json";
const CHOICES_VERSION: u32 = 1;

/// [`RemoteCompatibility::NotRemoteCompatible`] reason: the user has not
/// marked the configuration for remote machines.
pub const REMOTE_INCOMPATIBLE_NOT_MARKED: &str = "not-marked";

/// The field error for marking a configuration that sets an account folder.
const REMOTE_ACCOUNT_FOLDER_FIELD: &str = "remote-account-folder";

/// The persisted choices. Missing entries are not migrated yet.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RemoteChoices {
    version: u32,
    #[serde(default)]
    profiles: BTreeMap<String, bool>,
    #[serde(default)]
    defaults: BTreeMap<String, bool>,
}

/// Where a remote agent runs: an exposed machine, or an E2B sandbox, which
/// has no sign-in of its own.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RemoteTarget {
    Machine,
    Cloud,
}

/// What a remote launch of a provider reference sends.
#[derive(Clone)]
pub struct RemoteLaunchPlan {
    /// The overlay sent as `profileEnv`; `None` sends nothing.
    pub env: Option<intent_acp::ProfileEnv>,
    /// A machine sign-in launch: the bare `codex` / `claude-code` reference
    /// sending nothing, so the machine's own sign-in must work.
    pub machine_sign_in: bool,
}

impl std::fmt::Debug for RemoteLaunchPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteLaunchPlan")
            .field("sends", &self.env.is_some())
            .field("machine_sign_in", &self.machine_sign_in)
            .finish()
    }
}

/// `DefaultConfig.remoteLaunch` and the catalog's `defaultRemoteLaunch`.
fn default_remote_launch(compat: &RemoteCompatibility, machine_sign_in: bool) -> &'static str {
    match compat {
        RemoteCompatibility::Compatible => "send",
        RemoteCompatibility::NotRemoteCompatible { .. } if machine_sign_in => "machine-sign-in",
        RemoteCompatibility::NotRemoteCompatible { .. } => "refused",
    }
}

pub(super) fn load_choices(path: &Path) -> Option<RemoteChoices> {
    match std::fs::read(path) {
        Ok(bytes) => {
            let parsed = serde_json::from_slice::<RemoteChoices>(&bytes)
                .ok()
                .filter(|c| c.version == CHOICES_VERSION);
            if parsed.is_none() {
                tracing::warn!(path = %path.display(), "remote-compatible choices file is malformed or has an unknown version; every configuration reads as not marked until it is fixed");
            }
            parsed
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Some(RemoteChoices {
            version: CHOICES_VERSION,
            ..RemoteChoices::default()
        }),
        Err(e) => {
            tracing::warn!(path = %path.display(), error = %e, "remote-compatible choices file is unreadable");
            None
        }
    }
}

impl ProviderProfileStore {
    /// The saved choices, or the unreadable error.
    pub(super) fn choices(&self) -> Result<RemoteChoices> {
        self.choices.read().unwrap().clone().ok_or_else(unreadable)
    }

    /// A profile's saved choice: `None` until migrated, `Some(false)` while
    /// the choices file is unreadable.
    pub(super) fn profile_choice(&self, id: &str) -> Option<bool> {
        match &*self.choices.read().unwrap() {
            Some(choices) => choices.profiles.get(id).copied(),
            None => Some(false),
        }
    }

    /// A Default configuration's saved choice, as [`Self::profile_choice`].
    pub(super) fn default_choice(&self, base: &str) -> Option<bool> {
        match &*self.choices.read().unwrap() {
            Some(choices) => choices.defaults.get(base).copied(),
            None => Some(false),
        }
    }

    /// Atomically persist `next`. Callers hold the mutation lock.
    pub(super) fn persist_choices(&self, mut next: RemoteChoices) -> Result<()> {
        next.version = CHOICES_VERSION;
        if let Some(path) = &self.choices_path {
            let bytes = serde_json::to_vec_pretty(&next).map_err(|e| {
                Error::Internal(format!("encode remote-compatible choices failed: {e}"))
            })?;
            write_atomic(path, &bytes)?;
        }
        *self.choices.write().unwrap() = Some(next);
        Ok(())
    }

    /// Set one entry (`profile` or Default `base`) and persist; returns the
    /// previous choices so a failed later write can restore them.
    pub(super) fn save_choice(
        &self,
        key: ChoiceKey<'_>,
        marked: bool,
    ) -> Result<Option<RemoteChoices>> {
        let previous = self.choices()?;
        let mut next = previous.clone();
        // Prune choices of deleted profiles on every write.
        if let Ok(profiles) = self.profiles() {
            next.profiles
                .retain(|id, _| profiles.iter().any(|p| &p.id == id));
        }
        match key {
            ChoiceKey::Profile(id) => next.profiles.insert(id.to_owned(), marked),
            ChoiceKey::Default(base) => next.defaults.insert(base.to_owned(), marked),
        };
        if next == previous {
            return Ok(None);
        }
        self.persist_choices(next)?;
        Ok(Some(previous))
    }

    /// Put back choices saved before a write that then failed.
    pub(super) fn restore_choices(&self, previous: Option<RemoteChoices>) {
        if let Some(previous) = previous {
            if let Err(e) = self.persist_choices(previous) {
                tracing::warn!(error = %e, "remote-compatible choice rollback failed");
            }
        }
    }

    /// Drop a deleted profile's choice. Best effort: a left-over entry is
    /// inert and pruned at the next migration.
    pub(super) fn forget_choice(&self, id: &str) {
        let Ok(mut next) = self.choices() else {
            return;
        };
        if next.profiles.remove(id).is_some() {
            if let Err(e) = self.persist_choices(next) {
                tracing::warn!(profile_id = %id, error = %e, "remote-compatible choice cleanup failed; it is pruned at next start");
            }
        }
    }
}

/// Which saved choice to write.
#[derive(Clone, Copy)]
pub(super) enum ChoiceKey<'a> {
    Profile(&'a str),
    Default(&'a str),
}

/// The account-folder variables an environment sets, as saved.
fn folder_keys<'a>(set_keys: impl Iterator<Item = &'a str>) -> Vec<String> {
    set_keys
        .filter(|key| is_folder_path_key(key))
        .map(str::to_owned)
        .collect()
}

fn doc_folder_keys(entries: &[StoredEnv]) -> Vec<String> {
    folder_keys(entries.iter().filter_map(|entry| match entry {
        StoredEnv::Set { key, .. } => Some(key.as_str()),
        StoredEnv::Unset { .. } => None,
    }))
}

/// The API key and token variables whose addition marks a Default for
/// remote machines (§5.54 "Credential auto-mark").
const CREDENTIAL_KEYS: &[&str] = &[
    "ANTHROPIC_API_KEY",
    "ANTHROPIC_AUTH_TOKEN",
    "CLAUDE_CODE_OAUTH_TOKEN",
    "CODEX_ACCESS_TOKEN",
    "CODEX_API_KEY",
    "OPENAI_API_KEY",
];

/// Whether `next` sets a credential that `saved` did not set: a Default
/// write that adds one is marked for remote machines unless it says
/// otherwise or sets an account folder.
pub(super) fn adds_credential(saved: &[StoredEnv], next: &[StoredEnv]) -> bool {
    let sets = |entries: &[StoredEnv], key: &str| {
        entries.iter().any(
            |entry| matches!(entry, StoredEnv::Set { key: k, .. } if k.eq_ignore_ascii_case(key)),
        )
    };
    CREDENTIAL_KEYS
        .iter()
        .any(|key| sets(next, key) && !sets(saved, key))
}

/// The migration rule, which is today's behaviour: a profile is marked
/// unless it sets an account folder; a Default is marked when it has saved
/// entries and no account folder.
pub(super) fn migration_rule(entries: &[StoredEnv], default: bool) -> bool {
    doc_folder_keys(entries).is_empty() && (!default || !entries.is_empty())
}

/// The read classification of a saved configuration.
pub(super) fn classify(
    choice: Option<bool>,
    entries: &[StoredEnv],
    default: bool,
) -> RemoteCompatibility {
    let keys = doc_folder_keys(entries);
    if !keys.is_empty() {
        return RemoteCompatibility::NotRemoteCompatible {
            reason: REMOTE_INCOMPATIBLE_ACCOUNT_FOLDER,
            keys,
        };
    }
    if choice.unwrap_or_else(|| migration_rule(entries, default)) {
        RemoteCompatibility::Compatible
    } else {
        RemoteCompatibility::NotRemoteCompatible {
            reason: REMOTE_INCOMPATIBLE_NOT_MARKED,
            keys: Vec::new(),
        }
    }
}

/// [`classify`] of a resolved overlay (`None` = no overrides).
fn classify_overlay(
    choice: Option<bool>,
    env: Option<&intent_acp::ProfileEnv>,
    default: bool,
) -> RemoteCompatibility {
    let entries: Vec<StoredEnv> = env
        .into_iter()
        .flat_map(intent_acp::ProfileEnv::entries)
        .map(|(key, value)| match value {
            Some(_) => StoredEnv::Set {
                key: key.to_owned(),
                value: String::new(),
            },
            None => StoredEnv::Unset {
                key: key.to_owned(),
            },
        })
        .collect();
    classify(choice, &entries, default)
}

/// Validate a write's `remoteCompatible` against the environment it saves
/// and resolve the choice to store. `requested` is the param, `current` the
/// effective saved choice (`None` on create).
pub(super) fn resolve_choice(
    requested: Option<bool>,
    current: Option<bool>,
    entries: &[StoredEnv],
    default: bool,
    errors: &mut Vec<FieldError>,
) -> bool {
    let keys = doc_folder_keys(entries);
    if !keys.is_empty() {
        if requested == Some(true) {
            errors.push(FieldError::new(
                "remoteCompatible",
                REMOTE_ACCOUNT_FOLDER_FIELD,
                format!(
                    "{} is an account folder on this backend, so this configuration can't be used on remote machines. Turn off Use on remote machines, or remove the folder.",
                    keys.join(", ")
                ),
            ));
        }
        return false;
    }
    requested
        .or(current)
        .unwrap_or_else(|| migration_rule(entries, default))
}

/// The user-facing name of a base provider.
fn base_display(base: &str) -> &str {
    match base {
        "claude-code" => "Claude Code",
        "codex" => "Codex",
        other => other,
    }
}

/// `profile-not-remote-compatible` (nodes.md "Remote machine errors"):
/// names the configuration, says why and how to fix it. `-32602`, with the
/// variable names but never their values.
pub(crate) fn not_remote_compatible_error(
    reference: &str,
    name: &str,
    reason: &str,
    keys: &[String],
    machine_sign_in: Option<bool>,
    target: RemoteTarget,
) -> Error {
    let base = parse_profile_ref(reference).map_or(reference, |(base, _)| base);
    let provider = base_display(base);
    let folders = keys.join(", ");
    let folder = reason == REMOTE_INCOMPATIBLE_ACCOUNT_FOLDER;
    let message = match (machine_sign_in.is_some(), target, folder) {
        (false, _, false) => format!(
            "{name} isn't marked for remote machines. Turn on Use on remote machines for it in Settings → Providers → {provider}, or pick a configuration that is marked."
        ),
        (false, _, true) => format!(
            "{name} uses an account folder ({folders}) that only exists on this backend, so it can't run on another machine; pick a configuration that uses an API key or token and is marked for remote machines."
        ),
        (true, RemoteTarget::Cloud, false) => format!(
            "{name} isn't marked for remote machines, and E2B has no sign-in of its own; pick a profile with an API key or token that is marked for remote machines."
        ),
        (true, RemoteTarget::Cloud, true) => format!(
            "{name} uses an account folder ({folders}) that only exists on this backend, and E2B has no sign-in of its own; pick a profile with an API key or token that is marked for remote machines."
        ),
        (true, RemoteTarget::Machine, false) => format!(
            "{name} isn't marked for remote machines, and using the machine's own sign-in is off for {provider}. In Settings → Providers → {provider}, turn on either option on the Default row, or pick a profile marked for remote machines."
        ),
        (true, RemoteTarget::Machine, true) => format!(
            "{name} uses an account folder ({folders}) that only exists on this backend, and using the machine's own sign-in is off for {provider}. Turn that on in Settings → Providers → {provider}, or pick a profile marked for remote machines."
        ),
    };
    let mut data = json!({
        "code": PROFILE_NOT_REMOTE_COMPATIBLE,
        "providerRef": reference,
        "name": name,
        "reason": reason,
        "keys": keys,
    });
    if let Some(on) = machine_sign_in {
        data["machineSignIn"] = json!(on);
    }
    Error::provider_profile(false, message, data)
}

impl Services {
    /// Whether an unmarked Default of `base` runs with the machine's own
    /// sign-in (`providers.remoteMachineSignIn`; missing is on).
    pub(crate) fn machine_sign_in_on(&self, base: &str) -> bool {
        self.effective_settings()
            .providers
            .remote_machine_sign_in
            .get(base)
            .copied()
            .unwrap_or(true)
    }

    /// The saved choice of `reference` (a profile or a Default).
    fn choice_of(&self, reference: &str) -> Option<bool> {
        match parse_profile_ref(reference) {
            Some((_, id)) => self.provider_profiles.profile_choice(id),
            None => self.provider_profiles.default_choice(reference),
        }
    }

    /// The display name of `reference` for refusals.
    fn configuration_name(&self, reference: &str) -> String {
        match parse_profile_ref(reference) {
            Some((base, id)) => self
                .provider_profiles
                .profiles()
                .ok()
                .and_then(|profiles| {
                    profiles
                        .iter()
                        .find(|p| p.id == id && p.base_provider == base)
                        .map(|p| p.name.clone())
                })
                .unwrap_or_else(|| reference.to_owned()),
            None => self
                .provider_profiles
                .default_name(reference)
                .unwrap_or_else(|| DEFAULT_NAME.to_owned()),
        }
    }

    /// Whether `reference` may send its settings to a remote machine: the
    /// saved choice, and never with an account folder.
    ///
    /// # Errors
    /// As [`Self::resolve_remote_profile_env`].
    pub async fn remote_compatibility(&self, reference: &str) -> Result<RemoteCompatibility> {
        if !may_have_overlay(reference) {
            return Ok(RemoteCompatibility::Compatible);
        }
        let env = self.resolve_remote_profile_env(reference).await?;
        Ok(classify_overlay(
            self.choice_of(reference),
            env.as_ref(),
            !is_qualified_ref(reference),
        ))
    }

    /// The launch rules: what a remote launch of `reference` sends, or the
    /// refusal. Placement, sub-agents, `agent.setModel`, quota failover and
    /// every start on a machine use it.
    ///
    /// # Errors
    /// `profile-not-remote-compatible`, or as
    /// [`Self::resolve_remote_profile_env`].
    pub async fn remote_launch_plan(
        &self,
        reference: &str,
        target: RemoteTarget,
    ) -> Result<RemoteLaunchPlan> {
        if !may_have_overlay(reference) {
            return Ok(RemoteLaunchPlan {
                env: None,
                machine_sign_in: false,
            });
        }
        let default = !is_qualified_ref(reference);
        let env = self.resolve_remote_profile_env(reference).await?;
        match classify_overlay(self.choice_of(reference), env.as_ref(), default) {
            RemoteCompatibility::Compatible => {
                let machine_sign_in = default && env.is_none();
                Ok(RemoteLaunchPlan {
                    env,
                    machine_sign_in,
                })
            }
            RemoteCompatibility::NotRemoteCompatible { reason, keys } => {
                let sign_in = default.then(|| self.machine_sign_in_on(reference));
                if target == RemoteTarget::Machine && sign_in == Some(true) {
                    return Ok(RemoteLaunchPlan {
                        env: None,
                        machine_sign_in: true,
                    });
                }
                Err(not_remote_compatible_error(
                    reference,
                    &self.configuration_name(reference),
                    reason,
                    &keys,
                    sign_in,
                    target,
                ))
            }
        }
    }

    /// Annotate a Default row with `remoteCompatible`,
    /// `remoteIncompatibility` and `remoteLaunch`.
    pub(super) fn annotate_default(&self, row: &mut Value, base: &str, entries: &[StoredEnv]) {
        let compat = classify(self.provider_profiles.default_choice(base), entries, true);
        row["remoteLaunch"] = json!(default_remote_launch(
            &compat,
            self.machine_sign_in_on(base)
        ));
        compat.annotate(row);
    }

    /// The catalog's `defaultRemoteLaunch` (every profile-capable base) and
    /// `defaultRemoteIncompatibility` (only Defaults that cannot run).
    pub(super) async fn default_remote_catalog(&self) -> (Option<Value>, Option<Value>) {
        let Ok(saved) = self.provider_profiles.defaults() else {
            return (None, None);
        };
        let mut launch = serde_json::Map::new();
        let mut refused = serde_json::Map::new();
        for base in PROFILE_BASE_PROVIDERS {
            let meta = saved.get(*base).cloned().unwrap_or_default();
            let Ok(env) = self.load_default_env(&meta).await else {
                // Launches fail closed on an unreadable environment.
                launch.insert((*base).to_owned(), json!("refused"));
                continue;
            };
            let compat = classify(
                self.provider_profiles.default_choice(base),
                &env.entries,
                true,
            );
            let value = default_remote_launch(&compat, self.machine_sign_in_on(base));
            launch.insert((*base).to_owned(), json!(value));
            if value == "refused" {
                if let Some(incompatibility) = compat.incompatibility_json() {
                    refused.insert((*base).to_owned(), incompatibility);
                }
            }
        }
        (
            (!launch.is_empty()).then_some(Value::Object(launch)),
            (!refused.is_empty()).then_some(Value::Object(refused)),
        )
    }

    /// Write the migration rule for every profile and Default with no saved
    /// choice, and prune choices of deleted profiles, in one write. Run once
    /// at startup, after [`Self::sweep_provider_profile_secrets`]. Reads
    /// already fall back to the rule, so a failure here changes nothing.
    pub async fn migrate_remote_choices(&self) {
        let store = &self.provider_profiles;
        let _guard = store.lock().await;
        let (Ok(profiles), Ok(defaults), Ok(previous)) =
            (store.profiles(), store.defaults(), store.choices())
        else {
            return;
        };
        let mut next = previous.clone();
        next.profiles
            .retain(|id, _| profiles.iter().any(|p| &p.id == id));
        for meta in profiles.iter() {
            if next.profiles.contains_key(&meta.id) {
                continue;
            }
            let Ok(env) = load_env(&self.secrets, &meta.id).await else {
                continue;
            };
            let entries = env.map(|doc| doc.entries).unwrap_or_default();
            next.profiles
                .insert(meta.id.clone(), migration_rule(&entries, false));
        }
        for base in PROFILE_BASE_PROVIDERS {
            if next.defaults.contains_key(*base) {
                continue;
            }
            let meta = defaults.get(*base).cloned().unwrap_or_default();
            let Ok(env) = self.load_default_env(&meta).await else {
                continue;
            };
            next.defaults
                .insert((*base).to_owned(), migration_rule(&env.entries, true));
        }
        if next != previous || store.choices_path.as_ref().is_some_and(|p| !p.exists()) {
            if let Err(e) = store.persist_choices(next) {
                tracing::warn!(error = %e, "remote-compatible choice migration failed; reads keep today's rule");
            }
        }
    }

    /// `providers.remoteMachineSignIn` changed: each Default's
    /// `remoteLaunch` may have moved, so announce a `default-updated` per
    /// base provider (clients re-list).
    pub(crate) async fn announce_machine_sign_in_change(&self) {
        for base in PROFILE_BASE_PROVIDERS {
            let revision = self.provider_profiles.bump_revision();
            crate::publish_event(
                self.event_bus.as_ref(),
                default_changed_event(revision, "default-updated", base, Some(false)),
            )
            .await;
        }
    }
}
