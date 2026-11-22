//! The resolved environment of one named provider profile (PROTOCOL §5.54).
//!
//! The daemon reads a profile's saved `set` / `unset` entries when it prepares
//! a provider process or probe and carries them here, together with the
//! profile reference and its opaque settings fingerprint. Every execution path
//! — auth probe, model probe, one-shot call, persistent launch — applies the
//! same overlay with the same precedence:
//!
//! 1. inherited daemon environment and captured login-shell credentials;
//! 2. **the profile**: `set` replaces, `unset` removes (values are literal,
//!    never shell-expanded — except that the daemon expands a leading `~` /
//!    `~/` of an account folder ([`ACCOUNT_DIR_KEYS`]) to its home directory
//!    before building this overlay);
//! 3. Intent's own launch wiring (provider env, session/bootstrap variables,
//!    the selected executable, enforced configuration and `PATH`), which a
//!    profile can never override.
//!
//! Values are secrets: this type deliberately has no value-bearing `Debug`.

use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::path::Path;
use std::sync::LazyLock;

use serde_json::{json, Map, Value};

/// Account-directory variables whose leading `~` is expanded where the
/// provider runs: by the daemon for local launches, by the node for remote
/// ones (keys compared ignoring ASCII case). Every profile base's
/// [`intent_providers::ProfileSpec::account_dir_keys`].
pub static ACCOUNT_DIR_KEYS: LazyLock<Vec<&'static str>> =
    LazyLock::new(|| intent_providers::profile_account_dir_keys().collect());

pub use intent_providers::CODEX_API_KEY_AUTH_REQUEST;
const DEFAULT_AUTH_REQUEST: &str = "DEFAULT_AUTH_REQUEST";

/// Launch-only wiring an API key home adds next to the home folder
/// (`DEFAULT_AUTH_REQUEST`, Cursor's credential store): not a secret.
fn is_api_key_home_wiring(key: &str) -> bool {
    intent_providers::ACP_PROVIDERS
        .iter()
        .filter_map(|p| p.profile.as_ref())
        .flat_map(|spec| {
            spec.api_key_home
                .iter()
                .flat_map(|home| home.launch_env.iter())
                .chain(spec.credential_launch_env.iter())
        })
        .any(|(k, _)| *k == key)
}

fn is_account_dir_key(key: &str) -> bool {
    ACCOUNT_DIR_KEYS.iter().any(|k| k.eq_ignore_ascii_case(key))
}

/// Expand a leading `~` in an account-directory value: for
/// [`ACCOUNT_DIR_KEYS`] only, a value of exactly `~` or one starting with
/// `~/` is joined onto `home`. `~user`, variables and every other key stay
/// literal, and without a home the value is used as typed.
#[must_use]
pub fn expand_account_dir(key: &str, value: &str, home: Option<&Path>) -> String {
    let Some(home) = home.and_then(Path::to_str) else {
        return value.to_owned();
    };
    if !is_account_dir_key(key) {
        return value.to_owned();
    }
    match value.strip_prefix('~') {
        Some("") => home.to_owned(),
        Some(rest) if rest.starts_with('/') => format!("{}{rest}", home.trim_end_matches('/')),
        _ => value.to_owned(),
    }
}

/// A profile's account folder does not exist on the node that would run the
/// provider. Names the variable only; the value is never rendered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeAccountDirMissing {
    key: String,
}

impl NodeAccountDirMissing {
    /// The account-directory variable the profile sets.
    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }
}

impl std::fmt::Display for NodeAccountDirMissing {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "The selected provider profile sets {key} to a folder that does not exist on this node. \
             Create it and sign in on this node with {key} pointing at it, or change the profile; \
             no other account was used.",
            key = self.key
        )
    }
}

impl std::error::Error for NodeAccountDirMissing {}

/// One profile's resolved overlay. Cheap to clone; never logged.
#[derive(Clone, PartialEq, Eq)]
pub struct ProfileEnv {
    provider_ref: String,
    fingerprint: String,
    /// Ordered entries: `Some(value)` sets (an empty string is a real value),
    /// `None` unsets an inherited variable.
    entries: Vec<(String, Option<String>)>,
}

impl ProfileEnv {
    /// Build an overlay. Callers filter reserved keys before constructing it.
    #[must_use]
    pub fn new(
        provider_ref: impl Into<String>,
        fingerprint: impl Into<String>,
        entries: Vec<(String, Option<String>)>,
    ) -> Self {
        Self {
            provider_ref: provider_ref.into(),
            fingerprint: fingerprint.into(),
            entries,
        }
    }

    /// The profile reference (`<baseProviderId>@<profileId>`).
    #[must_use]
    pub fn provider_ref(&self) -> &str {
        &self.provider_ref
    }

    /// The saved settings fingerprint this overlay was read under.
    #[must_use]
    pub fn fingerprint(&self) -> &str {
        &self.fingerprint
    }

    /// Cache identity: the reference plus the settings fingerprint, so cached
    /// models and auth verdicts never cross profiles or configuration edits.
    #[must_use]
    pub fn cache_key(&self) -> String {
        format!("{}#{}", self.provider_ref, self.fingerprint)
    }

    /// Entries in saved order (`None` = unset).
    pub fn entries(&self) -> impl Iterator<Item = (&str, Option<&str>)> {
        self.entries.iter().map(|(k, v)| (k.as_str(), v.as_deref()))
    }

    /// Whether the profile sets or unsets `key`.
    #[must_use]
    pub fn controls(&self, key: &str) -> bool {
        self.entries.iter().any(|(k, _)| k == key)
    }

    /// Non-empty `set` values, for output redaction.
    #[must_use]
    pub fn secret_values(&self) -> Vec<String> {
        self.entries
            .iter()
            .filter(|(k, _)| !is_api_key_home_wiring(k))
            .filter_map(|(_, v)| v.clone())
            .filter(|v| !v.is_empty())
            .collect()
    }

    /// A profile whose base has an API key home
    /// ([`intent_providers::ApiKeyHome`]: Codex, Grok, Cursor) and that sets
    /// a non-empty key and not the home variable, resolved where the provider
    /// runs (`home` is that host's home): the overlay gains `<home_key>=<home>
    /// /.intent/<dir>/<profile id>-<key digest>` (created) plus the base's
    /// launch variables, so the CLI never uses the user's own saved login.
    /// A changed key gets a new home, so a stale login never wins.
    /// Launch-only: never saved. A Default configuration, any other profile,
    /// or no home is returned unchanged.
    ///
    /// Before that, a profile that sets one of its base's credentials gets
    /// the base's `credential_launch_env` (Cursor's memory credential store),
    /// whatever else it sets.
    #[must_use]
    pub fn with_api_key_home(mut self, home: Option<&Path>) -> Self {
        let Some((base, _)) = self.provider_ref.split_once('@') else {
            return self;
        };
        let Some(spec) = intent_providers::profile_spec(base) else {
            return self;
        };
        let sets_credential = spec
            .credential_keys
            .iter()
            .any(|key| self.set_value_ignoring_case(key).is_some());
        if sets_credential {
            for (key, value) in spec.credential_launch_env {
                if !self
                    .entries
                    .iter()
                    .any(|(k, v)| k == key && v.as_deref() == Some(value))
                {
                    self.entries
                        .push(((*key).to_owned(), Some((*value).to_owned())));
                }
            }
        }
        self.with_own_home(home, spec.api_key_home)
    }

    fn with_own_home(
        mut self,
        home: Option<&Path>,
        spec: Option<intent_providers::ApiKeyHome>,
    ) -> Self {
        use sha2::{Digest, Sha256};
        use std::fmt::Write as _;
        let Some(id) = self
            .provider_ref
            .split_once('@')
            .map(|(_, id)| id.to_owned())
        else {
            return self;
        };
        let Some(spec) = spec else {
            return self;
        };
        let (Some(home), Some(key)) = (home, self.set_value_ignoring_case(spec.key)) else {
            return self;
        };
        if self.controls_ignoring_case(spec.home_key) {
            return self;
        }
        let digest = Sha256::digest(key.as_bytes()).iter().take(8).fold(
            String::with_capacity(16),
            |mut hex, b| {
                let _ = write!(hex, "{b:02x}");
                hex
            },
        );
        let id = id
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                    c
                } else {
                    '-'
                }
            })
            .collect::<String>();
        let api_key_home = home
            .join(".intent")
            .join(spec.dir_name)
            .join(format!("{id}-{digest}"));
        if let Err(e) = create_private_dir(&api_key_home) {
            tracing::warn!(
                home_key = spec.home_key,
                "could not create the home of an API key profile: {e}"
            );
        }
        self.entries.push((
            spec.home_key.to_owned(),
            Some(api_key_home.to_string_lossy().into_owned()),
        ));
        for (key, value) in spec.launch_env {
            self.entries
                .push(((*key).to_owned(), Some((*value).to_owned())));
        }
        self
    }

    /// Whether [`Self::with_api_key_home`] applied to a Codex profile: the adapter signs
    /// this profile in with its key at launch.
    #[must_use]
    pub fn signs_in_with_codex_api_key(&self) -> bool {
        self.entries.iter().any(|(k, v)| {
            k == DEFAULT_AUTH_REQUEST && v.as_deref() == Some(CODEX_API_KEY_AUTH_REQUEST)
        })
    }

    fn set_value_ignoring_case(&self, key: &str) -> Option<&str> {
        self.entries
            .iter()
            .filter(|(k, _)| k.eq_ignore_ascii_case(key))
            .find_map(|(_, v)| v.as_deref())
            .filter(|v| !v.is_empty())
    }

    fn controls_ignoring_case(&self, key: &str) -> bool {
        self.entries
            .iter()
            .any(|(k, _)| k.eq_ignore_ascii_case(key))
    }

    /// Apply the overlay to a resolved environment map.
    pub fn apply_to_map(&self, env: &mut BTreeMap<OsString, OsString>) {
        for (key, value) in &self.entries {
            match value {
                Some(value) => {
                    env.insert(OsString::from(key), OsString::from(value));
                }
                None => {
                    env.remove(OsStr::new(key));
                }
            }
        }
    }

    /// The head→node form of this overlay: `{ providerRef, fingerprint,
    /// env: [{ key, value | null }] }`. Values are secrets: callers send it
    /// only on the authenticated launch reply and never log it.
    #[must_use]
    pub fn to_node_wire(&self) -> Value {
        json!({
            "providerRef": self.provider_ref,
            "fingerprint": self.fingerprint,
            "env": self
                .entries
                .iter()
                .map(|(key, value)| json!({ "key": key, "value": value }))
                .collect::<Vec<_>>(),
        })
    }

    /// Parse [`Self::to_node_wire`] strictly; any unknown field, missing
    /// field or mistyped entry rejects the whole overlay.
    #[must_use]
    pub fn from_node_wire(wire: &Value) -> Option<Self> {
        fn exact<'a>(value: &'a Value, keys: &[&str]) -> Option<&'a Map<String, Value>> {
            let object = value.as_object()?;
            (object.len() == keys.len() && keys.iter().all(|k| object.contains_key(*k)))
                .then_some(object)
        }
        let object = exact(wire, &["providerRef", "fingerprint", "env"])?;
        let entries = object["env"]
            .as_array()?
            .iter()
            .map(|entry| {
                let entry = exact(entry, &["key", "value"])?;
                let key = entry["key"].as_str().filter(|k| !k.is_empty())?;
                let value = match &entry["value"] {
                    Value::Null => None,
                    Value::String(value) => Some(value.clone()),
                    _ => return None,
                };
                Some((key.to_owned(), value))
            })
            .collect::<Option<Vec<_>>>()?;
        Some(Self::new(
            object["providerRef"].as_str()?,
            object["fingerprint"].as_str()?,
            entries,
        ))
    }

    /// Resolve a received overlay on the node that runs the provider:
    /// expand a leading `~` of an account directory against `home` (the
    /// node's home), then require every account directory the profile sets
    /// to be an existing absolute folder there. A missing folder is a sign-in
    /// error for this node; nothing falls back to another account.
    ///
    /// # Errors
    /// [`NodeAccountDirMissing`] naming the variable.
    pub fn for_node(&self, home: Option<&Path>) -> Result<Self, NodeAccountDirMissing> {
        let entries = self
            .entries
            .iter()
            .map(|(key, value)| {
                let Some(value) = value else {
                    return Ok((key.clone(), None));
                };
                if !is_account_dir_key(key) {
                    return Ok((key.clone(), Some(value.clone())));
                }
                let expanded = expand_account_dir(key, value, home);
                let path = Path::new(&expanded);
                if path.is_absolute() && path.is_dir() {
                    Ok((key.clone(), Some(expanded)))
                } else {
                    Err(NodeAccountDirMissing { key: key.clone() })
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self::new(
            self.provider_ref.clone(),
            self.fingerprint.clone(),
            entries,
        ))
    }

    /// Apply the overlay to a command as explicit overrides.
    pub fn apply_to_command(&self, command: &mut tokio::process::Command) {
        for (key, value) in &self.entries {
            match value {
                Some(value) => command.env(key, value),
                None => command.env_remove(key),
            };
        }
    }
}

fn create_private_dir(path: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

impl std::fmt::Debug for ProfileEnv {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProfileEnv")
            .field("provider_ref", &self.provider_ref)
            .field("fingerprint", &self.fingerprint)
            .field(
                "keys",
                &self.entries.iter().map(|(k, _)| k).collect::<Vec<_>>(),
            )
            .finish()
    }
}

#[cfg(test)]
mod tests {

    fn codex(entries: &[(&str, Option<&str>)]) -> ProfileEnv {
        ProfileEnv::new(
            "codex@work/1",
            "f1",
            entries
                .iter()
                .map(|(k, v)| ((*k).to_owned(), v.map(str::to_owned)))
                .collect(),
        )
    }

    fn value<'a>(profile: &'a ProfileEnv, key: &str) -> Option<&'a str> {
        profile
            .entries()
            .find(|(k, _)| *k == key)
            .and_then(|(_, v)| v)
    }

    #[test]
    fn codex_api_key_profile_gets_a_home_per_key_and_the_api_key_sign_in() {
        let home = tempfile::tempdir().unwrap();
        let one = codex(&[("CODEX_API_KEY", Some("sk-one")), ("OPENAI_API_KEY", None)])
            .with_api_key_home(Some(home.path()));
        let path = std::path::PathBuf::from(value(&one, "CODEX_HOME").unwrap());
        assert!(path.starts_with(home.path().join(".intent/codex-api-key")));
        assert!(path
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .starts_with("work-1-"));
        assert!(path.is_dir());
        assert_eq!(
            value(&one, "DEFAULT_AUTH_REQUEST"),
            Some(CODEX_API_KEY_AUTH_REQUEST)
        );
        assert!(one.signs_in_with_codex_api_key());
        assert!(!one
            .secret_values()
            .contains(&CODEX_API_KEY_AUTH_REQUEST.to_owned()));
        let again =
            codex(&[("CODEX_API_KEY", Some("sk-one"))]).with_api_key_home(Some(home.path()));
        assert_eq!(value(&again, "CODEX_HOME"), value(&one, "CODEX_HOME"));
        let two = codex(&[("CODEX_API_KEY", Some("sk-two"))]).with_api_key_home(Some(home.path()));
        assert_ne!(value(&two, "CODEX_HOME"), value(&one, "CODEX_HOME"));
        // Applying it twice changes nothing.
        let twice = one.clone().with_api_key_home(Some(home.path()));
        assert_eq!(twice.entries().count(), one.entries().count());
    }

    fn profile(reference: &str, entries: &[(&str, Option<&str>)]) -> ProfileEnv {
        ProfileEnv::new(
            reference,
            "f1",
            entries
                .iter()
                .map(|(k, v)| ((*k).to_owned(), v.map(str::to_owned)))
                .collect(),
        )
    }

    #[test]
    fn grok_api_key_profile_gets_its_own_grok_home_per_key() {
        let home = tempfile::tempdir().unwrap();
        let one = profile("grok@p1", &[("XAI_API_KEY", Some("xai-one"))])
            .with_api_key_home(Some(home.path()));
        let path = std::path::PathBuf::from(value(&one, "GROK_HOME").unwrap());
        assert!(path.starts_with(home.path().join(".intent/grok-api-key")));
        assert!(path
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .starts_with("p1-"));
        assert!(path.is_dir());
        assert!(!one.signs_in_with_codex_api_key());
        let two = profile("grok@p1", &[("XAI_API_KEY", Some("xai-two"))])
            .with_api_key_home(Some(home.path()));
        assert_ne!(value(&two, "GROK_HOME"), value(&one, "GROK_HOME"));
        let twice = one.clone().with_api_key_home(Some(home.path()));
        assert_eq!(twice.entries().count(), one.entries().count());
    }

    #[test]
    fn cursor_api_key_profile_ignores_a_stored_login_and_keeps_its_own_config() {
        let home = tempfile::tempdir().unwrap();
        let one = profile(
            "cursor@p2",
            &[
                ("CURSOR_API_KEY", Some("key_one")),
                ("CURSOR_AUTH_TOKEN", None),
            ],
        )
        .with_api_key_home(Some(home.path()));
        assert_eq!(value(&one, "AGENT_CLI_CREDENTIAL_STORE"), Some("memory"));
        let path = std::path::PathBuf::from(value(&one, "CURSOR_CONFIG_DIR").unwrap());
        assert!(path.starts_with(home.path().join(".intent/cursor-api-key")));
        assert!(path.is_dir());
        // The store switch is wiring, not a secret to redact from output.
        assert!(!one.secret_values().contains(&"memory".to_owned()));
        let twice = one.clone().with_api_key_home(Some(home.path()));
        assert_eq!(twice.entries().count(), one.entries().count());
        // Its own config dir, or a token instead of a key: still no stored
        // login (the login is not in the config dir), but no Intent dir.
        for own in [
            profile(
                "cursor@p",
                &[
                    ("CURSOR_API_KEY", Some("k")),
                    ("CURSOR_CONFIG_DIR", Some("/c")),
                ],
            ),
            profile("cursor@p", &[("CURSOR_AUTH_TOKEN", Some("t"))]),
        ] {
            let after = own.clone().with_api_key_home(Some(home.path()));
            assert_eq!(value(&after, "AGENT_CLI_CREDENTIAL_STORE"), Some("memory"));
            assert_eq!(after.entries().count(), own.entries().count() + 1);
            assert!(!value(&after, "CURSOR_CONFIG_DIR").is_some_and(|v| v.contains(".intent")));
        }
    }

    #[test]
    fn providers_whose_key_wins_and_default_configs_are_left_as_saved() {
        let home = tempfile::tempdir().unwrap();
        for profile in [
            profile("droid@p", &[("FACTORY_API_KEY", Some("fk-1"))]),
            profile("auggie@p", &[("AUGMENT_SESSION_AUTH", Some("{}"))]),
            profile("grok", &[("XAI_API_KEY", Some("xai-1"))]),
            profile("cursor", &[("CURSOR_API_KEY", Some("k"))]),
            profile(
                "grok@p",
                &[("XAI_API_KEY", Some("x")), ("GROK_HOME", Some("/g"))],
            ),
            profile(
                "cursor@p",
                &[("CURSOR_API_KEY", Some("")), ("HTTPS_PROXY", None)],
            ),
        ] {
            let before = profile.entries().count();
            let after = profile.clone().with_api_key_home(Some(home.path()));
            assert_eq!(
                after.entries().count(),
                before,
                "{}",
                profile.provider_ref()
            );
        }
        assert!(!home.path().join(".intent").exists());
    }

    #[test]
    fn account_dir_keys_come_from_every_profile_base() {
        for key in ["CLAUDE_CONFIG_DIR", "CODEX_HOME", "GROK_HOME"] {
            assert!(ACCOUNT_DIR_KEYS.contains(&key), "{key}");
        }
        assert_eq!(
            expand_account_dir("grok_home", "~/.grok-work", Some(Path::new("/h"))),
            "/h/.grok-work"
        );
        assert_eq!(
            expand_account_dir("CURSOR_CONFIG_DIR", "~/.c", Some(Path::new("/h"))),
            "~/.c"
        );
    }

    #[test]
    fn other_profiles_are_left_as_saved() {
        let home = tempfile::tempdir().unwrap();
        let claude = ProfileEnv::new(
            "claude-code@p",
            "f1",
            vec![("CODEX_API_KEY".into(), Some("sk".into()))],
        );
        for profile in [
            claude,
            codex(&[("CODEX_ACCESS_TOKEN", Some("a.b.c"))]),
            codex(&[("CODEX_API_KEY", Some(""))]),
            codex(&[("CODEX_API_KEY", Some("sk")), ("CODEX_HOME", Some("/x"))]),
        ] {
            let before = profile.entries().count();
            let after = profile.clone().with_api_key_home(Some(home.path()));
            assert_eq!(after.entries().count(), before);
            assert!(!after.signs_in_with_codex_api_key());
        }
        assert!(!home.path().join(".intent").exists());
    }

    use super::*;

    fn sample() -> ProfileEnv {
        ProfileEnv::new(
            "codex@work",
            "fabc",
            vec![
                ("OPENAI_API_KEY".into(), Some("sk-work-secret".into())),
                ("EMPTY_VALUE".into(), Some(String::new())),
                ("OPENAI_BASE_URL".into(), None),
            ],
        )
    }

    #[test]
    fn debug_names_keys_but_never_values() {
        let rendered = format!("{:?}", sample());
        assert!(rendered.contains("OPENAI_API_KEY"));
        assert!(rendered.contains("codex@work"));
        assert!(!rendered.contains("sk-work-secret"));
    }

    #[test]
    fn map_overlay_sets_empty_values_and_unsets() {
        let mut env = BTreeMap::from([
            (
                OsString::from("OPENAI_API_KEY"),
                OsString::from("inherited"),
            ),
            (
                OsString::from("OPENAI_BASE_URL"),
                OsString::from("https://x"),
            ),
            (OsString::from("UNRELATED"), OsString::from("kept")),
        ]);
        sample().apply_to_map(&mut env);
        assert_eq!(env[OsStr::new("OPENAI_API_KEY")], "sk-work-secret");
        assert_eq!(env[OsStr::new("EMPTY_VALUE")], "");
        assert!(!env.contains_key(OsStr::new("OPENAI_BASE_URL")));
        assert_eq!(env[OsStr::new("UNRELATED")], "kept");
    }

    #[test]
    fn node_wire_round_trips_set_empty_and_unset_in_order() {
        let profile = sample();
        let wire = profile.to_node_wire();
        assert_eq!(
            wire,
            serde_json::json!({
                "providerRef": "codex@work",
                "fingerprint": "fabc",
                "env": [
                    { "key": "OPENAI_API_KEY", "value": "sk-work-secret" },
                    { "key": "EMPTY_VALUE", "value": "" },
                    { "key": "OPENAI_BASE_URL", "value": null },
                ],
            })
        );
        assert_eq!(ProfileEnv::from_node_wire(&wire), Some(profile));
    }

    #[test]
    fn node_wire_rejects_malformed_overlays() {
        let good = sample().to_node_wire();
        let mut cases = Vec::new();
        for field in ["providerRef", "fingerprint", "env"] {
            let mut missing = good.clone();
            missing.as_object_mut().unwrap().remove(field);
            cases.push(missing);
        }
        let mut extra = good.clone();
        extra["secret"] = serde_json::json!("x");
        cases.push(extra);
        for entry in [
            serde_json::json!({ "key": 1, "value": "x" }),
            serde_json::json!({ "key": "", "value": "x" }),
            serde_json::json!({ "key": "A", "value": 7 }),
            serde_json::json!({ "key": "A" }),
            serde_json::json!({ "key": "A", "value": "x", "extra": true }),
            serde_json::json!(["A", "x"]),
        ] {
            let mut bad = good.clone();
            bad["env"] = serde_json::json!([entry]);
            cases.push(bad);
        }
        cases.push(serde_json::json!(null));
        for case in cases {
            assert_eq!(ProfileEnv::from_node_wire(&case), None, "{case}");
        }
    }

    fn node_profile(key: &str, value: &str) -> ProfileEnv {
        ProfileEnv::new(
            "codex@work",
            "fabc",
            vec![
                (key.into(), Some(value.into())),
                (
                    "CODEX_ACCESS_TOKEN".into(),
                    Some("node-secret-token".into()),
                ),
            ],
        )
    }

    #[test]
    fn account_dirs_expand_tilde_against_the_node_home() {
        let node = tempfile::tempdir().unwrap();
        std::fs::create_dir(node.path().join(".codex-work")).unwrap();
        std::fs::create_dir(node.path().join(".claude-work")).unwrap();
        for (key, dir) in [
            ("CODEX_HOME", ".codex-work"),
            ("CLAUDE_CONFIG_DIR", ".claude-work"),
        ] {
            let resolved = node_profile(key, &format!("~/{dir}"))
                .for_node(Some(node.path()))
                .unwrap();
            let expected = node.path().join(dir).to_string_lossy().into_owned();
            assert_eq!(
                resolved.entries().collect::<Vec<_>>(),
                vec![
                    (key, Some(expected.as_str())),
                    ("CODEX_ACCESS_TOKEN", Some("node-secret-token")),
                ]
            );
            assert_eq!(resolved.provider_ref(), "codex@work");
        }
        let bare = node_profile("CODEX_HOME", "~")
            .for_node(Some(node.path()))
            .unwrap();
        assert_eq!(
            bare.entries().next(),
            Some(("CODEX_HOME", Some(node.path().to_str().unwrap())))
        );
        let absolute = node.path().join(".codex-work");
        let kept = node_profile("CODEX_HOME", absolute.to_str().unwrap())
            .for_node(Some(node.path()))
            .unwrap();
        assert_eq!(
            kept.entries().next(),
            Some(("CODEX_HOME", Some(absolute.to_str().unwrap())))
        );
    }

    #[test]
    fn other_keys_stay_literal_on_the_node() {
        let node = tempfile::tempdir().unwrap();
        let profile = ProfileEnv::new(
            "claude-code@team",
            "f1",
            vec![
                ("OTHER_DIR".into(), Some("~/kept".into())),
                ("CLAUDE_CONFIG_DIR".into(), None),
            ],
        );
        assert_eq!(profile.for_node(Some(node.path())).unwrap(), profile);
    }

    #[test]
    fn a_missing_account_folder_is_a_node_specific_sign_in_error() {
        let node = tempfile::tempdir().unwrap();
        for (key, value, home) in [
            ("CODEX_HOME", "~/.codex-missing", Some(node.path())),
            ("CLAUDE_CONFIG_DIR", "~/.claude-missing", Some(node.path())),
            ("CODEX_HOME", "~/.codex-missing", None),
            ("CODEX_HOME", "~other/.codex", Some(node.path())),
            ("CODEX_HOME", "relative/.codex", Some(node.path())),
        ] {
            let err = node_profile(key, value).for_node(home).unwrap_err();
            let message = err.to_string();
            assert_eq!(err.key(), key);
            assert!(message.contains(key), "{message}");
            assert!(message.contains("this node"), "{message}");
            assert!(message.contains("sign in"), "{message}");
            assert!(!message.contains("node-secret-token"), "{message}");
            assert!(!format!("{err:?}").contains("node-secret-token"));
        }
        let file = node.path().join("not-a-dir");
        std::fs::write(&file, "x").unwrap();
        assert!(node_profile("CODEX_HOME", file.to_str().unwrap())
            .for_node(Some(node.path()))
            .is_err());
    }

    #[test]
    fn cache_key_and_secret_values() {
        let profile = sample();
        assert_eq!(profile.cache_key(), "codex@work#fabc");
        assert_eq!(profile.secret_values(), vec!["sk-work-secret".to_string()]);
        assert!(profile.controls("OPENAI_BASE_URL"));
        assert!(!profile.controls("UNRELATED"));
    }
}
