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
//!    never shell-expanded);
//! 3. Intent's own launch wiring (provider env, session/bootstrap variables,
//!    the selected executable, enforced configuration and `PATH`), which a
//!    profile can never override.
//!
//! Values are secrets: this type deliberately has no value-bearing `Debug`.

use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};

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
            .filter_map(|(_, v)| v.clone())
            .filter(|v| !v.is_empty())
            .collect()
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
    fn cache_key_and_secret_values() {
        let profile = sample();
        assert_eq!(profile.cache_key(), "codex@work#fabc");
        assert_eq!(profile.secret_values(), vec!["sk-work-secret".to_string()]);
        assert!(profile.controls("OPENAI_BASE_URL"));
        assert!(!profile.controls("UNRELATED"));
    }
}
