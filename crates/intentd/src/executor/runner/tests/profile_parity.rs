//! Local and paired-machine launches of every provider profile kind run with
//! the same profile environment and the same account folder.
//!
//! A profile's launch overlay is derived on two paths: locally
//! (`InstalledContext::with_profile` and the agent launch's
//! `local_launch_profile`) and on a paired machine (this runner's
//! `node_profile`). During intentd#3509 the Codex API key home was first
//! added to the local path only, so on machines Codex would have signed in
//! with the machine owner's login; review caught it, no test did. This test
//! runs every kind, as a named profile and as a Default configuration, through
//! both paths and requires identical profile overlays (what reaches
//! `SpawnOptions.profile_env` / the installed CLI context), not the final
//! process environment: how that overlay is applied is tested per path.

use std::collections::BTreeMap;

use intent_acp::ProfileEnv;
use intent_services::test_support::profile_launch::{local_profile_launch, resolve_saved_profile};

use super::harness_with;

/// The providers with profile kinds; checked against
/// `intent_providers::PROFILE_BASE_PROVIDERS`, so a new profile-capable
/// provider fails the test until it is covered here.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Provider {
    Claude,
    Codex,
}

/// Every kind the profile dialog saves (cloudlands-fe
/// `provider-profile-kinds.ts`). A variant missing from [`Self::ALL`] is
/// never constructed and fails the build under `dead_code`; a new variant
/// fails [`Self::saved`]'s exhaustive match until its entries are written.
/// The kinds are a frontend concept (never stored), so a kind added only in
/// the frontend is not caught here: add it to this enum alongside.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    /// Signs in with a token (`CLAUDE_CODE_OAUTH_TOKEN` / `CODEX_ACCESS_TOKEN`).
    Token,
    /// Signs in with an API key (`ANTHROPIC_API_KEY` / `CODEX_API_KEY`).
    ApiKey,
    /// A separate login in its own account folder.
    DifferentLogin,
    /// The raw environment editor: any set, empty and unset entries.
    CustomEnv,
}

impl Provider {
    const ALL: [Self; 2] = [Self::Claude, Self::Codex];

    fn base(self) -> &'static str {
        match self {
            Self::Claude => "claude-code",
            Self::Codex => "codex",
        }
    }
}

impl Kind {
    const ALL: [Self; 4] = [
        Self::Token,
        Self::ApiKey,
        Self::DifferentLogin,
        Self::CustomEnv,
    ];

    /// The entries the dialog saves for this kind (values are fake).
    fn saved(self, provider: Provider) -> Vec<(&'static str, Option<&'static str>)> {
        match (provider, self) {
            (Provider::Claude, Self::Token) => vec![
                ("CLAUDE_CODE_OAUTH_TOKEN", Some("sk-ant-oat-parity")),
                ("ANTHROPIC_API_KEY", None),
                ("ANTHROPIC_AUTH_TOKEN", None),
            ],
            (Provider::Claude, Self::ApiKey) => vec![
                ("ANTHROPIC_API_KEY", Some("sk-ant-parity")),
                ("ANTHROPIC_BASE_URL", Some("https://claude.parity.invalid")),
                ("ANTHROPIC_AUTH_TOKEN", None),
                ("CLAUDE_CODE_OAUTH_TOKEN", None),
            ],
            (Provider::Claude, Self::DifferentLogin) => vec![
                ("CLAUDE_CONFIG_DIR", Some("~/intent/profiles/claude/work")),
                ("ANTHROPIC_API_KEY", None),
                ("ANTHROPIC_AUTH_TOKEN", None),
                ("CLAUDE_CODE_OAUTH_TOKEN", None),
            ],
            (Provider::Codex, Self::Token) => vec![
                ("CODEX_ACCESS_TOKEN", Some("a.b.parity")),
                ("CODEX_API_KEY", None),
                ("OPENAI_API_KEY", None),
            ],
            (Provider::Codex, Self::ApiKey) => vec![
                ("CODEX_API_KEY", Some("sk-codex-parity")),
                ("CODEX_ACCESS_TOKEN", None),
                ("OPENAI_API_KEY", None),
            ],
            (Provider::Codex, Self::DifferentLogin) => vec![
                ("CODEX_HOME", Some("~/intent/profiles/codex/work")),
                ("CODEX_ACCESS_TOKEN", None),
                ("CODEX_API_KEY", None),
                ("OPENAI_API_KEY", None),
            ],
            (_, Self::CustomEnv) => vec![
                ("PARITY_SETTING", Some("custom-value")),
                ("PARITY_EMPTY", Some("")),
                ("HTTPS_PROXY", None),
            ],
        }
    }

    /// The Codex folder both paths must agree on, relative to the home:
    /// `Some` when the profile selects one, `None` when Codex keeps the
    /// host's own. Guards against both paths dropping the derivation. Only a
    /// named Codex API key profile gets its own home; a Default
    /// configuration with a key keeps the host's (`with_codex_api_key_home`
    /// applies to `codex@<id>` references only).
    fn codex_folder(self, provider: Provider, default: bool) -> Option<&'static str> {
        match (provider, self) {
            (Provider::Codex, Self::ApiKey) if !default => Some(".intent/codex-api-key/"),
            (Provider::Codex, Self::DifferentLogin) => Some("intent/profiles/codex/work"),
            (Provider::Claude, _)
            | (Provider::Codex, Self::ApiKey | Self::Token | Self::CustomEnv) => None,
        }
    }
}

fn entries(profile: &ProfileEnv) -> BTreeMap<String, Option<String>> {
    profile
        .entries()
        .map(|(k, v)| (k.to_owned(), v.map(str::to_owned)))
        .collect()
}

#[tokio::test]
async fn every_profile_kind_launches_the_same_locally_and_on_a_machine() {
    let mut bases: Vec<&str> = Provider::ALL.iter().map(|p| p.base()).collect();
    let mut expected = intent_providers::PROFILE_BASE_PROVIDERS.to_vec();
    bases.sort_unstable();
    expected.sort_unstable();
    assert_eq!(bases, expected, "cover every profile-capable provider");
    // Never starts a provider: any executable stands in for node, so the
    // test runs (never skips) without node on PATH.
    let h = harness_with("/bin/true".into(), 1, &[]);
    // One host plays both roles: a local launch and a paired machine see the
    // same home, so their derived folders must be byte-identical.
    let home = &h.home;
    for dir in ["intent/profiles/claude/work", "intent/profiles/codex/work"] {
        std::fs::create_dir_all(home.join(dir)).unwrap();
    }
    let bin = tempfile::tempdir().unwrap();
    for (provider, kind, default) in Provider::ALL.into_iter().flat_map(|provider| {
        Kind::ALL
            .into_iter()
            .flat_map(move |kind| [false, true].map(|default| (provider, kind, default)))
    }) {
        let case = format!(
            "{provider:?} {kind:?}{}",
            if default {
                " (Default configuration)"
            } else {
                ""
            }
        );
        let reference = if default {
            provider.base().to_owned()
        } else {
            format!("{}@parity-{kind:?}", provider.base())
        };
        let saved = kind.saved(provider);
        let local_resolved = resolve_saved_profile(&reference, provider.base(), &saved, Some(home));
        let local = local_profile_launch(provider.base(), &local_resolved, home, bin.path());
        let remote_resolved = resolve_saved_profile(&reference, provider.base(), &saved, None);
        let node = h
            .runner
            .node_profile(&remote_resolved.to_node_wire())
            .unwrap_or_else(|e| panic!("{case}: the machine refused the profile: {e}"));

        assert_eq!(
            entries(&local.agent),
            entries(&node),
            "{case}: local agent launch and machine launch differ"
        );
        assert_eq!(
            entries(&local.installed),
            entries(&node),
            "{case}: local installed CLI context and machine launch differ"
        );

        let node_codex_home = node
            .entries()
            .find(|(k, _)| *k == "CODEX_HOME")
            .and_then(|(_, v)| v)
            .map(std::path::PathBuf::from);
        match kind.codex_folder(provider, default) {
            Some(folder) => {
                let path = node_codex_home
                    .clone()
                    .unwrap_or_else(|| panic!("{case}: no CODEX_HOME on the machine"));
                assert!(
                    path.starts_with(home.join(folder)),
                    "{case}: CODEX_HOME {} is not under {folder}",
                    path.display()
                );
                assert!(path.is_dir(), "{case}: {}", path.display());
                assert_eq!(
                    local.installed_codex_home, node_codex_home,
                    "{case}: local and machine Codex folders differ"
                );
            }
            None => assert_eq!(node_codex_home, None, "{case}"),
        }
        if provider == Provider::Codex && node_codex_home.is_none() {
            // Codex keeps the host's own folder on both paths.
            assert_eq!(
                local.installed_codex_home,
                Some(home.join(".codex")),
                "{case}"
            );
        }
    }
}
