//! The local side of a provider profile launch, for the local / paired-machine
//! parity test in `intentd` (`executor::runner::tests::profile_parity`).
//!
//! A profile's launch overlay is derived twice: here on the daemon host
//! (resolution with the daemon's home, then `InstalledContext::with_profile`
//! for probes and quick actions and the agent launch's own derivation), and on
//! a paired machine (resolution without a home, the node wire, then the node
//! runner's `node_profile`). These seams expose the local half through the
//! production functions, so the parity test compares the real code paths.

use std::path::{Path, PathBuf};

use intent_acp::ProfileEnv;

use crate::provider_profiles::{overlay_from, EnvDoc, StoredEnv};

/// A saved profile's overlay as the daemon resolves it for a launch:
/// reserved keys dropped and, with `home`, a leading `~` of an account
/// folder expanded (local launches). `None` keeps values as typed, as a
/// launch on a paired machine receives them.
#[must_use]
pub fn resolve_saved_profile(
    reference: &str,
    base: &str,
    entries: &[(&str, Option<&str>)],
    home: Option<&Path>,
) -> ProfileEnv {
    let doc = EnvDoc {
        fingerprint: "f-parity".to_owned(),
        entries: entries
            .iter()
            .map(|(key, value)| match value {
                Some(value) => StoredEnv::Set {
                    key: (*key).to_owned(),
                    value: (*value).to_owned(),
                },
                None => StoredEnv::Unset {
                    key: (*key).to_owned(),
                },
            })
            .collect(),
    };
    overlay_from(reference, base, doc, home)
}

/// What a local launch runs a resolved profile with.
pub struct LocalProfileLaunch {
    /// The overlay a local agent launch hands to `SpawnOptions.profile_env`.
    pub agent: ProfileEnv,
    /// The overlay probes, quick actions and model catalogs run with:
    /// `InstalledContext::with_profile` for Codex and Claude Code, the
    /// adapter launch overlay for every other provider.
    pub installed: ProfileEnv,
    /// The Codex folder the installed context runs Codex in (Codex only).
    pub installed_codex_home: Option<PathBuf>,
}

/// Run `profile` through both local derivations for `provider_id` (any
/// profile base) on a host whose home is `home`. For Codex and Claude Code a
/// stub CLI is written to `bin`; nothing is executed.
///
/// # Panics
/// On an unknown provider or when the stub CLI cannot be written.
#[cfg(unix)]
#[must_use]
pub fn local_profile_launch(
    provider_id: &str,
    profile: &ProfileEnv,
    home: &Path,
    bin: &Path,
) -> LocalProfileLaunch {
    use std::collections::BTreeMap;
    use std::os::unix::fs::PermissionsExt;
    use std::sync::Arc;

    use intent_providers::installed_cli::InstalledCli;

    use crate::installed_cli::InstalledContext;

    assert!(
        intent_providers::profile_spec(provider_id).is_some(),
        "not a profile base: {provider_id:?}"
    );
    let agent = crate::agent_manager::local_launch_profile(profile, Some(home));
    let Some(cli) = InstalledCli::for_provider(provider_id) else {
        return LocalProfileLaunch {
            agent: (*agent).clone(),
            installed: crate::agent_manager::local_launch_overlay_in(profile, Some(home)),
            installed_codex_home: None,
        };
    };
    std::fs::create_dir_all(bin).unwrap();
    let exe = bin.join(cli.command());
    std::fs::write(&exe, "#!/bin/sh\n").unwrap();
    std::fs::set_permissions(&exe, std::fs::Permissions::from_mode(0o755)).unwrap();
    let runtime = cli.resolve_in_dirs(&[bin.to_owned()], false).unwrap();
    let inherited = BTreeMap::from([
        ("HOME".into(), home.as_os_str().to_owned()),
        ("PATH".into(), "/bin:/usr/bin".into()),
    ]);
    let context = InstalledContext::from_inputs(
        runtime,
        &BTreeMap::new(),
        inherited,
        &intent_core::cli_env::CodexEnvNames::default(),
    )
    .unwrap()
    .with_profile(Some(Arc::new(profile.clone())));
    LocalProfileLaunch {
        agent: (*agent).clone(),
        installed: (**context.profile().unwrap()).clone(),
        installed_codex_home: (cli == InstalledCli::Codex)
            .then(|| context.codex_home())
            .flatten(),
    }
}
