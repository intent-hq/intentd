//! Guards for `scripts/test-without-provider-clis.sh`, the sandboxed test run
//! that hides every provider CLI the way CI's machine lacks them.
//!
//! - [`hide_list_covers_every_provider_cli`] fails when a provider command is
//!   registered without being added to the script's `PROVIDER_CLIS` list.
//! - [`no_provider_cli_visible`] (ignored by default) asks the daemon's own
//!   discovery whether any provider CLI resolves. The script runs it inside
//!   its sandbox before the real command, and CI's `check` job runs it on the
//!   runner, so a missed hiding location or a runner image that starts
//!   shipping a provider CLI fails loudly instead of silently letting
//!   host-dependent tests pass.

use std::collections::BTreeSet;
use std::path::PathBuf;

use crate::config::ACP_PROVIDERS;

/// Every CLI name provider discovery can resolve: each registered provider's
/// command and secondary binary, plus the real `pi` CLI that pi-acp runs.
/// The mock provider's `node` command is a runtime, not a provider CLI.
fn provider_cli_names() -> BTreeSet<&'static str> {
    let mut names = BTreeSet::from(["pi"]);
    for provider in ACP_PROVIDERS {
        if provider.command != "node" {
            names.insert(provider.command);
        }
        names.extend(provider.requires_secondary_binary);
    }
    names
}

fn script_hide_list() -> BTreeSet<String> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../scripts/test-without-provider-clis.sh");
    let script = std::fs::read_to_string(&path).expect("read the script");
    script
        .lines()
        .skip_while(|line| line.trim() != "PROVIDER_CLIS=(")
        .skip(1)
        .take_while(|line| line.trim() != ")")
        .map(|line| line.trim().to_string())
        .collect()
}

#[test]
fn hide_list_covers_every_provider_cli() {
    let listed = script_hide_list();
    assert!(!listed.is_empty(), "PROVIDER_CLIS=( ... ) not found");
    let missing: Vec<_> = provider_cli_names()
        .into_iter()
        .filter(|name| !listed.contains(*name))
        .collect();
    assert!(
        missing.is_empty(),
        "add {missing:?} to PROVIDER_CLIS in scripts/test-without-provider-clis.sh"
    );
}

#[test]
#[ignore = "run by scripts/test-without-provider-clis.sh and the CI check job"]
fn no_provider_cli_visible() {
    let mut visible = Vec::new();
    for provider in ACP_PROVIDERS {
        // An npx-only provider's command is never resolved on PATH (the
        // pinned npx package runs instead); its secondary CLI is.
        let commands = (provider.command != "node" && provider.npx_only_package.is_none())
            .then_some(provider.command)
            .into_iter()
            .chain(provider.requires_secondary_binary);
        for command in commands {
            if let Some(path) = crate::discover::find_provider_binary(provider.id, command, None) {
                visible.push(format!("{} ({command}): {}", provider.id, path.display()));
            }
        }
    }
    for cli in [
        crate::installed_cli::InstalledCli::Codex,
        crate::installed_cli::InstalledCli::Claude,
    ] {
        if let Ok(runtime) = cli.resolve() {
            visible.push(format!("{cli:?}: {}", runtime.path().display()));
        }
    }
    if let Some(path) = crate::discover::find_pi_cli("pi") {
        visible.push(format!("pi: {}", path.display()));
    }
    if std::env::var_os("INTENTD_EXPECT_NO_NODE").is_some() {
        visible.extend(crate::discover::find_node().map(|p| format!("node: {}", p.display())));
        visible.extend(crate::discover::find_npx().map(|p| format!("npx: {}", p.display())));
    }
    assert!(
        visible.is_empty(),
        "provider CLIs visible to discovery (a test touching them passes here but \
         fails on CI): {visible:#?}"
    );
}
