//! Test-only Codex/Claude CLI apparatus shared by every crate's fixtures.
//!
//! Fake CLIs used to print hand-written versions (`2.0.0`, `fixture-cli
//! 1.0.0`). When an adapter or SDK bump raised a minimum, those fixtures
//! suddenly counted as "too old" and unrelated tests failed together
//! (intent-hq/intentd#2567). Ordinary fixtures print [`current_cli_version`]
//! (or are [`current_cli_script`]) instead; tests that need an exact version
//! keep the literal, marked with `// cli-fixture-version: allow — <reason>`
//! (enforced by `crates/intent-providers/tests/cli_fixture_version_lint.rs`).
//!
//! Compiled only for this crate's tests or with the `test-support` feature,
//! which other crates enable from `[dev-dependencies]` alone.

use crate::adapter_cli::requirement;
use crate::installed_cli::InstalledCli;

/// `--version` output of a `cli` that satisfies the current adapter-derived
/// requirement, formatted like the real CLI: Codex prints
/// `codex-cli <version>`, Claude prints `<version> (Claude Code)`. The version
/// is Intent's bundled pin, which always satisfies the minimum and follows
/// every adapter bump.
///
/// # Panics
/// When the adapter metadata declares no bundled pin for `cli`.
#[must_use]
pub fn current_cli_version(cli: InstalledCli) -> String {
    let provider = cli.provider_id();
    let version = requirement(provider)
        .and_then(|required| required.bundled)
        .unwrap_or_else(|| panic!("{provider} adapter metadata declares no bundled CLI pin"))
        .version;
    match cli {
        InstalledCli::Codex => format!("codex-cli {version}"),
        InstalledCli::Claude => format!("{version} (Claude Code)"),
    }
}

/// A `/bin/sh` fake `cli` that answers exactly `--version` with
/// [`current_cli_version`] and exits 91 for any other invocation, so a
/// fixture that is only version-probed can never run a session.
#[must_use]
pub fn current_cli_script(cli: InstalledCli) -> String {
    format!(
        "#!/bin/sh\n[ \"$#\" = 1 ] && [ \"$1\" = --version ] || exit 91\nprintf '%s\\n' '{}'\n",
        current_cli_version(cli)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn current_versions_satisfy_the_requirement_in_the_real_cli_format() {
        for cli in [InstalledCli::Codex, InstalledCli::Claude] {
            let provider = cli.provider_id();
            assert_eq!(InstalledCli::for_provider(provider), Some(cli));
            let output = current_cli_version(cli);
            let required = requirement(provider).unwrap();
            assert_eq!(required.version_ok(&output), Some(true), "{output}");
            let pin = required.bundled.unwrap().version.to_string();
            let expected = match cli {
                InstalledCli::Codex => format!("codex-cli {pin}"),
                InstalledCli::Claude => format!("{pin} (Claude Code)"),
            };
            assert_eq!(output, expected);
            assert_eq!(
                crate::adapter_cli::validate_launch_version(provider, &output),
                Ok(())
            );
            assert_eq!(
                crate::adapter_cli::bundled_fallback(provider, &output),
                None
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn current_cli_script_answers_only_version() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        for cli in [InstalledCli::Codex, InstalledCli::Claude] {
            let path = dir.path().join(cli.command());
            std::fs::write(&path, current_cli_script(cli)).unwrap();
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
            let run = |args: &[&str]| {
                std::process::Command::new(&path)
                    .args(args)
                    .output()
                    .unwrap()
            };
            let version = run(&["--version"]);
            assert!(version.status.success());
            assert_eq!(
                String::from_utf8(version.stdout).unwrap(),
                format!("{}\n", current_cli_version(cli))
            );
            for args in [&[][..], &["-p", "hi"], &["--version", "extra"]] {
                assert_eq!(run(args).status.code(), Some(91), "{args:?}");
            }
        }
    }
}
