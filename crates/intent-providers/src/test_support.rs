//! Test-only Codex/Claude CLI apparatus shared by every crate's fixtures.
//!
//! Fake CLIs used to print hand-written versions (`2.0.0`, `fixture-cli
//! 1.0.0`). When an adapter or SDK bump raised a minimum, those fixtures
//! suddenly counted as "too old" and unrelated tests failed together
//! (intent-hq/intentd#2567). Ordinary fixtures print [`current_cli_version`]
//! instead; tests about old CLIs keep explicit literals, marked with
//! `// cli-fixture-version: allow — <reason>` (enforced by
//! `crates/intent-providers/tests/cli_fixture_version_lint.rs`).
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
    let provider = match cli {
        InstalledCli::Codex => "codex",
        InstalledCli::Claude => "claude-code",
    };
    let version = requirement(provider)
        .and_then(|required| required.bundled)
        .unwrap_or_else(|| panic!("{provider} adapter metadata declares no bundled CLI pin"))
        .version;
    match cli {
        InstalledCli::Codex => format!("codex-cli {version}"),
        InstalledCli::Claude => format!("{version} (Claude Code)"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn current_versions_satisfy_the_requirement_in_the_real_cli_format() {
        for (cli, provider) in [
            (InstalledCli::Codex, "codex"),
            (InstalledCli::Claude, "claude-code"),
        ] {
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
}
