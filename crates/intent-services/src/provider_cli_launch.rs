//! One-shot provider CLI commands (model list, sign-in probe): the installed
//! CLI when found, else the provider's pinned npm package through npx
//! ([`intent_providers::ProviderConfig::fallback_npx_package`]), matching how
//! agents launch.

use std::path::{Path, PathBuf};
use std::time::Duration;

/// A first npx run downloads the package (~12 s for `OpenCode` on a fast link).
const NPX_TIMEOUT: Duration = Duration::from_secs(120);

/// Where a one-shot provider command runs from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ProviderCli {
    Installed(PathBuf),
    Npx { npx: PathBuf, package: String },
}

impl ProviderCli {
    /// The installed CLI (honoring `override_path`), else npx when the
    /// provider has an npm package and npx is present.
    pub(crate) fn resolve(provider_id: &str, override_path: Option<&str>) -> Option<Self> {
        let provider = intent_providers::find_provider(provider_id)?;
        intent_providers::find_provider_binary(provider_id, provider.command, override_path)
            .map(Self::Installed)
            .or_else(|| Self::npx(provider_id))
    }

    /// The provider's npm package through npx, when it has one.
    pub(crate) fn npx(provider_id: &str) -> Option<Self> {
        let package = intent_providers::find_provider(provider_id)?.fallback_npx_package?;
        Some(Self::Npx {
            npx: intent_providers::find_npx()?,
            package: package.to_owned(),
        })
    }

    /// The executable and the arguments that come before the CLI's own.
    pub(crate) fn program(&self) -> (&Path, Vec<&str>) {
        self.program_with("-y")
    }

    /// [`Self::program`] that never downloads: npx runs the package only
    /// from its cache and fails otherwise (`--no`). For sign-in checks, which
    /// run on every status sweep.
    pub(crate) fn cached_program(&self) -> (&Path, Vec<&str>) {
        self.program_with("--no")
    }

    fn program_with(&self, install: &'static str) -> (&Path, Vec<&str>) {
        match self {
            Self::Installed(bin) => (bin, Vec::new()),
            Self::Npx { npx, package } => (
                npx,
                vec![
                    intent_acp::spawn::NPX_NO_WORKSPACES_ARG,
                    install,
                    package.as_str(),
                ],
            ),
        }
    }

    /// Whether the CLI runs without a download: always for an installed
    /// CLI; through npx only when its package is already cached
    /// (`npx --no --package <pkg> -- node --version`, bounded by `timeout`).
    pub(crate) async fn runs_without_download(&self, timeout: Duration) -> bool {
        let Self::Npx { npx, package } = self else {
            return true;
        };
        let mut cmd = tokio::process::Command::new(npx);
        cmd.args([
            intent_acp::spawn::NPX_NO_WORKSPACES_ARG,
            "--no",
            "--package",
            package.as_str(),
            "--",
            "node",
            "--version",
        ])
        .env("PATH", intent_providers::enhanced_path(Some(npx)))
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true);
        matches!(
            tokio::time::timeout(timeout, cmd.status()).await,
            Ok(Ok(status)) if status.success()
        )
    }

    /// `<cli> <args>` with the enhanced PATH, so the CLI (or npx's node)
    /// resolves under a packaged app's minimal PATH.
    pub(crate) fn command(&self, args: &[&str]) -> tokio::process::Command {
        let (program, prefix) = self.program();
        let mut cmd = tokio::process::Command::new(program);
        cmd.args(prefix)
            .args(args)
            .env("PATH", intent_providers::enhanced_path(Some(program)));
        cmd
    }

    /// `installed` for the CLI; longer through npx, whose first run downloads.
    pub(crate) fn timeout(&self, installed: Duration) -> Duration {
        match self {
            Self::Installed(_) => installed,
            Self::Npx { .. } => NPX_TIMEOUT.max(installed),
        }
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    fn stub(dir: &Path, name: &str) -> PathBuf {
        let path = dir.join(name);
        std::fs::write(&path, "#!/bin/sh\nprintf '%s\\n' \"$*\"\n").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        path
    }

    async fn argv(cli: &ProviderCli) -> String {
        let output = cli.command(&["models"]).output().await.unwrap();
        assert!(output.status.success());
        String::from_utf8(output.stdout).unwrap().trim().to_owned()
    }

    #[tokio::test]
    async fn npx_runs_the_pinned_package_outside_npm_workspaces() {
        let dir = tempfile::tempdir().unwrap();
        let cli = ProviderCli::Npx {
            npx: stub(dir.path(), "npx"),
            package: intent_providers::OPENCODE_NPX_PACKAGE.to_owned(),
        };
        assert_eq!(
            argv(&cli).await,
            format!(
                "--workspaces=false -y {} models",
                intent_providers::OPENCODE_NPX_PACKAGE
            )
        );
        // fixed-deadline: allow — an input to the timeout choice, not a wait
        let installed = Duration::from_secs(10);
        assert_eq!(cli.timeout(installed), NPX_TIMEOUT);
    }

    #[test]
    fn sign_in_checks_never_download_through_npx() {
        let cli = ProviderCli::Npx {
            npx: PathBuf::from("/usr/bin/npx"),
            package: "@xai-official/grok@1.0.89".to_owned(),
        };
        let (_, args) = cli.cached_program();
        assert_eq!(
            args,
            ["--workspaces=false", "--no", "@xai-official/grok@1.0.89"]
        );
        let installed = ProviderCli::Installed(PathBuf::from("/usr/bin/grok"));
        assert!(installed.cached_program().1.is_empty());
    }

    #[tokio::test]
    async fn an_installed_cli_runs_directly() {
        let dir = tempfile::tempdir().unwrap();
        let cli = ProviderCli::Installed(stub(dir.path(), "opencode"));
        assert_eq!(argv(&cli).await, "models");
        // fixed-deadline: allow — an input to the timeout choice, not a wait
        let installed = Duration::from_secs(10);
        assert_eq!(cli.timeout(installed), installed);
    }

    #[test]
    fn an_explicit_cli_wins_over_npx() {
        let dir = tempfile::tempdir().unwrap();
        let bin = stub(dir.path(), "grok");
        assert_eq!(
            ProviderCli::resolve("grok", bin.to_str()),
            Some(ProviderCli::Installed(bin))
        );
    }

    #[test]
    fn providers_without_an_npm_package_never_use_npx() {
        for id in ["cursor", "unsloth", "antigravity"] {
            assert_eq!(ProviderCli::npx(id), None, "{id}");
        }
    }
}
