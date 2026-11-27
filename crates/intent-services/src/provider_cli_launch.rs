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

    /// [`Self::npx`], else Intent's bundled Codex / Claude Code CLI package
    /// through npx, with the command the package provides.
    pub(crate) fn npx_with_bundled(provider_id: &str) -> Option<(Self, &'static str)> {
        let provider = intent_providers::find_provider(provider_id)?;
        if let Some(cli) = Self::npx(provider_id) {
            return Some((cli, provider.command));
        }
        let bundled = intent_providers::adapter_cli::missing_fallback(provider_id)?;
        let npx = if provider_id == "codex" {
            intent_providers::find_codex_npx()
        } else {
            intent_providers::find_npx()
        }?;
        let command =
            intent_providers::installed_cli::InstalledCli::for_provider(provider_id)?.command();
        Some((
            Self::Npx {
                npx,
                package: bundled.spec(),
            },
            command,
        ))
    }

    /// The executable and the arguments that come before the CLI's own.
    pub(crate) fn program(&self) -> (&Path, Vec<String>) {
        match self {
            Self::Installed(bin) => (bin, Vec::new()),
            Self::Npx { npx, package } => {
                let mut args = vec![intent_acp::spawn::NPX_NO_WORKSPACES_ARG.to_owned()];
                args.extend(intent_providers::npx_install_args(package));
                args.extend(["-y".to_owned(), package.clone()]);
                (npx, args)
            }
        }
    }

    /// [`Self::program`] that never downloads: npx runs the package only
    /// from its cache and fails otherwise (`--no`). For sign-in checks, which
    /// run on every status sweep.
    pub(crate) fn cached_program(&self) -> (&Path, Vec<&str>) {
        match self {
            Self::Installed(bin) => (bin, Vec::new()),
            Self::Npx { npx, package } => (
                npx,
                vec![intent_acp::spawn::NPX_NO_WORKSPACES_ARG, "--no", package],
            ),
        }
    }

    /// `npx --no --package <pkg> -- <tail>`: runs `tail` with the cached
    /// package's bins on PATH, failing when the package is not cached.
    fn cached_npx(npx: &Path, package: &str, tail: &[&std::ffi::OsStr]) -> tokio::process::Command {
        let mut cmd = tokio::process::Command::new(npx);
        cmd.args([
            intent_acp::spawn::NPX_NO_WORKSPACES_ARG,
            "--no",
            "--package",
            package,
            "--",
        ])
        .args(tail)
        .env("PATH", intent_providers::enhanced_path(Some(npx)))
        .current_dir(neutral_cwd())
        .stdin(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true);
        cmd
    }

    /// Whether the CLI runs without a download: always for an installed
    /// CLI; through npx only when its package is already cached
    /// (`npx --no --package <pkg> -- node --version`, bounded by `timeout`).
    pub(crate) async fn runs_without_download(&self, timeout: Duration) -> bool {
        let Self::Npx { npx, package } = self else {
            return true;
        };
        let mut cmd = Self::cached_npx(npx, package, &["node".as_ref(), "--version".as_ref()]);
        cmd.stdout(std::process::Stdio::null());
        matches!(
            tokio::time::timeout(timeout, cmd.status()).await,
            Ok(Ok(status)) if status.success()
        )
    }

    /// Remove npx's cached install of the package when npm left it
    /// unfinished, so the next run installs it again: the package has an
    /// install script (which installs the native binary) and its `command`
    /// fails `--version` with the stub's own "install script did not run"
    /// message. npm 12's default, a user npm config that skips install
    /// scripts or optional packages, or a first run stopped mid-install
    /// leaves such a stub, and npx otherwise reuses it forever. An install changed within
    /// `grace` may still be running and is kept. Returns whether it removed
    /// one; never for an installed CLI or an uncached package.
    pub(crate) async fn remove_unfinished_install(&self, command: &str, grace: Duration) -> bool {
        let Self::Npx { npx, package } = self else {
            return false;
        };
        let Some((name, version)) = package.rsplit_once('@').filter(|(n, _)| !n.is_empty()) else {
            return false;
        };
        let finished = finished_installs();
        if finished.lock().is_ok_and(|set| set.contains(package)) {
            return false;
        }
        let Ok(script) = tempfile::Builder::new()
            .prefix("intent-npx-check-")
            .suffix(".cjs")
            .tempfile()
        else {
            return false;
        };
        if std::fs::write(script.path(), UNFINISHED_INSTALL_SCRIPT).is_err() {
            return false;
        }
        let mut cmd = Self::cached_npx(
            npx,
            package,
            &[
                "node".as_ref(),
                script.path().as_os_str(),
                name.as_ref(),
                version.as_ref(),
                command.as_ref(),
            ],
        );
        cmd.stdout(std::process::Stdio::piped());
        let output = match tokio::time::timeout(UNFINISHED_CHECK_TIMEOUT, cmd.output()).await {
            Ok(Ok(output)) if output.status.success() => output,
            _ => return false,
        };
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stdout = stdout.trim();
        if stdout == "ok" {
            if let Ok(mut set) = finished.lock() {
                set.insert(package.clone());
            }
            return false;
        }
        let root = PathBuf::from(stdout);
        tokio::task::spawn_blocking(move || remove_npx_install(&root, grace))
            .await
            .unwrap_or(false)
    }

    /// `<cli> <args>` with the enhanced PATH, so the CLI (or npx's node)
    /// resolves under a packaged app's minimal PATH, run from
    /// [`neutral_cwd`] rather than the daemon's own folder.
    pub(crate) fn command(&self, args: &[&str]) -> tokio::process::Command {
        let (program, prefix) = self.program();
        let mut cmd = tokio::process::Command::new(program);
        cmd.args(prefix)
            .args(args)
            .env("PATH", intent_providers::enhanced_path(Some(program)))
            .current_dir(neutral_cwd());
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

/// How long an install found unfinished may be untouched before it is
/// removed: a first `npx -y` may still be downloading or running its install
/// script. Matches the longest a one-shot npx command may run.
pub(crate) const UNFINISHED_INSTALL_GRACE: Duration = NPX_TIMEOUT;

const UNFINISHED_CHECK_TIMEOUT: Duration = Duration::from_secs(60);

/// Packages whose cached install was found finished this run: a finished
/// install stays finished, so they are not checked again.
fn finished_installs() -> &'static std::sync::Mutex<std::collections::HashSet<String>> {
    static SET: std::sync::OnceLock<std::sync::Mutex<std::collections::HashSet<String>>> =
        std::sync::OnceLock::new();
    SET.get_or_init(Default::default)
}

/// Run by node through `npx --no --package <pkg>` with `<name> <version>
/// <command>`: prints the npx install root (`.../_npx/<hash>`) only when the
/// package has an install script and `<command> --version` exits non-zero
/// with a stub's "install script did not run" message (`opencode-ai`,
/// `@anthropic-ai/claude-code`); `ok` when it ran or failed otherwise, which
/// a reinstall cannot fix; and nothing when inconclusive (no such bin, spawn
/// error such as a noexec cache, timeout or signal).
const UNFINISHED_INSTALL_SCRIPT: &str = r#"
const fs = require('node:fs'), path = require('node:path'), cp = require('node:child_process');
const [name, version, command] = process.argv.slice(2);
for (const bin of (process.env.PATH || '').split(path.delimiter)) {
  if (path.basename(bin) !== '.bin') continue;
  const modules = path.dirname(bin);
  let pkg;
  try { pkg = JSON.parse(fs.readFileSync(path.join(modules, name, 'package.json'), 'utf8')); } catch { continue; }
  if (pkg.name !== name || pkg.version !== version) continue;
  const scripts = pkg.scripts || {};
  if (!scripts.install && !scripts.postinstall) { process.stdout.write('ok'); break; }
  const windows = process.platform === 'win32';
  const shim = path.join(bin, windows ? command + '.cmd' : command);
  if (!fs.existsSync(shim)) break;
  // cmd.exe runs an unquoted path with a space as two words and exits 1.
  const run = cp.spawnSync(windows ? '"' + shim + '"' : shim, ['--version'], {
    stdio: ['ignore', 'pipe', 'pipe'], encoding: 'utf8', timeout: 30000, shell: windows,
  });
  if (run.error || typeof run.status !== 'number') break;
  const stub = /postinstall script was not run|native binary not installed/i;
  if (run.status !== 0 && stub.test((run.stdout || '') + (run.stderr || ''))) {
    process.stdout.write(path.dirname(modules));
  } else process.stdout.write('ok');
  break;
}
"#;

/// Delete `root` when it is an npx install (`<cache>/_npx/<hex>`) untouched
/// for `grace`.
fn remove_npx_install(root: &Path, grace: Duration) -> bool {
    let is_npx_install = root.is_absolute()
        && root.parent().and_then(Path::file_name) == Some("_npx".as_ref())
        && root
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|n| !n.is_empty() && n.chars().all(|c| c.is_ascii_hexdigit()));
    if !is_npx_install {
        return false;
    }
    let newest = [root.to_owned(), root.join("node_modules")]
        .iter()
        .filter_map(|p| std::fs::metadata(p).and_then(|m| m.modified()).ok())
        .max();
    let settled = newest
        .and_then(|t| t.elapsed().ok())
        .is_some_and(|age| age >= grace);
    if !settled {
        return false;
    }
    match std::fs::remove_dir_all(root) {
        Ok(()) => {
            tracing::warn!(root = %root.display(), "removed an npx install whose CLI was left unfinished; the next run reinstalls it");
            true
        }
        Err(error) => {
            tracing::warn!(root = %root.display(), %error, "cannot remove an unfinished npx install");
            false
        }
    }
}

/// A readable working folder for one-shot provider CLI commands: the user's
/// home, else the temp folder. The daemon's own folder can be unreadable
/// (`/root` under a system service), and `opencode` reads project config from
/// its working folder, failing with `PermissionDenied` there.
pub(crate) fn neutral_cwd() -> PathBuf {
    std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(PathBuf::from)
        .filter(|home| home.is_absolute() && std::fs::read_dir(home).is_ok())
        .unwrap_or_else(std::env::temp_dir)
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    /// The daemon can run from a folder its user cannot read (`/root` under a
    /// system service). `opencode` reads project config from its working
    /// folder and exits 1 there, which read as "signed out" (no Enable in
    /// Settings). Every CLI command runs from a readable folder instead.
    #[test]
    fn commands_run_from_a_readable_folder_not_the_daemons() {
        for cli in [
            ProviderCli::Installed(PathBuf::from("/usr/bin/opencode")),
            ProviderCli::Npx {
                npx: PathBuf::from("/usr/bin/npx"),
                package: intent_providers::OPENCODE_NPX_PACKAGE.to_owned(),
            },
        ] {
            let cmd = cli.command(&["models"]);
            let dir = cmd
                .as_std()
                .get_current_dir()
                .expect("a working folder is set");
            assert!(std::fs::read_dir(dir).is_ok(), "{dir:?} must be readable");
        }
        assert!(std::fs::read_dir(neutral_cwd()).is_ok());
    }

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
        // Scripts and optional dependencies stay on whatever the user's npm
        // config (or npm 12's default) says: they install the native binary.
        assert_eq!(
            argv(&cli).await,
            format!(
                "--workspaces=false --ignore-scripts=false --include=optional --allow-scripts={0} -y {0} models",
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

    /// A cached npx install of `<name>@1.0.0` under `<dir>/_npx/0a1b2c3d`
    /// whose `fake` bin exits with `status`, and a stub npx that runs the
    /// command after `--` with that install's `.bin` on PATH. Each case
    /// needs its own `name`: finished installs are remembered per package.
    fn npx_cache(
        dir: &Path,
        name: &str,
        install_script: bool,
        status: i32,
    ) -> (ProviderCli, PathBuf) {
        let root = dir.join("_npx").join("0a1b2c3d");
        let package = root.join("node_modules").join(name);
        let bin = root.join("node_modules/.bin");
        std::fs::create_dir_all(&package).unwrap();
        std::fs::create_dir_all(&bin).unwrap();
        let scripts = if install_script {
            r#","scripts":{"postinstall":"node postinstall.mjs"}"#
        } else {
            ""
        };
        std::fs::write(
            package.join("package.json"),
            format!(
                r#"{{"name":"{name}","version":"1.0.0","bin":{{"fake":"bin/fake"}}{scripts}}}"#
            ),
        )
        .unwrap();
        let fake = bin.join("fake");
        // 1: an unfinished install's stub; any other failure is the CLI's own.
        let message = if status == 1 {
            "Error: claude native binary not installed."
        } else {
            "Error: cannot read config"
        };
        std::fs::write(
            &fake,
            format!("#!/bin/sh\necho '{message}' >&2\nexit {status}\n"),
        )
        .unwrap();
        std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();
        let npx = dir.join("npx");
        std::fs::write(
            &npx,
            format!(
                "#!/bin/sh\nwhile [ \"$1\" != -- ]; do shift; done\nshift\nPATH=\"{}:$PATH\" exec \"$@\"\n",
                bin.display()
            ),
        )
        .unwrap();
        std::fs::set_permissions(&npx, std::fs::Permissions::from_mode(0o755)).unwrap();
        let cli = ProviderCli::Npx {
            npx,
            package: format!("{name}@1.0.0"),
        };
        (cli, root)
    }

    /// Sam's alpha report: npm skipped `opencode-ai`'s postinstall (npm 12's
    /// default, an `ignore-scripts` config or an interrupted first run), so
    /// npx kept a stub `opencode` that only prints an error, and every later
    /// run reused it. The check removes such an install so the next run
    /// reinstalls it; a working CLI, a CLI failing for its own reasons, a
    /// package with nothing to finish, and an install that may still be
    /// running stay.
    #[tokio::test]
    async fn a_cached_cli_left_unfinished_by_npm_is_removed() {
        if intent_providers::resolve_on_path("node").is_none() {
            eprintln!("skipping: node not on PATH");
            return;
        }
        for (name, install_script, status, grace, removed) in [
            ("unfinished-cli", true, 1, Duration::ZERO, true),
            ("finished-cli", true, 0, Duration::ZERO, false),
            ("failing-cli", true, 2, Duration::ZERO, false),
            ("scriptless-cli", false, 1, Duration::ZERO, false),
            ("installing-cli", true, 1, Duration::from_secs(600), false),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let (cli, root) = npx_cache(dir.path(), name, install_script, status);
            let case = format!("script={install_script} status={status} grace={grace:?}");
            assert_eq!(
                cli.remove_unfinished_install("fake", grace).await,
                removed,
                "{case}"
            );
            assert_eq!(root.exists(), !removed, "{case}");
        }
        // A missing bin is inconclusive, never an unfinished install.
        let dir = tempfile::tempdir().unwrap();
        let (cli, root) = npx_cache(dir.path(), "binless-cli", true, 1);
        std::fs::remove_file(root.join("node_modules/.bin/fake")).unwrap();
        assert!(!cli.remove_unfinished_install("fake", Duration::ZERO).await);
        assert!(root.exists());
        // A finished install is not checked again this run.
        let dir = tempfile::tempdir().unwrap();
        let (cli, root) = npx_cache(dir.path(), "finished-cli", true, 1);
        assert!(!cli.remove_unfinished_install("fake", Duration::ZERO).await);
        assert!(root.exists());
    }

    #[tokio::test]
    async fn an_installed_cli_or_an_uncached_package_is_never_removed() {
        let dir = tempfile::tempdir().unwrap();
        let installed = ProviderCli::Installed(stub(dir.path(), "opencode"));
        assert!(
            !installed
                .remove_unfinished_install("opencode", Duration::ZERO)
                .await
        );
        let npx = dir.path().join("npx-uncached");
        std::fs::write(&npx, "#!/bin/sh\nexit 1\n").unwrap();
        std::fs::set_permissions(&npx, std::fs::Permissions::from_mode(0o755)).unwrap();
        let uncached = ProviderCli::Npx {
            npx,
            package: "fake-cli@1.0.0".to_owned(),
        };
        assert!(
            !uncached
                .remove_unfinished_install("fake", Duration::ZERO)
                .await
        );
    }

    #[test]
    fn providers_without_an_npm_package_never_use_npx() {
        for id in ["cursor", "unsloth", "antigravity"] {
            assert_eq!(ProviderCli::npx(id), None, "{id}");
        }
    }
}
