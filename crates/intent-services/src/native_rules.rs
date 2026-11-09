//! Which workspace rule files a provider loads by itself, so a harness can
//! skip injecting them a second time (harness 3.10). Every check fails safe:
//! when anything is unknown the file counts as not loaded and is injected.
//!
//! Verified behavior:
//! - Claude Code always loads `CLAUDE.md` with the project settings source
//!   (agent sessions keep the adapter's `user,project,local` default). From
//!   2.1.281 it also loads `AGENTS.md` when the project has no `CLAUDE.md`,
//!   `.claude/CLAUDE.md` or `CLAUDE.local.md` (2.1.277 added it behind a
//!   feature flag that gateways and telemetry-off sessions never saw).
//! - Codex loads `AGENTS.md` from the project root (nearest `.git`) down to
//!   the cwd within `project_doc_max_bytes` in total (default 32 KiB, a
//!   longer file is cut), prefers `AGENTS.override.md` in the same folder,
//!   and loads nothing for a project marked untrusted. It never reads
//!   `CLAUDE.md`.

use std::path::{Path, PathBuf};

/// Claude Code loads `AGENTS.md` natively from this CLI version on.
const CLAUDE_AGENTS_MD_MIN: (u64, u64, u64) = (2, 1, 281);

/// Codex's default `project_doc_max_bytes`.
pub(crate) const CODEX_DEFAULT_PROJECT_DOC_MAX_BYTES: usize = 32 * 1024;

/// Env switches that stop Claude Code from loading instruction files.
const CLAUDE_INSTRUCTION_OFF_ENV: [&str; 2] =
    ["CLAUDE_CODE_DISABLE_CLAUDE_MDS", "CLAUDE_CODE_SIMPLE"];

/// Claude Code's managed (system) settings file on Linux.
const CLAUDE_MANAGED_SETTINGS: &str = "/etc/claude-code/managed-settings.json";

/// Codex's system config file.
const CODEX_SYSTEM_CONFIG: &str = "/etc/codex/config.toml";

/// What runs the agent, as far as rule loading goes. Built once per spawn.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RuleExecutor {
    /// Base provider id.
    pub provider_id: String,
    /// The provider CLI's `--version` on the machine that runs the agent;
    /// `None` when unknown (a remote node reports none today).
    pub cli_version: Option<String>,
    /// Codex's effective `project_doc_max_bytes` for this workspace; `None`
    /// when it cannot be read or the project does not load docs.
    pub codex_doc_cap: Option<usize>,
}

impl RuleExecutor {
    /// An agent on a remote node. The node reports no CLI version or Codex
    /// configuration, so nothing counts as loaded natively.
    pub(crate) fn remote(provider_id: &str) -> Self {
        Self {
            provider_id: provider_id.to_owned(),
            cli_version: None,
            codex_doc_cap: None,
        }
    }
}

/// The display name of the provider that loads `source` (a workspace rule
/// file, relative to `workspace_path`) by itself, or `None`.
pub(crate) fn native_loader(
    source: &str,
    workspace_path: &Path,
    executor: Option<&RuleExecutor>,
) -> Option<&'static str> {
    let executor = executor?;
    if !is_regular_file(&workspace_path.join(source)) {
        return None;
    }
    match (executor.provider_id.as_str(), source) {
        ("claude-code", "CLAUDE.md") => {
            intent_providers::version_gate::parse_cli_version(executor.cli_version.as_deref()?)?;
            Some("Claude Code")
        }
        ("claude-code", "AGENTS.md") => {
            let version = intent_providers::version_gate::parse_cli_version(
                executor.cli_version.as_deref()?,
            )?;
            (version >= CLAUDE_AGENTS_MD_MIN && !has_claude_instructions_above(workspace_path))
                .then_some("Claude Code")
        }
        ("codex", "AGENTS.md") => {
            let cap = executor.codex_doc_cap?;
            let len = std::fs::metadata(workspace_path.join(source)).ok()?.len();
            let fits = usize::try_from(len).is_ok_and(|len| len <= cap);
            // `.git` here makes this folder the project root, so no parent
            // AGENTS.md shares the budget.
            (fits
                && workspace_path.join(".git").exists()
                && !workspace_path.join("AGENTS.override.md").exists())
            .then_some("Codex")
        }
        _ => None,
    }
}

/// A regular file, not a symlink: a link may point outside the folders the
/// provider is allowed to read.
fn is_regular_file(path: &Path) -> bool {
    std::fs::symlink_metadata(path).is_ok_and(|m| m.file_type().is_file())
}

/// Whether Claude Code would treat the folder as a `CLAUDE.md` project (and
/// so not load `AGENTS.md`): any of its instruction files in the folder or
/// an ancestor.
fn has_claude_instructions_above(workspace_path: &Path) -> bool {
    workspace_path.ancestors().any(|dir| {
        ["CLAUDE.md", "CLAUDE.local.md", ".claude/CLAUDE.md"]
            .iter()
            .any(|name| dir.join(name).exists())
    })
}

/// The rule executor of a local spawn of `provider_id`, or `None` for a
/// provider that loads no workspace rule files by itself.
pub(crate) async fn local_executor(
    provider_id: &str,
    profile: Option<&intent_acp::ProfileEnv>,
    workspace_path: &Path,
) -> Option<RuleExecutor> {
    use intent_providers::installed_cli::InstalledCli;
    let cli = InstalledCli::for_provider(provider_id)?;
    let context = crate::installed_cli::InstalledContext::discover_for(
        cli,
        profile.cloned().map(std::sync::Arc::new),
    )
    .await
    .ok()?;
    let mut executor = RuleExecutor::remote(provider_id);
    match cli {
        InstalledCli::Claude => {
            let mut settings = vec![
                workspace_path.join(".claude").join("settings.json"),
                workspace_path.join(".claude").join("settings.local.json"),
                PathBuf::from(CLAUDE_MANAGED_SETTINGS),
            ];
            settings.extend(context.claude_config_dir().map(|d| d.join("settings.json")));
            let off = claude_native_off(
                |key| context.env_value(key).map(ToOwned::to_owned),
                |key| std::env::var_os(key),
                &settings,
            );
            if !off {
                let path = context.runtime.path().to_owned();
                executor.cli_version = tokio::task::spawn_blocking(move || {
                    crate::provider_version::cached_version(&path)
                })
                .await
                .ok()
                .flatten();
            }
        }
        InstalledCli::Codex => {
            let home = context.codex_home();
            let workspace = workspace_path.to_owned();
            executor.codex_doc_cap =
                tokio::task::spawn_blocking(move || codex_doc_cap(home.as_deref()?, &workspace))
                    .await
                    .ok()
                    .flatten();
        }
    }
    Some(executor)
}

/// Whether Claude Code may skip workspace instruction files: an off switch in
/// the launch or daemon environment, or a `claudeMdExcludes` setting (or an
/// unreadable settings file) in any tier.
fn claude_native_off(
    launch_env: impl Fn(&str) -> Option<std::ffi::OsString>,
    daemon_env: impl Fn(&str) -> Option<std::ffi::OsString>,
    settings: &[PathBuf],
) -> bool {
    let env_off = CLAUDE_INSTRUCTION_OFF_ENV.iter().any(|key| {
        launch_env(key).is_some_and(|v| env_truthy(&v))
            || daemon_env(key).is_some_and(|v| env_truthy(&v))
    });
    env_off
        || settings
            .iter()
            .any(|path| match std::fs::read_to_string(path) {
                Ok(text) => text.contains("claudeMdExcludes"),
                Err(e) => e.kind() != std::io::ErrorKind::NotFound,
            })
}

fn env_truthy(value: &std::ffi::OsStr) -> bool {
    let value = value.to_string_lossy();
    let value = value.trim();
    !value.is_empty()
        && !matches!(
            value.to_ascii_lowercase().as_str(),
            "0" | "false" | "no" | "off"
        )
}

/// Codex's effective doc cap for `workspace` from the user config in
/// `codex_home`, the system config and the project's `.codex/config.toml`: the smallest
/// `project_doc_max_bytes` set anywhere (top level or a profile), else the
/// default. `None` when a config cannot be read, sets custom project root
/// markers, or marks the workspace (or a parent) untrusted.
pub(crate) fn codex_doc_cap(codex_home: &Path, workspace: &Path) -> Option<usize> {
    codex_doc_cap_with(
        &[
            PathBuf::from(CODEX_SYSTEM_CONFIG),
            codex_home.join("config.toml"),
        ],
        workspace,
    )
}

fn codex_doc_cap_with(configs: &[PathBuf], workspace: &Path) -> Option<usize> {
    let trust_paths = codex_trust_paths(workspace)?;
    let mut cap = CODEX_DEFAULT_PROJECT_DOC_MAX_BYTES;
    let project = workspace.join(".codex").join("config.toml");
    for config in configs.iter().chain([&project]) {
        let text = match std::fs::read_to_string(config) {
            Ok(text) => text,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(_) => return None,
        };
        let doc: toml_edit::DocumentMut = text.parse().ok()?;
        if untrusted(&doc, &trust_paths) {
            return None;
        }
        cap = cap.min(smallest_cap(doc.as_item())?);
    }
    Some(cap)
}

/// The smallest `project_doc_max_bytes` under `item` (`usize::MAX` when
/// none); `None` when one is not a whole number or root markers are set.
fn smallest_cap(item: &toml_edit::Item) -> Option<usize> {
    let Some(table) = item.as_table_like() else {
        return Some(usize::MAX);
    };
    let mut cap = usize::MAX;
    for (key, value) in table.iter() {
        match key {
            "project_doc_max_bytes" => {
                cap = cap.min(usize::try_from(value.as_integer()?).ok()?);
            }
            "project_root_markers" => return None,
            "projects" => {}
            _ => cap = cap.min(smallest_cap(value)?),
        }
    }
    Some(cap)
}

/// The paths Codex may key a trust entry on for `workspace`: the path as
/// given, its canonical form and, for a git worktree, the main repository
/// root (Codex resolves a worktree's trust there). `None` when a worktree's
/// main root cannot be resolved.
fn codex_trust_paths(workspace: &Path) -> Option<Vec<PathBuf>> {
    let mut paths = vec![workspace.to_owned()];
    paths.extend(std::fs::canonicalize(workspace).ok());
    let dot_git = workspace.join(".git");
    if std::fs::symlink_metadata(&dot_git).is_ok_and(|m| !m.is_dir()) {
        let text = std::fs::read_to_string(&dot_git).ok()?;
        let gitdir = workspace.join(text.trim().strip_prefix("gitdir:")?.trim());
        let common = std::fs::read_to_string(gitdir.join("commondir")).ok()?;
        let common = std::fs::canonicalize(gitdir.join(common.trim())).ok()?;
        paths.push(common.parent()?.to_owned());
    }
    Some(paths)
}

/// Whether `[projects."<path>"]` marks any of `paths` (or a parent)
/// `trust_level = "untrusted"`.
fn untrusted(doc: &toml_edit::DocumentMut, paths: &[PathBuf]) -> bool {
    let Some(projects) = doc.get("projects").and_then(toml_edit::Item::as_table_like) else {
        return false;
    };
    projects.iter().any(|(key, entry)| {
        let level = entry.get("trust_level").and_then(toml_edit::Item::as_str);
        let key = PathBuf::from(key);
        let canonical = std::fs::canonicalize(&key).unwrap_or_else(|_| key.clone());
        level == Some("untrusted")
            && paths
                .iter()
                .any(|p| p.starts_with(&key) || p.starts_with(&canonical))
    })
}

#[cfg(test)]
pub(crate) mod test_versions {
    // cli-fixture-version: allow — exercises the native AGENTS.md threshold, not the adapter minimum
    pub(crate) const NEW: &str = "2.1.303 (Claude Code)";
    // cli-fixture-version: allow — first Claude Code release that loads AGENTS.md natively
    pub(crate) const FIRST: &str = "2.1.281 (Claude Code)";
    // cli-fixture-version: allow — last Claude Code release without native AGENTS.md
    pub(crate) const BELOW: &str = "2.1.280 (Claude Code)";
    // cli-fixture-version: allow — an old CLI that still loads CLAUDE.md natively
    pub(crate) const OLD: &str = "2.1.0 (Claude Code)";
}

#[cfg(test)]
mod tests {
    use super::test_versions::*;
    use super::*;

    fn claude(version: Option<&str>) -> RuleExecutor {
        RuleExecutor {
            provider_id: "claude-code".into(),
            cli_version: version.map(str::to_owned),
            codex_doc_cap: None,
        }
    }

    fn codex(cap: Option<usize>) -> RuleExecutor {
        RuleExecutor {
            provider_id: "codex".into(),
            cli_version: None,
            codex_doc_cap: cap,
        }
    }

    fn repo(files: &[(&str, usize)]) -> tempfile::TempDir {
        let dir = crate::test_support::test_tempdir("native-rules-");
        std::fs::create_dir(dir.path().join(".git")).unwrap();
        for (name, len) in files {
            let path = dir.path().join(name);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, "x".repeat(*len)).unwrap();
        }
        dir
    }

    #[test]
    fn claude_agents_md_needs_a_known_new_enough_cli() {
        let dir = repo(&[("AGENTS.md", 100)]);
        let at = |v| native_loader("AGENTS.md", dir.path(), Some(&claude(v)));
        assert_eq!(at(Some(NEW)), Some("Claude Code"));
        assert_eq!(at(Some(FIRST)), Some("Claude Code"));
        assert_eq!(at(Some(BELOW)), None);
        assert_eq!(at(Some("garbled")), None);
        assert_eq!(at(None), None);
        assert_eq!(native_loader("AGENTS.md", dir.path(), None), None);
    }

    #[test]
    fn claude_skips_agents_md_in_a_claude_md_project() {
        for other in ["CLAUDE.local.md", ".claude/CLAUDE.md"] {
            let dir = repo(&[("AGENTS.md", 100), (other, 10)]);
            let cli = claude(Some(NEW));
            assert_eq!(
                native_loader("AGENTS.md", dir.path(), Some(&cli)),
                None,
                "{other}"
            );
        }
        let parent = repo(&[("CLAUDE.md", 10)]);
        let nested = parent.path().join("ws");
        std::fs::create_dir(&nested).unwrap();
        std::fs::write(nested.join("AGENTS.md"), "rules").unwrap();
        let cli = claude(Some(NEW));
        assert_eq!(native_loader("AGENTS.md", &nested, Some(&cli)), None);
    }

    #[test]
    fn claude_md_is_native_on_any_known_claude_cli_and_never_on_codex() {
        let dir = repo(&[("CLAUDE.md", 100)]);
        let old = claude(Some(OLD));
        assert_eq!(
            native_loader("CLAUDE.md", dir.path(), Some(&old)),
            Some("Claude Code")
        );
        assert_eq!(
            native_loader("CLAUDE.md", dir.path(), Some(&claude(None))),
            None
        );
        let cx = codex(Some(CODEX_DEFAULT_PROJECT_DOC_MAX_BYTES));
        assert_eq!(native_loader("CLAUDE.md", dir.path(), Some(&cx)), None);
    }

    #[test]
    fn codex_agents_md_must_fit_the_cap() {
        let cap = CODEX_DEFAULT_PROJECT_DOC_MAX_BYTES;
        let under = repo(&[("AGENTS.md", cap)]);
        let over = repo(&[("AGENTS.md", cap + 1)]);
        let cx = codex(Some(cap));
        assert_eq!(
            native_loader("AGENTS.md", under.path(), Some(&cx)),
            Some("Codex")
        );
        assert_eq!(native_loader("AGENTS.md", over.path(), Some(&cx)), None);
        assert_eq!(
            native_loader("AGENTS.md", under.path(), Some(&codex(None))),
            None
        );
    }

    #[test]
    fn codex_needs_the_project_root_and_no_override() {
        let cx = codex(Some(CODEX_DEFAULT_PROJECT_DOC_MAX_BYTES));
        let overridden = repo(&[("AGENTS.md", 10), ("AGENTS.override.md", 10)]);
        assert_eq!(
            native_loader("AGENTS.md", overridden.path(), Some(&cx)),
            None
        );
        let no_root = repo(&[("AGENTS.md", 10)]);
        std::fs::remove_dir(no_root.path().join(".git")).unwrap();
        assert_eq!(native_loader("AGENTS.md", no_root.path(), Some(&cx)), None);
    }

    #[test]
    fn other_files_and_providers_are_never_native() {
        let dir = repo(&[(".intent/guidelines.md", 10), ("AGENTS.md", 10)]);
        let cli = claude(Some(NEW));
        assert_eq!(
            native_loader(".intent/guidelines.md", dir.path(), Some(&cli)),
            None
        );
        let other = RuleExecutor {
            provider_id: "opencode".into(),
            cli_version: Some("1.0.0".into()),
            codex_doc_cap: Some(1 << 20),
        };
        assert_eq!(native_loader("AGENTS.md", dir.path(), Some(&other)), None);
    }

    #[test]
    fn remote_nodes_load_nothing_natively_until_they_report_a_version() {
        let dir = repo(&[("AGENTS.md", 10), ("CLAUDE.md", 10)]);
        for provider in ["claude-code", "codex"] {
            let remote = RuleExecutor::remote(provider);
            for file in ["AGENTS.md", "CLAUDE.md"] {
                assert_eq!(
                    native_loader(file, dir.path(), Some(&remote)),
                    None,
                    "{provider} {file}"
                );
            }
        }
    }

    #[cfg(unix)]
    #[test]
    fn a_symlinked_rule_file_is_injected() {
        let dir = repo(&[("real.md", 10)]);
        std::os::unix::fs::symlink(dir.path().join("real.md"), dir.path().join("AGENTS.md"))
            .unwrap();
        let cli = claude(Some(NEW));
        assert_eq!(native_loader("AGENTS.md", dir.path(), Some(&cli)), None);
    }

    #[test]
    fn codex_cap_reads_user_and_project_config() {
        let home = crate::test_support::test_tempdir("codex-home-");
        let ws = repo(&[]);
        assert_eq!(
            codex_doc_cap_with(&[home.path().join("config.toml")], ws.path()),
            Some(CODEX_DEFAULT_PROJECT_DOC_MAX_BYTES)
        );
        std::fs::write(
            home.path().join("config.toml"),
            "project_doc_max_bytes = 65536\n[profiles.small]\nproject_doc_max_bytes = 4096\n",
        )
        .unwrap();
        assert_eq!(
            codex_doc_cap_with(&[home.path().join("config.toml")], ws.path()),
            Some(4096)
        );
        std::fs::create_dir(ws.path().join(".codex")).unwrap();
        std::fs::write(
            ws.path().join(".codex/config.toml"),
            "project_doc_max_bytes = 100\n",
        )
        .unwrap();
        assert_eq!(
            codex_doc_cap_with(&[home.path().join("config.toml")], ws.path()),
            Some(100)
        );
    }

    #[test]
    fn codex_cap_is_unknown_for_untrusted_custom_markers_or_bad_config() {
        let ws = repo(&[]);
        let cases = [
            format!(
                "[projects.\"{}\"]\ntrust_level = \"untrusted\"\n",
                ws.path().parent().unwrap().display()
            ),
            "project_root_markers = [\".hg\"]\n".to_owned(),
            "project_doc_max_bytes = \"big\"\n".to_owned(),
            "not toml [[[".to_owned(),
        ];
        for config in cases {
            let home = crate::test_support::test_tempdir("codex-home-");
            std::fs::write(home.path().join("config.toml"), &config).unwrap();
            assert_eq!(
                codex_doc_cap_with(&[home.path().join("config.toml")], ws.path()),
                None,
                "{config}"
            );
        }
        let home = crate::test_support::test_tempdir("codex-home-");
        std::fs::write(
            home.path().join("config.toml"),
            format!(
                "[projects.\"{}\"]\ntrust_level = \"trusted\"\n",
                ws.path().display()
            ),
        )
        .unwrap();
        assert_eq!(
            codex_doc_cap_with(&[home.path().join("config.toml")], ws.path()),
            Some(CODEX_DEFAULT_PROJECT_DOC_MAX_BYTES)
        );
    }

    fn worktree_of(main: &Path) -> tempfile::TempDir {
        let gitdir = main.join(".git/worktrees/ws");
        std::fs::create_dir_all(&gitdir).unwrap();
        std::fs::write(gitdir.join("commondir"), "../..\n").unwrap();
        let ws = crate::test_support::test_tempdir("codex-wt-");
        std::fs::write(
            ws.path().join(".git"),
            format!("gitdir: {}\n", gitdir.display()),
        )
        .unwrap();
        ws
    }

    #[test]
    fn codex_worktree_inherits_the_main_repository_trust() {
        let main = repo(&[]);
        let ws = worktree_of(main.path());
        let home = crate::test_support::test_tempdir("codex-home-");
        let config = home.path().join("config.toml");
        std::fs::write(
            &config,
            format!(
                "[projects.\"{}\"]\ntrust_level = \"untrusted\"\n",
                main.path().display()
            ),
        )
        .unwrap();
        assert_eq!(
            codex_doc_cap_with(std::slice::from_ref(&config), ws.path()),
            None
        );
        std::fs::write(&config, "").unwrap();
        assert_eq!(
            codex_doc_cap_with(&[config], ws.path()),
            Some(CODEX_DEFAULT_PROJECT_DOC_MAX_BYTES)
        );
    }

    #[cfg(unix)]
    #[test]
    fn codex_trust_matches_a_symlinked_workspace_and_fails_safe_on_a_broken_worktree() {
        let main = repo(&[]);
        let link_parent = crate::test_support::test_tempdir("codex-link-");
        let link = link_parent.path().join("ws");
        std::os::unix::fs::symlink(main.path(), &link).unwrap();
        let home = crate::test_support::test_tempdir("codex-home-");
        let config = home.path().join("config.toml");
        std::fs::write(
            &config,
            format!(
                "[projects.\"{}\"]\ntrust_level = \"untrusted\"\n",
                std::fs::canonicalize(main.path()).unwrap().display()
            ),
        )
        .unwrap();
        assert_eq!(
            codex_doc_cap_with(std::slice::from_ref(&config), &link),
            None
        );

        let broken = crate::test_support::test_tempdir("codex-wt-");
        std::fs::write(
            broken.path().join(".git"),
            "gitdir: /nonexistent/worktrees/x\n",
        )
        .unwrap();
        std::fs::write(&config, "").unwrap();
        assert_eq!(codex_doc_cap_with(&[config], broken.path()), None);
    }

    #[test]
    fn claude_native_is_off_for_env_switches_excludes_and_unreadable_settings() {
        let none = |_: &str| None;
        let on = |key: &str| (key == "CLAUDE_CODE_SIMPLE").then(|| "1".into());
        let falsy = |_: &str| Some("0".into());
        assert!(!claude_native_off(none, none, &[]));
        assert!(claude_native_off(on, none, &[]));
        assert!(claude_native_off(none, on, &[]));
        assert!(!claude_native_off(falsy, falsy, &[]));

        let dir = crate::test_support::test_tempdir("claude-settings-");
        let missing = dir.path().join("missing.json");
        let plain = dir.path().join("plain.json");
        let excludes = dir.path().join("excludes.json");
        std::fs::write(&plain, r#"{"model":"opus"}"#).unwrap();
        std::fs::write(&excludes, r#"{"claudeMdExcludes":["**/AGENTS.md"]}"#).unwrap();
        assert!(!claude_native_off(none, none, &[missing, plain.clone()]));
        assert!(claude_native_off(none, none, &[plain, excludes]));
        assert!(claude_native_off(none, none, &[dir.path().to_owned()]));
    }
}
