//! Spawning piped-stdio provider processes (§6.2).
//!
//! Resolves args/env from the `intent_providers` registry, enriches `PATH` so a
//! `#!/usr/bin/env node` shebang resolves the right `node`, applies Codex
//! `-c model=…` overrides, and spawns with all three pipes captured and
//! `kill_on_drop(true)`. The captured pipes are handed to a [`Connection`].

use std::collections::BTreeMap;
use std::path::Path;
use std::process::Stdio;

use intent_providers::{
    apply_codex_config_args, build_provider_args, build_provider_env, enhanced_path, ArgInputs,
    ProviderConfig,
};
use tokio::io::AsyncRead;
use tokio::process::{Child, Command};

use crate::error::{AcpError, AcpResult};
use crate::transport::{Connection, ConnectionHooks};

/// Inputs for spawning a provider process.
pub struct SpawnOptions<'a> {
    /// The resolved provider config (registry entry, §6.9).
    pub provider: &'a ProviderConfig,
    /// Raw (provider-stripped) model id, or `None`.
    pub model: Option<&'a str>,
    /// Working directory for the child.
    pub cwd: Option<&'a Path>,
    /// Path to a rules file (appended when the provider supports rules).
    pub rules_file: Option<&'a str>,
    /// Path to an MCP config file (appended when the provider supports MCP).
    pub mcp_config_file: Option<&'a str>,
    /// Whether to append the provider's quiet flag.
    pub quiet: bool,
    /// Discovered provider binary, used for `PATH` enrichment.
    pub provider_binary: Option<&'a Path>,
    /// Extra environment overrides applied last.
    pub extra_env: BTreeMap<String, String>,
    /// Provider-native tools to strip via the provider's `--remove-tool`
    /// equivalent (§18.4 CLI-side enforcement). Gated on
    /// [`ProviderConfig::remove_tool_flag`] — providers that don't advertise a
    /// flag silently ignore the input rather than receive an unknown arg.
    pub tools_to_remove: Vec<&'static str>,
    /// When provider_binary is None and the provider spawns via npx (either a
    /// `fallback_npx_package` or an npx-only provider's pinned
    /// `npx_only_package`), this is the resolved npx path.
    pub npx_fallback_binary: Option<&'a Path>,
    /// The package spec to pass to npx when npx_fallback_binary is set (may
    /// carry a pinned `@<version>` suffix).
    pub npx_fallback_package: Option<&'static str>,
}

impl<'a> SpawnOptions<'a> {
    /// Construct options for a provider with all optional inputs unset.
    pub fn new(provider: &'a ProviderConfig) -> Self {
        Self {
            provider,
            model: None,
            cwd: None,
            rules_file: None,
            mcp_config_file: None,
            quiet: false,
            provider_binary: None,
            extra_env: BTreeMap::new(),
            tools_to_remove: Vec::new(),
            npx_fallback_binary: None,
            npx_fallback_package: None,
        }
    }
}

/// Assemble the launch arguments, including the Codex `-c` config overrides
/// (which read `CODEX_REASONING_EFFORT` / `CODEX_MODEL_REASONING_EFFORT`).
/// When spawning via npx fallback, prepends `-y <package>` before the provider's args.
pub fn build_args(opts: &SpawnOptions) -> Vec<String> {
    let mut args = Vec::new();

    // When using npx fallback (provider_binary not set AND both npx fields are set),
    // prepend the npx-specific args before the provider's args
    if opts.provider_binary.is_none() {
        if let (Some(_), Some(pkg)) = (opts.npx_fallback_binary, opts.npx_fallback_package) {
            args.push("-y".to_string());
            args.push(pkg.to_string());
        }
    }

    // Then append the provider's normal ACP args
    let mut provider_args = build_provider_args(
        opts.provider,
        &ArgInputs {
            model: opts.model,
            rules_file: opts.rules_file,
            mcp_config_file: opts.mcp_config_file,
            quiet: opts.quiet,
            tools_to_remove: &opts.tools_to_remove,
        },
    );
    if opts.provider.id == "codex" {
        let env_effort = std::env::var("CODEX_REASONING_EFFORT")
            .ok()
            .or_else(|| std::env::var("CODEX_MODEL_REASONING_EFFORT").ok());
        provider_args = apply_codex_config_args(provider_args, opts.model, env_effort.as_deref());
    }
    args.extend(provider_args);
    args
}

/// Build the `tokio` command (args + env + enriched `PATH` + piped stdio +
/// `kill_on_drop`) without spawning it. Exposed for testing/inspection.
///
/// When `opts.provider_binary` is set (resolved to an absolute path), spawns
/// that path directly; otherwise, when `opts.npx_fallback_binary` is set,
/// spawns npx; otherwise falls back to the bare `opts.provider.command`
/// and relies on the enriched `PATH`.
pub fn build_command(opts: &SpawnOptions) -> Command {
    let args = build_args(opts);

    // Decide which binary to spawn: provider_binary > npx_fallback (both fields) > provider.command
    let command = if let Some(p) = opts.provider_binary {
        p.as_os_str()
    } else if let (Some(npx), Some(_)) = (opts.npx_fallback_binary, opts.npx_fallback_package) {
        npx.as_os_str()
    } else {
        std::ffi::OsStr::new(opts.provider.command)
    };

    let mut cmd = Command::new(command);
    cmd.args(&args);
    if let Some(cwd) = opts.cwd {
        cmd.current_dir(cwd);
    }
    for (key, value) in build_provider_env(opts.provider, opts.model, opts.rules_file) {
        cmd.env(key, value);
    }
    for (key, value) in &opts.extra_env {
        cmd.env(key, value);
    }

    // Enhanced PATH must include the binary's parent dir so dependencies resolve
    // (e.g., when spawning npx, node must be findable)
    let path_binary = opts.provider_binary.or_else(|| {
        if opts.npx_fallback_package.is_some() {
            opts.npx_fallback_binary
        } else {
            None
        }
    });
    cmd.env("PATH", enhanced_path(path_binary));
    cmd.stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    // Put the provider in its own process group (leader pgid == child pid) so
    // reaping/stop can signal the WHOLE tree via `killpg(-pgid)` — `kill_on_drop`
    // only reaches the direct child, leaving grandchildren orphaned (§5.6).
    #[cfg(unix)]
    cmd.process_group(0);
    cmd
}

/// A spawned provider child paired with its live ACP [`Connection`].
pub struct SpawnedAgent {
    child: Child,
    connection: Connection,
}

impl SpawnedAgent {
    /// The live JSON-RPC connection to this agent.
    pub fn connection(&self) -> &Connection {
        &self.connection
    }

    /// Mutable access to the underlying child process.
    pub fn child_mut(&mut self) -> &mut Child {
        &mut self.child
    }

    /// Kill the child process.
    pub async fn kill(&mut self) -> std::io::Result<()> {
        self.child.kill().await
    }

    /// Decompose into the child and connection (e.g. to store separately).
    pub fn into_parts(self) -> (Child, Connection) {
        (self.child, self.connection)
    }
}

/// Spawn the provider and wire up its [`Connection`] (§6.2 + §6.3).
pub fn spawn_provider(opts: &SpawnOptions, hooks: ConnectionHooks) -> AcpResult<SpawnedAgent> {
    let mut cmd = build_command(opts);
    let command_name = opts
        .provider_binary
        .map(|p| p.display().to_string())
        .unwrap_or_else(|| opts.provider.command.to_string());
    let mut child = cmd
        .spawn()
        .map_err(|e| AcpError::Spawn(format!("{command_name}: {e}")))?;
    let stdin = child
        .stdin
        .take()
        .ok_or_else(|| AcpError::Spawn("child stdin not piped".to_string()))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| AcpError::Spawn("child stdout not piped".to_string()))?;
    let stderr = child
        .stderr
        .take()
        .map(|s| Box::new(s) as Box<dyn AsyncRead + Unpin + Send>);
    let connection = Connection::new(stdin, stdout, stderr, hooks);
    Ok(SpawnedAgent { child, connection })
}

#[cfg(test)]
mod build_args_tests {
    use super::*;

    #[test]
    fn build_args_propagates_tools_to_remove_for_auggie() {
        let auggie = intent_providers::find_provider("auggie").unwrap();
        let mut opts = SpawnOptions::new(auggie);
        opts.tools_to_remove = vec!["str-replace-editor", "sub-agent-explore"];
        let args = build_args(&opts);
        assert!(args
            .windows(2)
            .any(|w| w == ["--remove-tool", "str-replace-editor"]));
        assert!(args
            .windows(2)
            .any(|w| w == ["--remove-tool", "sub-agent-explore"]));
    }

    #[test]
    fn build_args_omits_remove_tool_flags_for_non_supporting_providers() {
        // claude-code / codex etc. don't advertise --remove-tool support; the
        // spawn layer must not leak an unknown flag to them.
        for id in [
            "claude-code",
            "codex",
            "cortex",
            "opencode",
            "droid",
            "mock",
        ] {
            let provider = intent_providers::find_provider(id).unwrap();
            let mut opts = SpawnOptions::new(provider);
            opts.tools_to_remove = vec!["str-replace-editor"];
            let args = build_args(&opts);
            assert!(
                !args.iter().any(|a| a == "--remove-tool"),
                "{id} spawn args unexpectedly include --remove-tool: {args:?}"
            );
        }
    }
}

#[cfg(test)]
mod build_command_tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn build_command_uses_bare_command_when_provider_binary_is_none() {
        let provider = intent_providers::find_provider("auggie").unwrap();
        let opts = SpawnOptions::new(provider);
        let cmd = build_command(&opts);
        let program = cmd.as_std().get_program();
        assert_eq!(program, "auggie");
    }

    #[test]
    fn build_command_uses_absolute_path_when_provider_binary_is_set() {
        let provider = intent_providers::find_provider("auggie").unwrap();
        let mut opts = SpawnOptions::new(provider);
        let resolved_path = PathBuf::from("/usr/local/bin/auggie");
        opts.provider_binary = Some(&resolved_path);
        let cmd = build_command(&opts);
        let program = cmd.as_std().get_program();
        assert_eq!(program, resolved_path.as_os_str());
    }

    #[test]
    fn build_command_enriches_path_with_provider_binary_parent() {
        let provider = intent_providers::find_provider("auggie").unwrap();
        let mut opts = SpawnOptions::new(provider);
        let resolved_path = PathBuf::from("/custom/dir/auggie");
        opts.provider_binary = Some(&resolved_path);
        let cmd = build_command(&opts);
        let env_path = cmd.as_std().get_envs().find(|(k, _)| *k == "PATH");
        assert!(env_path.is_some());
        let path_value = env_path.unwrap().1.unwrap().to_string_lossy();
        // The parent dir should be first in the PATH
        let parent_dir = resolved_path.parent().unwrap().display().to_string();
        let sep = if cfg!(windows) { ";" } else { ":" };
        let expected_prefix = format!("{}{}", parent_dir, sep);
        assert!(
            path_value.starts_with(&expected_prefix),
            "PATH should start with {}, got: {}",
            expected_prefix,
            path_value
        );
    }

    #[test]
    fn build_command_uses_npx_fallback_when_set() {
        let provider = intent_providers::find_provider("claude-code").unwrap();
        let mut opts = SpawnOptions::new(provider);
        let npx_path = PathBuf::from("/usr/local/bin/npx");
        opts.npx_fallback_binary = Some(&npx_path);
        opts.npx_fallback_package = Some(intent_providers::CLAUDE_AGENT_ACP_NPX_PACKAGE);
        let cmd = build_command(&opts);
        let program = cmd.as_std().get_program();
        assert_eq!(program, npx_path.as_os_str());
    }

    #[test]
    fn claude_code_npx_spawn_argv_is_pinned() {
        // The exact spawn argv for claude-code: `<npx> -y <pinned package>` —
        // no other args (claude-code has no base args).
        let provider = intent_providers::find_provider("claude-code").unwrap();
        let mut opts = SpawnOptions::new(provider);
        let npx_path = PathBuf::from("/usr/local/bin/npx");
        opts.npx_fallback_binary = Some(&npx_path);
        opts.npx_fallback_package = provider.npx_only_package;
        let cmd = build_command(&opts);
        assert_eq!(cmd.as_std().get_program(), npx_path.as_os_str());
        let args = build_args(&opts);
        assert_eq!(
            args,
            vec![
                "-y".to_string(),
                format!(
                    "@agentclientprotocol/claude-agent-acp@{}",
                    intent_providers::CLAUDE_AGENT_ACP_VERSION
                ),
            ]
        );
    }

    #[test]
    fn build_command_prefers_provider_binary_over_npx_fallback() {
        // Uses codex (a fallback-npx provider) — claude-code is npx-only and
        // never resolves a provider binary.
        let provider = intent_providers::find_provider("codex").unwrap();
        let mut opts = SpawnOptions::new(provider);
        let provider_binary = PathBuf::from("/custom/codex-acp");
        let npx_path = PathBuf::from("/usr/local/bin/npx");
        opts.provider_binary = Some(&provider_binary);
        opts.npx_fallback_binary = Some(&npx_path);
        opts.npx_fallback_package = provider.fallback_npx_package;
        let cmd = build_command(&opts);
        let program = cmd.as_std().get_program();
        // Should use provider_binary, not npx
        assert_eq!(program, provider_binary.as_os_str());
        // And args should NOT include npx -y package
        let args = build_args(&opts);
        assert!(!args.contains(&"-y".to_string()));
    }

    #[test]
    fn build_command_enriches_path_with_npx_parent_when_using_fallback() {
        let provider = intent_providers::find_provider("claude-code").unwrap();
        let mut opts = SpawnOptions::new(provider);
        let npx_path = PathBuf::from("/custom/node/bin/npx");
        opts.npx_fallback_binary = Some(&npx_path);
        opts.npx_fallback_package = Some(intent_providers::CLAUDE_AGENT_ACP_NPX_PACKAGE);
        let cmd = build_command(&opts);
        let env_path = cmd.as_std().get_envs().find(|(k, _)| *k == "PATH");
        assert!(env_path.is_some());
        let path_value = env_path.unwrap().1.unwrap().to_string_lossy();
        let parent_dir = npx_path.parent().unwrap().display().to_string();
        let sep = if cfg!(windows) { ";" } else { ":" };
        let expected_prefix = format!("{}{}", parent_dir, sep);
        assert!(
            path_value.starts_with(&expected_prefix),
            "PATH should start with {} so npx can find node, got: {}",
            expected_prefix,
            path_value
        );
    }

    #[test]
    fn providers_without_fallback_package_do_not_use_npx() {
        // Test that providers like auggie don't get npx fallback
        let provider = intent_providers::find_provider("auggie").unwrap();
        assert_eq!(
            provider.fallback_npx_package, None,
            "auggie should not have fallback_npx_package"
        );
        assert_eq!(
            provider.npx_only_package, None,
            "auggie should not have npx_only_package"
        );
    }

    #[test]
    fn claude_code_is_npx_only_with_pinned_package() {
        let provider = intent_providers::find_provider("claude-code").unwrap();
        assert_eq!(
            provider.npx_only_package,
            Some(intent_providers::CLAUDE_AGENT_ACP_NPX_PACKAGE),
            "claude-code must spawn exclusively via the pinned npx package"
        );
        assert_eq!(
            provider.fallback_npx_package, None,
            "claude-code must not have a fallback (npx is the only path)"
        );
    }
}
