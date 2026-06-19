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
        }
    }
}

/// Assemble the launch arguments, including the Codex `-c` config overrides
/// (which read `CODEX_REASONING_EFFORT` / `CODEX_MODEL_REASONING_EFFORT`).
pub fn build_args(opts: &SpawnOptions) -> Vec<String> {
    let mut args = build_provider_args(
        opts.provider,
        &ArgInputs {
            model: opts.model,
            rules_file: opts.rules_file,
            mcp_config_file: opts.mcp_config_file,
            quiet: opts.quiet,
        },
    );
    if opts.provider.id == "codex" {
        let env_effort = std::env::var("CODEX_REASONING_EFFORT")
            .ok()
            .or_else(|| std::env::var("CODEX_MODEL_REASONING_EFFORT").ok());
        args = apply_codex_config_args(args, opts.model, env_effort.as_deref());
    }
    args
}

/// Build the `tokio` command (args + env + enriched `PATH` + piped stdio +
/// `kill_on_drop`) without spawning it. Exposed for testing/inspection.
pub fn build_command(opts: &SpawnOptions) -> Command {
    let args = build_args(opts);
    let mut cmd = Command::new(opts.provider.command);
    cmd.args(&args);
    if let Some(cwd) = opts.cwd {
        cmd.current_dir(cwd);
    }
    for (key, value) in build_provider_env(opts.provider.id, opts.model) {
        cmd.env(key, value);
    }
    for (key, value) in &opts.extra_env {
        cmd.env(key, value);
    }
    cmd.env("PATH", enhanced_path(opts.provider_binary));
    cmd.stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
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
    let mut child = cmd
        .spawn()
        .map_err(|e| AcpError::Spawn(format!("{}: {e}", opts.provider.command)))?;
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
