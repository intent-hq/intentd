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
    apply_codex_config_args, build_provider_args, build_provider_env_for_spawn, enhanced_path,
    ArgInputs, ProviderConfig, UnslothEndpoint,
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
    /// Session-level reasoning-effort level (PROTOCOL §5.5, Option B), or
    /// `None` for the provider default. Consumed by the codex `-c
    /// model_reasoning_effort=…` config override; an effort embedded in a
    /// compound `{base}/{effort}` model id still wins over this value.
    pub reasoning_effort: Option<&'a str>,
    /// Working directory for the child.
    pub cwd: Option<&'a Path>,
    /// Path to a rules file (appended when the provider supports rules).
    pub rules_file: Option<&'a str>,
    /// Path to an MCP config file (appended when the provider supports MCP).
    pub mcp_config_file: Option<&'a str>,
    /// Pre-serialized MCP block (OpenCode `mcp` config shape) merged into
    /// `OPENCODE_CONFIG_CONTENT` for providers that take env config
    /// (opencode, unsloth). Ignored by every other provider.
    pub env_mcp_config: Option<&'a str>,
    /// Unsloth-managed server endpoint injected as the
    /// `provider.unsloth-studio` block in `OPENCODE_CONFIG_CONTENT` (unsloth
    /// provider only; supplied by the managed-server lifecycle at spawn
    /// time). Ignored by every other provider.
    pub unsloth_endpoint: Option<&'a UnslothEndpoint>,
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
    /// True when this spawn will run via npx: no resolved provider binary and
    /// both npx fields set. Single source of truth for the program selection,
    /// the `-y <pkg>` arg prepend, and the Node-child env decisions (heap cap,
    /// #555 codex env scrub).
    pub fn via_npx(&self) -> bool {
        self.provider_binary.is_none()
            && self.npx_fallback_binary.is_some()
            && self.npx_fallback_package.is_some()
    }

    /// Construct options for a provider with all optional inputs unset.
    pub fn new(provider: &'a ProviderConfig) -> Self {
        Self {
            provider,
            model: None,
            reasoning_effort: None,
            cwd: None,
            rules_file: None,
            mcp_config_file: None,
            env_mcp_config: None,
            unsloth_endpoint: None,
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
    if opts.via_npx() {
        if let Some(pkg) = opts.npx_fallback_package {
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
        // Effort fallback precedence: session `reasoningEffort` field, then
        // the env seam. An effort embedded in a compound `{base}/{effort}`
        // model id wins over both (inside `apply_codex_config_args`).
        let env_effort = std::env::var("CODEX_REASONING_EFFORT")
            .ok()
            .or_else(|| std::env::var("CODEX_MODEL_REASONING_EFFORT").ok());
        let effort = opts.reasoning_effort.or(env_effort.as_deref());
        provider_args = apply_codex_config_args(provider_args, opts.model, effort);
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
    build_command_with_captured_env(opts, captured_credential_env())
}

/// The login-shell credential capture merged by [`build_command`]. In this
/// crate's unit tests this compiles to an empty map, so env assertions are
/// deterministic and real captured credentials never enter a test-built
/// `Command`. The seam does NOT prevent the login-shell spawn itself
/// ([`build_command`]'s `enhanced_path` still triggers the shared PATH
/// capture), and `#[cfg(test)]` is crate-local — a cross-crate test calling
/// [`build_command`] gets the production capture, so such tests must not
/// assert on the command's env. The merge logic itself is covered by driving
/// [`build_command_with_captured_env`] directly.
fn captured_credential_env() -> &'static BTreeMap<String, String> {
    #[cfg(not(test))]
    {
        intent_core::path_utils::login_shell_credential_env()
    }
    #[cfg(test)]
    {
        static EMPTY: BTreeMap<String, String> = BTreeMap::new();
        &EMPTY
    }
}

/// [`build_command`] with an injectable captured credential-env map (the
/// cached login-shell capture in production). Captured vars are gap-fill
/// only — see the precedence comment at the merge site below.
fn build_command_with_captured_env(
    opts: &SpawnOptions,
    captured: &BTreeMap<String, String>,
) -> Command {
    let args = build_args(opts);

    // Decide which binary to spawn: provider_binary > npx_fallback (both fields) > provider.command
    let command = if let Some(p) = opts.provider_binary {
        p.as_os_str()
    } else if let (true, Some(npx)) = (opts.via_npx(), opts.npx_fallback_binary) {
        npx.as_os_str()
    } else {
        std::ffi::OsStr::new(opts.provider.command)
    };

    let mut cmd = Command::new(command);
    cmd.args(&args);
    if let Some(cwd) = opts.cwd {
        cmd.current_dir(cwd);
    }
    // An npx spawn (fallback or npx-only) always runs a Node child, so env
    // assembly applies the V8 heap cap even when the provider's declared
    // runtime is Native (codex's npx fallback, intent-hq/monorepo#1661).
    let via_npx = opts.via_npx();
    let provider_env = build_provider_env_for_spawn(
        opts.provider,
        opts.model,
        opts.rules_file,
        opts.env_mcp_config,
        opts.unsloth_endpoint,
        via_npx,
    );
    for (key, value) in &provider_env {
        cmd.env(key, value);
    }
    for (key, value) in &opts.extra_env {
        cmd.env(key, value);
    }

    // Gap-fill the login-shell-captured credential vars (monorepo#1671).
    // Precedence: provider env / extra_env win, then the daemon's own process
    // env (the child inherits it; a var already set there is never
    // overridden), then captured vars fill the remaining gaps — the
    // Dock/auto-update launch case where the daemon env is stripped.
    // SECURITY: values are secrets — never log, trace, or return them.
    for (key, value) in captured {
        if provider_env.contains_key(key)
            || opts.extra_env.contains_key(key)
            || std::env::var_os(key).is_some()
        {
            continue;
        }
        cmd.env(key, value);
    }

    // The pinned codex-acp npx fallback is daemon-managed (not a user escape
    // hatch), so remove CODEX_PATH / CODEX_CONFIG from its inherited env — a
    // stray or hostile value could redirect the adapter away from the vendored
    // binary (#555). Applies after the captured-env merge above, so a captured
    // login-shell value is stripped too. Resolved binaries (providers.paths /
    // PATH scan) keep the daemon env untouched.
    if opts.provider.id == "codex" && via_npx {
        cmd.env_remove("CODEX_PATH");
        cmd.env_remove("CODEX_CONFIG");
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

    /// Kill the child process and, on unix, its whole process group — the
    /// child is its own group leader (`process_group(0)` in [`build_command`]),
    /// so `killpg` terminates grandchildren a bare `kill()` would orphan (the
    /// direct child is reaped via `wait()` below; grandchildren are reaped by
    /// init). Descendants that escaped into their OWN process groups survive
    /// the `killpg`, so they are snapshotted before the kill and swept
    /// afterwards ([`crate::descendant_sweep`]).
    pub async fn kill(&mut self) -> std::io::Result<()> {
        #[cfg(unix)]
        let descendants = match self.child.id() {
            Some(pid) => crate::descendant_sweep::descendant_pids(pid).await,
            None => Vec::new(),
        };
        #[cfg(unix)]
        if let Some(pid) = self.child.id() {
            use nix::sys::signal::{killpg, Signal};
            use nix::unistd::Pid;
            let _ = killpg(Pid::from_raw(pid as i32), Signal::SIGKILL);
        }
        // The group SIGKILL above may already have terminated the direct child,
        // making `start_kill` report a spurious "already exited" error
        // (InvalidInput) — tolerate it. `wait()` runs unconditionally so the
        // child is always reaped instead of lingering as a zombie (the group
        // SIGKILL was already sent even when `start_kill` errors), and the
        // sweep likewise runs on every path; the first error is then returned.
        let kill_result = match self.child.start_kill() {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::InvalidInput => Ok(()),
            Err(e) => Err(e),
        };
        let wait_result = self.child.wait().await.map(|_| ());
        #[cfg(unix)]
        crate::descendant_sweep::sweep_escaped_descendants(&descendants).await;
        kill_result.and(wait_result)
    }

    /// Decompose into the child and connection (e.g. to store separately).
    pub fn into_parts(self) -> (Child, Connection) {
        (self.child, self.connection)
    }
}

/// Spawn the provider and wire up its [`Connection`] (§6.2 + §6.3).
pub fn spawn_provider(opts: &SpawnOptions, hooks: ConnectionHooks) -> AcpResult<SpawnedAgent> {
    let mut cmd = build_command(opts);
    let command_name = opts.provider_binary.map_or_else(
        || opts.provider.command.to_string(),
        |p| p.display().to_string(),
    );
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
    fn build_args_applies_session_reasoning_effort_for_codex() {
        let codex = intent_providers::find_provider("codex").unwrap();
        let mut opts = SpawnOptions::new(codex);
        opts.model = Some("gpt-5.3-codex");
        opts.reasoning_effort = Some("xhigh");
        let args = build_args(&opts);
        assert!(
            args.windows(2)
                .any(|w| w[0] == "-c" && w[1] == "model_reasoning_effort=\"xhigh\""),
            "session effort missing from codex args: {args:?}"
        );

        // A compound {base}/{effort} model id wins over the session field.
        let mut opts = SpawnOptions::new(codex);
        opts.model = Some("gpt-5.3-codex/high");
        opts.reasoning_effort = Some("xhigh");
        let args = build_args(&opts);
        assert!(
            args.windows(2)
                .any(|w| w[0] == "-c" && w[1] == "model_reasoning_effort=\"high\""),
            "model-embedded effort must win: {args:?}"
        );
        assert!(
            !args.iter().any(|a| a.contains("xhigh")),
            "session effort must not override the model-embedded one: {args:?}"
        );
    }

    #[test]
    fn build_args_ignores_reasoning_effort_for_non_codex_providers() {
        let auggie = intent_providers::find_provider("auggie").unwrap();
        let mut opts = SpawnOptions::new(auggie);
        opts.reasoning_effort = Some("high");
        let args = build_args(&opts);
        assert!(
            !args.iter().any(|a| a.contains("model_reasoning_effort")),
            "non-codex spawn args unexpectedly carry effort config: {args:?}"
        );
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
            "grok",
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

#[cfg(all(test, unix))]
mod kill_tests {
    use super::*;
    use std::time::Duration;

    /// `SpawnedAgent::kill` must terminate the WHOLE process group: a `sh`
    /// child that forks a `sleep 30` grandchild (writing its pid to a file)
    /// leaves no survivor after `kill()` — a direct-child-only kill would
    /// orphan it (the grandchild is reaped by init once killed).
    #[tokio::test]
    async fn kill_reaps_grandchildren_via_process_group() {
        let pidfile =
            std::env::temp_dir().join(format!("intent-acp-groupkill-{}.pid", uuid::Uuid::new_v4()));
        let base = *intent_providers::find_provider("auggie").unwrap();
        let provider = intent_providers::ProviderConfig {
            command: "sh",
            base_args: &["-c", r#"sleep 30 & echo $! > "$INTENT_TEST_PIDFILE"; wait"#],
            model_flag: None,
            rules_flag: None,
            mcp_config_flag: None,
            quiet_flag: None,
            supports_mcp_config: false,
            supports_rules_file: false,
            ..base
        };
        let mut opts = SpawnOptions::new(&provider);
        opts.extra_env.insert(
            "INTENT_TEST_PIDFILE".to_string(),
            pidfile.display().to_string(),
        );
        let mut agent = spawn_provider(&opts, ConnectionHooks::default()).expect("spawn sh child");

        let mut grandchild_pid = None;
        for _ in 0..100 {
            if let Ok(s) = tokio::fs::read_to_string(&pidfile).await {
                if let Ok(pid) = s.trim().parse::<i32>() {
                    grandchild_pid = Some(pid);
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let grandchild_pid = grandchild_pid.expect("grandchild pid written");

        agent.kill().await.expect("kill agent");
        tokio::fs::remove_file(&pidfile).await.ok();

        // The grandchild is not our direct child, so it lingers until init
        // reaps it; `kill(pid, 0)` returns ESRCH once the pid is gone.
        for _ in 0..100 {
            if nix::sys::signal::kill(nix::unistd::Pid::from_raw(grandchild_pid), None).is_err() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("grandchild pid {grandchild_pid} still alive after group kill");
    }

    /// Regression for the killpg-escape vector: an MCP-server-style grandchild
    /// that moves into its OWN process group survives the group SIGKILL in
    /// `kill()` (observed live: codex-acp's auggie ran with pgid == its own
    /// pid); the descendant sweep must still reap it. The grandchild escapes
    /// the group via `set -m` job control (background jobs become their own
    /// group leaders).
    #[tokio::test]
    async fn kill_sweeps_grandchild_in_foreign_process_group() {
        use nix::unistd::{getpgid, Pid};

        let pidfile =
            std::env::temp_dir().join(format!("intent-acp-sweep-{}.pid", uuid::Uuid::new_v4()));
        let base = *intent_providers::find_provider("auggie").unwrap();
        let provider = intent_providers::ProviderConfig {
            command: "bash",
            base_args: &[
                "-c",
                r#"set -m; sleep 300 & echo $! > "$INTENT_TEST_PIDFILE"; wait"#,
            ],
            model_flag: None,
            rules_flag: None,
            mcp_config_flag: None,
            quiet_flag: None,
            supports_mcp_config: false,
            supports_rules_file: false,
            ..base
        };
        let mut opts = SpawnOptions::new(&provider);
        opts.extra_env.insert(
            "INTENT_TEST_PIDFILE".to_string(),
            pidfile.display().to_string(),
        );
        let mut agent =
            spawn_provider(&opts, ConnectionHooks::default()).expect("spawn bash child");

        let mut grandchild_pid = None;
        for _ in 0..250 {
            if let Ok(s) = tokio::fs::read_to_string(&pidfile).await {
                if let Ok(pid) = s.trim().parse::<i32>() {
                    grandchild_pid = Some(pid);
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let grandchild_pid = grandchild_pid.expect("grandchild pid written");

        // Prove the grandchild actually escaped the child's process group —
        // otherwise killpg would reach it and the test would be vacuous.
        let child_pid = agent.child_mut().id().expect("child pid");
        let child_pgid = getpgid(Some(Pid::from_raw(child_pid as i32))).expect("child pgid");
        let grandchild_pgid =
            getpgid(Some(Pid::from_raw(grandchild_pid))).expect("grandchild pgid");
        assert_ne!(
            grandchild_pgid, child_pgid,
            "grandchild must be in a foreign process group for this regression test"
        );

        // Distinct failure signal for the snapshot path: if `ps` stalls past
        // its budget on a loaded runner the snapshot comes back empty and the
        // sweep silently no-ops — fail here, not at the terminal panic below.
        let snapshot = crate::descendant_sweep::descendant_pids(child_pid).await;
        assert!(
            snapshot.contains(&grandchild_pid),
            "descendant snapshot {snapshot:?} must include grandchild {grandchild_pid} \
             (empty/partial snapshot ⇒ `ps` walk failed, not the sweep)"
        );

        agent.kill().await.expect("kill agent");
        tokio::fs::remove_file(&pidfile).await.ok();

        // `kill(pid, 0)` returns ESRCH once the pid is gone (the grandchild
        // is not our direct child, so init reaps it after the sweep's kill).
        for _ in 0..100 {
            if nix::sys::signal::kill(nix::unistd::Pid::from_raw(grandchild_pid), None).is_err() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("grandchild pid {grandchild_pid} still alive after kill() sweep");
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
        let expected_prefix = format!("{parent_dir}{sep}");
        assert!(
            path_value.starts_with(&expected_prefix),
            "PATH should start with {expected_prefix}, got: {path_value}"
        );
    }

    #[test]
    fn build_command_merges_env_mcp_config_into_opencode_config_content() {
        let provider = intent_providers::find_provider("opencode").unwrap();
        let mut opts = SpawnOptions::new(provider);
        opts.model = Some("claude-sonnet-4");
        let mcp_json = r#"{"workspace-mcp":{"type":"local","command":["intentd","mcp-bridge","--connect","127.0.0.1:9999"],"enabled":true,"environment":{}}}"#;
        opts.env_mcp_config = Some(mcp_json);
        let cmd = build_command(&opts);
        let content = cmd
            .as_std()
            .get_envs()
            .find(|(k, _)| *k == "OPENCODE_CONFIG_CONTENT")
            .and_then(|(_, v)| v)
            .expect("OPENCODE_CONFIG_CONTENT must be set")
            .to_string_lossy()
            .into_owned();
        let parsed: serde_json::Value =
            serde_json::from_str(&content).expect("OPENCODE_CONFIG_CONTENT must be valid JSON");
        assert_eq!(parsed["permission"]["task"], "deny");
        assert_eq!(parsed["model"], "claude-sonnet-4");
        assert_eq!(
            parsed["mcp"]["workspace-mcp"]["command"][1], "mcp-bridge",
            "bridge server must ride in the mcp block"
        );
    }

    #[test]
    fn build_command_uses_npx_when_no_provider_binary() {
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
    fn build_command_enriches_path_with_npx_parent_when_spawning_via_npx() {
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
        let expected_prefix = format!("{parent_dir}{sep}");
        assert!(
            path_value.starts_with(&expected_prefix),
            "PATH should start with {expected_prefix} so npx can find node, got: {path_value}"
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

    #[test]
    fn codex_fallback_npx_package_is_pinned() {
        let provider = intent_providers::find_provider("codex").unwrap();
        let pkg = provider
            .fallback_npx_package
            .expect("codex should have fallback_npx_package configured");
        assert_eq!(pkg, intent_providers::config::CODEX_ACP_NPX_PACKAGE);
        assert!(
            pkg.starts_with("@agentclientprotocol/codex-acp@"),
            "codex npx fallback should use the @agentclientprotocol package, got: {pkg}"
        );
        let version = pkg.rsplit('@').next().unwrap();
        let parts: Vec<&str> = version.split('.').collect();
        assert!(
            parts.len() == 3 && parts.iter().all(|part| part.parse::<u32>().is_ok()),
            "codex npx fallback must be pinned to an exact semver version, got: {version}"
        );
    }

    /// Whether `cmd` explicitly removes `key` from the child's inherited env
    /// (`get_envs()` yields `(key, None)` for `env_remove` entries).
    fn env_removed(cmd: &Command, key: &str) -> bool {
        cmd.as_std()
            .get_envs()
            .any(|(k, v)| k == key && v.is_none())
    }

    /// The explicitly-set value of `key` on `cmd`, if any.
    fn env_value(cmd: &Command, key: &str) -> Option<String> {
        cmd.as_std()
            .get_envs()
            .find(|(k, _)| *k == key)
            .and_then(|(_, v)| v)
            .map(|v| v.to_string_lossy().into_owned())
    }

    #[test]
    fn build_command_applies_heap_cap_on_codex_npx_fallback() {
        // codex is declared Native (Rust binary), but the npx-fallback child
        // is Node — the STAB-50 heap cap must apply (intent-hq/monorepo#1661).
        let provider = intent_providers::find_provider("codex").unwrap();
        let mut opts = SpawnOptions::new(provider);
        let npx_path = PathBuf::from("/usr/local/bin/npx");
        opts.npx_fallback_binary = Some(&npx_path);
        opts.npx_fallback_package = provider.fallback_npx_package;
        let cmd = build_command(&opts);
        let node_options = env_value(&cmd, "NODE_OPTIONS");
        if std::env::var("NODE_OPTIONS").is_ok_and(|v| v.contains("--max-old-space-size")) {
            // An inherited cap wins: injection is (correctly) skipped.
            assert!(
                node_options.is_none(),
                "inherited --max-old-space-size must suppress injection"
            );
        } else {
            let v = node_options.expect("npx-fallback codex spawn must set NODE_OPTIONS");
            assert!(
                v.contains("--max-old-space-size="),
                "NODE_OPTIONS must carry the heap cap, got: {v}"
            );
        }
    }

    #[test]
    fn build_command_no_heap_cap_on_codex_resolved_binary() {
        // The resolved native codex-acp binary is not V8: no NODE_OPTIONS.
        let provider = intent_providers::find_provider("codex").unwrap();
        let mut opts = SpawnOptions::new(provider);
        let provider_binary = PathBuf::from("/custom/codex-acp");
        let npx_path = PathBuf::from("/usr/local/bin/npx");
        opts.provider_binary = Some(&provider_binary);
        opts.npx_fallback_binary = Some(&npx_path);
        opts.npx_fallback_package = provider.fallback_npx_package;
        let cmd = build_command(&opts);
        assert!(
            env_value(&cmd, "NODE_OPTIONS").is_none(),
            "resolved-binary codex spawn must not inject NODE_OPTIONS"
        );
    }

    #[test]
    fn build_command_strips_codex_env_on_npx_fallback_spawn() {
        // The pinned npx fallback is daemon-managed: a stray CODEX_PATH /
        // CODEX_CONFIG in the daemon env must not redirect the adapter (#555).
        let provider = intent_providers::find_provider("codex").unwrap();
        let mut opts = SpawnOptions::new(provider);
        let npx_path = PathBuf::from("/usr/local/bin/npx");
        opts.npx_fallback_binary = Some(&npx_path);
        opts.npx_fallback_package = provider.fallback_npx_package;
        let cmd = build_command(&opts);
        assert!(
            env_removed(&cmd, "CODEX_PATH"),
            "npx-fallback codex spawn must remove CODEX_PATH from the child env"
        );
        assert!(
            env_removed(&cmd, "CODEX_CONFIG"),
            "npx-fallback codex spawn must remove CODEX_CONFIG from the child env"
        );
    }

    #[test]
    fn build_command_keeps_codex_env_for_resolved_binary_spawn() {
        // A resolved codex-acp binary (providers.paths override or PATH scan)
        // is the user's escape hatch — its env must be left untouched.
        let provider = intent_providers::find_provider("codex").unwrap();
        let mut opts = SpawnOptions::new(provider);
        let provider_binary = PathBuf::from("/custom/codex-acp");
        let npx_path = PathBuf::from("/usr/local/bin/npx");
        opts.provider_binary = Some(&provider_binary);
        opts.npx_fallback_binary = Some(&npx_path);
        opts.npx_fallback_package = provider.fallback_npx_package;
        let cmd = build_command(&opts);
        let touched = cmd
            .as_std()
            .get_envs()
            .any(|(k, _)| k == "CODEX_PATH" || k == "CODEX_CONFIG");
        assert!(
            !touched,
            "resolved-binary codex spawn must inherit CODEX_PATH/CODEX_CONFIG untouched"
        );
    }

    #[test]
    fn build_command_leaves_codex_env_alone_for_other_npx_providers() {
        let provider = intent_providers::find_provider("claude-code").unwrap();
        let mut opts = SpawnOptions::new(provider);
        let npx_path = PathBuf::from("/usr/local/bin/npx");
        opts.npx_fallback_binary = Some(&npx_path);
        opts.npx_fallback_package = provider.npx_only_package;
        let cmd = build_command(&opts);
        let touched = cmd
            .as_std()
            .get_envs()
            .any(|(k, _)| k == "CODEX_PATH" || k == "CODEX_CONFIG");
        assert!(
            !touched,
            "non-codex npx spawns must not touch CODEX_PATH/CODEX_CONFIG"
        );
    }
}

#[cfg(test)]
mod captured_env_tests {
    use super::*;
    use std::path::PathBuf;

    /// The explicit env entry for `key` on `cmd`, if any (`None` also when the
    /// entry is an `env_remove` marker).
    fn env_value(cmd: &Command, key: &str) -> Option<String> {
        cmd.as_std()
            .get_envs()
            .find(|(k, _)| *k == key)
            .and_then(|(_, v)| v)
            .map(|v| v.to_string_lossy().into_owned())
    }

    /// A var name guaranteed absent from this process's env.
    fn absent_var_name() -> String {
        let name = format!(
            "INTENT_TEST_CAPTURED_{}",
            uuid::Uuid::new_v4().simple().to_string().to_uppercase()
        );
        assert!(std::env::var_os(&name).is_none());
        name
    }

    #[test]
    fn captured_var_gap_fills_when_absent_everywhere() {
        let provider = intent_providers::find_provider("auggie").unwrap();
        let opts = SpawnOptions::new(provider);
        let name = absent_var_name();
        let mut captured = BTreeMap::new();
        captured.insert(name.clone(), "captured-value".to_string());
        let cmd = build_command_with_captured_env(&opts, &captured);
        assert_eq!(env_value(&cmd, &name).as_deref(), Some("captured-value"));
    }

    #[test]
    fn captured_var_never_overrides_daemon_process_env() {
        let provider = intent_providers::find_provider("auggie").unwrap();
        let opts = SpawnOptions::new(provider);
        // Pick a var actually present in the daemon (test process) env that
        // the command does not already set explicitly (provider env / PATH).
        // Restricted to stable well-known names: scanning all of
        // `std::env::vars()` can race sibling tests that mutate process env
        // (e.g. session.rs's INTENTD_PROMPT_IDLE_TIMEOUT_MS guard).
        let baseline = build_command_with_captured_env(&opts, &BTreeMap::new());
        let preset: std::collections::HashSet<String> = baseline
            .as_std()
            .get_envs()
            .map(|(k, _)| k.to_string_lossy().into_owned())
            .collect();
        let present = ["HOME", "USER", "TMPDIR", "SHELL", "PWD", "LOGNAME"]
            .into_iter()
            .find(|k| std::env::var_os(k).is_some() && !preset.contains(*k))
            .expect("process env has at least one stable var the command leaves alone")
            .to_string();
        let mut captured = BTreeMap::new();
        captured.insert(present.clone(), "captured-must-lose".to_string());
        let cmd = build_command_with_captured_env(&opts, &captured);
        assert!(
            !cmd.as_std()
                .get_envs()
                .any(|(k, _)| k.to_string_lossy() == present),
            "a var already in the daemon's process env must be inherited, not set from the capture"
        );
    }

    #[test]
    fn provider_env_wins_over_captured() {
        // OPENCODE_CONFIG_CONTENT is both provider-built (opencode always
        // emits it) and on the capture allow-list (OPENCODE_ prefix) — the
        // provider-built value must win.
        let provider = intent_providers::find_provider("opencode").unwrap();
        let opts = SpawnOptions::new(provider);
        let mut captured = BTreeMap::new();
        captured.insert(
            "OPENCODE_CONFIG_CONTENT".to_string(),
            "captured-must-lose".to_string(),
        );
        let cmd = build_command_with_captured_env(&opts, &captured);
        let value = env_value(&cmd, "OPENCODE_CONFIG_CONTENT")
            .expect("opencode provider env sets OPENCODE_CONFIG_CONTENT");
        assert_ne!(value, "captured-must-lose");
        serde_json::from_str::<serde_json::Value>(&value)
            .expect("provider-built config must win and stay valid JSON");
    }

    #[test]
    fn extra_env_wins_over_captured() {
        let provider = intent_providers::find_provider("auggie").unwrap();
        let mut opts = SpawnOptions::new(provider);
        opts.extra_env
            .insert("ANTHROPIC_API_KEY".to_string(), "from-extra".to_string());
        let mut captured = BTreeMap::new();
        captured.insert(
            "ANTHROPIC_API_KEY".to_string(),
            "captured-must-lose".to_string(),
        );
        let cmd = build_command_with_captured_env(&opts, &captured);
        assert_eq!(
            env_value(&cmd, "ANTHROPIC_API_KEY").as_deref(),
            Some("from-extra")
        );
    }

    #[test]
    fn codex_env_remove_strips_captured_values_on_npx_fallback() {
        // The #555 hatch runs after the captured-env merge: even a captured
        // login-shell CODEX_PATH / CODEX_CONFIG must be removed from the
        // daemon-managed npx fallback spawn.
        let provider = intent_providers::find_provider("codex").unwrap();
        let mut opts = SpawnOptions::new(provider);
        let npx_path = PathBuf::from("/usr/local/bin/npx");
        opts.npx_fallback_binary = Some(&npx_path);
        opts.npx_fallback_package = provider.fallback_npx_package;
        let mut captured = BTreeMap::new();
        captured.insert("CODEX_PATH".to_string(), "/tmp/evil".to_string());
        captured.insert("CODEX_CONFIG".to_string(), "/tmp/evil.toml".to_string());
        let cmd = build_command_with_captured_env(&opts, &captured);
        for key in ["CODEX_PATH", "CODEX_CONFIG"] {
            assert!(
                cmd.as_std()
                    .get_envs()
                    .any(|(k, v)| k == key && v.is_none()),
                "{key} must be env_remove'd from the npx-fallback codex spawn even when captured"
            );
        }
    }
}
