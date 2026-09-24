//! Spawning piped-stdio provider processes (§6.2).
//!
//! Resolves args/env from the `intent_providers` registry, enriches `PATH` so a
//! `#!/usr/bin/env node` shebang resolves the right `node`, applies Codex
//! subagent policy, and spawns with all three pipes captured and
//! `kill_on_drop(true)`. The captured pipes are handed to a [`Connection`].
//!
//! Children start at reduced scheduling priority relative to the daemon's own
//! (nice `daemon + 5` on Unix, one priority class below the daemon's on
//! Windows; see [`agent_nice`]) so an agent process tree competing for CPU
//! never starves the daemon that drives it — and never outranks it, however
//! the daemon itself was niced.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;

use intent_providers::{
    build_provider_args, build_provider_env_for_spawn, enhanced_path, ArgInputs, ProviderConfig,
    UnslothEndpoint, CODEX_SUBAGENT_POLICY_CONFIG,
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
    /// `None` for the provider default. Codex receives it through ACP session
    /// configuration; the pinned adapter does not consume CLI effort flags.
    pub reasoning_effort: Option<&'a str>,
    /// The agent's workspace: the ACP session cwd, and the child's working
    /// directory for resolved-binary and bare-command launches. An npx
    /// launch does NOT start here — see [`SpawnOptions::npx_launch_root`].
    pub cwd: Option<&'a Path>,
    /// Parent directory for the neutral per-spawn [`NpxLaunchDir`] an npx
    /// launch starts in (`None` → the OS temp dir). npm reads the package
    /// configuration of its cwd, so `npx -y <adapter>` run inside a Bun/pnpm
    /// workspace with `catalog:` specifiers dies before the adapter starts
    /// (intent-hq/intent#5738); the workspace stays the ACP session cwd,
    /// never the npx process cwd. The launch dir plus
    /// [`NPX_NO_WORKSPACES_ARG`] shield npx from this root's ancestors too, so
    /// any daemon-owned directory serves. Ignored by every other launch tier.
    pub npx_launch_root: Option<&'a Path>,
    /// Path to a rules file (appended when the provider supports rules).
    pub rules_file: Option<&'a str>,
    /// Path to an MCP config file (appended when the provider supports MCP).
    pub mcp_config_file: Option<&'a str>,
    /// Pre-serialized MCP block (`OpenCode` `mcp` config shape) merged into
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
    /// Extra environment overrides, applied before provider-owned launch policy.
    pub extra_env: BTreeMap<String, String>,
    /// Provider-native tools to strip via the provider's `--remove-tool`
    /// equivalent (§18.4 CLI-side enforcement). Gated on
    /// [`ProviderConfig::remove_tool_flag`] — providers that don't advertise a
    /// flag silently ignore the input rather than receive an unknown arg.
    pub tools_to_remove: Vec<&'static str>,
    /// When `provider_binary` is None and the provider spawns via npx (either a
    /// `fallback_npx_package` or an npx-only provider's pinned
    /// `npx_only_package`), this is the resolved npx path.
    pub npx_fallback_binary: Option<&'a Path>,
    /// The package spec to pass to npx when `npx_fallback_binary` is set (may
    /// carry a pinned `@<version>` suffix).
    pub npx_fallback_package: Option<&'static str>,
    /// The `agents.acpNodeMaxOldSpaceMb` setting at spawn time: the V8
    /// `--max-old-space-size` cap (MB) injected via `NODE_OPTIONS` for
    /// Node/Electron children (and npx spawns). `None` means unset — the
    /// built-in 8192 MB default applies. The `INTENTD_ACP_NODE_MAX_OLD_SPACE_MB`
    /// env var still overrides either. Native runtimes ignore it.
    pub node_max_old_space_mb: Option<u32>,
}

impl<'a> SpawnOptions<'a> {
    /// True when this spawn will run via npx: no resolved provider binary and
    /// both npx fields set. Single source of truth for the program selection,
    /// the `-y <pkg>` arg prepend, and the Node-child heap cap.
    #[must_use]
    pub fn via_npx(&self) -> bool {
        self.provider_binary.is_none()
            && self.npx_fallback_binary.is_some()
            && self.npx_fallback_package.is_some()
    }

    /// The launch tier this spawn will use and the program it execs:
    /// `provider_binary` > npx fallback (both fields) > bare `provider.command`.
    /// Single decision point shared by [`build_command`] and the spawn-failure
    /// attribution in [`spawn_provider`].
    #[must_use]
    pub fn launch_target(&self) -> (LaunchMode, &'a std::ffi::OsStr) {
        if let Some(p) = self.provider_binary {
            (LaunchMode::ResolvedBinary, p.as_os_str())
        } else if let (true, Some(npx)) = (self.via_npx(), self.npx_fallback_binary) {
            (LaunchMode::NpxFallback, npx.as_os_str())
        } else {
            (
                LaunchMode::BareCommand,
                std::ffi::OsStr::new(self.provider.command),
            )
        }
    }

    /// The binary whose parent dir enriches the child's `PATH`
    /// (`provider_binary`, else the npx binary when a package is pinned).
    fn path_enrichment_binary(&self) -> Option<&'a Path> {
        self.provider_binary.or_else(|| {
            if self.npx_fallback_package.is_some() {
                self.npx_fallback_binary
            } else {
                None
            }
        })
    }

    /// Construct options for a provider with all optional inputs unset.
    #[must_use]
    pub fn new(provider: &'a ProviderConfig) -> Self {
        Self {
            provider,
            model: None,
            reasoning_effort: None,
            cwd: None,
            npx_launch_root: None,
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
            node_max_old_space_mb: None,
        }
    }
}

/// Which launch tier [`SpawnOptions::launch_target`] selected. Carried by
/// [`AcpError::ProviderNotFound`] so a missing **bare** command (nothing
/// resolved a provider binary and the `PATH` lookup failed) is told apart
/// from a resolved binary path that vanished.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LaunchMode {
    /// `provider_binary` — an explicit `providers.paths` override or a
    /// discovered install — is exec'd directly.
    ResolvedBinary,
    /// No resolved binary; the provider's pinned npx package runs via npx.
    NpxFallback,
    /// No resolved binary and no npx fallback: the bare `provider.command`
    /// is exec'd and resolution is left to the enriched `PATH`.
    BareCommand,
}

/// Redirecting Codex variables removed by managed npm launches. Local
/// adapters retain these overrides. Diagnostics and catalog probes use this
/// same policy rather than carrying another environment-removal list.
#[must_use]
pub fn codex_managed_env_removals(provider_id: &str, via_npx: bool) -> &'static [&'static str] {
    if provider_id == "codex" && via_npx {
        &["CODEX_PATH", "CODEX_CONFIG"]
    } else {
        &[]
    }
}

/// The neutral directory an npx launch starts in (intent-hq/intent#5738).
/// Created per spawn under [`SpawnOptions::npx_launch_root`] (owner-only on
/// Unix, named `intent_core::NPX_LAUNCH_DIR_PREFIX` + uuid so a launch root
/// sweep can tell it from ordinary leftovers) and removed when dropped — the
/// owner keeps it alive for the child's lifetime, since Node resolves
/// relative paths against, and `process.cwd()` fails inside, a removed
/// directory.
///
/// Being empty is not enough: npm picks its project root by walking up from
/// the cwd to the nearest `package.json` (or `node_modules`) and reads that
/// root's `.npmrc`, so the directory holds [`NPX_LAUNCH_SENTINEL_MANIFEST`] — a
/// private, dependency-less manifest that makes the launch dir npm's nearest
/// root. The sentinel alone is not enough either: npm walks on past it to an
/// ancestor whose `workspaces` glob matches the launch dir, so
/// [`build_args`] also passes [`NPX_NO_WORKSPACES_ARG`]; together they keep
/// the launch dir the project root whatever the launch root's ancestors
/// contain.
#[derive(Debug)]
pub struct NpxLaunchDir {
    path: PathBuf,
}

/// The `package.json` written into every [`NpxLaunchDir`].
pub const NPX_LAUNCH_SENTINEL_MANIFEST: &str =
    "{\n  \"name\": \"intentd-npx-launch\",\n  \"private\": true\n}\n";

/// The npx argument that keeps npm's project root at the [`NpxLaunchDir`]
/// (intent-hq/intent#5738). The sentinel manifest is only npm's *first*
/// candidate: `@npmcli/config` `loadLocalPrefix` keeps walking up and adopts
/// any ancestor `package.json` whose `workspaces` glob matches the launch
/// dir — loading that root's `.npmrc`, and rejecting two live launch dirs as
/// duplicate workspace names — unless `workspaces` is `false` on the command
/// line (the env layer does not count). Must precede the package positional:
/// npx passes everything after it to the adapter.
pub const NPX_NO_WORKSPACES_ARG: &str = "--workspaces=false";

/// Whether `key` is an npm environment config selecting workspaces. npm reads
/// every `npm_config_<key>` variable case-insensitively and normalises
/// `<key>` like `@npmcli/config` `loadEnv` (`_` → `-`, lowercased), so
/// `npm_config_workspace`, `NPM_CONFIG_WORKSPACE` and
/// `npm_config_include_workspace_root` all count. An inherited `workspace`
/// selector is fatal next to [`NPX_NO_WORKSPACES_ARG`] (`npm error Cannot use
/// --no-workspaces and --workspace at the same time`) and, without the
/// switch, when the launch dir has no such workspace (`No workspaces found`);
/// `workspaces` / `include-workspace-root` steer the same selection.
#[must_use]
pub fn is_npm_workspace_selector_env(key: &str) -> bool {
    const PREFIX: &str = "npm_config_";
    let Some(prefix) = key.get(..PREFIX.len()) else {
        return false;
    };
    if !prefix.eq_ignore_ascii_case(PREFIX) {
        return false;
    }
    let normalized = key[PREFIX.len()..].replace('_', "-").to_ascii_lowercase();
    matches!(
        normalized.as_str(),
        "workspace" | "workspaces" | "include-workspace-root"
    )
}

/// The environment keys a managed npx launch removes so no workspace selector
/// reaches npm: every [`is_npm_workspace_selector_env`] key in the daemon's own
/// environment (the child inherits it) plus those among `explicit`, the keys
/// the launch would otherwise set itself. Apply with `env_remove` after every
/// other env merge.
#[must_use]
pub fn npm_workspace_selector_env_keys<'a>(
    explicit: impl IntoIterator<Item = &'a str>,
) -> std::collections::BTreeSet<String> {
    std::env::vars_os()
        .filter_map(|(key, _)| key.into_string().ok())
        .chain(explicit.into_iter().map(str::to_owned))
        .filter(|key| is_npm_workspace_selector_env(key))
        .collect()
}

impl NpxLaunchDir {
    /// Create a fresh directory under `root` (the OS temp dir when `None`),
    /// creating missing parents, holding only the sentinel `package.json`.
    ///
    /// # Errors
    ///
    /// Returns the underlying I/O error when the directory or its sentinel
    /// manifest cannot be created.
    pub fn create(root: Option<&Path>) -> std::io::Result<Self> {
        let root = root.map_or_else(std::env::temp_dir, Path::to_path_buf);
        let path = root.join(format!(
            "{}{}",
            intent_core::NPX_LAUNCH_DIR_PREFIX,
            uuid::Uuid::new_v4()
        ));
        let mut builder = std::fs::DirBuilder::new();
        builder.recursive(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        builder.create(&path)?;
        let dir = Self { path };
        std::fs::write(dir.path.join("package.json"), NPX_LAUNCH_SENTINEL_MANIFEST)?;
        Ok(dir)
    }

    /// The directory the npx child runs in.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for NpxLaunchDir {
    fn drop(&mut self) {
        if let Err(e) = std::fs::remove_dir_all(&self.path) {
            if e.kind() != std::io::ErrorKind::NotFound {
                tracing::debug!(
                    path = %self.path.display(),
                    error = %e,
                    "failed to remove npx launch dir"
                );
            }
        }
    }
}

impl std::fmt::Display for LaunchMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::ResolvedBinary => "resolved provider binary",
            Self::NpxFallback => "npx fallback binary",
            Self::BareCommand => {
                "bare command; no providers.paths override or discovered binary resolved, \
                 so it was looked up on the daemon PATH"
            }
        })
    }
}

/// Assemble provider launch arguments. Codex model and effort are applied
/// through ACP config options; its selected adapter ignores CLI config flags.
/// When spawning via npx, prepends [`NPX_NO_WORKSPACES_ARG`] and
/// `-y <package>` before the provider's args.
#[must_use]
pub fn build_args(opts: &SpawnOptions) -> Vec<String> {
    let mut args = Vec::new();

    // When using npx fallback (provider_binary not set AND both npx fields are set),
    // prepend the npx-specific args before the provider's args
    if opts.via_npx() {
        if let Some(pkg) = opts.npx_fallback_package {
            args.push(NPX_NO_WORKSPACES_ARG.to_string());
            args.push("-y".to_string());
            args.push(pkg.to_string());
        }
    }

    // Then append the provider's normal ACP args
    let provider_args = build_provider_args(
        opts.provider,
        &ArgInputs {
            model: opts.model,
            rules_file: opts.rules_file,
            mcp_config_file: opts.mcp_config_file,
            quiet: opts.quiet,
            tools_to_remove: &opts.tools_to_remove,
        },
    );
    args.extend(provider_args);
    args
}

/// Env var tuning how much ACP agent children are niced relative to the
/// daemon (added to the daemon's own nice value, capped at 19). `0` disables
/// the priority reduction; other values are clamped to `0..=19`.
pub const AGENT_NICE_ENV: &str = "INTENTD_AGENT_NICE";

/// Nice increment ACP agent children get when [`AGENT_NICE_ENV`] is unset.
pub const DEFAULT_AGENT_NICE: i32 = 5;

/// The nice increment ACP agent children start with over the daemon's own
/// nice value: [`AGENT_NICE_ENV`] when set (clamped to `0..=19`; an
/// unparseable value falls back to the default), else [`DEFAULT_AGENT_NICE`].
/// The child's nice is `min(daemon + increment, 19)`, so a child never
/// outranks the daemon even when the daemon is already niced. `0` leaves the
/// child at the daemon's priority. On Windows any non-zero value maps to the
/// priority class one step below the daemon's (see [`apply_reduced_priority`]).
#[must_use]
pub fn agent_nice() -> i32 {
    agent_nice_from(std::env::var(AGENT_NICE_ENV).ok().as_deref())
}

/// [`agent_nice`] over an injected raw env value.
fn agent_nice_from(raw: Option<&str>) -> i32 {
    let Some(raw) = raw.map(str::trim).filter(|s| !s.is_empty()) else {
        return DEFAULT_AGENT_NICE;
    };
    let Ok(n) = raw.parse::<i64>() else {
        tracing::warn!(
            env = AGENT_NICE_ENV,
            value = raw,
            default = DEFAULT_AGENT_NICE,
            "unparseable agent nice value; using the default"
        );
        return DEFAULT_AGENT_NICE;
    };
    i32::try_from(n.clamp(0, 19)).unwrap_or(DEFAULT_AGENT_NICE)
}

/// Cap on the nice value the demotion targets — the portable
/// least-favourable value (Linux's `PRIO_MAX`). Not every OS stops there
/// (macOS's `PRIO_MAX` is 20), so a parent already at or above it is left
/// where it is rather than pulled down to the cap.
#[cfg(unix)]
const MAX_NICE: i32 = 19;

/// The nice value of process `pid` (`0` = the calling process), or the raw
/// `errno` when it cannot be read. `getpriority` legitimately returns `-1`
/// for nice `-1`, so the error is told apart via `errno`, which is cleared
/// first. Only plain syscall wrappers and thread-local `errno` are touched,
/// so this is safe to call between fork and exec.
#[cfg(unix)]
fn nice_of(pid: libc::id_t) -> Result<i32, i32> {
    use nix::errno::Errno;
    Errno::clear();
    // SAFETY: plain syscall wrapper with no pointer arguments.
    let got = unsafe { libc::getpriority(libc::PRIO_PROCESS, pid) };
    let errno = Errno::last_raw();
    if got == -1 && errno != 0 {
        Err(errno)
    } else {
        Ok(got)
    }
}

/// The nice value a child of a process at `parent` nice should run at for
/// increment `increment`: `parent + increment`, capped at [`MAX_NICE`], but
/// never below `parent` — a parent already at or past the cap (possible on
/// macOS, whose range reaches 20) keeps its value — so the child never
/// outranks the daemon.
#[cfg(unix)]
fn target_nice(parent: i32, increment: i32) -> i32 {
    parent
        .saturating_add(increment.max(0))
        .min(MAX_NICE)
        .max(parent)
}

/// Configure `cmd` so the child starts at reduced scheduling priority
/// relative to the spawning daemon. `increment <= 0` leaves the command
/// untouched.
///
/// On Unix the child reads the nice value it inherited (the daemon's) in
/// `pre_exec` and raises it by `increment` (see [`target_nice`]); it is never
/// lowered, so a daemon that is itself niced keeps its edge over the child.
/// On Windows the child gets the priority class one step below the daemon's
/// (`REALTIME`→`HIGH`, `HIGH`→`ABOVE_NORMAL`, `ABOVE_NORMAL`→`NORMAL`,
/// `NORMAL`→`BELOW_NORMAL`, `BELOW_NORMAL`→`IDLE`); an `IDLE` daemon's child
/// inherits `IDLE`.
///
/// A failure to lower the priority must never fail the spawn, so on Unix the
/// `getpriority`/`setpriority` results are deliberately ignored inside
/// `pre_exec` (logging is not async-signal-safe there);
/// [`reduced_priority_shortfall`] reports it from the parent once the child
/// is up.
fn apply_reduced_priority(cmd: &mut Command, increment: i32) {
    if increment <= 0 {
        return;
    }
    #[cfg(unix)]
    {
        // SAFETY: `getpriority`/`setpriority` are plain syscall wrappers that
        // take no pointers and touch no locks or heap state, so they are safe
        // to call between fork and exec.
        unsafe {
            cmd.pre_exec(move || {
                if let Ok(inherited) = nice_of(0) {
                    let target = target_nice(inherited, increment);
                    if target > inherited {
                        libc::setpriority(libc::PRIO_PROCESS, 0, target);
                    }
                }
                Ok(())
            });
        }
    }
    #[cfg(windows)]
    {
        use windows_sys::Win32::System::Threading::{
            GetCurrentProcess, GetPriorityClass, ABOVE_NORMAL_PRIORITY_CLASS,
            BELOW_NORMAL_PRIORITY_CLASS, HIGH_PRIORITY_CLASS, IDLE_PRIORITY_CLASS,
            NORMAL_PRIORITY_CLASS, REALTIME_PRIORITY_CLASS,
        };
        // SAFETY: both take no pointers; `GetCurrentProcess` returns a
        // pseudo-handle that needs no closing.
        let own = unsafe { GetPriorityClass(GetCurrentProcess()) };
        let below = match own {
            REALTIME_PRIORITY_CLASS => HIGH_PRIORITY_CLASS,
            HIGH_PRIORITY_CLASS => ABOVE_NORMAL_PRIORITY_CLASS,
            ABOVE_NORMAL_PRIORITY_CLASS => NORMAL_PRIORITY_CLASS,
            NORMAL_PRIORITY_CLASS => BELOW_NORMAL_PRIORITY_CLASS,
            BELOW_NORMAL_PRIORITY_CLASS | IDLE_PRIORITY_CLASS => IDLE_PRIORITY_CLASS,
            // Unknown or unreadable: leave the default (never above the
            // parent's class) rather than guess.
            _ => return,
        };
        cmd.creation_flags(below);
    }
}

/// Why the freshly spawned child `pid` is NOT running at the nice value
/// [`target_nice`] derives from this process's own nice and `increment` (or
/// a less favourable one), if it is not — `None` when it is, when
/// `increment <= 0`, or when the child is already gone. `Command::spawn`
/// returns only after the exec, so the `pre_exec` `setpriority` has already
/// run when this is read.
#[cfg(unix)]
fn reduced_priority_shortfall(pid: u32, increment: i32) -> Option<String> {
    if increment <= 0 {
        return None;
    }
    let parent = match nice_of(0) {
        Ok(n) => n,
        Err(errno) => {
            return Some(format!(
                "could not read the daemon's own priority: {}",
                std::io::Error::from_raw_os_error(errno)
            ))
        }
    };
    let expected = target_nice(parent, increment);
    let got = match nice_of(pid as libc::id_t) {
        Ok(n) => n,
        Err(libc::ESRCH) => return None,
        Err(errno) => {
            return Some(format!(
                "could not read the child's priority: {}",
                std::io::Error::from_raw_os_error(errno)
            ))
        }
    };
    (got < expected).then(|| {
        format!("child runs at nice {got}, expected at least {expected} (daemon at {parent})")
    })
}

/// Build the `tokio` command (args + env + enriched `PATH` + piped stdio +
/// `kill_on_drop` + reduced priority per [`agent_nice`]) without spawning
/// it. Exposed for testing/inspection.
///
/// When `opts.provider_binary` is set (resolved to an absolute path), spawns
/// that path directly; otherwise, when `opts.npx_fallback_binary` is set,
/// spawns npx; otherwise falls back to the bare `opts.provider.command`
/// and relies on the enriched `PATH`.
///
/// An npx launch built here starts in `opts.npx_launch_root` (else the OS
/// temp dir) rather than the workspace; only [`spawn_provider`] creates the
/// per-spawn [`NpxLaunchDir`] underneath it.
#[must_use]
pub fn build_command(opts: &SpawnOptions) -> Command {
    build_command_with_captured_env(opts, captured_credential_env(), agent_nice())
}

/// The working directory the child starts in: for an npx launch the neutral
/// `npx_launch_dir` (falling back to `npx_launch_root` / the OS temp dir when
/// no per-spawn dir was created), otherwise the workspace `opts.cwd`.
fn process_cwd(opts: &SpawnOptions, npx_launch_dir: Option<&Path>) -> Option<PathBuf> {
    if opts.via_npx() {
        Some(
            npx_launch_dir
                .or(opts.npx_launch_root)
                .map_or_else(std::env::temp_dir, Path::to_path_buf),
        )
    } else {
        opts.cwd.map(Path::to_path_buf)
    }
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
/// cached login-shell capture in production) and nice increment
/// ([`agent_nice`] in production). Captured vars are gap-fill only — see the
/// precedence comment at the merge site below.
fn build_command_with_captured_env(
    opts: &SpawnOptions,
    captured: &BTreeMap<String, String>,
    nice_increment: i32,
) -> Command {
    build_command_in(opts, captured, nice_increment, None)
}

/// [`build_command_with_captured_env`] with the per-spawn [`NpxLaunchDir`]
/// an npx launch starts in (`None` → see [`process_cwd`]).
fn build_command_in(
    opts: &SpawnOptions,
    captured: &BTreeMap<String, String>,
    nice_increment: i32,
    npx_launch_dir: Option<&Path>,
) -> Command {
    let args = build_args(opts);

    // Decide which binary to spawn: provider_binary > npx_fallback (both fields) > provider.command
    let (_, command) = opts.launch_target();

    let mut cmd = Command::new(command);
    cmd.args(&args);
    if let Some(cwd) = process_cwd(opts, npx_launch_dir) {
        cmd.current_dir(cwd);
    }
    // An npx spawn (fallback or npx-only) always runs a Node child, so env
    // assembly applies the V8 heap cap regardless of its declared runtime.
    let via_npx = opts.via_npx();
    let provider_env = build_provider_env_for_spawn(
        opts.provider,
        opts.model,
        opts.rules_file,
        opts.env_mcp_config,
        opts.unsloth_endpoint,
        via_npx,
        opts.node_max_old_space_mb,
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

    // Every Codex launch uses the pinned npx adapter. Enforce both denial
    // settings after every env merge so user/captured overrides cannot enable
    // V2 or select an incompatible Codex executable. The adapter applies this
    // config on each thread start and resume; Intent's MCP tools are unchanged.
    if opts.provider.id == "codex" {
        cmd.env_remove("CODEX_PATH");
        cmd.env("CODEX_CONFIG", CODEX_SUBAGENT_POLICY_CONFIG);
        tracing::debug!(
            mechanism = "CODEX_CONFIG",
            package = intent_providers::CODEX_ACP_NPX_PACKAGE,
            "applied Codex subagent policy for pinned npx adapter"
        );
    }

    // The npx bootstrap targets the neutral launch dir only: an inherited (or
    // merged) npm workspace selector such as `npm_config_workspace` makes npm
    // reject `--workspaces=false` before the adapter starts
    // (intent-hq/intent#5738). Applies after every env merge above so no
    // source — provider env, extra_env, captured login shell, daemon env —
    // can reintroduce one; unrelated npm settings (registry, auth, proxy,
    // cache) pass through untouched.
    if via_npx {
        let explicit = provider_env
            .keys()
            .chain(opts.extra_env.keys())
            .chain(captured.keys())
            .map(String::as_str);
        for key in npm_workspace_selector_env_keys(explicit) {
            cmd.env_remove(key);
        }
    }

    // Enhanced PATH must include the binary's parent dir so dependencies resolve
    // (e.g., when spawning npx, node must be findable)
    cmd.env("PATH", enhanced_path(opts.path_enrichment_binary()));
    cmd.stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    // Put the provider in its own process group (leader pgid == child pid) so
    // reaping/stop can signal the WHOLE tree via `killpg(-pgid)` — `kill_on_drop`
    // only reaches the direct child, leaving grandchildren orphaned (§5.6).
    #[cfg(unix)]
    cmd.process_group(0);
    apply_reduced_priority(&mut cmd, nice_increment);
    cmd
}

/// A spawned provider child paired with its live ACP [`Connection`] and, for
/// an npx launch, the neutral directory it started in.
pub struct SpawnedAgent {
    child: Child,
    connection: Connection,
    npx_launch_dir: Option<NpxLaunchDir>,
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
    ///
    /// # Errors
    ///
    /// Returns the first I/O error from the kill or the wait; a child that already exited is tolerated.
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
            let _ = killpg(Pid::from_raw(pid.cast_signed()), Signal::SIGKILL);
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

    /// Decompose into the child, connection and npx launch dir (e.g. to store
    /// separately). The launch dir must be kept alive as long as the child.
    pub fn into_parts(self) -> (Child, Connection, Option<NpxLaunchDir>) {
        (self.child, self.connection, self.npx_launch_dir)
    }
}

/// Spawn the provider and wire up its [`Connection`] (§6.2 + §6.3).
///
/// # Errors
///
/// Returns [`AcpError::ProviderNotFound`] when the spawn failed with `ENOENT`
/// and the launched program is established to be missing (see
/// [`classify_not_found`]), naming the launch tier that was missing, and
/// [`AcpError::Spawn`] for every other spawn failure, when the neutral npx
/// launch directory cannot be created, or when the stdio pipes cannot be
/// taken.
pub fn spawn_provider(opts: &SpawnOptions, hooks: ConnectionHooks) -> AcpResult<SpawnedAgent> {
    let nice_increment = agent_nice();
    let (launch, target) = opts.launch_target();
    let command_name = target.to_string_lossy().into_owned();
    // An npx launch starts in a fresh neutral directory, never the workspace
    // (intent-hq/intent#5738); the workspace remains the ACP session cwd.
    let npx_launch_dir = if opts.via_npx() {
        Some(NpxLaunchDir::create(opts.npx_launch_root).map_err(|e| {
            AcpError::Spawn(format!(
                "{command_name}: cannot create npx launch directory: {e}"
            ))
        })?)
    } else {
        None
    };
    let launch_cwd = npx_launch_dir.as_ref().map(NpxLaunchDir::path);
    let mut cmd = build_command_in(opts, captured_credential_env(), nice_increment, launch_cwd);
    let process_cwd = process_cwd(opts, launch_cwd);
    let mut child = cmd.spawn().map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            classify_not_found(
                opts,
                launch,
                target,
                &command_name,
                process_cwd.as_deref(),
                &e,
            )
        } else {
            AcpError::Spawn(format!("{command_name}: {e}"))
        }
    })?;
    #[cfg(unix)]
    if let Some(shortfall) = child
        .id()
        .and_then(|pid| reduced_priority_shortfall(pid, nice_increment))
    {
        tracing::warn!(
            command = %command_name,
            pid = child.id(),
            nice_increment,
            "agent child not started at reduced priority: {shortfall}"
        );
    }
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
    Ok(SpawnedAgent {
        child,
        connection,
        npx_launch_dir,
    })
}

/// Attribute a spawn `ENOENT`. The kernel returns `ENOENT` for more than a
/// missing program — a missing `cwd` and a script whose shebang interpreter is
/// absent surface identically — so [`AcpError::ProviderNotFound`] is reserved
/// for the case where the program is established to be missing: a resolved
/// path (or a bare command containing a path separator) that does not exist,
/// or a bare command that no directory of the child's `PATH` (the same
/// [`enhanced_path`] `build_command` sets) contains. Every other `ENOENT`
/// stays an [`AcpError::Spawn`] carrying the original error plus the
/// established fact (missing working directory, or "program exists").
/// `process_cwd` is the directory the child was actually started in (the
/// neutral npx launch dir for an npx launch, else `opts.cwd`).
pub(crate) fn classify_not_found(
    opts: &SpawnOptions,
    launch: LaunchMode,
    target: &std::ffi::OsStr,
    command_name: &str,
    process_cwd: Option<&Path>,
    e: &std::io::Error,
) -> AcpError {
    let child_path = enhanced_path(opts.path_enrichment_binary());
    classify_not_found_with_path(
        launch,
        target,
        command_name,
        process_cwd,
        e,
        child_path.as_ref(),
    )
}

/// [`classify_not_found`] with the child's `PATH` injected (test seam — avoids
/// mutating the process-global `PATH` in parallel tests).
pub(crate) fn classify_not_found_with_path(
    launch: LaunchMode,
    target: &std::ffi::OsStr,
    command_name: &str,
    process_cwd: Option<&Path>,
    e: &std::io::Error,
    child_path: &std::ffi::OsStr,
) -> AcpError {
    if let Some(cwd) = process_cwd.filter(|cwd| !cwd.is_dir()) {
        return AcpError::Spawn(format!(
            "{command_name}: {e} (working directory `{}` does not exist)",
            cwd.display()
        ));
    }
    let program = Path::new(target);
    // The exec happens after the chdir, so a relative program path — and a
    // relative `PATH` entry — resolves against the child's working directory.
    let in_child_cwd = |p: &Path| match process_cwd {
        Some(cwd) if p.is_relative() => cwd.join(p).exists(),
        _ => p.exists(),
    };
    let program_exists = if launch != LaunchMode::BareCommand || program.components().count() > 1 {
        in_child_cwd(program)
    } else {
        std::env::split_paths(child_path).any(|dir| in_child_cwd(&dir.join(program)))
    };
    if program_exists {
        AcpError::Spawn(format!(
            "{command_name}: {e} (the program exists; ENOENT from a missing shebang \
             interpreter or dynamic loader)"
        ))
    } else {
        AcpError::ProviderNotFound {
            command: command_name.to_string(),
            launch,
        }
    }
}

#[cfg(test)]
mod build_args_tests {
    use super::*;

    #[test]
    fn build_args_codex_npx_uses_pinned_package_without_ignored_config_flags() {
        let codex = intent_providers::find_provider("codex").unwrap();
        let npx = Path::new("/usr/local/bin/npx");
        for model in [
            None,
            Some(""),
            Some("default"),
            Some("gpt-5.3-codex"),
            Some("gpt-5.3-codex/high"),
        ] {
            for effort in [None, Some(""), Some("xhigh")] {
                let mut opts = SpawnOptions::new(codex);
                opts.npx_fallback_binary = Some(npx);
                opts.npx_fallback_package = codex.npx_only_package;
                opts.model = model;
                opts.reasoning_effort = effort;
                assert_eq!(
                    build_args(&opts),
                    [
                        NPX_NO_WORKSPACES_ARG,
                        "-y",
                        intent_providers::CODEX_ACP_NPX_PACKAGE
                    ],
                    "model={model:?}, effort={effort:?}"
                );
            }
        }
    }

    #[test]
    fn build_args_keeps_codex_policy_out_of_other_providers() {
        for provider in intent_providers::ACP_PROVIDERS {
            if provider.id == "codex" {
                continue;
            }
            let args = build_args(&SpawnOptions::new(provider));
            assert!(
                !args.iter().any(|arg| arg.contains("agents.enabled")),
                "{} must not receive Codex config: {args:?}",
                provider.id
            );
        }
    }

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
    fn build_args_propagates_tools_to_remove_for_droid() {
        // droid takes a single comma-joined denylist flag.
        let droid = intent_providers::find_provider("droid").unwrap();
        let mut opts = SpawnOptions::new(droid);
        opts.tools_to_remove = vec!["Edit", "Create", "ApplyPatch", "Task"];
        let args = build_args(&opts);
        assert!(
            args.windows(2)
                .any(|w| w == ["--disabled-tools", "Edit,Create,ApplyPatch,Task"]),
            "droid spawn args missing comma-joined denylist: {args:?}"
        );
    }

    #[test]
    fn build_args_omits_remove_tool_flags_for_non_supporting_providers() {
        // claude-code / codex etc. don't advertise a spawn-time tool-removal
        // flag; the spawn layer must not leak an unknown flag to them. grok
        // is in this set: its `--disallowed-tools` flag is headless-only and
        // clap-rejected on `agent stdio`, so the registry leaves it unset.
        for id in [
            "claude-code",
            "codex",
            "cortex",
            "grok",
            "opencode",
            "pi",
            "mock",
        ] {
            let provider = intent_providers::find_provider(id).unwrap();
            let mut opts = SpawnOptions::new(provider);
            opts.tools_to_remove = vec!["str-replace-editor"];
            let args = build_args(&opts);
            assert!(
                !args.iter().any(|a| a == "--remove-tool"
                    || a == "--disallowed-tools"
                    || a == "--disabled-tools"),
                "{id} spawn args unexpectedly include a tool-removal flag: {args:?}"
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

    #[expect(clippy::similar_names)] // pid/pgid are the POSIX terms; preset/present name distinct env sets
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
        let child_pgid = getpgid(Some(Pid::from_raw(child_pid.cast_signed()))).expect("child pgid");
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
        // The exact spawn argv for claude-code: `<npx> --workspaces=false -y
        // <pinned package>` — no other args (claude-code has no base args).
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
                "--workspaces=false".to_string(),
                "-y".to_string(),
                "@agentclientprotocol/claude-agent-acp@0.81.1".to_string(),
            ],
            "bumping the adapter pin is a deliberate change — update this literal with it"
        );
        assert_eq!(intent_providers::CLAUDE_AGENT_ACP_VERSION, "0.81.1");
    }

    #[test]
    fn claude_code_override_binary_spawns_directly_without_npx() {
        // monorepo#4352: a validated `providers.paths["claude-code"]` override
        // arrives as `provider_binary` with the npx fields unset — the argv is
        // the override alone, no `npx -y <pinned>`.
        let provider = intent_providers::find_provider("claude-code").unwrap();
        let mut opts = SpawnOptions::new(provider);
        let adapter = PathBuf::from(
            "/opt/lib/node_modules/@agentclientprotocol/claude-agent-acp/dist/index.js",
        );
        opts.provider_binary = Some(&adapter);
        let cmd = build_command(&opts);
        assert_eq!(cmd.as_std().get_program(), adapter.as_os_str());
        assert!(!opts.via_npx());
        let args = build_args(&opts);
        assert!(
            args.is_empty(),
            "no npx args on the override path: {args:?}"
        );
    }

    #[test]
    fn build_command_prefers_claude_override_over_npx() {
        let provider = intent_providers::find_provider("claude-code").unwrap();
        let mut opts = SpawnOptions::new(provider);
        let adapter = Path::new("/custom/claude-agent-acp");
        opts.provider_binary = Some(adapter);
        opts.npx_fallback_binary = Some(Path::new("/usr/local/bin/npx"));
        opts.npx_fallback_package = provider.npx_only_package;
        let cmd = build_command(&opts);
        assert_eq!(cmd.as_std().get_program(), adapter.as_os_str());
        assert!(!build_args(&opts).contains(&"-y".to_string()));
    }

    /// intent-hq/intent#5738: an npx launch (npx-only provider or npx
    /// fallback) must NOT start in the workspace — npm reads the package
    /// configuration of its cwd, so a Bun/pnpm workspace with `catalog:`
    /// specifiers breaks `npx -y <adapter>` before the adapter starts. The
    /// workspace stays the ACP session cwd (passed separately by the agent
    /// manager), never the npx process cwd.
    #[test]
    fn build_command_does_not_start_npx_launches_in_the_workspace() {
        let workspace = PathBuf::from("/repos/bun workspace");
        let npx_path = PathBuf::from("/usr/local/bin/npx");

        // npx-only provider (claude-code).
        let claude = intent_providers::find_provider("claude-code").unwrap();
        let mut opts = SpawnOptions::new(claude);
        opts.cwd = Some(&workspace);
        opts.npx_fallback_binary = Some(&npx_path);
        opts.npx_fallback_package = claude.npx_only_package;
        assert!(opts.via_npx());
        let cmd = build_command(&opts);
        assert_ne!(
            cmd.as_std().get_current_dir(),
            Some(workspace.as_path()),
            "npx-only launch must not run npx inside the workspace"
        );

        // pinned npx Codex adapter.
        let codex = intent_providers::find_provider("codex").unwrap();
        let mut opts = SpawnOptions::new(codex);
        opts.cwd = Some(&workspace);
        opts.npx_fallback_binary = Some(&npx_path);
        opts.npx_fallback_package = codex.npx_only_package;
        assert!(opts.via_npx());
        let cmd = build_command(&opts);
        assert_ne!(
            cmd.as_std().get_current_dir(),
            Some(workspace.as_path()),
            "npx fallback launch must not run npx inside the workspace"
        );
    }

    /// The counterpart of the test above: resolved binaries (discovered or
    /// `providers.paths` overrides) and bare commands keep the workspace as
    /// their process cwd — only the npx tier is isolated.
    #[test]
    fn build_command_keeps_workspace_cwd_for_resolved_binaries_and_bare_commands() {
        let workspace = PathBuf::from("/repos/bun workspace");
        let npx_path = PathBuf::from("/usr/local/bin/npx");

        let droid = intent_providers::find_provider("droid").unwrap();
        let resolved = PathBuf::from("/custom/droid");
        let mut opts = SpawnOptions::new(droid);
        opts.cwd = Some(&workspace);
        opts.provider_binary = Some(&resolved);
        opts.npx_fallback_binary = Some(&npx_path);
        opts.npx_fallback_package = droid.fallback_npx_package;
        assert!(!opts.via_npx());
        let cmd = build_command(&opts);
        assert_eq!(cmd.as_std().get_current_dir(), Some(workspace.as_path()));

        let claude = intent_providers::find_provider("claude-code").unwrap();
        let adapter = PathBuf::from("/opt/claude-agent-acp/dist/index.js");
        let mut opts = SpawnOptions::new(claude);
        opts.cwd = Some(&workspace);
        opts.provider_binary = Some(&adapter);
        assert!(!opts.via_npx());
        let cmd = build_command(&opts);
        assert_eq!(cmd.as_std().get_current_dir(), Some(workspace.as_path()));

        let auggie = intent_providers::find_provider("auggie").unwrap();
        let mut opts = SpawnOptions::new(auggie);
        opts.cwd = Some(&workspace);
        assert_eq!(opts.launch_target().0, LaunchMode::BareCommand);
        let cmd = build_command(&opts);
        assert_eq!(cmd.as_std().get_current_dir(), Some(workspace.as_path()));
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
    fn codex_npx_only_package_is_pinned() {
        let provider = intent_providers::find_provider("codex").unwrap();
        let pkg = provider
            .npx_only_package
            .expect("codex should have npx_only_package configured");
        assert_eq!(pkg, intent_providers::config::CODEX_ACP_NPX_PACKAGE);
        assert!(
            pkg.starts_with("@agentclientprotocol/codex-acp@"),
            "codex npx adapter should use the @agentclientprotocol package, got: {pkg}"
        );
        let version = pkg.rsplit('@').next().unwrap();
        let parts: Vec<&str> = version.split('.').collect();
        assert!(
            parts.len() == 3 && parts.iter().all(|part| part.parse::<u32>().is_ok()),
            "codex npx adapter must be pinned to an exact semver version, got: {version}"
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
    fn build_command_codex_initial_mode_default_and_extra_env_precedence() {
        let provider = intent_providers::find_provider("codex").unwrap();
        let npx = Path::new("/usr/local/bin/npx");
        let mut opts = SpawnOptions::new(provider);
        opts.npx_fallback_binary = Some(npx);
        opts.npx_fallback_package = provider.npx_only_package;
        let cmd = build_command(&opts);
        let expected = std::env::var_os("INITIAL_AGENT_MODE")
            .is_none()
            .then_some("agent-full-access");
        assert_eq!(env_value(&cmd, "INITIAL_AGENT_MODE").as_deref(), expected);
        assert!(!env_removed(&cmd, "INITIAL_AGENT_MODE"));
        for explicit in [
            "agent",
            "read-only",
            "agent-full-access",
            "",
            "invalid-mode",
        ] {
            opts.extra_env
                .insert("INITIAL_AGENT_MODE".to_string(), explicit.to_string());
            let cmd = build_command(&opts);
            assert_eq!(
                env_value(&cmd, "INITIAL_AGENT_MODE").as_deref(),
                Some(explicit)
            );
        }
    }

    #[test]
    fn build_command_applies_heap_cap_on_codex_npx() {
        // Codex always runs the pinned Node adapter, with the V8 heap cap.
        let provider = intent_providers::find_provider("codex").unwrap();
        let mut opts = SpawnOptions::new(provider);
        let npx_path = PathBuf::from("/usr/local/bin/npx");
        opts.npx_fallback_binary = Some(&npx_path);
        opts.npx_fallback_package = provider.npx_only_package;
        let cmd = build_command(&opts);
        let node_options = env_value(&cmd, "NODE_OPTIONS");
        if std::env::var("NODE_OPTIONS").is_ok_and(|v| v.contains("--max-old-space-size")) {
            // An inherited cap wins: injection is (correctly) skipped.
            assert!(
                node_options.is_none(),
                "inherited --max-old-space-size must suppress injection"
            );
        } else {
            let v = node_options.expect("pinned npx Codex spawn must set NODE_OPTIONS");
            assert!(
                v.contains("--max-old-space-size="),
                "NODE_OPTIONS must carry the heap cap, got: {v}"
            );
        }
    }

    #[test]
    fn build_command_threads_configured_heap_cap_into_node_options() {
        // `agents.acpNodeMaxOldSpaceMb` rides SpawnOptions into the injected
        // NODE_OPTIONS for a Node-runtime provider (intent-hq/intent#4330).
        // Skipped when the ambient env pins the cap itself (env var / inherited
        // NODE_OPTIONS), since both legitimately win over the setting.
        if std::env::var_os("INTENTD_ACP_NODE_MAX_OLD_SPACE_MB").is_some()
            || std::env::var("NODE_OPTIONS").is_ok_and(|v| v.contains("--max-old-space-size"))
        {
            return;
        }
        let provider = intent_providers::find_provider("mock").unwrap();
        let mut opts = SpawnOptions::new(provider);
        opts.node_max_old_space_mb = Some(4096);
        let cmd = build_command(&opts);
        let v = env_value(&cmd, "NODE_OPTIONS").expect("Node provider must set NODE_OPTIONS");
        assert!(
            v.contains("--max-old-space-size=4096"),
            "NODE_OPTIONS must carry the configured cap, got: {v}"
        );

        // Unset setting keeps the built-in default.
        let opts = SpawnOptions::new(provider);
        let cmd = build_command(&opts);
        let v = env_value(&cmd, "NODE_OPTIONS").expect("Node provider must set NODE_OPTIONS");
        assert!(
            v.contains("--max-old-space-size=8192"),
            "unset setting must fall back to the 8192 default, got: {v}"
        );
    }

    #[test]
    fn build_command_no_heap_cap_on_native_droid() {
        let provider = intent_providers::find_provider("droid").unwrap();
        let mut opts = SpawnOptions::new(provider);
        opts.provider_binary = Some(Path::new("/custom/droid"));
        assert!(env_value(&build_command(&opts), "NODE_OPTIONS").is_none());
    }

    #[test]
    fn build_command_sets_codex_subagent_policy_on_npx_spawn() {
        // The pinned npx adapter is daemon-managed: a stray CODEX_PATH /
        // CODEX_CONFIG in the daemon env must not redirect the adapter (#555).
        let provider = intent_providers::find_provider("codex").unwrap();
        let mut opts = SpawnOptions::new(provider);
        let npx_path = PathBuf::from("/usr/local/bin/npx");
        opts.npx_fallback_binary = Some(&npx_path);
        opts.npx_fallback_package = provider.npx_only_package;
        let cmd = build_command(&opts);
        assert!(
            env_removed(&cmd, "CODEX_PATH"),
            "pinned npx Codex spawn must remove CODEX_PATH from the child env"
        );
        let config = env_value(&cmd, "CODEX_CONFIG")
            .expect("pinned npx Codex spawn must set daemon-owned CODEX_CONFIG");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&config).unwrap(),
            serde_json::json!({"agents": {"enabled": false}, "features": {"multi_agent_v2": false}})
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

    /// npm's env-config normalisation (`@npmcli/config` `loadEnv`): the
    /// `npm_config_` prefix is case-insensitive and `_` folds to `-`, so every
    /// spelling of the workspace selectors is recognised while the registry /
    /// auth / proxy / cache settings and non-npm keys are not.
    #[test]
    fn npm_workspace_selector_env_recognises_npm_case_and_separator_variants() {
        for key in [
            "npm_config_workspace",
            "NPM_CONFIG_WORKSPACE",
            "Npm_Config_Workspace",
            "npm_config_workspaces",
            "npm_config_include_workspace_root",
            "npm_config_include-workspace-root",
            "NPM_CONFIG_INCLUDE_WORKSPACE_ROOT",
        ] {
            assert!(is_npm_workspace_selector_env(key), "{key}");
        }
        for key in [
            "npm_config_registry",
            "npm_config_userconfig",
            "npm_config_cache",
            "npm_config_offline",
            "npm_config_proxy",
            "npm_config_//registry.npmjs.org/:_authToken",
            "npm_config_workspace_root",
            "npm_config_",
            "npm_config",
            "workspace",
            "NPM_WORKSPACE",
            "",
        ] {
            assert!(!is_npm_workspace_selector_env(key), "{key}");
        }
        let keys = npm_workspace_selector_env_keys(["NPM_CONFIG_WORKSPACE", "npm_config_registry"]);
        assert!(keys.contains("NPM_CONFIG_WORKSPACE"));
        assert!(!keys.contains("npm_config_registry"));
    }

    /// intent-hq/intent#5738: a workspace selector reaching the npx bootstrap
    /// from any env source — here the explicit `extra_env` merged last — is
    /// removed from the child env on both npx tiers (npx-only claude-code,
    /// codex), while unrelated npm settings merged alongside it stay.
    #[test]
    fn build_command_strips_npm_workspace_selectors_on_npx_spawns() {
        let npx_path = PathBuf::from("/usr/local/bin/npx");
        let claude = intent_providers::find_provider("claude-code").unwrap();
        let codex = intent_providers::find_provider("codex").unwrap();
        for (provider, package) in [
            (claude, claude.npx_only_package),
            (codex, codex.npx_only_package),
        ] {
            let mut opts = SpawnOptions::new(provider);
            opts.npx_fallback_binary = Some(&npx_path);
            opts.npx_fallback_package = package;
            for (k, v) in [
                ("npm_config_workspace", "some-workspace"),
                ("NPM_CONFIG_WORKSPACE", "some-workspace"),
                ("npm_config_include_workspace_root", "true"),
                ("npm_config_registry", "https://registry.example.test/"),
                ("npm_config_proxy", "http://proxy.example.test:3128"),
            ] {
                opts.extra_env.insert(k.to_string(), v.to_string());
            }
            assert!(opts.via_npx());
            let cmd = build_command(&opts);
            for key in [
                "npm_config_workspace",
                "NPM_CONFIG_WORKSPACE",
                "npm_config_include_workspace_root",
            ] {
                assert!(
                    env_removed(&cmd, key),
                    "{}: {key} must be env_remove'd from the npx spawn",
                    provider.id
                );
            }
            assert_eq!(
                env_value(&cmd, "npm_config_registry").as_deref(),
                Some("https://registry.example.test/"),
                "{}: unrelated npm settings must survive",
                provider.id
            );
            assert_eq!(
                env_value(&cmd, "npm_config_proxy").as_deref(),
                Some("http://proxy.example.test:3128"),
                "{}: unrelated npm settings must survive",
                provider.id
            );
        }
    }

    /// Control: a resolved binary or bare command is not an npx bootstrap, so
    /// an explicit `npm_config_workspace` reaches it unchanged.
    #[test]
    fn build_command_keeps_npm_workspace_selectors_for_non_npx_spawns() {
        let droid = intent_providers::find_provider("droid").unwrap();
        let provider_binary = PathBuf::from("/custom/droid");
        let npx_path = PathBuf::from("/usr/local/bin/npx");
        let mut resolved = SpawnOptions::new(droid);
        resolved.provider_binary = Some(&provider_binary);
        resolved.npx_fallback_binary = Some(&npx_path);
        resolved.npx_fallback_package = droid.fallback_npx_package;
        let auggie = intent_providers::find_provider("auggie").unwrap();
        let bare = SpawnOptions::new(auggie);
        for mut opts in [resolved, bare] {
            opts.extra_env.insert(
                "npm_config_workspace".to_string(),
                "some-workspace".to_string(),
            );
            assert!(!opts.via_npx());
            let cmd = build_command(&opts);
            assert_eq!(
                env_value(&cmd, "npm_config_workspace").as_deref(),
                Some("some-workspace"),
                "{}: non-npx spawns keep the selector",
                opts.provider.id
            );
            assert!(!env_removed(&cmd, "npm_config_workspace"));
        }
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
        let cmd = build_command_with_captured_env(&opts, &captured, DEFAULT_AGENT_NICE);
        assert_eq!(env_value(&cmd, &name).as_deref(), Some("captured-value"));
    }

    #[expect(clippy::similar_names)] // pid/pgid are the POSIX terms; preset/present name distinct env sets
    #[test]
    fn captured_var_never_overrides_daemon_process_env() {
        let provider = intent_providers::find_provider("auggie").unwrap();
        let opts = SpawnOptions::new(provider);
        // Pick a var actually present in the daemon (test process) env that
        // the command does not already set explicitly (provider env / PATH).
        // Restricted to stable well-known names: scanning all of
        // `std::env::vars()` can race sibling tests that mutate process env
        // (e.g. session.rs's INTENTD_PROMPT_IDLE_TIMEOUT_MS guard).
        let baseline = build_command_with_captured_env(&opts, &BTreeMap::new(), DEFAULT_AGENT_NICE);
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
        let cmd = build_command_with_captured_env(&opts, &captured, DEFAULT_AGENT_NICE);
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
        let cmd = build_command_with_captured_env(&opts, &captured, DEFAULT_AGENT_NICE);
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
        let cmd = build_command_with_captured_env(&opts, &captured, DEFAULT_AGENT_NICE);
        assert_eq!(
            env_value(&cmd, "ANTHROPIC_API_KEY").as_deref(),
            Some("from-extra")
        );
    }

    #[test]
    fn codex_subagent_policy_overrides_captured_and_extra_env_on_npx() {
        // The daemon policy runs after every env merge, replacing arbitrary
        // CODEX_CONFIG and keeping CODEX_PATH removed (#555).
        let provider = intent_providers::find_provider("codex").unwrap();
        let mut opts = SpawnOptions::new(provider);
        let npx_path = PathBuf::from("/usr/local/bin/npx");
        opts.npx_fallback_binary = Some(&npx_path);
        opts.npx_fallback_package = provider.npx_only_package;
        let unrelated = absent_var_name();
        for source in ["captured", "extra", "both"] {
            let mut captured = BTreeMap::new();
            opts.extra_env.clear();
            for (key, value) in [
                ("CODEX_PATH", "/untrusted/codex"),
                (
                    "CODEX_CONFIG",
                    r#"{"agents":{"enabled":true},"features":{"multi_agent_v2":true},"model":"untrusted"}"#,
                ),
                (unrelated.as_str(), "preserved"),
            ] {
                if source != "extra" {
                    captured.insert(key.to_string(), value.to_string());
                }
                if source != "captured" {
                    opts.extra_env.insert(key.to_string(), value.to_string());
                }
            }
            let cmd = build_command_with_captured_env(&opts, &captured, DEFAULT_AGENT_NICE);
            assert!(
                cmd.as_std()
                    .get_envs()
                    .any(|(k, v)| k == "CODEX_PATH" && v.is_none()),
                "CODEX_PATH must stay removed for {source} env"
            );
            let config = env_value(&cmd, "CODEX_CONFIG")
                .expect("pinned adapter must set the daemon-owned subagent policy");
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(&config).unwrap(),
                serde_json::json!({"agents": {"enabled": false}, "features": {"multi_agent_v2": false}}),
                "policy must win over {source} env"
            );
            assert_eq!(env_value(&cmd, &unrelated).as_deref(), Some("preserved"));
        }
    }

    #[test]
    fn codex_policy_preserves_env_precedence_for_other_providers() {
        let binary = PathBuf::from("/custom/provider");
        let npx_path = PathBuf::from("/usr/local/bin/npx");
        for id in ["claude-code", "auggie", "opencode", "droid"] {
            let provider = intent_providers::find_provider(id).unwrap();
            let mut opts = SpawnOptions::new(provider);
            if id == "claude-code" {
                opts.npx_fallback_binary = Some(&npx_path);
                opts.npx_fallback_package = provider.npx_only_package;
            } else {
                opts.provider_binary = Some(&binary);
            }
            let captured = BTreeMap::from([
                ("CODEX_PATH".to_string(), "/captured/codex".to_string()),
                ("CODEX_CONFIG".to_string(), "captured-config".to_string()),
            ]);
            let cmd = build_command_with_captured_env(&opts, &captured, DEFAULT_AGENT_NICE);
            for (key, value) in &captured {
                if std::env::var_os(key).is_some() {
                    assert!(
                        !cmd.as_std().get_envs().any(|(k, _)| k == key.as_str()),
                        "{id} must inherit {key} when present in the daemon env"
                    );
                } else {
                    assert_eq!(env_value(&cmd, key).as_deref(), Some(value.as_str()));
                }
            }
            opts.extra_env = BTreeMap::from([
                ("CODEX_PATH".to_string(), "/extra/codex".to_string()),
                ("CODEX_CONFIG".to_string(), "extra-config".to_string()),
            ]);
            let cmd = build_command_with_captured_env(&opts, &captured, DEFAULT_AGENT_NICE);
            for (key, value) in &opts.extra_env {
                assert_eq!(env_value(&cmd, key).as_deref(), Some(value.as_str()));
            }
        }
    }
}

#[cfg(test)]
mod agent_nice_tests {
    use super::*;

    #[test]
    fn agent_nice_defaults_when_unset_or_blank() {
        assert_eq!(agent_nice_from(None), DEFAULT_AGENT_NICE);
        assert_eq!(agent_nice_from(Some("")), DEFAULT_AGENT_NICE);
        assert_eq!(agent_nice_from(Some("  ")), DEFAULT_AGENT_NICE);
    }

    #[test]
    fn agent_nice_zero_disables() {
        assert_eq!(agent_nice_from(Some("0")), 0);
    }

    #[test]
    fn agent_nice_honours_and_clamps_values() {
        assert_eq!(agent_nice_from(Some("7")), 7);
        assert_eq!(agent_nice_from(Some(" 12 ")), 12);
        assert_eq!(agent_nice_from(Some("19")), 19);
        assert_eq!(agent_nice_from(Some("40")), 19);
        assert_eq!(agent_nice_from(Some("-3")), 0);
        assert_eq!(agent_nice_from(Some("99999999999999")), 19);
    }

    #[test]
    fn agent_nice_unparseable_falls_back_to_default() {
        assert_eq!(agent_nice_from(Some("high")), DEFAULT_AGENT_NICE);
        assert_eq!(agent_nice_from(Some("5.5")), DEFAULT_AGENT_NICE);
    }
}

#[cfg(all(test, unix))]
mod reduced_priority_tests {
    use super::*;

    /// A provider whose child is `sh -c <script>` with no ACP flags (the
    /// args slice is leaked to satisfy the registry's `'static` lifetime).
    fn sh_provider(script: &'static str) -> intent_providers::ProviderConfig {
        let base = *intent_providers::find_provider("auggie").unwrap();
        intent_providers::ProviderConfig {
            command: "sh",
            base_args: Box::leak(vec!["-c", script].into_boxed_slice()),
            model_flag: None,
            rules_flag: None,
            mcp_config_flag: None,
            quiet_flag: None,
            supports_mcp_config: false,
            supports_rules_file: false,
            ..base
        }
    }

    /// The nice value of process `pid` (`0` = this test process), read with
    /// `getpriority` so the check needs no platform-specific `nice(1)`
    /// (macOS's prints nothing and exits 1 without a utility argument).
    fn nice_of(pid: u32) -> i32 {
        super::nice_of(pid as libc::id_t).unwrap_or_else(|errno| {
            panic!(
                "getpriority({pid}) failed: {}",
                std::io::Error::from_raw_os_error(errno)
            )
        })
    }

    /// This test process's own nice value.
    fn own_nice() -> i32 {
        nice_of(0)
    }

    /// Nice value a live child built with `increment` is running at, read
    /// from the parent. `spawn` returns after the exec, so the `pre_exec`
    /// `setpriority` has already taken effect.
    fn child_nice(increment: i32) -> i32 {
        let provider = sh_provider("exec sleep 30");
        let opts = SpawnOptions::new(&provider);
        let mut cmd = build_command_with_captured_env(&opts, &BTreeMap::new(), increment);
        let child = cmd.spawn().expect("spawn sleeper");
        let got = nice_of(child.id().expect("child pid"));
        drop(child);
        got
    }

    #[test]
    fn target_nice_is_parent_plus_increment_capped() {
        assert_eq!(target_nice(0, DEFAULT_AGENT_NICE), DEFAULT_AGENT_NICE);
        assert_eq!(target_nice(10, DEFAULT_AGENT_NICE), 15);
        assert_eq!(target_nice(-10, DEFAULT_AGENT_NICE), -5);
        assert_eq!(target_nice(15, DEFAULT_AGENT_NICE), MAX_NICE);
        assert_eq!(target_nice(19, DEFAULT_AGENT_NICE), MAX_NICE);
        assert_eq!(target_nice(3, 19), MAX_NICE);
        assert_eq!(target_nice(7, i32::MAX), MAX_NICE);
        // Never below the parent.
        assert_eq!(target_nice(7, 0), 7);
        assert_eq!(target_nice(7, -4), 7);
        // A parent past the portable cap (macOS PRIO_MAX is 20) stays put
        // rather than being pulled down to the cap.
        assert_eq!(target_nice(20, DEFAULT_AGENT_NICE), 20);
        assert_eq!(target_nice(20, 0), 20);
        assert_eq!(target_nice(i32::MAX, DEFAULT_AGENT_NICE), i32::MAX);
    }

    #[tokio::test]
    async fn child_starts_at_default_increment_over_parent() {
        let expected = target_nice(own_nice(), DEFAULT_AGENT_NICE);
        assert_eq!(child_nice(DEFAULT_AGENT_NICE), expected);
    }

    #[tokio::test]
    async fn child_starts_at_configured_increment_over_parent() {
        let expected = target_nice(own_nice(), 12);
        assert_eq!(child_nice(12), expected);
    }

    #[tokio::test]
    async fn nice_zero_leaves_child_at_daemon_priority() {
        assert_eq!(child_nice(0), own_nice());
    }

    /// Nice value [`child_of_niced_parent_is_niced_relative_to_it`] re-runs
    /// this test binary under, so the builder runs from a parent already
    /// niced above [`DEFAULT_AGENT_NICE`].
    const NICED_PARENT: i32 = 10;

    /// Driven only via [`child_of_niced_parent_is_niced_relative_to_it`]
    /// (hence `#[ignore]`): asserts this process is at nice ≥
    /// [`NICED_PARENT`] and its children land at `parent + increment`,
    /// capped at [`MAX_NICE`] — never back down at the absolute increment.
    #[tokio::test]
    #[ignore = "re-executed under nice(1) by child_of_niced_parent_is_niced_relative_to_it"]
    async fn niced_parent_inner() {
        let own = own_nice();
        assert!(
            own >= NICED_PARENT.min(MAX_NICE),
            "expected to run at nice >= {NICED_PARENT}, got {own}"
        );
        assert_eq!(
            child_nice(DEFAULT_AGENT_NICE),
            target_nice(own, DEFAULT_AGENT_NICE)
        );
        assert!(child_nice(DEFAULT_AGENT_NICE) > DEFAULT_AGENT_NICE);
        assert_eq!(child_nice(12), target_nice(own, 12));
        assert_eq!(child_nice(0), own);
    }

    #[test]
    fn child_of_niced_parent_is_niced_relative_to_it() {
        let exe = std::env::current_exe().expect("test binary path");
        // libtest names omit the crate segment of `module_path!()`.
        let (_, module) = module_path!().split_once("::").expect("crate::module path");
        let out = std::process::Command::new("nice")
            .arg("-n")
            .arg(NICED_PARENT.to_string())
            .arg(&exe)
            .args([
                "--exact",
                &format!("{module}::niced_parent_inner"),
                "--ignored",
                "--nocapture",
            ])
            .output()
            .expect("re-exec the test binary under nice(1)");
        assert!(
            out.status.success(),
            "niced re-run failed ({}):\nstdout:\n{}\nstderr:\n{}",
            out.status,
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        assert!(
            String::from_utf8_lossy(&out.stdout).contains("test result: ok. 1 passed"),
            "niced re-run did not run the inner test:\n{}",
            String::from_utf8_lossy(&out.stdout)
        );
    }

    #[tokio::test]
    async fn reduced_priority_shortfall_reports_only_when_child_is_not_niced() {
        let provider = sh_provider("exec sleep 30");
        let opts = SpawnOptions::new(&provider);
        let mut cmd = build_command_with_captured_env(&opts, &BTreeMap::new(), DEFAULT_AGENT_NICE);
        let child = cmd.spawn().expect("spawn sleeper");
        let pid = child.id().expect("child pid");

        assert_eq!(reduced_priority_shortfall(pid, 0), None);
        assert_eq!(reduced_priority_shortfall(pid, DEFAULT_AGENT_NICE), None);
        let own = own_nice();
        if target_nice(own, DEFAULT_AGENT_NICE) < target_nice(own, 19) {
            let shortfall =
                reduced_priority_shortfall(pid, 19).expect("child is below the +19 target");
            assert!(
                shortfall.contains(&format!(
                    "expected at least {} (daemon at {own})",
                    target_nice(own, 19)
                )),
                "{shortfall}"
            );
        }
        drop(child);
    }

    #[test]
    fn reduced_priority_shortfall_is_silent_for_a_gone_child() {
        // pid_max on Linux is at most 2^22; this pid cannot exist.
        assert_eq!(
            reduced_priority_shortfall(0x7fff_fff0, DEFAULT_AGENT_NICE),
            None
        );
    }
}
