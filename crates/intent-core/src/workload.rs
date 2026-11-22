//! Workload memory containment (intent-hq/intent#9809).
//!
//! Every process that runs user, agent or model code — agent CLIs, adapter
//! chains, MCP servers, `ws.script` runs, terminals, `host.exec` — is a
//! *workload*. On Linux with a usable cgroup v2 hierarchy the daemon builds
//!
//! ```text
//! <domain>/            the daemon's own cgroup (or `intentd-<key>` under root)
//! ├── supervisor/      the sitter that spawned the daemon, when it shared the cgroup
//! ├── daemon/          the daemon and its internal helpers (git, probes)
//! └── workloads/       memory.max = ceiling, no memory.high, no swap
//!     └── <kind>-<pid>-<seq>/   one leaf per workload
//! ```
//!
//! so a runaway workload is OOM-killed inside `workloads/` while the daemon
//! keeps its memory. Without a usable cgroup (not delegated, shared with
//! other processes, read-only) the daemon falls back to `oom_score_adj` on
//! workload roots, so the kernel still prefers them over the daemon. macOS
//! and Windows report [`ContainmentMode::None`].
//!
//! [`Workload`] is the one spawn helper. A `std` / `tokio` command joins its
//! leaf in a `pre_exec` hook (before the program runs, so nothing it forks can
//! escape, and a missing program still fails the spawn). A PTY command, which
//! has no `pre_exec`, runs through the `intentd cgroup-exec` shim
//! ([`dispatch_shim`]), which joins the leaf, sets the score and `exec`s.

#[cfg(not(target_os = "linux"))]
use std::path::Path;
use std::path::PathBuf;
use std::sync::OnceLock;

use serde::Serialize;

#[cfg(target_os = "linux")]
mod cgroup;
#[cfg(target_os = "linux")]
mod leaf;
#[cfg(all(test, target_os = "linux"))]
mod tests;

#[cfg(target_os = "linux")]
pub use leaf::Leaf;

/// Placeholder leaf handle on platforms without cgroups; never constructed.
#[cfg(not(target_os = "linux"))]
#[derive(Clone, Debug)]
pub struct Leaf(std::convert::Infallible);

#[cfg(not(target_os = "linux"))]
impl Leaf {
    /// See the Linux implementation.
    #[must_use]
    pub fn path(&self) -> &Path {
        match self.0 {}
    }
    /// See the Linux implementation.
    #[must_use]
    pub fn oom_kills(&self) -> u64 {
        match self.0 {}
    }
    /// See the Linux implementation.
    pub fn kill(&self) {
        match self.0 {}
    }
    /// See the Linux implementation.
    pub fn pin(&self) {
        match self.0 {}
    }
}

/// The hidden subcommand the PTY shim runs as.
pub const SHIM_SUBCOMMAND: &str = "cgroup-exec";

/// What a workload is; sets its leaf name and its `oom_score_adj`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkloadKind {
    /// An agent provider CLI.
    Agent,
    /// A short-lived ACP adapter chain (model probes, quick actions).
    Adapter,
    /// An MCP server.
    Mcp,
    /// A local model server (Unsloth).
    ModelServer,
    /// A `ws.script` run.
    Script,
    /// An interactive terminal.
    Terminal,
    /// A `host.exec` call.
    HostExec,
    /// A repository's own archive script.
    Archive,
}

/// `oom_score_adj` of agent-type workload roots.
pub const AGENT_OOM_SCORE_ADJ: i32 = 500;
/// `oom_score_adj` of scripts, terminals and `host.exec`: above agents, so
/// services and test runs die before the agent CLIs that drive them.
pub const TASK_OOM_SCORE_ADJ: i32 = 800;

impl WorkloadKind {
    fn prefix(self) -> &'static str {
        match self {
            Self::Agent => "agent",
            Self::Adapter => "adapter",
            Self::Mcp => "mcp",
            Self::ModelServer => "model",
            Self::Script => "script",
            Self::Terminal => "term",
            Self::HostExec => "exec",
            Self::Archive => "archive",
        }
    }

    /// The `oom_score_adj` the workload root starts with.
    #[must_use]
    pub fn oom_score_adj(self) -> i32 {
        match self {
            Self::Agent | Self::Adapter | Self::Mcp | Self::ModelServer => AGENT_OOM_SCORE_ADJ,
            Self::Script | Self::Terminal | Self::HostExec | Self::Archive => TASK_OOM_SCORE_ADJ,
        }
    }

    /// Whether an OOM kill in this workload is reported by a daemon log line
    /// rather than on a run result the daemon owns.
    #[must_use]
    pub fn is_agent_like(self) -> bool {
        self.oom_score_adj() == AGENT_OOM_SCORE_ADJ
    }
}

const MIB: u64 = 1024 * 1024;
/// Smallest daemon reserve. Measured daemon RSS was 438 MB on a busy host
/// (many workspaces, 16 h uptime); 768 MiB leaves room for the OS and the
/// daemon's page cache on a 4 GB machine.
pub const MIN_RESERVE_BYTES: u64 = 768 * MIB;
/// Largest daemon reserve.
pub const MAX_RESERVE_BYTES: u64 = 2048 * MIB;
/// Smallest ceiling ever written, so a tiny or misread box still runs work.
const MIN_CEILING_BYTES: u64 = 256 * MIB;

/// The memory kept outside the workloads box for the daemon and the OS:
/// `clamp(15 % of the box, 768 MiB, 2 GiB)`.
#[must_use]
pub fn reserve_bytes(box_total: u64) -> u64 {
    (box_total / 100 * 15).clamp(MIN_RESERVE_BYTES, MAX_RESERVE_BYTES)
}

/// The auto workloads ceiling for a box of `box_total` bytes (at least
/// 256 MiB).
#[must_use]
pub fn auto_ceiling_bytes(box_total: u64) -> u64 {
    box_total
        .saturating_sub(reserve_bytes(box_total))
        .max(MIN_CEILING_BYTES)
}

/// How workloads are contained on this host (`system.status`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum ContainmentMode {
    /// Workloads run in a memory-limited cgroup.
    Cgroup,
    /// Workload roots only get a raised `oom_score_adj`.
    OomScoreAdj,
    /// No containment (macOS, Windows, or not initialised).
    None,
}

/// The `agents.workloadMemoryMaxMb` setting, resolved.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Ceiling {
    /// Absent key: total memory less the daemon reserve.
    Auto,
    /// `0`: no cgroup containment (the `oom_score_adj` fallback still applies).
    Off,
    /// An explicit ceiling in bytes.
    Bytes(u64),
}

/// Startup inputs for [`init`].
#[derive(Clone, Debug)]
pub struct ContainmentConfig {
    /// The resolved ceiling setting.
    pub ceiling: Ceiling,
    /// Stable per-daemon key (the data dir) naming the domain cgroup when the
    /// daemon starts in the root cgroup.
    pub instance_key: String,
}

/// A point-in-time containment report (`system.status.workloadContainment`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ContainmentStatus {
    /// The active mode.
    pub mode: ContainmentMode,
    /// Why cgroup containment is not active (absent in cgroup mode).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// The workloads cgroup's `memory.max`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memory_max_bytes: Option<u64>,
    /// The workloads cgroup's `memory.current`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memory_current_bytes: Option<u64>,
    /// OOM kills inside the workloads cgroup since it was created.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub oom_kills: Option<u64>,
}

impl ContainmentStatus {
    fn without(mode: ContainmentMode, reason: impl Into<String>) -> Self {
        Self {
            mode,
            reason: Some(reason.into()),
            memory_max_bytes: None,
            memory_current_bytes: None,
            oom_kills: None,
        }
    }
}

/// The daemon-side state [`init`] installs.
#[derive(Debug)]
struct State {
    mode: ContainmentMode,
    reason: Option<String>,
    /// `workloads/` in cgroup mode.
    workloads: Option<PathBuf>,
    memory_max_bytes: Option<u64>,
}

static STATE: OnceLock<State> = OnceLock::new();

/// Set up containment for this daemon process. Call once, before the first
/// workload spawn; later calls return the first result. Never fails: every
/// problem degrades to a weaker mode with a reason.
pub fn init(config: &ContainmentConfig) -> ContainmentStatus {
    STATE.get_or_init(|| init_state(config));
    status()
}

#[cfg(target_os = "linux")]
fn init_state(config: &ContainmentConfig) -> State {
    let fallback = |reason: String| State {
        mode: ContainmentMode::OomScoreAdj,
        reason: Some(reason),
        workloads: None,
        memory_max_bytes: None,
    };
    if config.ceiling == Ceiling::Off {
        return fallback("turned off by agents.workloadMemoryMaxMb = 0".into());
    }
    match cgroup::Host::current().and_then(|host| cgroup::setup(&host, config)) {
        Ok(layout) => {
            leaf::sweep(&layout.workloads);
            State {
                mode: ContainmentMode::Cgroup,
                reason: None,
                workloads: Some(layout.workloads),
                memory_max_bytes: Some(layout.memory_max_bytes),
            }
        }
        Err(reason) => fallback(reason),
    }
}

#[cfg(not(target_os = "linux"))]
fn init_state(_config: &ContainmentConfig) -> State {
    State {
        mode: ContainmentMode::None,
        reason: Some("memory containment needs Linux cgroup v2".into()),
        workloads: None,
        memory_max_bytes: None,
    }
}

/// The current containment report. Before [`init`] (tests, CLI commands) it
/// is [`ContainmentMode::None`].
#[must_use]
pub fn status() -> ContainmentStatus {
    let Some(state) = STATE.get() else {
        return ContainmentStatus::without(ContainmentMode::None, "not initialised");
    };
    let mut status = ContainmentStatus {
        mode: state.mode,
        reason: state.reason.clone(),
        memory_max_bytes: state.memory_max_bytes,
        memory_current_bytes: None,
        oom_kills: None,
    };
    #[cfg(target_os = "linux")]
    if let Some(workloads) = &state.workloads {
        status.memory_current_bytes = cgroup::read_u64(&workloads.join("memory.current"));
        status.oom_kills = cgroup::oom_kills(workloads);
    }
    status
}

/// The active mode ([`ContainmentMode::None`] before [`init`]).
#[must_use]
pub fn mode() -> ContainmentMode {
    STATE.get().map_or(ContainmentMode::None, |s| s.mode)
}

/// One workload about to be spawned.
#[derive(Clone, Debug)]
pub struct Workload {
    kind: WorkloadKind,
    label: String,
}

/// The program and argv a PTY spawn should use for a [`Workload`].
#[derive(Debug)]
pub struct PtyLaunch {
    /// Program to execute (the shim, or the original program).
    pub program: String,
    /// Arguments after the program.
    pub args: Vec<String>,
    /// The workload's leaf in cgroup mode.
    pub leaf: Option<Leaf>,
}

impl Workload {
    /// A workload of `kind`; `label` names it in logs (e.g. `agent <id>`).
    #[must_use]
    pub fn new(kind: WorkloadKind, label: impl Into<String>) -> Self {
        Self {
            kind,
            label: label.into(),
        }
    }

    /// The workload's kind.
    #[must_use]
    pub fn kind(&self) -> WorkloadKind {
        self.kind
    }

    /// Configure `cmd` so its child joins this workload's leaf and takes its
    /// `oom_score_adj` before it runs any code. Keep the returned leaf for as
    /// long as the workload's result matters (memory-limit reporting, stop);
    /// dropping it early is harmless. Never makes the spawn fail.
    #[cfg_attr(not(target_os = "linux"), expect(clippy::unused_self))]
    pub fn apply(&self, cmd: &mut std::process::Command) -> Option<Leaf> {
        #[cfg(target_os = "linux")]
        {
            if mode() == ContainmentMode::None {
                return None;
            }
            let leaf = self.create_leaf();
            leaf::install_pre_exec(cmd, leaf.as_ref(), self.kind.oom_score_adj());
            leaf
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = cmd;
            None
        }
    }

    /// [`Self::apply`] for a `tokio` command.
    pub fn apply_tokio(&self, cmd: &mut tokio::process::Command) -> Option<Leaf> {
        self.apply(cmd.as_std_mut())
    }

    /// [`Self::apply_tokio`] for a site that does not keep the handle: the
    /// leaf stays registered (OOM-kill reporting, sweeping) until its
    /// processes have exited.
    pub fn apply_pinned(&self, cmd: &mut tokio::process::Command) {
        if let Some(leaf) = self.apply_tokio(cmd) {
            leaf.pin();
        }
    }

    /// The program and argv for a PTY spawn of `program args…`: the
    /// `cgroup-exec` shim around it on Linux, unchanged elsewhere.
    #[must_use]
    pub fn pty_launch(&self, program: &str, args: &[String]) -> PtyLaunch {
        let passthrough = || PtyLaunch {
            program: program.to_owned(),
            args: args.to_vec(),
            leaf: None,
        };
        #[cfg(target_os = "linux")]
        {
            if mode() == ContainmentMode::None {
                return passthrough();
            }
            let leaf = self.create_leaf();
            let mut shim = vec![SHIM_SUBCOMMAND.to_owned()];
            if let Some(leaf) = &leaf {
                shim.push("--leaf".into());
                shim.push(leaf.path().to_string_lossy().into_owned());
            }
            shim.push("--oom-score-adj".into());
            shim.push(self.kind.oom_score_adj().to_string());
            shim.push("--".into());
            shim.push(program.to_owned());
            shim.extend(args.iter().cloned());
            PtyLaunch {
                program: "/proc/self/exe".into(),
                args: shim,
                leaf,
            }
        }
        #[cfg(not(target_os = "linux"))]
        passthrough()
    }

    #[cfg(target_os = "linux")]
    fn create_leaf(&self) -> Option<Leaf> {
        let workloads = STATE.get()?.workloads.as_deref()?;
        Leaf::create(workloads, self.kind, &self.label)
    }
}

/// An agent-type workload that lost a process to the memory limit since the
/// last [`poll_oom_kills`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OomKillReport {
    /// The workload's kind.
    pub kind: WorkloadKind,
    /// The workload's label.
    pub label: String,
    /// New OOM kills since the previous poll.
    pub new_kills: u64,
}

/// Collect new OOM kills in live leaves and remove empty leaves nobody holds.
/// Called by the daemon's memory sampler.
#[must_use]
pub fn poll_oom_kills() -> Vec<OomKillReport> {
    #[cfg(target_os = "linux")]
    {
        let Some(workloads) = STATE.get().and_then(|s| s.workloads.as_deref()) else {
            return Vec::new();
        };
        leaf::sweep(workloads);
        leaf::poll_oom_kills()
    }
    #[cfg(not(target_os = "linux"))]
    Vec::new()
}

/// Run the `cgroup-exec` shim when this process was started as one
/// (`<exe> cgroup-exec [--leaf DIR] [--oom-score-adj N] -- program args…`).
/// Returns `None` for any other invocation; otherwise it only returns when
/// the `exec` failed.
#[must_use]
pub fn dispatch_shim() -> Option<std::process::ExitCode> {
    let mut args = std::env::args_os().skip(1);
    if args.next()? != SHIM_SUBCOMMAND {
        return None;
    }
    #[cfg(target_os = "linux")]
    {
        Some(leaf::run_shim(args.collect()))
    }
    #[cfg(not(target_os = "linux"))]
    {
        eprintln!("intentd {SHIM_SUBCOMMAND}: only supported on Linux");
        Some(std::process::ExitCode::from(127))
    }
}
