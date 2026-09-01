//! Daemon configuration and path resolution (§11.2).
//!
//! Paths are resolved via the `directories` crate, honoring the
//! `INTENTD_DATA_DIR` and `INTENTD_CONFIG` environment overrides. The data dir
//! holds the `SQLite` database (`intentd.db`), the UDS (`intentd.sock`), and the
//! non-secret settings file (`config.toml`), which is loaded strictly through
//! [`crate::settings_file::SettingsFile`] — a malformed file fails `resolve()`
//! instead of being silently ignored.

use std::path::PathBuf;

use crate::error::{Error, Result};
use crate::settings_file::SettingsFile;

/// Default idle-reap TTL in minutes (`agents.idleReapMinutes`, §11.1); `0`
/// disables the sweep entirely. Lowered from 30 to 10 (monorepo#2109): idle
/// reaping is the main lever on resident memory — every agent touched inside
/// the window holds its whole subtree alive (~0.66 GB idle each) — and a
/// 30-minute window kept a day's worth of touched agents resident at once.
pub const DEFAULT_IDLE_REAP_MINUTES: u32 = 10;

/// Default daemon-wide cap on concurrently live ephemeral ACP adapters
/// (`agents.maxConcurrentAdapters`): the one-shot `agent.completeOnce`
/// completions and model probes that spawn a provider-CLI chain without
/// holding an agent slot. Each chain measures ~610 MB, so 6 bounds a
/// quick-action fan-out at ~3.7 GB (monorepo#2062). There is no unlimited
/// value — see [`MAX_CONCURRENT_ADAPTERS_LIMIT`].
pub const DEFAULT_MAX_CONCURRENT_ADAPTERS: u32 = 6;

/// Upper bound accepted for `agents.maxConcurrentAdapters`. Well above the
/// 4–8 product range so an operator on a large host can raise it, and low
/// enough that the setting cannot be turned back into the unbounded spawn
/// this bound replaced (~610 MB × 64 ≈ 39 GB is already a bad day).
pub const MAX_CONCURRENT_ADAPTERS_LIMIT: u32 = 64;

/// Default cap on live top-level (depth-0) agents in a workspace
/// (`agents.maxTopLevelAgents`), enforced on the top-level-create path
/// (`ws.agent.create({ topLevel: true })`) as a runaway-spawn guard;
/// user-created agents are never blocked by it. Minimum 1 — there is no
/// unlimited value.
pub const DEFAULT_MAX_TOP_LEVEL_AGENTS: u32 = 20;

/// Default grace window in seconds (`agents.reportToParentDebounceSeconds`)
/// before an ungrouped child's `reportToParent` wake is delivered to the
/// parent, giving the child time to finish its turn so the parent receives
/// one combined wake instead of two; `0` disables the debounce (legacy
/// immediate wake). Read live from the settings snapshot at each call — no
/// restart required.
pub const DEFAULT_REPORT_TO_PARENT_DEBOUNCE_SECONDS: u32 = 30;

/// Default ephemeral-event retention TTL in hours (`events.streamRetentionHours`,
/// §10.2); `0` disables the retention/compaction sweep entirely. Defaults to 72h
/// (3 days) so dev/release databases do not grow unboundedly; set to `0` to opt
/// out and preserve all events.
pub const DEFAULT_STREAM_RETENTION_HOURS: u32 = 72;

/// Default max characters of one `workspace_api` tool result before the
/// output is redirected to a file (`workspaceApi.maxOutputChars`); `0` means
/// unlimited (never redirect).
pub(crate) const DEFAULT_WORKSPACE_API_MAX_OUTPUT_CHARS: u32 = 100_000;

/// Default for `workspaceApi.toonOutput` — whether `workspace_api` tool
/// results are TOON-encoded (token-efficient) instead of plain JSON.
pub(crate) const DEFAULT_WORKSPACE_API_TOON_OUTPUT: bool = true;

/// Default cap on concurrently active (scheduled/running) background hooks per
/// agent (`hooks.maxPerAgent`).
pub const DEFAULT_HOOKS_MAX_PER_AGENT: u32 = 5;

/// Default daemon-wide cap on outstanding slow-path RPCs
/// (`server.maxOutstandingRpcs`); `0` means unlimited.
pub const DEFAULT_SERVER_MAX_OUTSTANDING_RPCS: u32 = 256;

/// Default for `wakeResume.enabled` — whether the daemon detects host
/// sleep/wake and resumes work on wake. On by default.
pub const DEFAULT_WAKE_RESUME_ENABLED: bool = true;

/// Default for `wakeResume.thresholdSeconds` — the minimum suspend duration
/// (in seconds) that counts as a sleep for the resume/enrollment gate.
pub const DEFAULT_WAKE_RESUME_THRESHOLD_SECONDS: u32 = 10;

/// Floor for `wakeResume.thresholdSeconds`. The clock-skew detector samples
/// roughly every second and flags a suspend when `skew >= threshold`; skew is
/// non-negative by construction, so a `0` threshold classifies EVERY ~1s tick
/// as a suspend (and the wake debounce may never settle). Clamp any sub-minimum
/// value (notably `0`) up to this floor so the detector always requires a real
/// ≥1s wall/monotonic divergence.
pub const MIN_WAKE_RESUME_THRESHOLD_SECONDS: u32 = 1;

/// Default poll cadence for the centralized PR-monitor loop
/// (`prMonitor.pollSeconds`).
pub const DEFAULT_PR_MONITOR_POLL_SECONDS: u64 = 30;

/// Floor for `prMonitor.pollSeconds` — a tighter interval would hammer the
/// forge. Sub-minimum values (notably `0`) are clamped up at read time.
pub const MIN_PR_MONITOR_POLL_SECONDS: u64 = 10;

/// Default quiet window a changed PR must observe before its consolidated
/// wake is delivered (`prMonitor.debounceSeconds`).
pub const DEFAULT_PR_MONITOR_DEBOUNCE_SECONDS: u64 = 60;

/// Floor for `prMonitor.debounceSeconds`. Sub-minimum values (notably `0`)
/// are clamped up at read time.
pub const MIN_PR_MONITOR_DEBOUNCE_SECONDS: u64 = 10;

/// Resolved filesystem locations for the daemon.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    /// Data directory (database, certs, runtime socket).
    pub data_dir: PathBuf,
    /// Path to `config.toml` (non-secret settings).
    pub config_path: PathBuf,
    /// Path to the `SQLite` database file.
    pub db_path: PathBuf,
    /// Path to the Unix-domain-socket the daemon listens on.
    pub socket_path: PathBuf,
    /// Path to the single-instance pidfile (`intentd.pid`, §5.6/§9.3).
    pub pid_path: PathBuf,
    /// Minutes an agent may sit idle before the reap sweep evicts it
    /// (`agents.idleReapMinutes`, §11.1); `0` disables idle reaping.
    pub idle_reap_minutes: u32,
    /// Hours ephemeral events (`agent:stream:*`, `file:*`, `terminal:data`,
    /// `host:exec:*`) are retained before the retention/compaction sweep deletes
    /// them (`events.streamRetentionHours`, §10.2); `0` disables the sweep.
    /// Lifecycle/tool/note/task/workspace events are preserved regardless of age.
    pub stream_retention_hours: u32,
    /// Cap on concurrently active (scheduled/running) background hooks per
    /// agent (`hooks.maxPerAgent`).
    pub hooks_max_per_agent: u32,
    /// Daemon-wide cap on outstanding slow-path RPCs
    /// (`server.maxOutstandingRpcs`); `0` means unlimited.
    pub server_max_outstanding_rpcs: u32,
    /// Whether the daemon detects host sleep/wake and resumes work on wake
    /// (`wakeResume.enabled`); on by default.
    pub wake_resume_enabled: bool,
    /// Minimum suspend duration in seconds that counts as a sleep for the
    /// resume/enrollment gate (`wakeResume.thresholdSeconds`).
    pub wake_resume_threshold_seconds: u32,
}

impl Config {
    /// Resolve paths from the platform defaults and env overrides (§11.2),
    /// then load `config.toml` strictly via [`SettingsFile::load_or_init`].
    ///
    /// `config.toml` lives in the **data dir** (`<data_dir>/config.toml`);
    /// `INTENTD_CONFIG` overrides the full path. A missing file is initialized
    /// with the commented default template; a malformed file (unknown key,
    /// wrong type, out-of-range value) is an error — never silently ignored.
    /// `INTENTD_IDLE_REAP_MINUTES` / `INTENTD_STREAM_RETENTION_HOURS` env vars
    /// still take precedence over the file for their respective knobs.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the data directory cannot be resolved or `config.toml` cannot be read/initialized; `Error::InvalidInput` if the file is malformed.
    pub fn resolve() -> Result<Self> {
        let data_dir = match std::env::var_os("INTENTD_DATA_DIR") {
            Some(p) => PathBuf::from(p),
            None => directories::ProjectDirs::from("", "", "intentd")
                .map(|d| d.data_dir().to_path_buf())
                .ok_or_else(|| Error::Internal("could not resolve data directory".to_string()))?,
        };

        let config_path = match std::env::var_os("INTENTD_CONFIG") {
            Some(p) => PathBuf::from(p),
            None => data_dir.join("config.toml"),
        };

        let settings = SettingsFile::load_or_init(&config_path)?;

        let db_path = data_dir.join("intentd.db");
        let socket_path = data_dir.join("intentd.sock");
        let pid_path = data_dir.join("intentd.pid");
        let idle_reap_minutes =
            env_u32("INTENTD_IDLE_REAP_MINUTES").unwrap_or(settings.agents.idle_reap_minutes);
        let stream_retention_hours = env_u32("INTENTD_STREAM_RETENTION_HOURS")
            .unwrap_or(settings.events.stream_retention_hours);
        let hooks_max_per_agent = settings.hooks.max_per_agent;
        let server_max_outstanding_rpcs = settings.server.max_outstanding_rpcs;
        let wake_resume_enabled = settings.wake_resume.enabled;
        // Clamp the configured threshold up to the floor: a `0` (or any
        // sub-minimum) value would make the clock-skew detector flag every
        // sampling tick as a suspend (see [`MIN_WAKE_RESUME_THRESHOLD_SECONDS`]).
        let wake_resume_threshold_seconds = settings
            .wake_resume
            .threshold_seconds
            .max(MIN_WAKE_RESUME_THRESHOLD_SECONDS);

        Ok(Self {
            data_dir,
            config_path,
            db_path,
            socket_path,
            pid_path,
            idle_reap_minutes,
            stream_retention_hours,
            hooks_max_per_agent,
            server_max_outstanding_rpcs,
            wake_resume_enabled,
            wake_resume_threshold_seconds,
        })
    }
}

/// Read a `u32` env override; unset or unparseable values yield `None` so the
/// caller falls through to the file-backed value.
fn env_u32(name: &str) -> Option<u32> {
    std::env::var_os(name)?
        .to_string_lossy()
        .trim()
        .parse()
        .ok()
}
