//! Supervisor loop: run the installed daemon, keep it updated, babysit it.
//!
//! Only a `serve` invocation ever talks to the updater. One-shot subcommands
//! (`status`, `stop`, `doctor`, `call`, …, including empty args) have zero
//! updater side effects — no manifest fetch, no `state.json` write, no prune:
//! they run the installed `state.current_version` directly, and when nothing
//! is installed they exit non-zero with guidance to run `intentd serve` (or
//! `brew services start intentd`) so the daemon gets installed.
//!
//! `serve` lifecycle:
//!
//! 1. startup update check (fail-fast): on success run the manifest version;
//!    on failure fall back to `state.current_version`; nothing installed AND
//!    check failed → exit non-zero with a clear message
//! 2. spawn `versions/<current>/intentd` with all forwarded args verbatim,
//!    inheriting stdio and environment. The sitter's one injection:
//!    respawning a version different from the one that just ran in this
//!    sitter's lifetime sets [`UPDATE_RESTART_ENV`]`=1` on the child, so
//!    the daemon can tell an update-triggered restart apart from a first
//!    spawn, a crash respawn, or a same-version SIGHUP restart (none of
//!    which set it)
//! 3. after every check, pick the next check uniformly at random in
//!    [`SupervisorConfig::check_min`], [`SupervisorConfig::check_max`]) and
//!    persist it to `state.json`
//! 4. update found mid-run: download/verify/install first, then stop the
//!    child gracefully (SIGTERM + kill timeout on unix; terminate on
//!    windows) and respawn the new version with the same args
//! 5. unexpected child exit (non-zero or signal) → respawn the same version
//!    with exponential backoff — but not forever:
//!    [`SupervisorConfig::give_up_after_failures`] consecutive failed
//!    starts, none of which stayed up for
//!    [`SupervisorConfig::backoff_reset_after`] (spawn errors count too),
//!    make the sitter log the failure prominently and exit **0**. Zero is
//!    load-bearing: launchd's `KeepAlive`/`SuccessfulExit: false` and
//!    systemd's `Restart=on-failure` both relaunch a non-zero exit, so only
//!    a clean exit actually stops a daemon that can never start. Any start
//!    that lasts `backoff_reset_after`, an installed update, and a SIGHUP
//!    restart each clear the counter alongside the backoff, so healthy and
//!    transiently-failing daemons are supervised exactly as before. Clean
//!    exit 0 → sitter exits 0; sitter-initiated stops never respawn. Only a
//!    `serve` invocation is babysat this way: one-shot subcommands
//!    (`status`, `stop`, `doctor`, `call`, …) legitimately exit non-zero, so
//!    they run exactly once and their exit status passes through
//! 6. every failed start (crash or spawn error) also forces an off-schedule
//!    channel re-check before the respawn/give-up decision, persisting the
//!    check schedule as usual: a crash loop is the strongest signal that
//!    the pinned version is broken, so a fixed build published on the
//!    channel is installed and respawned instead of the sitter respawning
//!    the broken version until it gives up (intent-hq/monorepo#3191). The
//!    give-up only fires after the final failure's re-check found nothing
//!    newer (or could not be reached); an installed fix clears the backoff
//!    and the failure counter like any other installed update. The re-check
//!    honors shutdown/restart signals immediately (never deferring them
//!    behind the manifest fetch or a download) and adopts a version a
//!    concurrent updater (e.g. `sitter channel --redownload`) installed
//!    while the loop was crashing
//! 7. SIGINT/SIGTERM (ctrl-c on windows) are forwarded to the child and the
//!    sitter exits with the child's status
//! 8. SIGHUP (unix only, sent by `intentd restart`) stops the child
//!    gracefully and respawns it on the current `state.json` version —
//!    activating a prior `sitter channel --redownload` install — without
//!    the sitter exiting. A SIGHUP that lands during a crash-backoff sleep
//!    has the same semantics: it cuts the wait short, re-resolves the
//!    version from `state.json`, and resets the backoff. Serve mode
//!    advertises itself for this via `<data_dir>/sitter/sitter.pid`,
//!    written before the supervision loop and removed on exit
//! 9. SIGUSR1 (unix only) runs the update check immediately — the same
//!    path as the periodic check, rescheduling it — and, when a newer
//!    version installs (or a concurrently installed one is found), stops
//!    the child gracefully and respawns it on the new version (an
//!    update-triggered respawn, so [`UPDATE_RESTART_ENV`] is set) with
//!    the backoff and failure counter reset. Already current (or a
//!    failed check, which is logged and non-fatal) leaves the daemon
//!    running. A SIGUSR1 during a crash-backoff sleep cuts the wait
//!    short, checks, and respawns
//!
//! When the startup channel came from `config.toml` or the stable default
//! (not the `--sitter-channel` flag or `INTENTD_CHANNEL` env), every update
//! check re-resolves the channel from `config.toml` first, so
//! `intentd sitter channel <value>` takes effect on a running service at its
//! next periodic check. Flag/env selections stay pinned for the process
//! lifetime.
//!
//! The updater API is blocking, so checks run on the blocking thread pool
//! (`spawn_blocking`) and never stall the supervisor's timers or signal
//! handling.

use std::ffi::OsString;
use std::io;
use std::path::Path;
use std::process::ExitStatus;
use std::sync::Arc;
use std::time::Duration;

use time::OffsetDateTime;
use tokio::time::Instant;

use crate::cli::Channel;
use crate::config::{self, ChannelOrigin, ResolvedChannel};
use crate::paths::SitterPaths;
use crate::state;
use crate::updater::{UpdateError, UpdateOutcome, Updater};

/// Env override for the channel-manifest base URL — exactly one base, no
/// fallback (tests point this at a local fixture server; production never
/// sets it).
pub const MANIFEST_BASE_URL_ENV: &str = "INTENTD_SITTER_MANIFEST_BASE_URL";

/// Set to `1` in the child's environment when a respawn is
/// update-triggered: the version being spawned differs from the one that
/// just ran in this sitter's lifetime (periodic mid-run install, SIGHUP
/// after a CLI update, or a fix adopted after a failed start). First
/// spawns, crash respawns, and same-version SIGHUP restarts never set it.
/// The daemon reads it to force the startup interrupted-agent resume
/// sweep after updates.
pub const UPDATE_RESTART_ENV: &str = "INTENTD_UPDATE_RESTART";

/// Test-only env overrides (integer milliseconds) for the timing knobs in
/// [`SupervisorConfig`], so integration tests run at millisecond scale.
/// Production never sets these.
pub const CHECK_MIN_ENV: &str = "INTENTD_SITTER_CHECK_MIN_MS";
pub const CHECK_MAX_ENV: &str = "INTENTD_SITTER_CHECK_MAX_MS";
pub const BACKOFF_INITIAL_ENV: &str = "INTENTD_SITTER_BACKOFF_INITIAL_MS";
pub const BACKOFF_CAP_ENV: &str = "INTENTD_SITTER_BACKOFF_CAP_MS";
pub const BACKOFF_RESET_ENV: &str = "INTENTD_SITTER_BACKOFF_RESET_MS";
pub const KILL_TIMEOUT_ENV: &str = "INTENTD_SITTER_KILL_TIMEOUT_MS";

/// Test-only env override (a count, not milliseconds) for
/// [`SupervisorConfig::give_up_after_failures`]. Production never sets it.
/// `0` does not disable the give-up: the counter is checked after each
/// failure, so `0` behaves like `1` — give up on the first failed start.
pub const GIVE_UP_AFTER_ENV: &str = "INTENTD_SITTER_GIVE_UP_AFTER";

/// Timing knobs for the supervisor loop. Injectable so tests never sleep
/// for hours; production uses [`SupervisorConfig::default`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SupervisorConfig {
    /// Lower bound (inclusive) of the randomized update-check interval.
    pub check_min: Duration,
    /// Upper bound (exclusive) of the randomized update-check interval.
    pub check_max: Duration,
    /// First respawn delay after an unexpected child exit.
    pub backoff_initial: Duration,
    /// Maximum respawn delay (doubling stops here).
    pub backoff_cap: Duration,
    /// Child uptime after which the backoff resets to `backoff_initial`.
    pub backoff_reset_after: Duration,
    /// Consecutive failed starts — none of which stayed up for
    /// `backoff_reset_after` — after which the sitter gives up instead of
    /// respawning forever. See [`Supervisor::report_give_up`].
    pub give_up_after_failures: u32,
    /// How long a graceful stop waits before force-killing the child.
    pub kill_timeout: Duration,
}

impl Default for SupervisorConfig {
    fn default() -> Self {
        Self {
            check_min: Duration::from_secs(12 * 60 * 60),
            check_max: Duration::from_secs(24 * 60 * 60),
            backoff_initial: Duration::from_secs(1),
            backoff_cap: Duration::from_secs(60),
            backoff_reset_after: Duration::from_secs(5 * 60),
            // Ten consecutive failures. With the backoff above (1s doubling
            // to a 60s cap) the tenth start lands ~4 minutes after the
            // first, which outlasts every transient cause we have seen — a
            // stale socket or lock left by a hard-killed daemon, a slow
            // first-boot or resume-from-sleep disk, an upgrade swapping the
            // binary underneath us — while still stopping a genuinely broken
            // install fast enough to leave one readable diagnosis instead of
            // an endless log. Any single start that survives
            // `backoff_reset_after` clears the count, so a daemon that works
            // at all can never trip it.
            give_up_after_failures: 10,
            kill_timeout: Duration::from_secs(30),
        }
    }
}

impl SupervisorConfig {
    /// Defaults with any test-only `INTENTD_SITTER_*_MS` env overrides
    /// applied.
    #[must_use]
    pub fn from_env() -> Self {
        Self::from_lookup(|name| std::env::var(name).ok())
    }

    /// [`Self::from_env`] with an injectable lookup so tests never mutate
    /// process state. Unset, empty, or unparseable values keep the default.
    pub fn from_lookup(get: impl Fn(&str) -> Option<String>) -> Self {
        let mut config = Self::default();
        let ms = |name: &str| {
            get(name)
                .and_then(|v| v.parse::<u64>().ok())
                .map(Duration::from_millis)
        };
        if let Some(v) = ms(CHECK_MIN_ENV) {
            config.check_min = v;
        }
        if let Some(v) = ms(CHECK_MAX_ENV) {
            config.check_max = v;
        }
        if let Some(v) = ms(BACKOFF_INITIAL_ENV) {
            config.backoff_initial = v;
        }
        if let Some(v) = ms(BACKOFF_CAP_ENV) {
            config.backoff_cap = v;
        }
        if let Some(v) = ms(BACKOFF_RESET_ENV) {
            config.backoff_reset_after = v;
        }
        if let Some(v) = ms(KILL_TIMEOUT_ENV) {
            config.kill_timeout = v;
        }
        if let Some(v) = get(GIVE_UP_AFTER_ENV).and_then(|v| v.parse::<u32>().ok()) {
            config.give_up_after_failures = v;
        }
        config
    }
}

/// Delay until the next update check: uniformly distributed in
/// [`min`, `max`) driven by `random` (pure, so tests can assert the
/// distribution). Degenerate ranges (`max <= min`) collapse to `min`.
#[must_use]
pub fn next_check_delay(min: Duration, max: Duration, random: u64) -> Duration {
    let span = max.saturating_sub(min).as_nanos();
    if span == 0 {
        return min;
    }
    let offset = u128::from(random) % span;
    min + Duration::from_nanos(u64::try_from(offset).unwrap_or(u64::MAX))
}

/// A random `u64` from the standard library's randomly-keyed `SipHash`
/// (no `rand` dependency; jitter does not need cryptographic quality).
fn random_u64() -> u64 {
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hasher};
    RandomState::new().build_hasher().finish()
}

/// Read a sitter pidfile and probe the recorded process for liveness.
/// Returns the pid only when the file holds one and that process is alive;
/// a missing, unparsable, or stale (dead pid) file is treated as absent.
#[cfg(unix)]
#[must_use]
pub fn read_live_pid(path: &Path) -> Option<nix::unistd::Pid> {
    let contents = std::fs::read_to_string(path).ok()?;
    let pid = contents.trim().parse::<i32>().ok().filter(|pid| *pid > 0)?;
    let pid = nix::unistd::Pid::from_raw(pid);
    nix::sys::signal::kill(pid, None).is_ok().then_some(pid)
}

/// Pidfile guard: writes the sitter's own pid on creation and removes the
/// file on drop — but only when it still holds this process's pid, so a
/// later serve sitter's entry (last writer wins) is never deleted by an
/// earlier one exiting. Serve mode only — `intentd restart` reads it to
/// find the supervising sitter.
#[cfg(unix)]
struct PidFile {
    path: std::path::PathBuf,
}

#[cfg(unix)]
impl PidFile {
    fn create(path: &Path) -> Option<Self> {
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        if let Some(pid) = read_live_pid(path) {
            eprintln!(
                "intentd-sitter: pidfile {} already names live pid {pid}; another \
                 serve sitter appears to be running (overwriting — `intentd restart` \
                 will target this sitter)",
                path.display()
            );
        }
        match std::fs::write(path, format!("{}\n", std::process::id())) {
            Ok(()) => Some(Self {
                path: path.to_path_buf(),
            }),
            Err(e) => {
                eprintln!(
                    "intentd-sitter: failed to write pidfile {}: {e} \
                     (`intentd restart` will not find this sitter)",
                    path.display()
                );
                None
            }
        }
    }
}

#[cfg(unix)]
impl Drop for PidFile {
    fn drop(&mut self) {
        // Only remove the file when it still holds our pid: another serve
        // sitter may have overwritten it, and deleting its entry would
        // break `intentd restart` for that live sitter.
        let ours = std::process::id().to_string();
        if std::fs::read_to_string(&self.path).is_ok_and(|s| s.trim() == ours) {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

/// The channel an update check should use. Flag/env selections stay pinned
/// for the process lifetime; config/default selections re-resolve from
/// `config.toml` so a running service follows `intentd sitter channel`
/// pins without a restart.
fn effective_channel(startup: ResolvedChannel, config_path: &Path) -> Channel {
    match startup.origin {
        ChannelOrigin::Flag | ChannelOrigin::Env => startup.channel,
        ChannelOrigin::Config | ChannelOrigin::Default => {
            config::resolve_channel(None, config::load_channel(config_path)).channel
        }
    }
}

/// Build a tokio runtime and drive the supervisor to completion, returning
/// the sitter's process exit code.
#[must_use]
pub fn run(
    paths: SitterPaths,
    channel: ResolvedChannel,
    passthrough: Vec<OsString>,
    config: SupervisorConfig,
    base_urls: Vec<String>,
) -> i32 {
    let updater = match Updater::with_base_urls(paths.clone(), base_urls) {
        Ok(updater) => Arc::new(updater),
        Err(e) => {
            eprintln!("intentd-sitter: {e}");
            return 1;
        }
    };
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("intentd-sitter: failed to start async runtime: {e}");
            return 1;
        }
    };
    let supervisor = Supervisor {
        paths,
        channel,
        passthrough,
        config,
        updater,
    };
    let code = runtime.block_on(supervisor.supervise());
    // Dropping the runtime waits for in-flight `spawn_blocking` tasks — and
    // an update check abandoned mid-shutdown (its blocking HTTP client has a
    // minutes-long timeout against a stalled endpoint) must not wedge the
    // exit. Shut down in the background instead: the task is detached and
    // dies with the process.
    runtime.shutdown_background();
    code
}

struct Supervisor {
    paths: SitterPaths,
    channel: ResolvedChannel,
    passthrough: Vec<OsString>,
    config: SupervisorConfig,
    updater: Arc<Updater>,
}

impl Supervisor {
    async fn supervise(self) -> i32 {
        // Only a long-running `serve` child is babysat (startup + periodic
        // update checks, crash respawn, mid-run update restarts). One-shot
        // subcommands never touch the updater: they run the installed
        // version exactly once and their exit status passes through.
        let supervised = self.passthrough.first().is_some_and(|arg| arg == "serve");

        let (mut current_version, mut next_check_at) = if supervised {
            // Startup check: always runs, regardless of the persisted schedule.
            let startup = self.check().await;
            let next_check_at = self.schedule_next_check();
            let version = match startup {
                Ok(UpdateOutcome::Installed { version, previous }) => {
                    match previous {
                        Some(previous) => {
                            eprintln!("intentd-sitter: updated intentd {previous} -> {version}");
                        }
                        None => eprintln!("intentd-sitter: installed intentd {version}"),
                    }
                    version
                }
                Ok(UpdateOutcome::AlreadyCurrent { version }) => version,
                Err(e) => {
                    eprintln!("intentd-sitter: update check failed: {e}");
                    let state = state::load(&self.paths.state_path);
                    if let Some(version) = state
                        .current_version
                        .filter(|v| self.paths.daemon_binary(v).exists())
                    {
                        eprintln!("intentd-sitter: falling back to installed intentd {version}");
                        version
                    } else {
                        eprintln!(
                            "intentd-sitter: no intentd daemon is installed for channel {} \
                             and the update check failed; cannot start (check network access \
                             and retry)",
                            self.channel.channel
                        );
                        return 1;
                    }
                }
            };
            (version, next_check_at)
        } else {
            // One-shot: resolve the installed version with no updater
            // activity (no manifest fetch, no state.json write, no prune).
            let state = state::load(&self.paths.state_path);
            let version = if let Some(version) = state
                .current_version
                .filter(|v| self.paths.daemon_binary(v).exists())
            {
                // The channel flag only governs updater behavior, which
                // one-shots don't have; surface a mismatch but run anyway.
                if state.channel != self.channel.channel {
                    eprintln!(
                        "intentd-sitter: note: channel {} requested but the installed \
                         daemon was installed from channel {}; one-shot commands run \
                         the installed daemon as-is",
                        self.channel.channel, state.channel
                    );
                }
                version
            } else {
                eprintln!(
                    "intentd-sitter: no intentd daemon is installed for channel {}; \
                     start the daemon first (`intentd serve` or \
                     `brew services start intentd`) so it gets installed",
                    self.channel.channel
                );
                return 1;
            };
            // Never polled: the periodic-check select arm is serve-only.
            (version, Instant::now())
        };

        let mut signals = match Signals::new() {
            Ok(signals) => signals,
            Err(e) => {
                eprintln!("intentd-sitter: failed to install signal handlers: {e}");
                return 1;
            }
        };
        // `intentd restart` finds the serve sitter through this pidfile;
        // written only after the signal handlers are installed so a reader
        // can never SIGHUP a sitter that would still die to it. Removed on
        // drop (any return path); a hard kill leaves a stale file, which
        // readers detect via a liveness probe.
        #[cfg(unix)]
        let _pidfile = if supervised {
            PidFile::create(&self.paths.pid_path)
        } else {
            None
        };
        let mut backoff = self.config.backoff_initial;
        // Failed starts since the last one that stayed up (see
        // `give_up_after_failures`); reset wherever the backoff resets.
        let mut failures: u32 = 0;
        // Version of the child that last ran (successfully spawned):
        // spawning a different one means the respawn is update-triggered,
        // which the child is told via UPDATE_RESTART_ENV. Failed spawns
        // don't count — retrying an updated version that couldn't spawn is
        // still update-triggered relative to the version that last ran.
        // Accepted trade-off: recorded at spawn, so if the freshly updated
        // version crashes before its startup resume sweep completes, the
        // subsequent respawn is same-version and unmarked — with
        // `agents.resumeInterruptedOnStart=off`, agents interrupted by the
        // update then stay unresumed (a crash respawn is a plain restart).
        let mut last_ran_version: Option<String> = None;

        loop {
            let binary = self.paths.daemon_binary(&current_version);
            let mut command = tokio::process::Command::new(&binary);
            command.args(&self.passthrough).kill_on_drop(true);
            if last_ran_version
                .as_ref()
                .is_some_and(|last| *last != current_version)
            {
                command.env(UPDATE_RESTART_ENV, "1");
            } else {
                // Clear rather than inherit: if the sitter itself was
                // launched with the marker set, a first spawn, crash
                // respawn, or same-version restart must not carry it.
                command.env_remove(UPDATE_RESTART_ENV);
            }
            let mut child = match command.spawn() {
                Ok(child) => child,
                Err(e) => {
                    // "failed to spawn" is part of the install-script log
                    // contract (see the `what` match in the wait arm below).
                    let what = format!("failed to spawn {}: {e}", binary.display());
                    if !supervised {
                        eprintln!("intentd-sitter: {what}");
                        return 1;
                    }
                    // A binary that cannot be spawned at all (missing,
                    // truncated, wrong arch) is as permanent as one that
                    // starts and dies, so it feeds the same counter.
                    failures += 1;
                    eprintln!("intentd-sitter: {what}");
                    // A failed start forces an off-schedule channel re-check
                    // (see the module docs and the wait arm below): a fix
                    // published on the channel heals the loop.
                    match self
                        .check_after_failed_start(
                            &current_version,
                            &mut signals,
                            &mut next_check_at,
                        )
                        .await
                    {
                        FailedStartCheck::Respawn(version) => {
                            current_version = version;
                            backoff = self.config.backoff_initial;
                            failures = 0;
                            continue;
                        }
                        FailedStartCheck::Shutdown(code) => return code,
                        #[cfg(unix)]
                        FailedStartCheck::RestartRequested => {
                            self.refresh_version_from_state(&mut current_version);
                            backoff = self.config.backoff_initial;
                            failures = 0;
                            continue;
                        }
                        FailedStartCheck::NothingNewer => {}
                    }
                    if failures >= self.config.give_up_after_failures {
                        self.report_give_up(failures);
                        return 0;
                    }
                    match self.backoff_sleep(&mut backoff, &mut signals).await {
                        BackoffOutcome::Shutdown(code) => return code,
                        #[cfg(unix)]
                        BackoffOutcome::RestartRequested => {
                            self.refresh_version_from_state(&mut current_version);
                            backoff = self.config.backoff_initial;
                            failures = 0;
                            continue;
                        }
                        #[cfg(unix)]
                        BackoffOutcome::CheckNowRequested => {
                            match self
                                .check_now(&current_version, &mut signals, &mut next_check_at)
                                .await
                            {
                                CheckNowOutcome::Shutdown(signal) => return 128 + signal,
                                CheckNowOutcome::RestartRequested => {
                                    self.refresh_version_from_state(&mut current_version);
                                }
                                CheckNowOutcome::Respawn(version) => current_version = version,
                                CheckNowOutcome::Unchanged => {}
                            }
                            backoff = self.config.backoff_initial;
                            failures = 0;
                            continue;
                        }
                        BackoffOutcome::Elapsed => continue,
                    }
                }
            };
            last_ran_version = Some(current_version.clone());
            let spawned_at = Instant::now();

            // Supervise this child until it exits or the sitter stops it.
            loop {
                tokio::select! {
                    status = child.wait() => {
                        // The failure phrasings built here ("exited
                        // unexpectedly", "failed waiting on intentd", plus
                        // "failed to spawn" above) and the give-up banner in
                        // `report_give_up` are a detection contract with
                        // scripts/install.sh and scripts/install.ps1: their
                        // post-timeout diagnosis greps this run's service log
                        // for these substrings. Reword only in lockstep with
                        // both scripts and the `install_log_contract_*` tests
                        // in tests/supervisor_e2e.rs.
                        let what = match status {
                            Ok(status) if status.success() => return 0,
                            Ok(status) if !supervised => return exit_code(status),
                            Ok(status) => format!(
                                "intentd {current_version} exited unexpectedly ({})",
                                describe_exit(status)
                            ),
                            Err(e) if !supervised => {
                                eprintln!(
                                    "intentd-sitter: failed waiting on intentd {current_version}: {e}"
                                );
                                return 1;
                            }
                            Err(e) => format!(
                                "failed waiting on intentd {current_version}: {e}"
                            ),
                        };
                        // A start that lasted `backoff_reset_after` counts as
                        // a real serve: it clears both the backoff and the
                        // give-up counter, so a daemon that crashes only
                        // occasionally keeps being respawned forever.
                        if spawned_at.elapsed() >= self.config.backoff_reset_after {
                            backoff = self.config.backoff_initial;
                            failures = 0;
                        }
                        failures += 1;
                        eprintln!("intentd-sitter: {what}");
                        // A failed start forces an off-schedule channel
                        // re-check (see the module docs): a fix published on
                        // the channel is installed and respawned instead of
                        // respawning the broken version until give-up.
                        match self
                            .check_after_failed_start(&current_version, &mut signals, &mut next_check_at)
                            .await
                        {
                            FailedStartCheck::Respawn(version) => {
                                current_version = version;
                                backoff = self.config.backoff_initial;
                                failures = 0;
                                break; // respawn the fixed version immediately
                            }
                            FailedStartCheck::Shutdown(code) => return code,
                            #[cfg(unix)]
                            FailedStartCheck::RestartRequested => {
                                self.refresh_version_from_state(&mut current_version);
                                backoff = self.config.backoff_initial;
                                failures = 0;
                                break; // respawn the state.json version
                            }
                            FailedStartCheck::NothingNewer => {}
                        }
                        if failures >= self.config.give_up_after_failures {
                            self.report_give_up(failures);
                            return 0;
                        }
                        match self.backoff_sleep(&mut backoff, &mut signals).await {
                            BackoffOutcome::Shutdown(code) => return code,
                            #[cfg(unix)]
                            BackoffOutcome::RestartRequested => {
                                self.refresh_version_from_state(&mut current_version);
                                backoff = self.config.backoff_initial;
                                failures = 0;
                            }
                            #[cfg(unix)]
                            BackoffOutcome::CheckNowRequested => {
                                match self
                                    .check_now(&current_version, &mut signals, &mut next_check_at)
                                    .await
                                {
                                    CheckNowOutcome::Shutdown(signal) => return 128 + signal,
                                    CheckNowOutcome::RestartRequested => {
                                        self.refresh_version_from_state(&mut current_version);
                                    }
                                    CheckNowOutcome::Respawn(version) => current_version = version,
                                    CheckNowOutcome::Unchanged => {}
                                }
                                backoff = self.config.backoff_initial;
                                failures = 0;
                            }
                            BackoffOutcome::Elapsed => {}
                        }
                        break; // respawn (possibly a new version after SIGHUP)
                    }
                    () = tokio::time::sleep_until(next_check_at), if supervised => {
                        match self.check().await {
                            Ok(UpdateOutcome::Installed { version, previous }) => {
                                eprintln!(
                                    "intentd-sitter: installed intentd {version} (was {}); restarting daemon",
                                    previous.as_deref().unwrap_or("none")
                                );
                                next_check_at = self.schedule_next_check();
                                self.graceful_stop(&mut child).await;
                                current_version = version;
                                backoff = self.config.backoff_initial;
                                failures = 0;
                                break; // respawn the new version
                            }
                            Ok(UpdateOutcome::AlreadyCurrent { .. }) => {}
                            Err(e) => eprintln!(
                                "intentd-sitter: update check failed: {e}; will retry at the next scheduled check"
                            ),
                        }
                        next_check_at = self.schedule_next_check();
                    }
                    event = signals.recv() => {
                        let signal = match event {
                            // `intentd restart` (SIGHUP): stop the child
                            // gracefully and respawn it on the current
                            // state.json version — activating a prior
                            // `sitter channel --redownload` install — with
                            // the backoff reset. The sitter never exits on
                            // SIGHUP; the channel pin is re-resolved by the
                            // next periodic check as usual. One-shots have
                            // no supervised child to restart.
                            #[cfg(unix)]
                            SignalEvent::Restart => {
                                if !supervised {
                                    eprintln!("intentd-sitter: ignoring SIGHUP (one-shot invocation)");
                                    continue;
                                }
                                eprintln!("intentd-sitter: SIGHUP received; restarting intentd");
                                self.graceful_stop(&mut child).await;
                                self.refresh_version_from_state(&mut current_version);
                                backoff = self.config.backoff_initial;
                                failures = 0;
                                break; // respawn (possibly a new version)
                            }
                            // `kill -USR1` (update now): run the update
                            // check immediately — the same path as the
                            // periodic check, rescheduling it — and
                            // restart the daemon only when a different
                            // version installed; already current or a
                            // failed check leaves it running. One-shots
                            // have no updater.
                            #[cfg(unix)]
                            SignalEvent::CheckNow => {
                                if !supervised {
                                    eprintln!("intentd-sitter: ignoring SIGUSR1 (one-shot invocation)");
                                    continue;
                                }
                                eprintln!("intentd-sitter: SIGUSR1 received; checking for updates now");
                                match self
                                    .check_now(&current_version, &mut signals, &mut next_check_at)
                                    .await
                                {
                                    CheckNowOutcome::Respawn(version) => {
                                        self.graceful_stop(&mut child).await;
                                        current_version = version;
                                        backoff = self.config.backoff_initial;
                                        failures = 0;
                                        break; // respawn the new version
                                    }
                                    CheckNowOutcome::Unchanged => continue, // daemon untouched
                                    CheckNowOutcome::RestartRequested => {
                                        self.graceful_stop(&mut child).await;
                                        self.refresh_version_from_state(&mut current_version);
                                        backoff = self.config.backoff_initial;
                                        failures = 0;
                                        break; // respawn (possibly a new version)
                                    }
                                    // Fall through to the shutdown handling
                                    // below, exactly as if the signal had
                                    // arrived outside the check.
                                    CheckNowOutcome::Shutdown(signal) => signal,
                                }
                            }
                            SignalEvent::Shutdown(signal) => signal,
                        };
                        forward_signal(&child, signal);
                        let status = match tokio::time::timeout(self.config.kill_timeout, child.wait()).await {
                            Ok(Ok(status)) => status,
                            Ok(Err(e)) => {
                                eprintln!("intentd-sitter: failed waiting on intentd: {e}");
                                return 1;
                            }
                            Err(_) => {
                                eprintln!(
                                    "intentd-sitter: intentd did not exit within {:?} of forwarded signal; killing",
                                    self.config.kill_timeout
                                );
                                let _ = child.kill().await;
                                return 128 + signal;
                            }
                        };
                        return exit_code(status);
                    }
                }
            }
        }
    }

    /// One blocking update check on the blocking pool (the updater's HTTP
    /// client is blocking; never run it on the async runtime). Re-resolves
    /// the channel from `config.toml` first unless flag/env pinned it.
    async fn check(&self) -> Result<UpdateOutcome, UpdateError> {
        let updater = Arc::clone(&self.updater);
        let channel = effective_channel(self.channel, &self.paths.config_path);
        tokio::task::spawn_blocking(move || updater.check_and_install(channel))
            .await
            .map_err(|e| UpdateError::Io(io::Error::other(e)))?
    }

    /// Pick the next check time in [`check_min`, `check_max`) and persist it
    /// (with `last_check_at = now`) so restarts don't reset the clock.
    fn schedule_next_check(&self) -> Instant {
        let delay = next_check_delay(self.config.check_min, self.config.check_max, random_u64());
        let now = OffsetDateTime::now_utc();
        let mut state = state::load(&self.paths.state_path);
        state.last_check_at = Some(now);
        state.next_check_at = Some(now + delay);
        if let Err(e) = state::save(&self.paths.state_path, &state) {
            eprintln!("intentd-sitter: failed to persist state.json: {e}");
        }
        Instant::now() + delay
    }

    /// Re-resolve the version to respawn from `state.json` (the SIGHUP
    /// semantics): picks up whatever `sitter channel --redownload`
    /// force-installed, keeping the current version when `state.json`
    /// names nothing installed.
    fn refresh_version_from_state(&self, current_version: &mut String) {
        let state = state::load(&self.paths.state_path);
        match state
            .current_version
            .filter(|v| self.paths.daemon_binary(v).exists())
        {
            Some(version) => *current_version = version,
            None => eprintln!(
                "intentd-sitter: state.json names no installed version; \
                 respawning intentd {current_version}"
            ),
        }
    }

    /// One off-schedule channel check after a failed daemon start, on top
    /// of the periodic schedule (which it re-arms, persisting `state.json`
    /// so `last_check_at` keeps moving while crash-looping and the stall is
    /// diagnosable). Reports a version to respawn when the channel published
    /// a fix — or when the check finds a version a concurrent updater (e.g.
    /// `sitter channel --redownload`) installed while the loop was crashing;
    /// [`FailedStartCheck::NothingNewer`] sends the caller to its normal
    /// backoff/give-up handling.
    ///
    /// Signals are observed while the check runs: a shutdown or restart
    /// request must not be deferred behind the manifest fetch (30s timeout)
    /// or an update download (10min timeout), or service stop/restart could
    /// exceed the service manager's own timeout exactly when the daemon is
    /// crash-looping. The abandoned check finishes on the blocking pool (or
    /// dies with the process); an install it commits is adopted by the next
    /// respawn's re-check via the concurrent-updater arm above.
    async fn check_after_failed_start(
        &self,
        current_version: &str,
        signals: &mut Signals,
        next_check_at: &mut Instant,
    ) -> FailedStartCheck {
        let check = self.check();
        tokio::pin!(check);
        let outcome = loop {
            tokio::select! {
                outcome = &mut check => break outcome,
                event = signals.recv() => match event {
                    SignalEvent::Shutdown(signal) => {
                        return FailedStartCheck::Shutdown(128 + signal);
                    }
                    #[cfg(unix)]
                    SignalEvent::Restart => {
                        eprintln!("intentd-sitter: SIGHUP received; restarting intentd");
                        return FailedStartCheck::RestartRequested;
                    }
                    // A check is already in flight, which is exactly what
                    // SIGUSR1 asks for: let it finish.
                    #[cfg(unix)]
                    SignalEvent::CheckNow => {
                        eprintln!(
                            "intentd-sitter: SIGUSR1 received; an update check is already running"
                        );
                    }
                },
            }
        };
        *next_check_at = self.schedule_next_check();
        match outcome {
            Ok(UpdateOutcome::Installed { version, previous }) => {
                eprintln!(
                    "intentd-sitter: installed intentd {version} (was {}); \
                     restarting daemon",
                    previous.as_deref().unwrap_or("none")
                );
                FailedStartCheck::Respawn(version)
            }
            // "Already current" relative to the manifest, but not the
            // version this loop has been respawning: a concurrent updater
            // installed it after the failed start. Respawn it instead of
            // sticking with (or giving up on) the crashing version.
            Ok(UpdateOutcome::AlreadyCurrent { version })
                if version != current_version && self.paths.daemon_binary(&version).exists() =>
            {
                eprintln!(
                    "intentd-sitter: found concurrently installed intentd {version} \
                     (was {current_version}); restarting daemon"
                );
                FailedStartCheck::Respawn(version)
            }
            Ok(UpdateOutcome::AlreadyCurrent { .. }) => FailedStartCheck::NothingNewer,
            Err(e) => {
                eprintln!("intentd-sitter: update check failed: {e}");
                FailedStartCheck::NothingNewer
            }
        }
    }

    /// The SIGUSR1 ("update now") check: one immediate on-demand check on
    /// top of the periodic schedule (which it re-arms, persisting
    /// `state.json` exactly like a periodic check). Selects on
    /// `signals.recv()` while the check runs (like
    /// [`Supervisor::check_after_failed_start`]) so a stalled update
    /// endpoint — the updater's download timeout is minutes long — can never
    /// make SIGTERM/SIGHUP service management hang behind an in-flight
    /// check; a signal cutting the check short leaves the schedule
    /// untouched (the abandoned check re-arms nothing).
    #[cfg(unix)]
    async fn check_now(
        &self,
        current_version: &str,
        signals: &mut Signals,
        next_check_at: &mut Instant,
    ) -> CheckNowOutcome {
        let check = self.check();
        tokio::pin!(check);
        let outcome = loop {
            tokio::select! {
                outcome = &mut check => break outcome,
                event = signals.recv() => match event {
                    SignalEvent::Shutdown(signal) => {
                        return CheckNowOutcome::Shutdown(signal);
                    }
                    SignalEvent::Restart => {
                        eprintln!("intentd-sitter: SIGHUP received; restarting intentd");
                        return CheckNowOutcome::RestartRequested;
                    }
                    // A check is already in flight, which is exactly what
                    // SIGUSR1 asks for: let it finish.
                    SignalEvent::CheckNow => {
                        eprintln!(
                            "intentd-sitter: SIGUSR1 received; an update check is already running"
                        );
                    }
                },
            }
        };
        *next_check_at = self.schedule_next_check();
        match outcome {
            Ok(UpdateOutcome::Installed { version, previous }) => {
                eprintln!(
                    "intentd-sitter: installed intentd {version} (was {}); restarting daemon",
                    previous.as_deref().unwrap_or("none")
                );
                CheckNowOutcome::Respawn(version)
            }
            Ok(UpdateOutcome::AlreadyCurrent { version })
                if version != current_version && self.paths.daemon_binary(&version).exists() =>
            {
                eprintln!(
                    "intentd-sitter: found concurrently installed intentd {version} \
                     (was {current_version}); restarting daemon"
                );
                CheckNowOutcome::Respawn(version)
            }
            Ok(UpdateOutcome::AlreadyCurrent { version }) => {
                eprintln!("intentd-sitter: intentd {version} is already current");
                CheckNowOutcome::Unchanged
            }
            Err(e) => {
                eprintln!("intentd-sitter: update check failed: {e}");
                CheckNowOutcome::Unchanged
            }
        }
    }

    /// Report a permanently failing daemon on the way out of serve mode.
    ///
    /// The caller must `return 0` right after: launchd (`KeepAlive` with
    /// `SuccessfulExit: false`) and systemd (`Restart=on-failure`) both
    /// relaunch a non-zero exit, so giving up with a failure status would
    /// only move the respawn loop one level up. A clean exit is what makes
    /// the service stay down until a human fixes the cause.
    ///
    /// The daemon inherits the sitter's stdio, so its own error message is
    /// already in the same log directly above this banner (the supervise
    /// loop also logs each failure before its re-check) — point at it
    /// rather than trying to guess the cause.
    ///
    /// "times in a row without ever staying up" is how `scripts/install.sh`
    /// and `scripts/install.ps1` recognize this banner (the install-script
    /// log contract; see the wait arm in [`Supervisor::supervise`]): reword
    /// only in lockstep with both scripts and the `install_log_contract_*`
    /// tests in `tests/supervisor_e2e.rs`.
    fn report_give_up(&self, failures: u32) {
        eprintln!(
            "intentd-sitter: intentd failed {failures} times in a row without ever staying up \
             for {:?}; this looks permanent, not transient, so the sitter is giving up instead \
             of respawning it forever",
            self.config.backoff_reset_after
        );
        eprintln!(
            "intentd-sitter: the daemon's own error is logged above — read it first. A common \
             cause is a data dir written by a newer intentd (downgrades are unsupported): \
             install the newer version again, or point INTENTD_DATA_DIR at a different dir"
        );
        eprintln!(
            "intentd-sitter: exiting 0 so the service manager leaves it stopped; once the cause \
             is fixed, start it again (`intentd serve`, `brew services start intentd`, or \
             `systemctl --user restart intentd`)"
        );
    }

    /// Sleep the current backoff delay (doubling it, capped, for next
    /// time), reporting how the sleep ended so the caller can exit on a
    /// shutdown signal or re-resolve the version from `state.json` (and
    /// reset the backoff) on a restart request (SIGHUP).
    async fn backoff_sleep(&self, backoff: &mut Duration, signals: &mut Signals) -> BackoffOutcome {
        let delay = *backoff;
        *backoff = backoff.saturating_mul(2).min(self.config.backoff_cap);
        eprintln!("intentd-sitter: respawning intentd in {delay:?}");
        tokio::select! {
            () = tokio::time::sleep(delay) => BackoffOutcome::Elapsed,
            event = signals.recv() => match event {
                SignalEvent::Shutdown(signal) => BackoffOutcome::Shutdown(128 + signal),
                #[cfg(unix)]
                SignalEvent::Restart => {
                    eprintln!("intentd-sitter: SIGHUP received; restarting intentd");
                    BackoffOutcome::RestartRequested
                }
                #[cfg(unix)]
                SignalEvent::CheckNow => {
                    eprintln!("intentd-sitter: SIGUSR1 received; checking for updates now");
                    BackoffOutcome::CheckNowRequested
                }
            },
        }
    }

    /// Sitter-initiated stop: graceful signal, then force-kill after the
    /// kill timeout. Never triggers a respawn by itself.
    async fn graceful_stop(&self, child: &mut tokio::process::Child) {
        #[cfg(unix)]
        if let Some(id) = child.id() {
            let _ = nix::sys::signal::kill(
                nix::unistd::Pid::from_raw(id.cast_signed()),
                nix::sys::signal::Signal::SIGTERM,
            );
        }
        #[cfg(not(unix))]
        let _ = child.start_kill();

        if tokio::time::timeout(self.config.kill_timeout, child.wait())
            .await
            .is_err()
        {
            eprintln!(
                "intentd-sitter: intentd did not stop within {:?}; killing",
                self.config.kill_timeout
            );
            let _ = child.kill().await;
        }
    }
}

/// How a failed-start channel re-check
/// ([`Supervisor::check_after_failed_start`]) ended.
enum FailedStartCheck {
    /// A fixed (or concurrently installed) version is available; respawn it
    /// with the backoff and failure counter reset.
    Respawn(String),
    /// Nothing newer than the crashing version; the caller proceeds to its
    /// normal backoff/give-up handling.
    NothingNewer,
    /// A restart request (SIGHUP) arrived during the check; the caller must
    /// re-resolve the version from `state.json` and reset the backoff
    /// before respawning.
    #[cfg(unix)]
    RestartRequested,
    /// A shutdown signal arrived during the check; exit with this code.
    Shutdown(i32),
}

/// How the SIGUSR1 on-demand update check ([`Supervisor::check_now`]) ended.
#[cfg(unix)]
enum CheckNowOutcome {
    /// The check installed (or found concurrently installed) a different
    /// version; respawn it with the backoff and failure counter reset.
    Respawn(String),
    /// Already current or the check failed; both non-fatal — the caller
    /// leaves the daemon alone.
    Unchanged,
    /// A restart request (SIGHUP) cut the check short; the caller must
    /// re-resolve the version from `state.json` before respawning.
    RestartRequested,
    /// A shutdown signal cut the check short; the caller shuts down with
    /// this raw signal number, exactly as if it had arrived outside the
    /// check.
    Shutdown(i32),
}

/// How a crash-backoff sleep ([`Supervisor::backoff_sleep`]) ended.
enum BackoffOutcome {
    /// The full delay elapsed; respawn the current version.
    Elapsed,
    /// A restart request (SIGHUP) cut the wait short; the caller must
    /// re-resolve the version from `state.json` and reset the backoff
    /// before respawning.
    #[cfg(unix)]
    RestartRequested,
    /// An update-now request (SIGUSR1) cut the wait short; the caller must
    /// run the check ([`Supervisor::check_now`]) and respawn — the new
    /// version when one installed, the current one otherwise — with the
    /// backoff reset.
    #[cfg(unix)]
    CheckNowRequested,
    /// A shutdown signal arrived; exit with this code.
    Shutdown(i32),
}

/// What a received signal asks the sitter to do.
enum SignalEvent {
    /// Forward the raw signal number to the child and exit with its status
    /// (SIGTERM/SIGINT; ctrl-c on windows).
    Shutdown(i32),
    /// Restart the supervised child in place without exiting the sitter
    /// (SIGHUP, sent by `intentd restart`).
    #[cfg(unix)]
    Restart,
    /// Run the update check immediately, restarting the child only when a
    /// different version installs (SIGUSR1).
    #[cfg(unix)]
    CheckNow,
}

/// Signals the sitter reacts to. `recv()` resolves to the requested
/// [`SignalEvent`] (only ctrl-c/shutdown exists on windows).
#[cfg(unix)]
struct Signals {
    term: tokio::signal::unix::Signal,
    int: tokio::signal::unix::Signal,
    hup: tokio::signal::unix::Signal,
    usr1: tokio::signal::unix::Signal,
}

#[cfg(unix)]
impl Signals {
    fn new() -> io::Result<Self> {
        use tokio::signal::unix::{signal, SignalKind};
        Ok(Self {
            term: signal(SignalKind::terminate())?,
            int: signal(SignalKind::interrupt())?,
            hup: signal(SignalKind::hangup())?,
            usr1: signal(SignalKind::user_defined1())?,
        })
    }

    async fn recv(&mut self) -> SignalEvent {
        tokio::select! {
            _ = self.term.recv() => SignalEvent::Shutdown(nix::sys::signal::Signal::SIGTERM as i32),
            _ = self.int.recv() => SignalEvent::Shutdown(nix::sys::signal::Signal::SIGINT as i32),
            _ = self.hup.recv() => SignalEvent::Restart,
            _ = self.usr1.recv() => SignalEvent::CheckNow,
        }
    }
}

#[cfg(not(unix))]
struct Signals;

#[cfg(not(unix))]
impl Signals {
    fn new() -> io::Result<Self> {
        Ok(Self)
    }

    async fn recv(&mut self) -> SignalEvent {
        let _ = tokio::signal::ctrl_c().await;
        SignalEvent::Shutdown(2) // SIGINT's conventional number
    }
}

/// Forward a shutdown signal to the child. On windows the child shares the
/// sitter's console (ctrl-c reaches it directly), so terminate it instead.
#[cfg(unix)]
fn forward_signal(child: &tokio::process::Child, signal: i32) {
    let Some(id) = child.id() else { return };
    let Ok(signal) = nix::sys::signal::Signal::try_from(signal) else {
        return;
    };
    let _ = nix::sys::signal::kill(nix::unistd::Pid::from_raw(id.cast_signed()), signal);
}

/// On windows the child shares the sitter's console, so ctrl-c is already
/// delivered to it directly; the kill-timeout fallback in the caller covers
/// a child that ignores it.
#[cfg(not(unix))]
fn forward_signal(_child: &tokio::process::Child, _signal: i32) {}

/// The sitter's exit code for a child exit status: the code when there is
/// one, the shell convention `128 + signal` for signal deaths.
fn exit_code(status: ExitStatus) -> i32 {
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        if let Some(signal) = status.signal() {
            return 128 + signal;
        }
    }
    status.code().unwrap_or(1)
}

/// Human-readable exit status for respawn logs.
fn describe_exit(status: ExitStatus) -> String {
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        if let Some(signal) = status.signal() {
            return format!("killed by signal {signal}");
        }
    }
    match status.code() {
        Some(code) => format!("exit code {code}"),
        None => "unknown exit status".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const HOUR: Duration = Duration::from_secs(60 * 60);

    #[test]
    fn defaults_are_the_documented_cadence() {
        let config = SupervisorConfig::default();
        assert_eq!(config.check_min, 12 * HOUR);
        assert_eq!(config.check_max, 24 * HOUR);
        assert_eq!(config.backoff_initial, Duration::from_secs(1));
        assert_eq!(config.backoff_cap, Duration::from_secs(60));
        assert_eq!(config.backoff_reset_after, Duration::from_secs(5 * 60));
        assert_eq!(config.give_up_after_failures, 10);
        assert_eq!(config.kill_timeout, Duration::from_secs(30));
    }

    #[test]
    fn env_overrides_apply_and_bad_values_keep_defaults() {
        let config = SupervisorConfig::from_lookup(|name| match name {
            CHECK_MIN_ENV => Some("100".to_string()),
            CHECK_MAX_ENV => Some("200".to_string()),
            BACKOFF_INITIAL_ENV => Some("not a number".to_string()),
            BACKOFF_CAP_ENV => Some(String::new()),
            KILL_TIMEOUT_ENV => Some("5000".to_string()),
            GIVE_UP_AFTER_ENV => Some("3".to_string()),
            _ => None,
        });
        assert_eq!(config.check_min, Duration::from_millis(100));
        assert_eq!(config.check_max, Duration::from_millis(200));
        assert_eq!(config.backoff_initial, Duration::from_secs(1));
        assert_eq!(config.backoff_cap, Duration::from_secs(60));
        assert_eq!(config.backoff_reset_after, Duration::from_secs(5 * 60));
        assert_eq!(config.give_up_after_failures, 3);
        assert_eq!(config.kill_timeout, Duration::from_secs(5));
    }

    #[test]
    fn effective_channel_pins_flag_env_and_follows_config_default() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("config.toml");
        std::fs::write(&config_path, "channel = \"beta\"\n").unwrap();

        // Flag/env selections stay pinned regardless of the config file.
        for origin in [ChannelOrigin::Flag, ChannelOrigin::Env] {
            let startup = ResolvedChannel {
                channel: Channel::Stable,
                origin,
            };
            assert_eq!(effective_channel(startup, &config_path), Channel::Stable);
        }

        // Config/default selections re-resolve from the file each time.
        for origin in [ChannelOrigin::Config, ChannelOrigin::Default] {
            let startup = ResolvedChannel {
                channel: Channel::Stable,
                origin,
            };
            assert_eq!(effective_channel(startup, &config_path), Channel::Beta);
        }

        // Pin removed mid-run: back to the stable default.
        std::fs::remove_file(&config_path).unwrap();
        let startup = ResolvedChannel {
            channel: Channel::Beta,
            origin: ChannelOrigin::Config,
        };
        assert_eq!(effective_channel(startup, &config_path), Channel::Stable);
    }

    /// Deterministic 12h–24h jitter sweep: every draw lands in [min, max)
    /// and the draws spread across both halves of the window.
    #[test]
    fn jitter_lands_in_half_open_window_and_spreads() {
        let (min, max) = (12 * HOUR, 24 * HOUR);
        let mut seed: u64 = 0x9e37_79b9_7f4a_7c15;
        let (mut lower_half, mut upper_half) = (0u32, 0u32);
        for _ in 0..10_000 {
            // xorshift64* — deterministic, seeded, no dependencies.
            seed ^= seed >> 12;
            seed ^= seed << 25;
            seed ^= seed >> 27;
            let draw = seed.wrapping_mul(0x2545_f491_4f6c_dd1d);
            let delay = next_check_delay(min, max, draw);
            assert!(delay >= min && delay < max, "jitter {delay:?} out of range");
            if delay < 18 * HOUR {
                lower_half += 1;
            } else {
                upper_half += 1;
            }
        }
        assert!(
            lower_half > 3_000,
            "lower half underrepresented: {lower_half}"
        );
        assert!(
            upper_half > 3_000,
            "upper half underrepresented: {upper_half}"
        );
    }

    #[test]
    fn jitter_edge_draws_hit_window_bounds() {
        let (min, max) = (12 * HOUR, 24 * HOUR);
        assert_eq!(next_check_delay(min, max, 0), min);
        let span_nanos =
            u64::try_from(max.checked_sub(min).unwrap().as_nanos()).unwrap_or(u64::MAX);
        assert_eq!(
            next_check_delay(min, max, span_nanos - 1),
            max.checked_sub(Duration::from_nanos(1)).unwrap()
        );
        assert_eq!(next_check_delay(min, max, span_nanos), min);
    }

    #[test]
    fn degenerate_jitter_window_collapses_to_min() {
        assert_eq!(next_check_delay(HOUR, HOUR, 123), HOUR);
        assert_eq!(next_check_delay(2 * HOUR, HOUR, 123), 2 * HOUR);
    }

    #[cfg(unix)]
    #[test]
    fn read_live_pid_treats_missing_garbage_and_dead_pids_as_absent() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sitter.pid");
        assert_eq!(read_live_pid(&path), None, "missing file");

        for garbage in ["", "not a pid", "-4", "0"] {
            std::fs::write(&path, garbage).unwrap();
            assert_eq!(read_live_pid(&path), None, "garbage {garbage:?}");
        }

        // A live pid (our own) is returned.
        std::fs::write(&path, format!("{}\n", std::process::id())).unwrap();
        assert_eq!(
            read_live_pid(&path),
            Some(nix::unistd::Pid::from_raw(std::process::id().cast_signed()))
        );

        // A stale pid (spawned and already reaped) is treated as absent.
        let mut dead = std::process::Command::new("true").spawn().unwrap();
        let dead_pid = dead.id();
        dead.wait().unwrap();
        std::fs::write(&path, dead_pid.to_string()).unwrap();
        assert_eq!(read_live_pid(&path), None, "stale pid must read as absent");
    }

    #[cfg(unix)]
    #[test]
    fn pidfile_guard_writes_own_pid_and_removes_on_drop() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nested").join("sitter.pid");
        let guard = PidFile::create(&path).expect("pidfile created");
        assert_eq!(
            std::fs::read_to_string(&path).unwrap().trim(),
            std::process::id().to_string()
        );
        drop(guard);
        assert!(!path.exists(), "pidfile must be removed on drop");
    }

    #[cfg(unix)]
    #[test]
    fn pidfile_guard_drop_leaves_another_sitters_pid_alone() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sitter.pid");
        let guard = PidFile::create(&path).expect("pidfile created");
        // A later serve sitter overwrites the pidfile (last writer wins);
        // this guard's drop must not delete that sitter's entry.
        std::fs::write(&path, "999999\n").unwrap();
        drop(guard);
        assert_eq!(
            std::fs::read_to_string(&path).unwrap().trim(),
            "999999",
            "drop must leave a pidfile it no longer owns in place"
        );
    }

    #[cfg(unix)]
    #[test]
    fn exit_codes_map_codes_and_signals() {
        use std::os::unix::process::ExitStatusExt;
        assert_eq!(exit_code(ExitStatus::from_raw(0)), 0);
        assert_eq!(exit_code(ExitStatus::from_raw(7 << 8)), 7);
        assert_eq!(exit_code(ExitStatus::from_raw(15)), 143); // SIGTERM
        assert_eq!(describe_exit(ExitStatus::from_raw(7 << 8)), "exit code 7");
        assert_eq!(describe_exit(ExitStatus::from_raw(9)), "killed by signal 9");
    }
}
