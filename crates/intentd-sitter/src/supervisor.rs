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
//!    inheriting stdio and environment (the sitter injects nothing)
//! 3. after every check, pick the next check uniformly at random in
//!    [`SupervisorConfig::check_min`], [`SupervisorConfig::check_max`]) and
//!    persist it to `state.json`
//! 4. update found mid-run: download/verify/install first, then stop the
//!    child gracefully (SIGTERM + kill timeout on unix; terminate on
//!    windows) and respawn the new version with the same args
//! 5. unexpected child exit (non-zero or signal) → respawn the same version
//!    forever with exponential backoff; clean exit 0 → sitter exits 0;
//!    sitter-initiated stops never respawn. Only a `serve` invocation is
//!    babysat this way: one-shot subcommands (`status`, `stop`, `doctor`,
//!    `call`, …) legitimately exit non-zero, so they run exactly once and
//!    their exit status passes through
//! 6. SIGINT/SIGTERM (ctrl-c on windows) are forwarded to the child and the
//!    sitter exits with the child's status
//! 7. SIGHUP (unix only, sent by `intentd restart`) stops the child
//!    gracefully and respawns it on the current `state.json` version —
//!    activating a prior `sitter channel --redownload` install — without
//!    the sitter exiting. A SIGHUP that lands during a crash-backoff sleep
//!    has the same semantics: it cuts the wait short, re-resolves the
//!    version from `state.json`, and resets the backoff. Serve mode
//!    advertises itself for this via `<data_dir>/sitter/sitter.pid`,
//!    written before the supervision loop and removed on exit
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

/// Test-only env overrides (integer milliseconds) for the timing knobs in
/// [`SupervisorConfig`], so integration tests run at millisecond scale.
/// Production never sets these.
pub const CHECK_MIN_ENV: &str = "INTENTD_SITTER_CHECK_MIN_MS";
pub const CHECK_MAX_ENV: &str = "INTENTD_SITTER_CHECK_MAX_MS";
pub const BACKOFF_INITIAL_ENV: &str = "INTENTD_SITTER_BACKOFF_INITIAL_MS";
pub const BACKOFF_CAP_ENV: &str = "INTENTD_SITTER_BACKOFF_CAP_MS";
pub const BACKOFF_RESET_ENV: &str = "INTENTD_SITTER_BACKOFF_RESET_MS";
pub const KILL_TIMEOUT_ENV: &str = "INTENTD_SITTER_KILL_TIMEOUT_MS";

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
            kill_timeout: Duration::from_secs(30),
        }
    }
}

impl SupervisorConfig {
    /// Defaults with any test-only `INTENTD_SITTER_*_MS` env overrides
    /// applied.
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
        config
    }
}

/// Delay until the next update check: uniformly distributed in
/// [`min`, `max`) driven by `random` (pure, so tests can assert the
/// distribution). Degenerate ranges (`max <= min`) collapse to `min`.
pub fn next_check_delay(min: Duration, max: Duration, random: u64) -> Duration {
    let span = max.saturating_sub(min).as_nanos();
    if span == 0 {
        return min;
    }
    let offset = (random as u128) % span;
    min + Duration::from_nanos(u64::try_from(offset).unwrap_or(u64::MAX))
}

/// A random `u64` from the standard library's randomly-keyed SipHash
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
    runtime.block_on(supervisor.supervise())
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
                    match state
                        .current_version
                        .filter(|v| self.paths.daemon_binary(v).exists())
                    {
                        Some(version) => {
                            eprintln!(
                                "intentd-sitter: falling back to installed intentd {version}"
                            );
                            version
                        }
                        None => {
                            eprintln!(
                                "intentd-sitter: no intentd daemon is installed for channel {} \
                                 and the update check failed; cannot start (check network access \
                                 and retry)",
                                self.channel.channel
                            );
                            return 1;
                        }
                    }
                }
            };
            (version, next_check_at)
        } else {
            // One-shot: resolve the installed version with no updater
            // activity (no manifest fetch, no state.json write, no prune).
            let state = state::load(&self.paths.state_path);
            let version = match state
                .current_version
                .filter(|v| self.paths.daemon_binary(v).exists())
            {
                Some(version) => {
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
                }
                None => {
                    eprintln!(
                        "intentd-sitter: no intentd daemon is installed for channel {}; \
                         start the daemon first (`intentd serve` or \
                         `brew services start intentd`) so it gets installed",
                        self.channel.channel
                    );
                    return 1;
                }
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

        loop {
            let binary = self.paths.daemon_binary(&current_version);
            let mut command = tokio::process::Command::new(&binary);
            command.args(&self.passthrough).kill_on_drop(true);
            let mut child = match command.spawn() {
                Ok(child) => child,
                Err(e) => {
                    eprintln!("intentd-sitter: failed to spawn {}: {e}", binary.display());
                    if !supervised {
                        return 1;
                    }
                    match self.backoff_sleep(&mut backoff, &mut signals).await {
                        BackoffOutcome::Shutdown(code) => return code,
                        #[cfg(unix)]
                        BackoffOutcome::RestartRequested => {
                            self.refresh_version_from_state(&mut current_version);
                            backoff = self.config.backoff_initial;
                            continue;
                        }
                        BackoffOutcome::Elapsed => continue,
                    }
                }
            };
            let spawned_at = Instant::now();

            // Supervise this child until it exits or the sitter stops it.
            loop {
                tokio::select! {
                    status = child.wait() => {
                        match status {
                            Ok(status) if status.success() => return 0,
                            Ok(status) if !supervised => return exit_code(status),
                            Ok(status) => eprintln!(
                                "intentd-sitter: intentd {current_version} exited unexpectedly ({}); respawning",
                                describe_exit(status)
                            ),
                            Err(e) if !supervised => {
                                eprintln!(
                                    "intentd-sitter: failed waiting on intentd {current_version}: {e}"
                                );
                                return 1;
                            }
                            Err(e) => eprintln!(
                                "intentd-sitter: failed waiting on intentd {current_version}: {e}; respawning"
                            ),
                        }
                        if spawned_at.elapsed() >= self.config.backoff_reset_after {
                            backoff = self.config.backoff_initial;
                        }
                        match self.backoff_sleep(&mut backoff, &mut signals).await {
                            BackoffOutcome::Shutdown(code) => return code,
                            #[cfg(unix)]
                            BackoffOutcome::RestartRequested => {
                                self.refresh_version_from_state(&mut current_version);
                                backoff = self.config.backoff_initial;
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
                                break; // respawn (possibly a new version)
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

    /// Pick the next check time in [check_min, check_max) and persist it
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
            },
        }
    }

    /// Sitter-initiated stop: graceful signal, then force-kill after the
    /// kill timeout. Never triggers a respawn by itself.
    async fn graceful_stop(&self, child: &mut tokio::process::Child) {
        #[cfg(unix)]
        if let Some(id) = child.id() {
            let _ = nix::sys::signal::kill(
                nix::unistd::Pid::from_raw(id as i32),
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

/// How a crash-backoff sleep ([`Supervisor::backoff_sleep`]) ended.
enum BackoffOutcome {
    /// The full delay elapsed; respawn the current version.
    Elapsed,
    /// A restart request (SIGHUP) cut the wait short; the caller must
    /// re-resolve the version from `state.json` and reset the backoff
    /// before respawning.
    #[cfg(unix)]
    RestartRequested,
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
}

/// Signals the sitter reacts to. `recv()` resolves to the requested
/// [`SignalEvent`] (only ctrl-c/shutdown exists on windows).
#[cfg(unix)]
struct Signals {
    term: tokio::signal::unix::Signal,
    int: tokio::signal::unix::Signal,
    hup: tokio::signal::unix::Signal,
}

#[cfg(unix)]
impl Signals {
    fn new() -> io::Result<Self> {
        use tokio::signal::unix::{signal, SignalKind};
        Ok(Self {
            term: signal(SignalKind::terminate())?,
            int: signal(SignalKind::interrupt())?,
            hup: signal(SignalKind::hangup())?,
        })
    }

    async fn recv(&mut self) -> SignalEvent {
        tokio::select! {
            _ = self.term.recv() => SignalEvent::Shutdown(nix::sys::signal::Signal::SIGTERM as i32),
            _ = self.int.recv() => SignalEvent::Shutdown(nix::sys::signal::Signal::SIGINT as i32),
            _ = self.hup.recv() => SignalEvent::Restart,
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
    let _ = nix::sys::signal::kill(nix::unistd::Pid::from_raw(id as i32), signal);
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
            _ => None,
        });
        assert_eq!(config.check_min, Duration::from_millis(100));
        assert_eq!(config.check_max, Duration::from_millis(200));
        assert_eq!(config.backoff_initial, Duration::from_secs(1));
        assert_eq!(config.backoff_cap, Duration::from_secs(60));
        assert_eq!(config.backoff_reset_after, Duration::from_secs(5 * 60));
        assert_eq!(config.kill_timeout, Duration::from_millis(5000));
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
        let span_nanos = (max - min).as_nanos() as u64;
        assert_eq!(
            next_check_delay(min, max, span_nanos - 1),
            max - Duration::from_nanos(1)
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
            Some(nix::unistd::Pid::from_raw(std::process::id() as i32))
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
