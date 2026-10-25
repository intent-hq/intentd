use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

/// How often [`Barrier::sh_wait`] polls for the release file.
const POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Base hold budget for [`Barrier::sh_wait`], stretched by
/// [`test_timeout`](crate::test_timeout). Legitimate holds last seconds and
/// nextest kills a test at 180s, so a hold this long is always a leak.
pub const DEFAULT_HOLD_DEADLINE: Duration = Duration::from_secs(120);

/// A release-file barrier between a test and its fake daemon: the daemon's
/// shell script blocks at [`Barrier::sh_wait`] until the test calls
/// [`Barrier::release`], so a held state stays stable for as long as the
/// test's assertions take instead of racing a fixed `sleep`. A script can
/// also [`Barrier::sh_arrive`] just before waiting so the test can prove,
/// via [`Barrier::entered`], that the daemon actually reached the hold.
///
/// A suite that hands the release path to its fake through an environment
/// variable instead of an inlined snippet reads it from [`Barrier::path`].
///
/// A hold never outlives its test: [`Barrier::sh_wait`] gives up with
/// [`Barrier::ABANDONED_EXIT_CODE`] once the barrier's directory is gone
/// (the test's `TempDir` was dropped without a release) or after its hold
/// deadline (the test process was killed, e.g. by a nextest timeout). An
/// unbounded hold used to leak: a fake parked in a signal trap ignores
/// SIGTERM, so a test that gave up on its sitter left the fake looping
/// forever under `systemd --user` (hundreds of them on a shared dev host).
pub struct Barrier {
    path: PathBuf,
    deadline: Duration,
}

impl Barrier {
    /// Exit status of a script whose [`Barrier::sh_wait`] gave up instead of
    /// being released (the `timeout(1)` convention). Distinct from every
    /// status a fake daemon reports on purpose, so an abandoned hold fails
    /// loudly instead of passing for a deliberate exit.
    pub const ABANDONED_EXIT_CODE: i32 = 124;

    /// A barrier whose release file is `<data_dir>/barrier-<name>`, holding
    /// for at most [`DEFAULT_HOLD_DEADLINE`] stretched by
    /// [`test_timeout`](crate::test_timeout).
    #[must_use]
    pub fn new(data_dir: &Path, name: &str) -> Self {
        Self {
            path: data_dir.join(format!("barrier-{name}")),
            deadline: crate::test_timeout(DEFAULT_HOLD_DEADLINE),
        }
    }

    /// Replace the hold deadline (taken as is, not stretched). The snippet
    /// counts polls, so under load a hold may run longer than `deadline`,
    /// never shorter.
    #[must_use]
    pub fn with_deadline(mut self, deadline: Duration) -> Self {
        self.deadline = deadline;
        self
    }

    /// The release file a script waits on (see [`Barrier::sh_wait`]).
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Let every script blocked in [`Barrier::sh_wait`] proceed.
    ///
    /// # Panics
    ///
    /// Panics when the release file cannot be created (the test's data
    /// directory is gone).
    pub fn release(&self) {
        fs::write(&self.path, b"").unwrap();
    }

    /// Shell snippet that blocks until the barrier is released, or exits
    /// the script with [`Barrier::ABANDONED_EXIT_CODE`] (after one line on
    /// stderr) once the barrier's directory is gone or the hold deadline has
    /// passed. POSIX `sh`; it uses only double quotes, so it also fits inside
    /// a single-quoted `trap` handler.
    #[must_use]
    pub fn sh_wait(&self) -> String {
        let path = self.path.display();
        let dir = self.path.parent().unwrap_or(Path::new(".")).display();
        let polls = self.max_polls();
        let code = Self::ABANDONED_EXIT_CODE;
        let held = self.deadline.as_secs_f64();
        let interval = POLL_INTERVAL.as_secs_f64();
        let polled = "_intentd_barrier_polls";
        let abandon = |why: &str| {
            format!(
                "echo \"intentd-test-support: barrier {path} abandoned: {why}\" >&2; exit {code}"
            )
        };
        // timing-guard: poll interval
        format!(
            "{polled}=0; while [ ! -e \"{path}\" ]; do \
             if [ ! -d \"{dir}\" ]; then {gone}; fi; \
             if [ \"${polled}\" -ge {polls} ]; then {late}; fi; \
             {polled}=$(({polled} + 1)); \
             sleep {interval}; done",
            gone = abandon("its directory is gone"),
            late = abandon(&format!("still held after {held}s")),
        )
    }

    /// Shell snippet that marks the barrier as reached (see [`Barrier::entered`]).
    #[must_use]
    pub fn sh_arrive(&self) -> String {
        format!(": > \"{}\"", self.entered_path().display())
    }

    /// Whether a script has run [`Barrier::sh_arrive`].
    #[must_use]
    pub fn entered(&self) -> bool {
        self.entered_path().exists()
    }

    /// Polls [`Barrier::sh_wait`] makes before giving up (at least one).
    fn max_polls(&self) -> u64 {
        let polls = self.deadline.as_nanos().div_ceil(POLL_INTERVAL.as_nanos());
        u64::try_from(polls).unwrap_or(u64::MAX).max(1)
    }

    fn entered_path(&self) -> PathBuf {
        let mut path = self.path.clone().into_os_string();
        path.push(".entered");
        path.into()
    }
}
