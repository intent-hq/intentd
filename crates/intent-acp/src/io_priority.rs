//! Reduced disk-IO priority for agent children, the IO counterpart of the
//! CPU nice demotion in [`crate::spawn`].
//!
//! An agent process tree (npx → node ACP adapter → provider CLI → its tools)
//! can saturate the disk with installs, builds, and searches; the daemon that
//! drives it must keep getting its own (small, latency-sensitive) IO through.
//! Children therefore start at a less favourable IO priority than the daemon
//! — never a more favourable one, however the daemon itself was started —
//! and their descendants inherit it.
//!
//! - **Linux:** `ioprio_set(IOPRIO_WHO_PROCESS)` in `pre_exec`. [`AgentIoPriority::Low`]
//!   is the best-effort class at its least favourable level (7);
//!   [`AgentIoPriority::Idle`] (the Linux default) is the idle class. The kernel honours these
//!   only under an IO scheduler that implements priorities: BFQ honours
//!   classes and levels, mq-deadline only classes (so `Low` is a no-op
//!   there and only `Idle` separates agents from the daemon), and `none`
//!   ignores both.
//! - **macOS:** `setiopolicy_np(IOPOL_TYPE_DISK, IOPOL_SCOPE_PROCESS)` in
//!   `pre_exec`: `Low` (the macOS default) is `IOPOL_UTILITY`, `Idle` is
//!   `IOPOL_THROTTLE`.
//! - **Elsewhere** (including Windows, which only lets a process put
//!   *itself* into background IO mode) this is a no-op; the CPU demotion
//!   still applies.
//!
//! A failure to lower the priority never fails the spawn: `pre_exec` ignores
//! the syscall results (logging is not async-signal-safe there), and on Linux
//! [`io_priority_shortfall`] reports it from the parent once the child is up.

/// Env var choosing the IO priority ACP agent children start at:
/// `low`, `idle`, or `off` (leave IO priority untouched); unset means
/// [`DEFAULT_AGENT_IO_PRIORITY`].
pub const AGENT_IO_PRIORITY_ENV: &str = "INTENTD_AGENT_IO_PRIORITY";

/// How far agent children's disk IO is demoted below the daemon's.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentIoPriority {
    /// Leave the inherited IO priority untouched.
    Off,
    /// Gentle demotion: Linux best-effort level 7 (honoured by BFQ only),
    /// macOS `IOPOL_UTILITY`.
    Low,
    /// Strong demotion: Linux idle class, macOS `IOPOL_THROTTLE`. The child's
    /// IO yields to everything else, bounded by the scheduler's aging
    /// (mq-deadline's `prio_aging_expire`, BFQ's idle-class timeout).
    Idle,
}

/// IO priority agent children get when [`AGENT_IO_PRIORITY_ENV`] is unset:
/// [`AgentIoPriority::Idle`] on Linux, where the common mq-deadline
/// scheduler honours IO classes but not levels (so `Low` would separate
/// nothing), and [`AgentIoPriority::Low`] elsewhere — macOS `IOPOL_UTILITY`
/// already demotes without the heavy `IOPOL_THROTTLE` throttling.
pub const DEFAULT_AGENT_IO_PRIORITY: AgentIoPriority = if cfg!(target_os = "linux") {
    AgentIoPriority::Idle
} else {
    AgentIoPriority::Low
};

/// The IO priority ACP agent children start at: [`AGENT_IO_PRIORITY_ENV`]
/// when set to a recognised value, else [`DEFAULT_AGENT_IO_PRIORITY`].
/// Read once per process, so a misconfigured value warns once rather than
/// on every spawn.
#[must_use]
pub fn agent_io_priority() -> AgentIoPriority {
    static RESOLVED: std::sync::OnceLock<AgentIoPriority> = std::sync::OnceLock::new();
    *RESOLVED.get_or_init(|| {
        agent_io_priority_from(std::env::var(AGENT_IO_PRIORITY_ENV).ok().as_deref())
    })
}

/// [`agent_io_priority`] over an injected raw env value. Matching is
/// case-insensitive; blank means unset; anything unrecognised logs a warning
/// and falls back to the default.
fn agent_io_priority_from(raw: Option<&str>) -> AgentIoPriority {
    let Some(raw) = raw.map(str::trim).filter(|s| !s.is_empty()) else {
        return DEFAULT_AGENT_IO_PRIORITY;
    };
    match raw.to_ascii_lowercase().as_str() {
        "off" => AgentIoPriority::Off,
        "low" => AgentIoPriority::Low,
        "idle" => AgentIoPriority::Idle,
        _ => {
            tracing::warn!(
                env = AGENT_IO_PRIORITY_ENV,
                value = raw,
                default = ?DEFAULT_AGENT_IO_PRIORITY,
                "unrecognised agent IO priority (expected low, idle, or off); using the default"
            );
            DEFAULT_AGENT_IO_PRIORITY
        }
    }
}

/// Configure `cmd` so the child starts at the reduced IO priority `want`
/// (never more favourable than the IO priority it inherits from the daemon).
/// [`AgentIoPriority::Off`] and unsupported platforms leave `cmd` untouched.
pub fn apply_reduced_io_priority(cmd: &mut std::process::Command, want: AgentIoPriority) {
    if want == AgentIoPriority::Off {
        return;
    }
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::process::CommandExt;
        // SAFETY: `ioprio_get`/`ioprio_set`/`sched_getscheduler` are raw
        // syscalls with integer arguments only; they take no locks and touch
        // no heap state, so they are safe to call between fork and exec.
        unsafe {
            cmd.pre_exec(move || {
                if let Ok(inherited) = linux::get_effective(0) {
                    if let Some(target) = linux::target(inherited, want) {
                        let _ = linux::set(0, target);
                    }
                }
                Ok(())
            });
        }
    }
    #[cfg(target_os = "macos")]
    {
        use std::os::unix::process::CommandExt;
        // SAFETY: `getiopolicy_np`/`setiopolicy_np` are thin wrappers over the
        // `iopolicysys` syscall with integer arguments only, so they are safe
        // to call between fork and exec.
        unsafe {
            cmd.pre_exec(move || {
                let inherited =
                    macos::getiopolicy_np(macos::IOPOL_TYPE_DISK, macos::IOPOL_SCOPE_PROCESS);
                if inherited >= 0 {
                    if let Some(target) = macos::target(inherited, want) {
                        let _ = macos::setiopolicy_np(
                            macos::IOPOL_TYPE_DISK,
                            macos::IOPOL_SCOPE_PROCESS,
                            target,
                        );
                    }
                }
                Ok(())
            });
        }
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    let _ = cmd;
}

/// Why the freshly spawned child `pid` is NOT running at (or below) the IO
/// priority [`apply_reduced_io_priority`] derives from this process's own
/// and `want`, if it is not — `None` when it is, when `want` is `Off`, when
/// the child is already gone, or on platforms where another process's IO
/// priority cannot be read (everything but Linux).
#[must_use]
pub fn io_priority_shortfall(pid: u32, want: AgentIoPriority) -> Option<String> {
    #[cfg(target_os = "linux")]
    {
        if want == AgentIoPriority::Off {
            return None;
        }
        let parent = match linux::get_effective(0) {
            Ok(v) => v,
            Err(errno) => {
                return Some(format!(
                    "could not read the daemon's own IO priority: {}",
                    std::io::Error::from_raw_os_error(errno)
                ))
            }
        };
        let Ok(pid) = i32::try_from(pid) else {
            return None;
        };
        let got = match linux::get_effective(pid) {
            Ok(v) => v,
            Err(libc::ESRCH) => return None,
            Err(errno) => {
                return Some(format!(
                    "could not read the child's IO priority: {}",
                    std::io::Error::from_raw_os_error(errno)
                ))
            }
        };
        let expected = linux::target(parent, want).unwrap_or(parent);
        // An exited (zombie) child has already released its IO context, so
        // it reads back as `none` whatever it ran at: not a shortfall. Only
        // checked on a mismatch, so the common path reads no `/proc` file.
        (linux::rank(got) < linux::rank(expected) && !linux::has_exited(pid)).then(|| {
            format!(
                "child runs at IO priority {}, expected at most as favourable as {} (daemon at {})",
                linux::describe(got),
                linux::describe(expected),
                linux::describe(parent),
            )
        })
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (pid, want);
        None
    }
}

/// Linux `ioprio_*` plumbing (`include/uapi/linux/ioprio.h`).
#[cfg(target_os = "linux")]
pub(crate) mod linux {
    use super::AgentIoPriority;

    pub(crate) const IOPRIO_WHO_PROCESS: libc::c_long = 1;
    pub(crate) const IOPRIO_CLASS_SHIFT: u32 = 13;
    pub(crate) const IOPRIO_CLASS_NONE: i32 = 0;
    pub(crate) const IOPRIO_CLASS_RT: i32 = 1;
    pub(crate) const IOPRIO_CLASS_BE: i32 = 2;
    pub(crate) const IOPRIO_CLASS_IDLE: i32 = 3;
    /// Least favourable level within the RT/BE classes.
    pub(crate) const IOPRIO_LEVEL_LOWEST: i32 = 7;
    /// `IOPRIO_PRIO_LEVEL`: the low 3 bits. Bits 3..13 carry the kernel 6.5+
    /// hints (e.g. command duration limits), which are not part of the level.
    const IOPRIO_LEVEL_MASK: i32 = 0x7;
    const IOPRIO_CLASS_MASK: i32 = 0x7;

    /// Pack a class and level into an ioprio value.
    pub(crate) const fn value(class: i32, level: i32) -> i32 {
        (class << IOPRIO_CLASS_SHIFT) | level
    }

    pub(crate) const fn class(v: i32) -> i32 {
        (v >> IOPRIO_CLASS_SHIFT) & IOPRIO_CLASS_MASK
    }

    pub(crate) const fn level(v: i32) -> i32 {
        v & IOPRIO_LEVEL_MASK
    }

    /// The ioprio value of process `pid` (`0` = the calling process), or the
    /// raw `errno`. Only a raw syscall and thread-local `errno` are touched,
    /// so this is safe between fork and exec.
    pub(crate) fn get(pid: i32) -> Result<i32, i32> {
        // SAFETY: integer-only syscall.
        let got = unsafe {
            libc::syscall(
                libc::SYS_ioprio_get,
                IOPRIO_WHO_PROCESS,
                libc::c_long::from(pid),
            )
        };
        if got < 0 {
            Err(nix::errno::Errno::last_raw())
        } else {
            Ok(i32::try_from(got).unwrap_or(i32::MAX))
        }
    }

    /// The ioprio of process `pid` (`0` = the caller) as the kernel actually
    /// applies it: an explicit class as read, but the `NONE` class ("derived
    /// from the CPU scheduler") resolved to idle for a `SCHED_IDLE` task,
    /// which the kernel puts in the idle IO class. Without this a
    /// `SCHED_IDLE` daemon reads as best-effort and its children would be
    /// *raised* to best-effort/7. Async-signal-safe (raw syscalls only).
    pub(crate) fn get_effective(pid: i32) -> Result<i32, i32> {
        let v = get(pid)?;
        if class(v) != IOPRIO_CLASS_NONE {
            return Ok(v);
        }
        // SAFETY: integer-only syscall.
        let policy = unsafe { libc::sched_getscheduler(pid) };
        Ok(effective(v, policy))
    }

    /// [`get_effective`] over an already-read ioprio and scheduler policy
    /// (a negative policy — the read failed — leaves `v` unchanged).
    pub(crate) const fn effective(v: i32, policy: libc::c_int) -> i32 {
        if class(v) == IOPRIO_CLASS_NONE && policy == libc::SCHED_IDLE {
            value(IOPRIO_CLASS_IDLE, 0)
        } else {
            v
        }
    }

    /// Set process `pid`'s (`0` = the caller's) ioprio to `v`.
    pub(crate) fn set(pid: i32, v: i32) -> Result<(), i32> {
        // SAFETY: integer-only syscall.
        let rc = unsafe {
            libc::syscall(
                libc::SYS_ioprio_set,
                IOPRIO_WHO_PROCESS,
                libc::c_long::from(pid),
                libc::c_long::from(v),
            )
        };
        if rc < 0 {
            Err(nix::errno::Errno::last_raw())
        } else {
            Ok(())
        }
    }

    /// Whether process `pid` has exited (zombie/dead) or is gone, per the
    /// state field of `/proc/<pid>/stat` (the field after the `)` closing
    /// the command name, which may itself contain spaces or parentheses).
    pub(crate) fn has_exited(pid: i32) -> bool {
        let Ok(stat) = std::fs::read_to_string(format!("/proc/{pid}/stat")) else {
            return true;
        };
        let state = stat
            .rfind(')')
            .and_then(|i| stat[i + 1..].trim_start().chars().next());
        matches!(state, Some('Z' | 'X' | 'x') | None)
    }

    /// Total order of how unfavourable ioprio `v` is (higher = less IO).
    /// The `NONE` class means "derived from the CPU nice", which the kernel
    /// maps into the best-effort class at a level ≤ 7, so it ranks with
    /// best-effort below level 7 — never above an explicit best-effort 7.
    pub(crate) fn rank(v: i32) -> i32 {
        match class(v) {
            IOPRIO_CLASS_RT => level(v).clamp(0, IOPRIO_LEVEL_LOWEST),
            IOPRIO_CLASS_NONE => 8 + IOPRIO_LEVEL_LOWEST - 1,
            IOPRIO_CLASS_BE => 8 + level(v).clamp(0, IOPRIO_LEVEL_LOWEST),
            // IOPRIO_CLASS_IDLE, and any future class: least favourable.
            _ => 16,
        }
    }

    /// The ioprio a child inheriting `inherited` should be set to for
    /// `want`, or `None` when it should keep what it inherited (already as
    /// unfavourable as `want`, or `want` is `Off`).
    pub(crate) fn target(inherited: i32, want: AgentIoPriority) -> Option<i32> {
        let wanted = match want {
            AgentIoPriority::Off => return None,
            AgentIoPriority::Low => value(IOPRIO_CLASS_BE, IOPRIO_LEVEL_LOWEST),
            AgentIoPriority::Idle => value(IOPRIO_CLASS_IDLE, 0),
        };
        (rank(inherited) < rank(wanted)).then_some(wanted)
    }

    /// Human-readable `class/level` for logs.
    pub(crate) fn describe(v: i32) -> String {
        match class(v) {
            IOPRIO_CLASS_NONE => "none (from nice)".to_string(),
            IOPRIO_CLASS_RT => format!("realtime/{}", level(v)),
            IOPRIO_CLASS_BE => format!("best-effort/{}", level(v)),
            IOPRIO_CLASS_IDLE => "idle".to_string(),
            c => format!("class {c}/{}", level(v)),
        }
    }
}

/// macOS `setiopolicy_np(3)` plumbing (`<sys/resource.h>`); the `libc`
/// crate does not bind these.
#[cfg(target_os = "macos")]
pub(crate) mod macos {
    use super::AgentIoPriority;
    use libc::c_int;

    pub(crate) const IOPOL_TYPE_DISK: c_int = 0;
    pub(crate) const IOPOL_SCOPE_PROCESS: c_int = 0;
    #[cfg(test)]
    pub(crate) const IOPOL_DEFAULT: c_int = 0;
    #[cfg(test)]
    pub(crate) const IOPOL_IMPORTANT: c_int = 1;
    pub(crate) const IOPOL_THROTTLE: c_int = 3;
    pub(crate) const IOPOL_UTILITY: c_int = 4;
    pub(crate) const IOPOL_STANDARD: c_int = 5;

    extern "C" {
        pub(crate) fn getiopolicy_np(iotype: c_int, scope: c_int) -> c_int;
        pub(crate) fn setiopolicy_np(iotype: c_int, scope: c_int, policy: c_int) -> c_int;
    }

    /// Total order of how unfavourable policy `p` is (higher = less IO).
    pub(crate) fn rank(p: c_int) -> i32 {
        match p {
            IOPOL_STANDARD => 1,
            IOPOL_UTILITY => 2,
            IOPOL_THROTTLE => 3,
            // IOPOL_DEFAULT, IOPOL_IMPORTANT, IOPOL_PASSIVE, and anything
            // unknown: top tier.
            _ => 0,
        }
    }

    /// The policy a child inheriting `inherited` should be set to for
    /// `want`, or `None` when it should keep what it inherited.
    pub(crate) fn target(inherited: c_int, want: AgentIoPriority) -> Option<c_int> {
        let wanted = match want {
            AgentIoPriority::Off => return None,
            AgentIoPriority::Low => IOPOL_UTILITY,
            AgentIoPriority::Idle => IOPOL_THROTTLE,
        };
        (rank(inherited) < rank(wanted)).then_some(wanted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn io_priority_defaults_when_unset_or_blank() {
        assert_eq!(agent_io_priority_from(None), DEFAULT_AGENT_IO_PRIORITY);
        assert_eq!(agent_io_priority_from(Some("")), DEFAULT_AGENT_IO_PRIORITY);
        assert_eq!(
            agent_io_priority_from(Some("   ")),
            DEFAULT_AGENT_IO_PRIORITY
        );
    }

    /// Linux defaults to the idle class (mq-deadline honours classes, not
    /// levels); macOS and others to the gentle level.
    #[test]
    fn default_is_idle_on_linux_and_low_elsewhere() {
        if cfg!(target_os = "linux") {
            assert_eq!(DEFAULT_AGENT_IO_PRIORITY, AgentIoPriority::Idle);
        } else {
            assert_eq!(DEFAULT_AGENT_IO_PRIORITY, AgentIoPriority::Low);
        }
    }

    #[test]
    fn io_priority_parses_known_values_case_insensitively() {
        assert_eq!(agent_io_priority_from(Some("off")), AgentIoPriority::Off);
        assert_eq!(agent_io_priority_from(Some(" OFF ")), AgentIoPriority::Off);
        assert_eq!(agent_io_priority_from(Some("low")), AgentIoPriority::Low);
        assert_eq!(agent_io_priority_from(Some("Idle")), AgentIoPriority::Idle);
    }

    #[test]
    fn io_priority_unrecognised_falls_back_to_default() {
        assert_eq!(
            agent_io_priority_from(Some("high")),
            DEFAULT_AGENT_IO_PRIORITY
        );
        assert_eq!(agent_io_priority_from(Some("7")), DEFAULT_AGENT_IO_PRIORITY);
    }

    #[cfg(target_os = "linux")]
    mod linux_target {
        use super::super::linux::*;
        use super::super::AgentIoPriority::{Idle, Low, Off};

        const BE7: i32 = value(IOPRIO_CLASS_BE, IOPRIO_LEVEL_LOWEST);
        const IDLE: i32 = value(IOPRIO_CLASS_IDLE, 0);

        #[test]
        fn off_never_changes_anything() {
            for inherited in [
                0,
                value(IOPRIO_CLASS_RT, 0),
                value(IOPRIO_CLASS_BE, 0),
                IDLE,
            ] {
                assert_eq!(target(inherited, Off), None);
            }
        }

        #[test]
        fn low_demotes_everything_more_favourable_than_best_effort_7() {
            assert_eq!(target(value(IOPRIO_CLASS_NONE, 0), Low), Some(BE7));
            assert_eq!(target(value(IOPRIO_CLASS_RT, 0), Low), Some(BE7));
            assert_eq!(target(value(IOPRIO_CLASS_RT, 7), Low), Some(BE7));
            assert_eq!(target(value(IOPRIO_CLASS_BE, 0), Low), Some(BE7));
            assert_eq!(target(value(IOPRIO_CLASS_BE, 4), Low), Some(BE7));
        }

        #[test]
        fn low_never_raises_an_already_lower_daemon() {
            assert_eq!(target(BE7, Low), None);
            assert_eq!(target(IDLE, Low), None);
            assert_eq!(target(value(IOPRIO_CLASS_IDLE, 7), Low), None);
        }

        #[test]
        fn idle_demotes_everything_but_idle() {
            assert_eq!(target(value(IOPRIO_CLASS_NONE, 0), Idle), Some(IDLE));
            assert_eq!(target(value(IOPRIO_CLASS_RT, 3), Idle), Some(IDLE));
            assert_eq!(target(BE7, Idle), Some(IDLE));
            assert_eq!(target(IDLE, Idle), None);
        }

        #[test]
        fn rank_orders_classes_then_levels() {
            assert!(rank(value(IOPRIO_CLASS_RT, 7)) < rank(value(IOPRIO_CLASS_BE, 0)));
            assert!(rank(value(IOPRIO_CLASS_BE, 3)) < rank(value(IOPRIO_CLASS_BE, 4)));
            assert!(rank(value(IOPRIO_CLASS_NONE, 0)) < rank(BE7));
            assert!(rank(BE7) < rank(IDLE));
        }

        /// A `SCHED_IDLE` daemon with no explicit ioprio runs in the idle IO
        /// class; `low` must not raise its children to best-effort/7.
        #[test]
        fn sched_idle_none_class_counts_as_idle() {
            let none = value(IOPRIO_CLASS_NONE, 0);
            assert_eq!(effective(none, libc::SCHED_IDLE), IDLE);
            assert_eq!(target(effective(none, libc::SCHED_IDLE), Low), None);
            assert_eq!(target(effective(none, libc::SCHED_IDLE), Idle), None);
            assert_eq!(effective(none, libc::SCHED_OTHER), none);
            assert_eq!(effective(none, libc::SCHED_BATCH), none);
            assert_eq!(effective(none, -1), none, "unreadable policy");
            // An explicit class is taken as read, whatever the scheduler.
            let be0 = value(IOPRIO_CLASS_BE, 0);
            assert_eq!(effective(be0, libc::SCHED_IDLE), be0);
        }

        /// Kernel 6.5+ hint bits (3..13) are not part of the level: a hinted
        /// best-effort/0 is still best-effort/0 and still gets demoted.
        #[test]
        fn hint_bits_do_not_count_as_level() {
            let hinted_be0 = value(IOPRIO_CLASS_BE, 0) | (1 << 3);
            assert_eq!(class(hinted_be0), IOPRIO_CLASS_BE);
            assert_eq!(level(hinted_be0), 0);
            assert_eq!(rank(hinted_be0), rank(value(IOPRIO_CLASS_BE, 0)));
            assert_eq!(target(hinted_be0, Low), Some(BE7));
            assert_eq!(describe(hinted_be0), "best-effort/0");
        }

        #[test]
        fn describe_names_each_class() {
            assert_eq!(describe(0), "none (from nice)");
            assert_eq!(describe(BE7), "best-effort/7");
            assert_eq!(describe(value(IOPRIO_CLASS_RT, 2)), "realtime/2");
            assert_eq!(describe(IDLE), "idle");
        }
    }

    #[cfg(target_os = "linux")]
    mod linux_live {
        use super::super::linux::*;
        use super::super::{apply_reduced_io_priority, io_priority_shortfall, AgentIoPriority};
        use std::process::Stdio;
        use tokio::io::{AsyncBufReadExt, BufReader};
        use tokio::process::{Child, Command};

        const BE7: i32 = value(IOPRIO_CLASS_BE, IOPRIO_LEVEL_LOWEST);
        const IDLE: i32 = value(IOPRIO_CLASS_IDLE, 0);

        fn ioprio_of(pid: u32) -> i32 {
            get(i32::try_from(pid).expect("pid fits i32")).unwrap_or_else(|errno| {
                panic!(
                    "ioprio_get({pid}) failed: {}",
                    std::io::Error::from_raw_os_error(errno)
                )
            })
        }

        /// `sh` that backgrounds a sleeping grandchild, prints its pid, and
        /// waits, so both generations can be inspected from here.
        fn family() -> Command {
            let mut cmd = Command::new("sh");
            cmd.args(["-c", "sleep 30 & echo $!; wait"])
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .kill_on_drop(true);
            cmd
        }

        /// Kills the backgrounded grandchild when dropped, so the `sh`
        /// child stays alive (not a zombie) while a test inspects it.
        struct Grandchild(libc::pid_t);

        impl Drop for Grandchild {
            fn drop(&mut self) {
                // SAFETY: plain kill(2) on a pid this test started.
                unsafe {
                    libc::kill(self.0, libc::SIGKILL);
                }
            }
        }

        /// Spawn `cmd` and return the child plus the ioprio of the child and
        /// of its grandchild, and a guard that reaps the grandchild.
        async fn spawn_family(mut cmd: Command) -> (Child, i32, i32, Grandchild) {
            let mut child = cmd.spawn().expect("spawn sh");
            let child_pid = child.id().expect("child pid");
            let mut line = String::new();
            BufReader::new(child.stdout.take().expect("piped stdout"))
                .read_line(&mut line)
                .await
                .expect("read grandchild pid");
            let grandchild: libc::pid_t = line.trim().parse().expect("grandchild pid");
            let guard = Grandchild(grandchild);
            let got = (ioprio_of(child_pid), ioprio_of(grandchild.cast_unsigned()));
            (child, got.0, got.1, guard)
        }

        fn own_rank() -> i32 {
            rank(get(0).expect("own ioprio"))
        }

        #[tokio::test]
        async fn low_puts_child_and_grandchild_at_best_effort_7() {
            if own_rank() >= rank(BE7) {
                return; // the test process is itself already at or below BE7.
            }
            let mut cmd = family();
            apply_reduced_io_priority(cmd.as_std_mut(), AgentIoPriority::Low);
            let (child, kid, grandkid, _grandchild) = spawn_family(cmd).await;
            assert_eq!(describe(kid), describe(BE7));
            assert_eq!(describe(grandkid), describe(BE7), "grandchild inherits");
            assert_eq!(
                io_priority_shortfall(child.id().unwrap(), AgentIoPriority::Low),
                None
            );
            let shortfall = io_priority_shortfall(child.id().unwrap(), AgentIoPriority::Idle)
                .expect("a best-effort/7 child falls short of idle");
            assert!(shortfall.contains("best-effort/7"), "{shortfall}");
        }

        #[tokio::test]
        async fn idle_puts_child_and_grandchild_in_idle_class() {
            let mut cmd = family();
            apply_reduced_io_priority(cmd.as_std_mut(), AgentIoPriority::Idle);
            let (child, kid, grandkid, _grandchild) = spawn_family(cmd).await;
            assert_eq!(class(kid), IOPRIO_CLASS_IDLE);
            assert_eq!(class(grandkid), IOPRIO_CLASS_IDLE);
            assert_eq!(
                io_priority_shortfall(child.id().unwrap(), AgentIoPriority::Idle),
                None
            );
        }

        #[tokio::test]
        async fn off_leaves_child_at_daemon_io_priority() {
            let own = get(0).expect("own ioprio");
            let mut cmd = family();
            apply_reduced_io_priority(cmd.as_std_mut(), AgentIoPriority::Off);
            let (_child, kid, grandkid, _grandchild) = spawn_family(cmd).await;
            assert_eq!(kid, own);
            assert_eq!(grandkid, own);
        }

        /// A daemon already in the idle class (simulated by an earlier
        /// `pre_exec` that runs before the demotion's) never gets children
        /// promoted back to best-effort.
        #[tokio::test]
        async fn low_never_raises_an_idle_parent() {
            let mut cmd = family();
            // SAFETY: integer-only syscall between fork and exec.
            unsafe {
                cmd.pre_exec(|| {
                    let _ = set(0, IDLE);
                    Ok(())
                });
            }
            apply_reduced_io_priority(cmd.as_std_mut(), AgentIoPriority::Low);
            let (_child, kid, grandkid, _grandchild) = spawn_family(cmd).await;
            assert_eq!(class(kid), IOPRIO_CLASS_IDLE);
            assert_eq!(class(grandkid), IOPRIO_CLASS_IDLE);
        }

        /// Regression: a `SCHED_IDLE` daemon with no explicit ioprio is in
        /// the idle IO class, so `low` must leave its children there rather
        /// than raise them to best-effort/7 (simulated by an earlier
        /// `pre_exec` that switches the child to `SCHED_IDLE` first).
        #[tokio::test]
        async fn low_never_raises_a_sched_idle_parent() {
            if class(get(0).expect("own ioprio")) != IOPRIO_CLASS_NONE {
                return; // needs a test process with no explicit ioprio.
            }
            let mut cmd = family();
            // SAFETY: integer/stack-pointer syscall between fork and exec.
            unsafe {
                cmd.pre_exec(|| {
                    let param = libc::sched_param { sched_priority: 0 };
                    let _ = libc::sched_setscheduler(0, libc::SCHED_IDLE, &raw const param);
                    Ok(())
                });
            }
            apply_reduced_io_priority(cmd.as_std_mut(), AgentIoPriority::Low);
            let (child, kid, grandkid, _grandchild) = spawn_family(cmd).await;
            assert_eq!(
                class(kid),
                IOPRIO_CLASS_NONE,
                "not raised: {}",
                describe(kid)
            );
            assert_eq!(class(grandkid), IOPRIO_CLASS_NONE);
            let pid = child.id().expect("child pid").cast_signed();
            assert_eq!(get_effective(pid), Ok(IDLE));
        }

        /// Regression: a child that already exited but is not yet reaped
        /// (a zombie) reads back as IO class `none`, which must not be
        /// reported as a demotion shortfall (it was flaky in the live tests
        /// and would log a false warning for an agent that dies at startup).
        #[tokio::test]
        async fn shortfall_is_silent_for_an_exited_unreaped_child() {
            let mut cmd = Command::new("sh");
            cmd.args(["-c", "exit 0"]).kill_on_drop(true);
            apply_reduced_io_priority(cmd.as_std_mut(), AgentIoPriority::Idle);
            let child = cmd.spawn().expect("spawn sh");
            let pid = child.id().expect("child pid");
            let deadline = std::time::Instant::now()
                + intentd_test_support::test_timeout(std::time::Duration::from_secs(10));
            while !has_exited(pid.cast_signed()) {
                assert!(std::time::Instant::now() < deadline, "sh never exited");
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            assert_eq!(io_priority_shortfall(pid, AgentIoPriority::Idle), None);
            drop(child);
        }

        #[test]
        fn has_exited_reads_live_and_gone_processes() {
            assert!(!has_exited(std::process::id().cast_signed()));
            assert!(has_exited(0x7fff_fff0));
        }

        #[test]
        fn shortfall_is_silent_for_a_gone_child_or_off() {
            assert_eq!(
                io_priority_shortfall(0x7fff_fff0, AgentIoPriority::Low),
                None
            );
            assert_eq!(
                io_priority_shortfall(std::process::id(), AgentIoPriority::Off),
                None
            );
        }
    }

    #[cfg(target_os = "macos")]
    mod macos_target {
        use super::super::macos::*;
        use super::super::AgentIoPriority::{Idle, Low, Off};

        #[test]
        fn targets_follow_the_never_raise_rule() {
            assert_eq!(target(IOPOL_DEFAULT, Off), None);
            assert_eq!(target(IOPOL_DEFAULT, Low), Some(IOPOL_UTILITY));
            assert_eq!(target(IOPOL_IMPORTANT, Low), Some(IOPOL_UTILITY));
            assert_eq!(target(IOPOL_STANDARD, Low), Some(IOPOL_UTILITY));
            assert_eq!(target(IOPOL_UTILITY, Low), None);
            assert_eq!(target(IOPOL_THROTTLE, Low), None);
            assert_eq!(target(IOPOL_UTILITY, Idle), Some(IOPOL_THROTTLE));
            assert_eq!(target(IOPOL_THROTTLE, Idle), None);
        }
    }
}
