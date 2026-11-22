//! Per-workload leaf cgroups, the `pre_exec` join and the PTY shim (Linux).

use std::collections::HashMap;
use std::ffi::OsString;
use std::fs;
use std::os::fd::{AsRawFd, OwnedFd};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex, MutexGuard, Weak};

use super::cgroup;
use super::{OomKillReport, WorkloadKind};

/// A workload's leaf cgroup. Cloning shares it; when the last clone drops
/// the leaf is removed if empty (else the next sweep removes it once its
/// processes exit).
#[derive(Clone, Debug)]
pub struct Leaf(Arc<Inner>);

#[derive(Debug)]
struct Inner {
    path: PathBuf,
    kind: WorkloadKind,
    label: String,
}

impl Drop for Inner {
    fn drop(&mut self) {
        registry().remove(&self.path);
        let _ = fs::remove_dir(&self.path);
    }
}

struct Entry {
    leaf: Weak<Inner>,
    seen_oom_kills: u64,
}

fn registry() -> MutexGuard<'static, HashMap<PathBuf, Entry>> {
    static REGISTRY: LazyLock<Mutex<HashMap<PathBuf, Entry>>> = LazyLock::new(Mutex::default);
    REGISTRY
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

impl Leaf {
    /// Create `workloads/<kind>-<pid>-<seq>`; `None` (logged) when it cannot.
    pub(super) fn create(workloads: &Path, kind: WorkloadKind, label: &str) -> Option<Self> {
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let seq = SEQ.fetch_add(1, Ordering::Relaxed);
        let path = workloads.join(format!("{}-{}-{seq}", kind.prefix(), std::process::id()));
        let inner = Arc::new(Inner {
            path: path.clone(),
            kind,
            label: label.to_owned(),
        });
        // Registered before the directory exists, so a concurrent sweep never
        // removes it between creation and the child joining.
        registry().insert(
            path.clone(),
            Entry {
                leaf: Arc::downgrade(&inner),
                seen_oom_kills: 0,
            },
        );
        if let Err(error) = fs::create_dir(&path) {
            tracing::warn!(%error, path = %path.display(), label, "workload leaf cgroup not created; the workload runs in the shared box");
            return None;
        }
        Some(Self(inner))
    }

    /// The leaf's directory.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.0.path
    }

    /// OOM kills of processes in this leaf so far.
    #[must_use]
    pub fn oom_kills(&self) -> u64 {
        cgroup::oom_kills(&self.0.path).unwrap_or(0)
    }

    /// SIGKILL every process in the leaf, including ones that left the
    /// workload's process group or session.
    pub fn kill(&self) {
        if fs::write(self.0.path.join("cgroup.kill"), "1").is_ok() {
            return;
        }
        // Kernels before 5.14 have no `cgroup.kill`.
        for _ in 0..3 {
            let Ok(pids) = cgroup::read_procs(&self.0.path) else {
                return;
            };
            if pids.is_empty() {
                return;
            }
            for pid in pids {
                if let Ok(pid) = i32::try_from(pid) {
                    // SAFETY: plain syscall on a pid read from the leaf.
                    unsafe { libc::kill(pid, libc::SIGKILL) };
                }
            }
        }
    }
}

/// Make `cmd`'s child join `leaf` (when given) and take `score` as its
/// `oom_score_adj`, in the forked child before `exec`. Failures are ignored
/// there: containment never makes a spawn fail.
pub(super) fn install_pre_exec(cmd: &mut std::process::Command, leaf: Option<&Leaf>, score: i32) {
    // Opened here (O_CLOEXEC, like every std file) because the child may not
    // allocate; the closure keeps it open until the command is dropped.
    let procs: Option<Arc<OwnedFd>> = leaf.and_then(|leaf| {
        fs::OpenOptions::new()
            .write(true)
            .open(leaf.path().join("cgroup.procs"))
            .map(|f| Arc::new(OwnedFd::from(f)))
            .map_err(|error| {
                tracing::warn!(%error, path = %leaf.path().display(), "workload leaf cgroup not joinable");
            })
            .ok()
    });
    let mut score_buf = [0u8; 16];
    let score_text = score.to_string();
    let score_len = score_text.len().min(score_buf.len());
    score_buf[..score_len].copy_from_slice(&score_text.as_bytes()[..score_len]);
    // SAFETY: the hook only makes async-signal-safe syscalls (write, open,
    // close) on memory prepared before the fork; it allocates nothing.
    unsafe {
        cmd.pre_exec(move || {
            if let Some(fd) = &procs {
                libc::write(fd.as_raw_fd(), b"0".as_ptr().cast(), 1);
            }
            let fd = libc::open(
                c"/proc/self/oom_score_adj".as_ptr(),
                libc::O_WRONLY | libc::O_CLOEXEC,
            );
            if fd >= 0 {
                libc::write(fd, score_buf.as_ptr().cast(), score_len);
                libc::close(fd);
            }
            Ok(())
        });
    }
}

/// Remove empty leaves under `workloads` that no live [`Leaf`] owns: those a
/// previous daemon left, and those whose processes outlived their handle.
pub(super) fn sweep(workloads: &Path) {
    let Ok(entries) = fs::read_dir(workloads) else {
        return;
    };
    let dirs: Vec<PathBuf> = entries
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.is_dir())
        .collect();
    let owned: Vec<PathBuf> = {
        let registry = registry();
        dirs.iter()
            .filter(|p| registry.get(*p).is_some_and(|e| e.leaf.strong_count() > 0))
            .cloned()
            .collect()
    };
    for dir in dirs.iter().filter(|d| !owned.contains(d)) {
        cgroup::remove_tree_if_empty(dir);
    }
}

/// New OOM kills in live agent-type leaves since the previous poll.
pub(super) fn poll_oom_kills() -> Vec<OomKillReport> {
    let mut registry = registry();
    let mut reports = Vec::new();
    registry.retain(|path, entry| {
        let Some(leaf) = entry.leaf.upgrade() else {
            return false;
        };
        if leaf.kind.is_agent_like() {
            let kills = cgroup::oom_kills(path).unwrap_or(0);
            if kills > entry.seen_oom_kills {
                reports.push(OomKillReport {
                    kind: leaf.kind,
                    label: leaf.label.clone(),
                    new_kills: kills - entry.seen_oom_kills,
                });
                entry.seen_oom_kills = kills;
            }
        }
        true
    });
    reports
}

/// `cgroup-exec [--leaf DIR] [--oom-score-adj N] -- program args…`
pub(super) fn run_shim(args: Vec<OsString>) -> std::process::ExitCode {
    let mut leaf: Option<PathBuf> = None;
    let mut score: Option<OsString> = None;
    let mut rest = args.into_iter();
    loop {
        match rest.next() {
            Some(flag) if flag == "--leaf" => leaf = rest.next().map(PathBuf::from),
            Some(flag) if flag == "--oom-score-adj" => score = rest.next(),
            Some(flag) if flag == "--" => break,
            _ => {
                eprintln!("intentd cgroup-exec: usage: cgroup-exec [--leaf DIR] [--oom-score-adj N] -- program [args…]");
                return std::process::ExitCode::from(127);
            }
        }
    }
    let Some(program) = rest.next() else {
        eprintln!("intentd cgroup-exec: missing program");
        return std::process::ExitCode::from(127);
    };
    if let Some(leaf) = leaf {
        let joined = match fs::create_dir(&leaf) {
            Err(e) if e.kind() != std::io::ErrorKind::AlreadyExists => Err(e),
            _ => fs::write(leaf.join("cgroup.procs"), "0"),
        };
        if let Err(error) = joined {
            eprintln!(
                "intentd cgroup-exec: could not join {} ({error}); running outside it",
                leaf.display()
            );
        }
    }
    if let Some(score) = score {
        let _ = fs::write("/proc/self/oom_score_adj", score.as_encoded_bytes());
    }
    let error = std::process::Command::new(&program).args(rest).exec();
    eprintln!("intentd cgroup-exec: {}: {error}", program.to_string_lossy());
    std::process::ExitCode::from(127)
}
