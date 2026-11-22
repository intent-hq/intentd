//! Building and reading the cgroup v2 hierarchy (Linux).
//!
//! Every function takes the cgroup mount from a [`Host`], so the decisions
//! are tested against a plain directory tree.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use super::{Ceiling, ContainmentConfig};

use super::{auto_ceiling_bytes, reserve_bytes};

const FIX: &str = "run intentd as root, or in its own systemd unit with Delegate=memory pids";

/// What the daemon knows about its own cgroup placement.
#[derive(Clone, Debug)]
pub(super) struct Host {
    /// The cgroup v2 mount (`/sys/fs/cgroup`).
    pub mount: PathBuf,
    /// This process's cgroup, relative to the mount (`/` for the root).
    pub start: String,
    /// This process.
    pub pid: u32,
    /// Its parent (the sitter when supervised).
    pub parent_pid: u32,
    /// `MemTotal`, when readable.
    pub mem_total: Option<u64>,
    /// Ask systemd whether a unit delegates its cgroup when the xattr is
    /// missing (off in tests).
    pub query_systemd: bool,
}

impl Host {
    /// This process's placement.
    pub fn current() -> Result<Self, String> {
        let raw = fs::read_to_string("/proc/self/cgroup")
            .map_err(|e| format!("cannot read /proc/self/cgroup: {e}"))?;
        let start = raw
            .lines()
            .find_map(|line| line.strip_prefix("0::"))
            .ok_or("cgroup v2 is not in use on this host (no unified hierarchy)")?
            .trim()
            .to_owned();
        let mount = PathBuf::from("/sys/fs/cgroup");
        if !mount.join("cgroup.controllers").is_file() {
            return Err("cgroup v2 is not mounted at /sys/fs/cgroup".into());
        }
        Ok(Self {
            mount,
            start,
            pid: std::process::id(),
            parent_pid: std::os::unix::process::parent_id(),
            mem_total: mem_total(),
            query_systemd: true,
        })
    }

    fn start_dir(&self) -> PathBuf {
        self.mount.join(self.start.trim_start_matches('/'))
    }
}

fn mem_total() -> Option<u64> {
    let meminfo = fs::read_to_string("/proc/meminfo").ok()?;
    let kb: u64 = meminfo
        .lines()
        .find_map(|l| l.strip_prefix("MemTotal:"))?
        .trim()
        .trim_end_matches("kB")
        .trim()
        .parse()
        .ok()?;
    Some(kb * 1024)
}

/// How the daemon takes over its cgroup.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Plan {
    /// The start cgroup is ours alone (plus the sitter): split it.
    Fresh { domain: PathBuf, move_parent: bool },
    /// A sitter respawn: the start cgroup is our own `supervisor` (or
    /// `daemon`) leaf; reuse its parent.
    Readopt { domain: PathBuf },
    /// The root cgroup (root in a VM): use a dedicated child domain.
    Root { domain: PathBuf },
}

impl Plan {
    fn domain(&self) -> &Path {
        match self {
            Self::Fresh { domain, .. } | Self::Readopt { domain } | Self::Root { domain } => domain,
        }
    }
}

/// The result of a successful [`setup`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Layout {
    pub workloads: PathBuf,
    pub memory_max_bytes: u64,
}

/// Decide how to take over the hierarchy, without changing anything.
pub(super) fn plan(host: &Host, instance_key: &str) -> Result<Plan, String> {
    if host.start == "/" {
        require_memory(&host.mount, "/")?;
        return Ok(Plan::Root {
            domain: host
                .mount
                .join(format!("intentd-{:016x}", fnv1a(instance_key))),
        });
    }
    let base = host.start_dir();
    if let Some(parent) = base.parent() {
        let name = base.file_name().and_then(|n| n.to_str()).unwrap_or("");
        if matches!(name, "supervisor" | "daemon") && parent.join("workloads").is_dir() {
            return Ok(Plan::Readopt {
                domain: parent.to_path_buf(),
            });
        }
    }
    require_memory(&base, &host.start)?;
    require_delegated(&base, &host.start, host.query_systemd)?;
    let procs =
        read_procs(&base).map_err(|e| format!("cannot read {}/cgroup.procs: {e}", host.start))?;
    let others = procs
        .iter()
        .filter(|&&p| p != host.pid && p != host.parent_pid)
        .count();
    if others > 0 {
        return Err(format!(
            "cgroup {} is shared with {others} other process(es); {FIX}",
            host.start
        ));
    }
    Ok(Plan::Fresh {
        domain: base,
        move_parent: procs.contains(&host.parent_pid),
    })
}

/// Build the hierarchy and set the ceiling.
pub(super) fn setup(host: &Host, config: &ContainmentConfig) -> Result<Layout, String> {
    let plan = plan(host, &config.instance_key)?;
    let domain = plan.domain().to_path_buf();
    let daemon = domain.join("daemon");
    let workloads = domain.join("workloads");
    match &plan {
        Plan::Fresh { move_parent, .. } => {
            if *move_parent {
                let supervisor = domain.join("supervisor");
                make_dir(&supervisor)?;
                write(
                    &supervisor.join("cgroup.procs"),
                    &host.parent_pid.to_string(),
                )?;
            }
            make_dir(&daemon)?;
            write(&daemon.join("cgroup.procs"), &host.pid.to_string())?;
            enable_memory(&domain)?;
        }
        Plan::Readopt { .. } => {
            make_dir(&daemon)?;
            write(&daemon.join("cgroup.procs"), &host.pid.to_string())?;
            enable_memory(&domain)?;
        }
        Plan::Root { .. } => {
            enable_memory(&host.mount)?;
            make_dir(&domain)?;
            enable_memory(&domain)?;
            make_dir(&daemon)?;
            write(&daemon.join("cgroup.procs"), &host.pid.to_string())?;
            sweep_stale_domains(&host.mount, &domain);
        }
    }
    make_dir(&workloads)?;
    enable_memory(&workloads)?;

    let box_total = box_total(host, &domain).ok_or("total memory is unknown")?;
    let reserve = reserve_bytes(box_total);
    let max = match config.ceiling {
        Ceiling::Bytes(bytes) => bytes,
        Ceiling::Auto | Ceiling::Off => auto_ceiling_bytes(box_total),
    };
    write(&workloads.join("memory.max"), &max.to_string())?;
    // No `memory.high` throttle: without swap, reclaim cannot free a
    // runaway's anonymous memory, so a throttled process stalls just under
    // the limit for minutes instead of being OOM-killed (measured: 104,921
    // high events, never killed). `max` also resets an older hierarchy.
    write(&workloads.join("memory.high"), "max")?;
    // No swap for the box: swapping workloads would thrash the host's disk
    // and stall the daemon just like running out of RAM. Best effort: the
    // file is absent when the kernel has no swap accounting.
    let _ = fs::write(workloads.join("memory.swap.max"), "0");
    // Best effort: protection only reaches as far as the parent grants.
    let _ = fs::write(daemon.join("memory.low"), reserve.to_string());
    Ok(Layout {
        workloads,
        memory_max_bytes: max,
    })
}

/// The memory the box can really use: `MemTotal`, or less when the domain
/// or an ancestor has a lower `memory.max` (a memory-limited container).
fn box_total(host: &Host, domain: &Path) -> Option<u64> {
    let mut total = host.mem_total;
    let mut dir = Some(domain);
    while let Some(d) = dir {
        if !d.starts_with(&host.mount) || d == host.mount {
            break;
        }
        if let Some(limit) = read_u64(&d.join("memory.max")) {
            total = Some(total.map_or(limit, |t| t.min(limit)));
        }
        dir = d.parent();
    }
    total
}

fn require_memory(dir: &Path, rel: &str) -> Result<(), String> {
    let controllers = fs::read_to_string(dir.join("cgroup.controllers"))
        .map_err(|e| format!("cannot read cgroup {rel}: {e}"))?;
    if controllers.split_whitespace().any(|c| c == "memory") {
        Ok(())
    } else {
        Err(format!(
            "the memory controller is not available in cgroup {rel}; {FIX}"
        ))
    }
}

/// A systemd-managed cgroup (a `.service` / `.scope`) may only be split
/// when the unit delegates it; anything else is trusted when writable.
fn require_delegated(dir: &Path, rel: &str, query_systemd: bool) -> Result<(), String> {
    let name = dir.file_name().and_then(|n| n.to_str()).unwrap_or("");
    let Some(kind) = [".service", ".scope"]
        .into_iter()
        .find(|s| name.ends_with(s))
    else {
        return Ok(());
    };
    if has_delegate_xattr(dir) {
        return Ok(());
    }
    if query_systemd && systemd_delegates(name, rel.contains("/user@")) {
        return Ok(());
    }
    let unit = name.trim_end_matches(kind);
    Err(format!(
        "systemd {} {unit} does not delegate its cgroup; add Delegate=memory pids to the unit",
        kind.trim_start_matches('.')
    ))
}

fn has_delegate_xattr(dir: &Path) -> bool {
    use std::os::unix::ffi::OsStrExt;
    let Ok(path) = std::ffi::CString::new(dir.as_os_str().as_bytes()) else {
        return false;
    };
    ["trusted.delegate", "user.delegate"].iter().any(|name| {
        let name = std::ffi::CString::new(*name).expect("static name");
        let mut buf = [0u8; 8];
        // SAFETY: valid C strings and a buffer of the length passed.
        let n = unsafe {
            libc::getxattr(
                path.as_ptr(),
                name.as_ptr(),
                buf.as_mut_ptr().cast(),
                buf.len(),
            )
        };
        n > 0 && buf[0] == b'1'
    })
}

fn systemd_delegates(unit: &str, user: bool) -> bool {
    let mut cmd = std::process::Command::new("systemctl");
    if user {
        cmd.arg("--user");
    }
    cmd.args(["show", "--property=Delegate", "--value", unit])
        .stdin(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    cmd.output()
        .is_ok_and(|o| o.status.success() && String::from_utf8_lossy(&o.stdout).trim() == "yes")
}

fn enable_memory(dir: &Path) -> Result<(), String> {
    let control = dir.join("cgroup.subtree_control");
    if fs::read_to_string(&control).is_ok_and(|s| s.split_whitespace().any(|c| c == "memory")) {
        return Ok(());
    }
    write(&control, "+memory")
}

fn make_dir(dir: &Path) -> Result<(), String> {
    match fs::create_dir(dir) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => Ok(()),
        Err(e) => Err(explain("create", dir, &e)),
    }
}

fn write(path: &Path, value: &str) -> Result<(), String> {
    fs::write(path, value).map_err(|e| explain("write", path, &e))
}

fn explain(what: &str, path: &Path, e: &io::Error) -> String {
    let hint = match e.raw_os_error() {
        Some(libc::EACCES | libc::EPERM | libc::EROFS) => format!("; {FIX}"),
        _ => String::new(),
    };
    format!("cannot {what} {}: {e}{hint}", path.display())
}

/// Remove other daemons' `intentd-*` domains under the root once empty
/// (`rmdir` refuses any cgroup that still holds a process or a child).
fn sweep_stale_domains(mount: &Path, ours: &Path) {
    let Ok(entries) = fs::read_dir(mount) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let is_domain = path
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|n| n.starts_with("intentd-"));
        // A populated domain belongs to a live daemon: removing even its
        // empty `workloads/` would leave its workloads unboxed.
        if is_domain && path != ours && path.is_dir() && !populated(&path) {
            remove_tree_if_empty(&path);
        }
    }
}

/// Whether `cgroup.events` reports a process in the cgroup or below it.
fn populated(dir: &Path) -> bool {
    fs::read_to_string(dir.join("cgroup.events"))
        .is_ok_and(|events| events.lines().any(|l| l.trim() == "populated 1"))
}

/// `rmdir` the cgroup's children, then the cgroup; stops at the first
/// cgroup that is still busy.
pub(super) fn remove_tree_if_empty(dir: &Path) -> bool {
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() && !remove_tree_if_empty(&path) {
                return false;
            }
        }
    }
    fs::remove_dir(dir).is_ok()
}

pub(super) fn read_procs(dir: &Path) -> io::Result<Vec<u32>> {
    Ok(fs::read_to_string(dir.join("cgroup.procs"))?
        .split_whitespace()
        .filter_map(|p| p.parse().ok())
        .collect())
}

/// A single-number interface file; `None` for `max` or when unreadable.
pub(super) fn read_u64(path: &Path) -> Option<u64> {
    fs::read_to_string(path).ok()?.trim().parse().ok()
}

/// `oom_kill` from the cgroup's hierarchical `memory.events`.
pub(super) fn oom_kills(dir: &Path) -> Option<u64> {
    fs::read_to_string(dir.join("memory.events"))
        .ok()?
        .lines()
        .find_map(|l| l.strip_prefix("oom_kill "))?
        .trim()
        .parse()
        .ok()
}

/// Stable across restarts and builds (unlike `DefaultHasher`).
fn fnv1a(s: &str) -> u64 {
    s.bytes().fold(0xcbf2_9ce4_8422_2325, |h, b| {
        (h ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3)
    })
}
