//! Hierarchy decisions against a plain directory tree standing in for the
//! cgroup mount. A real cgroup is exercised by the intentd containment e2e.

use std::fs;
use std::path::{Path, PathBuf};

use super::cgroup::{self, Host, Layout, Plan};
use super::{Ceiling, ContainmentConfig, WorkloadKind};

const MIB: u64 = 1024 * 1024;
const GIB: u64 = 1024 * MIB;
const PID: u32 = 4242;
const SITTER: u32 = 4241;

fn cgroup_dir(path: &Path, procs: &[u32]) {
    fs::create_dir_all(path).unwrap();
    fs::write(path.join("cgroup.controllers"), "cpu memory pids").unwrap();
    fs::write(path.join("cgroup.subtree_control"), "").unwrap();
    let procs: Vec<String> = procs.iter().map(u32::to_string).collect();
    fs::write(path.join("cgroup.procs"), procs.join("\n")).unwrap();
}

fn host(mount: &Path, start: &str) -> Host {
    Host {
        mount: mount.to_path_buf(),
        start: start.into(),
        pid: PID,
        parent_pid: SITTER,
        mem_total: Some(4 * GIB),
        query_systemd: false,
    }
}

fn config(ceiling: Ceiling) -> ContainmentConfig {
    ContainmentConfig {
        ceiling,
        instance_key: "/data".into(),
    }
}

fn read(path: PathBuf) -> String {
    fs::read_to_string(path).unwrap()
}

#[test]
fn fresh_unit_moves_the_sitter_and_daemon_and_sets_the_ceiling() {
    let mount = tempfile::tempdir().unwrap();
    let unit = mount.path().join("svc");
    cgroup_dir(&unit, &[SITTER, PID]);

    let layout = cgroup::setup(&host(mount.path(), "/svc"), &config(Ceiling::Auto)).unwrap();

    assert_eq!(read(unit.join("supervisor/cgroup.procs")), SITTER.to_string());
    assert_eq!(read(unit.join("daemon/cgroup.procs")), PID.to_string());
    assert_eq!(read(unit.join("cgroup.subtree_control")), "+memory");
    assert_eq!(read(unit.join("workloads/cgroup.subtree_control")), "+memory");
    // 4 GiB: reserve = clamp(15 %, 768 MiB, 2 GiB) = 768 MiB.
    let max = 4 * GIB - 768 * MIB;
    assert_eq!(
        layout,
        Layout {
            workloads: unit.join("workloads"),
            memory_max_bytes: max
        }
    );
    assert_eq!(read(unit.join("workloads/memory.max")), max.to_string());
    let high: u64 = read(unit.join("workloads/memory.high")).parse().unwrap();
    assert!(high < max && high >= max - max / 20 - 1, "{high} vs {max}");
}

#[test]
fn a_daemon_without_a_sitter_moves_only_itself() {
    let mount = tempfile::tempdir().unwrap();
    let unit = mount.path().join("svc");
    cgroup_dir(&unit, &[PID]);
    let plan = cgroup::plan(&host(mount.path(), "/svc"), "/data").unwrap();
    assert_eq!(
        plan,
        Plan::Fresh {
            domain: unit.clone(),
            move_parent: false
        }
    );
    cgroup::setup(&host(mount.path(), "/svc"), &config(Ceiling::Auto)).unwrap();
    assert!(!unit.join("supervisor").exists());
}

#[test]
fn a_shared_cgroup_falls_back_and_changes_nothing() {
    let mount = tempfile::tempdir().unwrap();
    let scope = mount.path().join("session.scope");
    cgroup_dir(&scope, &[1, 2, SITTER, PID]);
    let err = cgroup::setup(&host(mount.path(), "/session.scope"), &config(Ceiling::Auto))
        .unwrap_err();
    // A systemd scope without delegation is refused before the process check.
    assert!(err.contains("does not delegate"), "{err}");

    let shared = mount.path().join("shell");
    cgroup_dir(&shared, &[1, 2, SITTER, PID]);
    let err = cgroup::setup(&host(mount.path(), "/shell"), &config(Ceiling::Auto)).unwrap_err();
    assert!(err.contains("shared with 2 other process"), "{err}");
    assert!(!shared.join("daemon").exists());
    assert_eq!(read(shared.join("cgroup.subtree_control")), "");
}

#[test]
fn missing_memory_controller_falls_back_with_the_fix() {
    let mount = tempfile::tempdir().unwrap();
    let unit = mount.path().join("svc");
    cgroup_dir(&unit, &[PID]);
    fs::write(unit.join("cgroup.controllers"), "cpu pids").unwrap();
    let err = cgroup::setup(&host(mount.path(), "/svc"), &config(Ceiling::Auto)).unwrap_err();
    assert!(err.contains("memory controller") && err.contains("Delegate=memory pids"), "{err}");
}

#[test]
fn a_sitter_respawn_readopts_the_existing_hierarchy() {
    let mount = tempfile::tempdir().unwrap();
    let unit = mount.path().join("svc");
    cgroup_dir(&unit, &[SITTER, PID]);
    cgroup::setup(&host(mount.path(), "/svc"), &config(Ceiling::Auto)).unwrap();
    // The new daemon starts in the sitter's leaf.
    let respawned = Host {
        pid: PID + 10,
        ..host(mount.path(), "/svc/supervisor")
    };
    assert_eq!(
        cgroup::plan(&respawned, "/data").unwrap(),
        Plan::Readopt {
            domain: unit.clone()
        }
    );
    let layout = cgroup::setup(&respawned, &config(Ceiling::Bytes(GIB))).unwrap();
    assert_eq!(layout.workloads, unit.join("workloads"));
    assert_eq!(read(unit.join("daemon/cgroup.procs")), (PID + 10).to_string());
    assert_eq!(read(unit.join("workloads/memory.max")), GIB.to_string());
    assert_eq!(
        read(unit.join("supervisor/cgroup.procs")),
        SITTER.to_string(),
        "the sitter stays where it is"
    );
}

#[test]
fn the_root_cgroup_gets_a_dedicated_domain_and_sweeps_stale_ones() {
    let mount = tempfile::tempdir().unwrap();
    cgroup_dir(mount.path(), &[1, 77, PID]);
    let stale = mount.path().join("intentd-0000000000000001");
    fs::create_dir_all(stale.join("workloads/script-1-0")).unwrap();
    let busy = mount.path().join("intentd-0000000000000002");
    fs::create_dir_all(busy.join("daemon")).unwrap();
    fs::write(busy.join("daemon/cgroup.procs"), "99").unwrap();

    let layout = cgroup::setup(&host(mount.path(), "/"), &config(Ceiling::Auto)).unwrap();

    let domain = layout.workloads.parent().unwrap();
    let name = domain.file_name().unwrap().to_str().unwrap();
    assert!(name.starts_with("intentd-"), "{name}");
    assert_eq!(read(mount.path().join("cgroup.subtree_control")), "+memory");
    assert_eq!(read(domain.join("daemon/cgroup.procs")), PID.to_string());
    assert!(!stale.exists(), "an empty stale domain is removed");
    assert!(busy.exists(), "a busy domain is kept");
    // Same data dir, same domain.
    let again = cgroup::plan(&host(mount.path(), "/"), "/data").unwrap();
    assert_eq!(
        again,
        Plan::Root {
            domain: domain.to_path_buf()
        }
    );
}

#[test]
fn the_auto_ceiling_respects_a_lower_ancestor_limit() {
    let mount = tempfile::tempdir().unwrap();
    let container = mount.path().join("box");
    cgroup_dir(&container, &[]);
    fs::write(container.join("memory.max"), (2 * GIB).to_string()).unwrap();
    let unit = container.join("svc");
    cgroup_dir(&unit, &[PID]);
    fs::write(unit.join("memory.max"), "max").unwrap();
    let layout = cgroup::setup(
        &Host {
            mem_total: Some(64 * GIB),
            ..host(mount.path(), "/box/svc")
        },
        &config(Ceiling::Auto),
    )
    .unwrap();
    assert_eq!(layout.memory_max_bytes, 2 * GIB - 768 * MIB);
}

#[test]
fn reserve_and_high_bounds() {
    assert_eq!(cgroup::reserve_bytes(4 * GIB), 768 * MIB);
    assert_eq!(cgroup::reserve_bytes(8 * GIB), 8 * GIB / 100 * 15);
    assert_eq!(cgroup::reserve_bytes(64 * GIB), 2 * GIB);
    assert_eq!(cgroup::high_for(GIB), GIB - GIB / 20);
    assert_eq!(cgroup::high_for(256 * MIB), 224 * MIB);
}

#[test]
fn sweep_removes_only_empty_unowned_leaves() {
    let workloads = tempfile::tempdir().unwrap();
    let owned = super::Leaf::create(workloads.path(), WorkloadKind::Script, "script a").unwrap();
    let orphan = workloads.path().join("script-1-99");
    fs::create_dir(&orphan).unwrap();
    // A file stands in for a live process: `rmdir` refuses the directory.
    let busy = workloads.path().join("exec-1-98");
    fs::create_dir(&busy).unwrap();
    fs::write(busy.join("cgroup.procs"), "7").unwrap();

    super::leaf::sweep(workloads.path());

    assert!(owned.path().is_dir(), "an owned leaf stays");
    assert!(!orphan.exists(), "an empty orphan goes");
    assert!(busy.exists(), "a busy orphan stays");
    let path = owned.path().to_path_buf();
    drop(owned);
    assert!(!path.exists(), "the last handle removes its empty leaf");
}

#[test]
fn oom_kills_are_reported_once_per_increase_for_agent_leaves_only() {
    let workloads = tempfile::tempdir().unwrap();
    let agent = super::Leaf::create(workloads.path(), WorkloadKind::Agent, "agent A").unwrap();
    let script = super::Leaf::create(workloads.path(), WorkloadKind::Script, "script S").unwrap();
    let events = |n: u64| format!("low 0\nhigh 3\nmax 9\noom 2\noom_kill {n}\noom_group_kill 0\n");
    fs::write(agent.path().join("memory.events"), events(2)).unwrap();
    fs::write(script.path().join("memory.events"), events(1)).unwrap();

    let reports: Vec<_> = super::leaf::poll_oom_kills()
        .into_iter()
        .filter(|r| r.label == "agent A" || r.label == "script S")
        .collect();
    assert_eq!(reports.len(), 1, "{reports:?}");
    assert_eq!((reports[0].label.as_str(), reports[0].new_kills), ("agent A", 2));
    assert_eq!(script.oom_kills(), 1);
    assert!(super::leaf::poll_oom_kills()
        .iter()
        .all(|r| r.label != "agent A"));
    fs::write(agent.path().join("memory.events"), events(3)).unwrap();
    let again = super::leaf::poll_oom_kills();
    assert!(again.iter().any(|r| r.label == "agent A" && r.new_kills == 1));
}
