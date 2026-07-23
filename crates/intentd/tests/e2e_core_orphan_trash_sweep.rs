//! E2e for the startup orphaned-trash-dir sweep (monorepo#473): a daemon
//! crash between the locked detach rename (`<wt>.deleting-<nonce>-<attempt>`)
//! and the unlocked recursive removal leaks the trash directory. The next
//! daemon start must sweep it — and the emptied `<root>/<wsId>/` parent —
//! while leaving live worktree dirs and unrelated entries untouched.
//!
//! Spawns the REAL `intentd serve` binary with `INTENTD_WORKSPACES_DIR`
//! pointed at a pre-seeded tempdir (hermetic — never touches `$HOME`).

#![cfg(unix)]

mod common;

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use tokio::time::timeout;
use uuid::Uuid;

/// Layout for one spawned daemon: a short base dir (macOS caps UDS paths at
/// ~104 bytes) holding the data dir and the seeded workspaces root.
struct TestDirs {
    base: PathBuf,
    data_dir: PathBuf,
    workspaces: PathBuf,
}

fn make_dirs() -> TestDirs {
    let id = Uuid::new_v4().simple().to_string();
    let base = PathBuf::from("/tmp").join(format!("itdt-{}", &id[..8]));
    let data_dir = base.join("data");
    let workspaces = base.join("workspaces");
    std::fs::create_dir_all(&data_dir).expect("mkdir data dir");
    std::fs::create_dir_all(&workspaces).expect("mkdir workspaces root");
    TestDirs {
        base,
        data_dir,
        workspaces,
    }
}

fn spawn_daemon(dirs: &TestDirs) -> Child {
    let log = std::fs::File::create(dirs.data_dir.join("daemon.log")).expect("create daemon log");
    Command::new(env!("CARGO_BIN_EXE_intentd"))
        .arg("serve")
        .env("INTENTD_DATA_DIR", &dirs.data_dir)
        .env("INTENTD_WORKSPACES_DIR", &dirs.workspaces)
        .env("INTENTD_ASSERT_HERMETIC_ROOT", "1")
        .env("INTENTD_TCP_PORT", "0")
        .env_remove("INTENTD_AUTH_TOKEN")
        .stdout(Stdio::null())
        .stderr(Stdio::from(log))
        .spawn()
        .expect("spawn intentd serve")
}

/// Poll until `path` no longer exists, bounded by the shared startup budget
/// (the sweep runs as a background task after the socket is ready).
async fn await_removed(path: &Path) -> bool {
    timeout(common::daemon_startup_timeout(), async {
        loop {
            if !path.exists() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .is_ok()
}

#[tokio::test]
async fn startup_sweep_removes_orphaned_trash_dirs_only() {
    let dirs = make_dirs();

    // ws-orphan: only a leaked trash dir (with nested content) → the sweep
    // removes the trash and the now-empty parent workspace dir.
    let orphan_trash = dirs
        .workspaces
        .join("ws-orphan")
        .join("repo.deleting-abc-0");
    std::fs::create_dir_all(orphan_trash.join("nested")).expect("seed orphan trash");
    std::fs::write(orphan_trash.join("nested").join("f.txt"), "x").expect("seed trash file");

    // ws-live: a leaked trash dir NEXT TO a live (non-trash) sibling → only
    // the trash goes; the sibling and the parent survive.
    let live_ws = dirs.workspaces.join("ws-live");
    let live_trash = live_ws.join("repo.deleting-def-1");
    std::fs::create_dir_all(&live_trash).expect("seed live-ws trash");
    let live_dir = live_ws.join("repo");
    std::fs::create_dir_all(&live_dir).expect("seed live dir");
    std::fs::write(live_dir.join("keep.txt"), "keep").expect("seed live file");

    let socket = dirs.data_dir.join("intentd.sock");
    let log_path = dirs.data_dir.join("daemon.log");
    let mut daemon = common::DaemonGuard::new(spawn_daemon(&dirs), dirs.base.clone(), true);
    common::await_daemon_listening(daemon.child_mut(), &socket, &log_path).await;

    assert!(
        await_removed(&orphan_trash).await,
        "startup sweep did not remove the orphaned trash dir {}",
        orphan_trash.display()
    );
    assert!(
        await_removed(&dirs.workspaces.join("ws-orphan")).await,
        "startup sweep did not remove the emptied parent workspace dir"
    );
    assert!(
        await_removed(&live_trash).await,
        "startup sweep did not remove the trash dir next to a live sibling"
    );
    assert!(
        live_dir.join("keep.txt").exists(),
        "startup sweep must never touch live (non-trash) dirs"
    );
    assert!(live_ws.exists(), "non-empty workspace dir must survive");
}
