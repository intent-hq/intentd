//! Local daemon discovery record (§5.58) with real `intentd serve` daemons.
//!
//! Two daemons with separate `INTENTD_DATA_DIR`s share one
//! `INTENT_LOCAL_DAEMONS_DIR`: each publishes its own record matching its
//! `system.status`, rewrites it when its listener stops and restarts, removes
//! it on clean shutdown, and leaves it on a crash for the next start to sweep.

#![cfg(unix)]

use crate::common;
use crate::common::uds_exchange::uds_rpc;

use std::fmt::Write as _;
use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use serde_json::{json, Value};

struct Daemon {
    guard: common::DaemonGuard,
    pid: u32,
    data_dir: PathBuf,
    socket: PathBuf,
    log: PathBuf,
}

fn spawn(
    data_dir: &Path,
    discovery_dir: &Path,
    machine_name: Option<&str>,
    product_name: &str,
) -> Daemon {
    common::enable_ws_api(data_dir);
    if let Some(name) = machine_name {
        let path = data_dir.join("config.toml");
        let mut text = std::fs::read_to_string(&path).expect("read config");
        let _ = write!(text, "\n[sharing]\nmachineName = \"{name}\"\n");
        std::fs::write(&path, text).expect("seed machine name");
    }
    let workspaces = data_dir.join("workspaces");
    std::fs::create_dir_all(&workspaces).expect("mkdir workspaces");
    let log = data_dir.join("daemon.log");
    let mut command = common::hermetic_serve_command(data_dir);
    command
        .env("INTENT_LOCAL_DAEMONS_DIR", discovery_dir)
        .env("INTENTD_WORKSPACES_DIR", &workspaces)
        .env("INTENTD_ASSERT_HERMETIC_ROOT", "1")
        .env("INTENTD_PRODUCT_NAME", product_name)
        .stdout(Stdio::null())
        .stderr(Stdio::from(
            std::fs::File::create(&log).expect("daemon log"),
        ));
    let child = common::spawn_guarded(&mut command).expect("spawn intentd serve");
    Daemon {
        pid: child.id(),
        guard: common::DaemonGuard::process_only(child),
        data_dir: data_dir.to_path_buf(),
        socket: data_dir.join("intentd.sock"),
        log,
    }
}

fn instance_id(data_dir: &Path) -> String {
    std::fs::read_to_string(data_dir.join("local-instance-id"))
        .expect("instance id written")
        .trim()
        .to_string()
}

fn read_record(path: &Path) -> Option<Value> {
    serde_json::from_slice(&std::fs::read(path).ok()?).ok()
}

async fn await_record(path: &Path, what: &str, pred: impl Fn(&Value) -> bool) -> Value {
    let budget = common::daemon_startup_timeout();
    let deadline = tokio::time::Instant::now() + budget;
    loop {
        let record = read_record(path);
        if let Some(record) = record.as_ref().filter(|r| pred(r)) {
            return record.clone();
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "record {} never showed {what} within {budget:?}; last: {record:?}",
            path.display()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn await_gone(path: &Path, what: &str) {
    let budget = common::daemon_startup_timeout();
    let deadline = tokio::time::Instant::now() + budget;
    while path.exists() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "record {} not removed ({what}) within {budget:?}",
            path.display()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn set_listener(socket: &Path, enabled: bool) {
    let reply = uds_rpc(
        socket,
        11,
        "settings.update",
        json!({ "changes": [{ "path": "server.wsApi.enabled", "value": enabled }] }),
    )
    .await;
    assert!(
        reply["result"].is_object(),
        "settings.update server.wsApi.enabled={enabled}: {reply}"
    );
}

#[tokio::test]
async fn two_daemons_publish_update_and_remove_their_records() {
    let root = common::test_tempdir_in("/tmp", "itd-discovery-");
    let discovery = root.path().join("local-daemons");
    let dir_a = root.path().join("a");
    let dir_b = root.path().join("b");
    std::fs::create_dir_all(&dir_a).unwrap();
    std::fs::create_dir_all(&dir_b).unwrap();

    let mut a = spawn(&dir_a, &discovery, Some("Studio"), "Intent Alpha");
    let mut b = spawn(&dir_b, &discovery, None, "Intent Alpha");
    let status_a = common::await_wss_status_logged(&a.socket, &a.log).await;
    let status_b = common::await_wss_status_logged(&b.socket, &b.log).await;

    let id_a = instance_id(&a.data_dir);
    let id_b = instance_id(&b.data_dir);
    assert_ne!(id_a, id_b, "separate data dirs get separate instance ids");
    let path_a = discovery.join(format!("{id_a}.json"));
    let path_b = discovery.join(format!("{id_b}.json"));

    let port_a = status_a["result"]["port"].clone();
    let port_b = status_b["result"]["port"].clone();
    let rec_a = await_record(&path_a, "a's live port", |r| r["port"] == port_a).await;
    let rec_b = await_record(&path_b, "b's live port", |r| r["port"] == port_b).await;

    for (rec, status, id, data_dir, daemon) in [
        (&rec_a, &status_a, &id_a, &dir_a, &a),
        (&rec_b, &status_b, &id_b, &dir_b, &b),
    ] {
        let status = &status["result"];
        assert_eq!(rec["version"], 1, "{rec}");
        assert_eq!(rec["instanceId"], json!(id), "{rec}");
        assert_eq!(rec["fingerprint"], status["fingerprint"], "{rec}");
        assert_eq!(rec["daemonVersion"], status["version"], "{rec}");
        assert_eq!(rec["protocolVersion"], status["protocolVersion"], "{rec}");
        assert_eq!(
            rec["pid"],
            json!(daemon.pid),
            "pid is the daemon process: {rec}"
        );
        assert_eq!(
            rec["dataDir"],
            json!(std::fs::canonicalize(data_dir).unwrap()),
            "{rec}"
        );
        assert!(rec["startedAt"].is_string() && rec["updatedAt"].is_string());
        // Unsupported until the /local-pair endpoint lands: no flag at all.
        assert_eq!(rec["capabilities"], json!({}), "{rec}");
    }
    assert_eq!(rec_a["displayName"], "Studio");
    // machineName wins; otherwise the install's product name.
    assert_eq!(rec_b["displayName"], "Intent Alpha");
    assert_ne!(rec_a["port"], rec_b["port"]);

    // Owner-only folder and files.
    let dir_meta = std::fs::symlink_metadata(&discovery).unwrap();
    assert_eq!(dir_meta.permissions().mode() & 0o777, 0o700);
    assert_eq!(dir_meta.uid(), nix::unistd::geteuid().as_raw());
    for path in [&path_a, &path_b] {
        assert_eq!(
            std::fs::metadata(path).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }

    // Listener stops: port becomes null; restarts: port is live again.
    set_listener(&a.socket, false).await;
    common::await_wss_stopped_logged(&a.socket, &a.log).await;
    await_record(&path_a, "a's null port", |r| r["port"].is_null()).await;
    set_listener(&a.socket, true).await;
    let restarted = common::await_wss_status_logged(&a.socket, &a.log).await;
    let new_port = restarted["result"]["port"].clone();
    assert!(new_port.is_u64(), "{restarted}");
    await_record(&path_a, "a's restarted port", |r| r["port"] == new_port).await;
    // Unrelated daemon b is untouched.
    assert_eq!(read_record(&path_b).unwrap()["port"], port_b);

    // Clean shutdown removes only a's record.
    let reply = uds_rpc(&a.socket, 12, "system.shutdown", json!({})).await;
    assert!(reply.get("error").is_none(), "system.shutdown: {reply}");
    let exit = tokio::task::spawn_blocking(move || {
        let status = a.guard.child_mut().wait();
        (a, status)
    });
    let (_a, status) = tokio::time::timeout(common::daemon_startup_timeout(), exit)
        .await
        .expect("daemon a exits")
        .unwrap();
    assert!(status.expect("wait a").success());
    await_gone(&path_a, "clean exit").await;
    assert!(path_b.exists(), "b's record survives a's exit");

    // Crash leaves the record; its pid is no longer running.
    b.guard.kill_tree().expect("kill b");
    assert!(path_b.exists(), "crash leaves the record behind");
    let crashed = read_record(&path_b).unwrap();
    let crashed_pid = i32::try_from(crashed["pid"].as_u64().unwrap()).unwrap();
    assert!(nix::sys::signal::kill(nix::unistd::Pid::from_raw(crashed_pid), None).is_err());

    // The next daemon start (a again) sweeps b's stale record and republishes a.
    let a2 = spawn(&dir_a, &discovery, None, "Intent Alpha");
    let status = common::await_wss_status_logged(&a2.socket, &a2.log).await;
    assert_eq!(
        instance_id(&dir_a),
        id_a,
        "instance id is stable across restarts"
    );
    let port = status["result"]["port"].clone();
    await_record(&path_a, "a's record after restart", |r| r["port"] == port).await;
    await_gone(&path_b, "stale sweep at startup").await;
    drop(a2);
}
