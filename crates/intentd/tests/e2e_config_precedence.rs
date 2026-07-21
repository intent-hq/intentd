//! Startup flag/env precedence over config.toml (§9.8): boot a REAL
//! `intentd serve` and prove the pin layer end-to-end —
//!
//! - env-pinned keys beat the file value and report `origin: "flag"`;
//! - unpinned keys follow the file (`origin: "file"`) / defaults;
//! - `settings.update` on a pinned key rejects `-32602` naming the flag;
//! - a malformed config.toml (unknown key) refuses startup with a non-zero
//!   exit and an actionable stderr naming the offending key.

#![cfg(unix)]

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tokio::time::timeout;
use uuid::Uuid;

#[allow(dead_code)]
mod common;
use common::DaemonGuard;

/// Short data dir so `data_dir/intentd.sock` stays under `SUN_LEN`.
fn temp_data_dir() -> PathBuf {
    let id = Uuid::new_v4().simple().to_string();
    let dir = PathBuf::from("/tmp").join(format!("itd-cfgprec-{}", &id[..8]));
    std::fs::create_dir_all(&dir).expect("mkdir data dir");
    dir
}

/// Spawn `intentd serve --listen uds` with the hermetic env seams plus `env`.
fn spawn_serve(data_dir: &Path, env: &[(&str, &str)]) -> Child {
    let log = std::fs::File::create(data_dir.join("daemon.log")).expect("create daemon log");
    let workspaces_dir = data_dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_dir).expect("mkdir hermetic workspaces dir");
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_intentd"));
    cmd.arg("serve")
        .arg("--listen")
        .arg("uds")
        .env("INTENTD_DATA_DIR", data_dir)
        .env("INTENTD_WORKSPACES_DIR", &workspaces_dir)
        .env("INTENTD_ASSERT_HERMETIC_ROOT", "1")
        .stdout(Stdio::null())
        .stderr(Stdio::from(log));
    for (k, v) in env {
        cmd.env(k, v);
    }
    cmd.spawn().expect("spawn intentd serve")
}

/// Wait (up to 10s) for the daemon's UDS to accept connections.
async fn await_uds(socket: &Path) -> bool {
    timeout(Duration::from_secs(10), async {
        loop {
            if UnixStream::connect(socket).await.is_ok() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .is_ok()
}

/// One UDS JSON-RPC round-trip (one connection per call); the full frame.
async fn uds_rpc(socket: &Path, id: i64, method: &str, params: Value) -> Value {
    let stream = UnixStream::connect(socket).await.expect("connect uds");
    let (read_half, mut write_half) = stream.into_split();
    let frame = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
    let mut line = serde_json::to_string(&frame).unwrap();
    line.push('\n');
    write_half.write_all(line.as_bytes()).await.unwrap();
    write_half.flush().await.unwrap();
    let mut reader = BufReader::new(read_half);
    let mut buf = String::new();
    timeout(Duration::from_secs(5), reader.read_line(&mut buf))
        .await
        .expect("uds rpc timed out")
        .expect("read uds response");
    serde_json::from_str(buf.trim_end()).expect("invalid JSON frame")
}

/// Env pins beat config.toml; unpinned keys follow the file; a pinned key
/// rejects `settings.update` with `-32602` naming the flag; live `settings.*`
/// reads report per-key origins (`flag` | `file` | `default`).
#[tokio::test]
async fn env_pins_beat_file_and_reject_wire_mutation() {
    let data_dir = temp_data_dir();
    // File claims idleReapMinutes=7 and autoCommit=false; env pins the reap
    // knob to 3. The pinned key must read 3/flag, the file key 7 is ignored,
    // and the unpinned file key autoCommit=false stays effective.
    std::fs::write(
        data_dir.join("config.toml"),
        "[agents]\nidleReapMinutes = 7\n\n[git]\nautoCommit = false\n",
    )
    .expect("seed config.toml");

    let child = spawn_serve(&data_dir, &[("INTENTD_IDLE_REAP_MINUTES", "3")]);
    let _daemon = DaemonGuard::new(child, data_dir.clone(), true);
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");

    // Pinned key: effective value = env, origin = flag.
    let reap = uds_rpc(
        &socket,
        1,
        "settings.get",
        json!({ "path": "agents.idleReapMinutes" }),
    )
    .await;
    assert_eq!(
        reap["result"]["value"],
        json!(3.0),
        "flag beats file: {reap}"
    );
    assert_eq!(reap["result"]["origin"], json!("flag"), "{reap}");

    // Unpinned key present in the file: follows the file, origin = file.
    let auto = uds_rpc(
        &socket,
        2,
        "settings.get",
        json!({ "path": "git.autoCommit" }),
    )
    .await;
    assert_eq!(auto["result"]["value"], json!(false), "file value: {auto}");
    assert_eq!(auto["result"]["origin"], json!("file"), "{auto}");

    // Key absent from file and unpinned: schema default, origin = default.
    let rtk = uds_rpc(&socket, 3, "settings.get", json!({ "path": "rtk.enabled" })).await;
    assert_eq!(rtk["result"]["value"], json!(false), "{rtk}");
    assert_eq!(rtk["result"]["origin"], json!("default"), "{rtk}");

    // settings.update on the pinned key → -32602 naming the pinning env var.
    let update = uds_rpc(
        &socket,
        4,
        "settings.update",
        json!({ "changes": [{ "path": "agents.idleReapMinutes", "value": 10 }] }),
    )
    .await;
    assert_eq!(update["error"]["code"], json!(-32602), "{update}");
    let msg = update["error"]["message"].as_str().unwrap_or_default();
    assert!(
        msg.contains("INTENTD_IDLE_REAP_MINUTES"),
        "rejection names the flag: {update}"
    );

    // The unpinned file key stays mutable over the wire.
    let ok_update = uds_rpc(
        &socket,
        5,
        "settings.update",
        json!({ "changes": [{ "path": "git.autoCommit", "value": true }] }),
    )
    .await;
    assert!(ok_update.get("error").is_none(), "{ok_update}");
}

/// `--listen` pins `server.listenMode`: the wire reports the CLI value with
/// origin `flag` even when the file claims otherwise, and mutation rejects
/// naming `--listen`.
#[tokio::test]
async fn listen_flag_pins_listen_mode() {
    let data_dir = temp_data_dir();
    std::fs::write(
        data_dir.join("config.toml"),
        "[server]\nlistenMode = \"both\"\n",
    )
    .expect("seed config.toml");

    let child = spawn_serve(&data_dir, &[]);
    let _daemon = DaemonGuard::new(child, data_dir.clone(), true);
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");

    let get = uds_rpc(
        &socket,
        1,
        "settings.get",
        json!({ "path": "server.listenMode" }),
    )
    .await;
    assert_eq!(
        get["result"]["value"],
        json!("uds"),
        "CLI beats file: {get}"
    );
    assert_eq!(get["result"]["origin"], json!("flag"), "{get}");

    let update = uds_rpc(
        &socket,
        2,
        "settings.update",
        json!({ "changes": [{ "path": "server.listenMode", "value": "tcp" }] }),
    )
    .await;
    assert_eq!(update["error"]["code"], json!(-32602), "{update}");
    assert!(
        update["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .contains("--listen"),
        "rejection names --listen: {update}"
    );
}

/// A malformed config.toml (unknown key) refuses startup: non-zero exit and
/// a stderr error naming the offending key. A wrong-typed value refuses the
/// same way.
#[test]
fn invalid_config_refuses_startup_with_key_in_error() {
    for (body, needle) in [
        ("[agents]\nbogusKey = 1\n", "bogusKey"),
        ("[git]\nautoCommit = \"nope\"\n", "git.autoCommit"),
    ] {
        let data_dir = temp_data_dir();
        std::fs::write(data_dir.join("config.toml"), body).expect("seed config.toml");
        let out = Command::new(env!("CARGO_BIN_EXE_intentd"))
            .args(["serve", "--listen", "uds"])
            .env("INTENTD_DATA_DIR", &data_dir)
            .output()
            .expect("run intentd serve");
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            !out.status.success(),
            "serve must refuse a malformed config ({body:?}); stderr: {stderr}"
        );
        assert!(
            stderr.contains(needle),
            "stderr must name `{needle}`: {stderr}"
        );
        assert!(
            stderr.contains("config.toml"),
            "stderr must name the file: {stderr}"
        );
        let _ = std::fs::remove_dir_all(&data_dir);
    }
}

/// An invalid pin value refuses startup: INTENTD_IDLE_REAP_MINUTES parses but
/// violating the typed schema on another pinned key (out-of-range
/// INTENTD_TCP_PORT) must exit non-zero naming the flag.
#[test]
fn out_of_range_env_pin_refuses_startup() {
    let data_dir = temp_data_dir();
    let out = Command::new(env!("CARGO_BIN_EXE_intentd"))
        .args(["serve", "--listen", "uds"])
        .env("INTENTD_DATA_DIR", &data_dir)
        .env("INTENTD_TCP_PORT", "80")
        .output()
        .expect("run intentd serve");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        !out.status.success(),
        "serve must refuse an out-of-range INTENTD_TCP_PORT; stderr: {stderr}"
    );
    assert!(
        stderr.contains("INTENTD_TCP_PORT"),
        "stderr must name the flag: {stderr}"
    );
    let _ = std::fs::remove_dir_all(&data_dir);
}
