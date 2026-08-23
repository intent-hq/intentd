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

mod common;
use common::DaemonGuard;

/// Short data dir so `data_dir/intentd.sock` stays under `SUN_LEN`.
fn temp_data_dir() -> PathBuf {
    let id = Uuid::new_v4().simple().to_string();
    let dir = PathBuf::from("/tmp").join(format!("itd-cfgprec-{}", &id[..8]));
    std::fs::create_dir_all(&dir).expect("mkdir data dir");
    dir
}

/// Spawn `intentd serve` (UDS always serves) with the hermetic env seams plus `env`.
fn spawn_serve(data_dir: &Path, env: &[(&str, &str)]) -> Child {
    let log = std::fs::File::create(data_dir.join("daemon.log")).expect("create daemon log");
    let workspaces_dir = data_dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_dir).expect("mkdir hermetic workspaces dir");
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_intentd"));
    cmd.arg("serve")
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

/// Wait for the daemon's UDS to accept connections, up to the shared
/// daemon-startup budget (see `common::daemon_startup_timeout`).
async fn await_uds(socket: &Path) -> bool {
    timeout(common::daemon_startup_timeout(), async {
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
    timeout(common::rpc_read_timeout(), reader.read_line(&mut buf))
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
        "[agents]\nidleReapMinutes = 7\n\n[git]\nautoCommit = false\n\n[workspaceApi]\nmaxOutputChars = 5000\ntoonOutput = false\n",
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

    // `workspaceApi.*` from the file: effective values follow the seeded
    // config with `file` origin.
    let chars = uds_rpc(
        &socket,
        6,
        "settings.get",
        json!({ "path": "workspaceApi.maxOutputChars" }),
    )
    .await;
    assert_eq!(chars["result"]["value"], json!(5000.0), "{chars}");
    assert_eq!(chars["result"]["origin"], json!("file"), "{chars}");
    let toon = uds_rpc(
        &socket,
        7,
        "settings.get",
        json!({ "path": "workspaceApi.toonOutput" }),
    )
    .await;
    assert_eq!(toon["result"]["value"], json!(false), "{toon}");
    assert_eq!(toon["result"]["origin"], json!("file"), "{toon}");

    // A non-zero value under the 1000 floor rejects via the typed schema.
    let bad = uds_rpc(
        &socket,
        8,
        "settings.update",
        json!({ "changes": [{ "path": "workspaceApi.maxOutputChars", "value": 500 }] }),
    )
    .await;
    assert_eq!(bad["error"]["code"], json!(-32602), "{bad}");
    let msg = bad["error"]["message"].as_str().unwrap_or_default();
    assert!(
        msg.contains("workspaceApi.maxOutputChars"),
        "rejection names the key: {bad}"
    );

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

/// `server.listenMode` is retired as a settings key: a config.toml still
/// carrying it must NOT refuse startup — the daemon boots, DISCARDS the value
/// (no catalog entry remains, so `settings.get` rejects the path), strips the
/// key from the file, and the live `system.status` `listenMode` stays derived
/// from the actual listener state (UDS-only boot ⇒ `uds`) regardless of the
/// legacy file value.
#[tokio::test]
async fn legacy_listen_mode_is_discarded_and_stripped_on_boot() {
    let data_dir = temp_data_dir();
    let config_path = data_dir.join("config.toml");
    std::fs::write(&config_path, "[server]\nlistenMode = \"both\"\n").expect("seed config.toml");

    let child = spawn_serve(&data_dir, &[]);
    let _daemon = DaemonGuard::new(child, data_dir.clone(), true);
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");

    // The retired key has no catalog entry: settings.get rejects the path.
    let get = uds_rpc(
        &socket,
        1,
        "settings.get",
        json!({ "path": "server.listenMode" }),
    )
    .await;
    assert_eq!(get["error"]["code"], json!(-32602), "{get}");

    // …and so does settings.update — the key is gone from the wire surface.
    let update = uds_rpc(
        &socket,
        3,
        "settings.update",
        json!({ "changes": [{ "path": "server.listenMode", "value": "uds" }] }),
    )
    .await;
    assert_eq!(update["error"]["code"], json!(-32602), "{update}");

    // The legacy key was stripped from the file on boot.
    let rewritten = std::fs::read_to_string(&config_path).expect("config.toml readable");
    assert!(
        !rewritten.contains("listenMode"),
        "legacy key stripped: {rewritten}"
    );

    // system.status listenMode is derived from live listener state, not the
    // legacy value: no WSS listener is up (server.wsApi.enabled=false), so
    // `uds`.
    let status = uds_rpc(&socket, 2, "system.status", json!({})).await;
    assert_eq!(status["result"]["listenMode"], json!("uds"), "{status}");
}

/// One-time legacy handling: a config.toml carrying the retired
/// `model.workspaceOverrides` key and the retired `[ai]` table must NOT
/// refuse startup — the daemon boots, DISCARDS both values (neither has a
/// catalog entry since monorepo#1000), and strips both from the file with a
/// comment-preserving rewrite. Over the wire the retired path is unknown to
/// `settings.get` but tolerated-and-ignored by `settings.update` (old-client
/// compatibility). A second boot then reads the clean file untouched.
#[tokio::test]
async fn legacy_workspace_overrides_discards_and_strips_on_boot() {
    let data_dir = temp_data_dir();
    let config_path = data_dir.join("config.toml");
    std::fs::write(
        &config_path,
        "# my config\n\n[model]\n# my default\ndefault = \"m0\"\nworkspaceOverrides = { ws1 = \"m1\" }\n\n[git]\nautoCommit = false\n\n[ai]\napiUrl = \"https://api.example\"\nmodel = \"legacy-model\"\ntemperature = 0.5\n",
    )
    .expect("seed config.toml");

    // First boot: tolerated + discarded + stripped.
    let child = spawn_serve(&data_dir, &[]);
    let socket = data_dir.join("intentd.sock");
    {
        let _daemon = DaemonGuard::new(child, data_dir.clone(), false);
        assert!(await_uds(&socket).await, "daemon did not start");

        // The retired key has no catalog entry: settings.get rejects it as
        // unknown rather than serving the discarded file value.
        let get = uds_rpc(
            &socket,
            1,
            "settings.get",
            json!({ "path": "model.workspaceOverrides" }),
        )
        .await;
        assert_eq!(
            get["error"]["code"],
            json!(-32602),
            "retired path must be unknown to settings.get: {get}"
        );

        // But settings.update from an old client is tolerated-and-ignored:
        // the batch succeeds with nothing applied.
        let update = uds_rpc(
            &socket,
            2,
            "settings.update",
            json!({ "changes": [
                { "path": "model.workspaceOverrides", "value": { "ws1": "m2" } }
            ] }),
        )
        .await;
        assert_eq!(
            update["result"]["applied"],
            json!([]),
            "retired path must be ignored, not applied: {update}"
        );

        // The retired [ai] table is discarded: no catalog entry, so the wire
        // rejects the path as unknown rather than serving the file value.
        let ai = uds_rpc(&socket, 3, "settings.get", json!({ "path": "ai.apiUrl" })).await;
        assert!(
            ai.get("error").is_some(),
            "ai.apiUrl must be unknown after removal: {ai}"
        );

        // The file was stripped with comments + sibling keys preserved.
        let text = std::fs::read_to_string(&config_path).expect("read config");
        assert!(!text.contains("workspaceOverrides"), "stripped: {text}");
        assert!(!text.contains("[ai]"), "ai table stripped: {text}");
        assert!(!text.contains("apiUrl"), "ai keys stripped: {text}");
        assert!(text.contains("# my config"), "comment preserved: {text}");
        assert!(text.contains("# my default"), "comment preserved: {text}");
        assert!(text.contains("default = \"m0\""), "{text}");
        assert!(text.contains("autoCommit = false"), "{text}");
    } // guard drop kills the first daemon

    // Second boot: clean file, still no retired key on the wire.
    let stripped_text = std::fs::read_to_string(&config_path).expect("read config");
    let child = spawn_serve(&data_dir, &[]);
    let _daemon = DaemonGuard::new(child, data_dir.clone(), true);
    assert!(await_uds(&socket).await, "second boot did not start");
    let get = uds_rpc(
        &socket,
        4,
        "settings.get",
        json!({ "path": "model.workspaceOverrides" }),
    )
    .await;
    assert_eq!(
        get["error"]["code"],
        json!(-32602),
        "retired path stays unknown after restart: {get}"
    );
    // The clean second boot did not rewrite the file again.
    assert_eq!(
        std::fs::read_to_string(&config_path).expect("read config"),
        stripped_text,
        "second boot must not touch the file"
    );
}

/// A malformed config.toml (unknown key) refuses startup: non-zero exit and
/// a stderr error naming the offending key. A wrong-typed value refuses the
/// same way. The legacy-tolerated key does not weaken strictness for other
/// unknown keys, even when both appear in the same file.
#[test]
fn invalid_config_refuses_startup_with_key_in_error() {
    for (body, needle) in [
        ("[agents]\nbogusKey = 1\n", "bogusKey"),
        ("[git]\nautoCommit = \"nope\"\n", "git.autoCommit"),
        (
            "[model]\nworkspaceOverrides = {}\n\n[agents]\nbogusKey = 1\n",
            "bogusKey",
        ),
    ] {
        let data_dir = temp_data_dir();
        std::fs::write(data_dir.join("config.toml"), body).expect("seed config.toml");
        let out = Command::new(env!("CARGO_BIN_EXE_intentd"))
            .args(["serve"])
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

/// An invalid pin value refuses startup: `INTENTD_IDLE_REAP_MINUTES` parses but
/// violating the typed schema on another pinned key (out-of-range
/// `INTENTD_TCP_PORT`) must exit non-zero naming the flag.
#[test]
fn out_of_range_env_pin_refuses_startup() {
    let data_dir = temp_data_dir();
    let out = Command::new(env!("CARGO_BIN_EXE_intentd"))
        .args(["serve"])
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
