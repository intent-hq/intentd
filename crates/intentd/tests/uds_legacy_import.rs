//! Live-store legacy import over the UDS-only `system.importLegacy` RPC.

#![cfg(unix)]

mod common;

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tokio::time::timeout;
use uuid::Uuid;

struct Daemon {
    child: Child,
    data_dir: PathBuf,
    legacy_root: PathBuf,
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.data_dir);
        let _ = std::fs::remove_dir_all(&self.legacy_root);
    }
}

fn write_legacy_workspace(root: &Path, title: &str) -> PathBuf {
    let dir = root.join("ws-rpc-import");
    let metadata = dir.join(".workspace");
    std::fs::create_dir_all(metadata.join("notes")).unwrap();
    std::fs::create_dir_all(metadata.join("assets")).unwrap();
    std::fs::write(
        metadata.join("workspace.json"),
        json!({
            "id": "ws-rpc-import",
            "title": title,
            "branch": "legacy-branch",
            "status": "Active",
            "createdAt": "2025-05-01T00:00:00Z",
            "updatedAt": "2025-05-02T00:00:00Z",
            "tags": ["legacy"]
        })
        .to_string(),
    )
    .unwrap();
    std::fs::write(
        metadata.join("notes").join("extra.md"),
        "---\nid: extra\ntitle: Imported note\n---\n\nImported body\n",
    )
    .unwrap();
    std::fs::write(metadata.join("assets").join("image.png"), b"png").unwrap();
    dir
}

fn spawn_daemon(data_dir: &Path, legacy_root: &Path) -> Child {
    let log = std::fs::File::create(data_dir.join("daemon.log")).unwrap();
    let workspaces = data_dir.join("workspaces");
    std::fs::create_dir_all(&workspaces).unwrap();
    Command::new(env!("CARGO_BIN_EXE_intentd"))
        .arg("serve")
        .env("INTENTD_DATA_DIR", data_dir)
        .env("INTENTD_WORKSPACES_DIR", workspaces)
        .env("INTENTD_LEGACY_IMPORT_ROOTS", legacy_root)
        .env("INTENTD_LEGACY_APP_DIR", "")
        .env("INTENTD_ASSERT_HERMETIC_ROOT", "1")
        .stdout(Stdio::null())
        .stderr(Stdio::from(log))
        .spawn()
        .unwrap()
}

async fn await_socket(socket: &Path) {
    timeout(common::daemon_startup_timeout(), async {
        loop {
            if UnixStream::connect(socket).await.is_ok() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("daemon did not start");
}

async fn rpc(socket: &Path, params: Option<Value>) -> Value {
    let stream = UnixStream::connect(socket).await.unwrap();
    let (read_half, mut write_half) = stream.into_split();
    let mut frame = json!({
        "jsonrpc": "2.0", "id": 1, "method": "system.importLegacy"
    });
    if let Some(params) = params {
        frame["params"] = params;
    }
    let mut line = frame.to_string();
    line.push('\n');
    write_half.write_all(line.as_bytes()).await.unwrap();
    write_half.flush().await.unwrap();
    let mut response = String::new();
    timeout(
        Duration::from_secs(10),
        BufReader::new(read_half).read_line(&mut response),
    )
    .await
    .expect("import RPC timed out")
    .unwrap();
    serde_json::from_str(response.trim_end()).unwrap()
}

#[tokio::test]
async fn imports_against_live_store_with_default_and_forced_modes() {
    let id = Uuid::new_v4().simple().to_string();
    let data_dir = PathBuf::from("/tmp").join(format!("itd-li-{}", &id[..8]));
    let legacy_root = PathBuf::from("/tmp").join(format!("itd-lr-{}", &id[..8]));
    std::fs::create_dir_all(&data_dir).unwrap();
    std::fs::create_dir_all(&legacy_root).unwrap();
    write_legacy_workspace(&legacy_root, "Legacy title");
    // Existing DB path suppresses first-boot import so only the RPC drives it.
    std::fs::write(data_dir.join("intentd.db"), []).unwrap();
    let socket = data_dir.join("intentd.sock");
    let mut daemon = Daemon {
        child: spawn_daemon(&data_dir, &legacy_root),
        data_dir,
        legacy_root,
    };
    await_socket(&socket).await;

    let first = rpc(&socket, None).await;
    assert_eq!(first["jsonrpc"], "2.0", "{first}");
    assert_eq!(first["id"], 1, "{first}");
    assert_eq!(first["result"]["imported"], 1, "{first}");
    assert_eq!(first["result"]["notes"], 1, "{first}");
    assert_eq!(first["result"]["assets"], 1, "{first}");
    assert_eq!(first["result"]["comments"], 0, "{first}");
    assert_eq!(first["result"]["agents"], 0, "{first}");
    assert_eq!(first["result"]["compatibilityFailures"], false, "{first}");
    assert_eq!(first["result"]["markerWritten"], true, "{first}");

    let second = rpc(&socket, Some(json!({}))).await;
    assert_eq!(second["result"]["skipped"], 1, "{second}");
    assert_eq!(
        second["result"]["skipSummary"][0]["reason"],
        "already in DB"
    );

    write_legacy_workspace(&daemon.legacy_root, "Updated title");
    let forced = rpc(&socket, Some(json!({ "force": true }))).await;
    assert_eq!(forced["result"]["updated"], 1, "{forced}");
    assert_eq!(forced["result"]["imported"], 0, "{forced}");

    let _ = daemon.child.kill();
}
