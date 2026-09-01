//! Live-store legacy import over the UDS-only `system.importLegacy` RPC, and
//! daemon responsiveness while a first-boot import is in flight.

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

/// One workspace of the synthetic legacy tree for the in-flight test:
/// a manifest plus `notes` markdown notes.
fn write_synthetic_workspace(root: &Path, id: &str, notes: usize) {
    let metadata = root.join(id).join(".workspace");
    std::fs::create_dir_all(metadata.join("notes")).unwrap();
    std::fs::write(
        metadata.join("workspace.json"),
        json!({
            "id": id,
            "title": format!("Synthetic {id}"),
            "status": "Active",
            "createdAt": "2025-05-01T00:00:00Z",
            "updatedAt": "2025-05-02T00:00:00Z"
        })
        .to_string(),
    )
    .unwrap();
    for n in 0..notes {
        std::fs::write(
            metadata.join("notes").join(format!("note-{n}.md")),
            format!("---\nid: note-{n}\ntitle: Note {n}\n---\n\nBody of note {n}\n"),
        )
        .unwrap();
    }
}

fn spawn_daemon(data_dir: &Path, legacy_root: &Path, hold_file: Option<&Path>) -> Child {
    let log = std::fs::File::create(data_dir.join("daemon.log")).unwrap();
    let workspaces = data_dir.join("workspaces");
    std::fs::create_dir_all(&workspaces).unwrap();
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_intentd"));
    cmd.arg("serve")
        .env("INTENTD_DATA_DIR", data_dir)
        .env("INTENTD_WORKSPACES_DIR", workspaces)
        .env("INTENTD_LEGACY_IMPORT_ROOTS", legacy_root)
        .env("INTENTD_LEGACY_APP_DIR", "")
        .env("INTENTD_ASSERT_HERMETIC_ROOT", "1")
        .stdout(Stdio::null())
        .stderr(Stdio::from(log));
    if let Some(hold) = hold_file {
        cmd.env("INTENTD_TEST_LEGACY_IMPORT_HOLD_FILE", hold);
    }
    cmd.spawn().unwrap()
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

async fn rpc(socket: &Path, method: &str, params: Option<Value>) -> Value {
    let stream = UnixStream::connect(socket).await.unwrap();
    let (read_half, mut write_half) = stream.into_split();
    let mut frame = json!({
        "jsonrpc": "2.0", "id": 1, "method": method
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
        common::rpc_read_timeout(),
        BufReader::new(read_half).read_line(&mut response),
    )
    .await
    .expect("RPC timed out")
    .unwrap();
    serde_json::from_str(response.trim_end()).unwrap()
}

/// `workspace.list` over the live socket, returning how many of the synthetic
/// `ws-inflight-*` legacy workspaces have landed. Asserts the response is a
/// well-formed success envelope — this is the responsiveness probe.
async fn imported_inflight_count(socket: &Path) -> usize {
    let resp = rpc(socket, "workspace.list", None).await;
    assert_eq!(resp["jsonrpc"], "2.0", "{resp}");
    assert_eq!(resp["id"], 1, "{resp}");
    let workspaces = resp["result"]["workspaces"]
        .as_array()
        .unwrap_or_else(|| panic!("workspaces array missing: {resp}"));
    workspaces
        .iter()
        .filter(|w| {
            w["id"]
                .as_str()
                .is_some_and(|id| id.starts_with("ws-inflight-"))
        })
        .count()
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
        child: spawn_daemon(&data_dir, &legacy_root, None),
        data_dir,
        legacy_root,
    };
    await_socket(&socket).await;

    let first = rpc(&socket, "system.importLegacy", None).await;
    assert_eq!(first["jsonrpc"], "2.0", "{first}");
    assert_eq!(first["id"], 1, "{first}");
    assert_eq!(first["result"]["imported"], 1, "{first}");
    assert_eq!(first["result"]["notes"], 1, "{first}");
    assert_eq!(first["result"]["assets"], 1, "{first}");
    assert_eq!(first["result"]["comments"], 0, "{first}");
    assert_eq!(first["result"]["agents"], 0, "{first}");
    assert_eq!(first["result"]["compatibilityFailures"], false, "{first}");
    assert_eq!(first["result"]["markerWritten"], true, "{first}");

    let second = rpc(&socket, "system.importLegacy", Some(json!({}))).await;
    assert_eq!(second["result"]["skipped"], 1, "{second}");
    assert_eq!(
        second["result"]["skipSummary"][0]["reason"],
        "already in DB"
    );

    write_legacy_workspace(&daemon.legacy_root, "Updated title");
    let forced = rpc(
        &socket,
        "system.importLegacy",
        Some(json!({ "force": true })),
    )
    .await;
    assert_eq!(forced["result"]["updated"], 1, "{forced}");
    assert_eq!(forced["result"]["imported"], 0, "{forced}");

    let _ = daemon.child.kill();
}

/// The daemon must serve RPCs while the first-boot legacy import is still in
/// flight (monorepo: "E2E test: daemon serves RPCs during in-flight import").
///
/// A fresh daemon boots against a synthetic legacy tree; the
/// `INTENTD_TEST_LEGACY_IMPORT_HOLD_FILE` seam pauses the background import
/// before each workspace, so "in flight" is a deterministic state rather than
/// a race against import speed. While held, `workspace.list` must answer over
/// the live socket (with none of the legacy workspaces landed yet); deleting
/// the hold file releases the run, and polling — not sleeping — observes
/// every synthetic workspace eventually appear, the import-completion signal.
#[tokio::test]
async fn serves_rpcs_while_first_boot_import_is_in_flight() {
    const WORKSPACES: usize = 25;
    const NOTES_PER_WORKSPACE: usize = 4;
    let id = Uuid::new_v4().simple().to_string();
    let data_dir = PathBuf::from("/tmp").join(format!("itd-lif-{}", &id[..8]));
    let legacy_root = PathBuf::from("/tmp").join(format!("itd-lfr-{}", &id[..8]));
    std::fs::create_dir_all(&data_dir).unwrap();
    std::fs::create_dir_all(&legacy_root).unwrap();
    for i in 0..WORKSPACES {
        write_synthetic_workspace(
            &legacy_root,
            &format!("ws-inflight-{i:03}"),
            NOTES_PER_WORKSPACE,
        );
    }
    // The hold file pre-exists the daemon, so the import pauses before its
    // first workspace lands.
    let hold = data_dir.join("import.hold");
    std::fs::write(&hold, []).unwrap();
    // No pre-created DB file: a truly fresh boot, so the first-boot hook
    // fires and the background import starts alongside the transports.
    let socket = data_dir.join("intentd.sock");
    let mut daemon = Daemon {
        child: spawn_daemon(&data_dir, &legacy_root, Some(&hold)),
        data_dir: data_dir.clone(),
        legacy_root,
    };
    await_socket(&socket).await;

    // Import in flight (started before the transports, paused by the hold):
    // repeated RPCs must answer, and no synthetic workspace may have landed.
    for _ in 0..3 {
        assert_eq!(
            imported_inflight_count(&socket).await,
            0,
            "no legacy workspace may land while the import is held"
        );
    }

    // Release the import and poll with a timeout (no sleeps as the primary
    // synchronization) until every synthetic workspace appears.
    std::fs::remove_file(&hold).unwrap();
    timeout(common::rpc_read_timeout(), async {
        loop {
            if imported_inflight_count(&socket).await == WORKSPACES {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("legacy import did not complete after releasing the hold");

    // The imported workspaces are fully usable mid-session: their notes came
    // along with them.
    let notes = rpc(
        &socket,
        "note.list",
        Some(json!({ "workspaceId": "ws-inflight-000" })),
    )
    .await;
    let imported_notes = notes["result"]["notes"]
        .as_array()
        .unwrap_or_else(|| panic!("notes array missing: {notes}"))
        .iter()
        .filter(|n| n["id"].as_str().is_some_and(|id| id.starts_with("note-")))
        .count();
    assert_eq!(imported_notes, NOTES_PER_WORKSPACE, "{notes}");

    let _ = daemon.child.kill();
}
