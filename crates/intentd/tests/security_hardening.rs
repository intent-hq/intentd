//! §11.3 security-review regression tests.
//!
//! These cover two checklist items that earlier cycles implemented but did not
//! yet assert end-to-end:
//!
//! - **UDS socket perms == 0600** (§11.3 item 1): [`serve_uds`] must bind the
//!   Unix-domain socket with owner-only permissions so no other local user can
//!   reach the control transport.
//! - **`doctor` reports secret PRESENCE as a boolean only** (§11.3 item 8): the
//!   `intentd doctor` diagnostics must report that a GitHub token is present
//!   without ever echoing the token value into its output.

#![cfg(unix)]

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use intent_core::WorkspaceApi;
use intent_services::{EventBus, Services};
use intent_store::Store;
use intent_transport::{serve_uds, MAX_INBOUND_MESSAGE_BYTES};
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

/// Short `/tmp` data dir so `data_dir/intentd.sock` fits within `SUN_LEN`
/// (~104 bytes on macOS); a deep `temp_dir()` would overflow the UDS bind.
fn temp_data_dir(tag: &str) -> PathBuf {
    let id = uuid::Uuid::new_v4().simple().to_string();
    let dir = PathBuf::from("/tmp").join(format!("itd-sec-{tag}-{}", &id[..8]));
    std::fs::create_dir_all(&dir).expect("mkdir data dir");
    dir
}

/// Wait (up to ~1s) for the listener to create the socket file.
async fn await_socket(socket: &Path) -> bool {
    for _ in 0..50 {
        if socket.exists() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    false
}

#[tokio::test]
async fn uds_socket_is_owner_only_0600() {
    let dir = temp_data_dir("uds");
    let socket = dir.join("intentd.sock");

    let store = Store::open(&dir.join("intentd.db"))
        .await
        .expect("open store");
    let bus = EventBus::new(store.clone());
    let services: Arc<dyn WorkspaceApi> = Arc::new(Services::new(store).with_workspaces_root(
        std::env::temp_dir().join(format!("itd-hermetic-ws-{}", uuid::Uuid::new_v4())),
    ));

    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let socket_for_task = socket.clone();
    let server = tokio::spawn(async move {
        serve_uds(services, bus, &socket_for_task, None, async move {
            let _ = rx.await;
        })
        .await
        .expect("serve uds");
    });

    assert!(await_socket(&socket).await, "socket never appeared");

    let mode = std::fs::metadata(&socket)
        .expect("stat socket")
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(
        mode, 0o600,
        "UDS control socket must be owner-only 0600 (§11.3), got {mode:o}",
    );

    let _ = tx.send(());
    let _ = server.await;
    std::fs::remove_dir_all(&dir).ok();
}

/// Transport size-limit regression (monorepo#472): a single UDS line past the
/// 40 MiB cap yields a `-32600` error frame with `id: null` and the connection
/// closes, while an under-limit frame on a fresh connection still round-trips.
#[tokio::test]
async fn uds_oversized_line_rejected_and_connection_closed() {
    let dir = temp_data_dir("maxline");
    let socket = dir.join("intentd.sock");

    let store = Store::open(&dir.join("intentd.db"))
        .await
        .expect("open store");
    let bus = EventBus::new(store.clone());
    let services: Arc<dyn WorkspaceApi> = Arc::new(Services::new(store).with_workspaces_root(
        std::env::temp_dir().join(format!("itd-hermetic-ws-{}", uuid::Uuid::new_v4())),
    ));

    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let socket_for_task = socket.clone();
    let server = tokio::spawn(async move {
        serve_uds(services, bus, &socket_for_task, None, async move {
            let _ = rx.await;
        })
        .await
        .expect("serve uds");
    });
    assert!(await_socket(&socket).await, "socket never appeared");

    // Over-limit: one line just past the cap. The daemon must answer -32600
    // (id null) and close without buffering the whole line.
    let stream = UnixStream::connect(&socket).await.expect("connect");
    let (read_half, mut write_half) = stream.into_split();
    let payload = vec![b'a'; MAX_INBOUND_MESSAGE_BYTES + 1024];
    // The daemon stops reading (and closes) once the limit is hit, so any of
    // these writes may fail mid-payload with EPIPE/ECONNRESET — that's fine,
    // the assertion is on the error frame the daemon sent first.
    let _ = write_half.write_all(&payload).await;
    let _ = write_half.write_all(b"\n").await;
    let _ = write_half.flush().await;
    let mut reader = BufReader::new(read_half);
    let mut line = String::new();
    reader.read_line(&mut line).await.expect("read error frame");
    let v: Value = serde_json::from_str(line.trim()).expect("error frame json");
    assert_eq!(
        v["error"]["code"].as_i64(),
        Some(-32600),
        "oversized frame must be -32600: {v}"
    );
    assert!(v["id"].is_null(), "unparsed request ⇒ id null: {v}");
    let mut rest = String::new();
    let n = reader.read_line(&mut rest).await.expect("read eof");
    assert_eq!(n, 0, "connection must close after the oversized frame");

    // Under-limit: a fresh connection still round-trips a frame.
    let stream = UnixStream::connect(&socket).await.expect("reconnect");
    let (read_half, mut write_half) = stream.into_split();
    let frame =
        r#"{"jsonrpc":"2.0","id":1,"method":"client.hello","params":{"clientId":"cli-size"}}"#;
    write_half.write_all(frame.as_bytes()).await.expect("write");
    write_half.write_all(b"\n").await.expect("write nl");
    write_half.flush().await.expect("flush");
    let mut reader = BufReader::new(read_half);
    let mut line = String::new();
    reader.read_line(&mut line).await.expect("read response");
    let v: Value = serde_json::from_str(line.trim()).expect("response json");
    assert_eq!(
        v["result"]["clientId"], "cli-size",
        "under-limit frame must still round-trip: {v}"
    );

    let _ = tx.send(());
    let _ = server.await;
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn doctor_reports_secret_presence_as_boolean_not_value() {
    // A sentinel token value that must NEVER be echoed into diagnostics output.
    const SENTINEL: &str = "ghp_SECRETvalue000111222333444555666777";
    let dir = temp_data_dir("doctor");

    let output = Command::new(env!("CARGO_BIN_EXE_intentd"))
        .arg("doctor")
        .env("INTENTD_DATA_DIR", &dir)
        .env("GITHUB_TOKEN", SENTINEL)
        .output()
        .expect("run intentd doctor");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        stdout.contains("GitHub token present"),
        "doctor must report token presence as a boolean; stdout was:\n{stdout}",
    );
    assert!(
        !stdout.contains(SENTINEL),
        "doctor leaked the GitHub token VALUE into stdout (§11.3)",
    );
    assert!(
        !stderr.contains(SENTINEL),
        "doctor leaked the GitHub token VALUE into stderr (§11.3)",
    );

    std::fs::remove_dir_all(&dir).ok();
}
