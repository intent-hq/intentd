//! Regression: a wedged `SecretStore::load` (macOS Keychain auth prompt, slow
//! Security framework) must NOT stall the daemon. `settings.list` returns
//! within its bounded deadline with the sensitive value reported as unset, and
//! a concurrent `workspace.list` on the same connection still answers — proving
//! the async wrapper's `spawn_blocking + timeout + single-flight` guarantees
//! keep the tokio runtime free even when the OS keychain hangs forever.
//!
//! Mirrors `uds_concurrent_dispatch.rs` for the fast-vs-slow interleave and
//! `uds_settings.rs` for the transport wiring; the only new surface here is the
//! `BlockingSecrets` fake injected via `Services::with_secret_store`.

#![cfg(unix)]

use crate::common;

use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use intent_core::{Result as IntentResult, WorkspaceApi};
use intent_services::{EventBus, SecretStore, Services};
use intent_store::Store;
use intent_transport::serve_uds;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::unix::OwnedReadHalf;
use tokio::net::UnixStream;
use tokio::sync::oneshot;
use tokio::time::timeout;

/// A wedged secret store: every `load` blocks until the test opens the gate,
/// so the tokio runtime can only stay responsive if the wrapper off-loads the
/// call to `spawn_blocking` AND enforces a timeout. The test keeps the gate
/// shut until both replies have arrived, so no reply can come from a load
/// finishing. `store` / `delete` are trivial — this fake exists to exercise
/// the read path.
#[derive(Default)]
struct BlockingSecrets {
    load_calls: AtomicUsize,
    open: Mutex<bool>,
    opened: Condvar,
}

impl BlockingSecrets {
    fn open(&self) {
        *self.open.lock().unwrap() = true;
        self.opened.notify_all();
    }
}

/// Opens the gate on drop, so a failing test never leaves blocking-pool
/// threads parked past the runtime's shutdown.
struct OpenOnDrop(Arc<BlockingSecrets>);

impl Drop for OpenOnDrop {
    fn drop(&mut self) {
        self.0.open();
    }
}

impl SecretStore for BlockingSecrets {
    fn load(&self, _account: &str) -> IntentResult<Option<String>> {
        self.load_calls.fetch_add(1, Ordering::SeqCst);
        let open = self.open.lock().unwrap();
        drop(self.opened.wait_while(open, |open| !*open).unwrap());
        Ok(None)
    }
    fn store(&self, _account: &str, _value: &str) -> IntentResult<()> {
        Ok(())
    }
    fn delete(&self, _account: &str) -> IntentResult<()> {
        Ok(())
    }
}

struct TempDb {
    _dir: tempfile::TempDir,
    path: PathBuf,
}
impl TempDb {
    fn new() -> Self {
        let dir = common::test_tempdir("intentd-uds-nb-");
        let path = dir.path().join("intentd.db");
        Self { _dir: dir, path }
    }
}

async fn connect_retry(socket: &PathBuf) -> UnixStream {
    for _ in 0..100 {
        if let Ok(s) = UnixStream::connect(socket).await {
            return s;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("could not connect to {}", socket.display());
}

async fn send(write_half: &mut (impl AsyncWriteExt + Unpin), frame: &str) {
    write_half.write_all(frame.as_bytes()).await.unwrap();
    write_half.write_all(b"\n").await.unwrap();
    write_half.flush().await.unwrap();
}

async fn read_json(reader: &mut BufReader<OwnedReadHalf>, budget: Duration) -> Value {
    let mut line = String::new();
    let n = timeout(budget, reader.read_line(&mut line))
        .await
        .expect("timed out waiting for a frame")
        .expect("read failed");
    assert!(n > 0, "connection closed unexpectedly");
    serde_json::from_str(line.trim_end()).expect("invalid JSON frame")
}

/// Regression for the wedged-Keychain daemon starvation: while a `settings.list`
/// is stuck inside `SecretStore::load` (each sensitive setting takes up to the
/// wrapper's 3s timeout to fall through to null), an interleaved
/// `workspace.list` on the same UDS connection must still answer. Before the
/// async wrapper, the sync keychain call held the tokio worker and starved
/// every other RPC. Mirrors `uds_concurrent_dispatch.rs`: JSON-RPC correlates
/// responses by `id`, so the fast reply is expected out-of-order before the
/// slow `settings.list` finishes.
///
/// No clock is read: the keychain stays wedged until both replies are in, so
/// a runtime the wedge stalled answers neither and the hang guard fails the
/// test, however slow the runner.
#[intent_test_macros::daemon_test]
async fn wedged_settings_list_does_not_stall_concurrent_workspace_list() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let bus = EventBus::new(store.clone());
    let secrets = Arc::new(BlockingSecrets::default());
    let _open = OpenOnDrop(secrets.clone());
    let secrets_dyn: Arc<dyn SecretStore> = secrets.clone();
    let ws_root = common::hermetic_workspaces_root();
    let services: Arc<dyn WorkspaceApi> = Arc::new(
        Services::new_isolated(store)
            .with_event_bus(bus.clone())
            .with_secret_store(secrets_dyn)
            .with_workspaces_root(ws_root.path().to_path_buf()),
    );
    // Socket lives in a guarded dir under /tmp so the path stays short
    // (macOS SUN_LEN) and the file is swept even if the test panics.
    let sock_dir = common::test_tempdir_in("/tmp", "id-uds-nb-");
    let socket = sock_dir.path().join("uds.sock");

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let server = intent_core::spawn_daemon({
        let socket = socket.clone();
        async move {
            let _ = serve_uds(services, bus, &socket, None, async {
                let _ = shutdown_rx.await;
            })
            .await;
        }
    });
    let (rpc_read, mut w) = connect_retry(&socket).await.into_split();
    let mut r = BufReader::new(rpc_read);

    // Slow request first: settings.list will spend up to the wrapper's read
    // timeout on each sensitive setting because the fake keychain never
    // returns while the gate is shut.
    let slow = json!({ "jsonrpc": "2.0", "id": 1, "method": "settings.list" });
    // Fast request second: no keychain involvement.
    let fast = json!({ "jsonrpc": "2.0", "id": 2, "method": "workspace.list" });

    send(&mut w, &serde_json::to_string(&slow).unwrap()).await;
    send(&mut w, &serde_json::to_string(&fast).unwrap()).await;

    // Both replies must arrive while the keychain is still wedged. Before this
    // fix, the sync keychain call held the tokio worker, so workspace.list
    // waited alongside settings.list and neither answered until a load
    // returned — which, with the gate shut, is never. Their order is not
    // asserted: settings.list answers once the wrapper's 3s read timeout fires
    // on every sensitive setting, and a loaded runner can take longer than
    // that to serve workspace.list. The settings.list budget covers a full
    // sequential sweep of every sensitive definition at that timeout, with a
    // generous margin.
    let mut replies = std::collections::BTreeMap::new();
    for _ in 0..2 {
        let frame = read_json(
            &mut r,
            intentd_test_support::test_timeout(Duration::from_secs(30)),
        )
        .await;
        let id = frame["id"].as_u64().unwrap_or_else(|| panic!("reply id: {frame}"));
        assert!(replies.insert(id, frame).is_none(), "one reply per request");
    }
    let first = &replies[&2];
    assert!(
        first.get("result").is_some(),
        "workspace.list must succeed while the keychain is wedged: {first}"
    );
    let second = &replies[&1];
    assert!(
        second.get("result").is_some(),
        "settings.list must succeed even with wedged keychain: {second}"
    );
    let settings = second["result"]["settings"]
        .as_array()
        .expect("settings array");
    for entry in settings {
        if entry["sensitive"].as_bool().unwrap_or(false) {
            assert_eq!(
                entry["value"],
                Value::Null,
                "wedged secret must resolve to null: {entry}"
            );
        }
    }

    // Single-flight sanity: at most one keychain load per distinct account
    // (concurrent requests for the same account coalesce). The gate is still
    // shut, so every sensitive slot spawned at most one blocking call.
    let sensitive_count = settings
        .iter()
        .filter(|e| e["sensitive"].as_bool().unwrap_or(false))
        .count();
    let call_count = secrets.load_calls.load(Ordering::SeqCst);
    assert!(
        call_count <= sensitive_count,
        "expected <= {sensitive_count} keychain loads (one per sensitive setting), got {call_count}"
    );
    secrets.open();

    let _ = shutdown_tx.send(());
    let _ = server.await;
    let _ = std::fs::remove_file(&socket);
}
