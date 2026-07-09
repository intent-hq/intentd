//! Regression: a wedged `SecretStore::load` (macOS Keychain auth prompt, slow
//! Security framework) must NOT stall the daemon. `settings.list` returns
//! within its bounded deadline with the sensitive value reported as unset, and
//! a concurrent `workspace.list` on the same connection stays fast — proving
//! the async wrapper's `spawn_blocking + timeout + single-flight` guarantees
//! keep the tokio runtime free even when the OS keychain hangs forever.
//!
//! Mirrors `uds_concurrent_dispatch.rs` for the fast-vs-slow interleave and
//! `uds_settings.rs` for the transport wiring; the only new surface here is the
//! `BlockingSecrets` fake injected via `Services::with_secret_store`.

#![cfg(unix)]

use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

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
use uuid::Uuid;

/// A wedged secret store: every `load` sleeps well past the wrapper's ~3s read
/// deadline so the tokio runtime can only stay responsive if the wrapper
/// off-loads the call to `spawn_blocking` AND enforces a timeout. `store` /
/// `delete` are trivial — this fake exists to exercise the read path.
#[derive(Default)]
struct BlockingSecrets {
    load_calls: AtomicUsize,
}

impl SecretStore for BlockingSecrets {
    fn load(&self, _account: &str) -> Option<String> {
        self.load_calls.fetch_add(1, Ordering::SeqCst);
        // Longer than the production 3s read timeout; kept small enough that
        // the tokio blocking-pool shutdown at end-of-test doesn't hold up the
        // whole test binary.
        thread::sleep(Duration::from_secs(5));
        None
    }
    fn store(&self, _account: &str, _value: &str) -> IntentResult<()> {
        Ok(())
    }
    fn delete(&self, _account: &str) -> IntentResult<()> {
        Ok(())
    }
}

struct TempDb {
    path: PathBuf,
}
impl TempDb {
    fn new() -> Self {
        Self {
            path: std::env::temp_dir().join(format!("intentd-uds-nb-{}.db", Uuid::new_v4())),
        }
    }
}
impl Drop for TempDb {
    fn drop(&mut self) {
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(PathBuf::from(format!("{}{suffix}", self.path.display())));
        }
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
/// `workspace.list` on the same UDS connection must still return promptly.
/// Before the async wrapper, the sync keychain call held the tokio worker and
/// starved every other RPC. Mirrors `uds_concurrent_dispatch.rs`: JSON-RPC
/// correlates responses by `id`, so the fast reply is expected out-of-order
/// well before the slow `settings.list` finishes.
#[tokio::test]
async fn wedged_settings_list_does_not_stall_concurrent_workspace_list() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let bus = EventBus::new(store.clone());
    let secrets = Arc::new(BlockingSecrets::default());
    let secrets_dyn: Arc<dyn SecretStore> = secrets.clone();
    let services: Arc<dyn WorkspaceApi> = Arc::new(
        Services::new(store)
            .with_event_bus(bus.clone())
            .with_secret_store(secrets_dyn),
    );
    // Short suffix so the full socket path fits under macOS `SUN_LEN`.
    let socket = std::env::temp_dir().join(format!(
        "id-uds-nb-{}.sock",
        &Uuid::new_v4().simple().to_string()[..12]
    ));

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let server = tokio::spawn({
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
    // timeout on each sensitive setting because the fake keychain sleeps
    // beyond the deadline.
    let slow = json!({ "jsonrpc": "2.0", "id": 1, "method": "settings.list" });
    // Fast request second: no keychain involvement.
    let fast = json!({ "jsonrpc": "2.0", "id": 2, "method": "workspace.list" });

    let start = Instant::now();
    send(&mut w, &serde_json::to_string(&slow).unwrap()).await;
    send(&mut w, &serde_json::to_string(&fast).unwrap()).await;

    // First response must be the fast one, well under the wedged-keychain
    // budget. Before this fix, the sync keychain call held the tokio worker
    // and workspace.list waited alongside settings.list.
    let first = read_json(&mut r, Duration::from_secs(2)).await;
    let fast_elapsed = start.elapsed();
    assert_eq!(
        first["id"], 2,
        "fast workspace.list must respond before wedged settings.list: {first}"
    );
    assert!(
        first.get("result").is_some(),
        "workspace.list must succeed: {first}"
    );
    assert!(
        fast_elapsed < Duration::from_secs(2),
        "workspace.list took {fast_elapsed:?} — wedged keychain is still stalling the runtime"
    );

    // Eventually settings.list returns with every sensitive value reported as
    // null (the wrapper's timeout branch) rather than hanging forever. The
    // budget covers a full sequential sweep of every sensitive definition at
    // the wrapper's 3s read timeout, with a generous margin.
    let second = read_json(&mut r, Duration::from_secs(30)).await;
    assert_eq!(
        second["id"], 1,
        "second response must be settings.list: {second}"
    );
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
    // (concurrent requests for the same account coalesce). The fake sleeps
    // for 5s, so every sensitive slot spawned at most one blocking call.
    let sensitive_count = settings
        .iter()
        .filter(|e| e["sensitive"].as_bool().unwrap_or(false))
        .count();
    let call_count = secrets.load_calls.load(Ordering::SeqCst);
    assert!(
        call_count <= sensitive_count,
        "expected <= {sensitive_count} keychain loads (one per sensitive setting), got {call_count}"
    );

    let _ = shutdown_tx.send(());
    let _ = server.await;
    let _ = std::fs::remove_file(&socket);
}
