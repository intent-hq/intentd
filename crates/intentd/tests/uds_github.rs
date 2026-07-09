//! Over-the-wire `github.*` explicit-addressing slice (§5.27): drive the
//! pulls/issues/review-comment arms through the daemon over a temp UDS.
//!
//! Network-guarded: the default assertions exercise only the router param
//! validation and the in-service param parsing, which run **without any GitHub
//! call**. A real read is performed only when `INTENTD_GH_LIVE_TEST=1` and a
//! token (`GITHUB_TOKEN`/`GH_TOKEN`) are present.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use intent_core::{Config, WorkspaceApi};
use intent_services::{EventBus, Services};
use intent_store::Store;
use intent_transport::serve_uds;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

async fn send(socket: &Path, frame: &str) -> Value {
    let stream = UnixStream::connect(socket).await.expect("connect");
    let (read_half, mut write_half) = stream.into_split();
    write_half.write_all(frame.as_bytes()).await.expect("write");
    write_half.write_all(b"\n").await.expect("write nl");
    write_half.flush().await.expect("flush");
    let mut reader = BufReader::new(read_half);
    let mut line = String::new();
    reader.read_line(&mut line).await.expect("read");
    serde_json::from_str(line.trim()).expect("valid json")
}

#[tokio::test]
async fn uds_github_param_validation_and_routing() {
    let short = uuid::Uuid::new_v4().simple().to_string();
    let base = Path::new("/tmp").join(format!("intentd-gh-{}", &short[..8]));
    let data_dir = base.join("data");
    std::fs::create_dir_all(&data_dir).unwrap();

    std::env::set_var("INTENTD_DATA_DIR", &data_dir);
    let config = Config::resolve().expect("resolve config");

    let store = Store::open(&config.db_path).await.expect("open store");
    let bus = EventBus::new(store.clone());
    let services: Arc<dyn WorkspaceApi> = Arc::new(Services::new(store).with_workspaces_root(
        std::env::temp_dir().join(format!("itd-hermetic-ws-{}", uuid::Uuid::new_v4())),
    ));
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let socket = config.socket_path.clone();
    let server = tokio::spawn(async move {
        serve_uds(services, bus, &socket, None, async move {
            let _ = rx.await;
        })
        .await
        .expect("serve");
    });
    for _ in 0..50 {
        if config.socket_path.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // (a) Missing required params short-circuit in the router → -32602, with no
    // GitHub call attempted.
    for frame in [
        r#"{"jsonrpc":"2.0","id":1,"method":"github.pulls.list","params":{"repo":"r"}}"#,
        r#"{"jsonrpc":"2.0","id":2,"method":"github.pulls.get","params":{"owner":"o","repo":"r"}}"#,
        r#"{"jsonrpc":"2.0","id":3,"method":"github.replyReviewComment","params":{"owner":"o","repo":"r","number":1}}"#,
        r#"{"jsonrpc":"2.0","id":4,"method":"github.resolveThread","params":{}}"#,
    ] {
        let resp = send(&config.socket_path, frame).await;
        assert_eq!(resp["error"]["code"], json!(-32602), "frame={frame}");
    }

    // (b) A bad `filter` is rejected by the in-service parser **before** any
    // forge handle is resolved → -32603, proving the arm routes into the
    // service without touching the network.
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":5,"method":"github.pulls.search","params":{"owner":"o","repo":"r","filter":"bogus"}}"#,
    )
    .await;
    assert_eq!(resp["error"]["code"], json!(-32603));

    // (c) Unknown github.* sub-method is still a clean method-not-found.
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":6,"method":"github.pulls.nope","params":{}}"#,
    )
    .await;
    assert_eq!(resp["error"]["code"], json!(-32601));

    // (d) Optional live read — only when explicitly opted in with a token.
    let live = std::env::var("INTENTD_GH_LIVE_TEST").ok().as_deref() == Some("1");
    let has_token = std::env::var("GITHUB_TOKEN").is_ok() || std::env::var("GH_TOKEN").is_ok();
    if live && has_token {
        let resp = send(
            &config.socket_path,
            r#"{"jsonrpc":"2.0","id":7,"method":"github.pulls.get","params":{"owner":"octocat","repo":"Hello-World","number":1}}"#,
        )
        .await;
        let pull = &resp["result"]["pull"];
        assert_eq!(pull["number"], json!(1));
        assert!(pull["htmlUrl"].is_string());
        assert!(pull["headRef"].is_string());
    } else {
        eprintln!("uds_github: skipping live read (set INTENTD_GH_LIVE_TEST=1 + token)");
    }

    let _ = tx.send(());
    let _ = server.await;
}
