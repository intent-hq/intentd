//! Over-the-wire `linear.*` slice (§5.28): drive authStatus / listIssues /
//! searchIssues against the daemon over a temp UDS. A stub `LinearEngine` is
//! injected so the slice never touches the network (no `LINEAR_API_KEY`, no
//! GraphQL call) and the filter/param plumbing is asserted deterministically.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Serializes `INTENTD_DATA_DIR` set + `Config::resolve()` (process-global env).
static ENV_LOCK: Mutex<()> = Mutex::new(());

use async_trait::async_trait;
use intent_core::{Config, WorkspaceApi};
use intent_linear::{
    AuthStatus, Error as LinearError, IssueFilter, LinearEngine, LinearIssueResult,
};
use intent_services::{EventBus, Services};
use intent_store::Store;
use intent_transport::serve_uds;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

/// Canned issue with a given identifier; only fields needed for assertions.
fn issue(identifier: &str) -> LinearIssueResult {
    LinearIssueResult {
        id: format!("uuid-{identifier}"),
        identifier: identifier.to_string(),
        title: "Issue".to_string(),
        description: None,
        url: None,
        team_name: None,
        team_key: None,
        state: None,
        priority: None,
        assignee: None,
        labels: None,
        project: None,
        creator: None,
        created_at: None,
        updated_at: None,
    }
}

/// In-process [`LinearEngine`] stub. `fail` makes every method report
/// `NotConfigured` (→ the daemon's "not configured" `-32603`); otherwise it
/// records the typed `filter`/`query` it was called with and returns canned data.
struct StubEngine {
    fail: bool,
    seen_filter: Arc<Mutex<Option<IssueFilter>>>,
    seen_query: Arc<Mutex<Option<String>>>,
}

#[async_trait]
impl LinearEngine for StubEngine {
    async fn auth_status(&self) -> intent_linear::Result<AuthStatus> {
        if self.fail {
            return Err(LinearError::NotConfigured("no key".into()));
        }
        Ok(AuthStatus {
            authenticated: true,
            login: Some("Ada Lovelace".into()),
            scopes: vec![],
        })
    }

    async fn list_issues(
        &self,
        filter: IssueFilter,
        _limit: Option<u32>,
    ) -> intent_linear::Result<Vec<LinearIssueResult>> {
        if self.fail {
            return Err(LinearError::NotConfigured("no key".into()));
        }
        *self.seen_filter.lock().unwrap() = Some(filter);
        Ok(vec![issue("ENG-1")])
    }

    async fn search_issues(
        &self,
        query: &str,
        _limit: Option<u32>,
    ) -> intent_linear::Result<Vec<LinearIssueResult>> {
        if self.fail {
            return Err(LinearError::NotConfigured("no key".into()));
        }
        *self.seen_query.lock().unwrap() = Some(query.to_string());
        Ok(vec![issue("ENG-2")])
    }
}

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

/// Boot a daemon over a temp UDS with `engine` injected; returns the socket and
/// a shutdown sender (drop/send to stop the server).
async fn start(
    engine: Arc<dyn LinearEngine>,
    tag: &str,
) -> (PathBuf, tokio::sync::oneshot::Sender<()>) {
    let short = uuid::Uuid::new_v4().simple().to_string();
    let base = Path::new("/tmp").join(format!("intentd-linear-{tag}-{}", &short[..8]));
    let data_dir = base.join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    let config = {
        let _g = ENV_LOCK.lock().unwrap();
        std::env::set_var("INTENTD_DATA_DIR", &data_dir);
        Config::resolve().expect("resolve config")
    };
    let store = Store::open(&config.db_path).await.expect("open store");
    let bus = EventBus::new(store.clone());
    let services: Arc<dyn WorkspaceApi> = Arc::new(Services::new(store).with_linear_engine(engine));
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let socket = config.socket_path.clone();
    let serve_socket = socket.clone();
    tokio::spawn(async move {
        serve_uds(services, bus, &serve_socket, None, async move {
            let _ = rx.await;
        })
        .await
        .expect("serve");
    });
    for _ in 0..50 {
        if socket.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    (socket, tx)
}

#[tokio::test]
async fn uds_linear_read_surface_round_trip() {
    let seen_filter = Arc::new(Mutex::new(None));
    let seen_query = Arc::new(Mutex::new(None));
    let engine = Arc::new(StubEngine {
        fail: false,
        seen_filter: seen_filter.clone(),
        seen_query: seen_query.clone(),
    });
    let (socket, _tx) = start(engine, "ok").await;

    // (a) authStatus returns derived identity — never the key.
    let resp = send(
        &socket,
        r#"{"jsonrpc":"2.0","id":1,"method":"linear.authStatus","params":{}}"#,
    )
    .await;
    assert_eq!(resp["result"]["authenticated"], json!(true));
    assert_eq!(resp["result"]["login"], json!("Ada Lovelace"));
    assert_eq!(resp["result"]["scopes"], json!([]));

    // (b) listIssues with no filter → bare array; engine sees the `assigned` default.
    let resp = send(
        &socket,
        r#"{"jsonrpc":"2.0","id":2,"method":"linear.listIssues","params":{}}"#,
    )
    .await;
    let arr = resp["result"].as_array().expect("bare array");
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["identifier"], json!("ENG-1"));
    assert_eq!(*seen_filter.lock().unwrap(), Some(IssueFilter::Assigned));

    // (c) an explicit typed filter maps through server-side.
    let resp = send(
        &socket,
        r#"{"jsonrpc":"2.0","id":3,"method":"linear.listIssues","params":{"filter":"created","limit":5}}"#,
    )
    .await;
    assert!(resp["result"].is_array());
    assert_eq!(*seen_filter.lock().unwrap(), Some(IssueFilter::Created));

    // (d) an invalid filter is rejected with -32602 before the engine is touched.
    let resp = send(
        &socket,
        r#"{"jsonrpc":"2.0","id":4,"method":"linear.listIssues","params":{"filter":"bogus"}}"#,
    )
    .await;
    assert_eq!(resp["error"]["code"], json!(-32602));

    // (e) searchIssues forwards the query and returns a bare array.
    let resp = send(
        &socket,
        r#"{"jsonrpc":"2.0","id":5,"method":"linear.searchIssues","params":{"query":"login bug"}}"#,
    )
    .await;
    let arr = resp["result"].as_array().expect("bare array");
    assert_eq!(arr[0]["identifier"], json!("ENG-2"));
    assert_eq!(*seen_query.lock().unwrap(), Some("login bug".to_string()));

    // (f) a missing required `query` is -32602.
    let resp = send(
        &socket,
        r#"{"jsonrpc":"2.0","id":6,"method":"linear.searchIssues","params":{}}"#,
    )
    .await;
    assert_eq!(resp["error"]["code"], json!(-32602));
}

#[tokio::test]
async fn uds_linear_not_configured_is_internal() {
    let engine = Arc::new(StubEngine {
        fail: true,
        seen_filter: Arc::new(Mutex::new(None)),
        seen_query: Arc::new(Mutex::new(None)),
    });
    let (socket, _tx) = start(engine, "unconfigured").await;

    // A key that is absent / fails the viewer probe surfaces as -32603.
    let resp = send(
        &socket,
        r#"{"jsonrpc":"2.0","id":1,"method":"linear.authStatus","params":{}}"#,
    )
    .await;
    assert_eq!(resp["error"]["code"], json!(-32603));
}
