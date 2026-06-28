//! Over-the-wire `sentry.*` slice (§5.29): drive authStatus / listIssues /
//! searchIssues against the daemon over a temp UDS. A stub `SentryEngine` is
//! injected so the slice never touches the network (no `SENTRY_API_TOKEN`, no
//! REST call) and the request/param plumbing is asserted deterministically.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Serializes `INTENTD_DATA_DIR` set + `Config::resolve()` (process-global env).
static ENV_LOCK: Mutex<()> = Mutex::new(());

use async_trait::async_trait;
use intent_core::{Config, WorkspaceApi};
use intent_sentry::{
    Error as SentryError, FetchIssuesRequest, SentryAuthState, SentryEngine, SentryIssueLevel,
    SentryIssueResult, SentryIssueStatus,
};
use intent_services::{EventBus, Services};
use intent_store::Store;
use intent_transport::serve_uds;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

/// Canned issue with a given short id; only fields needed for assertions.
fn issue(short_id: &str) -> SentryIssueResult {
    SentryIssueResult {
        id: format!("uuid-{short_id}"),
        short_id: short_id.to_string(),
        title: "boom".into(),
        culprit: None,
        status: SentryIssueStatus::Unresolved,
        level: SentryIssueLevel::Error,
        count: "1".into(),
        user_count: 0,
        first_seen: "2026-01-01T00:00:00Z".into(),
        last_seen: "2026-01-02T00:00:00Z".into(),
        project_name: "Web".into(),
        project_slug: "web".into(),
        url: None,
        r#type: None,
        value: None,
        filename: None,
        function: None,
    }
}

/// In-process [`SentryEngine`] stub. `fail` makes every method report
/// `NotConfigured` (→ the daemon's "not configured" `-32603`); otherwise it
/// records the typed `request` / `query` it was called with and returns canned data.
struct StubEngine {
    fail: bool,
    seen_request: Arc<Mutex<Option<FetchIssuesRequest>>>,
    seen_query: Arc<Mutex<Option<String>>>,
    seen_search_project: Arc<Mutex<Option<String>>>,
}

#[async_trait]
impl SentryEngine for StubEngine {
    async fn auth_status(&self) -> intent_sentry::Result<SentryAuthState> {
        if self.fail {
            return Err(SentryError::NotConfigured("no creds".into()));
        }
        Ok(SentryAuthState {
            authenticated: true,
            organization: Some("acme".into()),
            error: None,
        })
    }

    async fn list_issues(
        &self,
        request: FetchIssuesRequest,
    ) -> intent_sentry::Result<Vec<SentryIssueResult>> {
        if self.fail {
            return Err(SentryError::NotConfigured("no creds".into()));
        }
        *self.seen_request.lock().unwrap() = Some(request);
        Ok(vec![issue("PROJ-1")])
    }

    async fn search_issues(
        &self,
        query: &str,
        project: Option<&str>,
        _limit: Option<u32>,
    ) -> intent_sentry::Result<Vec<SentryIssueResult>> {
        if self.fail {
            return Err(SentryError::NotConfigured("no creds".into()));
        }
        *self.seen_query.lock().unwrap() = Some(query.to_string());
        *self.seen_search_project.lock().unwrap() = project.map(str::to_string);
        Ok(vec![issue("PROJ-2")])
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
    engine: Arc<dyn SentryEngine>,
    tag: &str,
) -> (PathBuf, tokio::sync::oneshot::Sender<()>) {
    let short = uuid::Uuid::new_v4().simple().to_string();
    let base = Path::new("/tmp").join(format!("intentd-sentry-{tag}-{}", &short[..8]));
    let data_dir = base.join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    let config = {
        let _g = ENV_LOCK.lock().unwrap();
        std::env::set_var("INTENTD_DATA_DIR", &data_dir);
        Config::resolve().expect("resolve config")
    };
    let store = Store::open(&config.db_path).await.expect("open store");
    let bus = EventBus::new(store.clone());
    let services: Arc<dyn WorkspaceApi> = Arc::new(Services::new(store).with_sentry_engine(engine));
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
async fn uds_sentry_read_surface_round_trip() {
    let seen_request = Arc::new(Mutex::new(None));
    let seen_query = Arc::new(Mutex::new(None));
    let seen_search_project = Arc::new(Mutex::new(None));
    let engine = Arc::new(StubEngine {
        fail: false,
        seen_request: seen_request.clone(),
        seen_query: seen_query.clone(),
        seen_search_project: seen_search_project.clone(),
    });
    let (socket, _tx) = start(engine, "ok").await;

    // (a) authStatus returns derived identity — never the token.
    let resp = send(
        &socket,
        r#"{"jsonrpc":"2.0","id":1,"method":"sentry.authStatus","params":{}}"#,
    )
    .await;
    assert_eq!(resp["result"]["authenticated"], json!(true));
    assert_eq!(resp["result"]["organization"], json!("acme"));

    // (b) listIssues with no params → bare array; engine sees defaults (no status).
    let resp = send(
        &socket,
        r#"{"jsonrpc":"2.0","id":2,"method":"sentry.listIssues","params":{}}"#,
    )
    .await;
    let arr = resp["result"].as_array().expect("bare array");
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["shortId"], json!("PROJ-1"));
    let req = seen_request.lock().unwrap().clone().expect("captured");
    assert!(req.status.is_none(), "no status omits the field");
    assert!(req.project.is_none());
    assert!(req.query.is_none());

    // (c) explicit typed status + project + query + limit map through server-side.
    let resp = send(
        &socket,
        r#"{"jsonrpc":"2.0","id":3,"method":"sentry.listIssues","params":{"status":"resolved","project":"web","query":"login","limit":5}}"#,
    )
    .await;
    assert!(resp["result"].is_array());
    let req = seen_request.lock().unwrap().clone().expect("captured");
    assert_eq!(req.status, Some(intent_sentry::IssueStatusFilter::Resolved));
    assert_eq!(req.project.as_deref(), Some("web"));
    assert_eq!(req.query.as_deref(), Some("login"));
    assert_eq!(req.limit, Some(5));

    // (d) an invalid status is rejected with -32602 before the engine is touched.
    let resp = send(
        &socket,
        r#"{"jsonrpc":"2.0","id":4,"method":"sentry.listIssues","params":{"status":"bogus"}}"#,
    )
    .await;
    assert_eq!(resp["error"]["code"], json!(-32602));

    // (e) searchIssues forwards the query + optional project, returns a bare array.
    let resp = send(
        &socket,
        r#"{"jsonrpc":"2.0","id":5,"method":"sentry.searchIssues","params":{"query":"login bug","project":"web"}}"#,
    )
    .await;
    let arr = resp["result"].as_array().expect("bare array");
    assert_eq!(arr[0]["shortId"], json!("PROJ-2"));
    assert_eq!(*seen_query.lock().unwrap(), Some("login bug".to_string()));
    assert_eq!(
        *seen_search_project.lock().unwrap(),
        Some("web".to_string())
    );

    // (f) a missing required `query` is -32602.
    let resp = send(
        &socket,
        r#"{"jsonrpc":"2.0","id":6,"method":"sentry.searchIssues","params":{}}"#,
    )
    .await;
    assert_eq!(resp["error"]["code"], json!(-32602));
}

#[tokio::test]
async fn uds_sentry_not_configured_is_internal() {
    let engine = Arc::new(StubEngine {
        fail: true,
        seen_request: Arc::new(Mutex::new(None)),
        seen_query: Arc::new(Mutex::new(None)),
        seen_search_project: Arc::new(Mutex::new(None)),
    });
    let (socket, _tx) = start(engine, "unconfigured").await;

    // A credential pair that is absent / fails the org probe surfaces as -32603.
    // The P0 reads share the same not-configured mapping once param validation passes.
    for (id, frame) in [
        (
            1,
            r#"{"jsonrpc":"2.0","id":1,"method":"sentry.authStatus","params":{}}"#,
        ),
        (
            2,
            r#"{"jsonrpc":"2.0","id":2,"method":"sentry.listIssues","params":{}}"#,
        ),
        (
            3,
            r#"{"jsonrpc":"2.0","id":3,"method":"sentry.searchIssues","params":{"query":"x"}}"#,
        ),
    ] {
        let resp = send(&socket, frame).await;
        assert_eq!(resp["error"]["code"], json!(-32603), "frame id {id}");
    }
}
