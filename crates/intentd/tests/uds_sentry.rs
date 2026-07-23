//! Over-the-wire `sentry.*` slice (§5.29): drive authStatus / listIssues /
//! searchIssues against the daemon over a temp UDS. A stub `SentryEngine` is
//! injected so the slice never touches the network (no `SENTRY_API_TOKEN`, no
//! REST call) and the request/param plumbing is asserted deterministically.

mod common;

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Serializes `INTENTD_DATA_DIR` set + `Config::resolve()` (process-global env).
static ENV_LOCK: Mutex<()> = Mutex::new(());

use async_trait::async_trait;
use intent_core::{Config, WorkspaceApi};
use intent_sentry::{
    Error as SentryError, FetchIssuesRequest, SentryAuthState, SentryEngine, SentryIssueLevel,
    SentryIssuePage, SentryIssueResult, SentryIssueStatus, SentryProject,
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
/// `(id, assignedTo)` captured by `assignIssue`; aliased to keep clippy happy.
type SeenAssign = Arc<Mutex<Option<(String, Option<String>)>>>;

struct StubEngine {
    fail: bool,
    seen_request: Arc<Mutex<Option<FetchIssuesRequest>>>,
    seen_query: Arc<Mutex<Option<String>>>,
    seen_search_project: Arc<Mutex<Option<String>>>,
    seen_projects_limit: Arc<Mutex<Option<u32>>>,
    seen_get_issue_id: Arc<Mutex<Option<String>>>,
    seen_resolve_id: Arc<Mutex<Option<String>>>,
    seen_ignore_id: Arc<Mutex<Option<String>>>,
    seen_assign: SeenAssign,
}

impl StubEngine {
    fn new(fail: bool) -> Self {
        Self {
            fail,
            seen_request: Arc::new(Mutex::new(None)),
            seen_query: Arc::new(Mutex::new(None)),
            seen_search_project: Arc::new(Mutex::new(None)),
            seen_projects_limit: Arc::new(Mutex::new(None)),
            seen_get_issue_id: Arc::new(Mutex::new(None)),
            seen_resolve_id: Arc::new(Mutex::new(None)),
            seen_ignore_id: Arc::new(Mutex::new(None)),
            seen_assign: Arc::new(Mutex::new(None)),
        }
    }
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
    ) -> intent_sentry::Result<SentryIssuePage> {
        if self.fail {
            return Err(SentryError::NotConfigured("no creds".into()));
        }
        // Report a next page only on the first page (no cursor) so the wire
        // token round-trips exactly once.
        let next_token = match request.cursor {
            None => Some("0:100:0".to_string()),
            Some(_) => None,
        };
        *self.seen_request.lock().unwrap() = Some(request);
        Ok(SentryIssuePage {
            issues: vec![issue("PROJ-1")],
            next_token,
        })
    }

    async fn search_issues(
        &self,
        query: &str,
        project: Option<&str>,
        _limit: Option<u32>,
        cursor: Option<&str>,
    ) -> intent_sentry::Result<SentryIssuePage> {
        if self.fail {
            return Err(SentryError::NotConfigured("no creds".into()));
        }
        *self.seen_query.lock().unwrap() = Some(query.to_string());
        *self.seen_search_project.lock().unwrap() = project.map(str::to_string);
        Ok(SentryIssuePage {
            issues: vec![issue("PROJ-2")],
            next_token: match cursor {
                None => Some("0:100:0".to_string()),
                Some(_) => None,
            },
        })
    }

    async fn list_projects(&self, limit: Option<u32>) -> intent_sentry::Result<Vec<SentryProject>> {
        if self.fail {
            return Err(SentryError::NotConfigured("no creds".into()));
        }
        *self.seen_projects_limit.lock().unwrap() = limit;
        Ok(vec![SentryProject {
            id: "1".into(),
            slug: "web".into(),
            name: "Web".into(),
            platform: Some("javascript".into()),
            is_member: Some(true),
        }])
    }

    async fn get_issue(&self, id_or_short_id: &str) -> intent_sentry::Result<SentryIssueResult> {
        if self.fail {
            return Err(SentryError::NotConfigured("no creds".into()));
        }
        *self.seen_get_issue_id.lock().unwrap() = Some(id_or_short_id.to_string());
        Ok(issue("PROJ-3"))
    }

    async fn resolve_issue(&self, id: &str) -> intent_sentry::Result<SentryIssueResult> {
        if self.fail {
            return Err(SentryError::NotConfigured("no creds".into()));
        }
        *self.seen_resolve_id.lock().unwrap() = Some(id.to_string());
        Ok(issue("PROJ-4"))
    }

    async fn ignore_issue(&self, id: &str) -> intent_sentry::Result<SentryIssueResult> {
        if self.fail {
            return Err(SentryError::NotConfigured("no creds".into()));
        }
        *self.seen_ignore_id.lock().unwrap() = Some(id.to_string());
        Ok(issue("PROJ-5"))
    }

    async fn assign_issue(
        &self,
        id: &str,
        assigned_to: Option<&str>,
    ) -> intent_sentry::Result<SentryIssueResult> {
        if self.fail {
            return Err(SentryError::NotConfigured("no creds".into()));
        }
        *self.seen_assign.lock().unwrap() = Some((id.to_string(), assigned_to.map(str::to_string)));
        Ok(issue("PROJ-6"))
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
    let services: Arc<dyn WorkspaceApi> = Arc::new(
        Services::new(store)
            .with_workspaces_root(common::hermetic_workspaces_root())
            .with_sentry_engine(engine),
    );
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
    let engine = Arc::new(StubEngine::new(false));
    let seen_request = engine.seen_request.clone();
    let seen_query = engine.seen_query.clone();
    let seen_search_project = engine.seen_search_project.clone();
    let (socket, _tx) = start(engine, "ok").await;

    // (a) authStatus returns derived identity — never the token.
    let resp = send(
        &socket,
        r#"{"jsonrpc":"2.0","id":1,"method":"sentry.authStatus","params":{}}"#,
    )
    .await;
    assert_eq!(resp["result"]["authenticated"], json!(true));
    assert_eq!(resp["result"]["organization"], json!("acme"));

    // (b) listIssues with no params → `{ issues, nextToken }` envelope; engine
    // sees defaults (no status, no cursor) and the first page carries an
    // opaque non-null nextToken (§5.5).
    let resp = send(
        &socket,
        r#"{"jsonrpc":"2.0","id":2,"method":"sentry.listIssues","params":{}}"#,
    )
    .await;
    let issues = resp["result"]["issues"].as_array().expect("issues array");
    assert_eq!(issues.len(), 1);
    assert_eq!(issues[0]["shortId"], json!("PROJ-1"));
    let wire_token = resp["result"]["nextToken"]
        .as_str()
        .expect("non-null nextToken on first page")
        .to_string();
    let req = seen_request.lock().unwrap().clone().expect("captured");
    assert!(req.status.is_none(), "no status omits the field");
    assert!(req.project.is_none());
    assert!(req.query.is_none());
    assert!(req.cursor.is_none(), "first page sends no cursor");

    // (b2) echoing the opaque token back decodes onto the raw engine cursor;
    // the stub reports no further page → explicit `nextToken: null`.
    let resp = send(
        &socket,
        &format!(
            r#"{{"jsonrpc":"2.0","id":22,"method":"sentry.listIssues","params":{{"nextToken":"{wire_token}"}}}}"#
        ),
    )
    .await;
    assert_eq!(resp["result"]["issues"][0]["shortId"], json!("PROJ-1"));
    assert!(resp["result"]["nextToken"].is_null(), "last page is null");
    let req = seen_request.lock().unwrap().clone().expect("captured");
    assert_eq!(req.cursor.as_deref(), Some("0:100:0"));

    // (c) explicit typed status + project + query + limit map through server-side.
    let resp = send(
        &socket,
        r#"{"jsonrpc":"2.0","id":3,"method":"sentry.listIssues","params":{"status":"resolved","project":"web","query":"login","limit":5}}"#,
    )
    .await;
    assert!(resp["result"]["issues"].is_array());
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

    // (e) searchIssues forwards the query + optional project, returns the
    // paginated envelope; echoing the token back reaches the last page.
    let resp = send(
        &socket,
        r#"{"jsonrpc":"2.0","id":5,"method":"sentry.searchIssues","params":{"query":"login bug","project":"web"}}"#,
    )
    .await;
    assert_eq!(resp["result"]["issues"][0]["shortId"], json!("PROJ-2"));
    let wire_token = resp["result"]["nextToken"]
        .as_str()
        .expect("non-null nextToken on first page")
        .to_string();
    assert_eq!(*seen_query.lock().unwrap(), Some("login bug".to_string()));
    assert_eq!(
        *seen_search_project.lock().unwrap(),
        Some("web".to_string())
    );
    let resp = send(
        &socket,
        &format!(
            r#"{{"jsonrpc":"2.0","id":52,"method":"sentry.searchIssues","params":{{"query":"login bug","nextToken":"{wire_token}"}}}}"#
        ),
    )
    .await;
    assert!(resp["result"]["nextToken"].is_null(), "last page is null");

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
    let engine = Arc::new(StubEngine::new(true));
    let (socket, _tx) = start(engine, "unconfigured").await;

    // A credential pair that is absent / fails the org probe surfaces as -32603.
    // All P0/P1/P2 arms share the same not-configured mapping once param
    // validation passes.
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
        (
            4,
            r#"{"jsonrpc":"2.0","id":4,"method":"sentry.listProjects","params":{}}"#,
        ),
        (
            5,
            r#"{"jsonrpc":"2.0","id":5,"method":"sentry.getIssue","params":{"id":"1"}}"#,
        ),
        (
            6,
            r#"{"jsonrpc":"2.0","id":6,"method":"sentry.resolveIssue","params":{"id":"1"}}"#,
        ),
        (
            7,
            r#"{"jsonrpc":"2.0","id":7,"method":"sentry.ignoreIssue","params":{"id":"1"}}"#,
        ),
        (
            8,
            r#"{"jsonrpc":"2.0","id":8,"method":"sentry.assignIssue","params":{"id":"1"}}"#,
        ),
    ] {
        let resp = send(&socket, frame).await;
        assert_eq!(resp["error"]["code"], json!(-32603), "frame id {id}");
    }
}

#[tokio::test]
async fn uds_sentry_p1_p2_round_trip() {
    let engine = Arc::new(StubEngine::new(false));
    let seen_projects_limit = engine.seen_projects_limit.clone();
    let seen_get_issue_id = engine.seen_get_issue_id.clone();
    let seen_resolve_id = engine.seen_resolve_id.clone();
    let seen_ignore_id = engine.seen_ignore_id.clone();
    let seen_assign = engine.seen_assign.clone();
    let (socket, _tx) = start(engine, "p1p2").await;

    // (a) listProjects with an explicit limit → bare array; engine sees the limit.
    let resp = send(
        &socket,
        r#"{"jsonrpc":"2.0","id":1,"method":"sentry.listProjects","params":{"limit":25}}"#,
    )
    .await;
    let arr = resp["result"].as_array().expect("bare array");
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["slug"], json!("web"));
    assert_eq!(arr[0]["isMember"], json!(true));
    assert_eq!(*seen_projects_limit.lock().unwrap(), Some(25));

    // (b) getIssue with `id` and with `shortId` both reach the engine.
    let resp = send(
        &socket,
        r#"{"jsonrpc":"2.0","id":2,"method":"sentry.getIssue","params":{"id":"uuid-1"}}"#,
    )
    .await;
    assert_eq!(resp["result"]["shortId"], json!("PROJ-3"));
    assert_eq!(
        *seen_get_issue_id.lock().unwrap(),
        Some("uuid-1".to_string())
    );
    let resp = send(
        &socket,
        r#"{"jsonrpc":"2.0","id":3,"method":"sentry.getIssue","params":{"shortId":"WEB-7"}}"#,
    )
    .await;
    assert_eq!(resp["result"]["shortId"], json!("PROJ-3"));
    assert_eq!(
        *seen_get_issue_id.lock().unwrap(),
        Some("WEB-7".to_string())
    );

    // (c) getIssue with neither `id` nor `shortId` → -32602 before engine.
    let resp = send(
        &socket,
        r#"{"jsonrpc":"2.0","id":4,"method":"sentry.getIssue","params":{}}"#,
    )
    .await;
    assert_eq!(resp["error"]["code"], json!(-32602));

    // (d) resolveIssue + ignoreIssue forward `id` and return bare objects.
    let resp = send(
        &socket,
        r#"{"jsonrpc":"2.0","id":5,"method":"sentry.resolveIssue","params":{"id":"i-1"}}"#,
    )
    .await;
    assert!(resp["result"].is_object());
    assert_eq!(resp["result"]["shortId"], json!("PROJ-4"));
    assert_eq!(*seen_resolve_id.lock().unwrap(), Some("i-1".to_string()));

    let resp = send(
        &socket,
        r#"{"jsonrpc":"2.0","id":6,"method":"sentry.ignoreIssue","params":{"id":"i-2"}}"#,
    )
    .await;
    assert_eq!(resp["result"]["shortId"], json!("PROJ-5"));
    assert_eq!(*seen_ignore_id.lock().unwrap(), Some("i-2".to_string()));

    // (e) resolveIssue/ignoreIssue/assignIssue with empty id → -32602.
    for method in [
        "sentry.resolveIssue",
        "sentry.ignoreIssue",
        "sentry.assignIssue",
    ] {
        let frame =
            format!(r#"{{"jsonrpc":"2.0","id":7,"method":"{method}","params":{{"id":""}}}}"#);
        let resp = send(&socket, &frame).await;
        assert_eq!(resp["error"]["code"], json!(-32602), "{method}");
    }

    // (f) assignIssue with an explicit user, then with absent assignedTo to unassign.
    let resp = send(
        &socket,
        r#"{"jsonrpc":"2.0","id":8,"method":"sentry.assignIssue","params":{"id":"i-3","assignedTo":"user-1"}}"#,
    )
    .await;
    assert_eq!(resp["result"]["shortId"], json!("PROJ-6"));
    assert_eq!(
        *seen_assign.lock().unwrap(),
        Some(("i-3".to_string(), Some("user-1".to_string())))
    );

    let resp = send(
        &socket,
        r#"{"jsonrpc":"2.0","id":9,"method":"sentry.assignIssue","params":{"id":"i-4"}}"#,
    )
    .await;
    assert_eq!(resp["result"]["shortId"], json!("PROJ-6"));
    assert_eq!(
        *seen_assign.lock().unwrap(),
        Some(("i-4".to_string(), None))
    );
}
