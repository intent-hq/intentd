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
    AuthStatus, CreateIssueRequest, Error as LinearError, IssueFilter, LinearEngine,
    LinearIssueResult, LinearLabel, LinearProject, LinearTeam, LinearUser, LinearWorkflowState,
    UpdateIssueRequest,
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
    seen_id: Arc<Mutex<Option<String>>>,
    seen_create: Arc<Mutex<Option<CreateIssueRequest>>>,
    seen_update: Arc<Mutex<Option<UpdateIssueRequest>>>,
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

    async fn get_issue(&self, id_or_identifier: &str) -> intent_linear::Result<LinearIssueResult> {
        if self.fail {
            return Err(LinearError::NotConfigured("no key".into()));
        }
        *self.seen_id.lock().unwrap() = Some(id_or_identifier.to_string());
        Ok(issue("ENG-3"))
    }

    async fn viewer(&self) -> intent_linear::Result<LinearUser> {
        if self.fail {
            return Err(LinearError::NotConfigured("no key".into()));
        }
        Ok(LinearUser {
            id: "u1".into(),
            name: "Ada Lovelace".into(),
            display_name: None,
            email: None,
            avatar_url: None,
        })
    }

    async fn list_teams(&self, _limit: Option<u32>) -> intent_linear::Result<Vec<LinearTeam>> {
        if self.fail {
            return Err(LinearError::NotConfigured("no key".into()));
        }
        Ok(vec![LinearTeam {
            id: "t1".into(),
            key: "ENG".into(),
            name: "Engineering".into(),
            description: None,
        }])
    }

    async fn list_workflow_states(
        &self,
        _limit: Option<u32>,
    ) -> intent_linear::Result<Vec<LinearWorkflowState>> {
        if self.fail {
            return Err(LinearError::NotConfigured("no key".into()));
        }
        Ok(vec![LinearWorkflowState {
            id: "s1".into(),
            name: "Todo".into(),
            r#type: "unstarted".into(),
            description: None,
            color: None,
        }])
    }

    async fn list_projects(
        &self,
        _limit: Option<u32>,
    ) -> intent_linear::Result<Vec<LinearProject>> {
        if self.fail {
            return Err(LinearError::NotConfigured("no key".into()));
        }
        Ok(vec![LinearProject {
            id: "p1".into(),
            name: "Apollo".into(),
            description: None,
            state: "started".into(),
            url: None,
        }])
    }

    async fn list_labels(&self, _limit: Option<u32>) -> intent_linear::Result<Vec<LinearLabel>> {
        if self.fail {
            return Err(LinearError::NotConfigured("no key".into()));
        }
        Ok(vec![LinearLabel {
            id: "l1".into(),
            name: "bug".into(),
            description: None,
            color: None,
        }])
    }

    async fn create_issue(
        &self,
        req: CreateIssueRequest,
    ) -> intent_linear::Result<LinearIssueResult> {
        if self.fail {
            return Err(LinearError::NotConfigured("no key".into()));
        }
        *self.seen_create.lock().unwrap() = Some(req.clone());
        Ok(issue("ENG-100"))
    }

    async fn update_issue(
        &self,
        req: UpdateIssueRequest,
    ) -> intent_linear::Result<LinearIssueResult> {
        if self.fail {
            return Err(LinearError::NotConfigured("no key".into()));
        }
        *self.seen_update.lock().unwrap() = Some(req.clone());
        Ok(issue("ENG-101"))
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
    let seen_id = Arc::new(Mutex::new(None));
    let seen_create = Arc::new(Mutex::new(None));
    let seen_update = Arc::new(Mutex::new(None));
    let engine = Arc::new(StubEngine {
        fail: false,
        seen_filter: seen_filter.clone(),
        seen_query: seen_query.clone(),
        seen_id: seen_id.clone(),
        seen_create: seen_create.clone(),
        seen_update: seen_update.clone(),
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

    // (g) getIssue forwards `id` (or `identifier`) and returns a bare object.
    let resp = send(
        &socket,
        r#"{"jsonrpc":"2.0","id":7,"method":"linear.getIssue","params":{"id":"uuid-ENG-3"}}"#,
    )
    .await;
    assert!(resp["result"].is_object());
    assert_eq!(resp["result"]["identifier"], json!("ENG-3"));
    assert_eq!(*seen_id.lock().unwrap(), Some("uuid-ENG-3".to_string()));
    let resp = send(
        &socket,
        r#"{"jsonrpc":"2.0","id":8,"method":"linear.getIssue","params":{"identifier":"ENG-9"}}"#,
    )
    .await;
    assert_eq!(resp["result"]["identifier"], json!("ENG-3"));
    assert_eq!(*seen_id.lock().unwrap(), Some("ENG-9".to_string()));

    // (h) getIssue with neither `id` nor `identifier` is -32602.
    let resp = send(
        &socket,
        r#"{"jsonrpc":"2.0","id":9,"method":"linear.getIssue","params":{}}"#,
    )
    .await;
    assert_eq!(resp["error"]["code"], json!(-32602));

    // (i) viewer returns a bare object (never the key).
    let resp = send(
        &socket,
        r#"{"jsonrpc":"2.0","id":10,"method":"linear.viewer","params":{}}"#,
    )
    .await;
    assert!(resp["result"].is_object());
    assert_eq!(resp["result"]["name"], json!("Ada Lovelace"));

    // (j) the list reads each return a bare array.
    for (id, method, key) in [
        (11, "linear.listTeams", "ENG"),
        (12, "linear.listWorkflowStates", "Todo"),
        (13, "linear.listProjects", "Apollo"),
        (14, "linear.listLabels", "bug"),
    ] {
        let frame = format!(r#"{{"jsonrpc":"2.0","id":{id},"method":"{method}","params":{{}}}}"#);
        let resp = send(&socket, &frame).await;
        let arr = resp["result"].as_array().expect("bare array");
        assert_eq!(arr.len(), 1, "{method}");
        let names: Vec<&str> = arr[0]
            .as_object()
            .unwrap()
            .values()
            .filter_map(|v| v.as_str())
            .collect();
        assert!(names.contains(&key), "{method} missing {key}");
    }

    // (k) createIssue forwards the typed request and returns a bare object.
    let resp = send(
        &socket,
        r#"{"jsonrpc":"2.0","id":15,"method":"linear.createIssue","params":{"title":"New issue","teamId":"team-uuid","priority":2,"labelIds":["l1"]}}"#,
    )
    .await;
    assert!(resp["result"].is_object());
    assert_eq!(resp["result"]["identifier"], json!("ENG-100"));
    let recorded = seen_create.lock().unwrap().clone().expect("create seen");
    assert_eq!(recorded.title, "New issue");
    assert_eq!(recorded.team_id, "team-uuid");
    assert_eq!(recorded.priority, Some(2.0));
    assert_eq!(recorded.label_ids.as_deref(), Some(&["l1".to_string()][..]));

    // (l) createIssue rejects a missing `title` with `-32602`.
    let resp = send(
        &socket,
        r#"{"jsonrpc":"2.0","id":16,"method":"linear.createIssue","params":{"teamId":"t1"}}"#,
    )
    .await;
    assert_eq!(resp["error"]["code"], json!(-32602));

    // (m) createIssue rejects a missing `teamId` with `-32602`.
    let resp = send(
        &socket,
        r#"{"jsonrpc":"2.0","id":17,"method":"linear.createIssue","params":{"title":"X"}}"#,
    )
    .await;
    assert_eq!(resp["error"]["code"], json!(-32602));

    // (n) updateIssue forwards the typed request and returns a bare object.
    let resp = send(
        &socket,
        r#"{"jsonrpc":"2.0","id":18,"method":"linear.updateIssue","params":{"issueId":"uuid-1","title":"Updated","stateId":"s1"}}"#,
    )
    .await;
    assert!(resp["result"].is_object());
    assert_eq!(resp["result"]["identifier"], json!("ENG-101"));
    let recorded = seen_update.lock().unwrap().clone().expect("update seen");
    assert_eq!(recorded.issue_id, "uuid-1");
    assert_eq!(recorded.title.as_deref(), Some("Updated"));
    assert_eq!(recorded.state_id.as_deref(), Some("s1"));
    assert!(recorded.description.is_none());

    // (o) updateIssue rejects a missing `issueId` with `-32602`.
    let resp = send(
        &socket,
        r#"{"jsonrpc":"2.0","id":19,"method":"linear.updateIssue","params":{"title":"X"}}"#,
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
        seen_id: Arc::new(Mutex::new(None)),
        seen_create: Arc::new(Mutex::new(None)),
        seen_update: Arc::new(Mutex::new(None)),
    });
    let (socket, _tx) = start(engine, "unconfigured").await;

    // A key that is absent / fails the viewer probe surfaces as -32603. The P1
    // reads share the same not-configured mapping once param validation passes.
    for (id, frame) in [
        (
            1,
            r#"{"jsonrpc":"2.0","id":1,"method":"linear.authStatus","params":{}}"#,
        ),
        (
            2,
            r#"{"jsonrpc":"2.0","id":2,"method":"linear.getIssue","params":{"id":"uuid-1"}}"#,
        ),
        (
            3,
            r#"{"jsonrpc":"2.0","id":3,"method":"linear.viewer","params":{}}"#,
        ),
        (
            4,
            r#"{"jsonrpc":"2.0","id":4,"method":"linear.listTeams","params":{}}"#,
        ),
        (
            5,
            r#"{"jsonrpc":"2.0","id":5,"method":"linear.listWorkflowStates","params":{}}"#,
        ),
        (
            6,
            r#"{"jsonrpc":"2.0","id":6,"method":"linear.listProjects","params":{}}"#,
        ),
        (
            7,
            r#"{"jsonrpc":"2.0","id":7,"method":"linear.listLabels","params":{}}"#,
        ),
        (
            8,
            r#"{"jsonrpc":"2.0","id":8,"method":"linear.createIssue","params":{"title":"X","teamId":"t1"}}"#,
        ),
        (
            9,
            r#"{"jsonrpc":"2.0","id":9,"method":"linear.updateIssue","params":{"issueId":"uuid-1"}}"#,
        ),
    ] {
        let resp = send(&socket, frame).await;
        assert_eq!(resp["error"]["code"], json!(-32603), "frame id {id}");
    }
}
