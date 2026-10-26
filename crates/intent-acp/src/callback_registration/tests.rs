use super::*;
use std::future::{pending, ready};
use std::sync::atomic::{AtomicBool, Ordering};

use intent_core::{BoxFuture, WorkspaceApi};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, DuplexStream, ReadHalf, WriteHalf};
use tokio::sync::{oneshot, Notify};
use tokio::time::timeout;

use crate::handshake::handshake_with_callbacks;
use crate::session::{map_notification, map_notification_with_callback_routes, MappedUpdate};
use crate::transport::{ConnectionHooks, IncomingNotification};

fn wait() -> Duration {
    intentd_test_support::test_timeout(Duration::from_secs(15))
}
const RECEIPT: &str = "11111111-1111-4111-8111-111111111111";
const SERVER: &str = "intent-callback-22222222-2222-4222-8222-222222222222";

struct Peer {
    read: BufReader<ReadHalf<DuplexStream>>,
    write: WriteHalf<DuplexStream>,
}

fn pair(capacity: usize) -> (Arc<Connection>, Peer) {
    let (client, peer) = tokio::io::duplex(capacity);
    let (read, write) = tokio::io::split(client);
    let (pr, pw) = tokio::io::split(peer);
    (
        Arc::new(Connection::new(
            write,
            read,
            None,
            ConnectionHooks::default(),
        )),
        Peer {
            read: BufReader::new(pr),
            write: pw,
        },
    )
}

impl Peer {
    async fn read(&mut self) -> Value {
        let mut line = String::new();
        assert!(
            timeout(wait(), self.read.read_line(&mut line))
                .await
                .unwrap()
                .unwrap()
                > 0
        );
        serde_json::from_str(&line).unwrap()
    }
    async fn send(&mut self, value: Value) {
        self.write
            .write_all(format!("{value}\n").as_bytes())
            .await
            .unwrap();
    }
    async fn reply(&mut self, request: &Value, result: Value) {
        self.send(json!({"jsonrpc":"2.0", "id":request["id"], "result":result}))
            .await;
    }
}

fn config() -> intent_providers::ProviderConfig {
    let mut provider = *intent_providers::find_provider("claude-code").unwrap();
    provider.supports_authenticate = false;
    provider
}

fn capability() -> Value {
    json!({"version":1,"method":METHOD})
}
fn meta() -> Meta {
    serde_json::from_value(json!({META_KEY:{"version":1,"queryReceipt":RECEIPT}})).unwrap()
}
fn query(conn: &Arc<Connection>) -> CallbackQuery {
    CallbackClient {
        connection: Arc::clone(conn),
        monitoring: None,
    }
    .capture("session".into(), Some(&meta()))
    .unwrap()
}
fn server(tools: bool) -> CallbackStdioServer {
    CallbackStdioServer::new(
        "fixture-mcp".into(),
        vec!["127.0.0.1:1".into()],
        None,
        tools.then_some(CallbackTool::WorkspaceApi),
    )
    .unwrap()
}
fn remote(request: &Value, status: &str, result: &Value) -> Value {
    json!({"version":1,"sessionId":request["params"]["sessionId"],
        "queryReceipt":request["params"]["queryReceipt"],"registrationId":request["params"]["registrationId"],
        "serverName":SERVER,"status":status,"result":result})
}
fn success(request: &Value) -> Value {
    remote(
        request,
        "acknowledged",
        &json!({"added":[SERVER],"removed":[],"errors":{}}),
    )
}

#[tokio::test]
async fn only_exact_opt_in_capability_yields_original_client() {
    for (offer, advertised, enabled) in [
        (CallbackOffer::Disabled, capability(), false),
        (CallbackOffer::V1, capability(), true),
        (CallbackOffer::V1, Value::Null, false),
        (
            CallbackOffer::V1,
            json!({"version":1,"method":METHOD,"extra":true}),
            false,
        ),
        (
            CallbackOffer::V1,
            json!({"version":2,"method":METHOD}),
            false,
        ),
        (
            CallbackOffer::V1,
            json!({"version":1,"method":"other"}),
            false,
        ),
        (
            CallbackOffer::V1,
            json!({"version":"1","method":METHOD}),
            false,
        ),
    ] {
        let (conn, mut peer) = pair(8192);
        let c = Arc::clone(&conn);
        let run =
            tokio::spawn(
                async move { handshake_with_callbacks(c, &config(), offer).await.unwrap() },
            );
        let request = peer.read().await;
        assert_eq!(request["method"], "initialize");
        let offered = &request["params"]["clientCapabilities"]["_meta"][META_KEY];
        assert_eq!(
            *offered,
            if offer == CallbackOffer::V1 {
                json!({"version":1})
            } else {
                Value::Null
            }
        );
        peer.reply(
            &request,
            json!({"protocolVersion":1,"agentCapabilities":{},"authMethods":[],
            "_meta":{META_KEY:advertised,"ordinary":"retained"}}),
        )
        .await;
        let got = run.await.unwrap();
        assert_eq!(got.callbacks.is_some(), enabled);
        assert_eq!(
            got.ordinary.initialize.meta.unwrap()["ordinary"],
            "retained"
        );
        assert!(!got.ordinary.authenticated);
        assert_eq!(conn.pending_len(), 0);
        assert!(timeout(
            intentd_test_support::test_timeout(Duration::from_millis(10)),
            peer.read.read_line(&mut String::new())
        )
        .await
        .is_err());
    }
}

#[tokio::test]
async fn original_new_and_load_each_run_once_and_bad_receipts_preserve_response() {
    for load in [false, true] {
        for receipt in [
            json!({"version":1,"queryReceipt":RECEIPT}),
            Value::Null,
            json!({"version":1,"queryReceipt":RECEIPT,"extra":true}),
            json!({"version":1,"queryReceipt":"not-a-receipt"}),
        ] {
            let (conn, mut peer) = pair(8192);
            let client = CallbackClient {
                connection: Arc::clone(&conn),
                monitoring: None,
            };
            let expected = receipt == json!({"version":1,"queryReceipt":RECEIPT});
            let run = tokio::spawn(async move {
                if load {
                    let r = client
                        .load_session("original", "/fixture", vec![], None)
                        .await
                        .unwrap();
                    (r.query, r.response.meta)
                } else {
                    let r = client.new_session("/fixture", vec![], None).await.unwrap();
                    assert_eq!(r.response.session_id.to_string(), "original");
                    (r.query, r.response.meta)
                }
            });
            let request = peer.read().await;
            assert_eq!(
                request["method"],
                if load { "session/load" } else { "session/new" }
            );
            if load {
                assert_eq!(request["params"]["sessionId"], "original");
            }
            let mut response = json!({"_meta":{META_KEY:receipt,"unrelated":[1,2]}});
            if !load {
                response["sessionId"] = json!("original");
            }
            peer.reply(&request, response).await;
            let (query, meta) = run.await.unwrap();
            assert_eq!(query.is_some(), expected);
            if let Some(query) = query {
                assert_eq!(query.session_id, "original");
            }
            assert_eq!(meta.unwrap()["unrelated"], json!([1, 2]));
            assert_eq!(conn.pending_len(), 0);
        }
    }
}

#[tokio::test]
async fn original_errors_remain_errors_without_registration() {
    let (conn, mut peer) = pair(8192);
    let client = CallbackClient {
        connection: Arc::clone(&conn),
        monitoring: None,
    };
    let task = tokio::spawn(async move { client.new_session("/fixture", vec![], None).await });
    let request = peer.read().await;
    peer.send(json!({"jsonrpc":"2.0","id":request["id"],"error":{"code":-32010,"message":"original","data":{"kept":true}}})).await;
    let Err(AcpError::Rpc(error)) = task.await.unwrap() else {
        panic!("original error lost")
    };
    assert_eq!(error.code, -32010);
    assert_eq!(error.data, Some(json!({"kept":true})));
    assert_eq!(conn.pending_len(), 0);
}

fn note(session: &str, title: &str) -> IncomingNotification {
    IncomingNotification {
        method: "session/update".into(),
        params: json!({"sessionId":session,
        "update":{"sessionUpdate":"tool_call","toolCallId":"call","title":title,"status":"completed",
        "rawInput":{"code":"return 1"},"rawOutput":{"kept":true}}}),
    }
}
fn tool(
    note: &IncomingNotification,
    routes: &CallbackToolRoutes,
) -> crate::session::MappedToolCall {
    let Some(MappedUpdate::ToolCall(call)) = map_notification_with_callback_routes(note, routes)
    else {
        panic!("no mapped call")
    };
    call
}

#[tokio::test]
async fn correlated_outcomes_and_exact_connection_routes_preserve_diagnostics() {
    for (status, result) in [
        (
            "acknowledged",
            json!({"added":[SERVER],"removed":[],"errors":{}}),
        ),
        (
            "failed",
            json!({"added":[],"removed":["original"],"errors":{"fresh":"actual failure"}}),
        ),
        ("stale", json!({"added":[SERVER],"removed":[],"errors":{}})),
        ("uncertain", Value::Null),
        ("not-dispatched", Value::Null),
    ] {
        let (conn, mut peer) = pair(8192);
        let registration = query(&conn).registration(server(true));
        let task = tokio::spawn(registration.run(pending()));
        let request = peer.read().await;
        assert_eq!(request["method"], METHOD);
        let mut response = remote(&request, status, &result);
        response["reason"] = json!({"code":"sdk-error","message":"original diagnostic"});
        peer.reply(&request, response.clone()).await;
        let CallbackDeliveryOutcome::Remote(received) = task.await.unwrap() else {
            panic!("valid outcome rejected")
        };
        assert_eq!(serde_json::to_value(received).unwrap(), response);
        let routes = conn.callback_tool_routes();
        let title = format!("mcp__{SERVER}__workspace_api");
        let n = note("session", &title);
        let Some(MappedUpdate::ToolCall(original)) = map_notification(&n) else {
            panic!()
        };
        let mapped = tool(&n, &routes);
        assert_eq!(mapped.tool_name, "workspace_api");
        assert_eq!(mapped.title, original.title);
        assert_eq!(mapped.name_authoritative, original.name_authoritative);
        assert_eq!(mapped.output, original.output);
        for n in [
            note("other-session", &title),
            note("session", &format!("{title}_extra")),
            note("session", "mcp__foreign__workspace_api"),
        ] {
            assert_eq!(
                map_notification_with_callback_routes(&n, &routes),
                map_notification(&n)
            );
        }
        let (other, _peer) = pair(8192);
        assert_ne!(
            tool(&n, &other.callback_tool_routes()).tool_name,
            "workspace_api"
        );
    }
}

#[tokio::test]
async fn malformed_or_foreign_outcomes_keep_raw_evidence_and_never_add_routes() {
    for mutation in [
        "sessionId",
        "queryReceipt",
        "registrationId",
        "version",
        "serverName",
        "status",
        "result",
        "extra",
        "reason",
    ] {
        let (conn, mut peer) = pair(8192);
        let task = tokio::spawn(query(&conn).registration(server(true)).run(pending()));
        let request = peer.read().await;
        let mut response = success(&request);
        response[mutation] = if mutation == "reason" {
            json!(0)
        } else {
            json!("wrong")
        };
        peer.reply(&request, response.clone()).await;
        let CallbackDeliveryOutcome::Malformed(raw) = task.await.unwrap() else {
            panic!("{mutation} accepted")
        };
        assert_eq!(raw, response);
        assert!(conn.callback_tool_routes().names.lock().unwrap().is_empty());
    }
    let (conn, mut peer) = pair(8192);
    let task = tokio::spawn(query(&conn).registration(server(false)).run(pending()));
    let request = peer.read().await;
    peer.reply(&request, success(&request)).await;
    assert!(matches!(
        task.await.unwrap(),
        CallbackDeliveryOutcome::Remote(_)
    ));
    assert!(conn.callback_tool_routes().names.lock().unwrap().is_empty());
}

#[tokio::test]
async fn cancel_before_queue_sends_nothing_and_after_queue_cancels_only_original() {
    let (conn, mut peer) = pair(8192);
    assert!(matches!(
        query(&conn).registration(server(true)).run(ready(())).await,
        CallbackDeliveryOutcome::NotSent(CallbackFailure::Cancelled)
    ));
    assert_eq!(conn.pending_len(), 0);
    let (send, cancelled) = oneshot::channel();
    let task = tokio::spawn(query(&conn).registration(server(true)).run(async {
        let _ = cancelled.await;
    }));
    let request = peer.read().await;
    send.send(()).unwrap();
    assert!(matches!(
        task.await.unwrap(),
        CallbackDeliveryOutcome::Unknown(CallbackFailure::Cancelled)
    ));
    assert_eq!(conn.pending_len(), 0);
    let cancellation = peer.read().await;
    assert_eq!(cancellation["method"], "$/cancel_request");
    assert_eq!(cancellation["params"]["requestId"], request["id"]);
    peer.reply(&request, success(&request)).await;
    let later = Arc::clone(&conn);
    let ordinary = tokio::spawn(async move { later.request("ordinary", json!({})).await.unwrap() });
    let next = peer.read().await;
    assert_ne!(next["id"], request["id"]);
    peer.reply(&next, json!({"ordinary":"retained"})).await;
    assert_eq!(ordinary.await.unwrap(), json!({"ordinary":"retained"}));
    assert!(conn.callback_tool_routes().names.lock().unwrap().is_empty());
}

#[tokio::test]
async fn dropping_unpolled_and_pending_requests_cleans_without_detached_retry() {
    let (conn, mut peer) = pair(8192);
    drop(query(&conn).registration(server(true)).run(pending()));
    assert_eq!(conn.pending_len(), 0);
    let task = tokio::spawn(query(&conn).registration(server(true)).run(pending()));
    let request = peer.read().await;
    task.abort();
    let _ = task.await;
    assert_eq!(conn.pending_len(), 0);
    let cancel = peer.read().await;
    assert_eq!(cancel["params"]["requestId"], request["id"]);
    peer.reply(&request, success(&request)).await;
    assert!(conn.callback_tool_routes().names.lock().unwrap().is_empty());
}

#[tokio::test]
async fn one_deadline_includes_saturated_writer_and_response_wait() {
    let (conn, mut peer) = pair(1);
    // One blocked write and the exact 256 channel slots leave no admission.
    for _ in 0..257 {
        conn.notify("fill", json!({})).await.unwrap();
    }
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    let result = query(&conn)
        .registration(server(true))
        .run_with_timeout(pending(), Duration::from_millis(20))
        .await;
    assert!(matches!(
        result,
        CallbackDeliveryOutcome::NotSent(CallbackFailure::Deadline)
    ));
    assert_eq!(conn.pending_len(), 0);
    for _ in 0..257 {
        assert_eq!(peer.read().await["method"], "fill");
    }
    let task = tokio::spawn(
        query(&conn)
            .registration(server(true))
            .run_with_timeout(pending(), Duration::from_millis(20)),
    );
    let request = peer.read().await;
    assert!(matches!(
        task.await.unwrap(),
        CallbackDeliveryOutcome::Unknown(CallbackFailure::Deadline)
    ));
    assert_eq!(conn.pending_len(), 0);
    assert_eq!(peer.read().await["params"]["requestId"], request["id"]);
}

#[tokio::test]
async fn dropping_and_cancelling_while_writer_is_full_never_enqueues_registration() {
    let (conn, mut peer) = pair(1);
    for _ in 0..257 {
        conn.notify("fill", json!({})).await.unwrap();
    }
    let task = tokio::spawn(query(&conn).registration(server(true)).run(pending()));
    while conn.pending_len() == 0 {
        tokio::task::yield_now().await;
    }
    task.abort();
    let _ = task.await;
    assert_eq!(conn.pending_len(), 0);
    let (send, cancel) = oneshot::channel();
    let task = tokio::spawn(query(&conn).registration(server(true)).run(async {
        let _ = cancel.await;
    }));
    while conn.pending_len() == 0 {
        tokio::task::yield_now().await;
    }
    send.send(()).unwrap();
    assert!(matches!(
        task.await.unwrap(),
        CallbackDeliveryOutcome::NotSent(CallbackFailure::Cancelled)
    ));
    assert_eq!(conn.pending_len(), 0);
    for _ in 0..257 {
        assert_eq!(peer.read().await["method"], "fill");
    }
    assert!(timeout(
        intentd_test_support::test_timeout(Duration::from_millis(10)),
        peer.read.read_line(&mut String::new())
    )
    .await
    .is_err());
}

#[tokio::test]
async fn queued_rpc_error_disconnect_and_foreign_reply_remain_uncertain() {
    for disconnect in [false, true] {
        let (conn, mut peer) = pair(8192);
        let task = tokio::spawn(query(&conn).registration(server(true)).run(pending()));
        let request = peer.read().await;
        peer.send(json!({"jsonrpc":"2.0","id":999,"result":success(&request)}))
            .await;
        if disconnect {
            drop(peer);
        } else {
            peer.send(json!({"jsonrpc":"2.0","id":request["id"],"error":{"code":-32602,"message":"stale original","data":{"original":true}}})).await;
        }
        let CallbackDeliveryOutcome::Unknown(CallbackFailure::Connection(AcpError::Rpc(error))) =
            task.await.unwrap()
        else {
            panic!("lost original error")
        };
        if !disconnect {
            assert_eq!(error.data, Some(json!({"original":true})));
        }
        assert_eq!(conn.pending_len(), 0);
        assert!(conn.callback_tool_routes().names.lock().unwrap().is_empty());
    }
}

/// The frozen da577fff callback adapter. Only the monorepo gate runner
/// (`make test`, `make test-select`) sets the variable, from its verified
/// offline cache; raw `cargo nextest` leaves it unset.
fn callback_adapter_fixture() -> std::path::PathBuf {
    const HINT: &str = "run these tests with make test-select E='<nextest filterset>' (or make \
        test), which set it from the verified offline cache; raw cargo nextest does not. Setup: \
        the monorepo's CONTRIBUTING.md#linux-x64-callback-gates";
    let value = std::env::var_os("INTENT_ACP_CALLBACK_ADAPTER_FIXTURE")
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| {
            panic!("INTENT_ACP_CALLBACK_ADAPTER_FIXTURE (the frozen da577fff adapter fixture) is unset: {HINT}")
        });
    std::fs::canonicalize(&value).unwrap_or_else(|error| {
        panic!(
            "INTENT_ACP_CALLBACK_ADAPTER_FIXTURE={} is not a usable fixture ({error}): {HINT}",
            std::path::Path::new(&value).display()
        )
    })
}

struct NodePeer {
    // Kill the scripted adapter before deleting its allowed scratch directories.
    child: tokio::process::Child,
    connection: Arc<Connection>,
    scratch: tempfile::TempDir,
}

impl NodePeer {
    fn start() -> Self {
        let adapter = callback_adapter_fixture();
        let dependencies = std::fs::canonicalize(adapter.join("node_modules")).unwrap();
        let fixture = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/claude_callback_peer.mjs");
        let scratch = tempfile::Builder::new()
            .prefix("intent-acp-callback-")
            .tempdir()
            .unwrap();
        let mut command = tokio::process::Command::new("node");
        command
            .env_clear()
            .env("PATH", std::env::var_os("PATH").unwrap())
            .env("NODE_OPTIONS", "")
            .env("NODE_DISABLE_COMPILE_CACHE", "1")
            .env("DD_TRACE_ENABLED", "false")
            .env("DD_INJECTION_ENABLED", "false")
            .env("DD_INSTRUMENT_SERVICE_WITH_APM", "false")
            .env("DD_INJECT_NATIVE", "never")
            .env("DD_INSTRUMENTATION_TELEMETRY_ENABLED", "false")
            .env("DO_NOT_TRACK", "1")
            .env("CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC", "1")
            .env("CALLBACK_TEST_ROOT", scratch.path())
            .env("CLAUDE_CONFIG_DIR", scratch.path().join("claude-config"))
            .env("XDG_CONFIG_HOME", scratch.path().join("xdg"))
            .env("TMPDIR", scratch.path())
            .arg("--permission")
            .arg(format!("--allow-fs-read={}", fixture.display()))
            .arg(format!("--allow-fs-read={}", adapter.display()))
            .arg(format!("--allow-fs-read={}", dependencies.display()))
            .arg(format!("--allow-fs-read={}", scratch.path().display()))
            .arg(format!("--allow-fs-write={}", scratch.path().display()))
            .arg(fixture)
            .arg(adapter)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);
        let mut child = command.spawn().unwrap();
        let connection = Arc::new(Connection::new(
            child.stdin.take().unwrap(),
            child.stdout.take().unwrap(),
            Some(Box::new(child.stderr.take().unwrap())),
            ConnectionHooks::default(),
        ));
        Self {
            child,
            connection,
            scratch,
        }
    }
    async fn call(&self, method: &str, params: Value) -> Value {
        self.connection
            .request_timeout(method, params, wait())
            .await
            .unwrap_or_else(|e| panic!("{e}: {:?}", self.connection.recent_stderr()))
    }
    async fn finish(mut self) {
        self.child.kill().await.unwrap();
        self.child.wait().await.unwrap();
    }
}

struct McpApi {
    hold: AtomicBool,
    entered: Notify,
    release: Notify,
}

impl WorkspaceApi for McpApi {
    fn settings_get(&self, path: String) -> BoxFuture<'_, intent_core::Result<Value>> {
        Box::pin(async move {
            if self.hold.swap(false, Ordering::SeqCst) {
                self.entered.notify_one();
                self.release.notified().await;
            }
            Ok(json!({"path":path,"value":match path.as_str() {
                "workspaceApi.toonOutput" => json!(false),
                "workspaceApi.maxOutputChars" => json!(100_000),
                _ => Value::Null,
            }}))
        })
    }
}

async fn mcp() -> (crate::McpBridge, Arc<McpApi>) {
    let api = Arc::new(McpApi {
        hold: AtomicBool::new(false),
        entered: Notify::new(),
        release: Notify::new(),
    });
    let server = crate::WorkspaceMcpServer::new(api.clone(), "fixture-workspace".into());
    let bridge = crate::serve_workspace_mcp_tcp(Arc::new(server))
        .await
        .unwrap();
    (bridge, api)
}

fn mcp_entry(name: &str, bridge: &crate::McpBridge) -> McpServer {
    McpServer::Stdio(
        agent_client_protocol::schema::v1::McpServerStdio::new(name, "fixture-mcp")
            .args(vec![bridge.connect_addr()]),
    )
}
fn bridge_registration(query: CallbackQuery, bridge: &crate::McpBridge) -> CallbackRegistration {
    query.registration(
        CallbackStdioServer::new(
            "fixture-mcp".into(),
            vec![bridge.connect_addr()],
            None,
            [CallbackTool::WorkspaceApi],
        )
        .unwrap(),
    )
}

#[tokio::test]
async fn actual_adapter_sdk_and_rust_mcp_deliver_distinct_callback_preserving_old_result() {
    let f = NodePeer::start();
    assert_eq!(
        f.call("fixture/permissions", json!({})).await,
        json!({"filesystem":"denied","native":"denied"})
    );
    let handshake =
        handshake_with_callbacks(Arc::clone(&f.connection), &config(), CallbackOffer::V1)
            .await
            .unwrap();
    let client = handshake.callbacks.unwrap();
    let (old, old_api) = mcp().await;
    let (user, _) = mcp().await;
    let opened = client
        .new_session(
            f.scratch.path(),
            vec![mcp_entry("workspace-mcp", &old), mcp_entry("user", &user)],
            Some(
                serde_json::from_value(json!({"claudeCode":{"options":{"settingSources":[]}}}))
                    .unwrap(),
            ),
        )
        .await
        .unwrap();
    let session_id = opened.response.session_id.to_string();
    // The old operation has reached post-operation settings; its response is held.
    old_api.hold.store(true, Ordering::SeqCst);
    let conn = Arc::clone(&f.connection);
    let held = tokio::spawn(async move {
        conn.request_timeout(
            "fixture/call",
            json!({"name":"workspace-mcp","code":"return 'old ordinary result';"}),
            wait(),
        )
        .await
        .unwrap()
    });
    timeout(wait(), old_api.entered.notified()).await.unwrap();
    let (fresh, _) = mcp().await;
    let outcome = bridge_registration(opened.query.unwrap(), &fresh)
        .run(pending())
        .await;
    let CallbackDeliveryOutcome::Remote(outcome) = outcome else {
        panic!(
            "registration failed: {outcome:?}, {:?}",
            f.connection.recent_stderr()
        )
    };
    assert_eq!(outcome.status, CallbackStatus::Acknowledged);
    assert_eq!(
        outcome.result.as_ref().unwrap().added,
        std::slice::from_ref(&outcome.server_name)
    );
    old_api.release.notify_one();
    assert!(held
        .await
        .unwrap()
        .to_string()
        .contains("old ordinary result"));
    for (name, text) in [
        (&outcome.server_name, "fresh result"),
        (&"user".to_string(), "user result"),
        (&"workspace-mcp".to_string(), "old still callable"),
    ] {
        let result = f
            .call(
                "fixture/call",
                json!({"name":name,"code":format!("return '{text}';")}),
            )
            .await;
        assert!(result.to_string().contains(text), "{result}");
    }
    let loaded = client
        .load_session(
            &session_id,
            f.scratch.path(),
            vec![mcp_entry("workspace-mcp", &old), mcp_entry("user", &user)],
            Some(
                serde_json::from_value(json!({"claudeCode":{"options":{"settingSources":[]}}}))
                    .unwrap(),
            ),
        )
        .await
        .unwrap();
    assert_eq!(
        loaded.query.as_ref().unwrap().receipt,
        outcome.query_receipt
    );
    let inspect = f.call("fixture/inspect", json!({})).await;
    assert_eq!(inspect["queries"], 1);
    assert_eq!(inspect["initializations"], json!([1]));
    let requests = inspect["requests"].as_array().unwrap();
    for name in ["session/new", "session/load", METHOD] {
        assert_eq!(requests.iter().filter(|r| r["method"] == name).count(), 1);
    }
    let controls = inspect["frames"][0].as_array().unwrap();
    let mutations: Vec<_> = controls
        .iter()
        .filter(|r| r["request"]["subtype"] == "mcp_set_servers")
        .collect();
    assert_eq!(mutations.len(), 1);
    let servers = &mutations[0]["request"]["servers"];
    assert!(servers.get("workspace-mcp").is_some() && servers.get("user").is_some());
    assert_eq!(
        servers[&outcome.server_name]["args"],
        json!([fresh.connect_addr()])
    );
    assert_eq!(
        tool(
            &note(
                &session_id,
                &format!("mcp__{}__workspace_api", outcome.server_name)
            ),
            &f.connection.callback_tool_routes()
        )
        .tool_name,
        "workspace_api"
    );
    f.finish().await;
}

#[tokio::test]
async fn actual_adapter_replacement_rejects_old_query_and_disabled_never_registers() {
    let f = NodePeer::start();
    let handshake = handshake_with_callbacks(
        Arc::clone(&f.connection),
        &config(),
        CallbackOffer::Disabled,
    )
    .await
    .unwrap();
    assert!(handshake.callbacks.is_none());
    let opened = crate::session::new_session(&f.connection, f.scratch.path(), vec![], None)
        .await
        .unwrap();
    assert!(opened.meta.as_ref().and_then(|m| m.get(META_KEY)).is_none());
    let inspect = f.call("fixture/inspect", json!({})).await;
    assert!(!inspect["requests"]
        .as_array()
        .unwrap()
        .iter()
        .any(|r| r["method"] == METHOD));
    f.finish().await;

    let f = NodePeer::start();
    let client = handshake_with_callbacks(Arc::clone(&f.connection), &config(), CallbackOffer::V1)
        .await
        .unwrap()
        .callbacks
        .unwrap();
    let original = client
        .new_session(f.scratch.path(), vec![], None)
        .await
        .unwrap();
    let session = original.response.session_id.to_string();
    let replacement = f
        .call("fixture/replace", json!({"sessionId":session}))
        .await;
    assert_ne!(
        replacement["_meta"][META_KEY]["queryReceipt"],
        original.query.as_ref().unwrap().receipt
    );
    let (unused, _) = mcp().await;
    let result = bridge_registration(original.query.unwrap(), &unused)
        .run(pending())
        .await;
    assert!(matches!(
        result,
        CallbackDeliveryOutcome::Unknown(CallbackFailure::Connection(AcpError::Rpc(_)))
    ));
    assert!(f
        .connection
        .callback_tool_routes()
        .names
        .lock()
        .unwrap()
        .is_empty());
    let inspect = f.call("fixture/inspect", json!({})).await;
    assert_eq!(inspect["queries"], 2);
    for frames in inspect["frames"].as_array().unwrap() {
        assert!(!frames
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v["request"]["subtype"] == "mcp_set_servers"));
    }
    f.finish().await;
}

async fn monitor_client(
    conn: &Arc<Connection>,
    peer: &mut Peer,
    advertised: Value,
) -> Arc<CallbackClient> {
    let c = conn.clone();
    let run = tokio::spawn(async move {
        crate::handshake::handshake_with_review_monitoring(
            c,
            &config(),
            CallbackOffer::V1,
            crate::review_monitoring::ReviewMonitoringOffer::V1,
        )
        .await
        .unwrap()
    });
    let request = peer.read().await;
    assert_eq!(
        request["params"]["clientCapabilities"]["_meta"]["reviewMonitoring"],
        1
    );
    assert_eq!(
        request["params"]["clientCapabilities"]["_meta"][META_KEY],
        json!({"version":1})
    );
    peer.reply(
        &request,
        json!({"protocolVersion":1,"agentCapabilities":{},"authMethods":[],
        "_meta":{META_KEY:capability(),"reviewMonitoring":advertised,"ordinary":"retained"}}),
    )
    .await;
    let response = run.await.unwrap();
    assert_eq!(
        response.ordinary.initialize.meta.unwrap()["ordinary"],
        "retained"
    );
    Arc::new(response.callbacks.unwrap())
}

fn monitor_meta() -> Meta {
    let mut meta = Meta::new();
    crate::review_monitoring::ReviewMonitoringContext::new("original-owner".into())
        .unwrap()
        .insert_into(&mut meta);
    meta
}

async fn monitor_session(
    client: &Arc<CallbackClient>,
    peer: &mut Peer,
    session: &str,
    advertised: Value,
) -> CallbackQuery {
    let client = client.clone();
    let run = tokio::spawn(async move {
        client
            .new_session("/fixture", vec![], Some(monitor_meta()))
            .await
            .unwrap()
    });
    let request = peer.read().await;
    peer.reply(&request,json!({"sessionId":session,"_meta":{META_KEY:{"version":1,"queryReceipt":RECEIPT},
        "reviewMonitoring":advertised,"reviewMonitoringContext":request["params"]["_meta"]["reviewMonitoringContext"]}})).await;
    run.await.unwrap().query.unwrap()
}

async fn monitor_ack(
    query: CallbackQuery,
    peer: &mut Peer,
) -> crate::review_monitoring::ReviewMonitoringReceipt {
    let evidence = query.pending_review_monitoring().unwrap();
    assert!(
        evidence.acknowledged().is_none(),
        "a captured pre-ACK request has no recipient receipt"
    );
    let run = tokio::spawn(async move { query.registration(server(true)).run(pending()).await });
    let request = peer.read().await;
    peer.reply(&request, success(&request)).await;
    assert!(matches!(
        run.await.unwrap(),
        CallbackDeliveryOutcome::Remote(_)
    ));
    evidence
        .acknowledged()
        .expect("actual correlated ACK enables original session receipt")
}

#[tokio::test]
async fn monitor_receipt_intersects_initialize_session_and_actual_ack() {
    for version in [
        Value::Null,
        json!(0),
        json!(-1),
        json!(true),
        json!("1"),
        json!(1.5),
        json!(1),
        json!(9),
    ] {
        for initialize in [false, true] {
            let (conn, mut peer) = pair(8192);
            let client = monitor_client(
                &conn,
                &mut peer,
                if initialize {
                    version.clone()
                } else {
                    json!(1)
                },
            )
            .await;
            let query = monitor_session(
                &client,
                &mut peer,
                "original",
                if initialize {
                    json!(1)
                } else {
                    version.clone()
                },
            )
            .await;
            let supported = version.as_u64().is_some_and(|v| v >= 1);
            assert_eq!(query.pending_review_monitoring().is_some(), supported);
            if supported {
                let receipt = monitor_ack(query, &mut peer).await;
                assert!(receipt.is_current());
                assert_eq!(receipt.with_current(|| 42), Some(42));
                receipt.retire();
                assert_eq!(receipt.with_current(|| 42), None);
            }
        }
    }
}

#[tokio::test]
async fn monitor_receipt_reinitialize_and_late_ack_never_revive_generation() {
    let (conn, mut peer) = pair(8192);
    let client = monitor_client(&conn, &mut peer, json!(1)).await;
    let query = monitor_session(&client, &mut peer, "original", json!(1)).await;
    let retained = query.pending_review_monitoring().unwrap();
    let ack = tokio::spawn(async move { query.registration(server(true)).run(pending()).await });
    let registration = peer.read().await;
    let c = conn.clone();
    let reinitialize = tokio::spawn(async move { crate::handshake::initialize(&c).await });
    let request = peer.read().await;
    assert!(request["params"]["clientCapabilities"]["_meta"]["reviewMonitoring"].is_null());
    peer.reply(&registration, success(&registration)).await;
    let _ = ack.await.unwrap();
    assert!(
        retained.acknowledged().is_none(),
        "late ACK cannot resurrect old generation"
    );
    peer.reply(
        &request,
        json!({"protocolVersion":1,"agentCapabilities":{},"authMethods":[]}),
    )
    .await;
    reinitialize.await.unwrap().unwrap();
    let stale = monitor_session(&client, &mut peer, "original", json!(1)).await;
    assert!(stale.pending_review_monitoring().is_none());
}

#[tokio::test]
async fn monitor_receipt_same_id_load_retires_before_reply_and_ignores_stale_completion() {
    let (conn, mut peer) = pair(8192);
    let client = monitor_client(&conn, &mut peer, json!(1)).await;
    let query = monitor_session(&client, &mut peer, "original", json!(1)).await;
    let old = monitor_ack(query, &mut peer).await;
    let c = client.clone();
    let first = tokio::spawn(async move {
        c.load_session("original", "/fixture", vec![], Some(monitor_meta()))
            .await
    });
    let first_request = peer.read().await;
    assert!(!old.is_current());
    let c = client.clone();
    let second = tokio::spawn(async move {
        c.load_session("original", "/fixture", vec![], Some(monitor_meta()))
            .await
    });
    let second_request = peer.read().await;
    let response = |request: &Value| json!({"_meta":{META_KEY:{"version":1,"queryReceipt":RECEIPT},"reviewMonitoring":1,"reviewMonitoringContext":request["params"]["_meta"]["reviewMonitoringContext"]}});
    peer.reply(&second_request, response(&second_request)).await;
    let query = second.await.unwrap().unwrap().query.unwrap();
    let fresh = monitor_ack(query, &mut peer).await;
    peer.reply(&first_request, response(&first_request)).await;
    assert!(first
        .await
        .unwrap()
        .unwrap()
        .query
        .unwrap()
        .pending_review_monitoring()
        .is_none());
    assert!(fresh.is_current());
    drop(peer);
    timeout(wait(), async {
        while fresh.is_current() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(!old.is_current());
}

#[tokio::test]
async fn monitor_receipt_requires_exact_original_startup_context_echo() {
    let original =
        json!({"version":1,"ownerKey":"original-owner","loadAttemptId":"original-attempt"});
    for sent in [Value::Null, original.clone()] {
        for echoed in [
            Value::Null,
            json!({"version":1,"ownerKey":"foreign-owner","loadAttemptId":"original-attempt"}),
            json!({"version":1,"ownerKey":"original-owner","loadAttemptId":"replacement-attempt"}),
            json!({"version":1,"ownerKey":"original-owner","loadAttemptId":"original-attempt","extra":true}),
            original.clone(),
        ] {
            let (conn, mut peer) = pair(8192);
            let client = monitor_client(&conn, &mut peer, json!(1)).await;
            let c = client.clone();
            let meta = serde_json::from_value(json!({"reviewMonitoringContext":sent})).unwrap();
            let run = tokio::spawn(async move {
                c.new_session("/fixture", vec![], Some(meta)).await.unwrap()
            });
            let request = peer.read().await;
            peer.reply(&request,json!({"sessionId":"original","_meta":{META_KEY:{"version":1,"queryReceipt":RECEIPT},"reviewMonitoring":1,"reviewMonitoringContext":echoed}})).await;
            let query = run.await.unwrap().query.unwrap();
            assert_eq!(
                query.pending_review_monitoring().is_some(),
                sent == original && echoed == original,
                "sent={sent}, echoed={echoed}"
            );
        }
    }
}

#[tokio::test]
async fn monitor_startup_attempt_cannot_be_reused_for_same_id_replacement() {
    let (conn, mut peer) = pair(8192);
    let client = monitor_client(&conn, &mut peer, json!(1)).await;
    let meta = Some(monitor_meta());
    let mut original = None;
    for first in [true, false] {
        let c = client.clone();
        let m = meta.clone();
        let run = tokio::spawn(async move {
            c.load_session("original", "/fixture", vec![], m)
                .await
                .unwrap()
        });
        let request = peer.read().await;
        if let Some(old) = &original {
            assert!(!crate::review_monitoring::ReviewMonitoringReceipt::is_current(old));
        }
        peer.reply(&request,json!({"_meta":{META_KEY:{"version":1,"queryReceipt":RECEIPT},"reviewMonitoring":1,"reviewMonitoringContext":request["params"]["_meta"]["reviewMonitoringContext"]}})).await;
        let query = run.await.unwrap().query.unwrap();
        if first {
            original = Some(monitor_ack(query, &mut peer).await);
        } else {
            assert!(query.pending_review_monitoring().is_none());
        }
    }
}

fn prepared_history() -> Value {
    json!({"preparationId":"prepared-original","snapshotDigest":"a".repeat(64),"monitorMessages":[{"messageId":"original-message","storeEnvelopeDigest":"b".repeat(64),"acpPayloadDigest":"c".repeat(64)}]})
}

#[tokio::test]
async fn monitor_prepared_history_consumes_exact_original_snapshot_with_context_echo() {
    let (conn, mut peer) = pair(8192);
    let client = monitor_client(&conn, &mut peer, json!(1)).await;
    let context =
        crate::review_monitoring::ReviewMonitoringContext::new("original-owner".into()).unwrap();
    let c = client.clone();
    let selected = context.clone();
    let prepare = tokio::spawn(async move { c.prepare_review_history("original", selected).await });
    let request = peer.read().await;
    assert_eq!(request["method"], "_intent/session/prepare_review_history");
    assert_eq!(request["params"]["ownerKey"], "original-owner");
    assert!(request["params"].get("account").is_none());
    let attempt = request["params"]["loadAttemptId"].clone();
    peer.reply(&request, prepared_history()).await;
    let prepared = prepare.await.unwrap().unwrap();
    assert_eq!(prepared.messages().len(), 1);
    assert_eq!(prepared.messages()[0].message_id, "original-message");
    let target = prepared.load_target("/fixture", vec![], None).unwrap();
    let c = client.clone();
    let load = tokio::spawn(async move {
        prepared
            .load(
                &c,
                "/fixture",
                vec![],
                None,
                Box::new(TestBoundHistoryAdmission(target)),
            )
            .await
    });
    let request = peer.read().await;
    assert_eq!(request["method"], "session/load");
    assert_eq!(request["params"]["sessionId"], "original");
    assert_eq!(
        request["params"]["_meta"]["reviewMonitoringHistory"],
        json!({"version":1,"preparationId":"prepared-original","snapshotDigest":"a".repeat(64),"loadAttemptId":attempt})
    );
    assert_eq!(
        request["params"]["_meta"]["reviewMonitoringContext"],
        serde_json::to_value(context).unwrap()
    );
    peer.reply(&request,json!({"_meta":{META_KEY:{"version":1,"queryReceipt":RECEIPT},"reviewMonitoring":1,"reviewMonitoringContext":request["params"]["_meta"]["reviewMonitoringContext"]}})).await;
    let query = load.await.unwrap().unwrap().query.unwrap();
    assert!(query
        .pending_review_monitoring()
        .unwrap()
        .acknowledged()
        .is_none());
    assert!(monitor_ack(query, &mut peer).await.is_current());
}

#[tokio::test]
async fn monitor_prepared_history_rejects_malformed_lineage_and_retired_original() {
    let (conn, mut peer) = pair(8192);
    let client = monitor_client(&conn, &mut peer, json!(1)).await;
    for mutation in ["missing", "duplicate", "digest", "binding"] {
        let c = client.clone();
        let prepare = tokio::spawn(async move {
            c.prepare_review_history(
                "original",
                crate::review_monitoring::ReviewMonitoringContext::new("owner".into()).unwrap(),
            )
            .await
        });
        let request = peer.read().await;
        let mut response = prepared_history();
        match mutation {
            "missing" => {
                response.as_object_mut().unwrap().remove("monitorMessages");
            }
            "duplicate" => {
                let duplicate = response["monitorMessages"][0].clone();
                response["monitorMessages"]
                    .as_array_mut()
                    .unwrap()
                    .push(duplicate);
            }
            "digest" => {
                response["monitorMessages"][0]["storeEnvelopeDigest"] = json!("not-a-digest");
            }
            "binding" => {
                response["monitorMessages"][0]["privateBinding"] = json!({"account":"secret"});
            }
            _ => unreachable!(),
        }
        peer.reply(&request, response).await;
        assert!(prepare.await.unwrap().is_err(), "{mutation}");
    }
    let c = client.clone();
    let prepare = tokio::spawn(async move {
        c.prepare_review_history(
            "original",
            crate::review_monitoring::ReviewMonitoringContext::new("owner".into()).unwrap(),
        )
        .await
    });
    let request = peer.read().await;
    peer.reply(&request, prepared_history()).await;
    let prepared = prepare.await.unwrap().unwrap();
    let c = conn.clone();
    let reinitialize = tokio::spawn(async move { crate::handshake::initialize(&c).await });
    let request = peer.read().await;
    assert!(prepared
        .load(
            &client,
            "/fixture",
            vec![],
            None,
            Box::new(TestHistoryAdmission)
        )
        .await
        .is_err());
    peer.reply(
        &request,
        json!({"protocolVersion":1,"agentCapabilities":{},"authMethods":[]}),
    )
    .await;
    reinitialize.await.unwrap().unwrap();
    // A later peer reply or ordinary successful initialize cannot reattach the
    // consuming preparation to a fresh generation, and no load was queued.
    assert!(timeout(
        intentd_test_support::test_timeout(Duration::from_millis(20)),
        peer.read()
    )
    .await
    .is_err());
}

#[tokio::test]
async fn monitor_recipient_binding_requires_exact_connection_and_original_session() {
    let (connection, mut peer) = pair(8192);
    let client = monitor_client(&connection, &mut peer, json!(1)).await;
    let query = monitor_session(&client, &mut peer, "original", json!(1)).await;
    let pending = query.pending_review_monitoring().unwrap();
    let (other, _other_peer) = pair(8192);
    assert!(pending.is_for(&connection, "original"));
    assert!(!pending.is_for(&other, "original"));
    assert!(!pending.is_for(&connection, "replacement"));
    assert!(pending.acknowledged().is_none());
    let receipt = monitor_ack(query, &mut peer).await;
    assert!(pending.is_for(&connection, "original"));
    assert!(receipt.is_current());
    let replacement = monitor_session(&client, &mut peer, "original", json!(1)).await;
    assert!(!pending.is_for(&connection, "original"));
    assert!(!receipt.is_current());
    let current = replacement.pending_review_monitoring().unwrap();
    assert!(current.is_for(&connection, "original"));
    assert!(current.acknowledged().is_none());
}

struct TestHistoryAdmission;
impl crate::review_monitoring::history::ReviewHistoryLoadAdmission for TestHistoryAdmission {
    fn admit(
        &self,
        boundary: &crate::review_monitoring::history::ReviewHistoryLoadBoundary,
        packet: crate::review_monitoring::history::PreparedReviewHistoryLoad<'_>,
    ) -> crate::error::AcpResult<crate::review_monitoring::history::ReviewHistoryLoadReceipt> {
        packet.transfer(boundary)
    }
}

struct TestBoundHistoryAdmission(crate::review_monitoring::history::ReviewHistoryLoadTarget);
impl crate::review_monitoring::history::ReviewHistoryLoadAdmission for TestBoundHistoryAdmission {
    fn admit(
        &self,
        boundary: &crate::review_monitoring::history::ReviewHistoryLoadBoundary,
        packet: crate::review_monitoring::history::PreparedReviewHistoryLoad<'_>,
    ) -> crate::error::AcpResult<crate::review_monitoring::history::ReviewHistoryLoadReceipt> {
        if !self.0.matches(boundary) {
            return Err(crate::error::AcpError::Protocol(
                "foreign prepared load".into(),
            ));
        }
        packet.transfer(boundary)
    }
}

#[tokio::test]
async fn monitor_prepared_history_bound_target_refuses_changed_cwd_and_metadata() {
    for changed in ["cwd", "metadata"] {
        let (conn, mut peer) = pair(8192);
        let client = monitor_client(&conn, &mut peer, json!(1)).await;
        let context =
            crate::review_monitoring::ReviewMonitoringContext::new("original-owner".into())
                .unwrap();
        let c = client.clone();
        let prepare =
            tokio::spawn(async move { c.prepare_review_history("original", context).await });
        let request = peer.read().await;
        peer.reply(&request, prepared_history()).await;
        let prepared = prepare.await.unwrap().unwrap();
        let target = prepared.load_target("/fixture", vec![], None).unwrap();
        let cwd = if changed == "cwd" {
            "/replacement"
        } else {
            "/fixture"
        };
        let meta = (changed == "metadata")
            .then(|| serde_json::from_value(json!({"systemPrompt":"replacement"})).unwrap());
        assert!(prepared
            .load(
                &client,
                cwd,
                vec![],
                meta,
                Box::new(TestBoundHistoryAdmission(target))
            )
            .await
            .is_err());
        assert!(
            tokio::time::timeout(
                intentd_test_support::test_timeout(Duration::from_millis(20)),
                peer.read()
            )
            .await
            .is_err(),
            "changed load or fallback reached the peer"
        );
    }
}

#[tokio::test]
async fn monitor_context_retirement_closes_before_poll_and_requires_exact_remote_join() {
    for joined in [true, false] {
        let (connection, mut peer) = pair(8192);
        let client = monitor_client(&connection, &mut peer, json!(1)).await;
        let original = monitor_session(&client, &mut peer, "original", json!(1)).await;
        let receipt = monitor_ack(original, &mut peer).await;
        let context = receipt.context().clone();
        let sibling = monitor_session(&client, &mut peer, "sibling", json!(1)).await;
        let sibling = monitor_ack(sibling, &mut peer).await;
        let retirement = client
            .begin_review_context_retirement("original", &context)
            .unwrap();
        assert!(
            !receipt.is_current(),
            "local eligibility survived until the join future was polled"
        );
        assert!(sibling.is_current());
        assert!(
            timeout(
                intentd_test_support::test_timeout(Duration::from_millis(20)),
                peer.read()
            )
            .await
            .is_err(),
            "begin performed asynchronous I/O"
        );
        let join = tokio::spawn(retirement.join());
        let request = peer.read().await;
        assert_eq!(request["method"], "_intent/session/retire_review_context");
        assert_eq!(
            request["params"],
            json!({"sessionId":"original","loadAttemptId":serde_json::to_value(&context).unwrap()["loadAttemptId"]})
        );
        assert!(
            !join.is_finished(),
            "local closure fabricated remote completion"
        );
        peer.reply(&request, json!({"retired":joined})).await;
        assert_eq!(join.await.unwrap().is_ok(), joined);
        assert!(sibling.is_current());
        assert!(!receipt.is_current());
    }
}

#[tokio::test]
async fn monitor_context_retirement_keeps_original_attempt_after_same_id_replacement() {
    let (connection, mut peer) = pair(8192);
    let client = monitor_client(&connection, &mut peer, json!(1)).await;
    let query = monitor_session(&client, &mut peer, "original", json!(1)).await;
    let original = monitor_ack(query, &mut peer).await;
    let context = original.context().clone();
    let retirement = client
        .begin_review_context_retirement("original", &context)
        .unwrap();
    let query = monitor_session(&client, &mut peer, "original", json!(1)).await;
    let replacement = monitor_ack(query, &mut peer).await;
    assert_ne!(
        serde_json::to_value(replacement.context()).unwrap()["loadAttemptId"],
        serde_json::to_value(&context).unwrap()["loadAttemptId"]
    );
    let join = tokio::spawn(retirement.join());
    let request = peer.read().await;
    assert_eq!(
        request["params"]["loadAttemptId"],
        serde_json::to_value(&context).unwrap()["loadAttemptId"]
    );
    assert!(replacement.is_current());
    peer.reply(&request, json!({"retired":true})).await;
    join.await.unwrap().unwrap();
    assert!(
        replacement.is_current(),
        "old retirement targeted same-ID replacement"
    );
    assert!(!original.is_current());
}

#[tokio::test]
async fn monitor_context_retirement_before_capture_cannot_revive_original_load() {
    let (connection, mut peer) = pair(8192);
    let client = monitor_client(&connection, &mut peer, json!(1)).await;
    let context =
        crate::review_monitoring::ReviewMonitoringContext::new("original-owner".into()).unwrap();
    let mut meta = agent_client_protocol::schema::v1::Meta::new();
    context.insert_into(&mut meta);
    let c = client.clone();
    let load = tokio::spawn(async move {
        c.load_session("original", "/fixture", vec![], Some(meta))
            .await
    });
    let request = peer.read().await;
    let retirement = client
        .begin_review_context_retirement("original", &context)
        .unwrap();
    peer.reply(&request, json!({"_meta":{META_KEY:{"version":1,"queryReceipt":RECEIPT},"reviewMonitoring":1,"reviewMonitoringContext":request["params"]["_meta"]["reviewMonitoringContext"]}})).await;
    let query = load.await.unwrap().unwrap().query.unwrap();
    assert!(
        query.pending_review_monitoring().is_none(),
        "late load response revived a retired original attempt"
    );
    let replacement = monitor_session(&client, &mut peer, "original", json!(1)).await;
    let replacement = monitor_ack(replacement, &mut peer).await;
    let join = tokio::spawn(retirement.join());
    let request = peer.read().await;
    peer.reply(&request, json!({"retired":true})).await;
    join.await.unwrap().unwrap();
    assert!(replacement.is_current());
}

#[tokio::test]
async fn monitor_context_retirement_before_prepared_queue_refuses_private_load() {
    let (connection, mut peer) = pair(8192);
    let client = monitor_client(&connection, &mut peer, json!(1)).await;
    let context =
        crate::review_monitoring::ReviewMonitoringContext::new("original-owner".into()).unwrap();
    let c = client.clone();
    let selected = context.clone();
    let prepare = tokio::spawn(async move { c.prepare_review_history("original", selected).await });
    let request = peer.read().await;
    peer.reply(&request, prepared_history()).await;
    let prepared = prepare.await.unwrap().unwrap();
    let target = prepared.load_target("/fixture", vec![], None).unwrap();
    let retirement = client
        .begin_review_context_retirement("original", &context)
        .unwrap();
    let c = client.clone();
    let load = tokio::spawn(async move {
        prepared
            .load(
                &c,
                "/fixture",
                vec![],
                None,
                Box::new(TestBoundHistoryAdmission(target)),
            )
            .await
    });
    let queued = tokio::select! {
        response = load => { assert!(response.unwrap().is_err()); false },
        request = peer.read() => {
            peer.reply(&request, json!({})).await;
            true
        },
    };
    assert!(!queued, "retired prepared history reached the peer");
    let join = tokio::spawn(retirement.join());
    let request = peer.read().await;
    peer.reply(&request, json!({"retired":true})).await;
    join.await.unwrap().unwrap();
}

#[tokio::test]
async fn monitor_context_retirement_at_final_admission_refuses_reserved_history_slot() {
    struct RetireAtAdmission {
        client: Arc<CallbackClient>,
        context: crate::review_monitoring::ReviewMonitoringContext,
        retirement: Arc<
            std::sync::Mutex<Option<crate::review_monitoring::history::ReviewContextRetirement>>,
        >,
    }
    impl crate::review_monitoring::history::ReviewHistoryLoadAdmission for RetireAtAdmission {
        fn admit(
            &self,
            boundary: &crate::review_monitoring::history::ReviewHistoryLoadBoundary,
            packet: crate::review_monitoring::history::PreparedReviewHistoryLoad<'_>,
        ) -> crate::error::AcpResult<crate::review_monitoring::history::ReviewHistoryLoadReceipt>
        {
            *self.retirement.lock().unwrap() = Some(
                self.client
                    .begin_review_context_retirement("original", &self.context)?,
            );
            packet.transfer(boundary)
        }
    }
    let (connection, mut peer) = pair(8192);
    let client = monitor_client(&connection, &mut peer, json!(1)).await;
    let context =
        crate::review_monitoring::ReviewMonitoringContext::new("original-owner".into()).unwrap();
    let c = client.clone();
    let selected = context.clone();
    let prepare = tokio::spawn(async move { c.prepare_review_history("original", selected).await });
    let request = peer.read().await;
    peer.reply(&request, prepared_history()).await;
    let prepared = prepare.await.unwrap().unwrap();
    let retirement = Arc::new(std::sync::Mutex::new(None));
    let admission = RetireAtAdmission {
        client: client.clone(),
        context,
        retirement: retirement.clone(),
    };
    let load = prepared.load(&client, "/fixture", vec![], None, Box::new(admission));
    let mut load = Box::pin(load);
    tokio::select! {
        response = &mut load => assert!(response.is_err()),
        _ = peer.read() => panic!("retired history crossed its final reserved queue boundary"),
    }
    let operation = retirement.lock().unwrap().take().unwrap();
    let join = tokio::spawn(operation.join());
    let request = peer.read().await;
    peer.reply(&request, json!({"retired":true})).await;
    join.await.unwrap().unwrap();
}

#[tokio::test]
async fn monitor_context_retirement_reused_old_attempt_preserves_replacement() {
    let (connection, mut peer) = pair(8192);
    let client = monitor_client(&connection, &mut peer, json!(1)).await;
    let original = monitor_session(&client, &mut peer, "original", json!(1)).await;
    let original = monitor_ack(original, &mut peer).await;
    let context = original.context().clone();
    let retirement = client
        .begin_review_context_retirement("original", &context)
        .unwrap();
    let replacement = monitor_session(&client, &mut peer, "original", json!(1)).await;
    let replacement = monitor_ack(replacement, &mut peer).await;
    let mut meta = agent_client_protocol::schema::v1::Meta::new();
    context.insert_into(&mut meta);
    let c = client.clone();
    let stale = tokio::spawn(async move {
        c.load_session("original", "/fixture", vec![], Some(meta))
            .await
    });
    let request = peer.read().await;
    let current_before_response = replacement.is_current();
    peer.reply(&request, json!({"_meta":{META_KEY:{"version":1,"queryReceipt":RECEIPT},"reviewMonitoring":1,"reviewMonitoringContext":request["params"]["_meta"]["reviewMonitoringContext"]}})).await;
    assert!(stale
        .await
        .unwrap()
        .unwrap()
        .query
        .unwrap()
        .pending_review_monitoring()
        .is_none());
    assert!(
        current_before_response,
        "retired old attempt invalidated the replacement before capture"
    );
    assert!(replacement.is_current());
    let join = tokio::spawn(retirement.join());
    let request = peer.read().await;
    peer.reply(&request, json!({"retired":true})).await;
    join.await.unwrap().unwrap();
    assert!(replacement.is_current());
}

#[tokio::test]
async fn monitor_context_retirement_join_survives_waiter_drop_and_is_shared() {
    for joined in [true, false] {
        let (connection, mut peer) = pair(8192);
        let client = monitor_client(&connection, &mut peer, json!(1)).await;
        let query = monitor_session(&client, &mut peer, "original", json!(1)).await;
        let receipt = monitor_ack(query, &mut peer).await;
        let context = receipt.context().clone();
        let original = client
            .begin_review_context_retirement("original", &context)
            .unwrap();
        let waiter = tokio::spawn(original.join());
        let request = peer.read().await;
        waiter.abort();
        assert!(waiter.await.unwrap_err().is_cancelled());
        let repeated = client
            .begin_review_context_retirement("original", &context)
            .unwrap();
        let repeated = tokio::spawn(repeated.join());
        let duplicate = timeout(
            intentd_test_support::test_timeout(Duration::from_millis(20)),
            peer.read(),
        )
        .await;
        assert!(
            duplicate.is_err(),
            "a second waiter started a new native retirement request"
        );
        assert!(
            !repeated.is_finished(),
            "local closure substituted for the held remote join"
        );
        peer.reply(&request, json!({"retired":joined})).await;
        assert_eq!(
            timeout(wait(), repeated).await.unwrap().unwrap().is_ok(),
            joined
        );
        let again = client
            .begin_review_context_retirement("original", &context)
            .unwrap();
        assert_eq!(timeout(wait(), again.join()).await.unwrap().is_ok(), joined);
        assert!(
            timeout(
                intentd_test_support::test_timeout(Duration::from_millis(20)),
                peer.read()
            )
            .await
            .is_err(),
            "terminal join result triggered an implicit retry"
        );
        assert!(!receipt.is_current());
    }
}

#[tokio::test]
async fn monitor_context_retirement_drop_before_wait_retains_original_join() {
    let (connection, mut peer) = pair(8192);
    let client = monitor_client(&connection, &mut peer, json!(1)).await;
    let query = monitor_session(&client, &mut peer, "original", json!(1)).await;
    let receipt = monitor_ack(query, &mut peer).await;
    let context = receipt.context().clone();
    drop(
        client
            .begin_review_context_retirement("original", &context)
            .unwrap(),
    );
    let request = timeout(
        intentd_test_support::test_timeout(Duration::from_millis(100)),
        peer.read(),
    )
    .await
    .expect("dropping the caller lost its exact remote retirement obligation");
    peer.reply(&request, json!({"retired":true})).await;
    assert!(timeout(
        wait(),
        client
            .begin_review_context_retirement("original", &context)
            .unwrap()
            .join()
    )
    .await
    .unwrap()
    .is_ok());
    assert!(timeout(
        intentd_test_support::test_timeout(Duration::from_millis(20)),
        peer.read()
    )
    .await
    .is_err());
    assert!(!receipt.is_current());
}

/// PROTOCOL §5.5b: a provider that requests workers sends the AIR
/// async-tasks extension merged beside the callback offer; one that does not
/// sends no `jetbrains` key at all (codex-acp would switch formats on it).
#[tokio::test]
async fn initialize_requests_async_tasks_only_for_opted_in_providers() {
    for (requests, expected) in [
        (
            true,
            json!({ "air": { "version": 1, "capabilities": ["asyncTasks"] } }),
        ),
        (false, Value::Null),
    ] {
        let (conn, mut peer) = pair(64 * 1024);
        let mut provider = config();
        provider.requests_agent_workers = requests;
        let run = tokio::spawn(async move {
            handshake_with_callbacks(conn, &provider, CallbackOffer::V1)
                .await
                .unwrap()
        });
        let request = peer.read().await;
        let meta = &request["params"]["clientCapabilities"]["_meta"];
        assert_eq!(meta["jetbrains"], expected, "requests={requests}");
        assert_eq!(
            meta[META_KEY],
            json!({ "version": 1 }),
            "callback offer kept"
        );
        peer.reply(
            &request,
            json!({ "protocolVersion": 1, "agentCapabilities": {}, "authMethods": [] }),
        )
        .await;
        run.await.unwrap();
    }
}
