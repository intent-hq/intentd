//! Real writer/TCP receipts only; these do not establish any adapter/model role.

use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

use intent_core::{BoxFuture, Caller, Result, Workspace, WorkspaceApi, WorkspaceId};
use tokio::io::AsyncReadExt;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::Notify;
use tokio::time::timeout;

use crate::mcp_server::repository_guidance::tests::{operation_result, revision, scope, session};
use crate::mcp_server::repository_guidance::{
    GuidanceCandidate, GuidanceLease, RepositoryGuidanceFence, RepositoryGuidanceSource,
};

const WAIT: Duration = Duration::from_secs(10);
const GUIDANCE: &str =
    "[Repository guidance — harness 3.0]\nTransport fixture, not an adapter role.";

struct FixtureApi {
    checkout: Option<String>,
    output_limit: u64,
}

impl WorkspaceApi for FixtureApi {
    fn settings_get(&self, path: String) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move {
            Ok(json!({"path":path,"value":match path.as_str() {
                "workspaceApi.toonOutput" => json!(false),
                "workspaceApi.maxOutputChars" => json!(self.output_limit),
                _ => Value::Null,
            }}))
        })
    }

    fn get_workspace(&self, id: WorkspaceId) -> BoxFuture<'_, Result<Workspace>> {
        Box::pin(async move {
            Ok(serde_json::from_value(json!({
                "id":id,"title":"fixture","branch":"feature","status":"Active",
                "activity":"idle","attention":"none","createdAt":"2026-09-27",
                "updatedAt":"2026-09-27","tags":[],"skipWorktree":false,
                "isRemote":false,"archived":false,"worktreePath":self.checkout,
            }))
            .unwrap())
        })
    }
}

#[derive(Default)]
struct FixtureSource {
    calls: AtomicUsize,
    callers: Mutex<Vec<Caller>>,
    lease: Mutex<Option<GuidanceLease>>,
}

impl RepositoryGuidanceSource for FixtureSource {
    fn prepare<'a>(
        &'a self,
        workspace: &'a WorkspaceId,
        caller: &'a Caller,
        fence: &'a RepositoryGuidanceFence,
    ) -> Pin<Box<dyn Future<Output = Option<GuidanceCandidate>> + Send + 'a>> {
        Box::pin(async move {
            assert_eq!(workspace.as_str(), "workspace-1");
            assert_eq!(intent_core::current_caller().as_ref(), Some(caller));
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.callers.lock().unwrap().push(caller.clone());
            let mut lease = self.lease.lock().unwrap();
            let lease =
                lease.get_or_insert_with(|| fence.replace_context(scope(), revision(1)).unwrap());
            lease.current_candidate(scope(), revision(1), GUIDANCE.into())
        })
    }
}

fn server(api: Arc<dyn WorkspaceApi>) -> WorkspaceMcpServer {
    WorkspaceMcpServer::new(api, "workspace-1".into()).with_caller_agent_id(Some("agent-1".into()))
}

fn api() -> Arc<dyn WorkspaceApi> {
    Arc::new(FixtureApi {
        checkout: None,
        output_limit: 100_000,
    })
}

fn call(id: u64, code: &str) -> Value {
    json!({"jsonrpc":"2.0","id":id,"method":"tools/call","params":{
        "name":"workspace_api","arguments":{"code":code,"summary":"Sender regression"}
    }})
}

async fn connect(bridge: &McpBridge) -> (BufReader<OwnedReadHalf>, OwnedWriteHalf) {
    let stream = TcpStream::connect(bridge.addr()).await.unwrap();
    let (read, write) = stream.into_split();
    (BufReader::new(read), write)
}

async fn send(write: &mut OwnedWriteHalf, message: &Value) {
    write
        .write_all(format!("{message}\n").as_bytes())
        .await
        .unwrap();
    write.flush().await.unwrap();
}

async fn read(reader: &mut BufReader<OwnedReadHalf>) -> Value {
    let mut line = String::new();
    assert!(
        timeout(WAIT, reader.read_line(&mut line))
            .await
            .unwrap()
            .unwrap()
            > 0
    );
    serde_json::from_str(&line).unwrap()
}

#[tokio::test]
async fn real_tcp_preserves_ordinary_items_values_errors_and_caller_scope() {
    let baseline = server(api());
    let source = Arc::new(FixtureSource::default());
    let candidate =
        Arc::new(server(api()).with_repository_guidance(&session(Some("3.0")), source.clone()));
    let bridge = serve_workspace_mcp_tcp(candidate).await.unwrap();
    let (mut reader, mut write) = connect(&bridge).await;
    for (index, code) in [
        "return undefined;",
        "return null;",
        "return {project:'original-project',remoteSourceSha:'A',localHeadSha:'B'};",
        "throw new Error('operation failed for original-project');",
        "return {__mcpContentItems:[{type:'text',text:'original-project'},{type:'image',mimeType:'image/png',data:'fixture-base64'},{type:'resource',resource:{uri:'fixture://original-project',mimeType:'text/plain',text:'remote A'}},{type:'audio',mimeType:'audio/wav',data:'fixture-audio'}]};",
    ].iter().enumerate() {
        let message = call(index as u64,code);
        let expected = baseline.handle_message(&message).await.unwrap();
        send(&mut write,&message).await;
        let mut actual = read(&mut reader).await;
        let item = actual["result"]["content"].as_array_mut().unwrap().pop().unwrap();
        assert_eq!(item,json!({"type":"text","text":GUIDANCE}));
        assert_eq!(actual,expected,"{code}");
    }
    assert_eq!(source.calls.load(Ordering::SeqCst), 5);
    assert!(source.callers.lock().unwrap().iter().all(|caller| *caller
        == Caller::Agent {
            agent_id: "agent-1".into()
        }));
    for message in [
        json!({"jsonrpc":"2.0","id":10,"method":"initialize"}),
        json!({"jsonrpc":"2.0","id":11,"method":"tools/call","params":{"name":"workspace_api","arguments":{"code":"return null;"}}}),
        json!({"jsonrpc":"2.0","id":12,"method":"unknown"}),
    ] {
        let expected = baseline.handle_message(&message).await.unwrap();
        send(&mut write, &message).await;
        assert_eq!(read(&mut reader).await, expected);
    }
    assert_eq!(source.calls.load(Ordering::SeqCst), 5);
}

#[tokio::test]
async fn real_tcp_old_unknown_absent_stamp_and_no_caller_never_invoke_guidance() {
    for version in [
        None,
        Some("1.0"),
        Some("1.1"),
        Some("2.0"),
        Some("2.1"),
        Some("2.2"),
        Some("2.3"),
        Some("2.4"),
        Some("2.5"),
        Some("2.6"),
        Some("2.7"),
        Some("2.8"),
        Some("2.9"),
        Some("unknown"),
        Some("3.0\n"),
    ] {
        let source = Arc::new(FixtureSource::default());
        let candidate =
            Arc::new(server(api()).with_repository_guidance(&session(version), source.clone()));
        let bridge = serve_workspace_mcp_tcp(candidate).await.unwrap();
        let (mut reader, mut write) = connect(&bridge).await;
        let message = call(1, "return 'original-project';");
        let expected = server(api()).handle_message(&message).await.unwrap();
        send(&mut write, &message).await;
        assert_eq!(read(&mut reader).await, expected, "{version:?}");
        assert_eq!(source.calls.load(Ordering::SeqCst), 0);
    }
    for caller in [None, Some(intent_core::AgentId::from("another-agent"))] {
        let source = Arc::new(FixtureSource::default());
        let candidate = server(api())
            .with_repository_guidance(&session(Some("3.0")), source.clone())
            .with_caller_agent_id(caller);
        let bridge = serve_workspace_mcp_tcp(Arc::new(candidate)).await.unwrap();
        let (mut reader, mut write) = connect(&bridge).await;
        send(&mut write, &call(2, "return null;")).await;
        assert_eq!(
            read(&mut reader).await["result"]["content"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        assert_eq!(source.calls.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn real_tcp_oversized_redirect_keeps_file_payload_and_adds_only_guidance() {
    let mut tmp = tempfile::Builder::new()
        .prefix("guidance-output-")
        .tempdir()
        .unwrap();
    if std::env::var_os("INTENTD_TEST_KEEP_TMP").is_some_and(|v| !v.is_empty()) {
        tmp.disable_cleanup(true);
    }
    let checkout = tmp.path().join("repo");
    std::fs::create_dir(&checkout).unwrap();
    let api = Arc::new(FixtureApi {
        checkout: Some(checkout.to_string_lossy().into_owned()),
        output_limit: 100,
    });
    let candidate = server(api)
        .with_repository_guidance(&session(Some("3.0")), Arc::new(FixtureSource::default()));
    let bridge = serve_workspace_mcp_tcp(Arc::new(candidate)).await.unwrap();
    let (mut reader, mut write) = connect(&bridge).await;
    send(
        &mut write,
        &call(1, "return 'original-project-'.repeat(200);"),
    )
    .await;
    let result = read(&mut reader).await;
    assert_eq!(result["result"]["isError"], false);
    let items = result["result"]["content"].as_array().unwrap();
    assert_eq!(items.len(), 2);
    let original = items[0]["text"].as_str().unwrap();
    assert!(original.contains("Output too large:"));
    assert_eq!(items[1]["text"], GUIDANCE);
    let files: Vec<_> = std::fs::read_dir(tmp.path().join("tool-outputs"))
        .unwrap()
        .collect();
    assert_eq!(files.len(), 1);
    let path = files[0].as_ref().unwrap().path();
    assert!(original.contains(path.to_str().unwrap()));
    assert_eq!(
        std::fs::read_to_string(path).unwrap(),
        serde_json::to_string_pretty(&"original-project-".repeat(200)).unwrap()
    );
}

async fn drain_writer(responses: Vec<BridgeResponse>, connection: ConnectionToken) -> Vec<Value> {
    let (write, mut read) = tokio::io::duplex(65536);
    let (tx, rx) = mpsc::channel(RESPONSE_CHANNEL_CAPACITY);
    for response in responses {
        tx.send(response).await.unwrap();
    }
    drop(tx);
    let writer = tokio::spawn(write_responses(write, rx, connection));
    let mut output = String::new();
    timeout(WAIT, read.read_to_string(&mut output))
        .await
        .unwrap()
        .unwrap();
    writer.await.unwrap();
    output
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

#[tokio::test]
async fn actual_writer_rechecks_already_queued_sidecars_after_revision_and_connection_retirement() {
    let fence = RepositoryGuidanceFence::default();
    let lease = fence.replace_context(scope(), revision(1)).unwrap();
    let pending = BridgeResponse {
        value: operation_result(1),
        guidance: lease.current_candidate(scope(), revision(1), "old".into()),
        guidance_request: None,
    };
    let (tx, rx) = mpsc::channel(RESPONSE_CHANNEL_CAPACITY);
    tx.send(pending).await.unwrap();
    // The candidate is already queued. A request-side/render-only check would
    // miss this transition; the real writer must suppress the old append.
    assert!(lease.advance(scope(), revision(2)));
    drop(tx);
    let (write, mut read) = tokio::io::duplex(65536);
    let connection = ConnectionLifetime::new();
    let writer = tokio::spawn(write_responses(write, rx, connection.token()));
    let mut output = String::new();
    timeout(WAIT, read.read_to_string(&mut output))
        .await
        .unwrap()
        .unwrap();
    writer.await.unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&output).unwrap(),
        operation_result(1)
    );
    let pending = BridgeResponse {
        value: operation_result(2),
        guidance: lease.current_candidate(scope(), revision(2), "current".into()),
        guidance_request: None,
    };
    connection.retire();
    assert_eq!(
        drain_writer(vec![pending], connection.token()).await,
        vec![operation_result(2)]
    );
}

struct RacingDispatch {
    fence: RepositoryGuidanceFence,
    lease: GuidanceLease,
    entered: Notify,
    release: Notify,
    replace_epoch: bool,
}

impl RacingDispatch {
    fn new(replace_epoch: bool) -> Arc<Self> {
        let fence = RepositoryGuidanceFence::default();
        let lease = fence.replace_context(scope(), revision(1)).unwrap();
        Arc::new(Self {
            fence,
            lease,
            entered: Notify::new(),
            release: Notify::new(),
            replace_epoch,
        })
    }
}

impl BridgeDispatch for RacingDispatch {
    fn dispatch(
        self: Arc<Self>,
        message: Value,
    ) -> Pin<Box<dyn Future<Output = Option<Value>> + Send>> {
        Box::pin(async move { Some(json!({"jsonrpc":"2.0","id":message["id"],"result":{}})) })
    }

    fn dispatch_for_bridge(
        self: Arc<Self>,
        message: Value,
    ) -> Pin<Box<dyn Future<Output = Option<BridgeResponse>> + Send>> {
        Box::pin(async move {
            let id = message["id"].as_u64().unwrap();
            if id == 99 {
                return self.dispatch(message).await.map(BridgeResponse::plain);
            }
            let guidance = if id == 1 {
                let candidate =
                    self.lease
                        .current_candidate(scope(), revision(1), "old guidance".into());
                self.entered.notify_one();
                self.release.notified().await;
                candidate
            } else if self.replace_epoch {
                let next =
                    intent_core::repository_context::RepositoryContextRevision::new("boot-B", 1);
                self.fence
                    .replace_context(scope(), next.clone())
                    .unwrap()
                    .current_candidate(scope(), next, "new guidance".into())
            } else {
                assert!(self.lease.advance(scope(), revision(2)));
                self.lease
                    .current_candidate(scope(), revision(2), "new guidance".into())
            };
            Some(BridgeResponse {
                value: operation_result(id),
                guidance,
                guidance_request: None,
            })
        })
    }
}

#[tokio::test]
async fn real_tcp_late_results_cannot_regress_revision_or_epoch_across_connections() {
    for replace_epoch in [false, true] {
        let dispatch = RacingDispatch::new(replace_epoch);
        let bridge = serve_mcp_tcp(dispatch.clone()).await.unwrap();
        let (mut old_reader, mut old_write) = connect(&bridge).await;
        let (mut new_reader, mut new_write) = connect(&bridge).await;
        send(&mut old_write, &json!({"id":1,"method":"slow"})).await;
        timeout(WAIT, dispatch.entered.notified()).await.unwrap();
        send(&mut old_write, &json!({"id":99,"method":"ping"})).await;
        assert_eq!(read(&mut old_reader).await["id"], 99);
        send(&mut new_write, &json!({"id":2,"method":"new"})).await;
        let current = read(&mut new_reader).await;
        assert_eq!(current["result"]["content"][1]["text"], "new guidance");
        dispatch.release.notify_one();
        let old = read(&mut old_reader).await;
        assert_eq!(old, operation_result(1));
    }
}

#[derive(Clone, Copy)]
enum FailingSource {
    Panic,
    Pending,
    ForeignFence,
}

#[tokio::test]
async fn retired_endpoint_preserves_pending_operation_without_guidance() {
    let dispatch = RacingDispatch::new(false);
    let bridge = serve_mcp_tcp(dispatch.clone()).await.unwrap();
    let (mut reader, mut write) = connect(&bridge).await;
    send(&mut write, &json!({"id":1,"method":"slow"})).await;
    timeout(WAIT, dispatch.entered.notified()).await.unwrap();

    // The existing connection retains its ordinary response behavior when the
    // endpoint handle drops. Its prepared guidance must retire with the owner.
    drop(bridge);
    dispatch.release.notify_one();
    assert_eq!(read(&mut reader).await, operation_result(1));
}

impl RepositoryGuidanceSource for FailingSource {
    fn prepare<'a>(
        &'a self,
        _: &'a WorkspaceId,
        _: &'a Caller,
        _: &'a RepositoryGuidanceFence,
    ) -> Pin<Box<dyn Future<Output = Option<GuidanceCandidate>> + Send + 'a>> {
        Box::pin(async move {
            match self {
                Self::Panic => panic!("intentional optional producer failure"),
                Self::Pending => std::future::pending().await,
                Self::ForeignFence => RepositoryGuidanceFence::default()
                    .replace_context(scope(), revision(1))
                    .unwrap()
                    .current_candidate(scope(), revision(1), "wrong session fence".into()),
            }
        })
    }
}

#[tokio::test]
async fn optional_producer_failure_timeout_and_foreign_fence_preserve_real_tcp_result() {
    for source in [
        FailingSource::Panic,
        FailingSource::Pending,
        FailingSource::ForeignFence,
    ] {
        let baseline = server(api());
        let candidate =
            server(api()).with_repository_guidance(&session(Some("3.0")), Arc::new(source));
        let bridge = serve_workspace_mcp_tcp(Arc::new(candidate)).await.unwrap();
        let (mut reader, mut write) = connect(&bridge).await;
        let message = call(1, "return {original:'project',remote:'A'};");
        let expected = baseline.handle_message(&message).await.unwrap();
        send(&mut write, &message).await;
        assert_eq!(read(&mut reader).await, expected);
    }
}

#[derive(Default)]
struct SignalledPendingSource {
    entered: Notify,
    cancelled: Notify,
}

struct PendingSourceGuard<'a>(&'a Notify);

impl Drop for PendingSourceGuard<'_> {
    fn drop(&mut self) {
        self.0.notify_one();
    }
}

impl RepositoryGuidanceSource for SignalledPendingSource {
    fn prepare<'a>(
        &'a self,
        _: &'a WorkspaceId,
        _: &'a Caller,
        _: &'a RepositoryGuidanceFence,
    ) -> Pin<Box<dyn Future<Output = Option<GuidanceCandidate>> + Send + 'a>> {
        Box::pin(async move {
            let _guard = PendingSourceGuard(&self.cancelled);
            self.entered.notify_one();
            std::future::pending().await
        })
    }
}

#[tokio::test]
async fn completed_operation_survives_watchdog_while_optional_guidance_is_pending() {
    let source = Arc::new(SignalledPendingSource::default());
    let candidate =
        Arc::new(server(api()).with_repository_guidance(&session(Some("3.0")), source.clone()));
    let bridge = serve_mcp_tcp_with_timeout(candidate, Duration::from_millis(500))
        .await
        .unwrap();
    let (mut reader, mut write) = connect(&bridge).await;
    let message = call(
        1,
        "return {project:'original-project',remoteSourceSha:'A'};",
    );
    let expected = server(api()).handle_message(&message).await.unwrap();
    send(&mut write, &message).await;
    // Source entry occurs only after the original operation has completed and
    // all result shaping is done. Advance the operation deadline at that point.
    timeout(WAIT, source.entered.notified()).await.unwrap();
    tokio::time::pause();
    tokio::time::advance(Duration::from_millis(501)).await;
    assert_eq!(read(&mut reader).await, expected);
}

#[tokio::test]
async fn pending_guidance_preserves_ping_response_and_cancels_on_peer_disconnect() {
    let source = Arc::new(SignalledPendingSource::default());
    let candidate =
        Arc::new(server(api()).with_repository_guidance(&session(Some("3.0")), source.clone()));
    let bridge = serve_workspace_mcp_tcp(candidate).await.unwrap();
    let (mut reader, mut write) = connect(&bridge).await;
    let ping = json!({"jsonrpc":"2.0","id":99,"method":"ping"});
    let expected = server(api()).handle_message(&ping).await.unwrap();
    send(&mut write, &call(1, "return 'completed-operation';")).await;
    timeout(WAIT, source.entered.notified()).await.unwrap();
    send(&mut write, &ping).await;
    assert_eq!(read(&mut reader).await, expected);
    drop(write);
    drop(reader);
    // Observe cancellation before the independent one-second guidance budget,
    // proving disconnect tears down the producer task rather than detaching it.
    timeout(Duration::from_millis(500), source.cancelled.notified())
        .await
        .unwrap();
}
