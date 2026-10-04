//! Actual MCP/TCP capture receipts with fixture scopes, not R authority proof.

use super::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Mutex, Weak};

use intent_core::{BoxFuture, Caller, Result, WorkspaceApi, WorkspaceId};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::time::timeout;

use crate::mcp_server::repository_guidance::tests::{revision, scope, session};
use crate::mcp_server::repository_guidance::{
    GuidanceCandidate, RepositoryGuidanceFence, RepositoryGuidanceSource,
};
use crate::mcp_server::request_context::{McpContextFuture, McpRequestContext, McpRequestScope};

const WAIT: Duration = Duration::from_secs(10);
const GUIDANCE: &str = "original request guidance";

struct Origin {
    id: u64,
    retired: AtomicBool,
}

#[derive(Clone)]
struct Snapshot {
    request: usize,
    origin: Option<(u64, Weak<Origin>)>,
}

tokio::task_local! { static ORIGINAL: Snapshot; }

#[derive(Debug, PartialEq)]
struct Seen {
    request: Option<usize>,
    origin: Option<u64>,
    retired: bool,
    caller: Option<Caller>,
}

fn seen() -> Seen {
    let snapshot = ORIGINAL.try_with(Clone::clone).ok();
    let origin = snapshot.as_ref().and_then(|s| s.origin.as_ref());
    Seen {
        request: snapshot.as_ref().map(|s| s.request),
        origin: origin.map(|(id, _)| *id),
        retired: origin
            .and_then(|(_, weak)| weak.upgrade())
            .is_none_or(|origin| origin.retired.load(Ordering::SeqCst)),
        caller: intent_core::current_caller(),
    }
}

struct Context {
    next: AtomicUsize,
    current: Mutex<Option<Arc<Origin>>>,
    captures: UnboundedSender<usize>,
    dropped: UnboundedSender<usize>,
}

struct Captured {
    snapshot: Snapshot,
    dropped: UnboundedSender<usize>,
}

impl McpRequestContext for Context {
    fn capture(&self) -> Arc<dyn McpRequestScope> {
        let request = self.next.fetch_add(1, Ordering::SeqCst);
        let origin = self
            .current
            .lock()
            .unwrap()
            .as_ref()
            .map(|origin| (origin.id, Arc::downgrade(origin)));
        let _ = self.captures.send(request);
        Arc::new(Captured {
            snapshot: Snapshot { request, origin },
            dropped: self.dropped.clone(),
        })
    }
}

impl McpRequestScope for Captured {
    fn scope<'a>(&'a self, request: McpContextFuture<'a>) -> McpContextFuture<'a> {
        Box::pin(ORIGINAL.scope(self.snapshot.clone(), request))
    }
}

impl Drop for Captured {
    fn drop(&mut self) {
        let _ = self.dropped.send(self.snapshot.request);
    }
}

struct ContextHarness {
    context: Arc<Context>,
    origin: Arc<Origin>,
    captures: UnboundedReceiver<usize>,
    dropped: UnboundedReceiver<usize>,
}

impl ContextHarness {
    fn new(ready: bool) -> Self {
        let (capture_tx, captures) = unbounded_channel();
        let (drop_tx, dropped) = unbounded_channel();
        let origin = Arc::new(Origin {
            id: 1,
            retired: AtomicBool::new(false),
        });
        let context = Arc::new(Context {
            next: AtomicUsize::new(1),
            current: Mutex::new(ready.then(|| origin.clone())),
            captures: capture_tx,
            dropped: drop_tx,
        });
        Self {
            context,
            origin,
            captures,
            dropped,
        }
    }

    // Deliberately change the fixture producer to detect accidental recapture.
    // This is not an allowed production endpoint-rebinding API.
    fn replace_fixture_origin(&self) {
        *self.context.current.lock().unwrap() = Some(Arc::new(Origin {
            id: 2,
            retired: AtomicBool::new(false),
        }));
    }
}

struct ProbeApi {
    entered: UnboundedSender<Seen>,
    gate: Option<Arc<Semaphore>>,
}

impl ProbeApi {
    fn new(gate: Option<Arc<Semaphore>>) -> (Arc<Self>, UnboundedReceiver<Seen>) {
        let (entered, receiver) = unbounded_channel();
        (Arc::new(Self { entered, gate }), receiver)
    }
}

impl WorkspaceApi for ProbeApi {
    fn git_root_list(&self, workspace_id: WorkspaceId) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move {
            assert_eq!(workspace_id, WorkspaceId::from("workspace-1"));
            let _ = self.entered.send(seen());
            if let Some(gate) = &self.gate {
                gate.acquire().await.unwrap().forget();
            }
            Ok(json!({"gitRoots":[{"operation":"original-result"}]}))
        })
    }

    fn settings_get(&self, path: String) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move {
            Ok(json!({"value":match path.as_str() {
                "workspaceApi.toonOutput" => json!(false),
                "workspaceApi.maxOutputChars" => json!(100_000),
                _ => Value::Null,
            }}))
        })
    }
}

struct ProbeSource {
    entered: UnboundedSender<Seen>,
    gate: Option<Arc<Semaphore>>,
}

impl ProbeSource {
    fn new(gate: Option<Arc<Semaphore>>) -> (Arc<Self>, UnboundedReceiver<Seen>) {
        let (entered, receiver) = unbounded_channel();
        (Arc::new(Self { entered, gate }), receiver)
    }
}

impl RepositoryGuidanceSource for ProbeSource {
    fn prepare<'a>(
        &'a self,
        _: &'a WorkspaceId,
        _: &'a Caller,
        fence: &'a RepositoryGuidanceFence,
    ) -> Pin<Box<dyn Future<Output = Option<GuidanceCandidate>> + Send + 'a>> {
        Box::pin(async move {
            let _ = self.entered.send(seen());
            if let Some(gate) = &self.gate {
                gate.acquire().await.unwrap().forget();
            }
            if seen().retired {
                return None;
            }
            fence
                .replace_context(scope(), revision(1))?
                .current_candidate(scope(), revision(1), GUIDANCE.into())
        })
    }
}

fn server(api: Arc<ProbeApi>, context: Option<Arc<Context>>) -> WorkspaceMcpServer {
    let mut server = WorkspaceMcpServer::new(api, "workspace-1".into())
        .with_caller_agent_id(Some("agent-1".into()));
    if let Some(context) = context {
        server = server.with_request_context(context);
    }
    server
}

fn call(id: u64) -> Value {
    json!({"jsonrpc":"2.0","id":id,"method":"tools/call","params":{
        "name":"workspace_api","arguments":{
            "code":"return (await ws.git.listRoots())[0];","summary":"Context fixture"
        }
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

async fn receive<T>(receiver: &mut UnboundedReceiver<T>) -> T {
    timeout(WAIT, receiver.recv()).await.unwrap().unwrap()
}

fn assert_original(response: &Value) {
    assert!(response.get("error").is_none(), "{response}");
    let text = response["result"]["content"][0]["text"].as_str().unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(text).unwrap(),
        json!({"operation":"original-result"})
    );
}

#[tokio::test]
async fn queued_request_captures_before_permit_without_starting_operation_or_rebinding() {
    let mut fixture = ContextHarness::new(true);
    let gate = Arc::new(Semaphore::new(0));
    let (api, mut operations) = ProbeApi::new(Some(gate.clone()));
    let bridge = serve_workspace_mcp_tcp(Arc::new(server(api, Some(fixture.context.clone()))))
        .await
        .unwrap();
    let (mut reader, mut write) = connect(&bridge).await;
    for id in 1..=MAX_IN_FLIGHT_REQUESTS {
        send(&mut write, &call(id as u64)).await;
        assert_eq!(receive(&mut fixture.captures).await, id);
        assert_eq!(receive(&mut operations).await.request, Some(id));
    }
    let queued = MAX_IN_FLIGHT_REQUESTS + 1;
    send(&mut write, &call(queued as u64)).await;
    assert_eq!(receive(&mut fixture.captures).await, queued);
    assert!(
        operations.try_recv().is_err(),
        "queued operation must not have started"
    );
    fixture.origin.retired.store(true, Ordering::SeqCst);
    fixture.replace_fixture_origin();
    gate.add_permits(queued);
    let old = receive(&mut operations).await;
    assert_eq!(old.request, Some(queued));
    assert_eq!(old.origin, Some(1));
    assert!(old.retired);
    assert_eq!(
        old.caller,
        Some(Caller::Agent {
            agent_id: "agent-1".into()
        })
    );
    let mut ids = Vec::new();
    for _ in 0..queued {
        let response = read(&mut reader).await;
        assert_original(&response);
        ids.push(response["id"].as_u64().unwrap());
    }
    ids.sort_unstable();
    assert_eq!(ids, (1..=queued as u64).collect::<Vec<_>>());
    assert_eq!(fixture.context.next.load(Ordering::SeqCst), queued + 1);
}

#[tokio::test]
async fn pending_capture_stays_absent_through_later_guidance_preparation() {
    let fixture = ContextHarness::new(false);
    let gate = Arc::new(Semaphore::new(0));
    let (api, mut operations) = ProbeApi::new(Some(gate.clone()));
    let (source, mut guidance) = ProbeSource::new(None);
    let candidate = server(api, Some(fixture.context.clone()))
        .with_repository_guidance(&session(Some("3.0")), source);
    let bridge = serve_workspace_mcp_tcp(Arc::new(candidate)).await.unwrap();
    let (mut reader, mut write) = connect(&bridge).await;
    send(&mut write, &call(1)).await;
    let original = receive(&mut operations).await;
    assert_eq!(original.origin, None);
    fixture.replace_fixture_origin();
    gate.add_permits(1);
    assert_eq!(receive(&mut guidance).await, original);
    let response = read(&mut reader).await;
    assert_original(&response);
    assert_eq!(response["result"]["content"].as_array().unwrap().len(), 1);
    assert_eq!(fixture.context.next.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn guidance_reuses_exact_operation_scope_and_observes_its_retirement() {
    for retire in [false, true] {
        let fixture = ContextHarness::new(true);
        let gate = Arc::new(Semaphore::new(0));
        let (api, mut operations) = ProbeApi::new(None);
        let (source, mut guidance) = ProbeSource::new(Some(gate.clone()));
        let candidate = server(api, Some(fixture.context.clone()))
            .with_repository_guidance(&session(Some("3.0")), source);
        let bridge = serve_workspace_mcp_tcp(Arc::new(candidate)).await.unwrap();
        let (mut reader, mut write) = connect(&bridge).await;
        send(&mut write, &call(1)).await;
        let original = receive(&mut operations).await;
        assert_eq!(receive(&mut guidance).await, original);
        fixture.replace_fixture_origin();
        fixture.origin.retired.store(retire, Ordering::SeqCst);
        gate.add_permits(1);
        let response = read(&mut reader).await;
        assert_original(&response);
        assert_eq!(
            response["result"]["content"].as_array().unwrap().len(),
            if retire { 1 } else { 2 }
        );
        if !retire {
            assert_eq!(response["result"]["content"][1]["text"], GUIDANCE);
        }
        assert_eq!(fixture.context.next.load(Ordering::SeqCst), 2);
    }
}

#[tokio::test]
async fn disconnect_cancels_preparation_and_drops_the_captured_request() {
    let mut fixture = ContextHarness::new(true);
    let (api, _) = ProbeApi::new(None);
    let (source, mut guidance) = ProbeSource::new(Some(Arc::new(Semaphore::new(0))));
    let candidate = server(api, Some(fixture.context.clone()))
        .with_repository_guidance(&session(Some("3.0")), source);
    let bridge = serve_workspace_mcp_tcp(Arc::new(candidate)).await.unwrap();
    let (reader, mut write) = connect(&bridge).await;
    send(&mut write, &call(1)).await;
    assert_eq!(receive(&mut guidance).await.request, Some(1));
    drop(reader);
    drop(write);
    assert_eq!(receive(&mut fixture.dropped).await, 1);
}

#[tokio::test(start_paused = true)]
async fn captured_scope_keeps_completed_result_outside_watchdog_but_not_hung_operation() {
    for hang_operation in [false, true] {
        let mut fixture = ContextHarness::new(true);
        let pending = Arc::new(Semaphore::new(0));
        let (api, mut operations) = ProbeApi::new(hang_operation.then(|| pending.clone()));
        let (source, mut guidance) = ProbeSource::new(Some(pending));
        let candidate = server(api, Some(fixture.context.clone()))
            .with_repository_guidance(&session(Some("3.0")), source);
        let bridge = serve_mcp_tcp_with_timeout(Arc::new(candidate), Duration::from_millis(500))
            .await
            .unwrap();
        let (mut reader, mut write) = connect(&bridge).await;
        send(&mut write, &call(1)).await;
        assert_eq!(receive(&mut operations).await.request, Some(1));
        if !hang_operation {
            assert_eq!(receive(&mut guidance).await.request, Some(1));
        }
        tokio::time::advance(Duration::from_secs(2)).await;
        let response = read(&mut reader).await;
        if hang_operation {
            assert_eq!(response["error"]["code"], BRIDGE_DISPATCH_TIMEOUT_CODE);
            assert!(guidance.try_recv().is_err());
        } else {
            assert_original(&response);
            assert_eq!(response["result"]["content"].as_array().unwrap().len(), 1);
        }
        assert_eq!(receive(&mut fixture.dropped).await, 1);
    }
}

#[tokio::test]
async fn missing_context_and_ineligible_stamps_preserve_ordinary_responses() {
    for version in [None, Some("2.9"), Some("unknown"), Some("3.0")] {
        let (api, mut operations) = ProbeApi::new(None);
        let (source, mut guidance) = ProbeSource::new(None);
        let candidate = server(api, None).with_repository_guidance(&session(version), source);
        let bridge = serve_workspace_mcp_tcp(Arc::new(candidate)).await.unwrap();
        let (mut reader, mut write) = connect(&bridge).await;
        send(&mut write, &call(1)).await;
        let original = receive(&mut operations).await;
        assert_eq!(original.request, None);
        assert!(original.retired);
        let response = read(&mut reader).await;
        assert_original(&response);
        assert_eq!(response["result"]["content"].as_array().unwrap().len(), 1);
        if version == Some("3.0") {
            assert_eq!(receive(&mut guidance).await, original);
        } else {
            assert!(guidance.try_recv().is_err());
        }
    }
}

#[tokio::test]
async fn installed_scope_preserves_original_protocol_errors_and_content_items() {
    let fixture = ContextHarness::new(true);
    let (api, _) = ProbeApi::new(None);
    let baseline = server(api.clone(), None);
    let candidate = server(api, Some(fixture.context.clone()));
    let bridge = serve_workspace_mcp_tcp(Arc::new(candidate)).await.unwrap();
    let (mut reader, mut write) = connect(&bridge).await;
    let mut messages = vec![
        json!({"jsonrpc":"2.0","id":"init","method":"initialize"}),
        json!({"jsonrpc":"2.0","id":"missing","method":"unknown"}),
    ];
    for (id, code) in [
        "return undefined;",
        "return null;",
        "return {project:'original',remoteSha:'A',localSha:'B'};",
        "throw new Error('original failure');",
        "return {__mcpContentItems:[{type:'text',text:'original'},{type:'image',mimeType:'image/png',data:'fixture'},{type:'resource',resource:{uri:'fixture://original',mimeType:'text/plain',text:'A'}},{type:'audio',mimeType:'audio/wav',data:'fixture'}]};",
    ].iter().enumerate() {
        let mut message = call(id as u64);
        message["params"]["arguments"]["code"] = json!(code);
        messages.push(message);
    }
    for message in &messages {
        let expected = baseline.handle_message(message).await.unwrap();
        send(&mut write, message).await;
        assert_eq!(read(&mut reader).await, expected);
    }
    assert_eq!(
        fixture.context.next.load(Ordering::SeqCst),
        messages.len() + 1
    );
}
