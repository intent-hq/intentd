//! Real manager/Store/R, Rust MCP endpoints and the frozen adapter/SDK.
//! The SDK's native child is a scripted control peer, never a provider process.

use super::super::tests::{callback, current, handle, Fixture};
use super::*;
use crate::repository_admission::lifecycle::RepositorySourceLifetime;
use crate::repository_admission::read_request::RepositoryReadRequest;
use crate::repository_admission::request_context::{current_read_request, current_source_lifetime};
use crate::repository_admission::AdmissionError;
use intent_acp::callback_registration::CallbackOffer;
use intent_acp::mcp_server::request_context::{McpRequestContext, McpRequestScope};
use intent_acp::{Connection, ConnectionHooks, IncomingNotification};
use intent_core::{BoxFuture, Caller, WorkspaceApi};
use intent_store::{RepositoryLifecycleKey, Store};
use serde_json::{json, Value};
use std::sync::atomic::AtomicUsize;
use std::time::Duration;
use tokio::sync::{mpsc, Mutex as TokioMutex};

const WAIT: Duration = Duration::from_secs(15);
const METHOD: &str = "_intent/session/register_mcp_callback";

struct ReadObservation {
    phase: String,
    read: AdmissionResult<Arc<RepositoryReadRequest>>,
    source: Option<RepositorySourceLifetime>,
}

struct ProbeApi {
    store: Store,
    caller: Caller,
    observations: Mutex<Vec<(bool, bool)>>,
    phases: Mutex<Vec<(String, bool, bool)>>,
    reads: Mutex<Vec<ReadObservation>>,
    retire_operation: AtomicBool,
    hold: AtomicBool,
    entered: Notify,
    release: Notify,
    block_pending: AtomicBool,
    pending_entered: AtomicUsize,
    pending_captured: AtomicUsize,
    pending_changed: Notify,
    pending_release: Notify,
}

impl WorkspaceApi for ProbeApi {
    fn git_root_list(
        &self,
        _: intent_core::WorkspaceId,
    ) -> BoxFuture<'_, intent_core::Result<Value>> {
        Box::pin(async move {
            // A fixture operation observes real source lifetime only. No Git
            // access, NativeRead admission or production entry is supplied.
            let phase = if self.retire_operation.swap(false, Ordering::SeqCst) {
                "operation-retire"
            } else {
                "operation-cleanup"
            };
            self.settings_get(phase.into()).await?;
            Ok(json!({"gitRoots":[]}))
        })
    }

    fn settings_get(&self, path: String) -> BoxFuture<'_, intent_core::Result<Value>> {
        Box::pin(async move {
            let active = current_source_lifetime().ok();
            self.reads.lock().unwrap().push(ReadObservation {
                phase: path.clone(),
                read: current_read_request(),
                source: current_source_lifetime().ok(),
            });
            let admitted = active.as_ref().is_some_and(|source| {
                let Caller::Agent { agent_id } = &self.caller else {
                    return false;
                };
                let Ok(lock) = source.subscribe(
                    &self.store,
                    &self.caller,
                    &[
                        RepositoryLifecycleKey::Database,
                        RepositoryLifecycleKey::Agent(agent_id.clone()),
                    ],
                ) else {
                    return false;
                };
                let child = source.retirement();
                assert!(child.check_current().is_ok());
                drop(lock);
                assert!(child.check_current().is_err());
                true
            });
            let retained = current_source_lifetime().is_ok();
            self.observations.lock().unwrap().push((admitted, retained));
            self.phases
                .lock()
                .unwrap()
                .push((path.clone(), admitted, retained));
            if path == "operation-retire" {
                crate::repository_admission::request_context::retire_current_request_on_denial(
                    crate::repository_admission::AdmissionError::Denied,
                );
            }
            if !admitted && self.block_pending.load(Ordering::SeqCst) {
                let released = self.pending_release.notified();
                tokio::pin!(released);
                released.as_mut().enable();
                self.pending_entered.fetch_add(1, Ordering::SeqCst);
                self.pending_changed.notify_waiters();
                if self.block_pending.load(Ordering::SeqCst) {
                    released.await;
                }
            }
            if self.hold.swap(false, Ordering::SeqCst) {
                self.entered.notify_one();
                self.release.notified().await;
            }
            Ok(
                json!({"path":path,"definition":{"path":path},"value":match path.as_str() {
                    "workspaceApi.toonOutput" => json!(false),
                    "workspaceApi.maxOutputChars" => json!(100_000),
                    _ => Value::Null,
                }}),
            )
        })
    }
}

// Observe the original R capture without replacing its scope or granting authority.
struct ObservedPendingContext {
    original: RepositoryCallbackContext,
    probe: Arc<ProbeApi>,
}

impl McpRequestContext for ObservedPendingContext {
    fn capture(&self) -> Arc<dyn McpRequestScope> {
        let captured = McpRequestContext::capture(&self.original);
        self.probe.pending_captured.fetch_add(1, Ordering::SeqCst);
        self.probe.pending_changed.notify_waiters();
        captured
    }
}

async fn wait_count(counter: &AtomicUsize, changed: &Notify, wanted: usize) {
    tokio::time::timeout(WAIT, async {
        loop {
            let notification = changed.notified();
            tokio::pin!(notification);
            notification.as_mut().enable();
            if counter.load(Ordering::SeqCst) >= wanted {
                return;
            }
            notification.await;
        }
    })
    .await
    .unwrap();
}

// An explicitly enabled disposable preparation observer emits no guidance and
// grants no native/read permission. It exercises the real retained ACP scope.
struct PrepareProbe(Arc<ProbeApi>);

impl intent_acp::mcp_server::repository_guidance::RepositoryGuidanceSource for PrepareProbe {
    fn prepare<'a>(
        &'a self,
        _: &'a intent_core::WorkspaceId,
        _: &'a Caller,
        _: &'a intent_acp::mcp_server::repository_guidance::RepositoryGuidanceFence,
    ) -> BoxFuture<'a, Option<intent_acp::mcp_server::repository_guidance::GuidanceCandidate>> {
        Box::pin(async move {
            self.0
                .settings_get("optional-preparation".into())
                .await
                .unwrap();
            None
        })
    }
}

#[derive(Default)]
struct PeerWriterGate {
    blocked: AtomicBool,
    entered: Notify,
    waker: Mutex<Option<std::task::Waker>>,
}
impl PeerWriterGate {
    fn block(&self) {
        self.blocked.store(true, Ordering::SeqCst);
    }
    async fn reached(&self) {
        tokio::time::timeout(WAIT, self.entered.notified())
            .await
            .unwrap();
    }
    fn resume(&self) {
        self.blocked.store(false, Ordering::SeqCst);
        if let Some(waker) = self.waker.lock().unwrap().take() {
            waker.wake();
        }
    }
}
struct PeerWriter {
    stdin: tokio::process::ChildStdin,
    gate: Arc<PeerWriterGate>,
}
impl tokio::io::AsyncWrite for PeerWriter {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        data: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        {
            let mut waker = self.gate.waker.lock().unwrap();
            if self.gate.blocked.load(Ordering::SeqCst) {
                *waker = Some(cx.waker().clone());
                self.gate.entered.notify_one();
                return std::task::Poll::Pending;
            }
        }
        std::pin::Pin::new(&mut self.stdin).poll_write(cx, data)
    }
    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.stdin).poll_flush(cx)
    }
    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.stdin).poll_shutdown(cx)
    }
}

struct NodePeer {
    child: tokio::process::Child,
    connection: Arc<Connection>,
    notes: Arc<TokioMutex<mpsc::UnboundedReceiver<IncomingNotification>>>,
    scratch: tempfile::TempDir,
    writer: Arc<PeerWriterGate>,
}

impl NodePeer {
    fn new() -> Self {
        Self::with_original_repository(None)
    }

    fn with_original_repository(repository: Option<&std::path::Path>) -> Self {
        let adapter = std::fs::canonicalize(
            std::env::var("INTENT_ACP_CALLBACK_ADAPTER_FIXTURE")
                .expect("explicit frozen da577fff adapter fixture"),
        )
        .unwrap();
        let dependencies = std::fs::canonicalize(adapter.join("node_modules")).unwrap();
        let scratch = crate::test_support::test_tempdir("manager-callback-node");
        let base =
            include_str!("../../../../../intent-acp/tests/fixtures/claude_callback_peer.mjs");
        // Test controls wrap only the scripted native peer. The original adapter
        // dispatcher, Query, SDK control serialization and MCP connections run unchanged.
        let source = base.replace("const requests = [];", r#"
const requests = [];
const { deferred } = await moduleAt("tests/mcp-peer.mjs");
let next = {};
let capability = "normal";
let promptNotes = [];
let promptFaults = [];
let promptFaultStreams = false;
let failLoad = false;
const initialize = agent.initialize.bind(agent);
agent.initialize = async (params) => {
    const response = await initialize(params);
    if (capability === "unsupported") delete response._meta?.intentCallbackRegistration;
    if (capability === "malformed") response._meta = { ...response._meta, intentCallbackRegistration: {version: "1", method: "_intent/session/register_mcp_callback"} };
    return response;
};
// Prompt notifications model provider output only; callback registration still
// traverses the unmodified adapter, Query and SDK control implementation.
agent.prompt = async (params) => {
    for (const update of promptNotes) process.stdout.write(`${JSON.stringify({jsonrpc:"2.0",method:"session/update",params:{sessionId:params.sessionId,update}})}\n`);
    return { stopReason: "end_turn" };
};
const processWaiters = [];
const waitProcess = (index) => processes[index] ?? (processWaiters[index] ??= deferred()).promise;
"#).replace("processes.push(child);", r#"
const nativeHandle = child.handle.bind(child);
child.setSettled = deferred();
child.handle = async (frame) => {
    try { return await nativeHandle(frame); }
    finally { if (frame.request.subtype === "mcp_set_servers") child.setSettled.resolve(); }
};
if (next.hold) child.hold = deferred();
if (next.error) child.error = next.error;
next = {};
processes.push(child);
processWaiters[processes.length - 1]?.resolve(child);
"#).replace(r#""fixture/inspect": return {"#, r#""fixture/next": next = p; return {};
        case "fixture/capability": capability = p.mode; return {};
        case "fixture/prompt-notes": promptNotes = p.notes; return {};
        case "fixture/prompt-faults": promptFaults = p.errors; promptFaultStreams = p.stream ?? false; return {};
        case "fixture/prompts": return requests.filter(r => r.method === "session/prompt");
        case "fixture/fail-load": failLoad = true; return {};
        case "fixture/ignore": return {};
        case "fixture/hold": {
            const child = processes[p.query ?? 0];
            child.hold = deferred(); child.setEntered = deferred();
            child.error = p.error ?? null; return {};
        }
        case "fixture/entered": return (await waitProcess(p.query ?? 0)).setEntered.promise;
        case "fixture/release": processes[p.query ?? 0].hold?.resolve(); return {};
        case "fixture/settled": await processes[p.query ?? 0].setSettled.promise; return {};
        case "fixture/names": return [...processes[p.query ?? 0].clients.keys()];
        case "fixture/note": process.stdout.write(`${JSON.stringify({jsonrpc:"2.0",method:"session/update",params:p})}\n`); return {};
        case "fixture/inspect": return {"#);
        let source = source.replace(
            "allocated.push(agent.sessions[response.sessionId]);",
            r#"
allocated.push(agent.sessions[response.sessionId]);
if (capability === "missing-receipt") delete response._meta?.intentCallbackRegistration;
"#,
        );
        let source = source.replace(
            "requests.push({ method: frame.method, params: frame.params });",
            r#"requests.push({ id: frame.id, method: frame.method, params: frame.params });
        if (frame.method === "session/load" && failLoad) {
            failLoad = false;
            process.stdout.write(`${JSON.stringify({jsonrpc:"2.0",id:frame.id,error:{code:-32603,message:"scripted lost resumable session"}})}\n`);
            return;
        }
        if (frame.method === "session/prompt" && promptFaults.length) {
            if (promptFaultStreams) process.stdout.write(`${JSON.stringify({jsonrpc:"2.0",method:"session/update",params:{sessionId:frame.params.sessionId,update:{sessionUpdate:"agent_message_chunk",content:{type:"text",text:"completed scripted output"}}}})}\n`);
            process.stdout.write(`${JSON.stringify({jsonrpc:"2.0",id:frame.id,error:{code:-32603,message:promptFaults.shift()}})}\n`);
            return;
        }"#,
        );
        assert_ne!(source, base);
        let script = scratch.path().join("peer.mjs");
        std::fs::write(&script, source).unwrap();
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
            .arg(format!("--allow-fs-read={}", adapter.display()))
            .arg(format!("--allow-fs-read={}", dependencies.display()))
            .arg(format!("--allow-fs-read={}", scratch.path().display()))
            .arg(format!("--allow-fs-write={}", scratch.path().display()));
        // A replacement connection must see the same original disposable cwd.
        // Native process spawning and all other filesystem paths stay denied.
        if let Some(repository) = repository {
            command.arg(format!("--allow-fs-read={}", repository.display()));
        }
        command
            .arg(script)
            .arg(adapter)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);
        let mut child = command.spawn().unwrap();
        let (tx, rx) = mpsc::unbounded_channel();
        let writer = Arc::new(PeerWriterGate::default());
        let connection = Arc::new(Connection::new(
            PeerWriter {
                stdin: child.stdin.take().unwrap(),
                gate: writer.clone(),
            },
            child.stdout.take().unwrap(),
            Some(Box::new(child.stderr.take().unwrap())),
            ConnectionHooks {
                notifications: Some(tx),
                ..ConnectionHooks::default()
            },
        ));
        Self {
            child,
            connection,
            notes: Arc::new(TokioMutex::new(rx)),
            scratch,
            writer,
        }
    }

    async fn call(&self, method: &str, params: Value) -> Value {
        self.connection
            .request_timeout(method, params, WAIT)
            .await
            .unwrap_or_else(|error| {
                panic!("{method}: {error}: {:?}", self.connection.recent_stderr())
            })
    }

    async fn finish(mut self) {
        self.child.kill().await.unwrap();
        self.child.wait().await.unwrap();
    }
}

struct Harness {
    f: Arc<Fixture>,
    node: NodePeer,
    origin: Arc<RepositoryOrigin>,
    probe: Arc<ProbeApi>,
    _user: McpBridge,
}

impl Harness {
    async fn new(stamp: &str, opt_in: bool) -> Self {
        Self::with_prepare_probe(stamp, opt_in, false).await
    }

    async fn with_prepare_probe(stamp: &str, opt_in: bool, preparation: bool) -> Self {
        Self::with_anchor(stamp, opt_in, preparation, false).await
    }

    async fn with_anchor(
        stamp: &str,
        opt_in: bool,
        preparation: bool,
        capture_before_install: bool,
    ) -> Self {
        let f = Fixture::with_initial_stamp("claude-code", Some(stamp)).await;
        assert_eq!(f.stored().await.harness_version, stamp);
        f.count_writes().await;
        let node = NodePeer::new();
        let original = Arc::new(f.manager.services.clone());
        let failed = capture_before_install.then(|| RepositoryReadOwner::capture(original.clone()));
        let origin = RepositoryOrigin::allocate(&original, &f.row).await;
        let read_owner = failed.unwrap_or_else(|| RepositoryReadOwner::capture(original));
        let probe = Arc::new(ProbeApi {
            store: f.manager.services.store.clone(),
            caller: f.caller(),
            observations: Mutex::new(Vec::new()),
            phases: Mutex::new(Vec::new()),
            reads: Mutex::new(Vec::new()),
            retire_operation: AtomicBool::new(false),
            hold: AtomicBool::new(false),
            entered: Notify::new(),
            release: Notify::new(),
            block_pending: AtomicBool::new(false),
            pending_entered: AtomicUsize::new(0),
            pending_captured: AtomicUsize::new(0),
            pending_changed: Notify::new(),
            pending_release: Notify::new(),
        });
        let blueprint = {
            let api = probe.clone();
            let workspace = f.row.workspace_id.clone();
            let agent = f.row.id.clone();
            let row = f.row.clone();
            // This proxy observes real R ownership; the actual Services allocation
            // above supplies the anchor. It grants no private read permission.
            ServerBlueprint::new(read_owner, move || {
                let server =
                    WorkspaceMcpServer::for_agent_type(api.clone(), workspace.clone(), "default")
                        .with_caller_agent_id(Some(agent.clone()));
                if preparation {
                    server.with_repository_guidance(&row, Arc::new(PrepareProbe(api.clone())))
                } else {
                    server
                }
            })
        };
        let pending = blueprint
            .server()
            .with_request_context(Arc::new(ObservedPendingContext {
                original: origin.pending_callback().unwrap(),
                probe: probe.clone(),
            }));
        let bridge = serve_workspace_mcp_tcp(Arc::new(pending)).await.unwrap();
        let user = serve_workspace_mcp_tcp(Arc::new(blueprint.server()))
            .await
            .unwrap();
        let (mut child, _, _) = handle(origin.clone());
        child.connection = node.connection.clone();
        child.notifications = node.notes.clone();
        child.session_mcp_servers = serde_json::from_value(json!([
            {"name":"workspace-mcp","command":"fixture-mcp","args":[bridge.connect_addr()],"env":[]},
            {"name":"user-kept","command":"fixture-mcp","args":[user.connect_addr()],"env":[]},
        ])).unwrap();
        #[expect(
            clippy::used_underscore_binding,
            reason = "Install the existing handle-owned RAII bridge in the actual manager fixture"
        )]
        {
            child._mcp_bridge = Some(bridge);
        }
        origin.configure_callbacks(EndpointBlueprint {
            server: blueprint,
            command: "fixture-mcp".into(),
            args_before_address: vec![],
            env: BTreeMap::new(),
        });
        if opt_in {
            origin.state.lock().unwrap().callback_offer = CallbackOffer::V1;
        }
        f.manager
            .handles
            .lock()
            .unwrap()
            .insert(f.row.id.clone(), child);
        Self {
            f: Arc::new(f),
            node,
            origin,
            probe,
            _user: user,
        }
    }

    async fn replacement(&self) -> (NodePeer, Arc<RepositoryOrigin>) {
        let row = self.f.stored().await;
        let blueprint = self.origin.state.lock().unwrap().blueprint.clone().unwrap();
        let old = super::super::take(
            &self.f.manager.handles,
            &self.f.row.id,
            &self.origin,
            Some(&self.f.manager.registry),
        )
        .unwrap();
        let mut servers = serde_json::to_value(&old.session_mcp_servers).unwrap();
        drop(old);
        let origin = RepositoryOrigin::allocate(&self.f.manager.services, &row).await;
        let node = NodePeer::new();
        let bridge = serve_workspace_mcp_tcp(Arc::new(
            blueprint
                .server
                .server()
                .with_request_context(Arc::new(origin.pending_callback().unwrap())),
        ))
        .await
        .unwrap();
        servers[0]["args"] = json!([bridge.connect_addr()]);
        let (mut child, _, _) = handle(origin.clone());
        child.connection = node.connection.clone();
        child.notifications = node.notes.clone();
        child.session_mcp_servers = serde_json::from_value(servers).unwrap();
        #[expect(
            clippy::used_underscore_binding,
            reason = "Install the replacement handle-owned bridge"
        )]
        {
            child._mcp_bridge = Some(bridge);
        }
        origin.configure_callbacks(EndpointBlueprint {
            server: blueprint.server.clone(),
            command: blueprint.command.clone(),
            args_before_address: blueprint.args_before_address.clone(),
            env: blueprint.env.clone(),
        });
        origin.state.lock().unwrap().callback_offer = CallbackOffer::V1;
        self.f
            .manager
            .handles
            .lock()
            .unwrap()
            .insert(self.f.row.id.clone(), child);
        (node, origin)
    }

    async fn start(&self) -> String {
        self.f
            .manager
            .start_session(
                &self.f.row.id,
                self.node.scratch.path().into(),
                intent_providers::provider_config("claude-code"),
            )
            .await
            .unwrap()
    }

    async fn aliases(&self, query: usize) -> Vec<String> {
        self.node
            .call("fixture/names", json!({"query":query}))
            .await
            .as_array()
            .unwrap()
            .iter()
            .filter_map(Value::as_str)
            .filter(|s| s.starts_with("intent-callback-"))
            .map(str::to_owned)
            .collect()
    }

    async fn mcp(&self, query: usize, name: &str, text: &str) -> Value {
        self.node.call("fixture/call", json!({"query":query,"name":name,"code":format!("return {}", serde_json::to_string(text).unwrap())})).await
    }
}

fn count_requests(inspect: &Value, method: &str) -> usize {
    inspect["requests"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|r| r["method"] == method)
        .count()
}

#[tokio::test]
async fn actual_manager_delivers_confirmed_endpoint_and_preserves_pending_and_user_routes() {
    let h = Harness::new("3.0", true).await;
    let pending = h.origin.pending_callback().unwrap().capture();
    let id = h.start().await;
    assert_eq!(
        h.f.stored().await.acp_session_id.as_deref(),
        Some(id.as_str())
    );
    assert_eq!(h.f.writes().await, 1);
    let aliases = h.aliases(0).await;
    assert_eq!(aliases.len(), 1);
    h.probe.observations.lock().unwrap().clear();
    assert!(h
        .mcp(0, &aliases[0], "confirmed result")
        .await
        .to_string()
        .contains("confirmed result"));
    let seen = h.probe.observations.lock().unwrap().clone();
    assert!(!seen.is_empty());
    assert!(
        seen.iter().all(|pair| *pair == (true, true)),
        "actual source leaves: {seen:?}"
    );
    h.probe.observations.lock().unwrap().clear();
    assert!(h
        .mcp(0, "workspace-mcp", "pending ordinary result")
        .await
        .to_string()
        .contains("pending ordinary result"));
    assert!(h
        .probe
        .observations
        .lock()
        .unwrap()
        .iter()
        .all(|pair| *pair == (false, false)));
    assert!(!current(&pending, h.f.caller()).await);
    assert!(h
        .mcp(0, "user-kept", "user result")
        .await
        .to_string()
        .contains("user result"));
    let inspect = h.node.call("fixture/inspect", json!({})).await;
    assert_eq!(count_requests(&inspect, "session/new"), 1);
    assert_eq!(count_requests(&inspect, METHOD), 1);
    assert_eq!(inspect["initializations"], json!([1]));
    assert_eq!(
        h.node.call("fixture/permissions", json!({})).await,
        json!({"filesystem":"denied","native":"denied"})
    );
    h.origin.retire();
    h.node.finish().await;
}

#[tokio::test]
async fn disabled_and_older_stamps_make_no_custom_calls_and_keep_ordinary_results() {
    for (stamp, opt_in) in [("2.9", true), ("2.8", true), ("3.0", false)] {
        let h = Harness::new(stamp, opt_in).await;
        h.start().await;
        assert_eq!(h.f.writes().await, 1);
        assert!(h.aliases(0).await.is_empty());
        assert!(h
            .mcp(0, "workspace-mcp", "ordinary")
            .await
            .to_string()
            .contains("ordinary"));
        let inspect = h.node.call("fixture/inspect", json!({})).await;
        assert_eq!(count_requests(&inspect, METHOD), 0);
        let init = inspect["requests"]
            .as_array()
            .unwrap()
            .iter()
            .find(|r| r["method"] == "initialize")
            .unwrap();
        assert!(init["params"]["clientCapabilities"]["_meta"]
            .get("intentCallbackRegistration")
            .is_none());
        h.origin.retire();
        h.node.finish().await;
    }
}

#[tokio::test]
async fn failed_delivery_preserves_single_committed_session_and_retires_original_owner() {
    let h = Harness::new("3.0", true).await;
    h.node
        .call("fixture/next", json!({"error":"original sdk refusal"}))
        .await;
    let id = h.start().await;
    assert_eq!(
        h.f.stored().await.acp_session_id.as_deref(),
        Some(id.as_str())
    );
    assert_eq!(h.f.writes().await, 1);
    assert!(h.aliases(0).await.is_empty());
    let captured = callback(&h.origin).unwrap().capture();
    assert!(!current(&captured, h.f.caller()).await);
    assert!(h.origin.state.lock().unwrap().endpoint.is_none());
    let inspect = h.node.call("fixture/inspect", json!({})).await;
    assert_eq!(count_requests(&inspect, METHOD), 1);
    assert!(h
        .mcp(0, "workspace-mcp", "completed ordinary")
        .await
        .to_string()
        .contains("completed ordinary"));
    h.node.finish().await;
}

fn start_task(h: &Harness) -> tokio::task::JoinHandle<intent_core::Result<String>> {
    let fixture = h.f.clone();
    let id = h.f.row.id.clone();
    let cwd = h.node.scratch.path().to_path_buf();
    tokio::spawn(async move {
        fixture
            .manager
            .start_session(&id, cwd, intent_providers::provider_config("claude-code"))
            .await
    })
}

#[tokio::test]
async fn cancelled_registration_retires_before_endpoint_drop_without_replaying_persistence() {
    let h = Harness::new("3.0", true).await;
    h.node.call("fixture/next", json!({"hold":true})).await;
    let task = start_task(&h);
    h.node.call("fixture/entered", json!({})).await;
    let captured = callback(&h.origin).unwrap().capture();
    assert!(current(&captured, h.f.caller()).await);
    assert_eq!(h.f.writes().await, 1);
    assert!(h.origin.state.lock().unwrap().endpoint.is_some());
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(!current(&captured, h.f.caller()).await);
    assert!(h.origin.state.lock().unwrap().endpoint.is_none());
    h.node.call("fixture/release", json!({})).await;
    h.node.call("fixture/settled", json!({})).await;
    let inspect = h.node.call("fixture/inspect", json!({})).await;
    assert_eq!(count_requests(&inspect, METHOD), 1);
    assert_eq!(count_requests(&inspect, "$/cancel_request"), 1);
    assert_eq!(h.f.writes().await, 1);
    assert!(h.aliases(0).await.is_empty());
    assert!(h
        .mcp(0, "workspace-mcp", "still ordinary")
        .await
        .to_string()
        .contains("still ordinary"));
    h.node.finish().await;
}

#[tokio::test]
async fn uncertain_sdk_completion_keeps_the_committed_response_without_retry_or_late_authority() {
    let h = Harness::new("3.0", true).await;
    h.node.call("fixture/next", json!({"hold":true})).await;
    let task = start_task(&h);
    h.node.call("fixture/entered", json!({})).await;
    let captured = callback(&h.origin).unwrap().capture();
    let id = tokio::time::timeout(WAIT, task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        h.f.stored().await.acp_session_id.as_deref(),
        Some(id.as_str())
    );
    assert_eq!(h.f.writes().await, 1);
    assert!(!current(&captured, h.f.caller()).await);
    assert!(h.origin.state.lock().unwrap().endpoint.is_none());
    h.node.call("fixture/release", json!({})).await;
    h.node.call("fixture/settled", json!({})).await;
    assert!(h.aliases(0).await.is_empty());
    assert_eq!(
        count_requests(&h.node.call("fixture/inspect", json!({})).await, METHOD),
        1
    );
    h.node.finish().await;
}

#[tokio::test]
async fn actual_load_and_recreate_keep_one_original_producer_and_accounting_per_attempt() {
    let h = Harness::new("3.0", true).await;
    let first = h.start().await;
    let old = callback(&h.origin).unwrap().capture();
    let loaded = h.start().await;
    assert_eq!(loaded, first);
    assert_eq!(h.f.writes().await, 1, "load never writes the ACP ID");
    assert!(!current(&old, h.f.caller()).await);
    let loaded_owner = callback(&h.origin).unwrap().capture();
    assert!(current(&loaded_owner, h.f.caller()).await);
    h.f.usage(23).await;
    h.f.manager
        .force_recreate
        .lock()
        .unwrap()
        .insert(h.f.row.id.clone());
    let replaced = h.start().await;
    assert_ne!(replaced, first);
    assert_eq!(h.f.writes().await, 2);
    assert!(!current(&loaded_owner, h.f.caller()).await);
    assert!(current(&callback(&h.origin).unwrap().capture(), h.f.caller()).await);
    let (usage, baseline) = h.f.accounting().await;
    assert!(usage.is_none());
    assert_eq!(baseline.unwrap()["inputTokens"], 23);
    let inspect = h.node.call("fixture/inspect", json!({})).await;
    assert_eq!(count_requests(&inspect, "session/new"), 2);
    assert_eq!(count_requests(&inspect, "session/load"), 1);
    assert_eq!(count_requests(&inspect, METHOD), 3);
    let query = usize::try_from(inspect["queries"].as_u64().unwrap()).unwrap() - 1;
    let aliases = h.aliases(query).await;
    assert!(!aliases.is_empty());
    h.probe.observations.lock().unwrap().clear();
    assert!(h
        .mcp(query, aliases.last().unwrap(), "replacement result")
        .await
        .to_string()
        .contains("replacement result"));
    assert!(h
        .probe
        .observations
        .lock()
        .unwrap()
        .iter()
        .all(|pair| *pair == (true, true)));
    h.origin.retire();
    h.node.finish().await;
}

#[tokio::test]
async fn actual_soft_interrupt_and_idle_preserve_endpoint_but_hard_stop_retires_it() {
    let h = Harness::new("3.0", true).await;
    h.start().await;
    let aliases = h.aliases(0).await;
    let before = callback(&h.origin).unwrap().capture();
    let endpoint = h.origin.state.lock().unwrap().endpoint.clone();
    let endpoint = endpoint.expect("confirmed original endpoint installed");
    assert!(h.f.manager.interrupt(&h.f.row.id).await);
    assert!(!current(&before, h.f.caller()).await);
    assert!(current(&callback(&h.origin).unwrap().capture(), h.f.caller()).await);
    assert!(Arc::ptr_eq(
        &endpoint,
        h.origin.state.lock().unwrap().endpoint.as_ref().unwrap()
    ));
    h.probe.observations.lock().unwrap().clear();
    h.mcp(0, &aliases[0], "after soft stop").await;
    assert!(h
        .probe
        .observations
        .lock()
        .unwrap()
        .iter()
        .all(|pair| *pair == (true, true)));
    let captured = callback(&h.origin).unwrap().capture();
    assert!(h.f.manager.stop(&h.f.row.id).await);
    assert!(!current(&captured, h.f.caller()).await);
    assert!(endpoint.bridge.lock().unwrap().is_none());
    // An accepted old socket may still finish ordinary work after the listener closes.
    assert!(h
        .mcp(0, &aliases[0], "completed ordinary after hard stop")
        .await
        .to_string()
        .contains("completed ordinary after hard stop"));
    h.node.finish().await;
}

#[tokio::test]
async fn late_registration_for_the_same_handle_preserves_new_owner_in_both_sdk_completion_orders() {
    for old_first in [true, false] {
        let h = Harness::new("3.0", true).await;
        h.node.call("fixture/next", json!({"hold":true})).await;
        let old = start_task(&h);
        h.node.call("fixture/entered", json!({})).await;
        let old_id = h.f.stored().await.acp_session_id.unwrap();
        let old_capture = callback(&h.origin).unwrap().capture();
        h.node.call("fixture/next", json!({"hold":true})).await;
        h.f.manager
            .force_recreate
            .lock()
            .unwrap()
            .insert(h.f.row.id.clone());
        let replacement = start_task(&h);
        h.node.call("fixture/entered", json!({"query":1})).await;
        let fresh = callback(&h.origin).unwrap().capture();
        assert!(!current(&old_capture, h.f.caller()).await);
        assert!(current(&fresh, h.f.caller()).await);
        assert_eq!(old.await.unwrap().unwrap(), old_id);
        if old_first {
            h.node.call("fixture/release", json!({"query":0})).await;
            h.node.call("fixture/settled", json!({"query":0})).await;
        }
        h.node.call("fixture/release", json!({"query":1})).await;
        let new_id = replacement.await.unwrap().unwrap();
        if !old_first {
            h.node.call("fixture/release", json!({"query":0})).await;
            h.node.call("fixture/settled", json!({"query":0})).await;
        }
        assert_ne!(new_id, old_id);
        assert_eq!(
            h.f.stored().await.acp_session_id.as_deref(),
            Some(new_id.as_str())
        );
        assert_eq!(h.f.writes().await, 2);
        assert!(current(&fresh, h.f.caller()).await);
        assert!(h.aliases(0).await.is_empty());
        assert_eq!(h.aliases(1).await.len(), 1);
        let inspect = h.node.call("fixture/inspect", json!({})).await;
        assert_eq!(count_requests(&inspect, "session/new"), 2);
        assert_eq!(count_requests(&inspect, METHOD), 2);
        h.origin.retire();
        h.node.finish().await;
    }
}

#[tokio::test]
async fn held_old_completed_response_survives_distinct_registration() {
    let h = Harness::new("3.0", true).await;
    h.node.call("fixture/next", json!({"hold":true})).await;
    let start = start_task(&h);
    h.node.call("fixture/entered", json!({})).await;
    h.probe.hold.store(true, Ordering::SeqCst);
    let old_call = h.mcp(0, "workspace-mcp", "original completed value");
    tokio::pin!(old_call);
    tokio::select! {
        result = &mut old_call => panic!("old response bypassed the real settings await: {result}"),
        () = h.probe.entered.notified() => {},
    }
    h.node.call("fixture/release", json!({})).await;
    let id = start.await.unwrap().unwrap();
    assert_eq!(
        h.f.stored().await.acp_session_id.as_deref(),
        Some(id.as_str())
    );
    assert_eq!(h.aliases(0).await.len(), 1);
    h.probe.release.notify_one();
    assert!(old_call
        .await
        .to_string()
        .contains("original completed value"));
    assert_eq!(h.f.writes().await, 1);
    h.origin.retire();
    // The borrowed original response future is complete before fixture teardown.
}

#[tokio::test]
async fn malformed_unsupported_and_missing_receipts_preserve_session_without_registration() {
    for mode in ["unsupported", "malformed", "missing-receipt"] {
        let h = Harness::new("3.0", true).await;
        h.node
            .call("fixture/capability", json!({"mode":mode}))
            .await;
        let id = h.start().await;
        assert_eq!(
            h.f.stored().await.acp_session_id.as_deref(),
            Some(id.as_str())
        );
        assert_eq!(h.f.writes().await, 1);
        let inspect = h.node.call("fixture/inspect", json!({})).await;
        assert_eq!(count_requests(&inspect, "session/new"), 1);
        assert_eq!(count_requests(&inspect, METHOD), 0, "mode {mode}");
        assert!(h.origin.state.lock().unwrap().endpoint.is_none());
        assert!(h
            .mcp(0, "workspace-mcp", "ordinary unavailable extension")
            .await
            .to_string()
            .contains("ordinary unavailable extension"));
        assert!(h
            .probe
            .observations
            .lock()
            .unwrap()
            .iter()
            .all(|p| *p == (false, false)));
        h.origin.retire();
        h.node.finish().await;
    }
}

#[tokio::test]
async fn pending_request_queued_before_permit_never_adopts_new_endpoint_authority() {
    let h = Harness::new("3.0", true).await;
    h.node.call("fixture/next", json!({"hold":true})).await;
    let start = start_task(&h);
    h.node.call("fixture/entered", json!({})).await;
    h.probe.block_pending.store(true, Ordering::SeqCst);
    let original_captures = h.probe.pending_captured.load(Ordering::SeqCst);
    let mut requests = Vec::new();
    // One actual TCP connection has sixteen permits. Its seventeenth capture
    // is observed synchronously by a delegating observer before the permit wait.
    for i in 0..17 {
        let connection = h.node.connection.clone();
        requests.push(tokio::spawn(async move {
            connection
                .request_timeout(
                    "fixture/call",
                    json!({"name":"workspace-mcp","code":format!("return 'queued-{i}'")}),
                    WAIT,
                )
                .await
                .unwrap()
        }));
    }
    wait_count(&h.probe.pending_entered, &h.probe.pending_changed, 16).await;
    wait_count(
        &h.probe.pending_captured,
        &h.probe.pending_changed,
        original_captures + 17,
    )
    .await;
    assert_eq!(h.probe.pending_entered.load(Ordering::SeqCst), 16);
    h.node.call("fixture/release", json!({})).await;
    start.await.unwrap().unwrap();
    let alias = h.aliases(0).await.pop().unwrap();
    h.probe.observations.lock().unwrap().clear();
    h.probe.reads.lock().unwrap().clear();
    h.mcp(0, &alias, "new source").await;
    assert!(h
        .probe
        .observations
        .lock()
        .unwrap()
        .iter()
        .all(|p| *p == (true, true)));
    assert!(h.probe.reads.lock().unwrap().iter().all(|p| p.read.is_ok()));
    h.probe.observations.lock().unwrap().clear();
    h.probe.reads.lock().unwrap().clear();
    h.probe.block_pending.store(false, Ordering::SeqCst);
    h.probe.pending_release.notify_waiters();
    for request in requests {
        assert!(request.await.unwrap().to_string().contains("queued-"));
    }
    let observations = h.probe.observations.lock().unwrap().clone();
    assert!(!observations.is_empty());
    assert!(observations.iter().all(|p| *p == (false, false)));
    assert!(h
        .probe
        .reads
        .lock()
        .unwrap()
        .iter()
        .all(|p| matches!(p.read, Err(AdmissionError::Unavailable))));
    assert_eq!(h.f.writes().await, 1);
    h.origin.retire();
    h.node.finish().await;
}

#[tokio::test]
async fn late_registration_on_replaced_handle_leaves_new_connection_and_owner_intact() {
    for old_first in [true, false] {
        let h = Harness::new("3.0", true).await;
        h.node.call("fixture/next", json!({"hold":true})).await;
        let old = start_task(&h);
        h.node.call("fixture/entered", json!({})).await;
        let old_id = h.f.stored().await.acp_session_id.unwrap();
        let old_capture = callback(&h.origin).unwrap().capture();
        let old_endpoint = h.origin.state.lock().unwrap().endpoint.clone().unwrap();
        let (node, origin) = h.replacement().await;
        assert!(!current(&old_capture, h.f.caller()).await);
        assert!(old_endpoint.bridge.lock().unwrap().is_none());
        node.call("fixture/next", json!({"hold":true})).await;
        h.f.manager
            .force_recreate
            .lock()
            .unwrap()
            .insert(h.f.row.id.clone());
        let fixture = h.f.clone();
        let cwd = node.scratch.path().to_path_buf();
        let mut new = tokio::spawn(async move {
            fixture
                .manager
                .start_session(
                    &fixture.row.id,
                    cwd,
                    intent_providers::provider_config("claude-code"),
                )
                .await
        });
        tokio::select! {
            result = &mut new => panic!("replacement ended before SDK registration: {result:?}"),
            _ = node.call("fixture/entered", json!({})) => {},
        }
        let fresh = callback(&origin).unwrap().capture();
        assert_eq!(old.await.unwrap().unwrap(), old_id);
        if old_first {
            h.node.call("fixture/release", json!({})).await;
            h.node.call("fixture/settled", json!({})).await;
        }
        node.call("fixture/release", json!({})).await;
        let new_id = new.await.unwrap().unwrap();
        if !old_first {
            h.node.call("fixture/release", json!({})).await;
            h.node.call("fixture/settled", json!({})).await;
        }
        assert_ne!(new_id, old_id);
        assert_eq!(
            h.f.stored().await.acp_session_id.as_deref(),
            Some(new_id.as_str())
        );
        assert_eq!(h.f.writes().await, 2);
        assert!(current(&fresh, h.f.caller()).await);
        assert!(h.aliases(0).await.is_empty());
        let names = node.call("fixture/names", json!({})).await;
        let name = names
            .as_array()
            .unwrap()
            .iter()
            .filter_map(Value::as_str)
            .find(|name| name.starts_with("intent-callback-"))
            .unwrap();
        h.probe.observations.lock().unwrap().clear();
        let response = node
            .call(
                "fixture/call",
                json!({"name":name,"code":"return 'new physical result'"}),
            )
            .await;
        assert!(response.to_string().contains("new physical result"));
        assert!(h
            .probe
            .observations
            .lock()
            .unwrap()
            .iter()
            .all(|p| *p == (true, true)));
        for peer in [&h.node, &node] {
            let inspect = peer.call("fixture/inspect", json!({})).await;
            assert_eq!(count_requests(&inspect, "session/new"), 1);
            assert_eq!(count_requests(&inspect, METHOD), 1);
        }
        origin.retire();
        node.finish().await;
        h.node.finish().await;
    }
}

async fn metadata_delivery_ordering(replace_handle: bool, install_before_old: bool) {
    use crate::repository_admission::lifecycle::physical_owner::RepositoryCreationIntent;
    let h = Harness::new("3.0", true).await;
    let store = &h.f.manager.services.store;
    store
        .set_agent_effort_levels(
            &h.f.row.workspace_id,
            &h.f.row.id,
            Some(&["previous".into()]),
            "2026-09-28T00:00:00Z",
        )
        .await
        .unwrap();
    for sql in [
        "CREATE TABLE observed_model_metadata (id TEXT)",
        "CREATE TABLE observed_effort_metadata (id TEXT)",
        "CREATE TRIGGER pause_model_metadata AFTER UPDATE OF resolved_model ON agent_session BEGIN INSERT INTO observed_model_metadata VALUES (NEW.id); END",
        "CREATE TRIGGER pause_effort_metadata AFTER UPDATE OF effort_levels ON agent_session BEGIN INSERT INTO observed_effort_metadata VALUES (NEW.id); END",
    ] { sqlx::query(sql).execute(store.write_pool()).await.unwrap(); }
    let (model_entered, model_wait) = tokio::sync::oneshot::channel();
    let (model_release, model_resume) = std::sync::mpsc::channel();
    let (effort_entered, effort_wait) = tokio::sync::oneshot::channel();
    let (effort_release, effort_resume) = std::sync::mpsc::channel();
    let mut model_pause = Some((model_entered, model_resume));
    let mut effort_pause = Some((effort_entered, effort_resume));
    let mut writer = store.write_pool().acquire().await.unwrap();
    writer
        .lock_handle()
        .await
        .unwrap()
        .set_update_hook(move |change| {
            let gate = match change.table {
                "observed_model_metadata" => model_pause.take(),
                "observed_effort_metadata" => effort_pause.take(),
                _ => None,
            };
            if let Some((entered, resume)) = gate {
                let _ = entered.send(());
                let _ = resume.recv_timeout(WAIT);
            }
        });
    drop(writer);
    let original = start_task(&h);
    tokio::time::timeout(WAIT, model_wait)
        .await
        .unwrap()
        .unwrap();
    let old_id = h.f.stored().await.acp_session_id.unwrap();
    assert!(
        callback(&h.origin).is_none(),
        "consumed owner remains inside finish metadata await"
    );
    let replacement = if replace_handle {
        Some(h.replacement().await)
    } else {
        None
    };
    let (peer, origin) = replacement
        .as_ref()
        .map_or((&h.node, &h.origin), |(node, origin)| (node, origin));
    let client = intent_acp::handshake::handshake_with_callbacks(
        peer.connection.clone(),
        intent_providers::provider_config("claude-code"),
        CallbackOffer::V1,
    )
    .await
    .unwrap()
    .callbacks
    .unwrap();
    let servers = h.f.manager.handles.lock().unwrap()[&h.f.row.id]
        .session_mcp_servers
        .clone();
    let (creator, attempt) = origin
        .begin_session(RepositoryCreationIntent::Replace {
            expected: Some(old_id.clone()),
        })
        .unwrap();
    let (producer_done, produced) = tokio::sync::oneshot::channel();
    let mut initializing = Box::pin(creator.initialize_compatible(|| async {
        let response = client
            .new_session(peer.scratch.path(), servers, None)
            .await?;
        producer_done.send(()).unwrap();
        Ok::<_, intent_acp::AcpError>((response.response.session_id.0.to_string(), response))
    }));
    tokio::select! {
        biased;
        result = &mut initializing => panic!("Store writer still held: {:?}", result.result),
        produced = produced => produced.unwrap(),
    }
    model_release.send(()).unwrap();
    let result = initializing.await;
    let response = result.producer.unwrap();
    let new_id = response.response.session_id.0.to_string();
    assert_eq!(
        crate::agent_session::compatibility_session_id(result.result.unwrap()),
        new_id
    );
    let owner = result.owner.unwrap();
    let captured = owner.callback().capture();
    let outcome = crate::agent_session::RepositorySessionOutcome {
        response: new_id.clone(),
        owner: Ok(owner),
        query: response.query,
    };
    let mut later = Some((attempt, outcome));
    if install_before_old {
        let (attempt, outcome) = later.take().unwrap();
        let (id, delivery) = origin.accept_session(attempt, &h.node.connection, outcome);
        assert_eq!(id, new_id);
        assert!(delivery.is_some());
        deliver_optional(delivery).await;
    }
    tokio::time::timeout(WAIT, effort_wait)
        .await
        .unwrap()
        .unwrap();
    assert!(current(&captured, h.f.caller()).await);
    effort_release.send(()).unwrap();
    assert_eq!(original.await.unwrap().unwrap(), old_id);
    if let Some((attempt, outcome)) = later {
        assert!(
            callback(origin).is_none(),
            "old completion must not install before its successor"
        );
        let (id, delivery) = origin.accept_session(attempt, &h.node.connection, outcome);
        assert_eq!(id, new_id);
        deliver_optional(delivery).await;
    }
    assert!(current(&captured, h.f.caller()).await);
    assert_eq!(
        h.f.stored().await.acp_session_id.as_deref(),
        Some(new_id.as_str())
    );
    assert_eq!(h.f.writes().await, 2);
    let inspect = h.node.call("fixture/inspect", json!({})).await;
    let (new_calls, registrations) = if let Some((node, _)) = &replacement {
        let second = node.call("fixture/inspect", json!({})).await;
        (
            count_requests(&inspect, "session/new") + count_requests(&second, "session/new"),
            count_requests(&inspect, METHOD) + count_requests(&second, METHOD),
        )
    } else {
        (
            count_requests(&inspect, "session/new"),
            count_requests(&inspect, METHOD),
        )
    };
    assert_eq!(new_calls, 2);
    assert_eq!(
        registrations, 1,
        "obsolete receipt never reaches the custom dispatcher"
    );
    let query = usize::from(!replace_handle);
    let names = peer.call("fixture/names", json!({"query":query})).await;
    let alias = names
        .as_array()
        .unwrap()
        .iter()
        .filter_map(Value::as_str)
        .find(|name| name.starts_with("intent-callback-"))
        .unwrap();
    h.probe.observations.lock().unwrap().clear();
    peer.call(
        "fixture/call",
        json!({"query":query,"name":alias,"code":"return 'latest metadata owner'"}),
    )
    .await;
    assert!(h
        .probe
        .observations
        .lock()
        .unwrap()
        .iter()
        .all(|p| *p == (true, true)));
    let mut writer = store.write_pool().acquire().await.unwrap();
    writer.lock_handle().await.unwrap().remove_update_hook();
    drop(writer);
    origin.retire();
    if let Some((node, _)) = replacement {
        node.finish().await;
    }
    h.node.finish().await;
}

#[tokio::test]
async fn metadata_await_keeps_original_query_and_rejects_old_delivery_in_both_install_orders() {
    for replace_handle in [false, true] {
        for install_before_old in [false, true] {
            metadata_delivery_ordering(replace_handle, install_before_old).await;
        }
    }
}

fn tool_update(id: &str, title: &str) -> Value {
    json!({"sessionUpdate":"tool_call","toolCallId":id,"title":title,"status":"completed",
        "rawInput":{"code":"return 1"},"rawOutput":{"originalResult":id}})
}

async fn transcript_tools(h: &Harness) -> Vec<Value> {
    h.f.manager
        .services
        .store
        .get_agent_messages(&h.f.row.id, None)
        .await
        .unwrap()
        .into_iter()
        .flat_map(|m| m.content.as_array().unwrap().clone())
        .filter(|b| b["type"] == "tool_use")
        .collect()
}

#[tokio::test]
async fn prompt_transcript_uses_only_original_connection_exact_routes_and_preserves_result_names() {
    let h = Harness::new("3.0", true).await;
    let id = h.start().await;
    let alias = h.aliases(0).await.pop().unwrap();
    let title = format!("mcp__{alias}__workspace_api");
    let foreign = "mcp__foreign__workspace_api";
    let nested = format!("{title}__nested");
    h.node.call("fixture/prompt-notes",json!({"notes":[
        {"sessionUpdate":"tool_call","toolCallId":"known","title":title,"status":"in_progress","rawInput":{"code":"return 1"}},
        {"sessionUpdate":"tool_call_update","toolCallId":"known","status":"completed","rawOutput":{"originalResult":"known"}},
        tool_update("foreign",foreign), tool_update("nested",&nested),
    ]})).await;
    {
        let mut notes = h.node.notes.lock().await;
        while notes.try_recv().is_ok() {}
        h.f.manager
            .services
            .run_prompt_turn(
                &h.node.connection,
                &mut notes,
                &h.f.row.id,
                &h.f.row.workspace_id,
                &id,
                vec![
                    serde_json::from_value(json!({"type":"text","text":"local scripted output"}))
                        .unwrap(),
                ],
                None,
            )
            .await
            .unwrap();
    }
    let tools = transcript_tools(&h).await;
    assert_eq!(tools.len(), 3);
    let known = tools.iter().find(|b| b["toolCallId"] == "known").unwrap();
    assert_eq!(known["name"], "workspace_api");
    assert_eq!(known["input"]["_acpTitle"], title);
    assert_eq!(
        tools.iter().find(|b| b["toolCallId"] == "foreign").unwrap()["name"],
        "foreign_workspace_api"
    );
    assert_eq!(
        tools.iter().find(|b| b["toolCallId"] == "nested").unwrap()["name"],
        format!("{alias}_workspace_api__nested")
    );
    let messages =
        h.f.manager
            .services
            .store
            .get_agent_messages(&h.f.row.id, None)
            .await
            .unwrap();
    assert!(messages
        .iter()
        .any(|m| m.content.to_string().contains("originalResult")));
    // The same literal alias arriving on another actual ACP connection has no registration.
    let other = NodePeer::new();
    other
        .call(
            "fixture/prompt-notes",
            json!({"notes":[tool_update("other-connection",&title)]}),
        )
        .await;
    {
        let mut notes = other.notes.lock().await;
        h.f.manager
            .services
            .run_prompt_turn(
                &other.connection,
                &mut notes,
                &h.f.row.id,
                &h.f.row.workspace_id,
                &id,
                vec![
                    serde_json::from_value(json!({"type":"text","text":"foreign connection"}))
                        .unwrap(),
                ],
                None,
            )
            .await
            .unwrap();
    }
    let tools = transcript_tools(&h).await;
    assert_eq!(
        tools
            .iter()
            .find(|b| b["toolCallId"] == "other-connection")
            .unwrap()["name"],
        format!("{alias}_workspace_api")
    );
    assert_eq!(h.f.writes().await, 1);
    h.origin.retire();
    other.finish().await;
    h.node.finish().await;
}

#[tokio::test]
async fn idle_wake_and_zero_settle_transcripts_keep_captured_connection_routes() {
    let h = Harness::new("3.0", true).await;
    let id = h.start().await;
    let alias = h.aliases(0).await.pop().unwrap();
    let title = format!("mcp__{alias}__workspace_api");
    {
        let mut notes = h.node.notes.lock().await;
        while notes.try_recv().is_ok() {}
    }
    h.node
        .call(
            "fixture/note",
            json!({"sessionId":id,"update":tool_update("wake",&title)}),
        )
        .await;
    assert!(
        h.f.manager
            .wake_listener_tick(&h.f.row.id, &h.f.row.workspace_id)
            .await
    );
    tokio::time::timeout(WAIT, async {
        loop {
            if !h.f.manager.is_busy(&h.f.row.id) && !transcript_tools(&h).await.is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(transcript_tools(&h).await[0]["name"], "workspace_api");
    // Drive the same zero-settle path used by a wake tick which loses its busy
    // claim, with a later notification already buffered for the prompt owner.
    h.node
        .call(
            "fixture/note",
            json!({"sessionId":id,"update":tool_update("zero",&title)}),
        )
        .await;
    h.node.call("fixture/note",json!({"sessionId":id,"update":tool_update("left-buffered","mcp__user__workspace_api")})).await;
    let (notes, routes) = {
        let handles = h.f.manager.handles.lock().unwrap();
        let original = &handles[&h.f.row.id];
        (
            original.notifications.clone(),
            original.connection.callback_tool_routes(),
        )
    };
    {
        let mut notes = notes.lock().await;
        let first = notes.try_recv().unwrap();
        h.f.manager
            .services
            .run_harness_wake_turn_with_routes(
                &mut notes,
                first,
                &h.f.row.id,
                &h.f.row.workspace_id,
                Duration::ZERO,
                routes,
            )
            .await;
        assert_eq!(
            notes.try_recv().unwrap().params["update"]["toolCallId"],
            "left-buffered"
        );
    }
    let tools = transcript_tools(&h).await;
    assert_eq!(tools.len(), 2);
    assert!(tools.iter().all(|b| b["name"] == "workspace_api"));
    assert!(current(&callback(&h.origin).unwrap().capture(), h.f.caller()).await);
    h.origin.retire();
    h.node.finish().await;
}

#[tokio::test]
async fn actual_endpoint_keeps_original_scope_from_operation_cleanup_into_optional_preparation() {
    let h = Harness::with_prepare_probe("3.0", true, true).await;
    h.start().await;
    let alias = h.aliases(0).await.pop().unwrap();
    for (operation, expected) in [
        ("operation-cleanup", true),
        ("operation-retire", false),
        ("operation-cleanup", true),
    ] {
        h.probe.phases.lock().unwrap().clear();
        h.probe.retire_operation.store(!expected, Ordering::SeqCst);
        let response = h
            .node
            .call(
                "fixture/call",
                json!({"name":alias,
            "code":"await ws.git.listRoots(); return 'completed-original-operation'"}),
            )
            .await;
        assert!(
            response
                .to_string()
                .contains("completed-original-operation"),
            "{response}"
        );
        let phases = h.probe.phases.lock().unwrap().clone();
        assert!(
            phases
                .iter()
                .any(|(p, live, retained)| p == operation && *live && *retained),
            "{phases:?}"
        );
        assert!(
            phases
                .iter()
                .any(|(p, live, retained)| p == "optional-preparation"
                    && *live == expected
                    && *retained == expected),
            "{phases:?}"
        );
        // A denied request remains denied into preparation even though the
        // physical owner is still live and the next request may capture it.
        assert!(current(&callback(&h.origin).unwrap().capture(), h.f.caller()).await);
    }
    assert_eq!(h.f.writes().await, 1);
    h.origin.retire();
    h.node.finish().await;
}

#[tokio::test]
async fn hard_stop_joins_real_source_dispatch_before_dropping_confirmed_bridge() {
    let h = Harness::new("3.0", true).await;
    h.start().await;
    let captured = callback(&h.origin).unwrap().capture();
    let leaf = intent_core::with_caller(h.f.caller(), async {
        captured.source_lifetime().unwrap().retirement()
    })
    .await;
    // This existing injected permission fixture tests only retirement ordering;
    // it does not claim native credential or read authority from the endpoint.
    let (_stage, fence) = super::super::tests::held_stage_fence(&h.f, leaf).await;
    let (entered, inside) = std::sync::mpsc::channel();
    let (release, hold) = std::sync::mpsc::channel();
    let dispatch = std::thread::spawn(move || {
        fence.dispatch(&mut move || {
            entered.send(()).unwrap();
            hold.recv().unwrap();
            Ok(())
        })
    });
    inside.recv_timeout(WAIT).unwrap();
    let endpoint = h.origin.state.lock().unwrap().endpoint.clone().unwrap();
    let fixture = h.f.clone();
    let runtime = tokio::runtime::Handle::current();
    let stopping =
        std::thread::spawn(move || runtime.block_on(fixture.manager.stop(&fixture.row.id)));
    tokio::time::timeout(WAIT, async {
        loop {
            if h.origin.state.lock().unwrap().retired {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(
        endpoint.bridge.lock().unwrap().is_some(),
        "bridge must outlive the original dispatch drain"
    );
    assert!(
        h.f.manager.handles.try_lock().is_ok(),
        "no handles mutex during retirement drain"
    );
    assert!(!stopping.is_finished());
    release.send(()).unwrap();
    assert_eq!(dispatch.join().unwrap(), Ok(()));
    // The current-thread runtime must keep polling the Store work of stop.
    let stopped = tokio::task::spawn_blocking(move || stopping.join().unwrap())
        .await
        .unwrap();
    assert!(stopped);
    assert!(endpoint.bridge.lock().unwrap().is_none());
    assert!(!current(&captured, h.f.caller()).await);
    h.node.finish().await;
}

#[tokio::test]
async fn blueprint_retains_the_same_typed_services_allocation_after_original_install() {
    let f = Fixture::new().await;
    let original = Arc::new(f.manager.services.clone());
    let weak = Arc::downgrade(&original);
    let origin = RepositoryOrigin::allocate(&original, &f.row).await;
    let directory = original.repository_connection_directory();
    let observer = original.repository_lifecycle_registry.clone();
    // Capture cannot await either shared gate and must retain this very Arc.
    let credential = original.gitlab_credential_gate.lock().await;
    let read_owner = original
        .worktree_locks
        .with_lock(f.dir.path(), || async {
            RepositoryReadOwner::capture(original.clone())
        })
        .await;
    assert!(read_owner.is_ok());
    drop(credential);
    let api: Arc<dyn WorkspaceApi> = original.clone();
    let same_api: Arc<dyn WorkspaceApi> = original.clone();
    assert!(Arc::ptr_eq(&api, &same_api));
    drop(same_api);
    let workspace = f.row.workspace_id.clone();
    let blueprint = ServerBlueprint::new(read_owner, move || {
        WorkspaceMcpServer::new(api.clone(), workspace.clone())
    });
    let clone = blueprint.clone();
    drop(original);
    drop(blueprint);
    let retained = weak.upgrade().unwrap();
    assert!(Arc::ptr_eq(
        &directory,
        &retained.repository_connection_directory()
    ));
    assert!(Arc::ptr_eq(
        &observer,
        &retained.repository_lifecycle_registry
    ));
    assert!(retained
        .store()
        .shares_repository_lifecycle_domain(f.manager.services.store()));
    // Actual ordinary Services output still uses the same erased allocation.
    let response = clone
        .server()
        .handle_message(&json!({
            "jsonrpc":"2.0", "id":1, "method":"tools/call",
            "params":{"name":"workspace_api","arguments":{"code":"return 'original services body'","summary":"original Services allocation control"}}
        }))
        .await
        .unwrap();
    assert!(
        response.to_string().contains("original services body"),
        "{response}"
    );
    let anchor_only = clone.read_owner.clone();
    drop(retained);
    drop(clone);
    assert!(
        weak.upgrade().is_some(),
        "the retained result owns the same Arc"
    );
    drop(anchor_only);
    assert!(weak.upgrade().is_none());
    origin.retire();
}

#[tokio::test]
async fn confirmed_endpoint_retains_one_read_scope_through_preparation_and_retires_escaped_handles()
{
    let h = Harness::with_prepare_probe("3.0", true, true).await;
    let id = h.start().await;
    let alias = h.aliases(0).await.pop().unwrap();
    h.probe.reads.lock().unwrap().clear();
    h.probe.hold.store(true, Ordering::SeqCst);
    let held = h.mcp(0, &alias, "held ordinary result");
    tokio::pin!(held);
    tokio::select! {
        result = &mut held => panic!("ordinary settings barrier bypassed: {result}"),
        () = h.probe.entered.notified() => {},
    }
    let held_observations = std::mem::take(&mut *h.probe.reads.lock().unwrap());
    let held_read = held_observations[0].read.as_ref().unwrap().clone();
    let completed = h
        .node
        .call(
            "fixture/call",
            json!({
                "name":alias,
                "code":"await ws.git.listRoots(); return 'completed same scope'"
            }),
        )
        .await;
    assert!(completed.to_string().contains("completed same scope"));
    let completed_observations = std::mem::take(&mut *h.probe.reads.lock().unwrap());
    assert!(completed_observations
        .iter()
        .any(|p| p.phase == "operation-cleanup"));
    assert!(completed_observations
        .iter()
        .any(|p| p.phase == "optional-preparation"));
    let completed_read = completed_observations[0].read.as_ref().unwrap().clone();
    assert!(!Arc::ptr_eq(&held_read, &completed_read));
    intent_core::with_caller(h.f.caller(), async {
        assert!(held_read.check_current().is_ok(), "sibling remains live");
        for observation in &completed_observations {
            let read = observation.read.as_ref().unwrap();
            assert!(
                Arc::ptr_eq(&completed_read, read),
                "one physical request through preparation"
            );
            assert_eq!(read.check_current(), Err(AdmissionError::Retired));
            let source = observation.source.as_ref().unwrap();
            assert_eq!(
                source.retirement().check_current(),
                Err(AdmissionError::Retired)
            );
            assert!(matches!(
                source.subscribe(
                    h.f.manager.services.store(),
                    &h.f.caller(),
                    &[RepositoryLifecycleKey::Database]
                ),
                Err(AdmissionError::Retired)
            ));
        }
    })
    .await;
    h.probe.release.notify_one();
    assert!(held.await.to_string().contains("held ordinary result"));
    intent_core::with_caller(h.f.caller(), async {
        assert_eq!(held_read.check_current(), Err(AdmissionError::Retired));
    })
    .await;
    h.probe.reads.lock().unwrap().clear();
    assert!(h
        .mcp(0, &alias, "fresh ordinary result")
        .await
        .to_string()
        .contains("fresh ordinary result"));
    let fresh = std::mem::take(&mut *h.probe.reads.lock().unwrap());
    assert!(fresh.iter().all(|p| p.read.is_ok()));
    assert!(!Arc::ptr_eq(
        &completed_read,
        fresh[0].read.as_ref().unwrap()
    ));
    assert!(current(&callback(&h.origin).unwrap().capture(), h.f.caller()).await);
    assert_eq!(
        h.f.stored().await.acp_session_id.as_deref(),
        Some(id.as_str())
    );
    assert_eq!(h.f.writes().await, 1);
    let inspect = h.node.call("fixture/inspect", json!({})).await;
    assert_eq!(count_requests(&inspect, "session/new"), 1);
    assert_eq!(count_requests(&inspect, METHOD), 1);
    h.origin.retire();
}

#[tokio::test]
async fn failed_original_anchor_never_upgrades_while_confirmed_and_pending_results_survive() {
    let h = Harness::with_anchor("3.0", true, true, true).await;
    assert!(matches!(
        h.origin
            .state
            .lock()
            .unwrap()
            .blueprint
            .as_ref()
            .unwrap()
            .server
            .read_owner,
        Err(AdmissionError::Unavailable)
    ));
    // The real original installation is now available, but the saved failure
    // is immutable. This control must never become a retry in the blueprint.
    assert!(RepositoryReadOwner::capture(Arc::new(h.f.manager.services.clone())).is_ok());
    let id = h.start().await;
    let alias = h.aliases(0).await.pop().unwrap();
    for (name, source) in [
        (&alias[..], true),
        ("workspace-mcp", false),
        ("user-kept", false),
    ] {
        h.probe.reads.lock().unwrap().clear();
        h.probe.observations.lock().unwrap().clear();
        let result = h.mcp(0, name, "completed without read anchor").await;
        assert!(result.to_string().contains("completed without read anchor"));
        let observations = h.probe.reads.lock().unwrap();
        assert!(!observations.is_empty());
        assert!(observations
            .iter()
            .all(|p| matches!(p.read, Err(AdmissionError::Unavailable))));
        assert!(h
            .probe
            .observations
            .lock()
            .unwrap()
            .iter()
            .all(|p| *p == (source, source)));
    }
    assert_eq!(
        h.f.stored().await.acp_session_id.as_deref(),
        Some(id.as_str())
    );
    assert_eq!(h.f.writes().await, 1);
    let inspect = h.node.call("fixture/inspect", json!({})).await;
    assert_eq!(count_requests(&inspect, "session/new"), 1);
    assert_eq!(count_requests(&inspect, METHOD), 1);
    h.origin.retire();
    h.node.finish().await;
}

// Combined fixture: the manager creates the only physical owner, and the exact
// typed Services allocation supplies both the read anchor and WorkspaceApi.
// Only the native SDK child and local HTTP response producer are scripted.
use intent_acp::mcp_server::private_results::{
    McpHostCall, McpPrivateAdmission, McpPrivateBoundary, McpPrivateBoundaryKind as Boundary,
    McpPrivateHostScope, McpPrivatePolicy, McpReadEvidence, PreparedMcpTransfer,
};
use intent_acp::mcp_server::request_context::McpContextFuture;

struct NativeHold {
    entered: Notify,
    release: tokio::sync::Semaphore,
}

impl NativeHold {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            entered: Notify::new(),
            release: tokio::sync::Semaphore::new(0),
        })
    }

    async fn wait(&self) {
        self.entered.notify_one();
        self.release.acquire().await.unwrap().forget();
    }

    async fn reached(&self) {
        tokio::time::timeout(WAIT, self.entered.notified())
            .await
            .unwrap();
    }
}

#[derive(Default)]
struct NativeControl {
    hold: Mutex<Option<(Boundary, bool, Arc<NativeHold>)>>,
    events: Mutex<Vec<(Boundary, usize)>>,
    reads: Mutex<Vec<ReadObservation>>,
    records: Mutex<Vec<Arc<crate::repository_read_source::ReadRecord>>>,
    foreign: Mutex<Vec<Arc<crate::repository_read_source::ReadRecord>>>,
    captures: AtomicUsize,
    optional_captures: AtomicUsize,
    optional_events: Mutex<Vec<(Boundary, usize)>>,
    entered: AtomicUsize,
    changed: Notify,
    blocked: AtomicBool,
    release: Notify,
    completed_body: Mutex<Option<Arc<NativeHold>>>,
}

impl NativeControl {
    fn at(&self, boundary: Boundary, after: bool) -> Arc<NativeHold> {
        let gate = NativeHold::new();
        assert!(self
            .hold
            .lock()
            .unwrap()
            .replace((boundary, after, gate.clone()))
            .is_none());
        gate
    }

    fn wrap(self: &Arc<Self>, original: Arc<dyn McpRequestContext>) -> Arc<dyn McpRequestContext> {
        Arc::new(NativeContext {
            original,
            control: self.clone(),
        })
    }
}

struct NativeContext {
    original: Arc<dyn McpRequestContext>,
    control: Arc<NativeControl>,
}

struct NativeScope {
    original: Arc<dyn McpRequestScope>,
    control: Arc<NativeControl>,
}

struct NativePolicy {
    original: Arc<dyn McpPrivatePolicy>,
    control: Arc<NativeControl>,
}

impl McpRequestContext for NativeContext {
    fn capture(&self) -> Arc<dyn McpRequestScope> {
        let original = self.original.capture();
        self.control.captures.fetch_add(1, Ordering::SeqCst);
        self.control.changed.notify_waiters();
        Arc::new(NativeScope {
            original,
            control: self.control.clone(),
        })
    }
}

impl McpRequestScope for NativeScope {
    fn scope<'a>(&'a self, body: McpContextFuture<'a>) -> McpContextFuture<'a> {
        self.original.scope(Box::pin(async move {
            self.control.reads.lock().unwrap().push(ReadObservation {
                phase: "actual original scope".into(),
                read: current_read_request(),
                source: current_source_lifetime().ok(),
            });
            let release = self.control.release.notified();
            tokio::pin!(release);
            release.as_mut().enable();
            self.control.entered.fetch_add(1, Ordering::SeqCst);
            self.control.changed.notify_waiters();
            if self.control.blocked.load(Ordering::SeqCst) {
                release.await;
            }
            body.await;
            let held = self.control.completed_body.lock().unwrap().take();
            if let Some(held) = held {
                held.wait().await;
            }
        }))
    }

    fn private_result_policy(&self) -> Option<Arc<dyn McpPrivatePolicy>> {
        self.original.private_result_policy().map(|original| {
            Arc::new(NativePolicy {
                original,
                control: self.control.clone(),
            }) as Arc<dyn McpPrivatePolicy>
        })
    }
}

impl McpPrivatePolicy for NativePolicy {
    fn capture_optional_context(
        &self,
    ) -> Option<Box<dyn intent_acp::mcp_server::private_results::McpOptionalContextScope>> {
        let scope = self.original.capture_optional_context();
        if scope.is_some() {
            self.control
                .optional_captures
                .fetch_add(1, Ordering::SeqCst);
        }
        scope
    }

    fn admit_optional<'a>(
        &'a self,
        boundary: &'a McpPrivateBoundary,
        sealed: intent_acp::mcp_server::private_results::McpSealedReads<'a>,
        evidence: &'a intent_acp::mcp_server::private_results::McpOptionalEvidence,
        packet: intent_acp::mcp_server::private_results::PreparedMcpVariants<'a>,
    ) -> BoxFuture<'a, McpPrivateAdmission> {
        Box::pin(async move {
            self.control
                .optional_events
                .lock()
                .unwrap()
                .push((boundary.kind(), sealed.records().len()));
            self.control
                .events
                .lock()
                .unwrap()
                .push((boundary.kind(), sealed.records().len()));
            let retained =
                crate::repository_read_source::tests::retain_and_check_records(sealed.records())
                    .await;
            *self.control.records.lock().unwrap() = retained;
            let hold = {
                let mut held = self.control.hold.lock().unwrap();
                if held
                    .as_ref()
                    .is_some_and(|(kind, _, _)| *kind == boundary.kind())
                {
                    held.take()
                } else {
                    None
                }
            };
            if let Some((_, false, gate)) = &hold {
                gate.wait().await;
            }
            // Delegate the same successful seal, optional evidence, boundary,
            // one-use packet and resulting receipt. No fixture authority.
            let actual = self
                .original
                .admit_optional(boundary, sealed, evidence, packet)
                .await;
            if let Some((_, true, gate)) = &hold {
                gate.wait().await;
            }
            actual
        })
    }

    fn capture_host(&self, call: McpHostCall) -> Box<dyn McpPrivateHostScope> {
        self.original.capture_host(call)
    }

    fn admit<'a>(
        &'a self,
        boundary: &'a McpPrivateBoundary,
        records: &'a [McpReadEvidence],
        packet: PreparedMcpTransfer<'a>,
    ) -> BoxFuture<'a, McpPrivateAdmission> {
        Box::pin(async move {
            self.control
                .events
                .lock()
                .unwrap()
                .push((boundary.kind(), records.len()));
            let retained =
                crate::repository_read_source::tests::retain_and_check_records(records).await;
            let foreign = self.control.foreign.lock().unwrap().clone();
            if !foreign.is_empty() {
                crate::repository_read_source::tests::refuse_foreign_record_set(
                    &retained, &foreign,
                )
                .await;
            }
            *self.control.records.lock().unwrap() = retained;
            let hold = {
                let mut held = self.control.hold.lock().unwrap();
                if held
                    .as_ref()
                    .is_some_and(|(kind, _, _)| *kind == boundary.kind())
                {
                    held.take()
                } else {
                    None
                }
            };
            if let Some((_, false, gate)) = &hold {
                gate.wait().await;
            }
            let actual = self.original.admit(boundary, records, packet).await;
            if let Some((_, true, gate)) = &hold {
                gate.wait().await;
            }
            actual
        })
    }
}

struct NativeHarness {
    f: Arc<Fixture>,
    original: Arc<crate::Services>,
    auth: crate::source_control_auth_ops::repository_owner::secret_reader::tests::Fixture,
    git: crate::repository_admission_source_tests::fixtures::Fixture,
    http: crate::repository_read_source::tests::ReadServer,
    node: NodePeer,
    origin: Arc<RepositoryOrigin>,
    user: McpBridge,
}

impl NativeHarness {
    async fn new(stamp: &str, opt_in: bool, failed_anchor: bool) -> Self {
        Self::observed(stamp, opt_in, failed_anchor, None, None).await
    }

    async fn observed(
        stamp: &str,
        opt_in: bool,
        failed_anchor: bool,
        control: Option<Arc<NativeControl>>,
        pending_control: Option<Arc<NativeControl>>,
    ) -> Self {
        use crate::agent_manager::{AgentManager, BusEventSink};
        use crate::events::EventBus;

        let http = crate::repository_read_source::tests::ReadServer::new().await;
        let mut auth =
            crate::source_control_auth_ops::repository_owner::secret_reader::tests::Fixture::new(
                &http.fixture,
            )
            .await;
        let mut git = crate::repository_admission_source_tests::fixtures::Fixture::new().await;
        let node = NodePeer::new();
        // The actual ACP cwd and original Store root share this real repository.
        // Keep it inside the scripted peer's existing filesystem allowance.
        let path = node.scratch.path().join("repo");
        std::fs::rename(&git.path, &path).unwrap();
        git.path = path;
        git.workspace.repository_path = Some(git.path.to_str().unwrap().into());
        let bus = EventBus::new(auth.service.store.clone());
        let original = Arc::new(
            auth.service
                .as_ref()
                .clone()
                .with_workspaces_root(git.dir.path().join("owned-workspaces"))
                .with_event_bus(bus.clone()),
        );
        auth.service = original.clone();
        original
            .store
            .insert_workspace(&git.workspace)
            .await
            .unwrap();
        git.store = original.store.clone();
        git.git(
            &git.path,
            &[
                "remote",
                "add",
                "origin",
                &format!(
                    "{}/group/project.git",
                    http.fixture.descriptor.instance().as_str()
                ),
            ],
        );
        let mut row = json!({
            "id":intent_core::AgentId::new(),"workspaceId":git.workspace.id,
            "name":"original manager read","provider":"claude-code","status":"idle",
            "harnessVersion":stamp,"createdAt":"2026-09-28T00:00:00Z",
            "updatedAt":"2026-09-28T00:00:00Z"
        });
        if stamp == "<missing>" {
            row.as_object_mut().unwrap().remove("harnessVersion");
        }
        let row: intent_core::AgentSession = serde_json::from_value(row).unwrap();
        original.store.insert_agent_session(&row).await.unwrap();
        let manager = Arc::new(AgentManager::new(
            original.as_ref().clone(),
            Arc::new(BusEventSink::new(bus)),
            4,
        ));
        original.attach_agent_manager(&manager);
        assert!(Arc::ptr_eq(&original.agent_manager().unwrap(), &manager));
        let f = Arc::new(Fixture {
            dir: crate::test_support::test_tempdir("native-callback-accounting"),
            manager,
            row,
        });
        f.count_writes().await;
        let failed = failed_anchor.then(|| RepositoryReadOwner::capture(original.clone()));
        let origin = RepositoryOrigin::allocate(&original, &f.row).await;
        let read_owner = failed.unwrap_or_else(|| RepositoryReadOwner::capture(original.clone()));
        assert_eq!(read_owner.is_err(), failed_anchor);
        let workspace = f.row.workspace_id.clone();
        let agent = f.row.id.clone();
        let api: Arc<dyn WorkspaceApi> = original.clone();
        let attachments = original.turn_attachments();
        let mut blueprint = ServerBlueprint::new(read_owner, move || {
            WorkspaceMcpServer::for_agent_type(api.clone(), workspace.clone(), "default")
                .with_caller_agent_id(Some(agent.clone()))
                .with_turn_attachments(Some(attachments.clone()))
        })
        .with_original_services(original.clone(), f.row.clone());
        if let Some(control) = control {
            blueprint.context_decorator = Some(Arc::new(move |context| control.wrap(context)));
        }
        let mut pending_context: Arc<dyn McpRequestContext> =
            Arc::new(origin.pending_callback().unwrap());
        if let Some(control) = pending_control {
            pending_context = control.wrap(pending_context);
        }
        let pending = blueprint.server().with_request_context(pending_context);
        let bridge = serve_workspace_mcp_tcp(Arc::new(pending)).await.unwrap();
        let user = serve_workspace_mcp_tcp(Arc::new(blueprint.server()))
            .await
            .unwrap();
        let (mut child, _, _) = handle(origin.clone());
        child.connection = node.connection.clone();
        child.notifications = node.notes.clone();
        child.session_mcp_servers = serde_json::from_value(json!([
            {"name":"workspace-mcp","command":"fixture-mcp","args":[bridge.connect_addr()],"env":[]},
            {"name":"user-kept","command":"fixture-mcp","args":[user.connect_addr()],"env":[]}
        ]))
        .unwrap();
        #[expect(
            clippy::used_underscore_binding,
            reason = "The actual handle owns its original pending bridge"
        )]
        {
            child._mcp_bridge = Some(bridge);
        }
        origin.configure_callbacks(EndpointBlueprint {
            server: blueprint,
            command: "fixture-mcp".into(),
            args_before_address: vec![],
            env: BTreeMap::new(),
        });
        if opt_in {
            origin.state.lock().unwrap().callback_offer = CallbackOffer::V1;
        }
        f.manager
            .handles
            .lock()
            .unwrap()
            .insert(f.row.id.clone(), child);
        Self {
            f,
            original,
            auth,
            git,
            http,
            node,
            origin,
            user,
        }
    }

    async fn start(&self) -> String {
        self.f
            .manager
            .start_session(
                &self.f.row.id,
                self.git.path.clone(),
                intent_providers::provider_config("claude-code"),
            )
            .await
            .unwrap()
    }

    fn start_task(&self) -> tokio::task::JoinHandle<intent_core::Result<String>> {
        let f = self.f.clone();
        let cwd = self.git.path.clone();
        tokio::spawn(async move {
            f.manager
                .start_session(
                    &f.row.id,
                    cwd,
                    intent_providers::provider_config("claude-code"),
                )
                .await
        })
    }

    fn call_task(&self, query: usize, name: &str, code: &str) -> tokio::task::JoinHandle<Value> {
        let connection = self.node.connection.clone();
        let params = json!({"query":query,"name":name,"code":code});
        tokio::spawn(async move {
            connection
                .request_timeout("fixture/call", params, WAIT)
                .await
                .unwrap()
        })
    }

    fn recreate(&self) {
        self.f
            .manager
            .force_recreate
            .lock()
            .unwrap()
            .insert(self.f.row.id.clone());
    }

    async fn replacement(&self) -> (NodePeer, Arc<RepositoryOrigin>) {
        let row = self.f.stored().await;
        let blueprint = self.origin.state.lock().unwrap().blueprint.clone().unwrap();
        let old = super::super::take(
            &self.f.manager.handles,
            &self.f.row.id,
            &self.origin,
            Some(&self.f.manager.registry),
        )
        .unwrap();
        let mut servers = serde_json::to_value(&old.session_mcp_servers).unwrap();
        drop(old);
        let origin = RepositoryOrigin::allocate(&self.original, &row).await;
        let node = NodePeer::with_original_repository(Some(&self.git.path));
        let bridge = serve_workspace_mcp_tcp(Arc::new(
            blueprint
                .server
                .server()
                .with_request_context(Arc::new(origin.pending_callback().unwrap())),
        ))
        .await
        .unwrap();
        servers[0]["args"] = json!([bridge.connect_addr()]);
        let (mut child, _, _) = handle(origin.clone());
        child.connection = node.connection.clone();
        child.notifications = node.notes.clone();
        child.session_mcp_servers = serde_json::from_value(servers).unwrap();
        #[expect(
            clippy::used_underscore_binding,
            reason = "Install the replacement handle-owned bridge"
        )]
        {
            child._mcp_bridge = Some(bridge);
        }
        origin.configure_callbacks(EndpointBlueprint {
            server: blueprint.server.clone(),
            command: blueprint.command.clone(),
            args_before_address: blueprint.args_before_address.clone(),
            env: blueprint.env.clone(),
        });
        origin.state.lock().unwrap().callback_offer = CallbackOffer::V1;
        self.f
            .manager
            .handles
            .lock()
            .unwrap()
            .insert(self.f.row.id.clone(), child);
        (node, origin)
    }

    async fn names(&self, query: usize) -> Vec<String> {
        self.node
            .call("fixture/names", json!({"query":query}))
            .await
            .as_array()
            .unwrap()
            .iter()
            .map(|name| name.as_str().unwrap().to_owned())
            .collect()
    }

    async fn confirmed(&self, query: usize) -> String {
        let names = self.names(query).await;
        let confirmed = names
            .into_iter()
            .filter(|name| name.starts_with("intent-callback-"))
            .collect::<Vec<_>>();
        assert_eq!(confirmed.len(), 1, "one immutable registration per Query");
        confirmed.into_iter().next().unwrap()
    }

    async fn call(&self, query: usize, name: &str, code: &str) -> Value {
        self.node
            .call(
                "fixture/call",
                json!({"query":query,"name":name,"code":code}),
            )
            .await
    }

    async fn finish(self) {
        self.origin.retire();
        self.node.finish().await;
    }
}

#[intent_test_macros::daemon_test]
async fn confirmed_read_public_session_uses_original_services_cache_and_registered_endpoint() {
    let h = NativeHarness::new("3.0", true, false).await;
    let before = std::fs::read(h.original.gitlab_secret_store.path()).unwrap();
    let pending = h.origin.pending_callback().unwrap().capture();
    let session = h.start().await;
    assert_eq!(
        h.f.stored().await.acp_session_id.as_deref(),
        Some(session.as_str())
    );
    assert_eq!(h.f.writes().await, 1);
    assert!(Arc::ptr_eq(&h.original, &h.auth.service));
    assert!(h
        .original
        .store
        .shares_repository_lifecycle_domain(&h.git.store));
    assert_ne!(h.user.connect_addr(), String::new());
    let name = h.confirmed(0).await;
    let reply = h
        .call(
            0,
            &name,
            "const a=await ws.pr.snapshot(4); const b=await ws.pr.snapshot(4); return [a,b];",
        )
        .await;
    assert!(reply.to_string().contains("actual review"), "{reply}");
    assert!(!reply.to_string().contains("stored-pat"));
    assert!(h.http.count() > 0);
    assert_eq!(
        before,
        std::fs::read(h.original.gitlab_secret_store.path()).unwrap()
    );
    assert!(!current(&pending, h.f.caller()).await);
    for endpoint in ["workspace-mcp", "user-kept"] {
        let before = h.http.count();
        let denied = h
            .call(
                0,
                endpoint,
                "try { return await ws.pr.snapshot(4); } catch(e) { return e.message; }",
            )
            .await;
        assert!(
            denied
                .to_string()
                .contains(crate::repository_read_source::REFUSAL),
            "{denied}"
        );
        assert_eq!(h.http.count(), before);
        assert!(h
            .call(0, endpoint, "return 'ordinary original result';")
            .await
            .to_string()
            .contains("ordinary original result"));
    }
    let inspect = h.node.call("fixture/inspect", json!({})).await;
    assert_eq!(count_requests(&inspect, "session/new"), 1);
    assert_eq!(count_requests(&inspect, METHOD), 1);
    assert_eq!(inspect["initializations"], json!([1]));
    assert_eq!(h.f.writes().await, 1);
    h.finish().await;
}

#[intent_test_macros::daemon_test]
async fn confirmed_read_load_recreate_preserves_one_producer_and_original_accounting() {
    let h = NativeHarness::new("3.0", true, false).await;
    let first = h.start().await;
    assert!(h
        .call(0, &h.confirmed(0).await, "return await ws.pr.snapshot(4);")
        .await
        .to_string()
        .contains("actual review"));
    let old = callback(&h.origin).unwrap().capture();
    assert_eq!(h.start().await, first);
    assert!(!current(&old, h.f.caller()).await);
    assert_eq!(h.f.writes().await, 1);
    assert!(h
        .call(1, &h.confirmed(1).await, "return await ws.pr.snapshot(4);")
        .await
        .to_string()
        .contains("actual review"));
    h.f.usage(23).await;
    h.recreate();
    let new = h.start().await;
    assert_ne!(new, first);
    assert_eq!(h.f.writes().await, 2);
    let (usage, baseline) = h.f.accounting().await;
    assert!(usage.is_none());
    assert_eq!(baseline.unwrap()["inputTokens"], 23);
    assert!(h
        .call(2, &h.confirmed(2).await, "return await ws.pr.snapshot(4);")
        .await
        .to_string()
        .contains("actual review"));
    let inspect = h.node.call("fixture/inspect", json!({})).await;
    assert_eq!(count_requests(&inspect, "session/new"), 2);
    assert_eq!(count_requests(&inspect, "session/load"), 1);
    assert_eq!(count_requests(&inspect, METHOD), 3);
    assert_eq!(inspect["initializations"], json!([1, 1, 1]));
    h.finish().await;
}

#[intent_test_macros::daemon_test]
async fn confirmed_read_disabled_older_and_unavailable_receipts_keep_ordinary_results() {
    for (stamp, enabled, capability) in [
        ("3.0", false, "normal"),
        ("2.9", true, "normal"),
        ("3.0", true, "unsupported"),
        ("3.0", true, "malformed"),
        ("3.0", true, "missing-receipt"),
    ] {
        let h = NativeHarness::new(stamp, enabled, false).await;
        if capability != "normal" {
            h.node
                .call("fixture/capability", json!({"mode":capability}))
                .await;
        }
        h.start().await;
        let inspect = h.node.call("fixture/inspect", json!({})).await;
        assert_eq!(
            count_requests(&inspect, METHOD),
            0,
            "{stamp}/{capability}: {inspect}"
        );
        assert_eq!(h.f.writes().await, 1);
        assert!(h
            .names(0)
            .await
            .iter()
            .all(|n| !n.starts_with("intent-callback-")));
        let ordinary = h
            .call(0, "workspace-mcp", "return 'original ordinary result';")
            .await;
        assert!(ordinary.to_string().contains("original ordinary result"));
        let denied = h
            .call(
                0,
                "workspace-mcp",
                "try {return await ws.pr.snapshot(4);} catch(e) {return e.message;}",
            )
            .await;
        assert!(
            denied
                .to_string()
                .contains(crate::repository_read_source::REFUSAL),
            "{denied}"
        );
        assert_eq!(h.http.count(), 0);
        h.finish().await;
    }
    let h = NativeHarness::new("3.0", true, true).await;
    h.start().await;
    let name = h.confirmed(0).await;
    assert!(h
        .call(
            0,
            &name,
            "try {return await ws.pr.snapshot(4);} catch(e) {return e.message;}"
        )
        .await
        .to_string()
        .contains(crate::repository_read_source::REFUSAL));
    assert!(RepositoryReadOwner::capture(h.original.clone()).is_ok());
    assert!(h
        .call(
            0,
            &name,
            "try {return await ws.pr.snapshot(4);} catch(e) {return e.message;}"
        )
        .await
        .to_string()
        .contains(crate::repository_read_source::REFUSAL));
    assert_eq!(h.http.count(), 0, "original failed capture never retries");
    assert_eq!(h.f.writes().await, 1);
    h.finish().await;
}

#[intent_test_macros::daemon_test]
async fn confirmed_read_seventeenth_pending_capture_cannot_upgrade_after_registration() {
    let pending = Arc::new(NativeControl::default());
    let h = NativeHarness::observed("3.0", true, false, None, Some(pending.clone())).await;
    h.node.call("fixture/next", json!({"hold":true})).await;
    let start = h.start_task();
    h.node.call("fixture/entered", json!({})).await;
    pending.blocked.store(true, Ordering::SeqCst);
    let before_capture = pending.captures.load(Ordering::SeqCst);
    let before_entered = pending.entered.load(Ordering::SeqCst);
    let mut tasks = Vec::new();
    for i in 0..17 {
        tasks.push(h.call_task(
            0,
            "workspace-mcp",
            &format!(
                "try {{await ws.pr.snapshot(4);}} catch(e) {{return 'queued-{i}: '+e.message;}}"
            ),
        ));
    }
    wait_count(&pending.entered, &pending.changed, before_entered + 16).await;
    wait_count(&pending.captures, &pending.changed, before_capture + 17).await;
    assert_eq!(pending.entered.load(Ordering::SeqCst), before_entered + 16);
    h.node.call("fixture/release", json!({})).await;
    start.await.unwrap().unwrap();
    let fresh = h
        .call(0, &h.confirmed(0).await, "return await ws.pr.snapshot(4);")
        .await;
    assert!(fresh.to_string().contains("actual review"), "{fresh}");
    let reads = h.http.count();
    pending.blocked.store(false, Ordering::SeqCst);
    pending.release.notify_waiters();
    for task in tasks {
        let denied = task.await.unwrap();
        assert!(denied.to_string().contains("queued-"), "{denied}");
        assert!(
            denied
                .to_string()
                .contains(crate::repository_read_source::REFUSAL),
            "{denied}"
        );
    }
    assert_eq!(h.http.count(), reads);
    assert!(pending
        .reads
        .lock()
        .unwrap()
        .iter()
        .all(|r| r.read.is_err()));
    assert_eq!(h.f.writes().await, 1);
    h.finish().await;
}

#[intent_test_macros::daemon_test]
async fn confirmed_read_escaped_handles_retire_only_after_their_original_scope() {
    let control = Arc::new(NativeControl::default());
    let h = NativeHarness::observed("3.0", true, false, Some(control.clone()), None).await;
    h.start().await;
    let name = h.confirmed(0).await;
    // Initialization/list scopes have already completed. Observe the original
    // tool invocation below, whose response is held at the real TCP boundary.
    control.reads.lock().unwrap().clear();
    let gate = control.at(Boundary::TcpResponse, false);
    let first = h.call_task(0, &name, "return await ws.pr.snapshot(4);");
    gate.reached().await;
    let (read, source) = {
        let mut observed = control.reads.lock().unwrap();
        let r = observed.iter_mut().find(|r| r.read.is_ok()).unwrap();
        (r.read.as_ref().unwrap().clone(), r.source.take().unwrap())
    };
    let cloned = read.clone();
    intent_core::with_caller(h.f.caller(), async {
        assert!(read.retains(h.original.as_ref()));
        assert!(cloned.check_current().is_ok());
        assert!(source.retirement().check_current().is_ok());
    })
    .await;
    let sibling = h.call(0, &name, "return await ws.pr.snapshot(4);").await;
    assert!(sibling.to_string().contains("actual review"));
    assert!(
        intent_core::with_caller(h.f.caller(), async { read.check_current() })
            .await
            .is_ok()
    );
    drop(cloned);
    gate.release.add_permits(1);
    assert!(first.await.unwrap().to_string().contains("actual review"));
    tokio::time::timeout(WAIT, async {
        while source.retirement().check_current().is_ok() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(
        intent_core::with_caller(h.f.caller(), async { read.check_current() })
            .await
            .is_err()
    );
    assert!(current(&callback(&h.origin).unwrap().capture(), h.f.caller()).await);
    assert!(h
        .call(0, &name, "return await ws.pr.snapshot(4);")
        .await
        .to_string()
        .contains("actual review"));
    h.finish().await;
}

#[intent_test_macros::daemon_test]
async fn confirmed_read_host_and_tcp_boundary_keep_original_transfer_and_soft_interrupt() {
    for (boundary, after) in [
        (Boundary::HostPromise, false),
        (Boundary::HostPromise, true),
        (Boundary::TcpResponse, false),
        (Boundary::TcpResponse, true),
    ] {
        let control = Arc::new(NativeControl::default());
        let h = NativeHarness::observed("3.0", true, false, Some(control.clone()), None).await;
        h.start().await;
        let name = h.confirmed(0).await;
        let endpoint = h.origin.state.lock().unwrap().endpoint.clone().unwrap();
        let gate = control.at(boundary, after);
        let task = h.call_task(0, &name, "return await ws.pr.snapshot(4);");
        gate.reached().await;
        assert!(h.f.manager.interrupt(&h.f.row.id).await);
        assert!(Arc::ptr_eq(
            &endpoint,
            h.origin.state.lock().unwrap().endpoint.as_ref().unwrap()
        ));
        gate.release.add_permits(1);
        let reply = task.await.unwrap();
        // A TCP transfer already consumed the original packet before retirement.
        let committed = boundary == Boundary::TcpResponse && after;
        assert_eq!(
            reply.to_string().contains("actual review"),
            committed,
            "{boundary:?}/{after}: {reply}"
        );
        if !committed {
            assert!(
                reply
                    .to_string()
                    .contains("Private result delivery refused"),
                "{reply}"
            );
        }
        assert!(h
            .call(0, &name, "return await ws.pr.snapshot(4);")
            .await
            .to_string()
            .contains("actual review"));
        assert_eq!(h.f.writes().await, 1);
        h.finish().await;
    }
}

#[intent_test_macros::daemon_test]
async fn confirmed_read_caught_discarded_shared_root_and_budget_bind_every_original_read() {
    for code in [
        "await Promise.all([ws.pr.snapshot(4),ws.pr.snapshot(4)]); return 'constant';",
        "await ws.pr.snapshot(4); try {await ws.pr.snapshot(99);} catch(e) {} return 'caught';",
    ] {
        let control = Arc::new(NativeControl::default());
        let h = NativeHarness::observed("3.0", true, false, Some(control.clone()), None).await;
        h.start().await;
        let gate = control.at(Boundary::TcpResponse, false);
        let task = h.call_task(0, &h.confirmed(0).await, code);
        gate.reached().await;
        assert!(control
            .events
            .lock()
            .unwrap()
            .iter()
            .any(|(b, n)| *b == Boundary::TcpResponse && *n >= 3));
        assert!(h.f.manager.interrupt(&h.f.row.id).await);
        gate.release.add_permits(1);
        assert!(task
            .await
            .unwrap()
            .to_string()
            .contains("Private result delivery refused"));
        h.finish().await;
    }
    for count in [63, 64] {
        let control = Arc::new(NativeControl::default());
        let h = NativeHarness::observed("3.0", true, false, Some(control.clone()), None).await;
        h.start().await;
        let code = format!("for(let i=0;i<{count};i++) {{await ws.pr.snapshot(4);}} return 'bounded original success';");
        let reply = h.call(0, &h.confirmed(0).await, &code).await;
        assert_eq!(
            reply.to_string().contains("bounded original success"),
            count == 63,
            "{reply}"
        );
        if count == 63 {
            assert!(control
                .events
                .lock()
                .unwrap()
                .contains(&(Boundary::TcpResponse, 64)));
        } else {
            assert!(
                reply
                    .to_string()
                    .contains("Private result delivery refused"),
                "{reply}"
            );
        }
        h.finish().await;
    }
}

#[intent_test_macros::daemon_test]
async fn confirmed_read_mixed_live_requests_never_borrow_sibling_evidence() {
    let control = Arc::new(NativeControl::default());
    let h = NativeHarness::observed("3.0", true, false, Some(control.clone()), None).await;
    h.start().await;
    let name = h.confirmed(0).await;
    let gate = control.at(Boundary::TcpResponse, false);
    let first = h.call_task(0, &name, "return await ws.pr.snapshot(4);");
    gate.reached().await;
    *control.foreign.lock().unwrap() = control.records.lock().unwrap().clone();
    let second = h.call(0, &name, "return await ws.pr.snapshot(4);").await;
    assert!(second.to_string().contains("actual review"), "{second}");
    control.foreign.lock().unwrap().clear();
    gate.release.add_permits(1);
    assert!(first.await.unwrap().to_string().contains("actual review"));
    h.finish().await;
}

#[intent_test_macros::daemon_test]
async fn confirmed_read_original_source_and_credential_changes_fence_cached_final_output() {
    for change in ["head", "remote", "secret", "settings"] {
        let control = Arc::new(NativeControl::default());
        let h = NativeHarness::observed("3.0", true, false, Some(control.clone()), None).await;
        h.start().await;
        let gate = control.at(Boundary::TcpResponse, false);
        let task = h.call_task(
            0,
            &h.confirmed(0).await,
            "await ws.pr.snapshot(4); await ws.pr.snapshot(4); return 'constant';",
        );
        gate.reached().await;
        match change {
            "head" => {
                h.git.git(
                    &h.git.path,
                    &[
                        "-c",
                        "user.name=Fixture",
                        "-c",
                        "user.email=fixture@example.invalid",
                        "commit",
                        "--allow-empty",
                        "-m",
                        "replacement head",
                    ],
                );
            }
            "remote" => {
                h.git.git(
                    &h.git.path,
                    &[
                        "remote",
                        "set-url",
                        "origin",
                        "https://unknown.invalid/changed/repository.git",
                    ],
                );
            }
            _ => {
                let path = if change == "secret" {
                    intent_sourcecontrol::gitlab_token::SECRET_ACCOUNT
                } else {
                    "sourceControl.gitlab.oauthClientId"
                };
                intent_core::with_caller(
                    Caller::Daemon,
                    h.original
                        .settings_update(json!([{"path":path,"value":"new-original-setting"}])),
                )
                .await
                .unwrap();
            }
        }
        gate.release.add_permits(1);
        assert!(
            task.await
                .unwrap()
                .to_string()
                .contains("Private result delivery refused"),
            "{change}"
        );
        assert_eq!(h.f.writes().await, 1);
        h.finish().await;
    }
}

#[intent_test_macros::daemon_test]
async fn confirmed_read_provider_completion_reobserves_git_before_cache_application() {
    let h = NativeHarness::new("3.0", true, false).await;
    h.start().await;
    let name = h.confirmed(0).await;
    h.http
        .pause("/api/v4/projects/group%2Fproject/merge_requests/4/discussions");
    let task = h.call_task(0, &name, "return await ws.pr.snapshot(4);");
    h.http.entered().await;
    h.git.git(
        &h.git.path,
        &[
            "-c",
            "user.name=Fixture",
            "-c",
            "user.email=fixture@example.invalid",
            "commit",
            "--allow-empty",
            "-m",
            "changed during actual HTTP",
        ],
    );
    h.http.resume();
    let old = task.await.unwrap();
    assert!(!old.to_string().contains("actual review"), "{old}");
    let before = h.http.count();
    let fresh = h.call(0, &name, "return await ws.pr.snapshot(4);").await;
    assert!(fresh.to_string().contains("actual review"), "{fresh}");
    assert!(h.http.count() > before, "old response was not cached");
    h.finish().await;
}

#[intent_test_macros::daemon_test]
async fn confirmed_read_original_deletion_schedule_cancel_preserves_fresh_endpoint() {
    for agent in [false, true] {
        let control = Arc::new(NativeControl::default());
        let h = NativeHarness::observed("3.0", true, false, Some(control.clone()), None).await;
        h.start().await;
        let name = h.confirmed(0).await;
        let gate = control.at(Boundary::TcpResponse, false);
        let task = h.call_task(0, &name, "await ws.pr.snapshot(4); return 'constant';");
        gate.reached().await;
        intent_core::with_caller(Caller::Daemon, async {
            if agent {
                h.original
                    .agent_schedule_delete(
                        h.f.row.id.clone(),
                        Some(h.git.workspace.id.clone()),
                        60_000,
                    )
                    .await
                    .unwrap();
                assert!(h
                    .original
                    .agent_cancel_delete(h.f.row.id.clone(), Some(h.git.workspace.id.clone()))
                    .await
                    .unwrap());
            } else {
                h.original
                    .schedule_workspace_delete(h.git.workspace.id.clone(), 60_000)
                    .await
                    .unwrap();
                assert!(h
                    .original
                    .cancel_workspace_delete(h.git.workspace.id.clone())
                    .await
                    .unwrap());
            }
        })
        .await;
        gate.release.add_permits(1);
        assert!(task
            .await
            .unwrap()
            .to_string()
            .contains("Private result delivery refused"));
        assert!(h
            .call(0, &name, "return await ws.pr.snapshot(4);")
            .await
            .to_string()
            .contains("actual review"));
        assert_eq!(h.f.writes().await, 1);
        h.finish().await;
    }
}

#[intent_test_macros::daemon_test]
async fn confirmed_read_claimed_delete_retires_original_manager_before_late_output() {
    use crate::delete_grace::PendingDeleteSubject;
    let control = Arc::new(NativeControl::default());
    let h = NativeHarness::observed("3.0", true, false, Some(control.clone()), None).await;
    h.start().await;
    let gate = control.at(Boundary::TcpResponse, false);
    let task = h.call_task(
        0,
        &h.confirmed(0).await,
        "await ws.pr.snapshot(4); return 'constant';",
    );
    gate.reached().await;
    intent_core::with_caller(Caller::Daemon, async {
        h.original
            .schedule_workspace_delete(h.git.workspace.id.clone(), 0)
            .await
            .unwrap();
        tokio::time::timeout(WAIT, async {
            while h
                .original
                .pending_workspace_deletes
                .deadline(&PendingDeleteSubject::Workspace(h.git.workspace.id.clone()))
                .unwrap()
                .is_some()
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(!h
            .original
            .cancel_workspace_delete(h.git.workspace.id.clone())
            .await
            .unwrap());
    })
    .await;
    gate.release.add_permits(1);
    let reply = task.await.unwrap();
    assert!(
        reply
            .to_string()
            .contains("Private result delivery refused"),
        "{reply}"
    );
    tokio::time::timeout(WAIT, async {
        while h
            .original
            .store
            .get_workspace(&h.git.workspace.id)
            .await
            .is_ok()
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(!h
        .f
        .manager
        .handles
        .lock()
        .unwrap()
        .contains_key(&h.f.row.id));
    h.finish().await;
}

#[intent_test_macros::daemon_test]
async fn confirmed_read_artifact_and_attachment_start_preserve_actual_effects() {
    for (boundary, after) in [
        (Boundary::ArtifactStart, false),
        (Boundary::ArtifactStart, true),
        (Boundary::Attachments, false),
        (Boundary::Attachments, true),
    ] {
        let control = Arc::new(NativeControl::default());
        let h = NativeHarness::observed("3.0", true, false, Some(control.clone()), None).await;
        h.auth
            .registry
            .apply(&[("workspaceApi.maxOutputChars".into(), json!(1000))])
            .unwrap();
        h.start().await;
        let gate = control.at(boundary, after);
        let code = if boundary == Boundary::ArtifactStart {
            "const p=await ws.pr.snapshot(4); return p.title.repeat(300);"
        } else {
            "const p=await ws.pr.snapshot(4); return {__mcpContentItems:[{type:'resource',resource:{uri:'fixture://original',mimeType:'application/json',text:JSON.stringify({title:p.title})}}]};"
        };
        let task = h.call_task(0, &h.confirmed(0).await, code);
        gate.reached().await;
        let folder = h.git.dir.path().join("tool-outputs");
        assert!(!folder.exists());
        assert_eq!(
            h.original
                .turn_attachments()
                .pending_count_by_mime(&h.f.row.id, "application/json"),
            0
        );
        assert!(h.f.manager.interrupt(&h.f.row.id).await);
        gate.release.add_permits(1);
        let reply = task.await.unwrap();
        assert!(
            reply
                .to_string()
                .contains("Private result delivery refused"),
            "{boundary:?}/{after}: {reply}"
        );
        if boundary == Boundary::ArtifactStart {
            assert_eq!(folder.exists(), after);
            if after {
                let files = std::fs::read_dir(&folder)
                    .unwrap()
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap();
                assert_eq!(files.len(), 1);
                assert!(std::fs::read_to_string(files[0].path())
                    .unwrap()
                    .contains("actual review"));
            }
        } else {
            assert_eq!(
                h.original
                    .turn_attachments()
                    .pending_count_by_mime(&h.f.row.id, "application/json"),
                usize::from(after)
            );
        }
        assert!(h
            .call(0, "workspace-mcp", "return 'ordinary completed effect';")
            .await
            .to_string()
            .contains("ordinary completed effect"));
        h.finish().await;
    }
}

async fn native_metadata_delivery_ordering(replace_handle: bool, install_before_old: bool) {
    use crate::repository_admission::lifecycle::physical_owner::RepositoryCreationIntent;
    let h = NativeHarness::new("3.0", true, false).await;
    let store = &h.f.manager.services.store;
    store
        .set_agent_effort_levels(
            &h.f.row.workspace_id,
            &h.f.row.id,
            Some(&["previous".into()]),
            "2026-09-28T00:00:00Z",
        )
        .await
        .unwrap();
    for sql in [
        "CREATE TABLE observed_model_metadata (id TEXT)",
        "CREATE TABLE observed_effort_metadata (id TEXT)",
        "CREATE TRIGGER pause_model_metadata AFTER UPDATE OF resolved_model ON agent_session BEGIN INSERT INTO observed_model_metadata VALUES (NEW.id); END",
        "CREATE TRIGGER pause_effort_metadata AFTER UPDATE OF effort_levels ON agent_session BEGIN INSERT INTO observed_effort_metadata VALUES (NEW.id); END",
    ] { sqlx::query(sql).execute(store.write_pool()).await.unwrap(); }
    let (model_entered, model_wait) = tokio::sync::oneshot::channel();
    let (model_release, model_resume) = std::sync::mpsc::channel();
    let (effort_entered, effort_wait) = tokio::sync::oneshot::channel();
    let (effort_release, effort_resume) = std::sync::mpsc::channel();
    let mut model_pause = Some((model_entered, model_resume));
    let mut effort_pause = Some((effort_entered, effort_resume));
    let mut writer = store.write_pool().acquire().await.unwrap();
    writer
        .lock_handle()
        .await
        .unwrap()
        .set_update_hook(move |change| {
            let gate = match change.table {
                "observed_model_metadata" => model_pause.take(),
                "observed_effort_metadata" => effort_pause.take(),
                _ => None,
            };
            if let Some((entered, resume)) = gate {
                let _ = entered.send(());
                let _ = resume.recv_timeout(WAIT);
            }
        });
    drop(writer);
    let original = h.start_task();
    tokio::time::timeout(WAIT, model_wait)
        .await
        .unwrap()
        .unwrap();
    let old_id = h.f.stored().await.acp_session_id.unwrap();
    assert!(
        callback(&h.origin).is_none(),
        "consumed owner remains inside finish metadata await"
    );
    let replacement = if replace_handle {
        Some(h.replacement().await)
    } else {
        None
    };
    let (peer, origin) = replacement
        .as_ref()
        .map_or((&h.node, &h.origin), |(node, origin)| (node, origin));
    let client = intent_acp::handshake::handshake_with_callbacks(
        peer.connection.clone(),
        intent_providers::provider_config("claude-code"),
        CallbackOffer::V1,
    )
    .await
    .unwrap()
    .callbacks
    .unwrap();
    let servers = h.f.manager.handles.lock().unwrap()[&h.f.row.id]
        .session_mcp_servers
        .clone();
    let (creator, attempt) = origin
        .begin_session(RepositoryCreationIntent::Replace {
            expected: Some(old_id.clone()),
        })
        .unwrap();
    let (producer_done, produced) = tokio::sync::oneshot::channel();
    let mut initializing = Box::pin(creator.initialize_compatible(|| async {
        let response = client.new_session(&h.git.path, servers, None).await?;
        producer_done.send(()).unwrap();
        Ok::<_, intent_acp::AcpError>((response.response.session_id.0.to_string(), response))
    }));
    tokio::select! {
        biased;
        result = &mut initializing => panic!("Store writer still held: {:?}", result.result),
        produced = produced => produced.unwrap(),
    }
    model_release.send(()).unwrap();
    let result = initializing.await;
    let response = result.producer.unwrap();
    let new_id = response.response.session_id.0.to_string();
    assert_eq!(
        crate::agent_session::compatibility_session_id(result.result.unwrap()),
        new_id
    );
    let owner = result.owner.unwrap();
    let captured = owner.callback().capture();
    let outcome = crate::agent_session::RepositorySessionOutcome {
        response: new_id.clone(),
        owner: Ok(owner),
        query: response.query,
    };
    let mut later = Some((attempt, outcome));
    if install_before_old {
        let (attempt, outcome) = later.take().unwrap();
        let (id, delivery) = origin.accept_session(attempt, &h.node.connection, outcome);
        assert_eq!(id, new_id);
        assert!(delivery.is_some());
        deliver_optional(delivery).await;
    }
    tokio::time::timeout(WAIT, effort_wait)
        .await
        .unwrap()
        .unwrap();
    assert!(current(&captured, h.f.caller()).await);
    effort_release.send(()).unwrap();
    assert_eq!(original.await.unwrap().unwrap(), old_id);
    if let Some((attempt, outcome)) = later {
        assert!(
            callback(origin).is_none(),
            "old completion must not install before its successor"
        );
        let (id, delivery) = origin.accept_session(attempt, &h.node.connection, outcome);
        assert_eq!(id, new_id);
        deliver_optional(delivery).await;
    }
    assert!(current(&captured, h.f.caller()).await);
    assert_eq!(
        h.f.stored().await.acp_session_id.as_deref(),
        Some(new_id.as_str())
    );
    assert_eq!(h.f.writes().await, 2);
    let inspect = h.node.call("fixture/inspect", json!({})).await;
    let (new_calls, registrations) = if let Some((node, _)) = &replacement {
        let second = node.call("fixture/inspect", json!({})).await;
        (
            count_requests(&inspect, "session/new") + count_requests(&second, "session/new"),
            count_requests(&inspect, METHOD) + count_requests(&second, METHOD),
        )
    } else {
        (
            count_requests(&inspect, "session/new"),
            count_requests(&inspect, METHOD),
        )
    };
    assert_eq!(new_calls, 2);
    assert_eq!(
        registrations, 1,
        "obsolete receipt never reaches the custom dispatcher"
    );
    let query = usize::from(!replace_handle);
    let names = peer.call("fixture/names", json!({"query":query})).await;
    let alias = names
        .as_array()
        .unwrap()
        .iter()
        .filter_map(Value::as_str)
        .find(|name| name.starts_with("intent-callback-"))
        .unwrap();
    let reply = peer
        .call(
            "fixture/call",
            json!({"query":query,"name":alias,"code":"return await ws.pr.snapshot(4);"}),
        )
        .await;
    assert!(reply.to_string().contains("actual review"), "{reply}");
    let mut writer = store.write_pool().acquire().await.unwrap();
    writer.lock_handle().await.unwrap().remove_update_hook();
    drop(writer);
    origin.retire();
    if let Some((node, _)) = replacement {
        node.finish().await;
    }
    h.node.finish().await;
}

#[intent_test_macros::daemon_test]
async fn confirmed_read_metadata_completion_preserves_newer_owner_in_all_four_orders() {
    for replace_handle in [false, true] {
        for install_before_old in [false, true] {
            native_metadata_delivery_ordering(replace_handle, install_before_old).await;
        }
    }
}

#[intent_test_macros::daemon_test]
async fn confirmed_read_late_sdk_completion_keeps_new_original_owner_in_all_four_orders() {
    for replace in [false, true] {
        for old_first in [false, true] {
            let h = NativeHarness::new("3.0", true, false).await;
            h.node.call("fixture/next", json!({"hold":true})).await;
            let old = h.start_task();
            h.node.call("fixture/entered", json!({})).await;
            let old_id = h.f.stored().await.acp_session_id.unwrap();
            let old_capture = callback(&h.origin).unwrap().capture();
            let replacement = if replace {
                Some(h.replacement().await)
            } else {
                None
            };
            let (peer, origin) = replacement
                .as_ref()
                .map_or((&h.node, &h.origin), |(p, o)| (p, o));
            peer.call("fixture/next", json!({"hold":true})).await;
            h.recreate();
            let new = h.start_task();
            let query = usize::from(!replace);
            peer.call("fixture/entered", json!({"query":query})).await;
            let fresh = callback(origin).unwrap().capture();
            assert!(!current(&old_capture, h.f.caller()).await);
            assert!(current(&fresh, h.f.caller()).await);
            assert_eq!(old.await.unwrap().unwrap(), old_id);
            if old_first {
                h.node.call("fixture/release", json!({"query":0})).await;
                h.node.call("fixture/settled", json!({"query":0})).await;
            }
            peer.call("fixture/release", json!({"query":query})).await;
            let new_id = new.await.unwrap().unwrap();
            if !old_first {
                h.node.call("fixture/release", json!({"query":0})).await;
                h.node.call("fixture/settled", json!({"query":0})).await;
            }
            assert_ne!(new_id, old_id);
            assert_eq!(
                h.f.stored().await.acp_session_id.as_deref(),
                Some(new_id.as_str())
            );
            assert_eq!(h.f.writes().await, 2);
            assert!(current(&fresh, h.f.caller()).await);
            assert!(h
                .names(0)
                .await
                .iter()
                .all(|n| !n.starts_with("intent-callback-")));
            let names = peer.call("fixture/names", json!({"query":query})).await;
            let name = names
                .as_array()
                .unwrap()
                .iter()
                .filter_map(Value::as_str)
                .find(|n| n.starts_with("intent-callback-"))
                .unwrap();
            let reply = peer
                .call(
                    "fixture/call",
                    json!({"query":query,"name":name,"code":"return await ws.pr.snapshot(4);"}),
                )
                .await;
            assert!(
                reply.to_string().contains("actual review"),
                "{replace}/{old_first}: {reply}"
            );
            let original = h.node.call("fixture/inspect", json!({})).await;
            let mut new_calls = count_requests(&original, "session/new");
            let mut registrations = count_requests(&original, METHOD);
            if let Some((node, _)) = &replacement {
                let inspect = node.call("fixture/inspect", json!({})).await;
                new_calls += count_requests(&inspect, "session/new");
                registrations += count_requests(&inspect, METHOD);
            }
            assert_eq!(new_calls, 2);
            assert_eq!(registrations, 2);
            origin.retire();
            if let Some((node, _)) = replacement {
                node.finish().await;
            }
            h.finish().await;
        }
    }
}

#[intent_test_macros::daemon_test]
async fn confirmed_read_registration_failure_cancel_and_uncertainty_never_replay_or_upgrade() {
    for mode in ["error", "cancel", "uncertain"] {
        let h = NativeHarness::new("3.0", true, false).await;
        if mode == "error" {
            h.node
                .call("fixture/next", json!({"error":"original control refusal"}))
                .await;
        } else {
            h.node.call("fixture/next", json!({"hold":true})).await;
        }
        let start = h.start_task();
        if mode != "error" {
            h.node.call("fixture/entered", json!({})).await;
        }
        if mode == "cancel" {
            start.abort();
            assert!(start.await.unwrap_err().is_cancelled());
        } else {
            let id = tokio::time::timeout(WAIT, start)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert_eq!(
                h.f.stored().await.acp_session_id.as_deref(),
                Some(id.as_str())
            );
        }
        assert_eq!(h.f.writes().await, 1);
        let old = callback(&h.origin).unwrap().capture();
        assert!(!current(&old, h.f.caller()).await);
        assert!(h.origin.state.lock().unwrap().endpoint.is_none());
        if mode != "error" {
            h.node.call("fixture/release", json!({})).await;
            h.node.call("fixture/settled", json!({})).await;
        }
        assert!(h
            .names(0)
            .await
            .iter()
            .all(|n| !n.starts_with("intent-callback-")));
        assert!(h
            .call(0, "workspace-mcp", "return 'completed ordinary original';")
            .await
            .to_string()
            .contains("completed ordinary original"));
        assert_eq!(h.http.count(), 0);
        let inspect = h.node.call("fixture/inspect", json!({})).await;
        assert_eq!(count_requests(&inspect, METHOD), 1);
        assert_eq!(count_requests(&inspect, "session/new"), 1);
        // The adapter's own five-second uncertainty response precedes the
        // client's six-second guard. Only an aborted client sends cancellation.
        assert_eq!(
            count_requests(&inspect, "$/cancel_request"),
            usize::from(mode == "cancel")
        );
        h.recreate();
        h.start().await;
        assert!(h
            .call(1, &h.confirmed(1).await, "return await ws.pr.snapshot(4);")
            .await
            .to_string()
            .contains("actual review"));
        assert!(!current(&old, h.f.caller()).await);
        assert_eq!(h.f.writes().await, 2);
        h.finish().await;
    }
}

#[intent_test_macros::daemon_test]
async fn confirmed_read_pending_completed_response_survives_distinct_registration() {
    let pending = Arc::new(NativeControl::default());
    let h = NativeHarness::observed("3.0", true, false, None, Some(pending.clone())).await;
    h.node.call("fixture/next", json!({"hold":true})).await;
    let start = h.start_task();
    h.node.call("fixture/entered", json!({})).await;
    let held = NativeHold::new();
    *pending.completed_body.lock().unwrap() = Some(held.clone());
    let ordinary = h.call_task(
        0,
        "workspace-mcp",
        "return 'already completed original response';",
    );
    held.reached().await;
    h.node.call("fixture/release", json!({})).await;
    start.await.unwrap().unwrap();
    assert!(h
        .call(0, &h.confirmed(0).await, "return await ws.pr.snapshot(4);")
        .await
        .to_string()
        .contains("actual review"));
    held.release.add_permits(1);
    assert!(ordinary
        .await
        .unwrap()
        .to_string()
        .contains("already completed original response"));
    assert!(pending
        .reads
        .lock()
        .unwrap()
        .iter()
        .all(|r| r.read.is_err()));
    assert_eq!(h.f.writes().await, 1);
    h.finish().await;
}

#[intent_test_macros::daemon_test]
async fn confirmed_read_direct_companion_uses_same_original_policy_as_delivered_tcp() {
    for after in [false, true] {
        let control = Arc::new(NativeControl::default());
        let h = NativeHarness::observed("3.0", true, false, Some(control.clone()), None).await;
        h.start().await;
        assert!(h
            .call(0, &h.confirmed(0).await, "return await ws.pr.snapshot(4);")
            .await
            .to_string()
            .contains("actual review"));
        // Companion in-process boundary on the same original confirmed owner;
        // the positive delivered TCP endpoint above remains the delivery proof.
        let blueprint = h.origin.state.lock().unwrap().blueprint.clone().unwrap();
        let server = blueprint
            .server
            .confirmed_server(callback(&h.origin).unwrap(), None);
        let gate = control.at(Boundary::DirectResponse, after);
        let task = tokio::spawn(async move {
            crate::repository_read_source::tests::run(&server, "return await ws.pr.snapshot(4);")
                .await
        });
        gate.reached().await;
        assert!(h.f.manager.interrupt(&h.f.row.id).await);
        gate.release.add_permits(1);
        let reply = task.await.unwrap();
        assert_eq!(
            reply.to_string().contains("actual review"),
            after,
            "{reply}"
        );
        if !after {
            assert!(reply
                .to_string()
                .contains("Private result delivery refused"));
        }
        h.finish().await;
    }
}

#[intent_test_macros::daemon_test]
async fn confirmed_read_actual_file_retirement_and_provider_partial_results_stay_original() {
    let h = NativeHarness::new("3.0", true, false).await;
    h.start().await;
    let name = h.confirmed(0).await;
    let mut file =
        crate::source_control_auth_ops::repository_owner::secret_reader::tests::PausedRead::install(
            &h.auth,
        );
    let task = h.call_task(0, &name, "return await ws.pr.snapshot(4);");
    file.entered().await;
    assert!(h.f.manager.interrupt(&h.f.row.id).await);
    file.resume();
    assert!(!task.await.unwrap().to_string().contains("actual review"));
    assert_eq!(
        h.http.count(),
        0,
        "retired file acquisition never dispatches"
    );
    assert!(h
        .call(0, &name, "return await ws.pr.snapshot(4);")
        .await
        .to_string()
        .contains("actual review"));
    h.finish().await;
    for status in [401, 403, 404, 429] {
        let h = NativeHarness::new("3.0", true, false).await;
        h.start().await;
        h.http
            .status("/api/v4/projects/group%2Fproject/merge_requests/4", status);
        let reply=h.call(0,&h.confirmed(0).await,"try {await ws.pr.snapshot(4);} catch(e) {} return 'constant after original denial';").await;
        assert!(
            !reply.to_string().contains("actual review"),
            "{status}: {reply}"
        );
        assert!(h.http.count() > 0);
        assert_eq!(
            h.original.gitlab_repository_settled_connection().is_ok(),
            status != 401
        );
        h.finish().await;
    }
    let h = NativeHarness::new("3.0", true, false).await;
    h.start().await;
    h.http.status(
        "/api/v4/projects/group%2Fproject/merge_requests/4/approvals",
        429,
    );
    let reply = h
        .call(0, &h.confirmed(0).await, "return await ws.pr.snapshot(4);")
        .await;
    assert!(reply.to_string().contains("actual review"), "{reply}");
    assert!(reply.to_string().contains("rate-limited"), "{reply}");
    assert!(h.original.sweep_rate_limit_paused_until().is_none());
    h.finish().await;
}

#[intent_test_macros::daemon_test]
async fn confirmed_read_ambiguous_sources_refuse_without_acquisition_and_github_stays_ordinary() {
    let h = NativeHarness::new("3.0", true, false).await;
    h.start().await;
    let name = h.confirmed(0).await;
    h.git.git(
        &h.git.path,
        &[
            "remote",
            "add",
            "other",
            "https://github.com/Actual/Repository.git",
        ],
    );
    let reply = h
        .call(
            0,
            &name,
            "try {return await ws.pr.snapshot(4);} catch(e) {return e.message;}",
        )
        .await;
    assert!(
        reply
            .to_string()
            .contains(crate::repository_read_source::REFUSAL),
        "{reply}"
    );
    assert_eq!(h.http.count(), 0);
    h.git.git(&h.git.path, &["remote", "remove", "origin"]);
    intent_core::with_caller(Caller::Daemon, async {
        h.original
            .settings_update(
                json!([{"path":intent_sourcecontrol::gitlab_token::SECRET_ACCOUNT,"value":""}]),
            )
            .await
            .unwrap();
        assert!(h.original.gitlab_repository_settled_connection().is_err());
        let captured = crate::repository_read_source::CapturedReview::capture(
            &h.original,
            h.git.workspace.id.clone(),
            4,
        );
        let crate::repository_read_source::ReadOutcome::Github(repo) =
            captured.read(&h.original).await.unwrap()
        else {
            panic!("positive local GitHub discovery")
        };
        assert_eq!(repo.owner, "actual");
        assert_eq!(repo.name, "repository");
    })
    .await;
    // This asserts local ordinary dispatch selection, not a GitHub provider call.
    assert_eq!(h.http.count(), 0);
    h.finish().await;
}

async fn native_transcript_tools(h: &NativeHarness) -> Vec<Value> {
    h.f.manager
        .services
        .store
        .get_agent_messages(&h.f.row.id, None)
        .await
        .unwrap()
        .into_iter()
        .flat_map(|m| m.content.as_array().unwrap().clone())
        .filter(|b| b["type"] == "tool_use")
        .collect()
}

#[intent_test_macros::daemon_test]
async fn confirmed_read_prompt_transcript_uses_only_original_connection_exact_routes_and_preserves_result_names(
) {
    let h = NativeHarness::new("3.0", true, false).await;
    let id = h.start().await;
    let alias = h.confirmed(0).await;
    assert!(h
        .call(0, &alias, "return await ws.pr.snapshot(4);")
        .await
        .to_string()
        .contains("actual review"));
    let title = format!("mcp__{alias}__workspace_api");
    let foreign = "mcp__foreign__workspace_api";
    let nested = format!("{title}__nested");
    h.node.call("fixture/prompt-notes",json!({"notes":[
        {"sessionUpdate":"tool_call","toolCallId":"known","title":title,"status":"in_progress","rawInput":{"code":"return 1"}},
        {"sessionUpdate":"tool_call_update","toolCallId":"known","status":"completed","rawOutput":{"originalResult":"known"}},
        tool_update("foreign",foreign), tool_update("nested",&nested),
    ]})).await;
    {
        let mut notes = h.node.notes.lock().await;
        while notes.try_recv().is_ok() {}
        h.f.manager
            .services
            .run_prompt_turn(
                &h.node.connection,
                &mut notes,
                &h.f.row.id,
                &h.f.row.workspace_id,
                &id,
                vec![
                    serde_json::from_value(json!({"type":"text","text":"local scripted output"}))
                        .unwrap(),
                ],
                None,
            )
            .await
            .unwrap();
    }
    let tools = native_transcript_tools(&h).await;
    assert_eq!(tools.len(), 3);
    let known = tools.iter().find(|b| b["toolCallId"] == "known").unwrap();
    assert_eq!(known["name"], "workspace_api");
    assert_eq!(known["input"]["_acpTitle"], title);
    assert_eq!(
        tools.iter().find(|b| b["toolCallId"] == "foreign").unwrap()["name"],
        "foreign_workspace_api"
    );
    assert_eq!(
        tools.iter().find(|b| b["toolCallId"] == "nested").unwrap()["name"],
        format!("{alias}_workspace_api__nested")
    );
    let messages =
        h.f.manager
            .services
            .store
            .get_agent_messages(&h.f.row.id, None)
            .await
            .unwrap();
    assert!(messages
        .iter()
        .any(|m| m.content.to_string().contains("originalResult")));
    // The same literal alias arriving on another actual ACP connection has no registration.
    let other = NodePeer::new();
    other
        .call(
            "fixture/prompt-notes",
            json!({"notes":[tool_update("other-connection",&title)]}),
        )
        .await;
    {
        let mut notes = other.notes.lock().await;
        h.f.manager
            .services
            .run_prompt_turn(
                &other.connection,
                &mut notes,
                &h.f.row.id,
                &h.f.row.workspace_id,
                &id,
                vec![
                    serde_json::from_value(json!({"type":"text","text":"foreign connection"}))
                        .unwrap(),
                ],
                None,
            )
            .await
            .unwrap();
    }
    let tools = native_transcript_tools(&h).await;
    assert_eq!(
        tools
            .iter()
            .find(|b| b["toolCallId"] == "other-connection")
            .unwrap()["name"],
        format!("{alias}_workspace_api")
    );
    assert_eq!(h.f.writes().await, 1);
    assert!(h
        .call(0, &alias, "return await ws.pr.snapshot(4);")
        .await
        .to_string()
        .contains("actual review"));
    h.origin.retire();
    other.finish().await;
    h.node.finish().await;
}

#[intent_test_macros::daemon_test]
async fn confirmed_read_idle_wake_and_zero_settle_transcripts_keep_captured_connection_routes() {
    let h = NativeHarness::new("3.0", true, false).await;
    let id = h.start().await;
    let alias = h.confirmed(0).await;
    assert!(h
        .call(0, &alias, "return await ws.pr.snapshot(4);")
        .await
        .to_string()
        .contains("actual review"));
    let title = format!("mcp__{alias}__workspace_api");
    {
        let mut notes = h.node.notes.lock().await;
        while notes.try_recv().is_ok() {}
    }
    h.node
        .call(
            "fixture/note",
            json!({"sessionId":id,"update":tool_update("wake",&title)}),
        )
        .await;
    assert!(
        h.f.manager
            .wake_listener_tick(&h.f.row.id, &h.f.row.workspace_id)
            .await
    );
    tokio::time::timeout(WAIT, async {
        loop {
            if !h.f.manager.is_busy(&h.f.row.id) && !native_transcript_tools(&h).await.is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        native_transcript_tools(&h).await[0]["name"],
        "workspace_api"
    );
    // Drive the same zero-settle path used by a wake tick which loses its busy
    // claim, with a later notification already buffered for the prompt owner.
    h.node
        .call(
            "fixture/note",
            json!({"sessionId":id,"update":tool_update("zero",&title)}),
        )
        .await;
    h.node.call("fixture/note",json!({"sessionId":id,"update":tool_update("left-buffered","mcp__user__workspace_api")})).await;
    let (notes, routes) = {
        let handles = h.f.manager.handles.lock().unwrap();
        let original = &handles[&h.f.row.id];
        (
            original.notifications.clone(),
            original.connection.callback_tool_routes(),
        )
    };
    {
        let mut notes = notes.lock().await;
        let first = notes.try_recv().unwrap();
        h.f.manager
            .services
            .run_harness_wake_turn_with_routes(
                &mut notes,
                first,
                &h.f.row.id,
                &h.f.row.workspace_id,
                Duration::ZERO,
                routes,
            )
            .await;
        assert_eq!(
            notes.try_recv().unwrap().params["update"]["toolCallId"],
            "left-buffered"
        );
    }
    let tools = native_transcript_tools(&h).await;
    assert_eq!(tools.len(), 2);
    assert!(tools.iter().all(|b| b["name"] == "workspace_api"));
    assert!(current(&callback(&h.origin).unwrap().capture(), h.f.caller()).await);
    assert!(h
        .call(0, &alias, "return await ws.pr.snapshot(4);")
        .await
        .to_string()
        .contains("actual review"));
    h.origin.retire();
    h.node.finish().await;
}

#[intent_test_macros::daemon_test]
async fn confirmed_read_cache_hit_keeps_original_ledger_without_second_provider_read() {
    let control = Arc::new(NativeControl::default());
    let h = NativeHarness::observed("3.0", true, false, Some(control.clone()), None).await;
    h.start().await;
    let gate = control.at(Boundary::HostPromise, true);
    let task = h.call_task(
        0,
        &h.confirmed(0).await,
        "await ws.pr.snapshot(4); return await ws.pr.snapshot(4);",
    );
    gate.reached().await;
    let first = h.http.count();
    assert!(first > 0);
    gate.release.add_permits(1);
    let reply = task.await.unwrap();
    assert!(reply.to_string().contains("actual review"), "{reply}");
    assert_eq!(
        h.http.count(),
        first,
        "same original cache hit must not dispatch a second provider read"
    );
    assert!(
        control
            .events
            .lock()
            .unwrap()
            .contains(&(Boundary::TcpResponse, 3)),
        "lazy read and both original calls remain obligations"
    );
    h.finish().await;
}

#[intent_test_macros::daemon_test]
async fn confirmed_read_hard_stop_retires_delivery_but_keeps_accepted_ordinary_result_route() {
    let h = NativeHarness::new("3.0", true, false).await;
    h.start().await;
    let name = h.confirmed(0).await;
    assert!(h
        .call(0, &name, "return await ws.pr.snapshot(4);")
        .await
        .to_string()
        .contains("actual review"));
    let original = callback(&h.origin).unwrap().capture();
    let endpoint = h.origin.state.lock().unwrap().endpoint.clone().unwrap();
    assert!(h.f.manager.stop(&h.f.row.id).await);
    assert!(!current(&original, h.f.caller()).await);
    assert!(endpoint.bridge.lock().unwrap().is_none());
    let before = h.http.count();
    let denied = h
        .call(
            0,
            &name,
            "try {return await ws.pr.snapshot(4);} catch(e) {return e.message;}",
        )
        .await;
    assert!(
        denied
            .to_string()
            .contains(crate::repository_read_source::REFUSAL),
        "{denied}"
    );
    assert_eq!(h.http.count(), before);
    assert!(h
        .call(
            0,
            &name,
            "return 'ordinary completion on accepted old socket';"
        )
        .await
        .to_string()
        .contains("ordinary completion on accepted old socket"));
    assert_eq!(h.f.writes().await, 1);
    h.finish().await;
}

// Combined alias coverage uses the delivered original manager endpoint. All
// helpers above, the production decorator and the imported aliases stay exact.
mod confirmed_aliases {
    use super::*;

    const MR: &str = "/api/v4/projects/group%2Fproject/merge_requests/4";

    fn expression(namespace: &str, raw: bool) -> String {
        if raw {
            format!("host({{method:'{namespace}.snapshot',args:{{prNumber:4}}}})")
        } else {
            format!("ws.{namespace}.snapshot(4)")
        }
    }

    fn json_result(reply: &Value) -> Value {
        // The retained Query's MCP client returns CallToolResult directly.
        assert_eq!(reply["isError"], false, "{reply}");
        let text = reply["content"][0]["text"].as_str().unwrap();
        let encoded: String = serde_json::from_str(text).unwrap();
        serde_json::from_str(&encoded).unwrap()
    }

    fn delivery_refused(reply: &Value) {
        assert_eq!(
            reply,
            &json!({"content":[{"type":"text","text":"Private result delivery refused"}],"isError":true}),
            "{reply}"
        );
    }

    fn no_spill(h: &NativeHarness) {
        h.auth
            .registry
            .apply(&[("workspaceApi.maxOutputChars".into(), json!(0))])
            .unwrap();
    }

    fn original_records(control: &NativeControl, h: &NativeHarness, count: usize) {
        let records = control.records.lock().unwrap();
        assert_eq!(records.len(), count);
        assert!(records
            .iter()
            .all(|record| Arc::ptr_eq(&record.request, &records[0].request)));
        assert!(records[0].request.retains(h.original.as_ref()));
    }

    async fn one_original_session(h: &NativeHarness) {
        let inspect = h.node.call("fixture/inspect", json!({})).await;
        assert_eq!(count_requests(&inspect, "session/new"), 1);
        assert_eq!(count_requests(&inspect, "session/load"), 0);
        assert_eq!(count_requests(&inspect, METHOD), 1);
        assert_eq!(inspect["initializations"], json!([1]));
        assert_eq!(h.f.writes().await, 1);
    }

    #[intent_test_macros::daemon_test]
    async fn confirmed_alias_public_raw_parity_reuses_original_cache() {
        for first in ["pr", "mr"] {
            for observed in [false, true] {
                let control = observed.then(|| Arc::new(NativeControl::default()));
                let h = NativeHarness::observed("3.0", true, false, control.clone(), None).await;
                no_spill(&h);
                h.start().await;
                let name = h.confirmed(0).await;
                let other = if first == "pr" { "mr" } else { "pr" };
                let code = format!(
                    "const first=await ws.{first}.snapshot(4);
                    return JSON.stringify({{same:ws.pr===ws.mr,values:[first,
                        await ws.{other}.snapshot(4),await {},await {}]}});",
                    expression("pr", true),
                    expression("mr", true)
                );
                let gate = control
                    .as_ref()
                    .map(|control| control.at(Boundary::HostPromise, true));
                let task = h.call_task(0, &name, &code);
                if let Some(gate) = &gate {
                    gate.reached().await;
                    assert_eq!(h.http.count(), 4, "one complete original fetch");
                    gate.release.add_permits(1);
                }
                let reply = task.await.unwrap();
                let value = json_result(&reply);
                assert_eq!(value["same"], true);
                let values = value["values"].as_array().unwrap();
                assert_eq!(values.len(), 4);
                assert!(values.windows(2).all(|pair| pair[0] == pair[1]));
                let snapshot = &values[0];
                assert_eq!(snapshot["repo"], "group/project");
                assert_eq!(snapshot["prNumber"], 4);
                assert_eq!(snapshot["title"], "actual review");
                assert_eq!(snapshot["resource"]["repository"]["provider"], "gitlab");
                assert_eq!(
                    snapshot["resource"]["repository"]["instanceBaseUrl"],
                    "https://gitlab.test/forge"
                );
                assert_eq!(snapshot["details"]["resource"], snapshot["resource"]);
                assert_eq!(snapshot["details"]["source"]["projectId"], "42");
                assert_eq!(snapshot["availability"]["checks"], "available");
                assert!(snapshot["requirements"].is_object());
                assert_eq!(h.http.count(), 4, "all later alias calls reuse the cache");
                assert_eq!(h.original.pr_cache.lock().unwrap().len(), 1);
                assert!(!reply.to_string().contains("stored-pat"));
                if let Some(control) = &control {
                    original_records(control, &h, 5);
                    assert_eq!(
                        control
                            .events
                            .lock()
                            .unwrap()
                            .iter()
                            .filter(|event| **event == (Boundary::TcpResponse, 5))
                            .count(),
                        1
                    );
                }
                one_original_session(&h).await;
                h.finish().await;
            }
        }
    }

    #[intent_test_macros::daemon_test]
    async fn confirmed_alias_mixed_names_share_one_acquisition_budget() {
        for count in [63, 64] {
            let control = Arc::new(NativeControl::default());
            let h = NativeHarness::observed("3.0", true, false, Some(control.clone()), None).await;
            no_spill(&h);
            h.start().await;
            let code = format!(
                "const calls=[()=>ws.pr.snapshot(4),()=>ws.mr.snapshot(4),
                    ()=>{},()=>{}];
                for(let i=0;i<{count};i++) {{
                    try {{await calls[i%4]();}} catch(e) {{
                        try {{await calls[(i+1)%4]();}} catch(e) {{}}
                    }}
                }}
                return 'bounded mixed original success';",
                expression("pr", true),
                expression("mr", true)
            );
            let reply = h.call(0, &h.confirmed(0).await, &code).await;
            if count == 63 {
                assert!(reply.to_string().contains("bounded mixed original success"));
                original_records(&control, &h, 64);
                assert!(control
                    .events
                    .lock()
                    .unwrap()
                    .contains(&(Boundary::TcpResponse, 64)));
            } else {
                // Catching the 65th reservation or changing spelling cannot
                // discard the original poisoned ledger or allocate a new one.
                delivery_refused(&reply);
            }
            assert_eq!(
                h.http.count(),
                4,
                "refused reservations never acquire again"
            );
            assert_eq!(h.original.pr_cache.lock().unwrap().len(), 1);
            one_original_session(&h).await;
            h.finish().await;
        }
    }

    #[intent_test_macros::daemon_test]
    async fn confirmed_alias_later_tcp_and_artifact_keep_original_obligations() {
        // Both mutations surround TCP/artifact admission. Attachment controls
        // add the missing mixed-alias effect check without a full cross product.
        let cases = [
            (Boundary::TcpResponse, false, false),
            (Boundary::TcpResponse, true, false),
            (Boundary::TcpResponse, false, true),
            (Boundary::TcpResponse, true, true),
            (Boundary::ArtifactStart, false, false),
            (Boundary::ArtifactStart, true, false),
            (Boundary::ArtifactStart, false, true),
            (Boundary::ArtifactStart, true, true),
            (Boundary::Attachments, false, false),
            (Boundary::Attachments, true, false),
        ];
        for (boundary, after, change_source) in cases {
            let control = Arc::new(NativeControl::default());
            let h = NativeHarness::observed("3.0", true, false, Some(control.clone()), None).await;
            h.auth
                .registry
                .apply(&[("workspaceApi.maxOutputChars".into(), json!(1000))])
                .unwrap();
            h.http
                .status("/api/v4/projects/group%2Fproject/merge_requests/99", 404);
            h.start().await;
            let name = h.confirmed(0).await;
            let first = if after { "mr" } else { "pr" };
            let other = if after { "pr" } else { "mr" };
            let output = match boundary {
                Boundary::ArtifactStart => "return 'original mixed output'.repeat(300);",
                Boundary::Attachments => "return {__mcpContentItems:[{type:'resource',resource:{uri:'fixture://original',mimeType:'application/json',text:JSON.stringify({title:'original mixed output'})}}]};",
                _ => "return 'original mixed output';",
            };
            let code = format!(
                "await ws.workspace.setStatusMessage('completed ordinary effect');
                await ws.{first}.snapshot(4);
                try {{await host({{method:'{other}.snapshot',args:{{prNumber:99}}}});}}
                catch(e) {{}}
                {output}"
            );
            let gate = control.at(boundary, after);
            let task = h.call_task(0, &name, &code);
            gate.reached().await;
            let requests_before_retirement = h.http.count();
            assert!(
                requests_before_retirement > 4,
                "the caught error reached its original producer"
            );
            original_records(&control, &h, 4);
            let original_records = control.records.lock().unwrap().clone();
            let folder = h.git.dir.path().join("tool-outputs");
            assert!(
                !folder.exists(),
                "START admission precedes the artifact effect"
            );
            assert_eq!(
                h.original
                    .turn_attachments()
                    .pending_count_by_mime(&h.f.row.id, "application/json"),
                0
            );
            if change_source {
                h.git.git(
                    &h.git.path,
                    &[
                        "-c",
                        "user.name=Fixture",
                        "-c",
                        "user.email=fixture@example.invalid",
                        "commit",
                        "--allow-empty",
                        "-m",
                        "replacement head after mixed reads",
                    ],
                );
            } else {
                assert!(h.f.manager.interrupt(&h.f.row.id).await);
            }
            gate.release.add_permits(1);
            let reply = task.await.unwrap();
            if boundary == Boundary::TcpResponse && after {
                assert_eq!(reply["isError"], false, "{reply}");
                assert!(reply.to_string().contains("original mixed output"));
            } else {
                delivery_refused(&reply);
            }
            assert_eq!(h.http.count(), requests_before_retirement);
            {
                let final_records = control.records.lock().unwrap();
                assert_eq!(final_records.len(), original_records.len());
                assert!(final_records
                    .iter()
                    .zip(&original_records)
                    .all(|(last, original)| {
                        // The observer allocates wrappers; these are the actual
                        // original objects retained inside each opaque record.
                        Arc::ptr_eq(&last.request, &original.request)
                            && Arc::ptr_eq(&last.operation, &original.operation)
                            && Arc::ptr_eq(&last.eligibility, &original.eligibility)
                    }));
            }
            assert_eq!(
                h.original
                    .store
                    .get_workspace(&h.git.workspace.id)
                    .await
                    .unwrap()
                    .status_message
                    .as_deref(),
                Some("completed ordinary effect")
            );
            assert_eq!(
                folder.exists(),
                boundary == Boundary::ArtifactStart && after
            );
            if folder.exists() {
                let files = std::fs::read_dir(&folder)
                    .unwrap()
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap();
                assert_eq!(files.len(), 1);
                assert!(std::fs::read_to_string(files[0].path())
                    .unwrap()
                    .contains("original mixed output"));
            }
            assert_eq!(
                h.original
                    .turn_attachments()
                    .pending_count_by_mime(&h.f.row.id, "application/json"),
                usize::from(boundary == Boundary::Attachments && after)
            );
            one_original_session(&h).await;
            h.finish().await;
        }
    }

    #[intent_test_macros::daemon_test]
    async fn confirmed_alias_partial_quota_preserves_original_public_outcome() {
        let mut results = Vec::new();
        for first in ["pr", "mr"] {
            let control = Arc::new(NativeControl::default());
            let h = NativeHarness::observed("3.0", true, false, Some(control.clone()), None).await;
            no_spill(&h);
            h.http.status(&format!("{MR}/approvals"), 429);
            h.start().await;
            let other = if first == "pr" { "mr" } else { "pr" };
            let code = format!(
                "const first=await ws.{first}.snapshot(4);
                const errors=[];
                try {{await ws.{other}.snapshot(4);}} catch(e) {{errors.push(e.message);}}
                try {{await {};}} catch(e) {{errors.push(e.message);}}
                return JSON.stringify({{first,errors}});",
                expression(first, true)
            );
            let gate = control.at(Boundary::HostPromise, true);
            let task = h.call_task(0, &h.confirmed(0).await, &code);
            gate.reached().await;
            // MR, project, approvals; original quota prevents discussions.
            assert_eq!(h.http.count(), 3);
            gate.release.add_permits(1);
            let reply = task.await.unwrap();
            let value = json_result(&reply);
            let snapshot = &value["first"];
            assert_eq!(snapshot["title"], "actual review");
            assert_eq!(snapshot["availability"]["approvals"], "rate-limited");
            assert_eq!(snapshot["availability"]["discussions"], "rate-limited");
            assert_eq!(snapshot["reviews"]["approvals"], Value::Null);
            assert_eq!(snapshot["availability"]["checks"], "available");
            assert!(snapshot.get("pausedUntil").is_none());
            assert!(h.original.sweep_rate_limit_paused_until().is_none());
            let errors = value["errors"].as_array().unwrap();
            assert_eq!(errors.len(), 2);
            assert_eq!(errors[0], errors[1]);
            assert!(errors[0].as_str().unwrap().contains("Backoff"));
            assert_eq!(h.http.count(), 3, "neither alias retries HTTP");
            assert_eq!(h.original.pr_cache.lock().unwrap().len(), 1);
            original_records(&control, &h, 6);
            assert!(control
                .events
                .lock()
                .unwrap()
                .contains(&(Boundary::TcpResponse, 6)));
            assert!(!reply.to_string().contains("stored-pat"));
            one_original_session(&h).await;
            results.push(value);
            h.finish().await;
        }
        assert_eq!(results[0], results[1]);
        // Numeric raw quota receipts remain separately inherited producer proof;
        // public availability and HTTP counts are not an observation of them.
    }
}

mod live_context {
    use super::*;
    use crate::agent_manager::{OriginalTurn, TurnOptions};
    use intent_store::{RepositorySelectionChange, RepositorySelectionWriteResult};

    const FACTS: &str = "[Repository facts — inert JSON lines]";

    async fn select(h: &NativeHarness, change: RepositorySelectionChange) {
        let old = h
            .original
            .store
            .repository_selection_snapshot(&h.git.root())
            .await
            .unwrap();
        let result = h
            .original
            .store
            .write_repository_selection(&old, change)
            .await;
        assert!(matches!(
            result.result.unwrap(),
            RepositorySelectionWriteResult::Applied(_)
                | RepositorySelectionWriteResult::Unchanged(_)
        ));
    }
    async fn selected(control: Option<Arc<NativeControl>>) -> NativeHarness {
        let h = NativeHarness::observed("3.0", true, false, control, None).await;
        select(&h, RepositorySelectionChange::Automatic).await;
        h
    }
    fn owner(h: &NativeHarness) -> Arc<RepositoryContextOwner> {
        h.origin
            .state
            .lock()
            .unwrap()
            .endpoint
            .as_ref()
            .unwrap()
            .live
            .as_ref()
            .unwrap()
            .owner
            .as_ref()
            .unwrap()
            .clone()
    }
    async fn daemon<T: Send + 'static>(
        h: &NativeHarness,
        body: impl std::future::Future<Output = T> + Send + 'static,
    ) -> T {
        intent_core::spawn_daemon(crate::host_execution::background_execution(
            h.original.as_ref().clone(),
            None,
            body,
        ))
        .await
        .unwrap()
    }
    async fn capture(h: &NativeHarness) -> OriginalTurn {
        let f = h.f.clone();
        daemon(h, async move {
            assert_eq!(intent_core::current_caller(), Some(Caller::Daemon));
            let turn = f.manager.capture_turn(&f.row.id).unwrap();
            assert!(turn
                .prompt
                .as_ref()
                .unwrap()
                .captured
                .as_ref()
                .unwrap()
                .is_ok());
            turn
        })
        .await
    }
    async fn dispatch(
        h: &NativeHarness,
        id: &str,
        turn: OriginalTurn,
        text: &str,
    ) -> intent_core::Result<intent_acp::session::StopReason> {
        let f = h.f.clone();
        let id = id.to_owned();
        let text = text.to_owned();
        daemon(h, async move {
            let prompt = serde_json::from_value(json!([{"type":"text","text":text}])).unwrap();
            f.manager
                .run_turn_owned(&f.row.id, &f.row.workspace_id, &id, prompt, None, turn)
                .await
        })
        .await
    }
    async fn prompts(h: &NativeHarness) -> Vec<Value> {
        serde_json::from_value(h.node.call("fixture/prompts", json!({})).await).unwrap()
    }
    async fn worker(h: &NativeHarness, text: &str) {
        // Real spawn_worker -> spawn_daemon -> retry_spawn_owned -> captured
        // dispatch. The already-running native peer is explicitly scripted.
        let row = h.f.stored().await;
        let settings = h.original.effective_settings();
        let workspace = h
            .original
            .store
            .get_workspace(&row.workspace_id)
            .await
            .unwrap();
        let resolved =
            crate::agent_manager::resolve_spawn(&row, Some(&workspace), &settings, None).unwrap();
        {
            let mut map = h.f.manager.handles.lock().unwrap();
            let handle = map.get_mut(&row.id).unwrap();
            handle.spawned_model = resolved.model;
            handle.spawned_provider = resolved.provider.command.into();
        }
        h.f.manager
            .send_message(
                row.id.clone(),
                row.workspace_id.clone(),
                text.into(),
                None,
                TurnOptions::default(),
            )
            .await
            .unwrap();
        tokio::time::timeout(WAIT, async {
            loop {
                if !h.f.manager.is_busy(&row.id) && h.f.manager.workers.lock().unwrap().is_empty() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("actual manager worker settles");
    }

    #[intent_test_macros::daemon_test]
    async fn confirmed_context_original_manager_mcp_public_cache_and_unresolved_facts() {
        let h = selected(None).await;
        let secrets = std::fs::read(h.original.gitlab_secret_store.path()).unwrap();
        h.start().await;
        let original = owner(&h);
        assert!(Arc::ptr_eq(&original.services, &h.original));
        assert!(h
            .original
            .store
            .shares_repository_lifecycle_domain(&h.git.store));
        let name = h.confirmed(0).await;
        let first = h.call(0, &name, "return await ws.pr.snapshot(4);").await;
        assert!(first.to_string().contains("actual review"), "{first}");
        assert!(first.to_string().contains(FACTS), "{first}");
        let requests = h.http.count();
        assert!(requests > 0);
        // The original cache is request-scoped. Two calls in the next
        // invocation acquire only the same four HTTP responses as the first.
        let cached = h
            .call(
                0,
                &name,
                "await ws.pr.snapshot(4); return await ws.pr.snapshot(4);",
            )
            .await;
        assert!(cached.to_string().contains(FACTS), "{cached}");
        assert_eq!(h.http.count(), requests * 2);
        let requests = h.http.count();
        select(
            &h,
            RepositorySelectionChange::ExplicitRemote {
                remote_name: "missing-real-choice".into(),
            },
        )
        .await;
        let unresolved = h.call(0, &name, "return 'ordinary-unresolved';").await;
        assert!(unresolved.to_string().contains(FACTS), "{unresolved}");
        assert!(
            unresolved.to_string().contains("selectionRequired"),
            "{unresolved}"
        );
        assert_eq!(h.http.count(), requests);
        assert_eq!(
            std::fs::read(h.original.gitlab_secret_store.path()).unwrap(),
            secrets
        );
        assert_eq!(h.f.writes().await, 1);
        h.finish().await;
        original.drain_jobs().await;
    }

    #[intent_test_macros::daemon_test]
    async fn confirmed_context_successful_zero_pending_absent_and_failed_never_conflate() {
        let control = Arc::new(NativeControl::default());
        let h = selected(Some(control.clone())).await;
        h.start().await;
        let reply = h
            .call(0, &h.confirmed(0).await, "return 'sealed-zero-result';")
            .await;
        assert!(reply.to_string().contains(FACTS), "{reply}");
        assert!(reply.to_string().contains("sealed-zero-result"));
        assert!(control
            .optional_events
            .lock()
            .unwrap()
            .contains(&(Boundary::TcpResponse, 0)));
        assert_eq!(h.http.count(), 0);
        let captures = control.optional_captures.load(Ordering::SeqCst);
        for name in ["workspace-mcp", "user-kept"] {
            let ordinary = h.call(0, name, "return 'unqualified-ordinary';").await;
            assert!(ordinary.to_string().contains("unqualified-ordinary"));
            assert!(!ordinary.to_string().contains(FACTS));
        }
        assert_eq!(captures, control.optional_captures.load(Ordering::SeqCst));
        h.finish().await;
        let failed = NativeHarness::new("3.0", true, true).await;
        failed.start().await;
        let reply = failed
            .call(
                0,
                &failed.confirmed(0).await,
                "try { await ws.pr.snapshot(4); } catch(e) {} return 'caught-not-a-zero-proof';",
            )
            .await;
        assert!(!reply.to_string().contains(FACTS));
        // No original read scope was ever installed, so this caught error
        // precedes qualified acquisition. Its ordinary result stays ordinary.
        assert!(
            reply.to_string().contains("caught-not-a-zero-proof"),
            "{reply}"
        );
        assert_eq!(failed.http.count(), 0);
        failed.finish().await;

        let qualified = selected(None).await;
        qualified.start().await;
        // One lazy read plus 64 calls overflows the real original ledger.
        // Catching the acquisition error cannot manufacture a successful zero.
        let refused = qualified.call(0, &qualified.confirmed(0).await,
            "for(let i=0;i<64;i++){try{await ws.pr.snapshot(4);}catch(e){}} return 'must-not-escape-failed-seal';").await;
        assert!(
            refused
                .to_string()
                .contains("Private result delivery refused"),
            "{refused}"
        );
        assert!(!refused.to_string().contains("must-not-escape-failed-seal"));
        assert!(!refused.to_string().contains(FACTS));
        assert_eq!(qualified.http.count(), 4);
        qualified.finish().await;
    }

    #[intent_test_macros::daemon_test]
    async fn confirmed_context_actual_daemon_worker_and_continuation_retain_original_transport() {
        let h = selected(None).await;
        // The scripted child already exists; actual ensure/start confirms its
        // first session and carries that capture through the real worker.
        worker(&h, "first real manager message").await;
        let id = h.f.stored().await.acp_session_id.unwrap();
        let first = prompts(&h).await;
        assert_eq!(first.len(), 1);
        assert_eq!(first[0]["params"]["sessionId"], id);
        assert!(first[0].to_string().contains(FACTS), "{}", first[0]);
        h.git.git(
            &h.git.path,
            &[
                "remote",
                "add",
                "second",
                "https://github.com/team/continuation.git",
            ],
        );
        select(
            &h,
            RepositorySelectionChange::ExplicitRemote {
                remote_name: "second".into(),
            },
        )
        .await;
        worker(&h, "subsequent actual continuation").await;
        let frames = prompts(&h).await;
        assert_eq!(frames.len(), 2);
        assert_ne!(frames[0]["id"], frames[1]["id"]);
        assert_eq!(frames[1]["params"]["sessionId"], id);
        assert!(frames[1].to_string().contains(FACTS), "{}", frames[1]);
        assert!(
            frames[1].to_string().contains("team/continuation"),
            "{}",
            frames[1]
        );
        assert!(!frames[0].to_string().contains("team/continuation"));
        assert_eq!(h.http.count(), 0);
        assert_eq!(h.f.writes().await, 1);
        let inspect = h.node.call("fixture/inspect", json!({})).await;
        assert_eq!(count_requests(&inspect, "session/new"), 1);
        h.finish().await;
    }

    #[intent_test_macros::daemon_test]
    async fn confirmed_context_saved_legacy_and_disabled_outbound_messages_remain_ordinary() {
        for (stamp, enabled) in [
            ("2.9", true),
            ("<missing>", true),
            ("future-unknown", true),
            ("3.0", false),
        ] {
            let control = Arc::new(NativeControl::default());
            let h =
                NativeHarness::observed(stamp, enabled, false, Some(control.clone()), None).await;
            let first = h.start().await;
            if stamp == "<missing>" {
                assert_ne!(h.f.stored().await.harness_version, "3.0");
            }
            assert!(h.origin.state.lock().unwrap().endpoint.is_none());
            for id in [first, h.start().await] {
                let f = h.f.clone();
                daemon(&h, async move {
                    let prompt = serde_json::from_value(
                        json!([{"type":"text","text":"unchanged historical prompt"}]),
                    )
                    .unwrap();
                    f.manager
                        .run_turn(&f.row.id, &f.row.workspace_id, &id, prompt, None)
                        .await
                        .unwrap();
                })
                .await;
            }
            h.recreate();
            let id = h.start().await;
            let f = h.f.clone();
            daemon(&h, async move {
                let prompt = serde_json::from_value(
                    json!([{"type":"text","text":"unchanged historical prompt"}]),
                )
                .unwrap();
                f.manager
                    .run_turn(&f.row.id, &f.row.workspace_id, &id, prompt, None)
                    .await
                    .unwrap();
            })
            .await;
            let frames = prompts(&h).await;
            assert_eq!(frames.len(), 3);
            for frame in frames {
                assert_eq!(
                    frame["params"]["prompt"],
                    json!([{"type":"text","text":"unchanged historical prompt"}])
                );
            }
            assert_eq!(control.optional_captures.load(Ordering::SeqCst), 0);
            assert_eq!(h.http.count(), 0);
            assert_eq!(
                count_requests(&h.node.call("fixture/inspect", json!({})).await, METHOD),
                0
            );
            h.finish().await;
        }
    }

    async fn registered(h: &NativeHarness) -> intent_core::WorkspaceGitRoot {
        let path = h.node.scratch.path().join("optional-root");
        let repo = git2::Repository::init_opts(
            &path,
            git2::RepositoryInitOptions::new().initial_head("main"),
        )
        .unwrap();
        let oid = repo.index().unwrap().write_tree().unwrap();
        let tree = repo.find_tree(oid).unwrap();
        let sig = git2::Signature::now("Fixture", "fixture@example.invalid").unwrap();
        repo.commit(Some("HEAD"), &sig, &sig, "registered fixture", &tree, &[])
            .unwrap();
        let row=serde_json::from_value(json!({"id":intent_core::WorkspaceGitRootId::new(),"workspaceId":h.f.row.workspace_id,"path":path,"source":"agent","registeredByAgentIds":[h.f.row.id],"createdAt":"2026-09-28T00:00:00Z","updatedAt":"2026-09-28T00:00:00Z"})).unwrap();
        h.original
            .store
            .upsert_workspace_git_root(&row)
            .await
            .unwrap()
            .0
    }

    #[intent_test_macros::daemon_test]
    async fn confirmed_context_first_failure_and_equal_id_replacement_keep_original_binding() {
        let failed = NativeHarness::new("3.0", true, true).await;
        failed.start().await;
        let endpoint = failed
            .origin
            .state
            .lock()
            .unwrap()
            .endpoint
            .clone()
            .unwrap();
        let live = endpoint.live.as_ref().unwrap();
        assert!(live.owner.is_err());
        assert!(RepositoryReadOwner::capture(failed.original.clone()).is_ok());
        let origin = failed.origin.clone();
        let connection = failed.node.connection.clone();
        let mut original = daemon(&failed, async move {
            origin.capture_prompt(&connection).unwrap()
        })
        .await;
        assert!(original.captured.as_ref().unwrap().is_err());
        assert!(daemon(&failed, async move { original.prepare().await })
            .await
            .is_none());
        failed.finish().await;

        let h = selected(None).await;
        let old_id = h.start().await;
        let old_owner = owner(&h);
        let original = capture(&h).await;
        let (replacement, new_origin) = h.replacement().await;
        let f = h.f.clone();
        let path = h.git.path.clone();
        let started = daemon(&h, async move {
            f.manager
                .start_session_owned(
                    &f.row.id,
                    path,
                    intent_providers::provider_config("claude-code"),
                )
                .await
                .unwrap()
        })
        .await;
        assert_eq!(started.session_id, old_id);
        assert!(!Arc::ptr_eq(&started.turn.origin, &h.origin));
        assert!(Arc::ptr_eq(&started.turn.origin, &new_origin));
        assert!(Arc::ptr_eq(
            &started.turn.connection,
            &replacement.connection
        ));
        let new_owner = new_origin
            .state
            .lock()
            .unwrap()
            .endpoint
            .as_ref()
            .unwrap()
            .live
            .as_ref()
            .unwrap()
            .owner
            .as_ref()
            .unwrap()
            .clone();
        assert!(!Arc::ptr_eq(&old_owner, &new_owner));
        dispatch(&h, &old_id, original, "old captured transport")
            .await
            .unwrap();
        assert!(!prompts(&h).await[0].to_string().contains(FACTS));
        dispatch(
            &h,
            &started.session_id,
            started.turn,
            "fresh original transport",
        )
        .await
        .unwrap();
        let frames = replacement.call("fixture/prompts", json!({})).await;
        assert_eq!(frames.as_array().unwrap().len(), 1);
        assert!(frames[0].to_string().contains(FACTS), "{frames}");
        assert_eq!(prompts(&h).await.len(), 1);
        old_owner.drain_jobs().await;
        new_origin.retire();
        replacement.finish().await;
        h.finish().await;
    }

    #[intent_test_macros::daemon_test]
    async fn confirmed_context_load_and_failed_load_new_keep_original_producers_and_capture() {
        let h = selected(None).await;
        let first = h.start().await;
        let old = capture(&h).await;
        let old_owner = owner(&h);
        let f = h.f.clone();
        let path = h.git.path.clone();
        let loaded = daemon(&h, async move {
            f.manager
                .start_session_owned(
                    &f.row.id,
                    path,
                    intent_providers::provider_config("claude-code"),
                )
                .await
                .unwrap()
        })
        .await;
        assert_eq!(loaded.session_id, first);
        assert_eq!(h.f.writes().await, 1);
        dispatch(&h, &first, old, "old before load").await.unwrap();
        assert!(!prompts(&h).await[0].to_string().contains(FACTS));
        dispatch(&h, &loaded.session_id, loaded.turn, "loaded original")
            .await
            .unwrap();
        assert!(prompts(&h).await[1].to_string().contains(FACTS));
        // Script an error for the original session/load RPC; the unchanged
        // manager fallback must produce and settle exactly one session/new.
        h.node
            .call("fixture/fail-load", json!({"sessionId":first}))
            .await;
        let f = h.f.clone();
        let path = h.git.path.clone();
        let recreated = daemon(&h, async move {
            f.manager
                .start_session_owned(
                    &f.row.id,
                    path,
                    intent_providers::provider_config("claude-code"),
                )
                .await
                .unwrap()
        })
        .await;
        assert_ne!(recreated.session_id, first);
        assert_eq!(h.f.writes().await, 2);
        dispatch(
            &h,
            &recreated.session_id,
            recreated.turn,
            "fallback new original",
        )
        .await
        .unwrap();
        assert!(prompts(&h).await[2].to_string().contains(FACTS));
        let inspect = h.node.call("fixture/inspect", json!({})).await;
        assert_eq!(count_requests(&inspect, "session/new"), 2);
        assert_eq!(count_requests(&inspect, "session/load"), 2);
        assert_eq!(count_requests(&inspect, METHOD), 3);
        old_owner.drain_jobs().await;
        h.finish().await;
    }

    #[intent_test_macros::daemon_test]
    async fn confirmed_context_optional_root_omits_but_required_selection_refuses_completed_effect()
    {
        for required in [false, true] {
            let c = Arc::new(NativeControl::default());
            let h = selected(Some(c.clone())).await;
            let root = registered(&h).await;
            h.start().await;
            let gate = c.at(Boundary::TcpResponse, false);
            let task=h.call_task(0,&h.confirmed(0).await,"await ws.workspace.setStatusMessage('completed context effect'); await ws.pr.snapshot(4); return 'original context result';");
            gate.reached().await;
            assert!(c
                .optional_events
                .lock()
                .unwrap()
                .contains(&(Boundary::TcpResponse, 2)));
            if required {
                select(
                    &h,
                    RepositorySelectionChange::ExplicitRemote {
                        remote_name: "missing-now".into(),
                    },
                )
                .await;
            } else {
                h.original
                    .store
                    .delete_workspace_git_root(&root.id)
                    .await
                    .unwrap();
            }
            gate.release.add_permits(1);
            let reply = task.await.unwrap();
            assert_eq!(
                h.original
                    .store
                    .get_workspace(&h.f.row.workspace_id)
                    .await
                    .unwrap()
                    .status_message
                    .as_deref(),
                Some("completed context effect")
            );
            assert!(!reply.to_string().contains(FACTS), "{reply}");
            assert_eq!(
                reply.to_string().contains("original context result"),
                !required,
                "{reply}"
            );
            if required {
                assert!(
                    reply
                        .to_string()
                        .contains("Private result delivery refused"),
                    "{reply}"
                );
            }
            h.finish().await;
        }
    }

    #[intent_test_macros::daemon_test]
    async fn confirmed_context_settings_and_provider_changes_distinguish_optional_from_required() {
        use intent_sourcecontrol::gitlab_token::{
            EXPIRES_AT_SECRET_ACCOUNT, REFRESH_SECRET_ACCOUNT,
        };
        for mode in ["settings", "refresh", "replacement"] {
            let c = Arc::new(NativeControl::default());
            let h = selected(Some(c.clone())).await;
            if mode == "refresh" {
                h.original
                    .gitlab_secret_store
                    .store(REFRESH_SECRET_ACCOUNT, "refresh-old")
                    .unwrap();
                let expiry = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs()
                    + 7200;
                h.original
                    .gitlab_secret_store
                    .store(EXPIRES_AT_SECRET_ACCOUNT, &expiry.to_string())
                    .unwrap();
                h.original
                    .reconcile_gitlab_repository_binding()
                    .await
                    .unwrap();
            }
            h.start().await;
            let initial = h.auth.request();
            let gate = c.at(Boundary::TcpResponse, false);
            let task = h.call_task(
                0,
                &h.confirmed(0).await,
                "await ws.pr.snapshot(4);return 'completed required provider result';",
            );
            gate.reached().await;
            let services = h.original.clone();
            let host = h.http.fixture.host.clone();
            daemon(&h,async move {
                match mode {
                    "settings" => {services.settings_update(json!([{"path":"git.autoCommit","value":false}])).await.unwrap();}
                    "refresh" => {
                        services.gitlab_secret_store.store(EXPIRES_AT_SECRET_ACCOUNT,"0").unwrap();
                        services.stored_proof_token(&crate::source_control_auth_ops::Target::Gitlab{host}).await.unwrap();
                    }
                    _ => {services.settings_update(json!([{"path":intent_sourcecontrol::gitlab_token::SECRET_ACCOUNT,"value":"replacement-token"}])).await.unwrap();}
                }
            }).await;
            if mode == "refresh" {
                let now = h.auth.request();
                assert_eq!(initial.binding, now.binding);
                assert!(now.secret_revision > initial.secret_revision);
            }
            gate.release.add_permits(1);
            let reply = task.await.unwrap();
            assert!(!reply.to_string().contains(FACTS), "{mode}: {reply}");
            assert_eq!(
                reply
                    .to_string()
                    .contains("completed required provider result"),
                mode != "replacement",
                "{mode}: {reply}"
            );
            if mode == "replacement" {
                assert!(
                    reply
                        .to_string()
                        .contains("Private result delivery refused"),
                    "{reply}"
                );
            }
            h.finish().await;
        }
    }

    #[intent_test_macros::daemon_test]
    async fn confirmed_context_queued_prompt_and_full_original_writer_keep_original_id() {
        for writer in [false, true] {
            let h = selected(None).await;
            let id = h.start().await;
            let original = capture(&h).await;
            let notes = if writer {
                None
            } else {
                Some(h.node.notes.lock().await)
            };
            if writer {
                h.node.writer.block();
                h.node
                    .connection
                    .notify("fixture/ignore", json!({}))
                    .await
                    .unwrap();
                h.node.writer.reached().await;
                let mut filled = 0;
                loop {
                    let mut notification =
                        Box::pin(h.node.connection.notify("fixture/ignore", json!({})));
                    let queued = std::future::poll_fn(|cx| {
                        std::task::Poll::Ready(
                            match std::future::Future::poll(notification.as_mut(), cx) {
                                std::task::Poll::Ready(result) => {
                                    result.unwrap();
                                    true
                                }
                                std::task::Poll::Pending => false,
                            },
                        )
                    })
                    .await;
                    if !queued {
                        break;
                    }
                    filled += 1;
                    assert!(filled <= 256);
                }
                assert!(filled > 0);
            }
            let f = h.f.clone();
            let session = id.clone();
            let (entered, ready) = tokio::sync::oneshot::channel();
            let task = intent_core::spawn_daemon(crate::host_execution::background_execution(
                h.original.as_ref().clone(),
                None,
                async move {
                    let blocks = serde_json::from_value(
                        json!([{"type":"text","text":"original queued prompt"}]),
                    )
                    .unwrap();
                    if writer {
                        // Direct writer companion: actual manager-owned capture
                        // and original Connection, using only public ACP APIs.
                        let mut original = original;
                        let guidance = original.prompt.as_mut().unwrap().prepare().await;
                        assert!(guidance.is_some());
                        let activity = intent_acp::session::ActivityTracker::new();
                        let mut waiting = Box::pin(intent_acp::session::prompt_with_guidance(
                            &original.connection,
                            &session,
                            blocks,
                            &activity,
                            guidance,
                        ));
                        std::future::poll_fn(|cx| {
                            assert!(std::future::Future::poll(waiting.as_mut(), cx).is_pending());
                            std::task::Poll::Ready(())
                        })
                        .await;
                        entered.send(()).unwrap();
                        waiting
                            .await
                            .map(|_| ())
                            .map_err(|e| intent_core::Error::Internal(e.to_string()))
                    } else {
                        entered.send(()).unwrap();
                        f.manager
                            .run_turn_owned(
                                &f.row.id,
                                &f.row.workspace_id,
                                &session,
                                blocks,
                                None,
                                original,
                            )
                            .await
                            .map(|_| ())
                    }
                },
            ));
            ready.await.unwrap();
            h.origin.interrupt_requests();
            drop(notes);
            h.node.writer.resume();
            task.await.unwrap().unwrap();
            let frames = prompts(&h).await;
            assert_eq!(frames.len(), 1);
            assert_eq!(frames[0]["params"]["sessionId"], id);
            assert_eq!(
                frames[0]["params"]["prompt"],
                json!([{"type":"text","text":"original queued prompt"}])
            );
            assert!(frames[0]["id"].is_number());
            dispatch(&h, &id, capture(&h).await, "fresh after old wait")
                .await
                .unwrap();
            assert!(prompts(&h).await[1].to_string().contains(FACTS));
            h.finish().await;
        }
    }

    #[intent_test_macros::daemon_test]
    async fn confirmed_context_optional_contention_budget_and_owned_drain_preserve_base() {
        let h = selected(None).await;
        let id = h.start().await;
        let original_owner = owner(&h);
        let gate = NativeHold::new();
        let held = gate.clone();
        let path = h.git.path.clone();
        let locks = h.original.worktree_locks.clone();
        let holder = tokio::spawn(async move {
            locks.with_lock(&path, || async { held.wait().await }).await;
        });
        gate.reached().await;
        dispatch(&h, &id, capture(&h).await, "ordinary despite busy Git")
            .await
            .unwrap();
        assert!(!prompts(&h).await[0].to_string().contains(FACTS));
        // Optional Git lock contention omits immediately; no queued read exists.
        original_owner.drain_jobs().await;
        gate.release.add_permits(1);
        holder.await.unwrap();

        // The existing settings fixture holds the actual snapshot writer lock.
        // The real context worker blocks on its original snapshot read, even
        // after the bounded original prompt preparation waiter is dropped.
        let mut original = capture(&h).await;
        let held = crate::repository_context_live::tests::BlockingHold::new();
        let registry = h.auth.registry.clone();
        let block = held.clone();
        let holder = tokio::task::spawn_blocking(move || {
            registry.context_hold_snapshot_for_test(|| block.wait());
        });
        held.reached().await;
        let session = id.clone();
        daemon(&h, async move {
            let guidance = original.prompt.as_mut().unwrap().prepare().await;
            assert!(guidance.is_none());
            let activity = intent_acp::session::ActivityTracker::new();
            let prompt = serde_json::from_value(
                json!([{"type":"text","text":"base after bounded preparation"}]),
            )
            .unwrap();
            // Direct consuming-boundary companion, using the actual manager
            // capture and its original Connection. Ordinary Service settings
            // reads are intentionally outside this held settings-lock schedule.
            intent_acp::session::prompt_with_guidance(
                &original.connection,
                &session,
                prompt,
                &activity,
                guidance,
            )
            .await
            .unwrap();
        })
        .await;
        let frames = prompts(&h).await;
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[1]["params"]["sessionId"], id);
        assert!(!frames[1].to_string().contains(FACTS));
        h.origin.interrupt_requests();
        let mut draining = Box::pin(original_owner.drain_jobs());
        std::future::poll_fn(|cx| {
            assert!(
                std::future::Future::poll(draining.as_mut(), cx).is_pending(),
                "the real blocked snapshot job still owns its work"
            );
            std::task::Poll::Ready(())
        })
        .await;
        held.release();
        holder.await.unwrap();
        tokio::time::timeout(WAIT, draining).await.unwrap();
        dispatch(&h, &id, capture(&h).await, "fresh after real drain")
            .await
            .unwrap();
        assert!(prompts(&h).await[2].to_string().contains(FACTS));
        assert_eq!(h.http.count(), 0);
        h.finish().await;
    }

    #[intent_test_macros::daemon_test]
    async fn confirmed_context_soft_interrupt_drop_hard_stop_and_fresh_sibling() {
        let h = selected(None).await;
        let id = h.start().await;
        let mut old = capture(&h).await;
        h.origin.interrupt_requests();
        assert!(daemon(
            &h,
            async move { old.prompt.as_mut().unwrap().prepare().await }
        )
        .await
        .is_none());
        let mut fresh = capture(&h).await;
        daemon(&h, async move {
            assert!(fresh.prompt.as_mut().unwrap().prepare().await.is_some());
            assert!(
                fresh.prompt.as_mut().unwrap().prepare().await.is_none(),
                "one consumed original capture"
            );
        })
        .await;
        dispatch(&h, &id, capture(&h).await, "still live after optional drop")
            .await
            .unwrap();
        assert!(prompts(&h).await[0].to_string().contains(FACTS));
        let stopped = capture(&h).await;
        assert!(h.f.manager.stop(&h.f.row.id).await);
        assert!(h.origin.capture_prompt(&h.node.connection).is_none());
        dispatch(&h, &id, stopped, "ordinary captured result after stop")
            .await
            .unwrap();
        assert!(!prompts(&h).await[1].to_string().contains(FACTS));
        let sibling = selected(None).await;
        let sibling_id = sibling.start().await;
        dispatch(
            &sibling,
            &sibling_id,
            capture(&sibling).await,
            "fresh sibling",
        )
        .await
        .unwrap();
        assert!(prompts(&sibling).await[0].to_string().contains(FACTS));
        sibling.finish().await;
        h.finish().await;
    }

    #[intent_test_macros::daemon_test]
    async fn confirmed_context_output_free_retry_has_new_capture_and_effect_errors_never_replay() {
        for mode in ["retry", "streamed", "consumer"] {
            let h = selected(None).await;
            let id = h.start().await;
            // Complete one ordinary turn to consume original session-start
            // notifications before asserting that the next attempt is output-free.
            dispatch(&h, &id, capture(&h).await, "completed warmup")
                .await
                .unwrap();
            assert_eq!(prompts(&h).await.len(), 1);
            let message = if mode == "consumer" {
                "scripted terminal consumer error"
            } else {
                "Internal error: fetch failed (ECONNRESET)"
            };
            h.node
                .call(
                    "fixture/prompt-faults",
                    json!({"errors":[message],"stream":mode=="streamed"}),
                )
                .await;
            let result = dispatch(
                &h,
                &id,
                capture(&h).await,
                "same prompt through ordinary retry rules",
            )
            .await;
            assert_eq!(result.is_ok(), mode == "retry", "{mode}: {result:?}");
            let frames = prompts(&h).await.into_iter().skip(1).collect::<Vec<_>>();
            assert_eq!(frames.len(), if mode == "retry" { 2 } else { 1 });
            for frame in &frames {
                assert_eq!(frame["params"]["sessionId"], id);
                assert!(frame.to_string().contains(FACTS), "{mode}: {frame}");
            }
            if mode == "retry" {
                assert_ne!(frames[0]["id"], frames[1]["id"]);
            }
            assert_eq!(h.f.writes().await, 1);
            h.finish().await;
        }
    }
}
