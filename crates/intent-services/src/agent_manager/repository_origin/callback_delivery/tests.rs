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

struct NodePeer {
    child: tokio::process::Child,
    connection: Arc<Connection>,
    notes: Arc<TokioMutex<mpsc::UnboundedReceiver<IncomingNotification>>>,
    scratch: tempfile::TempDir,
}

impl NodePeer {
    fn new() -> Self {
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
            .arg(format!("--allow-fs-write={}", scratch.path().display()))
            .arg(script)
            .arg(adapter)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);
        let mut child = command.spawn().unwrap();
        let (tx, rx) = mpsc::unbounded_channel();
        let connection = Arc::new(Connection::new(
            child.stdin.take().unwrap(),
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
        let (id, delivery) = origin.accept_session(attempt, outcome);
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
        let (id, delivery) = origin.accept_session(attempt, outcome);
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
