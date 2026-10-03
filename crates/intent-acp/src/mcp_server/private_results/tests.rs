//! Real ACP/QuickJS effects under explicit injected policies. These fixtures
//! do NOT prove real repository admission, Store lifetime, provider eligibility,
//! `NativeRead`, or any production endpoint activation.

use super::*;
use crate::mcp_server::request_context::{McpRequestContext, McpRequestScope};
use crate::mcp_server::WorkspaceMcpServer;
use intent_core::{AgentId, Caller, TurnAttachmentRegistry, Workspace, WorkspaceApi, WorkspaceId};
use std::sync::atomic::{AtomicBool, AtomicUsize};
use tokio::sync::{Notify, Semaphore};

pub(crate) const WAIT: Duration = Duration::from_secs(5);
pub(crate) const SECRET: &str = "private-fixture-payload";
tokio::task_local! { static HOST: McpHostCall; }

pub(crate) struct Gate {
    pub(crate) entered: Notify,
    pub(crate) release: Semaphore,
}

impl Gate {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            entered: Notify::new(),
            release: Semaphore::new(0),
        })
    }
    async fn wait(&self) {
        self.entered.notify_one();
        self.release.acquire().await.unwrap().forget();
    }
    pub(crate) async fn reached(&self) {
        tokio::time::timeout(WAIT, self.entered.notified())
            .await
            .unwrap();
    }
}

struct Evidence {
    target: usize,
    live: Arc<AtomicBool>,
}

pub(crate) struct Policy {
    pub(crate) live: Arc<AtomicBool>,
    pub(crate) events: Mutex<Vec<(McpPrivateBoundaryKind, Vec<usize>)>>,
    pub(crate) hold: Mutex<Option<(McpPrivateBoundaryKind, bool, Arc<Gate>)>>,
    pub(crate) retire_after: Mutex<Option<McpPrivateBoundaryKind>>,
    panic: AtomicU8,
    captures: AtomicUsize,
    replay: AtomicBool,
    stolen: Mutex<Option<McpTransferReceipt>>,
}

impl Policy {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            live: Arc::new(AtomicBool::new(true)),
            events: Mutex::new(Vec::new()),
            hold: Mutex::new(None),
            retire_after: Mutex::new(None),
            panic: AtomicU8::new(0),
            captures: AtomicUsize::new(0),
            replay: AtomicBool::new(false),
            stolen: Mutex::new(None),
        })
    }
    pub(crate) fn pause(&self, kind: McpPrivateBoundaryKind, after: bool) -> Arc<Gate> {
        let gate = Gate::new();
        *self.hold.lock().unwrap() = Some((kind, after, gate.clone()));
        gate
    }
    pub(crate) fn retire(&self) {
        self.live.store(false, Ordering::SeqCst);
    }
}

struct HostScope(McpHostCall);
impl McpPrivateHostScope for HostScope {
    fn scope<'a>(&'a self, body: McpContextFuture<'a>) -> McpContextFuture<'a> {
        Box::pin(HOST.scope(self.0.clone(), body))
    }
}

impl McpPrivatePolicy for Policy {
    fn capture_host(&self, call: McpHostCall) -> Box<dyn McpPrivateHostScope> {
        self.captures.fetch_add(1, Ordering::SeqCst);
        assert_ne!(
            self.panic.load(Ordering::SeqCst),
            1,
            "fixture constructor panic"
        );
        Box::new(HostScope(call))
    }
    fn admit<'a>(
        &'a self,
        boundary: &'a McpPrivateBoundary,
        originals: &'a [McpReadEvidence],
        packet: PreparedMcpTransfer<'a>,
    ) -> BoxFuture<'a, McpPrivateAdmission> {
        Box::pin(async move {
            assert_eq!(
                intent_core::current_caller(),
                Some(Caller::Agent {
                    agent_id: "agent-1".into()
                })
            );
            assert!(!originals.is_empty());
            let targets = originals
                .iter()
                .map(|r| r.downcast_ref::<Evidence>().unwrap().target)
                .collect();
            self.events.lock().unwrap().push((boundary.kind(), targets));
            let hold = self
                .hold
                .lock()
                .unwrap()
                .clone()
                .filter(|(kind, _, _)| *kind == boundary.kind());
            if let Some((_, false, gate)) = &hold {
                gate.wait().await;
            }
            assert_ne!(
                self.panic.load(Ordering::SeqCst),
                2,
                "fixture admission panic"
            );
            if self.replay.load(Ordering::SeqCst) {
                if let Some(receipt) = self.stolen.lock().unwrap().take() {
                    return McpPrivateAdmission::Transferred(receipt);
                }
                if let McpPrivateAdmission::Transferred(receipt) = packet.transfer(boundary) {
                    *self.stolen.lock().unwrap() = Some(receipt);
                }
                return McpPrivateAdmission::Refused;
            }
            if !self.live.load(Ordering::SeqCst)
                || originals.iter().any(|r| {
                    !r.downcast_ref::<Evidence>()
                        .unwrap()
                        .live
                        .load(Ordering::SeqCst)
                })
            {
                return McpPrivateAdmission::Refused;
            }
            let result = packet.transfer(boundary);
            if *self.retire_after.lock().unwrap() == Some(boundary.kind()) {
                self.retire();
            }
            if let Some((_, true, gate)) = &hold {
                gate.wait().await;
            }
            result
        })
    }
}

struct RequestScope(Arc<Policy>);
pub(crate) struct RequestFactory(pub(crate) Arc<Policy>);
impl McpRequestContext for RequestFactory {
    fn capture(&self) -> Arc<dyn McpRequestScope> {
        Arc::new(RequestScope(self.0.clone()))
    }
}
impl McpRequestScope for RequestScope {
    fn private_result_policy(&self) -> Option<Arc<dyn McpPrivatePolicy>> {
        Some(self.0.clone())
    }
    fn scope<'a>(&'a self, body: McpContextFuture<'a>) -> McpContextFuture<'a> {
        body
    }
}

pub(crate) struct Api {
    pub(crate) acquired: AtomicUsize,
    pub(crate) ordinary: AtomicUsize,
    pub(crate) outcome: Mutex<Result<Value, String>>,
    pub(crate) reads: AtomicUsize,
    unbound: AtomicBool,
    pub(crate) settings_gate: Mutex<Option<Arc<Gate>>>,
    permission_gate: Mutex<Option<Arc<Gate>>>,
    host_gate: Mutex<Option<Arc<Gate>>>,
    all_host_gate: Mutex<Option<Arc<Gate>>>,
    panic_host: bool,
    mutations: Mutex<Vec<String>>,
    pub(crate) checkout: Option<String>,
    pub(crate) max_chars: usize,
    toon: bool,
    pub(crate) owners: Vec<Arc<AtomicBool>>,
}

impl Api {
    pub(crate) fn new() -> Self {
        Self {
            acquired: AtomicUsize::new(0),
            ordinary: AtomicUsize::new(0),
            outcome: Mutex::new(Ok(json!({"value":SECRET}))),
            reads: AtomicUsize::new(1),
            unbound: AtomicBool::new(false),
            settings_gate: Mutex::new(None),
            permission_gate: Mutex::new(None),
            host_gate: Mutex::new(None),
            all_host_gate: Mutex::new(None),
            panic_host: false,
            mutations: Mutex::new(Vec::new()),
            checkout: None,
            max_chars: 100_000,
            toon: false,
            owners: vec![
                Arc::new(AtomicBool::new(true)),
                Arc::new(AtomicBool::new(true)),
            ],
        }
    }
}

impl WorkspaceApi for Api {
    fn agent_is_retired(&self, _: AgentId) -> BoxFuture<'_, bool> {
        Box::pin(async {
            // This actual permission await is already inside the synchronously
            // captured original host scope, even before a read is qualified.
            HOST.try_with(|_| ()).unwrap();
            let gate = self.permission_gate.lock().unwrap().take();
            if let Some(gate) = gate {
                gate.wait().await;
            }
            false
        })
    }
    fn git_root_list(&self, _: WorkspaceId) -> BoxFuture<'_, intent_core::Result<Value>> {
        Box::pin(async move {
            for _ in 0..self.reads.load(Ordering::SeqCst) {
                let reservation = HOST.with(McpHostCall::reserve).map_err(|_| {
                    intent_core::Error::Internal("fixture reservation refused".into())
                })?;
                let number = self.acquired.fetch_add(1, Ordering::SeqCst);
                if self.unbound.load(Ordering::SeqCst) {
                    drop(reservation);
                } else {
                    reservation
                        .bind(Arc::new(Evidence {
                            target: number % 2,
                            live: self.owners[number % 2].clone(),
                        }))
                        .unwrap();
                }
                let gate = self.host_gate.lock().unwrap().clone();
                if number == 0 {
                    if let Some(gate) = gate {
                        gate.wait().await;
                    }
                }
                let all_gate = self.all_host_gate.lock().unwrap().clone();
                if let Some(gate) = all_gate {
                    gate.wait().await;
                }
            }
            assert!(!self.panic_host, "fixture host future panic");
            self.outcome
                .lock()
                .unwrap()
                .clone()
                .map(|value| json!({"gitRoots":value}))
                .map_err(intent_core::Error::Internal)
        })
    }
    fn settings_get(&self, path: String) -> BoxFuture<'_, intent_core::Result<Value>> {
        Box::pin(async move {
            self.ordinary.fetch_add(1, Ordering::SeqCst);
            let gate = self.settings_gate.lock().unwrap().take();
            if let Some(gate) = gate {
                gate.wait().await;
            }
            Ok(json!({"value":match path.as_str() {
                "workspaceApi.toonOutput" => json!(self.toon),
                "workspaceApi.maxOutputChars" => json!(self.max_chars),
                _ => json!("ordinary-completed"),
            }}))
        })
    }
    fn get_workspace(&self, id: WorkspaceId) -> BoxFuture<'_, intent_core::Result<Workspace>> {
        Box::pin(async move {
            self.ordinary.fetch_add(1, Ordering::SeqCst);
            Ok(serde_json::from_value(json!({
                "id":id,"title":"fixture","branch":"feature","status":"Active",
                "activity":"idle","attention":"none","createdAt":"2026-09-28",
                "updatedAt":"2026-09-28","tags":[],"skipWorktree":false,
                "isRemote":false,"archived":false,"worktreePath":self.checkout,
            }))
            .unwrap())
        })
    }

    fn update_workspace(
        &self,
        id: WorkspaceId,
        update: intent_core::WorkspaceUpdate,
    ) -> BoxFuture<'_, intent_core::Result<Workspace>> {
        Box::pin(async move {
            let message = update.status_message.expect("fixture status mutation");
            self.mutations.lock().unwrap().push(message.clone());
            let mut workspace = self.get_workspace(id).await?;
            workspace.status_message = Some(message);
            Ok(workspace)
        })
    }
}

pub(crate) fn server(api: Arc<Api>, policy: Arc<Policy>) -> WorkspaceMcpServer {
    WorkspaceMcpServer::new(api, "workspace-1".into())
        .with_caller_agent_id(Some("agent-1".into()))
        .with_request_context(Arc::new(RequestFactory(policy)))
}
pub(crate) fn call(id: u64, code: &str) -> Value {
    json!({"jsonrpc":"2.0","id":id,"method":"tools/call","params":{
        "name":"workspace_api","arguments":{"summary":"Private carrier fixture","code":code}
    }})
}
pub(crate) fn refused(value: &Value) {
    assert_eq!(value["result"], refusal_tool_result());
    assert!(!value.to_string().contains(SECRET));
}

#[tokio::test]
async fn host_retirement_refuses_success_and_private_error_without_retry() {
    for outcome in [Ok(json!({"ok":false,"error":SECRET})), Err(SECRET.into())] {
        let policy = Policy::new();
        let gate = policy.pause(McpPrivateBoundaryKind::HostPromise, false);
        let api = Arc::new(Api {
            outcome: Mutex::new(outcome),
            ..Api::new()
        });
        let server = server(api.clone(), policy.clone());
        let task = tokio::spawn(async move {
            server
                .handle_message(&call(
                    1,
                    "try { return await ws.git.listRoots(); } catch(e) { return e.message; }",
                ))
                .await
                .unwrap()
        });
        gate.reached().await;
        policy.retire();
        gate.release.add_permits(1);
        refused(&task.await.unwrap());
        assert_eq!(api.acquired.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn admitted_host_then_retirement_cannot_authorize_final_output() {
    let policy = Policy::new();
    *policy.retire_after.lock().unwrap() = Some(McpPrivateBoundaryKind::HostPromise);
    let api = Arc::new(Api::new());
    refused(
        &server(api.clone(), policy)
            .handle_message(&call(
                1,
                "await ws.git.listRoots(); await ws.workspace.info(); return 'constant';",
            ))
            .await
            .unwrap(),
    );
    assert!(
        api.ordinary.load(Ordering::SeqCst) > 0,
        "completed ordinary effects are factual"
    );
    assert_eq!(api.acquired.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn transferred_host_waits_for_guard_release_before_js_continues() {
    let policy = Policy::new();
    let gate = policy.pause(McpPrivateBoundaryKind::HostPromise, true);
    let api = Arc::new(Api::new());
    let server = server(api.clone(), policy);
    let task = tokio::spawn(async move {
        server
            .handle_message(&call(
                1,
                "await ws.git.listRoots(); return await ws.workspace.info();",
            ))
            .await
            .unwrap()
    });
    gate.reached().await;
    assert_eq!(api.ordinary.load(Ordering::SeqCst), 0);
    gate.release.add_permits(1);
    assert_eq!(task.await.unwrap()["result"]["isError"], false);
}

#[tokio::test]
async fn permission_pause_keeps_original_policy_when_public_ids_are_reused() {
    let old = Policy::new();
    let gate = Gate::new();
    let api = Arc::new(Api {
        permission_gate: Mutex::new(Some(gate.clone())),
        ..Api::new()
    });
    let original = server(api.clone(), old.clone());
    let pending = tokio::spawn(async move {
        original
            .handle_message(&call(1, "return await ws.git.listRoots();"))
            .await
            .unwrap()
    });
    gate.reached().await;
    assert_eq!(old.captures.load(Ordering::SeqCst), 1);
    assert_eq!(api.acquired.load(Ordering::SeqCst), 0);
    old.retire();
    let replacement = Policy::new();
    let next = server(api.clone(), replacement.clone())
        .handle_message(&call(1, "return await ws.git.listRoots();"))
        .await
        .unwrap();
    assert!(next.to_string().contains(SECRET));
    gate.release.add_permits(1);
    refused(&pending.await.unwrap());
    assert_eq!(old.captures.load(Ordering::SeqCst), 1);
    assert_eq!(replacement.captures.load(Ordering::SeqCst), 1);
    assert_eq!(api.acquired.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn output_knobs_preserve_original_evidence_and_admitted_rendering() {
    for toon in [false, true] {
        for max_chars in [0, 1, 100_000] {
            let mut rendered = Vec::new();
            for reads in [0, 1] {
                let dir = tempfile::tempdir().unwrap();
                let policy = Policy::new();
                let api = Arc::new(Api {
                    reads: AtomicUsize::new(reads),
                    checkout: Some(dir.path().join("checkout").to_string_lossy().into_owned()),
                    max_chars,
                    toon,
                    ..Api::new()
                });
                let value = server(api, policy.clone())
                    .handle_message(&call(1, "return await ws.git.listRoots();"))
                    .await
                    .unwrap();
                assert_eq!(value["result"]["isError"], false);
                if max_chars == 1 {
                    let files = std::fs::read_dir(dir.path().join("tool-outputs"))
                        .unwrap()
                        .collect::<Result<Vec<_>, _>>()
                        .unwrap();
                    assert_eq!(files.len(), 1);
                    rendered.push(std::fs::read(files[0].path()).unwrap());
                } else {
                    rendered.push(serde_json::to_vec(&value).unwrap());
                    assert!(!dir.path().join("tool-outputs").exists());
                }
                let events = policy.events.lock().unwrap();
                if reads == 0 {
                    assert!(events.is_empty());
                } else {
                    assert_eq!(
                        events.last().unwrap().0,
                        McpPrivateBoundaryKind::DirectResponse
                    );
                    assert!(events.iter().all(|(_, witnesses)| witnesses.len() == 1));
                }
            }
            assert_eq!(
                rendered[0], rendered[1],
                "toon={toon}, max_chars={max_chars}"
            );
        }
    }
}

#[tokio::test]
async fn completed_ordinary_mutation_survives_private_refusal_and_delivery_timeout() {
    for expiry in [false, true] {
        let policy = Policy::new();
        let gate = policy.pause(McpPrivateBoundaryKind::DirectResponse, false);
        let api = Arc::new(Api::new());
        let original = server(api.clone(), policy.clone());
        let pending = tokio::spawn(async move {
            original.handle_message(&call(1,
                "await ws.git.listRoots(); return await ws.workspace.setStatusMessage('committed once');"
            )).await.unwrap()
        });
        gate.reached().await;
        assert_eq!(*api.mutations.lock().unwrap(), vec!["committed once"]);
        if expiry {
            tokio::time::pause();
            tokio::time::advance(Duration::from_secs(121)).await;
        } else {
            policy.retire();
            gate.release.add_permits(1);
        }
        let result = pending.await.unwrap();
        if expiry {
            tokio::time::resume();
        }
        refused(&result);
        assert!(!result.to_string().contains("timed out"));
        assert_eq!(*api.mutations.lock().unwrap(), vec!["committed once"]);
        assert_eq!(api.acquired.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn parallel_equal_arguments_and_subreads_retain_every_original_owner() {
    let policy = Policy::new();
    let gate = policy.pause(McpPrivateBoundaryKind::DirectResponse, false);
    let api = Arc::new(Api {
        reads: AtomicUsize::new(2),
        ..Api::new()
    });
    let server = server(api.clone(), policy.clone());
    let task = tokio::spawn(async move {
        server.handle_message(&call(1,"await Promise.all([ws.git.listRoots(),ws.git.listRoots(),ws.git.listRoots()]); return 7;")).await.unwrap()
    });
    gate.reached().await;
    let events = policy.events.lock().unwrap().clone();
    assert_eq!(events.last().unwrap().1.len(), 6);
    assert_eq!(
        events
            .iter()
            .filter(|(kind, _)| *kind == McpPrivateBoundaryKind::HostPromise)
            .count(),
        3
    );
    api.owners[0].store(false, Ordering::SeqCst);
    gate.release.add_permits(1);
    refused(&task.await.unwrap());
}

#[tokio::test]
async fn discarded_caught_constant_string_error_and_content_item_shapes_stay_guarded() {
    for suffix in [
        "return;",
        "return null;",
        "return 9;",
        "return 'constant';",
        "throw new Error('transformed');",
        "return {__mcpContentItems:[{type:'text',text:'constant'}]};",
    ] {
        let policy = Policy::new();
        *policy.retire_after.lock().unwrap() = Some(McpPrivateBoundaryKind::HostPromise);
        let api = Arc::new(Api {
            outcome: Mutex::new(Err(SECRET.into())),
            ..Api::new()
        });
        let code = format!("try {{ await ws.git.listRoots(); }} catch (_) {{}} {suffix}");
        refused(
            &server(api, policy)
                .handle_message(&call(1, &code))
                .await
                .unwrap(),
        );
    }
}

#[tokio::test]
async fn reservation_cap_precedes_acquisition_and_never_counts_ordinary_calls() {
    for (count, success) in [(64, true), (65, false)] {
        let policy = Policy::new();
        let api = Arc::new(Api::new());
        let code = format!(
            "for(let i=0;i<70;i++) await ws.workspace.info(); for(let i=0;i<{count};i++) {{ try {{ await ws.git.listRoots(); }} catch (_) {{}} }} return 'done';"
        );
        let value = server(api.clone(), policy.clone())
            .handle_message(&call(1, &code))
            .await
            .unwrap();
        assert_eq!(api.acquired.load(Ordering::SeqCst), 64);
        if success {
            assert_eq!(value["result"]["isError"], false);
            assert_eq!(policy.events.lock().unwrap().last().unwrap().1.len(), 64);
        } else {
            refused(&value);
        }
        assert!(api.ordinary.load(Ordering::SeqCst) >= 70);
    }
}

#[tokio::test]
async fn unfinished_reservations_count_towards_the_bound_before_any_host_finishes() {
    let policy = Policy::new();
    let gate = Gate::new();
    let api = Arc::new(Api {
        all_host_gate: Mutex::new(Some(gate.clone())),
        ..Api::new()
    });
    let server = server(api.clone(), policy);
    let task = tokio::spawn(async move {
        server.handle_message(&call(1,
        "await Promise.allSettled(Array.from({length:65},()=>ws.git.listRoots())); return 'caught';")).await.unwrap()
    });
    while api.acquired.load(Ordering::SeqCst) < 64 {
        gate.reached().await;
    }
    assert!(!task.is_finished());
    gate.release.add_permits(64);
    refused(&task.await.unwrap());
    assert_eq!(api.acquired.load(Ordering::SeqCst), 64);
}

#[tokio::test]
async fn admitted_success_private_error_and_quota_shaped_outcomes_keep_ordinary_shapes() {
    for outcome in [
        Ok(json!({"ok":false,"error":SECRET,"status":429})),
        Err(format!("{SECRET}: 429 retry-after=30")),
    ] {
        let mut responses = Vec::new();
        for reads in [0, 1] {
            let policy = Policy::new();
            let api = Arc::new(Api {
                reads: AtomicUsize::new(reads),
                outcome: Mutex::new(outcome.clone()),
                ..Api::new()
            });
            responses.push(
                server(api.clone(), policy.clone())
                    .handle_message(&call(1, "return await ws.git.listRoots();"))
                    .await
                    .unwrap(),
            );
            assert_eq!(api.acquired.load(Ordering::SeqCst), reads);
            assert_eq!(policy.events.lock().unwrap().len(), reads * 2);
        }
        assert_eq!(responses[0], responses[1]);
        assert!(responses[1].to_string().contains(SECRET));
    }
}

#[tokio::test]
async fn absent_policy_preserves_ordinary_attachment_registration_and_file_output() {
    let dir = tempfile::tempdir().unwrap();
    let api = Arc::new(Api {
        checkout: Some(dir.path().join("checkout").to_string_lossy().into_owned()),
        max_chars: 1,
        ..Api::new()
    });
    let registry = Arc::new(TurnAttachmentRegistry::new());
    let server = WorkspaceMcpServer::new(api.clone(), "workspace-1".into())
        .with_caller_agent_id(Some("agent-1".into()))
        .with_turn_attachments(Some(registry.clone()));
    let result=server.handle_message(&call(1,"return {__mcpContentItems:[{type:'resource',resource:{uri:'fixture://ordinary',mimeType:'application/json',text:'{\"ordinary\":true}'}}]};")).await.unwrap();
    assert_eq!(result["result"]["isError"], false);
    assert_eq!(
        registry.pending_count_by_mime(&"agent-1".into(), "application/json"),
        1
    );
    let result = server
        .handle_message(&call(2, "return 'ordinary output';"))
        .await
        .unwrap();
    assert!(result.to_string().contains("The full output was written"));
    assert_eq!(
        std::fs::read_dir(dir.path().join("tool-outputs"))
            .unwrap()
            .count(),
        1
    );
    assert_eq!(api.acquired.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn discarded_binding_attachment_still_requires_the_complete_original_set() {
    let policy = Policy::new();
    *policy.retire_after.lock().unwrap() = Some(McpPrivateBoundaryKind::HostPromise);
    let registry = Arc::new(TurnAttachmentRegistry::new());
    let api = Arc::new(Api {
        outcome: Mutex::new(Ok(
            json!({"__mcpContentItems":[{"type":"resource","resource":{
        "uri":"fixture://original","mimeType":"application/json","text":"{\"value\":\"private-fixture-payload\"}"}}]}),
        )),
        ..Api::new()
    });
    let server = server(api, policy).with_turn_attachments(Some(registry.clone()));
    refused(
        &server
            .handle_message(&call(1, "await ws.git.listRoots(); return 'discarded';"))
            .await
            .unwrap(),
    );
    assert_eq!(
        registry.pending_count_by_mime(&"agent-1".into(), "application/json"),
        0
    );
}

#[tokio::test]
async fn unfinished_or_unbound_qualified_reads_cannot_seal() {
    let policy = Policy::new();
    let api = Arc::new(Api {
        unbound: AtomicBool::new(true),
        ..Api::new()
    });
    refused(
        &server(api, policy)
            .handle_message(&call(
                1,
                "try { await ws.git.listRoots(); } catch (_) {} return 'constant';",
            ))
            .await
            .unwrap(),
    );
    let policy = Policy::new();
    let api = Arc::new(Api::new());
    *api.host_gate.lock().unwrap() = Some(Gate::new());
    let value = server(api.clone(), policy)
        .with_workspace_api_timeout(Duration::from_millis(30))
        .handle_message(&call(
            1,
            "await Promise.race([ws.git.listRoots(),ws.git.listRoots()]); return 'constant';",
        ))
        .await
        .unwrap();
    refused(&value);
    assert_eq!(api.acquired.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn settings_pause_retirement_prevents_spill_and_private_projection() {
    let dir = tempfile::tempdir().unwrap();
    let gate = Gate::new();
    let policy = Policy::new();
    let api = Arc::new(Api {
        checkout: Some(dir.path().join("checkout").to_string_lossy().into_owned()),
        max_chars: 1,
        settings_gate: Mutex::new(Some(gate.clone())),
        ..Api::new()
    });
    let server = server(api, policy.clone());
    let task = tokio::spawn(async move {
        server
            .handle_message(&call(1, "return await ws.git.listRoots();"))
            .await
            .unwrap()
    });
    gate.reached().await;
    policy.retire();
    gate.release.add_permits(1);
    refused(&task.await.unwrap());
    assert!(!dir.path().join("tool-outputs").exists());
}

#[tokio::test]
async fn artifact_start_orders_are_distinct_from_response_admission() {
    for admitted in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let policy = Policy::new();
        let gate = policy.pause(McpPrivateBoundaryKind::ArtifactStart, admitted);
        let api = Arc::new(Api {
            checkout: Some(dir.path().join("checkout").to_string_lossy().into_owned()),
            max_chars: 1,
            ..Api::new()
        });
        let server = server(api, policy.clone());
        let task = tokio::spawn(async move {
            server
                .handle_message(&call(1, "return await ws.git.listRoots();"))
                .await
                .unwrap()
        });
        gate.reached().await;
        assert!(
            !dir.path().join("tool-outputs").exists(),
            "no I/O while admission holds its guards"
        );
        policy.retire();
        gate.release.add_permits(1);
        refused(&task.await.unwrap());
        let folder = dir.path().join("tool-outputs");
        assert_eq!(folder.exists(), admitted);
        if admitted {
            let entries = std::fs::read_dir(folder)
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert_eq!(entries.len(), 1);
            assert!(std::fs::read_to_string(entries[0].path())
                .unwrap()
                .contains(SECRET));
        }
    }
}

#[tokio::test]
async fn cancelling_before_or_after_start_keeps_distinct_original_job_accounting() {
    for admitted in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let policy = Policy::new();
        let gate = policy.pause(McpPrivateBoundaryKind::ArtifactStart, admitted);
        let api = Arc::new(Api {
            checkout: Some(dir.path().join("checkout").to_string_lossy().into_owned()),
            max_chars: 1,
            ..Api::new()
        });
        let server = server(api, policy);
        let context = server.capture_request_context();
        let accounting = context
            .private_invocation
            .as_ref()
            .unwrap()
            .0
            .artifact
            .clone();
        let task = tokio::spawn(async move {
            server
                .handle_message_for_delivery(&call(1, "return await ws.git.listRoots();"), context)
                .await
        });
        gate.reached().await;
        task.abort();
        assert!(matches!(task.await,Err(error) if error.is_cancelled()));
        assert_eq!(
            accounting.outcome(),
            if admitted {
                ArtifactOutcome::Unknown
            } else {
                ArtifactOutcome::NotStarted
            }
        );
        assert!(!dir.path().join("tool-outputs").exists());
        gate.release.add_permits(1);
    }
}

#[test]
fn cancelled_filesystem_work_can_have_a_late_effect_without_a_replacement_or_retry() {
    let dir = tempfile::tempdir().unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap();
    runtime.block_on(async {
        // Hold the one blocking worker so the actual Tokio create_dir_all job
        // is queued but cannot finish before cancellation of its original await.
        let (release, held) = std::sync::mpsc::channel::<()>();
        let (started, ready) = oneshot::channel();
        let worker = tokio::task::spawn_blocking(move || {
            started.send(()).unwrap();
            let _ = held.recv();
        });
        ready.await.unwrap();
        let api = Arc::new(Api {
            checkout: Some(dir.path().join("checkout").to_string_lossy().into_owned()),
            max_chars: 1,
            ..Api::new()
        });
        let server = server(api.clone(), Policy::new());
        let context = server.capture_request_context();
        let accounting = context
            .private_invocation
            .as_ref()
            .unwrap()
            .0
            .artifact
            .clone();
        let task = tokio::spawn(async move {
            server
                .handle_message_for_delivery(&call(1, "return await ws.git.listRoots();"), context)
                .await
        });
        tokio::time::timeout(WAIT, async {
            while accounting.outcome() != ArtifactOutcome::Unknown {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(!dir.path().join("tool-outputs").exists());
        task.abort();
        assert!(matches!(task.await,Err(error) if error.is_cancelled()));
        release.send(()).unwrap();
        worker.await.unwrap();
        let folder = dir.path().join("tool-outputs");
        tokio::time::timeout(WAIT, async {
            while !folder.exists() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(std::fs::read_dir(folder).unwrap().count(), 0);
        assert_eq!(accounting.outcome(), ArtifactOutcome::Unknown);
        assert_eq!(api.acquired.load(Ordering::SeqCst), 1);
    });
}

#[tokio::test]
async fn failed_artifact_keeps_actual_failure_but_projection_needs_admission() {
    for retired in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("tool-outputs"), b"successor-owned").unwrap();
        let policy = Policy::new();
        if retired {
            *policy.retire_after.lock().unwrap() = Some(McpPrivateBoundaryKind::ArtifactStart);
        }
        let api = Arc::new(Api {
            checkout: Some(dir.path().join("checkout").to_string_lossy().into_owned()),
            max_chars: 1,
            ..Api::new()
        });
        let value = server(api, policy)
            .handle_message(&call(1, "return await ws.git.listRoots();"))
            .await
            .unwrap();
        if retired {
            refused(&value);
        } else {
            assert!(value.to_string().contains("could NOT be written"));
        }
        assert_eq!(
            std::fs::read(dir.path().join("tool-outputs")).unwrap(),
            b"successor-owned"
        );
    }
}

#[tokio::test]
async fn arbitrary_attachment_batch_is_private_until_its_own_admission() {
    for admitted in [false, true] {
        let policy = Policy::new();
        let gate = policy.pause(McpPrivateBoundaryKind::Attachments, admitted);
        let registry = Arc::new(TurnAttachmentRegistry::new());
        let server = server(Arc::new(Api::new()), policy.clone())
            .with_turn_attachments(Some(registry.clone()));
        let task = tokio::spawn(async move {
            server.handle_message(&call(1,"await ws.git.listRoots(); return {__mcpContentItems:[{type:'resource',resource:{uri:'fixture://original',mimeType:'application/json',text:'{\"value\":\"private-fixture-payload\"}'}},{type:'text',text:'arbitrary'}]};")).await.unwrap()
        });
        gate.reached().await;
        assert_eq!(
            registry.pending_count_by_mime(&"agent-1".into(), "application/json"),
            0
        );
        policy.retire();
        gate.release.add_permits(1);
        refused(&task.await.unwrap());
        assert_eq!(
            registry.pending_count_by_mime(&"agent-1".into(), "application/json"),
            usize::from(admitted)
        );
    }
}

#[tokio::test]
async fn foreign_receipt_from_another_invocation_cannot_authorize_payload() {
    let policy = Policy::new();
    policy.replay.store(true, Ordering::SeqCst);
    let api = Arc::new(Api::new());
    let server = server(api.clone(), policy);
    for id in 1..=2 {
        refused(
            &server
                .handle_message(&call(id, "return await ws.git.listRoots();"))
                .await
                .unwrap(),
        );
    }
    assert_eq!(api.acquired.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn constructor_and_admission_panics_emit_only_fixed_control_errors() {
    for panic in [1, 2, 3] {
        let policy = Policy::new();
        policy.panic.store(panic, Ordering::SeqCst);
        let api = Arc::new(Api {
            panic_host: panic == 3,
            ..Api::new()
        });
        refused(
            &server(api.clone(), policy)
                .handle_message(&call(1, "return await ws.git.listRoots();"))
                .await
                .unwrap(),
        );
        assert_eq!(api.acquired.load(Ordering::SeqCst), usize::from(panic != 1));
    }
}

#[tokio::test]
async fn cancelled_and_unpolled_calls_never_retry_or_detach() {
    let policy = Policy::new();
    let api = Arc::new(Api::new());
    let server = Arc::new(server(api.clone(), policy.clone()));
    let message = call(1, "return await ws.git.listRoots();");
    drop(server.handle_message(&message));
    assert_eq!(policy.captures.load(Ordering::SeqCst), 0);
    let gate = policy.pause(McpPrivateBoundaryKind::HostPromise, false);
    let task = tokio::spawn(async move { server.handle_message(&message).await });
    gate.reached().await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    gate.release.add_permits(1);
    assert_eq!(api.acquired.load(Ordering::SeqCst), 1);
    assert_eq!(policy.events.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn direct_result_admitted_before_retirement_is_not_recalled() {
    let policy = Policy::new();
    *policy.retire_after.lock().unwrap() = Some(McpPrivateBoundaryKind::DirectResponse);
    let value = server(Arc::new(Api::new()), policy)
        .handle_message(&call(1, "return await ws.git.listRoots();"))
        .await
        .unwrap();
    assert!(value.to_string().contains(SECRET));
}

#[tokio::test]
async fn expired_delivery_budget_is_not_an_operation_timeout() {
    let policy = Policy::new();
    let api = Arc::new(Api::new());
    let server = server(api.clone(), policy.clone());
    let scope = RequestFactory(policy.clone());
    let context = CapturedRequestContext::capture_with_budget(
        Some(Caller::Agent {
            agent_id: "agent-1".into(),
        }),
        Some(&scope),
        Duration::from_secs(120),
    );
    let response = server
        .handle_message_for_delivery(&call(1, "return await ws.git.listRoots();"), context)
        .await
        .unwrap();
    let gate = policy.pause(McpPrivateBoundaryKind::DirectResponse, false);
    let task = tokio::spawn(response.into_direct());
    gate.reached().await;
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(121)).await;
    let result = task.await.unwrap();
    tokio::time::resume();
    refused(&result.value);
    assert!(!result.value.to_string().contains("timed out"));
    assert_eq!(api.acquired.load(Ordering::SeqCst), 1);
    gate.release.add_permits(1);
}

/// Explicit neutral policy fixtures. These locks/identities do not implement
/// the real R shared-parent planner, Store root lifetimes or P comparison.
pub(crate) mod optional {
    use super::*;
    use crate::mcp_server::repository_guidance::{
        tests::{revision, scope, session},
        ConnectionLifetime, GuidanceCandidate, GuidanceLease, GuidancePreparation,
        RepositoryGuidanceFence, RepositoryGuidanceSource,
    };

    pub(crate) const TEXT: &str = "qualified optional fixture guidance";
    pub(crate) const READ: &str = "return await ws.git.listRoots();";
    tokio::task_local! { static OPTIONAL: Arc<()>; }

    pub(crate) struct State {
        pub(crate) required: Arc<Policy>,
        original: Arc<()>,
        pub(crate) capture: AtomicUsize,
        pub(crate) qualifications: AtomicUsize,
        pub(crate) qualified: AtomicBool,
        pub(crate) constructor: AtomicUsize,
        pub(crate) polls: AtomicUsize,
        pub(crate) leaves_dropped: AtomicUsize,
        pub(crate) leaf_dropped: Notify,
        pub(crate) scope_runs: AtomicUsize,
        pub(crate) optional_calls: AtomicUsize,
        pub(crate) final_records: Mutex<Vec<Vec<usize>>>,
        pub(crate) live: AtomicBool,
        pub(crate) capture_enabled: AtomicBool,
        pub(crate) mode: AtomicU8,
        pub(crate) admission_mode: AtomicU8,
        pub(crate) gate: Mutex<Option<Arc<Gate>>>,
        pub(crate) admission_gate: Mutex<Option<(bool, Arc<Gate>)>>,
        pub(crate) lease: Mutex<Option<GuidanceLease>>,
        captured_invocation: Mutex<Option<std::sync::Weak<Invocation>>>,
        stolen: Mutex<Option<McpTransferReceipt>>,
        drop_count: Arc<AtomicUsize>,
        in_transfer: Arc<AtomicBool>,
    }

    impl State {
        pub(crate) fn new() -> Arc<Self> {
            Arc::new(Self {
                required: Policy::new(),
                original: Arc::new(()),
                capture: AtomicUsize::new(0),
                qualifications: AtomicUsize::new(0),
                qualified: AtomicBool::new(true),
                constructor: AtomicUsize::new(0),
                polls: AtomicUsize::new(0),
                leaves_dropped: AtomicUsize::new(0),
                leaf_dropped: Notify::new(),
                scope_runs: AtomicUsize::new(0),
                optional_calls: AtomicUsize::new(0),
                final_records: Mutex::new(Vec::new()),
                live: AtomicBool::new(true),
                capture_enabled: AtomicBool::new(true),
                mode: AtomicU8::new(0),
                admission_mode: AtomicU8::new(0),
                gate: Mutex::new(None),
                admission_gate: Mutex::new(None),
                lease: Mutex::new(None),
                captured_invocation: Mutex::new(None),
                stolen: Mutex::new(None),
                drop_count: Arc::new(AtomicUsize::new(0)),
                in_transfer: Arc::new(AtomicBool::new(false)),
            })
        }
        pub(crate) fn pause_source(&self) -> Arc<Gate> {
            let gate = Gate::new();
            *self.gate.lock().unwrap() = Some(gate.clone());
            gate
        }
        pub(crate) fn pause_admission(&self, after: bool) -> Arc<Gate> {
            let gate = Gate::new();
            *self.admission_gate.lock().unwrap() = Some((after, gate.clone()));
            gate
        }
    }

    struct OptionalScope(Arc<State>);
    impl Drop for OptionalScope {
        fn drop(&mut self) {
            self.0.leaves_dropped.fetch_add(1, Ordering::SeqCst);
            self.0.leaf_dropped.notify_one();
        }
    }
    impl McpOptionalContextScope for OptionalScope {
        fn scope<'a>(&'a self, body: McpContextFuture<'a>) -> McpContextFuture<'a> {
            Box::pin(OPTIONAL.scope(self.0.original.clone(), body))
        }
    }
    struct OriginalEvidence {
        original: Arc<()>,
        invocation: McpPrivateInvocation,
        drop_count: Arc<AtomicUsize>,
        in_transfer: Arc<AtomicBool>,
    }
    impl Drop for OriginalEvidence {
        fn drop(&mut self) {
            assert!(
                !self.in_transfer.load(Ordering::SeqCst),
                "evidence dropped inside action"
            );
            self.drop_count.fetch_add(1, Ordering::SeqCst);
        }
    }
    pub(crate) struct Source(pub(crate) Arc<State>);
    impl RepositoryGuidanceSource for Source {
        fn preparation(&self) -> GuidancePreparation {
            self.0.qualifications.fetch_add(1, Ordering::SeqCst);
            if self.0.qualified.load(Ordering::SeqCst) {
                GuidancePreparation::Qualified
            } else {
                GuidancePreparation::CorrelationOnly
            }
        }
        fn prepare<'a>(
            &'a self,
            workspace: &'a WorkspaceId,
            caller: &'a Caller,
            fence: &'a RepositoryGuidanceFence,
        ) -> BoxFuture<'a, Option<GuidanceCandidate>> {
            self.0.constructor.fetch_add(1, Ordering::SeqCst);
            assert!(self.0.capture.load(Ordering::SeqCst) > 0);
            assert_eq!(intent_core::current_caller().as_ref(), Some(caller));
            assert_eq!(workspace.as_str(), "workspace-1");
            OPTIONAL.with(|id| assert!(Arc::ptr_eq(id, &self.0.original)));
            let invocation = McpPrivateInvocation::current().unwrap();
            assert!(Arc::ptr_eq(
                &invocation.0,
                &self
                    .0
                    .captured_invocation
                    .lock()
                    .unwrap()
                    .as_ref()
                    .unwrap()
                    .upgrade()
                    .unwrap()
            ));
            assert_ne!(
                self.0.mode.load(Ordering::SeqCst),
                1,
                "optional constructor panic"
            );
            Box::pin(async move {
                self.0.polls.fetch_add(1, Ordering::SeqCst);
                assert_ne!(
                    self.0.mode.load(Ordering::SeqCst),
                    2,
                    "optional future panic"
                );
                let gate = self.0.gate.lock().unwrap().clone();
                if let Some(gate) = gate {
                    gate.wait().await;
                }
                let mode = self.0.mode.load(Ordering::SeqCst);
                if mode == 7 {
                    let late = McpHostCall {
                        invocation: invocation.0.clone(),
                        status: Arc::new(AtomicU8::new(0)),
                    };
                    assert!(matches!(late.reserve(), Err(PrivateReadRefusal::Closed)));
                }
                if mode == 3 {
                    return None;
                }
                let lease = fence.replace_context(scope(), revision(1)).unwrap();
                *self.0.lease.lock().unwrap() = Some(lease.clone());
                let candidate = lease
                    .current_candidate(scope(), revision(1), TEXT.into())
                    .unwrap();
                if mode == 4 {
                    return Some(candidate);
                }
                Some(
                    candidate.with_optional_evidence(McpOptionalEvidence::new(OriginalEvidence {
                        original: if mode == 5 {
                            Arc::new(())
                        } else {
                            self.0.original.clone()
                        },
                        invocation,
                        drop_count: self.0.drop_count.clone(),
                        in_transfer: self.0.in_transfer.clone(),
                    })),
                )
            })
        }
    }

    /// `DefaultMode` tests the actual trait default, not a copied implementation.
    struct DefaultMode(Arc<State>);
    pub(crate) struct Joint(pub(crate) Arc<State>);
    fn capture(state: &Arc<State>) -> Option<Box<dyn McpOptionalContextScope>> {
        assert_eq!(
            intent_core::current_caller(),
            Some(Caller::Agent {
                agent_id: "agent-1".into()
            })
        );
        state.capture.fetch_add(1, Ordering::SeqCst);
        *state.captured_invocation.lock().unwrap() =
            Some(Arc::downgrade(&McpPrivateInvocation::current().unwrap().0));
        assert_ne!(
            state.mode.load(Ordering::SeqCst),
            6,
            "optional capture panic"
        );
        state
            .capture_enabled
            .load(Ordering::SeqCst)
            .then(|| Box::new(OptionalScope(state.clone())) as Box<dyn McpOptionalContextScope>)
    }
    impl McpPrivatePolicy for DefaultMode {
        fn capture_host(&self, call: McpHostCall) -> Box<dyn McpPrivateHostScope> {
            self.0.required.capture_host(call)
        }
        fn capture_optional_context(&self) -> Option<Box<dyn McpOptionalContextScope>> {
            capture(&self.0)
        }
        fn admit<'a>(
            &'a self,
            boundary: &'a McpPrivateBoundary,
            originals: &'a [McpReadEvidence],
            packet: PreparedMcpTransfer<'a>,
        ) -> BoxFuture<'a, McpPrivateAdmission> {
            self.0.required.admit(boundary, originals, packet)
        }
    }
    impl McpPrivatePolicy for Joint {
        fn capture_host(&self, call: McpHostCall) -> Box<dyn McpPrivateHostScope> {
            self.0.required.capture_host(call)
        }
        fn capture_optional_context(&self) -> Option<Box<dyn McpOptionalContextScope>> {
            capture(&self.0)
        }
        fn admit<'a>(
            &'a self,
            boundary: &'a McpPrivateBoundary,
            originals: &'a [McpReadEvidence],
            packet: PreparedMcpTransfer<'a>,
        ) -> BoxFuture<'a, McpPrivateAdmission> {
            self.0.required.admit(boundary, originals, packet)
        }
        // Only this neutral fixture inspects ACP's private original anchor.
        #[expect(clippy::used_underscore_binding)]
        fn admit_optional<'a>(
            &'a self,
            boundary: &'a McpPrivateBoundary,
            sealed: McpSealedReads<'a>,
            optional: &'a McpOptionalEvidence,
            packet: PreparedMcpVariants<'a>,
        ) -> BoxFuture<'a, McpPrivateAdmission> {
            Box::pin(async move {
                assert_eq!(
                    intent_core::current_caller(),
                    Some(Caller::Agent {
                        agent_id: "agent-1".into()
                    })
                );
                assert_eq!(boundary.kind(), McpPrivateBoundaryKind::TcpResponse);
                self.0.optional_calls.fetch_add(1, Ordering::SeqCst);
                self.0.final_records.lock().unwrap().push(
                    sealed
                        .records()
                        .iter()
                        .map(|r| r.downcast_ref::<Evidence>().unwrap().target)
                        .collect(),
                );
                let hold = self.0.admission_gate.lock().unwrap().clone();
                if let Some((false, gate)) = &hold {
                    gate.wait().await;
                }
                let mode = self.0.admission_mode.load(Ordering::SeqCst);
                assert_ne!(mode, 1, "combined admission panic before consumption");
                if mode == 2 {
                    return McpPrivateAdmission::Refused;
                }
                if mode == 3 {
                    return packet.transfer(
                        &super::super::boundary(boundary.kind()),
                        McpGuidanceChoice::WithGuidance,
                    );
                }
                if mode == 4 {
                    if let Some(receipt) = self.0.stolen.lock().unwrap().take() {
                        return McpPrivateAdmission::Transferred(receipt);
                    }
                }
                if !sealed.is_empty()
                    && (!self.0.required.live.load(Ordering::SeqCst)
                        || sealed.records().iter().any(|r| {
                            !r.downcast_ref::<Evidence>()
                                .unwrap()
                                .live
                                .load(Ordering::SeqCst)
                        }))
                {
                    return McpPrivateAdmission::Refused;
                }
                let original = optional.downcast_ref::<OriginalEvidence>().unwrap();
                let choice = if self.0.live.load(Ordering::SeqCst)
                    && self.0.required.live.load(Ordering::SeqCst)
                    && Arc::ptr_eq(&original.original, &self.0.original)
                    && std::ptr::eq(sealed._original, original.invocation.0.as_ref())
                {
                    McpGuidanceChoice::WithGuidance
                } else {
                    McpGuidanceChoice::WithoutGuidance
                };
                self.0.in_transfer.store(true, Ordering::SeqCst);
                let result = packet.transfer(boundary, choice);
                self.0.in_transfer.store(false, Ordering::SeqCst);
                assert_ne!(mode, 5, "combined admission panic after consumption");
                if mode == 4 {
                    if let McpPrivateAdmission::Transferred(receipt) = result {
                        *self.0.stolen.lock().unwrap() = Some(receipt);
                    }
                    return McpPrivateAdmission::Refused;
                }
                if let Some((true, gate)) = &hold {
                    gate.wait().await;
                }
                result
            })
        }
    }
    struct Factory {
        state: Arc<State>,
        policy: Arc<dyn McpPrivatePolicy>,
    }
    impl McpRequestContext for Factory {
        fn capture(&self) -> Arc<dyn McpRequestScope> {
            Arc::new(RequiredScope {
                state: self.state.clone(),
                policy: self.policy.clone(),
            })
        }
    }
    struct RequiredScope {
        state: Arc<State>,
        policy: Arc<dyn McpPrivatePolicy>,
    }
    struct CancelRequired {
        live: Arc<AtomicBool>,
        completed: bool,
    }
    impl Drop for CancelRequired {
        fn drop(&mut self) {
            if !self.completed {
                self.live.store(false, Ordering::SeqCst);
            }
        }
    }
    impl McpRequestScope for RequiredScope {
        fn private_result_policy(&self) -> Option<Arc<dyn McpPrivatePolicy>> {
            Some(self.policy.clone())
        }
        fn scope<'a>(&'a self, body: McpContextFuture<'a>) -> McpContextFuture<'a> {
            Box::pin(async move {
                self.state.scope_runs.fetch_add(1, Ordering::SeqCst);
                let mut guard = CancelRequired {
                    live: self.state.required.live.clone(),
                    completed: false,
                };
                body.await;
                guard.completed = true;
            })
        }
    }
    pub(crate) fn server(api: Arc<Api>, state: Arc<State>, default: bool) -> WorkspaceMcpServer {
        let policy: Arc<dyn McpPrivatePolicy> = if default {
            Arc::new(DefaultMode(state.clone()))
        } else {
            Arc::new(Joint(state.clone()))
        };
        WorkspaceMcpServer::new(api, "workspace-1".into())
            .with_caller_agent_id(Some("agent-1".into()))
            .with_request_context(Arc::new(Factory {
                state: state.clone(),
                policy,
            }))
            .with_repository_guidance(&session(Some("3.0")), Arc::new(Source(state)))
    }
    pub(crate) async fn response(server: &WorkspaceMcpServer, code: &str) -> DeliveryResponse {
        server
            .handle_message_for_delivery(&call(1, code), server.capture_request_context())
            .await
            .unwrap()
    }
    pub(crate) async fn deliver(response: DeliveryResponse) -> (DeliveryOutcome, Value) {
        let (tx, mut rx) = mpsc::channel(1);
        let life = ConnectionLifetime::new();
        let result = response.enqueue(tx, &life.token()).await;
        let encoded = rx.recv().await.unwrap().into_line(&life.token());
        assert!(rx.recv().await.is_none());
        (result, serde_json::from_str(&encoded).unwrap())
    }
    pub(crate) fn has_guidance(value: &Value) -> bool {
        value.to_string().contains(TEXT)
    }

    #[tokio::test]
    async fn optional_default_uses_complete_nonempty_seal_once_and_never_empty_admit() {
        for empty in [false, true] {
            let state = State::new();
            let api = Arc::new(Api::new());
            let server = server(api, state.clone(), true);
            let code = if empty {
                "return 'ordinary';"
            } else {
                "await Promise.all([ws.git.listRoots(),ws.git.listRoots()]); return 'constant';"
            };
            let (outcome, value) = deliver(response(&server, code).await).await;
            assert_eq!(outcome, DeliveryOutcome::Admitted);
            assert!(!has_guidance(&value));
            assert_eq!(state.capture.load(Ordering::SeqCst), 1);
            assert_eq!(state.constructor.load(Ordering::SeqCst), 1);
            assert_eq!(state.leaves_dropped.load(Ordering::SeqCst), 1);
            assert_eq!(
                state.scope_runs.load(Ordering::SeqCst),
                if empty { 1 } else { 2 },
                "empty ordinary output must not re-enter the required scope"
            );
            let events = state.required.events.lock().unwrap();
            let final_events: Vec<_> = events
                .iter()
                .filter(|(kind, _)| *kind == McpPrivateBoundaryKind::TcpResponse)
                .collect();
            if empty {
                assert!(events.is_empty());
            } else {
                assert_eq!(final_events.len(), 1);
                assert_eq!(final_events[0].1, vec![0, 1]);
            }
            assert_eq!(state.drop_count.load(Ordering::SeqCst), 1);
        }
    }

    #[tokio::test]
    async fn optional_current_stale_foreign_and_missing_evidence_preserve_original_result() {
        for mode in [0, 3, 4, 5] {
            for stale in [false, true] {
                let state = State::new();
                state.mode.store(mode, Ordering::SeqCst);
                state.live.store(!stale, Ordering::SeqCst);
                let server = server(Arc::new(Api::new()), state.clone(), false);
                let (outcome, value) = deliver(response(&server, READ).await).await;
                assert_eq!(outcome, DeliveryOutcome::Admitted);
                assert!(value.to_string().contains(SECRET));
                assert_eq!(has_guidance(&value), mode == 0 && !stale);
                assert!(state.required.live.load(Ordering::SeqCst));
                assert_eq!(state.scope_runs.load(Ordering::SeqCst), 2);
            }
        }
    }

    #[tokio::test]
    async fn optional_constructor_future_and_capture_panics_do_not_retire_required_parent() {
        for mode in [1, 2, 6] {
            let state = State::new();
            state.mode.store(mode, Ordering::SeqCst);
            let server = server(Arc::new(Api::new()), state.clone(), false);
            let (_, value) = deliver(response(&server, READ).await).await;
            assert!(value.to_string().contains(SECRET));
            assert!(!has_guidance(&value));
            assert!(state.required.live.load(Ordering::SeqCst));
            assert_eq!(state.scope_runs.load(Ordering::SeqCst), 2);
            assert_eq!(
                state.constructor.load(Ordering::SeqCst),
                usize::from(mode != 6)
            );
        }
    }

    #[tokio::test]
    async fn optional_timeout_cancels_only_its_leaf_before_required_admission() {
        let state = State::new();
        let gate = state.pause_source();
        let server = server(Arc::new(Api::new()), state.clone(), false);
        let response = response(&server, READ).await;
        let task = tokio::spawn(deliver(response));
        gate.reached().await;
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(2)).await;
        let (outcome, value) = task.await.unwrap();
        tokio::time::resume();
        assert_eq!(outcome, DeliveryOutcome::Admitted);
        assert!(value.to_string().contains(SECRET));
        assert!(!has_guidance(&value));
        assert!(state.required.live.load(Ordering::SeqCst));
        assert_eq!(state.leaves_dropped.load(Ordering::SeqCst), 1);
        assert_eq!(state.scope_runs.load(Ordering::SeqCst), 2);
        gate.release.add_permits(1);
        assert_eq!(state.constructor.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn optional_missing_capture_never_invokes_qualified_source_or_falls_back() {
        for no_policy in [false, true] {
            let state = State::new();
            state.capture_enabled.store(false, Ordering::SeqCst);
            let server = if no_policy {
                WorkspaceMcpServer::new(Arc::new(Api::new()), "workspace-1".into())
                    .with_caller_agent_id(Some("agent-1".into()))
                    .with_repository_guidance(
                        &session(Some("3.0")),
                        Arc::new(Source(state.clone())),
                    )
            } else {
                server(Arc::new(Api::new()), state.clone(), false)
            };
            let (_, value) = deliver(response(&server, "return 'ordinary';").await).await;
            assert!(!has_guidance(&value));
            assert!(value.to_string().contains("ordinary"));
            assert_eq!(state.constructor.load(Ordering::SeqCst), 0);
            assert_eq!(
                state.capture.load(Ordering::SeqCst),
                usize::from(!no_policy)
            );
        }
        let state = State::new();
        let server = super::server(Arc::new(Api::new()), state.required.clone())
            .with_repository_guidance(&session(Some("3.0")), Arc::new(Source(state.clone())));
        let (_, value) = deliver(response(&server, READ).await).await;
        assert!(value.to_string().contains(SECRET));
        assert_eq!(state.capture.load(Ordering::SeqCst), 0);
        assert_eq!(state.constructor.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn optional_failed_unbound_and_unfinished_seals_never_become_empty() {
        for unfinished in [false, true] {
            let state = State::new();
            let api = Arc::new(Api {
                unbound: AtomicBool::new(!unfinished),
                ..Api::new()
            });
            let server = server(api, state.clone(), false);
            let context = server.capture_request_context();
            let reservation = if unfinished {
                Some(
                    McpHostCall {
                        invocation: context.private_invocation.as_ref().unwrap().0.clone(),
                        status: Arc::new(AtomicU8::new(0)),
                    }
                    .reserve()
                    .unwrap(),
                )
            } else {
                None
            };
            let code = if unfinished {
                "return 'ordinary';"
            } else {
                "try {await ws.git.listRoots();} catch(_){} return 'constant';"
            };
            let response = server
                .handle_message_for_delivery(&call(1, code), context)
                .await
                .unwrap();
            let (_, value) = deliver(response).await;
            refused(&value);
            assert_eq!(state.capture.load(Ordering::SeqCst), 0);
            assert_eq!(state.optional_calls.load(Ordering::SeqCst), 0);
            drop(reservation);
        }
    }

    #[tokio::test]
    async fn optional_cannot_bypass_required_retirement_or_poisoning_after_caught_reads() {
        let state = State::new();
        let gate = state.pause_source();
        let api = Arc::new(Api {
            outcome: Mutex::new(Err(SECRET.into())),
            ..Api::new()
        });
        let server = server(api.clone(), state.clone(), false);
        let response=response(&server,"try {await ws.git.listRoots();} catch(_){} await ws.workspace.setStatusMessage('completed'); return 'constant';").await;
        let task = tokio::spawn(deliver(response));
        gate.reached().await;
        state.required.retire();
        gate.release.add_permits(1);
        let (outcome, value) = task.await.unwrap();
        assert_eq!(outcome, DeliveryOutcome::Refused);
        refused(&value);
        assert_eq!(state.final_records.lock().unwrap().as_slice(), &[vec![0]]);
        assert_eq!(api.acquired.load(Ordering::SeqCst), 1);
        assert_eq!(*api.mutations.lock().unwrap(), vec!["completed"]);
    }

    #[tokio::test]
    async fn optional_joint_failure_preserves_empty_base_but_refuses_required_without_retry() {
        for empty in [false, true] {
            for mode in [1, 2, 3, 5] {
                let state = State::new();
                state.admission_mode.store(mode, Ordering::SeqCst);
                let server = server(Arc::new(Api::new()), state.clone(), false);
                let (outcome, value) = deliver(
                    response(&server, if empty { "return 'ordinary';" } else { READ }).await,
                )
                .await;
                if empty || mode == 5 {
                    assert_eq!(outcome, DeliveryOutcome::Admitted);
                    assert_eq!(has_guidance(&value), mode == 5);
                    assert!(value
                        .to_string()
                        .contains(if empty { "ordinary" } else { SECRET }));
                } else {
                    assert_eq!(outcome, DeliveryOutcome::Refused);
                    refused(&value);
                }
                assert_eq!(state.optional_calls.load(Ordering::SeqCst), 1);
            }
        }
    }

    #[tokio::test]
    async fn optional_foreign_receipt_cannot_authorize_another_original_invocation() {
        let state = State::new();
        state.admission_mode.store(4, Ordering::SeqCst);
        let server = server(Arc::new(Api::new()), state.clone(), false);
        let (outcome, first) = deliver(response(&server, READ).await).await;
        assert_eq!(outcome, DeliveryOutcome::Admitted);
        assert!(has_guidance(&first));
        let (outcome, second) = deliver(response(&server, READ).await).await;
        assert_eq!(outcome, DeliveryOutcome::Refused);
        refused(&second);
        assert_eq!(state.optional_calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn optional_does_not_enter_direct_host_attachment_or_artifact_admission() {
        let dir = tempfile::tempdir().unwrap();
        let state = State::new();
        let api = Arc::new(Api {
            checkout: Some(dir.path().to_str().unwrap().into()),
            max_chars: 20,
            ..Api::new()
        });
        let server = server(api, state.clone(), false);
        let value = server.handle_message(&call(1, READ)).await.unwrap();
        assert!(!has_guidance(&value));
        let kinds: Vec<_> = state
            .required
            .events
            .lock()
            .unwrap()
            .iter()
            .map(|(k, _)| *k)
            .collect();
        assert!(kinds.contains(&McpPrivateBoundaryKind::HostPromise));
        assert!(kinds.contains(&McpPrivateBoundaryKind::ArtifactStart));
        assert!(kinds.contains(&McpPrivateBoundaryKind::DirectResponse));
        assert_eq!(state.capture.load(Ordering::SeqCst), 0);
        assert_eq!(state.optional_calls.load(Ordering::SeqCst), 0);
        let registry = Arc::new(TurnAttachmentRegistry::new());
        let server = self::server(Arc::new(Api::new()), state.clone(), false)
            .with_turn_attachments(Some(registry.clone()));
        let result = server.handle_message(&call(2,"await ws.git.listRoots(); return {__mcpContentItems:[{type:'resource',resource:{uri:'fixture://original',mimeType:'application/json',text:'private-fixture-payload'}}]};")).await.unwrap();
        assert_eq!(result["result"]["isError"], false);
        assert_eq!(
            registry.pending_count_by_mime(&"agent-1".into(), "application/json"),
            1
        );
        assert!(state
            .required
            .events
            .lock()
            .unwrap()
            .iter()
            .any(|(kind, _)| *kind == McpPrivateBoundaryKind::Attachments));
        assert_eq!(state.capture.load(Ordering::SeqCst), 0);
        assert_eq!(state.optional_calls.load(Ordering::SeqCst), 0);
    }
    #[tokio::test]
    async fn optional_preparation_drop_retires_only_its_leaf_without_detached_completion() {
        let state = State::new();
        let gate = state.pause_source();
        let server = server(Arc::new(Api::new()), state.clone(), false);
        let response = response(&server, READ).await;
        let task = tokio::spawn(response.response.prepare_guidance());
        gate.reached().await;
        task.abort();
        assert!(matches!(task.await, Err(error) if error.is_cancelled()));
        tokio::time::timeout(WAIT, state.leaf_dropped.notified())
            .await
            .unwrap();
        assert!(state.required.live.load(Ordering::SeqCst));
        assert_eq!(state.leaves_dropped.load(Ordering::SeqCst), 1);
        assert_eq!(state.scope_runs.load(Ordering::SeqCst), 1);
        assert_eq!(state.optional_calls.load(Ordering::SeqCst), 0);
        gate.release.add_permits(1);
        assert_eq!(state.polls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn optional_preparation_cannot_add_late_records_to_either_successful_seal() {
        for empty in [false, true] {
            let state = State::new();
            state.mode.store(7, Ordering::SeqCst);
            let server = server(Arc::new(Api::new()), state.clone(), false);
            let (outcome, value) =
                deliver(response(&server, if empty { "return 'ordinary';" } else { READ }).await)
                    .await;
            assert_eq!(outcome, DeliveryOutcome::Admitted);
            assert!(has_guidance(&value));
            assert_eq!(
                state.final_records.lock().unwrap().as_slice(),
                &[if empty { vec![] } else { vec![0] }]
            );
            assert_eq!(
                state.scope_runs.load(Ordering::SeqCst),
                if empty { 1 } else { 2 }
            );
        }
    }
}

/// Actual last-owner destruction in a neutral ACP fixture. The factory, policy,
/// producer, evidence and probe never own the captured scope strongly. This is
/// not an execution of the Services/R lifetime implementation.
mod captured_scope_lifetime {
    use super::optional::{self, Joint, Source, State};
    use super::*;
    use crate::mcp_server::repository_guidance::{tests::session, ConnectionLifetime};
    use std::sync::Weak;

    #[derive(Default)]
    struct Probe {
        captures: AtomicUsize,
        drops: AtomicUsize,
        final_calls: AtomicUsize,
        live_at_final: AtomicBool,
        in_admission: AtomicBool,
        scope: Mutex<Weak<LastOwnerScope>>,
    }

    impl Probe {
        fn alive(&self) -> bool {
            // This temporary upgrade is dropped before returning to the test.
            self.scope.lock().unwrap().upgrade().is_some()
        }
        fn released(&self) {
            assert!(!self.alive());
            assert_eq!(self.drops.load(Ordering::SeqCst), 1);
            assert!(!self.in_admission.load(Ordering::SeqCst));
        }
    }

    struct Factory {
        state: Arc<State>,
        probe: Arc<Probe>,
        with_policy: bool,
    }

    struct LastOwnerScope {
        state: Arc<State>,
        probe: Arc<Probe>,
        policy: Arc<TrackedPolicy>,
        with_policy: bool,
    }

    impl Drop for LastOwnerScope {
        fn drop(&mut self) {
            assert!(
                !self.probe.in_admission.load(Ordering::SeqCst),
                "captured owner cleanup must occur outside the consuming admission"
            );
            self.state.required.retire();
            self.probe.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    impl McpRequestContext for Factory {
        fn capture(&self) -> Arc<dyn McpRequestScope> {
            self.probe.captures.fetch_add(1, Ordering::SeqCst);
            let captured = Arc::new_cyclic(|original| LastOwnerScope {
                state: self.state.clone(),
                probe: self.probe.clone(),
                policy: Arc::new(TrackedPolicy {
                    inner: Joint(self.state.clone()),
                    original: original.clone(),
                    probe: self.probe.clone(),
                }),
                with_policy: self.with_policy,
            });
            *self.probe.scope.lock().unwrap() = Arc::downgrade(&captured);
            captured
        }
    }

    impl McpRequestScope for LastOwnerScope {
        fn private_result_policy(&self) -> Option<Arc<dyn McpPrivatePolicy>> {
            self.with_policy
                .then(|| self.policy.clone() as Arc<dyn McpPrivatePolicy>)
        }
        fn scope<'a>(&'a self, body: McpContextFuture<'a>) -> McpContextFuture<'a> {
            Box::pin(async move {
                self.state.scope_runs.fetch_add(1, Ordering::SeqCst);
                body.await;
            })
        }
    }

    struct TrackedPolicy {
        inner: Joint,
        original: Weak<LastOwnerScope>,
        probe: Arc<Probe>,
    }

    struct AdmissionGuard(Arc<Probe>);
    impl Drop for AdmissionGuard {
        fn drop(&mut self) {
            self.0.in_admission.store(false, Ordering::SeqCst);
        }
    }

    impl McpPrivatePolicy for TrackedPolicy {
        fn capture_host(&self, call: McpHostCall) -> Box<dyn McpPrivateHostScope> {
            self.inner.capture_host(call)
        }
        fn capture_optional_context(&self) -> Option<Box<dyn McpOptionalContextScope>> {
            assert!(self.original.upgrade().is_some());
            self.inner.capture_optional_context()
        }
        fn admit<'a>(
            &'a self,
            boundary: &'a McpPrivateBoundary,
            originals: &'a [McpReadEvidence],
            packet: PreparedMcpTransfer<'a>,
        ) -> BoxFuture<'a, McpPrivateAdmission> {
            self.inner.admit(boundary, originals, packet)
        }
        fn admit_optional<'a>(
            &'a self,
            boundary: &'a McpPrivateBoundary,
            sealed: McpSealedReads<'a>,
            optional: &'a McpOptionalEvidence,
            packet: PreparedMcpVariants<'a>,
        ) -> BoxFuture<'a, McpPrivateAdmission> {
            Box::pin(async move {
                assert!(sealed.is_empty());
                self.probe.final_calls.fetch_add(1, Ordering::SeqCst);
                self.probe
                    .live_at_final
                    .store(self.original.upgrade().is_some(), Ordering::SeqCst);
                self.probe.in_admission.store(true, Ordering::SeqCst);
                let _guard = AdmissionGuard(self.probe.clone());
                self.inner
                    .admit_optional(boundary, sealed, optional, packet)
                    .await
            })
        }
    }

    fn fixture(api: Arc<Api>, with_policy: bool) -> (WorkspaceMcpServer, Arc<State>, Arc<Probe>) {
        let state = State::new();
        let probe = Arc::new(Probe::default());
        let server = WorkspaceMcpServer::new(api, "workspace-1".into())
            .with_caller_agent_id(Some("agent-1".into()))
            .with_request_context(Arc::new(Factory {
                state: state.clone(),
                probe: probe.clone(),
                with_policy,
            }))
            .with_repository_guidance(&session(Some("3.0")), Arc::new(Source(state.clone())));
        (server, state, probe)
    }

    async fn ready(server: &WorkspaceMcpServer, probe: &Probe) -> DeliveryResponse {
        // Move the original capture into the actual dispatch path. No test
        // variable or factory retains a strong captured-scope clone afterward.
        let response = optional::response(server, "return 'ordinary';").await;
        assert!(response.output.is_none());
        assert!(response.sealed_empty.is_some());
        assert_eq!(probe.captures.load(Ordering::SeqCst), 1);
        assert!(probe.alive());
        assert_eq!(probe.drops.load(Ordering::SeqCst), 0);
        response
    }

    #[tokio::test]
    async fn successful_empty_keeps_last_captured_scope_through_optional_transfer() {
        let (server, state, probe) = fixture(Arc::new(Api::new()), true);
        let response = ready(&server, &probe).await;
        let (outcome, value) = optional::deliver(response).await;
        assert_eq!(
            probe.final_calls.load(Ordering::SeqCst),
            1,
            "final probe was reached"
        );
        assert!(
            probe.live_at_final.load(Ordering::SeqCst),
            "qualified preparation dropped the last original captured scope before final admission"
        );
        assert_eq!(outcome, DeliveryOutcome::Admitted);
        assert!(optional::has_guidance(&value));
        assert!(value.to_string().contains("ordinary"));
        assert_eq!(state.scope_runs.load(Ordering::SeqCst), 1);
        assert_eq!(state.constructor.load(Ordering::SeqCst), 1);
        assert_eq!(state.optional_calls.load(Ordering::SeqCst), 1);
        probe.released();
    }

    #[tokio::test]
    async fn queued_empty_keeps_owner_until_original_cancel_close_or_consumption() {
        for finish in 0..3 {
            let (server, state, probe) = fixture(Arc::new(Api::new()), true);
            let response = ready(&server, &probe).await;
            let (tx, mut rx) = mpsc::channel(1);
            tx.send(PreparedBridgeLine::plain(json!({"queued":true})))
                .await
                .unwrap();
            let lifetime = ConnectionLifetime::new();
            let token = lifetime.token();
            let mut delivery = Box::pin(response.enqueue(tx, &token));
            tokio::select! {
                () = state.leaf_dropped.notified() => {},
                result = &mut delivery => panic!("full original queue completed: {result:?}"),
            }
            assert!(
                std::future::poll_fn(|cx| Poll::Ready(delivery.as_mut().poll(cx)))
                    .await
                    .is_pending()
            );
            assert!(probe.alive());
            assert_eq!(probe.drops.load(Ordering::SeqCst), 0);
            assert_eq!(probe.final_calls.load(Ordering::SeqCst), 0);
            match finish {
                0 => {
                    drop(delivery);
                    assert!(rx
                        .recv()
                        .await
                        .unwrap()
                        .into_line(&token)
                        .contains("queued"));
                    assert!(rx.recv().await.is_none());
                }
                1 => {
                    rx.close();
                    assert_eq!(delivery.await, DeliveryOutcome::ConsumerClosed);
                    assert!(rx
                        .recv()
                        .await
                        .unwrap()
                        .into_line(&token)
                        .contains("queued"));
                    assert!(rx.recv().await.is_none());
                }
                _ => {
                    assert!(rx
                        .recv()
                        .await
                        .unwrap()
                        .into_line(&token)
                        .contains("queued"));
                    assert_eq!(delivery.await, DeliveryOutcome::Admitted);
                    probe.released();
                    // The queued packet owns bytes/correlation only, not the
                    // request scope; its original decision has completed.
                    let value: Value =
                        serde_json::from_str(&rx.recv().await.unwrap().into_line(&token)).unwrap();
                    assert!(optional::has_guidance(&value));
                    assert!(rx.recv().await.is_none());
                }
            }
            probe.released();
            assert_eq!(state.scope_runs.load(Ordering::SeqCst), 1);
            assert_eq!(state.constructor.load(Ordering::SeqCst), 1);
            assert_eq!(
                probe.final_calls.load(Ordering::SeqCst),
                usize::from(finish == 2)
            );
        }
    }

    #[tokio::test]
    async fn held_empty_decision_releases_outside_admission_without_post_transfer_replay() {
        for after_transfer in [false, true] {
            for cancel in [false, true] {
                let (server, state, probe) = fixture(Arc::new(Api::new()), true);
                let gate = state.pause_admission(after_transfer);
                let response = ready(&server, &probe).await;
                let (tx, mut rx) = mpsc::channel(1);
                let lifetime = ConnectionLifetime::new();
                let token = lifetime.token();
                let mut delivery = Box::pin(response.enqueue(tx, &token));
                tokio::select! {
                    () = gate.reached() => {},
                    result = &mut delivery => panic!("held admission completed: {result:?}"),
                }
                assert!(probe.alive());
                assert!(probe.in_admission.load(Ordering::SeqCst));
                assert_eq!(probe.drops.load(Ordering::SeqCst), 0);
                if cancel {
                    drop(delivery);
                } else {
                    gate.release.add_permits(1);
                    assert_eq!(delivery.await, DeliveryOutcome::Admitted);
                }
                probe.released();
                if after_transfer || !cancel {
                    let value: Value =
                        serde_json::from_str(&rx.recv().await.unwrap().into_line(&token)).unwrap();
                    assert!(optional::has_guidance(&value));
                }
                assert!(rx.recv().await.is_none());
                assert_eq!(probe.final_calls.load(Ordering::SeqCst), 1);
                assert_eq!(state.constructor.load(Ordering::SeqCst), 1);
                assert_eq!(state.scope_runs.load(Ordering::SeqCst), 1);
            }
        }
    }

    #[tokio::test]
    async fn retained_owner_does_not_revive_explicitly_retired_original() {
        let (server, state, probe) = fixture(Arc::new(Api::new()), true);
        let gate = state.pause_admission(false);
        let response = ready(&server, &probe).await;
        let mut delivery = Box::pin(optional::deliver(response));
        tokio::select! {
            () = gate.reached() => {},
            _ = &mut delivery => panic!("held admission completed"),
        }
        assert!(probe.alive());
        state.required.retire();
        gate.release.add_permits(1);
        let (outcome, value) = delivery.await;
        assert_eq!(outcome, DeliveryOutcome::Admitted);
        assert!(!optional::has_guidance(&value));
        assert!(value.to_string().contains("ordinary"));
        assert_eq!(state.scope_runs.load(Ordering::SeqCst), 1);
        probe.released();
    }

    #[tokio::test]
    async fn omitted_preparation_and_unpolled_delivery_release_last_owner() {
        for mode in [1, 2, 3, 6] {
            let (server, state, probe) = fixture(Arc::new(Api::new()), true);
            state.mode.store(mode, Ordering::SeqCst);
            let response = ready(&server, &probe).await;
            let (outcome, value) = optional::deliver(response).await;
            assert_eq!(outcome, DeliveryOutcome::Admitted);
            assert!(!optional::has_guidance(&value));
            assert!(value.to_string().contains("ordinary"));
            assert_eq!(probe.final_calls.load(Ordering::SeqCst), 0);
            assert_eq!(state.scope_runs.load(Ordering::SeqCst), 1);
            probe.released();
        }
        for unpolled in [false, true] {
            let (server, state, probe) = fixture(Arc::new(Api::new()), true);
            let response = ready(&server, &probe).await;
            if unpolled {
                let (tx, mut rx) = mpsc::channel(1);
                let lifetime = ConnectionLifetime::new();
                let token = lifetime.token();
                let delivery = response.enqueue(tx, &token);
                drop(delivery);
                assert!(rx.recv().await.is_none());
            } else {
                drop(response);
            }
            assert_eq!(state.capture.load(Ordering::SeqCst), 0);
            assert_eq!(state.constructor.load(Ordering::SeqCst), 0);
            assert_eq!(probe.final_calls.load(Ordering::SeqCst), 0);
            assert_eq!(state.scope_runs.load(Ordering::SeqCst), 1);
            probe.released();
        }
    }

    #[tokio::test]
    async fn pending_preparation_timeout_or_cancel_releases_original_without_detachment() {
        for timeout in [false, true] {
            let (server, state, probe) = fixture(Arc::new(Api::new()), true);
            let gate = state.pause_source();
            let response = ready(&server, &probe).await;
            let mut delivery = Box::pin(optional::deliver(response));
            tokio::select! {
                () = gate.reached() => {},
                _ = &mut delivery => panic!("held preparation completed"),
            }
            assert!(probe.alive());
            if timeout {
                tokio::time::pause();
                tokio::time::advance(Duration::from_secs(2)).await;
                let (outcome, value) = delivery.await;
                tokio::time::resume();
                assert_eq!(outcome, DeliveryOutcome::Admitted);
                assert!(!optional::has_guidance(&value));
                assert!(value.to_string().contains("ordinary"));
            } else {
                drop(delivery);
            }
            tokio::time::timeout(WAIT, state.leaf_dropped.notified())
                .await
                .unwrap();
            probe.released();
            assert_eq!(probe.final_calls.load(Ordering::SeqCst), 0);
            assert_eq!(state.constructor.load(Ordering::SeqCst), 1);
            assert_eq!(state.scope_runs.load(Ordering::SeqCst), 1);
            gate.release.add_permits(1);
            assert_eq!(state.leaves_dropped.load(Ordering::SeqCst), 1);
        }
    }

    #[tokio::test]
    async fn absent_invocation_and_failed_seal_never_gain_empty_retention() {
        for failed_seal in [false, true] {
            let api = Arc::new(Api::new());
            api.unbound.store(failed_seal, Ordering::SeqCst);
            let (server, state, probe) = fixture(api, failed_seal);
            let code = if failed_seal {
                optional::READ
            } else {
                "return 'ordinary';"
            };
            let response = optional::response(&server, code).await;
            assert!(response.sealed_empty.is_none());
            assert!(response.output.is_none());
            let (outcome, value) = optional::deliver(response).await;
            assert_eq!(outcome, DeliveryOutcome::Admitted);
            assert!(!optional::has_guidance(&value));
            if failed_seal {
                assert_eq!(value["result"], refusal_tool_result());
            } else {
                assert!(value.to_string().contains("ordinary"));
            }
            assert_eq!(state.capture.load(Ordering::SeqCst), 0);
            assert_eq!(state.constructor.load(Ordering::SeqCst), 0);
            assert_eq!(probe.final_calls.load(Ordering::SeqCst), 0);
            assert_eq!(state.scope_runs.load(Ordering::SeqCst), 1);
            probe.released();
        }
    }
}
