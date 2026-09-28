use super::*;
use crate::repository_context_live::tests::LiveFixture;
use crate::repository_read_source::tests::{call, ReadServer};
use intent_acp::mcp_server::private_results::McpPrivateBoundaryKind;
use intent_acp::mcp_server::WorkspaceMcpServer;
use serde_json::Value;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::{Notify, Semaphore};

pub(crate) const FACTS: &str = "[Repository facts — inert JSON lines]";
pub(crate) struct Hold {
    entered: Notify,
    release: Semaphore,
}
impl Hold {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            entered: Notify::new(),
            release: Semaphore::new(0),
        })
    }
    pub(crate) async fn wait(&self) {
        self.entered.notify_one();
        self.release.acquire().await.unwrap().forget();
    }
    pub(crate) async fn reached(&self) {
        tokio::time::timeout(Duration::from_secs(15), self.entered.notified())
            .await
            .unwrap();
    }
    pub(crate) fn resume(&self) {
        self.release.add_permits(1);
    }
}
#[derive(Default)]
pub(crate) struct Control {
    pub(crate) hold: Mutex<Option<Arc<Hold>>>,
    pub(crate) after: Mutex<Option<Arc<Hold>>>,
    pub(crate) events: Mutex<Vec<usize>>,
    required_events: Mutex<Vec<(McpPrivateBoundaryKind, usize)>>,
    required_hold: Mutex<Option<(McpPrivateBoundaryKind, bool, Arc<Hold>)>>,
    optional: Mutex<Option<Arc<Preparation>>>,
}
struct Factory(Arc<dyn McpRequestContext>, Arc<Control>);
struct Scope(Arc<dyn McpRequestScope>, Arc<Control>);
struct Policy(Arc<dyn McpPrivatePolicy>, Arc<Control>);
impl McpRequestContext for Factory {
    fn capture(&self) -> Arc<dyn McpRequestScope> {
        Arc::new(Scope(self.0.capture(), self.1.clone()))
    }
}
impl McpRequestScope for Scope {
    fn scope<'a>(&'a self, body: McpContextFuture<'a>) -> McpContextFuture<'a> {
        self.0.scope(body)
    }
    fn private_result_policy(&self) -> Option<Arc<dyn McpPrivatePolicy>> {
        self.0
            .private_result_policy()
            .map(|p| Arc::new(Policy(p, self.1.clone())) as Arc<dyn McpPrivatePolicy>)
    }
}
impl McpPrivatePolicy for Policy {
    fn capture_host(&self, call: McpHostCall) -> Box<dyn McpPrivateHostScope> {
        self.0.capture_host(call)
    }
    fn admit<'a>(
        &'a self,
        b: &'a McpPrivateBoundary,
        r: &'a [McpReadEvidence],
        p: PreparedMcpTransfer<'a>,
    ) -> BoxFuture<'a, McpPrivateAdmission> {
        Box::pin(async move {
            self.1
                .required_events
                .lock()
                .unwrap()
                .push((b.kind(), r.len()));
            let hold = {
                let mut slot = self.1.required_hold.lock().unwrap();
                if slot.as_ref().is_some_and(|(kind, _, _)| *kind == b.kind()) {
                    slot.take()
                } else {
                    None
                }
            };
            if let Some((_, false, gate)) = &hold {
                gate.wait().await;
            }
            let result = self.0.admit(b, r, p).await;
            if let Some((_, true, gate)) = &hold {
                gate.wait().await;
            }
            result
        })
    }
    fn capture_optional_context(&self) -> Option<Box<dyn McpOptionalContextScope>> {
        self.0.capture_optional_context()
    }
    fn admit_optional<'a>(
        &'a self,
        b: &'a McpPrivateBoundary,
        r: McpSealedReads<'a>,
        e: &'a McpOptionalEvidence,
        p: PreparedMcpVariants<'a>,
    ) -> BoxFuture<'a, McpPrivateAdmission> {
        Box::pin(async move {
            self.1.events.lock().unwrap().push(r.records().len());
            let cell = e.downcast_ref::<Arc<Preparation>>().unwrap().clone();
            assert!(
                cell.state.lock().unwrap().ready.is_some(),
                "real one-shot preparation reached final boundary"
            );
            *self.1.optional.lock().unwrap() = Some(cell);
            let hold = self.1.hold.lock().unwrap().take();
            if let Some(h) = hold {
                h.wait().await;
            }
            let result = self.0.admit_optional(b, r, e, p).await;
            let after = self.1.after.lock().unwrap().take();
            if let Some(h) = after {
                h.wait().await;
            }
            result
        })
    }
}
pub(crate) fn controlled(f: &LiveFixture, c: Arc<Control>) -> WorkspaceMcpServer {
    WorkspaceMcpServer::new(f.base.api(), f.session.workspace_id.clone())
        .with_caller_agent_id(Some(f.session.id.clone()))
        .with_request_context(Arc::new(Factory(f.owner.mcp_context(), c)))
        .with_repository_guidance(&f.session, f.owner.guidance_source())
}
pub(crate) async fn tcp(server: WorkspaceMcpServer, code: &str) -> Value {
    let bridge = intent_acp::mcp_bridge::serve_workspace_mcp_tcp(Arc::new(server))
        .await
        .unwrap();
    let mut stream = tokio::net::TcpStream::connect(bridge.addr()).await.unwrap();
    stream
        .write_all(format!("{}\n", call(code)).as_bytes())
        .await
        .unwrap();
    let mut line = String::new();
    tokio::time::timeout(
        Duration::from_secs(15),
        BufReader::new(stream).read_line(&mut line),
    )
    .await
    .unwrap()
    .unwrap();
    drop(bridge);
    serde_json::from_str(&line).unwrap()
}
#[intent_test_macros::daemon_test]
async fn context_output_actual_tcp_nonempty_and_original_successful_zero_keep_last_owner() {
    for code in ["return 41;", "return await ws.pr.snapshot(4);"] {
        let http = ReadServer::new().await;
        let f = LiveFixture::new(&http).await;
        let control = Arc::new(Control::default());
        let hold = Hold::new();
        *control.hold.lock().unwrap() = Some(hold.clone());
        let server = controlled(&f, control.clone());
        let task = tokio::spawn(async move { tcp(server, code).await });
        hold.reached().await;
        let cell = control.optional.lock().unwrap().clone().unwrap();
        let ready = cell.state.lock().unwrap().ready.clone().unwrap();
        assert!(ready
            .prepared
            .metadata()
            .request()
            .retains(f.base.auth.service.as_ref()));
        assert_eq!(
            control.events.lock().unwrap().as_slice(),
            if code == "return 41;" {
                &[0][..]
            } else {
                &[2][..]
            }
        );
        assert!(
            intent_core::with_caller(
                Caller::Agent {
                    agent_id: f.session.id.clone()
                },
                async { cell.local.check_current() }
            )
            .await
            .is_ok(),
            "the original owning scope survives successful empty delivery through final decision"
        );
        hold.resume();
        let value = task.await.unwrap();
        assert!(value.to_string().contains(FACTS), "{value}");
        assert!(
            value.to_string().contains(if code == "return 41;" {
                "41"
            } else {
                "actual review"
            }),
            "{value}"
        );
        assert!(
            intent_core::with_caller(
                Caller::Agent {
                    agent_id: f.session.id.clone()
                },
                async { cell.local.check_current() }
            )
            .await
            .is_err(),
            "escaped metadata cannot retain the final original scope"
        );
        f.owner.drain_jobs().await;
        assert_eq!(http.count(), if code == "return 41;" { 0 } else { 4 });
    }
}

async fn prompt_exchange(
    guidance: Option<intent_acp::session::PromptGuidance>,
    fail: bool,
) -> (Value, bool) {
    use intent_acp::session::{prompt_with_guidance, ActivityTracker};
    use intent_acp::{Connection, ConnectionHooks};
    let (input, output) = tokio::io::duplex(32 * 1024);
    let (mut replies, read) = tokio::io::duplex(4096);
    let connection = Connection::new(input, read, None, ConnectionHooks::default());
    let activity = ActivityTracker::new();
    let blocks =
        serde_json::from_value(serde_json::json!([{"type":"text","text":"original user prompt"}]))
            .unwrap();
    let before = connection.response_seq();
    let request = prompt_with_guidance(
        &connection,
        "original-context-session",
        blocks,
        &activity,
        guidance,
    );
    let peer = async {
        let mut reader = BufReader::new(output);
        let mut line = String::new();
        reader.read_line(&mut line).await.unwrap();
        let frame: Value = serde_json::from_str(&line).unwrap();
        let response = if fail {
            serde_json::json!({"jsonrpc":"2.0","id":frame["id"],"error":{"code":-32603,"message":"scripted native consumer failure"}})
        } else {
            serde_json::json!({"jsonrpc":"2.0","id":frame["id"],"result":{"stopReason":"end_turn"}})
        };
        replies
            .write_all(format!("{response}\n").as_bytes())
            .await
            .unwrap();
        frame
    };
    let (result, frame) = tokio::time::timeout(Duration::from_secs(15), async {
        tokio::join!(request, peer)
    })
    .await
    .unwrap();
    assert_eq!(
        connection.response_seq(),
        before + 1,
        "one response to the original pending request"
    );
    assert_eq!(frame["id"], 1, "original prompt ID");
    (frame, result.is_ok())
}

#[intent_test_macros::daemon_test]
async fn context_output_genuine_prompt_pending_entry_current_stale_and_consumer_failure() {
    for mode in ["current", "stale", "retired", "consumer-error"] {
        let http = ReadServer::new().await;
        let f = LiveFixture::new(&http).await;
        let capture = f.prompt_capture().await;
        let guidance = intent_core::with_caller(
            Caller::Agent {
                agent_id: f.session.id.clone(),
            },
            capture.prepare(),
        )
        .await;
        assert!(guidance.is_some(), "real captured prompt preparation");
        match mode {
            "stale" => f.owner.invalidate(),
            "retired" => f.physical.interrupt_requests(),
            _ => {}
        }
        let (frame, ok) = intent_core::with_caller(
            Caller::Agent {
                agent_id: f.session.id.clone(),
            },
            prompt_exchange(guidance, mode == "consumer-error"),
        )
        .await;
        assert_eq!(frame["method"], "session/prompt");
        assert_eq!(frame["params"]["sessionId"], "original-context-session");
        assert!(frame.to_string().contains("original user prompt"));
        assert_eq!(
            frame.to_string().contains(FACTS),
            matches!(mode, "current" | "consumer-error"),
            "{mode}: {frame}"
        );
        assert_eq!(ok, mode != "consumer-error");
        assert_eq!(http.count(), 0);
        f.owner.drain_jobs().await;
    }
}

#[intent_test_macros::daemon_test(flavor = "multi_thread", worker_threads = 4)]
async fn context_output_actual_settings_publication_order_and_origin_only_revision() {
    use crate::repository_context_live::tests::BlockingHold;
    use intent_core::WorkspaceApi;
    for mode in ["apply", "reload", "pin", "auth"] {
        let http = ReadServer::new().await;
        let f = LiveFixture::new(&http).await;
        let control = Arc::new(Control::default());
        let final_hold = Hold::new();
        *control.hold.lock().unwrap() = Some(final_hold.clone());
        let endpoint = controlled(&f, control.clone());
        let task = tokio::spawn(async move {
            tcp(
                endpoint,
                "await ws.pr.snapshot(4); return 'publication-original';",
            )
            .await
        });
        final_hold.reached().await;
        let publication = BlockingHold::new();
        let probe = publication.clone();
        *f.base
            .auth
            .registry
            .context_publication_probe
            .lock()
            .unwrap() = Some(Arc::new(move || probe.wait()));
        let registry = f.base.auth.registry.clone();
        let service = f.base.auth.service.clone();
        let writer = tokio::spawn(async move {
            if mode == "auth" {
                intent_core::with_caller(Caller::Daemon,service.settings_update(serde_json::json!([
                    {"path":intent_sourcecontrol::gitlab_token::SECRET_ACCOUNT,"value":"replacement"},
                    {"path":"git.autoCommit","value":false}
                ]))).await.unwrap();
            } else {
                tokio::task::spawn_blocking(move || match mode {
                    "apply" => {
                        registry
                            .apply(&[("git.autoCommit".into(), serde_json::json!(false))])
                            .unwrap();
                    }
                    "reload" => {
                        let text = std::fs::read_to_string(registry.config_path()).unwrap();
                        registry.reload(&text).unwrap();
                    }
                    "pin" => {
                        let value = registry.get("git.autoCommit").unwrap();
                        registry
                            .pin("git.autoCommit", value, "--same-value-origin")
                            .unwrap();
                    }
                    _ => unreachable!(),
                })
                .await
                .unwrap();
            }
        });
        publication.reached().await;
        final_hold.resume();
        let result = tokio::time::timeout(Duration::from_secs(15), task)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            result.to_string().contains(FACTS),
            mode != "auth",
            "{mode}: {result}"
        );
        assert_eq!(
            result.to_string().contains("publication-original"),
            mode != "auth",
            "{result}"
        );
        if mode == "auth" {
            assert!(result
                .to_string()
                .contains("Private result delivery refused"));
        }
        publication.release();
        writer.await.unwrap();
        f.owner.drain_jobs().await;
    }
    let http = ReadServer::new().await;
    let f = LiveFixture::new(&http).await;
    let (_, before) = f.observe().await;
    let registry = f.base.auth.registry.clone();
    let value = registry.get("git.autoCommit").unwrap();
    registry
        .pin("git.autoCommit", value.clone(), "--origin-only")
        .unwrap();
    assert_eq!(registry.get("git.autoCommit"), Some(value));
    let (_, after) = f.observe().await;
    assert_ne!(
        before.value().context.revision,
        after.value().context.revision
    );
}

#[intent_test_macros::daemon_test(flavor = "multi_thread", worker_threads = 4)]
async fn context_output_busy_snapshot_and_optional_provider_fence_omit_without_waiting() {
    use crate::repository_context_live::tests::BlockingHold;
    for mode in ["snapshot", "provider"] {
        let http = ReadServer::new().await;
        let f = LiveFixture::new(&http).await;
        let control = Arc::new(Control::default());
        let gate = Hold::new();
        *control.hold.lock().unwrap() = Some(gate.clone());
        let endpoint = controlled(&f, control.clone());
        let task = tokio::spawn(async move { tcp(endpoint, "return 'ordinary-complete';").await });
        gate.reached().await;
        let held = BlockingHold::new();
        let probe = held.clone();
        let registry = f.base.auth.registry.clone();
        let facts = f
            .base
            .auth
            .service
            .gitlab_repository_connection_facts()
            .unwrap();
        let holder = tokio::task::spawn_blocking(move || {
            if mode == "snapshot" {
                registry.context_hold_snapshot_for_test(|| probe.wait());
            } else {
                crate::source_control_auth_ops::repository_owner::RepositoryConnectionFacts::with_prompt_current(Some(&facts),|current|{assert!(current);probe.wait();Ok(())}).unwrap();
            }
        });
        held.reached().await;
        gate.resume();
        let result = tokio::time::timeout(Duration::from_secs(15), task)
            .await
            .unwrap()
            .unwrap();
        assert!(result.to_string().contains("ordinary-complete"), "{result}");
        assert!(!result.to_string().contains(FACTS), "{result}");
        held.release();
        holder.await.unwrap();
        f.owner.drain_jobs().await;
    }
}

#[intent_test_macros::daemon_test]
async fn context_output_artifact_and_attachment_effects_stay_factual_before_optional_delivery() {
    use intent_acp::mcp_server::private_results::McpPrivateBoundaryKind as Boundary;
    for artifact in [true, false] {
        for after in [false, true] {
            let http = ReadServer::new().await;
            let f = LiveFixture::new(&http).await;
            f.base
                .auth
                .registry
                .apply(&[(
                    "workspaceApi.maxOutputChars".into(),
                    serde_json::json!(1000),
                )])
                .unwrap();
            let c = Arc::new(Control::default());
            let held = Hold::new();
            let kind = if artifact {
                Boundary::ArtifactStart
            } else {
                Boundary::Attachments
            };
            *c.required_hold.lock().unwrap() = Some((kind, after, held.clone()));
            let attachments = Arc::new(intent_core::TurnAttachmentRegistry::new());
            let endpoint =
                controlled(&f, c.clone()).with_turn_attachments(Some(attachments.clone()));
            let code = if artifact {
                "const p=await ws.pr.snapshot(4);return p.title.repeat(300);"
            } else {
                "const p=await ws.pr.snapshot(4);return {__mcpContentItems:[{type:'resource',resource:{uri:'fixture://actual-context',mimeType:'application/json',text:JSON.stringify({title:p.title})}}]};"
            };
            let task = tokio::spawn(async move { tcp(endpoint, code).await });
            held.reached().await;
            let folder = f.base.git.dir.path().join("tool-outputs");
            assert!(!folder.exists());
            f.physical.interrupt_requests();
            held.resume();
            let reply = task.await.unwrap();
            assert!(
                reply
                    .to_string()
                    .contains("Private result delivery refused"),
                "{reply}"
            );
            assert!(!reply.to_string().contains(FACTS));
            if artifact {
                assert_eq!(folder.exists(), after);
                if after {
                    assert_eq!(std::fs::read_dir(folder).unwrap().count(), 1);
                }
            } else {
                assert_eq!(
                    attachments.pending_count_by_mime(&f.session.id, "application/json"),
                    usize::from(after)
                );
            }
            assert!(c.required_events.lock().unwrap().contains(&(kind, 2)));
            f.owner.drain_jobs().await;
        }
    }
}

#[intent_test_macros::daemon_test]
async fn context_output_provider_child_revision_and_public_quota_do_not_become_permission() {
    for mode in ["child", "quota"] {
        let http = ReadServer::new().await;
        let f = LiveFixture::new(&http).await;
        if mode == "quota" {
            http.status(
                "/api/v4/projects/group%2Fproject/merge_requests/4/approvals",
                429,
            );
        }
        let c = Arc::new(Control::default());
        let held = Hold::new();
        *c.hold.lock().unwrap() = Some(held.clone());
        let endpoint = controlled(&f, c.clone());
        let task =
            tokio::spawn(async move { tcp(endpoint, "return await ws.pr.snapshot(4);").await });
        held.reached().await;
        if mode == "child" {
            let directory = f.base.auth.service.repository_connection_directory();
            directory
                .set_child_policy(&f.base.auth.request().binding, true)
                .unwrap();
        }
        held.resume();
        let reply = task.await.unwrap();
        assert!(reply.to_string().contains("actual review"), "{reply}");
        if mode == "child" {
            assert!(
                !reply.to_string().contains(FACTS),
                "old child-policy facts omitted, required native read stays valid"
            );
        } else {
            assert!(reply.to_string().contains("rate-limited"));
            assert!(reply.to_string().contains(FACTS));
            let calls = http.count();
            let next = tcp(
                f.server(),
                "try {await ws.pr.snapshot(4);}catch(e){} return 'quota-observed';",
            )
            .await;
            assert!(next.to_string().contains("quota-observed"), "{next}");
            assert_eq!(http.count(), calls);
            assert!(f
                .base
                .auth
                .service
                .sweep_rate_limit_paused_until()
                .is_none());
        }
        f.owner.drain_jobs().await;
    }
}

#[intent_test_macros::daemon_test]
async fn context_output_one_shot_scope_closes_duplicate_and_unpolled_without_required_retirement() {
    use intent_core::caller::with_caller;
    let http = ReadServer::new().await;
    let f = LiveFixture::new(&http).await;
    let caller = Caller::Agent {
        agent_id: f.session.id.clone(),
    };
    let (required, read) = f.owner.callback.capture_owned();
    let read = read.unwrap();
    let policy = ContextPolicy {
        owner: f.owner.clone(),
        request: read.clone(),
        original: required.private_result_policy().unwrap(),
    };
    let optional = with_caller(caller.clone(), async {
        policy.capture_optional_context().unwrap()
    })
    .await;
    let fence = RepositoryGuidanceFence::default();
    let source = f.owner.guidance_source();
    let mut cell = None;
    let mut candidate = None;
    with_caller(
        caller.clone(),
        optional.scope(Box::pin(async {
            cell = Some(PREPARATION.with(Clone::clone));
            candidate = source
                .prepare(&f.session.workspace_id, &caller, &fence)
                .await;
        })),
    )
    .await;
    assert!(candidate.is_some());
    let cell = cell.unwrap();
    assert!(cell.state.lock().unwrap().ready.is_some());
    let ran = std::sync::atomic::AtomicUsize::new(0);
    with_caller(
        caller.clone(),
        optional.scope(Box::pin(async {
            ran.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        })),
    )
    .await;
    assert_eq!(ran.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert!(cell.state.lock().unwrap().closed);
    with_caller(caller.clone(), async {
        assert!(read.check_current().is_ok());
        assert!(read.child().unwrap().transfer(|| Ok(())).is_ok());
    })
    .await;
    let unpolled = with_caller(caller.clone(), async {
        policy.capture_optional_context().unwrap()
    })
    .await;
    drop(unpolled.scope(Box::pin(async {
        ran.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    })));
    drop(unpolled);
    with_caller(caller.clone(), async {
        assert!(read.check_current().is_ok());
    })
    .await;
    assert_eq!(ran.load(std::sync::atomic::Ordering::SeqCst), 0);
    drop(required);
    with_caller(caller, async {
        assert!(read.check_current().is_err());
    })
    .await;
}

struct FailingSource {
    mode: &'static str,
    calls: Arc<std::sync::atomic::AtomicUsize>,
}
impl RepositoryGuidanceSource for FailingSource {
    fn preparation(&self) -> GuidancePreparation {
        GuidancePreparation::Qualified
    }
    fn prepare<'a>(
        &'a self,
        _: &'a WorkspaceId,
        _: &'a Caller,
        _: &'a RepositoryGuidanceFence,
    ) -> BoxFuture<'a, Option<GuidanceCandidate>> {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        assert_ne!(
            self.mode, "constructor-panic",
            "scripted optional constructor failure"
        );
        Box::pin(async move {
            assert_ne!(
                self.mode, "future-panic",
                "scripted optional future failure"
            );
            std::future::pending().await
        })
    }
}
#[intent_test_macros::daemon_test]
async fn context_output_optional_constructor_future_and_timeout_preserve_completed_required_output()
{
    for mode in ["constructor-panic", "future-panic", "timeout"] {
        let http = ReadServer::new().await;
        let f = LiveFixture::new(&http).await;
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let source = Arc::new(FailingSource {
            mode,
            calls: calls.clone(),
        });
        let endpoint = WorkspaceMcpServer::new(f.base.api(), f.session.workspace_id.clone())
            .with_caller_agent_id(Some(f.session.id.clone()))
            .with_request_context(f.owner.mcp_context())
            .with_repository_guidance(&f.session, source);
        let value = tcp(
            endpoint,
            "await ws.pr.snapshot(4);return 'required-still-current';",
        )
        .await;
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert!(
            value.to_string().contains("required-still-current"),
            "{mode}: {value}"
        );
        assert!(!value.to_string().contains(FACTS));
        f.owner.drain_jobs().await;
    }
}

#[intent_test_macros::daemon_test]
async fn context_output_absent_capture_and_legacy_session_never_prepare_qualified_guidance() {
    for absent in [true, false] {
        let http = ReadServer::new().await;
        let f = LiveFixture::new(&http).await;
        let mut session = f.session.clone();
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let source = Arc::new(FailingSource {
            mode: "constructor-panic",
            calls: calls.clone(),
        });
        let mut endpoint = WorkspaceMcpServer::new(f.base.api(), f.session.workspace_id.clone())
            .with_caller_agent_id(Some(f.session.id.clone()));
        if !absent {
            session.harness_version = "2.9".into();
            endpoint = endpoint.with_request_context(f.owner.mcp_context());
        }
        let value = tcp(
            endpoint.with_repository_guidance(&session, source),
            "return 'ordinary-legacy';",
        )
        .await;
        assert!(value.to_string().contains("ordinary-legacy"));
        assert!(!value.to_string().contains(FACTS));
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert_eq!(http.count(), 0);
    }
}

#[intent_test_macros::daemon_test(flavor = "multi_thread", worker_threads = 4)]
async fn context_output_actual_outer_settings_writers_release_metadata_before_snapshot_swap() {
    use crate::repository_context_live::tests::BlockingHold;
    use intent_core::WorkspaceApi;
    for mode in ["settings-write", "prepared-reload", "prepared-pin"] {
        let http = ReadServer::new().await;
        let f = LiveFixture::new(&http).await;
        let c = Arc::new(Control::default());
        let gate = Hold::new();
        *c.hold.lock().unwrap() = Some(gate.clone());
        let endpoint = controlled(&f, c.clone());
        let task = tokio::spawn(async move {
            tcp(endpoint, "await ws.pr.snapshot(4);return 'old-authority';").await
        });
        gate.reached().await;
        let hold = BlockingHold::new();
        let probe = hold.clone();
        *f.base
            .auth
            .registry
            .context_publication_probe
            .lock()
            .unwrap() = Some(Arc::new(move || probe.wait()));
        let service = f.base.auth.service.clone();
        let registry = f.base.auth.registry.clone();
        let writer = tokio::spawn(async move {
            match mode {
                "settings-write" => {
                    intent_core::with_caller(Caller::Daemon,service.settings_update(serde_json::json!([{"path":"sourceControl.gitlab.oauthClientId","value":"new-settings-client"}]))).await.unwrap();
                }
                "prepared-reload" => {
                    let text = std::fs::read_to_string(registry.config_path())
                        .unwrap()
                        .replace(
                            "oauthClientId = \"client\"",
                            "oauthClientId = \"new-reload-client\"",
                        );
                    assert!(text.contains("new-reload-client"));
                    service.apply_prepared_settings_reload(text).await.unwrap();
                }
                "prepared-pin" => {
                    let guard = service.gitlab_credential_gate.lock().await;
                    let candidate = registry
                        .preview(&[(
                            "sourceControl.gitlab.oauthClientId".into(),
                            serde_json::json!("new-pin-client"),
                        )])
                        .unwrap();
                    let write = service
                        .gitlab_credential_gate
                        .prepare_settings(&registry, &candidate, &guard)
                        .unwrap();
                    registry
                        .pin_with_repository_write(
                            "sourceControl.gitlab.oauthClientId",
                            serde_json::json!("new-pin-client"),
                            "--original-pin",
                            Some(&write),
                        )
                        .unwrap();
                    service
                        .settle_gitlab_repository_settings(Some(&write), true, false)
                        .await;
                }
                _ => unreachable!(),
            }
        });
        hold.reached().await;
        gate.resume();
        let reply = tokio::time::timeout(Duration::from_secs(15), task)
            .await
            .unwrap()
            .unwrap();
        assert!(
            reply
                .to_string()
                .contains("Private result delivery refused"),
            "{mode}: {reply}"
        );
        assert!(!reply.to_string().contains(FACTS));
        hold.release();
        writer.await.unwrap();
        f.owner.drain_jobs().await;
    }
}

#[intent_test_macros::daemon_test]
async fn context_output_original_missing_settings_attachment_omits_after_late_installation() {
    use crate::repository_admission::lifecycle::physical_owner::{
        RepositoryCreationIntent, RepositoryCreationOwner,
    };
    use crate::repository_admission::read_request::RepositoryReadOwner;
    use crate::repository_admission_source_tests::fixtures::Fixture as GitFixture;
    let git = GitFixture::new().await;
    git.git(
        &git.path,
        &[
            "remote",
            "add",
            "origin",
            "https://github.com/team/ordinary.git",
        ],
    );
    let registry = Arc::new(
        crate::SettingsRegistry::load(git.dir.path().join("context-settings.toml")).unwrap(),
    );
    let service = Arc::new(
        crate::Services::new_repository_fixture(
            git.store.clone(),
            intent_core::FileSecretStore::with_path(git.dir.path().join("context-secrets.json")),
            None,
        )
        .with_settings_registry(registry.clone())
        .with_workspaces_root(git.dir.path().join("workspaces")),
    );
    let session:intent_core::AgentSession=serde_json::from_value(serde_json::json!({"id":intent_core::AgentId::new(),"workspaceId":git.workspace.id,"name":"missing-boundary","status":"active","harnessVersion":"3.0","createdAt":"2026-09-28T00:00:00Z","updatedAt":"2026-09-28T00:00:00Z"})).unwrap();
    service.store.insert_agent_session(&session).await.unwrap();
    let lifecycle = service.repository_lifecycle_registry().await.unwrap();
    let physical = RepositoryCreationOwner::allocate(
        &lifecycle,
        &service.store,
        session.workspace_id.clone(),
        session.id.clone(),
        RepositoryCreationIntent::FirstSet,
    )
    .unwrap()
    .initialize(&service.store, || async {
        Ok("scripted ACP initialization".into())
    })
    .await
    .unwrap();
    let owner = RepositoryContextOwner::bind(
        service.clone(),
        RepositoryReadOwner::capture(service.clone()),
        &physical,
    )
    .unwrap();
    let caller = Caller::Agent {
        agent_id: session.id.clone(),
    };
    let guidance = intent_core::with_caller(caller.clone(), async {
        owner.capture_prompt().unwrap().prepare().await
    })
    .await;
    assert!(guidance.is_some());
    {
        let _guard = service.gitlab_credential_gate.lock().await;
        service
            .gitlab_credential_gate
            .install_settings_boundary(
                &registry,
                &service.secrets,
                &service.gitlab_secret_store,
                None,
            )
            .unwrap();
    }
    let (packet, ok) = intent_core::with_caller(caller, prompt_exchange(guidance, false)).await;
    assert!(ok);
    assert!(!packet.to_string().contains(FACTS));
    assert!(packet.to_string().contains("original user prompt"));
    owner.drain_jobs().await;
}

#[intent_test_macros::daemon_test]
async fn context_output_actual_same_binding_refresh_keeps_required_output_and_omits_old_facts() {
    use intent_sourcecontrol::gitlab_token::{EXPIRES_AT_SECRET_ACCOUNT, REFRESH_SECRET_ACCOUNT};
    let http = ReadServer::new().await;
    let f = LiveFixture::new(&http).await;
    f.base
        .auth
        .service
        .gitlab_secret_store
        .store(REFRESH_SECRET_ACCOUNT, "refresh-old")
        .unwrap();
    let expiry = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 7200;
    f.base
        .auth
        .service
        .gitlab_secret_store
        .store(EXPIRES_AT_SECRET_ACCOUNT, &expiry.to_string())
        .unwrap();
    f.base
        .auth
        .service
        .reconcile_gitlab_repository_binding()
        .await
        .unwrap();
    let original = f.base.auth.request();
    let c = Arc::new(Control::default());
    let gate = Hold::new();
    *c.hold.lock().unwrap() = Some(gate.clone());
    let endpoint = controlled(&f, c.clone());
    let task = tokio::spawn(async move {
        tcp(
            endpoint,
            "await ws.pr.snapshot(4);return 'required-refresh-current';",
        )
        .await
    });
    gate.reached().await;
    assert_eq!(c.events.lock().unwrap().as_slice(), &[2]);
    f.base
        .auth
        .service
        .gitlab_secret_store
        .store(EXPIRES_AT_SECRET_ACCOUNT, "0")
        .unwrap();
    f.base
        .auth
        .service
        .stored_proof_token(&crate::source_control_auth_ops::Target::Gitlab {
            host: http.fixture.host.clone(),
        })
        .await
        .unwrap();
    let current = f.base.auth.request();
    assert_eq!(current.binding, original.binding);
    assert!(current.secret_revision > original.secret_revision);
    let reads = http.count();
    gate.resume();
    let reply = task.await.unwrap();
    assert!(
        reply.to_string().contains("required-refresh-current"),
        "{reply}"
    );
    assert!(!reply.to_string().contains(FACTS));
    assert_eq!(http.count(), reads);
    f.owner.drain_jobs().await;
    let fresh = tcp(f.server(), "return 'fresh-facts';").await;
    assert!(fresh.to_string().contains(FACTS), "{fresh}");
    f.owner.drain_jobs().await;
}

#[intent_test_macros::daemon_test(flavor = "multi_thread", worker_threads = 4)]
async fn context_output_shared_provider_poison_refuses_required_but_omits_optional_only() {
    use crate::source_control_auth_ops::repository_owner::RepositoryConnectionFacts;
    for required in [false, true] {
        let http = ReadServer::new().await;
        let f = LiveFixture::new(&http).await;
        let c = Arc::new(Control::default());
        let gate = Hold::new();
        *c.hold.lock().unwrap() = Some(gate.clone());
        let endpoint = controlled(&f, c.clone());
        let task = tokio::spawn(async move {
            tcp(
                endpoint,
                if required {
                    "await ws.pr.snapshot(4);return 'required-poison';"
                } else {
                    "return 'ordinary-poison';"
                },
            )
            .await
        });
        gate.reached().await;
        assert_eq!(
            c.events.lock().unwrap().as_slice(),
            if required { &[2] } else { &[0] }
        );
        let facts = f
            .base
            .auth
            .service
            .gitlab_repository_connection_facts()
            .unwrap();
        assert!(std::thread::spawn(move || {
            RepositoryConnectionFacts::with_prompt_current(Some(&facts), |current| {
                assert!(current);
                panic!("negative actual shared metadata poison");
            })
            .unwrap();
        })
        .join()
        .is_err());
        gate.resume();
        let reply = tokio::time::timeout(Duration::from_secs(15), task)
            .await
            .unwrap()
            .unwrap();
        assert!(!reply.to_string().contains(FACTS));
        if required {
            assert!(
                reply
                    .to_string()
                    .contains("Private result delivery refused"),
                "{reply}"
            );
            assert!(!reply.to_string().contains("required-poison"));
        } else {
            assert!(reply.to_string().contains("ordinary-poison"), "{reply}");
        }
        f.owner.drain_jobs().await;
    }
}

#[intent_test_macros::daemon_test(flavor = "multi_thread", worker_threads = 4)]
async fn context_output_mixed_original_worktree_locks_omit_only_optional_and_keep_shared_required()
{
    for mode in ["optional-busy", "shared-current", "shared-retired"] {
        let http = ReadServer::new().await;
        let f = LiveFixture::new(&http).await;
        let root = f.root("mixed-lock-optional").await;
        let c = Arc::new(Control::default());
        let final_gate = Hold::new();
        *c.hold.lock().unwrap() = Some(final_gate.clone());
        let endpoint = controlled(&f, c.clone());
        let mut task = tokio::spawn(async move {
            tcp(
                endpoint,
                "await ws.pr.snapshot(4);return 'mixed-lock-result';",
            )
            .await
        });
        final_gate.reached().await;
        assert_eq!(c.events.lock().unwrap().as_slice(), &[2]);
        let lock_path = if mode == "optional-busy" {
            std::path::PathBuf::from(root.path)
        } else {
            f.base.git.path.clone()
        };
        let locks = f.base.auth.service.worktree_locks.clone();
        let held = Hold::new();
        let worker_hold = held.clone();
        let holder = tokio::spawn(async move {
            locks
                .with_lock(&lock_path, || async { worker_hold.wait().await })
                .await;
        });
        held.reached().await;
        final_gate.resume();
        if mode == "optional-busy" {
            let reply = tokio::time::timeout(Duration::from_secs(15), &mut task)
                .await
                .unwrap()
                .unwrap();
            assert!(reply.to_string().contains("mixed-lock-result"), "{reply}");
            assert!(!reply.to_string().contains(FACTS));
            held.resume();
            holder.await.unwrap();
        } else {
            assert!(
                tokio::time::timeout(Duration::from_millis(80), &mut task)
                    .await
                    .is_err(),
                "shared required lock cannot be bypassed"
            );
            if mode == "shared-retired" {
                f.physical.interrupt_requests();
            }
            held.resume();
            holder.await.unwrap();
            let reply = tokio::time::timeout(Duration::from_secs(15), task)
                .await
                .unwrap()
                .unwrap();
            if mode == "shared-retired" {
                assert!(
                    reply
                        .to_string()
                        .contains("Private result delivery refused"),
                    "{reply}"
                );
                assert!(!reply.to_string().contains("mixed-lock-result"));
            } else {
                assert!(reply.to_string().contains("mixed-lock-result"), "{reply}");
                assert!(reply.to_string().contains(FACTS), "{reply}");
            }
        }
        f.owner.drain_jobs().await;
    }
}

async fn manager_prompt_worker<T: Send + 'static>(
    services: Arc<crate::Services>,
    body: impl std::future::Future<Output = T> + Send + 'static,
) -> T {
    intent_core::spawn_daemon(crate::host_execution::background_execution(
        services.as_ref().clone(),
        None,
        body,
    ))
    .await
    .unwrap()
}

#[intent_test_macros::daemon_test]
async fn manager_prompt_output_actual_daemon_capture_prepare_and_separate_transfer() {
    for mode in [
        "current",
        "stale",
        "retired",
        "provider-changed",
        "consumer-error",
    ] {
        let http = ReadServer::new().await;
        let f = LiveFixture::new(&http).await;
        let services = f.owner.services.clone();
        let owner = f.owner.clone();
        let capture = manager_prompt_worker(services.clone(), async move {
            assert_eq!(current_caller(), Some(Caller::Daemon));
            assert!(
                owner.capture_prompt().is_err(),
                "expected strict Daemon baseline denial"
            );
            owner.capture_manager_prompt().unwrap()
        })
        .await;
        let guidance = manager_prompt_worker(services.clone(), async move {
            assert_eq!(current_caller(), Some(Caller::Daemon));
            let guidance = capture.prepare().await;
            assert_eq!(current_caller(), Some(Caller::Daemon));
            assert!(guidance.is_some());
            guidance
        })
        .await;
        match mode {
            "stale" => f.owner.invalidate(),
            "retired" => f.physical.interrupt_requests(),
            "provider-changed" => {
                f.base
                    .auth
                    .service
                    .gitlab_secret_store
                    .store(
                        intent_sourcecontrol::gitlab_token::SECRET_ACCOUNT,
                        "manager-replacement",
                    )
                    .unwrap();
                f.base
                    .auth
                    .service
                    .reconcile_gitlab_repository_binding()
                    .await
                    .unwrap();
            }
            _ => {}
        }
        let (frame, ok) = manager_prompt_worker(services, async move {
            assert_eq!(current_caller(), Some(Caller::Daemon));
            let result = prompt_exchange(guidance, mode == "consumer-error").await;
            assert_eq!(current_caller(), Some(Caller::Daemon));
            result
        })
        .await;
        assert_eq!(frame["method"], "session/prompt");
        assert!(frame.to_string().contains("original user prompt"));
        assert_eq!(
            frame.to_string().contains(FACTS),
            matches!(mode, "current" | "consumer-error"),
            "{mode}: {frame}"
        );
        assert_eq!(ok, mode != "consumer-error");
        assert_eq!(http.count(), 0);
        f.owner.drain_jobs().await;
    }
}

#[intent_test_macros::daemon_test]
async fn manager_prompt_output_preparation_construction_poll_and_final_caller_refusals() {
    use intent_core::caller::{with_caller, with_wire_credential, WireCredential};
    for mode in [
        "prepare-construction",
        "prepare-poll",
        "prepare-wire",
        "final-foreign",
        "final-wire",
        "final-unbound",
    ] {
        let http = ReadServer::new().await;
        let f = LiveFixture::new(&http).await;
        let services = f.owner.services.clone();
        let owner = f.owner.clone();
        let capture = manager_prompt_worker(services.clone(), async move {
            owner.capture_manager_prompt().unwrap()
        })
        .await;
        let escaped = capture.original.original().read().clone();
        let foreign = Caller::Agent {
            agent_id: intent_core::AgentId::new(),
        };
        let wire = WireCredential::Principal {
            principal_id: intent_core::PrincipalId::new(),
            token_hash: "original prompt test".into(),
        };
        let guidance = if mode == "prepare-construction" {
            let (future,) = with_caller(foreign.clone(), async { (capture.prepare(),) }).await;
            manager_prompt_worker(services.clone(), future).await
        } else if mode == "prepare-poll" {
            let (future,) =
                manager_prompt_worker(services.clone(), async { (capture.prepare(),) }).await;
            with_caller(foreign.clone(), future).await
        } else if mode == "prepare-wire" {
            let (future,) =
                manager_prompt_worker(services.clone(), async { (capture.prepare(),) }).await;
            with_wire_credential(Some(wire.clone()), future).await
        } else {
            manager_prompt_worker(services.clone(), async { capture.prepare().await }).await
        };
        assert_eq!(guidance.is_some(), mode.starts_with("final-"));
        let (frame, ok) = match mode {
            "final-foreign" => with_caller(foreign, prompt_exchange(guidance, false)).await,
            "final-wire" => {
                with_wire_credential(Some(wire), prompt_exchange(guidance, false)).await
            }
            "final-unbound" => tokio::spawn(prompt_exchange(guidance, false))
                .await
                .unwrap(),
            _ => manager_prompt_worker(services, prompt_exchange(guidance, false)).await,
        };
        assert!(ok);
        assert!(!frame.to_string().contains(FACTS), "{mode}: {frame}");
        assert!(frame.to_string().contains("original user prompt"));
        with_caller(
            Caller::Agent {
                agent_id: f.session.id.clone(),
            },
            async {
                assert!(
                    escaped.check_current().is_err(),
                    "last prompt owner has retired"
                );
            },
        )
        .await;
        f.owner.drain_jobs().await;
        assert_eq!(http.count(), 0);
    }
}

#[intent_test_macros::daemon_test]
async fn manager_prompt_output_unpolled_and_cancelled_preparation_retire_original_only() {
    use intent_core::caller::with_caller;
    for polled in [false, true] {
        let http = ReadServer::new().await;
        let f = LiveFixture::new(&http).await;
        let services = f.owner.services.clone();
        let owner = f.owner.clone();
        let capture = manager_prompt_worker(services.clone(), async move {
            owner.capture_manager_prompt().unwrap()
        })
        .await;
        let escaped = capture.original.original().read().clone();
        let (future,) =
            manager_prompt_worker(services.clone(), async move { (capture.prepare(),) }).await;
        if polled {
            let locks = services.worktree_locks.clone();
            let path = f.base.git.path.clone();
            let held = Hold::new();
            let h = held.clone();
            let holder =
                tokio::spawn(
                    async move { locks.with_lock(&path, || async { h.wait().await }).await },
                );
            held.reached().await;
            manager_prompt_worker(services.clone(), async move {
                let mut future = future;
                std::future::poll_fn(|cx| {
                    assert!(future.as_mut().poll(cx).is_pending());
                    std::task::Poll::Ready(())
                })
                .await;
                drop(future);
                assert_eq!(current_caller(), Some(Caller::Daemon));
            })
            .await;
            held.resume();
            holder.await.unwrap();
        } else {
            drop(future);
        }
        with_caller(
            Caller::Agent {
                agent_id: f.session.id.clone(),
            },
            async {
                assert!(escaped.check_current().is_err());
            },
        )
        .await;
        f.owner.drain_jobs().await;
        let (frame, ok) = manager_prompt_worker(services, prompt_exchange(None, false)).await;
        assert!(ok);
        assert!(!frame.to_string().contains(FACTS));
        assert!(f.owner.capture_prompt().is_err());
        assert_eq!(http.count(), 0);
    }
}

// This fixture creates the real private admission from the same captured
// manager request and factual preparation, so only the ambient entry is varied.
async fn manager_prompt_admission(owner: Arc<RepositoryContextOwner>) -> (String, PromptAdmission) {
    let original = PromptOrigin::Manager(owner.callback.capture_manager_prompt().unwrap());
    let prepared = original
        .run(Box::pin(async {
            let scope = original.original().read().capture_optional()?;
            scope.run_optional(|local| owner.prepare(local))?.await
        }))
        .await
        .unwrap();
    let facts = prepared.value().clone();
    let text = original
        .run(Box::pin(async { Ok(render_original(&facts).await) }))
        .await
        .unwrap()
        .unwrap();
    (
        text,
        PromptAdmission {
            original,
            optional: ContextOptional {
                prepared: Arc::new(prepared.map(|_| ())),
                facts,
            },
        },
    )
}

struct ManagerPromptAdmissionProbe {
    original: PromptAdmission,
    mode: &'static str,
    scope: Arc<dyn McpRequestScope>,
    transferred: Arc<std::sync::atomic::AtomicUsize>,
}
impl AcpPromptAdmission for ManagerPromptAdmissionProbe {
    fn admit<'a>(
        &'a self,
        boundary: &'a AcpPromptBoundary,
        packet: PreparedAcpPromptTransfer<'a>,
    ) -> BoxFuture<'a, AcpPromptAdmissionOutcome> {
        Box::pin(async move {
            use intent_core::caller::{with_caller, with_wire_credential, WireCredential};
            let wire = WireCredential::Principal {
                principal_id: intent_core::PrincipalId::new(),
                token_hash: "manager admission fixture".into(),
            };
            let future = match self.mode {
                "construct-foreign" => {
                    with_caller(
                        Caller::Agent {
                            agent_id: intent_core::AgentId::new(),
                        },
                        async { (self.original.admit(boundary, packet),) },
                    )
                    .await
                    .0
                }
                "construct-wire" => {
                    with_wire_credential(Some(wire.clone()), async {
                        (self.original.admit(boundary, packet),)
                    })
                    .await
                    .0
                }
                "construct-nested" => {
                    let mut future = None;
                    self.scope
                        .scope(Box::pin(async {
                            future = Some(self.original.admit(boundary, packet));
                        }))
                        .await;
                    future.unwrap()
                }
                _ => self.original.admit(boundary, packet),
            };
            let result = match self.mode {
                "poll-wire" => with_wire_credential(Some(wire), future).await,
                "poll-nested" => {
                    let mut result = None;
                    self.scope
                        .scope(Box::pin(async {
                            result = Some(future.await);
                        }))
                        .await;
                    result.unwrap()
                }
                _ => future.await,
            };
            assert_eq!(current_caller(), Some(Caller::Daemon));
            if matches!(result, AcpPromptAdmissionOutcome::Transferred(_)) {
                self.transferred
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            match self.mode {
                "after-panic" => panic!("after original manager transfer"),
                "after-omit" => AcpPromptAdmissionOutcome::OmitOptional,
                _ => result,
            }
        })
    }
}

#[intent_test_macros::daemon_test]
async fn manager_prompt_output_final_construction_poll_and_consumed_packet_never_replay() {
    for mode in [
        "construct-foreign",
        "construct-wire",
        "construct-nested",
        "poll-wire",
        "poll-nested",
        "after-panic",
        "after-omit",
    ] {
        let http = ReadServer::new().await;
        let f = LiveFixture::new(&http).await;
        let services = f.owner.services.clone();
        let owner = f.owner.clone();
        let (text, original) =
            manager_prompt_worker(services.clone(), manager_prompt_admission(owner)).await;
        let escaped = original.original.original().read().clone();
        let transferred = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let guidance = intent_acp::session::PromptGuidance::new(
            text,
            Box::new(ManagerPromptAdmissionProbe {
                original,
                mode,
                scope: f.owner.mcp_context().capture(),
                transferred: transferred.clone(),
            }),
        );
        let (frame, ok) = manager_prompt_worker(services, prompt_exchange(guidance, false)).await;
        let consumed = mode.starts_with("after-");
        assert!(ok, "a post-transfer callback failure cannot replay base");
        assert_eq!(
            frame.to_string().contains(FACTS),
            consumed,
            "{mode}: {frame}"
        );
        assert_eq!(
            transferred.load(std::sync::atomic::Ordering::SeqCst),
            usize::from(consumed)
        );
        assert!(frame.to_string().contains("original user prompt"));
        intent_core::with_caller(
            Caller::Agent {
                agent_id: f.session.id.clone(),
            },
            async {
                assert!(escaped.check_current().is_err());
            },
        )
        .await;
        f.owner.drain_jobs().await;
        assert_eq!(http.count(), 0);
    }
}
