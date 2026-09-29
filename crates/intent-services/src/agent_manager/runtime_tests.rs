//! The actual manager queue/turn/interrupt/reap paths with no ACP connection,
//! child process, bridge or PID. Only the execution side is replaced.

use super::super::runtime::{Notifications, RuntimeHandle};
use super::*;
use intent_acp::session::{ActivityTracker, ContentBlock, PromptOutcome, StopReason};
use intent_core::agent_runtime::AgentRuntime;
use intent_core::BoxFuture;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::sync::{Notify, Semaphore};

struct FakeRuntime {
    prompts: Mutex<Vec<(String, Vec<ContentBlock>)>>,
    entered: Notify,
    finish: Semaphore,
    notes: Notifications,
    updates: mpsc::UnboundedSender<IncomingNotification>,
    alive: AtomicBool,
    cancels: AtomicUsize,
    stops: AtomicUsize,
    responses: AtomicUsize,
}

impl FakeRuntime {
    fn new() -> Arc<Self> {
        let (updates, notes) = mpsc::unbounded_channel();
        Arc::new(Self {
            prompts: Mutex::new(Vec::new()),
            entered: Notify::new(),
            finish: Semaphore::new(0),
            notes: Arc::new(TokioMutex::new(notes)),
            updates,
            alive: AtomicBool::new(true),
            cancels: AtomicUsize::new(0),
            stops: AtomicUsize::new(0),
            responses: AtomicUsize::new(0),
        })
    }

    async fn wait_prompts(&self, count: usize) {
        timeout(Duration::from_secs(5), async {
            while self.prompts.lock().unwrap().len() < count {
                self.entered.notified().await;
            }
        })
        .await
        .expect("manager dispatches the expected prompt");
    }
}

impl AgentRuntime for FakeRuntime {
    type Prompt = Vec<ContentBlock>;
    type PromptOutcome = PromptOutcome;
    type Activity = ActivityTracker;
    type Error = AcpError;
    type Notifications = Notifications;

    fn prompt<'a>(
        &'a self,
        session_id: &'a str,
        prompt: Self::Prompt,
        _activity: &'a ActivityTracker,
    ) -> BoxFuture<'a, std::result::Result<PromptOutcome, AcpError>> {
        Box::pin(async move {
            self.prompts
                .lock()
                .unwrap()
                .push((session_id.to_owned(), prompt));
            self.updates
                .send(IncomingNotification {
                    method: "session/update".into(),
                    params: json!({"sessionId":session_id,"update":{
                        "sessionUpdate":"agent_message_chunk",
                        "content":{"type":"text","text":"fake runtime reply"}
                    }}),
                })
                .unwrap();
            self.entered.notify_one();
            self.finish.acquire().await.unwrap().forget();
            self.responses.fetch_add(1, Ordering::SeqCst);
            Ok(PromptOutcome {
                stop_reason: StopReason::EndTurn,
                usage: None,
                meta: None,
            })
        })
    }

    fn cancel<'a>(
        &'a self,
        _session_id: &'a str,
    ) -> BoxFuture<'a, std::result::Result<(), AcpError>> {
        Box::pin(async move {
            self.cancels.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
    }
    fn notifications(&self) -> Notifications {
        self.notes.clone()
    }
    fn is_alive(&self) -> bool {
        self.alive.load(Ordering::SeqCst)
    }
    fn client_request_seq(&self) -> u64 {
        0
    }
    fn response_seq(&self) -> u64 {
        self.responses.load(Ordering::SeqCst) as u64
    }
    fn await_response_after(&self, since: u64, _timeout: Duration) -> BoxFuture<'_, bool> {
        Box::pin(async move { self.response_seq() > since })
    }
    fn spawned_pid(&self) -> Option<u32> {
        None
    }
    fn root_pid(&self) -> Option<u32> {
        None
    }
    fn stop(&self) -> BoxFuture<'static, ()> {
        if self.alive.swap(false, Ordering::SeqCst) {
            self.stops.fetch_add(1, Ordering::SeqCst);
        }
        Box::pin(async {})
    }
}

async fn install_fake(mgr: &Arc<AgentManager>, id: &AgentId, ws: &WorkspaceId) -> Arc<FakeRuntime> {
    seed_agent(mgr, ws, id).await;
    let mut session = mgr.services.store.get_agent_session(id).await.unwrap();
    // Resolve the existing mock provider config but never launch its script.
    session.provider = Some("mock".into());
    mgr.services
        .store
        .update_agent_session(ws, &session)
        .await
        .unwrap();
    mgr.services
        .store
        .set_acp_session_id(ws, id, "fake-resumed-session")
        .await
        .unwrap();
    let runtime = FakeRuntime::new();
    mgr.handles.lock().unwrap().insert(
        id.clone(),
        AgentHandle {
            execution: RuntimeHandle::custom(runtime.clone()),
            antigravity_profile: None,
            session_mcp_servers: Vec::new(),
            spawned_model: None,
            spawned_provider: "node".into(),
            thought_level: None,
            config_options: None,
            confirmed_effort: None,
            wake_gate: Arc::new(AtomicUsize::new(0)),
            wake_listener: None,
        },
    );
    mgr.registry.register(id.clone(), mgr.make_kill(id.clone()));
    mgr.services.attach_agent_manager(mgr);
    runtime
}

async fn wait_idle(mgr: &AgentManager, id: &AgentId) {
    timeout(Duration::from_secs(5), async {
        while mgr.is_busy(id) || !mgr.workers.lock().unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("turn worker settles");
}

#[tokio::test]
async fn fake_runtime_drives_queued_delivery_streaming_and_idle_reap() {
    let script = mock_agent_script();
    let _env = EnvGuard::set_all(&[("MOCK_AGENT_SCRIPT_PATH", &script)]);
    let (_tmp, mgr) = manager().await;
    let mgr = Arc::new(mgr);
    let ws = WorkspaceId::from("ws-fake-queue");
    let id = AgentId::from("fake-queue");
    let fake = install_fake(&mgr, &id, &ws).await;
    assert!(mgr.handles.lock().unwrap()[&id]
        .execution
        .connection()
        .is_none());
    assert!(mgr.agent_root_pids().is_empty());
    assert_eq!(mgr.agent_spawn_details()[&id].root_pid, None);

    mgr.send_message(
        id.clone(),
        ws.clone(),
        "first request".into(),
        None,
        super::super::TurnOptions::default(),
    )
    .await
    .unwrap();
    fake.wait_prompts(1).await;
    let queued = mgr
        .send_message(
            id.clone(),
            ws.clone(),
            "queued follow-up".into(),
            None,
            super::super::TurnOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(queued["queued"], true);
    assert_eq!(
        fake.prompts.lock().unwrap().len(),
        1,
        "busy runtime is not prompted twice"
    );
    assert_eq!(
        mgr.reap_idle(None).await,
        0,
        "in-flight runtime cannot be evicted"
    );
    fake.finish.add_permits(1);
    fake.wait_prompts(2).await;
    {
        let prompts = fake.prompts.lock().unwrap();
        assert!(prompts.iter().all(|(sid, _)| sid == "fake-resumed-session"));
        assert!(serde_json::to_string(&prompts[0].1)
            .unwrap()
            .contains("first request"));
        assert!(serde_json::to_string(&prompts[1].1)
            .unwrap()
            .contains("queued follow-up"));
    }
    fake.finish.add_permits(1);
    wait_idle(&mgr, &id).await;
    assert!(mgr.services.queue_snapshot(&id).is_empty());
    let messages = mgr
        .services
        .store
        .get_agent_messages(&id, None)
        .await
        .unwrap();
    assert_eq!(messages.iter().filter(|m| m.role == "user").count(), 2);
    let answers: Vec<_> = messages.iter().filter(|m| m.role == "assistant").collect();
    assert_eq!(
        answers.len(),
        2,
        "runtime notifications reach durable transcript"
    );
    assert!(answers
        .iter()
        .all(|m| m.content.to_string().contains("fake runtime reply")));

    mgr.registry.set_last_active(&id, 1);
    assert_eq!(mgr.reap_idle_older_than(Duration::from_secs(60)).await, 1);
    assert_eq!(fake.stops.load(Ordering::SeqCst), 1);
    assert!(!mgr.contains(&id));
    assert!(!mgr.registry.is_registered(&id));
    assert_eq!(
        mgr.services
            .store
            .get_agent_session(&id)
            .await
            .unwrap()
            .acp_session_id
            .as_deref(),
        Some("fake-resumed-session")
    );
}

#[tokio::test]
async fn fake_runtime_interrupt_permissions_and_stop_need_no_local_handle() {
    let script = mock_agent_script();
    let _env = EnvGuard::set_all(&[("MOCK_AGENT_SCRIPT_PATH", &script)]);
    let (_tmp, mgr) = manager().await;
    let mgr = Arc::new(mgr.with_policy(PermissionPolicy::Interactive));
    let ws = WorkspaceId::from("ws-fake-cancel");
    let id = AgentId::from("fake-cancel");
    let fake = install_fake(&mgr, &id, &ws).await;
    mgr.send_message(
        id.clone(),
        ws.clone(),
        "cancel this".into(),
        None,
        super::super::TurnOptions::default(),
    )
    .await
    .unwrap();
    fake.wait_prompts(1).await;
    let mut permission = mgr.permissions.register(prompt("fake-permission", &id.0));
    assert!(mgr.respond_permission("fake-permission", PermissionOutcome::Cancelled));
    assert_eq!(permission.try_recv().unwrap(), PermissionOutcome::Cancelled);
    assert!(mgr.pending_permissions().is_empty());

    assert!(mgr.interrupt(&id).await);
    assert_eq!(fake.cancels.load(Ordering::SeqCst), 1);
    assert!(
        mgr.contains(&id),
        "keep-alive interrupt retains custom runtime"
    );
    assert!(fake.is_alive());
    assert!(!mgr.is_busy(&id));
    assert!(mgr.agent_root_pids().is_empty());
    assert!(mgr.stop(&id).await);
    assert!(!mgr.stop(&id).await);
    assert_eq!(fake.stops.load(Ordering::SeqCst), 1);
    assert!(!fake.is_alive());
}

#[tokio::test]
async fn fake_runtime_map_drop_stops_even_with_a_borrowed_runtime() {
    let (_tmp, mgr) = manager().await;
    let mgr = Arc::new(mgr);
    let id = AgentId::from("fake-drop");
    let fake = install_fake(&mgr, &id, &WorkspaceId::from("ws-fake-drop")).await;
    mgr.handles.lock().unwrap().clear();
    assert_eq!(fake.stops.load(Ordering::SeqCst), 1);
    assert!(!fake.is_alive());
}
