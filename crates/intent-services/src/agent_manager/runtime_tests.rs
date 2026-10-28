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
        timeout(
            intentd_test_support::test_timeout(Duration::from_secs(5)),
            async {
                while self.prompts.lock().unwrap().len() < count {
                    self.entered.notified().await;
                }
            },
        )
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
            repository_origin: crate::agent_manager::RepositoryOrigin::unavailable(),
            antigravity_profile: None,
            session_mcp_servers: Vec::new(),
            spawned_model: None,
            session_open_filled_model: None,
            spawned_provider: "node".into(),
            spawned_selection: crate::agent_manager::test_selection_for_command("node"),
            _activation: None,
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
    timeout(
        intentd_test_support::test_timeout(Duration::from_secs(5)),
        async {
            while mgr.is_busy(id) || !mgr.workers.lock().unwrap().is_empty() {
                tokio::task::yield_now().await;
            }
        },
    )
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

/// A remote-launch port whose agents all run on `runtime`.
struct RemoteRuntimes {
    runtime: Option<Arc<FakeRuntime>>,
    asked: AtomicUsize,
}

impl crate::feature_ports::RemoteLaunchPort for RemoteRuntimes {
    fn placements<'a>(
        &'a self,
        _svc: &'a Services,
    ) -> BoxFuture<'a, intent_core::Result<intent_core::placement::PlatformAvailability>> {
        Box::pin(async { Ok(intent_core::placement::PlatformAvailability::none(now_iso())) })
    }
    fn placement_help<'a>(&'a self, _svc: &'a Services) -> BoxFuture<'a, Option<String>> {
        Box::pin(async { None })
    }
    fn admit<'a>(
        &'a self,
        _svc: &'a Services,
        _workspace_id: &'a WorkspaceId,
        _placement: &'a intent_core::placement::AgentPlacement,
    ) -> BoxFuture<'a, intent_core::Result<intent_core::remote_launch::RemotePlacementView>> {
        Box::pin(async { Err(Error::Internal("not used".into())) })
    }
    fn launch<'a>(
        &'a self,
        _svc: &'a Services,
        _request: intent_core::remote_launch::RemoteLaunchRequest,
    ) -> BoxFuture<'a, intent_core::Result<()>> {
        Box::pin(async { Ok(()) })
    }
    fn runtime<'a>(
        &'a self,
        _svc: &'a Services,
        _agent_id: &'a AgentId,
    ) -> BoxFuture<'a, intent_core::Result<Arc<crate::feature_ports::RemoteRuntime>>> {
        self.asked.fetch_add(1, Ordering::SeqCst);
        let runtime = self.runtime.clone();
        Box::pin(async move {
            runtime
                .map(|r| r as Arc<crate::feature_ports::RemoteRuntime>)
                .ok_or_else(|| Error::InvalidParams("node-unavailable: no node".into()))
        })
    }
}

/// A manager whose services use `port` for remote launch, with agent `id`
/// placed on a node.
async fn remote_manager(
    port: Arc<RemoteRuntimes>,
    id: &AgentId,
    ws: &WorkspaceId,
) -> (TempDb, Arc<AgentManager>) {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let bus = EventBus::new(store.clone());
    let services = Services::new_with_file_secrets(
        store,
        intent_core::FileSecretStore::with_path(tmp.path.with_extension("secrets.json")),
    )
    .with_event_bus(bus.clone())
    .with_remote_launch_port(port);
    let sink: Arc<dyn EventSink> = Arc::new(BusEventSink::new(bus));
    let mgr = Arc::new(AgentManager::new(services, sink, 8));
    seed_agent(&mgr, ws, id).await;
    let view = intent_core::remote_launch::RemotePlacementView {
        node_id: "node-1".into(),
        lease_id: "lease-1".into(),
        node_name: "Mac".into(),
        placement: intent_core::placement::AgentPlacement::default(),
        effective_isolation: intent_core::remote_launch::EffectiveIsolation::Pending,
        placement_error: None,
        base_commit: "base".into(),
    };
    mgr.services
        .store
        .set_agent_session_metadata_key_json(
            ws,
            id,
            intent_core::remote_launch::REMOTE_PLACEMENT_METADATA_KEY,
            &serde_json::to_string(&view).unwrap(),
            &now_iso(),
        )
        .await
        .unwrap();
    mgr.services.attach_agent_manager(&mgr);
    (tmp, mgr)
}

/// A placed agent's turns run on the runtime the remote-launch port hands
/// out (asked once, then reused), never on a local process; transcripts
/// are written as for a local agent.
#[tokio::test]
async fn a_placed_agent_runs_its_turns_on_the_remote_runtime() {
    let ws = WorkspaceId::from("ws-remote-turns");
    let id = AgentId::from("agent-remote-turns");
    let fake = FakeRuntime::new();
    let port = Arc::new(RemoteRuntimes {
        runtime: Some(fake.clone()),
        asked: AtomicUsize::new(0),
    });
    let (_tmp, mgr) = remote_manager(port.clone(), &id, &ws).await;

    for (turn, text) in [(1, "first"), (2, "second")] {
        mgr.send_message(
            id.clone(),
            ws.clone(),
            text.into(),
            None,
            super::super::TurnOptions::default(),
        )
        .await
        .unwrap();
        fake.wait_prompts(turn).await;
        fake.finish.add_permits(1);
        wait_idle(&mgr, &id).await;
    }
    assert_eq!(
        port.asked.load(Ordering::SeqCst),
        1,
        "the live runtime is reused"
    );
    assert!(fake
        .prompts
        .lock()
        .unwrap()
        .iter()
        .all(|(session, _)| session == "remote:agent-remote-turns"));
    assert!(mgr.agent_root_pids().is_empty(), "no local process");
    let messages = mgr
        .services
        .store
        .get_agent_messages(&id, None)
        .await
        .unwrap();
    assert_eq!(
        messages.iter().filter(|m| m.role == "assistant").count(),
        2,
        "the remote runtime's output reaches the transcript"
    );

    // Stopping the agent stops its remote runtime.
    assert!(mgr.stop(&id).await);
    assert_eq!(fake.stops.load(Ordering::SeqCst), 1);
}

/// With no node to run it, a placed agent's turn fails; it never falls
/// back to a local process.
#[tokio::test]
async fn a_placed_agent_never_starts_locally() {
    let ws = WorkspaceId::from("ws-remote-none");
    let id = AgentId::from("agent-remote-none");
    let port = Arc::new(RemoteRuntimes {
        runtime: None,
        asked: AtomicUsize::new(0),
    });
    let (_tmp, mgr) = remote_manager(port.clone(), &id, &ws).await;
    let result = mgr.ensure_started(&id, &ws).await;
    assert!(
        matches!(&result, Err(Error::InvalidParams(m)) if m.starts_with("node-unavailable")),
        "{result:?}"
    );
    assert_eq!(port.asked.load(Ordering::SeqCst), 1);
    assert!(!mgr.contains(&id));
    assert!(mgr.agent_root_pids().is_empty());
}
