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
    /// The session each cancel targeted.
    cancelled: Mutex<Vec<String>>,
    stops: AtomicUsize,
    responses: AtomicUsize,
    /// Prompts still to reject with a provider quota error, before output.
    quota_failures: AtomicUsize,
    /// What a quota-rejected prompt streams before its error: assistant
    /// text and, optionally, one completed tool call's command.
    before_quota: Mutex<Option<(String, Option<String>)>>,
    /// When set, the node reported the agent exited before it started:
    /// every prompt fails before output, as the production runtime does.
    exited: Mutex<Option<String>>,
    /// Prompts still to end as stopped before output, the runtime then not
    /// alive: a final stop's close, as on a machine removal.
    final_stops: AtomicUsize,
    /// The launch put the instructions in the provider session.
    session_prompt: AtomicBool,
    /// Prompts whose future is still alive (not returned, not dropped).
    in_flight: AtomicUsize,
}

/// Counts a live prompt future; an aborted worker drops it.
struct InFlightPrompt<'a>(&'a AtomicUsize);

impl Drop for InFlightPrompt<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
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
            cancelled: Mutex::new(Vec::new()),
            stops: AtomicUsize::new(0),
            responses: AtomicUsize::new(0),
            quota_failures: AtomicUsize::new(0),
            before_quota: Mutex::new(None),
            exited: Mutex::new(None),
            final_stops: AtomicUsize::new(0),
            session_prompt: AtomicBool::new(false),
            in_flight: AtomicUsize::new(0),
        })
    }

    fn stream_before_quota(&self, session_id: &str, text: &str, command: Option<&str>) {
        let mut updates = Vec::new();
        if let Some(command) = command {
            updates.push(json!({
                "sessionUpdate": "tool_call", "toolCallId": "pre-quota-tool", "title": "Bash",
                "kind": "execute", "status": "in_progress", "rawInput": {"command": command},
            }));
            updates.push(json!({
                "sessionUpdate": "tool_call_update", "toolCallId": "pre-quota-tool",
                "status": "completed", "rawOutput": format!("ran: {command}"),
            }));
        }
        updates.push(json!({
            "sessionUpdate": "agent_message_chunk",
            "content": {"type": "text", "text": text},
        }));
        for update in updates {
            self.updates
                .send(IncomingNotification {
                    method: "session/update".into(),
                    params: json!({"sessionId": session_id, "update": update}),
                })
                .unwrap();
        }
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

    fn session_has_system_prompt(&self) -> bool {
        self.session_prompt.load(Ordering::SeqCst)
    }

    fn prompt<'a>(
        &'a self,
        session_id: &'a str,
        prompt: Self::Prompt,
        _activity: &'a ActivityTracker,
    ) -> BoxFuture<'a, std::result::Result<PromptOutcome, AcpError>> {
        Box::pin(async move {
            self.in_flight.fetch_add(1, Ordering::SeqCst);
            let _in_flight = InFlightPrompt(&self.in_flight);
            self.prompts
                .lock()
                .unwrap()
                .push((session_id.to_owned(), prompt));
            if let Some(error) = self.exited.lock().unwrap().clone() {
                self.entered.notify_one();
                return Err(AcpError::Transport(format!(
                    "node-agent-not-running: {error}"
                )));
            }
            if self
                .quota_failures
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_ok()
            {
                let before = self.before_quota.lock().unwrap().clone();
                if let Some((text, command)) = before {
                    self.stream_before_quota(session_id, &text, command.as_deref());
                }
                self.entered.notify_one();
                return Err(AcpError::Rpc(intent_acp::JsonRpcError {
                    code: -32603,
                    message: "Internal error".into(),
                    data: Some(json!(
                        "{\"type\":\"rate_limit_error\",\"message\":\"usage limit reached\"}"
                    )),
                }));
            }
            if self
                .final_stops
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_ok()
            {
                self.alive.store(false, Ordering::SeqCst);
                self.entered.notify_one();
                return Ok(PromptOutcome {
                    stop_reason: StopReason::Cancelled,
                    usage: None,
                    meta: None,
                });
            }
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
        session_id: &'a str,
    ) -> BoxFuture<'a, std::result::Result<(), AcpError>> {
        Box::pin(async move {
            self.cancels.fetch_add(1, Ordering::SeqCst);
            self.cancelled.lock().unwrap().push(session_id.to_owned());
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
    let mut session = mgr
        .services
        .store
        .get_agent_session_with_transcript(id)
        .await
        .unwrap();
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
            session_mcp_servers: Vec::new(),
            spawned_model: None,
            session_open_filled_model: None,
            spawned_provider: "node".into(),
            spawned_selection: crate::agent_manager::test_selection_for_command("node"),
            spawned_policy: mgr.policy(),
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
            while mgr.is_busy(id) || mgr.slots.has_workers() {
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
            .get_agent_session_with_transcript(&id)
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

/// The cancel goes to the session bound on the live runtime handle, not to
/// the `acpSessionId` the store holds.
#[tokio::test]
async fn an_interrupt_cancels_the_live_turns_session_not_the_stored_one() {
    let script = mock_agent_script();
    let _env = EnvGuard::set_all(&[("MOCK_AGENT_SCRIPT_PATH", &script)]);
    let (_tmp, mgr) = manager().await;
    let mgr = Arc::new(mgr);
    let ws = WorkspaceId::from("ws-fake-bound");
    let id = AgentId::from("fake-bound");
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
    mgr.bind_live_session_for_test(&id, "live-session");

    assert!(mgr.interrupt(&id).await);
    assert_eq!(
        *fake.cancelled.lock().unwrap(),
        ["live-session"],
        "the cancel targets the handle's session, not the stored one"
    );
    assert!(
        mgr.contains(&id),
        "keep-alive interrupt retains the runtime"
    );
    assert!(fake.is_alive());
    mgr.stop(&id).await;
}

/// A live runtime whose agent stores no `acpSessionId` (a remote agent) is
/// still cancellable: an interrupt keeps it alive, and an interrupt-priority
/// send preempts its turn instead of queueing behind it.
#[tokio::test]
async fn a_live_runtime_with_no_stored_session_is_interrupted_and_preempted() {
    let ws = WorkspaceId::from("ws-remote-cancel");
    let id = AgentId::from("agent-remote-cancel");
    let fake = FakeRuntime::new();
    let port = Arc::new(RemoteRuntimes {
        runtime: Some(fake.clone()),
        asked: AtomicUsize::new(0),
    });
    let (_tmp, mgr) = remote_manager(port, &id, &ws).await;
    // A provider without the kill-on-interrupt quirk, so the runtime is kept.
    let mut session = mgr
        .services
        .store
        .get_agent_session_with_transcript(&id)
        .await
        .unwrap();
    session.provider = Some("claude-code".into());
    mgr.services
        .store
        .update_agent_session(&ws, &session)
        .await
        .unwrap();
    let send = |text: &str| {
        mgr.send_message(
            id.clone(),
            ws.clone(),
            text.into(),
            None,
            super::super::TurnOptions::default(),
        )
    };
    send("first").await.unwrap();
    fake.wait_prompts(1).await;
    let stored = mgr
        .services
        .store
        .get_agent_session_summary(&id)
        .await
        .unwrap();
    assert_eq!(stored.acp_session_id, None, "no session id is stored");

    assert!(mgr.interrupt(&id).await);
    let remote_session = format!("remote:{id}");
    assert_eq!(
        fake.cancelled.lock().unwrap().as_slice(),
        std::slice::from_ref(&remote_session)
    );
    assert!(
        mgr.contains(&id),
        "keep-alive interrupt retains the runtime"
    );
    assert!(fake.is_alive());
    wait_idle(&mgr, &id).await;

    send("second").await.unwrap();
    fake.wait_prompts(2).await;
    let preempting = mgr
        .interrupt_send_message(
            id.clone(),
            ws.clone(),
            "now".into(),
            None,
            super::super::TurnOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(
        preempting["queued"], false,
        "the interrupt send preempts the turn: {preempting}"
    );
    assert_eq!(
        *fake.cancelled.lock().unwrap(),
        [remote_session.clone(), remote_session]
    );
    fake.wait_prompts(3).await;
    fake.finish.add_permits(1);
    wait_idle(&mgr, &id).await;
    mgr.stop(&id).await;
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
    fn placement_help<'a>(
        &'a self,
        _svc: &'a Services,
        _workspace_id: &'a intent_core::WorkspaceId,
        _render: intent_core::machine_selection::PlacementHelpRenderer,
    ) -> BoxFuture<'a, Option<String>> {
        Box::pin(async { None })
    }
    fn admit<'a>(
        &'a self,
        _svc: &'a Services,
        _workspace_id: &'a WorkspaceId,
        _placement: &'a intent_core::placement::AgentPlacement,
        _needs: &'a intent_core::remote_launch::RemoteLaunchNeeds,
    ) -> BoxFuture<'a, intent_core::Result<intent_core::remote_launch::RemotePlacementView>> {
        Box::pin(async { Ok(remote_view()) })
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
        submodules: None,
        setup: None,
        setup_script: None,
        skills: None,
        shared_checkout_of: None,
        parent_work: None,
        launched_on: None,
        launched_machine_sign_in: None,
        cloud_start: None,
        home_checkout: None,
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

fn remote_view() -> intent_core::remote_launch::RemotePlacementView {
    intent_core::remote_launch::RemotePlacementView {
        node_id: "node-1".into(),
        lease_id: "lease-1".into(),
        node_name: "Mac".into(),
        placement: intent_core::placement::AgentPlacement::default(),
        effective_isolation: intent_core::remote_launch::EffectiveIsolation::Pending,
        placement_error: None,
        base_commit: "base".into(),
        submodules: None,
        setup: None,
        setup_script: None,
        skills: None,
        shared_checkout_of: None,
        parent_work: None,
        launched_on: None,
        launched_machine_sign_in: None,
        cloud_start: None,
        home_checkout: None,
    }
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
        .all(|(session, _)| *session == format!("remote:{id}")));
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

/// Review of intentd#2779: a live remote run is not reused once the
/// session's identity changed; it is stopped and the turn is refused.
#[tokio::test]
async fn a_live_remote_run_is_not_reused_after_an_identity_change() {
    let ws = WorkspaceId::from("ws-remote-identity");
    let id = AgentId::from("agent-remote-identity");
    let fake = FakeRuntime::new();
    let port = Arc::new(RemoteRuntimes {
        runtime: Some(fake.clone()),
        asked: AtomicUsize::new(0),
    });
    let (_tmp, mgr) = remote_manager(port.clone(), &id, &ws).await;
    mgr.ensure_started(&id, &ws).await.unwrap();
    assert!(mgr.contains(&id));
    let mut session = mgr
        .services
        .store
        .get_agent_session_with_transcript(&id)
        .await
        .unwrap();
    session.model = Some("another-model".into());
    mgr.services
        .store
        .update_agent_session(&ws, &session)
        .await
        .unwrap();
    let result = mgr.ensure_started(&id, &ws).await;
    assert!(
        matches!(&result, Err(Error::InvalidParams(m)) if m.starts_with("remote-model-change-unsupported")),
        "{result:?}"
    );
    assert!(!mgr.contains(&id), "the old run is not kept");
    assert_eq!(fake.stops.load(Ordering::SeqCst), 1);
    assert_eq!(port.asked.load(Ordering::SeqCst), 1);
}

/// Review of intentd#2779: a remote agent's instructions (an explicit
/// `behaviorPrompt`, or a specialist's frozen body and role) reach the node
/// with its first prompt, as a `<system>` prepend, and only once.
#[tokio::test]
async fn a_remote_agents_instructions_ride_its_first_prompt() {
    for specialist in [false, true] {
        let ws = WorkspaceId::from(format!("ws-remote-prompt-{specialist}").as_str());
        let id = AgentId::from(format!("agent-remote-prompt-{specialist}").as_str());
        let fake = FakeRuntime::new();
        let port = Arc::new(RemoteRuntimes {
            runtime: Some(fake.clone()),
            asked: AtomicUsize::new(0),
        });
        let (_tmp, mgr) = remote_manager(port, &id, &ws).await;
        let marker = if specialist {
            "SPECIALIST-BODY-MARKER"
        } else {
            "BEHAVIOR-PROMPT-MARKER"
        };
        let store = &mgr.services.store;
        store
            .set_agent_session_metadata_key(&ws, &id, "behaviorPrompt", marker, None, None)
            .await
            .unwrap();
        if specialist {
            let mut session = store.get_agent_session_with_transcript(&id).await.unwrap();
            session.specialist = Some("implementor".into());
            store.update_agent_session(&ws, &session).await.unwrap();
            store
                .set_agent_session_metadata_key(
                    &ws,
                    &id,
                    "specialistName",
                    "Implementor",
                    None,
                    None,
                )
                .await
                .unwrap();
        }
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
        let prompts = fake.prompts.lock().unwrap().clone();
        let first = serde_json::to_string(&prompts[0].1).unwrap();
        let second = serde_json::to_string(&prompts[1].1).unwrap();
        assert!(first.contains("<system>"), "{first}");
        assert!(first.contains(marker), "{first}");
        if specialist {
            assert!(first.contains("Implementor"), "{first}");
        }
        assert!(
            !second.contains(marker),
            "the instructions are sent once: {second}"
        );
        let persisted = store
            .get_agent_session_with_transcript(&id)
            .await
            .unwrap()
            .system_prompt;
        assert!(persisted.is_some_and(|p| p.contains(marker)));
    }
}

/// Review of intentd#2779: an explicit `behaviorPrompt` on a remote
/// delegation reaches the child's first prompt through the real delegate
/// path.
#[tokio::test]
async fn a_remote_delegations_behavior_prompt_rides_its_first_prompt() {
    let script = mock_agent_script();
    let _env = EnvGuard::set_all(&[("MOCK_AGENT_SCRIPT_PATH", &script)]);
    let ws = WorkspaceId::from("ws-remote-delegate");
    let parent = AgentId::from("agent-remote-delegate-parent");
    let fake = FakeRuntime::new();
    let port = Arc::new(RemoteRuntimes {
        runtime: Some(fake.clone()),
        asked: AtomicUsize::new(0),
    });
    let (_tmp, mgr) = remote_manager(port.clone(), &parent, &ws).await;
    let delegated = mgr
        .services
        .agent_delegate_op(
            ws.clone(),
            intent_core::AgentDelegateInput {
                task_text: Some("Do the remote thing".into()),
                provider: Some("mock".into()),
                behavior_prompt: Some("CUSTOM-INSTRUCTION".into()),
                placement: Some(serde_json::from_value(json!({"target": "remote"})).unwrap()),
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();
    let child = AgentId::from(delegated["agentId"].as_str().unwrap());
    fake.wait_prompts(1).await;
    fake.finish.add_permits(1);
    wait_idle(&mgr, &child).await;
    let first = serde_json::to_string(&fake.prompts.lock().unwrap()[0].1).unwrap();
    assert!(first.contains("<system>"), "{first}");
    assert!(first.contains("CUSTOM-INSTRUCTION"), "{first}");
}

/// Review of intentd#2779: when the instructions cannot be stored, the turn
/// fails before the node starts anything, and the next turn prepares again.
#[tokio::test]
async fn a_remote_run_never_starts_without_its_instructions() {
    let ws = WorkspaceId::from("ws-remote-prepare");
    let id = AgentId::from("agent-remote-prepare");
    let fake = FakeRuntime::new();
    let port = Arc::new(RemoteRuntimes {
        runtime: Some(fake.clone()),
        asked: AtomicUsize::new(0),
    });
    let (_tmp, mgr) = remote_manager(port.clone(), &id, &ws).await;
    let pool = mgr.services.store.write_pool();
    sqlx::query("CREATE TRIGGER fail_system_prompt BEFORE UPDATE OF system_prompt ON agent_session BEGIN SELECT RAISE(FAIL, 'injected'); END")
        .execute(pool)
        .await
        .unwrap();
    let failed = mgr.ensure_started(&id, &ws).await;
    assert!(
        matches!(&failed, Err(Error::Internal(m)) if m.contains("could not store its instructions")),
        "{failed:?}"
    );
    assert_eq!(
        port.asked.load(Ordering::SeqCst),
        0,
        "nothing started on the node"
    );
    assert!(!mgr.contains(&id));

    sqlx::query("DROP TRIGGER fail_system_prompt")
        .execute(pool)
        .await
        .unwrap();
    mgr.ensure_started(&id, &ws).await.unwrap();
    assert_eq!(port.asked.load(Ordering::SeqCst), 1);
    assert!(mgr.prepend_pending.lock().unwrap().contains(&id));
}

/// Remote parity S6: a message with an image and a file reaches the remote
/// agent's runtime: the image inline in the prompt, the file as the notice
/// that points at `ws.file.getAttachment` (which runs on the machine).
#[tokio::test]
async fn a_remote_agent_receives_a_message_with_attachments() {
    let script = mock_agent_script();
    let _env = EnvGuard::set_all(&[("MOCK_AGENT_SCRIPT_PATH", &script)]);
    let ws = WorkspaceId::from("ws-remote-attachments");
    let id = AgentId::from("agent-remote-attachments");
    let fake = FakeRuntime::new();
    let port = Arc::new(RemoteRuntimes {
        runtime: Some(fake.clone()),
        asked: AtomicUsize::new(0),
    });
    let (_tmp, mgr) = remote_manager(port.clone(), &id, &ws).await;
    fake.finish.add_permits(1);
    mgr.send_message(
        id.clone(),
        ws.clone(),
        "look".into(),
        None,
        super::super::TurnOptions {
            image_blocks: Some(serde_json::json!([
                { "data": "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg==", "mimeType": "image/png" }
            ])),
            file_blocks: Some(serde_json::json!([
                { "attachmentId": "att-1", "fileName": "notes.txt" }
            ])),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    fake.wait_prompts(1).await;
    let prompt = serde_json::to_value(&fake.prompts.lock().unwrap()[0].1).unwrap();
    let blocks = prompt.as_array().unwrap();
    assert!(
        blocks
            .iter()
            .any(|b| b["type"] == "image" && b["data"].as_str().is_some_and(|d| !d.is_empty())),
        "{prompt}"
    );
    assert!(
        blocks.iter().any(|b| b["type"] == "text"
            && b["text"]
                .as_str()
                .is_some_and(|t| t.contains("ws.file.getAttachment(\"att-1\")"))),
        "{prompt}"
    );
}

#[path = "remote_quota_failover_tests.rs"]
mod remote_quota_failover_tests;

/// Re-stamp a session's harness version (the full-row write never touches
/// the stamp, which is set once) on the codex provider.
async fn stamp_harness(store: &Store, id: &AgentId, version: &str) {
    sqlx::query("UPDATE agent_session SET harness_version=?, provider='codex' WHERE id=?")
        .bind(version)
        .bind(id.as_str())
        .execute(store.write_pool())
        .await
        .unwrap();
}

/// The `promptBreakdown` field `agent.getSession` serves for `id`.
async fn served_breakdown(mgr: &AgentManager, id: &AgentId) -> Value {
    mgr.services
        .agent_get_session_bounded_op(id.clone())
        .await
        .unwrap()["promptBreakdown"]
        .clone()
}

/// A remote agent's spawn records no prompt breakdown (its machine serves
/// the `workspace_api` description, which the backend cannot size), and
/// clears an earlier one: `agent.getSession` reports `not-recorded`. A
/// pre-3.10 session reports `harness-predates-breakdown` even with a
/// breakdown stored against its exact prompt.
#[tokio::test]
async fn a_remote_spawn_records_no_prompt_breakdown() {
    use intent_core::prompt_breakdown::PromptBreakdown;

    let ws = WorkspaceId::from("ws-breakdown");
    let id = AgentId::from("agent-breakdown");
    let port = Arc::new(RemoteRuntimes {
        runtime: None,
        asked: AtomicUsize::new(0),
    });
    let (_tmp, mgr) = remote_manager(port, &id, &ws).await;
    let store = &mgr.services.store;
    let newest = "3.10";
    let seeded = PromptBreakdown::new(newest, now_iso(), Vec::new(), Vec::new());
    stamp_harness(store, &id, newest).await;
    store
        .set_agent_session_system_prompt_with_breakdown(&ws, &id, "", Some(&seeded))
        .await
        .unwrap();
    assert_eq!(
        served_breakdown(&mgr, &id).await["available"],
        json!(true),
        "seeded breakdown is served"
    );
    let session = store.get_agent_session_with_transcript(&id).await.unwrap();
    mgr.prepare_remote_system_prompt(&id, &ws, &session)
        .await
        .unwrap();
    assert!(store
        .get_agent_session_with_transcript(&id)
        .await
        .unwrap()
        .system_prompt
        .is_some_and(|p| !p.is_empty()));
    assert_eq!(
        served_breakdown(&mgr, &id).await,
        json!({"available": false, "reason": "not-recorded"})
    );

    stamp_harness(store, &id, "3.8").await;
    let prompt = store
        .get_agent_session_with_transcript(&id)
        .await
        .unwrap()
        .system_prompt
        .unwrap();
    store
        .set_agent_session_system_prompt_with_breakdown(&ws, &id, &prompt, Some(&seeded))
        .await
        .unwrap();
    assert_eq!(
        served_breakdown(&mgr, &id).await,
        json!({"available": false, "reason": "harness-predates-breakdown"})
    );
}

/// Remote parity S1: a remote agent's instructions match what its machine
/// can serve. A truncating provider gets the condensed `ws.*` reference
/// (its machine serves the compact tool description that points at it);
/// the backend's skills catalog stays out (its paths are on the backend);
/// and the prompt keeps the private `$TMPDIR` promise, which the machine
/// keeps (remote parity S6).
#[tokio::test]
async fn a_remote_agents_prompt_matches_what_its_machine_serves() {
    let ws = WorkspaceId::from("ws-remote-prompt");
    let id = AgentId::from("agent-remote-prompt");
    let port = Arc::new(RemoteRuntimes {
        runtime: None,
        asked: AtomicUsize::new(0),
    });
    let (_tmp, mgr) = remote_manager(port, &id, &ws).await;
    let store = &mgr.services.store;
    let worktree = tempfile::TempDir::new().unwrap();
    let skill = worktree.path().join(".agents/skills/backend-only-skill");
    std::fs::create_dir_all(&skill).unwrap();
    std::fs::write(
        skill.join("SKILL.md"),
        "---\nname: backend-only-skill\ndescription: Lives on the backend\n---\n\nBody.\n",
    )
    .unwrap();
    let mut workspace = store.get_workspace(&ws).await.unwrap();
    workspace.worktree_path = Some(worktree.path().to_string_lossy().into_owned());
    store.update_workspace(&workspace).await.unwrap();

    let prompt_for = |provider: &'static str| {
        let mgr = mgr.clone();
        let (ws, id) = (ws.clone(), id.clone());
        async move {
            let mut session = mgr
                .services
                .store
                .get_agent_session_with_transcript(&id)
                .await
                .unwrap();
            session.provider = Some(provider.to_owned());
            mgr.prepare_remote_system_prompt(&id, &ws, &session)
                .await
                .unwrap();
            mgr.services
                .store
                .get_agent_session_with_transcript(&id)
                .await
                .unwrap()
                .system_prompt
                .unwrap_or_default()
        }
    };

    let truncating = prompt_for("claude-code").await;
    assert!(
        truncating.contains(intent_acp::WORKSPACE_API_SYSTEM_PROMPT_HEADING),
        "{truncating}"
    );
    assert!(truncating.contains("ws.note.read("), "{truncating}");
    assert!(!truncating.contains("<available_skills>"), "{truncating}");
    assert!(!truncating.contains("backend-only-skill"), "{truncating}");
    assert!(
        truncating.contains("is a private temp folder for this agent"),
        "{truncating}"
    );

    let full = prompt_for("codex").await;
    assert!(
        !full.contains(intent_acp::WORKSPACE_API_SYSTEM_PROMPT_HEADING),
        "a provider that reads the full tool description gets no copy: {full}"
    );
    assert!(!full.contains("<available_skills>"), "{full}");
}

/// A machine that put the instructions in the provider session
/// (nodeProtocol 21) gets no first-prompt copy of them, as a local Claude
/// Code session gets none; the first turn still asks for names.
#[tokio::test]
async fn a_remote_session_holding_the_instructions_gets_no_first_prompt_copy() {
    let ws = WorkspaceId::from("ws-remote-session-prompt");
    let id = AgentId::from("agent-remote-session-prompt");
    let fake = FakeRuntime::new();
    fake.session_prompt.store(true, Ordering::SeqCst);
    let port = Arc::new(RemoteRuntimes {
        runtime: Some(fake.clone()),
        asked: AtomicUsize::new(0),
    });
    let (_tmp, mgr) = remote_manager(port, &id, &ws).await;
    let store = &mgr.services.store;
    store
        .set_agent_session_metadata_key(&ws, &id, "behaviorPrompt", "BEHAVIOR-MARKER", None, None)
        .await
        .unwrap();
    let mut workspace = store.get_workspace(&ws).await.unwrap();
    workspace.title = String::new();
    store.update_workspace(&workspace).await.unwrap();

    mgr.send_message(
        id.clone(),
        ws.clone(),
        "first".into(),
        None,
        super::super::TurnOptions::default(),
    )
    .await
    .unwrap();
    fake.wait_prompts(1).await;
    fake.finish.add_permits(1);
    wait_idle(&mgr, &id).await;
    let first = serde_json::to_string(&fake.prompts.lock().unwrap()[0].1).unwrap();
    assert!(!first.contains("BEHAVIOR-MARKER"), "{first}");
    assert!(first.contains("ws.workspace.setTitle"), "{first}");
    let stored = store
        .get_agent_session_with_transcript(&id)
        .await
        .unwrap()
        .system_prompt;
    assert!(
        stored.is_some_and(|prompt| prompt.contains("BEHAVIOR-MARKER")),
        "the machine's session gets the stored instructions"
    );
}

/// A runner agent whose first turn failed before any output (a remote
/// failover that could not start the backup) left an empty assistant row;
/// its retry is still its first answered turn, so it is still asked to name
/// itself and its untitled workspace. The transcript is the one recorded on
/// an affected backend.
#[tokio::test]
async fn a_remote_agents_retry_after_an_empty_failed_turn_still_asks_for_names() {
    let ws = WorkspaceId::from("ws-remote-naming");
    let id = AgentId::from("agent-remote-naming");
    let fake = FakeRuntime::new();
    let port = Arc::new(RemoteRuntimes {
        runtime: Some(fake.clone()),
        asked: AtomicUsize::new(0),
    });
    let (_tmp, mgr) = remote_manager(port, &id, &ws).await;
    let store = &mgr.services.store;
    let mut workspace = store.get_workspace(&ws).await.unwrap();
    workspace.title = String::new();
    store.update_workspace(&workspace).await.unwrap();
    let mut session = store.get_agent_session_with_transcript(&id).await.unwrap();
    session.name = "Agent".into();
    session.name_explicitly_set = false;
    store.update_agent_session(&ws, &session).await.unwrap();
    let text = |t: &str| json!([{ "type": "text", "text": t }]);
    for (role, content) in [
        ("user", text("first")),
        (
            "system",
            text("invalid params: remote-failover-unavailable: the backup couldn't start"),
        ),
        ("assistant", json!([])),
    ] {
        store
            .append_agent_message(&id, role, &content, &now_iso())
            .await
            .unwrap();
    }

    mgr.send_message(
        id.clone(),
        ws.clone(),
        "try again?".into(),
        None,
        super::super::TurnOptions::default(),
    )
    .await
    .unwrap();
    fake.wait_prompts(1).await;
    fake.finish.add_permits(1);
    wait_idle(&mgr, &id).await;
    let first = serde_json::to_string(&fake.prompts.lock().unwrap()[0].1).unwrap();
    assert!(first.contains("ws.workspace.setTitle"), "{first}");
    assert!(first.contains("ws.workspace.setAgentName"), "{first}");
}
