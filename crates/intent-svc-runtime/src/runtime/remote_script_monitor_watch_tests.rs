//! A remote agent's script monitor (a machine-run monitor) is a waiting
//! reason, as a home-run monitor is: an idle remote agent holding one stays
//! watchable, its idle does not settle its parent's watch, and the monitor's
//! silent end (a cancel that wakes nobody) settles that watch.
//!
//! The machine link is a fake; everything else is the real runtime, driven
//! through public requests and `agent:idle` events through the completion
//! delivery loop.

use std::sync::Arc;
use std::time::Duration;

use intent_core::events::AGENT_IDLE;
use intent_core::node_link::SCRIPT_PROTOCOL;
use intent_core::placement::{AgentPlacement, PlatformAvailability};
use intent_core::remote_launch::{RemoteLaunchNeeds, RemoteLaunchRequest, RemotePlacementView};
use intent_core::{
    now_iso, with_caller, ActorType, AgentCreateExtra, AgentId, AgentStatus, BoxFuture, Caller,
    EventActor, Result, ScriptCreateParams, ScriptMode, WorkspaceApi, WorkspaceId,
};
use intent_services::feature_api::feature_ports::{
    MachineScriptLink, RemoteLaunchPort, RemoteRuntime, UnavailableRemoteLaunch,
};
use intent_services::feature_api::test_support::fixtures::{
    test_registry_with_default_provider, workspace, TempDb,
};
use intent_services::feature_api::{publish_event, EventBus, Services};
use intent_store::{NewEvent, Store};
use serde_json::{json, Value};
use tokio::time::timeout;

use crate::runtime::RuntimeStateExt as _;
use crate::script_monitor::ScriptMonitorExt as _;
use crate::script_ops::machine_runs::RunPlace;

fn wait() -> Duration {
    intentd_test_support::test_timeout(Duration::from_secs(5))
}

/// Every agent's machine is "mac", reachable, and swallows script frames.
struct FakeMachine(UnavailableRemoteLaunch);

impl RemoteLaunchPort for FakeMachine {
    fn placements<'a>(&'a self, svc: &'a Services) -> BoxFuture<'a, Result<PlatformAvailability>> {
        self.0.placements(svc)
    }
    fn placement_help<'a>(&'a self, svc: &'a Services) -> BoxFuture<'a, Option<String>> {
        self.0.placement_help(svc)
    }
    fn admit<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: &'a WorkspaceId,
        placement: &'a AgentPlacement,
        needs: &'a RemoteLaunchNeeds,
    ) -> BoxFuture<'a, Result<RemotePlacementView>> {
        self.0.admit(svc, workspace_id, placement, needs)
    }
    fn launch<'a>(
        &'a self,
        svc: &'a Services,
        request: RemoteLaunchRequest,
    ) -> BoxFuture<'a, Result<()>> {
        self.0.launch(svc, request)
    }
    fn runtime<'a>(
        &'a self,
        svc: &'a Services,
        agent_id: &'a AgentId,
    ) -> BoxFuture<'a, Result<Arc<RemoteRuntime>>> {
        self.0.runtime(svc, agent_id)
    }
    fn machine_script_link<'a>(
        &'a self,
        _svc: &'a Services,
        _agent_id: &'a AgentId,
    ) -> BoxFuture<'a, Result<MachineScriptLink>> {
        Box::pin(async {
            Ok(MachineScriptLink {
                node_id: "mac".into(),
                protocol: SCRIPT_PROTOCOL,
                machine_name: "Mac".into(),
                machine_version: env!("CARGO_PKG_VERSION").into(),
                send: Arc::new(|_frame| Box::pin(async { Ok(()) })),
            })
        })
    }
}

struct Harness {
    _tmp: TempDb,
    svc: Services,
    bus: EventBus,
    ws: WorkspaceId,
    parent: AgentId,
    remote: AgentId,
    observer: AgentId,
    sentinel: AgentId,
    monitor_id: String,
    _delivery: tokio::task::JoinHandle<()>,
}

async fn agent(svc: &Services, ws: &WorkspaceId, name: &str) -> AgentId {
    let created = svc
        .agent_create(
            ws.clone(),
            Some(name.into()),
            None,
            None,
            None,
            None,
            AgentCreateExtra::default(),
        )
        .await
        .expect("agent.create");
    AgentId::from(created["agent"]["id"].as_str().expect("agent id"))
}

fn as_agent<F: std::future::Future>(
    agent: &AgentId,
    f: F,
) -> impl std::future::Future<Output = F::Output> {
    with_caller(
        Caller::Agent {
            agent_id: agent.clone(),
        },
        f,
    )
}

async fn messages(svc: &Services, agent: &AgentId) -> usize {
    svc.store()
        .get_agent_session_with_transcript(agent)
        .await
        .expect("session")
        .messages
        .len()
}

async fn messages_text(svc: &Services, agent: &AgentId) -> String {
    let session = svc
        .store()
        .get_agent_session_with_transcript(agent)
        .await
        .expect("session");
    let blocks: Vec<_> = session.messages.iter().map(|m| &m.content).collect();
    serde_json::to_string(&blocks).expect("serialize content blocks")
}

impl Harness {
    /// A parent and its idle remote child, whose machine run of "Mac tests"
    /// is under an active monitor.
    async fn new() -> Self {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let ws = WorkspaceId::new();
        store.insert_workspace(&workspace(&ws)).await.expect("ws");
        let bus = EventBus::new(store.clone());
        let svc = crate::test_runtime::with_runtime(Services::new_repository_fixture(
            store,
            intent_core::FileSecretStore::with_path(tmp.path.with_extension("secrets")),
            None,
        ))
        .with_settings_registry(test_registry_with_default_provider(&tmp))
        .with_event_bus(bus.clone())
        .with_remote_launch_port(Arc::new(FakeMachine(UnavailableRemoteLaunch)));
        let parent = agent(&svc, &ws, "Parent").await;
        let remote = agent(&svc, &ws, "Mac test runner").await;
        let observer = agent(&svc, &ws, "Observer").await;
        let sentinel = agent(&svc, &ws, "Sentinel").await;
        let delivery = svc.spawn_completion_delivery_loop();

        let script = svc
            .script_create(
                ws.clone(),
                ScriptCreateParams {
                    name: "Mac tests".into(),
                    command: "make test".into(),
                    mode: ScriptMode::Command,
                    ..Default::default()
                },
            )
            .await
            .expect("script.create");
        let script_id = script["id"].as_str().expect("script id").to_owned();
        let def = svc
            .store()
            .get_script_in_workspace(&ws, &script_id)
            .await
            .expect("read script")
            .expect("script");
        let place = RunPlace {
            workspace_id: ws.clone(),
            script_id,
            agent_id: remote.clone(),
        };
        let mgr = svc.script_manager();
        let runs = mgr.machine_runs().expect("machine runs");
        runs.start(&place, &def).await.expect("machine run starts");
        let reply = runs
            .monitor(&place, &json!({ "ttlMs": 600_000 }))
            .await
            .expect("machine monitor");
        assert_eq!(reply["ok"], true, "{reply}");
        assert_eq!(reply["monitor"]["remoteAgentId"], remote.as_str());
        let monitor_id = reply["monitor"]["monitorId"]
            .as_str()
            .expect("monitor id")
            .to_owned();

        // The remote agent's turn ended: it is idle, holding only the monitor.
        let mut session = svc
            .store()
            .get_agent_session_with_transcript(&remote)
            .await
            .expect("remote session");
        session.parent_agent_id = Some(parent.clone());
        session.status = AgentStatus::RuntimeIdle;
        svc.store()
            .update_agent_session(&ws, &session)
            .await
            .expect("mark remote idle");

        Self {
            _tmp: tmp,
            svc,
            bus,
            ws,
            parent,
            remote,
            observer,
            sentinel,
            monitor_id,
            _delivery: delivery,
        }
    }

    async fn watch(&self, caller: &AgentId, target: &AgentId) -> Result<Value> {
        as_agent(
            caller,
            self.svc
                .agent_watch(self.ws.clone(), caller.clone(), target.clone()),
        )
        .await
    }

    async fn idle(&self, agent: &AgentId, data: Value) {
        publish_event(
            Some(&self.bus),
            NewEvent {
                workspace_id: self.ws.clone(),
                timestamp: now_iso(),
                event_type: AGENT_IDLE.into(),
                actor: EventActor {
                    actor_type: ActorType::Agent,
                    id: Some(agent.0.clone()),
                    ..Default::default()
                },
                session_id: Some(agent.0.clone()),
                correlation_id: None,
                parent_event_id: None,
                metadata: None,
                data,
            },
        )
        .await;
    }

    /// The remote agent's turn-end idle, annotated as the emit sites do.
    async fn remote_idles(&self) {
        let mut data = json!({ "agentId": self.remote });
        self.svc
            .annotate_waiting_on_script_monitors(&self.remote, &mut data)
            .await;
        assert_eq!(
            data["waitingOnScriptMonitors"][0]["monitorId"], self.monitor_id,
            "the idle names the machine-run monitor: {data}"
        );
        self.idle(&self.remote, data).await;
    }

    /// Fence the in-order delivery loop: every idle published before this
    /// returns has been handled.
    async fn settle(&self) {
        let before = messages(&self.svc, &self.observer).await;
        self.watch(&self.observer, &self.sentinel)
            .await
            .expect("watch the sentinel");
        self.idle(&self.sentinel, json!({ "agentId": self.sentinel }))
            .await;
        timeout(wait(), async {
            while messages(&self.svc, &self.observer).await == before {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the sentinel idle was delivered");
    }

    /// The parent's live watches on the remote agent.
    async fn parent_watches(&self) -> usize {
        let response = as_agent(
            &self.parent,
            self.svc
                .agent_get_subscriptions(self.ws.clone(), self.parent.clone()),
        )
        .await
        .expect("agent.getSubscriptions");
        response["subscriptions"]
            .as_array()
            .expect("subscriptions array")
            .iter()
            .filter(|entry| entry["actorIds"] == json!([self.remote.as_str()]))
            .count()
    }
}

/// The "Full-pill taps" incident: an idle remote agent waiting on its Mac
/// test run was refused as "idle with nothing pending", and its idle settled
/// the parent's watch as completed.
#[intent_test_macros::daemon_test]
async fn an_idle_remote_agent_with_a_script_monitor_stays_watchable_and_unsettled() {
    let h = Harness::new().await;

    h.watch(&h.parent, &h.remote)
        .await
        .expect("an idle remote agent with a script monitor is watchable");
    h.remote_idles().await;
    h.settle().await;

    assert_eq!(h.parent_watches().await, 1, "the watch stays armed");
    assert_eq!(messages(&h.svc, &h.parent).await, 1, "one advisory");
    assert!(
        messages_text(&h.svc, &h.parent).await.contains("Mac tests"),
        "the advisory names the script the child waits on"
    );
    // Re-watching after the advisory is accepted too.
    h.watch(&h.parent, &h.remote)
        .await
        .expect("still watchable after the advisory");
}

/// A machine-run monitor cancelled without a wake (the app's cancel) is the
/// remote agent's last waiting reason: the deferred watch settles then, as a
/// home-run monitor's silent cancel settles it.
#[intent_test_macros::daemon_test]
async fn a_silently_cancelled_remote_script_monitor_settles_the_parents_watch() {
    let h = Harness::new().await;

    h.watch(&h.parent, &h.remote)
        .await
        .expect("watch the remote agent");
    h.remote_idles().await;
    h.settle().await;
    assert_eq!(h.parent_watches().await, 1);
    let advised = messages(&h.svc, &h.parent).await;

    let cancelled = h
        .svc
        .script_monitor_cancel(h.ws.clone(), h.monitor_id.clone(), None, false)
        .await
        .expect("scriptMonitor.cancel");
    assert_eq!(cancelled["ok"], true, "{cancelled}");
    assert!(
        h.svc
            .active_script_monitors_for_agent(&h.remote)
            .await
            .is_empty(),
        "the machine-run monitor ended"
    );

    timeout(wait(), async {
        while h.parent_watches().await != 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the silent cancel settles the parent's watch");
    assert_eq!(
        messages(&h.svc, &h.parent).await,
        advised + 1,
        "one completion wake"
    );
}
