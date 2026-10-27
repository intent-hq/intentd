//! The real runtime's script-monitor cancel redelivers a watched child's
//! completion exactly once, racing both a competing idle pass and the
//! detached `commit_monitor` → `dispatch_script_monitor` callback. Reauthored
//! from core's `real_runtime_script_monitor_cancel_redelivery_retires_watch_once`
//! (agent_ops tests; the same scenario against the runtime test double stays
//! there as `script_monitor_waiting_advisory_and_after_all_retire_on_silent_cancel`).
//!
//! Core is driven only through public requests: `agent.create`,
//! `app.agents.waitFor`, `agent.getSubscriptions`, and `agent:idle` events
//! through the completion delivery loop. The loop handles events in order, so
//! a sentinel idle for a separately watched agent, once its wake lands,
//! proves every earlier idle was handled. The claim park is the one core
//! test seam.

use std::sync::Arc;
use std::time::Duration;

use intent_core::events::AGENT_IDLE;
use intent_core::{
    now_iso, with_caller, ActorType, AgentCreateExtra, AgentId, Caller, EventActor,
    ScriptCreateParams, ScriptMode, WorkspaceApi, WorkspaceId,
};
use intent_store::{NewEvent, Store};
use serde_json::{json, Value};
use tokio::time::timeout;

use crate::runtime::RuntimeStateExt;
use crate::script_monitor::ScriptMonitorExt;
use crate::test_support::fixtures::{test_registry_with_default_provider, workspace, TempDb};
use crate::{publish_event, CompletionClassifyPark, EventBus, Services};

fn wait() -> Duration {
    intentd_test_support::test_timeout(Duration::from_secs(5))
}

struct Harness {
    svc: Services,
    bus: EventBus,
    ws: WorkspaceId,
    observer: AgentId,
    sentinel: AgentId,
}

impl Harness {
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

    async fn wait_for(&self, caller: &AgentId, targets: &[&AgentId], mode: &str) {
        with_caller(
            Caller::Agent {
                agent_id: caller.clone(),
            },
            self.svc.app_agents_wait(
                self.ws.clone(),
                caller.clone(),
                targets.iter().map(|t| t.0.clone()).collect(),
                Some(mode.into()),
            ),
        )
        .await
        .expect("app.agents.waitFor");
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

    /// Fence the delivery loop: every idle published before this returns has
    /// been handled.
    async fn settle(&self) {
        let before = messages(&self.svc, &self.observer).await;
        self.wait_for(&self.observer, &[&self.sentinel], "immediate")
            .await;
        self.idle(&self.sentinel, json!({"agentId": self.sentinel}))
            .await;
        timeout(wait(), async {
            while messages(&self.svc, &self.observer).await == before {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the sentinel idle was delivered");
    }

    async fn watches_child(&self, parent: &AgentId, child: &AgentId) -> bool {
        let subscriptions = with_caller(
            Caller::Agent {
                agent_id: parent.clone(),
            },
            self.svc
                .agent_get_subscriptions(self.ws.clone(), parent.clone()),
        )
        .await
        .expect("agent.getSubscriptions");
        subscriptions.to_string().contains(child.as_str())
    }

    async fn persisted_watches(&self, parent: &AgentId) -> usize {
        self.svc
            .store()
            .list_completion_watches()
            .await
            .expect("list persisted watches")
            .iter()
            .filter(|w| w.parent_agent_id == *parent)
            .count()
    }

    async fn wait_persisted_watches(&self, parent: &AgentId, expected: usize) {
        timeout(wait(), async {
            while self.persisted_watches(parent).await != expected {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("expected {expected} persisted watches"));
    }
}

async fn messages(svc: &Services, agent: &AgentId) -> usize {
    svc.store()
        .get_agent_session(agent)
        .await
        .expect("session")
        .messages
        .len()
}

async fn messages_text(svc: &Services, agent: &AgentId) -> String {
    let session = svc.store().get_agent_session(agent).await.expect("session");
    let blocks: Vec<_> = session.messages.iter().map(|m| &m.content).collect();
    serde_json::to_string(&blocks).expect("serialize content blocks")
}

#[intent_test_macros::daemon_test]
async fn script_monitor_cancel_redelivers_a_watched_childs_completion_once() {
    for grouped in [false, true] {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let ws = WorkspaceId::new();
        store.insert_workspace(&workspace(&ws)).await.expect("ws");
        let bus = EventBus::new(store.clone());
        let park = Arc::new(CompletionClassifyPark::default());
        // A per-test secrets file: `Services::new` would use the default
        // `~/intent/.secrets.json`.
        let svc = Services::new_repository_fixture(
            store,
            intent_core::FileSecretStore::with_path(tmp.path.with_extension("secrets")),
            None,
        )
        .with_settings_registry(test_registry_with_default_provider(&tmp))
        .with_event_bus(bus.clone());
        let svc = if grouped {
            svc
        } else {
            svc.with_completion_claim_park_for_test(park.clone())
        };
        let parent = Harness::agent(&svc, &ws, "Parent").await;
        let child = Harness::agent(&svc, &ws, "Child").await;
        let observer = Harness::agent(&svc, &ws, "Observer").await;
        let sentinel = Harness::agent(&svc, &ws, "Sentinel").await;
        let delivery = svc.spawn_completion_delivery_loop();
        let h = Harness {
            svc: svc.clone(),
            bus,
            ws: ws.clone(),
            observer,
            sentinel,
        };

        let script = svc
            .script_create(
                ws.clone(),
                ScriptCreateParams {
                    name: "Monitor check".into(),
                    command: "true".into(),
                    mode: ScriptMode::Command,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let sid = script["id"].as_str().unwrap().to_owned();
        svc.store()
            .admit_script_run(&ws, &sid, "accepted-reservation")
            .await
            .unwrap();
        let monitor = svc
            .script_monitor(ws.clone(), child.clone(), sid, json!({"ttlMs":60000}))
            .await
            .unwrap();
        h.wait_for(
            &parent,
            &[&child],
            if grouped { "after_all" } else { "immediate" },
        )
        .await;
        let mut data = json!({"agentId": child});
        svc.annotate_waiting_on_script_monitors(&child, &mut data)
            .await;
        assert_eq!(
            data["waitingOnScriptMonitors"][0]["monitorId"],
            monitor["monitor"]["monitorId"]
        );
        h.idle(&child, data.clone()).await;
        h.idle(&child, data).await;
        if grouped {
            h.idle(&parent, json!({"agentId": parent})).await;
        }
        h.settle().await;
        assert_eq!(messages(&svc, &parent).await, 1, "one advisory");
        assert!(messages_text(&svc, &parent).await.contains("Monitor check"));
        assert!(h.watches_child(&parent, &child).await);
        h.wait_persisted_watches(&parent, 1).await;

        let cancellation = tokio::spawn({
            let svc = svc.clone();
            let ws = ws.clone();
            let child = child.clone();
            async move {
                svc.cancel_script_monitors(&ws, Some(&child), "owner-retired")
                    .await
                    .unwrap();
            }
        });
        if !grouped {
            timeout(wait(), park.entered.notified())
                .await
                .expect("cancellation redelivery claims the watch");
            // The competing idle pass must yield to the claimed delivery.
            h.idle(&child, json!({"agentId": child})).await;
            h.settle().await;
            assert!(h.watches_child(&parent, &child).await);
            assert_eq!(messages(&svc, &parent).await, 1);
            park.release.notify_one();
        }
        timeout(wait(), cancellation).await.unwrap().unwrap();
        // The cancel's cleanup committed the monitor, which spawned a
        // detached dispatch_script_monitor; wait for that callback too.
        timeout(
            wait(),
            svc.runtime().script_locks.monitor_dispatched.notified(),
        )
        .await
        .expect("commit_monitor's dispatch callback finished");
        // The winner commits durable retirement before removing the
        // in-memory watch: await both.
        timeout(wait(), async {
            while h.watches_child(&parent, &child).await {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("cancellation redelivery retires the watch");
        h.wait_persisted_watches(&parent, 0).await;
        assert_eq!(messages(&svc, &parent).await, 2, "advisory + completion");
        assert!(svc
            .active_script_monitors_for_agent(&child)
            .await
            .is_empty());
        delivery.abort();
    }
}
