use super::*;
use crate::tests::{workspace, TempDb};
use crate::{EventBus, SubscriptionFilter};
use intent_core::{
    AgentReverseDispatch, BoxFuture, ClientId, ReverseDispatchError, ReverseTarget, WorkspaceApi,
};

struct Executor {
    connection: Mutex<DesktopConnection>,
    extra_connections: Mutex<Vec<DesktopConnection>>,
    calls: Mutex<Vec<Value>>,
    fail: Mutex<Option<String>>,
    hold_start: std::sync::atomic::AtomicBool,
    start_seen: tokio::sync::Notify,
    release_start: tokio::sync::Notify,
    result: Mutex<Option<Value>>,
}
impl AgentReverseDispatch for Executor {
    fn desktop_candidates(&self, principal: &PrincipalId) -> Vec<DesktopConnection> {
        std::iter::once(self.connection.lock().unwrap().clone())
            .chain(self.extra_connections.lock().unwrap().clone())
            .filter(|c| &c.principal_id == principal)
            .collect()
    }
    fn is_connected(&self) -> bool {
        true
    }
    fn dispatch<'a>(
        &'a self,
        _method: &'a str,
        _params: Value,
        _target: ReverseTarget,
    ) -> BoxFuture<'a, Result<Value, ReverseDispatchError>> {
        Box::pin(async { panic!("desktop must not use generic reverse dispatch") })
    }
    fn desktop_resolve(
        &self,
        target: &ReverseTarget,
        principal: &PrincipalId,
    ) -> DesktopResult<DesktopConnection> {
        let connection = match target {
            ReverseTarget::Default => self.connection.lock().unwrap().clone(),
            ReverseTarget::Client(client) | ReverseTarget::Pinned(client) => {
                std::iter::once(self.connection.lock().unwrap().clone())
                    .chain(self.extra_connections.lock().unwrap().clone())
                    .find(|c| &c.client_id == client)
                    .ok_or_else(|| error("desktop-offline", "Missing client"))?
            }
        };
        if &connection.principal_id != principal {
            return Err(error("forbidden", "Foreign principal"));
        }
        Ok(connection)
    }
    fn desktop_dispatch(
        &self,
        connection: DesktopConnection,
        params: Value,
    ) -> BoxFuture<'_, DesktopResult<Value>> {
        Box::pin(async move {
            if connection != *self.connection.lock().unwrap()
                && !self.extra_connections.lock().unwrap().contains(&connection)
            {
                return Err(error("desktop-offline", "Connection changed"));
            }
            self.calls.lock().unwrap().push(params.clone());
            if self.fail.lock().unwrap().as_deref() == params["operation"].as_str() {
                return Err(error("desktop-execution-failed", "Native failure"));
            }
            if params["operation"] == "startControl"
                && self.hold_start.load(std::sync::atomic::Ordering::Relaxed)
            {
                self.start_seen.notify_one();
                self.release_start.notified().await;
            }
            Ok(match params["operation"].as_str().unwrap() {
                "prepare" => {
                    json!({"computerId":if connection.client_id.as_str()=="primary" {"physical"} else {"second-physical"},"computerName":connection.client_id,"platform":"macos"})
                }
                "startControl" => {
                    json!({"ready":true,"sessionId":params["sessionId"],"computerId":params["computerId"]})
                }
                "renew" => json!({"renewed":true,"sessionId":params["sessionId"]}),
                "endControl" => json!({"ended":true,"sessionId":params["sessionId"]}),
                "prepareCommand" => {
                    json!({"commandId":params["commandId"],"sequence":params["sequence"],"deadlineId":"ticket","expiresInMs":10000})
                }
                "execute" => {
                    json!({"commandId":params["commandId"],"sequence":params["sequence"],"result":self.result.lock().unwrap().clone().unwrap_or(json!({"ok":true}))})
                }
                other => panic!("unexpected operation {other}"),
            })
        })
    }
}
struct Harness {
    _tmp: TempDb,
    services: Services,
    workspace: WorkspaceId,
    agent: AgentId,
    executor: Arc<Executor>,
    owner: Caller,
}
impl Harness {
    async fn new() -> Self {
        let tmp = TempDb::new();
        let store = intent_store::Store::open(&tmp.path).await.unwrap();
        let ws = WorkspaceId::new();
        store.insert_workspace(&workspace(&ws)).await.unwrap();
        store
            .set_workspace_browser_client(&ws, Some(&ClientId::from("primary")))
            .await
            .unwrap();
        let principal = store.get_primary_principal().await.unwrap().id;
        let owner = Caller::Wire {
            principal_id: principal.clone(),
            host_role: intent_core::HostRole::Owner,
        };
        let executor = Arc::new(Executor {
            connection: Mutex::new(DesktopConnection {
                client_id: ClientId::from("primary"),
                principal_id: principal,
                connection_epoch: "epoch".into(),
            }),
            calls: Mutex::new(vec![]),
            extra_connections: Mutex::new(vec![]),
            fail: Mutex::new(None),
            hold_start: std::sync::atomic::AtomicBool::default(),
            start_seen: tokio::sync::Notify::default(),
            release_start: tokio::sync::Notify::default(),
            result: Mutex::new(None),
        });
        let settings =
            Arc::new(crate::SettingsRegistry::load(tmp.path.with_extension("toml")).unwrap());
        settings
            .apply(&[
                ("model.defaultProvider".into(), json!("auggie")),
                ("providers.paths".into(), json!({"auggie":"/bin/sh"})),
            ])
            .unwrap();
        let services = Services::new(store.clone())
            .with_event_bus(EventBus::new(store))
            .with_settings_registry(settings)
            .with_reverse_dispatch(executor.clone());
        let created = intent_core::with_caller(
            owner.clone(),
            services.agent_create(
                ws.clone(),
                Some("Desktop agent".into()),
                None,
                None,
                None,
                None,
                intent_core::AgentCreateExtra::default(),
            ),
        )
        .await
        .unwrap();
        let agent = AgentId::from(created["agent"]["id"].as_str().expect("created agent ID"));
        Self {
            _tmp: tmp,
            services,
            workspace: ws,
            agent,
            executor,
            owner,
        }
    }
    async fn agent(&self, method: &str, args: Value) -> DesktopResult<Value> {
        intent_core::with_caller(
            Caller::Agent {
                agent_id: self.agent.clone(),
            },
            self.services
                .desktop_agent_op(self.workspace.clone(), method.into(), args),
        )
        .await
    }
    async fn client(&self, method: &str, mut args: Value) -> DesktopResult<Value> {
        args["workspaceId"] = self.workspace.0.clone().into();
        let connection = self.executor.connection.lock().unwrap().clone();
        intent_core::with_caller(
            self.owner.clone(),
            self.services
                .desktop_client_op(method.into(), args, connection),
        )
        .await
    }
    async fn remember(&self) {
        self.client(
            "setPermission",
            json!({"agentId":self.agent,"computerId":"physical","allowed":true}),
        )
        .await
        .unwrap();
    }
}
#[tokio::test]
async fn repeated_pending_start_withdrawal_and_stale_approval() {
    let h = Harness::new().await;
    let first = h.agent("startControl", json!({})).await.unwrap();
    assert_eq!(first["status"], "pending_permission");
    assert_eq!(first["message"], PENDING_HINT);
    assert_eq!(h.agent("startControl", json!({})).await.unwrap(), first);
    assert_eq!(
        h.agent("endControl", json!({})).await.unwrap(),
        json!({"ended":false,"withdrawn":true})
    );
    assert_eq!(
        h.client(
            "respondPermission",
            json!({"requestId":first["requestId"],"decision":"allow_future"})
        )
        .await
        .unwrap_err()
        .code,
        "desktop-stale-request"
    );
    assert_eq!(
        h.agent("endControl", json!({})).await.unwrap(),
        json!({"ended":false,"withdrawn":false})
    );
    assert!(!h
        .executor
        .calls
        .lock()
        .unwrap()
        .iter()
        .any(|c| c["operation"] == "startControl"));
}
#[tokio::test]
async fn remembered_start_repeated_start_action_tickets_and_end_results() {
    let h = Harness::new().await;
    h.remember().await;
    let started = h.agent("startControl", json!({})).await.unwrap();
    assert_eq!(started["alreadyGranted"], false);
    assert_eq!(started["hint"], RELEASE_HINT);
    let repeated = h.agent("startControl", json!({})).await.unwrap();
    assert_eq!(repeated["alreadyGranted"], true);
    assert_eq!(repeated["message"], "Control is already granted");
    assert_eq!(repeated["sessionId"], started["sessionId"]);
    assert_eq!(
        h.agent("type", json!({"text":"こんにちは"})).await.unwrap(),
        json!({"ok":true})
    );
    let calls = h.executor.calls.lock().unwrap().clone();
    let prepare = calls
        .iter()
        .find(|c| c["operation"] == "prepareCommand")
        .unwrap();
    let execute = calls.iter().find(|c| c["operation"] == "execute").unwrap();
    assert_eq!(
        prepare["action"],
        json!({"kind":"type","text":"こんにちは"})
    );
    assert!(execute.get("action").is_none());
    assert_eq!(execute["deadlineId"], "ticket");
    assert_eq!(
        calls
            .iter()
            .filter(|c| c["operation"] == "startControl")
            .count(),
        1
    );
    assert_eq!(
        h.agent("endControl", json!({})).await.unwrap(),
        json!({"ended":true,"withdrawn":false})
    );
    assert_eq!(
        h.agent("type", json!({"text":"no"}))
            .await
            .unwrap_err()
            .code,
        "desktop-not-active"
    );
    assert_eq!(h.services.desktop.state(&h.agent), DesktopState::Inactive);
    assert_eq!(
        h.agent("endControl", json!({})).await.unwrap(),
        json!({"ended":false,"withdrawn":false})
    );
}
#[tokio::test]
async fn fresh_approval_activates_then_queues_correlated_wake_once() {
    let h = Harness::new().await;
    sqlx::query("UPDATE agent_session SET harness_features=json_set(harness_features,'$.stateSnapshot',json('false'),'$.structuredQuestions',json('false')) WHERE id=?").bind(h.agent.as_str()).execute(h.services.store.write_pool()).await.unwrap();
    let mut events = h
        .services
        .event_bus
        .as_ref()
        .unwrap()
        .subscribe(SubscriptionFilter {
            workspace_id: Some(h.workspace.0.clone()),
            event_types: vec![DESKTOP_PERMISSION_RESOLVED.into()],
            ..Default::default()
        });
    let pending = h.agent("startControl", json!({})).await.unwrap();
    assert_eq!(
        h.client(
            "respondPermission",
            json!({"requestId":pending["requestId"],"decision":"allow_once"})
        )
        .await
        .unwrap(),
        json!({"accepted":true,"requestId":pending["requestId"]})
    );
    let batch = tokio::time::timeout(Duration::from_secs(10), events.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(batch[0].data["outcome"], "granted");
    assert_eq!(batch[0].data["state"]["hint"], RELEASE_HINT);
    h.services.desktop_flush_outbox().await;
    let messages:Vec<String>=sqlx::query_scalar("SELECT metadata FROM agent_message WHERE agent_id=? AND json_extract(metadata,'$.requestId')=?").bind(h.agent.as_str()).bind(pending["requestId"].as_str().unwrap()).fetch_all(h.services.store.read_pool()).await.unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(
        serde_json::from_str::<Value>(&messages[0]).unwrap()["message"],
        RELEASE_HINT
    );
    assert!(h
        .services
        .agent_state_snapshot_line(&h.agent)
        .await
        .is_none());
    let snapshot = intent_core::with_caller(
        Caller::Agent {
            agent_id: h.agent.clone(),
        },
        h.services
            .agent_snapshot_op(h.workspace.clone(), h.agent.clone()),
    )
    .await
    .unwrap();
    assert_eq!(snapshot["desktopControl"]["hint"], RELEASE_HINT);
    h.services.desktop_flush_outbox().await;
    let count:i64=sqlx::query_scalar("SELECT COUNT(*) FROM agent_message WHERE agent_id=? AND json_extract(metadata,'$.requestId')=?").bind(h.agent.as_str()).bind(pending["requestId"].as_str().unwrap()).fetch_one(h.services.store.read_pool()).await.unwrap();
    assert_eq!(count, 1);
    assert_eq!(
        h.client(
            "respondPermission",
            json!({"requestId":pending["requestId"],"decision":"allow_future"})
        )
        .await
        .unwrap_err()
        .code,
        "desktop-stale-request"
    );
    h.agent("endControl", json!({})).await.unwrap();
}
#[tokio::test]
async fn forged_agent_and_hook_calls_fail_closed() {
    let h = Harness::new().await;
    for caller in [h.owner.clone(), Caller::Daemon] {
        assert_eq!(
            intent_core::with_caller(
                caller,
                h.services
                    .desktop_agent_op(h.workspace.clone(), "startControl".into(), json!({}))
            )
            .await
            .unwrap_err()
            .code,
            "forbidden"
        );
    }
    assert_eq!(
        h.agent("startControl", json!({"agentId":"child"}))
            .await
            .unwrap_err()
            .code,
        "invalid-params"
    );
    assert_eq!(
        h.client(
            "setPermission",
            json!({"agentId":h.agent,"computerId":"other","allowed":true})
        )
        .await
        .unwrap_err()
        .code,
        "desktop-stale-request"
    );
}
#[tokio::test]
async fn teardown_failure_is_error_and_invalidates_authority() {
    let h = Harness::new().await;
    h.remember().await;
    h.agent("startControl", json!({})).await.unwrap();
    *h.executor.fail.lock().unwrap() = Some("endControl".into());
    assert_eq!(
        h.agent("endControl", json!({})).await.unwrap_err().code,
        "desktop-execution-failed"
    );
    assert_eq!(h.services.desktop.state(&h.agent), DesktopState::Inactive);
    assert!(h.agent("type", json!({"text":"forbidden"})).await.is_err());
}
#[tokio::test]
async fn offline_stop_deduplicates_and_does_not_end_successor() {
    let h = Harness::new().await;
    h.remember().await;
    let old = h.agent("startControl", json!({})).await.unwrap();
    let start = h
        .executor
        .calls
        .lock()
        .unwrap()
        .iter()
        .find(|c| c["operation"] == "startControl")
        .unwrap()
        .clone();
    h.agent("endControl", json!({})).await.unwrap();
    h.executor.connection.lock().unwrap().connection_epoch = "reconnected".into();
    let new = h.agent("startControl", json!({})).await.unwrap();
    let report = json!({"sessionId":old["sessionId"],"reason":"user_stop","stopReport":{"reportId":id(),"computerId":"physical","connectionEpoch":"epoch","stopReportToken":start["stopReportToken"]}});
    assert_eq!(
        h.client("revoke", report.clone()).await.unwrap(),
        json!({"revoked":false,"reported":true})
    );
    assert_eq!(
        h.client("revoke", report.clone()).await.unwrap(),
        json!({"revoked":false,"reported":false})
    );
    let mut duplicate = report.clone();
    duplicate["stopReport"]["reportId"] = id().into();
    assert_eq!(
        h.client("revoke", duplicate).await.unwrap(),
        json!({"revoked":false,"reported":false})
    );
    assert_eq!(
        value(&h.services.desktop.state(&h.agent))["sessionId"],
        new["sessionId"]
    );
    let mut forged = report;
    forged["stopReport"]["stopReportToken"] = "invalid".into();
    assert_eq!(
        h.client("revoke", forged).await.unwrap_err().code,
        "forbidden"
    );
    h.agent("endControl", json!({})).await.unwrap();
}
#[tokio::test]
async fn concurrent_starts_coalesce_and_permission_off_does_not_stop_active_control() {
    let h = Harness::new().await;
    let (first, second) = tokio::join!(
        h.agent("startControl", json!({})),
        h.agent("startControl", json!({}))
    );
    assert_eq!(first.unwrap(), second.unwrap());
    h.agent("endControl", json!({})).await.unwrap();
    h.remember().await;
    h.agent("startControl", json!({})).await.unwrap();
    h.client(
        "setPermission",
        json!({"agentId":h.agent,"computerId":"physical","allowed":false}),
    )
    .await
    .unwrap();
    assert!(matches!(
        h.services.desktop.state(&h.agent),
        DesktopState::Active { .. }
    ));
    h.agent("endControl", json!({})).await.unwrap();
    assert_eq!(
        h.agent("startControl", json!({})).await.unwrap()["status"],
        "pending_permission"
    );
}
#[tokio::test]
async fn snapshot_reports_live_state_and_clears_release_hint_on_connection_change() {
    let h = Harness::new().await;
    h.remember().await;
    h.agent("startControl", json!({})).await.unwrap();
    let snap = intent_core::with_caller(
        Caller::Agent {
            agent_id: h.agent.clone(),
        },
        h.services
            .agent_snapshot_op(h.workspace.clone(), h.agent.clone()),
    )
    .await
    .unwrap();
    assert_eq!(snap["desktopControl"]["hint"], RELEASE_HINT);
    h.executor.connection.lock().unwrap().connection_epoch = "replacement".into();
    let snap = intent_core::with_caller(
        Caller::Agent {
            agent_id: h.agent.clone(),
        },
        h.services
            .agent_snapshot_op(h.workspace.clone(), h.agent.clone()),
    )
    .await
    .unwrap();
    assert_eq!(snap["desktopControl"], json!({"status":"inactive"}));
    assert!(h
        .agent("click", json!({"displayId":"d","layoutId":"l","x":0,"y":0}))
        .await
        .is_err());
}
#[tokio::test]
async fn owner_change_invalidates_active_control_and_old_permission_is_not_transferred() {
    let h = Harness::new().await;
    h.remember().await;
    let active = h.agent("startControl", json!({})).await.unwrap();
    let mut principal = h.services.store.get_primary_principal().await.unwrap();
    principal.id = PrincipalId::new();
    principal.is_primary = false;
    h.services.store.upsert_principal(&principal).await.unwrap();
    sqlx::query("UPDATE workspace SET owner_principal_id=? WHERE id=?")
        .bind(principal.id.as_str())
        .bind(h.workspace.as_str())
        .execute(h.services.store.write_pool())
        .await
        .unwrap();
    assert_eq!(
        intent_core::with_caller(Caller::Daemon, h.services.desktop_current_state(&h.agent)).await,
        DesktopState::Inactive
    );
    assert_eq!(
        h.services
            .store
            .desktop_terminal(active["sessionId"].as_str().unwrap())
            .await
            .unwrap()
            .unwrap()["reason"],
        "owner_changed"
    );
    h.executor.connection.lock().unwrap().principal_id = principal.id;
    assert_eq!(
        h.agent("startControl", json!({})).await.unwrap()["status"],
        "pending_permission"
    );
}
#[tokio::test]
async fn readiness_failure_is_a_correlated_failure_not_a_grant() {
    let h = Harness::new().await;
    let mut events = h
        .services
        .event_bus
        .as_ref()
        .unwrap()
        .subscribe(SubscriptionFilter {
            workspace_id: Some(h.workspace.0.clone()),
            event_types: vec![DESKTOP_PERMISSION_RESOLVED.into()],
            ..Default::default()
        });
    let pending = h.agent("startControl", json!({})).await.unwrap();
    *h.executor.fail.lock().unwrap() = Some("startControl".into());
    h.client(
        "respondPermission",
        json!({"requestId":pending["requestId"],"decision":"allow_once"}),
    )
    .await
    .unwrap();
    let events = tokio::time::timeout(Duration::from_secs(10), events.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(events[0].data["outcome"], "failed");
    assert_eq!(events[0].data["state"], json!({"status":"inactive"}));
    assert_eq!(events[0].data["error"]["code"], "desktop-execution-failed");
}
#[tokio::test]
async fn restart_retains_stop_credential_and_permission_but_never_execution() {
    let h = Harness::new().await;
    h.remember().await;
    let active = h.agent("startControl", json!({})).await.unwrap();
    let start = h
        .executor
        .calls
        .lock()
        .unwrap()
        .iter()
        .find(|c| c["operation"] == "startControl")
        .unwrap()
        .clone();
    let restarted =
        Services::new(h.services.store.clone()).with_reverse_dispatch(h.executor.clone());
    intent_core::with_caller(Caller::Daemon, restarted.desktop_recover())
        .await
        .unwrap();
    assert_eq!(restarted.desktop.state(&h.agent), DesktopState::Inactive);
    let report = json!({"workspaceId":h.workspace,"sessionId":active["sessionId"],"reason":"user_stop","stopReport":{"reportId":id(),"computerId":"physical","connectionEpoch":"epoch","stopReportToken":start["stopReportToken"]}});
    let connection = h.executor.connection.lock().unwrap().clone();
    let result = intent_core::with_caller(
        h.owner.clone(),
        restarted.desktop_client_op("revoke".into(), report, connection),
    )
    .await
    .unwrap();
    assert_eq!(result, json!({"revoked":false,"reported":true}));
    let record = h
        .services
        .store
        .desktop_terminal(active["sessionId"].as_str().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert!(record.get("stopReportToken").is_none());
    assert_eq!(record["reason"], "user_stop");
}

#[tokio::test]
async fn expired_request_cannot_accept_delayed_readiness() {
    let h = Harness::new().await;
    h.executor
        .hold_start
        .store(true, std::sync::atomic::Ordering::Relaxed);
    let pending = h.agent("startControl", json!({})).await.unwrap();
    h.client(
        "respondPermission",
        json!({"requestId":pending["requestId"],"decision":"allow_once"}),
    )
    .await
    .unwrap();
    h.executor.start_seen.notified().await;
    let mut live = h.services.desktop.get(&h.agent).unwrap();
    if let Phase::Pending { expires, .. } = &mut live.phase {
        *expires = Instant::now().checked_sub(Duration::from_secs(1)).unwrap();
    }
    h.services.desktop.put(live);
    h.executor.release_start.notify_one();
    let gate = h.services.desktop.gate(&h.agent);
    let _guard = gate.lock().await;
    assert_eq!(h.services.desktop.state(&h.agent), DesktopState::Inactive);
    assert!(h
        .executor
        .calls
        .lock()
        .unwrap()
        .iter()
        .any(|p| p["operation"] == "endControl"));
}

#[tokio::test]
async fn user_stop_during_activation_wins_and_has_one_correlated_outcome() {
    let h = Harness::new().await;
    h.executor
        .hold_start
        .store(true, std::sync::atomic::Ordering::Relaxed);
    let pending = h.agent("startControl", json!({})).await.unwrap();
    h.client(
        "respondPermission",
        json!({"requestId":pending["requestId"],"decision":"allow_once"}),
    )
    .await
    .unwrap();
    h.executor.start_seen.notified().await;
    let start = h
        .executor
        .calls
        .lock()
        .unwrap()
        .iter()
        .find(|p| p["operation"] == "startControl")
        .unwrap()
        .clone();
    h.client("revoke",json!({"sessionId":start["sessionId"],"reason":"user_stop","stopReport":{"reportId":id(),"computerId":"physical","connectionEpoch":"epoch","stopReportToken":start["stopReportToken"]}})).await.unwrap();
    h.executor.release_start.notify_one();
    let gate = h.services.desktop.gate(&h.agent);
    let _guard = gate.lock().await;
    assert_eq!(h.services.desktop.state(&h.agent), DesktopState::Inactive);
    let outcomes: Vec<String> = sqlx::query_scalar(
        "SELECT json_extract(value,'$.payload') FROM settings WHERE key GLOB 'desktop.v1/outbox/*' AND json_extract(value,'$.payload.requestId')=?",
    )
    .bind(pending["requestId"].as_str().unwrap())
    .fetch_all(h.services.store.read_pool())
    .await
    .unwrap();
    assert_eq!(outcomes.len(), 1);
    assert_eq!(
        serde_json::from_str::<Value>(&outcomes[0]).unwrap()["message"],
        STOP_HINT
    );
}

#[tokio::test]
async fn unknown_native_result_invalidates_and_is_never_retried() {
    let h = Harness::new().await;
    h.remember().await;
    h.agent("startControl", json!({})).await.unwrap();
    *h.executor.result.lock().unwrap() = Some(json!({"ok":false}));
    let e = h.agent("type", json!({"text":"once"})).await.unwrap_err();
    assert_eq!(e.execution.as_deref(), Some("unknown"));
    assert_eq!(h.services.desktop.state(&h.agent), DesktopState::Inactive);
    assert!(h.agent("type", json!({"text":"again"})).await.is_err());
    assert_eq!(
        h.executor
            .calls
            .lock()
            .unwrap()
            .iter()
            .filter(|p| p["operation"] == "execute")
            .count(),
        1
    );
}

#[tokio::test]
async fn desktop_feature_is_frozen_per_session_and_release_is_always_available() {
    let h = Harness::new().await;
    let mut session = h.services.store.get_agent_session(&h.agent).await.unwrap();
    assert!(h.services.session_agent_features(&session).desktop_control);
    let settings = h.services.settings_registry.as_ref().unwrap();
    settings
        .apply(&[("agentFeatures.desktopControl".into(), json!(false))])
        .unwrap();
    assert!(h.services.session_agent_features(&session).desktop_control);
    let child = intent_core::with_caller(
        h.owner.clone(),
        h.services.agent_create(
            h.workspace.clone(),
            Some("new child".into()),
            None,
            None,
            Some(h.agent.clone()),
            None,
            intent_core::AgentCreateExtra::default(),
        ),
    )
    .await
    .unwrap();
    let child = AgentId::from(
        child["agent"]["id"]
            .as_str()
            .expect("created child agent ID"),
    );
    let child_session = h
        .services
        .store
        .get_agent_session_summary(&child)
        .await
        .unwrap();
    assert!(
        !h.services
            .session_agent_features(&child_session)
            .desktop_control
    );
    let calls = h.executor.calls.lock().unwrap().len();
    assert!(intent_core::with_caller(
        Caller::Agent { agent_id: child },
        h.services
            .desktop_agent_op(h.workspace.clone(), "startControl".into(), json!({}))
    )
    .await
    .unwrap_err()
    .detail
    .contains("agentFeatures.desktopControl"));
    assert_eq!(h.executor.calls.lock().unwrap().len(), calls);
    h.remember().await;
    h.agent("startControl", json!({})).await.unwrap();
    session.harness_features.as_mut().unwrap()["desktopControl"] = false.into();
    sqlx::query("UPDATE agent_session SET harness_features=? WHERE id=?")
        .bind(session.harness_features.as_ref().unwrap().to_string())
        .bind(h.agent.as_str())
        .execute(h.services.store.write_pool())
        .await
        .unwrap();
    assert_eq!(
        h.agent("startControl", json!({})).await.unwrap_err().code,
        "forbidden"
    );
    assert_eq!(
        h.agent("type", json!({"text":"blocked"}))
            .await
            .unwrap_err()
            .code,
        "forbidden"
    );
    assert_eq!(
        h.agent("endControl", json!({})).await.unwrap()["ended"],
        true
    );
    session
        .harness_features
        .as_mut()
        .unwrap()
        .as_object_mut()
        .unwrap()
        .remove("desktopControl");
    assert!(h.services.session_agent_features(&session).desktop_control);
}

#[test]
fn screenshot_result_requires_canonical_assets_and_display_geometry() {
    let ws = WorkspaceId::from("workspace");
    let mut result = json!({"capturedAt":"2026-10-02T09:00:00Z","layoutId":"layout","displays":[{"displayId":"screen","width":1920,"height":1080,"originX":-1920,"originY":0,"scaleFactor":2.0,"assetId":"asset","url":"workspace-asset://workspace/asset","mimeType":"image/png"}]});
    validate_result(&result, &json!({"kind":"screenshot"}), &ws).unwrap();
    validate_result(
        &result,
        &json!({"kind":"screenshot","displayId":"screen"}),
        &ws,
    )
    .unwrap();
    assert!(validate_result(
        &result,
        &json!({"kind":"screenshot","displayId":"other"}),
        &ws
    )
    .is_err());
    assert!(validate_result(
        &result,
        &json!({"kind":"screenshot","layoutId":"old-layout"}),
        &ws
    )
    .is_err());
    let mut all_displays = result.clone();
    all_displays["displays"]
        .as_array_mut()
        .unwrap()
        .push(result["displays"][0].clone());
    assert!(validate_result(&all_displays, &json!({"kind":"screenshot"}), &ws).is_err());
    result["displays"][0]["url"] = "file:///etc/passwd".into();
    assert!(validate_result(&result, &json!({"kind":"screenshot"}), &ws).is_err());
}

#[tokio::test]
async fn display_enumeration_requires_active_control_and_returns_metadata_only() {
    let h = Harness::new().await;
    assert_eq!(
        h.agent("listDisplay", json!({})).await.unwrap_err().code,
        "desktop-not-active"
    );
    assert!(h.executor.calls.lock().unwrap().is_empty());
    h.remember().await;
    h.agent("startControl", json!({})).await.unwrap();
    let result = json!({"layoutId":"layout","displays":[{"displayId":"screen","width":1920,"height":1080,"originX":-1920,"originY":0,"scaleFactor":2.0}]});
    *h.executor.result.lock().unwrap() = Some(result.clone());
    assert_eq!(h.agent("listDisplay", json!({})).await.unwrap(), result);
    assert!(h
        .executor
        .calls
        .lock()
        .unwrap()
        .iter()
        .any(
            |call| call["operation"] == "prepareCommand" && call["action"]["kind"] == "listDisplay"
        ));
    validate_result(
        &json!({"layoutId":"empty-layout","displays":[]}),
        &json!({"kind":"listDisplay"}),
        &h.workspace,
    )
    .unwrap();
    let mut malformed = result;
    malformed["displays"][0]["assetId"] = "must-not-capture".into();
    assert!(validate_result(&malformed, &json!({"kind":"listDisplay"}), &h.workspace).is_err());
    h.agent("endControl", json!({})).await.unwrap();
    assert_eq!(
        h.agent("listDisplay", json!({})).await.unwrap_err().code,
        "desktop-not-active"
    );
}

#[tokio::test]
async fn private_desktop_state_cannot_be_read_or_forged_through_settings() {
    let h = Harness::new().await;
    h.remember().await;
    let keys: Vec<String> =
        sqlx::query_scalar("SELECT key FROM settings WHERE key GLOB 'desktop.v1/*'")
            .fetch_all(h.services.store.read_pool())
            .await
            .unwrap();
    assert!(!keys.is_empty());
    for key in keys {
        let before = h.services.store.get_setting(&key).await.unwrap();
        assert!(crate::settings::find_definition(&key).is_none());
        assert!(
            intent_core::with_caller(h.owner.clone(), h.services.settings_get(key.clone()))
                .await
                .is_err()
        );
        assert!(intent_core::with_caller(
            h.owner.clone(),
            h.services
                .settings_update(json!([{"path":key,"value":false}]))
        )
        .await
        .is_err());
        assert!(
            intent_core::with_caller(h.owner.clone(), h.services.settings_reset(key.clone()))
                .await
                .is_err()
        );
        assert_eq!(h.services.store.get_setting(&key).await.unwrap(), before);
        assert!(
            !intent_core::with_caller(h.owner.clone(), h.services.settings_list())
                .await
                .unwrap()
                .to_string()
                .contains(&key)
        );
    }
    let tables: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name GLOB 'desktop_*'",
    )
    .fetch_one(h.services.store.read_pool())
    .await
    .unwrap();
    assert_eq!(
        tables, 0,
        "desktop persistence must not require a migration"
    );
}

#[tokio::test]
async fn request_outcome_rolls_back_when_durable_outbox_insert_fails() {
    let h = Harness::new().await;
    let pending = h.agent("startControl", json!({})).await.unwrap();
    let request = pending["requestId"].as_str().unwrap();
    sqlx::query("CREATE TRIGGER reject_desktop_outbox BEFORE INSERT ON settings WHEN NEW.key GLOB 'desktop.v1/outbox/*' BEGIN SELECT RAISE(ABORT,'injected failure'); END").execute(h.services.store.write_pool()).await.unwrap();
    let payload = json!({"type":"desktop_control","requestId":request,"outcome":"denied","state":{"status":"inactive"},"message":"denied"});
    assert!(h
        .services
        .store
        .desktop_resolve_request(request, &h.workspace, &h.agent, "denied", &payload)
        .await
        .is_err());
    let record: Value = serde_json::from_str(
        &h.services
            .store
            .get_setting(&format!("desktop.v1/request/{request}"))
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    assert!(record["outcome"].is_null());
    assert!(h.services.store.desktop_outbox().await.unwrap().is_empty());
    sqlx::query("DROP TRIGGER reject_desktop_outbox")
        .execute(h.services.store.write_pool())
        .await
        .unwrap();
    assert!(h
        .services
        .store
        .desktop_resolve_request(request, &h.workspace, &h.agent, "denied", &payload)
        .await
        .unwrap());
    assert!(!h
        .services
        .store
        .desktop_resolve_request(request, &h.workspace, &h.agent, "denied", &payload)
        .await
        .unwrap());
    assert_eq!(h.services.store.desktop_outbox().await.unwrap().len(), 1);
}

#[tokio::test]
async fn failed_wake_storage_retains_outbox_without_duplicate_memory_deliveries() {
    let h = Harness::new().await;
    let pending = h.agent("startControl", json!({})).await.unwrap();
    let request = pending["requestId"].as_str().unwrap();
    let payload = json!({"type":"desktop_control","requestId":request,"outcome":"denied","state":{"status":"inactive"},"message":"denied"});
    h.services
        .store
        .desktop_resolve_request(request, &h.workspace, &h.agent, "denied", &payload)
        .await
        .unwrap();
    for sql in [
        "CREATE TRIGGER reject_desktop_message BEFORE INSERT ON agent_message BEGIN SELECT RAISE(ABORT,'injected failure'); END",
        "CREATE TRIGGER reject_desktop_queue BEFORE INSERT ON agent_queue BEGIN SELECT RAISE(ABORT,'injected failure'); END",
    ] {
        sqlx::query(sql).execute(h.services.store.write_pool()).await.unwrap();
    }
    h.services.desktop_flush_outbox().await;
    assert_eq!(h.services.store.desktop_outbox().await.unwrap().len(), 1);
    h.services.desktop_flush_outbox().await;
    assert_eq!(h.services.agent_queues.lock().unwrap()[&h.agent].len(), 1);
    sqlx::query("DROP TRIGGER reject_desktop_queue")
        .execute(h.services.store.write_pool())
        .await
        .unwrap();
    h.services.desktop_flush_outbox().await;
    assert!(h.services.store.desktop_outbox().await.unwrap().is_empty());
    assert_eq!(h.services.agent_queues.lock().unwrap()[&h.agent].len(), 1);
    let persisted: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM agent_queue WHERE agent_id=?")
        .bind(h.agent.as_str())
        .fetch_one(h.services.store.read_pool())
        .await
        .unwrap();
    assert_eq!(persisted, 1);
}

#[tokio::test]
async fn first_eligible_allow_claims_unassigned_primary_once() {
    let h = Harness::new().await;
    h.remember().await;
    h.services
        .store
        .set_workspace_browser_client(&h.workspace, None)
        .await
        .unwrap();
    let first = h.executor.connection.lock().unwrap().clone();
    let second = DesktopConnection {
        client_id: ClientId::from("second"),
        connection_epoch: "second-epoch".into(),
        ..first.clone()
    };
    h.executor
        .extra_connections
        .lock()
        .unwrap()
        .push(second.clone());
    let pending = h.agent("startControl", json!({})).await.unwrap();
    assert!(h
        .services
        .store
        .workspace_browser_client(&h.workspace)
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        h.services
            .desktop
            .candidates(pending["requestId"].as_str().unwrap())
            .len(),
        2
    );
    assert_eq!(h.agent("startControl", json!({})).await.unwrap(), pending);
    let args =
        json!({"workspaceId":h.workspace,"requestId":pending["requestId"],"decision":"allow_once"});
    let (a, b) = tokio::join!(
        intent_core::with_caller(
            h.owner.clone(),
            h.services
                .desktop_client_op("respondPermission".into(), args.clone(), first)
        ),
        intent_core::with_caller(
            h.owner.clone(),
            h.services
                .desktop_client_op("respondPermission".into(), args, second)
        ),
    );
    assert_eq!(usize::from(a.is_ok()) + usize::from(b.is_ok()), 1);
    let winner = h
        .services
        .store
        .workspace_browser_client(&h.workspace)
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while !matches!(
            h.services.desktop.state(&h.agent),
            DesktopState::Active { .. }
        ) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let live = h.services.desktop.get(&h.agent).unwrap();
    assert_eq!(live.binding.connection.client_id, winner);
    assert_eq!(
        h.executor
            .calls
            .lock()
            .unwrap()
            .iter()
            .filter(|p| p["operation"] == "startControl")
            .count(),
        1
    );
    h.agent("endControl", json!({})).await.unwrap();
    assert_eq!(
        h.services
            .store
            .workspace_browser_client(&h.workspace)
            .await
            .unwrap(),
        Some(winner)
    );
}

#[tokio::test]
async fn invalidation_during_permission_response_never_restores_request() {
    for decision in ["deny", "allow_once", "allow_future"] {
        let h = Harness::new().await;
        h.services
            .store
            .set_workspace_browser_client(&h.workspace, None)
            .await
            .unwrap();
        let first = h.executor.connection.lock().unwrap().clone();
        let second = DesktopConnection {
            client_id: ClientId::from("second"),
            connection_epoch: "second-epoch".into(),
            ..first.clone()
        };
        h.executor
            .extra_connections
            .lock()
            .unwrap()
            .push(second.clone());
        let pending = h.agent("startControl", json!({})).await.unwrap();
        let live = h.services.desktop.get(&h.agent).unwrap();
        let seen = Arc::new(tokio::sync::Notify::new());
        let resume = Arc::new(tokio::sync::Notify::new());
        *h.services.desktop.decision_barrier.lock().unwrap() = Some((seen.clone(), resume.clone()));
        let services = h.services.clone();
        let owner = h.owner.clone();
        let args =
            json!({"workspaceId":h.workspace,"requestId":pending["requestId"],"decision":decision});
        let response = tokio::spawn(async move {
            intent_core::with_caller(
                owner,
                services.desktop_client_op("respondPermission".into(), args, second),
            )
            .await
        });
        tokio::time::timeout(Duration::from_secs(2), seen.notified())
            .await
            .unwrap();
        h.executor.extra_connections.lock().unwrap().clear();
        h.executor.connection.lock().unwrap().connection_epoch = "disconnected".into();
        intent_core::with_caller(
            Caller::Daemon,
            h.services.desktop_end_live(live, "disconnected", true),
        )
        .await
        .unwrap();
        assert!(h
            .services
            .desktop
            .candidates(pending["requestId"].as_str().unwrap())
            .is_empty());
        resume.notify_one();
        let error = response
            .await
            .expect("stale decision must not panic")
            .unwrap_err();
        assert_eq!(error.code, "desktop-stale-request");
        assert_eq!(h.services.desktop.state(&h.agent), DesktopState::Inactive);
        assert!(h
            .services
            .store
            .workspace_browser_client(&h.workspace)
            .await
            .unwrap()
            .is_none());
        assert!(!h
            .executor
            .calls
            .lock()
            .unwrap()
            .iter()
            .any(|call| call["operation"] == "startControl"));
    }
}

#[tokio::test]
async fn disconnected_candidate_does_not_prevent_last_live_denial() {
    for rehello in [false, true] {
        let h = Harness::new().await;
        h.services
            .store
            .set_workspace_browser_client(&h.workspace, None)
            .await
            .unwrap();
        let first = h.executor.connection.lock().unwrap().clone();
        let second = DesktopConnection {
            client_id: ClientId::from("second"),
            connection_epoch: "second-epoch".into(),
            ..first.clone()
        };
        h.executor
            .extra_connections
            .lock()
            .unwrap()
            .push(second.clone());
        let pending = h.agent("startControl", json!({})).await.unwrap();
        let request = pending["requestId"].as_str().unwrap();
        let mut replacement = first.clone();
        replacement.connection_epoch = "replacement-epoch".into();
        if !rehello {
            replacement.client_id = ClientId::from("unrelated-client");
        }
        *h.executor.connection.lock().unwrap() = replacement.clone();
        if rehello {
            let stale = intent_core::with_caller(
                h.owner.clone(),
                h.services.desktop_client_op(
                    "respondPermission".into(),
                    json!({"workspaceId":h.workspace,"requestId":request,"decision":"allow_once"}),
                    replacement,
                ),
            )
            .await
            .unwrap_err();
            assert_eq!(stale.code, "desktop-stale-request");
        }
        intent_core::with_caller(
            h.owner.clone(),
            h.services.desktop_client_op(
                "respondPermission".into(),
                json!({"workspaceId":h.workspace,"requestId":request,"decision":"deny"}),
                second,
            ),
        )
        .await
        .unwrap();
        assert!(
            !h.services
                .desktop
                .get(&h.agent)
                .is_some_and(|live| matches!(
                    live.phase,
                    Phase::Pending {
                        accepted: false,
                        ..
                    }
                )),
            "the last connected candidate's Deny must be terminal"
        );
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let raw = h
                    .services
                    .store
                    .get_setting(&format!("desktop.v1/request/{request}"))
                    .await
                    .unwrap()
                    .unwrap();
                let record: Value = serde_json::from_str(&raw).unwrap();
                if !record["outcome"].is_null() {
                    assert_eq!(record["outcome"], "denied");
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(h
            .services
            .store
            .workspace_browser_client(&h.workspace)
            .await
            .unwrap()
            .is_none());
        assert!(!h
            .executor
            .calls
            .lock()
            .unwrap()
            .iter()
            .any(|call| call["operation"] == "startControl"));
    }
}

#[tokio::test]
async fn candidate_denial_or_withdrawal_never_claims_primary() {
    let h = Harness::new().await;
    h.services
        .store
        .set_workspace_browser_client(&h.workspace, None)
        .await
        .unwrap();
    let first = h.executor.connection.lock().unwrap().clone();
    let second = DesktopConnection {
        client_id: ClientId::from("second"),
        connection_epoch: "second-epoch".into(),
        ..first.clone()
    };
    h.executor.extra_connections.lock().unwrap().push(second);
    let pending = h.agent("startControl", json!({})).await.unwrap();
    h.client(
        "respondPermission",
        json!({"requestId":pending["requestId"],"decision":"deny"}),
    )
    .await
    .unwrap();
    assert!(h
        .services
        .store
        .workspace_browser_client(&h.workspace)
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        h.services
            .desktop
            .candidates(pending["requestId"].as_str().unwrap())
            .len(),
        1
    );
    assert!(h
        .client(
            "respondPermission",
            json!({"requestId":pending["requestId"],"decision":"allow_once"})
        )
        .await
        .is_err());
    h.agent("endControl", json!({})).await.unwrap();
    assert!(h
        .client(
            "respondPermission",
            json!({"requestId":pending["requestId"],"decision":"allow_once"})
        )
        .await
        .is_err());
    assert!(h
        .services
        .store
        .workspace_browser_client(&h.workspace)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn primary_set_then_clear_invalidates_old_candidate_generation() {
    let h = Harness::new().await;
    h.services
        .store
        .set_workspace_browser_client(&h.workspace, None)
        .await
        .unwrap();
    h.services
        .store
        .upsert_client(
            &ClientId::from("primary"),
            Some("Primary"),
            Some(&json!({"browserExec":true,"desktopControl":1})),
            &intent_core::ClientHostInfo::default(),
        )
        .await
        .unwrap();
    let pending = h.agent("startControl", json!({})).await.unwrap();
    let state = h
        .client("getState", json!({"agentId":h.agent}))
        .await
        .unwrap();
    assert_eq!(state["pending"]["claimsPrimary"], true);
    assert!(value(&h.services.desktop.state(&h.agent))
        .get("computerName")
        .is_none());
    for pin in [Some(ClientId::from("primary")), None] {
        intent_core::with_caller(
            h.owner.clone(),
            h.services
                .set_workspace_browser_client(h.workspace.clone(), pin),
        )
        .await
        .unwrap();
    }
    assert_eq!(
        h.client(
            "respondPermission",
            json!({"requestId":pending["requestId"],"decision":"allow_once"})
        )
        .await
        .unwrap_err()
        .code,
        "desktop-stale-request"
    );
    assert!(h
        .services
        .store
        .workspace_browser_client(&h.workspace)
        .await
        .unwrap()
        .is_none());
    assert!(!h
        .executor
        .calls
        .lock()
        .unwrap()
        .iter()
        .any(|p| p["operation"] == "startControl"));
}

#[tokio::test]
async fn primary_claim_and_remembered_permission_roll_back_together() {
    let h = Harness::new().await;
    h.services
        .store
        .set_workspace_browser_client(&h.workspace, None)
        .await
        .unwrap();
    let pending = h.agent("startControl", json!({})).await.unwrap();
    sqlx::query("CREATE TRIGGER reject_desktop_permission BEFORE INSERT ON settings WHEN NEW.key GLOB 'desktop.v1/permission/*' BEGIN SELECT RAISE(ABORT,'injected grant failure'); END").execute(h.services.store.write_pool()).await.unwrap();
    let args = json!({"requestId":pending["requestId"],"decision":"allow_future"});
    assert!(h.client("respondPermission", args.clone()).await.is_err());
    assert!(h
        .services
        .store
        .workspace_browser_client(&h.workspace)
        .await
        .unwrap()
        .is_none());
    let request = h
        .services
        .store
        .get_setting(&format!(
            "desktop.v1/request/{}",
            pending["requestId"].as_str().unwrap()
        ))
        .await
        .unwrap()
        .unwrap();
    assert!(serde_json::from_str::<Value>(&request)
        .unwrap()
        .get("claimed")
        .is_none());
    assert!(!h
        .executor
        .calls
        .lock()
        .unwrap()
        .iter()
        .any(|p| p["operation"] == "startControl"));
    sqlx::query("DROP TRIGGER reject_desktop_permission")
        .execute(h.services.store.write_pool())
        .await
        .unwrap();
    h.client("respondPermission", args).await.unwrap();
    assert_eq!(
        h.services
            .store
            .workspace_browser_client(&h.workspace)
            .await
            .unwrap(),
        Some(ClientId::from("primary"))
    );
    let connection = h.executor.connection.lock().unwrap().clone();
    assert!(h
        .services
        .store
        .desktop_permission(&connection.principal_id, &h.workspace, &h.agent, "physical")
        .await
        .unwrap());
}

#[tokio::test]
async fn deletion_removes_private_consent_and_stop_records_in_the_same_scope() {
    let h = Harness::new().await;
    h.remember().await;
    let active = h.agent("startControl", json!({})).await.unwrap();
    h.agent("endControl", json!({})).await.unwrap();
    assert!(h
        .services
        .store
        .delete_agent_session(&h.workspace, &h.agent)
        .await
        .unwrap());
    assert!(h
        .services
        .store
        .desktop_terminal(active["sessionId"].as_str().unwrap())
        .await
        .unwrap()
        .is_none());
    let count:i64=sqlx::query_scalar("SELECT COUNT(*) FROM settings WHERE key GLOB 'desktop.v1/*' AND json_extract(value,'$.agentId')=?").bind(h.agent.as_str()).fetch_one(h.services.store.read_pool()).await.unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
async fn workspace_desktop_cleanup_failure_preserves_sessions_and_retry_removes_scope() {
    let h = Harness::new().await;
    h.remember().await;
    let active = h.agent("startControl", json!({})).await.unwrap();
    h.agent("endControl", json!({})).await.unwrap();
    sqlx::query("CREATE TRIGGER fail_desktop_cleanup BEFORE DELETE ON settings WHEN OLD.key GLOB 'desktop.v1/*' BEGIN SELECT RAISE(ABORT,'injected cleanup failure'); END")
        .execute(h.services.store.write_pool()).await.unwrap();
    assert!(h
        .services
        .store
        .delete_workspace(&h.workspace)
        .await
        .is_err());
    assert!(h
        .services
        .store
        .get_agent_session_summary(&h.agent)
        .await
        .is_ok());
    assert!(h
        .services
        .store
        .desktop_terminal(active["sessionId"].as_str().unwrap())
        .await
        .unwrap()
        .is_some());
    sqlx::query("DROP TRIGGER fail_desktop_cleanup")
        .execute(h.services.store.write_pool())
        .await
        .unwrap();
    h.services
        .store
        .delete_workspace(&h.workspace)
        .await
        .unwrap();
    assert!(h
        .services
        .store
        .desktop_terminal(active["sessionId"].as_str().unwrap())
        .await
        .unwrap()
        .is_none());
    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM settings WHERE key GLOB 'desktop.v1/*'")
            .fetch_one(h.services.store.read_pool())
            .await
            .unwrap();
    assert_eq!(count, 0);
}
