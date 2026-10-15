//! Native recovery uses original persisted targets and fresh account admission.
use super::*;
use intent_core::repository_request::{
    RepositoryPrivateOutput, RepositoryReadConnection, RepositoryReadReplyKind,
    RepositoryReadRetirements, RepositoryWireEntry,
};
use intent_core::review_monitor::ReviewMonitorRecipientSupport;
use intent_core::{with_caller, Caller, HostRole};

pub(crate) struct NativeRecipient {
    services: Arc<Services>,
    caller: Caller,
    connection: Arc<dyn RepositoryReadConnection>,
    _retirements: Box<dyn RepositoryReadRetirements>,
}
impl NativeRecipient {
    pub(crate) async fn new(services: Arc<Services>) -> Self {
        services.initialize_repository_wire().await.unwrap();
        let caller = Caller::Wire {
            principal_id: services.store.get_primary_principal().await.unwrap().id,
            host_role: HostRole::Owner,
        };
        let connection = with_caller(caller.clone(), async {
            services
                .repository_read_connection(RepositoryWireEntry::AdmittedLocal)
                .unwrap()
        })
        .await;
        let retirements = connection.take_retirements().unwrap();
        with_caller(caller.clone(), async {
            connection
                .review_monitoring_hello(ReviewMonitorRecipientSupport::from_capabilities(
                    &json!({"reviewMonitoring":1}),
                ))
                .unwrap()
                .acknowledge();
        })
        .await;
        Self {
            services,
            caller,
            connection,
            _retirements: retirements,
        }
    }
    async fn list(&self, workspace: WorkspaceId) -> (Value, Arc<dyn RepositoryPrivateOutput>) {
        let services = self.services.clone();
        let connection = self.connection.clone();
        with_caller(self.caller.clone(), async {
            let scope = connection.capture();
            let mut result = None;
            scope
                .scope(Box::pin(async {
                    let value = services.pr_monitor_list(workspace, None).await.unwrap();
                    let frame = json!({"jsonrpc":"2.0","id":1,"result":value}).to_string();
                    let output = scope.bind_private_output(&frame).unwrap().unwrap();
                    scope
                        .deliver(RepositoryReadReplyKind::Result, &mut || Ok(()))
                        .await
                        .unwrap();
                    result = Some((value, output));
                }))
                .await;
            scope.retire();
            result.unwrap()
        })
        .await
    }
    pub(crate) async fn cancel(
        &self,
        workspace: WorkspaceId,
        id: intent_core::PrMonitorId,
    ) -> intent_core::Result<Value> {
        self.cancel_output(workspace, id)
            .await
            .map(|(value, _)| value)
    }
    async fn cancel_output(
        &self,
        workspace: WorkspaceId,
        id: intent_core::PrMonitorId,
    ) -> intent_core::Result<(Value, Arc<dyn RepositoryPrivateOutput>)> {
        let services = self.services.clone();
        let connection = self.connection.clone();
        with_caller(self.caller.clone(), async {
            let scope = connection.capture();
            let mut result = None;
            scope
                .scope(Box::pin(async {
                    result = Some(
                        async {
                            let value = services.pr_monitor_cancel_by_id(workspace, id).await?;
                            let frame = json!({"jsonrpc":"2.0","id":2,"result":value}).to_string();
                            let output = scope
                                .bind_private_output(&frame)?
                                .expect("private cancel reply");
                            scope
                                .deliver(RepositoryReadReplyKind::Result, &mut || Ok(()))
                                .await?;
                            Ok((value, output))
                        }
                        .await,
                    );
                }))
                .await;
            scope.retire();
            result.unwrap()
        })
        .await
    }
    pub(crate) async fn history_subscription(
        &self,
        agent: AgentId,
        workspace: WorkspaceId,
    ) -> (
        Value,
        Arc<dyn RepositoryPrivateOutput>,
        Arc<dyn intent_core::repository_request::RepositoryMonitorHistoryRecipient>,
    ) {
        let services = self.services.clone();
        let connection = self.connection.clone();
        with_caller(self.caller.clone(), async {
            let recipient = connection.capture_monitor_history().expect("supported original history subscription");
            let mut result = None;
            recipient.scope(Box::pin(async {
                let first = services.agent_history_batch_op(agent.clone(),None,100).await.unwrap();
                let second = services.agent_get_conversation_op(agent,None,Some(workspace),None,None,None,None,false).await.unwrap();
                result = Some(json!({"first":first,"second":second}));
            })).await;
            let value = result.unwrap();
            let output = recipient.bind_output(json!({"jsonrpc":"2.0","method":"subscription.push","params":{"subscriptionId":"native-history-test","kind":"snapshot","seq":0,"snapshot":value["second"]}}).to_string());
            (value,output,recipient)
        }).await
    }

    #[cfg(unix)]
    pub(crate) async fn history_api(
        &self,
        agent: AgentId,
        workspace: WorkspaceId,
        method: &str,
        message_id: String,
    ) -> (Value, Option<Arc<dyn RepositoryPrivateOutput>>) {
        let services = self.services.clone();
        let connection = self.connection.clone();
        with_caller(self.caller.clone(), async {
            let scope = connection.capture();
            let mut result = None;
            scope
                .scope(Box::pin(async {
                    let value = match method {
                        "batch" => services.agent_history_batch_op(agent, None, 100).await,
                        "index" => {
                            services
                                .agent_list_user_messages_op(agent, Some(workspace), Some(80))
                                .await
                        }
                        "block" => {
                            services
                                .agent_get_message_block_op(
                                    agent,
                                    message_id.clone(),
                                    format!("{message_id}:0"),
                                    Some(workspace),
                                )
                                .await
                        }
                        _ => {
                            services
                                .agent_get_conversation_op(
                                    agent,
                                    None,
                                    Some(workspace),
                                    None,
                                    None,
                                    None,
                                    None,
                                    false,
                                )
                                .await
                        }
                    }
                    .unwrap();
                    let frame = json!({"jsonrpc":"2.0","id":3,"result":value}).to_string();
                    let output = scope.bind_private_output(&frame).unwrap();
                    scope
                        .deliver(RepositoryReadReplyKind::Result, &mut || Ok(()))
                        .await
                        .unwrap();
                    result = Some((value, output));
                }))
                .await;
            scope.retire();
            result.unwrap()
        })
        .await
    }

    #[cfg(unix)]
    pub(crate) fn retire(&self) {
        self.connection.retire();
    }

    #[cfg(unix)]
    pub(crate) async fn flush(
        &self,
        workspace: WorkspaceId,
        id: intent_core::PrMonitorId,
        check: bool,
    ) -> intent_core::Result<Value> {
        let services = self.services.clone();
        let connection = self.connection.clone();
        with_caller(self.caller.clone(), async {
            let scope = connection.capture();
            let mut result = None;
            scope
                .scope(Box::pin(async {
                    result = Some(
                        services
                            .pr_monitor_flush_pending(workspace, id, check)
                            .await,
                    );
                }))
                .await;
            scope.retire();
            result.unwrap()
        })
        .await
    }

    pub(crate) async fn events(
        &self,
    ) -> Arc<dyn intent_core::repository_request::RepositoryMonitorEventRecipient> {
        let connection = self.connection.clone();
        with_caller(self.caller.clone(), async {
            let scope = connection.capture();
            let mut result = None;
            scope
                .scope(Box::pin(async {
                    result = connection.capture_monitor_events();
                }))
                .await;
            scope.retire();
            result.expect("original event recipient")
        })
        .await
    }
}
impl Drop for NativeRecipient {
    fn drop(&mut self) {
        self.connection.retire();
    }
}

#[intent_test_macros::daemon_test]
async fn native_monitor_qualified_list_uses_original_target_without_runtime_authority() {
    let provider = ReadServer::new().await;
    let f = ActualRead::new(&provider).await;
    routing_options(&f);
    let registered = routing_json(&run(&f.server(), "return await ws.mr.monitor(4);").await);
    assert_eq!(registered["ok"], true);
    let id = intent_core::PrMonitorId(registered["monitor"]["monitorId"].as_str().unwrap().into());
    let services = &f.auth.service;
    let before = services.store.get_pr_monitor(&id).await.unwrap();
    // Loss of runtime registration does not turn the stored account/target into
    // permission. The native recipient must independently admit that same tuple.
    services.review_monitor_authorities.lock().unwrap().clear();
    f.git.git(
        &f.git.path,
        &[
            "remote",
            "set-url",
            "origin",
            "https://github.com/other/replacement.git",
        ],
    );
    let calls = provider.count();
    let native = NativeRecipient::new(services.clone()).await;
    let (listed, output) = native.list(f.git.workspace.id.clone()).await;
    assert_eq!(
        listed["monitors"].as_array().unwrap().len(),
        1,
        "fresh original account admission must recover the explicit monitor row"
    );
    assert_eq!(listed["monitors"][0]["monitorId"], id.0);
    assert_eq!(
        listed["monitors"][0]["resource"],
        registered["monitor"]["resource"]
    );
    assert_eq!(listed["monitors"][0]["agentId"], f.agent.0);
    assert!(!output.frame().contains("monitorBinding"));
    output.with_current(&mut || Ok(())).unwrap();
    assert_eq!(services.store.get_pr_monitor(&id).await.unwrap(), before);
    assert_eq!(
        provider.count(),
        calls,
        "list admission cannot read through today's remote"
    );
    f.git.git(&f.git.path, &["remote", "remove", "origin"]);
    let (listed, _) = native.list(f.git.workspace.id.clone()).await;
    assert_eq!(
        listed["monitors"][0]["resource"],
        registered["monitor"]["resource"]
    );
    assert_eq!(services.store.get_pr_monitor(&id).await.unwrap(), before);
}

#[intent_test_macros::daemon_test]
async fn native_monitor_qualified_list_account_replacement_retires_queued_output() {
    let provider = ReadServer::new().await;
    let f = ActualRead::new(&provider).await;
    routing_options(&f);
    let registered = routing_json(&run(&f.server(), "return await ws.mr.monitor(4);").await);
    let id = intent_core::PrMonitorId(registered["monitor"]["monitorId"].as_str().unwrap().into());
    let services = &f.auth.service;
    let original = services.store.get_pr_monitor(&id).await.unwrap();
    let native = NativeRecipient::new(services.clone()).await;
    let (first, pending) = native.list(f.git.workspace.id.clone()).await;
    assert_eq!(first["monitors"].as_array().unwrap().len(), 1);
    pending.with_current(&mut || Ok(())).unwrap();
    services
        .gitlab_connect_pat(provider.fixture.host.clone(), "pat-second".into())
        .await
        .unwrap();
    let mut wrote = false;
    assert!(pending
        .with_current(&mut || {
            wrote = true;
            Ok(())
        })
        .is_err());
    assert!(
        !wrote,
        "account replacement must fence the entire queued frame"
    );
    let calls = provider.count();
    let (other_account, _) = native.list(f.git.workspace.id.clone()).await;
    assert_eq!(
        other_account["monitors"][0]["monitorAvailability"],
        json!({"state":"unresolved","reason":"missing-authority"})
    );
    assert!(other_account["monitors"][0].get("resource").is_none());
    assert!(other_account["monitors"][0].get("lastSnapshot").is_none());
    assert_eq!(
        provider.count(),
        calls,
        "list cannot probe on another account"
    );
    assert_eq!(services.store.get_pr_monitor(&id).await.unwrap(), original);
    services
        .gitlab_connect_pat(provider.fixture.host.clone(), "stored-pat".into())
        .await
        .unwrap();
    assert!(
        pending.with_current(&mut || Ok(())).is_err(),
        "A-to-B-to-A cannot revive an old output receipt"
    );
    let (fresh, current) = native.list(f.git.workspace.id.clone()).await;
    assert_eq!(
        fresh["monitors"][0]["resource"],
        first["monitors"][0]["resource"]
    );
    current.with_current(&mut || Ok(())).unwrap();
    assert_eq!(services.store.get_pr_monitor(&id).await.unwrap(), original);
    native.connection.retire();
    assert!(current.with_current(&mut || Ok(())).is_err());
}

#[intent_test_macros::daemon_test]
async fn native_monitor_missing_account_is_visible_without_private_observations() {
    let provider = ReadServer::new().await;
    let f = ActualRead::new(&provider).await;
    routing_options(&f);
    let registered = routing_json(&run(&f.server(), "return await ws.mr.monitor(4);").await);
    let id = intent_core::PrMonitorId(registered["monitor"]["monitorId"].as_str().unwrap().into());
    let services = &f.auth.service;
    let original = services.store.get_pr_monitor(&id).await.unwrap();
    services.review_monitor_authorities.lock().unwrap().clear();
    services
        .gitlab_connect_pat(provider.fixture.host.clone(), "pat-second".into())
        .await
        .unwrap();
    let native = NativeRecipient::new(services.clone()).await;
    let calls = provider.count();
    let (listed, output) = native.list(f.git.workspace.id.clone()).await;
    assert_eq!(
        listed["monitors"].as_array().unwrap().len(),
        1,
        "held original obligations must remain inspectable"
    );
    let row = &listed["monitors"][0];
    assert_eq!(row["monitorId"], id.0);
    assert_eq!(
        row["monitorAvailability"],
        json!({"state":"unresolved","reason":"missing-authority"})
    );
    assert!(row.get("resource").is_none());
    assert!(row.get("title").is_none());
    assert!(row.get("url").is_none());
    assert!(row.get("lastSnapshot").is_none());
    assert_eq!(row["pendingChanges"], json!([]));
    assert!(!output.frame().contains("monitorBinding"));
    output.with_current(&mut || Ok(())).unwrap();
    assert_eq!(services.store.get_pr_monitor(&id).await.unwrap(), original);
    assert_eq!(provider.count(), calls);
}

#[intent_test_macros::daemon_test]
async fn native_monitor_missing_account_cancel_retains_raw_original_evidence() {
    let provider = ReadServer::new().await;
    let f = ActualRead::new(&provider).await;
    routing_options(&f);
    let registered = routing_json(&run(&f.server(), "return await ws.mr.monitor(4);").await);
    let id = intent_core::PrMonitorId(registered["monitor"]["monitorId"].as_str().unwrap().into());
    let services = &f.auth.service;
    let owner = services.store.get_primary_principal().await.unwrap();
    services
        .store
        .add_workspace_member(
            &f.git.workspace.id,
            &owner.id,
            intent_core::WorkspaceRole::Owner,
        )
        .await
        .unwrap();
    let original = services.store.get_qualified_pr_monitor(&id).await.unwrap();
    services.review_monitor_authorities.lock().unwrap().clear();
    services
        .gitlab_connect_pat(provider.fixture.host.clone(), "pat-second".into())
        .await
        .unwrap();
    let native = NativeRecipient::new(services.clone()).await;
    let calls = provider.count();
    let cancelled = native
        .cancel(f.git.workspace.id.clone(), id.clone())
        .await
        .expect("workspace manager may cancel held original obligation");
    assert_eq!(cancelled["ok"], true);
    assert_eq!(cancelled["monitor"]["state"], "cancelled");
    assert_eq!(
        cancelled["monitor"]["monitorAvailability"]["reason"],
        "missing-authority"
    );
    assert!(cancelled["monitor"].get("resource").is_none());
    assert!(cancelled["monitor"].get("title").is_none());
    let saved = services.store.get_qualified_pr_monitor(&id).await.unwrap();
    let mut expected = original.monitor().clone();
    expected.state = intent_core::PrMonitorState::Cancelled;
    expected.updated_at = saved.monitor().updated_at.clone();
    assert_eq!(saved.monitor(), &expected);
    assert_eq!(saved.target(), original.target());
    assert_eq!(provider.count(), calls);
    assert!(native.cancel(f.git.workspace.id.clone(), id).await.is_err());
}

#[intent_test_macros::daemon_test]
async fn native_monitor_admitted_cancel_emits_original_qualified_event() {
    let provider = ReadServer::new().await;
    let mut f = ActualRead::new(&provider).await;
    let bus = crate::EventBus::new(f.auth.service.store.clone());
    f.auth.service = Arc::new(f.auth.service.as_ref().clone().with_event_bus(bus.clone()));
    routing_options(&f);
    let registered = routing_json(&run(&f.server(), "return await ws.mr.monitor(4);").await);
    let id = intent_core::PrMonitorId(registered["monitor"]["monitorId"].as_str().unwrap().into());
    let services = &f.auth.service;
    let owner = services.store.get_primary_principal().await.unwrap();
    services
        .store
        .add_workspace_member(
            &f.git.workspace.id,
            &owner.id,
            intent_core::WorkspaceRole::Owner,
        )
        .await
        .unwrap();
    let before = services.store.get_qualified_pr_monitor(&id).await.unwrap();
    let source = NativeRecipient::new(services.clone()).await;
    let observer = NativeRecipient::new(services.clone()).await;
    let filter = crate::SubscriptionFilter {
        event_types: vec!["prMonitor:*".into()],
        batch_window: None,
        ..Default::default()
    };
    let mut legacy = bus.subscribe(filter.clone());
    let mut same = bus.subscribe_monitor(filter.clone(), source.events().await, "source".into());
    let mut sibling = bus.subscribe_monitor(filter, observer.events().await, "observer".into());
    let calls = provider.count();
    let (cancelled, reply) = source
        .cancel_output(f.git.workspace.id.clone(), id.clone())
        .await
        .expect("fresh original account cancellation");
    assert_eq!(
        cancelled["monitor"]["resource"],
        registered["monitor"]["resource"]
    );
    assert_eq!(cancelled["monitor"]["state"], "cancelled");
    reply.with_current(&mut || Ok(())).unwrap();
    let mut queued = Vec::new();
    for subscription in [&mut same, &mut sibling] {
        let delivery = timeout(BUDGET, subscription.recv_delivery())
            .await
            .unwrap()
            .unwrap();
        let crate::Delivery::Monitor(delivery) = delivery else {
            panic!("qualified event")
        };
        let output = delivery.into_output();
        output.with_current(&mut || Ok(())).unwrap();
        let frame: Value = serde_json::from_str(output.frame()).unwrap();
        assert_eq!(frame["params"]["event"]["type"], "prMonitor:cancelled");
        assert_eq!(
            frame["params"]["event"]["data"]["resource"],
            registered["monitor"]["resource"]
        );
        assert!(!output.frame().contains("monitorBinding"));
        queued.push(output);
    }
    assert!(legacy.try_recv_delivery().is_none());
    assert_eq!(provider.count(), calls);
    let saved = services.store.get_qualified_pr_monitor(&id).await.unwrap();
    let mut expected = before.monitor().clone();
    expected.state = intent_core::PrMonitorState::Cancelled;
    expected.updated_at = saved.monitor().updated_at.clone();
    assert_eq!(saved.monitor(), &expected);
    services
        .gitlab_connect_pat(provider.fixture.host.clone(), "pat-second".into())
        .await
        .unwrap();
    for output in queued.into_iter().chain([reply]) {
        let mut wrote = false;
        assert!(output
            .with_current(&mut || {
                wrote = true;
                Ok(())
            })
            .is_err());
        assert!(!wrote);
    }
}

pub(crate) async fn next_monitor_event(
    subscription: &mut crate::events::Subscription,
    kind: &str,
) -> Arc<dyn RepositoryPrivateOutput> {
    let delivery = timeout(BUDGET, subscription.recv_delivery())
        .await
        .expect("actual monitor lifecycle event")
        .unwrap();
    let crate::Delivery::Monitor(delivery) = delivery else {
        panic!("private lifecycle envelope")
    };
    let output = delivery.into_output();
    output.with_current(&mut || Ok(())).unwrap();
    let frame: Value = serde_json::from_str(output.frame()).unwrap();
    assert_eq!(frame["params"]["event"]["type"], kind);
    output
}

#[intent_test_macros::daemon_test]
async fn native_monitor_agent_registration_change_and_cancel_use_original_source() {
    let provider = ReadServer::new().await;
    let mut f = ActualRead::new(&provider).await;
    let bus = crate::EventBus::new(f.auth.service.store.clone());
    f.auth.service = Arc::new(f.auth.service.as_ref().clone().with_event_bus(bus.clone()));
    routing_options(&f);
    let native = NativeRecipient::new(f.auth.service.clone()).await;
    let filter = crate::SubscriptionFilter {
        event_types: vec!["prMonitor:*".into()],
        batch_window: None,
        ..Default::default()
    };
    let mut legacy = bus.subscribe(filter.clone());
    let mut events = bus.subscribe_monitor(filter, native.events().await, "agent-events".into());
    let registered = routing_json(&run(&f.server(), "return await ws.mr.monitor(4);").await);
    assert_eq!(registered["ok"], true);
    let registration = next_monitor_event(&mut events, "prMonitor:registered").await;
    let value: Value = serde_json::from_str(registration.frame()).unwrap();
    assert_eq!(
        value["params"]["event"]["data"]["resource"],
        registered["monitor"]["resource"]
    );
    provider.set_monitor_review("new-head", "opened", 1);
    f.auth.service.poll_pr_monitors().await;
    let changed = next_monitor_event(&mut events, "prMonitor:changed").await;
    let cancelled = routing_json(&run(&f.server(), "return await ws.mr.unmonitor(4);").await);
    assert_eq!(cancelled["ok"], true);
    let cancelled = next_monitor_event(&mut events, "prMonitor:cancelled").await;
    assert!(legacy.try_recv_delivery().is_none());
    // Neither a removed runtime map entry nor the subscriber's capability is
    // authority. Every queued envelope keeps its actual original agent source.
    f.owner.retirement().retire();
    for output in [registration, changed, cancelled] {
        let mut wrote = false;
        assert!(output
            .with_current(&mut || {
                wrote = true;
                Ok(())
            })
            .is_err());
        assert!(!wrote);
    }
}
