use super::*;
use intent_core::review_monitor::{
    ReviewMonitorOutput, ReviewMonitorRecipientSupport, ReviewMonitoringCapabilities,
};

async fn capture_receipt(socket: &Socket) -> Option<Arc<super::super::monitor::Receipt>> {
    socket
        .request(async { Ok(NATIVE_REQUEST.with(|request| request.monitor_receipt.clone())) })
        .await
        .unwrap()
}

fn supported(receipt: Option<&Arc<super::super::monitor::Receipt>>) -> bool {
    receipt
        .and_then(|receipt| receipt.support())
        .and_then(|support| {
            ReviewMonitorOutput::Qualified(()).project(
                &ReviewMonitoringCapabilities {
                    review_monitoring: Some(1),
                },
                support,
            )
        })
        .is_some()
}

#[intent_test_macros::daemon_test]
async fn native_monitor_receipt_requires_ack_and_never_survives_rehello_or_socket_replacement() {
    let fixture = Fixture::new().await;
    let socket = fixture.socket().await;
    assert!(!supported(capture_receipt(&socket).await.as_ref()));
    let capable = ReviewMonitorRecipientSupport::from_capabilities(&json!({"reviewMonitoring":1}));
    let first = socket
        .entered(async { socket.owner.review_monitoring_hello(capable).unwrap() })
        .await;
    let before_ack = capture_receipt(&socket).await;
    assert!(
        before_ack.is_none(),
        "an unacknowledged hello is not support"
    );
    socket.entered(async { first.acknowledge() }).await;
    let original = capture_receipt(&socket).await.unwrap();
    assert!(
        before_ack.is_none(),
        "a queued request cannot acquire a later receipt"
    );
    assert!(supported(Some(&original)));

    let legacy = socket
        .entered(async {
            socket
                .owner
                .review_monitoring_hello(ReviewMonitorRecipientSupport::default())
                .unwrap()
        })
        .await;
    assert!(
        !supported(Some(&original)),
        "replacement retires immediately"
    );
    socket.entered(async { first.acknowledge() }).await;
    assert!(
        !supported(Some(&original)),
        "late acknowledgement cannot resurrect A"
    );
    socket.entered(async { legacy.acknowledge() }).await;
    assert!(!supported(capture_receipt(&socket).await.as_ref()));

    let fresh = socket
        .entered(async { socket.owner.review_monitoring_hello(capable).unwrap() })
        .await;
    socket.entered(async { fresh.acknowledge() }).await;
    let replacement = capture_receipt(&socket).await.unwrap();
    assert!(supported(Some(&replacement)));
    assert!(
        !supported(Some(&original)),
        "A to B to A never revives A's receipt"
    );
    let other = fixture.socket().await;
    assert!(!supported(capture_receipt(&other).await.as_ref()));
    socket.owner.retire();
    assert!(!supported(Some(&replacement)), "physical retirement wins");
    socket.entered(async { fresh.acknowledge() }).await;
    assert!(!supported(Some(&replacement)));
}

async fn unresolved_row(fixture: &Fixture) -> intent_core::PrMonitor {
    use intent_core::{AgentId, PrMonitor, PrMonitorId, PrMonitorState};
    let workspace = fixture.git.workspace.id.clone();
    let agent = AgentId::new();
    sqlx::query("INSERT INTO agent_session(id,workspace_id,name,status,created_at,updated_at) VALUES (?,?,'Original owner','idle','created','updated')")
        .bind(&agent.0).bind(&workspace.0).execute(fixture.services.store.write_pool()).await.unwrap();
    let row = PrMonitor {
        monitor_id: PrMonitorId::new(),
        workspace_id: workspace,
        agent_id: agent,
        repo_owner: "original".into(),
        repo_name: "imported".into(),
        pr_number: 42,
        state: PrMonitorState::Active,
        last_snapshot: Some("{\"private\":\"original observation\"}".into()),
        baseline_snapshot: Some("original baseline bytes".into()),
        pending_changes: vec!["private pending text".into()],
        pending_since: Some("pending".into()),
        last_change_at: Some("changed".into()),
        last_polled_at: Some("polled".into()),
        last_error: Some("private imported error".into()),
        created_at: "created".into(),
        updated_at: "updated".into(),
    };
    assert!(fixture
        .services
        .store
        .insert_pr_monitor(&row)
        .await
        .unwrap());
    sqlx::query("UPDATE pr_monitor SET target_provider=NULL,target_instance_base_url=NULL,target_project_path=NULL,target_kind=NULL,target_provenance='unresolved',target_unresolved_reason='unverified-import' WHERE monitor_id=?")
        .bind(&row.monitor_id.0).execute(fixture.services.store.write_pool()).await.unwrap();
    row
}

#[intent_test_macros::daemon_test]
async fn native_monitor_unresolved_list_retains_original_workspace_and_output_receipt() {
    let fixture = Fixture::new().await;
    let row = unresolved_row(&fixture).await;
    let socket = fixture.socket().await;
    let legacy = socket
        .request(
            fixture
                .services
                .pr_monitor_list(row.workspace_id.clone(), None),
        )
        .await
        .unwrap();
    assert!(legacy["monitors"].as_array().unwrap().is_empty());
    let capable = ReviewMonitorRecipientSupport::from_capabilities(&json!({"reviewMonitoring":1}));
    let hello = socket
        .entered(async { socket.owner.review_monitoring_hello(capable).unwrap() })
        .await;
    socket.entered(async { hello.acknowledge() }).await;
    socket
        .entered(async {
            let scope = socket.owner.capture();
            let mut result = None;
            scope
                .scope(Box::pin(async {
                    let value = fixture
                        .services
                        .pr_monitor_list(row.workspace_id.clone(), None)
                        .await
                        .unwrap();
                    let rows = value["monitors"].as_array().unwrap();
                    assert_eq!(
                        rows.len(),
                        1,
                        "capable original recipient can inspect unresolved stored evidence"
                    );
                    assert_eq!(rows[0]["monitorId"], json!(row.monitor_id));
                    assert_eq!(rows[0]["repo"], "original/imported");
                    assert_eq!(
                        rows[0]["monitorAvailability"],
                        json!({"state":"unresolved","reason":"unverified-import"})
                    );
                    assert!(rows[0].get("resource").is_none());
                    assert!(!value.to_string().contains("private pending text"));
                    assert!(!value.to_string().contains("private imported error"));
                    let output = scope
                        .bind_private_output(&value.to_string())
                        .unwrap()
                        .expect("qualified list retains original final output owner");
                    scope
                        .deliver(RepositoryReadReplyKind::Result, &mut || Ok(()))
                        .await
                        .unwrap();
                    result = Some(output);
                }))
                .await;
            scope.retire();
            let output = result.unwrap();
            let mut effects = 0;
            output
                .with_current(&mut || {
                    effects += 1;
                    Ok(())
                })
                .unwrap();
            assert_eq!(
                effects, 1,
                "completed request retains detached output admission"
            );
            let withdrawn = socket
                .owner
                .review_monitoring_hello(ReviewMonitorRecipientSupport::default())
                .unwrap();
            withdrawn.acknowledge();
            assert!(output
                .with_current(&mut || {
                    effects += 1;
                    Ok(())
                })
                .is_err());
            assert_eq!(effects, 1);
        })
        .await;
    assert_eq!(
        fixture
            .services
            .store
            .get_pr_monitor(&row.monitor_id)
            .await
            .unwrap(),
        row
    );
}

async fn enable_monitor(socket: &Socket) {
    socket
        .entered(async {
            let hello = socket
                .owner
                .review_monitoring_hello(ReviewMonitorRecipientSupport::from_capabilities(
                    &json!({"reviewMonitoring":1}),
                ))
                .unwrap();
            hello.acknowledge();
        })
        .await;
}

async fn list_output(
    fixture: &Fixture,
    socket: &Socket,
) -> Result<Arc<dyn intent_core::repository_request::RepositoryPrivateOutput>> {
    socket
        .entered(async {
            let scope = socket.owner.capture();
            let mut result = None;
            scope
                .scope(Box::pin(async {
                    result = Some(
                        async {
                            let value = fixture
                                .services
                                .pr_monitor_list(fixture.git.workspace.id.clone(), None)
                                .await?;
                            let output = scope
                                .bind_private_output(&value.to_string())?
                                .ok_or_else(unavailable)?;
                            scope
                                .deliver(RepositoryReadReplyKind::Result, &mut || Ok(()))
                                .await?;
                            Ok(output)
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

#[intent_test_macros::daemon_test]
async fn native_monitor_unresolved_membership_and_physical_receipts_never_reparent() {
    let f = Fixture::new().await;
    unresolved_row(&f).await;
    let guest = f.guest("monitor-member", false).await;
    enable_monitor(&guest).await;
    assert!(list_output(&f, &guest).await.is_err());
    let Caller::Wire { principal_id, .. } = &guest.caller else {
        panic!()
    };
    f.services
        .store
        .add_workspace_member(
            &f.git.workspace.id,
            principal_id,
            WorkspaceRole::Collaborator,
        )
        .await
        .unwrap();
    let before = list_output(&f, &guest).await.unwrap();
    f.services
        .store
        .remove_workspace_member(&f.git.workspace.id, principal_id)
        .await
        .unwrap();
    f.services
        .store
        .add_workspace_member(
            &f.git.workspace.id,
            principal_id,
            WorkspaceRole::Collaborator,
        )
        .await
        .unwrap();
    assert!(
        before.with_current(&mut || Ok(())).is_err(),
        "re-adding a member never repairs queued private output"
    );
    let fresh = list_output(&f, &guest).await.unwrap();
    fresh.with_current(&mut || Ok(())).unwrap();
    let independent = f.socket().await;
    enable_monitor(&independent).await;
    let independent_output = list_output(&f, &independent).await.unwrap();
    guest.owner.retire();
    assert!(fresh.with_current(&mut || Ok(())).is_err());
    independent_output.with_current(&mut || Ok(())).unwrap();
}

#[intent_test_macros::daemon_test]
async fn native_monitor_unresolved_cancel_preserves_original_evidence_without_provider_reads() {
    let f = Fixture::new().await;
    let row = unresolved_row(&f).await;
    let socket = f.socket().await;
    let legacy = socket
        .request(
            f.services
                .pr_monitor_cancel_by_id(row.workspace_id.clone(), row.monitor_id.clone()),
        )
        .await;
    assert!(legacy.is_err());
    assert_eq!(
        f.services
            .store
            .get_pr_monitor(&row.monitor_id)
            .await
            .unwrap(),
        row
    );
    enable_monitor(&socket).await;
    let result = socket
        .request(
            f.services
                .pr_monitor_cancel_by_id(row.workspace_id.clone(), row.monitor_id.clone()),
        )
        .await
        .unwrap();
    assert_eq!(result["ok"], true);
    assert_eq!(result["monitor"]["state"], "cancelled");
    assert!(!result.to_string().contains("private pending text"));
    let saved = f
        .services
        .store
        .get_qualified_pr_monitor(&row.monitor_id)
        .await
        .unwrap();
    let mut expected = row.clone();
    expected.state = intent_core::PrMonitorState::Cancelled;
    expected.updated_at = saved.monitor().updated_at.clone();
    assert_eq!(saved.monitor(), &expected);
    assert!(matches!(
        saved.target(),
        intent_store::PersistedMonitorTarget::Unresolved {
            reason: intent_store::MonitorTargetUnresolvedReason::UnverifiedImport
        }
    ));
    assert!(
        socket
            .request(
                f.services
                    .pr_monitor_cancel_by_id(row.workspace_id.clone(), row.monitor_id.clone())
            )
            .await
            .is_err(),
        "terminal row must not be rearmed"
    );
}

#[intent_test_macros::daemon_test]
async fn native_monitor_unresolved_cancel_fences_waiting_writer_and_full_row() {
    for change in ["live", "socket", "hello", "row"] {
        let f = Fixture::new().await;
        let row = unresolved_row(&f).await;
        let socket = Arc::new(f.socket().await);
        enable_monitor(&socket).await;
        let ready = Arc::new(Notify::new());
        *f.services.test_review_monitor_write_ready.lock().unwrap() = Some(ready.clone());
        assert_eq!(
            f.services
                .store
                .write_pool()
                .options()
                .get_max_connections(),
            1
        );
        let mut writer = f.services.store.write_pool().acquire().await.unwrap();
        let services = f.services.clone();
        let original = row.clone();
        let task_socket = socket.clone();
        let task = tokio::spawn(async move {
            task_socket
                .request(
                    services.pr_monitor_cancel_by_id(original.workspace_id, original.monitor_id),
                )
                .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(10), ready.notified())
            .await
            .unwrap();
        assert!(!task.is_finished());
        assert_eq!(
            f.services
                .store
                .get_pr_monitor(&row.monitor_id)
                .await
                .unwrap(),
            row
        );
        match change {
            "socket" => socket.owner.retire(),
            "hello" => enable_monitor(&socket).await,
            "row" => {
                sqlx::query("UPDATE pr_monitor SET pending_changes = ? WHERE monitor_id = ?")
                    .bind("[ \"new original evidence\" ]")
                    .bind(&row.monitor_id.0)
                    .execute(&mut *writer)
                    .await
                    .unwrap();
            }
            _ => {}
        }
        let expected = f
            .services
            .store
            .get_pr_monitor(&row.monitor_id)
            .await
            .unwrap();
        drop(writer);
        let result = tokio::time::timeout(std::time::Duration::from_secs(10), task)
            .await
            .unwrap()
            .unwrap();
        let saved = f
            .services
            .store
            .get_pr_monitor(&row.monitor_id)
            .await
            .unwrap();
        if change == "live" {
            assert_eq!(result.unwrap()["ok"], true);
            assert_eq!(saved.state, intent_core::PrMonitorState::Cancelled);
        } else {
            assert!(
                result.is_err(),
                "{change} must refuse the captured cancellation"
            );
            assert_eq!(saved, expected, "{change} changed original evidence");
        }
    }
}

#[intent_test_macros::daemon_test]
async fn native_monitor_unresolved_cancel_requires_manager_not_recipient_support() {
    let f = Fixture::new().await;
    let row = unresolved_row(&f).await;
    let guest = f.guest("monitor-collaborator", true).await;
    enable_monitor(&guest).await;
    assert!(list_output(&f, &guest).await.is_ok());
    assert!(guest
        .request(
            f.services
                .pr_monitor_cancel_by_id(row.workspace_id.clone(), row.monitor_id.clone(),)
        )
        .await
        .is_err());
    assert_eq!(
        f.services
            .store
            .get_pr_monitor(&row.monitor_id)
            .await
            .unwrap(),
        row
    );
    let Caller::Wire { principal_id, .. } = &guest.caller else {
        panic!()
    };
    let owner = f.services.store.get_primary_principal().await.unwrap();
    f.services
        .store
        .set_workspace_member_role(&row.workspace_id, &owner.id, WorkspaceRole::Collaborator)
        .await
        .unwrap();
    f.services
        .store
        .set_workspace_member_role(&row.workspace_id, principal_id, WorkspaceRole::Owner)
        .await
        .unwrap();
    assert_eq!(
        guest
            .request(
                f.services
                    .pr_monitor_cancel_by_id(row.workspace_id.clone(), row.monitor_id.clone(),)
            )
            .await
            .unwrap()["ok"],
        true
    );
}

#[intent_test_macros::daemon_test]
async fn native_monitor_unresolved_cancel_event_retains_source_and_original_subscriber() {
    for retire_source in [false, true] {
        let git = GitFixture::new().await;
        let bus = crate::EventBus::new(git.store.clone());
        let services = Arc::new(
            Services::new_repository_fixture(
                git.store.clone(),
                intent_core::FileSecretStore::with_path(git.dir.path().join("event-secrets.json")),
                None,
            )
            .with_event_bus(bus.clone()),
        );
        services.initialize_repository_wire().await.unwrap();
        let principal = services.store.get_primary_principal().await.unwrap();
        let f = Fixture {
            git,
            services,
            caller: Caller::Wire {
                principal_id: principal.id,
                host_role: HostRole::Owner,
            },
        };
        let row = unresolved_row(&f).await;
        let source = f.socket().await;
        enable_monitor(&source).await;
        let observer = f.socket().await;
        enable_monitor(&observer).await;
        let filter = crate::SubscriptionFilter {
            event_types: vec!["prMonitor:*".into()],
            batch_window: None,
            ..Default::default()
        };
        let mut legacy = bus.subscribe(filter.clone());
        let original = observer
            .request(async { Ok(observer.owner.capture_monitor_events().unwrap()) })
            .await
            .unwrap();
        let mut native =
            bus.subscribe_monitor(filter.clone(), original, "original-subscription".into());
        let same = source
            .request(async { Ok(source.owner.capture_monitor_events().unwrap()) })
            .await
            .unwrap();
        let mut same_socket = bus.subscribe_monitor(filter, same, "same-socket".into());
        source
            .request(
                f.services
                    .pr_monitor_cancel_by_id(row.workspace_id.clone(), row.monitor_id.clone()),
            )
            .await
            .unwrap();
        let output =
            tokio::time::timeout(std::time::Duration::from_secs(10), native.recv_delivery())
                .await
                .unwrap()
                .unwrap();
        let crate::Delivery::Monitor(output) = output else {
            panic!("qualified event must retain private output")
        };
        let output = output.into_output();
        output.with_current(&mut || Ok(())).unwrap();
        let value: serde_json::Value = serde_json::from_str(output.frame()).unwrap();
        assert_eq!(value["method"], "events.event");
        assert_eq!(value["params"]["subscriptionId"], "original-subscription");
        assert_eq!(value["params"]["event"]["type"], "prMonitor:cancelled");
        assert_eq!(
            value["params"]["event"]["data"]["monitorId"],
            row.monitor_id.0
        );
        assert!(!output.frame().contains("private pending text"));
        let same_output = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            same_socket.recv_delivery(),
        )
        .await
        .unwrap()
        .unwrap();
        let crate::Delivery::Monitor(same_output) = same_output else {
            panic!()
        };
        same_output
            .into_output()
            .with_current(&mut || Ok(()))
            .unwrap();
        assert!(
            legacy.try_recv_delivery().is_none(),
            "ordinary subscriptions withhold entire qualified envelope"
        );
        if retire_source {
            source.owner.retire();
        } else {
            observer.owner.retire();
        }
        assert!(output.with_current(&mut || Ok(())).is_err());
        // A new supported socket never restores an already queued original frame.
        let replacement = f.socket().await;
        enable_monitor(&replacement).await;
        assert!(output.with_current(&mut || Ok(())).is_err());
    }
}
