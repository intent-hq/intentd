use super::*;

async fn pending_monitor(
    svc: &Services,
    ws: &WorkspaceId,
    child: &AgentId,
) -> intent_core::ScriptMonitor {
    let mut row = intent_core::ScriptMonitor {
        monitor_id: uuid::Uuid::new_v4().to_string(),
        workspace_id: ws.clone(),
        agent_id: child.clone(),
        script_id: uuid::Uuid::new_v4().to_string(),
        run_id: uuid::Uuid::new_v4().to_string(),
        script_name: "completion fixture".into(),
        mode: intent_core::ScriptMode::Command,
        state: "active".into(),
        created_at: now_iso(),
        expires_at: now_iso(),
        output_pattern: None,
        line_count: None,
        settled_at: None,
        reason: None,
        result: None,
        trigger: None,
    };
    svc.store.insert_script_monitor(&row).await.unwrap();
    row.state = "expired".into();
    row.reason = Some("ttl-expired".into());
    row.settled_at = Some(now_iso());
    assert!(svc.store.settle_script_monitor(&row).await.unwrap());
    row
}

// This completion-classifier harness has no agent manager. Exercise actual
// transcript and durable queue transitions explicitly, then simulate the
// successful worker acknowledgement that permits completion to settle.
async fn deliver_pending_notification(svc: &Services, row: &intent_core::ScriptMonitor) {
    FakeRuntime::of(svc).dispatch_script_monitor(svc, row).await;
    let Some(job) = svc
        .store
        .script_notification_jobs()
        .await
        .unwrap()
        .into_iter()
        .find(|job| job.wake_id == format!("script-monitor:{}", row.monitor_id))
    else {
        return;
    };
    let queued = intent_store::AgentQueueRow {
        id: job.wake_id.clone(),
        agent_id: row.agent_id.clone(),
        position: 0,
        payload: json!({"id":job.wake_id,"turnId":job.wake_id,"messageMetadata":job.metadata}),
        created_at: now_iso(),
        turn_id: job.wake_id.clone(),
    };
    svc.store
        .append_agent_message_with_provenance(
            &row.agent_id,
            &job.wake_id,
            "user",
            &json!([{"type":"text","text":job.body}]),
            Some(&job.metadata),
            &now_iso(),
            intent_store::UsageMessageOrigin::Excluded,
        )
        .await
        .unwrap();
    assert!(svc.store.admit_script_notification(&queued).await.unwrap());
    assert!(svc
        .store
        .accept_script_notification(&row.agent_id, &row.monitor_id)
        .await
        .unwrap());
    svc.store
        .complete_script_notification(&row.agent_id, &row.monitor_id)
        .await
        .unwrap();
    svc.redeliver_completion_after_queue_mutation(&row.agent_id)
        .await;
}

// Catch invalid admission at the first classification park, before it can
// recurse. Dropping the pinned future cancels that pass at the assertion.
async fn assert_deferred_before_delivery(
    svc: &Services,
    child: &AgentId,
    park: &crate::CompletionClassifyPark,
) {
    timeout(intentd_test_support::test_timeout(Duration::from_secs(2)), async {
        tokio::select! {
            () = svc.redeliver_completion_after_queue_mutation(child) => {},
            () = park.entered.notified() => panic!("pending notification admitted synthetic completion"),
        }
    }).await.expect("bounded completion admission");
    assert!(svc.has_interim_skipped_idle(child));
}

#[intent_test_macros::daemon_test]
async fn pending_notification_preserves_marker_before_synthetic_delivery() {
    let (_tmp, svc, ws) = setup().await;
    let park = Arc::new(crate::CompletionClassifyPark::default());
    let svc = svc.with_completion_classify_park(park.clone());
    let child = create_agent(&svc, &ws, "Child").await;
    pending_monitor(&svc, &ws, &child).await;
    svc.mark_interim_skipped_idle_stale_report(&child);
    assert_deferred_before_delivery(&svc, &child, &park).await;
    assert!(svc.has_stale_report_interim_skip(&child));
    svc.shutdown_agent_deliveries().await;
}

#[intent_test_macros::daemon_test]
async fn pending_notification_blocks_group_seal_before_active_wait_guard() {
    let (_tmp, svc, ws) = setup().await;
    let child = create_agent(&svc, &ws, "Coordinator").await;
    delegate_after_all(&svc, &ws, &child).await;
    seed_active_hook(&svc, &ws, &child, "active wait").await;
    pending_monitor(&svc, &ws, &child).await;
    svc.mark_interim_skipped_idle_stale_report(&child);
    svc.redeliver_completion_after_queue_mutation(&child).await;
    assert!(
        !svc.delegation_group_for_parent(&child).unwrap().sealed,
        "pending notification must keep the delegating turn open"
    );
    assert!(svc.has_stale_report_interim_skip(&child));
    svc.shutdown_agent_deliveries().await;
}

async fn watch_child(svc: &Services, ws: &WorkspaceId, child: &AgentId) -> AgentId {
    let parent = create_agent(svc, ws, "Watcher").await;
    svc.register_completion_watch(
        ws,
        ws,
        parent.clone(),
        "Watcher".into(),
        child.clone(),
        None,
    )
    .unwrap();
    parent
}

async fn break_pending_query(svc: &Services) {
    // Only the outbox predicate fails; session and active-monitor reads still
    // work. The fixture has no pending row, so recovery needs its own trigger.
    sqlx::query(
        "ALTER TABLE script_notification_job RENAME TO unavailable_script_notification_job",
    )
    .execute(svc.store.write_pool())
    .await
    .unwrap();
}

async fn restore_pending_query(svc: &Services) {
    sqlx::query(
        "ALTER TABLE unavailable_script_notification_job RENAME TO script_notification_job",
    )
    .execute(svc.store.write_pool())
    .await
    .unwrap();
}

async fn wait_for_completion(svc: &Services, parent: &AgentId, child: &AgentId) {
    timeout(
        intentd_test_support::test_timeout(Duration::from_secs(10)),
        async {
            loop {
                if parent_message_count(svc, parent).await == 1
                    && svc.find_watches_for_child(child).is_empty()
                    && svc.pending_completion_retries.lock().unwrap().is_empty()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        },
    )
    .await
    .expect("completion and retry ownership settle");
    assert!(!svc.has_interim_skipped_idle(child));
}

#[intent_test_macros::daemon_test]
async fn pending_query_error_preserves_advisory_provenance_before_delivery() {
    let (_tmp, svc, ws) = setup().await;
    let park = Arc::new(crate::CompletionClassifyPark::default());
    let svc = svc.with_completion_classify_park(park.clone());
    let child = create_agent(&svc, &ws, "Child").await;
    break_pending_query(&svc).await;
    svc.mark_interim_skipped_idle_advisory_pending(&child);
    assert_deferred_before_delivery(&svc, &child, &park).await;
    assert!(svc.has_advisory_pending_interim_skip(&child));
    svc.shutdown_agent_deliveries().await;
}

#[intent_test_macros::daemon_test]
async fn pending_query_error_recovers_without_pending_row_or_another_event() {
    let (_tmp, svc, ws) = setup().await;
    let child = create_agent(&svc, &ws, "Child").await;
    let parent = watch_child(&svc, &ws, &child).await;
    break_pending_query(&svc).await;
    let event = completion_event(&ws, AGENT_IDLE, &child, json!({"agentId":child.0}));
    timeout(
        intentd_test_support::test_timeout(Duration::from_secs(2)),
        svc.handle_completion_event(&event),
    )
    .await
    .unwrap();
    assert!(svc.has_interim_skipped_idle(&child));
    assert_eq!(parent_message_count(&svc, &parent).await, 0);
    for _ in 0..8 {
        svc.redeliver_completion_after_queue_mutation(&child).await;
    }
    assert_eq!(
        svc.pending_completion_retries.lock().unwrap().len(),
        1,
        "repeated deferrals share one worker"
    );
    // Let the worker itself encounter the fault before fixing it, so the
    // generation and backoff loop, not just its initial timer, are covered.
    let generation = svc.pending_completion_retries.lock().unwrap()[&child];
    timeout(
        intentd_test_support::test_timeout(Duration::from_secs(5)),
        async {
            loop {
                if svc.pending_completion_retries.lock().unwrap()[&child] > generation {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        },
    )
    .await
    .expect("worker retries the unreadable outbox");
    restore_pending_query(&svc).await;
    wait_for_completion(&svc, &parent, &child).await;
    svc.handle_completion_event(&event).await;
    assert_eq!(parent_message_count(&svc, &parent).await, 1);
    svc.shutdown_agent_deliveries().await;
}

#[intent_test_macros::daemon_test]
async fn pending_notifications_allow_other_dispatches_and_settle_exactly_once() {
    let (_tmp, svc, ws) = setup().await;
    let svc = FakeRuntime::install(svc);
    let child = create_agent(&svc, &ws, "Child").await;
    let parent = watch_child(&svc, &ws, &child).await;
    let other = create_agent(&svc, &ws, "Other").await;
    let first = pending_monitor(&svc, &ws, &child).await;
    let second = pending_monitor(&svc, &ws, &child).await;
    let unrelated = pending_monitor(&svc, &ws, &other).await;
    // Even an explicit report must wait for the pending notification.
    let event = completion_event(
        &ws,
        AGENT_IDLE,
        &child,
        json!({"agentId":child.0,"completionReport":"done"}),
    );
    timeout(
        intentd_test_support::test_timeout(Duration::from_secs(2)),
        svc.handle_completion_event(&event),
    )
    .await
    .unwrap();
    assert!(svc.has_interim_skipped_idle(&child));
    assert_eq!(parent_message_count(&svc, &parent).await, 0);
    // Same sequential dispatch shape used by notification maintenance: the
    // first owner's remaining pending row must not block unrelated owners.
    timeout(
        intentd_test_support::test_timeout(Duration::from_secs(2)),
        async {
            deliver_pending_notification(&svc, &first).await;
            deliver_pending_notification(&svc, &unrelated).await;
        },
    )
    .await
    .expect("maintenance can advance to the next owner");
    assert!(!svc
        .store
        .script_monitor_wake_pending(&unrelated.monitor_id)
        .await
        .unwrap());
    assert!(svc
        .store
        .script_monitor_wake_pending(&second.monitor_id)
        .await
        .unwrap());
    assert_eq!(parent_message_count(&svc, &parent).await, 0);
    deliver_pending_notification(&svc, &second).await;
    wait_for_completion(&svc, &parent, &child).await;
    deliver_pending_notification(&svc, &first).await;
    deliver_pending_notification(&svc, &second).await;
    svc.handle_completion_event(&event).await;
    assert_eq!(parent_message_count(&svc, &parent).await, 1);
    assert_eq!(
        parent_message_count(&svc, &child).await,
        2,
        "each script wake is durable once"
    );
    svc.shutdown_agent_deliveries().await;
}

#[intent_test_macros::daemon_test]
async fn pending_notification_settling_before_marker_is_retried() {
    let (_tmp, svc, ws) = setup().await;
    let svc = FakeRuntime::install(svc);
    let child = create_agent(&svc, &ws, "Child").await;
    let parent = watch_child(&svc, &ws, &child).await;
    let row = pending_monitor(&svc, &ws, &child).await;
    let park = Arc::new(crate::CompletionClassifyPark::default());
    let parked = svc.clone().with_completion_classify_park(park.clone());
    let event = completion_event(&ws, AGENT_IDLE, &child, json!({"agentId":child.0}));
    let delivery = tokio::spawn(async move { parked.handle_completion_event(&event).await });
    timeout(
        intentd_test_support::test_timeout(Duration::from_secs(2)),
        park.entered.notified(),
    )
    .await
    .unwrap();
    assert!(!svc.has_interim_skipped_idle(&child));
    deliver_pending_notification(&svc, &row).await;
    assert_eq!(parent_message_count(&svc, &parent).await, 0);
    park.release.notify_one();
    timeout(
        intentd_test_support::test_timeout(Duration::from_secs(2)),
        delivery,
    )
    .await
    .unwrap()
    .unwrap();
    // The deferred retry observes settlement after the marker exists.
    timeout(
        intentd_test_support::test_timeout(Duration::from_secs(5)),
        park.entered.notified(),
    )
    .await
    .unwrap();
    park.release.notify_one();
    wait_for_completion(&svc, &parent, &child).await;
    svc.shutdown_agent_deliveries().await;
}

#[intent_test_macros::daemon_test]
async fn pending_synthetic_reclassification_keeps_stale_report_provenance() {
    let (_tmp, svc, ws) = setup().await;
    let svc = FakeRuntime::install(svc);
    let child = create_agent(&svc, &ws, "Child").await;
    let parent = watch_child(&svc, &ws, &child).await;
    let row = pending_monitor(&svc, &ws, &child).await;
    // Model an outbox entry arriving after synthetic admission consumed a
    // stale-report marker. Its delivery snapshot must retain that provenance.
    let event = completion_event(&ws, AGENT_IDLE, &child, json!({"agentId":child.0}));
    let classification = svc
        .deliver_completion_to_watches_inner(
            &child,
            &event,
            false,
            true,
            None,
            Some(crate::InterimIdleProvenance::StaleReport),
        )
        .await;
    assert!(classification.queue_interim);
    assert!(svc.has_stale_report_interim_skip(&child));
    sqlx::query("UPDATE agent_session SET completion_report='old report' WHERE id=?")
        .bind(child.as_str())
        .execute(svc.store.write_pool())
        .await
        .unwrap();
    deliver_pending_notification(&svc, &row).await;
    wait_for_completion(&svc, &parent, &child).await;
    assert!(!parent_messages_text(&svc, &parent)
        .await
        .contains("old report"));
    svc.shutdown_agent_deliveries().await;
}

#[intent_test_macros::daemon_test]
async fn pending_advisory_provenance_clears_when_notification_settles() {
    let (_tmp, svc, ws) = setup().await;
    let svc = FakeRuntime::install(svc);
    let child = create_agent(&svc, &ws, "Child").await;
    let parent = watch_child(&svc, &ws, &child).await;
    seed_active_hook(&svc, &ws, &child, "still monitoring").await;
    let row = pending_monitor(&svc, &ws, &child).await;
    svc.mark_interim_skipped_idle_advisory_pending(&child);
    assert!(svc.has_advisory_pending_interim_skip(&child));
    // Settle directly so this test alone drives the first admission attempt;
    // no dispatch callback or retry worker competes for the marker/park.
    svc.store
        .finish_script_monitor_wake(&row.monitor_id, true)
        .await
        .unwrap();
    let park = Arc::new(crate::CompletionClassifyPark::default());
    let svc = svc.with_completion_classify_park(park.clone());
    let delivery = svc.redeliver_completion_after_queue_mutation(&child);
    tokio::pin!(delivery);
    timeout(intentd_test_support::test_timeout(Duration::from_secs(2)), async {
        tokio::select! {
            () = &mut delivery => panic!("owed advisory must enter delivery"),
            () = park.entered.notified() => {},
        }
        park.release.notify_one();
        tokio::select! {
            () = &mut delivery => {},
            () = park.entered.notified() => panic!("delivered advisory incorrectly re-entered delivery"),
        }
    }).await.expect("one bounded advisory delivery");
    assert!(!svc.has_advisory_pending_interim_skip(&child));
    assert_eq!(parent_message_count(&svc, &parent).await, 1);
    assert_eq!(svc.find_watches_for_child(&child).len(), 1);
    svc.redeliver_completion_after_queue_mutation(&child).await;
    assert_eq!(parent_message_count(&svc, &parent).await, 1);
    svc.shutdown_agent_deliveries().await;
}

#[intent_test_macros::daemon_test]
async fn removing_queued_monitor_wake_suppresses_durable_restart_delivery() {
    let (tmp, svc, ws) = setup().await;
    let agent = create_agent(&svc, &ws, "Notification owner").await;
    let monitor = pending_monitor(&svc, &ws, &agent).await;
    let job = svc
        .store
        .script_notification_jobs()
        .await
        .unwrap()
        .remove(0);
    let (queued, _) = svc.enqueue_message_with_id(
        &agent,
        Some(job.wake_id.clone()),
        job.body.clone(),
        None,
        None,
        Some(job.metadata),
        None,
        false,
        intent_core::MessageOrigin::Automatic,
    );
    assert!(svc
        .store
        .admit_script_notification(&intent_store::AgentQueueRow {
            id: queued.id.clone(),
            agent_id: agent.clone(),
            position: 0,
            payload: serde_json::to_value(&queued).unwrap(),
            created_at: queued.queued_at.clone(),
            turn_id: queued.turn_id.clone(),
        })
        .await
        .unwrap());
    assert_eq!(
        svc.agent_remove_queued_message_op(agent.clone(), queued.id.clone())
            .await
            .unwrap()["success"],
        true
    );
    assert!(
        !svc.store
            .script_monitor_pending_for_agent(&agent)
            .await
            .unwrap(),
        "authorized removal must cancel durable delivery ownership"
    );
    assert!(!svc
        .store
        .script_monitor_wake_allowed(&monitor.monitor_id)
        .await
        .unwrap());
    assert!(
        svc.store.load_all_agent_queues().await.unwrap().is_empty(),
        "startup must not resurrect an explicitly removed notification"
    );
    svc.store.close().await;
    drop(svc);
    let restarted = Services::new(Store::open(&tmp.path).await.unwrap());
    assert_eq!(restarted.rehydrate_agent_queues().await.unwrap(), 0);
    assert!(restarted.find_queued_message(&agent, &queued.id).is_none());
}

#[intent_test_macros::daemon_test]
async fn completion_marker_retry_redelivers_deferred_completion_watch() {
    let (_tmp, svc, ws) = setup().await;
    let svc = FakeRuntime::install(svc);
    let child = create_agent(&svc, &ws, "Child").await;
    let parent = watch_child(&svc, &ws, &child).await;
    let monitor = pending_monitor(&svc, &ws, &child).await;
    let job = svc
        .store
        .script_notification_jobs()
        .await
        .unwrap()
        .remove(0);
    let durable = intent_store::AgentQueueRow {
        id: job.wake_id.clone(),
        agent_id: child.clone(),
        position: 0,
        payload: json!({"messageMetadata":job.metadata}),
        created_at: now_iso(),
        turn_id: job.wake_id.clone(),
    };
    svc.store.admit_script_notification(&durable).await.unwrap();
    svc.store
        .accept_script_notification(&child, &monitor.monitor_id)
        .await
        .unwrap();
    svc.store.replace_agent_queue(&child, &[]).await.unwrap();
    let event = completion_event(&ws, AGENT_IDLE, &child, json!({"agentId":child.0}));
    svc.handle_completion_event(&event).await;
    assert!(svc.has_interim_skipped_idle(&child));
    sqlx::query("CREATE TRIGGER fail_marker BEFORE UPDATE OF phase ON agent_queue_admission WHEN new.phase='completed' BEGIN SELECT RAISE(ABORT,'marker failure'); END").execute(svc.store.write_pool()).await.unwrap();
    svc.ports
        .runtime()
        .complete_script_monitor_delivery(&svc, &child, &monitor.monitor_id)
        .await;
    assert_eq!(parent_message_count(&svc, &parent).await, 0);
    assert!(FakeRuntime::of(&svc).completion_retry_pending(&child, &monitor.monitor_id));
    assert!(
        !svc.ports
            .runtime()
            .script_monitor_delivery_allowed(&svc, &child, Some(&job.metadata))
            .await
            .unwrap(),
        "finished work must not reenter while its marker retries"
    );
    sqlx::query("DROP TRIGGER fail_marker")
        .execute(svc.store.write_pool())
        .await
        .unwrap();
    FakeRuntime::of(&svc)
        .retry_script_monitor_completion(&svc, &child, &monitor.monitor_id)
        .await;
    wait_for_completion(&svc, &parent, &child).await;
    assert!(!FakeRuntime::of(&svc).completion_retry_pending(&child, &monitor.monitor_id));
    assert!(!svc
        .store
        .script_monitor_pending_for_agent(&child)
        .await
        .unwrap());
    FakeRuntime::of(&svc)
        .retry_script_monitor_completion(&svc, &child, &monitor.monitor_id)
        .await;
    assert_eq!(parent_message_count(&svc, &parent).await, 1);
    svc.shutdown_agent_deliveries().await;
}
