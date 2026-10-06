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

// Catch invalid admission at the first classification park, before it can
// recurse. Dropping the pinned future cancels that pass at the assertion.
async fn assert_deferred_before_delivery(
    svc: &Services,
    child: &AgentId,
    park: &crate::CompletionClassifyPark,
) {
    timeout(Duration::from_secs(2), async {
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
    sqlx::query("ALTER TABLE script_monitor RENAME COLUMN wake_state TO unavailable_wake_state")
        .execute(svc.store.write_pool())
        .await
        .unwrap();
}

async fn restore_pending_query(svc: &Services) {
    sqlx::query("ALTER TABLE script_monitor RENAME COLUMN unavailable_wake_state TO wake_state")
        .execute(svc.store.write_pool())
        .await
        .unwrap();
}

async fn wait_for_completion(svc: &Services, parent: &AgentId, child: &AgentId) {
    timeout(Duration::from_secs(10), async {
        loop {
            if parent_message_count(svc, parent).await == 1
                && svc.find_watches_for_child(child).is_empty()
                && svc.pending_completion_retries.lock().unwrap().is_empty()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
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
    timeout(Duration::from_secs(2), svc.handle_completion_event(&event))
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
    timeout(Duration::from_secs(5), async {
        loop {
            if svc.pending_completion_retries.lock().unwrap()[&child] > generation {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
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
    timeout(Duration::from_secs(2), svc.handle_completion_event(&event))
        .await
        .unwrap();
    assert!(svc.has_interim_skipped_idle(&child));
    assert_eq!(parent_message_count(&svc, &parent).await, 0);
    // Same sequential dispatch shape used by notification maintenance: the
    // first owner's remaining pending row must not block unrelated owners.
    timeout(Duration::from_secs(2), async {
        svc.dispatch_script_monitor(&first).await;
        svc.dispatch_script_monitor(&unrelated).await;
    })
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
    svc.dispatch_script_monitor(&second).await;
    wait_for_completion(&svc, &parent, &child).await;
    svc.dispatch_script_monitor(&first).await;
    svc.dispatch_script_monitor(&second).await;
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
    let child = create_agent(&svc, &ws, "Child").await;
    let parent = watch_child(&svc, &ws, &child).await;
    let row = pending_monitor(&svc, &ws, &child).await;
    let park = Arc::new(crate::CompletionClassifyPark::default());
    let parked = svc.clone().with_completion_classify_park(park.clone());
    let event = completion_event(&ws, AGENT_IDLE, &child, json!({"agentId":child.0}));
    let delivery = tokio::spawn(async move { parked.handle_completion_event(&event).await });
    timeout(Duration::from_secs(2), park.entered.notified())
        .await
        .unwrap();
    assert!(!svc.has_interim_skipped_idle(&child));
    svc.dispatch_script_monitor(&row).await;
    assert_eq!(parent_message_count(&svc, &parent).await, 0);
    park.release.notify_one();
    timeout(Duration::from_secs(2), delivery)
        .await
        .unwrap()
        .unwrap();
    // The deferred retry observes settlement after the marker exists.
    timeout(Duration::from_secs(5), park.entered.notified())
        .await
        .unwrap();
    park.release.notify_one();
    wait_for_completion(&svc, &parent, &child).await;
    svc.shutdown_agent_deliveries().await;
}

#[intent_test_macros::daemon_test]
async fn pending_synthetic_reclassification_keeps_stale_report_provenance() {
    let (_tmp, svc, ws) = setup().await;
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
    svc.dispatch_script_monitor(&row).await;
    wait_for_completion(&svc, &parent, &child).await;
    assert!(!parent_messages_text(&svc, &parent)
        .await
        .contains("old report"));
    svc.shutdown_agent_deliveries().await;
}

#[intent_test_macros::daemon_test]
async fn pending_advisory_provenance_clears_when_notification_settles() {
    let (_tmp, svc, ws) = setup().await;
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
    timeout(Duration::from_secs(2), async {
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
