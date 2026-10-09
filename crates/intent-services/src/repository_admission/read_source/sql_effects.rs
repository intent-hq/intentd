//! Hold the real single writer after MCP admission; retirement changes no row.
use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Operation {
    Insert,
    Deferred,
    Refresh,
    Adopt,
    Cancel,
}

async fn queued_write(operation: Operation, retirement: Option<bool>) {
    let provider = ReadServer::new().await;
    let f = ActualRead::new(&provider).await;
    routing_options(&f);
    let services = &f.auth.service;
    let mut previous_owner = None;
    if matches!(
        operation,
        Operation::Refresh | Operation::Adopt | Operation::Cancel
    ) {
        let endpoint = if operation == Operation::Adopt {
            let (id, physical, endpoint) = routing_actor(&f, None).await;
            previous_owner = Some((id, physical));
            endpoint
        } else {
            f.server()
        };
        let first = routing_json(&run(&endpoint, "return await ws.pr.monitor(4);").await);
        assert_eq!(first["ok"], true);
        let id = intent_core::PrMonitorId::from(first["monitor"]["monitorId"].as_str().unwrap());
        // Nonempty pending content must survive a refused refresh/adoption/cancel.
        sqlx::query("UPDATE pr_monitor SET pending_changes = ?, pending_since = ?, last_change_at = ? WHERE monitor_id = ?")
            .bind("[\"checks\"]").bind("2026-10-09T18:00:00Z").bind("2026-10-09T18:00:00Z")
            .bind(&id.0).execute(services.store.write_pool()).await.unwrap();
    }
    if let Some((id, _)) = &previous_owner {
        services
            .store
            .set_agent_session_status(
                &f.git.workspace.id,
                id,
                intent_core::AgentStatus::Error,
                false,
                "2026-10-09T18:01:00Z",
                None,
            )
            .await
            .unwrap();
    }
    if operation == Operation::Deferred {
        provider.status(MR, 429);
    }
    let rows = || async {
        sqlx::query_as::<_, (String, String, String, Option<String>, Option<String>, Option<String>, String)>(
            "SELECT monitor_id, agent_id, state, last_snapshot, baseline_snapshot, pending_changes, updated_at FROM pr_monitor ORDER BY monitor_id")
            .fetch_all(services.store.read_pool()).await.unwrap()
    };
    let before = rows().await;
    let ready = Arc::new(Notify::new());
    *services.test_review_monitor_write_ready.lock().unwrap() = Some(ready.clone());
    assert_eq!(
        services.store.write_pool().options().get_max_connections(),
        1
    );
    let writer = services.store.write_pool().acquire().await.unwrap();
    let endpoint = f.server();
    let task = tokio::spawn(async move {
        run(
            &endpoint,
            if operation == Operation::Cancel {
                "return await ws.mr.unmonitor(4);"
            } else {
                "return await ws.mr.monitor(4);"
            },
        )
        .await
    });
    timeout(BUDGET, ready.notified())
        .await
        .expect("operation reached admitted SQL write");
    assert!(
        !task.is_finished(),
        "actual writer must keep the operation pending"
    );
    assert_eq!(
        rows().await,
        before,
        "no row change before releasing writer"
    );
    if let Some(account) = retirement {
        if account {
            services.repository_connection_directory().retire().unwrap();
        } else {
            f.owner.retirement().retire();
        }
    }
    drop(writer);
    let reply = timeout(BUDGET, task).await.unwrap().unwrap();
    let after = rows().await;
    if retirement.is_some() {
        assert_eq!(
            after, before,
            "revoked {operation:?} committed a queued SQL effect: {reply}"
        );
        assert_eq!(reply["result"]["isError"], true, "{reply}");
        assert!(!reply.to_string().contains("actual review"));
    } else {
        let output = routing_json(&reply);
        assert_eq!(output["ok"], true, "{output}");
        assert_ne!(after, before, "live {operation:?} did not commit");
    }
    let events = services
        .store
        .query_events(&intent_store::EventQuery {
            workspace_id: Some(f.git.workspace.id.clone()),
            event_types: vec![
                intent_core::events::PR_MONITOR_REGISTERED.into(),
                intent_core::events::PR_MONITOR_CHANGED.into(),
                intent_core::events::PR_MONITOR_EMITTED.into(),
                intent_core::events::PR_MONITOR_COMPLETED.into(),
                intent_core::events::PR_MONITOR_CANCELLED.into(),
            ],
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(
        events.is_empty(),
        "direct writes must not emit monitor wakes"
    );
}

#[intent_test_macros::daemon_test]
async fn review_monitor_sql_insert_revocation() {
    queued_write(Operation::Insert, Some(false)).await;
}
#[intent_test_macros::daemon_test]
async fn review_monitor_sql_deferred_revocation() {
    queued_write(Operation::Deferred, Some(false)).await;
}
#[intent_test_macros::daemon_test]
async fn review_monitor_sql_refresh_revocation() {
    queued_write(Operation::Refresh, Some(false)).await;
}
#[intent_test_macros::daemon_test]
async fn review_monitor_sql_adoption_revocation() {
    queued_write(Operation::Adopt, Some(false)).await;
}
#[intent_test_macros::daemon_test]
async fn review_monitor_sql_cancel_revocation() {
    queued_write(Operation::Cancel, Some(false)).await;
}
#[intent_test_macros::daemon_test]
async fn review_monitor_sql_live_authority_commits() {
    for operation in [
        Operation::Insert,
        Operation::Deferred,
        Operation::Refresh,
        Operation::Adopt,
        Operation::Cancel,
    ] {
        queued_write(operation, None).await;
    }
}

#[intent_test_macros::daemon_test]
async fn review_monitor_sql_insert_account_revocation() {
    queued_write(Operation::Insert, Some(true)).await;
}

#[intent_test_macros::daemon_test]
async fn review_monitor_sql_deferred_account_revocation() {
    queued_write(Operation::Deferred, Some(true)).await;
}

#[intent_test_macros::daemon_test]
async fn review_monitor_sql_refresh_account_revocation() {
    queued_write(Operation::Refresh, Some(true)).await;
}

#[intent_test_macros::daemon_test]
async fn review_monitor_sql_adoption_account_revocation() {
    queued_write(Operation::Adopt, Some(true)).await;
}

#[intent_test_macros::daemon_test]
async fn review_monitor_sql_cancel_account_revocation() {
    queued_write(Operation::Cancel, Some(true)).await;
}

/// Actual `SQLite` COMMIT is held, not merely the service future or provider.
async fn commit_boundary(drop_caller: bool, abort_commit: bool) {
    let provider = ReadServer::new().await;
    let f = ActualRead::new(&provider).await;
    routing_options(&f);
    let entered = Arc::new(Notify::new());
    let signal = entered.clone();
    let (release, wait) = std::sync::mpsc::channel();
    let mut wait = Some(wait);
    let pool = f.auth.service.store.write_pool();
    let mut writer = pool.acquire().await.unwrap();
    writer
        .lock_handle()
        .await
        .unwrap()
        .set_commit_hook(move || {
            if let Some(wait) = wait.take() {
                signal.notify_one();
                wait.recv_timeout(BUDGET).unwrap();
            }
            !abort_commit
        });
    drop(writer);
    let endpoint = f.server();
    let task = tokio::spawn(async move { run(&endpoint, "return await ws.mr.monitor(4);").await });
    timeout(BUDGET, entered.notified())
        .await
        .expect("SQL reached actual commit");
    if drop_caller {
        task.abort();
    }
    let retirement = f.owner.retirement();
    let probe = retirement.clone();
    let store = f.auth.service.store.clone();
    let agent = f.agent.clone();
    let runtime = tokio::runtime::Handle::current();
    let retired = tokio::task::spawn_blocking(move || {
        retirement.retire();
        // A lifecycle writer can finish after joining the original SQL effect;
        // no writer/leaf lock inversion may strand either operation.
        runtime.block_on(async {
            let mut connection = store.write_pool().acquire().await.unwrap();
            connection.lock_handle().await.unwrap().remove_commit_hook();
            sqlx::query("UPDATE agent_session SET name = 'retired-sql-owner' WHERE id = ?")
                .bind(&agent.0)
                .execute(&mut *connection)
                .await
                .unwrap();
        });
    });
    timeout(BUDGET, async {
        while !probe.detached_for_test() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(
        !retired.is_finished(),
        "physical retirement must join admitted SQL completion"
    );
    // Read pool still sees the old committed state while SQL holds its write.
    assert!(f
        .auth
        .service
        .store
        .list_pr_monitors_by_agent(&f.agent)
        .await
        .unwrap()
        .is_empty());
    release.send(()).unwrap();
    timeout(BUDGET, retired).await.unwrap().unwrap();
    let reply = timeout(BUDGET, task).await.unwrap();
    if drop_caller {
        assert!(reply.unwrap_err().is_cancelled());
    } else {
        assert_eq!(reply.unwrap()["result"]["isError"], true);
    }
    // Acquisition joins the original SQL worker even after its caller vanished.
    let mut writer = timeout(BUDGET, pool.acquire()).await.unwrap().unwrap();
    writer.lock_handle().await.unwrap().remove_commit_hook();
    drop(writer);
    let rows = f
        .auth
        .service
        .store
        .list_pr_monitors_by_agent(&f.agent)
        .await
        .unwrap();
    assert_eq!(
        rows.len(),
        usize::from(!abort_commit),
        "committed effects survive later refusal; failed commit rolls back"
    );
}

#[intent_test_macros::daemon_test]
async fn review_monitor_sql_retirement_joins_commit() {
    commit_boundary(false, false).await;
}
#[intent_test_macros::daemon_test]
async fn review_monitor_sql_dropped_caller_does_not_cancel_admitted_commit() {
    commit_boundary(true, false).await;
}
#[intent_test_macros::daemon_test]
async fn review_monitor_sql_failed_commit_rolls_back_before_retirement() {
    commit_boundary(false, true).await;
}
#[intent_test_macros::daemon_test]
async fn review_monitor_sql_dropped_caller_failed_commit_rolls_back() {
    commit_boundary(true, true).await;
}

#[intent_test_macros::daemon_test]
async fn review_monitor_sql_dropped_waiter_before_writer_acquisition_has_no_effect() {
    let provider = ReadServer::new().await;
    let f = ActualRead::new(&provider).await;
    routing_options(&f);
    let ready = Arc::new(Notify::new());
    *f.auth
        .service
        .test_review_monitor_write_ready
        .lock()
        .unwrap() = Some(ready.clone());
    let pool = f.auth.service.store.write_pool();
    let writer = pool.acquire().await.unwrap();
    let endpoint = f.server();
    let task = tokio::spawn(async move { run(&endpoint, "return await ws.mr.monitor(4);").await });
    timeout(BUDGET, ready.notified()).await.unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    f.owner.retirement().retire();
    drop(writer);
    drop(timeout(BUDGET, pool.acquire()).await.unwrap().unwrap());
    assert!(f
        .auth
        .service
        .store
        .list_pr_monitors_by_agent(&f.agent)
        .await
        .unwrap()
        .is_empty());
}
