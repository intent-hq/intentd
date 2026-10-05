//! Adaptive checks through the production TLS, bearer and Origin-checked transport.
//! Persisted wall clocks control monitor due selection; Tokio's test clock controls
//! automatic admission/cache expiry. No production clock or security bypass.
use super::*;

const TIERS: [(i64, u64); 5] = [
    (0, 60),
    (900, 120),
    (3600, 300),
    (21_600, 600),
    (86_400, 900),
];

fn ago(seconds: i64) -> String {
    (chrono::Utc::now() - chrono::Duration::seconds(seconds)).to_rfc3339()
}

async fn idle_for(fx: &Fixture, seconds: i64) {
    sqlx::query("UPDATE workspace SET created_at = ?, last_content_activity = ? WHERE id = ?")
        .bind(ago(seconds))
        .bind(ago(seconds))
        .bind(fx.ws_id.as_str())
        .execute(fx.services.store().write_pool())
        .await
        .unwrap();
}

async fn polled_ago(fx: &Fixture, id: &intent_core::PrMonitorId, seconds: i64) {
    let row = fx.services.store().get_pr_monitor(id).await.unwrap();
    assert!(fx
        .services
        .store()
        .update_pr_monitor_poll(
            id,
            PrMonitorPollUpdate {
                last_snapshot: row.last_snapshot.as_deref(),
                baseline_snapshot: row.baseline_snapshot.as_deref(),
                pending_changes: &row.pending_changes,
                pending_since: row.pending_since.as_deref(),
                last_change_at: row.last_change_at.as_deref(),
                last_polled_at: Some(&ago(seconds)),
                last_error: row.last_error.as_deref(),
                updated_at: &now_iso(),
                expected_updated_at: &row.updated_at,
            },
        )
        .await
        .unwrap());
}

async fn call(fx: &Fixture, method: &str, mut params: Value) -> Value {
    // Reconnects also prove admission belongs to the workspace, not the socket.
    let mut rpc = ancestry::browser_connect(fx, "http://localhost", TOKEN)
        .await
        .unwrap();
    params["workspaceId"] = json!(fx.ws_id);
    let response = wss_call(&mut rpc, 7, method, params).await;
    assert_eq!(response["jsonrpc"], "2.0");
    assert_eq!(response["id"], 7);
    assert!(response.get("error").is_none(), "{method}: {response}");
    rpc.close(None).await.unwrap();
    response["result"].clone()
}

async fn create_note(fx: &Fixture) {
    let result = call(
        fx,
        "note.create",
        json!({"title":"Resume work", "content":"Review the changed checks."}),
    )
    .await;
    assert!(result["note"]["id"].is_string(), "{result}");
}

#[intent_test_macros::daemon_test]
async fn adaptive_monitor_tiers_and_content_activity_over_wss() {
    let fx = boot().await;
    let mut sub = ancestry::browser_connect(&fx, "http://localhost", TOKEN)
        .await
        .unwrap();
    let response = wss_call(
        &mut sub,
        1,
        "events.subscribe",
        json!({"workspaceId":fx.ws_id,"eventTypes":["prMonitor:registered","prMonitor:changed"]}),
    )
    .await;
    assert_eq!(response["jsonrpc"], "2.0");
    assert_eq!(response["id"], 1);
    assert!(response["result"]["subscriptionId"].is_string());
    idle_for(&fx, 86_400).await;
    let (monitor, baseline) = fx
        .services
        .pr_monitor_register(&fx.ws_id, &fx.agent_id, "o", "r", 42)
        .await
        .unwrap();
    assert!(
        baseline.is_some(),
        "idle registration still fetches immediately"
    );
    assert_eq!(fx.forge.fetches(), 1);
    let registered = next_event(&mut sub, "prMonitor:registered").await;
    assert_eq!(registered["data"]["monitorId"], monitor.monitor_id.as_str());

    for (index, (idle, interval)) in TIERS.into_iter().enumerate() {
        idle_for(&fx, idle).await;
        fx.forge.edit(|s| s.conversation_comments = index + 1);
        let before = fx.forge.fetches();
        // Leave a wide margin for wall-clock I/O; exact boundary arithmetic
        // is covered by the service's fixed-now tests.
        polled_ago(
            &fx,
            &monitor.monitor_id,
            i64::try_from(interval / 2).unwrap(),
        )
        .await;
        let listed = call(&fx, "prMonitor.list", json!({})).await;
        fx.services.poll_due_pr_monitors().await;
        let skipped = call(&fx, "prMonitor.list", json!({})).await;
        assert_eq!(fx.forge.fetches(), before, "idle {idle}: not yet due");
        assert_eq!(skipped["monitors"], listed["monitors"]);

        polled_ago(
            &fx,
            &monitor.monitor_id,
            i64::try_from(interval).unwrap() + 1,
        )
        .await;
        fx.services.poll_due_pr_monitors().await;
        assert_eq!(fx.forge.fetches(), before + 1, "idle {idle}: due");
        let changed = next_event(&mut sub, "prMonitor:changed").await;
        assert_eq!(changed["data"]["monitorId"], monitor.monitor_id.as_str());
        let listed = call(&fx, "prMonitor.list", json!({})).await;
        let count = index + 1;
        let suffix = if count == 1 { "" } else { "s" };
        let changes = json!([format!(
            "+{count} conversation comment{suffix} ({count} total)"
        )]);
        assert_eq!(changed["data"]["changes"], changes);
        assert_eq!(listed["monitors"][0]["pendingChanges"], changes);
        assert!(listed["monitors"][0]["hasPendingChanges"]
            .as_bool()
            .unwrap());
        assert_eq!(owner_messages(&fx).await, "[]", "debounce still holds");
    }

    polled_ago(&fx, &monitor.monitor_id, 61).await;
    let before = fx.forge.fetches();
    fx.services.poll_due_pr_monitors().await;
    assert_eq!(
        fx.forge.fetches(),
        before,
        "polling did not reset idle activity"
    );
    create_note(&fx).await;
    fx.services.poll_due_pr_monitors().await;
    assert_eq!(
        fx.forge.fetches(),
        before + 1,
        "wire note restores active cadence"
    );
}

/// Paused Tokio time must not auto-jump to transport/database deadlines while
/// real socket or `SQLite` I/O is pending. Keep one runnable task until test drop;
/// only explicit `advance()` changes the clock, including on assertion failure.
struct ControlledClock(tokio::task::JoinHandle<()>);

impl ControlledClock {
    fn start() -> Self {
        tokio::time::pause();
        Self(tokio::spawn(async {
            loop {
                tokio::task::yield_now().await;
            }
        }))
    }
}

impl Drop for ControlledClock {
    fn drop(&mut self) {
        self.0.abort();
        tokio::time::resume();
    }
}

#[intent_test_macros::daemon_test]
async fn adaptive_automatic_refresh_tiers_and_explicit_bypass_over_wss() {
    let _clock = ControlledClock::start();
    for (idle, interval) in TIERS {
        let fx = boot().await;
        let mut ws = fx.services.store().get_workspace(&fx.ws_id).await.unwrap();
        ws.pr_number = Some(42);
        ws.pr_url = Some("https://github.com/o/r/pull/42".into());
        ws.pr_status = Some(intent_core::PullRequestStatus::Open);
        fx.services.store().update_workspace(&ws).await.unwrap();
        idle_for(&fx, idle).await;

        let first = call(&fx, "pr.refresh", json!({"automatic":true})).await;
        assert_eq!(first["prNumber"], 42, "{first}");
        assert_ne!(first["outcome"], "skipped");
        assert_eq!(fx.forge.fetches(), 1);
        tokio::time::advance(Duration::from_secs(interval - 1)).await;
        for _ in 0..2 {
            let skipped = call(&fx, "pr.refresh", json!({"automatic":true})).await;
            assert_eq!(skipped["outcome"], "skipped", "idle {idle}: {skipped}");
            assert_eq!(skipped["prNumber"], 42);
            assert_eq!(skipped["pullRequests"], first["pullRequests"]);
            assert_eq!(fx.forge.fetches(), 1, "reconnect cannot bypass admission");
        }
        tokio::time::advance(Duration::from_secs(1)).await;
        let due = call(&fx, "pr.refresh", json!({"automatic":true})).await;
        assert_ne!(due["outcome"], "skipped", "idle {idle}: {due}");
        assert_eq!(fx.forge.fetches(), 2, "exactly due after {interval}s");

        // Both wire forms retain explicit refresh behavior inside the window.
        for (params, expected) in [(json!({"automatic":false}), 3), (json!({}), 4)] {
            let explicit = call(&fx, "pr.refresh", params).await;
            assert_ne!(explicit["outcome"], "skipped");
            assert_eq!(explicit["prNumber"], 42);
            assert_eq!(fx.forge.fetches(), expected);
        }
        let suppressed = call(&fx, "pr.refresh", json!({"automatic":true})).await;
        assert_eq!(suppressed["outcome"], "skipped");
        assert_eq!(fx.forge.fetches(), 4);

        if idle == 86_400 {
            tokio::time::advance(Duration::from_secs(60)).await;
            let suppressed = call(&fx, "pr.refresh", json!({"automatic":true})).await;
            assert_eq!(suppressed["outcome"], "skipped");
            create_note(&fx).await;
            let resumed = call(&fx, "pr.refresh", json!({"automatic":true})).await;
            assert_ne!(resumed["outcome"], "skipped");
            assert_eq!(fx.forge.fetches(), 5, "activity restores automatic cadence");
        }
    }
}
