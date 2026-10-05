use super::*;

async fn outcome(h: &Harness, field: &str, id: &str) -> Value {
    h.services.desktop_flush_outbox().await;
    let conversation = intent_core::with_caller(
        h.owner.clone(),
        h.services.agent_get_conversation(
            h.agent.clone(),
            Some(30),
            Some(h.workspace.clone()),
            None,
            None,
            None,
            Some(intent_core::ConversationProjection::Slim),
            false,
        ),
    )
    .await
    .unwrap();
    conversation["messages"]
        .as_array()
        .unwrap()
        .iter()
        .rev()
        .find(|m| m["metadata"][field] == id)
        .unwrap()
        .clone()
}

fn visible_text(message: &Value) -> String {
    message["contentBlocks"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|b| b["text"].as_str())
        .collect::<Vec<_>>()
        .join("\n")
}

#[tokio::test]
async fn desktop_lifecycle_executor_revocation_exposes_reason_without_claiming_user_stop() {
    for reason in [
        "screen_locked",
        "os_permission_lost",
        "lease_expired",
        "executor_failed",
        "unsupported_environment",
    ] {
        let h = Harness::new().await;
        h.remember().await;
        let active = h.agent("startControl", json!({})).await.unwrap();
        let session = active["sessionId"].as_str().unwrap();
        h.client("revoke", json!({"sessionId":session,"reason":reason}))
            .await
            .unwrap();
        let message = outcome(&h, "sessionId", session).await;
        assert_eq!(message["metadata"]["outcome"], "revoked");
        assert_eq!(message["metadata"]["reason"], reason);
        let text = visible_text(&message);
        assert!(text.contains(reason), "missing reason: {text}");
        assert!(text.contains("Do not automatically restart"));
        assert!(!text.contains("rescinded by the user"));
        assert!(message["metadata"].get("reportId").is_none());
        assert!(!text.contains("stopReportToken"));
        assert_eq!(
            h.agent("listDisplay", json!({})).await.unwrap_err().code,
            "desktop-not-active"
        );
    }
}

#[tokio::test]
async fn desktop_lifecycle_pending_invalidation_reports_cause_and_request() {
    let h = Harness::new().await;
    let pending = h.agent("startControl", json!({})).await.unwrap();
    h.executor.connection.lock().unwrap().connection_epoch = "new-executor-incarnation".into();
    assert_eq!(
        h.services.desktop_current_state(&h.agent).await,
        DesktopState::Inactive
    );
    let message = outcome(&h, "requestId", pending["requestId"].as_str().unwrap()).await;
    assert_eq!(message["metadata"]["outcome"], "invalidated");
    assert_eq!(message["metadata"]["reason"], "primary_changed");
    let text = visible_text(&message);
    assert!(text.contains("primary_changed"), "{text}");
    assert!(!text.contains("rescinded by the user"));
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
}

#[tokio::test]
async fn desktop_lifecycle_terminal_failure_before_grant_delivery_keeps_actual_reason() {
    let h = Harness::new().await;
    let outbox_guard = h.services.desktop.outbox_gate.lock().await;
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
    h.client(
        "respondPermission",
        json!({"requestId":pending["requestId"],"decision":"allow_once"}),
    )
    .await
    .unwrap();
    let batch = tokio::time::timeout(Duration::from_secs(10), events.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(batch[0].data["outcome"], "granted");
    let active = h.services.desktop.state(&h.agent);
    let DesktopState::Active { session_id, .. } = active else {
        panic!("native readiness must have activated");
    };
    h.services.desktop_terminate_agent(&h.agent).await;
    assert_eq!(
        h.services
            .store
            .desktop_terminal(&session_id)
            .await
            .unwrap()
            .unwrap()["reason"],
        "agent_terminated"
    );
    drop(outbox_guard);
    let gate = h.services.desktop.gate(&h.agent);
    let guard = gate.lock().await;
    drop(guard);
    let message = outcome(&h, "requestId", pending["requestId"].as_str().unwrap()).await;
    assert_eq!(message["metadata"]["outcome"], "invalidated");
    assert_eq!(message["metadata"]["reason"], "agent_terminated");
    let text = visible_text(&message);
    assert!(text.contains("agent_terminated"), "{text}");
    assert!(!text.contains("rescinded by the user"));
    assert_eq!(
        h.agent("listDisplay", json!({})).await.unwrap_err().code,
        "desktop-not-active"
    );
}

#[tokio::test]
async fn desktop_lifecycle_startup_revocation_keeps_one_terminal_reason_without_grant() {
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
    // Real executor ordering: local cleanup/revoke completes before the start error reply.
    h.client(
        "revoke",
        json!({"sessionId":start["sessionId"],"reason":"executor_failed"}),
    )
    .await
    .unwrap();
    *h.executor.fail.lock().unwrap() = Some("startControl".into());
    *h.executor.failure.lock().unwrap() = Some(error(
        "desktop-execution-failed",
        "Local desktop readiness failed.",
    ));
    h.executor.release_start.notify_one();
    let gate = h.services.desktop.gate(&h.agent);
    let guard = gate.lock().await;
    drop(guard);
    assert_eq!(h.services.desktop.state(&h.agent), DesktopState::Inactive);
    let outcomes: Vec<String> = sqlx::query_scalar("SELECT json_extract(value,'$.payload') FROM settings WHERE key GLOB 'desktop.v1/outbox/*' AND json_extract(value,'$.payload.requestId')=?")
        .bind(pending["requestId"].as_str().unwrap()).fetch_all(h.services.store.read_pool()).await.unwrap();
    assert_eq!(
        outcomes.len(),
        1,
        "late readiness failure must not duplicate or replace terminal outcome"
    );
    let payload: Value = serde_json::from_str(&outcomes[0]).unwrap();
    assert_eq!(payload["outcome"], "revoked");
    assert!(!h
        .executor
        .calls
        .lock()
        .unwrap()
        .iter()
        .any(|p| p["operation"] == "prepareCommand" || p["operation"] == "execute"));
    assert_eq!(
        h.agent("listDisplay", json!({})).await.unwrap_err().code,
        "desktop-not-active"
    );
    let message = outcome(&h, "requestId", pending["requestId"].as_str().unwrap()).await;
    assert_eq!(message["metadata"]["reason"], "executor_failed");
    assert!(visible_text(&message).contains("executor_failed"));
    assert!(!visible_text(&message).contains("rescinded by the user"));
    assert!(message["metadata"].get("reportId").is_none());
}
