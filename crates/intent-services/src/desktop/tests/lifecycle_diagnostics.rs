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
async fn desktop_lifecycle_explicit_release_ignores_late_environment_report() {
    for during_teardown in [false, true] {
        let h = Harness::new().await;
        h.remember().await;
        let active = h.agent("startControl", json!({})).await.unwrap();
        let session = active["sessionId"].as_str().unwrap();
        *h.executor.result.lock().unwrap() = Some(json!({
            "capturedAt":"2026-10-02T09:00:00Z", "layoutId":"layout",
            "displays":[{"displayId":"screen","width":1920,"height":1080,
                "originX":0,"originY":0,"scaleFactor":1.0,"assetId":"asset",
                "url":format!("workspace-asset://{}/asset",h.workspace),"mimeType":"image/png"}]
        }));
        h.agent("screenshot", json!({})).await.unwrap();
        h.executor
            .hold_end
            .store(during_teardown, std::sync::atomic::Ordering::Relaxed);
        let release = h.agent("endControl", json!({}));
        let late_report = async {
            if during_teardown {
                h.executor.end_seen.notified().await;
            }
            let result = h
                .client(
                    "revoke",
                    json!({"sessionId":session,"reason":"unsupported_environment"}),
                )
                .await
                .unwrap();
            h.executor.release_end.notify_one();
            assert_eq!(result, json!({"revoked":false,"reported":false}));
        };
        let ended = if during_teardown {
            tokio::time::timeout(Duration::from_secs(10), async {
                let (ended, ()) = tokio::join!(release, late_report);
                ended
            })
            .await
            .unwrap()
            .unwrap()
        } else {
            let ended = release.await.unwrap();
            late_report.await;
            ended
        };
        assert_eq!(ended, json!({"ended":true,"withdrawn":false}));
        assert_eq!(
            h.services
                .store
                .desktop_terminal(session)
                .await
                .unwrap()
                .unwrap()["reason"],
            "agent_end"
        );
        let notifications: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM settings WHERE key GLOB 'desktop.v1/outbox/*' AND json_extract(value,'$.payload.sessionId')=?")
            .bind(session).fetch_one(h.services.store.read_pool()).await.unwrap();
        assert_eq!(
            notifications, 0,
            "a completed explicit release must not emit a late failure wake"
        );
        assert_eq!(
            h.agent("endControl", json!({})).await.unwrap(),
            json!({"ended":false,"withdrawn":false})
        );
        assert_eq!(
            h.agent("screenshot", json!({})).await.unwrap_err().code,
            "desktop-not-active"
        );
    }
}

#[tokio::test]
async fn desktop_lifecycle_environment_report_before_release_preserves_real_failure() {
    let h = Harness::new().await;
    h.remember().await;
    let active = h.agent("startControl", json!({})).await.unwrap();
    let session = active["sessionId"].as_str().unwrap();
    assert_eq!(
        h.client(
            "revoke",
            json!({"sessionId":session,"reason":"unsupported_environment"})
        )
        .await
        .unwrap(),
        json!({"revoked":true,"reported":false})
    );
    assert_eq!(
        h.agent("endControl", json!({})).await.unwrap(),
        json!({"ended":false,"withdrawn":false})
    );
    assert_eq!(
        h.services
            .store
            .desktop_terminal(session)
            .await
            .unwrap()
            .unwrap()["reason"],
        "unsupported_environment"
    );
    let message = outcome(&h, "sessionId", session).await;
    assert_eq!(message["metadata"]["reason"], "unsupported_environment");
    assert!(visible_text(&message).contains("Do not automatically restart"));
    assert_eq!(
        h.agent("screenshot", json!({})).await.unwrap_err().code,
        "desktop-not-active"
    );
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
    h.executor.connection.lock().unwrap().connection_epoch = "new-executor-incarnation".into();
    assert_eq!(
        intent_core::with_caller(Caller::Daemon, h.services.desktop_current_state(&h.agent)).await,
        DesktopState::Inactive
    );
    // A concurrent watcher may remove authority before committing the outcome.
    // The correlated resolution event is emitted after that durable commit.
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let batch = events.recv().await.unwrap();
            if batch.iter().any(|event| {
                event.data["requestId"] == pending["requestId"]
                    && event.data["outcome"] == "invalidated"
            }) {
                break;
            }
        }
    })
    .await
    .expect("pending invalidation must durably resolve this request");
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

#[test]
fn desktop_lifecycle_reason_projection_is_safe_and_idempotent() {
    let mut payload = json!({"message":"Desktop control ended."});
    annotate_wake_reason(&mut payload, "private-token-value");
    assert_eq!(payload, json!({"message":"Desktop control ended."}));
    annotate_wake_reason(&mut payload, "executor_failed");
    let once = payload.clone();
    annotate_wake_reason(&mut payload, "executor_failed");
    assert_eq!(payload, once);
    let mut stopped = json!({"message":STOP_HINT});
    annotate_wake_reason(&mut stopped, "user_stop");
    assert_eq!(stopped["message"], STOP_HINT);
    assert_eq!(stopped["reason"], "user_stop");
}

#[tokio::test]
async fn desktop_lifecycle_invalid_native_result_reports_outcome_unknown_reason() {
    for method in ["listDisplay", "screenshot"] {
        let h = Harness::new().await;
        h.remember().await;
        let active = h.agent("startControl", json!({})).await.unwrap();
        let mut events = h
            .services
            .event_bus
            .as_ref()
            .unwrap()
            .subscribe(SubscriptionFilter {
                workspace_id: Some(h.workspace.0.clone()),
                event_types: vec![DESKTOP_SESSION_CHANGED.into()],
                ..Default::default()
            });
        *h.executor.result.lock().unwrap() = Some(json!({"unexpected":"result shape"}));
        let failure = h.agent(method, json!({})).await.unwrap_err();
        assert_eq!(failure.code, "desktop-execution-failed");
        assert_eq!(failure.detail, "Invalid native desktop result");
        assert_eq!(failure.execution.as_deref(), Some("unknown"));
        assert_eq!(h.services.desktop.state(&h.agent), DesktopState::Inactive);
        let ended = tokio::time::timeout(Duration::from_secs(10), events.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(ended[0].data["sessionId"], active["sessionId"]);
        assert_eq!(ended[0].data["reason"], "outcome_unknown");
        let message = outcome(&h, "sessionId", active["sessionId"].as_str().unwrap()).await;
        assert_eq!(message["metadata"]["outcome"], "revoked");
        assert!(!visible_text(&message).contains("rescinded by the user"));
        assert_eq!(
            h.agent(method, json!({})).await.unwrap_err().code,
            "desktop-not-active"
        );
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
        assert_eq!(message["metadata"]["reason"], "outcome_unknown");
        assert!(visible_text(&message).contains("outcome_unknown"));
    }
}

#[test]
fn desktop_screenshot_save_asset_response_requires_public_field_projection() {
    let workspace = WorkspaceId::from("workspace");
    let asset = intent_core::SaveAssetResult {
        asset_id: "asset".into(),
        path: "/private/workspace/assets/asset.png".into(),
        url: "workspace-asset://workspace/asset".into(),
    };
    let asset_wire = serde_json::to_value(&asset).unwrap();
    assert_eq!(asset_wire.as_object().unwrap().len(), 3);
    let geometry = json!({"displayId":"screen","width":1920,"height":1080,"originX":-1920,"originY":0,"scaleFactor":2.0});
    // listDisplay never receives an asset object; the helper's six geometry
    // fields remain valid independently of the screenshot projection defect.
    validate_result(
        &json!({"layoutId":"layout","displays":[geometry.clone()]}),
        &json!({"kind":"listDisplay"}),
        &workspace,
    )
    .unwrap();
    let mut leaked = geometry.clone();
    leaked
        .as_object_mut()
        .unwrap()
        .extend(asset_wire.as_object().unwrap().clone());
    leaked["mimeType"] = "image/png".into();
    assert_eq!(leaked.as_object().unwrap().len(), 10);
    let mut screenshot =
        json!({"capturedAt":"2026-10-05T14:00:00Z","layoutId":"layout","displays":[leaked]});
    let action = json!({"kind":"screenshot","displayId":"screen","layoutId":"layout"});
    let error = validate_result(&screenshot, &action, &workspace).unwrap_err();
    assert_eq!(error.code, "desktop-execution-failed");
    assert_eq!(error.detail, "Invalid native desktop result");
    assert_eq!(error.execution.as_deref(), Some("unknown"));
    assert!(!error.to_string().contains(&asset.path));
    let mut public = geometry;
    public["assetId"] = asset.asset_id.into();
    public["url"] = asset.url.into();
    public["mimeType"] = "image/png".into();
    screenshot["displays"] = json!([public]);
    assert_eq!(screenshot["displays"][0].as_object().unwrap().len(), 9);
    validate_result(&screenshot, &action, &workspace).unwrap();
}
