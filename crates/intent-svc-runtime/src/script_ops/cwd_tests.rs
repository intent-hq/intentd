// Legacy rows bypass creation so launch-time containment remains covered.
async fn seed_legacy_script(h: &Harness, params: ScriptCreateParams) -> String {
    let id = uuid::Uuid::new_v4().to_string();
    let def = Script {
        id: id.clone(),
        workspace_id: h.ws.to_string(),
        name: params.name,
        command: params.command,
        mode: params.mode,
        cwd: params.cwd,
        env: params.env,
        category: params.category,
        auto_start: params.auto_start,
        purpose: params.purpose.unwrap_or(match params.mode {
            ScriptMode::Command => intent_core::ScriptPurpose::OneOff,
            ScriptMode::Service => intent_core::ScriptPurpose::Saved,
        }),
        source: "user".into(),
        created_at: now_iso(),
        updated_at: None,
        archived_at: None,
        last_run: None,
    };
    h.services.store().upsert_script(&def).await.unwrap();
    id
}

async fn hydrate_legacy_script(h: &Harness, params: ScriptCreateParams) -> String {
    let id = seed_legacy_script(h, params).await;
    assert_eq!(h.services.hydrate_scripts().await.unwrap(), 1);
    id
}

fn invalid_script_cwds(h: &Harness) -> Vec<String> {
    vec![
        h.worktree
            .as_ref()
            .unwrap()
            .0
            .to_string_lossy()
            .into_owned(),
        h.worktree
            .as_ref()
            .unwrap()
            .0
            .parent()
            .unwrap()
            .join("outside-workspace")
            .to_string_lossy()
            .into_owned(),
        "..".into(),
        "../escape".into(),
        "child/../sibling".into(),
        "child/..".into(),
    ]
}

fn assert_invalid_script_cwd(result: Result<Value>, cwd: &str) {
    let err = result.expect_err("invalid cwd must be rejected at creation");
    let Error::InvalidParams(message) = err else {
        panic!("expected InvalidParams for {cwd:?}, got {err:?}");
    };
    for guidance in ["workspace-relative", "absolute paths", "..", "omit", "'.'"] {
        assert!(
            message.contains(guidance),
            "missing {guidance:?}: {message}"
        );
    }
}

#[intent_test_macros::daemon_test]
async fn script_create_rejects_invalid_cwd_before_persistence() {
    let h = harness_with_worktree(true).await;
    for scoped in [false, true] {
        for cwd in invalid_script_cwds(&h) {
            let result = h
                .services
                .script_manager()
                .create_with_scope(
                    h.ws.clone(),
                    ScriptCreateParams {
                        name: "invalid cwd".into(),
                        command: "echo never".into(),
                        mode: ScriptMode::Command,
                        script_id: Some("invalid-cwd".into()),
                        cwd: Some(cwd.clone()),
                        ..Default::default()
                    },
                    scoped,
                )
                .await;
            assert_invalid_script_cwd(result, &cwd);
            assert!(h
                .services
                .store()
                .get_script_in_workspace(&h.ws, "invalid-cwd")
                .await
                .unwrap()
                .is_none());
            assert!(matches!(
                h.services
                    .script_status(h.ws.clone(), "invalid-cwd".into())
                    .await,
                Err(Error::NotFound(_))
            ));
        }
    }
}

async fn assert_invalid_cwd_preserves_predecessor(running: bool) {
    let h = harness_with_worktree(true).await;
    let mut sub = subscribe(&h);
    let id = create_simple(&h, "original", SERVICE_CMD, ScriptMode::Service).await;
    if running {
        h.services
            .script_start(h.ws.clone(), id.clone())
            .await
            .unwrap();
        await_state(&mut sub, LIVENESS, |v| v["data"]["status"] == "running").await;
    }
    let before = h.services.script_list(h.ws.clone()).await.unwrap();
    let stored = serde_json::to_value(
        h.services
            .store()
            .get_script_in_workspace(&h.ws, &id)
            .await
            .unwrap(),
    )
    .unwrap();
    let events = h
        .services
        .store()
        .query_events(&EventQuery {
            workspace_id: Some(h.ws.clone()),
            event_types: vec![SCRIPT_CHANGED.to_string()],
            ..Default::default()
        })
        .await
        .unwrap();
    for scoped in [false, true] {
        for cwd in invalid_script_cwds(&h) {
            let result = h
                .services
                .script_manager()
                .create_with_scope(
                    h.ws.clone(),
                    ScriptCreateParams {
                        name: "replacement".into(),
                        command: "echo replaced".into(),
                        mode: ScriptMode::Command,
                        script_id: Some(id.clone()),
                        cwd: Some(cwd.clone()),
                        ..Default::default()
                    },
                    scoped,
                )
                .await;
            assert_invalid_script_cwd(result, &cwd);
            assert_eq!(
                h.services.script_list(h.ws.clone()).await.unwrap(),
                before,
                "runtime and definition unchanged for {cwd:?}"
            );
            assert_eq!(
                serde_json::to_value(
                    h.services
                        .store()
                        .get_script_in_workspace(&h.ws, &id)
                        .await
                        .unwrap()
                )
                .unwrap(),
                stored,
                "durable definition unchanged for {cwd:?}"
            );
            if running {
                assert!(
                    !pid_gone(
                        u32::try_from(before["scripts"][0]["runtime"]["pid"].as_u64().unwrap())
                            .unwrap()
                    ),
                    "predecessor remains alive"
                );
            }
        }
    }
    let after_events = h
        .services
        .store()
        .query_events(&EventQuery {
            workspace_id: Some(h.ws.clone()),
            event_types: vec![SCRIPT_CHANGED.to_string()],
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(after_events.len(), events.len(), "no replacement event");
    if running {
        h.services.script_stop(h.ws.clone(), id).await.unwrap();
    }
}

#[intent_test_macros::daemon_test]
async fn script_create_invalid_cwd_preserves_idle_predecessor() {
    assert_invalid_cwd_preserves_predecessor(false).await;
}

#[intent_test_macros::daemon_test]
async fn script_create_invalid_cwd_preserves_running_predecessor() {
    assert_invalid_cwd_preserves_predecessor(true).await;
}

#[intent_test_macros::daemon_test]
async fn script_create_preserves_valid_cwd_semantics() {
    let h = harness_with_worktree(true).await;
    let root = &h.worktree.as_ref().unwrap().0;
    for scoped in [false, true] {
        for cwd in [
            None,
            Some(""),
            Some("."),
            Some("child"),
            Some("./child/nested"),
        ] {
            let value = h
                .services
                .script_manager()
                .create_with_scope(
                    h.ws.clone(),
                    ScriptCreateParams {
                        name: "valid cwd".into(),
                        command: "echo ok".into(),
                        mode: ScriptMode::Command,
                        script_id: Some("valid-cwd".into()),
                        cwd: cwd.map(str::to_string),
                        ..Default::default()
                    },
                    scoped,
                )
                .await
                .unwrap();
            assert_eq!(value.get("cwd").and_then(Value::as_str), cwd);
            let def = h
                .services
                .store()
                .get_script_in_workspace(&h.ws, "valid-cwd")
                .await
                .unwrap()
                .unwrap();
            assert_eq!(def.cwd.as_deref(), cwd);
            assert_eq!(
                h.services
                    .script_manager()
                    .resolve_cwd(&h.ws, &def)
                    .await
                    .unwrap(),
                Some(root.join(cwd.unwrap_or("")))
            );
        }
    }
}
