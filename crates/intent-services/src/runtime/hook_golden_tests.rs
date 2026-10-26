/// FE-cancel notices for hooks and PR monitors: exact wake bytes delivered
/// to the owning agent. (The archive sweep's consolidated notice is a v2.7
/// surface, pinned in `v2_7_goldens.rs`.)
#[tokio::test]
async fn golden_hook_and_pr_monitor_cancel_notice_bytes() {
    let (_t, svc, ws) = setup().await;
    let bus = crate::EventBus::new(svc.store().clone());
    let svc = svc.with_event_bus(bus);
    let owner = AgentId::from("agent-cancel");
    seed_agent(&svc, &ws, &owner).await;
    // Hook FE-cancel (caller = None).
    let out = svc
        .hook_schedule_op(
            &ws,
            &owner,
            &json!({
                "name": "watcher",
                "code": "return { dispatch: false };",
                "delayMs": 10_000,
            }),
        )
        .await
        .expect("schedule");
    let hook_id = intent_core::HookId::from(out["hook"]["hookId"].as_str().expect("hookId"));
    svc.hook_cancel_op(&ws, &hook_id, None)
        .await
        .expect("cancel");
    let texts = wake_texts_when(&svc, &owner, 1).await;
    assert_eq!(
        texts,
        vec!["[Background hook \"watcher\"] This hook was cancelled from the app.".to_string()]
    );
    // PR monitor FE-cancel notice.
    let mut monitor = pr_monitor_row();
    monitor.workspace_id = ws.clone();
    monitor.agent_id = owner.clone();
    assert!(svc
        .store()
        .insert_pr_monitor(&monitor)
        .await
        .expect("insert"));
    svc.pr_monitor_cancel(&ws, &monitor.monitor_id, None)
        .await
        .expect("cancel monitor");
    let texts = wake_texts_when(&svc, &owner, 2).await;
    assert_eq!(
        texts[1],
        "[PR monitor o/r#42] This monitor was cancelled from the app — it will not \
         report again."
    );
}

/// Hook eviction notice: exact bytes for a throwing run (failed-run wording
/// + terminal note).
#[tokio::test]
async fn golden_hook_eviction_notice_bytes() {
    let (_t, svc, ws) = setup().await;
    let bus = crate::EventBus::new(svc.store().clone());
    let svc = svc.with_event_bus(bus);
    let owner = AgentId::from("agent-evict");
    seed_agent(&svc, &ws, &owner).await;
    // Seed a scheduled row directly (a throwing script would fail the
    // schedule-time validation run) and drive one run via runNow.
    let hook = intent_core::Hook {
        hook_id: intent_core::HookId::from("hook-evict-1"),
        workspace_id: ws.clone(),
        agent_id: owner.clone(),
        name: "will-throw".to_string(),
        code: "throw new Error('kaput');".to_string(),
        delay_ms: 600_000,
        cron: None,
        run_at: None,
        state: intent_core::HookState::Scheduled,
        created_at: now_iso(),
        last_run_at: None,
        next_run_at: None,
        run_count: 0,
        last_error: None,
        last_logs: None,
        last_state: None,
        expires_at: None,
        perpetual: false,
        dispatch_count: 0,
    };
    svc.store().insert_hook(&hook).await.expect("insert");
    svc.rehydrate_hooks().await.expect("rehydrate");
    svc.hook_run_now_op(&ws, &hook.hook_id)
        .await
        .expect("runNow");
    let texts = wake_texts_when(&svc, &owner, 1).await;
    assert_eq!(
        texts,
        vec![
            "[Background hook \"will-throw\"] Your background hook \"will-throw\" was \
             evicted after a failed run: Error: kaput\n\
             \n\
             [This hook will not run again. Recover its script via \
             ws.hook.get(\"hook-evict-1\") and schedule a new hook via ws.hook.schedule \
             if the condition is still worth watching.]"
                .to_string()
        ]
    );
}

/// Hook expiry notices: exact bytes for the one-shot ("without a dispatch")
/// and perpetual ("N runs, N dispatches") tallies, including singular and
/// plural forms.
#[tokio::test]
async fn golden_hook_expiry_notice_bytes() {
    let (_t, svc, ws) = setup().await;
    let bus = crate::EventBus::new(svc.store().clone());
    let svc = svc.with_event_bus(bus);
    let owner = AgentId::from("agent-exp");
    seed_agent(&svc, &ws, &owner).await;
    let past = "2026-01-02T03:04:05Z".to_string();
    let hook =
        |id: &str, name: &str, perpetual: bool, runs: i64, dispatches: i64| intent_core::Hook {
            hook_id: intent_core::HookId::from(id),
            workspace_id: ws.clone(),
            agent_id: owner.clone(),
            name: name.to_string(),
            code: "return { dispatch: false };".to_string(),
            delay_ms: 600_000,
            cron: None,
            run_at: None,
            state: intent_core::HookState::Scheduled,
            created_at: past.clone(),
            last_run_at: None,
            next_run_at: None,
            run_count: runs,
            last_error: None,
            last_logs: None,
            last_state: None,
            expires_at: Some(past.clone()),
            perpetual,
            dispatch_count: dispatches,
        };
    // One-shot, plural runs; rehydration expires already-past hooks at boot.
    svc.store()
        .insert_hook(&hook("hook-exp-1", "one-shot", false, 2, 0))
        .await
        .expect("insert 1");
    // Perpetual, singular tallies.
    svc.store()
        .insert_hook(&hook("hook-exp-2", "perpetual", true, 1, 1))
        .await
        .expect("insert 2");
    svc.rehydrate_hooks().await.expect("rehydrate");
    let mut texts = wake_texts_when(&svc, &owner, 2).await;
    texts.sort();
    assert_eq!(
        texts,
        vec![
            "[Background hook \"one-shot\"] Your background hook \"one-shot\" expired after \
             reaching its TTL (2 runs completed without a dispatch). Schedule a new hook via \
             ws.hook.schedule if the condition is still worth watching — the original script \
             is retrievable via ws.hook.get(\"hook-exp-1\")."
                .to_string(),
            "[Background hook \"perpetual\"] Your background hook \"perpetual\" expired after \
             reaching its TTL (1 run, 1 dispatch). Schedule a new hook via ws.hook.schedule \
             if the condition is still worth watching — the original script is retrievable \
             via ws.hook.get(\"hook-exp-2\")."
                .to_string(),
        ]
    );
}

/// Perpetual dispatch wake: the active-state note names the TTL deadline.
#[tokio::test]
async fn golden_hook_perpetual_dispatch_wake_bytes() {
    let (_t, svc, ws) = setup().await;
    let bus = crate::EventBus::new(svc.store().clone());
    let svc = svc.with_event_bus(bus);
    let owner = AgentId::from("agent-hooks");
    seed_agent(&svc, &ws, &owner).await;
    let out = svc
        .hook_schedule_op(
            &ws,
            &owner,
            &json!({
                "name": "pr-watch",
                "code": "return { dispatch: true, message: 'PR moved' };",
                "delayMs": 10_000,
                "perpetual": true,
            }),
        )
        .await
        .expect("schedule");
    assert_eq!(out["dispatched"], json!(true));
    let expires_at = out["hook"]["expiresAt"]
        .as_str()
        .expect("expiresAt")
        .to_string();
    let text = wake_texts_when(&svc, &owner, 1).await;
    assert_eq!(
        text,
        vec![format!(
            "[Background hook \"pr-watch\"] PR moved\n\
             \n\
             [This hook remains active until {expires_at} — cancel via ws.hook.cancel \
             when no longer needed.]"
        )]
    );
}
