//! Hook wake and notice goldens that need the runtime's hook service (moved
//! from `v1_goldens.rs`).

use intent_core::{now_iso, AgentId};
use serde_json::json;

use super::test_fixtures::wake_texts_when;
use crate::hook_manager::HookServicesExt;
use crate::test_support::goldens::{pr_monitor_row, seed_agent, setup};

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

#[test]
fn golden_hook_wake_logs_section() {
    assert_eq!(crate::hook_manager::with_wake_logs("msg", None), "msg");
    assert_eq!(crate::hook_manager::with_wake_logs("msg", Some("")), "msg");
    assert_eq!(
        crate::hook_manager::with_wake_logs("msg", Some("line1\nline2")),
        "msg\n\n[hook logs]\nline1\nline2"
    );
    // Over-cap logs are head-truncated with the marker line: exactly the
    // LAST 2048 chars survive, nothing more. Positionally distinct bytes
    // pin which end is retained and the exact cap.
    let logs: String = (0..3000)
        .map(|i| char::from(b'a' + u8::try_from(i % 26).expect("< 26")))
        .collect();
    assert_eq!(
        crate::hook_manager::with_wake_logs("msg", Some(&logs)),
        format!(
            "msg\n\n[hook logs]\n[earlier log lines truncated]\n{}",
            &logs[3000 - 2048..]
        )
    );
}

/// End-to-end hook wake bytes: schedule a one-shot hook whose validation run
/// dispatches, and pin the exact framed message (framing + dispatch body +
/// retired-state note) delivered to the owner.
#[tokio::test]
async fn golden_hook_dispatch_wake_bytes() {
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
                "name": "ci-watch",
                "code": "return { dispatch: true, message: 'CI is green' };",
                "delayMs": 10_000,
            }),
        )
        .await
        .expect("schedule");
    assert_eq!(out["dispatched"], json!(true));
    let hook_id = out["hook"]["hookId"].as_str().expect("hookId").to_string();
    let text = wake_texts_when(&svc, &owner, 1).await;
    assert_eq!(
        text,
        vec![format!(
            "[Background hook \"ci-watch\"] CI is green\n\
             \n\
             [This hook is now retired and will not run again — recover its script via \
             ws.hook.get(\"{hook_id}\") and reschedule via ws.hook.schedule if still needed.]"
        )]
    );
}

/// Hook eviction notice, internal-error variant: exact bytes when the
/// scheduler evicts after a store error (`evict_hook_after_store_error`).
#[tokio::test]
async fn golden_hook_eviction_internal_error_notice_bytes() {
    let (_t, svc, ws) = setup().await;
    let bus = crate::EventBus::new(svc.store().clone());
    let svc = svc.with_event_bus(bus);
    let owner = AgentId::from("agent-evict-2");
    seed_agent(&svc, &ws, &owner).await;
    let mut hook = intent_core::Hook {
        hook_id: intent_core::HookId::from("hook-evict-2"),
        workspace_id: ws.clone(),
        agent_id: owner.clone(),
        name: "store-victim".to_string(),
        code: "return { dispatch: false };".to_string(),
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
    let cause = intent_core::Error::Internal("db locked".to_string());
    svc.evict_hook_after_store_error(&mut hook, &cause).await;
    let texts = wake_texts_when(&svc, &owner, 1).await;
    assert_eq!(
        texts,
        vec![
            "[Background hook \"store-victim\"] Your background hook \"store-victim\" was \
             evicted after an internal error: scheduler stopped after a store error: \
             internal error: db locked\n\
             \n\
             [This hook will not run again. Recover its script via \
             ws.hook.get(\"hook-evict-2\") and schedule a new hook via ws.hook.schedule \
             if the condition is still worth watching.]"
                .to_string()
        ]
    );
}
