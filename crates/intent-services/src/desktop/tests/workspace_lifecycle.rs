//! Archiving or retiring a workspace ends its desktop control and pending
//! consent prompts. A later unarchive must never let an agent inherit control
//! it was granted before the workspace closed.
use super::*;

async fn activate(h: &Harness) -> String {
    h.remember().await;
    h.agent("startControl", json!({})).await.unwrap();
    let DesktopState::Active { session_id, .. } = h.services.desktop.state(&h.agent) else {
        panic!("remembered permission must activate control");
    };
    session_id
}

fn session_events(h: &Harness) -> crate::Subscription {
    h.services
        .event_bus
        .as_ref()
        .unwrap()
        .subscribe(SubscriptionFilter {
            workspace_id: Some(h.workspace.0.clone()),
            event_types: vec![DESKTOP_SESSION_CHANGED.into()],
            ..Default::default()
        })
}

async fn archive(h: &Harness) {
    intent_core::with_caller(
        h.owner.clone(),
        h.services.archive_workspace(h.workspace.clone(), None),
    )
    .await
    .unwrap();
}

async fn retire(h: &Harness) {
    intent_core::with_caller(
        h.owner.clone(),
        h.services.retire_workspace(h.workspace.clone(), None),
    )
    .await
    .unwrap();
}

fn end_calls(h: &Harness, session_id: &str) -> usize {
    h.executor
        .calls
        .lock()
        .unwrap()
        .iter()
        .filter(|c| c["operation"] == "endControl" && c["sessionId"] == session_id)
        .count()
}

/// The session ended with `agent_terminated`, published its ended event and
/// sent the native end call exactly once.
async fn assert_ended(h: &Harness, session_id: &str, events: &mut crate::Subscription) {
    assert_eq!(h.services.desktop.state(&h.agent), DesktopState::Inactive);
    assert_eq!(
        h.services
            .store
            .desktop_terminal(session_id)
            .await
            .unwrap()
            .unwrap()["reason"],
        "agent_terminated"
    );
    assert_eq!(end_calls(h, session_id), 1);
    let deadline = intentd_test_support::test_timeout(Duration::from_secs(10));
    tokio::time::timeout(deadline, async {
        loop {
            let batch = events.recv().await.expect("session events");
            if batch.iter().any(|e| {
                e.data["sessionId"] == session_id
                    && e.data["status"] == "ended"
                    && e.data["reason"] == "agent_terminated"
            }) {
                return;
            }
        }
    })
    .await
    .expect("session-ended event");
    assert_eq!(
        h.agent("listDisplay", json!({})).await.unwrap_err().code,
        "desktop-not-active"
    );
}

#[tokio::test]
async fn archive_ends_active_desktop_control_and_unarchive_does_not_restore_it() {
    let h = Harness::new().await;
    let mut events = session_events(&h);
    let session_id = activate(&h).await;
    archive(&h).await;
    assert_ended(&h, &session_id, &mut events).await;
    intent_core::with_caller(
        h.owner.clone(),
        h.services.unarchive_workspace(h.workspace.clone()),
    )
    .await
    .unwrap();
    assert_eq!(h.services.desktop.state(&h.agent), DesktopState::Inactive);
    assert_eq!(
        h.agent("listDisplay", json!({})).await.unwrap_err().code,
        "desktop-not-active"
    );
    assert_eq!(end_calls(&h, &session_id), 1);
}

#[tokio::test]
async fn retire_ends_active_desktop_control() {
    let h = Harness::with_worktree().await;
    let mut events = session_events(&h);
    let session_id = activate(&h).await;
    retire(&h).await;
    assert_ended(&h, &session_id, &mut events).await;
}

#[tokio::test]
async fn archive_and_retire_invalidate_pending_permission_prompts() {
    for retiring in [false, true] {
        let h = if retiring {
            Harness::with_worktree().await
        } else {
            Harness::new().await
        };
        let pending = h.agent("startControl", json!({})).await.unwrap();
        assert_eq!(pending["status"], "pending_permission");
        if retiring {
            retire(&h).await;
        } else {
            archive(&h).await;
        }
        assert_eq!(
            h.services.desktop.state(&h.agent),
            DesktopState::Inactive,
            "retiring={retiring}"
        );
        let message = super::lifecycle_diagnostics::outcome(
            &h,
            "requestId",
            pending["requestId"].as_str().unwrap(),
        )
        .await;
        assert_eq!(message["metadata"]["outcome"], "invalidated");
        assert_eq!(message["metadata"]["reason"], "agent_terminated");
        assert_eq!(
            h.client(
                "respondPermission",
                json!({"requestId":pending["requestId"],"decision":"allow_once"})
            )
            .await
            .unwrap_err()
            .code,
            "desktop-stale-request",
            "retiring={retiring}"
        );
    }
}

#[tokio::test]
async fn retiring_an_archived_workspace_is_safe_and_idempotent() {
    let h = Harness::with_worktree().await;
    let mut events = session_events(&h);
    let session_id = activate(&h).await;
    archive(&h).await;
    retire(&h).await;
    retire(&h).await;
    assert_ended(&h, &session_id, &mut events).await;
}

/// Defense in depth: a workspace closed without the service sweep (for
/// example by a racing writer) still loses desktop control on the next
/// watchdog tick.
#[tokio::test]
async fn watchdog_ends_control_once_the_workspace_is_archived_or_retired() {
    for retired in [false, true] {
        let h = Harness::new().await;
        let mut events = session_events(&h);
        let session_id = activate(&h).await;
        let now = intent_core::now_iso();
        sqlx::query("UPDATE workspace SET archived=1, archived_at=? WHERE id=?")
            .bind(&now)
            .bind(h.workspace.as_str())
            .execute(h.services.store.write_pool())
            .await
            .unwrap();
        if retired {
            assert!(h
                .services
                .store
                .retire_workspace_if_archived(&h.workspace, &now)
                .await
                .unwrap());
        }
        // One 5s watchdog tick, scaled for loaded hosts.
        let deadline = tokio::time::Instant::now()
            + intentd_test_support::test_timeout(Duration::from_secs(7));
        // `desktop_end` drops the live session (state goes Inactive) before
        // it records the end and sends the native `endControl`, so wait for
        // the teardown call too or a loaded host races `assert_ended`.
        while h.services.desktop.state(&h.agent) != DesktopState::Inactive
            || end_calls(&h, &session_id) == 0
        {
            assert!(
                tokio::time::Instant::now() < deadline,
                "watchdog left control active (retired={retired})"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert_ended(&h, &session_id, &mut events).await;
    }
}
