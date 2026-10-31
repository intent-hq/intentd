//! A busy owner never reads a stale PR monitor update (intentd#2875): a
//! newer wake for the same monitor removes the older one while it is still
//! waiting, undelivered, in the owner's queue, and joins the back of the
//! queue reporting every change since the state the owner last read — not
//! only its own delta, because the emit baseline already moved when the
//! older wake was queued. Different monitors keep separate entries, an entry already
//! popped for delivery is never touched, and the final merged/closed wake
//! always arrives.

use super::*;
use crate::agent_manager::{AgentManager, BusEventSink};

/// Attach a manager and hold the owner's turn slot, so every wake queues
/// behind the "in-flight turn" exactly as it does behind a real busy agent.
async fn busy_owner(svc: &Services, ws: &WorkspaceId, owner: &AgentId) -> Arc<AgentManager> {
    let manager = Arc::new(AgentManager::new(
        svc.clone(),
        Arc::new(BusEventSink::new(svc.event_bus.clone().unwrap())),
        1,
    ));
    svc.attach_agent_manager(&manager);
    let admission = manager.try_begin_turn(owner, ws).await;
    assert!(admission.is_some(), "claim the owner's turn slot");
    assert!(manager.is_busy(owner));
    manager
}

/// The owner's queued (and draining) PR monitor wakes, in delivery order.
fn monitor_wakes(svc: &Services, owner: &AgentId) -> Vec<Value> {
    svc.queue_snapshot(owner)
        .into_iter()
        .filter(|e| e["messageMetadata"]["type"] == json!("pr_monitor_wake"))
        .collect()
}

fn content(entry: &Value) -> &str {
    entry["content"].as_str().unwrap()
}

async fn flush(svc: &Services, ws: &WorkspaceId, monitor: &PrMonitor) {
    assert!(
        svc.pr_monitor_check_and_flush(ws, &monitor.monitor_id)
            .await
            .unwrap(),
        "a change wake was emitted"
    );
}

#[intent_test_macros::daemon_test]
async fn two_updates_for_one_monitor_while_busy_deliver_one_about_the_latest_commit() {
    let (_db, _root, svc, forge, ws, owner) = setup().await;
    let _manager = busy_owner(&svc, &ws, &owner).await;
    let monitor = register(&svc, &ws, &owner).await;

    forge.edit(|s| s.conversation_comments = 2);
    flush(&svc, &ws, &monitor).await;
    let first = monitor_wakes(&svc, &owner);
    assert_eq!(first.len(), 1, "{first:?}");
    assert!(content(&first[0]).contains("head: aaaaaaaa"));
    // Unrelated work queued after the older update.
    let (ordinary, _) = svc.enqueue_message(
        &owner,
        "ordinary follow-up".into(),
        None,
        None,
        None,
        None,
        false,
        intent_core::MessageOrigin::Automatic,
    );

    forge.edit(|s| s.head_sha = "bbbbbbbb".into());
    flush(&svc, &ws, &monitor).await;
    forge.edit(|s| s.head_sha = "cccccccc".into());
    flush(&svc, &ws, &monitor).await;

    let wakes = monitor_wakes(&svc, &owner);
    assert_eq!(wakes.len(), 1, "one update per monitor: {wakes:?}");
    let wake = &wakes[0];
    assert_ne!(wake["id"], first[0]["id"], "the older entry was removed");
    assert_eq!(wake["messageMetadata"]["reason"], json!("changed"));
    assert_eq!(
        wake["messageMetadata"]["monitorId"],
        json!(monitor.monitor_id)
    );
    let text = content(wake);
    assert!(text.contains("head: cccccccc"), "{text}");
    assert!(
        text.contains("new commits pushed (head is now cccccccc)"),
        "{text}"
    );
    assert!(
        text.contains("+2 conversation comments (2 total)"),
        "the change only the older update carried is kept: {text}"
    );
    assert!(!text.contains("aaaaaaaa"), "{text}");
    assert!(!text.contains("bbbbbbbb"), "{text}");

    // The replacement joins the back of the queue.
    let queue = svc.queue_snapshot(&owner);
    assert_eq!(queue.len(), 2, "{queue:?}");
    assert_eq!(queue[0]["id"], json!(ordinary.id));
    assert_eq!(queue[1]["id"], wake["id"]);

    // The drain delivers the ordinary entry, then the one update, once.
    let text = text.to_string();
    assert_eq!(
        svc.dequeue_message(&owner).expect("ordinary").id,
        ordinary.id
    );
    let delivered = svc.dequeue_message(&owner).expect("wake");
    assert_eq!(delivered.content, text);
    assert!(monitor_wakes(&svc, &owner).is_empty());

    // The durable queue no longer carries the removed update.
    svc.persist_queue_snapshot(&owner).await;
    let rows = svc.store().load_all_agent_queues().await.unwrap();
    assert!(rows
        .iter()
        .all(|r| !r.payload.to_string().contains("bbbbbbbb")));
}

#[intent_test_macros::daemon_test]
async fn updates_for_different_monitors_stay_separate() {
    let (_db, _root, svc, forge, ws, owner) = setup().await;
    let _manager = busy_owner(&svc, &ws, &owner).await;
    let a = register(&svc, &ws, &owner).await;
    let b = svc
        .pr_monitor_register(&ws, &owner, "o", "r", 43)
        .await
        .expect("register 43")
        .0;
    for head in ["bbbbbbbb", "cccccccc"] {
        forge.edit(|s| s.head_sha = head.into());
        flush(&svc, &ws, &a).await;
        flush(&svc, &ws, &b).await;
    }
    let wakes = monitor_wakes(&svc, &owner);
    assert_eq!(wakes.len(), 2, "one entry per monitor: {wakes:?}");
    let ids: BTreeSet<_> = wakes
        .iter()
        .map(|w| w["messageMetadata"]["monitorId"].clone().to_string())
        .collect();
    assert_eq!(ids.len(), 2, "{wakes:?}");
    for wake in &wakes {
        assert!(content(wake).contains("head: cccccccc"), "{wake}");
    }
    assert!(content(&wakes[0]).contains("o/r#42"));
    assert!(content(&wakes[1]).contains("o/r#43"));
}

#[intent_test_macros::daemon_test]
async fn the_final_update_replaces_a_queued_change_and_keeps_its_changes() {
    let (_db, _root, svc, forge, ws, owner) = setup().await;
    let _manager = busy_owner(&svc, &ws, &owner).await;
    let monitor = register(&svc, &ws, &owner).await;
    forge.edit(|s| s.conversation_comments = 2);
    flush(&svc, &ws, &monitor).await;
    let first = monitor_wakes(&svc, &owner);
    let (ordinary, _) = svc.enqueue_message(
        &owner,
        "ordinary follow-up".into(),
        None,
        None,
        None,
        None,
        false,
        intent_core::MessageOrigin::Automatic,
    );

    forge.edit(|s| s.pr_state = PrState::Merged);
    svc.pr_monitor_check_and_flush(&ws, &monitor.monitor_id)
        .await
        .unwrap();

    let wakes = monitor_wakes(&svc, &owner);
    assert_eq!(wakes.len(), 1, "{wakes:?}");
    assert_eq!(
        wakes[0]["id"], first[0]["id"],
        "takes the older update's place"
    );
    assert_eq!(wakes[0]["messageMetadata"]["reason"], json!("completed"));
    let queue = svc.queue_snapshot(&owner);
    assert_eq!(queue.len(), 2, "{queue:?}");
    assert_eq!(queue[0]["id"], first[0]["id"]);
    assert_eq!(queue[1]["id"], json!(ordinary.id));
    let text = content(&wakes[0]);
    assert!(text.contains("was MERGED"), "{text}");
    assert!(
        text.contains("+2 conversation comments (2 total)"),
        "{text}"
    );
    assert_eq!(
        svc.store()
            .get_pr_monitor(&monitor.monitor_id)
            .await
            .unwrap()
            .state,
        PrMonitorState::Completed
    );
}

#[intent_test_macros::daemon_test]
async fn an_update_already_popped_for_delivery_is_never_rewritten() {
    let (_db, _root, svc, forge, ws, owner) = setup().await;
    let _manager = busy_owner(&svc, &ws, &owner).await;
    let monitor = register(&svc, &ws, &owner).await;
    forge.edit(|s| s.conversation_comments = 2);
    flush(&svc, &ws, &monitor).await;
    let (popped, draining) = svc.dequeue_message_draining(&owner).expect("pop");

    forge.edit(|s| s.head_sha = "bbbbbbbb".into());
    flush(&svc, &ws, &monitor).await;
    let wakes = monitor_wakes(&svc, &owner);
    assert_eq!(
        wakes.len(),
        2,
        "the popped update plus a new one: {wakes:?}"
    );
    assert_eq!(wakes[0]["id"], json!(popped.id));
    assert_eq!(content(&wakes[0]), popped.content, "untouched");
    let second = content(&wakes[1]);
    assert!(second.contains("head is now bbbbbbbb"), "{second}");
    assert!(
        !second.contains("conversation comment"),
        "the popped update already reports it: {second}"
    );

    // The final wake still arrives, replacing the queued change, never the
    // popped one.
    forge.edit(|s| s.pr_state = PrState::Merged);
    svc.pr_monitor_check_and_flush(&ws, &monitor.monitor_id)
        .await
        .unwrap();
    let wakes = monitor_wakes(&svc, &owner);
    assert_eq!(wakes.len(), 2, "{wakes:?}");
    assert_eq!(content(&wakes[0]), popped.content);
    assert_eq!(wakes[1]["messageMetadata"]["reason"], json!("completed"));
    assert!(content(&wakes[1]).contains("head is now bbbbbbbb"));
    drop(draining);
}

#[intent_test_macros::daemon_test]
async fn re_registering_keeps_the_queued_update_and_reports_only_what_moved_since() {
    let (_db, _root, svc, forge, ws, owner) = setup().await;
    let _manager = busy_owner(&svc, &ws, &owner).await;
    let monitor = register(&svc, &ws, &owner).await;
    forge.edit(|s| s.conversation_comments = 2);
    flush(&svc, &ws, &monitor).await;
    // The owner re-registers: its fresh checklist is its latest read.
    let rearmed = svc
        .pr_monitor_start_op(&ws, &owner, 42, None)
        .await
        .unwrap();
    assert_eq!(rearmed["ok"], json!(true), "{rearmed}");

    forge.edit(|s| s.head_sha = "bbbbbbbb".into());
    flush(&svc, &ws, &monitor).await;
    let wakes = monitor_wakes(&svc, &owner);
    assert_eq!(wakes.len(), 2, "{wakes:?}");
    assert!(content(&wakes[0]).contains("+2 conversation comments"));
    assert!(!content(&wakes[1]).contains("conversation comment"));
}

#[intent_test_macros::daemon_test]
async fn a_change_that_fully_reverted_keeps_the_queued_update_and_its_correction() {
    let (_db, _root, svc, forge, ws, owner) = setup().await;
    let _manager = busy_owner(&svc, &ws, &owner).await;
    let monitor = register(&svc, &ws, &owner).await;
    forge.edit(|s| s.head_sha = "bbbbbbbb".into());
    flush(&svc, &ws, &monitor).await;
    forge.edit(|s| s.head_sha = "aaaaaaaa".into());
    flush(&svc, &ws, &monitor).await;
    // Net nothing moved since the owner's last read; an empty rewrite would
    // say nothing, so both stay and the second corrects the first.
    let wakes = monitor_wakes(&svc, &owner);
    assert_eq!(wakes.len(), 2, "{wakes:?}");
    assert!(content(&wakes[1]).contains("head is now aaaaaaaa"));
}

/// The queue primitive itself: only a pristine, undelivered entry whose
/// metadata the caller confirms is removed, and nothing else moves.
#[intent_test_macros::daemon_test]
async fn remove_pending_wake_touches_only_an_undelivered_matching_entry() {
    let (_db, _root, svc, _forge, _ws, owner) = setup().await;
    let wake = json!({"type": "pr_monitor_wake", "monitorId": "m", "reason": "changed"});
    let queue = |metadata: Option<Value>, origin| {
        svc.enqueue_message(
            &owner,
            "old".into(),
            None,
            None,
            metadata,
            None,
            false,
            origin,
        )
        .0
    };
    let is_wake = |m: &Value| m["monitorId"] == json!("m");
    let entry = queue(Some(wake.clone()), intent_core::MessageOrigin::Automatic);
    let user = queue(Some(wake.clone()), intent_core::MessageOrigin::User);
    let other = queue(None, intent_core::MessageOrigin::Automatic);

    assert!(!svc.remove_pending_wake(&owner, &entry.id, |_| false));
    assert!(!svc.remove_pending_wake(&owner, "missing", is_wake));
    assert!(!svc.remove_pending_wake(&owner, &user.id, is_wake));
    assert!(!svc.remove_pending_wake(&owner, &other.id, is_wake));
    assert!(svc.remove_pending_wake(&owner, &entry.id, is_wake));
    let ids: Vec<_> = svc
        .queue_snapshot(&owner)
        .into_iter()
        .map(|row| row["id"].clone())
        .collect();
    assert_eq!(ids, vec![json!(user.id), json!(other.id)]);

    // Popped for delivery: never removed.
    let again = queue(Some(wake), intent_core::MessageOrigin::Automatic);
    while let Some((popped, draining)) = svc.dequeue_message_draining(&owner) {
        if popped.id == again.id {
            assert!(!svc.remove_pending_wake(&owner, &again.id, is_wake));
            drop(draining);
            break;
        }
    }
}

/// The in-place rewrite used by the final wake: same guards as removal,
/// same id and position, new content and metadata, re-dated.
#[intent_test_macros::daemon_test]
async fn rewrite_pending_wake_keeps_the_entry_place() {
    let (_db, _root, svc, _forge, _ws, owner) = setup().await;
    let wake = json!({"type": "pr_monitor_wake", "monitorId": "m", "reason": "changed"});
    let newer = json!({"type": "pr_monitor_wake", "monitorId": "m", "reason": "completed"});
    let is_wake = |m: &Value| m["monitorId"] == json!("m");
    let queue = |metadata: Option<Value>, origin| {
        svc.enqueue_message(
            &owner,
            "old".into(),
            None,
            None,
            metadata,
            None,
            false,
            origin,
        )
        .0
    };
    let entry = queue(Some(wake.clone()), intent_core::MessageOrigin::Automatic);
    let user = queue(Some(wake), intent_core::MessageOrigin::User);
    assert!(!svc.rewrite_pending_wake(&owner, &user.id, "new", &newer, is_wake));
    assert!(!svc.rewrite_pending_wake(&owner, &entry.id, "new", &newer, |_| false));
    std::thread::sleep(std::time::Duration::from_millis(5));
    assert!(svc.rewrite_pending_wake(&owner, &entry.id, "new", &newer, is_wake));
    let rows = svc.queue_snapshot(&owner);
    assert_eq!(rows[0]["id"], json!(entry.id), "same id and position");
    assert_eq!(rows[0]["content"], json!("new"));
    assert_eq!(rows[0]["messageMetadata"]["reason"], json!("completed"));
    assert_ne!(rows[0]["queuedAt"], json!(entry.queued_at), "re-dated");
    assert_eq!(rows[1]["id"], json!(user.id));
}
