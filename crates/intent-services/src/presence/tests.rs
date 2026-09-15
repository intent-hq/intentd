use std::time::{Duration, Instant};

use serde_json::{json, Value};

use super::{parse_cursor, parse_update, CursorThrottle, Offer, CURSOR_MIN_INTERVAL};

fn cursor(head: u64) -> Value {
    json!({ "rev": 1, "anchor": head, "head": head })
}

/// Drive a burst of `offers` carets `step` apart through the throttle,
/// honouring every `Defer` as the async driver would (a flush at the
/// deferred instant, folded into the timeline). Returns the published
/// carets with their publish instants.
fn drive(offers: usize, step: Duration) -> Vec<(Instant, Value)> {
    let start = Instant::now();
    let mut throttle = CursorThrottle::default();
    let mut published = Vec::new();
    let mut flush_at: Option<Instant> = None;
    for i in 0..offers {
        let now = start + step * u32::try_from(i).expect("small");
        if let Some(at) = flush_at.filter(|at| *at <= now) {
            if let Some(c) = throttle.flush(at) {
                published.push((at, c));
            }
            flush_at = None;
        }
        match throttle.offer(cursor(i as u64), now) {
            Offer::Publish => published.push((now, cursor(i as u64))),
            Offer::Defer(delay) => flush_at = Some(now + delay),
            Offer::Absorbed => assert!(flush_at.is_some(), "absorbed without a pending flush"),
        }
    }
    if let Some(at) = flush_at {
        if let Some(c) = throttle.flush(at) {
            published.push((at, c));
        }
    }
    published
}

/// Definition of done: a 1 s burst at 100 carets/s yields ≤10 deliveries per
/// second, consecutive deliveries are never closer than the floor, and the
/// last delivery is the last offered position.
#[test]
fn coalescer_caps_at_ten_per_second_and_ends_on_last_position() {
    let published = drive(100, Duration::from_millis(10));
    let first = published.first().expect("something published").0;
    let last = published.last().expect("something published");
    let span = last.0.duration_since(first);
    let seconds = (span.as_secs_f64()).max(1.0);
    let count = u32::try_from(published.len()).expect("small count");
    assert!(
        f64::from(count) <= 10.0 * seconds + 1.0,
        "{} deliveries over {span:?}",
        published.len()
    );
    for pair in published.windows(2) {
        assert!(
            pair[1].0.duration_since(pair[0].0) >= CURSOR_MIN_INTERVAL,
            "deliveries closer than the floor: {:?} then {:?}",
            pair[0].0,
            pair[1].0
        );
    }
    assert_eq!(
        last.1,
        cursor(99),
        "the trailing flush carries the last caret"
    );
    assert!(
        published.len() >= 10,
        "burst was over-throttled: {} deliveries",
        published.len()
    );
}

/// A lone caret publishes immediately (leading edge) and a second one inside
/// the floor is deferred by exactly the remainder.
#[test]
fn coalescer_leading_edge_then_defers_remainder() {
    let start = Instant::now();
    let mut throttle = CursorThrottle::default();
    assert_eq!(throttle.offer(cursor(1), start), Offer::Publish);
    let later = start + Duration::from_millis(30);
    assert_eq!(
        throttle.offer(cursor(2), later),
        Offer::Defer(CURSOR_MIN_INTERVAL.saturating_sub(Duration::from_millis(30)))
    );
    assert_eq!(throttle.offer(cursor(3), later), Offer::Absorbed);
    let at = start + CURSOR_MIN_INTERVAL;
    assert_eq!(throttle.flush(at), Some(cursor(3)), "last writer wins");
    assert_eq!(throttle.flush(at), None, "flush is one-shot");
    assert_eq!(
        throttle.offer(cursor(4), at + CURSOR_MIN_INTERVAL),
        Offer::Publish,
        "the floor restarts from the flush"
    );
}

/// A store with one workspace and one collaborator member, plus the services
/// and the wire caller for that member.
async fn member_services(
    tmp: &crate::tests::TempDb,
    root: &crate::tests::WorkspacesRoot,
) -> (
    intent_store::Store,
    crate::Services,
    intent_core::WorkspaceId,
    intent_core::Caller,
) {
    use intent_core::{Caller, Principal, PrincipalId, WorkspaceId, WorkspaceRole};
    let store = intent_store::Store::open(&tmp.path)
        .await
        .expect("open store");
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&crate::tests::workspace(&ws))
        .await
        .expect("workspace");
    let principal = Principal {
        id: PrincipalId::new(),
        github_user_id: None,
        login: Some("collab".to_string()),
        display_name: None,
        avatar_url: None,
        is_primary: false,
        created_at: intent_core::now_iso(),
        updated_at: intent_core::now_iso(),
    };
    store.upsert_principal(&principal).await.expect("principal");
    store
        .add_workspace_member(&ws, &principal.id, WorkspaceRole::Collaborator)
        .await
        .expect("member");
    let services = crate::Services::new(store.clone())
        .with_workspaces_root(root.path().to_path_buf())
        .with_event_bus(crate::events::EventBus::new(store.clone()));
    let caller = Caller::Wire {
        principal_id: principal.id,
        is_administrator: false,
    };
    (store, services, ws, caller)
}

/// A viewer that left and rejoined inside a deferred flush's window gets a
/// fresh generation: the stale timer must not flush the replacement's
/// pending caret ahead of the replacement's own deadline.
#[tokio::test]
async fn stale_trailing_flush_never_touches_a_rejoined_viewer() {
    use super::{spawn_trailing_flush, Viewer};
    let tmp = crate::tests::TempDb::new();
    let root = crate::tests::WorkspacesRoot::new();
    let (_store, services, ws, _caller) = member_services(&tmp, &root).await;
    let principal = intent_core::PrincipalId::from("viewer");
    let key = (ws, intent_core::NoteId::from("spec"));

    let mut old = Viewer::default();
    let old_start = Instant::now()
        .checked_sub(Duration::from_millis(50))
        .expect("clock is past its first 50ms");
    assert_eq!(old.throttle.offer(cursor(1), old_start), Offer::Publish);
    let Offer::Defer(old_delay) = old.throttle.offer(cursor(2), Instant::now()) else {
        panic!("second caret inside the floor defers");
    };
    let old_generation = old.generation;
    services
        .presence
        .lock()
        .viewers
        .entry(key.clone())
        .or_default()
        .insert(principal.clone(), old);
    spawn_trailing_flush(
        services.clone(),
        Some(intent_core::Caller::Daemon),
        key.clone(),
        principal.clone(),
        old_generation,
        old_delay,
    );

    services.presence.lock().viewers.remove(&key);
    let mut replacement = Viewer::default();
    assert_ne!(replacement.generation, old_generation);
    assert_eq!(
        replacement.throttle.offer(cursor(3), Instant::now()),
        Offer::Publish
    );
    assert!(matches!(
        replacement.throttle.offer(cursor(4), Instant::now()),
        Offer::Defer(_)
    ));
    services
        .presence
        .lock()
        .viewers
        .entry(key.clone())
        .or_default()
        .insert(principal.clone(), replacement);

    tokio::time::sleep(old_delay + Duration::from_millis(30)).await;
    assert_eq!(
        services.presence.lock().viewers[&key][&principal]
            .throttle
            .pending,
        Some(cursor(4)),
        "the old generation's timer fired but left the replacement's pending caret alone"
    );
}

/// A lease outlives a membership removal, so the caret path is gated on
/// every call: a removed collaborator's next update is `NotFound` and a
/// caret already deferred when the membership ended is dropped, not
/// published after the gate closed.
#[tokio::test]
async fn caret_updates_are_member_gated_including_the_deferred_flush() {
    use crate::events::SubscriptionFilter;
    use intent_core::{with_caller, Error, NoteId};
    let tmp = crate::tests::TempDb::new();
    let root = crate::tests::WorkspacesRoot::new();
    let (store, services, ws, caller) = member_services(&tmp, &root).await;
    let principal = caller.principal_id().cloned().expect("wire caller");
    let note = NoteId::from("spec");
    let mut sub = services
        .event_bus
        .as_ref()
        .expect("bus")
        .subscribe(SubscriptionFilter {
            event_types: vec![intent_core::events::NOTE_PRESENCE.to_string()],
            workspace_id: Some(ws.to_string()),
            ..Default::default()
        });
    let next_kinds = |batch: Option<Vec<intent_core::Event>>| -> Vec<(String, Value)> {
        batch
            .expect("bus open")
            .into_iter()
            .map(|e| {
                (
                    e.data["kind"].as_str().unwrap().to_string(),
                    e.data["cursor"].clone(),
                )
            })
            .collect()
    };

    with_caller(caller.clone(), async {
        services
            .note_presence_join_op("conn-1".into(), "lease-1".into(), ws.clone(), note.clone())
            .await
            .expect("subscribe as a member");
        services
            .note_presence_update_op("conn-1", ws.clone(), note.clone(), &cursor(1))
            .await
            .expect("leading caret");
        services
            .note_presence_update_op("conn-1", ws.clone(), note.clone(), &cursor(2))
            .await
            .expect("deferred caret");
    })
    .await;
    assert_eq!(
        next_kinds(sub.recv().await),
        vec![("joined".to_string(), Value::Null)]
    );
    assert_eq!(
        next_kinds(sub.recv().await),
        vec![("updated".to_string(), cursor(1))]
    );
    assert_eq!(
        services.presence.lock().viewers[&(ws.clone(), note.clone())][&principal]
            .throttle
            .pending,
        Some(cursor(2)),
        "the second caret waits for the trailing flush"
    );

    store
        .remove_workspace_member(&ws, &principal)
        .await
        .expect("remove member");

    let refused = with_caller(
        caller.clone(),
        services.note_presence_update_op("conn-1", ws.clone(), note.clone(), &cursor(3)),
    )
    .await;
    assert!(
        matches!(refused, Err(Error::NotFound(_))),
        "a removed collaborator's caret is refused even with a live lease: {refused:?}"
    );

    tokio::time::sleep(CURSOR_MIN_INTERVAL + Duration::from_millis(50)).await;
    assert_eq!(
        services.presence.lock().viewers[&(ws.clone(), note.clone())][&principal]
            .throttle
            .pending,
        None,
        "the flush fired and dropped the pending caret"
    );
    let leaked = tokio::time::timeout(Duration::from_millis(50), sub.recv()).await;
    assert!(
        leaked.is_err(),
        "no caret leaves after the membership ended: {leaked:?}"
    );
}

/// The "Someone (you)" regression: a principal first seen with an empty
/// profile whose GitHub identity is persisted later (the lazy `principal.me`
/// refresh landing after the first `presence.update`) must be renamed in
/// place — the next `presence:changed` roster, a `note:presence` delta on
/// every viewed note and `presence.snapshot` all carry the login without the
/// principal leaving and rejoining. Persisting the same identity again emits
/// nothing. The row carries the account id already so the refresh passes the
/// same-account guard of a locked identity (the fixture has two principals).
#[tokio::test]
async fn profile_persisted_after_first_sight_refreshes_rosters_without_a_reconnect() {
    use crate::events::SubscriptionFilter;
    use intent_core::events::{NOTE_PRESENCE, PRESENCE_CHANGED};
    use intent_core::{with_caller, NoteId};
    let tmp = crate::tests::TempDb::new();
    let root = crate::tests::WorkspacesRoot::new();
    let (store, services, ws, caller) = member_services(&tmp, &root).await;
    let principal = caller.principal_id().cloned().expect("wire caller");
    let mut row = store.get_principal(&principal).await.expect("principal");
    row.github_user_id = Some(583_231);
    row.login = None;
    store.upsert_principal(&row).await.expect("empty profile");
    let note = NoteId::from("spec");
    let mut sub = services
        .event_bus
        .as_ref()
        .expect("bus")
        .subscribe(SubscriptionFilter {
            event_types: vec![PRESENCE_CHANGED.to_string(), NOTE_PRESENCE.to_string()],
            workspace_id: Some(ws.to_string()),
            ..Default::default()
        });
    let next = |batch: Option<Vec<intent_core::Event>>| -> Vec<(String, Value)> {
        batch
            .expect("bus open")
            .into_iter()
            .map(|e| (e.event_type, e.data))
            .collect()
    };
    let roster_login = |data: &Value| -> Value {
        let members = data["members"].as_array().expect("members");
        assert_eq!(members.len(), 1, "{data}");
        members[0]["login"].clone()
    };

    with_caller(caller.clone(), async {
        services
            .presence_connect_op("conn-1".into())
            .await
            .expect("hello");
        services
            .note_presence_join_op("conn-1".into(), "lease-1".into(), ws.clone(), note.clone())
            .await
            .expect("subscribe");
    })
    .await;
    let seen = next(sub.recv().await);
    assert_eq!(seen[0].0, PRESENCE_CHANGED);
    assert_eq!(
        roster_login(&seen[0].1),
        Value::Null,
        "seen before identity"
    );
    let joined = next(sub.recv().await);
    assert_eq!(joined[0].0, NOTE_PRESENCE);
    assert_eq!(joined[0].1["kind"], "joined");
    assert_eq!(joined[0].1["login"], Value::Null);

    let identity = intent_sourcecontrol::UserIdentity {
        login: "octocat".to_string(),
        id: Some(583_231),
        name: Some("The Octocat".to_string()),
        avatar_url: Some("https://avatars.example/octocat".to_string()),
        html_url: None,
    };
    let applied = services
        .apply_primary_identity(row.clone(), &identity)
        .await
        .expect("identity applied");
    assert_eq!(applied.login.as_deref(), Some("octocat"));

    let renamed = next(sub.recv().await);
    assert_eq!(renamed[0].0, NOTE_PRESENCE);
    assert_eq!(renamed[0].1["kind"], "updated");
    assert_eq!(renamed[0].1["login"], "octocat");
    assert_eq!(renamed[0].1["displayName"], "The Octocat");
    assert_eq!(renamed[0].1["noteId"], "spec");
    assert_eq!(renamed[0].1["cursor"], Value::Null);
    let renamed = next(sub.recv().await);
    assert_eq!(renamed[0].0, PRESENCE_CHANGED);
    assert_eq!(roster_login(&renamed[0].1), "octocat", "{:?}", renamed[0].1);
    assert_eq!(
        renamed[0].1["members"][0]["avatarUrl"],
        "https://avatars.example/octocat"
    );

    let snapshot = with_caller(caller.clone(), services.presence_snapshot_op(ws.clone()))
        .await
        .expect("snapshot");
    assert_eq!(roster_login(&snapshot), "octocat");
    assert_eq!(
        services.presence.lock().conns.len(),
        1,
        "the connection never left"
    );

    services
        .apply_primary_identity(applied, &identity)
        .await
        .expect("idempotent");
    let quiet = tokio::time::timeout(Duration::from_millis(100), sub.recv()).await;
    assert!(
        quiet.is_err(),
        "an unchanged profile re-emits nothing: {quiet:?}"
    );
}

/// The first-sight read racing the identity write: `ensure_profile` has the
/// empty row in hand when `apply_primary_identity` persists the login and
/// finds nothing cached to refresh. The stale row must not be installed —
/// the read is retried and the `presence:changed` the connect emits, the
/// cache and `presence.snapshot` all carry the login. Driven through the
/// `profile_fetch_pause` seam, so the interleaving is exact.
#[tokio::test]
async fn first_sight_read_racing_the_identity_write_installs_the_fresh_profile() {
    use crate::events::SubscriptionFilter;
    use crate::presence::ProfileFetchPause;
    use intent_core::events::PRESENCE_CHANGED;
    use intent_core::with_caller;
    use std::sync::Arc;
    let tmp = crate::tests::TempDb::new();
    let root = crate::tests::WorkspacesRoot::new();
    let (store, services, ws, caller) = member_services(&tmp, &root).await;
    let principal = caller.principal_id().cloned().expect("wire caller");
    let mut row = store.get_principal(&principal).await.expect("principal");
    row.github_user_id = Some(583_231);
    row.login = None;
    store.upsert_principal(&row).await.expect("empty profile");
    let mut sub = services
        .event_bus
        .as_ref()
        .expect("bus")
        .subscribe(SubscriptionFilter {
            event_types: vec![PRESENCE_CHANGED.to_string()],
            workspace_id: Some(ws.to_string()),
            ..Default::default()
        });
    let pause = Arc::new(ProfileFetchPause::default());
    *services.presence.profile_fetch_pause.lock().unwrap() = Some(pause.clone());
    let identity = intent_sourcecontrol::UserIdentity {
        login: "octocat".to_string(),
        id: Some(583_231),
        name: None,
        avatar_url: None,
        html_url: None,
    };

    let connect = with_caller(
        caller.clone(),
        services.presence_connect_op("conn-1".into()),
    );
    let interleave = async {
        pause.fetched.notified().await;
        assert!(
            services.presence.lock().profiles.is_empty(),
            "the stale row is fetched but not yet installed"
        );
        services
            .apply_primary_identity(row.clone(), &identity)
            .await
            .expect("identity applied mid-read");
        pause.resume.notify_one();
    };
    let (connected, ()) = tokio::join!(connect, interleave);
    connected.expect("hello");

    assert_eq!(
        services.presence.lock().profiles[&principal]
            .login
            .as_deref(),
        Some("octocat"),
        "the re-read row is installed, not the stale one"
    );
    let batch = sub.recv().await.expect("bus open");
    let members = batch[0].data["members"].as_array().expect("members");
    assert_eq!(members.len(), 1, "{:?}", batch[0].data);
    assert_eq!(members[0]["login"], "octocat", "{:?}", batch[0].data);
    let snapshot = with_caller(caller.clone(), services.presence_snapshot_op(ws.clone()))
        .await
        .expect("snapshot");
    assert_eq!(snapshot["members"][0]["login"], "octocat");
    let quiet = tokio::time::timeout(Duration::from_millis(100), sub.recv()).await;
    assert!(quiet.is_err(), "one roster, from the connect: {quiet:?}");
}

/// A principal presence has never seen leaves no cache entry behind: the
/// refresh is a no-op rather than an unbounded insert.
#[tokio::test]
async fn profile_refresh_skips_a_principal_presence_never_saw() {
    let tmp = crate::tests::TempDb::new();
    let root = crate::tests::WorkspacesRoot::new();
    let (store, services, _ws, caller) = member_services(&tmp, &root).await;
    let principal = caller.principal_id().cloned().expect("wire caller");
    let row = store.get_principal(&principal).await.expect("principal");
    services.presence_profile_changed(&row).await;
    assert!(services.presence.lock().profiles.is_empty());
}

#[test]
fn parse_update_accepts_focus_set_and_optional_typing() {
    let (focus, typing) = parse_update(&json!({
        "focus": [
            { "workspaceId": "ws-1", "agentId": "agent-1" },
            { "workspaceId": "ws-1", "noteId": "spec" },
            { "workspaceId": "ws-1", "noteId": "spec" },
            { "workspaceId": "ws-2" }
        ],
        "typing": { "agentId": "agent-1" }
    }))
    .expect("valid");
    assert_eq!(focus.len(), 3, "duplicate focus items collapse");
    assert_eq!(typing.as_deref(), Some("agent-1"));
    let (focus, typing) = parse_update(&json!({ "focus": [], "typing": null })).expect("valid");
    assert!(focus.is_empty());
    assert!(typing.is_none());
}

#[test]
fn parse_update_rejects_malformed_params() {
    for bad in [
        json!({}),
        json!({ "focus": "ws-1" }),
        json!({ "focus": [{ "agentId": "a" }] }),
        json!({ "focus": [{ "workspaceId": "" }] }),
        json!({ "focus": [{ "workspaceId": "ws", "noteId": 3 }] }),
        json!({ "focus": [], "typing": {} }),
        json!({ "focus": [], "typing": "agent-1" }),
    ] {
        assert!(
            matches!(
                parse_update(&bad),
                Err(intent_core::Error::InvalidParams(_))
            ),
            "{bad}"
        );
    }
}

#[test]
fn parse_cursor_requires_three_non_negative_integers() {
    assert_eq!(
        parse_cursor(&json!({ "rev": 3, "anchor": 10, "head": 12, "extra": true })).expect("ok"),
        json!({ "rev": 3, "anchor": 10, "head": 12 })
    );
    for bad in [
        json!({ "rev": 3, "anchor": 10 }),
        json!({ "rev": -1, "anchor": 10, "head": 12 }),
        json!({ "rev": "3", "anchor": 10, "head": 12 }),
    ] {
        assert!(
            matches!(
                parse_cursor(&bad),
                Err(intent_core::Error::InvalidParams(_))
            ),
            "{bad}"
        );
    }
}
