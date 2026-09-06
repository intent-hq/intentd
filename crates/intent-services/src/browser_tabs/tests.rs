use std::time::Duration;

use intent_core::events::{BROWSER_TAB_CLOSED, BROWSER_TAB_OPENED, BROWSER_TAB_UPDATED};
use intent_core::{
    BrowserTabInput, BrowserTabVisibility, ClientHostInfo, ClientId, Event, WorkspaceId,
};
use intent_store::Store;
use serde_json::json;

use crate::tests::{workspace, TempDb};
use crate::{EventBus, Services, Subscription, SubscriptionFilter};

fn input(ws: &WorkspaceId, tab_id: &str) -> BrowserTabInput {
    BrowserTabInput {
        tab_id: tab_id.to_string(),
        workspace_id: ws.clone(),
        url: format!("https://example.test/{tab_id}"),
        requested_url: None,
        title: None,
        owner_agent_id: None,
        owner_agent_name: None,
        visibility: BrowserTabVisibility::default(),
        emulated_size: None,
    }
}

fn tab_id_of(ev: &Event) -> &str {
    ev.data["tab"]["tabId"].as_str().expect("tab id")
}

async fn recv_one(sub: &mut Subscription) -> Vec<Event> {
    tokio::time::timeout(Duration::from_secs(10), sub.recv())
        .await
        .expect("batch in time")
        .expect("subscription open")
}

/// A `tabId` is bound to the workspace that created it. Events are
/// workspace-scoped, so letting a report move the row would strand the old
/// workspace's subscribers with a ghost tab; instead a report naming another
/// `workspaceId` is rejected with `InvalidParams` (-32602) on both the
/// `upsertTab` and the `syncTabs` path, publishes nothing to either
/// workspace, and leaves the row where it was. A same-workspace update
/// afterwards still flows, so the rejection is side-effect free.
#[tokio::test]
async fn workspace_change_is_rejected_without_events_or_row_changes() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws_a = WorkspaceId::new();
    let ws_b = WorkspaceId::new();
    store
        .insert_workspace(&workspace(&ws_a))
        .await
        .expect("ws a");
    store
        .insert_workspace(&workspace(&ws_b))
        .await
        .expect("ws b");
    let host = ClientId::from_string("desktop-a");
    store
        .upsert_client(
            &host,
            Some("Desktop A"),
            Some(&json!({ "browserExec": true })),
            &ClientHostInfo::default(),
        )
        .await
        .expect("client");
    let bus = EventBus::new(store.clone());
    let services = Services::new(store.clone()).with_event_bus(bus.clone());
    let subscribe = |ws: &WorkspaceId| {
        bus.subscribe(SubscriptionFilter {
            workspace_id: Some(ws.0.clone()),
            event_types: vec!["browser:*".to_string()],
            ..Default::default()
        })
    };
    let mut sub_a = subscribe(&ws_a);
    let mut sub_b = subscribe(&ws_b);

    services
        .browser_tab_upsert(host.clone(), input(&ws_a, "t-1"))
        .await
        .expect("open in a");
    let opened = recv_one(&mut sub_a).await;
    assert_eq!(opened.len(), 1);
    assert_eq!(opened[0].event_type, BROWSER_TAB_OPENED);

    let before = store.list_browser_tabs(&ws_a).await.expect("list a");
    assert_eq!(before.len(), 1);

    // upsertTab naming workspace b for a's tab.
    let err = services
        .browser_tab_upsert(host.clone(), input(&ws_b, "t-1"))
        .await
        .expect_err("workspace change rejected");
    assert!(
        matches!(err, intent_core::Error::InvalidParams(_)),
        "{err:?}"
    );

    // syncTabs naming workspace b for a's tab: the whole snapshot is
    // rejected — the new row written ahead of the offending one in the same
    // transaction is rolled back, not partially applied.
    let err = services
        .browser_tabs_sync(
            host.clone(),
            vec![input(&ws_b, "t-new"), input(&ws_b, "t-1")],
        )
        .await
        .expect_err("workspace change rejected in sync");
    assert!(
        matches!(err, intent_core::Error::InvalidParams(_)),
        "{err:?}"
    );

    assert_eq!(
        store.list_browser_tabs(&ws_a).await.expect("list a"),
        before,
        "row untouched"
    );
    assert!(store
        .list_browser_tabs(&ws_b)
        .await
        .expect("list b")
        .is_empty());

    // Nothing was published to either workspace: the next event on a is the
    // legitimate same-workspace update, and b's stream stays silent.
    let mut changed = input(&ws_a, "t-1");
    changed.title = Some("after".to_string());
    services
        .browser_tab_upsert(host.clone(), changed)
        .await
        .expect("same-workspace update");
    let updated = recv_one(&mut sub_a).await;
    assert_eq!(updated.len(), 1, "{updated:?}");
    assert_eq!(updated[0].event_type, BROWSER_TAB_UPDATED);
    assert_eq!(tab_id_of(&updated[0]), "t-1");
    assert_eq!(updated[0].data["changes"], json!({ "title": "after" }));
    assert!(
        tokio::time::timeout(Duration::from_millis(200), sub_b.recv())
            .await
            .is_err(),
        "workspace b received an event for a rejected report"
    );
}

/// A `removeTab` racing a large `syncTabs` must not publish `tab-closed` for
/// an id before that snapshot's stale `tab-opened` for the same id: the
/// database ends without the row, so the subscriber's last word on the id
/// has to be `closed`. Each mutation holds `browser_tab_gate` across its
/// store transaction and its complete event publication, which orders the
/// stream as [opened, closed] for the removed id.
#[tokio::test]
async fn remove_racing_sync_never_publishes_a_stale_opened_after_closed() {
    const TABS: usize = 150;
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    store.insert_workspace(&workspace(&ws)).await.expect("ws");
    let host = ClientId::from_string("desktop-a");
    store
        .upsert_client(
            &host,
            Some("Desktop A"),
            Some(&json!({ "browserExec": true })),
            &ClientHostInfo::default(),
        )
        .await
        .expect("client");
    let bus = EventBus::new(store.clone());
    let services = Services::new(store.clone()).with_event_bus(bus.clone());
    let mut sub = bus.subscribe(SubscriptionFilter {
        workspace_id: Some(ws.0.clone()),
        event_types: vec!["browser:*".to_string()],
        ..Default::default()
    });

    let last = format!("t-{}", TABS - 1);
    let snapshot: Vec<BrowserTabInput> = (0..TABS).map(|i| input(&ws, &format!("t-{i}"))).collect();
    let sync = tokio::spawn({
        let services = services.clone();
        let host = host.clone();
        async move { services.browser_tabs_sync(host, snapshot).await }
    });

    // Once the first `tab-opened` is out, the snapshot is committed and sync
    // is mid-publication; a remove issued now is the reviewer's race.
    let mut seen: Vec<Event> = Vec::new();
    let first = tokio::time::timeout(Duration::from_secs(10), sub.recv())
        .await
        .expect("first batch in time")
        .expect("subscription open");
    assert!(first.iter().any(|e| e.event_type == BROWSER_TAB_OPENED));
    seen.extend(first);
    services
        .browser_tab_remove(host.clone(), last.clone())
        .await
        .expect("remove");
    let drop = sync.await.expect("sync task").expect("sync");
    assert!(drop.is_empty(), "nothing to drop: {drop:?}");

    // Drain until every opened and the one closed have been delivered.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while seen
        .iter()
        .filter(|e| e.event_type == BROWSER_TAB_OPENED)
        .count()
        < TABS
        || !seen.iter().any(|e| e.event_type == BROWSER_TAB_CLOSED)
    {
        let batch = tokio::time::timeout_at(deadline, sub.recv())
            .await
            .unwrap_or_else(|_| panic!("events still missing after deadline: {}", seen.len()))
            .expect("subscription open");
        seen.extend(batch);
    }

    let last_events: Vec<&str> = seen
        .iter()
        .filter(|e| tab_id_of(e) == last)
        .map(|e| e.event_type.as_str())
        .collect();
    assert_eq!(
        last_events,
        vec![BROWSER_TAB_OPENED, BROWSER_TAB_CLOSED],
        "removed id must never be re-opened by a stale snapshot event"
    );
    assert!(
        store.get_browser_tab(&last).await.expect("get").is_none(),
        "the removed row is gone"
    );
    // Every id's final event agrees with the database: open rows end on
    // `opened`, the removed one on `closed`.
    let open: std::collections::HashSet<String> = store
        .list_browser_tabs(&ws)
        .await
        .expect("list")
        .into_iter()
        .map(|t| t.tab_id)
        .collect();
    assert_eq!(open.len(), TABS - 1);
    for i in 0..TABS {
        let id = format!("t-{i}");
        let final_event = seen
            .iter()
            .rev()
            .find(|e| tab_id_of(e) == id)
            .map(|e| e.event_type.as_str())
            .expect("every id has an event");
        let expected = if open.contains(&id) {
            BROWSER_TAB_OPENED
        } else {
            BROWSER_TAB_CLOSED
        };
        assert_eq!(final_event, expected, "{id}");
    }
}
