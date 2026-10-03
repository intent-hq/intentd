//! Private cross-workspace invalidations are triggers only: never forward their payloads.
use super::{send_fast_path_error, spawn_forwarder, ConnSubs, OutboundSender};
use crate::{events, presence, subscriptions};
use futures::FutureExt;
use intent_core::events::{
    AGENT_DELETED, AGENT_UPDATED, HOST_MEMBERS_CHANGED, NOTE_DELETED, NOTE_UPDATED,
    PRESENCE_CHANGED, WORKSPACE_DELETED, WORKSPACE_UPDATED,
};
use intent_core::{PrincipalId, WorkspaceApi, WorkspaceId};
use intent_services::{EventBus, Subscription, SubscriptionFilter};
use serde_json::{json, Value};
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

pub(super) async fn subscribe(
    id: events::IdInfo,
    params: serde_json::Map<String, Value>,
    api: &Arc<dyn WorkspaceApi>,
    bus: &EventBus,
    out_tx: &OutboundSender,
    subs: &mut ConnSubs,
) -> bool {
    let parsed = subscriptions::parse_subscribe_params(&params);
    let principal = params
        .get("principalId")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty());
    let (p, principal) = match (parsed, principal) {
        (Ok(p), Some(principal)) => (p, PrincipalId::from(principal)),
        (Err(msg), _) => return send_fast_path_error(id, &msg, out_tx).await,
        (_, None) => return send_fast_path_error(id, "principalId is required", out_tx).await,
    };
    if let Some(group) = p.replace_group.as_deref() {
        subs.remove_group(api, group).await;
    }
    let workspace = WorkspaceId::from(p.workspace_id);
    // Deliberately daemon-internal and unscoped: a person's destination can be
    // outside the source. Only the separately authorized projection is serialized.
    let changes = bus.subscribe(SubscriptionFilter {
        event_types: vec![
            PRESENCE_CHANGED,
            HOST_MEMBERS_CHANGED,
            WORKSPACE_UPDATED,
            WORKSPACE_DELETED,
            AGENT_DELETED,
            AGENT_UPDATED,
            NOTE_DELETED,
            NOTE_UPDATED,
        ]
        .into_iter()
        .map(str::to_string)
        .collect(),
        ..Default::default()
    });
    if let Err(error) = api
        .presence_focus_snapshot(workspace.clone(), principal.clone())
        .await
    {
        return match presence::respond(id.present, &id.echo, Err(error)) {
            Some(frame) => out_tx.send_priority(frame).await.is_ok(),
            None => true,
        };
    }
    let subscription_id = events::next_subscription_id();
    if id.present
        && out_tx
            .send_priority(events::success_frame(
                &id.echo,
                &json!({"subscriptionId":subscription_id}),
            ))
            .await
            .is_err()
    {
        return false;
    }
    let handle = spawn_forwarder(forward(
        api.clone(),
        workspace,
        principal,
        changes,
        subscription_id.clone(),
        out_tx.clone(),
    ));
    subs.insert(subscription_id, handle, p.replace_group, None);
    true
}

async fn forward(
    api: Arc<dyn WorkspaceApi>,
    workspace: WorkspaceId,
    principal: PrincipalId,
    mut changes: Subscription,
    subscription_id: String,
    out_tx: OutboundSender,
) {
    let mut seq = 0;
    let mut previous = None;
    let result = AssertUnwindSafe(async {
        loop {
            // Backpressure precedes authorization; no await after the final read.
            let sender = out_tx.bulk_sender();
            let Ok(permit) = sender.reserve().await else { return; };
            while changes.try_recv_delivery().is_some() {}
            let result = tokio::select! {
                biased;
                event = changes.recv_delivery() => { if event.is_none() { return; } continue; }
                result = api.presence_focus_snapshot(workspace.clone(), principal.clone()) => result,
            };
            // A queued invalidation (including lag) overtakes this read.
            if changes.try_recv_delivery().is_some() { continue; }
            let Ok(snapshot) = result else { return; };
            if previous.as_ref() == Some(&snapshot) {
                drop(permit);
            } else {
                permit.send(subscriptions::build_snapshot_push(&subscription_id, seq, &snapshot));
                seq += 1;
                previous = Some(snapshot);
            }
            if changes.recv_delivery().await.is_none() { return; }
        }
    }).catch_unwind().await;
    if result.is_err() {
        tracing::error!("presence focus forwarder panicked");
    }
    // An explicit unsubscribe/replacement/connection close aborts this whole
    // future and is known locally. Every other exit, including panic and bus
    // closure, clears the target and tells the client this channel has ended.
    let closed =
        json!({"workspaceId":workspace,"principalId":principal,"target":null,"closed":true});
    let _ = out_tx
        .send_bulk(subscriptions::build_snapshot_push(
            &subscription_id,
            seq,
            &closed,
        ))
        .await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use intent_core::{Error, EventActor};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::{mpsc, Notify};

    struct DelayedFocus {
        calls: AtomicUsize,
        entered: Notify,
        release: Notify,
    }
    impl WorkspaceApi for DelayedFocus {
        fn presence_focus_snapshot(
            &self,
            workspace: WorkspaceId,
            person: PrincipalId,
        ) -> intent_core::BoxFuture<'_, intent_core::Result<Value>> {
            Box::pin(async move {
                if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    self.entered.notify_one();
                    self.release.notified().await;
                    Ok(
                        json!({"workspaceId":workspace,"principalId":person,"target":{"workspaceId":"secret-old"}}),
                    )
                } else {
                    Err(Error::NotFound("presence focus".into()))
                }
            })
        }
    }

    #[tokio::test]
    async fn membership_invalidation_overtakes_delayed_authorized_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let store = intent_store::Store::open(&dir.path().join("focus.db"))
            .await
            .unwrap();
        let bus = EventBus::new(store);
        let changes = bus.subscribe(SubscriptionFilter {
            event_types: vec![WORKSPACE_UPDATED.into()],
            ..Default::default()
        });
        let api = Arc::new(DelayedFocus {
            calls: AtomicUsize::new(0),
            entered: Notify::new(),
            release: Notify::new(),
        });
        let (priority, _priority_rx) = mpsc::channel(1);
        let (bulk, mut rx) = mpsc::channel(1);
        let task = tokio::spawn(forward(
            api.clone(),
            WorkspaceId::from("source"),
            PrincipalId::from("person"),
            changes,
            "focus-sub".into(),
            OutboundSender {
                priority,
                bulk,
                shutdown: None,
            },
        ));
        tokio::time::timeout(std::time::Duration::from_secs(5), api.entered.notified())
            .await
            .unwrap();
        let _ = bus.publish_transient(&intent_store::NewEvent {
            workspace_id: WorkspaceId::from("secret-old"),
            timestamp: intent_core::now_iso(),
            event_type: WORKSPACE_UPDATED.into(),
            actor: EventActor::default(),
            session_id: None,
            correlation_id: None,
            parent_event_id: None,
            metadata: None,
            data: json!({"private":"must not escape"}),
        });
        // The stale read is still parked: the event must cancel it, perform a
        // fresh authorization read, clear the target and stop without release.
        let frame = tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
            .await
            .unwrap()
            .unwrap();
        let value: Value = serde_json::from_str(&frame).unwrap();
        assert_eq!(value["params"]["seq"], 0);
        assert_eq!(
            value["params"]["snapshot"],
            json!({"workspaceId":"source","principalId":"person","target":null,"closed":true})
        );
        assert!(!frame.contains("secret-old"));
        assert!(!frame.contains("private"));
        task.await.unwrap();
        assert_eq!(api.calls.load(Ordering::SeqCst), 2);
        assert!(rx.recv().await.is_none());
    }
    struct EndsFocus {
        calls: AtomicUsize,
        panics: bool,
    }
    impl WorkspaceApi for EndsFocus {
        fn presence_focus_snapshot(
            &self,
            workspace: WorkspaceId,
            person: PrincipalId,
        ) -> intent_core::BoxFuture<'_, intent_core::Result<Value>> {
            Box::pin(async move {
                if self.calls.fetch_add(1, Ordering::SeqCst) > 0 {
                    assert!(!self.panics, "controlled forwarder panic");
                    return Err(Error::Internal("private backend detail".into()));
                }
                Ok(
                    json!({"workspaceId":workspace,"principalId":person,"target":{"workspaceId":"destination"}}),
                )
            })
        }
    }

    #[tokio::test]
    async fn focus_error_panic_and_bus_close_explicitly_end_a_live_channel() {
        for reason in ["error", "panic", "bus-close"] {
            let dir = tempfile::tempdir().unwrap();
            let store = intent_store::Store::open(&dir.path().join("focus.db"))
                .await
                .unwrap();
            let bus = Some(EventBus::new(store));
            let changes = bus.as_ref().unwrap().subscribe(SubscriptionFilter {
                event_types: vec![WORKSPACE_UPDATED.into()],
                ..Default::default()
            });
            let api = Arc::new(EndsFocus {
                calls: AtomicUsize::new(0),
                panics: reason == "panic",
            });
            let (priority, _priority_rx) = mpsc::channel(1);
            let (bulk, mut rx) = mpsc::channel(1);
            let task = tokio::spawn(forward(
                api,
                WorkspaceId::from("source"),
                PrincipalId::from("person"),
                changes,
                "focus-sub".into(),
                OutboundSender {
                    priority,
                    bulk,
                    shutdown: None,
                },
            ));
            let first = tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
                .await
                .unwrap()
                .unwrap();
            let first: Value = serde_json::from_str(&first).unwrap();
            assert_eq!(first["params"]["seq"], 0);
            assert!(first["params"]["snapshot"].get("closed").is_none());
            if reason != "bus-close" {
                let _ = bus
                    .as_ref()
                    .unwrap()
                    .publish_transient(&intent_store::NewEvent {
                        workspace_id: WorkspaceId::from("destination"),
                        timestamp: intent_core::now_iso(),
                        event_type: WORKSPACE_UPDATED.into(),
                        actor: EventActor::default(),
                        session_id: None,
                        correlation_id: None,
                        parent_event_id: None,
                        metadata: None,
                        data: json!({"private":"data"}),
                    });
            }
            let retained_bus = if reason == "bus-close" {
                drop(bus);
                None
            } else {
                bus
            };
            let closed = tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
                .await
                .unwrap()
                .unwrap();
            let value: Value = serde_json::from_str(&closed).unwrap();
            assert_eq!(value["params"]["seq"], 1);
            assert_eq!(
                value["params"]["snapshot"],
                json!({"workspaceId":"source","principalId":"person","target":null,"closed":true})
            );
            assert!(!closed.contains("private"));
            task.await.unwrap();
            drop(retained_bus);
            assert!(rx.recv().await.is_none());
        }
    }
}
