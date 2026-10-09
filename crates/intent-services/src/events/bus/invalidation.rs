//! Payload-free subscriptions for consumers that re-read authoritative state.
use super::{event_matches, EventBus, SubscriptionFilter};
use intent_core::Event;
use std::sync::Arc;
use tokio::sync::{broadcast, watch};
use tokio::task::JoinHandle;

/// A coalesced notification with no retained event payload. The consumer must
/// re-authorize and re-read its selected state after each notification.
pub struct InvalidationSubscription {
    receiver: watch::Receiver<()>,
    handle: JoinHandle<()>,
}

impl InvalidationSubscription {
    fn from_receiver(
        receiver: broadcast::Receiver<Arc<Event>>,
        filter: SubscriptionFilter,
    ) -> Self {
        let (sender, changes) = watch::channel(());
        let handle = intent_core::spawn_daemon(deliver(receiver, filter, sender));
        Self {
            receiver: changes,
            handle,
        }
    }

    /// Wait for a matching event or conservative lag invalidation. Multiple
    /// notifications coalesce while unread. Returns false after the source
    /// closes and the final pending notification has been consumed.
    pub async fn changed(&mut self) -> bool {
        self.receiver.changed().await.is_ok()
    }
}

impl Drop for InvalidationSubscription {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

impl EventBus {
    /// Subscribe without cloning or queuing decoded event payloads. Existing
    /// filters apply; lost broadcast entries conservatively invalidate state.
    /// Unlike payload subscriptions, `batch_window` does not delay notification:
    /// a single watch value coalesces all changes until the consumer reads it.
    #[must_use]
    pub fn subscribe_invalidation(&self, filter: SubscriptionFilter) -> InvalidationSubscription {
        InvalidationSubscription::from_receiver(self.tx.subscribe(), filter)
    }
}

async fn deliver(
    mut receiver: broadcast::Receiver<Arc<Event>>,
    filter: SubscriptionFilter,
    sender: watch::Sender<()>,
) {
    loop {
        let dirty = match receiver.recv().await {
            Ok(event) => {
                let matched = event_matches(&filter, &event);
                // Release even the borrowed broadcast payload before notifying
                // or awaiting another event. No Event clone enters this path.
                drop(event);
                matched
            }
            Err(broadcast::error::RecvError::Lagged(_)) => true,
            Err(broadcast::error::RecvError::Closed) => return,
        };
        if dirty && sender.send(()).is_err() {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use intent_core::{ActorType, EventActor, WorkspaceId};
    use serde_json::json;
    use std::time::Duration;

    fn event(workspace: &str, event_type: &str, data: serde_json::Value) -> Arc<Event> {
        Arc::new(Event {
            id: "event".into(),
            workspace_id: WorkspaceId::from(workspace),
            timestamp: "2026-10-05T00:00:00Z".into(),
            event_type: event_type.into(),
            actor: EventActor {
                actor_type: ActorType::System,
                ..Default::default()
            },
            session_id: None,
            correlation_id: None,
            parent_event_id: None,
            metadata: None,
            data,
        })
    }

    #[tokio::test]
    async fn invalidation_releases_large_payload_before_delivering_notification() {
        let (sender, receiver) = broadcast::channel(2);
        let mut subscription =
            InvalidationSubscription::from_receiver(receiver, SubscriptionFilter::default());
        let large = event(
            "ws",
            "note:updated",
            json!({"content":"x".repeat(2_000_000)}),
        );
        let weak = Arc::downgrade(&large);
        sender.send(large).unwrap();
        assert!(
            tokio::time::timeout(Duration::from_secs(10), subscription.changed())
                .await
                .unwrap()
        );
        assert!(
            weak.upgrade().is_none(),
            "neither the worker nor its unread watch retains the Event"
        );
        drop(sender);
        assert!(
            !tokio::time::timeout(Duration::from_secs(10), subscription.changed())
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn invalidation_burst_coalesces_to_one_unread_change() {
        let (sender, receiver) = broadcast::channel(128);
        let mut subscription =
            InvalidationSubscription::from_receiver(receiver, SubscriptionFilter::default());
        for _ in 0..100 {
            sender.send(event("ws", "note:updated", json!({}))).unwrap();
        }
        drop(sender);
        // Wait for actual worker settlement before consuming the coalesced bit.
        tokio::time::timeout(Duration::from_secs(10), &mut subscription.handle)
            .await
            .unwrap()
            .unwrap();
        assert!(subscription.changed().await);
        assert!(!subscription.changed().await);
    }

    #[tokio::test]
    async fn invalidation_lag_signals_refresh_without_retaining_overwritten_payload() {
        let (sender, receiver) = broadcast::channel(2);
        let mut subscription = InvalidationSubscription::from_receiver(
            receiver,
            SubscriptionFilter {
                event_types: vec!["note:updated".into()],
                ..Default::default()
            },
        );
        let large = event(
            "ws",
            "note:updated",
            json!({"content":"x".repeat(2_000_000)}),
        );
        let weak = Arc::downgrade(&large);
        // No await on this current-thread runtime: overwrite the matching
        // event before the worker can inspect it. Remaining events don't match.
        sender.send(large).unwrap();
        for _ in 0..8 {
            sender
                .send(event("ws", "comment:added", json!({})))
                .unwrap();
        }
        assert!(weak.upgrade().is_none());
        drop(sender);
        tokio::time::timeout(Duration::from_secs(10), &mut subscription.handle)
            .await
            .unwrap()
            .unwrap();
        assert!(
            subscription.changed().await,
            "lag itself must trigger refresh"
        );
        assert!(!subscription.changed().await);
    }

    #[tokio::test]
    async fn invalidation_preserves_workspace_and_type_filters() {
        for (workspace, event_type, expected) in [
            ("other", "note:updated", false),
            ("ws", "comment:added", false),
            ("ws", "note:updated", true),
        ] {
            let (sender, receiver) = broadcast::channel(2);
            let mut subscription = InvalidationSubscription::from_receiver(
                receiver,
                SubscriptionFilter {
                    workspace_id: Some("ws".into()),
                    event_types: vec!["note:updated".into()],
                    ..Default::default()
                },
            );
            sender
                .send(event(workspace, event_type, json!({})))
                .unwrap();
            drop(sender);
            tokio::time::timeout(Duration::from_secs(10), &mut subscription.handle)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(subscription.changed().await, expected);
            assert!(!subscription.changed().await);
        }
    }

    #[tokio::test]
    async fn invalidation_drop_aborts_worker_and_releases_broadcast_receiver() {
        let (sender, receiver) = broadcast::channel(2);
        let subscription =
            InvalidationSubscription::from_receiver(receiver, SubscriptionFilter::default());
        assert_eq!(sender.receiver_count(), 1);
        drop(subscription);
        tokio::time::timeout(Duration::from_secs(10), async {
            while sender.receiver_count() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(sender.send(event("ws", "note:updated", json!({}))).is_err());
    }
}
