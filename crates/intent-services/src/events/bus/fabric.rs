//! The event transport seam under [`super::EventBus`].
//!
//! [`EventBus`](super::EventBus) owns everything above the transport: the
//! durable writer (append-then-broadcast), attribution, filtering, batching,
//! fences and [`Delivery::Lagged`](super::Delivery::Lagged). An
//! [`EventFabric`] owns only the raw ring the writer and the transient
//! publishers put envelopes on and every subscription's delivery task reads.
//! [`LocalFabric`] is today's in-process `tokio::broadcast` ring and the only
//! implementation; a networked fabric can come later without touching the
//! bus or its consumers.
use std::sync::{Arc, Mutex};

use tokio::sync::{broadcast, mpsc};

use super::fence::{FenceHandle, FenceRequest};
use super::BroadcastEvent;

/// What one subscription reads from a fabric: the raw receiver, the raw
/// cursor at registration (the count of envelopes published before it), the
/// subscription's [`FenceHandle`], and the receiving side of its fence
/// controls. The bus's delivery task consumes all four unchanged.
pub(crate) type RawSubscription = (
    broadcast::Receiver<Arc<BroadcastEvent>>,
    u128,
    FenceHandle,
    mpsc::Receiver<FenceRequest>,
);

/// The raw transport under [`super::EventBus`].
///
/// # Contract
///
/// 1. **Order.** Envelopes passed to [`Self::publish`] reach every receiver
///    in publish order. The bus's single-publisher guarantee (a persisted
///    event is on the ring before its publisher wakes) depends on `publish`
///    having placed the envelope before it returns.
/// 2. **Cursor.** The cursor in [`Self::receiver`] and the boundary a
///    [`FenceHandle::request`] captures count raw envelopes in the same
///    sequence the receiver yields, so a fence completes exactly after every
///    envelope published before it. Registration and publication must be
///    atomic with respect to that count.
/// 3. **Loss.** A receiver that falls behind reports
///    `RecvError::Lagged(n)`; the bus turns that into
///    [`Delivery::Lagged`](super::Delivery::Lagged) and consumers recover
///    from the durable log. A fabric never drops silently.
///
/// # Local-only lanes
///
/// [`BroadcastEvent::Monitor`] carries an `Arc<dyn
/// RepositoryMonitorEventSource>` and is not serializable;
/// [`BroadcastEvent::HistoryChanged`] addresses in-process qualified
/// recipients. Both are local-only: a non-local fabric must still deliver
/// them to this process's receivers, in order with the public envelopes, and
/// must never send them off the process. The machine home-record tap
/// ([`super::EventBus::subscribe_home_records`]) is not on the fabric at
/// all; it stays an in-process channel on the bus.
pub(crate) trait EventFabric: Send + Sync + 'static {
    /// Put one envelope on the ring for every live receiver.
    fn publish(&self, envelope: Arc<BroadcastEvent>);

    /// Register a new receiver at the current raw tail.
    fn receiver(&self) -> RawSubscription;

    /// Number of live receivers.
    fn receiver_count(&self) -> usize;

    /// Envelopes not yet read by every live receiver.
    #[cfg(test)]
    fn undelivered(&self) -> usize;
}

impl<T: EventFabric + ?Sized> EventFabric for Arc<T> {
    fn publish(&self, envelope: Arc<BroadcastEvent>) {
        (**self).publish(envelope);
    }

    fn receiver(&self) -> RawSubscription {
        (**self).receiver()
    }

    fn receiver_count(&self) -> usize {
        (**self).receiver_count()
    }

    #[cfg(test)]
    fn undelivered(&self) -> usize {
        (**self).undelivered()
    }
}

/// The in-process fabric: one `tokio::broadcast` ring plus the raw cursor.
/// Publication and subscription registration share one short synchronous
/// lock. A captured cursor counts raw deliveries, including filtered
/// transient events.
#[derive(Clone)]
pub(crate) struct LocalFabric {
    sender: broadcast::Sender<Arc<BroadcastEvent>>,
    tail: Arc<Mutex<u128>>,
}

impl LocalFabric {
    /// A ring of `capacity` envelopes.
    ///
    /// # Panics
    /// Panics when `capacity` is zero.
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self::from(broadcast::channel(capacity).0)
    }

    pub(super) fn send(&self, event: Arc<BroadcastEvent>) {
        let mut tail = self.tail.lock().expect("raw bus cursor");
        *tail += 1;
        let _ = self.sender.send(event);
    }

    pub(super) fn receiver(&self) -> RawSubscription {
        let tail = self.tail.lock().expect("raw bus cursor");
        let receiver = self.sender.subscribe();
        let (tx, rx) = mpsc::channel(64);
        (
            receiver,
            *tail,
            FenceHandle::new(Arc::downgrade(&self.tail), tx),
            rx,
        )
    }
}

impl From<broadcast::Sender<Arc<BroadcastEvent>>> for LocalFabric {
    fn from(sender: broadcast::Sender<Arc<BroadcastEvent>>) -> Self {
        Self {
            sender,
            tail: Arc::new(Mutex::new(0)),
        }
    }
}

impl std::ops::Deref for LocalFabric {
    type Target = broadcast::Sender<Arc<BroadcastEvent>>;
    fn deref(&self) -> &Self::Target {
        &self.sender
    }
}

impl EventFabric for LocalFabric {
    fn publish(&self, envelope: Arc<BroadcastEvent>) {
        self.send(envelope);
    }

    fn receiver(&self) -> RawSubscription {
        LocalFabric::receiver(self)
    }

    fn receiver_count(&self) -> usize {
        self.sender.receiver_count()
    }

    #[cfg(test)]
    fn undelivered(&self) -> usize {
        self.sender.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use intent_core::Event;

    fn event(id: &str) -> Arc<BroadcastEvent> {
        Arc::new(BroadcastEvent::Public(Event {
            id: id.into(),
            workspace_id: intent_core::WorkspaceId::from("ws-a"),
            timestamp: "now".into(),
            event_type: "file:changed".into(),
            actor: intent_core::EventActor::default(),
            session_id: None,
            correlation_id: None,
            parent_event_id: None,
            metadata: None,
            data: serde_json::json!({}),
        }))
    }

    /// Through the trait object the bus holds, a receiver starts at the raw
    /// tail and sees later envelopes in publish order.
    #[tokio::test]
    async fn dyn_fabric_cursor_counts_prior_publishes_and_preserves_order() {
        let fabric: Arc<dyn EventFabric> = Arc::new(LocalFabric::with_capacity(8));
        fabric.publish(event("before"));
        let (mut rx, cursor, _fence, _controls) = fabric.receiver();
        assert_eq!(cursor, 1, "cursor counts envelopes published before");
        assert_eq!(fabric.receiver_count(), 1);
        fabric.publish(event("a"));
        fabric.publish(event("b"));
        assert_eq!(fabric.undelivered(), 2);
        assert_eq!(rx.recv().await.unwrap().id, "a");
        assert_eq!(rx.recv().await.unwrap().id, "b");
        drop(rx);
        assert_eq!(fabric.receiver_count(), 0);
    }

    /// A fence captured through the trait object resolves against the same
    /// cursor the fabric advances on publish.
    #[test]
    fn dyn_fabric_fence_boundary_tracks_publishes() {
        let fabric: Arc<dyn EventFabric> = Arc::new(LocalFabric::with_capacity(8));
        let (_rx, _cursor, fence, mut controls) = fabric.receiver();
        fabric.publish(event("a"));
        fabric.publish(event("b"));
        let _receipt = fence.request().expect("fence queued");
        let request = controls.try_recv().expect("fence request");
        assert_eq!(request.boundary, 2);
    }
}
