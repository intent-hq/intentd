//! Private raw-delivery receipts. Controls never occupy the broadcast ring.
use super::{Delivery, Event};
use std::sync::{Arc, Mutex, Weak};
use tokio::sync::{broadcast, mpsc, oneshot};

/// Publication and subscription registration share one short synchronous lock.
/// A captured cursor counts raw deliveries, including filtered transient events.
#[derive(Clone)]
pub(crate) struct RawBus {
    pub(super) sender: broadcast::Sender<Arc<Event>>,
    tail: Arc<Mutex<u128>>,
}

impl From<broadcast::Sender<Arc<Event>>> for RawBus {
    fn from(sender: broadcast::Sender<Arc<Event>>) -> Self {
        Self {
            sender,
            tail: Arc::new(Mutex::new(0)),
        }
    }
}

impl std::ops::Deref for RawBus {
    type Target = broadcast::Sender<Arc<Event>>;
    fn deref(&self) -> &Self::Target {
        &self.sender
    }
}

impl RawBus {
    pub(super) fn send(&self, event: Arc<Event>) {
        let mut tail = self.tail.lock().expect("raw bus cursor");
        *tail += 1;
        let _ = self.sender.send(event);
    }

    pub(super) fn receiver(
        &self,
    ) -> (
        broadcast::Receiver<Arc<Event>>,
        u128,
        FenceHandle,
        mpsc::Receiver<FenceRequest>,
    ) {
        let tail = self.tail.lock().expect("raw bus cursor");
        let receiver = self.sender.subscribe();
        let (tx, rx) = mpsc::channel(64);
        (
            receiver,
            *tail,
            FenceHandle {
                tail: Arc::downgrade(&self.tail),
                tx,
            },
            rx,
        )
    }
}

pub(super) struct FenceRequest {
    pub(super) boundary: u128,
    pub(super) ack: oneshot::Sender<()>,
}

/// A bounded, nonblocking control handle for one subscription. It does not keep
/// the source bus alive. A receipt completes only when the consumer acknowledges
/// its ordered `Delivery::Fence`, never just because the raw worker caught up.
#[derive(Clone)]
pub struct FenceHandle {
    tail: Weak<Mutex<u128>>,
    tx: mpsc::Sender<FenceRequest>,
}

impl FenceHandle {
    /// Capture the current raw tail and request an in-band receipt.
    ///
    /// # Errors
    /// Fails if the source/subscription closed or its bounded control queue is full.
    ///
    /// # Panics
    /// Panics if the raw publication cursor lock is poisoned.
    pub fn request(&self) -> Result<oneshot::Receiver<()>, &'static str> {
        let tail = self.tail.upgrade().ok_or("source closed")?;
        let boundary = *tail.lock().expect("raw bus cursor");
        let (ack, receipt) = oneshot::channel();
        self.tx
            .try_send(FenceRequest { boundary, ack })
            .map_err(|_| "fence queue unavailable")?;
        Ok(receipt)
    }
}

pub(super) async fn flush_fence(
    request: FenceRequest,
    buffer: &mut Vec<Event>,
    out: &mpsc::Sender<Delivery>,
) -> bool {
    if request.ack.is_closed() {
        return true;
    }
    if !buffer.is_empty()
        && out
            .send(Delivery::Batch(std::mem::take(buffer)))
            .await
            .is_err()
    {
        return false;
    }
    out.send(Delivery::Fence(request.ack)).await.is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::Future as _;
    use std::task::{Context, Waker};
    use std::time::Duration;

    #[tokio::test(start_paused = true)]
    async fn recovery_cancelled_fence_preserves_batch_deadline() {
        let raw = RawBus::from(broadcast::channel(4).0);
        let (rx, observed, controls, requests) = raw.receiver();
        let (out, mut deliveries) = mpsc::channel(4);
        let window = Duration::from_secs(60);
        let filter = crate::events::SubscriptionFilter {
            batch_window: Some(window),
            ..Default::default()
        };
        let worker = super::super::delivery_task(rx, filter, out, observed, requests);
        tokio::pin!(worker);
        let event = Event {
            id: "buffered".into(),
            workspace_id: intent_core::WorkspaceId::from("ws-a"),
            timestamp: "now".into(),
            event_type: "file:changed".into(),
            actor: intent_core::EventActor::default(),
            session_id: None,
            correlation_id: None,
            parent_event_id: None,
            metadata: None,
            data: serde_json::json!({}),
        };
        raw.send(Arc::new(event));
        assert!(worker
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending());
        assert!(deliveries.try_recv().is_err());
        drop(controls.request().unwrap());
        assert!(worker
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending());
        tokio::time::advance(window).await;
        assert!(worker
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending());
        assert!(
            matches!(deliveries.try_recv(), Ok(Delivery::Batch(batch)) if batch.len() == 1),
            "cancelled fence must leave the ordinary batch timer armed"
        );
    }
}
