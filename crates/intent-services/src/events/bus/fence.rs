//! Private raw-delivery receipts. Controls never occupy the broadcast ring.
use super::{Delivery, Event};
use std::sync::{Mutex, Weak};
use tokio::sync::{mpsc, oneshot};

pub(crate) struct FenceRequest {
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
    /// A handle over a fabric's raw cursor and one subscription's control queue.
    pub(super) fn new(tail: Weak<Mutex<u128>>, tx: mpsc::Sender<FenceRequest>) -> Self {
        Self { tail, tx }
    }

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
    use super::super::{BroadcastEvent, RawBus};
    use super::*;
    use std::future::Future as _;
    use std::sync::Arc;
    use std::task::{Context, Waker};
    use std::time::Duration;
    use tokio::sync::broadcast;

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
        let worker = super::super::delivery_task(rx, filter, out, observed, requests, None, None);
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
        raw.send(Arc::new(BroadcastEvent::Public(event)));
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
