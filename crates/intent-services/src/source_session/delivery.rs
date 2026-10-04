//! Typed outcome ownership from real source work through local write settlement.
use super::{
    map_error,
    owner::{Guard, Owner},
};
use crate::artifact_source::DeliveryHold;
use intent_core::note_source_session::{validate_id, wire, Reason, Result, SessionError};
use serde_json::Value;
use std::sync::{atomic::Ordering, Arc};

/// One outcome and its admitted continuation. Dropping it without explicit local
/// retirement quarantines the same operation; it does not manufacture a receipt.
pub struct Delivery {
    pub(super) owner: Arc<Owner>,
    outcome: Result<Value>,
    hold: Option<DeliveryHold>,
    wire_limit: usize,
    id: Value,
    sequence: u64,
    authorized: bool,
    writing: bool,
    retired: bool,
}
impl Delivery {
    pub(super) fn new(
        owner: Arc<Owner>,
        outcome: Result<Value>,
        hold: Option<DeliveryHold>,
        wire_limit: usize,
        id: Value,
        sequence: u64,
    ) -> Self {
        Self {
            owner,
            outcome,
            hold,
            wire_limit,
            id,
            sequence,
            authorized: false,
            writing: false,
            retired: false,
        }
    }
    /// Called while the sole writer retains exclusive sink readiness. Keep this
    /// exact future owned through cancellation; Drop retains uncertainty.
    ///
    /// # Errors
    /// Returns uncertain when authorization or the retained outcome cannot be
    /// safely disclosed. Other refusals replace the retained outcome with an error.
    pub async fn authorize(&mut self) -> Result<()> {
        let mut guard = Guard::new(self.owner.clone());
        let result = if let Some(hold) = &self.hold {
            hold.authorize().await.map_err(map_error)
        } else {
            self.owner.authorize().await
        };
        if let Err(error) = result {
            self.outcome = Err(error);
        }
        guard.complete = true;
        if matches!(self.outcome, Err(SessionError::Uncertain)) {
            self.owner.uncertain();
            return Err(SessionError::Uncertain);
        }
        self.authorized = true;
        Ok(())
    }
    /// Materialize only inside the already admitted frame reservation.
    ///
    /// # Errors
    /// Rejects an invalid captured ID, missing authorization, or an envelope that
    /// exceeds the admitted serialization budget.
    pub fn frame(&self) -> Result<String> {
        let id = &self.id;
        validate_id(id)?;
        if !self.authorized {
            return Err(SessionError::Unavailable);
        }
        match &self.outcome {
            Ok(result) => wire::bounded(
                &serde_json::json!({"jsonrpc":"2.0","id":id,"result":result}),
                self.wire_limit,
            ),
            Err(error) => wire::error(*error, id),
        }
    }
    /// The typed transport writer must exclusively own sink readiness. Context
    /// then operation locks span the synchronous local check and sink acceptance.
    /// No user callback, SQL, readiness wait, or await occurs in this section.
    ///
    /// # Errors
    /// Rejects stale context/operation authority, missing authorization, duplicate
    /// writes, invalid frame budgets, or uncertain sink acceptance/registry state.
    pub fn enqueue(&mut self, writer: &mut impl SourceWriter) -> Result<()> {
        if !self.authorized || self.writing {
            return Err(SessionError::Unavailable);
        }
        let frame = self.frame()?;
        let owner = self.owner.clone();
        let principal = &owner.caller.principal_id().ok_or(SessionError::Identity)?.0;
        let mut failure = Guard::new(owner.clone());
        let result = owner
            .identity
            .while_current(principal, || {
                let mut directory = owner
                    .registry
                    .state
                    .lock()
                    .map_err(|_| SessionError::Uncertain)?;
                let entry = directory
                    .get_mut(owner.slot)
                    .ok_or(SessionError::Uncertain)?;
                let fields = entry.metadata.fields()?;
                if fields.state >= 3
                    || fields.epoch != owner.identity.epoch
                    || fields.outstanding != 1
                    || fields.sequence != self.sequence
                    || owner.closing.load(Ordering::Acquire)
                    || time::OffsetDateTime::now_utc().unix_timestamp_nanos()
                        >= fields.source_expiry
                    || self.hold.as_ref().is_some_and(|h| !h.locally_current())
                {
                    return Err(SessionError::Unavailable);
                }
                // Charge the attempted local write before invoking the trusted sink.
                // Acceptance is recorded only after start_send actually returns Ok.
                entry.metadata.progress(
                    fields.state,
                    fields.reason,
                    (fields.flags & !128) | 256,
                    fields.sequence,
                    1,
                )?;
                if let Ok(()) = writer.start_send(frame) {
                    self.writing = true;
                    Ok(())
                } else {
                    let fields = entry.metadata.fields()?;
                    entry.metadata.progress(
                        4,
                        fields.reason,
                        fields.flags,
                        fields.sequence,
                        fields.outstanding,
                    )?;
                    Err(SessionError::Uncertain)
                }
            })
            .unwrap_or(Err(SessionError::Unavailable));
        // A normal refused enqueue has performed no pending IO. Panic/poison or
        // failed sink acceptance remains uncertain, never a cleanup receipt.
        failure.complete = !matches!(result, Err(SessionError::Uncertain));
        result
    }
    /// Only after actual local flush completion. Consumer ownership persists until
    /// the next legal read or close; flush does not establish remote consumption.
    ///
    /// # Errors
    /// Rejects an unaccepted write, duplicate delivery retirement, or uncertain
    /// registry/consumer ownership.
    pub fn flushed(mut self) -> Result<()> {
        if !self.writing {
            return Err(SessionError::Uncertain);
        }
        let mut state = self
            .owner
            .state
            .lock()
            .map_err(|_| SessionError::Uncertain)?;
        if state.delivered.is_some() {
            return Err(SessionError::Uncertain);
        }
        let consumer = self.hold.is_some();
        self.owner
            .registry
            .delivered(self.owner.slot, self.sequence, consumer)?;
        state.delivered = self.hold.take();
        drop(state);
        self.retired = true;
        self.owner.changed.notify_waiters();
        if self.outcome.is_err() {
            self.owner.request_close(Reason::Closed);
        }
        Ok(())
    }
    /// Suppress an outcome only after read/auth and any local socket continuation
    /// have actually retired. Unknown write settlement must call `uncertain` instead.
    ///
    /// # Errors
    /// Returns the source-hold retirement or registry ownership error without
    /// manufacturing a settled receipt.
    pub fn discard(mut self) -> Result<()> {
        self.owner.request_close(Reason::Closed);
        if let Some(hold) = self.hold.take() {
            hold.retire().map_err(map_error)?;
        }
        drop(std::mem::replace(
            &mut self.outcome,
            Err(SessionError::Unavailable),
        ));
        self.owner
            .registry
            .delivered(self.owner.slot, self.sequence, false)?;
        self.retired = true;
        self.owner.changed.notify_waiters();
        Ok(())
    }
    pub fn uncertain(mut self) {
        self.owner.uncertain();
        self.retired = true;
    }
}
impl Drop for Delivery {
    fn drop(&mut self) {
        if !self.retired {
            self.owner.uncertain();
        }
    }
}

/// Owns even an unpolled receiver. Cancellation transfers this exact receiver to
/// one retained drain; it never drops a still-running read to claim completion.
pub(super) struct AwaitDelivery {
    owner: Arc<Owner>,
    receiver: Option<tokio::sync::oneshot::Receiver<Delivery>>,
}
impl AwaitDelivery {
    pub(super) fn new(
        owner: Arc<Owner>,
        receiver: tokio::sync::oneshot::Receiver<Delivery>,
    ) -> Self {
        Self {
            owner,
            receiver: Some(receiver),
        }
    }
}
impl std::future::Future for AwaitDelivery {
    type Output = Result<Delivery>;
    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let Some(receiver) = self.receiver.as_mut() else {
            return std::task::Poll::Ready(Err(SessionError::Unavailable));
        };
        match std::pin::Pin::new(receiver).poll(cx) {
            std::task::Poll::Pending => std::task::Poll::Pending,
            std::task::Poll::Ready(result) => {
                self.receiver.take();
                std::task::Poll::Ready(result.map_err(|_| {
                    self.owner.uncertain();
                    SessionError::Uncertain
                }))
            }
        }
    }
}
impl Drop for AwaitDelivery {
    fn drop(&mut self) {
        if let Some(receiver) = self.receiver.take() {
            self.owner.request_close(Reason::Closed);
            let owner = self.owner.clone();
            let guard = Guard::new(owner.clone());
            if let Ok(runtime) = tokio::runtime::Handle::try_current() {
                runtime.spawn(async move {
                    let mut guard = guard;
                    match receiver.await {
                        Ok(delivery) => {
                            if delivery.discard().is_ok() {
                                guard.complete = true;
                            } else {
                                owner.uncertain();
                            }
                        }
                        Err(_) => owner.uncertain(),
                    }
                });
            }
        }
    }
}

/// Narrow trusted transport adapter, not a wire-supplied callback. Implementations
/// must only call their exclusively owned sink `start_send`, must not reenter any
/// Services/context/operation API, and must not block or poll readiness here.
pub trait SourceWriter {
    /// Attempt synchronous acceptance by the exclusively ready local sink.
    ///
    /// # Errors
    /// Returns an erased failure if acceptance is not known to have succeeded.
    /// Every such failure retains uncertain write ownership.
    #[expect(
        clippy::result_unit_err,
        reason = "Transport details are intentionally erased; every failure retains uncertain ownership."
    )]
    fn start_send(&mut self, frame: String) -> std::result::Result<(), ()>;
}
