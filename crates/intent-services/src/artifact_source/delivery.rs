//! One delivery retention owner, distinct from actual SQL activity and socket writes.
use super::{
    budget, expired, uncertain, CanonicalSourceSession, Inner, NotePageRequest, Result, Value,
    WorkGuard,
};
use std::{future::Future, pin::Pin, sync::Arc};

pub(crate) struct DeliveryRead {
    pub(crate) hold: DeliveryHold,
    pub(crate) read: Pin<Box<dyn Future<Output = Result<Value>> + Send + 'static>>,
}
pub(crate) struct DeliveryHold {
    inner: Arc<Inner>,
    retired: bool,
}
impl CanonicalSourceSession {
    pub(crate) fn hold_for_delivery(&self) -> Result<DeliveryHold> {
        self.inner.current()?;
        let mut state = self.inner.state.lock().expect("source state");
        if state.closed || state.delivery || state.busy || state.in_flight {
            return Err(budget());
        }
        state.delivery = true;
        Ok(DeliveryHold {
            inner: self.inner.clone(),
            retired: false,
        })
    }

    pub(crate) fn validate_delivery_request(&self, request: &NotePageRequest) -> Result<()> {
        self.inner.current()?;
        let state = self.inner.state.lock().expect("source state");
        if state.busy || state.in_flight {
            return Err(budget());
        }
        state.step.check(request)
    }
    pub(crate) fn read_for_delivery(
        &self,
        request: NotePageRequest,
        id: Value,
    ) -> Result<DeliveryRead> {
        self.validate_delivery_request(&request)?;
        let hold = self.hold_for_delivery()?;
        let read: Pin<Box<dyn Future<Output = Result<Value>> + Send + 'static>> =
            match self.read_owned(request, id, true) {
                Ok(read) => Box::pin(read),
                Err(error) => Box::pin(async move { Err(error) }),
            };
        Ok(DeliveryRead { hold, read })
    }
    pub(crate) fn original_expiry(&self) -> Result<String> {
        self.inner
            .state
            .lock()
            .expect("source state")
            .grant
            .as_ref()
            .map(|g| g.expires_at.clone())
            .ok_or_else(expired)
    }
}
impl DeliveryHold {
    /// Caller retains this exact continuation across cancellation; unwind quarantines.
    pub(crate) async fn authorize(&self) -> Result<()> {
        {
            let mut state = self.inner.state.lock().expect("source state");
            if state.quarantined {
                return Err(uncertain());
            }
            if !state.delivery || state.in_flight {
                return Err(budget());
            }
            // Even a stored error needs captured membership checked. Close cannot
            // retire the pin while this delivery flag owns the final check.
            state.in_flight = true;
        }
        let mut guard = WorkGuard::new(self.inner.clone());
        let result = async {
            self.inner.member().await?;
            let source = self.inner.source().await;
            self.inner.member().await?;
            let _source = source?;
            if self.inner.now() >= self.inner.deadline {
                return Err(expired());
            }
            Ok(())
        }
        .await;
        guard.finish();
        result
    }
    pub(crate) fn locally_current(&self) -> bool {
        self.inner.now() < self.inner.deadline
            && !self.inner.state.lock().expect("source state").quarantined
    }
    /// Only after owned auth/read/write retirement and consumption/discard.
    pub(crate) fn retire(mut self) -> Result<()> {
        let mut state = self.inner.state.lock().expect("source state");
        if state.in_flight || state.quarantined {
            return Err(uncertain());
        }
        state.delivery = false;
        if state.closed {
            state.grant.take();
            self.inner
                .retained
                .release(&self.inner.services.canonical_source_owners);
        }
        self.retired = true;
        drop(state);
        self.inner.settled.notify_waiters();
        Ok(())
    }
}
impl Drop for DeliveryHold {
    fn drop(&mut self) {
        if !self.retired {
            self.inner.quarantine();
        }
    }
}
