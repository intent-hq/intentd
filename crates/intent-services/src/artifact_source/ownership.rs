//! In-process retained owners. Unknown task retirement quarantines the SAME
//! admitted slot; no OS/`SQLite` settlement or physical allocation is inferred.
#[cfg(test)]
use super::Notify;
use super::{
    uncertain, Arc, ArtifactSourceGrant, Future, Inner, Mutex, OwnedSemaphorePermit, Result, Value,
};
use std::{
    pin::Pin,
    sync::atomic::{AtomicBool, Ordering},
    task::{Context, Poll},
};

pub(crate) type Registry = Arc<Mutex<Vec<Arc<Retained>>>>;

pub(crate) struct Retained {
    pub(super) grant: Mutex<Option<ArtifactSourceGrant>>,
    pub(super) hold: Mutex<Option<intent_store::CanonicalSourceHold>>,
    pub(super) uncertain: AtomicBool,
    // Registry, session, and outstanding result share one permit, not new credit.
    _admission: OwnedSemaphorePermit,
}
impl Retained {
    pub(super) fn register(registry: &Registry, permit: OwnedSemaphorePermit) -> Arc<Self> {
        let owner = Arc::new(Self {
            grant: Mutex::new(None),
            hold: Mutex::new(None),
            uncertain: AtomicBool::new(false),
            _admission: permit,
        });
        // Admission precedes registration; count cannot exceed admitted slots.
        registry
            .lock()
            .expect("source registry")
            .push(owner.clone());
        owner
    }
    pub(super) fn release(&self, registry: &Registry) {
        if !self.uncertain.load(Ordering::Acquire) {
            self.grant.lock().expect("retained source").take();
            self.hold.lock().expect("retained snapshot").take();
            registry
                .lock()
                .expect("source registry")
                .retain(|v| !std::ptr::eq(v.as_ref(), self));
        }
    }
}

// Created BEFORE launching open, so abort-before-first-poll also retains charge.
pub(super) struct OpenGuard {
    pub(super) owner: Arc<Retained>,
    pub(super) registry: Registry,
    pub(super) completed: bool,
}
impl Drop for OpenGuard {
    fn drop(&mut self) {
        if !self.completed {
            self.owner.uncertain.store(true, Ordering::Release);
        }
    }
}
impl OpenGuard {
    pub(super) fn finish(&mut self, success: bool) {
        self.completed = true;
        if !success {
            self.owner.release(&self.registry);
        }
    }
}

pub(super) struct WorkGuard {
    inner: Arc<Inner>,
    completed: bool,
}
impl WorkGuard {
    pub(super) fn new(inner: Arc<Inner>) -> Self {
        Self {
            inner,
            completed: false,
        }
    }
    pub(super) fn finish(&mut self) {
        self.inner.finish_work();
        self.completed = true;
    }
}
impl Drop for WorkGuard {
    fn drop(&mut self) {
        if !self.completed {
            self.inner.quarantine();
        }
    }
}

type Continuation = Pin<Box<dyn Future<Output = Result<Value>> + Send + 'static>>;

/// Owns the inline adoption continuation before its first poll. Cancellation
/// revokes synchronously, then transfers the exact pinned future once to the
/// captured runtime. Runtime shutdown is an uncertain outcome, not settlement.
pub(super) struct OwnedRead {
    inner: Arc<Inner>,
    continuation: Option<Continuation>,
}
impl OwnedRead {
    pub(super) fn new(
        inner: Arc<Inner>,
        continuation: impl Future<Output = Result<Value>> + Send + 'static,
    ) -> Self {
        Self {
            inner,
            continuation: Some(Box::pin(continuation)),
        }
    }
}
impl Future for OwnedRead {
    type Output = Result<Value>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.continuation
                .as_mut()
                .expect("source continuation already consumed")
                .as_mut()
                .poll(cx)
        }));
        match result {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(outcome)) => {
                self.continuation.take();
                if outcome.is_err() {
                    self.inner.revoke();
                }
                Poll::Ready(outcome)
            }
            Err(_) => {
                // Retained registry owner already exists before any unwind.
                self.inner.quarantine();
                self.continuation.take();
                Poll::Ready(Err(uncertain()))
            }
        }
    }
}
impl Drop for OwnedRead {
    fn drop(&mut self) {
        if let Some(continuation) = self.continuation.take() {
            self.inner.revoke();
            let guard = TransferGuard {
                inner: self.inner.clone(),
                completed: false,
            };
            self.inner.runtime.spawn(async move {
                let mut guard = guard;
                let _ = continuation.await;
                // Normal return from the preserved continuation is settlement;
                // its phase guard already finalized only the phase it owned.
                guard.completed = true;
            });
        }
    }
}
struct TransferGuard {
    inner: Arc<Inner>,
    completed: bool,
}
impl Drop for TransferGuard {
    fn drop(&mut self) {
        if !self.completed {
            self.inner.quarantine();
        }
    }
}

#[cfg(test)]
pub(crate) type OpenControl = Arc<Mutex<Option<Arc<OpenTest>>>>;
#[cfg(test)]
pub(crate) struct OpenTest {
    pub(crate) entered: Notify,
    pub(crate) release: Notify,
    pub(crate) fail: AtomicBool,
}
