//! One original allocation obligation, retained before dispatch and across
//! confirmation. Local closure and remote join are distinct facts.
use std::sync::Arc;

use intent_acp::{
    callback_registration::CallbackClient, review_monitoring::ReviewMonitoringContext,
};
use tokio::sync::watch;

use super::{AdmissionError, AdmissionResult};

#[derive(Clone, Default)]
struct State {
    session: Option<String>,
    retired: bool,
    producer_finished: bool,
    joined: Option<bool>,
}

pub(crate) struct NativeAllocation {
    callbacks: std::sync::Mutex<Option<CallbackClient>>,
    context: ReviewMonitoringContext,
    state: watch::Sender<State>,
}

impl NativeAllocation {
    pub(crate) fn new(callbacks: CallbackClient, context: ReviewMonitoringContext) -> Arc<Self> {
        Arc::new(Self {
            callbacks: std::sync::Mutex::new(Some(callbacks)),
            context,
            state: watch::channel(State::default()).0,
        })
    }

    /// A loaded attempt already has an immutable original destination. Retain
    /// its exact retirement selector before preflight dispatch; this is not a
    /// response receipt or evidence that an allocation exists or has joined.
    pub(crate) fn new_loaded(
        callbacks: CallbackClient,
        context: ReviewMonitoringContext,
        session: &str,
    ) -> Arc<Self> {
        let native = Self::new(callbacks, context);
        native
            .state
            .send_modify(|state| state.session = Some(session.to_owned()));
        native
    }

    pub(crate) fn context(&self) -> &ReviewMonitoringContext {
        &self.context
    }
    pub(crate) fn is_retired(&self) -> bool {
        self.state.borrow().retired
    }
    pub(crate) fn is_joined(&self) -> bool {
        self.state.borrow().joined == Some(true)
    }

    /// Must be called only with the actual original response, before its use.
    pub(crate) fn bind_response(&self, session: &str) -> AdmissionResult<()> {
        let mut valid = false;
        self.state.send_modify(|state| {
            if state.session.is_none() && !state.producer_finished {
                state.session = Some(session.to_owned());
                valid = true;
            }
        });
        if valid {
            Ok(())
        } else {
            Err(AdmissionError::Retired)
        }
    }

    /// Synchronous invalidation only: no remote call or async task starts here.
    /// A pre-registered worker drives the join after observing this signal.
    pub(crate) fn retire(&self) {
        if let Some(callbacks) = self
            .callbacks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
        {
            callbacks.retire_review_attempt(&self.context);
        }
        self.state.send_modify(|state| state.retired = true);
    }

    pub(crate) fn producer_finished(&self) {
        self.state
            .send_modify(|state| state.producer_finished = true);
    }

    /// Registered before dispatch; its lifetime does not depend on any waiter.
    pub(crate) async fn drive(self: Arc<Self>) {
        let mut state = self.state.subscribe();
        loop {
            let snapshot = state.borrow_and_update().clone();
            if snapshot.retired {
                if let Some(session) = snapshot.session {
                    let callbacks = self
                        .callbacks
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .clone();
                    let joined = match callbacks.as_ref() {
                        Some(callbacks) => callbacks
                            .retire_review_context(&session, &self.context)
                            .await
                            .is_ok(),
                        None => false,
                    };
                    drop(callbacks);
                    if joined {
                        // Existing waiters retain the exact context and terminal
                        // watch result, not the completed connection/worker tree.
                        let completed = self
                            .callbacks
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .take();
                        drop(completed);
                    }
                    self.state.send_modify(|state| state.joined = Some(joined));
                    return;
                }
                if snapshot.producer_finished {
                    // A lost response never proves that no native allocation
                    // exists. Keep that uncertainty distinct from a joined peer.
                    self.state.send_modify(|state| state.joined = Some(false));
                    return;
                }
            }
            if state.changed().await.is_err() {
                return;
            }
        }
    }

    pub(crate) async fn join(&self) -> AdmissionResult<()> {
        self.retire();
        let mut state = self.state.subscribe();
        loop {
            match state.borrow_and_update().joined {
                Some(true) => return Ok(()),
                Some(false) => return Err(AdmissionError::Unavailable),
                None => {}
            }
            state
                .changed()
                .await
                .map_err(|_| AdmissionError::Unavailable)?;
        }
    }
}

/// Always reconcile a producer's terminal/lost outcome, including panic or
/// cancellation, without fabricating a native join from its disappearance.
pub(crate) struct NativeProducer(pub(crate) Arc<NativeAllocation>);
impl Drop for NativeProducer {
    fn drop(&mut self) {
        self.0.producer_finished();
    }
}

/// Exact allocations registered while the owning source's admission fences are
/// held. This set is invalidation/completion bookkeeping, never authority.
#[derive(Default)]
pub(crate) struct NativeContextRetirements(std::sync::Mutex<Vec<Arc<NativeAllocation>>>);
impl NativeContextRetirements {
    pub(crate) fn retain(&self, native: Arc<NativeAllocation>) -> AdmissionResult<()> {
        let mut originals = self.0.lock().map_err(|_| AdmissionError::Retired)?;
        originals.retain(|original| !original.is_joined());
        if native.is_retired() {
            return Err(AdmissionError::Retired);
        }
        if !originals.iter().any(|old| Arc::ptr_eq(old, &native)) {
            originals.push(native);
        }
        Ok(())
    }
    pub(crate) fn retire(&self) {
        let originals = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for original in originals.iter() {
            original.retire();
        }
    }
    pub(crate) fn settled(&self) -> bool {
        self.0.lock().is_ok_and(|originals| {
            originals.iter().all(|original| {
                !original.is_retired() || original.state.borrow().joined == Some(true)
            })
        })
    }
    pub(crate) async fn join(&self) -> AdmissionResult<()> {
        let originals = self
            .0
            .lock()
            .map_err(|_| AdmissionError::Retired)?
            .iter()
            .filter(|native| native.is_retired())
            .cloned()
            .collect::<Vec<_>>();
        for original in originals {
            original.join().await?;
        }
        self.0
            .lock()
            .map_err(|_| AdmissionError::Retired)?
            .retain(|original| !original.is_joined());
        Ok(())
    }
}
