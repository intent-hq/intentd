//! Prepared source-only session; no router, renderer or artifact storage authority.
use crate::{Error, Result, Services};
use intent_core::{
    note_page::{NotePageError, NotePageRequest},
    Caller, WorkspaceId,
};
use intent_store::{ArtifactSourceGrant, CanonicalSourceBinding};
use serde_json::Value;
use std::{
    future::Future,
    sync::{Arc, Mutex},
};
use tokio::sync::{oneshot, Notify, OwnedSemaphorePermit};

mod closure;
mod delivery;
pub(crate) use delivery::DeliveryHold;
pub(crate) mod ownership;
use closure::Step;
use ownership::{OpenGuard, OwnedRead, Retained, WorkGuard};

fn uncertain() -> Error {
    Error::Internal("Source session quarantined: work settlement unknown".into())
}

fn expired() -> Error {
    Error::NotePage(NotePageError::Expired)
}
fn invalid() -> Error {
    Error::NotePage(NotePageError::CursorInvalid)
}
fn budget() -> Error {
    Error::NotePage(NotePageError::Budget)
}

#[expect(clippy::struct_excessive_bools)] // Revocation, delivery admission, IO activity and uncertain retirement are separate facts.
struct State {
    closed: bool,
    busy: bool,
    in_flight: bool,
    delivery: bool,
    quarantined: bool,
    step: Step,
    grant: Option<ArtifactSourceGrant>,
}

struct Inner {
    services: Services,
    caller: Caller,
    principal: String,
    binding: CanonicalSourceBinding,
    deadline: time::OffsetDateTime,
    state: Mutex<State>,
    settled: Notify,
    // Retained by session, pending work and unconsumed result ownership. Closing
    // does not refund this slot while any of those owners remains alive.
    retained: Arc<Retained>,
    runtime: tokio::runtime::Handle,
    #[cfg(test)]
    park: Mutex<Option<Arc<tests::ReadPark>>>,
    #[cfg(test)]
    now: Mutex<Option<time::OffsetDateTime>>,
    #[cfg(test)]
    fail_worker: Notify,
    #[cfg(test)]
    fail_adoption: std::sync::atomic::AtomicBool,
}

impl Inner {
    #[cfg_attr(not(test), expect(clippy::unused_self))] // Tests override this session's clock without changing production deadlines.
    fn now(&self) -> time::OffsetDateTime {
        #[cfg(test)]
        if let Some(now) = *self.now.lock().expect("source test clock") {
            return now;
        }
        time::OffsetDateTime::now_utc()
    }
    fn revoke(&self) {
        let mut state = self.state.lock().expect("source state");
        state.closed = true;
        if !state.in_flight && !state.delivery && !state.quarantined {
            state.grant.take();
            self.retained
                .release(&self.services.canonical_source_owners);
        }
    }
    fn quarantine(&self) {
        let mut state = self.state.lock().expect("source state");
        state.closed = true;
        if state.in_flight || state.delivery {
            self.retained
                .uncertain
                .store(true, std::sync::atomic::Ordering::Release);
            state.quarantined = true;
            // No assertion of SQL settlement or refund on abnormal exit.
        } else if !state.quarantined {
            // Delivery-only loss after proven work settlement is not new IO debt.
            state.grant.take();
            self.retained
                .release(&self.services.canonical_source_owners);
        }
        drop(state);
        self.settled.notify_waiters();
    }
    fn observe<T>(&self, outcome: &Result<T>) {
        // Conservative internal boundary: database/cleanup failures are not
        // retirement receipts. Domain denial/staleness after acknowledged cleanup
        // remains a normal result. No public error or wire variant is added.
        if matches!(outcome, Err(Error::Internal(_))) {
            self.quarantine();
        }
    }
    fn finish_work(&self) {
        let mut state = self.state.lock().expect("source state");
        if !state.quarantined {
            state.in_flight = false;
            if state.closed && !state.delivery {
                state.grant.take();
                self.retained
                    .release(&self.services.canonical_source_owners);
            }
        }
        drop(state);
        self.settled.notify_waiters();
    }
    fn current(&self) -> Result<()> {
        if self.state.lock().expect("source state").closed || self.now() >= self.deadline {
            return Err(expired());
        }
        Ok(())
    }
    async fn member(&self) -> Result<()> {
        let outcome = intent_core::with_caller(
            self.caller.clone(),
            self.services
                .require_member(&WorkspaceId::from(self.binding.scope.workspace_id.clone())),
        )
        .await;
        self.observe(&outcome);
        outcome
    }
    async fn source(&self) -> Result<ArtifactSourceGrant> {
        let outcome = self
            .services
            .store
            .authorize_canonical_source(
                &self.binding.scope.workspace_id,
                &self.principal,
                &self.binding,
            )
            .await;
        self.observe(&outcome);
        outcome
    }
    async fn perform(
        &self,
        request: NotePageRequest,
        rpc_id: Value,
        step: Step,
    ) -> Result<(Value, Step)> {
        self.current()?;
        self.member().await?;
        let before = self.source().await;
        self.member().await?;
        let _before = before?;
        self.current()?;
        #[cfg(test)]
        self.park(false).await;
        let read = self.services.store.read_note_page_settled(
            &self.binding.scope.workspace_id,
            &self.binding.scope.note_id,
            &self.principal,
            request,
            &rpc_id,
        );
        #[cfg(test)]
        let outcome = tokio::select! {
            result = read => result,
            () = self.fail_worker.notified() => panic!("injected source worker failure"),
        };
        #[cfg(not(test))]
        let outcome = read.await;
        self.observe(&outcome);
        #[cfg(test)]
        self.park(true).await;
        // Keep both outcomes until captured authorization has been rechecked.
        let after = self.source().await;
        self.member().await?;
        let _after = after?;
        self.current()?;
        let page = outcome?;
        let next = step.advance(&page, &self.binding)?;
        Ok((page, next))
    }
    #[cfg(test)]
    async fn park(&self, after: bool) {
        let park = self.park.lock().unwrap().clone();
        if let Some(park) = park {
            if park.after == after {
                park.entered.notify_one();
                park.release.notified().await;
            }
        }
    }
}

/// Non-clonable in-process source owner, never a serialized or renderer grant.
pub struct CanonicalSourceSession {
    inner: Arc<Inner>,
}

impl Drop for CanonicalSourceSession {
    fn drop(&mut self) {
        self.inner.revoke();
    }
}

impl CanonicalSourceSession {
    /// Read only the next admitted descriptor or exact code-value fragment.
    /// The owned task runs to settlement even if the returned future is dropped.
    ///
    /// # Errors
    /// Rejects closed/expired sessions, concurrent reads, unrelated or repeated
    /// continuations, revoked membership/source and invalid page budgets.
    ///
    /// # Panics
    /// Panics if an internal state lock was poisoned.
    pub fn read(
        &self,
        request: NotePageRequest,
        rpc_id: Value,
    ) -> Result<impl Future<Output = Result<Value>> + Send + 'static> {
        self.read_owned(request, rpc_id, false)
    }

    fn read_owned(
        &self,
        request: NotePageRequest,
        rpc_id: Value,
        delivery: bool,
    ) -> Result<impl Future<Output = Result<Value>> + Send + 'static> {
        self.inner.current()?;
        let step = {
            let mut state = self.inner.state.lock().expect("source state");
            if state.closed {
                return Err(expired());
            }
            if state.busy || (state.delivery && !delivery) {
                return Err(budget());
            }
            state.step.check(&request)?;
            let wire = request.max_wire_bytes.ok_or_else(budget)?;
            if serde_json::to_vec(&rpc_id).map_err(|_| invalid())?.len() > wire {
                return Err(budget());
            }
            state.busy = true;
            state.in_flight = true;
            state.step.clone()
        };
        let (tx, rx) = oneshot::channel();
        let inner = self.inner.clone();
        let mut guard = WorkGuard::new(inner.clone());
        let worker = self.inner.runtime.spawn(async move {
            let outcome = inner.perform(request, rpc_id, step).await;
            guard.finish();
            outcome
        });
        let inner = self.inner.clone();
        // Supervise the worker explicitly. The pre-registered retained owner and
        // guard also cover supervisor/worker cancellation during runtime shutdown.
        self.inner.runtime.spawn(async move {
            let outcome = if let Ok(outcome) = worker.await {
                outcome
            } else {
                inner.quarantine();
                Err(uncertain())
            };
            let _ = tx.send(outcome);
        });
        let inner = self.inner.clone();
        Ok(OwnedRead::new(inner.clone(), async move {
            let outcome = rx.await.map_err(|_| uncertain())?;
            // Atomic with close: never start final authorization after close has
            // observed no IO and released this session's pin.
            {
                let mut state = inner.state.lock().expect("source state");
                if state.quarantined {
                    return Err(uncertain());
                }
                if state.closed || inner.now() >= inner.deadline {
                    return Err(expired());
                }
                state.in_flight = true;
            }
            let mut guard = WorkGuard::new(inner.clone());
            #[cfg(test)]
            assert!(
                !inner
                    .fail_adoption
                    .load(std::sync::atomic::Ordering::Acquire),
                "injected adoption failure"
            );
            let result = async {
                inner.member().await?;
                let source = inner.source().await;
                inner.member().await?;
                let _source = source?;
                inner.current()?;
                // Revalidate BEFORE propagating either stored outcome arm.
                outcome
            }
            .await;
            guard.finish();
            let (page, next) = result?;
            // IO is settled, but busy remains held until this atomic adoption.
            // A concurrent close wins before any new read can be admitted.
            let mut state = inner.state.lock().expect("source state");
            if state.closed || inner.now() >= inner.deadline {
                return Err(expired());
            }
            state.step = next;
            state.busy = false;
            Ok(page)
        }))
    }

    /// Revoke synchronously, then await this session's owned read settlement.
    /// Dropping the wait does not cancel that read. Other snapshots are untouched.
    ///
    /// # Errors
    /// Reports quarantined ownership when worker retirement is uncertain; its
    /// existing pin and admission remain retained rather than refunded.
    ///
    /// # Panics
    /// Panics if an internal state lock was poisoned.
    pub fn close(&self) -> impl Future<Output = Result<()>> + Send + 'static {
        self.inner.revoke();
        let inner = self.inner.clone();
        async move {
            loop {
                let notified = inner.settled.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                {
                    let state = inner.state.lock().expect("source state");
                    if state.quarantined {
                        return Err(uncertain());
                    }
                    if !state.in_flight && !state.delivery {
                        return Ok(());
                    }
                }
                notified.await;
            }
        }
    }
}

impl Services {
    /// Open a source-only owner using real indexed binding and captured membership.
    /// No profile/font/storage authority or artifact arena is involved.
    ///
    /// # Errors
    /// Rejects missing/revoked callers, exhausted session admission and malformed,
    /// mismatched, stale or expired canonical source authority.
    ///
    /// # Panics
    /// Requires an active `Tokio` runtime and unpoisoned ownership locks.
    pub async fn open_canonical_source_session(
        &self,
        binding: CanonicalSourceBinding,
    ) -> Result<CanonicalSourceSession> {
        let caller = intent_core::current_caller()
            .ok_or_else(|| Error::Forbidden("Caller required".into()))?;
        let principal = match &caller {
            Caller::Wire { principal_id, .. } => format!("principal:{}", principal_id.0),
            Caller::Agent { agent_id } => format!("agent:{}", agent_id.0),
            Caller::Daemon => "daemon".into(),
        };
        let permit = self
            .canonical_source_admission
            .clone()
            .try_acquire_owned()
            .map_err(|_| budget())?;
        let retained = Retained::register(&self.canonical_source_owners, permit);
        let mut guard = OpenGuard {
            owner: retained.clone(),
            registry: self.canonical_source_owners.clone(),
            completed: false,
        };
        let services = self.clone();
        let runtime = tokio::runtime::Handle::current();
        let (tx, rx) = oneshot::channel();
        // Cancellation of open cannot detach SQL from its admission/pin owner.
        tokio::spawn(async move {
            let workspace = WorkspaceId::from(binding.scope.workspace_id.clone());
            let result = async {
                let membership =
                    intent_core::with_caller(caller.clone(), services.require_member(&workspace))
                        .await;
                if matches!(&membership, Err(Error::Internal(_))) {
                    retained
                        .uncertain
                        .store(true, std::sync::atomic::Ordering::Release);
                }
                membership?;
                *retained.hold.lock().expect("retained snapshot") = Some(
                    services
                        .store
                        .hold_canonical_source(&workspace.0, &principal, &binding)?,
                );
                #[cfg(test)]
                {
                    let control = services.canonical_source_open_test.lock().unwrap().clone();
                    if let Some(control) = control {
                        control.entered.notify_one();
                        control.release.notified().await;
                        assert!(
                            !control.fail.load(std::sync::atomic::Ordering::Acquire),
                            "injected open failure"
                        );
                    }
                }
                let result = services
                    .store
                    .authorize_canonical_source(&workspace.0, &principal, &binding)
                    .await;
                if matches!(&result, Err(Error::Internal(_))) {
                    retained
                        .uncertain
                        .store(true, std::sync::atomic::Ordering::Release);
                }
                if let Ok(grant) = &result {
                    *retained.grant.lock().expect("retained source") = Some(grant.clone());
                }
                let membership =
                    intent_core::with_caller(caller.clone(), services.require_member(&workspace))
                        .await;
                if matches!(&membership, Err(Error::Internal(_))) {
                    retained
                        .uncertain
                        .store(true, std::sync::atomic::Ordering::Release);
                }
                membership?;
                let grant = result?;
                let deadline = intent_core::parse_iso(&grant.expires_at).ok_or_else(invalid)?;
                if time::OffsetDateTime::now_utc() >= deadline {
                    return Err(expired());
                }
                let step = Step::Owner(binding.owner_ref.clone());
                Ok(CanonicalSourceSession {
                    inner: Arc::new(Inner {
                        services,
                        caller,
                        principal,
                        binding,
                        deadline,
                        state: Mutex::new(State {
                            closed: false,
                            busy: false,
                            in_flight: false,
                            delivery: false,
                            quarantined: false,
                            step,
                            grant: Some(grant),
                        }),
                        settled: Notify::new(),
                        retained,
                        runtime,
                        #[cfg(test)]
                        park: Mutex::new(None),
                        #[cfg(test)]
                        now: Mutex::new(None),
                        #[cfg(test)]
                        fail_worker: Notify::new(),
                        #[cfg(test)]
                        fail_adoption: std::sync::atomic::AtomicBool::new(false),
                    }),
                })
            }
            .await;
            guard.finish(result.is_ok());
            let _ = tx.send(result);
        });
        rx.await.map_err(|_| uncertain())?
    }
}

#[cfg(test)]
mod tests;
