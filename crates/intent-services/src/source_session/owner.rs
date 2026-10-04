//! Strong work ownership; the root directory contains only Weak references.
use super::{
    delivery::{AwaitDelivery, Delivery},
    map_error,
    registry::{Registry, Slot},
};
use crate::{
    artifact_source::{CanonicalSourceSession, DeliveryHold},
    Services,
};
use intent_core::{
    note_source_session::{Control, Reason, Result, SessionError},
    Caller, WorkspaceId,
};
use intent_store::CanonicalSourceBinding;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use tokio::sync::Notify;

pub(super) struct State {
    pub session: Option<Arc<CanonicalSourceSession>>,
    pub delivered: Option<DeliveryHold>,
    pub open_finished: bool,
}
pub(super) struct Owner {
    pub registry: Arc<Registry>,
    pub slot: Slot,
    pub services: Services,
    pub caller: Caller,
    pub identity: crate::prepared_source_bootstrap::SourceIdentity,
    pub binding: CanonicalSourceBinding,
    pub state: Mutex<State>,
    pub closing: AtomicBool,
    close_started: AtomicBool,
    pub changed: Notify,
}
impl Owner {
    pub fn new(
        registry: Arc<Registry>,
        slot: Slot,
        services: Services,
        caller: Caller,
        identity: crate::prepared_source_bootstrap::SourceIdentity,
        binding: CanonicalSourceBinding,
    ) -> Arc<Self> {
        Arc::new(Self {
            registry,
            slot,
            services,
            caller,
            identity,
            binding,
            state: Mutex::new(State {
                session: None,
                delivered: None,
                open_finished: false,
            }),
            closing: AtomicBool::new(false),
            close_started: AtomicBool::new(false),
            changed: Notify::new(),
        })
    }
    pub fn uncertain(&self) {
        self.registry.uncertain(self.slot);
        self.closing.store(true, Ordering::Release);
        self.changed.notify_waiters();
    }
    pub fn request_close(self: &Arc<Self>, reason: Reason) {
        if self.registry.close_state(self.slot, reason).is_err() {
            self.uncertain();
            return;
        }
        self.closing.store(true, Ordering::Release);
        // Registry state under its lock is the close linearization. The atomic
        // is a wake/fast refusal hint, never an earlier disclosure barrier.
        // Constructing close revokes source adoption synchronously. Its returned
        // wait is recreated by the one retained close continuation below.
        if let Ok(state) = self.state.lock() {
            if let Some(session) = &state.session {
                drop(session.close());
            }
        }
        self.changed.notify_waiters();
        if self.close_started.swap(true, Ordering::AcqRel) {
            return;
        }
        let owner = self.clone();
        let guard = Guard::new(owner.clone());
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            drop(guard);
            return;
        };
        runtime.spawn(async move {
            let mut guard = guard;
            if owner.close_owned().await.is_ok() {
                guard.complete = true;
                owner.changed.notify_waiters();
            } else {
                owner.uncertain();
            }
        });
    }
    async fn close_owned(&self) -> Result<()> {
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let mut state = self.state.lock().map_err(|_| SessionError::Uncertain)?;
                if let Some(hold) = state.delivered.take() {
                    hold.retire().map_err(map_error)?;
                    self.registry.consume(self.slot)?;
                }
                let d = self
                    .registry
                    .state
                    .lock()
                    .map_err(|_| SessionError::Uncertain)?;
                let entry = d.get(self.slot).ok_or(SessionError::Uncertain)?;
                let v = entry.metadata.fields()?;
                if v.state == 4 {
                    return Err(SessionError::Uncertain);
                }
                if state.open_finished
                    && v.outstanding == 0
                    && v.flags & 0x0380 == 0
                    && !entry.transport_pending
                {
                    break;
                }
            }
            notified.await;
        }
        let session = self
            .state
            .lock()
            .map_err(|_| SessionError::Uncertain)?
            .session
            .clone();
        if let Some(session) = session {
            session.close().await.map_err(map_error)?;
        }
        self.state
            .lock()
            .map_err(|_| SessionError::Uncertain)?
            .session
            .take();
        self.registry.settle(self.slot)
    }
    pub async fn authorize(&self) -> Result<()> {
        let workspace = WorkspaceId::from(self.binding.scope.workspace_id.clone());
        let principal = format!(
            "principal:{}",
            self.caller.principal_id().ok_or(SessionError::Identity)?.0
        );
        let result = async {
            intent_core::with_caller(
                self.caller.clone(),
                self.services.require_member(&workspace),
            )
            .await?;
            let source = self
                .services
                .store
                .authorize_canonical_source(&workspace.0, &principal, &self.binding)
                .await;
            let member = intent_core::with_caller(
                self.caller.clone(),
                self.services.require_member(&workspace),
            )
            .await;
            // Both outcomes are retained until the final captured member check.
            if matches!(&source, Err(crate::Error::Internal(_)))
                || matches!(&member, Err(crate::Error::Internal(_)))
            {
                self.uncertain();
            }
            member?;
            source?;
            Ok::<_, crate::Error>(())
        }
        .await;
        if matches!(&result, Err(crate::Error::Internal(_))) {
            self.uncertain();
        }
        result.map_err(map_error)
    }
    pub fn open(
        self: &Arc<Self>,
        id: serde_json::Value,
    ) -> impl std::future::Future<Output = Result<Delivery>> + Send + 'static {
        let owner = self.clone();
        let guard = Guard::new(owner.clone());
        let (tx, rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let mut guard = guard;
            let result = intent_core::with_caller(
                owner.caller.clone(),
                owner
                    .services
                    .open_canonical_source_session(owner.binding.clone()),
            )
            .await;
            if matches!(&result, Err(crate::Error::Internal(_))) {
                owner.uncertain();
            }
            let mut hold = None;
            let outcome = match result {
                Ok(session) => {
                    let expiry = session
                        .original_expiry()
                        .map_err(map_error)
                        .and_then(|expiry| {
                            hold = Some(session.hold_for_delivery().map_err(map_error)?);
                            Ok(expiry)
                        });
                    let session = Arc::new(session);
                    if owner.closing.load(Ordering::Acquire) {
                        drop(session.close());
                    }
                    owner.state.lock().expect("operation state").session = Some(session);
                    expiry.map(|source_expires_at| {
                        serde_json::to_value(Control::Opened {
                            operation_id: owner
                                .registry
                                .operation_id(owner.slot)
                                .expect("owned operation"),
                            daemon_incarnation: owner.registry.incarnation.clone(),
                            source_expires_at,
                        })
                        .expect("control serialization")
                    })
                }
                Err(error) => Err(map_error(error)),
            };
            owner.state.lock().expect("operation state").open_finished = true;
            owner.changed.notify_waiters();
            let delivery = Delivery::new(owner.clone(), outcome, hold, 4096, id, 0);
            guard.complete = true;
            let _ = tx.send(delivery);
        });
        AwaitDelivery::new(self.clone(), rx)
    }
}
impl Drop for Owner {
    fn drop(&mut self) {
        // Losing the last strong owner is never settlement proof. The fixed entry
        // and its signed hold remain accountable if no explicit retirement exists.
        if self
            .registry
            .close_result(self.slot)
            .is_ok_and(|c| !matches!(c, Control::Settled { .. }))
        {
            self.registry.uncertain(self.slot);
        }
    }
}
pub(super) struct Guard {
    owner: Arc<Owner>,
    pub complete: bool,
}
impl Guard {
    pub fn new(owner: Arc<Owner>) -> Self {
        Self {
            owner,
            complete: false,
        }
    }
}
impl Drop for Guard {
    fn drop(&mut self) {
        if !self.complete {
            self.owner.uncertain();
        }
    }
}
