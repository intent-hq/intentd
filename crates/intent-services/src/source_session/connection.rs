//! Authenticated bootstrap context to source-only operation ownership.
use super::{
    delivery::{AwaitDelivery, Delivery},
    map_error, metadata,
    owner::{Guard, Owner},
    registry::Registry,
};
use crate::{
    prepared_source_bootstrap::{Context, Mode, SourceIdentity},
    Services,
};
use intent_core::{
    note_source_session::{
        validate_id, Binding, Control, Operation, Read, Reason, Result, SessionError,
    },
    Caller,
};
use intent_store::CanonicalSourceBinding;
use serde_json::Value;
use std::sync::{atomic::Ordering, Arc};

/// Original actual connection. Not clonable; replacement cleanup does not obtain
/// its read owner. The transport must retain this through all owned continuations.
pub struct SourceConnection {
    services: Services,
    caller: Caller,
    identity: SourceIdentity,
    pub(super) registry: Arc<Registry>,
    pub(super) owner: Option<Arc<Owner>>,
    transport: Option<bool>,
}
impl Services {
    /// No listener activation. Requires a ready context and its actual wire caller.
    ///
    /// # Errors
    /// Rejects an invalid or already claimed context/caller binding, unavailable
    /// registry initialization, or uncertain ownership.
    pub fn prepared_source_connection(
        &self,
        context: &Context,
        caller: Caller,
    ) -> Result<SourceConnection> {
        let principal = caller.principal_id().ok_or(SessionError::Identity)?;
        if context.incarnation() != self.daemon_boot_id {
            return Err(SessionError::Identity);
        }
        let identity = context
            .source_identity(&principal.0)
            .ok_or(SessionError::Identity)?;
        if self.prepared_source_operations.get().is_none() {
            let registry = Registry::new(self.daemon_boot_id.clone())?;
            let _ = self.prepared_source_operations.set(registry);
        }
        let registry = self
            .prepared_source_operations
            .get()
            .ok_or(SessionError::Uncertain)?
            .clone();
        Ok(SourceConnection {
            services: self.clone(),
            caller,
            identity,
            registry,
            owner: None,
            transport: None,
        })
    }
}
fn binding(b: &Binding) -> CanonicalSourceBinding {
    CanonicalSourceBinding {
        scope: b.scope.clone(),
        snapshot_id: b.snapshot_id.clone(),
        source_revision: b.source_revision.clone(),
        primitive: b.primitive,
        owner_ref: b.owner_ref.clone(),
        source_ref: b.source_ref.clone(),
    }
}
impl SourceConnection {
    fn principal(&self) -> &str {
        &self.caller.principal_id().expect("captured wire caller").0
    }
    fn current(&self) -> Result<()> {
        if self.transport != Some(true) && self.identity.current(self.principal()) {
            Ok(())
        } else {
            Err(SessionError::Unavailable)
        }
    }
    /// Attach the original admitted transport before creating an operation.
    /// Its parser, socket and continuations must retire before a settled receipt.
    ///
    /// # Errors
    /// Returns unavailable if the context is no longer current, a transport was
    /// already attached, or an operation already exists.
    pub fn attach_transport(&mut self) -> Result<()> {
        self.current()?;
        if self.transport.is_some() || self.owner.is_some() {
            return Err(SessionError::Unavailable);
        }
        self.transport = Some(false);
        Ok(())
    }
    /// Called only after the original transport's owned continuations and local
    /// socket/parser have retired. This does not establish peer consumption.
    ///
    /// # Errors
    /// Rejects an absent/already retired attachment or an uncertain registry,
    /// operation state, or pending-transport record.
    pub fn transport_retired(&mut self) -> Result<()> {
        if self.transport != Some(false) {
            return Err(SessionError::Unavailable);
        }
        self.revoke();
        if let Some(owner) = &self.owner {
            let mut directory = self
                .registry
                .state
                .lock()
                .map_err(|_| SessionError::Uncertain)?;
            let entry = directory
                .get_mut(owner.slot)
                .ok_or(SessionError::Uncertain)?;
            if entry.metadata.fields()?.state != 3 || !entry.transport_pending {
                return Err(SessionError::Uncertain);
            }
            entry.transport_pending = false;
            owner.changed.notify_waiters();
        }
        self.transport = Some(true);
        Ok(())
    }
    /// Wake the original dispatcher when replacement cleanup terminalizes it.
    pub async fn revoked(&self) {
        let Some(owner) = &self.owner else {
            return std::future::pending().await;
        };
        loop {
            let notified = owner.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if owner.closing.load(Ordering::Acquire) {
                return;
            }
            notified.await;
        }
    }
    #[must_use]
    pub fn epoch(&self) -> [u8; 16] {
        self.identity.epoch
    }
    /// Admit one descriptor-bound operation before launching source authorization.
    ///
    /// # Errors
    /// Rejects invalid identities, descriptors, IDs, source references, deadlines,
    /// capacity exhaustion, or uncertain registry ownership.
    pub fn open(&mut self, op: Operation, id: Value) -> Result<Open> {
        self.current()?;
        validate_id(&id)?;
        op.validate()?;
        if self.identity.mode != Mode::Read {
            return Err(SessionError::Unavailable);
        }
        if op.descriptor.daemon_incarnation != self.registry.incarnation {
            return Err(SessionError::Identity);
        }
        let digest = metadata::digest(&op.operation_id)?;
        let mut directory = self
            .registry
            .state
            .lock()
            .map_err(|_| SessionError::Uncertain)?;
        self.registry.observe(&mut directory)?;
        directory.purge();
        if let Some(slot) = directory.find(
            self.principal(),
            &self.registry.incarnation,
            &op.descriptor.workspace_id,
            &digest,
        )? {
            let entry = directory.get(slot).ok_or(SessionError::Uncertain)?;
            if !entry.matches_descriptor(&op)? {
                return Err(SessionError::Identity);
            }
            return Ok(Open::Existing(Control::AlreadyRegistered {
                operation_id: op.operation_id,
            }));
        }
        if self.owner.is_some() {
            return Err(SessionError::Capacity);
        }
        let source = binding(&op.descriptor.binding);
        let principal = format!("principal:{}", self.principal());
        let hold = self
            .services
            .store
            .hold_canonical_source(&op.descriptor.workspace_id, &principal, &source)
            .map_err(map_error)?;
        let slot = directory.reserve(self.principal(), self.identity.epoch, &op, hold, false)?;
        let owner = Owner::new(
            self.registry.clone(),
            slot,
            self.services.clone(),
            self.caller.clone(),
            self.identity.clone(),
            source,
        );
        let entry = directory.get_mut(slot).ok_or(SessionError::Uncertain)?;
        entry.owner = Arc::downgrade(&owner);
        entry.transport_pending = self.transport == Some(false);
        self.owner = Some(owner.clone());
        drop(directory);
        Ok(Open::Pending(Box::pin(owner.open(id))))
    }
    /// Cleanup deliberately does not call `require_member`. It discloses no source
    /// and cannot rebind an old operation to this replacement connection.
    ///
    /// # Errors
    /// Rejects invalid/current-context bindings, malformed or mismatched
    /// descriptors, exhausted cancellation capacity, or uncertain registry state.
    ///
    /// # Panics
    /// Test builds panic if the injected lock-observation mutex is poisoned.
    #[expect(
        clippy::needless_pass_by_value,
        reason = "The public operation boundary takes ownership of the validated request."
    )]
    pub fn close(&self, op: Operation) -> Result<Control> {
        #[cfg(test)]
        if let Some(observe) = self.registry.close_blocked.lock().unwrap().clone() {
            self.identity.observe_current_lock(&observe);
        }
        self.current()?;
        op.validate()?;
        let unknown = || Control::Unknown {
            operation_id: op.operation_id.clone(),
        };
        if op.descriptor.daemon_incarnation != self.registry.incarnation {
            return Ok(unknown());
        }
        #[cfg(test)]
        if let Some(observe) = self.registry.close_blocked.lock().unwrap().clone() {
            if matches!(
                self.registry.state.try_lock(),
                Err(std::sync::TryLockError::WouldBlock)
            ) {
                let _ = observe.try_send(());
            }
        }
        let mut directory = self
            .registry
            .state
            .lock()
            .map_err(|_| SessionError::Uncertain)?;
        self.registry.observe(&mut directory)?;
        directory.purge();
        if let Some(slot) = directory.find(
            self.principal(),
            &self.registry.incarnation,
            &op.descriptor.workspace_id,
            &metadata::digest(&op.operation_id)?,
        )? {
            let entry = directory.get(slot).ok_or(SessionError::Uncertain)?;
            if !entry.matches_descriptor(&op)? {
                return Err(SessionError::Identity);
            }
            let state = entry.metadata.fields()?.state;
            if state >= 4 {
                return entry.close_result();
            }
            let owner = entry.owner.upgrade();
            drop(directory);
            if let Some(owner) = owner {
                owner.request_close(Reason::Closed);
            } else {
                self.registry.uncertain(slot);
            }
            return self.registry.close_result(slot);
        }
        if directory.clock.high()
            >= intent_core::note_source_session::instant(&op.descriptor.accept_until)?
        {
            return Ok(unknown());
        }
        let source = binding(&op.descriptor.binding);
        let principal = format!("principal:{}", self.principal());
        let hold = match self.services.store.hold_canonical_source(
            &op.descriptor.workspace_id,
            &principal,
            &source,
        ) {
            Ok(h) => h,
            Err(crate::Error::Internal(_)) => return Err(SessionError::Uncertain),
            Err(_) => return Ok(unknown()),
        };
        let slot = match directory.reserve(self.principal(), self.identity.epoch, &op, hold, true) {
            Ok(s) => s,
            Err(SessionError::Expired) => return Ok(unknown()),
            Err(e) => return Err(e),
        };
        directory
            .get(slot)
            .ok_or(SessionError::Uncertain)?
            .close_result()
    }
    /// Admit the next sequential read while retaining its exact request ID.
    ///
    /// # Errors
    /// Rejects stale contexts, wrong identities, invalid requests or sequences,
    /// unavailable read credit, and uncertain source/registry ownership.
    pub fn read(
        &self,
        read: Read,
        id: Value,
    ) -> Result<impl std::future::Future<Output = Result<Delivery>> + Send + 'static> {
        self.current()?;
        validate_id(&id)?;
        if self.identity.mode != Mode::Read {
            return Err(SessionError::Unavailable);
        }
        let owner = self
            .owner
            .as_ref()
            .ok_or(SessionError::Unavailable)?
            .clone();
        let admission = (|| {
            let request = read.request.into_page()?;
            let limit = request.max_wire_bytes.ok_or(SessionError::Budget)?;
            let mut state = owner.state.lock().map_err(|_| SessionError::Uncertain)?;
            let session = state
                .session
                .as_ref()
                .ok_or(SessionError::Unavailable)?
                .clone();
            session
                .validate_delivery_request(&request)
                .map_err(map_error)?;
            let mut d = self
                .registry
                .state
                .lock()
                .map_err(|_| SessionError::Uncertain)?;
            let entry = d.get_mut(owner.slot).ok_or(SessionError::Uncertain)?;
            let v = entry.metadata.fields()?;
            if v.workspace != read.workspace_id
                || metadata::hex(&v.digest) != read.operation_id
                || v.epoch != self.identity.epoch
            {
                return Err(SessionError::Identity);
            }
            if owner.closing.load(Ordering::Acquire)
                || v.state != 1
                || v.outstanding != 0
                || v.flags & 0x0180 != 0
            {
                return Err(SessionError::Unavailable);
            }
            if v.sequence != read.sequence || read.sequence >= 9_007_199_254_740_991 {
                return Err(SessionError::Sequence);
            }
            let next = v.sequence.checked_add(1).ok_or(SessionError::Sequence)?;
            // Consume only after all validation and the previous local write settled.
            if let Some(hold) = state.delivered.take() {
                hold.retire().map_err(map_error)?;
            }
            entry.metadata.progress(2, v.reason, 127 | 128, next, 1)?;
            drop(d);
            let pending = match session.read_for_delivery(request, id.clone()) {
                Ok(p) => p,
                Err(error) => {
                    owner.registry.delivered(owner.slot, next, false)?;
                    return Err(map_error(error));
                }
            };
            Ok((pending, limit))
        })();
        let (pending, limit) = match admission {
            Ok(p) => p,
            Err(error) => {
                owner.request_close(Reason::Closed);
                return Err(error);
            }
        };
        let guard = Guard::new(owner.clone());
        let (tx, rx) = tokio::sync::oneshot::channel();
        let task_owner = owner.clone();
        tokio::spawn(async move {
            let mut guard = guard;
            let result = pending.read.await.map_err(map_error);
            if matches!(result, Err(SessionError::Uncertain)) {
                task_owner.uncertain();
            }
            let delivery = Delivery::new(
                task_owner,
                result,
                Some(pending.hold),
                limit,
                id,
                read.sequence + 1,
            );
            guard.complete = true;
            let _ = tx.send(delivery);
        });
        Ok(AwaitDelivery::new(owner, rx))
    }

    #[must_use]
    pub fn expiry(&self) -> Option<i128> {
        let owner = self.owner.as_ref()?;
        let directory = self.registry.state.lock().ok()?;
        Some(
            directory
                .get(owner.slot)?
                .metadata
                .fields()
                .ok()?
                .source_expiry,
        )
    }
    /// Revoke the operation and await its owned settlement.
    ///
    /// # Errors
    /// Returns an error if ownership or the cleanup outcome remains uncertain.
    pub async fn retire(&self) -> Result<()> {
        let Some(owner) = &self.owner else {
            return Ok(());
        };
        owner.request_close(Reason::Closed);
        loop {
            let notified = owner.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            match self.registry.close_result(owner.slot)? {
                Control::Settled { .. } => return Ok(()),
                Control::Uncertain { .. } | Control::Unknown { .. } => {
                    return Err(SessionError::Uncertain)
                }
                _ => notified.await,
            }
        }
    }
    /// Synchronous terminalization; returned cleanup status still depends on owned IO.
    pub fn revoke(&self) {
        if let Some(owner) = &self.owner {
            owner.request_close(Reason::Closed);
        }
    }
}
impl Drop for SourceConnection {
    fn drop(&mut self) {
        if self.transport == Some(false) {
            if let Some(owner) = &self.owner {
                owner.uncertain();
            }
        }
        self.revoke();
    }
}
/// Duplicate open returns no owner; only a newly admitted open has a continuation.
pub enum Open {
    Existing(Control),
    Pending(std::pin::Pin<Box<dyn std::future::Future<Output = Result<Delivery>> + Send>>),
}
