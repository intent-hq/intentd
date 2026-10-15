//! Volatile, bounded grace deletion. The original row remains authoritative;
//! cancellation never recreates a row or transfers its source to the client.
use super::{delivery_tasks::DeliveryTasks, Services};
use intent_core::{
    caller::{current_caller, current_wire_credential, CredentialLease, WireCredential},
    note_delete::{
        valid_identifier, NoteDeleteCancel, NoteDeleteError, NoteDeleteKey, NoteDeleteOperation,
        NoteDeleteOperationResponse, NoteDeletePending, NoteDeleteReason, NoteDeleteReceipt,
        NoteDeleteSchedule, NoteDeleteState, NoteDeleteStatus, NoteDeleteStatusResponse,
        NoteDeleteUnknown, NoteDeleteUnknownReason, NoteDeleteUnknownState, GLOBAL_CAPACITY,
        KEY_WINDOW_MS, MAX_DELAY_MS, MAX_RESULT_BYTES, MAX_SAFE_INTEGER, RECEIPT_TTL_MS,
        WORKSPACE_CAPACITY,
    },
    Caller, Error, Result, WorkspaceId,
};
use intent_store::note_delete_repo::{
    NoteDeleteAuthority, NoteDeleteCommitOutcome, NoteDeleteGuard,
};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::{watch, Notify, Semaphore};

pub(crate) struct Registry {
    epoch: String,
    born: tokio::time::Instant,
    state: Mutex<State>,
    workers: Semaphore,
    pub(crate) tasks: super::delivery_tasks::DeliveryTasks,
    #[cfg(test)]
    before_claim: Mutex<Option<Arc<TestPause>>>,
    #[cfg(test)]
    after_commit: Mutex<Option<Arc<TestPause>>>,
}
#[derive(Default)]
struct State {
    closed: bool,
    sequence: u64,
    entries: HashMap<NoteDeleteKey, Entry>,
}
type AdmissionResult = Option<std::result::Result<(), NoteDeleteError>>;

struct Entry {
    request: NoteDeleteSchedule,
    owner: String,
    receipt: Option<NoteDeleteReceipt>,
    ready: watch::Sender<AdmissionResult>,
    wake: Arc<Notify>,
}
impl Default for Registry {
    fn default() -> Self {
        Self {
            epoch: uuid::Uuid::new_v4().to_string(),
            born: tokio::time::Instant::now(),
            state: Mutex::new(State::default()),
            workers: Semaphore::new(4),
            tasks: DeliveryTasks::default(),
            #[cfg(test)]
            before_claim: Mutex::new(None),
            #[cfg(test)]
            after_commit: Mutex::new(None),
        }
    }
}
#[cfg(test)]
#[derive(Default)]
struct TestPause {
    entered: Notify,
    release: Notify,
}
#[cfg(test)]
async fn test_pause(slot: &Mutex<Option<Arc<TestPause>>>) {
    let pause = slot.lock().unwrap().take();
    if let Some(pause) = pause {
        pause.entered.notify_one();
        pause.release.notified().await;
    }
}
fn failure(code: NoteDeleteError) -> Error {
    Error::NoteDelete(code)
}
fn classify(error: &Error) -> NoteDeleteError {
    match error {
        Error::NoteDelete(code) => *code,
        _ => NoteDeleteError::Unavailable,
    }
}
fn owner(caller: &Caller) -> String {
    match caller {
        Caller::Wire { principal_id, .. } => format!("principal:{principal_id}"),
        Caller::Agent { agent_id } => format!("agent:{agent_id}"),
        Caller::Daemon => "daemon".into(),
    }
}
fn authority() -> Result<(NoteDeleteAuthority, Option<WireCredential>)> {
    let caller = current_caller().ok_or_else(|| failure(NoteDeleteError::Forbidden))?;
    let wire = current_wire_credential();
    if wire
        .as_ref()
        .is_some_and(|wire| caller.principal_id() != Some(wire.principal_id()))
    {
        return Err(failure(NoteDeleteError::Forbidden));
    }
    let principal_token_hash = match &wire {
        Some(WireCredential::Principal { token_hash, .. }) => Some(token_hash.clone()),
        _ => None,
    };
    Ok((
        NoteDeleteAuthority {
            caller,
            principal_token_hash,
        },
        wire,
    ))
}
async fn lease(wire: Option<&WireCredential>) -> Result<Option<CredentialLease>> {
    match wire {
        Some(WireCredential::Legacy { authority, .. }) => authority
            .authorize()
            .await
            .map(Some)
            .map_err(|_| failure(NoteDeleteError::Forbidden)),
        _ => Ok(None),
    }
}
fn bounded<T: serde::Serialize>(value: T) -> Result<T> {
    if serde_json::to_vec(&value)
        .map_err(|_| failure(NoteDeleteError::Unavailable))?
        .len()
        > MAX_RESULT_BYTES
    {
        return Err(failure(NoteDeleteError::Unavailable));
    }
    Ok(value)
}
impl State {
    fn next(&mut self) -> Result<u64> {
        if self.sequence >= MAX_SAFE_INTEGER {
            self.closed = true;
            return Err(failure(NoteDeleteError::ShuttingDown));
        }
        self.sequence += 1;
        Ok(self.sequence)
    }
    fn sweep(&mut self, now: u64) {
        self.entries.retain(|_, entry| {
            entry
                .receipt
                .as_ref()
                .and_then(|r| r.expires_tick_ms)
                .is_none_or(|expiry| now < expiry)
        });
    }
}
impl Registry {
    fn tick(&self) -> Result<u64> {
        let tick = u64::try_from(self.born.elapsed().as_millis())
            .map_err(|_| failure(NoteDeleteError::ShuttingDown))?;
        if tick > MAX_SAFE_INTEGER - RECEIPT_TTL_MS - MAX_DELAY_MS {
            return Err(failure(NoteDeleteError::ShuttingDown));
        }
        Ok(tick)
    }
    fn unknown(&self, key: &NoteDeleteKey) -> NoteDeleteOperation {
        NoteDeleteOperation::Unknown(NoteDeleteUnknown {
            operation_key: key.clone(),
            state: NoteDeleteUnknownState::Unknown,
            reason: if key.epoch == self.epoch {
                NoteDeleteUnknownReason::Unavailable
            } else {
                NoteDeleteUnknownReason::PreviousEpoch
            },
        })
    }
    fn operation(
        &self,
        state: &State,
        key: &NoteDeleteKey,
        workspace: &WorkspaceId,
        note: &intent_core::NoteId,
        caller: &str,
    ) -> Result<NoteDeleteOperation> {
        let Some(entry) = state.entries.get(key) else {
            return Ok(self.unknown(key));
        };
        if entry.request.workspace_id != *workspace
            || entry.request.note_id != *note
            || entry.owner != caller
        {
            return Err(failure(NoteDeleteError::Forbidden));
        }
        Ok(entry
            .receipt
            .clone()
            .map_or_else(|| self.unknown(key), NoteDeleteOperation::Receipt))
    }
    fn response(
        &self,
        request: &NoteDeleteCancel,
        caller: &str,
    ) -> Result<NoteDeleteOperationResponse> {
        let now = self.tick()?;
        let mut state = self.state.lock().unwrap();
        state.sweep(now);
        bounded(NoteDeleteOperationResponse {
            epoch: self.epoch.clone(),
            server_tick_ms: now,
            sequence: state.sequence,
            operation: self.operation(
                &state,
                &request.operation_key,
                &request.workspace_id,
                &request.note_id,
                caller,
            )?,
        })
    }
    fn reserve(
        &self,
        request: &NoteDeleteSchedule,
        caller: &str,
    ) -> Result<(bool, watch::Receiver<AdmissionResult>)> {
        let now = self.tick()?;
        let mut state = self.state.lock().unwrap();
        state.sweep(now);
        // Exact replay precedes both age and quota checks and never rearms.
        if let Some(entry) = state.entries.get(&request.operation_key) {
            if entry.owner != caller {
                return Err(failure(NoteDeleteError::Forbidden));
            }
            if entry.request != *request {
                return Err(failure(NoteDeleteError::KeyMismatch));
            }
            return Ok((false, entry.ready.subscribe()));
        }
        if state.closed {
            return Err(failure(NoteDeleteError::ShuttingDown));
        }
        if request.operation_key.epoch != self.epoch
            || request.operation_key.issued_tick_ms > now
            || now - request.operation_key.issued_tick_ms > KEY_WINDOW_MS
        {
            return Err(failure(NoteDeleteError::KeyExpired));
        }
        if state.entries.values().any(|e| {
            e.request.workspace_id == request.workspace_id
                && e.request.note_id == request.note_id
                && e.request.note_instance_id == request.note_instance_id
                && e.receipt.as_ref().is_none_or(|r| {
                    r.expires_tick_ms.is_none() || r.state == NoteDeleteState::OutcomeUnknown
                })
        }) {
            return Err(failure(NoteDeleteError::AlreadyPending));
        }
        if state.entries.len() >= GLOBAL_CAPACITY
            || state
                .entries
                .values()
                .filter(|e| e.request.workspace_id == request.workspace_id)
                .count()
                >= WORKSPACE_CAPACITY
        {
            return Err(failure(NoteDeleteError::Quota));
        }
        let (ready, receiver) = watch::channel(None);
        state.entries.insert(
            request.operation_key.clone(),
            Entry {
                request: request.clone(),
                owner: caller.into(),
                receipt: None,
                ready,
                wake: Arc::new(Notify::new()),
            },
        );
        Ok((true, receiver))
    }
    fn reject(&self, key: &NoteDeleteKey, error: NoteDeleteError) {
        if let Some(entry) = self.state.lock().unwrap().entries.remove(key) {
            entry.ready.send_replace(Some(Err(error)));
        }
    }
    fn admit(&self, request: &NoteDeleteSchedule) -> Result<(NoteDeleteReceipt, Arc<Notify>)> {
        let now = self.tick()?;
        let delay_ms =
            i64::try_from(request.undo_delay_ms).map_err(|_| failure(NoteDeleteError::Invalid))?;
        let delete_at = (chrono::Utc::now() + chrono::Duration::milliseconds(delay_ms))
            .to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
        let mut state = self.state.lock().unwrap();
        if state.closed {
            return Err(failure(NoteDeleteError::ShuttingDown));
        }
        let sequence = state.next()?;
        let receipt = NoteDeleteReceipt {
            operation_key: request.operation_key.clone(),
            workspace_id: request.workspace_id.clone(),
            note_id: request.note_id.clone(),
            note_instance_id: request.note_instance_id.clone(),
            state: NoteDeleteState::Pending,
            sequence,
            deadline_tick_ms: now + request.undo_delay_ms,
            delete_at,
            expires_tick_ms: None,
            reason: None,
        };
        let entry = state
            .entries
            .get_mut(&request.operation_key)
            .ok_or_else(|| failure(NoteDeleteError::ShuttingDown))?;
        entry.receipt = Some(receipt.clone());
        entry.ready.send_replace(Some(Ok(())));
        Ok((receipt, entry.wake.clone()))
    }
    fn transition(
        &self,
        key: &NoteDeleteKey,
        expected: NoteDeleteState,
        next: NoteDeleteState,
        reason: Option<NoteDeleteReason>,
    ) -> Result<Option<NoteDeleteReceipt>> {
        let mut state = self.state.lock().unwrap();
        if state
            .entries
            .get(key)
            .and_then(|e| e.receipt.as_ref())
            .is_none_or(|r| r.state != expected)
        {
            return Ok(None);
        }
        let sequence = state.next()?;
        let entry = state
            .entries
            .get_mut(key)
            .expect("entry checked under lock");
        let receipt = entry.receipt.as_mut().expect("receipt checked under lock");
        receipt.state = next;
        receipt.sequence = sequence;
        receipt.reason = reason;
        entry.wake.notify_one();
        Ok(Some(receipt.clone()))
    }
    // No await or asynchronous notification remains after this boundary.
    fn settle_unknown(&self, key: &NoteDeleteKey) -> Result<NoteDeleteReceipt> {
        let now = self.tick()?;
        let mut state = self.state.lock().unwrap();
        let sequence = state.next()?;
        let receipt = state
            .entries
            .get_mut(key)
            .and_then(|e| e.receipt.as_mut())
            .filter(|r| r.state == NoteDeleteState::Committing)
            .ok_or_else(|| failure(NoteDeleteError::Unavailable))?;
        receipt.state = NoteDeleteState::OutcomeUnknown;
        receipt.reason = Some(NoteDeleteReason::CommitOutcomeUnknown);
        receipt.sequence = sequence;
        receipt.expires_tick_ms = Some(now + RECEIPT_TTL_MS);
        Ok(receipt.clone())
    }
    fn settle(&self, key: &NoteDeleteKey) -> Result<()> {
        let now = self.tick()?;
        let mut state = self.state.lock().unwrap();
        if let Some(receipt) = state.entries.get_mut(key).and_then(|e| e.receipt.as_mut()) {
            if !matches!(
                receipt.state,
                NoteDeleteState::Pending | NoteDeleteState::Committing
            ) && receipt.expires_tick_ms.is_none()
            {
                receipt.expires_tick_ms = Some(now + RECEIPT_TTL_MS);
            }
        }
        Ok(())
    }
    pub(crate) fn close(&self) {
        let mut state = self.state.lock().unwrap();
        state.closed = true;
        for entry in state.entries.values() {
            entry.wake.notify_one();
        }
    }
    fn closed(&self) -> bool {
        self.state.lock().unwrap().closed
    }
}
impl Services {
    pub(crate) async fn grace_schedule(
        &self,
        request: NoteDeleteSchedule,
    ) -> Result<NoteDeleteOperationResponse> {
        if !request.valid() {
            return Err(failure(NoteDeleteError::Invalid));
        }
        let (auth, wire) = authority()?;
        {
            let _lease = lease(wire.as_ref()).await?;
            self.store
                .note_delete_current(&auth, &request.workspace_id, None)
                .await?;
        }
        let caller = owner(&auth.caller);
        let (new, mut ready) = self.note_deletions.reserve(&request, &caller)?;
        if new {
            let services = self.clone();
            let work = request.clone();
            // No await between reserving capacity and registering its sole owner.
            if self
                .note_deletions
                .tasks
                .spawn_draining(async move {
                    let caller = auth.caller.clone();
                    let credential = wire.clone();
                    intent_core::caller::with_caller(
                        caller,
                        intent_core::caller::with_wire_credential(
                            credential,
                            services.grace_admit_and_wait(work, auth, wire),
                        ),
                    )
                    .await;
                })
                .is_none()
            {
                self.note_deletions
                    .reject(&request.operation_key, NoteDeleteError::ShuttingDown);
            }
        }
        loop {
            let result = *ready.borrow_and_update();
            if let Some(result) = result {
                result.map_err(failure)?;
                break;
            }
            ready
                .changed()
                .await
                .map_err(|_| failure(NoteDeleteError::Unavailable))?;
        }
        self.note_deletions.response(
            &NoteDeleteCancel {
                workspace_id: request.workspace_id,
                note_id: request.note_id,
                operation_key: request.operation_key,
            },
            &caller,
        )
    }
    pub(crate) async fn grace_cancel(
        &self,
        request: NoteDeleteCancel,
    ) -> Result<NoteDeleteOperationResponse> {
        if !valid_identifier(request.workspace_id.as_str())
            || !valid_identifier(request.note_id.as_str())
            || !request.operation_key.valid()
        {
            return Err(failure(NoteDeleteError::Invalid));
        }
        let (auth, wire) = authority()?;
        let credential_lease = lease(wire.as_ref()).await?;
        self.store
            .note_delete_current(&auth, &request.workspace_id, None)
            .await?;
        let caller = owner(&auth.caller);
        // Authorization and cancellation use one short registry critical section.
        let changed = {
            let now = self.note_deletions.tick()?;
            let mut state = self.note_deletions.state.lock().unwrap();
            state.sweep(now);
            let op = self.note_deletions.operation(
                &state,
                &request.operation_key,
                &request.workspace_id,
                &request.note_id,
                &caller,
            )?;
            if matches!(op, NoteDeleteOperation::Receipt(ref r) if r.state == NoteDeleteState::Pending)
            {
                let seq = state.next()?;
                let entry = state
                    .entries
                    .get_mut(&request.operation_key)
                    .expect("authorized receipt");
                let receipt = entry.receipt.as_mut().expect("authorized receipt");
                receipt.state = NoteDeleteState::Cancelled;
                receipt.reason = Some(NoteDeleteReason::Cancelled);
                receipt.sequence = seq;
                entry.wake.notify_one();
                Some(receipt.clone())
            } else {
                None
            }
        };
        drop(credential_lease);
        if let Some(receipt) = changed {
            self.grace_event(&receipt);
        }
        self.note_deletions.response(&request, &caller)
    }
    pub(crate) async fn grace_status(
        &self,
        request: NoteDeleteStatus,
    ) -> Result<NoteDeleteStatusResponse> {
        if !valid_identifier(request.workspace_id.as_str())
            || request
                .note_id
                .as_ref()
                .is_some_and(|n| !valid_identifier(n.as_str()))
            || request
                .operation_key
                .as_ref()
                .is_some_and(|k| !k.valid() || request.note_id.is_none())
        {
            return Err(failure(NoteDeleteError::Invalid));
        }
        let (auth, wire) = authority()?;
        let _lease = lease(wire.as_ref()).await?;
        let current = self
            .store
            .note_delete_current(&auth, &request.workspace_id, request.note_id.as_ref())
            .await?;
        let caller = owner(&auth.caller);
        let now = self.note_deletions.tick()?;
        let mut state = self.note_deletions.state.lock().unwrap();
        state.sweep(now);
        let operation = request
            .operation_key
            .as_ref()
            .map(|key| {
                self.note_deletions.operation(
                    &state,
                    key,
                    &request.workspace_id,
                    request.note_id.as_ref().expect("validated note"),
                    &caller,
                )
            })
            .transpose()?;
        let mut pending: Vec<_> = state
            .entries
            .values()
            .filter_map(|entry| {
                let r = entry.receipt.as_ref()?;
                if r.workspace_id != request.workspace_id
                    || !matches!(
                        r.state,
                        NoteDeleteState::Pending
                            | NoteDeleteState::Committing
                            | NoteDeleteState::OutcomeUnknown
                    )
                {
                    return None;
                }
                if let Some(note) = &request.note_id {
                    if r.note_id != *note
                        || current
                            .as_ref()
                            .is_none_or(|c| c.note_instance_id != r.note_instance_id)
                    {
                        return None;
                    }
                }
                Some(NoteDeletePending {
                    operation_key: r.operation_key.clone(),
                    note_id: r.note_id.clone(),
                    note_instance_id: r.note_instance_id.clone(),
                    state: r.state,
                    sequence: r.sequence,
                    deadline_tick_ms: r.deadline_tick_ms,
                    delete_at: r.delete_at.clone(),
                    can_cancel: r.state == NoteDeleteState::Pending && entry.owner == caller,
                })
            })
            .collect();
        pending.sort_by_key(|p| p.sequence);
        bounded(NoteDeleteStatusResponse {
            epoch: self.note_deletions.epoch.clone(),
            server_tick_ms: now,
            sequence: state.sequence,
            current,
            pending,
            operation,
        })
    }
    async fn grace_admit_and_wait(
        &self,
        request: NoteDeleteSchedule,
        auth: NoteDeleteAuthority,
        wire: Option<WireCredential>,
    ) {
        let prepared = async {
            let _lease = lease(wire.as_ref()).await?;
            self.store.note_delete_prepare(&auth, &request).await
        }
        .await;
        let guard = match prepared {
            Ok(guard) => guard,
            Err(error) => {
                self.note_deletions
                    .reject(&request.operation_key, classify(&error));
                return;
            }
        };
        let (receipt, wake) = match self.note_deletions.admit(&request) {
            Ok(value) => value,
            Err(error) => {
                self.note_deletions
                    .reject(&request.operation_key, classify(&error));
                return;
            }
        };
        self.grace_event(&receipt);
        let deadline = self.note_deletions.born + Duration::from_millis(receipt.deadline_tick_ms);
        loop {
            if self.note_deletions.closed() {
                if let Ok(Some(r)) = self.note_deletions.transition(
                    &request.operation_key,
                    NoteDeleteState::Pending,
                    NoteDeleteState::Cancelled,
                    Some(NoteDeleteReason::Shutdown),
                ) {
                    self.grace_event(&r);
                }
                let _ = self.note_deletions.settle(&request.operation_key);
                return;
            }
            let pending = self
                .note_deletions
                .state
                .lock()
                .unwrap()
                .entries
                .get(&request.operation_key)
                .and_then(|e| e.receipt.as_ref())
                .is_some_and(|r| r.state == NoteDeleteState::Pending);
            if !pending {
                let _ = self.note_deletions.settle(&request.operation_key);
                return;
            }
            tokio::select! { () = tokio::time::sleep_until(deadline) => break, () = wake.notified() => {} }
        }
        #[cfg(test)]
        test_pause(&self.note_deletions.before_claim).await;
        // The same short mutex serializes claim with cancel; never held across IO.
        let claimed = {
            let mut state = self.note_deletions.state.lock().unwrap();
            if state.closed {
                None
            } else if state
                .entries
                .get(&request.operation_key)
                .and_then(|e| e.receipt.as_ref())
                .is_some_and(|r| r.state == NoteDeleteState::Pending)
            {
                match state.next() {
                    Ok(seq) => {
                        let r = state
                            .entries
                            .get_mut(&request.operation_key)
                            .and_then(|e| e.receipt.as_mut())
                            .expect("pending entry");
                        r.state = NoteDeleteState::Committing;
                        r.sequence = seq;
                        Some(r.clone())
                    }
                    Err(_) => None,
                }
            } else {
                None
            }
        };
        let Some(claimed) = claimed else {
            if let Ok(Some(r)) = self.note_deletions.transition(
                &request.operation_key,
                NoteDeleteState::Pending,
                NoteDeleteState::Cancelled,
                Some(NoteDeleteReason::Shutdown),
            ) {
                self.grace_event(&r);
            }
            let _ = self.note_deletions.settle(&request.operation_key);
            return;
        };
        let acquire_deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        self.grace_event(&claimed);
        self.grace_commit(&request, &auth, wire.as_ref(), &guard, acquire_deadline)
            .await;
        let _ = self.note_deletions.settle(&request.operation_key);
    }
    fn grace_terminal(
        &self,
        request: &NoteDeleteSchedule,
        state: NoteDeleteState,
        reason: Option<NoteDeleteReason>,
    ) {
        if let Ok(Some(receipt)) = self.note_deletions.transition(
            &request.operation_key,
            NoteDeleteState::Committing,
            state,
            reason,
        ) {
            self.grace_event(&receipt);
        }
    }
    async fn grace_commit(
        &self,
        request: &NoteDeleteSchedule,
        auth: &NoteDeleteAuthority,
        wire: Option<&WireCredential>,
        guard: &NoteDeleteGuard,
        deadline: tokio::time::Instant,
    ) {
        let Ok(Ok(_permit)) =
            tokio::time::timeout_at(deadline, self.note_deletions.workers.acquire()).await
        else {
            self.grace_terminal(
                request,
                NoteDeleteState::Failed,
                Some(NoteDeleteReason::DeadlineBudget),
            );
            return;
        };
        let preparation = async {
            let note = match self
                .store
                .get_note(&request.workspace_id, &request.note_id)
                .await
            {
                Ok(note) => Some(note),
                Err(Error::NotFound(_)) => None,
                Err(error) => return Err(error),
            };
            let before = if note
                .as_ref()
                .is_some_and(|note| note.metadata.task.is_some())
            {
                Some(self.store.list_notes(&request.workspace_id).await?)
            } else {
                None
            };
            // Acquire rotation/revocation protection only after potentially long
            // callback preimage reads; hold it through physical writer settlement.
            let lease = lease(wire).await?;
            Ok::<_, Error>((lease, note, before))
        };
        let (lease, note, before) = match tokio::time::timeout_at(deadline, preparation).await {
            Ok(Ok(value)) => value,
            Ok(Err(error)) => {
                self.grace_terminal(
                    request,
                    NoteDeleteState::Failed,
                    Some(
                        if matches!(error, Error::NoteDelete(NoteDeleteError::Forbidden)) {
                            NoteDeleteReason::AuthorityLost
                        } else {
                            NoteDeleteReason::StorageFailure
                        },
                    ),
                );
                return;
            }
            Err(_) => {
                self.grace_terminal(
                    request,
                    NoteDeleteState::Failed,
                    Some(NoteDeleteReason::DeadlineBudget),
                );
                return;
            }
        };
        // No timeout or caller cancellation wraps this owned transaction.
        let outcome = self
            .store
            .note_delete_guarded_commit(auth, request, guard, deadline)
            .await;
        drop(lease);
        #[cfg(test)]
        test_pause(&self.note_deletions.after_commit).await;
        match outcome {
            Ok(NoteDeleteCommitOutcome::Deleted) => {
                self.grace_terminal(request, NoteDeleteState::Deleted, None);
                // Existing callback scans/parse costs are preserved. The worker
                // permit and non-expiring entry remain owned through this tail.
                if let Some(note) = note {
                    if let Err(error) = self.grace_delete_callbacks(&note, before.as_deref()).await
                    {
                        tracing::warn!(%error, "committed note deletion notification tail failed");
                    }
                }
            }
            Ok(NoteDeleteCommitOutcome::Rejected(reason)) => {
                self.grace_terminal(request, NoteDeleteState::Conflict, Some(reason));
            }
            Ok(NoteDeleteCommitOutcome::OutcomeUnknown) => {
                if let Ok(receipt) = self.note_deletions.settle_unknown(&request.operation_key) {
                    self.grace_event(&receipt);
                }
            }
            Ok(NoteDeleteCommitOutcome::Failed) => {
                self.grace_terminal(
                    request,
                    NoteDeleteState::Failed,
                    Some(NoteDeleteReason::StorageFailure),
                );
            }
            Err(error) => {
                // begin_before emits Unavailable only for its pool deadline;
                // actual acquisition/BEGIN failures remain storage failures.
                let reason = if matches!(error, Error::NoteDelete(NoteDeleteError::Unavailable)) {
                    NoteDeleteReason::DeadlineBudget
                } else {
                    NoteDeleteReason::StorageFailure
                };
                self.grace_terminal(request, NoteDeleteState::Failed, Some(reason));
            }
        }
    }
    fn grace_event(&self, receipt: &NoteDeleteReceipt) {
        let mut event = super::note_change_event(
            &receipt.workspace_id,
            &receipt.note_id,
            "",
            intent_core::events::NOTE_DELETE_OPERATION,
            "",
        );
        event.data = serde_json::json!({ "workspaceId":receipt.workspace_id,"noteId":receipt.note_id,"noteInstanceId":receipt.note_instance_id,
            "epoch":self.note_deletions.epoch,"sequence":receipt.sequence,"operationKey":receipt.operation_key,"state":receipt.state,"deadlineTickMs":receipt.deadline_tick_ms });
        if let Some(bus) = &self.event_bus {
            let _ = bus.publish_transient(&event);
        }
    }
    async fn grace_delete_callbacks(
        &self,
        note: &intent_core::Note,
        before: Option<&[intent_core::Note]>,
    ) -> Result<()> {
        use intent_core::events::NOTE_DELETED;
        let ws = &note.workspace_id;
        let id = &note.id;
        super::publish_event(
            self.event_bus.as_ref(),
            super::note_change_event(ws, id, &note.title, NOTE_DELETED, "delete"),
        )
        .await;
        if let Some(task) = &note.metadata.task {
            let remaining = self.store.list_notes(ws).await?;
            let depended_on = remaining.iter().any(|n| {
                n.metadata
                    .task
                    .as_ref()
                    .is_some_and(|t| t.depends_on.iter().any(|d| d == id))
            });
            let ready = super::compute_ready_task_ids(&remaining);
            if depended_on || before.is_none_or(|b| super::compute_ready_task_ids(b) != ready) {
                super::publish_event(
                    self.event_bus.as_ref(),
                    super::ready_tasks_changed_reason_event(
                        ws,
                        &ready,
                        id,
                        "note-deleted",
                        &super::now_iso(),
                    ),
                )
                .await;
            }
            if task.status == intent_core::TaskStatus::Complete {
                super::publish_dependent_note_updates(self.event_bus.as_ref(), ws, id, &remaining)
                    .await;
            }
            self.maybe_emit_display_status_changed(ws).await;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
