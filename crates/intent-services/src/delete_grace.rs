//! In-memory delete grace-window registry (PROTOCOL §5.1).
//!
//! A `*.delete` with `undoDelayMs > 0` registers a pending deletion here
//! instead of committing immediately: the entry pairs the ISO `deleteAt`
//! deadline with the timer task that commits the delete on expiry. On
//! `SQLite` (desktop) entries are **never persisted**: a daemon restart drops
//! every pending deletion and the entity survives (the spec's restart
//! semantics). On Postgres (orchestrator mode) the owned registry also
//! stores the deadline ([`Store::record_delete_grace`]): the next process to
//! serve the tenant re-arms it ([`OwnedPendingDeletes::rearm_owned`]), and
//! a commit claims the stored row before the delete and removes it after,
//! so the delete commits once and a crash in between is retried.
//!
//! Race safety is generation-based: each schedule mints a generation token
//! the timer must present to claim its entry at fire time. A cancel (or an
//! immediate delete while pending) removes the entry and aborts the timer;
//! a timer that already claimed its entry can no longer be cancelled — the
//! cancel observes "nothing pending" and reports `false`, the non-error
//! race-safe outcome.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// Cap on the caller-supplied `undoDelayMs` (the "sane cap, e.g. 60s" from
/// the wire contract). Values above it are clamped, never rejected.
pub(crate) const MAX_UNDO_DELAY_MS: u64 = 60_000;

/// Clamp a caller-supplied grace delay to [`MAX_UNDO_DELAY_MS`].
pub(crate) fn clamp_undo_delay_ms(ms: u64) -> u64 {
    ms.min(MAX_UNDO_DELAY_MS)
}

struct Pending {
    delete_at: String,
    generation: u64,
    handle: tokio::task::JoinHandle<()>,
}

/// Registry of pending deletions keyed by entity id. Shared across
/// [`crate::Services`] clones so every front door observes one set.
#[derive(Clone, Default)]
pub(crate) struct PendingDeletes {
    inner: Arc<Mutex<HashMap<String, Pending>>>,
    next_generation: Arc<AtomicU64>,
}

#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "Legacy standalone helper is retained for compatibility tests."
    )
)]
impl PendingDeletes {
    /// ISO deadline of the pending deletion for `key`, when one is scheduled.
    /// Backs both the idempotent re-schedule and the `pendingDeleteAt` row
    /// projection.
    pub(crate) fn deadline(&self, key: &str) -> Option<String> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(key)
            .map(|p| p.delete_at.clone())
    }

    /// Register a pending deletion unless one is already pending for `key`.
    /// The idempotent re-schedule check runs under the registry lock, so
    /// concurrent schedules for the same key cannot each arm a timer: the
    /// loser observes the winner's entry and gets its deadline back as
    /// `Some(existing)` without `spawn` ever running. `None` means the
    /// entry was newly armed with the supplied `delete_at`. `spawn`
    /// receives the minted generation token and must return the timer task
    /// that will present it to [`PendingDeletes::claim`] at fire time; it
    /// runs under the registry lock, so a (pathologically fast) timer
    /// cannot observe the map before its own entry is inserted.
    pub(crate) fn schedule(
        &self,
        key: String,
        delete_at: String,
        spawn: impl FnOnce(u64) -> tokio::task::JoinHandle<()>,
    ) -> Option<String> {
        let mut map = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(existing) = map.get(&key) {
            return Some(existing.delete_at.clone());
        }
        let generation = self.next_generation.fetch_add(1, Ordering::Relaxed);
        let handle = spawn(generation);
        map.insert(
            key,
            Pending {
                delete_at,
                generation,
                handle,
            },
        );
        None
    }

    /// Timer-side claim at fire time: removes the entry only when it still
    /// belongs to this timer (same generation). `true` means the timer owns
    /// the commit; `false` means the entry was cancelled or superseded and
    /// the timer must do nothing.
    pub(crate) fn claim(&self, key: &str, generation: u64) -> bool {
        let mut map = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match map.get(key) {
            Some(p) if p.generation == generation => {
                map.remove(key);
                true
            }
            _ => false,
        }
    }

    /// Cancel a pending deletion: removes the entry and aborts its timer.
    /// Returns `true` when something was pending, `false` otherwise (already
    /// committed, or never scheduled) — the race-safe non-error outcome.
    pub(crate) fn cancel(&self, key: &str) -> bool {
        let removed = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(key);
        match removed {
            Some(p) => {
                p.handle.abort();
                true
            }
            None => false,
        }
    }
}

use intent_core::{AgentId, Error, Result, WorkspaceId};
use intent_store::{PendingDelete, RepositoryLifecycleKey, RepositoryPendingDeleteGuard, Store};
use tokio::sync::{oneshot, watch};

/// Original operation selectors, never permission or reconstructed row identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum PendingDeleteSubject {
    Workspace(WorkspaceId),
    Agent {
        workspace_id: WorkspaceId,
        agent_id: AgentId,
    },
}

#[derive(Clone, PartialEq, Eq, Hash)]
enum OwnedKey {
    Workspace(WorkspaceId),
    Agent(AgentId),
}

impl PendingDeleteSubject {
    /// The stored grace row's kind and subject id, and its workspace.
    fn grace_parts(&self) -> (&'static str, &str, &WorkspaceId) {
        match self {
            Self::Workspace(id) => ("workspace", id.as_str(), id),
            Self::Agent {
                workspace_id,
                agent_id,
            } => ("agent", agent_id.0.as_str(), workspace_id),
        }
    }

    fn key(&self) -> OwnedKey {
        match self {
            Self::Workspace(id) => OwnedKey::Workspace(id.clone()),
            Self::Agent { agent_id, .. } => OwnedKey::Agent(agent_id.clone()),
        }
    }

    fn lifecycle_key(&self) -> RepositoryLifecycleKey {
        match self {
            Self::Workspace(id) => RepositoryLifecycleKey::Workspace(id.clone()),
            Self::Agent { agent_id, .. } => RepositoryLifecycleKey::Agent(agent_id.clone()),
        }
    }
}

/// Descriptive scheduling result. Only a newly armed operation emits its event.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct PendingDeleteSchedule {
    pub(crate) delete_at: String,
    pub(crate) newly_armed: bool,
}

/// Carry through actual terminal work, including any background cleanup.
/// A response acknowledgement alone never establishes confirmed completion.
#[must_use = "dropping an unconfirmed claim preserves unresolved ownership"]
pub(crate) struct PendingDeleteClaim {
    subject: PendingDeleteSubject,
    guard: RepositoryPendingDeleteGuard,
}

impl PendingDeleteClaim {
    pub(crate) fn subject(&self) -> &PendingDeleteSubject {
        &self.subject
    }

    pub(crate) async fn settle_confirmed(self) -> Result<()> {
        self.guard.settle_confirmed().await
    }
}

#[derive(Clone)]
enum PreparationStatus {
    Preparing,
    Armed(String),
    Abandoned,
}

struct Attempt {
    subject: PendingDeleteSubject,
    ready: watch::Sender<PreparationStatus>,
}

impl Attempt {
    async fn prepared(&self) -> Option<String> {
        let mut receiver = self.ready.subscribe();
        loop {
            match receiver.borrow_and_update().clone() {
                PreparationStatus::Preparing => {}
                PreparationStatus::Armed(deadline) => return Some(deadline),
                PreparationStatus::Abandoned => return None,
            }
            if receiver.changed().await.is_err() {
                return None;
            }
        }
    }
}

struct OwnedArmed {
    attempt: Arc<Attempt>,
    delete_at: String,
    guard: Option<RepositoryPendingDeleteGuard>,
    handle: tokio::task::JoinHandle<()>,
}

enum OwnedEntry {
    Preparing(Arc<Attempt>),
    Armed(OwnedArmed),
}

impl OwnedEntry {
    fn attempt(&self) -> &Arc<Attempt> {
        match self {
            Self::Preparing(attempt) => attempt,
            Self::Armed(armed) => &armed.attempt,
        }
    }
}

struct OwnedInner {
    store: Store,
    entries: Mutex<HashMap<OwnedKey, OwnedEntry>>,
}

impl OwnedInner {
    fn take_armed(
        &self,
        attempt: &Arc<Attempt>,
    ) -> Option<(PendingDeleteClaim, tokio::task::JoinHandle<()>)> {
        let key = attempt.subject.key();
        let mut map = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let OwnedEntry::Armed(armed) = map.get_mut(&key)? else {
            return None;
        };
        if !Arc::ptr_eq(&armed.attempt, attempt) {
            return None;
        }
        // The original guard is in its nonclone claim BEFORE marker removal.
        let claim = PendingDeleteClaim {
            subject: attempt.subject.clone(),
            guard: armed.guard.take()?,
        };
        let Some(OwnedEntry::Armed(removed)) = map.remove(&key) else {
            unreachable!()
        };
        drop(map);
        Some((claim, removed.handle))
    }
}

/// Cleans up only this unpublished attempt if preparation is cancelled or panics.
/// Unknown guard creation/spawn never confirms an operation during unwinding.
struct PreparationOwner {
    inner: Arc<OwnedInner>,
    attempt: Arc<Attempt>,
    guard: Option<RepositoryPendingDeleteGuard>,
    timer: Option<tokio::task::JoinHandle<()>>,
    armed: bool,
}

impl Drop for PreparationOwner {
    fn drop(&mut self) {
        if self.armed {
            return;
        }
        let removed = {
            let mut map = self
                .inner
                .entries
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let key = self.attempt.subject.key();
            if map
                .get(&key)
                .is_some_and(|entry| Arc::ptr_eq(entry.attempt(), &self.attempt))
            {
                map.remove(&key)
            } else {
                None
            }
        };
        if let Some(OwnedEntry::Armed(entry)) = removed {
            entry.handle.abort();
            // No implicit confirmation, including a panicked or unknown spawn.
            drop(entry.guard);
        }
        if let Some(timer) = self.timer.take() {
            timer.abort();
        }
        self.attempt
            .ready
            .send_replace(PreparationStatus::Abandoned);
    }
}

/// Separate typed form: legacy operations cannot look up or lose owned entries.
/// One original Store and one map are shared by clones; there is no default Store.
#[derive(Clone)]
pub(crate) struct OwnedPendingDeletes {
    inner: Arc<OwnedInner>,
    tasks: Arc<crate::delivery_tasks::DeliveryTasks>,
}

impl OwnedPendingDeletes {
    pub(crate) fn new(original_store: Store) -> Self {
        Self {
            inner: Arc::new(OwnedInner {
                store: original_store,
                entries: Mutex::default(),
            }),
            tasks: Arc::new(crate::delivery_tasks::DeliveryTasks::default()),
        }
    }

    pub(crate) fn with_tasks(mut self, tasks: Arc<crate::delivery_tasks::DeliveryTasks>) -> Self {
        self.tasks = tasks;
        self
    }

    fn check_subject(entry: &OwnedEntry, subject: &PendingDeleteSubject) -> Result<()> {
        if &entry.attempt().subject != subject {
            return Err(Error::InvalidParams(
                "pending deletion has a different original subject".into(),
            ));
        }
        Ok(())
    }

    fn original_attempt(&self, subject: &PendingDeleteSubject) -> Result<Option<Arc<Attempt>>> {
        let map = self
            .inner
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(entry) = map.get(&subject.key()) else {
            return Ok(None);
        };
        Self::check_subject(entry, subject)?;
        Ok(Some(entry.attempt().clone()))
    }

    pub(crate) fn deadline(&self, subject: &PendingDeleteSubject) -> Result<Option<String>> {
        let map = self
            .inner
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(entry) = map.get(&subject.key()) else {
            return Ok(None);
        };
        Self::check_subject(entry, subject)?;
        Ok(match entry {
            OwnedEntry::Preparing(_) => None,
            OwnedEntry::Armed(armed) => Some(armed.delete_at.clone()),
        })
    }

    pub(crate) async fn schedule_owned<F, Fut, O>(
        &self,
        subject: PendingDeleteSubject,
        delay_ms: u64,
        delete: F,
    ) -> Result<PendingDeleteSchedule>
    where
        F: FnOnce(PendingDeleteClaim) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = O> + Send + 'static,
        O: Into<GraceCommit> + Send + 'static,
    {
        let delay_ms = clamp_undo_delay_ms(delay_ms);
        let delete_at = intent_core::iso_ms_from_now(delay_ms);
        self.arm_owned(subject, delete_at, Some(delay_ms), delete)
            .await
    }

    /// Re-arms a deadline a previous process stored (Postgres): the timer
    /// fires at the stored `delete_at`, at once when it already passed.
    pub(crate) async fn rearm_owned<F, Fut, O>(
        &self,
        subject: PendingDeleteSubject,
        delete_at: String,
        delete: F,
    ) -> Result<PendingDeleteSchedule>
    where
        F: FnOnce(PendingDeleteClaim) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = O> + Send + 'static,
        O: Into<GraceCommit> + Send + 'static,
    {
        self.arm_owned(subject, delete_at, None, delete).await
    }

    /// Arms `subject` at `delete_at`; `requested_delay_ms` is the delay that
    /// deadline was made from, used as is when the store keeps it.
    async fn arm_owned<F, Fut, O>(
        &self,
        subject: PendingDeleteSubject,
        delete_at: String,
        requested_delay_ms: Option<u64>,
        delete: F,
    ) -> Result<PendingDeleteSchedule>
    where
        F: FnOnce(PendingDeleteClaim) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = O> + Send + 'static,
        O: Into<GraceCommit> + Send + 'static,
    {
        let (attempt, fresh) = {
            let mut map = self
                .inner
                .entries
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(entry) = map.get(&subject.key()) {
                Self::check_subject(entry, &subject)?;
                (entry.attempt().clone(), false)
            } else {
                let (ready, _) = watch::channel(PreparationStatus::Preparing);
                let attempt = Arc::new(Attempt { subject, ready });
                map.insert(
                    attempt.subject.key(),
                    OwnedEntry::Preparing(attempt.clone()),
                );
                (attempt, true)
            }
        };
        if !fresh {
            return attempt
                .prepared()
                .await
                .map(|delete_at| PendingDeleteSchedule {
                    delete_at,
                    newly_armed: false,
                })
                .ok_or_else(|| Error::Internal("original pending deletion did not arm".into()));
        }
        let mut owner = PreparationOwner {
            inner: self.inner.clone(),
            attempt: attempt.clone(),
            guard: None,
            timer: None,
            armed: false,
        };
        owner.guard = Some(
            self.inner
                .store
                .begin_repository_pending_delete(&[attempt.subject.lifecycle_key()])
                .await?,
        );
        let (start, started) = oneshot::channel();
        let inner = Arc::downgrade(&self.inner);
        let timer_attempt = attempt.clone();
        // Neither spawning nor invoking the deletion closure occurs under a map lock.
        let stopping = self.tasks.clone();
        owner.timer = Some(
            self.tasks
                .spawn_draining(async move {
                    let Ok(deadline) = started.await else {
                        return;
                    };
                    tokio::select! {
                        biased;
                        () = stopping.closed() => return,
                        () = tokio::time::sleep_until(deadline) => {}
                    }
                    let Some(inner) = inner.upgrade() else {
                        return;
                    };
                    let Some((claim, handle)) = inner.take_armed(&timer_attempt) else {
                        return;
                    };
                    drop(handle); // This timer already owns the original claim.
                    // Commit once: only the process that claims the stored
                    // deadline commits (always granted on SQLite), and the
                    // row goes only after the delete ran, so a crash or
                    // takeover in between leaves a stale claim the next owner
                    // takes over. Gone means a cancel removed it or another
                    // process finished it; held means another live process
                    // is committing it.
                    // A transient store error is retried while this process
                    // still serves the tenant; `NotOwner` means another
                    // process does, and re-arms the row itself.
                    let (kind, id, _) = timer_attempt.subject.grace_parts();
                    loop {
                        match inner.store.claim_delete_grace(kind, id).await {
                            Ok(true) => {
                                // The stored deadline goes only once the
                                // delete completed (or its subject is gone).
                                // A delete that failed before its own durable
                                // record releases the claim and keeps the
                                // row, so the next start re-arms and retries
                                // it instead of losing it.
                                let reported: GraceCommit = delete(claim).await.into();
                                // A delete path can acknowledge without
                                // deleting (it read nothing it could act on),
                                // so check the store too: the row goes only
                                // when the subject is gone or a durable
                                // delete record hands it to the startup
                                // sweep. SQLite stores no row to settle.
                                let settled = matches!(reported, GraceCommit::Done)
                                    && (!inner.store.persists_delete_graces()
                                        || delete_settled(&inner.store, &timer_attempt.subject)
                                            .await
                                            .unwrap_or(false));
                                let finished = if settled {
                                    inner.store.finish_delete_grace(kind, id).await
                                } else {
                                    inner.store.release_delete_grace(kind, id).await
                                };
                                if let Err(e) = finished {
                                    // A stale claim re-arms on the next
                                    // start; a finished delete then finds its
                                    // subject gone and drops the row.
                                    tracing::warn!(kind, id, error = %e, "could not settle a committed delete deadline");
                                }
                                return;
                            }
                            Ok(false) => {
                                tracing::info!(kind, id, "pending delete cancelled, finished or being committed elsewhere; skipping");
                                return;
                            }
                            Err(Error::NotOwner(e)) => {
                                tracing::info!(kind, id, error = %e, "tenant served by another process; it commits this pending delete");
                                return;
                            }
                            Err(e) => {
                                tracing::warn!(kind, id, error = %e, "could not claim the stored delete deadline; retrying");
                            }
                        }
                        tokio::select! {
                            biased;
                            // The row stays for the next start to re-arm.
                            () = stopping.closed() => return,
                            () = tokio::time::sleep(GRACE_TAKE_RETRY) => {}
                        }
                    }
                })
                .ok_or_else(|| Error::Internal("pending deletion admission closed".into()))?,
        );
        // Store the deadline before the timer can fire (Postgres; a no-op on
        // SQLite). A deadline another process already stored wins, so a
        // re-schedule never extends it. On an error the preparation owner
        // drops the attempt and nothing is armed.
        let (kind, id, workspace_id) = attempt.subject.grace_parts();
        let requested_at = delete_at.clone();
        let delete_at = self
            .inner
            .store
            .record_delete_grace(&intent_store::DeleteGrace {
                subject_kind: kind.to_string(),
                subject_id: id.to_string(),
                workspace_id: workspace_id.as_str().to_string(),
                delete_at,
            })
            .await?;
        let delay_ms = match requested_delay_ms {
            Some(ms) if delete_at == requested_at => ms,
            _ => ms_until(&delete_at),
        };
        let (delete_at, timer_deadline) = {
            let mut map = self
                .inner
                .entries
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let key = attempt.subject.key();
            assert!(map
                .get(&key)
                .is_some_and(|entry| Arc::ptr_eq(entry.attempt(), &attempt)));
            let timer_deadline =
                tokio::time::Instant::now() + std::time::Duration::from_millis(delay_ms);
            map.insert(
                key,
                OwnedEntry::Armed(OwnedArmed {
                    attempt: attempt.clone(),
                    delete_at: delete_at.clone(),
                    guard: owner.guard.take(),
                    handle: owner.timer.take().expect("prepared timer"),
                }),
            );
            (delete_at, timer_deadline)
        };
        attempt
            .ready
            .send_replace(PreparationStatus::Armed(delete_at.clone()));
        owner.armed = true;
        // The timer cannot inspect the entry or call delete until publication.
        let _ = start.send(timer_deadline);
        Ok(PendingDeleteSchedule {
            delete_at,
            newly_armed: true,
        })
    }

    pub(crate) async fn take_for_cascade(
        &self,
        subject: &PendingDeleteSubject,
    ) -> Result<Option<PendingDeleteClaim>> {
        let Some(attempt) = self.original_attempt(subject)? else {
            return Ok(None);
        };
        if attempt.prepared().await.is_none() {
            return Ok(None);
        }
        let Some((claim, timer)) = self.inner.take_armed(&attempt) else {
            return Ok(None);
        };
        timer.abort();
        // The pending deletion is cancelled or superseded by an immediate
        // delete: drop its stored deadline so no process re-arms it. A
        // failure leaves a row that a later commit finds and runs, so log.
        let (kind, id, _) = attempt.subject.grace_parts();
        if let Err(e) = self.inner.store.take_delete_grace(kind, id).await {
            tracing::warn!(kind, id, error = %e, "could not drop the stored delete deadline");
        }
        Ok(Some(claim))
    }

    pub(crate) async fn cancel_owned(&self, subject: &PendingDeleteSubject) -> Result<bool> {
        let Some(attempt) = self.original_attempt(subject)? else {
            return Ok(false);
        };
        if attempt.prepared().await.is_none() {
            return Ok(false);
        }
        // Drop the stored deadline first: if that fails the cancel fails and
        // nothing changed (the timer stays armed), so no later process can
        // re-arm and commit a delete the user undid. Only an unclaimed row
        // is removed: a claimed one is being committed, too late to cancel.
        // `true` on Postgres means this call removed it, so the commit
        // (which must claim the row) will not run even if its timer fired.
        let (kind, id, _) = subject.grace_parts();
        let took_row = self.inner.store.cancel_delete_grace(kind, id).await?
            && self.inner.store.persists_delete_graces();
        let Some((claim, timer)) = self.inner.take_armed(&attempt) else {
            return Ok(took_row);
        };
        timer.abort();
        claim.settle_confirmed().await?;
        Ok(true)
    }

    pub(crate) async fn take_for_immediate_delete(
        &self,
        subject: PendingDeleteSubject,
    ) -> Result<PendingDeleteClaim> {
        if let Some(claim) = self.take_for_cascade(&subject).await? {
            return Ok(claim);
        }
        let guard = self
            .inner
            .store
            .begin_repository_pending_delete(&[subject.lifecycle_key()])
            .await?;
        Ok(PendingDeleteClaim { subject, guard })
    }
}

/// What a pending deletion's commit reports to the registry.
pub(crate) enum GraceCommit {
    /// The delete completed, or its subject is already gone: the stored
    /// deadline is removed.
    Done,
    /// The delete failed before it was durably under way: the stored
    /// deadline stays (its claim released) for the next start to retry.
    Retry,
}

/// A commit closure that reports nothing counts as done (the in-memory
/// behaviour: a failed commit was only logged).
impl From<()> for GraceCommit {
    fn from((): ()) -> Self {
        Self::Done
    }
}

/// Whether a committed delete is settled durably: its subject is gone from
/// the store, or a `delete_purge` record (its own, or its workspace's) hands
/// it to the startup sweep. A read error counts as unsettled.
async fn delete_settled(store: &Store, subject: &PendingDeleteSubject) -> Result<bool> {
    let gone = match subject {
        PendingDeleteSubject::Workspace(id) => {
            matches!(store.get_workspace(id).await, Err(Error::NotFound(_)))
        }
        PendingDeleteSubject::Agent { agent_id, .. } => matches!(
            store.get_agent_session_summary(agent_id).await,
            Err(Error::NotFound(_))
        ),
    };
    if gone {
        return Ok(true);
    }
    Ok(store
        .pending_deletes()
        .await?
        .iter()
        .any(|record| match (record, subject) {
            (PendingDelete::Workspace(w), PendingDeleteSubject::Workspace(id)) => w == id,
            (PendingDelete::Workspace(w), PendingDeleteSubject::Agent { workspace_id, .. }) => {
                w == workspace_id
            }
            (
                PendingDelete::Agent { agent_id: a, .. },
                PendingDeleteSubject::Agent { agent_id, .. },
            ) => a == agent_id,
            (PendingDelete::Agent { .. }, PendingDeleteSubject::Workspace(_)) => false,
        }))
}

/// A service delete's result as a [`GraceCommit`]: done when it succeeded
/// or its subject is already gone, retry on any other error.
pub(crate) fn commit_outcome<T>(result: &Result<T>) -> GraceCommit {
    match result {
        Ok(_) | Err(Error::NotFound(_)) => GraceCommit::Done,
        Err(_) => GraceCommit::Retry,
    }
}

/// Pause between attempts to take a stored grace deadline after a store
/// error, while this process still serves the tenant.
const GRACE_TAKE_RETRY: std::time::Duration = std::time::Duration::from_secs(1);

/// Milliseconds from now until the ISO `delete_at` (0 when it passed or
/// does not parse).
fn ms_until(delete_at: &str) -> u64 {
    intent_core::parse_iso(delete_at)
        .map(|at| (at - time::OffsetDateTime::now_utc()).whole_milliseconds())
        .and_then(|ms| u64::try_from(ms).ok())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests;
