//! Eligibility metadata for the existing daemon detail and suggestion caches.
//!
//! This module retains no payloads, pages, credentials, timestamps or errors.
//! Cache owners keep their existing bounds and freshness policies. They must
//! first admit the caller, then check this metadata before serving a payload.
//! `S` combines routing's execution and per-target connection scope DTOs;
//! `R` is its canonical resource DTO. This policy neither parses targets nor
//! grants access.
//!
//! Keep one slot per qualified resource, shared by detail and retained-summary
//! readers. Hold the cache lock across completion and payload replacement.
//! Retiring a connection/account/authority epoch invalidates all its slots.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

/// A cache key has no PR/MR alias or mutable workspace remote in its identity.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct ObservationKey<S, R> {
    pub(crate) scope: S,
    pub(crate) resource: R,
}

/// Server-owned lifetime of an admitted execution scope. Construct once per
/// connection generation, share across readers, and retire on replacement or
/// disconnect. The handle is not a caller-supplied authorization capability.
#[derive(Debug, Clone)]
pub(crate) struct ObservationScope<S> {
    key: S,
    active: Arc<AtomicBool>,
    denial: Arc<Mutex<Arc<()>>>,
}

/// Captured before a list await, before its individual resource IDs are known.
#[derive(Debug, Clone)]
pub(crate) struct ScopeTicket {
    active: Arc<AtomicBool>,
    denial: Arc<()>,
}

impl<S> ObservationScope<S> {
    pub(crate) fn new(key: S) -> Self {
        Self {
            key,
            active: Arc::new(AtomicBool::new(true)),
            denial: Arc::new(Mutex::new(Arc::new(()))),
        }
    }

    pub(crate) fn retire(&self) {
        self.active.store(false, Ordering::SeqCst);
    }

    pub(crate) fn is_active(&self) -> bool {
        self.active.load(Ordering::SeqCst)
    }

    pub(crate) fn key(&self) -> &S {
        &self.key
    }

    pub(crate) fn begin(&self) -> Result<ScopeTicket, Ineligible> {
        let ticket = ScopeTicket {
            active: self.active.clone(),
            denial: self.denial.lock().unwrap().clone(),
        };
        self.validate_owner(&ticket)?;
        Ok(ticket)
    }

    pub(crate) fn validate_owner(&self, ticket: &ScopeTicket) -> Result<(), Ineligible> {
        if !self.is_active() {
            return Err(Ineligible::RetiredScope);
        }
        if !Arc::ptr_eq(&self.active, &ticket.active) {
            return Err(Ineligible::DifferentSlot);
        }
        Ok(())
    }

    pub(crate) fn validate(&self, ticket: &ScopeTicket) -> Result<(), Ineligible> {
        self.validate_owner(ticket)?;
        if !Arc::ptr_eq(&self.denial.lock().unwrap(), &ticket.denial) {
            return Err(Ineligible::DeniedSinceRequest);
        }
        Ok(())
    }

    /// Invalidate the whole admitted authority/connection when the caller has
    /// that breadth of denial evidence. A single resource 404 uses slot.deny
    /// instead. Fresh reads in this still-current scope may recover access.
    pub(crate) fn deny(&self) {
        *self.denial.lock().unwrap() = Arc::new(());
    }
}

/// Summary success cannot make an old detail/snapshot fresh or eligible.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Coverage {
    Detail,
    Summary,
}

impl Coverage {
    fn index(self) -> usize {
        match self {
            Self::Detail => 0,
            Self::Summary => 1,
        }
    }
}

/// A request-start stamp, captured before any provider await. Private fields
/// prevent constructing a new-generation receipt from a late old response.
#[derive(Debug)]
pub(crate) struct ReadTicket {
    slot: Arc<()>,
    denial: Arc<()>,
    scope_denial: Arc<()>,
    sequence: u64,
    coverage: Coverage,
}

/// Store beside the payload; never persist this process-local eligibility.
#[derive(Debug)]
pub(crate) struct EligibleRead(ReadTicket);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Ineligible {
    RetiredScope,
    DifferentSlot,
    DeniedSinceRequest,
    OlderObservation,
    SequenceExhausted,
    HistoryExpired,
}

/// Per-resource eligibility shared by full reads and bounded list summaries.
/// Arc identities avoid generation reuse after eviction/reinsertion and do
/// not need an unbounded tombstone map. A denial replaces the denial token;
/// old readers keep the old token alive until they finish and cannot restore
/// eligibility, even after a fresh read has recovered access.
#[derive(Debug)]
pub(crate) struct ObservationSlot<S, R> {
    key: ObservationKey<S, R>,
    active: Arc<AtomicBool>,
    scope_denial: Arc<Mutex<Arc<()>>>,
    slot: Arc<()>,
    denial: Arc<()>,
    sequence: u64,
    accepted: [Option<u64>; 2],
    observed: [Option<u64>; 2],
}

impl<S: Clone, R> ObservationSlot<S, R> {
    pub(crate) fn new(scope: &ObservationScope<S>, resource: R) -> Self {
        Self {
            key: ObservationKey {
                scope: scope.key.clone(),
                resource,
            },
            active: scope.active.clone(),
            scope_denial: scope.denial.clone(),
            slot: Arc::new(()),
            denial: Arc::new(()),
            sequence: 0,
            accepted: [None; 2],
            observed: [None; 2],
        }
    }

    pub(crate) fn key(&self) -> &ObservationKey<S, R> {
        &self.key
    }

    pub(crate) fn begin(&mut self, coverage: Coverage) -> Result<ReadTicket, Ineligible> {
        self.ensure_active()?;
        self.sequence = self
            .sequence
            .checked_add(1)
            .ok_or(Ineligible::SequenceExhausted)?;
        Ok(ReadTicket {
            slot: self.slot.clone(),
            denial: self.denial.clone(),
            scope_denial: self.scope_denial.lock().unwrap().clone(),
            sequence: self.sequence,
            coverage,
        })
    }

    /// Only an authorized successful PRIMARY-resource read calls this.
    /// Optional-field availability alone is not primary success; preserve
    /// restrictions/unknown/pending state within an otherwise successful read.
    /// A receipt never asserts passing checks or readiness to merge. Failed
    /// reads go through failure, without refreshing old payload timestamps.
    pub(crate) fn success(&mut self, ticket: ReadTicket) -> Result<EligibleRead, Ineligible> {
        self.observe(&ticket)?;
        self.accepted[ticket.coverage.index()] = Some(ticket.sequence);
        Ok(EligibleRead(ticket))
    }

    /// A partial primary read fences older responses without minting a complete
    /// receipt or extending the existing payload's freshness or eligibility.
    pub(crate) fn observe(&mut self, ticket: &ReadTicket) -> Result<(), Ineligible> {
        self.validate(ticket)?;
        self.observed[ticket.coverage.index()] = Some(ticket.sequence);
        Ok(())
    }

    /// Validate a response without promoting partial evidence to a full read.
    pub(crate) fn validate(&self, ticket: &ReadTicket) -> Result<(), Ineligible> {
        self.ensure_slot(ticket)?;
        if !Arc::ptr_eq(&self.denial, &ticket.denial)
            || !Arc::ptr_eq(&self.scope_denial.lock().unwrap(), &ticket.scope_denial)
        {
            return Err(Ineligible::DeniedSinceRequest);
        }
        let observed = self.observed[ticket.coverage.index()];
        if observed.is_some_and(|sequence| sequence > ticket.sequence) {
            return Err(Ineligible::OlderObservation);
        }
        Ok(())
    }

    /// A classified primary-resource authentication/access denial, including
    /// an access-hiding 404, suppresses BOTH detail and summary eligibility.
    /// Even a late denial is newly observed evidence; its age does not turn it
    /// into success. Only results from a retired scope/slot are discarded.
    pub(crate) fn deny(&mut self, ticket: ReadTicket) -> Result<(), Ineligible> {
        self.ensure_slot(&ticket)?;
        drop(ticket);
        self.denial = Arc::new(());
        self.accepted = [None; 2];
        self.observed = [None; 2];
        Ok(())
    }

    /// Apply the PROVIDER'S purpose-aware classification and return the error
    /// unchanged. This policy deliberately does not infer access loss from a
    /// status code: the same 403 may describe an optional policy endpoint.
    /// Non-denial errors neither mint a receipt nor refresh a cached payload.
    /// Keep the adapter with the provider category owner; `E` can be its typed
    /// failure, including distinctions such as rate-limited or unknown.
    pub(crate) fn failure<E>(
        &mut self,
        ticket: ReadTicket,
        error: E,
        denies_primary_access: impl FnOnce(&E) -> bool,
    ) -> Result<E, Ineligible> {
        self.ensure_slot(&ticket)?;
        if denies_primary_access(&error) {
            self.deny(ticket)?;
        }
        Ok(error)
    }

    /// Must be combined with the existing freshness/retention check and the
    /// current caller's independently admitted scope on EVERY cache hit.
    pub(crate) fn can_serve(&self, read: &EligibleRead, scope: &ObservationScope<S>) -> bool {
        self.belongs_to(scope)
            && self.ensure_slot(&read.0).is_ok()
            && Arc::ptr_eq(&self.denial, &read.0.denial)
            && Arc::ptr_eq(&self.scope_denial.lock().unwrap(), &read.0.scope_denial)
            && self.accepted[read.0.coverage.index()] == Some(read.0.sequence)
    }

    pub(crate) fn belongs_to(&self, scope: &ObservationScope<S>) -> bool {
        Arc::ptr_eq(&self.active, &scope.active) && self.ensure_active().is_ok()
    }

    fn ensure_active(&self) -> Result<(), Ineligible> {
        if self.active.load(Ordering::SeqCst) {
            Ok(())
        } else {
            Err(Ineligible::RetiredScope)
        }
    }

    fn ensure_slot(&self, ticket: &ReadTicket) -> Result<(), Ineligible> {
        self.ensure_active()?;
        if Arc::ptr_eq(&self.slot, &ticket.slot) {
            Ok(())
        } else {
            Err(Ineligible::DifferentSlot)
        }
    }
}
