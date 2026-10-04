//! TEST-ONLY retained authority and allocation-owner transition model.
//! The bounded dual checkpoint slots represent an already reserved owner record.
//! They simulate durable ordering/faults; they do not prove filesystem durability
//! or physical coverage, and no production API can accept this authority.
use super::{Error, Result};
use sha2::{Digest, Sha256};
use std::sync::{Arc, Mutex};

mod record;
use record::{Record, RECORD_BYTES};

fn denied() -> Error {
    Error::Internal("Artifact allocation owner refused transition".into())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Operation {
    Bootstrap = 1,
    Normal = 2,
    Recovery = 3,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum IoClass {
    Open = 1,
    Overwrite = 2,
    Preallocate = 3,
    Anonymous = 4,
}

pub(super) struct Permit {
    authority: Arc<Mutex<TestAuthority>>,
    identity: [u8; 32],
    epoch: u64,
    nonce: u64,
}

pub(super) struct IoPermit {
    operation: Permit,
    sequence: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Phase {
    Acquired = 0,
    Ready = 1,
    Quarantined = 2,
    Closed = 3,
    Retired = 4,
    Recovering = 5,
}

#[derive(Clone, Copy)]
pub(super) enum Fault {
    BeforeCheckpoint,
    TornCheckpoint,
}

pub(super) struct TestAuthority {
    instance: Arc<()>,
    identity: [u8; 32],
    pub(super) budget: u64,
    pub(super) bookkeeping: u64,
    pub(super) recovery: u64,
    pub(super) existing_charge: u64,
    pub(super) epoch: u64,
    pub(super) recovery_bound: Option<u64>,
    pub(super) fault: Option<Fault>,
    leased: bool,
    slots: [Option<[u8; RECORD_BYTES]>; 2],
    active: usize,
}

impl TestAuthority {
    pub(super) fn fresh(identity: [u8; 32]) -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(Self {
            instance: Arc::new(()),
            identity,
            budget: 4096,
            bookkeeping: (2 * RECORD_BYTES) as u64,
            recovery: 512,
            existing_charge: 0,
            epoch: 1,
            recovery_bound: None,
            fault: None,
            leased: false,
            slots: [None, None],
            active: 0,
        }))
    }

    fn checkpoint(&mut self, record: &Record) -> Result<()> {
        let target = 1 - self.active;
        let bytes = record.encode();
        match self.fault.take() {
            Some(Fault::BeforeCheckpoint) => return Err(denied()),
            Some(Fault::TornCheckpoint) => {
                let mut partial = [0; RECORD_BYTES];
                partial[..RECORD_BYTES / 2].copy_from_slice(&bytes[..RECORD_BYTES / 2]);
                self.slots[target] = Some(partial);
                return Err(denied());
            }
            None => {}
        }
        self.slots[target] = Some(bytes);
        self.active = target;
        Ok(())
    }

    /// Models authoritative confirmation that the previous process/IO retired.
    /// Merely dropping an Owner or a caller response cannot do this.
    pub(super) fn previous_process_retired(&mut self) {
        self.leased = false;
        self.epoch = self.epoch.checked_add(1).unwrap();
    }

    pub(super) fn confirm_test_reclamation(
        &self,
        physically_reclaimed: bool,
    ) -> Result<RetirementReceipt> {
        if !physically_reclaimed {
            return Err(denied());
        }
        let bytes = self.slots[self.active].as_ref().ok_or_else(denied)?;
        let record = Record::decode(bytes)?;
        if record.phase != Phase::Closed || record.epoch != self.epoch {
            return Err(denied());
        }
        Ok(RetirementReceipt {
            instance: self.instance.clone(),
            identity: self.identity,
            epoch: self.epoch,
            checkpoint: record.checkpoint,
        })
    }
}

pub(super) struct Handle {
    authority: Arc<Mutex<TestAuthority>>,
    identity: [u8; 32],
    epoch: u64,
    sequence: u64,
}

pub(super) struct Owner {
    live_handles: std::collections::BTreeSet<u64>,
    authority: Arc<Mutex<TestAuthority>>,
    record: Record,
}

impl Owner {
    pub(super) fn acquire(authority: Arc<Mutex<TestAuthority>>) -> Result<Self> {
        let mut backing = authority.lock().unwrap();
        if backing.leased || backing.bookkeeping < (2 * RECORD_BYTES) as u64 {
            return Err(denied());
        }
        let previous = backing
            .slots
            .iter()
            .flatten()
            .filter_map(|bytes| Record::decode(bytes).ok())
            .max_by_key(|record| record.checkpoint);
        if previous.is_none() && backing.slots.iter().any(Option::is_some) {
            return Err(denied());
        }
        let mut record = Record::new(
            backing.identity,
            backing.budget,
            backing.bookkeeping,
            backing.recovery,
            backing.epoch,
        );
        record.retained = backing.existing_charge;
        if let Some(old) = previous {
            if old.identity != backing.identity
                || old.budget != backing.budget
                || old.bookkeeping != backing.bookkeeping
                || old.epoch >= backing.epoch
            {
                return Err(denied());
            }
            // The prior persisted permit is a conservative upper bound even if
            // a completion checkpoint tore. Never free it by reopening.
            record.retained = old
                .retained
                .checked_add(old.pending)
                .and_then(|v| v.checked_add(old.available))
                .ok_or_else(denied)?;
            record.retained = record.retained.max(backing.existing_charge);
            record.checkpoint = old.checkpoint;
            record.recovery = old.recovery;
            record.phase = Phase::Quarantined;
        }
        record.check()?;
        backing.leased = true;
        drop(backing);
        let mut owner = Self {
            authority,
            record,
            live_handles: std::collections::BTreeSet::default(),
        };
        owner.persist()?;
        Ok(owner)
    }

    fn persist(&mut self) -> Result<()> {
        self.record.check()?;
        self.record.checkpoint = self.record.checkpoint.checked_add(1).ok_or_else(denied)?;
        if self
            .authority
            .lock()
            .unwrap()
            .checkpoint(&self.record)
            .is_err()
        {
            // No transition after this failure is admitted as normal work.
            // Prior durable pending/operation credit already covers delegated IO.
            self.record.phase = Phase::Quarantined;
            return Err(denied());
        }
        Ok(())
    }

    pub(super) fn begin(&mut self, kind: Operation, amount: u64) -> Result<Permit> {
        if amount == 0 || self.record.operation != 0 || self.record.io_active {
            return Err(denied());
        }
        let mut next = self.record.clone();
        match (kind, next.phase) {
            (Operation::Bootstrap, Phase::Acquired) | (Operation::Normal, Phase::Ready) => {}
            (Operation::Recovery, Phase::Quarantined) => {
                let bound = self
                    .authority
                    .lock()
                    .unwrap()
                    .recovery_bound
                    .ok_or_else(denied)?;
                if amount < bound || amount > next.recovery {
                    return Err(denied());
                }
                next.recovery -= amount;
                next.phase = Phase::Recovering;
            }
            _ => return Err(denied()),
        }
        next.available = amount;
        next.operation = kind as u64;
        next.nonce = next.nonce.checked_add(1).ok_or_else(denied)?;
        next.check()?;
        self.record = next;
        self.persist()?;
        self.current_operation()
    }

    pub(super) fn current_operation(&self) -> Result<Permit> {
        if self.record.operation == 0 {
            return Err(denied());
        }
        Ok(Permit {
            authority: self.authority.clone(),
            identity: self.record.identity,
            epoch: self.record.epoch,
            nonce: self.record.nonce,
        })
    }

    fn validate(&self, permit: &Permit) -> Result<()> {
        if !Arc::ptr_eq(&permit.authority, &self.authority)
            || permit.identity != self.record.identity
            || permit.epoch != self.record.epoch
            || permit.nonce != self.record.nonce
            || self.record.operation == 0
        {
            return Err(denied());
        }
        Ok(())
    }

    pub(super) fn start_io(
        &mut self,
        permit: &Permit,
        kind: IoClass,
        amount: u64,
    ) -> Result<IoPermit> {
        self.validate(permit)?;
        if self.record.io_active
            || self.record.pending != 0
            || amount == 0
            || amount > self.record.available
            || self.record.phase == Phase::Quarantined
            || (matches!(kind, IoClass::Open | IoClass::Anonymous) && self.live_handles.len() >= 16)
        {
            return Err(denied());
        }
        self.record.io_nonce = self.record.io_nonce.checked_add(1).ok_or_else(denied)?;
        self.record.available -= amount;
        self.record.pending = amount;
        self.record.io_active = true;
        self.record.io_kind = kind as u64;
        // Sublease credit, never add a second charge for the same reservation.
        self.persist()?;
        Ok(IoPermit {
            operation: Permit {
                authority: permit.authority.clone(),
                identity: permit.identity,
                epoch: permit.epoch,
                nonce: permit.nonce,
            },
            sequence: self.record.io_nonce,
        })
    }

    fn validate_io(&self, permit: &IoPermit) -> Result<()> {
        self.validate(&permit.operation)?;
        if !self.record.io_active || permit.sequence != self.record.io_nonce {
            return Err(denied());
        }
        Ok(())
    }

    pub(super) fn complete_io(
        &mut self,
        permit: &IoPermit,
        retained: u64,
        opened: bool,
    ) -> Result<()> {
        self.validate_io(permit)?;
        if !self.record.io_active
            || retained > self.record.pending
            || (opened
                && ![IoClass::Open as u64, IoClass::Anonymous as u64]
                    .contains(&self.record.io_kind))
        {
            return Err(denied());
        }
        let handles = if opened {
            self.record.handles.checked_add(1).ok_or_else(denied)?
        } else {
            self.record.handles
        };
        self.record.retained += retained;
        self.record.available += self.record.pending - retained;
        self.record.pending = 0;
        self.record.io_active = false;
        self.record.io_kind = 0;
        self.record.handles = handles;
        if opened {
            self.live_handles.insert(permit.sequence);
        }
        self.persist()
    }

    pub(super) fn partial_failure(
        &mut self,
        permit: &IoPermit,
        known_allocation: u64,
    ) -> Result<()> {
        self.validate_io(permit)?;
        if !self.record.io_active || known_allocation > self.record.pending {
            return Err(denied());
        }
        self.record.retained += known_allocation;
        self.record.pending -= known_allocation;
        self.record.phase = Phase::Quarantined;
        // Remaining uncertainty stays pending until physical IO settlement.
        self.persist()
    }

    pub(super) fn cancel(&mut self, permit: &Permit) -> Result<()> {
        self.validate(permit)?;
        self.record.phase = Phase::Quarantined;
        self.persist()
    }

    pub(super) fn settle_failed_io(&mut self, permit: &IoPermit) -> Result<()> {
        self.validate_io(permit)?;
        if self.record.phase != Phase::Quarantined || !self.record.io_active {
            return Err(denied());
        }
        self.record.retained += self.record.pending;
        self.record.pending = 0;
        self.record.io_active = false;
        self.record.io_kind = 0;
        self.persist()
    }

    pub(super) fn finish(&mut self, permit: &Permit) -> Result<()> {
        self.validate(permit)?;
        if self.record.pending != 0 || self.record.io_active {
            return Err(denied());
        }
        let mut next = self.record.clone();
        if next.operation == Operation::Recovery as u64 && next.phase == Phase::Recovering {
            next.recovery = self.authority.lock().unwrap().recovery;
            next.available = 0;
            next.phase = Phase::Acquired;
            // Having R<=B did not prove adequacy. Replenish the tested allowance
            // only if the now-retained charge leaves room inside the same B.
        } else if next.phase == Phase::Quarantined {
            next.retained += next.available;
            next.available = 0;
        } else {
            next.available = 0;
            next.phase = Phase::Ready;
        }
        next.operation = 0;
        next.check()?;
        self.record = next;
        self.persist()
    }

    pub(super) fn opened_handle(&self, io: &IoPermit) -> Result<Handle> {
        self.validate(&io.operation)?;
        if !self.live_handles.contains(&io.sequence) {
            return Err(denied());
        }
        Ok(Handle {
            authority: self.authority.clone(),
            identity: self.record.identity,
            epoch: self.record.epoch,
            sequence: io.sequence,
        })
    }

    // A test backend reports the physical outcome for this exact handle. A
    // repeated or foreign result cannot retire another live resource.
    pub(super) fn close_handle(&mut self, handle: &Handle, physically_closed: bool) -> Result<()> {
        if !Arc::ptr_eq(&handle.authority, &self.authority)
            || handle.identity != self.record.identity
            || handle.epoch != self.record.epoch
            || !self.live_handles.contains(&handle.sequence)
        {
            return Err(denied());
        }
        if !physically_closed {
            self.record.phase = Phase::Quarantined;
            return self.persist();
        }
        self.record.handles = self.record.handles.checked_sub(1).ok_or_else(denied)?;
        self.live_handles.remove(&handle.sequence);
        self.persist()
    }

    pub(super) fn close(&mut self) -> Result<()> {
        if self.record.handles != 0 || self.record.io_active || self.record.operation != 0 {
            return Err(denied());
        }
        self.record.phase = Phase::Closed;
        self.persist()
    }

    /// TEST authority supplies this receipt only after physical retirement in
    /// the model. Logical DELETE, release, close or rollback never calls this.
    pub(super) fn retire(&mut self, receipt: &RetirementReceipt) -> Result<()> {
        if self.record.phase != Phase::Closed
            || !Arc::ptr_eq(&receipt.instance, &self.authority.lock().unwrap().instance)
            || receipt.identity != self.record.identity
            || receipt.epoch != self.record.epoch
            || receipt.checkpoint != self.record.checkpoint
        {
            return Err(denied());
        }
        self.record.retained = 0;
        self.record.phase = Phase::Retired;
        self.persist()
    }

    pub(super) fn snapshot(&self) -> (u64, u64, u64, u64, u64, Phase) {
        (
            self.record.retained,
            self.record.pending,
            self.record.available,
            self.record.bookkeeping,
            self.record.recovery,
            self.record.phase,
        )
    }
}

pub(super) struct RetirementReceipt {
    instance: Arc<()>,
    identity: [u8; 32],
    epoch: u64,
    checkpoint: u64,
}
