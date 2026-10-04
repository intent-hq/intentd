//! Fixed encoded histories and weak directory. No session/Services owner cycle.
use super::{
    clock::AcceptanceClock,
    metadata::{self, Fields, Metadata, Receipt},
    owner::Owner,
};
use intent_core::note_source_session::{Control, Operation, Reason, Result, SessionError};
use intent_store::CanonicalSourceHold;
use std::{
    sync::{Arc, Mutex, Weak},
    time::Instant,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Slot {
    pub index: usize,
    pub generation: u64,
}
pub(super) struct Entry {
    pub slot: Slot,
    pub metadata: Metadata,
    pub descriptor: Box<[u8; 8192]>,
    pub descriptor_len: usize,
    pub receipt: Receipt,
    pub hold: Option<CanonicalSourceHold>,
    pub owner: Weak<Owner>,
    pub close_reason: Reason,
    pub transport_pending: bool,
}
pub(super) struct Directory {
    pub entries: [Option<Entry>; 256],
    next_generation: u64,
    pub clock: AcceptanceClock,
}
pub struct Registry {
    pub(super) incarnation: String,
    pub(super) state: Mutex<Directory>,
    boot: Instant,
    #[cfg(test)]
    pub(super) close_blocked: Mutex<Option<std::sync::mpsc::SyncSender<()>>>,
}
impl Registry {
    pub(super) fn new(incarnation: String) -> Result<Arc<Self>> {
        Ok(Arc::new(Self {
            incarnation,
            state: Mutex::new(Directory {
                entries: std::array::from_fn(|_| None),
                next_generation: 0,
                clock: AcceptanceClock::new(
                    time::OffsetDateTime::now_utc().unix_timestamp_nanos(),
                    0,
                )?,
            }),
            boot: Instant::now(),
            #[cfg(test)]
            close_blocked: Mutex::new(None),
        }))
    }
    pub(super) fn observe(&self, d: &mut Directory) -> Result<i128> {
        let elapsed =
            i128::try_from(self.boot.elapsed().as_nanos()).map_err(|_| SessionError::Uncertain)?;
        d.clock.observe(
            time::OffsetDateTime::now_utc().unix_timestamp_nanos(),
            elapsed,
        )
    }
    pub(super) fn uncertain(&self, slot: Slot) {
        if let Ok(mut d) = self.state.lock() {
            if let Some(e) = d.get_mut(slot) {
                if let Ok(v) = e.metadata.fields() {
                    if v.state < 4 {
                        let _ =
                            e.metadata
                                .progress(4, v.reason, v.flags, v.sequence, v.outstanding);
                    }
                }
            }
        }
    }
    pub(super) fn close_state(&self, slot: Slot, reason: Reason) -> Result<()> {
        #[cfg(test)]
        if let Some(observe) = self.close_blocked.lock().unwrap().clone() {
            if matches!(
                self.state.try_lock(),
                Err(std::sync::TryLockError::WouldBlock)
            ) {
                let _ = observe.try_send(());
            }
        }

        let mut d = self.state.lock().map_err(|_| SessionError::Uncertain)?;
        let e = d.get_mut(slot).ok_or(SessionError::Unavailable)?;
        let v = e.metadata.fields()?;
        if v.state < 3 {
            e.metadata
                .progress(3, v.reason, v.flags, v.sequence, v.outstanding)?;
            e.close_reason = reason;
        }
        Ok(())
    }
    pub(super) fn close_result(&self, slot: Slot) -> Result<Control> {
        let d = self.state.lock().map_err(|_| SessionError::Uncertain)?;
        let e = d.get(slot).ok_or(SessionError::Uncertain)?;
        e.close_result()
    }
    pub(super) fn settle(&self, slot: Slot) -> Result<()> {
        let mut d = self.state.lock().map_err(|_| SessionError::Uncertain)?;
        let e = d.get_mut(slot).ok_or(SessionError::Uncertain)?;
        let v = e.metadata.fields()?;
        if v.state == 4 {
            return Err(SessionError::Uncertain);
        }
        if v.state >= 5 {
            return Ok(());
        }
        if v.state != 3 || v.outstanding != 0 || v.flags & 0x0380 != 0 || e.transport_pending {
            return Err(SessionError::Uncertain);
        }
        let result = Control::Settled {
            operation_id: metadata::hex(&v.digest),
            reason: e.close_reason,
        };
        e.receipt.publish(
            &mut e.metadata,
            serde_json::to_value(result).map_err(|_| SessionError::Uncertain)?,
        )?;
        e.hold.take();
        Ok(())
    }
}
impl Directory {
    pub(super) fn get(&self, s: Slot) -> Option<&Entry> {
        self.entries.get(s.index)?.as_ref().filter(|e| e.slot == s)
    }
    pub(super) fn get_mut(&mut self, s: Slot) -> Option<&mut Entry> {
        self.entries
            .get_mut(s.index)?
            .as_mut()
            .filter(|e| e.slot == s)
    }
    pub(super) fn find(
        &self,
        principal: &str,
        root: &str,
        workspace: &str,
        digest: &[u8; 32],
    ) -> Result<Option<Slot>> {
        for e in self.entries.iter().flatten() {
            let v = e.metadata.fields()?;
            if v.principal == principal
                && v.root == root
                && v.workspace == workspace
                && &v.digest == digest
            {
                return Ok(Some(e.slot));
            }
        }
        Ok(None)
    }
    pub(super) fn purge(&mut self) {
        let now = self.clock.high();
        for entry in &mut self.entries {
            let remove = entry.as_ref().is_some_and(|e| {
                !e.transport_pending
                    && e.metadata.fields().is_ok_and(|v| {
                        matches!(v.state, 5 | 6)
                            && v.outstanding == 0
                            && v.flags & 0x0380 == 0
                            && now >= v.accept_until
                    })
                    && e.receipt.payload().is_ok()
            });
            if remove {
                entry.take();
            }
        }
    }
    // The caller holds the root lock and owns a context-admitted, signed snapshot hold.
    pub(super) fn reserve(
        &mut self,
        principal: &str,
        epoch: [u8; 16],
        op: &Operation,
        hold: CanonicalSourceHold,
        cancellation: bool,
    ) -> Result<Slot> {
        let d = &op.descriptor;
        if !metadata::context_fits(principal, &d.daemon_incarnation) {
            return Err(SessionError::Capacity);
        }
        let accept = intent_core::note_source_session::instant(&d.accept_until)?;
        let expiry = intent_core::note_source_session::instant(hold.expires_at())?;
        if self.clock.high() >= accept || accept > expiry {
            return Err(SessionError::Expired);
        }
        let raw = op.canonical_descriptor()?;
        let range = if cancellation { 240..256 } else { 0..240 };
        let index = range
            .into_iter()
            .find(|i| self.entries[*i].is_none())
            .ok_or(SessionError::Capacity)?;
        let generation = self
            .next_generation
            .checked_add(1)
            .ok_or(SessionError::Capacity)?;
        let slot = Slot { index, generation };
        let metadata = Metadata::new(Fields {
            class: if cancellation { 4 } else { 3 },
            state: if cancellation { 6 } else { 0 },
            reason: 0,
            flags: 127 | if cancellation { 0 } else { 128 },
            principal,
            root: &d.daemon_incarnation,
            workspace: &d.workspace_id,
            digest: metadata::digest(&op.operation_id)?,
            epoch,
            accept_until: accept,
            source_expiry: expiry,
            sequence: 0,
            outstanding: u32::from(!cancellation),
        })?;
        let mut descriptor = Box::new([0; 8192]);
        descriptor[..raw.len()].copy_from_slice(raw.as_bytes());
        let mut entry = Entry {
            slot,
            metadata,
            descriptor,
            descriptor_len: raw.len(),
            receipt: Receipt::default(),
            hold: Some(hold),
            owner: Weak::new(),
            close_reason: Reason::Closed,
            transport_pending: false,
        };
        if cancellation {
            entry.receipt.publish(
                &mut entry.metadata,
                serde_json::to_value(Control::Settled {
                    operation_id: op.operation_id.clone(),
                    reason: Reason::Cancelled,
                })
                .map_err(|_| SessionError::Uncertain)?,
            )?;
            entry.hold.take();
        }
        self.next_generation = generation;
        self.entries[index] = Some(entry);
        Ok(slot)
    }
}
impl Entry {
    pub(super) fn matches_descriptor(&self, op: &Operation) -> Result<bool> {
        Ok(self.descriptor[..self.descriptor_len] == *op.canonical_descriptor()?.as_bytes())
    }
    pub(super) fn close_result(&self) -> Result<Control> {
        let v = self.metadata.fields()?;
        let operation_id = metadata::hex(&v.digest);
        match v.state {
            4 => Ok(Control::Uncertain { operation_id }),
            5 | 6 => {
                serde_json::from_str(self.receipt.payload()?).map_err(|_| SessionError::Uncertain)
            }
            _ => Ok(Control::Closing { operation_id }),
        }
    }
}
impl Registry {
    pub(super) fn operation_id(&self, slot: Slot) -> Result<String> {
        let d = self.state.lock().map_err(|_| SessionError::Uncertain)?;
        Ok(metadata::hex(
            &d.get(slot)
                .ok_or(SessionError::Uncertain)?
                .metadata
                .fields()?
                .digest,
        ))
    }
    pub(super) fn consume(&self, slot: Slot) -> Result<()> {
        let mut d = self.state.lock().map_err(|_| SessionError::Uncertain)?;
        let e = d.get_mut(slot).ok_or(SessionError::Uncertain)?;
        let v = e.metadata.fields()?;
        if v.outstanding != 0 || v.flags & 0x0180 != 0 || v.state >= 4 {
            return Err(SessionError::Uncertain);
        }
        e.metadata
            .progress(v.state, v.reason, v.flags & !512, v.sequence, 0)
    }
    pub(super) fn delivered(&self, slot: Slot, sequence: u64, consumer: bool) -> Result<()> {
        let mut d = self.state.lock().map_err(|_| SessionError::Uncertain)?;
        let e = d.get_mut(slot).ok_or(SessionError::Uncertain)?;
        let v = e.metadata.fields()?;
        if v.state >= 4 || v.outstanding != 1 || v.sequence != sequence {
            return Err(SessionError::Uncertain);
        }
        let state = if v.state == 3 { 3 } else { 1 };
        e.metadata.progress(
            state,
            v.reason,
            127 | if consumer { 512 } else { 0 },
            v.sequence,
            0,
        )
    }
}
