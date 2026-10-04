//! Fixed-size checkpoint codec. Its checksum detects corruption; it is not
//! authorization. Only the already-held TEST authority supplies these slots.
use super::{denied, Digest, Phase, Result, Sha256};

pub(super) const RECORD_BYTES: usize = 192;

#[derive(Clone)]
pub(super) struct Record {
    pub identity: [u8; 32],
    pub budget: u64,
    pub retained: u64,
    pub bookkeeping: u64,
    pub recovery: u64,
    pub available: u64,
    pub pending: u64,
    pub handles: u64,
    pub epoch: u64,
    pub nonce: u64,
    pub checkpoint: u64,
    pub phase: Phase,
    pub operation: u64,
    pub io_kind: u64,
    pub io_active: bool,
    pub io_nonce: u64,
}

impl Record {
    pub fn new(
        identity: [u8; 32],
        budget: u64,
        bookkeeping: u64,
        recovery: u64,
        epoch: u64,
    ) -> Self {
        Self {
            identity,
            budget,
            retained: 0,
            bookkeeping,
            recovery,
            available: 0,
            pending: 0,
            handles: 0,
            epoch,
            nonce: 0,
            checkpoint: 0,
            phase: Phase::Acquired,
            operation: 0,
            io_kind: 0,
            io_active: false,
            io_nonce: 0,
        }
    }

    pub fn check(&self) -> Result<()> {
        let total = [
            self.retained,
            self.pending,
            self.available,
            self.bookkeeping,
            self.recovery,
        ]
        .into_iter()
        .try_fold(0_u64, u64::checked_add)
        .ok_or_else(denied)?;
        if total > self.budget
            || self.budget == 0
            || self.epoch == 0
            || self.operation > 3
            || self.io_kind > 4
            || (self.operation == 0 && (self.available != 0 || self.pending != 0 || self.io_active))
            || (self.io_active != (self.io_kind != 0))
            || (self.io_active && self.io_nonce == 0)
            || (matches!(self.phase, Phase::Closed | Phase::Retired)
                && (self.handles != 0 || self.operation != 0))
            || (self.phase == Phase::Recovering && self.operation != 3)
            || (self.phase == Phase::Retired && self.retained != 0)
        {
            return Err(denied());
        }
        Ok(())
    }

    pub fn encode(&self) -> [u8; RECORD_BYTES] {
        let mut bytes = [0; RECORD_BYTES];
        bytes[..32].copy_from_slice(&self.identity);
        let fields = [
            1,
            self.budget,
            self.retained,
            self.bookkeeping,
            self.recovery,
            self.available,
            self.pending,
            self.handles,
            self.epoch,
            self.nonce,
            self.checkpoint,
            self.phase as u64,
            self.operation,
            self.io_kind,
            u64::from(self.io_active),
            self.io_nonce,
        ];
        for (field, out) in fields.into_iter().zip(bytes[32..160].chunks_exact_mut(8)) {
            out.copy_from_slice(&field.to_be_bytes());
        }
        let digest = Sha256::digest(&bytes[..160]);
        bytes[160..].copy_from_slice(&digest);
        bytes
    }

    pub fn decode(bytes: &[u8; RECORD_BYTES]) -> Result<Self> {
        if Sha256::digest(&bytes[..160])[..] != bytes[160..] {
            return Err(denied());
        }
        let mut fields = [0_u64; 16];
        for (out, field) in fields.iter_mut().zip(bytes[32..160].chunks_exact(8)) {
            *out = u64::from_be_bytes(field.try_into().map_err(|_| denied())?);
        }
        if fields[0] != 1 || fields[14] > 1 {
            return Err(denied());
        }
        let phase = match fields[11] {
            0 => Phase::Acquired,
            1 => Phase::Ready,
            2 => Phase::Quarantined,
            3 => Phase::Closed,
            4 => Phase::Retired,
            5 => Phase::Recovering,
            _ => return Err(denied()),
        };
        let record = Self {
            identity: bytes[..32].try_into().map_err(|_| denied())?,
            budget: fields[1],
            retained: fields[2],
            bookkeeping: fields[3],
            recovery: fields[4],
            available: fields[5],
            pending: fields[6],
            handles: fields[7],
            epoch: fields[8],
            nonce: fields[9],
            checkpoint: fields[10],
            phase,
            operation: fields[12],
            io_kind: fields[13],
            io_active: fields[14] == 1,
            io_nonce: fields[15],
        };
        record.check()?;
        Ok(record)
    }
}
