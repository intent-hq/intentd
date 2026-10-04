//! Exact prepared encoded reservations. Directory, scratch and live work are separate owners.
use intent_core::note_source_session::{validate_id, wire, Control, Reason, Result, SessionError};
use serde_json::Value;

pub const CELL: usize = 4096;
pub const FIXED: usize = 112;
pub const RECEIPT_PAYLOAD: usize = 3677;

/// Borrowed identity and fixed-width fields; callers cannot create hidden retained map keys here.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Fields<'a> {
    pub class: u8,
    pub state: u8,
    pub reason: u8,
    pub flags: u32,
    pub principal: &'a str,
    pub root: &'a str,
    pub workspace: &'a str,
    pub digest: [u8; 32],
    pub epoch: [u8; 16],
    pub accept_until: i128,
    pub source_expiry: i128,
    pub sequence: u64,
    pub outstanding: u32,
}
fn bad() -> SessionError {
    SessionError::Identity
}
/// Ensure every admitted principal/root fits a replacement cleanup context and any workspace.
#[must_use]
pub fn context_fits(principal: &str, root: &str) -> bool {
    !root.is_empty()
        && root.len() <= 256
        && FIXED
            .checked_add(principal.len())
            .and_then(|n| n.checked_add(root.len()))
            .and_then(|n| n.checked_add(256))
            .is_some_and(|n| n <= CELL)
}
impl Fields<'_> {
    fn validate(&self) -> Result<()> {
        if !(1..=4).contains(&self.class)
            || self.state > if self.class <= 2 { 8 } else { 6 }
            || self.reason > 3
            || self.flags > 1023
            || self.root.is_empty()
            || self.root.len() > 256
            || self.workspace.len() > 256
        {
            return Err(bad());
        }
        if (self.flags & 1 == 0 && !self.principal.is_empty())
            || (self.flags & 2 == 0 && !self.workspace.is_empty())
            || (self.flags & 4 == 0 && self.digest != [0; 32])
            || (self.flags & 8 == 0 && self.epoch != [0; 16])
            || (self.flags & 16 == 0 && self.accept_until != 0)
            || (self.flags & 32 == 0 && self.source_expiry != 0)
            || (self.flags & 64 == 0 && self.sequence != 0)
        {
            return Err(bad());
        }
        Ok(())
    }
}
/// One fixed metadata cell. No variable-sized identity remains alongside it.
pub struct Metadata {
    bytes: Box<[u8; CELL]>,
}
impl Metadata {
    /// Encode every integer big-endian and preserve every original UTF-8 identity byte.
    /// # Errors
    /// Rejects invalid fields or a logical length exceeding4096 before allocating the cell.
    pub fn new(v: Fields<'_>) -> Result<Self> {
        v.validate()?;
        let n = FIXED
            .checked_add(v.principal.len())
            .and_then(|n| n.checked_add(v.root.len()))
            .and_then(|n| n.checked_add(v.workspace.len()))
            .ok_or(SessionError::Capacity)?;
        if n > CELL {
            return Err(SessionError::Capacity);
        }
        let mut b = Box::new([0; CELL]);
        b[0] = 1;
        b[1] = v.class;
        b[2] = v.state;
        b[3] = v.reason;
        b[4..8].copy_from_slice(&v.flags.to_be_bytes());
        let mut p = 8;
        for s in [v.principal, v.root, v.workspace] {
            let len = u32::try_from(s.len()).map_err(|_| bad())?;
            b[p..p + 4].copy_from_slice(&len.to_be_bytes());
            p += 4;
            b[p..p + s.len()].copy_from_slice(s.as_bytes());
            p += s.len();
        }
        b[p..p + 32].copy_from_slice(&v.digest);
        b[p + 32..p + 48].copy_from_slice(&v.epoch);
        b[p + 48..p + 64].copy_from_slice(&v.accept_until.to_be_bytes());
        b[p + 64..p + 80].copy_from_slice(&v.source_expiry.to_be_bytes());
        b[p + 80..p + 88].copy_from_slice(&v.sequence.to_be_bytes());
        b[p + 88..p + 92].copy_from_slice(&v.outstanding.to_be_bytes());
        Ok(Self { bytes: b })
    }
    /// Decode borrowed text without BOM stripping, normalization or identity allocation.
    /// # Errors
    /// Rejects invalid UTF-8, length arithmetic, tags, absence bits or nonzero tail.
    pub fn fields(&self) -> Result<Fields<'_>> {
        decode(&self.bytes)
    }
    #[must_use]
    pub fn bytes(&self) -> &[u8; CELL] {
        &self.bytes
    }
    /// Change only already-reserved fixed state; identity/class/deadlines remain immutable.
    /// # Errors
    /// Rejects invalid tags/flags before any byte mutation.
    pub(super) fn progress(
        &mut self,
        state: u8,
        reason: u8,
        flags: u32,
        sequence: u64,
        outstanding: u32,
    ) -> Result<()> {
        let mut v = self.fields()?;
        let allowed = if v.class >= 3 {
            match v.state {
                0 => matches!(state, 0 | 1 | 3 | 4),
                1 | 2 => matches!(state, 1..=4),
                3 => matches!(state, 3 | 4),
                4..=6 => {
                    state == v.state
                        && flags == v.flags
                        && sequence == v.sequence
                        && outstanding == v.outstanding
                }
                _ => false,
            }
        } else {
            state >= v.state && !(v.state == 7 && state != 7)
        };
        if !allowed || reason != v.reason || (flags & 127) != (v.flags & 127) {
            return Err(bad());
        }
        v.state = state;
        v.reason = reason;
        v.flags = flags;
        v.sequence = sequence;
        v.outstanding = outstanding;
        v.validate()?;
        let p = 20 + v.principal.len() + v.root.len() + v.workspace.len();
        self.bytes[2] = state;
        self.bytes[3] = reason;
        self.bytes[4..8].copy_from_slice(&flags.to_be_bytes());
        self.bytes[p + 80..p + 88].copy_from_slice(&sequence.to_be_bytes());
        self.bytes[p + 88..p + 92].copy_from_slice(&outstanding.to_be_bytes());
        Ok(())
    }
}
fn take<'a>(b: &'a [u8], p: &mut usize, n: usize) -> Result<&'a [u8]> {
    let end = p.checked_add(n).ok_or_else(bad)?;
    let s = b.get(*p..end).ok_or_else(bad)?;
    *p = end;
    Ok(s)
}
fn fixed<const N: usize>(b: &[u8], p: &mut usize) -> Result<[u8; N]> {
    take(b, p, N)?.try_into().map_err(|_| bad())
}
fn decode(b: &[u8; CELL]) -> Result<Fields<'_>> {
    if b[0] != 1 {
        return Err(bad());
    }
    let mut p = 8;
    let mut text = [""; 3];
    for s in &mut text {
        let n = u32::from_be_bytes(fixed(b, &mut p)?);
        *s = std::str::from_utf8(take(b, &mut p, usize::try_from(n).map_err(|_| bad())?)?)
            .map_err(|_| bad())?;
    }
    let v = Fields {
        class: b[1],
        state: b[2],
        reason: b[3],
        flags: u32::from_be_bytes(b[4..8].try_into().map_err(|_| bad())?),
        principal: text[0],
        root: text[1],
        workspace: text[2],
        digest: fixed(b, &mut p)?,
        epoch: fixed(b, &mut p)?,
        accept_until: i128::from_be_bytes(fixed(b, &mut p)?),
        source_expiry: i128::from_be_bytes(fixed(b, &mut p)?),
        sequence: u64::from_be_bytes(fixed(b, &mut p)?),
        outstanding: u32::from_be_bytes(fixed(b, &mut p)?),
    };
    if b[p..].iter().any(|&b| b != 0) {
        return Err(bad());
    }
    v.validate()?;
    Ok(v)
}

/// Initially empty, precharged terminal history. Only exact validated Settled can publish.
pub struct Receipt {
    bytes: Box<[u8; CELL]>,
}
impl Default for Receipt {
    fn default() -> Self {
        Self {
            bytes: Box::new([0; CELL]),
        }
    }
}
impl Receipt {
    #[must_use]
    pub fn bytes(&self) -> &[u8; CELL] {
        &self.bytes
    }
    /// Publish under the operation registry lock after all owned work has retired.
    /// # Errors
    /// Rejects nonterminal/extra-field/mismatched DTO, uncertain or busy metadata, and rewrites.
    pub fn publish(&mut self, metadata: &mut Metadata, result: Value) -> Result<()> {
        let c: Control = serde_json::from_value(result).map_err(|_| bad())?;
        c.validate()?;
        let Control::Settled {
            operation_id,
            reason,
        } = c
        else {
            return Err(bad());
        };
        let v = metadata.fields()?;
        if v.class < 3
            || !matches!(
                (v.state, reason),
                (6, Reason::Cancelled) | (3, Reason::Closed | Reason::Expired)
            )
            || v.outstanding != 0
            || v.flags & 0x0380 != 0
            || self.bytes[..4] != [0; 4]
            || hex(&v.digest) != operation_id
        {
            return Err(bad());
        }
        let c = Control::Settled {
            operation_id,
            reason,
        };
        let raw = intent_core::note_artifact::canonical::canonical_json(
            &serde_json::to_string(&c).map_err(|_| bad())?,
        )
        .map_err(|_| bad())?;
        if raw.len() > RECEIPT_PAYLOAD {
            return Err(SessionError::Budget);
        }
        let n = u32::try_from(raw.len()).map_err(|_| bad())?;
        // All validation has completed before the immutable receipt or terminal state changes.
        let reason_tag = match reason {
            Reason::Cancelled => 1,
            Reason::Closed => 2,
            Reason::Expired => 3,
        };
        let next = if reason == Reason::Cancelled { 6 } else { 5 };
        self.bytes[4..4 + raw.len()].copy_from_slice(raw.as_bytes());
        self.bytes[..4].copy_from_slice(&n.to_be_bytes());
        // Payload precedes terminal state under the caller's registry lock.
        metadata.bytes[2] = next;
        metadata.bytes[3] = reason_tag;
        Ok(())
    }
    /// Return a borrowed canonical result for the current request's independently owned frame.
    /// # Errors
    /// Rejects uncommitted/malformed/noncanonical receipt bytes, including a leading BOM.
    pub fn payload(&self) -> Result<&str> {
        let n = usize::try_from(u32::from_be_bytes(
            self.bytes[..4].try_into().map_err(|_| bad())?,
        ))
        .map_err(|_| bad())?;
        if n == 0 || n > RECEIPT_PAYLOAD || self.bytes[4 + n..].iter().any(|&b| b != 0) {
            return Err(bad());
        }
        let raw = std::str::from_utf8(&self.bytes[4..4 + n]).map_err(|_| bad())?;
        let canonical =
            intent_core::note_artifact::canonical::canonical_json(raw).map_err(|_| bad())?;
        if canonical != raw {
            return Err(bad());
        }
        Ok(raw)
    }
    /// Bound the complete escaped envelope, excluding the receipt's private4-byte prefix.
    /// # Errors
    /// Rejects invalid history/ID or an envelope above4096 bytes.
    pub fn frame(&self, id: &Value) -> Result<String> {
        validate_id(id)?;
        let result: Value = serde_json::from_str(self.payload()?).map_err(|_| bad())?;
        wire::bounded(
            &serde_json::json!({"jsonrpc":"2.0","id":id,"result":result}),
            CELL,
        )
    }
}
#[must_use]
pub fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(out, "{b:02x}");
    }
    out
}
/// Decode a canonical lowercase digest into its fixed-width representation.
/// # Errors
/// Rejects any other spelling or length.
pub fn digest(text: &str) -> Result<[u8; 32]> {
    if text.len() != 64 {
        return Err(bad());
    }
    let mut out = [0; 32];
    for (dst, pair) in out.iter_mut().zip(text.as_bytes().chunks_exact(2)) {
        let digit = |b| match b {
            b'0'..=b'9' => Ok(b - b'0'),
            b'a'..=b'f' => Ok(b - b'a' + 10),
            _ => Err(bad()),
        };
        *dst = digit(pair[0])? * 16 + digit(pair[1])?;
    }
    Ok(out)
}
#[cfg(test)]
mod tests;
