//! Prepared source lifecycle vocabulary. Shape/integrity checks confer no authority.
use crate::{
    note_artifact::{canonical, request::Primitive},
    note_page::{NotePageRequest, NoteScope},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const REQUEST_BYTES: usize = 65_536;
pub const DESCRIPTOR_BYTES: usize = 8192;
pub const CONTROL_BYTES: usize = 4096;

/// Deliberately source-free errors for the isolated prepared composition.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionError {
    Invalid,
    Identity,
    Sequence,
    Unavailable,
    Expired,
    Budget,
    Stale,
    NotFound,
    Capacity,
    Uncertain,
}
impl SessionError {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::Invalid => "invalid-params",
            Self::Identity => "source-session-identity",
            Self::Sequence => "source-session-sequence",
            Self::Unavailable => "source-session-unavailable",
            Self::Expired => "note-page-expired",
            Self::Budget => "note-page-budget",
            Self::Stale => "note-page-stale",
            Self::NotFound => "not-found",
            Self::Capacity => "source-session-capacity",
            Self::Uncertain => "source-session-uncertain",
        }
    }
    #[must_use]
    pub const fn number(self) -> i32 {
        match self {
            Self::Stale => -32005,
            Self::Capacity => -32011,
            Self::Uncertain => -32603,
            _ => -32602,
        }
    }
}
pub type Result<T> = std::result::Result<T, SessionError>;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Binding {
    pub scope: NoteScope,
    pub snapshot_id: String,
    pub source_revision: String,
    pub primitive: Primitive,
    pub owner_ref: String,
    pub source_ref: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Descriptor {
    pub nonce: String,
    pub daemon_incarnation: String,
    pub workspace_id: String,
    pub binding: Binding,
    pub accept_until: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Operation {
    pub descriptor: Descriptor,
    pub operation_id: String,
}
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Read {
    pub workspace_id: String,
    pub operation_id: String,
    #[serde(deserialize_with = "safe_u64")]
    pub sequence: u64,
    pub request: PageRequest,
}

// Internally tagged variants forbid null optional refs and mixed request kinds.
#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase", deny_unknown_fields)]
pub enum PageRequest {
    #[serde(rename_all = "camelCase")]
    Context {
        context_ref: String,
        #[serde(deserialize_with = "safe_u64")]
        max_items: u64,
        #[serde(deserialize_with = "safe_usize")]
        max_wire_bytes: usize,
    },
    #[serde(rename_all = "camelCase")]
    Metadata {
        #[serde(rename = "ref")]
        reference: String,
        #[serde(default, deserialize_with = "present_cursor")]
        cursor: Option<String>,
        #[serde(deserialize_with = "safe_u64")]
        max_items: u64,
        #[serde(deserialize_with = "safe_usize")]
        max_wire_bytes: usize,
    },
}
fn present_cursor<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> std::result::Result<Option<String>, D::Error> {
    String::deserialize(d).map(Some)
}
impl PageRequest {
    /// Validate the read subset before operation admission.
    /// # Errors
    /// Rejects invalid refs or unsupported item/wire budgets.
    pub fn validate(&self) -> Result<()> {
        let (items, wire) = match self {
            Self::Context {
                context_ref,
                max_items,
                max_wire_bytes,
            } => {
                token(context_ref)?;
                (*max_items, *max_wire_bytes)
            }
            Self::Metadata {
                reference,
                cursor,
                max_items,
                max_wire_bytes,
            } => {
                token(reference)?;
                if let Some(v) = cursor {
                    token(v)?;
                }
                (*max_items, *max_wire_bytes)
            }
        };
        if items != 1 || !(4096..=8192).contains(&wire) {
            return Err(SessionError::Budget);
        }
        Ok(())
    }

    /// Convert only the bounded source subset, without any snapshot/page substitution fields.
    /// # Errors
    /// Rejects empty/oversized refs and unsupported item or wire budgets.
    pub fn into_page(self) -> Result<NotePageRequest> {
        let (kind, context_ref, reference, cursor, items, wire) = match self {
            Self::Context {
                context_ref,
                max_items,
                max_wire_bytes,
            } => {
                token(&context_ref)?;
                (
                    "context",
                    Some(context_ref),
                    None,
                    None,
                    max_items,
                    max_wire_bytes,
                )
            }
            Self::Metadata {
                reference,
                cursor,
                max_items,
                max_wire_bytes,
            } => {
                token(&reference)?;
                if let Some(v) = &cursor {
                    token(v)?;
                }
                (
                    "metadata",
                    None,
                    Some(reference),
                    cursor,
                    max_items,
                    max_wire_bytes,
                )
            }
        };
        if items != 1 || !(4096..=8192).contains(&wire) {
            return Err(SessionError::Budget);
        }
        Ok(NotePageRequest {
            kind: kind.into(),
            at: None,
            direction: None,
            cursor,
            snapshot_id: None,
            source_revision: None,
            note_instance_id: None,
            context_ref,
            reference,
            max_source_bytes: None,
            max_wire_bytes: Some(wire),
            max_items: Some(1),
        })
    }
}

/// Existing page ID policy, independently of no-NUL identifiers.
/// # Errors
/// Rejects null/composite/unsafe numeric IDs and strings above64 UTF-8 bytes.
pub fn validate_id(id: &Value) -> Result<()> {
    if id.as_str().is_some_and(|s| s.len() <= 64) || safe_integer(id).is_ok() {
        Ok(())
    } else {
        Err(SessionError::Invalid)
    }
}
/// Checked numeric value conversion, including integral decimal/exponent spellings.
/// # Errors
/// Rejects nonnumeric, nonfinite, fractional and unsafe values before integer conversion.
pub fn safe_integer(value: &Value) -> Result<i64> {
    let n = value.as_f64().ok_or(SessionError::Invalid)?;
    if !n.is_finite() || n.fract() != 0.0 || n.abs() > 9_007_199_254_740_991.0 {
        return Err(SessionError::Invalid);
    }
    // At most17 bounded scalar bytes. Formatting an already-checked integral
    // binary64 value avoids a truncating or saturating float-to-integer cast.
    format!("{n:.0}").parse().map_err(|_| SessionError::Invalid)
}
fn safe_u64<'de, D: serde::Deserializer<'de>>(d: D) -> std::result::Result<u64, D::Error> {
    let v = Value::deserialize(d)?;
    let n = safe_integer(&v).map_err(|_| serde::de::Error::custom("safe integer required"))?;
    u64::try_from(n).map_err(|_| serde::de::Error::custom("nonnegative integer required"))
}
fn safe_usize<'de, D: serde::Deserializer<'de>>(d: D) -> std::result::Result<usize, D::Error> {
    usize::try_from(safe_u64(d)?).map_err(|_| serde::de::Error::custom("bounded integer required"))
}
pub(crate) fn token(v: &str) -> Result<()> {
    if v.is_empty() || v.len() > 256 || v.contains('\0') {
        Err(SessionError::Invalid)
    } else {
        Ok(())
    }
}
pub(crate) fn hash(v: &str, n: usize) -> Result<()> {
    if v.len() == n
        && v.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        Ok(())
    } else {
        Err(SessionError::Identity)
    }
}
/// Parse an exact UTC timestamp while preserving its original string in the DTO.
/// # Errors
/// Rejects invalid, non-UTC or over64-byte timestamps.
pub fn instant(v: &str) -> Result<i128> {
    if v.is_empty() || v.len() > 64 || v.contains('\0') {
        return Err(SessionError::Invalid);
    }
    let t = crate::parse_iso(v).ok_or(SessionError::Invalid)?;
    if !t.offset().is_utc() {
        return Err(SessionError::Invalid);
    }
    Ok(t.unix_timestamp_nanos())
}
impl Operation {
    /// Validate immutable descriptor shape and its domain-separated content digest.
    /// # Errors
    /// Rejects extra fields during deserialization, invalid scope/text/digest or descriptor budget.
    pub fn validate(&self) -> Result<()> {
        let d = &self.descriptor;
        hash(&d.nonce, 32)?;
        hash(&self.operation_id, 64)?;
        for v in [
            &d.daemon_incarnation,
            &d.workspace_id,
            &d.binding.scope.backend_id,
            &d.binding.scope.workspace_id,
            &d.binding.scope.note_id,
            &d.binding.scope.note_instance_id,
            &d.binding.snapshot_id,
            &d.binding.source_revision,
            &d.binding.owner_ref,
            &d.binding.source_ref,
        ] {
            token(v)?;
        }
        if d.workspace_id != d.binding.scope.workspace_id {
            return Err(SessionError::Identity);
        }
        instant(&d.accept_until)?;
        let raw = serde_json::to_string(d).map_err(|_| SessionError::Invalid)?;
        let stored = canonical::canonical_json(&raw).map_err(|_| SessionError::Invalid)?;
        if stored.len() > DESCRIPTOR_BYTES {
            return Err(SessionError::Budget);
        }
        let envelope = serde_json::json!({"domain":"note.sourceSession.open.v1","descriptor":d});
        if canonical::digest(&envelope.to_string()).map_err(|_| SessionError::Invalid)?
            != self.operation_id
        {
            return Err(SessionError::Identity);
        }
        Ok(())
    }
    /// Return canonical descriptor bytes for the independently reserved descriptor cell.
    /// # Errors
    /// Rejects any invalid descriptor/digest before returning storage bytes.
    pub fn canonical_descriptor(&self) -> Result<String> {
        self.validate()?;
        canonical::canonical_json(
            &serde_json::to_string(&self.descriptor).map_err(|_| SessionError::Invalid)?,
        )
        .map_err(|_| SessionError::Invalid)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Reason {
    Cancelled,
    Closed,
    Expired,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
pub enum Control {
    #[serde(rename = "sourceSessionOpened", rename_all = "camelCase")]
    Opened {
        operation_id: String,
        daemon_incarnation: String,
        source_expires_at: String,
    },
    #[serde(rename = "sourceSessionAlreadyRegistered", rename_all = "camelCase")]
    AlreadyRegistered { operation_id: String },
    #[serde(rename = "sourceSessionClosing", rename_all = "camelCase")]
    Closing { operation_id: String },
    #[serde(rename = "sourceSessionUncertain", rename_all = "camelCase")]
    Uncertain { operation_id: String },
    #[serde(rename = "sourceSessionUnknown", rename_all = "camelCase")]
    Unknown { operation_id: String },
    #[serde(rename = "sourceSessionSettled", rename_all = "camelCase")]
    Settled {
        operation_id: String,
        reason: Reason,
    },
}
impl Control {
    #[must_use]
    pub fn operation_id(&self) -> &str {
        match self {
            Self::Opened { operation_id, .. }
            | Self::AlreadyRegistered { operation_id }
            | Self::Closing { operation_id }
            | Self::Uncertain { operation_id }
            | Self::Unknown { operation_id }
            | Self::Settled { operation_id, .. } => operation_id,
        }
    }
    /// Validate the exact result vocabulary; constructors do not grant settlement authority.
    /// # Errors
    /// Rejects malformed IDs and invalid opened-root/expiry fields.
    pub fn validate(&self) -> Result<()> {
        hash(self.operation_id(), 64)?;
        if let Self::Opened {
            daemon_incarnation,
            source_expires_at,
            ..
        } = self
        {
            token(daemon_incarnation)?;
            instant(source_expires_at)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
pub mod wire;
