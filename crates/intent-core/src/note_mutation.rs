//! Revision-checked partial note mutations. Validation and caller edits are pure;
//! admission, canonical effects and receipts must commit together in the Store.
use crate::note_page::{NoteScope, SOURCE_BYTES};
use serde::{Deserialize, Serialize};
use time::{format_description::well_known::Rfc3339, OffsetDateTime};

mod provenance;
mod status;
pub use provenance::NoteSourceHistory;
pub use status::NoteOperationStatusQuery;

const SAFE_INTEGER: u64 = 9_007_199_254_740_991;
/// Maximum number of inline replacements, all addressed to the same base.
pub const MAX_SPLICES: usize = 32;

/// An exact half-open UTF-16 replacement in the original source.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NoteSplice {
    pub start: u64,
    pub end: u64,
    pub text: String,
}

/// Complete inline mutation identity. Scope fields are flattened on the wire.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NoteApplySplices {
    pub backend_id: String,
    pub workspace_id: String,
    pub note_id: String,
    pub note_instance_id: String,
    pub base_revision: String,
    pub operation_id: String,
    pub expires_at: String,
    pub payload_digest: String,
    pub splices: Vec<NoteSplice>,
}

/// Fixed classifications; errors never contain note text or the supplied payload.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NoteMutationError {
    Invalid,
    Budget,
    Mismatch,
    Expired,
    Conflict,
}

impl NoteMutationError {
    #[must_use]
    pub const fn wire_code(self) -> &'static str {
        match self {
            Self::Invalid => "invalid-params",
            Self::Budget => "note-page-budget",
            Self::Mismatch => "note-operation-mismatch",
            Self::Expired => "note-operation-expired",
            Self::Conflict => "note-revision-conflict",
        }
    }
}

/// Source-addressed mapping derived from edits, never from matching repeated text.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NoteSpliceMapping {
    pub start: u64,
    pub end: u64,
    pub inserted_length: u64,
}

/// Caller stage only. Canonical anchor/task effects are separate ordered phases.
#[derive(Debug, PartialEq, Eq)]
pub struct NoteCallerEdit {
    pub source: String,
    pub mapping: Vec<NoteSpliceMapping>,
    /// Addresses the caller-result source. Deleted bytes are retained internally,
    /// never placed in a commit receipt; large inverse text must be paged.
    pub inverse: Vec<NoteSplice>,
}

fn bounded_token(value: &str) -> bool {
    !value.is_empty() && value.len() <= 256 && !value.contains('\0')
}

impl NoteApplySplices {
    #[must_use]
    pub fn scope(&self) -> NoteScope {
        NoteScope {
            backend_id: self.backend_id.clone(),
            workspace_id: self.workspace_id.clone(),
            note_id: self.note_id.clone(),
            note_instance_id: self.note_instance_id.clone(),
        }
    }

    /// Exact method-bound digest, excluding the digest field itself.
    ///
    /// # Errors
    /// Rejects an integrity envelope exceeding the bounded canonical JSON format.
    pub fn computed_digest(&self) -> Result<String, NoteMutationError> {
        let value = serde_json::json!({
            "method": "note.applySplices", "backendId": self.backend_id,
            "workspaceId": self.workspace_id, "noteId": self.note_id,
            "noteInstanceId": self.note_instance_id, "baseRevision": self.base_revision,
            "operationId": self.operation_id, "expiresAt": self.expires_at,
            "splices": self.splices,
        });
        crate::note_artifact::canonical::digest(&value.to_string())
            .map_err(|_| NoteMutationError::Budget)
    }

    /// Validate identity and bounded payload without consulting current state or
    /// rejecting elapsed deadlines: retained exact replays must work after expiry.
    ///
    /// # Errors
    /// Rejects malformed identity, digest, order, source text or request budgets.
    pub fn validate(&self) -> Result<(), NoteMutationError> {
        for token in [
            &self.backend_id,
            &self.workspace_id,
            &self.note_id,
            &self.note_instance_id,
            &self.base_revision,
        ] {
            if !bounded_token(token) {
                return Err(NoteMutationError::Invalid);
            }
        }
        let id =
            uuid::Uuid::parse_str(&self.operation_id).map_err(|_| NoteMutationError::Invalid)?;
        if id.hyphenated().to_string() != self.operation_id {
            return Err(NoteMutationError::Invalid);
        }
        self.deadline()?;
        if self.payload_digest.len() != 64
            || !self
                .payload_digest
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(NoteMutationError::Invalid);
        }
        validate_splices(&self.splices)?;
        if self.computed_digest()? != self.payload_digest {
            return Err(NoteMutationError::Mismatch);
        }
        Ok(())
    }

    /// Check only for a *new* execution, after durable replay/mismatch lookup.
    ///
    /// # Errors
    /// Rejects expired admissions or deadlines more than 24 hours from admission.
    pub fn validate_new_admission(&self, now: OffsetDateTime) -> Result<(), NoteMutationError> {
        let deadline = self.deadline()?;
        if deadline <= now {
            return Err(NoteMutationError::Expired);
        }
        if deadline - now > time::Duration::hours(24) {
            return Err(NoteMutationError::Invalid);
        }
        Ok(())
    }

    /// Parse the canonical UTC millisecond deadline without normalization.
    ///
    /// # Errors
    /// Rejects noncanonical timestamps or invalid calendar values.
    pub fn deadline(&self) -> Result<OffsetDateTime, NoteMutationError> {
        let bytes = self.expires_at.as_bytes();
        if bytes.len() != 24
            || bytes[4] != b'-'
            || bytes[7] != b'-'
            || bytes[10] != b'T'
            || bytes[13] != b':'
            || bytes[16] != b':'
            || bytes[19] != b'.'
            || bytes[23] != b'Z'
            || !bytes[20..23].iter().all(u8::is_ascii_digit)
        {
            return Err(NoteMutationError::Invalid);
        }
        OffsetDateTime::parse(&self.expires_at, &Rfc3339).map_err(|_| NoteMutationError::Invalid)
    }
}

fn validate_splices(splices: &[NoteSplice]) -> Result<(), NoteMutationError> {
    if splices.is_empty() {
        return Err(NoteMutationError::Invalid);
    }
    if splices.len() > MAX_SPLICES {
        return Err(NoteMutationError::Budget);
    }
    validate_order(splices)?;
    let mut bytes = 0usize;
    for splice in splices {
        bytes = bytes
            .checked_add(splice.text.len())
            .ok_or(NoteMutationError::Budget)?;
        if bytes > SOURCE_BYTES {
            return Err(NoteMutationError::Budget);
        }
    }
    Ok(())
}

fn validate_order(splices: &[NoteSplice]) -> Result<(), NoteMutationError> {
    let mut previous: Option<&NoteSplice> = None;
    for splice in splices {
        if splice.start > splice.end || splice.end > SAFE_INTEGER || splice.text.contains('\0') {
            return Err(NoteMutationError::Invalid);
        }
        if let Some(last) = previous {
            if last.start >= splice.start || last.end > splice.start {
                return Err(NoteMutationError::Invalid);
            }
        }
        previous = Some(splice);
    }
    Ok(())
}

/// Apply one validated batch in one forward pass, preserving all intervening
/// bytes verbatim. This is intentionally document-sized write work, not a bounded
/// read. No CRLF, Unicode, Markdown or whitespace normalization takes place.
///
/// # Errors
/// Rejects out-of-range/split-surrogate addresses, overlapping edits and budgets.
pub fn apply_note_splices(
    source: &str,
    splices: &[NoteSplice],
) -> Result<NoteCallerEdit, NoteMutationError> {
    validate_splices(splices)?;
    apply_edits(source, splices)
}

fn apply_edits(source: &str, splices: &[NoteSplice]) -> Result<NoteCallerEdit, NoteMutationError> {
    validate_order(splices)?;
    let mut chars = source.char_indices().peekable();
    let mut units = 0u64;
    let mut seek = |offset: u64| -> Result<usize, NoteMutationError> {
        while units < offset {
            let (_, ch) = chars.next().ok_or(NoteMutationError::Invalid)?;
            units += ch.len_utf16() as u64;
        }
        if units != offset {
            return Err(NoteMutationError::Invalid);
        }
        Ok(chars.peek().map_or(source.len(), |(byte, _)| *byte))
    };
    let mut output = String::with_capacity(source.len());
    let mut mapping = Vec::with_capacity(splices.len());
    let mut inverse: Vec<NoteSplice> = Vec::with_capacity(splices.len());
    let mut previous_byte = 0;
    let mut previous_units = 0;
    let mut output_units = 0;
    for splice in splices {
        let start = seek(splice.start)?;
        let end = seek(splice.end)?;
        output.push_str(&source[previous_byte..start]);
        output_units += splice.start - previous_units;
        let inserted_length = splice.text.encode_utf16().count() as u64;
        let inverse_start = output_units;
        output.push_str(&splice.text);
        output_units += inserted_length;
        // Adjacent deletions can collapse onto one inverse insertion point.
        // Combine those actual adjacent edits, not matching source substrings.
        if let Some(last) = inverse
            .last_mut()
            .filter(|last| last.start == inverse_start)
        {
            last.end = output_units;
            last.text.push_str(&source[start..end]);
        } else {
            inverse.push(NoteSplice {
                start: inverse_start,
                end: output_units,
                text: source[start..end].to_owned(),
            });
        }
        mapping.push(NoteSpliceMapping {
            start: splice.start,
            end: splice.end,
            inserted_length,
        });
        previous_byte = end;
        previous_units = splice.end;
    }
    output.push_str(&source[previous_byte..]);
    Ok(NoteCallerEdit {
        source: output,
        mapping,
        inverse,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn splice(start: u64, end: u64, text: &str) -> NoteSplice {
        NoteSplice {
            start,
            end,
            text: text.into(),
        }
    }

    #[test]
    fn source_conservation_unicode_crlf_and_repeated_text() {
        let original = "same😀\r\nsame e\u{301}\r\nsame";
        let edited =
            apply_note_splices(original, &[splice(8, 12, "第二"), splice(17, 21, "last")]).unwrap();
        assert_eq!(edited.source, "same😀\r\n第二 e\u{301}\r\nlast");
        assert_eq!(
            apply_note_splices(&edited.source, &edited.inverse)
                .unwrap()
                .source,
            original
        );
        assert_eq!(
            edited.mapping[0],
            NoteSpliceMapping {
                start: 8,
                end: 12,
                inserted_length: 2
            }
        );
    }

    #[test]
    fn invalid_batch_never_produces_partial_output() {
        for edits in [
            vec![splice(0, 0, "x"), splice(2, 3, "y")], // inside the emoji
            vec![splice(1, 3, "x"), splice(1, 1, "y")],
            vec![splice(0, 3, "x"), splice(1, 3, "y")],
            vec![splice(0, 0, "x"), splice(20, 20, "y")],
            vec![splice(0, 0, "\0")],
            vec![splice(0, SAFE_INTEGER + 1, "")],
        ] {
            assert_eq!(
                apply_note_splices("a😀b", &edits),
                Err(NoteMutationError::Invalid)
            );
        }
    }

    #[test]
    fn touching_deletions_have_unambiguous_inverse() {
        let edited = apply_note_splices(
            "abcd",
            &[splice(0, 1, ""), splice(1, 2, ""), splice(2, 3, "Z")],
        )
        .unwrap();
        assert_eq!(edited.source, "Zd");
        assert_eq!(edited.inverse, vec![splice(0, 1, "abc")]);
        assert_eq!(
            apply_note_splices(&edited.source, &edited.inverse)
                .unwrap()
                .source,
            "abcd"
        );
    }

    #[test]
    fn touching_replacements_and_insert_at_predecessor_end_are_legal() {
        assert_eq!(
            apply_note_splices("ab", &[splice(0, 1, "X"), splice(1, 1, "Y")])
                .unwrap()
                .source,
            "XYb"
        );
        assert_eq!(
            apply_note_splices("", &[splice(0, 0, "😀")])
                .unwrap()
                .source,
            "😀"
        );
    }

    #[test]
    fn decoded_utf8_budget_and_scalar_boundaries_are_independent() {
        assert_eq!(
            apply_note_splices("x", &[splice(0, 1, &"😀".repeat(4097))]),
            Err(NoteMutationError::Budget)
        );
        assert!(apply_note_splices("\r\n", &[splice(0, 1, "")]).is_ok());
        assert!(
            serde_json::from_str::<NoteSplice>(r#"{"start":0,"end":0,"text":"\ud800"}"#).is_err()
        );
    }

    fn request() -> NoteApplySplices {
        let mut request = NoteApplySplices {
            backend_id: "db".into(),
            workspace_id: "ws".into(),
            note_id: "spec".into(),
            note_instance_id: "inc".into(),
            base_revision: "r:1:g".into(),
            operation_id: "550e8400-e29b-41d4-a716-446655440000".into(),
            expires_at: "2026-10-05T12:00:00.000Z".into(),
            payload_digest: String::new(),
            splices: vec![splice(0, 0, "😀\r\ne\u{301}")],
        };
        request.payload_digest = request.computed_digest().unwrap();
        request
    }

    #[test]
    fn retained_replay_validation_does_not_reject_expired_admission() {
        let request = request();
        assert_eq!(request.validate(), Ok(()));
        let deadline = request.deadline().unwrap();
        assert_eq!(
            request.validate_new_admission(deadline),
            Err(NoteMutationError::Expired)
        );
        assert_eq!(
            request.validate_new_admission(deadline - time::Duration::hours(24)),
            Ok(())
        );
        assert_eq!(
            request.validate_new_admission(deadline - time::Duration::hours(25)),
            Err(NoteMutationError::Invalid)
        );
    }

    #[test]
    fn identity_digest_binds_exact_source_deadline_and_revision() {
        let mut request = request();
        request.splices[0].text = "😀\ne\u{301}".into();
        assert_eq!(request.validate(), Err(NoteMutationError::Mismatch));
        request.payload_digest = request.computed_digest().unwrap();
        request.base_revision = "r:2:g".into();
        assert_eq!(request.validate(), Err(NoteMutationError::Mismatch));
        request.expires_at = "2026-10-05T12:00:00Z".into();
        assert_eq!(request.validate(), Err(NoteMutationError::Invalid));
    }
}
