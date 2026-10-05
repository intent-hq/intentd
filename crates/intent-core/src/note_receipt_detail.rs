//! Strict immutable receipt read admission, independent of the live source rev.
use crate::{
    note_mutation::{NoteMutationError, NoteOperationStatusQuery},
    note_page::NoteScope,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ReceiptDetailKind {
    Mapping,
    Effects,
    Inverse,
    InverseText,
    Detail,
}

impl ReceiptDetailKind {
    #[must_use]
    pub const fn storage_kind(self) -> &'static str {
        match self {
            Self::Mapping => "mapping",
            Self::Effects => "effects",
            Self::Inverse | Self::InverseText => "inverse",
            Self::Detail => "detail",
        }
    }
    #[must_use]
    pub const fn reference_field(self) -> &'static str {
        match self {
            Self::Mapping => "mappingRef",
            Self::Effects => "effectsRef",
            Self::Inverse | Self::InverseText => "inverseRef",
            Self::Detail => "",
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ReceiptDetailPage {
    pub kind: ReceiptDetailKind,
    pub operation_id: String,
    #[serde(rename = "ref")]
    pub reference: String,
    pub cursor: Option<String>,
    pub max_items: Option<usize>,
    pub max_wire_bytes: Option<usize>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NoteGetReceiptRequest {
    pub backend_id: String,
    pub workspace_id: String,
    pub note_id: String,
    pub note_instance_id: String,
    pub page: ReceiptDetailPage,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NoteOperationReceiptRead {
    pub backend_id: String,
    pub workspace_id: String,
    pub note_id: String,
    pub note_instance_id: String,
    pub operation_id: String,
    pub payload_digest: String,
    pub kind: ReceiptDetailKind,
    #[serde(rename = "ref")]
    pub reference: String,
    pub cursor: Option<String>,
    pub max_items: Option<usize>,
    pub max_wire_bytes: Option<usize>,
    pub max_source_bytes: Option<usize>,
    pub text_id: Option<String>,
    pub offset: Option<u64>,
}

/// Internal normalized request. Services authorize current visibility before
/// and after the Store read, including original-principal deleted incarnations.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReceiptDetailQuery {
    pub scope: NoteScope,
    pub operation_id: String,
    pub payload_digest: Option<String>,
    pub kind: ReceiptDetailKind,
    pub reference: String,
    #[serde(skip)]
    pub cursor: Option<String>,
    pub max_items: usize,
    pub max_wire_bytes: usize,
    pub max_source_bytes: usize,
    pub operation_envelope: bool,
    pub text_id: Option<String>,
    #[serde(skip)]
    pub offset: Option<u64>,
}

impl ReceiptDetailQuery {
    /// Validate normalized query before any storage read.
    /// # Errors
    /// Rejects malformed identity, unsupported envelope/kind or invalid budgets.
    pub fn validate(&self) -> Result<(), NoteMutationError> {
        let identity = NoteOperationStatusQuery {
            backend_id: self.scope.backend_id.clone(),
            workspace_id: self.scope.workspace_id.clone(),
            note_id: self.scope.note_id.clone(),
            note_instance_id: self.scope.note_instance_id.clone(),
            operation_id: self.operation_id.clone(),
            payload_digest: Some(
                self.payload_digest
                    .clone()
                    .unwrap_or_else(|| "0".repeat(64)),
            ),
            header_digest: None,
        };
        identity.validate()?;
        if (self.operation_envelope && self.payload_digest.is_none())
            || (!self.operation_envelope
                && (self.payload_digest.is_some()
                    || !matches!(
                        self.kind,
                        ReceiptDetailKind::Mapping | ReceiptDetailKind::Effects
                    )))
            || [&self.reference]
                .into_iter()
                .chain(self.cursor.iter())
                .chain(self.text_id.iter())
                .any(|s| s.is_empty() || s.len() > 256 || s.contains('\0'))
        {
            return Err(NoteMutationError::Invalid);
        }
        if (self.kind == ReceiptDetailKind::InverseText) != self.text_id.is_some()
            || (self.offset.is_some()
                && (self.cursor.is_some()
                    || !matches!(
                        self.kind,
                        ReceiptDetailKind::InverseText | ReceiptDetailKind::Detail
                    )))
            || self.offset.is_some_and(|n| n > 9_007_199_254_740_991)
        {
            return Err(NoteMutationError::Invalid);
        }
        if !(1..=128).contains(&self.max_items)
            || !(4096..=65536).contains(&self.max_wire_bytes)
            || !(4..=16384).contains(&self.max_source_bytes)
        {
            return Err(NoteMutationError::Budget);
        }
        Ok(())
    }
}

impl NoteGetReceiptRequest {
    /// Normalize the existing note.get receipt-page request.
    /// # Errors
    /// Rejects invalid identity, kind and page budgets.
    pub fn query(self) -> Result<ReceiptDetailQuery, NoteMutationError> {
        let q = ReceiptDetailQuery {
            scope: NoteScope {
                backend_id: self.backend_id,
                workspace_id: self.workspace_id,
                note_id: self.note_id,
                note_instance_id: self.note_instance_id,
            },
            operation_id: self.page.operation_id,
            payload_digest: None,
            kind: self.page.kind,
            reference: self.page.reference,
            cursor: self.page.cursor,
            max_items: self.page.max_items.unwrap_or(128),
            max_wire_bytes: self.page.max_wire_bytes.unwrap_or(65536),
            max_source_bytes: 16384,
            operation_envelope: false,
            text_id: None,
            offset: None,
        };
        q.validate()?;
        Ok(q)
    }
}
impl NoteOperationReceiptRead {
    /// Normalize the inline committed-receipt arm of note.operation.read.
    /// # Errors
    /// Rejects invalid identity, kind and page budgets.
    pub fn query(self) -> Result<ReceiptDetailQuery, NoteMutationError> {
        let q = ReceiptDetailQuery {
            scope: NoteScope {
                backend_id: self.backend_id,
                workspace_id: self.workspace_id,
                note_id: self.note_id,
                note_instance_id: self.note_instance_id,
            },
            operation_id: self.operation_id,
            payload_digest: Some(self.payload_digest),
            kind: self.kind,
            reference: self.reference,
            cursor: self.cursor,
            max_items: self.max_items.unwrap_or(128),
            max_wire_bytes: self.max_wire_bytes.unwrap_or(65536),
            max_source_bytes: self.max_source_bytes.unwrap_or(16384),
            operation_envelope: true,
            text_id: self.text_id,
            offset: self.offset,
        };
        q.validate()?;
        Ok(q)
    }
}

#[cfg(test)]
mod tests;
