//! Bounded requests for immutable staged source output. No current Note is read.
use crate::{
    note_mutation::{NoteMutationError, NoteOperationStatusQuery},
    note_page::NoteScope,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum NoteStageReadKind {
    Source,
    SelectionMarkdown,
    Search,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NoteStageRead {
    pub backend_id: String,
    pub workspace_id: String,
    pub note_id: String,
    pub note_instance_id: String,
    pub operation_id: String,
    pub header_digest: String,
    pub kind: NoteStageReadKind,
    #[serde(skip_serializing)]
    pub cursor: Option<String>,
    pub max_items: Option<usize>,
    pub max_source_bytes: Option<usize>,
    pub max_wire_bytes: Option<usize>,
}
impl NoteStageRead {
    #[must_use]
    pub fn scope(&self) -> NoteScope {
        NoteScope {
            backend_id: self.backend_id.clone(),
            workspace_id: self.workspace_id.clone(),
            note_id: self.note_id.clone(),
            note_instance_id: self.note_instance_id.clone(),
        }
    }
    /// Validate scope and explicit per-response resource limits.
    /// # Errors
    /// Rejects invalid identities, cursors or budgets; no implicit continuation.
    pub fn validate(&self) -> Result<(), NoteMutationError> {
        NoteOperationStatusQuery {
            backend_id: self.backend_id.clone(),
            workspace_id: self.workspace_id.clone(),
            note_id: self.note_id.clone(),
            note_instance_id: self.note_instance_id.clone(),
            operation_id: self.operation_id.clone(),
            header_digest: Some(self.header_digest.clone()),
            payload_digest: None,
        }
        .validate()?;
        if self
            .cursor
            .as_ref()
            .is_some_and(|s| s.is_empty() || s.len() > 256 || s.contains('\0'))
        {
            return Err(NoteMutationError::Invalid);
        }
        if !(1..=128).contains(&self.max_items.unwrap_or(64))
            || !(4..=16384).contains(&self.max_source_bytes.unwrap_or(8192))
            || !(4096..=65536).contains(&self.max_wire_bytes.unwrap_or(4096))
        {
            return Err(NoteMutationError::Budget);
        }
        Ok(())
    }
}
