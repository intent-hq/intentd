//! Opt-in bounded note read vocabulary. This does not advertise `notePaging`:
//! that capability also requires partial writes and bounded subscriptions.
use serde::{Deserialize, Serialize};

/// Hard decoded source budget.
pub const SOURCE_BYTES: usize = 16_384;
/// Complete escaped JSON-RPC frame budget.
pub const WIRE_BYTES: usize = 65_536;
/// Maximum page item count.
pub const PAGE_ITEMS: usize = 128;

/// Database, workspace and persistent note incarnation identity.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NoteScope {
    pub backend_id: String,
    pub workspace_id: String,
    pub note_id: String,
    pub note_instance_id: String,
}

/// Strict opt-in request. The store additionally validates kind-specific fields.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NotePageRequest {
    pub kind: String,
    pub at: Option<u64>,
    pub direction: Option<String>,
    pub cursor: Option<String>,
    pub snapshot_id: Option<String>,
    pub source_revision: Option<String>,
    pub note_instance_id: Option<String>,
    pub context_ref: Option<String>,
    #[serde(rename = "ref")]
    pub reference: Option<String>,
    pub max_source_bytes: Option<usize>,
    pub max_wire_bytes: Option<usize>,
    pub max_items: Option<usize>,
}

/// Fixed bounded failure classification, never a complete current Note.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NotePageError {
    CursorInvalid,
    Expired,
    Stale,
    Budget,
}
impl NotePageError {
    #[must_use]
    pub const fn wire_code(self) -> &'static str {
        match self {
            Self::CursorInvalid => "note-page-cursor-invalid",
            Self::Expired => "note-page-expired",
            Self::Stale => "note-page-stale",
            Self::Budget => "note-page-budget",
        }
    }
}
