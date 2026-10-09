//! Cancellable note deletion control contract. No source bodies or restore data.
use serde::{Deserialize, Serialize};

pub const MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;
pub const MAX_RESULT_BYTES: usize = 524_288;
pub const DEFAULT_DELAY_MS: u64 = 15_000;
pub const MAX_DELAY_MS: u64 = 60_000;
pub const KEY_WINDOW_MS: u64 = 60_000;
pub const RECEIPT_TTL_MS: u64 = 300_000;
pub const WORKSPACE_CAPACITY: usize = 256;
pub const GLOBAL_CAPACITY: usize = 1024;
pub const MAX_CHILDREN: usize = 256;

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NoteDeleteKey {
    pub epoch: String,
    pub issued_tick_ms: u64,
    pub nonce: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NoteDeleteIdentity {
    pub note_instance_id: String,
    pub revision: i64,
    pub source_revision: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NoteDeleteSchedule {
    pub workspace_id: crate::WorkspaceId,
    pub note_id: crate::NoteId,
    pub note_instance_id: String,
    pub expected_version: i64,
    pub source_revision: String,
    pub operation_key: NoteDeleteKey,
    #[serde(default = "default_delay")]
    pub undo_delay_ms: u64,
}
const fn default_delay() -> u64 {
    DEFAULT_DELAY_MS
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NoteDeleteCancel {
    pub workspace_id: crate::WorkspaceId,
    pub note_id: crate::NoteId,
    pub operation_key: NoteDeleteKey,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NoteDeleteStatus {
    pub workspace_id: crate::WorkspaceId,
    pub note_id: Option<crate::NoteId>,
    pub operation_key: Option<NoteDeleteKey>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum NoteDeleteState {
    Pending,
    Committing,
    Cancelled,
    Deleted,
    Conflict,
    Failed,
    OutcomeUnknown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum NoteDeleteReason {
    Cancelled,
    NoteChanged,
    ChildChanged,
    NoteMissing,
    WorkspaceMissing,
    AuthorityLost,
    DeadlineBudget,
    StorageFailure,
    Shutdown,
    CommitOutcomeUnknown,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NoteDeleteReceipt {
    pub operation_key: NoteDeleteKey,
    pub workspace_id: crate::WorkspaceId,
    pub note_id: crate::NoteId,
    pub note_instance_id: String,
    pub state: NoteDeleteState,
    pub sequence: u64,
    pub deadline_tick_ms: u64,
    pub delete_at: String,
    pub expires_tick_ms: Option<u64>,
    pub reason: Option<NoteDeleteReason>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum NoteDeleteUnknownState {
    #[serde(rename = "UNKNOWN")]
    Unknown,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum NoteDeleteUnknownReason {
    PreviousEpoch,
    Unavailable,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NoteDeleteUnknown {
    pub operation_key: NoteDeleteKey,
    pub state: NoteDeleteUnknownState,
    pub reason: NoteDeleteUnknownReason,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum NoteDeleteOperation {
    Receipt(NoteDeleteReceipt),
    Unknown(NoteDeleteUnknown),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NoteDeletePending {
    pub operation_key: NoteDeleteKey,
    pub note_id: crate::NoteId,
    pub note_instance_id: String,
    pub state: NoteDeleteState,
    pub sequence: u64,
    pub deadline_tick_ms: u64,
    pub delete_at: String,
    pub can_cancel: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NoteDeleteOperationResponse {
    pub epoch: String,
    pub server_tick_ms: u64,
    pub sequence: u64,
    pub operation: NoteDeleteOperation,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NoteDeleteStatusResponse {
    pub epoch: String,
    pub server_tick_ms: u64,
    pub sequence: u64,
    pub current: Option<NoteDeleteIdentity>,
    pub pending: Vec<NoteDeletePending>,
    pub operation: Option<NoteDeleteOperation>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NoteDeleteError {
    Invalid,
    Unavailable,
    Stale,
    KeyExpired,
    KeyMismatch,
    AlreadyPending,
    Quota,
    GraphLimit,
    ShuttingDown,
    Forbidden,
}
impl NoteDeleteError {
    #[must_use]
    pub const fn wire_code(self) -> &'static str {
        match self {
            Self::Invalid => "NOTE_DELETE_INVALID",
            Self::Unavailable => "NOTE_DELETE_UNAVAILABLE",
            Self::Stale => "NOTE_DELETE_STALE",
            Self::KeyExpired => "NOTE_DELETE_KEY_EXPIRED",
            Self::KeyMismatch => "NOTE_DELETE_KEY_MISMATCH",
            Self::AlreadyPending => "NOTE_DELETE_ALREADY_PENDING",
            Self::Quota => "NOTE_DELETE_QUOTA",
            Self::GraphLimit => "NOTE_DELETE_GRAPH_LIMIT",
            Self::ShuttingDown => "NOTE_DELETE_SHUTTING_DOWN",
            Self::Forbidden => "NOTE_DELETE_FORBIDDEN",
        }
    }
    #[must_use]
    pub const fn rpc_code(self) -> i32 {
        match self {
            Self::Invalid | Self::Unavailable => -32602,
            Self::Stale | Self::KeyExpired | Self::KeyMismatch | Self::AlreadyPending => -32005,
            Self::Forbidden => -32003,
            Self::Quota | Self::GraphLimit | Self::ShuttingDown => -32603,
        }
    }
}
#[must_use]
pub fn valid_identifier(value: &str) -> bool {
    !value.is_empty() && value.len() <= 128
}
impl NoteDeleteKey {
    #[must_use]
    pub fn valid(&self) -> bool {
        self.issued_tick_ms <= MAX_SAFE_INTEGER
            && uuid::Uuid::parse_str(&self.epoch).is_ok()
            && uuid::Uuid::parse_str(&self.nonce).is_ok()
            && self.epoch.len() == 36
            && self.nonce.len() == 36
    }
}
impl NoteDeleteSchedule {
    #[must_use]
    pub fn valid(&self) -> bool {
        valid_identifier(self.workspace_id.as_str())
            && valid_identifier(self.note_id.as_str())
            && valid_identifier(&self.note_instance_id)
            && valid_identifier(&self.source_revision)
            && u64::try_from(self.expected_version)
                .is_ok_and(|revision| revision <= MAX_SAFE_INTEGER)
            && (1..=MAX_DELAY_MS).contains(&self.undo_delay_ms)
            && self.operation_key.valid()
    }
}
