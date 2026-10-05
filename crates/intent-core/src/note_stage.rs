//! Pure admission for staged note operations. Durable replay, authorization,
//! reference resolution and scalar endpoints belong to storage/service integration.
use crate::{
    note_mutation::{NoteApplySplices, NoteMutationError, NoteOperationStatusQuery},
    note_page::NoteScope,
};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{json, Value};
use time::OffsetDateTime;

mod canonical;

const SAFE: u64 = 9_007_199_254_740_991;
type Result<T> = std::result::Result<T, NoteMutationError>;
fn token(s: &str) -> bool {
    !s.is_empty() && s.len() <= 256 && !s.contains('\0')
}
fn hash(s: &str) -> bool {
    s.len() == 64
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
fn digest(value: &Value) -> Result<String> {
    canonical::digest(value)
}
fn require(ok: bool) -> Result<()> {
    if ok {
        Ok(())
    } else {
        Err(NoteMutationError::Invalid)
    }
}

fn present_option<'de, D: Deserializer<'de>, T: Deserialize<'de>>(
    d: D,
) -> std::result::Result<Option<T>, D::Error> {
    T::deserialize(d).map(Some)
}
fn required_nullable<'de, D: Deserializer<'de>>(
    d: D,
) -> std::result::Result<Option<String>, D::Error> {
    Option::<String>::deserialize(d)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum NoteStageStream {
    Text,
    Dirty,
    Selection,
    Mutation,
    Live,
}
pub const NOTE_STAGE_STREAMS: [NoteStageStream; 5] = [
    NoteStageStream::Text,
    NoteStageStream::Dirty,
    NoteStageStream::Selection,
    NoteStageStream::Mutation,
    NoteStageStream::Live,
];
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum NoteStageAction {
    Read,
    Mutate,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum NoteStageOutput {
    Source,
    SelectionMarkdown,
    Search,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum NoteStageSelection {
    All,
    Ranges,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum NoteStageSearchMode {
    Source,
    RenderedText,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NoteStageSearch {
    pub text: String,
    pub case_sensitive: bool,
    pub mode: NoteStageSearchMode,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NoteStageHeader {
    pub base_revision: String,
    pub editor_session_id: String,
    pub local_edit_sequence: u64,
    pub live_generation: u64,
    pub selection_generation: u64,
    pub action: NoteStageAction,
    pub output: NoteStageOutput,
    pub selection: NoteStageSelection,
    #[serde(
        default,
        deserialize_with = "present_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub query: Option<NoteStageSearch>,
}
impl NoteStageHeader {
    /// Validate the captured header without consulting live source state.
    /// # Errors
    /// Rejects malformed identity, unsafe counters or unsupported search options.
    pub fn validate(&self) -> Result<()> {
        require(
            token(&self.base_revision)
                && token(&self.editor_session_id)
                && [
                    self.local_edit_sequence,
                    self.live_generation,
                    self.selection_generation,
                ]
                .into_iter()
                .all(|v| v <= SAFE)
                && (self.output == NoteStageOutput::Search) == self.query.is_some(),
        )?;
        if let Some(q) = &self.query {
            require(!q.case_sensitive && q.text.len() <= 1024 && !q.text.contains('\0'))?;
        }
        Ok(())
    }
}

macro_rules! request {
    ($name:ident { $( $(#[$attr:meta])* $field:ident : $ty:ty),* $(,)? }) => {
        #[derive(Clone,Debug,Serialize,Deserialize)]
        #[serde(rename_all="camelCase",deny_unknown_fields)]
        pub struct $name {
            pub backend_id:String,pub workspace_id:String,pub note_id:String,pub note_instance_id:String,
            pub operation_id:String,pub header_digest:String,$( $(#[$attr])* pub $field:$ty),*
        }
        impl $name {
            #[must_use]
            pub fn scope(&self)->NoteScope {NoteScope {backend_id:self.backend_id.clone(),workspace_id:self.workspace_id.clone(),note_id:self.note_id.clone(),note_instance_id:self.note_instance_id.clone()}}
            fn identity(&self)->Result<()> {
                NoteOperationStatusQuery {backend_id:self.backend_id.clone(),workspace_id:self.workspace_id.clone(),note_id:self.note_id.clone(),note_instance_id:self.note_instance_id.clone(),operation_id:self.operation_id.clone(),header_digest:Some(self.header_digest.clone()),payload_digest:None}.validate()
            }
        }
    }
}
request!(NoteStageBegin {
    expires_at: String,
    header: NoteStageHeader
});
request!(NoteStageAppend {stream:NoteStageStream,sequence:u64,#[serde(deserialize_with="required_nullable")] previous_digest:Option<String>,records:Vec<Value>,chunk_digest:String});
request!(NoteStageSeal {manifest:Vec<NoteStageManifestEntry>,payload_digest:String});
request!(NoteStageCommit {
    payload_digest: String
});
request!(NoteStageCancel {});

impl NoteStageBegin {
    /// Compute the method-bound canonical begin digest.
    /// # Errors
    /// Rejects an oversized canonical integrity envelope.
    pub fn computed_digest(&self) -> Result<String> {
        digest(
            &json!({"method":"note.operation.begin","backendId":self.backend_id,"workspaceId":self.workspace_id,"noteId":self.note_id,"noteInstanceId":self.note_instance_id,"operationId":self.operation_id,"expiresAt":self.expires_at,"header":self.header}),
        )
    }
    fn deadline_request(&self) -> NoteApplySplices {
        NoteApplySplices {
            backend_id: self.backend_id.clone(),
            workspace_id: self.workspace_id.clone(),
            note_id: self.note_id.clone(),
            note_instance_id: self.note_instance_id.clone(),
            base_revision: self.header.base_revision.clone(),
            operation_id: self.operation_id.clone(),
            expires_at: self.expires_at.clone(),
            payload_digest: String::new(),
            splices: vec![],
        }
    }
    /// Validate replay identity without rejecting an elapsed stored deadline.
    /// # Errors
    /// Rejects malformed headers, deadlines and digest mismatches.
    pub fn validate(&self) -> Result<()> {
        self.identity()?;
        self.header.validate()?;
        self.deadline_request().deadline()?;
        if self.computed_digest()? != self.header_digest {
            return Err(NoteMutationError::Mismatch);
        }
        Ok(())
    }
    /// Apply deadline policy only after checking durable exact replay.
    /// # Errors
    /// Rejects elapsed deadlines and new leases longer than 24 hours.
    pub fn validate_new_admission(&self, now: OffsetDateTime) -> Result<()> {
        self.deadline_request().validate_new_admission(now)
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NoteStageManifestEntry {
    pub stream: NoteStageStream,
    pub chunks: u64,
    pub records: u64,
    #[serde(deserialize_with = "required_nullable")]
    pub last_digest: Option<String>,
}
impl NoteStageSeal {
    /// Compute the canonical immutable manifest digest.
    /// # Errors
    /// Rejects a canonical envelope exceeding the request budget.
    pub fn computed_digest(&self) -> Result<String> {
        digest(&json!({"headerDigest":self.header_digest,"manifest":self.manifest}))
    }
    /// Validate fixed stream order and counts, without inventing an operation cap.
    /// Storage additionally compares every summary with its persisted chain.
    /// # Errors
    /// Rejects extra/missing streams, unsafe counts and mismatched digests.
    pub fn validate(&self) -> Result<()> {
        self.identity()?;
        require(hash(&self.payload_digest) && self.manifest.len() == 5)?;
        for (entry, stream) in self.manifest.iter().zip(NOTE_STAGE_STREAMS) {
            require(entry.stream == stream && entry.chunks <= SAFE && entry.records <= SAFE)?;
            require(if entry.chunks == 0 {
                entry.records == 0 && entry.last_digest.is_none()
            } else {
                entry.last_digest.as_deref().is_some_and(hash)
            })?;
            require(u128::from(entry.records) <= u128::from(entry.chunks) * 128)?;
        }
        if self.computed_digest()? != self.payload_digest {
            return Err(NoteMutationError::Mismatch);
        }
        Ok(())
    }
}
impl NoteStageCommit {
    /// Validate the sealed operation identity; storage performs CAS and replay.
    /// # Errors
    /// Rejects malformed scope, operation or digest.
    pub fn validate(&self) -> Result<()> {
        self.identity()?;
        require(hash(&self.payload_digest))
    }
}
impl NoteStageCancel {
    /// Validate identity without assuming the operation is still uncommitted.
    /// # Errors
    /// Rejects malformed scope, operation or header digest.
    pub fn validate(&self) -> Result<()> {
        self.identity()
    }
}

/// The complete enclosing frame is measured by transport before deserialization.
/// # Errors
/// Rejects a frame larger than the published per-request limit.
pub fn validate_stage_frame_bytes(bytes: usize) -> Result<()> {
    if bytes > 65536 {
        Err(NoteMutationError::Budget)
    } else {
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NoteStageTextReference {
    pub text_id: String,
    pub length: u64,
    pub utf8_bytes: u64,
    pub sha256: String,
}
impl NoteStageTextReference {
    fn validate(&self) -> Result<()> {
        require(
            token(&self.text_id)
                && self.length <= SAFE
                && self.utf8_bytes <= SAFE
                && hash(&self.sha256),
        )
    }
}
#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
pub enum NoteStageRecord {
    Text {
        id: String,
        offset: u64,
        text: String,
    },
    #[serde(rename_all = "camelCase")]
    Splice {
        #[serde(default)]
        local_sequence: Option<u64>,
        ordinal: u64,
        start: u64,
        end: u64,
        replacement: NoteStageTextReference,
    },
    #[serde(rename_all = "camelCase")]
    Range {
        ordinal: u64,
        start: u64,
        end: u64,
        anchor_affinity: NoteStageAffinity,
        head_affinity: NoteStageAffinity,
        direction: NoteStageDirection,
    },
    #[serde(rename_all = "camelCase")]
    Projection {
        ordinal: u64,
        source_range: NoteStageRange,
        role: NoteStageRole,
        #[serde(default, deserialize_with = "present_option")]
        canonical_id: Option<String>,
        detail: NoteStageTextReference,
    },
}
#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NoteStageRange {
    pub start: u64,
    pub end: u64,
}
#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum NoteStageAffinity {
    Before,
    After,
}
#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum NoteStageDirection {
    Forward,
    Backward,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum NoteStageRole {
    SelectionOwner,
    ParagraphSeam,
    InlineSpan,
    MarkerOccurrence,
}

impl NoteStageAppend {
    /// Compute the canonical per-stream chunk hash without operation-wide reads.
    /// # Errors
    /// Rejects a chunk exceeding canonical envelope limits.
    pub fn computed_digest(&self) -> Result<String> {
        digest(
            &json!({"stream":self.stream,"sequence":self.sequence,"previousDigest":self.previous_digest,"records":self.records}),
        )
    }
    /// Parse at most 128 records and validate stream-local shapes and byte budgets.
    /// Storage enforces persisted sequence/hash, text offsets and source endpoints.
    /// # Errors
    /// Rejects malformed chunks, forbidden stream writes or digest mismatches.
    pub fn validate(&self, header: &NoteStageHeader) -> Result<Vec<NoteStageRecord>> {
        self.identity()?;
        header.validate()?;
        require(self.sequence < SAFE && hash(&self.chunk_digest))?;
        require(if self.sequence == 0 {
            self.previous_digest.is_none()
        } else {
            self.previous_digest.as_deref().is_some_and(hash)
        })?;
        if self.records.len() > 128 {
            return Err(NoteMutationError::Budget);
        }
        require(
            self.stream != NoteStageStream::Mutation || header.action == NoteStageAction::Mutate,
        )?;
        require(
            self.stream != NoteStageStream::Selection
                || header.selection == NoteStageSelection::Ranges,
        )?;
        let mut bytes = 0;
        let mut parsed = Vec::with_capacity(self.records.len());
        for value in &self.records {
            let record: NoteStageRecord =
                serde_json::from_value(value.clone()).map_err(|_| NoteMutationError::Invalid)?;
            match &record {
                NoteStageRecord::Text { id, offset, text } => {
                    require(
                        self.stream == NoteStageStream::Text
                            && token(id)
                            && *offset <= SAFE
                            && !text.contains('\0'),
                    )?;
                    require(
                        u128::from(*offset) + text.encode_utf16().count() as u128
                            <= u128::from(SAFE),
                    )?;
                    bytes += text.len();
                }
                NoteStageRecord::Splice {
                    local_sequence,
                    ordinal,
                    start,
                    end,
                    replacement,
                } => {
                    require(*ordinal < SAFE && start <= end && *end <= SAFE)?;
                    replacement.validate()?;
                    match self.stream {
                        NoteStageStream::Dirty => require(
                            local_sequence.is_some_and(|v| v <= header.local_edit_sequence),
                        )?,
                        NoteStageStream::Mutation => require(
                            local_sequence.is_none()
                                && !value
                                    .as_object()
                                    .is_some_and(|v| v.contains_key("localSequence")),
                        )?,
                        _ => return Err(NoteMutationError::Invalid),
                    }
                }
                NoteStageRecord::Range {
                    ordinal,
                    start,
                    end,
                    ..
                } => require(
                    self.stream == NoteStageStream::Selection
                        && *ordinal < SAFE
                        && start <= end
                        && *end <= SAFE,
                )?,
                NoteStageRecord::Projection {
                    ordinal,
                    source_range,
                    role,
                    canonical_id,
                    detail,
                } => {
                    require(
                        self.stream == NoteStageStream::Live
                            && *ordinal < SAFE
                            && source_range.start <= source_range.end
                            && source_range.end <= SAFE,
                    )?;
                    require(
                        canonical_id.as_deref().is_none_or(token)
                            && (*role != NoteStageRole::MarkerOccurrence || canonical_id.is_some()),
                    )?;
                    detail.validate()?;
                }
            }
            if bytes > 16384 {
                return Err(NoteMutationError::Budget);
            }
            parsed.push(record);
        }
        if self.computed_digest()? != self.chunk_digest {
            return Err(NoteMutationError::Mismatch);
        }
        Ok(parsed)
    }
}

/// Persist this fixed-sized tail per non-text stream. Text IDs need separate
/// indexed offsets/digests; no all-text-ID map is held in this helper.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NoteStageTail {
    pub local_sequence: Option<u64>,
    pub next_ordinal: u64,
    pub previous_range: Option<(u64, u64)>,
}
impl NoteStageTail {
    /// Validate a page against the last persisted ordinal/range/history group.
    /// Returns a replacement tail only on complete success, never a partial edit.
    /// # Errors
    /// Rejects reopened groups, gaps, overlaps and equal-start splices/selections.
    pub fn advance(&self, stream: NoteStageStream, records: &[NoteStageRecord]) -> Result<Self> {
        self.advance_with_selection_union(stream, records, false)
    }

    /// Advance using the operation's immutable, admitted header. Only source
    /// search permits unordered, overlapping or duplicate selection ranges.
    /// Records must first pass `NoteStageAppend::validate` with this same header;
    /// the caller must bind the header and tail to the same persisted operation.
    /// No uploaded record or digest is changed here. The Store computes the
    /// selection union separately at seal; ordinals still describe upload order.
    /// # Errors
    /// Rejects invalid headers and all ordinary tail errors except selection
    /// ordering when output is search and query mode is source.
    pub fn advance_for_header(
        &self,
        stream: NoteStageStream,
        records: &[NoteStageRecord],
        header: &NoteStageHeader,
    ) -> Result<Self> {
        header.validate()?;
        let selection_union = stream == NoteStageStream::Selection
            && header.output == NoteStageOutput::Search
            && header
                .query
                .as_ref()
                .is_some_and(|query| query.mode == NoteStageSearchMode::Source);
        self.advance_with_selection_union(stream, records, selection_union)
    }

    fn advance_with_selection_union(
        &self,
        stream: NoteStageStream,
        records: &[NoteStageRecord],
        selection_union: bool,
    ) -> Result<Self> {
        require(stream != NoteStageStream::Text)?;
        let mut next = self.clone();
        for record in records {
            let (group, ordinal, range) = match record {
                NoteStageRecord::Splice {
                    local_sequence,
                    ordinal,
                    start,
                    end,
                    ..
                } if matches!(stream, NoteStageStream::Dirty | NoteStageStream::Mutation) => {
                    (*local_sequence, *ordinal, Some((*start, *end)))
                }
                NoteStageRecord::Range {
                    ordinal,
                    start,
                    end,
                    ..
                } if stream == NoteStageStream::Selection => (None, *ordinal, Some((*start, *end))),
                NoteStageRecord::Projection { ordinal, .. } if stream == NoteStageStream::Live => {
                    (None, *ordinal, None)
                }
                _ => return Err(NoteMutationError::Invalid),
            };
            if stream == NoteStageStream::Dirty {
                let group = group.ok_or(NoteMutationError::Invalid)?;
                if next.local_sequence != Some(group) {
                    require(next.local_sequence.is_none_or(|last| group > last))?;
                    next.local_sequence = Some(group);
                    next.next_ordinal = 0;
                    next.previous_range = None;
                }
            }
            require(ordinal == next.next_ordinal && ordinal < SAFE)?;
            if let (Some((start, _)), Some((old_start, old_end))) = (range, next.previous_range) {
                require(selection_union || (start > old_start && start >= old_end))?;
            }
            next.next_ordinal = ordinal + 1;
            next.previous_range = range;
        }
        Ok(next)
    }
}

#[cfg(test)]
mod tests;
