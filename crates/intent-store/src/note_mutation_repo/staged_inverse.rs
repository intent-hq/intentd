//! Stream inverse coordinates from captured edits, never from a text diff.
//!
//! Caller owns one immutable writer transaction, exact sealed operation/root,
//! verified records, authorization and original expiry. Writer retains source
//! and provenance, assigns receipt state tokens, and publishes atomically.
//! Canonical phases compose with the newest user group via `MappingCursor` and
//! that group's `NoteSourceHistory` mapping; earlier groups remain separate.
use intent_core::{
    note_mutation::{NoteMutationError, NoteSpliceMapping},
    note_stage::NoteStageRecord,
    Error, Result,
};
use sqlx::{Row, SqliteConnection};

const SAFE: u64 = 9_007_199_254_740_991;
const PREVIOUS_RECORD: &str = "SELECT chunk_sequence,ordinal,CASE WHEN length(CAST(value AS BLOB))<=65536 THEN value ELSE NULL END AS value FROM note_stage_record WHERE operation_key=? AND stream='dirty' AND (chunk_sequence,ordinal)<(?,?) ORDER BY chunk_sequence DESC,ordinal DESC LIMIT 1";
const NEXT_RECORD: &str = "SELECT chunk_sequence,ordinal,CASE WHEN length(CAST(value AS BLOB))<=65536 THEN value ELSE NULL END AS value FROM note_stage_record WHERE operation_key=? AND stream='dirty' AND (chunk_sequence,ordinal)>=(?,?) AND (chunk_sequence,ordinal)<=(?,?) ORDER BY chunk_sequence,ordinal LIMIT 1";
fn invalid() -> Error {
    Error::NoteMutation(NoteMutationError::Invalid)
}
fn db(error: impl std::fmt::Display) -> Error {
    Error::Internal(format!("staged inverse: {error}"))
}
fn number(value: i64) -> Result<u64> {
    u64::try_from(value)
        .ok()
        .filter(|n| *n <= SAFE)
        .ok_or_else(invalid)
}
fn add(a: u64, b: u64) -> Result<u64> {
    a.checked_add(b).filter(|n| *n <= SAFE).ok_or_else(invalid)
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct RecordKey(i64, i64);
#[derive(Debug)]
struct Record {
    key: RecordKey,
    local_sequence: u64,
    ordinal: u64,
    mapping: NoteSpliceMapping,
}
fn record(row: &sqlx::sqlite::SqliteRow) -> Result<Record> {
    let key = RecordKey(
        row.try_get("chunk_sequence").map_err(db)?,
        row.try_get("ordinal").map_err(db)?,
    );
    number(key.0)?;
    number(key.1)?;
    let value: Option<String> = row.try_get("value").map_err(db)?;
    let NoteStageRecord::Splice {
        local_sequence: Some(local_sequence),
        ordinal,
        start,
        end,
        replacement,
    } = serde_json::from_str(&value.ok_or_else(invalid)?).map_err(db)?
    else {
        return Err(invalid());
    };
    if local_sequence > SAFE
        || ordinal > SAFE
        || start > end
        || end > SAFE
        || replacement.length > SAFE
    {
        return Err(invalid());
    }
    Ok(Record {
        key,
        local_sequence,
        ordinal,
        mapping: NoteSpliceMapping {
            start,
            end,
            inserted_length: replacement.length,
        },
    })
}
/// One retained dirty group, with no text or whole-group record collection.
#[derive(Debug)]
pub(super) struct CapturedGroup {
    pub generation: u64,
    pub input_generation: u64,
    pub history_group: String,
    pub input_length: u64,
    pub output_length: u64,
    first: RecordKey,
    last: RecordKey,
}
/// Reverse chronological view/record walker. One reverse boundary pass and one
/// forward emission pass per group: O(records + groups) work, a constant number
/// of <=64KiB envelopes in memory. No repeated scan of the earlier prefix.
pub(super) struct GroupWalk {
    generation: u64,
    before: RecordKey,
}
impl GroupWalk {
    pub fn new(latest_generation: u64) -> Result<Self> {
        if latest_generation > SAFE {
            return Err(invalid());
        }
        Ok(Self {
            generation: latest_generation,
            before: RecordKey(i64::MAX, i64::MAX),
        })
    }
}
async fn previous_record(
    conn: &mut SqliteConnection,
    operation: &str,
    before: RecordKey,
) -> Result<Option<Record>> {
    sqlx::query(PREVIOUS_RECORD)
        .bind(operation)
        .bind(before.0)
        .bind(before.1)
        .fetch_optional(conn)
        .await
        .map_err(db)?
        .as_ref()
        .map(record)
        .transpose()
}
/// Start with the exact sealed latest generation and use the same operation
/// throughout. Cursor advancement happens only after successful discovery.
pub(super) async fn previous_group(
    conn: &mut SqliteConnection,
    operation: &str,
    walk: &mut GroupWalk,
) -> Result<Option<CapturedGroup>> {
    let newest = previous_record(conn, operation, walk.before).await?;
    if walk.generation == 0 {
        if newest.is_some() {
            return Err(invalid());
        }
        return Ok(None);
    }
    let row = sqlx::query("SELECT v.input_generation,v.history_group,v.length,p.length AS input_length FROM note_stage_view v JOIN note_stage_view p ON p.operation_key=v.operation_key AND p.generation=v.input_generation WHERE v.operation_key=? AND v.generation=?")
        .bind(operation).bind(i64::try_from(walk.generation).map_err(db)?).fetch_optional(&mut *conn).await.map_err(db)?.ok_or_else(invalid)?;
    let input_generation = number(row.try_get("input_generation").map_err(db)?)?;
    let history_group: String = row.try_get("history_group").map_err(db)?;
    let input_length = number(row.try_get("input_length").map_err(db)?)?;
    let output_length = number(row.try_get("length").map_err(db)?)?;
    if add(input_generation, 1)? != walk.generation
        || history_group.is_empty()
        || history_group.len() > 256
    {
        return Err(invalid());
    }
    let newest = newest.ok_or_else(invalid)?;
    // Seal's exact localSequence encoding, not a numeric interpretation of
    // public receipt historyGroup identities such as "0" versus "00".
    if newest.local_sequence.to_string() != history_group {
        return Err(invalid());
    }
    let sequence = newest.local_sequence;
    let last = newest.key;
    let mut first = last;
    let mut ordinal = newest.ordinal;
    while let Some(prior) = previous_record(conn, operation, first).await? {
        if prior.local_sequence != sequence {
            if prior.local_sequence >= sequence {
                return Err(invalid());
            }
            break;
        }
        if add(prior.ordinal, 1)? != ordinal {
            return Err(invalid());
        }
        ordinal = prior.ordinal;
        first = prior.key;
    }
    if ordinal != 0 {
        return Err(invalid());
    }
    let group = CapturedGroup {
        generation: walk.generation,
        input_generation,
        history_group,
        input_length,
        output_length,
        first,
        last,
    };
    walk.generation = input_generation;
    walk.before = first;
    Ok(Some(group))
}
/// UTF16 inverse coordinates. Replacement bytes come from the original input
/// generation's `[replacement_start,replacement_end)`, including scalar boundaries
/// already verified at seal; this helper does not supply marker authority.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct InverseRange {
    pub ordinal: u64,
    pub start: u64,
    pub end: u64,
    pub replacement_start: u64,
    pub replacement_end: u64,
}
/// Feed one group's exact provenance mappings in ascending input order. A
/// newest mapping may include canonical edits outside the user's splice range.
/// Explicit same-byte replacements keep identity; no text comparison is used.
#[derive(Clone, Debug)]
pub(super) struct MappingCursor {
    input_length: u64,
    consumed: u64,
    output: u64,
    ordinal: u64,
}
impl MappingCursor {
    pub fn new(input_length: u64) -> Result<Self> {
        if input_length > SAFE {
            return Err(invalid());
        }
        Ok(Self {
            input_length,
            consumed: 0,
            output: 0,
            ordinal: 0,
        })
    }
    pub fn push(&mut self, item: &NoteSpliceMapping) -> Result<InverseRange> {
        if item.start < self.consumed || item.start > item.end || item.end > self.input_length {
            return Err(invalid());
        }
        let start = add(self.output, item.start - self.consumed)?;
        let end = add(start, item.inserted_length)?;
        let next_ordinal = add(self.ordinal, 1)?;
        let inverse = InverseRange {
            ordinal: self.ordinal,
            start,
            end,
            replacement_start: item.start,
            replacement_end: item.end,
        };
        self.consumed = item.end;
        self.output = end;
        self.ordinal = next_ordinal;
        Ok(inverse)
    }
    pub fn finish(&self, output_length: u64) -> Result<()> {
        if add(self.output, self.input_length - self.consumed)? != output_length {
            return Err(invalid());
        }
        Ok(())
    }
}
pub(super) struct InverseCursor {
    next: RecordKey,
    mapping: MappingCursor,
    done: bool,
}
impl CapturedGroup {
    pub fn cursor(&self) -> Result<InverseCursor> {
        Ok(InverseCursor {
            next: self.first,
            mapping: MappingCursor::new(self.input_length)?,
            done: false,
        })
    }
}
/// Drain through None before publication and roll back the outer transaction on
/// any error. Mapping cursor is not advanced if a record/length check fails.
pub(super) async fn next_inverse(
    conn: &mut SqliteConnection,
    operation: &str,
    group: &CapturedGroup,
    cursor: &mut InverseCursor,
) -> Result<Option<InverseRange>> {
    if cursor.done {
        cursor.mapping.finish(group.output_length)?;
        return Ok(None);
    }
    let row = sqlx::query(NEXT_RECORD)
        .bind(operation)
        .bind(cursor.next.0)
        .bind(cursor.next.1)
        .bind(group.last.0)
        .bind(group.last.1)
        .fetch_optional(conn)
        .await
        .map_err(db)?
        .ok_or_else(invalid)?;
    let item = record(&row)?;
    if item.local_sequence.to_string() != group.history_group
        || item.ordinal != cursor.mapping.ordinal
    {
        return Err(invalid());
    }
    let mut mapping = cursor.mapping.clone();
    let inverse = mapping.push(&item.mapping)?;
    let done = item.key == group.last;
    if done {
        mapping.finish(group.output_length)?;
    }
    let next = RecordKey(item.key.0, item.key.1.checked_add(1).ok_or_else(invalid)?);
    cursor.mapping = mapping;
    cursor.next = next;
    cursor.done = done;
    Ok(Some(inverse))
}

/// One pending range per history group. Touching output ranges with contiguous
/// original input are one exact inverse splice, including adjacent deletions
/// which otherwise produce inadmissible equal-start insertions. Never crosses
/// history/state boundaries; replacement remains a retained contiguous span.
#[derive(Default)]
struct InverseCoalescer {
    pending: Option<InverseRange>,
    ordinal: u64,
}
impl InverseCoalescer {
    fn push(&mut self, range: InverseRange) -> Result<Option<InverseRange>> {
        if let Some(previous) = self.pending.as_mut() {
            if range.start < previous.end || range.replacement_start < previous.replacement_end {
                return Err(invalid());
            }
            if previous.end == range.start && previous.replacement_end == range.replacement_start {
                previous.end = range.end;
                previous.replacement_end = range.replacement_end;
                return Ok(None);
            }
            if range.start == previous.start {
                return Err(invalid());
            }
        }
        let emitted = self.finish()?;
        self.pending = Some(range);
        Ok(emitted)
    }
    fn finish(&mut self) -> Result<Option<InverseRange>> {
        let Some(mut range) = self.pending.take() else {
            return Ok(None);
        };
        range.ordinal = self.ordinal;
        self.ordinal = add(self.ordinal, 1)?;
        Ok(Some(range))
    }
}

// Zero-user operations must distinguish identity provenance mappings from actual
// source changes. Compare the retained base bytes incrementally, without a second
// whole-source String. This is write work, not public page-read latency.
async fn source_matches_base(
    conn: &mut SqliteConnection,
    operation: &str,
    source: &str,
) -> Result<bool> {
    let mut position = 0_i64;
    let mut byte = 0_usize;
    loop {
        let row:Option<(i64,i64,Option<String>)> = sqlx::query_as("SELECT start,end,CASE WHEN length(CAST(text AS BLOB))<=4096 THEN text ELSE NULL END FROM note_operation_source WHERE operation_key=? AND phase='base' AND start>=? ORDER BY start LIMIT 1")
            .bind(operation).bind(position).fetch_optional(&mut *conn).await.map_err(db)?;
        let Some((start, end, text)) = row else {
            return Ok(byte == source.len());
        };
        let text = text.ok_or_else(invalid)?;
        if start != position
            || end <= start
            || end - start != i64::try_from(text.encode_utf16().count()).map_err(db)?
        {
            return Err(invalid());
        }
        let next = byte.checked_add(text.len()).ok_or_else(invalid)?;
        if source.get(byte..next) != Some(text.as_str()) {
            return Ok(false);
        }
        byte = next;
        position = end;
    }
}

struct ReceiptGroup {
    history_group: String,
    input_state: String,
    output_state: String,
    input_generation: u64,
    input_length: u64,
}

fn scalar_byte(text: &str, units: u64) -> Result<usize> {
    let mut offset = 0;
    for (byte, scalar) in text.char_indices() {
        if offset == units {
            return Ok(byte);
        }
        offset += u64::try_from(scalar.len_utf16()).expect("UTF16 scalar length fits");
        if offset > units {
            return Err(invalid());
        }
    }
    if offset == units {
        Ok(text.len())
    } else {
        Err(invalid())
    }
}

// Persist only removed spans, at their original generation offsets. Disjoint
// ordered mappings cannot collide; empty spans still get an owned text registry
// entry through retain_text_reference. At most one <=4096-byte source chunk is
// loaded at a time, independently of the total group/source length.
async fn retain_input_span(
    conn: &mut SqliteConnection,
    operation: &str,
    group: &ReceiptGroup,
    range: &InverseRange,
) -> Result<String> {
    if range.replacement_start > range.replacement_end || range.replacement_end > group.input_length
    {
        return Err(invalid());
    }
    if group.input_generation == 0 {
        return Ok("base".into());
    }
    let phase = format!("stage-input:{}", group.input_generation);
    let mut position = range.replacement_start;
    while position < range.replacement_end {
        let (end, text) = crate::note_stage_repo::view_read::read_piece(
            conn,
            operation,
            group.input_generation,
            group.input_length,
            position,
            4096,
        )
        .await?;
        let next = end.min(range.replacement_end);
        if next <= position {
            return Err(invalid());
        }
        let text = &text[..scalar_byte(&text, next - position)?];
        sqlx::query("INSERT INTO note_operation_source(operation_key,phase,start,end,text) VALUES(?,?,?,?,?)")
            .bind(operation).bind(&phase).bind(i64::try_from(position).map_err(db)?).bind(i64::try_from(next).map_err(db)?).bind(text)
            .execute(&mut *conn).await.map_err(db)?;
        position = next;
    }
    Ok(phase)
}

impl super::NoteMutationWrite {
    /// Receipt states and data stay inside the active write. Any error must abort
    /// the outer transaction; partial inverse publication is never authoritative.
    pub(super) async fn write_staged_inverse(&mut self, revision: &str) -> Result<()> {
        let staged = self.staged.as_ref().ok_or_else(invalid)?;
        let generation = staged.view_generation;
        let mutation_present = staged.mutation_present;
        if generation == 0
            && !mutation_present
            && source_matches_base(
                &mut self.transaction,
                &self.operation_key,
                self.history.source(),
            )
            .await?
        {
            return Ok(());
        }
        // One group's numeric provenance only, never a cloned source/context or
        // a collection of source snapshots from all chronological groups.
        let newest = staged
            .newest_history
            .as_ref()
            .map(intent_core::note_mutation::NoteSourceHistory::mapping);
        let mut walk = GroupWalk::new(generation)?;
        let mut state = revision.to_owned();
        let mut sequence = 0;
        let mut latest_dirty = if generation > 0 {
            previous_group(&mut self.transaction, &self.operation_key, &mut walk).await?
        } else {
            None
        };
        if let Some(mapping) = newest {
            let (input_generation, input_length, history_group) = if mutation_present {
                (
                    generation,
                    self.inverse_generation_length(generation).await?,
                    format!("{}:mutation", self.operation_key),
                )
            } else if let Some(group) = latest_dirty.take() {
                (
                    group.input_generation,
                    group.input_length,
                    group.history_group,
                )
            } else {
                (
                    0,
                    self.inverse_generation_length(0).await?,
                    format!("{}:operation", self.operation_key),
                )
            };
            let output_state = self.inverse_generation_state(input_generation);
            let group = ReceiptGroup {
                history_group,
                input_state: state.clone(),
                output_state: output_state.clone(),
                input_generation,
                input_length,
            };
            let mut cursor = MappingCursor::new(input_length)?;
            let mut normalized = InverseCoalescer::default();
            for item in &mapping {
                if let Some(range) = normalized.push(cursor.push(item)?)? {
                    self.write_staged_inverse_range(&group, &range, &mut sequence)
                        .await?;
                }
            }
            // A captured gesture still owns a state transition when canonical
            // composition cancels its source edits. An ordinary empty splice
            // carries that group's owned text/provenance without changing bytes.
            let captured_group = mutation_present || generation > 0;
            if mapping.is_empty() && captured_group {
                normalized.push(cursor.push(&NoteSpliceMapping {
                    start: 0,
                    end: 0,
                    inserted_length: 0,
                })?)?;
            }
            cursor
                .finish(u64::try_from(self.history.source().encode_utf16().count()).map_err(db)?)?;
            if let Some(range) = normalized.finish()? {
                self.write_staged_inverse_range(&group, &range, &mut sequence)
                    .await?;
            }
            if !mapping.is_empty() || captured_group {
                state = output_state;
            }
        } else {
            return Err(invalid());
        }
        loop {
            let captured = if let Some(group) = latest_dirty.take() {
                Some(group)
            } else {
                previous_group(&mut self.transaction, &self.operation_key, &mut walk).await?
            };
            let Some(captured) = captured else {
                break;
            };
            let group = ReceiptGroup {
                history_group: captured.history_group.clone(),
                input_state: state.clone(),
                output_state: self.inverse_generation_state(captured.input_generation),
                input_generation: captured.input_generation,
                input_length: captured.input_length,
            };
            let mut cursor = captured.cursor()?;
            let mut normalized = InverseCoalescer::default();
            while let Some(range) = next_inverse(
                &mut self.transaction,
                &self.operation_key,
                &captured,
                &mut cursor,
            )
            .await?
            {
                if let Some(range) = normalized.push(range)? {
                    self.write_staged_inverse_range(&group, &range, &mut sequence)
                        .await?;
                }
            }
            if let Some(range) = normalized.finish()? {
                self.write_staged_inverse_range(&group, &range, &mut sequence)
                    .await?;
            }
            state = group.output_state;
        }
        Ok(())
    }
    async fn inverse_generation_length(&mut self, generation: u64) -> Result<u64> {
        let length: i64 = sqlx::query_scalar(
            "SELECT length FROM note_stage_view WHERE operation_key=? AND generation=?",
        )
        .bind(&self.operation_key)
        .bind(i64::try_from(generation).map_err(db)?)
        .fetch_optional(&mut *self.transaction)
        .await
        .map_err(db)?
        .ok_or_else(invalid)?;
        number(length)
    }
    fn inverse_generation_state(&self, generation: u64) -> String {
        if generation == 0 {
            self.request.base_revision.clone()
        } else {
            format!("{}:stage:{generation}", self.operation_key)
        }
    }
    async fn write_staged_inverse_range(
        &mut self,
        group: &ReceiptGroup,
        range: &InverseRange,
        sequence: &mut usize,
    ) -> Result<()> {
        if group.history_group.is_empty() || group.history_group.len() > 256 {
            return Err(invalid());
        }
        let phase =
            retain_input_span(&mut self.transaction, &self.operation_key, group, range).await?;
        let text_id = format!("{}:text:{}", self.operation_key, sequence);
        let replacement = self
            .retain_text_reference(
                &text_id,
                &phase,
                range.replacement_start,
                range.replacement_end,
            )
            .await?;
        let provenance = format!("{}:inverse-detail:{}", self.operation_key, sequence);
        self.retain_detail_tree(&provenance,serde_json::json!({
            "kind":"sourceProvenance","inputState":group.input_state,"outputState":group.output_state,
            "baseRange":{"start":range.replacement_start,"end":range.replacement_end},
            "finalRange":{"start":range.start,"end":range.end},"replacement":replacement
        })).await?;
        self.insert_item("inverse",*sequence,&serde_json::json!({
            "historyGroup":group.history_group,"inputState":group.input_state,"outputState":group.output_state,
            "ordinal":range.ordinal,"start":range.start,"end":range.end,"replacement":replacement,"provenanceRef":provenance
        })).await?;
        *sequence = sequence.checked_add(1).ok_or_else(invalid)?;
        Ok(())
    }
}

#[cfg(test)]
#[path = "staged_inverse_tests.rs"]
mod tests;
