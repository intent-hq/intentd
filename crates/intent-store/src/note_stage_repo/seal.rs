//! Bounded validation of uploaded stage streams. These helpers do not mark an
//! operation sealed: the owner must also finish text/reference, frozen-view and
//! projection validation, then publish the manifest in the same transaction.
use intent_core::{
    note_mutation::NoteMutationError,
    note_stage::{
        NoteStageAppend, NoteStageHeader, NoteStageSeal, NoteStageStream, NoteStageTail,
        NOTE_STAGE_STREAMS,
    },
    Error, Result,
};
use serde_json::Value;
use sqlx::{Row, SqliteConnection};

fn db(error: impl std::fmt::Display) -> Error {
    Error::Internal(format!("note stage seal: {error}"))
}
fn invalid() -> Error {
    Error::NoteMutation(NoteMutationError::Invalid)
}
fn mismatch() -> Error {
    Error::NoteMutation(NoteMutationError::Mismatch)
}
fn stream_name(stream: NoteStageStream) -> &'static str {
    match stream {
        NoteStageStream::Text => "text",
        NoteStageStream::Dirty => "dirty",
        NoteStageStream::Selection => "selection",
        NoteStageStream::Mutation => "mutation",
        NoteStageStream::Live => "live",
    }
}

/// Verify the five persisted stream summaries and every bounded chunk hash.
/// Work is O(uploaded record bytes + chunks); memory holds at most one chunk of <=64KiB encoded records.
/// No source string, full stream, or operation-wide text-ID map is reconstructed.
/// Caller owns the writer transaction, original operation binding and expiry.
/// # Errors
/// Rejects missing/gapped/changed chunks, bad manifests, invalid record order,
/// mismatched persisted tails and an incomplete captured dirty-history fence.
pub(super) async fn verify_manifest(
    conn: &mut SqliteConnection,
    operation: &str,
    header: &NoteStageHeader,
    request: &NoteStageSeal,
) -> Result<()> {
    request.validate().map_err(Error::NoteMutation)?;
    header.validate().map_err(Error::NoteMutation)?;
    let names: Vec<String> = sqlx::query_scalar(
        "SELECT stream FROM note_stage_stream WHERE operation_key=? ORDER BY stream LIMIT 6",
    )
    .bind(operation)
    .fetch_all(&mut *conn)
    .await
    .map_err(db)?;
    if names.len() != 5
        || NOTE_STAGE_STREAMS
            .iter()
            .any(|stream| !names.iter().any(|name| name == stream_name(*stream)))
    {
        return Err(mismatch());
    }
    for entry in &request.manifest {
        let stream = stream_name(entry.stream);
        let row=sqlx::query("SELECT next_sequence,last_digest,records,tail FROM note_stage_stream WHERE operation_key=? AND stream=?")
            .bind(operation).bind(stream).fetch_one(&mut *conn).await.map_err(db)?;
        let tail: NoteStageTail = serde_json::from_str(row.get::<&str, _>("tail")).map_err(db)?;
        if row.get::<i64, _>("next_sequence") != i64::try_from(entry.chunks).map_err(db)?
            || row.get::<i64, _>("records") != i64::try_from(entry.records).map_err(db)?
            || row.get::<Option<String>, _>("last_digest") != entry.last_digest
        {
            return Err(mismatch());
        }
        let mut previous = None;
        let mut count = 0_u64;
        let mut computed_tail = NoteStageTail::default();
        for sequence in 0..entry.chunks {
            let chunk=sqlx::query("SELECT previous_digest,chunk_digest,record_count FROM note_stage_chunk WHERE operation_key=? AND stream=? AND sequence=?")
                .bind(operation).bind(stream).bind(i64::try_from(sequence).map_err(db)?)
                .fetch_optional(&mut *conn).await.map_err(db)?.ok_or_else(mismatch)?;
            let chunk_hash: String = chunk.get("chunk_digest");
            let record_count: i64 = chunk.get("record_count");
            if chunk.get::<Option<String>, _>("previous_digest") != previous
                || !(0..=128).contains(&record_count)
            {
                return Err(mismatch());
            }
            let records = read_chunk(conn, operation, stream, sequence, record_count).await?;
            let chunk_request = NoteStageAppend {
                backend_id: request.backend_id.clone(),
                workspace_id: request.workspace_id.clone(),
                note_id: request.note_id.clone(),
                note_instance_id: request.note_instance_id.clone(),
                operation_id: request.operation_id.clone(),
                header_digest: request.header_digest.clone(),
                stream: entry.stream,
                sequence,
                previous_digest: previous,
                records,
                chunk_digest: chunk_hash.clone(),
            };
            let parsed = chunk_request
                .validate(header)
                .map_err(Error::NoteMutation)?;
            if entry.stream != NoteStageStream::Text {
                computed_tail = computed_tail
                    .advance(entry.stream, &parsed)
                    .map_err(Error::NoteMutation)?;
            }
            previous = Some(chunk_hash);
            count = count
                .checked_add(u64::try_from(record_count).map_err(db)?)
                .ok_or_else(invalid)?;
        }
        // Probe beyond the declared chain without scanning its accepted prefix.
        let extra:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM note_stage_chunk WHERE operation_key=? AND stream=? AND sequence>=?)")
            .bind(operation).bind(stream).bind(i64::try_from(entry.chunks).map_err(db)?).fetch_one(&mut *conn).await.map_err(db)?;
        if extra || count != entry.records || previous != entry.last_digest || computed_tail != tail
        {
            return Err(mismatch());
        }
        if entry.stream == NoteStageStream::Dirty
            && count > 0
            && computed_tail.local_sequence != Some(header.local_edit_sequence)
        {
            return Err(invalid());
        }
    }
    Ok(())
}

async fn read_chunk(
    conn: &mut SqliteConnection,
    operation: &str,
    stream: &str,
    sequence: u64,
    count: i64,
) -> Result<Vec<Value>> {
    let sequence = i64::try_from(sequence).map_err(db)?;
    let mut records = Vec::with_capacity(usize::try_from(count).map_err(db)?);
    let mut bytes = 0usize;
    for ordinal in 0..count {
        let raw:String=sqlx::query_scalar("SELECT value FROM note_stage_record WHERE operation_key=? AND stream=? AND chunk_sequence=? AND ordinal=?")
            .bind(operation).bind(stream).bind(sequence).bind(ordinal).fetch_optional(&mut *conn).await.map_err(db)?.ok_or_else(mismatch)?;
        bytes = bytes.checked_add(raw.len()).ok_or_else(invalid)?;
        if bytes > 65536 {
            return Err(Error::NoteMutation(NoteMutationError::Budget));
        }
        records.push(serde_json::from_str(&raw).map_err(db)?);
    }
    let extra:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM note_stage_record WHERE operation_key=? AND stream=? AND chunk_sequence=? AND (ordinal<0 OR ordinal>=?))")
        .bind(operation).bind(stream).bind(sequence).bind(count).fetch_one(&mut *conn).await.map_err(db)?;
    if extra {
        return Err(mismatch());
    }
    Ok(records)
}

#[cfg(test)]
#[path = "seal_tests.rs"]
mod tests;
