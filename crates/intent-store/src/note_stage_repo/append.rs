//! One bounded append inside the caller's write transaction. Authorization,
//! operation/header binding, phase and expiry are checked by the owner before
//! entry. Every error requires rolling back that transaction; never commit a
//! partially appended chunk. Transport fits the actual ack frame before commit.
use intent_core::{
    note_mutation::NoteMutationError,
    note_stage::{
        NoteStageAppend, NoteStageHeader, NoteStageRecord, NoteStageStream, NoteStageTail,
    },
    Error, Result,
};
use serde_json::{json, Value};
use sqlx::{Row, SqliteConnection};

const SAFE: i64 = 9_007_199_254_740_991;
const REPLAY_SQL: &str = "SELECT previous_digest,chunk_digest,record_count FROM note_stage_chunk WHERE operation_key=? AND stream=? AND sequence=?";
const STREAM_SQL: &str = "SELECT next_sequence,last_digest,records,tail FROM note_stage_stream WHERE operation_key=? AND stream=?";
const TEXT_SQL: &str =
    "SELECT length,utf8_bytes FROM note_stage_text WHERE operation_key=? AND text_id=?";

fn db(error: impl std::fmt::Display) -> Error {
    Error::Internal(format!("note stage append: {error}"))
}
fn invalid() -> Error {
    Error::NoteMutation(NoteMutationError::Invalid)
}
fn mismatch() -> Error {
    Error::NoteMutation(NoteMutationError::Mismatch)
}
fn count(n: usize) -> Result<i64> {
    i64::try_from(n).map_err(db)
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

/// Persist a bounded chunk and return its immutable acknowledgement.
/// The caller owns an active writer transaction and must roll it back on error.
/// # Errors
/// Rejects invalid/rebound chunks, gaps, wrong chains, noncontiguous text and SQL
/// failures. Same-sequence exact retries return the original seq+1 ack, even if
/// the stream has advanced since; they never write or apply text twice.
pub(super) async fn append(
    conn: &mut SqliteConnection,
    operation_key: &str,
    header: &NoteStageHeader,
    request: &NoteStageAppend,
) -> Result<Value> {
    let parsed = request.validate(header).map_err(Error::NoteMutation)?;
    let stream = stream_name(request.stream);
    let sequence = i64::try_from(request.sequence).map_err(|_| invalid())?;
    let records = count(request.records.len())?;
    let ack = json!({"kind":"noteStageAck","scope":request.scope(),"operationId":request.operation_id,
        "stream":request.stream,"sequence":request.sequence,"chunkDigest":request.chunk_digest,"nextSequence":request.sequence+1});
    if ack.to_string().len() > 4096 {
        return Err(Error::NoteMutation(NoteMutationError::Budget));
    }
    if let Some(row) = sqlx::query(REPLAY_SQL)
        .bind(operation_key)
        .bind(stream)
        .bind(sequence)
        .fetch_optional(&mut *conn)
        .await
        .map_err(db)?
    {
        if row.get::<String, _>("chunk_digest") != request.chunk_digest
            || row.get::<Option<String>, _>("previous_digest") != request.previous_digest
            || row.get::<i64, _>("record_count") != records
        {
            return Err(mismatch());
        }
        return Ok(ack);
    }
    let row = sqlx::query(STREAM_SQL)
        .bind(operation_key)
        .bind(stream)
        .fetch_optional(&mut *conn)
        .await
        .map_err(db)?
        .ok_or_else(invalid)?;
    let next: i64 = row.get("next_sequence");
    let previous: Option<String> = row.get("last_digest");
    let total: i64 = row.get("records");
    if sequence != next || previous != request.previous_digest {
        return Err(mismatch());
    }
    let total = total
        .checked_add(records)
        .filter(|n| (0..=SAFE).contains(n))
        .ok_or_else(invalid)?;
    let tail: NoteStageTail = serde_json::from_str(row.get::<&str, _>("tail")).map_err(db)?;
    let tail = if request.stream == NoteStageStream::Text {
        tail
    } else {
        tail.advance(request.stream, &parsed)
            .map_err(Error::NoteMutation)?
    };
    sqlx::query("INSERT INTO note_stage_chunk(operation_key,stream,sequence,previous_digest,chunk_digest,record_count) VALUES(?,?,?,?,?,?)")
        .bind(operation_key).bind(stream).bind(sequence).bind(&request.previous_digest).bind(&request.chunk_digest).bind(records)
        .execute(&mut *conn).await.map_err(db)?;
    for (ordinal, (record, value)) in parsed.iter().zip(&request.records).enumerate() {
        if let NoteStageRecord::Text { id, offset, text } = record {
            append_text(conn, operation_key, id, *offset, text).await?;
        }
        sqlx::query("INSERT INTO note_stage_record(operation_key,stream,chunk_sequence,ordinal,value) VALUES(?,?,?,?,?)")
            .bind(operation_key).bind(stream).bind(sequence).bind(count(ordinal)?).bind(value.to_string())
            .execute(&mut *conn).await.map_err(db)?;
    }
    let updated=sqlx::query("UPDATE note_stage_stream SET next_sequence=?,last_digest=?,records=?,tail=? WHERE operation_key=? AND stream=? AND next_sequence=?")
        .bind(sequence+1).bind(&request.chunk_digest).bind(total).bind(serde_json::to_string(&tail).map_err(db)?)
        .bind(operation_key).bind(stream).bind(sequence).execute(&mut *conn).await.map_err(db)?;
    if updated.rows_affected() != 1 {
        return Err(mismatch());
    }
    Ok(ack)
}

async fn append_text(
    conn: &mut SqliteConnection,
    operation: &str,
    id: &str,
    offset: u64,
    text: &str,
) -> Result<()> {
    let row = sqlx::query(TEXT_SQL)
        .bind(operation)
        .bind(id)
        .fetch_optional(&mut *conn)
        .await
        .map_err(db)?;
    let (length, bytes) = row.as_ref().map_or((0, 0), |row| {
        (row.get::<i64, _>("length"), row.get::<i64, _>("utf8_bytes"))
    });
    let offset = i64::try_from(offset).map_err(|_| invalid())?;
    if offset != length || bytes < 0 {
        return Err(invalid());
    }
    let end = length
        .checked_add(count(text.encode_utf16().count())?)
        .filter(|n| (0..=SAFE).contains(n))
        .ok_or_else(invalid)?;
    let bytes = bytes
        .checked_add(count(text.len())?)
        .filter(|n| (0..=SAFE).contains(n))
        .ok_or_else(invalid)?;
    if row.is_some() {
        sqlx::query(
            "UPDATE note_stage_text SET length=?,utf8_bytes=?,sha256=NULL WHERE operation_key=? AND text_id=?",
        )
        .bind(end)
        .bind(bytes)
        .bind(operation)
        .bind(id)
        .execute(&mut *conn)
        .await
        .map_err(db)?;
    } else {
        sqlx::query(
            "INSERT INTO note_stage_text(operation_key,text_id,length,utf8_bytes) VALUES(?,?,?,?)",
        )
        .bind(operation)
        .bind(id)
        .bind(end)
        .bind(bytes)
        .execute(&mut *conn)
        .await
        .map_err(db)?;
    }
    let mut byte = 0;
    let mut start = offset;
    while byte < text.len() {
        let mut last = (byte + 4096).min(text.len());
        while !text.is_char_boundary(last) {
            last -= 1;
        }
        let part = &text[byte..last];
        let next = start + count(part.encode_utf16().count())?;
        sqlx::query("INSERT INTO note_stage_text_piece(operation_key,text_id,start,end,text) VALUES(?,?,?,?,?)")
            .bind(operation).bind(id).bind(start).bind(next).bind(part).execute(&mut *conn).await.map_err(db)?;
        start = next;
        byte = last;
    }
    Ok(())
}

#[cfg(test)]
#[path = "append_tests.rs"]
mod tests;
