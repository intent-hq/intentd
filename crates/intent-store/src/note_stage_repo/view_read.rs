//! Scalar-safe reads of retained stage views. The caller holds one read snapshot
//! and owns original operation authorization, expiry and sealed-view admission.
use intent_core::{note_mutation::NoteMutationError, Error, Result};
use sqlx::{Row, SqliteConnection};

const SAFE: u64 = 9_007_199_254_740_991;
const VIEW_PIECE: &str = "SELECT start,end,origin_kind,origin_id,origin_start FROM note_stage_view_piece WHERE operation_key=? AND generation=? AND start<=? ORDER BY start DESC LIMIT 1";
const TEXT_PIECE: &str = "SELECT p.start,p.end,CASE WHEN length(CAST(p.text AS BLOB))<=4096 THEN p.text ELSE NULL END AS text,t.length AS owner_length FROM note_stage_text_piece p JOIN note_stage_text t ON t.operation_key=p.operation_key AND t.text_id=p.text_id WHERE p.operation_key=? AND p.text_id=? AND p.start<=? ORDER BY p.start DESC LIMIT 1";
fn invalid() -> Error {
    Error::NoteMutation(NoteMutationError::Invalid)
}
fn db(error: impl std::fmt::Display) -> Error {
    Error::Internal(format!("note stage view read: {error}"))
}
fn number(value: i64) -> Result<u64> {
    u64::try_from(value)
        .ok()
        .filter(|n| *n <= SAFE)
        .ok_or_else(invalid)
}
fn integer(value: u64) -> Result<i64> {
    if value > SAFE {
        return Err(invalid());
    }
    i64::try_from(value).map_err(db)
}
fn add(a: u64, b: u64) -> Result<u64> {
    a.checked_add(b).filter(|n| *n <= SAFE).ok_or_else(invalid)
}
fn byte_at(text: &str, offset: u64) -> Result<usize> {
    let mut units = 0;
    for (byte, c) in text.char_indices() {
        if units == offset {
            return Ok(byte);
        }
        units += u64::try_from(c.len_utf16()).expect("scalar UTF16 size fits");
        if units > offset {
            return Err(invalid());
        }
    }
    if units == offset {
        Ok(text.len())
    } else {
        Err(invalid())
    }
}

/// Read at most `max_bytes` UTF8 bytes, advancing a UTF16 scalar offset. Empty
/// output is returned only at EOF. Any retained generation is readable, but its
/// exact operation and stored length must match; this does not admit a current
/// live note or make a prepared generation publicly authoritative.
///
/// Each indexed seek loads one descriptor and at most two root pieces or one
/// text piece (each <=4096 bytes). Output is <=16384 bytes. Work is proportional
/// to returned scalars/pieces, never the total view or its preceding prefix.
pub(super) async fn read_piece(
    conn: &mut SqliteConnection,
    operation: &str,
    generation: u64,
    view_length: u64,
    offset: u64,
    max_bytes: usize,
) -> Result<(u64, String)> {
    if !(4..=16384).contains(&max_bytes) || offset > view_length {
        return Err(invalid());
    }
    let generation = integer(generation)?;
    integer(offset)?;
    let binding:Option<(i64,String,i64)>=sqlx::query_as("SELECT v.length,s.root_key,r.source_length FROM note_stage_view v JOIN note_stage s ON s.operation_key=v.operation_key JOIN note_stage_root r ON r.root_key=s.root_key WHERE v.operation_key=? AND v.generation=?")
        .bind(operation).bind(generation).fetch_optional(&mut *conn).await.map_err(db)?;
    let (stored_length, root, root_length) = binding.ok_or_else(invalid)?;
    if number(stored_length)? != view_length {
        return Err(invalid());
    }
    let root_length = number(root_length)?;
    let mut position = offset;
    let mut output = String::with_capacity(max_bytes);
    while position < view_length {
        let descriptor = sqlx::query(VIEW_PIECE)
            .bind(operation)
            .bind(generation)
            .bind(integer(position)?)
            .fetch_optional(&mut *conn)
            .await
            .map_err(db)?
            .ok_or_else(invalid)?;
        let start = number(descriptor.get("start"))?;
        let end = number(descriptor.get("end"))?;
        let origin_start = number(descriptor.get("origin_start"))?;
        if start > position || end <= position || end > view_length {
            return Err(invalid());
        }
        let origin = add(origin_start, position - start)?;
        let origin_end = add(origin_start, end - start)?;
        let id: &str = descriptor.get("origin_id");
        let (source_start, source_end, text, owner_length) = match descriptor
            .get::<&str, _>("origin_kind")
        {
            "root" if id == root => {
                let (a, b, text) = super::source::source_piece(conn, id, integer(origin)?).await?;
                (a, b, text, root_length)
            }
            "text" => {
                let row = sqlx::query(TEXT_PIECE)
                    .bind(operation)
                    .bind(id)
                    .bind(integer(origin)?)
                    .fetch_optional(&mut *conn)
                    .await
                    .map_err(db)?
                    .ok_or_else(invalid)?;
                (
                    row.get("start"),
                    row.get("end"),
                    row.get::<Option<String>, _>("text").ok_or_else(invalid)?,
                    number(row.get("owner_length"))?,
                )
            }
            _ => return Err(invalid()),
        };
        let source_start = number(source_start)?;
        let source_end = number(source_end)?;
        if source_start > origin
            || source_end <= origin
            || source_end > owner_length
            || origin_end > owner_length
            || text.len() > 4096
            || source_end - source_start
                != u64::try_from(text.encode_utf16().count()).map_err(db)?
        {
            return Err(invalid());
        }
        let from = byte_at(&text, origin - source_start)?;
        let to = byte_at(&text, source_end.min(origin_end) - source_start)?;
        for c in text[from..to].chars() {
            if c.len_utf8() > max_bytes - output.len() {
                return if output.is_empty() {
                    Err(invalid())
                } else {
                    Ok((position, output))
                };
            }
            output.push(c);
            position = add(
                position,
                u64::try_from(c.len_utf16()).expect("scalar UTF16 size fits"),
            )?;
        }
        if output.len() == max_bytes {
            break;
        }
    }
    if position < view_length && output.is_empty() {
        return Err(invalid());
    }
    Ok((position, output))
}

#[cfg(test)]
#[path = "view_read_tests.rs"]
mod tests;
