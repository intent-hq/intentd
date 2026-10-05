//! Bounded validation of uploaded stage streams. These helpers do not mark an
//! operation sealed: the owner must also finish text/reference, frozen-view and
//! projection validation, then publish the manifest in the same transaction.
use intent_core::{
    note_mutation::NoteMutationError,
    note_stage::{
        NoteStageAppend, NoteStageHeader, NoteStageRecord, NoteStageSeal, NoteStageStream,
        NoteStageTail, NoteStageTextReference, NOTE_STAGE_STREAMS,
    },
    Error, Result,
};
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::{Row, SqliteConnection};

fn db(error: impl std::fmt::Display) -> Error {
    Error::Internal(format!("note stage seal: {error}"))
}
fn hex_digest(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    bytes
        .iter()
        .flat_map(|b| {
            [
                char::from(HEX[usize::from(b >> 4)]),
                char::from(HEX[usize::from(b & 15)]),
            ]
        })
        .collect()
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
/// Work is O(uploaded record bytes + chunks); resident values are bounded by a
/// constant multiple of one chunk of <=64KiB encoded records.
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

/// Verified raw UTF-8 digest and scalar extents for one operation-owned text ID.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct VerifiedText {
    pub(super) length: u64,
    pub(super) utf8_bytes: u64,
    pub(super) sha256: String,
}

/// Stream one text ID through its indexed pieces, without reconstructing it.
/// Work is O(text bytes + pieces), retaining only one <=4096-byte source piece.
/// The owner may cache this result transactionally during seal; callers must not
/// repeat this whole-text verification for each page or each referencing record.
pub(super) async fn verify_text(
    conn: &mut SqliteConnection,
    operation: &str,
    text_id: &str,
) -> Result<VerifiedText> {
    let row = sqlx::query(
        "SELECT length,utf8_bytes FROM note_stage_text WHERE operation_key=? AND text_id=?",
    )
    .bind(operation)
    .bind(text_id)
    .fetch_optional(&mut *conn)
    .await
    .map_err(db)?
    .ok_or_else(invalid)?;
    let length = u64::try_from(row.get::<i64, _>("length")).map_err(|_| invalid())?;
    let utf8_bytes = u64::try_from(row.get::<i64, _>("utf8_bytes")).map_err(|_| invalid())?;
    if length > 9_007_199_254_740_991 || utf8_bytes > 9_007_199_254_740_991 {
        return Err(invalid());
    }
    let negative: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM note_stage_text_piece WHERE operation_key=? AND text_id=? AND start<0)")
        .bind(operation).bind(text_id).fetch_one(&mut *conn).await.map_err(db)?;
    if negative {
        return Err(invalid());
    }
    let mut last_start = -1_i64;
    let mut position = 0_u64;
    let mut bytes = 0_u64;
    let mut hash = Sha256::new();
    loop {
        let row = sqlx::query("SELECT start,end,CASE WHEN length(CAST(text AS BLOB))<=4096 THEN text END AS text FROM note_stage_text_piece WHERE operation_key=? AND text_id=? AND start>? ORDER BY start LIMIT 1")
            .bind(operation).bind(text_id).bind(last_start).fetch_optional(&mut *conn).await.map_err(db)?;
        let Some(row) = row else {
            break;
        };
        let start = row.get::<i64, _>("start");
        let end = u64::try_from(row.get::<i64, _>("end")).map_err(|_| invalid())?;
        let text = row.get::<Option<&str>, _>("text").ok_or_else(invalid)?;
        let piece_length = u64::try_from(text.encode_utf16().count()).map_err(db)?;
        if u64::try_from(start).map_err(|_| invalid())? != position
            || text.is_empty()
            || end != position.checked_add(piece_length).ok_or_else(invalid)?
            || end > length
        {
            return Err(invalid());
        }
        bytes = bytes
            .checked_add(u64::try_from(text.len()).map_err(db)?)
            .ok_or_else(invalid)?;
        if bytes > utf8_bytes {
            return Err(invalid());
        }
        hash.update(text.as_bytes());
        position = end;
        last_start = start;
    }
    if position != length || bytes != utf8_bytes {
        return Err(invalid());
    }
    let sha256 = hex_digest(hash.finalize().as_ref());
    Ok(VerifiedText {
        length,
        utf8_bytes,
        sha256,
    })
}

/// Structural fields only. This is not editor-schema or role-semantic admission.
/// The caller must validate operation-owned attributes and actual earlier parent
/// existence, frozen source coordinates, and current output-adapter support.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct ProjectionDescriptor<'a> {
    pub(super) node_type: &'a str,
    pub(super) parent_ordinal: Option<u64>,
    pub(super) native_from: u64,
    pub(super) native_to: u64,
    pub(super) attributes_ref: Option<&'a str>,
}

/// Check the resolved version-1 shape without inventing a native node allowlist.
/// `value` must come from the operation-owned, digest-verified detail text. The
/// caller supplies a bounded/streaming JSON reader; this does not load text IDs.
pub(super) fn projection_descriptor(
    value: &Value,
    ordinal: u64,
) -> Result<ProjectionDescriptor<'_>> {
    const SAFE: u64 = 9_007_199_254_740_991;
    let object = value.as_object().ok_or_else(invalid)?;
    if object.keys().any(|key| {
        !matches!(
            key.as_str(),
            "version" | "nodeType" | "parentOrdinal" | "nativeRange" | "attributesRef"
        )
    }) || ordinal > SAFE
        || object.get("version").and_then(Value::as_u64) != Some(1)
    {
        return Err(invalid());
    }
    let node_type = object
        .get("nodeType")
        .and_then(Value::as_str)
        .ok_or_else(invalid)?;
    if node_type.is_empty() || node_type.len() > 1024 || node_type.contains('\0') {
        return Err(invalid());
    }
    let parent_ordinal = match object.get("parentOrdinal") {
        Some(Value::Null) => None,
        Some(value) => Some(
            value
                .as_u64()
                .filter(|parent| *parent < ordinal)
                .ok_or_else(invalid)?,
        ),
        None => return Err(invalid()),
    };
    let range = object
        .get("nativeRange")
        .and_then(Value::as_object)
        .ok_or_else(invalid)?;
    if range.len() != 2 {
        return Err(invalid());
    }
    let native_from = range
        .get("from")
        .and_then(Value::as_u64)
        .ok_or_else(invalid)?;
    let native_to = range
        .get("to")
        .and_then(Value::as_u64)
        .ok_or_else(invalid)?;
    if native_from > native_to || native_to > SAFE {
        return Err(invalid());
    }
    let attributes_ref = match object.get("attributesRef") {
        None => None,
        Some(value) => {
            let reference = value.as_str().ok_or_else(invalid)?;
            if reference.is_empty() || reference.len() > 256 || reference.contains('\0') {
                return Err(invalid());
            }
            Some(reference)
        }
    };
    Ok(ProjectionDescriptor {
        node_type,
        parent_ordinal,
        native_from,
        native_to,
        attributes_ref,
    })
}

const SAFE_LENGTH: u64 = 9_007_199_254_740_991;

/// A prepared immutable piece view, not a published sealed operation. Resolved
/// live descriptor/metadata graph and provenance checks must finish before the
/// owning transaction may publish it. The wrapper checks current authorization,
/// cancellation and ORIGINAL expiry again immediately before publication.
#[derive(Debug)]
pub(super) struct PreparedView {
    pub(super) view_id: String,
    pub(super) length: u64,
    pub(super) generation: u64,
}

fn integer(value: u64) -> Result<i64> {
    i64::try_from(value).map_err(|_| invalid())
}
fn extent(value: i64) -> Result<u64> {
    u64::try_from(value)
        .ok()
        .filter(|value| *value <= SAFE_LENGTH)
        .ok_or_else(invalid)
}
fn sum(a: u64, b: u64) -> Result<u64> {
    a.checked_add(b)
        .filter(|value| *value <= SAFE_LENGTH)
        .ok_or_else(invalid)
}

/// Verify all text IDs once at seal, keeping their digests in indexed storage.
/// Appends must clear a text's cache on mutation. Do not reuse this cache outside
/// the enclosing immutable seal transaction until the operation is sealed.
async fn cache_text_digests(conn: &mut SqliteConnection, operation: &str) -> Result<()> {
    let mut after: Option<String> = None;
    loop {
        let id:Option<String> = match &after {
            None => sqlx::query_scalar("SELECT text_id FROM note_stage_text WHERE operation_key=? ORDER BY text_id LIMIT 1")
                .bind(operation).fetch_optional(&mut *conn).await.map_err(db)?,
            Some(after) => sqlx::query_scalar("SELECT text_id FROM note_stage_text WHERE operation_key=? AND text_id>? ORDER BY text_id LIMIT 1")
                .bind(operation).bind(after).fetch_optional(&mut *conn).await.map_err(db)?,
        };
        let Some(id) = id else {
            break;
        };
        if id.is_empty() || id.len() > 256 || id.contains('\0') {
            return Err(invalid());
        }
        let verified = verify_text(conn, operation, &id).await?;
        sqlx::query("UPDATE note_stage_text SET sha256=? WHERE operation_key=? AND text_id=?")
            .bind(verified.sha256)
            .bind(operation)
            .bind(&id)
            .execute(&mut *conn)
            .await
            .map_err(db)?;
        after = Some(id);
    }
    Ok(())
}

async fn verify_reference(
    conn: &mut SqliteConnection,
    operation: &str,
    reference: &NoteStageTextReference,
) -> Result<()> {
    let row = sqlx::query(
        "SELECT length,utf8_bytes,sha256 FROM note_stage_text WHERE operation_key=? AND text_id=?",
    )
    .bind(operation)
    .bind(&reference.text_id)
    .fetch_optional(&mut *conn)
    .await
    .map_err(db)?
    .ok_or_else(invalid)?;
    if extent(row.get("length"))? != reference.length
        || extent(row.get("utf8_bytes"))? != reference.utf8_bytes
        || row.get::<Option<String>, _>("sha256").as_deref() != Some(reference.sha256.as_str())
    {
        return Err(invalid());
    }
    Ok(())
}

// Primary-key keyset continuation, one <=64KiB encoded record at a time. Semantic
// ordinals may reset per history group; chunk-local ordinals never do.
async fn next_record(
    conn: &mut SqliteConnection,
    operation: &str,
    stream: &str,
    after: &mut (i64, i64),
) -> Result<Option<NoteStageRecord>> {
    let row=sqlx::query("SELECT chunk_sequence,ordinal,value FROM note_stage_record WHERE operation_key=? AND stream=? AND (chunk_sequence,ordinal)>(?,?) ORDER BY chunk_sequence,ordinal LIMIT 1")
        .bind(operation).bind(stream).bind(after.0).bind(after.1).fetch_optional(&mut *conn).await.map_err(db)?;
    let Some(row) = row else { return Ok(None) };
    *after = (row.get("chunk_sequence"), row.get("ordinal"));
    let raw: &str = row.get("value");
    if raw.len() > 65536 {
        return Err(invalid());
    }
    Ok(Some(serde_json::from_str(raw).map_err(db)?))
}

struct ViewPiece {
    start: u64,
    end: u64,
    origin_kind: String,
    origin_id: String,
    origin_start: u64,
}
async fn view_piece(
    conn: &mut SqliteConnection,
    operation: &str,
    generation: u64,
    offset: u64,
) -> Result<ViewPiece> {
    let row=sqlx::query("SELECT start,end,origin_kind,origin_id,origin_start FROM note_stage_view_piece WHERE operation_key=? AND generation=? AND start<=? ORDER BY start DESC LIMIT 1")
        .bind(operation).bind(integer(generation)?).bind(integer(offset)?).fetch_optional(&mut *conn).await.map_err(db)?.ok_or_else(invalid)?;
    let piece = ViewPiece {
        start: extent(row.get("start"))?,
        end: extent(row.get("end"))?,
        origin_kind: row.get("origin_kind"),
        origin_id: row.get("origin_id"),
        origin_start: extent(row.get("origin_start"))?,
    };
    if piece.start > offset || piece.end <= offset {
        return Err(invalid());
    }
    Ok(piece)
}

fn scalar_boundary(text: &str, offset: u64) -> bool {
    let mut units = 0;
    for c in text.chars() {
        if units == offset {
            return true;
        }
        units += u64::try_from(c.len_utf16()).expect("scalar UTF16 length fits");
        if units > offset {
            return false;
        }
    }
    units == offset
}

async fn view_boundary(
    conn: &mut SqliteConnection,
    operation: &str,
    generation: u64,
    length: u64,
    offset: u64,
) -> Result<()> {
    if offset > length {
        return Err(invalid());
    }
    if offset == 0 || offset == length {
        return Ok(());
    }
    let piece = view_piece(conn, operation, generation, offset).await?;
    if offset == piece.start {
        return Ok(());
    }
    let origin = sum(piece.origin_start, offset - piece.start)?;
    let (start,end,text)=match piece.origin_kind.as_str() {
        "root"=>super::source::source_piece(conn,&piece.origin_id,integer(origin)?).await?,
        "text"=>sqlx::query_as::<_,(i64,i64,String)>("SELECT start,end,text FROM note_stage_text_piece WHERE operation_key=? AND text_id=? AND start<=? ORDER BY start DESC LIMIT 1")
            .bind(operation).bind(&piece.origin_id).bind(integer(origin)?).fetch_optional(&mut *conn).await.map_err(db)?.ok_or_else(invalid)?,
        _=>return Err(invalid()),
    };
    let start = extent(start)?;
    let end = extent(end)?;
    if start > origin
        || end <= origin
        || text.len() > 4096
        || end - start != u64::try_from(text.encode_utf16().count()).map_err(db)?
        || !scalar_boundary(&text, origin - start)
    {
        return Err(invalid());
    }
    Ok(())
}

async fn insert_piece(
    conn: &mut SqliteConnection,
    operation: &str,
    generation: u64,
    output: &mut u64,
    length: u64,
    origin: (&str, &str, u64),
) -> Result<()> {
    if length == 0 {
        return Ok(());
    }
    let (kind, id, origin_start) = origin;
    let end = sum(*output, length)?;
    sqlx::query("INSERT INTO note_stage_view_piece(operation_key,generation,start,end,origin_kind,origin_id,origin_start) VALUES(?,?,?,?,?,?,?)")
        .bind(operation).bind(integer(generation)?).bind(integer(*output)?).bind(integer(end)?).bind(kind).bind(id).bind(integer(origin_start)?).execute(&mut *conn).await.map_err(db)?;
    *output = end;
    Ok(())
}

async fn copy_range(
    conn: &mut SqliteConnection,
    operation: &str,
    input: u64,
    output_generation: u64,
    start: u64,
    end: u64,
    output: &mut u64,
) -> Result<()> {
    let mut position = start;
    while position < end {
        let piece = view_piece(conn, operation, input, position).await?;
        let next = end.min(piece.end);
        insert_piece(
            conn,
            operation,
            output_generation,
            output,
            next - position,
            (
                &piece.origin_kind,
                &piece.origin_id,
                sum(piece.origin_start, position - piece.start)?,
            ),
        )
        .await?;
        position = next;
    }
    Ok(())
}

struct DirtyGroup {
    local_sequence: u64,
    generation: u64,
    input_generation: u64,
    input_length: u64,
    consumed: u64,
    output: u64,
}
async fn finish_group(
    conn: &mut SqliteConnection,
    operation: &str,
    mut group: DirtyGroup,
) -> Result<(u64, u64)> {
    copy_range(
        conn,
        operation,
        group.input_generation,
        group.generation,
        group.consumed,
        group.input_length,
        &mut group.output,
    )
    .await?;
    sqlx::query("UPDATE note_stage_view SET length=? WHERE operation_key=? AND generation=?")
        .bind(integer(group.output)?)
        .bind(operation)
        .bind(integer(group.generation)?)
        .execute(&mut *conn)
        .await
        .map_err(db)?;
    Ok((group.generation, group.output))
}

/// Prepare chronological external piece views in the caller's write transaction.
/// Every error requires rollback, including cached hashes and partial generations.
/// Does NOT publish a sealed phase: live metadata/provenance validation is still
/// required. Work is O(upload bytes + records + copied descriptors per group),
/// and retained descriptor storage may grow with every history group. No complete
/// source string or operation-sized collection enters Rust. Mutation records are
/// checked against the final DIRTY view but are not applied during this phase.
pub(super) async fn prepare_frozen_view(
    conn: &mut SqliteConnection,
    operation: &str,
    header: &NoteStageHeader,
    request: &NoteStageSeal,
    root_key: &str,
) -> Result<PreparedView> {
    verify_manifest(conn, operation, header, request).await?;
    cache_text_digests(conn, operation).await?;
    let base: i64 =
        sqlx::query_scalar("SELECT source_length FROM note_stage_root WHERE root_key=?")
            .bind(root_key)
            .fetch_optional(&mut *conn)
            .await
            .map_err(db)?
            .ok_or_else(invalid)?;
    let mut length = extent(base)?;
    let mut generation = 0;
    sqlx::query("INSERT INTO note_stage_view(operation_key,generation,input_generation,history_group,length) VALUES(?,0,NULL,NULL,?)")
        .bind(operation).bind(base).execute(&mut *conn).await.map_err(db)?;
    let mut output = 0;
    insert_piece(
        conn,
        operation,
        0,
        &mut output,
        length,
        ("root", root_key, 0),
    )
    .await?;
    let mut group: Option<DirtyGroup> = None;
    let mut after = (-1, -1);
    while let Some(record) = next_record(conn, operation, "dirty", &mut after).await? {
        let NoteStageRecord::Splice {
            local_sequence: Some(local_sequence),
            start,
            end,
            replacement,
            ..
        } = record
        else {
            return Err(invalid());
        };
        verify_reference(conn, operation, &replacement).await?;
        if group
            .as_ref()
            .is_none_or(|group| group.local_sequence != local_sequence)
        {
            if let Some(prior) = group.take() {
                (generation, length) = finish_group(conn, operation, prior).await?;
            }
            let next = sum(generation, 1)?;
            sqlx::query("INSERT INTO note_stage_view(operation_key,generation,input_generation,history_group,length) VALUES(?,?,?,?,0)")
                .bind(operation).bind(integer(next)?).bind(integer(generation)?).bind(local_sequence.to_string()).execute(&mut *conn).await.map_err(db)?;
            group = Some(DirtyGroup {
                local_sequence,
                generation: next,
                input_generation: generation,
                input_length: length,
                consumed: 0,
                output: 0,
            });
        }
        let current = group.as_mut().ok_or_else(invalid)?;
        if start < current.consumed || end < start || end > current.input_length {
            return Err(invalid());
        }
        view_boundary(
            conn,
            operation,
            current.input_generation,
            current.input_length,
            start,
        )
        .await?;
        view_boundary(
            conn,
            operation,
            current.input_generation,
            current.input_length,
            end,
        )
        .await?;
        copy_range(
            conn,
            operation,
            current.input_generation,
            current.generation,
            current.consumed,
            start,
            &mut current.output,
        )
        .await?;
        insert_piece(
            conn,
            operation,
            current.generation,
            &mut current.output,
            replacement.length,
            ("text", &replacement.text_id, 0),
        )
        .await?;
        current.consumed = end;
    }
    if let Some(group) = group {
        (generation, length) = finish_group(conn, operation, group).await?;
    }
    for stream in ["selection", "mutation", "live"] {
        let mut after = (-1, -1);
        while let Some(record) = next_record(conn, operation, stream, &mut after).await? {
            let (start, end) = match record {
                NoteStageRecord::Range { start, end, .. } => (start, end),
                NoteStageRecord::Splice {
                    start,
                    end,
                    replacement,
                    ..
                } => {
                    verify_reference(conn, operation, &replacement).await?;
                    (start, end)
                }
                NoteStageRecord::Projection {
                    source_range,
                    detail,
                    ..
                } => {
                    verify_reference(conn, operation, &detail).await?;
                    (source_range.start, source_range.end)
                }
                NoteStageRecord::Text { .. } => return Err(invalid()),
            };
            if end < start {
                return Err(invalid());
            }
            view_boundary(conn, operation, generation, length, start).await?;
            view_boundary(conn, operation, generation, length, end).await?;
        }
    }
    Ok(PreparedView {
        view_id: uuid::Uuid::new_v4().to_string(),
        length,
        generation,
    })
}

#[cfg(test)]
#[path = "seal_tests.rs"]
mod tests;
