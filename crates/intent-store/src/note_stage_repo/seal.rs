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
                    .advance_for_header(entry.stream, &parsed, header)
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
    projection_descriptor_version(value, ordinal, 1)
}

fn projection_descriptor_version(
    value: &Value,
    ordinal: u64,
    version: u64,
) -> Result<ProjectionDescriptor<'_>> {
    const SAFE: u64 = 9_007_199_254_740_991;
    let object = value.as_object().ok_or_else(invalid)?;
    if object.keys().any(|key| {
        !(matches!(
            key.as_str(),
            "version" | "nodeType" | "parentOrdinal" | "nativeRange" | "attributesRef"
        ) || (version == 2 && key == "renderedText"))
    }) || ordinal > SAFE
        || object.get("version").and_then(Value::as_u64) != Some(version)
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

pub(super) async fn verify_reference(
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
pub(super) async fn next_record(
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

const METADATA_BYTES: u64 = 16_384;

fn token_value(value: &Value) -> Result<&str> {
    value
        .as_str()
        .filter(|s| !s.is_empty() && s.len() <= 256 && !s.contains('\0'))
        .ok_or_else(invalid)
}
fn optional_token<'a>(
    object: &'a serde_json::Map<String, Value>,
    key: &str,
) -> Result<Option<&'a str>> {
    object.get(key).map(token_value).transpose()
}
async fn cached_text(
    conn: &mut SqliteConnection,
    operation: &str,
    id: &str,
) -> Result<VerifiedText> {
    let row = sqlx::query(
        "SELECT length,utf8_bytes,sha256 FROM note_stage_text WHERE operation_key=? AND text_id=?",
    )
    .bind(operation)
    .bind(id)
    .fetch_optional(&mut *conn)
    .await
    .map_err(db)?
    .ok_or_else(invalid)?;
    Ok(VerifiedText {
        length: extent(row.get("length"))?,
        utf8_bytes: extent(row.get("utf8_bytes"))?,
        sha256: row.get::<Option<String>, _>("sha256").ok_or_else(invalid)?,
    })
}

// Only a specifically referenced entry/directory is reconstructed, and only
// after its verified byte count passes the logical resource budget. Raw scalar
// resources are never parsed as JSON or loaded wholesale by this function.
pub(super) async fn metadata_resource(
    conn: &mut SqliteConnection,
    operation: &str,
    id: &str,
) -> Result<Value> {
    let metadata = cached_text(conn, operation, id).await?;
    if metadata.utf8_bytes > METADATA_BYTES {
        return Err(Error::NoteMutation(NoteMutationError::Budget));
    }
    let mut raw = String::with_capacity(usize::try_from(metadata.utf8_bytes).map_err(db)?);
    let mut offset = 0;
    while offset < metadata.length {
        let (end,text):(i64,String)=sqlx::query_as("SELECT end,text FROM note_stage_text_piece WHERE operation_key=? AND text_id=? AND start=?")
            .bind(operation).bind(id).bind(integer(offset)?).fetch_optional(&mut *conn).await.map_err(db)?.ok_or_else(invalid)?;
        let end = extent(end)?;
        if end <= offset
            || end > metadata.length
            || text.len() > 4096
            || u64::try_from(text.encode_utf16().count()).map_err(db)? != end - offset
        {
            return Err(invalid());
        }
        if raw.len().saturating_add(text.len()) > usize::try_from(METADATA_BYTES).map_err(db)? {
            return Err(Error::NoteMutation(NoteMutationError::Budget));
        }
        raw.push_str(&text);
        offset = end;
    }
    if u64::try_from(raw.len()).map_err(db)? != metadata.utf8_bytes
        || hex_digest(Sha256::digest(raw.as_bytes()).as_ref()) != metadata.sha256
    {
        return Err(invalid());
    }
    // Supported entry/directory shapes have <=64 child IDs and fixed shallow
    // fields, safely below this canonicalizer's structural limits. This is not
    // reused for the separate 128-record staged integrity envelope.
    let canonical =
        intent_core::note_artifact::canonical::canonical_json(&raw).map_err(|_| invalid())?;
    if canonical != raw {
        return Err(invalid());
    }
    serde_json::from_str(&raw).map_err(|_| invalid())
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
enum MetadataKey {
    Inline(String),
    Text(String),
}
struct MetadataEntry {
    id: String,
    kind: String,
    key: Option<MetadataKey>,
    index: Option<u64>,
    children: Option<String>,
}
async fn metadata_entry(
    conn: &mut SqliteConnection,
    operation: &str,
    value: &Value,
    parent: Option<&str>,
    parent_kind: Option<&str>,
) -> Result<MetadataEntry> {
    let object = value.as_object().ok_or_else(invalid)?;
    if object.keys().any(|key| {
        !matches!(
            key.as_str(),
            "id" | "parentId"
                | "key"
                | "keyRef"
                | "index"
                | "type"
                | "value"
                | "valueRef"
                | "childrenRef"
        )
    }) {
        return Err(invalid());
    }
    let id = token_value(object.get("id").ok_or_else(invalid)?)?.to_owned();
    let parent_id = match object.get("parentId") {
        Some(Value::Null) => None,
        Some(value) => Some(token_value(value)?),
        None => return Err(invalid()),
    };
    if parent_id != parent {
        return Err(invalid());
    }
    let key = match (object.get("key"), optional_token(object, "keyRef")?) {
        (None, None) => None,
        (Some(Value::String(key)), None) if key.len() <= 1024 => {
            Some(MetadataKey::Inline(key.clone()))
        }
        (None, Some(id)) => {
            cached_text(conn, operation, id).await?;
            Some(MetadataKey::Text(id.to_owned()))
        }
        _ => return Err(invalid()),
    };
    let index = object
        .get("index")
        .map(|value| {
            value
                .as_u64()
                .filter(|n| *n <= SAFE_LENGTH)
                .ok_or_else(invalid)
        })
        .transpose()?;
    match parent_kind {
        None if key.is_none() && index.is_none() => (),
        Some("object") if key.is_some() && index.is_none() => (),
        Some("array") if key.is_none() && index.is_some() => (),
        _ => return Err(invalid()),
    }
    let kind = object
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(invalid)?;
    let children = optional_token(object, "childrenRef")?;
    let value_ref = optional_token(object, "valueRef")?;
    let scalar = object.get("value");
    match kind {
        "object" | "array" if children.is_some() && value_ref.is_none() && scalar.is_none() => (),
        "string" if children.is_none() && scalar.is_none() => {
            cached_text(conn, operation, value_ref.ok_or_else(invalid)?).await?;
        }
        "number"
            if children.is_none()
                && value_ref.is_none()
                && scalar.is_some_and(Value::is_number) => {}
        "boolean"
            if children.is_none()
                && value_ref.is_none()
                && scalar.is_some_and(Value::is_boolean) => {}
        "null" if children.is_none() && value_ref.is_none() && scalar == Some(&Value::Null) => (),
        _ => return Err(invalid()),
    }
    Ok(MetadataEntry {
        id,
        kind: kind.to_owned(),
        key,
        index,
        children: children.map(str::to_owned),
    })
}

async fn claim_metadata(
    conn: &mut SqliteConnection,
    operation: &str,
    kind: &str,
    id: &str,
    owner: Option<&str>,
    position: Option<u64>,
    value: &Value,
) -> Result<()> {
    let value = serde_json::to_string(value).map_err(db)?;
    if value.len() > 32768 {
        return Err(Error::NoteMutation(NoteMutationError::Budget));
    }
    let inserted=sqlx::query("INSERT INTO note_stage_validation(operation_key,kind,id,owner,position,state,value) VALUES(?,?,?,?,?,'active',?) ON CONFLICT(operation_key,kind,id) DO NOTHING")
        .bind(operation).bind(kind).bind(id).bind(owner).bind(position.map(integer).transpose()?).bind(value).execute(&mut *conn).await.map_err(db)?;
    if inserted.rows_affected() != 1 {
        return Err(invalid());
    }
    Ok(())
}
async fn finish_metadata(
    conn: &mut SqliteConnection,
    operation: &str,
    text_id: &str,
) -> Result<()> {
    sqlx::query("UPDATE note_stage_validation SET state='done' WHERE operation_key=? AND kind='entry' AND id=?")
        .bind(operation).bind(text_id).execute(&mut *conn).await.map_err(db)?;
    Ok(())
}
async fn admit_metadata(
    conn: &mut SqliteConnection,
    operation: &str,
    text_id: &str,
    parent: Option<&str>,
    parent_kind: Option<&str>,
) -> Result<MetadataEntry> {
    let value = metadata_resource(conn, operation, text_id).await?;
    let entry = metadata_entry(conn, operation, &value, parent, parent_kind).await?;
    claim_metadata(conn, operation, "entry", text_id, parent, None, &value).await?;
    claim_metadata(
        conn,
        operation,
        "entryId",
        &entry.id,
        Some(text_id),
        None,
        &Value::Null,
    )
    .await?;
    Ok(entry)
}

#[derive(serde::Serialize, serde::Deserialize)]
struct MetadataFrame {
    text_id: String,
    entry_id: String,
    kind: String,
    directory: Option<String>,
    next_directory: Option<String>,
    has_directory: bool,
    item: usize,
    previous_key: Option<MetadataKey>,
    next_index: u64,
}
async fn push_metadata(
    conn: &mut SqliteConnection,
    operation: &str,
    depth: u64,
    text_id: &str,
    entry: MetadataEntry,
) -> Result<()> {
    let frame = MetadataFrame {
        text_id: text_id.to_owned(),
        entry_id: entry.id,
        kind: entry.kind,
        directory: None,
        next_directory: entry.children,
        has_directory: false,
        item: 0,
        previous_key: None,
        next_index: 0,
    };
    claim_metadata(
        conn,
        operation,
        "stack",
        &depth.to_string(),
        Some(text_id),
        Some(depth),
        &serde_json::to_value(frame).map_err(db)?,
    )
    .await
}
async fn save_metadata_frame(
    conn: &mut SqliteConnection,
    operation: &str,
    depth: u64,
    frame: &MetadataFrame,
) -> Result<()> {
    let value = serde_json::to_string(frame).map_err(db)?;
    if value.len() > 32768 {
        return Err(Error::NoteMutation(NoteMutationError::Budget));
    }
    sqlx::query(
        "UPDATE note_stage_validation SET value=? WHERE operation_key=? AND kind='stack' AND id=?",
    )
    .bind(value)
    .bind(operation)
    .bind(depth.to_string())
    .execute(&mut *conn)
    .await
    .map_err(db)?;
    Ok(())
}

struct MetadataDirectory {
    next: Option<String>,
}
fn metadata_directory(value: &Value, first: bool) -> Result<MetadataDirectory> {
    let object = value.as_object().ok_or_else(invalid)?;
    if object.len() != 3 || object.get("kind").and_then(Value::as_str) != Some("metadataChildren") {
        return Err(invalid());
    }
    let items = object
        .get("items")
        .and_then(Value::as_array)
        .ok_or_else(invalid)?;
    if items.len() > 64 {
        return Err(Error::NoteMutation(NoteMutationError::Budget));
    }
    for item in items {
        token_value(item)?;
    }
    let next = match object.get("nextRef") {
        Some(Value::Null) => None,
        Some(value) => Some(token_value(value)?.to_owned()),
        None => return Err(invalid()),
    };
    if items.is_empty() && (!first || next.is_some()) {
        return Err(invalid());
    }
    Ok(MetadataDirectory { next })
}

struct MetadataKeyReader {
    text_id: Option<String>,
    length: u64,
    next_offset: u64,
    bytes: Vec<u8>,
    byte: usize,
}
impl MetadataKeyReader {
    async fn new(conn: &mut SqliteConnection, operation: &str, key: &MetadataKey) -> Result<Self> {
        match key {
            MetadataKey::Inline(key) => Ok(Self {
                text_id: None,
                length: 0,
                next_offset: 0,
                bytes: key.as_bytes().to_vec(),
                byte: 0,
            }),
            MetadataKey::Text(id) => Ok(Self {
                text_id: Some(id.clone()),
                length: cached_text(conn, operation, id).await?.length,
                next_offset: 0,
                bytes: vec![],
                byte: 0,
            }),
        }
    }
    async fn next(&mut self, conn: &mut SqliteConnection, operation: &str) -> Result<Option<u8>> {
        if self.byte == self.bytes.len() {
            let Some(id) = &self.text_id else {
                return Ok(None);
            };
            if self.next_offset == self.length {
                return Ok(None);
            }
            let (end,text):(i64,String)=sqlx::query_as("SELECT end,text FROM note_stage_text_piece WHERE operation_key=? AND text_id=? AND start=?")
                .bind(operation).bind(id).bind(integer(self.next_offset)?).fetch_optional(&mut *conn).await.map_err(db)?.ok_or_else(invalid)?;
            let end = extent(end)?;
            if end <= self.next_offset
                || end > self.length
                || text.len() > 4096
                || u64::try_from(text.encode_utf16().count()).map_err(db)? != end - self.next_offset
            {
                return Err(invalid());
            }
            self.next_offset = end;
            self.bytes = text.into_bytes();
            self.byte = 0;
        }
        let byte = *self.bytes.get(self.byte).ok_or_else(invalid)?;
        self.byte += 1;
        Ok(Some(byte))
    }
}
async fn metadata_key_order(
    conn: &mut SqliteConnection,
    operation: &str,
    a: &MetadataKey,
    b: &MetadataKey,
) -> Result<std::cmp::Ordering> {
    let mut a = MetadataKeyReader::new(conn, operation, a).await?;
    let mut b = MetadataKeyReader::new(conn, operation, b).await?;
    loop {
        match (
            a.next(conn, operation).await?,
            b.next(conn, operation).await?,
        ) {
            (None, None) => return Ok(std::cmp::Ordering::Equal),
            (None, Some(_)) => return Ok(std::cmp::Ordering::Less),
            (Some(_), None) => return Ok(std::cmp::Ordering::Greater),
            (Some(a), Some(b)) if a != b => return Ok(a.cmp(&b)),
            _ => (),
        }
    }
}

/// Resolve only explicit uploaded metadata edges, using an external indexed DFS
/// stack and ownership ledger. Resident state is bounded by individual <=16KiB
/// resources, <=64 directory IDs and two <=4096-byte long-key pieces, independent
/// of graph depth/size. Each directory can be decoded once per child (<=64 times);
/// key comparisons stream common prefixes. Work/storage are graph-dependent.
///
/// Requires the caller's exact operation binding and immutable text digest cache
/// from `prepare_frozen_view`, inside the SAME writer transaction. All errors must
/// roll back. Completed exact roots can be shared; child/directory reuse, cycles
/// and identity aliases reject. This is structural, not editor role/provenance
/// authority, and must not itself publish a sealed operation.
pub(super) async fn validate_attribute_graph(
    conn: &mut SqliteConnection,
    operation: &str,
    root: &str,
) -> Result<()> {
    let pending: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM note_stage_validation WHERE operation_key=? AND kind='stack')",
    )
    .bind(operation)
    .fetch_one(&mut *conn)
    .await
    .map_err(db)?;
    if pending {
        return Err(invalid());
    }
    let prior=sqlx::query("SELECT owner,state FROM note_stage_validation WHERE operation_key=? AND kind='entry' AND id=?")
        .bind(operation).bind(root).fetch_optional(&mut *conn).await.map_err(db)?;
    if let Some(prior) = prior {
        if prior.get::<Option<String>, _>("owner").is_none()
            && prior.get::<&str, _>("state") == "done"
        {
            return Ok(());
        }
        return Err(invalid());
    }
    let entry = admit_metadata(conn, operation, root, None, None).await?;
    if entry.children.is_some() {
        push_metadata(conn, operation, 0, root, entry).await?;
    } else {
        finish_metadata(conn, operation, root).await?;
        return Ok(());
    }
    loop {
        let top=sqlx::query("SELECT position,value FROM note_stage_validation WHERE operation_key=? AND kind='stack' ORDER BY position DESC LIMIT 1")
            .bind(operation).fetch_optional(&mut *conn).await.map_err(db)?;
        let Some(top) = top else { return Ok(()) };
        let depth = extent(top.get("position"))?;
        let mut frame: MetadataFrame = serde_json::from_str(top.get("value")).map_err(db)?;
        if frame.directory.is_none() {
            let Some(next) = frame.next_directory.take() else {
                finish_metadata(conn, operation, &frame.text_id).await?;
                sqlx::query("DELETE FROM note_stage_validation WHERE operation_key=? AND kind='stack' AND position=?")
                    .bind(operation).bind(integer(depth)?).execute(&mut *conn).await.map_err(db)?;
                continue;
            };
            let value = metadata_resource(conn, operation, &next).await?;
            let directory = metadata_directory(&value, !frame.has_directory)?;
            claim_metadata(
                conn,
                operation,
                "directory",
                &next,
                Some(&frame.entry_id),
                None,
                &value,
            )
            .await?;
            frame.directory = Some(next);
            frame.next_directory = directory.next;
            frame.item = 0;
            frame.has_directory = true;
            save_metadata_frame(conn, operation, depth, &frame).await?;
        }
        let raw:String=sqlx::query_scalar("SELECT value FROM note_stage_validation WHERE operation_key=? AND kind='directory' AND id=? AND owner=?")
            .bind(operation).bind(frame.directory.as_deref()).bind(&frame.entry_id).fetch_one(&mut *conn).await.map_err(db)?;
        let directory: Value = serde_json::from_str(&raw).map_err(db)?;
        let items = directory["items"].as_array().ok_or_else(invalid)?;
        if frame.item == items.len() {
            frame.directory = None;
            save_metadata_frame(conn, operation, depth, &frame).await?;
            continue;
        }
        let child_id = token_value(items.get(frame.item).ok_or_else(invalid)?)?.to_owned();
        let child = admit_metadata(
            conn,
            operation,
            &child_id,
            Some(&frame.entry_id),
            Some(&frame.kind),
        )
        .await?;
        if frame.kind == "object" {
            let key = child.key.as_ref().ok_or_else(invalid)?;
            if let Some(prior) = &frame.previous_key {
                if metadata_key_order(conn, operation, prior, key).await?
                    != std::cmp::Ordering::Less
                {
                    return Err(invalid());
                }
            }
            frame.previous_key = Some(key.clone());
        } else {
            if child.index != Some(frame.next_index) {
                return Err(invalid());
            }
            frame.next_index = sum(frame.next_index, 1)?;
        }
        frame.item += 1;
        save_metadata_frame(conn, operation, depth, &frame).await?;
        if child.children.is_some() {
            push_metadata(conn, operation, sum(depth, 1)?, &child_id, child).await?;
        } else {
            finish_metadata(conn, operation, &child_id).await?;
        }
    }
}

/// Resolve the live stream into an operation-owned structural ledger. Requires
/// this exact operation's `PreparedView`, verified manifest/text cache and the
/// SAME immutable caller-owned writer transaction. The owner must still enforce
/// canonical marker provenance, supported output adapters, authorization and
/// original expiry/cancellation before publishing any sealed authority.
///
/// Detail JSON is <=16KiB, records use indexed one-record seeks, parent existence
/// uses the ledger PK, and attribute graphs use their external traversal stack.
/// Native ranges stay in native units; no source/native coordinate conversion or
/// node/mark allowlist is inferred here. All errors require outer rollback.
pub(super) async fn validate_live_descriptors(
    conn: &mut SqliteConnection,
    operation: &str,
    view: &PreparedView,
) -> Result<()> {
    let (generation,length):(i64,i64)=sqlx::query_as("SELECT generation,length FROM note_stage_view WHERE operation_key=? ORDER BY generation DESC LIMIT 1")
        .bind(operation).fetch_optional(&mut *conn).await.map_err(db)?.ok_or_else(invalid)?;
    if extent(generation)? != view.generation || extent(length)? != view.length {
        return Err(invalid());
    }
    let prior: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM note_stage_validation WHERE operation_key=? AND kind='live')",
    )
    .bind(operation)
    .fetch_one(&mut *conn)
    .await
    .map_err(db)?;
    if prior {
        return Err(invalid());
    }
    let mut after = (-1, -1);
    let mut next_ordinal = 0;
    while let Some(record) = next_record(conn, operation, "live", &mut after).await? {
        let NoteStageRecord::Projection {
            ordinal,
            source_range,
            role,
            canonical_id,
            detail,
        } = record
        else {
            return Err(invalid());
        };
        if ordinal != next_ordinal
            || ordinal >= SAFE_LENGTH
            || source_range.start > source_range.end
        {
            return Err(invalid());
        }
        next_ordinal = sum(ordinal, 1)?;
        verify_reference(conn, operation, &detail).await?;
        let value = metadata_resource(conn, operation, &detail.text_id).await?;
        let rendered: Option<NoteStageTextReference> = if value["version"] == 2 {
            // Version two is an explicit captured rendered-search resource, not
            // an extension accepted by existing version-one selection adapters.
            let raw: Option<String> =
                sqlx::query_scalar("SELECT header FROM note_stage WHERE operation_key=?")
                    .bind(operation)
                    .fetch_optional(&mut *conn)
                    .await
                    .map_err(db)?;
            let header: NoteStageHeader =
                serde_json::from_str(&raw.ok_or_else(invalid)?).map_err(db)?;
            header.validate().map_err(Error::NoteMutation)?;
            if header.action != intent_core::note_stage::NoteStageAction::Read
                || header.output != intent_core::note_stage::NoteStageOutput::Search
                || header.selection != intent_core::note_stage::NoteStageSelection::Ranges
                || !header.query.as_ref().is_some_and(|q| {
                    q.mode == intent_core::note_stage::NoteStageSearchMode::RenderedText
                })
                || ordinal != 1
                || role != intent_core::note_stage::NoteStageRole::InlineSpan
                || canonical_id.is_some()
                || value.as_object().is_none_or(|object| object.len() != 6)
                || value["nodeType"] != "text"
                || value["parentOrdinal"] != 0
                || !value["attributesRef"].is_string()
            {
                return Err(invalid());
            }
            let reference: NoteStageTextReference =
                serde_json::from_value(value["renderedText"].clone()).map_err(|_| invalid())?;
            if reference.length == 0 || reference.length > SAFE_LENGTH {
                return Err(invalid());
            }
            verify_reference(conn, operation, &reference).await?;
            Some(reference)
        } else {
            None
        };
        let descriptor = if rendered.is_some() {
            projection_descriptor_version(&value, ordinal, 2)?
        } else {
            projection_descriptor(&value, ordinal)?
        };
        if let Some(parent) = descriptor.parent_ordinal {
            let exists:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM note_stage_validation WHERE operation_key=? AND kind='live' AND id=? AND state='done')")
                .bind(operation).bind(parent.to_string()).fetch_one(&mut *conn).await.map_err(db)?;
            if !exists {
                return Err(invalid());
            }
        }
        view_boundary(
            conn,
            operation,
            view.generation,
            view.length,
            source_range.start,
        )
        .await?;
        view_boundary(
            conn,
            operation,
            view.generation,
            view.length,
            source_range.end,
        )
        .await?;
        if let Some(reference) = descriptor.attributes_ref {
            validate_attribute_graph(conn, operation, reference).await?;
        }
        let role = match role {
            intent_core::note_stage::NoteStageRole::SelectionOwner => "selection-owner",
            intent_core::note_stage::NoteStageRole::ParagraphSeam => "paragraph-seam",
            intent_core::note_stage::NoteStageRole::InlineSpan => "inline-span",
            intent_core::note_stage::NoteStageRole::MarkerOccurrence => "marker-occurrence",
        };
        let mut metadata = serde_json::json!({"generation":view.generation,"sourceRange":{"start":source_range.start,"end":source_range.end},"nativeRange":{"from":descriptor.native_from,"to":descriptor.native_to},"parentOrdinal":descriptor.parent_ordinal,"nodeType":descriptor.node_type,"role":role});
        if let Some(id) = canonical_id {
            metadata["canonicalId"] = Value::String(id);
        }
        if let Some(reference) = descriptor.attributes_ref {
            metadata["attributesRef"] = Value::String(reference.to_owned());
        }
        if let Some(reference) = rendered {
            metadata["renderedText"] = serde_json::to_value(reference).map_err(db)?;
        }
        claim_metadata(
            conn,
            operation,
            "live",
            &ordinal.to_string(),
            Some(&detail.text_id),
            Some(ordinal),
            &metadata,
        )
        .await?;
        sqlx::query("UPDATE note_stage_validation SET state='done' WHERE operation_key=? AND kind='live' AND id=?")
            .bind(operation).bind(ordinal.to_string()).execute(&mut *conn).await.map_err(db)?;
    }
    Ok(())
}

#[cfg(test)]
#[path = "seal_tests.rs"]
mod tests;
