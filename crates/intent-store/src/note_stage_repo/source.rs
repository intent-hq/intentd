//! Immutable staged base: metadata admission and two bounded piece seeks.
use intent_core::{note_mutation::NoteMutationError, note_page::NoteScope, Error, Result};
use sqlx::{Row, SqliteConnection};

fn db(error: impl std::fmt::Display) -> Error {
    Error::Internal(format!("staged note source: {error}"))
}
fn conflict() -> Error {
    Error::NoteMutation(NoteMutationError::Conflict)
}

pub(super) struct BaseRoot {
    pub key: String,
}

// The caller holds the writer for admission only, never across requests. Source
// pieces already belong to the indexed generation; begin does not enumerate them.
pub(super) async fn pin_base(
    conn: &mut SqliteConnection,
    scope: &NoteScope,
    revision: &str,
) -> Result<BaseRoot> {
    let backend: String =
        sqlx::query_scalar("SELECT backend_id FROM note_page_backend WHERE singleton=1")
            .fetch_one(&mut *conn)
            .await
            .map_err(db)?;
    if backend != scope.backend_id {
        return Err(conflict());
    }
    let row=sqlx::query("SELECT h.instance_id,h.current_rev,h.indexed_rev,h.profile_revision,h.generation,h.content_generation,h.source_length,h.source_bytes FROM note_page_head h WHERE h.workspace_id=? AND h.note_id=? AND NOT EXISTS(SELECT 1 FROM note_annotation_workspace_retirement r WHERE r.workspace_id=h.workspace_id)")
        .bind(&scope.workspace_id).bind(&scope.note_id).fetch_optional(&mut *conn).await.map_err(db)?.ok_or_else(conflict)?;
    let current: i64 = row.try_get("current_rev").map_err(db)?;
    if row.try_get::<String, _>("instance_id").map_err(db)? != scope.note_instance_id
        || row.try_get::<i64, _>("indexed_rev").map_err(db)? != current
        || row.try_get::<String, _>("profile_revision").map_err(db)?
            != crate::note_page_index::profile_revision()
        || format!(
            "r:{current}:{}",
            row.try_get::<String, _>("generation").map_err(db)?
        ) != revision
    {
        return Err(conflict());
    }
    let generation: String = row.try_get("content_generation").map_err(db)?;
    if generation.is_empty() {
        return Err(conflict());
    }
    let length: i64 = row.try_get("source_length").map_err(db)?;
    let bytes: i64 = row.try_get("source_bytes").map_err(db)?;
    if !(0..=9_007_199_254_740_991).contains(&length) || bytes < 0 {
        return Err(conflict());
    }
    let key = uuid::Uuid::new_v4().to_string();
    sqlx::query("INSERT INTO note_stage_root(root_key,workspace_id,note_id,instance_id,content_generation,source_length,source_bytes) VALUES(?,?,?,?,?,?,?) ON CONFLICT(workspace_id,note_id,instance_id,content_generation) DO NOTHING")
        .bind(&key).bind(&scope.workspace_id).bind(&scope.note_id).bind(&scope.note_instance_id).bind(&generation).bind(length).bind(bytes).execute(&mut *conn).await.map_err(db)?;
    let root:(String,i64,i64)=sqlx::query_as("SELECT root_key,source_length,source_bytes FROM note_stage_root WHERE workspace_id=? AND note_id=? AND instance_id=? AND content_generation=?")
        .bind(&scope.workspace_id).bind(&scope.note_id).bind(&scope.note_instance_id).bind(&generation).fetch_one(&mut *conn).await.map_err(db)?;
    if root.1 != length || root.2 != bytes {
        return Err(conflict());
    }
    Ok(BaseRoot { key: root.0 })
}

// Read under the request's single SQLite read snapshot, after checking original
// operation expiry/current authorization. Recreated notes cannot satisfy the
// pinned generation. At most two <=4096-byte rows enter Rust per seek.
pub(super) async fn source_piece(
    conn: &mut SqliteConnection,
    root: &str,
    offset: i64,
) -> Result<(i64, i64, String)> {
    let binding:(String,String,String,i64)=sqlx::query_as("SELECT workspace_id,note_id,content_generation,source_length FROM note_stage_root WHERE root_key=?")
        .bind(root).fetch_optional(&mut *conn).await.map_err(db)?.ok_or_else(conflict)?;
    if offset < 0 || offset >= binding.3 {
        return Err(Error::NoteMutation(NoteMutationError::Invalid));
    }
    let old:Option<(i64,i64,String)>=sqlx::query_as("SELECT start,end,text FROM note_stage_base_piece WHERE root_key=? AND start<=? ORDER BY start DESC LIMIT 1")
        .bind(root).bind(offset).fetch_optional(&mut *conn).await.map_err(db)?;
    let current:Option<(i64,i64,String)>=sqlx::query_as("SELECT start,end,text FROM note_page_piece WHERE workspace_id=? AND note_id=? AND content_generation=? AND start<=? ORDER BY start DESC LIMIT 1")
        .bind(&binding.0).bind(&binding.1).bind(&binding.2).bind(offset).fetch_optional(&mut *conn).await.map_err(db)?;
    let piece = match (old, current) {
        (Some(a), Some(b)) => {
            if a.0 >= b.0 {
                a
            } else {
                b
            }
        }
        (Some(a), None) | (None, Some(a)) => a,
        (None, None) => return Err(conflict()),
    };
    if piece.0 > offset
        || piece.1 <= offset
        || piece.2.len() > 4096
        || piece.1 - piece.0 != i64::try_from(piece.2.encode_utf16().count()).map_err(db)?
    {
        return Err(conflict());
    }
    Ok(piece)
}
