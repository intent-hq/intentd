use super::{db_error, head, invalid, stale, AnnotationEpochs};
use crate::Store;
use intent_core::{Error, NoteId, Result, WorkspaceId};
use sqlx::Row;

/// Fields retain their exact persisted values; null and empty are distinct.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CommentDetailField {
    Body,
    Author,
    Anchor,
    AnchorText,
    Extra,
    AuthorPrincipalId,
    Provider,
    Host,
    ExternalUserId,
    StartId,
    EndId,
    PointId,
}

impl CommentDetailField {
    pub(super) fn name(self) -> &'static str {
        match self {
            Self::Body => "body",
            Self::Author => "author",
            Self::Anchor => "anchor",
            Self::AnchorText => "anchorText",
            Self::Extra => "extra",
            Self::AuthorPrincipalId => "authorPrincipalId",
            Self::Provider => "provider",
            Self::Host => "host",
            Self::ExternalUserId => "externalUserId",
            Self::StartId => "startId",
            Self::EndId => "endId",
            Self::PointId => "pointId",
        }
    }
}

/// Internal reference target. Public references authenticate the scope, relevant
/// epoch, field and continuation before calling this storage primitive.
#[derive(Clone, Debug)]
pub enum AnnotationDetail<'a> {
    Comment {
        comment_id: &'a str,
        field: CommentDetailField,
    },
    AttributionAuthor {
        line: i64,
    },
}

#[derive(Clone, Debug)]
pub struct AnnotationFragment {
    pub text: String,
    pub byte_end: i64,
    pub utf16_length: usize,
    pub total_bytes: i64,
    pub is_null: bool,
}

impl Store {
    /// Fetch only fixed-size pieces covering the requested field continuation.
    /// The authenticated context token carries the cumulative UTF-16 position;
    /// neither deep reads nor Unicode offset reporting scan earlier pieces.
    ///
    /// # Errors
    /// Returns invalid params for bad budgets/offsets or split scalars, stale
    /// for changed epochs, not found for inaccessible details, or a database error.
    pub async fn read_annotation_fragment(
        &self,
        workspace_id: &WorkspaceId,
        note_id: &NoteId,
        expected: &AnnotationEpochs,
        detail: AnnotationDetail<'_>,
        byte_at: i64,
        max_bytes: usize,
    ) -> Result<AnnotationFragment> {
        if byte_at < 0 || !(4..=16384).contains(&max_bytes) {
            return Err(invalid());
        }
        let mut tx = self.read_pool().begin().await.map_err(db_error)?;
        let (head_id, epochs) = head(&mut tx, workspace_id, note_id).await?;
        let attribution = matches!(detail, AnnotationDetail::AttributionAuthor { .. });
        if epochs.source_revision != expected.source_revision
            || if attribution {
                epochs.attribution_generation != expected.attribution_generation
                    || !epochs.attribution_ready
            } else {
                epochs.comment_revision != expected.comment_revision
            }
        {
            return Err(stale());
        }
        let first = byte_at / 1024 * 1024;
        let last = byte_at
            .checked_add(i64::try_from(max_bytes).map_err(|_| invalid())?)
            .ok_or_else(invalid)?;
        let (metadata, rows) = match detail {
            AnnotationDetail::Comment { comment_id, field } => {
                let metadata=sqlx::query("SELECT d.byte_length,d.is_null FROM note_comment_detail d JOIN note_comment_projection p ON p.comment_id=d.comment_id WHERE p.head_id=? AND d.comment_id=? AND d.field=?")
                    .bind(head_id).bind(comment_id).bind(field.name()).fetch_optional(&mut *tx).await.map_err(db_error)?
                    .ok_or_else(||Error::NotFound("annotation detail".into()))?;
                let rows=sqlx::query("SELECT position,data FROM note_comment_detail_piece WHERE comment_id=? AND field=? AND position>=? AND position<? ORDER BY position LIMIT 17")
                    .bind(comment_id).bind(field.name()).bind(first).bind(last).fetch_all(&mut *tx).await.map_err(db_error)?;
                (metadata, rows)
            }
            AnnotationDetail::AttributionAuthor { line } => {
                let metadata=sqlx::query("SELECT byte_length,0 AS is_null FROM note_attribution_author WHERE head_id=? AND line=?")
                    .bind(head_id).bind(line).fetch_optional(&mut *tx).await.map_err(db_error)?
                    .ok_or_else(||Error::NotFound("annotation author".into()))?;
                let rows=sqlx::query("SELECT position,data FROM note_attribution_author_piece WHERE head_id=? AND line=? AND position>=? AND position<? ORDER BY position LIMIT 17")
                    .bind(head_id).bind(line).bind(first).bind(last).fetch_all(&mut *tx).await.map_err(db_error)?;
                (metadata, rows)
            }
        };
        let total_bytes: i64 = metadata.get("byte_length");
        if byte_at > total_bytes {
            return Err(invalid());
        }
        let mut bytes = Vec::new();
        for (index, row) in rows.iter().enumerate() {
            if row.get::<i64, _>("position")
                != first + i64::try_from(index).map_err(|_| invalid())? * 1024
            {
                return Err(Error::Internal("annotation fragment index gap".into()));
            }
            bytes.extend_from_slice(&row.get::<Vec<u8>, _>("data"));
        }
        let skip = usize::try_from(byte_at - first).map_err(|_| invalid())?;
        let remaining = usize::try_from(total_bytes - byte_at).map_err(|_| invalid())?;
        let wanted = max_bytes.min(remaining);
        let slice = bytes
            .get(skip..skip + wanted)
            .ok_or_else(|| Error::Internal("incomplete annotation fragment index".into()))?;
        let text = match std::str::from_utf8(slice) {
            Ok(text) => text,
            Err(error) if error.error_len().is_none() => {
                std::str::from_utf8(&slice[..error.valid_up_to()]).map_err(|_| invalid())?
            }
            Err(_) => return Err(invalid()),
        };
        if text.is_empty() && byte_at < total_bytes {
            return Err(invalid());
        }
        let result = AnnotationFragment {
            text: text.to_owned(),
            byte_end: byte_at + i64::try_from(text.len()).map_err(|_| invalid())?,
            utf16_length: text.encode_utf16().count(),
            total_bytes,
            is_null: metadata.get("is_null"),
        };
        tx.commit().await.map_err(db_error)?;
        Ok(result)
    }
}
