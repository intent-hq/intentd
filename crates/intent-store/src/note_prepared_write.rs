//! Optional preparation for versioned parent writes; publication stays atomic.
use crate::{note_page_index::Projection, Store};
use intent_core::{Error, Note, Result};
use sqlx::{Row, SqliteConnection};
use std::{io::Write, sync::Arc};
use tokio::sync::OwnedSemaphorePermit;

const MAX_SOURCE_BYTES: usize = 2 * 1024 * 1024;
const MAX_INPUT_JSON_BYTES: usize = 4 * 1024 * 1024;

#[derive(PartialEq, Eq)]
struct Identity {
    instance: String,
    row_revision: i64,
    current_revision: i64,
    generation: String,
    indexed_revision: i64,
    profile: String,
}

const IDENTITY_SQL: &str = "SELECT h.instance_id,n.rev AS row_revision,h.current_rev,h.generation,h.indexed_rev,h.profile_revision FROM note n JOIN note_page_head h ON h.workspace_id=n.workspace_id AND h.note_id=n.id WHERE n.workspace_id=? AND n.id=?";

fn identity(row: &sqlx::sqlite::SqliteRow) -> Result<Identity> {
    let decode = || -> std::result::Result<Identity, sqlx::Error> {
        Ok(Identity {
            instance: row.try_get("instance_id")?,
            row_revision: row.try_get("row_revision")?,
            current_revision: row.try_get("current_rev")?,
            generation: row.try_get("generation")?,
            indexed_revision: row.try_get("indexed_rev")?,
            profile: row.try_get("profile_revision")?,
        })
    };
    decode().map_err(|e| Error::Internal(format!("read preparation identity: {e}")))
}

struct CountBytes(usize);
impl Write for CountBytes {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.0 {
            return Err(std::io::Error::other("preparation input budget"));
        }
        self.0 -= bytes.len();
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Owns its immutable input and permit even when output limits require a legacy
/// rebuild. Dropping only the generated projection must never discard identity.
pub(crate) struct Attempt {
    note: Arc<Note>,
    captured: Identity,
    expected_revision: i64,
    parser_profile: &'static str,
    projection: Option<Projection>,
    _permit: OwnedSemaphorePermit,
}

impl Attempt {
    pub(crate) async fn try_prepare(
        store: &Store,
        note: &Note,
        expected_revision: i64,
    ) -> Result<Option<Self>> {
        Self::try_prepare_using(store, note, expected_revision, Projection::prepare).await
    }

    pub(crate) async fn try_prepare_using<F>(
        store: &Store,
        note: &Note,
        expected_revision: i64,
        prepare: F,
    ) -> Result<Option<Self>>
    where
        F: FnOnce(&Note) -> Result<Option<Projection>> + Send + 'static,
    {
        if note.content.len() > MAX_SOURCE_BYTES {
            return Ok(None);
        }
        // One statement captures row and head coherently, before any CPU work.
        // Actual stale index/profile values are valid inputs to a full save.
        let row = sqlx::query(IDENTITY_SQL)
            .bind(&note.workspace_id.0)
            .bind(&note.id.0)
            .fetch_optional(store.read_pool())
            .await
            .map_err(|e| Error::Internal(format!("capture preparation identity: {e}")))?;
        let Some(row) = row else { return Ok(None) };
        let captured = identity(&row)?;
        let Ok(permit) = store.note_preparation.clone().try_acquire_owned() else {
            return Ok(None);
        };
        if serde_json::to_writer(CountBytes(MAX_INPUT_JSON_BYTES), note).is_err() {
            return Ok(None);
        }
        // The caller still owns its original Note. This permit bounds only the
        // additional clone/preparation, not all request bodies or a legacy writer.
        let note = Arc::new(note.clone());
        let parser_profile = crate::note_page_index::profile_revision();
        tokio::task::spawn_blocking(move || {
            // The permit travels inside the job and its result: cancelling the
            // await cannot release admission while a detached CPU job is alive.
            let projection = prepare(&note)?;
            Ok(Some(Self {
                note,
                captured,
                expected_revision,
                parser_profile,
                projection,
                _permit: permit,
            }))
        })
        .await
        .map_err(|e| Error::Internal(format!("prepare note index: {e}")))?
    }

    pub(crate) fn expected_revision(&self) -> i64 {
        self.expected_revision
    }

    pub(crate) fn note(&self) -> Arc<Note> {
        self.note.clone()
    }

    pub(crate) fn take_projection(&mut self) -> Option<Projection> {
        self.projection.take()
    }

    pub(crate) async fn matches(&self, conn: &mut SqliteConnection) -> Result<bool> {
        let row = sqlx::query(IDENTITY_SQL)
            .bind(&self.note.workspace_id.0)
            .bind(&self.note.id.0)
            .fetch_optional(conn)
            .await
            .map_err(|e| Error::Internal(format!("validate preparation identity: {e}")))?;
        Ok(
            self.parser_profile == crate::note_page_index::profile_revision()
                && row.as_ref().map(identity).transpose()?.as_ref() == Some(&self.captured),
        )
    }
}
