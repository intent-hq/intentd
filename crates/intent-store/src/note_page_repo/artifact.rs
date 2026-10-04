//! Indexed canonical primitive source authorization. No profile is registered or
//! advertised here; the artifact service must also authorize its renderer profile.
use super::{db_error, failure, invalid};
use crate::Store;
use intent_core::{
    note_artifact::request::{ArtifactHeader, ArtifactSource, Primitive},
    note_page::{NotePageError, NoteScope},
    Result,
};
use sqlx::Row;

/// Verified source binding at one read transaction. Mutating lifecycle transitions
/// must revalidate this source under their own transaction before publication.
#[derive(Clone, Debug)]
pub struct ArtifactSourceGrant {
    pub scope: NoteScope,
    pub snapshot_id: String,
    pub source_revision: String,
    pub source_collection: String,
    pub native_collection: String,
    pub expires_at: String,
}

impl Store {
    /// Resolve both signed refs and the writer-maintained owner-to-code binding.
    /// Caller supplies the captured connection principal, never a header identity.
    ///
    /// # Errors
    /// Rejects invalid, stale, expired or mismatched source authority, unsupported
    /// live sources, and database failures.
    pub async fn authorize_note_artifact_source(
        &self,
        workspace_id: &str,
        principal: &str,
        header: &ArtifactHeader,
    ) -> Result<ArtifactSourceGrant> {
        header.validate(workspace_id).map_err(|_| invalid())?;
        let ArtifactSource::Snapshot {
            snapshot_id,
            source_revision,
            owner_ref,
            source_ref,
        } = &header.source
        else {
            // Session-live requires the real frozen operation-view authority.
            // Its future adapter cannot mint a grant from caller-supplied IDs.
            return Err(invalid());
        };
        let owner = self.note_pages.decode(owner_ref)?;
        let source = self.note_pages.decode(source_ref)?;
        if owner.0 != *snapshot_id
            || source.0 != *snapshot_id
            || owner.1 != "r"
            || source.1 != "r"
            || owner.3 != 0
            || source.3 != 0
            || !owner.2.starts_with("d:")
            || !source.2.starts_with("f:")
            || owner.2.matches(':').count() != 1
            || source.2.matches(':').count() != 1
            || owner.2.contains('@')
            || source.2.contains('@')
        {
            return Err(failure(NotePageError::CursorInvalid));
        }
        let snapshot = self.note_pages.snapshot(
            snapshot_id,
            workspace_id,
            &header.scope.note_id,
            principal,
        )?;
        if snapshot.scope != header.scope || snapshot.revision != *source_revision {
            return Err(failure(NotePageError::Stale));
        }
        let mut tx = self.read_pool().begin().await.map_err(db_error)?;
        let head = sqlx::query("SELECT instance_id,indexed_rev,current_rev,generation,profile_revision FROM note_page_head WHERE workspace_id=? AND note_id=?")
            .bind(workspace_id).bind(&header.scope.note_id)
            .fetch_optional(&mut *tx).await.map_err(db_error)?
            .ok_or_else(|| failure(NotePageError::Stale))?;
        let revision: i64 = head.try_get("current_rev").map_err(db_error)?;
        let generation: String = head.try_get("generation").map_err(db_error)?;
        if head.try_get::<String, _>("instance_id").map_err(db_error)?
            != snapshot.scope.note_instance_id
            || generation != snapshot.generation
            || format!("r:{revision}:{generation}") != snapshot.revision
        {
            return Err(failure(NotePageError::Stale));
        }
        if head.try_get::<i64, _>("indexed_rev").map_err(db_error)? != revision
            || head
                .try_get::<String, _>("profile_revision")
                .map_err(db_error)?
                != crate::note_page_index::profile_revision()
        {
            return Err(failure(NotePageError::Expired));
        }
        let primitive = match header.primitive {
            Primitive::Diff => "diff",
            Primitive::Mermaid => "mermaid",
        };
        let found = sqlx::query("SELECT 1 FROM note_artifact_source WHERE workspace_id=? AND note_id=? AND native_collection=? AND source_collection=? AND primitive=?")
            .bind(workspace_id).bind(&header.scope.note_id).bind(&owner.2).bind(&source.2).bind(primitive)
            .fetch_optional(&mut *tx).await.map_err(db_error)?;
        if found.is_none() {
            return Err(failure(NotePageError::CursorInvalid));
        }
        // Expiry/eviction can occur while waiting for SQL. Never return a newly
        // granted source after its original runtime lease has been retired.
        self.note_pages
            .snapshot(snapshot_id, workspace_id, &header.scope.note_id, principal)?;
        tx.commit().await.map_err(db_error)?;
        Ok(ArtifactSourceGrant {
            scope: snapshot.scope,
            snapshot_id: snapshot_id.clone(),
            source_revision: snapshot.revision,
            source_collection: source.2,
            native_collection: owner.2,
            expires_at: snapshot.expires,
        })
    }
}
