//! Indexed canonical primitive source authorization. No profile is registered or
//! advertised here; the artifact service must also authorize its renderer profile.
use super::{db_error, failure, invalid};
use crate::Store;
use intent_core::{
    note_artifact::request::{ArtifactHeader, ArtifactSource, Primitive},
    note_page::{NotePageError, NoteScope},
    Result,
};
use sqlx::{Acquire, Row, SqliteConnection};

/// Internal snapshot source identity. It conveys no renderer, font or storage authority.
#[derive(Clone, Debug)]
pub struct CanonicalSourceBinding {
    pub scope: NoteScope,
    pub snapshot_id: String,
    pub source_revision: String,
    pub primitive: Primitive,
    pub owner_ref: String,
    pub source_ref: String,
}

impl CanonicalSourceBinding {
    fn validate(&self, workspace: &str) -> Result<()> {
        for value in [
            &self.scope.backend_id,
            &self.scope.workspace_id,
            &self.scope.note_id,
            &self.scope.note_instance_id,
            &self.snapshot_id,
            &self.source_revision,
            &self.owner_ref,
            &self.source_ref,
        ] {
            if value.is_empty() || value.contains('\0') || value.len() > 256 {
                return Err(invalid());
            }
        }
        if self.scope.workspace_id != workspace {
            return Err(invalid());
        }
        Ok(())
    }
}

/// Runtime snapshot retention only, not current head/code or renderer authority.
/// Held across source authorization and any uncertain connection cleanup.
#[derive(Clone, Debug)]
pub struct CanonicalSourceHold {
    expires_at: String,
    _pin: std::sync::Arc<super::SnapshotPin>,
}

impl CanonicalSourceHold {
    /// Original signed snapshot deadline. Retention does not renew it or grant current source authority.
    #[must_use]
    pub fn expires_at(&self) -> &str {
        &self.expires_at
    }
}

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
    _pin: std::sync::Arc<super::SnapshotPin>,
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
        let mut tx = self.read_pool().begin().await.map_err(db_error)?;
        let grant = self
            .authorize_artifact_source_in(&mut tx, workspace_id, principal, header)
            .await?;
        tx.commit().await.map_err(db_error)?;
        Ok(grant)
    }

    pub(super) async fn authorize_artifact_source_in(
        &self,
        connection: &mut SqliteConnection,
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
        let binding = CanonicalSourceBinding {
            scope: header.scope.clone(),
            snapshot_id: snapshot_id.clone(),
            source_revision: source_revision.clone(),
            primitive: header.primitive,
            owner_ref: owner_ref.clone(),
            source_ref: source_ref.clone(),
        };
        self.authorize_canonical_source_in(connection, workspace_id, principal, &binding)
            .await
    }

    /// Retain a principal-bound signed snapshot before source authorization IO.
    /// This does not authorize its current SQL head or owner-to-code relation.
    ///
    /// # Errors
    /// Rejects malformed, foreign or expired snapshot references.
    pub fn hold_canonical_source(
        &self,
        workspace_id: &str,
        principal: &str,
        binding: &CanonicalSourceBinding,
    ) -> Result<CanonicalSourceHold> {
        binding.validate(workspace_id)?;
        for (reference, prefix) in [(&binding.owner_ref, "d:"), (&binding.source_ref, "f:")] {
            let token = self.note_pages.decode(reference)?;
            if token.0 != binding.snapshot_id
                || token.1 != "r"
                || token.3 != 0
                || !token.2.starts_with(prefix)
                || token.2.matches(':').count() != 1
                || token.2.contains('@')
            {
                return Err(invalid());
            }
        }
        let snapshot = self.note_pages.snapshot(
            &binding.snapshot_id,
            workspace_id,
            &binding.scope.note_id,
            principal,
        )?;
        if snapshot.scope != binding.scope || snapshot.revision != binding.source_revision {
            return Err(failure(NotePageError::Stale));
        }
        Ok(CanonicalSourceHold {
            expires_at: snapshot.expires,
            _pin: self.note_pages.pin_snapshot(&binding.snapshot_id)?,
        })
    }

    /// Authorize only a canonical snapshot owner and its exact attribute value.
    /// No artifact arena, renderer profile, font or reservation is involved.
    ///
    /// # Errors
    /// Rejects malformed, mismatched, stale or expired source bindings.
    pub async fn authorize_canonical_source(
        &self,
        workspace_id: &str,
        principal: &str,
        binding: &CanonicalSourceBinding,
    ) -> Result<ArtifactSourceGrant> {
        let _hold = self.hold_canonical_source(workspace_id, principal, binding)?;
        let mut connection = self.read_pool().acquire().await.map_err(db_error)?;
        let outcome = async {
            let mut tx = connection.begin().await.map_err(db_error)?;
            let outcome = self
                .authorize_canonical_source_in(&mut tx, workspace_id, principal, binding)
                .await;
            match outcome {
                Ok(grant) => {
                    tx.commit().await.map_err(db_error)?;
                    Ok(grant)
                }
                Err(error) => {
                    tx.rollback().await.map_err(db_error)?;
                    Err(error)
                }
            }
        }
        .await;
        // The caller retains its separate hold if this acknowledgement fails.
        connection.close().await.map_err(db_error)?;
        outcome
    }

    async fn authorize_canonical_source_in(
        &self,
        connection: &mut SqliteConnection,
        workspace_id: &str,
        principal: &str,
        binding: &CanonicalSourceBinding,
    ) -> Result<ArtifactSourceGrant> {
        binding.validate(workspace_id)?;
        let CanonicalSourceBinding {
            snapshot_id,
            source_revision,
            owner_ref,
            source_ref,
            ..
        } = binding;
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
        let pin = self.note_pages.pin_snapshot(snapshot_id)?;
        let snapshot = self.note_pages.snapshot(
            snapshot_id,
            workspace_id,
            &binding.scope.note_id,
            principal,
        )?;
        if snapshot.scope != binding.scope || snapshot.revision != *source_revision {
            return Err(failure(NotePageError::Stale));
        }
        let head = sqlx::query("SELECT instance_id,indexed_rev,current_rev,generation,profile_revision FROM note_page_head WHERE workspace_id=? AND note_id=?")
            .bind(workspace_id).bind(&binding.scope.note_id)
            .fetch_optional(&mut *connection).await.map_err(db_error)?
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
        let primitive = match binding.primitive {
            Primitive::Diff => "diff",
            Primitive::Mermaid => "mermaid",
        };
        let found = sqlx::query("SELECT 1 FROM note_artifact_source WHERE workspace_id=? AND note_id=? AND native_collection=? AND source_collection=? AND primitive=?")
            .bind(workspace_id).bind(&binding.scope.note_id).bind(&owner.2).bind(&source.2).bind(primitive)
            .fetch_optional(&mut *connection).await.map_err(db_error)?;
        if found.is_none() {
            return Err(failure(NotePageError::CursorInvalid));
        }
        // Expiry/eviction can occur while waiting for SQL. Never return a newly
        // granted source after its original runtime lease has been retired.
        self.note_pages
            .snapshot(snapshot_id, workspace_id, &binding.scope.note_id, principal)?;
        Ok(ArtifactSourceGrant {
            scope: snapshot.scope,
            snapshot_id: snapshot_id.clone(),
            source_revision: snapshot.revision,
            source_collection: source.2,
            native_collection: owner.2,
            expires_at: snapshot.expires,
            _pin: pin,
        })
    }
}
