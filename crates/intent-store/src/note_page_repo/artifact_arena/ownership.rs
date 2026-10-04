//! One bounded process-local source owner per generation. No receipt replay or
//! restart reconstructs these grants, and retaining one never extends authority.
use super::ArtifactSourceGrant;
use crate::note_page_repo::{failure, invalid, MAX_SNAPSHOTS};
use intent_core::{note_page::NotePageError, Result};
use std::{collections::BTreeMap, sync::Mutex};

#[derive(Default)]
pub(in crate::note_page_repo) struct SourceOwners {
    owners: Mutex<BTreeMap<String, SourceOwner>>,
}

struct SourceOwner {
    _source: ArtifactSourceGrant,
    expires_at: i64,
}

impl SourceOwners {
    /// Called only for a newly inserted generation, before COMMIT, while its
    /// temporary authorization grant already pins the snapshot. Keep the charge
    /// conservatively if commit outcome is uncertain; expiry/retirement drains it.
    pub(in crate::note_page_repo) fn register(
        &self,
        generation: &str,
        source: ArtifactSourceGrant,
        expires_at: i64,
    ) -> Result<()> {
        let now = i64::try_from(intent_core::now_epoch_ms()).map_err(|_| invalid())?;
        let mut owners = self.owners.lock().map_err(|_| invalid())?;
        owners.retain(|_, owner| owner.expires_at > now);
        if owners.contains_key(generation) || expires_at <= now {
            return Err(invalid());
        }
        // Separate finite generation ownership, including multiple jobs sharing
        // a source. This is operational backpressure, not a document size limit.
        if owners.len() >= MAX_SNAPSHOTS {
            return Err(failure(NotePageError::Budget));
        }
        owners.insert(
            generation.into(),
            SourceOwner {
                _source: source,
                expires_at,
            },
        );
        Ok(())
    }

    pub(in crate::note_page_repo) fn retire(&self, generations: &[String]) {
        let mut owners = self
            .owners
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for generation in generations {
            owners.remove(generation);
        }
        let now = intent_core::now_epoch_ms();
        owners.retain(|_, owner| u64::try_from(owner.expires_at).is_ok_and(|expiry| expiry > now));
    }

    pub(super) fn clear(&self) {
        self.owners
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
    }
}
