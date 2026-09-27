//! Private, inactive bridge from existing writer ownership to repository lifetimes.
//!
//! Reserve before the first await. Under the EXISTING writer gates, finish all
//! validation (including startup residency, exact binding and token comparisons)
//! and call `begin` immediately before the first effective write/publication.
//! A no-op, placeholder or failed preflight must never call it with `Change`.
//!
//! The original writer alone performs persistence, sibling cleanup, verification
//! and compensation. Its completion handle reports facts only after ALL of that
//! work settles. Timeouts, task exit and dropped guards are not settlement.
//! No store, retry, rollback, credential read or async work lives in this module.
//! Registry reload/direct apply need a synchronous pre-publication hook; an
//! event delivered after a settings snapshot changes cannot supply that fence.

use std::sync::{Arc, Mutex};

use crate::repository_credentials::{
    RepositoryConnectionBinding, RepositoryConnectionDirectory, RepositoryCredentialError,
    RepositoryMutationKind, RepositoryMutationTicket, Result, SettledCredentialState,
};

#[path = "repository_credential_writers/mutation.rs"]
mod mutation;
#[path = "repository_credential_writers/policy.rs"]
mod policy;
pub(crate) use mutation::{
    RepositoryWriterCompletion, RepositoryWriterMutation, RepositoryWriterReservation,
};
pub(crate) use policy::{
    RepositoryChildPolicyCompletion, RepositoryChildPolicyMutation,
    RepositoryChildPolicyReservation,
};

/// Result of the original owner's final validated candidate/intent check, under
/// its existing locks. This is not caller authority or a deserializable input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RepositoryWriterPreflight {
    NoChange,
    Change,
}

/// One adapter per directory, owned by the existing credential/settings writer.
/// Child ownership is separate: an unsettled child write disables only children.
pub(crate) struct RepositoryCredentialWriters {
    directory: Arc<RepositoryConnectionDirectory>,
    policy: Arc<Mutex<policy::PolicySlot>>,
}

impl RepositoryCredentialWriters {
    pub(crate) fn new(directory: Arc<RepositoryConnectionDirectory>) -> Self {
        Self {
            directory,
            policy: Arc::new(Mutex::new(policy::PolicySlot::default())),
        }
    }

    /// Reserve original intent before network/secret preflight. Refresh is the
    /// only continuity operation; config/source/endpoint changes use Replace.
    pub(crate) fn reserve(
        &self,
        kind: RepositoryMutationKind,
    ) -> Result<RepositoryWriterReservation> {
        Ok(RepositoryWriterReservation {
            directory: self.directory.clone(),
            ticket: Some(self.directory.reserve_mutation(kind)?),
        })
    }

    pub(crate) fn reserve_child_policy(
        &self,
        binding: RepositoryConnectionBinding,
    ) -> Result<RepositoryChildPolicyReservation> {
        policy::reserve(self.directory.clone(), self.policy.clone(), binding)
    }
}
