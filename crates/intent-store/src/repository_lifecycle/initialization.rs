//! Original-owner initialization receipts, never repository permission.

use std::any::Any;
use std::sync::Arc;

use intent_core::{AgentId, WorkspaceId};

use super::{lifecycle_error, LifecycleDomain, LifecycleWrite, RepositoryLifecycleObserver};
use crate::{Result, Store};

/// A pending owner's opaque proof bound to the actual managed database/observer.
/// Only that observer authenticates the erased proof; this envelope is no grant.
pub struct RepositoryInitializationClaim {
    domain: Arc<LifecycleDomain>,
    observer: Arc<dyn RepositoryLifecycleObserver>,
    original_owner: Box<dyn Any + Send>,
}

/// A committed original initialization, still subject to owner retirement.
/// The service must consume its private proof and allocate a NEW live origin;
/// neither this receipt nor a stored ID upgrades any old/pending callback.
pub struct RepositoryInitializationConfirmation {
    domain: Arc<LifecycleDomain>,
    observer: Arc<dyn RepositoryLifecycleObserver>,
    binding: RepositoryInitializationBinding,
    completion: Box<dyn Any + Send>,
}

/// Exact original intent, not caller-supplied authority.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RepositoryInitializationBinding {
    pub workspace_id: WorkspaceId,
    pub agent_id: AgentId,
    pub action: RepositoryAcpInitialization,
}

/// Evidence required at the serialized Store boundary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RepositoryAcpInitialization {
    /// Only a real NULL-to-value winner can confirm first creation.
    FirstSet { session_id: String },
    /// The observer must also prove the ORIGINAL owner successfully loaded it.
    Loaded { session_id: String },
    /// Exact original NULL/value expectation, with an actual changed ID.
    Replace {
        expected: Option<String>,
        session_id: String,
    },
}

/// One original observer owner. Drop MUST leave its barrier unsettled.
pub trait RepositoryInitializationTicket: Send {
    /// Consume after confirmed original commit/load. A concurrent retirement
    /// may reject completion even though the database effect already happened.
    /// Settle the original known-completed barrier even on that rejection;
    /// only an unknown/drop outcome must leave it blocked.
    ///
    /// # Errors
    /// Rejects an invalidated original pending owner or uncertain completion.
    fn finish_confirmed(self: Box<Self>) -> Result<Box<dyn Any + Send>>;

    /// Settle only a positively confirmed original no-effect outcome. Produces
    /// no completion proof and never revives the original pending attempt.
    fn settle_no_effect(self: Box<Self>);
}

/// Internal result of the SAME replacement transaction used by legacy writers.
pub(crate) struct AcpSessionWriteOutcome {
    pub(crate) canonical: String,
    pub(crate) rows_affected: u64,
}

impl LifecycleWrite {
    fn begin_initialization(
        &mut self,
        observer: &Arc<dyn RepositoryLifecycleObserver>,
        original_owner: Box<dyn Any + Send>,
        binding: &RepositoryInitializationBinding,
    ) -> Result<Box<dyn RepositoryInitializationTicket>> {
        if self.begun {
            return Err(lifecycle_error("mutation owner already began"));
        }
        let state = self
            .domain
            .state
            .lock()
            .map_err(|_| lifecycle_error("database domain poisoned"))?;
        if state.invalidated
            || !state
                .observer
                .as_ref()
                .is_some_and(|installed| Arc::ptr_eq(installed, observer))
        {
            return Err(lifecycle_error("original observer is unavailable"));
        }
        drop(state);
        self.begun = true;
        // The same retained domain/write guard now protects any uncertain SQL
        // worker completion. No domain or observer mutex crosses the callback.
        observer.begin_initialization(original_owner, binding)
    }
}

impl Store {
    /// Bind an erased original-owner proof to this exact retained domain/observer.
    /// Physical ownership is authenticated later by that observer, not Store.
    ///
    /// # Errors
    /// Rejects absent, replaced or retired observers/domains.
    pub fn bind_repository_initialization_claim(
        &self,
        observer: Arc<dyn RepositoryLifecycleObserver>,
        original_owner: Box<dyn Any + Send>,
    ) -> Result<RepositoryInitializationClaim> {
        if !self.has_repository_lifecycle_observer(&observer) {
            return Err(lifecycle_error("original observer is unavailable"));
        }
        Ok(RepositoryInitializationClaim {
            domain: self.repository_lifecycle.clone(),
            observer,
            original_owner,
        })
    }

    /// Confirm only the original physical owner's real winning write or load.
    /// Uses the existing writer serialization and commit/rollback machinery.
    /// No permission check or live-origin allocation is performed here.
    ///
    /// # Errors
    /// Rejects invalid bindings/ownership, missing rows, losing or unchanged
    /// writes, and uncertain SQL/owner completion. An error after COMMIT does
    /// not imply the database effect was rolled back.
    pub async fn initialize_repository_acp_session(
        &self,
        claim: RepositoryInitializationClaim,
        binding: RepositoryInitializationBinding,
    ) -> Result<RepositoryInitializationConfirmation> {
        if !Arc::ptr_eq(&claim.domain, &self.repository_lifecycle)
            || !self.has_repository_lifecycle_observer(&claim.observer)
        {
            return Err(lifecycle_error("initialization domain/observer changed"));
        }
        let session_id = match &binding.action {
            RepositoryAcpInitialization::FirstSet { session_id }
            | RepositoryAcpInitialization::Loaded { session_id }
            | RepositoryAcpInitialization::Replace { session_id, .. } => session_id,
        };
        if session_id.is_empty() {
            return Err(lifecycle_error("initialization session is empty"));
        }
        let mut lifecycle = self.repository_lifecycle_write().await?;
        let mut conn = self
            .write_pool()
            .acquire()
            .await
            .map_err(|e| lifecycle_error(&format!("initialization acquire failed: {e}")))?;
        sqlx::query("BEGIN IMMEDIATE")
            .execute(&mut *conn)
            .await
            .map_err(|e| lifecycle_error(&format!("initialization begin failed: {e}")))?;
        let mut ticket = None;
        let body = async {
            let stored: Option<Option<String>> = sqlx::query_scalar(
                "SELECT acp_session_id FROM agent_session WHERE id=? AND workspace_id=?",
            )
            .bind(&binding.agent_id.0)
            .bind(&binding.workspace_id.0)
            .fetch_optional(&mut *conn)
            .await
            .map_err(|e| lifecycle_error(&format!("initialization read failed: {e}")))?;
            let stored = stored.ok_or_else(|| lifecycle_error("initialization agent is absent"))?;
            let matches = match &binding.action {
                RepositoryAcpInitialization::FirstSet { .. } => stored.is_none(),
                RepositoryAcpInitialization::Loaded { .. } => stored.as_ref() == Some(session_id),
                RepositoryAcpInitialization::Replace { expected, .. } => {
                    stored == *expected && stored.as_ref() != Some(session_id)
                }
            };
            if !matches {
                return Err(lifecycle_error("original initialization condition no longer holds"));
            }
            let begin = || -> Result<()> {
                ticket = Some(lifecycle.begin_initialization(
                    &claim.observer,
                    claim.original_owner,
                    &binding,
                )?);
                Ok(())
            };
            let won = match &binding.action {
                RepositoryAcpInitialization::Replace { expected, .. } => {
                    let outcome = Self::write_acp_session_id_in_transaction(
                        &mut conn,
                        &binding.workspace_id,
                        &binding.agent_id,
                        expected.as_deref(),
                        session_id,
                        begin,
                    )
                    .await?;
                    outcome.rows_affected == 1
                }
                RepositoryAcpInitialization::FirstSet { .. } => {
                    begin()?;
                    let rows = sqlx::query(
                        "UPDATE agent_session SET acp_session_id=? WHERE id=? AND workspace_id=? AND acp_session_id IS NULL",
                    )
                    .bind(session_id)
                    .bind(&binding.agent_id.0)
                    .bind(&binding.workspace_id.0)
                    .execute(&mut *conn)
                    .await
                    .map_err(|e| lifecycle_error(&format!("initialization set failed: {e}")))?
                    .rows_affected();
                    rows == 1
                }
                RepositoryAcpInitialization::Loaded { .. } => {
                    begin()?;
                    true
                }
            };
            if !won {
                // A zero-row statement may still have run trigger effects.
                // The original rollback guard exposes no positive rollback
                // receipt, so leave the original observer owner unsettled.
                return Err(lifecycle_error("initialization did not win the write"));
            }
            let actual: Option<Option<String>> = sqlx::query_scalar(
                "SELECT acp_session_id FROM agent_session WHERE id=? AND workspace_id=?",
            )
            .bind(&binding.agent_id.0)
            .bind(&binding.workspace_id.0)
            .fetch_optional(&mut *conn)
            .await
            .map_err(|e| lifecycle_error(&format!("initialization winner read failed: {e}")))?;
            if actual.as_ref().and_then(Option::as_ref) != Some(session_id) {
                return Err(lifecycle_error("initialization winner changed inside transaction"));
            }
            Ok(())
        }
        .await;
        crate::commit_with_rollback_guard(conn, body, "initialization commit failed").await?;
        let ticket = ticket.ok_or_else(|| lifecycle_error("initialization was not observed"))?;
        let completion = ticket.finish_confirmed();
        lifecycle.settle();
        let completion = completion?;
        Ok(RepositoryInitializationConfirmation {
            domain: claim.domain,
            observer: claim.observer,
            binding,
            completion,
        })
    }

    /// Consume a receipt only in its original managed domain/observer.
    /// The observer's owner must STILL validate its private completion proof
    /// against intervening retirement before allocating any new live origin.
    ///
    /// # Errors
    /// Rejects a receipt from another or retired database/observer allocation.
    pub fn consume_repository_initialization_confirmation(
        &self,
        observer: &Arc<dyn RepositoryLifecycleObserver>,
        confirmation: RepositoryInitializationConfirmation,
    ) -> Result<(RepositoryInitializationBinding, Box<dyn Any + Send>)> {
        if !Arc::ptr_eq(&confirmation.domain, &self.repository_lifecycle)
            || !Arc::ptr_eq(&confirmation.observer, observer)
            || !self.has_repository_lifecycle_observer(observer)
        {
            return Err(lifecycle_error("confirmation domain/observer changed"));
        }
        Ok((confirmation.binding, confirmation.completion))
    }
}

#[cfg(all(test, unix))]
mod tests;
