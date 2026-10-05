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

/// Original persistence facts, separate from the one-use owner confirmation.
/// A failed confirmation never erases a known committed database effect.
/// Neither these facts nor a canonical competing ID grant physical ownership.
pub struct RepositoryInitializationOutcome {
    pub persistence: RepositoryInitializationPersistence,
    pub confirmation: Result<RepositoryInitializationConfirmation>,
}

/// What this attempt established about ACP-ID/accounting persistence only.
/// No variant settles an observer ticket or authorizes a retry on its own.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RepositoryInitializationPersistence {
    /// Rejected before the original Store transaction began.
    NotAttempted,
    /// No ACP-ID/accounting DML was dispatched by this transaction. This is
    /// also the successful Loaded path; its confirmation is independent.
    NoEffect {
        observed: RepositoryInitializationObservation,
    },
    /// The original winner checks passed and its COMMIT was acknowledged.
    /// Later rejection of ownership cannot undo this historical fact.
    Committed { session_id: String },
    /// DML may have run without acknowledged original completion. A generic
    /// rollback error, later row read or timeout cannot supply a canonical ID.
    Unknown,
}

/// Scoped row facts actually read in this attempt's serialized transaction.
/// These are historical observations, never current authority or caller data.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RepositoryInitializationObservation {
    NotRead,
    Missing,
    Present { session_id: Option<String> },
}

/// Ordinary ACP persistence and optional original ownership are independent.
/// In particular, a successful legacy result may have no usable confirmation.
pub struct RepositoryAcpCompatibilityOutcome {
    pub persistence: RepositoryAcpCompatibilityPersistence,
    pub result: Result<RepositoryAcpCompatibilityResult>,
    pub confirmation: Result<RepositoryInitializationConfirmation>,
}

/// Unlike a strict committed-ID winner, a committed legacy statement can have
/// zero affected rows or trigger effects. Keep its actual row observation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RepositoryAcpCompatibilityPersistence {
    NotAttempted,
    NoEffect {
        observed: RepositoryInitializationObservation,
    },
    Committed {
        observed: RepositoryInitializationObservation,
        affected_rows: u64,
    },
    Unknown,
}

/// Provenance of the legacy returned string, never physical-owner authority.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RepositoryAcpCompatibilityResult {
    Observed {
        session_id: String,
    },
    /// The original response/helper supplied this value without a matching
    /// stored-row observation (including the legacy missing-row fallback).
    SubmittedFallback {
        session_id: String,
    },
    Committed {
        session_id: String,
        effect: RepositoryAcpCompatibilityEffect,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RepositoryAcpCompatibilityEffect {
    FirstSet,
    Replace {
        previous: Option<String>,
    },
    /// The ID stayed equal; the original replacement accounting fold ran.
    AccountingOnly,
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

    /// Consume only this original known-committed barrier and retire its
    /// pending attempt, without producing ANY completion proof or owner.
    /// Other or uncertain barriers must remain untouched. This must never
    /// emulate settlement by creating a confirmation and discarding it.
    ///
    /// # Errors
    /// Defaults to unavailable and leaves the dropped ticket unsettled.
    fn settle_committed_without_confirmation(self: Box<Self>) -> Result<()> {
        Err(lifecycle_error(
            "unconfirmed commit settlement is unavailable",
        ))
    }
}

/// Internal result of the SAME replacement transaction used by legacy writers.
pub(crate) struct AcpSessionWriteOutcome {
    pub(crate) canonical: String,
    pub(crate) rows_affected: u64,
    pub(crate) statement_dispatched: bool,
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

    fn begin_compatibility(
        &mut self,
        claim: &mut Option<RepositoryInitializationClaim>,
        binding: &RepositoryInitializationBinding,
        ticket: &mut Option<Box<dyn RepositoryInitializationTicket>>,
        confirmation: &mut Result<RepositoryInitializationConfirmation>,
        ordinary_invalidation: bool,
    ) -> Result<()> {
        if let Some(claim) = claim.take() {
            match self.begin_initialization(&claim.observer, claim.original_owner, binding) {
                Ok(original) => {
                    *ticket = Some(original);
                    return Ok(());
                }
                Err(error) => {
                    *confirmation = Err(error);
                    let state = self
                        .domain
                        .state
                        .lock()
                        .map_err(|_| lifecycle_error("database domain poisoned"))?;
                    if state.invalidated
                        || !state
                            .observer
                            .as_ref()
                            .is_some_and(|installed| Arc::ptr_eq(installed, &claim.observer))
                    {
                        return Err(lifecycle_error("initialization domain/observer changed"));
                    }
                }
            }
        }
        if ordinary_invalidation {
            let keys = [super::RepositoryLifecycleKey::Agent(
                binding.agent_id.clone(),
            )];
            if !self.begun {
                return self.begin(&keys);
            }
            // An unsuccessful original begin may have left a barrier. Do NOT
            // reset or settle it. Acquire a distinct ordinary ticket, whose
            // confirmed settlement removes ONLY its own barrier.
            let observer = self
                .domain
                .state
                .lock()
                .map_err(|_| lifecycle_error("database domain poisoned"))?
                .observer
                .clone();
            if let Some(observer) = observer {
                self.ticket = Some(observer.begin_mutation(&keys)?);
            }
        }
        Ok(())
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
        self.initialize_repository_acp_session_outcome(claim, binding)
            .await
            .confirmation
    }

    /// Preserve original persistence facts even when owner confirmation fails.
    /// Executes the same strict transaction once; no fallback read or write is
    /// used to recover a canonical ID. A canceled future yields no receipt and
    /// retains the existing unknown-worker protection.
    pub async fn initialize_repository_acp_session_outcome(
        &self,
        claim: RepositoryInitializationClaim,
        binding: RepositoryInitializationBinding,
    ) -> RepositoryInitializationOutcome {
        let mut persistence = RepositoryInitializationPersistence::NotAttempted;
        let confirmation = self
            .initialize_repository_acp_session_inner(claim, binding, &mut persistence)
            .await;
        RepositoryInitializationOutcome {
            persistence,
            confirmation,
        }
    }

    async fn initialize_repository_acp_session_inner(
        &self,
        claim: RepositoryInitializationClaim,
        binding: RepositoryInitializationBinding,
        persistence: &mut RepositoryInitializationPersistence,
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
        *persistence = RepositoryInitializationPersistence::NoEffect {
            observed: RepositoryInitializationObservation::NotRead,
        };
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
            *persistence = RepositoryInitializationPersistence::NoEffect {
                observed: stored.as_ref().map_or(
                    RepositoryInitializationObservation::Missing,
                    |session_id| RepositoryInitializationObservation::Present {
                        session_id: session_id.clone(),
                    },
                ),
            };
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
                if !matches!(binding.action, RepositoryAcpInitialization::Loaded { .. }) {
                    // Before handing DML to SQLx: cancellation or a generic
                    // rollback result must never masquerade as a no-op.
                    *persistence = RepositoryInitializationPersistence::Unknown;
                }
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
        if !matches!(binding.action, RepositoryAcpInitialization::Loaded { .. }) {
            *persistence = RepositoryInitializationPersistence::Committed {
                session_id: session_id.clone(),
            };
        }
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

    /// Preserve ordinary ACP result/accounting behavior in one transaction.
    /// Optional proof never authorizes the ordinary write: existing service
    /// gates still apply. Callers must retain their ORIGINAL Store and producer
    /// result. Strict initialization APIs above are unchanged.
    pub async fn initialize_repository_acp_session_compatible(
        &self,
        claim: Option<RepositoryInitializationClaim>,
        binding: RepositoryInitializationBinding,
    ) -> RepositoryAcpCompatibilityOutcome {
        let mut persistence = RepositoryAcpCompatibilityPersistence::NotAttempted;
        let mut confirmation = Err(lifecycle_error(
            "original initialization proof is unavailable",
        ));
        let result = self
            .initialize_repository_acp_session_compatible_inner(
                claim,
                binding,
                &mut persistence,
                &mut confirmation,
            )
            .await;
        RepositoryAcpCompatibilityOutcome {
            persistence,
            result,
            confirmation,
        }
    }

    async fn initialize_repository_acp_session_compatible_inner(
        &self,
        mut claim: Option<RepositoryInitializationClaim>,
        binding: RepositoryInitializationBinding,
        persistence: &mut RepositoryAcpCompatibilityPersistence,
        confirmation: &mut Result<RepositoryInitializationConfirmation>,
    ) -> Result<RepositoryAcpCompatibilityResult> {
        if claim.as_ref().is_some_and(|claim| {
            !Arc::ptr_eq(&claim.domain, &self.repository_lifecycle)
                || !self.has_repository_lifecycle_observer(&claim.observer)
        }) {
            return Err(lifecycle_error("initialization domain/observer changed"));
        }
        let original = claim
            .as_ref()
            .map(|claim| (claim.domain.clone(), claim.observer.clone()));
        let session_id = match &binding.action {
            RepositoryAcpInitialization::FirstSet { session_id }
            | RepositoryAcpInitialization::Loaded { session_id }
            | RepositoryAcpInitialization::Replace { session_id, .. } => session_id,
        };
        let mut lifecycle = self.repository_lifecycle_write().await?;
        let mut conn = self.write_pool().acquire().await.map_err(|error| {
            lifecycle_error(&format!(
                "compatibility initialization acquire failed: {error}"
            ))
        })?;
        sqlx::query("BEGIN IMMEDIATE")
            .execute(&mut *conn)
            .await
            .map_err(|error| {
                lifecycle_error(&format!(
                    "compatibility initialization begin failed: {error}"
                ))
            })?;
        *persistence = RepositoryAcpCompatibilityPersistence::NoEffect {
            observed: RepositoryInitializationObservation::NotRead,
        };
        let mut ticket = None;
        let body = async {
            let before = compatibility_read(&mut conn, &binding).await?;
            *persistence = RepositoryAcpCompatibilityPersistence::NoEffect {
                observed: observation(before.as_ref()),
            };
            let loaded = matches!(binding.action, RepositoryAcpInitialization::Loaded { .. });
            let matches_loaded = before.as_ref().and_then(Option::as_ref) == Some(session_id);
            if loaded {
                if matches_loaded && !session_id.is_empty() {
                    lifecycle.begin_compatibility(
                        &mut claim,
                        &binding,
                        &mut ticket,
                        confirmation,
                        false,
                    )?;
                }
                return Ok(CompatibilityBody {
                    canonical: session_id.clone(),
                    observed: observation(before.as_ref()),
                    effect: None,
                    rows: 0,
                    dispatched: false,
                    strict_winner: matches_loaded && !session_id.is_empty(),
                });
            }
            let stored = before.clone().ok_or_else(|| {
                intent_core::Error::NotFound(format!("agent session {}", binding.agent_id))
            })?;
            let effect = match &binding.action {
                RepositoryAcpInitialization::FirstSet { .. } => {
                    if let Some(existing) = stored.as_ref() {
                        if existing != session_id {
                            return Err(intent_core::Error::Internal(
                                "acpSessionId is write-once".into(),
                            ));
                        }
                        return Ok(CompatibilityBody::observed(
                            existing.clone(),
                            observation(before.as_ref()),
                        ));
                    }
                    RepositoryAcpCompatibilityEffect::FirstSet
                }
                RepositoryAcpInitialization::Replace { expected, .. } => {
                    if let Some(existing) = stored.as_ref() {
                        if Some(existing) != expected.as_ref() {
                            return Ok(CompatibilityBody::observed(
                                existing.clone(),
                                observation(before.as_ref()),
                            ));
                        }
                    }
                    if stored.as_ref() == Some(session_id) {
                        RepositoryAcpCompatibilityEffect::AccountingOnly
                    } else {
                        RepositoryAcpCompatibilityEffect::Replace {
                            previous: stored.clone(),
                        }
                    }
                }
                RepositoryAcpInitialization::Loaded { .. } => unreachable!(),
            };
            let strict_eligible = !session_id.is_empty()
                && match &binding.action {
                    RepositoryAcpInitialization::FirstSet { .. } => stored.is_none(),
                    RepositoryAcpInitialization::Replace { expected, .. } => {
                        stored == *expected && stored.as_ref() != Some(session_id)
                    }
                    RepositoryAcpInitialization::Loaded { .. } => false,
                };
            lifecycle.begin_compatibility(
                &mut claim,
                &binding,
                &mut ticket,
                confirmation,
                stored.as_ref() != Some(session_id),
            )?;
            // Even accounting-only DML without a binding invalidation must
            // retain the managed domain if its SQL worker outlives this owner.
            lifecycle.begun = true;
            *persistence = RepositoryAcpCompatibilityPersistence::Unknown;
            let (canonical, rows, dispatched) =
                if matches!(binding.action, RepositoryAcpInitialization::FirstSet { .. }) {
                    let rows = sqlx::query(
                        "UPDATE agent_session SET acp_session_id=? WHERE id=? AND workspace_id=?",
                    )
                    .bind(session_id)
                    .bind(&binding.agent_id.0)
                    .bind(&binding.workspace_id.0)
                    .execute(&mut *conn)
                    .await
                    .map_err(|error| {
                        lifecycle_error(&format!("set acp session id failed: {error}"))
                    })?
                    .rows_affected();
                    (session_id.clone(), rows, true)
                } else {
                    let result = Self::write_acp_session_id_in_transaction(
                        &mut conn,
                        &binding.workspace_id,
                        &binding.agent_id,
                        stored.as_deref(),
                        session_id,
                        || Ok(()),
                    )
                    .await?;
                    (
                        result.canonical,
                        result.rows_affected,
                        result.statement_dispatched,
                    )
                };
            let actual = compatibility_read(&mut conn, &binding).await?;
            if !dispatched {
                *persistence = RepositoryAcpCompatibilityPersistence::NoEffect {
                    observed: observation(actual.as_ref()),
                };
            }
            Ok(CompatibilityBody {
                canonical,
                strict_winner: strict_eligible
                    && rows == 1
                    && actual.as_ref().and_then(Option::as_ref) == Some(session_id),
                observed: observation(actual.as_ref()),
                effect: Some(effect),
                rows,
                dispatched,
            })
        }
        .await;
        let committed = crate::commit_with_rollback_guard(
            conn,
            body,
            "compatibility initialization commit failed",
        )
        .await?;
        if committed.dispatched {
            *persistence = RepositoryAcpCompatibilityPersistence::Committed {
                observed: committed.observed.clone(),
                affected_rows: committed.rows,
            };
        }
        if let Some(ticket) = ticket {
            if committed.strict_winner {
                let completion = ticket.finish_confirmed();
                lifecycle.settle();
                *confirmation = completion.and_then(|completion| {
                    let (domain, observer) = original.ok_or_else(|| {
                        lifecycle_error("original initialization envelope is absent")
                    })?;
                    Ok(RepositoryInitializationConfirmation {
                        domain,
                        observer,
                        binding,
                        completion,
                    })
                });
            } else if committed.dispatched {
                let settled = ticket.settle_committed_without_confirmation();
                if settled.is_ok() {
                    lifecycle.settle();
                }
                *confirmation = settled.and_then(|()| {
                    Err(lifecycle_error(
                        "legacy commit has no strict original winner",
                    ))
                });
            } else {
                ticket.settle_no_effect();
                lifecycle.settle();
            }
        } else {
            lifecycle.settle();
        }
        Ok(committed.into_result())
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

struct CompatibilityBody {
    canonical: String,
    observed: RepositoryInitializationObservation,
    effect: Option<RepositoryAcpCompatibilityEffect>,
    rows: u64,
    dispatched: bool,
    strict_winner: bool,
}

impl CompatibilityBody {
    fn observed(canonical: String, observed: RepositoryInitializationObservation) -> Self {
        Self {
            canonical,
            observed,
            effect: None,
            rows: 0,
            dispatched: false,
            strict_winner: false,
        }
    }

    fn into_result(self) -> RepositoryAcpCompatibilityResult {
        if !matches!(&self.observed, RepositoryInitializationObservation::Present {
            session_id: Some(session_id),
        } if *session_id == self.canonical)
        {
            return RepositoryAcpCompatibilityResult::SubmittedFallback {
                session_id: self.canonical,
            };
        }
        if self.dispatched && self.rows == 1 {
            if let Some(effect) = self.effect {
                return RepositoryAcpCompatibilityResult::Committed {
                    session_id: self.canonical,
                    effect,
                };
            }
        }
        RepositoryAcpCompatibilityResult::Observed {
            session_id: self.canonical,
        }
    }
}

fn observation(stored: Option<&Option<String>>) -> RepositoryInitializationObservation {
    stored.map_or(RepositoryInitializationObservation::Missing, |session_id| {
        RepositoryInitializationObservation::Present {
            session_id: session_id.clone(),
        }
    })
}

async fn compatibility_read(
    conn: &mut sqlx::SqliteConnection,
    binding: &RepositoryInitializationBinding,
) -> Result<Option<Option<String>>> {
    sqlx::query_scalar("SELECT acp_session_id FROM agent_session WHERE id=? AND workspace_id=?")
        .bind(&binding.agent_id.0)
        .bind(&binding.workspace_id.0)
        .fetch_optional(conn)
        .await
        .map_err(|error| {
            lifecycle_error(&format!(
                "compatibility initialization read failed: {error}"
            ))
        })
}

#[cfg(all(test, unix))]
mod tests;
