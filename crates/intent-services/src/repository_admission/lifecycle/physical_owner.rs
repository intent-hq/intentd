//! Original physical creation and one-use Store confirmation. The producer
//! must run its actual create/load future here; no current row proves ownership.

use std::any::Any;
use std::collections::HashSet;
use std::future::Future;
use std::sync::{Arc, Mutex, Weak};

use intent_core::{AgentId, WorkspaceId};
use intent_store::{
    RepositoryAcpCompatibilityOutcome, RepositoryAcpCompatibilityPersistence,
    RepositoryAcpCompatibilityResult, RepositoryAcpInitialization, RepositoryInitializationBinding,
    RepositoryInitializationClaim, RepositoryInitializationConfirmation,
    RepositoryInitializationOutcome, RepositoryInitializationPersistence,
    RepositoryInitializationTicket,
};

use super::{
    finish_retirement, overlaps, Origin, RepositoryLifecycleRegistry, RepositoryPhysicalOrigin,
    State, Subscription,
};
use crate::repository_admission::request_context::RepositoryCallbackContext;
use crate::repository_admission::{AdmissionError, AdmissionResult};
use intent_core::caller::Caller;
use intent_store::{RepositoryLifecycleKey, RepositoryLifecycleObserver, Store};

#[derive(Clone)]
pub(crate) enum RepositoryCreationIntent {
    FirstSet,
    Loaded { session_id: String },
    Replace { expected: Option<String> },
}

/// The actual producer result and original transaction receipt survive even
/// when the separately checked owner cannot be confirmed. No field authorizes
/// a retry, canonical fallback or pending callback upgrade.
pub(crate) struct RepositoryCreationOutcome<T, E> {
    pub producer: Result<T, E>,
    pub persistence: RepositoryInitializationPersistence,
    pub owner: AdmissionResult<RepositoryPhysicalOwner>,
}

/// The original producer, ordinary persistence/result and optional ownership
/// remain independent. Returned session strings never supply ownership proof.
pub(crate) struct RepositoryCompatibleCreationOutcome<T, E> {
    pub producer: Result<T, E>,
    pub persistence: RepositoryAcpCompatibilityPersistence,
    pub result: intent_core::Result<RepositoryAcpCompatibilityResult>,
    pub owner: intent_core::Result<RepositoryPhysicalOwner>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Allocated,
    Claimed,
    Installing,
    Confirmed,
    Retired,
}

pub(super) struct Pending {
    token: Weak<()>,
    keys: HashSet<RepositoryLifecycleKey>,
    phase: Phase,
    binding: Option<RepositoryInitializationBinding>,
    native: Option<Arc<super::native_allocation::NativeAllocation>>,
}

/// Nonclone original owner, allocated before exposing any pending endpoint.
pub(crate) struct RepositoryCreationOwner {
    registry: Arc<RepositoryLifecycleRegistry>,
    store: Store,
    token: Arc<()>,
    id: u64,
    workspace: WorkspaceId,
    agent: AgentId,
    intent: RepositoryCreationIntent,
    retirement: Arc<CreationAllocation>,
}

/// The private family candidate stays with its exact nonclone startup owner.
/// It cannot be routed through the ordinary compatibility/CAS fallback path.
pub(crate) struct RepositoryReviewCreationOwner {
    original: RepositoryCreationOwner,
    enrollment: intent_store::ReviewContextEnrollmentIntent,
    context: intent_acp::review_monitoring::ReviewMonitoringContext,
}

/// A pre-confirmation capture of exactly one retained startup. It cannot mint
/// a callback, agent caller, physical origin, or repository/account admission.
/// It only fences the approved history bootstrap while that owner is pending.
#[derive(Clone)]
pub(crate) struct RepositoryReviewBootstrap {
    store: Store,
    session: Option<String>,
    registry: Weak<RepositoryLifecycleRegistry>,
    token: Weak<()>,
    id: u64,
    workspace: WorkspaceId,
    agent: AgentId,
}

impl RepositoryReviewBootstrap {
    pub(crate) fn loaded_session(&self) -> AdmissionResult<&str> {
        self.session.as_deref().ok_or(AdmissionError::Denied)
    }
    pub(crate) fn workspace(&self) -> &WorkspaceId {
        &self.workspace
    }
    pub(crate) fn agent(&self) -> &AgentId {
        &self.agent
    }

    /// Register original history/source dependencies before reading them. A
    /// mutation already pending refuses enrollment rather than borrowing its
    /// later settled state as fresh authority.
    pub(crate) fn subscribe(&self, keys: &[RepositoryLifecycleKey]) -> AdmissionResult<()> {
        let registry = self.registry.upgrade().ok_or(AdmissionError::Retired)?;
        let mut state = registry.state.lock().map_err(|_| AdmissionError::Retired)?;
        let keys: HashSet<_> = keys.iter().cloned().collect();
        if keys.is_empty() || state.blocked(&keys) {
            return Err(AdmissionError::Retired);
        }
        let pending = state
            .creations
            .get_mut(&self.id)
            .ok_or(AdmissionError::Retired)?;
        if self.token.upgrade().is_none()
            || !Weak::ptr_eq(&pending.token, &self.token)
            || pending.phase != Phase::Allocated
        {
            return Err(AdmissionError::Retired);
        }
        pending.keys.extend(keys);
        Ok(())
    }

    pub(crate) fn native_allocation(
        &self,
    ) -> AdmissionResult<Arc<super::native_allocation::NativeAllocation>> {
        let registry = self.registry.upgrade().ok_or(AdmissionError::Retired)?;
        let state = registry.state.lock().map_err(|_| AdmissionError::Retired)?;
        let pending = state
            .creations
            .get(&self.id)
            .ok_or(AdmissionError::Retired)?;
        if self.token.upgrade().is_none()
            || !Weak::ptr_eq(&pending.token, &self.token)
            || pending.phase != Phase::Allocated
            || state.blocked(&pending.keys)
        {
            return Err(AdmissionError::Retired);
        }
        pending.native.clone().ok_or(AdmissionError::Denied)
    }

    /// Join the synchronous prebuilt transfer against pending-owner mutation.
    /// The caller must separately hold every freshly admitted source fence.
    /// This cannot remain usable after initialization claims or confirms it.
    pub(crate) fn with_current<T>(
        &self,
        transfer: impl FnOnce() -> AdmissionResult<T>,
    ) -> AdmissionResult<T> {
        let registry = self.registry.upgrade().ok_or(AdmissionError::Retired)?;
        let state = registry.state.lock().map_err(|_| AdmissionError::Retired)?;
        let pending = state
            .creations
            .get(&self.id)
            .ok_or(AdmissionError::Retired)?;
        if self.token.upgrade().is_none()
            || !Weak::ptr_eq(&pending.token, &self.token)
            || pending.phase != Phase::Allocated
            || state.blocked(&pending.keys)
        {
            return Err(AdmissionError::Retired);
        }
        transfer()
    }
}

impl RepositoryReviewCreationOwner {
    pub(crate) fn attach_native(
        &self,
        native: Arc<super::native_allocation::NativeAllocation>,
    ) -> AdmissionResult<()> {
        let mut state = self
            .original
            .registry
            .state
            .lock()
            .map_err(|_| AdmissionError::Retired)?;
        if state.shutdown {
            return Err(AdmissionError::Retired);
        }
        let pending = state
            .creations
            .get_mut(&self.original.id)
            .ok_or(AdmissionError::Retired)?;
        if pending.phase != Phase::Allocated
            || pending.native.is_some()
            || !Weak::ptr_eq(&pending.token, &Arc::downgrade(&self.original.token))
        {
            return Err(AdmissionError::Retired);
        }
        pending.native = Some(native);
        Ok(())
    }
    pub(crate) fn bootstrap(&self) -> AdmissionResult<RepositoryReviewBootstrap> {
        self.original.check_pending()?;
        let capture = RepositoryReviewBootstrap {
            store: self.original.store.clone(),
            session: match &self.original.intent {
                RepositoryCreationIntent::Loaded { session_id } => Some(session_id.clone()),
                _ => None,
            },
            registry: Arc::downgrade(&self.original.registry),
            token: Arc::downgrade(&self.original.token),
            id: self.original.id,
            workspace: self.original.workspace.clone(),
            agent: self.original.agent.clone(),
        };
        capture.subscribe(&[RepositoryLifecycleKey::MonitorHistory(
            self.original.agent.clone(),
        )])?;
        Ok(capture)
    }

    pub(crate) fn context(&self) -> &intent_acp::review_monitoring::ReviewMonitoringContext {
        &self.context
    }

    pub(crate) async fn initialize_with_outcome<T, E, F, Fut>(
        self,
        store: &Store,
        operation: F,
    ) -> AdmissionResult<RepositoryCreationOutcome<T, E>>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<(String, T), E>>,
    {
        self.original.check_pending()?;
        Ok(self
            .original
            .initialize_with_enrollment(store, Some(self.enrollment), operation)
            .await)
    }
}

#[derive(Default)]
struct CreationRetirementState {
    retired: bool,
    confirmed: Option<RepositoryPhysicalRetirement>,
}

struct CreationAllocation {
    registry: Weak<RepositoryLifecycleRegistry>,
    token: Weak<()>,
    id: u64,
    state: Mutex<CreationRetirementState>,
}

/// A pre-abort projection of this original attempt. It owns no physical
/// lifetime and cannot recover an allocation from an agent or session ID.
#[derive(Clone)]
pub(crate) struct RepositoryCreationRetirement {
    allocation: Weak<CreationAllocation>,
}

impl RepositoryCreationRetirement {
    pub(crate) fn retire(&self) {
        let Some(allocation) = self.allocation.upgrade() else {
            return;
        };
        let Some(registry) = allocation.registry.upgrade() else {
            return;
        };
        let confirmed = {
            // Consume takes these locks in the same order. A confirmation
            // cannot be published between pending retirement and this lookup.
            let mut retirement = allocation
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            retirement.retired = true;
            let mut state = registry
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(pending) = state.creations.get_mut(&allocation.id) {
                if Weak::ptr_eq(&pending.token, &allocation.token) {
                    pending.phase = Phase::Retired;
                    if let Some(native) = &pending.native {
                        native.retire();
                    }
                }
            }
            retirement.confirmed.clone()
        };
        if let Some(confirmed) = confirmed {
            // Every caller joins an already-confirmed original leaf without
            // holding the creation or registry mutex over the leaf wait.
            confirmed.retire();
        }
    }
}

fn unavailable() -> intent_core::Error {
    intent_core::Error::Internal("original repository initialization is unavailable".into())
}

fn retire_pending(state: &Mutex<State>, id: u64) {
    let mut state = state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(pending) = state.creations.get_mut(&id) {
        pending.phase = Phase::Retired;
        if let Some(native) = &pending.native {
            native.retire();
        }
    }
}

impl RepositoryCreationOwner {
    pub(crate) fn allocate(
        registry: &Arc<RepositoryLifecycleRegistry>,
        store: &Store,
        workspace: WorkspaceId,
        agent: AgentId,
        intent: RepositoryCreationIntent,
    ) -> AdmissionResult<Self> {
        let observer: Arc<dyn RepositoryLifecycleObserver> = registry.clone();
        if !store.has_repository_lifecycle_observer(&observer) {
            return Err(AdmissionError::Unavailable);
        }
        let keys = HashSet::from([
            RepositoryLifecycleKey::Database,
            RepositoryLifecycleKey::Workspace(workspace.clone()),
            RepositoryLifecycleKey::Agent(agent.clone()),
        ]);
        let token = Arc::new(());
        let mut state = registry.state.lock().map_err(|_| AdmissionError::Retired)?;
        if state.blocked(&keys) {
            return Err(AdmissionError::Unavailable);
        }
        let id = state.next()?;
        state.creations.insert(
            id,
            Pending {
                token: Arc::downgrade(&token),
                keys,
                phase: Phase::Allocated,
                binding: None,
                native: None,
            },
        );
        drop(state);
        let retirement = Arc::new(CreationAllocation {
            registry: Arc::downgrade(registry),
            token: Arc::downgrade(&token),
            id,
            state: Mutex::default(),
        });
        Ok(Self {
            registry: registry.clone(),
            store: store.clone(),
            token,
            id,
            workspace,
            agent,
            intent,
            retirement,
        })
    }

    /// Pin a random new family, or read the exact installed family for a load.
    /// This supplies no account authority and cannot confirm pending callbacks.
    pub(crate) async fn prepare_review(self) -> AdmissionResult<RepositoryReviewCreationOwner> {
        self.check_pending()?;
        let enrollment = match &self.intent {
            RepositoryCreationIntent::FirstSet | RepositoryCreationIntent::Replace { .. } => {
                intent_store::ReviewContextEnrollmentIntent::Created(
                    intent_store::ReviewContextOwnerKey::generate(),
                )
            }
            RepositoryCreationIntent::Loaded { session_id } => {
                let original = self
                    .store
                    .get_review_context_enrollment(&self.workspace, &self.agent, session_id)
                    .await
                    .map_err(|_| AdmissionError::Unavailable)?
                    .ok_or(AdmissionError::Unavailable)?;
                intent_store::ReviewContextEnrollmentIntent::Loaded(original)
            }
        };
        self.check_pending()?;
        let key = match &enrollment {
            intent_store::ReviewContextEnrollmentIntent::Created(key) => key.as_str(),
            intent_store::ReviewContextEnrollmentIntent::Loaded(original) => original.owner_key(),
        };
        let context = intent_acp::review_monitoring::ReviewMonitoringContext::new(key.to_owned())
            .ok_or(AdmissionError::Unavailable)?;
        Ok(RepositoryReviewCreationOwner {
            original: self,
            enrollment,
            context,
        })
    }

    fn check_pending(&self) -> AdmissionResult<()> {
        let state = self
            .registry
            .state
            .lock()
            .map_err(|_| AdmissionError::Retired)?;
        let pending = state
            .creations
            .get(&self.id)
            .ok_or(AdmissionError::Retired)?;
        if pending.phase != Phase::Allocated
            || !Weak::ptr_eq(&pending.token, &Arc::downgrade(&self.token))
        {
            return Err(AdmissionError::Retired);
        }
        Ok(())
    }

    /// This projection remains unavailable forever, including after success.
    pub(crate) fn callback(&self) -> RepositoryCallbackContext {
        RepositoryCallbackContext::new(&self.registry, None)
    }

    pub(crate) fn retirement(&self) -> RepositoryCreationRetirement {
        RepositoryCreationRetirement {
            allocation: Arc::downgrade(&self.retirement),
        }
    }

    /// Own the original producer future and its exact intent across the await.
    /// A successful load must return the originally requested session. Only
    /// this path can construct the private proof consumed by the Store observer.
    pub(crate) async fn initialize<F, Fut>(
        self,
        store: &Store,
        operation: F,
    ) -> AdmissionResult<RepositoryPhysicalOwner>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = AdmissionResult<String>>,
    {
        let outcome = self
            .initialize_with_outcome(store, || async move {
                operation().await.map(|session| (session, ()))
            })
            .await;
        match outcome.producer {
            Ok(()) => outcome.owner,
            Err(error) => Err(error),
        }
    }

    /// Run the original producer exactly once and retain its owned payload or
    /// error. The session comes from that producer; Store facts and ownership
    /// stay separate. This uses the strict initialization transaction and does
    /// not claim the ordinary legacy writer's excluded accounting effects.
    pub(crate) async fn initialize_with_outcome<T, E, F, Fut>(
        self,
        store: &Store,
        operation: F,
    ) -> RepositoryCreationOutcome<T, E>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<(String, T), E>>,
    {
        self.initialize_with_enrollment(store, None, operation)
            .await
    }

    async fn initialize_with_enrollment<T, E, F, Fut>(
        self,
        store: &Store,
        enrollment: Option<intent_store::ReviewContextEnrollmentIntent>,
        operation: F,
    ) -> RepositoryCreationOutcome<T, E>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<(String, T), E>>,
    {
        let (session, payload) = match operation().await {
            Ok(produced) => produced,
            Err(error) => {
                return RepositoryCreationOutcome {
                    producer: Err(error),
                    persistence: RepositoryInitializationPersistence::NotAttempted,
                    owner: Err(AdmissionError::Unavailable),
                };
            }
        };
        let (claim, binding) = match self.claim_after_success(session) {
            Ok(claim) => claim,
            Err(error) => {
                return RepositoryCreationOutcome {
                    producer: Ok(payload),
                    persistence: RepositoryInitializationPersistence::NotAttempted,
                    owner: Err(error),
                };
            }
        };
        let outcome = match enrollment {
            Some(enrollment) => {
                store
                    .initialize_repository_review_session_outcome(claim, binding, enrollment)
                    .await
            }
            None => {
                store
                    .initialize_repository_acp_session_outcome(claim, binding)
                    .await
            }
        };
        self.complete_outcome(store, payload, outcome)
    }

    fn complete_outcome<T, E>(
        self,
        store: &Store,
        payload: T,
        outcome: RepositoryInitializationOutcome,
    ) -> RepositoryCreationOutcome<T, E> {
        let owner = outcome
            .confirmation
            .map_err(|_| AdmissionError::Unavailable)
            .and_then(|confirmation| self.consume(store, confirmation));
        RepositoryCreationOutcome {
            producer: Ok(payload),
            persistence: outcome.persistence,
            owner,
        }
    }

    /// Run the original producer once, retaining ordinary ACP behavior even
    /// when this pending allocation was retired. Only the captured Store and
    /// original intent can reach the sole compatibility transaction.
    pub(crate) async fn initialize_compatible<T, E, F, Fut>(
        self,
        operation: F,
    ) -> RepositoryCompatibleCreationOutcome<T, E>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<(String, T), E>>,
    {
        let (session_id, payload) = match operation().await {
            Ok(produced) => produced,
            Err(error) => {
                return RepositoryCompatibleCreationOutcome {
                    producer: Err(error),
                    persistence: RepositoryAcpCompatibilityPersistence::NotAttempted,
                    result: Err(unavailable()),
                    owner: Err(unavailable()),
                };
            }
        };
        let action = match &self.intent {
            RepositoryCreationIntent::FirstSet => RepositoryAcpInitialization::FirstSet {
                session_id: session_id.clone(),
            },
            RepositoryCreationIntent::Loaded {
                session_id: expected,
            } => {
                if expected != &session_id {
                    return RepositoryCompatibleCreationOutcome {
                        producer: Ok(payload),
                        persistence: RepositoryAcpCompatibilityPersistence::NotAttempted,
                        result: Err(intent_core::Error::Internal(
                            "original repository load returned another session".into(),
                        )),
                        owner: Err(unavailable()),
                    };
                }
                RepositoryAcpInitialization::Loaded {
                    session_id: session_id.clone(),
                }
            }
            RepositoryCreationIntent::Replace { expected } => {
                RepositoryAcpInitialization::Replace {
                    expected: expected.clone(),
                    session_id: session_id.clone(),
                }
            }
        };
        let binding = RepositoryInitializationBinding {
            workspace_id: self.workspace.clone(),
            agent_id: self.agent.clone(),
            action,
        };
        // Missing/retired proof cannot become a new claim. Store may preserve
        // ordinary behavior under its distinct mutation owner, never by
        // replacing this original attempt or changing its captured database.
        let claim = self
            .claim_after_success(session_id)
            .ok()
            .map(|(claim, _)| claim);
        let outcome = self
            .store
            .initialize_repository_acp_session_compatible(claim, binding)
            .await;
        self.complete_compatible(payload, outcome)
    }

    fn complete_compatible<T, E>(
        self,
        payload: T,
        outcome: RepositoryAcpCompatibilityOutcome,
    ) -> RepositoryCompatibleCreationOutcome<T, E> {
        let store = self.store.clone();
        // Preserve the original Store confirmation error without projecting
        // it into a generic denial or erasing a known committed result.
        let owner = outcome.confirmation.and_then(|confirmation| {
            self.consume(&store, confirmation).map_err(|error| {
                intent_core::Error::Internal(format!(
                    "original repository owner confirmation rejected: {error:?}"
                ))
            })
        });
        RepositoryCompatibleCreationOutcome {
            producer: Ok(payload),
            persistence: outcome.persistence,
            result: outcome.result,
            owner,
        }
    }

    fn claim_after_success(
        &self,
        session_id: String,
    ) -> AdmissionResult<(
        RepositoryInitializationClaim,
        RepositoryInitializationBinding,
    )> {
        if session_id.is_empty() {
            return Err(AdmissionError::Denied);
        }
        let action = match &self.intent {
            RepositoryCreationIntent::FirstSet => {
                RepositoryAcpInitialization::FirstSet { session_id }
            }
            RepositoryCreationIntent::Loaded {
                session_id: expected,
            } => {
                if &session_id != expected {
                    return Err(AdmissionError::BindingChanged);
                }
                RepositoryAcpInitialization::Loaded { session_id }
            }
            RepositoryCreationIntent::Replace { expected } => {
                RepositoryAcpInitialization::Replace {
                    expected: expected.clone(),
                    session_id,
                }
            }
        };
        let binding = RepositoryInitializationBinding {
            workspace_id: self.workspace.clone(),
            agent_id: self.agent.clone(),
            action,
        };
        {
            let mut state = self
                .registry
                .state
                .lock()
                .map_err(|_| AdmissionError::Retired)?;
            let pending = state
                .creations
                .get_mut(&self.id)
                .ok_or(AdmissionError::Retired)?;
            if pending.phase != Phase::Allocated
                || !Weak::ptr_eq(&pending.token, &Arc::downgrade(&self.token))
            {
                return Err(AdmissionError::Retired);
            }
            pending.phase = Phase::Claimed;
            pending.binding = Some(binding.clone());
        }
        let proof = OriginalProof {
            registry: Arc::downgrade(&self.registry),
            token: Arc::downgrade(&self.token),
            id: self.id,
            binding: binding.clone(),
            consumed: false,
        };
        let observer: Arc<dyn RepositoryLifecycleObserver> = self.registry.clone();
        // Preserve the actual allocation domain even when another Store uses
        // the same observer and identical workspace/agent IDs.
        let claim = self
            .store
            .bind_repository_initialization_claim(observer, Box::new(proof))
            .map_err(|_| AdmissionError::Unavailable)?;
        Ok((claim, binding))
    }

    fn consume(
        self,
        store: &Store,
        confirmation: RepositoryInitializationConfirmation,
    ) -> AdmissionResult<RepositoryPhysicalOwner> {
        let observer: Arc<dyn RepositoryLifecycleObserver> = self.registry.clone();
        let (binding, proof) = store
            .consume_repository_initialization_confirmation(&observer, confirmation)
            .map_err(|_| AdmissionError::Unavailable)?;
        let mut proof = proof
            .downcast::<Completion>()
            .map_err(|_| AdmissionError::Denied)?;
        let registry = proof.registry.upgrade().ok_or(AdmissionError::Retired)?;
        if !Arc::ptr_eq(&registry, &self.registry)
            || proof.id != self.id
            || !Weak::ptr_eq(&proof.token, &Arc::downgrade(&self.token))
            || proof.binding != binding
        {
            return Err(AdmissionError::Denied);
        }
        let mut retirement = self
            .retirement
            .state
            .lock()
            .map_err(|_| AdmissionError::Retired)?;
        if retirement.retired {
            return Err(AdmissionError::Retired);
        }
        let token = Arc::new(());
        let mut state = self
            .registry
            .state
            .lock()
            .map_err(|_| AdmissionError::Retired)?;
        let pending = state
            .creations
            .get(&self.id)
            .ok_or(AdmissionError::Retired)?;
        if pending.phase != Phase::Confirmed
            || pending.binding.as_ref() != Some(&binding)
            || pending.token.upgrade().is_none()
            || state.blocked(&pending.keys)
        {
            return Err(AdmissionError::Retired);
        }
        let keys = pending.keys.clone();
        let native = pending.native.clone();
        let id = state.next()?;
        state.creations.remove(&self.id);
        state.origins.insert(
            id,
            Origin {
                token: Arc::downgrade(&token),
                caller: Caller::Agent {
                    agent_id: self.agent.clone(),
                },
                keys,
                retired: false,
                native,
            },
        );
        proof.consumed = true;
        let owner = RepositoryPhysicalOwner {
            registry: self.registry.clone(),
            store: self.store.clone(),
            token,
            id,
            session_id: match binding.action {
                RepositoryAcpInitialization::FirstSet { session_id }
                | RepositoryAcpInitialization::Loaded { session_id }
                | RepositoryAcpInitialization::Replace { session_id, .. } => session_id,
            },
            _creation_retirement: self.retirement.clone(),
        };
        retirement.confirmed = Some(owner.retirement());
        drop(state);
        drop(retirement);
        Ok(owner)
    }
}

impl Drop for RepositoryCreationOwner {
    fn drop(&mut self) {
        // An unknown Store ticket retains its own barrier even after this
        // creator is gone. No dropped creator is revived by a later receipt.
        let mut state = self
            .registry
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(pending) = state.creations.remove(&self.id) {
            if let Some(native) = pending.native {
                state.retain_native_retirement(pending.keys, native);
            }
        }
    }
}

struct OriginalProof {
    registry: Weak<RepositoryLifecycleRegistry>,
    token: Weak<()>,
    id: u64,
    binding: RepositoryInitializationBinding,
    consumed: bool,
}

impl Drop for OriginalProof {
    fn drop(&mut self) {
        if !self.consumed {
            if let Some(registry) = self.registry.upgrade() {
                retire_pending(&registry.state, self.id);
            }
        }
    }
}

struct Completion {
    registry: Weak<RepositoryLifecycleRegistry>,
    token: Weak<()>,
    id: u64,
    binding: RepositoryInitializationBinding,
    consumed: bool,
}

impl Drop for Completion {
    fn drop(&mut self) {
        if !self.consumed {
            if let Some(registry) = self.registry.upgrade() {
                retire_pending(&registry.state, self.id);
            }
        }
    }
}

struct Initialization {
    registry: Arc<RepositoryLifecycleRegistry>,
    token: Weak<()>,
    creator: u64,
    barrier: u64,
    binding: RepositoryInitializationBinding,
    settled: bool,
}

impl RepositoryInitializationTicket for Initialization {
    fn join_retirement(
        &self,
        confirm_original: bool,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = intent_core::Result<()>> + Send + '_>>
    {
        let registry = self.registry.clone();
        let barrier = self.barrier;
        let creator = self.creator;
        Box::pin(async move {
            let obligations = {
                let mut state = registry.state.lock().map_err(|_| unavailable())?;
                let keys = state.pending.get(&barrier).ok_or_else(unavailable)?.clone();
                if !confirm_original {
                    if let Some(pending) = state.creations.get_mut(&creator) {
                        pending.phase = Phase::Retired;
                        if let Some(native) = &pending.native {
                            native.retire();
                        }
                    }
                }
                super::native_obligations(&state, &keys)
            };
            for native in obligations {
                native.join().await.map_err(|_| unavailable())?;
            }
            Ok(())
        })
    }
    fn finish_confirmed(mut self: Box<Self>) -> intent_core::Result<Box<dyn Any + Send>> {
        let mut state = self.registry.state.lock().map_err(|_| unavailable())?;
        // SQL already committed: settle ONLY this original known-completed
        // barrier even when a competing mutation invalidated the creator.
        state.pending.remove(&self.barrier);
        self.settled = true;
        let pending = state
            .creations
            .get_mut(&self.creator)
            .ok_or_else(unavailable)?;
        if pending.phase != Phase::Installing
            || self.token.upgrade().is_none()
            || !Weak::ptr_eq(&pending.token, &self.token)
            || pending.binding.as_ref() != Some(&self.binding)
        {
            pending.phase = Phase::Retired;
            if let Some(native) = &pending.native {
                native.retire();
            }
            return Err(unavailable());
        }
        pending.phase = Phase::Confirmed;
        Ok(Box::new(Completion {
            registry: Arc::downgrade(&self.registry),
            token: self.token.clone(),
            id: self.creator,
            binding: self.binding.clone(),
            consumed: false,
        }))
    }

    fn settle_no_effect(mut self: Box<Self>) {
        let mut state = self
            .registry
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.pending.remove(&self.barrier);
        if let Some(pending) = state.creations.get_mut(&self.creator) {
            pending.phase = Phase::Retired;
            if let Some(native) = &pending.native {
                native.retire();
            }
        }
        self.settled = true;
    }

    fn settle_committed_without_confirmation(mut self: Box<Self>) -> intent_core::Result<()> {
        let mut state = self.registry.state.lock().map_err(|_| unavailable())?;
        // This ticket acknowledges only its own actual committed effect.
        // No completion is created, even if the pending attempt is still live.
        state.pending.remove(&self.barrier);
        if let Some(pending) = state.creations.get_mut(&self.creator) {
            if Weak::ptr_eq(&pending.token, &self.token)
                && pending.binding.as_ref() == Some(&self.binding)
            {
                pending.phase = Phase::Retired;
                if let Some(native) = &pending.native {
                    native.retire();
                }
            }
        }
        self.settled = true;
        Ok(())
    }
}

impl Drop for Initialization {
    fn drop(&mut self) {
        if !self.settled {
            retire_pending(&self.registry.state, self.creator);
        }
    }
}

pub(super) fn shutdown_pending_native(
    state: &mut State,
) -> Vec<(
    HashSet<RepositoryLifecycleKey>,
    Arc<super::native_allocation::NativeAllocation>,
)> {
    state
        .creations
        .values_mut()
        .filter_map(|pending| {
            pending.phase = Phase::Retired;
            pending
                .native
                .clone()
                .map(|native| (pending.keys.clone(), native))
        })
        .collect()
}

pub(super) fn retire_pending_native_contexts(
    state: &mut State,
    keys: &HashSet<RepositoryLifecycleKey>,
) {
    for pending in state
        .creations
        .values_mut()
        .filter(|pending| overlaps(keys, &pending.keys))
    {
        if let Some(native) = &pending.native {
            native.retire();
        }
    }
}

pub(super) fn pending_native_retirements(
    state: &State,
    keys: &HashSet<RepositoryLifecycleKey>,
) -> Vec<Arc<super::native_allocation::NativeAllocation>> {
    state
        .creations
        .values()
        .filter(|pending| overlaps(keys, &pending.keys))
        .filter_map(|pending| pending.native.as_ref())
        .filter(|native| native.is_retired())
        .cloned()
        .collect()
}

pub(super) fn invalidate_pending(
    state: &mut State,
    keys: &HashSet<RepositoryLifecycleKey>,
    except: Option<u64>,
) {
    for (id, pending) in &mut state.creations {
        if Some(*id) != except && overlaps(keys, &pending.keys) {
            pending.phase = Phase::Retired;
            if let Some(native) = &pending.native {
                native.retire();
            }
        }
    }
}

pub(super) fn begin_initialization(
    observer: &RepositoryLifecycleRegistry,
    proof: Box<dyn Any + Send>,
    binding: &RepositoryInitializationBinding,
) -> intent_core::Result<Box<dyn RepositoryInitializationTicket>> {
    let mut proof = proof
        .downcast::<OriginalProof>()
        .map_err(|_| unavailable())?;
    let registry = proof.registry.upgrade().ok_or_else(unavailable)?;
    if !std::ptr::eq(observer, Arc::as_ptr(&registry))
        || &proof.binding != binding
        || proof.token.upgrade().is_none()
    {
        return Err(unavailable());
    }
    let mut state = registry.state.lock().map_err(|_| unavailable())?;
    let pending = state.creations.get(&proof.id).ok_or_else(unavailable)?;
    if pending.phase != Phase::Claimed
        || pending.binding.as_ref() != Some(binding)
        || !Weak::ptr_eq(&pending.token, &proof.token)
        || state.blocked(&pending.keys)
    {
        return Err(unavailable());
    }
    // Initialization replaces this agent's physical session, not every session
    // observing the same workspace. The pending allocation still validates its
    // workspace/database keys above; workspace mutations retire it separately.
    let keys = HashSet::from([RepositoryLifecycleKey::Agent(binding.agent_id.clone())]);
    let barrier = state.next().map_err(|_| unavailable())?;
    state.pending.insert(barrier, keys.clone());
    invalidate_pending(&mut state, &keys, Some(proof.id));
    state
        .creations
        .get_mut(&proof.id)
        .ok_or_else(unavailable)?
        .phase = Phase::Installing;
    let mut native = Vec::new();
    let mut origins = HashSet::new();
    for (id, origin) in &mut state.origins {
        if overlaps(&keys, &origin.keys) {
            origin.retired = true;
            if let Some(allocation) = origin.native.clone() {
                native.push((origin.keys.clone(), allocation));
            }
            origins.insert(*id);
        }
    }
    for (keys, allocation) in native {
        state.retain_native_retirement(keys, allocation);
    }
    // LOAD confirms a new physical owner of the same persisted session. It
    // retires old physical producers, but does not replace the agent's stored
    // history or revoke independently admitted wire readers of that history.
    // Actual session replacement and Store mutations still retire those readers.
    let replaces_session = !matches!(binding.action, RepositoryAcpInitialization::Loaded { .. });
    let affected = |entry: &Subscription| {
        origins.contains(&entry.origin) || (replaces_session && overlaps(&keys, &entry.keys))
    };
    state.detach(affected);
    let leaves = state.pending_leaves(affected);
    proof.consumed = true;
    drop(state);
    finish_retirement(&registry.state, &leaves);
    Ok(Box::new(Initialization {
        registry,
        token: proof.token.clone(),
        creator: proof.id,
        barrier,
        binding: binding.clone(),
        settled: false,
    }))
}

/// The nonclone strong lifetime belongs to the actual physical handle.
pub(crate) struct RepositoryPhysicalOwner {
    registry: Arc<RepositoryLifecycleRegistry>,
    store: Store,
    token: Arc<()>,
    id: u64,
    session_id: String,
    // The original pending retirement handle stays weak, but can follow the
    // confirmed allocation for as long as the actual physical owner exists.
    _creation_retirement: Arc<CreationAllocation>,
}

#[derive(Clone)]
pub(crate) struct RepositoryPhysicalRetirement {
    origin: RepositoryPhysicalOrigin,
}

impl RepositoryPhysicalRetirement {
    #[cfg(test)]
    pub(crate) fn detached_for_test(&self) -> bool {
        self.origin.registry.upgrade().is_none_or(|registry| {
            !registry
                .state
                .lock()
                .unwrap()
                .origins
                .contains_key(&self.origin.id)
        })
    }

    pub(crate) fn retire(&self) {
        let Some(registry) = self.origin.registry.upgrade() else {
            return;
        };
        let leaves = {
            let mut state = registry
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(origin) = state.origins.get(&self.origin.id) {
                if !Weak::ptr_eq(&origin.token, &self.origin.token) {
                    return;
                }
                if let Some(origin) = state.origins.remove(&self.origin.id) {
                    if let Some(native) = origin.native {
                        state.retain_native_retirement(origin.keys, native);
                    }
                }
                state.detach(|entry| entry.origin == self.origin.id);
            }
            // Every original pre-abort caller must join an ongoing retirement,
            // including one whose peer already removed the live map entry.
            state.pending_leaves(|entry| entry.origin == self.origin.id)
        };
        finish_retirement(&registry.state, &leaves);
    }
}

impl RepositoryPhysicalOwner {
    #[cfg(test)]
    pub(crate) fn native_allocation_for_test(
        &self,
    ) -> Arc<super::native_allocation::NativeAllocation> {
        self.registry
            .state
            .lock()
            .unwrap()
            .origins
            .get(&self.id)
            .unwrap()
            .native
            .clone()
            .unwrap()
    }

    /// Captured from the one-use initialization confirmation, never a later
    /// session-row lookup or caller-supplied prompt parameter.
    pub(crate) fn confirmed_session_id(&self) -> &str {
        &self.session_id
    }

    fn origin(&self) -> RepositoryPhysicalOrigin {
        RepositoryPhysicalOrigin {
            registry: Arc::downgrade(&self.registry),
            id: self.id,
            token: Arc::downgrade(&self.token),
        }
    }

    pub(crate) fn callback(&self) -> RepositoryCallbackContext {
        RepositoryCallbackContext::for_physical_owner(self)
    }

    pub(in crate::repository_admission) fn callback_binding(
        &self,
    ) -> (
        &Arc<RepositoryLifecycleRegistry>,
        RepositoryPhysicalOrigin,
        &Store,
    ) {
        (&self.registry, self.origin(), &self.store)
    }

    pub(crate) fn retirement(&self) -> RepositoryPhysicalRetirement {
        RepositoryPhysicalRetirement {
            origin: self.origin(),
        }
    }

    pub(crate) fn interrupt_requests(&self) {
        self.registry.cancel_origin_requests(&self.origin());
    }
}

impl Drop for RepositoryPhysicalOwner {
    fn drop(&mut self) {
        self.retirement().retire();
    }
}

#[cfg(test)]
#[path = "physical_owner/tests.rs"]
mod tests;

#[path = "physical_owner/history.rs"]
mod history;
