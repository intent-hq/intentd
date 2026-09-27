//! Original physical creation and one-use Store confirmation. The producer
//! must run its actual create/load future here; no current row proves ownership.

use std::any::Any;
use std::collections::HashSet;
use std::future::Future;
use std::sync::{Arc, Mutex, Weak};

use intent_core::{AgentId, WorkspaceId};
use intent_store::{
    RepositoryAcpInitialization, RepositoryInitializationBinding, RepositoryInitializationClaim,
    RepositoryInitializationConfirmation, RepositoryInitializationTicket,
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
            },
        );
        drop(state);
        Ok(Self {
            registry: registry.clone(),
            store: store.clone(),
            token,
            id,
            workspace,
            agent,
            intent,
        })
    }

    /// This projection remains unavailable forever, including after success.
    pub(crate) fn callback(&self) -> RepositoryCallbackContext {
        RepositoryCallbackContext::new(&self.registry, None)
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
        let session = operation().await?;
        let (claim, binding) = self.claim_after_success(session)?;
        let confirmation = store
            .initialize_repository_acp_session(claim, binding)
            .await
            .map_err(|_| AdmissionError::Unavailable)?;
        self.consume(store, confirmation)
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
            },
        );
        proof.consumed = true;
        drop(state);
        Ok(RepositoryPhysicalOwner {
            registry: self.registry.clone(),
            token,
            id,
        })
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
        state.creations.remove(&self.id);
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
        }
        self.settled = true;
    }
}

impl Drop for Initialization {
    fn drop(&mut self) {
        if !self.settled {
            retire_pending(&self.registry.state, self.creator);
        }
    }
}

pub(super) fn invalidate_pending(
    state: &mut State,
    keys: &HashSet<RepositoryLifecycleKey>,
    except: Option<u64>,
) {
    for (id, pending) in &mut state.creations {
        if Some(*id) != except && overlaps(keys, &pending.keys) {
            pending.phase = Phase::Retired;
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
    let mut keys = pending.keys.clone();
    keys.remove(&RepositoryLifecycleKey::Database);
    let barrier = state.next().map_err(|_| unavailable())?;
    state.pending.insert(barrier, keys.clone());
    invalidate_pending(&mut state, &keys, Some(proof.id));
    state
        .creations
        .get_mut(&proof.id)
        .ok_or_else(unavailable)?
        .phase = Phase::Installing;
    let mut origins = HashSet::new();
    for (id, origin) in &mut state.origins {
        if overlaps(&keys, &origin.keys) {
            origin.retired = true;
            origins.insert(*id);
        }
    }
    let affected =
        |entry: &Subscription| origins.contains(&entry.origin) || overlaps(&keys, &entry.keys);
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
    token: Arc<()>,
    id: u64,
}

#[derive(Clone)]
pub(crate) struct RepositoryPhysicalRetirement {
    origin: RepositoryPhysicalOrigin,
}

impl RepositoryPhysicalRetirement {
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
                state.origins.remove(&self.origin.id);
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
    fn origin(&self) -> RepositoryPhysicalOrigin {
        RepositoryPhysicalOrigin {
            registry: Arc::downgrade(&self.registry),
            id: self.id,
            token: Arc::downgrade(&self.token),
        }
    }

    pub(crate) fn callback(&self) -> RepositoryCallbackContext {
        RepositoryCallbackContext::new(&self.registry, Some(self.origin()))
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
