//! Ownership of one physical handle, independent of its reusable `AgentId`.

use std::sync::{Arc, Mutex};

use intent_acp::callback_registration::CallbackOffer;
use intent_core::{AgentId, AgentSession, WorkspaceId};
use intent_store::Store;

use crate::repository_admission::lifecycle::physical_owner::{
    RepositoryCreationIntent, RepositoryCreationOwner, RepositoryCreationRetirement,
    RepositoryPhysicalOwner, RepositoryPhysicalRetirement,
};
use crate::repository_admission::lifecycle::RepositoryLifecycleRegistry;
use crate::repository_admission::request_context::RepositoryCallbackContext;
use crate::Services;

use super::{AgentHandle, Handles, ProcessRegistry};

pub(super) mod callback_delivery;
use callback_delivery::{ConfirmedCallbackAttempt, Endpoint, EndpointBlueprint};

struct Original {
    registry: Arc<RepositoryLifecycleRegistry>,
    store: Store,
    workspace: WorkspaceId,
    agent: AgentId,
}

#[derive(Default)]
struct State {
    retired: bool,
    blueprint: Option<Arc<EndpointBlueprint>>,
    endpoint: Option<Arc<Endpoint>>,
    callback_offer: CallbackOffer,
    attempt: Option<Arc<()>>,
    pending: Option<(RepositoryCreationIntent, RepositoryCreationOwner)>,
    creation_retirements: Vec<RepositoryCreationRetirement>,
    owner: Option<RepositoryPhysicalOwner>,
    retirement: Option<RepositoryPhysicalRetirement>,
}

/// The actual handle owns this slot. Escaped references can only retain that
/// original slot; teardown retires it before aborting or removing the handle.
pub(super) struct RepositoryOrigin {
    original: Option<Original>,
    state: Mutex<State>,
}

/// Correlates a completion with its original attempt. This carries no authority;
/// only the independently confirmed physical owner can supply a usable callback.
pub(super) struct SessionAttempt(Arc<()>);

impl RepositoryOrigin {
    pub(super) async fn allocate(services: &Services, session: &AgentSession) -> Arc<Self> {
        let registry = match services.repository_lifecycle_registry().await {
            Ok(registry) => registry,
            Err(error) => {
                tracing::debug!(agent = %session.id, ?error, "repository ownership unavailable");
                return Self::unavailable();
            }
        };
        let intent = session.acp_session_id.as_ref().map_or(
            RepositoryCreationIntent::FirstSet,
            |session_id| RepositoryCreationIntent::Loaded {
                session_id: session_id.clone(),
            },
        );
        let creator = RepositoryCreationOwner::allocate(
            &registry,
            &services.store,
            session.workspace_id.clone(),
            session.id.clone(),
            intent.clone(),
        );
        let Ok(creator) = creator else {
            return Self::unavailable();
        };
        Arc::new(Self {
            original: Some(Original {
                registry,
                store: services.store.clone(),
                workspace: session.workspace_id.clone(),
                agent: session.id.clone(),
            }),
            state: Mutex::new(State {
                callback_offer: if session.harness_version == "3.0" && session.retired_at.is_none()
                {
                    CallbackOffer::V1
                } else {
                    CallbackOffer::Disabled
                },
                creation_retirements: vec![creator.retirement()],
                pending: Some((intent, creator)),
                ..State::default()
            }),
        })
    }

    pub(super) fn unavailable() -> Arc<Self> {
        Arc::new(Self {
            original: None,
            state: Mutex::new(State::default()),
        })
    }

    /// This context is installed before the child receives its MCP endpoint.
    /// It is never replaced by the confirmed owner's distinct callback.
    pub(super) fn pending_callback(&self) -> Option<RepositoryCallbackContext> {
        self.state
            .lock()
            .unwrap()
            .pending
            .as_ref()
            .map(|(_, creator)| creator.callback())
    }

    /// Take the original creation attempt before its actual ACP operation.
    /// A fallback load/new attempt allocates its own original proof, without
    /// changing any callback that was already given to the process.
    pub(super) fn begin_session(
        &self,
        intent: RepositoryCreationIntent,
    ) -> Option<(RepositoryCreationOwner, SessionAttempt)> {
        let attempt = SessionAttempt(Arc::new(()));
        let previous = {
            let mut state = self.state.lock().unwrap();
            if state.retired {
                return None;
            }
            state.attempt = Some(attempt.0.clone());
            (state.owner.take(), state.endpoint.take())
        };
        // A previously admitted stage may be draining. Keep the physical slot
        // and the manager map available to other retirement callers meanwhile.
        if let Some(owner) = previous.0 {
            owner.retirement().retire();
        }
        if let Some(endpoint) = previous.1 {
            endpoint.retire();
        }
        let mut state = self.state.lock().unwrap();
        if state.retired
            || !state
                .attempt
                .as_ref()
                .is_some_and(|current| Arc::ptr_eq(current, &attempt.0))
        {
            return None;
        }
        if let Some((original_intent, creator)) = state.pending.take() {
            if same_intent(&original_intent, &intent) {
                return Some((creator, attempt));
            }
        }
        let original = self.original.as_ref()?;
        let creator = RepositoryCreationOwner::allocate(
            &original.registry,
            &original.store,
            original.workspace.clone(),
            original.agent.clone(),
            intent,
        )
        .ok()?;
        // Keep only the weak projection before the nonclone creator escapes
        // into its original ACP future. Every hard-retire caller joins it.
        state.creation_retirements.push(creator.retirement());
        Some((creator, attempt))
    }

    pub(super) fn install(&self, attempt: SessionAttempt, owner: RepositoryPhysicalOwner) -> bool {
        let SessionAttempt(attempt) = attempt;
        let mut state = self.state.lock().unwrap();
        if state.retired
            || !state
                .attempt
                .as_ref()
                .is_some_and(|current| Arc::ptr_eq(current, &attempt))
        {
            drop(state);
            owner.retirement().retire();
            return false;
        }
        state.retirement = Some(owner.retirement());
        state.owner = Some(owner);
        true
    }

    pub(super) fn configure_callbacks(&self, blueprint: EndpointBlueprint) {
        let mut state = self.state.lock().unwrap();
        if state.blueprint.is_none() && !state.retired {
            state.blueprint = Some(Arc::new(blueprint));
        }
    }

    pub(super) fn callback_offer(
        &self,
        session: &AgentSession,
        provider: &intent_providers::ProviderConfig,
    ) -> CallbackOffer {
        // Offering the extension grants no authority. The original physical
        // confirmation, caller, read anchor and final admission remain required.
        let Some(original) = &self.original else {
            return CallbackOffer::Disabled;
        };
        if session.harness_version != "3.0"
            || session.retired_at.is_some()
            || session.id != original.agent
            || session.workspace_id != original.workspace
            || provider.id != "claude-code"
        {
            return CallbackOffer::Disabled;
        }
        let state = self.state.lock().unwrap();
        if !state.retired && state.blueprint.is_some() {
            return state.callback_offer;
        }
        CallbackOffer::Disabled
    }

    fn current_attempt(&self, attempt: &SessionAttempt) -> bool {
        let state = self.state.lock().unwrap();
        !state.retired
            && state.owner.is_some()
            && state
                .attempt
                .as_ref()
                .is_some_and(|current| Arc::ptr_eq(current, &attempt.0))
    }

    fn attach_endpoint(&self, attempt: &SessionAttempt, endpoint: Arc<Endpoint>) -> bool {
        let mut state = self.state.lock().unwrap();
        if state.retired
            || state.endpoint.is_some()
            || state.owner.is_none()
            || !state
                .attempt
                .as_ref()
                .is_some_and(|current| Arc::ptr_eq(current, &attempt.0))
        {
            return false;
        }
        state.endpoint = Some(endpoint);
        true
    }

    fn remove_endpoint(&self, attempt: &SessionAttempt, endpoint: &Arc<Endpoint>) {
        let removed = {
            let mut state = self.state.lock().unwrap();
            if state
                .attempt
                .as_ref()
                .is_some_and(|current| Arc::ptr_eq(current, &attempt.0))
                && state
                    .endpoint
                    .as_ref()
                    .is_some_and(|current| Arc::ptr_eq(current, endpoint))
            {
                state.endpoint.take()
            } else {
                None
            }
        };
        drop(removed);
    }

    pub(super) fn accept_session<T>(
        self: &Arc<Self>,
        attempt: SessionAttempt,
        connection: &Arc<intent_acp::Connection>,
        outcome: crate::agent_session::RepositorySessionOutcome<T>,
    ) -> (T, Option<ConfirmedCallbackAttempt>) {
        let mut delivery = None;
        match outcome.owner {
            Ok(owner) => {
                // Capture only from this original consumed owner, before moving it into the slot.
                let context = owner.callback();
                let retirement = owner.retirement();
                let original_attempt = SessionAttempt(attempt.0.clone());
                let blueprint = self.state.lock().unwrap().blueprint.clone();
                // Bind once, synchronously, outside the state lock while the
                // consumed original physical owner is still directly available.
                let live = outcome.query.as_ref().and_then(|_| {
                    let original = self.original.as_ref()?;
                    blueprint.as_ref()?.bind_context(
                        connection,
                        &owner,
                        &original.workspace,
                        &original.agent,
                    )
                });
                if self.install(attempt, owner) {
                    if let (Some(query), Some(blueprint)) = (outcome.query, blueprint) {
                        delivery = Some(ConfirmedCallbackAttempt::new(
                            self,
                            original_attempt,
                            context,
                            retirement,
                            blueprint,
                            query,
                            live,
                        ));
                    }
                }
            }
            Err(error) => {
                tracing::debug!(%error, "original session has no repository owner");
            }
        }
        (outcome.response, delivery)
    }

    /// Every caller joins the SAME R retirement, even if another caller has
    /// already marked this handle retired. No handles-map lock is held here.
    pub(super) fn retire(&self) {
        let (pending, creations, retirement, endpoint) = {
            let mut state = self.state.lock().unwrap();
            state.retired = true;
            (
                state.pending.take(),
                state.creation_retirements.clone(),
                state.retirement.clone(),
                state.endpoint.clone(),
            )
        };
        for creation in creations {
            creation.retire();
        }
        if let Some(retirement) = retirement {
            retirement.retire();
        }
        if let Some(endpoint) = endpoint {
            endpoint.retire();
        }
        drop(pending);
    }

    pub(super) fn capture_prompt(
        &self,
        connection: &Arc<intent_acp::Connection>,
    ) -> Option<callback_delivery::RepositoryPromptInput> {
        let endpoint = {
            let state = self.state.lock().unwrap();
            if state.retired {
                return None;
            }
            state.endpoint.clone()?
        };
        endpoint.capture_prompt(connection)
    }

    pub(super) fn interrupt_requests(&self) {
        let endpoint = {
            let state = self.state.lock().unwrap();
            if let Some(owner) = &state.owner {
                owner.interrupt_requests();
            }
            state.endpoint.clone()
        };
        if let Some(endpoint) = endpoint {
            endpoint.interrupt_context();
        }
    }
}

impl Drop for RepositoryOrigin {
    fn drop(&mut self) {
        self.retire();
    }
}

fn same_intent(left: &RepositoryCreationIntent, right: &RepositoryCreationIntent) -> bool {
    match (left, right) {
        (RepositoryCreationIntent::FirstSet, RepositoryCreationIntent::FirstSet) => true,
        (
            RepositoryCreationIntent::Loaded { session_id: left },
            RepositoryCreationIntent::Loaded { session_id: right },
        ) => left == right,
        (
            RepositoryCreationIntent::Replace { expected: left },
            RepositoryCreationIntent::Replace { expected: right },
        ) => left == right,
        _ => false,
    }
}

pub(super) fn capture(handles: &Handles, agent: &AgentId) -> Option<Arc<RepositoryOrigin>> {
    handles
        .lock()
        .unwrap()
        .get(agent)
        .map(|handle| handle.repository_origin.clone())
}

pub(super) fn is_current(
    handles: &Handles,
    agent: &AgentId,
    original: &Arc<RepositoryOrigin>,
) -> bool {
    handles
        .lock()
        .unwrap()
        .get(agent)
        .is_some_and(|handle| Arc::ptr_eq(&handle.repository_origin, original))
}

/// Drain retirement outside the map, then remove only the originally captured
/// handle. A late callback cannot detach another incarnation with the same ID.
pub(super) fn take(
    handles: &Handles,
    agent: &AgentId,
    original: &Arc<RepositoryOrigin>,
    registry: Option<&ProcessRegistry>,
) -> Option<AgentHandle> {
    original.retire();
    let mut map = handles.lock().unwrap();
    if map
        .get(agent)
        .is_some_and(|handle| Arc::ptr_eq(&handle.repository_origin, original))
    {
        if let Some(registry) = registry {
            registry.deregister(agent);
        }
        map.remove(agent)
    } else {
        None
    }
}

#[cfg(test)]
mod tests;
