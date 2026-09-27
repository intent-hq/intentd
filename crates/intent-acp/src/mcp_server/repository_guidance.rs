//! Optional repository-guidance transport metadata. No context discovery,
//! instruction rendering or authorization happens here. The service producer
//! must admit the caller, refresh context and use the versioned renderer.

use std::cmp::Ordering;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use intent_core::repository_context::{ExecutionScope, RepositoryContextRevision};
use intent_core::{AgentId, AgentSession, Caller, WorkspaceId};
use serde_json::{json, Value};

use super::request_context::CapturedRequestContext;

const MAX_GUIDANCE_BYTES: usize = 8 * 1024;
const PREPARE_TIMEOUT: Duration = Duration::from_secs(1);

/// Trusted service adapter, absent unless explicitly wired for a stamped session.
/// Implementations must authorize the bound caller and workspace, including
/// session retirement, before reading context. IDs and transport leases are not
/// grants. Use the accepted renderer; never derive instructions from raw results.
pub trait RepositoryGuidanceSource: Send + Sync {
    /// Runs after tool-result shaping within the original caller scope. Share
    /// the supplied fence across calls; only authoritative context transitions
    /// may replace its lease. A late operation result must not replace a lease.
    fn prepare<'a>(
        &'a self,
        workspace_id: &'a WorkspaceId,
        caller: &'a Caller,
        fence: &'a RepositoryGuidanceFence,
    ) -> Pin<Box<dyn Future<Output = Option<GuidanceCandidate>> + Send + 'a>>;
}

#[derive(Clone)]
struct Stamp {
    scope: ExecutionScope,
    revision: RepositoryContextRevision,
}

impl Stamp {
    fn compare(&self, other: &Self) -> Option<Ordering> {
        self.revision
            .compare_in_scopes(&self.scope, &other.revision, &other.scope)
    }
}

struct ActiveContext {
    generation: Arc<()>,
    stamp: Option<Stamp>,
}

/// Session-wide output fence shared by every connection of one bound server.
/// It retains correlation/freshness only and never grants read or write access.
#[derive(Clone, Default)]
pub struct RepositoryGuidanceFence {
    active: Arc<Mutex<Option<ActiveContext>>>,
}

impl RepositoryGuidanceFence {
    /// Install a newly admitted scope/epoch. This is an explicit producer
    /// transition, never inferred from a completed response. Prior leases stay
    /// retired even when their late response carries a larger sequence number.
    /// Returns `None` if the fence has been poisoned.
    #[must_use]
    pub fn replace_context(
        &self,
        scope: ExecutionScope,
        revision: RepositoryContextRevision,
    ) -> Option<GuidanceLease> {
        self.replace(Some(Stamp { scope, revision }))
    }

    /// Explicitly invalidate old targets after denial or a failed context read.
    /// The new lease accepts only the renderer's target-free unavailable text.
    #[must_use]
    pub fn unavailable_context(&self) -> Option<GuidanceLease> {
        self.replace(None)
    }

    fn replace(&self, stamp: Option<Stamp>) -> Option<GuidanceLease> {
        let mut state = self.active.lock().ok()?;
        if let (Some(next), Some(current)) = (&stamp, state.as_ref().and_then(|s| s.stamp.as_ref()))
        {
            if next.compare(current) == Some(Ordering::Less) {
                return None;
            }
        }
        let generation = Arc::new(());
        *state = Some(ActiveContext {
            generation: generation.clone(),
            stamp,
        });
        Some(GuidanceLease {
            fence: self.clone(),
            generation,
        })
    }

    /// Retire all pending guidance, for example when the admitted session ends.
    pub fn retire(&self) {
        if let Ok(mut state) = self.active.lock() {
            *state = None;
        }
    }
}

/// A producer-held context lifetime, separate from any TCP connection lifetime.
/// Replacing the active scope makes all old copies permanently unusable.
#[derive(Clone)]
pub struct GuidanceLease {
    fence: RepositoryGuidanceFence,
    generation: Arc<()>,
}

impl GuidanceLease {
    /// Publish a freshly observed revision only within this admitted scope and
    /// epoch. Replacement requires the fence's explicit context transition.
    /// Older or incomparable revisions and retired leases are refused.
    #[must_use]
    pub fn advance(&self, scope: ExecutionScope, revision: RepositoryContextRevision) -> bool {
        let Ok(mut state) = self.fence.active.lock() else {
            return false;
        };
        let Some(active) = state.as_mut() else {
            return false;
        };
        if !Arc::ptr_eq(&active.generation, &self.generation) {
            return false;
        }
        let next = Stamp { scope, revision };
        if !active.stamp.as_ref().is_some_and(|current| {
            matches!(
                next.compare(current),
                Some(Ordering::Equal | Ordering::Greater)
            )
        }) {
            return false;
        }
        active.stamp = Some(next);
        true
    }

    /// Carry accepted renderer output without parsing its text or its facts.
    /// The writer checks the typed stamp again; preparing a candidate does not
    /// publish it, advance the fence or authorize any operation.
    #[must_use]
    pub fn current_candidate(
        &self,
        scope: ExecutionScope,
        revision: RepositoryContextRevision,
        text: String,
    ) -> Option<GuidanceCandidate> {
        self.candidate(Some(Stamp { scope, revision }), text)
    }

    /// Carry the accepted renderer's explicit unavailable guidance. A current
    /// target-bearing lease cannot emit this unstamped candidate, or vice versa.
    #[must_use]
    pub fn unavailable_candidate(&self, text: String) -> Option<GuidanceCandidate> {
        self.candidate(None, text)
    }

    fn candidate(&self, stamp: Option<Stamp>, text: String) -> Option<GuidanceCandidate> {
        (!text.is_empty() && text.len() <= MAX_GUIDANCE_BYTES).then(|| GuidanceCandidate {
            lease: self.clone(),
            stamp,
            text,
        })
    }
}

/// Not serializable: only the bounded rendered text may enter an MCP response.
pub struct GuidanceCandidate {
    lease: GuidanceLease,
    stamp: Option<Stamp>,
    text: String,
}

impl GuidanceCandidate {
    fn belongs_to(&self, fence: &RepositoryGuidanceFence) -> bool {
        Arc::ptr_eq(&self.lease.fence.active, &fence.active)
    }

    fn serialize(self, mut response: Value, connection: &ConnectionToken) -> String {
        // No await between the last check and serialization. This is the
        // writer's enqueue boundary; already-written socket bytes cannot be
        // retracted. Producer transitions serialize with this final check.
        if let Ok(state) = self.lease.fence.active.lock() {
            let current = state.as_ref().is_some_and(|active| {
                Arc::ptr_eq(&active.generation, &self.lease.generation)
                    && match (&self.stamp, &active.stamp) {
                        (Some(candidate), Some(current)) => {
                            candidate.compare(current) == Some(Ordering::Equal)
                        }
                        (None, None) => true,
                        _ => false,
                    }
            });
            if current && connection.is_live() {
                if let Some(items) = response
                    .pointer_mut("/result/content")
                    .and_then(Value::as_array_mut)
                {
                    items.push(json!({"type":"text","text":self.text}));
                }
            }
            return format!("{response}\n");
        }
        // A poisoned fence may suppress guidance, never the operation result.
        format!("{response}\n")
    }
}

pub(super) struct GuidanceBinding {
    agent_id: AgentId,
    source: Arc<dyn RepositoryGuidanceSource>,
    fence: RepositoryGuidanceFence,
}

impl GuidanceBinding {
    pub(super) fn new(
        session: &AgentSession,
        workspace_id: &WorkspaceId,
        caller_id: Option<&AgentId>,
        source: Arc<dyn RepositoryGuidanceSource>,
    ) -> Option<Self> {
        (session.harness_version == "3.0"
            && session.retired_at.is_none()
            && session.workspace_id == *workspace_id
            && caller_id == Some(&session.id))
        .then(|| Self {
            agent_id: session.id.clone(),
            source,
            fence: RepositoryGuidanceFence::default(),
        })
    }

    pub(super) fn capture(
        &self,
        workspace_id: &WorkspaceId,
        caller_id: Option<&AgentId>,
    ) -> Option<GuidanceRequest> {
        let caller = intent_core::current_caller()?;
        if caller_id != Some(&self.agent_id)
            || caller
                != (Caller::Agent {
                    agent_id: self.agent_id.clone(),
                })
        {
            return None;
        }
        Some(GuidanceRequest {
            source: self.source.clone(),
            workspace_id: workspace_id.clone(),
            context: CapturedRequestContext::capture(Some(caller.clone()), None),
            caller,
            fence: self.fence.clone(),
        })
    }
}

/// Captured under the original caller, but not started until the bridge owns
/// the completed operation result outside the operation watchdog.
pub(crate) struct GuidanceRequest {
    source: Arc<dyn RepositoryGuidanceSource>,
    workspace_id: WorkspaceId,
    caller: Caller,
    fence: RepositoryGuidanceFence,
    context: CapturedRequestContext,
}

impl GuidanceRequest {
    pub(super) fn set_context(&mut self, context: CapturedRequestContext) {
        self.context = context;
    }

    async fn prepare(self) -> Option<GuidanceCandidate> {
        let Self {
            source,
            workspace_id,
            caller,
            fence,
            context,
        } = self;
        let source_fence = fence.clone();
        // Optional guidance must not turn a completed operation into an error
        // or an unbounded wait. A separate task also contains producer panics;
        // the captured caller is explicitly retained across that task boundary.
        let mut task = PendingGuidance(tokio::spawn(async move {
            context
                .run(async { source.prepare(&workspace_id, &caller, &source_fence).await })
                .await
        }));
        tokio::time::timeout(PREPARE_TIMEOUT, &mut task.0)
            .await
            .ok()?
            .ok()?
            .filter(|candidate| candidate.belongs_to(&fence))
    }
}

struct PendingGuidance(tokio::task::JoinHandle<Option<GuidanceCandidate>>);

impl Drop for PendingGuidance {
    fn drop(&mut self) {
        self.0.abort();
    }
}

pub(crate) struct BridgeResponse {
    pub(crate) value: Value,
    pub(crate) guidance: Option<GuidanceCandidate>,
    pub(crate) guidance_request: Option<GuidanceRequest>,
}

impl BridgeResponse {
    pub(crate) fn plain(value: Value) -> Self {
        Self {
            value,
            guidance: None,
            guidance_request: None,
        }
    }

    /// The operation has already won its watchdog. Resolve optional guidance
    /// per request so it cannot block the connection's serial response writer.
    pub(crate) async fn prepare_guidance(mut self) -> Self {
        if let Some(request) = self.guidance_request.take() {
            self.guidance = request.prepare().await;
        }
        self
    }

    pub(crate) fn into_line(self, connection: &ConnectionToken) -> String {
        match self.guidance {
            Some(guidance) => guidance.serialize(self.value, connection),
            None => format!("{}\n", self.value),
        }
    }
}

#[derive(Clone)]
pub(crate) struct ConnectionToken {
    live: Arc<AtomicBool>,
    endpoint: Option<Arc<AtomicBool>>,
}

impl ConnectionToken {
    fn is_live(&self) -> bool {
        self.live.load(AtomicOrdering::Acquire)
            && self
                .endpoint
                .as_ref()
                .is_none_or(|endpoint| endpoint.load(AtomicOrdering::Acquire))
    }

    pub(crate) fn retire(&self) {
        self.live.store(false, AtomicOrdering::Release);
    }
}

pub(crate) struct ConnectionLifetime(ConnectionToken);

impl ConnectionLifetime {
    pub(crate) fn new() -> Self {
        Self(ConnectionToken {
            live: Arc::new(AtomicBool::new(true)),
            endpoint: None,
        })
    }

    /// Each accepted connection also belongs to its bridge endpoint. Retiring
    /// either lifetime suppresses guidance without changing operation results.
    pub(crate) fn for_endpoint(endpoint: &ConnectionToken) -> Self {
        Self(ConnectionToken {
            live: Arc::new(AtomicBool::new(true)),
            endpoint: Some(endpoint.live.clone()),
        })
    }

    pub(crate) fn token(&self) -> ConnectionToken {
        self.0.clone()
    }

    pub(crate) fn retire(&self) {
        self.0.retire();
    }
}

impl Drop for ConnectionLifetime {
    fn drop(&mut self) {
        self.retire();
    }
}

#[cfg(test)]
pub(crate) mod tests;
