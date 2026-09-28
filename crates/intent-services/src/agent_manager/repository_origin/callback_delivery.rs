//! Optional delivery on one confirmed physical owner and its original Query.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};

use intent_acp::callback_registration::{
    CallbackDeliveryOutcome, CallbackQuery, CallbackStatus, CallbackStdioServer, CallbackTool,
};
use intent_acp::{
    serve_workspace_mcp_tcp, Connection, McpBridge, NormalizedMcpServer, WorkspaceMcpServer,
};
use intent_core::AgentSession;
use tokio::sync::Notify;

use crate::repository_admission::lifecycle::physical_owner::RepositoryPhysicalRetirement;
use crate::repository_admission::read_request::RepositoryReadOwner;
use crate::repository_admission::request_context::RepositoryCallbackContext;
use crate::repository_admission::{AdmissionError, AdmissionResult};
use crate::repository_context_live::RepositoryContextOwner;
use crate::repository_context_output::RepositoryPromptContext;
use crate::Services;

use super::{RepositoryOrigin, SessionAttempt};

#[cfg(test)]
type ContextDecorator = Arc<
    dyn Fn(
            Arc<dyn intent_acp::mcp_server::request_context::McpRequestContext>,
        ) -> Arc<dyn intent_acp::mcp_server::request_context::McpRequestContext>
        + Send
        + Sync,
>;

/// Captured once from the original server inputs, before exposing its pending endpoint.
#[derive(Clone)]
pub(in crate::agent_manager) struct ServerBlueprint {
    build: Arc<dyn Fn() -> WorkspaceMcpServer + Send + Sync>,
    read_owner: AdmissionResult<Arc<RepositoryReadOwner>>,
    original: Option<(Arc<Services>, AgentSession)>,
    #[cfg(test)]
    context_decorator: Option<ContextDecorator>,
}

impl ServerBlueprint {
    pub(in crate::agent_manager) fn new(
        read_owner: AdmissionResult<Arc<RepositoryReadOwner>>,
        build: impl Fn() -> WorkspaceMcpServer + Send + Sync + 'static,
    ) -> Self {
        Self {
            build: Arc::new(build),
            read_owner,
            original: None,
            #[cfg(test)]
            context_decorator: None,
        }
    }

    pub(in crate::agent_manager) fn with_original_services(
        mut self,
        services: Arc<Services>,
        session: AgentSession,
    ) -> Self {
        self.original = Some((services, session));
        self
    }

    pub(in crate::agent_manager) fn server(&self) -> WorkspaceMcpServer {
        (self.build)()
    }

    fn confirmed_server(
        &self,
        context: RepositoryCallbackContext,
        live: Option<&Arc<LiveContext>>,
    ) -> WorkspaceMcpServer {
        // Optional binding failure never repairs the retained required anchor.
        let (server, context): (
            _,
            Arc<dyn intent_acp::mcp_server::request_context::McpRequestContext>,
        ) = match live.and_then(|live| live.owner.as_ref().ok()) {
            Some(owner) => (
                self.server().with_repository_guidance(
                    &self.original.as_ref().expect("bound original session").1,
                    owner.guidance_source(),
                ),
                owner.mcp_context(),
            ),
            None => (
                self.server(),
                Arc::new(context.with_read_owner(self.read_owner.clone())),
            ),
        };
        #[cfg(test)]
        let context = self
            .context_decorator
            .as_ref()
            .map_or_else(|| context.clone(), |decorate| decorate(context.clone()));
        server.with_request_context(context)
    }
}

/// Only the original reserved workspace command is copied. User sets stay in the Query.
pub(in crate::agent_manager) struct EndpointBlueprint {
    server: ServerBlueprint,
    command: String,
    args_before_address: Vec<String>,
    env: BTreeMap<String, String>,
}

impl EndpointBlueprint {
    pub(in crate::agent_manager) fn from_original(
        server: ServerBlueprint,
        original: &NormalizedMcpServer,
        address: &str,
    ) -> Option<Self> {
        let NormalizedMcpServer::Stdio { command, args, env } = original else {
            return None;
        };
        if args != &["mcp-bridge", "--connect", address] {
            return None;
        }
        Some(Self {
            server,
            command: command.clone(),
            args_before_address: args[..2].to_vec(),
            env: env.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
        })
    }

    pub(super) fn bind_context(
        &self,
        connection: &Arc<Connection>,
        physical: &crate::repository_admission::lifecycle::physical_owner::RepositoryPhysicalOwner,
        workspace: &intent_core::WorkspaceId,
        agent: &intent_core::AgentId,
    ) -> Option<Arc<LiveContext>> {
        let (services, session) = self.server.original.as_ref()?;
        if session.harness_version != "3.0"
            || session.retired_at.is_some()
            || &session.workspace_id != workspace
            || &session.id != agent
        {
            return None;
        }
        Some(Arc::new(LiveContext {
            connection: Arc::downgrade(connection),
            owner: RepositoryContextOwner::bind(
                services.clone(),
                self.server.read_owner.clone(),
                physical,
            ),
            acknowledged: AtomicBool::new(false),
            retired: AtomicBool::new(false),
        }))
    }

    fn registration(&self, address: String) -> intent_acp::AcpResult<CallbackStdioServer> {
        let mut args = self.args_before_address.clone();
        args.push(address);
        CallbackStdioServer::new(
            self.command.clone(),
            args,
            Some(self.env.clone()),
            [CallbackTool::WorkspaceApi],
        )
    }
}

/// Retains the first binding result and its original transport, never a lookup key.
pub(super) struct LiveContext {
    connection: Weak<Connection>,
    owner: AdmissionResult<Arc<RepositoryContextOwner>>,
    acknowledged: AtomicBool,
    retired: AtomicBool,
}

impl LiveContext {
    fn capture(&self) -> AdmissionResult<RepositoryPromptContext> {
        if !self.acknowledged.load(Ordering::SeqCst) || self.retired.load(Ordering::SeqCst) {
            return Err(AdmissionError::Unavailable);
        }
        self.owner
            .as_ref()
            .map_err(|error| *error)?
            .capture_manager_prompt()
    }

    fn invalidate_and_drain(&self) {
        if let Ok(owner) = &self.owner {
            owner.invalidate();
            if let Ok(runtime) = tokio::runtime::Handle::try_current() {
                let owner = owner.clone();
                // This cleanup owns the real jobs independently of a canceled
                // waiter, child termination, or an ordinary output receipt.
                super::super::spawn_owned_cleanup(&runtime, async move {
                    owner.drain_jobs().await;
                });
            }
        }
    }

    fn retire(&self) {
        if !self.retired.swap(true, Ordering::SeqCst) {
            self.invalidate_and_drain();
        }
    }
}

/// One genuine prompt capture. A retry captures again on the SAME binding
/// before backoff; the nonclone request and prepared admission are never reused.
pub(crate) struct RepositoryPromptInput {
    binding: Arc<LiveContext>,
    captured: Option<AdmissionResult<RepositoryPromptContext>>,
}
impl RepositoryPromptInput {
    pub(crate) async fn prepare(&mut self) -> Option<intent_acp::session::PromptGuidance> {
        let original = self.captured.take()?.ok()?;
        tokio::time::timeout(std::time::Duration::from_secs(1), original.prepare())
            .await
            .ok()?
    }

    pub(crate) fn recapture(&mut self) {
        self.captured = Some(self.binding.capture());
    }
}

/// Retirement always joins the original R fence before a bridge is removed or aborted.
/// Accepted TCP sockets may still finish ordinary responses after listener teardown.
pub(super) struct Endpoint {
    retirement: RepositoryPhysicalRetirement,
    cancelled: AtomicBool,
    changed: Notify,
    bridge: Mutex<Option<McpBridge>>,
    live: Option<Arc<LiveContext>>,
}

impl Endpoint {
    fn new(retirement: RepositoryPhysicalRetirement, live: Option<Arc<LiveContext>>) -> Self {
        Self {
            retirement,
            cancelled: AtomicBool::new(false),
            changed: Notify::new(),
            bridge: Mutex::new(None),
            live,
        }
    }

    pub(super) fn retire(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
        if let Some(live) = &self.live {
            live.retire();
        }
        self.retirement.retire();
        self.changed.notify_waiters();
        let bridge = self.bridge.lock().unwrap().take();
        drop(bridge);
    }

    pub(super) fn capture_prompt(
        &self,
        connection: &Arc<Connection>,
    ) -> Option<RepositoryPromptInput> {
        let live = self.live.as_ref()?;
        if !Weak::ptr_eq(&live.connection, &Arc::downgrade(connection))
            || !live.acknowledged.load(Ordering::SeqCst)
            || live.retired.load(Ordering::SeqCst)
        {
            return None;
        }
        Some(RepositoryPromptInput {
            binding: live.clone(),
            captured: Some(live.capture()),
        })
    }

    pub(super) fn interrupt_context(&self) {
        if let Some(live) = &self.live {
            live.invalidate_and_drain();
        }
    }

    fn install_bridge(&self, bridge: McpBridge) -> Option<String> {
        let mut slot = self.bridge.lock().unwrap();
        if self.cancelled.load(Ordering::SeqCst) {
            drop(slot);
            self.retirement.retire();
            drop(bridge);
            return None;
        }
        let address = bridge.connect_addr();
        *slot = Some(bridge);
        Some(address)
    }

    async fn cancelled(&self) {
        let changed = self.changed.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        if !self.cancelled.load(Ordering::SeqCst) {
            changed.await;
        }
    }
}

impl Drop for Endpoint {
    fn drop(&mut self) {
        self.retire();
    }
}

/// Never reconstructed from a session ID or the current handle map. Dropping the
/// optional work retires only the captured physical owner, including during metadata awaits.
pub(in crate::agent_manager) struct ConfirmedCallbackAttempt {
    origin: Weak<RepositoryOrigin>,
    attempt: SessionAttempt,
    context: Option<RepositoryCallbackContext>,
    retirement: RepositoryPhysicalRetirement,
    blueprint: Arc<EndpointBlueprint>,
    query: Option<CallbackQuery>,
    endpoint: Option<Arc<Endpoint>>,
    retained: bool,
    live: Option<Arc<LiveContext>>,
}

impl ConfirmedCallbackAttempt {
    pub(super) fn new(
        origin: &Arc<RepositoryOrigin>,
        attempt: SessionAttempt,
        context: RepositoryCallbackContext,
        retirement: RepositoryPhysicalRetirement,
        blueprint: Arc<EndpointBlueprint>,
        query: CallbackQuery,
        live: Option<Arc<LiveContext>>,
    ) -> Self {
        Self {
            origin: Arc::downgrade(origin),
            attempt,
            context: Some(context),
            retirement,
            blueprint,
            query: Some(query),
            endpoint: None,
            retained: false,
            live,
        }
    }

    fn current(&self) -> bool {
        self.origin
            .upgrade()
            .is_some_and(|origin| origin.current_attempt(&self.attempt))
    }

    async fn deliver(&mut self) -> Option<CallbackDeliveryOutcome> {
        if !self.current() {
            return None;
        }
        let endpoint = Arc::new(Endpoint::new(self.retirement.clone(), self.live.clone()));
        self.endpoint = Some(endpoint.clone());
        // Own the delivery slot before binding; no exposed address can escape ownership.
        if !self
            .origin
            .upgrade()
            .is_some_and(|origin| origin.attach_endpoint(&self.attempt, endpoint.clone()))
        {
            return None;
        }
        let server = self.blueprint.server.confirmed_server(
            self.context.take().expect("one original context"),
            self.live.as_ref(),
        );
        let bridge = match serve_workspace_mcp_tcp(Arc::new(server)).await {
            Ok(bridge) => bridge,
            Err(error) => {
                tracing::debug!(%error, "optional callback endpoint unavailable");
                return None;
            }
        };
        let address = endpoint.install_bridge(bridge)?;
        if !self.current() {
            return None;
        }
        let server = match self.blueprint.registration(address) {
            Ok(server) => server,
            Err(error) => {
                tracing::debug!(%error, "optional callback command unavailable");
                return None;
            }
        };
        let outcome = self
            .query
            .take()
            .expect("one original Query")
            .registration(server)
            .run(endpoint.cancelled())
            .await;
        self.retained = matches!(&outcome, CallbackDeliveryOutcome::Remote(remote)
            if remote.status == CallbackStatus::Acknowledged)
            && self.current()
            && !endpoint.cancelled.load(Ordering::SeqCst);
        if self.retained {
            if let Some(live) = &self.live {
                live.acknowledged.store(true, Ordering::SeqCst);
            }
        }
        tracing::debug!(
            retained = self.retained,
            ?outcome,
            "original callback delivery completed"
        );
        Some(outcome)
    }
}

impl Drop for ConfirmedCallbackAttempt {
    fn drop(&mut self) {
        if self.retained {
            return;
        }
        if let Some(live) = &self.live {
            live.retire();
        }
        self.retirement.retire();
        if let Some(endpoint) = &self.endpoint {
            endpoint.retire();
            if let Some(origin) = self.origin.upgrade() {
                origin.remove_endpoint(&self.attempt, endpoint);
            }
        }
    }
}

pub(in crate::agent_manager) async fn deliver_captured(
    delivery: Option<ConfirmedCallbackAttempt>,
) -> Option<RepositoryPromptInput> {
    let mut delivery = delivery?;
    // Keep the completed ordinary session result independent of registration.
    let _ = delivery.deliver().await;
    if !delivery.retained {
        return None;
    }
    let endpoint = delivery.endpoint.as_ref()?;
    let connection = delivery.live.as_ref()?.connection.upgrade()?;
    // Capture from THIS acknowledged attempt, never the origin's newer slot.
    endpoint.capture_prompt(&connection)
}

#[cfg(test)]
async fn deliver_optional(delivery: Option<ConfirmedCallbackAttempt>) {
    let _ = deliver_captured(delivery).await;
}

#[cfg(test)]
mod tests;
