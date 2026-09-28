//! Optional delivery on one confirmed physical owner and its original Query.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};

use intent_acp::callback_registration::{
    CallbackDeliveryOutcome, CallbackQuery, CallbackStatus, CallbackStdioServer, CallbackTool,
};
use intent_acp::{serve_workspace_mcp_tcp, McpBridge, NormalizedMcpServer, WorkspaceMcpServer};
use tokio::sync::Notify;

use crate::repository_admission::lifecycle::physical_owner::RepositoryPhysicalRetirement;
use crate::repository_admission::read_request::RepositoryReadOwner;
use crate::repository_admission::request_context::RepositoryCallbackContext;
use crate::repository_admission::AdmissionResult;

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
            #[cfg(test)]
            context_decorator: None,
        }
    }

    pub(in crate::agent_manager) fn server(&self) -> WorkspaceMcpServer {
        (self.build)()
    }

    fn confirmed_server(&self, context: RepositoryCallbackContext) -> WorkspaceMcpServer {
        #[cfg(test)]
        if let Some(decorate) = &self.context_decorator {
            return self.server().with_request_context(decorate(Arc::new(
                context.with_read_owner(self.read_owner.clone()),
            )));
        }
        // Keep the original success or failure; pending servers never receive
        // this anchor, and later endpoint construction cannot recapture it.
        self.server()
            .with_request_context(Arc::new(context.with_read_owner(self.read_owner.clone())))
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

/// Retirement always joins the original R fence before a bridge is removed or aborted.
/// Accepted TCP sockets may still finish ordinary responses after listener teardown.
pub(super) struct Endpoint {
    retirement: RepositoryPhysicalRetirement,
    cancelled: AtomicBool,
    changed: Notify,
    bridge: Mutex<Option<McpBridge>>,
}

impl Endpoint {
    fn new(retirement: RepositoryPhysicalRetirement) -> Self {
        Self {
            retirement,
            cancelled: AtomicBool::new(false),
            changed: Notify::new(),
            bridge: Mutex::new(None),
        }
    }

    pub(super) fn retire(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
        self.retirement.retire();
        self.changed.notify_waiters();
        let bridge = self.bridge.lock().unwrap().take();
        drop(bridge);
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
}

impl ConfirmedCallbackAttempt {
    pub(super) fn new(
        origin: &Arc<RepositoryOrigin>,
        attempt: SessionAttempt,
        context: RepositoryCallbackContext,
        retirement: RepositoryPhysicalRetirement,
        blueprint: Arc<EndpointBlueprint>,
        query: CallbackQuery,
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
        }
    }

    fn current(&self) -> bool {
        self.origin
            .upgrade()
            .is_some_and(|origin| origin.current_attempt(&self.attempt))
    }

    async fn deliver(mut self) -> Option<CallbackDeliveryOutcome> {
        if !self.current() {
            return None;
        }
        let endpoint = Arc::new(Endpoint::new(self.retirement.clone()));
        self.endpoint = Some(endpoint.clone());
        // Own the delivery slot before binding; no exposed address can escape ownership.
        if !self
            .origin
            .upgrade()
            .is_some_and(|origin| origin.attach_endpoint(&self.attempt, endpoint.clone()))
        {
            return None;
        }
        let server = self
            .blueprint
            .server
            .confirmed_server(self.context.take().expect("one original context"));
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
        self.retirement.retire();
        if let Some(endpoint) = &self.endpoint {
            endpoint.retire();
            if let Some(origin) = self.origin.upgrade() {
                origin.remove_endpoint(&self.attempt, endpoint);
            }
        }
    }
}

pub(in crate::agent_manager) async fn deliver_optional(delivery: Option<ConfirmedCallbackAttempt>) {
    if let Some(delivery) = delivery {
        // Deliberately independent of the completed session result and committed SQL effects.
        let _ = delivery.deliver().await;
    }
}

#[cfg(test)]
mod tests;
