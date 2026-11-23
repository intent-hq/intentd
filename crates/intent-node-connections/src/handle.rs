//! What the head's domain holds for one machine: a connection handle.
//!
//! A held handle keeps answering its holder after the connector map forgets
//! it, so these are methods on the handle, not calls keyed by machine id.
//! The traits are split by what a caller does with a machine; a caller
//! takes only the part it uses and a test fake implements only that part.
//! [`MachineConnection`] bundles them. [`RemoteNode`] implements every
//! method by forwarding to its own.

use std::sync::Arc;
use std::time::Duration;

use intent_core::machine_calls::{MachineSecretAnswer, MachineSecretOp, MachineSkills};
use intent_core::node_link::{
    CheckoutRead, CheckoutReadReply, HomeCall, HomeCallReply, LaunchCommand, LinkFrame, LinkId,
    MachineCapabilities, OneShot, OneShotReply, PushWipResult, StopId,
};
use intent_core::placement::PlatformAvailability;
use intent_core::remote_launch::FinalStopReason;
use intent_core::{AgentId, BoxFuture, HomeId, Result, WorkspaceId};
use intent_node_link::NodeLink;
use tokio::time::Instant;

use crate::connector::{NodeStatus, RemoteNode};
use crate::link::gate::GateLink;
use crate::link::home_scripts::Stopped;
use crate::link::homes::HomeReply;
use crate::runtime::RemoteAgentRuntime;
use crate::LinkView;

/// A use of the machine in flight. While it is held, [`MachineLifecycle::quiesce`]
/// waits.
pub struct UseGuard {
    _held: Box<dyn Send + Sync>,
}

/// The machine's uses are all drained and no new one starts while this is
/// held.
pub struct QuiesceGuard {
    _held: Box<dyn Send + Sync>,
}

impl UseGuard {
    /// Wrap `held`; the use ends when the guard drops.
    pub fn new(held: impl Send + Sync + 'static) -> Self {
        Self {
            _held: Box::new(held),
        }
    }
}

impl QuiesceGuard {
    /// Wrap `held`; the machine is free again when the guard drops.
    pub fn new(held: impl Send + Sync + 'static) -> Self {
        Self {
            _held: Box::new(held),
        }
    }
}

/// The link's state and the frames over it.
pub trait MachineLink: Send + Sync {
    fn status(&self) -> NodeStatus;
    fn route(&self) -> Option<String>;
    fn protocol(&self) -> u32;
    fn current_link(&self) -> Option<NodeLink>;
    fn availability(&self) -> PlatformAvailability;
    fn offers_prepared_checkout(&self) -> bool;
    fn view(&self) -> &dyn LinkView;
    fn has_claimed(&self) -> bool;
    fn machine_id(&self) -> String;
    /// Connect now if no link is up, and wait for the bind.
    fn connect(&self) -> BoxFuture<'_, Result<()>>;
    fn reclaim(&self) -> BoxFuture<'_, bool>;
    fn send<'a>(&'a self, frame: &'a LinkFrame) -> BoxFuture<'a, Result<()>>;
    fn shutdown(&self);
    /// Mark one use of the machine; its link is not quiesced while held.
    fn begin_use(&self) -> BoxFuture<'_, UseGuard>;
}

/// The machine's agents, their stops and the calls made to the machine.
pub trait MachineAgents: Send + Sync {
    fn launch(&self, command: LaunchCommand) -> BoxFuture<'_, Result<Arc<RemoteAgentRuntime>>>;
    fn launching(&self, id: &AgentId);
    fn reattaches(&self, id: &AgentId) -> bool;
    fn live_agents(&self) -> usize;
    fn running(&self) -> usize;
    fn turns_in_flight(&self) -> usize;
    fn final_stop<'a>(&'a self, id: &'a AgentId, reason: FinalStopReason) -> BoxFuture<'a, StopId>;
    fn await_turn_before_final_stop<'a>(
        &'a self,
        id: &'a AgentId,
        bound: Duration,
    ) -> BoxFuture<'a, bool>;
    fn stop_all(&self) -> BoxFuture<'_, Vec<AgentId>>;
    fn wait_stops<'a>(
        &'a self,
        ids: &'a [AgentId],
        deadline: Instant,
    ) -> BoxFuture<'a, Vec<AgentId>>;
    fn end_agents_machine_runs<'a>(
        &'a self,
        ids: &'a [AgentId],
        deadline: Instant,
        reason: &'a str,
    ) -> BoxFuture<'a, Vec<AgentId>>;
    fn processes_lost(&self, ids: &[AgentId], error: &str);
    fn setup_output_tail(&self, id: &AgentId) -> Option<(String, bool)>;
    fn remove_budget(&self) -> Duration;
    fn machine_skills(&self, query: bool) -> BoxFuture<'_, MachineSkills>;
    fn refresh_capabilities(&self) -> BoxFuture<'_, Result<MachineCapabilities>>;
    fn machine_secret<'a>(
        &'a self,
        id: &'a AgentId,
        op: MachineSecretOp,
        wait: Duration,
    ) -> BoxFuture<'a, MachineSecretAnswer>;
    fn push_wip<'a>(
        &'a self,
        id: &'a AgentId,
        wait: Duration,
    ) -> BoxFuture<'a, Result<PushWipResult>>;
    fn checkout_read(
        &self,
        read: CheckoutRead,
        wait: Duration,
    ) -> BoxFuture<'_, Result<CheckoutReadReply>>;
    fn one_shot(&self, frame: OneShot, wait: Duration) -> BoxFuture<'_, Result<OneShotReply>>;
}

/// The machine's homes, and the terminals and scripts that run in them.
pub trait MachineHomes: Send + Sync {
    fn home_call(&self, frame: HomeCall, wait: Duration) -> BoxFuture<'_, Result<HomeCallReply>>;
    fn home_request(
        &self,
        request_id: LinkId,
        frame: LinkFrame,
        wait: Duration,
    ) -> BoxFuture<'_, Result<HomeReply>>;
    fn home_link_lost(&self);
    /// # Errors
    ///
    /// When the machine is not connected or the terminal cannot start.
    fn open_home_terminal(
        &self,
        home: &intent_core::WorkspaceHome,
        workspace_id: &WorkspaceId,
        pty: &Arc<intent_pty::PtyHost>,
        open: intent_pty::home::HomeTerminalOpen,
    ) -> Result<intent_pty::PtyId>;
    fn open_home_exec(
        &self,
        home: &intent_core::WorkspaceHome,
        workspace_id: &WorkspaceId,
        open: intent_pty::home::HomeExecOpen,
    ) -> intent_pty::home::HomeExec;
    fn end_home_terminals<'a>(&'a self, home_id: &'a HomeId) -> BoxFuture<'a, ()>;
    /// # Errors
    ///
    /// When the machine is not connected or the run cannot start.
    fn start_home_script(
        &self,
        home: &intent_core::WorkspaceHome,
        workspace_id: &WorkspaceId,
        pty: &Arc<intent_pty::PtyHost>,
        open: intent_pty::home::HomeScriptOpen,
    ) -> Result<Option<intent_pty::PtyId>>;
    fn stop_home_script<'a>(
        &'a self,
        pty: &'a Arc<intent_pty::PtyHost>,
        id: intent_pty::PtyId,
        wait: Duration,
    ) -> BoxFuture<'a, Stopped>;
}

/// What the idle sweep and the cloud lifecycle ask of a connection.
pub trait MachineLifecycle: Send + Sync {
    fn close_link(&self);
    fn link_wanted(&self) -> bool;
    fn wake(&self);
    fn busy(&self) -> bool;
    fn last_activity(&self) -> Instant;
    /// Wait up to `wait` for every use to end, then hold new ones off.
    fn quiesce(&self, wait: Duration) -> BoxFuture<'_, Option<QuiesceGuard>>;
}

/// One machine's connection, everything the domain does with it.
pub trait MachineConnection:
    MachineLink + MachineAgents + MachineHomes + MachineLifecycle + GateLink
{
}

impl<T: MachineLink + MachineAgents + MachineHomes + MachineLifecycle + GateLink> MachineConnection
    for T
{
}

impl MachineLink for RemoteNode {
    fn status(&self) -> NodeStatus {
        RemoteNode::status(self)
    }
    fn route(&self) -> Option<String> {
        RemoteNode::route(self)
    }
    fn protocol(&self) -> u32 {
        RemoteNode::protocol(self)
    }
    fn current_link(&self) -> Option<NodeLink> {
        RemoteNode::current_link(self)
    }
    fn availability(&self) -> PlatformAvailability {
        RemoteNode::availability(self)
    }
    fn offers_prepared_checkout(&self) -> bool {
        RemoteNode::offers_prepared_checkout(self)
    }
    fn view(&self) -> &dyn LinkView {
        RemoteNode::view(self)
    }
    fn has_claimed(&self) -> bool {
        RemoteNode::has_claimed(self)
    }
    fn machine_id(&self) -> String {
        self.shared_ref().machine_id()
    }
    fn connect(&self) -> BoxFuture<'_, Result<()>> {
        Box::pin(RemoteNode::connect(self))
    }
    fn reclaim(&self) -> BoxFuture<'_, bool> {
        Box::pin(RemoteNode::reclaim(self))
    }
    fn send<'a>(&'a self, frame: &'a LinkFrame) -> BoxFuture<'a, Result<()>> {
        Box::pin(RemoteNode::send(self, frame))
    }
    fn shutdown(&self) {
        RemoteNode::shutdown(self);
    }
    fn begin_use(&self) -> BoxFuture<'_, UseGuard> {
        Box::pin(async move { UseGuard::new(RemoteNode::begin_use(self).await) })
    }
}

impl MachineAgents for RemoteNode {
    fn launch(&self, command: LaunchCommand) -> BoxFuture<'_, Result<Arc<RemoteAgentRuntime>>> {
        Box::pin(RemoteNode::launch(self, command))
    }
    fn launching(&self, id: &AgentId) {
        RemoteNode::launching(self, id);
    }
    fn reattaches(&self, id: &AgentId) -> bool {
        RemoteNode::reattaches(self, id)
    }
    fn live_agents(&self) -> usize {
        RemoteNode::live_agents(self)
    }
    fn running(&self) -> usize {
        RemoteNode::running(self)
    }
    fn turns_in_flight(&self) -> usize {
        RemoteNode::turns_in_flight(self)
    }
    fn final_stop<'a>(&'a self, id: &'a AgentId, reason: FinalStopReason) -> BoxFuture<'a, StopId> {
        Box::pin(RemoteNode::final_stop(self, id, reason))
    }
    fn await_turn_before_final_stop<'a>(
        &'a self,
        id: &'a AgentId,
        bound: Duration,
    ) -> BoxFuture<'a, bool> {
        Box::pin(RemoteNode::await_turn_before_final_stop(self, id, bound))
    }
    fn stop_all(&self) -> BoxFuture<'_, Vec<AgentId>> {
        Box::pin(RemoteNode::stop_all(self))
    }
    fn wait_stops<'a>(
        &'a self,
        ids: &'a [AgentId],
        deadline: Instant,
    ) -> BoxFuture<'a, Vec<AgentId>> {
        Box::pin(RemoteNode::wait_stops(self, ids, deadline))
    }
    fn end_agents_machine_runs<'a>(
        &'a self,
        ids: &'a [AgentId],
        deadline: Instant,
        reason: &'a str,
    ) -> BoxFuture<'a, Vec<AgentId>> {
        Box::pin(RemoteNode::end_agents_machine_runs(
            self, ids, deadline, reason,
        ))
    }
    fn processes_lost(&self, ids: &[AgentId], error: &str) {
        RemoteNode::processes_lost(self, ids, error);
    }
    fn setup_output_tail(&self, id: &AgentId) -> Option<(String, bool)> {
        RemoteNode::setup_output_tail(self, id)
    }
    fn remove_budget(&self) -> Duration {
        RemoteNode::remove_budget(self)
    }
    fn machine_skills(&self, query: bool) -> BoxFuture<'_, MachineSkills> {
        Box::pin(RemoteNode::machine_skills(self, query))
    }
    fn refresh_capabilities(&self) -> BoxFuture<'_, Result<MachineCapabilities>> {
        Box::pin(RemoteNode::refresh_capabilities(self))
    }
    fn machine_secret<'a>(
        &'a self,
        id: &'a AgentId,
        op: MachineSecretOp,
        wait: Duration,
    ) -> BoxFuture<'a, MachineSecretAnswer> {
        Box::pin(RemoteNode::machine_secret(self, id, op, wait))
    }
    fn push_wip<'a>(
        &'a self,
        id: &'a AgentId,
        wait: Duration,
    ) -> BoxFuture<'a, Result<PushWipResult>> {
        Box::pin(RemoteNode::push_wip(self, id, wait))
    }
    fn checkout_read(
        &self,
        read: CheckoutRead,
        wait: Duration,
    ) -> BoxFuture<'_, Result<CheckoutReadReply>> {
        Box::pin(RemoteNode::checkout_read(self, read, wait))
    }
    fn one_shot(&self, frame: OneShot, wait: Duration) -> BoxFuture<'_, Result<OneShotReply>> {
        Box::pin(RemoteNode::one_shot(self, frame, wait))
    }
}

impl MachineHomes for RemoteNode {
    fn home_call(&self, frame: HomeCall, wait: Duration) -> BoxFuture<'_, Result<HomeCallReply>> {
        Box::pin(RemoteNode::home_call(self, frame, wait))
    }
    fn home_request(
        &self,
        request_id: LinkId,
        frame: LinkFrame,
        wait: Duration,
    ) -> BoxFuture<'_, Result<HomeReply>> {
        Box::pin(RemoteNode::home_request(self, request_id, frame, wait))
    }
    fn home_link_lost(&self) {
        RemoteNode::home_link_lost(self);
    }
    fn open_home_terminal(
        &self,
        home: &intent_core::WorkspaceHome,
        workspace_id: &WorkspaceId,
        pty: &Arc<intent_pty::PtyHost>,
        open: intent_pty::home::HomeTerminalOpen,
    ) -> Result<intent_pty::PtyId> {
        RemoteNode::open_home_terminal(self, home, workspace_id, pty, open)
    }
    fn open_home_exec(
        &self,
        home: &intent_core::WorkspaceHome,
        workspace_id: &WorkspaceId,
        open: intent_pty::home::HomeExecOpen,
    ) -> intent_pty::home::HomeExec {
        RemoteNode::open_home_exec(self, home, workspace_id, open)
    }
    fn end_home_terminals<'a>(&'a self, home_id: &'a HomeId) -> BoxFuture<'a, ()> {
        Box::pin(RemoteNode::end_home_terminals(self, home_id))
    }
    fn start_home_script(
        &self,
        home: &intent_core::WorkspaceHome,
        workspace_id: &WorkspaceId,
        pty: &Arc<intent_pty::PtyHost>,
        open: intent_pty::home::HomeScriptOpen,
    ) -> Result<Option<intent_pty::PtyId>> {
        RemoteNode::start_home_script(self, home, workspace_id, pty, open)
    }
    fn stop_home_script<'a>(
        &'a self,
        pty: &'a Arc<intent_pty::PtyHost>,
        id: intent_pty::PtyId,
        wait: Duration,
    ) -> BoxFuture<'a, Stopped> {
        Box::pin(RemoteNode::stop_home_script(self, pty, id, wait))
    }
}

impl MachineLifecycle for RemoteNode {
    fn close_link(&self) {
        RemoteNode::close_link(self);
    }
    fn link_wanted(&self) -> bool {
        RemoteNode::link_wanted(self)
    }
    fn wake(&self) {
        RemoteNode::wake(self);
    }
    fn busy(&self) -> bool {
        RemoteNode::busy(self)
    }
    fn last_activity(&self) -> Instant {
        RemoteNode::last_activity(self)
    }
    fn quiesce(&self, wait: Duration) -> BoxFuture<'_, Option<QuiesceGuard>> {
        Box::pin(async move { RemoteNode::quiesce(self, wait).await.map(QuiesceGuard::new) })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn is_connection<T: MachineConnection + ?Sized>() {}

    #[test]
    fn remote_node_is_a_machine_connection_and_the_trait_is_object_safe() {
        is_connection::<RemoteNode>();
        is_connection::<dyn MachineConnection>();
        let _: Option<Arc<dyn MachineConnection>> = None;
    }
}
