//! What the node-connection service asks of the head's domain: the calls a
//! machine link makes back into the head (forwarded `workspace_api`
//! requests, home events, machine script frames, journal projections and
//! change notices). The domain implements [`NodeInbound`] once; the service
//! never names the domain's types.

use std::sync::Arc;

use intent_core::node_link::{HomeEvents, HomeRef, LeaseBinding, LinkFrame};
use intent_core::{AgentId, BoxFuture, WorkspaceId};
use intent_node_link::IncomingRequest;
use tokio::time::Instant;

/// What the domain may read about one machine's link while it answers.
pub trait LinkView: Send + Sync {
    /// The exposed machine this link reaches.
    fn machine_id(&self) -> String;

    /// Whether a link is up now.
    fn is_connected(&self) -> bool;

    /// How many of this backend's agents run on the machine now.
    fn live_agents(&self) -> usize;

    /// The protocol the current (or last) link negotiated.
    fn protocol(&self) -> u32;

    /// The binding of the link that is up now, if any.
    fn current_binding(&self) -> Option<LeaseBinding>;
}

/// The head's domain, as the node-connection service calls it.
pub trait NodeInbound: Send + Sync + 'static {
    /// Answer a forwarded request that arrived on `link`, on its own task.
    fn link_request(&self, link: Arc<dyn LinkView>, request: IncomingRequest);

    /// The machine's home events, in order.
    fn home_events<'a>(&'a self, machine_id: &'a str, events: HomeEvents) -> BoxFuture<'a, ()>;

    /// A script-channel frame of one of the machine's agent runs.
    fn machine_script_frame<'a>(
        &'a self,
        machine_id: &'a str,
        frame: LinkFrame,
    ) -> BoxFuture<'a, ()>;

    /// Whether one of the machine's script runs is live.
    fn machine_runs_live(&self, machine_id: &str) -> bool;

    /// End `agents`' machine runs by `deadline`; the agents whose script
    /// stop was not acknowledged.
    fn end_agents_machine_runs<'a>(
        &'a self,
        agents: &'a [AgentId],
        deadline: Instant,
        reason: &'a str,
    ) -> BoxFuture<'a, Vec<AgentId>>;

    /// Every live script run on the machine is lost with `reason`.
    fn machine_runs_lost<'a>(&'a self, machine_id: &'a str, reason: &'a str) -> BoxFuture<'a, ()>;

    /// A final stop began for `agent_id`: none of its runs may start.
    fn final_stop_begun(&self, agent_id: &AgentId);

    /// `agent_id`'s final stop settled: its runs end with `reason`.
    fn final_stop_settled<'a>(
        &'a self,
        agent_id: &'a AgentId,
        reason: &'a str,
    ) -> BoxFuture<'a, ()>;

    /// The machine behind `link` changed: announce it as it is now.
    fn machine_changed<'a>(&'a self, link: &'a dyn LinkView) -> BoxFuture<'a, ()>;

    /// Checkout and setup output of a home's open.
    fn home_setup_output<'a>(
        &'a self,
        home: &'a HomeRef,
        seq: u64,
        data: &'a str,
    ) -> BoxFuture<'a, ()>;

    /// An agent's projected placement changed.
    fn agent_placement_changed<'a>(
        &'a self,
        workspace_id: &'a WorkspaceId,
        agent_id: &'a AgentId,
    ) -> BoxFuture<'a, ()>;

    /// The skills a machine reported for an agent changed.
    fn agent_skills_changed<'a>(
        &'a self,
        workspace_id: &'a WorkspaceId,
        agent_id: &'a AgentId,
    ) -> BoxFuture<'a, ()>;

    /// The machine now runs as an orchestrator and refused `refused_token`.
    fn machine_became_orchestrator(&self, machine_id: String, refused_token: String);

    /// The in-process implementation, for domain code that has not moved
    /// behind these calls yet.
    fn as_any(&self) -> &dyn std::any::Any;
}
