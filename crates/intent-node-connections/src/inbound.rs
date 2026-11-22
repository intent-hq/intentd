//! What the node-connection service asks of the head's domain: the calls a
//! machine link makes back into the head (forwarded `workspace_api`
//! requests, home events, machine script frames, journal projections and
//! change notices). The domain implements [`NodeInbound`] once; the service
//! never names the domain's types.

use std::sync::Arc;

use intent_core::node_link::{
    HomeEvents, HomeRef, HomeStopAck, HomeStopRecord, JournalRecord, LeaseBinding, LinkFrame,
};
use intent_core::{AgentId, BoxFuture, HomeId, WorkspaceId};
use intent_git::ref_storage::GitStorage;
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

    /// The lease the link binds.
    fn lease_id(&self) -> String;

    /// Whether the machine holds a home of this backend: its link is kept
    /// up.
    fn set_holds_homes(&self, holds: bool);

    /// A backup of `home_id` arrived: its backup-warning wait ends.
    fn home_backed_up(&self, home_id: &HomeId);

    /// Send `stop` on the current link if it speaks nodeProtocol 19;
    /// otherwise it goes after the next bind.
    fn send_home_stop<'a>(&'a self, stop: &'a HomeStopRecord) -> BoxFuture<'a, ()>;

    /// Make sure the machine's pending home stops are resent on a timer
    /// while it is linked.
    fn resend_home_stops(&self);

    /// Hold `wait`, the wait for `home_id`'s backup warning, until it ends,
    /// the home backs up, or the machine's connector goes away. One wait
    /// per home: while one is pending, `wait` is dropped unstarted.
    fn watch_backup_warning(&self, home_id: &HomeId, wait: BoxFuture<'static, ()>);
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

    /// A link to the machine behind `link` bound on nodeProtocol 19: open
    /// its homes again and send its pending home stops, on a task.
    fn homes_after_bind(&self, link: Arc<dyn LinkView>);

    /// A journal record of one of the machine's homes, in order.
    fn home_record<'a>(
        &'a self,
        link: &'a dyn LinkView,
        record: JournalRecord,
    ) -> BoxFuture<'a, ()>;

    /// The machine acknowledged a home stop.
    fn home_stop_acked(&self, link: Arc<dyn LinkView>, ack: HomeStopAck) -> BoxFuture<'_, ()>;

    /// The machine behind `link` stayed unlinked for the link-loss grace,
    /// lost at `lost_at`: its homes are detached.
    fn homes_link_lost<'a>(&'a self, link: &'a dyn LinkView, lost_at: &'a str)
        -> BoxFuture<'a, ()>;

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

    /// Whether this backend serves workspace homes.
    fn homes_enabled(&self) -> bool;

    /// The backend's GitHub token for a machine's forge fetch, if it has
    /// a usable one.
    fn forge_token(&self) -> BoxFuture<'_, Option<String>>;

    /// Where the head keeps the refs machines push.
    fn git_storage(&self) -> Arc<dyn GitStorage>;
}
