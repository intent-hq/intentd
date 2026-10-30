//! The agent-facing egress registry shared by the egress contract tests.
//!
//! [`AGENT_EGRESS_CALLS`] lists every `workspace_api` call that serves
//! session or event data to an agent. Two contract tests drive it:
//!
//! - `tests_hidden_field_egress` (this crate) runs each call against a stub
//!   whose payloads carry every `AGENT_HIDDEN_FIELDS` key and fails if a
//!   key survives;
//! - `secret_egress` (intent-services) runs each call against the real
//!   services with a planted secret value (PROTOCOL §5.56) and fails if the
//!   value comes back.
//!
//! A new agent-facing egress that serves session or event data must be
//! appended here, so both contracts cover it.

/// Which `workspace_api` server a call runs on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EgressServer {
    /// An ordinary workspace, as an agent of that workspace.
    Workspace,
    /// The Chief-of-Staff workspace (`ws.app.*`).
    Chief,
}

/// One registered egress: a name for failure messages, the server it runs
/// on, and the JS body. `{target}` is replaced by the agent whose data is
/// read and `{workspace}` by that agent's workspace id.
#[derive(Debug, Clone, Copy)]
pub struct EgressCall {
    pub name: &'static str,
    pub server: EgressServer,
    pub code: &'static str,
}

impl EgressCall {
    /// The JS body with `{target}` and `{workspace}` filled in.
    #[must_use]
    pub fn render(&self, target: &str, workspace: &str) -> String {
        self.code
            .replace("{target}", target)
            .replace("{workspace}", workspace)
    }
}

const fn call(name: &'static str, server: EgressServer, code: &'static str) -> EgressCall {
    EgressCall { name, server, code }
}

/// Every agent-facing `workspace_api` egress that serves session or event
/// data (the wake-metadata builder is driven separately by each test).
pub const AGENT_EGRESS_CALLS: &[EgressCall] = &[
    call(
        "ws.agent.status",
        EgressServer::Workspace,
        "return await ws.agent.status('{target}');",
    ),
    call(
        "ws.agent.list",
        EgressServer::Workspace,
        "return await ws.agent.list();",
    ),
    call(
        "ws.agent.getQueue",
        EgressServer::Workspace,
        "return await ws.agent.getQueue('{target}');",
    ),
    call(
        "ws.agent.diagnostics",
        EgressServer::Workspace,
        "return await ws.agent.diagnostics();",
    ),
    call(
        "ws.agent.snapshot",
        EgressServer::Workspace,
        "return await ws.agent.snapshot();",
    ),
    call(
        "ws.agent.readConversation",
        EgressServer::Workspace,
        "return await ws.agent.readConversation('{target}', { lastN: 10 });",
    ),
    call(
        "ws.agent.summary",
        EgressServer::Workspace,
        "return await ws.agent.summary('{target}');",
    ),
    call(
        "ws.event.query",
        EgressServer::Workspace,
        "return await ws.event.query({ eventType: 'agent:*' });",
    ),
    call(
        "ws.event.query (paginated)",
        EgressServer::Workspace,
        "return await ws.event.query({ eventType: 'agent:*', paginate: true });",
    ),
    call(
        "ws.event.agentActivity (agentId)",
        EgressServer::Workspace,
        "return await ws.event.agentActivity('{target}');",
    ),
    call(
        "ws.event.agentActivity (workspace)",
        EgressServer::Workspace,
        "return await ws.event.agentActivity();",
    ),
    call(
        "ws.event.workspaceSummary",
        EgressServer::Workspace,
        "return await ws.event.workspaceSummary();",
    ),
    call(
        "ws.app.agents.list",
        EgressServer::Chief,
        "return await ws.app.agents.list();",
    ),
    call(
        "ws.app.agents.readConversation",
        EgressServer::Chief,
        "return await ws.app.agents.readConversation('{workspace}', '{target}', { lastN: 10 });",
    ),
    call(
        "ws.app.chats.list",
        EgressServer::Chief,
        "return await ws.app.chats.list();",
    ),
    call(
        "ws.app.chats.readConversation",
        EgressServer::Chief,
        "return await ws.app.chats.readConversation('{target}', { lastN: 10 });",
    ),
    call(
        "ws.app.chats.getMessageBlock",
        EgressServer::Chief,
        "return await ws.app.chats.getMessageBlock('{target}', 'm-1', 'b-1');",
    ),
    call(
        "ws.app.chats.rename",
        EgressServer::Chief,
        "return await ws.app.chats.rename('{target}', 'Renamed chat');",
    ),
];
