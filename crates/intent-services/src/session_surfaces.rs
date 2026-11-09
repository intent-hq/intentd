//! The spawn-time context surfaces of one agent session: the `workspace_api`
//! tool description its bridge serves, the reference its system prompt
//! carries and the remote-placement renderer, all read through the
//! session's stamped harness version. Local spawns, remote agents and the
//! goldens build them here, so what a respawned session gets is what its
//! tests pin.

use intent_acp::{WorkspaceApiDocs, WorkspaceMcpServer};
use intent_core::machine_selection::PlacementHelpRenderer;
use intent_core::AgentSession;
use intent_providers::ProviderConfig;

/// What decides a session's context surfaces, captured once at spawn.
#[derive(Clone, Copy)]
pub(crate) struct SessionSurfaces {
    workspace_api: &'static dyn WorkspaceApiDocs,
    placement_help: PlacementHelpRenderer,
    truncates_tool_descriptions: bool,
}

impl SessionSurfaces {
    pub(crate) fn new(session: &AgentSession, provider: &ProviderConfig) -> Self {
        let context = crate::harness::resolve_entry(&session.harness_version).context;
        Self {
            workspace_api: context.workspace_api,
            placement_help: context.placement_help,
            truncates_tool_descriptions: provider.truncates_tool_descriptions,
        }
    }

    /// Configure `server` to render this session's `workspace_api` text.
    pub(crate) fn configure(&self, server: WorkspaceMcpServer) -> WorkspaceMcpServer {
        server
            .with_compact_tool_descriptions(self.truncates_tool_descriptions)
            .with_workspace_api_docs(self.workspace_api)
    }

    /// The `workspace_api` reference the session's system prompt carries,
    /// from a server built by [`Self::configure`].
    pub(crate) fn prompt_docs(&self, server: &WorkspaceMcpServer) -> Option<String> {
        server.workspace_api_prompt_reference()
    }

    /// The session's remote-placement renderer.
    pub(crate) fn placement_help(&self) -> PlacementHelpRenderer {
        self.placement_help
    }
}
