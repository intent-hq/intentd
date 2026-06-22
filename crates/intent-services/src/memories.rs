//! Long-term agent memory accessor (§9.12, §18.5). **Internal only**: there is
//! no `memories.*` RPC in v1 (§18.5) — rows are written/read internally and
//! surfaced to agents through the agent→BE MCP callback (§6.8) as a context
//! source. The thin read path here backs the `search.memories` adapter
//! (`search_ops::memory_matches`, PROTOCOL §5.15). Ports
//! `src/features/memories/main/memories.service.ts`.

use intent_core::{Memory, Result, WorkspaceId};
use intent_store::Store;

/// List memories for internal use / the `search.memories` adapter (PROTOCOL
/// §5.15). `Some(ws)` scopes to a workspace; `None` spans every workspace. The
/// internal write path is [`Store::insert_memory`] (no `memories.*` RPC, §18.5).
pub(crate) async fn list(store: &Store, workspace_id: Option<&WorkspaceId>) -> Result<Vec<Memory>> {
    store.list_memories(workspace_id).await
}
