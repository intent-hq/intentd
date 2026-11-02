//! The remote agent's projected placement (`remotePlacement` session
//! metadata): read, change and write it back.

use intent_core::remote_launch::{RemotePlacementView, REMOTE_PLACEMENT_METADATA_KEY};
use intent_core::{AgentId, Error, Result, WorkspaceId};
use intent_store::Store;

/// Apply `change` to the agent's view and store it. An agent without a
/// view (not remote, or deleted) is left alone.
pub(crate) async fn update(
    store: &Store,
    workspace_id: &WorkspaceId,
    agent_id: &AgentId,
    change: impl FnOnce(&mut RemotePlacementView),
) -> Result<()> {
    let session = match store.get_agent_session(agent_id).await {
        Ok(session) => session,
        Err(Error::NotFound(_)) => return Ok(()),
        Err(error) => return Err(error),
    };
    if &session.workspace_id != workspace_id {
        return Ok(());
    }
    let Some(mut view) = RemotePlacementView::from_metadata(session.metadata.as_ref()) else {
        return Ok(());
    };
    let before = view.clone();
    change(&mut view);
    if view == before {
        return Ok(());
    }
    let encoded = serde_json::to_string(&view)
        .map_err(|e| Error::Internal(format!("encode remote placement: {e}")))?;
    store
        .set_agent_session_metadata_key_json(
            workspace_id,
            agent_id,
            REMOTE_PLACEMENT_METADATA_KEY,
            &encoded,
            &intent_core::now_iso(),
        )
        .await
}
