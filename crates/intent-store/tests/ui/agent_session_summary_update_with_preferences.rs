// The preference-carrying full-row writer must reject a summary too.
fn forbidden(
    store: &intent_store::Store,
    workspace_id: &intent_core::WorkspaceId,
    summary: &intent_store::AgentSessionSummary,
) {
    let _ = store.update_agent_session_with_preferences(
        workspace_id,
        summary,
        false,
        None,
        None,
        false,
    );
}
fn main() {}
