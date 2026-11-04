// A summary lacks `system_prompt`; the full-row writer must reject it.
fn forbidden(
    store: &intent_store::Store,
    workspace_id: &intent_core::WorkspaceId,
    summary: &intent_store::AgentSessionSummary,
) {
    let _ = store.update_agent_session(workspace_id, summary);
}
fn main() {}
