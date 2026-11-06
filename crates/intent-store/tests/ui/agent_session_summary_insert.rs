// Inserting a summary would persist a row without its heavy columns.
fn forbidden(store: &intent_store::Store, summary: &intent_store::AgentSessionSummary) {
    let _ = store.insert_agent_session(summary);
}
fn main() {}
