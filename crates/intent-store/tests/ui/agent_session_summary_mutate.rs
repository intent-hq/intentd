// A summary is read-only: no field can be edited in place.
fn forbidden(mut summary: intent_store::AgentSessionSummary) {
    summary.status = intent_core::AgentStatus::Idle;
}
fn main() {}
