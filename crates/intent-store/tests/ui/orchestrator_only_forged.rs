// The token has a private field: no struct literal outside intent-store.
fn forged() -> intent_store::OrchestratorOnly {
    intent_store::OrchestratorOnly { _sealed: () }
}
fn main() {}
