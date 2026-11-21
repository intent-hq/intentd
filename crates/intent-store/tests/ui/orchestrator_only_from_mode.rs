// The public `DaemonMode` enum is `Copy` and anyone can name
// `DaemonMode::Orchestrator`, so it must not convert into the token.
use intent_core::settings_file::DaemonMode;
fn from_mode() -> intent_store::OrchestratorOnly {
    DaemonMode::Orchestrator.into()
}
fn main() {}
