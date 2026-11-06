//! A remote agent's skills, discovered on this machine (monorepo
//! `docs/protocol/node-link.md` § Skills on the machine, nodeProtocol 12).

use std::path::PathBuf;

use intent_core::node_link::MachineSkill;

/// Discover off the runtime: user roots under `home` (this daemon's own
/// Claude folder) and project roots in `checkout`.
pub(super) async fn discover(
    checkout: Option<PathBuf>,
    home: Option<PathBuf>,
) -> Vec<MachineSkill> {
    tokio::task::spawn_blocking(move || {
        intent_services::skills::discover_on_machine(checkout.as_deref(), home)
    })
    .await
    .unwrap_or_default()
}

/// The catalog prompt block, or `None` without skills. Wrapped like head's
/// instructions, so it reads as instructions rather than the user's text.
pub(super) fn catalog_block(skills: &[MachineSkill]) -> Option<String> {
    let catalog = intent_services::skills::machine_catalog(skills);
    (!catalog.is_empty()).then(|| format!("<system>\n{catalog}\n</system>"))
}
