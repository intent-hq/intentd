//! The agent manager one backend runs, built the same way by the daemon (for
//! its one backend) and by an orchestrator (once per tenant), and the agent
//! state a restart restores before any client is served.
//!
//! Every `agent.*` turn, local or on a machine, goes through the
//! [`AgentManager`] attached to the backend's [`Services`]: without one an
//! `agent.sendMessage` is stored and never runs. [`start_agent_manager`]
//! builds and attaches it in one step so no caller can do one without the
//! other.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use intent_core::settings_file::SettingsFile;
use intent_services::{
    default_process_cap, max_concurrent_agents, AgentManager, BusEventSink, EventBus,
    PermissionPolicy, Services,
};

/// Where one backend's agent manager keeps its files, and its limits.
pub struct AgentManagerSetup<'a> {
    /// The backend's data dir: agent logs, generated configs and temp
    /// folders live under it (`intent_core::agent_logs_root`,
    /// `agent_configs_root`, `agent_tmp_root`).
    pub data_dir: &'a Path,
    /// The spawn cwd of the backend's chief provider children (the same dir
    /// `Services::with_chief_cwd_root` was given).
    pub chief_cwd_root: PathBuf,
    /// How many agents may run at once ([`process_cap`]).
    pub process_cap: usize,
    /// A process-wide permission policy replacing the
    /// `agents.permissionPolicy` setting, if any.
    pub policy_override: Option<PermissionPolicy>,
}

/// The agent cap `settings` resolve to: a positive `agents.maxConcurrent`,
/// else the RAM-based default.
#[must_use]
pub fn process_cap(settings: &SettingsFile) -> usize {
    max_concurrent_agents(settings).unwrap_or_else(default_process_cap)
}

/// Build the backend's agent manager over `services`, publishing onto `bus`,
/// and attach it to `services` (every clone sees it). The roots are swept of
/// what a previous run left behind: no agent child is live yet.
///
/// The caller keeps the returned strong handle for as long as the backend
/// serves: `services` holds only a weak one.
#[must_use]
pub fn start_agent_manager(
    services: &Services,
    bus: &EventBus,
    setup: AgentManagerSetup<'_>,
) -> Arc<AgentManager> {
    let manager = Arc::new(
        AgentManager::new(
            services.clone(),
            Arc::new(BusEventSink::new(bus.clone())),
            setup.process_cap,
        )
        .with_policy_override(setup.policy_override)
        // STAB-53: capture each spawned child's stderr under
        // `<data_dir>/agent-logs/<agent-id>/<YYYY-MM-DD>.log`.
        .with_agent_log_root(intent_core::agent_logs_root(setup.data_dir))
        // Per-agent generated config files (`--mcp-config`, `--rules`,
        // pi-extension delivery) are written under the daemon-owned
        // `<data_dir>/agent-configs` instead of the global OS temp dir
        // (monorepo#1302). Swept at startup (no agent child is live yet)
        // so files leaked by a killed daemon don't accumulate.
        .with_agent_config_root({
            let root = intent_core::agent_configs_root(setup.data_dir);
            if let Err(e) = intent_core::sweep_agent_configs(&root) {
                tracing::warn!(error = %e, path = %root.display(), "agent-configs sweep failed");
            }
            root
        })
        // Every agent gets a private temp folder (`TMPDIR` / `TMP` / `TEMP`)
        // under a per-daemon root in the OS temp dir, so agents never share
        // fixed temp file names. Folders outlive each launch so a restarted
        // agent finds its files; the startup sweep (and the hourly one in
        // `tenant::start_background`) reclaims only folders unused for
        // `AGENT_TMP_MAX_IDLE`.
        .with_agent_tmp_root({
            let root = intent_core::agent_tmp_root(setup.data_dir);
            if let Err(e) =
                intent_core::sweep_agent_tmp_root(&root, intent_core::AGENT_TMP_MAX_IDLE)
            {
                tracing::warn!(error = %e, path = %root.display(), "agent temp sweep failed");
            }
            root
        })
        // STAB-50: chief provider children spawn in the dedicated, empty
        // `<data_dir>/chief-cwd` directory instead of `/tmp`. Swept at
        // startup (no chief child is live yet) so leftovers a provider
        // scribbled into its cwd don't accumulate across daemon runs and
        // get re-indexed.
        .with_chief_cwd_root({
            let root = setup.chief_cwd_root;
            if let Err(e) = intent_core::sweep_chief_cwd(&root) {
                tracing::warn!(error = %e, path = %root.display(), "chief-cwd sweep failed");
            }
            root
        }),
    );
    // Attach the manager to the services surface so the `agent.*` RPC
    // handlers drive the live spawn/turn/MCP loop at runtime (the shared
    // `OnceLock` is visible to every clone, including the ones the
    // transport and the runtime already hold).
    services.attach_agent_manager(&manager);
    manager
}

/// Restore the agent send queues and the desktop-control state a restart
/// left in the store, before any listener serves: messages queued at the
/// previous shutdown survive it. Restore only; it never starts a turn. The
/// number of queued messages restored.
///
/// # Errors
/// Any recovery error: the caller must not serve queue mutations then, since
/// a new one would replace durable entries from an unreconciled, empty
/// in-memory map.
pub async fn restore_startup_queues(services: &Services) -> intent_core::Result<usize> {
    let count = services.rehydrate_agent_queues().await?;
    services.desktop_recover().await?;
    Ok(count)
}
