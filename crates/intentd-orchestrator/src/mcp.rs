//! A tenant's external MCP servers: its hub starts the enabled servers in
//! its settings when the tenant is served, and the health monitor re-probes
//! them, as `intentd` does at start. Only `http` and `sse` servers run
//! here: a tenant's `Services` are in orchestrator mode, whose hub refuses
//! stdio servers (no process starts on this host for a tenant).

use intent_core::tenant_fence::SelfFence;
use intent_services::Services;

/// Start `services`' MCP health monitor and the boot start of its enabled
/// servers as tasks of the tenant's self-fence `runner`. The fence cancels
/// them, then its MCP step stops every server.
pub(crate) fn start_tenant_mcp(services: &Services, runner: &mut SelfFence) {
    runner.adopt_task(services.mcp_hub().spawn_health_monitor());
    let services = services.clone();
    runner.adopt_task(intent_core::spawn_daemon(async move {
        services.start_enabled_mcp_servers().await;
    }));
}
