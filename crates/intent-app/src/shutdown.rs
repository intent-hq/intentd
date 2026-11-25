//! The agent side of a graceful stop, shared by `intentd` (process
//! shutdown) and the orchestrator (a tenant's stop), in this order:
//!
//! 1. [`close_admission`]: no new settings hooks, agent turns or host
//!    commands are admitted.
//! 2. [`stop_host_commands`]: running host commands end and their durable
//!    results are written.
//! 3. [`checkpoint_agents`]: settings hooks finish, running turns are
//!    recorded as interrupted, and agent deliveries stop.
//!
//! The caller runs its own steps between them (PTYs before 2, request
//! drains), then drains its store writers. Every step writes, so a
//! tenant runs them before its fence fires: a fired fence refuses every
//! write.

use intent_services::host_exec_stream::HostExecStreamRegistry;
use intent_services::{AgentManager, Services};

/// Close admission: settings hooks, agent turns, host commands and, when
/// given, host command streams (`streams` is process-wide: pass it only
/// when the process stops).
pub fn close_admission(
    services: &Services,
    manager: &AgentManager,
    streams: Option<&HostExecStreamRegistry>,
) {
    services.begin_settings_shutdown();
    manager.begin_shutdown();
    services.host_exec_runtime().begin_shutdown();
    if let Some(streams) = streams {
        streams.begin_shutdown();
    }
}

/// Stop the running host commands and join their durable results; then,
/// when given, the host command streams.
pub async fn stop_host_commands(services: &Services, streams: Option<&HostExecStreamRegistry>) {
    services.host_exec_runtime().shutdown().await;
    if let Some(streams) = streams {
        if let Err(error) = streams.shutdown().await {
            tracing::error!(error = %error.message, "host command shutdown failed");
        }
    }
}

/// Finish the settings hooks, record running turns as interrupted (so they
/// resume on the next start), then stop agent deliveries without retiring
/// their durable watches. `phase` is told each timed step as it starts
/// (`agent_checkpoint`, `agent_deliveries`), then `None` once all ended.
pub async fn checkpoint_agents(
    services: &Services,
    manager: &AgentManager,
    mut phase: impl FnMut(Option<&'static str>),
) {
    services.shutdown_settings().await;
    phase(Some("agent_checkpoint"));
    manager.checkpoint_shutdown().await;
    phase(Some("agent_deliveries"));
    services.shutdown_agent_deliveries().await;
    phase(None);
}
