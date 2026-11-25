//! Process-wide startup, as `intentd serve` does it: the descriptor limit
//! and workload memory containment once per process, before any tenant
//! opens; the copy-on-write probe of each tenant's workspaces root.

use std::path::{Path, PathBuf};

/// Raise the soft descriptor limit (the process holds every tenant's client
/// and machine sockets) and set up workload memory containment, logging
/// both. Call once, before any workload spawns. Never fails: each step
/// degrades with a warning.
///
/// Containment is one ceiling for the whole process, so no tenant's
/// `agents.workloadMemoryMaxMb` sets it: the host's memory does
/// ([`Ceiling::Auto`](intent_core::workload::Ceiling::Auto)). `data_dir`
/// names the domain cgroup when the process starts in the root cgroup.
pub fn setup(data_dir: &Path) {
    intent_core::fd_limit::raise_at_startup();
    let containment = intent_core::workload::init(&intent_core::workload::ContainmentConfig {
        ceiling: intent_core::workload::Ceiling::Auto,
        instance_key: data_dir.display().to_string(),
    });
    match containment.mode {
        intent_core::workload::ContainmentMode::Cgroup => tracing::info!(
            memory_max_bytes = containment.memory_max_bytes,
            "workload memory containment on"
        ),
        mode => tracing::warn!(
            ?mode,
            reason = containment.reason.as_deref().unwrap_or(""),
            "workload memory containment unavailable"
        ),
    }
}

/// Probe `workspaces` (a tenant's workspaces root) for copy-on-write
/// support off the async runtime, so its first workspace checkout reads
/// the cached result. Best-effort: a failure is retried on first use.
pub fn probe_cow(workspaces: PathBuf) {
    tokio::task::spawn_blocking(move || {
        if let Err(error) = std::fs::create_dir_all(&workspaces) {
            tracing::debug!(%error, "copy-on-write probe skipped");
            return;
        }
        match intent_git::cow_probe(&workspaces, &workspaces) {
            Ok(support) => tracing::debug!(?support, "copy-on-write probe"),
            Err(error) => tracing::debug!(%error, "copy-on-write probe failed"),
        }
    });
}
