//! Process-wide startup, as `intentd serve` does it: the descriptor limit
//! and workload memory containment once per process, before any tenant
//! opens; the copy-on-write probe of each tenant's workspaces root.

use std::path::{Path, PathBuf};

use intent_core::workload::Ceiling;

/// The workload memory ceiling of the whole process, in MiB: unset is
/// automatic (from the host's memory), `0` turns containment off.
pub const WORKLOAD_MEMORY_ENV: &str = "INTENTD_WORKLOAD_MEMORY_MAX_MB";

/// The ceiling [`WORKLOAD_MEMORY_ENV`] sets, with
/// `agents.workloadMemoryMaxMb`'s meaning.
///
/// # Errors
///
/// A value that is not a whole number of MiB.
pub fn ceiling_from_env(value: Option<&str>) -> Result<Ceiling, String> {
    let Some(value) = value else {
        return Ok(Ceiling::Auto);
    };
    match value.trim().parse::<u32>() {
        Ok(0) => Ok(Ceiling::Off),
        Ok(mb) => Ok(Ceiling::Bytes(u64::from(mb) * 1024 * 1024)),
        Err(_) => Err(format!(
            "{WORKLOAD_MEMORY_ENV}: {value:?} is not a whole number of MiB"
        )),
    }
}

/// Raise the soft descriptor limit (the process holds every tenant's client
/// and machine sockets) and set up workload memory containment under
/// `ceiling`, logging both. Call once, before any workload spawns and after
/// every secret is cleared from the environment (containment may run
/// `systemctl`). Never fails: each step degrades with a warning.
///
/// The ceiling is one for the whole process ([`WORKLOAD_MEMORY_ENV`]), so
/// no tenant's `agents.workloadMemoryMaxMb` sets it. `data_dir` names the
/// domain cgroup when the process starts in the root cgroup.
pub fn setup(data_dir: &Path, ceiling: Ceiling) {
    intent_core::fd_limit::raise_at_startup();
    let containment = intent_core::workload::init(&intent_core::workload::ContainmentConfig {
        ceiling,
        instance_key: data_dir.display().to_string(),
    });
    match containment.mode {
        _ if ceiling == Ceiling::Off => {
            tracing::info!("workload memory containment off: {WORKLOAD_MEMORY_ENV} is 0");
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_ceiling_comes_from_the_environment() {
        assert_eq!(ceiling_from_env(None), Ok(Ceiling::Auto));
        assert_eq!(ceiling_from_env(Some("0")), Ok(Ceiling::Off));
        assert_eq!(
            ceiling_from_env(Some(" 512 ")),
            Ok(Ceiling::Bytes(512 * 1024 * 1024))
        );
        for bad in ["", "-1", "1.5", "lots", "99999999999999999999"] {
            let error = ceiling_from_env(Some(bad)).unwrap_err();
            assert!(error.contains(WORKLOAD_MEMORY_ENV), "{bad}: {error}");
        }
    }
}
