//! Daemon configuration and path resolution (§11.2).
//!
//! Paths are resolved via the `directories` crate, honoring the
//! `INTENTD_DATA_DIR` and `INTENTD_CONFIG` environment overrides. The data dir
//! holds the SQLite database (`intentd.db`), the UDS (`intentd.sock`), and the
//! non-secret settings file (`config.toml`), which is loaded strictly through
//! [`crate::settings_file::SettingsFile`] — a malformed file fails `resolve()`
//! instead of being silently ignored.

use std::path::PathBuf;

use crate::error::{Error, Result};
use crate::settings_file::SettingsFile;

/// Default idle-reap TTL in minutes (`agents.idleReapMinutes`, §11.1); `0`
/// disables the sweep entirely.
pub const DEFAULT_IDLE_REAP_MINUTES: u32 = 30;

/// Default ephemeral-event retention TTL in hours (`events.streamRetentionHours`,
/// §10.2); `0` disables the retention/compaction sweep entirely. Defaults to 72h
/// (3 days) so dev/release databases do not grow unboundedly; set to `0` to opt
/// out and preserve all events.
pub const DEFAULT_STREAM_RETENTION_HOURS: u32 = 72;

/// Resolved filesystem locations for the daemon.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    /// Data directory (database, certs, runtime socket).
    pub data_dir: PathBuf,
    /// Path to `config.toml` (non-secret settings).
    pub config_path: PathBuf,
    /// Path to the SQLite database file.
    pub db_path: PathBuf,
    /// Path to the Unix-domain-socket the daemon listens on.
    pub socket_path: PathBuf,
    /// Path to the single-instance pidfile (`intentd.pid`, §5.6/§9.3).
    pub pid_path: PathBuf,
    /// Minutes an agent may sit idle before the reap sweep evicts it
    /// (`agents.idleReapMinutes`, §11.1); `0` disables idle reaping.
    pub idle_reap_minutes: u32,
    /// Hours ephemeral events (`agent:stream:*`, `file:*`, `terminal:data`,
    /// `host:exec:*`) are retained before the retention/compaction sweep deletes
    /// them (`events.streamRetentionHours`, §10.2); `0` disables the sweep.
    /// Lifecycle/tool/note/task/workspace events are preserved regardless of age.
    pub stream_retention_hours: u32,
}

impl Config {
    /// Resolve paths from the platform defaults and env overrides (§11.2),
    /// then load `config.toml` strictly via [`SettingsFile::load_or_init`].
    ///
    /// `config.toml` lives in the **data dir** (`<data_dir>/config.toml`);
    /// `INTENTD_CONFIG` overrides the full path. A missing file is initialized
    /// with the commented default template; a malformed file (unknown key,
    /// wrong type, out-of-range value) is an error — never silently ignored.
    /// `INTENTD_IDLE_REAP_MINUTES` / `INTENTD_STREAM_RETENTION_HOURS` env vars
    /// still take precedence over the file for their respective knobs.
    pub fn resolve() -> Result<Self> {
        let data_dir = match std::env::var_os("INTENTD_DATA_DIR") {
            Some(p) => PathBuf::from(p),
            None => directories::ProjectDirs::from("", "", "intentd")
                .map(|d| d.data_dir().to_path_buf())
                .ok_or_else(|| Error::Internal("could not resolve data directory".to_string()))?,
        };

        let config_path = match std::env::var_os("INTENTD_CONFIG") {
            Some(p) => PathBuf::from(p),
            None => data_dir.join("config.toml"),
        };

        let settings = SettingsFile::load_or_init(&config_path)?;

        let db_path = data_dir.join("intentd.db");
        let socket_path = data_dir.join("intentd.sock");
        let pid_path = data_dir.join("intentd.pid");
        let idle_reap_minutes =
            env_u32("INTENTD_IDLE_REAP_MINUTES").unwrap_or(settings.agents.idle_reap_minutes);
        let stream_retention_hours = env_u32("INTENTD_STREAM_RETENTION_HOURS")
            .unwrap_or(settings.events.stream_retention_hours);

        Ok(Self {
            data_dir,
            config_path,
            db_path,
            socket_path,
            pid_path,
            idle_reap_minutes,
            stream_retention_hours,
        })
    }
}

/// Read a `u32` env override; unset or unparseable values yield `None` so the
/// caller falls through to the file-backed value.
fn env_u32(name: &str) -> Option<u32> {
    std::env::var_os(name)?
        .to_string_lossy()
        .trim()
        .parse()
        .ok()
}
