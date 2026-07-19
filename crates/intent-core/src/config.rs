//! Daemon configuration and path resolution (§11.2).
//!
//! Paths are resolved via the `directories` crate, honoring the
//! `INTENTD_DATA_DIR` and `INTENTD_CONFIG` environment overrides. The data dir
//! holds the SQLite database (`intentd.db`) and the UDS (`intentd.sock`).

use std::path::{Path, PathBuf};

use crate::error::{Error, Result};

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
    /// Resolve paths from the platform defaults and env overrides (§11.2).
    pub fn resolve() -> Result<Self> {
        let proj = directories::ProjectDirs::from("", "", "intentd");

        let data_dir = match std::env::var_os("INTENTD_DATA_DIR") {
            Some(p) => PathBuf::from(p),
            None => proj
                .as_ref()
                .map(|d| d.data_dir().to_path_buf())
                .ok_or_else(|| Error::Internal("could not resolve data directory".to_string()))?,
        };

        let config_path = match std::env::var_os("INTENTD_CONFIG") {
            Some(p) => PathBuf::from(p),
            None => proj
                .as_ref()
                .map(|d| d.config_dir().join("config.toml"))
                .ok_or_else(|| Error::Internal("could not resolve config directory".to_string()))?,
        };

        let db_path = data_dir.join("intentd.db");
        let socket_path = data_dir.join("intentd.sock");
        let pid_path = data_dir.join("intentd.pid");
        let idle_reap_minutes = load_idle_reap_minutes(&config_path);
        let stream_retention_hours = load_stream_retention_hours(&config_path);

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

/// Read `agents.idleReapMinutes` from `config.toml`, falling back to the
/// `INTENTD_IDLE_REAP_MINUTES` env override and finally
/// [`DEFAULT_IDLE_REAP_MINUTES`]. A missing/unparseable file or key is not an
/// error — the daemon simply uses the default.
fn load_idle_reap_minutes(config_path: &Path) -> u32 {
    if let Some(v) = std::env::var_os("INTENTD_IDLE_REAP_MINUTES") {
        if let Ok(n) = v.to_string_lossy().trim().parse::<u32>() {
            return n;
        }
    }
    let Ok(text) = std::fs::read_to_string(config_path) else {
        return DEFAULT_IDLE_REAP_MINUTES;
    };
    let Ok(value) = text.parse::<toml::Value>() else {
        return DEFAULT_IDLE_REAP_MINUTES;
    };
    value
        .get("agents")
        .and_then(|a| {
            a.get("idleReapMinutes")
                .or_else(|| a.get("idle_reap_minutes"))
        })
        .and_then(toml::Value::as_integer)
        .and_then(|n| u32::try_from(n).ok())
        .unwrap_or(DEFAULT_IDLE_REAP_MINUTES)
}

/// Read `events.streamRetentionHours` from `config.toml`, falling back to the
/// `INTENTD_STREAM_RETENTION_HOURS` env override and finally
/// [`DEFAULT_STREAM_RETENTION_HOURS`]. A missing/unparseable file or key is not
/// an error — the daemon simply uses the default (72h opt-out retention).
fn load_stream_retention_hours(config_path: &Path) -> u32 {
    if let Some(v) = std::env::var_os("INTENTD_STREAM_RETENTION_HOURS") {
        if let Ok(n) = v.to_string_lossy().trim().parse::<u32>() {
            return n;
        }
    }
    let Ok(text) = std::fs::read_to_string(config_path) else {
        return DEFAULT_STREAM_RETENTION_HOURS;
    };
    let Ok(value) = text.parse::<toml::Value>() else {
        return DEFAULT_STREAM_RETENTION_HOURS;
    };
    value
        .get("events")
        .and_then(|e| {
            e.get("streamRetentionHours")
                .or_else(|| e.get("stream_retention_hours"))
        })
        .and_then(toml::Value::as_integer)
        .and_then(|n| u32::try_from(n).ok())
        .unwrap_or(DEFAULT_STREAM_RETENTION_HOURS)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_config(body: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!("intentd-cfg-{}.toml", uuid::Uuid::new_v4()));
        std::fs::write(&path, body).unwrap();
        path
    }

    #[test]
    fn idle_reap_minutes_defaults_when_file_missing() {
        let missing =
            std::env::temp_dir().join(format!("intentd-missing-{}.toml", uuid::Uuid::new_v4()));
        assert_eq!(load_idle_reap_minutes(&missing), DEFAULT_IDLE_REAP_MINUTES);
    }

    #[test]
    fn idle_reap_minutes_parsed_from_camel_and_snake_case() {
        let camel = temp_config("[agents]\nidleReapMinutes = 5\n");
        assert_eq!(load_idle_reap_minutes(&camel), 5);
        std::fs::remove_file(&camel).ok();

        let snake = temp_config("[agents]\nidle_reap_minutes = 12\n");
        assert_eq!(load_idle_reap_minutes(&snake), 12);
        std::fs::remove_file(&snake).ok();
    }

    #[test]
    fn idle_reap_minutes_zero_disables() {
        let path = temp_config("[agents]\nidleReapMinutes = 0\n");
        assert_eq!(load_idle_reap_minutes(&path), 0);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn stream_retention_hours_defaults_to_72h_when_file_missing() {
        let missing =
            std::env::temp_dir().join(format!("intentd-missing-{}.toml", uuid::Uuid::new_v4()));
        assert_eq!(
            load_stream_retention_hours(&missing),
            DEFAULT_STREAM_RETENTION_HOURS
        );
        assert_eq!(DEFAULT_STREAM_RETENTION_HOURS, 72);
    }

    #[test]
    fn stream_retention_hours_parsed_from_camel_and_snake_case() {
        let camel = temp_config("[events]\nstreamRetentionHours = 48\n");
        assert_eq!(load_stream_retention_hours(&camel), 48);
        std::fs::remove_file(&camel).ok();

        let snake = temp_config("[events]\nstream_retention_hours = 72\n");
        assert_eq!(load_stream_retention_hours(&snake), 72);
        std::fs::remove_file(&snake).ok();
    }

    #[test]
    fn stream_retention_hours_zero_disables() {
        let path = temp_config("[events]\nstreamRetentionHours = 0\n");
        assert_eq!(load_stream_retention_hours(&path), 0);
        std::fs::remove_file(&path).ok();
    }
}
