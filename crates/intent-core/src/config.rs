//! Daemon configuration and path resolution (§11.2).
//!
//! Paths are resolved via the `directories` crate, honoring the
//! `INTENTD_DATA_DIR` and `INTENTD_CONFIG` environment overrides. The data dir
//! holds the SQLite database (`intentd.db`) and the UDS (`intentd.sock`).

use std::path::PathBuf;

use crate::error::{Error, Result};

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

        Ok(Self {
            data_dir,
            config_path,
            db_path,
            socket_path,
        })
    }
}
