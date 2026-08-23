//! Persisted sitter state (`<data_dir>/sitter/state.json`).
//!
//! A missing, unreadable, corrupt, or unknown-schema `state.json` is treated
//! as "nothing installed" — [`load`] always returns a usable state and never
//! panics.

use std::fs;
use std::io;
use std::path::Path;

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::cli::Channel;

/// Current `state.json` schema version.
pub const STATE_SCHEMA_VERSION: u32 = 1;

/// Contents of `state.json`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SitterState {
    /// Schema version (see [`STATE_SCHEMA_VERSION`]).
    pub schema: u32,
    /// Channel the installed daemon was fetched from.
    pub channel: Channel,
    /// Currently installed daemon version (`versions/<version>/`), if any.
    pub current_version: Option<String>,
    /// When the last update check ran (RFC 3339).
    #[serde(default, with = "time::serde::rfc3339::option")]
    pub last_check_at: Option<OffsetDateTime>,
    /// When the next periodic update check is due (RFC 3339).
    #[serde(default, with = "time::serde::rfc3339::option")]
    pub next_check_at: Option<OffsetDateTime>,
}

impl Default for SitterState {
    fn default() -> Self {
        Self {
            schema: STATE_SCHEMA_VERSION,
            channel: Channel::default(),
            current_version: None,
            last_check_at: None,
            next_check_at: None,
        }
    }
}

/// Load state from `path`. Missing, unreadable, corrupt, or unknown-schema
/// files all yield the default "nothing installed" state.
pub fn load(path: &Path) -> SitterState {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(e) => {
            if e.kind() != io::ErrorKind::NotFound {
                tracing::warn!(path = %path.display(), error = %e, "failed to read state.json; treating as nothing installed");
            }
            return SitterState::default();
        }
    };
    match serde_json::from_slice::<SitterState>(&bytes) {
        Ok(state) if state.schema == STATE_SCHEMA_VERSION => state,
        Ok(state) => {
            tracing::warn!(path = %path.display(), schema = state.schema, "unknown state.json schema; treating as nothing installed");
            SitterState::default()
        }
        Err(e) => {
            tracing::warn!(path = %path.display(), error = %e, "corrupt state.json; treating as nothing installed");
            SitterState::default()
        }
    }
}

/// Persist state to `path` (creating parent directories), writing via a
/// temp file + rename so a crash never leaves a truncated `state.json`.
///
/// # Errors
///
/// Returns the underlying I/O error if serialization, creating parent directories, writing the temp file, or the rename fails.
pub fn save(path: &Path, state: &SitterState) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let json = serde_json::to_vec_pretty(state).map_err(io::Error::other)?;
    let tmp = path.with_extension("json.tmp");
    fs::write(&tmp, json)?;
    fs::rename(&tmp, path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::macros::datetime;

    #[test]
    fn save_load_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sitter").join("state.json");
        let state = SitterState {
            schema: STATE_SCHEMA_VERSION,
            channel: Channel::Beta,
            current_version: Some("1.2.3".to_string()),
            last_check_at: Some(datetime!(2026-07-23 01:02:03 UTC)),
            next_check_at: Some(datetime!(2026-07-23 18:00:00 UTC)),
        };
        save(&path, &state).unwrap();
        assert_eq!(load(&path), state);
    }

    #[test]
    fn missing_file_is_nothing_installed() {
        let dir = tempfile::tempdir().unwrap();
        let state = load(&dir.path().join("state.json"));
        assert_eq!(state, SitterState::default());
        assert_eq!(state.current_version, None);
    }

    #[test]
    fn corrupt_file_is_nothing_installed() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.json");
        for corrupt in ["not json at all", "{\"schema\": \"one\"}", ""] {
            fs::write(&path, corrupt).unwrap();
            assert_eq!(load(&path), SitterState::default());
        }
    }

    #[test]
    fn unknown_schema_is_nothing_installed() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.json");
        let state = SitterState {
            schema: STATE_SCHEMA_VERSION + 1,
            current_version: Some("9.9.9".to_string()),
            ..SitterState::default()
        };
        save(&path, &state).unwrap();
        assert_eq!(load(&path), SitterState::default());
    }

    #[test]
    fn timestamps_serialize_as_rfc3339() {
        let state = SitterState {
            last_check_at: Some(datetime!(2026-07-23 01:02:03 UTC)),
            ..SitterState::default()
        };
        let json: serde_json::Value =
            serde_json::from_str(&serde_json::to_string(&state).unwrap()).unwrap();
        assert_eq!(json["last_check_at"], "2026-07-23T01:02:03Z");
        assert_eq!(json["schema"], 1);
        assert_eq!(json["channel"], "stable");
    }
}
