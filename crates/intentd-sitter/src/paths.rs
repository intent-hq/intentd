//! Data-dir resolution and the on-disk sitter layout.
//!
//! The data dir mirrors intent-core's `Config::resolve` (`INTENTD_DATA_DIR`
//! env override, else `directories::ProjectDirs::from("", "", "intentd")`)
//! rather than calling it: `Config::resolve` strictly loads — and, when
//! missing, initializes — the daemon's `<data_dir>/config.toml`, so reusing
//! it would make the sitter create daemon config files and fail on malformed
//! daemon settings just to locate its own state directory.
//!
//! Layout under `<data_dir>/sitter/`:
//!
//! ```text
//! sitter/
//! ├── versions/<version>/intentd[.exe]   # installed daemon binaries
//! ├── versions/<version>/libexec/…       # sidecar payload from the archive (tailcat + LICENSE)
//! ├── tmp/                               # in-flight downloads/extractions
//! ├── config.toml                        # user-editable channel pin
//! ├── state.json                         # persisted sitter state
//! └── sitter.pid                         # serve-mode sitter pid (while running)
//! ```

use std::ffi::OsString;
use std::path::{Path, PathBuf};

/// Environment variable overriding the intentd data directory.
pub const DATA_DIR_ENV: &str = "INTENTD_DATA_DIR";

/// Installed daemon binary file name.
pub const DAEMON_BIN_NAME: &str = if cfg!(windows) {
    "intentd.exe"
} else {
    "intentd"
};

/// Failed to resolve a platform data directory.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[error("could not resolve data directory")]
pub struct DataDirError;

/// Resolved sitter filesystem locations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SitterPaths {
    /// The intentd data directory (shared with the daemon).
    pub data_dir: PathBuf,
    /// `<data_dir>/sitter/` — everything the sitter owns lives below this.
    pub sitter_dir: PathBuf,
    /// `<sitter_dir>/versions/` — one subdirectory per installed version.
    pub versions_dir: PathBuf,
    /// `<sitter_dir>/tmp/` — in-flight downloads and extractions.
    pub tmp_dir: PathBuf,
    /// `<sitter_dir>/state.json`.
    pub state_path: PathBuf,
    /// `<sitter_dir>/config.toml` — user-editable channel pin.
    pub config_path: PathBuf,
    /// `<sitter_dir>/sitter.pid` — pid of the serve-mode sitter while it
    /// runs (`intentd restart` reads it to find the supervisor; on Windows
    /// `install.ps1` reads it as the upgrade-allowance ownership witness).
    pub pid_path: PathBuf,
}

impl SitterPaths {
    /// Resolve from the process environment (`INTENTD_DATA_DIR`).
    ///
    /// # Errors
    ///
    /// Returns [`DataDirError`] when no override is set and the platform data directory cannot be resolved.
    pub fn resolve() -> Result<Self, DataDirError> {
        Self::from_env(std::env::var_os(DATA_DIR_ENV))
    }

    /// Resolve from an explicit env-override value (parameterized so tests
    /// never mutate process state). Mirrors intent-core's `Config::resolve`:
    /// any set `INTENTD_DATA_DIR` wins, else the platform default.
    ///
    /// # Errors
    ///
    /// Returns [`DataDirError`] when no override is set and the platform data directory cannot be resolved.
    pub fn from_env(data_dir_override: Option<OsString>) -> Result<Self, DataDirError> {
        let data_dir = match data_dir_override {
            Some(p) => PathBuf::from(p),
            None => directories::ProjectDirs::from("", "", "intentd")
                .map(|d| d.data_dir().to_path_buf())
                .ok_or(DataDirError)?,
        };
        Ok(Self::from_data_dir(&data_dir))
    }

    /// Derive the sitter layout below a known data dir.
    #[must_use]
    pub fn from_data_dir(data_dir: &Path) -> Self {
        let sitter_dir = data_dir.join("sitter");
        Self {
            data_dir: data_dir.to_path_buf(),
            versions_dir: sitter_dir.join("versions"),
            tmp_dir: sitter_dir.join("tmp"),
            state_path: sitter_dir.join("state.json"),
            config_path: sitter_dir.join("config.toml"),
            pid_path: sitter_dir.join("sitter.pid"),
            sitter_dir,
        }
    }

    /// Path of the installed daemon binary for `version`:
    /// `versions/<version>/intentd[.exe]`.
    #[must_use]
    pub fn daemon_binary(&self, version: &str) -> PathBuf {
        self.versions_dir.join(version).join(DAEMON_BIN_NAME)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn env_override_wins() {
        let paths = SitterPaths::from_env(Some(OsString::from("/tmp/intentd-data"))).unwrap();
        assert_eq!(paths.data_dir, PathBuf::from("/tmp/intentd-data"));
        assert_eq!(
            paths.sitter_dir,
            PathBuf::from("/tmp/intentd-data").join("sitter")
        );
    }

    #[test]
    fn no_override_uses_platform_default() {
        let paths = SitterPaths::from_env(None).unwrap();
        let expected = directories::ProjectDirs::from("", "", "intentd")
            .unwrap()
            .data_dir()
            .to_path_buf();
        assert_eq!(paths.data_dir, expected);
    }

    #[test]
    fn layout_under_sitter_dir() {
        let paths = SitterPaths::from_data_dir(Path::new("/data"));
        let sitter = PathBuf::from("/data").join("sitter");
        assert_eq!(paths.state_path, sitter.join("state.json"));
        assert_eq!(paths.config_path, sitter.join("config.toml"));
        assert_eq!(paths.pid_path, sitter.join("sitter.pid"));
        assert_eq!(paths.versions_dir, sitter.join("versions"));
        assert_eq!(paths.tmp_dir, sitter.join("tmp"));
        let bin = paths.daemon_binary("1.2.3");
        assert_eq!(
            bin,
            sitter.join("versions").join("1.2.3").join(DAEMON_BIN_NAME)
        );
        if cfg!(windows) {
            assert_eq!(bin.file_name().unwrap(), "intentd.exe");
        } else {
            assert_eq!(bin.file_name().unwrap(), "intentd");
        }
    }
}
