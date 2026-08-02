//! Auggie binary discovery + enhanced PATH construction (§8.2).
//!
//! Ports `auggie-path.ts` (`getEnhancedPath`, `findAuggiePathAsync`) and the
//! exec-PATH helper from `execute-auggie-command.ts` (`getAuggieExecPATH`).
//! GUI/daemon-launched processes inherit a minimal PATH (on macOS just
//! `/usr/bin:/bin:/usr/sbin:/sbin`); the enhanced PATH adds the common
//! node/npm/nvm locations so the `auggie` binary — and the `node` its shebang
//! needs — are discoverable.

use std::collections::HashSet;
use std::ffi::OsString;
use std::path::{Path, PathBuf};

use directories::BaseDirs;
use intent_core::path_utils;

/// Candidate auggie file names for the current platform (npm installs leave a
/// `.cmd`/`.bat` shim on Windows; the Intent-managed binary is `auggie.exe`).
fn candidate_names() -> &'static [&'static str] {
    if cfg!(windows) {
        &["auggie.exe", "auggie.cmd", "auggie.bat", "auggie"]
    } else {
        &["auggie"]
    }
}

fn home_dir() -> Option<PathBuf> {
    BaseDirs::new().map(|b| b.home_dir().to_path_buf())
}

/// The auggie-managed binary path (`~/.augment/bin/auggie[.exe]`), highest
/// priority in auggie discovery. This is auggie's own install location, not a
/// generic Intent-managed binary tier.
pub fn managed_binary_path() -> Option<PathBuf> {
    let name = if cfg!(windows) {
        "auggie.exe"
    } else {
        "auggie"
    };
    Some(home_dir()?.join(".augment").join("bin").join(name))
}

/// Build the ordered, de-duplicated list of directories to search (port of
/// `getEnhancedPath`): the current PATH first, then common node/npm locations,
/// then each nvm-managed node version's `bin`.
///
/// Re-exported from `intent_core::path_utils` for backward compatibility.
pub fn enhanced_path_dirs() -> Vec<PathBuf> {
    path_utils::enhanced_path_dirs()
}

/// The enhanced PATH joined into a single `OsString` (for a child's `PATH` env).
pub fn enhanced_path() -> OsString {
    std::env::join_paths(enhanced_path_dirs()).unwrap_or_default()
}

/// True when `p` is a file that is executable (unix checks the exec bit; on
/// other platforms existence as a file is sufficient — PATHEXT/shell invoke it).
fn is_executable_file(p: &Path) -> bool {
    if !p.is_file() {
        return false;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::metadata(p)
            .map(|m| m.permissions().mode() & 0o111 != 0)
            .unwrap_or(false)
    }
    #[cfg(not(unix))]
    {
        true
    }
}

/// Find the first auggie executable across `dirs` (first dir, first candidate
/// name wins).
pub fn find_in_dirs(dirs: &[PathBuf]) -> Option<PathBuf> {
    for dir in dirs {
        for name in candidate_names() {
            let candidate = dir.join(name);
            if is_executable_file(&candidate) {
                return Some(candidate);
            }
        }
    }
    None
}

/// Discover the auggie binary (focused port of `findAuggiePathAsync`): the
/// Intent-managed binary first, then a scan of the enhanced PATH.
pub fn find_auggie() -> Option<PathBuf> {
    if let Some(managed) = managed_binary_path() {
        if is_executable_file(&managed) {
            return Some(managed);
        }
    }
    find_in_dirs(&enhanced_path_dirs())
}

/// Build the PATH for *executing* auggie (port of `getAuggieExecPATH`): prepend
/// the binary's own directory so its co-located `node` resolves, then the
/// enhanced PATH.
pub fn exec_path(auggie_path: &Path) -> OsString {
    let mut dirs: Vec<PathBuf> = Vec::new();
    let mut seen: HashSet<PathBuf> = HashSet::new();
    if auggie_path.is_absolute() {
        if let Some(parent) = auggie_path.parent() {
            path_utils::push_dir(&mut dirs, &mut seen, parent.to_path_buf());
        }
    }
    for dir in enhanced_path_dirs() {
        path_utils::push_dir(&mut dirs, &mut seen, dir);
    }
    std::env::join_paths(dirs).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A fresh RAII temp directory for `tag` under the system temp root. The
    /// returned guard removes the dir on drop (including on panic); set
    /// `INTENTD_TEST_KEEP_TMP` (non-empty) to keep it around for debugging.
    fn unique_temp_dir(tag: &str) -> tempfile::TempDir {
        let mut dir = tempfile::Builder::new()
            .prefix(&format!("intent-ctx-{tag}-"))
            .tempdir()
            .expect("create test temp dir");
        if std::env::var_os("INTENTD_TEST_KEEP_TMP").is_some_and(|v| !v.is_empty()) {
            dir.disable_cleanup(true);
        }
        dir
    }

    #[test]
    fn find_in_dirs_returns_none_when_absent() {
        let dir = unique_temp_dir("absent");
        assert_eq!(find_in_dirs(&[dir.path().to_path_buf()]), None);
    }

    #[cfg(unix)]
    #[test]
    fn find_in_dirs_finds_executable() {
        use std::os::unix::fs::PermissionsExt;
        let dir = unique_temp_dir("found");
        let bin = dir.path().join("auggie");
        std::fs::write(&bin, "#!/bin/sh\nexit 0\n").unwrap();
        std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert_eq!(find_in_dirs(&[dir.path().to_path_buf()]), Some(bin));
    }

    #[cfg(unix)]
    #[test]
    fn find_in_dirs_skips_non_executable() {
        let dir = unique_temp_dir("nonexec");
        let bin = dir.path().join("auggie");
        std::fs::write(&bin, "not executable").unwrap();
        assert_eq!(find_in_dirs(&[dir.path().to_path_buf()]), None);
    }

    #[test]
    fn enhanced_path_includes_current_path_entries() {
        let dirs = enhanced_path_dirs();
        assert!(!dirs.is_empty());
    }
}
