//! Shared PATH enrichment utilities for packaged app environments.
//!
//! When launched via Finder/launchd, macOS apps inherit a minimal PATH
//! (/usr/bin:/bin:/usr/sbin:/sbin). This module provides utilities to
//! enrich PATH with common development tool directories (node, nvm, homebrew,
//! volta, asdf, etc.) so that tools and their dependencies can be discovered.

use std::collections::HashSet;
use std::path::PathBuf;

use directories::BaseDirs;

fn home_dir() -> Option<PathBuf> {
    BaseDirs::new().map(|b| b.home_dir().to_path_buf())
}

/// Helper to push a directory to the list if it's not empty and not already seen.
pub fn push_dir(dirs: &mut Vec<PathBuf>, seen: &mut HashSet<PathBuf>, dir: PathBuf) {
    if dir.as_os_str().is_empty() {
        return;
    }
    if seen.insert(dir.clone()) {
        dirs.push(dir);
    }
}

/// Returns an ordered, de-duplicated list of directories commonly containing
/// development tools (node, npm, homebrew, volta, asdf, nvm, etc.), PLUS the
/// inherited PATH directories.
///
/// The list starts with the current PATH environment variable, then adds
/// platform-specific common tool directories. This ensures tools and their
/// dependencies (e.g., #!/usr/bin/env node scripts) can be found even when
/// running in packaged app environments with minimal inherited PATH.
///
/// Note: Callers that need custom precedence (e.g., provider-binary dir first,
/// then ~/.augment/bin, then enriched dirs, then inherited PATH) should use
/// `enriched_tool_dirs()` + split_paths to build the order themselves.
pub fn enhanced_path_dirs() -> Vec<PathBuf> {
    let mut dirs: Vec<PathBuf> = Vec::new();
    let mut seen: HashSet<PathBuf> = HashSet::new();

    // Start with current PATH
    if let Some(path) = std::env::var_os("PATH") {
        for dir in std::env::split_paths(&path) {
            push_dir(&mut dirs, &mut seen, dir);
        }
    }

    // Add enriched tool directories
    for dir in enriched_tool_dirs() {
        push_dir(&mut dirs, &mut seen, dir);
    }

    dirs
}

/// Returns platform-specific directories commonly containing development tools
/// (node, npm, homebrew, volta, asdf, nvm, etc.), WITHOUT the inherited PATH.
///
/// Use this when you need to control precedence explicitly (e.g., prepend
/// provider-binary dir and ~/.augment/bin, then these enriched dirs, then
/// inherited PATH last).
pub fn enriched_tool_dirs() -> Vec<PathBuf> {
    let mut dirs: Vec<PathBuf> = Vec::new();
    let mut seen: HashSet<PathBuf> = HashSet::new();

    let home = home_dir();

    if cfg!(windows) {
        if let Some(appdata) = std::env::var_os("APPDATA") {
            push_dir(&mut dirs, &mut seen, PathBuf::from(&appdata).join("npm"));
        }
        if let Some(home) = &home {
            push_dir(&mut dirs, &mut seen, home.join(".npm-global"));
        }
    } else {
        // Add common Unix/macOS bin directories
        for p in [
            "/usr/local/bin",
            "/usr/bin",
            "/bin",
            "/usr/sbin",
            "/sbin",
            "/opt/homebrew/bin",
            "/opt/homebrew/sbin",
            "/usr/local/opt/node/bin",
        ] {
            push_dir(&mut dirs, &mut seen, PathBuf::from(p));
        }

        // Add user-local tool directories
        if let Some(home) = &home {
            for sub in [
                [".npm-global", "bin"],
                [".npm-packages", "bin"],
                [".local", "bin"],
                [".volta", "bin"],
            ] {
                push_dir(&mut dirs, &mut seen, home.join(sub[0]).join(sub[1]));
            }
            push_dir(&mut dirs, &mut seen, home.join(".asdf").join("shims"));
        }
    }

    // Add all nvm-managed node versions
    if let Some(home) = &home {
        let nvm_dir = home.join(".nvm").join("versions").join("node");
        if let Ok(entries) = std::fs::read_dir(&nvm_dir) {
            for entry in entries.flatten() {
                push_dir(&mut dirs, &mut seen, entry.path().join("bin"));
            }
        }
    }

    dirs
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enhanced_path_dirs_includes_current_path() {
        let dirs = enhanced_path_dirs();
        // Should at least include the directories from current PATH
        assert!(!dirs.is_empty());
    }

    #[test]
    fn enhanced_path_dirs_deduplicates() {
        let dirs = enhanced_path_dirs();
        let mut seen = HashSet::new();
        for dir in &dirs {
            assert!(
                seen.insert(dir),
                "Directory {:?} appears multiple times",
                dir
            );
        }
    }

    #[test]
    #[cfg(not(windows))]
    fn enhanced_path_dirs_includes_common_unix_dirs() {
        let dirs = enhanced_path_dirs();
        let dir_strs: Vec<String> = dirs
            .iter()
            .map(|p| p.to_string_lossy().to_string())
            .collect();

        // Should include at least some common Unix directories
        let has_usr_bin = dir_strs.iter().any(|d| d.contains("/usr/bin"));
        let has_bin = dir_strs.iter().any(|d| d.ends_with("/bin"));

        assert!(
            has_usr_bin || has_bin,
            "Should include common Unix bin directories"
        );
    }
}
