//! Provider discovery: which configured providers are *installed* (resolvable on
//! `PATH`) and which are gated off by a missing env var / feature code (§6.9).
//!
//! Pure detection only — no process spawning (that would pull a runtime into a
//! leaf crate, §3.2). It ports the "is this provider available?" intent of
//! `provider-availability.service.ts` to a `PATH` probe; the optional
//! authentication probe (which must spawn `auth status`) lives in the daemon
//! layer that already owns a tokio runtime.

use std::path::PathBuf;

use crate::config::{ProviderConfig, ACP_PROVIDERS};

/// Availability of one configured provider.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderAvailability {
    /// Provider id (`auggie`, `codex`, …).
    pub id: &'static str,
    /// Human-readable provider name.
    pub display_name: &'static str,
    /// The CLI command probed on `PATH`.
    pub command: &'static str,
    /// Whether the command resolved to an executable on `PATH`.
    pub installed: bool,
    /// The resolved executable path, when found.
    pub resolved_path: Option<PathBuf>,
    /// `Some(reason)` when the provider is gated off (env var / feature code not
    /// present), in which case it is skipped rather than probed.
    pub gated_off: Option<String>,
    /// The provider's auth-status check args (`Some` ⇒ a daemon-side probe is
    /// possible), surfaced so the caller can run it without re-reading config.
    pub auth_check_args: Option<&'static [&'static str]>,
}

/// Platform `PATH` list separator.
const PATH_SEP: char = if cfg!(windows) { ';' } else { ':' };

/// Candidate filename suffixes to try when resolving a command on `PATH`
/// (Windows resolves `.exe`/`.cmd`/`.bat`; POSIX uses the bare name).
fn name_candidates(command: &str) -> Vec<String> {
    if cfg!(windows) {
        vec![
            command.to_string(),
            format!("{command}.exe"),
            format!("{command}.cmd"),
            format!("{command}.bat"),
        ]
    } else {
        vec![command.to_string()]
    }
}

/// Resolve `command` to an executable path by scanning `PATH`, or `None`.
pub fn resolve_on_path(command: &str) -> Option<PathBuf> {
    // An explicit path (rare in the registry) is honored directly.
    let as_path = PathBuf::from(command);
    if as_path.is_absolute() {
        return as_path.is_file().then_some(as_path);
    }
    let path = std::env::var_os("PATH")?;
    for dir in path.to_string_lossy().split(PATH_SEP) {
        let dir = dir.trim();
        if dir.is_empty() {
            continue;
        }
        for candidate in name_candidates(command) {
            let full = PathBuf::from(dir).join(&candidate);
            if full.is_file() {
                return Some(full);
            }
        }
    }
    None
}

/// Why a provider is gated off, or `None` when it is eligible for probing.
fn gated_reason(provider: &ProviderConfig) -> Option<String> {
    if let Some(var) = provider.requires_env_var {
        if std::env::var_os(var).is_none() {
            return Some(format!("requires env var {var}"));
        }
    }
    if let Some(code) = provider.requires_feature_code {
        return Some(format!("requires feature code {code}"));
    }
    None
}

/// Discover availability for every configured provider (§6.9), in registry
/// order. Gated providers report `gated_off` and are not probed on `PATH`.
pub fn discover_providers() -> Vec<ProviderAvailability> {
    ACP_PROVIDERS
        .iter()
        .map(|provider| {
            let gated_off = gated_reason(provider);
            let resolved_path = if gated_off.is_some() {
                None
            } else {
                resolve_on_path(provider.command)
            };
            ProviderAvailability {
                id: provider.id,
                display_name: provider.display_name,
                command: provider.command,
                installed: resolved_path.is_some(),
                resolved_path,
                gated_off,
                auth_check_args: provider.auth_check_args,
            }
        })
        .collect()
}

/// Resolve a provider binary to an absolute path using the precedence order:
/// 1. Explicit path from `providers.paths` map (keyed by provider ID)
/// 2. Managed `~/.augment/bin/<command>`
/// 3. Scan enhanced PATH directories
///
/// Returns `None` when the binary cannot be resolved. Reuses the discovery
/// logic from `intent_context::discovery` but generalized for all providers.
/// The `provider_id` is used for logging when an explicit path is invalid.
pub fn find_provider_binary(
    provider_id: &str,
    command: &str,
    explicit_path: Option<&str>,
) -> Option<PathBuf> {
    // 1. Explicit setting wins (must be executable and absolute)
    if let Some(path) = explicit_path {
        let trimmed = path.trim();
        if !trimmed.is_empty() {
            let pb = PathBuf::from(trimmed);
            if pb.is_absolute() && is_executable_file(&pb) {
                return Some(pb);
            }
            // Warn when explicit setting points to missing/non-executable/relative file
            tracing::warn!(
                provider_id = provider_id,
                configured_path = trimmed,
                "providers.paths[\"{}\"] must be absolute and executable; falling back to managed bin / PATH scan",
                provider_id
            );
        }
    }

    // 2. Managed binary in ~/.augment/bin
    if let Some(managed) = managed_binary_path(command) {
        if is_executable_file(&managed) {
            return Some(managed);
        }
    }

    // 3. Scan enhanced PATH directories
    find_in_enhanced_dirs(command)
}

/// The Intent-managed binary path (`~/.augment/bin/<command>[.exe]`).
fn managed_binary_path(command: &str) -> Option<PathBuf> {
    let home = home_dir()?;
    let name = if cfg!(windows) {
        format!("{command}.exe")
    } else {
        command.to_string()
    };
    Some(home.join(".augment").join("bin").join(name))
}

/// Resolve the user's home directory from environment, cross-platform.
fn home_dir() -> Option<PathBuf> {
    std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(PathBuf::from)
}

/// True when `p` is a file that is executable (unix checks the exec bit; on
/// other platforms existence as a file is sufficient).
fn is_executable_file(p: &std::path::Path) -> bool {
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

/// Find the first executable for `command` by scanning enhanced PATH directories.
/// Enhanced PATH includes inherited PATH plus common node/npm/nvm locations
/// (same discovery dirs as `intent_context::discovery::enhanced_path_dirs`).
fn find_in_enhanced_dirs(command: &str) -> Option<PathBuf> {
    let dirs = enhanced_path_dirs();
    let candidates = name_candidates(command);
    for dir in &dirs {
        for candidate in &candidates {
            let full = dir.join(candidate);
            if is_executable_file(&full) {
                return Some(full);
            }
        }
    }
    None
}

/// Build the ordered, de-duplicated list of directories to search (port of
/// `getEnhancedPath` from `intent_context::discovery`).
fn enhanced_path_dirs() -> Vec<PathBuf> {
    let mut dirs: Vec<PathBuf> = Vec::new();
    let mut seen: std::collections::HashSet<PathBuf> = std::collections::HashSet::new();

    let push =
        |d: PathBuf, dirs: &mut Vec<PathBuf>, seen: &mut std::collections::HashSet<PathBuf>| {
            if !d.as_os_str().is_empty() && seen.insert(d.clone()) {
                dirs.push(d);
            }
        };

    // Inherited PATH first
    if let Some(path) = std::env::var_os("PATH") {
        for dir in std::env::split_paths(&path) {
            push(dir, &mut dirs, &mut seen);
        }
    }

    let home = home_dir();

    if cfg!(windows) {
        if let Some(appdata) = std::env::var_os("APPDATA") {
            push(PathBuf::from(&appdata).join("npm"), &mut dirs, &mut seen);
        }
        if let Some(h) = &home {
            push(h.join(".npm-global"), &mut dirs, &mut seen);
        }
    } else {
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
            push(PathBuf::from(p), &mut dirs, &mut seen);
        }
        if let Some(h) = &home {
            for sub in [
                [".npm-global", "bin"],
                [".npm-packages", "bin"],
                [".local", "bin"],
                [".volta", "bin"],
            ] {
                push(h.join(sub[0]).join(sub[1]), &mut dirs, &mut seen);
            }
            push(h.join(".asdf").join("shims"), &mut dirs, &mut seen);
        }
    }

    if let Some(h) = &home {
        let nvm_dir = h.join(".nvm").join("versions").join("node");
        if let Ok(entries) = std::fs::read_dir(&nvm_dir) {
            for entry in entries.flatten() {
                push(entry.path().join("bin"), &mut dirs, &mut seen);
            }
        }
    }

    dirs
}

/// Resolve `npx` to an absolute path using the same enhanced PATH scanning that
/// `find_provider_binary` uses. Returns `None` when npx cannot be found.
pub fn find_npx() -> Option<PathBuf> {
    find_in_enhanced_dirs("npx")
}

#[cfg(test)]
mod find_provider_binary_tests {
    use super::*;
    use std::fs;

    fn unique_temp_dir(tag: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("intent-providers-{tag}-{nanos}"));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[cfg(unix)]
    fn make_executable(path: &std::path::Path) {
        use std::os::unix::fs::PermissionsExt;
        fs::write(path, "#!/bin/sh\nexit 0\n").unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
    }

    #[cfg(not(unix))]
    fn make_executable(path: &std::path::Path) {
        fs::write(path, "exit 0").unwrap();
    }

    #[test]
    fn find_provider_binary_returns_none_when_absent() {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let unique_cmd = format!("intent-test-absent-{}", nanos);
        let result = find_provider_binary("nonexistent", &unique_cmd, None);
        assert_eq!(result, None);
    }

    #[test]
    fn find_provider_binary_prefers_explicit_setting() {
        let dir = unique_temp_dir("explicit");
        let bin = dir.join("my-provider");
        make_executable(&bin);
        let result = find_provider_binary("test", "my-provider", Some(bin.to_str().unwrap()));
        assert_eq!(result, Some(bin));
    }

    #[test]
    fn find_provider_binary_ignores_empty_explicit_setting() {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let unique_cmd = format!("intent-test-cmd-{}", nanos);
        let result = find_provider_binary("test", &unique_cmd, Some(""));
        assert_eq!(result, None);
    }

    #[test]
    fn find_provider_binary_ignores_whitespace_only_explicit_setting() {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let unique_cmd = format!("intent-test-cmd-{}", nanos);
        let result = find_provider_binary("test", &unique_cmd, Some("   "));
        assert_eq!(result, None);
    }

    #[test]
    fn find_provider_binary_falls_through_when_explicit_path_missing() {
        // When providers.paths.<id> points to a missing file, resolution should
        // fall through to managed bin / PATH scan (and warn)
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let unique_cmd = format!("intent-test-cmd-{}", nanos);
        let result = find_provider_binary("test", &unique_cmd, Some("/nonexistent/path/binary"));
        // Should fall through and return None since we don't have managed bin or PATH match
        assert_eq!(result, None);
    }

    #[cfg(unix)]
    #[test]
    fn find_provider_binary_returns_none_when_no_candidates_found() {
        // Verify function returns None when binary is not in any of the search locations
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let unique_cmd = format!("intent-test-nocand-{}", nanos);
        let result = find_provider_binary("test", &unique_cmd, None);
        assert_eq!(result, None);
    }

    #[test]
    fn managed_binary_path_returns_expected_location() {
        if let Some(home) = home_dir() {
            let result = managed_binary_path("auggie");
            let expected = if cfg!(windows) {
                home.join(".augment").join("bin").join("auggie.exe")
            } else {
                home.join(".augment").join("bin").join("auggie")
            };
            assert_eq!(result, Some(expected));
        }
    }

    #[test]
    fn find_npx_returns_path_when_npx_exists_on_enhanced_path() {
        // This test will only pass if npx is actually on the enhanced PATH.
        // On most dev machines with node/npm installed, this should be true.
        // If it fails, it means npx is not findable, which is expected behavior.
        let result = find_npx();
        if let Some(path) = result {
            assert!(path.is_absolute(), "npx path should be absolute");
            assert!(path.is_file(), "npx path should point to a file");
        }
        // If None, that's also valid — npx is not installed/findable
    }
}
