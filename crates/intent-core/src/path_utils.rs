//! Shared PATH enrichment utilities for packaged app environments.
//!
//! When launched via Finder/launchd, macOS apps inherit a minimal PATH
//! (/usr/bin:/bin:/usr/sbin:/sbin). This module provides utilities to
//! enrich PATH with common development tool directories (node, nvm, homebrew,
//! volta, asdf, etc.) so that tools and their dependencies can be discovered.

use std::collections::HashSet;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::OnceLock;
use std::time::Duration;

use directories::BaseDirs;

fn home_dir() -> Option<PathBuf> {
    BaseDirs::new().map(|b| b.home_dir().to_path_buf())
}

/// Cached login-shell PATH directories. Runs at most once, with a 2s timeout.
static LOGIN_SHELL_DIRS: OnceLock<Vec<PathBuf>> = OnceLock::new();

/// Capture PATH from the user's login shell (unix only, cached, short timeout).
/// On failure (timeout, spawn error, no $SHELL, non-unix), returns an empty vec.
/// Exposed for testing via an injectable shell path.
#[cfg(unix)]
fn capture_login_shell_path_with(shell: Option<&str>) -> Vec<PathBuf> {
    use std::io::Read;

    let shell = match shell {
        Some(s) => s.to_string(),
        None => match std::env::var("SHELL") {
            Ok(s) if !s.is_empty() => s,
            _ => return Vec::new(),
        },
    };

    // Run shell -lc 'printf %s "$PATH"' with 2s timeout
    let mut child = match Command::new(&shell)
        .args(["-lc", r#"printf %s "$PATH""#])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
    {
        Ok(c) => c,
        Err(_) => return Vec::new(),
    };

    // Poll for completion with 2s timeout
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                // Process exited - read stdout directly (can't use wait_with_output after try_wait succeeds)
                if !status.success() {
                    return Vec::new();
                }
                let mut stdout = match child.stdout.take() {
                    Some(s) => s,
                    None => return Vec::new(),
                };
                let mut output = Vec::new();
                if stdout.read_to_end(&mut output).is_err() {
                    return Vec::new();
                }
                let path_str = String::from_utf8_lossy(&output);
                return std::env::split_paths(&path_str.as_ref()).collect::<Vec<_>>();
            }
            Ok(None) => {
                // Still running
                if std::time::Instant::now() >= deadline {
                    // Timeout - kill and return empty
                    let _ = child.kill();
                    let _ = child.wait();
                    return Vec::new();
                }
                // Sleep a bit before polling again
                std::thread::sleep(Duration::from_millis(50));
            }
            Err(_) => return Vec::new(),
        }
    }
}

#[cfg(not(unix))]
fn capture_login_shell_path_with(_shell: Option<&str>) -> Vec<PathBuf> {
    Vec::new()
}

/// Returns cached login-shell PATH directories (unix only).
/// Runs the shell at most once, with a 2s timeout, and degrades silently on failure.
pub fn login_shell_dirs() -> &'static [PathBuf] {
    LOGIN_SHELL_DIRS.get_or_init(|| capture_login_shell_path_with(None))
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
    enriched_tool_dirs_with(login_shell_dirs)
}

/// Injectable variant for testing - accepts a function that returns login-shell dirs.
/// This allows tests to avoid spawning the real shell.
fn enriched_tool_dirs_with<F>(login_dirs_fn: F) -> Vec<PathBuf>
where
    F: FnOnce() -> &'static [PathBuf],
{
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

    // Add login-shell PATH directories (unix only, cached, silent degradation)
    for dir in login_dirs_fn() {
        push_dir(&mut dirs, &mut seen, dir.clone());
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

    #[test]
    #[cfg(unix)]
    fn capture_login_shell_path_with_fake_shell() {
        // Create a fake shell script that outputs a known PATH
        use std::fs;
        use std::os::unix::fs::PermissionsExt;

        let temp_dir = std::env::temp_dir();
        let pid = std::process::id();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let fake_shell = temp_dir.join(format!("fake_shell_test_{pid}_{nanos}.sh"));
        fs::write(
            &fake_shell,
            "#!/bin/sh\nif [ \"$1\" = \"-lc\" ]; then\n  printf '/custom/bin:/other/bin'\nfi\n",
        )
        .unwrap();
        fs::set_permissions(&fake_shell, fs::Permissions::from_mode(0o755)).unwrap();

        let dirs = capture_login_shell_path_with(Some(fake_shell.to_str().unwrap()));
        fs::remove_file(&fake_shell).ok();

        assert_eq!(dirs.len(), 2);
        assert_eq!(dirs[0], PathBuf::from("/custom/bin"));
        assert_eq!(dirs[1], PathBuf::from("/other/bin"));
    }

    #[test]
    #[cfg(unix)]
    fn capture_login_shell_path_with_invalid_shell_degrades_silently() {
        let dirs = capture_login_shell_path_with(Some("/nonexistent/shell"));
        assert!(dirs.is_empty());
    }

    #[test]
    #[cfg(unix)]
    fn capture_login_shell_path_with_empty_shell_string_degrades_silently() {
        let dirs = capture_login_shell_path_with(Some(""));
        assert!(dirs.is_empty());
    }

    #[test]
    fn enriched_tool_dirs_includes_login_shell_dirs() {
        use std::sync::LazyLock;

        // Use the injectable variant with known fake login-shell dirs
        // to verify that login-shell dirs are actually included
        static FAKE_LOGIN_DIRS: LazyLock<Vec<PathBuf>> = LazyLock::new(|| {
            vec![
                PathBuf::from("/fake/login/bin"),
                PathBuf::from("/another/fake/bin"),
            ]
        });

        let dirs = enriched_tool_dirs_with(|| &FAKE_LOGIN_DIRS);

        // Verify the fake login-shell dirs actually appear in the result
        assert!(
            dirs.contains(&PathBuf::from("/fake/login/bin")),
            "Login-shell dir /fake/login/bin should be included"
        );
        assert!(
            dirs.contains(&PathBuf::from("/another/fake/bin")),
            "Login-shell dir /another/fake/bin should be included"
        );
        // Should also include the hardcoded dirs
        assert!(!dirs.is_empty());
    }
}
