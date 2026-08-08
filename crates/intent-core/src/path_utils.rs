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

/// Cached login-shell PATH directories. Runs at most once, with a 5s timeout.
static LOGIN_SHELL_DIRS: OnceLock<Vec<PathBuf>> = OnceLock::new();

/// Sentinels for extracting PATH from potentially noisy shell output.
#[cfg(unix)]
const PATH_START_SENTINEL: &str = "__INTENT_PATH_S__";
#[cfg(unix)]
const PATH_END_SENTINEL: &str = "__INTENT_PATH_E__";

/// Capture PATH from the user's login shell (unix only, cached, short timeout).
/// On failure (timeout, spawn error, no shell, non-unix), returns an empty vec.
/// Exposed for testing via an injectable shell path.
#[cfg(unix)]
fn capture_login_shell_path_with(shell: Option<&str>) -> Vec<PathBuf> {
    let shell = match shell {
        Some(explicit) => match (!explicit.is_empty()).then(|| explicit.to_string()) {
            Some(shell) => shell,
            None => return Vec::new(),
        },
        None => match login_shell() {
            Some(shell) => shell,
            None => return Vec::new(),
        },
    };

    // Try interactive login shell first (-ilc), fall back to login shell (-lc)
    // Interactive shells source ~/.zshrc and similar rc files that may add PATH entries
    if let Some(dirs) = try_capture_with_flags(&shell, &["-ilc"]) {
        return dirs;
    }

    // Fallback to non-interactive login shell
    try_capture_with_flags(&shell, &["-lc"]).unwrap_or_default()
}

/// Resolve the user's login shell: the `SHELL` env var, else the user
/// database (`getpwuid`, unix only), else `/bin/zsh` on macOS (Finder/launchd
/// omit `SHELL`, and the user-db lookup can still fail), else `None`.
///
/// This is the single source of truth for login-shell resolution: both the
/// login-shell PATH capture in this module and the `host.env` probe consume
/// it, so the reported shell and the enrichment shell always agree.
pub fn login_shell() -> Option<String> {
    let env_shell = std::env::var_os("SHELL");
    let env_shell = env_shell.as_deref().filter(|shell| !shell.is_empty());
    // Only pay for the user-db lookup (a potential NSS/LDAP round-trip) when
    // the env var is missing or empty.
    let user_db = if env_shell.is_some() {
        None
    } else {
        user_db_shell()
    };
    resolve_login_shell(env_shell, user_db.as_deref())
}

/// Pure resolution core for [`login_shell`], injectable for tests.
fn resolve_login_shell(
    env_shell: Option<&std::ffi::OsStr>,
    user_db_shell: Option<&str>,
) -> Option<String> {
    if let Some(shell) = env_shell.filter(|shell| !shell.is_empty()) {
        return Some(shell.to_string_lossy().into_owned());
    }
    if let Some(shell) = user_db_shell.filter(|shell| !shell.is_empty()) {
        return Some(shell.to_string());
    }
    cfg!(target_os = "macos").then(|| "/bin/zsh".to_string())
}

/// Look up the current user's login shell in the user database via
/// `getpwuid_r`. Returns `None` on lookup failure or a null, empty, or
/// non-UTF-8 `pw_shell`.
#[cfg(unix)]
fn user_db_shell() -> Option<String> {
    use std::ffi::CStr;

    let mut pwd: libc::passwd = unsafe { std::mem::zeroed() };
    let mut result: *mut libc::passwd = std::ptr::null_mut();
    let mut buf = vec![0 as libc::c_char; 1024];
    loop {
        let rc = unsafe {
            libc::getpwuid_r(
                libc::getuid(),
                &mut pwd,
                buf.as_mut_ptr(),
                buf.len(),
                &mut result,
            )
        };
        if rc == libc::ERANGE && buf.len() < (1 << 20) {
            let doubled = buf.len() * 2;
            buf.resize(doubled, 0);
            continue;
        }
        if rc != 0 || result.is_null() {
            return None;
        }
        // On success `result` points at `pwd`, which getpwuid_r filled in, so
        // read the field from `pwd` rather than dereferencing the raw pointer.
        if pwd.pw_shell.is_null() {
            return None;
        }
        let shell = unsafe { CStr::from_ptr(pwd.pw_shell) };
        return shell
            .to_str()
            .ok()
            .filter(|shell| !shell.is_empty())
            .map(str::to_string);
    }
}

#[cfg(not(unix))]
fn user_db_shell() -> Option<String> {
    None
}

/// Helper to attempt PATH capture with specific shell flags.
/// Returns Some(dirs) on success, None on any failure (spawn, timeout, non-zero exit, missing sentinels).
#[cfg(unix)]
fn try_capture_with_flags(shell: &str, flags: &[&str]) -> Option<Vec<PathBuf>> {
    use std::io::Read;
    use std::sync::{Arc, Mutex};

    // Build command with sentinel-wrapped printf
    let cmd = format!(
        r#"printf '{}%s{}'  "$PATH""#,
        PATH_START_SENTINEL, PATH_END_SENTINEL
    );
    let mut args = flags.to_vec();
    args.push(&cmd);

    let mut child = Command::new(shell)
        .args(&args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .ok()?;

    // Drain stdout concurrently to avoid pipe-buffer deadlock when rc files print >64KB of noise
    let stdout = child.stdout.take()?;
    let output_buffer = Arc::new(Mutex::new(Vec::new()));
    let output_clone = output_buffer.clone();
    let reader_thread = std::thread::spawn(move || {
        let mut stdout = stdout;
        let mut buf = Vec::new();
        let _ = stdout.read_to_end(&mut buf);
        let mut out = output_clone.lock().unwrap();
        *out = buf;
    });

    // Poll for completion with 5s timeout (interactive shells with nvm can take ~1.9s)
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let exit_status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break Some(status),
            Ok(None) => {
                // Still running
                if std::time::Instant::now() >= deadline {
                    // Timeout - kill and return None
                    let _ = child.kill();
                    let _ = child.wait();
                    break None;
                }
                // Sleep a bit before polling again
                std::thread::sleep(Duration::from_millis(50));
            }
            Err(_) => break None,
        }
    };

    // Wait for reader thread to finish
    let _ = reader_thread.join();

    // Check exit status
    let status = exit_status?;
    if !status.success() {
        return None;
    }

    // Extract output from buffer
    let output = output_buffer.lock().unwrap();
    let output_str = String::from_utf8_lossy(&output);

    // Extract PATH between sentinels (last complete pair wins if sentinels appear multiple times)
    let path_str = extract_path_from_sentinels(&output_str)?;

    // Filter to absolute paths only to avoid unsafe relative entries like "." or "bin"
    Some(
        std::env::split_paths(path_str)
            .filter(|p| p.is_absolute())
            .collect::<Vec<_>>(),
    )
}

/// Extract PATH value from between sentinels in shell output.
/// Returns Some(path_str) if both sentinels found, None otherwise.
/// If sentinels appear multiple times, uses the last complete pair (last END, then last START before it).
#[cfg(unix)]
fn extract_path_from_sentinels(output: &str) -> Option<&str> {
    // Find the last END sentinel
    let end_idx = output.rfind(PATH_END_SENTINEL)?;
    // Find the last START sentinel before that END
    let start_pos = output[..end_idx].rfind(PATH_START_SENTINEL)? + PATH_START_SENTINEL.len();
    Some(&output[start_pos..end_idx])
}

#[cfg(not(unix))]
fn capture_login_shell_path_with(_shell: Option<&str>) -> Vec<PathBuf> {
    Vec::new()
}

/// Returns cached login-shell PATH directories (unix only).
/// Runs the shell at most once, with a 5s timeout, and degrades silently on failure.
pub(crate) fn login_shell_dirs() -> &'static [PathBuf] {
    LOGIN_SHELL_DIRS.get_or_init(|| capture_login_shell_path_with(None))
}

/// Force the login-shell PATH capture (`$SHELL -ilc`, falling back to `-lc`;
/// up to ~5s per attempt) to run now instead of on the first caller. Intended
/// to be spawned off the async runtime (e.g. via `tokio::task::spawn_blocking`)
/// at daemon startup so the first `host.providerDiscovery` / `host.findBinary`
/// / `host.toolAvailability` RPC does not pay the cold-shell cost. Idempotent
/// — the underlying `OnceLock` runs the shell probe at most once per process,
/// so a redundant call (or one racing an on-demand caller) is a no-op.
pub fn prewarm_login_shell_path() {
    login_shell_dirs();
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
/// then ~/.augment/bin for auggie, then enriched dirs, then inherited PATH) should
/// use `enriched_tool_dirs()` + split_paths to build the order themselves.
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
/// provider-binary dir and ~/.augment/bin for auggie, then these enriched dirs,
/// then inherited PATH last).
pub fn enriched_tool_dirs() -> Vec<PathBuf> {
    enriched_tool_dirs_with_home(home_dir().as_deref())
}

/// Variant of [`enriched_tool_dirs`] with the home directory injected instead
/// of resolved from the environment. Lets tests point the user-local tool
/// directories (`~/.local/bin`, `~/.nvm`, …) at a scratch home without
/// mutating process-global `HOME`, which races parallel tests.
pub fn enriched_tool_dirs_with_home(home: Option<&std::path::Path>) -> Vec<PathBuf> {
    enriched_tool_dirs_impl(home, login_shell_dirs)
}

/// Injectable core - accepts the home directory and a function that returns
/// login-shell dirs, so tests can avoid spawning the real shell.
fn enriched_tool_dirs_impl<F>(home: Option<&std::path::Path>, login_dirs_fn: F) -> Vec<PathBuf>
where
    F: FnOnce() -> &'static [PathBuf],
{
    let mut dirs: Vec<PathBuf> = Vec::new();
    let mut seen: HashSet<PathBuf> = HashSet::new();

    if cfg!(windows) {
        if let Some(appdata) = std::env::var_os("APPDATA") {
            push_dir(&mut dirs, &mut seen, PathBuf::from(&appdata).join("npm"));
        }
        if let Some(home) = home {
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
        if let Some(home) = home {
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
    if let Some(home) = home {
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

    #[cfg(unix)]
    fn write_fake_shell(path: &std::path::Path, content: &str) {
        use std::fs;
        use std::io::Write;
        use std::os::unix::fs::PermissionsExt;

        // Use File::create + write_all + sync_all + drop + set_permissions
        // to ensure file is fully written and flushed before execution (avoids TOCTOU flakes)
        let mut file = fs::File::create(path).unwrap();
        file.write_all(content.as_bytes()).unwrap();
        file.sync_all().unwrap();
        drop(file); // Ensure file is closed before setting permissions
        fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
    }

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
        // Create a fake shell script that outputs a known PATH with sentinel markers
        use std::fs;

        let temp_dir = std::env::temp_dir();
        let pid = std::process::id();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let fake_shell = temp_dir.join(format!("fake_shell_test_{pid}_{nanos}.sh"));

        // Script responds to -ilc with sentinel-wrapped PATH
        write_fake_shell(
            &fake_shell,
            "#!/bin/sh\nif [ \"$1\" = \"-ilc\" ]; then\n  printf '__INTENT_PATH_S__/custom/bin:/other/bin__INTENT_PATH_E__'\nfi\n",
        );

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
    fn resolve_login_shell_prefers_env_shell() {
        assert_eq!(
            resolve_login_shell(Some(std::ffi::OsStr::new("/bin/fish")), Some("/bin/bash"))
                .as_deref(),
            Some("/bin/fish")
        );
    }

    #[test]
    fn resolve_login_shell_uses_user_db_when_env_missing() {
        assert_eq!(
            resolve_login_shell(None, Some("/usr/local/bin/fish")).as_deref(),
            Some("/usr/local/bin/fish")
        );
    }

    #[test]
    fn resolve_login_shell_skips_empty_env_shell() {
        assert_eq!(
            resolve_login_shell(Some(std::ffi::OsStr::new("")), Some("/bin/bash")).as_deref(),
            Some("/bin/bash")
        );
    }

    #[test]
    #[cfg(target_os = "macos")]
    fn resolve_login_shell_empty_user_db_falls_back_to_zsh_on_macos() {
        assert_eq!(
            resolve_login_shell(None, Some("")).as_deref(),
            Some("/bin/zsh")
        );
    }

    #[test]
    #[cfg(target_os = "macos")]
    fn missing_shell_falls_back_to_macos_login_shell() {
        assert_eq!(resolve_login_shell(None, None).as_deref(), Some("/bin/zsh"));
    }

    #[test]
    #[cfg(not(target_os = "macos"))]
    fn missing_shell_fails_open_on_non_macos() {
        assert_eq!(resolve_login_shell(None, None), None);
        assert_eq!(resolve_login_shell(None, Some("")), None);
    }

    #[test]
    #[cfg(unix)]
    fn user_db_shell_never_returns_empty() {
        // The lookup itself may legitimately fail (e.g. minimal containers),
        // but a successful lookup must never yield an empty shell.
        if let Some(shell) = user_db_shell() {
            assert!(!shell.is_empty());
        }
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

        let dirs = enriched_tool_dirs_impl(home_dir().as_deref(), || &FAKE_LOGIN_DIRS);

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

    #[test]
    #[cfg(unix)]
    fn capture_login_shell_path_extracts_sentinels_amid_noise() {
        // Test that sentinel extraction works even when rc files print noise to stdout
        use std::fs;

        let temp_dir = std::env::temp_dir();
        let pid = std::process::id();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let fake_shell = temp_dir.join(format!("fake_shell_noise_{pid}_{nanos}.sh"));

        // Script prints noise before and after the sentinel-wrapped PATH
        write_fake_shell(
            &fake_shell,
            "#!/bin/sh\nif [ \"$1\" = \"-ilc\" ]; then\n  echo 'Loading nvm...'\n  echo 'nvm initialized'\n  printf '__INTENT_PATH_S__/noise/bin:/test/bin__INTENT_PATH_E__'\n  echo 'Shell ready'\nfi\n",
        );

        let dirs = capture_login_shell_path_with(Some(fake_shell.to_str().unwrap()));
        fs::remove_file(&fake_shell).ok();

        assert_eq!(dirs.len(), 2);
        assert_eq!(dirs[0], PathBuf::from("/noise/bin"));
        assert_eq!(dirs[1], PathBuf::from("/test/bin"));
    }

    #[test]
    #[cfg(unix)]
    fn capture_login_shell_path_missing_sentinels_degrades_to_empty() {
        // Test that missing sentinels result in empty vec (not a panic or crash)
        use std::fs;

        let temp_dir = std::env::temp_dir();
        let pid = std::process::id();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let fake_shell = temp_dir.join(format!("fake_shell_no_sentinel_{pid}_{nanos}.sh"));

        // Script outputs PATH without sentinels
        write_fake_shell(
            &fake_shell,
            "#!/bin/sh\nif [ \"$1\" = \"-ilc\" ]; then\n  printf '/missing/bin:/sentinels/bin'\nfi\n",
        );

        let dirs = capture_login_shell_path_with(Some(fake_shell.to_str().unwrap()));
        fs::remove_file(&fake_shell).ok();

        assert!(
            dirs.is_empty(),
            "Should degrade to empty vec when sentinels are missing"
        );
    }

    #[test]
    #[cfg(unix)]
    fn capture_login_shell_path_falls_back_to_lc_when_ilc_fails() {
        // Test that -ilc failure triggers -lc fallback
        use std::fs;

        let temp_dir = std::env::temp_dir();
        let pid = std::process::id();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let fake_shell = temp_dir.join(format!("fake_shell_fallback_{pid}_{nanos}.sh"));

        // Script fails on -ilc but succeeds on -lc
        write_fake_shell(
            &fake_shell,
            "#!/bin/sh\nif [ \"$1\" = \"-ilc\" ]; then\n  exit 1\nelif [ \"$1\" = \"-lc\" ]; then\n  printf '__INTENT_PATH_S__/fallback/bin:/backup/bin__INTENT_PATH_E__'\nfi\n",
        );

        let dirs = capture_login_shell_path_with(Some(fake_shell.to_str().unwrap()));
        fs::remove_file(&fake_shell).ok();

        assert_eq!(dirs.len(), 2, "Should fall back to -lc when -ilc fails");
        assert_eq!(dirs[0], PathBuf::from("/fallback/bin"));
        assert_eq!(dirs[1], PathBuf::from("/backup/bin"));
    }

    #[test]
    #[cfg(unix)]
    fn capture_login_shell_path_uses_last_sentinel_occurrence() {
        // Test that when sentinels appear multiple times, we use the last occurrence
        use std::fs;

        let temp_dir = std::env::temp_dir();
        let pid = std::process::id();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let fake_shell = temp_dir.join(format!("fake_shell_multi_{pid}_{nanos}.sh"));

        // Script outputs sentinels multiple times
        write_fake_shell(
            &fake_shell,
            "#!/bin/sh\nif [ \"$1\" = \"-ilc\" ]; then\n  printf '__INTENT_PATH_S__/first/bin__INTENT_PATH_E__'\n  printf '__INTENT_PATH_S__/last/bin:/final/bin__INTENT_PATH_E__'\nfi\n",
        );

        let dirs = capture_login_shell_path_with(Some(fake_shell.to_str().unwrap()));
        fs::remove_file(&fake_shell).ok();

        assert_eq!(dirs.len(), 2, "Should use last sentinel occurrence");
        assert_eq!(dirs[0], PathBuf::from("/last/bin"));
        assert_eq!(dirs[1], PathBuf::from("/final/bin"));
    }

    #[test]
    #[cfg(unix)]
    fn capture_login_shell_path_handles_trailing_incomplete_sentinel() {
        // Test that a trailing incomplete start sentinel after a complete pair is handled correctly
        use std::fs;

        let temp_dir = std::env::temp_dir();
        let pid = std::process::id();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let fake_shell = temp_dir.join(format!("fake_shell_trailing_{pid}_{nanos}.sh"));

        // Script outputs a complete pair followed by a bare start sentinel
        write_fake_shell(
            &fake_shell,
            "#!/bin/sh\nif [ \"$1\" = \"-ilc\" ]; then\n  printf '__INTENT_PATH_S__/valid/bin__INTENT_PATH_E__'\n  printf '__INTENT_PATH_S__'\nfi\n",
        );

        let dirs = capture_login_shell_path_with(Some(fake_shell.to_str().unwrap()));
        fs::remove_file(&fake_shell).ok();

        assert_eq!(
            dirs.len(),
            1,
            "Should extract from the complete pair, ignoring trailing bare start"
        );
        assert_eq!(dirs[0], PathBuf::from("/valid/bin"));
    }

    #[test]
    #[cfg(unix)]
    fn capture_login_shell_path_handles_large_output_without_deadlock() {
        // Test that output >64KB doesn't cause pipe-buffer deadlock
        use std::fs;

        let temp_dir = std::env::temp_dir();
        let pid = std::process::id();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let fake_shell = temp_dir.join(format!("fake_shell_large_{pid}_{nanos}.sh"));

        // Script prints >64KB of noise before the sentinel pair
        // 70,000 'X' characters = 70KB, well over typical pipe buffer size
        let noise = "X".repeat(70_000);
        let script = format!(
            "#!/bin/sh\nif [ \"$1\" = \"-ilc\" ]; then\n  printf '{}'\n  printf '__INTENT_PATH_S__/large/bin__INTENT_PATH_E__'\nfi\n",
            noise
        );
        write_fake_shell(&fake_shell, &script);

        let start = std::time::Instant::now();
        let dirs = capture_login_shell_path_with(Some(fake_shell.to_str().unwrap()));
        let elapsed = start.elapsed();
        fs::remove_file(&fake_shell).ok();

        assert_eq!(
            dirs.len(),
            1,
            "Should extract PATH even with >64KB of noise"
        );
        assert_eq!(dirs[0], PathBuf::from("/large/bin"));
        assert!(
            elapsed < Duration::from_secs(5),
            "Should complete well within timeout (no deadlock), took {:?}",
            elapsed
        );
    }
}
