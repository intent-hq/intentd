//! Shared PATH enrichment utilities for packaged app environments.
//!
//! When launched via Finder/launchd, macOS apps inherit a minimal PATH
//! (/usr/bin:/bin:/usr/sbin:/sbin). This module provides utilities to
//! enrich PATH with common development tool directories (node, nvm, homebrew,
//! volta, asdf, etc.) so that tools and their dependencies can be discovered.

use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::OnceLock;
use std::time::Duration;

use directories::BaseDirs;

fn home_dir() -> Option<PathBuf> {
    BaseDirs::new().map(|b| b.home_dir().to_path_buf())
}

/// Result of the one-shot login-shell capture: PATH directories plus the
/// allow-listed credential env vars, collected in the same shell invocation.
///
/// Deliberately does NOT derive `Debug`/`Display`: `credential_env` holds
/// secret values that must never be logged or formatted.
struct LoginShellCapture {
    dirs: Vec<PathBuf>,
    credential_env: BTreeMap<String, String>,
}

impl LoginShellCapture {
    fn empty() -> Self {
        Self {
            dirs: Vec::new(),
            credential_env: BTreeMap::new(),
        }
    }
}

/// Cached login-shell capture (PATH dirs + allow-listed credential env).
/// Runs at most once, with a 5s timeout.
static LOGIN_SHELL_CAPTURE: OnceLock<LoginShellCapture> = OnceLock::new();

/// Sentinels for extracting PATH from potentially noisy shell output.
#[cfg(unix)]
const PATH_START_SENTINEL: &str = "__INTENT_PATH_S__";
#[cfg(unix)]
const PATH_END_SENTINEL: &str = "__INTENT_PATH_E__";

/// Sentinels for extracting the NUL-separated `env -0` payload from the same
/// shell invocation as the PATH capture.
#[cfg(unix)]
const ENV_START_SENTINEL: &str = "__INTENT_ENV_S__";
#[cfg(unix)]
const ENV_END_SENTINEL: &str = "__INTENT_ENV_E__";

/// Credential env vars captured by exact name.
///
/// Inclusion criterion (both lists): a var is listed when a shipped provider
/// CLI (or its backing SDK) reads it for authentication/configuration —
/// exact names for cross-provider credentials (Anthropic/OpenAI/xAI keys,
/// AWS credentials for Bedrock, Hugging Face tokens), one prefix per
/// provider CLI's own env namespace. Keep both in sync with the provider
/// catalog (`intent-providers`) as providers are added or removed.
#[cfg(unix)]
const CREDENTIAL_ENV_EXACT: &[&str] = &[
    "ANTHROPIC_API_KEY",
    "OPENAI_API_KEY",
    "XAI_API_KEY",
    "AWS_PROFILE",
    "AWS_REGION",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "HF_TOKEN",
    "HUGGING_FACE_HUB_TOKEN",
];

/// Credential env vars captured by name prefix. See the inclusion criterion
/// on [`CREDENTIAL_ENV_EXACT`].
#[cfg(unix)]
const CREDENTIAL_ENV_PREFIXES: &[&str] = &[
    "AUGGIE_",
    "CLAUDE_",
    "CODEX_",
    "OPENCODE_",
    "DROID_",
    "CORTEX_",
    "GROK_",
    "PI_",
];

/// Whether an env var name is on the credential allow-list.
#[cfg(unix)]
fn is_credential_env_allow_listed(name: &str) -> bool {
    CREDENTIAL_ENV_EXACT.contains(&name)
        || CREDENTIAL_ENV_PREFIXES
            .iter()
            .any(|prefix| name.starts_with(prefix))
}

/// Parse a NUL-separated `env -0` payload into the allow-listed credential
/// vars. NUL separation tolerates values containing newlines. Non-allow-listed
/// vars are discarded here and never leave this function.
#[cfg(unix)]
fn parse_credential_env(payload: &str) -> BTreeMap<String, String> {
    let mut map = BTreeMap::new();
    for entry in payload.split('\0') {
        if let Some((name, value)) = entry.split_once('=') {
            if is_credential_env_allow_listed(name) {
                map.insert(name.to_string(), value.to_string());
            }
        }
    }
    map
}

/// Capture PATH and allow-listed credential env vars from the user's login
/// shell (unix only, cached, short timeout). On failure (timeout, spawn error,
/// no shell, non-unix), returns an empty capture.
/// Exposed for testing via an injectable shell path.
#[cfg(unix)]
fn capture_login_shell_with(shell: Option<&str>) -> LoginShellCapture {
    let shell = match shell {
        Some(explicit) => match (!explicit.is_empty()).then(|| explicit.to_string()) {
            Some(shell) => shell,
            None => return LoginShellCapture::empty(),
        },
        None => match login_shell() {
            Some(shell) => shell,
            None => return LoginShellCapture::empty(),
        },
    };

    // Try interactive login shell first (-ilc), fall back to login shell (-lc)
    // Interactive shells source ~/.zshrc and similar rc files that may add PATH entries
    if let Some(capture) = try_capture_with_flags(&shell, &["-ilc"]) {
        return capture;
    }

    // Fallback to non-interactive login shell
    try_capture_with_flags(&shell, &["-lc"]).unwrap_or_else(LoginShellCapture::empty)
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

/// Helper to attempt the login-shell capture with specific shell flags.
/// Returns Some(capture) on success, None on any failure (spawn, timeout,
/// non-zero exit, missing PATH sentinels). The env payload is optional: if its
/// sentinels are missing, the capture still succeeds with an empty env map.
#[cfg(unix)]
fn try_capture_with_flags(shell: &str, flags: &[&str]) -> Option<LoginShellCapture> {
    use std::io::Read;
    use std::sync::{Arc, Mutex};

    // Build command with sentinel-wrapped printf for PATH, plus a second
    // sentinel pair wrapping a NUL-separated env dump in the SAME invocation.
    // `env -0` is present on GNU coreutils and modern macOS (FreeBSD-derived
    // env); where it is missing, fall back to POSIX awk's ENVIRON, which
    // emits the same NUL-separated shape (`%c`, 0). `|| true` keeps an env
    // dump failure from failing the whole capture — the env payload is
    // optional and degrades to an empty map.
    let cmd = format!(
        r#"printf '{}%s{}'  "$PATH"; printf '{}'; /usr/bin/env -0 2>/dev/null || /usr/bin/awk 'BEGIN{{for(k in ENVIRON)printf "%s=%s%c",k,ENVIRON[k],0}}' 2>/dev/null || true; printf '{}'"#,
        PATH_START_SENTINEL, PATH_END_SENTINEL, ENV_START_SENTINEL, ENV_END_SENTINEL
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

    // Extract PATH between sentinels (last complete pair wins if sentinels
    // appear multiple times — rc-file noise prints before our payload).
    // Search only the prefix before the env payload: the env dump prints
    // after the PATH pair, so an env *value* containing a literal PATH
    // sentinel must not re-anchor the extraction.
    let path_region_end = output_str
        .rfind(ENV_START_SENTINEL)
        .unwrap_or(output_str.len());
    let path_str = extract_between_sentinels(
        &output_str[..path_region_end],
        PATH_START_SENTINEL,
        PATH_END_SENTINEL,
    )?;

    // Filter to absolute paths only to avoid unsafe relative entries like "." or "bin"
    let dirs = std::env::split_paths(path_str)
        .filter(|p| p.is_absolute())
        .collect::<Vec<_>>();

    // Extract the env payload from the same output; missing env sentinels
    // degrade to an empty map (PATH capture still succeeds). Only allow-listed
    // vars survive parsing — the raw payload never leaves this function.
    let credential_env =
        extract_between_sentinels(&output_str, ENV_START_SENTINEL, ENV_END_SENTINEL)
            .map(parse_credential_env)
            .unwrap_or_default();

    Some(LoginShellCapture {
        dirs,
        credential_env,
    })
}

/// Extract the value between a sentinel pair in shell output.
/// Returns Some(value) if both sentinels found, None otherwise.
/// If sentinels appear multiple times, uses the last complete pair (last END, then last START before it).
#[cfg(unix)]
fn extract_between_sentinels<'a>(output: &'a str, start: &str, end: &str) -> Option<&'a str> {
    // Find the last END sentinel
    let end_idx = output.rfind(end)?;
    // Find the last START sentinel before that END
    let start_pos = output[..end_idx].rfind(start)? + start.len();
    Some(&output[start_pos..end_idx])
}

#[cfg(not(unix))]
fn capture_login_shell_with(_shell: Option<&str>) -> LoginShellCapture {
    LoginShellCapture::empty()
}

/// Returns the cached login-shell capture, running the shell at most once.
fn login_shell_capture() -> &'static LoginShellCapture {
    LOGIN_SHELL_CAPTURE.get_or_init(|| capture_login_shell_with(None))
}

/// Returns cached login-shell PATH directories (unix only).
/// Runs the shell at most once, with a 5s timeout, and degrades silently on failure.
pub(crate) fn login_shell_dirs() -> &'static [PathBuf] {
    &login_shell_capture().dirs
}

/// Returns the allow-listed credential env vars captured from the login shell
/// (unix only, cached, same single shell invocation as [`login_shell_dirs`]).
/// Empty on any capture failure or on non-unix platforms.
///
/// SECURITY: values are secrets — callers must never log, trace, or serialize
/// them; keys-only if any logging is needed at all.
pub fn login_shell_credential_env() -> &'static BTreeMap<String, String> {
    &login_shell_capture().credential_env
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
    enhanced_path_dirs_with_home(home_dir().as_deref())
}

/// Variant of [`enhanced_path_dirs`] with the home directory injected instead
/// of resolved from the environment. Inherited PATH precedence is unchanged.
pub fn enhanced_path_dirs_with_home(home: Option<&std::path::Path>) -> Vec<PathBuf> {
    let mut dirs: Vec<PathBuf> = Vec::new();
    let mut seen: HashSet<PathBuf> = HashSet::new();

    // Start with current PATH
    if let Some(path) = std::env::var_os("PATH") {
        for dir in std::env::split_paths(&path) {
            push_dir(&mut dirs, &mut seen, dir);
        }
    }

    // Add enriched tool directories
    for dir in enriched_tool_dirs_with_home(home) {
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

fn nvm_node_version(path: &Path) -> Option<(u64, u64, u64, bool)> {
    let version = path.file_name()?.to_str()?.strip_prefix('v')?;
    let core = version.split(['-', '+']).next()?;
    let mut parts = core.split('.');
    let major = parts.next()?.parse().ok()?;
    let minor = parts.next()?.parse().ok()?;
    let patch = parts.next()?.parse().ok()?;
    let is_stable = !version.contains('-');
    (parts.next().is_none()).then_some((major, minor, patch, is_stable))
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

    // Prefer the newest installed nvm Node. read_dir order is unspecified and
    // often put an older installation first, which made first-match discovery
    // report an obsolete Node even when a current version was installed.
    if let Some(home) = home {
        let nvm_dir = home.join(".nvm").join("versions").join("node");
        if let Ok(entries) = std::fs::read_dir(&nvm_dir) {
            let mut version_dirs: Vec<PathBuf> =
                entries.flatten().map(|entry| entry.path()).collect();
            version_dirs.sort_by(|a, b| {
                nvm_node_version(b)
                    .cmp(&nvm_node_version(a))
                    .then_with(|| b.file_name().cmp(&a.file_name()))
            });
            for version_dir in version_dirs {
                push_dir(&mut dirs, &mut seen, version_dir.join("bin"));
            }
        }
    }

    // Add login-shell PATH directories (unix only, cached, silent degradation)
    for dir in login_dirs_fn() {
        push_dir(&mut dirs, &mut seen, dir.clone());
    }

    dirs
}

/// Extensions Windows can actually run for a discovered entry point
/// (`CreateProcess` / `cmd.exe`-runnable), in resolution-preference order.
///
/// This is the single Windows runnable-extension policy shared by every
/// binary-discovery site: provider discovery in `intent-providers`, auggie
/// discovery in `intent-context`, and `host.findBinary` in `intent-transport`.
pub const WINDOWS_EXEC_EXTENSIONS: [&str; 3] = ["exe", "cmd", "bat"];

/// True when `path` carries a Windows-runnable executable extension
/// (`.exe`/`.cmd`/`.bat`, case-insensitive).
pub fn has_windows_exec_extension(path: &Path) -> bool {
    path.extension()
        .and_then(|e| e.to_str())
        .is_some_and(|ext| {
            WINDOWS_EXEC_EXTENSIONS
                .iter()
                .any(|e| ext.eq_ignore_ascii_case(e))
        })
}

/// True when `p` is a file that is executable (unix checks the exec bit;
/// Windows requires a runnable executable extension — `CreateProcess` cannot
/// run a bare extensionless file, so its mere existence is not enough).
pub fn is_executable_file(p: &Path) -> bool {
    is_executable_file_for(p, cfg!(windows))
}

/// [`is_executable_file`] parametrized on the platform (test seam — Windows
/// CI is disabled, so the Windows arm is unit-tested on POSIX).
pub fn is_executable_file_for(p: &Path, is_windows: bool) -> bool {
    if !p.is_file() {
        return false;
    }
    if is_windows {
        return has_windows_exec_extension(p);
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

    /// Serializes every fake-shell test's script write + shell spawn.
    ///
    /// Under parallel test runs, a sibling test's `fork` can inherit the
    /// still-open write fd of another test's script (CLOEXEC only closes it
    /// at `exec`), so exec'ing that just-written script intermittently fails
    /// with ETXTBSY and the capture silently degrades to empty
    /// (intent-hq/monorepo#1968).
    #[cfg(unix)]
    static FAKE_SHELL_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[cfg(unix)]
    fn lock_fake_shell() -> std::sync::MutexGuard<'static, ()> {
        FAKE_SHELL_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Write the fake shell script and run the capture under the shared
    /// lock, so no other test's fork can race this script's write + exec.
    #[cfg(unix)]
    fn write_and_capture(path: &std::path::Path, content: &str) -> LoginShellCapture {
        let _guard = lock_fake_shell();
        write_fake_shell(path, content);
        capture_login_shell_with(Some(path.to_str().unwrap()))
    }

    #[test]
    fn enhanced_path_dirs_includes_current_path() {
        // Hold the fake-shell lock: this can be the first caller of the
        // LOGIN_SHELL_CAPTURE OnceLock, whose lazy init forks the real
        // login shell and must not overlap a sibling's script write.
        #[cfg(unix)]
        let _guard = lock_fake_shell();
        let dirs = enhanced_path_dirs();
        // Should at least include the directories from current PATH
        assert!(!dirs.is_empty());
    }

    #[test]
    fn enhanced_path_dirs_deduplicates() {
        // Hold the fake-shell lock: may lazily init LOGIN_SHELL_CAPTURE (forks)
        #[cfg(unix)]
        let _guard = lock_fake_shell();
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
        // Hold the fake-shell lock: may lazily init LOGIN_SHELL_CAPTURE (forks)
        #[cfg(unix)]
        let _guard = lock_fake_shell();
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
        let capture = write_and_capture(
            &fake_shell,
            "#!/bin/sh\nif [ \"$1\" = \"-ilc\" ]; then\n  printf '__INTENT_PATH_S__/custom/bin:/other/bin__INTENT_PATH_E__'\nfi\n",
        );
        fs::remove_file(&fake_shell).ok();

        let dirs = capture.dirs;
        assert_eq!(dirs.len(), 2);
        assert_eq!(dirs[0], PathBuf::from("/custom/bin"));
        assert_eq!(dirs[1], PathBuf::from("/other/bin"));
    }

    #[test]
    #[cfg(unix)]
    fn capture_login_shell_path_with_invalid_shell_degrades_silently() {
        // Locked too: this spawn's fork must not overlap a sibling's script write
        let _guard = lock_fake_shell();
        let capture = capture_login_shell_with(Some("/nonexistent/shell"));
        assert!(capture.dirs.is_empty());
        assert!(capture.credential_env.is_empty());
    }

    #[test]
    #[cfg(unix)]
    fn capture_login_shell_path_with_empty_shell_string_degrades_silently() {
        let capture = capture_login_shell_with(Some(""));
        assert!(capture.dirs.is_empty());
        assert!(capture.credential_env.is_empty());
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
    #[cfg(not(windows))]
    fn enriched_tool_dirs_scans_every_nvm_node_version_newest_first() {
        let unique = format!(
            "intent-path-utils-nvm-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let home = std::env::temp_dir().join(unique);
        let v20_bin = home.join(".nvm/versions/node/v20.19.0/bin");
        let v24_rc_bin = home.join(".nvm/versions/node/v24.5.0-rc.1/bin");
        let v24_bin = home.join(".nvm/versions/node/v24.5.0/bin");
        std::fs::create_dir_all(&v20_bin).unwrap();
        std::fs::create_dir_all(&v24_rc_bin).unwrap();
        std::fs::create_dir_all(&v24_bin).unwrap();

        let dirs = enriched_tool_dirs_impl(Some(&home), || &[]);

        assert!(dirs.contains(&v20_bin));
        assert!(dirs.contains(&v24_rc_bin));
        assert!(dirs.contains(&v24_bin));
        let v20_position = dirs.iter().position(|dir| dir == &v20_bin).unwrap();
        let v24_rc_position = dirs.iter().position(|dir| dir == &v24_rc_bin).unwrap();
        let v24_position = dirs.iter().position(|dir| dir == &v24_bin).unwrap();
        assert!(v24_position < v20_position, "newest nvm Node must be first");
        assert!(
            v24_position < v24_rc_position,
            "stable nvm Node must sort ahead of a matching prerelease"
        );
        assert!(dirs.contains(&home.join(".local/bin")));
        assert!(dirs.contains(&PathBuf::from("/opt/homebrew/bin")));
        std::fs::remove_dir_all(&home).unwrap();
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
        let dirs = write_and_capture(
            &fake_shell,
            "#!/bin/sh\nif [ \"$1\" = \"-ilc\" ]; then\n  echo 'Loading nvm...'\n  echo 'nvm initialized'\n  printf '__INTENT_PATH_S__/noise/bin:/test/bin__INTENT_PATH_E__'\n  echo 'Shell ready'\nfi\n",
        )
        .dirs;
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
        let dirs = write_and_capture(
            &fake_shell,
            "#!/bin/sh\nif [ \"$1\" = \"-ilc\" ]; then\n  printf '/missing/bin:/sentinels/bin'\nfi\n",
        )
        .dirs;
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
        let dirs = write_and_capture(
            &fake_shell,
            "#!/bin/sh\nif [ \"$1\" = \"-ilc\" ]; then\n  exit 1\nelif [ \"$1\" = \"-lc\" ]; then\n  printf '__INTENT_PATH_S__/fallback/bin:/backup/bin__INTENT_PATH_E__'\nfi\n",
        )
        .dirs;
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
        let dirs = write_and_capture(
            &fake_shell,
            "#!/bin/sh\nif [ \"$1\" = \"-ilc\" ]; then\n  printf '__INTENT_PATH_S__/first/bin__INTENT_PATH_E__'\n  printf '__INTENT_PATH_S__/last/bin:/final/bin__INTENT_PATH_E__'\nfi\n",
        )
        .dirs;
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
        let dirs = write_and_capture(
            &fake_shell,
            "#!/bin/sh\nif [ \"$1\" = \"-ilc\" ]; then\n  printf '__INTENT_PATH_S__/valid/bin__INTENT_PATH_E__'\n  printf '__INTENT_PATH_S__'\nfi\n",
        )
        .dirs;
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
        let start = std::time::Instant::now();
        let dirs = write_and_capture(&fake_shell, &script).dirs;
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

    #[test]
    #[cfg(unix)]
    fn credential_env_allow_list_matches_exact_and_prefix_names() {
        assert!(is_credential_env_allow_listed("ANTHROPIC_API_KEY"));
        assert!(is_credential_env_allow_listed("HUGGING_FACE_HUB_TOKEN"));
        assert!(is_credential_env_allow_listed("XAI_API_KEY"));
        assert!(is_credential_env_allow_listed("AUGGIE_API_TOKEN"));
        assert!(is_credential_env_allow_listed("CLAUDE_CODE_OAUTH_TOKEN"));
        assert!(is_credential_env_allow_listed("CORTEX_ANY_SUFFIX"));
        assert!(is_credential_env_allow_listed("GROK_API_KEY"));
        assert!(is_credential_env_allow_listed("PI_API_KEY"));
        // Prefix requires the trailing underscore
        assert!(!is_credential_env_allow_listed("CLAUDEX_TOKEN"));
        assert!(!is_credential_env_allow_listed("AUGGIE"));
        assert!(!is_credential_env_allow_listed("PIN"));
        assert!(!is_credential_env_allow_listed("HOME"));
        assert!(!is_credential_env_allow_listed("PATH"));
    }

    #[test]
    #[cfg(unix)]
    fn parse_credential_env_filters_and_tolerates_newlines() {
        let payload = "ANTHROPIC_API_KEY=exact-value\0AUGGIE_SESSION=prefix-value\0RANDOM_SECRET=dropped\0MULTI=a\nb\0CODEX_KEY=line1\nline2\0";
        let map = parse_credential_env(payload);
        assert_eq!(map.len(), 3);
        assert_eq!(
            map.get("ANTHROPIC_API_KEY").map(String::as_str),
            Some("exact-value")
        );
        assert_eq!(
            map.get("AUGGIE_SESSION").map(String::as_str),
            Some("prefix-value")
        );
        assert_eq!(
            map.get("CODEX_KEY").map(String::as_str),
            Some("line1\nline2")
        );
        assert!(!map.contains_key("RANDOM_SECRET"));
        assert!(!map.contains_key("MULTI"));
    }

    #[test]
    #[cfg(unix)]
    fn capture_login_shell_env_captures_allow_listed_vars_only() {
        use std::fs;

        let temp_dir = std::env::temp_dir();
        let pid = std::process::id();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let fake_shell = temp_dir.join(format!("fake_shell_env_{pid}_{nanos}.sh"));

        // Script emits PATH sentinels plus a NUL-separated env payload with
        // an exact-name var, a prefix var, and a non-allow-listed var
        let capture = write_and_capture(
            &fake_shell,
            "#!/bin/sh\nif [ \"$1\" = \"-ilc\" ]; then\n  printf '__INTENT_PATH_S__/env/bin__INTENT_PATH_E__'\n  printf '__INTENT_ENV_S__'\n  printf 'ANTHROPIC_API_KEY=test-exact\\0AUGGIE_TOKEN=test-prefix\\0NOT_ALLOWED=dropped\\0'\n  printf '__INTENT_ENV_E__'\nfi\n",
        );
        fs::remove_file(&fake_shell).ok();

        assert_eq!(capture.dirs, vec![PathBuf::from("/env/bin")]);
        assert_eq!(capture.credential_env.len(), 2);
        assert_eq!(
            capture
                .credential_env
                .get("ANTHROPIC_API_KEY")
                .map(String::as_str),
            Some("test-exact")
        );
        assert_eq!(
            capture
                .credential_env
                .get("AUGGIE_TOKEN")
                .map(String::as_str),
            Some("test-prefix")
        );
        assert!(!capture.credential_env.contains_key("NOT_ALLOWED"));
    }

    #[test]
    #[cfg(unix)]
    fn capture_env_value_containing_path_sentinel_does_not_corrupt_path() {
        use std::fs;

        let temp_dir = std::env::temp_dir();
        let pid = std::process::id();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let fake_shell = temp_dir.join(format!("fake_shell_env_sentinel_{pid}_{nanos}.sh"));

        // An env *value* containing the literal PATH end sentinel must not
        // re-anchor the PATH extraction into the env payload.
        let capture = write_and_capture(
            &fake_shell,
            "#!/bin/sh\nif [ \"$1\" = \"-ilc\" ]; then\n  printf '__INTENT_PATH_S__/real/bin__INTENT_PATH_E__'\n  printf '__INTENT_ENV_S__'\n  printf 'CODEX_EVIL=/fake/bin__INTENT_PATH_E__\\0'\n  printf '__INTENT_ENV_E__'\nfi\n",
        );
        fs::remove_file(&fake_shell).ok();

        assert_eq!(capture.dirs, vec![PathBuf::from("/real/bin")]);
        assert_eq!(
            capture.credential_env.get("CODEX_EVIL").map(String::as_str),
            Some("/fake/bin__INTENT_PATH_E__")
        );
    }

    #[test]
    #[cfg(unix)]
    fn capture_login_shell_env_value_with_newline_survives() {
        use std::fs;

        let temp_dir = std::env::temp_dir();
        let pid = std::process::id();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let fake_shell = temp_dir.join(format!("fake_shell_env_nl_{pid}_{nanos}.sh"));

        // Value contains a newline; NUL separation must keep it intact
        let capture = write_and_capture(
            &fake_shell,
            "#!/bin/sh\nif [ \"$1\" = \"-ilc\" ]; then\n  printf '__INTENT_PATH_S__/env/bin__INTENT_PATH_E__'\n  printf '__INTENT_ENV_S__'\n  printf 'CODEX_MULTI=line1\\nline2\\0HF_TOKEN=test-hf\\0'\n  printf '__INTENT_ENV_E__'\nfi\n",
        );
        fs::remove_file(&fake_shell).ok();

        assert_eq!(
            capture
                .credential_env
                .get("CODEX_MULTI")
                .map(String::as_str),
            Some("line1\nline2")
        );
        assert_eq!(
            capture.credential_env.get("HF_TOKEN").map(String::as_str),
            Some("test-hf")
        );
    }

    #[test]
    #[cfg(unix)]
    fn capture_login_shell_env_missing_sentinels_degrades_to_empty_map() {
        use std::fs;

        let temp_dir = std::env::temp_dir();
        let pid = std::process::id();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let fake_shell = temp_dir.join(format!("fake_shell_env_none_{pid}_{nanos}.sh"));

        // PATH sentinels only — env sentinels absent
        let capture = write_and_capture(
            &fake_shell,
            "#!/bin/sh\nif [ \"$1\" = \"-ilc\" ]; then\n  printf '__INTENT_PATH_S__/only/bin__INTENT_PATH_E__'\nfi\n",
        );
        fs::remove_file(&fake_shell).ok();

        assert_eq!(capture.dirs, vec![PathBuf::from("/only/bin")]);
        assert!(
            capture.credential_env.is_empty(),
            "Missing env sentinels should degrade to an empty map"
        );
    }

    /// A fresh RAII temp directory for `tag` under the system temp root. The
    /// returned guard removes the dir on drop (including on panic); set
    /// `INTENTD_TEST_KEEP_TMP` (non-empty) to keep it around for debugging.
    fn unique_temp_dir(tag: &str) -> tempfile::TempDir {
        let mut dir = tempfile::Builder::new()
            .prefix(&format!("intent-core-{tag}-"))
            .tempdir()
            .expect("create test temp dir");
        if std::env::var_os("INTENTD_TEST_KEEP_TMP").is_some_and(|v| !v.is_empty()) {
            dir.disable_cleanup(true);
        }
        dir
    }

    #[test]
    fn has_windows_exec_extension_only_matches_runnable_exts() {
        assert!(has_windows_exec_extension(Path::new("tool.exe")));
        assert!(has_windows_exec_extension(Path::new("tool.cmd")));
        assert!(has_windows_exec_extension(Path::new("tool.bat")));
        // Case-insensitive, matching Windows filename semantics.
        assert!(has_windows_exec_extension(Path::new("tool.EXE")));
        assert!(has_windows_exec_extension(Path::new("tool.Cmd")));
        assert!(!has_windows_exec_extension(Path::new("tool")));
        assert!(!has_windows_exec_extension(Path::new("tool.ps1")));
        assert!(!has_windows_exec_extension(Path::new("tool.txt")));
    }

    #[test]
    fn is_executable_file_windows_requires_runnable_extension() {
        let dir = unique_temp_dir("win-exec");
        let bare = dir.path().join("tool");
        let cmd = dir.path().join("tool.cmd");
        let exe_upper = dir.path().join("tool.EXE");
        std::fs::write(&bare, "#!/bin/sh\nexit 0\n").unwrap();
        std::fs::write(&cmd, "@echo off\r\n").unwrap();
        std::fs::write(&exe_upper, "MZ").unwrap();
        assert!(
            !is_executable_file_for(&bare, true),
            "an extensionless file is not runnable on Windows"
        );
        assert!(is_executable_file_for(&cmd, true));
        assert!(
            is_executable_file_for(&exe_upper, true),
            "extension matching must be case-insensitive"
        );
        assert!(!is_executable_file_for(
            &dir.path().join("missing.exe"),
            true
        ));
        assert!(
            !is_executable_file_for(dir.path(), true),
            "directories never resolve"
        );
    }

    #[cfg(unix)]
    #[test]
    fn is_executable_file_posix_requires_exec_bit() {
        use std::os::unix::fs::PermissionsExt;
        let dir = unique_temp_dir("posix-exec");
        let plain = dir.path().join("tool");
        std::fs::write(&plain, "#!/bin/sh\nexit 0\n").unwrap();
        assert!(
            !is_executable_file_for(&plain, false),
            "a file without the exec bit is not executable on POSIX"
        );
        std::fs::set_permissions(&plain, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(is_executable_file_for(&plain, false));
        assert!(!is_executable_file_for(&dir.path().join("missing"), false));
        assert!(
            !is_executable_file_for(dir.path(), false),
            "directories never resolve"
        );
    }
}
