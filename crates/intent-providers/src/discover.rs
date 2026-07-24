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
    /// Whether this provider supports npx fallback when binary is unresolved.
    pub has_npx_fallback: bool,
    /// When set, the provider is spawned exclusively via `npx -y <package>`
    /// (pinned spec); `installed`/`resolved_path` then reflect npx itself
    /// rather than a local provider binary.
    pub npx_only_package: Option<&'static str>,
}

/// Status of npx availability for provider fallback spawning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NpxStatus {
    /// Resolved absolute path to npx, when found.
    pub resolved_path: Option<PathBuf>,
    /// Version string from `npx --version`, when successfully probed.
    pub version: Option<String>,
    /// Whether the version meets the minimum requirement (major >= 7).
    pub version_ok: bool,
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
/// npx-only providers (claude-code) are probed for `npx` availability instead
/// of a local provider binary — there is no local-binary path for them.
/// All other providers resolve through [`find_provider_binary`] so this
/// aggregate surface and `host.providerAuthStatus` share one resolution
/// precedence: native installer locations (grok `~/.grok/bin`, opencode
/// `~/.opencode/bin`), then `~/.augment/bin`, then the enhanced PATH scan
/// (inherited PATH + enriched tool dirs + login-shell capture).
pub fn discover_providers() -> Vec<ProviderAvailability> {
    ACP_PROVIDERS
        .iter()
        .map(|provider| {
            let gated_off = gated_reason(provider);
            let resolved_path = if gated_off.is_some() {
                None
            } else if provider.npx_only_package.is_some() {
                find_npx()
            } else {
                find_provider_binary(provider.id, provider.command, None)
            };
            ProviderAvailability {
                id: provider.id,
                display_name: provider.display_name,
                command: provider.command,
                installed: resolved_path.is_some(),
                resolved_path,
                gated_off,
                auth_check_args: provider.auth_check_args,
                has_npx_fallback: provider.fallback_npx_package.is_some(),
                npx_only_package: provider.npx_only_package,
            }
        })
        .collect()
}

/// Probe npx availability (path only, no spawning). Returns the resolved path
/// when npx is found on PATH. Version probing requires spawning `npx --version`
/// and is handled at the transport layer where a tokio runtime is available.
pub fn probe_npx() -> NpxStatus {
    let resolved_path = find_npx();
    NpxStatus {
        resolved_path,
        version: None,
        version_ok: false,
    }
}

/// Resolve a provider binary to an absolute path using the precedence order:
/// 1. Explicit path from `providers.paths` map (keyed by provider ID)
/// 2. Native installer location (grok: `~/.grok/bin`, opencode: `~/.opencode/bin`)
/// 3. `~/.augment/bin/<command>` (auggie-specific, not a generic managed tier)
/// 4. Scan enhanced PATH directories (`intent_core::path_utils`: inherited
///    PATH + enriched tool dirs + login-shell PATH capture)
///
/// Returns `None` when the binary cannot be resolved. The `provider_id` is
/// used for logging when an explicit path is invalid.
pub fn find_provider_binary(
    provider_id: &str,
    command: &str,
    explicit_path: Option<&str>,
) -> Option<PathBuf> {
    find_provider_binary_with_home(provider_id, command, explicit_path, home_dir().as_deref())
}

/// [`find_provider_binary`] with an explicit `home` for the native-installer
/// tier (test seam — avoids mutating the process-global `HOME` in parallel
/// tests).
fn find_provider_binary_with_home(
    provider_id: &str,
    command: &str,
    explicit_path: Option<&str>,
    home: Option<&std::path::Path>,
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
                "providers.paths[\"{}\"] must be absolute and executable; falling back to native install dir / managed bin / PATH scan",
                provider_id
            );
        }
    }

    // 2. Native installer locations (grok: `~/.grok/bin/grok`, opencode:
    // `~/.opencode/bin/opencode`) are preferred over any PATH-resolved
    // npm-global wrapper (parity with `grok-resolver.ts` / `opencode-resolver.ts`:
    // wrappers can emit update banners before real stdout).
    if let Some(home) = home {
        if let Some(native) = find_provider_native_binary_in(provider_id, command, home) {
            return Some(native);
        }
    }

    // 3. ~/.augment/bin (auggie's install location; kept for auggie back-compat)
    if let Some(managed) = managed_binary_path(command) {
        if is_executable_file(&managed) {
            return Some(managed);
        }
    }

    // 4. Scan enhanced PATH directories
    find_in_enhanced_dirs(command)
}

/// The `$HOME`-relative directory a provider's native installer places its
/// binary under (`~/<dot_dir>/bin/<command>`), or `None` for providers without
/// a native-installer tier.
fn native_install_dir(provider_id: &str) -> Option<&'static str> {
    match provider_id {
        "grok" => Some(".grok"),
        "opencode" => Some(".opencode"),
        _ => None,
    }
}

/// Candidate paths for a provider's native installer location under `home`
/// (`~/<dot_dir>/bin/<command>`, plus `.exe`/`.cmd` variants on Windows).
/// Port of `GROK_NATIVE_PATHS` / `OPENCODE_NATIVE_PATHS` from the FE resolvers.
fn native_install_candidates(home: &std::path::Path, dot_dir: &str, command: &str) -> Vec<PathBuf> {
    let bin = home.join(dot_dir).join("bin");
    let mut candidates = vec![bin.join(command)];
    if cfg!(windows) {
        candidates.push(bin.join(format!("{command}.exe")));
        candidates.push(bin.join(format!("{command}.cmd")));
    }
    candidates
}

/// Resolve a provider's native installer binary under an explicit `home`
/// (test seam — avoids mutating the process-global `HOME` in parallel tests).
fn find_provider_native_binary_in(
    provider_id: &str,
    command: &str,
    home: &std::path::Path,
) -> Option<PathBuf> {
    let dot_dir = native_install_dir(provider_id)?;
    native_install_candidates(home, dot_dir, command)
        .into_iter()
        .find(|p| is_executable_file(p))
}

/// The auggie binary path (`~/.augment/bin/<command>[.exe]`). This is auggie's
/// own install location, not a generic Intent-managed binary tier.
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

/// Find the first executable for `command` by scanning enhanced PATH directories
/// from `intent_core::path_utils::enhanced_path_dirs()`: inherited PATH plus
/// enriched tool dirs (node/npm/nvm/homebrew/volta/asdf, …) plus the cached
/// login-shell PATH capture.
///
/// Blocking: this scans the filesystem, and on Unix the *first* per-process
/// call can spawn `$SHELL -ilc` (up to 5s, then cached). Latency-sensitive
/// async callers should prewarm or wrap in `spawn_blocking`.
fn find_in_enhanced_dirs(command: &str) -> Option<PathBuf> {
    find_in_dirs(&intent_core::path_utils::enhanced_path_dirs(), command)
}

/// Find the first executable for `command` in `dirs`, in order (test seam —
/// lets tests scan a controlled dir list without spawning a login shell).
fn find_in_dirs(dirs: &[PathBuf], command: &str) -> Option<PathBuf> {
    let candidates = name_candidates(command);
    for dir in dirs {
        for candidate in &candidates {
            let full = dir.join(candidate);
            if is_executable_file(&full) {
                return Some(full);
            }
        }
    }
    None
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
    fn discover_providers_reports_claude_code_as_npx_only() {
        let providers = discover_providers();
        let cc = providers.iter().find(|p| p.id == "claude-code").unwrap();
        assert_eq!(
            cc.npx_only_package,
            Some(crate::config::CLAUDE_AGENT_ACP_NPX_PACKAGE),
            "claude-code availability must carry the pinned npx package"
        );
        // Assert against the single discovery snapshot rather than re-resolving
        // npx (no test mutates process-global PATH anymore — monorepo#628 —
        // but the snapshot assertion stays robust regardless).
        assert_eq!(cc.installed, cc.resolved_path.is_some());
        if let Some(path) = &cc.resolved_path {
            assert!(path
                .file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("npx"));
        }
    }

    #[test]
    fn discover_providers_reports_pi_as_npx_only() {
        let providers = discover_providers();
        let pi = providers.iter().find(|p| p.id == "pi").unwrap();
        assert_eq!(
            pi.npx_only_package,
            Some(crate::config::PI_ACP_NPX_PACKAGE),
            "pi availability must carry the pinned npx package"
        );
        // Assert against the same discovery snapshot rather than re-resolving
        // npx (see the claude-code test above; monorepo#628).
        assert_eq!(pi.installed, pi.resolved_path.is_some());
        if let Some(path) = &pi.resolved_path {
            assert!(path
                .file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("npx"));
        }
    }

    #[test]
    fn discover_providers_non_npx_only_providers_unchanged() {
        let providers = discover_providers();
        for p in providers
            .iter()
            .filter(|p| p.id != "claude-code" && p.id != "pi")
        {
            assert_eq!(p.npx_only_package, None, "{} must not be npx-only", p.id);
        }
    }

    #[test]
    fn grok_native_candidates_prefer_home_grok_bin() {
        let home = PathBuf::from("/home/tester");
        let candidates = native_install_candidates(&home, ".grok", "grok");
        assert_eq!(
            candidates[0],
            home.join(".grok").join("bin").join("grok"),
            "native installer path must be the first candidate"
        );
        if cfg!(windows) {
            assert!(candidates
                .iter()
                .any(|p| p.ends_with(PathBuf::from("bin").join("grok.exe"))));
            assert!(candidates
                .iter()
                .any(|p| p.ends_with(PathBuf::from("bin").join("grok.cmd"))));
        } else {
            assert_eq!(candidates.len(), 1);
        }
    }

    #[test]
    fn opencode_native_candidates_prefer_home_opencode_bin() {
        let home = PathBuf::from("/home/tester");
        let candidates = native_install_candidates(&home, ".opencode", "opencode");
        assert_eq!(
            candidates[0],
            home.join(".opencode").join("bin").join("opencode"),
            "native installer path must be the first candidate"
        );
        if cfg!(windows) {
            assert!(candidates
                .iter()
                .any(|p| p.ends_with(PathBuf::from("bin").join("opencode.exe"))));
            assert!(candidates
                .iter()
                .any(|p| p.ends_with(PathBuf::from("bin").join("opencode.cmd"))));
        } else {
            assert_eq!(candidates.len(), 1);
        }
    }

    #[cfg(unix)]
    #[test]
    fn find_grok_native_binary_in_requires_executable_at_native_path() {
        // End-to-end against a fake home: `<home>/.grok/bin/grok` resolves
        // only once it is executable (non-executable files must not resolve).
        let home = unique_temp_dir("grok-home");
        let bin_dir = home.join(".grok").join("bin");
        fs::create_dir_all(&bin_dir).unwrap();
        assert_eq!(find_provider_native_binary_in("grok", "grok", &home), None);

        let bin = bin_dir.join("grok");
        fs::write(&bin, "not executable").unwrap();
        assert_eq!(find_provider_native_binary_in("grok", "grok", &home), None);

        make_executable(&bin);
        assert_eq!(
            find_provider_native_binary_in("grok", "grok", &home),
            Some(bin)
        );
    }

    #[cfg(unix)]
    #[test]
    fn find_opencode_native_binary_in_requires_executable_at_native_path() {
        // Regression for opencode installed only via its native installer
        // (`<home>/.opencode/bin/opencode`, no PATH entry): resolution must
        // find it, and only once it is executable.
        let home = unique_temp_dir("opencode-home");
        let bin_dir = home.join(".opencode").join("bin");
        fs::create_dir_all(&bin_dir).unwrap();
        assert_eq!(
            find_provider_native_binary_in("opencode", "opencode", &home),
            None
        );

        let bin = bin_dir.join("opencode");
        fs::write(&bin, "not executable").unwrap();
        assert_eq!(
            find_provider_native_binary_in("opencode", "opencode", &home),
            None
        );

        make_executable(&bin);
        assert_eq!(
            find_provider_native_binary_in("opencode", "opencode", &home),
            Some(bin)
        );
    }

    #[cfg(unix)]
    #[test]
    fn find_provider_native_binary_in_ignores_providers_without_native_installs() {
        // Only grok/opencode have native-installer tiers; other providers must
        // not resolve from a lookalike dot-dir layout.
        let home = unique_temp_dir("native-other");
        let bin_dir = home.join(".auggie").join("bin");
        fs::create_dir_all(&bin_dir).unwrap();
        let bin = bin_dir.join("auggie");
        make_executable(&bin);
        assert_eq!(
            find_provider_native_binary_in("auggie", "auggie", &home),
            None
        );
    }

    #[cfg(unix)]
    #[test]
    fn find_provider_binary_explicit_setting_wins_over_native_for_opencode() {
        // Precedence: with a native binary present in the fake home, the
        // explicit `providers.paths` setting still wins; without the explicit
        // setting, the native tier resolves.
        let home = unique_temp_dir("opencode-precedence-home");
        let native_dir = home.join(".opencode").join("bin");
        fs::create_dir_all(&native_dir).unwrap();
        let native = native_dir.join("opencode");
        make_executable(&native);

        let explicit_dir = unique_temp_dir("opencode-explicit");
        let explicit = explicit_dir.join("opencode");
        make_executable(&explicit);

        let result = find_provider_binary_with_home(
            "opencode",
            "opencode",
            Some(explicit.to_str().unwrap()),
            Some(&home),
        );
        assert_eq!(result, Some(explicit), "explicit setting must beat native");

        let result = find_provider_binary_with_home("opencode", "opencode", None, Some(&home));
        assert_eq!(
            result,
            Some(native),
            "native tier must resolve without an explicit setting"
        );
    }

    #[cfg(unix)]
    #[test]
    fn find_in_dirs_scans_login_shell_style_dirs() {
        // The enhanced scan must find binaries in dirs that only appear via
        // the login-shell PATH capture (injected here as a controlled dir
        // list — no real login shell is spawned, same seam pattern as
        // `intent_core::path_utils` tests).
        let login_dir = unique_temp_dir("login-shell-bin");
        let bin = login_dir.join("opencode");
        make_executable(&bin);

        let dirs = vec![PathBuf::from("/nonexistent/first"), login_dir];
        assert_eq!(find_in_dirs(&dirs, "opencode"), Some(bin));
        assert_eq!(find_in_dirs(&dirs, "intent-test-absent-cmd"), None);
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
}
