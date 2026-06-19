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
