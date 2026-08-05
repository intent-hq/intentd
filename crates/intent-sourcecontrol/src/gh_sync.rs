//! Best-effort sync of the stored GitHub token into the `gh` CLI.
//!
//! After the device flow authorizes ([`crate::device_flow`]), the daemon
//! offers the freshly stored token to a locally installed `gh` so terminal
//! work is authenticated too. The sync is **fail-soft** by design: `gh`
//! missing, an existing `gh` login (never clobbered), or a login failure all
//! leave the device-flow outcome untouched — callers get no error, only logs.
//!
//! 🔒 The token is loaded back from the secret store
//! (`sourceControl.github.token`) here — it is never plumbed out of the
//! device-flow engine — and reaches `gh auth login --with-token` via **stdin
//! only**: never argv (process listings), never logs, and child output is
//! never echoed (`gh auth status` can print masked token material).

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

use intent_core::FileSecretStore;
use secrecy::{ExposeSecret, SecretString};
use tokio::time::timeout;

use crate::token::SECRET_ACCOUNT;

/// Bounded budget for the whole sync (lookup + status probe + login) so a
/// wedged `gh` or filesystem never holds the spawned sync task hostage. The
/// blocking closure itself cannot be cancelled — on timeout only the *wait*
/// is abandoned: the closure (and any in-flight `gh` subprocess) keeps
/// running to completion on the blocking pool, so a late sync may still
/// succeed after the timeout is logged. This mirrors the secret-store
/// patterns in [`crate::device_flow`].
const GH_SYNC_TIMEOUT: Duration = Duration::from_secs(10);

/// Terminal outcome of one sync attempt (log/test surface only — never wire).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GhSyncOutcome {
    /// `gh auth login --with-token` succeeded.
    Synced,
    /// No stored token to sync (nothing in `sourceControl.github.token`).
    NoToken,
    /// `gh` is not on `PATH` — nothing to do.
    GhNotInstalled,
    /// `gh auth status` already reports a github.com login — never clobbered.
    AlreadyAuthenticated,
    /// The login subprocess failed (spawn error / non-zero exit).
    LoginFailed,
}

/// Injected command seam: the decision logic ([`sync_with`]) is unit-tested
/// against a mock, so tests never spawn a real `gh` (and can never touch a
/// developer's actual `gh` login).
pub trait GhCli: Send + Sync {
    /// Locate the `gh` binary on `PATH`, or `None` when not installed.
    fn locate(&self) -> Option<PathBuf>;
    /// True iff `gh auth status --hostname github.com` reports logged in.
    fn is_authenticated(&self, gh: &Path) -> bool;
    /// Run `gh auth login --with-token --hostname github.com`, piping the
    /// token via stdin. True on success.
    fn login_with_token(&self, gh: &Path, token: &SecretString) -> bool;
}

/// The sync decision ladder: no token → gh missing → existing login →
/// attempt. Pure over the [`GhCli`] seam so every arm is unit-testable.
fn sync_with(cli: &dyn GhCli, token: Option<SecretString>) -> GhSyncOutcome {
    let Some(token) = token else {
        return GhSyncOutcome::NoToken;
    };
    let Some(gh) = cli.locate() else {
        return GhSyncOutcome::GhNotInstalled;
    };
    if cli.is_authenticated(&gh) {
        return GhSyncOutcome::AlreadyAuthenticated;
    }
    if cli.login_with_token(&gh, &token) {
        GhSyncOutcome::Synced
    } else {
        GhSyncOutcome::LoginFailed
    }
}

/// Best-effort sync of the stored `sourceControl.github.token` into the `gh`
/// CLI. Never returns an error: every failure mode is logged (with the token
/// redacted by construction — it only ever crosses a child's stdin) and the
/// caller's flow proceeds unaffected. Runs the blocking work (secret-store
/// read + subprocesses) on the blocking pool, bounded by [`GH_SYNC_TIMEOUT`].
pub async fn sync_token_to_gh(store: FileSecretStore) {
    let handle = tokio::task::spawn_blocking(move || {
        let token = store
            .load(SECRET_ACCOUNT)
            .unwrap_or_else(|e| {
                // Corrupt/unreadable secrets file: warn (mirrors
                // `token::file_store_token`) but keep the fail-soft skip.
                tracing::warn!(
                    account = %SECRET_ACCOUNT,
                    error = %e,
                    "secrets-store load failed for gh CLI token sync (corrupt/unreadable file)"
                );
                None
            })
            .filter(|t| !t.trim().is_empty())
            .map(SecretString::from);
        sync_with(&SystemGhCli, token)
    });
    match timeout(GH_SYNC_TIMEOUT, handle).await {
        Ok(Ok(GhSyncOutcome::Synced)) => {
            tracing::info!("synced github token into the gh CLI");
        }
        Ok(Ok(GhSyncOutcome::LoginFailed)) => {
            tracing::warn!("gh CLI token sync failed (gh auth login); device flow unaffected");
        }
        Ok(Ok(outcome)) => {
            tracing::debug!(?outcome, "skipped gh CLI token sync");
        }
        Ok(Err(join_err)) => {
            tracing::warn!(error = %join_err, "gh CLI token sync task failed");
        }
        Err(_) => {
            // Only the wait is abandoned (see [`GH_SYNC_TIMEOUT`]): the
            // blocking closure keeps running and the sync may still complete
            // after this line is logged.
            tracing::warn!(
                "gh CLI token sync still running after 10s; no longer waiting (it may still \
                 complete in the background)"
            );
        }
    }
}

/// Production [`GhCli`]: real `PATH` lookup + real `gh` subprocesses.
struct SystemGhCli;

impl GhCli for SystemGhCli {
    fn locate(&self) -> Option<PathBuf> {
        find_on_path("gh")
    }

    fn is_authenticated(&self, gh: &Path) -> bool {
        // Output is discarded: `gh auth status` prints (masked) token
        // material, which must never reach logs. Only the exit code matters.
        Command::new(gh)
            .args(["auth", "status", "--hostname", "github.com"])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map(|s| s.success())
            .unwrap_or(false)
    }

    fn login_with_token(&self, gh: &Path, token: &SecretString) -> bool {
        // 🔒 Token via stdin only — never argv. Output is discarded so no
        // error path can echo credential material.
        let spawned = Command::new(gh)
            .args(["auth", "login", "--with-token", "--hostname", "github.com"])
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn();
        let Ok(mut child) = spawned else {
            return false;
        };
        if let Some(mut stdin) = child.stdin.take() {
            if stdin.write_all(token.expose_secret().as_bytes()).is_err() {
                let _ = child.kill();
                let _ = child.wait();
                return false;
            }
            // Dropping the handle closes stdin so `gh` sees EOF.
        }
        child.wait().map(|s| s.success()).unwrap_or(false)
    }
}

/// `which`-style lookup: first executable `name` (plus `.exe` on Windows)
/// across the `PATH` entries. `intent-transport`'s resolver cannot be reused
/// here (transport depends on services, never the reverse), so this crate
/// carries its own minimal lookup.
fn find_on_path(name: &str) -> Option<PathBuf> {
    let path = std::env::var_os("PATH")?;
    for dir in std::env::split_paths(&path) {
        if dir.as_os_str().is_empty() {
            continue;
        }
        let candidate = dir.join(name);
        if is_executable(&candidate) {
            return Some(candidate);
        }
        if cfg!(windows) {
            let candidate = dir.join(format!("{name}.exe"));
            if is_executable(&candidate) {
                return Some(candidate);
            }
        }
    }
    None
}

#[cfg(unix)]
fn is_executable(path: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;
    path.metadata()
        .map(|m| m.is_file() && m.permissions().mode() & 0o111 != 0)
        .unwrap_or(false)
}

#[cfg(not(unix))]
fn is_executable(path: &Path) -> bool {
    path.is_file()
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::*;

    /// Scripted [`GhCli`] that records whether a login was attempted — no
    /// real `gh` is ever spawned from tests.
    struct MockGhCli {
        installed: bool,
        authenticated: bool,
        login_succeeds: bool,
        login_attempted: AtomicBool,
    }

    impl MockGhCli {
        fn new(installed: bool, authenticated: bool, login_succeeds: bool) -> Self {
            Self {
                installed,
                authenticated,
                login_succeeds,
                login_attempted: AtomicBool::new(false),
            }
        }
    }

    impl GhCli for MockGhCli {
        fn locate(&self) -> Option<PathBuf> {
            self.installed.then(|| PathBuf::from("/mock/bin/gh"))
        }

        fn is_authenticated(&self, _gh: &Path) -> bool {
            self.authenticated
        }

        fn login_with_token(&self, _gh: &Path, _token: &SecretString) -> bool {
            self.login_attempted.store(true, Ordering::SeqCst);
            self.login_succeeds
        }
    }

    fn token() -> Option<SecretString> {
        Some(SecretString::from("gho_test_sync"))
    }

    #[test]
    fn syncs_when_gh_installed_and_not_authenticated() {
        let cli = MockGhCli::new(true, false, true);
        assert_eq!(sync_with(&cli, token()), GhSyncOutcome::Synced);
        assert!(cli.login_attempted.load(Ordering::SeqCst));
    }

    #[test]
    fn skips_without_a_stored_token() {
        let cli = MockGhCli::new(true, false, true);
        assert_eq!(sync_with(&cli, None), GhSyncOutcome::NoToken);
        assert!(!cli.login_attempted.load(Ordering::SeqCst));
    }

    #[test]
    fn skips_when_gh_is_not_installed() {
        let cli = MockGhCli::new(false, false, true);
        assert_eq!(sync_with(&cli, token()), GhSyncOutcome::GhNotInstalled);
        assert!(!cli.login_attempted.load(Ordering::SeqCst));
    }

    #[test]
    fn never_clobbers_an_existing_gh_login() {
        let cli = MockGhCli::new(true, true, true);
        assert_eq!(
            sync_with(&cli, token()),
            GhSyncOutcome::AlreadyAuthenticated
        );
        assert!(!cli.login_attempted.load(Ordering::SeqCst));
    }

    #[test]
    fn login_failure_is_reported_not_raised() {
        let cli = MockGhCli::new(true, false, false);
        assert_eq!(sync_with(&cli, token()), GhSyncOutcome::LoginFailed);
        assert!(cli.login_attempted.load(Ordering::SeqCst));
    }

    #[test]
    fn outcome_debug_never_carries_a_token() {
        // The outcome enum is the only sync surface that reaches logs.
        for outcome in [
            GhSyncOutcome::Synced,
            GhSyncOutcome::NoToken,
            GhSyncOutcome::GhNotInstalled,
            GhSyncOutcome::AlreadyAuthenticated,
            GhSyncOutcome::LoginFailed,
        ] {
            assert!(!format!("{outcome:?}").contains("gho_"));
        }
    }

    #[tokio::test]
    async fn sync_entry_is_fail_soft_with_an_empty_store() {
        // No stored token → the decision ladder exits at `NoToken` before any
        // PATH lookup, so this never spawns `gh` on the host. Completing at
        // all (no panic, no error) is the fail-soft contract.
        let dir = tempfile::tempdir().expect("tempdir");
        let store = FileSecretStore::with_path(dir.path().join("secrets.json"));
        sync_token_to_gh(store).await;
    }
}
