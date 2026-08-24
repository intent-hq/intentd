//! Best-effort sync of the stored GitHub token into the `gh` CLI.
//!
//! After the device flow authorizes ([`crate::device_flow`]), the daemon
//! offers the freshly stored token to a locally installed `gh` so terminal
//! work is authenticated too. The sync is **fail-soft** by design: `gh`
//! missing, an existing `gh` login (never clobbered), or a login failure all
//! leave the device-flow outcome untouched — callers get no error, only logs.
//!
//! The revoke side mirrors this ([`logout_gh_after_revoke`]): when the daemon
//! token is revoked, `gh` is logged out of github.com — but **only** when its
//! active token is exactly the one being revoked (i.e. the login this sync
//! created). A login we did not create is never touched, and every failure
//! mode leaves the revoke outcome unaffected.
//!
//! 🔒 The token is loaded back from the secret store
//! (`sourceControl.github.token`) here — it is never plumbed out of the
//! device-flow engine — and reaches `gh auth login --with-token` via **stdin
//! only**: never argv (process listings), never logs, and child output is
//! never echoed (`gh auth status` can print masked token material). The
//! revoke-side match compares tokens **in memory only** — neither side ever
//! reaches logs, errors, or argv.

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
pub(crate) enum GhSyncOutcome {
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

/// Terminal outcome of one revoke-side logout attempt (log/test surface only
/// — never wire).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GhLogoutOutcome {
    /// `gh auth logout` succeeded — gh held exactly the revoked token.
    LoggedOut,
    /// No revoked token was captured (nothing was stored) — nothing to match.
    NoToken,
    /// `gh` is not on `PATH` — nothing to do.
    GhNotInstalled,
    /// `gh auth token` reports no github.com login — nothing to log out.
    NotLoggedIn,
    /// gh's active token differs from the revoked one — a login we did not
    /// create is never logged out.
    TokenMismatch,
    /// The logout subprocess failed (spawn error / non-zero exit).
    LogoutFailed,
}

/// Injected command seam: the decision logic ([`sync_with`] /
/// [`logout_with`]) is unit-tested against a mock, so tests never spawn a
/// real `gh` (and can never touch a developer's actual `gh` login).
pub trait GhCli: Send + Sync {
    /// Locate the `gh` binary on `PATH`, or `None` when not installed.
    fn locate(&self) -> Option<PathBuf>;
    /// True iff `gh auth status --hostname github.com` reports logged in.
    fn is_authenticated(&self, gh: &Path) -> bool;
    /// Run `gh auth login --with-token --hostname github.com`, piping the
    /// token via stdin. True on success.
    fn login_with_token(&self, gh: &Path, token: &SecretString) -> bool;
    /// The github.com login gh reports as its active account, or `None` when
    /// it cannot be determined (gh < 2.40, no login, parse failure). Used to
    /// pin the token read and the logout to one named account.
    fn active_login(&self, gh: &Path) -> Option<String>;
    /// The token `gh auth token --hostname github.com` resolves — pinned via
    /// `--user` when `user` is known — or `None` when gh has no matching
    /// github.com login. 🔒 Held in memory only — must never reach logs,
    /// errors, or argv.
    fn active_token(&self, gh: &Path, user: Option<&str>) -> Option<SecretString>;
    /// Run `gh auth logout --hostname github.com`, pinned via `--user` when
    /// `user` is known. True on success.
    fn logout(&self, gh: &Path, user: Option<&str>) -> bool;
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

/// The logout decision ladder: no revoked token → gh missing → gh not logged
/// in → token mismatch → attempt. The mismatch arm is the guardrail: a gh
/// login whose active token is not exactly the revoked one was not created by
/// [`sync_with`], so it is never logged out. Pure over the [`GhCli`] seam so
/// every arm is unit-testable. 🔒 The compare happens in memory only.
///
/// The whole sequence is pinned to one named account when gh reports it
/// (gh ≥ 2.40, the multi-account versions): the token is read with
/// `--user <login>` and the logout names the same login, so (a) multi-account
/// setups log out non-interactively instead of erroring ("unable to determine
/// which account to log out of"), and (b) a concurrent `gh auth switch`
/// between the check and the logout cannot redirect either step to a
/// different account. Residual race: the *named* account re-logging in with
/// a different token inside that window cannot be excluded — the gh CLI has
/// no atomic compare-and-logout — which is accepted for this best-effort,
/// fail-soft cleanup. When no login can be resolved (gh < 2.40 predates both
/// multi-account and `--user`), the unpinned single-account path applies.
fn logout_with(cli: &dyn GhCli, revoked: Option<SecretString>) -> GhLogoutOutcome {
    let Some(revoked) = revoked else {
        return GhLogoutOutcome::NoToken;
    };
    let Some(gh) = cli.locate() else {
        return GhLogoutOutcome::GhNotInstalled;
    };
    let user = cli.active_login(&gh);
    let Some(active) = cli.active_token(&gh, user.as_deref()) else {
        return GhLogoutOutcome::NotLoggedIn;
    };
    if active.expose_secret() != revoked.expose_secret() {
        return GhLogoutOutcome::TokenMismatch;
    }
    if cli.logout(&gh, user.as_deref()) {
        GhLogoutOutcome::LoggedOut
    } else {
        GhLogoutOutcome::LogoutFailed
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

/// Best-effort logout of the `gh` CLI after `github.revoke`. `revoked` is the
/// daemon token captured **before** it was deleted from the secret store; gh
/// is logged out of github.com only when its active token matches it exactly
/// (see [`logout_with`]). Never returns an error: every failure mode is
/// logged (never with token material) and the caller's revoke proceeds
/// unaffected. Runs the blocking work (subprocesses) on the blocking pool,
/// bounded by [`GH_SYNC_TIMEOUT`].
pub async fn logout_gh_after_revoke(revoked: Option<String>) {
    let revoked = revoked
        .filter(|t| !t.trim().is_empty())
        .map(SecretString::from);
    let handle = tokio::task::spawn_blocking(move || {
        let outcome = logout_with(&SystemGhCli, revoked);
        if outcome == GhLogoutOutcome::LoggedOut {
            // The gh-CLI token cache may still hold the token gh just lost;
            // drop it so the next resolution re-probes. Inside the closure so
            // a logout that outlives the timed-out wait still invalidates.
            crate::token::invalidate_gh_cli_cache();
        }
        outcome
    });
    match timeout(GH_SYNC_TIMEOUT, handle).await {
        Ok(Ok(GhLogoutOutcome::LoggedOut)) => {
            tracing::info!("logged the gh CLI out of github.com after token revoke");
        }
        Ok(Ok(GhLogoutOutcome::LogoutFailed)) => {
            tracing::warn!("gh CLI logout failed (gh auth logout); revoke unaffected");
        }
        Ok(Ok(outcome)) => {
            tracing::debug!(?outcome, "skipped gh CLI logout after revoke");
        }
        Ok(Err(join_err)) => {
            tracing::warn!(error = %join_err, "gh CLI logout task failed");
        }
        Err(_) => {
            // Only the wait is abandoned (see [`GH_SYNC_TIMEOUT`]): the
            // blocking closure keeps running and the logout may still
            // complete after this line is logged.
            tracing::warn!(
                "gh CLI logout still running after 10s; no longer waiting (it may still \
                 complete in the background)"
            );
        }
    }
}

/// Production [`GhCli`]: real `PATH` lookup + real `gh` subprocesses.
struct SystemGhCli;

impl GhCli for SystemGhCli {
    fn locate(&self) -> Option<PathBuf> {
        crate::token::find_gh_binary()
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
            .is_ok_and(|s| s.success())
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
        child.wait().is_ok_and(|s| s.success())
    }

    fn active_login(&self, gh: &Path) -> Option<String> {
        // 🔒 `gh auth status` output carries (masked) token material: it is
        // parsed in memory only and never logged. Checked on both streams —
        // gh has printed status to stderr historically and stdout since 2.40.
        let out = Command::new(gh)
            .args(["auth", "status", "--hostname", "github.com"])
            .stdin(Stdio::null())
            .output()
            .ok()?;
        if !out.status.success() {
            return None;
        }
        parse_active_login(&String::from_utf8_lossy(&out.stdout))
            .or_else(|| parse_active_login(&String::from_utf8_lossy(&out.stderr)))
    }

    fn active_token(&self, gh: &Path, user: Option<&str>) -> Option<SecretString> {
        // 🔒 stdout IS the token: captured in memory only, wrapped in a
        // `SecretString` immediately, never logged. stderr is discarded.
        // The login name is not a secret, so `--user` via argv is fine.
        let mut cmd = Command::new(gh);
        cmd.args(["auth", "token", "--hostname", "github.com"]);
        if let Some(user) = user {
            cmd.args(["--user", user]);
        }
        let out = cmd
            .stdin(Stdio::null())
            .stderr(Stdio::null())
            .output()
            .ok()?;
        if !out.status.success() {
            return None;
        }
        let token = String::from_utf8(out.stdout).ok()?;
        let trimmed = token.trim();
        (!trimmed.is_empty()).then(|| SecretString::from(trimmed.to_string()))
    }

    fn logout(&self, gh: &Path, user: Option<&str>) -> bool {
        // Output is discarded: only the exit code matters, so no error path
        // can echo credential material.
        let mut cmd = Command::new(gh);
        cmd.args(["auth", "logout", "--hostname", "github.com"]);
        if let Some(user) = user {
            cmd.args(["--user", user]);
        }
        cmd.stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .is_ok_and(|s| s.success())
    }
}

/// Extract the active github.com login from `gh auth status` output: the
/// account named by a "Logged in to github.com account <login>" line whose
/// block carries "Active account: true". Both markers exist only on gh ≥ 2.40
/// (older gh says "as <login>" and has no active-account concept), exactly
/// the versions whose `auth token` / `auth logout` accept `--user` — so a
/// `None` here self-gates the pinning to the gh versions that support it.
/// 🔒 The input carries (masked) token material — callers must never log it.
fn parse_active_login(status: &str) -> Option<String> {
    let mut candidate: Option<&str> = None;
    for line in status.lines() {
        if let Some(rest) = line.split("Logged in to github.com account ").nth(1) {
            candidate = rest.split_whitespace().next();
        }
        if line.contains("Active account: true") {
            if let Some(login) = candidate {
                return Some(login.to_string());
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Mutex;

    use super::*;

    /// Scripted [`GhCli`] that records whether a login/logout was attempted —
    /// no real `gh` is ever spawned from tests.
    // Test mock: independent scenario bools, and `Option<Option<_>>` recorders
    // distinguishing "never called" from "called with None".
    #[allow(clippy::struct_excessive_bools, clippy::option_option)]
    struct MockGhCli {
        installed: bool,
        authenticated: bool,
        login_succeeds: bool,
        login_attempted: AtomicBool,
        active_login: Option<&'static str>,
        active_token: Option<&'static str>,
        logout_succeeds: bool,
        logout_attempted: AtomicBool,
        token_user_seen: Mutex<Option<Option<String>>>,
        logout_user_seen: Mutex<Option<Option<String>>>,
    }

    impl MockGhCli {
        fn new(installed: bool, authenticated: bool, login_succeeds: bool) -> Self {
            Self {
                installed,
                authenticated,
                login_succeeds,
                login_attempted: AtomicBool::new(false),
                active_login: None,
                active_token: None,
                logout_succeeds: false,
                logout_attempted: AtomicBool::new(false),
                token_user_seen: Mutex::new(None),
                logout_user_seen: Mutex::new(None),
            }
        }

        fn for_logout(
            installed: bool,
            active_token: Option<&'static str>,
            logout_succeeds: bool,
        ) -> Self {
            Self {
                installed,
                authenticated: active_token.is_some(),
                login_succeeds: false,
                login_attempted: AtomicBool::new(false),
                active_login: None,
                active_token,
                logout_succeeds,
                logout_attempted: AtomicBool::new(false),
                token_user_seen: Mutex::new(None),
                logout_user_seen: Mutex::new(None),
            }
        }

        fn with_login(mut self, login: &'static str) -> Self {
            self.active_login = Some(login);
            self
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

        fn active_login(&self, _gh: &Path) -> Option<String> {
            self.active_login.map(str::to_string)
        }

        fn active_token(&self, _gh: &Path, user: Option<&str>) -> Option<SecretString> {
            *self.token_user_seen.lock().unwrap() = Some(user.map(str::to_string));
            self.active_token.map(SecretString::from)
        }

        fn logout(&self, _gh: &Path, user: Option<&str>) -> bool {
            *self.logout_user_seen.lock().unwrap() = Some(user.map(str::to_string));
            self.logout_attempted.store(true, Ordering::SeqCst);
            self.logout_succeeds
        }
    }

    #[allow(clippy::unnecessary_wraps)] // helper mirrors the Option the API under test takes
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

    #[allow(clippy::unnecessary_wraps)] // helper mirrors the Option the API under test takes
    fn revoked() -> Option<SecretString> {
        Some(SecretString::from("gho_test_revoked"))
    }

    #[test]
    fn logs_out_when_gh_holds_the_revoked_token() {
        let cli = MockGhCli::for_logout(true, Some("gho_test_revoked"), true);
        assert_eq!(logout_with(&cli, revoked()), GhLogoutOutcome::LoggedOut);
        assert!(cli.logout_attempted.load(Ordering::SeqCst));
    }

    #[test]
    fn never_logs_out_a_login_we_did_not_create() {
        let cli = MockGhCli::for_logout(true, Some("gho_someone_elses"), true);
        assert_eq!(logout_with(&cli, revoked()), GhLogoutOutcome::TokenMismatch);
        assert!(!cli.logout_attempted.load(Ordering::SeqCst));
    }

    #[test]
    fn logout_skips_without_a_revoked_token() {
        let cli = MockGhCli::for_logout(true, Some("gho_test_revoked"), true);
        assert_eq!(logout_with(&cli, None), GhLogoutOutcome::NoToken);
        assert!(!cli.logout_attempted.load(Ordering::SeqCst));
    }

    #[test]
    fn logout_skips_when_gh_is_not_installed() {
        let cli = MockGhCli::for_logout(false, Some("gho_test_revoked"), true);
        assert_eq!(
            logout_with(&cli, revoked()),
            GhLogoutOutcome::GhNotInstalled
        );
        assert!(!cli.logout_attempted.load(Ordering::SeqCst));
    }

    #[test]
    fn logout_skips_when_gh_is_not_logged_in() {
        let cli = MockGhCli::for_logout(true, None, true);
        assert_eq!(logout_with(&cli, revoked()), GhLogoutOutcome::NotLoggedIn);
        assert!(!cli.logout_attempted.load(Ordering::SeqCst));
    }

    #[test]
    fn logout_failure_is_reported_not_raised() {
        let cli = MockGhCli::for_logout(true, Some("gho_test_revoked"), false);
        assert_eq!(logout_with(&cli, revoked()), GhLogoutOutcome::LogoutFailed);
        assert!(cli.logout_attempted.load(Ordering::SeqCst));
    }

    #[test]
    fn logout_pins_token_read_and_logout_to_the_active_login() {
        // gh ≥ 2.40 reports the active login: both the token read and the
        // logout must name it via --user, so multi-account setups log out
        // non-interactively and a concurrent account switch cannot redirect
        // either step.
        let cli = MockGhCli::for_logout(true, Some("gho_test_revoked"), true).with_login("octocat");
        assert_eq!(logout_with(&cli, revoked()), GhLogoutOutcome::LoggedOut);
        assert_eq!(
            *cli.token_user_seen.lock().unwrap(),
            Some(Some("octocat".to_string()))
        );
        assert_eq!(
            *cli.logout_user_seen.lock().unwrap(),
            Some(Some("octocat".to_string()))
        );
    }

    #[test]
    fn logout_stays_unpinned_when_no_login_is_reported() {
        // gh < 2.40 (single-account, no --user support) reports no login:
        // both steps run unpinned, matching the old single-account behavior.
        let cli = MockGhCli::for_logout(true, Some("gho_test_revoked"), true);
        assert_eq!(logout_with(&cli, revoked()), GhLogoutOutcome::LoggedOut);
        assert_eq!(*cli.token_user_seen.lock().unwrap(), Some(None));
        assert_eq!(*cli.logout_user_seen.lock().unwrap(), Some(None));
    }

    #[test]
    fn parses_the_active_login_from_gh_auth_status() {
        // Multi-account gh ≥ 2.40 shape: only the block with
        // "Active account: true" names the login to pin.
        let status = "github.com\n\
             ✓ Logged in to github.com account inactive-user (keyring)\n\
             - Active account: false\n\
             - Git operations protocol: https\n\
             ✓ Logged in to github.com account octocat (keyring)\n\
             - Active account: true\n\
             - Token: gho_************************************\n";
        assert_eq!(parse_active_login(status), Some("octocat".to_string()));
    }

    #[test]
    fn active_login_is_none_for_pre_multi_account_gh_output() {
        // gh < 2.40 prints "as <login>" and has no active-account marker —
        // None keeps the sequence unpinned, which those versions accept.
        let status = "github.com\n\
             ✓ Logged in to github.com as octocat (keyring)\n\
             ✓ Token: gho_************************************\n";
        assert_eq!(parse_active_login(status), None);
        assert_eq!(parse_active_login(""), None);
    }

    #[test]
    fn logout_outcome_debug_never_carries_a_token() {
        // The outcome enum is the only logout surface that reaches logs.
        for outcome in [
            GhLogoutOutcome::LoggedOut,
            GhLogoutOutcome::NoToken,
            GhLogoutOutcome::GhNotInstalled,
            GhLogoutOutcome::NotLoggedIn,
            GhLogoutOutcome::TokenMismatch,
            GhLogoutOutcome::LogoutFailed,
        ] {
            assert!(!format!("{outcome:?}").contains("gho_"));
        }
    }

    #[tokio::test]
    async fn logout_entry_is_fail_soft_without_a_revoked_token() {
        // No captured token → the decision ladder exits at `NoToken` before
        // any PATH lookup, so this never spawns `gh` on the host. Completing
        // at all (no panic, no error) is the fail-soft contract. Blank
        // captures fold to `NoToken` too.
        logout_gh_after_revoke(None).await;
        logout_gh_after_revoke(Some("   ".to_string())).await;
    }
}
