//! Shared remote credential resolution for the network git operations.
//!
//! Both `push` and `fetch` install the same best-effort credential callback
//! (ssh-agent → credential helper). Local/`file://` remotes — the test path —
//! never invoke it. The interactive keychain consent flow the TS service drives
//! is deferred (see the accept-changes parity notes in `intent-services`).
//!
//! libgit2 re-invokes the credentials callback on every auth failure and keeps
//! going until the callback returns `Err` (or a working `Cred`). Falling
//! through to `Cred::default()` produces an anonymous credential that libgit2
//! silently retries, so an auth-shaped failure (missing/rotated keys after
//! device de-pairing, for example) can pin the caller's spawn_blocking worker
//! indefinitely — the runtime-saturation vector behind the FE
//! "JSON-RPC request timed out: host.status" surface. The bounded closure
//! installed by [`remote_callbacks`] gives libgit2 a fixed number of attempts
//! before returning `Err`, mirroring the TS handler's `GIT_TERMINAL_PROMPT=0`
//! fail-fast semantics for both fetch and push.

use git2::{Cred, RemoteCallbacks};

/// Maximum number of times the credential callback is entered per fetch/push
/// before it returns `Err` — enough to cover the ssh-agent, credential-helper,
/// and one retry libgit2 issues per allowed cred type, without leaving room for
/// an unbounded auth-failure loop.
pub(crate) const MAX_CREDENTIAL_ATTEMPTS: u32 = 3;

/// Resolve one credential attempt for the callback wired into [`remote_callbacks`].
/// `attempt` is 0-based; returns `Err` once `attempt >= max_attempts`, otherwise
/// walks the ssh-agent → credential-helper chain and errors when nothing is
/// usable. Pure of the closure state so unit tests can drive the bound directly.
pub(crate) fn resolve_credential(
    url: &str,
    username: Option<&str>,
    allowed: git2::CredentialType,
    attempt: u32,
    max_attempts: u32,
) -> std::result::Result<Cred, git2::Error> {
    if attempt >= max_attempts {
        return Err(git2::Error::from_str(&format!(
            "git authentication failed: exhausted {max_attempts} credential attempts"
        )));
    }
    if allowed.contains(git2::CredentialType::SSH_KEY) {
        if let Ok(cred) = Cred::ssh_key_from_agent(username.unwrap_or("git")) {
            return Ok(cred);
        }
    }
    if allowed.contains(git2::CredentialType::USER_PASS_PLAINTEXT) {
        if let Ok(config) = git2::Config::open_default() {
            if let Ok(cred) = Cred::credential_helper(&config, url, username) {
                return Ok(cred);
            }
        }
    }
    // No credential source produced a usable `Cred` — return `Err` rather than
    // falling through to `Cred::default()` (an anonymous credential libgit2
    // would silently retry, driving the auth-failure loop this module bounds).
    Err(git2::Error::from_str(
        "no usable git credentials (ssh-agent / credential helper)",
    ))
}

/// Build a bounded credentials closure suitable for
/// [`RemoteCallbacks::credentials`]. Each invocation increments a per-callback
/// counter; once it exceeds `max_attempts` the closure returns `Err` so libgit2
/// stops re-entering it. Exposed at the module level so unit tests can drive
/// the counter without a real remote.
pub(crate) fn make_credentials_callback(
    max_attempts: u32,
) -> impl FnMut(&str, Option<&str>, git2::CredentialType) -> std::result::Result<Cred, git2::Error>
{
    let mut attempts: u32 = 0;
    move |url, username, allowed| {
        let n = attempts;
        attempts = attempts.saturating_add(1);
        resolve_credential(url, username, allowed, n, max_attempts)
    }
}

/// Build [`RemoteCallbacks`] with the bounded credential callback installed.
/// Applies to fetch and push alike via [`crate::fetch`], [`crate::push`], and
/// [`crate::remote::ls_remote_has_branch`].
pub(crate) fn remote_callbacks<'cb>() -> RemoteCallbacks<'cb> {
    let mut callbacks = RemoteCallbacks::new();
    callbacks.credentials(make_credentials_callback(MAX_CREDENTIAL_ATTEMPTS));
    callbacks
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The pure resolver returns `Err` the moment `attempt >= max_attempts`,
    /// regardless of the developer's actual credential-helper state — the
    /// invariant every fetch/push relies on to escape a libgit2 auth loop.
    #[test]
    fn resolve_credential_errors_once_attempt_reaches_max() {
        let res = resolve_credential(
            "https://example.invalid/repo.git",
            Some("git"),
            git2::CredentialType::USER_PASS_PLAINTEXT,
            MAX_CREDENTIAL_ATTEMPTS,
            MAX_CREDENTIAL_ATTEMPTS,
        );
        let err = match res {
            Ok(_) => panic!("attempt == max must fail regardless of environment"),
            Err(e) => e,
        };
        assert!(
            err.message().contains("exhausted"),
            "unexpected error message: {}",
            err.message()
        );
    }

    /// The bounded callback closure surfaces `Err` after `MAX_CREDENTIAL_ATTEMPTS`
    /// invocations even when the allowed cred type is empty (no source ever
    /// matches). This is the "no available creds" shape libgit2 sees when auth
    /// keeps failing — the runtime-saturation loop must terminate.
    #[test]
    fn bounded_callback_stops_after_max_invocations() {
        let mut cb = make_credentials_callback(MAX_CREDENTIAL_ATTEMPTS);
        for _ in 0..MAX_CREDENTIAL_ATTEMPTS {
            // Early attempts fail (empty allowed set → no credential source),
            // but the *reason* is the resolver's fallthrough, not the bound.
            let _ = cb(
                "https://example.invalid/repo.git",
                None,
                git2::CredentialType::empty(),
            );
        }
        let res = cb(
            "https://example.invalid/repo.git",
            None,
            git2::CredentialType::empty(),
        );
        let err = match res {
            Ok(_) => panic!("callback must error after MAX_CREDENTIAL_ATTEMPTS"),
            Err(e) => e,
        };
        assert!(
            err.message().contains("exhausted"),
            "unexpected error message: {}",
            err.message()
        );
    }

    /// A fetch/push against a remote requiring auth with no usable credentials
    /// must fail fast rather than spin: libgit2 re-enters the callback until it
    /// returns `Err`, so the bound is what makes the failure observable to the
    /// caller in bounded wall-clock time.
    #[test]
    fn tight_bound_forces_immediate_error_on_first_call() {
        let mut cb = make_credentials_callback(0);
        let res = cb(
            "https://example.invalid/repo.git",
            None,
            git2::CredentialType::USER_PASS_PLAINTEXT,
        );
        let err = match res {
            Ok(_) => panic!("max_attempts=0 must error on the first call"),
            Err(e) => e,
        };
        assert!(err.message().contains("exhausted"));
    }
}
