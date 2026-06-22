//! Shared remote credential resolution for the network git operations.
//!
//! Both `push` and `fetch` install the same best-effort credential callback
//! (default → ssh-agent → credential helper). Local/`file://` remotes — the test
//! path — never invoke it. The interactive keychain consent flow the TS service
//! drives is deferred (see the accept-changes parity notes in `intent-services`).

use git2::{Cred, RemoteCallbacks};

/// Best-effort credential resolution for non-local remotes: SSH agent for SSH
/// remotes, then the configured credential helper, then default. Local/`file://`
/// remotes never invoke this callback.
pub(crate) fn credentials_cb(
    url: &str,
    username: Option<&str>,
    allowed: git2::CredentialType,
) -> std::result::Result<Cred, git2::Error> {
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
    Cred::default()
}

/// Build [`RemoteCallbacks`] with the shared credential callback installed.
pub(crate) fn remote_callbacks<'cb>() -> RemoteCallbacks<'cb> {
    let mut callbacks = RemoteCallbacks::new();
    callbacks.credentials(credentials_cb);
    callbacks
}
