//! Shared remote credential resolution for the libgit2-backed network git
//! operations (currently `push` and `ls_remote_has_branch`; `fetch` shells out
//! to system `git` and does not go through this callback). Local/`file://`
//! remotes — the test path — never invoke it. The interactive keychain consent
//! flow the TS service drives is deferred (see the accept-changes parity
//! notes in `intent-services`).
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
//! fail-fast semantics for the remaining libgit2 network paths.

use git2::{Cred, RemoteCallbacks};

/// Maximum number of times the credential callback is entered per fetch/push
/// before it returns `Err` — attempt 0 walks the full ssh-agent →
/// credential-helper → resolved-token chain, and the re-entries (which skip
/// the helper, see [`resolve_credential`]) give the ssh-agent and token steps
/// one more turn each after a server rejection, without leaving room for an
/// unbounded auth-failure loop.
pub(crate) const MAX_CREDENTIAL_ATTEMPTS: u32 = 3;

/// Username GitHub expects when a token is presented as an HTTPS basic-auth
/// password (OAuth / device-flow / installation tokens alike).
pub const TOKEN_USERNAME: &str = "x-access-token";

/// Environment variable carrying the caller-resolved GitHub token into a
/// shelled-out git child's credential helper (see [`token_helper_config`]).
/// The value never appears on the command line.
pub const TOKEN_ENV: &str = "INTENT_GIT_GITHUB_TOKEN";

/// The `git -c` config entry offering the resolved token as a github.com-scoped
/// credential helper, shared by the shell-git paths (`fetch`, and the
/// `intent-services` clone pipeline). Note the `{{`/`}}`/`{TOKEN_ENV}` are
/// **Rust** `format!` escapes and interpolation — the shell sees a plain
/// `"$INTENT_GIT_GITHUB_TOKEN"` expansion (no token bytes in the string
/// itself). `|| exit 0` keeps the helper silent-but-successful for the
/// `store`/`erase` ops git may also invoke.
pub(crate) fn token_helper_config() -> String {
    format!(
        "credential.https://github.com.helper=!f() {{ test \"$1\" = get || exit 0; printf 'username={TOKEN_USERNAME}\\npassword=%s\\n' \"${TOKEN_ENV}\"; }}; f"
    )
}

/// Environment variable git reads for command-line-scoped config entries —
/// the same mechanism `git -c` uses to reach child processes. Entries here
/// rank as "command line" config: applied after every config file, so an
/// appended helper never displaces a user-configured one.
pub const GIT_CONFIG_PARAMETERS_ENV: &str = "GIT_CONFIG_PARAMETERS";

/// Build the environment pairs a spawn site injects to offer `token` as a
/// github.com-scoped credential helper (monorepo#884): the sq-quoted
/// [`token_helper_config`] entry **appended** to any
/// `inherited_config_parameters` (the caller's pre-existing
/// [`GIT_CONFIG_PARAMETERS_ENV`] value, so inherited entries — and the user's
/// configured helpers, which git applies first — keep winning), plus
/// [`TOKEN_ENV`] carrying the token itself. The config value never contains
/// token bytes; the token travels only under [`TOKEN_ENV`]. Returns no pairs
/// when the token is unusable (see [`usable_token`]), leaving the child env
/// untouched.
pub fn scoped_credential_env(
    token: Option<&str>,
    inherited_config_parameters: Option<&str>,
) -> Vec<(String, String)> {
    let Some(token) = usable_token(token) else {
        return Vec::new();
    };
    let entry = sq_quote(&token_helper_config());
    let params = match inherited_config_parameters {
        Some(prev) if !prev.trim().is_empty() => format!("{prev} {entry}"),
        _ => entry,
    };
    vec![
        (GIT_CONFIG_PARAMETERS_ENV.to_string(), params),
        (TOKEN_ENV.to_string(), token.to_string()),
    ]
}

/// The `git -c` config entry offering the daemon-backed
/// `intentd git-credential` helper for github.com (monorepo#884 Phase 2.2):
/// `!<intentd> git-credential` — git runs the `!`-prefixed value through
/// `sh -c` with the operation appended, so `intentd_path` is sh-quoted to
/// survive spaces and quotes in the install path. No token bytes anywhere:
/// the helper fetches the credential from the daemon over UDS on demand.
pub(crate) fn daemon_helper_config(intentd_path: &str) -> String {
    format!(
        "credential.https://github.com.helper=!{} git-credential",
        sh_quote(intentd_path)
    )
}

/// Build the environment pairs a spawn site injects to offer the
/// daemon-backed `intentd git-credential` helper to a child's git
/// (monorepo#884 Phase 2.2): the single [`GIT_CONFIG_PARAMETERS_ENV`] pair
/// carrying the sq-quoted [`daemon_helper_config`] entry **appended** to any
/// `inherited_config_parameters` (the caller's pre-existing value, so
/// inherited entries — and the user's configured helpers, which git applies
/// first — keep winning). Unlike [`scoped_credential_env`] there is no token
/// pair at all: no token bytes ever enter the child environment.
pub fn daemon_helper_env(
    intentd_path: &str,
    inherited_config_parameters: Option<&str>,
) -> Vec<(String, String)> {
    let entry = sq_quote(&daemon_helper_config(intentd_path));
    let params = match inherited_config_parameters {
        Some(prev) if !prev.trim().is_empty() => format!("{prev} {entry}"),
        _ => entry,
    };
    vec![(GIT_CONFIG_PARAMETERS_ENV.to_string(), params)]
}

/// Single-quote `src` for the POSIX shell git hands a `!`-prefixed helper
/// value to: wrap in `'…'` and escape embedded `'` as `'\''`. Distinct from
/// [`sq_quote`], which targets git's own `sq_dequote` parser (that layer is
/// applied on top when the entry travels via `GIT_CONFIG_PARAMETERS`).
fn sh_quote(src: &str) -> String {
    let mut out = String::with_capacity(src.len() + 2);
    out.push('\'');
    for c in src.chars() {
        if c == '\'' {
            out.push_str("'\\''");
        } else {
            out.push(c);
        }
    }
    out.push('\'');
    out
}

/// Single-quote `src` for `GIT_CONFIG_PARAMETERS`, mirroring git's own
/// `sq_quote_buf` (quote.c): wrap in `'…'` and escape embedded `'` and `!`
/// as `'\''` / `'\!'` — the exact forms git's `sq_dequote` parser accepts.
fn sq_quote(src: &str) -> String {
    let mut out = String::with_capacity(src.len() + 2);
    out.push('\'');
    for c in src.chars() {
        if c == '\'' || c == '!' {
            out.push('\'');
            out.push('\\');
            out.push(c);
            out.push('\'');
        } else {
            out.push(c);
        }
    }
    out.push('\'');
    out
}

/// Whether `url` is an HTTPS remote on `github.com` (the only host the
/// resolved-token fallback applies to). Handles optional userinfo and port in
/// the authority; subdomains and other hosts do not match. Exposed so callers
/// (e.g. `intent-services`) can skip token resolution entirely for remotes the
/// fallback would never apply to.
pub fn is_https_github_url(url: &str) -> bool {
    let Some(scheme) = url.get(..8) else {
        return false;
    };
    if !scheme.eq_ignore_ascii_case("https://") {
        return false;
    }
    let authority = url[8..].split(['/', '?', '#']).next().unwrap_or("");
    let host = authority.rsplit('@').next().unwrap_or(authority);
    let host = host.split(':').next().unwrap_or(host);
    host.eq_ignore_ascii_case("github.com")
}

/// Trim the resolved token and reject values that cannot travel as an HTTPS
/// basic-auth password or through the line-oriented git-credential protocol
/// (control characters — `\n`/`\r`/`\0` and friends — would corrupt either).
/// `None`/empty/invalid all normalize to `None` so every consumer applies the
/// same "no usable token" rule. The token value is never logged.
pub fn usable_token(token: Option<&str>) -> Option<&str> {
    let token = token?.trim();
    if token.is_empty() || token.chars().any(|c| c.is_control()) {
        return None;
    }
    Some(token)
}

/// Final chain step: the caller-resolved GitHub token as an HTTPS basic-auth
/// credential (username [`TOKEN_USERNAME`]). Applies only when libgit2 allows
/// userpass credentials, the remote is an HTTPS `github.com` URL, and a
/// usable token was resolved (see [`usable_token`]); otherwise `None` so the
/// caller falls through to the no-credential error. The token value is never
/// logged.
pub(crate) fn token_fallback(
    url: &str,
    allowed: git2::CredentialType,
    token: Option<&str>,
) -> Option<Cred> {
    if !allowed.contains(git2::CredentialType::USER_PASS_PLAINTEXT) {
        return None;
    }
    if !is_https_github_url(url) {
        return None;
    }
    let token = usable_token(token)?;
    Cred::userpass_plaintext(TOKEN_USERNAME, token).ok()
}

/// Resolve one credential attempt for the callback wired into [`remote_callbacks`].
/// `attempt` is 0-based; returns `Err` once `attempt >= max_attempts`, otherwise
/// walks the ssh-agent → credential-helper → resolved-token chain and errors
/// when nothing is usable. The token step is last so existing ssh-agent /
/// credential-helper setups are untouched, and it only applies to HTTPS
/// `github.com` remotes (see [`token_fallback`]).
///
/// libgit2 only re-enters the callback after the server *rejected* the
/// previous answer, so on re-entry (`attempt >= 1`) the credential-helper step
/// is skipped: a stale helper credential (e.g. an expired PAT in the OS
/// keychain) would otherwise be returned identically on every attempt,
/// starving the token step of its turn. Pure of the closure state so unit
/// tests can drive the bound directly.
pub(crate) fn resolve_credential(
    url: &str,
    username: Option<&str>,
    allowed: git2::CredentialType,
    attempt: u32,
    max_attempts: u32,
    token: Option<&str>,
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
    if attempt == 0 && allowed.contains(git2::CredentialType::USER_PASS_PLAINTEXT) {
        if let Ok(config) = git2::Config::open_default() {
            if let Ok(cred) = Cred::credential_helper(&config, url, username) {
                return Ok(cred);
            }
        }
    }
    if let Some(cred) = token_fallback(url, allowed, token) {
        return Ok(cred);
    }
    // No credential source produced a usable `Cred` — return `Err` rather than
    // falling through to `Cred::default()` (an anonymous credential libgit2
    // would silently retry, driving the auth-failure loop this module bounds).
    // Mention the token step only where it was actually in play: an HTTPS
    // github.com remote with a usable token and a userpass-capable ask.
    let token_in_play = is_https_github_url(url)
        && usable_token(token).is_some()
        && allowed.contains(git2::CredentialType::USER_PASS_PLAINTEXT);
    let sources = if token_in_play {
        "ssh-agent / credential helper / GitHub token"
    } else {
        "ssh-agent / credential helper"
    };
    Err(git2::Error::from_str(&format!(
        "no usable git credentials ({sources})"
    )))
}

/// Build a bounded credentials closure suitable for
/// [`RemoteCallbacks::credentials`]. Each invocation increments a per-callback
/// counter; once it reaches `max_attempts` the closure returns `Err` so libgit2
/// stops re-entering it. `token` is the caller-resolved GitHub token (if any)
/// used as the final chain step. Exposed at the module level so unit tests can
/// drive the counter without a real remote.
pub(crate) fn make_credentials_callback(
    max_attempts: u32,
    token: Option<String>,
) -> impl FnMut(&str, Option<&str>, git2::CredentialType) -> std::result::Result<Cred, git2::Error>
{
    let mut attempts: u32 = 0;
    move |url, username, allowed| {
        let n = attempts;
        attempts = attempts.saturating_add(1);
        resolve_credential(url, username, allowed, n, max_attempts, token.as_deref())
    }
}

/// Build [`RemoteCallbacks`] with the bounded credential callback installed.
/// `token` is an optional caller-resolved GitHub token applied as the final
/// chain step for HTTPS `github.com` remotes. Applies to the libgit2-backed
/// remote operations that still install these callbacks: [`crate::push`] and
/// [`crate::remote::ls_remote_has_branch`]. (`crate::fetch` shells out to
/// system `git`, so it does not use this callback — see the module docs.)
pub(crate) fn remote_callbacks<'cb>(token: Option<&str>) -> RemoteCallbacks<'cb> {
    let mut callbacks = RemoteCallbacks::new();
    callbacks.credentials(make_credentials_callback(
        MAX_CREDENTIAL_ATTEMPTS,
        token.map(str::to_owned),
    ));
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
            None,
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
        let mut cb = make_credentials_callback(MAX_CREDENTIAL_ATTEMPTS, None);
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
        let mut cb = make_credentials_callback(0, None);
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

    /// Host matching for the token fallback: only HTTPS `github.com` remotes
    /// qualify — never SSH, other hosts, subdomains, or lookalike suffixes.
    #[test]
    fn https_github_url_matching() {
        assert!(is_https_github_url("https://github.com/owner/repo.git"));
        assert!(is_https_github_url("HTTPS://GITHUB.COM/owner/repo.git"));
        assert!(is_https_github_url(
            "https://user@github.com/owner/repo.git"
        ));
        assert!(is_https_github_url("https://github.com:443/owner/repo.git"));
        assert!(!is_https_github_url("http://github.com/owner/repo.git"));
        assert!(!is_https_github_url("ssh://git@github.com/owner/repo.git"));
        assert!(!is_https_github_url("git@github.com:owner/repo.git"));
        assert!(!is_https_github_url("https://gitlab.com/owner/repo.git"));
        assert!(!is_https_github_url(
            "https://gist.github.com/owner/repo.git"
        ));
        assert!(!is_https_github_url(
            "https://github.com.evil.example/repo.git"
        ));
        assert!(!is_https_github_url(""));
    }

    /// The token step produces a userpass credential only for HTTPS github.com
    /// remotes when libgit2 allows `USER_PASS_PLAINTEXT` — the deterministic
    /// half of the chain, independent of the developer's ssh-agent / helper.
    #[test]
    fn token_fallback_applies_only_to_https_github_userpass() {
        let userpass = git2::CredentialType::USER_PASS_PLAINTEXT;
        let cred = token_fallback("https://github.com/o/r.git", userpass, Some("tok"))
            .expect("token must yield a credential for HTTPS github.com");
        drop(cred);
        // Wrong host / scheme → no token credential.
        assert!(token_fallback("https://gitlab.com/o/r.git", userpass, Some("tok")).is_none());
        assert!(token_fallback("ssh://git@github.com/o/r.git", userpass, Some("tok")).is_none());
        // Userpass not allowed (ssh-only ask) → no token credential.
        assert!(token_fallback(
            "https://github.com/o/r.git",
            git2::CredentialType::SSH_KEY,
            Some("tok")
        )
        .is_none());
        // No / empty token → no token credential.
        assert!(token_fallback("https://github.com/o/r.git", userpass, None).is_none());
        assert!(token_fallback("https://github.com/o/r.git", userpass, Some("")).is_none());
    }

    /// With a token but an ssh-only credential ask, the resolver must never
    /// answer with the token: the ssh-agent step is allowed to fail and the
    /// chain falls through to the structured error (order preserved).
    #[test]
    fn resolver_never_answers_ssh_ask_with_token() {
        let res = resolve_credential(
            "ssh://git@example.invalid/repo.git",
            Some("git"),
            git2::CredentialType::SSH_KEY,
            0,
            MAX_CREDENTIAL_ATTEMPTS,
            Some("tok"),
        );
        if let Ok(cred) = res {
            // Only reachable when a local ssh-agent supplied an identity —
            // never the token (the token step requires USER_PASS_PLAINTEXT).
            drop(cred);
        }
    }

    /// Token normalization: whitespace is trimmed, and empty or
    /// control-character-bearing values (which would corrupt basic-auth or the
    /// git-credential protocol) are rejected outright.
    #[test]
    fn usable_token_normalizes_and_rejects_invalid() {
        assert_eq!(usable_token(Some("tok")), Some("tok"));
        assert_eq!(usable_token(Some("  tok\n")), Some("tok"));
        assert_eq!(usable_token(None), None);
        assert_eq!(usable_token(Some("")), None);
        assert_eq!(usable_token(Some("   ")), None);
        assert_eq!(usable_token(Some("to\nk")), None);
        assert_eq!(usable_token(Some("to\rk")), None);
        assert_eq!(usable_token(Some("to\0k")), None);
    }

    /// On re-entry (the server rejected the previous answer) the credential
    /// helper is skipped, so a stale helper credential cannot starve the token
    /// step: attempt 1 for a userpass github.com ask must answer with the
    /// token deterministically, regardless of the developer's helper state.
    #[test]
    fn reentry_skips_helper_and_reaches_token() {
        let res = resolve_credential(
            "https://github.com/o/r.git",
            None,
            git2::CredentialType::USER_PASS_PLAINTEXT,
            1,
            MAX_CREDENTIAL_ATTEMPTS,
            Some("tok"),
        );
        assert!(
            res.is_ok(),
            "attempt 1 must reach the token step: {:?}",
            res.err().map(|e| e.message().to_string())
        );
    }

    /// Without a token the resolver errors exactly as before for a userpass
    /// ask against a non-github host with no helper hit — the pre-existing
    /// fall-through contract that keeps the auth loop bounded.
    #[test]
    fn no_token_fall_through_errors_as_before() {
        let res = resolve_credential(
            "https://github.example.invalid/repo.git",
            None,
            git2::CredentialType::USER_PASS_PLAINTEXT,
            0,
            MAX_CREDENTIAL_ATTEMPTS,
            None,
        );
        if let Err(err) = res {
            assert!(
                err.message().contains("no usable git credentials"),
                "unexpected error message: {}",
                err.message()
            );
        }
        // (`Ok` is only reachable when the developer's credential helper
        // answers for this host — environment-dependent, so not asserted.)
    }

    /// The env builder yields exactly the two pairs: `GIT_CONFIG_PARAMETERS`
    /// carrying the sq-quoted github.com-scoped helper (no token bytes), and
    /// `TOKEN_ENV` carrying the token — and real git parses the quoting back
    /// to the exact helper string.
    #[test]
    fn scoped_credential_env_builds_parseable_helper_without_token_bytes() {
        let token = "ghp_secret1234567890";
        let pairs = scoped_credential_env(Some(token), None);
        assert_eq!(pairs.len(), 2);
        let (k0, params) = &pairs[0];
        assert_eq!(k0, GIT_CONFIG_PARAMETERS_ENV);
        assert!(
            !params.contains(token),
            "config value must not embed the token"
        );
        assert_eq!(pairs[1], (TOKEN_ENV.to_string(), token.to_string()));

        // Round-trip through real git: the command-line-scoped entry must
        // dequote back to the exact helper snippet.
        let out = std::process::Command::new("git")
            .env(GIT_CONFIG_PARAMETERS_ENV, params)
            .args(["config", "--get", "credential.https://github.com.helper"])
            .output()
            .expect("git must be runnable");
        assert!(out.status.success(), "git must parse the quoted parameters");
        let value = String::from_utf8_lossy(&out.stdout);
        let expected = token_helper_config();
        let expected_value = expected
            .strip_prefix("credential.https://github.com.helper=")
            .unwrap();
        assert_eq!(value.trim_end_matches('\n'), expected_value);
    }

    /// A pre-existing `GIT_CONFIG_PARAMETERS` value is preserved and the
    /// helper entry is appended after it (space-separated), so inherited
    /// entries keep their precedence and both remain parseable by git.
    #[test]
    fn scoped_credential_env_appends_to_inherited_parameters() {
        let inherited = "'foo.bar=baz'";
        let pairs = scoped_credential_env(Some("tok"), Some(inherited));
        let params = &pairs[0].1;
        assert!(
            params.starts_with("'foo.bar=baz' '"),
            "inherited entry must come first: {params}"
        );
        let out = std::process::Command::new("git")
            .env(GIT_CONFIG_PARAMETERS_ENV, params)
            .args(["config", "--get", "foo.bar"])
            .output()
            .expect("git must be runnable");
        assert!(out.status.success());
        assert_eq!(String::from_utf8_lossy(&out.stdout).trim_end(), "baz");

        // Blank inherited values are treated as absent — no stray separator.
        let pairs = scoped_credential_env(Some("tok"), Some("   "));
        assert!(
            pairs[0].1.starts_with('\''),
            "no leading junk: {}",
            pairs[0].1
        );
    }

    /// No usable token → no pairs at all, so the spawn site leaves the child
    /// environment (including any inherited `GIT_CONFIG_PARAMETERS`) alone.
    #[test]
    fn scoped_credential_env_empty_without_usable_token() {
        for token in [None, Some(""), Some("   "), Some("bad\ntoken")] {
            assert!(scoped_credential_env(token, Some("'foo.bar=baz'")).is_empty());
        }
    }

    /// The sq-quoting matches git's `sq_quote_buf`: `'` and `!` escape as
    /// `'\''` / `'\!'` — the helper snippet contains both.
    #[test]
    fn sq_quote_escapes_like_git() {
        assert_eq!(sq_quote("plain"), "'plain'");
        assert_eq!(sq_quote("a'b"), "'a'\\''b'");
        assert_eq!(sq_quote("!f"), "''\\!'f'");
    }

    /// The daemon-helper env builder yields exactly one pair — the
    /// `GIT_CONFIG_PARAMETERS` entry — and real git dequotes it back to the
    /// exact `!<path> git-credential` helper value, even for a binary path
    /// with spaces and an embedded single quote.
    #[test]
    fn daemon_helper_env_builds_parseable_helper() {
        for path in ["/usr/local/bin/intentd", "/Apps/In tent'd/bin/intentd"] {
            let pairs = daemon_helper_env(path, None);
            assert_eq!(pairs.len(), 1, "single env pair — no token pair");
            let (key, params) = &pairs[0];
            assert_eq!(key, GIT_CONFIG_PARAMETERS_ENV);

            let out = std::process::Command::new("git")
                .env(GIT_CONFIG_PARAMETERS_ENV, params)
                .args(["config", "--get", "credential.https://github.com.helper"])
                .output()
                .expect("git must be runnable");
            assert!(out.status.success(), "git must parse the quoted parameters");
            let value = String::from_utf8_lossy(&out.stdout);
            let expected = daemon_helper_config(path);
            let expected_value = expected
                .strip_prefix("credential.https://github.com.helper=")
                .unwrap();
            assert_eq!(
                value.trim_end_matches('\n'),
                expected_value,
                "path {path:?}"
            );
        }
    }

    /// A pre-existing `GIT_CONFIG_PARAMETERS` value is preserved and the
    /// daemon-helper entry is appended after it, mirroring the
    /// [`scoped_credential_env`] composition rule; blank inherited values are
    /// treated as absent.
    #[test]
    fn daemon_helper_env_appends_to_inherited_parameters() {
        let pairs = daemon_helper_env("/usr/local/bin/intentd", Some("'foo.bar=baz'"));
        let params = &pairs[0].1;
        assert!(
            params.starts_with("'foo.bar=baz' '"),
            "inherited entry must come first: {params}"
        );
        let out = std::process::Command::new("git")
            .env(GIT_CONFIG_PARAMETERS_ENV, params)
            .args(["config", "--get", "foo.bar"])
            .output()
            .expect("git must be runnable");
        assert!(out.status.success());
        assert_eq!(String::from_utf8_lossy(&out.stdout).trim_end(), "baz");

        let pairs = daemon_helper_env("/usr/local/bin/intentd", Some("   "));
        assert!(
            pairs[0].1.starts_with('\''),
            "no leading junk: {}",
            pairs[0].1
        );
    }

    /// The `!`-prefixed helper value runs through `sh -c` with the operation
    /// appended: a sh-quoted stand-in "binary" whose path contains a space
    /// and a single quote must receive `git-credential <op>` as its argv.
    #[cfg(unix)]
    #[test]
    fn daemon_helper_shell_invocation_survives_quoted_path() {
        use std::os::unix::fs::PermissionsExt;
        // Guard-cleaned scratch dir (no tempfile dev-dep in this crate).
        struct Scratch(std::path::PathBuf);
        impl Drop for Scratch {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
        let dir =
            Scratch(std::env::temp_dir().join(format!("intent-git-helper-{}", std::process::id())));
        let bin_dir = dir.0.join("in tent'd");
        std::fs::create_dir_all(&bin_dir).expect("mkdir quoted bin dir");
        let stub = bin_dir.join("intentd");
        let capture = dir.0.join("capture");
        std::fs::write(
            &stub,
            format!(
                "#!/bin/sh\nprintf '%s\\n' \"$@\" > {}\n",
                sh_quote(&capture.to_string_lossy())
            ),
        )
        .expect("write stub");
        std::fs::set_permissions(&stub, std::fs::Permissions::from_mode(0o755)).unwrap();

        let config = daemon_helper_config(&stub.to_string_lossy());
        let helper_value = config
            .strip_prefix("credential.https://github.com.helper=!")
            .expect("shell-helper prefix");
        // git invokes `sh -c '<value> "$@"' <value> get` for a `!` helper.
        let status = std::process::Command::new("sh")
            .args(["-c", &format!("{helper_value} \"$@\""), helper_value, "get"])
            .status()
            .expect("sh must run the helper snippet");
        assert!(status.success());
        let argv = std::fs::read_to_string(&capture).expect("stub captured argv");
        assert_eq!(argv, "git-credential\nget\n");
    }
}
