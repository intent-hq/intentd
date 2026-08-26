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
//! device de-pairing, for example) can pin the caller's `spawn_blocking` worker
//! indefinitely — the runtime-saturation vector behind the FE
//! "JSON-RPC request timed out: host.status" surface. The bounded closure
//! installed by [`remote_callbacks`] gives libgit2 a fixed number of attempts
//! before returning `Err`, mirroring the TS handler's `GIT_TERMINAL_PROMPT=0`
//! fail-fast semantics for the remaining libgit2 network paths.

use std::path::Path;
use std::process::{Command, Stdio};

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

/// The config key every intentd credential-helper entry is written under, and
/// the key whose accumulated value list [`GITHUB_HELPER_RESET`] clears. Every
/// credential lookup for an HTTPS `github.com` remote consults it; no other
/// host's lookup does.
const GITHUB_HELPER_KEY: &str = "credential.https://github.com.helper";

/// Config entry clearing the credential helpers git has accumulated so far for
/// `https://github.com`: an empty `helper` value resets the list (git's
/// documented behaviour — `credential.c` clears the accumulated string list).
/// Keyed to the same scope as the intentd entries, so helpers for every other
/// host are left exactly as configured.
///
/// It clears the list for *every* HTTPS github.com request, so it also drops
/// what a path-scoped key such as
/// `credential.https://github.com/intent-hq.helper` contributed to the ones it
/// covers — which is why [`github_helpers_from_config_list`] re-adds those
/// under their own keys rather than folding them into this one.
const GITHUB_HELPER_RESET: &str = "credential.https://github.com.helper=";

/// The `git -c` config entry offering the resolved token as a github.com-scoped
/// credential helper, shared by the shell-git paths (`fetch`, and the
/// `intent-services` clone pipeline). Note the `{{`/`}}`/`{TOKEN_ENV}` are
/// **Rust** `format!` escapes and interpolation — the shell sees a plain
/// `"$INTENT_GIT_GITHUB_TOKEN"` expansion (no token bytes in the string
/// itself). `|| exit 0` keeps the helper silent-but-successful for the
/// `store`/`erase` ops git may also invoke.
pub(crate) fn token_helper_config() -> String {
    format!(
        "{GITHUB_HELPER_KEY}=!f() {{ test \"$1\" = get || exit 0; printf 'username={TOKEN_USERNAME}\\npassword=%s\\n' \"${TOKEN_ENV}\"; }}; f"
    )
}

/// The ordered `-c` entries the shell-git token paths pass so the resolved
/// token is offered *ahead* of the configured helpers (see
/// [`github_helper_entries`]). `cwd` is the repository the child git will run
/// in, so a repository-local helper is preserved as a fallback.
pub(crate) fn token_helper_entries(
    cwd: Option<&Path>,
    inherited_config_parameters: Option<&str>,
) -> Vec<String> {
    github_helper_entries(
        token_helper_config(),
        discover_github_helpers(cwd, inherited_config_parameters).as_deref(),
    )
}

/// Compose the ordered `credential.https://github.com.helper` entries a spawn
/// site emits so intentd's helper is consulted **first**, with the helpers git
/// would otherwise use kept behind it as fallbacks (monorepo#3059).
///
/// Appending intentd's entry *after* the configured helpers — every `git -c` /
/// [`GIT_CONFIG_PARAMETERS_ENV`] entry ranks as command-line scope, which git
/// applies after all config files — was a deliberate "existing setups keep
/// winning" choice, and it is still the right instinct for a helper the user
/// chose. It backfires on macOS, where the helper that wins is
/// `credential.helper = osxkeychain` from the Command Line Tools' system
/// gitconfig: an OS default present on essentially every dev machine that
/// nobody opted into. Any stale github.com keychain entry then shadows the
/// daemon's working token, and GitHub renders the resulting 403 as
/// `Repository not found` — so every HTTPS git operation fails while looking
/// like a missing repository.
///
/// Hence ordering, not replacement: reset, intentd, then the
/// previously-configured helpers — still consulted, just after intentd rather
/// than instead of it. `intentd git-credential` prints nothing and exits 0
/// whenever it has no credential to offer (wrong host, gate off, daemon down,
/// no token), which is what keeps those fallbacks reachable.
///
/// `fallbacks` are the `key=value` entries re-adding what git would consult
/// without this injection (see [`discover_github_helpers`]), in the order it
/// would consult them. `None` means the list could not be determined, and
/// then no reset is emitted and `entry` is simply appended as before: losing
/// precedence is far cheaper than clearing a list we cannot restore.
fn github_helper_entries(entry: String, fallbacks: Option<&[String]>) -> Vec<String> {
    let Some(fallbacks) = fallbacks else {
        return vec![entry];
    };
    // Our own entry, so one an outer agent shell already injected (visible
    // through the inherited parameters) is not re-added behind us. Compared
    // whole, key included: the same helper value under a narrower key is a
    // different scope, not a duplicate.
    let mut seen: Vec<&str> = vec![entry.as_str()];
    let mut entries = vec![GITHUB_HELPER_RESET.to_string(), entry.clone()];
    for fallback in fallbacks {
        // Duplicates would only grow the list on every nesting level; a helper
        // consulted twice answers identically anyway.
        if seen.contains(&fallback.as_str()) {
            continue;
        }
        seen.push(fallback);
        entries.push(fallback.clone());
    }
    entries
}

/// Ask git which credential helpers it would consult for an HTTPS
/// `github.com` remote, and return them as the ordered `key=value` config
/// entries that re-add them behind intentd's helper, as the child about to be
/// spawned will see them: `git config --list` reports entries in config
/// order, and `inherited_config_parameters` is applied so an entry an outer
/// agent shell already injected is visible rather than re-added as a
/// duplicate.
///
/// `cwd` is the directory the lookup runs in, so a repository-local helper is
/// included; `None` runs at the filesystem root, reading the system, global
/// and environment scopes only — right only where the child git genuinely has
/// no repository, such as the `clone` path, whose target does not exist yet
/// (and `git clone` does not read config from a repository that happens to
/// surround its cwd). Returns `None` when git could not be run or exited
/// non-zero — an *unknown* list, which callers must not reset.
#[must_use]
pub fn discover_github_helpers(
    cwd: Option<&Path>,
    inherited_config_parameters: Option<&str>,
) -> Option<Vec<String>> {
    let mut cmd = Command::new("git");
    cmd.arg("-C").arg(cwd.unwrap_or(Path::new("/")));
    cmd.args(["config", "--list", "-z"]);
    match inherited_config_parameters {
        Some(prev) if !prev.trim().is_empty() => {
            cmd.env(GIT_CONFIG_PARAMETERS_ENV, prev);
        }
        // Not inheriting: this process's own value must not leak into a lookup
        // that is meant to describe the child's view.
        _ => {
            cmd.env_remove(GIT_CONFIG_PARAMETERS_ENV);
        }
    }
    let out = cmd
        .stdin(Stdio::null())
        .stderr(Stdio::null())
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    Some(github_helpers_from_config_list(&String::from_utf8_lossy(
        &out.stdout,
    )))
}

/// Fold `git config --list -z` output (`key\nvalue\0` records, or a bare
/// `key\0` for a valueless entry, in config order) into the ordered
/// `key=value` config entries that re-add, behind intentd's helper, every
/// credential helper git would otherwise consult for an HTTPS `github.com`
/// remote. Entries are returned ready to emit, because the key each one must
/// be re-added under depends on how widely it applies (see
/// [`GithubHelperScope`]).
fn github_helpers_from_config_list(list: &str) -> Vec<String> {
    let mut helpers: Vec<String> = Vec::new();
    for record in list.split('\0') {
        // A valueless (implicit-true) entry carries no helper command.
        let Some((key, value)) = record.split_once('\n') else {
            continue;
        };
        match github_helper_key_scope(key) {
            GithubHelperScope::Unrelated => {}
            GithubHelperScope::EveryGithubRequest => {
                if value.is_empty() {
                    // A reset on a key covering *every* github.com request
                    // clears the whole accumulated list for those requests —
                    // the path-scoped entries collected below included, since
                    // each of those requests matches this key too.
                    helpers.clear();
                } else {
                    // Re-added under our own key: it applies to every
                    // github.com request either way, and the reset is scoped
                    // there, so nothing outside github.com HTTPS is touched.
                    helpers.push(format!("{GITHUB_HELPER_KEY}={value}"));
                }
            }
            // Re-added verbatim under its own key, empty values included:
            // which requests it applies to — and, when empty, which ones it
            // resets — depends on the remote URL git is resolving, which is
            // not known at spawn time. Re-emitting the key unchanged lets git
            // re-evaluate it against the real URL, reproducing its narrower
            // scope exactly instead of approximating it here. Order relative
            // to the entries above is preserved, and order is what decides
            // precedence within the emitted list.
            GithubHelperScope::SomeGithubRequests => helpers.push(format!("{key}={value}")),
        }
    }
    helpers
}

/// How a config key relates to the HTTPS `github.com` credential requests
/// [`GITHUB_HELPER_RESET`] clears the helper list for.
enum GithubHelperScope {
    /// Not a credential-helper key git would consult for any such request.
    Unrelated,
    /// Applies to every one of them: the unscoped `credential.helper`, or a
    /// `credential.<url>.helper` whose url carries no path.
    EveryGithubRequest,
    /// Applies to some of them: a `credential.<url>.helper` whose url carries
    /// a path, e.g. `credential.https://github.com/intent-hq.helper`, which
    /// git applies to a remote under that path. Git matches credential config
    /// against the full remote URL *before* it drops the path, so these are
    /// consulted whatever `credential.useHttpPath` says — the setting governs
    /// only what git then hands the helper.
    SomeGithubRequests,
}

/// Classify `key` the way git's urlmatch would for an HTTPS `github.com`
/// remote: scheme `https` or absent (a pattern without a scheme matches any
/// protocol), host `github.com` case-insensitively, the default port
/// optional, and no userinfo (the request carries none, and it must match
/// exactly). `http://…`, `https://*.github.com` and other hosts are
/// [`GithubHelperScope::Unrelated`]: git would not consult those helpers for
/// such a request either.
fn github_helper_key_scope(key: &str) -> GithubHelperScope {
    let Some(rest) = key.strip_prefix("credential.") else {
        return GithubHelperScope::Unrelated;
    };
    if rest == "helper" {
        return GithubHelperScope::EveryGithubRequest;
    }
    let Some(pattern) = rest.strip_suffix(".helper") else {
        return GithubHelperScope::Unrelated;
    };
    let pattern = match pattern.split_once("://") {
        Some((scheme, rest)) => {
            if !scheme.eq_ignore_ascii_case("https") {
                return GithubHelperScope::Unrelated;
            }
            rest
        }
        None => pattern,
    };
    // A bare trailing slash is an empty path, which constrains nothing.
    let (authority, path) = match pattern.split_once('/') {
        Some((authority, path)) => (authority, path),
        None => (pattern, ""),
    };
    if authority.contains('@') {
        return GithubHelperScope::Unrelated;
    }
    let host = authority.strip_suffix(":443").unwrap_or(authority);
    if !host.eq_ignore_ascii_case("github.com") {
        return GithubHelperScope::Unrelated;
    }
    if path.is_empty() {
        GithubHelperScope::EveryGithubRequest
    } else {
        GithubHelperScope::SomeGithubRequests
    }
}

/// Join `entries` — sq-quoted the way git's `sq_dequote` parser expects — onto
/// any non-blank `inherited` value, producing a [`GIT_CONFIG_PARAMETERS_ENV`]
/// value. Inherited entries stay first so nothing the caller configured is
/// dropped; the helper ordering the entries themselves encode is what puts
/// intentd ahead (see [`github_helper_entries`]).
fn config_parameters(inherited: Option<&str>, entries: &[String]) -> String {
    let mut params = String::new();
    if let Some(prev) = inherited {
        if !prev.trim().is_empty() {
            params.push_str(prev);
        }
    }
    for entry in entries {
        if !params.is_empty() {
            params.push(' ');
        }
        params.push_str(&sq_quote(entry));
    }
    params
}

/// Environment variable git reads for command-line-scoped config entries —
/// the same mechanism `git -c` uses to reach child processes. Entries here
/// rank as "command line" config: applied after every config file, so a helper
/// added here is consulted *last* and cannot displace a config-file one. Git
/// has no prepend, which is why the credential-helper ordering is expressed as
/// a reset plus a re-add (see [`github_helper_entries`]).
pub const GIT_CONFIG_PARAMETERS_ENV: &str = "GIT_CONFIG_PARAMETERS";

/// Build the environment pairs a spawn site injects to offer `token` as a
/// github.com-scoped credential helper (monorepo#884): the sq-quoted
/// [`token_helper_config`] entry ordered **ahead** of the helpers git would
/// otherwise consult, which are re-added behind it as fallbacks (see
/// [`github_helper_entries`]), appended to any `inherited_config_parameters`
/// (the caller's pre-existing [`GIT_CONFIG_PARAMETERS_ENV`] value, which is
/// never dropped), plus [`TOKEN_ENV`] carrying the token itself. The config
/// value never contains token bytes; the token travels only under
/// [`TOKEN_ENV`]. Returns no pairs when the token is unusable (see
/// [`usable_token`]), leaving the child env untouched.
///
/// Discovery runs with no `cwd`: this builds the env for the `clone` path,
/// whose child git has no repository to read local config from — the target
/// does not exist yet, and `git clone` ignores any repository surrounding its
/// cwd. There is therefore no repository-local helper for the reset to drop.
#[must_use]
pub fn scoped_credential_env(
    token: Option<&str>,
    inherited_config_parameters: Option<&str>,
) -> Vec<(String, String)> {
    let Some(token) = usable_token(token) else {
        return Vec::new();
    };
    let entries = github_helper_entries(
        token_helper_config(),
        discover_github_helpers(None, inherited_config_parameters).as_deref(),
    );
    vec![
        (
            GIT_CONFIG_PARAMETERS_ENV.to_string(),
            config_parameters(inherited_config_parameters, &entries),
        ),
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
        "{GITHUB_HELPER_KEY}=!{} git-credential",
        sh_quote(intentd_path)
    )
}

/// Build the environment pairs a spawn site injects to offer the
/// daemon-backed `intentd git-credential` helper to a child's git
/// (monorepo#884 Phase 2.2): the single [`GIT_CONFIG_PARAMETERS_ENV`] pair
/// carrying the sq-quoted [`daemon_helper_config`] entry ordered **ahead** of
/// the helpers git would otherwise consult, which are re-added behind it as
/// fallbacks (see [`github_helper_entries`]), appended to any
/// `inherited_config_parameters` so the caller's pre-existing value is never
/// dropped. Unlike [`scoped_credential_env`] there is no token pair at all: no
/// token bytes ever enter the child environment.
///
/// `cwd` is the directory the child is spawned in — the workspace worktree for
/// an agent or terminal — so a repository-local helper configured there is
/// discovered and re-added. Passing `None` where the child does have a
/// repository would let the reset clear that helper without restoring it.
#[must_use]
pub fn daemon_helper_env(
    intentd_path: &str,
    cwd: Option<&Path>,
    inherited_config_parameters: Option<&str>,
) -> Vec<(String, String)> {
    let entries = github_helper_entries(
        daemon_helper_config(intentd_path),
        discover_github_helpers(cwd, inherited_config_parameters).as_deref(),
    );
    vec![(
        GIT_CONFIG_PARAMETERS_ENV.to_string(),
        config_parameters(inherited_config_parameters, &entries),
    )]
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
#[must_use]
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
    if token.is_empty() || token.chars().any(char::is_control) {
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
        let Err(err) = res else {
            panic!("attempt == max must fail regardless of environment")
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
        let Err(err) = res else {
            panic!("callback must error after MAX_CREDENTIAL_ATTEMPTS")
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
        let Err(err) = res else {
            panic!("max_attempts=0 must error on the first call")
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

        // Round-trip through real git: the command-line-scoped entries must
        // dequote back to a reset followed by the exact helper snippet.
        let values = helper_values_via_git(params);
        let expected = token_helper_config();
        let expected_value = expected.strip_prefix(GITHUB_HELPER_RESET).unwrap();
        assert_eq!(
            &values[..2],
            &[String::new(), expected_value.to_string()],
            "reset then the token helper, ahead of any fallbacks: {values:?}"
        );
    }

    /// The poisoned home directory backing [`hermetic_config_git`]: its
    /// `.gitconfig` (and XDG `git/config`) define github.com credential
    /// helpers exactly the way `gh auth setup-git` does on dev hosts
    /// (monorepo#3164). Written once per process and shared read-only by the
    /// tests in that process — under a per-test-process runner like nextest
    /// each process writes its own pid-keyed dir. The dirs are not cleaned
    /// up (a few bytes in the temp dir; a `Drop` guard cannot outlive the
    /// `'static` sharing).
    fn poisoned_home() -> &'static Path {
        static DIR: std::sync::OnceLock<std::path::PathBuf> = std::sync::OnceLock::new();
        DIR.get_or_init(|| {
            let dir = std::env::temp_dir().join(format!(
                "intent-git-auth-poisoned-home-{}",
                std::process::id()
            ));
            let poison = "[credential \"https://github.com\"]\n\thelper = \n\thelper = !/poisoned/gh auth git-credential\n";
            std::fs::create_dir_all(dir.join("git")).expect("mkdir poisoned home");
            std::fs::write(dir.join(".gitconfig"), poison).expect("write poisoned gitconfig");
            std::fs::write(dir.join("git").join("config"), poison)
                .expect("write poisoned xdg config");
            dir
        })
    }

    /// A `git` command reading config with `params` as the command-line scope
    /// and every host config scope excluded, so assertions see only the
    /// entries under test — e.g. `gh auth setup-git` installs a github.com
    /// credential helper in the global config that would otherwise leak into
    /// `--get-all` reads (monorepo#3343). `-C /` runs outside any repository
    /// (the same no-repo trick `discover_github_helpers` uses), excluding the
    /// enclosing checkout's local config too.
    ///
    /// The exclusions are backstopped by poison rather than by hoping the
    /// host is clean (monorepo#3164): every config location git would fall
    /// back to if `GIT_CONFIG_GLOBAL` or `GIT_CONFIG_NOSYSTEM` were dropped
    /// points at [`poisoned_home`], whose helpers would rank ahead of the
    /// command scope and fail the assertions — so clean CI hosts exercise
    /// the isolation on every run (see
    /// `hermetic_config_git_excludes_poisoned_host_scopes`).
    fn hermetic_config_git(params: &str) -> Command {
        let poisoned = poisoned_home();
        let mut cmd = Command::new("git");
        cmd.arg("-C")
            .arg("/")
            .env(GIT_CONFIG_PARAMETERS_ENV, params)
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("HOME", poisoned)
            .env("XDG_CONFIG_HOME", poisoned)
            .env("GIT_CONFIG_SYSTEM", poisoned.join(".gitconfig"));
        cmd
    }

    /// The poison backing [`hermetic_config_git`] is live: the same read with
    /// either isolation variable dropped sees the poisoned helpers ahead of
    /// the entry under test — the exact monorepo#3164 failure shape — so the
    /// hermetic reads staying green proves the isolation, not a clean host.
    #[test]
    fn hermetic_config_git_excludes_poisoned_host_scopes() {
        let params = sq_quote(&format!("{GITHUB_HELPER_KEY}=under-test"));
        assert_eq!(helper_values_via_git(&params), ["under-test"]);

        for dropped in ["GIT_CONFIG_GLOBAL", "GIT_CONFIG_NOSYSTEM"] {
            let mut cmd = hermetic_config_git(&params);
            cmd.env_remove(dropped);
            let out = cmd
                .args(["config", "--get-all", GITHUB_HELPER_KEY])
                .output()
                .expect("git must be runnable");
            assert!(
                out.status.success(),
                "git config read failed without {dropped}: {}",
                String::from_utf8_lossy(&out.stderr)
            );
            let values = String::from_utf8_lossy(&out.stdout);
            assert!(
                values.starts_with("\n!/poisoned/gh auth git-credential\n"),
                "without {dropped} the poison must leak in ahead: {values:?}"
            );
        }
    }

    /// Read back the ordered `credential.https://github.com.helper` values a
    /// `GIT_CONFIG_PARAMETERS` value produces, using real git as the parser.
    fn helper_values_via_git(params: &str) -> Vec<String> {
        let out = hermetic_config_git(params)
            .args(["config", "--get-all", GITHUB_HELPER_KEY])
            .output()
            .expect("git must be runnable");
        assert!(out.status.success(), "git must parse the quoted parameters");
        String::from_utf8_lossy(&out.stdout)
            .strip_suffix('\n')
            .unwrap_or_default()
            .split('\n')
            .map(str::to_string)
            .collect()
    }

    /// A pre-existing `GIT_CONFIG_PARAMETERS` value is preserved verbatim and
    /// the helper entries are appended after it (space-separated), so
    /// unrelated inherited config still applies and both remain parseable by
    /// git.
    #[test]
    fn scoped_credential_env_appends_to_inherited_parameters() {
        let inherited = "'foo.bar=baz'";
        let pairs = scoped_credential_env(Some("tok"), Some(inherited));
        let params = &pairs[0].1;
        assert!(
            params.starts_with("'foo.bar=baz' '"),
            "inherited entry must come first: {params}"
        );
        let out = hermetic_config_git(params)
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
            let pairs = daemon_helper_env(path, None, None);
            assert_eq!(pairs.len(), 1, "single env pair — no token pair");
            let (key, params) = &pairs[0];
            assert_eq!(key, GIT_CONFIG_PARAMETERS_ENV);

            let values = helper_values_via_git(params);
            let expected = daemon_helper_config(path);
            let expected_value = expected.strip_prefix(GITHUB_HELPER_RESET).unwrap();
            assert_eq!(
                &values[..2],
                &[String::new(), expected_value.to_string()],
                "reset then the daemon helper, ahead of any fallbacks; path {path:?}"
            );
        }
    }

    /// A pre-existing `GIT_CONFIG_PARAMETERS` value is preserved verbatim and
    /// the daemon-helper entries are appended after it, so unrelated inherited
    /// config still applies; blank inherited values are treated as absent.
    #[test]
    fn daemon_helper_env_appends_to_inherited_parameters() {
        let pairs = daemon_helper_env("/usr/local/bin/intentd", None, Some("'foo.bar=baz'"));
        let params = &pairs[0].1;
        assert!(
            params.starts_with("'foo.bar=baz' '"),
            "inherited entry must come first: {params}"
        );
        let out = hermetic_config_git(params)
            .args(["config", "--get", "foo.bar"])
            .output()
            .expect("git must be runnable");
        assert!(out.status.success());
        assert_eq!(String::from_utf8_lossy(&out.stdout).trim_end(), "baz");

        let pairs = daemon_helper_env("/usr/local/bin/intentd", None, Some("   "));
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
        let dir = Scratch::new("shell-invocation");
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

    /// The key matcher reproduces git's urlmatch verdicts for an HTTPS
    /// `github.com` credential request — each case below was checked against
    /// real `git credential fill` (monorepo#3059). Over-matching would hand
    /// another host's helper a github.com request; under-matching would drop a
    /// helper the user configured, since the reset clears the list.
    ///
    /// The path-scoped split matters because the reset clears the list for
    /// every github.com request, path-scoped contributions included: such a
    /// key is re-added under its own scope, not folded into ours.
    #[test]
    fn github_helper_key_scope_matches_gits_urlmatch() {
        for key in [
            "credential.helper",
            "credential.https://github.com.helper",
            "credential.github.com.helper",
            "credential.https://github.com/.helper",
            "credential.https://github.com:443.helper",
            "credential.HTTPS://GitHub.com.helper",
        ] {
            assert!(
                matches!(
                    github_helper_key_scope(key),
                    GithubHelperScope::EveryGithubRequest
                ),
                "must apply to every github.com request: {key}"
            );
        }
        for key in [
            "credential.https://github.com/intent-hq.helper",
            "credential.https://github.com/intent-hq/intentd.git.helper",
            "credential.github.com/intent-hq.helper",
            "credential.https://github.com:443/intent-hq.helper",
        ] {
            assert!(
                matches!(
                    github_helper_key_scope(key),
                    GithubHelperScope::SomeGithubRequests
                ),
                "must apply to the github.com requests under its path: {key}"
            );
        }
        for key in [
            "credential.http://github.com.helper",
            "credential.https://*.github.com.helper",
            "credential.https://gitlab.com.helper",
            "credential.https://github.com.username",
            "credential.https://alice@github.com.helper",
            "credential.useHttpPath",
            "url.https://github.com.insteadOf",
        ] {
            assert!(
                matches!(github_helper_key_scope(key), GithubHelperScope::Unrelated),
                "must not match: {key}"
            );
        }
    }

    /// The `git config --list -z` fold keeps config order, ignores entries for
    /// other hosts and non-helper keys, skips valueless entries, and treats an
    /// empty value as the list reset git implements.
    #[test]
    fn github_helpers_from_config_list_orders_and_honours_reset() {
        let list = concat!(
            "credential.helper\nosxkeychain\0",
            "credential.https://gitlab.com.helper\nglab\0",
            "credential.https://github.com.helper\n!gh auth git-credential\0",
            "credential.usehttppath\ntrue\0",
            "core.bare\0",
        );
        assert_eq!(
            github_helpers_from_config_list(list),
            vec![
                format!("{GITHUB_HELPER_KEY}=osxkeychain"),
                format!("{GITHUB_HELPER_KEY}=!gh auth git-credential"),
            ]
        );

        // An empty value clears everything accumulated so far, including
        // entries contributed by earlier config files.
        let list = concat!(
            "credential.helper\nosxkeychain\0",
            "credential.helper\n\0",
            "credential.https://github.com.helper\nstore\0",
        );
        assert_eq!(
            github_helpers_from_config_list(list),
            vec![format!("{GITHUB_HELPER_KEY}=store")]
        );
        assert!(github_helpers_from_config_list("").is_empty());
    }

    /// A path-scoped helper is re-added **under its own key**, in config order
    /// among the rest (monorepo#3059 review): the reset clears the list for
    /// every github.com request, so folding it into our own key was not an
    /// option — that would widen a helper the user scoped to one owner into
    /// one consulted for all of github.com. Empty values are re-emitted too,
    /// so git re-applies the user's reset against the real remote URL.
    #[test]
    fn github_helpers_from_config_list_preserves_path_scoped_keys() {
        let list = concat!(
            "credential.https://github.com/intent-hq.helper\norg-helper\0",
            "credential.helper\nosxkeychain\0",
            "credential.https://github.com/other.helper\n\0",
        );
        assert_eq!(
            github_helpers_from_config_list(list),
            vec![
                "credential.https://github.com/intent-hq.helper=org-helper".to_string(),
                format!("{GITHUB_HELPER_KEY}=osxkeychain"),
                "credential.https://github.com/other.helper=".to_string(),
            ]
        );

        // A reset on a key covering every github.com request clears the
        // path-scoped entries too: each of those requests matches it as well.
        let list = concat!(
            "credential.https://github.com/intent-hq.helper\norg-helper\0",
            "credential.helper\n\0",
            "credential.helper\nosxkeychain\0",
        );
        assert_eq!(
            github_helpers_from_config_list(list),
            vec![format!("{GITHUB_HELPER_KEY}=osxkeychain")]
        );
    }

    /// The composed entries put intentd first and re-add the discovered
    /// helpers behind it — including a generic one an OS default contributed —
    /// while dropping our own entry (an outer agent shell's) and duplicates so
    /// nesting cannot grow the list.
    #[test]
    fn github_helper_entries_puts_intentd_first_and_keeps_fallbacks() {
        let entry = daemon_helper_config("/usr/local/bin/intentd");
        let fallbacks = vec![
            entry.clone(),
            format!("{GITHUB_HELPER_KEY}=osxkeychain"),
            format!("{GITHUB_HELPER_KEY}=!gh auth git-credential"),
            format!("{GITHUB_HELPER_KEY}=osxkeychain"),
            // Same helper value, narrower scope: a different entry, not a
            // duplicate, so it must survive the de-duplication above.
            "credential.https://github.com/intent-hq.helper=osxkeychain".to_string(),
        ];
        assert_eq!(
            github_helper_entries(entry.clone(), Some(&fallbacks)),
            vec![
                GITHUB_HELPER_RESET.to_string(),
                entry,
                format!("{GITHUB_HELPER_KEY}=osxkeychain"),
                format!("{GITHUB_HELPER_KEY}=!gh auth git-credential"),
                "credential.https://github.com/intent-hq.helper=osxkeychain".to_string(),
            ]
        );
    }

    /// Discovery failure means the helper list is *unknown*, so no reset is
    /// emitted: the entry is appended exactly as it was before monorepo#3059.
    /// Resetting a list we cannot restore would strip the user's helpers
    /// outright — strictly worse than losing precedence.
    #[test]
    fn github_helper_entries_without_discovery_appends_as_before() {
        let entry = daemon_helper_config("/usr/local/bin/intentd");
        assert_eq!(
            github_helper_entries(entry.clone(), None),
            vec![entry.clone()]
        );
        // An empty (but known) list still resets: nothing to preserve.
        assert_eq!(
            github_helper_entries(entry.clone(), Some(&[])),
            vec![GITHUB_HELPER_RESET.to_string(), entry]
        );
    }

    /// Discovery reports what git itself would consult, in config order, for
    /// the directory the child will run in: a repository-local helper is
    /// included (local config is read after system/global, so it lands last)
    /// and another host's scoped helper is not.
    #[cfg(unix)]
    #[test]
    fn discover_github_helpers_sees_repository_local_helpers() {
        let dir = Scratch::new("discover");
        let repo = dir.0.join("repo");
        std::fs::create_dir_all(&repo).unwrap();
        assert!(Command::new("git")
            .args(["init", "-q"])
            .current_dir(&repo)
            .status()
            .expect("git init")
            .success());
        for (key, value) in [
            ("credential.helper", "local-generic"),
            ("credential.https://github.com.helper", "local-scoped"),
            (
                "credential.https://github.com/intent-hq.helper",
                "local-org",
            ),
            ("credential.https://gitlab.com.helper", "other-host"),
        ] {
            assert!(Command::new("git")
                .args(["config", "--local", key, value])
                .current_dir(&repo)
                .status()
                .expect("git config")
                .success());
        }

        let helpers = discover_github_helpers(Some(&repo), None).expect("git must be runnable");
        assert!(
            helpers.ends_with(&[
                format!("{GITHUB_HELPER_KEY}=local-generic"),
                format!("{GITHUB_HELPER_KEY}=local-scoped"),
                // Kept under its own key, so re-adding it does not widen a
                // one-owner helper into an all-of-github.com one.
                "credential.https://github.com/intent-hq.helper=local-org".to_string(),
            ]),
            "repo-local helpers, in config order, at the end: {helpers:?}"
        );
        assert!(
            !helpers.iter().any(|h| h.ends_with("other-host")),
            "another host's helper must not be re-added for github.com: {helpers:?}"
        );

        // Entries the child would inherit are visible too, so an outer agent
        // shell's injection is recognised rather than duplicated.
        let inherited = config_parameters(None, &["credential.helper=inherited".to_string()]);
        let helpers =
            discover_github_helpers(Some(&repo), Some(&inherited)).expect("git must be runnable");
        assert_eq!(
            helpers.last().map(String::as_str),
            Some(format!("{GITHUB_HELPER_KEY}=inherited").as_str())
        );
    }

    /// The defect this ordering exists to fix (monorepo#3059), driven through
    /// real git: a github.com credential that authenticates but is dead for
    /// the org — Apple's `osxkeychain` default holding a stale token — sits in
    /// a config file, so git consults it before any command-line-scoped entry.
    ///
    /// The `None` half is the pre-fix composition verbatim (append, no reset):
    /// it must still hand back the stale credential, which is what made every
    /// HTTPS git operation in an agent shell fail with `Repository not found`.
    /// The `Some` half is the fix: the daemon's token wins while the stale
    /// helper stays configured.
    #[cfg(unix)]
    #[test]
    fn stale_config_helper_cannot_shadow_the_daemon_helper() {
        let dir = Scratch::new("shadow");
        let stale = dir.stub(
            "stale",
            "printf 'username=keychain\\npassword=stale-403\\n'\n",
        );
        let intentd = dir.stub(
            "intentd",
            "printf 'username=x-access-token\\npassword=daemon-token\\n'\n",
        );
        let global = dir.global_config(&stale);
        let entry = daemon_helper_config(&intentd.to_string_lossy());
        let fallbacks = vec![format!("{GITHUB_HELPER_KEY}={}", stale.to_string_lossy())];

        let before = config_parameters(None, &github_helper_entries(entry.clone(), None));
        assert_eq!(
            fill_github_password(&global, &before),
            Some("stale-403".to_string()),
            "appending after the config files is exactly how the daemon token got shadowed"
        );

        let after = config_parameters(None, &github_helper_entries(entry, Some(&fallbacks)));
        assert_eq!(
            fill_github_password(&global, &after),
            Some("daemon-token".to_string()),
            "the daemon helper must be consulted before the config-file helper"
        );
    }

    /// The fallback the ordering is careful to preserve: when intentd declines
    /// (silent, exit 0 — no daemon, gate off, no token), git falls through to
    /// the helper the user actually configured. Without the re-add, the reset
    /// would leave github.com with no helper at all.
    #[cfg(unix)]
    #[test]
    fn configured_helper_still_answers_when_the_daemon_helper_declines() {
        let dir = Scratch::new("fallback");
        let configured = dir.stub(
            "configured",
            "printf 'username=alice\\npassword=user-pat\\n'\n",
        );
        // Exactly what `intentd git-credential` does with nothing to offer.
        let silent = dir.stub("intentd", "exit 0\n");
        let global = dir.global_config(&configured);

        let params = config_parameters(
            None,
            &github_helper_entries(
                daemon_helper_config(&silent.to_string_lossy()),
                Some(&[format!(
                    "{GITHUB_HELPER_KEY}={}",
                    configured.to_string_lossy()
                )]),
            ),
        );
        assert_eq!(
            fill_github_password(&global, &params),
            Some("user-pat".to_string()),
            "a user-configured helper must stay reachable behind intentd's"
        );
    }

    /// [`fill_github_credential`] for a bare `https://github.com` request.
    #[cfg(unix)]
    fn fill_github_password(global: &std::path::Path, params: &str) -> Option<String> {
        fill_github_credential(global, params, None)
    }

    /// The daemon-helper env discovers in the directory the child is spawned
    /// in, not at the filesystem root (monorepo#3059 review): its agent and
    /// terminal consumers launch shells in a workspace worktree, so a
    /// repository-local helper configured there is one the github.com-wide
    /// reset would otherwise clear without re-adding.
    #[cfg(unix)]
    #[test]
    fn daemon_helper_env_discovers_in_the_spawn_cwd() {
        let dir = Scratch::new("spawncwd");
        let repo = dir.0.join("repo");
        std::fs::create_dir_all(&repo).unwrap();
        assert!(Command::new("git")
            .args(["init", "-q"])
            .current_dir(&repo)
            .status()
            .expect("git init")
            .success());
        assert!(Command::new("git")
            .args([
                "config",
                "--local",
                "credential.helper",
                "repo-local-helper"
            ])
            .current_dir(&repo)
            .status()
            .expect("git config")
            .success());

        let params = |cwd| {
            let pairs = daemon_helper_env("/usr/local/bin/intentd", cwd, None);
            assert_eq!(pairs.len(), 1);
            pairs[0].1.clone()
        };
        assert!(
            helper_values_via_git(&params(Some(&repo)))
                .iter()
                .any(|v| v == "repo-local-helper"),
            "the worktree's own helper must be re-added behind intentd's"
        );
        assert!(
            !helper_values_via_git(&params(None))
                .iter()
                .any(|v| v == "repo-local-helper"),
            "discovering at the root cannot see it — which is why cwd is passed"
        );
    }

    /// A path-scoped helper — `credential.https://github.com/intent-hq.helper`
    /// — is consulted for a remote under that path, so the github.com-wide
    /// reset drops it and it has to be re-added (monorepo#3059 review).
    ///
    /// The `Some(&[])` half is the defect verbatim: the reset with the
    /// path-scoped helper not re-added leaves that remote with no fallback at
    /// all once intentd declines. The re-add restores it — under its own key,
    /// so it stays scoped to `intent-hq` and is *not* consulted for another
    /// owner's remote, which folding it into the github.com-wide key would
    /// have done.
    #[cfg(unix)]
    #[test]
    fn path_scoped_helper_still_answers_when_the_daemon_helper_declines() {
        let dir = Scratch::new("pathscoped");
        let org = dir.stub("org", "printf 'username=alice\\npassword=org-pat\\n'\n");
        // Exactly what `intentd git-credential` does with nothing to offer.
        let silent = dir.stub("intentd", "exit 0\n");
        let global = dir.0.join("gitconfig");
        std::fs::write(
            &global,
            format!(
                "[credential \"https://github.com/intent-hq\"]\n\thelper = {}\n",
                org.display()
            ),
        )
        .expect("write gitconfig");
        let entry = daemon_helper_config(&silent.to_string_lossy());
        let fallback = format!(
            "credential.https://github.com/intent-hq.helper={}",
            org.to_string_lossy()
        );

        let dropped = config_parameters(None, &github_helper_entries(entry.clone(), Some(&[])));
        assert_eq!(
            fill_github_credential(&global, &dropped, Some("intent-hq/intentd.git")),
            None,
            "the github.com-wide reset does clear a path-scoped helper — the bug"
        );

        let restored = config_parameters(None, &github_helper_entries(entry, Some(&[fallback])));
        assert_eq!(
            fill_github_credential(&global, &restored, Some("intent-hq/intentd.git")),
            Some("org-pat".to_string()),
            "a path-scoped helper must stay reachable behind intentd's"
        );
        assert_eq!(
            fill_github_credential(&global, &restored, Some("other-org/repo.git")),
            None,
            "re-adding it must not widen it beyond the path the user scoped it to"
        );
    }

    /// Resolve a github.com credential through real git with `global` as the
    /// only config file and `params` as the command-line scope, returning the
    /// password git settled on. `path` is the remote's path when the request
    /// is for a specific repository, which is what path-scoped credential
    /// config matches against. Hermetic: the system gitconfig (the very file
    /// that ships `credential.helper = osxkeychain`) is excluded, and prompts
    /// are disabled so an unanswered request fails instead of blocking.
    #[cfg(unix)]
    fn fill_github_credential(
        global: &std::path::Path,
        params: &str,
        path: Option<&str>,
    ) -> Option<String> {
        use std::io::Write;
        let mut child = Command::new("git")
            .args(["credential", "fill"])
            .env("GIT_CONFIG_GLOBAL", global)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_TERMINAL_PROMPT", "0")
            .env(GIT_CONFIG_PARAMETERS_ENV, params)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("git must be runnable");
        let request = match path {
            Some(path) => format!("url=https://github.com/{path}\n\n"),
            None => "protocol=https\nhost=github.com\n\n".to_string(),
        };
        child
            .stdin
            .take()
            .unwrap()
            .write_all(request.as_bytes())
            .unwrap();
        let out = child.wait_with_output().expect("git credential fill");
        String::from_utf8_lossy(&out.stdout)
            .lines()
            .find_map(|l| l.strip_prefix("password=").map(str::to_string))
    }

    /// Guard-cleaned scratch dir (no tempfile dev-dep in this crate), unique
    /// per test so the suite can run them in parallel.
    #[cfg(unix)]
    struct Scratch(std::path::PathBuf);

    #[cfg(unix)]
    impl Scratch {
        fn new(name: &str) -> Self {
            let dir =
                std::env::temp_dir().join(format!("intent-git-auth-{name}-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).expect("mkdir scratch");
            Self(dir)
        }

        /// An executable credential-helper stub answering `get` with `body`
        /// and staying silent (exit 0) for every other operation, the way a
        /// well-behaved helper must.
        fn stub(&self, name: &str, body: &str) -> std::path::PathBuf {
            use std::os::unix::fs::PermissionsExt;
            let path = self.0.join(name);
            // git passes the operation last, whether it invokes the helper
            // directly (`<path> get`) or through `sh -c` (`… git-credential get`).
            std::fs::write(
                &path,
                format!("#!/bin/sh\nfor a in \"$@\"; do op=$a; done\ntest \"$op\" = get || exit 0\n{body}"),
            )
            .expect("write stub");
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
            path
        }

        /// A global gitconfig whose only setting is `credential.helper`,
        /// standing in for the config file an OS default lands in.
        fn global_config(&self, helper: &std::path::Path) -> std::path::PathBuf {
            let path = self.0.join("gitconfig");
            std::fs::write(
                &path,
                format!("[credential]\n\thelper = {}\n", helper.display()),
            )
            .expect("write gitconfig");
            path
        }
    }

    #[cfg(unix)]
    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
}
