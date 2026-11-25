//! Dead local proxy for test binaries and the daemons they spawn.
//!
//! A test that forgets its fake forge server fails at once on this proxy
//! instead of reaching the real GitHub or GitLab, whatever host the code
//! picked (intentd#3743 and intentd#3758 each closed one host). reqwest
//! clients, the GitHub REST/GraphQL transport and `git` children honor these
//! vars; loopback fixtures pass through
//! `NO_PROXY` only on `127.0.0.1`, `localhost` (and `*.localhost`) and `::1`.
//!
//! Not covered: clients built with `.no_proxy()` and libgit2 transports
//! without auto proxy (native checkout and push in `intent-git`), which refuse
//! proxies by design; the GitHub device-flow login client (octocrab's default
//! transport, aimed by `INTENTD_GITHUB_LOGIN_BASE_URI`); children spawned with
//! `env_clear()` that skip [`apply_test_egress`]; and `git` under an
//! `http.proxy` config setting, which wins over these vars.
//!
//! A live opt-in test (e.g. `INTENTD_GH_LIVE_TEST`) runs with
//! [`ALLOW_TEST_EGRESS_ENV`]` = 1`, which turns the guard off.

use std::ffi::OsStr;
use std::process::Command;

const DEAD_PROXY: &str = "http://127.0.0.1:0";
const LOOPBACK: &str = "127.0.0.1,localhost,::1";

/// The proxy env every guarded test process and spawned daemon carries.
pub const TEST_EGRESS_ENV: [(&str, &str); 8] = [
    ("HTTPS_PROXY", DEAD_PROXY),
    ("https_proxy", DEAD_PROXY),
    ("HTTP_PROXY", DEAD_PROXY),
    ("http_proxy", DEAD_PROXY),
    ("ALL_PROXY", DEAD_PROXY),
    ("all_proxy", DEAD_PROXY),
    ("NO_PROXY", LOOPBACK),
    ("no_proxy", LOOPBACK),
];

/// Set to `1` to let a live test reach the real network.
pub const ALLOW_TEST_EGRESS_ENV: &str = "INTENTD_TEST_ALLOW_EGRESS";

/// Whether the developer opted out of the guard for a live run.
#[must_use]
pub fn test_egress_allowed() -> bool {
    std::env::var_os(ALLOW_TEST_EGRESS_ENV).as_deref() == Some(OsStr::new("1"))
}

/// Install the dead proxy in this process. Call it only from a
/// `#[ctor::ctor(unsafe)]` function, before any test thread exists:
///
/// ```ignore
/// #[ctor::ctor(unsafe)]
/// fn block_test_egress() {
///     intentd_test_support::block_test_egress();
/// }
/// ```
pub fn block_test_egress() {
    if test_egress_allowed() {
        return;
    }
    for (key, value) in TEST_EGRESS_ENV {
        std::env::set_var(key, value);
    }
}

/// Put the dead proxy on `cmd`, also after `env_clear()`. A variable the
/// command already sets or removes, in either case, stays the caller's for
/// both spellings, so a test can aim a child at its own proxy.
pub fn apply_test_egress(cmd: &mut Command) {
    if test_egress_allowed() {
        return;
    }
    for pair in TEST_EGRESS_ENV.chunks_exact(2) {
        let [(upper, value), (lower, _)] = [pair[0], pair[1]];
        let explicit = |key: &str| {
            cmd.get_envs()
                .find(|(k, _)| *k == OsStr::new(key))
                .map(|(_, v)| v.map(ToOwned::to_owned))
        };
        let (upper_set, lower_set) = (explicit(upper), explicit(lower));
        let Some(choice) = upper_set.clone().or_else(|| lower_set.clone()) else {
            cmd.env(upper, value).env(lower, value);
            continue;
        };
        for (key, set) in [(upper, upper_set), (lower, lower_set)] {
            match (&choice, set) {
                (_, Some(_)) => {}
                (Some(caller), None) => {
                    cmd.env(key, caller);
                }
                (None, None) => {
                    cmd.env_remove(key);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, PartialEq, Eq)]
    enum Setting<'a> {
        Inherited,
        Removed,
        Set(&'a str),
    }

    fn explicit<'a>(cmd: &'a Command, key: &str) -> Setting<'a> {
        match cmd.get_envs().find(|(k, _)| *k == OsStr::new(key)) {
            None => Setting::Inherited,
            Some((_, None)) => Setting::Removed,
            Some((_, Some(v))) => Setting::Set(v.to_str().unwrap()),
        }
    }

    #[test]
    fn apply_keeps_caller_keys_and_survives_env_clear() {
        let mut cleared = Command::new("never-executed");
        cleared.env_clear().env("HTTPS_PROXY", "http://127.0.0.1:9");
        apply_test_egress(&mut cleared);
        let mut removed = Command::new("never-executed");
        removed.env_remove("no_proxy");
        apply_test_egress(&mut removed);
        if test_egress_allowed() {
            assert_eq!(explicit(&cleared, "HTTP_PROXY"), Setting::Inherited);
            return;
        }
        assert_eq!(
            explicit(&cleared, "HTTPS_PROXY"),
            Setting::Set("http://127.0.0.1:9")
        );
        // curl and git read the lower-case name first, so a caller's choice
        // covers both spellings.
        assert_eq!(
            explicit(&cleared, "https_proxy"),
            Setting::Set("http://127.0.0.1:9")
        );
        assert_eq!(explicit(&cleared, "ALL_PROXY"), Setting::Set(DEAD_PROXY));
        assert_eq!(explicit(&cleared, "NO_PROXY"), Setting::Set(LOOPBACK));
        assert_eq!(explicit(&removed, "no_proxy"), Setting::Removed);
        assert_eq!(explicit(&removed, "NO_PROXY"), Setting::Removed);
        assert_eq!(explicit(&removed, "http_proxy"), Setting::Set(DEAD_PROXY));
    }
}
