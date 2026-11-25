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

/// Put the dead proxy on `cmd`, also after `env_clear()`. A key the command
/// already sets or removes stays the caller's, so a test can aim a child at
/// its own proxy.
pub fn apply_test_egress(cmd: &mut Command) {
    if test_egress_allowed() {
        return;
    }
    for (key, value) in TEST_EGRESS_ENV {
        if !cmd.get_envs().any(|(k, _)| k == OsStr::new(key)) {
            cmd.env(key, value);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn apply_keeps_caller_keys_and_survives_env_clear() {
        let mut cmd = Command::new("never-executed");
        cmd.env_clear()
            .env("HTTPS_PROXY", "http://127.0.0.1:9")
            .env_remove("no_proxy");
        apply_test_egress(&mut cmd);
        let envs: Vec<_> = cmd.get_envs().collect();
        let get = |key: &str| {
            envs.iter()
                .find(|(k, _)| *k == OsStr::new(key))
                .map(|(_, v)| v.map(|v| v.to_str().unwrap()))
        };
        if test_egress_allowed() {
            assert_eq!(get("HTTP_PROXY"), None);
            return;
        }
        assert_eq!(get("HTTPS_PROXY"), Some(Some("http://127.0.0.1:9")));
        assert_eq!(get("no_proxy"), Some(None));
        assert_eq!(get("https_proxy"), Some(Some(DEAD_PROXY)));
        assert_eq!(get("ALL_PROXY"), Some(Some(DEAD_PROXY)));
        assert_eq!(get("NO_PROXY"), Some(Some(LOOPBACK)));
    }
}
