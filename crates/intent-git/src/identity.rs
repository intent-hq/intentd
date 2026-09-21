//! Commit-identity environment for agent-spawned processes
//! (intent-hq/intent#4142).
//!
//! Resolves the same committer identity `git.commit` uses — git2's
//! [`git2::Repository::signature`], i.e. the worktree → local → global →
//! system/XDG `user.name`/`user.email` config chain — and exposes it as the
//! four `GIT_AUTHOR_*` / `GIT_COMMITTER_*` variables, so a plain `git commit`
//! run inside an agent-spawned shell/terminal/script/exec commits as the
//! user's real identity instead of a hostname-derived fallback, even when the
//! repo/worktree carries no local `user.*` config. When no identity resolves,
//! no variables are exported (the spawn keeps its current environment;
//! empty/placeholder values are never emitted).
//!
//! Because env vars outrank *all* config files in git's own resolution, two
//! consequences are deliberate (#4142 explicitly wants env to outrank config
//! to close the fallback hole in /tmp clones and fresh worktrees):
//!
//! - **Spawn-time snapshot** — a long-lived PTY freezes the identity at
//!   create time; changing `user.*` config later does not affect terminals
//!   already running.
//! - **Cross-repo override** — `cd` inside a spawned shell into another repo
//!   whose local `user.*` differs still commits with the spawn-cwd identity
//!   (the env shadows that repo's local config).

use std::path::Path;

/// The four environment variables git reads for author/committer identity.
pub const GIT_IDENTITY_ENV_VARS: [&str; 4] = [
    "GIT_AUTHOR_NAME",
    "GIT_AUTHOR_EMAIL",
    "GIT_COMMITTER_NAME",
    "GIT_COMMITTER_EMAIL",
];

/// Resolve the commit identity for a process spawned in `cwd` and return it
/// as the four `GIT_*` env pairs. `cwd` may be anywhere inside the repository
/// (the repo is discovered upward, matching git's own resolution from a
/// subdirectory). Returns an empty vec — export nothing — when `cwd` is
/// absent, not inside a repository, or the repository resolves no identity.
/// A variable already set in the daemon's own process environment is never
/// emitted: the child inherits it untouched, so a user-specified process
/// identity keeps git's env-over-config precedence. Never fails or blocks a
/// spawn.
#[must_use]
pub fn commit_identity_env(cwd: Option<&Path>) -> Vec<(String, String)> {
    commit_identity_env_with(cwd, |key| std::env::var_os(key).is_some())
}

/// Seam behind [`commit_identity_env`]: `inherited` reports whether the
/// spawning process's own environment already carries `key` (the child
/// inherits such a variable, so it is skipped here rather than overridden —
/// gap-filling only, per key).
pub fn commit_identity_env_with(
    cwd: Option<&Path>,
    inherited: impl Fn(&str) -> bool,
) -> Vec<(String, String)> {
    let Some(cwd) = cwd else {
        return Vec::new();
    };
    let Ok(repo) = git2::Repository::discover(cwd) else {
        return Vec::new();
    };
    let Ok(sig) = repo.signature() else {
        return Vec::new();
    };
    identity_pairs(sig.name().ok(), sig.email().ok())
        .into_iter()
        .filter(|(key, _)| !inherited(key))
        .collect()
}

/// Pure pairing seam: both a non-empty name AND email are required (git
/// itself rejects a commit with only one half, and exporting a partial
/// identity would mask the other half's absence).
fn identity_pairs(name: Option<&str>, email: Option<&str>) -> Vec<(String, String)> {
    let (Some(name), Some(email)) = (name, email) else {
        return Vec::new();
    };
    if name.is_empty() || email.is_empty() {
        return Vec::new();
    }
    GIT_IDENTITY_ENV_VARS
        .iter()
        .map(|&key| {
            let value = if key.ends_with("EMAIL") { email } else { name };
            (key.to_string(), value.to_string())
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::init_repo;

    /// Hermetic stand-in for [`commit_identity_env`]: the test harness itself
    /// may inherit the four `GIT_*` identity vars (agent-spawned shells do,
    /// post-#4142), and the real `std::env::var_os` probe would then correctly
    /// gap-fill nothing. Tests exercising resolution — not inheritance — go
    /// through the seam with a fixed "nothing inherited" closure
    /// (intent-hq/monorepo#4191).
    fn resolved_identity_env(cwd: Option<&Path>) -> Vec<(String, String)> {
        commit_identity_env_with(cwd, |_| false)
    }

    #[test]
    fn resolves_four_pairs_from_repo_root_and_subdir() {
        let repo = init_repo("identity");
        let expected: Vec<(String, String)> = GIT_IDENTITY_ENV_VARS
            .iter()
            .map(|&key| {
                let value = if key.ends_with("EMAIL") {
                    "test@example.com"
                } else {
                    "Test"
                };
                (key.to_string(), value.to_string())
            })
            .collect();
        assert_eq!(resolved_identity_env(Some(repo.path())), expected);

        let sub = repo.path().join("nested/dir");
        std::fs::create_dir_all(&sub).unwrap();
        assert_eq!(resolved_identity_env(Some(&sub)), expected);
    }

    #[test]
    fn no_cwd_or_non_repo_cwd_exports_nothing() {
        assert!(resolved_identity_env(None).is_empty());
        let dir =
            std::env::temp_dir().join(format!("intent-identity-norepo-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        // GIT_CEILING is irrelevant: temp dirs are never inside a repo, but
        // guard against a repo above temp on exotic hosts by asserting shape
        // only when empty.
        let pairs = resolved_identity_env(Some(&dir));
        if !pairs.is_empty() {
            assert_eq!(pairs.len(), 4, "either nothing or the full identity");
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn partial_or_empty_identity_exports_nothing() {
        assert!(identity_pairs(None, Some("a@b.c")).is_empty());
        assert!(identity_pairs(Some("Name"), None).is_empty());
        assert!(identity_pairs(Some(""), Some("a@b.c")).is_empty());
        assert!(identity_pairs(Some("Name"), Some("")).is_empty());
    }

    /// A `GIT_*` identity var already set in the daemon's own environment is
    /// never emitted (per key): the child inherits it, preserving git's
    /// env-over-config precedence for a user-specified process identity.
    #[test]
    fn daemon_env_identity_vars_are_never_overridden() {
        let repo = init_repo("identity-inherited");
        let pairs = commit_identity_env_with(Some(repo.path()), |key| key == "GIT_AUTHOR_NAME");
        let keys: Vec<&str> = pairs.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(
            keys,
            vec![
                "GIT_AUTHOR_EMAIL",
                "GIT_COMMITTER_NAME",
                "GIT_COMMITTER_EMAIL"
            ],
            "the inherited key is skipped, the rest still fill gaps"
        );

        let all = commit_identity_env_with(Some(repo.path()), |_| true);
        assert!(
            all.is_empty(),
            "fully user-specified identity: emit nothing"
        );
    }

    /// Functional round-trip (#4142 definition of done): a `git commit` made
    /// in a repository with NO local user config, using the env resolved from
    /// the workspace repo, produces the resolved identity — not a
    /// hostname-derived fallback. Global/system config is disabled on the
    /// child so only the exported vars can supply the identity.
    #[test]
    fn exported_env_carries_identity_into_unconfigured_repo_commit() {
        let source = init_repo("identity-src");
        let pairs = resolved_identity_env(Some(source.path()));
        assert_eq!(pairs.len(), 4);

        let target = init_repo("identity-target");
        // Strip the fixture's local user config so the repo itself resolves
        // no identity.
        let repo = git2::Repository::open(target.path()).unwrap();
        let mut cfg = repo.config().unwrap();
        cfg.remove("user.name").unwrap();
        cfg.remove("user.email").unwrap();
        drop(cfg);

        // A nonexistent path disables the global config portably (git treats
        // an unreadable GIT_CONFIG_GLOBAL target as empty).
        let no_global = target.path().join("no-global-gitconfig");
        let out = std::process::Command::new("git")
            .arg("-C")
            .arg(target.path())
            .args(["commit", "--allow-empty", "-m", "identity probe"])
            .env("GIT_CONFIG_GLOBAL", &no_global)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .envs(pairs)
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );

        let head = repo.head().unwrap().peel_to_commit().unwrap();
        assert_eq!(head.author().name().ok(), Some("Test"));
        assert_eq!(head.author().email().ok(), Some("test@example.com"));
        assert_eq!(head.committer().name().ok(), Some("Test"));
        assert_eq!(head.committer().email().ok(), Some("test@example.com"));
    }
}
