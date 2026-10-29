//! Test-harness guard that keeps every test away from the developer's real
//! `~/intent` state.
//!
//! The guard is active in every nextest test process — nextest exports
//! [`NEXTEST_RUN_ENV`] to each test and the children it spawns inherit it —
//! with no harness configuration: the forbidden home is then the current
//! user's home from the user database (`getpwuid_r`), deliberately **not**
//! `$HOME`, so tests that point `HOME` at a tempdir keep exercising the default
//! resolution while the real home stays off-limits. [`FORBIDDEN_HOME_ENV`]
//! overrides that choice (and activates the guard outside nextest, e.g. for
//! the regression tests or a non-unix host).
//!
//! Two behaviours, by how the path was chosen:
//!
//! - **Default resolution redirects.** [`FileSecretStore::new`](crate::FileSecretStore::new)
//!   — reached from module-level token loaders (GitHub, GitLab, Linear,
//!   Sentry, voice) that have no Services context — gets an empty
//!   per-process scratch file instead ([`redirect_default_secrets`]), so those
//!   tests behave like a clean CI home and never read the developer's tokens.
//! - **Explicit paths abort.** A store built with `with_path` into the real
//!   home, or the default workspaces root resolving there, aborts the
//!   process via [`assert_outside_forbidden_home`].
//!
//! The forbidden set is `<home>/intent` and everything below it, except paths
//! inside a checkout — `<home>/intent/{workspaces,worktrees}/<id>/…`
//! ([`CHECKOUT_ROOTS`]): developer checkouts (and their `target/` dirs) live
//! there, and tests legitimately write inside them. The checkout roots
//! themselves and a bare `<id>` directory stay forbidden, so the default
//! workspaces root can never be provisioned into. Tests that point `HOME` at a tempdir keep exercising the
//! default resolution; only the real home is off-limits. Production processes
//! never see the variable, so the guard is inert outside the harness.
//!
//! Isolated constructors: `FileSecretStore::with_path(<tempdir>)`,
//! `Services::new_isolated` / `new_repository_fixture` (intent-services), or
//! an `INTENTD_SECRETS_FILE` / `INTENTD_WORKSPACES_DIR` pointing at a tempdir.

use std::path::{Path, PathBuf};

/// Environment variable naming the real home tests must never touch
/// (overrides the user-database home; see the module docs).
pub const FORBIDDEN_HOME_ENV: &str = "INTENTD_TEST_FORBIDDEN_HOME";

/// Set by nextest in every test process; activates the guard.
pub const NEXTEST_RUN_ENV: &str = "NEXTEST_RUN_ID";

/// `<home>/intent` children that hold developer checkouts; paths at least two
/// levels inside them (`<root>/<id>/…`) are allowed.
pub const CHECKOUT_ROOTS: &[&str] = &["workspaces", "worktrees"];

/// The forbidden `<home>/intent` directory, or `None` when the guard is
/// inactive (neither [`FORBIDDEN_HOME_ENV`] nor [`NEXTEST_RUN_ENV`] is set —
/// every production process). Under nextest the home is the user-database
/// home, falling back to `$HOME` when that lookup fails (an unlisted container
/// UID), so the guard never silently switches off. Cached per input env.
#[must_use]
pub fn forbidden_intent_dir() -> Option<PathBuf> {
    type Key = (
        Option<std::ffi::OsString>,
        Option<std::ffi::OsString>,
        Option<std::ffi::OsString>,
    );
    static CACHE: std::sync::Mutex<Option<(Key, Option<PathBuf>)>> = std::sync::Mutex::new(None);
    let non_empty = |key: &str| std::env::var_os(key).filter(|v| !v.is_empty());
    let key: Key = (
        non_empty(FORBIDDEN_HOME_ENV),
        non_empty(NEXTEST_RUN_ENV),
        non_empty("HOME"),
    );
    let mut cache = CACHE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some((cached, dir)) = cache.as_ref() {
        if *cached == key {
            return dir.clone();
        }
    }
    let home = match &key {
        (Some(home), _, _) => Some(PathBuf::from(home)),
        (None, Some(_), home) => {
            crate::path_utils::user_db_home().or_else(|| home.clone().map(PathBuf::from))
        }
        (None, None, _) => None,
    };
    let dir = home.map(|home| normalize(&home.join("intent")));
    *cache = Some((key, dir.clone()));
    dir
}

/// Whether `path` is inside the forbidden `<home>/intent` state (see the
/// module docs).
#[must_use]
pub fn is_in_forbidden_home(path: &Path) -> bool {
    let Some(forbidden) = forbidden_intent_dir() else {
        return false;
    };
    let path = normalize(path);
    let Ok(rest) = path.strip_prefix(&forbidden) else {
        return false;
    };
    let mut parts = rest.components();
    let in_checkout = parts
        .next()
        .is_some_and(|root| CHECKOUT_ROOTS.iter().any(|r| root.as_os_str() == *r))
        && parts.next().is_some()
        && parts.next().is_some();
    !in_checkout
}

/// Under the test harness, the scratch file that replaces a default secrets
/// path landing in the real home (see the module docs): `None` when the guard
/// is inactive or `path` is outside the forbidden home. The file lives in a
/// unique per-process directory under the system temp dir and is only created
/// if a test writes through the default store.
///
/// Aborts (via [`assert_outside_forbidden_home`]) if the temp dir itself is
/// inside the forbidden home, rather than writing there.
#[must_use]
pub fn redirect_default_secrets(path: &Path) -> Option<PathBuf> {
    if !is_in_forbidden_home(path) {
        return None;
    }
    let scratch = scratch_secrets_path();
    assert_outside_forbidden_home(&scratch, "the redirected default secrets store");
    Some(scratch)
}

/// This process's scratch secrets file: a unique, unpredictable directory
/// name (pid + random UUID) so a later process reusing the PID can never
/// inherit its secrets and no other user can pre-create it. The directory
/// is created (owner-only) on first write by the store itself.
fn scratch_secrets_path() -> PathBuf {
    static SCRATCH: std::sync::OnceLock<PathBuf> = std::sync::OnceLock::new();
    SCRATCH
        .get_or_init(|| {
            std::env::temp_dir()
                .join(format!(
                    "intentd-test-default-secrets-{}-{}",
                    std::process::id(),
                    uuid::Uuid::new_v4()
                ))
                .join(".secrets.json")
        })
        .clone()
}

/// Abort the process when `path` is inside the forbidden home (see the
/// module docs); `what` names the resolver for the message.
///
/// Aborts rather than panics on purpose: default stores are often built
/// inside `spawn_blocking` or background tasks whose panics are swallowed
/// into a `JoinError` (the token loaders treat that as "no token"), which
/// would let a violating test pass silently. An abort always fails the test
/// process — or the daemon it spawned — with the message on stderr.
pub fn assert_outside_forbidden_home(path: &Path, what: &str) {
    if is_in_forbidden_home(path) {
        eprintln!(
            "hermetic-tests: {what} resolved {} inside the real home's Intent state \
             (test harness active). Tests must never touch the developer's ~/intent: \
             use FileSecretStore::with_path(<tempdir>), Services::new_isolated / \
             Services::new_repository_fixture, or point INTENTD_SECRETS_FILE / \
             INTENTD_WORKSPACES_DIR at a tempdir. Aborting.\n{}",
            path.display(),
            std::backtrace::Backtrace::force_capture(),
        );
        std::process::abort();
    }
}

/// Resolve `path` the way the OS would: make it absolute, then walk its
/// components, canonicalizing every prefix that exists (so `/var` →
/// `/private/var` and symlinked homes compare equal) and applying `..` to the
/// resolved prefix, never lexically past a symlink. Components below the
/// first missing one are appended (with `..` popping them) unchanged.
fn normalize(path: &Path) -> PathBuf {
    use std::path::Component;
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir().map_or_else(|_| path.to_path_buf(), |cwd| cwd.join(path))
    };
    let mut out = PathBuf::new();
    for component in absolute.components() {
        match component {
            Component::Prefix(_) | Component::RootDir => out.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                out.pop();
            }
            Component::Normal(name) => {
                out.push(name);
                if let Ok(canonical) = std::fs::canonicalize(&out) {
                    out = canonical;
                }
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn forbidden_set_covers_intent_state_but_not_checkouts() {
        let _env = crate::secrets::tests_env_lock();
        let home = std::env::temp_dir().join(format!("intent-core-iso-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(home.join("intent").join("workspaces").join("ws")).unwrap();
        let saved = std::env::var_os(FORBIDDEN_HOME_ENV);
        std::env::set_var(FORBIDDEN_HOME_ENV, &home);
        let intent = home.join("intent");
        let forbidden = [
            intent.clone(),
            intent.join(".secrets.json"),
            intent.join("secrets.json"),
            intent.join(".secrets.json.collaboration").join("d.json"),
            intent.join("workspaces"),
            intent.join("workspaces").join("new-ws"),
            intent.join("worktrees"),
            intent.join("logs").join("x.log"),
            intent
                .join("workspaces")
                .join("new")
                .join("..")
                .join("..")
                .join(".secrets.json"),
            intent
                .join("workspaces")
                .join("ws")
                .join("..")
                .join("..")
                .join("x"),
        ];
        let allowed = [
            intent.join("workspaces").join("ws").join("repo"),
            intent.join("worktrees").join("wt").join("target").join("x"),
            home.join(".secrets.json"),
            home.join("intentional").join("x"),
        ];
        let forbidden_hits: Vec<_> = forbidden.iter().map(|p| is_in_forbidden_home(p)).collect();
        let allowed_hits: Vec<_> = allowed.iter().map(|p| is_in_forbidden_home(p)).collect();
        std::env::remove_var(FORBIDDEN_HOME_ENV);
        let inert = is_in_forbidden_home(&intent.join(".secrets.json"));
        match saved {
            Some(v) => std::env::set_var(FORBIDDEN_HOME_ENV, v),
            None => std::env::remove_var(FORBIDDEN_HOME_ENV),
        }
        std::fs::remove_dir_all(&home).unwrap();
        assert!(
            forbidden_hits.iter().all(|h| *h),
            "{forbidden:?} -> {forbidden_hits:?}"
        );
        assert!(
            allowed_hits.iter().all(|h| !*h),
            "{allowed:?} -> {allowed_hits:?}"
        );
        assert!(
            !inert,
            "with no marker only the user-database home is forbidden"
        );
    }

    #[test]
    fn normalize_applies_parent_dirs_inside_missing_tails() {
        let dir = std::env::temp_dir().join(format!("intent-core-iso-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(dir.join("intent").join("workspaces")).unwrap();
        let canonical = std::fs::canonicalize(&dir).unwrap();
        assert_eq!(
            normalize(
                &dir.join("intent")
                    .join("workspaces")
                    .join("new")
                    .join("..")
                    .join("..")
                    .join(".secrets.json")
            ),
            canonical.join("intent").join(".secrets.json")
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn normalize_resolves_parent_dirs_through_symlinks() {
        let dir = std::env::temp_dir().join(format!("intent-core-iso-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(dir.join("real").join("inner")).unwrap();
        std::os::unix::fs::symlink(dir.join("real").join("inner"), dir.join("link")).unwrap();
        let canonical = std::fs::canonicalize(&dir).unwrap();
        assert_eq!(
            normalize(&dir.join("link").join("..").join("x")),
            canonical.join("real").join("x")
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn normalize_resolves_existing_prefix_and_keeps_missing_tail() {
        let dir = std::env::temp_dir().join(format!("intent-core-iso-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(dir.join("a")).unwrap();
        let canonical = std::fs::canonicalize(&dir).unwrap();
        assert_eq!(
            normalize(&dir.join("a").join("..").join("missing").join("x")),
            canonical.join("missing").join("x")
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
