//! Hidden, daemon-managed cache of read-only GitHub clones.
//!
//! Layout: `<cache_root>/<owner>/<repo>` — a normal clone with the remote's
//! default branch checked out. The caller passes the cache root (e.g.
//! `<workspaces_root>/.repo-cache`, dot-prefixed so it stays invisible to
//! users and recent-repo derivation); this module never reads config.
//!
//! [`ensure_cached_repo`] is the single entry point: it serializes callers on
//! a per-repo async lock, then either clones fresh (cache miss) or refreshes
//! the existing cache (`git fetch --prune origin` + hard reset to the remote
//! default branch). A refresh anomaly — diverged history, corrupt object
//! store, an interrupted prior clone, a vanished `origin/HEAD` — never fails
//! the flow: the cache dir is deleted and re-cloned from scratch. Only a
//! failed *clone* (nothing left to fall back to) surfaces as an error.
//!
//! Network git work shells out to system `git` (same rationale as
//! [`crate::fetch`]): the child inherits OpenSSH config + credential-helper
//! resolution, `GIT_TERMINAL_PROMPT=0` fails fast instead of prompting, and a
//! wall-clock deadline kills a hung child. A caller-resolved GitHub token is
//! offered via the env-backed github.com-scoped credential helper
//! ([`crate::auth::token_helper_config`]) — never argv.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use git2::Repository;
use intent_core::{Error, Result};
use tokio::sync::Mutex as AsyncMutex;

use crate::auth::{token_helper_config, TOKEN_ENV};
use crate::map_git_err;

/// Wall-clock bound for the cache clone, matching the service-layer clone
/// budget (`intent-services` CLONE_TIMEOUT).
const CACHE_CLONE_TIMEOUT: Duration = Duration::from_secs(300);

/// Wall-clock bound for the refresh fetch, matching [`crate::fetch`]'s
/// SHELL_FETCH_TIMEOUT.
const CACHE_FETCH_TIMEOUT: Duration = Duration::from_secs(100);

/// Poll interval while waiting for a shelled-out git child to exit.
const GIT_POLL: Duration = Duration::from_millis(50);

/// Process-global per-repo lock registry, keyed by cache path. Concurrent
/// workspace creates for the same repo serialize their refresh/clone here;
/// different repos never contend.
fn repo_locks() -> &'static Mutex<HashMap<PathBuf, Arc<AsyncMutex<()>>>> {
    static LOCKS: OnceLock<Mutex<HashMap<PathBuf, Arc<AsyncMutex<()>>>>> = OnceLock::new();
    LOCKS.get_or_init(Default::default)
}

/// Resolve (or create) the async lock for `cache_path`.
fn lock_for(cache_path: &Path) -> Arc<AsyncMutex<()>> {
    let mut map = repo_locks().lock().expect("repo cache lock map poisoned");
    map.entry(cache_path.to_path_buf()).or_default().clone()
}

/// Ensure `<cache_root>/<owner>/<repo>` holds a fresh cached clone of
/// `github_url` and return that path.
///
/// - Cache miss: full clone into the cache path.
/// - Cache hit: `git fetch --prune origin` + hard reset of the remote default
///   branch. Any anomaly self-heals by deleting the cache dir and re-cloning —
///   refresh never fails the flow.
///
/// `token` is an optional caller-resolved GitHub token offered to the child
/// git via the environment (see [`crate::auth`]); it never appears in argv.
/// Callers for the same repo serialize on a per-repo async lock; the git work
/// itself runs on the blocking pool.
pub async fn ensure_cached_repo(
    cache_root: &Path,
    github_url: &str,
    owner: &str,
    repo: &str,
    token: Option<&str>,
) -> Result<PathBuf> {
    validate_segment("owner", owner)?;
    validate_segment("repo", repo)?;
    let cache_path = cache_root.join(owner).join(repo);

    let lock = lock_for(&cache_path);
    let _guard = lock.lock().await;

    let path = cache_path.clone();
    let url = github_url.to_string();
    let token = token.map(str::to_owned);
    tokio::task::spawn_blocking(move || ensure_blocking(&path, &url, token.as_deref()))
        .await
        .map_err(|e| Error::Internal(format!("repo cache task failed: {e}")))??;
    Ok(cache_path)
}

/// Blocking body of [`ensure_cached_repo`]: refresh when a cache is present,
/// otherwise (or when the refresh reports any anomaly) wipe and re-clone.
fn ensure_blocking(cache_path: &Path, github_url: &str, token: Option<&str>) -> Result<()> {
    if cache_path.exists() {
        match refresh(cache_path, token) {
            Ok(()) => return Ok(()),
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    path = %cache_path.display(),
                    "repo cache refresh failed; deleting cache and re-cloning"
                );
            }
        }
    }
    remove_cache_path(cache_path)?;
    clone(github_url, cache_path, token)
}

/// Refresh an existing cache: fetch + prune, then hard-reset the remote's
/// default branch (resolved from the `origin/HEAD` symref the clone created)
/// so the checkout exactly mirrors the remote — a diverged or dirty cache is
/// clobbered, never merged. Every failure here is an anomaly the caller
/// self-heals by re-cloning.
fn refresh(cache_path: &Path, token: Option<&str>) -> Result<()> {
    run_git(
        cache_path,
        &["fetch", "--prune", "origin"],
        token,
        CACHE_FETCH_TIMEOUT,
    )?;
    let repo = Repository::open(cache_path).map_err(map_git_err)?;
    let default = default_branch(&repo)?;
    let target = repo
        .find_reference(&format!("refs/remotes/origin/{default}"))
        .map_err(map_git_err)?
        .target()
        .ok_or_else(|| Error::Internal(format!("origin/{default} has no target")))?;
    repo.reference(
        &format!("refs/heads/{default}"),
        target,
        true,
        "repo-cache refresh",
    )
    .map_err(map_git_err)?;
    repo.set_head(&format!("refs/heads/{default}"))
        .map_err(map_git_err)?;
    drop(repo);
    crate::reset::reset_hard(cache_path, "HEAD")
}

/// Resolve the remote's default branch from the `refs/remotes/origin/HEAD`
/// symref `git clone` records. A cache where the symref vanished or is not
/// symbolic is an anomaly — the caller re-clones (which re-creates it).
fn default_branch(repo: &Repository) -> Result<String> {
    let head = repo
        .find_reference("refs/remotes/origin/HEAD")
        .map_err(map_git_err)?;
    let target = head
        .symbolic_target()
        .map_err(map_git_err)?
        .ok_or_else(|| Error::Internal("origin/HEAD is not a symbolic ref".to_string()))?;
    target
        .strip_prefix("refs/remotes/origin/")
        .map(str::to_owned)
        .ok_or_else(|| Error::Internal(format!("unexpected origin/HEAD target: {target}")))
}

/// Fresh clone of `github_url` into `cache_path` (parent dirs created first).
/// A plain clone — the remote's default branch ends up checked out and
/// `origin/HEAD` recorded, exactly the state [`refresh`] relies on.
fn clone(github_url: &str, cache_path: &Path, token: Option<&str>) -> Result<()> {
    if let Some(parent) = cache_path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| Error::Internal(format!("failed to create cache dir: {e}")))?;
    }
    // `git -C <parent> clone <url> <dir-name>` so no argument is ever
    // interpreted relative to this process's cwd.
    let parent = cache_path
        .parent()
        .ok_or_else(|| Error::Internal("cache path has no parent".to_string()))?;
    let dir_name = cache_path
        .file_name()
        .ok_or_else(|| Error::Internal("cache path has no file name".to_string()))?;
    let mut args: Vec<&std::ffi::OsStr> = vec!["clone".as_ref(), github_url.as_ref()];
    args.push(dir_name.as_ref());
    run_git_os(parent, &args, token, CACHE_CLONE_TIMEOUT).inspect_err(|_| {
        // A failed clone must not leave a half-written cache behind for the
        // next caller's refresh to trip over.
        let _ = std::fs::remove_dir_all(cache_path);
    })
}

/// Delete the cache dir (self-heal / pre-clone cleanup). Missing path is fine.
fn remove_cache_path(cache_path: &Path) -> Result<()> {
    match std::fs::remove_dir_all(cache_path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(Error::Internal(format!(
            "failed to remove repo cache dir: {e}"
        ))),
    }
}

/// Reject `owner`/`repo` values that would escape the cache root or collapse
/// path segments — they come from external input (GitHub URLs / API payloads).
fn validate_segment(what: &str, value: &str) -> Result<()> {
    let bad = value.is_empty()
        || value == "."
        || value == ".."
        || value.contains('/')
        || value.contains('\\')
        || value.contains('\0');
    if bad {
        return Err(Error::InvalidParams(format!(
            "invalid repo cache {what} segment: {value:?}"
        )));
    }
    Ok(())
}

/// Shell out to `git -C <dir> <args…>` with the fail-fast/deadline-kill
/// semantics of [`crate::fetch`]: `GIT_TERMINAL_PROMPT=0`, discarded stdout,
/// piped stderr for the error message, and a poll loop that kills the child
/// at `timeout`. The optional token travels only via [`TOKEN_ENV`].
fn run_git(dir: &Path, args: &[&str], token: Option<&str>, timeout: Duration) -> Result<()> {
    let os_args: Vec<&std::ffi::OsStr> = args.iter().map(|a| a.as_ref()).collect();
    run_git_os(dir, &os_args, token, timeout)
}

fn run_git_os(
    dir: &Path,
    args: &[&std::ffi::OsStr],
    token: Option<&str>,
    timeout: Duration,
) -> Result<()> {
    let mut cmd = Command::new("git");
    cmd.arg("-C").arg(dir);
    // Offer the resolved token as an extra github.com-scoped credential
    // helper, appended after any configured helpers (see `crate::auth`). The
    // helper reads the secret from the environment — argv carries no token.
    if let Some(token) = crate::auth::usable_token(token) {
        cmd.arg("-c").arg(token_helper_config());
        cmd.env(TOKEN_ENV, token);
    }
    let mut child = cmd
        .args(args)
        .env("GIT_TERMINAL_PROMPT", "0")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| Error::Internal(format!("failed to spawn git: {e}")))?;

    let deadline = Instant::now() + timeout;
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                if status.success() {
                    return Ok(());
                }
                let stderr = read_stderr(&mut child);
                return Err(Error::Internal(format!(
                    "git {} failed: {}",
                    args.first()
                        .map(|a| a.to_string_lossy())
                        .unwrap_or_default(),
                    stderr.trim()
                )));
            }
            Ok(None) => {
                if Instant::now() >= deadline {
                    let _ = child.kill();
                    let _ = child.wait();
                    return Err(Error::Internal(format!(
                        "git {} timed out after {}s",
                        args.first()
                            .map(|a| a.to_string_lossy())
                            .unwrap_or_default(),
                        timeout.as_secs()
                    )));
                }
                std::thread::sleep(GIT_POLL);
            }
            Err(e) => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(Error::Internal(format!("git wait failed: {e}")));
            }
        }
    }
}

fn read_stderr(child: &mut std::process::Child) -> String {
    use std::io::Read;
    let mut buf = String::new();
    if let Some(mut stderr) = child.stderr.take() {
        let _ = stderr.read_to_string(&mut buf);
    }
    buf
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{commit_file, init_repo};

    /// Self-cleaning scratch dir for a cache root (testutil's TempDir is tied
    /// to `init_repo`; a cache root must start empty and non-git).
    struct CacheRoot(PathBuf);

    impl CacheRoot {
        fn new(tag: &str) -> Self {
            let path = std::env::temp_dir().join(format!(
                "intent-git-repocache-{tag}-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            std::fs::create_dir_all(&path).unwrap();
            CacheRoot(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for CacheRoot {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn file_url(path: &Path) -> String {
        format!("file://{}", path.display())
    }

    fn head_sha(path: &Path) -> String {
        Repository::open(path)
            .unwrap()
            .head()
            .unwrap()
            .target()
            .unwrap()
            .to_string()
    }

    /// Cache miss → a fresh clone lands at `<root>/<owner>/<repo>` with the
    /// origin's default branch checked out and its content on disk.
    #[tokio::test]
    async fn fresh_clone_creates_cache() {
        let origin = init_repo("repocache-origin-fresh");
        commit_file(origin.path(), "a.txt", "one\n");
        let root = CacheRoot::new("fresh");

        let path = ensure_cached_repo(
            root.path(),
            &file_url(origin.path()),
            "acme",
            "widget",
            None,
        )
        .await
        .unwrap();

        assert_eq!(path, root.path().join("acme").join("widget"));
        assert_eq!(
            std::fs::read_to_string(path.join("a.txt")).unwrap(),
            "one\n"
        );
        assert_eq!(head_sha(&path), head_sha(origin.path()));
        // The clone checked out the origin's default branch (not detached).
        let repo = Repository::open(&path).unwrap();
        assert!(repo.head().unwrap().is_branch());
    }

    /// Cache hit → refresh fetches the new origin commit and hard-resets to
    /// it, without re-cloning (a marker planted in `.git` survives).
    #[tokio::test]
    async fn refresh_updates_existing_cache_without_reclone() {
        let origin = init_repo("repocache-origin-refresh");
        commit_file(origin.path(), "a.txt", "one\n");
        let root = CacheRoot::new("refresh");
        let url = file_url(origin.path());

        let path = ensure_cached_repo(root.path(), &url, "acme", "widget", None)
            .await
            .unwrap();
        let marker = path.join(".git").join("intent-cache-marker");
        std::fs::write(&marker, "keep").unwrap();

        commit_file(origin.path(), "a.txt", "two\n");
        let path2 = ensure_cached_repo(root.path(), &url, "acme", "widget", None)
            .await
            .unwrap();

        assert_eq!(path, path2);
        assert!(marker.exists(), "refresh must not re-clone");
        assert_eq!(
            std::fs::read_to_string(path.join("a.txt")).unwrap(),
            "two\n"
        );
        assert_eq!(head_sha(&path), head_sha(origin.path()));
    }

    /// A cache that diverged from origin (local commit) is clobbered back to
    /// the origin tip by the refresh's hard reset — still no re-clone.
    #[tokio::test]
    async fn refresh_clobbers_diverged_cache() {
        let origin = init_repo("repocache-origin-diverge");
        commit_file(origin.path(), "a.txt", "one\n");
        let root = CacheRoot::new("diverge");
        let url = file_url(origin.path());

        let path = ensure_cached_repo(root.path(), &url, "acme", "widget", None)
            .await
            .unwrap();
        // Diverge the cache with a local commit origin never saw.
        {
            let repo = Repository::open(&path).unwrap();
            let mut cfg = repo.config().unwrap();
            cfg.set_str("user.name", "Test").unwrap();
            cfg.set_str("user.email", "test@example.com").unwrap();
        }
        commit_file(&path, "local.txt", "rogue\n");
        let marker = path.join(".git").join("intent-cache-marker");
        std::fs::write(&marker, "keep").unwrap();

        let path2 = ensure_cached_repo(root.path(), &url, "acme", "widget", None)
            .await
            .unwrap();

        assert_eq!(path, path2);
        assert!(marker.exists(), "divergence heals via reset, not re-clone");
        assert_eq!(head_sha(&path), head_sha(origin.path()));
        assert!(
            !path.join("local.txt").exists(),
            "hard reset must discard the rogue commit's file"
        );
    }

    /// A corrupt cache (gutted `.git`) self-heals: the dir is deleted and
    /// re-cloned instead of failing the flow.
    #[tokio::test]
    async fn corrupt_cache_self_heals_by_recloning() {
        let origin = init_repo("repocache-origin-corrupt");
        commit_file(origin.path(), "a.txt", "one\n");
        let root = CacheRoot::new("corrupt");
        let url = file_url(origin.path());

        let path = ensure_cached_repo(root.path(), &url, "acme", "widget", None)
            .await
            .unwrap();
        let marker = path.join(".git").join("intent-cache-marker");
        std::fs::write(&marker, "keep").unwrap();

        // Corrupt: replace .git with a garbage file.
        std::fs::remove_dir_all(path.join(".git")).unwrap();
        std::fs::write(path.join(".git"), "not a git dir").unwrap();

        let path2 = ensure_cached_repo(root.path(), &url, "acme", "widget", None)
            .await
            .unwrap();

        assert_eq!(path, path2);
        assert!(!marker.exists(), "self-heal must have re-cloned");
        assert_eq!(
            std::fs::read_to_string(path.join("a.txt")).unwrap(),
            "one\n"
        );
        assert_eq!(head_sha(&path), head_sha(origin.path()));
    }

    /// A cache dir that is not a repository at all (e.g. an interrupted prior
    /// clone left plain files) also self-heals into a fresh clone.
    #[tokio::test]
    async fn non_repo_cache_dir_self_heals() {
        let origin = init_repo("repocache-origin-nonrepo");
        commit_file(origin.path(), "a.txt", "one\n");
        let root = CacheRoot::new("nonrepo");
        let url = file_url(origin.path());

        let path = root.path().join("acme").join("widget");
        std::fs::create_dir_all(&path).unwrap();
        std::fs::write(path.join("junk.txt"), "leftover").unwrap();

        let path2 = ensure_cached_repo(root.path(), &url, "acme", "widget", None)
            .await
            .unwrap();

        assert_eq!(path, path2);
        assert!(!path.join("junk.txt").exists());
        assert_eq!(
            std::fs::read_to_string(path.join("a.txt")).unwrap(),
            "one\n"
        );
    }

    /// A clone failure (unreachable URL, no prior cache) surfaces as an error
    /// and leaves no half-written cache dir behind.
    #[tokio::test]
    async fn failed_clone_errors_and_leaves_no_cache() {
        let root = CacheRoot::new("clonefail");
        let missing = std::env::temp_dir().join("intent-git-repocache-definitely-missing");
        let err = ensure_cached_repo(root.path(), &file_url(&missing), "acme", "widget", None)
            .await
            .expect_err("clone from a missing path must fail");
        assert!(matches!(err, Error::Internal(_)));
        assert!(!root.path().join("acme").join("widget").exists());
    }

    /// Owner/repo segments that would escape the cache root are rejected
    /// before any filesystem or git work.
    #[tokio::test]
    async fn invalid_segments_are_rejected() {
        let root = CacheRoot::new("badseg");
        for (owner, repo) in [
            ("", "repo"),
            ("owner", ""),
            ("..", "repo"),
            ("owner", ".."),
            (".", "repo"),
            ("a/b", "repo"),
            ("owner", "a\\b"),
        ] {
            let err = ensure_cached_repo(root.path(), "file:///nowhere", owner, repo, None)
                .await
                .expect_err("invalid segment must be rejected");
            assert!(matches!(err, Error::InvalidParams(_)), "{owner:?}/{repo:?}");
        }
    }

    /// Concurrent callers for the same repo serialize on the per-repo lock:
    /// while the lock is held externally, `ensure_cached_repo` makes no
    /// progress; once released it completes.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_ensures_serialize_on_the_repo_lock() {
        let origin = init_repo("repocache-origin-lock");
        commit_file(origin.path(), "a.txt", "one\n");
        let root = CacheRoot::new("lock");
        let url = file_url(origin.path());
        let cache_path = root.path().join("acme").join("widget");

        let lock = lock_for(&cache_path);
        let guard = lock.lock().await;

        let root_path = root.path().to_path_buf();
        let url2 = url.clone();
        let task = tokio::spawn(async move {
            ensure_cached_repo(&root_path, &url2, "acme", "widget", None).await
        });

        // While the lock is held the clone must not have started.
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(
            !cache_path.exists(),
            "ensure must block on the per-repo lock"
        );

        drop(guard);
        let path = task.await.unwrap().unwrap();
        assert_eq!(path, cache_path);
        assert!(path.join("a.txt").exists());
    }

    /// Several concurrent ensures for the same repo all succeed and agree on
    /// the final state (the lock serializes the refresh/clone work).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn parallel_ensures_all_succeed() {
        let origin = init_repo("repocache-origin-parallel");
        commit_file(origin.path(), "a.txt", "one\n");
        let root = CacheRoot::new("parallel");
        let url = file_url(origin.path());

        let mut tasks = Vec::new();
        for _ in 0..4 {
            let root_path = root.path().to_path_buf();
            let url = url.clone();
            tasks.push(tokio::spawn(async move {
                ensure_cached_repo(&root_path, &url, "acme", "widget", None).await
            }));
        }
        for task in tasks {
            let path = task.await.unwrap().unwrap();
            assert_eq!(head_sha(&path), head_sha(origin.path()));
        }
    }
}
