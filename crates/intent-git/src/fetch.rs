//! Single-branch fetch (`git fetch <remote> <branch>`).
//!
//! Ports the `git fetch origin <trunk>` step the accept-changes merge/reset/rebase
//! handlers run before comparing against trunk, and the fetch step of `pull_branch`.
//! Shells out to system `git` (not libgit2) so the caller inherits OpenSSH's
//! `~/.ssh/config` + agent-forwarding + credential-helper resolution — the reference
//! TS handler ran shell git and never hit the auth-loop that pins libgit2 when
//! `ssh-agent` has no identities (the runtime-saturation vector behind the FE
//! `git.pull` and `host.status` timeouts). `GIT_TERMINAL_PROMPT=0` forces fail-fast
//! instead of a hidden prompt; a wall-clock deadline kills the child via
//! `Child::kill` if the remote hangs.
//!
//! A caller-resolved GitHub token (if any) is offered to the child as a
//! `credential.https://github.com.helper` scoped to github.com only, ordered
//! ahead of the configured helpers with those re-added behind it as fallbacks
//! (monorepo#3059 — see `auth::github_helper_entries` for why deferring to
//! them outright backfires on macOS). The token value travels via an
//! environment variable — never argv, so it cannot leak through process
//! listings or error messages.

use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use intent_core::{Error, Result};

/// Wall-clock bound for a single `git fetch` shell-out. Chosen below the
/// service-layer `GIT_PULL_TIMEOUT` (120s) so the fetch child is killed cleanly
/// by this helper before the outer `spawn_blocking + timeout` wrapper fires and
/// leaves the process orphaned.
const SHELL_FETCH_TIMEOUT: Duration = Duration::from_secs(100);

/// Poll interval used while waiting for the fetch child to exit. Small enough
/// that a completed fetch returns near-instantly, large enough that idle CPU
/// stays negligible for a long-running remote.
const SHELL_FETCH_POLL: Duration = Duration::from_millis(50);

use crate::auth::TOKEN_ENV;

/// Fetch a single `branch` from `remote` (typically `origin`), updating the local
/// remote-tracking ref `refs/remotes/<remote>/<branch>`. `token` is an optional
/// caller-resolved GitHub token used as the final credential-chain step for
/// HTTPS github.com remotes (see [`crate::auth`]). Errors when the branch
/// name is empty, `git` is not on PATH, the remote is unreachable, or the fetch
/// exceeds [`SHELL_FETCH_TIMEOUT`].
///
/// # Errors
///
/// Returns `Error::Internal` if the branch name is empty, `git` cannot be spawned, the fetch fails or times out.
pub fn fetch(worktree_path: &Path, remote: &str, branch: &str, token: Option<&str>) -> Result<()> {
    fetch_with_timeout(worktree_path, remote, branch, token, SHELL_FETCH_TIMEOUT)
}

/// The branch a workspace base refresh would fetch, if any: `base_ref` names
/// a branch (bare or `refs/remotes/<remote>/<branch>`) whose remote-tracking
/// ref already exists locally. Explicit local refs, tags, commits, `HEAD`,
/// and branches with no tracking ref return `None` (no fetch).
///
/// # Errors
/// Returns an error if the repository cannot be opened or the ref lookup fails.
pub fn workspace_base_refresh_branch(
    path: &Path,
    base_ref: Option<&str>,
    remote: &str,
) -> Result<Option<String>> {
    let Some(base) = base_ref.filter(|base| !base.is_empty()) else {
        return Ok(None);
    };
    let prefix = format!("refs/remotes/{remote}/");
    let branch = base.strip_prefix(&prefix).unwrap_or(base);
    let tracking_ref = format!("{prefix}{branch}");
    if branch == "HEAD" || !git2::Reference::is_valid_name(&tracking_ref) {
        return Ok(None);
    }
    let repo = git2::Repository::open(path).map_err(crate::map_git_err)?;
    let found = match repo.find_reference(&tracking_ref) {
        // A symbolic tracking ref (`refs/remotes/<remote>/HEAD`) names no
        // remote branch to fetch.
        Ok(reference) => reference.kind() != Some(git2::ReferenceType::Symbolic),
        Err(error) if error.code() == git2::ErrorCode::NotFound => false,
        Err(error) => return Err(crate::map_git_err(error)),
    };
    Ok(found.then(|| branch.to_string()))
}

/// Refresh a workspace base that resolves to a remote-tracking branch.
/// Explicit local refs, tags, commits, and HEAD retain their local semantics.
/// Callers hold the provisioning lock across refresh and checkout.
///
/// # Errors
/// Returns an error if the repository cannot be opened, and
/// [`Error::BaseRefreshFailed`] (classified, credential-redacted) if the
/// required fetch fails or times out.
pub fn refresh_workspace_base(
    path: &Path,
    base_ref: Option<&str>,
    remote: &str,
    token: Option<&str>,
) -> Result<()> {
    refresh_workspace_base_with_timeout(path, base_ref, remote, token, None)
}

/// [`refresh_workspace_base`] with an explicit fetch bound (`None` keeps the
/// default [`SHELL_FETCH_TIMEOUT`]); tests use a short bound to drive the
/// `timeout` category without waiting 100 s.
///
/// # Errors
/// As [`refresh_workspace_base`].
pub fn refresh_workspace_base_with_timeout(
    path: &Path,
    base_ref: Option<&str>,
    remote: &str,
    token: Option<&str>,
    timeout: Option<Duration>,
) -> Result<()> {
    let Some(branch) = workspace_base_refresh_branch(path, base_ref, remote)? else {
        return Ok(());
    };
    let refspec = format!("+refs/heads/{branch}:refs/remotes/{remote}/{branch}");
    run_fetch(
        path,
        remote,
        &refspec,
        token,
        timeout.unwrap_or(SHELL_FETCH_TIMEOUT),
    )
    .map_err(|failure| failure.into_base_refresh_error(&branch, remote))
}

/// Bound on the git stderr tail carried in a base-refresh `detail`.
const BASE_REFRESH_DETAIL_MAX: usize = 4096;

/// Why a shelled-out `git fetch` did not succeed.
#[derive(Debug)]
enum FetchFailure {
    Spawn(String),
    Failed(String),
    TimedOut(Duration),
    Wait(String),
}

fn describe_timeout(timeout: Duration) -> String {
    if timeout >= Duration::from_secs(1) {
        format!("git fetch timed out after {}s", timeout.as_secs())
    } else {
        format!("git fetch timed out after {}ms", timeout.as_millis())
    }
}

impl FetchFailure {
    fn into_internal(self) -> Error {
        Error::Internal(match self {
            Self::Spawn(e) => format!("failed to spawn git: {e}"),
            Self::Failed(stderr) => format!("git fetch failed: {}", stderr.trim()),
            Self::TimedOut(timeout) => describe_timeout(timeout),
            Self::Wait(e) => format!("git fetch wait failed: {e}"),
        })
    }

    fn into_base_refresh_error(self, base_ref: &str, remote: &str) -> Error {
        use intent_core::BaseRefreshErrorCategory as C;
        let (category, raw) = match self {
            Self::TimedOut(timeout) => (C::Timeout, describe_timeout(timeout)),
            Self::Failed(stderr) => {
                let stderr = stderr.trim().to_string();
                let raw = if stderr.is_empty() {
                    "git fetch failed".to_string()
                } else {
                    stderr
                };
                (classify_fetch_error(&raw), raw)
            }
            Self::Spawn(e) => (C::Other, format!("failed to spawn git: {e}")),
            Self::Wait(e) => (C::Other, format!("git fetch wait failed: {e}")),
        };
        Error::BaseRefreshFailed(Box::new(intent_core::BaseRefreshFailure {
            category,
            detail: bounded_tail(
                &crate::redact::redact_credentials(&raw),
                BASE_REFRESH_DETAIL_MAX,
            ),
            base_ref: base_ref.to_string(),
            remote: remote.to_string(),
        }))
    }
}

/// Keep the last `max` bytes of `text` (on a char boundary).
fn bounded_tail(text: &str, max: usize) -> String {
    if text.len() <= max {
        return text.to_string();
    }
    let mut start = text.len() - max;
    while !text.is_char_boundary(start) {
        start += 1;
    }
    text[start..].to_string()
}

/// Fetch-specific classifier for a failed base refresh (PROTOCOL §9.2),
/// checked in order, first match wins. Deliberately not the clone
/// classifier: a bare "permission denied" never reads as a bad path, and
/// "Host key verification failed" is its own category. The `timeout`
/// category comes from the daemon's own kill, never from stderr.
#[must_use]
pub fn classify_fetch_error(stderr: &str) -> intent_core::BaseRefreshErrorCategory {
    use intent_core::BaseRefreshErrorCategory as C;
    let m = stderr.to_lowercase();
    let any = |needles: &[&str]| needles.iter().any(|n| m.contains(n));
    if any(&["host key verification failed"]) {
        C::SshHostKey
    } else if any(&[
        "permission denied (publickey",
        "could not read username",
        "could not read password",
        "authentication failed",
        "terminal prompts disabled",
        "returned error: 401",
    ]) {
        C::AuthRequired
    } else if any(&[
        "could not resolve host",
        "could not resolve hostname",
        "connection refused",
        "connection timed out",
        "operation timed out",
        "network is unreachable",
        "no route to host",
        "failed to connect to",
        "connection reset by peer",
        "connection closed by",
        "kex_exchange_identification",
        "could not resolve proxy",
    ]) {
        C::Network
    } else if any(&[
        "repository not found",
        "does not appear to be a git repository",
        "returned error: 404",
    ]) {
        C::RepoNotFound
    } else if any(&["returned error: 403", "403 forbidden", "denied to"]) {
        C::AccessDenied
    } else {
        C::Other
    }
}

/// Timeout-parameterised body of [`fetch`], factored out so tests can drive
/// the deadline-kill path against a stub git binary without waiting 100s.
pub(crate) fn fetch_with_timeout(
    worktree_path: &Path,
    remote: &str,
    branch: &str,
    token: Option<&str>,
    timeout: Duration,
) -> Result<()> {
    if branch.is_empty() {
        return Err(Error::Internal(
            "cannot fetch: empty branch name".to_string(),
        ));
    }

    // Explicit refspec so the remote-tracking ref is written even when the remote
    // is not configured with a default fetch refspec (parity with the previous
    // libgit2 fetch that installed the same refspec).
    let refspec = format!("+refs/heads/{branch}:refs/remotes/{remote}/{branch}");
    fetch_refspec_with_timeout(worktree_path, remote, &refspec, token, timeout)
}

/// Fetch GitHub's canonical PR head (including fork heads) and materialize its
/// named local branch without ever overwriting an existing local branch.
/// Callers hold their repository provisioning lock across this and checkout.
///
/// # Errors
/// Returns an error on fetch failure, a moved PR head, or a conflicting local branch.
pub fn prepare_pr_branch(
    path: &Path,
    remote: &str,
    number: u64,
    branch: &str,
    expected_sha: Option<&str>,
    token: Option<&str>,
) -> Result<()> {
    prepare_pr_branch_inner(path, remote, number, branch, expected_sha, token, false)
}

/// Prepare a PR in the daemon-owned repository cache. Only branches created by
/// this helper and still at their recorded tip may advance. Callers must hold
/// the cache lock; never use this for a user repository.
///
/// # Errors
/// Returns an error on fetch failure, a moved PR head, or a conflicting branch.
pub fn prepare_cached_pr_branch(
    path: &Path,
    remote: &str,
    number: u64,
    branch: &str,
    expected_sha: Option<&str>,
    token: Option<&str>,
) -> Result<()> {
    prepare_pr_branch_inner(path, remote, number, branch, expected_sha, token, true)
}

fn prepare_pr_branch_inner(
    path: &Path,
    remote: &str,
    number: u64,
    branch: &str,
    expected_sha: Option<&str>,
    token: Option<&str>,
    cache: bool,
) -> Result<()> {
    let target = format!("refs/intent/pr/{number}/head");
    let refspec = format!("+refs/pull/{number}/head:{target}");
    fetch_refspec_with_timeout(path, remote, &refspec, token, SHELL_FETCH_TIMEOUT)?;
    let repo = git2::Repository::open(path).map_err(crate::map_git_err)?;
    let commit = repo
        .find_reference(&target)
        .and_then(|reference| reference.peel_to_commit())
        .map_err(crate::map_git_err)?;
    if expected_sha.is_some_and(|sha| !sha.eq_ignore_ascii_case(&commit.id().to_string())) {
        return Err(Error::InvalidParams(
            "PR head changed while preparing checkout; refresh the pull request and try again"
                .into(),
        ));
    }
    let ownership_ref = format!("refs/intent/pr/{number}/materialized/{branch}");
    let owned_tip = if cache {
        repo.find_reference(&ownership_ref)
            .ok()
            .and_then(|reference| reference.target())
    } else {
        None
    };
    let owns_branch = match repo.find_branch(branch, git2::BranchType::Local) {
        Ok(mut existing) => {
            let existing_tip = existing
                .get()
                .peel_to_commit()
                .map_err(crate::map_git_err)?
                .id();
            let owned = owned_tip == Some(existing_tip) && !existing.is_head();
            if existing_tip != commit.id() {
                if !owned {
                    return Err(Error::InvalidParams(format!("local branch '{branch}' differs from PR #{number}; preserving it instead of creating a workspace on the wrong commits")));
                }
                existing
                    .get_mut()
                    .set_target(commit.id(), "refresh daemon-owned PR cache branch")
                    .map_err(crate::map_git_err)?;
            }
            owned
        }
        Err(error) if error.code() == git2::ErrorCode::NotFound => {
            repo.branch(branch, &commit, false)
                .map_err(crate::map_git_err)?;
            cache
        }
        Err(error) => return Err(crate::map_git_err(error)),
    };
    if owns_branch {
        repo.reference(
            &ownership_ref,
            commit.id(),
            true,
            "record daemon-owned PR cache branch",
        )
        .map_err(crate::map_git_err)?;
    }
    Ok(())
}

fn fetch_refspec_with_timeout(
    worktree_path: &Path,
    remote: &str,
    refspec: &str,
    token: Option<&str>,
    timeout: Duration,
) -> Result<()> {
    run_fetch(worktree_path, remote, refspec, token, timeout).map_err(FetchFailure::into_internal)
}

fn run_fetch(
    worktree_path: &Path,
    remote: &str,
    refspec: &str,
    token: Option<&str>,
    timeout: Duration,
) -> std::result::Result<(), FetchFailure> {
    // `git -C <path>` so the child cwd is not this crate's cwd (parity with the
    // reference TS handler); `GIT_TERMINAL_PROMPT=0` turns any credential prompt
    // into a fast error rather than a hidden hang.
    let mut cmd = Command::new("git");
    cmd.arg("-C").arg(worktree_path);
    // Offer the resolved token as an extra credential helper scoped to
    // github.com HTTPS only, consulted *ahead* of the configured helpers
    // (which stay reachable behind it) so an OS-default helper holding a stale
    // github.com credential cannot shadow the resolved token — monorepo#3059,
    // see `auth::github_helper_entries`. `-c` entries are applied after the
    // inherited `GIT_CONFIG_PARAMETERS`, so the reset they carry covers both.
    // The helper reads the secret from the environment — the argv below
    // carries no token bytes.
    if let Some(token) = crate::auth::usable_token(token) {
        let inherited = std::env::var(crate::auth::GIT_CONFIG_PARAMETERS_ENV).ok();
        for entry in crate::auth::token_helper_entries(Some(worktree_path), inherited.as_deref()) {
            cmd.arg("-c").arg(entry);
        }
        cmd.env(TOKEN_ENV, token);
    }
    // Own process group, so a timeout kill also reaps the transport
    // (`ssh`, `git-remote-https`) instead of orphaning it.
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        cmd.process_group(0);
    }
    let mut child = cmd
        .arg("fetch")
        .arg(remote)
        .arg(refspec)
        .env("GIT_TERMINAL_PROMPT", "0")
        .stdin(Stdio::null())
        // Discard stdout: git-fetch progress output is only for TTY users, and
        // an undrained piped stdout would risk pipe backpressure blocking the
        // child before the deadline fires.
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| FetchFailure::Spawn(e.to_string()))?;

    let deadline = Instant::now() + timeout;
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                if status.success() {
                    return Ok(());
                }
                let stderr = read_stderr(&mut child);
                return Err(FetchFailure::Failed(stderr));
            }
            Ok(None) => {
                if Instant::now() >= deadline {
                    // Real cancellation: kill the child so no orphaned git process
                    // survives the wall-clock bound. `wait` reaps the exit status.
                    kill_fetch_group(&mut child);
                    return Err(FetchFailure::TimedOut(timeout));
                }
                std::thread::sleep(SHELL_FETCH_POLL);
            }
            Err(e) => {
                kill_fetch_group(&mut child);
                return Err(FetchFailure::Wait(e.to_string()));
            }
        }
    }
}

/// Kill the fetch child's whole process group (started with
/// `process_group(0)`), then the child itself, and reap it.
fn kill_fetch_group(child: &mut std::process::Child) {
    #[cfg(unix)]
    {
        // SAFETY: this PID belongs to our child, started with process_group(0).
        // A negative PID signals only that owned process group.
        unsafe {
            libc::kill(-child.id().cast_signed(), libc::SIGKILL);
        }
    }
    let _ = child.kill();
    let _ = child.wait();
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
    use crate::auth::token_helper_config;
    use crate::testutil::{commit_file, init_repo};
    use git2::Repository;

    #[test]
    fn workspace_base_refreshes_selected_remote_without_changing_source() {
        let upstream = init_repo("workspace-base-upstream");
        commit_file(upstream.path(), "a.txt", "old\n");
        let source = init_repo("workspace-base-source");
        let source_path = source.path().join("clone");
        let repo = Repository::clone(upstream.path().to_str().unwrap(), &source_path).unwrap();
        repo.remote_rename("origin", "team").unwrap();
        let branch = crate::status::current_branch(&repo);
        let old = crate::refs::rev_parse(&source_path, "HEAD").unwrap();
        std::fs::write(source_path.join("a.txt"), "local draft\n").unwrap();
        for base in [branch.clone(), format!("refs/remotes/team/{branch}")] {
            commit_file(upstream.path(), "a.txt", &base);
            let tip = crate::refs::rev_parse(upstream.path(), "HEAD").unwrap();
            refresh_workspace_base(&source_path, Some(&base), "team", None).unwrap();
            assert_eq!(
                crate::refs::rev_parse(&source_path, &format!("refs/remotes/team/{branch}"))
                    .unwrap(),
                tip
            );
            assert_eq!(crate::refs::rev_parse(&source_path, "HEAD").unwrap(), old);
            assert_eq!(
                std::fs::read_to_string(source_path.join("a.txt")).unwrap(),
                "local draft\n"
            );
        }
    }

    #[test]
    fn workspace_base_preserves_local_refs_and_fails_on_unreachable_remote_base() {
        let source = init_repo("workspace-base-local");
        commit_file(source.path(), "a.txt", "local\n");
        let repo = Repository::open(source.path()).unwrap();
        let tip = repo.head().unwrap().peel_to_commit().unwrap();
        repo.branch("local-only", &tip, false).unwrap();
        repo.tag_lightweight("v1", tip.as_object(), false).unwrap();
        repo.reference("refs/remotes/origin/main", tip.id(), false, "test")
            .unwrap();
        repo.reference_symbolic(
            "refs/remotes/origin/HEAD",
            "refs/remotes/origin/main",
            false,
            "test",
        )
        .unwrap();
        repo.remote("origin", source.path().join("missing").to_str().unwrap())
            .unwrap();
        let sha = tip.id().to_string();
        for base in [
            None,
            Some(""),
            Some("HEAD"),
            Some("refs/remotes/origin/HEAD"),
            Some("HEAD~0"),
            Some("local-only"),
            Some("refs/heads/local-only"),
            Some("v1"),
            Some(sha.as_str()),
        ] {
            refresh_workspace_base(source.path(), base, "origin", None).unwrap();
        }
        match refresh_workspace_base(source.path(), Some("main"), "origin", None) {
            Err(Error::BaseRefreshFailed(failure)) => {
                assert_eq!(
                    failure.category,
                    intent_core::BaseRefreshErrorCategory::RepoNotFound
                );
                assert_eq!(failure.base_ref, "main");
                assert_eq!(failure.remote, "origin");
            }
            other => panic!("expected BaseRefreshFailed, got {other:?}"),
        }
        assert_eq!(repo.head().unwrap().target(), Some(tip.id()));
    }

    #[test]
    fn classify_fetch_error_follows_documented_order() {
        use intent_core::BaseRefreshErrorCategory as C;
        for (stderr, want) in [
            ("Host key verification failed.\nfatal: Could not read from remote repository.", C::SshHostKey),
            ("git@github.com: Permission denied (publickey).\nfatal: Could not read from remote repository.", C::AuthRequired),
            ("fatal: could not read Username for 'https://github.com': terminal prompts disabled", C::AuthRequired),
            ("remote: Invalid username or password.\nfatal: Authentication failed for 'https://x/'", C::AuthRequired),
            ("fatal: unable to access 'https://x/': Could not resolve host: x", C::Network),
            ("ssh: Could not resolve hostname x: Name or service not known", C::Network),
            ("ssh: connect to host x port 22: Connection refused", C::Network),
            ("fatal: unable to access 'https://x/': Failed to connect to x port 443", C::Network),
            ("ssh: connect to host x port 22: Network is unreachable", C::Network),
            ("remote: Repository not found.\nfatal: repository 'https://x/' not found", C::RepoNotFound),
            ("fatal: '/tmp/missing' does not appear to be a git repository", C::RepoNotFound),
            ("remote: Permission to o/r.git denied to someone.\nfatal: unable to access: The requested URL returned error: 403", C::AccessDenied),
            ("error: cannot open .git/FETCH_HEAD: Permission denied", C::Other),
            ("fatal: couldn't find remote ref main", C::Other),
            ("fatal: could not read Password for 'https://u@x': terminal prompts disabled", C::AuthRequired),
            ("fatal: unable to access 'https://x/': The requested URL returned error: 401", C::AuthRequired),
            ("fatal: unable to access 'https://x/': The requested URL returned error: 404", C::RepoNotFound),
            ("fatal: unable to access 'https://x/': The requested URL returned error: 403", C::AccessDenied),
            ("error: 403 Forbidden while accessing https://x/", C::AccessDenied),
            ("ssh: connect to host x port 22: Connection timed out", C::Network),
            ("kex_exchange_identification: Connection closed by remote host", C::Network),
            ("fatal: unable to access 'https://x/': Could not resolve proxy: p", C::Network),
            // First match wins across categories.
            ("Host key verification failed.\nPermission denied (publickey).", C::SshHostKey),
            ("Permission denied (publickey).\nssh: Could not resolve hostname x", C::AuthRequired),
            ("Could not resolve host: x\nRepository not found", C::Network),
            ("Repository not found\nThe requested URL returned error: 403", C::RepoNotFound),
        ] {
            assert_eq!(classify_fetch_error(stderr), want, "{stderr}");
        }
    }

    #[test]
    fn base_refresh_error_redacts_and_bounds_detail() {
        let noisy = format!(
            "{}fatal: unable to access 'https://user:s3cret@github.com/o/r.git/': Could not resolve host: github.com",
            "x".repeat(BASE_REFRESH_DETAIL_MAX)
        );
        let Error::BaseRefreshFailed(failure) =
            FetchFailure::Failed(noisy).into_base_refresh_error("main", "origin")
        else {
            panic!("expected BaseRefreshFailed");
        };
        assert!(!failure.detail.contains("s3cret"), "{}", failure.detail);
        assert!(failure.detail.contains("***@github.com"));
        assert!(failure.detail.len() <= BASE_REFRESH_DETAIL_MAX);
        assert_eq!(
            failure.category,
            intent_core::BaseRefreshErrorCategory::Network
        );
        let Error::BaseRefreshFailed(failure) = FetchFailure::TimedOut(Duration::from_secs(100))
            .into_base_refresh_error("main", "origin")
        else {
            panic!("expected BaseRefreshFailed");
        };
        assert_eq!(
            failure.category,
            intent_core::BaseRefreshErrorCategory::Timeout
        );
        assert_eq!(failure.detail, "git fetch timed out after 100s");
    }

    /// Fetch a branch from a local bare remote and confirm the local
    /// remote-tracking ref now points at the remote commit.
    #[test]
    fn fetch_updates_tracking_ref() {
        // Seed a source repo and push it into a bare remote to act as origin.
        let src = init_repo("fetch-src");
        commit_file(src.path(), "a.txt", "one\n");
        let src_repo = Repository::open(src.path()).unwrap();
        let branch = crate::status::current_branch(&src_repo);

        let bare_dir = std::env::temp_dir().join(format!(
            "intent-git-fetch-bare-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        Repository::init_bare(&bare_dir).unwrap();
        src_repo
            .remote("origin", bare_dir.to_str().unwrap())
            .unwrap();
        crate::push::push(src.path(), "origin", &branch, false, None).unwrap();

        // A second clone-like repo points at the same bare remote and fetches.
        let consumer = init_repo("fetch-consumer");
        commit_file(consumer.path(), "seed.txt", "seed\n");
        let consumer_repo = Repository::open(consumer.path()).unwrap();
        consumer_repo
            .remote("origin", bare_dir.to_str().unwrap())
            .unwrap();

        fetch(consumer.path(), "origin", &branch, None).unwrap();

        let bare = Repository::open_bare(&bare_dir).unwrap();
        let remote_sha = bare
            .find_reference(&format!("refs/heads/{branch}"))
            .unwrap()
            .target()
            .unwrap()
            .to_string();
        let tracking = consumer_repo
            .find_reference(&format!("refs/remotes/origin/{branch}"))
            .unwrap()
            .target()
            .unwrap()
            .to_string();
        assert_eq!(tracking, remote_sha);

        let _ = std::fs::remove_dir_all(&bare_dir);
    }

    #[test]
    fn empty_branch_is_rejected() {
        let dir = init_repo("fetch-empty-branch");
        commit_file(dir.path(), "a.txt", "x\n");
        assert!(fetch(dir.path(), "origin", "", None).is_err());
    }

    /// A missing / unreachable remote produces a structured `Err`, not a hang.
    /// This exercises the fail-fast path enforced by `GIT_TERMINAL_PROMPT=0`.
    #[test]
    fn missing_remote_errors_fast() {
        let dir = init_repo("fetch-no-remote");
        commit_file(dir.path(), "a.txt", "x\n");
        let err = fetch(dir.path(), "origin", "main", None)
            .expect_err("fetch against a missing remote must error");
        let msg = match err {
            Error::Internal(m) => m,
            other => panic!("expected Internal error, got {other:?}"),
        };
        assert!(
            msg.contains("git fetch failed"),
            "unexpected error message: {msg}"
        );
    }

    /// The `-c` credential-helper config never contains token bytes (the token
    /// travels via the environment), and the snippet emits the expected
    /// `username=`/`password=` lines when driven exactly as git drives a
    /// `!`-helper (`sh -c '<snippet> get'` with the env var set).
    #[test]
    fn token_helper_config_is_token_free_and_emits_credentials() {
        let config = token_helper_config();
        let token = "tok%$\"weird";
        assert!(
            !config.contains(token),
            "helper config must not embed the token"
        );
        // The shell must see `"$INTENT_GIT_GITHUB_TOKEN"`, not Rust's
        // `{TOKEN_ENV}` placeholder.
        assert!(config.contains(&format!("\"${TOKEN_ENV}\"")));

        let snippet = config
            .strip_prefix("credential.https://github.com.helper=!")
            .expect("config must be a github.com-scoped ! helper");
        let out = Command::new("sh")
            .arg("-c")
            .arg(format!("{snippet} get"))
            .env(TOKEN_ENV, token)
            .output()
            .expect("sh must run the helper snippet");
        assert!(out.status.success());
        let stdout = String::from_utf8_lossy(&out.stdout);
        assert_eq!(
            stdout,
            format!("username=x-access-token\npassword={token}\n")
        );
        // Non-`get` ops exit 0 with no output (store/erase must not fail).
        let out = Command::new("sh")
            .arg("-c")
            .arg(format!("{snippet} store"))
            .env(TOKEN_ENV, token)
            .output()
            .unwrap();
        assert!(out.status.success());
        assert!(out.stdout.is_empty());
    }

    /// The wall-clock timeout kills the child and returns a structured error
    /// rather than letting it run indefinitely. Simulated by pointing `git`
    /// at a loopback listener that is never accepted from: the kernel
    /// completes the TCP handshake into the backlog, the child sends its
    /// HTTP request and then stalls forever waiting for a response, so the
    /// timeout must fire. (A reserved TEST-NET-1 IP was used before, but
    /// some environments reject it immediately with "Network is
    /// unreachable" instead of stalling — intent-hq/monorepo#4210.)
    #[test]
    fn fetch_timeout_kills_child() {
        let dir = init_repo("fetch-timeout");
        commit_file(dir.path(), "a.txt", "x\n");
        // Bound but never accepted: connections sit in the listen backlog
        // with no peer ever responding. Kept alive past the fetch so the
        // pending connection is not reset by the listener closing.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let repo = Repository::open(dir.path()).unwrap();
        repo.remote("origin", &format!("http://127.0.0.1:{port}/repo.git"))
            .unwrap();

        // The remote never answers, so only the timeout can end the fetch:
        // the "timed out" error below names it.
        let err = fetch_with_timeout(
            dir.path(),
            "origin",
            "main",
            None,
            Duration::from_millis(500),
        )
        .expect_err("fetch must time out against an unresponsive remote");
        let msg = match err {
            Error::Internal(m) => m,
            other => panic!("expected Internal error, got {other:?}"),
        };
        assert!(msg.contains("timed out"), "unexpected error message: {msg}");
    }
}
