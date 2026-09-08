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
    let mut child = cmd
        .arg("fetch")
        .arg(remote)
        .arg(&refspec)
        .env("GIT_TERMINAL_PROMPT", "0")
        .stdin(Stdio::null())
        // Discard stdout: git-fetch progress output is only for TTY users, and
        // an undrained piped stdout would risk pipe backpressure blocking the
        // child before the deadline fires.
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
                    "git fetch failed: {}",
                    stderr.trim()
                )));
            }
            Ok(None) => {
                if Instant::now() >= deadline {
                    // Real cancellation: kill the child so no orphaned git process
                    // survives the wall-clock bound. `wait` reaps the exit status.
                    let _ = child.kill();
                    let _ = child.wait();
                    return Err(Error::Internal(format!(
                        "git fetch timed out after {}s",
                        timeout.as_secs()
                    )));
                }
                std::thread::sleep(SHELL_FETCH_POLL);
            }
            Err(e) => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(Error::Internal(format!("git fetch wait failed: {e}")));
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
    use crate::auth::token_helper_config;
    use crate::testutil::{commit_file, init_repo};
    use git2::Repository;

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

        let start = Instant::now();
        let err = fetch_with_timeout(
            dir.path(),
            "origin",
            "main",
            None,
            Duration::from_millis(500),
        )
        .expect_err("fetch must time out against an unresponsive remote");
        let elapsed = start.elapsed();
        assert!(
            elapsed < Duration::from_secs(10),
            "fetch did not honour the deadline: took {elapsed:?}"
        );
        let msg = match err {
            Error::Internal(m) => m,
            other => panic!("expected Internal error, got {other:?}"),
        };
        assert!(msg.contains("timed out"), "unexpected error message: {msg}");
    }
}
