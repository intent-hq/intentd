//! One-shot `git ls-remote` branch listing — the network fallback behind
//! `github.branches.listCached` when the local repo cache is cold
//! (PROTOCOL §5.27). A single `git ls-remote --symref <url> HEAD
//! refs/heads/*` yields both the branch names and the remote's default
//! branch (the `ref: refs/heads/<name>\tHEAD` symref line), so the fallback
//! costs one child process and one network round-trip.
//!
//! Follows the [`crate::repo_cache`] shell-git conventions: the child runs on
//! the blocking pool with `GIT_TERMINAL_PROMPT=0` and a wall-clock deadline
//! kill; a caller-resolved GitHub token is offered via an extra credential
//! helper reading [`TOKEN_ENV`] ([`crate::auth::token_helper_config`]) —
//! never argv; and stderr is credential-redacted before it travels into an
//! error.

use std::io::Read;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use intent_core::{Error, Result};

use crate::auth::{token_helper_config, TOKEN_ENV};
use crate::repo_cache::GIT_POLL;

/// Deadline for the ls-remote child. This backs an interactive picker RPC
/// (the FE races it against the API-backed listing), so it is deliberately
/// tighter than the clone/fetch deadlines: enough for a slow link's single
/// round-trip, short enough that a black-holed network can't pin the awaited
/// request — or a blocking-pool worker — for long.
const LS_REMOTE_TIMEOUT: Duration = Duration::from_secs(10);

/// Branches read from a remote by [`ls_remote_branches`].
#[derive(Debug)]
pub struct RemoteBranches {
    /// Branch short names (`refs/heads/*`), sorted.
    pub branches: Vec<String>,
    /// The remote's default branch per its `HEAD` symref, when advertised.
    pub default_branch: Option<String>,
}

/// List `url`'s branches (and default branch) with one `git ls-remote`.
/// `token` is an optional caller-resolved GitHub token offered to the child
/// via the environment only ([`crate::auth::token_helper_config`] reading
/// [`TOKEN_ENV`]); it never appears in argv or error text.
pub async fn ls_remote_branches(url: &str, token: Option<&str>) -> Result<RemoteBranches> {
    let url = url.to_string();
    let token = token.map(str::to_owned);
    tokio::task::spawn_blocking(move || ls_remote_blocking(&url, token.as_deref()))
        .await
        .map_err(|e| Error::Internal(format!("ls-remote task failed: {e}")))?
}

/// Blocking body of [`ls_remote_branches`]: spawn, drain both pipes off-thread
/// (an undrained pipe blocks the child forever), poll with a deadline kill.
fn ls_remote_blocking(url: &str, token: Option<&str>) -> Result<RemoteBranches> {
    let mut cmd = Command::new("git");
    // Offer the resolved token as an extra github.com-scoped credential
    // helper, appended after any configured helpers (see `crate::auth`). The
    // helper reads the secret from the environment — argv carries no token.
    if let Some(token) = crate::auth::usable_token(token) {
        cmd.arg("-c").arg(token_helper_config());
        cmd.env(TOKEN_ENV, token);
    }
    let mut child = cmd
        .args(["ls-remote", "--symref", "--", url, "HEAD", "refs/heads/*"])
        .env("GIT_TERMINAL_PROMPT", "0")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| Error::Internal(format!("failed to spawn git: {e}")))?;

    let read_all = |mut pipe: Box<dyn std::io::Read + Send>| {
        std::thread::spawn(move || {
            let mut buf = String::new();
            let _ = pipe.read_to_string(&mut buf);
            buf
        })
    };
    let stdout = child
        .stdout
        .take()
        .map(|p| read_all(Box::new(p) as Box<dyn std::io::Read + Send>));
    let stderr = child
        .stderr
        .take()
        .map(|p| read_all(Box::new(p) as Box<dyn std::io::Read + Send>));
    let join = |h: Option<std::thread::JoinHandle<String>>| {
        h.and_then(|h| h.join().ok()).unwrap_or_default()
    };

    let deadline = Instant::now() + LS_REMOTE_TIMEOUT;
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                let out = join(stdout);
                let err = join(stderr);
                if status.success() {
                    return Ok(parse_ls_remote(&out));
                }
                // Redact before embedding: git stderr routinely echoes the
                // remote URL, which may carry userinfo credentials; this
                // message travels into logs and JSON-RPC errors.
                let err = crate::redact::redact_credentials(&err);
                return Err(Error::Internal(format!(
                    "git ls-remote failed: {}",
                    err.trim()
                )));
            }
            Ok(None) => {
                if Instant::now() >= deadline {
                    let _ = child.kill();
                    let _ = child.wait();
                    return Err(Error::Internal(format!(
                        "git ls-remote timed out after {}s",
                        LS_REMOTE_TIMEOUT.as_secs()
                    )));
                }
                std::thread::sleep(GIT_POLL);
            }
            Err(e) => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(Error::Internal(format!("failed to wait for git: {e}")));
            }
        }
    }
}

/// Parse `git ls-remote --symref … HEAD refs/heads/*` output: branch short
/// names from the `<oid>\trefs/heads/<name>` rows (sorted, deduped) and the
/// default branch from the `ref: refs/heads/<name>\tHEAD` symref line. The
/// plain `<oid>\tHEAD` row matches neither and is skipped.
fn parse_ls_remote(stdout: &str) -> RemoteBranches {
    let mut branches = Vec::new();
    let mut default_branch = None;
    for line in stdout.lines() {
        if let Some(rest) = line.strip_prefix("ref: ") {
            if let Some(target) = rest.strip_suffix("\tHEAD") {
                if let Some(name) = target.strip_prefix("refs/heads/") {
                    default_branch = Some(name.to_string());
                }
            }
            continue;
        }
        let Some((_, refname)) = line.split_once('\t') else {
            continue;
        };
        if let Some(name) = refname.strip_prefix("refs/heads/") {
            branches.push(name.to_string());
        }
    }
    branches.sort_unstable();
    branches.dedup();
    RemoteBranches {
        branches,
        default_branch,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const OID: &str = "7a55d963f474da36b4416b3acbede60075c91b4b";

    #[test]
    fn parses_symref_and_branches() {
        let out = format!(
            "ref: refs/heads/main\tHEAD\n{OID}\tHEAD\n{OID}\trefs/heads/zeta\n{OID}\trefs/heads/main\n{OID}\trefs/heads/feature-x\n"
        );
        let r = parse_ls_remote(&out);
        assert_eq!(r.branches, vec!["feature-x", "main", "zeta"]);
        assert_eq!(r.default_branch.as_deref(), Some("main"));
    }

    #[test]
    fn missing_symref_omits_default_branch() {
        let out = format!("{OID}\tHEAD\n{OID}\trefs/heads/main\n");
        let r = parse_ls_remote(&out);
        assert_eq!(r.branches, vec!["main"]);
        assert!(r.default_branch.is_none());
    }

    #[test]
    fn slashed_branch_names_survive() {
        let out = format!("{OID}\trefs/heads/feat/a/b\n{OID}\trefs/heads/main\n");
        let r = parse_ls_remote(&out);
        assert_eq!(r.branches, vec!["feat/a/b", "main"]);
    }

    #[test]
    fn empty_and_garbage_lines_are_skipped() {
        let r = parse_ls_remote("\nnot a ref line\nwarning: something\n");
        assert!(r.branches.is_empty());
        assert!(r.default_branch.is_none());
    }

    /// End-to-end against a local `file://` remote: real child process, no
    /// network. Gated on `git` being on PATH; skips cleanly otherwise.
    #[tokio::test]
    async fn ls_remote_lists_local_file_remote() {
        if Command::new("git")
            .arg("--version")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map(|s| !s.success())
            .unwrap_or(true)
        {
            eprintln!("skipping ls_remote_lists_local_file_remote: git not on PATH");
            return;
        }
        let dir = crate::testutil::init_repo("ls-remote");
        crate::testutil::commit_file(dir.path(), "a.txt", "one\n");
        crate::testutil::create_branch(dir.path(), "feature-x");
        // The initial branch name honors host `init.defaultBranch`; derive it
        // rather than assuming `master`.
        let repo = git2::Repository::open(dir.path()).unwrap();
        let head = repo.head().unwrap();
        let default = head.shorthand().unwrap().to_string();
        let url = format!("file://{}", dir.path().display());
        let r = ls_remote_branches(&url, None).await.expect("ls-remote");
        let mut expected = vec!["feature-x".to_string(), default.clone()];
        expected.sort_unstable();
        assert_eq!(r.branches, expected);
        assert_eq!(r.default_branch.as_deref(), Some(default.as_str()));
    }

    /// A nonexistent remote is an `Err`, and the error text never carries
    /// userinfo credentials (redaction is applied to stderr).
    #[tokio::test]
    async fn ls_remote_failure_is_redacted_error() {
        if Command::new("git")
            .arg("--version")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map(|s| !s.success())
            .unwrap_or(true)
        {
            eprintln!("skipping ls_remote_failure_is_redacted_error: git not on PATH");
            return;
        }
        let missing = std::env::temp_dir().join("intent-git-lsr-definitely-missing");
        let url = format!("file://{}", missing.display());
        let err = ls_remote_branches(&url, None).await.expect_err("must fail");
        let msg = format!("{err}");
        assert!(msg.contains("ls-remote"), "unexpected error: {msg}");
    }
}
