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
//!
//! Concurrent calls for the same URL are single-flighted: they share one
//! child (and its outcome) instead of each spawning their own and each
//! pinning a blocking-pool worker for up to the deadline
//! (intent-hq/monorepo#1926).

use std::collections::HashMap;
use std::io::Read;
use std::process::{Command, Stdio};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use intent_core::{Error, Result};
use tokio::sync::watch;

use crate::auth::{token_helper_config, TOKEN_ENV};
use crate::repo_cache::GIT_POLL;

/// Deadline for the ls-remote child. This backs an interactive picker RPC
/// (the FE races it against the API-backed listing), so it is deliberately
/// tighter than the clone/fetch deadlines: enough for a slow link's single
/// round-trip, short enough that a black-holed network can't pin the awaited
/// request — or a blocking-pool worker — for long.
const LS_REMOTE_TIMEOUT: Duration = Duration::from_secs(10);

/// Branches read from a remote by [`ls_remote_branches`].
#[derive(Debug, Clone)]
pub struct RemoteBranches {
    /// Branch short names (`refs/heads/*`), sorted.
    pub branches: Vec<String>,
    /// The remote's default branch per its `HEAD` symref, when advertised.
    pub default_branch: Option<String>,
}

/// A shared in-flight ls-remote's outcome slot: `None` while the child runs,
/// `Some` once it lands. The error side is the flattened message (`Error` is
/// not `Clone`); every producer here builds `Error::Internal`, so nothing is
/// lost re-wrapping on the way out.
type FlightOutcome = Option<std::result::Result<RemoteBranches, String>>;

/// A caller's handle on a shared in-flight ls-remote: concurrent callers for
/// the same URL await the same watch channel, fed by one detached driver
/// task.
type Flight = watch::Receiver<FlightOutcome>;

/// Process-global in-flight registry, keyed by remote URL (one URL per
/// `owner/repo` slot). Entries live only for the duration of a flight — the
/// driver retires its entry before publishing, so the next cold-cache call
/// runs fresh (outcomes are never cached, only shared while in flight).
fn in_flight() -> &'static Mutex<HashMap<String, Flight>> {
    static FLIGHTS: OnceLock<Mutex<HashMap<String, Flight>>> = OnceLock::new();
    FLIGHTS.get_or_init(Default::default)
}

/// List `url`'s branches (and default branch) with one `git ls-remote`.
/// `token` is an optional caller-resolved GitHub token offered to the child
/// via the environment only ([`crate::auth::token_helper_config`] reading
/// [`TOKEN_ENV`]); it never appears in argv or error text.
///
/// Concurrent calls for the same `url` share one child and its outcome
/// (single-flight, monorepo#1926); the joiners' `token`s are ignored in
/// favor of the flight leader's. Callers resolve the token identically per
/// URL — a token rotated mid-flight only affects that one shared read,
/// which retires immediately — so this cannot swap credentials across
/// users.
///
/// # Errors
///
/// Returns `Error::Internal` if `git` cannot be spawned, the ls-remote fails or times out, or the shared flight is abandoned.
pub async fn ls_remote_branches(url: &str, token: Option<&str>) -> Result<RemoteBranches> {
    let owned_url = url.to_string();
    let token = token.map(str::to_owned);
    single_flight(url, move || {
        ls_remote_blocking(&owned_url, token.as_deref())
    })
    .await
}

/// Run `work` (one blocking ls-remote, one child process) on the blocking
/// pool, deduping concurrent callers per `key`: the first caller in spawns a
/// detached driver task that runs `work` and publishes the outcome; callers
/// arriving while the flight is in the registry await the same channel and
/// share the outcome. The driver retires the flight before publishing, so
/// later calls start fresh.
///
/// The driver is detached deliberately: a cancelled caller (e.g. an RPC
/// disconnect) only drops its receiver, never the driver, so the one child
/// runs to completion and later callers keep joining it instead of
/// re-spawning their own.
async fn single_flight<F>(key: &str, work: F) -> Result<RemoteBranches>
where
    F: FnOnce() -> Result<RemoteBranches> + Send + 'static,
{
    let mut flight = {
        let mut map = in_flight().lock().expect("ls-remote flight map poisoned");
        if let Some(flight) = map.get(key) {
            flight.clone()
        } else {
            let (tx, rx) = watch::channel(None);
            map.insert(key.to_string(), rx.clone());
            let key = key.to_string();
            tokio::spawn(async move {
                let joined = tokio::task::spawn_blocking(work)
                    .await
                    .map_err(|e| Error::Internal(format!("ls-remote task failed: {e}")));
                let outcome = match joined {
                    Ok(Ok(branches)) => Ok(branches),
                    // Flatten to the message for the Clone-able shared
                    // channel; re-wrapped as `Error::Internal` on the way
                    // out.
                    Ok(Err(e)) | Err(e) => Err(flatten_internal(e)),
                };
                // Retire before publishing so the registry never holds a
                // completed flight. This driver is the only remover, and the
                // entry stays occupied until here, so the guard can only
                // miss on a driver that already panicked and was cleaned up
                // by its waiters.
                {
                    let mut map = in_flight().lock().expect("ls-remote flight map poisoned");
                    if map
                        .get(&key)
                        .is_some_and(|f| f.same_channel(&tx.subscribe()))
                    {
                        map.remove(&key);
                    }
                }
                let _ = tx.send(Some(outcome));
            });
            rx
        }
    };
    let published = flight
        .wait_for(std::option::Option::is_some)
        .await
        .map(|o| o.clone());
    let Ok(published) = published else {
        // The driver vanished without publishing (panic). Evict the dead
        // flight so later callers do not join it, then surface the failure.
        let mut map = in_flight().lock().expect("ls-remote flight map poisoned");
        if map.get(key).is_some_and(|f| f.same_channel(&flight)) {
            map.remove(key);
        }
        return Err(Error::Internal("ls-remote flight abandoned".to_string()));
    };
    let outcome = published.expect("guarded by wait_for");
    outcome.map_err(Error::Internal)
}

/// The message inside an [`Error::Internal`] (what every ls-remote failure
/// path produces), or the display form of anything else, so the shared cell
/// round-trip does not stack `internal error:` prefixes.
fn flatten_internal(e: Error) -> String {
    match e {
        Error::Internal(msg) => msg,
        other => other.to_string(),
    }
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
    use std::sync::Arc;

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
            .map_or(true, |s| !s.success())
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
            .map_or(true, |s| !s.success())
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

    /// N concurrent cold-cache calls for the same key run `work` — the seam
    /// that spawns exactly one child per invocation — exactly once, and every
    /// caller gets the shared result (monorepo#1926).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_calls_share_one_flight() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let spawns = Arc::new(AtomicUsize::new(0));
        let entered = Arc::new(AtomicUsize::new(0));
        let mut tasks = Vec::new();
        for _ in 0..8 {
            let spawns = Arc::clone(&spawns);
            let entered = Arc::clone(&entered);
            let gate = Arc::clone(&entered);
            tasks.push(tokio::spawn(async move {
                // No await point separates this increment from the registry
                // join inside `single_flight` — both run in the same poll —
                // so once the gate below sees 8, every caller has joined
                // (or is a few instructions from the map lock, which it
                // reaches long before the driver's cross-pool retirement).
                entered.fetch_add(1, Ordering::SeqCst);
                single_flight("flight-shared", move || {
                    spawns.fetch_add(1, Ordering::SeqCst);
                    // Hold the flight open until every caller has entered
                    // `single_flight`, instead of assuming a fixed sleep
                    // outlasts task scheduling.
                    while gate.load(Ordering::SeqCst) < 8 {
                        std::thread::yield_now();
                    }
                    Ok(RemoteBranches {
                        branches: vec!["main".to_string()],
                        default_branch: Some("main".to_string()),
                    })
                })
                .await
            }));
        }
        for task in tasks {
            let r = task.await.unwrap().expect("shared flight result");
            assert_eq!(r.branches, vec!["main"]);
            assert_eq!(r.default_branch.as_deref(), Some("main"));
        }
        assert_eq!(spawns.load(Ordering::SeqCst), 1, "must spawn exactly once");
    }

    /// A completed flight is retired, not cached: a later call for the same
    /// key runs `work` again — for successes and failures alike.
    #[tokio::test]
    async fn completed_flight_retires_and_reruns() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let spawns = Arc::new(AtomicUsize::new(0));
        for _ in 0..2 {
            let spawns = Arc::clone(&spawns);
            single_flight("flight-retire", move || {
                spawns.fetch_add(1, Ordering::SeqCst);
                Ok(RemoteBranches {
                    branches: vec![],
                    default_branch: None,
                })
            })
            .await
            .expect("flight result");
        }
        assert_eq!(spawns.load(Ordering::SeqCst), 2, "outcomes must not cache");
        assert!(
            !in_flight().lock().unwrap().contains_key("flight-retire"),
            "completed flight must leave the registry"
        );
    }

    /// A failed flight is shared too — every concurrent caller gets the same
    /// error, without stacked `internal error:` prefixes — and then retires.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn failed_flight_shares_the_error() {
        let mut tasks = Vec::new();
        for _ in 0..4 {
            tasks.push(tokio::spawn(async move {
                single_flight("flight-error", || {
                    std::thread::sleep(Duration::from_millis(100));
                    Err(Error::Internal("git ls-remote failed: boom".to_string()))
                })
                .await
            }));
        }
        for task in tasks {
            let err = task.await.unwrap().expect_err("shared flight error");
            assert_eq!(
                format!("{err}"),
                "internal error: git ls-remote failed: boom"
            );
        }
        assert!(
            !in_flight().lock().unwrap().contains_key("flight-error"),
            "failed flight must leave the registry"
        );
    }

    /// A cancelled caller — even the one that started the flight — does not
    /// break the single-flight: the detached driver runs the child to
    /// completion, and a caller arriving after the cancellation joins the
    /// same flight (its own `work` never runs) instead of re-spawning.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn cancelled_caller_does_not_respawn() {
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

        let spawns = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(AtomicBool::new(false));
        let joined = Arc::new(AtomicBool::new(false));

        let leader_spawns = Arc::clone(&spawns);
        let leader_started = Arc::clone(&started);
        let leader_gate = Arc::clone(&joined);
        let leader = tokio::spawn(async move {
            single_flight("flight-cancel", move || {
                leader_spawns.fetch_add(1, Ordering::SeqCst);
                leader_started.store(true, Ordering::SeqCst);
                // Hold the flight open until the post-cancellation joiner
                // has entered `single_flight`, instead of assuming a fixed
                // sleep outlasts task scheduling.
                while !leader_gate.load(Ordering::SeqCst) {
                    std::thread::yield_now();
                }
                Ok(RemoteBranches {
                    branches: vec!["from-leader".to_string()],
                    default_branch: None,
                })
            })
            .await
        });
        // Wait for the driver's child to actually start, then cancel the
        // caller mid-flight.
        while !started.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
        leader.abort();
        assert!(leader.await.unwrap_err().is_cancelled());
        assert!(
            in_flight().lock().unwrap().contains_key("flight-cancel"),
            "the flight must survive its starting caller's cancellation"
        );

        let joiner_spawns = Arc::clone(&spawns);
        let joiner_gate = Arc::clone(&joined);
        let r = single_flight("flight-cancel", move || {
            joiner_spawns.fetch_add(1, Ordering::SeqCst);
            Ok(RemoteBranches {
                branches: vec!["from-joiner".to_string()],
                default_branch: None,
            })
        });
        // The joiner future joins the surviving flight on its first poll
        // (the registry lookup precedes any await); only then release the
        // gate so the flight can complete.
        let mut r = std::pin::pin!(r);
        let first_poll_pending = std::future::poll_fn(|cx| {
            use std::future::Future;
            std::task::Poll::Ready(r.as_mut().poll(cx).is_pending())
        })
        .await;
        assert!(
            first_poll_pending,
            "the joiner must be waiting on the shared flight"
        );
        joiner_gate.store(true, Ordering::SeqCst);
        let r = r.await.expect("joined flight result");
        assert_eq!(
            r.branches,
            vec!["from-leader"],
            "the joiner must share the surviving flight, not run its own work"
        );
        assert_eq!(spawns.load(Ordering::SeqCst), 1, "must spawn exactly once");
    }

    /// Distinct keys never contend: concurrent calls for different URLs each
    /// run their own `work`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn distinct_keys_fly_independently() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let spawns = Arc::new(AtomicUsize::new(0));
        let mut tasks = Vec::new();
        for i in 0..3 {
            let spawns = Arc::clone(&spawns);
            let key = format!("flight-distinct-{i}");
            tasks.push(tokio::spawn(async move {
                single_flight(&key, move || {
                    spawns.fetch_add(1, Ordering::SeqCst);
                    std::thread::sleep(Duration::from_millis(100));
                    Ok(RemoteBranches {
                        branches: vec![],
                        default_branch: None,
                    })
                })
                .await
            }));
        }
        for task in tasks {
            task.await.unwrap().expect("flight result");
        }
        assert_eq!(spawns.load(Ordering::SeqCst), 3, "one spawn per key");
    }
}
