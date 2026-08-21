//! Streaming `git.clone` (§5.6 / §6.5).
//!
//! Spawns `git clone --progress <url> <target>` with a piped stderr, parses the
//! canonical progress phases (counting / compressing / receiving / resolving /
//! checkout) into `git:clone:progress` bus frames, and emits a terminal
//! `git:clone:done` when the child exits — mirroring the streaming shape of
//! `search:result` / `search:done` (§14.3) but over a real subprocess rather
//! than an in-process walk.
//!
//! Secret-safety: neither the URL nor the environment ever crosses the wire.
//! The URL is used at spawn time only; if the child fails, its stderr is
//! stripped of any `user:pass@` credential fragment before being surfaced in
//! the terminal `git:clone:done { error }` frame.
//!
//! A caller-resolved GitHub token (if any) is offered to the child as a
//! `credential.https://github.com.helper` scoped to github.com only, appended
//! after any configured helpers so existing setups keep winning. The token
//! value travels via an environment variable — never argv, so it cannot leak
//! through process listings or error messages (monorepo#825; same pattern as
//! `intent_git::fetch`).

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use intent_core::events::{GIT_CLONE_DONE, GIT_CLONE_PROGRESS};
use intent_core::{expand_tilde, now_iso, CloneErrorCategory, Error, Result, WorkspaceId};
use intent_store::NewEvent;
use serde_json::json;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;

use crate::events::EventBus;
use crate::system_actor;

/// Hard cap on a clone: the FE has always allowed 5 minutes; keep that budget
/// so a stalled child never wedges the daemon.
const CLONE_TIMEOUT: Duration = Duration::from_secs(300);

/// Grace period between SIGTERM and SIGKILL when reaping a timed-out clone,
/// mirroring `host_exec`'s `TERM_GRACE` so the whole process group (git-remote-
/// https / git-fetch-pack / git-index-pack) settles before we escalate.
const TERM_GRACE: Duration = Duration::from_millis(500);

/// Bound on the stderr tail retained by [`stream_stderr`] for error messages.
const STDERR_TAIL_MAX: usize = 4096;

/// Preserved from the reference `cloneWithProgress`: skip LFS smudge during
/// checkout so a missing/unreachable LFS object does not fail the clone. The
/// caller can invoke `git lfs pull` after the clone succeeds.
const GIT_LFS_SKIP_SMUDGE: &str = "1";

/// Derive the on-disk `<parent_dir>/<target>` a clone would produce. When
/// `target_name` is `None`, port `git`'s own basename-of-URL default (strip a
/// trailing `.git`), rejecting anything that would escape `parent_dir`. A
/// leading `~` / `~/` in `parent_dir` expands to `$HOME`
/// (intent-hq/monorepo#822); `~user` forms pass through unchanged.
pub(crate) fn resolve_target_path(
    parent_dir: &str,
    url: &str,
    target_name: Option<&str>,
) -> Result<PathBuf> {
    let parent = expand_tilde(parent_dir);
    if parent.as_os_str().is_empty() {
        return Err(Error::InvalidParams("parentDir is required".to_string()));
    }
    let name = match target_name {
        Some(n) => n.trim().to_string(),
        None => derive_default_target(url),
    };
    if name.is_empty() || name == "." || name == ".." || name.contains('/') || name.contains('\\') {
        return Err(Error::InvalidParams(
            "targetName must be a single path segment".to_string(),
        ));
    }
    Ok(parent.join(name))
}

/// Basename-of-URL default (strip a trailing `.git`), matching `git clone`.
pub(crate) fn derive_default_target(url: &str) -> String {
    let trimmed = url.trim().trim_end_matches('/');
    let base = trimmed.rsplit(['/', ':']).next().unwrap_or("");
    base.strip_suffix(".git").unwrap_or(base).to_string()
}

/// Best-effort `(owner, name)` extraction for a GitHub-style clone URL. Returns
/// `None` when the URL does not carry an `owner/name` pair (bare filesystem
/// paths, single-segment URLs, etc.); callers should fall back to any
/// caller-supplied override.
pub(crate) fn parse_owner_repo(url: &str) -> Option<(String, String)> {
    let trimmed = url.trim().trim_end_matches('/');
    let after_scheme = match trimmed.split_once("://") {
        Some((_, rest)) => rest,
        None => trimmed,
    };
    let path = match after_scheme.split_once(['/', ':']) {
        Some((_host, rest)) => rest,
        None => return None,
    };
    let mut segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    if segments.len() < 2 {
        return None;
    }
    let raw_name = segments.pop()?;
    let owner = segments.pop()?.to_string();
    let name = raw_name
        .strip_suffix(".git")
        .unwrap_or(raw_name)
        .to_string();
    if owner.is_empty() || name.is_empty() {
        return None;
    }
    Some((owner, name))
}

/// Redact a `user[:pass]@` credential fragment from any URL-like substring in
/// `text` (see [`intent_git::redact::redact_credentials`], monorepo#836).
/// Best-effort; used for the terminal `error` payload.
pub(crate) fn redact_credentials(text: &str) -> String {
    intent_git::redact::redact_credentials(text)
}

/// Classify a (already-redacted) clone failure message into a machine-readable
/// [`CloneErrorCategory`] so `workspace.create` can surface a typed error
/// instead of a bare "Internal error" (monorepo#826). Best-effort pattern
/// match over git's stderr prose; anything unrecognized is `Other` — the
/// sanitized detail still travels with the error either way.
pub(crate) fn classify_clone_error(detail: &str) -> intent_core::CloneErrorCategory {
    use intent_core::CloneErrorCategory as C;
    let m = detail.to_lowercase();
    if m.contains("already exists and is not an empty directory") {
        return C::DestinationExistsNonEmpty;
    }
    // Ordered before auth: an askpass exec failure drags the auth prose along
    // with it ("could not read Username … terminal prompts disabled" follows
    // once the helper fails to run), but the remedy is local — the helper
    // script is missing/unreachable (e.g. macOS quarantine), not the
    // credentials (monorepo#837).
    if m.contains("ssh-askpass-intent")
        || (m.contains("cannot exec") && m.contains("askpass"))
        || (m.contains("app.asar") && m.contains("not a directory"))
    {
        return C::AskpassMissing;
    }
    if m.contains("authentication failed")
        || m.contains("could not read username")
        || m.contains("could not read password")
        || m.contains("terminal prompts disabled")
        // No closing paren: sshd emits multi-method forms like
        // `Permission denied (publickey,password).` on non-GitHub hosts.
        || m.contains("permission denied (publickey")
        || m.contains("invalid username or password")
        // GitLab's credential rejection ("remote: HTTP Basic: Access denied.
        // The provided password or token is incorrect…") carries no
        // "authentication failed" prose; match it here so it does not fall
        // through to the access-denied row — the remedy is credentials.
        || m.contains("http basic: access denied")
    {
        return C::AuthRequired;
    }
    // Ordered after auth: GitHub answers 404/"Repository not found" for
    // private repositories the presented credentials cannot see, but the
    // agreed taxonomy still spells that `repo-not-found` (monorepo#825).
    if m.contains("repository not found")
        || m.contains("returned error: 404")
        || m.contains("404: not found")
    {
        return C::RepoNotFound;
    }
    if m.contains("returned error: 403") || m.contains("access denied") {
        return C::AccessDenied;
    }
    if m.contains("could not resolve host")
        || m.contains("network is unreachable")
        || m.contains("connection refused")
        || m.contains("connection reset")
        || m.contains("timed out")
        || m.contains("could not connect to")
        || m.contains("early eof")
        || m.contains("remote end hung up unexpectedly")
    {
        return C::Network;
    }
    // Path-shaped git stderr (remaining #826/#562 slice): git reports
    // destination failures via `die_errno` prefixes ("could not create
    // leading directories of '…': <errno>") whose errno suffix varies, so
    // match the prefixes errno-agnostically alongside the common errno
    // spellings (ENOENT/EACCES/EROFS/ENOTDIR/ENAMETOOLONG).
    //
    // Checkout-time `error: invalid path '…'` is deliberately NOT matched:
    // it is caused by repository content (git's verify_path /
    // core.protectNTFS / core.protectHFS rejecting a tracked filename),
    // not the user-supplied destination, so §9.1's "correct the path and
    // retry" remedy does not apply; it rides the clone-failed catch-all.
    //
    // The §9.1 `askpass-missing` shape ("… app.asar …: not a directory",
    // monorepo#837) is handled by the `AskpassMissing` arm above, ordered
    // ahead of both the auth arm and this path arm — its "not a directory"
    // row must not swallow the askpass ENOTDIR spelling.
    if m.contains("is not a valid path")
        || m.contains("could not create work tree")
        || m.contains("could not create directory")
        || m.contains("could not create leading directories")
        || m.contains("permission denied")
        || m.contains("read-only file system")
        || m.contains("no such file or directory")
        || m.contains("not a directory")
        || m.contains("file name too long")
    {
        return C::PathInvalid;
    }
    C::Other
}

/// Streaming-clone request handed to [`run_clone`]. Held plain (not `Clone`)
/// because it is consumed once by the spawned task.
pub(crate) struct CloneJob {
    pub request_id: String,
    /// Workspace to publish progress under. `None` when the caller has no
    /// workspace context (workspace-adjacent clone); the empty [`WorkspaceId`]
    /// is used so `workspaceId`-less subscribers still receive the frames.
    pub workspace_id: Option<WorkspaceId>,
    pub url: String,
    pub target_path: PathBuf,
    /// Caller-resolved GitHub token offered to the child git as a
    /// github.com-scoped credential helper (monorepo#825). `None` for SSH /
    /// non-GitHub URLs or when no token resolves; the value travels via the
    /// environment only — never argv, never logged.
    pub token: Option<String>,
    pub bus: EventBus,
    /// Unified `workspace.create` progress reporter (PROTOCOL §5.1). When
    /// set, every frame routes through the reporter — stderr percentages are
    /// normalized into the 0–85 clone segment of the create's unified scale,
    /// carry the create's `progressId`, and NO terminal frames are emitted
    /// here (the create wrapper owns the exactly-one `git:clone:done`).
    /// `None` keeps the legacy standalone `git.clone` framing exactly.
    pub progress: Option<std::sync::Arc<crate::create_progress::CreateProgress>>,
    /// Clone with `--recurse-submodules` so the checkout lands with populated
    /// submodule work trees. Set by the `workspace.create` explicit-`clonePath`
    /// arm; the standalone `git.clone` RPC keeps its historical plain clone.
    pub recurse_submodules: bool,
}

/// Kick off a streaming clone on a background task and return immediately. The
/// task publishes `git:clone:progress` frames as they are parsed and one
/// terminal `git:clone:done` when the child exits, times out, or fails to
/// spawn. Never returns an error — spawn failures are surfaced on the terminal
/// event so the caller only correlates by `requestId`.
pub(crate) fn spawn_clone(job: CloneJob) {
    tokio::spawn(async move {
        let _ = run_clone(job).await;
    });
}

/// Same pipeline as [`spawn_clone`] but runs on the current task and returns
/// the clone outcome. Used by `workspace.create` (`githubUrl` orchestration,
/// PROTOCOL §5.1): the caller needs to fail the whole create atomically when
/// the clone fails, and needs to know the target checkout succeeded before
/// promoting it to `repositoryPath`. The terminal `git:clone:done` frame is
/// still published, so remote clients observe the same event flow as
/// `git.clone`.
pub(crate) async fn perform_clone(job: CloneJob) -> std::result::Result<(), CloneFailure> {
    run_clone(job).await
}

/// A failed clone with its classification already decided by [`run_clone`],
/// so the JSON-RPC surface (`workspace.create`) and the streamed
/// `git:clone:done` frame share one classification decision. `category` is
/// `None` for failures that were deliberately not classified (spawn / wait
/// errors — environmental, not git stderr); callers map that to the
/// `clone-failed` catch-all (PROTOCOL §9.1).
pub(crate) struct CloneFailure {
    pub category: Option<CloneErrorCategory>,
    /// Human-readable cause, already credential-redacted.
    pub detail: String,
}

/// The machine-readable `errorCode` for a `git:clone:done` frame: the
/// classified category, or `None` when classification fell through to the
/// catch-all — the event omits the key for unclassified failures so existing
/// consumers of the `error` prose are unaffected (§6.5, monorepo#825).
fn classified(detail: &str) -> Option<CloneErrorCategory> {
    match classify_clone_error(detail) {
        CloneErrorCategory::Other => None,
        c => Some(c),
    }
}

/// Build the `git clone` command: argv, credential-helper config, and
/// environment. Factored out of [`run_clone`] so tests can assert the
/// secret-safety invariants directly — the token (when usable) is offered via
/// the shared [`intent_git::auth::scoped_credential_env`] builder: a
/// github.com-scoped helper carried in `GIT_CONFIG_PARAMETERS` (appended after
/// `inherited_config_parameters` so existing setups keep winning) whose config
/// string carries no token bytes, with the value travelling through
/// [`intent_git::auth::TOKEN_ENV`] only (monorepo#825, monorepo#884).
fn build_clone_command(
    url: &str,
    target_path: &Path,
    token: Option<&str>,
    inherited_config_parameters: Option<&str>,
    recurse_submodules: bool,
) -> Command {
    let mut cmd = Command::new("git");
    for (key, value) in intent_git::auth::scoped_credential_env(token, inherited_config_parameters)
    {
        cmd.env(key, value);
    }
    cmd.arg("clone").arg("--progress");
    if recurse_submodules {
        cmd.arg("--recurse-submodules");
    }
    cmd.arg(url)
        .arg(target_path)
        .env("GIT_LFS_SKIP_SMUDGE", GIT_LFS_SKIP_SMUDGE)
        .env("GIT_TERMINAL_PROMPT", "0");
    cmd
}

/// How [`run_clone`] emits its frames: the legacy standalone `git.clone`
/// framing (raw stderr percentages, terminal frames owned here), or a
/// `workspace.create` reporter (normalized percentages + `progressId`,
/// terminal frames owned by the create wrapper — see [`CloneJob::progress`]).
#[derive(Clone)]
enum ProgressSink {
    Legacy {
        bus: EventBus,
        ws: WorkspaceId,
        request_id: String,
    },
    Create(std::sync::Arc<crate::create_progress::CreateProgress>),
}

impl ProgressSink {
    async fn progress(&self, phase: &str, percent: u32, message: &str) {
        match self {
            ProgressSink::Legacy {
                bus,
                ws,
                request_id,
            } => {
                publish(
                    bus,
                    ws,
                    progress_event(ws, request_id, phase, percent, message, None),
                )
                .await;
            }
            ProgressSink::Create(reporter) => {
                reporter.clone_progress(phase, percent, message).await;
            }
        }
    }

    /// Legacy-only terminal done; the create wrapper owns the terminal frame
    /// when a reporter is active (exactly-one `git:clone:done` per create).
    async fn done(&self, ok: bool, error: Option<&str>, error_code: Option<CloneErrorCategory>) {
        if let ProgressSink::Legacy {
            bus,
            ws,
            request_id,
        } = self
        {
            publish(
                bus,
                ws,
                done_event(ws, request_id, ok, error, error_code, None),
            )
            .await;
        }
    }
}

async fn run_clone(job: CloneJob) -> std::result::Result<(), CloneFailure> {
    let CloneJob {
        request_id,
        workspace_id,
        url,
        target_path,
        token,
        bus,
        progress,
        recurse_submodules,
    } = job;
    let ws = workspace_id.unwrap_or_else(|| WorkspaceId::from_string(String::new()));
    let sink = match progress {
        Some(reporter) => ProgressSink::Create(reporter),
        None => ProgressSink::Legacy {
            bus,
            ws,
            request_id,
        },
    };

    // Initial "starting" frame (parity with the FE's first tick).
    sink.progress("starting", 0, "Starting clone...").await;

    // Ensure a target-parent exists so `git clone` doesn't fail on a fresh
    // workspace path. Not fatal if it already exists.
    if let Some(parent) = target_path.parent() {
        if !parent.as_os_str().is_empty() {
            let _ = tokio::fs::create_dir_all(parent).await;
        }
    }

    let inherited_config_parameters =
        std::env::var(intent_git::auth::GIT_CONFIG_PARAMETERS_ENV).ok();
    let mut cmd = build_clone_command(
        &url,
        &target_path,
        token.as_deref(),
        inherited_config_parameters.as_deref(),
        recurse_submodules,
    );
    cmd.stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    #[cfg(unix)]
    cmd.process_group(0);

    // Spawn / wait errors are deliberately NOT run through the stderr
    // classifier: "git spawn failed: No such file or directory" is an
    // environmental failure, not the user-fixable `path-invalid` the prose
    // would match. Both surfaces (the `git:clone:done` frame and the
    // JSON-RPC error) share the `None` decision made here.
    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => {
            let msg = format!("git spawn failed: {e}");
            sink.done(false, Some(&msg), None).await;
            return Err(CloneFailure {
                category: None,
                detail: msg,
            });
        }
    };
    let stderr = match child.stderr.take() {
        Some(s) => s,
        None => {
            let _ = child.kill().await;
            let msg = "git stderr not piped".to_string();
            sink.done(false, Some(&msg), None).await;
            return Err(CloneFailure {
                category: None,
                detail: msg,
            });
        }
    };

    let sink_reader = sink.clone();
    let reader_task = tokio::spawn(async move { stream_stderr(stderr, sink_reader).await });

    // Wait for the child under a hard timeout so a stalled clone never wedges
    // the daemon. On timeout, reap the process group and emit `ok:false`.
    let wait_result = tokio::time::timeout(CLONE_TIMEOUT, child.wait()).await;
    // Ensure the reader task drains any final stderr before we settle.
    let tail_error = reader_task.await.ok().flatten();

    match wait_result {
        Ok(Ok(status)) if status.success() => {
            sink.progress("complete", 100, "Clone complete!").await;
            sink.done(true, None, None).await;
            Ok(())
        }
        Ok(Ok(status)) => {
            let msg = match tail_error {
                Some(t) if !t.is_empty() => format!("git clone failed ({status}): {t}"),
                _ => format!("git clone failed ({status})"),
            };
            let redacted = redact_credentials(&msg);
            let category = classified(&redacted);
            sink.done(false, Some(&redacted), category).await;
            Err(CloneFailure {
                category,
                detail: redacted,
            })
        }
        Ok(Err(e)) => {
            let msg = format!("git wait failed: {e}");
            sink.done(false, Some(&msg), None).await;
            Err(CloneFailure {
                category: None,
                detail: msg,
            })
        }
        Err(_) => {
            reap_child_group(&mut child).await;
            // The daemon's own clone timeout is a `network`-category failure
            // per the clone failure taxonomy (PROTOCOL §9.1).
            let msg = "git clone timed out".to_string();
            let category = classified(&msg);
            sink.done(false, Some(&msg), category).await;
            Err(CloneFailure {
                category,
                detail: msg,
            })
        }
    }
}

/// Reap a timed-out clone's whole process group: SIGTERM → grace → SIGKILL,
/// then `wait()` to drain the zombie. Mirrors `host_exec::run`'s group-reap so
/// git's helper subprocesses (`git-remote-https`, `git-fetch-pack`,
/// `git-index-pack`, and any LFS helpers) cannot survive a clone timeout.
async fn reap_child_group(child: &mut tokio::process::Child) {
    #[cfg(unix)]
    {
        if let Some(pid) = child.id() {
            kill_group(pid, nix::sys::signal::Signal::SIGTERM);
            tokio::time::sleep(TERM_GRACE).await;
            if !matches!(child.try_wait(), Ok(Some(_))) {
                kill_group(pid, nix::sys::signal::Signal::SIGKILL);
            }
            let _ = child.wait().await;
            return;
        }
    }
    let _ = child.start_kill();
    let _ = child.wait().await;
}

/// Signal a whole process group by its leader pid (pgid == pid via
/// `process_group(0)` at spawn). Mirrors `host_exec::kill_group`.
#[cfg(unix)]
fn kill_group(pid: u32, sig: nix::sys::signal::Signal) {
    use nix::sys::signal::killpg;
    use nix::unistd::Pid;
    let _ = killpg(Pid::from_raw(pid as i32), sig);
}

/// Parse `git clone --progress` stderr line-by-line and publish one
/// `git:clone:progress` per matched phase transition. Returns the final chunk
/// of stderr text (best-effort) so a non-zero exit can surface a useful error.
async fn stream_stderr<R>(stderr: R, sink: ProgressSink) -> Option<String>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut reader = BufReader::new(stderr);
    let mut buf: Vec<u8> = Vec::with_capacity(256);
    let mut parser = SubmoduleAwareParser::for_clone();
    let mut last_frame: Option<(String, u32, String)> = None;
    let mut tail: String = String::new();
    loop {
        buf.clear();
        // Git emits carriage-returned progress; split on either \r or \n so we
        // observe each in-place update, not just terminal lines.
        let n = match read_until_any(&mut reader, b"\r\n", &mut buf).await {
            Ok(0) => break,
            Ok(n) => n,
            Err(_) => break,
        };
        let text = String::from_utf8_lossy(&buf[..n]);
        for (phase, percent, message) in parser.parse(&text) {
            // Message-aware dedup: submodule frames advance by message
            // ("Cloning submodules (N/M)") even when the aggregate percent
            // holds still, and per-phase percents never move backwards.
            let key = (phase.to_string(), percent, message.clone());
            let emit = match &last_frame {
                Some((lp, lpct, lmsg)) => *lp != key.0 || *lmsg != key.2 || percent > *lpct,
                None => true,
            };
            if emit {
                last_frame = Some(key);
                sink.progress(phase, percent, &message).await;
            }
        }
        // Keep a bounded tail (~4KiB) of stderr for error messages.
        tail.push_str(&text);
        trim_tail(&mut tail);
    }
    if tail.trim().is_empty() {
        None
    } else {
        Some(tail.trim().to_string())
    }
}

/// Drop the front of `tail` so at most ~[`STDERR_TAIL_MAX`] bytes remain,
/// cutting only at a line boundary: a byte-offset cut can split a URL's
/// scheme, leaving a bare `user:pass@host` fragment the `://`-anchored pass
/// of [`redact_credentials`] cannot find (monorepo#836). The partially-cut
/// line is dropped whole — up to the next `\n`/`\r`. When a single line
/// exceeds the cap the boundary cut would keep nothing but its trailing
/// delimiter (the reader includes `\n`/`\r` in each chunk), so any cut that
/// keeps only whitespace falls back to a char-boundary byte cut instead —
/// never dropping the message entirely; the scheme-less redaction pass
/// covers any token that byte cut can split.
fn trim_tail(tail: &mut String) {
    if tail.len() <= STDERR_TAIL_MAX {
        return;
    }
    let drop_to = tail.len() - STDERR_TAIL_MAX;
    let cut = tail.as_bytes()[drop_to..]
        .iter()
        .position(|&b| b == b'\n' || b == b'\r')
        .map(|i| drop_to + i + 1)
        .filter(|&cut| !tail[cut..].trim().is_empty())
        .unwrap_or_else(|| {
            let mut cut = drop_to;
            while !tail.is_char_boundary(cut) {
                cut += 1;
            }
            cut
        });
    tail.drain(..cut);
}

/// `read_until` but matching *any* byte in `delims`, mirroring `tokio`'s
/// single-byte helper. Needed because git progress uses `\r` for in-place
/// updates.
async fn read_until_any<R>(
    reader: &mut BufReader<R>,
    delims: &[u8],
    out: &mut Vec<u8>,
) -> std::io::Result<usize>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut total = 0usize;
    loop {
        let (done, used) = {
            let available = match reader.fill_buf().await {
                Ok(b) => b,
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(e),
            };
            if available.is_empty() {
                return Ok(total);
            }
            match available.iter().position(|b| delims.contains(b)) {
                Some(i) => {
                    out.extend_from_slice(&available[..=i]);
                    (true, i + 1)
                }
                None => {
                    out.extend_from_slice(available);
                    (false, available.len())
                }
            }
        };
        reader.consume(used);
        total += used;
        if done {
            return Ok(total);
        }
    }
}

/// The percent-bearing phase rules, ported from the FE's stderr regex table
/// (one canonical phase per line). [`SubmoduleAwareParser`] layers the
/// "Cloning into" boundary / registration handling on top. Static regex-lite
/// scanners keep the dep footprint at zero; each rule pushes
/// `(phase, percent, human_message)`. `Updating files:` is git ≥2.29's
/// spelling of the checkout phase.
fn scan_phase_percents(text: &str, out: &mut Vec<(&'static str, u32, String)>) {
    if text.contains("Counting objects") {
        out.push(("counting", 0, "Counting objects...".to_string()));
    }
    if let Some(pct) = percent_after(text, "Compressing objects:") {
        out.push(("compressing", pct, format!("Compressing objects: {pct}%")));
    }
    if let Some(pct) = percent_after(text, "Receiving objects:") {
        out.push(("receiving", pct, format!("Receiving objects: {pct}%")));
    }
    if let Some(pct) = percent_after(text, "Resolving deltas:") {
        out.push(("resolving", pct, format!("Resolving deltas: {pct}%")));
    }
    if let Some(pct) = percent_after(text, "Checking out files:") {
        out.push(("checkout", pct, format!("Checking out files: {pct}%")));
    }
    if let Some(pct) = percent_after(text, "Updating files:") {
        out.push(("checkout", pct, format!("Updating files: {pct}%")));
    }
}

/// Local-completion cap: a submodule's own phases top out at this share of
/// its slice, leaving headroom for submodules discovered later — nested
/// submodules register mid-flight, after earlier ones may already have
/// finished — so the aggregate percent never pins at 100 early (the
/// downstream reporter clamps monotonically and would otherwise freeze).
const SUBMODULE_LOCAL_CAP: u32 = 95;

/// Stateful, submodule-aware progress parser over git's `--progress` stderr.
///
/// A `git clone --recurse-submodules --progress` stream interleaves the
/// superproject clone with one nested clone per submodule, each announced by
/// its own `Cloning into '<path>'...` boundary and preceded by
/// `Submodule '<name>' (<url>) registered for path '<path>'` registrations.
/// This parser keeps the superproject phases as-is and folds everything after
/// the first boundary into one aggregated `submodules` phase whose percent is
/// distributed across the registered submodules ("Cloning submodules (N/M)").
///
/// The aggregate percent is allocated forward-only: each submodule gets a
/// slice of the *remaining* distance to 100 (sized by how many known
/// submodules are left), and its own clone phases fill the slice up to
/// [`SUBMODULE_LOCAL_CAP`]. The series is monotone by construction even when
/// the total grows mid-flight (nested registrations), so late submodules
/// always show forward movement instead of being pinned by the reporter's
/// monotonic clamp.
///
/// [`SubmoduleAwareParser::for_submodule_update`] parses a standalone
/// `git submodule update --init --recursive --force --progress` stream
/// instead: every frame is submodule-scoped, and `Submodule path '<p>':
/// checked out` completions drive an "Updating submodules (N)" counter for
/// already-cloned modules that print no clone phases (each completion
/// advances halfway through the remaining distance — the total is unknown).
///
/// Known limitation: in streamed cache runs, stdout ("checked out" lines)
/// and stderr ("Cloning into" boundaries) are drained by independent
/// threads, so cross-stream ordering into the parser is not guaranteed —
/// counts in the message can momentarily mis-sequence; percents stay
/// monotone regardless.
pub(crate) struct SubmoduleAwareParser {
    /// Every frame is submodule-scoped (no superproject clone in front).
    submodules_only: bool,
    /// The superproject's own "Cloning into" boundary has been consumed.
    seen_superproject: bool,
    /// Submodules registered so far ("registered for path" lines). Nested
    /// submodules register during their parent's clone, so this can still
    /// grow after cloning starts.
    registered: usize,
    /// Submodule clones started so far ("Cloning into" boundaries).
    started: usize,
    /// Submodules checked out so far ("Submodule path '…': checked out").
    checked_out: usize,
    /// Monotone floor of the aggregate percent: everything at or below is
    /// spoken for by finished (or superseded) submodules.
    base: u32,
    /// The in-flight submodule's `(lo, hi)` share of the aggregate percent.
    slice: Option<(u32, u32)>,
}

impl SubmoduleAwareParser {
    /// Parser for a full `git clone [--recurse-submodules] --progress` stream.
    pub(crate) fn for_clone() -> Self {
        Self::new(false)
    }

    /// Parser for a `git submodule update … --progress` stream.
    pub(crate) fn for_submodule_update() -> Self {
        Self::new(true)
    }

    fn new(submodules_only: bool) -> Self {
        Self {
            submodules_only,
            seen_superproject: false,
            registered: 0,
            started: 0,
            checked_out: 0,
            base: 0,
            slice: None,
        }
    }

    /// Whether frames are currently submodule-scoped.
    fn in_submodules(&self) -> bool {
        self.submodules_only || self.started > 0
    }

    /// The highest percent a slice may emit: its local cap point.
    fn capped_top((lo, hi): (u32, u32)) -> u32 {
        lo + (hi - lo) * SUBMODULE_LOCAL_CAP / 100
    }

    /// Open the next submodule's slice: seal any in-flight slice at its cap,
    /// then give the new one an equal share of the remaining distance to 100
    /// (counting known-but-unstarted registrations). Integer widths can
    /// collapse to zero near 100 — the slice then pins at `base`, still
    /// monotone.
    fn open_slice(&mut self) {
        if let Some(slice) = self.slice.take() {
            self.base = Self::capped_top(slice);
        }
        let remaining = (self.registered.saturating_sub(self.started) + 1) as u32;
        let width = (100 - self.base) / remaining;
        self.slice = Some((self.base, self.base + width));
    }

    /// Position `local` (a 0–100 weighted clone-phase percent) inside the
    /// in-flight slice, capped at [`SUBMODULE_LOCAL_CAP`] of it. With no
    /// slice open (phases arriving outside a boundary), hold at `base`.
    fn slice_percent(&self, local: u32) -> u32 {
        match self.slice {
            Some((lo, hi)) => lo + (hi - lo) * local.min(SUBMODULE_LOCAL_CAP) / 100,
            None => self.base,
        }
    }

    /// A module finished ("checked out" completion): seal the in-flight
    /// slice at its cap, or — with no slice open (already-cloned modules
    /// print no clone phases at all) — advance halfway through the remaining
    /// distance, since the total is unknown. Returns the new floor.
    fn complete_one(&mut self) -> u32 {
        self.base = match self.slice.take() {
            Some(slice) => Self::capped_top(slice),
            None => self.base + (100 - self.base) / 2,
        };
        self.base
    }

    /// Human message for clone boundaries and phases: "Cloning submodules
    /// (N)" or, when the registration count is known, "Cloning submodules
    /// (N/M)". Update-mode completions format "Updating submodules (N)"
    /// directly at the call site.
    fn submodule_message(&self) -> String {
        if self.registered > 0 {
            let total = self.registered.max(self.started);
            format!(
                "Cloning submodules ({}/{})",
                self.started.clamp(1, total),
                total
            )
        } else {
            format!("Cloning submodules ({})", self.started.max(1))
        }
    }

    /// Parse one stderr chunk (any mix of `\r`/`\n`-separated lines) into
    /// `(phase, percent, message)` frames.
    pub(crate) fn parse(&mut self, text: &str) -> Vec<(&'static str, u32, String)> {
        let mut out = Vec::new();
        for line in text.split(['\r', '\n']) {
            if !line.trim().is_empty() {
                self.parse_line(line, &mut out);
            }
        }
        out
    }

    fn parse_line(&mut self, line: &str, out: &mut Vec<(&'static str, u32, String)>) {
        if line.contains("registered for path") {
            self.registered += 1;
            return;
        }
        if line.contains("Cloning into") {
            if !self.submodules_only && !self.seen_superproject {
                self.seen_superproject = true;
                out.push(("starting", 0, "Cloning repository...".to_string()));
            } else {
                self.started += 1;
                self.open_slice();
                out.push((
                    "submodules",
                    self.slice_percent(0),
                    self.submodule_message(),
                ));
            }
            return;
        }
        // `git submodule update` prints one completion line per module
        // ("Submodule path 'sub': checked out '<sha>'"); count them so
        // already-cloned modules (no clone phases at all) still move the
        // percent and message forward.
        if self.submodules_only && line.contains("Submodule path") && line.contains("checked out") {
            self.checked_out += 1;
            let pct = self.complete_one();
            out.push((
                "submodules",
                pct,
                format!("Updating submodules ({})", self.checked_out),
            ));
            return;
        }
        let mut frames = Vec::new();
        scan_phase_percents(line, &mut frames);
        for (phase, pct, msg) in frames {
            if self.in_submodules() {
                // Weight the submodule's own clone phase into its slice of
                // the aggregate percent.
                let (lo, hi) = crate::create_progress::clone_phase_segment(phase);
                let local = crate::create_progress::map_segment(lo, hi, pct);
                out.push((
                    "submodules",
                    self.slice_percent(local),
                    self.submodule_message(),
                ));
            } else {
                out.push((phase, pct, msg));
            }
        }
    }
}

/// Return the integer percent immediately after `label` in `text`, e.g. for
/// "Receiving objects:  45% (1234/2743)" returns `Some(45)`. Whitespace between
/// the label and digits is skipped.
fn percent_after(text: &str, label: &str) -> Option<u32> {
    let idx = text.find(label)?;
    let rest = &text[idx + label.len()..];
    let mut chars = rest.char_indices().peekable();
    while let Some(&(_, c)) = chars.peek() {
        if c.is_whitespace() {
            chars.next();
        } else {
            break;
        }
    }
    let start = chars.peek().map(|(i, _)| *i)?;
    let mut end = start;
    for (i, c) in rest[start..].char_indices() {
        if c.is_ascii_digit() {
            end = start + i + c.len_utf8();
        } else {
            break;
        }
    }
    if end == start {
        return None;
    }
    rest[start..end].parse::<u32>().ok().map(|p| p.min(100))
}

/// Shared with the `workspace.create` cache-hydration arm in `lib.rs`, which
/// streams the same `git:clone:progress` / `git:clone:done` frames while
/// hydrating from the repo cache instead of a network clone. `progress_id`
/// is the client-supplied `workspace.create` correlation id (PROTOCOL §5.1):
/// present only on create-scoped frames whose request carried one — the key
/// is omitted entirely otherwise, so legacy consumers are unaffected.
pub(crate) fn progress_event(
    workspace_id: &WorkspaceId,
    request_id: &str,
    phase: &str,
    percent: u32,
    message: &str,
    progress_id: Option<&str>,
) -> NewEvent {
    let mut data = json!({
        "requestId": request_id,
        "phase": phase,
        "percent": percent,
        "message": message,
    });
    if let Some(pid) = progress_id {
        data.as_object_mut()
            .unwrap()
            .insert("progressId".to_string(), json!(pid));
    }
    NewEvent {
        workspace_id: workspace_id.clone(),
        timestamp: now_iso(),
        event_type: GIT_CLONE_PROGRESS.to_string(),
        actor: system_actor(),
        session_id: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data,
    }
}

pub(crate) fn done_event(
    workspace_id: &WorkspaceId,
    request_id: &str,
    ok: bool,
    error: Option<&str>,
    error_code: Option<CloneErrorCategory>,
    progress_id: Option<&str>,
) -> NewEvent {
    let mut data = json!({ "requestId": request_id, "ok": ok });
    if let Some(err) = error {
        data.as_object_mut()
            .unwrap()
            .insert("error".to_string(), json!(err));
    }
    if let Some(pid) = progress_id {
        data.as_object_mut()
            .unwrap()
            .insert("progressId".to_string(), json!(pid));
    }
    // Machine-readable failure category (monorepo#825/#826): present only on
    // classified failures so existing consumers of the `error` prose are
    // unaffected.
    if let Some(code) = error_code {
        data.as_object_mut()
            .unwrap()
            .insert("errorCode".to_string(), json!(code.as_str()));
    }
    NewEvent {
        workspace_id: workspace_id.clone(),
        timestamp: now_iso(),
        event_type: GIT_CLONE_DONE.to_string(),
        actor: system_actor(),
        session_id: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data,
    }
}

pub(crate) async fn publish(bus: &EventBus, _ws: &WorkspaceId, event: NewEvent) {
    if let Err(e) = bus.publish(&event).await {
        tracing::warn!(error = %e, "failed to publish git:clone event");
    }
}

/// Read-only helper for the caller: whether `path` already exists on disk.
/// Kept here so `Services::git_clone` can early-reject a non-empty target
/// without duplicating path logic.
pub(crate) fn target_exists(target_path: &Path) -> bool {
    target_path.exists()
}

/// Whether `path` exists as something a clone cannot target: a file, or a
/// directory with at least one entry. An existing *empty* directory is fine —
/// `git clone` accepts it — so `workspace.create` only rejects when this
/// returns true (`destination-exists-non-empty`, monorepo#826).
pub(crate) fn target_exists_non_empty(target_path: &Path) -> bool {
    if !target_path.exists() {
        return false;
    }
    match std::fs::read_dir(target_path) {
        Ok(mut entries) => entries.next().is_some(),
        // Not a directory (or unreadable): the clone cannot use it either way.
        Err(_) => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derive_default_target_strips_dot_git() {
        assert_eq!(derive_default_target("https://github.com/a/b.git"), "b");
        assert_eq!(derive_default_target("https://github.com/a/b"), "b");
        assert_eq!(derive_default_target("git@github.com:a/b.git"), "b");
        assert_eq!(derive_default_target("https://github.com/a/b/"), "b");
    }

    #[test]
    fn parse_owner_repo_handles_https_and_ssh() {
        assert_eq!(
            parse_owner_repo("https://github.com/owner/repo.git"),
            Some(("owner".to_string(), "repo".to_string()))
        );
        assert_eq!(
            parse_owner_repo("https://github.com/owner/repo"),
            Some(("owner".to_string(), "repo".to_string()))
        );
        assert_eq!(
            parse_owner_repo("git@github.com:owner/repo.git"),
            Some(("owner".to_string(), "repo".to_string()))
        );
        assert_eq!(parse_owner_repo("https://github.com/repo"), None);
        assert_eq!(parse_owner_repo(""), None);
    }

    #[test]
    fn resolve_target_rejects_traversal() {
        assert!(resolve_target_path("/tmp", "https://x/y.git", Some("../evil")).is_err());
        assert!(resolve_target_path("/tmp", "https://x/y.git", Some("a/b")).is_err());
        assert!(resolve_target_path("/tmp", "https://x/y.git", Some("")).is_err());
    }

    #[test]
    fn resolve_target_uses_default_when_missing() {
        let p = resolve_target_path("/tmp", "https://github.com/a/b.git", None).unwrap();
        assert_eq!(p, PathBuf::from("/tmp/b"));
    }

    #[test]
    fn resolve_target_expands_tilde_parent() {
        // Regression for intent-hq/monorepo#822: a leading `~/` in `parentDir`
        // must resolve under `$HOME`, never reach git as a literal `./~` path.
        let Some(home) = std::env::var_os("HOME").map(PathBuf::from) else {
            eprintln!("skipping tilde expansion test: HOME not set");
            return;
        };
        let p = resolve_target_path("~/clones", "https://x/y.git", Some("repo")).unwrap();
        assert_eq!(p, home.join("clones").join("repo"));
        let p = resolve_target_path("~", "https://x/y.git", Some("repo")).unwrap();
        assert_eq!(p, home.join("repo"));
        // `~user` forms pass through unchanged.
        let p = resolve_target_path("~alice/clones", "https://x/y.git", Some("repo")).unwrap();
        assert_eq!(p, PathBuf::from("~alice/clones/repo"));
    }

    #[test]
    fn redact_credentials_masks_user_pass() {
        let input = "fatal: unable to access 'https://user:secret@host/x.git': timed out";
        let out = redact_credentials(input);
        assert!(!out.contains("user:secret"));
        assert!(out.contains("***@host"));
    }

    #[test]
    fn redact_credentials_passthrough_when_none() {
        let input = "fatal: repository not found";
        assert_eq!(redact_credentials(input), input);
    }

    #[test]
    fn redact_credentials_masks_front_truncated_scheme() {
        // Regression for monorepo#836: a front-truncated tail can cut inside
        // `https://`, leaving no `://` anchor for the authority pass.
        let input = "/user:secret@host/x.git': timed out";
        let out = redact_credentials(input);
        assert!(!out.contains("secret"), "{out}");
        assert!(out.contains("/***@host/x.git"), "{out}");
    }

    #[test]
    fn redact_credentials_masks_bare_userinfo() {
        let out = redact_credentials("fatal: unable to access 'user:secret@host/x.git'");
        assert!(!out.contains("secret"), "{out}");
        assert!(out.contains("'***@host/x.git'"), "{out}");

        // Token-as-username carries no `:pass` and must still be masked.
        let out = redact_credentials("fetch of ghp_abc123@github.com failed");
        assert!(!out.contains("ghp_abc123"), "{out}");
        assert_eq!(out, "fetch of ***@github.com failed");
    }

    #[test]
    fn redact_credentials_scp_like_masking_keeps_classification() {
        use intent_core::CloneErrorCategory as C;
        // The scheme-less pass is over-eager by design: `git@` is masked too,
        // but the auth marker survives for classify_clone_error.
        let out = redact_credentials("git@github.com: Permission denied (publickey).");
        assert_eq!(out, "***@github.com: Permission denied (publickey).");
        assert_eq!(classify_clone_error(&out), C::AuthRequired);
    }

    #[test]
    fn trim_tail_drops_partially_cut_line_whole() {
        // Arrange the cut to land mid-URL on the first line: the whole line
        // must be dropped, never leaving a credential fragment (monorepo#836).
        let secret_line = "fatal: 'https://user:secret@host/x.git': fail\n";
        let mut tail = format!("{secret_line}{}\n", "x".repeat(STDERR_TAIL_MAX - 26));
        assert!(tail.len() > STDERR_TAIL_MAX);
        trim_tail(&mut tail);
        assert!(tail.len() <= STDERR_TAIL_MAX);
        assert!(!tail.contains("secret"), "{tail}");
        assert!(tail.starts_with('x'));
    }

    #[test]
    fn trim_tail_bounds_and_noop_under_cap() {
        let line = format!("{}\n", "y".repeat(99));
        let mut tail = line.repeat(45); // 4500 bytes, cut lands mid-line
        trim_tail(&mut tail);
        assert!(tail.len() <= STDERR_TAIL_MAX);
        assert_eq!(tail.len() % 100, 0, "only whole lines survive");

        let mut small = String::from("short line\n");
        trim_tail(&mut small);
        assert_eq!(small, "short line\n");
    }

    #[test]
    fn trim_tail_oversized_single_line_falls_back_to_byte_cut() {
        // One giant line with no boundary: keep the last ~4KiB rather than
        // dropping the message; multi-byte chars never split (no panic).
        // 6000 bytes of 3-byte chars: drop_to = 1904, 1904 % 3 == 2 → the
        // raw offset lands mid-char and the boundary-advance loop must run.
        let mut tail = "€".repeat(2000);
        trim_tail(&mut tail);
        assert!(tail.len() <= STDERR_TAIL_MAX);
        assert!(!tail.is_empty());
        assert!(tail.chars().all(|c| c == '€'));
    }

    #[test]
    fn trim_tail_keeps_newline_terminated_oversized_line() {
        // Regression: `read_until_any` includes the delimiter, so a single
        // oversized line usually ends with `\n` — the boundary cut would
        // keep only that whitespace and drop the message entirely.
        let mut tail = format!("fatal: {}\n", "z".repeat(5000));
        trim_tail(&mut tail);
        assert!(tail.len() <= STDERR_TAIL_MAX);
        assert!(!tail.trim().is_empty(), "message must survive: {tail:?}");
        assert!(tail.trim_end().chars().all(|c| c == 'z'));
        assert!(tail.ends_with('\n'));
    }

    #[test]
    fn classify_clone_error_auth() {
        use intent_core::CloneErrorCategory as C;
        for msg in [
            "fatal: Authentication failed for 'https://github.com/a/b.git/'",
            "fatal: could not read Username for 'https://github.com': terminal prompts disabled",
            "git@github.com: Permission denied (publickey).",
            "git@host: Permission denied (publickey,password).",
            "git@host: Permission denied (publickey,gssapi-keyex,gssapi-with-mic).",
            "remote: Invalid username or password.",
        ] {
            assert_eq!(classify_clone_error(msg), C::AuthRequired, "msg: {msg}");
        }
    }

    /// Askpass exec failures outrank auth-required even though git's stderr
    /// carries the auth prose alongside them: the helper script is
    /// missing/unreachable (e.g. macOS quarantine relocated the app bundle),
    /// so the remedy is local — not credentials (monorepo#837).
    #[test]
    fn classify_clone_error_askpass_missing() {
        use intent_core::CloneErrorCategory as C;
        for msg in [
            "fatal: cannot exec '/Users/x/Downloads/Intent.app/Contents/Resources/app.asar/resources/bin/ssh-askpass-intent.sh': Not a directory\nfatal: could not read Username for 'https://github.com': terminal prompts disabled",
            "fatal: cannot exec ssh-askpass-intent.sh\nfatal: could not read Username for 'https://github.com': terminal prompts disabled",
            "fatal: cannot exec '/usr/local/bin/my-askpass': No such file or directory\nfatal: could not read Password for 'https://github.com': terminal prompts disabled",
            "sh: /Applications/Intent.app/Contents/Resources/app.asar/resources/bin/helper.sh: Not a directory",
        ] {
            assert_eq!(classify_clone_error(msg), C::AskpassMissing, "msg: {msg}");
        }
        // Plain auth prose without any askpass signal stays auth-required.
        assert_eq!(
            classify_clone_error(
                "fatal: could not read Username for 'https://github.com': terminal prompts disabled"
            ),
            C::AuthRequired
        );
    }

    #[test]
    fn classify_clone_error_network() {
        use intent_core::CloneErrorCategory as C;
        for msg in [
            "fatal: unable to access 'https://github.com/a/b.git/': Could not resolve host: github.com",
            "fatal: unable to access 'https://github.com/a/b.git/': Failed to connect to github.com port 443: Connection refused",
            "error: RPC failed; curl 56 Recv failure: Connection reset by peer",
            "git clone timed out",
            "fatal: the remote end hung up unexpectedly",
        ] {
            assert_eq!(classify_clone_error(msg), C::Network, "msg: {msg}");
        }
    }

    #[test]
    fn target_exists_non_empty_semantics() {
        let base = std::env::temp_dir().join(format!(
            "clone-target-check-{}",
            uuid::Uuid::new_v4().simple()
        ));
        std::fs::create_dir_all(&base).unwrap();

        assert!(!target_exists_non_empty(&base.join("missing")));

        let empty = base.join("empty");
        std::fs::create_dir(&empty).unwrap();
        assert!(!target_exists_non_empty(&empty));

        let occupied = base.join("occupied");
        std::fs::create_dir(&occupied).unwrap();
        std::fs::write(occupied.join("keep.txt"), "x").unwrap();
        assert!(target_exists_non_empty(&occupied));

        let file = base.join("plain-file");
        std::fs::write(&file, "x").unwrap();
        assert!(target_exists_non_empty(&file));

        let _ = std::fs::remove_dir_all(&base);
    }

    #[test]
    fn classify_clone_error_destination_and_path() {
        use intent_core::CloneErrorCategory as C;
        assert_eq!(
            classify_clone_error(
                "fatal: destination path 'x' already exists and is not an empty directory."
            ),
            C::DestinationExistsNonEmpty
        );
        assert_eq!(
            classify_clone_error("fatal: could not create work tree dir 'x': Permission denied"),
            C::PathInvalid
        );
        assert_eq!(
            classify_clone_error("fatal: could not create directory '/nope/x'"),
            C::PathInvalid
        );
    }

    /// Git-stderr path failure shapes classify as `path-invalid` (remaining
    /// #826/#562 slice): git's `die_errno` destination messages carry a
    /// varying errno suffix, so the prefixes match errno-agnostically and
    /// the common errno spellings match on their own.
    #[test]
    fn classify_clone_error_git_stderr_path_shapes() {
        use intent_core::CloneErrorCategory as C;
        for msg in [
            // The #826 symptom shape, plus errno variants not covered by
            // the bare errno rows.
            "fatal: could not create leading directories of '/x/repo': Read-only file system",
            "fatal: could not create leading directories of '/tmp/f/repo': Not a directory",
            "fatal: could not create leading directories of '/x/repo': File name too long",
            // ENOENT-style path error.
            "fatal: could not create work tree dir 'repo': No such file or directory",
            // Local filesystem denial on the destination.
            "fatal: could not create work tree dir '/usr/local/repo': Permission denied",
        ] {
            assert_eq!(classify_clone_error(msg), C::PathInvalid, "msg: {msg}");
        }
        // Precedence guard: SSH auth denials stay `auth-required` despite the
        // broadened "permission denied" / path rows.
        assert_eq!(
            classify_clone_error("git@host: Permission denied (publickey,password)."),
            C::AuthRequired
        );
        // Checkout-time `invalid path` is repository-content-caused (git's
        // verify_path rejecting a tracked filename), not a destination
        // failure — §9.1's "correct the path and retry" doesn't apply, so it
        // stays on the clone-failed catch-all.
        assert_eq!(
            classify_clone_error("error: invalid path 'aux/config'"),
            C::Other
        );
        // The §9.1 `askpass-missing` shape (monorepo#837): its ENOTDIR
        // spelling previously rode this arm's "not a directory" row; the
        // `AskpassMissing` arm is ordered ahead of both the auth arm and
        // this path arm, so it now classifies askpass-missing.
        assert_eq!(
            classify_clone_error(
                "error: cannot run /Applications/X.app/Contents/Resources/app.asar/bin/ssh-askpass-intent: Not a directory"
            ),
            C::AskpassMissing
        );
    }

    #[test]
    fn classify_clone_error_fallback_other() {
        use intent_core::CloneErrorCategory as C;
        assert_eq!(
            classify_clone_error("fatal: repository 'https://github.com/a/b.git/' not found"),
            C::Other
        );
        assert_eq!(classify_clone_error(""), C::Other);
    }

    /// The additive `repo-not-found` / `access-denied` categories
    /// (monorepo#825): per the agreed taxonomy, GitHub's
    /// "Repository not found" is `repo-not-found` — NOT `auth-required` —
    /// even though GitHub answers it for private repos the presented token
    /// cannot see. Auth-shaped prose still wins when both appear.
    #[test]
    fn classify_clone_error_repo_not_found_and_access_denied() {
        use intent_core::CloneErrorCategory as C;
        for msg in [
            "remote: Repository not found.",
            "fatal: repository 'https://github.com/a/b.git/' not found. remote: Repository not found.",
            "The requested URL returned error: 404",
            "fatal: remote error: 404: Not Found",
        ] {
            assert_eq!(classify_clone_error(msg), C::RepoNotFound, "msg: {msg}");
        }
        for msg in [
            "The requested URL returned error: 403",
            "remote: Access denied to repository.",
        ] {
            assert_eq!(classify_clone_error(msg), C::AccessDenied, "msg: {msg}");
        }
        // Auth prose outranks the access-denied row when both appear.
        assert_eq!(
            classify_clone_error("remote: HTTP Basic: Access denied. Authentication failed"),
            C::AuthRequired
        );
        // GitLab's credential rejection carries no "authentication failed"
        // prose but is still an auth failure — the remedy is credentials.
        assert_eq!(
            classify_clone_error(
                "remote: HTTP Basic: Access denied. The provided password or token \
                 is incorrect or your account has 2FA enabled"
            ),
            C::AuthRequired
        );
        // Wire spellings match the clone failure taxonomy (PROTOCOL §9.1).
        assert_eq!(C::RepoNotFound.as_str(), "repo-not-found");
        assert_eq!(C::AccessDenied.as_str(), "access-denied");
    }

    /// Regression for monorepo#825: a usable token is offered to the clone
    /// child via the env-backed github.com-scoped credential helper — the
    /// token bytes never appear in argv (process listings / logs), only in
    /// the child environment under `TOKEN_ENV`, with the helper config
    /// carried by `GIT_CONFIG_PARAMETERS` (monorepo#884).
    #[test]
    fn build_clone_command_injects_token_via_env_not_argv() {
        let token = "ghp_secret1234567890";
        let cmd = build_clone_command(
            "https://github.com/acme/private.git",
            Path::new("/tmp/x"),
            Some(token),
            None,
            false,
        );
        let std_cmd = cmd.as_std();
        let args: Vec<String> = std_cmd
            .get_args()
            .map(|a| a.to_string_lossy().to_string())
            .collect();
        assert!(
            args.iter().all(|a| !a.contains(token)),
            "token must never appear in argv: {args:?}"
        );
        assert!(
            args.iter().all(|a| !a.contains("credential.")),
            "helper config travels via the environment, not argv: {args:?}"
        );
        let env: Vec<(String, Option<String>)> = std_cmd
            .get_envs()
            .map(|(k, v)| {
                (
                    k.to_string_lossy().to_string(),
                    v.map(|v| v.to_string_lossy().to_string()),
                )
            })
            .collect();
        let params = env
            .iter()
            .find(|(k, _)| k == intent_git::auth::GIT_CONFIG_PARAMETERS_ENV)
            .and_then(|(_, v)| v.clone())
            .expect("GIT_CONFIG_PARAMETERS must carry the helper config");
        assert!(
            params.contains("credential.https://github.com.helper="),
            "github.com-scoped helper present: {params}"
        );
        assert!(
            !params.contains(token),
            "helper config must not embed the token"
        );
        assert!(
            env.iter()
                .any(|(k, v)| k == intent_git::auth::TOKEN_ENV && v.as_deref() == Some(token)),
            "token travels via {} only",
            intent_git::auth::TOKEN_ENV
        );
        assert!(
            env.iter()
                .any(|(k, v)| k == "GIT_TERMINAL_PROMPT" && v.as_deref() == Some("0")),
            "terminal prompts stay disabled"
        );
    }

    /// An inherited `GIT_CONFIG_PARAMETERS` value survives token injection:
    /// the helper entry is appended after it, never replacing it.
    #[test]
    fn build_clone_command_appends_helper_to_inherited_parameters() {
        let cmd = build_clone_command(
            "https://github.com/acme/private.git",
            Path::new("/tmp/x"),
            Some("tok"),
            Some("'foo.bar=baz'"),
            false,
        );
        let params = cmd
            .as_std()
            .get_envs()
            .find(|(k, _)| k.to_string_lossy() == intent_git::auth::GIT_CONFIG_PARAMETERS_ENV)
            .and_then(|(_, v)| v.map(|v| v.to_string_lossy().to_string()))
            .expect("GIT_CONFIG_PARAMETERS must be set");
        assert!(
            params.starts_with("'foo.bar=baz' "),
            "inherited entries keep precedence: {params}"
        );
        assert!(params.contains("credential.https://github.com.helper="));
    }

    /// No token (or an unusable one) leaves the command untouched: no helper
    /// config anywhere, no token env var, no `GIT_CONFIG_PARAMETERS`
    /// override (any inherited value passes through untouched).
    #[test]
    fn build_clone_command_without_token_adds_no_helper() {
        for token in [None, Some(""), Some("  "), Some("bad\ntoken")] {
            let cmd = build_clone_command(
                "https://github.com/acme/repo.git",
                Path::new("/tmp/x"),
                token,
                None,
                false,
            );
            let std_cmd = cmd.as_std();
            let args: Vec<String> = std_cmd
                .get_args()
                .map(|a| a.to_string_lossy().to_string())
                .collect();
            assert!(
                args.iter().all(|a| !a.contains("credential.")),
                "no helper config without a usable token: {args:?}"
            );
            assert!(
                std_cmd.get_envs().all(|(k, _)| {
                    let k = k.to_string_lossy();
                    k != intent_git::auth::TOKEN_ENV
                        && k != intent_git::auth::GIT_CONFIG_PARAMETERS_ENV
                }),
                "no token / config-parameters env vars without a usable token"
            );
        }
    }

    /// `git:clone:done` carries `errorCode` only for classified failures —
    /// success and unclassified (`Other`) frames omit the key entirely
    /// (§6.5).
    #[test]
    fn done_event_carries_error_code_only_when_classified() {
        use intent_core::CloneErrorCategory as C;
        let ws = WorkspaceId::from_string(String::new());
        let ev = done_event(
            &ws,
            "r1",
            false,
            Some("terminal prompts disabled"),
            classified("terminal prompts disabled"),
            None,
        );
        assert_eq!(ev.data["errorCode"], json!("auth-required"));
        let ev = done_event(
            &ws,
            "r2",
            false,
            Some("weird failure"),
            classified("weird failure"),
            None,
        );
        assert!(ev.data.get("errorCode").is_none());
        let ev = done_event(&ws, "r3", true, None, None, None);
        assert!(ev.data.get("errorCode").is_none());
        // The daemon's own timeout message classifies as `network`.
        assert_eq!(classified("git clone timed out"), Some(C::Network));
    }

    #[test]
    fn parse_progress_matches_phases() {
        let mut parser = SubmoduleAwareParser::for_clone();
        let ph = parser.parse("Receiving objects:  45% (10/22)");
        assert_eq!(ph.len(), 1);
        assert_eq!(ph[0].0, "receiving");
        assert_eq!(ph[0].1, 45);
        let ch = parser.parse("Checking out files: 100% (1/1), done.");
        assert_eq!(ch[0].0, "checkout");
        assert_eq!(ch[0].1, 100);
        // git ≥2.29 spells the checkout phase "Updating files:".
        let uf = parser.parse("Updating files:  60% (3/5)");
        assert_eq!(uf[0].0, "checkout");
        assert_eq!(uf[0].1, 60);
    }

    /// A recursive clone stream: superproject phases pass through untouched;
    /// everything after the first submodule "Cloning into" boundary becomes
    /// the aggregated `submodules` phase, with the registration count carried
    /// in the message and the percent distributed across submodule slices.
    #[test]
    fn submodule_aware_parser_detects_boundaries() {
        let mut parser = SubmoduleAwareParser::for_clone();
        // Superproject boundary + phases.
        let f = parser.parse("Cloning into 'repo'...");
        assert_eq!(
            f,
            vec![("starting", 0, "Cloning repository...".to_string())]
        );
        let f = parser.parse("Receiving objects:  50% (5/10)");
        assert_eq!(f[0].0, "receiving");
        assert_eq!(f[0].1, 50);
        // Registrations announce M before any submodule clone starts.
        assert!(parser
            .parse("Submodule 'liba' (https://x/liba.git) registered for path 'liba'")
            .is_empty());
        assert!(parser
            .parse("Submodule 'libb' (https://x/libb.git) registered for path 'libb'")
            .is_empty());
        // First submodule boundary: N/M in the message, percent at its
        // slice's start. Two known submodules split the remaining 0..100
        // evenly: slice 1 is 0..50.
        let f = parser.parse("Cloning into '/tmp/repo/liba'...");
        assert_eq!(f.len(), 1);
        assert_eq!(f[0].0, "submodules");
        assert_eq!(f[0].1, 0);
        assert_eq!(f[0].2, "Cloning submodules (1/2)");
        // Its clone phases stay inside slice 1: a finished checkout tops the
        // slice at its 95% local cap → 47.
        let f = parser.parse("Checking out files: 100% (4/4), done.");
        assert_eq!(f[0].0, "submodules");
        assert_eq!(f[0].1, 47);
        assert_eq!(f[0].2, "Cloning submodules (1/2)");
        // Second boundary seals slice 1 and opens slice 2 (47..100).
        let f = parser.parse("Cloning into '/tmp/repo/libb'...");
        assert_eq!(f[0].1, 47);
        assert_eq!(f[0].2, "Cloning submodules (2/2)");
        let f = parser.parse("Receiving objects: 100% (8/8), done.");
        assert_eq!(f[0].0, "submodules");
        // receiving tops at 70 of the local weight → 47 + 53*70/100 = 84.
        assert_eq!(f[0].1, 84);
    }

    /// Multi-line chunks parse line-by-line: registrations and a boundary in
    /// one chunk yield the boundary frame with the right count.
    #[test]
    fn submodule_aware_parser_handles_multiline_chunks() {
        let mut parser = SubmoduleAwareParser::for_clone();
        let chunk = "Cloning into 'repo'...\n\
                     Submodule 'a' (https://x/a.git) registered for path 'a'\n\
                     Cloning into '/tmp/repo/a'...\n";
        let frames = parser.parse(chunk);
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].0, "starting");
        assert_eq!(frames[1].0, "submodules");
        assert_eq!(frames[1].2, "Cloning submodules (1/1)");
    }

    /// Submodule-update mode: no superproject boundary — every frame is
    /// submodule-scoped, and "checked out" completions advance the percent
    /// (halfway through the remaining distance) and message for modules that
    /// stream no clone phases at all.
    #[test]
    fn submodule_aware_parser_update_mode() {
        let mut parser = SubmoduleAwareParser::for_submodule_update();
        let f = parser.parse("Cloning into '/ws/repo/liba'...");
        assert_eq!(f[0].0, "submodules");
        assert_eq!(f[0].2, "Cloning submodules (1)");
        let f = parser.parse("Submodule path 'liba': checked out 'abc123'");
        assert_eq!(f[0].0, "submodules");
        assert_eq!(f[0].2, "Updating submodules (1)");
        let first = f[0].1;
        // A pre-cloned module with no clone boundary of its own still moves
        // the percent forward.
        let f = parser.parse("Submodule path 'libb': checked out 'def456'");
        assert_eq!(f[0].2, "Updating submodules (2)");
        assert!(f[0].1 > first, "{} !> {first}", f[0].1);
        assert!(f[0].1 < 100);
    }

    /// Nested submodules can register mid-flight (during their parent's
    /// clone); the total in the message grows and the aggregate percent stays
    /// non-decreasing — the parent's finished slice is sealed below 100, so
    /// late modules still show forward movement.
    #[test]
    fn submodule_aware_parser_nested_registration() {
        let mut parser = SubmoduleAwareParser::for_clone();
        parser.parse("Cloning into 'repo'...");
        parser.parse("Submodule 'a' (https://x/a.git) registered for path 'a'");
        let mut last = 0;
        let mut check = |frames: Vec<(&'static str, u32, String)>| -> (u32, String) {
            let (_, pct, msg) = frames.into_iter().next().expect("one frame");
            assert!(pct >= last, "{pct} regressed below {last}");
            last = pct;
            (pct, msg)
        };
        let (_, msg) = check(parser.parse("Cloning into '/tmp/repo/a'..."));
        assert_eq!(msg, "Cloning submodules (1/1)");
        // `a` finishes its own clone phases before the nested module is known:
        // the local cap keeps its slice sealed below 100.
        let (pct, _) = check(parser.parse("Checking out files: 100% (2/2), done."));
        assert!(pct < 100);
        // A nested submodule registers while `a` clones, then starts — the
        // denominator grows and its frames keep advancing past the parent's.
        parser.parse("Submodule 'inner' (https://x/i.git) registered for path 'a/inner'");
        let (_, msg) = check(parser.parse("Cloning into '/tmp/repo/a/inner'..."));
        assert_eq!(msg, "Cloning submodules (2/2)");
        let (start, _) = check(parser.parse("Receiving objects:  50% (1/2)"));
        let (end, _) = check(parser.parse("Resolving deltas: 100% (3/3), done."));
        assert!(end > start, "nested module shows forward movement");
        assert!(end <= 100);
    }

    /// Spawn a shell that forks a `sleep 30` grandchild in the same process
    /// group, then reap the parent — the grandchild pid must be gone after the
    /// reap. This mirrors what happens on a `git clone` timeout: git spawns
    /// helpers (`git-remote-https`, `git-fetch-pack`, `git-index-pack`) that we
    /// need to reap alongside the direct child. Using `killpg` (via
    /// `reap_child_group`) is what makes that possible; a bare `start_kill()`
    /// on the direct child would leave the grandchild orphaned.
    #[cfg(unix)]
    #[tokio::test]
    async fn reap_child_group_kills_grandchildren() {
        use std::process::Stdio;
        use tokio::io::AsyncReadExt;
        use tokio::process::Command;

        let mut cmd = Command::new("sh");
        cmd.arg("-c")
            .arg("sleep 30 & echo $! ; wait")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        cmd.process_group(0);
        let mut child = cmd.spawn().expect("spawn sh");

        let mut stdout = child.stdout.take().expect("piped stdout");
        let mut line = String::new();
        tokio::time::timeout(Duration::from_secs(2), async {
            let mut byte = [0u8; 1];
            loop {
                let n = stdout.read(&mut byte).await.unwrap_or(0);
                if n == 0 || byte[0] == b'\n' {
                    break;
                }
                line.push(byte[0] as char);
            }
        })
        .await
        .expect("read grandchild pid");
        let grandchild_pid: i32 = line.trim().parse().expect("parse grandchild pid");

        reap_child_group(&mut child).await;

        // Give the kernel a moment to deliver signals and reap zombies. The
        // grandchild is not our direct child, so it stays until init reaps it;
        // `kill(pid, 0)` returns ESRCH once the pid is gone.
        for _ in 0..20 {
            if nix::sys::signal::kill(nix::unistd::Pid::from_raw(grandchild_pid), None).is_err() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("grandchild pid {grandchild_pid} still alive after group-reap");
    }
}
