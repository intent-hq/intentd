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
/// mirroring `host_exec`'s TERM_GRACE so the whole process group (git-remote-
/// https / git-fetch-pack / git-index-pack) settles before we escalate.
const TERM_GRACE: Duration = Duration::from_millis(500);

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
/// `text`. Best-effort; used only for the terminal `error` payload.
fn redact_credentials(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(scheme_end) = rest.find("://") {
        out.push_str(&rest[..scheme_end + 3]);
        rest = &rest[scheme_end + 3..];
        let end_authority = rest.find(['/', ' ', '\t', '\n']).unwrap_or(rest.len());
        let authority = &rest[..end_authority];
        if let Some(at) = authority.rfind('@') {
            out.push_str("***@");
            out.push_str(&authority[at + 1..]);
        } else {
            out.push_str(authority);
        }
        rest = &rest[end_authority..];
    }
    out.push_str(rest);
    out
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
    if m.contains("is not a valid path")
        || m.contains("could not create work tree")
        || m.contains("could not create directory")
        || m.contains("permission denied")
        || m.contains("read-only file system")
        || m.contains("no such file or directory")
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
/// a github.com-scoped `-c credential.…helper` whose config string carries no
/// token bytes, with the value travelling through [`intent_git::auth::TOKEN_ENV`]
/// only (monorepo#825).
fn build_clone_command(url: &str, target_path: &Path, token: Option<&str>) -> Command {
    let mut cmd = Command::new("git");
    // Offer the resolved token as an extra credential helper scoped to
    // github.com HTTPS only, appended after any configured helpers so an
    // existing credential setup still wins (same chain as
    // `intent_git::fetch`). The helper reads the secret from the environment
    // — the argv below carries no token bytes.
    if let Some(token) = intent_git::auth::usable_token(token) {
        cmd.arg("-c").arg(intent_git::auth::token_helper_config());
        cmd.env(intent_git::auth::TOKEN_ENV, token);
    }
    cmd.arg("clone")
        .arg("--progress")
        .arg(url)
        .arg(target_path)
        .env("GIT_LFS_SKIP_SMUDGE", GIT_LFS_SKIP_SMUDGE)
        .env("GIT_TERMINAL_PROMPT", "0");
    cmd
}

async fn run_clone(job: CloneJob) -> std::result::Result<(), CloneFailure> {
    let CloneJob {
        request_id,
        workspace_id,
        url,
        target_path,
        token,
        bus,
    } = job;
    let ws = workspace_id.unwrap_or_else(|| WorkspaceId::from_string(String::new()));

    // Initial "starting" frame (parity with the FE's first tick).
    publish(
        &bus,
        &ws,
        progress_event(&ws, &request_id, "starting", 0, "Starting clone..."),
    )
    .await;

    // Ensure a target-parent exists so `git clone` doesn't fail on a fresh
    // workspace path. Not fatal if it already exists.
    if let Some(parent) = target_path.parent() {
        if !parent.as_os_str().is_empty() {
            let _ = tokio::fs::create_dir_all(parent).await;
        }
    }

    let mut cmd = build_clone_command(&url, &target_path, token.as_deref());
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
            publish(
                &bus,
                &ws,
                done_event(&ws, &request_id, false, Some(&msg), None),
            )
            .await;
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
            publish(
                &bus,
                &ws,
                done_event(&ws, &request_id, false, Some(&msg), None),
            )
            .await;
            return Err(CloneFailure {
                category: None,
                detail: msg,
            });
        }
    };

    let bus_reader = bus.clone();
    let ws_reader = ws.clone();
    let request_id_reader = request_id.clone();
    let reader_task = tokio::spawn(async move {
        stream_stderr(stderr, bus_reader, ws_reader, request_id_reader).await
    });

    // Wait for the child under a hard timeout so a stalled clone never wedges
    // the daemon. On timeout, reap the process group and emit `ok:false`.
    let wait_result = tokio::time::timeout(CLONE_TIMEOUT, child.wait()).await;
    // Ensure the reader task drains any final stderr before we settle.
    let tail_error = reader_task.await.ok().flatten();

    match wait_result {
        Ok(Ok(status)) if status.success() => {
            publish(
                &bus,
                &ws,
                progress_event(&ws, &request_id, "complete", 100, "Clone complete!"),
            )
            .await;
            publish(&bus, &ws, done_event(&ws, &request_id, true, None, None)).await;
            Ok(())
        }
        Ok(Ok(status)) => {
            let msg = match tail_error {
                Some(t) if !t.is_empty() => format!("git clone failed ({}): {}", status, t),
                _ => format!("git clone failed ({})", status),
            };
            let redacted = redact_credentials(&msg);
            let category = classified(&redacted);
            publish(
                &bus,
                &ws,
                done_event(&ws, &request_id, false, Some(&redacted), category),
            )
            .await;
            Err(CloneFailure {
                category,
                detail: redacted,
            })
        }
        Ok(Err(e)) => {
            let msg = format!("git wait failed: {e}");
            publish(
                &bus,
                &ws,
                done_event(&ws, &request_id, false, Some(&msg), None),
            )
            .await;
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
            publish(
                &bus,
                &ws,
                done_event(&ws, &request_id, false, Some(&msg), category),
            )
            .await;
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
async fn stream_stderr<R>(
    stderr: R,
    bus: EventBus,
    ws: WorkspaceId,
    request_id: String,
) -> Option<String>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut reader = BufReader::new(stderr);
    let mut buf: Vec<u8> = Vec::with_capacity(256);
    let mut last_phase = String::from("starting");
    let mut last_percent: u32 = 0;
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
        for (phase, percent, message) in parse_progress(&text) {
            if phase != last_phase || percent > last_percent {
                last_phase = phase.to_string();
                last_percent = percent;
                publish(
                    &bus,
                    &ws,
                    progress_event(&ws, &request_id, phase, percent, &message),
                )
                .await;
            }
        }
        // Keep a bounded tail (~4KiB) of stderr for error messages.
        tail.push_str(&text);
        if tail.len() > 4096 {
            let drop_to = tail.len() - 4096;
            tail.drain(..drop_to);
        }
    }
    if tail.trim().is_empty() {
        None
    } else {
        Some(tail.trim().to_string())
    }
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

/// Ported from the FE's stderr regex table: match one canonical phase per line.
fn parse_progress(text: &str) -> Vec<(&'static str, u32, String)> {
    let mut out = Vec::new();
    // Static regex-lite scanners keep the dep footprint at zero. Each rule
    // returns (phase, percent, human_message).
    if text.contains("Cloning into") {
        out.push(("starting", 0, "Cloning repository...".to_string()));
    }
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
    out
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

fn progress_event(
    workspace_id: &WorkspaceId,
    request_id: &str,
    phase: &str,
    percent: u32,
    message: &str,
) -> NewEvent {
    NewEvent {
        workspace_id: workspace_id.clone(),
        timestamp: now_iso(),
        event_type: GIT_CLONE_PROGRESS.to_string(),
        actor: system_actor(),
        session_id: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data: json!({
            "requestId": request_id,
            "phase": phase,
            "percent": percent,
            "message": message,
        }),
    }
}

fn done_event(
    workspace_id: &WorkspaceId,
    request_id: &str,
    ok: bool,
    error: Option<&str>,
    error_code: Option<CloneErrorCategory>,
) -> NewEvent {
    let mut data = json!({ "requestId": request_id, "ok": ok });
    if let Some(err) = error {
        data.as_object_mut()
            .unwrap()
            .insert("error".to_string(), json!(err));
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

async fn publish(bus: &EventBus, _ws: &WorkspaceId, event: NewEvent) {
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
    /// the child environment under `TOKEN_ENV`.
    #[test]
    fn build_clone_command_injects_token_via_env_not_argv() {
        let token = "ghp_secret1234567890";
        let cmd = build_clone_command(
            "https://github.com/acme/private.git",
            Path::new("/tmp/x"),
            Some(token),
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
            args.iter()
                .any(|a| a.starts_with("credential.https://github.com.helper=")),
            "credential helper config present: {args:?}"
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

    /// No token (or an unusable one) leaves the command untouched: no helper
    /// config in argv, no token env var.
    #[test]
    fn build_clone_command_without_token_adds_no_helper() {
        for token in [None, Some(""), Some("  "), Some("bad\ntoken")] {
            let cmd = build_clone_command(
                "https://github.com/acme/repo.git",
                Path::new("/tmp/x"),
                token,
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
                std_cmd
                    .get_envs()
                    .all(|(k, _)| k.to_string_lossy() != intent_git::auth::TOKEN_ENV),
                "no token env var without a usable token"
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
        );
        assert_eq!(ev.data["errorCode"], json!("auth-required"));
        let ev = done_event(
            &ws,
            "r2",
            false,
            Some("weird failure"),
            classified("weird failure"),
        );
        assert!(ev.data.get("errorCode").is_none());
        let ev = done_event(&ws, "r3", true, None, None);
        assert!(ev.data.get("errorCode").is_none());
        // The daemon's own timeout message classifies as `network`.
        assert_eq!(classified("git clone timed out"), Some(C::Network));
    }

    #[test]
    fn parse_progress_matches_phases() {
        let ph = parse_progress("Receiving objects:  45% (10/22)");
        assert_eq!(ph.len(), 1);
        assert_eq!(ph[0].0, "receiving");
        assert_eq!(ph[0].1, 45);
        let ch = parse_progress("Checking out files: 100% (1/1), done.");
        assert_eq!(ch[0].0, "checkout");
        assert_eq!(ch[0].1, 100);
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
