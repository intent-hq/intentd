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

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use intent_core::events::{GIT_CLONE_DONE, GIT_CLONE_PROGRESS};
use intent_core::{expand_tilde, now_iso, Error, Result, WorkspaceId};
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
pub(crate) async fn perform_clone(job: CloneJob) -> std::result::Result<(), String> {
    run_clone(job).await
}

async fn run_clone(job: CloneJob) -> std::result::Result<(), String> {
    let CloneJob {
        request_id,
        workspace_id,
        url,
        target_path,
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

    let mut cmd = Command::new("git");
    cmd.arg("clone")
        .arg("--progress")
        .arg(&url)
        .arg(&target_path)
        .env("GIT_LFS_SKIP_SMUDGE", GIT_LFS_SKIP_SMUDGE)
        .env("GIT_TERMINAL_PROMPT", "0")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    #[cfg(unix)]
    cmd.process_group(0);

    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => {
            let msg = format!("git spawn failed: {e}");
            publish(&bus, &ws, done_event(&ws, &request_id, false, Some(&msg))).await;
            return Err(msg);
        }
    };
    let stderr = match child.stderr.take() {
        Some(s) => s,
        None => {
            let _ = child.kill().await;
            let msg = "git stderr not piped".to_string();
            publish(&bus, &ws, done_event(&ws, &request_id, false, Some(&msg))).await;
            return Err(msg);
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
            publish(&bus, &ws, done_event(&ws, &request_id, true, None)).await;
            Ok(())
        }
        Ok(Ok(status)) => {
            let msg = match tail_error {
                Some(t) if !t.is_empty() => format!("git clone failed ({}): {}", status, t),
                _ => format!("git clone failed ({})", status),
            };
            let redacted = redact_credentials(&msg);
            publish(
                &bus,
                &ws,
                done_event(&ws, &request_id, false, Some(&redacted)),
            )
            .await;
            Err(redacted)
        }
        Ok(Err(e)) => {
            let msg = format!("git wait failed: {e}");
            publish(&bus, &ws, done_event(&ws, &request_id, false, Some(&msg))).await;
            Err(msg)
        }
        Err(_) => {
            reap_child_group(&mut child).await;
            let msg = "git clone timed out".to_string();
            publish(&bus, &ws, done_event(&ws, &request_id, false, Some(&msg))).await;
            Err(msg)
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
) -> NewEvent {
    let mut data = json!({ "requestId": request_id, "ok": ok });
    if let Some(err) = error {
        data.as_object_mut()
            .unwrap()
            .insert("error".to_string(), json!(err));
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
