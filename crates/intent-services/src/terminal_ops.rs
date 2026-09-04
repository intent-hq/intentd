//! `terminal.*` + ACP `terminal/*` support over the unified `intent-pty` host
//! (§5.13, §6.7, §12).
//!
//! Two front doors share one [`PtyHost`]:
//!
//! - The transport `terminal.*` RPC (dispatched through [`WorkspaceApi`]) spawns
//!   PTYs scoped to a workspace and fans their output to event subscribers as
//!   `terminal:data` (base64 `chunk`) / `terminal:exit` events (§6.5). Late
//!   attach back-fills via `terminal.getBuffer`, then tails the live events.
//! - The ACP [`TerminalHost`] adapter ([`PtyTerminalHost`]) lets an agent's
//!   client-served `terminal/*` calls run on the same host, scoped to the agent
//!   session id.
//!
//! [`WorkspaceApi`]: intent_core::WorkspaceApi

use std::path::{Path, PathBuf};
use std::sync::Arc;

use base64::Engine as _;
use intent_acp::{
    AcpError, AcpResult, TerminalCreateParams, TerminalExitInfo, TerminalHost, TerminalOutputInfo,
};
use intent_core::events::{TERMINAL_DATA, TERMINAL_EXIT};
use intent_core::{now_iso, BoxFuture, Error, Result, WorkspaceId};
use intent_pty::{LineSnapshot, PtyExit, PtyHost, PtyId, PtySize, SpawnSpec};
use intent_store::{NewEvent, Store};
use serde_json::{json, Value};
use std::time::Duration;

use tokio::sync::broadcast::error::{RecvError, TryRecvError};

use crate::events::EventBus;
use crate::shell::{default_shell, scrubbed_env_vars_except, shell_true_invocation};
use crate::{publish_event, publish_event_transient, system_actor, SettingsRegistry};

/// How often the output streamer polls for natural process exit. The broadcast
/// sender lives in the (still-tracked) session, so a child that exits on its own
/// — without a `terminal.kill` — never closes the live channel; polling the
/// child is what lets the streamer emit `terminal:exit` in that case.
const EXIT_POLL: Duration = Duration::from_millis(25);

/// Terminal type advertised by interactive PTYs rendered by xterm.js clients.
const DEFAULT_TERM: &str = "xterm-256color";

/// Ensure the effective terminal environment has a usable terminal type while
/// preserving explicit or inherited non-empty values.
fn ensure_terminal_term(env: &mut Vec<(String, String)>, inherited_term: Option<&str>) {
    match env.iter_mut().rev().find(|(name, _)| name == "TERM") {
        Some((_, value)) if value.is_empty() => *value = DEFAULT_TERM.to_string(),
        None if inherited_term.is_none_or(str::is_empty) => {
            env.push(("TERM".to_string(), DEFAULT_TERM.to_string()));
        }
        Some(_) | None => {}
    }
}

/// Resolve a wire terminal id (`pty-{n}`) to a [`PtyId`], or `NotFound`.
fn resolve(terminal_id: &str) -> Result<PtyId> {
    PtyId::parse(terminal_id).ok_or_else(|| Error::NotFound(format!("terminal {terminal_id}")))
}

/// Spawn a workspace-scoped PTY for `terminal.create` and begin streaming its
/// output onto the bus; returns `{ terminalId }`. `env` is an optional
/// overlay layered onto the daemon's inherited environment (`portable-pty`
/// inherits by default), so callers can pass per-terminal variables through
/// without dropping them. When the `exposeGitCredentialToChildren` setting is
/// on, the github.com-scoped daemon-backed credential-helper env pair is
/// injected under the caller's overlay — caller-supplied keys
/// always win (see [`git_credential_env`]). An absent caller `TERM` preserves
/// non-empty daemon inheritance; otherwise a missing or empty effective value
/// defaults to `xterm-256color`. When `cwd` is omitted the PTY spawns in the
/// workspace's worktree root (see [`default_cwd`]); an explicit `cwd` wins.
/// The inherited `npm_config_prefix` launcher variable is scrubbed so nvm can
/// initialize, while an explicit caller value is preserved. On POSIX, an omitted
/// command launches zsh/bash with `-l` so login profiles are loaded; explicit
/// commands and Windows defaults are unchanged.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn create(
    pty: Arc<PtyHost>,
    bus: Option<EventBus>,
    store: Option<Store>,
    settings: Option<Arc<SettingsRegistry>>,
    workspace_id: WorkspaceId,
    cols: u16,
    rows: u16,
    cwd: Option<String>,
    command: Option<String>,
    env: Option<std::collections::BTreeMap<String, String>>,
) -> Result<Value> {
    let (command, is_default_shell) = match command {
        Some(command) => (command, false),
        None => (default_shell(), true),
    };
    let user_env: Vec<(String, String)> = env.map(|m| m.into_iter().collect()).unwrap_or_default();
    // Resolved before the credential env, which discovers the helpers already
    // configured *in the spawn directory* so they stay reachable behind
    // intentd's (monorepo#3059).
    let spawn_cwd = match cwd {
        Some(cwd) => Some(PathBuf::from(cwd)),
        None => match store.as_ref() {
            Some(store) => default_cwd(store, &workspace_id, None).await,
            None => None,
        },
    };
    let mut spawn_env = overlay_credential_env(
        injected_git_env(settings.as_deref(), spawn_cwd.as_deref()),
        user_env,
    );
    let inherited_term = std::env::var("TERM").ok();
    ensure_terminal_term(&mut spawn_env, inherited_term.as_deref());
    let mut spec = terminal_spawn_spec_for(
        workspace_id.as_str(),
        &command,
        is_default_shell,
        cfg!(windows),
        spawn_env,
    );
    spec.size = PtySize { rows, cols };
    spec.cwd = spawn_cwd;
    let pty_id = pty.spawn(spec)?;
    let terminal_id = pty_id.to_string();
    spawn_output_stream(pty, bus, workspace_id, pty_id, terminal_id.clone());
    Ok(json!({ "terminalId": terminal_id }))
}

/// Base spawn spec for an interactive workspace terminal. Only the omitted-
/// command path receives login-shell arguments; explicit commands retain their
/// caller-specified argv (empty for the `terminal.create` wire shape).
fn terminal_spawn_spec_for(
    scope: &str,
    command: &str,
    is_default_shell: bool,
    is_windows: bool,
    env: Vec<(String, String)>,
) -> SpawnSpec {
    let mut spec = SpawnSpec::new(scope, command);
    if is_default_shell {
        spec.args = interactive_login_args(command, is_windows);
    }
    spec.env = env;
    spec.env_remove = scrubbed_env_vars_except(&spec.env);
    spec
}

/// Login arguments for a default interactive POSIX shell. A PTY already makes
/// the shell interactive, so zsh/bash need only `-l`; plain sh and Windows keep
/// their existing argument-free behavior.
fn interactive_login_args(shell: &str, is_windows: bool) -> Vec<String> {
    if is_windows {
        return Vec::new();
    }
    let base = std::path::Path::new(shell)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(shell)
        .to_ascii_lowercase();
    if matches!(base.as_str(), "zsh" | "bash") {
        vec!["-l".to_string()]
    } else {
        Vec::new()
    }
}

/// Default working directory when `terminal.create` omits `cwd`: the
/// workspace's worktree root, resolved the same way `script_ops` resolves a
/// script cwd (`worktreePath`, else `repositoryPath`). When `caller_agent_id`
/// is provided and the agent has a sandbox, the sandbox path overrides the
/// workspace worktree (sandboxed agent containment). A missing workspace row
/// or one without a resolvable worktree yields `None`, so the PTY inherits the
/// daemon's cwd (the prior behavior).
async fn default_cwd(
    store: &Store,
    workspace_id: &WorkspaceId,
    caller_agent_id: Option<&intent_core::AgentId>,
) -> Option<PathBuf> {
    if let Some(agent_id) = caller_agent_id {
        if let Ok(session) = store.get_agent_session(agent_id).await {
            if let Some(sandbox_path) = session.sandbox_path {
                return Some(sandbox_path.into());
            }
        }
    }
    let workspace = store.get_workspace(workspace_id).await.ok()?;
    crate::git_ops::worktree_path(&workspace)
}

/// Environment pairs offering the daemon-managed GitHub credential to git
/// run inside a spawned terminal, gated on
/// `sourceControl.github.exposeGitCredentialToChildren` (monorepo#884 Phase
/// 2.2): the github.com-scoped `intentd git-credential` helper carried by
/// `GIT_CONFIG_PARAMETERS` — **no token bytes in the child environment** (the
/// helper fetches the credential from the daemon over UDS on each `get`, so
/// tokens refresh live and revocation applies immediately; never raw
/// `GITHUB_TOKEN`/`GH_TOKEN`). Building the pairs never fails or blocks the
/// spawn: an unresolvable daemon binary path simply yields no pairs (logged
/// at debug).
///
/// `cwd` is the directory the terminal is spawned in, and must be the one the
/// spawn actually uses: the helpers already configured there are re-added
/// behind intentd's, so a repository-local one stays reachable
/// (monorepo#3059).
pub(crate) fn git_credential_env(
    settings: Option<&SettingsRegistry>,
    cwd: Option<&Path>,
) -> Vec<(String, String)> {
    if !expose_git_credential(settings) {
        return Vec::new();
    }
    credential_pairs(cwd)
}

/// All git env pairs injected under the caller's overlay for a spawn in
/// `cwd`: the gated credential-helper pair ([`git_credential_env`]) plus the
/// ungated commit-identity vars (`GIT_AUTHOR_*`/`GIT_COMMITTER_*`, resolved
/// from `cwd`'s repository via the same config chain `git.commit` uses —
/// intent-hq/intent#4142; identity is not a secret, so no settings gate).
/// An identity var already set in the daemon's own env is inherited untouched,
/// and caller-supplied keys still win via [`overlay_credential_env`].
pub(crate) fn injected_git_env(
    settings: Option<&SettingsRegistry>,
    cwd: Option<&Path>,
) -> Vec<(String, String)> {
    let mut env = git_credential_env(settings, cwd);
    env.extend(intent_git::identity::commit_identity_env(cwd));
    env
}

/// The `exposeGitCredentialToChildren` gate. `None` (registry not wired —
/// minimal/test compositions) reads as **off** so bare spawns never trigger
/// token resolution; the production composition root always wires the
/// registry, where the schema default (`true`) applies. Shared with the
/// `system.gitCredential` UDS RPC (see [`crate::github_git_credential`]).
pub(crate) fn expose_git_credential(settings: Option<&SettingsRegistry>) -> bool {
    settings.is_some_and(|r| {
        r.snapshot()
            .effective
            .source_control
            .github
            .expose_git_credential_to_children
    })
}

/// Build the daemon-backed helper env pair on top of the daemon's own
/// inherited `GIT_CONFIG_PARAMETERS` (the PTY child inherits the daemon env,
/// so overwriting the variable without re-appending would drop inherited
/// entries). The helper path is the running daemon's own binary
/// (`current_exe`); an unresolvable path yields no pairs (logged at debug).
fn credential_pairs(cwd: Option<&Path>) -> Vec<(String, String)> {
    let Some(intentd) = crate::daemon_exe_path() else {
        return Vec::new();
    };
    let inherited = std::env::var(intent_git::auth::GIT_CONFIG_PARAMETERS_ENV).ok();
    intent_git::auth::daemon_helper_env(&intentd, cwd, inherited.as_deref())
}

/// Layer caller-supplied env over the injected credential pairs: a key the
/// caller sets drops the injected pair outright (user env always wins), and
/// caller pairs come last so they also win the host's later-entry-overrides
/// application order.
fn overlay_credential_env(
    credential: Vec<(String, String)>,
    user: Vec<(String, String)>,
) -> Vec<(String, String)> {
    let mut env: Vec<(String, String)> = credential
        .into_iter()
        .filter(|(key, _)| !user.iter().any(|(user_key, _)| user_key == key))
        .collect();
    env.extend(user);
    env
}

/// Write base64-encoded input to a PTY's stdin.
pub(crate) fn write(pty: &PtyHost, terminal_id: &str, data: &str) -> Result<Value> {
    let id = resolve(terminal_id)?;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(data)
        .map_err(|e| Error::InvalidParams(format!("invalid base64 data: {e}")))?;
    pty.write(id, &bytes)?;
    Ok(json!({ "ok": true }))
}

/// Resize a PTY's visible area.
pub(crate) fn resize(pty: &PtyHost, terminal_id: &str, cols: u16, rows: u16) -> Result<Value> {
    let id = resolve(terminal_id)?;
    pty.resize(id, PtySize { rows, cols })?;
    Ok(json!({ "ok": true }))
}

/// Kill a PTY; the streamer emits `terminal:exit` once its process ends.
pub(crate) async fn kill(pty: &PtyHost, terminal_id: &str) -> Result<Value> {
    let id = resolve(terminal_id)?;
    pty.kill(id).await;
    Ok(json!({ "ok": true }))
}

/// Snapshot a PTY's scrollback for replay, base64-encoded (optionally keeping
/// only the trailing `max_bytes`).
pub(crate) fn get_buffer(
    pty: &PtyHost,
    terminal_id: &str,
    max_bytes: Option<i64>,
) -> Result<Value> {
    let id = resolve(terminal_id)?;
    // Omitted (and legacy negative) bounds retain full-history semantics. A
    // usable bound takes the ring tail directly, without cloning its prefix.
    let bytes = if let Some(max) = max_bytes.and_then(|n| usize::try_from(n).ok()) {
        pty.scrollback_tail(id, max)?
    } else {
        pty.scrollback(id)?
    };
    let data = base64::engine::general_purpose::STANDARD.encode(&bytes);
    Ok(json!({ "terminalId": terminal_id, "data": data }))
}

/// The workspace's live terminals wrapped in the per-boot envelope
/// `{ terminals: [{ id, name, cwd, isExecutingCommand }], daemonBootId }`
/// (§5.13; monorepo#1334). Exited PTYs are omitted while their sessions remain
/// retained for post-exit output and release. `name` is the display name given
/// at spawn (`SpawnSpec::name`, e.g. "Setup Script"), else the constant
/// `"Terminal"`; `cwd` is the working directory resolved at spawn;
/// `isExecutingCommand` is the child's liveness (the spawned process is the
/// running command). `daemon_boot_id` is the daemon's per-process boot id, so
/// clients can tell which daemon lifetime a (possibly empty) list belongs to.
#[allow(clippy::unnecessary_wraps)] // WorkspaceApi surface; keeps the uniform Result shape
pub(crate) fn list(
    pty: &PtyHost,
    workspace_id: &WorkspaceId,
    daemon_boot_id: &str,
) -> Result<Value> {
    let mut terminals: Vec<(String, Value)> = pty
        .list_scope(workspace_id.as_str())
        .into_iter()
        .map(|id| {
            let id_str = id.to_string();
            let info = pty.info(id);
            let cwd = info.as_ref().and_then(|i| i.cwd.as_deref()).unwrap_or("");
            let name = info
                .as_ref()
                .and_then(|i| i.name.as_deref())
                .unwrap_or("Terminal");
            let is_executing = info.as_ref().is_some_and(|i| i.alive);
            let value = json!({
                "id": id_str,
                "name": name,
                "cwd": cwd,
                "isExecutingCommand": is_executing,
            });
            (id_str, value)
        })
        .collect();
    terminals.sort_by(|a, b| a.0.cmp(&b.0));
    let terminals: Vec<Value> = terminals.into_iter().map(|(_, v)| v).collect();
    Ok(json!({ "terminals": terminals, "daemonBootId": daemon_boot_id }))
}

/// `terminal.readOutput`: a formatted, ANSI-stripped view of a terminal's
/// scrollback (TS `ws.terminal.readOutput`). Returns a bare string: a header
/// (`Terminal {id} (cwd: ...)[ showing last N of M lines]`), a `─`×40 separator,
/// then the trailing `max_lines` (default 200, clamped to 1..=10000) lines with
/// trailing blank lines trimmed; or `"Terminal has no output yet."` when empty.
pub(crate) fn read_output(
    pty: &PtyHost,
    workspace_id: &WorkspaceId,
    terminal_id: &str,
    max_lines: Option<i64>,
    paginate: bool,
    page_token: Option<&String>,
) -> Result<Value> {
    let id = PtyId::parse(terminal_id)
        .ok_or_else(|| Error::Internal(format!("Terminal not found: {terminal_id}")))?;
    let info = pty
        .info(id)
        .ok_or_else(|| Error::Internal(format!("Terminal not found: {terminal_id}")))?;
    if info.scope != workspace_id.as_str() {
        return Err(Error::Internal(
            "Terminal does not belong to this workspace".to_string(),
        ));
    }

    // TA-2 / §5.5 opt-in pagination: when engaged, return the historical
    // scrollback as a `{ items, nextToken }` envelope of ANSI-stripped lines
    // ordered newest→oldest, with an opaque append-stable continuation token.
    // Absent the opt-in, preserve the legacy bare formatted string verbatim.
    if paginate || page_token.is_some() {
        let limit = crate::pagination::clamp_limit(max_lines);
        let token = page_token.map(std::string::String::as_str);
        let boundary = token.and_then(crate::pagination::backward_page_boundary);

        let (snapshot, lines, effective_total, window_token) = if boundary.is_some() {
            let snapshot = pty.scrollback_lines(id, limit, boundary)?;
            let lines = decoded_snapshot_lines(&snapshot);
            let total = snapshot.total_lines;
            (snapshot, lines, total, token)
        } else {
            // The historical helper trims blank lines at the newest end before
            // paging. Probe backward in bounded chunks so even an ANSI-only
            // blank suffix does not require cloning the complete scrollback.
            let mut probe_end = None;
            let effective_end = loop {
                let probe = pty.scrollback_lines(id, limit, probe_end)?;
                let probe_lines = decoded_snapshot_lines(&probe);
                if let Some(last_content) =
                    probe_lines.iter().rposition(|line| !line.trim().is_empty())
                {
                    break probe.start_line + last_content + 1;
                }
                if probe.start_line == 0 {
                    break 0;
                }
                probe_end = Some(probe.start_line);
            };
            let snapshot = pty.scrollback_lines(id, limit, Some(effective_end))?;
            let lines = decoded_snapshot_lines(&snapshot);
            (snapshot, lines, effective_end, None)
        };

        let window = crate::pagination::page_window(effective_total, max_lines, window_token);
        debug_assert_eq!(
            (snapshot.start_line, snapshot.end_line),
            (window.start, window.end)
        );
        return Ok(json!({
            "items": lines.into_iter().rev().collect::<Vec<_>>(),
            "nextToken": window.next_token,
        }));
    }

    let max_line_count =
        usize::try_from(max_lines.unwrap_or(200).clamp(1, 10000)).expect("value fits in usize");
    let snapshot = pty.scrollback_lines(id, max_line_count, None)?;
    if !snapshot.retained_has_non_whitespace {
        return Ok(Value::String("Terminal has no output yet.".to_string()));
    }

    let mut output_lines = decoded_snapshot_lines(&snapshot);
    while output_lines.last().is_some_and(|l| l.trim().is_empty()) {
        output_lines.pop();
    }

    let truncated = snapshot.total_lines > max_line_count;
    let cwd = info.cwd.unwrap_or_default();
    let header = if truncated {
        format!(
            "Terminal {terminal_id} (cwd: {cwd}) [showing last {max_line_count} of {} lines]",
            snapshot.total_lines
        )
    } else {
        format!("Terminal {terminal_id} (cwd: {cwd})")
    };
    let separator = "\u{2500}".repeat(40);
    Ok(Value::String(format!(
        "{header}\n{separator}\n{}",
        output_lines.join("\n")
    )))
}

/// Decode and ANSI-strip the raw lines copied by `scrollback_lines`, including
/// any leading unmatched OSC context needed to make the window sequence-safe.
/// The ring includes the newline after a window that ends before the live tail;
/// `take(line_count)` excludes the synthetic split item after that delimiter.
fn decoded_snapshot_lines(snapshot: &LineSnapshot) -> Vec<String> {
    let raw = String::from_utf8_lossy(&snapshot.bytes);
    let clean = strip_ansi(&raw);
    let mut lines: Vec<String> = clean
        .split('\n')
        .take(snapshot.line_count())
        .map(str::to_string)
        .collect();
    lines.resize(snapshot.line_count(), String::new());
    lines
}

/// Strip ANSI escape sequences from terminal output, mirroring the TS
/// `readOutput` regex (`CSI`/`OSC`/private-mode sequences).
fn strip_ansi(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut out = String::with_capacity(input.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == 0x1b {
            // ESC
            match bytes.get(i + 1) {
                // OSC: ESC ] ... BEL(0x07) or ST(ESC \\)
                Some(b']') => {
                    i += 2;
                    while i < bytes.len() {
                        if bytes[i] == 0x07 {
                            i += 1;
                            break;
                        }
                        if bytes[i] == 0x1b && bytes.get(i + 1) == Some(&b'\\') {
                            i += 2;
                            break;
                        }
                        i += 1;
                    }
                    continue;
                }
                // CSI: ESC [ (optional '?') params (0-9;) final letter
                Some(b'[') => {
                    i += 2;
                    if i < bytes.len() && bytes[i] == b'?' {
                        i += 1;
                    }
                    while i < bytes.len() && (bytes[i].is_ascii_digit() || bytes[i] == b';') {
                        i += 1;
                    }
                    if i < bytes.len() && bytes[i].is_ascii_alphabetic() {
                        i += 1; // consume final byte
                    }
                    continue;
                }
                _ => {
                    i += 1;
                    continue;
                }
            }
        }
        // Copy the next UTF-8 scalar intact.
        let ch_len = utf8_len(bytes[i]);
        let end = (i + ch_len).min(bytes.len());
        out.push_str(&input[i..end]);
        i = end;
    }
    out
}

/// Length in bytes of the UTF-8 scalar starting with `b`.
fn utf8_len(b: u8) -> usize {
    if b < 0x80 {
        1
    } else if b >> 5 == 0b110 {
        2
    } else if b >> 4 == 0b1110 {
        3
    } else if b >> 3 == 0b11110 {
        4
    } else {
        1
    }
}

/// Attach to a freshly created PTY and fan its output onto the bus as
/// `terminal:data`, emitting a terminal `terminal:exit` when the stream closes.
pub(crate) fn spawn_output_stream(
    pty: Arc<PtyHost>,
    bus: Option<EventBus>,
    workspace_id: WorkspaceId,
    pty_id: PtyId,
    terminal_id: String,
) {
    let Ok(attachment) = pty.attach(pty_id) else {
        return;
    };
    tokio::spawn(async move {
        let mut live = attachment.live;
        // Emit any output captured between spawn and attach exactly once, then
        // tail live chunks (the host guarantees history XOR live, never both).
        if !attachment.backlog.is_empty() {
            emit_data(
                bus.as_ref(),
                &workspace_id,
                &terminal_id,
                &attachment.backlog,
            );
        }
        loop {
            tokio::select! {
                recv = live.recv() => match recv {
                    Ok(chunk) => emit_data(bus.as_ref(), &workspace_id, &terminal_id, &chunk),
                    Err(RecvError::Lagged(_)) => {},
                    // A `terminal.kill` tore down the session and dropped the
                    // sender; the process is gone.
                    Err(RecvError::Closed) => break,
                },
                () = tokio::time::sleep(EXIT_POLL) => {
                    if matches!(pty.try_exit(pty_id), Ok(Some(_))) {
                        // Reaped: drain any output the reader flushed just before
                        // EOF, then stop tailing.
                        drain_pending(&mut live, bus.as_ref(), &workspace_id, &terminal_id);
                        break;
                    }
                }
            }
        }
        let exit = pty.try_exit(pty_id).ok().flatten();
        emit_exit(bus.as_ref(), &workspace_id, &terminal_id, exit).await;
    });
}

/// Flush any output buffered on the live channel without blocking (used once the
/// child has exited so trailing output still streams before `terminal:exit`).
fn drain_pending(
    live: &mut tokio::sync::broadcast::Receiver<Arc<Vec<u8>>>,
    bus: Option<&EventBus>,
    workspace_id: &WorkspaceId,
    terminal_id: &str,
) {
    loop {
        match live.try_recv() {
            Ok(chunk) => emit_data(bus, workspace_id, terminal_id, &chunk),
            Err(TryRecvError::Lagged(_)) => {}
            Err(TryRecvError::Empty | TryRecvError::Closed) => break,
        }
    }
}

/// Broadcast a `terminal:data` event carrying a base64 output `chunk`.
///
/// Transient (broadcast-only, never persisted — same path as
/// `chat:stream:delta`): PTY output is high-volume and must not serialize
/// behind a durable `SQLite` commit per chunk, which throttled paste echo to one
/// chunk per writer-batch window. Scrollback replay reads the PTY host ring
/// buffer via `terminal.getBuffer`, so nothing consumes persisted
/// `terminal:data` rows. Ordering vs `terminal:exit` is preserved: the stream
/// task broadcasts every chunk synchronously before it awaits the durable
/// `emit_exit`, so exit can never overtake data.
fn emit_data(bus: Option<&EventBus>, ws: &WorkspaceId, terminal_id: &str, bytes: &[u8]) {
    let chunk = base64::engine::general_purpose::STANDARD.encode(bytes);
    publish_event_transient(
        bus,
        &terminal_event(
            ws,
            TERMINAL_DATA,
            json!({ "terminalId": terminal_id, "chunk": chunk }),
        ),
    );
}

/// Publish a self-sufficient `terminal:exit` event (durable, emitted after the
/// stream task has broadcast every `terminal:data` chunk).
async fn emit_exit(
    bus: Option<&EventBus>,
    ws: &WorkspaceId,
    terminal_id: &str,
    exit: Option<PtyExit>,
) {
    let exit_code = exit.map(|e| e.exit_code);
    publish_event(
        bus,
        terminal_event(
            ws,
            TERMINAL_EXIT,
            json!({ "terminalId": terminal_id, "exitCode": exit_code, "signal": Value::Null }),
        ),
    )
    .await;
}

/// Build a terminal change event with the daemon system actor.
fn terminal_event(workspace_id: &WorkspaceId, event_type: &str, data: Value) -> NewEvent {
    NewEvent {
        workspace_id: workspace_id.clone(),
        timestamp: now_iso(),
        event_type: event_type.to_string(),
        actor: system_actor(),
        session_id: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data,
    }
}

/// ACP [`TerminalHost`] adapter: runs an agent's client-served `terminal/*`
/// calls on the shared [`PtyHost`], scoped to the agent session id (§6.7).
pub(crate) struct PtyTerminalHost {
    pty: Arc<PtyHost>,
    /// Settings registry backing the credential-injection gate
    /// ([`git_credential_env`]); `None` (minimal compositions) disables
    /// injection.
    settings: Option<Arc<SettingsRegistry>>,
    /// When true, `terminal/create` spawns via a shell (`shell: true` semantics)
    /// so providers that pack a shell line into `command` still work. Sourced
    /// from [`intent_providers::ProviderConfig::terminal_requires_shell`].
    terminal_requires_shell: bool,
    /// The agent session's working directory, used when a `terminal/create`
    /// request omits `cwd`: the spawn (and thus the git env resolution) falls
    /// back to it, so a provider terminal without an explicit cwd still runs
    /// in — and resolves the commit identity from — the session's repository
    /// (intent-hq/intent#4142) instead of the daemon's own cwd.
    session_cwd: Option<PathBuf>,
}

impl PtyTerminalHost {
    /// Wire the adapter over the shared host (argv-only terminal spawn).
    #[cfg(test)]
    pub fn new(pty: Arc<PtyHost>, settings: Option<Arc<SettingsRegistry>>) -> Self {
        Self::with_shell_mode(pty, settings, false, None)
    }

    /// Like [`Self::new`], with an explicit shell-wrap mode for the provider
    /// and the session cwd fallback for requests that omit `cwd`.
    pub(crate) fn with_shell_mode(
        pty: Arc<PtyHost>,
        settings: Option<Arc<SettingsRegistry>>,
        terminal_requires_shell: bool,
        session_cwd: Option<PathBuf>,
    ) -> Self {
        Self {
            pty,
            settings,
            terminal_requires_shell,
            session_cwd,
        }
    }
}

impl TerminalHost for PtyTerminalHost {
    fn create(&self, params: TerminalCreateParams) -> BoxFuture<'_, AcpResult<String>> {
        let pty = self.pty.clone();
        let settings = self.settings.clone();
        let terminal_requires_shell = self.terminal_requires_shell;
        let session_cwd = self.session_cwd.clone();
        Box::pin(async move {
            let spawn_cwd = params.cwd.or(session_cwd);
            let credential = injected_git_env(settings.as_deref(), spawn_cwd.as_deref());
            let (command, args) = if terminal_requires_shell {
                shell_true_invocation(&params.command, &params.args)
            } else {
                (params.command, params.args)
            };
            let mut spec = SpawnSpec::new(params.session_id, command);
            spec.args = args;
            spec.env = overlay_credential_env(credential, params.env);
            let inherited_term = std::env::var("TERM").ok();
            ensure_terminal_term(&mut spec.env, inherited_term.as_deref());
            spec.env_remove = scrubbed_env_vars_except(&spec.env);
            spec.cwd = spawn_cwd;
            if let Some(limit) = params
                .output_byte_limit
                .and_then(|n| usize::try_from(n).ok())
            {
                spec.scrollback_bytes = limit;
            }
            let pty_id = pty
                .spawn(spec)
                .map_err(|e: intent_core::Error| acp_err(&e))?;
            Ok(pty_id.to_string())
        })
    }

    fn output(&self, terminal_id: String) -> BoxFuture<'_, AcpResult<TerminalOutputInfo>> {
        let pty = self.pty.clone();
        Box::pin(async move {
            let id = acp_resolve(&terminal_id)?;
            let limit = pty
                .scrollback(id)
                .map_err(|e: intent_core::Error| acp_err(&e))?;
            let exit = pty
                .try_exit(id)
                .ok()
                .flatten()
                .map(|exit: PtyExit| to_exit_info(&exit));
            Ok(TerminalOutputInfo {
                output: String::from_utf8_lossy(&limit).into_owned(),
                truncated: false,
                exit_status: exit,
            })
        })
    }

    fn wait_for_exit(&self, terminal_id: String) -> BoxFuture<'_, AcpResult<TerminalExitInfo>> {
        let pty = self.pty.clone();
        Box::pin(async move {
            let id = acp_resolve(&terminal_id)?;
            let exit = pty
                .wait(id)
                .await
                .map_err(|e: intent_core::Error| acp_err(&e))?;
            Ok(to_exit_info(&exit))
        })
    }

    fn release(&self, terminal_id: String) -> BoxFuture<'_, AcpResult<()>> {
        let pty = self.pty.clone();
        Box::pin(async move {
            let id = acp_resolve(&terminal_id)?;
            pty.kill(id).await;
            Ok(())
        })
    }

    fn kill(&self, terminal_id: String) -> BoxFuture<'_, AcpResult<()>> {
        let pty = self.pty.clone();
        Box::pin(async move {
            let id = acp_resolve(&terminal_id)?;
            pty.kill(id).await;
            Ok(())
        })
    }
}

/// Map a host error into an ACP terminal error.
fn acp_err(e: &Error) -> AcpError {
    AcpError::Terminal(e.to_string())
}

/// Resolve a wire terminal id into a [`PtyId`] for the ACP adapter.
fn acp_resolve(terminal_id: &str) -> AcpResult<PtyId> {
    PtyId::parse(terminal_id)
        .ok_or_else(|| AcpError::Terminal(format!("unknown terminal {terminal_id}")))
}

/// Convert a host [`PtyExit`] into the ACP exit shape (`signal` is unavailable
/// through the host abstraction).
fn to_exit_info(exit: &PtyExit) -> TerminalExitInfo {
    TerminalExitInfo {
        exit_code: Some(exit.exit_code),
        signal: None,
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    use std::time::Instant;

    use intent_core::{Event, Workspace, WorkspaceActivity, WorkspaceAttention, WorkspaceStatus};
    use intent_store::Store;

    use crate::events::{Subscription, SubscriptionFilter};

    /// Generous deadline so the real-PTY tests stay green under loaded CI.
    const TIMEOUT: Duration = Duration::from_secs(8);

    /// Extra-generous deadline for awaiting a child's natural exit under
    /// full-suite load (monorepo#573): it bounds only how long a *failure*
    /// takes to surface, never how long a passing run waits.
    const LONG_TIMEOUT: Duration = Duration::from_secs(60);

    /// A temp `SQLite` path cleaned up on drop (mirrors `events::bus_tests`).
    struct TempDb {
        path: PathBuf,
    }

    impl TempDb {
        fn new() -> Self {
            let path =
                std::env::temp_dir().join(format!("intentd-term-{}.db", uuid::Uuid::new_v4()));
            Self { path }
        }
    }

    impl Drop for TempDb {
        fn drop(&mut self) {
            for suffix in ["", "-wal", "-shm"] {
                let _ =
                    std::fs::remove_file(PathBuf::from(format!("{}{suffix}", self.path.display())));
            }
        }
    }

    /// A temp-backed bus whose subscribers receive each matched event promptly
    /// (the default filter does no batching).
    async fn bus() -> (TempDb, EventBus) {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        (tmp, EventBus::new(store))
    }

    fn host() -> Arc<PtyHost> {
        Arc::new(PtyHost::new())
    }

    /// Reap a test PTY even when an assertion unwinds before async cleanup.
    #[cfg(target_os = "macos")]
    struct PtyKillGuard {
        pty: Arc<PtyHost>,
        id: Option<PtyId>,
    }

    #[cfg(target_os = "macos")]
    impl PtyKillGuard {
        fn new(pty: Arc<PtyHost>, id: PtyId) -> Self {
            Self { pty, id: Some(id) }
        }

        fn disarm(&mut self) {
            self.id = None;
        }
    }

    #[cfg(target_os = "macos")]
    impl Drop for PtyKillGuard {
        fn drop(&mut self) {
            let Some(id) = self.id.take() else {
                return;
            };
            let pty = self.pty.clone();
            if let Ok(thread) = std::thread::Builder::new()
                .name("pty-test-cleanup".to_string())
                .spawn(move || {
                    if let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
                        .enable_time()
                        .build()
                    {
                        runtime.block_on(pty.kill(id));
                    }
                })
            {
                let _ = thread.join();
            }
        }
    }

    fn ws(id: &str) -> WorkspaceId {
        WorkspaceId::from(id)
    }

    fn contains_sub(haystack: &[u8], needle: &[u8]) -> bool {
        needle.is_empty() || haystack.windows(needle.len()).any(|w| w == needle)
    }

    fn decode(data: &str) -> Vec<u8> {
        base64::engine::general_purpose::STANDARD
            .decode(data)
            .expect("valid base64")
    }

    fn term_id(res: &Value) -> String {
        res["terminalId"].as_str().expect("terminalId").to_string()
    }

    /// Poll a synchronous closure until it yields `Some`, or the deadline passes.
    async fn poll_until<T>(mut f: impl FnMut() -> Option<T>, timeout: Duration) -> Option<T> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(v) = f() {
                return Some(v);
            }
            if Instant::now() >= deadline {
                return None;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Drain subscription batches until an event of `event_type` arrives.
    async fn wait_for_event(
        sub: &mut Subscription,
        event_type: &str,
        timeout: Duration,
    ) -> Option<Event> {
        let deadline = Instant::now() + timeout;
        loop {
            let remaining = deadline.checked_duration_since(Instant::now())?;
            match tokio::time::timeout(remaining, sub.recv()).await {
                Ok(Some(batch)) => {
                    if let Some(ev) = batch.into_iter().find(|e| e.event_type == event_type) {
                        return Some(ev);
                    }
                }
                _ => return None,
            }
        }
    }

    /// Accumulate decoded `terminal:data` chunks until `needle` appears.
    async fn collect_data_until(
        sub: &mut Subscription,
        needle: &[u8],
        timeout: Duration,
    ) -> Vec<u8> {
        let mut acc = Vec::new();
        let deadline = Instant::now() + timeout;
        while !contains_sub(&acc, needle) {
            let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                break;
            };
            match tokio::time::timeout(remaining, sub.recv()).await {
                Ok(Some(batch)) => {
                    for ev in batch {
                        if ev.event_type == TERMINAL_DATA {
                            if let Some(chunk) = ev.data.get("chunk").and_then(Value::as_str) {
                                acc.extend_from_slice(&decode(chunk));
                            }
                        }
                    }
                }
                _ => break,
            }
        }
        acc
    }

    // ---- pure helpers (no spawn) ----

    #[test]
    fn resolve_parses_and_rejects() {
        assert_eq!(resolve("pty-5").unwrap(), PtyId::parse("pty-5").unwrap());
        assert!(matches!(resolve("not-a-pty"), Err(Error::NotFound(_))));
    }

    #[test]
    fn default_shell_is_nonempty() {
        assert!(!default_shell().is_empty());
    }

    #[test]
    fn terminal_spawn_spec_scrubs_only_inherited_env_and_selects_login_argv() {
        for shell in ["/bin/zsh", "/opt/homebrew/bin/bash"] {
            let spec = terminal_spawn_spec_for("ws-1", shell, true, false, Vec::new());
            assert_eq!(spec.command, shell);
            assert_eq!(spec.args, ["-l"]);
            assert_eq!(spec.env_remove, ["npm_config_prefix"]);
        }

        let sh = terminal_spawn_spec_for("ws-1", "/bin/sh", true, false, Vec::new());
        assert!(sh.args.is_empty());

        let explicit = terminal_spawn_spec_for("ws-1", "/usr/bin/env", false, false, Vec::new());
        assert_eq!(explicit.command, "/usr/bin/env");
        assert!(explicit.args.is_empty());
        assert_eq!(explicit.env_remove, ["npm_config_prefix"]);

        let overlay = vec![("npm_config_prefix".to_string(), "/custom".to_string())];
        let user_value = terminal_spawn_spec_for("ws-1", "/bin/zsh", true, false, overlay);
        assert!(user_value.env_remove.is_empty());
        assert_eq!(
            user_value.env,
            [("npm_config_prefix".to_string(), "/custom".to_string())]
        );

        let windows = terminal_spawn_spec_for("ws-1", "pwsh", true, true, Vec::new());
        assert!(windows.args.is_empty());
    }

    #[test]
    fn terminal_env_defaults_missing_or_empty_term() {
        let mut missing = Vec::new();
        ensure_terminal_term(&mut missing, None);
        assert_eq!(
            missing,
            vec![("TERM".to_string(), DEFAULT_TERM.to_string())]
        );

        let mut inherited_empty = Vec::new();
        ensure_terminal_term(&mut inherited_empty, Some(""));
        assert_eq!(
            inherited_empty,
            vec![("TERM".to_string(), DEFAULT_TERM.to_string())]
        );

        let mut empty = vec![("TERM".to_string(), String::new())];
        ensure_terminal_term(&mut empty, Some("screen-256color"));
        assert_eq!(empty, vec![("TERM".to_string(), DEFAULT_TERM.to_string())]);
    }

    #[test]
    fn terminal_env_preserves_explicit_or_inherited_nonempty_term() {
        let mut env = vec![("TERM".to_string(), "screen-256color".to_string())];
        ensure_terminal_term(&mut env, None);
        assert_eq!(
            env,
            vec![("TERM".to_string(), "screen-256color".to_string())]
        );

        let mut inherited = Vec::new();
        ensure_terminal_term(&mut inherited, Some("tmux-256color"));
        assert!(inherited.is_empty());
    }

    #[test]
    fn utf8_len_handles_all_widths() {
        assert_eq!(utf8_len(b'A'), 1);
        assert_eq!(utf8_len(0xC3), 2);
        assert_eq!(utf8_len(0xE2), 3);
        assert_eq!(utf8_len(0xF0), 4);
        assert_eq!(utf8_len(0x80), 1); // bare continuation byte
    }

    #[test]
    fn strip_ansi_removes_sequences_and_keeps_unicode() {
        let input = "\u{1b}[31mred\u{1b}[0m \u{1b}[?25lhide \u{1b}]0;title\u{07}é✓😀";
        assert_eq!(strip_ansi(input), "red hide é✓😀");
    }

    #[test]
    fn bounded_line_decode_tolerates_utf8_and_ansi_split_edges() {
        let snapshot = LineSnapshot {
            bytes: b"\xa9prefix\x1b[31mred\x1b[0m\x1b[32".to_vec(),
            total_lines: 1,
            start_line: 0,
            end_line: 1,
            retained_has_non_whitespace: true,
        };
        assert_eq!(decoded_snapshot_lines(&snapshot), ["�prefixred"]);
    }

    #[test]
    fn bounded_line_decode_strips_osc_sequence_straddling_window_start() {
        let snapshot = LineSnapshot {
            bytes: b"\x1b]0;hidden\nhidden-tail\x07visible-after\nlast".to_vec(),
            total_lines: 4,
            start_line: 2,
            end_line: 4,
            retained_has_non_whitespace: true,
        };
        assert_eq!(decoded_snapshot_lines(&snapshot), ["visible-after", "last"]);
    }

    #[test]
    fn bounded_line_decode_strips_st_terminated_osc_straddling_window_start() {
        let snapshot = LineSnapshot {
            bytes: b"\x1b]0;hidden\nhidden-tail\x1b\\visible-after\nlast".to_vec(),
            total_lines: 4,
            start_line: 2,
            end_line: 4,
            retained_has_non_whitespace: true,
        };
        assert_eq!(decoded_snapshot_lines(&snapshot), ["visible-after", "last"]);
    }

    // ---- credential injection helpers (no spawn) ----

    /// A registry backed by a throwaway config file, optionally overriding
    /// the `exposeGitCredentialToChildren` gate. The returned `TempDir` guard
    /// must be held for the registry's lifetime (it self-cleans on drop).
    fn registry_with_expose(value: Option<bool>) -> (Arc<SettingsRegistry>, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("temp config dir");
        let registry =
            SettingsRegistry::load(dir.path().join("config.toml")).expect("load registry");
        if let Some(v) = value {
            registry
                .apply(&[(
                    "sourceControl.github.exposeGitCredentialToChildren".to_string(),
                    serde_json::json!(v),
                )])
                .expect("apply exposeGitCredentialToChildren");
        }
        (Arc::new(registry), dir)
    }

    /// No registry (minimal/test compositions) reads as off; a wired registry
    /// follows the setting, whose schema default is on.
    #[test]
    fn expose_gate_defaults() {
        assert!(!expose_git_credential(None));
        assert!(expose_git_credential(Some(&registry_with_expose(None).0)));
        assert!(expose_git_credential(Some(
            &registry_with_expose(Some(true)).0
        )));
        assert!(!expose_git_credential(Some(
            &registry_with_expose(Some(false)).0
        )));
    }

    /// The gate short-circuits injection: no registry and setting-off both
    /// yield no pairs.
    #[test]
    fn git_credential_env_respects_gate() {
        assert!(git_credential_env(None, None).is_empty());
        let (off, _guard) = registry_with_expose(Some(false));
        assert!(git_credential_env(Some(&off), None).is_empty());
    }

    /// Gate on ⇒ exactly the single daemon-backed helper pair: the
    /// `GIT_CONFIG_PARAMETERS` entry names `git-credential` and **no**
    /// `INTENT_GIT_GITHUB_TOKEN` pair exists — no token bytes can enter the
    /// child environment (monorepo#884 Phase 2.2).
    #[test]
    fn credential_pairs_are_helper_only_without_token_env() {
        let pairs = credential_pairs(None);
        let keys: Vec<&str> = pairs.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(keys, vec![intent_git::auth::GIT_CONFIG_PARAMETERS_ENV]);
        assert!(
            pairs[0].1.contains("credential.https://github.com.helper="),
            "helper entry present: {}",
            pairs[0].1
        );
        assert!(
            pairs[0].1.contains("git-credential"),
            "daemon-backed helper subcommand named: {}",
            pairs[0].1
        );
        assert!(
            !keys.contains(&intent_git::auth::TOKEN_ENV),
            "no token env pair may be injected"
        );
    }

    /// intent-hq/intent#4142: the combined injection carries the four
    /// commit-identity `GIT_*` vars when the spawn cwd's repository resolves
    /// an identity — ungated (no settings registry needed) — and none when
    /// the cwd is absent. The four vars are unset for the test's lifetime:
    /// the harness itself may inherit them (agent-spawned shells do,
    /// post-#4142), and `commit_identity_env` correctly gap-fills nothing
    /// then (intent-hq/monorepo#4191).
    #[test]
    fn injected_git_env_carries_commit_identity_ungated() {
        let _env = crate::agent_manager::tests::EnvGuard::apply(&[
            ("GIT_AUTHOR_NAME", None),
            ("GIT_AUTHOR_EMAIL", None),
            ("GIT_COMMITTER_NAME", None),
            ("GIT_COMMITTER_EMAIL", None),
        ]);
        let dir =
            std::env::temp_dir().join(format!("intentd-term-identity-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let repo = git2::Repository::init(&dir).unwrap();
        let mut cfg = repo.config().unwrap();
        cfg.set_str("user.name", "Term Test").unwrap();
        cfg.set_str("user.email", "term@example.com").unwrap();
        drop(cfg);

        let env = injected_git_env(None, Some(&dir));
        for key in intent_git::identity::GIT_IDENTITY_ENV_VARS {
            assert!(env.iter().any(|(k, _)| k == key), "missing {key}");
        }
        assert!(env
            .iter()
            .any(|(k, v)| k == "GIT_COMMITTER_EMAIL" && v == "term@example.com"));

        assert!(
            !injected_git_env(None, None)
                .iter()
                .any(|(k, _)| k.starts_with("GIT_AUTHOR") || k.starts_with("GIT_COMMITTER")),
            "no cwd must inject no identity vars"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Caller-supplied env wins: a colliding key drops the injected pair and
    /// caller pairs stay last (later-entry-overrides application order).
    #[test]
    fn overlay_credential_env_user_wins() {
        let credential = vec![
            ("GIT_CONFIG_PARAMETERS".to_string(), "injected".to_string()),
            ("INTENT_GIT_GITHUB_TOKEN".to_string(), "tok".to_string()),
        ];
        let user = vec![
            ("GIT_CONFIG_PARAMETERS".to_string(), "user".to_string()),
            ("OTHER".to_string(), "x".to_string()),
        ];
        let merged = overlay_credential_env(credential, user);
        assert_eq!(
            merged,
            vec![
                ("INTENT_GIT_GITHUB_TOKEN".to_string(), "tok".to_string()),
                ("GIT_CONFIG_PARAMETERS".to_string(), "user".to_string()),
                ("OTHER".to_string(), "x".to_string()),
            ]
        );
        assert_eq!(
            overlay_credential_env(Vec::new(), vec![("A".to_string(), "1".to_string())]),
            vec![("A".to_string(), "1".to_string())]
        );
    }

    // ---- error / not-found paths (no live process) ----

    #[test]
    fn write_rejects_malformed_terminal_id() {
        let h = PtyHost::new();
        assert!(matches!(
            write(&h, "not-a-pty", "Zm9v"),
            Err(Error::NotFound(_))
        ));
        assert!(matches!(
            write(&h, "pty-9999", "Zm9v"),
            Err(Error::NotFound(_))
        ));
    }

    #[test]
    fn resize_unknown_terminal_is_not_found() {
        let h = PtyHost::new();
        assert!(matches!(
            resize(&h, "pty-9999", 80, 24),
            Err(Error::NotFound(_))
        ));
    }

    #[test]
    fn get_buffer_unknown_terminal_is_not_found() {
        let h = PtyHost::new();
        assert!(matches!(
            get_buffer(&h, "pty-9999", None),
            Err(Error::NotFound(_))
        ));
    }

    #[tokio::test]
    async fn kill_malformed_errors_but_unknown_well_formed_is_ok() {
        let h = PtyHost::new();
        assert!(matches!(
            kill(&h, "not-a-pty").await,
            Err(Error::NotFound(_))
        ));
        // A well-formed but absent id resolves, then `PtyHost::kill` reports
        // `false`; the wire op still succeeds (idempotent teardown).
        let ok = kill(&h, "pty-9999").await.unwrap();
        assert_eq!(ok["ok"], json!(true));
    }

    #[test]
    fn read_output_unknown_and_malformed_are_internal() {
        let h = PtyHost::new();
        let w = ws("ws-1");
        assert!(matches!(
            read_output(&h, &w, "not-a-pty", None, false, None),
            Err(Error::Internal(_))
        ));
        assert!(matches!(
            read_output(&h, &w, "pty-9999", None, false, None),
            Err(Error::Internal(_))
        ));
    }

    // ---- lifecycle over real (short-lived / echo-style) PTYs ----

    #[tokio::test]
    async fn create_with_default_shell_lists_then_kills() {
        let pty = host();
        let res = create(
            pty.clone(),
            None,
            None,
            None,
            ws("ws-1"),
            80,
            24,
            None,
            None,
            None,
        )
        .await
        .unwrap();
        let id = term_id(&res);

        let listed = list(pty.as_ref(), &ws("ws-1"), "boot-test").unwrap();
        assert_eq!(listed["daemonBootId"], json!("boot-test"));
        let arr = listed["terminals"].as_array().unwrap();
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["id"], json!(id));
        assert_eq!(arr[0]["name"], json!("Terminal"));
        assert_eq!(arr[0]["isExecutingCommand"], json!(true));

        kill(pty.as_ref(), &id).await.unwrap();
        let empty = poll_until(
            || {
                let v = list(pty.as_ref(), &ws("ws-1"), "boot-test").unwrap();
                v["terminals"]
                    .as_array()
                    .filter(|a| a.is_empty())
                    .map(|_| ())
            },
            TIMEOUT,
        )
        .await;
        assert!(
            empty.is_some(),
            "terminal must drop from the list after kill"
        );
    }

    #[tokio::test]
    async fn create_records_cwd_in_list() {
        let pty = host();
        let res = create(
            pty.clone(),
            None,
            None,
            None,
            ws("ws-1"),
            80,
            24,
            Some("/".to_string()),
            Some("cat".to_string()),
            None,
        )
        .await
        .unwrap();
        let id = term_id(&res);

        let listed = list(pty.as_ref(), &ws("ws-1"), "boot-test").unwrap();
        let entry = &listed["terminals"].as_array().unwrap()[0];
        assert_eq!(entry["cwd"], json!("/"));

        kill(pty.as_ref(), &id).await.unwrap();
    }

    /// `list` surfaces the spawn-time display name; unnamed PTYs fall back to
    /// the `"Terminal"` constant.
    #[tokio::test]
    async fn list_returns_spawn_name_or_default() {
        let pty = host();
        let mut named = SpawnSpec::new("ws-named", "cat");
        named.name = Some(crate::SETUP_TERMINAL_NAME.to_string());
        let named_id = pty.spawn(named).unwrap().to_string();

        let res = create(
            pty.clone(),
            None,
            None,
            None,
            ws("ws-named"),
            80,
            24,
            None,
            Some("cat".to_string()),
            None,
        )
        .await
        .unwrap();
        let unnamed_id = term_id(&res);

        let listed = list(pty.as_ref(), &ws("ws-named"), "boot-test").unwrap();
        let arr = listed["terminals"].as_array().unwrap();
        let by_id = |id: &str| {
            arr.iter()
                .find(|e| e["id"] == json!(id))
                .expect("terminal listed")
        };
        assert_eq!(by_id(&named_id)["name"], json!("Setup Script"));
        assert_eq!(by_id(&unnamed_id)["name"], json!("Terminal"));

        kill(pty.as_ref(), &named_id).await.unwrap();
        kill(pty.as_ref(), &unnamed_id).await.unwrap();
    }

    /// `list` always returns the `{ terminals, daemonBootId }` envelope —
    /// even for a workspace with no PTYs — echoing the caller's boot id
    /// verbatim (monorepo#1334).
    #[test]
    fn list_returns_envelope_even_when_empty() {
        let pty = host();
        let listed = list(pty.as_ref(), &ws("ws-empty"), "boot-abc").unwrap();
        assert_eq!(
            listed,
            json!({ "terminals": [], "daemonBootId": "boot-abc" })
        );
    }

    /// The setup script wrapper appends a newline-prefixed completion summary
    /// after the script's own output (a blank separator line when the script's
    /// output ends with a newline) and preserves its exit code.
    #[test]
    fn setup_wrapper_appends_summary_and_preserves_exit_code() {
        let run = |body: &str| {
            let path =
                std::env::temp_dir().join(format!("intentd-term-wrap-{}.sh", uuid::Uuid::new_v4()));
            std::fs::write(&path, body).expect("write script");
            let out = std::process::Command::new("/bin/sh")
                .args(["-c", crate::SETUP_SCRIPT_WRAPPER, "sh"])
                .arg(&path)
                .output()
                .expect("run wrapper");
            let _ = std::fs::remove_file(&path);
            out
        };

        let ok = run("echo hello-from-script\n");
        assert_eq!(ok.status.code(), Some(0));
        let stdout = String::from_utf8_lossy(&ok.stdout);
        assert!(
            stdout.contains("hello-from-script\n\nSetup script completed in "),
            "blank line then summary must follow the script output: {stdout:?}"
        );
        let last = stdout.trim_end().lines().last().unwrap();
        assert!(
            last.starts_with("Setup script completed in "),
            "got {last:?}"
        );
        assert!(last.ends_with("s (exit code 0)"), "got {last:?}");

        let failed = run("exit 3\n");
        assert_eq!(failed.status.code(), Some(3), "script exit code preserved");
        let stdout = String::from_utf8_lossy(&failed.stdout);
        let last = stdout.trim_end().lines().last().unwrap();
        assert!(last.starts_with("Setup script failed in "), "got {last:?}");
        assert!(last.ends_with("s (exit code 3)"), "got {last:?}");
    }

    /// A workspace row with an optional worktree path, for default-cwd tests.
    fn workspace_row(id: &WorkspaceId, worktree: Option<&PathBuf>) -> Workspace {
        let ts = now_iso();
        Workspace {
            id: id.clone(),
            title: "WS".to_string(),
            branch: "main".to_string(),
            base_ref: None,
            base_commit_sha: None,
            status: WorkspaceStatus::Active,
            status_message: None,
            status_image_asset_id: None,
            activity: WorkspaceActivity::Idle,
            attention: WorkspaceAttention::None,
            created_at: ts.clone(),
            updated_at: ts,
            last_activity: None,
            tags: vec![],
            path: None,
            repository_path: None,
            repository_owner: None,
            repository_name: None,
            worktree_path: worktree.map(|p| p.display().to_string()),
            scope: None,
            skip_worktree: false,
            setup_script: None,
            is_remote: false,
            default_model: None,
            pr_number: None,
            pr_url: None,
            pr_status: None,
            active_pull_request: None,
            pull_requests: None,
            context_links: None,
            archived: false,
            archived_at: None,
            task_stats: None,
            agent_summary: None,
            diff_summary: None,
            token_usage: None,
            cow_supported: None,
            display_status: None,
            waiting: false,
            checkout_mode: None,
            disk_usage: None,
            pending_delete_at: None,
        }
    }

    #[tokio::test]
    async fn default_cwd_worktree_present_vs_absent() {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let with = ws("ws-with-wt");
        let without = ws("ws-without-wt");
        let worktree = PathBuf::from("/tmp/intentd-term-fake-worktree");
        store
            .insert_workspace(&workspace_row(&with, Some(&worktree)))
            .await
            .expect("insert ws with worktree");
        store
            .insert_workspace(&workspace_row(&without, None))
            .await
            .expect("insert ws without worktree");

        assert_eq!(default_cwd(&store, &with, None).await, Some(worktree));
        assert_eq!(default_cwd(&store, &without, None).await, None);
        // Unknown workspace rows fall back without erroring.
        assert_eq!(default_cwd(&store, &ws("ws-missing"), None).await, None);
    }

    #[tokio::test]
    async fn create_defaults_cwd_to_worktree_root() {
        let pty = host();
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let wsid = ws("ws-wt");
        let worktree =
            std::env::temp_dir().join(format!("intentd-term-wt-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&worktree).expect("mkdir worktree");
        store
            .insert_workspace(&workspace_row(&wsid, Some(&worktree)))
            .await
            .expect("insert workspace");

        let res = create(
            pty.clone(),
            None,
            Some(store),
            None,
            wsid.clone(),
            80,
            24,
            None,
            Some("cat".to_string()),
            None,
        )
        .await
        .unwrap();
        let id = term_id(&res);

        let listed = list(pty.as_ref(), &wsid, "boot-test").unwrap();
        let entry = &listed["terminals"].as_array().unwrap()[0];
        assert_eq!(entry["cwd"], json!(worktree.display().to_string()));

        kill(pty.as_ref(), &id).await.unwrap();
        let _ = std::fs::remove_dir_all(&worktree);
    }

    #[tokio::test]
    async fn create_streams_data_events_for_child_output() {
        let pty = host();
        let (_tmp, bus) = bus().await;
        let mut sub = bus.subscribe(SubscriptionFilter::default());

        let res = create(
            pty.clone(),
            Some(bus),
            None,
            None,
            ws("ws-1"),
            80,
            24,
            None,
            Some("cat".to_string()),
            None,
        )
        .await
        .unwrap();
        let id = term_id(&res);

        write(
            pty.as_ref(),
            &id,
            &base64::engine::general_purpose::STANDARD.encode(b"HELLO_STREAM\n"),
        )
        .unwrap();

        let acc = collect_data_until(&mut sub, b"HELLO_STREAM", TIMEOUT).await;
        assert!(
            contains_sub(&acc, b"HELLO_STREAM"),
            "terminal:data must carry the child's output"
        );

        kill(pty.as_ref(), &id).await.unwrap();
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn terminal_write_del_emits_erase_redraw_on_bus_without_launcher_term() {
        let pty = host();
        let (_tmp, bus) = bus().await;
        let mut sub = bus.subscribe(SubscriptionFilter::default());
        let zdotdir = tempfile::tempdir().expect("isolated zsh config dir");
        let env = std::collections::BTreeMap::from([
            // Electron-launched daemons can inherit an empty terminal type.
            // `create` must coerce it before zsh initializes ZLE.
            ("TERM".to_string(), String::new()),
            ("ZDOTDIR".to_string(), zdotdir.path().display().to_string()),
        ]);
        let res = create(
            pty.clone(),
            Some(bus),
            None,
            None,
            ws("ws-erase"),
            80,
            24,
            None,
            Some("/bin/zsh".to_string()),
            Some(env),
        )
        .await
        .unwrap();
        let id = term_id(&res);
        let mut kill_guard = PtyKillGuard::new(pty.clone(), resolve(&id).unwrap());

        let ready = collect_data_until(&mut sub, b"\x1b[?2004h", TIMEOUT).await;
        assert!(
            contains_sub(&ready, b"\x1b[?2004h"),
            "zsh prompt must be ready before probing ZLE; got {ready:?}"
        );

        write(
            pty.as_ref(),
            &id,
            &base64::engine::general_purpose::STANDARD.encode(b"ab\x7f"),
        )
        .unwrap();

        let acc = collect_data_until(&mut sub, b"\x08", Duration::from_secs(1)).await;
        assert!(
            contains_sub(&acc, b"\x08"),
            "zsh must emit a cursor-left redraw for DEL instead of a visible space; got {acc:?}"
        );

        kill(pty.as_ref(), &id).await.unwrap();
        kill_guard.disarm();
    }

    #[tokio::test]
    async fn natural_exit_emits_exit_event_with_code() {
        let pty = host();
        let (_tmp, bus) = bus().await;
        let mut sub = bus.subscribe(SubscriptionFilter::default());

        let res = create(
            pty.clone(),
            Some(bus),
            None,
            None,
            ws("ws-1"),
            80,
            24,
            None,
            Some("echo".to_string()),
            None,
        )
        .await
        .unwrap();
        let id = term_id(&res);

        let exit = wait_for_event(&mut sub, TERMINAL_EXIT, TIMEOUT)
            .await
            .expect("terminal:exit event");
        assert_eq!(exit.data["terminalId"], json!(id));
        assert_eq!(exit.data["exitCode"], json!(0));
        assert!(exit.data["signal"].is_null());

        let listed = list(pty.as_ref(), &ws("ws-1"), "boot-test").unwrap();
        assert!(
            listed["terminals"]
                .as_array()
                .is_some_and(|terminals| terminals.iter().all(|terminal| terminal["id"] != id)),
            "naturally exited terminal must not remain in terminal.list: {listed}"
        );
        assert!(
            get_buffer(pty.as_ref(), &id, None).is_ok(),
            "post-exit output must remain available until release"
        );

        kill(pty.as_ref(), &id).await.unwrap();
    }

    #[tokio::test]
    async fn kill_emits_exit_event_without_code() {
        let pty = host();
        let (_tmp, bus) = bus().await;
        let mut sub = bus.subscribe(SubscriptionFilter::default());

        let res = create(
            pty.clone(),
            Some(bus),
            None,
            None,
            ws("ws-1"),
            80,
            24,
            None,
            Some("cat".to_string()),
            None,
        )
        .await
        .unwrap();
        let id = term_id(&res);

        kill(pty.as_ref(), &id).await.unwrap();

        let exit = wait_for_event(&mut sub, TERMINAL_EXIT, TIMEOUT)
            .await
            .expect("terminal:exit event");
        assert_eq!(exit.data["terminalId"], json!(id));
        // After a kill the session is gone, so no exit code can be latched.
        assert!(exit.data["exitCode"].is_null());
        assert!(exit.data["signal"].is_null());
    }

    /// Regression (paste/echo throughput): `terminal:data` is transient —
    /// broadcast to live subscribers but never persisted to the event table —
    /// while `terminal:exit` stays durable and never overtakes data chunks.
    #[tokio::test]
    async fn data_is_transient_exit_is_durable() {
        let pty = host();
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let bus = EventBus::new(store.clone());
        let mut sub = bus.subscribe(SubscriptionFilter::default());

        let res = create(
            pty.clone(),
            Some(bus),
            None,
            None,
            ws("ws-1"),
            80,
            24,
            None,
            Some("cat".to_string()),
            None,
        )
        .await
        .unwrap();
        let id = term_id(&res);

        write(
            pty.as_ref(),
            &id,
            &base64::engine::general_purpose::STANDARD.encode(b"TRANSIENT_MARK\n"),
        )
        .unwrap();
        let acc = collect_data_until(&mut sub, b"TRANSIENT_MARK", TIMEOUT).await;
        assert!(
            contains_sub(&acc, b"TRANSIENT_MARK"),
            "terminal:data must reach live subscribers"
        );

        kill(pty.as_ref(), &id).await.unwrap();
        // `publish` commits before broadcasting, so once the subscriber sees
        // the exit the durable row is already queryable.
        wait_for_event(&mut sub, TERMINAL_EXIT, TIMEOUT)
            .await
            .expect("terminal:exit event");

        let data_rows = store
            .query_events(&intent_store::EventQuery {
                event_types: vec![TERMINAL_DATA.to_string()],
                ..Default::default()
            })
            .await
            .expect("query terminal:data");
        assert!(
            data_rows.is_empty(),
            "terminal:data must not be persisted (transient publish path)"
        );
        let exit_rows = store
            .query_events(&intent_store::EventQuery {
                event_types: vec![TERMINAL_EXIT.to_string()],
                ..Default::default()
            })
            .await
            .expect("query terminal:exit");
        assert_eq!(
            exit_rows.len(),
            1,
            "terminal:exit stays durable exactly once"
        );
    }

    #[tokio::test]
    async fn write_resize_and_get_buffer_roundtrip() {
        let pty = host();
        let res = create(
            pty.clone(),
            None,
            None,
            None,
            ws("ws-1"),
            80,
            24,
            None,
            Some("cat".to_string()),
            None,
        )
        .await
        .unwrap();
        let id = term_id(&res);

        write(
            pty.as_ref(),
            &id,
            &base64::engine::general_purpose::STANDARD.encode(b"buffer-test\n"),
        )
        .unwrap();
        write(pty.as_ref(), &id, "!!!not base64!!!").unwrap_err();
        resize(pty.as_ref(), &id, 120, 50).unwrap();

        let full = poll_until(
            || {
                let v = get_buffer(pty.as_ref(), &id, None).ok()?;
                let bytes = decode(v["data"].as_str()?);
                contains_sub(&bytes, b"buffer-test").then_some(bytes)
            },
            TIMEOUT,
        )
        .await
        .expect("echoed output must reach the buffer");
        assert!(contains_sub(&full, b"buffer-test"));

        let capped = get_buffer(pty.as_ref(), &id, Some(4)).unwrap();
        assert_eq!(
            decode(capped["data"].as_str().unwrap()),
            full[full.len() - 4..]
        );
        let zero = get_buffer(pty.as_ref(), &id, Some(0)).unwrap();
        assert!(decode(zero["data"].as_str().unwrap()).is_empty());
        let exact = get_buffer(
            pty.as_ref(),
            &id,
            Some(i64::try_from(full.len()).expect("test buffer length fits in i64")),
        )
        .unwrap();
        assert_eq!(decode(exact["data"].as_str().unwrap()), full);
        let oversized = get_buffer(pty.as_ref(), &id, Some(i64::MAX)).unwrap();
        assert_eq!(decode(oversized["data"].as_str().unwrap()), full);
        let legacy_negative = get_buffer(pty.as_ref(), &id, Some(-1)).unwrap();
        assert_eq!(decode(legacy_negative["data"].as_str().unwrap()), full);

        kill(pty.as_ref(), &id).await.unwrap();
    }

    #[tokio::test]
    async fn write_rejects_invalid_base64() {
        let pty = host();
        let res = create(
            pty.clone(),
            None,
            None,
            None,
            ws("ws-1"),
            80,
            24,
            None,
            Some("cat".to_string()),
            None,
        )
        .await
        .unwrap();
        let id = term_id(&res);
        assert!(matches!(
            write(pty.as_ref(), &id, "*not*valid*"),
            Err(Error::InvalidParams(_))
        ));
        kill(pty.as_ref(), &id).await.unwrap();
    }

    // ---- read_output formatting / pagination ----

    #[tokio::test]
    async fn read_output_empty_returns_placeholder() {
        let pty = host();
        let res = create(
            pty.clone(),
            None,
            None,
            None,
            ws("ws-1"),
            80,
            24,
            None,
            Some("cat".to_string()),
            None,
        )
        .await
        .unwrap();
        let id = term_id(&res);
        let out = read_output(pty.as_ref(), &ws("ws-1"), &id, None, false, None).unwrap();
        assert_eq!(
            out,
            Value::String("Terminal has no output yet.".to_string())
        );
        kill(pty.as_ref(), &id).await.unwrap();
    }

    #[tokio::test]
    async fn read_output_wrong_workspace_is_internal() {
        let pty = host();
        let res = create(
            pty.clone(),
            None,
            None,
            None,
            ws("ws-a"),
            80,
            24,
            None,
            Some("cat".to_string()),
            None,
        )
        .await
        .unwrap();
        let id = term_id(&res);
        let err = read_output(pty.as_ref(), &ws("ws-b"), &id, None, false, None).unwrap_err();
        assert!(matches!(err, Error::Internal(ref m) if m.contains("does not belong")));
        kill(pty.as_ref(), &id).await.unwrap();
    }

    #[tokio::test]
    async fn read_output_formats_header_and_separator() {
        let pty = host();
        let res = create(
            pty.clone(),
            None,
            None,
            None,
            ws("ws-1"),
            80,
            24,
            None,
            Some("cat".to_string()),
            None,
        )
        .await
        .unwrap();
        let id = term_id(&res);
        write(
            pty.as_ref(),
            &id,
            &base64::engine::general_purpose::STANDARD.encode(b"single-line\n"),
        )
        .unwrap();

        let text = poll_until(
            || {
                let v = read_output(pty.as_ref(), &ws("ws-1"), &id, None, false, None).ok()?;
                let s = v.as_str()?.to_string();
                s.contains("single-line").then_some(s)
            },
            TIMEOUT,
        )
        .await
        .expect("formatted output");
        assert!(text.contains(&format!("Terminal {id}")));
        assert!(text.contains('\u{2500}'));
        assert!(!text.contains("showing last"));
        assert!(!text.contains('\u{1b}'));

        kill(pty.as_ref(), &id).await.unwrap();
    }

    #[tokio::test]
    async fn read_output_truncates_with_header() {
        let pty = host();
        let res = create(
            pty.clone(),
            None,
            None,
            None,
            ws("ws-1"),
            80,
            24,
            None,
            Some("cat".to_string()),
            None,
        )
        .await
        .unwrap();
        let id = term_id(&res);
        write(
            pty.as_ref(),
            &id,
            &base64::engine::general_purpose::STANDARD.encode(b"l1\nl2\nl3\nl4\nl5\n"),
        )
        .unwrap();

        let text = poll_until(
            || {
                let v = read_output(pty.as_ref(), &ws("ws-1"), &id, Some(2), false, None).ok()?;
                let s = v.as_str()?.to_string();
                s.contains("showing last 2 of").then_some(s)
            },
            TIMEOUT,
        )
        .await
        .expect("truncated header");
        assert!(text.contains("showing last 2 of"));

        kill(pty.as_ref(), &id).await.unwrap();
    }

    #[tokio::test]
    async fn read_output_reports_exact_line_total_after_exit() {
        let pty = host();
        let mut spec = SpawnSpec::new("ws-1", "sh");
        spec.args = vec![
            "-c".to_string(),
            "printf '\x1b[31mone\x1b[0m\\ntwo\\nthree'".to_string(),
        ];
        let id = pty.spawn(spec).unwrap();
        pty.wait(id).await.unwrap();

        let text = poll_until(
            || {
                let value = read_output(
                    pty.as_ref(),
                    &ws("ws-1"),
                    &id.to_string(),
                    Some(2),
                    false,
                    None,
                )
                .ok()?;
                let text = value.as_str()?.to_string();
                text.contains("three").then_some(text)
            },
            TIMEOUT,
        )
        .await
        .expect("post-exit output drains into scrollback");
        assert!(text.contains("[showing last 2 of 3 lines]"), "{text}");
        assert!(text.ends_with("two\r\nthree") || text.ends_with("two\nthree"));
        assert!(!text.contains('\u{1b}'));

        kill(pty.as_ref(), &id.to_string()).await.unwrap();
    }

    #[tokio::test]
    async fn read_output_paginates_with_token() {
        let pty = host();
        let res = create(
            pty.clone(),
            None,
            None,
            None,
            ws("ws-1"),
            80,
            24,
            None,
            Some("cat".to_string()),
            None,
        )
        .await
        .unwrap();
        let id = term_id(&res);
        write(
            pty.as_ref(),
            &id,
            &base64::engine::general_purpose::STANDARD.encode(b"p1\np2\np3\np4\np5\np6\n"),
        )
        .unwrap();

        let page1 = poll_until(
            || {
                let v = read_output(pty.as_ref(), &ws("ws-1"), &id, Some(2), true, None).ok()?;
                let items = v.get("items")?.as_array()?;
                (items.len() == 2 && v.get("nextToken")?.is_string()).then(|| v.clone())
            },
            TIMEOUT,
        )
        .await
        .expect("first page with continuation token");

        let token = page1["nextToken"].as_str().unwrap().to_string();
        write(
            pty.as_ref(),
            &id,
            &base64::engine::general_purpose::STANDARD.encode(b"new-tail\n"),
        )
        .unwrap();
        poll_until(
            || {
                let bytes = pty.scrollback(PtyId::parse(&id)?).ok()?;
                contains_sub(&bytes, b"new-tail").then_some(())
            },
            TIMEOUT,
        )
        .await
        .expect("concurrent append reaches scrollback");
        let page2 =
            read_output(pty.as_ref(), &ws("ws-1"), &id, Some(2), false, Some(&token)).unwrap();
        assert!(!page2["items"].as_array().unwrap().is_empty());
        assert!(page2["items"]
            .as_array()
            .unwrap()
            .iter()
            .all(|item| !item.as_str().unwrap_or_default().contains("new-tail")));

        kill(pty.as_ref(), &id).await.unwrap();
    }

    // ---- ACP `PtyTerminalHost` adapter ----

    /// ACP terminal create → output → `wait_for_exit` happy path.
    ///
    /// Load-independent (monorepo#573): the child prints the marker and then
    /// stays alive (`exec cat`) until the test has *observed* the output, so
    /// the host's reader thread can never lose the race where a fast-exiting
    /// child closes the PTY slave before the first `read()` and macOS discards
    /// the buffered output. Only then does the test send canonical-mode EOF
    /// (`^D`) so `cat` — and thus the child — exits 0 naturally, awaited with
    /// a generous bounded deadline.
    #[tokio::test]
    async fn acp_create_output_and_wait_for_exit() {
        let pty = host();
        let adapter = PtyTerminalHost::new(pty.clone(), None);
        let params = TerminalCreateParams {
            session_id: "sess-1".to_string(),
            command: "sh".to_string(),
            args: vec![
                "-c".to_string(),
                "printf 'acp-output\\n'; exec cat".to_string(),
            ],
            env: Vec::new(),
            cwd: None,
            output_byte_limit: None,
        };
        let id = adapter.create(params).await.unwrap();
        let pty_id = PtyId::parse(&id).expect("wire id parses");

        let deadline = Instant::now() + LONG_TIMEOUT;
        loop {
            let info = adapter.output(id.clone()).await.unwrap();
            if info.output.contains("acp-output") {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "ACP output must surface the child's stdout; got: {:?}",
                info.output
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        pty.write(pty_id, b"\x04").unwrap();

        let exit = tokio::time::timeout(LONG_TIMEOUT, adapter.wait_for_exit(id))
            .await
            .expect("child exits within the generous deadline")
            .unwrap();
        assert_eq!(exit.exit_code, Some(0));
        assert!(exit.signal.is_none());
    }

    /// intent-hq/intent#4142: a `terminal/create` that omits `cwd` falls back
    /// to the agent session's cwd for both the spawn directory and the git
    /// env resolution, so the commit identity resolved from the session's
    /// repository still reaches the child. The four `GIT_*` identity vars are
    /// unset for the test's lifetime: the harness itself may inherit them
    /// (agent-spawned shells do, post-#4142), injection is gap-filling only,
    /// and the PTY child would print the inherited value instead
    /// (intent-hq/monorepo#4191).
    #[tokio::test]
    async fn acp_create_without_cwd_falls_back_to_session_cwd_with_identity() {
        let _env = crate::agent_manager::tests::EnvGuard::apply(&[
            ("GIT_AUTHOR_NAME", None),
            ("GIT_AUTHOR_EMAIL", None),
            ("GIT_COMMITTER_NAME", None),
            ("GIT_COMMITTER_EMAIL", None),
        ]);
        let dir =
            std::env::temp_dir().join(format!("intentd-acp-session-cwd-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let repo = git2::Repository::init(&dir).unwrap();
        let mut cfg = repo.config().unwrap();
        cfg.set_str("user.name", "Session Test").unwrap();
        cfg.set_str("user.email", "session@example.com").unwrap();
        drop(cfg);
        let dir = dir.canonicalize().unwrap();

        let pty = host();
        let adapter = PtyTerminalHost::with_shell_mode(pty.clone(), None, false, Some(dir.clone()));
        let params = TerminalCreateParams {
            session_id: "sess-cwd".to_string(),
            command: "sh".to_string(),
            args: vec![
                "-c".to_string(),
                "printf 'cwd=%s email=%s\\n' \"$(pwd)\" \"$GIT_AUTHOR_EMAIL\"".to_string(),
            ],
            env: Vec::new(),
            cwd: None,
            output_byte_limit: None,
        };
        let id = adapter.create(params).await.unwrap();
        let exit = tokio::time::timeout(LONG_TIMEOUT, adapter.wait_for_exit(id.clone()))
            .await
            .expect("child exits within the deadline")
            .unwrap();
        assert_eq!(exit.exit_code, Some(0));
        let out = adapter.output(id).await.unwrap();
        assert!(
            out.output.contains(&format!("cwd={}", dir.display())),
            "spawn falls back to the session cwd; got {:?}",
            out.output
        );
        assert!(
            out.output.contains("email=session@example.com"),
            "identity resolved from the session cwd's repo; got {:?}",
            out.output
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn acp_create_with_byte_limit_then_release() {
        let pty = host();
        let adapter = PtyTerminalHost::new(pty.clone(), None);
        let params = TerminalCreateParams {
            session_id: "sess-2".to_string(),
            command: "cat".to_string(),
            args: Vec::new(),
            env: Vec::new(),
            cwd: None,
            output_byte_limit: Some(16),
        };
        let id = adapter.create(params).await.unwrap();
        adapter.release(id.clone()).await.unwrap();
        // Released → the id no longer resolves to a tracked session.
        assert!(matches!(
            adapter.output(id).await,
            Err(AcpError::Terminal(_))
        ));
    }

    #[tokio::test]
    async fn acp_unknown_terminal_errors_on_every_op() {
        let pty = host();
        let adapter = PtyTerminalHost::new(pty, None);
        assert!(matches!(
            adapter.output("bad".to_string()).await,
            Err(AcpError::Terminal(_))
        ));
        assert!(matches!(
            adapter.wait_for_exit("bad".to_string()).await,
            Err(AcpError::Terminal(_))
        ));
        assert!(matches!(
            adapter.release("bad".to_string()).await,
            Err(AcpError::Terminal(_))
        ));
        assert!(matches!(
            adapter.kill("bad".to_string()).await,
            Err(AcpError::Terminal(_))
        ));
    }

    #[tokio::test]
    async fn acp_shell_mode_accepts_packed_shell_command() {
        let pty = host();
        let adapter = PtyTerminalHost::with_shell_mode(pty.clone(), None, true, None);
        let params = TerminalCreateParams {
            session_id: "sess-shell".to_string(),
            // Grok-style packed shell line (would ENOENT under argv-only
            // spawn). `/bin/sh` rather than a hard-coded `/bin/bash` so the
            // test is portable to hosts without bash.
            command: "/bin/sh -c 'printf shell-mode-ok\\n'".to_string(),
            args: Vec::new(),
            env: Vec::new(),
            cwd: None,
            output_byte_limit: None,
        };
        let id = adapter.create(params).await.unwrap();
        let exit = adapter.wait_for_exit(id.clone()).await.unwrap();
        assert_eq!(exit.exit_code, Some(0));
        let out = adapter.output(id).await.unwrap();
        assert!(
            out.output.contains("shell-mode-ok"),
            "expected shell-mode-ok in output, got {:?}",
            out.output
        );
    }

    #[tokio::test]
    async fn acp_kill_terminates_tracked_terminal() {
        let pty = host();
        let adapter = PtyTerminalHost::new(pty.clone(), None);
        let params = TerminalCreateParams {
            session_id: "sess-3".to_string(),
            command: "cat".to_string(),
            args: Vec::new(),
            env: Vec::new(),
            cwd: None,
            output_byte_limit: None,
        };
        let id = adapter.create(params).await.unwrap();
        adapter.kill(id.clone()).await.unwrap();
        assert!(matches!(
            adapter.output(id).await,
            Err(AcpError::Terminal(_))
        ));
    }
}
