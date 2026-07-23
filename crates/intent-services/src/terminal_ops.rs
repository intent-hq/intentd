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

use std::path::PathBuf;
use std::sync::Arc;

use base64::Engine as _;
use intent_acp::{
    AcpError, AcpResult, TerminalCreateParams, TerminalExitInfo, TerminalHost, TerminalOutputInfo,
};
use intent_core::events::{TERMINAL_DATA, TERMINAL_EXIT};
use intent_core::{now_iso, BoxFuture, Error, Result, WorkspaceId};
use intent_pty::{PtyExit, PtyHost, PtyId, PtySize, SpawnSpec};
use intent_store::{NewEvent, Store};
use serde_json::{json, Value};
use std::time::Duration;

use tokio::sync::broadcast::error::{RecvError, TryRecvError};

use crate::events::EventBus;
use crate::{publish_event, system_actor};

/// How often the output streamer polls for natural process exit. The broadcast
/// sender lives in the (still-tracked) session, so a child that exits on its own
/// — without a `terminal.kill` — never closes the live channel; polling the
/// child is what lets the streamer emit `terminal:exit` in that case.
const EXIT_POLL: Duration = Duration::from_millis(25);

/// The default shell spawned when `terminal.create` omits a command. Mirrors the
/// ancestor's reliance on the user's login shell (`$SHELL`, then `/bin/sh`).
fn default_shell() -> String {
    std::env::var("SHELL").unwrap_or_else(|_| "/bin/sh".to_string())
}

/// Resolve a wire terminal id (`pty-{n}`) to a [`PtyId`], or `NotFound`.
fn resolve(terminal_id: &str) -> Result<PtyId> {
    PtyId::parse(terminal_id).ok_or_else(|| Error::NotFound(format!("terminal {terminal_id}")))
}

/// Spawn a workspace-scoped PTY for `terminal.create` and begin streaming its
/// output onto the bus; returns `{ terminalId }`. `env` is an optional
/// overlay layered onto the daemon's inherited environment (`portable-pty`
/// inherits by default), so callers can pass per-terminal variables through
/// without dropping them. When `cwd` is omitted the PTY spawns in the
/// workspace's worktree root (see [`default_cwd`]); an explicit `cwd` always
/// wins.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn create(
    pty: Arc<PtyHost>,
    bus: Option<EventBus>,
    store: Option<Store>,
    workspace_id: WorkspaceId,
    cols: u16,
    rows: u16,
    cwd: Option<String>,
    command: Option<String>,
    env: Option<std::collections::BTreeMap<String, String>>,
) -> Result<Value> {
    let mut spec = SpawnSpec::new(workspace_id.as_str(), command.unwrap_or_else(default_shell));
    spec.size = PtySize { rows, cols };
    spec.cwd = match cwd {
        Some(cwd) => Some(PathBuf::from(cwd)),
        None => match store.as_ref() {
            Some(store) => default_cwd(store, &workspace_id, None).await,
            None => None,
        },
    };
    if let Some(map) = env {
        spec.env = map.into_iter().collect();
    }
    let pty_id = pty.spawn(spec)?;
    let terminal_id = pty_id.to_string();
    spawn_output_stream(pty, bus, workspace_id, pty_id, terminal_id.clone());
    Ok(json!({ "terminalId": terminal_id }))
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
    let mut bytes = pty.scrollback(id)?;
    if let Some(max) = max_bytes.and_then(|n| usize::try_from(n).ok()) {
        if bytes.len() > max {
            bytes = bytes.split_off(bytes.len() - max);
        }
    }
    let data = base64::engine::general_purpose::STANDARD.encode(&bytes);
    Ok(json!({ "terminalId": terminal_id, "data": data }))
}

/// The workspace's live terminals as a bare array
/// `[{ id, name, cwd, isExecutingCommand }]` (TS `ws.terminal.list`). `name` is
/// the display name given at spawn (`SpawnSpec::name`, e.g. "Setup Script"),
/// else the constant `"Terminal"`; `cwd` is the working directory resolved at
/// spawn; `isExecutingCommand` is the child's liveness (the spawned process is
/// the running command).
pub(crate) fn list(pty: &PtyHost, workspace_id: &WorkspaceId) -> Result<Value> {
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
            let is_executing = info.as_ref().map(|i| i.alive).unwrap_or(false);
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
    Ok(Value::Array(terminals))
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
    page_token: Option<String>,
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

    let bytes = pty.scrollback(id)?;
    let raw = String::from_utf8_lossy(&bytes);

    // TA-2 / §5.5 opt-in pagination: when engaged, return the historical
    // scrollback as a `{ items, nextToken }` envelope of ANSI-stripped lines
    // ordered newest→oldest, with an opaque append-stable continuation token.
    // Absent the opt-in, preserve the legacy bare formatted string verbatim.
    if paginate || page_token.is_some() {
        return Ok(crate::pagination::paginate_text_lines(
            &strip_ansi(&raw),
            max_lines,
            page_token.as_deref(),
        ));
    }

    if raw.trim().is_empty() {
        return Ok(Value::String("Terminal has no output yet.".to_string()));
    }

    let clean = strip_ansi(&raw);
    let lines: Vec<&str> = clean.split('\n').collect();
    let max_line_count = max_lines.unwrap_or(200).clamp(1, 10000) as usize;
    let mut output_lines: Vec<&str> = if lines.len() > max_line_count {
        lines[lines.len() - max_line_count..].to_vec()
    } else {
        lines.clone()
    };
    while output_lines
        .last()
        .map(|l| l.trim().is_empty())
        .unwrap_or(false)
    {
        output_lines.pop();
    }

    let truncated = lines.len() > max_line_count;
    let cwd = info.cwd.unwrap_or_default();
    let header = if truncated {
        format!(
            "Terminal {terminal_id} (cwd: {cwd}) [showing last {max_line_count} of {} lines]",
            lines.len()
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
                // OSC: ESC ] ... BEL(0x07)
                Some(b']') => {
                    i += 2;
                    while i < bytes.len() && bytes[i] != 0x07 {
                        i += 1;
                    }
                    i += 1; // consume BEL
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
    let attachment = match pty.attach(pty_id) {
        Ok(a) => a,
        Err(_) => return,
    };
    tokio::spawn(async move {
        let mut live = attachment.live;
        // Emit any output captured between spawn and attach exactly once, then
        // tail live chunks (the host guarantees history XOR live, never both).
        if !attachment.backlog.is_empty() {
            emit_data(&bus, &workspace_id, &terminal_id, &attachment.backlog).await;
        }
        loop {
            tokio::select! {
                recv = live.recv() => match recv {
                    Ok(chunk) => emit_data(&bus, &workspace_id, &terminal_id, &chunk).await,
                    Err(RecvError::Lagged(_)) => continue,
                    // A `terminal.kill` tore down the session and dropped the
                    // sender; the process is gone.
                    Err(RecvError::Closed) => break,
                },
                _ = tokio::time::sleep(EXIT_POLL) => {
                    if matches!(pty.try_exit(pty_id), Ok(Some(_))) {
                        // Reaped: drain any output the reader flushed just before
                        // EOF, then stop tailing.
                        drain_pending(&mut live, &bus, &workspace_id, &terminal_id).await;
                        break;
                    }
                }
            }
        }
        let exit = pty.try_exit(pty_id).ok().flatten();
        emit_exit(&bus, &workspace_id, &terminal_id, exit).await;
    });
}

/// Flush any output buffered on the live channel without blocking (used once the
/// child has exited so trailing output still streams before `terminal:exit`).
async fn drain_pending(
    live: &mut tokio::sync::broadcast::Receiver<Arc<Vec<u8>>>,
    bus: &Option<EventBus>,
    workspace_id: &WorkspaceId,
    terminal_id: &str,
) {
    loop {
        match live.try_recv() {
            Ok(chunk) => emit_data(bus, workspace_id, terminal_id, &chunk).await,
            Err(TryRecvError::Lagged(_)) => continue,
            Err(TryRecvError::Empty) | Err(TryRecvError::Closed) => break,
        }
    }
}

/// Publish a `terminal:data` event carrying a base64 output `chunk`.
async fn emit_data(bus: &Option<EventBus>, ws: &WorkspaceId, terminal_id: &str, bytes: &[u8]) {
    let chunk = base64::engine::general_purpose::STANDARD.encode(bytes);
    publish_event(
        bus,
        terminal_event(
            ws,
            TERMINAL_DATA,
            json!({ "terminalId": terminal_id, "chunk": chunk }),
        ),
    )
    .await;
}

/// Publish a self-sufficient `terminal:exit` event.
async fn emit_exit(
    bus: &Option<EventBus>,
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
pub struct PtyTerminalHost {
    pty: Arc<PtyHost>,
}

impl PtyTerminalHost {
    /// Wire the adapter over the shared host.
    pub fn new(pty: Arc<PtyHost>) -> Self {
        Self { pty }
    }
}

impl TerminalHost for PtyTerminalHost {
    fn create(&self, params: TerminalCreateParams) -> BoxFuture<'_, AcpResult<String>> {
        let pty = self.pty.clone();
        Box::pin(async move {
            let mut spec = SpawnSpec::new(params.session_id, params.command);
            spec.args = params.args;
            spec.env = params.env;
            spec.cwd = params.cwd;
            if let Some(limit) = params
                .output_byte_limit
                .and_then(|n| usize::try_from(n).ok())
            {
                spec.scrollback_bytes = limit;
            }
            let pty_id = pty.spawn(spec).map_err(acp_err)?;
            Ok(pty_id.to_string())
        })
    }

    fn output(&self, terminal_id: String) -> BoxFuture<'_, AcpResult<TerminalOutputInfo>> {
        let pty = self.pty.clone();
        Box::pin(async move {
            let id = acp_resolve(&terminal_id)?;
            let limit = pty.scrollback(id).map_err(acp_err)?;
            let exit = pty.try_exit(id).ok().flatten().map(to_exit_info);
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
            let exit = pty.wait(id).await.map_err(acp_err)?;
            Ok(to_exit_info(exit))
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
fn acp_err(e: Error) -> AcpError {
    AcpError::Terminal(e.to_string())
}

/// Resolve a wire terminal id into a [`PtyId`] for the ACP adapter.
fn acp_resolve(terminal_id: &str) -> AcpResult<PtyId> {
    PtyId::parse(terminal_id)
        .ok_or_else(|| AcpError::Terminal(format!("unknown terminal {terminal_id}")))
}

/// Convert a host [`PtyExit`] into the ACP exit shape (`signal` is unavailable
/// through the host abstraction).
fn to_exit_info(exit: PtyExit) -> TerminalExitInfo {
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

    /// A temp SQLite path cleaned up on drop (mirrors `events::bus_tests`).
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
            let remaining = match deadline.checked_duration_since(Instant::now()) {
                Some(d) => d,
                None => break,
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

        let listed = list(pty.as_ref(), &ws("ws-1")).unwrap();
        let arr = listed.as_array().unwrap();
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["id"], json!(id));
        assert_eq!(arr[0]["name"], json!("Terminal"));
        assert_eq!(arr[0]["isExecutingCommand"], json!(true));

        kill(pty.as_ref(), &id).await.unwrap();
        let empty = poll_until(
            || {
                let v = list(pty.as_ref(), &ws("ws-1")).unwrap();
                v.as_array().filter(|a| a.is_empty()).map(|_| ())
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

        let arr = list(pty.as_ref(), &ws("ws-1")).unwrap();
        let entry = &arr.as_array().unwrap()[0];
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

        let listed = list(pty.as_ref(), &ws("ws-named")).unwrap();
        let arr = listed.as_array().unwrap();
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
            archived: false,
            archived_at: None,
            task_stats: None,
            agent_summary: None,
            diff_summary: None,
            token_usage: None,
            cow_supported: None,
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

        let arr = list(pty.as_ref(), &wsid).unwrap();
        let entry = &arr.as_array().unwrap()[0];
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

    #[tokio::test]
    async fn natural_exit_emits_exit_event_with_code() {
        let pty = host();
        let (_tmp, bus) = bus().await;
        let mut sub = bus.subscribe(SubscriptionFilter::default());

        let res = create(
            pty.clone(),
            Some(bus),
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

    #[tokio::test]
    async fn write_resize_and_get_buffer_roundtrip() {
        let pty = host();
        let res = create(
            pty.clone(),
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
        assert!(decode(capped["data"].as_str().unwrap()).len() <= 4);

        kill(pty.as_ref(), &id).await.unwrap();
    }

    #[tokio::test]
    async fn write_rejects_invalid_base64() {
        let pty = host();
        let res = create(
            pty.clone(),
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
    async fn read_output_paginates_with_token() {
        let pty = host();
        let res = create(
            pty.clone(),
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
        let page2 =
            read_output(pty.as_ref(), &ws("ws-1"), &id, Some(2), false, Some(token)).unwrap();
        assert!(!page2["items"].as_array().unwrap().is_empty());

        kill(pty.as_ref(), &id).await.unwrap();
    }

    // ---- ACP `PtyTerminalHost` adapter ----

    /// ACP terminal create → output → wait_for_exit happy path.
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
        let adapter = PtyTerminalHost::new(pty.clone());
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

    #[tokio::test]
    async fn acp_create_with_byte_limit_then_release() {
        let pty = host();
        let adapter = PtyTerminalHost::new(pty.clone());
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
        let adapter = PtyTerminalHost::new(pty);
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
    async fn acp_kill_terminates_tracked_terminal() {
        let pty = host();
        let adapter = PtyTerminalHost::new(pty.clone());
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
