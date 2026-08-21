//! Streaming / interactive process exec for `host.execStream` (PROTOCOL §5.14).
//!
//! `host.exec` is one-shot and buffers stdio; a streaming FE surface (e.g.
//! `augment-cli`'s newline-delimited JSON chat) needs live `stdout` / `stderr`
//! chunks plus a `stdin` channel that neither `host.exec` nor the PTY-mangling
//! `terminal.*` nor the workspace-scoped `script.*` fit. This module mirrors
//! `git.clone`'s streaming shape (`{ requestId }` up front + `host:exec:*` bus
//! frames + terminal `host:exec:exit`, §5.6 / §6.5) while reusing every
//! `host_exec` guarantee: argv-only (no shell), workspace-containment on `cwd`,
//! process-group leader + `kill_on_drop`, PATH enrichment, and secret-safety
//! (env values never logged or streamed — only `stdout`/`stderr`/exit metadata
//! cross the wire).

use std::collections::HashMap;
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use base64::Engine as _;
use intent_core::events::{HOST_EXEC_EXIT, HOST_EXEC_STDERR, HOST_EXEC_STDOUT};
use intent_core::{now_iso, WorkspaceApi, WorkspaceId};
use intent_store::NewEvent;
use serde_json::{json, Map, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;

use crate::events::EventBus;
use crate::host_exec::{self, build_command, HostExecArgs, HostExecError, INVALID_PARAMS};
use crate::system_actor;

/// Grace period between SIGTERM and SIGKILL when reaping a cancelled / timed-out
/// stream, mirroring [`host_exec`]'s constant so both surfaces settle helper
/// subprocesses the same way.
const TERM_GRACE: Duration = Duration::from_millis(500);

/// Chunk size for reads off the child's stdout/stderr pipes. Small enough that
/// interactive output surfaces promptly; large enough that a chatty child does
/// not saturate the bus with tiny frames.
const READ_BUF_SIZE: usize = 4096;

/// Capacity of the per-stream stdin mpsc. Small: a follow-up `write` blocks in
/// the caller only if the child is slow to drain — this backpressure is what we
/// want.
const STDIN_QUEUE_CAP: usize = 32;

/// Parsed `host.execStream` params: the shared exec fields (validated by
/// [`host_exec::parse_args`]) plus the streaming-only `requestId` / initial
/// `stdin` payload.
#[derive(Debug)]
pub struct HostExecStreamArgs {
    /// Shared exec fields (command, args, cwd, env, timeout_ms, workspace_id).
    pub common: HostExecArgs,
    /// Caller-supplied correlation id; a fresh `hexec-<uuid>` is minted when
    /// absent so the response always carries a `requestId` echoable back on
    /// follow-up `host.execStream.write` / `host.execStream.cancel`.
    pub request_id: Option<String>,
    /// Optional initial stdin payload, written to the child before the reader
    /// tasks start. Encoded either as a plain UTF-8 string (`stdin: "hi\n"`) or
    /// as a base64 blob under `stdinBase64` for binary payloads.
    pub stdin: Option<Vec<u8>>,
}

/// Parse a JSON-RPC params object into [`HostExecStreamArgs`]. Layers the
/// streaming-only fields on top of [`host_exec::parse_args`] so validation is
/// identical to the one-shot surface.
pub fn parse_args(params: &Map<String, Value>) -> Result<HostExecStreamArgs, HostExecError> {
    let common = host_exec::parse_args(params)?;
    let request_id = params
        .get("requestId")
        .and_then(Value::as_str)
        .map(str::to_string)
        .filter(|s| !s.is_empty());
    let stdin = match (params.get("stdin"), params.get("stdinBase64")) {
        (None, None) | (Some(Value::Null), None) | (None, Some(Value::Null)) => None,
        (Some(Value::Null), Some(Value::Null)) => None,
        (Some(text), None) | (Some(text), Some(Value::Null)) => match text {
            Value::String(s) => Some(s.as_bytes().to_vec()),
            _ => {
                return Err(HostExecError {
                    code: INVALID_PARAMS,
                    message: "Invalid parameter: stdin must be a string".to_string(),
                });
            }
        },
        (None, Some(b64)) | (Some(Value::Null), Some(b64)) => match b64 {
            Value::String(s) => Some(decode_base64_field(s, "stdinBase64")?),
            _ => {
                return Err(HostExecError {
                    code: INVALID_PARAMS,
                    message: "Invalid parameter: stdinBase64 must be a string".to_string(),
                });
            }
        },
        (Some(_), Some(_)) => {
            return Err(HostExecError {
                code: INVALID_PARAMS,
                message: "Invalid parameter: pass either stdin or stdinBase64, not both"
                    .to_string(),
            });
        }
    };
    Ok(HostExecStreamArgs {
        common,
        request_id,
        stdin,
    })
}

/// Decode a base64 field or return a `-32602` with the field name. Uses the
/// same STANDARD alphabet as `terminal:data.chunk` so payloads round-trip.
fn decode_base64_field(s: &str, field: &str) -> Result<Vec<u8>, HostExecError> {
    base64::engine::general_purpose::STANDARD
        .decode(s.as_bytes())
        .map_err(|_| HostExecError {
            code: INVALID_PARAMS,
            message: format!("Invalid parameter: {field} is not valid base64"),
        })
}

/// Per-stream handle held by [`HostExecStreamRegistry`]. The stdin sender is
/// consumed by the forwarder task; the cancel token is polled by the wait task
/// so `host.execStream.cancel` reaps the whole process group.
struct StreamHandle {
    stdin_tx: mpsc::Sender<StdinMsg>,
    cancel_token: Arc<AtomicBool>,
}

/// Stdin control channel messages. `Data` is written verbatim to the child;
/// `Close` drops the child's stdin end so a `cat` / `augment-cli` reading to EOF
/// finishes cleanly.
enum StdinMsg {
    Data(Vec<u8>),
    Close,
}

/// Process-wide registry of live [`host.execStream`] jobs, keyed by
/// `requestId`. Cheap to clone (shares the inner map). Held in a global
/// [`OnceLock`] so the transport fast-path can drive `write` / `cancel`
/// without threading state through every layer.
#[derive(Clone, Default)]
pub struct HostExecStreamRegistry {
    inner: Arc<Mutex<HashMap<String, StreamHandle>>>,
}

impl HostExecStreamRegistry {
    /// Empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of live streams (test/diagnostics use).
    pub fn len(&self) -> usize {
        self.inner.lock().expect("registry poisoned").len()
    }

    /// Whether the registry has no live streams.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn insert(&self, request_id: String, handle: StreamHandle) {
        self.inner
            .lock()
            .expect("registry poisoned")
            .insert(request_id, handle);
    }

    fn remove(&self, request_id: &str) {
        self.inner
            .lock()
            .expect("registry poisoned")
            .remove(request_id);
    }

    /// Send a stdin chunk to the running stream. `eof=true` closes the child's
    /// stdin after the (possibly empty) payload so a reader-to-EOF exits.
    /// Returns `Err(-32603)` for an unknown / already-finished `requestId` so
    /// the wire surface can surface a clear "no such stream" error.
    pub async fn write(
        &self,
        request_id: &str,
        data: Option<Vec<u8>>,
        eof: bool,
    ) -> Result<(), HostExecError> {
        let sender = {
            let map = self.inner.lock().expect("registry poisoned");
            map.get(request_id).map(|h| h.stdin_tx.clone())
        };
        let sender = sender.ok_or_else(|| HostExecError {
            code: -32603,
            message: format!("unknown host.execStream requestId: {request_id}"),
        })?;
        if let Some(bytes) = data {
            if !bytes.is_empty() {
                sender
                    .send(StdinMsg::Data(bytes))
                    .await
                    .map_err(|_| HostExecError {
                        code: -32603,
                        message: format!("host.execStream {request_id} stdin closed"),
                    })?;
            }
        }
        if eof {
            let _ = sender.send(StdinMsg::Close).await;
        }
        Ok(())
    }

    /// Signal cancellation. Returns `true` when a live stream was flipped,
    /// `false` for an unknown / already-finished id (still surfaces `ok:true`
    /// on the wire so the surface is idempotent).
    pub fn cancel(&self, request_id: &str) -> bool {
        let map = self.inner.lock().expect("registry poisoned");
        match map.get(request_id) {
            Some(handle) => {
                handle.cancel_token.store(true, Ordering::SeqCst);
                // Drop the stdin sender's clone on the caller side; the
                // forwarder task's copy still lives so the wait path can close
                // stdin during reap.
                true
            }
            None => false,
        }
    }
}

/// Process-wide singleton so the transport fast-path can reach the same
/// registry the spawn path populated.
static REGISTRY: OnceLock<HostExecStreamRegistry> = OnceLock::new();

/// Borrow the process-wide registry (lazy-initialized).
pub fn registry() -> &'static HostExecStreamRegistry {
    REGISTRY.get_or_init(HostExecStreamRegistry::new)
}

/// Mint a fresh `requestId` for streams that omit one.
pub fn mint_request_id() -> String {
    format!("hexec-{}", uuid::Uuid::new_v4())
}

/// Validate + spawn a streaming exec: returns the `requestId` once the child is
/// live and the reader/stdin/wait tasks are running. Errors from validation or
/// spawn are returned to the caller (mapped to `-32602` / `-32603`); once the
/// child is up, all further outcomes surface on `host:exec:exit`.
pub async fn start_stream(
    api: &dyn WorkspaceApi,
    bus: EventBus,
    args: HostExecStreamArgs,
) -> Result<String, HostExecError> {
    let HostExecStreamArgs {
        common,
        request_id,
        stdin,
    } = args;

    // Resolve `cwd` inside the workspace root (reuses the `host_exec` guard
    // via a public helper so the two surfaces stay bit-identical).
    let cwd_resolved = match (common.cwd.as_deref(), common.workspace_id.as_deref()) {
        (Some(cwd), Some(ws_id)) => {
            Some(host_exec::resolve_cwd_within_workspace(api, ws_id, cwd, None).await?)
        }
        _ => None,
    };

    let request_id = request_id.unwrap_or_else(mint_request_id);
    let workspace_id = common.workspace_id.as_deref().map_or_else(
        || WorkspaceId::from_string(String::new()),
        WorkspaceId::from,
    );

    // Spawn with piped stdin so follow-up writes reach the child.
    let mut cmd = build_command(&common, cwd_resolved.as_deref());
    cmd.stdin(Stdio::piped());
    let mut child = cmd.spawn().map_err(|e| HostExecError {
        code: -32603,
        message: format!("spawn failed: {}: {e}", common.command),
    })?;

    let pid = child.id();
    let child_stdin = child.stdin.take();
    let child_stdout = child.stdout.take();
    let child_stderr = child.stderr.take();

    let (stdin_tx, mut stdin_rx) = mpsc::channel::<StdinMsg>(STDIN_QUEUE_CAP);
    let cancel_token = Arc::new(AtomicBool::new(false));

    // Seed with the initial stdin payload BEFORE registering, so a fast
    // subscriber does not observe an empty stream.
    if let Some(seed) = stdin {
        if !seed.is_empty() {
            let _ = stdin_tx.send(StdinMsg::Data(seed)).await;
        }
    }

    registry().insert(
        request_id.clone(),
        StreamHandle {
            stdin_tx,
            cancel_token: cancel_token.clone(),
        },
    );

    // Stdin forwarder: drain `stdin_rx` into the child. Exits on `Close` or
    // channel close; a write error also ends the task (child likely gone).
    if let Some(mut stdin_pipe) = child_stdin {
        tokio::spawn(async move {
            while let Some(msg) = stdin_rx.recv().await {
                match msg {
                    StdinMsg::Data(bytes) => {
                        if stdin_pipe.write_all(&bytes).await.is_err() {
                            break;
                        }
                        let _ = stdin_pipe.flush().await;
                    }
                    StdinMsg::Close => break,
                }
            }
            // Dropping `stdin_pipe` closes the child's stdin end.
        });
    }

    // Reader tasks: publish base64 chunks per event type as they arrive.
    if let Some(reader) = child_stdout {
        spawn_reader(
            bus.clone(),
            workspace_id.clone(),
            request_id.clone(),
            reader,
            HOST_EXEC_STDOUT,
        );
    }
    if let Some(reader) = child_stderr {
        spawn_reader(
            bus.clone(),
            workspace_id.clone(),
            request_id.clone(),
            reader,
            HOST_EXEC_STDERR,
        );
    }

    // Wait task: watches for exit, timeout, and cancellation. Reaps the whole
    // process group and publishes the terminal `host:exec:exit` frame.
    let bus_wait = bus.clone();
    let ws_wait = workspace_id.clone();
    let req_wait = request_id.clone();
    let timeout_ms = common.timeout_ms;
    tokio::spawn(async move {
        run_wait_loop(
            bus_wait,
            ws_wait,
            req_wait,
            child,
            pid,
            cancel_token,
            timeout_ms,
        )
        .await;
    });

    Ok(request_id)
}

/// Spawn a per-pipe reader that chunks bytes into base64 and broadcasts them as
/// `event_type` frames until the pipe closes. Chunks are transient
/// (broadcast-only, never persisted — same path as `chat:stream:delta`):
/// streamed output is consumed live by the correlated subscriber and has no
/// event-table readback, so a chatty child must not serialize behind a durable
/// SQLite commit per chunk. The terminal `host:exec:exit` stays durable but is
/// published from [`run_wait_loop`] — a different task from these readers — so
/// unlike the terminal/script paths there is no exit-never-overtakes-data
/// guarantee: `child.wait()` can return while a pipe still holds unread bytes,
/// and the exit frame may precede trailing chunks (pre-existing behavior).
fn spawn_reader<R>(
    bus: EventBus,
    workspace_id: WorkspaceId,
    request_id: String,
    mut reader: R,
    event_type: &'static str,
) where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        let mut buf = vec![0u8; READ_BUF_SIZE];
        loop {
            match reader.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => {
                    let chunk = base64::engine::general_purpose::STANDARD.encode(&buf[..n]);
                    let ev = chunk_event(&workspace_id, &request_id, event_type, chunk);
                    bus.publish_transient(&ev);
                }
                Err(_) => break,
            }
        }
    });
}

/// Wait for the child, honoring `timeoutMs` and cancellation. On any terminal
/// outcome publishes one `host:exec:exit` and unregisters the stream.
async fn run_wait_loop(
    bus: EventBus,
    workspace_id: WorkspaceId,
    request_id: String,
    mut child: tokio::process::Child,
    pid: Option<u32>,
    cancel_token: Arc<AtomicBool>,
    timeout_ms: Option<u64>,
) {
    let deadline = timeout_ms.map(|ms| tokio::time::Instant::now() + Duration::from_millis(ms));
    let mut timed_out = false;
    let mut cancelled = false;

    let status = loop {
        // Fast-path exit check.
        if cancel_token.load(Ordering::SeqCst) {
            cancelled = true;
            reap_child_group(&mut child, pid).await;
            break child.wait().await;
        }
        if let Some(dl) = deadline {
            if tokio::time::Instant::now() >= dl {
                timed_out = true;
                reap_child_group(&mut child, pid).await;
                break child.wait().await;
            }
        }
        // Poll every 100ms so a cancel / timeout is observed promptly without
        // burning a task on a tight loop.
        let poll = Duration::from_millis(100);
        if let Ok(status) = tokio::time::timeout(poll, child.wait()).await {
            break status;
        }
    };

    registry().remove(&request_id);

    let exit_code = status
        .as_ref()
        .ok()
        .and_then(std::process::ExitStatus::code)
        .map(|c| c as i64);
    let ok = matches!(
        status.as_ref().ok().map(std::process::ExitStatus::success),
        Some(true)
    );

    let mut data = json!({
        "requestId": &request_id,
        "ok": ok,
    });
    if let Some(code) = exit_code {
        data["exitCode"] = json!(code);
    }
    if timed_out {
        data["timedOut"] = json!(true);
    }
    if cancelled {
        data["cancelled"] = json!(true);
    }

    let ev = NewEvent {
        workspace_id,
        timestamp: now_iso(),
        event_type: HOST_EXEC_EXIT.to_string(),
        actor: system_actor(),
        session_id: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data,
    };
    if let Err(e) = bus.publish(&ev).await {
        tracing::warn!(error = %e, "failed to publish host:exec:exit");
    }
    // Prevent unused-var lints on the caller side.
    let _ = pid;
}

/// Reap the whole process group: SIGTERM → grace → SIGKILL, mirroring
/// `host_exec::run`'s reap so helper subprocesses do not survive a
/// cancel/timeout. Descendants that escaped into their OWN process groups
/// survive the group kill, so they are snapshotted before signalling and
/// swept afterwards (`intent_acp::descendant_sweep`).
async fn reap_child_group(child: &mut tokio::process::Child, pid: Option<u32>) {
    #[cfg(unix)]
    {
        if let Some(pid) = pid {
            let descendants = intent_acp::descendant_pids(pid).await;
            kill_group(pid, nix::sys::signal::Signal::SIGTERM);
            tokio::time::sleep(TERM_GRACE).await;
            if !matches!(child.try_wait(), Ok(Some(_))) {
                kill_group(pid, nix::sys::signal::Signal::SIGKILL);
            }
            intent_acp::sweep_escaped_descendants(&descendants).await;
            return;
        }
    }
    #[cfg(not(unix))]
    let _ = pid;
    let _ = child.start_kill();
}

#[cfg(unix)]
fn kill_group(pid: u32, sig: nix::sys::signal::Signal) {
    use nix::sys::signal::killpg;
    use nix::unistd::Pid;
    let _ = killpg(Pid::from_raw(pid as i32), sig);
}

/// Build a `host:exec:{stdout,stderr}` event with the base64 chunk payload.
fn chunk_event(
    workspace_id: &WorkspaceId,
    request_id: &str,
    event_type: &'static str,
    chunk_b64: String,
) -> NewEvent {
    NewEvent {
        workspace_id: workspace_id.clone(),
        timestamp: now_iso(),
        event_type: event_type.to_string(),
        actor: system_actor(),
        session_id: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data: json!({
            "requestId": request_id,
            "chunk": chunk_b64,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn map(v: Value) -> Map<String, Value> {
        v.as_object().cloned().unwrap_or_default()
    }

    #[test]
    fn parse_args_defaults_no_stdin_no_request_id() {
        let a = parse_args(&map(json!({ "command": "echo" }))).unwrap();
        assert!(a.request_id.is_none());
        assert!(a.stdin.is_none());
    }

    #[test]
    fn parse_args_accepts_utf8_stdin() {
        let a = parse_args(&map(json!({ "command": "cat", "stdin": "hello\n" }))).unwrap();
        assert_eq!(a.stdin.as_deref(), Some(b"hello\n".as_slice()));
    }

    #[test]
    fn parse_args_accepts_base64_stdin() {
        let a = parse_args(&map(json!({ "command": "cat", "stdinBase64": "aGVsbG8K" }))).unwrap();
        assert_eq!(a.stdin.as_deref(), Some(b"hello\n".as_slice()));
    }

    #[test]
    fn parse_args_rejects_both_stdin_forms() {
        let err = parse_args(&map(
            json!({ "command": "cat", "stdin": "x", "stdinBase64": "eA==" }),
        ))
        .unwrap_err();
        assert_eq!(err.code, INVALID_PARAMS);
    }

    #[test]
    fn parse_args_rejects_non_string_stdin() {
        let err = parse_args(&map(json!({ "command": "cat", "stdin": 42 }))).unwrap_err();
        assert_eq!(err.code, INVALID_PARAMS);
    }

    #[test]
    fn parse_args_caps_timeout_via_host_exec() {
        use crate::host_exec::MAX_TIMEOUT_MS;
        let a = parse_args(&map(json!({
            "command": "sleep",
            "timeoutMs": MAX_TIMEOUT_MS + 1_000,
        })))
        .unwrap();
        assert_eq!(a.common.timeout_ms, Some(MAX_TIMEOUT_MS));
    }

    #[test]
    fn mint_request_id_is_prefixed_and_unique() {
        let a = mint_request_id();
        let b = mint_request_id();
        assert!(a.starts_with("hexec-"));
        assert_ne!(a, b);
    }

    #[test]
    fn registry_cancel_unknown_is_false() {
        let r = HostExecStreamRegistry::new();
        assert!(!r.cancel("nope"));
    }
}
