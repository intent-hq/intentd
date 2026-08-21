//! Daemon-owned one-shot process exec for `host.exec` (PROTOCOL §5.14).
//!
//! Spawns `command` with `args` on the daemon host (no shell interpolation —
//! `argv` only), captures stdout/stderr/exit code, and enforces a
//! workspace-containment guard on `cwd` in the same spirit as `file_ops`
//! (lexical resolve against the workspace root). The guard here uses the
//! component-aware `Path::starts_with` instead of a raw-string prefix so
//! sibling paths like `/tmp/ws2` cannot escape a `/tmp/ws` root; `file_ops`
//! is not (yet) bit-for-bit identical.
//! Reuses the process-group leader + `kill_on_drop` discipline (`mcp_servers` /
//! `intent-acp::spawn`) so a `timeoutMs` reaps the whole tree (no orphaned
//! grandchildren). The child env is assembled with a strict precedence:
//! caller-supplied `env` wins, then the daemon's own process env (a var
//! already set in `std::env` is never overridden), and allow-listed
//! credential vars captured from the login shell
//! (`intent_core::path_utils::login_shell_credential_env`) fill gaps only.
//! PATH is enriched via `intent_providers::enhanced_path` (caller `env["PATH"]`
//! still wins). Secret-safe: no env values — captured or otherwise — are ever
//! logged, traced, or returned; only stdout/stderr/exitCode/timedOut cross
//! the wire.
//!
//! This is a one-shot primitive: long-lived / streaming processes stay on the
//! `script.*` / `terminal.*` surface (§5.8, §12).

use std::collections::BTreeMap;
use std::path::{Component, Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use intent_core::{WorkspaceApi, WorkspaceId};
use intent_providers::enhanced_path;
use serde_json::{json, Map, Value};
use tokio::io::AsyncReadExt;
use tokio::process::Command;

use crate::file_ops;

/// Grace period between SIGTERM and SIGKILL when reaping a timed-out child,
/// mirroring `mcp_servers::reap`'s TERM_GRACE.
const TERM_GRACE: Duration = Duration::from_millis(500);

/// Wire error codes surfaced by `host.exec` (PROTOCOL §9: `-32602` for invalid
/// params, `-32603` for internal execution failures).
pub const INVALID_PARAMS: i32 = -32602;
pub const INTERNAL_ERROR: i32 = -32603;

/// Settings-driven allow/deny hook seam (v1: stub always allows). The daemon
/// composition root will plug a real policy later; for now the seam exists so
/// callers can gate exec without changing the wire contract.
pub trait ExecPolicy: Send + Sync {
    /// `Ok(())` allows the invocation; `Err(reason)` rejects it with `reason`
    /// surfaced as `-32603` at the transport layer.
    fn evaluate(&self, command: &str, args: &[String]) -> Result<(), String>;
}

/// v1 stub: every command is allowed. The seam is what matters; policies land
/// separately behind this trait.
pub(crate) struct AllowAllPolicy;

impl ExecPolicy for AllowAllPolicy {
    fn evaluate(&self, _command: &str, _args: &[String]) -> Result<(), String> {
        Ok(())
    }
}

/// Parsed `host.exec` params. `command` is required; `args`/`env` default empty;
/// `cwd` requires `workspace_id` so the containment guard has a root to check
/// against. `timeout_ms` capped by [`MAX_TIMEOUT_MS`].
#[derive(Debug)]
pub struct HostExecArgs {
    pub command: String,
    pub args: Vec<String>,
    pub cwd: Option<String>,
    pub env: BTreeMap<String, String>,
    pub timeout_ms: Option<u64>,
    pub workspace_id: Option<String>,
}

/// Upper bound on `timeoutMs` to keep a runaway request from wedging the daemon
/// (10 minutes matches the outer bound on other short-lived host probes).
pub(crate) const MAX_TIMEOUT_MS: u64 = 10 * 60 * 1000;

/// A `host.exec` failure with the error code the transport should surface.
#[derive(Debug)]
pub struct HostExecError {
    pub code: i32,
    pub message: String,
}

impl HostExecError {
    fn invalid(msg: impl Into<String>) -> Self {
        Self {
            code: INVALID_PARAMS,
            message: msg.into(),
        }
    }
    fn internal(msg: impl Into<String>) -> Self {
        Self {
            code: INTERNAL_ERROR,
            message: msg.into(),
        }
    }
}

/// Parse a JSON-RPC params object into [`HostExecArgs`]. Rejects a missing /
/// empty `command`, non-string `args`/`env`, and negative `timeoutMs`.
pub fn parse_args(params: &Map<String, Value>) -> Result<HostExecArgs, HostExecError> {
    let command = params
        .get("command")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| HostExecError::invalid("Missing required parameter: command"))?
        .to_string();
    let args: Vec<String> = match params.get("args") {
        None | Some(Value::Null) => Vec::new(),
        Some(Value::Array(a)) => {
            let mut out = Vec::with_capacity(a.len());
            for v in a {
                match v.as_str() {
                    Some(s) => out.push(s.to_string()),
                    None => {
                        return Err(HostExecError::invalid(
                            "Invalid parameter: args must be an array of strings",
                        ))
                    }
                }
            }
            out
        }
        _ => {
            return Err(HostExecError::invalid(
                "Invalid parameter: args must be an array of strings",
            ))
        }
    };
    let env = match params.get("env") {
        None | Some(Value::Null) => BTreeMap::new(),
        Some(Value::Object(o)) => {
            let mut out = BTreeMap::new();
            for (k, v) in o {
                match v.as_str() {
                    Some(s) => {
                        out.insert(k.clone(), s.to_string());
                    }
                    None => {
                        return Err(HostExecError::invalid(
                            "Invalid parameter: env values must be strings",
                        ))
                    }
                }
            }
            out
        }
        _ => {
            return Err(HostExecError::invalid(
                "Invalid parameter: env must be an object of string values",
            ))
        }
    };
    let cwd = params
        .get("cwd")
        .and_then(Value::as_str)
        .map(str::to_string)
        .filter(|s| !s.is_empty());
    let workspace_id = params
        .get("workspaceId")
        .and_then(Value::as_str)
        .map(str::to_string)
        .filter(|s| !s.is_empty());
    let timeout_ms = match params.get("timeoutMs") {
        None | Some(Value::Null) => None,
        Some(v) => {
            let n = v.as_u64().ok_or_else(|| {
                HostExecError::invalid("Invalid parameter: timeoutMs must be a positive integer")
            })?;
            Some(n.min(MAX_TIMEOUT_MS))
        }
    };
    if cwd.is_some() && workspace_id.is_none() {
        return Err(HostExecError::invalid(
            "Invalid parameter: cwd requires workspaceId for the containment guard",
        ));
    }
    Ok(HostExecArgs {
        command,
        args,
        cwd,
        env,
        timeout_ms,
        workspace_id,
    })
}

/// Lexical `..`/`.` normalization (mirrors `file_ops::normalize_lexical`,
/// duplicated here because `file_ops` keeps it crate-private).
fn normalize_lexical(p: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for comp in p.components() {
        match comp {
            Component::ParentDir => {
                out.pop();
            }
            Component::CurDir => {}
            other => out.push(other.as_os_str()),
        }
    }
    out
}

/// Node-style `path.resolve(base, rel)` (mirrors `file_ops::node_resolve`):
/// absolute `rel` wins; otherwise join onto an absolute `base`. Unlike Node's
/// `path.resolve`, an empty or non-absolute `base` is an explicit error rather
/// than a silent rebase onto the daemon's CWD — the containment guard cannot
/// be honoured against a base we didn't choose, and quietly falling back to
/// `std::env::current_dir()` is the same archetype as the fixed terminal-cwd
/// escape (a caller thinks it constrained the exec to a workspace root; the
/// daemon-CWD fallback lets it escape).
fn node_resolve(base: &str, rel: &str) -> Result<PathBuf, HostExecError> {
    let rel_path = Path::new(rel);
    let combined = if rel_path.is_absolute() {
        PathBuf::from(rel)
    } else {
        let base_path = Path::new(base);
        if !base_path.is_absolute() {
            return Err(HostExecError::internal(
                "cwd resolution requires an absolute workspace root",
            ));
        }
        base_path.join(rel)
    };
    Ok(normalize_lexical(&combined))
}

/// Resolve the workspace filesystem root, then apply the same lexical
/// prefix-check `file_ops` uses. An empty root (unset `worktreePath`/`path`)
/// rejects any `cwd` — the containment guard cannot be enforced without one.
/// When `caller_agent_id` is provided and the agent has a sandbox, the sandbox
/// path is used as the containment root (sandboxed agent containment).
/// Public so the streaming surface (`host_exec_stream`) can reuse the guard
/// bit-identically instead of re-deriving it.
pub(crate) async fn resolve_cwd_within_workspace(
    api: &dyn WorkspaceApi,
    workspace_id: &str,
    cwd: &str,
    caller_agent_id: Option<&intent_core::AgentId>,
) -> Result<PathBuf, HostExecError> {
    let ws = api
        .get_workspace(WorkspaceId::from(workspace_id))
        .await
        .map_err(|e| HostExecError::internal(format!("workspace lookup failed: {e}")))?;

    let root = if let Some(agent_id) = caller_agent_id {
        if let Ok(session) = api.agent_get_session(agent_id.clone(), None).await {
            if let Some(sandbox_path) = session.sandbox_path {
                sandbox_path
            } else {
                file_ops::workspace_root(&ws)
            }
        } else {
            file_ops::workspace_root(&ws)
        }
    } else {
        file_ops::workspace_root(&ws)
    };

    if root.is_empty() {
        return Err(HostExecError::internal(
            "Access denied: workspace has no filesystem root",
        ));
    }
    let full = node_resolve(&root, cwd)?;
    if !cwd_within_root(Path::new(&root), &full) {
        return Err(HostExecError::internal(
            "Access denied: cwd outside workspace",
        ));
    }
    Ok(full)
}

/// Component-aware containment check for the `cwd` guard: a raw-string
/// `starts_with` on `root` admits sibling prefixes (root `/tmp/ws` wrongly
/// matches `/tmp/ws2`); `Path::starts_with` compares whole components, so
/// only `/tmp/ws` and `/tmp/ws/…` are accepted.
fn cwd_within_root(root: &Path, full: &Path) -> bool {
    full.starts_with(root)
}

/// Assemble the tokio `Command` for a validated exec request. Pipes stdio,
/// sets `kill_on_drop`, puts the child in its own process group (unix), and
/// assembles the child env per the precedence contract in the module doc
/// (caller `env` > daemon process env > captured login-shell credential vars;
/// enhanced PATH with caller `env["PATH"]` winning). Exposed for tests.
pub fn build_command(args: &HostExecArgs, cwd_resolved: Option<&Path>) -> Command {
    build_command_with_captured(
        args,
        cwd_resolved,
        intent_core::path_utils::login_shell_credential_env(),
    )
}

/// Injectable core of [`build_command`]: `captured` is the login-shell
/// credential env map. Production passes the process-global cached capture;
/// tests inject a fake map so they never spawn a real shell.
fn build_command_with_captured(
    args: &HostExecArgs,
    cwd_resolved: Option<&Path>,
    captured: &BTreeMap<String, String>,
) -> Command {
    let mut cmd = Command::new(&args.command);
    cmd.args(&args.args);
    if let Some(dir) = cwd_resolved {
        cmd.current_dir(dir);
    }
    // Captured login-shell credential vars fill gaps only: a var already in
    // the daemon's own process env is inherited untouched, and the caller's
    // `env` (applied last, below) still wins.
    for (k, v) in captured_env_to_apply(captured, &args.env, |k| std::env::var_os(k).is_some()) {
        cmd.env(k, v);
    }
    // Enrich PATH so a user-supplied `env["PATH"]` still wins if provided.
    cmd.env("PATH", enhanced_path(None));
    for (k, v) in &args.env {
        cmd.env(k, v);
    }
    cmd.stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    #[cfg(unix)]
    cmd.process_group(0);
    cmd
}

/// Select which captured login-shell credential vars to apply to a spawn:
/// only those absent from BOTH the caller's `env` map and the daemon's own
/// process env (probed via `daemon_env_has`). Pure so tests can drive the
/// precedence contract without a real shell or process-env mutation.
/// SECRET SAFETY: values are credentials — never log or serialize them.
fn captured_env_to_apply<'a>(
    captured: &'a BTreeMap<String, String>,
    caller_env: &BTreeMap<String, String>,
    daemon_env_has: impl Fn(&str) -> bool,
) -> Vec<(&'a str, &'a str)> {
    captured
        .iter()
        .filter(|(k, _)| !caller_env.contains_key(k.as_str()) && !daemon_env_has(k))
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect()
}

/// Signal a whole process group by its leader pid (pgid == pid via
/// `process_group`). Mirrors `mcp_servers::kill_group`.
#[cfg(unix)]
fn kill_group(pid: u32, sig: nix::sys::signal::Signal) {
    use nix::sys::signal::killpg;
    use nix::unistd::Pid;
    let _ = killpg(Pid::from_raw(pid as i32), sig);
}

/// Execute one `host.exec` request end-to-end: validate policy + `cwd`, spawn,
/// wait (with `timeoutMs`), reap the process group on timeout, and collect
/// stdout/stderr. Returns the `{ stdout, stderr, exitCode, timedOut? }` JSON.
pub async fn run(
    api: &dyn WorkspaceApi,
    args: HostExecArgs,
    policy: &dyn ExecPolicy,
) -> Result<Value, HostExecError> {
    policy
        .evaluate(&args.command, &args.args)
        .map_err(HostExecError::internal)?;

    let cwd_resolved = match (args.cwd.as_deref(), args.workspace_id.as_deref()) {
        (Some(cwd), Some(ws_id)) => {
            Some(resolve_cwd_within_workspace(api, ws_id, cwd, None).await?)
        }
        _ => None,
    };

    let mut cmd = build_command(&args, cwd_resolved.as_deref());
    let mut child = cmd
        .spawn()
        .map_err(|e| HostExecError::internal(format!("spawn failed: {}: {e}", args.command)))?;
    let pid = child.id();
    let mut stdout_reader = child.stdout.take();
    let mut stderr_reader = child.stderr.take();

    let wait_fut = async {
        let mut stdout_buf = Vec::new();
        let mut stderr_buf = Vec::new();
        let stdout_task = async {
            if let Some(mut r) = stdout_reader.take() {
                let _ = r.read_to_end(&mut stdout_buf).await;
            }
            stdout_buf
        };
        let stderr_task = async {
            if let Some(mut r) = stderr_reader.take() {
                let _ = r.read_to_end(&mut stderr_buf).await;
            }
            stderr_buf
        };
        let (out, err, status) = tokio::join!(stdout_task, stderr_task, child.wait());
        (out, err, status)
    };

    let (stdout_bytes, stderr_bytes, wait_result, timed_out) = if let Some(ms) = args.timeout_ms {
        match tokio::time::timeout(Duration::from_millis(ms), wait_fut).await {
            Ok((out, err, status)) => (out, err, status, false),
            Err(_) => {
                // Reap the whole process group: SIGTERM → grace → SIGKILL,
                // plus a snapshot-before-kill descendant sweep for anything
                // that escaped into its own process group
                // (`intent_acp::descendant_sweep`). On non-unix `kill_on_drop`
                // will still reap the direct child when `child` is dropped by
                // the returned future's scope.
                #[cfg(unix)]
                if let Some(pid) = pid {
                    let descendants = intent_acp::descendant_pids(pid).await;
                    kill_group(pid, nix::sys::signal::Signal::SIGTERM);
                    tokio::time::sleep(TERM_GRACE).await;
                    if matches!(child.try_wait(), Ok(Some(_))) {
                        // exited during grace
                    } else {
                        kill_group(pid, nix::sys::signal::Signal::SIGKILL);
                    }
                    intent_acp::sweep_escaped_descendants(&descendants).await;
                }
                #[cfg(not(unix))]
                let _ = pid;
                // Best-effort drain of whatever the child produced pre-timeout.
                let status = child.wait().await;
                let mut out = Vec::new();
                if let Some(mut r) = stdout_reader.take() {
                    let _ = r.read_to_end(&mut out).await;
                }
                let mut err = Vec::new();
                if let Some(mut r) = stderr_reader.take() {
                    let _ = r.read_to_end(&mut err).await;
                }
                (out, err, status, true)
            }
        }
    } else {
        let (out, err, status) = wait_fut.await;
        (out, err, status, false)
    };

    let status = wait_result.map_err(|e| HostExecError::internal(format!("wait failed: {e}")))?;
    let exit_code = status.code().unwrap_or(-1);
    let mut result = json!({
        "stdout": String::from_utf8_lossy(&stdout_bytes),
        "stderr": String::from_utf8_lossy(&stderr_bytes),
        "exitCode": exit_code,
    });
    if timed_out {
        result["timedOut"] = json!(true);
    }
    Ok(result)
}

/// Convenience for the transport layer: run with the default v1 policy.
pub async fn run_default(
    api: &dyn WorkspaceApi,
    args: HostExecArgs,
) -> Result<Value, HostExecError> {
    static POLICY: AllowAllPolicy = AllowAllPolicy;
    run(api, args, &POLICY).await
}

/// `WorkspaceApi::host_exec` seam for the `ws.host.exec` binding: parse the
/// raw JS args object, pin the containment root to the calling workspace (any
/// caller-supplied `workspaceId` is overwritten before parsing), run with the
/// default policy, and fold [`HostExecError`] codes onto the domain error enum
/// (`-32602` → `InvalidParams`, else `Internal`).
pub async fn run_for_workspace(
    api: &dyn WorkspaceApi,
    workspace_id: WorkspaceId,
    params: Value,
) -> intent_core::Result<Value> {
    let mut map = match params {
        Value::Object(m) => m,
        Value::Null => Map::new(),
        _ => {
            return Err(intent_core::Error::InvalidParams(
                "host.exec args must be an object".to_string(),
            ))
        }
    };
    map.insert(
        "workspaceId".to_string(),
        Value::String(workspace_id.as_str().to_string()),
    );
    let args = parse_args(&map).map_err(domain_err)?;
    run_default(api, args).await.map_err(domain_err)
}

/// Fold a [`HostExecError`] onto the domain error enum for the trait seam.
fn domain_err(e: HostExecError) -> intent_core::Error {
    match e.code {
        INVALID_PARAMS => intent_core::Error::InvalidParams(e.message),
        _ => intent_core::Error::Internal(e.message),
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
    fn parse_args_requires_command() {
        let err = parse_args(&map(json!({}))).unwrap_err();
        assert_eq!(err.code, INVALID_PARAMS);
        assert!(err.message.contains("command"));
    }

    #[test]
    fn parse_args_rejects_non_string_args() {
        let err = parse_args(&map(json!({ "command": "echo", "args": [1, 2] }))).unwrap_err();
        assert_eq!(err.code, INVALID_PARAMS);
    }

    #[test]
    fn parse_args_caps_timeout() {
        let a = parse_args(&map(json!({
            "command": "echo",
            "timeoutMs": MAX_TIMEOUT_MS + 1_000,
        })))
        .unwrap();
        assert_eq!(a.timeout_ms, Some(MAX_TIMEOUT_MS));
    }

    #[test]
    fn parse_args_cwd_requires_workspace_id() {
        let err = parse_args(&map(json!({ "command": "echo", "cwd": "/tmp" }))).unwrap_err();
        assert_eq!(err.code, INVALID_PARAMS);
        assert!(err.message.contains("workspaceId"));
    }

    #[test]
    fn parse_args_defaults_are_empty() {
        let a = parse_args(&map(json!({ "command": "echo" }))).unwrap();
        assert!(a.args.is_empty());
        assert!(a.env.is_empty());
        assert!(a.cwd.is_none());
        assert!(a.timeout_ms.is_none());
    }

    #[test]
    fn allow_all_policy_never_rejects() {
        let p = AllowAllPolicy;
        assert!(p.evaluate("rm", &["-rf".into(), "/".into()]).is_ok());
    }

    #[test]
    fn cwd_within_root_rejects_sibling_prefix() {
        // Raw-string `starts_with` would wrongly admit `/tmp/ws2` when the
        // root is `/tmp/ws`; the component-aware guard must reject it while
        // still admitting genuine subdirectories.
        let root = Path::new("/tmp/ws");
        assert!(!cwd_within_root(root, Path::new("/tmp/ws2")));
        assert!(!cwd_within_root(root, Path::new("/tmp/ws-other")));
        assert!(cwd_within_root(root, Path::new("/tmp/ws")));
        assert!(cwd_within_root(root, Path::new("/tmp/ws/sub")));
        assert!(cwd_within_root(root, Path::new("/tmp/ws/sub/nested")));
    }

    #[test]
    fn node_resolve_rejects_empty_base() {
        let err = node_resolve("", "sub").unwrap_err();
        assert_eq!(err.code, INTERNAL_ERROR);
        assert!(err.message.contains("absolute workspace root"));
    }

    #[test]
    fn node_resolve_rejects_relative_base() {
        let err = node_resolve("relative/base", "sub").unwrap_err();
        assert_eq!(err.code, INTERNAL_ERROR);
        assert!(err.message.contains("absolute workspace root"));
    }

    #[test]
    fn node_resolve_joins_absolute_base() {
        let full = node_resolve("/tmp/ws", "sub/nested").unwrap();
        assert_eq!(full, PathBuf::from("/tmp/ws/sub/nested"));
    }

    #[test]
    fn node_resolve_absolute_rel_wins() {
        let full = node_resolve("/tmp/ws", "/other/abs").unwrap();
        assert_eq!(full, PathBuf::from("/other/abs"));
    }

    fn btree(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn captured_env_fills_gap_when_absent_everywhere() {
        let captured = btree(&[("CODEX_TEST_FAKE", "from-shell")]);
        let caller = BTreeMap::new();
        let applied = captured_env_to_apply(&captured, &caller, |_| false);
        assert_eq!(applied, vec![("CODEX_TEST_FAKE", "from-shell")]);
    }

    #[test]
    fn captured_env_never_overrides_daemon_env() {
        let captured = btree(&[
            ("CODEX_TEST_FAKE", "from-shell"),
            ("CODEX_TEST_OTHER", "fills-gap"),
        ]);
        let caller = BTreeMap::new();
        let applied = captured_env_to_apply(&captured, &caller, |k| k == "CODEX_TEST_FAKE");
        assert_eq!(applied, vec![("CODEX_TEST_OTHER", "fills-gap")]);
    }

    #[test]
    fn caller_env_suppresses_captured_var() {
        let captured = btree(&[("CODEX_TEST_FAKE", "from-shell")]);
        let caller = btree(&[("CODEX_TEST_FAKE", "caller-wins")]);
        let applied = captured_env_to_apply(&captured, &caller, |_| false);
        assert!(applied.is_empty(), "caller env suppresses captured var");
    }

    /// Collect the explicitly-set env of a built command for assertions.
    fn cmd_envs(cmd: &Command) -> BTreeMap<String, String> {
        cmd.as_std()
            .get_envs()
            .filter_map(|(k, v)| {
                Some((
                    k.to_str()?.to_string(),
                    v?.to_str().unwrap_or_default().to_string(),
                ))
            })
            .collect()
    }

    #[test]
    fn build_command_applies_captured_under_caller_env() {
        // Var names chosen to be absent from any real process env so the
        // std::env probe inside the builder sees a genuine gap.
        let args = HostExecArgs {
            command: "echo".to_string(),
            args: vec![],
            cwd: None,
            env: btree(&[("INTENT_TEST_OVERRIDDEN", "caller-wins")]),
            timeout_ms: None,
            workspace_id: None,
        };
        let captured = btree(&[
            ("INTENT_TEST_CAPTURED", "from-shell"),
            ("INTENT_TEST_OVERRIDDEN", "from-shell"),
        ]);
        let cmd = build_command_with_captured(&args, None, &captured);
        let envs = cmd_envs(&cmd);
        assert_eq!(
            envs.get("INTENT_TEST_CAPTURED").map(String::as_str),
            Some("from-shell")
        );
        assert_eq!(
            envs.get("INTENT_TEST_OVERRIDDEN").map(String::as_str),
            Some("caller-wins"),
            "caller env wins over captured"
        );
    }

    #[test]
    fn build_command_captured_path_never_clobbers_enhanced_path() {
        // PATH is always present in the daemon's process env (and cargo
        // test's), so a captured PATH is filtered out and the enhanced PATH
        // set by the builder survives.
        assert!(std::env::var_os("PATH").is_some(), "test env has PATH");
        let args = HostExecArgs {
            command: "echo".to_string(),
            args: vec![],
            cwd: None,
            env: BTreeMap::new(),
            timeout_ms: None,
            workspace_id: None,
        };
        let captured = btree(&[("PATH", "/captured/only")]);
        let cmd = build_command_with_captured(&args, None, &captured);
        let envs = cmd_envs(&cmd);
        assert_ne!(
            envs.get("PATH").map(String::as_str),
            Some("/captured/only"),
            "captured PATH must not clobber the enhanced PATH"
        );
    }
}
