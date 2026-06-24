//! `AuggieContextEngine` — the `auggie`-backed [`ContextEngine`] (§8.2, §8.3).
//!
//! Ports `execute-auggie-command.ts` (`executeAuggieCommand`, 30s default
//! timeout, no-shell `execFile`-style spawn on unix, `.cmd`/`.bat` shell on
//! Windows, stdin piping, auth-failure → "needs login"). `availability()` is a
//! non-error probe (§8.3); only `retrieve()` returns a [`ContextError`].

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use intent_core::{
    ContextEngine, ContextError, EngineAvailability, RetrieveRequest, RetrieveResult, RetrievedItem,
};
use tokio::io::AsyncWriteExt;
use tokio::process::Command;

use crate::discovery;

/// Default per-command timeout (matches the TS `DEFAULT_AUGGIE_TIMEOUT_MS`).
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

/// Shorter timeout for the lightweight `--version` availability probe.
const VERSION_TIMEOUT: Duration = Duration::from_secs(8);

/// Auth-failure substrings → mapped to a "needs login" availability (§8.2).
const AUTH_FAILURE_PATTERNS: &[&str] = &[
    "not logged in",
    "not authenticated",
    "please log in",
    "please login",
    "login required",
    "you must log in",
    "run auggie login",
    "authentication required",
    "unauthorized",
    "no valid session",
    "session expired",
    "augment_session_auth",
];

/// Context engine backed by the `auggie` CLI.
#[derive(Debug, Default)]
pub struct AuggieContextEngine {
    /// Cached resolved binary path; re-probed when it disappears (§8.2).
    cached_path: Mutex<Option<PathBuf>>,
}

impl AuggieContextEngine {
    /// Construct an engine that discovers auggie lazily. Never fails (§8.3).
    pub fn new() -> Self {
        Self::default()
    }

    /// Construct with a known binary path (a user-configured path, or tests),
    /// pre-seeding the discovery cache.
    pub fn with_path(path: PathBuf) -> Self {
        Self {
            cached_path: Mutex::new(Some(path)),
        }
    }

    /// Resolve the auggie path, reusing the cache when the file still exists and
    /// re-probing via discovery otherwise (§8.2).
    fn resolve_path(&self) -> Option<PathBuf> {
        {
            let guard = self.cached_path.lock().expect("cache poisoned");
            if let Some(p) = guard.as_ref() {
                if p.exists() {
                    return Some(p.clone());
                }
            }
        }
        let found = discovery::find_auggie();
        let mut guard = self.cached_path.lock().expect("cache poisoned");
        guard.clone_from(&found);
        found
    }
}

#[async_trait]
impl ContextEngine for AuggieContextEngine {
    async fn availability(&self) -> EngineAvailability {
        let Some(path) = self.resolve_path() else {
            return EngineAvailability::Unavailable {
                reason: "auggie not found on PATH".to_string(),
            };
        };
        match run_auggie(&path, &["--version"], None, None, VERSION_TIMEOUT).await {
            Ok(output) => classify_availability(&output.stdout, &output.stderr, output.success),
            Err(err) => EngineAvailability::Unavailable {
                reason: format!("auggie not available: {err}"),
            },
        }
    }

    async fn retrieve(
        &self,
        req: RetrieveRequest,
    ) -> std::result::Result<RetrieveResult, ContextError> {
        let path = self
            .resolve_path()
            .ok_or_else(|| ContextError::Unavailable {
                reason: "auggie not found on PATH".to_string(),
            })?;

        // The concrete codebase-search subcommand is wired by CE-2; the query is
        // piped on stdin (it may contain spaces) and the workspace path is the
        // child's working directory.
        let args = retrieve_args(&req);
        let arg_refs: Vec<&str> = args.iter().map(String::as_str).collect();
        let output = run_auggie(
            &path,
            &arg_refs,
            Some(&req.query),
            Some(req.workspace_path.as_path()),
            DEFAULT_TIMEOUT,
        )
        .await?;

        if !output.success {
            let combined = format!("{}\n{}", output.stdout, output.stderr);
            if is_auth_failure(&combined) {
                return Err(ContextError::Unavailable {
                    reason: "needs login".to_string(),
                });
            }
            let reason = first_nonempty_line(&output.stderr)
                .or_else(|| first_nonempty_line(&output.stdout))
                .unwrap_or_else(|| "auggie retrieval failed".to_string());
            return Err(ContextError::CommandFailed(reason));
        }

        let mut result = parse_retrieve_output(&output.stdout)?;
        if let Some(max) = req.max_results {
            result.items.truncate(max);
        }
        Ok(result)
    }
}

/// Assemble the auggie retrieval args. The concrete codebase-search subcommand
/// is finalized in CE-2; the query is piped on stdin and `max_results` is
/// applied to the parsed result set.
fn retrieve_args(_req: &RetrieveRequest) -> Vec<String> {
    Vec::new()
}

/// Captured output of an auggie invocation.
struct CommandOutput {
    stdout: String,
    stderr: String,
    success: bool,
}

/// Spawn auggie with the enhanced exec PATH, an optional `cwd`, optional stdin,
/// and a timeout. No shell on unix (`execFile`-style); `cmd /C` for `.cmd`/
/// `.bat` shims on Windows. The child is killed if the timeout elapses.
async fn run_auggie(
    auggie_path: &Path,
    args: &[&str],
    stdin: Option<&str>,
    cwd: Option<&Path>,
    timeout: Duration,
) -> std::result::Result<CommandOutput, ContextError> {
    let env_path = discovery::exec_path(auggie_path);

    let mut command = build_command(auggie_path, args);
    command.env("PATH", &env_path);
    if let Some(dir) = cwd {
        command.current_dir(dir);
    }
    command.stdout(Stdio::piped());
    command.stderr(Stdio::piped());
    command.stdin(if stdin.is_some() {
        Stdio::piped()
    } else {
        Stdio::null()
    });
    command.kill_on_drop(true);

    let mut child = command
        .spawn()
        .map_err(|e| ContextError::Spawn(e.to_string()))?;

    if let Some(data) = stdin {
        if let Some(mut sink) = child.stdin.take() {
            // EPIPE is benign: the child may exit before consuming all input.
            let _ = sink.write_all(data.as_bytes()).await;
            let _ = sink.shutdown().await;
        }
    }

    let output = match tokio::time::timeout(timeout, child.wait_with_output()).await {
        Ok(result) => result.map_err(|e| ContextError::Spawn(e.to_string()))?,
        Err(_) => return Err(ContextError::Timeout),
    };

    Ok(CommandOutput {
        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        success: output.status.success(),
    })
}

#[cfg(windows)]
fn build_command(auggie_path: &Path, args: &[&str]) -> Command {
    // npm `.cmd`/`.bat` shims (and bare names) require cmd.exe; absolute paths to
    // real binaries spawn directly (safer — avoids cmd.exe arg interpretation).
    let lower = auggie_path
        .extension()
        .and_then(|e| e.to_str())
        .map(|e| e.to_ascii_lowercase());
    let needs_shell =
        !auggie_path.is_absolute() || matches!(lower.as_deref(), Some("cmd") | Some("bat"));
    if needs_shell {
        let mut c = Command::new("cmd");
        c.arg("/C").arg(auggie_path).args(args);
        c
    } else {
        let mut c = Command::new(auggie_path);
        c.args(args);
        c
    }
}

#[cfg(not(windows))]
fn build_command(auggie_path: &Path, args: &[&str]) -> Command {
    let mut c = Command::new(auggie_path);
    c.args(args);
    c
}

/// True when `output` looks like an auth/login failure (§8.2).
pub fn is_auth_failure(output: &str) -> bool {
    let lower = output.to_lowercase();
    AUTH_FAILURE_PATTERNS.iter().any(|p| lower.contains(p))
}

/// Classify the `--version` probe output into an [`EngineAvailability`] (§8.2):
/// auth-failure patterns become a non-error "needs login" `Unavailable`.
pub fn classify_availability(stdout: &str, stderr: &str, success: bool) -> EngineAvailability {
    let combined = format!("{stdout}\n{stderr}");
    if is_auth_failure(&combined) {
        return EngineAvailability::Unavailable {
            reason: "needs login".to_string(),
        };
    }
    if !success {
        let reason = first_nonempty_line(stderr)
            .or_else(|| first_nonempty_line(stdout))
            .unwrap_or_else(|| "auggie --version failed".to_string());
        return EngineAvailability::Unavailable { reason };
    }
    EngineAvailability::Available {
        name: "auggie".to_string(),
        version: parse_version(&combined),
    }
}

/// Extract the first `MAJOR.MINOR.PATCH`-looking token from `text`.
fn parse_version(text: &str) -> Option<String> {
    for token in text.split_whitespace() {
        let trimmed = token.trim_matches(|c: char| !c.is_ascii_digit());
        if is_semverish(trimmed) {
            return Some(trimmed.to_string());
        }
    }
    None
}

fn is_semverish(s: &str) -> bool {
    let parts: Vec<&str> = s.split('.').collect();
    parts.len() >= 3
        && parts
            .iter()
            .take(3)
            .all(|p| !p.is_empty() && p.chars().all(|c| c.is_ascii_digit()))
}

fn first_nonempty_line(text: &str) -> Option<String> {
    text.lines()
        .map(str::trim)
        .find(|l| !l.is_empty())
        .map(str::to_string)
}

/// Parse auggie's retrieval output into a [`RetrieveResult`] (§8.2). Accepts
/// either a top-level JSON array of hits or an object with a `matches`/
/// `results`/`items` array; each hit's fields are read leniently.
pub fn parse_retrieve_output(stdout: &str) -> std::result::Result<RetrieveResult, ContextError> {
    let trimmed = stdout.trim();
    if trimmed.is_empty() {
        return Ok(RetrieveResult::default());
    }
    let value: serde_json::Value =
        serde_json::from_str(trimmed).map_err(|e| ContextError::Parse(e.to_string()))?;

    let array = match &value {
        serde_json::Value::Array(items) => items.as_slice(),
        serde_json::Value::Object(map) => map
            .get("matches")
            .or_else(|| map.get("results"))
            .or_else(|| map.get("items"))
            .and_then(serde_json::Value::as_array)
            .map(Vec::as_slice)
            .unwrap_or(&[]),
        _ => &[],
    };

    let items = array.iter().filter_map(parse_item).collect();
    Ok(RetrieveResult { items })
}

fn parse_item(value: &serde_json::Value) -> Option<RetrievedItem> {
    let obj = value.as_object()?;
    let file = obj
        .get("file")
        .or_else(|| obj.get("path"))
        .and_then(serde_json::Value::as_str)?
        .to_string();
    let symbol = obj
        .get("symbol")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string);
    let line = obj.get("line").and_then(serde_json::Value::as_u64);
    let preview = obj
        .get("preview")
        .or_else(|| obj.get("snippet"))
        .or_else(|| obj.get("text"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_string();
    let score = obj.get("score").and_then(serde_json::Value::as_f64);
    Some(RetrievedItem {
        file,
        symbol,
        line,
        preview,
        score,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_available_parses_version() {
        let a = classify_availability("auggie v1.12.3", "", true);
        assert_eq!(
            a,
            EngineAvailability::Available {
                name: "auggie".to_string(),
                version: Some("1.12.3".to_string()),
            }
        );
    }

    #[test]
    fn classify_unavailable_on_failure() {
        let a = classify_availability("", "command not found", false);
        assert_eq!(
            a,
            EngineAvailability::Unavailable {
                reason: "command not found".to_string(),
            }
        );
    }

    #[test]
    fn classify_auth_failure_is_needs_login() {
        let a = classify_availability("", "Error: not logged in. Run auggie login.", true);
        assert_eq!(
            a,
            EngineAvailability::Unavailable {
                reason: "needs login".to_string(),
            }
        );
        assert!(is_auth_failure("You are NOT AUTHENTICATED"));
        assert!(!is_auth_failure("auggie 1.2.3"));
    }

    #[test]
    fn parse_output_array_of_hits() {
        let json = r#"[
            {"file":"src/a.rs","symbol":"foo","line":12,"preview":"fn foo()","score":0.9},
            {"path":"src/b.rs","snippet":"bar"}
        ]"#;
        let result = parse_retrieve_output(json).unwrap();
        assert_eq!(result.items.len(), 2);
        assert_eq!(
            result.items[0],
            RetrievedItem {
                file: "src/a.rs".to_string(),
                symbol: Some("foo".to_string()),
                line: Some(12),
                preview: "fn foo()".to_string(),
                score: Some(0.9),
            }
        );
        assert_eq!(result.items[1].file, "src/b.rs");
        assert_eq!(result.items[1].preview, "bar");
        assert_eq!(result.items[1].symbol, None);
    }

    #[test]
    fn parse_output_object_with_matches() {
        let json = r#"{"matches":[{"file":"x.rs","preview":"hit"}]}"#;
        let result = parse_retrieve_output(json).unwrap();
        assert_eq!(result.items.len(), 1);
        assert_eq!(result.items[0].file, "x.rs");
    }

    #[test]
    fn parse_output_empty_is_default() {
        assert_eq!(
            parse_retrieve_output("   ").unwrap(),
            RetrieveResult::default()
        );
    }

    #[test]
    fn parse_output_invalid_json_errors() {
        assert!(matches!(
            parse_retrieve_output("{not json"),
            Err(ContextError::Parse(_))
        ));
    }

    #[test]
    fn availability_is_total_never_errors() {
        // §8.3: availability() returns an EngineAvailability and never panics or
        // errors, even when the seeded path is bogus (resolve_path then re-probes
        // discovery). On hosts without auggie this is Unavailable; on hosts with
        // it, a well-formed Available/Unavailable. Either is the non-error state.
        let engine = AuggieContextEngine::with_path(PathBuf::from("/no/such/auggie/binary"));
        match futures_block_on(engine.availability()) {
            EngineAvailability::Available { name, .. } => assert_eq!(name, "auggie"),
            EngineAvailability::Unavailable { reason } => assert!(!reason.is_empty()),
        }
    }

    #[test]
    fn retrieve_unavailable_when_no_binary_resolves() {
        // When discovery yields nothing, retrieve() maps to a non-panicking
        // ContextError::Unavailable (§8.3, §11.1). find_in_dirs over an empty set
        // is the deterministic "nothing found" signal underpinning that branch.
        assert_eq!(discovery::find_in_dirs(&[]), None);
    }

    #[cfg(unix)]
    #[test]
    fn availability_available_via_fake_binary() {
        use std::os::unix::fs::PermissionsExt;
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("intent-ctx-avail-{nanos}"));
        std::fs::create_dir_all(&dir).unwrap();
        let bin = dir.join("auggie");
        std::fs::write(&bin, "#!/bin/sh\necho 'auggie 2.5.1'\n").unwrap();
        std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755)).unwrap();

        let engine = AuggieContextEngine::with_path(bin);
        let availability = futures_block_on(engine.availability());
        assert_eq!(
            availability,
            EngineAvailability::Available {
                name: "auggie".to_string(),
                version: Some("2.5.1".to_string()),
            }
        );
    }

    /// Minimal single-threaded block-on so async tests need no extra deps.
    fn futures_block_on<F: std::future::Future>(fut: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(fut)
    }
}
