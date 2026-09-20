//! `intent-exec/1` client (monorepo#1120, EE-5).
//!
//! The helper's `--vsock-listen PORT=SOCKET` forwards connections made to the
//! host unix socket into the guest's vsock port, where the image's exec agent
//! accepts them. One connection = one command execution:
//!
//! 1. send ONE newline-terminated JSON header
//!    `{ "argv": [...], "env": {...}, "cwd": "...", "stderr": "merge"|"discard" }`
//! 2. read ONE newline-terminated JSON status line
//!    `{ "ok": true, "protocol": "intent-exec/1", "pid": N }` (or `ok: false`)
//! 3. on ok the socket carries the child's raw stdio (the guest agent relays
//!    it to real pipes on the child — libuv stdio needs pipes, not a vsock
//!    fd); EOF = child exited.

use std::collections::BTreeMap;
use std::path::Path;
use std::time::Duration;

use serde::Deserialize;
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;

use super::MicrovmError;

/// Wire protocol identifier this client speaks.
pub const EXEC_PROTOCOL: &str = "intent-exec/1";

/// Cap on the guest's status-line reply.
const STATUS_LINE_LIMIT: usize = 64 * 1024;

/// A command to run in the guest over the exec protocol.
#[derive(Debug, Clone)]
pub struct ExecRequest {
    pub argv: Vec<String>,
    pub env: BTreeMap<String, String>,
    pub cwd: String,
    /// `true` merges the child's stderr into the socket stream; `false`
    /// discards it guest-side (redirect it yourself in a shell wrapper when
    /// it must be captured).
    pub merge_stderr: bool,
}

/// Parsed guest status line.
#[derive(Debug, Deserialize)]
struct ExecStatus {
    ok: bool,
    #[serde(default)]
    protocol: String,
    #[serde(default)]
    pid: Option<u32>,
    #[serde(default)]
    error: Option<String>,
}

/// A live guest exec: the stream is the child's raw stdio from here on.
pub struct GuestExec {
    pub stream: UnixStream,
    pub guest_pid: Option<u32>,
}

/// Connect to the exec agent's forwarded unix socket and start `req`,
/// returning the stream positioned right after the status line.
///
/// # Errors
///
/// Returns `MicrovmError::Exec` when the connection, header exchange, or
/// guest-side start fails.
pub async fn start(socket: &Path, req: &ExecRequest) -> Result<GuestExec, MicrovmError> {
    let mut stream = UnixStream::connect(socket)
        .await
        .map_err(|e| MicrovmError::Exec(format!("connect {}: {e}", socket.display())))?;

    let header = json!({
        "argv": req.argv,
        "env": req.env,
        "cwd": req.cwd,
        "stderr": if req.merge_stderr { "merge" } else { "discard" },
    });
    let mut line = serde_json::to_vec(&header)
        .map_err(|e| MicrovmError::Exec(format!("encode header: {e}")))?;
    line.push(b'\n');
    stream
        .write_all(&line)
        .await
        .map_err(|e| MicrovmError::Exec(format!("send header: {e}")))?;

    let status = read_status_line(&mut stream).await?;
    let status: ExecStatus = serde_json::from_slice(&status)
        .map_err(|e| MicrovmError::Exec(format!("parse status line: {e}")))?;
    if status.protocol != EXEC_PROTOCOL {
        return Err(MicrovmError::Exec(format!(
            "unsupported exec protocol {:?} (expected {EXEC_PROTOCOL:?})",
            status.protocol
        )));
    }
    if !status.ok {
        return Err(MicrovmError::Exec(format!(
            "guest exec refused: {}",
            status.error.unwrap_or_else(|| "unknown error".to_string())
        )));
    }
    Ok(GuestExec {
        stream,
        guest_pid: status.pid,
    })
}

/// [`start`] bounded by `timeout` over connect + header write + status-line
/// read. Streaming callers (the provider launch) use this so a guest agent
/// that accepts the connection but never answers cannot hang the spawn.
///
/// # Errors
///
/// Returns `MicrovmError::Exec` on any [`start`] failure, or when the guest
/// has not answered the status line within `timeout`.
pub async fn start_within(
    socket: &Path,
    req: &ExecRequest,
    timeout: Duration,
) -> Result<GuestExec, MicrovmError> {
    tokio::time::timeout(timeout, start(socket, req))
        .await
        .map_err(|_| {
            MicrovmError::Exec(format!(
                "guest exec agent did not acknowledge the start within {timeout:?}"
            ))
        })?
}

/// Connect + start `req`, then wait for the child to exit (EOF), returning
/// everything the child wrote to the socket. Used for setup commands that
/// must complete before the provider launches.
///
/// # Errors
///
/// Returns `MicrovmError::Exec` when the exec fails or `timeout` elapses.
pub async fn run_to_completion(
    socket: &Path,
    req: &ExecRequest,
    timeout: Duration,
) -> Result<Vec<u8>, MicrovmError> {
    let fut = async {
        let mut exec = start(socket, req).await?;
        let mut out = Vec::new();
        exec.stream
            .read_to_end(&mut out)
            .await
            .map_err(|e| MicrovmError::Exec(format!("read output: {e}")))?;
        Ok(out)
    };
    tokio::time::timeout(timeout, fut)
        .await
        .map_err(|_| MicrovmError::Exec(format!("guest exec timed out after {timeout:?}")))?
}

/// Read up to and including the first `\n` (bounded), byte-at-a-time so no
/// post-status bytes (the child's stdout) are consumed from the stream.
async fn read_status_line(stream: &mut UnixStream) -> Result<Vec<u8>, MicrovmError> {
    let mut line = Vec::with_capacity(128);
    let mut byte = [0u8; 1];
    loop {
        let n = stream
            .read(&mut byte)
            .await
            .map_err(|e| MicrovmError::Exec(format!("read status line: {e}")))?;
        if n == 0 {
            return Err(MicrovmError::Exec(
                "connection closed before status line".to_string(),
            ));
        }
        if byte[0] == b'\n' {
            return Ok(line);
        }
        line.push(byte[0]);
        if line.len() > STATUS_LINE_LIMIT {
            return Err(MicrovmError::Exec("status line too long".to_string()));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncBufReadExt;
    use tokio::net::UnixListener;

    /// Fake guest exec agent: accept one connection, parse the header line,
    /// reply with `status`, then echo `payload` and close.
    fn fake_agent(
        socket: &std::path::Path,
        status: String,
        payload: &'static [u8],
    ) -> tokio::task::JoinHandle<serde_json::Value> {
        let listener = UnixListener::bind(socket).expect("bind fake agent");
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let mut reader = tokio::io::BufReader::new(stream);
            let mut header = String::new();
            reader.read_line(&mut header).await.expect("read header");
            let parsed: serde_json::Value = serde_json::from_str(&header).expect("header json");
            let mut stream = reader.into_inner();
            stream
                .write_all(format!("{status}\n").as_bytes())
                .await
                .expect("write status");
            stream.write_all(payload).await.expect("write payload");
            drop(stream);
            parsed
        })
    }

    fn sock_path(name: &str) -> std::path::PathBuf {
        // Short path: macOS caps sun_path at 104 bytes.
        std::env::temp_dir().join(format!("iexec-{name}-{}.sock", std::process::id()))
    }

    #[tokio::test]
    async fn header_framing_and_ok_status() {
        let sock = sock_path("ok");
        let _ = std::fs::remove_file(&sock);
        let server = fake_agent(
            &sock,
            format!(r#"{{"ok":true,"protocol":"{EXEC_PROTOCOL}","pid":42}}"#),
            b"hello from guest",
        );

        let req = ExecRequest {
            argv: vec!["/bin/echo".into(), "hi".into()],
            env: BTreeMap::from([("A".to_string(), "b".to_string())]),
            cwd: "/workspace".into(),
            merge_stderr: true,
        };
        let out = run_to_completion(&sock, &req, Duration::from_secs(5))
            .await
            .expect("exec ok");
        assert_eq!(out, b"hello from guest");

        // ONE newline-terminated JSON header with the documented fields.
        let header = server.await.expect("server task");
        assert_eq!(header["argv"][0], "/bin/echo");
        assert_eq!(header["env"]["A"], "b");
        assert_eq!(header["cwd"], "/workspace");
        assert_eq!(header["stderr"], "merge");
        std::fs::remove_file(&sock).ok();
    }

    #[tokio::test]
    async fn refused_and_wrong_protocol_are_errors() {
        for (name, status, want) in [
            (
                "refused",
                format!(r#"{{"ok":false,"protocol":"{EXEC_PROTOCOL}","error":"no such file"}}"#),
                "guest exec refused",
            ),
            (
                "wrongproto",
                r#"{"ok":true,"protocol":"intent-exec/999"}"#.to_string(),
                "unsupported exec protocol",
            ),
        ] {
            let sock = sock_path(name);
            let _ = std::fs::remove_file(&sock);
            let _server = fake_agent(&sock, status, b"");
            let req = ExecRequest {
                argv: vec!["/bin/true".into()],
                env: BTreeMap::new(),
                cwd: "/".into(),
                merge_stderr: false,
            };
            let err = run_to_completion(&sock, &req, Duration::from_secs(5))
                .await
                .expect_err("must fail");
            assert!(
                err.to_string().contains(want),
                "{name}: expected {want:?} in {err}"
            );
            std::fs::remove_file(&sock).ok();
        }
    }

    /// Regression (#873 review): a guest agent that accepts the connection
    /// but never writes the status line must not hang the streaming start;
    /// `start_within` fails with a structured timeout error.
    #[tokio::test]
    async fn start_within_times_out_when_guest_never_answers() {
        let sock = sock_path("wedged");
        let _ = std::fs::remove_file(&sock);
        let listener = UnixListener::bind(&sock).expect("bind wedged agent");
        let wedged = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            // Hold the connection open without answering until the test ends.
            std::future::pending::<()>().await;
            drop(stream);
        });
        let req = ExecRequest {
            argv: vec!["/bin/true".into()],
            env: BTreeMap::new(),
            cwd: "/".into(),
            merge_stderr: false,
        };
        let Err(err) = start_within(&sock, &req, Duration::from_millis(200)).await else {
            panic!("must time out");
        };
        assert!(matches!(err, MicrovmError::Exec(_)), "{err}");
        assert!(
            err.to_string()
                .contains("did not acknowledge the start within 200ms"),
            "{err}"
        );
        wedged.abort();
        std::fs::remove_file(&sock).ok();
    }

    /// Path of the guest exec agent script shipped in the image
    /// (`guest-image/intent-vsock-exec`, copied verbatim by the Dockerfile).
    fn guest_agent_script() -> std::path::PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../guest-image/intent-vsock-exec")
            .canonicalize()
            .expect("guest-image/intent-vsock-exec exists")
    }

    /// Run the REAL guest agent's `handle()` behind a unix socket (its
    /// `main()` is vsock-only) so the host client is exercised against the
    /// script that ships in the image. Returns `None` when no `python3` is
    /// on PATH.
    fn real_agent(socket: &std::path::Path, connections: usize) -> Option<std::process::Child> {
        let have_python = std::process::Command::new("python3")
            .arg("--version")
            .output()
            .is_ok_and(|o| o.status.success());
        if !have_python {
            eprintln!("skipping: python3 not available");
            return None;
        }
        let driver = r#"
import importlib.machinery, importlib.util, socket, sys
loader = importlib.machinery.SourceFileLoader("vexec", sys.argv[1])
spec = importlib.util.spec_from_loader("vexec", loader)
mod = importlib.util.module_from_spec(spec)
loader.exec_module(mod)
srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
srv.bind(sys.argv[2])
srv.listen(4)
sys.stdout.write("READY\n")
sys.stdout.flush()
for _ in range(int(sys.argv[3])):
    conn, _ = srv.accept()
    try:
        mod.handle(conn)
    finally:
        conn.close()
"#;
        let mut child = std::process::Command::new("python3")
            .arg("-c")
            .arg(driver)
            .arg(guest_agent_script())
            .arg(socket)
            .arg(connections.to_string())
            // Keep the checkout clean: no __pycache__ beside the script.
            .env("PYTHONDONTWRITEBYTECODE", "1")
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::inherit())
            .spawn()
            .expect("spawn python3 driver");
        let mut ready = String::new();
        std::io::BufRead::read_line(
            &mut std::io::BufReader::new(child.stdout.take().unwrap()),
            &mut ready,
        )
        .expect("read READY");
        assert_eq!(ready.trim(), "READY");
        Some(child)
    }

    #[tokio::test]
    async fn real_agent_refuses_missing_cwd_before_ok_and_runs_in_valid_cwd() {
        let sock = sock_path("realagent");
        let _ = std::fs::remove_file(&sock);
        let Some(mut agent) = real_agent(&sock, 2) else {
            return;
        };
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("vanished-mount");

        // 1. Missing cwd: the agent must answer `ok: false` (never start the
        //    child in `/`), which the host maps to a structured exec error.
        let req = ExecRequest {
            argv: vec!["/bin/sh".into(), "-c".into(), "pwd -P".into()],
            env: BTreeMap::new(),
            cwd: missing.display().to_string(),
            merge_stderr: true,
        };
        let err = run_to_completion(&sock, &req, Duration::from_secs(10))
            .await
            .expect_err("missing cwd must refuse the exec");
        assert!(matches!(err, MicrovmError::Exec(_)), "{err}");
        let msg = err.to_string();
        assert!(msg.contains("guest exec refused"), "{msg}");
        assert!(msg.contains("cwd is not an existing directory"), "{msg}");
        assert!(msg.contains("vanished-mount"), "{msg}");

        // 2. Valid cwd: the child really runs there.
        let req = ExecRequest {
            cwd: dir.path().display().to_string(),
            ..req
        };
        let out = run_to_completion(&sock, &req, Duration::from_secs(10))
            .await
            .expect("valid cwd runs");
        let want = std::fs::canonicalize(dir.path()).unwrap();
        assert_eq!(
            String::from_utf8_lossy(&out).trim(),
            want.display().to_string()
        );

        let status = agent.wait().expect("driver exits");
        assert!(status.success(), "driver exit: {status}");
        std::fs::remove_file(&sock).ok();
    }
}
