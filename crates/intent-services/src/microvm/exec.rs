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
}
