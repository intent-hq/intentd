//! Thin local JSON-RPC client used by the `call` / `status` subcommands (§5.7).
//!
//! Connects over the platform's local transport: the Unix domain socket on
//! Unix, the named pipe on Windows (name derived from the resolved socket path
//! via `intent_transport::pipe_name_for_socket_path` — the exact helper the
//! listener binds with). On other platforms `rpc_call` builds but returns an
//! error at runtime.

use std::path::Path;

use serde_json::Value;

/// Send one JSON-RPC request over an established local-transport stream and
/// return the parsed response envelope. The request `id` is fixed at `1`
/// (one request per connection).
#[cfg(any(unix, windows))]
async fn exchange<S>(stream: S, method: &str, params: Value) -> anyhow::Result<Value>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

    let request =
        serde_json::json!({ "jsonrpc": "2.0", "id": 1, "method": method, "params": params });
    let mut frame = serde_json::to_string(&request)?;
    frame.push('\n');

    let (read_half, mut write_half) = tokio::io::split(stream);
    write_half.write_all(frame.as_bytes()).await?;
    write_half.flush().await?;

    let mut reader = BufReader::new(read_half);
    let mut line = String::new();
    let n = reader.read_line(&mut line).await?;
    if n == 0 {
        anyhow::bail!("daemon closed the connection without responding");
    }
    let response: Value = serde_json::from_str(line.trim())
        .map_err(|e| anyhow::anyhow!("invalid response from daemon: {e}"))?;
    Ok(response)
}

/// Connect to the daemon socket, send one request, and return the parsed
/// response envelope.
#[cfg(unix)]
pub async fn rpc_call(socket: &Path, method: &str, params: Value) -> anyhow::Result<Value> {
    let stream = tokio::net::UnixStream::connect(socket)
        .await
        .map_err(|e| anyhow::anyhow!("cannot connect to daemon at {}: {e}", socket.display()))?;
    exchange(stream, method, params).await
}

/// Connect to the daemon's named pipe (derived from the socket path), send one
/// request, and return the parsed response envelope. `ERROR_PIPE_BUSY` — all
/// server instances momentarily taken — is retried briefly per the tokio
/// named-pipe contract; the listener creates the next instance eagerly, so
/// contention clears quickly.
#[cfg(windows)]
pub async fn rpc_call(socket: &Path, method: &str, params: Value) -> anyhow::Result<Value> {
    use tokio::net::windows::named_pipe::ClientOptions;

    const ERROR_PIPE_BUSY: i32 = 231;

    let pipe = intent_transport::pipe_name_for_socket_path(socket)
        .map_err(|e| anyhow::anyhow!("cannot derive pipe name for {}: {e}", socket.display()))?;
    let mut attempts = 0u32;
    let stream = loop {
        match ClientOptions::new().open(&pipe) {
            Ok(s) => break s,
            Err(e) if e.raw_os_error() == Some(ERROR_PIPE_BUSY) && attempts < 10 => {
                attempts += 1;
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            Err(e) => anyhow::bail!(
                "cannot connect to daemon at {pipe} (socket {}): {e}",
                socket.display()
            ),
        }
    };
    exchange(stream, method, params).await
}

/// Fallback for targets that are neither unix nor windows: there is no local
/// transport, so report a clear runtime error instead of failing to compile.
#[cfg(not(any(unix, windows)))]
pub async fn rpc_call(_socket: &Path, _method: &str, _params: Value) -> anyhow::Result<Value> {
    anyhow::bail!("local IPC transport is not supported on this platform")
}
