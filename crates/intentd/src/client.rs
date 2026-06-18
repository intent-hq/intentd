//! Thin UDS JSON-RPC client used by the `call` / `status` subcommands (§5.7).
//!
//! UDS is Unix-only; the client is gated behind `#[cfg(unix)]`. On other
//! platforms `rpc_call` builds but returns an error at runtime.

use std::path::Path;

use serde_json::Value;

#[cfg(unix)]
use serde_json::json;
#[cfg(unix)]
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
#[cfg(unix)]
use tokio::net::UnixStream;

/// Connect to the daemon socket, send one request, and return the parsed
/// response envelope. The request `id` is auto-generated.
#[cfg(unix)]
pub async fn rpc_call(socket: &Path, method: &str, params: Value) -> anyhow::Result<Value> {
    let mut stream = UnixStream::connect(socket)
        .await
        .map_err(|e| anyhow::anyhow!("cannot connect to daemon at {}: {e}", socket.display()))?;

    let request = json!({ "jsonrpc": "2.0", "id": 1, "method": method, "params": params });
    let mut frame = serde_json::to_string(&request)?;
    frame.push('\n');

    let (read_half, mut write_half) = stream.split();
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

/// Non-Unix fallback: UDS is unavailable, so report a clear runtime error
/// instead of failing to compile.
#[cfg(not(unix))]
pub async fn rpc_call(_socket: &Path, _method: &str, _params: Value) -> anyhow::Result<Value> {
    anyhow::bail!("UDS transport is not supported on this platform")
}
