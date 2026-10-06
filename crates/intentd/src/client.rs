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
    loop {
        line.clear();
        let n = reader.read_line(&mut line).await?;
        if n == 0 {
            anyhow::bail!("daemon closed the connection without responding");
        }
        let response: Value = serde_json::from_str(line.trim())
            .map_err(|e| anyhow::anyhow!("invalid response from daemon: {e}"))?;
        // Listener shutdown can publish retirement notifications before the
        // control reply. Only this request's response completes the call.
        if response.get("id") == request.get("id") {
            return Ok(response);
        }
    }
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

#[cfg(all(test, any(unix, windows)))]
mod tests {
    use super::exchange;
    use serde_json::{json, Value};
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

    async fn exchange_with_frames(frames: &str) -> anyhow::Result<Value> {
        let (client, server) = tokio::io::duplex(4096);
        let peer = async {
            let mut server = BufReader::new(server);
            let mut request = String::new();
            server.read_line(&mut request).await.unwrap();
            assert_eq!(
                serde_json::from_str::<Value>(&request).unwrap(),
                json!({"jsonrpc": "2.0", "id": 1, "method": "system.shutdown", "params": {}})
            );
            server.get_mut().write_all(frames.as_bytes()).await.unwrap();
        };
        let (response, ()) = tokio::join!(exchange(client, "system.shutdown", json!({})), peer);
        response
    }

    #[tokio::test]
    async fn shutdown_response_survives_preceding_retirement_notifications() {
        let response = exchange_with_frames(concat!(
            "{\"jsonrpc\":\"2.0\",\"method\":\"workspace.repositoryContext.retired\",\"params\":{\"terminal\":true}}\n",
            "{\"jsonrpc\":\"2.0\",\"method\":\"workspace.repositorySelection.retired\",\"params\":{\"terminal\":true}}\n",
            "{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"ok\":true,\"stopping\":true}}\n",
        ))
        .await
        .unwrap();
        assert_eq!(
            response,
            json!({"jsonrpc":"2.0","id":1,"result":{"ok":true,"stopping":true}})
        );
    }

    #[tokio::test]
    async fn only_the_matching_response_id_completes_the_call() {
        let response = exchange_with_frames(concat!(
            "{\"jsonrpc\":\"2.0\",\"id\":2,\"result\":{\"wrong\":true}}\n",
            "{\"jsonrpc\":\"2.0\",\"id\":\"1\",\"result\":{\"wrong\":true}}\n",
            "{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"ok\":true}}\n",
        ))
        .await
        .unwrap();
        assert_eq!(response["result"], json!({"ok":true}));
    }

    #[tokio::test]
    async fn matching_rpc_errors_are_preserved_after_notifications() {
        let response = exchange_with_frames(concat!(
            "{\"jsonrpc\":\"2.0\",\"method\":\"workspace.repositoryContext.retired\",\"params\":{}}\n",
            "{\"jsonrpc\":\"2.0\",\"id\":1,\"error\":{\"code\":-32001,\"message\":\"refused\"}}\n",
        ))
        .await
        .unwrap();
        assert_eq!(
            response,
            json!({"jsonrpc":"2.0","id":1,"error":{"code":-32001,"message":"refused"}})
        );
    }

    #[tokio::test]
    async fn eof_without_a_matching_reply_is_not_success() {
        for frames in [
            "",
            "{\"jsonrpc\":\"2.0\",\"method\":\"workspace.repositoryContext.retired\",\"params\":{}}\n",
            "{\"jsonrpc\":\"2.0\",\"id\":2,\"result\":{\"ok\":true}}\n",
        ] {
            let error = exchange_with_frames(frames).await.unwrap_err();
            assert!(error.to_string().contains("without responding"), "{error}");
        }
    }

    #[tokio::test]
    async fn malformed_reply_remains_an_error() {
        let error = exchange_with_frames("not JSON\n").await.unwrap_err();
        assert!(
            error.to_string().contains("invalid response from daemon"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn direct_response_is_preserved() {
        let response =
            exchange_with_frames("{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"ok\":true}}\n")
                .await
                .unwrap();
        assert_eq!(
            response,
            json!({"jsonrpc":"2.0","id":1,"result":{"ok":true}})
        );
    }
}
