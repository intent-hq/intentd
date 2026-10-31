//! One-shot UDS JSON-RPC exchange correlated by reply `id`.
//!
//! A UDS connection carries notifications on the same stream as replies, and
//! some (the terminal `*.retired` feeds emitted when listener shutdown retires
//! every live connection) share the priority lane with control replies such as
//! `system.shutdown`. Under load one can arrive before the reply, so a helper
//! that treats the first line as the acknowledgement can be falsely satisfied
//! or fail (intent-hq/intent#8199). Correlate by `id` like the shipped CLI
//! (`intentd::client::exchange`): skip only id-less notifications, and fail on
//! EOF, a foreign-id frame, or one overall read timeout.

use std::path::Path;

use serde_json::{json, Value};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tokio::time::timeout;

/// Send one request on a fresh UDS connection and return the reply whose `id`
/// matches, skipping interleaved id-less notifications.
pub async fn uds_rpc(socket: &Path, id: i64, method: &str, params: Value) -> Value {
    let stream = UnixStream::connect(socket).await.expect("connect uds");
    let (read_half, mut write_half) = stream.into_split();
    let mut line =
        json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params }).to_string();
    line.push('\n');
    write_half.write_all(line.as_bytes()).await.unwrap();
    write_half.flush().await.unwrap();
    let mut reader = BufReader::new(read_half);
    timeout(
        super::rpc_read_timeout(),
        read_reply(&mut reader, id, method),
    )
    .await
    .unwrap_or_else(|_| panic!("UDS RPC {method} (id {id}) timed out waiting for response"))
}

/// Read frames until the reply whose `id` matches; the caller bounds it with
/// one overall timeout.
pub async fn read_reply<R: AsyncBufRead + Unpin>(reader: &mut R, id: i64, method: &str) -> Value {
    loop {
        let mut buf = String::new();
        let n = reader.read_line(&mut buf).await.expect("read uds response");
        assert!(
            n > 0,
            "UDS RPC {method} (id {id}): daemon closed the connection without responding"
        );
        let frame: Value = serde_json::from_str(buf.trim_end()).expect("invalid JSON frame");
        if frame.get("id") == Some(&json!(id)) {
            return frame;
        }
        assert!(
            frame.get("id").is_none() && frame["method"].is_string(),
            "UDS RPC {method} (id {id}): unexpected non-notification frame {frame}"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::UnixListener;

    /// Serve one connection: read the request line, then write `frames`
    /// verbatim and close.
    fn serve_once(frames: &'static [&'static str]) -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("d.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (read_half, mut write_half) = stream.into_split();
            let mut request = String::new();
            BufReader::new(read_half)
                .read_line(&mut request)
                .await
                .unwrap();
            for frame in frames {
                write_half.write_all(frame.as_bytes()).await.unwrap();
                write_half.write_all(b"\n").await.unwrap();
            }
            write_half.flush().await.unwrap();
        });
        (dir, socket)
    }

    #[tokio::test]
    async fn skips_retirement_notification_before_the_reply() {
        let (_dir, socket) = serve_once(&[
            r#"{"jsonrpc":"2.0","method":"accept-changes.retired","params":{"operationIds":[]}}"#,
            r#"{"jsonrpc":"2.0","id":7,"result":{"ok":true}}"#,
        ]);
        let reply = uds_rpc(&socket, 7, "system.shutdown", json!({})).await;
        assert_eq!(reply["result"]["ok"], json!(true), "{reply}");
    }

    #[tokio::test]
    #[should_panic(expected = "unexpected non-notification frame")]
    async fn rejects_a_foreign_id_reply() {
        let (_dir, socket) = serve_once(&[r#"{"jsonrpc":"2.0","id":8,"result":{"ok":true}}"#]);
        uds_rpc(&socket, 7, "system.shutdown", json!({})).await;
    }

    #[tokio::test]
    #[should_panic(expected = "closed the connection without responding")]
    async fn rejects_eof_after_only_notifications() {
        let (_dir, socket) = serve_once(&[
            r#"{"jsonrpc":"2.0","method":"workspace.repositorySelection.retired","params":{"selectionIds":[]}}"#,
        ]);
        uds_rpc(&socket, 7, "system.shutdown", json!({})).await;
    }
}
