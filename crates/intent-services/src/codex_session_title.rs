//! codex-acp 2.1.1 starts its title-generation thread without `CODEX_CONFIG`.
//! An explicit title suppresses that auxiliary inference. Keep the existing
//! title on restoration and confirm the supported /rename control before any
//! user turn; neither a response alone nor a stale title is confirmation.
use std::time::Duration;

use intent_acp::{AcpError, AcpResult, Connection};
use serde_json::json;

pub(crate) async fn confirm(
    conn: &Connection,
    session_id: &str,
    fallback: &str,
    timeout: Duration,
) -> AcpResult<()> {
    let (revision, existing) = conn.session_title(session_id).unwrap_or_default();
    let title = existing
        .as_deref()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or(fallback);
    let title = title.trim();
    if title.is_empty() {
        return Err(AcpError::Protocol(
            "Codex session needs a nonempty title".into(),
        ));
    }
    let _control = conn.session_title_control(session_id, title)?;
    let operation = async {
        let response = conn
            .request_timeout(
                "session/prompt",
                json!({
                    "sessionId": session_id,
                    "prompt": [{"type": "text", "text": format!("/rename {title}")}],
                }),
                timeout,
            )
            .await?;
        if response["stopReason"] != "end_turn" {
            return Err(AcpError::Protocol(
                "Codex did not confirm its session title command".into(),
            ));
        }
        conn.wait_session_title(session_id, title, revision).await
    };
    tokio::time::timeout(timeout, operation)
        .await
        .map_err(|_| AcpError::Timeout("Codex session title confirmation".into()))?
}

#[cfg(test)]
mod tests {
    use super::*;
    use intent_acp::ConnectionHooks;
    use serde_json::Value;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

    async fn check(existing: Option<&str>, echo_first: bool, fail: bool, echo: bool) {
        let (client_write, peer_read) = tokio::io::duplex(4096);
        let (mut peer_write, client_read) = tokio::io::duplex(4096);
        let conn = Connection::new(client_write, client_read, None, ConnectionHooks::default());
        let mut peer_read = BufReader::new(peer_read).lines();
        let title = existing.unwrap_or("Intent utility").to_owned();
        let initial = existing.map(str::to_owned);
        let peer = tokio::spawn(async move {
            if let Some(title) = initial {
                peer_write.write_all(format!("{}\n", json!({"jsonrpc":"2.0", "method":"session/update", "params":{"sessionId":"s", "update":{"sessionUpdate":"session_info_update", "title":title}}})).as_bytes()).await.unwrap();
            }
            let barrier: Value =
                serde_json::from_str(&peer_read.next_line().await.unwrap().unwrap()).unwrap();
            peer_write
                .write_all(
                    format!(
                        "{}\n",
                        json!({"jsonrpc":"2.0", "id":barrier["id"], "result":{}})
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            let request: Value =
                serde_json::from_str(&peer_read.next_line().await.unwrap().unwrap()).unwrap();
            assert_eq!(request["method"], "session/prompt");
            assert_eq!(
                request["params"]["prompt"][0]["text"],
                format!("/rename {title}")
            );
            let notification = format!(
                "{}\n",
                json!({"jsonrpc":"2.0", "method":"session/update", "params":{"sessionId":"s", "update":{"sessionUpdate":"session_info_update", "title":title}}})
            );
            let response = if fail {
                json!({"jsonrpc":"2.0", "id":request["id"], "error":{"code":-1,"message":"rename refused"}})
            } else {
                json!({"jsonrpc":"2.0", "id":request["id"], "result":{"stopReason":"end_turn"}})
            };
            if echo && echo_first {
                peer_write.write_all(notification.as_bytes()).await.unwrap();
            }
            peer_write
                .write_all(format!("{response}\n").as_bytes())
                .await
                .unwrap();
            if echo && !echo_first {
                peer_write.write_all(notification.as_bytes()).await.unwrap();
            }
            // Keep stdout open: a missing echo must time out, not pass because
            // the response or a pre-existing matching title looked successful.
            tokio::time::sleep(Duration::from_millis(200)).await;
        });
        conn.request_timeout("test/barrier", json!({}), Duration::from_secs(1))
            .await
            .unwrap();
        let result = confirm(&conn, "s", "Intent utility", Duration::from_millis(100)).await;
        assert_eq!(result.is_ok(), !fail && echo, "{result:?}");
        peer.await.unwrap();
    }

    #[tokio::test]
    async fn title_control_consumes_only_matching_echo_and_restores_routing() {
        let (client_write, peer_read) = tokio::io::duplex(4096);
        let (mut peer_write, client_read) = tokio::io::duplex(4096);
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let conn = Connection::new(
            client_write,
            client_read,
            None,
            ConnectionHooks {
                notifications: Some(tx),
                ..ConnectionHooks::default()
            },
        );
        let peer = tokio::spawn(async move {
            let mut lines = BufReader::new(peer_read).lines();
            for _ in 0..2 {
                let request: Value =
                    serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
                for update in [
                    json!({"sessionUpdate":"session_info_update","title":"Confirmed"}),
                    json!({"sessionUpdate":"session_info_update","title":"Unrelated"}),
                    json!({"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"Actual output"}}),
                ] {
                    let note = json!({"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s","update":update}});
                    peer_write
                        .write_all(format!("{note}\n").as_bytes())
                        .await
                        .unwrap();
                }
                let response = json!({"jsonrpc":"2.0","id":request["id"],"result":{}});
                peer_write
                    .write_all(format!("{response}\n").as_bytes())
                    .await
                    .unwrap();
            }
        });
        let guard = conn.session_title_control("s", "Confirmed").unwrap();
        assert!(conn.session_title_control("s", "Other").is_err());
        conn.request("test/barrier", json!({})).await.unwrap();
        assert_eq!(
            rx.try_recv().unwrap().params["update"]["title"],
            "Unrelated"
        );
        assert_eq!(
            rx.try_recv().unwrap().params["update"]["sessionUpdate"],
            "agent_message_chunk"
        );
        assert!(rx.try_recv().is_err());
        drop(guard);
        conn.request("test/barrier", json!({})).await.unwrap();
        assert_eq!(
            rx.try_recv().unwrap().params["update"]["title"],
            "Confirmed"
        );
        assert_eq!(
            rx.try_recv().unwrap().params["update"]["title"],
            "Unrelated"
        );
        assert_eq!(
            rx.try_recv().unwrap().params["update"]["sessionUpdate"],
            "agent_message_chunk"
        );
        peer.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn confirmation_preserves_title_and_accepts_both_response_echo_orders() {
        check(Some("User's existing title"), true, false, true).await;
        check(Some("User's existing title"), false, false, true).await;
        check(None, false, false, true).await;
    }

    #[tokio::test(start_paused = true)]
    async fn confirmation_fails_on_rpc_error_or_missing_fresh_echo() {
        check(None, true, true, true).await;
        check(Some("User's existing title"), false, false, false).await;
    }
}
