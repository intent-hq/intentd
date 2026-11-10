//! `/node/claim` (monorepo `docs/protocol/node-link.md`, "Node invite
//! claim"): the one anonymous `/node/*` path. Order, every refusal before
//! the `101`: no executor service installed `404` → `server.wsApi.enabled`
//! off or any `Origin` `403` → not ready `503` → no claim permit `503`. No
//! credential is resolved; an `Authorization` header is ignored. The
//! connection then carries one `claim` text frame and one reply, within
//! [`CLAIM_DEADLINE`], and closes with `1000`.

use std::time::Duration;

use intent_core::node_invite::{codes, ClaimFrame, ClaimReply, MAX_CLAIM_FRAME_BYTES};

use super::{
    derive_accept_key, reject, Arc, AsyncRead, AsyncWrite, AsyncWriteExt, CloseCode, CloseFrame,
    Message, Role, SinkExt, StreamExt, WebSocketConfig, WebSocketStream, WsInner,
};

/// The whole exchange after the upgrade.
const CLAIM_DEADLINE: Duration = Duration::from_secs(10);

impl WsInner {
    pub(super) async fn claim_upgrade<S>(
        self: Arc<Self>,
        mut stream: S,
        has_origin: bool,
        key: Option<String>,
    ) -> std::io::Result<()>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        if self
            .executor_links
            .as_ref()
            .is_none_or(|links| !links.ready())
        {
            return reject(&mut stream, 404, "Not Found").await;
        }
        if !self.enabled || has_origin {
            return reject(&mut stream, 403, "Forbidden").await;
        }
        let Some(services) = self.dispatch.ready() else {
            return super::startup::unavailable(&mut stream).await;
        };
        let api = services.api.clone();
        let Some(key) = key else {
            return reject(&mut stream, 400, "Bad Request").await;
        };
        let Ok(permit) = self.claim_permits.clone().try_acquire_owned() else {
            return reject(&mut stream, 503, "Service Unavailable").await;
        };
        let accept = derive_accept_key(key.as_bytes());
        let response = format!(
            "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: {accept}\r\n\r\n"
        );
        stream.write_all(response.as_bytes()).await?;
        stream.flush().await?;
        let config = WebSocketConfig::default()
            .max_message_size(Some(MAX_CLAIM_FRAME_BYTES))
            .max_frame_size(Some(MAX_CLAIM_FRAME_BYTES));
        let mut ws = WebSocketStream::from_raw_socket(stream, Role::Server, Some(config)).await;
        tokio::spawn(async move {
            let _permit = permit;
            let exchange = async {
                let frame = loop {
                    match ws.next().await {
                        Some(Ok(Message::Text(text))) => break ClaimFrame::decode(&text),
                        Some(Ok(Message::Ping(_) | Message::Pong(_))) => {}
                        _ => return,
                    }
                };
                let reply = match frame {
                    None => ClaimReply::refused(codes::PROTOCOL),
                    Some(frame) => match api.claim_node_invite(frame).await {
                        Ok(reply) => reply,
                        Err(error) => {
                            tracing::warn!(error = %error, "node invite claim failed");
                            return;
                        }
                    },
                };
                let Ok(text) = serde_json::to_string(&reply) else {
                    return;
                };
                if ws.send(Message::Text(text.into())).await.is_ok() {
                    let _ = ws
                        .close(Some(CloseFrame {
                            code: CloseCode::Normal,
                            reason: "".into(),
                        }))
                        .await;
                }
            };
            let _ = tokio::time::timeout(CLAIM_DEADLINE, exchange).await;
        });
        Ok(())
    }
}
