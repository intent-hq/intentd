//! The executor link's upgrade gate on `/node/link` and `/node/git`
//! (daemon-to-daemon remote agents). Order, every refusal before the `101`:
//! no executor service installed `404` → `server.wsApi.enabled` off `403` →
//! any `Origin` `403` → not ready `503` → bearer header looked up **only**
//! among active executor credentials `401` (so a machine that never minted
//! one, or whose credentials are all revoked, refuses every request) → git
//! binding headers not current `409`. No setting switches the link on or off. Owner and per-principal tokens
//! are never looked up here, and executor tokens never on `/ws`, `/tunnel`
//! or `/invite`. Insecure dev mode skips TLS, never the executor token.

use super::{
    derive_accept_key, reject, Arc, AsyncRead, AsyncWrite, AsyncWriteExt, Role, WebSocketStream,
    WsInner,
};
use crate::node_link::{ExecutorRoute, GIT_PATH};

/// The parts of a `/node/*` upgrade request the gate reads.
pub(super) struct ExecutorRequest<'a> {
    pub path: &'a str,
    pub authorization: Option<&'a str>,
    pub has_origin: bool,
    pub key: Option<String>,
    /// The `x-intent-*` binding headers.
    pub headers: &'a [(String, String)],
    /// The link server's epoch when the TCP connection was accepted.
    pub epoch: u64,
}

impl WsInner {
    pub(super) async fn executor_upgrade<S>(
        self: Arc<Self>,
        mut stream: S,
        request: ExecutorRequest<'_>,
    ) -> std::io::Result<()>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let ExecutorRequest {
            path,
            authorization,
            has_origin,
            key,
            headers,
            epoch,
        } = request;
        let Some(links) = self.executor_links.clone().filter(|links| links.ready()) else {
            return reject(&mut stream, 404, "Not Found").await;
        };
        if !self.enabled || has_origin {
            return reject(&mut stream, 403, "Forbidden").await;
        }
        let Some(services) = self.dispatch.ready() else {
            return super::startup::unavailable(&mut stream).await;
        };
        let api = services.api.clone();
        // Subscribe before resolving so a revocation racing the upgrade is
        // not missed.
        let revocations = api.subscribe_executor_revocations();
        let Some(token) = crate::auth::extract_bearer_token(authorization) else {
            return reject(&mut stream, 401, "Unauthorized").await;
        };
        let credential = match api
            .resolve_executor_credential(crate::auth::hash_token(&token))
            .await
        {
            Ok(Some(credential)) if credential.is_active() => credential,
            Ok(_) => return reject(&mut stream, 401, "Unauthorized").await,
            Err(e) => {
                tracing::warn!(error = %e, "executor credential lookup failed; rejecting upgrade");
                return reject(&mut stream, 401, "Unauthorized").await;
            }
        };
        let route = if path == GIT_PATH {
            let header = |name: &str| {
                headers
                    .iter()
                    .find(|(n, _)| n.eq_ignore_ascii_case(name))
                    .map(|(_, v)| v.clone())
            };
            match links.git_route(&credential, header).await {
                Some(route) => route,
                None => return reject(&mut stream, 409, "Conflict").await,
            }
        } else {
            ExecutorRoute::Link
        };
        // The listener stopped since this connection was accepted.
        if links.epoch() != epoch {
            return super::startup::unavailable(&mut stream).await;
        }
        let Some(key) = key else {
            return reject(&mut stream, 400, "Bad Request").await;
        };
        let accept = derive_accept_key(key.as_bytes());
        let response = format!(
            "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: {accept}\r\n\r\n"
        );
        stream.write_all(response.as_bytes()).await?;
        stream.flush().await?;
        let ws = WebSocketStream::from_raw_socket(
            stream,
            Role::Server,
            Some(crate::node_link::socket_config()),
        )
        .await;
        tokio::spawn(links.serve(api, credential, route, ws, revocations, epoch));
        Ok(())
    }
}
