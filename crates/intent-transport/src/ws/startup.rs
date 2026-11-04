//! Bootstrap seats share TLS, origin checks, token cache, heartbeat and lifecycle.
use super::{
    derive_accept_key, extract_token, mono_ms, mpsc, oneshot, reject, Arc, AsyncRead, AsyncWrite,
    AsyncWriteExt, AtomicI64, Bytes, ClientHandle, CloseCode, CloseFrame, ConnCmd, Duration,
    Message, Ordering, Role, SinkExt, StreamExt, WebSocketConfig, WebSocketStream, WsInner,
};

impl WsInner {
    pub(super) async fn bootstrap_upgrade<S>(
        self: Arc<Self>,
        mut stream: S,
        path: &str,
        target: &str,
        authorization: Option<&str>,
        key: Option<String>,
    ) -> std::io::Result<()>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        if path != "/ws" || !self.dispatch.admit_bootstrap().await {
            return unavailable(&mut stream).await;
        }
        let mut rotation = None;
        let mut credential = None;
        if self.auth_enabled {
            let Some(candidate) = extract_token(authorization, target) else {
                return reject(&mut stream, 401, "Unauthorized").await;
            };
            let Some(store) = self.token_store.as_ref() else {
                return unavailable(&mut stream).await;
            };
            rotation = Some(crate::auth::LegacyRotation::new(store, &candidate));
            let valid = store
                .load_token()
                .await
                .is_some_and(|owner| crate::auth::token_matches(&owner, &candidate));
            if !valid {
                return unavailable(&mut stream).await;
            }
            credential = Some(candidate);
        }
        let Some(key) = key else {
            return reject(&mut stream, 400, "Bad Request").await;
        };
        let accept = derive_accept_key(key.as_bytes());
        stream.write_all(format!("HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: {accept}\r\n\r\n").as_bytes()).await?;
        stream.flush().await?;
        let config = WebSocketConfig::default()
            .max_message_size(Some(crate::MAX_INBOUND_MESSAGE_BYTES))
            .max_frame_size(Some(crate::MAX_INBOUND_MESSAGE_BYTES));
        let ws = WebSocketStream::from_raw_socket(stream, Role::Server, Some(config)).await;
        let id = self.next_client_id.fetch_add(1, Ordering::Relaxed);
        let (cmd_tx, cmd_rx) = mpsc::channel(8);
        let last_pong = Arc::new(AtomicI64::new(mono_ms()));
        // Register before polling the task so even immediate retirement removes
        // the seat after registration. Same registry gives bounded stop/reaping.
        let (start_tx, start_rx) = oneshot::channel();
        let handle = tokio::spawn({
            let this = self.clone();
            let pong = last_pong.clone();
            async move {
                let _ = start_rx.await;
                this.bootstrap_connection(id, ws, cmd_rx, pong, rotation, credential)
                    .await;
            }
        });
        self.clients.lock().expect("ws clients poisoned").insert(
            id,
            ClientHandle {
                cmd_tx,
                last_pong,
                abort: handle.abort_handle(),
            },
        );
        let _ = start_tx.send(());
        Ok(())
    }

    async fn bootstrap_connection<S>(
        self: Arc<Self>,
        id: u64,
        mut ws: WebSocketStream<S>,
        mut commands: mpsc::Receiver<ConnCmd>,
        pong: Arc<AtomicI64>,
        mut rotation: Option<crate::auth::LegacyRotation>,
        credential: Option<String>,
    ) where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let reason = loop {
            tokio::select! {
                biased;
                () = self.dispatch.retired() => break "Startup complete; reconnect",
                () = crate::auth::await_rotation(&mut rotation) => break "Credential changed; reconnect",
                command = commands.recv() => match command {
                    Some(ConnCmd::Ping) => if ws.send(Message::Ping(Bytes::new())).await.is_err() { break "Connection closed"; },
                    _ => break "Server shutting down",
                },
                message = ws.next() => match message {
                    Some(Ok(Message::Text(raw))) => {
                        if let Some(candidate) = &credential {
                            let valid = match &self.token_store {
                                Some(store) => store.load_token().await.is_some_and(|owner| crate::auth::token_matches(&owner, candidate)),
                                None => false,
                            };
                            if !valid { break "Credential changed; reconnect"; }
                        }
                        if let Some(frame) = crate::startup::response(self.dispatch.status(), raw.as_str()) {
                            if frame.len() > crate::MAX_OUTBOUND_MESSAGE_BYTES { break "Response too large"; }
                            tokio::select! {
                                biased;
                                () = self.dispatch.retired() => break "Startup complete; reconnect",
                                sent = ws.send(Message::Text(frame.into())) => if sent.is_err() { break "Connection closed"; },
                            }
                        }
                    }
                    Some(Ok(Message::Ping(data))) => if ws.send(Message::Pong(data)).await.is_err() { break "Connection closed"; },
                    Some(Ok(Message::Pong(_))) => pong.store(mono_ms(), Ordering::Relaxed),
                    Some(Ok(Message::Binary(_) | Message::Frame(_))) => {},
                    _ => break "Connection closed",
                },
            }
        };
        self.deregister(id);
        // A peer that stops reading must not hold shutdown/handoff indefinitely.
        let _ = tokio::time::timeout(
            Duration::from_secs(1),
            ws.close(Some(CloseFrame {
                code: CloseCode::Away,
                reason: reason.into(),
            })),
        )
        .await;
    }
}

pub(super) async fn unavailable<W: AsyncWrite + Unpin>(stream: &mut W) -> std::io::Result<()> {
    stream.write_all(b"HTTP/1.1 503 Service Unavailable\r\nRetry-After: 1\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await?;
    stream.shutdown().await
}
