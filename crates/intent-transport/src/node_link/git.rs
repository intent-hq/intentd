//! The second authenticated connection, carrying git traffic as opaque
//! binary messages so a large transfer never delays control or journal
//! frames. It belongs to one binding and closes when a newer one replaces it.

use futures_util::{SinkExt as _, StreamExt as _};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{mpsc, watch};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

use intent_core::node_link::LeaseBinding;
use intent_core::{Error, Result};

use super::codes;

const QUEUE: usize = 64;

/// A git connection for one binding.
#[derive(Debug)]
pub struct GitConnection {
    binding: LeaseBinding,
    out: mpsc::Sender<Vec<u8>>,
    incoming: mpsc::Receiver<Vec<u8>>,
    close: watch::Sender<bool>,
}

impl GitConnection {
    /// Pump `socket`; `retire` (when given) closes the connection once it
    /// reports `true`, which the node uses to drop an older generation's
    /// connections at re-bind.
    pub(super) fn start<S>(
        socket: WebSocketStream<S>,
        binding: LeaseBinding,
        retire: Option<watch::Receiver<bool>>,
    ) -> Self
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let (out, mut out_rx) = mpsc::channel::<Vec<u8>>(QUEUE);
        let (in_tx, incoming) = mpsc::channel(QUEUE);
        let (close, _) = watch::channel(false);
        let (mut sink, mut stream) = socket.split();
        let mut closed = close.subscribe();
        let close_tx = close.clone();
        tokio::spawn(async move {
            let mut retire = retire;
            loop {
                let retired = async {
                    match retire.as_mut() {
                        Some(rx) => super::wait_true(rx).await,
                        None => std::future::pending().await,
                    }
                };
                tokio::select! {
                    bytes = out_rx.recv() => {
                        let Some(bytes) = bytes else { break };
                        if sink.send(Message::Binary(bytes.into())).await.is_err() {
                            break;
                        }
                    }
                    message = stream.next() => match message {
                        Some(Ok(Message::Binary(bytes))) => {
                            if in_tx.send(bytes.to_vec()).await.is_err() {
                                break;
                            }
                        }
                        Some(Ok(Message::Ping(_) | Message::Pong(_))) => {}
                        _ => break,
                    },
                    () = super::wait_true(&mut closed) => break,
                    () = retired => break,
                }
            }
            close_tx.send_replace(true);
            let _ = sink.send(Message::Close(None)).await;
            let _ = sink.close().await;
        });
        Self {
            binding,
            out,
            incoming,
            close,
        }
    }

    #[must_use]
    pub fn binding(&self) -> &LeaseBinding {
        &self.binding
    }

    /// Send one message.
    ///
    /// # Errors
    ///
    /// `node-link-closed` when the connection is gone.
    pub async fn send(&self, bytes: Vec<u8>) -> Result<()> {
        if self.is_closed() {
            return Err(Error::Internal(codes::CLOSED.into()));
        }
        self.out
            .send(bytes)
            .await
            .map_err(|_| Error::Internal(codes::CLOSED.into()))
    }

    /// The next message, `None` once the connection has closed.
    pub async fn recv(&mut self) -> Option<Vec<u8>> {
        self.incoming.recv().await
    }

    #[must_use]
    pub fn is_closed(&self) -> bool {
        *self.close.borrow()
    }

    pub fn close(&self) {
        self.close.send_replace(true);
    }

    /// Resolves once the connection has closed.
    pub async fn closed(&self) {
        let mut rx = self.close.subscribe();
        super::wait_true(&mut rx).await;
    }
}

impl Drop for GitConnection {
    fn drop(&mut self) {
        self.close.send_replace(true);
    }
}
