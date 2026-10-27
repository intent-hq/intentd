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
        if let Some(mut retire) = retire {
            let close = close.clone();
            let mut closed = close.subscribe();
            tokio::spawn(async move {
                tokio::select! {
                    retired = retire.wait_for(|retired| *retired) => {
                        // A dropped listener retires nothing.
                        if retired.is_ok() {
                            close.send_replace(true);
                        }
                    }
                    () = super::wait_true(&mut closed) => {}
                }
            });
        }
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    bytes = out_rx.recv() => {
                        let Some(bytes) = bytes else { break };
                        tokio::select! {
                            sent = sink.send(Message::Binary(bytes.into())) => {
                                if sent.is_err() {
                                    break;
                                }
                            }
                            () = super::wait_true(&mut closed) => break,
                        }
                    }
                    message = stream.next() => match message {
                        Some(Ok(Message::Binary(bytes))) => {
                            // A full receive queue must not keep a retired
                            // connection open.
                            tokio::select! {
                                sent = in_tx.send(bytes.to_vec()) => {
                                    if sent.is_err() {
                                        break;
                                    }
                                }
                                () = super::wait_true(&mut closed) => break,
                            }
                        }
                        Some(Ok(Message::Ping(_) | Message::Pong(_))) => {}
                        _ => break,
                    },
                    () = super::wait_true(&mut closed) => break,
                }
            }
            close_tx.send_replace(true);
            // A peer that stopped reading must not hold this task open.
            let _ = tokio::time::timeout(std::time::Duration::from_secs(5), async {
                let _ = sink.send(Message::Close(None)).await;
                let _ = sink.close().await;
            })
            .await;
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
        self.sender().send(bytes).await
    }

    /// A cloneable handle that sends on this connection, so one task can
    /// receive while others send.
    #[must_use]
    pub fn sender(&self) -> GitSender {
        GitSender {
            out: self.out.clone(),
            close: self.close.clone(),
        }
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

/// Sends on a [`GitConnection`]; see [`GitConnection::sender`].
#[derive(Debug, Clone)]
pub struct GitSender {
    out: mpsc::Sender<Vec<u8>>,
    close: watch::Sender<bool>,
}

impl GitSender {
    /// Send one message.
    ///
    /// # Errors
    ///
    /// `node-link-closed` when the connection is gone.
    pub async fn send(&self, bytes: Vec<u8>) -> Result<()> {
        if *self.close.borrow() {
            return Err(Error::Internal(codes::CLOSED.into()));
        }
        let mut closed = self.close.subscribe();
        tokio::select! {
            sent = self.out.send(bytes) => sent.map_err(|_| Error::Internal(codes::CLOSED.into())),
            () = super::wait_true(&mut closed) => Err(Error::Internal(codes::CLOSED.into())),
        }
    }

    /// Queue one message without waiting; dropped when the queue is full or
    /// the connection is gone.
    pub fn try_send(&self, bytes: Vec<u8>) {
        if !*self.close.borrow() {
            let _ = self.out.try_send(bytes);
        }
    }

    /// Close the connection.
    pub fn close(&self) {
        self.close.send_replace(true);
    }
}

impl Drop for GitConnection {
    fn drop(&mut self) {
        self.close.send_replace(true);
    }
}
