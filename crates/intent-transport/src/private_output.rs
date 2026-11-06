//! Final native writes retain the original immutable output owner. No fence is
//! held while a socket is Pending; readiness always re-enters admission.
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::{future::poll_fn, Sink};
use intent_core::repository_request::RepositoryPrivateOutput;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use tokio_tungstenite::tungstenite::Message;

pub(crate) struct PrivateOutput(Arc<dyn RepositoryPrivateOutput>);

/// An item in the existing bounded native outbound lanes. Private items retain
/// their immutable producer owner instead of becoming ordinary JSON strings.
pub struct OutboundFrame(Frame);
enum Frame {
    Public(String),
    Private(PrivateOutput),
}

impl From<String> for OutboundFrame {
    fn from(text: String) -> Self {
        Self(Frame::Public(text))
    }
}
impl From<&str> for OutboundFrame {
    fn from(text: &str) -> Self {
        text.to_string().into()
    }
}
impl std::fmt::Debug for OutboundFrame {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OutboundFrame")
            .field("private", &self.is_private())
            .field("bytes", &self.len())
            .finish()
    }
}
impl std::ops::Deref for OutboundFrame {
    type Target = str;
    fn deref(&self) -> &str {
        match &self.0 {
            Frame::Public(text) => text,
            Frame::Private(_) => panic!("private native output requires its admitted writer"),
        }
    }
}
impl OutboundFrame {
    pub(crate) fn private(owner: Arc<dyn RepositoryPrivateOutput>) -> Self {
        Self(Frame::Private(PrivateOutput::new(owner)))
    }
    /// Public recovery controls may carry receipts; private payloads never do.
    pub(crate) fn public_text(&self) -> Option<&str> {
        match &self.0 {
            Frame::Public(text) => Some(text),
            Frame::Private(_) => None,
        }
    }
    pub(crate) fn is_private(&self) -> bool {
        matches!(self.0, Frame::Private(_))
    }
    pub(crate) fn len(&self) -> usize {
        match &self.0 {
            Frame::Public(s) => s.len(),
            Frame::Private(p) => p.0.frame().len(),
        }
    }
    pub(crate) async fn write_uds<W: AsyncWrite + Unpin + Send>(
        &self,
        writer: &mut W,
    ) -> Result<(), ()> {
        match &self.0 {
            Frame::Private(output) => output.write_uds(writer).await,
            Frame::Public(text) => {
                writer.write_all(text.as_bytes()).await.map_err(|_| ())?;
                writer.write_all(b"\n").await.map_err(|_| ())?;
                writer.flush().await.map_err(|_| ())
            }
        }
    }
    pub(crate) async fn write_wss<S>(&self, sink: &mut S) -> Result<(), ()>
    where
        S: Sink<Message> + Unpin + Send,
        S::Error: Send,
    {
        match &self.0 {
            Frame::Private(output) => output.write_wss(sink).await,
            Frame::Public(text) => futures::SinkExt::send(sink, Message::Text(text.clone().into()))
                .await
                .map_err(|_| ()),
        }
    }
}

#[cfg(test)]
impl PartialEq<&str> for OutboundFrame {
    fn eq(&self, other: &&str) -> bool {
        !self.is_private() && &**self == *other
    }
}
#[cfg(test)]
impl PartialEq<String> for OutboundFrame {
    fn eq(&self, other: &String) -> bool {
        !self.is_private() && &**self == other
    }
}

impl PrivateOutput {
    pub(crate) fn new(owner: Arc<dyn RepositoryPrivateOutput>) -> Self {
        Self(owner)
    }

    fn poll<T: Send, E: Send>(
        &self,
        cx: &mut Context<'_>,
        action: impl FnOnce(&mut Context<'_>) -> Poll<Result<T, E>> + Send,
    ) -> Poll<Result<T, ()>> {
        // Context is not Send. Rebuild it inside the synchronous fence from
        // its Send waker rather than holding a guard across an async write.
        let waker = cx.waker().clone();
        let mut action = Some(action);
        let mut result = None;
        let admitted = self.0.with_current(&mut || {
            let action = action
                .take()
                .ok_or_else(|| intent_core::Error::Internal("output already polled".into()))?;
            result = Some(action(&mut Context::from_waker(&waker)));
            Ok(())
        });
        if admitted.is_err() {
            return Poll::Ready(Err(()));
        }
        match result {
            Some(Poll::Pending) => Poll::Pending,
            Some(Poll::Ready(result)) => Poll::Ready(result.map_err(|_| ())),
            None => Poll::Ready(Err(())),
        }
    }

    /// On refusal/error the writer must close without flushing any buffered
    /// private bytes. A later public frame must not flush a retired envelope.
    pub(crate) async fn write_uds<W: AsyncWrite + Unpin + Send>(
        &self,
        writer: &mut W,
    ) -> Result<(), ()> {
        #[cfg(feature = "repository-test-fixtures")]
        fixture_hold(self.0.frame(), "uds-before-write").await?;
        for bytes in [self.0.frame().as_bytes(), b"\n"] {
            let mut offset = 0;
            while offset < bytes.len() {
                let written = poll_fn(|cx| {
                    self.poll(cx, |cx| {
                        std::pin::Pin::new(&mut *writer).poll_write(cx, &bytes[offset..])
                    })
                })
                .await?;
                if written == 0 {
                    return Err(());
                }
                offset += written;
            }
        }
        poll_fn(|cx| self.poll(cx, |cx| std::pin::Pin::new(&mut *writer).poll_flush(cx))).await
    }

    pub(crate) async fn write_wss<S>(&self, sink: &mut S) -> Result<(), ()>
    where
        S: Sink<Message> + Unpin + Send,
        S::Error: Send,
    {
        poll_fn(|cx| self.poll(cx, |cx| std::pin::Pin::new(&mut *sink).poll_ready(cx))).await?;
        self.0
            .with_current(&mut || {
                std::pin::Pin::new(&mut *sink)
                    .start_send(Message::Text(self.0.frame().to_owned().into()))
                    .map_err(|_| {
                        intent_core::Error::Internal("private output writer unavailable".into())
                    })
            })
            .map_err(|_| ())?;
        #[cfg(feature = "repository-test-fixtures")]
        fixture_hold(self.0.frame(), "wss-before-flush").await?;
        poll_fn(|cx| self.poll(cx, |cx| std::pin::Pin::new(&mut *sink).poll_flush(cx))).await
    }
}

/// Disposable daemon fixtures only. Never compiled into a packaged daemon.
/// Holds one selected private frame outside admission locks; resumption still
/// goes through the original production admission and writer error paths.
#[cfg(feature = "repository-test-fixtures")]
async fn fixture_hold(frame: &str, stage: &str) -> Result<(), ()> {
    use std::io::Write as _;
    let Some(root) = std::env::var_os("INTENT_TEST_PRIVATE_WRITE_HOLD") else {
        return Ok(());
    };
    let root = std::path::PathBuf::from(root);
    let Ok(needle) = std::fs::read_to_string(root.join("arm")) else {
        return Ok(());
    };
    if needle.is_empty() || !frame.contains(&needle) || !frame.contains("subscription.push") {
        return Ok(());
    }
    let marker = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(root.join("entered"));
    let mut marker = match marker {
        Ok(marker) => marker,
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => return Ok(()),
        Err(_) => return Err(()),
    };
    marker.write_all(stage.as_bytes()).map_err(|_| ())?;
    tokio::time::timeout(std::time::Duration::from_secs(60), async {
        while !root.join("release").exists() {
            // timing-guard: disposable cross-process writer barrier readiness
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .map_err(|_| ())
}

#[cfg(feature = "repository-test-fixtures")]
pub(crate) fn fixture_discarded_queue(queue: &mut crate::conn::OutboundReceiver) {
    let Some(root) = std::env::var_os("INTENT_TEST_PRIVATE_WRITE_HOLD") else {
        return;
    };
    let root = std::path::PathBuf::from(root);
    if !root.join("entered").exists() {
        return;
    }
    let mut public = Vec::new();
    for lane in [&mut queue.priority, &mut queue.bulk] {
        while let Ok(frame) = lane.try_recv() {
            if !frame.is_private() {
                public.push(frame.to_string());
            }
        }
    }
    // These items would be dropped on the same error path; this records their
    // presence without writing or flushing them through the failed socket.
    let _ = std::fs::write(
        root.join("discarded-public.json"),
        serde_json::to_vec(&public).unwrap(),
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use tokio::io::AsyncReadExt;

    struct Owner {
        frame: String,
        live: AtomicBool,
        inside: AtomicBool,
    }
    impl RepositoryPrivateOutput for Owner {
        fn frame(&self) -> &str {
            &self.frame
        }
        fn with_current(
            &self,
            action: &mut (dyn FnMut() -> intent_core::Result<()> + Send),
        ) -> intent_core::Result<()> {
            if !self.live.load(Ordering::Acquire) {
                return Err(intent_core::Error::Forbidden("retired".into()));
            }
            self.inside.store(true, Ordering::Release);
            let result = action();
            self.inside.store(false, Ordering::Release);
            result
        }
    }
    fn owner() -> Arc<Owner> {
        Arc::new(Owner {
            frame: "{\"private\":\"original\"}".into(),
            live: AtomicBool::new(true),
            inside: AtomicBool::new(false),
        })
    }

    #[tokio::test]
    async fn native_private_output_existing_queue_retains_owner_until_final_write() {
        let owner = owner();
        let (tx, mut rx) = crate::conn::outbound_channel();
        tx.bulk_sender()
            .send(OutboundFrame::private(owner.clone()))
            .await
            .unwrap();
        let frame = rx.recv().await.unwrap();
        assert!(frame.is_private());
        owner.live.store(false, Ordering::Release);
        let (mut writer, mut reader) = tokio::io::duplex(128);
        assert!(frame.write_uds(&mut writer).await.is_err());
        drop(writer);
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes).await.unwrap();
        assert!(bytes.is_empty());
    }

    #[tokio::test]
    async fn native_private_output_uds_positive_and_revoked_before_write() {
        let owner = owner();
        let output = PrivateOutput::new(owner.clone());
        let (mut writer, mut reader) = tokio::io::duplex(128);
        output.write_uds(&mut writer).await.unwrap();
        let mut bytes = vec![0; owner.frame.len() + 1];
        reader.read_exact(&mut bytes).await.unwrap();
        assert_eq!(bytes, format!("{}\n", owner.frame).as_bytes());
        owner.live.store(false, Ordering::Release);
        assert!(output.write_uds(&mut writer).await.is_err());
        drop(writer);
        let mut rest = Vec::new();
        reader.read_to_end(&mut rest).await.unwrap();
        assert!(rest.is_empty(), "revoked queued envelope writes no bytes");
    }

    #[tokio::test]
    async fn native_private_output_uds_rechecks_after_backpressure_without_holding_fences() {
        let owner = owner();
        let output = PrivateOutput::new(owner.clone());
        let (mut writer, mut reader) = tokio::io::duplex(1);
        let send = output.write_uds(&mut writer);
        tokio::pin!(send);
        assert!(futures::poll!(&mut send).is_pending());
        assert!(!owner.inside.load(Ordering::Acquire));
        owner.live.store(false, Ordering::Release);
        assert_eq!(reader.read_u8().await.unwrap(), b'{');
        assert!(
            send.await.is_err(),
            "retired owner cannot send the remainder"
        );
    }

    struct Socket {
        owner: Arc<Owner>,
        pending: bool,
        sent: usize,
        flushed: bool,
    }
    impl Sink<Message> for Socket {
        type Error = ();
        fn poll_ready(self: std::pin::Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), ()>> {
            assert!(self.owner.inside.load(Ordering::Acquire));
            Poll::Ready(Ok(()))
        }
        fn start_send(mut self: std::pin::Pin<&mut Self>, message: Message) -> Result<(), ()> {
            assert!(self.owner.inside.load(Ordering::Acquire));
            assert_eq!(message, Message::Text(self.owner.frame.clone().into()));
            self.sent += 1;
            Ok(())
        }
        fn poll_flush(
            mut self: std::pin::Pin<&mut Self>,
            _: &mut Context<'_>,
        ) -> Poll<Result<(), ()>> {
            assert!(self.owner.inside.load(Ordering::Acquire));
            if self.pending {
                Poll::Pending
            } else {
                self.flushed = true;
                Poll::Ready(Ok(()))
            }
        }
        fn poll_close(self: std::pin::Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), ()>> {
            panic!("retired buffer must be dropped without flushing close")
        }
    }

    #[tokio::test]
    async fn native_private_output_wss_rechecks_flush_and_never_falls_back_after_revocation() {
        let owner = owner();
        let output = PrivateOutput::new(owner.clone());
        let mut socket = Socket {
            owner: owner.clone(),
            pending: false,
            sent: 0,
            flushed: false,
        };
        output.write_wss(&mut socket).await.unwrap();
        assert_eq!(socket.sent, 1);
        assert!(socket.flushed);
        socket.pending = true;
        socket.flushed = false;
        {
            let send = output.write_wss(&mut socket);
            tokio::pin!(send);
            assert!(futures::poll!(&mut send).is_pending());
            assert!(!owner.inside.load(Ordering::Acquire));
            owner.live.store(false, Ordering::Release);
            assert!(send.await.is_err());
        }
        assert!(!socket.flushed);
    }
    #[tokio::test]
    async fn recovery_private_uds_write_keeps_authority_and_cancels_backpressure() {
        let owner = owner();
        let frame = OutboundFrame::private(owner.clone());
        let (tx, mut rx) = crate::conn::outbound_channel();
        tx.bulk_sender().send(frame).await.unwrap();
        let frame = rx.recv().await.unwrap();
        let (mut writer, mut reader) = tokio::io::duplex(128);
        assert!(
            tx.recovery
                .prepare_frame(&frame)
                .finish(async { frame.write_uds(&mut writer).await.is_ok() })
                .await
        );
        let mut bytes = vec![0; owner.frame.len() + 1];
        reader.read_exact(&mut bytes).await.unwrap();
        assert_eq!(bytes, format!("{}\n", owner.frame).as_bytes());
        let (mut held, mut tail) = tokio::io::duplex(1);
        {
            let send = tx
                .recovery
                .prepare_frame(&frame)
                .finish(async { frame.write_uds(&mut held).await.is_ok() });
            tokio::pin!(send);
            assert!(futures::poll!(&mut send).is_pending());
            assert!(!owner.inside.load(Ordering::Acquire));
            tx.recovery.close();
            assert!(!send.await);
        }
        drop(held);
        let mut remainder = Vec::new();
        tail.read_to_end(&mut remainder).await.unwrap();
        assert_eq!(
            remainder, b"{",
            "only the byte admitted before cancellation may leave"
        );
    }

    #[tokio::test]
    async fn recovery_private_wss_write_cancels_without_flushing_queued_bytes() {
        let owner = owner();
        let frame = OutboundFrame::private(owner.clone());
        let (tx, mut rx) = crate::conn::outbound_channel();
        let mut socket = Socket {
            owner: owner.clone(),
            pending: false,
            sent: 0,
            flushed: false,
        };
        assert!(
            tx.recovery
                .prepare_frame(&frame)
                .finish(async { frame.write_wss(&mut socket).await.is_ok() })
                .await
        );
        assert!(socket.flushed);
        socket.pending = true;
        socket.flushed = false;
        tx.send_bulk("later public frame".into()).await.unwrap();
        {
            let send = tx
                .recovery
                .prepare_frame(&frame)
                .finish(async { frame.write_wss(&mut socket).await.is_ok() });
            tokio::pin!(send);
            assert!(futures::poll!(&mut send).is_pending());
            assert!(!owner.inside.load(Ordering::Acquire));
            tx.recovery.close();
            assert!(!send.await);
        }
        assert!(!socket.flushed);
        assert!(tx.recovery.is_closed());
        let queued = rx.recv().await.unwrap();
        socket.pending = false;
        assert!(
            !tx.recovery
                .prepare_frame(&queued)
                .finish(async { queued.write_wss(&mut socket).await.is_ok() })
                .await
        );
        assert!(
            !socket.flushed,
            "later traffic must not flush canceled private bytes"
        );
    }
}
