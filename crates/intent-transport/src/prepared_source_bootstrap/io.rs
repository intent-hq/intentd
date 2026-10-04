//! Count raw TCP ciphertext during bootstrap; closure means local handle disposal only.
use std::{
    io,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    task::{Context, Poll},
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, ReadBuf},
    net::TcpStream,
};

pub(super) const CIPHER_LIMIT: usize = 65536;
pub(super) const HTTP_LIMIT: usize = 16384;
const ACTIVE_IO_CHUNK: usize = 8192;

#[derive(Default)]
pub(super) struct Meter {
    pub read: AtomicUsize,
    pub written: AtomicUsize,
    pub closed: AtomicBool,
    pub ready: AtomicBool,
    active: AtomicBool,
    pub bootstrap_read: AtomicUsize,
    pub bootstrap_written: AtomicUsize,
    pub hello_in: AtomicUsize,
    pub hello_out: AtomicUsize,
    #[cfg(test)]
    pub fail_write_at: AtomicUsize,
    pub http_in: AtomicUsize,
    pub http_out: AtomicUsize,
}

impl Meter {
    /// Transfer the existing TLS/parser owner after authenticated hello flush.
    /// Totals and buffered read-ahead are never reset or declared consumed.
    pub fn activate(&self) -> io::Result<()> {
        if self.active.load(Ordering::Acquire) {
            return Err(io::Error::other("duplicate source transition"));
        }
        self.bootstrap_read
            .store(self.read.load(Ordering::Acquire), Ordering::Release);
        self.bootstrap_written
            .store(self.written.load(Ordering::Acquire), Ordering::Release);
        self.active.store(true, Ordering::Release);
        Ok(())
    }
    fn remaining(&self, counter: &AtomicUsize) -> usize {
        let observed = counter.load(Ordering::Acquire);
        if self.active.load(Ordering::Acquire) {
            ACTIVE_IO_CHUNK.min(usize::MAX - observed)
        } else {
            CIPHER_LIMIT.saturating_sub(observed)
        }
    }
}

pub(super) struct CountedTcp {
    stream: Option<TcpStream>,
    pub meter: Arc<Meter>,
}
impl CountedTcp {
    pub fn new(stream: TcpStream, meter: Arc<Meter>) -> Self {
        Self {
            stream: Some(stream),
            meter,
        }
    }
}
impl AsyncRead for CountedTcp {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        let left = self.meter.remaining(&self.meter.read);
        if left == 0 {
            return Poll::Ready(Err(io::Error::other("bootstrap ciphertext read budget")));
        }
        let n = left.min(buf.remaining());
        let mut bounded = ReadBuf::new(buf.initialize_unfilled_to(n));
        match Pin::new(self.stream.as_mut().expect("live TCP")).poll_read(cx, &mut bounded) {
            Poll::Ready(Ok(())) => {
                let n = bounded.filled().len();
                buf.advance(n);
                self.meter
                    .read
                    .fetch_update(Ordering::AcqRel, Ordering::Acquire, |v| v.checked_add(n))
                    .map_err(|_| io::Error::other("ciphertext counter overflow"))?;
                Poll::Ready(Ok(()))
            }
            other => other,
        }
    }
}
impl AsyncWrite for CountedTcp {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        let left = self.meter.remaining(&self.meter.written);
        #[cfg(test)]
        let left = match self.meter.fail_write_at.load(Ordering::Acquire) {
            0 => left,
            injected => {
                left.min(injected.saturating_sub(self.meter.written.load(Ordering::Acquire)))
            }
        };
        if left == 0 {
            return Poll::Ready(Err(io::Error::other("bootstrap ciphertext write budget")));
        }
        match Pin::new(self.stream.as_mut().expect("live TCP"))
            .poll_write(cx, &buf[..left.min(buf.len())])
        {
            Poll::Ready(Ok(n)) => {
                self.meter
                    .written
                    .fetch_update(Ordering::AcqRel, Ordering::Acquire, |v| v.checked_add(n))
                    .map_err(|_| io::Error::other("ciphertext counter overflow"))?;
                Poll::Ready(Ok(n))
            }
            other => other,
        }
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(self.stream.as_mut().expect("live TCP")).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(self.stream.as_mut().expect("live TCP")).poll_shutdown(cx)
    }
}
impl Drop for CountedTcp {
    fn drop(&mut self) {
        drop(self.stream.take());
        self.meter.closed.store(true, Ordering::Release);
    }
}

pub(super) async fn read_head<S: AsyncRead + Unpin>(stream: &mut S) -> io::Result<Vec<u8>> {
    let mut buf = Vec::with_capacity(HTTP_LIMIT);
    while buf.len() < HTTP_LIMIT {
        buf.push(stream.read_u8().await?);
        if buf.ends_with(b"\r\n\r\n") {
            return Ok(buf);
        }
    }
    Err(io::Error::other("prepared HTTP head budget"))
}
