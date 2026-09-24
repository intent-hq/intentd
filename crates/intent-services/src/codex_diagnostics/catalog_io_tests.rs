use super::*;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

/// Like Tokio's filesystem writer, accepting bytes need not publish them yet.
#[derive(Default)]
struct DeferredWriter {
    pending: Vec<u8>,
    visible: Vec<u8>,
    ready: Arc<AtomicBool>,
    fail_write: bool,
    fail_flush: bool,
}

impl AsyncWrite for DeferredWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        if self.fail_write {
            return Poll::Ready(Err(std::io::Error::other("write failed")));
        }
        self.pending.extend_from_slice(bytes);
        Poll::Ready(Ok(bytes.len()))
    }

    fn poll_flush(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        if self.fail_flush {
            return Poll::Ready(Err(std::io::Error::other("deferred write failed")));
        }
        if !self.ready.load(Ordering::Acquire) {
            return Poll::Pending;
        }
        self.visible = std::mem::take(&mut self.pending);
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        self.poll_flush(cx)
    }
}

#[test]
fn private_contents_wait_until_bytes_are_visible() {
    let mut writer = DeferredWriter::default();
    let ready = writer.ready.clone();
    let mut cx = Context::from_waker(Waker::noop());
    {
        let mut write = std::pin::pin!(write_private_contents(&mut writer, b"complete auth"));
        assert!(
            write.as_mut().poll(&mut cx).is_pending(),
            "queued bytes are not ready for a child"
        );
        ready.store(true, Ordering::Release);
        assert_eq!(write.as_mut().poll(&mut cx), Poll::Ready(Ok(())));
    }
    assert_eq!(writer.visible, b"complete auth");
}

#[tokio::test]
async fn private_contents_propagate_write_and_completion_errors() {
    for (fail_write, fail_flush) in [(true, false), (false, true)] {
        let mut writer = DeferredWriter {
            fail_write,
            fail_flush,
            ..DeferredWriter::default()
        };
        assert_eq!(
            write_private_contents(&mut writer, b"auth").await,
            Err(CatalogFailure::IsolationFailed)
        );
    }
    let mut writer = DeferredWriter {
        ready: Arc::new(AtomicBool::new(true)),
        ..DeferredWriter::default()
    };
    write_private_contents(&mut writer, b"complete config")
        .await
        .unwrap();
    assert_eq!(writer.visible, b"complete config");
}

#[tokio::test]
async fn private_files_are_ready_private_and_never_overwrite() {
    let root = crate::test_support::test_tempdir("codex-private-file");
    let path = root.path().join("auth.json");
    private_file(&path, b"original auth").await.unwrap();
    assert_eq!(std::fs::read(&path).unwrap(), b"original auth");
    assert_eq!(
        private_file(&path, b"replacement").await,
        Err(CatalogFailure::IsolationFailed)
    );
    assert_eq!(std::fs::read(&path).unwrap(), b"original auth");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(path).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
}
