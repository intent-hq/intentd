//! Nonblocking PTY I/O with bounded reader cancellation. The duplicated master
//! descriptors share `O_NONBLOCK`, so the writer retries backpressure too.

use std::fs::File;
use std::io::{self, Read, Write};
use std::os::fd::{AsFd, AsRawFd, BorrowedFd};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

use nix::poll::{poll, PollFd, PollFlags};
use portable_pty::MasterPty;

use super::{DRAIN_GRACE, REAP_POLL};

pub(super) struct Reader {
    fd: Arc<File>,
    stop: Arc<AtomicBool>,
    drain_deadline: Option<Instant>,
}

pub(super) struct Writer {
    inner: Box<dyn Write + Send>,
    fd: Arc<File>,
    stop: Arc<AtomicBool>,
}

pub(super) fn open(master: &dyn MasterPty, stop: &Arc<AtomicBool>) -> io::Result<(Reader, Writer)> {
    let raw = master
        .as_raw_fd()
        .ok_or_else(|| io::Error::other("PTY master has no Unix fd"))?;
    // SAFETY: master owns raw throughout this borrow; cloning gives the reader
    // its own descriptor, which remains valid when teardown releases master.
    let fd = unsafe { BorrowedFd::borrow_raw(raw) }.try_clone_to_owned()?;
    let fd = Arc::new(File::from(fd));
    // SAFETY: fd is live, and these fcntl commands take only integer arguments.
    let flags = unsafe { nix::libc::fcntl(fd.as_raw_fd(), nix::libc::F_GETFL) };
    if flags == -1 {
        return Err(io::Error::last_os_error());
    }
    // Nonblocking read is essential even after poll: a child may flush the
    // terminal between readiness and read, otherwise wedging cancellation.
    // SAFETY: same live fd and integer-only fcntl command as above.
    if unsafe {
        nix::libc::fcntl(
            fd.as_raw_fd(),
            nix::libc::F_SETFL,
            flags | nix::libc::O_NONBLOCK,
        )
    } == -1
    {
        return Err(io::Error::last_os_error());
    }
    let inner = master.take_writer().map_err(io::Error::other)?;
    Ok((
        Reader {
            fd: Arc::clone(&fd),
            stop: Arc::clone(stop),
            drain_deadline: None,
        },
        Writer {
            inner,
            fd,
            stop: Arc::clone(stop),
        },
    ))
}

fn wait(fd: &File, events: PollFlags) -> io::Result<()> {
    let mut fds = [PollFd::new(fd.as_fd(), events)];
    match poll(&mut fds, u16::try_from(REAP_POLL.as_millis()).unwrap()) {
        Ok(_) | Err(nix::errno::Errno::EINTR) => Ok(()),
        Err(e) => Err(e.into()),
    }
}

impl Read for Reader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            if self.stop.load(Ordering::Acquire) {
                let deadline = self
                    .drain_deadline
                    .get_or_insert_with(|| Instant::now() + DRAIN_GRACE);
                if Instant::now() >= *deadline {
                    return Ok(0);
                }
            }
            match self.fd.as_ref().read(buf) {
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    // Cancellation drains bytes already queued, but must not
                    // wait for EOF from an escaped holder of the slave.
                    if self.drain_deadline.is_some() {
                        return Ok(0);
                    }
                    wait(&self.fd, PollFlags::POLLIN)?;
                }
                result => return result,
            }
        }
    }
}

impl Write for Writer {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        loop {
            if self.stop.load(Ordering::Acquire) {
                return Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "PTY is shut down",
                ));
            }
            match self.inner.write(buf) {
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    wait(&self.fd, PollFlags::POLLOUT)?;
                }
                result => return result,
            }
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::OpenOptionsExt;
    use std::os::unix::net::UnixStream;
    use std::time::Duration;

    #[test]
    fn cancelled_reader_drains_queued_output_with_slave_still_open() {
        let pair = portable_pty::native_pty_system()
            .openpty(portable_pty::PtySize::default())
            .unwrap();
        let mut slave = std::fs::OpenOptions::new()
            .write(true)
            .custom_flags(nix::libc::O_NOCTTY)
            .open(pair.master.tty_name().unwrap())
            .unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let (mut reader, _writer) = open(pair.master.as_ref(), &stop).unwrap();
        slave.write_all(b"queued-before-cancellation").unwrap();
        stop.store(true, Ordering::Release);
        let mut output = Vec::new();
        reader.read_to_end(&mut output).unwrap();
        assert_eq!(output, b"queued-before-cancellation");
        // Both slave owners are still held here: finishing cannot rely on EOF.
        drop(slave);
        drop(pair.slave);
    }

    #[test]
    fn cancelled_reader_bounds_drain_even_when_output_never_stops() {
        let (socket, mut peer) = UnixStream::pair().unwrap();
        socket.set_nonblocking(true).unwrap();
        let mut reader = Reader {
            fd: Arc::new(File::from(socket.as_fd().try_clone_to_owned().unwrap())),
            stop: Arc::new(AtomicBool::new(true)),
            drain_deadline: None,
        };
        let started = Instant::now();
        let mut buf = [0; 4];
        loop {
            // Every read has queued bytes: an escaped holder that continues
            // writing must not turn graceful drain into another unbounded join.
            peer.write_all(b"more").unwrap();
            if reader.read(&mut buf).unwrap() == 0 {
                break;
            }
            assert!(started.elapsed() < DRAIN_GRACE * 2, "drain did not stop");
        }
        assert!(started.elapsed() >= DRAIN_GRACE);
    }

    /// Fill a real nonblocking descriptor before calling the Write adapter,
    /// so the tests exercise backpressure without a scheduling/sleep race.
    fn full_writer() -> (Writer, UnixStream, usize) {
        let (socket, peer) = UnixStream::pair().unwrap();
        socket.set_nonblocking(true).unwrap();
        peer.set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let fd = Arc::new(File::from(socket.as_fd().try_clone_to_owned().unwrap()));
        let mut inner = fd.try_clone().unwrap();
        let mut filled = 0;
        loop {
            match inner.write(&[b'x'; 8192]) {
                Ok(n) => filled += n,
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                other => panic!("could not fill writer: {other:?}"),
            }
        }
        (
            Writer {
                inner: Box::new(inner),
                fd,
                stop: Arc::new(AtomicBool::new(false)),
            },
            peer,
            filled,
        )
    }

    #[test]
    fn writer_retries_backpressure_without_losing_bytes() {
        let (mut writer, mut peer, filled) = full_writer();
        let thread = std::thread::spawn(move || writer.write_all(b"after-backpressure"));
        let mut output = vec![0; filled + b"after-backpressure".len()];
        let result = peer.read_exact(&mut output);
        // Close on error too, so a panicking assertion cannot strand the writer.
        drop(peer);
        thread.join().unwrap().unwrap();
        result.unwrap();
        assert!(output[..filled].iter().all(|b| *b == b'x'));
        assert_eq!(&output[filled..], b"after-backpressure");
    }

    #[test]
    fn cancellation_releases_a_writer_under_backpressure() {
        let (mut writer, peer, _) = full_writer();
        let stop = Arc::clone(&writer.stop);
        let (tx, rx) = std::sync::mpsc::channel();
        let thread = std::thread::spawn(move || {
            tx.send(writer.write_all(b"blocked")).unwrap();
        });
        stop.store(true, Ordering::Release);
        let result = rx.recv_timeout(Duration::from_secs(5));
        drop(peer);
        thread.join().unwrap();
        assert_eq!(
            result.unwrap().unwrap_err().kind(),
            io::ErrorKind::BrokenPipe
        );
    }
}
