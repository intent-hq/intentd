//! Terminals whose process runs on another machine (a workspace home,
//! nodeProtocol 19). The host keeps their scrollback, replay offsets,
//! subscribers, list rows and exit status exactly as for a local PTY; the
//! caller feeds the output in and the [`RemotePty`] carries input, resizes
//! and kills to the machine.

use std::io::Write;
use std::sync::{Arc, Mutex};

use portable_pty::{Child, ChildKiller, ExitStatus, MasterPty, PtySize as PortablePtySize};
use tokio::sync::broadcast;

use intent_core::Result;

use super::{
    internal, publish_output, Fanout, PtyExit, PtyHost, PtyId, PtySession, PtySize, FANOUT_CAPACITY,
};
use crate::scrollback::{Scrollback, DEFAULT_SCROLLBACK_BYTES};

/// Where a remote terminal's input, resizes and kill go.
pub trait RemotePty: Send + Sync + 'static {
    /// Send input to the process.
    ///
    /// # Errors
    ///
    /// When the input cannot be queued for the machine.
    fn write(&self, data: &[u8]) -> std::io::Result<()>;
    /// Resize the terminal.
    ///
    /// # Errors
    ///
    /// When the resize cannot be queued for the machine.
    fn resize(&self, size: PtySize) -> std::io::Result<()>;
    /// End the process; its exit arrives through [`PtyHost::remote_exited`].
    fn kill(&self);
}

/// What a remote terminal shows in lists, like [`super::SpawnSpec`].
#[derive(Clone, Debug)]
pub struct RemoteSpec {
    pub scope: String,
    pub name: Option<String>,
    pub listed: bool,
    /// The working directory shown for the terminal.
    pub cwd: Option<String>,
    pub size: PtySize,
    pub scrollback_bytes: usize,
}

impl RemoteSpec {
    /// A listed terminal in `scope` with default size and scrollback.
    pub fn new(scope: impl Into<String>) -> Self {
        Self {
            scope: scope.into(),
            name: None,
            listed: true,
            cwd: None,
            size: PtySize::default(),
            scrollback_bytes: DEFAULT_SCROLLBACK_BYTES,
        }
    }
}

type ExitSlot = Arc<Mutex<Option<ExitStatus>>>;

#[derive(Clone)]
struct Remote {
    io: Arc<dyn RemotePty>,
    exit: ExitSlot,
}

impl std::fmt::Debug for Remote {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RemotePty")
    }
}

impl ChildKiller for Remote {
    fn kill(&mut self) -> std::io::Result<()> {
        self.io.kill();
        Ok(())
    }

    fn clone_killer(&self) -> Box<dyn ChildKiller + Send + Sync> {
        Box::new(self.clone())
    }
}

impl Child for Remote {
    fn try_wait(&mut self) -> std::io::Result<Option<ExitStatus>> {
        Ok(self
            .exit
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone())
    }

    fn wait(&mut self) -> std::io::Result<ExitStatus> {
        loop {
            if let Some(exit) = self.try_wait()? {
                return Ok(exit);
            }
            std::thread::sleep(super::REAP_POLL);
        }
    }

    fn process_id(&self) -> Option<u32> {
        None
    }

    #[cfg(windows)]
    fn as_raw_handle(&self) -> Option<std::os::windows::io::RawHandle> {
        None
    }
}

struct Master {
    io: Arc<dyn RemotePty>,
    size: Mutex<PortablePtySize>,
}

impl MasterPty for Master {
    fn resize(&self, size: PortablePtySize) -> std::result::Result<(), anyhow::Error> {
        self.io.resize(PtySize {
            rows: size.rows,
            cols: size.cols,
        })?;
        *self
            .size
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = size;
        Ok(())
    }

    fn get_size(&self) -> std::result::Result<PortablePtySize, anyhow::Error> {
        Ok(*self
            .size
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner))
    }

    fn try_clone_reader(
        &self,
    ) -> std::result::Result<Box<dyn std::io::Read + Send>, anyhow::Error> {
        anyhow::bail!("a remote terminal has no local reader")
    }

    fn take_writer(&self) -> std::result::Result<Box<dyn Write + Send>, anyhow::Error> {
        anyhow::bail!("a remote terminal has no local writer")
    }

    #[cfg(unix)]
    fn process_group_leader(&self) -> Option<i32> {
        None
    }

    #[cfg(unix)]
    fn as_raw_fd(&self) -> Option<std::os::fd::RawFd> {
        None
    }

    #[cfg(unix)]
    fn tty_name(&self) -> Option<std::path::PathBuf> {
        None
    }
}

struct Writer(Arc<dyn RemotePty>);

impl Write for Writer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.write(buf)?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl PtyHost {
    /// Track a terminal whose process runs on another machine. Feed its
    /// output with [`Self::push_remote_output`] and its end with
    /// [`Self::remote_exited`]; `write`, `resize` and `kill` go to `io`.
    ///
    /// # Errors
    ///
    /// `Error::Internal` once the host is shut down.
    ///
    /// # Panics
    ///
    /// Panics if the sessions mutex is poisoned.
    pub fn spawn_remote(&self, spec: RemoteSpec, io: Arc<dyn RemotePty>) -> Result<PtyId> {
        let exit: ExitSlot = Arc::default();
        let remote = Remote {
            io: io.clone(),
            exit: exit.clone(),
        };
        let (tx, _rx) = broadcast::channel(FANOUT_CAPACITY);
        let session = Arc::new(PtySession {
            scope: spec.scope,
            name: spec.name,
            process: Mutex::default(),
            listed: spec.listed,
            cwd: spec.cwd,
            pid: None,
            master: Mutex::new(Some(Box::new(Master {
                io: io.clone(),
                size: Mutex::new(spec.size.to_portable()),
            }))),
            slave: Mutex::new(None),
            writer: Arc::new(Mutex::new(Box::new(Writer(io)))),
            child: Mutex::new(Box::new(remote.clone())),
            killer: Mutex::new(Box::new(remote)),
            fanout: Arc::new(Mutex::new(Fanout {
                eof: false,
                eof_notify: Arc::default(),
                framing: intent_core::script_output::LineDecoder::new(false),
                scrollback: Scrollback::new(spec.scrollback_bytes),
                tx,
            })),
            reader: Mutex::new(None),
            #[cfg(unix)]
            stop_reader: Arc::default(),
            watcher: Mutex::new(None),
            exit: Mutex::new(None),
            remote_exit: Some(exit),
        });
        let id = PtyId(
            self.next_id
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        );
        let mut sessions = self.sessions.lock().unwrap();
        if self.closed.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(internal("pty host is shut down"));
        }
        sessions.insert(id, session);
        Ok(id)
    }

    /// Append a remote terminal's output, in order.
    ///
    /// # Errors
    ///
    /// `Error::NotFound` for an unknown or released terminal.
    pub fn push_remote_output(&self, id: PtyId, bytes: &[u8]) -> Result<()> {
        let session = self.get(id)?;
        publish_output(&session.fanout, bytes);
        Ok(())
    }

    /// A remote terminal's process ended: `exit_code`, or `None` when the
    /// machine could not report one. Its output is complete.
    ///
    /// # Errors
    ///
    /// `Error::NotFound` for an unknown or released terminal.
    ///
    /// # Panics
    ///
    /// Panics if a per-session mutex is poisoned.
    pub fn remote_exited(&self, id: PtyId, exit_code: Option<i32>) -> Result<()> {
        let session = self.get(id)?;
        let Some(slot) = &session.remote_exit else {
            return Err(internal("not a remote terminal"));
        };
        let exit = match exit_code.and_then(|code| u32::try_from(code).ok()) {
            Some(code) => PtyExit {
                exit_code: code,
                success: code == 0,
                observed: true,
            },
            None => PtyExit::unobservable(),
        };
        {
            let mut latched = session.exit.lock().unwrap();
            if latched.is_none() {
                *latched = Some(exit.clone());
            }
        }
        *slot
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) =
            Some(ExitStatus::with_exit_code(exit.exit_code));
        let mut fanout = session.fanout.lock().unwrap();
        fanout.eof = true;
        fanout.eof_notify.notify_waiters();
        Ok(())
    }

    /// Whether `id` is a remote terminal.
    #[must_use]
    pub fn is_remote(&self, id: PtyId) -> bool {
        self.get(id)
            .is_ok_and(|session| session.remote_exit.is_some())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct Recorder {
        input: Mutex<Vec<u8>>,
        sizes: Mutex<Vec<PtySize>>,
        kills: Mutex<usize>,
    }

    impl RemotePty for Recorder {
        fn write(&self, data: &[u8]) -> std::io::Result<()> {
            self.input.lock().unwrap().extend_from_slice(data);
            Ok(())
        }
        fn resize(&self, size: PtySize) -> std::io::Result<()> {
            self.sizes.lock().unwrap().push(size);
            Ok(())
        }
        fn kill(&self) {
            *self.kills.lock().unwrap() += 1;
        }
    }

    /// Output fed in reaches scrollback, replay offsets and live
    /// subscribers; input and resizes go to the machine; the reported exit
    /// ends the terminal like a local one's.
    #[tokio::test]
    async fn a_remote_terminal_behaves_like_a_local_one() {
        let host = PtyHost::new();
        let io = Arc::new(Recorder::default());
        let mut spec = RemoteSpec::new("ws-1");
        spec.cwd = Some("/home/checkout".into());
        let id = host.spawn_remote(spec, io.clone()).unwrap();
        assert!(host.is_remote(id));
        assert_eq!(host.list_scope("ws-1"), vec![id]);

        host.push_remote_output(id, b"hello ").unwrap();
        let mut attached = host.attach(id).unwrap();
        assert_eq!(attached.backlog.bytes, b"hello ");
        host.push_remote_output(id, b"world\n").unwrap();
        let live = attached.live.recv().await.unwrap();
        assert_eq!(live.bytes, b"world\n");
        assert_eq!(live.start_offset, 6);
        assert_eq!(host.scrollback(id).unwrap(), b"hello world\n");

        host.write(id, b"ls\r").unwrap();
        host.resize(
            id,
            PtySize {
                rows: 40,
                cols: 120,
            },
        )
        .unwrap();
        assert_eq!(*io.input.lock().unwrap(), b"ls\r");
        assert_eq!(
            *io.sizes.lock().unwrap(),
            vec![PtySize {
                rows: 40,
                cols: 120
            }]
        );
        let info = host.info(id).unwrap();
        assert!(info.alive);
        assert_eq!(info.cwd.as_deref(), Some("/home/checkout"));
        assert_eq!(info.process, crate::PtyProcess::default());
        assert_eq!(host.try_exit(id).unwrap(), None);

        host.remote_exited(id, Some(3)).unwrap();
        host.wait_output_eof(id).await.unwrap();
        let exit = host.wait(id).await.unwrap();
        assert_eq!(
            (exit.exit_code, exit.success, exit.observed),
            (3, false, true)
        );
        assert!(!host.is_alive(id));
        assert!(host.list_scope("ws-1").is_empty());
        assert_eq!(*io.kills.lock().unwrap(), 0);
    }

    /// Killing a remote terminal asks the machine to end it and releases
    /// the terminal; an exit without a code is unobservable.
    #[tokio::test]
    async fn killing_a_remote_terminal_reaches_the_machine() {
        let host = PtyHost::new();
        let io = Arc::new(Recorder::default());
        let id = host
            .spawn_remote(RemoteSpec::new("ws-1"), io.clone())
            .unwrap();
        assert!(host.kill(id).await);
        assert_eq!(*io.kills.lock().unwrap(), 1);
        assert!(host.push_remote_output(id, b"late").is_err());

        let other = host.spawn_remote(RemoteSpec::new("ws-1"), io).unwrap();
        host.remote_exited(other, None).unwrap();
        assert_eq!(host.try_exit(other).unwrap(), Some(PtyExit::unobservable()));
    }
}
