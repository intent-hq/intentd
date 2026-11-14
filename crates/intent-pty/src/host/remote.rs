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
    /// Receives every output chunk, as [`super::SpawnSpec::output_sink`].
    pub output_sink: Option<super::OutputSink>,
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
            output_sink: None,
        }
    }
}

type ExitSlot = Arc<Mutex<Option<ExitStatus>>>;

/// What the host keeps of a remote terminal besides its stand-in parts.
pub(super) struct RemoteHandle {
    io: Arc<dyn RemotePty>,
    exit: ExitSlot,
    sink: Option<super::OutputSink>,
    /// Why the process ended without an exit status, when the caller knows
    /// ([`PtyHost::remote_lost`]).
    lost: Mutex<Option<String>>,
}

impl RemoteHandle {
    /// `PtyHost::signal` for a remote terminal: Ctrl-C as input, the
    /// others end it.
    pub(super) fn signal(&self, sig: super::PtySignal) -> Result<()> {
        match sig {
            super::PtySignal::Interrupt => self.io.write(&[0x03]).map_err(super::io_error),
            super::PtySignal::Terminate | super::PtySignal::Kill => {
                self.io.kill();
                Ok(())
            }
        }
    }
}

#[derive(Clone)]
struct Remote {
    io: Arc<dyn RemotePty>,
    exit: ExitSlot,
    fanout: Arc<Mutex<Fanout>>,
}

impl std::fmt::Debug for Remote {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RemotePty")
    }
}

impl ChildKiller for Remote {
    /// Ask the machine to end the process. The terminal ends here at once
    /// (its session is being released), so nothing waits on the machine.
    fn kill(&mut self) -> std::io::Result<()> {
        self.io.kill();
        {
            let mut exit = self
                .exit
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if exit.is_none() {
                *exit = Some(ExitStatus::with_exit_code(PtyExit::UNOBSERVABLE_CODE));
            }
        }
        let mut fanout = self
            .fanout
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        fanout.eof = true;
        fanout.eof_notify.notify_waiters();
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
        let (tx, _rx) = broadcast::channel(FANOUT_CAPACITY);
        let fanout = Arc::new(Mutex::new(Fanout {
            eof: false,
            eof_notify: Arc::default(),
            framing: intent_core::script_output::LineDecoder::new(false),
            scrollback: Scrollback::new(spec.scrollback_bytes),
            tx,
        }));
        let remote = Remote {
            io: io.clone(),
            exit: exit.clone(),
            fanout: fanout.clone(),
        };
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
            writer: Arc::new(Mutex::new(Box::new(Writer(io.clone())))),
            child: Mutex::new(Box::new(remote.clone())),
            killer: Mutex::new(Box::new(remote)),
            fanout,
            reader: Mutex::new(None),
            #[cfg(unix)]
            stop_reader: Arc::default(),
            watcher: Mutex::new(None),
            exit: Mutex::new(None),
            remote: Some(RemoteHandle {
                io,
                exit,
                sink: spec.output_sink,
                lost: Mutex::default(),
            }),
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
        if let Some(sink) = session.remote.as_ref().and_then(|r| r.sink.as_ref()) {
            sink.write(bytes);
        }
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
        let Some(slot) = session.remote.as_ref().map(|remote| &remote.exit) else {
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

    /// A remote process ended without an exit status for `reason` (its
    /// machine was lost, or refused to start it): as
    /// [`Self::remote_exited`] with no code, keeping `reason` for
    /// [`Self::remote_lost_reason`].
    ///
    /// # Errors
    ///
    /// `Error::NotFound` for an unknown or released terminal.
    pub fn remote_lost(&self, id: PtyId, reason: &str) -> Result<()> {
        let session = self.get(id)?;
        let Some(remote) = session.remote.as_ref() else {
            return Err(internal("not a remote terminal"));
        };
        remote
            .lost
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get_or_insert_with(|| reason.to_owned());
        drop(session);
        self.remote_exited(id, None)
    }

    /// Why remote process `id` ended without an exit status, if it was
    /// ended by [`Self::remote_lost`].
    #[must_use]
    pub fn remote_lost_reason(&self, id: PtyId) -> Option<String> {
        let session = self.get(id).ok()?;
        let reason = session.remote.as_ref()?.lost.lock().ok()?.clone();
        reason
    }

    /// Whether `id` is a remote terminal.
    #[must_use]
    pub fn is_remote(&self, id: PtyId) -> bool {
        self.get(id).is_ok_and(|session| session.remote.is_some())
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

    /// A remote PTY's output reaches its output sink, beyond what the
    /// scrollback keeps, as a local PTY's does.
    #[tokio::test]
    async fn a_remote_pty_feeds_its_output_sink() {
        let host = PtyHost::new();
        let seen = Arc::new(Mutex::new(Vec::<u8>::new()));
        let mut spec = RemoteSpec::new("ws-1");
        spec.listed = false;
        spec.scrollback_bytes = 4;
        let sink = seen.clone();
        spec.output_sink = Some(crate::OutputSink::new(move |bytes| {
            sink.lock().unwrap().extend_from_slice(bytes);
        }));
        let id = host
            .spawn_remote(spec, Arc::new(Recorder::default()))
            .unwrap();
        assert!(host.list_scope("ws-1").is_empty());
        host.push_remote_output(id, b"first ").unwrap();
        host.push_remote_output(id, b"second\n").unwrap();
        assert_eq!(*seen.lock().unwrap(), b"first second\n");
    }

    /// A remote process lost without an exit status keeps the reason, and
    /// ends like one whose status was unobservable.
    #[tokio::test]
    async fn a_lost_remote_process_keeps_its_reason() {
        let host = PtyHost::new();
        let id = host
            .spawn_remote(RemoteSpec::new("ws-1"), Arc::new(Recorder::default()))
            .unwrap();
        assert_eq!(host.remote_lost_reason(id), None);
        host.remote_lost(id, "remote-script-lost: the machine disconnected")
            .unwrap();
        assert_eq!(
            host.remote_lost_reason(id).as_deref(),
            Some("remote-script-lost: the machine disconnected")
        );
        let exit = host.try_exit(id).unwrap().unwrap();
        assert!(!exit.observed);
        host.wait_output_eof(id).await.unwrap();
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
