//! Local, synchronous exit readiness for the exclusively owned fixture child.
//! Linux uses a pidfd registered with epoll; macOS uses kqueue `NOTE_EXIT`.
//! Neither registration reaps. Cancellation drops only descriptors, leaving the
//! same Child available for failure cleanup. Other Unix platforms fail explicitly.

use std::io::{self, Read, Write};
use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::MetadataExt;
use std::os::unix::net::UnixStream;
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Child, ExitStatus};
use std::task::Poll;
use std::time::{Duration, Instant};

use serde_json::{json, Value};

pub(super) const BOUND: Duration = Duration::from_secs(5);

/// Explicitly finalized ownership, retaining the existing common panic/`KEEP_TMP`
/// policy. No implicit `TempDir` removal may happen after a completion receipt.
pub(super) struct FixtureDir {
    root: Option<tempfile::TempDir>,
    path: PathBuf,
    identity: (u64, u64),
}

impl From<tempfile::TempDir> for FixtureDir {
    fn from(mut root: tempfile::TempDir) -> Self {
        root.disable_cleanup(true);
        let path = root.path().to_owned();
        let metadata = std::fs::symlink_metadata(&path).expect("owned fixture directory");
        Self {
            root: Some(root),
            path,
            identity: (metadata.dev(), metadata.ino()),
        }
    }
}

impl FixtureDir {
    pub(super) fn path(&self) -> &Path {
        &self.path
    }

    pub(super) fn finalize(&mut self, failed: bool) -> Value {
        let requested = std::env::var_os("INTENTD_TEST_KEEP_TMP").is_some_and(|v| !v.is_empty());
        let mut row = json!({"path": self.path, "identity": self.identity,
            "retentionRequested": requested, "failureRetention": failed,
            "closeAttempted": false, "closeOk": false, "removed": false,
            "retained": null, "error": null});
        let result = (|| -> io::Result<()> {
            let root = self
                .root
                .take()
                .ok_or_else(|| invalid("directory already finalized"))?;
            // From disabled implicit removal; every failure preserves what remains.
            let original = root.path().to_owned();
            let actual = match std::fs::symlink_metadata(&original) {
                Ok(_) => original.clone(),
                Err(error) if failed && error.kind() == io::ErrorKind::NotFound => {
                    super::common::retained_path_for(&original)
                }
                Err(error) => return Err(error),
            };
            let metadata = std::fs::symlink_metadata(&actual)?;
            if !metadata.is_dir() || (metadata.dev(), metadata.ino()) != self.identity {
                return Err(invalid("fixture directory ownership changed"));
            }
            if failed || requested {
                let _ = root.keep();
                row["retained"] = json!(actual);
            } else {
                row["closeAttempted"] = json!(true);
                root.close()?;
                row["closeOk"] = json!(true);
                match std::fs::symlink_metadata(&original) {
                    Err(error) if error.kind() == io::ErrorKind::NotFound => {
                        row["removed"] = json!(true);
                    }
                    Err(error) => return Err(error),
                    Ok(_) => return Err(invalid("fixture directory remains after close")),
                }
            }
            Ok(())
        })();
        if let Err(error) = result {
            row["error"] = json!(error.to_string());
        }
        row
    }
}

pub(super) fn directory_complete(row: &Value) -> bool {
    row["error"].is_null()
        && row["failureRetention"] == false
        && ((row["closeAttempted"] == true && row["closeOk"] == true && row["removed"] == true)
            || (row["retentionRequested"] == true && row["retained"].is_string()))
}

/// One close, never a retry against a possibly reused descriptor. Linux close
/// errors and the platform's EINTR ambiguity are retained as failed evidence.
pub(super) fn close_descriptor(name: &str, descriptor: impl IntoRawFd) -> Value {
    let fd = descriptor.into_raw_fd();
    // SAFETY: into_raw_fd transfers this descriptor's unique ownership here.
    let result = unsafe { libc::close(fd) };
    json!({"name": name, "fd": fd, "ok": result == 0,
        "error": if result == 0 { None } else { Some(io::Error::last_os_error().to_string()) }})
}

pub(super) fn completion(
    start: Instant,
    deadline: Instant,
    finished: Instant,
    finalized: bool,
) -> Value {
    let elapsed = finished.checked_duration_since(start);
    let budget = deadline.checked_duration_since(start);
    json!({"clock": "monotonic", "elapsedNs": elapsed.map(|d| d.as_nanos()),
        "deadlineAfterStartNs": budget.map(|d| d.as_nanos()), "semanticFinalizationComplete": finalized,
        "withinDeadline": elapsed.is_some() && budget.is_some() && finished < deadline,
        "normalCompletion": finalized && elapsed.is_some() && budget == Some(BOUND) && finished < deadline})
}

pub(super) fn finalize_and_measure(
    start: Instant,
    deadline: Instant,
    finalize: impl FnOnce() -> Value,
    now: impl FnOnce() -> Instant,
) -> (Value, Value) {
    let resources = finalize();
    // This ordering is shared with the injected-clock finalization control.
    // The clock is read AFTER explicit resource finalization, not at entry.
    let finished = now();
    let mut timing = completion(start, deadline, finished, resources["complete"] == true);
    timing["observedUnixNs"] = json!(std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .map(|d| d.as_nanos()));
    (resources, timing)
}

/// Reporting follows semantic completion. A synchronous filesystem/output call
/// cannot be hard-preempted locally; late finalization fails the measured check,
/// and missing/partial reporting or an output error must fail the run.
pub(super) fn report(prefix: &str, row: &Value) -> io::Result<()> {
    let bytes = format!("{prefix} {row}\n");
    let mut output = io::stdout();
    output.write_all(bytes.as_bytes())?;
    output.flush()
}

pub(super) fn propagate_teardown_outcome<T>(
    was_panicking: bool,
    attempt: std::thread::Result<io::Result<()>>,
    finalizing: std::thread::Result<T>,
    failure: Option<String>,
    reporting: std::thread::Result<io::Result<()>>,
    record: impl FnOnce(&Value) -> io::Result<()>,
) {
    let recorded = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| -> io::Result<()> {
        // Diagnostic construction can invoke a custom error formatter. Keep
        // every original outcome borrowed and independently available below.
        let metadata = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let panic_detail = |payload: &(dyn std::any::Any + Send)| {
                json!({"kind":"panic",
                "message":payload.downcast_ref::<&str>().copied()
                    .or_else(|| payload.downcast_ref::<String>().map(String::as_str)),
                "payloadTypeId":format!("{:?}",payload.type_id())})
            };
            let io_detail = |error: &io::Error| {
                json!({"kind":"io_error","message":error.to_string(),
                "errorKind":format!("{:?}",error.kind()),"rawOsError":error.raw_os_error()})
            };
            json!({"alreadyUnwinding":was_panicking,"failure":failure,
            "attempt":match &attempt {
                Err(payload) => panic_detail(payload.as_ref()),
                Ok(Err(error)) => io_detail(error), Ok(Ok(())) => json!({"kind":"ok"}),
            },
            "finalization":match &finalizing {
                Err(payload) => panic_detail(payload.as_ref()), Ok(_) => json!({"kind":"ok"}),
            },
            "reporting":match &reporting {
                Err(payload) => panic_detail(payload.as_ref()),
                Ok(Err(error)) => io_detail(error), Ok(Ok(())) => json!({"kind":"ok"}),
            }})
        }));
        let outcomes = match &metadata {
            Ok(row) => row.clone(),
            // One protected, non-recursive fallback; do not invoke the failed
            // formatter again. A failed sink leaves honestly missing evidence.
            Err(_) => json!({"alreadyUnwinding":was_panicking,"failure":failure,
            "metadata":{"status":"unavailable","reason":"construction panicked"},
            "attempt":{"kind":match &attempt {
                Err(_) => "panic", Ok(Err(_)) => "io_error", Ok(Ok(())) => "ok",
            }},
            "finalization":{"kind":if finalizing.is_err() { "panic" } else { "ok" }},
            "reporting":{"kind":match &reporting {
                Err(_) => "panic", Ok(Err(_)) => "io_error", Ok(Ok(())) => "ok",
            }}}),
        };
        if was_panicking
            || failure.is_some()
            || !matches!(&attempt, Ok(Ok(())))
            || finalizing.is_err()
            || !matches!(&reporting, Ok(Ok(())))
            || metadata.is_err()
        {
            record(&outcomes)?;
        }
        match metadata {
            Ok(_) => Ok(()),
            Err(payload) => std::panic::resume_unwind(payload),
        }
    }));
    // An existing test unwind is already failing; never introduce a second one.
    if was_panicking {
        return;
    }
    match attempt {
        Err(payload) => std::panic::resume_unwind(payload),
        // Keep the typed error (including its custom payload) rather than
        // formatting it again and possibly substituting a formatter panic.
        Ok(Err(error)) => std::panic::resume_unwind(Box::new(error)),
        Ok(Ok(())) => {}
    }
    if let Err(payload) = finalizing {
        std::panic::resume_unwind(payload);
    }
    if let Some(error) = failure {
        panic!("wake fixture teardown failed: {error}");
    }
    match reporting {
        Ok(result) => result.expect("complete graceful teardown report"),
        Err(payload) => std::panic::resume_unwind(payload),
    }
    match recorded {
        Ok(result) => result.expect("complete secondary teardown outcome report"),
        Err(payload) => std::panic::resume_unwind(payload),
    }
}

#[test]
fn teardown_failure_priority_preserves_original_outcome() {
    // Synthetic caught outcomes; no daemon, filesystem fault, or lifecycle claim.
    // This invokes the same propagation boundary used by Daemon::drop.
    #[derive(Debug)]
    struct PanickingDisplay(std::sync::Arc<std::sync::atomic::AtomicUsize>);
    impl std::fmt::Display for PanickingDisplay {
        fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            panic!("synthetic metadata formatter panic")
        }
    }
    impl std::error::Error for PanickingDisplay {}
    for case in [
        "io-before-finalization",
        "original-panic",
        "finalization-only",
        "reporting-only",
        "already-unwinding",
        "later-formatter-original-panic",
        "later-formatter-already-unwinding",
        "original-error-formatter",
    ] {
        let formats = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let original: Box<dyn std::any::Any + Send> = Box::new(String::from("original panic"));
        let original_address = std::ptr::from_ref(original.downcast_ref::<String>().unwrap());
        let formatter_error = io::Error::new(
            io::ErrorKind::PermissionDenied,
            PanickingDisplay(formats.clone()),
        );
        let error_address = std::ptr::from_ref(
            formatter_error
                .get_ref()
                .unwrap()
                .downcast_ref::<PanickingDisplay>()
                .unwrap(),
        );
        let error_kind = formatter_error.kind();
        let raw_error = formatter_error.raw_os_error();
        let attempt = match case {
            "io-before-finalization" | "already-unwinding" => Ok(Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "original I/O error",
            ))),
            "original-panic"
            | "later-formatter-original-panic"
            | "later-formatter-already-unwinding" => Err(original),
            "original-error-formatter" => Ok(Err(formatter_error)),
            _ => Ok(Ok(())),
        };
        let reporting_error = if case.starts_with("later-formatter-") {
            io::Error::new(io::ErrorKind::BrokenPipe, PanickingDisplay(formats.clone()))
        } else {
            io::Error::new(io::ErrorKind::BrokenPipe, "later reporting error")
        };
        let finalizing: std::thread::Result<()> = if case == "reporting-only" {
            Ok(())
        } else {
            Err(Box::new(String::from("later finalization panic")))
        };
        let mut recorded = Value::Null;
        let was_panicking =
            case == "already-unwinding" || case == "later-formatter-already-unwinding";
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            propagate_teardown_outcome(
                was_panicking,
                attempt,
                finalizing,
                None,
                Ok(Err(reporting_error)),
                |row| {
                    recorded = row.clone();
                    Ok(())
                },
            );
        }));
        if case.contains("formatter") {
            assert_eq!(
                formats.load(std::sync::atomic::Ordering::SeqCst),
                1,
                "the failed formatter must not be invoked again"
            );
            assert_eq!(recorded["metadata"]["status"], "unavailable");
            assert_eq!(recorded["metadata"]["reason"], "construction panicked");
            assert_eq!(recorded["reporting"]["kind"], "io_error");
        } else {
            assert_eq!(recorded["reporting"]["errorKind"], "BrokenPipe");
            assert_eq!(recorded["reporting"]["message"], "later reporting error");
        }
        if case != "reporting-only" && !case.contains("formatter") {
            assert_eq!(recorded["finalization"]["kind"], "panic");
            assert_eq!(
                recorded["finalization"]["message"],
                "later finalization panic"
            );
        }
        if was_panicking {
            assert!(
                outcome.is_ok(),
                "an earlier test unwind must not double-panic"
            );
            assert_eq!(recorded["alreadyUnwinding"], true);
        } else {
            let payload = outcome.expect_err("a failed stage must not pass");
            match case {
                "io-before-finalization" => {
                    let primary = payload
                        .downcast_ref::<io::Error>()
                        .expect("original typed I/O error");
                    assert_eq!(primary.kind(), io::ErrorKind::PermissionDenied);
                    assert_eq!(primary.raw_os_error(), None);
                    assert_eq!(primary.to_string(), "original I/O error");
                    assert_eq!(recorded["attempt"]["errorKind"], "PermissionDenied");
                    assert_eq!(recorded["attempt"]["message"], "original I/O error");
                }
                "original-error-formatter" => {
                    let primary = payload
                        .downcast_ref::<io::Error>()
                        .expect("original typed I/O error");
                    assert_eq!(primary.kind(), error_kind);
                    assert_eq!(primary.raw_os_error(), raw_error);
                    assert_eq!(
                        std::ptr::from_ref(
                            primary
                                .get_ref()
                                .unwrap()
                                .downcast_ref::<PanickingDisplay>()
                                .unwrap()
                        ),
                        error_address
                    );
                    assert_eq!(recorded["attempt"]["kind"], "io_error");
                }
                "original-panic" | "later-formatter-original-panic" => assert_eq!(
                    std::ptr::from_ref(payload.downcast_ref::<String>().unwrap()),
                    original_address
                ),
                "finalization-only" => assert_eq!(
                    payload.downcast_ref::<String>().unwrap(),
                    "later finalization panic"
                ),
                "reporting-only" => {
                    let primary = payload.downcast_ref::<String>().unwrap();
                    assert!(
                        primary.contains("complete graceful teardown report")
                            && primary.contains("later reporting error")
                    );
                }
                _ => unreachable!(),
            }
        }
        report(
            "teardown-propagation-contract",
            &json!({"case":case,"outcomes":recorded}),
        )
        .unwrap();
    }
}

pub(super) fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

pub(super) fn remaining(deadline: Instant) -> io::Result<Duration> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|d| !d.is_zero())
        .ok_or_else(|| io::Error::new(io::ErrorKind::TimedOut, "fixture teardown deadline"))
}

fn milliseconds(duration: Duration) -> i32 {
    i32::try_from(duration.as_millis().saturating_add(u128::from(
        !duration.subsec_nanos().is_multiple_of(1_000_000),
    )))
    .unwrap_or(i32::MAX)
}

fn descriptor(fd: libc::c_int) -> io::Result<OwnedFd> {
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: the successful syscall returned a new descriptor, owned here once.
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

pub(super) fn readable(fd: libc::c_int, deadline: Instant) -> io::Result<()> {
    ready(fd, libc::POLLIN, deadline)
}

fn ready(fd: libc::c_int, events: libc::c_short, deadline: Instant) -> io::Result<()> {
    loop {
        let mut row = libc::pollfd {
            fd,
            events,
            revents: 0,
        };
        // SAFETY: a single valid pollfd; only its revents field is written.
        let result = unsafe { libc::poll(&raw mut row, 1, milliseconds(remaining(deadline)?)) };
        if result < 0 {
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(error);
        }
        remaining(deadline)?;
        if result == 0 {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "socket readiness deadline",
            ));
        }
        if row.revents & libc::POLLNVAL != 0 {
            return Err(invalid("invalid socket descriptor"));
        }
        // HUP/ERR are passed to the actual nonblocking read/write/SO_ERROR,
        // which retains the concrete error rather than treating readiness as success.
        if row.revents & (events | libc::POLLHUP | libc::POLLERR) != 0 {
            return Ok(());
        }
        return Err(invalid("unexpected socket readiness"));
    }
}

pub(super) struct ExitWait<'a> {
    child: &'a mut Child,
    queue: OwnedFd,
    #[cfg(target_os = "linux")]
    pidfd: OwnedFd,
    exited: bool,
}

impl<'a> ExitWait<'a> {
    pub(super) fn new(child: &'a mut Child) -> io::Result<Self> {
        #[cfg(target_os = "linux")]
        {
            // SAFETY: pidfd_open takes a PID and zero flags. Child has not been
            // waited/reaped, so its PID cannot be reused during registration.
            let pidfd = descriptor(
                i32::try_from(unsafe { libc::syscall(libc::SYS_pidfd_open, child.id(), 0) })
                    .map_err(|_| invalid("pidfd descriptor out of range"))?,
            )?;
            let queue = descriptor(unsafe { libc::epoll_create1(libc::EPOLL_CLOEXEC) })?;
            let mut event = libc::epoll_event {
                events: libc::EPOLLIN as u32,
                u64: u64::from(child.id()),
            };
            // SAFETY: both descriptors and the event are valid for this call.
            if unsafe {
                libc::epoll_ctl(
                    queue.as_raw_fd(),
                    libc::EPOLL_CTL_ADD,
                    pidfd.as_raw_fd(),
                    &raw mut event,
                )
            } != 0
            {
                return Err(io::Error::last_os_error());
            }
            Ok(Self {
                child,
                queue,
                pidfd,
                exited: false,
            })
        }
        #[cfg(target_os = "macos")]
        {
            let queue = descriptor(unsafe { libc::kqueue() })?;
            if unsafe { libc::fcntl(queue.as_raw_fd(), libc::F_SETFD, libc::FD_CLOEXEC) } < 0 {
                return Err(io::Error::last_os_error());
            }
            let event = libc::kevent {
                ident: usize::try_from(child.id())
                    .map_err(|_| invalid("child PID out of range"))?,
                filter: libc::EVFILT_PROC,
                flags: libc::EV_ADD | libc::EV_ENABLE,
                fflags: libc::NOTE_EXIT,
                data: 0,
                udata: std::ptr::null_mut(),
            };
            // SAFETY: the change registers exit readiness only; no wait/reap.
            if unsafe {
                libc::kevent(
                    queue.as_raw_fd(),
                    &raw const event,
                    1,
                    std::ptr::null_mut(),
                    0,
                    std::ptr::null(),
                )
            } < 0
            {
                return Err(io::Error::last_os_error());
            }
            Ok(Self {
                child,
                queue,
                exited: false,
            })
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            let _ = child;
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "fixture exit readiness requires Linux pidfd or macOS kqueue",
            ))
        }
    }

    fn poll(&mut self, duration: Duration) -> io::Result<Poll<()>> {
        if self.exited {
            return Ok(Poll::Ready(()));
        }
        #[cfg(target_os = "linux")]
        {
            let mut event = libc::epoll_event { events: 0, u64: 0 };
            // SAFETY: valid queue and space for one event.
            let count = unsafe {
                libc::epoll_wait(
                    self.queue.as_raw_fd(),
                    &raw mut event,
                    1,
                    milliseconds(duration),
                )
            };
            if count < 0 {
                return Err(io::Error::last_os_error());
            }
            if count == 0 {
                return Ok(Poll::Pending);
            }
            if event.u64 != u64::from(self.child.id()) || event.events & libc::EPOLLIN as u32 == 0 {
                return Err(invalid("unqualified pidfd exit event"));
            }
        }
        #[cfg(target_os = "macos")]
        {
            let mut event: libc::kevent = unsafe { std::mem::zeroed() };
            let bound = libc::timespec {
                tv_sec: duration
                    .as_secs()
                    .try_into()
                    .map_err(|_| invalid("wait duration out of range"))?,
                tv_nsec: duration.subsec_nanos().into(),
            };
            // SAFETY: valid queue/output/timespec; this does not consume wait status.
            let count = unsafe {
                libc::kevent(
                    self.queue.as_raw_fd(),
                    std::ptr::null(),
                    0,
                    &raw mut event,
                    1,
                    &raw const bound,
                )
            };
            if count < 0 {
                return Err(io::Error::last_os_error());
            }
            if count == 0 {
                return Ok(Poll::Pending);
            }
            if event.ident
                != usize::try_from(self.child.id())
                    .map_err(|_| invalid("child PID out of range"))?
                || event.filter != libc::EVFILT_PROC
                || event.flags & libc::EV_ERROR != 0
                || event.fflags & libc::NOTE_EXIT == 0
            {
                return Err(invalid("unqualified kqueue exit event"));
            }
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "unsupported fixture exit readiness",
        ));
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            self.exited = true;
            Ok(Poll::Ready(()))
        }
    }

    pub(super) fn until_exit(
        &mut self,
        deadline: Instant,
        pending: impl FnOnce() -> io::Result<()>,
    ) -> io::Result<()> {
        remaining(deadline)?;
        // Registration is already armed in the kernel. Release evidence exists
        // only after this actual registered wait reports Pending, never before it.
        if self.poll(Duration::ZERO)?.is_pending() {
            pending()?;
            loop {
                match self.poll(remaining(deadline)?) {
                    Ok(Poll::Ready(())) => break,
                    Ok(Poll::Pending) => {
                        return Err(io::Error::new(
                            io::ErrorKind::TimedOut,
                            "daemon exit deadline",
                        ))
                    }
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                    Err(error) => return Err(error),
                }
            }
        }
        remaining(deadline)?;
        Ok(())
    }

    pub(super) fn reap(self) -> io::Result<ExitStatus> {
        if !self.exited {
            return Err(invalid("exit ACK/readiness is not a child wait"));
        }
        // The child is exclusively borrowed and exit readiness is established.
        // This is the sole consuming wait; receipt accounting must precede it.
        self.child.wait()
    }
}

fn peer(stream: &UnixStream, expected_pid: u32) -> io::Result<Value> {
    #[cfg(target_os = "linux")]
    {
        let mut credentials: libc::ucred = unsafe { std::mem::zeroed() };
        let mut size = libc::socklen_t::try_from(std::mem::size_of_val(&credentials))
            .map_err(|_| invalid("peer credentials size out of range"))?;
        let result = unsafe {
            libc::getsockopt(
                stream.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_PEERCRED,
                std::ptr::from_mut(&mut credentials).cast(),
                &raw mut size,
            )
        };
        if result != 0 {
            return Err(io::Error::last_os_error());
        }
        if size as usize != std::mem::size_of_val(&credentials)
            || u32::try_from(credentials.pid).ok() != Some(expected_pid)
            || credentials.uid != unsafe { libc::geteuid() }
        {
            return Err(invalid("shutdown socket belongs to another peer"));
        }
        Ok(
            json!({"pid": credentials.pid, "uid": credentials.uid, "gid": credentials.gid, "proof": "SO_PEERCRED"}),
        )
    }
    #[cfg(target_os = "macos")]
    {
        let (mut uid, mut gid, mut pid): (libc::uid_t, libc::gid_t, libc::pid_t) = (0, 0, 0);
        let mut size = libc::socklen_t::try_from(std::mem::size_of_val(&pid))
            .map_err(|_| invalid("peer PID size out of range"))?;
        if unsafe { libc::getpeereid(stream.as_raw_fd(), &raw mut uid, &raw mut gid) } != 0
            || unsafe {
                libc::getsockopt(
                    stream.as_raw_fd(),
                    libc::SOL_LOCAL,
                    libc::LOCAL_PEERPID,
                    std::ptr::from_mut(&mut pid).cast(),
                    &raw mut size,
                )
            } != 0
        {
            return Err(io::Error::last_os_error());
        }
        if size as usize != std::mem::size_of_val(&pid)
            || u32::try_from(pid).ok() != Some(expected_pid)
            || uid != unsafe { libc::geteuid() }
        {
            return Err(invalid("shutdown socket belongs to another peer"));
        }
        Ok(json!({"pid": pid, "uid": uid, "gid": gid, "proof": "LOCAL_PEERPID/getpeereid"}))
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = (stream, expected_pid);
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "unsupported shutdown peer credentials",
        ))
    }
}

fn response(value: &Value, id: &str) -> io::Result<()> {
    if value["jsonrpc"] != "2.0"
        || value["id"] != id
        || value.get("error").is_some()
        || value["result"] != json!({"ok": true, "stopping": true})
    {
        return Err(invalid("unqualified shutdown response"));
    }
    Ok(())
}

pub(super) fn shutdown(
    path: &Path,
    pid: u32,
    deadline: Instant,
    events: &mut Vec<Value>,
) -> io::Result<()> {
    remaining(deadline)?;
    let fd = descriptor(unsafe { libc::socket(libc::AF_UNIX, libc::SOCK_STREAM, 0) })?;
    if unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_SETFD, libc::FD_CLOEXEC) } < 0
        || unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_SETFL, libc::O_NONBLOCK) } < 0
    {
        return Err(io::Error::last_os_error());
    }
    let mut address: libc::sockaddr_un = unsafe { std::mem::zeroed() };
    address.sun_family = libc::AF_UNIX
        .try_into()
        .map_err(|_| invalid("address family out of range"))?;
    let name = path.as_os_str().as_bytes();
    if name.len() >= address.sun_path.len() || name.contains(&0) {
        return Err(invalid("invalid private UDS path"));
    }
    for (slot, byte) in address.sun_path.iter_mut().zip(name) {
        *slot = libc::c_char::from_ne_bytes([*byte]);
    }
    #[cfg(target_os = "macos")]
    {
        address.sun_len = std::mem::size_of_val(&address)
            .try_into()
            .map_err(|_| invalid("socket address length out of range"))?;
    }
    let result = unsafe {
        libc::connect(
            fd.as_raw_fd(),
            std::ptr::from_ref(&address).cast(),
            libc::socklen_t::try_from(std::mem::size_of_val(&address))
                .map_err(|_| invalid("socket address size out of range"))?,
        )
    };
    if result < 0 {
        let error = io::Error::last_os_error();
        if error.raw_os_error() != Some(libc::EINPROGRESS) {
            return Err(error);
        }
        ready(fd.as_raw_fd(), libc::POLLOUT, deadline)?;
        let mut code = 0 as libc::c_int;
        let mut size = libc::socklen_t::try_from(std::mem::size_of_val(&code))
            .map_err(|_| invalid("socket status size out of range"))?;
        if unsafe {
            libc::getsockopt(
                fd.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_ERROR,
                std::ptr::from_mut(&mut code).cast(),
                &raw mut size,
            )
        } != 0
        {
            return Err(io::Error::last_os_error());
        }
        if size as usize != std::mem::size_of_val(&code) {
            return Err(invalid("invalid connect status"));
        }
        if code != 0 {
            return Err(io::Error::from_raw_os_error(code));
        }
    }
    let mut stream = UnixStream::from(fd);
    let credentials = peer(&stream, pid)?;
    let id = uuid::Uuid::new_v4().to_string();
    let request = json!({"jsonrpc": "2.0", "id": id, "method": "system.shutdown", "params": {}});
    events.push(json!({"event": "ShutdownRequest", "peer": credentials, "request": request}));
    let bytes = format!("{request}\n").into_bytes();
    let mut sent = 0;
    while sent < bytes.len() {
        remaining(deadline)?;
        match stream.write(&bytes[sent..]) {
            Ok(0) => return Err(invalid("shutdown request write closed")),
            Ok(count) => sent += count,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                ready(stream.as_raw_fd(), libc::POLLOUT, deadline)?;
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
    let mut bytes = Vec::new();
    loop {
        remaining(deadline)?;
        let mut buffer = [0; 1024];
        match stream.read(&mut buffer) {
            Ok(0) => return Err(invalid("shutdown closed before response")),
            Ok(count) => bytes.extend_from_slice(&buffer[..count]),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                readable(stream.as_raw_fd(), deadline)?;
                continue;
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        }
        if bytes.len() > 4096 {
            return Err(invalid("shutdown response exceeds bound"));
        }
        if let Some(end) = bytes.iter().position(|byte| *byte == b'\n') {
            events.push(json!({"event": "ShutdownResponse", "bytes": bytes}));
            if end + 1 != bytes.len() {
                return Err(invalid("extra shutdown response data"));
            }
            let value: Value = serde_json::from_slice(&bytes[..end]).map_err(io::Error::other)?;
            response(&value, &id)?;
            remaining(deadline)?;
            events.push(json!({"event": "ShutdownAccepted", "id": id, "peer": credentials}));
            return Ok(());
        }
    }
}

pub(super) fn normal(status: ExitStatus) -> bool {
    status.into_raw() == 0
        && status.code() == Some(0)
        && status.signal().is_none()
        && !status.core_dumped()
}

pub(super) fn wait_record(result: &io::Result<ExitStatus>) -> Value {
    json!({"event": "DaemonWaitResult", "ok": result.is_ok(),
        "rawStatus": result.as_ref().ok().map(|s| s.into_raw()),
        "code": result.as_ref().ok().and_then(ExitStatus::code),
        "signal": result.as_ref().ok().and_then(ExitStatusExt::signal),
        "coreDumped": result.as_ref().ok().map(ExitStatusExt::core_dumped),
        "normalExit": result.as_ref().is_ok_and(|status| normal(*status)), "error": result.as_ref().err().map(ToString::to_string)})
}

#[test]
fn normal_exit_and_response_contract() {
    for raw in [0, 1 << 8, libc::SIGKILL, libc::SIGTERM] {
        assert_eq!(normal(ExitStatus::from_raw(raw)), raw == 0);
    }
    assert!(
        !wait_record(&Err(io::Error::from_raw_os_error(libc::ECHILD)))["normalExit"]
            .as_bool()
            .unwrap()
    );
    let accepted = json!({"jsonrpc":"2.0","id":"owned","result":{"ok":true,"stopping":true}});
    assert!(response(&accepted, "owned").is_ok());
    for bad in [
        json!({"id":"owned","result":{"ok":true,"stopping":true}}),
        json!({"jsonrpc":"2.0","id":"foreign","result":{"ok":true,"stopping":true}}),
        json!({"jsonrpc":"2.0","id":"owned","result":{"ok":true}}),
        json!({"jsonrpc":"2.0","id":"owned","error":null,"result":{"ok":true,"stopping":true}}),
    ] {
        assert!(response(&bad, "owned").is_err());
    }
}

// Only these controls own a shell. The daemon-owned provider is never passed
// here. Construct the single Child owner immediately after spawn, before any
// fallible registration, identity read or assertion; catch the whole body.
struct ControlShell {
    // raw-child: allow — the control catches every body outcome and performs a bounded direct wait before propagation.
    child: Child,
    input: Option<std::process::ChildStdin>,
    waited: Option<Value>,
    identity: Value,
}

impl ControlShell {
    fn initialize(&mut self) -> io::Result<()> {
        self.input = self.child.stdin.take();
        let input = self
            .input
            .as_ref()
            .ok_or_else(|| invalid("control stdin missing"))?;
        // This descriptor belongs solely to this control's pipe.
        let flags = unsafe { libc::fcntl(input.as_raw_fd(), libc::F_GETFL) };
        if flags < 0
            || unsafe { libc::fcntl(input.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) }
                < 0
        {
            return Err(io::Error::last_os_error());
        }
        #[cfg(target_os = "linux")]
        {
            let raw = std::fs::read_to_string(format!("/proc/{}/stat", self.child.id()))?;
            let fields: Vec<_> = raw
                .rsplit_once(") ")
                .ok_or_else(|| invalid("control stat"))?
                .1
                .split_whitespace()
                .collect();
            if fields.len() < 20 || fields[1] != std::process::id().to_string() {
                return Err(invalid("control child parent changed"));
            }
            self.identity = json!({"pid":self.child.id(),"ppid":fields[1],"pgid":fields[2],
                "sid":fields[3],"start":fields[19],"executable":std::fs::read_link(format!("/proc/{}/exe",self.child.id()))?,
                "argv":["/bin/sh","-c","read -r line; test \"$line\" = release"]});
        }
        Ok(())
    }

    fn wait_with_release(&mut self, deadline: Instant) -> io::Result<()> {
        let input = self
            .input
            .as_mut()
            .ok_or_else(|| invalid("control stdin missing"))?;
        let mut wait = ExitWait::new(&mut self.child)?;
        let mut pending = false;
        wait.until_exit(deadline, || {
            pending = true;
            let mut bytes = b"release\n".as_slice();
            while !bytes.is_empty() {
                remaining(deadline)?;
                match input.write(bytes) {
                    Ok(0) => return Err(invalid("control release closed")),
                    Ok(count) => bytes = &bytes[count..],
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                        ready(input.as_raw_fd(), libc::POLLOUT, deadline)?;
                    }
                    Err(error) => return Err(error),
                }
            }
            Ok(())
        })?;
        let result = wait.reap();
        self.waited = Some(wait_record(&result));
        let status = result?;
        assert!(
            pending,
            "a real pending registered wait must precede release"
        );
        assert!(normal(status));
        Ok(())
    }

    fn finish(&mut self) -> Value {
        let deadline = Instant::now() + BOUND;
        let closed = self
            .input
            .take()
            .map(|input| close_descriptor("control-stdin", input));
        let forced = self.waited.is_none();
        let mut row = json!({"pid":self.child.id(),"identity":self.identity,"stdinClose":closed,
            "forcedFailureCleanup":forced,"scope":"only this control shell; never daemon provider",
            "cleanupBudgetNs":BOUND.as_nanos(),"kill":null,"wait":self.waited,"error":null});
        if forced {
            let result = (|| -> io::Result<()> {
                let killed = self.child.kill();
                row["kill"] = json!({"ok":killed.is_ok(),"error":killed.as_ref().err().map(ToString::to_string)});
                // Even a failed kill must not discard the direct owner. Qualified
                // readiness can still allow its one actual consuming wait.
                let mut wait = ExitWait::new(&mut self.child)?;
                wait.until_exit(deadline, || Ok(()))?;
                let waited = wait.reap();
                self.waited = Some(wait_record(&waited));
                row["wait"] = json!(self.waited);
                waited?;
                killed?;
                remaining(deadline)?;
                Ok(())
            })();
            if let Err(error) = result {
                row["error"] = json!(error.to_string());
            }
        }
        row["settled"] = json!(
            row["error"].is_null()
                && row["wait"]["ok"] == true
                && row["stdinClose"]["ok"] == true
                && remaining(deadline).is_ok()
        );
        row
    }
}

struct ControlOutcome {
    body: std::thread::Result<io::Result<()>>,
    receipt: Value,
    reporting: io::Result<()>,
}

fn shell_control(body: impl FnOnce(&mut ControlShell) -> io::Result<()>) -> ControlOutcome {
    use std::process::{Command, Stdio};
    let child = Command::new("/bin/sh")
        .args(["-c", "read -r line; test \"$line\" = release"])
        .env_clear()
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("control shell");
    let mut owner = ControlShell {
        child,
        input: None,
        waited: None,
        identity: Value::Null,
    };
    let body = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        owner.initialize()?;
        body(&mut owner)
    }));
    // Settle before allocating/serializing the classification: the original
    // payload remains retained independently of any later reporting failure.
    let cleanup = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| owner.finish()));
    let classification = match &body {
        Ok(Ok(())) => json!({"kind":"ok"}),
        Ok(Err(error)) => {
            json!({"kind":"error","errorKind":format!("{:?}",error.kind()),"message":error.to_string()})
        }
        Err(payload) => json!({"kind":"panic","message":payload.downcast_ref::<&str>().copied()
            .or_else(|| payload.downcast_ref::<String>().map(String::as_str))}),
    };
    // Every assertion/error/panic was caught while this owner remained alive.
    // Only this shell is settled; errors/uncertainty stay failed, not supervisor
    // adoption. This separately labelled control budget extends no daemon wait.
    let receipt = json!({"body":classification,"settlement":match cleanup {
        Ok(row) => row, Err(_) => json!({"settled":false,"error":"control cleanup panicked"}),
    }});
    let reporting = report("owned-shell-control", &receipt);
    ControlOutcome {
        body,
        receipt,
        reporting,
    }
}

fn require_control(outcome: ControlOutcome) {
    // Original outcome takes precedence. The separately emitted receipt exposes
    // cleanup/report errors even when the original error or panic is propagated.
    match outcome.body {
        Err(payload) => std::panic::resume_unwind(payload),
        Ok(Err(error)) => panic!("control body failed: {error}"),
        Ok(Ok(())) => {}
    }
    outcome.reporting.expect("complete shell control report");
    assert_eq!(
        outcome.receipt["settlement"]["settled"], true,
        "{}",
        outcome.receipt
    );
    assert_eq!(
        outcome.receipt["settlement"]["forcedFailureCleanup"], false,
        "{}",
        outcome.receipt
    );
}

#[test]
fn actual_child_pending_cancellation_and_wait() {
    require_control(shell_control(|owner| {
        let mut cancelled = ExitWait::new(&mut owner.child)?;
        let queue = cancelled.queue.as_raw_fd();
        #[cfg(target_os = "linux")]
        let pidfd = cancelled.pidfd.as_raw_fd();
        assert!(cancelled.poll(Duration::ZERO)?.is_pending());
        drop(cancelled);
        // No descriptors are allocated between cancellation and these checks.
        assert_eq!(unsafe { libc::fcntl(queue, libc::F_GETFD) }, -1);
        assert_eq!(io::Error::last_os_error().raw_os_error(), Some(libc::EBADF));
        #[cfg(target_os = "linux")]
        {
            assert_eq!(unsafe { libc::fcntl(pidfd, libc::F_GETFD) }, -1);
            assert_eq!(io::Error::last_os_error().raw_os_error(), Some(libc::EBADF));
        }
        owner.wait_with_release(Instant::now() + BOUND)
    }));
}

#[test]
fn ack_and_expired_budget_cannot_complete_a_child_wait() {
    require_control(shell_control(|owner| {
        let mut wait = ExitWait::new(&mut owner.child)?;
        assert_eq!(
            wait.until_exit(Instant::now(), || panic!(
                "expired waiter released provider"
            ))
            .unwrap_err()
            .kind(),
            io::ErrorKind::TimedOut
        );
        assert!(
            wait.reap().is_err(),
            "an accepted request is not exit readiness"
        );
        // Explicit finite test-control settlement after the unchanged negative
        // operation. This is neither fixture success nor extra daemon grace.
        owner.wait_with_release(Instant::now() + BOUND)
    }));
}

#[test]
fn post_spawn_failure_and_panic_keep_child_ownership() {
    for injected in ["error", "panic"] {
        let outcome = shell_control(|owner| {
            let mut wait = ExitWait::new(&mut owner.child)?;
            assert!(wait.poll(Duration::ZERO)?.is_pending());
            assert!(injected != "panic", "injected post-spawn panic");
            Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "injected post-spawn error",
            ))
        });
        outcome
            .reporting
            .expect("complete injected ownership report");
        assert_eq!(
            outcome.receipt["settlement"]["settled"], true,
            "{}",
            outcome.receipt
        );
        assert_eq!(outcome.receipt["settlement"]["forcedFailureCleanup"], true);
        match outcome.body {
            Ok(Err(error)) if injected == "error" => {
                assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
                assert_eq!(error.to_string(), "injected post-spawn error");
            }
            Err(payload) if injected == "panic" => assert_eq!(
                payload.downcast_ref::<&str>(),
                Some(&"injected post-spawn panic")
            ),
            _ => panic!("injected classification changed"),
        }
    }
}

#[test]
fn finalization_expiry_and_incomplete_cleanup_cannot_pass() {
    use std::cell::Cell;
    for case in ["on-time", "late-finalization", "incomplete"] {
        let mut directory: FixtureDir = super::common::test_tempdir("itd-woc-finalization-").into();
        let start = Instant::now();
        let deadline = start + BOUND;
        let clock = Cell::new(start);
        let path = directory.path().to_owned();
        let (resources, completed) = finalize_and_measure(
            start,
            deadline,
            || {
                // Real explicit directory close; only the clock advancement and the
                // incomplete semantic classification are synthetic contract inputs.
                let directory = directory.finalize(false);
                clock.set(if case == "late-finalization" {
                    deadline + Duration::from_nanos(1)
                } else {
                    start + Duration::from_nanos(1)
                });
                json!({"complete":directory_complete(&directory) && case != "incomplete", "directory":directory})
            },
            || clock.get(),
        );
        report(
            "finalization-contract",
            &json!({"case":case,"resources":resources,"completion":completed}),
        )
        .unwrap();
        assert_eq!(resources["directory"]["closeOk"], true);
        assert!(!path.exists());
        assert_eq!(completed["normalCompletion"], case == "on-time");
        assert_eq!(completed["withinDeadline"], case != "late-finalization");
        assert_eq!(
            completed["semanticFinalizationComplete"],
            case != "incomplete"
        );
    }
}

#[test]
fn shutdown_rejects_another_peer_before_sending() {
    use std::os::unix::net::UnixListener;
    let root = super::common::test_tempdir("itd-woc-peer-");
    let socket = root.path().join("peer.sock");
    let listener = UnixListener::bind(&socket).unwrap();
    let mut events = Vec::new();
    assert!(shutdown(&socket, u32::MAX, Instant::now() + BOUND, &mut events).is_err());
    assert!(events.is_empty(), "no request sent to an unqualified peer");
    drop(listener);
}
