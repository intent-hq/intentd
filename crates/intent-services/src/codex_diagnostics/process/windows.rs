//! Diagnostic-local Windows containment, independent of console process groups.
//!
//! The leader is created with `CREATE_SUSPENDED`, assigned to a private Job Object,
//! and only then resumed. Its original Command owns argv/env/cwd/stdio handling.
//! The unnamed, noninheritable job stays open after leader exit; neither breakaway
//! limit is enabled. Nested jobs require Windows 8+, and an incompatible enclosing
//! job is an error before provider execution, never a reason to launch unowned.
//!
//! Platform contracts:
//! - <https://learn.microsoft.com/en-us/windows/win32/procthread/process-creation-flags>
//! - <https://learn.microsoft.com/en-us/windows/win32/procthread/job-objects>
//! - <https://learn.microsoft.com/en-us/windows/win32/procthread/nested-jobs>
//! - <https://learn.microsoft.com/en-us/windows/win32/api/jobapi2/nf-jobapi2-terminatejobobject>
//!
//! Accounting can reach zero before process handles signal termination. A private
//! completion port records process identities from before the first assignment.
//! Cleanup reconciles distinct identities with lifetime accounting AND waits for
//! every obtainable process handle. Missing notifications or ambiguous accounting
//! fail closed; neither an empty job nor a notification proves termination.
//! - <https://learn.microsoft.com/en-us/windows/win32/api/winnt/ns-winnt-jobobject_associate_completion_port>
//! - <https://learn.microsoft.com/en-us/windows/win32/api/winnt/ns-winnt-jobobject_basic_accounting_information>
//! - <https://devblogs.microsoft.com/oldnewthing/20110107-00/?p=11803>

use std::collections::{btree_map::Entry, BTreeMap};
use std::io;
use std::mem::size_of;
use std::os::windows::io::{AsHandle, AsRawHandle, FromRawHandle, OwnedHandle};
use std::os::windows::process::CommandExt;
use std::process::{Child, ExitStatus, Stdio};
use std::ptr::{null, null_mut};
use std::time::{Duration, Instant};

use tokio::process::{ChildStderr, ChildStdin, ChildStdout, Command};
use windows_sys::Win32::Foundation::{
    ERROR_INVALID_PARAMETER, ERROR_NO_MORE_FILES, INVALID_HANDLE_VALUE, WAIT_OBJECT_0, WAIT_TIMEOUT,
};
use windows_sys::Win32::System::Diagnostics::ToolHelp::{
    CreateToolhelp32Snapshot, Thread32First, Thread32Next, TH32CS_SNAPTHREAD, THREADENTRY32,
};
use windows_sys::Win32::System::JobObjects::{
    AssignProcessToJobObject, CreateJobObjectW, JobObjectAssociateCompletionPortInformation,
    JobObjectBasicAccountingInformation, JobObjectExtendedLimitInformation,
    QueryInformationJobObject, SetInformationJobObject, TerminateJobObject,
    JOBOBJECT_ASSOCIATE_COMPLETION_PORT, JOBOBJECT_BASIC_ACCOUNTING_INFORMATION,
    JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
};
use windows_sys::Win32::System::SystemServices::JOB_OBJECT_MSG_NEW_PROCESS;
use windows_sys::Win32::System::Threading::{
    GetProcessIdOfThread, OpenProcess, OpenThread, ResumeThread, WaitForSingleObject,
    CREATE_NO_WINDOW, CREATE_SUSPENDED, PROCESS_SYNCHRONIZE, THREAD_QUERY_LIMITED_INFORMATION,
    THREAD_SUSPEND_RESUME,
};
use windows_sys::Win32::System::IO::{CreateIoCompletionPort, GetQueuedCompletionStatus};

const CLEANUP_TIMEOUT: Duration = Duration::from_secs(5);
const POLL_INTERVAL: Duration = Duration::from_millis(10);
const COMPLETION_KEY: usize = 1;
const MESSAGE_BATCH: usize = 64;

struct Job {
    handle: OwnedHandle,
    port: OwnedHandle,
}

impl Job {
    fn new() -> io::Result<Self> {
        // SAFETY: null security attributes make the unnamed handle noninheritable.
        let raw = unsafe { CreateJobObjectW(null(), null()) };
        if raw.is_null() {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: successful CreateJobObjectW transfers this unique handle to us.
        let handle = unsafe { OwnedHandle::from_raw_handle(raw) };
        let mut limits = JOBOBJECT_EXTENDED_LIMIT_INFORMATION::default();
        limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
        // No BREAKAWAY_OK or SILENT_BREAKAWAY_OK: new console groups and nested
        // jobs cannot release a descendant from this job's lifetime.
        // SAFETY: the handle is live and the buffer has the documented layout/size.
        let ok = unsafe {
            SetInformationJobObject(
                handle.as_raw_handle(),
                JobObjectExtendedLimitInformation,
                std::ptr::from_ref(&limits).cast(),
                size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>()
                    .try_into()
                    .expect("job information fits DWORD"),
            )
        };
        if ok == 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: INVALID_HANDLE_VALUE creates a new, unassociated completion
        // port. Its handle is not inheritable and is used only by this owner.
        // Async polls may move between workers; do not impose a one-thread port
        // quota that stays occupied by a previous worker after a task yields.
        let raw = unsafe { CreateIoCompletionPort(INVALID_HANDLE_VALUE, null_mut(), 0, u32::MAX) };
        if raw.is_null() {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: the newly created port transfers its unique handle to us.
        let port = unsafe { OwnedHandle::from_raw_handle(raw) };
        let association = JOBOBJECT_ASSOCIATE_COMPLETION_PORT {
            CompletionKey: std::ptr::without_provenance_mut(COMPLETION_KEY),
            CompletionPort: port.as_raw_handle(),
        };
        // Associate while the job is empty, before any provider can execute or
        // exit. Late association would lose its historical process identities.
        // SAFETY: both handles are live; the buffer has the required layout.
        if unsafe {
            SetInformationJobObject(
                handle.as_raw_handle(),
                JobObjectAssociateCompletionPortInformation,
                std::ptr::from_ref(&association).cast(),
                size_of::<JOBOBJECT_ASSOCIATE_COMPLETION_PORT>()
                    .try_into()
                    .expect("completion association fits DWORD"),
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        Ok(Self { handle, port })
    }

    fn assign(&self, child: &Child) -> io::Result<()> {
        // SAFETY: both handles remain owned throughout this call. The child has
        // not been resumed. Nested-job restrictions fail closed here.
        if unsafe { AssignProcessToJobObject(self.handle.as_raw_handle(), child.as_raw_handle()) }
            == 0
        {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    fn terminate(&self) -> io::Result<()> {
        // SAFETY: the job handle remains live; termination also covers child jobs.
        if unsafe { TerminateJobObject(self.handle.as_raw_handle(), 1) } == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    fn accounting(&self) -> io::Result<JOBOBJECT_BASIC_ACCOUNTING_INFORMATION> {
        let mut info = JOBOBJECT_BASIC_ACCOUNTING_INFORMATION::default();
        // SAFETY: the writable buffer has the requested information class's size.
        let ok = unsafe {
            QueryInformationJobObject(
                self.handle.as_raw_handle(),
                JobObjectBasicAccountingInformation,
                std::ptr::from_mut(&mut info).cast(),
                size_of::<JOBOBJECT_BASIC_ACCOUNTING_INFORMATION>()
                    .try_into()
                    .expect("job accounting fits DWORD"),
                null_mut(),
            )
        };
        if ok == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(info)
    }

    fn next_message(&self) -> io::Result<Option<(u32, u32)>> {
        let mut message = 0;
        let mut key = 0;
        let mut value = null_mut();
        // SAFETY: valid private port and writable output buffers. The zero
        // timeout never blocks the executor. Job messages encode a PID in the
        // OVERLAPPED value; it is never dereferenced as a pointer.
        if unsafe {
            GetQueuedCompletionStatus(
                self.port.as_raw_handle(),
                &raw mut message,
                &raw mut key,
                &raw mut value,
                0,
            )
        } == 0
        {
            let error = io::Error::last_os_error();
            if error.raw_os_error() == Some(WAIT_TIMEOUT.try_into().expect("error fits i32")) {
                return Ok(None);
            }
            return Err(error);
        }
        if key != COMPLETION_KEY {
            return Err(io::Error::other("unexpected diagnostic job notification"));
        }
        let pid = value
            .addr()
            .try_into()
            .map_err(|_| io::Error::other("invalid diagnostic process identity"))?;
        Ok(Some((message, pid)))
    }
}

#[derive(Default)]
struct ProcessHistory {
    // Keep handles until cleanup settles, including already signaled handles.
    // None means OpenProcess confirmed that this nonzero PID no longer exists.
    // Distinct PIDs deliberately undercount recycled lifetimes: a reused PID or
    // missing notification must not compensate for a different missing process.
    entries: BTreeMap<u32, Option<OwnedHandle>>,
}

impl ProcessHistory {
    fn observe(&mut self, pid: u32) -> io::Result<()> {
        if pid == 0 {
            return Err(io::Error::other("missing diagnostic process identity"));
        }
        if let Entry::Vacant(entry) = self.entries.entry(pid) {
            // SAFETY: only request synchronization, not termination or memory
            // access. If this PID was recycled, waiting on its later lifetime is
            // conservative: it can never confirm exit before the original one.
            let raw = unsafe { OpenProcess(PROCESS_SYNCHRONIZE, 0, pid) };
            let process = if raw.is_null() {
                let error = io::Error::last_os_error();
                if error.raw_os_error()
                    != Some(ERROR_INVALID_PARAMETER.try_into().expect("error fits i32"))
                {
                    return Err(error);
                }
                // A running or not-yet-destroyed process object keeps its PID.
                // Access denied and all other errors remain unconfirmed.
                None
            } else {
                // SAFETY: successful OpenProcess transfers the unique handle.
                Some(unsafe { OwnedHandle::from_raw_handle(raw) })
            };
            entry.insert(process);
        }
        Ok(())
    }

    fn reconciles(&self, accounting: &JOBOBJECT_BASIC_ACCOUNTING_INFORMATION) -> bool {
        accounting.ActiveProcesses == 0
            && usize::try_from(accounting.TotalProcesses).expect("DWORD fits usize")
                == self.entries.len()
    }

    fn confirmed(&self, accounting: &JOBOBJECT_BASIC_ACCOUNTING_INFORMATION) -> io::Result<bool> {
        if !self.reconciles(accounting) {
            // TotalProcesses also includes limit-rejected assignments. Without
            // complete identities their termination cannot be certified either.
            return Ok(false);
        }
        for process in self.entries.values().flatten() {
            // SAFETY: each retained handle has SYNCHRONIZE access. Neither job
            // counters nor EXIT_PROCESS messages replace this kernel wait.
            match unsafe { WaitForSingleObject(process.as_raw_handle(), 0) } {
                WAIT_OBJECT_0 => {}
                WAIT_TIMEOUT => return Ok(false),
                _ => return Err(io::Error::last_os_error()),
            }
        }
        Ok(true)
    }
}

pub(super) struct Ownership {
    job: Job,
    child: Child,
    processes: ProcessHistory,
}

impl Ownership {
    pub(super) async fn wait(&mut self) -> io::Result<ExitStatus> {
        loop {
            if let Some(status) = self.child.try_wait()? {
                return Ok(status);
            }
            // Polling the retained process handle is cancellation safe. The
            // caller bounds leader execution; wait never relinquishes the job.
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }

    pub(super) async fn cleanup(mut self) -> io::Result<()> {
        let deadline = Instant::now() + CLEANUP_TIMEOUT;
        // Retain handles for queued identities before initiating termination.
        // Even a collection error must still request whole-job termination.
        let observed = self.collect_processes();
        self.job.terminate()?;
        observed?;
        // TerminateJobObject initiates termination, but is not its acknowledgement.
        // Yield once before checking, also allowing cancellation at this boundary.
        tokio::task::yield_now().await;
        loop {
            self.collect_processes()?;
            if self.processes.confirmed(&self.job.accounting()?)?
                && self.child.try_wait()?.is_some()
                // Re-read after the handles signal: no known process can still
                // be completing creation of an unobserved child at this point.
                && self.processes.reconciles(&self.job.accounting()?)
            {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "diagnostic job termination was not confirmed",
                ));
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }

    fn collect_processes(&mut self) -> io::Result<()> {
        // Bounded batches preserve the cleanup deadline and cancellation even
        // if the provider produces a large notification backlog.
        for _ in 0..MESSAGE_BATCH {
            let Some((message, pid)) = self.job.next_message()? else {
                break;
            };
            if message == JOB_OBJECT_MSG_NEW_PROCESS {
                self.processes.observe(pid)?;
            }
        }
        Ok(())
    }
}

impl Drop for Ownership {
    fn drop(&mut self) {
        // The child may not yet be assigned if setup failed. Never leave that
        // suspended process behind. Closing the last job handle then kills every
        // descendant even if wait/cleanup was cancelled or the runtime shut down.
        let _ = self.child.kill();
    }
}

fn spawn_suspended(mut command: Command) -> io::Result<Ownership> {
    let job = Job::new()?;
    command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut command = command.into_std();
    command.creation_flags(CREATE_SUSPENDED | CREATE_NO_WINDOW);
    // There is no await between creation, guarding, assignment and resumption.
    // Use std Child so a failed Tokio pipe conversion cannot discard an unowned
    // process; every fallible operation after spawn is protected by Ownership.
    let child = command.spawn()?;
    let mut ownership = Ownership {
        job,
        child,
        processes: ProcessHistory::default(),
    };
    ownership.job.assign(&ownership.child)?;
    // The leader's identity and handle are already authoritative. Record it
    // independently so loss of its redundant NEW_PROCESS message is harmless.
    ownership.processes.entries.insert(
        ownership.child.id(),
        Some(ownership.child.as_handle().try_clone_to_owned()?),
    );
    Ok(ownership)
}

#[expect(
    clippy::unused_async,
    reason = "shared platform API; startup has no cancellation gap"
)]
pub(super) async fn spawn(command: Command) -> io::Result<super::Started> {
    let mut ownership = spawn_suspended(command)?;
    let stdin = ownership
        .child
        .stdin
        .take()
        .map(ChildStdin::from_std)
        .transpose()?;
    let stdout = ownership
        .child
        .stdout
        .take()
        .map(ChildStdout::from_std)
        .transpose()?;
    let stderr = ownership
        .child
        .stderr
        .take()
        .map(ChildStderr::from_std)
        .transpose()?;
    let thread = suspended_thread(&ownership.child)?;
    // SAFETY: we retain the verified primary thread handle and its owning process.
    // The job is installed and all three stdio conversions have succeeded.
    match unsafe { ResumeThread(thread.as_raw_handle()) } {
        1 => Ok(super::Started {
            ownership,
            stdin,
            stdout,
            stderr,
        }),
        u32::MAX => Err(io::Error::last_os_error()),
        _ => Err(io::Error::other(
            "unexpected diagnostic thread suspension state",
        )),
    }
}

/// Stable Rust does not expose a Child's primary thread handle. Its suspended
/// primary thread is its only thread; enumerate without running provider code.
/// An ambiguous snapshot fails closed instead of resuming an arbitrary thread.
fn suspended_thread(child: &Child) -> io::Result<OwnedHandle> {
    let deadline = Instant::now() + CLEANUP_TIMEOUT;
    // SAFETY: SNAPTHREAD snapshots thread metadata, not the uninitialized loader.
    let raw = unsafe { CreateToolhelp32Snapshot(TH32CS_SNAPTHREAD, 0) };
    if raw == INVALID_HANDLE_VALUE {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: the successful snapshot returns a unique owned handle.
    let snapshot = unsafe { OwnedHandle::from_raw_handle(raw) };
    let mut entry = THREADENTRY32 {
        dwSize: size_of::<THREADENTRY32>()
            .try_into()
            .expect("thread entry fits DWORD"),
        ..THREADENTRY32::default()
    };
    let mut thread_id = None;
    // SAFETY: snapshot is valid and dwSize describes the writable entry buffer.
    let mut found = unsafe { Thread32First(snapshot.as_raw_handle(), &raw mut entry) };
    while found != 0 {
        // Thread32First can reduce dwSize; do not use fields absent in its output.
        if usize::try_from(entry.dwSize).expect("DWORD fits usize")
            < std::mem::offset_of!(THREADENTRY32, th32OwnerProcessID) + size_of::<u32>()
        {
            return Err(io::Error::other("incomplete diagnostic thread information"));
        }
        if entry.th32OwnerProcessID == child.id() && thread_id.replace(entry.th32ThreadID).is_some()
        {
            return Err(io::Error::other("ambiguous diagnostic primary thread"));
        }
        if Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "diagnostic thread lookup timed out",
            ));
        }
        entry.dwSize = size_of::<THREADENTRY32>()
            .try_into()
            .expect("thread entry fits DWORD");
        // SAFETY: same snapshot and initialized output buffer as Thread32First.
        found = unsafe { Thread32Next(snapshot.as_raw_handle(), &raw mut entry) };
    }
    let error = io::Error::last_os_error();
    if error.raw_os_error() != Some(ERROR_NO_MORE_FILES.try_into().expect("error fits i32")) {
        return Err(error);
    }
    let thread_id =
        thread_id.ok_or_else(|| io::Error::other("diagnostic primary thread missing"))?;
    // SAFETY: request only resume and identity-query rights, without inheritance.
    let raw = unsafe {
        OpenThread(
            THREAD_SUSPEND_RESUME | THREAD_QUERY_LIMITED_INFORMATION,
            0,
            thread_id,
        )
    };
    if raw.is_null() {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: a successful OpenThread transfers this unique handle to us.
    let thread = unsafe { OwnedHandle::from_raw_handle(raw) };
    // SAFETY: thread has query rights. Revalidate identity to reject a reused TID
    // if something outside the probe terminated the suspended process meanwhile.
    if unsafe { GetProcessIdOfThread(thread.as_raw_handle()) } != child.id() {
        return Err(io::Error::other("diagnostic primary thread owner changed"));
    }
    Ok(thread)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::Future;
    use std::io::{BufRead, Read, Write};
    use std::net::TcpStream;
    use std::task::Poll;
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
    use tokio::net::TcpListener;
    use tokio::time::timeout;
    use windows_sys::Win32::Foundation::{
        SetHandleInformation, ERROR_ACCESS_DENIED, HANDLE_FLAG_INHERIT, WAIT_OBJECT_0, WAIT_TIMEOUT,
    };
    use windows_sys::Win32::System::Threading::{
        GetCurrentProcess, OpenProcess, TerminateProcess, WaitForSingleObject,
        CREATE_BREAKAWAY_FROM_JOB, CREATE_NEW_PROCESS_GROUP, DETACHED_PROCESS,
        PROCESS_QUERY_LIMITED_INFORMATION, PROCESS_SYNCHRONIZE, PROCESS_TERMINATE,
    };

    const TEST_TIMEOUT: Duration = Duration::from_secs(15);
    const FIXTURE_MODE: &str = "INTENTD_WINDOWS_PROBE_FIXTURE_MODE";
    const FIXTURE_ADDRESS: &str = "INTENTD_WINDOWS_PROBE_FIXTURE_ADDRESS";
    const ARGUMENT: &str = r#"spaces \"quoted\" λ & | < > trailing\"#;

    fn fixture_command(mode: &str, address: std::net::SocketAddr) -> Command {
        let mut command = Command::new(std::env::current_exe().unwrap());
        let module = module_path!().split_once("::").unwrap().1;
        command
            .args([
                "--exact",
                &format!("{module}::native_fixture"),
                "--nocapture",
            ])
            .env(FIXTURE_MODE, mode)
            .env(FIXTURE_ADDRESS, address.to_string());
        command
    }

    // Run this same test executable as the provider; no shell, Node, PowerShell,
    // downloaded binaries, or account credentials are needed. The test harness
    // may prefix stdout. Provider payloads have explicit markers; fixture startup
    // acknowledgements use a separate TCP connection, never provider stdio.
    #[test]
    fn native_fixture() {
        let Ok(mode) = std::env::var(FIXTURE_MODE) else {
            return;
        };
        let address = std::env::var(FIXTURE_ADDRESS).unwrap().parse().unwrap();
        let _nested = (mode == "descendant_nested").then(|| {
            let job = Job::new().unwrap();
            // SAFETY: both handles are valid, and the new job has no UI limits.
            assert_ne!(
                unsafe {
                    AssignProcessToJobObject(job.handle.as_raw_handle(), GetCurrentProcess())
                },
                0
            );
            job
        });
        let mut control = TcpStream::connect(address).unwrap();
        // A broken assertion in the test driver must not leave its fixtures alive.
        control
            .set_read_timeout(Some(Duration::from_secs(30)))
            .unwrap();
        writeln!(control, "{}", std::process::id()).unwrap();
        if mode == "conversation" {
            let mut request = String::new();
            std::io::stdin().read_line(&mut request).unwrap();
            let reply = serde_json::json!({
                "request": request,
                "argument": std::env::args().next_back().unwrap(),
                "value": std::env::var("DIAGNOSTIC_FIXTURE_VALUE").unwrap(),
                "removed": std::env::var_os("DIAGNOSTIC_FIXTURE_REMOVED").is_none(),
                "cwd": std::env::current_dir().unwrap(),
            });
            writeln!(std::io::stdout(), "\nprovider-out:{reply}").unwrap();
            std::io::stdout().flush().unwrap();
            writeln!(std::io::stderr(), "provider-err").unwrap();
            std::io::stderr().flush().unwrap();
            std::process::exit(
                std::env::var("DIAGNOSTIC_FIXTURE_EXIT")
                    .unwrap()
                    .parse()
                    .unwrap(),
            );
        }
        if mode.starts_with("tree_") || mode == "breakaway" {
            // Redirecting STARTUPINFO's standard streams does not suppress
            // inheritance of the old pipe handles. Clear their inherit flags
            // before spawning; Stdio::inherit explicitly duplicates them when
            // the fixture is meant to keep the provider's output open.
            for handle in [
                std::io::stdin().as_raw_handle(),
                std::io::stdout().as_raw_handle(),
                std::io::stderr().as_raw_handle(),
            ] {
                // SAFETY: these are this fixture's live standard pipe handles.
                assert_ne!(
                    unsafe { SetHandleInformation(handle, HANDLE_FLAG_INHERIT, 0) },
                    0
                );
            }
            let child_mode = if mode == "tree_nested" {
                "descendant_nested"
            } else {
                "descendant"
            };
            let mut child = fixture_command(child_mode, address).into_std();
            let mut flags = CREATE_NEW_PROCESS_GROUP | DETACHED_PROCESS;
            if mode == "breakaway" {
                flags |= CREATE_BREAKAWAY_FROM_JOB;
            }
            child.creation_flags(flags).stdin(Stdio::null());
            if mode == "tree_closed" {
                child.stdout(Stdio::null()).stderr(Stdio::null());
            } else {
                child.stdout(Stdio::inherit()).stderr(Stdio::inherit());
            }
            let result = child.spawn();
            if mode == "breakaway" {
                match result {
                    Err(error) => assert_eq!(
                        error.raw_os_error(),
                        Some(ERROR_ACCESS_DENIED.try_into().unwrap())
                    ),
                    Ok(mut escaped) => {
                        let _ = escaped.kill();
                        let _ = escaped.wait();
                        panic!("probe descendant escaped the job");
                    }
                }
                writeln!(control, "breakaway-denied").unwrap();
            } else {
                // The job, test-driver process handle, and child's control socket
                // own its lifetime; natural leader exit must NOT kill it here.
                drop(result.unwrap());
            }
        }
        let mut request = String::new();
        let _ = std::io::BufReader::new(control).read_line(&mut request);
        std::process::exit(request.trim().parse().unwrap_or(0));
    }

    struct ObservedProcess(OwnedHandle);

    impl ObservedProcess {
        fn open(pid: u32) -> Self {
            // SAFETY: only query/wait/terminate a PID acknowledged by our fixture.
            let raw = unsafe {
                OpenProcess(
                    PROCESS_SYNCHRONIZE | PROCESS_TERMINATE | PROCESS_QUERY_LIMITED_INFORMATION,
                    0,
                    pid,
                )
            };
            assert!(!raw.is_null(), "open acknowledged fixture process");
            // SAFETY: OpenProcess succeeded and this guard owns the unique handle.
            Self(unsafe { OwnedHandle::from_raw_handle(raw) })
        }

        fn stopped(&self) -> bool {
            // SAFETY: retained process handle prevents PID reuse from affecting
            // this assertion; zero timeout inspects the kernel termination state.
            match unsafe { WaitForSingleObject(self.0.as_raw_handle(), 0) } {
                WAIT_OBJECT_0 => true,
                WAIT_TIMEOUT => false,
                _ => panic!("fixture process wait failed"),
            }
        }

        async fn wait_stopped(&self) {
            timeout(TEST_TIMEOUT, async {
                while !self.stopped() {
                    tokio::time::sleep(POLL_INTERVAL).await;
                }
            })
            .await
            .expect("fixture process must stop");
        }
    }

    impl Drop for ObservedProcess {
        fn drop(&mut self) {
            // SAFETY: only our fixture's retained process handle is terminated.
            // This independent guard also cleans up after a failing regression.
            unsafe {
                TerminateProcess(self.0.as_raw_handle(), 1);
            }
        }
    }

    struct Control {
        stream: BufReader<tokio::net::TcpStream>,
        process: ObservedProcess,
    }

    impl Control {
        async fn accept(listener: &TcpListener) -> Self {
            let (socket, _) = timeout(TEST_TIMEOUT, listener.accept())
                .await
                .unwrap()
                .unwrap();
            let mut stream = BufReader::new(socket);
            let mut pid = String::new();
            timeout(TEST_TIMEOUT, stream.read_line(&mut pid))
                .await
                .unwrap()
                .unwrap();
            let process = ObservedProcess::open(pid.trim().parse().unwrap());
            Self { stream, process }
        }

        async fn exit(&mut self, code: u32) {
            self.stream
                .get_mut()
                .write_all(format!("{code}\n").as_bytes())
                .await
                .unwrap();
        }
    }

    async fn tree(mode: &str) -> (super::super::Started, Control, Control) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let started = spawn(fixture_command(mode, listener.local_addr().unwrap()))
            .await
            .unwrap();
        let leader = Control::accept(&listener).await;
        let descendant = Control::accept(&listener).await;
        assert!(
            !descendant.process.stopped(),
            "readiness acknowledged by live child"
        );
        (started, leader, descendant)
    }

    async fn conversation(exit: u32) {
        let home = crate::test_support::test_tempdir("windows-probe-stdio-");
        let cwd = home.path().join("working directory λ");
        std::fs::create_dir(&cwd).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut command = fixture_command("conversation", listener.local_addr().unwrap());
        command
            .env_clear()
            // Winsock's registered provider DLL paths may contain SystemRoot.
            // Keep that OS prerequisite while clearing the rest of the fixture
            // environment; production spawn must not modify the supplied env.
            .env("SystemRoot", std::env::var_os("SystemRoot").unwrap())
            .env(FIXTURE_MODE, "conversation")
            .env(FIXTURE_ADDRESS, listener.local_addr().unwrap().to_string())
            .env("DIAGNOSTIC_FIXTURE_EXIT", exit.to_string())
            .env("DIAGNOSTIC_FIXTURE_VALUE", "value with spaces λ")
            .env("DIAGNOSTIC_FIXTURE_REMOVED", "discard")
            .env_remove("DIAGNOSTIC_FIXTURE_REMOVED")
            .args(["--skip", ARGUMENT])
            .current_dir(&cwd);
        let mut started = spawn(command).await.unwrap();
        let leader = tokio::select! {
            leader = Control::accept(&listener) => leader,
            status = started.ownership.wait() => {
                let mut stderr = String::new();
                timeout(TEST_TIMEOUT, started.stderr.as_mut().unwrap().read_to_string(&mut stderr))
                    .await.unwrap().unwrap();
                panic!("conversation fixture exited before readiness: {status:?}; {stderr}");
            }
        };
        let mut input = started.stdin.take().unwrap();
        input
            .write_all(b"request over provider stdin\n")
            .await
            .unwrap();
        input.flush().await.unwrap();
        drop(input);
        let status = timeout(TEST_TIMEOUT, started.ownership.wait())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(status.code(), Some(exit.try_into().unwrap()));
        assert_eq!(started.ownership.wait().await.unwrap(), status);
        let mut out = String::new();
        let mut err = String::new();
        timeout(
            TEST_TIMEOUT,
            started.stdout.as_mut().unwrap().read_to_string(&mut out),
        )
        .await
        .unwrap()
        .unwrap();
        timeout(
            TEST_TIMEOUT,
            started.stderr.as_mut().unwrap().read_to_string(&mut err),
        )
        .await
        .unwrap()
        .unwrap();
        let payload = out
            .lines()
            .find_map(|line| line.strip_prefix("provider-out:"))
            .expect("provider stdout preserved");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(payload).unwrap(),
            serde_json::json!({
                "request": "request over provider stdin\n",
                "argument": ARGUMENT,
                "value": "value with spaces λ",
                "removed": true,
                "cwd": cwd,
            })
        );
        assert_eq!(err.trim(), "provider-err");
        started.ownership.cleanup().await.unwrap();
        assert!(leader.process.stopped());
    }

    #[tokio::test]
    async fn preserves_success_status_argv_env_cwd_and_all_stdio() {
        conversation(0).await;
    }

    #[tokio::test]
    async fn preserves_nonzero_leader_status() {
        conversation(23).await;
    }

    #[tokio::test]
    async fn provider_cannot_execute_before_job_assignment_and_resume() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut ownership = spawn_suspended(fixture_command(
            "conversation",
            listener.local_addr().unwrap(),
        ))
        .unwrap();
        let leader = ObservedProcess::open(ownership.child.id());
        let mut stdout = ownership.child.stdout.take().unwrap();
        assert_eq!(ownership.job.accounting().unwrap().ActiveProcesses, 1);
        assert!(ownership.child.try_wait().unwrap().is_none());
        ownership.cleanup().await.unwrap();
        assert!(leader.stopped());
        let mut output = String::new();
        stdout.read_to_string(&mut output).unwrap();
        assert!(
            output.is_empty(),
            "even the fixture harness must not execute while suspended"
        );
    }

    async fn exited_leader(mode: &str) {
        let (mut started, mut leader, descendant) = tree(mode).await;
        leader.exit(17).await;
        assert_eq!(
            timeout(TEST_TIMEOUT, started.ownership.wait())
                .await
                .unwrap()
                .unwrap()
                .code(),
            Some(17)
        );
        assert!(leader.process.stopped());
        assert!(!descendant.process.stopped());
        assert_eq!(
            started.ownership.job.accounting().unwrap().ActiveProcesses,
            1
        );
        let mut stdout = started.stdout.take().unwrap();
        let mut stderr = started.stderr.take().unwrap();
        let mut output = Vec::new();
        let mut error_output = Vec::new();
        if mode == "tree_closed" {
            timeout(TEST_TIMEOUT, stdout.read_to_end(&mut output))
                .await
                .unwrap()
                .unwrap();
            timeout(TEST_TIMEOUT, stderr.read_to_end(&mut error_output))
                .await
                .unwrap()
                .unwrap();
        } else {
            assert!(
                timeout(Duration::from_millis(50), stdout.read_to_end(&mut output))
                    .await
                    .is_err(),
                "live descendant holds inherited stdout after leader exit"
            );
        }
        started.ownership.cleanup().await.unwrap();
        // No grace period after cleanup: success itself promises termination.
        assert!(descendant.process.stopped());
        timeout(TEST_TIMEOUT, stdout.read_to_end(&mut output))
            .await
            .unwrap()
            .unwrap();
        timeout(TEST_TIMEOUT, stderr.read_to_end(&mut error_output))
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn early_leader_exit_with_closed_output_keeps_descendant_owned() {
        exited_leader("tree_closed").await;
    }

    #[tokio::test]
    async fn early_leader_exit_with_inherited_output_cleans_up_at_deadline() {
        exited_leader("tree_inherit").await;
    }

    #[tokio::test]
    async fn nested_job_and_new_process_group_remain_owned() {
        exited_leader("tree_nested").await;
    }

    #[tokio::test]
    async fn dropping_ownership_after_leader_exit_kills_orphaned_descendant() {
        let (mut started, mut leader, descendant) = tree("tree_closed").await;
        leader.exit(0).await;
        assert!(timeout(TEST_TIMEOUT, started.ownership.wait())
            .await
            .unwrap()
            .unwrap()
            .success());
        assert!(!descendant.process.stopped());
        drop(started.ownership);
        descendant.process.wait_stopped().await;
    }

    #[tokio::test]
    async fn explicit_breakaway_is_denied() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut started = spawn(fixture_command("breakaway", listener.local_addr().unwrap()))
            .await
            .unwrap();
        let mut leader = Control::accept(&listener).await;
        let mut reply = String::new();
        timeout(TEST_TIMEOUT, leader.stream.read_line(&mut reply))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(reply.trim(), "breakaway-denied");
        leader.exit(0).await;
        assert!(timeout(TEST_TIMEOUT, started.ownership.wait())
            .await
            .unwrap()
            .unwrap()
            .success());
        started.ownership.cleanup().await.unwrap();
    }

    #[tokio::test]
    async fn leader_deadline_preserves_ownership_until_confirmed_cleanup() {
        let (mut started, leader, descendant) = tree("tree_inherit").await;
        assert!(timeout(Duration::from_millis(25), started.ownership.wait())
            .await
            .is_err());
        assert!(!leader.process.stopped());
        assert!(!descendant.process.stopped());
        started.ownership.cleanup().await.unwrap();
        assert!(leader.process.stopped());
        assert!(descendant.process.stopped());
    }

    #[tokio::test]
    async fn cancelled_wait_closes_job_and_kills_descendants() {
        let (started, leader, descendant) = tree("tree_inherit").await;
        let mut ownership = started.ownership;
        let task = tokio::spawn(async move { ownership.wait().await });
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        leader.process.wait_stopped().await;
        descendant.process.wait_stopped().await;
    }

    #[tokio::test]
    async fn cancelling_cleanup_before_and_after_termination_does_not_leak() {
        for poll_cleanup in [false, true] {
            let (started, leader, descendant) = tree("tree_inherit").await;
            let mut cleanup = Box::pin(started.ownership.cleanup());
            if poll_cleanup {
                std::future::poll_fn(|cx| {
                    assert!(
                        cleanup.as_mut().poll(cx).is_pending(),
                        "cleanup yields after requesting termination"
                    );
                    Poll::Ready(())
                })
                .await;
            }
            drop(cleanup);
            leader.process.wait_stopped().await;
            descendant.process.wait_stopped().await;
        }
    }

    #[tokio::test]
    async fn shared_guard_removes_home_only_after_descendants_stop() {
        let home = crate::test_support::test_tempdir("windows-probe-home-");
        let path = home.path().to_path_buf();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut command = fixture_command("tree_closed", listener.local_addr().unwrap());
        command.current_dir(&path);
        let mut process = super::super::ProbeProcess::spawn(command, home)
            .await
            .unwrap();
        let mut leader = Control::accept(&listener).await;
        let descendant = Control::accept(&listener).await;
        leader.exit(0).await;
        assert!(timeout(TEST_TIMEOUT, process.wait())
            .await
            .unwrap()
            .unwrap()
            .success());
        assert!(path.is_dir());
        assert!(!descendant.process.stopped());
        process.cleanup().await.unwrap();
        assert!(leader.process.stopped());
        assert!(descendant.process.stopped());
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn shared_cleanup_survives_caller_cancellation_and_retry() {
        let home = crate::test_support::test_tempdir("windows-probe-cancel-home-");
        let path = home.path().to_path_buf();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let command = fixture_command("tree_inherit", listener.local_addr().unwrap());
        let mut process = super::super::ProbeProcess::spawn(command, home)
            .await
            .unwrap();
        let leader = Control::accept(&listener).await;
        let descendant = Control::accept(&listener).await;
        let mut cleanup = Box::pin(process.cleanup());
        // The current-thread runtime cannot poll the newly spawned cleanup task
        // before this first poll returns. Cancel exactly at its join boundary.
        std::future::poll_fn(|cx| {
            assert!(cleanup.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        assert!(path.is_dir());
        drop(cleanup);
        process.cleanup().await.unwrap();
        assert!(leader.process.stopped());
        assert!(descendant.process.stopped());
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn empty_accounting_never_substitutes_for_a_signaled_process() {
        let (mut started, mut leader, descendant) = tree("tree_inherit").await;
        leader.exit(0).await;
        started.ownership.wait().await.unwrap();
        timeout(TEST_TIMEOUT, async {
            while started.ownership.processes.entries.len() != 2 {
                started.ownership.collect_processes().unwrap();
                tokio::time::sleep(POLL_INTERVAL).await;
            }
        })
        .await
        .unwrap();
        // Model the native failure deterministically: accounting has already
        // reached zero, but the independently retained descendant is still live.
        let mut accounting = started.ownership.job.accounting().unwrap();
        accounting.ActiveProcesses = 0;
        assert_eq!(accounting.TotalProcesses, 2);
        assert!(!started.ownership.processes.confirmed(&accounting).unwrap());
        assert!(!descendant.process.stopped());
        started.ownership.cleanup().await.unwrap();
        assert!(descendant.process.stopped());
    }

    #[tokio::test]
    async fn repeated_pid_notifications_cannot_cover_missing_lifetimes() {
        let mut ownership = spawn_suspended(fixture_command(
            "descendant",
            "127.0.0.1:1".parse().unwrap(),
        ))
        .unwrap();
        ownership.processes.observe(ownership.child.id()).unwrap();
        ownership.processes.observe(ownership.child.id()).unwrap();
        assert_eq!(ownership.processes.entries.len(), 1);
        ownership.child.kill().unwrap();
        ownership.wait().await.unwrap();
        let mut accounting = ownership.job.accounting().unwrap();
        accounting.ActiveProcesses = 0;
        accounting.TotalProcesses = 2;
        assert!(!ownership.processes.confirmed(&accounting).unwrap());
        ownership.cleanup().await.unwrap();
    }

    #[tokio::test]
    async fn naturally_exited_descendant_remains_in_cleanup_history() {
        let (mut started, mut leader, mut descendant) = tree("tree_closed").await;
        descendant.exit(0).await;
        descendant.process.wait_stopped().await;
        // Release the independent observer before collecting notifications.
        // Cleanup must also handle a process object that has already vanished.
        drop(descendant);
        leader.exit(0).await;
        started.ownership.wait().await.unwrap();
        assert_eq!(
            started.ownership.job.accounting().unwrap().TotalProcesses,
            2
        );
        started.ownership.cleanup().await.unwrap();
        assert!(leader.process.stopped());
    }

    #[tokio::test]
    async fn missing_process_notification_retains_home_and_failed_retry() {
        let home = crate::test_support::test_tempdir("windows-probe-missing-history-");
        let path = home.path().to_path_buf();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut command = fixture_command("tree_closed", listener.local_addr().unwrap());
        command.current_dir(&path);
        let mut process = super::super::ProbeProcess::spawn(command, home)
            .await
            .unwrap();
        let leader = Control::accept(&listener).await;
        let descendant = Control::accept(&listener).await;
        let ownership = process.ownership.as_mut().unwrap();
        timeout(TEST_TIMEOUT, async {
            loop {
                if let Some((message, pid)) = ownership.job.next_message().unwrap() {
                    if message == JOB_OBJECT_MSG_NEW_PROCESS && pid != ownership.child.id() {
                        break;
                    }
                } else {
                    tokio::time::sleep(POLL_INTERVAL).await;
                }
            }
        })
        .await
        .unwrap();
        // Deliberately consume the descendant's sole start notification without
        // recording it. An empty port/count must not imply confirmed cleanup.
        assert_eq!(ownership.job.accounting().unwrap().TotalProcesses, 2);
        assert_eq!(ownership.processes.entries.len(), 1);
        assert!(matches!(
            process.cleanup().await,
            Err(super::super::UnknownReason::CleanupFailed)
        ));
        leader.process.wait_stopped().await;
        descendant.process.wait_stopped().await;
        assert!(path.is_dir(), "unconfirmed history must retain the HOME");
        assert!(matches!(
            process.cleanup().await,
            Err(super::super::UnknownReason::CleanupFailed)
        ));
        assert!(
            path.is_dir(),
            "retry must preserve the failed cleanup result"
        );
        std::fs::remove_dir_all(path).unwrap();
    }
}
