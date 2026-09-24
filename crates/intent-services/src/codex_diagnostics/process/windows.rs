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
//! Job accounting includes child jobs; a job handle itself is NOT a general
//! all-processes-exited wait object, and completion-port messages are not reliable
//! acknowledgements. Cleanup therefore checks authoritative active-process counts.

use std::io;
use std::mem::size_of;
use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle};
use std::os::windows::process::CommandExt;
use std::process::{Child, ExitStatus, Stdio};
use std::ptr::{null, null_mut};
use std::time::{Duration, Instant};

use tokio::process::{ChildStderr, ChildStdin, ChildStdout, Command};
use windows_sys::Win32::Foundation::{ERROR_NO_MORE_FILES, INVALID_HANDLE_VALUE};
use windows_sys::Win32::System::Diagnostics::ToolHelp::{
    CreateToolhelp32Snapshot, Thread32First, Thread32Next, TH32CS_SNAPTHREAD, THREADENTRY32,
};
use windows_sys::Win32::System::JobObjects::{
    AssignProcessToJobObject, CreateJobObjectW, JobObjectBasicAccountingInformation,
    JobObjectExtendedLimitInformation, QueryInformationJobObject, SetInformationJobObject,
    TerminateJobObject, JOBOBJECT_BASIC_ACCOUNTING_INFORMATION,
    JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
};
use windows_sys::Win32::System::Threading::{
    GetProcessIdOfThread, OpenThread, ResumeThread, CREATE_NO_WINDOW, CREATE_SUSPENDED,
    THREAD_QUERY_LIMITED_INFORMATION, THREAD_SUSPEND_RESUME,
};

const CLEANUP_TIMEOUT: Duration = Duration::from_secs(5);
const POLL_INTERVAL: Duration = Duration::from_millis(10);

struct Job(OwnedHandle);

impl Job {
    fn new() -> io::Result<Self> {
        // SAFETY: null security attributes make the unnamed handle noninheritable.
        let raw = unsafe { CreateJobObjectW(null(), null()) };
        if raw.is_null() {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: successful CreateJobObjectW transfers this unique handle to us.
        let job = Self(unsafe { OwnedHandle::from_raw_handle(raw) });
        let mut limits = JOBOBJECT_EXTENDED_LIMIT_INFORMATION::default();
        limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
        // No BREAKAWAY_OK or SILENT_BREAKAWAY_OK: new console groups and nested
        // jobs cannot release a descendant from this job's lifetime.
        // SAFETY: the handle is live and the buffer has the documented layout/size.
        let ok = unsafe {
            SetInformationJobObject(
                job.0.as_raw_handle(),
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
        Ok(job)
    }

    fn assign(&self, child: &Child) -> io::Result<()> {
        // SAFETY: both handles remain owned throughout this call. The child has
        // not been resumed. Nested-job restrictions fail closed here.
        if unsafe { AssignProcessToJobObject(self.0.as_raw_handle(), child.as_raw_handle()) } == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    fn terminate(&self) -> io::Result<()> {
        // SAFETY: the job handle remains live; termination also covers child jobs.
        if unsafe { TerminateJobObject(self.0.as_raw_handle(), 1) } == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    fn active_processes(&self) -> io::Result<u32> {
        let mut info = JOBOBJECT_BASIC_ACCOUNTING_INFORMATION::default();
        // SAFETY: the writable buffer has the requested information class's size.
        let ok = unsafe {
            QueryInformationJobObject(
                self.0.as_raw_handle(),
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
        Ok(info.ActiveProcesses)
    }
}

pub(super) struct Ownership {
    job: Job,
    child: Child,
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
        self.job.terminate()?;
        // TerminateJobObject initiates termination, but is not its acknowledgement.
        // Yield once before checking, also allowing cancellation at this boundary.
        tokio::task::yield_now().await;
        loop {
            if self.job.active_processes()? == 0 && self.child.try_wait()?.is_some() {
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
    let ownership = Ownership { job, child };
    ownership.job.assign(&ownership.child)?;
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
    use windows_sys::Win32::Foundation::{ERROR_ACCESS_DENIED, WAIT_OBJECT_0, WAIT_TIMEOUT};
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
                unsafe { AssignProcessToJobObject(job.0.as_raw_handle(), GetCurrentProcess()) },
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
            .env(FIXTURE_MODE, "conversation")
            .env(FIXTURE_ADDRESS, listener.local_addr().unwrap().to_string())
            .env("DIAGNOSTIC_FIXTURE_EXIT", exit.to_string())
            .env("DIAGNOSTIC_FIXTURE_VALUE", "value with spaces λ")
            .env("DIAGNOSTIC_FIXTURE_REMOVED", "discard")
            .env_remove("DIAGNOSTIC_FIXTURE_REMOVED")
            .args(["--skip", ARGUMENT])
            .current_dir(&cwd);
        let mut started = spawn(command).await.unwrap();
        let leader = Control::accept(&listener).await;
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
        assert_eq!(ownership.job.active_processes().unwrap(), 1);
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
        assert_eq!(started.ownership.job.active_processes().unwrap(), 1);
        let mut stdout = started.stdout.take().unwrap();
        let mut output = Vec::new();
        if mode == "tree_closed" {
            timeout(TEST_TIMEOUT, stdout.read_to_end(&mut output))
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
}
