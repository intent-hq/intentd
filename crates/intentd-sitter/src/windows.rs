//! Windows lifecycle primitives. Handles pin process identity across PID reuse;
//! private global events also work for supervisors launched in another session.
use std::io;
use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle};
use std::path::Path;
use windows_sys::Win32::Foundation::{
    GetLastError, ERROR_ALREADY_EXISTS, FILETIME, WAIT_OBJECT_0, WAIT_TIMEOUT,
};
use windows_sys::Win32::Security::Authorization::{
    ConvertStringSecurityDescriptorToSecurityDescriptorW, SDDL_REVISION_1,
};
use windows_sys::Win32::Security::SECURITY_ATTRIBUTES;
use windows_sys::Win32::System::Threading::{
    CreateEventW, GetCurrentProcess, GetProcessTimes, OpenEventW, OpenProcess, SetEvent,
    TerminateProcess, WaitForSingleObject, EVENT_MODIFY_STATE, PROCESS_QUERY_LIMITED_INFORMATION,
    PROCESS_SYNCHRONIZE, PROCESS_TERMINATE,
};

fn owned(handle: windows_sys::Win32::Foundation::HANDLE) -> io::Result<OwnedHandle> {
    if handle.is_null() {
        Err(io::Error::last_os_error())
    } else {
        // SAFETY: a successful Win32 creation/open transfers this unique handle.
        Ok(unsafe { OwnedHandle::from_raw_handle(handle) })
    }
}

/// A retained handle never refers to a later process that reuses this PID.
#[derive(Debug)]
pub struct Process(OwnedHandle);
impl Process {
    /// # Errors
    /// Returns the underlying Windows or filesystem error when the operation fails.
    pub fn open(pid: u32, terminate: bool) -> io::Result<Self> {
        let access = PROCESS_SYNCHRONIZE
            | PROCESS_QUERY_LIMITED_INFORMATION
            | if terminate { PROCESS_TERMINATE } else { 0 };
        // SAFETY: no pointers; non-inheritable handle.
        owned(unsafe { OpenProcess(access, 0, pid) }).map(Self)
    }
    /// # Errors
    /// Returns the underlying Windows or filesystem error when the operation fails.
    pub fn exited(&self) -> io::Result<bool> {
        signaled(&self.0)
    }
    /// # Errors
    /// Returns the underlying Windows or filesystem error when the operation fails.
    pub fn creation_time(&self) -> io::Result<u64> {
        let mut created = FILETIME::default();
        let mut exit = FILETIME::default();
        let mut kernel = FILETIME::default();
        let mut user = FILETIME::default();
        // SAFETY: all output buffers are writable; handle is retained.
        if unsafe {
            GetProcessTimes(
                self.0.as_raw_handle(),
                &raw mut created,
                &raw mut exit,
                &raw mut kernel,
                &raw mut user,
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        Ok((u64::from(created.dwHighDateTime) << 32) | u64::from(created.dwLowDateTime))
    }
    /// The daemon publishes its creation time beside the backwards-compatible
    /// numeric PID file. Missing legacy identity permits RPC, never force-kill.
    #[must_use]
    pub fn matches_record(&self, pid_path: &Path, pid: u32) -> bool {
        self.creation_time().is_ok_and(|time| {
            std::fs::read_to_string(pid_path.with_extension("identity"))
                .is_ok_and(|record| record == format!("{pid}:{time}"))
        })
    }
    /// # Errors
    /// Returns the underlying Windows or filesystem error when the operation fails.
    pub fn terminate(&self) -> io::Result<()> {
        // Exit zero prevents a supervising sitter from treating stop as a crash.
        // SAFETY: this is the retained, identity-checked process handle.
        if unsafe { TerminateProcess(self.0.as_raw_handle(), 0) } == 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }
}

/// # Errors
/// Returns the underlying Windows or filesystem error when the operation fails.
pub fn publish_identity(pid_path: &Path) -> io::Result<()> {
    let pid = std::process::id();
    let time = Process::open(pid, false)?.creation_time()?;
    std::fs::write(pid_path.with_extension("identity"), format!("{pid}:{time}"))
}

fn signaled(handle: &OwnedHandle) -> io::Result<bool> {
    // SAFETY: retained waitable handle, nonblocking wait.
    match unsafe { WaitForSingleObject(handle.as_raw_handle(), 0) } {
        WAIT_OBJECT_0 => Ok(true),
        WAIT_TIMEOUT => Ok(false),
        _ => Err(io::Error::last_os_error()),
    }
}

/// Kill the entire owned tree if the supervisor exits, including abrupt death.
/// Install before spawning anything. Nested jobs are supported on Windows 8+.
pub(crate) struct ProcessTree(OwnedHandle);
impl ProcessTree {
    pub(crate) fn contain_current_process() -> io::Result<Self> {
        use windows_sys::Win32::System::JobObjects::{
            AssignProcessToJobObject, CreateJobObjectW, JobObjectExtendedLimitInformation,
            SetInformationJobObject, JOBOBJECT_EXTENDED_LIMIT_INFORMATION,
            JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
        };
        // SAFETY: unnamed job with default security, owned handle.
        let job = owned(unsafe { CreateJobObjectW(std::ptr::null(), std::ptr::null()) })?;
        let mut limits = JOBOBJECT_EXTENDED_LIMIT_INFORMATION::default();
        limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
        // SAFETY: typed information buffer has the required layout and length.
        if unsafe {
            SetInformationJobObject(
                job.as_raw_handle(),
                JobObjectExtendedLimitInformation,
                (&raw const limits).cast(),
                u32::try_from(std::mem::size_of_val(&limits)).unwrap(),
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: assign only ourselves, before any descendants are launched.
        if unsafe { AssignProcessToJobObject(job.as_raw_handle(), GetCurrentProcess()) } == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Self(job))
    }
    /// Keep the job open until process teardown rather than dropping it while
    /// the supervisor still needs to return its exit status and remove records.
    pub(crate) fn retain_until_exit(self) {
        std::mem::forget(self.0);
    }
}

/// Owner-only events, named by PID and kernel creation time, not by PID alone.
/// The protected DACL grants access only to the actual user SID (not a group).
pub(crate) struct RestartControl {
    request: OwnedHandle,
    request_path: std::path::PathBuf,
    response_path: std::path::PathBuf,
    stop: OwnedHandle,
    owner: bool,
}
impl RestartControl {
    pub(crate) fn create(dir: &Path) -> io::Result<Self> {
        let pid = std::process::id();
        let time = Process::open(pid, false)?.creation_time()?;
        Ok(Self {
            owner: true,
            request: create_event(&event_name(pid, time, "request"))?,
            request_path: dir.join(format!("restart-{pid}-{time}.request")),
            response_path: dir.join(format!("restart-{pid}-{time}.response")),
            stop: create_event(&event_name(pid, time, "stop"))?,
        })
    }
    pub(crate) fn open(pid: u32, process: &Process, dir: &Path) -> io::Result<Self> {
        // Numeric liveness is only a duplicate-start guard. Before opening any
        // control event, bind this retained handle to this instance's saved
        // identity. A legacy/mismatched record must not target a reused PID.
        if !process.matches_record(&dir.join("sitter.pid"), pid) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "refusing to control unverified supervisor",
            ));
        }
        let time = process.creation_time()?;
        let open = |name: String| {
            let wide: Vec<u16> = name.encode_utf16().chain(Some(0)).collect();
            // SAFETY: nul-terminated name; non-inheritable handle.
            owned(unsafe { OpenEventW(EVENT_MODIFY_STATE | PROCESS_SYNCHRONIZE, 0, wide.as_ptr()) })
        };
        Ok(Self {
            owner: false,
            request: open(event_name(pid, time, "request"))?,
            request_path: dir.join(format!("restart-{pid}-{time}.request")),
            response_path: dir.join(format!("restart-{pid}-{time}.response")),
            stop: open(event_name(pid, time, "stop"))?,
        })
    }
    pub(crate) fn request_stop(&self) -> io::Result<()> {
        // SAFETY: retained event handle belonging to the discovered supervisor.
        if unsafe { SetEvent(self.stop.as_raw_handle()) } == 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }
    pub(crate) fn take_stop(&self) -> io::Result<bool> {
        signaled(&self.stop)
    }
    pub(crate) fn request(&self) -> io::Result<String> {
        // A request nonce prevents a timed-out caller's late completion from
        // acknowledging a newer restart. CLI callers hold start.lock.
        let pid = std::process::id();
        let nonce = format!(
            "{pid}:{}:{:?}",
            Process::open(pid, false)?.creation_time()?,
            std::time::SystemTime::now()
        );
        std::fs::write(&self.request_path, &nonce)?;
        // SAFETY: retained owner-restricted event handle.
        if unsafe { SetEvent(self.request.as_raw_handle()) } == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(nonce)
    }
    pub(crate) fn take_request(&self) -> io::Result<Option<String>> {
        if signaled(&self.request)? {
            std::fs::read_to_string(&self.request_path).map(Some)
        } else {
            Ok(None)
        }
    }
    pub(crate) fn completed(&self, nonce: &str) -> bool {
        std::fs::read_to_string(&self.response_path).is_ok_and(|response| response == nonce)
    }
    pub(crate) fn finish(&self, nonce: &str) -> io::Result<()> {
        std::fs::write(&self.response_path, nonce)
    }
}
impl Drop for RestartControl {
    fn drop(&mut self) {
        if self.owner {
            let _ = std::fs::remove_file(&self.request_path);
            let _ = std::fs::remove_file(&self.response_path);
        }
    }
}

fn event_name(pid: u32, time: u64, kind: &str) -> String {
    format!("Global\\intentd-sitter-{pid}-{time}-{kind}")
}
fn create_event(name: &str) -> io::Result<OwnedHandle> {
    let sddl: Vec<u16> = owner_sddl()?.encode_utf16().chain(Some(0)).collect();
    let wide: Vec<u16> = name.encode_utf16().chain(Some(0)).collect();
    let mut descriptor = std::ptr::null_mut();
    // SAFETY: nul-terminated SDDL; Win32 allocates descriptor, freed below.
    if unsafe {
        ConvertStringSecurityDescriptorToSecurityDescriptorW(
            sddl.as_ptr(),
            SDDL_REVISION_1,
            &raw mut descriptor,
            std::ptr::null_mut(),
        )
    } == 0
    {
        return Err(io::Error::last_os_error());
    }
    let security = SECURITY_ATTRIBUTES {
        nLength: u32::try_from(std::mem::size_of::<SECURITY_ATTRIBUTES>()).unwrap(),
        lpSecurityDescriptor: descriptor,
        bInheritHandle: 0,
    };
    // SAFETY: descriptor and name remain live during creation. Auto-reset event.
    let handle = unsafe { CreateEventW(&raw const security, 0, 0, wide.as_ptr()) };
    let error = unsafe { GetLastError() };
    // SAFETY: descriptor was allocated by the SDDL conversion above.
    unsafe {
        windows_sys::Win32::Foundation::LocalFree(descriptor);
    }
    let handle = owned(handle)?;
    if error == ERROR_ALREADY_EXISTS {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "private supervisor event already exists",
        ));
    }
    Ok(handle)
}

fn owner_sddl() -> io::Result<String> {
    use windows_sys::Win32::Security::Authorization::ConvertSidToStringSidW;
    use windows_sys::Win32::Security::{GetTokenInformation, TokenUser, TOKEN_QUERY, TOKEN_USER};
    use windows_sys::Win32::System::Threading::OpenProcessToken;
    let mut raw = std::ptr::null_mut();
    // SAFETY: current process token, returned handle is immediately owned.
    if unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &raw mut raw) } == 0 {
        return Err(io::Error::last_os_error());
    }
    let token = owned(raw)?;
    let mut size = 0;
    // SAFETY: first query obtains the required buffer length.
    unsafe {
        GetTokenInformation(
            token.as_raw_handle(),
            TokenUser,
            std::ptr::null_mut(),
            0,
            &raw mut size,
        );
    }
    let mut buffer = vec![0usize; (size as usize).div_ceil(std::mem::size_of::<usize>())];
    // SAFETY: aligned buffer is at least size bytes; token is retained.
    if unsafe {
        GetTokenInformation(
            token.as_raw_handle(),
            TokenUser,
            buffer.as_mut_ptr().cast(),
            size,
            &raw mut size,
        )
    } == 0
    {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: successful TokenUser query initialized TOKEN_USER and its SID.
    let sid = unsafe { (*buffer.as_ptr().cast::<TOKEN_USER>()).User.Sid };
    let mut text = std::ptr::null_mut();
    // SAFETY: SID remains in the buffer; Win32 allocates the string.
    if unsafe { ConvertSidToStringSidW(sid, &raw mut text) } == 0 {
        return Err(io::Error::last_os_error());
    }
    let mut len = 0;
    // SAFETY: conversion returned a nul-terminated UTF-16 string.
    while unsafe { *text.add(len) } != 0 {
        len += 1;
    }
    let sid = String::from_utf16_lossy(unsafe { std::slice::from_raw_parts(text, len) });
    // SAFETY: string was allocated by ConvertSidToStringSidW.
    unsafe {
        windows_sys::Win32::Foundation::LocalFree(text.cast());
    }
    Ok(format!("D:P(A;;GA;;;{sid})"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn process_identity_rejects_stale_records() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("daemon.pid");
        let pid = std::process::id();
        let process = Process::open(pid, false).unwrap();
        assert!(!process.exited().unwrap());
        assert!(!process.matches_record(&path, pid));
        publish_identity(&path).unwrap();
        assert!(process.matches_record(&path, pid));
        std::fs::write(path.with_extension("identity"), format!("{pid}:0")).unwrap();
        assert!(!process.matches_record(&path, pid));
    }

    #[test]
    fn restart_events_reject_foreign_identity_and_require_explicit_completion() {
        let dir = tempfile::tempdir().unwrap();
        let control = RestartControl::create(dir.path()).unwrap();
        let process = Process::open(std::process::id(), false).unwrap();
        let foreign = tempfile::tempdir().unwrap();
        let foreign_pid = foreign.path().join("sitter.pid");
        let pid = std::process::id();
        std::fs::write(&foreign_pid, pid.to_string()).unwrap();
        assert!(RestartControl::open(pid, &process, foreign.path()).is_err());
        std::fs::write(foreign_pid.with_extension("identity"), format!("{pid}:0")).unwrap();
        assert!(RestartControl::open(pid, &process, foreign.path()).is_err());
        assert!(!control.take_stop().unwrap());
        assert_eq!(control.take_request().unwrap(), None);
        publish_identity(&dir.path().join("sitter.pid")).unwrap();
        let client = RestartControl::open(std::process::id(), &process, dir.path()).unwrap();
        client.request_stop().unwrap();
        assert!(control.take_stop().unwrap());
        assert!(!control.take_stop().unwrap());
        let first = client.request().unwrap();
        assert_eq!(control.take_request().unwrap(), Some(first.clone()));
        assert_eq!(control.take_request().unwrap(), None);
        assert!(!client.completed(&first));
        let second = client.request().unwrap();
        control.finish(&first).unwrap();
        assert!(
            !client.completed(&second),
            "late completion cannot acknowledge another request"
        );
        assert_eq!(control.take_request().unwrap(), Some(second.clone()));
        control.finish(&second).unwrap();
        assert!(client.completed(&second));
        // Reused PID with a different kernel creation time names another object.
        assert_ne!(event_name(10, 1, "request"), event_name(10, 2, "request"));
    }
    struct TestChild(std::process::Child);
    impl Drop for TestChild {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    #[test]
    fn wait_fixture() {
        if std::env::var_os("INTENTD_JOB_FIXTURE").is_some() {
            std::thread::park_timeout(std::time::Duration::from_secs(60));
        }
    }
    #[test]
    fn process_tree_fixture() {
        let Some(path) = std::env::var_os("INTENTD_JOB_FIXTURE") else {
            return;
        };
        ProcessTree::contain_current_process()
            .unwrap()
            .retain_until_exit();
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "windows::tests::wait_fixture"])
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();
        std::fs::write(path, child.id().to_string()).unwrap();
        let _ = child.wait();
    }
    #[test]
    fn supervisor_death_terminates_owned_descendants_only() {
        use std::time::{Duration, Instant};
        let dir = tempfile::tempdir().unwrap();
        let record = dir.path().join("descendant.pid");
        let child = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "windows::tests::process_tree_fixture"])
            .env("INTENTD_JOB_FIXTURE", &record)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();
        let mut supervisor = TestChild(child);
        let deadline = Instant::now() + Duration::from_secs(10);
        let pid = loop {
            if let Ok(text) = std::fs::read_to_string(&record) {
                if let Ok(pid) = text.parse::<u32>() {
                    break pid;
                }
            }
            assert!(
                supervisor.0.try_wait().unwrap().is_none(),
                "job fixture exited before publication"
            );
            assert!(
                Instant::now() < deadline,
                "job fixture did not publish child"
            );
            std::thread::sleep(Duration::from_millis(20));
        };
        let descendant = Process::open(pid, false).unwrap();
        supervisor.0.kill().unwrap();
        supervisor.0.wait().unwrap();
        while !descendant.exited().unwrap() {
            assert!(
                Instant::now() < deadline,
                "job leaked a descendant after supervisor death"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
        assert!(!Process::open(std::process::id(), false)
            .unwrap()
            .exited()
            .unwrap());
    }
}
