//! Test-local Windows job containment. The exact-test child waits on stdin
//! before fixture creation; assign it to the job before releasing that gate.
use super::*;
use std::mem::size_of;
use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle};
use std::ptr::null;
use windows_sys::Win32::System::JobObjects::{
    AssignProcessToJobObject, CreateJobObjectW, JobObjectBasicAccountingInformation,
    JobObjectExtendedLimitInformation, QueryInformationJobObject, SetInformationJobObject,
    JOBOBJECT_BASIC_ACCOUNTING_INFORMATION, JOBOBJECT_EXTENDED_LIMIT_INFORMATION,
    JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
};

pub(super) struct Child {
    // raw-child: allow — held with a kill-on-close job and bounded exit polling
    process: std::process::Child,
    job: OwnedHandle,
}

impl Child {
    pub(super) fn spawn(command: &mut Command, _nested: bool) -> Self {
        // SAFETY: null attributes create an unnamed, noninheritable job.
        let raw = unsafe { CreateJobObjectW(null(), null()) };
        assert!(
            !raw.is_null(),
            "create child job: {}",
            std::io::Error::last_os_error()
        );
        // SAFETY: successful creation transfers unique handle ownership.
        let job = unsafe { OwnedHandle::from_raw_handle(raw) };
        let mut limits = JOBOBJECT_EXTENDED_LIMIT_INFORMATION::default();
        limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
        // SAFETY: live handle and buffer of the documented size.
        assert_ne!(
            unsafe {
                SetInformationJobObject(
                    job.as_raw_handle(),
                    JobObjectExtendedLimitInformation,
                    std::ptr::from_ref(&limits).cast(),
                    size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>()
                        .try_into()
                        .unwrap(),
                )
            },
            0,
            "configure child job: {}",
            std::io::Error::last_os_error()
        );
        let mut process = command.spawn().unwrap();
        // SAFETY: both handles remain owned and open. Nested jobs retain the
        // controller's outer job as well; no breakaway flag is enabled.
        if unsafe { AssignProcessToJobObject(job.as_raw_handle(), process.as_raw_handle()) } == 0 {
            let error = std::io::Error::last_os_error();
            process.kill().unwrap();
            process.wait().unwrap();
            panic!("contain child before fixture startup: {error}");
        }
        Self { process, job }
    }

    pub(super) fn start(&mut self) {
        self.process.stdin.take().unwrap().write_all(&[1]).unwrap();
    }

    pub(super) fn wait(&mut self, timeout: Duration) -> Option<ExitStatus> {
        let deadline = std::time::Instant::now() + timeout;
        loop {
            if let Some(status) = self.process.try_wait().unwrap() {
                return Some(status);
            }
            if std::time::Instant::now() >= deadline {
                return None;
            }
            // timing-guard: bounded process exit polling
            std::thread::sleep(Duration::from_millis(20));
        }
    }
    pub(super) fn cleanup(&mut self) -> bool {
        use windows_sys::Win32::System::JobObjects::TerminateJobObject;
        // SAFETY: live owned job; descendants cannot opt out of containment.
        if unsafe { TerminateJobObject(self.job.as_raw_handle(), 1) } == 0 {
            return false;
        }
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            let mut info = JOBOBJECT_BASIC_ACCOUNTING_INFORMATION::default();
            // SAFETY: live job and correctly sized output buffer.
            let ok = unsafe {
                QueryInformationJobObject(
                    self.job.as_raw_handle(),
                    JobObjectBasicAccountingInformation,
                    std::ptr::from_mut(&mut info).cast(),
                    size_of::<JOBOBJECT_BASIC_ACCOUNTING_INFORMATION>()
                        .try_into()
                        .unwrap(),
                    std::ptr::null_mut(),
                )
            };
            if ok == 0 {
                return false;
            }
            if info.ActiveProcesses == 0 && matches!(self.process.try_wait(), Ok(Some(_))) {
                return true;
            }
            if std::time::Instant::now() >= deadline {
                return false;
            }
            // timing-guard: bounded job termination accounting and leader reap
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}

impl Drop for Child {
    fn drop(&mut self) {
        use windows_sys::Win32::System::JobObjects::TerminateJobObject;
        // SAFETY: job handle is still owned. Kill descendants even after the
        // leader exits; closing the job is also a kill-on-close backstop.
        unsafe { TerminateJobObject(self.job.as_raw_handle(), 1) };
        let _ = self.process.try_wait();
    }
}
