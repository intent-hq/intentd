//! Linux, explicit-feature receipts for the fixture's ordinary graceful path.
//! The pending wait is direct-daemon exit readiness, not a production admission
//! fence. Executed direct-kill v2 source and RED evidence are preserved separately.

use std::collections::HashMap;
use std::ffi::CString;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::os::unix::net::UnixDatagram;
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::ExitStatus;
use std::cell::RefCell;
use std::rc::Rc;
use std::time::{Duration, Instant};

use serde_json::{json, Value};

const CONFIG_ENV: &str = "INTENT_TEST_PROBE_OBSERVER";
const RECEIPT_LIMIT: usize = 16 * 1024;
const CONTROL_BOUND: Duration = Duration::from_secs(5);

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn identity(pid: u32) -> io::Result<Value> {
    let read = || fs::read_to_string(format!("/proc/{pid}/stat"));
    let before = read()?;
    let fields: Vec<_> = before.rsplit_once(") ").ok_or_else(|| invalid("stat"))?.1
        .split_whitespace().collect();
    if fields.len() < 20 || matches!(fields[0], "Z" | "X") {
        return Err(invalid("no live identity"));
    }
    let number = |i: usize| fields[i].parse::<u64>().map_err(|_| invalid("stat number"));
    let status = fs::read_to_string(format!("/proc/{pid}/status"))?;
    let tgid = status.lines().find_map(|line| line.strip_prefix("Tgid:"))
        .ok_or_else(|| invalid("TGID"))?.trim().parse::<u32>().map_err(|_| invalid("TGID"))?;
    let executable = fs::read_link(format!("/proc/{pid}/exe"))?;
    let after = read()?;
    let fresh: Vec<_> = after.rsplit_once(") ").ok_or_else(|| invalid("fresh stat"))?.1
        .split_whitespace().collect();
    if fresh.len() < 20 || fresh[19] != fields[19] || fresh[1..4] != fields[1..4]
        || matches!(fresh[0], "Z" | "X")
    {
        return Err(invalid("identity changed"));
    }
    Ok(json!({"pid": pid, "start": number(19)?, "tgid": tgid,
        "ppid": number(1)?, "pgid": number(2)?, "sid": number(3)?, "executable": executable}))
}

fn matches_identity(expected: &Value, observed: &Value) -> bool {
    ["pid", "start", "tgid", "ppid", "pgid", "sid", "executable"]
        .iter().all(|key| expected[*key] == observed[*key])
}

fn file_identity(metadata: &fs::Metadata) -> Value {
    json!({"device": metadata.dev(), "inode": metadata.ino(), "size": metadata.len(),
        "mtimeNs": metadata.mtime() * 1_000_000_000 + metadata.mtime_nsec(),
        "ctimeNs": metadata.ctime() * 1_000_000_000 + metadata.ctime_nsec(),
        "mode": metadata.mode(), "uid": metadata.uid()})
}

fn live_daemon_executable(owner: &Value) -> io::Result<Value> {
    let pid = owner["pid"].as_u64().and_then(|pid| u32::try_from(pid).ok())
        .ok_or_else(|| invalid("missing daemon PID"))?;
    let target = std::env::var_os("CARGO_TARGET_DIR")
        .ok_or_else(|| invalid("missing frozen target directory"))?;
    let path = PathBuf::from(target).join("debug/intentd");
    let before = fs::symlink_metadata(&path)?;
    if !before.is_file() { return Err(invalid("daemon executable is not a regular file")) }
    let canonical = fs::canonicalize(&path)?;
    let proc_path = PathBuf::from(format!("/proc/{pid}/exe"));
    let executable = fs::read_link(&proc_path)?;
    let live_file = file_identity(&fs::metadata(&proc_path)?);
    let expected_file = file_identity(&before);
    if executable != canonical || live_file != expected_file
        || file_identity(&fs::symlink_metadata(&path)?) != expected_file
        || fs::canonicalize(&path)? != canonical
        || !matches_identity(owner, &identity(pid)?)
        || fs::read_link(&proc_path)? != executable
    { return Err(invalid("daemon executable identity changed while live")) }
    // Only cheap live metadata here. The supervisor hashes this exact file
    // after primary settlement, checking this identity before and after hashing.
    Ok(json!({"pid": pid, "start": owner["start"], "canonicalPath": canonical,
        "procExecutable": executable, "fileIdentity": live_file}))
}

#[derive(Clone)]
pub(super) struct TeardownObservation(Rc<RefCell<State>>);

struct State {
    owner: Value,
    probe: Value,
    held: bool,
    owner_reaped: bool,
    failure: Option<String>,
    daemon_waited: bool,
    expected_termination: bool,
    events: Vec<Value>,
    provider: Option<Provider>,
    provider_finalization: Value,
    completion: Value,
    pending: bool,
    terminal: Value,
}

impl TeardownObservation {
    fn change(&self, action: impl FnOnce(&mut State)) {
        // A single-thread inline controller cannot block on a mutex or join.
        // Reentrancy is a caught fixture panic, never an unbounded lock wait.
        let mut state = self.0.borrow_mut();
        action(&mut state);
    }

    pub(super) fn entered(&self, daemon_pid: u32) {
        self.change(|state| {
            if state.owner["pid"] != daemon_pid {
                state.failure = Some("teardown owner mismatch".into());
            }
            state.events.push(json!({"event": "TeardownEntered", "pid": daemon_pid}));
        });
    }

    pub(super) fn kill_attempt(&self, daemon_pid: u32) {
        self.change(|state| {
            if state.held {
                let probe_pid = state.probe["pid"].as_u64().and_then(|pid| u32::try_from(pid).ok());
                let live = probe_pid.and_then(|pid| identity(pid).ok());
                state.failure.get_or_insert_with(|| if live.as_ref().is_some_and(|row| matches_identity(&state.probe, row)) {
                    "KillWhileProbeHeld".into()
                } else {
                    "held-probe identity unavailable before kill; precondition failed".into()
                });
            } else if !state.owner_reaped {
                state.failure.get_or_insert_with(|| "kill without a normal owner-wait receipt".into());
            }
            // This records the actual next operation. It neither blocks it nor
            // releases the provider or supplies a settlement acknowledgement.
            state.events.push(json!({"event": "KillAttempt", "pid": daemon_pid,
                "held": state.held, "latchedFailure": state.failure}));
        });
    }

    pub(super) fn kill_result(&self, result: &io::Result<()>) {
        self.change(|state| {
            state.events.push(json!({"event": "KillResult", "ok": result.is_ok(),
                "error": result.as_ref().err().map(ToString::to_string)}));
            if result.is_err() { state.failure.get_or_insert_with(|| "daemon kill failed".into()); }
        });
    }

    pub(super) fn wait_result(&self, result: &io::Result<ExitStatus>) {
        self.change(|state| {
            state.daemon_waited = result.is_ok();
            state.expected_termination = result.as_ref().is_ok_and(|status| {
                status.signal() == Some(libc::SIGKILL) && status.code().is_none()
                    && !status.core_dumped()
            });
            state.events.push(json!({"event": "DaemonWaitResult", "ok": result.is_ok(),
                "rawStatus": result.as_ref().ok().map(|status| status.into_raw()),
                "code": result.as_ref().ok().and_then(ExitStatus::code),
                "signal": result.as_ref().ok().and_then(ExitStatusExt::signal),
                "coreDumped": result.as_ref().ok().map(ExitStatusExt::core_dumped),
                "expectedSigkill": state.expected_termination,
                "error": result.as_ref().err().map(ToString::to_string)}));
            if result.is_err() {
                state.failure = Some("daemon wait failed".into());
            } else if !state.expected_termination {
                state.failure = Some("unexpected daemon termination status".into());
            }
        });
    }

    fn snapshot(&self) -> Value {
        let mut snapshot = Value::Null;
        self.change(|state| snapshot = json!({"owner": state.owner, "probe": state.probe,
            "held": state.held, "ownerReaped": state.owner_reaped,
            "failure": state.failure, "daemonWaited": state.daemon_waited,
            "expectedTermination": state.expected_termination, "events": state.events,
            "pendingDaemonExitWait": state.pending, "terminal": state.terminal,
            "observer": state.provider.as_ref().map(|provider| &provider.records),
            "providerFinalization": state.provider_finalization, "completion": state.completion}));
        snapshot
    }

    pub(super) fn failed(&self, message: &str) {
        self.change(|state| { state.failure.get_or_insert_with(|| message.to_owned()); });
    }

    pub(super) fn graceful_entered(&self, pid: u32) -> io::Result<()> {
        self.entered(pid);
        let live = identity(pid)?;
        let mut valid = false;
        self.change(|state| valid = state.failure.is_none() && matches_identity(&state.owner, &live));
        if !valid { return Err(invalid("cached live daemon binding changed before shutdown")) }
        Ok(())
    }

    pub(super) fn wait_pending(&self, pid: u32, deadline: Instant) -> io::Result<()> {
        super::daemon_exit::remaining(deadline)?;
        let mut state = self.0.try_borrow_mut().map_err(|_| invalid("reentrant inline release"))?;
        if state.owner["pid"] != pid || state.pending || !state.held || state.failure.is_some() {
            return Err(invalid("missing or repeated held-provider wait registration"));
        }
        state.pending = true;
        state.events.push(json!({"event":"DaemonExitWaitPending", "pid":pid,
            "scope":"fixture daemon exit; not a production admission fence"}));
        // This call is made only at the registered wait's actual first Pending
        // result, after qualified shutdown. One nonblocking FIFO write, no thread.
        state.provider.as_mut().ok_or_else(|| invalid("no bound provider receiver"))?.release()?;
        state.held = false;
        state.events.push(json!({"event":"ProviderReleasedAfterPending", "pid":pid}));
        super::daemon_exit::remaining(deadline)?;
        Ok(())
    }

    pub(super) fn before_reap(&self, pid: u32, deadline: Instant) -> io::Result<()> {
        super::daemon_exit::remaining(deadline)?;
        let mut owner = Value::Null;
        let mut valid = false;
        self.change(|state| {
            owner = state.owner.clone();
            valid = state.pending && !state.held && state.failure.is_none() && state.owner["pid"] == pid;
        });
        if !valid { return Err(invalid("missing release/pending-wait or failed controller")) }
        // Called only after registered exit readiness, with Child still exclusively
        // borrowed and unreaped by the adapter. No PID reuse is possible here.
        // The emitter proves live owner/creator/candidate at send time after its
        // actual successful wait. Missing live /proc data is NOT used as a pass.
        self.0.try_borrow_mut().map_err(|_| invalid("reentrant provider receipt"))?
            .provider.as_mut().ok_or_else(|| invalid("no bound provider receiver"))?
            .owner_settled_unreaped(pid, &owner)?;
        super::daemon_exit::remaining(deadline)?;
        self.change(|state| {
            state.owner_reaped = true;
            state.events.push(json!({"event":"OwnerReceiptsAcceptedUnreaped", "pid":pid,
                "proof":"send-time stable owner plus cached live binding and unreaped direct child"}));
        });
        Ok(())
    }

    pub(super) fn normal_wait_result(&self, result: &io::Result<ExitStatus>) {
        self.change(|state| {
            state.daemon_waited = result.is_ok();
            state.expected_termination = result.as_ref().is_ok_and(super::daemon_exit::normal);
            state.events.push(super::daemon_exit::wait_record(result));
            if !state.expected_termination { state.failure.get_or_insert_with(|| "unexpected normal daemon wait result".into()); }
        });
    }

    pub(super) fn cleanup_wait_result(&self, result: &io::Result<ExitStatus>) {
        self.change(|state| {
            state.daemon_waited = result.is_ok();
            let mut row = super::daemon_exit::wait_record(result);
            row["failureCleanup"] = json!(true);
            state.events.push(row);
            state.failure.get_or_insert_with(|| "forced cleanup cannot satisfy graceful teardown".into());
        });
    }

    pub(super) fn after_reap(&self, pid: u32, deadline: Instant) -> io::Result<()> {
        super::daemon_exit::remaining(deadline)?;
        let mut confirmed = false;
        self.change(|state| confirmed = state.daemon_waited);
        let terminal = self.0.try_borrow_mut().map_err(|_| invalid("reentrant terminal receipt"))?
            .provider.as_mut().ok_or_else(|| invalid("no bound provider receiver"))?
            .terminal_check(pid, confirmed);
        self.change(|state| state.terminal = terminal.clone());
        if terminal["empty"] != true { return Err(invalid("nonempty or invalid terminal observer queue")) }
        super::daemon_exit::remaining(deadline)?;
        Ok(())
    }
    pub(super) fn finalize_provider(&self, failed: bool, deadline: Instant) -> Value {
        let mut state = self.0.borrow_mut();
        let failed = failed || state.failure.is_some();
        let terminal = state.terminal.clone();
        let release_after_death = failed && state.daemon_waited && state.held;
        let row = match state.provider.as_mut() {
            Some(provider) => {
                let release = if release_after_death { provider.release() } else { Ok(()) };
                provider.finalize(failed, deadline, &terminal, &release)
            }
            None => json!({"complete":false,"error":"no bound provider resources"}),
        };
        if row["complete"] != true { state.failure.get_or_insert_with(|| "provider finalization failed".into()); }
        state.provider_finalization = row.clone();
        row
    }

    pub(super) fn completed(&self, completion: &Value) {
        self.change(|state| state.completion = completion.clone());
    }

}

struct Provider {
    root: super::daemon_exit::FixtureDir,
    executable: PathBuf,
    home: PathBuf,
    socket_path: PathBuf,
    socket: Option<UnixDatagram>,
    started: Option<File>,
    release: Option<File>,
    nonce: String,
    records: Vec<Value>,
    last_receipt: Option<Value>,
}

fn quote(path: &Path) -> String {
    format!("'{}'", path.to_str().expect("private UTF-8 path").replace('\'', "'\\''"))
}

fn fifo(path: &Path) -> io::Result<File> {
    let path_c = CString::new(path.as_os_str().as_bytes())?;
    // SAFETY: exclusive test-owned path; no existing node is replaced.
    if unsafe { libc::mkfifo(path_c.as_ptr(), 0o600) } != 0 { return Err(io::Error::last_os_error()) }
    OpenOptions::new().read(true).write(true).custom_flags(libc::O_NONBLOCK).open(path)
}

impl Provider {
    fn new() -> Self {
        let root = super::common::test_tempdir("itd-woc-probe-");
        let nonce = uuid::Uuid::new_v4().to_string();
        let started_path = root.path().join("started");
        let release_path = root.path().join("release");
        let started = fifo(&started_path).expect("start FIFO");
        let release = fifo(&release_path).expect("release FIFO");
        let executable = root.path().join("private-auggie");
        fs::write(&executable, format!(
            "#!/bin/sh\n[ \"$#\" -eq 1 ] && [ \"$1\" = --version ] || exit 42\nprintf '%s %s %s\\n' '{nonce}' \"$$\" \"${{INTENT_TEST_PROBE_OBSERVER+x}}\" > {}\nread -r release < {}\n[ \"$release\" = '{nonce}' ] || exit 44\nprintf '0.1.0\\n'\n",
            quote(&started_path), quote(&release_path),
        )).expect("private provider");
        fs::set_permissions(&executable, fs::Permissions::from_mode(0o700)).unwrap();
        let home = root.path().join("home");
        fs::create_dir(&home).unwrap();
        let socket_path = root.path().join("owner.sock");
        let socket = UnixDatagram::bind(&socket_path).expect("owner observation socket");
        fs::set_permissions(&socket_path, fs::Permissions::from_mode(0o600)).unwrap();
        socket.set_read_timeout(Some(CONTROL_BOUND)).unwrap();
        let enabled: libc::c_int = 1;
        // SAFETY: valid socket and correctly sized immutable socket option.
        assert_eq!(unsafe { libc::setsockopt(socket.as_raw_fd(), libc::SOL_SOCKET, libc::SO_PASSCRED,
            std::ptr::from_ref(&enabled).cast(), std::mem::size_of_val(&enabled) as libc::socklen_t) }, 0);
        Self { root: root.into(), executable, home, socket_path, socket: Some(socket), started: Some(started), release: Some(release),
            nonce, records: Vec::new(), last_receipt: None }
    }

    fn config(&self) -> String {
        json!({"socket": self.socket_path, "executable": self.executable, "fixture_nonce": self.nonce}).to_string()
    }

    fn receive(&mut self, daemon_pid: u32, nonblocking: bool) -> io::Result<Value> {
        self.last_receipt = None;
        let mut bytes = [0_u8; RECEIPT_LIMIT];
        // usize gives the ancillary buffer cmsghdr alignment.
        let mut control = [0_usize; 8];
        let mut iov = libc::iovec { iov_base: bytes.as_mut_ptr().cast(), iov_len: bytes.len() };
        // SAFETY: zero is a valid empty msghdr; buffers remain alive through recvmsg.
        let mut message: libc::msghdr = unsafe { std::mem::zeroed() };
        message.msg_iov = &mut iov;
        message.msg_iovlen = 1;
        message.msg_control = control.as_mut_ptr().cast();
        message.msg_controllen = std::mem::size_of_val(&control);
        let flags = if nonblocking { libc::MSG_DONTWAIT } else { 0 };
        // SAFETY: all writable ranges are valid and bounded above.
        let count = unsafe { libc::recvmsg(self.socket.as_ref().ok_or_else(|| invalid("closed receipt socket"))?.as_raw_fd(), &mut message, flags) };
        if count < 0 { return Err(io::Error::last_os_error()) }
        self.last_receipt = Some(json!({"bytesReturned": count, "messageFlags": message.msg_flags,
            "payloadBytes": &bytes[..(count as usize).min(bytes.len())],
            "ancillaryBytes": &control[..], "ancillaryLength": message.msg_controllen}));
        if message.msg_flags & (libc::MSG_TRUNC | libc::MSG_CTRUNC) != 0 {
            return Err(invalid("truncated ownership receipt"));
        }
        // SAFETY: libc validates ancillary length; header/data are checked before reading.
        let credentials = unsafe {
            let header = libc::CMSG_FIRSTHDR(&message);
            if header.is_null() || (*header).cmsg_level != libc::SOL_SOCKET
                || (*header).cmsg_type != libc::SCM_CREDENTIALS
                || (*header).cmsg_len != libc::CMSG_LEN(std::mem::size_of::<libc::ucred>() as u32) as usize
                || !libc::CMSG_NXTHDR(&message, header).is_null()
            { return Err(invalid("missing or ambiguous sender credentials")) }
            std::ptr::read_unaligned(libc::CMSG_DATA(header).cast::<libc::ucred>())
        };
        self.last_receipt.as_mut().unwrap()["credentials"] = json!({
            "pid": credentials.pid, "uid": credentials.uid, "gid": credentials.gid});
        // SAFETY: geteuid has no arguments or borrowed state.
        if credentials.pid != daemon_pid as i32 || credentials.uid != unsafe { libc::geteuid() } {
            return Err(invalid("receipt was not sent by the selected daemon"));
        }
        let record: Value = serde_json::from_slice(&bytes[..count as usize]).map_err(io::Error::other)?;
        if record["fixtureNonce"] != self.nonce || record["owner"]["pid"] != daemon_pid
            || record["feature"] != "probe-wait-observer" || record["platform"] != "linux"
            || record["sequence"] != self.records.len() + 1
        { return Err(invalid("receipt binding or sequence mismatch")) }
        if let Some(first) = self.records.first() {
            for key in ["invocation", "owner", "creator", "candidate", "child"] {
                if record[key] != first[key] { return Err(invalid("invocation identity changed")) }
            }
        }
        self.records.push(record.clone());
        if self.records.len() > 16 { return Err(invalid("too many observer events")) }
        Ok(record)
    }

    fn terminal_check(&mut self, daemon_pid: u32, confirmed_dead: bool) -> Value {
        // Exactly one nonblocking receive can prove an empty final queue.
        // Any extra packet fails immediately; its bounded raw bytes/error are
        // retained, and an unexamined tail is explicitly not an empty queue.
        let result = if !confirmed_dead {
            json!({"checked": false, "empty": false,
                "error": "daemon death was not confirmed", "extra": null, "rawReceipt": null})
        } else {
            match self.receive(daemon_pid, true) {
                Err(error) if error.kind() == io::ErrorKind::WouldBlock =>
                    json!({"checked": true, "empty": true, "error": null,
                        "extra": null, "rawReceipt": null}),
                Ok(extra) => json!({"checked": true, "empty": false,
                    "error": "unexpected extra receipt after daemon wait", "extra": extra,
                    "rawReceipt": self.last_receipt, "unexaminedTail": true}),
                Err(error) => json!({"checked": true, "empty": false,
                    "error": error.to_string(), "extra": null,
                    "rawReceipt": self.last_receipt, "unexaminedTail": true}),
            }
        };
        result
    }

    fn held_start(&mut self, daemon_pid: u32) -> Value {
        let record = self.receive(daemon_pid, false).expect("daemon ProbeStarted receipt");
        assert_eq!(record["event"], "ProbeStarted", "{record}");
        let mut poll = libc::pollfd { fd: self.started.as_ref().expect("owned start FIFO").as_raw_fd(), events: libc::POLLIN, revents: 0 };
        // SAFETY: one valid pollfd; timeout bounds a readiness wait, not a sleep.
        assert_eq!(unsafe { libc::poll(&mut poll, 1, 5000) }, 1, "provider start deadline");
        let mut buffer = [0_u8; 256];
        let count = self.started.as_mut().expect("owned start FIFO").read(&mut buffer).expect("provider start message");
        let text = std::str::from_utf8(&buffer[..count]).unwrap();
        let expected = format!("{} {} \n", self.nonce, record["child"]["pid"].as_u64().unwrap());
        assert_eq!(text, expected, "provider identity/channel-inheritance mismatch");
        for key in ["owner", "creator", "child"] {
            let pid = u32::try_from(record[key]["pid"].as_u64().unwrap()).unwrap();
            let live = identity(pid).expect("required live identity");
            assert!(matches_identity(&record[key], &live), "{key} identity changed");
        }
        assert_eq!(record["child"]["ppid"], daemon_pid);
        assert_eq!(record["creator"]["tgid"], daemon_pid);
        assert_eq!(record["candidate"]["path"], self.executable.to_str().unwrap());
        record
    }

    fn release(&mut self) -> io::Result<()> {
        let bytes = format!("{}\n", self.nonce);
        // The short nonce fits PIPE_BUF. This one O_NONBLOCK write cannot
        // hide an unbounded write_all/EINTR loop inside the teardown deadline.
        let count = self.release.as_mut().ok_or_else(|| invalid("closed release FIFO"))?
            .write(bytes.as_bytes())?;
        if count != bytes.len() { return Err(invalid("partial provider release")) }
        Ok(())
    }

    fn owner_settled_unreaped(&mut self, daemon_pid: u32, cached_owner: &Value) -> io::Result<()> {
        if self.records.first().map(|row| &row["owner"]) != Some(cached_owner)
            || cached_owner["pid"] != daemon_pid
        { return Err(invalid("missing cached live owner binding")) }
        // The daemon has exited but remains our unreaped direct child. Every
        // expected send therefore already happened; no blocking receive or new
        // receiver-side liveness claim is needed. receive retains raw failures.
        let waited = self.receive(daemon_pid, true)?;
        if waited["event"] != "OwnerReaped" || waited["detail"]["cause"] != "normal"
            || waited["detail"]["normalSuccess"] != true || waited["detail"]["rawStatus"] != 0
        { return Err(invalid("no authentic normal owner-wait receipt")) }
        let returned = self.receive(daemon_pid, true)?;
        if returned["event"] != "ProbeReturned" || returned["detail"]["observationFailed"] != false
            || returned["detail"]["timedOut"] != false || returned["detail"]["ownerWaitObserved"] != true
        { return Err(invalid("incomplete or failed probe-return receipt")) }
        Ok(())
    }

    fn finalize(&mut self, failed: bool, deadline: Instant, terminal: &Value, release: &io::Result<()>) -> Value {
        let permitted = super::daemon_exit::remaining(deadline).is_ok();
        let failed = failed || !permitted;
        let mut row = json!({"complete":false,"failureCleanup":failed,"terminalWrite":null,
            "failureRelease": {"ok":release.is_ok(),"error":release.as_ref().err().map(ToString::to_string)},
            "descriptors":[],"directory":null,"error":null});
        // Synchronous writes/removal are measured at the semantic completion
        // boundary; they have no hard-preemption guarantee. Failures retain roots.
        let write = if failed { Ok(()) } else {
            serde_json::to_vec_pretty(terminal).map_err(io::Error::other)
                .and_then(|bytes| fs::write(self.root.path().join("terminal-observation.json"), bytes))
        };
        row["terminalWrite"] = json!({"attempted":!failed,"ok":!failed && write.is_ok(),
            "error":write.as_ref().err().map(ToString::to_string)});
        let mut closed = Vec::new();
        if let Some(fd) = self.socket.take() { closed.push(super::daemon_exit::close_descriptor("observer-socket", fd)); }
        if let Some(fd) = self.started.take() { closed.push(super::daemon_exit::close_descriptor("started-fifo", fd)); }
        if let Some(fd) = self.release.take() { closed.push(super::daemon_exit::close_descriptor("release-fifo", fd)); }
        let close_ok = closed.len() == 3 && closed.iter().all(|row| row["ok"] == true);
        row["descriptors"] = json!(closed);
        let failed = failed || write.is_err() || !close_ok || super::daemon_exit::remaining(deadline).is_err();
        row["directory"] = self.root.finalize(failed);
        row["complete"] = json!(!failed && super::daemon_exit::directory_complete(&row["directory"]));
        if failed {
            let evidence = json!({"kind":"held-probe teardown failure", "observer":self.records,
                "rawLastReceipt":self.last_receipt,"terminal":terminal,"finalization":row,
                "ownerSettlementAccepted":false,"probeWaitedByTest":false});
            let write = row["directory"]["retained"].as_str()
                .ok_or_else(|| invalid("provider failure directory unavailable"))
                .and_then(|path| serde_json::to_vec_pretty(&evidence).map_err(io::Error::other)
                    .and_then(|bytes| fs::write(Path::new(path).join("failure.json"), bytes)));
            row["failureEvidenceWrite"] = json!({"ok":write.is_ok(),"error":write.err().map(|e|e.to_string())});
        }
        row
    }
}

impl Drop for Provider {
    fn drop(&mut self) {
        // Only a failed/incomplete setup can reach this without explicit closure.
        // Preserve its roots. No implicit removal may later turn it into success.
        if self.socket.is_some() || self.started.is_some() || self.release.is_some() {
            self.root.finalize(true);
        }
    }
}

async fn boot(provider: &Provider) -> (super::Daemon, String, String, u16, String) {
    // A fresh HOME owns all home-based candidates. Fail before boot if the
    // inherited/enriched executable search could probe another Auggie binary.
    for directory in intent_core::path_utils::enhanced_path_dirs_with_home(Some(&provider.home)) {
        match fs::symlink_metadata(directory.join("auggie")) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => panic!("cannot exclude another provider candidate: {error}"),
            Ok(_) => panic!("unexpected auggie candidate under {}", directory.display()),
        }
    }
    let data_dir: super::daemon_exit::FixtureDir = super::temp_data_dir().into();
    let (workspace, task) = super::seed_workspace_and_task(data_dir.path(), "Probe ownership").await;
    let path = provider.executable.to_str().expect("provider path");
    assert!(path.is_ascii(), "frozen fixture requires an ASCII private path");
    fs::write(data_dir.path().join("config.toml"), format!(
        "[model]\ndefaultProvider = \"auggie\"\n[providers.paths]\nauggie = {}\n",
        serde_json::to_string(path).unwrap(),
    )).unwrap();
    let mut command = super::serve_command(data_dir.path(), "both", &[("INTENTD_AUTH_TOKEN", super::TOKEN)]);
    command.env(CONFIG_ENV, provider.config()).env("HOME", &provider.home);
    for key in ["XDG_CONFIG_HOME", "XDG_CACHE_HOME", "XDG_DATA_HOME", "XDG_STATE_HOME"] {
        let path = provider.home.join(key);
        fs::create_dir(&path).unwrap();
        command.env(key, path);
    }
    let daemon = super::Daemon { child: command.spawn().expect("observed daemon"), data_dir,
        probe_observation: None };
    let socket = daemon.data_dir.path().join("intentd.sock");
    assert!(super::await_uds(&socket).await, "daemon startup deadline");
    let status = super::common::await_wss_status(&socket).await;
    let port = u16::try_from(status["result"]["port"].as_u64().unwrap()).unwrap();
    let fingerprint = status["result"]["fingerprint"].as_str().unwrap().to_owned();
    (daemon, workspace, task, port, fingerprint)
}

fn verify_fixture_roots(daemon: &super::Daemon) -> Value {
    let bytes = fs::read(format!("/proc/{}/environ", daemon.child.id())).expect("owned daemon environment");
    let variables: HashMap<_, _> = bytes.split(|byte| *byte == 0)
        .filter_map(|entry| entry.iter().position(|byte| *byte == b'=').map(|at| (&entry[..at], &entry[at + 1..])))
        .collect();
    for key in ["GH_TOKEN", "GITHUB_TOKEN", "GH_HOST", "GH_ENTERPRISE_TOKEN", "GITHUB_ENTERPRISE_TOKEN"] {
        assert!(!variables.contains_key(key.as_bytes()), "fixture inherited {key}");
    }
    for (key, path) in [
        ("INTENTD_DATA_DIR", daemon.data_dir.path().to_owned()),
        ("INTENTD_CONFIG", daemon.data_dir.path().join("config.toml")),
        ("INTENTD_SECRETS_FILE", daemon.data_dir.path().join("secrets.json")),
        ("INTENTD_WORKSPACES_DIR", daemon.data_dir.path().join("workspaces")),
        ("GH_CONFIG_DIR", daemon.data_dir.path().join("gh-config")),
    ] {
        assert_eq!(variables.get(key.as_bytes()).copied(), Some(path.as_os_str().as_bytes()));
    }
    assert_eq!(variables.get(b"INTENTD_ASSERT_HERMETIC_ROOT".as_slice()).copied(), Some(b"1".as_slice()));
    assert_eq!(variables.get(b"INTENTD_TCP_PORT".as_slice()).copied(), Some(b"0".as_slice()));
    assert_eq!(fs::read_dir(daemon.data_dir.path().join("gh-config")).unwrap().count(), 0);
    let owner = identity(daemon.child.id()).unwrap();
    let executable = live_daemon_executable(&owner).expect("live daemon executable binding");
    json!({"path": daemon.data_dir.path(), "identity": owner, "daemonExecutable": executable,
        "identityEnvironmentVerified": true, "channelFeature": "probe-wait-observer"})
}

async fn started(provider: &mut Provider) -> (super::Daemon, TeardownObservation, Value) {
    let (mut daemon, workspace, task, port, fingerprint) = boot(provider).await;
    let mut rpc = super::connect_ws(port, super::client_config(&fingerprint)).await;
    let response = super::wss_rpc(&mut rpc, 1, "agent.wakeOrCreate", json!({
        "workspaceId": workspace, "taskNoteId": task, "contextMessage": "probe ownership control",
    })).await;
    assert_eq!(response["action"], "created_new");
    let record = provider.held_start(daemon.child.id());
    let fixture = verify_fixture_roots(&daemon);
    let observation = TeardownObservation(Rc::new(RefCell::new(State {
        owner: record["owner"].clone(), probe: record["child"].clone(), held: true,
        owner_reaped: false, failure: None, daemon_waited: false,
        expected_termination: false, events: Vec::new(),
        provider: None, provider_finalization: Value::Null, completion: Value::Null,
        pending: false, terminal: Value::Null,
    })));
    daemon.probe_observation = Some(observation.clone());
    (daemon, observation, fixture)
}

#[intent_test_macros::daemon_test]
async fn held_probe_settles_during_graceful_teardown() {
    let mut provider = Provider::new();
    let (daemon, observation, fixture) = started(&mut provider).await;
    let fixture_path = daemon.data_dir.path().to_owned();
    observation.change(|state| state.provider = Some(provider));
    let attempt = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(daemon)));
    let teardown = observation.snapshot();
    let terminal = teardown["terminal"].clone();
    // Semantic provider/directory finalization already finished inside Drop's
    // absolute deadline. This is serialized reporting, with errors propagated.
    let reporting = super::daemon_exit::report("probe-settlement-graceful", &json!({"fixture":fixture,
        "removed":!fixture_path.exists(),"observer":teardown["observer"],"teardown":teardown,
        "terminal":terminal}));
    if let Err(payload) = attempt { std::panic::resume_unwind(payload); }
    reporting.expect("complete held-provider report");
    assert!(teardown["failure"].is_null(), "{teardown}");
    assert_eq!(teardown["daemonWaited"], true);
    assert_eq!(teardown["expectedTermination"], true, "{teardown}");
    assert_eq!(terminal["empty"], true, "{terminal}");
    assert_eq!(teardown["providerFinalization"]["complete"], true, "{teardown}");
    assert_eq!(teardown["completion"]["normalCompletion"], true, "{teardown}");
    assert!(!fixture_path.exists(), "fixture directory was not test-cleaned");
}

#[test]
fn daemon_termination_status_contract() {
    // Raw Linux wait-status controls, not reproduced daemon crashes.
    for (raw, code, signal, expected) in [
        (libc::SIGKILL, None, Some(libc::SIGKILL), true),
        (0, Some(0), None, false),
        (1 << 8, Some(1), None, false),
        (libc::SIGTERM, None, Some(libc::SIGTERM), false),
    ] {
        let observation = TeardownObservation(Rc::new(RefCell::new(State {
            owner: Value::Null, probe: Value::Null, held: false, owner_reaped: true,
            failure: None, daemon_waited: false, expected_termination: false, events: Vec::new(),
            provider: None, provider_finalization: Value::Null, completion: Value::Null,
        pending: false, terminal: Value::Null,
        })));
        observation.wait_result(&Ok(ExitStatus::from_raw(raw)));
        let result = observation.snapshot();
        assert_eq!(result["daemonWaited"], true, "confirmed death remains usable for failure cleanup");
        assert_eq!(result["expectedTermination"], expected);
        assert_eq!(result["failure"].is_null(), expected);
        let event = &result["events"][0];
        assert_eq!(event["rawStatus"], raw);
        assert_eq!(event["code"], json!(code));
        assert_eq!(event["signal"], json!(signal));
        assert_eq!(event["expectedSigkill"], expected);
    }
}

#[test]
fn terminal_check_rejects_queued_extra_receipt() {
    // Exercise the real post-wait checker and SCM_CREDENTIALS socket path.
    // Receipt bodies and confirmed-death input are a same-process contract
    // control; this does not claim a daemon death or OS adoption reproduction.
    let mut provider = Provider::new();
    let pid = std::process::id();
    let sender = UnixDatagram::unbound().unwrap();
    sender.set_nonblocking(true).unwrap();
    sender.connect(&provider.socket_path).unwrap();
    for (index, event) in ["ProbeStarted", "OwnerReaped", "ProbeReturned"].iter().enumerate() {
        let receipt = json!({"fixtureNonce": provider.nonce, "feature": "probe-wait-observer",
            "platform": "linux", "owner": {"pid": pid}, "invocation": "terminal-contract",
            "sequence": index + 1, "event": event});
        sender.send(&serde_json::to_vec(&receipt).unwrap()).unwrap();
        assert_eq!(provider.receive(pid, false).unwrap(), receipt);
    }
    let empty = provider.terminal_check(pid, true);
    assert_eq!(empty["empty"], true);
    let extra = json!({"fixtureNonce": provider.nonce, "feature": "probe-wait-observer",
        "platform": "linux", "owner": {"pid": pid}, "invocation": "terminal-contract",
        "sequence": 4, "event": "OutputError"});
    sender.send(&serde_json::to_vec(&extra).unwrap()).unwrap();
    let terminal = provider.terminal_check(pid, true);
    assert_eq!(terminal["empty"], false);
    assert_eq!(terminal["extra"], extra);
    assert!(terminal["error"].is_string());
    assert_eq!(provider.records.len(), 4);
    assert_eq!(terminal["rawReceipt"]["credentials"]["pid"], pid);
    assert_eq!(super::daemon_exit::close_descriptor("contract-sender", sender)["ok"], true);
    let finalized = provider.finalize(false, Instant::now() + CONTROL_BOUND, &terminal, &Ok(()));
    assert_eq!(finalized["complete"], true, "{finalized}");
}

#[test]
fn queued_settlement_requires_complete_authenticated_receipts() {
    // Same-process contract controls exercise the actual SCM receiver and
    // post-exit acceptance path. Only the real held-daemon case proves OS
    // ownership; these synthetic records do not claim a provider was reaped.
    for case in ["valid", "cached-owner", "timeout", "return-error", "missing-wait",
        "invocation", "sequence", "nonce", "missing-return", "malformed", "truncated"]
    {
        let mut provider = Provider::new();
        let pid = std::process::id();
        let owner = identity(pid).unwrap();
        let sender = UnixDatagram::unbound().unwrap();
        sender.set_nonblocking(true).unwrap();
        sender.connect(&provider.socket_path).unwrap();
        let first = json!({"fixtureNonce":provider.nonce, "feature":"probe-wait-observer",
            "platform":"linux", "owner":owner, "creator":owner, "child":{"pid":123,"start":456},
            "candidate":{"path":"contract-only"}, "invocation":"queued-contract", "sequence":1,
            "event":"ProbeStarted"});
        sender.send(&serde_json::to_vec(&first).unwrap()).unwrap();
        provider.receive(pid, true).unwrap();
        let mut waited = first.clone();
        waited["sequence"] = json!(2);
        waited["event"] = json!("OwnerReaped");
        waited["detail"] = json!({"cause":"normal","normalSuccess":true,"rawStatus":0});
        let mut returned = first.clone();
        returned["sequence"] = json!(3);
        returned["event"] = json!("ProbeReturned");
        returned["detail"] = json!({"ownerWaitObserved":true,"observationFailed":false,"timedOut":false});
        match case {
            "timeout" => waited["detail"]["cause"] = json!("timeout"),
            "return-error" => returned["detail"]["observationFailed"] = json!(true),
            "missing-wait" => returned["detail"].as_object_mut().unwrap().remove("ownerWaitObserved").map(|_| ()).unwrap(),
            "invocation" => waited["invocation"] = json!("other"),
            "sequence" => waited["sequence"] = json!(3),
            "nonce" => waited["fixtureNonce"] = json!("other"),
            _ => {},
        }
        let bytes = match case {
            "malformed" => b"not json".to_vec(),
            "truncated" => vec![b'x'; RECEIPT_LIMIT + 1],
            _ => serde_json::to_vec(&waited).unwrap(),
        };
        sender.send(&bytes).unwrap();
        if case != "missing-return" { sender.send(&serde_json::to_vec(&returned).unwrap()).unwrap(); }
        let mut cached = owner.clone();
        if case == "cached-owner" { cached["start"] = json!(0); }
        let result = provider.owner_settled_unreaped(pid, &cached);
        println!("queued-settlement-contract {}", json!({"case":case, "accepted":result.is_ok(),
            "error":result.as_ref().err().map(ToString::to_string), "rawLastReceipt":provider.last_receipt}));
        assert_eq!(result.is_ok(), case == "valid", "{case}");
        if case == "valid" { assert_eq!(provider.terminal_check(pid, true)["empty"], true); }
        assert_eq!(super::daemon_exit::close_descriptor("contract-sender", sender)["ok"], true);
        let finalized = provider.finalize(false, Instant::now() + CONTROL_BOUND,
            &json!({"syntheticCase":case,"rawLastReceipt":provider.last_receipt}), &Ok(()));
        assert_eq!(finalized["complete"], true, "{finalized}");
    }
}
