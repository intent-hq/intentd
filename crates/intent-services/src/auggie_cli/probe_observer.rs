//! Explicit Linux test-build observation of the existing probe owner.
//! This module never waits, kills, retries, releases a provider, or changes a
//! probe result. Datagram loss invalidates evidence instead of delaying a wait.

use std::fs;
use std::io;
use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};
use std::os::unix::net::UnixDatagram;
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

pub(super) const CONFIG_ENV: &str = "INTENT_TEST_PROBE_OBSERVER";
const MAX_RECEIPT: usize = 16 * 1024;

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    socket: PathBuf,
    executable: PathBuf,
    fixture_nonce: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct Identity {
    pid: u32,
    start: u64,
    tgid: u32,
    ppid: u32,
    pgid: u32,
    sid: u32,
    state: String,
    executable: PathBuf,
    argv: Vec<String>,
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn stat_fields(pid: u32) -> io::Result<Vec<String>> {
    let stat = fs::read_to_string(format!("/proc/{pid}/stat"))?;
    let (_, fields) = stat.rsplit_once(") ").ok_or_else(|| invalid("invalid stat"))?;
    let fields: Vec<_> = fields.split_whitespace().map(str::to_owned).collect();
    if fields.len() < 20 {
        return Err(invalid("incomplete stat"));
    }
    Ok(fields)
}

impl Identity {
    fn read(pid: u32) -> io::Result<Self> {
        let fields = stat_fields(pid)?;
        let number = |i: usize| -> io::Result<u32> {
            fields[i].parse().map_err(|_| invalid("invalid process identity"))
        };
        let status = fs::read_to_string(format!("/proc/{pid}/status"))?;
        let tgid = status.lines().find_map(|line| line.strip_prefix("Tgid:"))
            .ok_or_else(|| invalid("missing TGID"))?.trim().parse()
            .map_err(|_| invalid("invalid TGID"))?;
        let argv = fs::read(format!("/proc/{pid}/cmdline"))?;
        if argv.len() > 4096 || matches!(fields[0].as_str(), "Z" | "X") {
            return Err(invalid("missing live process identity"));
        }
        let result = Self {
            pid,
            start: fields[19].parse().map_err(|_| invalid("invalid start time"))?,
            tgid,
            ppid: number(1)?,
            pgid: number(2)?,
            sid: number(3)?,
            state: fields[0].clone(),
            executable: fs::read_link(format!("/proc/{pid}/exe"))?,
            argv: argv.split(|byte| *byte == 0).filter(|part| !part.is_empty())
                .map(|part| String::from_utf8(part.to_vec()).map_err(|_| invalid("non-UTF8 argv")))
                .collect::<io::Result<_>>()?,
        };
        let fresh = stat_fields(pid)?;
        if fresh[19] != fields[19] || fresh[1..4] != fields[1..4]
            || matches!(fresh[0].as_str(), "Z" | "X")
        {
            return Err(invalid("process changed during identity observation"));
        }
        Ok(result)
    }

    fn same_owner(&self, other: &Self) -> bool {
        self.pid == other.pid && self.start == other.start && self.tgid == other.tgid
            && self.ppid == other.ppid && self.pgid == other.pgid && self.sid == other.sid
            && self.executable == other.executable
    }
}

#[derive(Clone, PartialEq, Eq, Serialize)]
struct Executable {
    path: PathBuf,
    device: u64,
    inode: u64,
    sha256: String,
}

impl Executable {
    fn read(path: &Path) -> io::Result<Self> {
        let before = fs::symlink_metadata(path)?;
        if !path.is_absolute() || !before.is_file() || before.len() > 16 * 1024
            || before.permissions().mode() & 0o111 == 0
        {
            return Err(invalid("observer requires a small private regular executable"));
        }
        let bytes = fs::read(path)?;
        let after = fs::symlink_metadata(path)?;
        if (before.dev(), before.ino(), before.len(), before.mtime(), before.mtime_nsec())
            != (after.dev(), after.ino(), after.len(), after.mtime(), after.mtime_nsec())
        {
            return Err(invalid("candidate changed during observation"));
        }
        Ok(Self {
            path: path.to_owned(), device: before.dev(), inode: before.ino(),
            sha256: Sha256::digest(bytes).iter().map(|byte| format!("{byte:02x}")).collect(),
        })
    }
}

fn binding(owner: &Identity, creator: &Identity, child: &Identity) -> io::Result<()> {
    if owner.pid != owner.tgid || creator.tgid != owner.pid || child.pid != child.tgid
        || child.ppid != owner.pid || child.pgid != owner.pgid || child.sid != owner.sid
    {
        return Err(invalid("ambiguous or adopted probe identity"));
    }
    Ok(())
}

struct Active {
    config: Config,
    invocation: String,
    socket: UnixDatagram,
    owner: Identity,
    creator: Identity,
    candidate: Option<Executable>,
    child: Option<Identity>,
    sequence: u32,
    failed: bool,
    channel_lost: bool,
    timed_out: bool,
    owner_wait: bool,
}

pub(super) struct Observation(Option<Active>);

pub(super) fn remove_child_channel(command: &mut Command) {
    // The socket itself is CLOEXEC; its address/config is also daemon-only.
    command.env_remove(CONFIG_ENV);
}

#[cfg(test)]
thread_local! {
    static TEST_CONFIG: std::cell::RefCell<Option<Config>> = const { std::cell::RefCell::new(None) };
}

impl Observation {
    pub(super) fn begin(path: &Path) -> Self {
        #[cfg(test)]
        if let Some(config) = TEST_CONFIG.with(|value| value.borrow().clone()) {
            return Self::configured(path, config);
        }
        let Ok(raw) = std::env::var(CONFIG_ENV) else { return Self(None) };
        if raw.len() > 4096 {
            eprintln!("probe observer: invalid oversized configuration");
            return Self(None);
        }
        match serde_json::from_str(&raw) {
            Ok(config) => Self::configured(path, config),
            Err(error) => {
                eprintln!("probe observer: invalid configuration: {error}");
                Self(None)
            }
        }
    }

    fn configured(path: &Path, config: Config) -> Self {
        if path != config.executable { return Self(None) }
        let prepare = || -> io::Result<Active> {
            if config.fixture_nonce.is_empty() || config.fixture_nonce.len() > 128 {
                return Err(invalid("invalid fixture nonce"));
            }
            let metadata = fs::symlink_metadata(&config.socket)?;
            if !metadata.file_type().is_socket() || metadata.permissions().mode() & 0o077 != 0 {
                return Err(invalid("observer socket is not private"));
            }
            let socket = UnixDatagram::unbound()?;
            socket.set_nonblocking(true)?;
            socket.connect(&config.socket)?;
            let owner = Identity::read(std::process::id())?;
            // SAFETY: gettid has no arguments and is local to this calling thread.
            let creator = Identity::read(unsafe { libc::gettid() } as u32)?;
            Ok(Active {
                config: config.clone(), invocation: uuid::Uuid::new_v4().to_string(),
                socket, owner, creator, candidate: None, child: None,
                sequence: 0, failed: false, channel_lost: false,
                timed_out: false, owner_wait: false,
            })
        };
        let mut this = match prepare() {
            Ok(active) => Self(Some(active)),
            Err(error) => {
                eprintln!("probe observer: unavailable for configured invocation: {error}");
                return Self(None);
            }
        };
        match Executable::read(path) {
            Ok(candidate) => this.0.as_mut().expect("active observation").candidate = Some(candidate),
            Err(error) => this.fail(&error.to_string()),
        }
        this
    }

    fn emit(&mut self, event: &str, detail: Value) {
        let Some(active) = &mut self.0 else { return };
        if active.channel_lost { return }
        active.sequence += 1;
        let receipt = json!({
            "schema": 1, "feature": "probe-wait-observer", "platform": "linux",
            "fixtureNonce": active.config.fixture_nonce, "invocation": active.invocation,
            "sequence": active.sequence, "event": event, "owner": active.owner,
            "creator": active.creator, "candidate": active.candidate,
            "child": active.child, "detail": detail,
        });
        let sent = serde_json::to_vec(&receipt).map_err(io::Error::other).and_then(|bytes| {
            if bytes.len() > MAX_RECEIPT { return Err(invalid("oversized receipt")) }
            let count = active.socket.send(&bytes)?;
            if count != bytes.len() { return Err(invalid("partial receipt")) }
            Ok(())
        });
        if let Err(error) = sent {
            active.failed = true;
            active.channel_lost = true;
            eprintln!("probe observer: channel lost for {}: {error}", active.invocation);
        }
    }

    fn fail(&mut self, reason: &str) {
        if let Some(active) = &mut self.0 { active.failed = true; }
        self.emit("ObservationFailed", json!({"reason": reason}));
    }

    pub(super) fn spawn_error(&mut self, error: &io::Error) {
        self.fail("spawn failed");
        self.emit("SpawnFailed", json!({"error": error.to_string(), "errno": error.raw_os_error()}));
    }

    pub(super) fn spawned(&mut self, child: &Child) {
        let Some(active) = &self.0 else { return };
        let capture = || -> io::Result<Identity> {
            let identity = Identity::read(child.id())?;
            binding(&active.owner, &active.creator, &identity)?;
            if !active.owner.same_owner(&Identity::read(active.owner.pid)?)
                || !active.creator.same_owner(&Identity::read(active.creator.pid)?)
                || active.candidate.as_ref() != Some(&Executable::read(&active.config.executable)?)
            {
                return Err(invalid("owner or candidate changed before spawn receipt"));
            }
            Ok(identity)
        };
        match capture() {
            Ok(identity) => self.0.as_mut().expect("active observation").child = Some(identity),
            Err(error) => self.fail(&error.to_string()),
        }
        self.emit("ProbeStarted", json!({"spawnedPid": child.id()}));
    }

    pub(super) fn waited(&mut self, pid: u32, result: &io::Result<ExitStatus>, timeout: bool) {
        let Some(active) = &self.0 else { return };
        let status = match result {
            Ok(status) => status,
            Err(error) => { self.wait_error(error); return; }
        };
        let valid_owner = active.child.as_ref().is_some_and(|child| {
            child.pid == pid && binding(&active.owner, &active.creator, child).is_ok()
        })
            && Identity::read(active.owner.pid).is_ok_and(|fresh| active.owner.same_owner(&fresh))
            && Identity::read(active.creator.pid).is_ok_and(|fresh| active.creator.same_owner(&fresh))
            && Executable::read(&active.config.executable).is_ok_and(|fresh| active.candidate.as_ref() == Some(&fresh));
        if !valid_owner { self.fail("successful wait has no stable original-owner binding"); }
        let active = self.0.as_mut().expect("active observation");
        active.owner_wait = true;
        let normal_success = !active.failed && !active.timed_out && !timeout && status.success();
        self.emit(if valid_owner { "OwnerReaped" } else { "OwnerWaitUnqualified" }, json!({
            "cause": if timeout { "timeoutCleanup" } else { "normal" },
            "rawStatus": status.into_raw(), "code": status.code(), "signal": status.signal(),
            "normalSuccess": normal_success,
        }));
    }

    pub(super) fn timed_out(&mut self) {
        if let Some(active) = &mut self.0 { active.timed_out = true; active.failed = true; }
        self.emit("ProbeTimedOut", json!({}));
    }

    pub(super) fn killed(&mut self, result: &io::Result<()>) {
        self.emit("TimeoutKillResult", match result {
            Ok(()) => json!({"ok": true}),
            Err(error) => json!({"ok": false, "error": error.to_string(), "errno": error.raw_os_error()}),
        });
    }

    pub(super) fn wait_error(&mut self, error: &io::Error) {
        if let Some(active) = &mut self.0 { active.failed = true; }
        self.emit("OwnerWaitError", json!({"error": error.to_string(), "errno": error.raw_os_error()}));
    }

    pub(super) fn output_error(&mut self, reason: &str) {
        self.emit("OutputError", json!({"reason": reason}));
    }
}

impl Drop for Observation {
    fn drop(&mut self) {
        let Some(active) = &self.0 else { return };
        let detail = json!({"ownerWaitObserved": active.owner_wait,
            "observationFailed": active.failed, "timedOut": active.timed_out});
        self.emit("ProbeReturned", detail);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CString;
    use std::fs::{File, OpenOptions};
    use std::io::Write;
    use std::os::unix::ffi::OsStrExt;
    use std::process::Stdio;
    use std::time::Duration;

    struct Harness {
        _root: tempfile::TempDir,
        config: Config,
        socket: UnixDatagram,
        release: File,
    }

    fn quoted(path: &Path) -> String {
        format!("'{}'", path.to_str().expect("private path").replace('\'', "'\\''"))
    }

    impl Harness {
        fn new(exit: u8) -> Self {
            let root = tempfile::Builder::new().prefix("itd-probe-owner-").tempdir().unwrap();
            let fifo = root.path().join("release");
            let cpath = CString::new(fifo.as_os_str().as_bytes()).unwrap();
            // SAFETY: cpath is a valid, terminated, exclusively owned pathname.
            assert_eq!(unsafe { libc::mkfifo(cpath.as_ptr(), 0o600) }, 0);
            let release = OpenOptions::new().read(true).write(true).open(&fifo).unwrap();
            let executable = root.path().join("probe");
            fs::write(&executable, format!(
                "#!/bin/sh\n[ -z \"${{INTENT_TEST_PROBE_OBSERVER+x}}\" ] || exit 43\nread -r release < {}\nprintf '0.35.0\\n'\nexit {exit}\n",
                quoted(&fifo),
            )).unwrap();
            fs::set_permissions(&executable, fs::Permissions::from_mode(0o700)).unwrap();
            let socket_path = root.path().join("owner.sock");
            let socket = UnixDatagram::bind(&socket_path).unwrap();
            fs::set_permissions(&socket_path, fs::Permissions::from_mode(0o600)).unwrap();
            socket.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
            let config = Config {
                socket: socket_path, executable, fixture_nonce: uuid::Uuid::new_v4().to_string(),
            };
            Self { _root: root, config, socket, release }
        }

        fn receive(&self) -> Value {
            let mut bytes = [0_u8; MAX_RECEIPT];
            let length = self.socket.recv(&mut bytes).unwrap();
            serde_json::from_slice(&bytes[..length]).unwrap()
        }

        fn pipeline(mut self, release: bool) -> (Option<String>, Vec<Value>) {
            let config = self.config.clone();
            let worker = std::thread::spawn(move || {
                TEST_CONFIG.with(|slot| *slot.borrow_mut() = Some(config.clone()));
                let result = super::super::run_version_probe(&config.executable);
                TEST_CONFIG.with(|slot| *slot.borrow_mut() = None);
                result
            });
            let started = self.receive();
            assert_eq!(started["event"], "ProbeStarted", "{started}");
            assert_eq!(started["owner"]["pid"], std::process::id());
            assert_eq!(started["child"]["ppid"], std::process::id());
            assert!(started["child"]["start"].as_u64().unwrap() > 0);
            if release { self.release.write_all(b"release\n").unwrap(); }
            let mut records = vec![started];
            loop {
                let record = self.receive();
                let last = record["event"] == "ProbeReturned";
                records.push(record);
                if last { break }
                assert!(records.len() <= 8, "unbounded observation output");
            }
            (worker.join().unwrap(), records)
        }

        fn direct_child(&self) -> (Observation, Child) {
            let observation = Observation::configured(&self.config.executable, self.config.clone());
            let mut command = Command::new(&self.config.executable);
            command.arg("--version").stdout(Stdio::null()).stderr(Stdio::null());
            remove_child_channel(&mut command);
            (observation, command.spawn().unwrap())
        }
    }

    #[test]
    fn actual_child_success_requires_owner_wait() {
        let (result, records) = Harness::new(0).pipeline(true);
        assert_eq!(result.as_deref(), Some("0.35.0"));
        assert_eq!(records.len(), 3);
        assert_eq!(records[1]["event"], "OwnerReaped");
        assert_eq!(records[1]["detail"]["normalSuccess"], true);
        assert_eq!(records[1]["detail"]["rawStatus"], 0);
        assert_eq!(records[2]["detail"]["ownerWaitObserved"], true);
        assert_eq!(records[2]["detail"]["observationFailed"], false);
        for (index, record) in records.iter().enumerate() {
            assert_eq!(record["sequence"], index + 1);
            assert_eq!(record["invocation"], records[0]["invocation"]);
            assert_eq!(record["child"], records[0]["child"]);
        }
    }

    #[test]
    fn actual_nonzero_exit_is_reaped_but_not_normal_success() {
        let (result, records) = Harness::new(2).pipeline(true);
        assert_eq!(result, None);
        assert_eq!(records[1]["event"], "OwnerReaped");
        assert_eq!(records[1]["detail"]["code"], 2);
        assert_eq!(records[1]["detail"]["normalSuccess"], false);
    }

    #[test]
    fn original_timeout_cleanup_cannot_satisfy_settlement() {
        // The real unchanged three-second timeout ends the held child. No
        // control-side wait/kill or sleep substitutes for the probe's owner.
        let (result, records) = Harness::new(0).pipeline(false);
        assert_eq!(result, None);
        assert!(records.iter().any(|r| r["event"] == "ProbeTimedOut"));
        let waited = records.iter().find(|r| r["event"] == "OwnerReaped").unwrap();
        assert_eq!(waited["detail"]["cause"], "timeoutCleanup");
        assert_eq!(waited["detail"]["normalSuccess"], false);
        assert_eq!(records.last().unwrap()["detail"]["timedOut"], true);
    }

    #[test]
    fn wait_error_contract_stays_failed_after_later_real_cleanup() {
        let mut harness = Harness::new(0);
        let (mut observation, mut child) = harness.direct_child();
        observation.spawned(&child);
        assert_eq!(harness.receive()["event"], "ProbeStarted");
        // Contract fault, explicitly not a claimed kernel wait-error repro.
        observation.wait_error(&io::Error::from_raw_os_error(libc::ECHILD));
        assert_eq!(harness.receive()["event"], "OwnerWaitError");
        harness.release.write_all(b"release\n").unwrap();
        let result = child.wait();
        observation.waited(child.id(), &result, false);
        assert_eq!(harness.receive()["detail"]["normalSuccess"], false);
        assert!(observation.0.as_ref().unwrap().failed);
    }

    #[test]
    fn lost_channel_does_not_change_actual_child_result_or_pass() {
        let mut harness = Harness::new(0);
        let (mut observation, mut child) = harness.direct_child();
        observation.spawned(&child);
        assert_eq!(harness.receive()["event"], "ProbeStarted");
        drop(harness.socket);
        harness.release.write_all(b"release\n").unwrap();
        let result = child.wait();
        assert!(result.as_ref().unwrap().success());
        observation.waited(child.id(), &result, false);
        let active = observation.0.as_ref().unwrap();
        assert!(active.owner_wait && active.failed && active.channel_lost);
    }

    #[test]
    fn adoption_and_reused_identity_contracts_are_rejected() {
        // Contract faults applied to recorded identity, not a claimed OS
        // adoption/reuse reproduction. Each child still has its real owner.
        for fault in ["adopted", "reused", "missing"] {
            let mut harness = Harness::new(0);
            let (mut observation, mut child) = harness.direct_child();
            observation.spawned(&child);
            assert_eq!(harness.receive()["event"], "ProbeStarted");
            let active = observation.0.as_mut().unwrap();
            match fault {
                "adopted" => active.child.as_mut().unwrap().ppid = 1,
                "reused" => active.owner.start += 1,
                "missing" => active.child = None,
                _ => unreachable!(),
            }
            harness.release.write_all(b"release\n").unwrap();
            let result = child.wait();
            observation.waited(child.id(), &result, false);
            assert!(result.unwrap().success());
            assert_eq!(harness.receive()["event"], "ObservationFailed");
            let receipt = harness.receive();
            assert_eq!(receipt["event"], "OwnerWaitUnqualified");
            assert_eq!(receipt["detail"]["normalSuccess"], false);
            assert!(observation.0.as_ref().unwrap().failed);
        }
    }

    #[test]
    fn actual_spawn_error_cannot_supply_a_wait_receipt() {
        let harness = Harness::new(0);
        // The candidate is a valid private executable, but its interpreter
        // deliberately does not exist. Exercise the real Command::spawn error.
        let missing = harness._root.path().join("missing-interpreter");
        assert!(!missing.exists());
        fs::write(&harness.config.executable, format!("#!{}\n", missing.display())).unwrap();
        TEST_CONFIG.with(|slot| *slot.borrow_mut() = Some(harness.config.clone()));
        let result = super::super::run_version_probe(&harness.config.executable);
        TEST_CONFIG.with(|slot| *slot.borrow_mut() = None);
        assert_eq!(result, None);
        assert_eq!(harness.receive()["event"], "ObservationFailed");
        assert_eq!(harness.receive()["event"], "SpawnFailed");
        let returned = harness.receive();
        assert_eq!(returned["event"], "ProbeReturned");
        assert_eq!(returned["detail"]["ownerWaitObserved"], false);
        assert_eq!(returned["detail"]["observationFailed"], true);
    }
}
