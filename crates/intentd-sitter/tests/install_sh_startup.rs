//! Guard tests for the two startup decisions `scripts/install.sh` makes around
//! service setup:
//!
//! 1. `check_data_dir_not_owned` — refusing to register a service against a
//!    data dir a live daemon already owns (the daemon locks its data dir, so a
//!    second service on it can only crash-loop).
//! 2. `verify_daemon` — resolving the first start into an outcome (up, failed,
//!    or genuinely undecided) instead of classifying the service log once at a
//!    fixed deadline and calling everything else "may still be downloading".
//!
//! Both functions are extracted from the real script, so these tests pin the
//! shipped code rather than a copy.

#![cfg(unix)]

use std::fs;
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

fn install_sh() -> String {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../scripts/install.sh");
    fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

/// Extract one shell function from install.sh (its header through the first
/// column-0 `}`), so a driver can run the shipped implementation verbatim.
fn sh_function(script: &str, name: &str) -> String {
    let header = format!("{name}() {{");
    let start = script
        .find(&header)
        .unwrap_or_else(|| panic!("install.sh must define {name}()"));
    let rest = &script[start..];
    let end = rest
        .find("\n}")
        .unwrap_or_else(|| panic!("{name}() must close with a column-0 brace"));
    rest[..end + 2].to_string()
}

fn sh_functions(names: &[&str]) -> String {
    let script = install_sh();
    names
        .iter()
        .map(|name| sh_function(&script, name))
        .collect::<Vec<_>>()
        .join("\n")
}

fn shell_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "'\\''"))
}

fn quoted(path: &Path) -> String {
    shell_quote(path.to_str().unwrap())
}

fn stdout_of(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn stderr_of(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

fn write_executable(path: &Path, body: &str) {
    fs::write(path, format!("#!/bin/sh\n{body}\n")).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
}

// ---------------------------------------------------------------------------
// Part 1 — a data dir another daemon owns
// ---------------------------------------------------------------------------

/// The installer's helpers that answer "does a live daemon own this data dir",
/// plus install.sh's own `fail`. The driver prints PROCEEDED only when the
/// check lets the install continue, so "refused" and "allowed" can never be
/// confused with each other.
fn run_owner_check(os: &str, home: &Path, data_dir: Option<&Path>) -> Output {
    run_owner_check_in(None, os, home, data_dir.map(|d| d.to_str().unwrap()))
}

/// As `run_owner_check`, but from a chosen working directory and with
/// `INTENTD_DATA_DIR` given verbatim — so a relative override can be pointed at
/// the dir the installer was run from.
fn run_owner_check_in(cwd: Option<&Path>, os: &str, home: &Path, data_dir: Option<&str>) -> Output {
    let driver = format!(
        "set -eu\n\
         fail() {{ printf '%s\\n' \"install.sh: error: $*\" >&2; exit 1; }}\n\
         os={os}\n\
         install_dir=/opt/intentd/bin\n\
         {functions}\n\
         check_data_dir_not_owned\n\
         printf '%s\\n' 'PROCEEDED'\n",
        os = shell_quote(os),
        functions = sh_functions(&[
            "resolve_data_dir",
            "pid_is_alive",
            "data_dir_owner_pid",
            "process_path",
            "check_data_dir_not_owned",
        ]),
    );
    let mut cmd = Command::new("/bin/sh");
    cmd.arg("-c")
        .arg(driver)
        .env("HOME", home)
        .env_remove("XDG_DATA_HOME")
        .env_remove("INTENTD_DATA_DIR");
    if let Some(cwd) = cwd {
        cmd.current_dir(cwd);
    }
    if let Some(dir) = data_dir {
        cmd.env("INTENTD_DATA_DIR", dir);
    }
    cmd.output().unwrap()
}

/// Run the shipped `resolve_data_dir` and return the dir it names.
fn run_resolve_data_dir(cwd: &Path, os: &str, home: &Path, data_dir: Option<&str>) -> String {
    let driver = format!(
        "set -eu\nos={os}\n{functions}\nresolve_data_dir\n",
        os = shell_quote(os),
        functions = sh_functions(&["resolve_data_dir"]),
    );
    let mut cmd = Command::new("/bin/sh");
    cmd.arg("-c")
        .arg(driver)
        .current_dir(cwd)
        .env("HOME", home)
        .env_remove("XDG_DATA_HOME")
        .env_remove("INTENTD_DATA_DIR");
    if let Some(dir) = data_dir {
        cmd.env("INTENTD_DATA_DIR", dir);
    }
    let output = cmd.output().unwrap();
    assert!(
        output.status.success(),
        "resolve_data_dir failed: {}",
        stderr_of(&output)
    );
    stdout_of(&output)
}

fn write_pid_file(data_dir: &Path, contents: &str) {
    fs::create_dir_all(data_dir).unwrap();
    fs::write(data_dir.join("intentd.pid"), contents).unwrap();
}

/// Create a directory and return the path `pwd` will report for it: on macOS a
/// temp dir is reached through a symlink, so the logical and physical paths
/// differ and only the physical one can be compared against.
fn created_dir(path: &Path) -> PathBuf {
    fs::create_dir_all(path).unwrap();
    path.canonicalize().unwrap()
}

/// A live process to stand in for the running daemon, killed on drop so a
/// failing test never leaks it.
struct LiveProcess(Child);

impl LiveProcess {
    fn spawn() -> Self {
        let child = Command::new("sleep")
            .arg("120")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        Self(child)
    }

    fn pid(&self) -> u32 {
        self.0.id()
    }
}

impl Drop for LiveProcess {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// A pid that is definitely not running: a child that has already exited and
/// been reaped — exactly the stale pidfile a crash or a hard reboot leaves.
fn dead_pid() -> u32 {
    let mut child = Command::new("true").spawn().unwrap();
    let pid = child.id();
    child.wait().unwrap();
    pid
}

/// The daemon's own pidfile rule, mirrored from `read_pid`
/// (`crates/intentd/src/main.rs`): the **whole** file, trimmed, parsed as a
/// `u32`. The installer has to read a pidfile the same way — every case where
/// it finds an owner the daemon does not is a legitimate install aborted over
/// a file the daemon would have deleted and started anyway, and there is no
/// way around that abort short of editing the pidfile by hand.
fn daemon_read_pid(contents: &str) -> Option<u32> {
    contents.trim().parse::<u32>().ok()
}

/// The pid the shipped `data_dir_owner_pid` reads out of a pidfile holding
/// `contents`, if any.
fn installer_owner_pid(root: &Path, case: &str, contents: &str) -> Option<u32> {
    let data_dir = root.join(case);
    write_pid_file(&data_dir, contents);
    let driver = format!(
        "set -eu\n{functions}\nprintf 'OWNER[%s]\\n' \"$(data_dir_owner_pid {dir})\"\n",
        functions = sh_functions(&["pid_is_alive", "data_dir_owner_pid"]),
        dir = quoted(&data_dir),
    );
    let output = Command::new("/bin/sh")
        .arg("-c")
        .arg(driver)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "data_dir_owner_pid must not fail the caller: {}",
        stderr_of(&output)
    );
    let stdout = stdout_of(&output);
    let owner = stdout
        .lines()
        .find_map(|line| line.strip_prefix("OWNER[")?.strip_suffix(']'))
        .unwrap_or_else(|| panic!("driver printed no OWNER line: {stdout}"));
    if owner.is_empty() {
        None
    } else {
        Some(owner.parse().unwrap_or_else(|e| {
            panic!("data_dir_owner_pid printed {owner:?}, which is not a pid: {e}")
        }))
    }
}

/// The live pid with a space pushed into the middle of it: `1 23` is two
/// tokens to the daemon, not pid 123 — the shape that made deleting whitespace
/// from anywhere in the file unsafe.
fn split_by_a_space(pid: u32) -> String {
    let digits = pid.to_string();
    format!("{} {}", &digits[..1], &digits[1..])
}

#[test]
fn a_live_owner_refuses_the_install_naming_the_pid_and_its_binary() {
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().join("data");
    let owner = LiveProcess::spawn();
    write_pid_file(&data_dir, &format!("{}\n", owner.pid()));

    let output = run_owner_check("Linux", dir.path(), Some(&data_dir));

    assert_eq!(
        output.status.code(),
        Some(1),
        "a data dir owned by a live daemon must abort the install; stderr: {}",
        stderr_of(&output)
    );
    assert!(
        !stdout_of(&output).contains("PROCEEDED"),
        "the install must stop before anything is registered, stdout: {}",
        stdout_of(&output)
    );
    let stderr = stderr_of(&output);
    assert!(
        stderr.contains("already running and owns the data dir"),
        "stderr: {stderr}"
    );
    assert!(
        stderr.contains(&format!("pid {}", owner.pid())),
        "the refusal must name the owning pid, got: {stderr}"
    );
    assert!(
        stderr.contains("sleep"),
        "the refusal must name the owner's binary path, got: {stderr}"
    );
    assert!(
        stderr.contains(data_dir.to_str().unwrap()),
        "the refusal must name the contended data dir, got: {stderr}"
    );
    // The three real ways out, so the message is actionable rather than a dead
    // end (the running daemon is usually the one the user wants to keep).
    for hint in [
        "intentd status",
        "INTENTD_DATA_DIR=",
        "INTENTD_INSTALL_SERVICE=0",
    ] {
        assert!(stderr.contains(hint), "missing {hint:?} in: {stderr}");
    }
}

#[test]
fn a_stale_pid_file_is_not_ownership() {
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().join("data");
    write_pid_file(&data_dir, &format!("{}\n", dead_pid()));

    let output = run_owner_check("Linux", dir.path(), Some(&data_dir));

    assert_eq!(
        output.status.code(),
        Some(0),
        "a pidfile whose owner is gone must not block the install; stderr: {}",
        stderr_of(&output)
    );
    assert!(stdout_of(&output).contains("PROCEEDED"));
}

#[test]
fn an_absent_or_unusable_pid_file_is_not_ownership() {
    let dir = tempfile::tempdir().unwrap();
    // No pidfile at all, an empty one, junk, and pid 0 (which `kill -0` would
    // aim at our own process group) must all read as "nobody owns this".
    let cases: [(&str, Option<&str>); 6] = [
        ("absent", None),
        ("empty", Some("")),
        ("blank", Some("\n")),
        ("junk", Some("not-a-pid\n")),
        ("zero", Some("0\n")),
        // Zero however it is spelled: `kill -0 00` aims at our process group
        // just as `kill -0 0` does, so padding must not sneak past the check.
        ("padded-zero", Some("00\n")),
    ];
    for (name, contents) in cases {
        let data_dir = dir.path().join(name);
        fs::create_dir_all(&data_dir).unwrap();
        if let Some(contents) = contents {
            fs::write(data_dir.join("intentd.pid"), contents).unwrap();
        }
        let output = run_owner_check("Linux", dir.path(), Some(&data_dir));
        assert_eq!(
            output.status.code(),
            Some(0),
            "{name}: must not read as ownership; stderr: {}",
            stderr_of(&output)
        );
        assert!(stdout_of(&output).contains("PROCEEDED"), "{name}");
    }
}

#[test]
fn an_unreadable_pid_file_is_not_ownership() {
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().join("data");
    write_pid_file(&data_dir, "1\n");
    let pid_file = data_dir.join("intentd.pid");
    fs::set_permissions(&pid_file, fs::Permissions::from_mode(0o000)).unwrap();
    if fs::read(&pid_file).is_ok() {
        // Running as root: nothing is unreadable, so there is nothing to test.
        return;
    }

    let output = run_owner_check("Linux", dir.path(), Some(&data_dir));

    assert_eq!(
        output.status.code(),
        Some(0),
        "an unreadable pidfile proves nothing and must not block the install; stderr: {}",
        stderr_of(&output)
    );
    assert!(stdout_of(&output).contains("PROCEEDED"));
}

/// Without `INTENTD_DATA_DIR` the check must look where the daemon actually
/// resolves its data dir (`intent_core::Config::resolve` → the `directories`
/// crate) — otherwise the common case, the user's default dir, is never
/// checked at all.
#[test]
fn the_default_data_dir_matches_the_daemons_own_resolution() {
    for (os, relative) in [
        ("Darwin", "Library/Application Support/intentd"),
        ("Linux", ".local/share/intentd"),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let data_dir = dir.path().join(relative);
        let owner = LiveProcess::spawn();
        write_pid_file(&data_dir, &format!("{}\n", owner.pid()));

        let output = run_owner_check(os, dir.path(), None);

        assert_eq!(
            output.status.code(),
            Some(1),
            "{os}: the default data dir must be checked; stderr: {}",
            stderr_of(&output)
        );
        assert!(
            stderr_of(&output).contains(data_dir.to_str().unwrap()),
            "{os}: expected {} in: {}",
            data_dir.display(),
            stderr_of(&output)
        );
    }
}

/// The installer's reading of a pidfile must be the daemon's reading of it.
/// Anywhere the two disagree the installer either refuses an install the
/// daemon would have accepted — the worse half, since a false "already owned"
/// blocks a legitimate install outright — or waves through a genuine conflict.
#[test]
fn the_pidfile_verdict_matches_the_daemons_read_pid() {
    let dir = tempfile::tempdir().unwrap();
    let owner = LiveProcess::spawn();
    let live = owner.pid();
    let dead = dead_pid();

    let cases: Vec<(&str, String)> = vec![
        ("plain", format!("{live}\n")),
        ("no trailing newline", format!("{live}")),
        ("surrounded by whitespace", format!("  \t{live} \n\n")),
        // u32::from_str accepts a leading sign, so the daemon does too.
        ("leading plus", format!("+{live}")),
        ("leading zeros", format!("000{live}\n")),
        // The two cases that made reading only the first line, and deleting
        // whitespace from anywhere in the file, wrong: malformed to the daemon.
        ("second line junk", format!("{live}\nnot-a-pid\n")),
        ("split by a space", format!("{}\n", split_by_a_space(live))),
        ("trailing junk", format!("{live} junk\n")),
        ("stale but numeric", format!("{dead}\n")),
        ("empty", String::new()),
        ("whitespace only", "  \n\t\n".to_string()),
        ("non-numeric", "not-a-pid\n".to_string()),
        ("beyond u32", "4294967296\n".to_string()),
        ("wildly beyond u32", "999999999999999999999\n".to_string()),
        ("negative", format!("-{live}\n")),
    ];

    for (index, (name, contents)) in cases.iter().enumerate() {
        // The helper is the only live pid in the table, so the daemon reads a
        // live owner exactly when its parse lands on it. (Pid 0 is left out:
        // it is the one value the installer deliberately reads stricter than
        // the daemon — see `an_absent_or_unusable_pid_file_is_not_ownership`.)
        let expected = daemon_read_pid(contents).filter(|pid| *pid == live);
        let got = installer_owner_pid(dir.path(), &format!("case{index}"), contents);
        assert_eq!(
            got, expected,
            "{name}: pidfile {contents:?} — the installer must read the same owner \
             out of it that the daemon's read_pid does"
        );
    }
}

/// The consequence of the parity above, end to end: a pidfile the daemon calls
/// malformed must let the install continue. The daemon deletes such a file and
/// starts, so refusing here would block an install that was going to work.
#[test]
fn a_pidfile_the_daemon_calls_malformed_does_not_abort_the_install() {
    let dir = tempfile::tempdir().unwrap();
    let owner = LiveProcess::spawn();
    let cases = [
        ("second-line-junk", format!("{}\nnot-a-pid\n", owner.pid())),
        (
            "split-by-a-space",
            format!("{}\n", split_by_a_space(owner.pid())),
        ),
    ];

    for (name, contents) in cases {
        let data_dir = dir.path().join(name);
        write_pid_file(&data_dir, &contents);

        let output = run_owner_check("Linux", dir.path(), Some(&data_dir));

        assert_eq!(
            output.status.code(),
            Some(0),
            "{name}: {contents:?} is malformed to the daemon, so it must not abort the install; \
             stderr: {}",
            stderr_of(&output)
        );
        assert!(stdout_of(&output).contains("PROCEEDED"), "{name}");
    }
}

/// A relative `INTENTD_DATA_DIR` has to name one absolute dir. It is carried
/// into the unit/plist, and neither a systemd user unit nor a `LaunchAgent`
/// inherits the installer's working directory — so left relative it would name
/// the dir this check tests and, later, a different dir the service serves.
#[test]
fn a_relative_data_dir_is_anchored_to_the_installers_working_directory() {
    let dir = tempfile::tempdir().unwrap();
    // Canonical: on macOS a temp dir is reached through a symlink, and `pwd`
    // reports the physical path.
    let cwd = created_dir(&dir.path().join("project"));
    let home = dir.path();

    for (given, expected) in [
        ("data", cwd.join("data")),
        ("./data", cwd.join("data")),
        ("nested/data", cwd.join("nested/data")),
    ] {
        assert_eq!(
            run_resolve_data_dir(&cwd, "Linux", home, Some(given)),
            expected.to_str().unwrap(),
            "{given} must resolve against the installer's working directory"
        );
    }

    // An absolute value is passed through untouched, which is what makes the
    // resolution idempotent: the installer anchors the override once and then
    // resolves it again for the unit/plist.
    let absolute = "/var/lib/intentd-service";
    assert_eq!(
        run_resolve_data_dir(&cwd, "Linux", home, Some(absolute)),
        absolute
    );
    let once = run_resolve_data_dir(&cwd, "Linux", home, Some("./data"));
    assert_eq!(run_resolve_data_dir(&cwd, "Linux", home, Some(&once)), once);
}

/// And the check follows that anchoring: the dir tested is the one under the
/// installer's working directory, not whatever `./data` happens to mean
/// somewhere else.
#[test]
fn a_relative_data_dir_is_checked_where_the_service_would_serve_it() {
    let dir = tempfile::tempdir().unwrap();
    let owned = created_dir(&dir.path().join("owned"));
    let elsewhere = created_dir(&dir.path().join("elsewhere"));
    let daemon = LiveProcess::spawn();
    write_pid_file(&owned.join("data"), &format!("{}\n", daemon.pid()));

    let output = run_owner_check_in(Some(&owned), "Linux", dir.path(), Some("./data"));
    assert_eq!(
        output.status.code(),
        Some(1),
        "the owned dir must be found through the relative override; stderr: {}",
        stderr_of(&output)
    );
    assert!(
        stderr_of(&output).contains(owned.join("data").to_str().unwrap()),
        "the refusal must name the absolute dir, not './data': {}",
        stderr_of(&output)
    );

    // Same override, run from a directory where `./data` is nobody's: the
    // check must not reach across to the other one.
    let output = run_owner_check_in(Some(&elsewhere), "Linux", dir.path(), Some("./data"));
    assert_eq!(
        output.status.code(),
        Some(0),
        "an unrelated ./data must not block the install; stderr: {}",
        stderr_of(&output)
    );
    assert!(stdout_of(&output).contains("PROCEEDED"));
}

/// The service must be given the dir that was checked. Both writers go through
/// `resolve_data_dir` rather than interpolating the raw override, so the
/// anchoring cannot be true for the check and false for the unit/plist.
#[test]
fn the_unit_and_the_plist_carry_the_resolved_data_dir() {
    let script = install_sh();
    for name in ["setup_service_linux", "setup_service_macos"] {
        let body = sh_function(&script, name);
        assert!(
            body.contains("$(resolve_data_dir)"),
            "{name} must write the resolved data dir into the service"
        );
        for line in body.lines() {
            let writes_service_file =
                line.contains("systemd_escape") || line.contains("xml_escape");
            assert!(
                !(writes_service_file && line.contains("\"$INTENTD_DATA_DIR\"")),
                "{name} must not put the raw override into the service — a relative \
                 value would resolve elsewhere once the service starts: {line}"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Part 2 — resolving the first start into an outcome
// ---------------------------------------------------------------------------

/// The sitter's own lines, verbatim in shape (the substrings are the install
/// log contract pinned by `install_log_contract_*` in `supervisor_e2e.rs`).
const CRASH_LINE: &str =
    "intentd-sitter: intentd 0.6.8 exited unexpectedly (exit status 1); respawning in 200ms";
const GIVE_UP_LINE: &str = "intentd-sitter: intentd failed 5 times in a row without ever staying up for 30s; this looks permanent, not transient, so the sitter is giving up instead of respawning it forever";

struct WaitCase {
    dir: tempfile::TempDir,
    log_file: PathBuf,
    install_dir: PathBuf,
    deadline: u32,
    poll: u32,
    settle: u32,
}

impl WaitCase {
    /// `status_body` is the fake `intentd`'s script: install.sh polls it with
    /// `intentd status`, so exit 0 means "the daemon answers".
    fn new(status_body: &str, deadline: u32, poll: u32, settle: u32) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let install_dir = dir.path().join("bin");
        fs::create_dir_all(&install_dir).unwrap();
        write_executable(&install_dir.join("intentd"), status_body);
        let log_file = dir.path().join("intentd.err.log");
        fs::write(&log_file, "").unwrap();
        Self {
            dir,
            log_file,
            install_dir,
            deadline,
            poll,
            settle,
        }
    }

    /// Re-point the fake `intentd` at a marker file: it answers `status` only
    /// once the marker exists, i.e. once the daemon has come up. Returns the
    /// marker's path.
    fn up_when_marker_exists(&self) -> PathBuf {
        let marker = self.dir.path().join("up.marker");
        write_executable(
            &self.install_dir.join("intentd"),
            &format!("[ -f {} ] || exit 1\nexit 0", quoted(&marker)),
        );
        marker
    }

    fn append_log(&self, line: &str) {
        let mut f = fs::OpenOptions::new()
            .append(true)
            .open(&self.log_file)
            .unwrap();
        writeln!(f, "{line}").unwrap();
    }

    /// Append `line` to the service log after `delay`, mimicking a sitter that
    /// only reports a failure once it has finally spawned the daemon.
    fn append_log_after(&self, delay: Duration, line: &'static str) -> std::thread::JoinHandle<()> {
        let log_file = self.log_file.clone();
        std::thread::spawn(move || {
            std::thread::sleep(delay);
            if let Ok(mut f) = fs::OpenOptions::new().append(true).open(&log_file) {
                let _ = writeln!(f, "{line}");
            }
        })
    }

    /// Create `path` after `delay` — the marker the fake `intentd` waits for
    /// before answering, i.e. the moment the daemon comes up.
    fn touch_after(delay: Duration, path: PathBuf) -> std::thread::JoinHandle<()> {
        std::thread::spawn(move || {
            std::thread::sleep(delay);
            let _ = fs::write(&path, "");
        })
    }

    /// Run the shipped `verify_daemon` and report both the verdict and how long
    /// it took. A `fail` exits the driver, so an absent VERDICT line on stdout
    /// is itself the "reported a failure" signal.
    fn run(&self) -> (Output, Duration) {
        let driver = format!(
            "set -eu\n\
             info() {{ printf '%s\\n' \"install.sh: $*\"; }}\n\
             warn() {{ printf '%s\\n' \"install.sh: warning: $*\" >&2; }}\n\
             fail() {{ printf '%s\\n' \"install.sh: error: $*\" >&2; exit 1; }}\n\
             log_tail_lines=40\n\
             verify_deadline={deadline}\n\
             verify_poll={poll}\n\
             verify_settle={settle}\n\
             verify_progress=100000\n\
             install_dir={install_dir}\n\
             log_file={log_file}\n\
             log_desc={log_file}\n\
             log_offset=0\n\
             log_unit=''\n\
             log_since=''\n\
             restart_hint='launchctl kickstart -k gui/501/com.intenthq.intentd'\n\
             auto_resume=on\n\
             {functions}\n\
             if verify_daemon; then printf '%s\\n' 'VERDICT up'; else printf '%s\\n' 'VERDICT undecided'; fi\n",
            deadline = self.deadline,
            poll = self.poll,
            settle = self.settle,
            install_dir = quoted(&self.install_dir),
            log_file = quoted(&self.log_file),
            functions = sh_functions(&[
                "service_log_tail",
                "report_daemon_log",
                "auto_resume_pending_note",
                "verify_daemon",
            ]),
        );
        let started = Instant::now();
        let output = Command::new("/bin/sh")
            .arg("-c")
            .arg(driver)
            .output()
            .unwrap();
        (output, started.elapsed())
    }
}

/// The case the shipped fix missed, reproduced literally: the sitter is still
/// resolving the channel and downloading at the 60s mark, so the first crash
/// line lands *after* the old fixed window — which classified the log once, at
/// the end, and reported "may still be downloading" seconds before the daemon
/// proved it could never start.
///
/// Deliberately wall-clock literal: 60s was a constant in the shipped script,
/// so no scaled-down timing reproduces the miss.
#[test]
fn a_crash_after_the_old_sixty_second_window_is_still_reported() {
    let case = WaitCase::new("exit 1", 150, 2, 2);
    let writer = case.append_log_after(Duration::from_secs(64), CRASH_LINE);

    let (output, elapsed) = case.run();
    writer.join().unwrap();

    let stdout = stdout_of(&output);
    let stderr = stderr_of(&output);
    assert!(
        !stdout.contains("VERDICT"),
        "a daemon whose crash lands after 60s must be reported as a failure, not returned as a verdict; stdout: {stdout}\nstderr: {stderr}"
    );
    assert_eq!(output.status.code(), Some(1), "stderr: {stderr}");
    assert!(
        stderr.contains("failing to start") && stderr.contains(CRASH_LINE),
        "the failure must quote the sitter's own line, got: {stderr}"
    );
    assert!(
        !stderr.contains("may still be downloading"),
        "the old racy verdict must be gone, got: {stderr}"
    );
    assert!(
        elapsed > Duration::from_secs(60),
        "the wait must outlive the old 60s window for this case to mean anything (took {elapsed:?})"
    );
}

/// The flip side of waiting longer: once the outcome is decided, the wait ends
/// — a decided failure must not sit out the (now much longer) deadline.
#[test]
fn crash_evidence_ends_the_wait_instead_of_burning_the_deadline() {
    let case = WaitCase::new("exit 1", 120, 1, 2);
    let writer = case.append_log_after(Duration::from_secs(3), CRASH_LINE);

    let (output, elapsed) = case.run();
    writer.join().unwrap();

    assert_eq!(
        output.status.code(),
        Some(1),
        "stdout: {}",
        stdout_of(&output)
    );
    let stderr = stderr_of(&output);
    assert!(stderr.contains("failing to start"), "stderr: {stderr}");
    // The dropped auto-resume answer is named rather than silently lost.
    assert!(
        stderr.contains("Your auto-resume choice ('on') was not applied"),
        "stderr: {stderr}"
    );
    assert!(
        elapsed < Duration::from_secs(30),
        "a decided failure must be reported promptly, took {elapsed:?}"
    );
}

/// The sitter's give-up banner is terminal — nothing can change after it, so
/// it is reported at once, with no settle grace.
#[test]
fn the_give_up_banner_is_reported_immediately() {
    let case = WaitCase::new("exit 1", 60, 1, 30);
    case.append_log(GIVE_UP_LINE);

    let (output, elapsed) = case.run();

    assert_eq!(
        output.status.code(),
        Some(1),
        "stdout: {}",
        stdout_of(&output)
    );
    let stderr = stderr_of(&output);
    assert!(stderr.contains("has given up"), "stderr: {stderr}");
    assert!(
        elapsed < Duration::from_secs(20),
        "a stopped service is decided; took {elapsed:?}"
    );
}

/// Polling the log continuously means crash lines are seen early — including
/// the ones a working install produces. The sitter respawns, so a daemon that
/// died once and then came up is a success, not a failure.
#[test]
fn a_crash_the_daemon_recovers_from_is_not_a_failure() {
    let case = WaitCase::new("exit 1", 60, 1, 10);
    let marker = case.up_when_marker_exists();
    case.append_log(CRASH_LINE);
    let up = WaitCase::touch_after(Duration::from_secs(3), marker);

    let (output, _elapsed) = case.run();
    up.join().unwrap();

    assert_eq!(
        output.status.code(),
        Some(0),
        "stderr: {}",
        stderr_of(&output)
    );
    let stdout = stdout_of(&output);
    assert!(stdout.contains("VERDICT up"), "stdout: {stdout}");
    assert!(stdout.contains("daemon is up"), "stdout: {stdout}");
}

/// A daemon that answers straight away is reported straight away: waiting
/// longer for undecided cases must not make a working install slower.
#[test]
fn a_responsive_daemon_returns_at_once() {
    let case = WaitCase::new("exit 0", 300, 2, 10);

    let (output, elapsed) = case.run();

    assert_eq!(
        output.status.code(),
        Some(0),
        "stderr: {}",
        stderr_of(&output)
    );
    assert!(stdout_of(&output).contains("VERDICT up"));
    assert!(
        elapsed < Duration::from_secs(5),
        "success must not wait, took {elapsed:?}"
    );
}

/// Deadline reached with a quiet log: a genuinely slow download still looks
/// like a slow download. The outcome is unknown, and the message says exactly
/// that instead of asserting either half of it.
#[test]
fn a_quiet_log_at_the_deadline_reports_what_is_unknown_not_a_failure() {
    let case = WaitCase::new("exit 1", 6, 1, 2);

    let (output, _elapsed) = case.run();

    assert_eq!(
        output.status.code(),
        Some(0),
        "an undecided wait is not an install failure; stderr: {}",
        stderr_of(&output)
    );
    assert!(
        stdout_of(&output).contains("VERDICT undecided"),
        "stdout: {}",
        stdout_of(&output)
    );
    let stderr = stderr_of(&output);
    assert!(stderr.contains("warning:"), "stderr: {stderr}");
    assert!(
        stderr.contains("could not tell whether") && stderr.contains("still downloading"),
        "the message must name both possibilities it cannot separate, got: {stderr}"
    );
    assert!(
        stderr.contains("Your auto-resume choice ('on') was not applied"),
        "an undecided wait drops the auto-resume answer too, got: {stderr}"
    );
}
