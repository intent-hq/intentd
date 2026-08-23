//! Guard tests for the ownership check in `scripts/install.ps1` — the Windows
//! half of `install_sh_startup.rs`'s Part 1.
//!
//! Both installers refuse to register a service against a data dir a live
//! daemon already owns, and both decide it by reading `<data_dir>/intentd.pid`
//! the way the daemon's `read_pid` does. Two implementations of one rule drift
//! apart unless something exercises both, so these tests run the shipped
//! PowerShell: the blocks between the `>>> BEGIN`/`<<< END` markers in
//! install.ps1 are extracted verbatim and executed, exactly as
//! `install_sh_startup.rs` extracts and runs install.sh's shell functions.
//!
//! **Scope, and what still is not covered.** The extracted blocks are ordinary
//! .NET and run under PowerShell 7 on any platform, so these tests are real
//! anywhere `pwsh` is installed — they were developed against pwsh 7.4.6 on
//! macOS. They are skipped where it is not, so they are a supplement to
//! reviewing install.ps1, not a substitute: a run without `pwsh` proves
//! nothing. Everything in install.ps1 that needs Windows itself — Scheduled
//! Task registration, and the startup wait that polls the task's log — remains
//! unexercised by any test and can still regress silently.

#![cfg(unix)]

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};

fn install_ps1() -> String {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../scripts/install.ps1");
    fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

/// Extract one marked region of install.ps1 — the lines between its
/// `>>> BEGIN <name>` and `<<< END <name>` markers — so a driver runs the
/// shipped code rather than a copy of it.
fn ps_region(name: &str) -> String {
    let script = install_ps1();
    let begin = format!(">>> BEGIN {name}");
    let end = format!("<<< END {name}");
    let start = script
        .find(&begin)
        .unwrap_or_else(|| panic!("install.ps1 must mark the {name} region with `{begin}`"));
    let after_marker = script[start..].find('\n').map_or_else(
        || panic!("{begin} must be followed by the region"),
        |nl| start + nl + 1,
    );
    let stop = script[after_marker..].find(&end).map_or_else(
        || panic!("install.ps1 must close the {name} region with `{end}`"),
        |at| after_marker + at,
    );
    // The end marker is its own comment line; drop the `#` that opens it.
    let region = script[after_marker..stop].trim_end();
    let region = region.strip_suffix('#').unwrap_or(region);
    region.trim_end().to_string()
}

/// PowerShell, if this machine has it. Absent on stock macOS and on most Linux
/// hosts, hence the skips; `pwsh` is the cross-platform build, `powershell` the
/// Windows-only one.
fn pwsh() -> Option<PathBuf> {
    for name in ["pwsh", "powershell"] {
        let Ok(output) = Command::new("sh")
            .arg("-c")
            .arg(format!("command -v {name}"))
            .output()
        else {
            continue;
        };
        if !output.status.success() {
            continue;
        }
        let path = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if !path.is_empty() {
            return Some(PathBuf::from(path));
        }
    }
    None
}

/// The verdict the shipped owner check reaches: it either throws (refusing the
/// install) or falls through.
#[derive(Debug, PartialEq, Eq)]
enum Verdict {
    Proceeded,
    Refused(String),
}

/// Run the marked regions of install.ps1 for a given `INTENTD_DATA_DIR`, from a
/// given working directory. The resolve region runs first, exactly as the
/// script orders them, so a relative override reaches the check the same way it
/// does in a real run.
fn run_owner_check(pwsh: &Path, cwd: &Path, data_dir: &str) -> Verdict {
    let script = format!(
        "$ErrorActionPreference = 'Stop'\n\
         {resolve}\n\
         try {{\n\
         {check}\n\
         Write-Output 'PROCEEDED'\n\
         }} catch {{\n\
         Write-Output 'REFUSED'\n\
         Write-Output $_.Exception.Message\n\
         }}\n",
        resolve = ps_region("resolve-data-dir"),
        check = ps_region("data-dir-owner-check"),
    );
    let dir = tempfile::tempdir().unwrap();
    let script_path = dir.path().join("driver.ps1");
    fs::write(&script_path, script).unwrap();

    let output: Output = Command::new(pwsh)
        .arg("-NoProfile")
        .arg("-NonInteractive")
        .arg("-File")
        .arg(&script_path)
        .current_dir(cwd)
        .env("INTENTD_DATA_DIR", data_dir)
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout).replace('\r', "");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "the driver itself must not fail (a PowerShell error is not a verdict)\n\
         stdout: {stdout}\nstderr: {stderr}"
    );
    // The refusal is many lines long (it lists the ways out), so everything
    // after the marker is the message.
    if let Some(at) = stdout.find("REFUSED\n") {
        Verdict::Refused(stdout[at + "REFUSED\n".len()..].to_string())
    } else {
        assert!(
            stdout.contains("PROCEEDED"),
            "the driver reached neither verdict\nstdout: {stdout}\nstderr: {stderr}"
        );
        Verdict::Proceeded
    }
}

/// The daemon's own pidfile rule, mirrored from `read_pid`
/// (`crates/intentd/src/main.rs`): the whole file, trimmed, parsed as a `u32`.
/// Same mirror `install_sh_startup.rs` holds the shell check to.
fn daemon_read_pid(contents: &str) -> Option<u32> {
    contents.trim().parse::<u32>().ok()
}

fn write_pid_file(data_dir: &Path, contents: &str) {
    fs::create_dir_all(data_dir).unwrap();
    fs::write(data_dir.join("intentd.pid"), contents).unwrap();
}

/// A live process to stand in for the running daemon, killed on drop.
struct LiveProcess(Child);

impl LiveProcess {
    fn spawn() -> Self {
        Self(
            Command::new("sleep")
                .arg("120")
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap(),
        )
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

/// A pid that is definitely not running — the stale pidfile a crash leaves.
fn dead_pid() -> u32 {
    let mut child = Command::new("true").spawn().unwrap();
    let pid = child.id();
    child.wait().unwrap();
    pid
}

#[test]
fn a_live_owner_refuses_the_install_naming_the_pid_and_the_data_dir() {
    let Some(pwsh) = pwsh() else { return };
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().join("data");
    let owner = LiveProcess::spawn();
    write_pid_file(&data_dir, &format!("{}\n", owner.pid()));

    let verdict = run_owner_check(&pwsh, dir.path(), data_dir.to_str().unwrap());

    let Verdict::Refused(message) = verdict else {
        panic!("a data dir owned by a live daemon must abort the install, got {verdict:?}");
    };
    assert!(
        message.contains("already running and owns the data dir"),
        "{message}"
    );
    assert!(
        message.contains(&format!("pid {}", owner.pid())),
        "the refusal must name the owning pid: {message}"
    );
    assert!(
        message.contains(data_dir.to_str().unwrap()),
        "the refusal must name the contended data dir: {message}"
    );
    // The same ways out install.sh offers, so neither platform leaves the user
    // at a dead end.
    for hint in [
        "intentd status",
        "INTENTD_DATA_DIR",
        "INTENTD_INSTALL_SERVICE",
    ] {
        assert!(message.contains(hint), "missing {hint:?} in: {message}");
    }
}

/// The Windows check must read a pidfile the way the daemon does, for the same
/// reason the shell one must: a pidfile the daemon calls malformed becoming an
/// "already owned" abort blocks a legitimate install with no way around it.
/// Reading only the first line did exactly that.
#[test]
fn the_pidfile_verdict_matches_the_daemons_read_pid() {
    let Some(pwsh) = pwsh() else { return };
    let dir = tempfile::tempdir().unwrap();
    let owner = LiveProcess::spawn();
    let live = owner.pid();
    let dead = dead_pid();
    let split = {
        let digits = live.to_string();
        format!("{} {}", &digits[..1], &digits[1..])
    };

    let cases: Vec<(&str, String)> = vec![
        ("plain", format!("{live}\n")),
        ("no trailing newline", format!("{live}")),
        ("surrounded by whitespace", format!("  \t{live} \n\n")),
        ("leading plus", format!("+{live}")),
        ("leading zeros", format!("000{live}\n")),
        ("second line junk", format!("{live}\nnot-a-pid\n")),
        ("split by a space", format!("{split}\n")),
        ("trailing junk", format!("{live} junk\n")),
        ("stale but numeric", format!("{dead}\n")),
        ("empty", String::new()),
        ("whitespace only", "  \n\t\n".to_string()),
        ("non-numeric", "not-a-pid\n".to_string()),
        ("beyond u32", "4294967296\n".to_string()),
        ("negative", format!("-{live}\n")),
    ];

    for (index, (name, contents)) in cases.iter().enumerate() {
        let data_dir = dir.path().join(format!("case{index}"));
        write_pid_file(&data_dir, contents);
        // The helper is the only live pid in the table, so the daemon reads a
        // live owner exactly when its parse lands on it. Pid 0 is left out: it
        // is refused on both sides regardless of what the daemon makes of it.
        let is_owned = daemon_read_pid(contents) == Some(live);

        let verdict = run_owner_check(&pwsh, dir.path(), data_dir.to_str().unwrap());

        assert_eq!(
            matches!(verdict, Verdict::Refused(_)),
            is_owned,
            "{name}: pidfile {contents:?} — install.ps1 must read the same owner out of it \
             that the daemon's read_pid does, got {verdict:?}"
        );
    }
}

/// A relative `INTENTD_DATA_DIR` is anchored to the directory the installer
/// runs in, before the check reads it. A Scheduled Task has no working
/// directory of its own, so an unanchored override would let the task serve a
/// different dir than the one that was checked.
#[test]
fn a_relative_data_dir_is_anchored_to_the_installers_working_directory() {
    let Some(pwsh) = pwsh() else { return };
    let dir = tempfile::tempdir().unwrap();
    let owned = dir.path().join("owned");
    fs::create_dir_all(&owned).unwrap();
    let owned = owned.canonicalize().unwrap();
    let elsewhere = dir.path().join("elsewhere");
    fs::create_dir_all(&elsewhere).unwrap();
    let daemon = LiveProcess::spawn();
    write_pid_file(&owned.join("data"), &format!("{}\n", daemon.pid()));

    let verdict = run_owner_check(&pwsh, &owned, "./data");
    let Verdict::Refused(message) = verdict else {
        panic!("the owned dir must be found through the relative override, got {verdict:?}");
    };
    assert!(
        message.contains(owned.join("data").to_str().unwrap()),
        "the refusal must name the absolute dir, not './data': {message}"
    );

    // Same override, run from a directory where `./data` is nobody's.
    assert_eq!(
        run_owner_check(&pwsh, &elsewhere, "./data"),
        Verdict::Proceeded,
        "an unrelated ./data must not block the install"
    );
}
