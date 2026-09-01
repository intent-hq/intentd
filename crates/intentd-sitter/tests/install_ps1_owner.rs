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
    run_owner_check_with(pwsh, cwd, data_dir, "", &[]).0
}

/// Like [`run_owner_check`], with a stub prelude prepended to the driver (see
/// [`service_stubs`]) and extra environment variables set. Also returns the
/// driver's stdout, so a test can assert on the allowance's info line.
fn run_owner_check_with(
    pwsh: &Path,
    cwd: &Path,
    data_dir: &str,
    stubs: &str,
    envs: &[(&str, &str)],
) -> (Verdict, String) {
    let script = format!(
        "$ErrorActionPreference = 'Stop'\n\
         {stubs}\n\
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

    let mut command = Command::new(pwsh);
    command
        .arg("-NoProfile")
        .arg("-NonInteractive")
        .arg("-File")
        .arg(&script_path)
        .current_dir(cwd)
        .env_remove("INTENTD_SERVICE_NAME")
        .env_remove("INTENTD_INSTALL_DIR")
        .env("INTENTD_DATA_DIR", data_dir);
    for (key, value) in envs {
        command.env(key, value);
    }
    let output: Output = command.output().unwrap();
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
        let message = stdout[at + "REFUSED\n".len()..].to_string();
        (Verdict::Refused(message), stdout)
    } else {
        assert!(
            stdout.contains("PROCEEDED"),
            "the driver reached neither verdict\nstdout: {stdout}\nstderr: {stderr}"
        );
        (Verdict::Proceeded, stdout)
    }
}

/// A `Win32_Process` row for the [`service_stubs`] table: pid, parent pid,
/// started-at-seconds.
struct StubProc(u32, u32, i64);

/// PowerShell shims for the Windows-only service queries the upgrade
/// allowance makes: a `New-Object` answering the `Schedule.Service` COM
/// lookup with one running task (leaf name `task_name`, full path
/// `task_path`, engine pid `engine_pid`), a `Get-CimInstance` serving
/// `Win32_Process` rows from a fixed [`StubProc`] table, and a `Get-Process`
/// that treats every pid in that table as live (the fake tree's pids do not
/// run on the test host) while delegating any other pid — notably the real
/// `LiveProcess` owner — to the genuine cmdlet. Functions shadow cmdlets in
/// PowerShell, so the extracted block picks these up unmodified — the same
/// PATH-stub trick `install_sh_startup.rs` plays on systemctl/launchctl.
fn service_stubs(task_name: &str, task_path: &str, engine_pid: u32, procs: &[StubProc]) -> String {
    const TEMPLATE: &str = r#"
$StubProcTable = @(
@ROWS@
)
function Get-CimInstance {
    param([string]$ClassName, [string]$Filter, [string]$ErrorAction)
    if ($ClassName -ne 'Win32_Process') { throw "stub Get-CimInstance: unexpected class '$ClassName'" }
    if ($Filter -notmatch '^ProcessId = ([0-9]+)$') { throw "stub Get-CimInstance: unexpected filter '$Filter'" }
    $wanted = [int64]$Matches[1]
    foreach ($row in $StubProcTable) { if ([int64]$row.ProcessId -eq $wanted) { return $row } }
}
function Get-Process {
    param([int]$Id, [string]$ErrorAction)
    foreach ($row in $StubProcTable) { if ([int64]$row.ProcessId -eq $Id) { return $row } }
    return Microsoft.PowerShell.Management\Get-Process -Id $Id -ErrorAction SilentlyContinue
}
function New-Object {
    param([string]$ComObject)
    if ($ComObject -ne 'Schedule.Service') { throw "stub New-Object: unexpected COM class '$ComObject'" }
    $service = [pscustomobject]@{}
    Add-Member -InputObject $service -MemberType ScriptMethod -Name Connect -Value { }
    Add-Member -InputObject $service -MemberType ScriptMethod -Name GetRunningTasks -Value {
        param($flags)
        @([pscustomobject]@{ Name = '@TASKNAME@'; Path = '@TASKPATH@'; EnginePID = @ENGINEPID@ })
    }
    return $service
}
"#;
    let rows = procs
        .iter()
        .map(|StubProc(pid, ppid, started_at)| {
            format!(
                "    [pscustomobject]@{{ ProcessId = {pid}; ParentProcessId = {ppid}; \
                 CreationDate = ([datetime]'2026-01-01T00:00:00').AddSeconds({started_at}) }}"
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    TEMPLATE
        .replace("@ROWS@", &rows)
        .replace("@TASKNAME@", task_name)
        .replace("@TASKPATH@", task_path)
        .replace("@ENGINEPID@", &engine_pid.to_string())
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

/// No service stubs here, so the block's `Schedule.Service` COM lookup fails
/// (as it does on any host where the scheduler cannot be asked) — which
/// doubles as the guard that an unreachable scheduler grants no allowance.
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

/// The allowance's ownership witness: `<data_dir>\sitter\sitter.pid`,
/// written exactly as the serve-mode sitter's `PidFile` guard writes it
/// (`crates/intentd-sitter/src/supervisor.rs`).
fn write_sitter_pid_file(data_dir: &Path, contents: &str) {
    let sitter_dir = data_dir.join("sitter");
    fs::create_dir_all(&sitter_dir).unwrap();
    fs::write(sitter_dir.join("sitter.pid"), contents).unwrap();
}

/// The upgrade allowance: an owner inside the running "intentd" task's
/// process tree does not block a re-run — the installer restarts that very
/// task onto the new binary. The scheduler stub reports the task's engine
/// pid; the CIM table gives the tree the engine spawned: engine (cmd) →
/// sitter (whose pid the data dir's sitter.pid names) → daemon (the pidfile
/// owner, the only real process).
#[test]
fn an_owner_under_our_running_task_lets_the_install_proceed() {
    let Some(pwsh) = pwsh() else { return };
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().join("data");
    let owner = LiveProcess::spawn();
    write_pid_file(&data_dir, &format!("{}\n", owner.pid()));

    let engine: u32 = 1_900_000_001;
    let sitter: u32 = 1_900_000_002;
    write_sitter_pid_file(&data_dir, &format!("{sitter}\n"));
    let stubs = service_stubs(
        "intentd",
        "\\intentd",
        engine,
        &[
            StubProc(owner.pid(), sitter, 20),
            StubProc(sitter, engine, 10),
            StubProc(engine, 4, 0),
        ],
    );

    let (verdict, stdout) =
        run_owner_check_with(&pwsh, dir.path(), data_dir.to_str().unwrap(), &stubs, &[]);
    assert_eq!(
        verdict,
        Verdict::Proceeded,
        "an owner under our own running task must not refuse the re-install"
    );
    assert!(
        stdout.contains("belongs to the 'intentd' scheduled task"),
        "the allowance must say why it proceeded: {stdout}"
    );
}

/// The task the allowance matches is the one this installer manages: the
/// default name or the `INTENTD_SERVICE_NAME` override — never whatever task
/// happens to be running.
#[test]
fn the_allowance_follows_the_service_name_override() {
    let Some(pwsh) = pwsh() else { return };
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().join("data");
    let owner = LiveProcess::spawn();
    write_pid_file(&data_dir, &format!("{}\n", owner.pid()));

    let engine: u32 = 1_900_000_001;
    let sitter: u32 = 1_900_000_002;
    write_sitter_pid_file(&data_dir, &format!("{sitter}\n"));
    let table = [
        StubProc(owner.pid(), sitter, 20),
        StubProc(sitter, engine, 10),
        StubProc(engine, 4, 0),
    ];
    let stubs = service_stubs("intentd-test", "\\intentd-test", engine, &table);

    // The override names the running task: allowed.
    let (verdict, stdout) = run_owner_check_with(
        &pwsh,
        dir.path(),
        data_dir.to_str().unwrap(),
        &stubs,
        &[("INTENTD_SERVICE_NAME", "intentd-test")],
    );
    assert_eq!(
        verdict,
        Verdict::Proceeded,
        "the allowance must honor INTENTD_SERVICE_NAME"
    );
    assert!(
        stdout.contains("belongs to the 'intentd-test' scheduled task"),
        "the info line must name the overridden task: {stdout}"
    );

    // Without the override the installer manages 'intentd', so the running
    // 'intentd-test' tree is somebody else's daemon: refused.
    let (verdict, _) =
        run_owner_check_with(&pwsh, dir.path(), data_dir.to_str().unwrap(), &stubs, &[]);
    assert!(
        matches!(verdict, Verdict::Refused(_)),
        "a same-tree owner under a differently-named task must still refuse, got {verdict:?}"
    );
}

/// A running task that matches only by leaf name is not ours: the installer
/// registers and restarts the root `\<name>` task, so a `\other\<name>` task
/// — same `IRunningTask.Name`, different `Path` — must not unlock the
/// allowance even when the owner sits under its tree with the sitter.pid
/// witness on the chain.
#[test]
fn a_running_task_matching_only_by_leaf_name_is_refused() {
    let Some(pwsh) = pwsh() else { return };
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().join("data");
    let owner = LiveProcess::spawn();
    write_pid_file(&data_dir, &format!("{}\n", owner.pid()));

    let engine: u32 = 1_900_000_001;
    let sitter: u32 = 1_900_000_002;
    write_sitter_pid_file(&data_dir, &format!("{sitter}\n"));
    let stubs = service_stubs(
        "intentd",
        "\\other\\intentd",
        engine,
        &[
            StubProc(owner.pid(), sitter, 20),
            StubProc(sitter, engine, 10),
            StubProc(engine, 4, 0),
        ],
    );

    let (verdict, _) =
        run_owner_check_with(&pwsh, dir.path(), data_dir.to_str().unwrap(), &stubs, &[]);
    assert!(
        matches!(verdict, Verdict::Refused(_)),
        "a task matching only by leaf name must not unlock the allowance, got {verdict:?}"
    );
}

/// A shared Task Scheduler engine must not vouch for a foreign tree: the
/// owner descends from our task's `EnginePID`, but the walked chain never
/// crosses the pid in this data dir's sitter.pid (the foreign tree runs
/// under its own supervisor), so the allowance is forfeited — `EnginePID`
/// ancestry alone proves only "some task on this engine", not ours.
#[test]
fn a_foreign_tasks_daemon_on_a_shared_engine_is_refused() {
    let Some(pwsh) = pwsh() else { return };
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().join("data");
    let owner = LiveProcess::spawn();
    write_pid_file(&data_dir, &format!("{}\n", owner.pid()));

    let engine: u32 = 1_900_000_001;
    let foreign_sitter: u32 = 1_900_000_002;
    let our_sitter: u32 = 1_900_000_004;
    // Our own sitter is live but off the owner's chain.
    write_sitter_pid_file(&data_dir, &format!("{our_sitter}\n"));
    let stubs = service_stubs(
        "intentd",
        "\\intentd",
        engine,
        &[
            StubProc(owner.pid(), foreign_sitter, 20),
            StubProc(foreign_sitter, engine, 10),
            StubProc(engine, 4, 0),
            StubProc(our_sitter, engine, 10),
        ],
    );

    let (verdict, _) =
        run_owner_check_with(&pwsh, dir.path(), data_dir.to_str().unwrap(), &stubs, &[]);
    assert!(
        matches!(verdict, Verdict::Refused(_)),
        "a foreign task's daemon on a shared engine must be refused, got {verdict:?}"
    );
}

/// No sitter.pid, no witness: the owner sits squarely under our running
/// task's engine, but the data dir carries no sitter.pid file, so nothing
/// ties that tree to this data dir and the refusal stands.
#[test]
fn a_missing_sitter_pidfile_forfeits_the_allowance() {
    let Some(pwsh) = pwsh() else { return };
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().join("data");
    let owner = LiveProcess::spawn();
    write_pid_file(&data_dir, &format!("{}\n", owner.pid()));

    let engine: u32 = 1_900_000_001;
    let sitter: u32 = 1_900_000_002;
    let stubs = service_stubs(
        "intentd",
        "\\intentd",
        engine,
        &[
            StubProc(owner.pid(), sitter, 20),
            StubProc(sitter, engine, 10),
            StubProc(engine, 4, 0),
        ],
    );

    let (verdict, _) =
        run_owner_check_with(&pwsh, dir.path(), data_dir.to_str().unwrap(), &stubs, &[]);
    assert!(
        matches!(verdict, Verdict::Refused(_)),
        "engine ancestry without the sitter.pid witness must be refused, got {verdict:?}"
    );
}

/// A sitter.pid naming a dead pid is a stale leftover (hard kill), not a
/// witness: the pid appears in no CIM row and no real process, so the
/// allowance is forfeited even though the file parses cleanly.
#[test]
fn a_stale_sitter_pidfile_forfeits_the_allowance() {
    let Some(pwsh) = pwsh() else { return };
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().join("data");
    let owner = LiveProcess::spawn();
    write_pid_file(&data_dir, &format!("{}\n", owner.pid()));

    let engine: u32 = 1_900_000_001;
    let sitter: u32 = 1_900_000_002;
    // Names the chain's sitter pid... except nothing live has that pid: the
    // stub table is the liveness oracle, so keep the walked chain's middle
    // hop under a different pid and leave the named one dead.
    let dead_sitter: u32 = 1_900_000_005;
    write_sitter_pid_file(&data_dir, &format!("{dead_sitter}\n"));
    let stubs = service_stubs(
        "intentd",
        "\\intentd",
        engine,
        &[
            StubProc(owner.pid(), sitter, 20),
            StubProc(sitter, engine, 10),
            StubProc(engine, 4, 0),
        ],
    );

    let (verdict, _) =
        run_owner_check_with(&pwsh, dir.path(), data_dir.to_str().unwrap(), &stubs, &[]);
    assert!(
        matches!(verdict, Verdict::Refused(_)),
        "a dead sitter.pid must not unlock the allowance, got {verdict:?}"
    );
}

/// An owner the running task does not contain — a manual `intentd serve`,
/// some unrelated tree — keeps today's refusal, options list included, even
/// though a same-named task is running.
#[test]
fn an_owner_outside_the_tasks_process_tree_is_still_refused() {
    let Some(pwsh) = pwsh() else { return };
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().join("data");
    let owner = LiveProcess::spawn();
    write_pid_file(&data_dir, &format!("{}\n", owner.pid()));

    // The owner's chain tops out at an unrelated shell; the task's engine pid
    // is nowhere on it — even with a live sitter.pid witness on the chain.
    let shell: u32 = 1_900_000_003;
    write_sitter_pid_file(&data_dir, &format!("{shell}\n"));
    let stubs = service_stubs(
        "intentd",
        "\\intentd",
        1_900_000_001,
        &[StubProc(owner.pid(), shell, 20), StubProc(shell, 4, 0)],
    );

    let (verdict, _) =
        run_owner_check_with(&pwsh, dir.path(), data_dir.to_str().unwrap(), &stubs, &[]);
    let Verdict::Refused(message) = verdict else {
        panic!("an owner outside the task's tree must abort the install, got {verdict:?}");
    };
    assert!(
        message.contains("already running and owns the data dir"),
        "{message}"
    );
    for hint in [
        "intentd status",
        "INTENTD_DATA_DIR",
        "INTENTD_INSTALL_SERVICE",
    ] {
        assert!(message.contains(hint), "missing {hint:?} in: {message}");
    }
}

/// The pid-reuse guard: a parent that started *after* its child holds a
/// recycled pid, so the chain is broken there — the walk must stop and
/// forfeit the allowance rather than trust ancestry through it, even with
/// the sitter.pid witness already sighted below the break.
#[test]
fn a_parent_younger_than_its_child_breaks_the_chain_and_forfeits_the_allowance() {
    let Some(pwsh) = pwsh() else { return };
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().join("data");
    let owner = LiveProcess::spawn();
    write_pid_file(&data_dir, &format!("{}\n", owner.pid()));

    // owner → sitter is sound, but sitter's recorded parent (the engine pid)
    // started long after the sitter: that pid was reused, the real parent is
    // gone, so the engine match beyond the break must not count.
    let engine: u32 = 1_900_000_001;
    let sitter: u32 = 1_900_000_002;
    write_sitter_pid_file(&data_dir, &format!("{sitter}\n"));
    let stubs = service_stubs(
        "intentd",
        "\\intentd",
        engine,
        &[
            StubProc(owner.pid(), sitter, 20),
            StubProc(sitter, engine, 10),
            StubProc(engine, 4, 500),
        ],
    );

    let (verdict, _) =
        run_owner_check_with(&pwsh, dir.path(), data_dir.to_str().unwrap(), &stubs, &[]);
    assert!(
        matches!(verdict, Verdict::Refused(_)),
        "ancestry through a reused pid must not unlock the allowance, got {verdict:?}"
    );
}
