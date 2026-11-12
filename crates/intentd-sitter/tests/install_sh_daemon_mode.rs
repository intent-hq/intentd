//! Whole runs of `scripts/install.sh` for the "How will you use this computer?"
//! question: interactive full, interactive node, scripted node, and the
//! scripted edge cases (no service, a bad value, switching a node back, the
//! unasked default).
//!
//! Each test runs the shipped script end to end on the Linux path. Only the
//! outside world is faked: `curl` serves a local release archive whose
//! `intentd` is a shell script that records every call, `systemctl` /
//! `loginctl` / `uname` are stubs, and the terminal is a pipe — the script's
//! `</dev/tty` reads become `</dev/stdin` and its `>/dev/tty` writes go to a
//! transcript file. A run "without a terminal" points those paths at a file
//! that does not exist, so a developer's real terminal never leaks in.

#![cfg(target_os = "linux")]

use std::fs;
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};

const TRIPLE: &str = "x86_64-unknown-linux-musl";

/// The fake installed `intentd`. Every call is appended to `$FAKE_STATE/calls`;
/// settings persist as one file per path; `restart` starts a stand-in daemon
/// and records its pid in the data dir's pidfile, as a real restart does;
/// `pair` reads the picker selection from stdin unless `--yes` and prints
/// output shaped like the real `intentd pair` (QR block, `URL:`, `Token:`,
/// `Fingerprint:`) with fake credentials, or the node link for `--node`.
const FAKE_INTENTD: &str = r#"#!/bin/sh
state="${FAKE_STATE:?}"
printf '%s\n' "$*" >>"$state/calls"
case "$1" in
  --sitter-version) printf '%s\n' 'intentd-sitter 9.9.9' ;;
  status) exit 0 ;;
  settings)
    if [ "$#" -eq 2 ]; then
      value=regular
      [ -f "$state/setting.$2" ] && value=$(cat "$state/setting.$2")
      printf '%s = %s\n' "$2" "$value"
    else
      printf '%s' "$3" >"$state/setting.$2"
      printf '%s = %s\n' "$2" "$3"
    fi ;;
  restart)
    tail -f /dev/null >/dev/null 2>&1 &
    printf '%s\n' "$!" >"$INTENTD_DATA_DIR/intentd.pid"
    printf '%s\n' "$!" >>"$state/daemons" ;;
  pair)
    case " $* " in
      *" --yes "*) ;;
      *)
        printf 'Selection [1]: ' >&2
        read -r selection || selection=EOF
        printf '%s\n' "$selection" >"$state/pair-selection" ;;
    esac
    printf '%s\n' '█▀▀▀▀▀█ ▄▀ █▀▀▀▀▀█' '█ ███ █ ▀▄ █ ███ █' ''
    case " $* " in
      *" --node "*)
        printf '%s\n' 'URL:         intent://node?v=1&host=&port=5181&fp=FP&invite=ni-1&secret=s&tc=tc-fake' ;;
      *)
        printf '%s\n' 'URL:         intent://pair?v=1&host=&port=5180&token=SECRET-TOKEN&fp=FP&tc=tc-fake' \
          '             Same payload as the QR code.' \
          'Token:       SECRET-TOKEN' \
          'Fingerprint: FP' ;;
    esac ;;
esac
exit 0
"#;

struct Run {
    output: Output,
    tty: String,
    calls: Vec<String>,
    state: PathBuf,
    _dir: tempfile::TempDir,
}

impl Run {
    fn stdout(&self) -> String {
        String::from_utf8_lossy(&self.output.stdout).into_owned()
    }

    fn stderr(&self) -> String {
        String::from_utf8_lossy(&self.output.stderr).into_owned()
    }

    fn called(&self, call: &str) -> bool {
        self.calls.iter().any(|c| c == call)
    }

    #[track_caller]
    fn position(&self, call: &str) -> usize {
        self.calls
            .iter()
            .position(|c| c == call)
            .unwrap_or_else(|| panic!("`intentd {call}` was not run; calls: {:?}", self.calls))
    }

    fn describe(&self) -> String {
        format!(
            "status: {:?}\n--- terminal ---\n{}\n--- stdout ---\n{}\n--- stderr ---\n{}\n--- calls ---\n{}",
            self.output.status,
            self.tty,
            self.stdout(),
            self.stderr(),
            self.calls.join("\n")
        )
    }
}

impl Drop for Run {
    fn drop(&mut self) {
        if let Ok(pids) = fs::read_to_string(self.state.join("daemons")) {
            for pid in pids.lines() {
                let _ = Command::new("kill").arg(pid).status();
            }
        }
    }
}

fn write_executable(path: &Path, body: &str) {
    fs::write(path, body).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
}

#[track_caller]
fn sh(dir: &Path, command: &str) {
    let status = Command::new("sh")
        .arg("-c")
        .arg(command)
        .current_dir(dir)
        .status()
        .unwrap();
    assert!(status.success(), "fixture command failed: {command}");
}

/// The release the fake `curl` serves: the archive with the fake `intentd`
/// and its sha256 sidecar, laid out as the sitter-latest release.
fn build_release(dir: &Path) -> PathBuf {
    let triple = TRIPLE;
    let release = dir.join("release");
    let payload = release.join(format!("intentd-{triple}"));
    fs::create_dir_all(&payload).unwrap();
    write_executable(&payload.join("intentd"), FAKE_INTENTD);
    let archive = format!("intentd-{triple}.tar.xz");
    sh(
        &release,
        &format!(
            "tar -cf - intentd-{triple} | xz -c >{archive} && sha256sum {archive} >{archive}.sha256"
        ),
    );
    release
}

/// `loginctl` keeps lingering in `$FAKE_STATE/linger`: `FAKE_LINGER=yes`
/// starts it on, and `enable-linger` without sudo works only when
/// `FAKE_LINGER_ALLOWED` is set (most servers refuse it). Calls are recorded
/// in `$FAKE_STATE/loginctl`.
const FAKE_LOGINCTL: &str = r#"#!/bin/sh
printf '%s\n' "$*" >>"$FAKE_STATE/loginctl"
[ "${FAKE_LINGER:-}" = yes ] && : >"$FAKE_STATE/linger"
case "$1" in
  show-user)
    if [ -f "$FAKE_STATE/linger" ]; then echo Linger=yes; else echo Linger=no; fi ;;
  enable-linger)
    if [ -n "${FAKE_LINGER_ALLOWED:-}" ]; then : >"$FAKE_STATE/linger"; else
      echo 'Could not enable linger: Access denied' >&2; exit 1; fi ;;
esac
"#;

fn build_stubs(dir: &Path) -> PathBuf {
    let bin = dir.join("stubs");
    fs::create_dir_all(&bin).unwrap();
    write_executable(
        &bin.join("curl"),
        r#"#!/bin/sh
printf '%s\n' "$*" >>"$FAKE_STATE/downloads"
out=""; url=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    -o) out="$2"; shift 2 ;;
    *) url="$1"; shift ;;
  esac
done
cp "$FAKE_RELEASE/${url##*/}" "$out"
"#,
    );
    write_executable(
        &bin.join("uname"),
        "#!/bin/sh\ncase \"$1\" in -m) echo x86_64 ;; *) echo Linux ;; esac\n",
    );
    write_executable(
        &bin.join("systemctl"),
        "#!/bin/sh\ncase \"$*\" in *show*MainPID*) echo MainPID=0 ;; esac\nexit 0\n",
    );
    write_executable(&bin.join("loginctl"), FAKE_LOGINCTL);
    bin
}

#[derive(Clone, Copy)]
enum Terminal<'a> {
    /// A terminal that answers with these lines, one per read.
    Answers(&'a str),
    /// No terminal at all (CI, provisioning).
    None,
}

/// Run the shipped install.sh with `args` and `envs`, answering the terminal
/// as `terminal` says. `node_already` seeds a daemon whose saved
/// `daemon.mode` is `node`.
fn run_install(
    terminal: Terminal,
    args: &[&str],
    envs: &[(&str, &str)],
    node_already: bool,
) -> Run {
    let envs: Vec<(&str, &str)> = [("FAKE_LINGER", "yes")]
        .into_iter()
        .chain(envs.iter().copied())
        .collect();
    run_install_on(terminal, args, &envs, node_already)
}

fn run_install_on(
    terminal: Terminal,
    args: &[&str],
    envs: &[(&str, &str)],
    node_already: bool,
) -> Run {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let state = root.join("state");
    let home = root.join("home");
    let data = root.join("data");
    let install_dir = root.join("bin");
    for d in [&state, &home, &data] {
        fs::create_dir_all(d).unwrap();
    }
    if node_already {
        fs::write(state.join("setting.daemon.mode"), "node").unwrap();
    }
    let release = build_release(root);
    let stubs = build_stubs(root);
    let tty_out = root.join("tty.out");
    fs::write(&tty_out, "").unwrap();

    let script =
        fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("../../scripts/install.sh"))
            .unwrap();
    assert!(script.contains("</dev/tty") && script.contains(">/dev/tty"));
    let script = match terminal {
        Terminal::Answers(_) => script
            .replace(">/dev/tty", &format!(">>'{}'", tty_out.display()))
            .replace("</dev/tty", "</dev/stdin"),
        Terminal::None => script.replace("/dev/tty", &root.join("no-tty").display().to_string()),
    };
    let script_path = root.join("install.sh");
    fs::write(&script_path, script).unwrap();

    let path = format!(
        "{}:{}",
        stubs.display(),
        std::env::var("PATH").unwrap_or_default()
    );
    let mut command = Command::new("sh");
    command
        .arg(&script_path)
        .args(args)
        .current_dir(root)
        .env_clear()
        .env("PATH", path)
        .env("HOME", &home)
        .env("INTENTD_DATA_DIR", &data)
        .env("INTENTD_INSTALL_DIR", &install_dir)
        .env("INTENTD_SERVICE_NAME", "intentd-install-test")
        .env("INTENTD_RESTART_TIMEOUT", "20")
        .env("FAKE_STATE", &state)
        .env("FAKE_RELEASE", &release)
        .envs(envs.iter().copied())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command.spawn().unwrap();
    {
        let mut stdin = child.stdin.take().unwrap();
        if let Terminal::Answers(answers) = terminal {
            stdin.write_all(answers.as_bytes()).unwrap();
        }
    }
    let output = child.wait_with_output().unwrap();
    let calls = fs::read_to_string(state.join("calls"))
        .unwrap_or_default()
        .lines()
        .map(str::to_string)
        .collect();
    Run {
        output,
        tty: fs::read_to_string(&tty_out).unwrap_or_default(),
        calls,
        state,
        _dir: dir,
    }
}

const QUESTION: [&str; 4] = [
    "How will you use this computer?",
    "1) Run Intent here (recommended if you're just getting started): workspaces, agents and the app connect to it.",
    "2) Node only: lets another Intent run agents on this computer; nothing else runs here.",
    "Choose 1 or 2 [1]: ",
];

fn mode_writes(run: &Run) -> Vec<&String> {
    run.calls
        .iter()
        .filter(|c| {
            c.starts_with("settings daemon.mode ") || c.starts_with("settings server.tunnel")
        })
        .collect()
}

#[test]
fn interactive_full_install_asks_first_and_changes_nothing() {
    // Enter at the mode question, the service question and auto-resume.
    let run = run_install(Terminal::Answers("\n\n\n"), &[], &[], false);
    assert!(run.output.status.success(), "{}", run.describe());
    for line in QUESTION {
        assert!(
            run.tty.contains(line),
            "missing `{line}`\n{}",
            run.describe()
        );
    }
    assert!(
        run.tty.find(QUESTION[0]) < run.tty.find("Set up intentd to start at login"),
        "the mode question comes first\n{}",
        run.describe()
    );
    assert!(mode_writes(&run).is_empty(), "{}", run.describe());
    assert!(!run.called("restart") && !run.calls.iter().any(|c| c.starts_with("pair")));
    assert!(
        run.stdout().contains("Run: intentd pair"),
        "{}",
        run.describe()
    );
}

#[test]
fn interactive_node_install_restarts_then_pairs_through_the_picker() {
    // 2 at the mode question, Enter at service and auto-resume, then the
    // listen-target picker inside `intentd pair` takes "1".
    let run = run_install(Terminal::Answers("2\n\n\n1\n"), &[], &[], false);
    assert!(run.output.status.success(), "{}", run.describe());
    let node = run.position("settings daemon.mode node");
    let tunnel = run.position("settings server.tunnel.enabled true");
    let restart = run.position("restart");
    let pair = run.position("pair --node --tunnel");
    assert!(
        node < restart && tunnel < restart && restart < pair,
        "{}",
        run.describe()
    );
    assert_eq!(
        fs::read_to_string(run.state.join("pair-selection"))
            .unwrap()
            .trim(),
        "1",
        "the picker reads the terminal\n{}",
        run.describe()
    );
    let stdout = run.stdout();
    assert!(
        stdout.contains("restarting the daemon in node mode"),
        "{}",
        run.describe()
    );
    assert!(
        stdout.contains("intent://node?") && !stdout.contains("token="),
        "{}",
        run.describe()
    );
    assert!(!stdout.contains("Run: intentd pair"), "{}", run.describe());
}

#[test]
fn interactive_answer_is_asked_again_when_unrecognized() {
    let run = run_install(Terminal::Answers("maybe\nnode\n\n\n\n"), &[], &[], false);
    assert!(run.output.status.success(), "{}", run.describe());
    assert!(
        run.stderr().contains("unrecognized answer 'maybe'"),
        "{}",
        run.describe()
    );
    assert_eq!(
        run.tty.matches(QUESTION[3]).count(),
        2,
        "{}",
        run.describe()
    );
    assert!(
        run.called("settings daemon.mode node"),
        "{}",
        run.describe()
    );
}

#[test]
fn scripted_node_install_sets_up_the_service_and_pairs_unattended() {
    let run = run_install(
        Terminal::None,
        &[],
        &[("INTENTD_DAEMON_MODE", "Node")],
        false,
    );
    assert!(run.output.status.success(), "{}", run.describe());
    assert!(run.tty.is_empty(), "nothing is asked\n{}", run.describe());
    assert!(
        run.called("status"),
        "the service was started\n{}",
        run.describe()
    );
    let restart = run.position("restart");
    let pair = run.position("pair --node --tunnel --yes");
    assert!(run.position("settings daemon.mode node") < restart && restart < pair);
    assert!(
        run.stdout().contains("intent://node?"),
        "{}",
        run.describe()
    );
}

#[test]
fn scripted_node_flag_beats_the_env_var() {
    let run = run_install(
        Terminal::None,
        &["--daemon-mode=node", "--no-service"],
        &[("INTENTD_DAEMON_MODE", "full")],
        false,
    );
    assert!(run.output.status.success(), "{}", run.describe());
    let stdout = run.stdout();
    assert!(
        stdout.contains("Next steps for a node:"),
        "{}",
        run.describe()
    );
    assert!(
        stdout.contains("intentd serve --node") && stdout.contains("intentd pair --node --tunnel")
    );
    assert!(
        !run.called("status") && mode_writes(&run).is_empty(),
        "{}",
        run.describe()
    );
}

#[test]
fn a_bad_daemon_mode_fails_before_downloading() {
    let run = run_install(
        Terminal::None,
        &[],
        &[("INTENTD_DAEMON_MODE", "server")],
        false,
    );
    assert!(!run.output.status.success(), "{}", run.describe());
    assert!(
        run.stderr()
            .contains("invalid daemon mode 'server' (expected full or node)"),
        "{}",
        run.describe()
    );
    assert!(!run.state.join("downloads").exists(), "{}", run.describe());
}

#[test]
fn an_explicit_full_answer_switches_a_node_back() {
    let run = run_install(
        Terminal::None,
        &["--service", "--daemon-mode=full"],
        &[],
        true,
    );
    assert!(run.output.status.success(), "{}", run.describe());
    assert!(run.position("settings daemon.mode regular") < run.position("restart"));
    assert!(
        run.stdout().contains("runs Intent again"),
        "{}",
        run.describe()
    );
    assert!(
        !run.calls.iter().any(|c| c.starts_with("pair")),
        "{}",
        run.describe()
    );
}

#[test]
fn enter_on_a_node_keeps_it_and_a_typed_1_switches_it_back() {
    let enter = run_install(Terminal::Answers("\n\n\n"), &[], &[], true);
    assert!(enter.output.status.success(), "{}", enter.describe());
    assert!(
        !enter
            .calls
            .iter()
            .any(|c| c.starts_with("settings daemon.mode"))
            && !enter.called("restart"),
        "{}",
        enter.describe()
    );
    let typed = run_install(Terminal::Answers("1\n\n\n"), &[], &[], true);
    assert!(typed.output.status.success(), "{}", typed.describe());
    assert!(
        typed.position("settings daemon.mode regular") < typed.position("restart"),
        "{}",
        typed.describe()
    );
}

#[test]
fn the_unasked_default_leaves_a_node_alone() {
    let run = run_install(Terminal::None, &["--service"], &[], true);
    assert!(run.output.status.success(), "{}", run.describe());
    assert!(
        !run.calls
            .iter()
            .any(|c| c.starts_with("settings daemon.mode")),
        "{}",
        run.describe()
    );
    assert!(!run.called("restart"), "{}", run.describe());
}

const PAIR_URI: &str = "intent://pair?v=1&host=&port=5180&token=SECRET-TOKEN&fp=FP&tc=tc-fake";
const NODE_URI: &str = "intent://node?v=1&host=&port=5181&fp=FP&invite=ni-1&secret=s&tc=tc-fake";

fn recorded(run: &Run, tool: &str) -> Vec<String> {
    fs::read_to_string(run.state.join(tool))
        .unwrap_or_default()
        .lines()
        .map(str::to_string)
        .collect()
}

fn user() -> String {
    String::from_utf8(Command::new("id").arg("-un").output().unwrap().stdout)
        .unwrap()
        .trim()
        .to_string()
}

#[test]
fn the_pair_option_pairs_a_full_install_and_shows_the_link_without_a_terminal() {
    for (args, envs) in [
        (&["--pair"][..], &[("INTENTD_INSTALL_SERVICE", "1")][..]),
        (
            &[][..],
            &[("INTENTD_INSTALL_SERVICE", "1"), ("INTENTD_PAIR", "1")][..],
        ),
    ] {
        let run = run_install(Terminal::None, args, envs, false);
        assert!(run.output.status.success(), "{}", run.describe());
        assert!(
            run.position("status") < run.position("pair --yes --tunnel"),
            "pairs once the daemon answers\n{}",
            run.describe()
        );
        let stdout = run.stdout();
        assert!(
            stdout.contains(&format!("URL:         {PAIR_URI}")),
            "the normal pair output is shown\n{}",
            run.describe()
        );
        assert!(!stdout.contains("INTENT_LINK="), "{}", run.describe());
        assert!(!stdout.contains("Run: intentd pair"), "{}", run.describe());
        assert!(mode_writes(&run).is_empty(), "{}", run.describe());
    }
}

#[test]
fn without_the_pair_option_a_full_install_pairs_nothing() {
    let run = run_install(
        Terminal::None,
        &[],
        &[("INTENTD_INSTALL_SERVICE", "1")],
        false,
    );
    assert!(run.output.status.success(), "{}", run.describe());
    assert!(run.called("status"), "{}", run.describe());
    assert!(
        !run.calls.iter().any(|c| c.starts_with("pair")),
        "{}",
        run.describe()
    );
    assert!(
        run.stdout().contains("Run: intentd pair"),
        "{}",
        run.describe()
    );
}

#[test]
fn the_pair_option_without_a_service_says_how_to_pair_later() {
    let run = run_install(
        Terminal::None,
        &["--pair"],
        &[("INTENTD_INSTALL_SERVICE", "0")],
        false,
    );
    assert!(run.output.status.success(), "{}", run.describe());
    assert!(
        !run.calls.iter().any(|c| c.starts_with("pair")),
        "{}",
        run.describe()
    );
    assert!(
        run.stderr().contains("pairing needs the service"),
        "{}",
        run.describe()
    );
}

#[track_caller]
fn assert_only_the_marker(run: &Run, uri: &str) {
    let stdout = run.stdout();
    let markers: Vec<&str> = stdout
        .lines()
        .filter(|l| l.starts_with("INTENT_LINK="))
        .collect();
    assert_eq!(
        markers,
        vec![format!("INTENT_LINK={uri}").as_str()],
        "{}",
        run.describe()
    );
    let all = format!("{stdout}{}", run.stderr());
    for line in all.lines().filter(|l| !l.starts_with("INTENT_LINK=")) {
        assert!(
            !line.contains("intent://")
                && !line.contains("SECRET-TOKEN")
                && !line.contains('█')
                && !line.starts_with("URL:")
                && !line.starts_with("Token:"),
            "only the marker line may carry the link: `{line}`\n{}",
            run.describe()
        );
    }
}

#[test]
fn the_link_marker_prints_one_link_line_and_no_credentials_for_a_full_install() {
    let run = run_install(
        Terminal::None,
        &["--pair"],
        &[
            ("INTENTD_INSTALL_SERVICE", "1"),
            ("INTENTD_LINK_MARKER", "1"),
        ],
        false,
    );
    assert!(run.output.status.success(), "{}", run.describe());
    assert!(run.called("pair --yes --tunnel"), "{}", run.describe());
    assert_only_the_marker(&run, PAIR_URI);
}

#[test]
fn the_link_marker_prints_one_link_line_for_a_node_even_on_a_terminal() {
    for terminal in [Terminal::None, Terminal::Answers("\n\n\n")] {
        let run = run_install(
            terminal,
            &[],
            &[
                ("INTENTD_INSTALL_SERVICE", "1"),
                ("INTENTD_DAEMON_MODE", "node"),
                ("INTENTD_LINK_MARKER", "1"),
            ],
            false,
        );
        assert!(run.output.status.success(), "{}", run.describe());
        assert!(
            run.called("pair --node --tunnel --yes"),
            "the marker never opens the picker\n{}",
            run.describe()
        );
        assert_only_the_marker(&run, NODE_URI);
    }
}

#[test]
fn linux_service_setup_turns_lingering_on_without_sudo_when_allowed() {
    let run = run_install_on(
        Terminal::None,
        &["--service"],
        &[("FAKE_LINGER_ALLOWED", "1")],
        false,
    );
    assert!(run.output.status.success(), "{}", run.describe());
    assert!(
        recorded(&run, "loginctl").contains(&format!("enable-linger {}", user())),
        "{}",
        run.describe()
    );
    assert!(
        !run.stdout().contains("sudo loginctl enable-linger"),
        "{}",
        run.describe()
    );
}

#[test]
fn linux_service_setup_prints_the_sudo_step_when_lingering_is_refused() {
    let run = run_install_on(Terminal::None, &["--service"], &[], false);
    assert!(run.output.status.success(), "{}", run.describe());
    assert!(
        recorded(&run, "loginctl").contains(&format!("enable-linger {}", user())),
        "{}",
        run.describe()
    );
    assert!(
        run.stdout()
            .contains(&format!("sudo loginctl enable-linger {}", user())),
        "{}",
        run.describe()
    );
}

#[test]
fn linux_service_setup_leaves_lingering_alone_when_it_is_on() {
    let run = run_install(Terminal::None, &["--service"], &[], false);
    assert!(run.output.status.success(), "{}", run.describe());
    assert!(
        !recorded(&run, "loginctl")
            .iter()
            .any(|c| c.starts_with("enable-linger")),
        "{}",
        run.describe()
    );
}
