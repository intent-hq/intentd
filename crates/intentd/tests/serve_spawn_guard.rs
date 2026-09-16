//! Source-scan guard: e2e suites spawn `intentd serve` only through the shared
//! builders in `crates/intentd/tests/common/mod.rs`.
//!
//! `common::serve_command()` owns the `INTENTD_TCP_PORT=0` ephemeral-port seam
//! (monorepo#1051); `common::serve_command_fixed_port()` is the explicit opt-in
//! for suites that must bind the settings-file port. A file-local
//! `Command::new(env!("CARGO_BIN_EXE_intentd")).arg("serve")` bypasses the seam
//! and races other daemons for the seeded port (intentd#1924, thread
//! r4022009875) — this test fails naming the offending `file:line`s so the
//! race class cannot return silently.
//!
//! Rules, applied to every `.rs` under `crates/intentd/tests/` except the
//! builder module and this guard:
//!
//! 1. A line containing `Command::new(env!("CARGO_BIN_EXE_intentd"))`
//!    (whitespace-insensitive; std and tokio forms alike) is an offender when
//!    the statement it starts — from that line through the first line whose
//!    code part ends with `;` or `{`, capped at 30 lines — contains the literal
//!    `"serve"`. Non-`serve` CLI invocations (`doctor`, `status`, `stop`, …)
//!    are not flagged.
//! 2. A file that calls `enable_ws_api(` must also mention `serve_command`;
//!    this catches launchers that bind the binary path to a variable and add
//!    `"serve"` in a later statement.
//!
//! A deliberate exception opts out with `// serve-spawn: allow — <reason>`: on
//! the `Command::new` line for rule 1, or on any line of the file for rule 2.
//! The reason is required; a marker without one is itself an offender.

use std::fs;
use std::path::{Path, PathBuf};

const ALLOW_MARKER: &str = "// serve-spawn: allow";
const SPAWN: &str = r#"Command::new(env!("CARGO_BIN_EXE_intentd"))"#;
const SERVE_LITERAL: &str = "\"serve\"";
const MAX_STATEMENT_LINES: usize = 30;

/// The builder module (defines the only sanctioned spawns) and this guard
/// (whose docs quote the patterns).
const EXEMPT_FILES: &[&str] = &[
    "crates/intentd/tests/common/mod.rs",
    "crates/intentd/tests/serve_spawn_guard.rs",
];

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("canonicalize workspace root")
}

fn collect_rs_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_rs_files(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

fn scanned_files(root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    collect_rs_files(&root.join("crates/intentd/tests"), &mut files);
    files.retain(|p| {
        let rel = p.strip_prefix(root).unwrap_or(p).to_string_lossy();
        !EXEMPT_FILES.contains(&rel.as_ref())
    });
    files.sort();
    files
}

fn strip_ws(s: &str) -> String {
    s.chars().filter(|c| !c.is_whitespace()).collect()
}

fn code_part(line: &str) -> &str {
    line.split("//").next().unwrap_or(line)
}

fn ends_statement(line: &str) -> bool {
    let code = code_part(line).trim_end();
    code.ends_with(';') || code.ends_with('{')
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Marker {
    Absent,
    /// Starts like the marker but lacks the ` — <reason>` tail.
    Malformed,
    WithReason,
}

/// Marker state of one line: `Absent` unless it contains the marker token;
/// `WithReason` only when the token is followed by whitespace, an em dash or
/// hyphen, and a nonempty reason.
fn classify_marker(line: &str) -> Marker {
    let Some(idx) = line.find(ALLOW_MARKER) else {
        return Marker::Absent;
    };
    let rest = &line[idx + ALLOW_MARKER.len()..];
    if rest
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_alphanumeric() || c == '_')
    {
        return Marker::Malformed;
    }
    let after_space = rest.trim_start();
    if after_space.len() == rest.len() && !rest.is_empty() {
        return Marker::Malformed;
    }
    let Some(reason) = after_space
        .strip_prefix('—')
        .or_else(|| after_space.strip_prefix('-'))
    else {
        return Marker::Malformed;
    };
    if reason.trim().is_empty() {
        Marker::Malformed
    } else {
        Marker::WithReason
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Offense {
    /// Rule 1: a raw `intentd serve` spawn statement starting on `line`.
    RawServeSpawn { line: usize },
    /// An allow marker without a reason on `line`.
    MalformedMarker { line: usize },
    /// Rule 2: the file enables the WSS listener but never names the builder.
    MissingBuilder,
}

impl Offense {
    fn describe(&self, rel: &str) -> String {
        match self {
            Offense::RawServeSpawn { line } => {
                format!("{rel}:{line}: raw `intentd serve` spawn")
            }
            Offense::MalformedMarker { line } => format!(
                "{rel}:{line}: malformed opt-out marker (expected `{ALLOW_MARKER} — <reason>`)"
            ),
            Offense::MissingBuilder => format!(
                "{rel}: calls `enable_ws_api(` but never uses `serve_command` — \
                 the daemon is spawned some other way"
            ),
        }
    }
}

/// The statement starting at `lines[start]`: that line through the first line
/// whose code part ends with `;` or `{`, capped at [`MAX_STATEMENT_LINES`].
/// Comments are stripped so a mention of `"serve"` in prose does not count.
fn statement_code(lines: &[&str], start: usize) -> String {
    let mut out = String::new();
    for line in lines.iter().skip(start).take(MAX_STATEMENT_LINES) {
        out.push_str(code_part(line));
        out.push('\n');
        if ends_statement(line) {
            break;
        }
    }
    out
}

/// Classify one test source file. Offenses are in line order; the file-level
/// [`Offense::MissingBuilder`] comes last.
fn classify(src: &str) -> Vec<Offense> {
    let lines: Vec<&str> = src.lines().collect();
    let mut offenses = Vec::new();
    let mut has_reasoned_marker = false;
    for (i, line) in lines.iter().enumerate() {
        let marker = classify_marker(line);
        match marker {
            Marker::Malformed => offenses.push(Offense::MalformedMarker { line: i + 1 }),
            Marker::WithReason => has_reasoned_marker = true,
            Marker::Absent => {}
        }
        if !strip_ws(code_part(line)).contains(SPAWN) || marker == Marker::WithReason {
            continue;
        }
        if statement_code(&lines, i).contains(SERVE_LITERAL) {
            offenses.push(Offense::RawServeSpawn { line: i + 1 });
        }
    }
    if src.contains("enable_ws_api(") && !src.contains("serve_command") && !has_reasoned_marker {
        offenses.push(Offense::MissingBuilder);
    }
    offenses
}

fn scan(root: &Path) -> Vec<String> {
    let mut report = Vec::new();
    for file in scanned_files(root) {
        let src = fs::read_to_string(&file).expect("read test source");
        let rel = file
            .strip_prefix(root)
            .unwrap_or(&file)
            .display()
            .to_string();
        report.extend(classify(&src).iter().map(|o| o.describe(&rel)));
    }
    report
}

#[test]
fn e2e_suites_spawn_serve_through_the_shared_builder() {
    let root = workspace_root();
    let files = scanned_files(&root);
    assert!(
        files.len() > 100,
        "guard scanned only {} files — file discovery is broken",
        files.len()
    );
    let offenders = scan(&root);
    assert!(
        offenders.is_empty(),
        "\n`intentd serve` spawns bypassing the shared builder ({} site{}):\n  {}\n\n\
         Spawn the daemon with `common::serve_command()` (crates/intentd/tests/common/mod.rs), \
         which carries the INTENTD_TCP_PORT=0 ephemeral-port seam, or \
         `common::serve_command_fixed_port()` when the test must bind the settings-file port. \
         Callers keep adding their own data dir, token, stdio and env. A deliberate wrapper-program \
         launcher opts out with `{ALLOW_MARKER} — <reason>` on the `Command::new` line (or any line \
         of the file for the enable_ws_api rule); the reason is required.\n",
        offenders.len(),
        if offenders.len() == 1 { "" } else { "s" },
        offenders.join("\n  "),
    );
}

mod classifier {
    use super::*;

    const SINGLE_LINE: &str = r#"
fn spawn_serve(data_dir: &Path) -> Child {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_intentd")).arg("serve").spawn().unwrap();
    cmd
}
"#;

    const MULTI_LINE: &str = r#"
fn spawn_serve(data_dir: &Path) -> Child {
    Command::new(env!("CARGO_BIN_EXE_intentd"))
        .arg("serve")
        .env("INTENTD_DATA_DIR", data_dir)
        .stdout(Stdio::null())
        .spawn()
        .expect("spawn intentd")
}
"#;

    const TOKIO: &str = r#"
async fn spawn() {
    let child = tokio::process::Command::new(env!("CARGO_BIN_EXE_intentd"))
        .args(["serve", "--resume-all"])
        .kill_on_drop(true)
        .spawn()?;
}
"#;

    #[test]
    fn single_line_serve_spawn_is_flagged() {
        assert_eq!(
            classify(SINGLE_LINE),
            vec![Offense::RawServeSpawn { line: 3 }]
        );
    }

    #[test]
    fn multi_line_statement_is_flagged_at_its_first_line() {
        assert_eq!(
            classify(MULTI_LINE),
            vec![Offense::RawServeSpawn { line: 3 }]
        );
    }

    #[test]
    fn tokio_form_is_flagged() {
        assert_eq!(classify(TOKIO), vec![Offense::RawServeSpawn { line: 3 }]);
    }

    #[test]
    fn whitespace_inside_the_constructor_call_is_ignored() {
        let src = "let c = Command::new( env!( \"CARGO_BIN_EXE_intentd\" ) ).arg(\"serve\");\n";
        assert_eq!(classify(src), vec![Offense::RawServeSpawn { line: 1 }]);
    }

    #[test]
    fn non_serve_cli_invocations_are_not_flagged() {
        let src = r#"
fn doctor(data_dir: &Path) -> Output {
    Command::new(env!("CARGO_BIN_EXE_intentd"))
        .arg("doctor")
        .env("INTENTD_TCP_PORT", "0")
        .output()
        .expect("run doctor")
}
let status = Command::new(env!("CARGO_BIN_EXE_intentd")).args(["status"]).output();
let stop = std::process::Command::new(env!("CARGO_BIN_EXE_intentd")).arg("stop").status();
"#;
        assert_eq!(classify(src), Vec::<Offense>::new());
    }

    #[test]
    fn serve_in_a_comment_or_later_statement_does_not_count() {
        let src = r#"
let mut cmd = Command::new(env!("CARGO_BIN_EXE_intentd")); // we will add "serve" below
cmd.arg("serve");
"#;
        assert_eq!(classify(src), Vec::<Offense>::new());
    }

    #[test]
    fn statement_scan_is_capped_at_thirty_lines() {
        let mut src = String::from("Command::new(env!(\"CARGO_BIN_EXE_intentd\"))\n");
        for _ in 0..MAX_STATEMENT_LINES {
            src.push_str("    .env(\"K\", \"v\")\n");
        }
        src.push_str("    .arg(\"serve\");\n");
        assert_eq!(classify(&src), Vec::<Offense>::new());
    }

    #[test]
    fn allow_marker_with_a_reason_is_honored() {
        let src = r#"
let mut cmd = Command::new(env!("CARGO_BIN_EXE_intentd")); // serve-spawn: allow — argv probe, never spawned
cmd.arg("serve");
let dashed = Command::new(env!("CARGO_BIN_EXE_intentd")).arg("serve"); // serve-spawn: allow - hyphen form
"#;
        assert_eq!(classify(src), Vec::<Offense>::new());
    }

    #[test]
    fn allow_marker_without_a_reason_is_rejected() {
        for tail in [
            "// serve-spawn: allow",
            "// serve-spawn: allow —",
            "// serve-spawn: allow -",
            "// serve-spawn: allow —   ",
            "// serve-spawn: allow reason without a dash",
            "// serve-spawn: allow—no space before the dash",
            "// serve-spawn: allowance — longer token",
        ] {
            let src =
                format!("Command::new(env!(\"CARGO_BIN_EXE_intentd\")).arg(\"serve\"); {tail}\n");
            assert_eq!(
                classify(&src),
                vec![
                    Offense::MalformedMarker { line: 1 },
                    Offense::RawServeSpawn { line: 1 },
                ],
                "{tail:?}"
            );
        }
    }

    #[test]
    fn enable_ws_api_without_the_builder_is_flagged() {
        let src = r#"
fn spawn(data_dir: &Path) -> Child {
    common::enable_ws_api(data_dir);
    let bin = env!("CARGO_BIN_EXE_intentd");
    Command::new(bin).arg("serve").spawn().unwrap()
}
"#;
        assert_eq!(classify(src), vec![Offense::MissingBuilder]);
    }

    #[test]
    fn enable_ws_api_with_either_builder_passes() {
        let default = "common::enable_ws_api(&d);\nlet mut cmd = common::serve_command();\n";
        let fixed =
            "common::enable_ws_api(&d);\nlet mut cmd = common::serve_command_fixed_port();\n";
        assert_eq!(classify(default), Vec::<Offense>::new());
        assert_eq!(classify(fixed), Vec::<Offense>::new());
    }

    #[test]
    fn enable_ws_api_rule_honors_a_reasoned_marker_anywhere_in_the_file() {
        let src = "// serve-spawn: allow — bash driver execs $INTENTD_BIN serve\n\
                   common::enable_ws_api(&d);\ncmd.env(\"INTENTD_BIN\", env!(\"CARGO_BIN_EXE_intentd\"));\n";
        assert_eq!(classify(src), Vec::<Offense>::new());
        let unreasoned = "// serve-spawn: allow\ncommon::enable_ws_api(&d);\n";
        assert_eq!(
            classify(unreasoned),
            vec![
                Offense::MalformedMarker { line: 1 },
                Offense::MissingBuilder
            ]
        );
    }

    #[test]
    fn non_spawn_uses_of_the_binary_path_are_not_flagged() {
        let src = r#"
let agent = MockAgent::new().with_mcp_bridge_exe(env!("CARGO_BIN_EXE_intentd"));
cmd.env("INTENTD_BIN", env!("CARGO_BIN_EXE_intentd"));
assert_eq!(cmd.get_program(), env!("CARGO_BIN_EXE_intentd"), "serve");
"#;
        assert_eq!(classify(src), Vec::<Offense>::new());
    }
}
