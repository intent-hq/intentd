//! Source guard for the complete daemon fixture constructor and port seam.
//!
//! Scans all intentd test Rust sources except `common/mod.rs` (the implementation)
//! and this lint. Shared lexing excludes comments/quoted examples and decodes
//! literals. Shared statement splitting handles multiline/chained calls with
//! no line cap. Calls to retired partial builders are always rejected.
//!
//! The bounded command analysis follows simple `let [mut] name = constructor`
//! bindings and subsequent `name.method(...)` statements, in source order until
//! the next function or rebinding. It recognizes direct intentd binary commands,
//! both hermetic builders, and wrappers setting `INTENTD_BIN`. Each raw serve
//! command needs the port marker; each daemon command independently needs complete
//! identity. Identity env writes/removals, `env_clear` and dynamic env/envs invalidate
//! identity until an explicit reset on that same variable, before spawn or return.
//! Removing either token is harmless. Named mock helpers require a local reason.
//!
//! `// serve-spawn: allow — reason` permits only raw/wrapper launch mechanics,
//! never identity inheritance. `// fixture-identity: allow — reason` authorizes
//! only an allowlisted mock helper call on that line or immediately below it;
//! it does not exempt arbitrary env writes or other commands in the file.
//!
//! This is not Rust name resolution or control-flow analysis: aliases, fields,
//! command factories passed between functions, macro expansion, conditional resets,
//! shadowing across nested blocks, and mutations inside opaque helpers are outside
//! the contract. Source order cannot prove execution order or private path ownership;
//! constructor/mock contract tests supply the latter. The WSS file-level backstop
//! still catches files with no actual builder call; a safe call elsewhere cannot
//! excuse a recognized raw/partial command or identity mutation.

use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::{Path, PathBuf};

use intentd_test_support::source_lint::{
    classify_marker, lex, rust_files, split_statements, word_at, workspace_root, Literal, Marker,
};

const ALLOW_MARKER: &str = "// serve-spawn: allow";
const BUILDERS: &[&str] = &[
    "hermetic_serve_command",
    "hermetic_serve_command_fixed_port",
];
const RETIRED: &[&str] = &[
    "serve_command",
    "serve_command_fixed_port",
    "hermetic_github_identity",
];
const MOCK_HELPERS: &[&str] = &["mock_github_token"];
const RESET_HELPERS: &[&str] = &["hermetic_fixture_identity", "hermetic_pty_fixture_identity"];
const IDENTITY_KEYS: &[&str] = &[
    "GITHUB_TOKEN",
    "GH_TOKEN",
    "GH_CONFIG_DIR",
    "INTENTD_SECRETS_FILE",
];
const EXEMPT_FILES: &[&str] = &[
    "crates/intentd/tests/common/mod.rs",
    "crates/intentd/tests/serve_spawn_lint.rs",
];

fn scanned_files(root: &Path) -> Vec<PathBuf> {
    rust_files(&root.join("crates/intentd/tests"))
        .into_iter()
        .filter(|p| {
            !EXEMPT_FILES.contains(&p.strip_prefix(root).unwrap().to_string_lossy().as_ref())
        })
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Offense {
    RawServeSpawn { line: usize },
    MalformedMarker { line: usize },
    MissingBuilder { line: usize },
    IncompleteIdentity { line: usize },
    UnreasonedMock { line: usize },
    UnusedIdentityMarker { line: usize },
}

impl Offense {
    fn describe(&self, rel: &str) -> String {
        let (line, message) = match self {
            Self::RawServeSpawn { line } => (*line, "raw `intentd serve` spawn bypasses the port builder"),
            Self::MalformedMarker { line } => (*line, "malformed opt-out marker: expected `allow — <reason>`"),
            Self::MissingBuilder { line } => (*line, "calls enable_ws_api without common::hermetic_serve_command / hermetic_serve_command_fixed_port"),
            Self::IncompleteIdentity { line } => (*line, "incomplete fixture identity: use the complete constructor or restore hermetic_fixture_identity after overrides"),
            Self::UnreasonedMock { line } => (*line, "named mock identity call needs local `// fixture-identity: allow — <private mock reason>`"),
            Self::UnusedIdentityMarker { line } => (*line, "identity marker must annotate an approved named mock helper call"),
        };
        format!("{rel}:{line}: {message}")
    }
}

#[derive(Debug)]
struct Token {
    text: String,
    line: usize,
}

// Rule-specific tokens over the shared lexer output. Literals have already been
// blanked; a one-character sentinel retains their position for statement splitting.
fn statement_tokens(text: &str, first_line: usize, literals: &[String]) -> Vec<Token> {
    let chars: Vec<_> = text.trim_start().chars().collect();
    let mut out = Vec::new();
    let mut i = 0;
    let mut line = first_line;
    while i < chars.len() {
        let c = chars[i];
        if c == '\n' {
            line += 1;
        } else if !c.is_whitespace() {
            let (text, next) = if (c as u32) >= 0xf0000 {
                (format!("\"{}", literals[c as usize - 0xf0000]), i + 1)
            } else if let Some(word) = word_at(&chars, i) {
                word
            } else {
                (c.to_string(), i + 1)
            };
            out.push(Token { text, line });
            i = next;
            continue;
        }
        i += 1;
    }
    out
}

fn matches_at(tokens: &[Token], i: usize, words: &[&str]) -> bool {
    tokens.get(i..i + words.len()).is_some_and(|part| {
        part.iter()
            .zip(words)
            .all(|(token, word)| token.text == *word)
    })
}

fn call(tokens: &[Token], i: usize, names: &[&str]) -> bool {
    names.contains(&tokens[i].text.as_str())
        && tokens.get(i + 1).is_some_and(|t| t.text == "(")
        && (i == 0 || tokens[i - 1].text != "fn")
}

// The literal `serve` must be an argument of arg/args, not a later env value.
fn arguments(tokens: &[Token], method: usize) -> &[Token] {
    let start = method + 2;
    let mut depth = 1usize;
    for (i, token) in tokens.iter().enumerate().skip(start) {
        match token.text.as_str() {
            "(" => depth += 1,
            ")" => {
                depth -= 1;
                if depth == 0 {
                    return &tokens[start..i];
                }
            }
            _ => {}
        }
    }
    &tokens[start..]
}

#[derive(Debug)]
struct CommandState {
    line: usize,
    serve: bool,
    raw: bool,
    port_allowed: bool,
    dirty: Option<usize>,
}

impl CommandState {
    fn report(&self, offenses: &mut Vec<Offense>) {
        if !self.serve {
            return;
        }
        if self.raw && !self.port_allowed {
            offenses.push(Offense::RawServeSpawn { line: self.line });
        } else if let Some(line) = self.dirty {
            offenses.push(Offense::IncompleteIdentity { line });
        }
    }
}

fn classify(src: &str) -> Vec<Offense> {
    let parsed = lex(src);
    let mut chars: Vec<_> = parsed.blanked.chars().collect();
    let literals: Vec<_> = parsed.literals.iter().map(Literal::cooked).collect();
    for (i, literal) in parsed.literals.iter().enumerate() {
        chars[literal.offset] =
            char::from_u32(0xf0000 + u32::try_from(i).expect("literal count fits u32"))
                .expect("literal sentinel");
    }
    let mut port_markers = BTreeMap::new();
    let mut identity_markers = BTreeMap::new();
    let mut offenses = Vec::new();
    for comment in &parsed.line_comments {
        for (tag, markers) in [
            ("serve-spawn", &mut port_markers),
            ("fixture-identity", &mut identity_markers),
        ] {
            match classify_marker(&comment.text, tag) {
                Marker::Malformed => offenses.push(Offense::MalformedMarker { line: comment.line }),
                Marker::WithReason => {
                    markers.insert(comment.line, comment.standalone);
                }
                Marker::Absent => {}
            }
        }
    }
    let local_marker = |markers: &BTreeMap<usize, bool>, line: usize| {
        if markers.contains_key(&line) {
            Some(line)
        } else if line > 1 && markers.get(&(line - 1)) == Some(&true) {
            Some(line - 1)
        } else {
            None
        }
    };
    let mut used_identity_markers = Vec::new();
    let mut commands: HashMap<String, CommandState> = HashMap::new();
    let mut has_builder = false;
    let mut wss_line = None;
    for statement in split_statements(&chars.into_iter().collect::<String>()) {
        let tokens = statement_tokens(&statement.text, statement.line, &literals);
        if tokens.is_empty() {
            continue;
        }
        if tokens.iter().any(|t| t.text == "fn") {
            for (_, command) in commands.drain() {
                command.report(&mut offenses);
            }
        }
        let binding = if tokens[0].text == "let" {
            let i = if tokens.get(1).is_some_and(|t| t.text == "mut") {
                2
            } else {
                1
            };
            tokens.get(i).map(|t| t.text.clone())
        } else {
            None
        };
        if let Some(name) = &binding {
            if let Some(old) = commands.remove(name) {
                old.report(&mut offenses);
            }
        }
        let mut active = None;
        for i in 0..tokens.len() {
            let token = &tokens[i];
            if matches_at(&tokens, i, &["enable_ws_api", "("]) {
                wss_line.get_or_insert(token.line);
            }
            if call(&tokens, i, RETIRED) {
                offenses.push(Offense::IncompleteIdentity { line: token.line });
            }
            let builder = call(&tokens, i, BUILDERS);
            let raw = matches_at(
                &tokens,
                i,
                &[
                    "Command",
                    ":",
                    ":",
                    "new",
                    "(",
                    "env",
                    "!",
                    "(",
                    "\"CARGO_BIN_EXE_intentd",
                    ")",
                    ")",
                ],
            );
            if builder || raw {
                has_builder |= builder;
                let name = binding
                    .clone()
                    .unwrap_or_else(|| format!("@{}:{i}", token.line));
                commands.insert(
                    name.clone(),
                    CommandState {
                        line: token.line,
                        serve: builder,
                        raw,
                        port_allowed: local_marker(&port_markers, token.line).is_some(),
                        dirty: raw.then_some(token.line),
                    },
                );
                active = Some(name);
            }
            if matches_at(&tokens, i + 1, &["."]) && commands.contains_key(&token.text) {
                active = Some(token.text.clone());
            }
            let reset = call(&tokens, i, RESET_HELPERS);
            let mock = call(&tokens, i, MOCK_HELPERS);
            if reset || mock {
                let authorized = if mock {
                    if let Some(line) = local_marker(&identity_markers, token.line) {
                        used_identity_markers.push(line);
                        true
                    } else {
                        offenses.push(Offense::UnreasonedMock { line: token.line });
                        false
                    }
                } else {
                    true
                };
                if authorized && matches_at(&tokens, i + 1, &["(", "&", "mut"]) {
                    if let Some(command) = tokens.get(i + 4).and_then(|t| commands.get_mut(&t.text))
                    {
                        command.dirty = None;
                    }
                }
            }
            // A wrapper's explicit binary environment identifies its command too.
            if matches_at(
                &tokens,
                i,
                &[
                    "env",
                    "(",
                    "\"INTENTD_BIN",
                    ",",
                    "env",
                    "!",
                    "(",
                    "\"CARGO_BIN_EXE_intentd",
                ],
            ) && i >= 2
                && tokens[i - 1].text == "."
            {
                let name = active
                    .clone()
                    .or_else(|| binding.clone())
                    .unwrap_or_else(|| tokens[i - 2].text.clone());
                commands.entry(name.clone()).or_insert(CommandState {
                    line: token.line,
                    serve: true,
                    raw: false,
                    port_allowed: true,
                    dirty: Some(token.line),
                });
                active = Some(name);
            }
            // GuardedChild owns most real fixture launches; a later reset must
            // not retroactively sanitize the environment that was already spawned.
            if matches_at(&tokens, i, &["spawn", "(", "&", "mut"]) {
                if let Some(command) = tokens.get(i + 4).and_then(|t| commands.get(&t.text)) {
                    command.report(&mut offenses);
                }
            }
            if matches_at(&tokens, i, &["spawn_command", "("]) {
                if let Some(command) = tokens.get(i + 2).and_then(|t| commands.get(&t.text)) {
                    command.report(&mut offenses);
                }
            }
            if i == 0
                || tokens[i - 1].text != "."
                || tokens.get(i + 1).is_none_or(|t| t.text != "(")
            {
                continue;
            }
            let Some(command) = active.as_ref().and_then(|name| commands.get_mut(name)) else {
                continue;
            };
            if ["arg", "args"].contains(&token.text.as_str())
                && arguments(&tokens, i).iter().any(|t| t.text == "\"serve")
            {
                command.serve = true;
            }
            let key = tokens.get(i + 2).map_or("", |t| t.text.as_str());
            let identity_key = key
                .strip_prefix('"')
                .is_some_and(|k| IDENTITY_KEYS.contains(&k));
            let dynamic_key = !key.starts_with('"');
            let harmless_removal =
                token.text == "env_remove" && ["\"GITHUB_TOKEN", "\"GH_TOKEN"].contains(&key);
            if ["env_clear", "envs"].contains(&token.text.as_str())
                || (["env", "env_remove"].contains(&token.text.as_str())
                    && (identity_key || dynamic_key)
                    && !harmless_removal)
            {
                command.dirty = Some(token.line);
            }
            if ["spawn", "status", "output"].contains(&token.text.as_str()) {
                command.report(&mut offenses);
            }
        }
    }
    for command in commands.values() {
        command.report(&mut offenses);
    }
    for line in identity_markers.keys() {
        if !used_identity_markers.contains(line) {
            offenses.push(Offense::UnusedIdentityMarker { line: *line });
        }
    }
    if !has_builder && port_markers.is_empty() {
        if let Some(line) = wss_line {
            offenses.push(Offense::MissingBuilder { line });
        }
    }
    offenses.sort_by_key(|o| match o {
        Offense::RawServeSpawn { line }
        | Offense::MalformedMarker { line }
        | Offense::MissingBuilder { line }
        | Offense::IncompleteIdentity { line }
        | Offense::UnreasonedMock { line }
        | Offense::UnusedIdentityMarker { line } => *line,
    });
    offenses.dedup();
    offenses
}

fn scan(root: &Path) -> Vec<String> {
    scanned_files(root)
        .iter()
        .flat_map(|file| {
            let src = fs::read_to_string(file).expect("read test source");
            let rel = file.strip_prefix(root).unwrap().display().to_string();
            classify(&src)
                .iter()
                .map(|o| o.describe(&rel))
                .collect::<Vec<_>>()
        })
        .collect()
}

#[test]
fn e2e_suites_spawn_serve_through_the_shared_builder() {
    let root = workspace_root();
    assert!(
        scanned_files(&root).len() > 100,
        "broken test source discovery"
    );
    let offenders = scan(&root);
    assert!(offenders.is_empty(), "\nDaemon fixture contract violations:\n{}\nUse common::hermetic_serve_command(data_dir) or hermetic_serve_command_fixed_port(data_dir). {ALLOW_MARKER} only exempts launch mechanics, never identity.", offenders.join("\n"));
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
    fn serve_added_in_a_later_statement_is_flagged() {
        let src = r#"
let mut cmd = Command::new(env!("CARGO_BIN_EXE_intentd")); // we will add "serve" below
cmd.arg("serve");
"#;
        assert_eq!(classify(src), vec![Offense::RawServeSpawn { line: 2 }]);
    }

    #[test]
    fn statement_scan_has_no_thirty_line_blind_spot() {
        let mut src = String::from("Command::new(env!(\"CARGO_BIN_EXE_intentd\"))\n");
        for _ in 0..35 {
            src.push_str("    .env(\"K\", \"v\")\n");
        }
        src.push_str("    .arg(\"serve\");\n");
        assert_eq!(classify(&src), vec![Offense::RawServeSpawn { line: 1 }]);
    }

    #[test]
    fn allow_marker_with_a_reason_requires_separate_identity_reset() {
        let src = r#"
let mut cmd = Command::new(env!("CARGO_BIN_EXE_intentd")); // serve-spawn: allow — argv probe
cmd.arg("serve");
common::hermetic_fixture_identity(&mut cmd, &d);
let mut dashed = Command::new(env!("CARGO_BIN_EXE_intentd")).arg("serve"); // serve-spawn: allow - hyphen form
common::hermetic_fixture_identity(&mut dashed, &d);
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
        assert_eq!(classify(src), vec![Offense::MissingBuilder { line: 3 }]);
    }

    #[test]
    fn missing_builder_diagnostic_names_the_first_enable_ws_api_line() {
        let src = "use std::process::Command;\n\nfn boot(d: &Path) {\n    common::enable_ws_api(d);\n    common::enable_ws_api(d);\n}\n";
        let offenses = classify(src);
        assert_eq!(offenses, vec![Offense::MissingBuilder { line: 4 }]);
        let diagnostic = offenses[0].describe("crates/intentd/tests/x.rs");
        assert!(
            diagnostic.starts_with("crates/intentd/tests/x.rs:4: "),
            "{diagnostic}"
        );
        assert!(
            diagnostic.contains("common::hermetic_serve_command"),
            "{diagnostic}"
        );
        assert!(
            diagnostic.contains("hermetic_serve_command_fixed_port"),
            "{diagnostic}"
        );
    }

    #[test]
    fn enable_ws_api_with_either_builder_passes() {
        let default =
            "common::enable_ws_api(&d);\nlet mut cmd = common::hermetic_serve_command(&d);\n";
        let fixed =
            "common::enable_ws_api(&d);\nlet mut cmd = common::hermetic_serve_command_fixed_port(&d);\n";
        assert_eq!(classify(default), Vec::<Offense>::new());
        assert_eq!(classify(fixed), Vec::<Offense>::new());
    }

    /// Reviewer reproducer (intentd#1927 r4022725383): the pre-migration
    /// two-statement launcher shape plus a builder mention in a comment.
    #[test]
    fn comment_only_builder_mention_does_not_satisfy_the_file_rule() {
        let src = r#"// Prefer common::hermetic_serve_command(&d) for launches
common::enable_ws_api(&dir);
let mut command = Command::new(env!("CARGO_BIN_EXE_intentd"));
command.arg("serve");
"#;
        assert_eq!(
            classify(src),
            vec![
                Offense::MissingBuilder { line: 2 },
                Offense::RawServeSpawn { line: 3 }
            ]
        );
    }

    #[test]
    fn block_comment_builder_mention_does_not_satisfy_the_file_rule() {
        let inline = "common::enable_ws_api(&d); /* see common::hermetic_serve_command(&d) */\n";
        let spanning =
            "/*\n * Spawn via common::hermetic_serve_command(&d)\n */\ncommon::enable_ws_api(&d);\n";
        assert_eq!(classify(inline), vec![Offense::MissingBuilder { line: 1 }]);
        assert_eq!(
            classify(spanning),
            vec![Offense::MissingBuilder { line: 4 }]
        );
    }

    #[test]
    fn code_builder_call_with_a_comment_passes_the_file_rule() {
        let src =
            "// helpers live in common::hermetic_serve_command(&d)\ncommon::enable_ws_api(&d);\n\
                   let mut cmd = common::hermetic_serve_command(&d); // seam applied\n";
        assert_eq!(classify(src), Vec::<Offense>::new());
    }

    #[test]
    fn enable_ws_api_in_a_comment_does_not_trigger_the_file_rule() {
        let src = "// callers of enable_ws_api( must use the builder\n/* enable_ws_api(&d); */\n";
        assert_eq!(classify(src), Vec::<Offense>::new());
    }

    #[test]
    fn builder_call_does_not_excuse_a_raw_single_statement_serve_spawn() {
        let src = r#"common::enable_ws_api(&d);
let _ok = common::hermetic_serve_command(&d);
let raw = Command::new(env!("CARGO_BIN_EXE_intentd")).arg("serve");
"#;
        assert_eq!(classify(src), vec![Offense::RawServeSpawn { line: 3 }]);
    }

    #[test]
    fn enable_ws_api_rule_honors_a_reasoned_marker_anywhere_in_the_file() {
        let src = "// serve-spawn: allow — bash driver execs $INTENTD_BIN serve\n\
                   common::enable_ws_api(&d);\ncmd.env(\"INTENTD_BIN\", env!(\"CARGO_BIN_EXE_intentd\"));\ncommon::hermetic_pty_fixture_identity(&mut cmd, &d);\n";
        assert_eq!(classify(src), Vec::<Offense>::new());
        let unreasoned = "// serve-spawn: allow\ncommon::enable_ws_api(&d);\n";
        assert_eq!(
            classify(unreasoned),
            vec![
                Offense::MalformedMarker { line: 1 },
                Offense::MissingBuilder { line: 2 }
            ]
        );
    }

    #[test]
    fn non_spawn_uses_of_the_binary_path_are_not_flagged() {
        let src = r#"
let agent = MockAgent::new().with_mcp_bridge_exe(env!("CARGO_BIN_EXE_intentd"));
cmd.env("UNRELATED_BIN", env!("CARGO_BIN_EXE_intentd"));
assert_eq!(cmd.get_program(), env!("CARGO_BIN_EXE_intentd"), "serve");
"#;
        assert_eq!(classify(src), Vec::<Offense>::new());
    }
}

mod identity_regressions {
    use super::*;

    #[test]
    fn incomplete_builders_are_rejected_even_beside_a_safe_call() {
        for builder in [
            "serve_command",
            "serve_command_fixed_port",
            "hermetic_github_identity",
        ] {
            let src = format!("let safe = common::hermetic_serve_command(&d);\nlet unsafe_cmd = common::{builder}();\n");
            assert!(!classify(&src).is_empty(), "{src}");
        }
    }

    #[test]
    fn raw_split_launch_is_rejected_beside_safe_builder() {
        let src = "let safe = common::hermetic_serve_command(&d);\nlet mut raw = Command::new(\n env!(\"CARGO_BIN_EXE_intentd\")\n);\nraw.arg(\"serve\");\nraw.spawn();\n";
        assert!(!classify(src).is_empty());
    }

    #[test]
    fn identity_overrides_and_env_clear_are_rejected() {
        for mutation in [
            "env(\"GITHUB_TOKEN\", \"mock\")",
            "env(\"GH_TOKEN\", \"mock\")",
            "env(\"GH_CONFIG_DIR\", outside)",
            "env(\"INTENTD_SECRETS_FILE\", outside)",
            "env_remove(\"GH_CONFIG_DIR\")",
            "env_remove(\"INTENTD_SECRETS_FILE\")",
            "env_clear()",
        ] {
            for src in [
                format!("common::hermetic_serve_command(&d).{mutation}.spawn();"),
                format!("let mut cmd = common::hermetic_serve_command(&d);\ncmd.{mutation};\ncmd.spawn();"),
            ] {
                assert!(!classify(&src).is_empty(), "{src}");
            }
        }
    }

    #[test]
    fn unrelated_and_comment_only_helpers_do_not_authorize_overrides() {
        for mention in [
            "// common::hermetic_fixture_identity(&mut cmd, &d);",
            "let prose = \"common::hermetic_fixture_identity(&mut cmd, &d)\";",
            "common::hermetic_fixture_identity(&mut other, &d);",
        ] {
            let src = format!("let mut cmd = common::hermetic_serve_command(&d);\ncmd.env_clear();\n{mention}\ncmd.spawn();");
            assert!(!classify(&src).is_empty(), "{src}");
        }
    }

    #[test]
    fn port_or_wrapper_marker_does_not_exempt_identity() {
        let src = "// serve-spawn: allow — private port probe\nlet mut cmd = common::hermetic_serve_command(&d);\ncmd.env(\"GH_TOKEN\", \"host\");\ncmd.spawn();";
        assert!(!classify(src).is_empty());
        let raw = "let cmd = Command::new(env!(\"CARGO_BIN_EXE_intentd\")).arg(\"serve\"); // serve-spawn: allow — fixed port\ncmd.spawn();";
        assert!(!classify(raw).is_empty());
    }

    #[test]
    fn missing_identity_exception_reason_is_rejected() {
        for marker in ["// fixture-identity: allow", "// fixture-identity: allow —"] {
            assert!(!classify(marker).is_empty(), "{marker}");
        }
    }

    #[test]
    fn ordinary_fixed_port_and_post_override_reset_pass() {
        for builder in [
            "hermetic_serve_command",
            "hermetic_serve_command_fixed_port",
        ] {
            let src = format!("common::enable_ws_api(&d);\nlet mut cmd = common::{builder}(&d);\ncmd.env_clear();\ncommon::hermetic_fixture_identity(&mut cmd, &d);\ncmd.env(\"INTENTD_TCP_PORT\", \"0\").spawn();");
            assert!(classify(&src).is_empty(), "{src}");
        }
    }
}

mod mutation_controls {
    use super::*;

    #[test]
    fn mock_calls_require_their_own_reason_and_never_exempt_adjacent_mutations() {
        for helper in MOCK_HELPERS {
            let valid = format!("let mut cmd = common::hermetic_serve_command(&d);\n// fixture-identity: allow — private mock identity server\ncommon::{helper}(&mut cmd, &d, mock);\ncmd.spawn();");
            assert!(classify(&valid).is_empty(), "{valid}");
            for mutated in [
                valid.replace("// fixture-identity: allow — private mock identity server", ""),
                valid.replace(" — private mock identity server", ""),
                valid.replace(helper, "unapproved_identity_helper"),
                valid.replace("cmd.spawn();", "cmd.env(\"GH_TOKEN\", \"unexpected\").spawn();"),
                valid.replace("cmd.spawn();", "let mut other = common::hermetic_serve_command(&d); other.env_clear(); other.spawn(); cmd.spawn();"),
            ] {
                assert!(!classify(&mutated).is_empty(), "{mutated}");
            }
        }
    }

    #[test]
    fn reason_on_direct_override_is_not_an_exception() {
        let src = "let mut cmd = common::hermetic_serve_command(&d);\ncmd.env(\"GH_TOKEN\", \"mock\"); // fixture-identity: allow — private mock token\ncmd.spawn();";
        let offenses = classify(src);
        assert!(offenses.contains(&Offense::IncompleteIdentity { line: 2 }));
        assert!(offenses.contains(&Offense::UnusedIdentityMarker { line: 2 }));
    }

    #[test]
    fn wrapper_needs_identity_reset_independent_of_port_opt_out() {
        let valid = "// serve-spawn: allow — PTY driver executes daemon\ncommon::enable_ws_api(&d);\nlet mut cmd = CommandBuilder::new(\"bash\");\ncmd.env(\"INTENTD_BIN\", env!(\"CARGO_BIN_EXE_intentd\"));\ncommon::hermetic_pty_fixture_identity(&mut cmd, &d);\npair.slave.spawn_command(cmd);";
        assert!(classify(valid).is_empty());
        for mutated in [
            valid.replace("common::hermetic_pty_fixture_identity(&mut cmd, &d);", ""),
            valid.replace("&mut cmd", "&mut other"),
            valid.replace(
                "common::hermetic_pty_fixture_identity",
                "// common::hermetic_pty_fixture_identity",
            ),
            valid.replace(
                "pair.slave.spawn_command(cmd);",
                "cmd.env_clear(); pair.slave.spawn_command(cmd);",
            ),
        ] {
            assert!(!classify(&mutated).is_empty(), "{mutated}");
        }
    }

    #[test]
    fn dynamic_environment_requires_reset_before_launch() {
        for mutation in [
            "cmd.env(key, value);",
            "cmd.envs(extra);",
            "cmd.env_remove(key);",
        ] {
            let valid = format!("let mut cmd = common::hermetic_serve_command(&d); {mutation}\ncommon::hermetic_fixture_identity(&mut cmd, &d); cmd.spawn();");
            assert!(classify(&valid).is_empty(), "{valid}");
            let invalid = valid.replace("common::hermetic_fixture_identity(&mut cmd, &d);", "");
            assert!(!classify(&invalid).is_empty(), "{invalid}");
        }
    }

    #[test]
    fn reset_after_launch_cannot_erase_violation() {
        let src = "let mut cmd = common::hermetic_serve_command(&d);\ncmd.env_clear();\ncmd.spawn();\ncommon::hermetic_fixture_identity(&mut cmd, &d);";
        assert_eq!(classify(src), vec![Offense::IncompleteIdentity { line: 2 }]);
    }

    #[test]
    fn quoted_builder_names_and_longer_identifiers_do_not_count() {
        for mention in [
            "let s = \"hermetic_serve_command(&d)\";",
            "unrelated_hermetic_serve_command(&d);",
            "let helper = common::hermetic_serve_command;",
            "/* common::hermetic_serve_command(&d); */",
        ] {
            let src = format!("common::enable_ws_api(&d); {mention}");
            assert_eq!(classify(&src), vec![Offense::MissingBuilder { line: 1 }]);
        }
    }

    #[test]
    fn literal_lexing_preserves_urls_raw_strings_and_escaped_keys() {
        let src = r##"let mut cmd = common::hermetic_serve_command(&d);
cmd.env("URL", "https://mock.invalid");
cmd.env(r#"GH_TOKEN"#, "fake");
cmd.spawn();"##;
        assert_eq!(classify(src), vec![Offense::IncompleteIdentity { line: 3 }]);
        let escaped = src.replace("r#\"GH_TOKEN\"#", "\"GH_\\u{54}OKEN\"");
        assert_eq!(
            classify(&escaped),
            vec![Offense::IncompleteIdentity { line: 3 }]
        );
    }

    #[test]
    fn comments_strings_and_unrelated_non_daemon_commands_are_inert() {
        let src = r##"// serve_command();
/* Command::new(env!("CARGO_BIN_EXE_intentd")).arg("serve"); */
let prose = r#"// fixture-identity: allow
common::mock_github_token(&mut cmd, &d, "fake");"#;
let mut cmd = Command::new("never-executed");
cmd.env("GH_TOKEN", "synthetic").env_clear();"##;
        assert!(classify(src).is_empty(), "{:?}", classify(src));
    }

    #[test]
    fn token_removal_and_unrelated_environment_are_allowed() {
        let src = "common::hermetic_serve_command(&d).env_remove(\"GITHUB_TOKEN\").env_remove(\"GH_TOKEN\").env(\"INTENTD_AUTH_TOKEN\", mock).env(\"URL\", \"https://mock.invalid\").spawn();";
        assert!(classify(src).is_empty());
    }
    #[test]
    fn serve_in_unrelated_argument_does_not_make_a_doctor_command_a_daemon() {
        let src = r#"Command::new(env!("CARGO_BIN_EXE_intentd")).arg("doctor").env("K", "serve").output();"#;
        assert!(classify(src).is_empty(), "{:?}", classify(src));
    }

    #[test]
    fn wrapper_reset_after_spawn_is_too_late() {
        let src = r#"let mut cmd = CommandBuilder::new("bash");
cmd.env("INTENTD_BIN", env!("CARGO_BIN_EXE_intentd"));
pair.slave.spawn_command(cmd);
common::hermetic_pty_fixture_identity(&mut cmd, &d);"#;
        assert_eq!(classify(src), vec![Offense::IncompleteIdentity { line: 2 }]);
    }
    #[test]
    fn guarded_spawn_must_observe_identity_before_a_later_reset() {
        let src = r"let mut cmd = common::hermetic_serve_command(&d);
cmd.env_clear();
let child = GuardedChild::spawn(&mut cmd);
common::hermetic_fixture_identity(&mut cmd, &d);";
        assert_eq!(classify(src), vec![Offense::IncompleteIdentity { line: 2 }]);
    }
}
