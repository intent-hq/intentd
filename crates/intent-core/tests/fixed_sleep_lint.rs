//! Fixed-sleep annotation lint.
//!
//! A fixed sleep in an integration test — `std::thread::sleep(..)`,
//! `tokio::time::sleep(..)`, or a shell `sleep <n>` inside a fake-daemon
//! script — is a timing assumption: on a positive path it wastes the whole
//! delay, and it flakes as soon as the machine is slower than the number.
//! Positive-path waits belong on an observable event (a barrier file, a
//! `wait_until` poll on the state under test). This source-scanning test
//! fails, naming `file:line`, for every fixed sleep under
//! `crates/*/tests/**/*.rs` that is neither justified with a `timing-guard:`
//! marker nor grandfathered by the committed baseline.
//!
//! - Predicate ([`line_has_fixed_sleep`], lifted from the `supervisor_e2e.rs`
//!   self-lint of intent-hq/intentd#1924 and extended to `time::sleep(`): a
//!   line has a fixed sleep when it contains `thread::sleep(` or
//!   `time::sleep(` anywhere, or a shell `sleep` at a word boundary followed
//!   by blanks and a numeral / `{}` argument. Only the `sleep 60 &` stay-alive
//!   idiom is exempt — the `&` must be the whole backgrounding operator, so
//!   `sleep 60 && …` and `sleep 60 &> …` still count; comment lines never
//!   count.
//! - Marker: `// timing-guard: <reason>` on the sleep's own line or the line
//!   immediately above exempts it (`// timing-guard: poll interval`). It
//!   counts only inside a `//` line comment (standalone or trailing) and only
//!   with a nonempty reason. A marker with no reason is malformed: it never
//!   exempts the sleep and the report names it. Detection is line-based: it
//!   skips ordinary `"…"` / `'…'` literals on that line but tracks no
//!   block-comment or raw-string state, so a `//` inside `/* … */` or `r#"…"#`
//!   reads as a comment — a marker can be spoofed only deliberately.
//! - Baseline (`tests/fixed_sleep_baseline.txt`): one `<path> <count>` line
//!   per file that still has unannotated sleeps, sorted by path. It only
//!   ratchets down: a file over its entry (or absent from the baseline) fails
//!   naming every unannotated line; a file under its entry, or an entry whose
//!   file is gone, fails naming the exact line to write so the entry follows
//!   the fix.
//! - Skipped: this file, whose fixtures spell the patterns out.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

/// Marker that exempts a fixed sleep when it sits in a `//` comment, followed
/// by a reason, on the sleep's line or the one above.
const TIMING_GUARD_MARKER: &str = "timing-guard:";

/// Split so the lint's own source does not match itself.
const THREAD_SLEEP: &str = concat!("thread::", "sleep(");
/// Also covers `tokio::time::sleep(`.
const TIME_SLEEP: &str = concat!("time::", "sleep(");

const BASELINE_FILE: &str = "crates/intent-core/tests/fixed_sleep_baseline.txt";
const SELF_FILE: &str = "crates/intent-core/tests/fixed_sleep_lint.rs";

/// Does `line` contain a fixed sleep? Either a Rust [`THREAD_SLEEP`] /
/// [`TIME_SLEEP`] anywhere, or a shell `sleep` command — at a word boundary,
/// followed by blanks — whose first argument is a numeral (`0.2`, `.2`,
/// `"2"`, `'2'`) or a `{}` interpolation. Only the `sleep 60 &` stay-alive
/// occurrence is exempt — and only when that `&` is the whole backgrounding
/// operator, not the start of `&&` or `&>`; any other sleep on the same line
/// still counts. Comment lines never count.
fn line_has_fixed_sleep(line: &str) -> bool {
    let line = line.trim_start();
    if line.starts_with("//") {
        return false;
    }
    if line.contains(THREAD_SLEEP) || line.contains(TIME_SLEEP) {
        return true;
    }
    line.match_indices("sleep").any(|(at, word)| {
        let boundary = line[..at]
            .chars()
            .next_back()
            .is_none_or(|c| !(c.is_alphanumeric() || c == '_'));
        let after = &line[at + word.len()..];
        let arg = after.trim_start_matches([' ', '\t']);
        if !boundary || arg.len() == after.len() || is_stay_alive(arg) {
            return false;
        }
        arg.trim_start_matches(['"', '\''])
            .starts_with(|c: char| c.is_ascii_digit() || c == '.' || c == '{')
    })
}

/// Is `arg` (the text after a shell `sleep`) the `60 &` stay-alive idiom? The
/// `&` must be the complete operator: followed by nothing, whitespace, or any
/// character other than `&` (`&&`) or `>` (`&>`).
fn is_stay_alive(arg: &str) -> bool {
    arg.strip_prefix("60 &")
        .is_some_and(|rest| !rest.starts_with(['&', '>']))
}

/// Marker state of one line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Marker {
    Absent,
    WithReason,
    /// `timing-guard:` in a `//` comment with nothing after it.
    Malformed,
}

/// The `//` line comment on `line`, if any: the first `//` that is not inside
/// an ordinary `"…"` or `'…'` literal. Line-based on purpose: block comments
/// and raw strings are not tracked, so a `//` inside them is taken as a
/// comment.
fn line_comment(line: &str) -> Option<&str> {
    let bytes = line.as_bytes();
    let mut in_string = false;
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'\\' if in_string => i += 1,
            b'"' => in_string = !in_string,
            b'\'' if !in_string => {
                if bytes.get(i + 1) == Some(&b'\\') {
                    if let Some(end) = line[i + 2..].find('\'') {
                        i += 2 + end;
                    }
                } else if bytes.get(i + 2) == Some(&b'\'') {
                    i += 2;
                }
            }
            b'/' if !in_string && bytes.get(i + 1) == Some(&b'/') => return Some(&line[i..]),
            _ => {}
        }
        i += 1;
    }
    None
}

/// `WithReason` when `line`'s `//` comment carries [`TIMING_GUARD_MARKER`]
/// followed by a nonempty reason, `Malformed` when the marker has no reason,
/// `Absent` otherwise (including a marker in an ordinary string literal or
/// with no `//` before it; see [`line_comment`] for what is not tracked).
fn classify_marker(line: &str) -> Marker {
    let Some(comment) = line_comment(line) else {
        return Marker::Absent;
    };
    let Some((_, reason)) = comment.split_once(TIMING_GUARD_MARKER) else {
        return Marker::Absent;
    };
    if reason.trim().is_empty() {
        Marker::Malformed
    } else {
        Marker::WithReason
    }
}

/// An unannotated fixed sleep: its 1-based line and trimmed text.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Sleep {
    line: usize,
    text: String,
    /// Its line or the one above carries a marker with no reason.
    marker_malformed: bool,
}

/// Every fixed sleep in `source` that carries no reasoned marker on its own
/// line or the line immediately above.
fn unannotated_sleeps(source: &str) -> Vec<Sleep> {
    let lines: Vec<&str> = source.lines().collect();
    lines
        .iter()
        .enumerate()
        .filter(|(_, line)| line_has_fixed_sleep(line))
        .filter_map(|(i, line)| {
            let above = if i > 0 { lines[i - 1] } else { "" };
            let markers = [classify_marker(line), classify_marker(above)];
            if markers.contains(&Marker::WithReason) {
                return None;
            }
            Some(Sleep {
                line: i + 1,
                text: line.trim().to_string(),
                marker_malformed: markers.contains(&Marker::Malformed),
            })
        })
        .collect()
}

/// Parses the baseline: `<path> <count>` per line, blank lines and `#`
/// comments ignored, paths strictly increasing (sorted, unique), counts
/// positive — a file with no unannotated sleeps has no entry.
fn parse_baseline(text: &str) -> Result<BTreeMap<String, usize>, String> {
    let mut out = BTreeMap::new();
    let mut previous: Option<String> = None;
    for (i, raw) in text.lines().enumerate() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let n = i + 1;
        let (path, count) = line
            .rsplit_once([' ', '\t'])
            .ok_or_else(|| format!("line {n}: expected `<path> <count>`, got {raw:?}"))?;
        let path = path.trim_end();
        let count: usize = count
            .parse()
            .map_err(|_| format!("line {n}: count {count:?} is not a number"))?;
        if count == 0 {
            return Err(format!(
                "line {n}: a file with no unannotated sleeps has no entry; remove {raw:?}"
            ));
        }
        if let Some(prev) = previous.as_deref().filter(|prev| *prev >= path) {
            return Err(format!(
                "line {n}: entries must be sorted by path and unique, but {path:?} follows {prev:?}"
            ));
        }
        previous = Some(path.to_string());
        out.insert(path.to_string(), count);
    }
    Ok(out)
}

/// One way a scanned tree disagrees with the baseline.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Finding {
    /// More unannotated sleeps than the baseline allows (`None`: no entry).
    Over {
        path: String,
        count: usize,
        allowed: Option<usize>,
    },
    /// Fewer than the entry allows: the entry must follow the count down.
    Under {
        path: String,
        count: usize,
        allowed: usize,
    },
    /// The entry names a file that no longer exists.
    Stale { path: String, allowed: usize },
}

/// Compares per-file unannotated counts (every scanned file, zero included)
/// against the baseline. Baseline order, then stale entries.
fn classify(counts: &BTreeMap<String, usize>, baseline: &BTreeMap<String, usize>) -> Vec<Finding> {
    let mut findings = Vec::new();
    for (path, &count) in counts {
        let path = path.clone();
        match (count, baseline.get(&path).copied()) {
            (0, None) => {}
            (count, None) => findings.push(Finding::Over {
                path,
                count,
                allowed: None,
            }),
            (count, Some(allowed)) if count > allowed => findings.push(Finding::Over {
                path,
                count,
                allowed: Some(allowed),
            }),
            (count, Some(allowed)) if count < allowed => findings.push(Finding::Under {
                path,
                count,
                allowed,
            }),
            _ => {}
        }
    }
    for (path, &allowed) in baseline {
        if !counts.contains_key(path) {
            findings.push(Finding::Stale {
                path: path.clone(),
                allowed,
            });
        }
    }
    findings
}

/// Every sleep whose marker is malformed, as `path:line: text`.
fn malformed_markers(sleeps: &BTreeMap<String, Vec<Sleep>>) -> Vec<String> {
    sleeps
        .iter()
        .flat_map(|(path, sleeps)| {
            sleeps
                .iter()
                .filter(|sleep| sleep.marker_malformed)
                .map(move |sleep| format!("{path}:{}: {}", sleep.line, sleep.text))
        })
        .collect()
}

/// The failure report: every malformed marker, every unannotated `file:line`
/// of an over-baseline file plus the fix, and the exact baseline line to
/// write for a file whose count went down or disappeared.
fn render(findings: &[Finding], sleeps: &BTreeMap<String, Vec<Sleep>>) -> String {
    let mut out = Vec::new();
    for site in malformed_markers(sleeps) {
        out.push(format!(
            "{site}\n  timing-guard marker is malformed: expected `// {TIMING_GUARD_MARKER} <reason>` \
             in a `//` comment; the reason is required."
        ));
    }
    for finding in findings {
        match finding {
            Finding::Over {
                path,
                count,
                allowed,
            } => {
                let allowed = match allowed {
                    Some(allowed) => format!("the baseline allows {allowed}"),
                    None => "the baseline has no entry for it".to_string(),
                };
                out.push(format!(
                    "{path}: {count} unannotated fixed sleep(s), {allowed}:"
                ));
                for sleep in sleeps.get(path).map_or(&[][..], Vec::as_slice) {
                    out.push(format!("  {path}:{}: {}", sleep.line, sleep.text));
                }
                out.push(format!(
                    "  fix: justify each new sleep with `// {TIMING_GUARD_MARKER} <reason>` on \
                     its line or the one above, or replace it with a wait on an observable event; \
                     as a last resort (never preferred) set its entry in {BASELINE_FILE} to \
                     `{path} {count}`."
                ));
            }
            Finding::Under {
                path,
                count: 0,
                allowed,
            } => out.push(format!(
                "{path}: no unannotated fixed sleeps left but the baseline allows {allowed}; \
                 the ratchet only moves down — remove its line from {BASELINE_FILE}: \
                 `{path} {allowed}`"
            )),
            Finding::Under {
                path,
                count,
                allowed,
            } => out.push(format!(
                "{path}: {count} unannotated fixed sleep(s) but the baseline allows {allowed}; \
                 the ratchet only moves down — replace its line in {BASELINE_FILE} with: \
                 `{path} {count}`"
            )),
            Finding::Stale { path, allowed } => out.push(format!(
                "{path}: no longer exists; remove its line from {BASELINE_FILE}: \
                 `{path} {allowed}`"
            )),
        }
    }
    out.join("\n")
}

/// Every `*.rs` under `dir`, recursively.
fn collect_rust_sources(dir: &Path, out: &mut Vec<PathBuf>) {
    let entries = fs::read_dir(dir).unwrap_or_else(|e| panic!("read_dir {}: {e}", dir.display()));
    for entry in entries {
        let entry = entry.unwrap_or_else(|e| panic!("read_dir {}: {e}", dir.display()));
        let path = entry.path();
        if path.is_dir() {
            collect_rust_sources(&path, out);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            out.push(path);
        }
    }
}

/// Every `*.rs` under `crates/*/tests/`, sorted.
fn collect_test_sources(root: &Path) -> Vec<PathBuf> {
    let crates_dir = root.join("crates");
    let crates = fs::read_dir(&crates_dir)
        .unwrap_or_else(|e| panic!("read_dir {}: {e}", crates_dir.display()));
    let mut files = Vec::new();
    for entry in crates {
        let tests = entry.expect("crate dir entry").path().join("tests");
        if tests.is_dir() {
            collect_rust_sources(&tests, &mut files);
        }
    }
    files.sort();
    files
}

fn display_rel(rel: &Path) -> String {
    rel.components()
        .map(|c| c.as_os_str().to_string_lossy().into_owned())
        .collect::<Vec<_>>()
        .join("/")
}

#[test]
fn fixed_sleeps_are_annotated_or_baselined() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("..").join("..");
    let baseline_text = fs::read_to_string(root.join(BASELINE_FILE))
        .unwrap_or_else(|e| panic!("read {BASELINE_FILE}: {e}"));
    let baseline =
        parse_baseline(&baseline_text).unwrap_or_else(|e| panic!("{BASELINE_FILE}: {e}"));

    let files = collect_test_sources(&root);
    let mut sleeps = BTreeMap::new();
    let mut saw_self = false;
    for file in &files {
        let rel = display_rel(file.strip_prefix(&root).expect("under root"));
        if rel == SELF_FILE {
            saw_self = true;
            continue;
        }
        let src =
            fs::read_to_string(file).unwrap_or_else(|e| panic!("read {}: {e}", file.display()));
        sleeps.insert(rel, unannotated_sleeps(&src));
    }
    assert!(
        saw_self,
        "{SELF_FILE} moved; update SELF_FILE so the lint keeps skipping its own fixtures"
    );
    assert!(
        !sleeps.is_empty(),
        "no test sources found under {}",
        root.display()
    );

    let counts = sleeps
        .iter()
        .map(|(path, sleeps)| (path.clone(), sleeps.len()))
        .collect();
    let findings = classify(&counts, &baseline);
    assert!(
        findings.is_empty() && malformed_markers(&sleeps).is_empty(),
        "fixed sleeps in e2e tests out of step with {BASELINE_FILE}:\n\n{}\n",
        render(&findings, &sleeps)
    );
}

// ---- fixtures ---------------------------------------------------------------

#[test]
fn line_has_fixed_sleep_cases() {
    // The 16 cases pinned by the supervisor_e2e.rs self-lint (#1924), then
    // the tokio, identifier-suffix and `&`-operator cases this lint adds.
    let cases: [(&str, bool); 23] = [
        ("sleep 0.2", true),
        ("sleep .2", true),
        ("sleep  0.2", true),
        ("sleep\t0.2", true),
        ("sleep \"0.2\"", true),
        ("sleep '2'", true),
        ("sleep {secs}\\n\\", true),
        ("while [ ! -e x ]; do sleep 0.05; done", true),
        ("sleep 0.2; sleep 60 &", true),
        ("thread::sleep(Duration::from_secs(1)); // sleep 60 &", true),
        ("sleep 60 &\\n\\", false),
        ("while :; do sleep 60 & wait $!; done\\n\"", false),
        ("nosleep 10", false),
        ("thread_sleep 10", false),
        ("// sleep 5", false),
        ("echo sleep", false),
        (
            "tokio::time::sleep(Duration::from_millis(100)).await;",
            true,
        ),
        ("time::sleep(d).await", true),
        ("fn sleep_then(after: Duration, f: impl FnOnce()) {", false),
        ("sleep 60 && echo", true),
        ("sleep 60 &>/dev/null", true),
        ("sleep 60 & wait $!", false),
        ("sleep 60 &", false),
    ];
    for (line, expected) in cases {
        assert_eq!(
            line_has_fixed_sleep(line),
            expected,
            "line_has_fixed_sleep({line:?})"
        );
    }
}

#[test]
fn marker_on_the_line_or_the_one_above_exempts_a_sleep() {
    let src = "\
use std::thread;

fn poll() {
    thread::sleep(Duration::from_millis(5)); // timing-guard: poll interval
    // timing-guard: settle window
    tokio::time::sleep(Duration::from_millis(5)).await;
    // timing-guard: two lines above does not count
    let _ = 1;
    thread::sleep(Duration::from_millis(5));
    tokio::time::sleep(Duration::from_millis(5)).await;
    let script = \"sleep 0.2\\n\";
    let alive = \"sleep 60 &\\n\";
    let _ = sleep_then(Duration::from_millis(5), || {});
}
";
    let found = unannotated_sleeps(src);
    assert_eq!(
        found.iter().map(|s| s.line).collect::<Vec<_>>(),
        vec![9, 10, 11]
    );
    assert_eq!(found[0].text, "thread::sleep(Duration::from_millis(5));");
    assert!(found.iter().all(|s| !s.marker_malformed), "{found:?}");
}

#[test]
fn marker_counts_only_in_a_line_comment_and_needs_a_reason() {
    let src = "\
fn cases() {
    thread::sleep(d); // timing-guard: trailing reason exempts
    let c = '\"'; thread::sleep(d); // timing-guard: char literal before the comment
    // timing-guard: standalone reason exempts
    thread::sleep(d);
    // timing-guard:
    thread::sleep(d);
    thread::sleep(d); // timing-guard:
    let _ = 1;
    thread::sleep(d); // timing-guard:   \t
    let _ = 1;
    let s = \"timing-guard: in a string\"; thread::sleep(d);
    let s = \"// timing-guard: in a string\";
    thread::sleep(d);
    /* timing-guard: block comment */ thread::sleep(d);
    thread::sleep(d);
}
";
    let found = unannotated_sleeps(src);
    assert_eq!(
        found
            .iter()
            .map(|s| (s.line, s.marker_malformed))
            .collect::<Vec<_>>(),
        vec![
            (7, true),
            (8, true),
            (10, true),
            (12, false),
            (14, false),
            (15, false),
            (16, false),
        ]
    );
    assert_eq!(
        classify_marker("    // timing-guard: reason"),
        Marker::WithReason
    );
    assert_eq!(classify_marker("    // timing-guard:"), Marker::Malformed);
    assert_eq!(classify_marker("    let _ = 1;"), Marker::Absent);
}

#[test]
fn a_sleep_on_the_first_line_has_no_line_above() {
    assert_eq!(
        unannotated_sleeps("thread::sleep(d);\n"),
        vec![Sleep {
            line: 1,
            text: "thread::sleep(d);".to_string(),
            marker_malformed: false,
        }]
    );
    assert_eq!(
        unannotated_sleeps("thread::sleep(d); // timing-guard: fixture\n"),
        vec![]
    );
}

#[test]
fn baseline_parses_sorted_entries_and_skips_comments() {
    let text = "# path count\n\ncrates/a/tests/x.rs 3\ncrates/b/tests/common/mod.rs\t1\n";
    let parsed = parse_baseline(text).expect("valid baseline");
    assert_eq!(
        parsed,
        BTreeMap::from([
            ("crates/a/tests/x.rs".to_string(), 3),
            ("crates/b/tests/common/mod.rs".to_string(), 1),
        ])
    );
    assert_eq!(parse_baseline("").expect("empty baseline"), BTreeMap::new());
}

#[test]
fn baseline_rejects_malformed_unsorted_or_zero_entries() {
    for (text, expected) in [
        ("crates/a/tests/x.rs", "expected `<path> <count>`"),
        ("crates/a/tests/x.rs many", "is not a number"),
        ("crates/a/tests/x.rs 0", "has no entry"),
        (
            "crates/b/tests/x.rs 1\ncrates/a/tests/x.rs 1\n",
            "sorted by path and unique",
        ),
        (
            "crates/a/tests/x.rs 1\ncrates/a/tests/x.rs 2\n",
            "sorted by path and unique",
        ),
    ] {
        let err = parse_baseline(text).expect_err(text);
        assert!(err.contains(expected), "{text:?}: {err}");
    }
}

#[test]
fn classification_against_the_baseline() {
    let counts = BTreeMap::from([
        ("crates/a/tests/equal.rs".to_string(), 2),
        ("crates/a/tests/clean.rs".to_string(), 0),
        ("crates/a/tests/over.rs".to_string(), 3),
        ("crates/a/tests/new.rs".to_string(), 1),
        ("crates/a/tests/under.rs".to_string(), 1),
        ("crates/a/tests/fixed.rs".to_string(), 0),
    ]);
    let baseline = BTreeMap::from([
        ("crates/a/tests/equal.rs".to_string(), 2),
        ("crates/a/tests/over.rs".to_string(), 2),
        ("crates/a/tests/under.rs".to_string(), 2),
        ("crates/a/tests/fixed.rs".to_string(), 1),
        ("crates/a/tests/gone.rs".to_string(), 4),
    ]);
    assert_eq!(
        classify(&counts, &baseline),
        vec![
            Finding::Under {
                path: "crates/a/tests/fixed.rs".to_string(),
                count: 0,
                allowed: 1,
            },
            Finding::Over {
                path: "crates/a/tests/new.rs".to_string(),
                count: 1,
                allowed: None,
            },
            Finding::Over {
                path: "crates/a/tests/over.rs".to_string(),
                count: 3,
                allowed: Some(2),
            },
            Finding::Under {
                path: "crates/a/tests/under.rs".to_string(),
                count: 1,
                allowed: 2,
            },
            Finding::Stale {
                path: "crates/a/tests/gone.rs".to_string(),
                allowed: 4,
            },
        ]
    );
    assert_eq!(classify(&baseline, &baseline), vec![]);
}

#[test]
fn report_names_every_line_over_baseline_and_the_corrected_entry() {
    let sleeps = BTreeMap::from([
        (
            "crates/a/tests/over.rs".to_string(),
            vec![
                Sleep {
                    line: 12,
                    text: "thread::sleep(d);".to_string(),
                    marker_malformed: false,
                },
                Sleep {
                    line: 40,
                    text: "tokio::time::sleep(d).await;".to_string(),
                    marker_malformed: false,
                },
            ],
        ),
        (
            "crates/a/tests/marked.rs".to_string(),
            vec![Sleep {
                line: 7,
                text: "thread::sleep(d); // timing-guard:".to_string(),
                marker_malformed: true,
            }],
        ),
    ]);
    assert_eq!(
        malformed_markers(&sleeps),
        vec!["crates/a/tests/marked.rs:7: thread::sleep(d); // timing-guard:".to_string()]
    );
    let findings = vec![
        Finding::Over {
            path: "crates/a/tests/over.rs".to_string(),
            count: 2,
            allowed: Some(1),
        },
        Finding::Under {
            path: "crates/a/tests/under.rs".to_string(),
            count: 1,
            allowed: 2,
        },
        Finding::Under {
            path: "crates/a/tests/fixed.rs".to_string(),
            count: 0,
            allowed: 1,
        },
        Finding::Stale {
            path: "crates/a/tests/gone.rs".to_string(),
            allowed: 4,
        },
    ];
    let report = render(&findings, &sleeps);
    assert!(
        report.contains(
            "crates/a/tests/marked.rs:7: thread::sleep(d); // timing-guard:\n  timing-guard marker is malformed: expected `// timing-guard: <reason>`"
        ),
        "{report}"
    );
    assert!(
        report.contains("crates/a/tests/over.rs:12: thread::sleep(d);"),
        "{report}"
    );
    assert!(
        report.contains("crates/a/tests/over.rs:40: tokio::time::sleep(d).await;"),
        "{report}"
    );
    assert!(report.contains("`crates/a/tests/over.rs 2`"), "{report}");
    assert!(report.contains("replace its line in crates/intent-core/tests/fixed_sleep_baseline.txt with: `crates/a/tests/under.rs 1`"), "{report}");
    assert!(report.contains("remove its line from crates/intent-core/tests/fixed_sleep_baseline.txt: `crates/a/tests/fixed.rs 1`"), "{report}");
    assert!(report.contains("remove its line from crates/intent-core/tests/fixed_sleep_baseline.txt: `crates/a/tests/gone.rs 4`"), "{report}");
}
