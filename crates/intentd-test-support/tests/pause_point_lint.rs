//! Hand-rolled pause-point lint.
//!
//! Tests hold an operation part-way through with a pause point: the
//! operation signals "entered" and waits for a "release". Each suite used to
//! hand-roll one (`Gate`, `Pause`, `Park`, …), and every copy waited on both
//! signals with no time limit. On 2026-11-27 an orchestrator test run as plain
//! `cargo test` (so nextest's slow-timeout never applied) awaited
//! `park.entered` for an operation that had already failed, hung for about
//! six hours, and blocked a PR all that time.
//! `intentd_test_support::PausePoint` bounds every wait by `test_timeout` and
//! reports the operation's error when it ends before the pause point.
//!
//! This source-scanning test fails, naming `file:line`, for a `struct` in
//! test code (see `source_lint::test_code_files`) that looks like a
//! hand-rolled pause point and is neither marked nor grandfathered:
//!
//! - **Name:** a CamelCase word of its name is one of [`PAUSE_WORDS`]
//!   (`Gate`, `Pause`, `Park`, `Hold`, `Latch`, `Checkpoint`, …): `Gate`,
//!   `WritePause`, `ParkLease`. Adjectives (`GatedSecrets`, `PausedStore`)
//!   name the fake that owns one, not the pause point, and do not count.
//! - **Shape:** its named fields include one of [`ENTERED_FIELDS`] and one of
//!   [`RELEASE_FIELDS`] (`entered` + `release`), whatever it is called.
//!
//! Fix: hold a `PausePoint` instead (`pause.pause().await` /
//! `pause_blocking()` in the fake, `pause.reached(&mut op).await` and
//! `pause.release()` in the test). A struct that only looks like one opts
//! out with a standalone `// pause-point: allow — <reason>` on the line above
//! the `struct` keyword or above its first attribute; a marker without a
//! reason never exempts and is itself reported.
//!
//! Baseline (`tests/baselines/pause_point/<crate>.txt`, one file per crate):
//! `<path> <count>` per file that still has hand-rolled pause points. It only
//! ratchets down; `INTENTD_UPDATE_PAUSE_POINT_BASELINE=1` rewrites it after a
//! migration and refuses to raise or add an entry.

use std::collections::BTreeMap;

use intentd_test_support::source_lint::{
    count_baseline_file, markers_by_line, read_count_baselines, statement_line, test_code_files,
    tokenize, workspace_root, write_count_baselines, Marker, TestCodeText, Token,
};

const TAG: &str = "pause-point";
const BASELINE_DIR: &str = "crates/intentd-test-support/tests/baselines/pause_point";
const BASELINE_HEADER: &str = "# Hand-rolled test pause points not yet moved to \
intentd_test_support::PausePoint (pause_point_lint). Ratchets down only.";

/// CamelCase name words that make a struct a pause point.
const PAUSE_WORDS: &[&str] = &[
    "Gate",
    "Gates",
    "Pause",
    "Pauses",
    "Park",
    "Parks",
    "Hold",
    "Holds",
    "Latch",
    "Checkpoint",
    "Stall",
];
/// Field names for the "the operation got here" half of a pause point.
const ENTERED_FIELDS: &[&str] = &[
    "entered", "reached", "arrived", "parked", "paused", "started",
];
/// Field names for the "let it continue" half.
const RELEASE_FIELDS: &[&str] = &[
    "release", "released", "resume", "proceed", "unblock", "unpark", "go",
];
/// Channel-end and signal suffixes stripped from a field name before it is
/// matched (`entered_tx`, `release_rx`, `started_signal`).
const FIELD_SUFFIXES: &[&str] = &[
    "_tx",
    "_rx",
    "_sender",
    "_receiver",
    "_signal",
    "_notify",
    "_ch",
];

#[derive(Debug)]
struct Hit {
    line: usize,
    name: String,
    malformed: bool,
}

/// CamelCase words of `name`, each in `Title` case. An acronym run is one
/// word and ends before the capital that starts the next word, so
/// `GATEState` reads as `Gate`, `State` and `HTTPGate` as `Http`, `Gate`.
fn camel_words(name: &str) -> Vec<String> {
    let chars: Vec<char> = name.chars().collect();
    let mut words: Vec<String> = Vec::new();
    let mut current = String::new();
    for (i, &c) in chars.iter().enumerate() {
        if c == '_' {
            if !current.is_empty() {
                words.push(std::mem::take(&mut current));
            }
            continue;
        }
        let prev = i.checked_sub(1).map(|p| chars[p]);
        let next = chars.get(i + 1).copied();
        let starts_word = c.is_ascii_uppercase()
            && !current.is_empty()
            && (prev.is_some_and(|p| !p.is_ascii_uppercase())
                || next.is_some_and(|n| n.is_ascii_lowercase()));
        if starts_word {
            words.push(std::mem::take(&mut current));
        }
        current.push(c);
    }
    if !current.is_empty() {
        words.push(current);
    }
    words
        .into_iter()
        .map(|w| {
            let mut chars = w.chars();
            chars.next().map_or_else(String::new, |first| {
                first.to_ascii_uppercase().to_string() + &chars.as_str().to_ascii_lowercase()
            })
        })
        .collect()
}

/// `field` without one [`FIELD_SUFFIXES`] suffix.
fn field_stem(field: &str) -> &str {
    FIELD_SUFFIXES
        .iter()
        .find_map(|suffix| field.strip_suffix(suffix))
        .filter(|stem| !stem.is_empty())
        .unwrap_or(field)
}

/// Index of the token that ends the `struct` item whose keyword is at `at`:
/// the `;` of a unit or tuple struct, or the `}` closing its body.
fn struct_end(tokens: &[Token], at: usize) -> usize {
    let depth = tokens[at].depth;
    let mut i = at + 1;
    while i < tokens.len() {
        let t = &tokens[i];
        if t.depth == depth && (t.is(";") || t.is("}")) {
            return i;
        }
        i += 1;
    }
    tokens.len() - 1
}

/// Named fields of the `{ … }` body opening at token `open`.
fn field_names(tokens: &[Token], open: usize) -> Vec<String> {
    let depth = tokens[open].depth + 1;
    let mut out = Vec::new();
    let mut i = open + 1;
    while i < tokens.len() && !(tokens[i].is("}") && tokens[i].depth + 1 == depth) {
        if tokens[i].depth == depth
            && tokens[i].is_ident()
            && tokens.get(i + 1).is_some_and(|t| t.is(":"))
        {
            out.push(tokens[i].text.clone());
        }
        i += 1;
    }
    out
}

fn why(tokens: &[Token], at: usize) -> Option<String> {
    let name = &tokens[at + 1].text;
    // A fake that holds the shared helper is the migration, not a hand-rolled
    // pause point (`struct WritePause { pause: Arc<PausePoint> }`).
    if tokens[at + 1..=struct_end(tokens, at)]
        .iter()
        .any(|t| t.is("PausePoint"))
    {
        return None;
    }
    if camel_words(name)
        .iter()
        .any(|w| PAUSE_WORDS.contains(&w.as_str()))
    {
        return Some(name.clone());
    }
    let open = tokens[at + 2..]
        .iter()
        .position(|t| t.is("{") || t.is(";") || t.is("("))
        .map(|p| at + 2 + p)
        .filter(|p| tokens[*p].is("{"))?;
    let fields = field_names(tokens, open);
    let has = |set: &[&str]| fields.iter().any(|f| set.contains(&field_stem(f)));
    (has(ENTERED_FIELDS) && has(RELEASE_FIELDS)).then(|| name.clone())
}

fn scan_source(src: &str, whole: bool) -> Vec<Hit> {
    let text = TestCodeText::new(src, whole);
    let markers = markers_by_line(src, &text.lexed.line_comments, TAG);
    let marker = |line: usize| {
        line.checked_sub(1)
            .and_then(|above| markers.get(above))
            .copied()
            .unwrap_or(Marker::Absent)
    };
    let tokens = tokenize(&text.chars);
    let mut out = Vec::new();
    for (at, token) in tokens.iter().enumerate() {
        if !token.is("struct")
            || !tokens.get(at + 1).is_some_and(Token::is_ident)
            || !text.in_test(token.start)
        {
            continue;
        }
        let Some(name) = why(&tokens, at) else {
            continue;
        };
        let consulted = [
            marker(token.line),
            marker(statement_line(&text.chars, token.start)),
        ];
        if consulted.contains(&Marker::WithReason) {
            continue;
        }
        out.push(Hit {
            line: token.line,
            name,
            malformed: consulted.contains(&Marker::Malformed),
        });
    }
    out
}

fn report(found: &BTreeMap<String, Vec<Hit>>, baseline: &BTreeMap<String, usize>) -> String {
    let mut out = Vec::new();
    for (path, hits) in found {
        for hit in hits.iter().filter(|h| h.malformed) {
            out.push(format!(
                "{path}:{}: `// {TAG}: allow` marker above `{}` needs a reason \
                 (`// {TAG}: allow — <reason>`)",
                hit.line, hit.name
            ));
        }
        let count = hits.len();
        let file = count_baseline_file(BASELINE_DIR, path);
        match baseline.get(path).copied() {
            Some(allowed) if count == allowed => {}
            None if count == 0 => {}
            Some(allowed) if count < allowed => out.push(if count == 0 {
                format!(
                    "{path}: no hand-rolled pause points left but the baseline allows {allowed}; \
                     remove its line from {file}: `{path} {allowed}`"
                )
            } else {
                format!(
                    "{path}: {count} hand-rolled pause point(s) but the baseline allows \
                     {allowed}; replace its line in {file} with: `{path} {count}`"
                )
            }),
            allowed => {
                let allowed = allowed.map_or_else(
                    || "the baseline has no entry for it".to_string(),
                    |n| format!("the baseline allows {n}"),
                );
                out.push(format!(
                    "{path}: {count} hand-rolled pause point(s), {allowed}:"
                ));
                for hit in hits {
                    out.push(format!("  {path}:{}: struct {}", hit.line, hit.name));
                }
                out.push(
                    "  fix: hold an `intentd_test_support::PausePoint` instead — the fake calls \
                     `pause.pause().await` (or `pause_blocking()`), the test calls \
                     `pause.reached(&mut op).await` then `pause.release()`. Its waits are bounded \
                     by `test_timeout` even under plain `cargo test`, and `reached` fails with \
                     the operation's error if it ends before the pause point (a hand-rolled \
                     pause point hung a test for six hours on 2026-11-27). A struct that only \
                     looks like one: `// pause-point: allow — <reason>` on the line above."
                        .to_string(),
                );
            }
        }
    }
    for (path, allowed) in baseline {
        if !found.contains_key(path) {
            out.push(format!(
                "{path}: no longer exists; remove its line from {}: `{path} {allowed}`",
                count_baseline_file(BASELINE_DIR, path)
            ));
        }
    }
    out.join("\n")
}

#[test]
fn test_pause_points_use_the_bounded_helper() {
    let root = workspace_root();
    let baseline = read_count_baselines(&root, BASELINE_DIR, "hand-rolled pause points")
        .unwrap_or_else(|e| panic!("{e}"));
    let found: BTreeMap<String, Vec<Hit>> = test_code_files(&root)
        .into_iter()
        // Blanking only removes text, so a file without `struct` has no hit;
        // a baselined one is kept so its entry reads "none left", not "gone".
        .filter(|file| file.src.contains("struct") || baseline.contains_key(&file.rel))
        .map(|file| {
            let hits = scan_source(&file.src, file.whole);
            (file.rel, hits)
        })
        .collect();
    if std::env::var_os("INTENTD_UPDATE_PAUSE_POINT_BASELINE").is_some() {
        let raised: Vec<String> = found
            .iter()
            .filter(|(path, hits)| hits.len() > baseline.get(*path).copied().unwrap_or(0))
            .map(|(path, hits)| format!("{path} {}", hits.len()))
            .collect();
        let malformed = found.values().flatten().any(|h| h.malformed);
        assert!(
            raised.is_empty() && !malformed,
            "update mode only lowers the baselines in {BASELINE_DIR}/; fix these first:\n{}",
            report(&found, &baseline)
        );
        let counts = found
            .iter()
            .map(|(path, hits)| (path.clone(), hits.len()))
            .collect();
        write_count_baselines(&root, BASELINE_DIR, BASELINE_HEADER, &counts)
            .unwrap_or_else(|e| panic!("{e}"));
        return;
    }
    let report = report(&found, &baseline);
    assert!(
        report.is_empty(),
        "hand-rolled test pause points out of step with {BASELINE_DIR}/:\n\n{report}\n"
    );
}

// ---- fixtures ---------------------------------------------------------------

fn names(src: &str, whole: bool) -> Vec<(usize, String)> {
    scan_source(src, whole)
        .into_iter()
        .map(|h| (h.line, h.name))
        .collect()
}

#[test]
fn a_new_ad_hoc_pause_struct_is_flagged() {
    let src = "\
struct Gate {
    entered: Notify,
    release: Semaphore,
}
pub(crate) struct WritePause;
struct ParkLease(u8);
";
    assert_eq!(
        names(src, true),
        vec![
            (1, "Gate".into()),
            (5, "WritePause".into()),
            (6, "ParkLease".into())
        ]
    );
}

#[test]
fn an_entered_and_release_pair_is_flagged_under_any_name() {
    let src = "struct Rendezvous {\n    entered: oneshot::Sender<()>,\n    \
               release: mpsc::Receiver<()>,\n}\n";
    assert_eq!(names(src, true), vec![(1, "Rendezvous".into())]);
}

#[test]
fn ordinary_structs_and_adjectives_pass() {
    let src = "\
struct GatedSecrets { inner: Store, gate: Mutex<Option<u8>> }
struct PausedStore { entered: bool }
struct Gateway { release: u8 }
struct Nested { a: Inner }
struct Inner { entered: u8 }
struct Outer { inner: Inner, release: u8 }
";
    assert!(names(src, true).is_empty(), "{:?}", names(src, true));
}

#[test]
fn suffixed_channel_fields_and_started_go_pairs_are_flagged() {
    let src = "\
struct HeldClaim { started: Arc<Notify>, release: Arc<Notify> }
struct Handshake { entered_tx: oneshot::Sender<()>, release_rx: mpsc::Receiver<()> }
struct Starter { started_signal: Notify, go: Semaphore }
struct Timing { started: Instant, elapsed: Duration }
";
    assert_eq!(
        names(src, true),
        vec![
            (1, "HeldClaim".into()),
            (2, "Handshake".into()),
            (3, "Starter".into())
        ]
    );
}

#[test]
fn a_fake_holding_the_shared_helper_passes() {
    let src = "\
struct WritePause { pause: Arc<PausePoint>, op: &'static str }
struct StoreGate(Arc<intentd_test_support::PausePoint>);
struct Gate { entered: Notify, release: Semaphore }
";
    assert_eq!(names(src, true), vec![(3, "Gate".into())]);
}

#[test]
fn acronym_runs_split_into_words() {
    assert_eq!(camel_words("GATEState"), ["Gate", "State"]);
    assert_eq!(camel_words("HTTPGate"), ["Http", "Gate"]);
    assert_eq!(camel_words("write_pause"), ["Write", "Pause"]);
    assert_eq!(camel_words("GatedSecrets"), ["Gated", "Secrets"]);
    assert_eq!(camel_words("Gateway"), ["Gateway"]);
}

#[test]
fn only_test_code_is_scanned() {
    let src = "\
pub struct Gate { entered: u8, release: u8 }
#[cfg(test)]
mod tests {
    struct Pause { entered: u8, release: u8 }
}
";
    assert_eq!(names(src, false), vec![(4, "Pause".into())]);
}

#[test]
fn a_marker_with_a_reason_exempts_and_one_without_is_reported() {
    let src = "\
// pause-point: allow — admission gate under test, not a pause point
struct RemovalAdmissionGate;
#[derive(Default)]
// pause-point: allow
struct Gate;
// pause-point: allow — derive above is covered
#[derive(Default)]
struct Hold;
";
    let hits = scan_source(src, true);
    assert_eq!(hits.len(), 1, "{hits:?}");
    assert_eq!(hits[0].line, 5);
    assert!(hits[0].malformed);
}

#[test]
fn comments_and_strings_never_count() {
    let src = "// struct Gate { entered: u8, release: u8 }\nconst S: &str = \"struct Pause;\";\n";
    assert!(names(src, true).is_empty());
}

#[test]
fn report_follows_the_ratchet() {
    let hit = |line| Hit {
        line,
        name: "Gate".into(),
        malformed: false,
    };
    let path = "crates/x/tests/a.rs".to_string();
    let mut found = BTreeMap::new();
    found.insert(path.clone(), vec![hit(1), hit(2)]);
    let mut baseline = BTreeMap::new();
    assert!(report(&found, &baseline).contains("the baseline has no entry"));
    baseline.insert(path.clone(), 2);
    assert!(report(&found, &baseline).is_empty());
    baseline.insert(path.clone(), 3);
    assert!(report(&found, &baseline).contains("`crates/x/tests/a.rs 2`"));
    baseline.insert("crates/x/tests/gone.rs".into(), 1);
    assert!(report(&found, &baseline).contains("no longer exists"));
}
