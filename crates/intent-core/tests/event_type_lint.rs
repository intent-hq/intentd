//! Event-type literal lint.
//!
//! `intent_core::events::ALL_EVENT_TYPES` is the canonical event catalog and
//! is mirrored into the checked-in golden `tests/goldens/event_types.json`
//! that clients copy verbatim. An emitter that spells a `note:` / `task:` /
//! `workspace:` / `agent:` type as a bare string literal can drift from that
//! catalog silently. This source-scanning test fails, naming `file:line`,
//! for every string literal under `crates/*/src/**/*.rs` that looks like an
//! event type in one of those namespaces but is not in `ALL_EVENT_TYPES`.
//!
//! The heuristic is deliberately small:
//!
//! - A literal "looks like an event type" when its whole content matches
//!   `(note|task|workspace|agent):[A-Za-z:_-]+`. Wildcard subscription
//!   patterns (`"task:*"`), bare namespaces (`"agent:"`), format strings, and
//!   prose containing a type are therefore never considered.
//! - A literal ending in `:` is a documented prefix and passes when some
//!   catalog type starts with it (`"agent:stream:"`).
//! - Skipped: `crates/intent-core/src/events.rs` (the catalog itself), any
//!   file named `tests.rs` or under a `tests/` directory, any module file
//!   declared as `#[cfg(test)] mod name;` (and everything under its
//!   directory), and any `#[cfg(test)]` item, attribute to end of item.
//!   Comments are ignored, so a type quoted in a doc comment never counts.
//! - Opt-out: `// event-type-lint: allow — <reason>` on the line immediately
//!   above the literal's line. A malformed marker never suppresses the hit;
//!   the report says so. The opt-out is for non-event uses of such a string
//!   (a message-metadata pseudo-type, a fixture); a real emitter must be
//!   added to the catalog.
//!
//! The lexer (comment / literal blanking, escape cooking), the `#[cfg(test)]`
//! item bounds, the opt-out marker grammar, and the crate walk are the shared
//! scaffolding in `intentd_test_support::source_lint`; only the rule, the
//! catalog lookup, and the `#[cfg(test)] mod name;` → module-file mapping
//! live here.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Component, Path, PathBuf};

use intent_core::events::ALL_EVENT_TYPES;
use intentd_test_support::source_lint::{
    cfg_test_items, crate_src_files, lex, markers_by_line, workspace_root, Marker,
};

const NAMESPACES: &[&str] = &["note", "task", "workspace", "agent"];
const OPT_OUT_TAG: &str = "event-type-lint";
const EXEMPT_FILE: &[&str] = &["crates", "intent-core", "src", "events.rs"];

#[derive(Debug, Clone, PartialEq, Eq)]
struct Hit {
    line: usize,
    literal: String,
    /// The line above carried something that starts like the opt-out marker
    /// but is malformed (longer token, or no reason).
    marker_malformed: bool,
}

/// One scanned file: its unsuppressed hits and the names of the modules it
/// declares as `#[cfg(test)] mod name;` (whose files are test code too).
struct Scanned {
    hits: Vec<Hit>,
    test_mods: Vec<String>,
}

/// Whether the whole literal matches `(note|task|workspace|agent):[A-Za-z:_-]+`.
fn looks_like_event_type(literal: &str) -> bool {
    let Some((namespace, rest)) = literal.split_once(':') else {
        return false;
    };
    NAMESPACES.contains(&namespace)
        && !rest.is_empty()
        && rest
            .chars()
            .all(|c| c.is_ascii_alphabetic() || matches!(c, ':' | '_' | '-'))
}

/// Whether an event-looking literal is in the catalog, or is a prefix of a
/// catalog type (a documented prefix such as `"agent:stream:"`).
fn is_catalogued(literal: &str) -> bool {
    ALL_EVENT_TYPES.contains(&literal)
        || (literal.ends_with(':') && ALL_EVENT_TYPES.iter().any(|t| t.starts_with(literal)))
}

/// Scans one Rust source file's text: every event-looking literal (on its
/// cooked value) outside `#[cfg(test)]` items that is not catalogued and not
/// suppressed by a reasoned opt-out marker on the line above, plus the file's
/// `#[cfg(test)] mod name;` declarations.
fn scan_source(src: &str) -> Scanned {
    let lexed = lex(src);
    let markers = markers_by_line(src, &lexed.line_comments, OPT_OUT_TAG);
    let chars: Vec<char> = lexed.blanked.chars().collect();
    let items = cfg_test_items(&chars);
    let test_mods = items
        .iter()
        .filter_map(|item| item.out_of_line_mod.clone())
        .collect();
    let hits = lexed
        .literals
        .into_iter()
        .filter(|lit| {
            !items
                .iter()
                .any(|item| (item.start..item.end).contains(&lit.offset))
        })
        .map(|lit| (lit.line, lit.cooked()))
        .filter(|(_, text)| looks_like_event_type(text) && !is_catalogued(text))
        .filter_map(|(line, literal)| {
            let marker = markers.get(line - 1).copied().unwrap_or(Marker::Absent);
            match marker {
                Marker::WithReason => None,
                Marker::Malformed | Marker::Absent => Some(Hit {
                    line,
                    literal,
                    marker_malformed: marker == Marker::Malformed,
                }),
            }
        })
        .collect();
    Scanned { hits, test_mods }
}

fn is_exempt(rel: &Path) -> bool {
    let parts: Vec<_> = rel.components().map(Component::as_os_str).collect();
    parts.len() == EXEMPT_FILE.len()
        && parts
            .iter()
            .zip(EXEMPT_FILE)
            .all(|(have, want)| *have == *want)
}

fn display_rel(rel: &Path) -> String {
    rel.components()
        .map(|c| c.as_os_str().to_string_lossy().into_owned())
        .collect::<Vec<_>>()
        .join("/")
}

/// The directory holding the out-of-line child modules of `file`:
/// `lib.rs` / `main.rs` / `mod.rs` own their directory, any other `foo.rs`
/// owns `foo/`.
fn child_module_dir(file: &Path) -> PathBuf {
    let parent = file.parent().unwrap_or(Path::new(""));
    match file.file_name().and_then(|n| n.to_str()) {
        Some("lib.rs" | "main.rs" | "mod.rs") => parent.to_path_buf(),
        _ => parent.join(file.file_stem().unwrap_or_default()),
    }
}

/// Paths (a `name.rs` file and a `name/` directory) that hold the module
/// `name` declared from `file`.
fn test_module_paths(file: &Path, name: &str) -> [PathBuf; 2] {
    let dir = child_module_dir(file);
    [dir.join(format!("{name}.rs")), dir.join(name)]
}

/// Whether `file` is (or lives under) one of the collected test-module paths.
fn is_test_module_file(file: &Path, test_paths: &BTreeSet<PathBuf>) -> bool {
    test_paths.iter().any(|p| file == p || file.starts_with(p))
}

#[test]
fn event_type_literals_are_in_the_catalog() {
    let root = workspace_root();
    let exempt: PathBuf = EXEMPT_FILE.iter().collect();
    assert!(
        root.join(&exempt).is_file(),
        "{} moved; update EXEMPT_FILE so the exemption keeps pointing at the catalog",
        display_rel(&exempt)
    );

    let files = crate_src_files(&root);
    assert!(
        !files.is_empty(),
        "no Rust sources found under {}",
        root.join("crates").display()
    );

    let mut scanned = Vec::new();
    let mut test_paths = BTreeSet::new();
    for file in &files {
        let rel = file
            .strip_prefix(&root)
            .expect("source path under the workspace root");
        if is_exempt(rel) {
            continue;
        }
        let src =
            fs::read_to_string(file).unwrap_or_else(|e| panic!("read {}: {e}", file.display()));
        let result = scan_source(&src);
        for name in &result.test_mods {
            test_paths.extend(test_module_paths(file, name));
        }
        scanned.push((file, result.hits));
    }

    let mut report = Vec::new();
    for (file, hits) in scanned {
        if is_test_module_file(file, &test_paths) {
            continue;
        }
        let rel = file.strip_prefix(&root).expect("under root");
        for hit in hits {
            let note = if hit.marker_malformed {
                "  (opt-out marker is malformed: expected `// event-type-lint: allow — <reason>`)"
            } else {
                ""
            };
            report.push(format!(
                "{}:{}: \"{}\"{note}",
                display_rel(rel),
                hit.line,
                hit.literal
            ));
        }
    }

    assert!(
        report.is_empty(),
        "event-type string literals outside intent_core::events::ALL_EVENT_TYPES:\n\n{}\n\n\
         A real emitter must add its type to `ALL_EVENT_TYPES` in \
         crates/intent-core/src/events.rs (then regenerate the golden with \
         `INTENTD_UPDATE_GOLDENS=1 cargo test -p intent-core --test events`) and use the \
         constant. A string that is not an emitted event type may opt out with \
         `// event-type-lint: allow — <reason>` on the line immediately above; the reason is \
         required.",
        report.join("\n")
    );
}

// ---- scanner fixtures -------------------------------------------------------

/// 1-based line of the first line containing `needle`.
fn line_of(src: &str, needle: &str) -> usize {
    src.lines()
        .position(|l| l.contains(needle))
        .map_or_else(|| panic!("fixture lacks {needle:?}"), |i| i + 1)
}

fn hit_lines(src: &str) -> Vec<usize> {
    scan_source(src).hits.into_iter().map(|h| h.line).collect()
}

#[test]
fn flags_an_uncatalogued_literal_naming_its_line() {
    let src = r#"
fn emit(bus: &Bus) {
    bus.publish(NewEvent {
        event_type: "task:bogus".to_string(),
        ..Default::default()
    });
}
"#;
    let scanned = scan_source(src);
    assert_eq!(
        scanned.hits.iter().map(|h| h.line).collect::<Vec<_>>(),
        vec![line_of(src, "task:bogus")]
    );
    assert_eq!(scanned.hits[0].literal, "task:bogus");
    assert!(!scanned.hits[0].marker_malformed);
}

#[test]
fn catalogued_types_and_prefixes_pass() {
    let src = r#"
fn ok() -> Vec<&'static str> {
    vec!["task:status-changed", "note:created", "workspace:updated", "agent:idle", "agent:stream:"]
}
"#;
    assert_eq!(hit_lines(src), Vec::<usize>::new());
}

#[test]
fn non_event_shapes_are_never_considered() {
    let src = r#"
fn shapes(id: &str) -> Vec<String> {
    vec![
        "task:*".to_string(),
        "agent:".to_string(),
        format!("agent:{id}"),
        "expected task:status-changed here".to_string(),
        "file:changed".to_string(),
        "TASK:BOGUS".to_string(),
        "task:bogus/".to_string(),
    ]
}
"#;
    assert_eq!(hit_lines(src), Vec::<usize>::new());
}

#[test]
fn unknown_prefix_is_flagged() {
    let src = r#"
fn f() -> &'static str { "agent:bogus:" }
"#;
    assert_eq!(hit_lines(src), vec![2]);
}

#[test]
fn literals_in_comments_are_ignored() {
    let src = r#"
/// Emits `"task:bogus"` — see "workspace:nope".
fn f() -> &'static str {
    // "note:bogus"
    /* "agent:bogus" */
    "task:status-changed"
}
"#;
    assert_eq!(hit_lines(src), Vec::<usize>::new());
}

#[test]
fn escaped_literals_are_classified_on_their_cooked_value() {
    let unicode = "fn f() -> &'static str { \"task\\u{3a}bogus\" }\n";
    let scanned = scan_source(unicode);
    assert_eq!(
        scanned.hits.iter().map(|h| h.line).collect::<Vec<_>>(),
        vec![1]
    );
    assert_eq!(scanned.hits[0].literal, "task:bogus");
    let hex = "fn f() -> &'static str { \"\\x74ask:\\x62ogus\" }\n";
    assert_eq!(hit_lines(hex), vec![1]);
    let continuation = "fn f() -> &'static str { \"task:\\\n        bogus\" }\n";
    assert_eq!(hit_lines(continuation), vec![1]);
    let cooked_to_non_event = "fn f() -> &'static str { \"task:bo\\ngus\" }\n";
    assert_eq!(hit_lines(cooked_to_non_event), Vec::<usize>::new());
    let raw_is_not_cooked = "fn f() -> &'static str { r\"task\\u{3a}bogus\" }\n";
    assert_eq!(hit_lines(raw_is_not_cooked), Vec::<usize>::new());
}

#[test]
fn cfg_test_items_are_skipped_but_code_after_them_is_not() {
    let src = r#"
#[cfg(test)]
mod tests {
    fn fixture() -> &'static str { "task:bogus" }
}

#[cfg(test)]
mod more_tests;

#[cfg(test)]
fn helper() -> &'static str { "note:bogus" }

#[cfg(test)]
const FIXTURE: &str = if cfg!(a) { "workspace:bogus" } else { "agent:bogus" };

#[cfg( test )]
pub(crate) fn spaced() -> &'static str { "task:also-bogus" }

#[cfg(not(test))]
fn real() -> &'static str { "task:bogus" }
"#;
    let scanned = scan_source(src);
    assert_eq!(
        scanned.hits.iter().map(|h| h.line).collect::<Vec<_>>(),
        vec![line_of(src, "fn real()")]
    );
    assert_eq!(scanned.test_mods, vec!["more_tests".to_string()]);
}

#[test]
fn cfg_test_mod_declarations_map_to_module_files() {
    let lib = Path::new("crates/x/src/lib.rs");
    assert_eq!(
        test_module_paths(lib, "v1_goldens"),
        [
            PathBuf::from("crates/x/src/v1_goldens.rs"),
            PathBuf::from("crates/x/src/v1_goldens"),
        ]
    );
    let events_mod = Path::new("crates/x/src/events/mod.rs");
    assert_eq!(
        test_module_paths(events_mod, "bus_tests")[0],
        PathBuf::from("crates/x/src/events/bus_tests.rs")
    );
    let plain = Path::new("crates/x/src/conflate.rs");
    assert_eq!(
        test_module_paths(plain, "fixtures"),
        [
            PathBuf::from("crates/x/src/conflate/fixtures.rs"),
            PathBuf::from("crates/x/src/conflate/fixtures"),
        ]
    );
    let test_paths: BTreeSet<PathBuf> = test_module_paths(lib, "v1_goldens").into_iter().collect();
    assert!(is_test_module_file(
        Path::new("crates/x/src/v1_goldens.rs"),
        &test_paths
    ));
    assert!(is_test_module_file(
        Path::new("crates/x/src/v1_goldens/nested.rs"),
        &test_paths
    ));
    assert!(!is_test_module_file(
        Path::new("crates/x/src/v1_goldens_extra.rs"),
        &test_paths
    ));
}

#[test]
fn opt_out_marker_with_a_reason_suppresses_the_literal() {
    let src = r#"
fn wake_metadata() -> serde_json::Value {
    json!({
        // event-type-lint: allow — message-metadata pseudo-type, not a bus event
        "eventTypes": ["agent:reportToParent"],
    })
}
"#;
    assert_eq!(hit_lines(src), Vec::<usize>::new());
}

#[test]
fn opt_out_marker_without_a_reason_still_fails() {
    for marker in [
        "// event-type-lint: allow",
        "// event-type-lint: allow —",
        "// event-type-lint: allow -",
        "// event-type-lint: allow reason without a dash",
        "// event-type-lint: allowance",
        "// event-type-lint: allow_me — reason",
    ] {
        let src = format!("fn f() -> &'static str {{\n    {marker}\n    \"task:bogus\"\n}}\n");
        let scanned = scan_source(&src);
        assert_eq!(scanned.hits.len(), 1, "{marker:?}: {:?}", scanned.hits);
        assert_eq!(scanned.hits[0].line, 3, "{marker:?}");
        assert!(scanned.hits[0].marker_malformed, "{marker:?}");
    }
}

#[test]
fn opt_out_marker_must_sit_immediately_above_the_literal() {
    let src = "fn f() -> &'static str {\n    // event-type-lint: allow — reason\n\n    \"task:bogus\"\n}\n";
    let scanned = scan_source(src);
    assert_eq!(hit_lines(src), vec![4]);
    assert!(!scanned.hits[0].marker_malformed);
}

#[test]
fn only_the_catalog_is_exempt() {
    let exempt: PathBuf = EXEMPT_FILE.iter().collect();
    assert!(is_exempt(&exempt));
    let sibling: PathBuf = ["crates", "intent-core", "src", "model.rs"]
        .iter()
        .collect();
    assert!(!is_exempt(&sibling));
    assert_eq!(display_rel(&exempt), "crates/intent-core/src/events.rs");
}
