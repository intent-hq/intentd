//! Source-scan guard: service-layer background tasks bind a `Caller`.
//!
//! Every `intent-services` capability gate refuses an unbound request
//! (`current_caller() == None`, fail-closed — multiplayer w3). `Caller` is a
//! `tokio::task_local!`, so a bare `tokio::spawn` drops the binding and the
//! spawned work reaches the gates unbound: the event fan-out, refreshers,
//! timers and finalisers would all be refused. Production code in
//! `intent-services` and the `intentd` composition root therefore spawns
//! daemon-internal work through `intent_core::spawn_daemon` (binds
//! `Caller::Daemon`) or re-establishes the request's caller with
//! `intent_core::with_caller`; this test fails naming the `file:line` of every
//! bare `tokio::spawn(` in that production code. (The transport re-binds the
//! request caller in its own spawn sites and is covered by its e2e suite.)
//!
//! A spawn that provably never reaches the service layer may opt out with a
//! trailing `// caller-binding: allow — <reason>`; the marker must be the
//! line's trailing comment and the reason is required.

use std::fs;
use std::path::{Path, PathBuf};

const ALLOW_MARKER: &str = "// caller-binding: allow";
const NEEDLE: &str = "tokio::spawn(";
const SCANNED_DIRS: &[&str] = &["crates/intent-services/src", "crates/intentd/src"];

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

/// `tests.rs`, `*_tests.rs`, `test_support.rs` and anything under a `tests/`
/// directory are test code and out of scope.
fn is_test_file(rel: &Path) -> bool {
    let name = rel.file_name().and_then(|n| n.to_str()).unwrap_or("");
    name == "tests.rs"
        || name == "test_support.rs"
        || name.ends_with("_tests.rs")
        || rel.components().any(|c| c.as_os_str() == "tests")
}

/// Line index ranges (0-based, half-open) of `#[cfg(test)] mod … { … }`
/// blocks, matched by brace depth so inline unit tests are skipped.
fn inline_test_ranges(lines: &[&str]) -> Vec<(usize, usize)> {
    let mut ranges = Vec::new();
    let mut i = 0;
    while i < lines.len() {
        let is_cfg_test = lines[i].trim() == "#[cfg(test)]";
        let opens_mod = lines
            .get(i + 1)
            .is_some_and(|l| l.trim_start().starts_with("mod ") && l.trim_end().ends_with('{'));
        if is_cfg_test && opens_mod {
            let mut depth = 0usize;
            let mut j = i + 1;
            while j < lines.len() {
                depth += lines[j].matches('{').count();
                depth = depth.saturating_sub(lines[j].matches('}').count());
                if depth == 0 {
                    break;
                }
                j += 1;
            }
            ranges.push((i, j + 1));
            i = j + 1;
        } else {
            i += 1;
        }
    }
    ranges
}

fn split_comment(line: &str) -> (&str, Option<&str>) {
    let bytes = line.as_bytes();
    let mut in_str = false;
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'\\' if in_str => i += 1,
            b'"' => in_str = !in_str,
            b'/' if !in_str && bytes.get(i + 1) == Some(&b'/') => {
                return (&line[..i], Some(&line[i..]));
            }
            _ => {}
        }
        i += 1;
    }
    (line, None)
}

fn has_allow_marker(comment: Option<&str>) -> bool {
    let Some(rest) = comment
        .map(str::trim_start)
        .and_then(|c| c.strip_prefix(ALLOW_MARKER))
    else {
        return false;
    };
    let reason =
        rest.trim_start_matches(|c: char| c.is_whitespace() || matches!(c, '—' | '-' | ':'));
    !reason.trim().is_empty()
}

fn is_bare_spawn(line: &str) -> bool {
    let (code, comment) = split_comment(line);
    code.contains(NEEDLE) && !has_allow_marker(comment)
}

fn scan(root: &Path) -> (usize, Vec<String>) {
    let mut files = Vec::new();
    for dir in SCANNED_DIRS {
        collect_rs_files(&root.join(dir), &mut files);
    }
    files.retain(|p| !is_test_file(p.strip_prefix(root).unwrap_or(p)));
    files.sort();
    let mut offenders = Vec::new();
    for file in &files {
        let src = fs::read_to_string(file).expect("read source");
        let lines: Vec<&str> = src.lines().collect();
        let skip = inline_test_ranges(&lines);
        for (i, line) in lines.iter().enumerate() {
            if skip.iter().any(|(a, b)| (*a..*b).contains(&i)) {
                continue;
            }
            if is_bare_spawn(line) {
                let rel = file.strip_prefix(root).unwrap_or(file);
                offenders.push(format!("{}:{}", rel.display(), i + 1));
            }
        }
    }
    (files.len(), offenders)
}

#[test]
fn service_layer_spawns_bind_a_caller() {
    let root = workspace_root();
    let (scanned, offenders) = scan(&root);
    assert!(
        scanned > 0,
        "no production sources found under {SCANNED_DIRS:?}"
    );
    assert!(
        offenders.is_empty(),
        "bare `tokio::spawn(` in service-layer production code drops the task-local `Caller`; \
         use `intent_core::spawn_daemon` (or re-bind with `with_caller`), or annotate a spawn \
         that provably never reaches a capability gate with `{ALLOW_MARKER} — <reason>`:\n  {}",
        offenders.join("\n  ")
    );
}

#[test]
fn bare_spawn_detection_respects_marker_and_comments() {
    assert!(is_bare_spawn("    tokio::spawn(async move {"));
    assert!(is_bare_spawn(
        "    tokio::spawn(async move {}); // caller-binding: allow"
    ));
    assert!(is_bare_spawn(
        "    tokio::spawn(async move {}); // caller-binding: allow —"
    ));
    assert!(!is_bare_spawn(
        "    tokio::spawn(async move {}); // caller-binding: allow — never touches the api"
    ));
    assert!(!is_bare_spawn(
        "    tokio::spawn(async move {}); // caller-binding: allow: never touches the api"
    ));
    assert!(!is_bare_spawn("    // tokio::spawn(async move {"));
    assert!(!is_bare_spawn(
        "    /// wraps `tokio::spawn(` with a bound caller"
    ));
    assert!(!is_bare_spawn("    spawn_daemon(async move {"));
}

#[test]
fn inline_test_modules_are_skipped() {
    let src = [
        "fn prod() {",
        "    tokio::spawn(async {});",
        "}",
        "#[cfg(test)]",
        "mod tests {",
        "    fn t() {",
        "        tokio::spawn(async {});",
        "    }",
        "}",
        "fn after() { tokio::spawn(async {}); }",
    ];
    let ranges = inline_test_ranges(&src);
    assert_eq!(ranges, vec![(3, 9)]);
    let flagged: Vec<usize> = src
        .iter()
        .enumerate()
        .filter(|(i, l)| !ranges.iter().any(|(a, b)| (*a..*b).contains(i)) && is_bare_spawn(l))
        .map(|(i, _)| i + 1)
        .collect();
    assert_eq!(flagged, vec![2, 10]);
}
