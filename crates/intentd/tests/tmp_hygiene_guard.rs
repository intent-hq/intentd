//! Source-scan guard: test code must not construct raw temp paths.
//!
//! Every scratch dir a test creates must come from the shared helpers
//! (`intentd/tests/common::test_tempdir` / `test_tempdir_in`, or
//! `intent_services::test_support::test_tempdir`), which sweep the dir on
//! drop (including on panic) and honor `INTENTD_TEST_KEEP_TMP`. A raw
//! `PathBuf::from("/tmp")` / `Path::new("/tmp")` / `temp_dir().join(..)` in a
//! test file bypasses that and leaks into `/tmp` — this test fails naming
//! the offending `file:line`s so the leak class cannot return silently.
//!
//! Pure path arithmetic that never touches the filesystem may opt out with a
//! trailing `// tmp-hygiene: allow — <reason>` on the flagged line.

use std::fs;
use std::path::{Path, PathBuf};

const ALLOW_MARKER: &str = "// tmp-hygiene: allow";

/// Files that *define* the helpers (and this guard, whose docs quote the
/// patterns).
const EXEMPT_FILES: &[&str] = &[
    "crates/intentd/tests/common/mod.rs",
    "crates/intent-services/src/test_support.rs",
    "crates/intentd/tests/tmp_hygiene_guard.rs",
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

/// `crates/*/src/**/{tests.rs,*_tests.rs}` and anything under a
/// `crates/*/src/**/tests/` directory.
fn is_src_test_file(rel: &Path) -> bool {
    let name = rel.file_name().and_then(|n| n.to_str()).unwrap_or("");
    name == "tests.rs"
        || name.ends_with("_tests.rs")
        || rel.components().any(|c| c.as_os_str() == "tests")
}

fn scanned_files(root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    let Ok(crates) = fs::read_dir(root.join("crates")) else {
        panic!("no crates/ under {}", root.display());
    };
    for krate in crates.flatten() {
        let krate = krate.path();
        collect_rs_files(&krate.join("tests"), &mut files);
        let mut src = Vec::new();
        collect_rs_files(&krate.join("src"), &mut src);
        files.extend(src.into_iter().filter(|p| {
            let rel = p.strip_prefix(&krate).unwrap_or(p);
            is_src_test_file(rel.strip_prefix("src").unwrap_or(rel))
        }));
    }
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

fn is_raw_temp_path(line: &str, next: Option<&str>) -> bool {
    let compact = strip_ws(code_part(line));
    if compact.contains(r#"PathBuf::from("/tmp")"#)
        || compact.contains(r#"Path::new("/tmp")"#)
        || compact.contains("temp_dir().join(")
    {
        return true;
    }
    // `temp_dir()` at end of line with `.join(` continuing on the next one.
    compact.ends_with("temp_dir()")
        && next.is_some_and(|n| strip_ws(code_part(n)).starts_with(".join("))
}

fn scan(root: &Path) -> Vec<String> {
    let mut offenders = Vec::new();
    for file in scanned_files(root) {
        let src = fs::read_to_string(&file).expect("read test source");
        let lines: Vec<&str> = src.lines().collect();
        for (i, line) in lines.iter().enumerate() {
            if line.contains(ALLOW_MARKER) {
                continue;
            }
            if is_raw_temp_path(line, lines.get(i + 1).copied()) {
                let rel = file.strip_prefix(root).unwrap_or(&file);
                offenders.push(format!("{}:{}", rel.display(), i + 1));
            }
        }
    }
    offenders
}

#[test]
fn test_code_uses_shared_tempdir_helpers() {
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
        "\nraw temp paths in test code ({} site{}):\n  {}\n\n\
         Use `common::test_tempdir(prefix)` / `common::test_tempdir_in(\"/tmp\", prefix)` \
         (crates/intentd/tests/common/mod.rs) or `crate::test_support::test_tempdir` \
         (intent-services) so the dir is swept on drop and honors INTENTD_TEST_KEEP_TMP. \
         For pure path arithmetic that never touches the filesystem, append \
         `{ALLOW_MARKER} — <reason>` to the line.\n",
        offenders.len(),
        if offenders.len() == 1 { "" } else { "s" },
        offenders.join("\n  "),
    );
}
