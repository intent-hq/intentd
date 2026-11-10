//! Source lint: intent-services persists versioned note content only in
//! `note_write.rs`.
//!
//! `scripts/changed-tests.sh` routes a change to
//! `crates/intent-services/src/note_write.rs` to the note end-to-end suites
//! (`.config/e2e-test-map.txt`). `lib.rs` is deliberately left unmapped,
//! because most intent-services PRs edit it. So a note save written in
//! `lib.rs`, or in any other unmapped file, skips those suites before review,
//! and its broken e2e expectations show up for the first time in the merge
//! queue (intentd#3204: an hour per round trip). The map comment used to ask,
//! in prose, that note write paths stay in `note_write.rs`. This lint enforces
//! that.
//!
//! It fails naming `file:line` for every call to the store's versioned note
//! write methods ([`WATCHED_METHODS`]), whether written as a method call
//! (`.update_note_with_version(`) or as a path call
//! (`Store::update_note_with_version(`), in any non-test Rust file under
//! `crates/intent-services/src/` other than `note_write.rs`. Fix a failure by
//! calling, or adding, a `note_write.rs` helper. Test code is exempt: whole
//! test files and `#[cfg(test)]` items as `source_lint::test_code_files`
//! classifies them, plus the `test_support` module, which builds fixtures for
//! other crates' tests. Comments and string literals never count. There is no
//! opt-out marker. Limit: a call made through an alias or a function pointer
//! is not seen.

use std::fs;

use intentd_test_support::source_lint::{
    rust_files, test_code_files, tokenize, workspace_root, TestCodeText,
};

const SRC_PREFIX: &str = "crates/intent-services/src/";
const NOTE_WRITE_FILE: &str = "crates/intent-services/src/note_write.rs";
const STORE_SRC: &str = "crates/intent-store/src";

/// The `intent_store::Store` methods that write a note row together with its
/// version snapshot.
const WATCHED_METHODS: &[&str] = &[
    "insert_note_with_version",
    "update_note_with_version",
    "update_note_with_version_and_children",
];

/// Whether `rel` (workspace-relative) sits in the exempt `test_support` module.
fn is_test_support(rel: &str) -> bool {
    let Some(inner) = rel.strip_prefix(SRC_PREFIX) else {
        return false;
    };
    inner == "test_support.rs" || inner.starts_with("test_support/")
}

/// Every non-test call to a [`WATCHED_METHODS`] entry in `src`, as `rel:line`.
/// `whole` marks a file that is test code throughout.
fn offenders_in(rel: &str, src: &str, whole: bool) -> Vec<String> {
    if whole {
        return Vec::new();
    }
    let text = TestCodeText::new(src, false);
    let tokens = tokenize(&text.chars);
    let mut out = Vec::new();
    for window in tokens.windows(3) {
        let [sep, name, open] = window else {
            continue;
        };
        if (sep.is(".") || sep.is("::"))
            && name.is_ident()
            && WATCHED_METHODS.contains(&name.text.as_str())
            && open.is("(")
            && !text.in_test(name.start)
        {
            out.push(format!("{rel}:{}", name.line));
        }
    }
    out
}

#[test]
fn versioned_note_writes_go_through_note_write() {
    let root = workspace_root();
    let files: Vec<_> = test_code_files(&root)
        .into_iter()
        .filter(|f| f.rel.starts_with(SRC_PREFIX))
        .collect();
    assert!(
        files.iter().any(|f| f.rel == NOTE_WRITE_FILE),
        "{NOTE_WRITE_FILE} not found under {}",
        root.display()
    );
    let mut offenders = Vec::new();
    for file in &files {
        if file.rel == NOTE_WRITE_FILE || is_test_support(&file.rel) {
            continue;
        }
        offenders.extend(offenders_in(&file.rel, &file.src, file.whole));
    }
    assert!(
        offenders.is_empty(),
        "persist versioned note content through a helper in {NOTE_WRITE_FILE} \
         (persist_new_note / persist_note_content / persist_note_content_with_children / \
         persist_note_content_new_version, or add one there), so the e2e test map runs the \
         note end-to-end suites on the change (intentd#3204). Direct store calls:\n{}",
        offenders.join("\n")
    );
}

/// Guards against the lint silently passing after a rename: every watched
/// method must still be defined by the store and called from `note_write.rs`.
#[test]
fn watched_methods_exist_and_note_write_uses_them() {
    let root = workspace_root();
    let store_src: String = rust_files(&root.join(STORE_SRC))
        .iter()
        .map(|p| fs::read_to_string(p).unwrap_or_else(|e| panic!("read {}: {e}", p.display())))
        .collect();
    let note_write = fs::read_to_string(root.join(NOTE_WRITE_FILE))
        .unwrap_or_else(|e| panic!("read {NOTE_WRITE_FILE}: {e}"));
    for method in WATCHED_METHODS {
        assert!(
            store_src.contains(&format!("pub async fn {method}(")),
            "{STORE_SRC} no longer defines `{method}`; update WATCHED_METHODS"
        );
        assert!(
            note_write.contains(&format!(".{method}(")),
            "{NOTE_WRITE_FILE} no longer calls `{method}`; update WATCHED_METHODS"
        );
    }
    let found = offenders_in(NOTE_WRITE_FILE, &note_write, false).len();
    assert!(
        found >= WATCHED_METHODS.len(),
        "the detector finds {found} watched calls in {NOTE_WRITE_FILE}, \
         expected at least {}",
        WATCHED_METHODS.len()
    );
}

#[test]
fn detector_flags_direct_calls_outside_test_code_only() {
    let method = "store\n    .update_note_with_version(&lane, &note, None, &a, &d)\n    .await?;\n";
    assert_eq!(offenders_in("a.rs", method, false), vec!["a.rs:2"]);
    let path = "Store::insert_note_with_version(&store, &n, &a, &d).await?;\n";
    assert_eq!(offenders_in("b.rs", path, false), vec!["b.rs:1"]);
    let children = "s.update_note_with_version_and_children(l, n, None, &[], a, d)\n";
    assert_eq!(offenders_in("c.rs", children, false), vec!["c.rs:1"]);
    let whole = "s.update_note_with_version(l, n, None, a, d)\n";
    assert!(offenders_in("d.rs", whole, true).is_empty());
    let cfg_test = "#[cfg(test)]\nmod tests {\n    fn f() { s.update_note_with_version(l); }\n}\n";
    assert!(offenders_in("e.rs", cfg_test, false).is_empty());
    let after_cfg_test = format!("{cfg_test}fn g() {{ s.insert_note_with_version(n); }}\n");
    assert_eq!(offenders_in("f.rs", &after_cfg_test, false), vec!["f.rs:5"]);
    let comment =
        "/// calls [`Store::update_note_with_version`]\n// s.update_note_with_version(x)\n";
    assert!(offenders_in("g.rs", comment, false).is_empty());
    let literal = "let s = \"s.update_note_with_version(x)\";\n";
    assert!(offenders_in("h.rs", literal, false).is_empty());
    let other = "s.update_note_versioned(l, n)\ns.update_note_with_version_extra(x)\n";
    assert!(offenders_in("i.rs", other, false).is_empty());
    let helper = "persist_note_content(store, &lane, &note, None, &author).await?;\n";
    assert!(offenders_in("j.rs", helper, false).is_empty());
    assert!(is_test_support(
        "crates/intent-services/src/test_support/large_state.rs"
    ));
    assert!(is_test_support(
        "crates/intent-services/src/test_support.rs"
    ));
    assert!(!is_test_support("crates/intent-services/src/lib.rs"));
    assert!(!is_test_support(
        "crates/intent-services/src/test_supporting.rs"
    ));
}
