//! Source lint: the transfer manifest's creating-daemon version has one source.
//!
//! `workspace.import.begin` rejects an archive unless its
//! `creatingIntentdVersion` equals the importing daemon's version exactly. Both
//! sides must therefore read the same value:
//! `intent_core::transfer::CREATING_INTENTD_VERSION`. Spelling
//! `env!("CARGO_PKG_VERSION")` instead silently picks up whichever crate the
//! code lives in. That only works while every crate shares one version, and
//! release tooling can let them drift: release #2464 left the intentd binary
//! crate at 0.11.6 and the libraries at 0.11.5, and every merge-queue run then
//! failed `deleted_workspace_metadata_can_be_reimported_over_wss`. Its manifest
//! used the intentd crate's version but the importer checked intent-services'.
//!
//! This lint fails naming `file:line` for every `CARGO_PKG_VERSION` string
//! literal within [`WINDOW`] lines of a mention of the manifest version field
//! (the `creating_intentd_version` identifier or a string literal containing
//! `creatingIntentdVersion`) in any Rust file under `crates/`. It reads the
//! shared lexer's output, so comments neither count as mentions nor offend.
//! The proximity rule covers manifest builders, the import check and
//! assertions on its error message, while leaving unrelated uses alone (e.g.
//! `client.hello` reporting the binary's own version). Limit: a package version
//! routed through an alias or helper defined further away is not seen. The only
//! exemption is the defining module.

use std::fs;

use intentd_test_support::source_lint::{lex, rust_files, workspace_root};

const DEFINING_FILE: &str = "crates/intent-core/src/transfer.rs";
const THIS_FILE: &str = "crates/intent-core/tests/transfer_version_source_lint.rs";
/// Lines either side of a field mention that count as transfer-version code.
const WINDOW: usize = 20;

/// Built at runtime so this file never contains the tokens it searches for.
fn snake_field() -> String {
    ["creating", "_intentd", "_version"].concat()
}

fn camel_field() -> String {
    ["creating", "Intentd", "Version"].concat()
}

fn pkg_version() -> String {
    ["CARGO", "_PKG", "_VERSION"].concat()
}

/// Every package-version literal within [`WINDOW`] lines of a field mention,
/// as `file:line`.
fn offenders_in(rel: &str, src: &str) -> Vec<String> {
    let lexed = lex(src);
    let (snake, camel, pkg) = (snake_field(), camel_field(), pkg_version());
    let mut mentions: Vec<usize> = lexed
        .blanked
        .lines()
        .enumerate()
        .filter(|(_, line)| line.contains(snake.as_str()))
        .map(|(i, _)| i + 1)
        .collect();
    mentions.extend(
        lexed
            .literals
            .iter()
            .filter(|l| l.cooked().contains(camel.as_str()))
            .map(|l| l.line),
    );
    let mut lines: Vec<usize> = lexed
        .literals
        .iter()
        .filter(|l| l.cooked() == pkg && mentions.iter().any(|m| m.abs_diff(l.line) <= WINDOW))
        .map(|l| l.line)
        .collect();
    lines.dedup();
    lines
        .into_iter()
        .map(|line| format!("{rel}:{line}"))
        .collect()
}

#[test]
fn transfer_manifest_version_uses_the_shared_constant() {
    let root = workspace_root();
    let files = rust_files(&root.join("crates"));
    assert!(!files.is_empty(), "no Rust files under {}", root.display());
    let mut offenders = Vec::new();
    for path in files {
        let rel = path
            .strip_prefix(&root)
            .unwrap_or(&path)
            .to_string_lossy()
            .replace('\\', "/");
        if rel == DEFINING_FILE || rel == THIS_FILE {
            continue;
        }
        let text =
            fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
        offenders.extend(offenders_in(&rel, &text));
    }
    assert!(
        offenders.is_empty(),
        "use intent_core::transfer::CREATING_INTENTD_VERSION for the transfer manifest \
         version instead of the expanding crate's own package version:\n{}",
        offenders.join("\n")
    );
}

#[test]
fn detector_flags_package_version_beside_the_field_only() {
    let (snake, camel, pkg) = (snake_field(), camel_field(), pkg_version());
    let flagged = format!("let m = M {{ {snake}: env!(\"{pkg}\").into() }};\n");
    assert_eq!(offenders_in("a.rs", &flagged), vec!["a.rs:1"]);
    let json = format!("json!({{\"{camel}\":\n    env!(\"{pkg}\")}})\n");
    assert_eq!(offenders_in("b.rs", &json), vec!["b.rs:2"]);
    let unrelated = format!("const V: &str = env!(\"{pkg}\");\n");
    assert!(offenders_in("c.rs", &unrelated).is_empty());
    let shared = format!("{snake}: CREATING_INTENTD_VERSION.into()\n");
    assert!(offenders_in("d.rs", &shared).is_empty());
    let far = format!("{snake}: x,\n{}env!(\"{pkg}\")\n", "\n".repeat(WINDOW));
    assert!(offenders_in("e.rs", &far).is_empty());
    let near = format!("{snake}: x,\n{}env!(\"{pkg}\")\n", "\n".repeat(WINDOW - 1));
    assert_eq!(offenders_in("f.rs", &near).len(), 1);
    let comment_mention = format!("/// documents {camel}\nlet v = env!(\"{pkg}\");\n");
    assert!(offenders_in("g.rs", &comment_mention).is_empty());
    let comment_offender = format!("{snake}: SHARED, // not env!(\"{pkg}\")\n");
    assert!(offenders_in("h.rs", &comment_offender).is_empty());
    let raw_json = format!("let s = r#\"{{\"{camel}\":1}}\"#;\nlet v = env!(\"{pkg}\");\n");
    assert_eq!(offenders_in("i.rs", &raw_json), vec!["i.rs:2"]);
}
