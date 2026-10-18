//! Source lint: the transfer manifest's creating-daemon version has one source.
//!
//! `workspace.import.begin` rejects an archive unless its
//! `creatingIntentdVersion` equals the importing daemon's version exactly. Both
//! sides must therefore read the same value:
//! `intent_core::transfer::CREATING_INTENTD_VERSION`. Spelling
//! `env!("CARGO_PKG_VERSION")` instead silently picks up whichever crate the
//! code lives in. That only works while every crate shares one version, and the
//! release tooling once let them drift: the intentd binary crate reached
//! 0.11.6 while the libraries stayed on 0.11.5, and every merge-queue run failed
//! `deleted_workspace_metadata_can_be_reimported_over_wss`. Its manifest used
//! the intentd crate's version but the importer checked intent-services'.
//!
//! This lint fails naming `file:line` for every `CARGO_PKG_VERSION` line within
//! [`WINDOW`] lines of a mention of the manifest version field
//! (`creating_intentd_version` / `creatingIntentdVersion`) in any Rust file
//! under `crates/`. That proximity covers manifest builders, the import check
//! and assertions on its error message, while leaving unrelated uses alone
//! (e.g. `client.hello` reporting the binary's own version). The only
//! exemption is the defining module.

use std::fs;

use intentd_test_support::source_lint::{rust_files, workspace_root};

const DEFINING_FILE: &str = "crates/intent-core/src/transfer.rs";
const THIS_FILE: &str = "crates/intent-core/tests/transfer_version_source_lint.rs";
/// Lines either side of a field mention that count as transfer-version code.
const WINDOW: usize = 20;

/// Built at runtime so this file never contains the tokens it searches for.
fn field_names() -> [String; 2] {
    [
        ["creating", "_intentd", "_version"].concat(),
        ["creating", "Intentd", "Version"].concat(),
    ]
}

fn pkg_version() -> String {
    ["CARGO", "_PKG", "_VERSION"].concat()
}

/// Every line expanding the package version within [`WINDOW`] lines of a
/// mention of the manifest version field.
fn offenders_in(rel: &str, text: &str) -> Vec<String> {
    let fields = field_names();
    let needle = pkg_version();
    let lines: Vec<&str> = text.lines().collect();
    let mentions: Vec<usize> = lines
        .iter()
        .enumerate()
        .filter(|(_, line)| fields.iter().any(|f| line.contains(f.as_str())))
        .map(|(i, _)| i)
        .collect();
    lines
        .iter()
        .enumerate()
        .filter(|(i, line)| {
            line.contains(needle.as_str()) && mentions.iter().any(|m| m.abs_diff(*i) <= WINDOW)
        })
        .map(|(i, line)| format!("{rel}:{}: {}", i + 1, line.trim()))
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
    let [snake, camel] = field_names();
    let pkg = pkg_version();
    let flagged = format!("let m = M {{ {snake}: env!(\"{pkg}\").into() }};\n");
    assert_eq!(offenders_in("a.rs", &flagged).len(), 1);
    let json = format!("json!({{\"{camel}\":\n    env!(\"{pkg}\")}})\n");
    assert_eq!(
        offenders_in("b.rs", &json),
        vec![format!("b.rs:2: env!(\"{pkg}\")}})")]
    );
    let unrelated = format!("const V: &str = env!(\"{pkg}\");\n");
    assert!(offenders_in("c.rs", &unrelated).is_empty());
    let far = format!("{snake}: x,\n{}env!(\"{pkg}\")\n", "\n".repeat(WINDOW));
    assert!(offenders_in("e.rs", &far).is_empty());
    let near = format!("{snake}: x,\n{}env!(\"{pkg}\")\n", "\n".repeat(WINDOW - 1));
    assert_eq!(offenders_in("f.rs", &near).len(), 1);
    let shared = format!("{snake}: CREATING_INTENTD_VERSION.into()\n");
    assert!(offenders_in("d.rs", &shared).is_empty());
}
