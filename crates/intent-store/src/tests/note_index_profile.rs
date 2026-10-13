//! Note index dependency fingerprint: release-only and unrelated changes must
//! keep the profile; output-relevant dependency changes must retire it.

#[path = "../../build/note_index_dependencies.rs"]
mod deps;

use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::path::Path;

/// Cargo.lock at v0.10.45 (6c7a679a9) and the exact Cargo.lock diff shipped by
/// v0.10.46 (b6fc9f881). That release invalidated every note index on upgrade.
const LOCK_V0_10_45: &str = include_str!("fixtures/note_index_profile/cargo-lock-v0.10.45.toml");
const LOCK_DIFF_V0_10_46: &str =
    include_str!("fixtures/note_index_profile/cargo-lock-v0.10.45-to-v0.10.46.diff");
const LOCK_V0_10_45_SHA256: &str =
    "d7874dcfb46c5511bc3308b31deb2522c2feb8509b302a72908cc09e82364403";
const LOCK_V0_10_46_SHA256: &str =
    "9a51d437c916b9a8fb8fdebdfb5c10e04f4ece32791aba67961d60661c9d4733";

/// Crates referenced by the index sources that cannot change indexed output.
const NOT_OUTPUT: [(&str, &str); 5] = [
    (
        "intent_core",
        "workspace types; index sources are hashed separately",
    ),
    ("regex", "test-only oracle in the index sources"),
    ("sha2", "hashes the profile revision itself"),
    ("sqlx", "stores rows; does not shape entries"),
    ("uuid", "opaque content generation identifiers"),
];

const REGISTRY: &str = "registry+https://github.com/rust-lang/crates.io-index";

fn sha256(text: &str) -> String {
    format!("{:x}", Sha256::digest(text.as_bytes()))
}

/// Apply a unified diff produced by `git diff`, checking every context line.
fn apply_diff(original: &str, diff: &str) -> String {
    let source: Vec<&str> = original.split_inclusive('\n').collect();
    let mut out = String::new();
    let mut cursor = 0;
    let mut lines = diff.split_inclusive('\n').peekable();
    while let Some(line) = lines.next() {
        let Some(header) = line.strip_prefix("@@ -") else {
            continue;
        };
        let start: usize = header
            .split([',', ' '])
            .next()
            .and_then(|value| value.parse().ok())
            .expect("hunk start");
        while cursor < start - 1 {
            out.push_str(source[cursor]);
            cursor += 1;
        }
        while let Some(body) = lines.next_if(|next| !next.starts_with("@@")) {
            match body.split_at(1) {
                (" ", text) => {
                    assert_eq!(source[cursor], text, "context mismatch");
                    out.push_str(text);
                    cursor += 1;
                }
                ("-", text) => {
                    assert_eq!(source[cursor], text, "removed line mismatch");
                    cursor += 1;
                }
                ("+", text) => out.push_str(text),
                _ => panic!("unexpected diff line {body:?}"),
            }
        }
    }
    for line in &source[cursor..] {
        out.push_str(line);
    }
    out
}

fn real_inputs() -> deps::Inputs {
    deps::collect(Path::new(env!("CARGO_MANIFEST_DIR"))).expect("collect workspace inputs")
}

fn try_profile(lock: &str, manifests: &[String]) -> Result<String, String> {
    let manifests: Vec<&str> = manifests.iter().map(String::as_str).collect();
    deps::dependency_profile(lock, &manifests)
}

fn profile(lock: &str, manifests: &[String]) -> String {
    try_profile(lock, manifests).expect("dependency profile")
}

fn profile_error(lock: &str, manifests: &[&str]) -> String {
    deps::dependency_profile(lock, manifests).expect_err("profile must be rejected")
}

/// Split a lockfile into its header and `[[package]]` blocks.
fn blocks(lock: &str) -> (String, Vec<String>) {
    let mut parts = lock.split("\n[[package]]\n");
    let header = parts.next().unwrap_or_default().to_owned();
    (
        header,
        parts
            .map(|block| format!("[[package]]\n{}", block.trim_end()))
            .collect(),
    )
}

fn join(header: &str, blocks: &[String]) -> String {
    let mut out = header.trim_end().to_owned();
    for block in blocks {
        out.push_str("\n\n");
        out.push_str(block);
    }
    out.push('\n');
    out
}

/// Rewrite the package blocks matching `name` (and `version` when given).
fn edit_package(
    lock: &str,
    name: &str,
    version: Option<&str>,
    edit: impl Fn(&str) -> String,
) -> String {
    let (header, mut blocks) = blocks(lock);
    let mut edited = 0;
    for block in &mut blocks {
        let name_matches = block.contains(&format!("\nname = \"{name}\"\n"));
        let version_matches =
            version.is_none_or(|version| block.contains(&format!("\nversion = \"{version}\"\n")));
        if name_matches && version_matches {
            *block = edit(block);
            edited += 1;
        }
    }
    assert!(edited > 0, "no lock package {name} {version:?}");
    join(&header, &blocks)
}

fn replace_field(block: &str, field: &str, value: &str) -> String {
    block
        .lines()
        .map(|line| {
            if line.starts_with(&format!("{field} = ")) {
                format!("{field} = \"{value}\"")
            } else {
                line.to_owned()
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// `(name, version)` of each package line in a profile.
fn profiled_packages(profile: &str) -> Vec<(String, String)> {
    profile
        .lines()
        .filter_map(|line| line.strip_prefix("package "))
        .map(|line| {
            let mut parts = line.split(' ');
            (
                parts.next().unwrap().to_owned(),
                parts.next().unwrap().to_owned(),
            )
        })
        .collect()
}

fn own_version(lock: &str) -> String {
    let (_, blocks) = blocks(lock);
    let block = blocks
        .iter()
        .find(|block| {
            block.contains("\nname = \"intent-store\"\n") && !block.contains("\nsource = ")
        })
        .expect("intent-store lock entry");
    block
        .lines()
        .find_map(|line| line.strip_prefix("version = \""))
        .and_then(|version| version.strip_suffix('"'))
        .expect("intent-store version")
        .to_owned()
}

#[test]
fn observed_v0_10_46_release_lock_change_preserves_dependency_profile() {
    let old = LOCK_V0_10_45;
    let new = apply_diff(old, LOCK_DIFF_V0_10_46);
    assert_eq!(sha256(old), LOCK_V0_10_45_SHA256);
    assert_eq!(sha256(&new), LOCK_V0_10_46_SHA256);
    assert_ne!(old, new);
    let manifests = real_inputs().manifests;
    assert_eq!(profile(old, &manifests), profile(&new, &manifests));
}

#[test]
fn build_script_profile_matches_tested_helper() {
    let inputs = real_inputs();
    assert_eq!(
        include_str!(concat!(env!("OUT_DIR"), "/note-index-dependencies.txt")),
        profile(&inputs.lock, &inputs.manifests)
    );
}

#[test]
fn real_profile_covers_parser_configuration_and_excludes_workspace_packages() {
    let inputs = real_inputs();
    let text = profile(&inputs.lock, &inputs.manifests);
    for root in deps::ROOTS {
        assert!(
            text.contains(&format!("\nroot {root} -> {root} ")),
            "{root}"
        );
    }
    assert!(text.contains("\ndeclare intent-store dependencies pulldown-cmark -> pulldown-cmark default=false optional=absent workspace=absent features=[html]\n"));
    assert!(text.contains("\ndeclare workspace workspace.dependencies serde_json -> serde_json default=absent optional=absent workspace=absent features=[preserve_order]\n"));
    assert!(
        text.contains("\ndeclare intent-store dependencies html5gum -> html5gum default=false ")
    );
    assert!(
        profiled_packages(&text)
            .iter()
            .all(|(name, _)| !name.starts_with("intent-")),
        "workspace release identities must not participate"
    );
    assert!(
        text.contains("\npackage tokio "),
        "reverse dependents can unify shared parser/build features"
    );
    assert!(
        profiled_packages(&text).len() > deps::ROOTS.len(),
        "transitive dependencies must participate"
    );
}

#[test]
fn index_sources_reference_only_audited_crates() {
    let inputs = real_inputs();
    let dependencies: BTreeSet<String> = blocks(&inputs.lock)
        .1
        .iter()
        .find(|block| {
            block.contains("\nname = \"intent-store\"\n") && !block.contains("\nsource = ")
        })
        .expect("intent-store lock entry")
        .lines()
        .filter_map(|line| line.trim().strip_prefix('"'))
        .map(|entry| entry.split([' ', '"']).next().unwrap().replace('-', "_"))
        .collect();
    let roots: BTreeSet<String> = deps::ROOTS
        .iter()
        .map(|root| root.replace('-', "_"))
        .collect();
    let audited: BTreeSet<&str> = NOT_OUTPUT.iter().map(|(name, _)| *name).collect();
    let mut referenced = BTreeSet::new();
    for source in crate::note_page_index::INDEX_SOURCES {
        for (index, _) in source.match_indices("::") {
            let start = source[..index]
                .rfind(|ch: char| !(ch.is_ascii_alphanumeric() || ch == '_'))
                .map_or(0, |position| position + 1);
            let name = &source[start..index];
            if dependencies.contains(name) {
                referenced.insert(name.to_owned());
            }
        }
    }
    for name in &referenced {
        assert!(
            roots.contains(name) || audited.contains(name.as_str()),
            "index sources use `{name}`: add it to ROOTS in build/note_index_dependencies.rs or justify it in NOT_OUTPUT"
        );
    }
    for root in &roots {
        assert!(referenced.contains(root), "stale dependency root `{root}`");
    }
}

#[test]
fn release_only_workspace_version_bumps_preserve_profile() {
    let inputs = real_inputs();
    let version = own_version(&inputs.lock);
    let (header, mut lock_blocks) = blocks(&inputs.lock);
    for block in &mut lock_blocks {
        if !block.contains("\nsource = ") {
            *block = replace_field(block, "version", "99.0.0");
        }
    }
    let bumped_lock = join(&header, &lock_blocks);
    let quoted = format!("\"{version}\"");
    let bumped_manifests: Vec<String> = inputs
        .manifests
        .iter()
        .map(|manifest| manifest.replace(&quoted, "\"99.0.0\""))
        .collect();
    assert_ne!(bumped_lock, inputs.lock);
    assert_ne!(bumped_manifests, inputs.manifests);
    assert_eq!(
        profile(&inputs.lock, &inputs.manifests),
        profile(&bumped_lock, &bumped_manifests)
    );
}

#[test]
fn unrelated_dependency_changes_preserve_profile() {
    let inputs = real_inputs();
    let base = profile(&inputs.lock, &inputs.manifests);
    let added = edit_package(&inputs.lock, "intent-store", None, |block| {
        block.replace(
            "dependencies = [\n",
            "dependencies = [\n \"unrelated-crate\",\n",
        )
    });
    let added = format!(
        "{added}\n[[package]]\nname = \"unrelated-crate\"\nversion = \"1.0.0\"\nsource = \"{REGISTRY}\"\nchecksum = \"{}\"\n",
        "1".repeat(64)
    );
    assert_eq!(base, profile(&added, &inputs.manifests));
    let bumped = edit_package(&added, "unrelated-crate", None, |block| {
        replace_field(
            &replace_field(block, "version", "9.9.9"),
            "checksum",
            &"0".repeat(64),
        )
    });
    assert_eq!(base, profile(&bumped, &inputs.manifests));
    let mut manifests = inputs.manifests.clone();
    manifests[0].push_str("\n[workspace.metadata.unrelated]\nnote = \"ignored\"\n");
    assert_eq!(base, profile(&inputs.lock, &manifests));
}

#[test]
fn lockfile_order_and_formatting_preserve_profile() {
    let inputs = real_inputs();
    let base = profile(&inputs.lock, &inputs.manifests);
    let (header, mut lock_blocks) = blocks(&inputs.lock);
    lock_blocks.reverse();
    let reordered = join(&format!("# reformatted\n{header}\n\n"), &lock_blocks);
    assert_eq!(base, profile(&reordered, &inputs.manifests));
    let table: toml::Table = inputs.lock.parse().unwrap();
    let reserialized = toml::to_string_pretty(&table).unwrap();
    assert_ne!(reserialized, inputs.lock);
    assert_eq!(base, profile(&reserialized, &inputs.manifests));
}

#[test]
fn direct_and_transitive_dependency_changes_retire_profile() {
    let inputs = real_inputs();
    let base = profile(&inputs.lock, &inputs.manifests);
    let packages = profiled_packages(&base);
    for (name, version) in &packages {
        let changed = edit_package(&inputs.lock, name, Some(version), |block| {
            if let Some(source) = block
                .lines()
                .find_map(|line| line.strip_prefix("source = \"git+"))
            {
                replace_field(
                    block,
                    "source",
                    &format!(
                        "git+{}#{}",
                        source.split('#').next().unwrap(),
                        "f".repeat(40)
                    ),
                )
            } else {
                replace_field(block, "checksum", &"f".repeat(64))
            }
        });
        assert_ne!(
            sha256(&base),
            sha256(&profile(&changed, &inputs.manifests)),
            "{name} {version}"
        );
    }
    let bumped = edit_package(&inputs.lock, "pulldown-cmark", None, |block| {
        replace_field(block, "version", "0.13.999")
    });
    assert_ne!(base, profile(&bumped, &inputs.manifests));
}

#[test]
fn dependency_source_changes_retire_profile() {
    let inputs = real_inputs();
    let base = profile(&inputs.lock, &inputs.manifests);
    let sparse = edit_package(&inputs.lock, "pulldown-cmark", None, |block| {
        replace_field(block, "source", "sparse+https://index.crates.io/")
    });
    assert_ne!(base, profile(&sparse, &inputs.manifests));
    let git = edit_package(&inputs.lock, "pulldown-cmark", None, |block| {
        replace_field(
            block,
            "source",
            "git+https://example.invalid/pulldown-cmark?branch=x#0123abcd",
        )
        .lines()
        .filter(|line| !line.starts_with("checksum = "))
        .collect::<Vec<_>>()
        .join("\n")
    });
    let git_profile = profile(&git, &inputs.manifests);
    assert_ne!(base, git_profile);
    let other_commit = git.replace("#0123abcd", "#4567ef01");
    assert_ne!(git_profile, profile(&other_commit, &inputs.manifests));
}

/// A small synthetic workspace with duplicate versions.
fn synthetic_lock(store_base64: &str) -> String {
    let package = |name: &str, version: &str, checksum: char, dependencies: &[&str]| {
        let mut block = format!(
            "[[package]]\nname = \"{name}\"\nversion = \"{version}\"\nsource = \"{REGISTRY}\"\nchecksum = \"{}\"\n",
            checksum.to_string().repeat(64)
        );
        if !dependencies.is_empty() {
            block.push_str("dependencies = [\n");
            for dependency in dependencies {
                block.push_str(" \"");
                block.push_str(dependency);
                block.push_str("\",\n");
            }
            block.push_str("]\n");
        }
        block
    };
    [
        "version = 4\n".to_owned(),
        format!(
            "[[package]]\nname = \"intent-store\"\nversion = \"0.10.46\"\ndependencies = [\n \"{store_base64}\",\n \"html5ever\",\n \"html5gum\",\n \"other\",\n \"pulldown-cmark\",\n \"serde_json\",\n]\n"
        ),
        package("base64", "0.22.1", 'a', &[]),
        package("base64", "0.23.1", 'b', &[]),
        package("html5ever", "0.29.1", 'c', &["markup5ever"]),
        package("html5gum", "0.8.4", 'd', &[]),
        package("markup5ever", "0.14.1", 'e', &["memchr 2.7.4"]),
        package("memchr", "1.0.0", '1', &[]),
        package("memchr", "2.7.4", '2', &[]),
        package("other", "1.0.0", '3', &["base64 0.22.1", "memchr 1.0.0"]),
        package("pulldown-cmark", "0.13.4", '4', &["memchr 2.7.4"]),
        package("serde_json", "1.0.140", '5', &["memchr 2.7.4"]),
    ]
    .join("\n")
}

#[test]
fn duplicate_versions_fingerprint_only_the_resolved_package() {
    let lock = synthetic_lock("base64 0.23.1");
    let text = profile(&lock, &[]);
    assert!(text.contains("\nroot base64 -> base64 0.23.1 "));
    assert!(text.contains("\npackage memchr 2.7.4 "));
    assert!(!text.contains("base64 0.22.1"));
    assert!(!text.contains("memchr 1.0.0"));
    let unused = edit_package(&lock, "base64", Some("0.22.1"), |block| {
        replace_field(block, "checksum", &"9".repeat(64))
    });
    assert_eq!(text, profile(&unused, &[]));
    let used = edit_package(&lock, "memchr", Some("2.7.4"), |block| {
        replace_field(block, "checksum", &"9".repeat(64))
    });
    assert_ne!(text, profile(&used, &[]));
    let error = profile_error(&synthetic_lock("base64"), &[]);
    assert!(error.contains("`base64` matches 2 packages"), "{error}");
}

const WORKSPACE: &str = r#"
[workspace]
members = ["crates/*"]
[workspace.dependencies]
serde_json = { version = "1", features = ["preserve_order"] }
tokio = { version = "1", features = ["rt"] }
"#;

const MEMBER: &str = r#"
[package]
name = "intent-store"
version = "0.10.46"
[dependencies]
intent-core = { path = "../intent-core", version = "0.10.46" }
pulldown-cmark = { version = "0.13", default-features = false, features = ["html"] }
serde_json = { workspace = true }
memchr = "2"
tokio = { workspace = true }
"#;

#[test]
fn feature_configuration_changes_retire_profile() {
    let lock = synthetic_lock("base64 0.23.1");
    let manifests = |workspace: &str, member: &str| vec![workspace.to_owned(), member.to_owned()];
    let base = profile(&lock, &manifests(WORKSPACE, MEMBER));
    let retired = [
        manifests(
            &WORKSPACE.replace("[\"preserve_order\"]", "[\"preserve_order\", \"arbitrary_precision\"]"),
            MEMBER,
        ),
        manifests(WORKSPACE, &MEMBER.replace("default-features = false, ", "")),
        manifests(WORKSPACE, &MEMBER.replace("[\"html\"]", "[\"html\", \"simd\"]")),
        manifests(WORKSPACE, &MEMBER.replace("memchr = \"2\"", "memchr = { version = \"2\", default-features = false }")),
        manifests(WORKSPACE, &format!("{MEMBER}[features]\nexact = [\"serde_json/float_roundtrip\"]\n")),
        manifests(WORKSPACE, &format!("{MEMBER}[target.'cfg(unix)'.dependencies]\nhtml5gum = {{ version = \"0.8\", features = [\"tracing\"] }}\n")),
        manifests(WORKSPACE, &format!("{MEMBER}[build-dependencies]\nmarkup5ever = {{ version = \"0.14\", features = [\"x\"] }}\n")),
        vec![
            WORKSPACE.to_owned(),
            MEMBER.to_owned(),
            "[package]\nname = \"other-member\"\n[dependencies]\njson = { package = \"serde_json\", version = \"1\", features = [\"raw_value\"] }\n".to_owned(),
        ],
    ];
    for (position, changed) in retired.iter().enumerate() {
        assert_ne!(
            base,
            profile(&lock, changed),
            "configuration change #{position}"
        );
    }
    let preserved = [
        manifests(
            &WORKSPACE.replace("[\"rt\"]", "[\"rt\", \"macros\"]"),
            MEMBER,
        ),
        manifests(WORKSPACE, &MEMBER.replace("0.10.46", "0.10.47")),
        manifests(
            WORKSPACE,
            &MEMBER.replace("version = \"0.13\"", "version = \"0.13.4\""),
        ),
        manifests(
            WORKSPACE,
            &MEMBER.replace("[\"html\"]", "[\"html\", \"html\"]"),
        ),
        manifests(
            WORKSPACE,
            &format!("{MEMBER}[features]\nunrelated = [\"tokio/macros\"]\n"),
        ),
    ];
    for (position, changed) in preserved.iter().enumerate() {
        assert_eq!(
            base,
            profile(&lock, changed),
            "unrelated change #{position}"
        );
    }
}

#[test]
fn malformed_or_incomplete_inputs_are_build_errors() {
    let lock = synthetic_lock("base64 0.23.1");
    let cases: Vec<(String, Vec<&str>, &str)> = vec![
        ("not toml [".to_owned(), vec![], "cannot parse Cargo.lock"),
        (lock.replace("version = 4\n", ""), vec![], "unsupported Cargo.lock format version None"),
        (lock.replace("version = 4\n", "version = 1\n"), vec![], "unsupported Cargo.lock format version Some(1)"),
        (lock.replace("name = \"intent-store\"", "name = \"renamed\""), vec![], "exactly one local `intent-store`"),
        (lock.replace(" \"html5gum\",\n \"other\"", " \"other\""), vec![], "exactly one `html5gum`"),
        (
            edit_package(&lock, "markup5ever", None, |block| {
                block.lines().filter(|line| !line.starts_with("checksum")).collect::<Vec<_>>().join("\n")
            }),
            vec![],
            "`markup5ever 0.14.1 (registry+https://github.com/rust-lang/crates.io-index)` has no checksum",
        ),
        (
            edit_package(&lock, "markup5ever", None, |block| {
                block.lines().filter(|line| !line.starts_with("checksum") && !line.starts_with("source")).collect::<Vec<_>>().join("\n")
            }),
            vec![],
            "path or workspace package `markup5ever 0.14.1 (path)`",
        ),
        (
            edit_package(&lock, "markup5ever", None, |block| replace_field(block, "source", "git+https://example.invalid/m")),
            vec![],
            "has no pinned commit",
        ),
        (
            edit_package(&lock, "markup5ever", None, |block| replace_field(block, "source", "directory+/vendor")),
            vec![],
            "unsupported source",
        ),
        (lock.replace("\"memchr 2.7.4\"", "\"memchr 3.0.0\""), vec![], "`memchr 3.0.0` matches 0 packages"),
        (lock.replace("\"memchr 2.7.4\"", "\"memchr 2.7.4 registry\""), vec![], "malformed Cargo.lock dependency"),
        (lock.clone(), vec!["[dependencies"], "cannot parse workspace manifest #0"),
        (lock.clone(), vec!["[dependencies]\nserde_json = { features = \"x\" }\n"], "features must be an array"),
        (lock.clone(), vec!["[dependencies]\nserde_json = { optional = \"yes\" }\n"], "`optional` must be a single boolean"),
        (lock.clone(), vec!["[dependencies]\nserde_json = 1\n"], "dependency must be a string or table"),
        (lock.clone(), vec!["[features]\nx = \"serde_json/y\"\n"], "features.x must be an array"),
    ];
    for (lock, manifests, expected) in cases {
        let error = profile_error(&lock, &manifests);
        assert!(
            error.contains(expected),
            "expected {expected:?} in {error:?}"
        );
    }
}

fn write(path: &Path, text: &str) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, text).unwrap();
}

#[test]
fn collect_reads_workspace_members_and_packaged_crates_without_cargo() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    write(&root.join("Cargo.lock"), "version = 4\n");
    write(
        &root.join("Cargo.toml"),
        "[workspace]\nmembers = [\"crates/*\", \"tools/extra\"]\nexclude = [\"crates/skipped\"]\n",
    );
    write(
        &root.join("crates/intent-store/Cargo.toml"),
        "[package]\nname = \"intent-store\"\n",
    );
    write(
        &root.join("crates/other/Cargo.toml"),
        "[package]\nname = \"other\"\n",
    );
    write(
        &root.join("crates/skipped/Cargo.toml"),
        "[package]\nname = \"skipped\"\n",
    );
    write(&root.join("crates/no-manifest/README"), "");
    write(
        &root.join("tools/extra/Cargo.toml"),
        "[package]\nname = \"extra\"\n",
    );
    let store = root.join("crates/intent-store");
    let inputs = deps::collect(&store).unwrap();
    assert_eq!(inputs.lock, "version = 4\n");
    let names: Vec<&str> = inputs
        .manifests
        .iter()
        .map(|manifest| {
            manifest
                .lines()
                .find(|line| line.starts_with("name") || line.starts_with("members"))
                .unwrap()
        })
        .collect();
    assert_eq!(
        names,
        [
            "members = [\"crates/*\", \"tools/extra\"]",
            "name = \"intent-store\"",
            "name = \"other\"",
            "name = \"extra\"",
        ]
    );
    assert_eq!(inputs.watched.len(), 5);
    assert_eq!(inputs.watched[0], root.join("Cargo.lock"));

    write(
        &root.join("Cargo.toml"),
        "[workspace]\nmembers = [\"crates/intent-*\"]\n",
    );
    let error = deps::collect(&store).err().unwrap();
    assert!(
        error.contains("unsupported workspace member pattern"),
        "{error}"
    );
    write(
        &root.join("Cargo.toml"),
        "[workspace]\nmembers = [\"crates/other\"]\n",
    );
    let error = deps::collect(&store).err().unwrap();
    assert!(
        error.contains("is not a member of the workspace"),
        "{error}"
    );
    write(
        &root.join("Cargo.toml"),
        "[package]\nname = \"elsewhere\"\n",
    );
    let error = deps::collect(&store).err().unwrap();
    assert!(error.contains("has no [workspace]"), "{error}");

    // `cargo package` verification: lock and normalized manifest side by side.
    write(&store.join("Cargo.lock"), "version = 4\n# packaged\n");
    let packaged = deps::collect(&store).unwrap();
    assert_eq!(packaged.lock, "version = 4\n# packaged\n");
    assert_eq!(packaged.manifests, ["[package]\nname = \"intent-store\"\n"]);

    let empty = tempfile::tempdir().unwrap();
    write(
        &empty.path().join("Cargo.toml"),
        "[package]\nname = \"intent-store\"\n",
    );
    // Only fails when no ancestor of the temporary directory has a lockfile.
    if !empty
        .path()
        .ancestors()
        .skip(1)
        .any(|path| path.join("Cargo.lock").is_file())
    {
        let error = deps::collect(empty.path()).err().unwrap();
        assert!(error.contains("Cargo.lock is required"), "{error}");
    }
}

#[test]
fn feature_alias_activation_retires_profile() {
    let lock = synthetic_lock("base64 0.23.1");
    let manifest = format!("{MEMBER}[features]\nexact = [\"serde_json/arbitrary_precision\"]\nlayer = [\"exact\"]\ndefault = []\n");
    let base = profile(&lock, &[WORKSPACE.to_owned(), manifest.clone()]);
    for feature in ["exact", "layer"] {
        let changed = manifest.replace("default = []", &format!("default = [\"{feature}\"]"));
        assert_ne!(
            base,
            profile(&lock, &[WORKSPACE.to_owned(), changed]),
            "activation via {feature}"
        );
    }
    let renamed = manifest.replace("exact", "precise");
    assert_ne!(
        base,
        profile(&lock, &[WORKSPACE.to_owned(), renamed]),
        "feature owner must participate"
    );
}

#[test]
fn outside_closure_feature_unification_retires_profile() {
    let lock = edit_package(&synthetic_lock("base64 0.23.1"), "other", None, |block| {
        block.replace("dependencies = [", "dependencies = [\n \"serde_json\",")
    });
    let manifest = format!("{MEMBER}other = {{ version = \"1\", features = [] }}\n");
    let base = profile(&lock, &[WORKSPACE.to_owned(), manifest.clone()]);
    let changed = manifest.replace("features = []", "features = [\"exact\"]");
    assert_ne!(
        base,
        profile(&lock, &[WORKSPACE.to_owned(), changed]),
        "outside package enables serde_json/arbitrary_precision"
    );
    let changed = edit_package(&lock, "other", None, |block| {
        replace_field(block, "checksum", &"9".repeat(64))
    });
    assert_ne!(
        base,
        profile(&changed, &[WORKSPACE.to_owned(), manifest]),
        "external manifest feature forwarding can change"
    );
}

#[test]
fn collect_follows_implicit_path_dependency_manifests() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    write(&root.join("Cargo.lock"), "version = 4\n");
    write(
        &root.join("Cargo.toml"),
        "[workspace]\nmembers = [\"crates/intent-store\"]\n",
    );
    write(&root.join("crates/intent-store/Cargo.toml"), "[package]\nname = \"intent-store\"\n[dependencies]\nimplicit = { path = \"../implicit\" }\n");
    write(&root.join("crates/implicit/Cargo.toml"), "[package]\nname = \"implicit\"\n[dependencies]\nserde_json = { version = \"1\", features = [\"arbitrary_precision\"] }\n");
    let inputs = deps::collect(&root.join("crates/intent-store")).unwrap();
    assert_eq!(
        inputs.manifests.len(),
        3,
        "Cargo auto-includes the path dependency"
    );
    assert!(inputs
        .watched
        .iter()
        .any(|path| path.canonicalize().unwrap()
            == root
                .join("crates/implicit/Cargo.toml")
                .canonicalize()
                .unwrap()));
}

#[test]
fn renamed_optional_and_cyclic_feature_aliases_are_tracked() {
    let lock = synthetic_lock("base64 0.23.1");
    let workspace =
        "[workspace.dependencies]\njson = { package = \"serde_json\", version = \"1\" }\n";
    let member = "[package]\nname = \"consumer\"\n[dependencies]\njson = { workspace = true, optional = true }\n[features]\ndefault = []\nexact = [\"json?/arbitrary_precision\", \"cycle\"]\ncycle = [\"exact\"]\nactivate = [\"dep:json\"]\n";
    let base = profile(&lock, &[workspace.to_owned(), member.to_owned()]);
    for feature in ["exact", "cycle", "activate"] {
        let changed = member.replace("default = []", &format!("default = [\"{feature}\"]"));
        assert_ne!(
            base,
            profile(&lock, &[workspace.to_owned(), changed]),
            "{feature}"
        );
    }
    let changed = member.replace("optional = true", "optional = false");
    assert_ne!(base, profile(&lock, &[workspace.to_owned(), changed]));
    let changed = member.replace("name = \"consumer\"", "name = \"another\"");
    assert_ne!(
        base,
        profile(&lock, &[workspace.to_owned(), changed]),
        "declaration owner"
    );
    let changed = member.replace(
        "[\"json?/arbitrary_precision\", \"cycle\"]",
        "[\"cycle\", \"json?/arbitrary_precision\", \"cycle\"]",
    );
    assert_eq!(
        base,
        profile(&lock, &[workspace.to_owned(), changed]),
        "feature ordering and duplicates"
    );
    assert_eq!(
        base,
        profile(&lock, &[member.to_owned(), workspace.to_owned()]),
        "manifest ordering"
    );
}

#[test]
fn local_feature_contributors_require_manifests_and_ignore_release_versions() {
    let lock = edit_package(&synthetic_lock("base64 0.23.1"), "other", None, |block| {
        block
            .lines()
            .filter(|line| !line.starts_with("source") && !line.starts_with("checksum"))
            .collect::<Vec<_>>()
            .join("\n")
            .replace("dependencies = [", "dependencies = [\n \"serde_json\",")
    });
    let error = profile_error(&lock, &[]);
    assert!(
        error.contains("local feature contributor `other` has no collected manifest"),
        "{error}"
    );
    let manifest = "[package]\nname = \"other\"\nversion = \"1.0.0\"\n[dependencies]\nserde_json = \"1\"\n[features]\nexact = [\"serde_json/arbitrary_precision\"]\ndefault = []\n";
    let base = profile(&lock, &[manifest.to_owned()]);
    assert_ne!(
        base,
        profile(
            &lock,
            &[manifest.replace("default = []", "default = [\"exact\"]")]
        )
    );
    let bumped = edit_package(&lock, "other", None, |block| {
        replace_field(block, "version", "2.0.0")
    });
    assert_eq!(
        base,
        profile(&bumped, &[manifest.replace("1.0.0", "2.0.0")])
    );
}

#[test]
fn collect_follows_inherited_target_build_and_patch_paths_recursively() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("workspace");
    write(&root.join("Cargo.lock"), "version = 4\n");
    write(&root.join("Cargo.toml"), "[workspace]\nmembers = [\"store\"]\nexclude = [\"vendor\"]\n[workspace.dependencies]\nbridge = { path = \"../bridge\" }\n[patch.crates-io]\npatched = { path = \"vendor\" }\n");
    write(&root.join("store/Cargo.toml"), "[package]\nname = \"intent-store\"\n[target.'cfg(unix)'.build-dependencies]\nbridge = { workspace = true }\n");
    write(
        &dir.path().join("bridge/Cargo.toml"),
        "[package]\nname = \"bridge\"\n[dev-dependencies]\nnested = { path = \"nested\" }\n",
    );
    write(
        &dir.path().join("bridge/nested/Cargo.toml"),
        "[package]\nname = \"nested\"\n[dependencies]\nbridge = { path = \"..\" }\n",
    );
    write(
        &root.join("vendor/Cargo.toml"),
        "[package]\nname = \"patched\"\n",
    );
    let inputs = deps::collect(&root.join("store")).unwrap();
    assert_eq!(
        inputs.manifests.len(),
        5,
        "local manifests outside members, excluded patches and cycles"
    );
    assert_eq!(inputs.watched.len(), 6);
    assert!(inputs
        .manifests
        .iter()
        .any(|manifest| manifest.contains("name = \"nested\"")));
    write(
        &dir.path().join("bridge/nested/Cargo.toml"),
        "[dependencies]\nmissing = { path = \"missing\" }\n",
    );
    let error = deps::collect(&root.join("store")).err().unwrap();
    assert!(
        error.contains("cannot resolve") && error.contains("missing/Cargo.toml"),
        "{error}"
    );
}

#[test]
fn workspace_resolution_settings_retire_profile() {
    let lock = synthetic_lock("base64 0.23.1");
    let workspace = format!("{WORKSPACE}\n").replace(
        "members = [\"crates/*\"]",
        "members = [\"crates/*\"]\nresolver = \"2\"\ndefault-members = [\"crates/intent-store\"]",
    );
    let base = profile(&lock, &[workspace.clone(), MEMBER.to_owned()]);
    for changed in [
        workspace.replace("resolver = \"2\"", "resolver = \"1\""),
        workspace.replace(
            "default-members = [\"crates/intent-store\"]",
            "default-members = [\"crates/intent-store\", \"crates/other\"]",
        ),
    ] {
        assert_ne!(base, profile(&lock, &[changed, MEMBER.to_owned()]));
    }
}

#[test]
fn duplicate_version_alias_selection_retires_profile() {
    let lock = synthetic_lock("base64 0.23.1");
    // Both versions remain locked and referenced; only the alias receiving
    // feature flags changes. Lock edges alone cannot distinguish this swap.
    let manifest = "[package]\nname = \"consumer\"\n[dependencies]\na = { package = \"base64\", version = \"0.22.1\", default-features = false }\nb = { package = \"base64\", version = \"0.23.1\", default-features = true }\n";
    let changed = manifest
        .replace("0.22.1", "TEMP")
        .replace("0.23.1", "0.22.1")
        .replace("TEMP", "0.23.1");
    assert_ne!(
        profile(&lock, &[manifest.to_owned()]),
        profile(&lock, &[changed])
    );
}
