//! Note index dependency fingerprint: release-only and unrelated changes must
//! keep the profile; output-relevant dependency changes must retire it.

#[path = "../../build/note_index_dependencies.rs"]
mod deps;

use sha2::{Digest, Sha256};
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

fn workspace_manifests() -> Vec<String> {
    deps::collect(Path::new(env!("CARGO_MANIFEST_DIR")))
        .expect("collect workspace inputs")
        .manifests
}

fn profile(lock: &str, manifests: &[String]) -> String {
    let manifests: Vec<&str> = manifests.iter().map(String::as_str).collect();
    deps::dependency_profile(lock, &manifests).expect("dependency profile")
}

#[test]
fn observed_v0_10_46_release_lock_change_preserves_dependency_profile() {
    let old = LOCK_V0_10_45;
    let new = apply_diff(old, LOCK_DIFF_V0_10_46);
    assert_eq!(sha256(old), LOCK_V0_10_45_SHA256);
    assert_eq!(sha256(&new), LOCK_V0_10_46_SHA256);
    assert_ne!(old, new);
    let manifests = workspace_manifests();
    assert_eq!(profile(old, &manifests), profile(&new, &manifests));
}

#[test]
fn build_script_profile_matches_tested_helper() {
    let inputs = deps::collect(Path::new(env!("CARGO_MANIFEST_DIR"))).expect("collect inputs");
    let manifests: Vec<&str> = inputs.manifests.iter().map(String::as_str).collect();
    let expected = deps::dependency_profile(&inputs.lock, &manifests).expect("profile");
    assert_eq!(
        include_str!(concat!(env!("OUT_DIR"), "/note-index-dependencies.txt")),
        expected
    );
}
