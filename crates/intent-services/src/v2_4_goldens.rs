//! Harness v2.4 golden fixtures. This version keeps every v2.3 text surface
//! and v2.2 instruction byte while versioning the shared PR-context handoff
//! in three specialist definitions.

use std::fmt::Write as _;

use sha2::{Digest, Sha256};

fn sha256_hex(s: &str) -> String {
    let mut h = Sha256::new();
    h.update(s.as_bytes());
    h.finalize().iter().fold(String::new(), |mut s, b| {
        let _ = write!(s, "{b:02x}");
        s
    })
}

#[test]
fn v2_4_registry_selects_the_pr_context_bundle() {
    let v2_3 = crate::harness::resolve_entry("2.3");
    let v2_4 = crate::harness::resolve_entry("2.4");
    assert_eq!(
        v2_3.doctrine.specialists,
        crate::specialists::EMBEDDED_BUNDLED_V2_1
    );
    assert_eq!(
        v2_4.doctrine.specialists,
        crate::specialists::EMBEDDED_BUNDLED_V2_4
    );
    assert!(std::ptr::eq(
        v2_3.doctrine.instructions,
        v2_4.doctrine.instructions
    ));
    assert_eq!(
        v2_3.harness.suggested_next_steps_block(false),
        v2_4.harness.suggested_next_steps_block(false)
    );
}

#[test]
fn v2_4_bundle_changes_only_pr_context_collaborators() {
    let old = crate::harness::resolve_entry("2.3").doctrine.specialists;
    let new = crate::specialists::EMBEDDED_BUNDLED_V2_4;
    assert_eq!(old.len(), new.len());
    for ((old_id, old_content), (new_id, new_content)) in old.iter().zip(new.iter()) {
        assert_eq!(old_id, new_id);
        if matches!(*old_id, "implementor" | "spec-writer" | "verifier") {
            assert_ne!(old_content, new_content, "{old_id} must be versioned");
        } else {
            assert_eq!(
                old_content, new_content,
                "{old_id} must remain byte-identical"
            );
        }
    }
}

#[test]
fn current_harness_version_is_v2_4() {
    assert_eq!(intent_core::model::CURRENT_HARNESS_VERSION, "2.4");
    assert_eq!(
        crate::harness::resolve_entry(intent_core::model::CURRENT_HARNESS_VERSION).version,
        "2.4"
    );
}

#[test]
fn golden_pr_context_specialist_hashes() {
    let expected = [
        (
            "implementor",
            "f7b9cdf79ceb56e06652a083567c33c37785fc7a3ac25f61edcb06351b5e6c15",
        ),
        (
            "spec-writer",
            "dd7e05c7083c38ef4ba77d726507f5627d5e54bc985b77b1b265389c7641374a",
        ),
        (
            "verifier",
            "ea316bfa27c8c66d533c8fc24d22e59921b0f53fd62c057c1ea2a2facc2698c7",
        ),
    ];
    for (id, hash) in expected {
        let content = crate::specialists::EMBEDDED_BUNDLED_V2_4
            .iter()
            .find_map(|(candidate, content)| (*candidate == id).then_some(*content))
            .expect("versioned specialist is bundled");
        assert_eq!(sha256_hex(content), hash, "{id}");
    }
}
