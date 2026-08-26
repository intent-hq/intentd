//! v1.1 golden fixtures (harness versioning, docs/HARNESS.md procedure).
//!
//! The v1→v1.1 doctrine diff is exactly the feature-section rewrites in
//! `common.md` ("Task relations during delegation", "Waiting on External
//! Conditions", "Rich Chat Rendering" — compressed to doctrine-only text);
//! every other instruction body and the whole specialist bundle are
//! byte-identical copies of v1. These pins freeze the v1.1 bytes the same
//! way `v1_goldens` freezes v1: any change to the shipped v1.1 markdown (or
//! the gating composition over it) fails here and forces a harness-version
//! decision. The system-string goldens (wake messages, envelopes, static
//! prompt layers) live in `v1_goldens` — those surfaces are version-shared
//! and unchanged by v1.1.

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

/// SHA-256 pins of the v1.1-set composition with all-default agent features
/// (the exact bytes sessions stamped "1.1" receive). Counterpart of `v1_goldens`'
/// `golden_bundled_doctrine_hashes`, over `instructions::V1_1`.
#[test]
fn golden_bundled_doctrine_hashes_v1_1() {
    let features = intent_core::settings_file::AgentFeaturesSettings::default();
    let agent_types = [
        "task-loop",
        "interactive",
        "workspace-agent",
        "task-breakdown",
        "common",
        "workspace",
    ];
    let actual: Vec<String> = agent_types
        .iter()
        .map(|agent_type| {
            format!(
                "{}: {}",
                agent_type,
                sha256_hex(&crate::instructions::get_instruction_with_common_for(
                    &crate::instructions::V1_1,
                    agent_type,
                    &features
                ))
            )
        })
        .collect();
    let expected = vec![
        "task-loop: 13c99a477e31d6aa8503088187570281a798520c06ed47fed7305aa94426e6f5".to_string(),
        "interactive: be1f029e55d719bbe14da82162a52afdf205a77d3652f5638dc0eb176586153f".to_string(),
        "workspace-agent: 9732954a360134242a448cbc82e8c1047d98ede7246be8a5c03d9df73cb5b835"
            .to_string(),
        "task-breakdown: aff9eaf33ff512f00d41e4d3a9fd76d883f4db75956eecc80994f2b1ebdf1699"
            .to_string(),
        "common: a8b61b71fc496d0643c16839843be7960c79f9a2648343a17d15d51127f710b4".to_string(),
        "workspace: 0a97dee3a391aa541689d71274923978bcab2c1e3ac410a5221f842250ccf2b8".to_string(),
    ];
    assert_eq!(actual, expected);
}

/// The v1.1 registry row remains pinned after a later version becomes current.
#[test]
fn v1_1_registry_row_remains_pinned() {
    let entry = crate::harness::resolve_entry("1.1");
    assert_eq!(entry.version, "1.1");
    assert!(std::ptr::eq(
        entry.doctrine.instructions,
        std::ptr::addr_of!(crate::instructions::V1_1)
    ));
    assert_eq!(
        entry.doctrine.specialists,
        crate::specialists::EMBEDDED_BUNDLED_V1_1
    );
}

/// The v1.1 specialist bundle is a byte-identical copy of v1's (the v1.1
/// diff is instruction-only). If a specialist edit is ever wanted, it needs
/// a new harness version — not an in-place v1.1 edit.
#[test]
fn v1_1_specialists_are_byte_identical_to_v1() {
    let v1 = crate::specialists::EMBEDDED_BUNDLED_V1;
    let v1_1 = crate::specialists::EMBEDDED_BUNDLED_V1_1;
    assert_eq!(v1.len(), v1_1.len());
    for ((id_a, body_a), (id_b, body_b)) in v1.iter().zip(v1_1.iter()) {
        assert_eq!(id_a, id_b);
        assert_eq!(body_a, body_b, "specialist {id_a} diverged from v1");
    }
}

/// Non-common v1.1 instruction bodies are byte-identical v1 copies, and the
/// common.md rewrite carries the three approved feature-section rewrites
/// (spot-checked by distinctive phrases; the full bytes are pinned by hash
/// above).
#[test]
fn v1_1_common_carries_the_feature_section_rewrites() {
    let common = crate::instructions::V1_1.common;
    // Task relations: compressed to a single advisory paragraph.
    assert!(common.contains("### Task relations during delegation"));
    assert!(common.contains("Holds are advisory and never auto-start"));
    assert!(!common.contains("**Batch results report graph state.**"));
    // Waiting: mechanics deferred to the ws.hook.schedule docs; monitor
    // preferred; the cross-repo snapshot clause preserved.
    assert!(common.contains("Mechanics (validation run, `hookState`, `perpetual`, TTL) are in the `ws.hook.schedule` docs."));
    assert!(common.contains("`ws.pr.monitor` (PRs)"));
    assert!(common.contains("ws.pr.snapshot(prNumber, { repo: \"owner/name\" })"));
    assert!(!common.contains("ws.host.exec"));
    // Rich chat: table kept, example + image paragraph folded into one.
    assert!(common.contains("| Mermaid diagram | `mermaid` |"));
    assert!(!common.contains("```mermaid"));
    assert!(common.contains("png/jpg/gif/webp only"));
}
