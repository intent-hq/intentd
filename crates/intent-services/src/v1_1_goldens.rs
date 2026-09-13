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
        "task-loop: 444d34b8eccae6a9d9c224fd2bd6b7d38f6fc3b2bfaa6a47653fbec849e2b33b".to_string(),
        "interactive: d4a08a7562e3907ed9537063dd47767b12c121c666f65b008cce91cb21652b42".to_string(),
        "workspace-agent: 31c7aa47b1bfbdbf5a32b02d807bab3ea2b4baed3483865f4994c5e3c0d49508"
            .to_string(),
        "task-breakdown: c5d0d4e95fcf40107e36642d2d8ae21382a63b721f4d5c28815691b28a6e6a7f"
            .to_string(),
        "common: 97ecb3a77913ecb08ee2fefe1d6dcd7fcbd2c47bf9b98e12889e5b4197ae7224".to_string(),
        "workspace: 5b656b7695f94db057eb3ab069727ca559bac4c4d8a0821c2f381a9824f3efc1".to_string(),
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

/// The v1.1 specialist bundle carries body-identical copies of the v1
/// prompts (the v1.1 doctrine diff is instruction-only); only the
/// picker-metadata frontmatter (`role`/`teamAgents`/`icon`, PROTOCOL §5.11)
/// diverges — every OTHER frontmatter key (`name`/`description`/`hidden`/
/// `agentType`/`roleReminder`/`modelOptions`/…) stays pinned to its v1
/// value. If a prompt-body (or non-metadata frontmatter) edit is ever
/// wanted, it needs a new harness version — not an in-place v1.1 edit.
#[test]
fn v1_1_specialist_bodies_are_identical_to_v1() {
    let v1 = crate::specialists::EMBEDDED_BUNDLED_V1;
    let v1_1 = crate::specialists::EMBEDDED_BUNDLED_V1_1;
    assert_eq!(v1.len(), v1_1.len());
    for ((id_a, content_a), (id_b, content_b)) in v1.iter().zip(v1_1.iter()) {
        assert_eq!(id_a, id_b);
        let (mut fm_a, body_a) = crate::specialists::parse_frontmatter(content_a);
        let (mut fm_b, body_b) = crate::specialists::parse_frontmatter(content_b);
        assert_eq!(body_a, body_b, "specialist {id_a} body diverged from v1");
        for key in crate::specialists::PICKER_METADATA_KEYS {
            fm_a.remove(*key);
            fm_b.remove(*key);
        }
        assert_eq!(
            fm_a, fm_b,
            "specialist {id_a} frontmatter diverged from v1 beyond the picker-metadata keys"
        );
    }
}

/// The v1.1 bundle's picker-metadata frontmatter (intent-hq/monorepo#3007):
/// spec-writer is the orchestrator with its advisory team roster and the
/// `coordinator` alias, implementor/verifier are internal, and every
/// specialist carries an icon.
#[test]
fn v1_1_specialists_carry_picker_metadata() {
    use serde_json::json;
    type Expectation<'a> = (
        &'a str,
        Option<&'a str>,
        Option<serde_json::Value>,
        &'a str,
        Option<serde_json::Value>,
    );
    let expectations: &[Expectation] = &[
        ("chief-of-staff", None, None, "chief-of-staff", None),
        ("developer", None, None, "verifier", None),
        ("implementor", Some("internal"), None, "implementor", None),
        ("pr-reviewer", None, None, "pr-reviewer", None),
        ("ralph", None, None, "ralph", None),
        (
            "spec-writer",
            Some("orchestrator"),
            Some(json!(r#"["implementor","verifier"]"#)),
            "coordinator",
            Some(json!(r#"["coordinator"]"#)),
        ),
        ("ui-designer", None, None, "ui-designer", None),
        ("verifier", Some("internal"), None, "verifier", None),
    ];
    let v1_1 = crate::specialists::EMBEDDED_BUNDLED_V1_1;
    assert_eq!(v1_1.len(), expectations.len());
    for ((id, content), (exp_id, role, team, icon, aliases)) in v1_1.iter().zip(expectations) {
        assert_eq!(id, exp_id);
        let (fm, _) = crate::specialists::parse_frontmatter(content);
        assert_eq!(
            fm.get("role").and_then(serde_json::Value::as_str),
            *role,
            "{id}: role"
        );
        assert_eq!(fm.get("teamAgents"), team.as_ref(), "{id}: teamAgents");
        assert_eq!(
            fm.get("icon").and_then(serde_json::Value::as_str),
            Some(*icon),
            "{id}: icon"
        );
        assert_eq!(fm.get("aliases"), aliases.as_ref(), "{id}: aliases");
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
