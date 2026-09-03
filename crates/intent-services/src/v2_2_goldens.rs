//! Harness v2.2 golden fixtures. This version reuses v2.1 system-text bytes
//! and the v2.1 specialist bundle, and rewrites only the two workspace
//! instruction bodies so the workspace status message is one short plain
//! sentence instead of a dense 1–2 sentence summary.

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

fn status_section(workspace: &str) -> &str {
    let start = workspace
        .find("## Workspace Status Message")
        .expect("status-message section present");
    let rest = &workspace[start..];
    let end = rest[1..].find("\n## ").map_or(rest.len(), |i| i + 1);
    &rest[..end]
}

fn example_lines(section: &str) -> Vec<&str> {
    section
        .lines()
        .filter_map(|l| l.strip_prefix("- "))
        .filter(|l| !l.starts_with('❌') && !l.starts_with('✅'))
        .collect()
}

#[test]
fn v2_2_registry_selects_rewritten_workspace_bodies() {
    let v2_1 = crate::harness::resolve_entry("2.1");
    let v2_2 = crate::harness::resolve_entry("2.2");
    assert_eq!(
        v2_2.doctrine.instructions.workspace,
        crate::instructions::V2_2.workspace
    );
    assert_eq!(
        v2_2.doctrine.instructions.workspace_agent,
        crate::instructions::V2_2.workspace_agent
    );
    assert_eq!(v2_1.doctrine.specialists, v2_2.doctrine.specialists);
    assert_eq!(
        v2_1.doctrine.instructions.common,
        v2_2.doctrine.instructions.common
    );
    assert_ne!(
        v2_1.doctrine.instructions.workspace,
        v2_2.doctrine.instructions.workspace
    );
    assert_ne!(
        v2_1.doctrine.instructions.workspace_agent,
        v2_2.doctrine.instructions.workspace_agent
    );
}

#[test]
fn current_harness_version_is_v2_2() {
    assert_eq!(intent_core::model::CURRENT_HARNESS_VERSION, "2.2");
    assert_eq!(
        crate::harness::resolve_entry(intent_core::model::CURRENT_HARNESS_VERSION).version,
        "2.2"
    );
}

#[test]
fn golden_v2_2_workspace_body_hashes() {
    assert_eq!(
        sha256_hex(crate::instructions::V2_2.workspace),
        "552716d4b53c9a11e6de4b3f24bf9aee652d185187a465f00c7c6d02e07a97c1"
    );
    assert_eq!(
        sha256_hex(crate::instructions::V2_2.workspace_agent),
        "72d41b4bd71060e62d7dad0410c1a12a6fb2341c9e5ba00852721be749325981"
    );
}

/// The v2.2 status-message guidance demands a single short sentence and
/// drops the old "1–2 sentence" wording everywhere it appeared.
#[test]
fn v2_2_status_guidance_demands_one_short_sentence() {
    let ws = crate::instructions::V2_2.workspace;
    let wa = crate::instructions::V2_2.workspace_agent;
    let section = status_section(ws);

    assert!(section.contains("Write exactly one plain sentence"));
    assert!(section.contains("ideally under 15 words"));
    assert!(section.contains("Leave out counts"));
    assert!(section.contains("Too dense"));

    for body in [ws, wa] {
        assert!(!body.contains("1–2 sentence"), "old wording survives");
        assert!(!body.contains("1-2 sentence"), "old wording survives");
        assert!(
            !body.contains("8 more tasks to go"),
            "old dense example survives"
        );
    }
    assert!(wa.contains("one plain sentence, ideally under 15 words"));
    assert!(wa.contains("keep it to one short plain sentence"));
}

/// Every positive example is itself a single short sentence within the
/// budget the guidance asks for; the negative example is the dense status
/// the rewrite exists to rule out.
#[test]
fn v2_2_status_examples_stay_within_budget() {
    let section = status_section(crate::instructions::V2_2.workspace);
    let good = example_lines(section);
    assert!(good.len() >= 5, "expected several positive examples");
    for line in &good {
        let words = line.split_whitespace().count();
        assert!(
            (3..15).contains(&words),
            "example outside 3–14 word budget ({words} words): {line}"
        );
        let sentence_ends = line.matches(". ").count();
        assert_eq!(
            sentence_ends, 0,
            "example is more than one sentence: {line}"
        );
        assert!(!line.contains(';'), "example uses a semicolon: {line}");
    }
    assert!(good
        .iter()
        .any(|l| l.starts_with("The sidebar PR dropdown is in a PR")));

    let bad: Vec<&str> = section
        .lines()
        .filter_map(|l| l.strip_prefix("- ❌ "))
        .collect();
    assert_eq!(bad.len(), 1);
    assert!(bad[0].split_whitespace().count() > 30);
    assert!(bad[0].contains("Tests, svelte-check, lint, i18n checks pass."));

    let fixed: Vec<&str> = section
        .lines()
        .filter_map(|l| l.strip_prefix("- ✅ "))
        .collect();
    assert_eq!(fixed.len(), 1);
    assert!(fixed[0].split_whitespace().count() <= 15);
}

/// Composition for the default interactive agent type resolves the v2.2
/// workspace body (and therefore the succinct guidance) under the current
/// harness version, while a v2.1-pinned session still gets the old text.
#[test]
fn v2_2_composed_prompt_carries_succinct_guidance() {
    let current = crate::harness::resolve_entry("2.2");
    let composed = crate::instructions::get_instruction_with_common_for(
        current.doctrine.instructions,
        "interactive",
        &(current.default_features)(),
    );
    assert!(composed.contains("Write exactly one plain sentence"));
    assert!(!composed.contains("1–2 concise sentences"));

    let prior = crate::harness::resolve_entry("2.1");
    let composed_prior = crate::instructions::get_instruction_with_common_for(
        prior.doctrine.instructions,
        "interactive",
        &(prior.default_features)(),
    );
    assert!(composed_prior.contains("1–2 concise sentences"));
    assert!(!composed_prior.contains("Write exactly one plain sentence"));
}
