//! Harness 3.10, 3.12 and 3.13 doctrine goldens: the composed bundled rules are
//! pinned per agent type and feature/plain-folder variant, the cut sections
//! stay out, and every feature gate and plain-folder rewrite still bites.

use std::fmt::Write as _;

use intent_core::settings_file::AgentFeaturesSettings;
use sha2::{Digest, Sha256};

use crate::instructions::{get_instruction_with_common_in, InstructionSet};

const AGENT_TYPES: &[&str] = &[
    "interactive",
    "workspace",
    "workspace-agent",
    "task-breakdown",
    "chat",
    "code-review",
];

/// `(agent type, bytes, SHA-256)` of the four variants joined: defaults,
/// every gate off, plain folder, plain folder with every gate off.
const GOLDEN: &[(&str, usize, &str)] = &[
    (
        "interactive",
        37_463,
        "792ec424fbd5ae2b54a5c0a0d40d327f5ba600b30f1a355d618897c99a9fd5e7",
    ),
    (
        "workspace",
        37_463,
        "792ec424fbd5ae2b54a5c0a0d40d327f5ba600b30f1a355d618897c99a9fd5e7",
    ),
    (
        "workspace-agent",
        52_219,
        "c7b5304423795047c3ddc8da0b70bb14a323d511247ff7b51ce96dae8489568e",
    ),
    (
        "task-breakdown",
        71_586,
        "09ff31a155355b6d033c3da994e57f490240c5988bc26ee07864d4074159512a",
    ),
    (
        "chat",
        38_959,
        "77cf8da8259d6942950891f8a85f270713a14586249a50cabd1e760c4f8aeb20",
    ),
    (
        "code-review",
        14_393,
        "c530af1e49d099ffd8e067fb97edd8aa2e0f3b114bb68e063ec6a81ab709b45f",
    ),
];

/// 3.13's composed doctrine (3.12's with the explicit-rename Workspace Title
/// rule), in the same layout as [`GOLDEN`]. Change it only through a new
/// harness version once released.
const GOLDEN_V3_13: &[(&str, usize, &str)] = &[
    (
        "interactive",
        41_029,
        "1383d070df264ab3fef94de49e5fad74608f6eca66c4ac2947586e98e2f37eb6",
    ),
    (
        "workspace",
        41_029,
        "1383d070df264ab3fef94de49e5fad74608f6eca66c4ac2947586e98e2f37eb6",
    ),
    (
        "workspace-agent",
        55_785,
        "04e43b4529a80ce73b66fb4faf8095f770eae6b773904afead5490f857f06041",
    ),
    (
        "task-breakdown",
        75_152,
        "3dfff186ef838c76ef87eaf9f2066b8b644e3d8cbdfceed2d8bef4c4f3806041",
    ),
    (
        "chat",
        42_525,
        "7797f046d953815eb62b7c096dab895d0ab826278bb29a9923af154a5f3149f6",
    ),
    (
        "code-review",
        14_393,
        "c530af1e49d099ffd8e067fb97edd8aa2e0f3b114bb68e063ec6a81ab709b45f",
    ),
];

const CUT_HEADINGS: &[&str] = &[
    "## Note Editing",
    "## Waiting on External Conditions",
    "## Long-running Processes",
    "### Inline visuals",
];

/// 3.12's composed doctrine (3.10's with a shorter Follow-up Workspaces
/// section and 3.11's released visuals block), in the same layout as
/// [`GOLDEN`]. Released: change it only through a new harness version.
const GOLDEN_V3_12: &[(&str, usize, &str)] = &[
    (
        "interactive",
        40_825,
        "20309a977e493258a7458f74a0cdfcad7cb47edb872a12ccde0bfa14d2f5efb7",
    ),
    (
        "workspace",
        40_825,
        "20309a977e493258a7458f74a0cdfcad7cb47edb872a12ccde0bfa14d2f5efb7",
    ),
    (
        "workspace-agent",
        55_581,
        "4da9c7e761b91990f53bb3721f055b0cf80506c3596f442d607b0c7899a7ebf9",
    ),
    (
        "task-breakdown",
        74_948,
        "d53b714646cd26a303a4e3e60804de837b2b849ff2dda25d0719e6b3c4dbecae",
    ),
    (
        "chat",
        42_321,
        "b4584e6f896a3361cd2c0ee759cc680a095fde343e4075d21f389c6fc6005de4",
    ),
    (
        "code-review",
        14_393,
        "c530af1e49d099ffd8e067fb97edd8aa2e0f3b114bb68e063ec6a81ab709b45f",
    ),
];

fn set() -> &'static InstructionSet {
    crate::harness::resolve_entry("3.10").doctrine.instructions
}

fn gates_off() -> AgentFeaturesSettings {
    AgentFeaturesSettings {
        task_graph: false,
        attention_requests: false,
        background_hooks: false,
        host_exec: false,
        pr_monitor: false,
        scripts: false,
        rich_chat_blocks: false,
        terminal_access: false,
        ..AgentFeaturesSettings::default()
    }
}

fn composed(agent_type: &str) -> String {
    composed_in(set(), agent_type)
}

fn composed_in(set: &'static InstructionSet, agent_type: &str) -> String {
    let defaults = AgentFeaturesSettings::default();
    let off = gates_off();
    [
        (&defaults, false),
        (&off, false),
        (&defaults, true),
        (&off, true),
    ]
    .iter()
    .map(|(features, plain)| get_instruction_with_common_in(set, agent_type, features, *plain))
    .collect::<Vec<_>>()
    .join("\n\u{0}\n")
}

fn sha256_hex(text: &str) -> String {
    Sha256::digest(text.as_bytes())
        .iter()
        .fold(String::new(), |mut hex, byte| {
            let _ = write!(hex, "{byte:02x}");
            hex
        })
}

#[test]
fn golden_v3_10_composed_doctrine() {
    let actual: Vec<(&str, usize, String)> = AGENT_TYPES
        .iter()
        .map(|agent_type| {
            let text = composed(agent_type);
            (*agent_type, text.len(), sha256_hex(&text))
        })
        .collect();
    let matches = actual.len() == GOLDEN.len()
        && actual
            .iter()
            .zip(GOLDEN)
            .all(|(a, g)| a.0 == g.0 && a.1 == g.1 && a.2 == g.2);
    assert!(
        matches,
        "harness 3.10 doctrine changed. 3.10 never shipped on its own (3.12 releases \
         its surfaces), but eval sessions carry it: gate a change on a new harness \
         version, or for a deliberate fix paste the rows below into GOLDEN (rerun \
         `make test-select E='test(golden_v3_10_composed)'`):\n{}",
        actual
            .iter()
            .map(|(t, len, sha)| format!("    (\"{t}\", {len}, \"{sha}\"),"))
            .collect::<Vec<_>>()
            .join("\n")
    );
}

#[test]
fn v3_10_cuts_sections_and_keeps_the_rest_of_v3_8() {
    let current = set();
    let previous = crate::harness::resolve_entry("3.8").doctrine.instructions;
    for heading in CUT_HEADINGS {
        assert!(previous.common.contains(heading), "{heading} was in 3.8");
        assert!(!current.common.contains(heading), "{heading}");
    }
    for text in [current.common, current.workspace] {
        assert!(!text.contains("ws-block:html"));
        assert!(!text.contains("intentVisual"));
    }
    for retired in [
        "get_note",
        "add_to_note",
        "create_agent",
        "set_workspace_title",
    ] {
        assert!(!current.workspace.contains(retired), "{retired}");
    }
    assert!(current.common.len() * 2 < previous.common.len());
    assert!(current.workspace.len() < previous.workspace.len());
    assert_eq!(current.workspace_agent, previous.workspace_agent);
    assert_eq!(current.task_breakdown, previous.task_breakdown);
    assert_eq!(current.chat, previous.chat);
    assert_eq!(current.assistant, previous.assistant);
    assert!(std::ptr::eq(
        current.plain_folder.expect("plain-folder doctrine"),
        previous.plain_folder.expect("plain-folder doctrine")
    ));
}

/// Nothing in the doctrine varies between spawns (M7): no dates or times.
#[test]
fn v3_10_doctrine_has_no_time_varying_text() {
    for agent_type in AGENT_TYPES {
        let text = composed(agent_type);
        let digits: Vec<&str> = text
            .split(|c: char| !c.is_ascii_digit() && c != '-' && c != ':')
            .filter(|token| {
                let bytes = token.as_bytes();
                bytes.len() >= 8
                    && bytes.iter().filter(|b| b.is_ascii_digit()).count() >= 6
                    && bytes.iter().any(|b| *b == b'-' || *b == b':')
            })
            .collect();
        assert!(digits.is_empty(), "{agent_type}: {digits:?}");
    }
}

#[test]
fn v3_10_feature_gates_still_remove_their_sections() {
    let defaults = AgentFeaturesSettings::default();
    let on = get_instruction_with_common_in(set(), "common", &defaults, false);
    let off = get_instruction_with_common_in(set(), "common", &gates_off(), false);
    for section in [
        "### Task relations during delegation",
        "## Raising Attention",
        "## Rich Chat Rendering",
    ] {
        assert!(on.contains(section), "{section}");
        assert!(!off.contains(section), "{section}");
    }
    assert!(off.contains("## Delegating Tasks"));
    assert!(off.contains("Keep delegated tasks visible in the note"));
    assert!(off.contains("## Response Organization"));
}

#[test]
fn v3_10_plain_folder_drops_forge_sections_and_rewrites_examples() {
    let defaults = AgentFeaturesSettings::default();
    let plain_folder = set().plain_folder.expect("plain-folder doctrine");
    let git = get_instruction_with_common_in(set(), "interactive", &defaults, false);
    let plain = get_instruction_with_common_in(set(), "interactive", &defaults, true);
    for heading in [
        "## Follow-up Workspaces",
        "## GitHub & Git Operations",
        "## Link other workspaces",
    ] {
        assert!(git.contains(heading), "{heading}");
        assert!(!plain.contains(heading), "{heading}");
    }
    assert!(plain.contains(plain_folder.note));
    let breakdown = get_instruction_with_common_in(set(), "task-breakdown", &defaults, false);
    for (from, to) in plain_folder.rewrites {
        assert!(
            git.contains(from) || breakdown.contains(from),
            "rewrite source missing: {from}"
        );
        assert!(!plain.contains(from), "{from}");
        if git.contains(from) {
            assert!(plain.contains(to), "{to}");
        }
    }
}

#[test]
fn golden_v3_12_composed_doctrine() {
    let set = &crate::instructions::V3_12;
    let actual: Vec<(&str, usize, String)> = AGENT_TYPES
        .iter()
        .map(|agent_type| {
            let text = composed_in(set, agent_type);
            (*agent_type, text.len(), sha256_hex(&text))
        })
        .collect();
    let matches = actual.len() == GOLDEN_V3_12.len()
        && actual
            .iter()
            .zip(GOLDEN_V3_12)
            .all(|(a, g)| a.0 == g.0 && a.1 == g.1 && a.2 == g.2);
    assert!(
        matches,
        "harness 3.12 doctrine changed. 3.12 is released, so live sessions carry it: \
         gate the change on a new harness version, or for a deliberate all-session fix \
         paste the rows below into GOLDEN_V3_12:\n{}",
        actual
            .iter()
            .map(|(t, len, sha)| format!("    (\"{t}\", {len}, \"{sha}\"),"))
            .collect::<Vec<_>>()
            .join("\n")
    );
}

#[test]
fn golden_v3_13_composed_doctrine() {
    let set = &crate::instructions::V3_13;
    let actual: Vec<(&str, usize, String)> = AGENT_TYPES
        .iter()
        .map(|agent_type| {
            let text = composed_in(set, agent_type);
            (*agent_type, text.len(), sha256_hex(&text))
        })
        .collect();
    let matches = actual.len() == GOLDEN_V3_13.len()
        && actual
            .iter()
            .zip(GOLDEN_V3_13)
            .all(|(a, g)| a.0 == g.0 && a.1 == g.1 && a.2 == g.2);
    assert!(
        matches,
        "harness 3.13 doctrine changed. Gate the change on a new harness version, or \
         for a deliberate all-session fix paste the rows below into GOLDEN_V3_13:\n{}",
        actual
            .iter()
            .map(|(t, len, sha)| format!("    (\"{t}\", {len}, \"{sha}\"),"))
            .collect::<Vec<_>>()
            .join("\n")
    );
}
