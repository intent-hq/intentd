//! Harness 3.13: 3.12 with one workspace-layer change. The Workspace Title
//! rule keeps first-turn naming from overwriting a title but lets an agent
//! rename a titled workspace when the user explicitly asks, through
//! `ws.workspace.setTitle(title, { userRequested: true })`. Sessions stamped
//! 3.12 and older keep the old "never rename" rule.

use super::{Doctrine, HarnessEntry};

static DOCTRINE: Doctrine = Doctrine {
    instructions: &crate::instructions::V3_13,
    specialists: super::v3_12::ENTRY.doctrine.specialists,
};

pub(crate) static ENTRY: HarnessEntry = HarnessEntry {
    version: "3.13",
    harness: super::v3_12::ENTRY.harness,
    doctrine: &DOCTRINE,
    context: super::v3_12::ENTRY.context,
    default_features: intent_core::settings_file::AgentFeaturesSettings::default,
    feature_labels: super::v1::FEATURE_LABELS,
};

#[cfg(test)]
mod tests {
    use crate::instructions::{V3_12, V3_13};

    const OLD_RULE: &str = "Never rename a workspace that already has a meaningful title.";
    const EXPLICIT_RENAME: &str = "`ws.workspace.setTitle(title, { userRequested: true })`";

    #[test]
    fn v3_13_is_3_12_with_the_explicit_rename_rule() {
        let current = super::super::resolve_entry("3.13");
        let prior = super::super::resolve_entry("3.12");
        assert_eq!(current.version, intent_core::CURRENT_HARNESS_VERSION);
        assert_eq!(intent_core::STAGED_HARNESS_VERSION, None);
        assert!(std::ptr::eq(current.context, prior.context));
        assert!(std::ptr::addr_eq(current.harness, prior.harness));
        assert_eq!(current.doctrine.specialists, prior.doctrine.specialists);
        assert_eq!((current.default_features)(), (prior.default_features)());
        assert_eq!(current.feature_labels, prior.feature_labels);
        assert!(intent_core::prompt_breakdown::harness_records_prompt_breakdown("3.13"));
        assert_eq!(V3_13.common, V3_12.common);
        assert_eq!(V3_13.chat, V3_12.chat);

        // Only the Workspace Title rule changes; 3.12 keeps its old wording.
        assert!(V3_12.workspace.contains(OLD_RULE));
        assert!(!V3_12.workspace.contains(EXPLICIT_RENAME));
        assert!(!V3_13.workspace.contains(OLD_RULE));
        assert!(V3_13.workspace.contains(EXPLICIT_RENAME));
        assert!(V3_13
            .workspace
            .contains("Rename a titled workspace only when the user asks"));
        let section = |body: &'static str| {
            let start = body.find("## Workspace Title\n").expect("section");
            let end = body.find("## Agent Collaboration\n").expect("next section");
            (&body[..start], &body[end..])
        };
        assert_eq!(section(V3_13.workspace), section(V3_12.workspace));
    }
}
