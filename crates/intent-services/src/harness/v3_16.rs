//! Harness 3.16: 3.15 with load-aware remote placement text. The prompt's
//! machine list and `ws.help("agent")` tell agents to read each machine's
//! `load` from `ws.agent.machineModels` and spread parallel sub-agents onto
//! the least-loaded machines; the lookup's line documents `load` and
//! `local`. Every other surface stays 3.15's.

use super::{ContextSurfaces, HarnessEntry};

static CONTEXT: ContextSurfaces = ContextSurfaces {
    workspace_api: super::v3_15::ENTRY.context.workspace_api,
    repo_rules: super::v3_15::ENTRY.context.repo_rules,
    repo_rules_reads_executor: super::v3_15::ENTRY.context.repo_rules_reads_executor,
    skills_block: super::v3_15::ENTRY.context.skills_block,
    hidden_skills: super::v3_15::ENTRY.context.hidden_skills,
    placement_help: intent_core::machine_selection::load_aware_machines_help,
    placement_reference: intent_core::machine_selection::load_aware_machines_reference,
};

pub(crate) static ENTRY: HarnessEntry = HarnessEntry {
    version: "3.16",
    harness: super::v3_15::ENTRY.harness,
    doctrine: super::v3_15::ENTRY.doctrine,
    context: &CONTEXT,
    default_features: intent_core::settings_file::AgentFeaturesSettings::default,
    feature_labels: super::v1::FEATURE_LABELS,
};

#[cfg(test)]
mod tests {
    use intent_core::machine_selection::PlacementHelpRenderer;

    /// Pin what 3.16 keeps from 3.15: everything but the placement text.
    #[test]
    fn v3_16_is_3_15_with_load_aware_placement_text() {
        let current = super::super::resolve_entry("3.16");
        let prior = super::super::resolve_entry("3.15");
        assert_eq!(current.version, intent_core::CURRENT_HARNESS_VERSION);
        assert_eq!(intent_core::STAGED_HARNESS_VERSION, None);
        assert!(std::ptr::addr_eq(current.harness, prior.harness));
        assert!(std::ptr::eq(current.doctrine, prior.doctrine));
        assert_eq!((current.default_features)(), (prior.default_features)());
        assert_eq!(current.feature_labels, prior.feature_labels);
        assert!(intent_core::prompt_breakdown::harness_records_prompt_breakdown("3.16"));

        let (now, before) = (current.context, prior.context);
        assert!(std::ptr::addr_eq(now.workspace_api, before.workspace_api));
        assert!(std::ptr::fn_addr_eq(now.repo_rules, before.repo_rules));
        assert_eq!(
            now.repo_rules_reads_executor,
            before.repo_rules_reads_executor
        );
        assert!(std::ptr::fn_addr_eq(now.skills_block, before.skills_block));
        assert!(std::ptr::fn_addr_eq(
            now.hidden_skills,
            before.hidden_skills
        ));
        assert!(std::ptr::fn_addr_eq(
            now.placement_help,
            intent_core::machine_selection::load_aware_machines_help as PlacementHelpRenderer
        ));
        assert!(std::ptr::fn_addr_eq(
            now.placement_reference,
            intent_core::machine_selection::load_aware_machines_reference as PlacementHelpRenderer
        ));
        assert!(std::ptr::fn_addr_eq(
            before.placement_reference,
            intent_core::machine_selection::stable_machines_reference as PlacementHelpRenderer
        ));
    }
}
