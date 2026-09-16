//! Harness v2.6 adds plain-language guidance for questions and attention requests.

use super::{Doctrine, HarnessEntry};

static DOCTRINE: Doctrine = Doctrine {
    instructions: &crate::instructions::V2_6,
    specialists: crate::specialists::EMBEDDED_BUNDLED_V2_5,
};

pub(crate) static ENTRY: HarnessEntry = HarnessEntry {
    version: "2.6",
    harness: &super::v2_4::V2_4,
    doctrine: &DOCTRINE,
    default_features: intent_core::settings_file::AgentFeaturesSettings::default,
    feature_labels: super::v1::FEATURE_LABELS,
};

#[cfg(test)]
mod tests {
    use crate::instructions::get_instruction_with_common_for;
    use intent_core::settings_file::AgentFeaturesSettings;

    #[test]
    fn plain_language_reaches_interactive_roles_even_without_attention_tools() {
        let previous = super::super::resolve_entry("2.5");
        let current = super::super::latest_entry();
        assert_eq!(current.version, "2.6");
        for auto_commit in [true, false] {
            assert_eq!(
                current.harness.suggested_next_steps_block(auto_commit),
                previous.harness.suggested_next_steps_block(auto_commit)
            );
        }
        assert_eq!(current.doctrine.specialists, previous.doctrine.specialists);
        let common = current.doctrine.instructions.common;
        let guidance = common
            .strip_suffix(previous.doctrine.instructions.common)
            .unwrap();
        assert!(guidance.starts_with("## Plain language for the user"));
        for attention_requests in [true, false] {
            let features = AgentFeaturesSettings {
                attention_requests,
                ..AgentFeaturesSettings::default()
            };
            for role in [
                "interactive",
                "workspace-agent",
                "task-focused",
                "task-loop",
                "chat",
            ] {
                let composed =
                    get_instruction_with_common_for(current.doctrine.instructions, role, &features);
                let old = get_instruction_with_common_for(
                    previous.doctrine.instructions,
                    role,
                    &features,
                );
                assert_eq!(composed, format!("{guidance}{old}"), "role: {role}");
            }
        }
    }
}
