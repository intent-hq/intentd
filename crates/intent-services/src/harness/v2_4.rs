//! Harness v2.4 adds plain-language guidance for questions and attention requests.

use super::{Doctrine, HarnessEntry};

static DOCTRINE: Doctrine = Doctrine {
    instructions: &crate::instructions::V2_4,
    specialists: crate::specialists::EMBEDDED_BUNDLED_V2_1,
};

pub(crate) static ENTRY: HarnessEntry = HarnessEntry {
    version: "2.4",
    harness: &super::v2_3::V2_3,
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
        let previous = super::super::resolve_entry("2.3");
        let current = super::super::latest_entry();
        assert_eq!(current.version, "2.4");
        assert!(std::ptr::eq(current.harness, previous.harness));
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
