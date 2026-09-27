//! Harness v2.8 narrows discussion requests to assigned work that needs a
//! decision before it can continue; routine questions stay in conversation.

use super::{Doctrine, HarnessEntry};

static DOCTRINE: Doctrine = Doctrine {
    instructions: &crate::instructions::V2_8,
    specialists: crate::specialists::EMBEDDED_BUNDLED_V2_5,
};

pub(crate) static ENTRY: HarnessEntry = HarnessEntry {
    version: "2.8",
    harness: &super::v2_4::V2_4,
    doctrine: &DOCTRINE,
    default_features: intent_core::settings_file::AgentFeaturesSettings::default,
    feature_labels: super::v1::FEATURE_LABELS,
};

#[cfg(test)]
mod tests {
    #[test]
    fn v2_8_keeps_previous_text_surfaces_and_feature_defaults() {
        let previous = super::super::resolve_entry("2.7");
        let current = super::super::resolve_entry("2.8");
        assert_eq!(current.version, "2.8");
        for auto_commit in [true, false] {
            assert_eq!(
                current.harness.suggested_next_steps_block(auto_commit),
                previous.harness.suggested_next_steps_block(auto_commit)
            );
        }
        assert_eq!(current.feature_labels, previous.feature_labels);
        assert_eq!((current.default_features)(), (previous.default_features)());
        assert_eq!(current.doctrine.specialists, previous.doctrine.specialists);
    }
}
