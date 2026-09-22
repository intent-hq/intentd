//! Harness v2.7: v2.6 doctrine and text surfaces plus the consolidated
//! archive-watch notice — the single, resume-aware wake an agent reads
//! after its workspace was unarchived, naming every background hook and PR
//! monitor the archive cancelled and how to re-arm each kind
//! ([`super::Harness::workspace_archived_watches_cancelled_notice`]).

use super::{Doctrine, HarnessEntry};

static DOCTRINE: Doctrine = Doctrine {
    instructions: &crate::instructions::V2_6,
    specialists: crate::specialists::EMBEDDED_BUNDLED_V2_5,
};

pub(crate) static ENTRY: HarnessEntry = HarnessEntry {
    version: "2.7",
    harness: &super::v2_4::V2_4,
    doctrine: &DOCTRINE,
    default_features: intent_core::settings_file::AgentFeaturesSettings::default,
    feature_labels: super::v1::FEATURE_LABELS,
};

#[cfg(test)]
mod tests {
    #[test]
    fn v2_7_is_latest_and_keeps_v2_6_doctrine() {
        let previous = super::super::resolve_entry("2.6");
        let current = super::super::latest_entry();
        assert_eq!(current.version, "2.7");
        assert!(std::ptr::eq(
            current.doctrine.instructions,
            previous.doctrine.instructions
        ));
        assert_eq!(current.doctrine.specialists, previous.doctrine.specialists);
        for auto_commit in [true, false] {
            assert_eq!(
                current.harness.suggested_next_steps_block(auto_commit),
                previous.harness.suggested_next_steps_block(auto_commit)
            );
        }
    }
}
