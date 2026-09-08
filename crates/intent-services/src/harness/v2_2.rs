//! Harness v2.2: v2.1 doctrine with the workspace status-message guidance
//! rewritten in `workspace.md` and `workspace-agent.md` to require one
//! short plain sentence. Specialist bytes and system-text surfaces remain
//! unchanged.

use super::{Doctrine, HarnessEntry};

static DOCTRINE: Doctrine = Doctrine {
    instructions: &crate::instructions::V2_2,
    specialists: crate::specialists::EMBEDDED_BUNDLED_V2_1,
};

pub(crate) static ENTRY: HarnessEntry = HarnessEntry {
    version: "2.2",
    harness: &super::v1::V1,
    doctrine: &DOCTRINE,
    default_features: intent_core::settings_file::AgentFeaturesSettings::default,
    feature_labels: super::v1::FEATURE_LABELS,
};
