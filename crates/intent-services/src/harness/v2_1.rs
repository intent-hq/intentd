//! Harness v2.1: v2 doctrine with the bundled Vulnerability Scanner added.
//! Instruction and system-text bytes remain unchanged.

use super::{Doctrine, HarnessEntry};

static DOCTRINE: Doctrine = Doctrine {
    instructions: &crate::instructions::V2,
    specialists: crate::specialists::EMBEDDED_BUNDLED_V2_1,
};

pub(crate) static ENTRY: HarnessEntry = HarnessEntry {
    version: "2.1",
    harness: &super::v1::V1,
    doctrine: &DOCTRINE,
    default_features: intent_core::settings_file::AgentFeaturesSettings::default,
    feature_labels: super::v1::FEATURE_LABELS,
};
