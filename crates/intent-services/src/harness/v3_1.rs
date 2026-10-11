//! Focused app-level Assistant instructions; other agent doctrine is unchanged.

use super::{Doctrine, HarnessEntry};

static DOCTRINE: Doctrine = Doctrine {
    instructions: &crate::instructions::V3_1,
    specialists: crate::specialists::EMBEDDED_BUNDLED_V3_1,
};

pub(crate) static ENTRY: HarnessEntry = HarnessEntry {
    version: "3.1",
    harness: &super::v2_9::V2_9,
    doctrine: &DOCTRINE,
    default_features: intent_core::settings_file::AgentFeaturesSettings::default,
    feature_labels: super::v1::FEATURE_LABELS,
};
