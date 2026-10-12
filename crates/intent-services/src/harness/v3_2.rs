//! Development-only inline visual guidance; existing sessions retain their doctrine.

use super::{Doctrine, HarnessEntry};

static DOCTRINE: Doctrine = Doctrine {
    instructions: &crate::instructions::V3_2,
    specialists: crate::specialists::EMBEDDED_BUNDLED_V3_1,
};

pub(crate) static ENTRY: HarnessEntry = HarnessEntry {
    version: "3.2",
    harness: super::v3_1::ENTRY.harness,
    doctrine: &DOCTRINE,
    default_features: intent_core::settings_file::AgentFeaturesSettings::default,
    feature_labels: super::v1::FEATURE_LABELS,
};
