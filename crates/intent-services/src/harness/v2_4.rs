//! Harness v2.4: v2.3 text surfaces and instructions with versioned
//! PR-context handoffs for the Coordinator, Implementor, and Verifier.

use super::{Doctrine, HarnessEntry};

static DOCTRINE: Doctrine = Doctrine {
    instructions: &crate::instructions::V2_2,
    specialists: crate::specialists::EMBEDDED_BUNDLED_V2_4,
};

pub(crate) static ENTRY: HarnessEntry = HarnessEntry {
    version: "2.4",
    harness: &super::v2_3::V2_3,
    doctrine: &DOCTRINE,
    default_features: intent_core::settings_file::AgentFeaturesSettings::default,
    feature_labels: super::v1::FEATURE_LABELS,
};
