//! Harness v2: v1.1 doctrine with scoped sibling workspace handoffs added to
//! the common guidance. Text surfaces and specialist bytes remain unchanged.

use super::{Doctrine, HarnessEntry};

static DOCTRINE: Doctrine = Doctrine {
    instructions: &crate::instructions::V2,
    specialists: crate::specialists::EMBEDDED_BUNDLED_V1_1,
};

pub(crate) static ENTRY: HarnessEntry = HarnessEntry {
    version: "2.0",
    harness: &super::v1::V1,
    doctrine: &DOCTRINE,
    default_features: intent_core::settings_file::AgentFeaturesSettings::default,
    feature_labels: super::v1::FEATURE_LABELS,
};
