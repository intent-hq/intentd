//! Harness v2.10 teaches ordinary Markdown app file links for workspace files.
//! All system-text surfaces and specialist prompts remain those of v2.9.

use super::{Doctrine, HarnessEntry};

static DOCTRINE: Doctrine = Doctrine {
    instructions: &crate::instructions::V2_10,
    specialists: crate::specialists::EMBEDDED_BUNDLED_V2_5,
};

pub(crate) static ENTRY: HarnessEntry = HarnessEntry {
    version: "2.10",
    harness: &super::v2_9::V2_9,
    doctrine: &DOCTRINE,
    default_features: intent_core::settings_file::AgentFeaturesSettings::default,
    feature_labels: super::v1::FEATURE_LABELS,
};
