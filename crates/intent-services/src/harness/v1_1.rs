//! Harness **v1.1**: the v1 text surfaces with a rewritten doctrine set —
//! the feature-section rewrites in `common.md` ("Task relations during
//! delegation", "Waiting on External Conditions", and "Rich Chat Rendering"
//! compressed to doctrine-only text; mechanics live in the ws.* docs).
//! Every other instruction body and the specialist bundle are byte-identical
//! copies of v1 (pinned by `crate::v1_1_goldens`). The [`Harness`] text
//! surfaces are unchanged: this entry reuses the v1 singleton, so the
//! v1→v1.1 diff is exactly the doctrine.

use super::{v1, Doctrine, HarnessEntry};

/// v1.1's bundled doctrine: the `resources/agent-instructions/v1.1/`
/// instruction set and the `resources/specialists/v1.1/` embedded specialist
/// bundle.
static DOCTRINE: Doctrine = Doctrine {
    instructions: &crate::instructions::V1_1,
    specialists: crate::specialists::EMBEDDED_BUNDLED_V1_1,
};

/// The v1.1 registry row. `version` is intent-core's stamped `"1.1"`
/// (asserted equal to `CURRENT_HARNESS_VERSION` by registry tests). The
/// harness (text surfaces), feature defaults, and feature labels are v1's —
/// only the doctrine changed.
pub(crate) static ENTRY: HarnessEntry = HarnessEntry {
    version: "1.1",
    harness: &v1::V1,
    doctrine: &DOCTRINE,
    default_features: intent_core::settings_file::AgentFeaturesSettings::default,
    feature_labels: v1::FEATURE_LABELS,
};
