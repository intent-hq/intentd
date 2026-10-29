//! Focused specialist workflows with shared, risk-based verification evidence.

use super::{Doctrine, HarnessEntry};

static DOCTRINE: Doctrine = Doctrine {
    instructions: super::v3_5::ENTRY.doctrine.instructions,
    specialists: crate::specialists::EMBEDDED_BUNDLED_V3_6,
};

pub(crate) static ENTRY: HarnessEntry = HarnessEntry {
    version: "3.6",
    harness: super::v3_5::ENTRY.harness,
    doctrine: &DOCTRINE,
    default_features: intent_core::settings_file::AgentFeaturesSettings::default,
    feature_labels: super::v1::FEATURE_LABELS,
};

#[cfg(test)]
mod tests {
    use std::fmt::Write as _;

    use sha2::{Digest, Sha256};

    #[test]
    fn golden_v3_6_specialist_bytes() {
        for (id, expected) in [
            (
                "developer",
                "da2e7af947cfab164c7195a2a1dce2eb338781051bd2667564b3586e41477ffb",
            ),
            (
                "implementor",
                "9112d15e740935b67492fcca7b88fa906aa36178c2303f4b32c430fe9edd57fb",
            ),
            (
                "spec-writer",
                "23519f72689f58af8f88174b11886662278f8fd844c63f1f0f6db0b51dbe3d93",
            ),
            (
                "verifier",
                "f1e7caa77c837942b2e0643cd017417ea8958d163a94f4a9c991b11e56f6aaa9",
            ),
        ] {
            let content = super::ENTRY
                .doctrine
                .specialists
                .iter()
                .find_map(|(candidate, content)| (*candidate == id).then_some(*content))
                .expect("versioned specialist");
            let actual = Sha256::digest(content)
                .iter()
                .fold(String::new(), |mut hex, byte| {
                    let _ = write!(hex, "{byte:02x}");
                    hex
                });
            assert_eq!(actual, expected, "{id}");
        }
    }

    #[test]
    fn v3_6_versions_only_the_four_workflow_specialists() {
        let current = super::super::resolve_entry("3.6");
        let previous = super::super::resolve_entry("3.5");
        assert_eq!(current.version, "3.6");
        assert!(std::ptr::eq(
            current.doctrine.instructions,
            previous.doctrine.instructions
        ));
        assert!(std::ptr::addr_eq(current.harness, previous.harness));
        assert_eq!((current.default_features)(), (previous.default_features)());
        assert_eq!(current.feature_labels, previous.feature_labels);
        assert_eq!(
            current.doctrine.specialists.len(),
            previous.doctrine.specialists.len()
        );
        for ((old_id, old), (new_id, new)) in previous
            .doctrine
            .specialists
            .iter()
            .zip(current.doctrine.specialists)
        {
            assert_eq!(old_id, new_id);
            if matches!(
                *new_id,
                "developer" | "spec-writer" | "implementor" | "verifier"
            ) {
                assert_ne!(old, new, "{new_id}");
                let (old_meta, _) = crate::specialists::parse_frontmatter(old);
                let (new_meta, _) = crate::specialists::parse_frontmatter(new);
                let keys = if *new_id == "spec-writer" {
                    &["teamAgents", "icon", "model"][..]
                } else {
                    &["name", "role", "teamAgents", "icon", "aliases", "model"][..]
                };
                for key in keys {
                    assert_eq!(old_meta.get(*key), new_meta.get(*key), "{new_id}: {key}");
                }
            } else {
                assert_eq!(old, new, "{new_id}");
            }
        }
    }
}
