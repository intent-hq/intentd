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
                "1348b1d2841ac50dfed5c0b8432deecb2b0e243813804b2a240ef6c4e2f5a67e",
            ),
            (
                "implementor",
                "97845965252272d19d9a273510efb49d03d506216e7433f1d6b661d3446320ae",
            ),
            (
                "spec-writer",
                "b5f04fff9a330727b405c99e0a7d36219e68cb90d5d2c111beb9c3714d404676",
            ),
            (
                "verifier",
                "2c3661cc46cd9787186491752c6106375109e45b318d1a88e53943df1f662a2e",
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
                for key in ["name", "role", "teamAgents", "icon", "aliases", "model"] {
                    assert_eq!(old_meta.get(key), new_meta.get(key), "{new_id}: {key}");
                }
            } else {
                assert_eq!(old, new, "{new_id}");
            }
        }
    }
}
