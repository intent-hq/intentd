//! Source-aligned Developer and Lead workflows with minimal verification.

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
                "78a5c202fb320a498445e02c88459035b2976f26b303986346c28ca7437fa51e",
            ),
            (
                "implementor",
                "88eba423004822b1fa6320f10f1a9c80d71783774957b9313e123c665d2a32b8",
            ),
            (
                "spec-writer",
                "ea4d894f36a2863db0738045f326b2c502cc4b35003efa053209d3a6fc3ed903",
            ),
            (
                "pr-reviewer",
                "0ca55d5b0bd14f942cea27f96b6f5e5a3a3115e86c25229b65845a55045729a7",
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
    fn v3_6_versions_workflows_and_removes_verifier() {
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
            previous.doctrine.specialists.len() - 1
        );
        for (old_id, old) in previous.doctrine.specialists {
            let replacement = current
                .doctrine
                .specialists
                .iter()
                .find(|(id, _)| id == old_id);
            if *old_id == "verifier" {
                assert!(replacement.is_none());
                continue;
            }
            let (new_id, new) = replacement.expect("retained specialist");
            if matches!(
                *new_id,
                "developer" | "spec-writer" | "implementor" | "pr-reviewer"
            ) {
                assert_ne!(old, new, "{new_id}");
                let (old_meta, _) = crate::specialists::parse_frontmatter(old);
                let (new_meta, _) = crate::specialists::parse_frontmatter(new);
                let keys = if *new_id == "spec-writer" {
                    &["icon", "model"][..]
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
