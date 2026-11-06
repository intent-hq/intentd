//! Source-aligned Developer workflow and general-agent delegation.

use super::{Doctrine, HarnessEntry};

static DOCTRINE: Doctrine = Doctrine {
    instructions: &crate::instructions::V3_8,
    specialists: crate::specialists::EMBEDDED_BUNDLED_V3_8,
};

pub(crate) static ENTRY: HarnessEntry = HarnessEntry {
    version: "3.8",
    harness: super::v3_7::ENTRY.harness,
    doctrine: &DOCTRINE,
    default_features: intent_core::settings_file::AgentFeaturesSettings::default,
    feature_labels: super::v1::FEATURE_LABELS,
};

#[cfg(test)]
mod tests {
    use std::fmt::Write as _;

    use sha2::{Digest, Sha256};

    #[test]
    fn golden_v3_8_specialist_bytes() {
        for (id, expected) in [
            (
                "developer",
                "88c23a198f88a6bf313618baa69eef3d4acea31268b6a81e88b8d22d9335286d",
            ),
            (
                "pr-reviewer",
                "cac2b97a8be75d09178f486531ff42c28e4089f3b53e0ae5a7500bb256dabfba",
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
    fn v3_8_versions_workflows_and_removes_specialized_delegation_roles() {
        let current = super::super::resolve_entry("3.8");
        let previous = super::super::resolve_entry("3.7");
        assert_eq!(current.version, "3.8");
        let (now, then) = (
            current.doctrine.instructions,
            previous.doctrine.instructions,
        );
        assert_eq!(
            now.common,
            then.common.replace(", specialist: \"implementor\"", "")
        );
        assert_eq!(now.workspace, then.workspace);
        assert_eq!(now.workspace_agent, then.workspace_agent);
        assert_eq!(now.assistant, then.assistant);
        assert_eq!(
            now.single_workspace_fallback,
            then.single_workspace_fallback
        );
        assert!(std::ptr::eq(
            now.plain_folder.unwrap(),
            then.plain_folder.unwrap()
        ));
        assert!(std::ptr::addr_eq(current.harness, previous.harness));
        assert_eq!((current.default_features)(), (previous.default_features)());
        assert_eq!(current.feature_labels, previous.feature_labels);
        assert_eq!(
            current.doctrine.specialists.len(),
            previous.doctrine.specialists.len() - 3
        );
        for (old_id, old) in previous.doctrine.specialists {
            let replacement = current
                .doctrine
                .specialists
                .iter()
                .find(|(id, _)| id == old_id);
            if matches!(*old_id, "verifier" | "spec-writer" | "implementor") {
                assert!(replacement.is_none());
                continue;
            }
            let (new_id, new) = replacement.expect("retained specialist");
            if matches!(*new_id, "developer" | "pr-reviewer") {
                assert_ne!(old, new, "{new_id}");
                let (old_meta, _) = crate::specialists::parse_frontmatter(old);
                let (new_meta, _) = crate::specialists::parse_frontmatter(new);
                let keys = &["name", "role", "teamAgents", "icon", "aliases", "model"];
                for key in keys {
                    assert_eq!(old_meta.get(*key), new_meta.get(*key), "{new_id}: {key}");
                }
            } else {
                assert_eq!(old, new, "{new_id}");
            }
        }
    }
}
