//! Adds the bundled Product Manager specialist; every other surface is 3.8's.

use super::{Doctrine, HarnessEntry};

static DOCTRINE: Doctrine = Doctrine {
    instructions: &crate::instructions::V3_8,
    specialists: crate::specialists::EMBEDDED_BUNDLED_V3_9,
};

pub(crate) static ENTRY: HarnessEntry = HarnessEntry {
    version: "3.9",
    harness: super::v3_8::ENTRY.harness,
    doctrine: &DOCTRINE,
    default_features: intent_core::settings_file::AgentFeaturesSettings::default,
    feature_labels: super::v1::FEATURE_LABELS,
};

#[cfg(test)]
mod tests {
    use std::fmt::Write as _;

    use sha2::{Digest, Sha256};

    #[test]
    fn golden_v3_9_product_manager_bytes() {
        let content = super::ENTRY
            .doctrine
            .specialists
            .iter()
            .find_map(|(id, content)| (*id == "product-manager").then_some(*content))
            .expect("product manager specialist");
        let actual = Sha256::digest(content)
            .iter()
            .fold(String::new(), |mut hex, byte| {
                let _ = write!(hex, "{byte:02x}");
                hex
            });
        assert_eq!(
            actual,
            "9b07977dcae665abfe636e9cf863eef280666dcb7b709874a34f140d22aa6cec"
        );
    }

    #[test]
    fn v3_9_only_adds_the_product_manager() {
        let current = super::super::resolve_entry("3.9");
        let previous = super::super::resolve_entry("3.8");
        assert_eq!(current.version, "3.9");
        assert!(std::ptr::eq(
            current.doctrine.instructions,
            previous.doctrine.instructions
        ));
        assert!(std::ptr::addr_eq(current.harness, previous.harness));
        assert_eq!((current.default_features)(), (previous.default_features)());
        assert_eq!(current.feature_labels, previous.feature_labels);
        let specialists: &[(&str, &str)] = current.doctrine.specialists;
        let (added, kept) = specialists.split_last().expect("non-empty bundle");
        assert_eq!(added.0, "product-manager");
        assert_eq!(kept, previous.doctrine.specialists);
        let (meta, body) = crate::specialists::parse_frontmatter(added.1);
        assert_eq!(meta["name"], "Product Manager");
        assert_eq!(meta["icon"], "coordinator");
        assert!(meta.get("model").is_none());
        assert!(body.contains(r#"{ specialist: "product-manager" }"#));
        assert!(body.contains(r#"{ topLevel: true, specialist: "developer" }"#));
    }
}
