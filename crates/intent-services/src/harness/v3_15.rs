//! Harness 3.15: 3.14 with the Maintenance Coordinator specialist renamed to
//! Maintainer. Only its display name and heading change; its id, body and
//! every other surface are 3.14's.

use super::{Doctrine, HarnessEntry};

static DOCTRINE: Doctrine = Doctrine {
    instructions: super::v3_14::ENTRY.doctrine.instructions,
    specialists: crate::specialists::EMBEDDED_BUNDLED_V3_15,
};

pub(crate) static ENTRY: HarnessEntry = HarnessEntry {
    version: "3.15",
    harness: super::v3_14::ENTRY.harness,
    doctrine: &DOCTRINE,
    context: super::v3_14::ENTRY.context,
    default_features: intent_core::settings_file::AgentFeaturesSettings::default,
    feature_labels: super::v1::FEATURE_LABELS,
};

#[cfg(test)]
mod tests {
    fn maintainer(version: &str) -> &'static str {
        super::super::resolve_entry(version)
            .doctrine
            .specialists
            .iter()
            .find_map(|(id, content)| (*id == "maintenance-coordinator").then_some(*content))
            .expect("maintainer specialist")
    }

    #[test]
    fn v3_15_is_3_14_with_the_maintainer_rename() {
        let current = super::super::resolve_entry("3.15");
        let prior = super::super::resolve_entry("3.14");
        assert_eq!(current.version, intent_core::CURRENT_HARNESS_VERSION);
        assert_eq!(intent_core::STAGED_HARNESS_VERSION, None);
        assert!(std::ptr::eq(current.context, prior.context));
        assert!(std::ptr::addr_eq(current.harness, prior.harness));
        assert!(std::ptr::eq(
            current.doctrine.instructions,
            prior.doctrine.instructions
        ));
        assert_eq!((current.default_features)(), (prior.default_features)());
        assert_eq!(current.feature_labels, prior.feature_labels);
        assert!(intent_core::prompt_breakdown::harness_records_prompt_breakdown("3.15"));

        let ids = |bundle: &[(&'static str, &'static str)]| -> Vec<&'static str> {
            bundle.iter().map(|(id, _)| *id).collect()
        };
        assert_eq!(
            ids(current.doctrine.specialists),
            ids(prior.doctrine.specialists)
        );
        for ((id, now), (_, before)) in current
            .doctrine
            .specialists
            .iter()
            .zip(prior.doctrine.specialists)
        {
            if *id != "maintenance-coordinator" {
                assert_eq!(now, before, "{id} is unchanged in 3.15");
            }
        }
    }

    #[test]
    fn v3_15_renames_only_the_name_and_heading() {
        let (now, before) = (maintainer("3.15"), maintainer("3.14"));
        assert_eq!(
            now,
            before
                .replacen(
                    "name: \"Maintenance Coordinator\"",
                    "name: \"Maintainer\"",
                    1
                )
                .replacen("## Maintenance Coordinator\n", "## Maintainer\n", 1)
        );
        assert!(!now.contains("Maintenance Coordinator"));
        let (meta, _) = crate::specialists::parse_frontmatter(now);
        assert_eq!(meta["name"], "Maintainer");
        assert_eq!(meta["icon"], "ralph");
    }
}
