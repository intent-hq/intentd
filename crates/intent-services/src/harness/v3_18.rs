//! Harness 3.18: 3.17 with a Developer specialist that writes every unit of
//! work as a `@@@task` block, also when it does the work itself, and keeps
//! the status of the tasks it does current. Every other surface is 3.17's.

use super::{Doctrine, HarnessEntry};

static DOCTRINE: Doctrine = Doctrine {
    instructions: super::v3_17::ENTRY.doctrine.instructions,
    specialists: crate::specialists::EMBEDDED_BUNDLED_V3_18,
};

pub(crate) static ENTRY: HarnessEntry = HarnessEntry {
    version: "3.18",
    harness: super::v3_17::ENTRY.harness,
    doctrine: &DOCTRINE,
    context: super::v3_17::ENTRY.context,
    default_features: intent_core::settings_file::AgentFeaturesSettings::default,
    feature_labels: super::v1::FEATURE_LABELS,
};

#[cfg(test)]
mod tests {
    fn developer(version: &str) -> &'static str {
        super::super::resolve_entry(version)
            .doctrine
            .specialists
            .iter()
            .find_map(|(id, content)| (*id == "developer").then_some(*content))
            .expect("developer specialist")
    }

    #[test]
    fn v3_18_is_3_17_with_a_new_developer() {
        let current = super::super::resolve_entry("3.18");
        let prior = super::super::resolve_entry("3.17");
        assert_eq!(current.version, "3.18");
        assert!(std::ptr::addr_eq(current.harness, prior.harness));
        assert!(std::ptr::eq(current.context, prior.context));
        assert!(std::ptr::eq(
            current.doctrine.instructions,
            prior.doctrine.instructions
        ));
        assert_eq!((current.default_features)(), (prior.default_features)());
        assert_eq!(current.feature_labels, prior.feature_labels);
        assert!(intent_core::prompt_breakdown::harness_records_prompt_breakdown("3.18"));

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
            if *id != "developer" {
                assert_eq!(now, before, "{id} is unchanged in 3.18");
            }
        }
    }

    #[test]
    fn v3_18_developer_writes_task_blocks_for_its_own_work() {
        let (now, before) = (developer("3.18"), developer("3.17"));
        let old_rule = "Delegate to subagents as needed, but be sure to not duplicate efforts. \
                        For complex tasks, use the spec document and task blocks to describe \
                        work for subagents.";
        assert!(before.contains(old_rule));
        assert!(!now.contains(old_rule));
        assert!(now.contains("also when you do the work yourself"));
        assert!(now.contains("ws.task.updateNoteStatus"));

        let (meta_now, _) = crate::specialists::parse_frontmatter(now);
        let (meta_before, _) = crate::specialists::parse_frontmatter(before);
        assert_eq!(meta_now, meta_before);
    }
}
