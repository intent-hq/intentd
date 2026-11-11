//! Harness 3.12: releases 3.10's context cuts (compact ws.* reference,
//! native repo-rule dedupe, budgeted skills, stable machine list, trimmed
//! doctrine, prompt breakdown) together with 3.11's released inline-visuals
//! guidance. 3.11 shipped while 3.10 was staged, so the cuts take the next
//! number instead of moving new sessions back to 3.10.

use super::{Doctrine, HarnessEntry};

static DOCTRINE: Doctrine = Doctrine {
    instructions: &crate::instructions::V3_12,
    specialists: super::v3_10::ENTRY.doctrine.specialists,
};

pub(crate) static ENTRY: HarnessEntry = HarnessEntry {
    version: "3.12",
    harness: super::v3_10::ENTRY.harness,
    doctrine: &DOCTRINE,
    context: &super::v3_10::CONTEXT,
    default_features: intent_core::settings_file::AgentFeaturesSettings::default,
    feature_labels: super::v1::FEATURE_LABELS,
};

#[cfg(test)]
mod tests {
    use crate::instructions::{V3_10, V3_11, V3_12};

    #[test]
    fn v3_12_is_3_10_context_with_3_11_visuals() {
        let current = super::super::resolve_entry("3.12");
        let cuts = super::super::resolve_entry("3.10");
        assert_eq!(current.version, "3.12");
        assert!(std::ptr::eq(current.context, cuts.context));
        assert!(std::ptr::addr_eq(current.harness, cuts.harness));
        assert_eq!(current.doctrine.specialists, cuts.doctrine.specialists);
        assert_eq!((current.default_features)(), (cuts.default_features)());
        assert_eq!(current.feature_labels, cuts.feature_labels);
        assert!(intent_core::prompt_breakdown::harness_records_prompt_breakdown("3.12"));
        // The body is 3.10's with a shorter Follow-up Workspaces section (it
        // keeps the 50% budget), plus 3.11's released visuals block verbatim.
        let visuals = &V3_11.common[V3_11
            .common
            .find("### Inline visuals\n")
            .expect("3.11 visuals")..];
        assert!(V3_12.common.ends_with(&format!("\n\n{visuals}")));
        let section = |body: &'static str| {
            let start = body.find("## Follow-up Workspaces\n").expect("section");
            let end = body
                .find("## GitHub & Git Operations\n")
                .expect("next section");
            (&body[..start], &body[start..end], &body[end..])
        };
        let (before, follow_up, after) = section(V3_12.common);
        let (before_3_10, follow_up_3_10, after_3_10) = section(V3_10.common);
        assert_eq!(before, before_3_10);
        assert_eq!(
            after,
            format!("{}\n\n{visuals}", after_3_10.trim_end()),
            "only Follow-up Workspaces differs from 3.10, plus the visuals block"
        );
        assert!(follow_up.len() < follow_up_3_10.len());
        for kept in [
            "ws.workspace.proposeSibling({ title, initialPrompt, specialist?, baseRef? })",
            "clearly separate from the current request",
            "inherited and locked",
            "the user must approve",
            "never say the workspace exists before Apply succeeds",
            "Delegated or background agents never call this",
            "ws.agent.reportToParent",
        ] {
            assert!(follow_up.contains(kept), "{kept}");
        }
        assert!(!V3_12.common.contains("(unreleased)"));
        assert_eq!(V3_12.workspace, V3_10.workspace);
    }
}
