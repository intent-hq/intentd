//! Harness v3.4 teaches agents to link other workspaces
//! (`[Title](intent://local/workspace/<id>)`) instead of naming them by their
//! internal IDs. Only the common instruction body changes; text surfaces,
//! specialist prompts, repository guidance and feature defaults remain those
//! of v3.3.

use super::{Doctrine, HarnessEntry};

static DOCTRINE: Doctrine = Doctrine {
    instructions: &crate::instructions::V3_4,
    specialists: crate::specialists::EMBEDDED_BUNDLED_V3_1,
};

pub(crate) static ENTRY: HarnessEntry = HarnessEntry {
    version: "3.4",
    harness: super::v3_3::ENTRY.harness,
    doctrine: &DOCTRINE,
    default_features: intent_core::settings_file::AgentFeaturesSettings::default,
    feature_labels: super::v1::FEATURE_LABELS,
};

#[cfg(test)]
mod tests {
    use intent_core::{settings_file::AgentFeaturesSettings, AgentId};

    use crate::v1_goldens::{seed_agent, setup};

    const RULE_HEADING: &str = "## Link other workspaces\n";
    const LINK_EXAMPLE: &str = "`[Workspace Title](intent://local/workspace/<workspaceId>)`";

    /// The workspace link section: its heading through the text before the
    /// next `## ` heading.
    fn workspace_links_section() -> &'static str {
        let common = super::ENTRY.doctrine.instructions.common;
        let start = common.find(RULE_HEADING).expect("workspace link section");
        let body = start + RULE_HEADING.len();
        let end = common[body..]
            .find("\n## ")
            .map_or(common.len(), |i| body + i + 1);
        &common[start..end]
    }

    /// 3.4 adds exactly one common-body section; every other instruction
    /// body, text surface, specialist bundle and feature default is 3.3's.
    #[test]
    fn v3_4_only_adds_the_workspace_link_section() {
        let current = crate::harness::resolve_entry("3.4");
        let previous = crate::harness::resolve_entry("3.3");
        assert_eq!(current.version, "3.4");
        let (a, b) = (
            current.doctrine.instructions,
            previous.doctrine.instructions,
        );
        assert_eq!(
            a.common.replacen(workspace_links_section(), "", 1),
            b.common,
            "only the workspace link section changes"
        );
        for guidance in [
            LINK_EXAMPLE,
            "current title as the label",
            "Never name a workspace by its bare internal ID",
            "use the ID only in tool calls",
            "`markdownLink`",
            "Outside Intent",
        ] {
            assert!(workspace_links_section().contains(guidance), "{guidance}");
        }
        assert_eq!(a.workspace, b.workspace);
        assert_eq!(a.workspace_agent, b.workspace_agent);
        assert_eq!(a.assistant, b.assistant);
        assert_eq!(a.single_workspace_fallback, b.single_workspace_fallback);
        let data_ptr = |h: &'static dyn crate::harness::Harness| {
            std::ptr::from_ref::<dyn crate::harness::Harness>(h).cast::<()>()
        };
        assert!(
            std::ptr::eq(data_ptr(current.harness), data_ptr(previous.harness)),
            "3.4 keeps 3.3's text surfaces"
        );
        assert_eq!(current.doctrine.specialists, previous.doctrine.specialists);
        assert_eq!((current.default_features)(), (previous.default_features)());
        assert_eq!(current.feature_labels, previous.feature_labels);
    }

    /// End to end: a 3.4 session's prompt teaches workspace links and a
    /// 3.3 session's prompt stays exactly as it was, minus nothing else.
    #[tokio::test]
    async fn assembled_prompts_teach_workspace_links_only_from_3_4() {
        let (_tmp, svc, ws) = setup().await;
        let id = AgentId::from("agent-workspace-links");
        seed_agent(&svc, &ws, &id).await;
        let mut current = svc.store().get_agent_session(&id).await.unwrap();
        current.harness_version = "3.4".into();
        let mut previous = current.clone();
        previous.harness_version = "3.3".into();
        for agent_type in ["interactive", "workspace", "workspace-agent", "task-loop"] {
            let mut prompts = Vec::new();
            for session in [&current, &previous] {
                prompts.push(
                    crate::rules::assemble_system_prompt(
                        svc.store(),
                        None,
                        agent_type,
                        None,
                        false,
                        false,
                        false,
                        &AgentFeaturesSettings::default(),
                        None,
                        Some(session),
                        None,
                    )
                    .await
                    .expect("assembled prompt"),
                );
            }
            assert!(prompts[0].contains(LINK_EXAMPLE), "{agent_type}");
            assert!(!prompts[1].contains(RULE_HEADING), "{agent_type}");
            assert_eq!(
                prompts[0].replacen(workspace_links_section(), "", 1),
                prompts[1],
                "{agent_type}: 3.3 and 3.4 prompts differ only in the workspace link section"
            );
        }
    }
}
