//! Harness v3.5 adds development-only persistent inline visual guidance.
//! The released v3.4 workspace-link guidance and all earlier session prompts
//! remain unchanged; text surfaces, specialists and defaults stay v3.4's.

use super::{Doctrine, HarnessEntry};

static DOCTRINE: Doctrine = Doctrine {
    instructions: &crate::instructions::V3_5,
    specialists: super::v3_4::ENTRY.doctrine.specialists,
};

pub(crate) static ENTRY: HarnessEntry = HarnessEntry {
    version: "3.5",
    harness: super::v3_4::ENTRY.harness,
    doctrine: &DOCTRINE,
    default_features: intent_core::settings_file::AgentFeaturesSettings::default,
    feature_labels: super::v1::FEATURE_LABELS,
};

#[cfg(test)]
mod tests {
    use intent_core::{settings_file::AgentFeaturesSettings, AgentId};

    use crate::v1_goldens::{seed_agent, setup};

    #[test]
    fn v3_5_only_extends_rich_chat_guidance() {
        let current = &super::ENTRY;
        let previous = crate::harness::resolve_entry("3.4");
        let common = current.doctrine.instructions.common;
        let (released, visual) = common
            .split_once("\n\n### Inline visuals (unreleased)\n")
            .expect("development-only visual guidance");
        assert_eq!(released, previous.doctrine.instructions.common);
        assert!(!released.contains("ws-block:html"));
        for guidance in [
            "ws-block:html",
            "previewHtml",
            "window.intentVisual.state",
            "window.intentVisual.publish",
            "supporting frontend",
        ] {
            assert!(visual.contains(guidance), "{guidance}");
        }
        assert_eq!(
            current.doctrine.instructions.single_workspace_fallback,
            previous.doctrine.instructions.single_workspace_fallback
        );
        assert!(std::ptr::eq(current.harness, previous.harness));
        assert_eq!(current.doctrine.specialists, previous.doctrine.specialists);
        assert_eq!((current.default_features)(), (previous.default_features)());
        assert_eq!(current.feature_labels, previous.feature_labels);
    }

    #[tokio::test]
    async fn assembled_visual_guidance_is_session_pinned_and_rich_chat_gated() {
        let (_tmp, svc, ws) = setup().await;
        let id = AgentId::from("agent-inline-visual-guidance");
        seed_agent(&svc, &ws, &id).await;
        let mut visual = svc.store().get_agent_session(&id).await.unwrap();
        visual.harness_version = "3.5".into();
        let mut released = visual.clone();
        released.harness_version = "3.4".into();
        for rich_chat_blocks in [true, false] {
            let features = AgentFeaturesSettings {
                rich_chat_blocks,
                ..AgentFeaturesSettings::default()
            };
            for agent_type in ["interactive", "workspace", "workspace-agent", "task-loop"] {
                let mut prompts = Vec::new();
                for session in [&visual, &released] {
                    prompts.push(
                        crate::rules::assemble_system_prompt(
                            svc.store(),
                            None,
                            agent_type,
                            None,
                            false,
                            false,
                            false,
                            &features,
                            None,
                            Some(session),
                            None,
                        )
                        .await
                        .expect("assembled prompt"),
                    );
                }
                assert_eq!(prompts[0].contains("ws-block:html"), rich_chat_blocks);
                assert!(!prompts[1].contains("ws-block:html"));
                for prompt in &prompts {
                    assert!(prompt.contains("## Link other workspaces\n"));
                }
                if !rich_chat_blocks {
                    assert_eq!(
                        prompts[0], prompts[1],
                        "{agent_type}: disabling rich chat removes only the visual extension"
                    );
                }
            }
        }
    }
}
