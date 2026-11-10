//! Released visual guidance, based on 3.9 without the 3.10 context experiment.

use super::{Doctrine, HarnessEntry};

static DOCTRINE: Doctrine = Doctrine {
    instructions: &crate::instructions::V3_11,
    specialists: super::v3_9::ENTRY.doctrine.specialists,
};

pub(crate) static ENTRY: HarnessEntry = HarnessEntry {
    version: "3.11",
    harness: super::v3_9::ENTRY.harness,
    doctrine: &DOCTRINE,
    context: &super::CONTEXT_V1,
    default_features: intent_core::settings_file::AgentFeaturesSettings::default,
    feature_labels: super::v1::FEATURE_LABELS,
};

#[cfg(test)]
mod tests {
    use intent_core::{settings_file::AgentFeaturesSettings, AgentId};

    use crate::v1_goldens::{seed_agent, setup};

    #[tokio::test]
    async fn released_visual_guidance_is_session_pinned_and_feature_gated() {
        let (_tmp, svc, ws) = setup().await;
        let id = AgentId::from("agent-released-inline-visual-guidance");
        seed_agent(&svc, &ws, &id).await;
        let mut session = svc
            .store()
            .get_agent_session_with_transcript(&id)
            .await
            .unwrap();
        let current = super::super::resolve_entry("3.11");
        let previous = super::super::resolve_entry("3.9");
        let staged = super::super::resolve_entry("3.10");
        assert_eq!(intent_core::CURRENT_HARNESS_VERSION, current.version);
        assert_eq!(intent_core::STAGED_HARNESS_VERSION, Some(staged.version));
        assert!(std::ptr::eq(current.context, previous.context));
        assert!(!std::ptr::eq(current.context, staged.context));
        assert!(std::ptr::addr_eq(current.harness, previous.harness));
        assert_eq!(current.doctrine.specialists, previous.doctrine.specialists);
        assert_eq!((current.default_features)(), (previous.default_features)());
        assert!(!intent_core::prompt_breakdown::harness_records_prompt_breakdown(current.version));
        for plain_folder in [false, true] {
            let workspace = intent_core::Workspace {
                plain_folder,
                ..Default::default()
            };
            for rich_chat_blocks in [true, false] {
                let features = AgentFeaturesSettings {
                    rich_chat_blocks,
                    ..AgentFeaturesSettings::default()
                };
                for agent_type in ["interactive", "workspace", "workspace-agent", "task-loop"] {
                    let mut prompts = Vec::new();
                    for version in ["3.9", "3.11"] {
                        session.harness_version = version.into();
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
                                Some(&workspace),
                                Some(&session),
                                None,
                            )
                            .await
                            .expect("assembled prompt"),
                        );
                    }
                    let [old, new] = prompts.as_slice() else {
                        unreachable!()
                    };
                    if rich_chat_blocks {
                        assert!(old.contains("### Inline visuals (unreleased)"));
                        assert!(!new.contains("### Inline visuals (unreleased)"));
                        assert!(new.contains("ws-block:html"));
                        assert_eq!(
                            old.split("### Inline visuals").next(),
                            new.split("### Inline visuals").next()
                        );
                    } else {
                        assert_eq!(new, old, "feature off preserves the released prompt");
                        assert!(!new.contains("ws-block:html"));
                    }
                }
            }
        }
    }
}
