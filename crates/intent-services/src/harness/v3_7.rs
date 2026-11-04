//! Harness v3.7 adds a one-sentence common "Scratch Files" section right
//! after "GitHub & Git Operations": scratch files (drafts, patches, logs) go in the agent's
//! private `$TMPDIR` (`intent_core::agent_tmp`) or come from `mktemp`, never a
//! fixed `/tmp/<name>` path shared with other agents (cloudlands-fe#4102:
//! one agent's `/tmp` PR body was overwritten by another's and posted to the
//! wrong PR). It is its own section so plain-folder prompts, which drop
//! the GitHub section, keep it. Every other body, text surfaces, specialists and defaults stay
//! v3.6's, and earlier sessions keep their prompts byte for byte.

use super::{Doctrine, HarnessEntry};

static DOCTRINE: Doctrine = Doctrine {
    instructions: &crate::instructions::V3_7,
    specialists: super::v3_6::ENTRY.doctrine.specialists,
};

pub(crate) static ENTRY: HarnessEntry = HarnessEntry {
    version: "3.7",
    harness: super::v3_6::ENTRY.harness,
    doctrine: &DOCTRINE,
    default_features: intent_core::settings_file::AgentFeaturesSettings::default,
    feature_labels: super::v1::FEATURE_LABELS,
};

#[cfg(test)]
mod tests {
    use intent_core::{settings_file::AgentFeaturesSettings, AgentId};

    use crate::v1_goldens::{seed_agent, setup};

    /// The whole v3.7 section, from its heading through the blank line
    /// before the next heading.
    const PARAGRAPH: &str =
        "## Scratch Files\n\nYour shell's `$TMPDIR` is a private temp folder for this agent: \
put scratch files (drafts, patches, logs) there or use `mktemp`, never a fixed \
`/tmp/<name>` path that other agents share.\n\n";

    #[test]
    fn v3_7_only_adds_the_private_tmpdir_sentence() {
        let current = &super::ENTRY;
        let previous = crate::harness::resolve_entry("3.6");
        let common = current.doctrine.instructions.common;
        assert_eq!(common.matches(PARAGRAPH).count(), 1);
        assert_eq!(
            common.replacen(PARAGRAPH, "", 1),
            previous.doctrine.instructions.common
        );
        assert!(!previous.doctrine.instructions.common.contains("$TMPDIR"));
        let at = common.find(PARAGRAPH).unwrap();
        assert!(
            common[..at].ends_with("`gh` CLI.\n\n")
                && common[at + PARAGRAPH.len()..].starts_with("## Browser tabs\n"),
            "the section follows GitHub & Git Operations"
        );
        let (now, then) = (
            current.doctrine.instructions,
            previous.doctrine.instructions,
        );
        assert_eq!(now.workspace, then.workspace);
        assert_eq!(now.workspace_agent, then.workspace_agent);
        assert_eq!(now.assistant, then.assistant);
        assert_eq!(
            now.single_workspace_fallback,
            then.single_workspace_fallback
        );
        assert!(std::ptr::eq(
            now.plain_folder.expect("plain-folder doctrine"),
            then.plain_folder.expect("plain-folder doctrine")
        ));
        // Trait-object vtables can be duplicated across codegen units.
        assert!(std::ptr::addr_eq(current.harness, previous.harness));
        assert_eq!(current.doctrine.specialists, previous.doctrine.specialists);
        assert_eq!((current.default_features)(), (previous.default_features)());
        assert_eq!(current.feature_labels, previous.feature_labels);
    }

    #[test]
    fn plain_folder_prompts_keep_the_section() {
        let set = super::ENTRY.doctrine.instructions;
        let features = AgentFeaturesSettings::default();
        for agent_type in ["interactive", "workspace", "workspace-agent", "task-loop"] {
            let plain = crate::instructions::get_instruction_with_common_in(
                set, agent_type, &features, true,
            );
            assert!(plain.contains("## Plain folder workspace"), "{agent_type}");
            assert!(
                !plain.contains("## GitHub & Git Operations"),
                "{agent_type}"
            );
            assert!(
                plain.contains(PARAGRAPH),
                "{agent_type}: plain folder keeps it"
            );
        }
    }

    #[tokio::test]
    async fn tmpdir_guidance_is_session_pinned_and_ungated() {
        let (_tmp, svc, ws) = setup().await;
        let id = AgentId::from("agent-private-tmpdir-guidance");
        seed_agent(&svc, &ws, &id).await;
        let mut current = svc
            .store()
            .get_agent_session_with_transcript(&id)
            .await
            .unwrap();
        current.harness_version = "3.7".into();
        let mut previous = current.clone();
        previous.harness_version = "3.6".into();
        for scripts in [true, false] {
            let features = AgentFeaturesSettings {
                scripts,
                ..AgentFeaturesSettings::default()
            };
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
                            &features,
                            None,
                            Some(session),
                            None,
                        )
                        .await
                        .expect("assembled prompt"),
                    );
                }
                assert!(
                    prompts[0].contains(PARAGRAPH),
                    "{agent_type}: 3.7 carries the sentence"
                );
                assert!(
                    !prompts[1].contains("$TMPDIR"),
                    "{agent_type}: 3.6 stays frozen"
                );
                assert_eq!(
                    prompts[0].replacen(PARAGRAPH, "", 1),
                    prompts[1],
                    "{agent_type}: 3.7 differs from 3.6 only by the sentence"
                );
            }
        }
        let plain = intent_core::Workspace {
            plain_folder: true,
            ..Default::default()
        };
        for agent_type in ["interactive", "workspace", "workspace-agent", "task-loop"] {
            let prompt = crate::rules::assemble_system_prompt(
                svc.store(),
                None,
                agent_type,
                None,
                false,
                false,
                false,
                &AgentFeaturesSettings::default(),
                Some(&plain),
                Some(&current),
                None,
            )
            .await
            .expect("assembled prompt");
            assert!(
                !prompt.contains("## GitHub & Git Operations"),
                "{agent_type}: plain folder drops the GitHub section"
            );
            assert!(
                prompt.contains(PARAGRAPH),
                "{agent_type}: plain folder keeps the scratch-file section"
            );
        }
    }
}
