//! Harness v3.7 adds one sentence to the common "GitHub & Git Operations"
//! section: scratch files (PR bodies, patches, logs) go in the agent's
//! private `$TMPDIR` (`intent_core::agent_tmp`) or come from `mktemp`, never a
//! fixed `/tmp/<name>` path shared with other agents (cloudlands-fe#4102:
//! one agent's `/tmp` PR body was overwritten by another's and posted to the
//! wrong PR). Every other body, text surfaces, specialists and defaults stay
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

    /// The v3.7 paragraph, including its trailing blank line.
    const PARAGRAPH: &str = "Your shell's `$TMPDIR` is a private temp folder for this agent: \
put scratch files (PR bodies, patches, logs) there or use `mktemp`, never a fixed \
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
        let git = common.find("## GitHub & Git Operations\n").unwrap();
        let at = common.find(PARAGRAPH).unwrap();
        let next = common[git + 1..].find("\n## ").unwrap() + git + 1;
        assert!(
            git < at && at < next,
            "the sentence sits in the GitHub & Git Operations section"
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

    #[tokio::test]
    async fn tmpdir_guidance_is_session_pinned_and_ungated() {
        let (_tmp, svc, ws) = setup().await;
        let id = AgentId::from("agent-private-tmpdir-guidance");
        seed_agent(&svc, &ws, &id).await;
        let mut current = svc.store().get_agent_session(&id).await.unwrap();
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
    }
}
