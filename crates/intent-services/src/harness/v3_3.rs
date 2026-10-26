//! Harness v3.3 composes unknown/default agent types with the workspace body
//! once instead of twice (intent-hq/intent#5835). Text surfaces, specialist
//! prompts, repository guidance and feature defaults remain those of v3.2.

use super::{Doctrine, HarnessEntry};

static DOCTRINE: Doctrine = Doctrine {
    instructions: &crate::instructions::V3_3,
    specialists: crate::specialists::EMBEDDED_BUNDLED_V3_1,
};

pub(crate) static ENTRY: HarnessEntry = HarnessEntry {
    version: "3.3",
    harness: super::v3_1::ENTRY.harness,
    doctrine: &DOCTRINE,
    default_features: intent_core::settings_file::AgentFeaturesSettings::default,
    feature_labels: super::v1::FEATURE_LABELS,
};

#[cfg(test)]
mod tests {
    use intent_core::{settings_file::AgentFeaturesSettings, AgentId};

    use crate::v1_goldens::{seed_agent, setup};

    const SPACE_HEADING: &str = "\n# Space\n";

    async fn assemble(
        svc: &crate::Services,
        agent_type: &str,
        session: Option<&intent_core::AgentSession>,
    ) -> String {
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
            session,
            None,
        )
        .await
        .expect("assembled prompt")
    }

    /// End to end through system-prompt assembly: a new (3.3) default agent
    /// carries the workspace block once, a 3.2-stamped session keeps its exact
    /// duplicated prompt, and a specialist agent type is byte-identical.
    #[tokio::test]
    async fn assembled_default_prompt_has_workspace_block_once_and_old_stamps_are_pinned() {
        let (_tmp, svc, ws) = setup().await;
        let id = AgentId::from("agent-5835");
        seed_agent(&svc, &ws, &id).await;
        let current = svc.store().get_agent_session(&id).await.unwrap();
        assert_eq!(current.harness_version, "3.3");
        let mut previous = current.clone();
        previous.harness_version = "3.2".into();
        let workspace = super::ENTRY.doctrine.instructions.workspace;
        let new = assemble(&svc, "interactive", Some(&current)).await;
        let old = assemble(&svc, "interactive", Some(&previous)).await;
        assert_eq!(new.matches(SPACE_HEADING).count(), 1);
        assert_eq!(old.matches(SPACE_HEADING).count(), 2);
        assert_eq!(
            old.replacen(&format!("\n\n---\n\n{workspace}"), "", 1),
            new,
            "3.3 removes exactly one workspace layer"
        );
        eprintln!(
            "interactive system prompt bytes: 3.2={} 3.3={} (-{})",
            old.len(),
            new.len(),
            old.len() - new.len()
        );
        for agent_type in ["task-loop", "workspace-agent", "workspace", "task-focused"] {
            assert_eq!(
                assemble(&svc, agent_type, Some(&current)).await,
                assemble(&svc, agent_type, Some(&previous)).await,
                "specialist agent type {agent_type}"
            );
        }
    }
}
