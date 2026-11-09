//! Harness 3.10: less context per agent. Staged
//! (`intent_core::STAGED_HARNESS_VERSION`) until it becomes current; only
//! the eval-only override stamps it. Each spawn-time builder below is the
//! extension point its work fills; until then each renders 3.9's bytes.

use super::{ContextSurfaces, Doctrine, HarnessEntry, RepoRulesInputs, SkillsBlockInputs};
use intent_core::machine_selection::PlacementHelpInputs;

static DOCTRINE: Doctrine = Doctrine {
    instructions: &crate::instructions::V3_10,
    specialists: crate::specialists::EMBEDDED_BUNDLED_V3_9,
};

static CONTEXT: ContextSurfaces = ContextSurfaces {
    workspace_api: &intent_acp::WORKSPACE_API_DOCS_V3_10,
    repo_rules,
    repo_rules_reads_executor: true,
    skills_block,
    placement_help,
};

pub(crate) static ENTRY: HarnessEntry = HarnessEntry {
    version: "3.10",
    harness: super::v3_9::ENTRY.harness,
    doctrine: &DOCTRINE,
    context: &CONTEXT,
    default_features: intent_core::settings_file::AgentFeaturesSettings::default,
    feature_labels: super::v1::FEATURE_LABELS,
};

/// Repo-rule injection: skip the picked rule file when the provider
/// running the agent loads it by itself.
fn repo_rules(inputs: &RepoRulesInputs<'_>) -> crate::rules::RepoRules {
    crate::rules::workspace_rules_native_aware(inputs)
}

/// The skills block: budget and hidden skills.
fn skills_block(inputs: &SkillsBlockInputs<'_>) -> String {
    super::skills_block_v1(inputs)
}

/// The remote-placement text: stable machine identity only.
fn placement_help(inputs: &PlacementHelpInputs<'_>) -> Option<String> {
    intent_core::machine_selection::exposed_machines_help(inputs)
}

#[cfg(test)]
mod tests {
    use intent_core::ids::ExecutorCredentialId;
    use intent_core::machines::{ExposedMachine, RemoteMachineState};

    use crate::session_surface_goldens::{fixture, surfaces_for};

    fn machine(id: &str) -> ExposedMachine {
        ExposedMachine {
            id: id.into(),
            name: format!("{id} name"),
            hosts: vec!["10.0.0.1".into()],
            port: 7443,
            tc_address: None,
            fingerprint: "ab".repeat(32),
            credential_id: ExecutorCredentialId::new(),
            platform: None,
            capabilities: None,
            observed_at: None,
            state: RemoteMachineState::Disconnected,
            added_at: "2026-10-01T00:00:00Z".into(),
            last_connected_at: Some("2026-11-01T08:00:00Z".into()),
            node_protocol: None,
            cloud: None,
        }
    }

    #[test]
    fn v3_10_starts_from_v3_9() {
        let current = super::super::resolve_entry("3.10");
        let previous = super::super::resolve_entry("3.9");
        assert_eq!(current.version, "3.10");
        assert_eq!(current.doctrine.specialists, previous.doctrine.specialists);
        assert!(std::ptr::addr_eq(current.harness, previous.harness));
        assert_eq!((current.default_features)(), (previous.default_features)());
        assert_eq!(current.feature_labels, previous.feature_labels);
        assert_eq!(
            Some(current.version),
            intent_core::STAGED_HARNESS_VERSION,
            "3.10 is staged until it becomes current"
        );
    }

    /// Until each builder is filled, a 3.10 session gets exactly a 3.9
    /// session's tool description, for every provider. (The doctrine already
    /// differs; `v3_10_goldens` pins it.)
    #[tokio::test]
    async fn v3_10_context_matches_v3_9_until_changed() {
        let (_db, dir, services, session) = fixture().await;
        for provider in intent_providers::config::ACP_PROVIDERS {
            assert_eq!(
                surfaces_for(&services, dir.path(), &session, "3.10", provider)
                    .await
                    .0,
                surfaces_for(&services, dir.path(), &session, "3.9", provider)
                    .await
                    .0,
                "{}",
                provider.id
            );
        }
        let (current, previous) = (
            super::super::resolve_entry("3.10").context,
            super::super::resolve_entry("3.9").context,
        );
        let rules = super::RepoRulesInputs {
            workspace_path: dir.path(),
            executor: None,
        };
        assert_eq!((current.repo_rules)(&rules), (previous.repo_rules)(&rules));
        let skills = [crate::skills::SkillMetadata {
            name: "pdf".into(),
            description: "Read and write PDF files.".into(),
            location: "/skills/pdf/SKILL.md".into(),
            resource_directory: None,
            scope: "user".into(),
            allowed_tools: None,
            compatibility: None,
        }];
        let skills = super::SkillsBlockInputs { skills: &skills };
        assert_eq!(
            (current.skills_block)(&skills),
            (previous.skills_block)(&skills)
        );
        let machines = [machine("d-mac"), machine("d-box")];
        let connected = |id: &str| id == "d-mac";
        let inputs =
            intent_core::machine_selection::PlacementHelpInputs::new(&machines, &connected);
        assert_eq!(
            (current.placement_help)(&inputs),
            (previous.placement_help)(&inputs)
        );
    }
}
