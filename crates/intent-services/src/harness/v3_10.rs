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
    hidden_skills,
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

/// The skills block: hides the skills this agent cannot use and lists the
/// rest within a budget.
fn skills_block(inputs: &SkillsBlockInputs<'_>) -> String {
    crate::skill_catalog::build(inputs.skills, inputs.environment).text
}

/// The skills the skills block leaves out, with why.
fn hidden_skills(
    inputs: &SkillsBlockInputs<'_>,
) -> Vec<intent_core::prompt_breakdown::HiddenSkill> {
    crate::skill_catalog::build(inputs.skills, inputs.environment).hidden
}

/// The remote-placement text: stable machine identity only.
fn placement_help(inputs: &PlacementHelpInputs<'_>) -> Option<String> {
    intent_core::machine_selection::exposed_machines_help(inputs)
}

#[cfg(test)]
mod tests {
    use intent_core::ids::ExecutorCredentialId;
    use intent_core::machines::{ExposedMachine, RemoteMachineState};

    use crate::session_surface_goldens::fixture;

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

    /// Until each builder is filled, a 3.10 session gets a 3.9 session's
    /// context; the `workspace_api` surfaces already differ (pinned by
    /// `v3_10_workspace_api_surfaces`).
    #[tokio::test]
    async fn v3_10_context_matches_v3_9_until_changed() {
        let (_db, dir, _services, _session) = fixture().await;
        let (current, previous) = (
            super::super::resolve_entry("3.10").context,
            super::super::resolve_entry("3.9").context,
        );
        let rules = super::RepoRulesInputs {
            workspace_path: dir.path(),
            executor: None,
        };
        assert_eq!((current.repo_rules)(&rules), (previous.repo_rules)(&rules));
        let machines = [machine("d-mac"), machine("d-box")];
        let connected = |id: &str| id == "d-mac";
        let inputs =
            intent_core::machine_selection::PlacementHelpInputs::new(&machines, &connected);
        assert_eq!(
            (current.placement_help)(&inputs),
            (previous.placement_help)(&inputs)
        );
    }

    /// 3.10 hides the skills an agent cannot use and budgets the rest;
    /// older harnesses keep the full catalog.
    #[test]
    fn v3_10_skills_block_hides_and_budgets() {
        let skill = |name: &str, compatibility: Option<&str>| crate::skills::SkillMetadata {
            name: name.into(),
            description: "Read and write PDF files.".into(),
            location: format!("/skills/{name}/SKILL.md"),
            resource_directory: None,
            scope: "user".into(),
            allowed_tools: None,
            compatibility: compatibility.map(str::to_string),
            mcp_servers: Vec::new(),
        };
        let skills = [skill("pdf", None), skill("desktop", Some("Cowork only"))];
        let environment = crate::skill_catalog::SkillEnvironment::new("codex", None);
        let inputs = super::SkillsBlockInputs {
            skills: &skills,
            environment: &environment,
        };
        let block = crate::skill_catalog::build(&skills, &environment);
        let current = super::super::resolve_entry("3.10").context;
        assert_eq!((current.skills_block)(&inputs), block.text);
        let hidden = (current.hidden_skills)(&inputs);
        assert_eq!(hidden, block.hidden);
        assert_eq!(
            hidden
                .iter()
                .map(|h| (h.name.as_str(), h.reason))
                .collect::<Vec<_>>(),
            [(
                "desktop",
                intent_core::prompt_breakdown::HiddenSkillReason::Compatibility
            )]
        );
        let previous = super::super::resolve_entry("3.8").context;
        assert_eq!(
            (previous.skills_block)(&inputs),
            crate::skills::build_skills_catalog(&skills)
        );
        assert_eq!((previous.hidden_skills)(&inputs), []);
    }
}
