//! Harness 3.10: less context per agent. Staged
//! (`intent_core::STAGED_HARNESS_VERSION`) until it becomes current; only
//! the eval-only override stamps it. Each spawn-time builder below is the
//! extension point its work fills; until then each renders 3.9's bytes.

use super::{ContextSurfaces, Doctrine, HarnessEntry, RepoRulesInputs, SkillsBlockInputs};

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
    placement_help: intent_core::machine_selection::stable_machines_help,
    placement_reference: intent_core::machine_selection::stable_machines_reference,
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

#[cfg(test)]
mod tests {
    use intent_core::ids::ExecutorCredentialId;
    use intent_core::machine_selection::PlacementHelpInputs;
    use intent_core::machines::{ExposedMachine, RemoteMachineState};
    use intent_core::node_link::{MachineCapabilities, ProviderCapability, ProviderStatus};
    use intent_core::nodes::{NodeArch, NodeOs};
    use intent_core::placement::AgentPlatform;

    use crate::session_surface_goldens::{fixture, surfaces_for, variant};

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

    fn reported(
        id: &str,
        observed_at: &str,
        status: ProviderStatus,
        models: &[&str],
    ) -> ExposedMachine {
        let mut row = machine(id);
        let platform = AgentPlatform {
            os: NodeOs::Macos,
            arch: NodeArch::Aarch64,
        };
        let mut caps = MachineCapabilities::new(platform, "0.11.163", 4, observed_at);
        caps.providers.push(ProviderCapability {
            id: "claude-code".into(),
            visible: true,
            status,
            models: models.iter().map(|m| (*m).to_owned()).collect(),
            models_stale: false,
        });
        row.platform = Some(platform);
        row.observed_at = Some(observed_at.into());
        row.capabilities = Some(caps);
        row
    }

    /// Two spawns minutes apart get byte-identical surfaces on every
    /// provider while the machines' state, last-seen time, report time,
    /// sign-in status and model lists change. The same change does show in
    /// a 3.9 session's text, so the check would catch a regression.
    #[tokio::test]
    async fn v3_10_surfaces_stay_byte_identical_while_machines_change() {
        let (_db, dir, services, mut session) = fixture().await;
        session.harness_version = "3.10".into();
        let mut before = vec![
            reported(
                "d-mac",
                "2026-11-09T12:00:00Z",
                ProviderStatus::SignedIn,
                &["opus"],
            ),
            reported(
                "d-box",
                "2026-11-09T12:01:00Z",
                ProviderStatus::SignedOut,
                &[],
            ),
        ];
        before[1].state = RemoteMachineState::Unreachable;
        let mut after = before.clone();
        after[0] = reported(
            "d-mac",
            "2026-11-09T12:07:00Z",
            ProviderStatus::AuthFailed,
            &["opus", "sonnet", "haiku"],
        );
        after[0].last_connected_at = Some("2026-11-09T12:06:30Z".into());
        after[1].state = RemoteMachineState::Connected;
        after[1].last_connected_at = Some("2026-11-09T12:05:00Z".into());
        let connected_before = |id: &str| id == "d-mac";
        let connected_after = |id: &str| id == "d-box";

        let render = super::super::resolve_entry("3.10").context.placement_help;
        let text_before =
            render(&PlacementHelpInputs::new(&before, &connected_before)).expect("machines listed");
        let text_after =
            render(&PlacementHelpInputs::new(&after, &connected_after)).expect("machines listed");
        assert_eq!(text_before, text_after);
        for live in [
            "2026-11-09",
            "signed",
            "opus",
            "connected",
            "unreachable",
            "last seen",
        ] {
            assert!(!text_before.contains(live), "{live} in {text_before}");
        }
        assert!(
            text_before.contains("ws.agent.machineModels"),
            "{text_before}"
        );

        for provider in intent_providers::config::ACP_PROVIDERS {
            let first = variant(
                &services,
                dir.path(),
                &session,
                provider,
                Some(&text_before),
                false,
            )
            .await;
            let second = variant(
                &services,
                dir.path(),
                &session,
                provider,
                Some(&text_after),
                false,
            )
            .await;
            assert_eq!(first, second, "{}", provider.id);
        }

        let old = super::super::resolve_entry(intent_core::CURRENT_HARNESS_VERSION)
            .context
            .placement_help;
        assert_ne!(
            old(&PlacementHelpInputs::new(&before, &connected_before)),
            old(&PlacementHelpInputs::new(&after, &connected_after)),
            "3.9 text carries live machine facts"
        );
    }
}
