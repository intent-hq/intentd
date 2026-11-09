//! Harness v3.6 adds a "Long-running Processes" common section that steers
//! dev servers, watchers and long builds to Intent scripts (`ws.script.*`)
//! instead of provider-native background shells such as Claude Code's
//! `Bash` `run_in_background` and `Monitor`. Every other body, text surfaces,
//! specialists and defaults stay v3.5's, and earlier sessions keep their
//! prompts byte for byte.

use super::{Doctrine, HarnessEntry};

static DOCTRINE: Doctrine = Doctrine {
    instructions: &crate::instructions::V3_6,
    specialists: super::v3_5::ENTRY.doctrine.specialists,
};

pub(crate) static ENTRY: HarnessEntry = HarnessEntry {
    version: "3.6",
    harness: super::v3_5::ENTRY.harness,
    doctrine: &DOCTRINE,
    context: &super::CONTEXT_V1,
    default_features: intent_core::settings_file::AgentFeaturesSettings::default,
    feature_labels: super::v1::FEATURE_LABELS,
};

#[cfg(test)]
mod tests {
    use intent_core::{settings_file::AgentFeaturesSettings, AgentId};

    use crate::v1_goldens::{seed_agent, setup};

    const HEADING: &str = "## Long-running Processes\n";

    /// Split `common` into (text without the v3.6 section, the section).
    fn split_section(common: &str) -> (String, &str) {
        let start = common.find(HEADING).expect("v3.6 section");
        let body = start + HEADING.len();
        let end = common[body..].find("\n## ").expect("next section") + body + 1;
        (
            format!("{}{}", &common[..start], &common[end..]),
            &common[start..end],
        )
    }

    #[test]
    fn v3_6_only_adds_the_long_running_processes_section() {
        let current = &super::ENTRY;
        let previous = crate::harness::resolve_entry("3.5");
        let common = current.doctrine.instructions.common;
        let (rest, section) = split_section(common);
        assert_eq!(rest, previous.doctrine.instructions.common);
        assert!(!previous.doctrine.instructions.common.contains(HEADING));
        for guidance in [
            "ws.script.create",
            "ws.script.start",
            "ws.script.monitor",
            "ws.script.stop",
            "autoMonitor.ok",
            "autoMonitorError",
            "run_in_background",
            "`Monitor`",
            "nohup",
        ] {
            assert!(section.contains(guidance), "{guidance}");
        }
        assert!(
            common.contains("## Waiting on External Conditions")
                && common.find("## Waiting on External Conditions").unwrap()
                    < common.find(HEADING).unwrap(),
            "the section follows the waiting guidance"
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
    fn heading_constant_matches_the_bundled_section() {
        assert_eq!(
            format!(
                "## {}\n",
                crate::instructions::COMMON_LONG_RUNNING_PROCESSES
            ),
            HEADING
        );
    }

    #[tokio::test]
    async fn script_guidance_is_session_pinned_and_scripts_gated() {
        let (_tmp, svc, ws) = setup().await;
        let id = AgentId::from("agent-long-running-guidance");
        seed_agent(&svc, &ws, &id).await;
        let mut current = svc
            .store()
            .get_agent_session_with_transcript(&id)
            .await
            .unwrap();
        current.harness_version = "3.6".into();
        let mut previous = current.clone();
        previous.harness_version = "3.5".into();
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
                assert_eq!(
                    prompts[0].contains(HEADING),
                    scripts,
                    "{agent_type}: section follows agentFeatures.scripts"
                );
                assert!(
                    !prompts[1].contains(HEADING),
                    "{agent_type}: 3.5 stays frozen"
                );
                let without_section = if scripts {
                    let start = prompts[0].find(HEADING).unwrap();
                    let body = start + HEADING.len();
                    let end = prompts[0][body..].find("\n## ").unwrap() + body + 1;
                    format!("{}{}", &prompts[0][..start], &prompts[0][end..])
                } else {
                    prompts[0].clone()
                };
                assert_eq!(
                    without_section, prompts[1],
                    "{agent_type}: 3.6 differs from 3.5 only by the section"
                );
            }
        }
    }
}
