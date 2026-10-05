//! App file links must survive rich-block gating and remain session-pinned.

use std::fmt::Write as _;

use intent_core::{settings_file::AgentFeaturesSettings, AgentId};
use sha2::{Digest, Sha256};

use crate::v1_goldens::{seed_agent, setup};

const PDF_LINK: &str =
    "[Open the report](intent://local/file/.intent/artifacts/Quarterly%20report%20%231.pdf)";

fn app_file_links_section() -> &'static str {
    let common = crate::harness::resolve_entry("2.10")
        .doctrine
        .instructions
        .common;
    let start = common.find("## Open workspace files in the app\n").unwrap();
    let end = common[start..].find("## Show media\n").unwrap() + start;
    &common[start..end]
}

#[test]
fn golden_v2_10_common_body_and_unchanged_surfaces() {
    let current = crate::harness::resolve_entry("2.10");
    let previous = crate::harness::resolve_entry("2.9");
    assert_eq!(current.version, "2.10");
    let common = current.doctrine.instructions.common;
    assert_eq!(
        Sha256::digest(common.as_bytes())
            .iter()
            .fold(String::new(), |mut s, b| {
                let _ = write!(s, "{b:02x}");
                s
            }),
        "9a9aa2dbbbec8fa03aa70462c64836e2894c8fd311aa77c961999222267013e6"
    );
    assert_eq!(
        common.replacen(app_file_links_section(), "", 1),
        previous.doctrine.instructions.common,
        "only the app file-link section changes"
    );
    let sender = || crate::harness::HostMemberSender {
        login: Some("member"),
        display_name: Some("Member"),
        principal_id: "principal-1",
        identity: None,
    };
    assert_eq!(
        current.harness.host_member_sender_preamble(sender()),
        previous.harness.host_member_sender_preamble(sender())
    );
    assert_eq!(current.doctrine.specialists, previous.doctrine.specialists);
    assert_eq!((current.default_features)(), (previous.default_features)());
    assert_eq!(current.feature_labels, previous.feature_labels);
}

#[tokio::test]
async fn app_file_links_in_assembled_prompts_are_session_pinned() {
    let (_tmp, svc, ws) = setup().await;
    let id = AgentId::from("agent-app-file-links");
    seed_agent(&svc, &ws, &id).await;
    let current = svc.store().get_agent_session(&id).await.unwrap();
    assert_eq!(
        current.harness_version,
        intent_core::CURRENT_HARNESS_VERSION
    );
    let mut previous = current.clone();
    previous.harness_version = "2.9".into();

    for rich_chat_blocks in [true, false] {
        let features = AgentFeaturesSettings {
            rich_chat_blocks,
            ..AgentFeaturesSettings::default()
        };
        for agent_type in ["interactive", "workspace", "task-loop"] {
            let mut prompts = Vec::new();
            for session in [Some(&current), None, Some(&previous)] {
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
                        session,
                        None,
                    )
                    .await
                    .expect("assembled prompt"),
                );
            }
            assert_eq!(prompts[0], prompts[1], "new session uses latest doctrine");
            assert!(prompts[0].contains(PDF_LINK), "new {agent_type} prompt must teach app file links with richChatBlocks={rich_chat_blocks}");
            assert!(!prompts[2].contains(PDF_LINK), "v2.9 stays frozen");
            assert_eq!(
                prompts[0].replacen(app_file_links_section(), "", 1),
                prompts[2],
                "assembled old/new prompts differ only in app file-link teaching"
            );
            for guidance in [
                "For any workspace file",
                "Percent-encode each path segment",
                "Remote absolute OS paths",
                "`file://` URLs",
                "generic relative Markdown URLs",
                "PDFs use clickable links, not image embeds",
                "preview and editing capabilities depend on that file's viewer",
                "Ordinary Markdown app file links work independently of rich chat blocks",
            ] {
                assert!(
                    prompts[0].contains(guidance),
                    "missing guidance: {guidance}"
                );
            }
            for prompt in &prompts {
                assert_eq!(prompt.contains("## Rich Chat Rendering"), rich_chat_blocks);
            }
        }
    }
}
