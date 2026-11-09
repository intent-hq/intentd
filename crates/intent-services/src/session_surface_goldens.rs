//! Old sessions keep their context surfaces (harness gating M0). For every
//! provider in the registry, a session stamped 3.8 or 3.9 and respawned on
//! this build gets a byte-identical `workspace_api` tool description and
//! system prompt. Each row pins the bytes (length + SHA-256) of four
//! variants joined: top-level, sub-agent, chief, and top-level with remote
//! placement text.

use std::fmt::Write as _;
use std::path::Path;
use std::sync::Arc;

use intent_acp::WorkspaceMcpServer;
use intent_core::settings_file::AgentFeaturesSettings;
use intent_core::{AgentId, AgentSession, WorkspaceApi, WorkspaceId};
use intent_providers::ProviderConfig;
use sha2::{Digest, Sha256};

use crate::session_surfaces::SessionSurfaces;
use crate::test_support::goldens::{seed_agent, setup};
use crate::Services;

/// Harness versions whose surfaces must never change.
const FROZEN_VERSIONS: &[&str] = &["3.8", "3.9"];

/// `(provider, tool description bytes, SHA-256, system prompt bytes, SHA-256)`.
const FROZEN_SURFACES: &[(&str, usize, &str, usize, &str)] = &[
    (
        "auggie",
        373602,
        "e9f8b4649f75bd119a73828db99c4a04954e45c0c0bcb956bab92da5c05f2667",
        96897,
        "feb9cb81916a6181ee44c8555d017d727102b49c3ef0d696371f53f583073d54",
    ),
    (
        "claude-code",
        7704,
        "a35deab89ba2ae13aec1efaecf88cefc46ed652d7aa0f6e2fc4d7934c1e57c5a",
        195279,
        "7635c3d168cda224a22183b22e16c5eb402b57884b7e9c60299f979cbe45ad8d",
    ),
    (
        "codex",
        373602,
        "e9f8b4649f75bd119a73828db99c4a04954e45c0c0bcb956bab92da5c05f2667",
        96897,
        "feb9cb81916a6181ee44c8555d017d727102b49c3ef0d696371f53f583073d54",
    ),
    (
        "cortex",
        373602,
        "e9f8b4649f75bd119a73828db99c4a04954e45c0c0bcb956bab92da5c05f2667",
        96897,
        "feb9cb81916a6181ee44c8555d017d727102b49c3ef0d696371f53f583073d54",
    ),
    (
        "opencode",
        373602,
        "e9f8b4649f75bd119a73828db99c4a04954e45c0c0bcb956bab92da5c05f2667",
        96897,
        "feb9cb81916a6181ee44c8555d017d727102b49c3ef0d696371f53f583073d54",
    ),
    (
        "unsloth",
        373602,
        "e9f8b4649f75bd119a73828db99c4a04954e45c0c0bcb956bab92da5c05f2667",
        96897,
        "feb9cb81916a6181ee44c8555d017d727102b49c3ef0d696371f53f583073d54",
    ),
    (
        "pi",
        373602,
        "e9f8b4649f75bd119a73828db99c4a04954e45c0c0bcb956bab92da5c05f2667",
        96897,
        "feb9cb81916a6181ee44c8555d017d727102b49c3ef0d696371f53f583073d54",
    ),
    (
        "droid",
        7704,
        "a35deab89ba2ae13aec1efaecf88cefc46ed652d7aa0f6e2fc4d7934c1e57c5a",
        195279,
        "7635c3d168cda224a22183b22e16c5eb402b57884b7e9c60299f979cbe45ad8d",
    ),
    (
        "grok",
        7704,
        "a35deab89ba2ae13aec1efaecf88cefc46ed652d7aa0f6e2fc4d7934c1e57c5a",
        195279,
        "7635c3d168cda224a22183b22e16c5eb402b57884b7e9c60299f979cbe45ad8d",
    ),
    (
        "antigravity",
        373602,
        "e9f8b4649f75bd119a73828db99c4a04954e45c0c0bcb956bab92da5c05f2667",
        96897,
        "feb9cb81916a6181ee44c8555d017d727102b49c3ef0d696371f53f583073d54",
    ),
    (
        "cursor",
        373602,
        "e9f8b4649f75bd119a73828db99c4a04954e45c0c0bcb956bab92da5c05f2667",
        97809,
        "d3ff4c1bdb59ba7fdc7caa9930ed134beb352714f2e68038808e5bf2f43bdd1c",
    ),
    (
        "mock",
        373602,
        "e9f8b4649f75bd119a73828db99c4a04954e45c0c0bcb956bab92da5c05f2667",
        96897,
        "feb9cb81916a6181ee44c8555d017d727102b49c3ef0d696371f53f583073d54",
    ),
];

const RULES: &str = "# Repo rules\n\nKeep changes small.\n";
const PLACEMENT: &str = "Remote placement (experimental): fixture machine list.\n";
const SEPARATOR: &str = "\n\u{0}\n";

fn sha256_hex(text: &str) -> String {
    Sha256::digest(text.as_bytes())
        .iter()
        .fold(String::new(), |mut hex, byte| {
            let _ = write!(hex, "{byte:02x}");
            hex
        })
}

/// One variant's `(tool description, system prompt)`, built the way a spawn
/// builds them for `session` on `provider`.
async fn variant(
    services: &Services,
    workspace_path: &Path,
    session: &AgentSession,
    provider: &ProviderConfig,
    placement: Option<&str>,
) -> (String, String) {
    let is_sub_agent = session.parent_agent_id.is_some();
    let features = AgentFeaturesSettings::default();
    let surfaces = SessionSurfaces::new(session, provider);
    let api: Arc<dyn WorkspaceApi> = Arc::new(services.clone());
    let server = surfaces.configure(
        WorkspaceMcpServer::for_agent_type(api, session.workspace_id.clone(), "interactive")
            .with_agent_features(features.clone())
            .with_sub_agent(is_sub_agent)
            .with_remote_placement_help(placement.map(str::to_owned)),
    );
    let docs = surfaces.prompt_docs(&server);
    let prompt = crate::rules::assemble_system_prompt(
        services.store(),
        Some(workspace_path),
        "interactive",
        None,
        is_sub_agent,
        false,
        false,
        &features,
        None,
        Some(session),
        docs.as_deref(),
    )
    .await
    .expect("assembled prompt");
    (
        server.workspace_api_tool_description(),
        crate::rules::with_provider_prompt_note(prompt, provider),
    )
}

/// Every variant's surfaces for `provider` with the session stamped
/// `version`, joined: `(tool descriptions, system prompts)`.
pub(crate) async fn surfaces_for(
    services: &Services,
    workspace_path: &Path,
    base: &AgentSession,
    version: &str,
    provider: &ProviderConfig,
) -> (String, String) {
    let mut top = base.clone();
    top.harness_version = version.to_owned();
    let mut sub = top.clone();
    sub.parent_agent_id = Some(AgentId::from("agent-parent"));
    let mut chief = top.clone();
    chief.workspace_id = WorkspaceId::chief();
    let mut tools = Vec::new();
    let mut prompts = Vec::new();
    for (session, placement) in [
        (&top, None),
        (&sub, None),
        (&chief, None),
        (&top, Some(PLACEMENT)),
    ] {
        let (tool, prompt) = variant(services, workspace_path, session, provider, placement).await;
        tools.push(tool);
        prompts.push(prompt);
    }
    // The rule file's source is its absolute path, which differs per run.
    let path = workspace_path.to_string_lossy();
    (
        tools.join(SEPARATOR),
        prompts
            .join(SEPARATOR)
            .replace(path.as_ref(), "<workspace>"),
    )
}

/// A seeded session and a workspace folder holding an `AGENTS.md`.
pub(crate) async fn fixture() -> (
    crate::test_support::goldens::TempDb,
    tempfile::TempDir,
    Services,
    AgentSession,
) {
    let (db, services, ws) = setup().await;
    let id = AgentId::from("agent-surfaces");
    seed_agent(&services, &ws, &id).await;
    let session = services
        .store()
        .get_agent_session_with_transcript(&id)
        .await
        .expect("seeded session");
    let dir = crate::test_support::test_tempdir("session-surfaces-");
    std::fs::write(dir.path().join("AGENTS.md"), RULES).expect("write AGENTS.md");
    (db, dir, services, session)
}

#[tokio::test]
async fn frozen_sessions_keep_tool_description_and_system_prompt() {
    let (_db, dir, services, session) = fixture().await;
    let mut actual = Vec::new();
    let mut mismatches = Vec::new();
    for provider in intent_providers::config::ACP_PROVIDERS {
        let mut rows = Vec::new();
        for version in FROZEN_VERSIONS {
            let (tool, prompt) =
                surfaces_for(&services, dir.path(), &session, version, provider).await;
            rows.push((
                tool.len(),
                sha256_hex(&tool),
                prompt.len(),
                sha256_hex(&prompt),
            ));
        }
        assert!(
            rows.windows(2).all(|pair| pair[0] == pair[1]),
            "{}: frozen versions disagree",
            provider.id
        );
        let (tool_len, tool_sha, prompt_len, prompt_sha) = rows.remove(0);
        let expected = FROZEN_SURFACES.iter().find(|row| row.0 == provider.id);
        if expected
            != Some(&(
                provider.id,
                tool_len,
                tool_sha.as_str(),
                prompt_len,
                prompt_sha.as_str(),
            ))
        {
            mismatches.push(provider.id);
        }
        actual.push(format!(
            "    (\"{}\", {tool_len}, \"{tool_sha}\", {prompt_len}, \"{prompt_sha}\"),",
            provider.id
        ));
    }
    let registered: Vec<&str> = intent_providers::config::ACP_PROVIDERS
        .iter()
        .map(|provider| provider.id)
        .collect();
    let stale: Vec<&str> = FROZEN_SURFACES
        .iter()
        .map(|row| row.0)
        .filter(|id| !registered.contains(id))
        .collect();
    assert!(
        mismatches.is_empty() && stale.is_empty(),
        "frozen surfaces changed for {mismatches:?} (stale rows {stale:?}). A new provider \
         adds its row; an existing row never changes. Actual:\n{}",
        actual.join("\n")
    );
}
