//! Old sessions keep their context surfaces (harness gating M0). For every
//! provider in the registry, a session stamped 3.8 or 3.9 and respawned on
//! this build gets a byte-identical `workspace_api` tool description and
//! system prompt. Each row pins the bytes (length + SHA-256) of five
//! variants joined: top-level, sub-agent, chief, top-level with remote
//! placement text, and a plain folder with placement text (dropped there).

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
        443_409,
        "538f4e6903f6677c1e2d1bef4f049f3f24e5b25594cbcd5c82dde7f7f4ae4339",
        128_784,
        "28623c0928067b359b58504796cc59d75e5dc3cd21eb5d20b0bd2df28d9fcf0c",
    ),
    (
        "claude-code",
        9_486,
        "3493bceb2c12efa2442451acb19eb4523d1797d4acbc562f4d5142127e455ad9",
        249_125,
        "d235d02687553c886adf13393a8ad63b4a3264c7a168dd1731bd2a4f20120d22",
    ),
    (
        "codex",
        443_409,
        "538f4e6903f6677c1e2d1bef4f049f3f24e5b25594cbcd5c82dde7f7f4ae4339",
        128_784,
        "28623c0928067b359b58504796cc59d75e5dc3cd21eb5d20b0bd2df28d9fcf0c",
    ),
    (
        "cortex",
        443_409,
        "538f4e6903f6677c1e2d1bef4f049f3f24e5b25594cbcd5c82dde7f7f4ae4339",
        128_784,
        "28623c0928067b359b58504796cc59d75e5dc3cd21eb5d20b0bd2df28d9fcf0c",
    ),
    (
        "opencode",
        443_409,
        "538f4e6903f6677c1e2d1bef4f049f3f24e5b25594cbcd5c82dde7f7f4ae4339",
        128_784,
        "28623c0928067b359b58504796cc59d75e5dc3cd21eb5d20b0bd2df28d9fcf0c",
    ),
    (
        "unsloth",
        443_409,
        "538f4e6903f6677c1e2d1bef4f049f3f24e5b25594cbcd5c82dde7f7f4ae4339",
        128_784,
        "28623c0928067b359b58504796cc59d75e5dc3cd21eb5d20b0bd2df28d9fcf0c",
    ),
    (
        "pi",
        443_409,
        "538f4e6903f6677c1e2d1bef4f049f3f24e5b25594cbcd5c82dde7f7f4ae4339",
        128_784,
        "28623c0928067b359b58504796cc59d75e5dc3cd21eb5d20b0bd2df28d9fcf0c",
    ),
    (
        "droid",
        9_486,
        "3493bceb2c12efa2442451acb19eb4523d1797d4acbc562f4d5142127e455ad9",
        249_125,
        "d235d02687553c886adf13393a8ad63b4a3264c7a168dd1731bd2a4f20120d22",
    ),
    (
        "grok",
        9_486,
        "3493bceb2c12efa2442451acb19eb4523d1797d4acbc562f4d5142127e455ad9",
        249_125,
        "d235d02687553c886adf13393a8ad63b4a3264c7a168dd1731bd2a4f20120d22",
    ),
    (
        "antigravity",
        443_409,
        "538f4e6903f6677c1e2d1bef4f049f3f24e5b25594cbcd5c82dde7f7f4ae4339",
        128_784,
        "28623c0928067b359b58504796cc59d75e5dc3cd21eb5d20b0bd2df28d9fcf0c",
    ),
    (
        "cursor",
        443_409,
        "538f4e6903f6677c1e2d1bef4f049f3f24e5b25594cbcd5c82dde7f7f4ae4339",
        129_924,
        "220273303306a122e130b914bd99d4067fa16d8536f3840210ce6d24576633b5",
    ),
    (
        "mock",
        443_409,
        "538f4e6903f6677c1e2d1bef4f049f3f24e5b25594cbcd5c82dde7f7f4ae4339",
        128_784,
        "28623c0928067b359b58504796cc59d75e5dc3cd21eb5d20b0bd2df28d9fcf0c",
    ),
];

const RULES: &str = "# Repo rules\n\nKeep changes small.\n";
const PLACEMENT: &str = "Remote placement (experimental): fixture machine list.\n";
const SEPARATOR: &str = "\n\u{0}\n";

/// `n` with `_` thousands separators, as the table spells it.
fn grouped(n: usize) -> String {
    let digits = n.to_string();
    let mut out = String::new();
    for (i, digit) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i).is_multiple_of(3) {
            out.push('_');
        }
        out.push(digit);
    }
    out
}

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
    plain_folder: bool,
) -> (String, String) {
    let is_sub_agent = session.parent_agent_id.is_some();
    let features = AgentFeaturesSettings::default();
    let surfaces = SessionSurfaces::new(session, provider);
    let api: Arc<dyn WorkspaceApi> = Arc::new(services.clone());
    let server = surfaces.configure(
        WorkspaceMcpServer::for_agent_type(api, session.workspace_id.clone(), "interactive")
            .with_agent_features(features.clone())
            .with_sub_agent(is_sub_agent)
            .with_remote_placement_help(placement.map(str::to_owned))
            .with_plain_folder(plain_folder),
    );
    let docs = server.workspace_api_prompt_reference();
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
    for (session, placement, plain_folder) in [
        (&top, None, false),
        (&sub, None, false),
        (&chief, None, false),
        (&top, Some(PLACEMENT), false),
        (&top, Some(PLACEMENT), true),
    ] {
        let (tool, prompt) = variant(
            services,
            workspace_path,
            session,
            provider,
            placement,
            plain_folder,
        )
        .await;
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
            "    (\"{}\", {}, \"{tool_sha}\", {}, \"{prompt_sha}\"),",
            provider.id,
            grouped(tool_len),
            grouped(prompt_len)
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
         adds its row. A changed row means sessions on a frozen harness see new text: \
         gate the change on a new harness version, or update the row for a deliberate \
         fix to every version (rerun `make test-select \
         E='test(frozen_sessions_keep)'` and paste the rows below). Actual:\n{}",
        actual.join("\n")
    );
}

/// The frozen versions keep the original builders for the surfaces the
/// prompt fixture above does not reach (skills, placement), whose bytes
/// `v1_goldens` and the machine-selection tests pin.
#[test]
fn frozen_sessions_keep_the_original_builders() {
    for version in FROZEN_VERSIONS {
        let context = crate::harness::resolve_entry(version).context;
        assert!(
            std::ptr::eq(context, &raw const crate::harness::CONTEXT_V1),
            "{version}"
        );
    }
    let context = &crate::harness::CONTEXT_V1;
    let skills = [crate::skills::SkillMetadata {
        name: "pdf".into(),
        description: "Read & write <PDF> files.".into(),
        location: "/skills/pdf/SKILL.md".into(),
        resource_directory: Some("/skills/pdf".into()),
        scope: "user".into(),
        allowed_tools: None,
        compatibility: None,
    }];
    assert_eq!(
        (context.skills_block)(&crate::harness::SkillsBlockInputs { skills: &skills }),
        crate::skills::build_skills_catalog(&skills)
    );
    assert!(std::ptr::fn_addr_eq(
        context.placement_help,
        intent_core::machine_selection::exposed_machines_help
            as intent_core::machine_selection::PlacementHelpRenderer
    ));
    assert!(std::ptr::addr_eq(
        context.workspace_api,
        &raw const intent_acp::WORKSPACE_API_DOCS_V1
    ));
}
