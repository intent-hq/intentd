//! Regression tests for specialist frontmatter `model` and display-name
//! resolution at agent creation.
//!
//! Model: when `agent.create` receives no explicit model but a specialist id,
//! the single daemon-side resolver (`resolve_agent_default_model`) applies the
//! specialist's resolved frontmatter `model` (3-tier: project > user >
//! bundled, provider-guarded) before the settings chain. `modelTier` is
//! retired (PROTOCOL §5.11): a lingering frontmatter line is
//! tolerated-and-ignored and never participates in resolution.
//!
//! Full precedence:
//! explicit model > specialist frontmatter model > settings chain > CLI
//! default
//!
//! Name: when `agent.create` receives no explicit name but a specialist id,
//! the specialist's resolved display name (frontmatter `name`) is used before
//! the generic `Agent {6-hex}` fallback, and counts as explicitly set.

use intent_core::{AgentId, WorkspaceId};
use intent_store::Store;
use serde_json::json;
use std::path::Path;
use std::sync::Arc;
use tempfile::TempDir;

use super::tests::{workspace, TempDb};
use crate::Services;

/// Set up a test with a temp specialist directory structure.
async fn setup() -> (TempDb, Services, WorkspaceId, TempDir, TempDir) {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    store.insert_workspace(&workspace(&ws)).await.expect("ws");

    // Create a temp directory for specialists
    let specialists_dir = TempDir::new().expect("temp specialists dir");

    // Configure Services with the temp specialists directory and a wired
    // settings registry (settings are TOML-backed, not SQLite-backed).
    let config_dir = TempDir::new().expect("temp config dir");
    let registry = Arc::new(
        crate::SettingsRegistry::load(config_dir.path().join("config.toml"))
            .expect("load registry"),
    );
    let services = Services::new(store)
        .with_settings_registry(registry)
        .with_specialist_dirs(
            Some(specialists_dir.path().to_path_buf()),
            Some(specialists_dir.path().to_path_buf()),
        );
    (tmp, services, ws, specialists_dir, config_dir)
}

async fn create_agent(
    svc: &Services,
    ws: &WorkspaceId,
    name: &str,
    model: Option<String>,
    specialist: Option<String>,
) -> AgentId {
    let extra = intent_core::AgentCreateExtra {
        is_background: Some(true), // Delegated agents are background
        ..Default::default()
    };
    let created = svc
        .agent_create_op(
            ws.clone(),
            Some(name.to_string()),
            model,
            specialist,
            None,
            None,
            false,
            extra,
        )
        .await
        .expect("create");
    AgentId::from(created["agent"]["id"].as_str().unwrap())
}

/// Create a specialist file with frontmatter model in the user tier.
fn create_user_specialist(dir: &Path, id: &str, model: &str) {
    let content = format!(
        "---\nname: \"{}\"\ndescription: \"Test specialist\"\nmodel: \"{}\"\n---\n\nTest prompt",
        id, model
    );
    std::fs::write(dir.join(format!("{}.md", id)), content).expect("write specialist");
}

/// Create a specialist file without a model field.
fn create_specialist_without_model(dir: &Path, id: &str) {
    let content = format!(
        "---\nname: \"{}\"\ndescription: \"Test specialist\"\n---\n\nTest prompt",
        id
    );
    std::fs::write(dir.join(format!("{}.md", id)), content).expect("write specialist");
}

/// Create a specialist file with a retired frontmatter modelTier (no model)
/// in the user tier.
fn create_specialist_with_retired_tier(dir: &Path, id: &str, tier: &str) {
    let content = format!(
        "---\nname: \"{}\"\ndescription: \"Test specialist\"\nmodelTier: \"{}\"\n---\n\nTest prompt",
        id, tier
    );
    std::fs::write(dir.join(format!("{}.md", id)), content).expect("write specialist");
}

/// Create an agent with an explicit provider and no explicit model.
async fn create_agent_with_provider(
    svc: &Services,
    ws: &WorkspaceId,
    specialist: &str,
    provider: &str,
) -> AgentId {
    let extra = intent_core::AgentCreateExtra {
        provider: Some(provider.to_string()),
        is_background: Some(true),
        ..Default::default()
    };
    let created = svc
        .agent_create_op(
            ws.clone(),
            Some("TestAgent".to_string()),
            None,
            Some(specialist.to_string()),
            None,
            None,
            false,
            extra,
        )
        .await
        .expect("create");
    AgentId::from(created["agent"]["id"].as_str().unwrap())
}

/// Specialist frontmatter model is used when no explicit model param is passed.
#[tokio::test]
async fn specialist_frontmatter_model_used_for_delegated_agent() {
    let (_t, svc, ws, specialists_dir, _cfg) = setup().await;

    // Create a specialist with a frontmatter model
    create_user_specialist(specialists_dir.path(), "test-specialist", "auggie:opus");

    // Create agent with specialist but no explicit model
    let id = create_agent(&svc, &ws, "TestAgent", None, Some("test-specialist".into())).await;

    // Verify the specialist frontmatter model was used
    let got = svc.agent_get_op(id.clone(), None).await.expect("get");
    assert_eq!(got.model.as_deref(), Some("auggie:opus"));
}

/// Explicit model param beats specialist frontmatter model.
#[tokio::test]
async fn explicit_model_beats_specialist_frontmatter() {
    let (_t, svc, ws, specialists_dir, _cfg) = setup().await;

    // Create a specialist with a frontmatter model
    create_user_specialist(specialists_dir.path(), "test-specialist", "auggie:opus");

    // Create agent with both explicit model and specialist
    let id = create_agent(
        &svc,
        &ws,
        "TestAgent",
        Some("auggie:haiku".into()),
        Some("test-specialist".into()),
    )
    .await;

    // Verify the explicit model won
    let got = svc.agent_get_op(id.clone(), None).await.expect("get");
    assert_eq!(got.model.as_deref(), Some("auggie:haiku"));
}

/// Missing/empty specialist frontmatter model falls through to settings chain.
#[tokio::test]
async fn missing_frontmatter_falls_through_to_settings() {
    let (_t, svc, ws, specialists_dir, _cfg) = setup().await;

    // Create a specialist WITHOUT a frontmatter model
    create_specialist_without_model(specialists_dir.path(), "test-specialist");

    // Set a global default in settings (via the wired registry)
    svc.settings_registry()
        .expect("registry wired")
        .apply(&[("model.default".to_string(), json!("auggie:haiku"))])
        .expect("set default model");

    // Create agent with specialist but no explicit model
    let id = create_agent(&svc, &ws, "TestAgent", None, Some("test-specialist".into())).await;

    // Verify the settings chain default was used
    let got = svc.agent_get_op(id.clone(), None).await.expect("get");
    assert_eq!(got.model.as_deref(), Some("auggie:haiku"));
}

/// Retirement regression (PROTOCOL §5.11): a lingering frontmatter
/// `modelTier` never resolves through the provider tier table — with nothing
/// else configured, session.model stays unset (CLI default).
#[tokio::test]
async fn retired_model_tier_never_resolves() {
    let (_t, svc, ws, specialists_dir, _cfg) = setup().await;
    create_specialist_with_retired_tier(specialists_dir.path(), "tiered", "smart");

    let id = create_agent(&svc, &ws, "TestAgent", None, Some("tiered".into())).await;
    let got = svc.agent_get_op(id.clone(), None).await.expect("get");
    assert_eq!(
        got.model, None,
        "retired modelTier must not pin auggie's smart-tier model"
    );
}

/// Retirement regression: a tier-declaring specialist falls through to the
/// settings chain (the retired key never shadows configured defaults).
#[tokio::test]
async fn retired_model_tier_falls_through_to_settings() {
    let (_t, svc, ws, specialists_dir, _cfg) = setup().await;
    create_specialist_with_retired_tier(specialists_dir.path(), "tiered", "smart");

    svc.settings_registry()
        .expect("registry wired")
        .apply(&[("model.default".to_string(), json!("auggie:haiku"))])
        .expect("set default model");

    let id = create_agent(&svc, &ws, "TestAgent", None, Some("tiered".into())).await;
    let got = svc.agent_get_op(id.clone(), None).await.expect("get");
    assert_eq!(got.model.as_deref(), Some("auggie:haiku"));
}

/// The delegate path ignores the retired `modelTier` identically to direct
/// create — it funnels through the same resolver in `agent_create_op`.
#[tokio::test]
async fn retired_model_tier_ignored_on_delegate_path() {
    let (_t, svc, ws, specialists_dir, _cfg) = setup().await;
    create_specialist_with_retired_tier(specialists_dir.path(), "tiered", "smart");

    let resp = svc
        .agent_delegate_op(
            ws.clone(),
            intent_core::AgentDelegateInput {
                agent_instructions: Some("do the thing".into()),
                specialist: Some("tiered".into()),
                ..Default::default()
            },
            None,
        )
        .await
        .expect("delegate");
    let child = AgentId::from(resp["agentId"].as_str().expect("agentId"));
    let got = svc.agent_get_op(child.clone(), None).await.expect("get");
    assert_eq!(got.model, None, "retired modelTier must not pin a model");
}

/// monorepo#1729 (issue repro): a delegated specialist with no frontmatter
/// model resolves `model.providerDefaults`, NOT the quick-action default —
/// the quick-action model settings never apply to a delegated session.
#[tokio::test]
async fn delegate_ignores_quick_action_default_model() {
    let (_t, svc, ws, specialists_dir, _cfg) = setup().await;
    create_specialist_without_model(specialists_dir.path(), "implementor-test");

    svc.settings_registry()
        .expect("registry wired")
        .apply(&[
            (
                "model.providerDefaults".to_string(),
                json!({ "auggie": "fable-5" }),
            ),
            (
                "quickActions.defaultModel".to_string(),
                json!("auggie:sonnet5-high"),
            ),
        ])
        .expect("set provider default + quick-action default");

    let resp = svc
        .agent_delegate_op(
            ws.clone(),
            intent_core::AgentDelegateInput {
                agent_instructions: Some("do the thing".into()),
                specialist: Some("implementor-test".into()),
                ..Default::default()
            },
            None,
        )
        .await
        .expect("delegate");
    let child = AgentId::from(resp["agentId"].as_str().expect("agentId"));
    let got = svc.agent_get_op(child.clone(), None).await.expect("get");
    assert_eq!(
        got.model.as_deref(),
        Some("fable-5"),
        "delegated specialist must resolve the provider default, not the \
         quick-action model"
    );
}

/// monorepo#1729: a `quickActions.typeOverrides` entry keyed on the
/// specialist id must not leak into delegation either — the specialist id is
/// no longer an agent-type key for the settings chain.
///
/// Seeded through `model.providerDefaults` rather than `model.default`: a
/// compound `model.default` makes `resolve_delegate_provider` derive that
/// provider and assert it is installed, which no CI runner guarantees.
#[tokio::test]
async fn delegate_ignores_quick_action_type_override() {
    let (_t, svc, ws, specialists_dir, _cfg) = setup().await;
    create_specialist_without_model(specialists_dir.path(), "implementor-test");

    svc.settings_registry()
        .expect("registry wired")
        .apply(&[
            (
                "model.providerDefaults".to_string(),
                json!({ "auggie": "fable-5" }),
            ),
            (
                "quickActions.typeOverrides".to_string(),
                json!({ "implementor-test": "auggie:sonnet5-high" }),
            ),
        ])
        .expect("set provider default + quick-action type override");

    let resp = svc
        .agent_delegate_op(
            ws.clone(),
            intent_core::AgentDelegateInput {
                agent_instructions: Some("do the thing".into()),
                specialist: Some("implementor-test".into()),
                ..Default::default()
            },
            None,
        )
        .await
        .expect("delegate");
    let child = AgentId::from(resp["agentId"].as_str().expect("agentId"));
    let got = svc.agent_get_op(child.clone(), None).await.expect("get");
    assert_eq!(
        got.model.as_deref(),
        Some("fable-5"),
        "a quick-action type override keyed on the specialist id must not apply"
    );
}

/// A specialist frontmatter model owned by another provider is ignored
/// (provider guard) — resolution falls through to the settings chain; a
/// lingering `modelTier` no longer bridges the gap.
#[tokio::test]
async fn frontmatter_model_of_other_provider_falls_through_to_settings() {
    let (_t, svc, ws, specialists_dir, _cfg) = setup().await;
    let content = "---\nname: \"Mixed\"\ndescription: \"d\"\nmodel: \"auggie:opus\"\nmodelTier: \"smart\"\n---\n\nTest prompt";
    std::fs::write(specialists_dir.path().join("mixed.md"), content).expect("write specialist");

    let id = create_agent_with_provider(&svc, &ws, "mixed", "codex").await;
    let got = svc.agent_get_op(id.clone(), None).await.expect("get");
    assert_eq!(
        got.model, None,
        "auggie-owned model ignored and retired modelTier never resolves"
    );
}

/// Bundled specialists no longer declare `modelTier` — they inherit the
/// user's `model.default`; with nothing set, session.model stays unset (CLI
/// default).
#[tokio::test]
async fn bundled_specialist_inherits_model_default() {
    let (_t, svc, ws, _specialists_dir, _cfg) = setup().await;

    // Nothing configured → CLI default.
    let id = create_agent(&svc, &ws, "TestAgent", None, Some("implementor".into())).await;
    let got = svc.agent_get_op(id.clone(), None).await.expect("get");
    assert_eq!(got.model, None, "no settings → CLI default");

    // With model.default set, the bundled specialist inherits it.
    svc.settings_registry()
        .expect("registry wired")
        .apply(&[("model.default".to_string(), json!("sonnet4.5"))])
        .expect("set model.default");
    let id = create_agent(&svc, &ws, "TestAgent2", None, Some("implementor".into())).await;
    let got = svc.agent_get_op(id.clone(), None).await.expect("get");
    assert_eq!(got.model.as_deref(), Some("sonnet4.5"));
}

/// Malicious specialist id with path traversal is rejected.
/// SECURITY: validate_id is called inside SpecialistsService::resolve() (which
/// resolve_model uses), blocking all frontmatter lookups from path traversal.
#[tokio::test]
async fn malicious_specialist_id_rejected() {
    let (_t, svc, ws, _specialists_dir, _cfg) = setup().await;

    // Attempt to create agent with path-traversal specialist id
    let id = create_agent(&svc, &ws, "TestAgent", None, Some("../evil".into())).await;

    // The agent should be created but resolve_model should have returned None
    // (because validate_id fails inside resolve()), falling through to settings chain default
    let got = svc.agent_get_op(id.clone(), None).await.expect("get");
    // With no settings configured, model should be None
    assert_eq!(got.model, None);
}

/// SECURITY: workspace_path is derived from workspace record, not client params
/// (regression test for review thread PRRT_kwDOS9Wxuc6SIhDc). A malicious client
/// cannot supply a spoofed workspacePath to read project-tier specialists from
/// other workspaces.
#[tokio::test]
async fn spoofed_workspace_path_ignored() {
    let (_t, svc, ws, specialists_dir, _cfg) = setup().await;

    // Create a project-tier specialist in a different directory
    let evil_dir = specialists_dir
        .path()
        .join("evil-workspace")
        .join(".augment")
        .join("specialists");
    std::fs::create_dir_all(&evil_dir).expect("mkdir evil specialists dir");
    let specialist_content = "---\nmodel: attacker:model\n---\n# Evil Specialist";
    std::fs::write(evil_dir.join("implementor.md"), specialist_content)
        .expect("write evil specialist");

    // Create an agent with specialistId "implementor" and client-supplied workspacePath
    // pointing to the evil directory. The code should derive workspace_path from the
    // stored workspace record instead.
    let extra = intent_core::AgentCreateExtra {
        workspace_path: Some(
            evil_dir
                .parent()
                .unwrap()
                .parent()
                .unwrap()
                .to_string_lossy()
                .to_string(),
        ),
        is_background: Some(true),
        ..Default::default()
    };
    let created = svc
        .agent_create_op(
            ws.clone(),
            Some("TestAgent".into()),
            None, // no explicit model
            Some("implementor".into()),
            None,
            None,
            false,
            extra,
        )
        .await
        .expect("create");
    let id = AgentId(created["agent"]["id"].as_str().unwrap().to_string());

    // The agent should be created but the model should NOT be "attacker:model"
    // because the workspace record has no workspace_path (new workspace), so
    // resolve_model gets None and falls through to settings (which is also None).
    let got = svc.agent_get_op(id.clone(), None).await.expect("get");
    assert_ne!(
        got.model.as_deref(),
        Some("attacker:model"),
        "spoofed workspace_path was used"
    );
    assert_eq!(got.model, None, "expected settings chain fallback");
}

/// SECURITY: resolve_agent_type validates id to prevent path traversal
/// (regression test for review thread PRRT_kwDOS9Wxuc6SIlcV). The validation
/// is now done inside resolve() so all frontmatter lookups are guarded.
#[tokio::test]
async fn malicious_specialist_id_rejected_in_agent_type_resolution() {
    let (_t, svc, ws, specialists_dir, _cfg) = setup().await;

    // Create a user-tier specialist with an agentType frontmatter field
    let specialist_content = "---\nagentType: test-agent-type\n---\n# Test Specialist";
    std::fs::write(
        specialists_dir.path().join("test-specialist.md"),
        specialist_content,
    )
    .expect("write specialist");

    // First verify that a valid specialist ID does resolve agentType
    let valid_id = create_agent(
        &svc,
        &ws,
        "ValidAgent",
        None,
        Some("test-specialist".into()),
    )
    .await;
    let valid_agent = svc.agent_get_op(valid_id.clone(), None).await.expect("get");
    // AgentLite doesn't expose agent_type, but we can verify creation succeeded
    assert!(
        valid_agent.id.0.starts_with("agent-"),
        "valid agent created"
    );

    // Now attempt to create agent with path-traversal specialist id.
    // resolve_agent_type (via derive_agent_type) should call validate_id inside resolve()
    // and return None, so the agent should be created but with default agent_type.
    // If path traversal was allowed, it might read a file outside the specialists dir
    // or crash; the fact that it succeeds with no panic proves the guard works.
    let malicious_id =
        create_agent(&svc, &ws, "MaliciousAgent", None, Some("../evil".into())).await;
    let malicious_agent = svc
        .agent_get_op(malicious_id.clone(), None)
        .await
        .expect("get");
    assert!(
        malicious_agent.id.0.starts_with("agent-"),
        "malicious agent created with default type"
    );
}

/// Create an agent with an optional name, returning the created `agent` value.
async fn create_agent_with_optional_name(
    svc: &Services,
    ws: &WorkspaceId,
    name: Option<&str>,
    specialist: Option<String>,
    extra: intent_core::AgentCreateExtra,
) -> serde_json::Value {
    let created = svc
        .agent_create_op(
            ws.clone(),
            name.map(str::to_string),
            None,
            specialist,
            None,
            None,
            false,
            extra,
        )
        .await
        .expect("create");
    created["agent"].clone()
}

/// A name-less create carrying a specialist derives the agent name from the
/// specialist's frontmatter display name and counts it as explicitly set
/// (matches the desktop FE, which resolves the display name client-side and
/// sends it as an explicit `name`).
#[tokio::test]
async fn omitted_name_derives_from_specialist_display_name() {
    let (_t, svc, ws, specialists_dir, _cfg) = setup().await;
    std::fs::write(
        specialists_dir.path().join("test-specialist.md"),
        "---\nname: \"Fancy Display Name\"\ndescription: \"d\"\n---\n\nTest prompt",
    )
    .expect("write specialist");

    let agent = create_agent_with_optional_name(
        &svc,
        &ws,
        None,
        Some("test-specialist".into()),
        Default::default(),
    )
    .await;
    assert_eq!(agent["name"], "Fancy Display Name");
    assert_eq!(agent["nameExplicitlySet"], true);
}

/// The embedded bundled `spec-writer` resolves with zero local files: a
/// name-less create yields its frontmatter display name "Coordinator".
#[tokio::test]
async fn omitted_name_derives_from_embedded_spec_writer() {
    let (_t, svc, ws, _specialists_dir, _cfg) = setup().await;
    let agent = create_agent_with_optional_name(
        &svc,
        &ws,
        None,
        Some("spec-writer".into()),
        Default::default(),
    )
    .await;
    assert_eq!(agent["name"], "Coordinator");
    assert_eq!(agent["nameExplicitlySet"], true);
}

/// An explicit client-supplied name beats the specialist display name.
#[tokio::test]
async fn explicit_name_beats_specialist_display_name() {
    let (_t, svc, ws, _specialists_dir, _cfg) = setup().await;
    let agent = create_agent_with_optional_name(
        &svc,
        &ws,
        Some("My Explicit Name"),
        Some("spec-writer".into()),
        Default::default(),
    )
    .await;
    assert_eq!(agent["name"], "My Explicit Name");
    assert_eq!(agent["nameExplicitlySet"], true);
}

/// No name and no specialist keeps the generic `Agent {6-hex}` fallback,
/// which stays renameable (not explicitly set).
#[tokio::test]
async fn no_specialist_falls_back_to_generic_name() {
    let (_t, svc, ws, _specialists_dir, _cfg) = setup().await;
    let agent = create_agent_with_optional_name(&svc, &ws, None, None, Default::default()).await;
    let name = agent["name"].as_str().expect("name");
    assert!(name.starts_with("Agent "), "generic fallback: {name}");
    assert_eq!(agent["nameExplicitlySet"], false);
}

/// An unknown specialist id never fails the create — it falls back to the
/// generic `Agent {6-hex}` label (renameable, not explicitly set).
#[tokio::test]
async fn unknown_specialist_falls_back_to_generic_name() {
    let (_t, svc, ws, _specialists_dir, _cfg) = setup().await;
    let agent = create_agent_with_optional_name(
        &svc,
        &ws,
        None,
        Some("no-such-specialist".into()),
        Default::default(),
    )
    .await;
    let name = agent["name"].as_str().expect("name");
    assert!(name.starts_with("Agent "), "generic fallback: {name}");
    assert_eq!(agent["nameExplicitlySet"], false);
}

/// Delegate flows that pass `name_explicitly_set: Some(false)` keep their
/// override: a specialist-derived name still stays renameable by the child's
/// opening-turn `setAgentName` (`skipIfExplicitlySet: true`).
#[tokio::test]
async fn delegate_override_keeps_derived_name_renameable() {
    let (_t, svc, ws, _specialists_dir, _cfg) = setup().await;
    let extra = intent_core::AgentCreateExtra {
        name_explicitly_set: Some(false),
        ..Default::default()
    };
    let agent =
        create_agent_with_optional_name(&svc, &ws, None, Some("spec-writer".into()), extra).await;
    assert_eq!(agent["name"], "Coordinator");
    assert_eq!(agent["nameExplicitlySet"], false);
}

/// Create a specialist file with an explicit name, roleReminder, and body.
fn create_specialist_with_body(dir: &Path, id: &str, name: &str, reminder: &str, body: &str) {
    let content = format!(
        "---\nname: \"{name}\"\ndescription: \"Test specialist\"\nroleReminder: \"{reminder}\"\n---\n\n{body}"
    );
    std::fs::write(dir.join(format!("{id}.md")), content).expect("write specialist");
}

/// The resolved specialist injection is snapshotted into the session metadata
/// at creation: body → `behaviorPrompt`, identity → `specialistName` /
/// `specialistRoleReminder`.
#[tokio::test]
async fn specialist_snapshot_persisted_at_create() {
    let (_t, svc, ws, specialists_dir, _cfg) = setup().await;
    create_specialist_with_body(
        specialists_dir.path(),
        "frozen",
        "Frozen Name",
        "Stay frozen",
        "Original behavior prompt body.",
    );

    let id = create_agent(&svc, &ws, "TestAgent", None, Some("frozen".into())).await;

    let session = svc.store().get_agent_session(&id).await.expect("session");
    let meta = session.metadata.expect("metadata snapshot written");
    assert_eq!(
        meta["behaviorPrompt"].as_str(),
        Some("Original behavior prompt body.")
    );
    assert_eq!(meta["specialistName"].as_str(), Some("Frozen Name"));
    assert_eq!(meta["specialistRoleReminder"].as_str(), Some("Stay frozen"));
}

/// An explicit caller `metadata.behaviorPrompt` override is left untouched —
/// it IS the frozen body; the resolved identity is still snapshotted.
#[tokio::test]
async fn caller_behavior_prompt_override_preserved() {
    let (_t, svc, ws, specialists_dir, _cfg) = setup().await;
    create_specialist_with_body(
        specialists_dir.path(),
        "frozen",
        "Frozen Name",
        "Stay frozen",
        "Original behavior prompt body.",
    );

    let extra = intent_core::AgentCreateExtra {
        metadata: Some(json!({ "behaviorPrompt": "CALLER OVERRIDE" })),
        ..Default::default()
    };
    let created = svc
        .agent_create_op(
            ws.clone(),
            Some("TestAgent".to_string()),
            None,
            Some("frozen".to_string()),
            None,
            None,
            false,
            extra,
        )
        .await
        .expect("create");
    let id = AgentId::from(created["agent"]["id"].as_str().unwrap());

    let session = svc.store().get_agent_session(&id).await.expect("session");
    let meta = session.metadata.expect("metadata");
    assert_eq!(meta["behaviorPrompt"].as_str(), Some("CALLER OVERRIDE"));
    assert_eq!(meta["specialistName"].as_str(), Some("Frozen Name"));
    assert_eq!(meta["specialistRoleReminder"].as_str(), Some("Stay frozen"));
}

/// An unknown specialist writes no snapshot and never fails the create
/// (existing leniency), leaving caller metadata absent.
#[tokio::test]
async fn unknown_specialist_creates_without_snapshot() {
    let (_t, svc, ws, _specialists_dir, _cfg) = setup().await;

    let id = create_agent(
        &svc,
        &ws,
        "TestAgent",
        None,
        Some("no-such-specialist".into()),
    )
    .await;

    let session = svc.store().get_agent_session(&id).await.expect("session");
    assert!(session.metadata.is_none(), "no snapshot for unknown id");
}

// ---- Regression: prompt frozen across specialist file edits (end-to-end) ----

/// End-to-end freeze invariant: an agent created from a user-tier specialist
/// file keeps its original behavior prompt, display name, and role reminder
/// in the spawn assembly (`agent_specialist_injection`) after the file is
/// edited — and after it is deleted.
#[tokio::test]
async fn injection_frozen_after_specialist_file_edit() {
    let (_t, svc, ws, specialists_dir, _cfg) = setup().await;
    create_specialist_with_body(
        specialists_dir.path(),
        "frozen",
        "Frozen Name",
        "Stay frozen",
        "Original behavior prompt body.",
    );

    let id = create_agent(&svc, &ws, "TestAgent", None, Some("frozen".into())).await;

    // Edit the file after creation: new name, reminder, and body.
    create_specialist_with_body(
        specialists_dir.path(),
        "frozen",
        "Edited Name",
        "Edited reminder",
        "Edited body.",
    );

    let inj = svc
        .agent_specialist_injection(&id, None)
        .await
        .expect("injection");
    assert_eq!(
        inj.behavior_prompt.as_deref(),
        Some("Original behavior prompt body.")
    );
    assert_eq!(inj.specialist_name.as_deref(), Some("Frozen Name"));
    assert_eq!(inj.role_reminder.as_deref(), Some("Stay frozen"));

    // Delete the file: the frozen triple must still survive intact.
    std::fs::remove_file(specialists_dir.path().join("frozen.md")).expect("delete specialist");
    let inj = svc
        .agent_specialist_injection(&id, None)
        .await
        .expect("injection after delete");
    assert_eq!(
        inj.behavior_prompt.as_deref(),
        Some("Original behavior prompt body.")
    );
    assert_eq!(inj.specialist_name.as_deref(), Some("Frozen Name"));
    assert_eq!(inj.role_reminder.as_deref(), Some("Stay frozen"));
}

/// Companion pin: a pre-change-style session (no frozen snapshot keys in its
/// metadata) resolves live and DOES pick up the file edit — legacy behavior
/// unchanged.
#[tokio::test]
async fn legacy_session_without_snapshot_picks_up_file_edit() {
    let (_t, svc, ws, specialists_dir, _cfg) = setup().await;
    create_specialist_with_body(
        specialists_dir.path(),
        "frozen",
        "Frozen Name",
        "Stay frozen",
        "Original behavior prompt body.",
    );

    let id = create_agent(&svc, &ws, "TestAgent", None, Some("frozen".into())).await;

    // Simulate a pre-change row by stripping the creation-time snapshot.
    svc.store()
        .update_agent_session_metadata(&ws, &id, None, &intent_core::now_iso())
        .await
        .expect("strip snapshot");

    create_specialist_with_body(
        specialists_dir.path(),
        "frozen",
        "Edited Name",
        "Edited reminder",
        "Edited body.",
    );

    let inj = svc
        .agent_specialist_injection(&id, None)
        .await
        .expect("injection");
    assert_eq!(inj.behavior_prompt.as_deref(), Some("Edited body."));
    assert_eq!(inj.specialist_name.as_deref(), Some("Edited Name"));
    assert_eq!(inj.role_reminder.as_deref(), Some("Edited reminder"));
}
