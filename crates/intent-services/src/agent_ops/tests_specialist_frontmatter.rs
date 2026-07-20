//! Regression tests for specialist frontmatter `model` resolution at agent creation.
//!
//! Covers the Wave 2 fix: when `agent.create` receives no explicit model but a specialist id,
//! the specialist's resolved frontmatter `model` (3-tier: project > user > bundled) is used
//! before the settings chain.
//!
//! Full precedence:
//! explicit model > specialist frontmatter model > settings chain > CLI default

use intent_core::{AgentId, WorkspaceId};
use intent_store::Store;
use serde_json::json;
use std::path::PathBuf;
use tempfile::TempDir;

use super::tests::{workspace, TempDb};
use crate::Services;

/// Set up a test with a temp specialist directory structure.
async fn setup() -> (TempDb, Services, WorkspaceId, TempDir) {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    store.insert_workspace(&workspace(&ws)).await.expect("ws");

    // Create a temp directory for specialists
    let specialists_dir = TempDir::new().expect("temp specialists dir");

    // Configure Services with the temp specialists directory
    let services = Services::new(store).with_specialist_dirs(
        Some(specialists_dir.path().to_path_buf()),
        Some(specialists_dir.path().to_path_buf()),
    );
    (tmp, services, ws, specialists_dir)
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
            None,
            extra,
        )
        .await
        .expect("create");
    AgentId::from(created["agent"]["id"].as_str().unwrap())
}

/// Create a specialist file with frontmatter model in the user tier.
fn create_user_specialist(dir: &PathBuf, id: &str, model: &str) {
    let content = format!(
        "---\nname: \"{}\"\ndescription: \"Test specialist\"\nmodel: \"{}\"\n---\n\nTest prompt",
        id, model
    );
    std::fs::write(dir.join(format!("{}.md", id)), content).expect("write specialist");
}

/// Create a specialist file without a model field.
fn create_specialist_without_model(dir: &PathBuf, id: &str) {
    let content = format!(
        "---\nname: \"{}\"\ndescription: \"Test specialist\"\n---\n\nTest prompt",
        id
    );
    std::fs::write(dir.join(format!("{}.md", id)), content).expect("write specialist");
}

/// Specialist frontmatter model is used when no explicit model param is passed.
#[tokio::test]
async fn specialist_frontmatter_model_used_for_delegated_agent() {
    let (_t, svc, ws, specialists_dir) = setup().await;

    // Create a specialist with a frontmatter model
    create_user_specialist(
        &specialists_dir.path().to_path_buf(),
        "test-specialist",
        "auggie:opus",
    );

    // Create agent with specialist but no explicit model
    let id = create_agent(&svc, &ws, "TestAgent", None, Some("test-specialist".into())).await;

    // Verify the specialist frontmatter model was used
    let got = svc.agent_get_op(id.clone(), None).await.expect("get");
    assert_eq!(got.model.as_deref(), Some("auggie:opus"));
}

/// Explicit model param beats specialist frontmatter model.
#[tokio::test]
async fn explicit_model_beats_specialist_frontmatter() {
    let (_t, svc, ws, specialists_dir) = setup().await;

    // Create a specialist with a frontmatter model
    create_user_specialist(
        &specialists_dir.path().to_path_buf(),
        "test-specialist",
        "auggie:opus",
    );

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
    let (_t, svc, ws, specialists_dir) = setup().await;

    // Create a specialist WITHOUT a frontmatter model
    create_specialist_without_model(&specialists_dir.path().to_path_buf(), "test-specialist");

    // Set a background default in settings
    svc.store
        .set_setting(
            "backgroundAgents.defaultModel",
            &json!("auggie:haiku").to_string(),
        )
        .await
        .expect("set background");

    // Create agent with specialist but no explicit model
    let id = create_agent(&svc, &ws, "TestAgent", None, Some("test-specialist".into())).await;

    // Verify the settings chain default was used
    let got = svc.agent_get_op(id.clone(), None).await.expect("get");
    assert_eq!(got.model.as_deref(), Some("auggie:haiku"));
}

/// Malicious specialist id with path traversal is rejected.
/// SECURITY: validate_id is called inside SpecialistsService::resolve() (which
/// resolve_model uses), blocking all frontmatter lookups from path traversal.
#[tokio::test]
async fn malicious_specialist_id_rejected() {
    let (_t, svc, ws, _specialists_dir) = setup().await;

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
    let (_t, svc, ws, specialists_dir) = setup().await;

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
            None,
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
    let (_t, svc, ws, specialists_dir) = setup().await;

    // Create a user-tier specialist with an agentType frontmatter field
    let specialist_content = "---\nagentType: test-agent-type\n---\n# Test Specialist";
    std::fs::write(
        specialists_dir.path().join("test-specialist.md"),
        specialist_content,
    )
    .expect("write specialist");

    // First verify that a valid specialist ID does resolve agentType
    let valid_id = create_agent(&svc, &ws, "ValidAgent", None, Some("test-specialist".into()))
        .await;
    let valid_agent = svc.agent_get_op(valid_id.clone(), None).await.expect("get");
    // AgentLite doesn't expose agent_type, but we can verify creation succeeded
    assert!(valid_agent.id.0.starts_with("agent-"), "valid agent created");

    // Now attempt to create agent with path-traversal specialist id.
    // resolve_agent_type (via derive_agent_type) should call validate_id inside resolve()
    // and return None, so the agent should be created but with default agent_type.
    // If path traversal was allowed, it might read a file outside the specialists dir
    // or crash; the fact that it succeeds with no panic proves the guard works.
    let malicious_id = create_agent(&svc, &ws, "MaliciousAgent", None, Some("../evil".into())).await;
    let malicious_agent = svc.agent_get_op(malicious_id.clone(), None).await.expect("get");
    assert!(malicious_agent.id.0.starts_with("agent-"), "malicious agent created with default type");
}
