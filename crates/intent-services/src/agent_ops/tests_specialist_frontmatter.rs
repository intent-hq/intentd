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
#[tokio::test]
async fn malicious_specialist_id_rejected() {
    let (_t, svc, ws, _specialists_dir) = setup().await;

    // Attempt to create agent with path-traversal specialist id
    let id = create_agent(&svc, &ws, "TestAgent", None, Some("../evil".into())).await;

    // The agent should be created but resolve_model should have returned None
    // (because validate_id fails), falling through to settings chain default
    let got = svc.agent_get_op(id.clone(), None).await.expect("get");
    // With no settings configured, model should be None
    assert_eq!(got.model, None);
}
