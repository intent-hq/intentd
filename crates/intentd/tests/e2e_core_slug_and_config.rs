//! E2E tests exercising intent-core slug generation and config paths.
//!
//! Calls Services directly to exercise slug generation (extract_local_slug,
//! generate_workspace_slug) and config parsing WITHOUT spawning a daemon.

mod common;

use intent_core::config::DEFAULT_IDLE_REAP_MINUTES;
use intent_core::{Config, WorkspaceApi, WorkspaceCreate, WorkspaceCreateInitialAgent};
use intent_services::Services;
use intent_store::Store;
use std::sync::Mutex;

static ENV_LOCK: Mutex<()> = Mutex::new(());

/// Create a workspace with an initial-agent prompt and verify that the workspace
/// ID is derived from the prompt via `extract_local_slug` ("fix auth" → "auth-fix").
#[tokio::test]
async fn workspace_id_derived_from_initial_agent_prompt() {
    let db = std::env::temp_dir().join(format!("intentd-e2e-core-{}.db", uuid::Uuid::new_v4()));
    let ws_root = std::env::temp_dir().join(format!("itd-e2e-ws-{}", uuid::Uuid::new_v4()));
    let store = Store::open(&db).await.expect("open store");
    let services = Services::new(store.clone()).with_workspaces_root(ws_root.clone());

    // Workspace create with an initialAgent prompt that should extract to "auth-fix"
    let result = services
        .create_workspace(
            WorkspaceCreate {
                title: None,
                initial_agent: Some(WorkspaceCreateInitialAgent {
                    prompt: Some("fix the auth flow".to_string()),
                    name: Some("Test Agent".to_string()),
                    ..Default::default()
                }),
                ..Default::default()
            },
            Some(format!("idem-{}", uuid::Uuid::new_v4())),
        )
        .await
        .expect("create workspace");

    let ws_id = result.workspace.id.0.as_str();
    assert_eq!(
        ws_id, "auth-fix",
        "workspace id should be derived from prompt via extract_local_slug"
    );

    // Clean up (drop store/services before removing SQLite files)
    drop(services);
    drop(store);
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", db.display()));
    }
    let _ = std::fs::remove_dir_all(&ws_root);
}

/// Create a workspace with no prompt and verify the ID is a random slug
/// (adjective-animal from generate_workspace_slug).
#[tokio::test]
async fn workspace_id_random_slug_when_no_prompt() {
    let db = std::env::temp_dir().join(format!("intentd-e2e-core-{}.db", uuid::Uuid::new_v4()));
    let ws_root = std::env::temp_dir().join(format!("itd-e2e-ws-{}", uuid::Uuid::new_v4()));
    let store = Store::open(&db).await.expect("open store");
    let services = Services::new(store.clone()).with_workspaces_root(ws_root.clone());

    // Workspace create with no prompt → should generate random slug
    let result = services
        .create_workspace(
            WorkspaceCreate {
                title: None,
                initial_agent: None,
                ..Default::default()
            },
            Some(format!("idem-{}", uuid::Uuid::new_v4())),
        )
        .await
        .expect("create workspace");

    let ws_id = result.workspace.id.0.as_str();
    // Verify it's a valid slug shape: word-word
    let parts: Vec<&str> = ws_id.split('-').collect();
    assert_eq!(parts.len(), 2, "random slug should be word-word: {ws_id}");
    assert!(
        intent_core::slug::is_workspace_slug(ws_id),
        "random slug should be recognized as a workspace slug: {ws_id}"
    );

    // Clean up (drop store/services before removing SQLite files)
    drop(services);
    drop(store);
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", db.display()));
    }
    let _ = std::fs::remove_dir_all(&ws_root);
}

/// Verify Config::resolve parses env vars and fills defaults.
#[tokio::test]
async fn config_resolve_fills_defaults() {
    let tmp_dir = std::env::temp_dir().join(format!("intentd-cfg-{}", uuid::Uuid::new_v4()));
    let tmp_cfg = tmp_dir.join("nonexistent-config.toml");

    let _guard = ENV_LOCK.lock().unwrap();
    std::env::set_var("INTENTD_DATA_DIR", &tmp_dir);
    std::env::set_var("INTENTD_CONFIG", &tmp_cfg);

    let config = Config::resolve().expect("resolve config");
    assert_eq!(config.data_dir, tmp_dir);
    assert!(config.db_path.to_string_lossy().contains("intentd.db"));
    assert!(config
        .socket_path
        .to_string_lossy()
        .contains("intentd.sock"));
    assert_eq!(config.idle_reap_minutes, DEFAULT_IDLE_REAP_MINUTES);

    std::env::remove_var("INTENTD_DATA_DIR");
    std::env::remove_var("INTENTD_CONFIG");
    drop(_guard);
    let _ = std::fs::remove_dir_all(&tmp_dir);
}

/// Verify Config::resolve reads idle_reap_minutes from config.toml.
#[tokio::test]
async fn config_resolve_reads_idle_reap_from_file() {
    let tmp_dir = std::env::temp_dir().join(format!("intentd-cfg-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&tmp_dir).expect("create temp dir");

    let cfg_path = tmp_dir.join("config.toml");
    std::fs::write(
        &cfg_path,
        r"
[agents]
idleReapMinutes = 50
",
    )
    .expect("write config");

    let _guard = ENV_LOCK.lock().unwrap();
    std::env::set_var("INTENTD_DATA_DIR", &tmp_dir);
    std::env::set_var("INTENTD_CONFIG", &cfg_path);

    let config = Config::resolve().expect("resolve config");
    assert_eq!(config.idle_reap_minutes, 50);

    std::env::remove_var("INTENTD_DATA_DIR");
    std::env::remove_var("INTENTD_CONFIG");
    drop(_guard);
    let _ = std::fs::remove_dir_all(&tmp_dir);
}
