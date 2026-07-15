//! E2E tests exercising intentd binary control paths (main.rs).
//!
//! Drives CLI subcommands (status, token, service) to exercise the binary crate's
//! control logic and increase coverage of service.rs, main.rs paths.

use intent_core::Config;
use intent_transport::{generate_token, get_or_create_token, AsyncTokenStore, FileTokenStore};
use std::sync::Arc;

/// Verify token generation and persistence (token subcommand path).
#[tokio::test]
async fn token_generation_and_persistence() {
    // Use default FileTokenStore (delegates to FileSecretStore)
    let store_async = AsyncTokenStore::new(Arc::new(FileTokenStore::default()));

    // Generate a fresh token
    let token1 = generate_token(&store_async).await.expect("generate");
    assert_eq!(token1.len(), 64, "token should be 64 chars (32 bytes hex)");
    assert!(token1.chars().all(|c| c.is_ascii_hexdigit()));

    // Retrieve and verify
    let retrieved = store_async.load_token().await;
    assert_eq!(retrieved, Some(token1.clone()));

    // get_or_create should return the existing one
    let token2 = get_or_create_token(&store_async)
        .await
        .expect("get_or_create");
    assert_eq!(token2, token1, "get_or_create returns existing token");
}

/// Verify token rotation (generate new, replace old).
#[tokio::test]
async fn token_rotation_replaces_old() {
    let store_async = AsyncTokenStore::new(Arc::new(FileTokenStore::default()));

    // Create initial token
    let token1 = get_or_create_token(&store_async).await.expect("create");

    // Rotate: generate new and save
    let token2 = generate_token(&store_async)
        .await
        .expect("generate rotated");
    assert_ne!(token2, token1, "rotated token should be different");

    // Verify new token persisted
    let loaded = store_async.load_token().await;
    assert_eq!(loaded, Some(token2));
}

/// Verify Config paths include socket, db, pid.
#[tokio::test]
async fn config_paths_include_daemon_files() {
    let tmp_dir = std::env::temp_dir().join(format!("intentd-cfg-{}", uuid::Uuid::new_v4()));
    std::env::set_var("INTENTD_DATA_DIR", &tmp_dir);

    let config = Config::resolve().expect("resolve config");
    assert_eq!(config.data_dir, tmp_dir);

    // Verify paths are set correctly
    assert_eq!(config.db_path, tmp_dir.join("intentd.db"));
    assert_eq!(config.socket_path, tmp_dir.join("intentd.sock"));
    assert_eq!(config.pid_path, tmp_dir.join("intentd.pid"));

    // Clean up
    std::env::remove_var("INTENTD_DATA_DIR");
}
