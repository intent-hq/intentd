//! E2E tests exercising intent-core config and intent-transport auth paths.
//!
//! Tests token generation/rotation and config resolution WITHOUT spawning a daemon.

mod common;

use intent_core::Config;
use intent_transport::{generate_token, get_or_create_token, AsyncTokenStore, FileTokenStore};
use std::sync::{Arc, Mutex};

static ENV_LOCK: Mutex<()> = Mutex::new(());

/// Verify token generation and persistence via `AsyncTokenStore`.
#[tokio::test]
async fn token_generation_and_persistence() {
    let tmp_dir = std::env::temp_dir().join(format!("intentd-token-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&tmp_dir).expect("create temp dir");
    let secrets_file = tmp_dir.join("secrets.json");

    let store_async = {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var("INTENTD_SECRETS_FILE", &secrets_file);
        let store = AsyncTokenStore::new(Arc::new(FileTokenStore::default()));
        std::env::remove_var("INTENTD_SECRETS_FILE");
        drop(_guard);
        store
    };

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

    let _ = std::fs::remove_dir_all(&tmp_dir);
}

/// Verify token rotation (generate new, replace old).
#[tokio::test]
async fn token_rotation_replaces_old() {
    let tmp_dir = std::env::temp_dir().join(format!("intentd-token-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&tmp_dir).expect("create temp dir");
    let secrets_file = tmp_dir.join("secrets.json");

    let store_async = {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var("INTENTD_SECRETS_FILE", &secrets_file);
        let store = AsyncTokenStore::new(Arc::new(FileTokenStore::default()));
        std::env::remove_var("INTENTD_SECRETS_FILE");
        drop(_guard);
        store
    };

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

    let _ = std::fs::remove_dir_all(&tmp_dir);
}

/// Verify `Config::resolve` correctly sets daemon paths.
#[tokio::test]
async fn config_paths_include_daemon_files() {
    let tmp_dir = std::env::temp_dir().join(format!("intentd-cfg-{}", uuid::Uuid::new_v4()));

    let config = {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var("INTENTD_DATA_DIR", &tmp_dir);
        let cfg = Config::resolve().expect("resolve config");
        std::env::remove_var("INTENTD_DATA_DIR");
        drop(_guard);
        cfg
    };

    assert_eq!(config.data_dir, tmp_dir);
    assert_eq!(config.db_path, tmp_dir.join("intentd.db"));
    assert_eq!(config.socket_path, tmp_dir.join("intentd.sock"));
    assert_eq!(config.pid_path, tmp_dir.join("intentd.pid"));

    let _ = std::fs::remove_dir_all(&tmp_dir);
}
