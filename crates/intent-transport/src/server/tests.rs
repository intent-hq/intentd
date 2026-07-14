//! Unit tests for server pairing fast-path: classify/handle for server.pairingInfo and
//! server.rotateToken, local-only gating, INTENTD_AUTH_TOKEN handling.

use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

use serde_json::{json, Value};

use super::*;
use crate::auth::TokenStore;

/// In-memory token store for tests.
#[derive(Default, Clone)]
struct MemoryStore {
    value: Arc<std::sync::Mutex<Option<String>>>,
}

impl MemoryStore {
    fn with(token: &str) -> Self {
        Self {
            value: Arc::new(std::sync::Mutex::new(Some(token.to_string()))),
        }
    }
}

impl TokenStore for MemoryStore {
    fn load_token(&self) -> Option<String> {
        self.value.lock().unwrap().clone()
    }
    fn store_token(&self, token: &str) -> intent_core::Result<()> {
        *self.value.lock().unwrap() = Some(token.to_string());
        Ok(())
    }
}

/// Mock pairing info provider for tests.
struct MockPairingInfo {
    port: Option<u16>,
    data_dir: PathBuf,
    token_store: crate::AsyncTokenStore,
}

impl ServerPairingInfo for MockPairingInfo {
    fn pairing_snapshot(&self) -> Pin<Box<dyn Future<Output = PairingSnapshot> + Send + '_>> {
        let port = self.port;
        Box::pin(async move { PairingSnapshot { port } })
    }
    fn data_dir(&self) -> &std::path::Path {
        &self.data_dir
    }
    fn token_store(&self) -> &crate::AsyncTokenStore {
        &self.token_store
    }
}

#[test]
fn classify_pairing_info() {
    let req = json!({"jsonrpc": "2.0", "method": "server.pairingInfo", "id": 1});
    let r = classify(&req).unwrap();
    assert!(matches!(r.method, ServerMethod::PairingInfo));
    assert!(r.id_present);
    assert_eq!(r.id_echo, json!(1));
}

#[test]
fn classify_rotate_token() {
    let req = json!({"jsonrpc": "2.0", "method": "server.rotateToken", "id": "abc"});
    let r = classify(&req).unwrap();
    assert!(matches!(r.method, ServerMethod::RotateToken));
    assert!(r.id_present);
    assert_eq!(r.id_echo, json!("abc"));
}

#[test]
fn classify_notification() {
    let req = json!({"jsonrpc": "2.0", "method": "server.pairingInfo"});
    let r = classify(&req).unwrap();
    assert!(!r.id_present);
}

#[test]
fn classify_other_method() {
    let req = json!({"jsonrpc": "2.0", "method": "system.status", "id": 1});
    assert!(classify(&req).is_none());
}

#[tokio::test]
async fn handle_pairing_info_local_success() {
    use std::env;
    let tmpdir = env::temp_dir().join(format!(
        "intentd-test-{}-{}",
        std::process::id(),
        "pairing_info_local"
    ));
    std::fs::create_dir_all(&tmpdir).unwrap();
    let store = crate::AsyncTokenStore::new(Arc::new(MemoryStore::with("test-token-abc123")));
    let provider: Arc<dyn ServerPairingInfo> = Arc::new(MockPairingInfo {
        port: Some(5181),
        data_dir: tmpdir.clone(),
        token_store: store,
    });

    let req = ServerRequest {
        method: ServerMethod::PairingInfo,
        id_present: true,
        id_echo: json!(1),
    };

    let resp = handle(req, &provider, true).await.unwrap();
    let parsed: Value = serde_json::from_str(&resp).unwrap();
    assert_eq!(parsed["jsonrpc"], "2.0");
    assert_eq!(parsed["id"], 1);
    let result = &parsed["result"];
    assert_eq!(result["token"], "test-token-abc123");
    assert_eq!(result["port"], 5181);
    assert_eq!(result["path"], "/ws");
    assert!(result["certFingerprint"].is_string());
    assert!(result["localIps"].is_array());
    assert!(result["hostname"].is_string());
    let _ = std::fs::remove_dir_all(&tmpdir);
}

#[tokio::test]
async fn handle_pairing_info_remote_rejects() {
    use std::env;
    let tmpdir = env::temp_dir().join(format!(
        "intentd-test-{}-{}",
        std::process::id(),
        "pairing_info_remote"
    ));
    std::fs::create_dir_all(&tmpdir).unwrap();
    let store = crate::AsyncTokenStore::new(Arc::new(MemoryStore::default()));
    let provider: Arc<dyn ServerPairingInfo> = Arc::new(MockPairingInfo {
        port: Some(5181),
        data_dir: tmpdir.clone(),
        token_store: store,
    });

    let req = ServerRequest {
        method: ServerMethod::PairingInfo,
        id_present: true,
        id_echo: json!(1),
    };

    let resp = handle(req, &provider, false).await.unwrap();
    let parsed: Value = serde_json::from_str(&resp).unwrap();
    assert_eq!(parsed["jsonrpc"], "2.0");
    assert!(parsed["error"].is_object());
    assert_eq!(parsed["error"]["code"], -32001);
    let _ = std::fs::remove_dir_all(&tmpdir);
}

#[tokio::test]
async fn handle_rotate_token_local_success() {
    use std::env;
    let tmpdir = env::temp_dir().join(format!(
        "intentd-test-{}-{}",
        std::process::id(),
        "rotate_token_local"
    ));
    std::fs::create_dir_all(&tmpdir).unwrap();
    let store = crate::AsyncTokenStore::new(Arc::new(MemoryStore::with("old-token")));
    let provider: Arc<dyn ServerPairingInfo> = Arc::new(MockPairingInfo {
        port: Some(5181),
        data_dir: tmpdir.clone(),
        token_store: store.clone(),
    });

    let req = ServerRequest {
        method: ServerMethod::RotateToken,
        id_present: true,
        id_echo: json!(2),
    };

    let resp = handle(req, &provider, true).await.unwrap();
    let parsed: Value = serde_json::from_str(&resp).unwrap();
    assert_eq!(parsed["jsonrpc"], "2.0");
    assert_eq!(parsed["id"], 2);
    let new_token = parsed["result"]["token"].as_str().unwrap();
    assert_ne!(new_token, "old-token");
    assert_eq!(new_token.len(), 64);
    let _ = std::fs::remove_dir_all(&tmpdir);
}
