//! Unit tests for server pairing fast-path: classify/handle for server.pairingInfo and
//! server.rotateToken, local-only gating. INTENTD_AUTH_TOKEN rejection is tested in
//! e2e_wss_server_pairing.rs (env var interaction makes it unsuitable for unit testing).

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
        Box::pin(async move {
            PairingSnapshot {
                port,
                bind_address: None,
            }
        })
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

    // RAII guard to restore INTENTD_AUTH_TOKEN on drop (even if test panics).
    struct EnvGuard(Option<String>);
    impl Drop for EnvGuard {
        fn drop(&mut self) {
            match self.0.as_ref() {
                Some(val) => env::set_var("INTENTD_AUTH_TOKEN", val),
                None => env::remove_var("INTENTD_AUTH_TOKEN"),
            }
        }
    }

    // Temporarily clear INTENTD_AUTH_TOKEN to ensure rotation succeeds in this test
    let _guard = EnvGuard(env::var("INTENTD_AUTH_TOKEN").ok());
    env::remove_var("INTENTD_AUTH_TOKEN");

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
    // _guard drops here, restoring the original env var value
}

#[test]
fn pairing_hosts_specific_bind_advertises_only_that_address() {
    // A listener bound to one address (loopback or a single interface) is
    // reachable only there (monorepo#2900).
    for addr in ["127.0.0.1", "192.168.1.23"] {
        let snapshot = PairingSnapshot {
            port: Some(5181),
            bind_address: Some(addr.parse().unwrap()),
        };
        assert_eq!(pairing_hosts(&snapshot), vec![addr.to_string()]);
    }
}

#[test]
fn pairing_hosts_unspecified_or_unknown_bind_enumerates_local_ips() {
    // 0.0.0.0 listens everywhere; an unknown bind (older snapshot source)
    // keeps the historical enumeration behavior. Both fall back to
    // collect_local_ips — assert equality rather than contents, since the
    // machine's interfaces vary.
    let unspecified = PairingSnapshot {
        port: Some(5181),
        bind_address: Some("0.0.0.0".parse().unwrap()),
    };
    let unknown = PairingSnapshot {
        port: Some(5181),
        bind_address: None,
    };
    assert_eq!(pairing_hosts(&unspecified), collect_local_ips());
    assert_eq!(pairing_hosts(&unknown), collect_local_ips());
}

#[test]
fn pairing_hosts_v6_unspecified_bind_includes_v4_and_specific_v6_stays_exact() {
    // `::` accepts native IPv6 plus v4-mapped IPv4: the advertised set starts
    // with the v4 enumeration (v6 additions vary by machine, so assert the
    // prefix). A SPECIFIC v6 bind still advertises exactly that address.
    let v6_unspecified = PairingSnapshot {
        port: Some(5181),
        bind_address: Some("::".parse().unwrap()),
    };
    let hosts = pairing_hosts(&v6_unspecified);
    let v4 = collect_local_ips();
    assert_eq!(&hosts[..v4.len()], &v4[..]);
    for extra in &hosts[v4.len()..] {
        let ip: std::net::IpAddr = extra.parse().expect("v6 host entries parse");
        assert!(
            ip.is_ipv6(),
            "extra hosts beyond the v4 set are v6: {extra}"
        );
    }

    let v6_specific = PairingSnapshot {
        port: Some(5181),
        bind_address: Some("2001:db8::7".parse().unwrap()),
    };
    assert_eq!(pairing_hosts(&v6_specific), vec!["2001:db8::7".to_string()]);
}

#[test]
fn collect_bind_interfaces_loopback_first_no_duplicates() {
    let ifaces = collect_bind_interfaces();
    // Machines without any interface (rare CI sandboxes) yield an empty list;
    // everything else must lead with loopback and never repeat an address.
    if let Some((_, first)) = ifaces.first() {
        if ifaces.iter().any(|(_, ip)| ip.is_loopback()) {
            assert!(first.is_loopback(), "loopback sorts first: {ifaces:?}");
        }
    }
    let mut seen = std::collections::HashSet::new();
    for (_, ip) in &ifaces {
        assert!(seen.insert(*ip), "duplicate address in {ifaces:?}");
    }
}
