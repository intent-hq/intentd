//! Unit tests for server pairing fast-path: classify/handle for server.pairingInfo and
//! server.rotateToken, local-only gating. `INTENTD_AUTH_TOKEN` rejection is tested in
//! `e2e_wss_server_pairing.rs` (env var interaction makes it unsuitable for unit testing).

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
    bind_addresses: Option<Vec<std::net::IpAddr>>,
    tc_address: Option<String>,
    data_dir: PathBuf,
    token_store: crate::AsyncTokenStore,
}

impl ServerPairingInfo for MockPairingInfo {
    fn pairing_snapshot(&self) -> Pin<Box<dyn Future<Output = PairingSnapshot> + Send + '_>> {
        let port = self.port;
        let bind_addresses = self.bind_addresses.clone();
        let tc_address = self.tc_address.clone();
        Box::pin(async move {
            PairingSnapshot {
                port,
                bind_addresses,
                tc_address,
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
        bind_addresses: None,
        tc_address: None,
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
    // availableIps is always present and is exactly the bind-candidate
    // enumeration (machine-dependent contents, so compare against the source).
    assert_eq!(result["availableIps"], json!(collect_local_ips()));
    assert!(result["hostname"].is_string());
    assert!(
        !result["prettyHostname"]
            .as_str()
            .expect("prettyHostname is string")
            .is_empty(),
        "prettyHostname non-empty"
    );
    // No tunnel in the snapshot: tcAddress is ABSENT, not null.
    assert!(result.get("tcAddress").is_none());
    let _ = std::fs::remove_dir_all(&tmpdir);
}

#[tokio::test]
async fn handle_pairing_info_available_ips_ignore_loopback_bind() {
    // A loopback-only bind advertises no pairing host (localIps is empty),
    // but availableIps still lists every bind candidate — the FE's bind
    // picker needs the candidates precisely when the daemon is locked to
    // loopback. Loopback itself is never in the candidate list.
    use std::env;
    let tmpdir = env::temp_dir().join(format!(
        "intentd-test-{}-{}",
        std::process::id(),
        "pairing_info_available_ips"
    ));
    std::fs::create_dir_all(&tmpdir).unwrap();
    let store = crate::AsyncTokenStore::new(Arc::new(MemoryStore::with("test-token-abc123")));
    let provider: Arc<dyn ServerPairingInfo> = Arc::new(MockPairingInfo {
        port: Some(5181),
        bind_addresses: Some(vec!["127.0.0.1".parse().unwrap()]),
        tc_address: None,
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
    let result = &parsed["result"];
    assert_eq!(result["localIps"], json!([]));
    assert_eq!(result["availableIps"], json!(collect_local_ips()));
    let available = result["availableIps"].as_array().unwrap();
    assert!(
        available.iter().all(|v| !v
            .as_str()
            .unwrap()
            .parse::<std::net::IpAddr>()
            .unwrap()
            .is_loopback()),
        "availableIps never contains loopback: {available:?}"
    );
    let _ = std::fs::remove_dir_all(&tmpdir);
}

#[tokio::test]
async fn handle_pairing_info_includes_tc_address_when_tunnel_up() {
    use std::env;
    let tmpdir = env::temp_dir().join(format!(
        "intentd-test-{}-{}",
        std::process::id(),
        "pairing_info_tc"
    ));
    std::fs::create_dir_all(&tmpdir).unwrap();
    let store = crate::AsyncTokenStore::new(Arc::new(MemoryStore::with("test-token-abc123")));
    let provider: Arc<dyn ServerPairingInfo> = Arc::new(MockPairingInfo {
        port: Some(5181),
        bind_addresses: None,
        tc_address: Some("tc7f2a91.tailcat.net".to_string()),
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
    assert_eq!(parsed["result"]["tcAddress"], "tc7f2a91.tailcat.net");
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
        bind_addresses: None,
        tc_address: None,
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
        bind_addresses: None,
        tc_address: None,
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
    // A listener bound to one address is reachable only there
    // (monorepo#2900).
    let snapshot = PairingSnapshot {
        port: Some(5181),
        bind_addresses: Some(vec!["192.168.1.23".parse().unwrap()]),
        tc_address: None,
    };
    assert_eq!(pairing_hosts(&snapshot), vec!["192.168.1.23".to_string()]);
}

#[test]
fn pairing_hosts_never_advertise_loopback() {
    // Pairing hosts feed remote clients (QR payload, keychain sync), and
    // loopback is not dialable from another device: a bound loopback entry
    // is dropped — a mixed bind keeps only the non-loopback entries, and a
    // loopback-only bind advertises nothing.
    let mixed = PairingSnapshot {
        port: Some(5181),
        bind_addresses: Some(vec![
            "127.0.0.1".parse().unwrap(),
            "192.168.1.23".parse().unwrap(),
        ]),
        tc_address: None,
    };
    assert_eq!(pairing_hosts(&mixed), vec!["192.168.1.23".to_string()]);
    for lo in ["127.0.0.1", "::1"] {
        let loopback_only = PairingSnapshot {
            port: Some(5181),
            bind_addresses: Some(vec![lo.parse().unwrap()]),
            tc_address: None,
        };
        assert_eq!(
            pairing_hosts(&loopback_only),
            Vec::<String>::new(),
            "loopback-only bind ({lo}) advertises nothing"
        );
    }
}

#[test]
fn pairing_hosts_multi_bind_advertises_every_address() {
    // A list bind (monorepo#3314) is reachable on exactly its entries.
    let snapshot = PairingSnapshot {
        port: Some(5181),
        bind_addresses: Some(vec![
            "192.168.1.23".parse().unwrap(),
            "100.64.0.3".parse().unwrap(),
        ]),
        tc_address: None,
    };
    assert_eq!(
        pairing_hosts(&snapshot),
        vec!["192.168.1.23".to_string(), "100.64.0.3".to_string()]
    );
}

#[test]
fn pairing_hosts_unspecified_or_unknown_bind_enumerates_local_ips() {
    // 0.0.0.0 listens everywhere; an unknown bind (older snapshot source)
    // keeps the historical enumeration behavior. Both fall back to
    // collect_local_ips — assert equality rather than contents, since the
    // machine's interfaces vary.
    let unspecified = PairingSnapshot {
        port: Some(5181),
        bind_addresses: Some(vec!["0.0.0.0".parse().unwrap()]),
        tc_address: None,
    };
    let unknown = PairingSnapshot {
        port: Some(5181),
        bind_addresses: None,
        tc_address: None,
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
        bind_addresses: Some(vec!["::".parse().unwrap()]),
        tc_address: None,
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
        bind_addresses: Some(vec!["2001:db8::7".parse().unwrap()]),
        tc_address: None,
    };
    assert_eq!(pairing_hosts(&v6_specific), vec!["2001:db8::7".to_string()]);
}

#[test]
fn advertised_hosts_filters_by_bind_set_deterministically() {
    // Pure core shared by pairing.getInfo and system.status localIps: with
    // fixed enumerations, every bind shape maps to a deterministic host set.
    let bind = |s: &str| s.parse::<std::net::IpAddr>().unwrap();
    let v4 = vec!["192.168.1.23".to_string(), "100.64.0.3".to_string()];
    let v6 = vec!["2001:db8::7".to_string()];

    // Specific binds (loopback included) advertise exactly those addresses —
    // never the full enumeration.
    assert_eq!(
        advertised_hosts(Some(&[bind("127.0.0.1")]), &v4, &v6),
        vec!["127.0.0.1".to_string()]
    );
    assert_eq!(
        advertised_hosts(Some(&[bind("192.168.1.23"), bind("100.64.0.3")]), &v4, &v6),
        vec!["192.168.1.23".to_string(), "100.64.0.3".to_string()]
    );

    // 0.0.0.0 → the v4 enumeration only; :: → v4 + v6.
    assert_eq!(advertised_hosts(Some(&[bind("0.0.0.0")]), &v4, &v6), v4);
    assert_eq!(
        advertised_hosts(Some(&[bind("::")]), &v4, &v6),
        vec![
            "192.168.1.23".to_string(),
            "100.64.0.3".to_string(),
            "2001:db8::7".to_string()
        ]
    );

    // Unknown bind set (None) and an empty bind list keep the historical
    // v4 enumeration fallback.
    assert_eq!(advertised_hosts(None, &v4, &v6), v4);
    assert_eq!(advertised_hosts(Some(&[]), &v4, &v6), v4);
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
