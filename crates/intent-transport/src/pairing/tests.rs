//! Unit tests for the `pairing.getInfo` fast-path: classify/handle, payload
//! URI construction, local-only gating, and the no-TCP-listener error.

use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

use serde_json::{json, Value};

use super::*;
use crate::auth::TokenStore;
use crate::server::PairingSnapshot;

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

fn provider(port: Option<u16>, dir: &str, token: &str) -> (Arc<dyn ServerPairingInfo>, PathBuf) {
    provider_full(port, None, None, dir, token)
}

fn provider_with_bind(
    port: Option<u16>,
    bind_addresses: Option<Vec<std::net::IpAddr>>,
    dir: &str,
    token: &str,
) -> (Arc<dyn ServerPairingInfo>, PathBuf) {
    provider_full(port, bind_addresses, None, dir, token)
}

fn provider_full(
    port: Option<u16>,
    bind_addresses: Option<Vec<std::net::IpAddr>>,
    tc_address: Option<String>,
    dir: &str,
    token: &str,
) -> (Arc<dyn ServerPairingInfo>, PathBuf) {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let tmpdir =
        std::env::temp_dir().join(format!("intentd-test-{}-{nanos}-{dir}", std::process::id()));
    std::fs::create_dir_all(&tmpdir).unwrap();
    let store = crate::AsyncTokenStore::new(Arc::new(MemoryStore::with(token)));
    let p: Arc<dyn ServerPairingInfo> = Arc::new(MockPairingInfo {
        port,
        bind_addresses,
        tc_address,
        data_dir: tmpdir.clone(),
        token_store: store,
    });
    (p, tmpdir)
}

#[test]
fn build_uri_joins_hosts_with_commas() {
    let hosts = vec!["192.168.1.10".to_string(), "10.0.0.5".to_string()];
    let uri = build_pairing_uri(&hosts, 5181, "AA:BB:CC", "deadbeef", None);
    assert_eq!(
        uri,
        "intent://pair?v=1&host=192.168.1.10,10.0.0.5&port=5181&fp=AA:BB:CC&token=deadbeef"
    );
}

#[test]
fn build_uri_single_host() {
    let hosts = vec!["192.168.1.10".to_string()];
    let uri = build_pairing_uri(&hosts, 443, "FP", "t0k3n", None);
    assert_eq!(
        uri,
        "intent://pair?v=1&host=192.168.1.10&port=443&fp=FP&token=t0k3n"
    );
}

#[test]
fn build_uri_percent_encodes_reserved_characters() {
    // Generated values pass through unchanged (hex, colons, dots), but an
    // env-injected token with reserved characters must not break the query.
    let hosts = vec!["192.168.1.10".to_string()];
    let uri = build_pairing_uri(&hosts, 443, "AA:BB", "a&b=c%d", None);
    assert_eq!(
        uri,
        "intent://pair?v=1&host=192.168.1.10&port=443&fp=AA:BB&token=a%26b%3Dc%25d"
    );
}

#[test]
fn build_uri_appends_tc_param_when_present() {
    // Additive last param: existing clients parse the leading fields
    // unchanged and tolerate the unknown `tc=`.
    let hosts = vec!["192.168.1.10".to_string()];
    let uri = build_pairing_uri(&hosts, 443, "FP", "tok", Some("tc7f2a91.tailcat.net"));
    assert_eq!(
        uri,
        "intent://pair?v=1&host=192.168.1.10&port=443&fp=FP&token=tok&tc=tc7f2a91.tailcat.net"
    );
}

#[test]
fn classify_get_info() {
    let req = json!({"jsonrpc": "2.0", "method": "pairing.getInfo", "id": 7});
    let r = classify(&req).unwrap();
    assert!(r.id_present);
    assert_eq!(r.id_echo, json!(7));
}

#[test]
fn classify_ignores_other_methods_and_bad_envelope() {
    assert!(
        classify(&json!({"jsonrpc": "2.0", "method": "server.pairingInfo", "id": 1})).is_none()
    );
    assert!(classify(&json!({"jsonrpc": "1.0", "method": "pairing.getInfo", "id": 1})).is_none());
}

#[tokio::test]
async fn handle_get_info_local_success_shape() {
    let token = "abababababababababababababababababababababababababababababababab";
    let (provider, tmpdir) = provider(Some(5181), "pairing_get_info_local", token);
    let req = PairingRequest {
        id_present: true,
        id_echo: json!(1),
    };
    let resp = handle(req, &provider, true).await.unwrap();
    let parsed: Value = serde_json::from_str(&resp).unwrap();
    assert_eq!(parsed["jsonrpc"], "2.0");
    assert_eq!(parsed["id"], 1);
    let result = &parsed["result"];
    assert_eq!(result["token"], token);
    assert_eq!(result["port"], 5181);
    assert_eq!(result["version"], 1);
    assert!(result["hosts"].is_array());
    let fp = result["fingerprint"].as_str().unwrap();
    assert!(fp.contains(':'), "colon-separated hex fingerprint");
    let hosts: Vec<String> = serde_json::from_value(result["hosts"].clone()).unwrap();
    let expected_uri = build_pairing_uri(&hosts, 5181, fp, token, None);
    assert_eq!(result["uri"].as_str().unwrap(), expected_uri);
    assert!(result["uri"]
        .as_str()
        .unwrap()
        .starts_with("intent://pair?v=1&host="));
    // No tunnel in the snapshot: tcAddress is ABSENT, not null, and the URI
    // carries no tc= param.
    assert!(result.get("tcAddress").is_none());
    assert!(!result["uri"].as_str().unwrap().contains("&tc="));
    let _ = std::fs::remove_dir_all(&tmpdir);
}

#[tokio::test]
async fn handle_get_info_includes_tc_address_when_tunnel_up() {
    let token = "abababababababababababababababababababababababababababababababab";
    let (provider, tmpdir) = provider_full(
        Some(5181),
        None,
        Some("tc7f2a91.tailcat.net".to_string()),
        "pairing_get_info_tc",
        token,
    );
    let req = PairingRequest {
        id_present: true,
        id_echo: json!(1),
    };
    let resp = handle(req, &provider, true).await.unwrap();
    let parsed: Value = serde_json::from_str(&resp).unwrap();
    let result = &parsed["result"];
    assert_eq!(result["tcAddress"], "tc7f2a91.tailcat.net");
    let uri = result["uri"].as_str().unwrap();
    assert!(
        uri.ends_with("&tc=tc7f2a91.tailcat.net"),
        "tc= is the additive last param: {uri}"
    );
    let _ = std::fs::remove_dir_all(&tmpdir);
}

#[tokio::test]
async fn handle_get_info_remote_rejects() {
    let (provider, tmpdir) = provider(Some(5181), "pairing_get_info_remote", "tok");
    let req = PairingRequest {
        id_present: true,
        id_echo: json!(1),
    };
    let resp = handle(req, &provider, false).await.unwrap();
    let parsed: Value = serde_json::from_str(&resp).unwrap();
    assert_eq!(parsed["error"]["code"], -32001);
    assert!(parsed["error"]["message"]
        .as_str()
        .unwrap()
        .contains("local-only"));
    let _ = std::fs::remove_dir_all(&tmpdir);
}

#[tokio::test]
async fn handle_get_info_no_tcp_listener_errors() {
    let (provider, tmpdir) = provider(None, "pairing_get_info_no_tcp", "tok");
    let req = PairingRequest {
        id_present: true,
        id_echo: json!(1),
    };
    let resp = handle(req, &provider, true).await.unwrap();
    let parsed: Value = serde_json::from_str(&resp).unwrap();
    assert!(parsed["error"].is_object());
    assert!(parsed["error"]["message"]
        .as_str()
        .unwrap()
        .contains("TCP listener is not running"));
    // Machine-readable discriminator so `intentd pair` stops matching on
    // prose (monorepo#1822).
    assert_eq!(parsed["error"]["data"]["code"], "listener-down");
    let _ = std::fs::remove_dir_all(&tmpdir);
}

#[tokio::test]
async fn handle_get_info_specific_bind_advertises_only_that_host() {
    // A listener bound to a specific address is reachable only there, so the
    // payload must advertise exactly that host — never LAN IPs the listener
    // does not answer on (monorepo#2900).
    let token = "abababababababababababababababababababababababababababababababab";
    let bind: std::net::IpAddr = "192.168.1.23".parse().unwrap();
    let (provider, tmpdir) =
        provider_with_bind(Some(5181), Some(vec![bind]), "pairing_bind_specific", token);
    let req = PairingRequest {
        id_present: true,
        id_echo: json!(1),
    };
    let resp = handle(req, &provider, true).await.unwrap();
    let parsed: Value = serde_json::from_str(&resp).unwrap();
    let hosts: Vec<String> = serde_json::from_value(parsed["result"]["hosts"].clone()).unwrap();
    assert_eq!(hosts, vec!["192.168.1.23".to_string()]);
    let fp = parsed["result"]["fingerprint"].as_str().unwrap();
    let expected_uri = build_pairing_uri(&hosts, 5181, fp, token, None);
    assert_eq!(parsed["result"]["uri"].as_str().unwrap(), expected_uri);
    let _ = std::fs::remove_dir_all(&tmpdir);
}

#[tokio::test]
async fn handle_get_info_loopback_bind_advertises_no_hosts() {
    // Loopback is never advertised to pairing clients — it is not dialable
    // from another device even when bound, so a loopback-only bind yields an
    // empty host list (and a `host=`-less route set in the URI).
    let token = "abababababababababababababababababababababababababababababababab";
    let bind: std::net::IpAddr = "127.0.0.1".parse().unwrap();
    let (provider, tmpdir) =
        provider_with_bind(Some(5181), Some(vec![bind]), "pairing_bind_loopback", token);
    let req = PairingRequest {
        id_present: true,
        id_echo: json!(1),
    };
    let resp = handle(req, &provider, true).await.unwrap();
    let parsed: Value = serde_json::from_str(&resp).unwrap();
    let hosts: Vec<String> = serde_json::from_value(parsed["result"]["hosts"].clone()).unwrap();
    assert_eq!(hosts, Vec::<String>::new());
    let fp = parsed["result"]["fingerprint"].as_str().unwrap();
    let expected_uri = build_pairing_uri(&hosts, 5181, fp, token, None);
    assert_eq!(parsed["result"]["uri"].as_str().unwrap(), expected_uri);
    let _ = std::fs::remove_dir_all(&tmpdir);
}
