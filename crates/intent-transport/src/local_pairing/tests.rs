//! Unit tests for the local code pairing fast paths (§5.58): local-only
//! administration, redemption refused outside `/local-pair`, listener-down,
//! and the grant built by [`redeem_grant`].

use std::future::Future;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::pin::Pin;
use std::sync::Arc;

use intent_core::{Caller, HostRole, PrincipalId, WorkspaceApi};
use serde_json::{json, Value};

use super::*;
use crate::server::{PairingSnapshot, ServerPairingInfo};

struct Api {
    pairing: Arc<intent_services::local_pairing::LocalPairing>,
}

impl WorkspaceApi for Api {
    fn local_pairing(&self) -> Option<Arc<dyn LocalPairingService>> {
        Some(self.pairing.clone())
    }
}

struct Provider {
    port: Option<u16>,
    data_dir: std::path::PathBuf,
    token_store: crate::AsyncTokenStore,
}

struct NoTokens;

impl crate::auth::TokenStore for NoTokens {
    fn load_token(&self) -> Option<String> {
        None
    }
    fn store_token(&self, _token: &str) -> intent_core::Result<()> {
        Ok(())
    }
}

impl ServerPairingInfo for Provider {
    fn pairing_snapshot(&self) -> Pin<Box<dyn Future<Output = PairingSnapshot> + Send + '_>> {
        let port = self.port;
        Box::pin(async move {
            PairingSnapshot {
                port,
                bind_addresses: None,
                tc_address: None,
            }
        })
    }
    fn host_environment(&self) -> crate::host_env::HostEnvironment {
        crate::host_env::HostEnvironment {
            hostname: "host".into(),
            pretty_hostname: "Host Mac".into(),
            device_kind: None,
            hardware_model: None,
        }
    }
    fn data_dir(&self) -> &std::path::Path {
        &self.data_dir
    }
    fn token_store(&self) -> &crate::AsyncTokenStore {
        &self.token_store
    }
}

struct Fixture {
    _dir: tempfile::TempDir,
    api: Api,
    provider: Provider,
}

async fn fixture(port: Option<u16>) -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let store = intent_store::Store::open(&dir.path().join("pairing.db"))
        .await
        .unwrap();
    let pairing = intent_services::local_pairing::LocalPairing::new(store, None).unwrap();
    Fixture {
        provider: Provider {
            port,
            data_dir: dir.path().to_path_buf(),
            token_store: crate::AsyncTokenStore::new(Arc::new(NoTokens)),
        },
        api: Api { pairing },
        _dir: dir,
    }
}

fn owner() -> Caller {
    Caller::Wire {
        principal_id: PrincipalId::from("owner"),
        host_role: HostRole::Owner,
    }
}

async fn call(f: &Fixture, is_tcp: bool, method: &str, params: Value) -> Value {
    let frame = json!({"jsonrpc":"2.0","id":7,"method":method,"params":params});
    let req = classify(&frame).expect("classified");
    let reply = crate::context::with_request_context(
        is_tcp,
        Some(owner()),
        handle(req, &f.api, &f.provider),
    )
    .await
    .expect("reply");
    serde_json::from_str(&reply).unwrap()
}

#[test]
fn loopback_peers() {
    assert!(is_loopback_peer(IpAddr::V4(Ipv4Addr::LOCALHOST)));
    assert!(is_loopback_peer(IpAddr::V4(Ipv4Addr::new(127, 9, 8, 7))));
    assert!(is_loopback_peer(IpAddr::V6(Ipv6Addr::LOCALHOST)));
    assert!(is_loopback_peer(IpAddr::V6(
        Ipv4Addr::LOCALHOST.to_ipv6_mapped()
    )));
    assert!(!is_loopback_peer(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 2))));
    assert!(!is_loopback_peer(IpAddr::V6(
        Ipv4Addr::new(10, 0, 0, 1).to_ipv6_mapped()
    )));
    assert!(!is_loopback_peer(IpAddr::V4(Ipv4Addr::UNSPECIFIED)));
}

#[test]
fn classifies_only_the_five_methods() {
    for method in [
        "pairing.createLocalCode",
        "pairing.cancelLocalCode",
        "pairing.listLocalCredentials",
        "pairing.revokeLocalCredential",
        "pairing.redeemLocalCode",
    ] {
        assert!(classify(&json!({"jsonrpc":"2.0","id":1,"method":method})).is_some());
    }
    assert!(classify(&json!({"jsonrpc":"2.0","id":1,"method":"pairing.getInfo"})).is_none());
    assert!(classify(&json!({"id":1,"method":"pairing.createLocalCode"})).is_none());
}

#[tokio::test]
async fn administration_is_local_only() {
    let f = fixture(Some(4123)).await;
    for (method, params) in [
        ("pairing.createLocalCode", json!({})),
        ("pairing.cancelLocalCode", json!({})),
        ("pairing.listLocalCredentials", json!({})),
        ("pairing.revokeLocalCredential", json!({"credentialId":"x"})),
    ] {
        let reply = call(&f, true, method, params).await;
        assert_eq!(reply["error"]["code"], -32001, "{method}");
        assert_eq!(reply["error"]["message"], format!("{method} is local-only"));
    }
    // Nothing was created by the refused TCP call.
    assert_eq!(
        call(&f, false, "pairing.cancelLocalCode", json!({})).await["result"],
        json!({"cancelled": false})
    );
}

#[tokio::test]
async fn redeem_is_refused_outside_local_pair() {
    let f = fixture(Some(4123)).await;
    for is_tcp in [true, false] {
        let reply = call(
            &f,
            is_tcp,
            "pairing.redeemLocalCode",
            json!({"code":"ABC-DEF","appName":"Intent"}),
        )
        .await;
        assert_eq!(reply["error"]["code"], -32001);
        assert_eq!(reply["error"]["message"], REDEEM_ELSEWHERE_MESSAGE);
    }
}

#[tokio::test]
async fn create_needs_a_listener() {
    let f = fixture(None).await;
    let reply = call(&f, false, "pairing.createLocalCode", json!({})).await;
    assert_eq!(reply["error"]["code"], -32603);
    assert_eq!(reply["error"]["data"]["code"], "listener-down");
    assert_eq!(
        call(&f, false, "pairing.cancelLocalCode", json!({})).await["result"]["cancelled"],
        false
    );
}

#[tokio::test]
async fn local_admin_round_trip() {
    let f = fixture(Some(4123)).await;
    let created = call(&f, false, "pairing.createLocalCode", json!({})).await;
    let code = created["result"]["code"].as_str().unwrap().to_owned();
    let local = &created["result"]["localCode"];
    assert_eq!(local["state"], "active");
    assert_eq!(local["attemptsRemaining"], 5);
    assert!(local.get("cancelReason").is_none());

    let grant = redeem_grant(
        &f.api,
        &f.provider,
        Some(&json!({"code": code, "appName": "Intent Alpha"})),
        IpAddr::V4(Ipv4Addr::LOCALHOST),
        "Intent (dev build)",
    )
    .await
    .unwrap();
    assert_eq!(grant["version"], 1);
    assert_eq!(grant["host"], "127.0.0.1");
    assert_eq!(grant["port"], 4123);
    assert_eq!(grant["displayName"], "Intent (dev build)");
    assert_eq!(grant["prettyHostname"], "Host Mac");
    let fingerprint = crate::ensure_tls_certificate(&f.provider.data_dir)
        .unwrap()
        .fingerprint256;
    assert_eq!(grant["fingerprint"], fingerprint);
    assert_eq!(grant["token"].as_str().unwrap().len(), 64);
    let credential_id = grant["credentialId"].as_str().unwrap().to_owned();

    let listed = call(&f, false, "pairing.listLocalCredentials", json!({})).await;
    let credentials = listed["result"]["credentials"].as_array().unwrap();
    assert_eq!(credentials.len(), 1);
    assert_eq!(credentials[0]["credentialId"], credential_id.as_str());
    assert_eq!(credentials[0]["appName"], "Intent Alpha");
    assert_eq!(credentials[0]["connected"], false);
    assert!(credentials[0].get("token").is_none());

    let revoked = call(
        &f,
        false,
        "pairing.revokeLocalCredential",
        json!({"credentialId": credential_id}),
    )
    .await;
    assert_eq!(revoked["result"], json!({"revoked": true}));
    let again = call(
        &f,
        false,
        "pairing.revokeLocalCredential",
        json!({"credentialId": credential_id}),
    )
    .await;
    assert_eq!(again["result"], json!({"revoked": false}));
    let unknown = call(
        &f,
        false,
        "pairing.revokeLocalCredential",
        json!({"credentialId": "nope"}),
    )
    .await;
    assert_eq!(unknown["error"]["code"], -32602);
    assert_eq!(unknown["error"]["data"]["code"], "not-found");
    let missing = call(&f, false, "pairing.revokeLocalCredential", json!({})).await;
    assert_eq!(missing["error"]["data"]["code"], "invalid-params");
}

#[tokio::test]
async fn new_code_replaces_and_cancel_by_id() {
    let f = fixture(Some(4123)).await;
    let first = call(&f, false, "pairing.createLocalCode", json!({})).await;
    let first_id = first["result"]["localCode"]["codeId"].clone();
    let second = call(&f, false, "pairing.createLocalCode", json!({})).await;
    let second_id = second["result"]["localCode"]["codeId"].clone();
    assert_eq!(
        call(
            &f,
            false,
            "pairing.cancelLocalCode",
            json!({"codeId": first_id})
        )
        .await["result"]["cancelled"],
        false
    );
    assert_eq!(
        call(
            &f,
            false,
            "pairing.cancelLocalCode",
            json!({"codeId": second_id})
        )
        .await["result"]["cancelled"],
        true
    );
}

#[tokio::test]
async fn redeem_grant_checks_peer_then_params_then_code() {
    let f = fixture(Some(4123)).await;
    let created = call(&f, false, "pairing.createLocalCode", json!({})).await;
    let code = created["result"]["code"].as_str().unwrap().to_owned();
    let lan = IpAddr::V4(Ipv4Addr::new(192, 168, 1, 20));
    assert_eq!(
        redeem_grant(
            &f.api,
            &f.provider,
            Some(&json!({"code": code, "appName": "A"})),
            lan,
            "B"
        )
        .await
        .err(),
        Some(LocalPairingError::NotLoopback)
    );
    let v6 = IpAddr::V6(Ipv6Addr::LOCALHOST);
    assert!(matches!(
        redeem_grant(&f.api, &f.provider, Some(&json!({"code": code})), v6, "B").await,
        Err(LocalPairingError::InvalidParams(_))
    ));
    assert!(matches!(
        redeem_grant(
            &f.api,
            &f.provider,
            Some(&json!({"code": "AB", "appName": "A"})),
            v6,
            "B"
        )
        .await,
        Err(LocalPairingError::InvalidParams(_))
    ));
    let grant = redeem_grant(
        &f.api,
        &f.provider,
        Some(&json!({"code": code, "appName": "A"})),
        v6,
        "B",
    )
    .await
    .unwrap();
    assert_eq!(grant["host"], "::1");
    assert_eq!(
        redeem_grant(
            &f.api,
            &f.provider,
            Some(&json!({"code": code, "appName": "A"})),
            v6,
            "B"
        )
        .await
        .err(),
        Some(LocalPairingError::Expired)
    );
}

#[tokio::test]
async fn redeem_with_listener_down_keeps_the_code() {
    let mut f = fixture(Some(4123)).await;
    let created = call(&f, false, "pairing.createLocalCode", json!({})).await;
    let code = created["result"]["code"].as_str().unwrap().to_owned();
    f.provider.port = None;
    let peer = IpAddr::V4(Ipv4Addr::LOCALHOST);
    let params = json!({"code": code, "appName": "A"});
    assert_eq!(
        redeem_grant(&f.api, &f.provider, Some(&params), peer, "B")
            .await
            .err(),
        Some(LocalPairingError::ListenerDown)
    );
    f.provider.port = Some(4123);
    assert!(redeem_grant(&f.api, &f.provider, Some(&params), peer, "B")
        .await
        .is_ok());
}

#[tokio::test]
async fn notifications_get_no_reply() {
    let f = fixture(Some(4123)).await;
    let frame = json!({"jsonrpc":"2.0","method":"pairing.listLocalCredentials"});
    let req = classify(&frame).unwrap();
    let reply = crate::context::with_request_context(
        false,
        Some(owner()),
        handle(req, &f.api, &f.provider),
    )
    .await;
    assert!(reply.is_none());
}

#[test]
fn error_frames_carry_contract_data() {
    let frame: Value = serde_json::from_str(&error_response(
        &json!(3),
        &LocalPairingError::Invalid {
            attempts_remaining: 2,
        },
    ))
    .unwrap();
    assert_eq!(frame["id"], 3);
    assert_eq!(frame["error"]["code"], -32602);
    assert_eq!(
        frame["error"]["data"],
        json!({"code":"local-code-invalid","attemptsRemaining":2})
    );
}
