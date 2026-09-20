//! Unit tests for the invite fast paths: link construction, classify, the
//! `invite.redeem` phase split, and the invite-error `data.code` mapping.

use std::sync::Arc;

use intent_core::{InviteErrorKind, WorkspaceApi};
use serde_json::{json, Value};

use super::*;

#[test]
fn invite_uri_carries_envelope_minus_token_plus_invite_fields() {
    let uri = build_invite_uri(
        &["192.168.1.10".to_string(), "10.0.0.5".to_string()],
        7443,
        "AB:CD",
        "inv-1",
        "s3cret",
        None,
    );
    assert_eq!(
        uri,
        "intent://invite?v=1&host=192.168.1.10,10.0.0.5&port=7443&fp=AB:CD&inviteId=inv-1&secret=s3cret"
    );
    assert!(!uri.contains("token="));
    let with_tc = build_invite_uri(&[], 7443, "AB", "i", "s", Some("tc-abc"));
    assert!(with_tc.ends_with("&tc=tc-abc"), "{with_tc}");
    let encoded = build_invite_uri(&[], 1, "AB", "a&b", "x=y", None);
    assert!(encoded.contains("inviteId=a%26b&secret=x%3Dy"), "{encoded}");
}

#[test]
fn classify_picks_the_two_invite_methods_only() {
    let create = classify(&json!({
        "jsonrpc": "2.0", "id": 1, "method": "workspace.invite.create",
        "params": { "workspaceId": "ws" }
    }))
    .expect("classified");
    assert_eq!(create.method, InviteMethod::Create);
    assert!(create.id_present);
    let redeem =
        classify(&json!({ "jsonrpc": "2.0", "method": "invite.redeem" })).expect("classified");
    assert_eq!(redeem.method, InviteMethod::Redeem);
    assert!(!redeem.id_present);
    assert!(
        classify(&json!({ "jsonrpc": "2.0", "id": 1, "method": "workspace.invite.list" }))
            .is_none()
    );
    assert!(classify(&json!({ "jsonrpc": "1.0", "id": 1, "method": "invite.redeem" })).is_none());
    assert!(classify(&json!({ "jsonrpc": "2.0", "id": {}, "method": "invite.redeem" })).is_none());
}

/// Records which redeem phase ran and returns a canned invite error.
struct RedeemStub {
    calls: std::sync::Mutex<Vec<String>>,
}

impl WorkspaceApi for RedeemStub {
    fn invite_redeem_start(
        &self,
        invite_id: String,
        secret: String,
    ) -> intent_core::BoxFuture<'_, intent_core::Result<Value>> {
        self.calls
            .lock()
            .unwrap()
            .push(format!("start:{invite_id}:{secret}"));
        Box::pin(async { Err(intent_core::Error::Invite(InviteErrorKind::Expired)) })
    }
    fn invite_redeem_wait(
        &self,
        flow_id: String,
    ) -> intent_core::BoxFuture<'_, intent_core::Result<Value>> {
        self.calls.lock().unwrap().push(format!("wait:{flow_id}"));
        Box::pin(async { Ok(json!({ "status": "authorized", "token": "t" })) })
    }
}

#[tokio::test]
async fn redeem_routes_phases_and_maps_invite_errors_to_data_code() {
    let stub = Arc::new(RedeemStub {
        calls: std::sync::Mutex::new(Vec::new()),
    });
    let api: Arc<dyn WorkspaceApi> = stub.clone();

    let req = classify(&json!({
        "jsonrpc": "2.0", "id": 7, "method": "invite.redeem",
        "params": { "inviteId": "inv", "secret": "sec" }
    }))
    .unwrap();
    let frame: Value = serde_json::from_str(&handle_redeem(req, &api).await.unwrap()).unwrap();
    assert_eq!(frame["id"], json!(7));
    assert_eq!(frame["error"]["data"]["code"], json!("invite-expired"));
    assert_eq!(
        frame["error"]["code"],
        json!(intent_core::Error::Invite(InviteErrorKind::Expired).code())
    );

    let req = classify(&json!({
        "jsonrpc": "2.0", "id": 8, "method": "invite.redeem",
        "params": { "flowId": " flow-1 " }
    }))
    .unwrap();
    let frame: Value = serde_json::from_str(&handle_redeem(req, &api).await.unwrap()).unwrap();
    assert_eq!(frame["result"]["status"], json!("authorized"));

    let req =
        classify(&json!({ "jsonrpc": "2.0", "id": 9, "method": "invite.redeem", "params": {} }))
            .unwrap();
    let frame: Value = serde_json::from_str(&handle_redeem(req, &api).await.unwrap()).unwrap();
    assert_eq!(frame["error"]["code"], json!(-32602));

    let req =
        classify(&json!({ "jsonrpc": "2.0", "method": "invite.redeem", "params": {} })).unwrap();
    assert!(
        handle_redeem(req, &api).await.is_none(),
        "notification: no frame"
    );

    assert_eq!(
        *stub.calls.lock().unwrap(),
        vec!["start:inv:sec".to_string(), "wait:flow-1".to_string()]
    );
}

#[tokio::test]
async fn create_without_a_pairing_provider_is_unsupported_and_mints_nothing() {
    let stub = Arc::new(RedeemStub {
        calls: std::sync::Mutex::new(Vec::new()),
    });
    let api: Arc<dyn WorkspaceApi> = stub.clone();
    let req = classify(&json!({
        "jsonrpc": "2.0", "id": 1, "method": "workspace.invite.create",
        "params": { "workspaceId": "ws" }
    }))
    .unwrap();
    let frame: Value =
        serde_json::from_str(&handle_create(req, &api, None).await.unwrap()).unwrap();
    assert_eq!(
        frame["error"]["code"],
        json!(intent_core::Error::Unsupported(String::new()).code())
    );
    assert!(stub.calls.lock().unwrap().is_empty());
}

/// Minimal pairing provider for the resolver: a fixed port / tunnel address
/// over a fresh data dir (the TLS certificate is generated on first use).
struct StubPairingInfo {
    port: Option<u16>,
    tc_address: Option<String>,
    data_dir: std::path::PathBuf,
    token_store: crate::AsyncTokenStore,
}

struct NoToken;

impl crate::auth::TokenStore for NoToken {
    fn load_token(&self) -> Option<String> {
        None
    }
    fn store_token(&self, _token: &str) -> intent_core::Result<()> {
        Ok(())
    }
}

impl crate::server::ServerPairingInfo for StubPairingInfo {
    fn pairing_snapshot(
        &self,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = crate::server::PairingSnapshot> + Send + '_>,
    > {
        let port = self.port;
        let tc_address = self.tc_address.clone();
        Box::pin(async move {
            crate::server::PairingSnapshot {
                port,
                bind_addresses: Some(vec![std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)]),
                tc_address,
            }
        })
    }
    fn host_environment(&self) -> crate::host_env::HostEnvironment {
        crate::host_env::HostEnvironment {
            hostname: "test".to_string(),
            pretty_hostname: "test".to_string(),
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

fn stub_provider(
    port: Option<u16>,
    tc_address: Option<&str>,
) -> (Arc<dyn crate::server::ServerPairingInfo>, tempfile::TempDir) {
    let mut tmpdir = tempfile::Builder::new()
        .prefix("intentd-test-invite-links-")
        .tempdir()
        .expect("create test temp dir");
    if std::env::var_os("INTENTD_TEST_KEEP_TMP").is_some_and(|v| !v.is_empty()) {
        tmpdir.disable_cleanup(true);
    }
    let provider = Arc::new(StubPairingInfo {
        port,
        tc_address: tc_address.map(str::to_string),
        data_dir: tmpdir.path().to_path_buf(),
        token_store: crate::AsyncTokenStore::new(Arc::new(NoToken)),
    });
    (provider, tmpdir)
}

/// The services-facing resolver rebuilds exactly the link `create` mints —
/// same envelope, same formatter — and answers `None` rather than an error
/// when the listener is down or nothing is dialable (loopback bind, no
/// tunnel), so `workspace.invite.list` never fails for it.
#[tokio::test]
async fn link_resolver_rebuilds_the_minted_link_and_is_none_when_undialable() {
    let (provider, _dir) = stub_provider(Some(7443), Some("tc-abc"));
    let minted = link_envelope(Some(&provider)).await.expect("envelope");
    let resolved = InviteLinkResolver::new(provider.clone())
        .invite_link_envelope()
        .await
        .expect("resolves");
    let url = resolved.invite_url("inv-1", "s3cret");
    assert_eq!(url, minted.invite_url("inv-1", "s3cret"));
    assert_eq!(
        url,
        format!(
            "intent://invite?v=1&host=&port=7443&fp={}&inviteId=inv-1&secret=s3cret&tc=tc-abc",
            encode_query_value(&minted.fingerprint)
        )
    );

    let (down, _dir) = stub_provider(None, Some("tc-abc"));
    assert!(matches!(
        link_envelope(Some(&down)).await,
        Err(Error::ListenerDown)
    ));
    assert!(InviteLinkResolver::new(down)
        .invite_link_envelope()
        .await
        .is_none());

    let (undialable, _dir) = stub_provider(Some(7443), None);
    assert!(matches!(
        link_envelope(Some(&undialable)).await,
        Err(Error::Unsupported(_))
    ));
    assert!(InviteLinkResolver::new(undialable)
        .invite_link_envelope()
        .await
        .is_none());
}

/// Service half of `workspace.invite.create`: answers `{ invite, secret }`
/// with no `url` (the transport stamps it) and counts its calls.
struct CreateStub {
    calls: std::sync::atomic::AtomicUsize,
}

impl WorkspaceApi for CreateStub {
    fn workspace_invite_create(
        &self,
        workspace_id: WorkspaceId,
        _pin_login: Option<String>,
        _expires_in_secs: Option<u64>,
    ) -> intent_core::BoxFuture<'_, intent_core::Result<Value>> {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Box::pin(async move {
            Ok(json!({
                "invite": { "id": "inv-1", "workspaceId": workspace_id },
                "secret": "s3cret",
            }))
        })
    }
}

/// `create` resolves the envelope once and stamps the one link it formats
/// as both the top-level `url` and `invite.url`; when no link can be built
/// (listener down) the create is refused before the service mints anything,
/// so neither `url` can exist without the other.
#[tokio::test]
async fn create_stamps_the_same_link_as_url_and_invite_url() {
    let stub = Arc::new(CreateStub {
        calls: std::sync::atomic::AtomicUsize::new(0),
    });
    let api: Arc<dyn WorkspaceApi> = stub.clone();
    let create_req = || {
        classify(&json!({
            "jsonrpc": "2.0", "id": 1, "method": "workspace.invite.create",
            "params": { "workspaceId": "ws" }
        }))
        .unwrap()
    };

    let (provider, _dir) = stub_provider(Some(7443), Some("tc-abc"));
    let frame: Value = serde_json::from_str(
        &handle_create(create_req(), &api, Some(&provider))
            .await
            .unwrap(),
    )
    .unwrap();
    assert!(frame.get("error").is_none(), "{frame}");
    let r = &frame["result"];
    let url = r["url"].as_str().expect("url");
    assert_eq!(r["invite"]["url"], json!(url), "{r}");
    assert_eq!(
        url,
        link_envelope(Some(&provider))
            .await
            .expect("envelope")
            .invite_url("inv-1", "s3cret")
    );
    assert_eq!(r["secret"], json!("s3cret"));
    assert_eq!(r["invite"]["id"], json!("inv-1"));
    assert_eq!(stub.calls.load(std::sync::atomic::Ordering::SeqCst), 1);

    let (down, _dir) = stub_provider(None, Some("tc-abc"));
    let frame: Value = serde_json::from_str(
        &handle_create(create_req(), &api, Some(&down))
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        frame["error"]["data"]["code"],
        json!("listener-down"),
        "{frame}"
    );
    assert!(frame.get("result").is_none(), "{frame}");
    assert_eq!(
        stub.calls.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "nothing minted without an envelope"
    );
}

#[test]
fn non_invite_method_on_invite_endpoint_is_unauthorized() {
    let frame =
        refuse_non_invite(&json!({ "jsonrpc": "2.0", "id": 3, "method": "workspace.list" }))
            .expect("frame");
    let v: Value = serde_json::from_str(&frame).unwrap();
    assert_eq!(v["error"]["code"], json!(-32001));
    assert_eq!(v["error"]["message"], json!(INVITE_ENDPOINT_ONLY_MESSAGE));
    assert!(refuse_non_invite(&json!({ "jsonrpc": "2.0", "method": "workspace.list" })).is_none());
}

fn start_req(id: i64) -> InviteRequest {
    classify(&json!({
        "jsonrpc": "2.0", "id": id, "method": "invite.redeem",
        "params": { "inviteId": "inv", "secret": format!("guess-{id}") }
    }))
    .unwrap()
}

/// The start throttle is a token bucket: the burst is admitted, the next
/// serial attempt is refused with `invite-flow-busy` until a whole refill
/// interval has elapsed, one token comes back per interval, and the bucket
/// never overfills past the burst.
#[test]
fn redeem_start_throttle_refuses_after_the_burst_until_refill() {
    let t0 = Instant::now();
    let throttle = Arc::new(Mutex::new(RedeemThrottle::new(t0)));
    for i in 0..INVITE_START_BURST {
        assert!(
            admit_redeem(&start_req(i64::from(i)), &throttle, t0).is_ok(),
            "burst attempt {i} admitted"
        );
    }
    let refused = admit_redeem(&start_req(100), &throttle, t0).expect_err("burst exhausted");
    let v: Value = serde_json::from_str(&refused.expect("frame")).unwrap();
    assert_eq!(v["id"], json!(100));
    assert_eq!(v["error"]["data"]["code"], json!("invite-flow-busy"));
    let just_short = t0 + INVITE_START_REFILL.saturating_sub(Duration::from_millis(1));
    assert!(
        admit_redeem(&start_req(101), &throttle, just_short).is_err(),
        "still refused just before the refill interval"
    );
    let refilled = t0 + INVITE_START_REFILL;
    assert!(
        admit_redeem(&start_req(102), &throttle, refilled).is_ok(),
        "one token back after one interval"
    );
    assert!(
        admit_redeem(&start_req(103), &throttle, refilled).is_err(),
        "exactly one token per interval"
    );
    let much_later = t0 + INVITE_START_REFILL * (INVITE_START_BURST * 4);
    for i in 0..INVITE_START_BURST {
        assert!(
            admit_redeem(&start_req(200 + i64::from(i)), &throttle, much_later).is_ok(),
            "refilled burst attempt {i}"
        );
    }
    assert!(
        admit_redeem(&start_req(300), &throttle, much_later).is_err(),
        "never overfills past the burst"
    );
    // A throttled notification is dropped without a frame.
    let note =
        classify(&json!({ "jsonrpc": "2.0", "method": "invite.redeem", "params": { "inviteId": "inv", "secret": "s" } }))
            .unwrap();
    assert_eq!(admit_redeem(&note, &throttle, much_later), Err(None));
}

/// Phase-2 waits are never throttled (the start already paid), and a
/// throttled start reaches neither the store nor the upstream flow: the
/// refusal is produced before `handle_redeem` runs.
#[tokio::test]
async fn throttled_starts_make_no_upstream_calls_and_waits_pass() {
    let stub = Arc::new(RedeemStub {
        calls: std::sync::Mutex::new(Vec::new()),
    });
    let api: Arc<dyn WorkspaceApi> = stub.clone();
    let t0 = Instant::now();
    let throttle = Arc::new(Mutex::new(RedeemThrottle::new(t0)));
    for i in 0..INVITE_START_BURST {
        assert!(admit_redeem(&start_req(i64::from(i)), &throttle, t0).is_ok());
    }
    let mut refusals = 0;
    for i in 0..20 {
        match admit_redeem(&start_req(500 + i), &throttle, t0) {
            Ok(()) => {
                handle_redeem(start_req(500 + i), &api).await;
            }
            Err(_) => refusals += 1,
        }
    }
    assert_eq!(refusals, 20, "every post-burst start refused");
    assert!(
        stub.calls.lock().unwrap().is_empty(),
        "no store/upstream call while throttled: {:?}",
        stub.calls.lock().unwrap()
    );
    let wait = classify(&json!({
        "jsonrpc": "2.0", "id": 9, "method": "invite.redeem", "params": { "flowId": "flow-1" }
    }))
    .unwrap();
    assert_eq!(admit_redeem(&wait, &throttle, t0), Ok(()));
    let frame: Value = serde_json::from_str(&handle_redeem(wait, &api).await.unwrap()).unwrap();
    assert_eq!(frame["result"]["status"], json!("authorized"));
    assert_eq!(*stub.calls.lock().unwrap(), vec!["wait:flow-1".to_string()]);
}
